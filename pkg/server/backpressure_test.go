package server

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// Signals entry into the real pipe write. The peer controls whether it finishes.
type backpressureConn struct {
	net.Conn
	writes       chan struct{}
	mu           sync.Mutex
	deadlines    []time.Time
	expireWrites atomic.Bool
}

func (c *backpressureConn) Write(p []byte) (int, error) {
	c.writes <- struct{}{}
	return c.Conn.Write(p)
}

func (c *backpressureConn) SetWriteDeadline(deadline time.Time) error {
	c.mu.Lock()
	c.deadlines = append(c.deadlines, deadline)
	c.mu.Unlock()
	if c.expireWrites.Load() {
		deadline = time.Now().Add(-time.Second)
	}
	return c.Conn.SetWriteDeadline(deadline)
}

type backpressureDisconnectHook struct {
	id     uint64
	done   chan struct{}
	once   sync.Once
	reason string
}

func (h *backpressureDisconnectHook) Levels() []logrus.Level { return logrus.AllLevels }
func (h *backpressureDisconnectHook) Fire(entry *logrus.Entry) error {
	if entry.Message == "Client disconnected" && entry.Data["id"] == h.id {
		h.once.Do(func() { h.reason = entry.Data["reason"].(string); close(h.done) })
	}
	return nil
}

func backpressureServer() *Server {
	log := logrus.New()
	log.SetOutput(io.Discard)
	return &Server{Log: log, registry: *newLifecycleRegistry()}
}

func joinBackpressurePeer(t *testing.T, srv *Server, id uint64, name string) (*backpressureConn, net.Conn, *json.Decoder, channelMember) {
	t.Helper()
	conn, peer := net.Pipe()
	observed := &backpressureConn{Conn: conn, writes: make(chan struct{}, 1024)}
	hook := &backpressureDisconnectHook{id: id, done: make(chan struct{})}
	srv.Log.AddHook(hook)
	srv.serveClient(observed, id, "test peer")
	t.Cleanup(func() { peer.Close(); conn.Close(); awaitLivenessSignal(t, hook.done) })
	peer.SetDeadline(time.Now().Add(5 * time.Second))
	if err := json.NewEncoder(peer).Encode(ClientJoinMessage{GenericClientMessage: GenericClientMessage{Type: "join"}, Channel: name, ConnectionType: "master"}); err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(peer)
	var response ClientChannelJoinedResponse
	if err := dec.Decode(&response); err != nil || response.Type != "channel_joined" {
		t.Fatalf("join response = %#v, error = %v", response, err)
	}
	awaitLivenessSignal(t, observed.writes)
	srv.registry.lock.RLock()
	member := srv.registry.clients[id]
	srv.registry.lock.RUnlock()
	return observed, peer, dec, member
}

func readBackpressureResponse(t *testing.T, dec *json.Decoder, kind string) ClientResponse {
	t.Helper()
	var response ClientResponse
	if err := dec.Decode(&response); err != nil {
		t.Fatalf("reading %s: %v", kind, err)
	}
	if response["type"] != kind {
		t.Fatalf("response = %#v, want %s", response, kind)
	}
	return response
}

func sendBackpressureMessage(t *testing.T, ch *channel, sequence int) {
	t.Helper()
	select {
	case ch.messages <- channelMessage{origin: 99, msg: map[string]interface{}{"type": "speak", "sequence": sequence}}:
	case <-time.After(5 * time.Second):
		t.Fatal("channel stopped accepting messages")
	}
}

func TestBackpressureUnreadPeerDoesNotBlockChannel(t *testing.T) {
	srv := backpressureServer()
	slowConn, _, slowDec, slow := joinBackpressurePeer(t, srv, 7, "test")
	_, _, healthyDec, _ := joinBackpressurePeer(t, srv, 8, "test")
	readBackpressureResponse(t, slowDec, "client_joined")
	awaitLivenessSignal(t, slowConn.writes)
	srv.registry.lock.RLock()
	ch := srv.registry.channels["test"]
	srv.registry.lock.RUnlock()
	// First write stays blocked because the slow peer no longer reads.
	sendBackpressureMessage(t, ch, 0)
	awaitLivenessSignal(t, slowConn.writes)
	readBackpressureResponse(t, healthyDec, "speak")
	// Fill exactly the available queue slots, without scheduler assumptions.
	for i := 1; i <= cap(slow.events); i++ {
		sendBackpressureMessage(t, ch, i)
		response := readBackpressureResponse(t, healthyDec, "speak")
		if response["sequence"] != float64(i) {
			t.Fatalf("out of order response: %#v", response)
		}
	}
	sendBackpressureMessage(t, ch, cap(slow.events)+1)
	readBackpressureResponse(t, healthyDec, "speak")
	response := readBackpressureResponse(t, healthyDec, "client_left")
	if response["client"].(map[string]interface{})["id"] != float64(slow.id) {
		t.Fatalf("wrong departed client: %#v", response)
	}
	// A new join is a barrier after leave; the membership snapshot is authoritative.
	_, _, _, _ = joinBackpressurePeer(t, srv, 9, "test")
	readBackpressureResponse(t, healthyDec, "client_joined")
	srv.registry.lock.RLock()
	_, remains := srv.registry.clients[slow.id]
	srv.registry.lock.RUnlock()
	if remains {
		t.Fatal("overflowed client remains registered")
	}
	sendBackpressureMessage(t, ch, 100)
	readBackpressureResponse(t, healthyDec, "speak")
}

func TestBackpressureSendSetsWriteDeadline(t *testing.T) {
	conn, peer := net.Pipe()
	defer peer.Close()
	defer conn.Close()
	observed := &backpressureConn{Conn: conn, writes: make(chan struct{}, 1)}
	c := &client{conn: observed, encoder: json.NewEncoder(observed), log: backpressureServer().Log}
	done := make(chan struct{})
	go func() { c.send(GenericClientResponse{Type: "ping"}); close(done) }()
	awaitLivenessSignal(t, observed.writes)
	observed.mu.Lock()
	deadlines := append([]time.Time(nil), observed.deadlines...)
	observed.mu.Unlock()
	// Expire the real net.Pipe write now, without waiting for a production timeout.
	conn.SetWriteDeadline(time.Now().Add(-time.Second))
	awaitLivenessSignal(t, done)
	if len(deadlines) != 1 || deadlines[0].IsZero() {
		t.Fatalf("send did not bound its socket write: deadlines = %v", deadlines)
	}
	if !c.isStopped() {
		t.Fatal("write timeout did not stop client")
	}
}

func TestBackpressurePingDispatchReleasesRegistryLock(t *testing.T) {
	srv := backpressureServer()
	full := make(chan Message, 1)
	full <- pingMessage{}
	stopEntered, releaseStop, dispatched := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var release sync.Once
	unblock := func() { release.Do(func() { close(releaseStop) }) }
	defer unblock()
	// Make connection shutdown itself wait on a barrier: even this must happen
	// outside registry.lock. There is no event consumer to relieve the queue.
	srv.registry.clients[7] = channelMember{id: 7, events: full, stop: func(string) {
		close(stopEntered)
		<-releaseStop
	}}
	go func() { srv.dispatchPings(); close(dispatched) }()
	awaitLivenessSignal(t, stopEntered)
	joined := make(chan *client, 1)
	go func() { joined <- joinLifecycleClient(t, &srv.registry, "independent", 8) }()
	var independent *client
	select {
	case independent = <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("stuck ping dispatch blocked an independent channel join")
	}
	sendBackpressureMessage(t, independent.channel, 1)
	if msg := receiveLifecycleEvent(t, independent.events); msg.Name() != "channel_message" {
		t.Fatalf("independent delivery = %#v", msg)
	}
	left := make(chan struct{})
	go func() { independent.leaveChannel(); close(left) }()
	awaitLivenessSignal(t, left)
	unblock()
	awaitLivenessSignal(t, dispatched)
}

func TestBackpressurePingOverflowCleansUnreadPeer(t *testing.T) {
	srv := backpressureServer()
	srv.EventQueueSize = 4
	slowConn, _, _, slow := joinBackpressurePeer(t, srv, 7, "slow")
	_, _, healthyDec, _ := joinBackpressurePeer(t, srv, 8, "independent")
	hook := &backpressureDisconnectHook{id: slow.id, done: make(chan struct{})}
	srv.Log.AddHook(hook)
	slow.enqueue(pingMessage{})
	awaitLivenessSignal(t, slowConn.writes)
	for i := 0; i < cap(slow.events); i++ {
		slow.events <- pingMessage{}
	}
	done := make(chan struct{})
	go func() { srv.dispatchPings(); close(done) }()
	awaitLivenessSignal(t, done)
	readBackpressureResponse(t, healthyDec, "ping")
	awaitLivenessSignal(t, hook.done)
	if hook.reason != "Client event queue overflow" {
		t.Fatalf("stop reason = %q", hook.reason)
	}
	srv.registry.lock.RLock()
	_, registered := srv.registry.clients[slow.id]
	_, channelExists := srv.registry.channels["slow"]
	independent := srv.registry.channels["independent"]
	srv.registry.lock.RUnlock()
	if registered || channelExists {
		t.Fatal("ping overflow did not clean membership")
	}
	sendBackpressureMessage(t, independent, 1)
	readBackpressureResponse(t, healthyDec, "speak")
	// Repeated enqueue through a stale snapshot must never send to a closed queue.
	for i := 0; i < 10; i++ {
		slow.enqueue(pingMessage{})
	}
}

func TestBackpressureBlockedWriterCleanup(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "stop", true: "write_timeout"}[timeout], func(t *testing.T) {
			srv := backpressureServer()
			conn, _, _, member := joinBackpressurePeer(t, srv, 7, "test")
			hook := &backpressureDisconnectHook{id: member.id, done: make(chan struct{})}
			srv.Log.AddHook(hook)
			member.enqueue(pingMessage{})
			awaitLivenessSignal(t, conn.writes)
			if timeout {
				// Trigger expiry of the pending real pipe write at a known barrier.
				conn.Conn.SetWriteDeadline(time.Now().Add(-time.Second))
			} else {
				member.stop("test stop")
			}
			awaitLivenessSignal(t, hook.done)
			want := "test stop"
			if timeout {
				want = "Send error"
			}
			if hook.reason != want {
				t.Fatalf("stop reason = %q, want %q", hook.reason, want)
			}
			assertLifecycleRegistryEmpty(t, &srv.registry)
		})
	}
}

func TestBackpressureQueuePreservesBurst(t *testing.T) {
	srv := backpressureServer()
	srv.EventQueueSize = 8
	conn, _, dec, member := joinBackpressurePeer(t, srv, 7, "test")
	hook := &backpressureDisconnectHook{id: member.id, done: make(chan struct{})}
	srv.Log.AddHook(hook)
	member.enqueue(pingMessage{})
	awaitLivenessSignal(t, conn.writes)
	// With the consumer at its socket write, all eight slots remain available
	// for a burst. No event may be dropped, coalesced or reordered.
	events := []Message{
		channelMessage{origin: 99, msg: map[string]interface{}{"type": "key", "pressed": true}},
		channelMessage{origin: 99, msg: map[string]interface{}{"type": "key", "pressed": false}},
		channelMessage{origin: 99, msg: map[string]interface{}{"type": "set_clipboard_text", "text": "clipboard"}},
		channelMessage{origin: 99, msg: map[string]interface{}{"type": "speak", "text": "speech"}},
		joinedChannelMSG(channelMember{id: 99, connectionType: "slave"}),
		pingMessage{},
		leftChannelMSG(channelMember{id: 99, connectionType: "slave"}),
		pingMessage{},
	}
	if cap(member.events) != len(events) {
		t.Fatalf("configured capacity = %d", cap(member.events))
	}
	for _, event := range events {
		member.enqueue(event)
	}
	readBackpressureResponse(t, dec, "ping")
	for i, kind := range []string{"key", "key", "set_clipboard_text", "speak", "client_joined", "ping", "client_left", "ping"} {
		response := readBackpressureResponse(t, dec, kind)
		switch i {
		case 0, 1:
			if response["pressed"] != (i == 0) {
				t.Fatalf("input state = %#v", response)
			}
		case 2, 3:
			want := map[int]string{2: "clipboard", 3: "speech"}[i]
			if response["text"] != want {
				t.Fatalf("text = %#v", response)
			}
		case 4, 6:
			if response["client"].(map[string]interface{})["id"] != float64(99) {
				t.Fatalf("notification = %#v", response)
			}
		}
	}
	select {
	case <-hook.done:
		t.Fatalf("normal burst disconnected client: %s", hook.reason)
	default:
	}
}

func TestBackpressureOverflowDisconnectsEveryEventType(t *testing.T) {
	for _, event := range []Message{pingMessage{}, channelMessage{}, joinedChannelMSG{}, leftChannelMSG{}} {
		t.Run(event.Name(), func(t *testing.T) {
			conn, peer := net.Pipe()
			defer conn.Close()
			defer peer.Close()
			peer.SetReadDeadline(time.Now().Add(5 * time.Second))
			c := &client{conn: conn, events: make(chan Message, 1)}
			c.events <- pingMessage{}
			member := channelMember{events: c.events, stop: c.stop}
			member.enqueue(event)
			if !c.isStopped() || c.stopReason != "Client event queue overflow" {
				t.Fatal("overflow did not terminate connection")
			}
			if len(c.events) != 1 {
				t.Fatal("overflow replaced a queued event")
			}
		})
	}
}

func TestBackpressureConcurrentDispatchDeliveryDisconnect(t *testing.T) {
	srv := backpressureServer()
	firstConn, firstPeer, firstDec, first := joinBackpressurePeer(t, srv, 7, "test")
	_, secondPeer, _, _ := joinBackpressurePeer(t, srv, 8, "test")
	readBackpressureResponse(t, firstDec, "client_joined")
	awaitLivenessSignal(t, firstConn.writes)
	_, thirdPeer, _, _ := joinBackpressurePeer(t, srv, 9, "independent")
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, work := range []func(){
		func() {
			for i := 0; i < 100; i++ {
				srv.dispatchPings()
			}
		},
		func() {
			for i := 0; i < 100; i++ {
				first.enqueue(channelMessage{origin: 99, msg: map[string]interface{}{"type": "speak"}})
			}
		},
		func() {
			for i := 0; i < 100; i++ {
				first.stop("concurrent stop")
			}
		},
		func() { firstPeer.Close(); secondPeer.Close(); thirdPeer.Close() },
	} {
		wg.Add(1)
		go func(work func()) { defer wg.Done(); <-start; work() }(work)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	close(start)
	awaitLivenessSignal(t, done)
}

func TestBackpressureChannelSubmissionCycle(t *testing.T) {
	reg := newLifecycleRegistry()
	blocked := joinLifecycleClient(t, reg, "test", 7)
	healthy := joinLifecycleClient(t, reg, "test", 8)
	receiveLifecycleEvent(t, blocked.events)
	for i := 0; i < cap(blocked.events); i++ {
		blocked.events <- pingMessage{}
	}
	// Delivery to blocked has a full queue. Its handler also submits an inbound
	// message synchronously. A socket deadline cannot break this channel cycle.
	sendBackpressureMessage(t, blocked.channel, 0)
	submitted := make(chan struct{})
	go func() {
		handleClientChannelMessage(blocked, &channelMessage{origin: blocked.id, msg: map[string]interface{}{"type": "key"}})
		close(submitted)
	}()
	awaitLivenessSignal(t, submitted)
	for _, kind := range []string{"speak", "key"} {
		msg := receiveLifecycleEvent(t, healthy.events).(channelMessage)
		if msg.msg["type"] != kind {
			t.Fatalf("delivery = %#v, want %s", msg, kind)
		}
	}
	if !blocked.isStopped() {
		t.Fatal("full queue did not stop blocked handler's client")
	}
	blocked.leaveChannel()
	if msg := receiveLifecycleEvent(t, healthy.events); msg.Name() != "left_channel" {
		t.Fatalf("notification = %#v", msg)
	}
	healthy.leaveChannel()
	assertLifecycleRegistryEmpty(t, reg)
}

type deadlineErrorConn struct{ net.Conn }

func (c deadlineErrorConn) SetWriteDeadline(time.Time) error {
	return errors.New("deadline unavailable")
}

func TestBackpressureDeadlineConfigurationAndFailure(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Second, time.Minute} {
		t.Run(timeout.String(), func(t *testing.T) {
			conn, peer := net.Pipe()
			defer conn.Close()
			defer peer.Close()
			observed := &backpressureConn{Conn: conn, writes: make(chan struct{}, 2)}
			peer.SetReadDeadline(time.Now().Add(5 * time.Second))
			c := &client{conn: observed, encoder: json.NewEncoder(observed), log: backpressureServer().Log, writeTimeout: timeout}
			want := timeout
			if want <= 0 {
				want = defaultWriteTimeout
			}
			for i := 0; i < 2; i++ {
				before := time.Now()
				done := make(chan struct{})
				go func() { c.send(GenericClientResponse{Type: "ping"}); close(done) }()
				readBackpressureResponse(t, json.NewDecoder(peer), "ping")
				awaitLivenessSignal(t, done)
				deadline := observed.deadlines[2*i]
				if deadline.Before(before.Add(want)) || deadline.After(time.Now().Add(want)) {
					t.Fatalf("deadline = %v, timeout = %v", deadline, want)
				}
				if !observed.deadlines[2*i+1].IsZero() {
					t.Fatal("successful send retained its write deadline while idle")
				}
				// The next send must replace an expired deadline.
				conn.SetWriteDeadline(time.Now().Add(-time.Second))
			}
		})
	}
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	c := &client{conn: deadlineErrorConn{conn}, encoder: json.NewEncoder(conn), log: backpressureServer().Log}
	done := make(chan struct{})
	go func() { c.send(GenericClientResponse{Type: "ping"}); close(done) }()
	awaitLivenessSignal(t, done)
	if !c.isStopped() {
		t.Fatal("deadline failure left an unbounded writer active")
	}
}

func newBackpressureTLS(t *testing.T) (*tls.Conn, *tls.Conn, *backpressureConn) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"test"}}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	conn, peer := net.Pipe()
	observed := &backpressureConn{Conn: conn, writes: make(chan struct{}, 16)}
	serverTLS := tls.Server(observed, &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}})
	peerTLS := tls.Client(peer, &tls.Config{RootCAs: roots, ServerName: "test"})
	t.Cleanup(func() { conn.Close(); peer.Close() })
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	peer.SetDeadline(time.Now().Add(5 * time.Second))
	handshaken := make(chan error, 1)
	go func() { handshaken <- serverTLS.Handshake() }()
	if err := peerTLS.Handshake(); err != nil {
		t.Fatal(err)
	}
	if err := <-handshaken; err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Time{})
	peer.SetDeadline(time.Time{})
	for len(observed.writes) > 0 {
		<-observed.writes
	}
	return serverTLS, peerTLS, observed
}

func TestBackpressureTLSWriteTimeoutAndOverflow(t *testing.T) {
	for _, overflow := range []bool{false, true} {
		t.Run(map[bool]string{false: "write_timeout", true: "overflow_without_active_write"}[overflow], func(t *testing.T) {
			conn, _, raw := newBackpressureTLS(t)
			c := &client{conn: conn, encoder: json.NewEncoder(conn), log: backpressureServer().Log, events: make(chan Message, 1)}
			if overflow {
				// No active TLS Write: Close would ordinarily try close_notify,
				// and the peer isn't reading. Forced stop must close transport first.
				c.events <- pingMessage{}
				done := make(chan struct{})
				go func() { (channelMember{events: c.events, stop: c.stop}).enqueue(pingMessage{}); close(done) }()
				awaitLivenessSignal(t, done)
			} else {
				done := make(chan struct{})
				go func() { c.send(GenericClientResponse{Type: "ping"}); close(done) }()
				awaitLivenessSignal(t, raw.writes)
				raw.Conn.SetWriteDeadline(time.Now().Add(-time.Second))
				awaitLivenessSignal(t, done)
			}
			if !c.isStopped() {
				t.Fatal("TLS backpressure did not terminate connection")
			}
			// TLS Close can try an alert on the already closed transport. Further
			// protocol sends must not attempt any additional writes after stop.
			writesAfterStop := len(raw.writes)
			c.send(GenericClientResponse{Type: "ping"})
			if len(raw.writes) != writesAfterStop {
				t.Fatal("stopped TLS client attempted another write")
			}
		})
	}
}

func TestBackpressureConcurrentSendsAndStop(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	observed := &backpressureConn{Conn: conn, writes: make(chan struct{}, 32)}
	c := &client{conn: observed, encoder: json.NewEncoder(observed), log: backpressureServer().Log}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); c.send(GenericClientResponse{Type: "ping"}) }()
	awaitLivenessSignal(t, observed.writes)
	start := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; c.send(GenericClientResponse{Type: "ping"}) }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); <-start; c.stop("concurrent stop") }()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	close(start)
	awaitLivenessSignal(t, done)
	if !c.isStopped() {
		t.Fatal("concurrent stop did not terminate writer")
	}
}
