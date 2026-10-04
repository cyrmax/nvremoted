package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"
)

// A small configured budget reproduces the unbounded receive path without
// allocating a large payload. Keep the peer open, including for incomplete JSON.
func TestMessageLimitReceiveRegression(t *testing.T) {
	for _, payload := range []string{
		`{"type":"future","data":"` + strings.Repeat("x", 80) + `"}`,
		`{"type":"protocol_version","version":2,"extra":"` + strings.Repeat("x", 80) + `"}`,
		`{"type":"future","data":"` + strings.Repeat("x", 80),
	} {
		t.Run(payload[:20], func(t *testing.T) {
			srv := &Server{MaxMessageSize: 64}
			c, peer, _, finished := newReadTestClient(t, srv)
			go func() {
				for range c.recv {
					c.readNext <- struct{}{}
				}
			}()
			peer.SetWriteDeadline(time.Now().Add(time.Second))
			peer.Write([]byte(payload))
			select {
			case <-finished:
			case <-time.After(time.Second):
				peer.Close()
				awaitLivenessSignal(t, finished)
				t.Fatal("oversized message did not disconnect the receiver")
			}
		})
	}
}

// Baseline behavior that the bounded decoder must preserve: TCP reads are not
// frames, and historical nvremoted accepts multiline and adjacent JSON values.
func TestMessageLimitHistoricalStream(t *testing.T) {
	input := " \r\n\t" + `{"type":"future",` + "\n" + `"data":[{"text":"escaped \\\" } ["}]}` +
		`{"type":"protocol_version","version":2}` + " \t\r\n"
	c, peer, observed, finished := newReadTestClient(t, &Server{})
	readSignalsDone := make(chan struct{})
	defer close(readSignalsDone)
	go func() {
		for {
			select {
			case <-observed.reads:
			case <-readSignalsDone:
				return
			}
		}
	}()
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		for i := range input {
			if _, err := peer.Write([]byte{input[i]}); err != nil {
				return
			}
		}
		peer.Close()
	}()
	for i := 0; i < 2; i++ {
		select {
		case msg, ok := <-c.recv:
			if !ok || msg == nil {
				t.Fatal("lost a fragmented message")
			}
			c.readNext <- struct{}{}
		case <-time.After(time.Second):
			t.Fatal("fragmented message stalled")
		}
	}
	awaitLivenessSignal(t, finished)
	awaitLivenessSignal(t, writeDone)
}

func TestMessageLimitBoundariesAndFragments(t *testing.T) {
	values := []string{
		`{"type":"future","data":"` + strings.Repeat("x", 70) + `"}`,
		`{"type":"protocol_version","version":2,"extra":"` + strings.Repeat("x", 70) + `"}`,
		`{"type":"future","data":[{"a":[1,2,{"b":"text"}]}]}`,
		"{\n\t\"type\":\"future\",\r\n\"text\":\"雪😀\\\"\\\\\"\n}",
		`"` + strings.Repeat("x", 70) + `"`,
		`[[[[{"a":[1,2,3]}]]]]`,
		`true`, `null`, `123456789`, `-12.5e2`,
	}
	for _, value := range values {
		for _, delta := range []int{-1, 0, 1} {
			for _, fragments := range []string{"whole", "one-byte", "halves"} {
				t.Run(fragments+"/"+value+"/"+string(rune('1'+delta)), func(t *testing.T) {
					input := io.Reader(strings.NewReader(" \r\n\t" + value + " \t\r\n" + value + "\n" + value))
					if fragments == "one-byte" {
						input = iotest.OneByteReader(input)
					} else if fragments == "halves" {
						input = iotest.HalfReader(input)
					}
					r := newMessageReader(input, len(value)+delta)
					for i := 0; i < 3; i++ {
						raw, err := r.read()
						if delta < 0 {
							if !errors.Is(err, errMessageTooLarge) || raw != nil {
								t.Fatalf("oversize: raw=%q error=%v", raw, err)
							}
							return
						}
						if err != nil || string(raw) != value {
							t.Fatalf("message %d: raw=%q error=%v", i, raw, err)
						}
					}
					if _, err := r.read(); err != io.EOF {
						t.Fatalf("end of stream: %v", err)
					}
				})
			}
		}
	}
}

func TestMessageLimitEOFAndMalformed(t *testing.T) {
	for _, input := range []string{`{"data":"unfinished`, `[[[1`, `"text`, `{"a":`, `tru`} {
		for _, limit := range []int{len(input), len(input) + 1} {
			if _, err := newMessageReader(strings.NewReader(input), limit).read(); err != io.ErrUnexpectedEOF {
				t.Errorf("input=%q limit=%d: %v", input, limit, err)
			}
		}
		if _, err := newMessageReader(strings.NewReader(input), len(input)-1).read(); err != errMessageTooLarge {
			t.Errorf("unfinished oversize input=%q: %v", input, err)
		}
	}
	for _, input := range []string{":invalid" + strings.Repeat("x", 200), `{"a":]}` + strings.Repeat("x", 200)} {
		_, err := newMessageReader(strings.NewReader(input), 20).read()
		var syntax *json.SyntaxError
		if !errors.As(err, &syntax) {
			t.Errorf("malformed within budget became oversize: %v", err)
		}
	}
	for _, input := range []string{"", " \t\r\n"} {
		if _, err := newMessageReader(strings.NewReader(input), 1).read(); err != io.EOF {
			t.Errorf("empty stream: %v", err)
		}
	}
}

func TestMessageLimitReadAheadAndRepeatedValues(t *testing.T) {
	small := `{"type":"future"}`
	large := `{"type":"future","data":"` + strings.Repeat("x", 9000) + `"}`
	input := strings.Repeat(small+"\n", 500) + large + small
	r := newMessageReader(strings.NewReader(input), len(large))
	for i := 0; i < 502; i++ {
		want := small
		if i == 500 {
			want = large
		}
		raw, err := r.read()
		if err != nil || string(raw) != want {
			t.Fatalf("message %d: size=%d error=%v", i, len(raw), err)
		}
		if len(r.pending) > 4096 {
			t.Fatalf("retained read-ahead = %d", len(r.pending))
		}
	}
	// A following oversized value must not poison the allowed first value.
	r = newMessageReader(strings.NewReader(small+large), len(small))
	if raw, err := r.read(); err != nil || string(raw) != small {
		t.Fatalf("small before oversize: %q %v", raw, err)
	}
	if _, err := r.read(); err != errMessageTooLarge {
		t.Fatalf("oversized second value: %v", err)
	}
}

type messageCountingReader struct {
	io.Reader
	bytes int
}

func (r *messageCountingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.bytes += n
	return n, err
}

func TestMessageLimitStopsBeforeReadingWholeValue(t *testing.T) {
	input := &messageCountingReader{Reader: strings.NewReader(`{"type":"future","data":"` + strings.Repeat("x", 20000))}
	if _, err := newMessageReader(input, 64).read(); err != errMessageTooLarge {
		t.Fatal(err)
	}
	if input.bytes > 4096+65 {
		t.Fatalf("consumed %d bytes for limit 64", input.bytes)
	}
	// Whitespace can be arbitrarily long without growing the decoder buffer.
	r := newMessageReader(strings.NewReader(strings.Repeat(" \t\r\n", 10000)+`{}`), 2)
	if raw, err := r.read(); err != nil || string(raw) != `{}` {
		t.Fatalf("whitespace: %q %v", raw, err)
	}
}

func TestMessageLimitConfiguration(t *testing.T) {
	for _, limit := range []int{0, -1, -100} {
		r := newMessageReader(strings.NewReader(`{}`), limit)
		if r.limit != defaultMaxMessageSize || r.limit <= 0 {
			t.Fatalf("nonpositive limit %d: %d", limit, r.limit)
		}
	}
	maxInt := int(^uint(0) >> 1)
	if raw, err := newMessageReader(strings.NewReader(`{}`), maxInt).read(); err != nil || string(raw) != `{}` {
		t.Fatalf("maximum int limit overflow: %q %v", raw, err)
	}
}

func TestMessageLimitOffendersLifecycle(t *testing.T) {
	for _, concurrent := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "concurrent"}[concurrent], func(t *testing.T) {
			srv := backpressureServer()
			srv.MaxMessageSize = 128
			_, sender, senderDec, _ := joinBackpressurePeer(t, srv, 1, "healthy")
			_, _, receiver, _ := joinBackpressurePeer(t, srv, 2, "healthy")
			// Drain notifications on the sender so its writes do not wait on events.
			readBackpressureResponse(t, senderDec, "client_joined")
			payloads := []string{`{"type":"future","text":"` + strings.Repeat("x", 140) + `"}`}
			if concurrent {
				payloads = append(payloads,
					`{"type":"protocol_version","version":2,"extra":"`+strings.Repeat("x", 140)+`"}`,
					`{"type":"future","text":"`+strings.Repeat("x", 140))
			}
			peers := make([]net.Conn, len(payloads))
			hooks := make([]*backpressureDisconnectHook, len(payloads))
			clients := make([]*client, len(payloads))
			for i := range payloads {
				id := uint64(i + 10)
				hooks[i] = &backpressureDisconnectHook{id: id, done: make(chan struct{})}
				srv.Log.AddHook(hooks[i])
				_, peers[i], _, _ = joinBackpressurePeer(t, srv, id, "healthy")
				readBackpressureResponse(t, senderDec, "client_joined")
				readBackpressureResponse(t, receiver, "client_joined")
				srv.lifecycleMu.Lock()
				for c := range srv.active {
					if c.id == id {
						clients[i] = c
					}
				}
				srv.lifecycleMu.Unlock()
			}
			var writes sync.WaitGroup
			for i, peer := range peers {
				writes.Add(1)
				go func(peer net.Conn, payload string) {
					defer writes.Done()
					// No response reads: oversize must stop an unread peer immediately.
					peer.Write([]byte(payload))
				}(peer, payloads[i])
			}
			for range peers {
				readBackpressureResponse(t, senderDec, "client_left")
				readBackpressureResponse(t, receiver, "client_left")
			}
			writes.Wait()
			for i, hook := range hooks {
				awaitLivenessSignal(t, hook.done)
				if hook.reason != "Incoming message too large" {
					t.Fatalf("offender %d reason = %q", i, hook.reason)
				}
				clients[i].stop("later reason")
				clients[i].stopMTX.RLock()
				reason := clients[i].stopReason
				clients[i].stopMTX.RUnlock()
				if reason != hook.reason {
					t.Fatal("first stop reason replaced")
				}
			}
			// An authoritative new join also proves no stale channel membership.
			_, probe, _, _ := joinBackpressurePeer(t, srv, 30, "healthy")
			readBackpressureResponse(t, senderDec, "client_joined")
			readBackpressureResponse(t, receiver, "client_joined")
			srv.registry.lock.RLock()
			remaining := len(srv.registry.clients)
			srv.registry.lock.RUnlock()
			if remaining != 3 {
				t.Fatalf("registered clients after oversize = %d", remaining)
			}
			probe.Close()
			readBackpressureResponse(t, senderDec, "client_left")
			readBackpressureResponse(t, receiver, "client_left")
			// Healthy clients continue relaying arbitrary payloads, including
			// characters whose outbound HTML escaping expands the encoding.
			payload := []byte(`{"type":"future","text":"<>&雪"}`)
			if _, err := sender.Write(payload); err != nil {
				t.Fatal(err)
			}
			response := readBackpressureResponse(t, receiver, "future")
			if response["text"] != "<>&雪" || response["origin"] != float64(1) {
				t.Fatalf("relay after offender stop: %#v", response)
			}
			sender.Close()
			// Closing the other healthy peer unblocks any leave write.
			srv.lifecycleMu.Lock()
			var live []*client
			for c := range srv.active {
				live = append(live, c)
			}
			srv.lifecycleMu.Unlock()
			for _, c := range live {
				c.stop("test complete")
			}
			done := make(chan struct{})
			go func() { srv.clients.Wait(); close(done) }()
			awaitLivenessSignal(t, done)
			srv.registry.lock.RLock()
			empty := len(srv.registry.clients) == 0 && len(srv.registry.channels) == 0
			srv.registry.lock.RUnlock()
			if !empty {
				t.Fatal("registry/channel cleanup incomplete")
			}
		})
	}
}

func TestMessageLimitLastMemberAndUnjoinedCleanup(t *testing.T) {
	for _, joined := range []bool{false, true} {
		t.Run(map[bool]string{false: "unjoined", true: "last-member"}[joined], func(t *testing.T) {
			srv := backpressureServer()
			srv.MaxMessageSize = 128
			hook := &backpressureDisconnectHook{id: 50, done: make(chan struct{})}
			srv.Log.AddHook(hook)
			var peer net.Conn
			if joined {
				_, peer, _, _ = joinBackpressurePeer(t, srv, 50, "solo")
			} else {
				conn, p := net.Pipe()
				peer = p
				srv.serveClient(conn, 50, "test")
				t.Cleanup(func() { peer.Close(); conn.Close() })
			}
			peer.SetWriteDeadline(time.Now().Add(time.Second))
			peer.Write([]byte(`{"type":"join","channel":"` + strings.Repeat("x", 150)))
			awaitLivenessSignal(t, hook.done)
			if hook.reason != "Incoming message too large" {
				t.Fatalf("reason = %q", hook.reason)
			}
			done := make(chan struct{})
			go func() { srv.clients.Wait(); close(done) }()
			awaitLivenessSignal(t, done)
			srv.registry.lock.RLock()
			empty := len(srv.registry.clients) == 0 && len(srv.registry.channels) == 0
			srv.registry.lock.RUnlock()
			srv.lifecycleMu.Lock()
			active := len(srv.active)
			srv.lifecycleMu.Unlock()
			if !empty || active != 0 {
				t.Fatal("oversized control left membership/session state")
			}
		})
	}
}

func TestMessageLimitUnfinishedWithinBudget(t *testing.T) {
	c, peer, observed, finished := newReadTestClient(t, &Server{MaxMessageSize: 128})
	awaitLivenessSignal(t, observed.reads)
	if _, err := peer.Write([]byte(`{"type":"future","text":"partial`)); err != nil {
		t.Fatal(err)
	}
	// Reader is requesting more bytes: an unfinished in-budget value has
	// neither been delivered nor disconnected. No assembly timer is installed.
	awaitLivenessSignal(t, observed.reads)
	select {
	case msg := <-c.recv:
		t.Fatalf("partial value delivered: %#v", msg)
	default:
	}
	if c.isStopped() {
		t.Fatal("in-budget partial value stopped the client")
	}
	assertNoInactivityDeadline(t, observed)
	if _, err := peer.Write([]byte(`"}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-c.recv:
		if msg == nil {
			t.Fatal("completed value lost")
		}
		c.readNext <- struct{}{}
	case <-time.After(time.Second):
		t.Fatal("completion stalled")
	}
	c.stop("test complete")
	awaitLivenessSignal(t, finished)
}

func TestMessageLimitUnmarshalArbitraryAndControl(t *testing.T) {
	for _, input := range []string{`{"type":"future","x":[1,{"y":true}]}`, `{"type":"protocol_version","version":2}`} {
		raw, err := newMessageReader(bytes.NewBufferString(input), len(input)).read()
		if err != nil {
			t.Fatal(err)
		}
		if msg, err := unmarshalClientMessage(42, raw); err != nil || msg == nil {
			t.Fatalf("message=%#v error=%v", msg, err)
		}
	}
}
