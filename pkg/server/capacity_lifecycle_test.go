package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

type capacityCleanupHook struct {
	entered, release chan struct{}
	once             sync.Once
}

func (h *capacityCleanupHook) Levels() []logrus.Level { return logrus.AllLevels }
func (h *capacityCleanupHook) Fire(e *logrus.Entry) error {
	if e.Message == "Client disconnected" {
		h.once.Do(func() { close(h.entered); <-h.release })
	}
	return nil
}

func TestCapacityPhysicalPermitHeldThroughFullCleanup(t *testing.T) {
	srv := capacityWireServer(t)
	hook := &capacityCleanupHook{entered: make(chan struct{}), release: make(chan struct{})}
	srv.Log.AddHook(hook)
	p := newCapacityPeer(t, srv, 1)
	p.Close()
	awaitLivenessSignal(t, hook.entered)
	stats := srv.capacity.stats()
	if stats.PhysicalActive != 1 || stats.PendingCleanup != 1 {
		t.Fatal(stats)
	}
	close(hook.release)
	srv.clients.Wait()
	if stats := srv.capacity.stats(); stats.PhysicalActive != 0 {
		t.Fatal(stats)
	}
}

func TestCapacityTLSHandshakeFloodBound(t *testing.T) {
	srv := capacityWireServer(t)
	config, _ := handshakeConfigs(t)
	for i := 1; i <= 2; i++ {
		conn, _ := handshakePipe(t)
		srv.serveClient(tls.Server(conn, config), uint64(i), "test")
		awaitLivenessSignal(t, conn.reads)
	}
	conn, peer := net.Pipe()
	srv.serveClient(tls.Server(conn, config), 3, "test")
	peer.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err := peer.Read(b[:]); err == nil {
		t.Fatal("TLS limit bypass")
	}
	peer.Close()
	if stats := srv.capacity.stats(); stats.PhysicalActive != 2 || stats.PendingTLS != 2 {
		t.Fatal(stats)
	}
}

func TestCapacityRejectedBackpressureAndWriteFailure(t *testing.T) {
	for _, mode := range []string{"deadline", "write failure"} {
		t.Run(mode, func(t *testing.T) {
			srv := capacityWireServer(t)
			for i := 1; i <= 4; i++ {
				p := newCapacityPeer(t, srv, uint64(i))
				p.send(t, `{"type":"join","channel":"`+string(rune('a'+i))+`","connection_type":"master"}`)
				p.read(t)
			}
			conn, peer := net.Pipe()
			observed := &backpressureConn{Conn: conn, writes: make(chan struct{}, 4)}
			hook := &backpressureDisconnectHook{id: 5, done: make(chan struct{})}
			srv.Log.AddHook(hook)
			if mode == "write failure" {
				observed.expireWrites.Store(true)
			}
			srv.serveClient(observed, 5, "test")
			defer peer.Close()
			peer.SetDeadline(time.Now().Add(2 * time.Second))
			start := time.Now()
			if err := json.NewEncoder(peer).Encode(ClientJoinMessage{GenericClientMessage: GenericClientMessage{Type: "join"}, Channel: "new", ConnectionType: "master"}); err != nil {
				t.Fatal(err)
			}
			awaitLivenessSignal(t, observed.writes)
			awaitLivenessSignal(t, hook.done)
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("rejection exceeds absolute window: %v", elapsed)
			}
			if hook.reason != "Send error" {
				t.Fatal(hook.reason)
			}
			if stats := srv.registry.Stats(); stats.NumClients != 4 {
				t.Fatal(stats)
			}
		})
	}
}

func TestCapacityStatsStartupWithoutVersion(t *testing.T) {
	srv := capacityWireServer(t, func(srv *Server) { srv.StatsPassword = "operator" })
	p := newCapacityPeer(t, srv, 1)
	p.send(t, `{"type":"stat","password":"operator"}`)
	m := p.read(t)
	if m["type"] != "stats" || m["stats"].(map[string]interface{})["admission"] == nil {
		t.Fatal(m)
	}
	var response map[string]interface{}
	if err := p.decoder.Decode(&response); err == nil {
		t.Fatal("service did not close")
	}
}

func TestCapacityConfiguredOverlapDeadlineIncludesJoinWrite(t *testing.T) {
	srv := capacityWireServer(t, func(srv *Server) {
		srv.Admission.ProtectedChannels = []string{channelDigest("critical")}
		srv.Admission.OverlapTimeout = 60 * time.Millisecond
	})
	first := newCapacityPeer(t, srv, 1)
	first.send(t, `{"type":"join","channel":"critical","connection_type":"slave"}`)
	first.read(t)
	master := newCapacityPeer(t, srv, 2)
	master.send(t, `{"type":"join","channel":"critical","connection_type":"master"}`)
	master.read(t)
	first.read(t)
	for i := 3; i <= 4; i++ {
		p := newCapacityPeer(t, srv, uint64(i))
		p.send(t, `{"type":"join","channel":"`+string(rune('a'+i))+`","connection_type":"master"}`)
		p.read(t)
	}
	extra := newCapacityPeer(t, srv, 5)
	extra.send(t, `{"type":"join","channel":"critical","connection_type":"observer"}`)
	extra.read(t)
	first.read(t)
	master.read(t)
	conn, peer := net.Pipe()
	observed := &backpressureConn{Conn: conn, writes: make(chan struct{}, 4)}
	hook := &backpressureDisconnectHook{id: 6, done: make(chan struct{})}
	srv.Log.AddHook(hook)
	srv.serveClient(observed, 6, "test")
	defer peer.Close()
	peer.SetDeadline(time.Now().Add(time.Second))
	start := time.Now()
	if _, err := peer.Write([]byte(`{"type":"join","channel":"critical","connection_type":"slave"}`)); err != nil {
		t.Fatal(err)
	}
	awaitLivenessSignal(t, observed.writes) // channel_joined, not read by peer
	awaitLivenessSignal(t, hook.done)
	if time.Since(start) > 500*time.Millisecond || hook.reason != "Configured recovery overlap expired" {
		t.Fatalf("deadline slipped: %v %s", time.Since(start), hook.reason)
	}
	first.read(t)
	first.read(t) // actual overlap join and actual leave
}

func TestCapacityServeShutdownDrainsRejectionTLSAndReservations(t *testing.T) {
	srv := backpressureServer()
	srv.Admission = AdmissionConfig{HardLimit: 8, PendingLimit: 4, TLSLimit: 2, StartupLimit: 4, AdmittedLimit: 4}
	l := &handshakeListener{incoming: make(chan net.Conn), stop: make(chan struct{})}
	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(l) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	accept := func(conn net.Conn, peer net.Conn) *capacityPeer {
		t.Helper()
		peer.SetDeadline(time.Now().Add(2 * time.Second))
		t.Cleanup(func() { peer.Close() })
		select {
		case l.incoming <- conn:
		case <-time.After(2 * time.Second):
			t.Fatal("accept blocked")
		}
		return &capacityPeer{peer, json.NewDecoder(peer)}
	}
	conn, peer := net.Pipe()
	first := accept(conn, peer)
	first.send(t, `{"type":"join","channel":"recovery","connection_type":"master"}`)
	first.read(t)
	conn, peer = net.Pipe()
	healthy := accept(conn, peer)
	healthy.send(t, `{"type":"join","channel":"healthy","connection_type":"slave"}`)
	healthy.read(t)
	first.Close()
	for {
		_, changed := srv.capacity.acceptReady()
		if srv.capacity.stats().GenericReservations == 1 {
			break
		}
		select {
		case <-changed:
		case <-time.After(2 * time.Second):
			t.Fatal("no offline reservation")
		}
	}
	conn, peer = net.Pipe()
	observed := &backpressureConn{Conn: conn, writes: make(chan struct{}, 4)}
	rejected := accept(observed, peer)
	rejected.send(t, `{"type":"join","channel":"overload","connection_type":"master"}`)
	awaitLivenessSignal(t, observed.writes) // reject speech blocked on unread peer
	config, _ := handshakeConfigs(t)
	tlsConn, tlsPeer := handshakePipe(t)
	accept(tls.Server(tlsConn, config), tlsPeer)
	awaitLivenessSignal(t, tlsConn.reads)
	if stats := srv.capacity.stats(); stats.PendingRejection != 1 || stats.PendingTLS != 1 || stats.GenericReservations != 1 {
		t.Fatal(stats)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Serve cleanup hangs")
	}
	if stats := srv.capacity.stats(); stats.PhysicalActive != 0 || stats.GenericReservations != 0 || stats.AdmittedConnections != 0 {
		t.Fatal(stats)
	}
	assertLifecycleRegistryEmpty(t, &srv.registry)
}
