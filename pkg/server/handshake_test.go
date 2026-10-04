package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// Observe transport operations without imposing any server-side deadlines.
type handshakeConn struct {
	net.Conn
	reads, writes chan struct{}
	writeCleared  chan struct{}
	failClose     bool
	mu            sync.Mutex
	deadlines     []time.Time
}

func (c *handshakeConn) Close() error {
	err := c.Conn.Close()
	if c.failClose {
		return errors.New("test close error")
	}
	return err
}

func (c *handshakeConn) Read(b []byte) (int, error) {
	select {
	case c.reads <- struct{}{}:
	default:
	}
	return c.Conn.Read(b)
}

func (c *handshakeConn) Write(b []byte) (int, error) {
	select {
	case c.writes <- struct{}{}:
	default:
	}
	return c.Conn.Write(b)
}

func (c *handshakeConn) SetDeadline(d time.Time) error {
	c.mu.Lock()
	c.deadlines = append(c.deadlines, d)
	c.mu.Unlock()
	return c.Conn.SetDeadline(d)
}

func (c *handshakeConn) SetReadDeadline(d time.Time) error {
	c.mu.Lock()
	c.deadlines = append(c.deadlines, d)
	c.mu.Unlock()
	return c.Conn.SetReadDeadline(d)
}

func (c *handshakeConn) SetWriteDeadline(d time.Time) error {
	err := c.Conn.SetWriteDeadline(d)
	if err == nil && d.IsZero() {
		select {
		case c.writeCleared <- struct{}{}:
		default:
		}
	}
	return err
}

func (c *handshakeConn) assertNoReadDeadline(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, d := range c.deadlines {
		if !d.IsZero() {
			t.Fatalf("unexpected transport read deadline: %v", d)
		}
	}
}

type handshakeLog struct{ entries chan *logrus.Entry }

func (h *handshakeLog) Levels() []logrus.Level { return logrus.AllLevels }
func (h *handshakeLog) Fire(e *logrus.Entry) error {
	entry := e.Dup()
	entry.Level, entry.Message = e.Level, e.Message
	h.entries <- entry
	return nil
}

func handshakeServer(t *testing.T) (*Server, *handshakeLog) {
	t.Helper()
	srv := backpressureServer()
	srv.TLSHandshakeTimeout = 100 * time.Millisecond
	srv.Log.SetLevel(logrus.DebugLevel)
	h := &handshakeLog{entries: make(chan *logrus.Entry, 256)}
	srv.Log.AddHook(h)
	return srv, h
}

func (h *handshakeLog) disconnected(t *testing.T, id uint64, reason string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		select {
		case e := <-h.entries:
			if e.Level <= logrus.WarnLevel {
				t.Fatalf("unexpected warning: %s: %v", e.Message, e.Data)
			}
			if e.Message == "Client disconnected" && e.Data["id"] == id {
				if reason != "" && e.Data["reason"] != reason {
					t.Fatalf("disconnect reason: %v", e.Data)
				}
				return
			}
		case <-ctx.Done():
			t.Fatal("client cleanup did not finish")
		}
	}
}

func handshakeConfigs(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"test"}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}, &tls.Config{RootCAs: roots, ServerName: "test"}
}

func handshakePipe(t *testing.T) (*handshakeConn, net.Conn) {
	t.Helper()
	conn, peer := net.Pipe()
	c := &handshakeConn{Conn: conn, reads: make(chan struct{}, 64), writes: make(chan struct{}, 64), writeCleared: make(chan struct{}, 64)}
	t.Cleanup(func() { conn.Close(); peer.Close() })
	if err := peer.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return c, peer
}

func TestTLSHandshakeStalledPeers(t *testing.T) {
	for _, mode := range []string{"no hello", "partial hello", "MOTD"} {
		t.Run(mode, func(t *testing.T) {
			srv, log := handshakeServer(t)
			if mode == "MOTD" {
				srv.MOTD = "hello"
				srv.WriteTimeout = time.Nanosecond
			}
			config, _ := handshakeConfigs(t)
			conn, peer := handshakePipe(t)
			srv.serveClient(tls.Server(conn, config), 7, "test")
			awaitLivenessSignal(t, conn.reads)
			if mode == "partial hello" {
				if _, err := peer.Write([]byte{22}); err != nil {
					t.Fatal(err)
				}
				awaitLivenessSignal(t, conn.reads)
			}
			log.disconnected(t, 7, "TLS handshake timeout")
			if n, err := peer.Read(make([]byte, 1)); n != 0 || err == nil {
				t.Fatalf("peer not closed: %d, %v", n, err)
			}
			assertLifecycleRegistryEmpty(t, &srv.registry)
		})
	}
}

func TestTLSHandshakeFailureIsTerminal(t *testing.T) {
	srv, log := handshakeServer(t)
	srv.MOTD = "must not be sent"
	config, _ := handshakeConfigs(t)
	conn, peer := handshakePipe(t)
	conn.failClose = true // A later close error must not replace the TLS reason.
	serverTLS := tls.Server(conn, config)
	srv.serveClient(serverTLS, 7, "test")
	if _, err := io.WriteString(peer, "GET /"); err != nil {
		t.Fatal(err)
	}
	log.disconnected(t, 7, "TLS handshake error")
	assertLifecycleRegistryEmpty(t, &srv.registry)
	if _, err := serverTLS.Write([]byte("retry")); err == nil {
		t.Fatal("failed TLS connection was reusable")
	}
	select {
	case <-conn.writes:
		t.Fatal("protocol write attempted after handshake failure")
	default:
	}
}

type handshakeGatedReader struct {
	net.Conn
	reading chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *handshakeGatedReader) Read(b []byte) (int, error) {
	c.once.Do(func() { close(c.reading) })
	<-c.release
	return c.Conn.Read(b)
}

func TestTLSHandshakeBlockedWriteTimeout(t *testing.T) {
	srv, log := handshakeServer(t)
	config, clientConfig := handshakeConfigs(t)
	conn, peer := handshakePipe(t)
	clientTransport := &handshakeGatedReader{Conn: peer, reading: make(chan struct{}), release: make(chan struct{})}
	clientTLS := tls.Client(clientTransport, clientConfig)
	finished := make(chan struct{})
	go func() { defer close(finished); _ = clientTLS.Handshake() }()
	t.Cleanup(func() { peer.Close(); close(clientTransport.release); awaitLivenessSignal(t, finished) })
	serverTLS := tls.Server(conn, config)
	srv.serveClient(serverTLS, 7, "test")
	awaitLivenessSignal(t, clientTransport.reading) // ClientHello was sent.
	awaitLivenessSignal(t, conn.writes)             // Server is blocked sending its reply.
	log.disconnected(t, 7, "TLS handshake timeout")
	assertLifecycleRegistryEmpty(t, &srv.registry)
	if _, err := serverTLS.Write([]byte("retry")); err == nil {
		t.Fatal("timed out TLS connection was reusable")
	}
}

// This finite listener exits its owning goroutine on Close because the production
// accept loop currently retries all Accept errors, including closed listeners.
// No shutdown redesign is needed to test the actual accept loop without leaks.
type handshakeListener struct {
	incoming chan net.Conn
	stop     chan struct{}
}

func (l *handshakeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.incoming:
		return c, nil
	case <-l.stop:
		runtime.Goexit()
		return nil, net.ErrClosed
	}
}
func (l *handshakeListener) Close() error   { close(l.stop); return nil }
func (l *handshakeListener) Addr() net.Addr { return &net.TCPAddr{} }

func TestTLSHandshakeAcceptsRemainIndependent(t *testing.T) {
	srv, log := handshakeServer(t)
	// A long timeout makes this test fail if a stalled handshake is performed
	// inside the serial accept loop, rather than relying on its eventual timeout.
	srv.TLSHandshakeTimeout = time.Minute
	srv.MOTD = "hello"
	config, clientConfig := handshakeConfigs(t)
	l := &handshakeListener{incoming: make(chan net.Conn), stop: make(chan struct{})}
	done := make(chan struct{})
	go func() { defer close(done); srv.acceptClients(tls.NewListener(l, config)) }()
	t.Cleanup(func() { l.Close(); awaitLivenessSignal(t, done) })
	stalled, stalledPeer := handshakePipe(t)
	l.incoming <- stalled
	awaitLivenessSignal(t, stalled.reads)
	conn, peer := handshakePipe(t)
	select {
	case l.incoming <- conn:
	case <-time.After(2 * time.Second):
		t.Fatal("accept blocked by handshake")
	}
	clientTLS := tls.Client(peer, clientConfig)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := clientTLS.HandshakeContext(ctx); err != nil {
		t.Fatal(err)
	}
	var response ClientMOTDResponse
	if err := json.NewDecoder(clientTLS).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.MOTD != "hello" {
		t.Fatalf("response: %#v", response)
	}
	awaitLivenessSignal(t, conn.writeCleared)
	stalledPeer.Close()
	log.disconnected(t, 0, "TLS handshake error")
	peer.Close()
	log.disconnected(t, 1, "")
	assertLifecycleRegistryEmpty(t, &srv.registry)
}

func TestTLSHandshakeSuccessAllowsIdle(t *testing.T) {
	for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
		t.Run(tls.VersionName(version), func(t *testing.T) {
			srv, log := handshakeServer(t)
			config, clientConfig := handshakeConfigs(t)
			config.MinVersion = version
			config.MaxVersion = version
			conn, peer := handshakePipe(t)
			srv.serveClient(tls.Server(conn, config), 7, "test")
			clientTLS := tls.Client(peer, clientConfig)
			if err := clientTLS.Handshake(); err != nil {
				t.Fatal(err)
			}
			// Crossing the old handshake expiry is part of the behavior under
			// test, not synchronization. Then exercise reads and writes with join.
			timer := time.NewTimer(2 * srv.TLSHandshakeTimeout)
			defer timer.Stop()
			<-timer.C
			conn.assertNoReadDeadline(t)
			if _, err := io.WriteString(clientTLS, "{\"type\":\"join\",\"channel\":\"test\",\"connection_type\":\"master\"}\n"); err != nil {
				t.Fatal(err)
			}
			var response ClientChannelJoinedResponse
			if err := json.NewDecoder(clientTLS).Decode(&response); err != nil {
				t.Fatal(err)
			}
			if response.Type != "channel_joined" {
				t.Fatalf("response: %#v", response)
			}
			awaitLivenessSignal(t, conn.writeCleared)
			peer.Close()
			log.disconnected(t, 7, "")
			assertLifecycleRegistryEmpty(t, &srv.registry)
		})
	}
}

func TestTLSHandshakeTimeoutDoesNotApplyToPlainTCP(t *testing.T) {
	srv, log := handshakeServer(t)
	srv.TLSHandshakeTimeout = time.Nanosecond
	conn, peer := handshakePipe(t)
	srv.serveClient(conn, 7, "test")
	awaitLivenessSignal(t, conn.reads)
	conn.assertNoReadDeadline(t)
	if _, err := io.WriteString(peer, "{\"type\":\"join\",\"channel\":\"test\",\"connection_type\":\"master\"}\n"); err != nil {
		t.Fatal(err)
	}
	var response ClientChannelJoinedResponse
	if err := json.NewDecoder(peer).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Type != "channel_joined" {
		t.Fatalf("response: %#v", response)
	}
	awaitLivenessSignal(t, conn.writeCleared)
	peer.Close()
	log.disconnected(t, 7, "")
	assertLifecycleRegistryEmpty(t, &srv.registry)
}

func TestTLSHandshakeConcurrentTimeoutCleanup(t *testing.T) {
	srv, log := handshakeServer(t)
	config, _ := handshakeConfigs(t)
	const count = 16
	for id := uint64(0); id < count; id++ {
		conn, _ := handshakePipe(t)
		srv.serveClient(tls.Server(conn, config), id, "test")
		awaitLivenessSignal(t, conn.reads)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	seen := make(map[uint64]bool)
	for len(seen) < count {
		select {
		case e := <-log.entries:
			if e.Level <= logrus.WarnLevel {
				t.Fatalf("unexpected warning: %s", e.Message)
			}
			if e.Message == "Client disconnected" {
				id := e.Data["id"].(uint64)
				if seen[id] || e.Data["reason"] != "TLS handshake timeout" {
					t.Fatalf("disconnect: %v", e.Data)
				}
				seen[id] = true
			}
		case <-ctx.Done():
			t.Fatalf("only %d/%d client lifecycles finished", len(seen), count)
		}
	}
	assertLifecycleRegistryEmpty(t, &srv.registry)
}

func TestTLSHandshakeTimeoutConfiguration(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Second, 3 * time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			srv, log := handshakeServer(t)
			srv.TLSHandshakeTimeout = timeout
			config, clientConfig := handshakeConfigs(t)
			durations := make(chan time.Duration, 1)
			config.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
				d, ok := hello.Context().Deadline()
				if !ok {
					durations <- 0
				} else {
					durations <- time.Until(d)
				}
				return nil, errors.New("test handshake rejection")
			}
			conn, peer := handshakePipe(t)
			srv.serveClient(tls.Server(conn, config), 7, "test")
			if err := tls.Client(peer, clientConfig).Handshake(); err == nil {
				t.Fatal("expected rejection")
			}
			got := <-durations
			want := timeout
			if want <= 0 {
				want = 10 * time.Second
			}
			if got <= want-time.Second || got > want {
				t.Fatalf("handshake context remaining=%v, want near %v", got, want)
			}
			log.disconnected(t, 7, "TLS handshake error")
			assertLifecycleRegistryEmpty(t, &srv.registry)
		})
	}
}
