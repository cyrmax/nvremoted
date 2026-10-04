package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

type shutdownObservedConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func startShutdownServer(t *testing.T, srv *Server, inner net.Listener) (<-chan error, net.Listener) {
	t.Helper()
	l := &acceptObservedListener{Listener: inner, entered: make(chan struct{}, 1)}
	result := make(chan error, 1)
	go func() { result <- srv.Serve(l) }()
	awaitLivenessSignal(t, l.entered)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("cleanup Shutdown: %v", err)
		}
	})
	return result, l
}

func shutdownAndCheck(t *testing.T, srv *Server, result <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := receiveAcceptResult(t, result); err != nil {
		t.Fatalf("Serve after Shutdown: %v", err)
	}
	assertShutdownComplete(t, srv)
}

func assertShutdownComplete(t *testing.T, srv *Server) {
	t.Helper()
	assertLifecycleRegistryEmpty(t, &srv.registry)
	srv.lifecycleMu.Lock()
	defer srv.lifecycleMu.Unlock()
	if len(srv.active) != 0 {
		t.Fatalf("remaining lifecycle clients: %d", len(srv.active))
	}
	select {
	case <-srv.done:
	default:
		t.Fatal("server completion not signaled")
	}
}

func TestShutdownNoClientsAndIdempotence(t *testing.T) {
	srv, _ := handshakeServer(t)
	l := &acceptSequenceListener{steps: make(chan acceptStep), closed: make(chan struct{})}
	result, _ := startShutdownServer(t, srv, l)
	shutdownAndCheck(t, srv, result)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("completed Shutdown with canceled context: %v", err)
	}
	select {
	case <-l.closed:
	default:
		t.Fatal("explicit Shutdown did not close caller listener")
	}
}

func TestShutdownBeforeServe(t *testing.T) {
	srv := &Server{}
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	l := &acceptErrorListener{err: net.ErrClosed}
	if err := srv.Serve(l); !errors.Is(err, ErrServerUsed) || l.calls.Load() != 0 {
		t.Fatalf("Serve after Shutdown: %v, calls %d", err, l.calls.Load())
	}
}

func TestShutdownIdleAndTLSClients(t *testing.T) {
	for _, mode := range []string{"plain", "tls", "handshake"} {
		t.Run(mode, func(t *testing.T) {
			srv, log := handshakeServer(t)
			srv.TLSHandshakeTimeout = time.Hour // Shutdown must not wait for this timer.
			if mode == "tls" {
				srv.MOTD = "TLS established"
			}
			l := &acceptSequenceListener{steps: make(chan acceptStep), closed: make(chan struct{})}
			result, _ := startShutdownServer(t, srv, l)
			conn, peer := handshakePipe(t)
			var accepted net.Conn = conn
			if mode != "plain" {
				serverConfig, clientConfig := handshakeConfigs(t)
				accepted = tls.Server(conn, serverConfig)
				l.steps <- acceptStep{conn: accepted}
				if mode == "tls" {
					client := tls.Client(peer, clientConfig)
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()
					if err := client.HandshakeContext(ctx); err != nil {
						t.Fatal(err)
					}
					var motd ClientMOTDResponse
					if err := json.NewDecoder(client).Decode(&motd); err != nil || motd.MOTD != srv.MOTD {
						t.Fatalf("TLS protocol: %#v, %v", motd, err)
					}
				}
			} else {
				l.steps <- acceptStep{conn: accepted}
			}
			awaitLivenessSignal(t, conn.reads)
			shutdownAndCheck(t, srv, result)
			log.disconnected(t, 0, "Server shutdown")
			peer.SetReadDeadline(time.Now().Add(2 * time.Second))
			if _, err := peer.Read(make([]byte, 1)); err == nil {
				t.Fatal("peer transport still open")
			} else if e, ok := err.(net.Error); ok && e.Timeout() {
				t.Fatalf("peer transport not closed: %v", err)
			}
		})
	}
}

func TestShutdownUnreadPeer(t *testing.T) {
	srv, log := handshakeServer(t)
	srv.MOTD = "unread MOTD"
	srv.WriteTimeout = time.Hour
	l := &acceptSequenceListener{steps: make(chan acceptStep), closed: make(chan struct{})}
	result, _ := startShutdownServer(t, srv, l)
	conn, _ := handshakePipe(t)
	l.steps <- acceptStep{conn: conn}
	awaitLivenessSignal(t, conn.writes)
	shutdownAndCheck(t, srv, result)
	assertShutdownDisconnect(t, log, 0, "Server shutdown")
}

func assertShutdownDisconnect(t *testing.T, log *handshakeLog, id uint64, reason string) {
	t.Helper()
	for len(log.entries) > 0 {
		e := <-log.entries
		if e.Message == "Client disconnected" && e.Data["id"] == id {
			if e.Data["reason"] != reason {
				t.Fatalf("first stop reason overwritten: %v", e.Data)
			}
			return
		}
	}
	t.Fatalf("missing completed disconnect for %d", id)
}

func joinShutdownPeer(t *testing.T, l *acceptSequenceListener, name string) net.Conn {
	t.Helper()
	conn, peer := handshakePipe(t)
	l.steps <- acceptStep{conn: conn}
	peer.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := fmt.Fprintf(peer, "{\"type\":\"join\",\"channel\":%q,\"connection_type\":\"master\"}\n", name); err != nil {
		t.Fatal(err)
	}
	var joined ClientChannelJoinedResponse
	if err := json.NewDecoder(peer).Decode(&joined); err != nil || joined.Type != "channel_joined" {
		t.Fatalf("join: %#v, %v", joined, err)
	}
	peer.SetDeadline(time.Time{})
	return peer
}

func TestShutdownMultipleChannelsAndConcurrentCallers(t *testing.T) {
	srv, log := handshakeServer(t)
	l := &acceptSequenceListener{steps: make(chan acceptStep), closed: make(chan struct{})}
	result, _ := startShutdownServer(t, srv, l)
	for _, name := range []string{"one", "one", "two", "two", "E2E_" + strings.Repeat("a", 64)} {
		joinShutdownPeer(t, l, name)
	}
	before := srv.registry.Stats()
	if before.NumClients != 5 || before.NumChannels != 3 || before.NumE2eChannels != 1 {
		t.Fatalf("before shutdown: %#v", before)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	callers := make(chan error, 8)
	for i := 0; i < cap(callers); i++ {
		go func() { callers <- srv.Shutdown(ctx) }()
	}
	for i := 0; i < cap(callers); i++ {
		if err := receiveAcceptResult(t, callers); err != nil {
			t.Fatal(err)
		}
	}
	if err := receiveAcceptResult(t, result); err != nil {
		t.Fatal(err)
	}
	assertShutdownComplete(t, srv)
	after := srv.registry.Stats()
	if after.MaxClients != before.MaxClients || after.MaxChannels != before.MaxChannels || !after.MaxClientsTime.Equal(before.MaxClientsTime) {
		t.Fatalf("shutdown reset historical stats: before=%#v after=%#v", before, after)
	}
	seen := make(map[uint64]bool)
	for len(log.entries) > 0 {
		e := <-log.entries
		if e.Message == "Client disconnected" {
			id := e.Data["id"].(uint64)
			if seen[id] {
				t.Fatalf("duplicate cleanup for %d", id)
			}
			seen[id] = true
		}
	}
	if len(seen) != 5 {
		t.Fatalf("disconnect logs: %d", len(seen))
	}
}

type shutdownBarrierHook struct {
	entered, release chan struct{}
	once             sync.Once
}

func (h *shutdownBarrierHook) Levels() []logrus.Level { return logrus.AllLevels }
func (h *shutdownBarrierHook) Fire(e *logrus.Entry) error {
	if e.Message == "Client disconnected" {
		h.once.Do(func() { close(h.entered) })
		<-h.release
	}
	return nil
}

func TestShutdownContextBoundsWaitAndContinuesCleanup(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%v", timeout), func(t *testing.T) {
			srv, _ := handshakeServer(t)
			h := &shutdownBarrierHook{entered: make(chan struct{}), release: make(chan struct{})}
			srv.Log.AddHook(h)
			// Release before the server cleanup registered below runs, even on failure.
			l := &acceptSequenceListener{steps: make(chan acceptStep), closed: make(chan struct{})}
			result, _ := startShutdownServer(t, srv, l)
			var once sync.Once
			release := func() { once.Do(func() { close(h.release) }) }
			t.Cleanup(release)
			conn, _ := handshakePipe(t)
			l.steps <- acceptStep{conn: conn}
			awaitLivenessSignal(t, conn.reads)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			shutdown := make(chan error, 1)
			go func() { shutdown <- srv.Shutdown(ctx) }()
			awaitLivenessSignal(t, h.entered)
			select {
			case err := <-result:
				t.Fatalf("Serve returned before disconnect logging: %v", err)
			default:
			}
			if timeout {
				deadlineCtx, deadlineCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer deadlineCancel()
				if err := srv.Shutdown(deadlineCtx); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("deadline: %v", err)
				}
			}
			cancel()
			if err := receiveAcceptResult(t, shutdown); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation: %v", err)
			}
			if err := srv.Serve(&acceptErrorListener{err: net.ErrClosed}); !errors.Is(err, ErrServerUsed) {
				t.Fatalf("Serve during old cleanup: %v", err)
			}
			release()
			shutdownAndCheck(t, srv, result)
		})
	}
}

func TestShutdownDuringJoin(t *testing.T) {
	srv, _ := handshakeServer(t)
	l := &acceptSequenceListener{steps: make(chan acceptStep), closed: make(chan struct{})}
	result, _ := startShutdownServer(t, srv, l)
	joinShutdownPeer(t, l, "one")
	conn, peer := handshakePipe(t)
	l.steps <- acceptStep{conn: conn}
	peer.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.WriteString(peer, "{\"type\":\"join\",\"channel\":\"one\",\"connection_type\":\"master\"}\n"); err != nil {
		t.Fatal(err)
	}
	// Registry admission has finished but the join handler is blocked sending
	// channel_joined, before assigning c.channel. Stop must still clean it up.
	awaitLivenessSignal(t, conn.writes)
	shutdownAndCheck(t, srv, result)
}

func TestShutdownDuringPeriodicDispatch(t *testing.T) {
	srv, _ := handshakeServer(t)
	l := &acceptSequenceListener{steps: make(chan acceptStep), closed: make(chan struct{})}
	result, _ := startShutdownServer(t, srv, l)
	joinShutdownPeer(t, l, "one")
	// A controlled snapshot dispatch uses the same function as the ticker.
	// Hold the registry until shutdown cancellation is visible to dispatch.
	srv.registry.lock.Lock()
	var once sync.Once
	release := func() { once.Do(srv.registry.lock.Unlock) }
	t.Cleanup(release)
	dispatched := make(chan struct{})
	go func() { srv.dispatchPings(); close(dispatched) }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := srv.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	release()
	awaitLivenessSignal(t, dispatched)
	shutdownAndCheck(t, srv, result)
}

func TestShutdownDuringLeave(t *testing.T) {
	srv, _ := handshakeServer(t)
	l := &acceptSequenceListener{steps: make(chan acceptStep), closed: make(chan struct{})}
	result, _ := startShutdownServer(t, srv, l)
	departing := joinShutdownPeer(t, l, "one")
	joinShutdownPeer(t, l, "one")
	srv.lifecycleMu.Lock()
	var remaining *handshakeConn
	for c := range srv.active {
		if c.id == 1 {
			remaining = c.conn.(*handshakeConn)
		}
	}
	srv.lifecycleMu.Unlock()
	// Consume the joined response observation, then pin registry cleanup.
	awaitLivenessSignal(t, remaining.writes)
	srv.registry.lock.Lock()
	var once sync.Once
	release := func() { once.Do(srv.registry.lock.Unlock) }
	t.Cleanup(release)
	departing.Close()
	// A left notification proves channel.leave is executing, before its
	// registry cleanup can take the held lock.
	awaitLivenessSignal(t, remaining.writes)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := srv.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown at leave barrier: %v", err)
	}
	release()
	shutdownAndCheck(t, srv, result)
}

func TestShutdownActualPeriodicPing(t *testing.T) {
	srv, _ := handshakeServer(t)
	srv.TimeBetweenPings = 20 * time.Millisecond
	srv.WriteTimeout = time.Hour
	l := &acceptSequenceListener{steps: make(chan acceptStep), closed: make(chan struct{})}
	result, _ := startShutdownServer(t, srv, l)
	joinShutdownPeer(t, l, "one")
	srv.lifecycleMu.Lock()
	var conn *handshakeConn
	for c := range srv.active {
		conn = c.conn.(*handshakeConn)
	}
	srv.lifecycleMu.Unlock()
	awaitLivenessSignal(t, conn.writes) // joined response
	awaitLivenessSignal(t, conn.writes) // periodic ping blocked on unread peer
	shutdownAndCheck(t, srv, result)
}

func TestServeFailureCleansChannelsAndPreservesListenerOwnership(t *testing.T) {
	for _, failure := range []error{net.ErrClosed, errors.New("terminal failure")} {
		t.Run(failure.Error(), func(t *testing.T) {
			srv, _ := handshakeServer(t)
			l := &acceptSequenceListener{steps: make(chan acceptStep), closed: make(chan struct{})}
			result, _ := startShutdownServer(t, srv, l)
			joinShutdownPeer(t, l, "one")
			joinShutdownPeer(t, l, "two")
			l.steps <- acceptStep{err: failure}
			if err := receiveAcceptResult(t, result); !errors.Is(err, failure) {
				t.Fatalf("Serve failure: %v", err)
			}
			assertShutdownComplete(t, srv)
			select {
			case <-l.closed:
				t.Fatal("accept failure closed caller-owned listener")
			default:
			}
			if err := srv.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			l.Close()
		})
	}
}

type shutdownCloseErrorListener struct {
	*acceptSequenceListener
	err error
}

func (l *shutdownCloseErrorListener) Close() error {
	l.acceptSequenceListener.Close()
	return l.err
}

func TestShutdownListenerCloseError(t *testing.T) {
	srv, _ := handshakeServer(t)
	failure := errors.New("listener close failure")
	l := &shutdownCloseErrorListener{acceptSequenceListener: &acceptSequenceListener{
		steps: make(chan acceptStep), closed: make(chan struct{}),
	}, err: failure}
	observed := &acceptObservedListener{Listener: l, entered: make(chan struct{}, 1)}
	result := make(chan error, 1)
	go func() { result <- srv.Serve(observed) }()
	awaitLivenessSignal(t, observed.entered)
	if err := srv.Shutdown(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("listener close error: %v", err)
	}
	if err := receiveAcceptResult(t, result); err != nil {
		t.Fatalf("graceful Serve result: %v", err)
	}
	if err := srv.Shutdown(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("repeated close error: %v", err)
	}
	assertShutdownComplete(t, srv)
}

func TestShutdownPreservesFirstStopReasonAndIgnoresClientCloseError(t *testing.T) {
	srv, log := handshakeServer(t)
	h := &shutdownBarrierHook{entered: make(chan struct{}), release: make(chan struct{})}
	srv.Log.AddHook(h)
	l := &acceptSequenceListener{steps: make(chan acceptStep), closed: make(chan struct{})}
	result, _ := startShutdownServer(t, srv, l)
	var once sync.Once
	release := func() { once.Do(func() { close(h.release) }) }
	t.Cleanup(release)
	conn, _ := handshakePipe(t)
	conn.failClose = true
	l.steps <- acceptStep{conn: conn}
	awaitLivenessSignal(t, conn.reads)
	srv.lifecycleMu.Lock()
	var active *client
	for c := range srv.active {
		active = c
	}
	srv.lifecycleMu.Unlock()
	active.stop("original stop reason")
	awaitLivenessSignal(t, h.entered)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := srv.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	release()
	shutdownAndCheck(t, srv, result)
	assertShutdownDisconnect(t, log, 0, "original stop reason")
}

func TestServingMethodsRejectConcurrentAndRepeatedRuns(t *testing.T) {
	srv, _ := handshakeServer(t)
	l := &acceptSequenceListener{steps: make(chan acceptStep), closed: make(chan struct{})}
	result, _ := startShutdownServer(t, srv, l)
	methods := []func() error{
		func() error { return srv.Serve(&acceptErrorListener{err: net.ErrClosed}) },
		func() error { return srv.ListenAndServe("invalid address") },
		func() error { return srv.ListenAndServeTLS("invalid address", "missing cert", "missing key") },
	}
	for _, call := range methods {
		if err := call(); !errors.Is(err, ErrServerUsed) {
			t.Fatalf("concurrent serving method: %v", err)
		}
	}
	shutdownAndCheck(t, srv, result)
	created := srv.registry.createdTime
	for _, call := range methods {
		if err := call(); !errors.Is(err, ErrServerUsed) {
			t.Fatalf("repeated serving method: %v", err)
		}
	}
	if !srv.registry.createdTime.Equal(created) {
		t.Fatal("rejected run changed createdTime")
	}
}

func TestFailedStartIsSingleRun(t *testing.T) {
	for _, tlsMode := range []bool{false, true} {
		srv, _ := handshakeServer(t)
		var err error
		if tlsMode {
			err = srv.ListenAndServeTLS("invalid address", "", "")
		} else {
			err = srv.ListenAndServe("invalid address")
		}
		if err == nil || errors.Is(err, ErrServerUsed) {
			t.Fatalf("setup error: %v", err)
		}
		if err := srv.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := srv.Serve(&acceptErrorListener{err: net.ErrClosed}); !errors.Is(err, ErrServerUsed) {
			t.Fatalf("reuse after setup failure: %v", err)
		}
		assertShutdownComplete(t, srv)
	}
}

type shutdownGatedConn struct {
	net.Conn
	entered, release chan struct{}
	once             sync.Once
}

func (c *shutdownGatedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return err
}

func TestShutdownClosesClientsIndependently(t *testing.T) {
	srv, _ := handshakeServer(t)
	l := &acceptSequenceListener{steps: make(chan acceptStep), closed: make(chan struct{})}
	result, _ := startShutdownServer(t, srv, l)
	conn1, _ := handshakePipe(t)
	gated := &shutdownGatedConn{Conn: conn1, entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(gated.release) }) }
	t.Cleanup(release)
	conn2, _ := handshakePipe(t)
	observed := &shutdownObservedConn{Conn: conn2, closed: make(chan struct{})}
	l.steps <- acceptStep{conn: gated}
	l.steps <- acceptStep{conn: observed}
	awaitLivenessSignal(t, conn1.reads)
	awaitLivenessSignal(t, conn2.reads)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := srv.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	awaitLivenessSignal(t, gated.entered)
	awaitLivenessSignal(t, observed.closed)
	select {
	case err := <-result:
		t.Fatalf("Serve did not wait for stop worker: %v", err)
	default:
	}
	release()
	shutdownAndCheck(t, srv, result)
}

// Deliver one result after Close to control the Accept/shutdown race.
type shutdownLateListener struct {
	step   chan acceptStep
	closed chan struct{}
	once   sync.Once
}

func (l *shutdownLateListener) Accept() (net.Conn, error) {
	step := <-l.step
	return step.conn, step.err
}
func (l *shutdownLateListener) Close() error   { l.once.Do(func() { close(l.closed) }); return nil }
func (l *shutdownLateListener) Addr() net.Addr { return &net.TCPAddr{} }

func TestShutdownAcceptInFlight(t *testing.T) {
	for _, failure := range []error{nil, errors.New("late terminal failure")} {
		t.Run(fmt.Sprint(failure), func(t *testing.T) {
			srv, log := handshakeServer(t)
			l := &shutdownLateListener{step: make(chan acceptStep, 1), closed: make(chan struct{})}
			result, _ := startShutdownServer(t, srv, l)
			// Unblock Accept on test failure before the server cleanup.
			t.Cleanup(func() {
				select {
				case l.step <- acceptStep{err: net.ErrClosed}:
				default:
				}
			})
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := srv.Shutdown(ctx); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			awaitLivenessSignal(t, l.closed)
			if failure == nil {
				conn, _ := handshakePipe(t)
				observed := &shutdownObservedConn{Conn: conn, closed: make(chan struct{})}
				l.step <- acceptStep{conn: observed}
				shutdownAndCheck(t, srv, result)
				awaitLivenessSignal(t, observed.closed)
				for len(log.entries) > 0 {
					if e := <-log.entries; e.Message == "Client connected" {
						t.Fatal("connection admitted after shutdown")
					}
				}
			} else {
				l.step <- acceptStep{err: failure}
				if err := receiveAcceptResult(t, result); !errors.Is(err, failure) {
					t.Fatalf("terminal failure masked by shutdown: %v", err)
				}
				if err := srv.Shutdown(context.Background()); err != nil {
					t.Fatal(err)
				}
				assertShutdownComplete(t, srv)
			}
		})
	}
}

func (c *shutdownObservedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { close(c.closed) })
	return err
}

func TestServeFailureWaitsForIdleClientCleanup(t *testing.T) {
	srv, _ := handshakeServer(t)
	conn, _ := handshakePipe(t)
	observed := &shutdownObservedConn{Conn: conn, closed: make(chan struct{})}
	l := &acceptSequenceListener{steps: make(chan acceptStep), closed: make(chan struct{})}
	t.Cleanup(func() { l.Close(); observed.Close() })
	result := make(chan error, 1)
	go func() { result <- srv.Serve(l) }()
	l.steps <- acceptStep{conn: observed}
	awaitLivenessSignal(t, conn.reads)
	failure := errors.New("terminal accept failure")
	l.steps <- acceptStep{err: failure}
	if err := receiveAcceptResult(t, result); !errors.Is(err, failure) {
		t.Fatalf("Serve: %v", err)
	}
	select {
	case <-observed.closed:
	default:
		t.Fatal("Serve returned with an active idle connection")
	}
	assertLifecycleRegistryEmpty(t, &srv.registry)
}

func TestServeRejectsSecondRun(t *testing.T) {
	srv, _ := handshakeServer(t)
	if err := srv.Serve(&acceptErrorListener{err: net.ErrClosed}); !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	l := &acceptErrorListener{err: net.ErrClosed}
	if err := srv.Serve(l); err == nil || errors.Is(err, net.ErrClosed) {
		t.Fatalf("second Serve did not reject reuse: %v", err)
	}
	if l.calls.Load() != 0 {
		t.Fatal("second Serve touched the new listener")
	}
}
