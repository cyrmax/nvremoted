// Copyright © 2026
//
// This source code is governed by the MIT license found in the LICENSE file.

package commands

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/n0ot/nvremoted/pkg/server"
	"github.com/sirupsen/logrus"
)

func TestStopError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	if err := stopError(ctx); err != nil {
		t.Fatalf("stopError before cancellation = %v", err)
	}
	cancel()
	if err := stopError(ctx); err != errStopRequested {
		t.Fatalf("stopError after cancellation = %v, want errStopRequested", err)
	}
}

func TestSupervise(t *testing.T) {
	t.Run("already cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		called := false
		if err := supervise(ctx, time.Second, func(context.Context) error {
			called = true
			return nil
		}); err != nil {
			t.Fatalf("supervise = %v, want nil", err)
		}
		if called {
			t.Fatal("run called after context was already cancelled")
		}
	})

	t.Run("stop request is clean", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		started := make(chan struct{})
		result := make(chan error, 1)
		go func() {
			result <- supervise(ctx, time.Second, func(ctx context.Context) error {
				close(started)
				<-ctx.Done()
				return errStopRequested
			})
		}()
		<-started
		cancel()
		cancel() // Repeated stop requests remain the same latched cancellation.
		if err := <-result; err != nil {
			t.Fatalf("supervise = %v, want clean stop", err)
		}
	})

	t.Run("normal completion", func(t *testing.T) {
		want := errors.New("serve finished")
		if err := supervise(context.Background(), time.Second, func(context.Context) error { return want }); !errors.Is(err, want) {
			t.Fatalf("supervise = %v, want %v", err, want)
		}
		if err := supervise(context.Background(), time.Second, func(context.Context) error { return nil }); err != nil {
			t.Fatalf("supervise = %v, want nil", err)
		}
	})

	t.Run("error wins cancellation race", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		entered := make(chan struct{})
		result := make(chan error, 1)
		want := errors.New("terminal accept failure")
		go func() {
			result <- supervise(ctx, time.Second, func(ctx context.Context) error {
				close(entered)
				<-ctx.Done()
				return want
			})
		}()
		<-entered
		cancel()
		if err := <-result; !errors.Is(err, want) {
			t.Fatalf("supervise = %v, want the concurrent real error %v", err, want)
		}
	})

	t.Run("deadline bounds blocked worker", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		release := make(chan struct{})
		started := make(chan struct{})
		workerDone := make(chan struct{})
		result := make(chan error, 1)
		go func() {
			result <- supervise(ctx, 25*time.Millisecond, func(context.Context) error {
				close(started)
				defer close(workerDone)
				<-release
				return nil
			})
		}()
		<-started
		cancel()
		if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("supervise = %v, want deadline exceeded", err)
		}
		close(release)
		select {
		case <-workerDone:
		case <-time.After(time.Second):
			t.Fatal("worker did not finish after test released it")
		}
	})
}

func TestStartServerStopsBeforeCreatingServerOrListening(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	prepared, listened := false, false
	err := startServer(ctx, func(context.Context) (runningServer, error) {
		prepared = true
		return &testRunningServer{}, nil
	}, func(context.Context) (net.Listener, error) {
		listened = true
		return newTestListener(), nil
	}, time.Second)
	if err != errStopRequested {
		t.Fatalf("startServer = %v, want errStopRequested", err)
	}
	if prepared || listened {
		t.Fatalf("startup continued: prepared=%t listened=%t", prepared, listened)
	}
}

func TestStartServerCancellationDuringPreparePreventsListen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prepared, listened := make(chan struct{}), false
	err := startServer(ctx, func(context.Context) (runningServer, error) {
		close(prepared)
		cancel()
		return &testRunningServer{}, nil
	}, func(context.Context) (net.Listener, error) {
		listened = true
		return newTestListener(), nil
	}, time.Second)
	<-prepared
	if err != errStopRequested || listened {
		t.Fatalf("startServer = %v, listened=%t; want stop and no listen", err, listened)
	}
}

func TestStartServerCancellationDuringListenClosesReturnedListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := newTestListener()
	var srv *testRunningServer
	err := startServer(ctx, func(context.Context) (runningServer, error) {
		srv = newTestRunningServer(nil)
		return srv, nil
	}, func(context.Context) (net.Listener, error) {
		cancel() // Simulates the request arriving as Listen returns a listener.
		return l, nil
	}, time.Second)
	if err != nil {
		t.Fatalf("startServer = %v, want clean cancellation", err)
	}
	if !l.isClosed() {
		t.Fatal("listener returned after stop was not closed")
	}
	if srv.serveCalls != 0 {
		t.Fatalf("Serve called %d times after stop", srv.serveCalls)
	}
}

func TestStartServerListenerErrorsAndCancellationPrecedence(t *testing.T) {
	t.Run("prepare error survives concurrent cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		want := errors.New("configuration read failed")
		err := startServer(ctx, func(context.Context) (runningServer, error) {
			cancel()
			return nil, want
		}, func(context.Context) (net.Listener, error) {
			t.Fatal("listen called after preparation failed")
			return nil, nil
		}, time.Second)
		if !errors.Is(err, want) {
			t.Fatalf("startServer = %v, want preparation error %v", err, want)
		}
	})

	t.Run("bind error survives concurrent cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		want := errors.New("address already in use")
		err := startServer(ctx, func(context.Context) (runningServer, error) {
			return &testRunningServer{}, nil
		}, func(context.Context) (net.Listener, error) {
			cancel()
			return nil, want
		}, time.Second)
		if !errors.Is(err, want) {
			t.Fatalf("startServer = %v, want bind error %v", err, want)
		}
	})

	t.Run("causal listen cancellation is clean stop", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := startServer(ctx, func(context.Context) (runningServer, error) {
			return &testRunningServer{}, nil
		}, func(context.Context) (net.Listener, error) {
			cancel()
			return nil, context.Canceled
		}, time.Second)
		if err != errStopRequested {
			t.Fatalf("startServer = %v, want errStopRequested", err)
		}
	})
}

func TestStartServerShutdownAndListenerCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := newTestListener()
	srv := newTestRunningServer(nil)
	result := make(chan error, 1)
	go func() {
		result <- startServer(ctx, func(context.Context) (runningServer, error) {
			return srv, nil
		}, func(context.Context) (net.Listener, error) {
			return l, nil
		}, time.Second)
	}()
	<-srv.started
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("startServer = %v, want graceful stop", err)
	}
	if srv.shutdownCalls != 1 {
		t.Fatalf("Shutdown called %d times, want 1", srv.shutdownCalls)
	}
	if !l.isClosed() {
		t.Fatal("listener not closed after serving stopped")
	}
	l.mu.Lock()
	closeCalls := l.closeCalls
	l.mu.Unlock()
	if closeCalls != 1 {
		t.Fatalf("underlying listener closed %d times, want exactly once", closeCalls)
	}
}

func TestOwnedListenerClosesUnderlyingOnce(t *testing.T) {
	underlying := newTestListener()
	owned := &ownedListener{Listener: underlying}
	if err := owned.Close(); err != nil {
		t.Fatal(err)
	}
	if err := owned.Close(); err != nil {
		t.Fatal(err)
	}
	underlying.mu.Lock()
	closeCalls := underlying.closeCalls
	underlying.mu.Unlock()
	if closeCalls != 1 {
		t.Fatalf("underlying listener closed %d times, want exactly once", closeCalls)
	}
}

func TestServeUntilStoppedPreservesServeErrors(t *testing.T) {
	want := errors.New("terminal accept error")
	srv := newTestRunningServer(want)
	srv.finishServe()
	if err := serveUntilStopped(context.Background(), srv, newTestListener(), time.Second); !errors.Is(err, want) {
		t.Fatalf("serveUntilStopped = %v, want serve error %v", err, want)
	}
	if srv.shutdownCalls != 1 {
		t.Fatalf("Shutdown called %d times after Serve returned, want 1", srv.shutdownCalls)
	}
}

func TestServeErrorWinsConcurrentStopAndPreservesShutdownError(t *testing.T) {
	serveFailure := errors.New("terminal accept failure")
	closeFailure := errors.New("listener close failure")
	for _, shutdownFailure := range []error{nil, closeFailure} {
		t.Run(fmt.Sprint(shutdownFailure), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			shutdownStarted := make(chan struct{})
			release := make(chan struct{})
			srv := newTestRunningServerWithShutdown(serveFailure, func(shutdownCtx context.Context, srv *testRunningServer) error {
				if shutdownCtx.Err() != nil {
					t.Error("Shutdown received the cancelled startup context")
				}
				close(shutdownStarted)
				<-release
				srv.finishServe()
				return shutdownFailure
			})
			result := make(chan error, 1)
			go func() { result <- serveUntilStopped(ctx, srv, newTestListener(), time.Second) }()
			select {
			case <-srv.started:
			case <-time.After(time.Second):
				t.Fatal("Serve did not start")
			}
			cancel()
			select {
			case <-shutdownStarted:
			case <-time.After(time.Second):
				t.Fatal("Shutdown did not start")
			}
			cancel() // Repeat during cleanup must not create a second path.
			close(release)
			select {
			case err := <-result:
				if !errors.Is(err, serveFailure) {
					t.Fatalf("stop hid terminal serving error: %v", err)
				}
				if shutdownFailure != nil && !errors.Is(err, shutdownFailure) {
					t.Fatalf("cleanup error lost: %v", err)
				}
				if !strings.HasPrefix(err.Error(), serveFailure.Error()) {
					t.Fatalf("primary serving error is not first: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cleanup did not finish")
			}
			if srv.shutdownCalls != 1 {
				t.Fatalf("repeated stop started %d shutdowns", srv.shutdownCalls)
			}
		})
	}
}

func TestServeUntilStoppedNormalizesServerUsedOnlyAfterStop(t *testing.T) {
	t.Run("stop won reservation race", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		srv := newTestRunningServer(server.ErrServerUsed)
		result := make(chan error, 1)
		go func() { result <- serveUntilStopped(ctx, srv, newTestListener(), time.Second) }()
		<-srv.started
		cancel()
		if err := <-result; err != nil {
			t.Fatalf("serveUntilStopped = %v, want benign reservation race", err)
		}
	})

	t.Run("server used without stop remains error", func(t *testing.T) {
		srv := newTestRunningServer(server.ErrServerUsed)
		srv.finishServe()
		if err := serveUntilStopped(context.Background(), srv, newTestListener(), time.Second); !errors.Is(err, server.ErrServerUsed) {
			t.Fatalf("serveUntilStopped = %v, want %v", err, server.ErrServerUsed)
		}
	})
}

func TestServeUntilStoppedNormalCompletion(t *testing.T) {
	srv := newTestRunningServer(nil)
	srv.finishServe()
	if err := serveUntilStopped(context.Background(), srv, newTestListener(), time.Second); err != nil {
		t.Fatalf("serveUntilStopped = %v, want nil", err)
	}
}

func TestServeUntilStoppedShutdownTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := newTestRunningServerWithShutdown(nil, func(ctx context.Context, _ *testRunningServer) error {
		<-ctx.Done()
		return ctx.Err()
	})
	result := make(chan error, 1)
	go func() { result <- serveUntilStopped(ctx, srv, newTestListener(), 25*time.Millisecond) }()
	<-srv.started
	cancel()
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("serveUntilStopped = %v, want shutdown deadline", err)
	}
	// The production Server may continue its cleanup after the caller's context
	// expires. Release this fake Serve goroutine explicitly and verify it exits.
	srv.finishServe()
	select {
	case <-srv.serveDone:
	case <-time.After(time.Second):
		t.Fatal("Serve goroutine did not exit after test release")
	}
}

func TestServeUntilStoppedWithRealServerAndIdleTCPClient(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	observed := &acceptObservedListener{Listener: listener, accepted: make(chan struct{})}
	defer observed.Close()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	defer client.Close()

	srv := &server.Server{Log: logrus.New()}
	srv.Log.SetOutput(io.Discard)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- serveUntilStopped(ctx, srv, observed, time.Second) }()
	select {
	case <-observed.accepted:
	case <-time.After(time.Second):
		cancel()
		_ = listener.Close()
		t.Fatal("server did not accept the idle TCP client")
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("serveUntilStopped = %v, want graceful shutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("real Server did not finish Shutdown")
	}
	if !observed.closed.Load() {
		t.Fatal("listener was not closed by Server.Shutdown")
	}
}

func TestServeUntilStoppedWithRealServerAndTLSClients(t *testing.T) {
	for _, mode := range []string{"established", "stalled handshake"} {
		t.Run(mode, func(t *testing.T) {
			certificate := testTLSCertificate(t)
			rawListener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			tlsListener := &tlsWrappingListener{
				Listener:  rawListener,
				config:    &tls.Config{Certificates: []tls.Certificate{certificate}},
				handshake: make(chan struct{}),
			}
			observed := &acceptObservedListener{Listener: tlsListener, accepted: make(chan struct{})}
			defer observed.Close()

			client, err := net.Dial("tcp", rawListener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			srv := &server.Server{Log: quietLogger(), TLSHandshakeTimeout: time.Hour}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- serveUntilStopped(ctx, srv, observed, time.Second) }()
			select {
			case <-observed.accepted:
			case <-time.After(time.Second):
				cancel()
				t.Fatal("server did not accept TLS client")
			}
			if mode == "established" {
				tlsClient := tls.Client(client, &tls.Config{InsecureSkipVerify: true}) // test certificate is self-signed
				handshakeCtx, handshakeCancel := context.WithTimeout(context.Background(), time.Second)
				if err := tlsClient.HandshakeContext(handshakeCtx); err != nil {
					handshakeCancel()
					cancel()
					t.Fatalf("TLS handshake: %v", err)
				}
				handshakeCancel()
			} else {
				select {
				case <-tlsListener.handshake:
				case <-time.After(time.Second):
					cancel()
					t.Fatal("server did not enter the stalled TLS handshake")
				}
			}
			cancel()
			select {
			case err := <-result:
				if err != nil {
					t.Fatalf("serveUntilStopped = %v, want graceful shutdown", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("real Server did not finish TLS client shutdown")
			}
		})
	}
}

func TestServeUntilStoppedWithRealServerAndBackpressuredClient(t *testing.T) {
	listener := newPipeListener()
	srv := &server.Server{Log: quietLogger(), MOTD: strings.Repeat("x", 1<<20), WriteTimeout: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- serveUntilStopped(ctx, srv, listener, time.Second) }()
	select {
	case <-listener.accepting:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("server did not begin accepting")
	}
	serverConn, peer := net.Pipe()
	observed := &writeObservedConn{Conn: serverConn, writing: make(chan struct{})}
	listener.connections <- observed
	select {
	case <-observed.writing:
	case <-time.After(time.Second):
		cancel()
		_ = peer.Close()
		t.Fatal("server did not begin writing the client MOTD")
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("serveUntilStopped = %v, want graceful shutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("real Server did not release the backpressured client")
	}
	_ = peer.Close()
}

func testTLSCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func quietLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	return logger
}

type testRunningServer struct {
	started       chan struct{}
	serveDone     chan struct{}
	serveErr      error
	shutdownFunc  func(context.Context, *testRunningServer) error
	startOnce     sync.Once
	finishOnce    sync.Once
	mu            sync.Mutex
	serveCalls    int
	shutdownCalls int
}

func newTestRunningServer(serveErr error) *testRunningServer {
	return newTestRunningServerWithShutdown(serveErr, nil)
}

func newTestRunningServerWithShutdown(serveErr error, shutdown func(context.Context, *testRunningServer) error) *testRunningServer {
	return &testRunningServer{
		started:      make(chan struct{}),
		serveDone:    make(chan struct{}),
		serveErr:     serveErr,
		shutdownFunc: shutdown,
	}
}

func (s *testRunningServer) Serve(net.Listener) error {
	s.mu.Lock()
	s.serveCalls++
	s.mu.Unlock()
	s.startOnce.Do(func() { close(s.started) })
	<-s.serveDone
	return s.serveErr
}

func (s *testRunningServer) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.shutdownCalls++
	s.mu.Unlock()
	if s.shutdownFunc != nil {
		return s.shutdownFunc(ctx, s)
	}
	s.finishServe()
	return nil
}

func (s *testRunningServer) finishServe() {
	s.finishOnce.Do(func() { close(s.serveDone) })
}

type testListener struct {
	closed     chan struct{}
	closeOnce  sync.Once
	closeCalls int
	mu         sync.Mutex
}

func newTestListener() *testListener { return &testListener{closed: make(chan struct{})} }

func (l *testListener) Accept() (net.Conn, error) {
	<-l.closed
	return nil, net.ErrClosed
}

func (l *testListener) Close() error {
	l.mu.Lock()
	l.closeCalls++
	l.mu.Unlock()
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (l *testListener) Addr() net.Addr { return testAddr("test") }

func (l *testListener) isClosed() bool {
	select {
	case <-l.closed:
		return true
	default:
		return false
	}
}

type testAddr string

func (a testAddr) Network() string { return "test" }
func (a testAddr) String() string  { return string(a) }

type acceptObservedListener struct {
	net.Listener
	accepted chan struct{}
	closed   atomic.Bool
	once     sync.Once
}

type tlsWrappingListener struct {
	net.Listener
	config    *tls.Config
	handshake chan struct{}
}

func (l *tlsWrappingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return tls.Server(&handshakeObservedConn{Conn: conn, started: l.handshake}, l.config), nil
}

type handshakeObservedConn struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

func (c *handshakeObservedConn) Read(p []byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	return c.Conn.Read(p)
}

type pipeListener struct {
	connections chan net.Conn
	accepting   chan struct{}
	closed      chan struct{}
	closeOnce   sync.Once
}

func newPipeListener() *pipeListener {
	return &pipeListener{
		connections: make(chan net.Conn),
		accepting:   make(chan struct{}),
		closed:      make(chan struct{}),
	}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case <-l.closed:
		return nil, net.ErrClosed
	default:
	}
	select {
	case <-l.closed:
		return nil, net.ErrClosed
	case l.accepting <- struct{}{}:
	}
	select {
	case conn := <-l.connections:
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (l *pipeListener) Addr() net.Addr { return testAddr("pipe") }

type writeObservedConn struct {
	net.Conn
	writing chan struct{}
	once    sync.Once
}

func (c *writeObservedConn) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.writing) })
	return c.Conn.Write(p)
}

func (l *acceptObservedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.once.Do(func() { close(l.accepted) })
	}
	return c, err
}

func (l *acceptObservedListener) Close() error {
	l.closed.Store(true)
	return l.Listener.Close()
}

var _ runningServer = (*testRunningServer)(nil)
var _ net.Listener = (*testListener)(nil)
var _ runningServer = (*server.Server)(nil)
