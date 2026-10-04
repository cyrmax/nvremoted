package server

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

type acceptErrorListener struct {
	err   error
	calls atomic.Int32
}

type acceptStep struct {
	conn net.Conn
	err  error
}
type acceptSequenceListener struct {
	steps  chan acceptStep
	closed chan struct{}
	once   sync.Once
	calls  atomic.Int32
}

func (l *acceptSequenceListener) Accept() (net.Conn, error) {
	l.calls.Add(1)
	select {
	case <-l.closed:
		return nil, net.ErrClosed
	default:
	}
	select {
	case <-l.closed:
		return nil, net.ErrClosed
	case step := <-l.steps:
		return step.conn, step.err
	}
}
func (l *acceptSequenceListener) Close() error   { l.once.Do(func() { close(l.closed) }); return nil }
func (l *acceptSequenceListener) Addr() net.Addr { return &net.TCPAddr{} }

func TestAcceptResourceErrorRecovery(t *testing.T) {
	srv, log := handshakeServer(t)
	conn, peer := handshakePipe(t)
	l := &acceptSequenceListener{steps: make(chan acceptStep, 2), closed: make(chan struct{})}
	l.steps <- acceptStep{err: &net.OpError{Op: "accept", Err: os.NewSyscallError("accept", syscall.EMFILE)}}
	l.steps <- acceptStep{conn: conn}
	result := make(chan error, 1)
	go func() { result <- srv.acceptClients(l) }()
	t.Cleanup(func() { l.Close() })
	select {
	case err := <-result:
		t.Fatalf("resource exhaustion stopped admission: %v", err)
	case <-conn.reads:
	case <-time.After(2 * time.Second):
		t.Fatal("admission did not recover")
	}
	peer.Close()
	l.Close()
	if err := receiveAcceptResult(t, result); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("close: %v", err)
	}
	// Consume the expected exhaustion warning before waiting for cleanup.
	for {
		select {
		case e := <-log.entries:
			if e.Message == "Client disconnected" {
				assertLifecycleRegistryEmpty(t, &srv.registry)
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatal("client cleanup did not finish")
		}
	}
}

func testAcceptRetryBackoff(t *testing.T, resourceError error) {
	t.Helper()
	srv, log := handshakeServer(t)
	conn, peer := handshakePipe(t)
	l := &acceptSequenceListener{steps: make(chan acceptStep, 16), closed: make(chan struct{})}
	want := []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond, 80 * time.Millisecond, 160 * time.Millisecond, 320 * time.Millisecond, 640 * time.Millisecond, time.Second, time.Second}
	wrapped := &net.OpError{Op: "accept", Err: os.NewSyscallError("accept", resourceError)}
	for range want {
		l.steps <- acceptStep{err: wrapped}
	}
	l.steps <- acceptStep{conn: conn}
	l.steps <- acceptStep{err: wrapped}
	waited := make(chan time.Duration, 1)
	resume := make(chan struct{})
	abort := make(chan struct{})
	done := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		defer close(done)
		result <- srv.acceptClientsWithRetry(l, func(delay time.Duration) {
			waited <- delay
			select {
			case <-resume:
			case <-abort:
			}
		})
	}()
	t.Cleanup(func() { l.Close(); close(abort); awaitLivenessSignal(t, done) })
	readWait := func(want time.Duration, calls int32) {
		t.Helper()
		select {
		case got := <-waited:
			if got != want {
				t.Fatalf("retry delay: %s, want %s", got, want)
			}
		case err := <-result:
			t.Fatalf("retry stopped: %v", err)
		case <-time.After(2 * time.Second):
			t.Fatal("retry did not reach wait barrier")
		}
		if l.calls.Load() != calls {
			t.Fatalf("Accept bypassed wait: %d calls, want %d", l.calls.Load(), calls)
		}
	}
	for i, delay := range want {
		readWait(delay, int32(i+1))
		resume <- struct{}{}
	}
	readWait(5*time.Millisecond, int32(len(want)+2))
	awaitLivenessSignal(t, conn.reads)
	// Close during the controlled retry wait; the next Accept must terminate.
	l.Close()
	resume <- struct{}{}
	if err := receiveAcceptResult(t, result); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("close during retry: %v", err)
	}
	awaitLivenessSignal(t, done)
	peer.Close()
	warnings := 0
	for {
		select {
		case e := <-log.entries:
			if e.Level <= logrus.ErrorLevel {
				t.Fatalf("unexpected error log: %s", e.Message)
			}
			if e.Level == logrus.WarnLevel {
				if e.Message != "Accept resource exhaustion; retrying" {
					t.Fatalf("warning: %s", e.Message)
				}
				warnings++
			}
			if e.Message == "Client disconnected" {
				if warnings != 2 {
					t.Fatalf("retry log spam: %d warnings, want 2 series", warnings)
				}
				assertLifecycleRegistryEmpty(t, &srv.registry)
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatal("client cleanup did not finish")
		}
	}
}

func TestAcceptResourceRetryBackoff(t *testing.T) { testAcceptRetryBackoff(t, syscall.EMFILE) }

func (l *acceptErrorListener) Accept() (net.Conn, error) {
	if l.calls.Add(1) > 1 {
		// Bound a regressed retry loop without sleeps or flooding the logs.
		// The call count still makes that regression fail the test.
		runtime.Goexit()
	}
	return nil, l.err
}
func (l *acceptErrorListener) Close() error   { return nil }
func (l *acceptErrorListener) Addr() net.Addr { return &net.TCPAddr{} }

func TestAcceptErrorsStopLoop(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		errorLogs int
	}{
		{"closed", net.ErrClosed, 0},
		{"wrapped_closed", fmt.Errorf("listener: %w", net.ErrClosed), 0},
		{"unexpected", errors.New("broken listener"), 1},
		{"timeout", &net.OpError{Op: "accept", Net: "tcp", Err: os.ErrDeadlineExceeded}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, log := handshakeServer(t)
			l := &acceptErrorListener{err: tc.err}
			done := make(chan struct{})
			var result error
			go func() { defer close(done); result = srv.acceptClients(l) }()
			awaitLivenessSignal(t, done)
			if !errors.Is(result, tc.err) {
				t.Fatalf("returned %v, want wrapped %v", result, tc.err)
			}
			if l.calls.Load() != 1 {
				t.Fatalf("Accept called %d times after terminal error", l.calls.Load())
			}
			errorLogs := 0
			for len(log.entries) > 0 {
				e := <-log.entries
				if e.Level <= logrus.ErrorLevel {
					errorLogs++
				}
			}
			if errorLogs != tc.errorLogs {
				t.Fatalf("error logs: %d, want %d", errorLogs, tc.errorLogs)
			}
		})
	}
}

func receiveAcceptResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after accept failure")
		return nil
	}
}

func TestServeReturnsAcceptErrors(t *testing.T) {
	for _, pings := range []time.Duration{0, time.Hour} {
		for _, failure := range []error{net.ErrClosed, errors.New("broken listener"), &net.OpError{Op: "accept", Err: os.ErrDeadlineExceeded}} {
			t.Run(fmt.Sprintf("pings=%s/error=%v", pings, failure), func(t *testing.T) {
				srv, _ := handshakeServer(t)
				srv.TimeBetweenPings = pings
				l := &acceptErrorListener{err: failure}
				result := make(chan error, 1)
				go func() { result <- srv.Serve(l) }()
				if err := receiveAcceptResult(t, result); !errors.Is(err, failure) {
					t.Fatalf("Serve error: %v", err)
				}
				if l.calls.Load() != 1 {
					t.Fatalf("Accept called %d times", l.calls.Load())
				}
			})
		}
	}
}

type acceptObservedListener struct {
	net.Listener
	entered chan struct{}
}

func (l *acceptObservedListener) Accept() (net.Conn, error) {
	select {
	case l.entered <- struct{}{}:
	default:
	}
	return l.Listener.Accept()
}

func acceptTCPListener(t *testing.T, encrypted bool) (net.Listener, *tls.Config) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	if encrypted {
		serverConfig, clientConfig := handshakeConfigs(t)
		return tls.NewListener(l, serverConfig), clientConfig
	}
	return l, nil
}

func TestServeListenerClose(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		for _, pings := range []time.Duration{0, time.Hour} {
			t.Run(fmt.Sprintf("tls=%v/pings=%s", encrypted, pings), func(t *testing.T) {
				srv, log := handshakeServer(t)
				srv.TimeBetweenPings = pings
				inner, _ := acceptTCPListener(t, encrypted)
				l := &acceptObservedListener{Listener: inner, entered: make(chan struct{}, 1)}
				result := make(chan error, 1)
				go func() { result <- srv.Serve(l) }()
				awaitLivenessSignal(t, l.entered)
				l.Close()
				if err := receiveAcceptResult(t, result); !errors.Is(err, net.ErrClosed) {
					t.Fatalf("closure error: %v", err)
				}
				for len(log.entries) > 0 {
					e := <-log.entries
					if e.Level <= logrus.WarnLevel {
						t.Fatalf("shutdown warning: %s", e.Message)
					}
				}
			})
		}
	}
}

func TestServeConcurrentTCPAndTLSAdmission(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%v", encrypted), func(t *testing.T) {
			srv, log := handshakeServer(t)
			srv.MOTD = "admission test"
			srv.TLSHandshakeTimeout = 2 * time.Second
			srv.TimeBetweenPings = time.Hour
			l, config := acceptTCPListener(t, encrypted)
			result := make(chan error, 1)
			go func() { result <- srv.Serve(l) }()
			const count = 16
			ready := make(chan error, count)
			finished := make(chan error, count)
			proceed := make(chan struct{})
			defer close(proceed)
			continueClients := make(chan struct{})
			for id := 0; id < count; id++ {
				go func(id int) {
					conn, err := net.DialTimeout("tcp", l.Addr().String(), 2*time.Second)
					if err != nil {
						ready <- err
						finished <- err
						return
					}
					defer conn.Close()
					conn.SetDeadline(time.Now().Add(5 * time.Second))
					if encrypted {
						conn = tls.Client(conn, config)
					}
					decoder := json.NewDecoder(conn)
					var motd ClientMOTDResponse
					if err := decoder.Decode(&motd); err != nil {
						ready <- err
						finished <- err
						return
					}
					if motd.MOTD != srv.MOTD {
						err = fmt.Errorf("MOTD: %#v", motd)
						ready <- err
						finished <- err
						return
					}
					ready <- nil
					select {
					case <-continueClients:
					case <-proceed:
						finished <- errors.New("test aborted")
						return
					}
					_, err = io.WriteString(conn, fmt.Sprintf("{\"type\":\"join\",\"channel\":\"client-%d\",\"connection_type\":\"master\"}\n", id))
					if err == nil {
						var joined ClientChannelJoinedResponse
						err = decoder.Decode(&joined)
						if err == nil && joined.Type != "channel_joined" {
							err = fmt.Errorf("join: %#v", joined)
						}
					}
					finished <- err
				}(id)
			}
			for i := 0; i < count; i++ {
				if err := receiveAcceptResult(t, ready); err != nil {
					t.Fatal(err)
				}
			}
			l.Close()
			if err := receiveAcceptResult(t, result); !errors.Is(err, net.ErrClosed) {
				t.Fatalf("Serve: %v", err)
			}
			// Already accepted clients must still complete protocol I/O and cleanup.
			close(continueClients)
			for i := 0; i < count; i++ {
				if err := receiveAcceptResult(t, finished); err != nil {
					t.Fatal(err)
				}
			}
			seen := make(map[uint64]bool)
			for len(seen) < count {
				select {
				case e := <-log.entries:
					if e.Level <= logrus.WarnLevel {
						t.Fatalf("unexpected warning: %s", e.Message)
					}
					if e.Message == "Client connected" || e.Message == "Client disconnected" {
						if e.Data["remote_host"] != "127.0.0.1" {
							t.Fatalf("peer IP: %v", e.Data)
						}
						if e.Message == "Client disconnected" {
							id := e.Data["id"].(uint64)
							if seen[id] {
								t.Fatalf("duplicate cleanup: %d", id)
							}
							seen[id] = true
						}
					}
				case <-time.After(2 * time.Second):
					t.Fatalf("cleanup: %d/%d", len(seen), count)
				}
			}
			assertLifecycleRegistryEmpty(t, &srv.registry)
		})
	}
}
