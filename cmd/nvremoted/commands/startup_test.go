package commands

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfigReadCancellationAndErrors(t *testing.T) {
	failure := errors.New("invalid TOML")
	for _, mode := range []string{"before", "during", "error-with-stop", "normal"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "before" {
				cancel()
			}
			calls := 0
			err := readConfig(ctx, func() error {
				calls++
				if mode != "normal" {
					cancel()
				}
				if mode == "error-with-stop" {
					return failure
				}
				return nil
			})
			switch mode {
			case "before":
				if calls != 0 || err != errStopRequested {
					t.Fatalf("pre-read stop: calls=%d, err=%v", calls, err)
				}
			case "during":
				if calls != 1 || err != errStopRequested {
					t.Fatalf("continued after read with stop: calls=%d, err=%v", calls, err)
				}
			case "error-with-stop":
				if !errors.Is(err, failure) {
					t.Fatalf("config error lost: %v", err)
				}
			case "normal":
				if err != nil || calls != 1 {
					t.Fatalf("normal read: calls=%d, err=%v", calls, err)
				}
			}
		})
	}
}

func TestTLSLoadCancellationAndErrors(t *testing.T) {
	failure := errors.New("invalid key pair")
	for _, mode := range []string{"before", "during", "error-with-stop", "normal"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "before" {
				cancel()
			}
			calls := 0
			config, err := loadTLSConfig(ctx, "cert", "key", func(cert, key string) (tls.Certificate, error) {
				calls++
				if cert != "cert" || key != "key" {
					t.Error("incorrect loader paths")
				}
				if mode != "normal" {
					cancel()
				}
				if mode == "error-with-stop" {
					return tls.Certificate{}, failure
				}
				return tls.Certificate{}, nil
			})
			if mode == "normal" {
				if err != nil || config == nil || len(config.Certificates) != 1 {
					t.Fatalf("normal load: config=%v, err=%v", config, err)
				}
			} else {
				if config != nil {
					t.Fatal("published TLS config after cancellation")
				}
				if mode == "error-with-stop" {
					if !errors.Is(err, failure) {
						t.Fatalf("TLS load error lost: %v", err)
					}
				} else if err != errStopRequested {
					t.Fatalf("stop not propagated: %v", err)
				}
			}
			if mode == "before" && calls != 0 {
				t.Fatal("loader ran after pre-start stop")
			}
		})
	}
}

func TestTLSRequiresBothPaths(t *testing.T) {
	for _, paths := range [][2]string{{"", ""}, {"cert", ""}, {"", "key"}} {
		config, err := loadTLSConfig(context.Background(), paths[0], paths[1], func(string, string) (tls.Certificate, error) {
			t.Fatal("loader invoked with incomplete paths")
			return tls.Certificate{}, nil
		})
		if config != nil || err == nil {
			t.Fatalf("missing paths accepted: %v, %v", config, err)
		}
	}
}

func TestInitConfigAndStartRespectPreCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := initConfig(ctx); err != errStopRequested {
		t.Fatalf("initConfig continued startup: %v", err)
	}
	startCmd.SetContext(ctx)
	defer startCmd.SetContext(context.Background())
	if err := runServer(startCmd, nil); err != errStopRequested {
		t.Fatalf("start continued startup: %v", err)
	}
}

func TestBlockedStartupReadCannotResumeListeningAfterTimeout(t *testing.T) {
	for _, stage := range []string{"config", "TLS"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan struct{})
			release := make(chan struct{})
			workerDone := make(chan struct{})
			var listened atomic.Bool
			result := make(chan error, 1)
			go func() {
				result <- supervise(ctx, 20*time.Millisecond, func(ctx context.Context) error {
					defer close(workerDone)
					return startServer(ctx, func(ctx context.Context) (runningServer, error) {
						block := func() { close(entered); <-release }
						var err error
						if stage == "config" {
							err = readConfig(ctx, func() error { block(); return nil })
						} else {
							_, err = loadTLSConfig(ctx, "cert", "key", func(string, string) (tls.Certificate, error) {
								block()
								return tls.Certificate{}, nil
							})
						}
						return newTestRunningServer(nil), err
					}, func(context.Context) (net.Listener, error) {
						listened.Store(true)
						return newTestListener(), nil
					}, time.Second)
				})
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("startup did not reach read barrier")
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("blocked startup reported success: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("coordinator did not bound startup waiting")
			}
			cancel() // A further request after the deadline remains idempotent.
			close(release)
			select {
			case <-workerDone:
			case <-time.After(time.Second):
				t.Fatal("released startup worker leaked")
			}
			if listened.Load() {
				t.Fatal("startup resumed listening after timed-out stop")
			}
		})
	}
}
