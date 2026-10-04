package commands

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/n0ot/nvremoted/pkg/server"
)

const shutdownTimeout = 10 * time.Second

var errStopRequested = errors.New("startup stopped")

func stopError(ctx context.Context) error {
	if ctx.Err() != nil {
		return errStopRequested
	}
	return nil
}

// supervise bounds the whole command after cancellation, even if a filesystem
// operation or logging hook cannot be interrupted. On timeout only the process
// exit boundary may abandon the worker; normal completion waits for its defers.
func supervise(ctx context.Context, timeout time.Duration, run func(context.Context) error) error {
	if stopError(ctx) != nil {
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	normalize := func(err error) error {
		if err == errStopRequested {
			return nil
		}
		return err
	}
	select {
	case err := <-done:
		return normalize(err)
	case <-ctx.Done():
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return normalize(err)
	case <-timer.C:
		// A published complete result outranks the timeout, independent of
		// which ready select case was chosen.
		select {
		case err := <-done:
			return normalize(err)
		default:
			return fmt.Errorf("command did not stop within %s: %w", timeout, context.DeadlineExceeded)
		}
	}
}

type runningServer interface {
	Serve(net.Listener) error
	Shutdown(context.Context) error
}

// startServer owns the newly prepared server and the listener, including the
// gap between successful listen and Serve's single-run reservation.
func startServer(ctx context.Context, prepare func(context.Context) (runningServer, error), listen func(context.Context) (net.Listener, error), timeout time.Duration) (err error) {
	if err = stopError(ctx); err != nil {
		return err
	}
	srv, err := prepare(ctx)
	if err != nil {
		return err
	}
	if err = stopError(ctx); err != nil {
		return err
	}
	l, err := listen(ctx)
	if err != nil {
		// Only cancellation caused by our context is an expected listen
		// result; a concurrent bind/configuration error remains a failure.
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return errStopRequested
		}
		return err
	}
	owned := &ownedListener{Listener: l}
	defer func() {
		if closeErr := owned.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close listener: %w", closeErr))
		}
	}()
	if stopError(ctx) != nil {
		return nil
	}
	return serveUntilStopped(ctx, srv, owned, timeout)
}

// Close once so Server shutdown and the CLI's ownership defer share one result.
type ownedListener struct {
	net.Listener
	once sync.Once
	err  error
}

func (l *ownedListener) Close() error {
	l.once.Do(func() { l.err = l.Listener.Close() })
	return l.err
}

// srv must be fresh and exclusively owned by this invocation. Shutdown already
// closes transports; the CLI waits for both its result and Serve's return.
func serveUntilStopped(ctx context.Context, srv runningServer, listener net.Listener, timeout time.Duration) error {
	if stopError(ctx) != nil {
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(listener) }()
	var serveErr error
	var finished bool
	select {
	case serveErr = <-done:
		finished = true
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	shutdownErr := srv.Shutdown(shutdownCtx)
	if !finished {
		select {
		case serveErr = <-done:
			finished = true
		case <-shutdownCtx.Done():
			select {
			case serveErr = <-done:
				finished = true
			default:
				shutdownErr = errors.Join(shutdownErr, shutdownCtx.Err())
			}
		}
	}
	if serveErr == server.ErrServerUsed && ctx.Err() != nil {
		// Stop won before this fresh server reserved Serve; it never ran.
		serveErr = nil
	}
	if shutdownErr != nil {
		shutdownErr = fmt.Errorf("shutdown: %w", shutdownErr)
	}
	return errors.Join(serveErr, shutdownErr)
}
