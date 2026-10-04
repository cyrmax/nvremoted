// Copyright © 2023 Niko Carpenter <niko@nikocarpenter.com>
//
// This source code is governed by the MIT license, which can be found in the LICENSE file.

// Package server implements an NVDA Remote server.
package server

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"crypto/tls"
)

// Server contains state for one NVRemoted server run. Configure it before use;
// it must not be copied or served again, even after the first run has ended.
type Server struct {
	// Admission defines finite physical limits and backed channel entitlements.
	// Zero fields select server policy defaults. Configure before serving.
	Admission AdmissionConfig
	capacity  *capacity
	// MaxMessageSize bounds the encoded bytes of each incoming JSON value,
	// including whitespace inside the value but excluding whitespace between
	// values. Nonpositive values use 4 MiB. Oversize closes the connection
	// without forwarding the value or sending a protocol response.
	MaxMessageSize int

	// EventQueueSize bounds pending events per client (in addition to the event
	// being handled). Nonpositive values use 64, allowing short bursts without
	// accumulating an unbounded backlog. Overflow disconnects the client; no
	// message types are dropped or coalesced on a continuing connection.
	EventQueueSize int

	// WriteTimeout bounds each response write, independently of pings and TCP
	// keepalive. Nonpositive values use 10 seconds, tolerating transient network
	// stalls even when there are no more events. This is a socket write deadline,
	// not a timeout for TLS handshake reads or the entire send operation.
	WriteTimeout time.Duration

	// TLSHandshakeTimeout bounds TLS session establishment before protocol I/O.
	// Nonpositive values use 10 seconds. It is independent of WriteTimeout,
	// pings and TCP keepalive, and does not limit application idle time.
	TLSHandshakeTimeout time.Duration

	// TimeBetweenPings specifies the amount of time that will elapse before clients will be sent a ping.
	// If 0, no pings will be sent.
	TimeBetweenPings time.Duration

	// PingsUntilTimeout is retained for compatibility with existing configurations.
	// Deprecated: ignored because NVDA Remote clients do not acknowledge pings.
	// Idle connections are allowed; TCP keepalive detects dead peers independently.
	PingsUntilTimeout int

	// TLSConfig optionally provides a TLS configuration for use by ListenAndServeTLS.
	TLSConfig *tls.Config

	// MOTD contains the message of the day, which will be sent to clients when connecting.
	MOTD string

	// StatsPassword sets the password for retreiving stats.
	StatsPassword string

	Log *logrus.Logger

	// registry stores information about clients and channels on the server.
	registry registry

	lifecycleMu sync.Mutex
	started     bool
	stopping    bool
	stop        chan struct{}
	done        chan struct{}
	active      map[*client]struct{}
	clients     sync.WaitGroup
	shutdownErr error
}

// ErrServerUsed means a serving method was called on an already used or
// shut down Server. Create another Server for a restart.
var ErrServerUsed = errors.New("Server has already been used or shut down")

// ListenAndServe listens for connections on the network, and connects them to the NVDA Remote server.
func (srv *Server) ListenAndServe(addr string) error {
	if err := srv.beginRun(); err != nil {
		return err
	}
	defer srv.finishRun()
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Wrap(err, "Listen")
	}
	defer listener.Close()

	srv.Log.WithFields(logrus.Fields{
		"addr":        addr,
		"tls_enabled": false,
	}).Info("Listening for incoming connections")
	return srv.serve(listener)
}

// ListenAndServeTLS behaves just like ListenAndServe, but wraps the connection with TLS.
func (srv *Server) ListenAndServeTLS(addr, certFile, keyFile string) error {
	if err := srv.beginRun(); err != nil {
		return err
	}
	defer srv.finishRun()
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return errors.Wrap(err, "Load X.509 key pair")
		}
		srv.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	}
	if srv.TLSConfig == nil {
		return errors.New("No TLSConfig set in server, and no certFile/keyFile given")
	}

	listener, err := tls.Listen("tcp", addr, srv.TLSConfig)
	if err != nil {
		return errors.Wrap(err, "Listen TLS")
	}
	defer listener.Close()

	srv.Log.WithFields(logrus.Fields{
		"addr":        addr,
		"tls_enabled": true,
	}).Info("Listening for incoming connections")
	return srv.serve(listener)
}

func (srv *Server) acceptClients(listener net.Listener) error {
	return srv.acceptClientsWithRetry(listener, func(delay time.Duration) {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-srv.stop:
		}
	})
}

// acceptClientsWithRetry keeps the retry wait injectable for deterministic tests.
// No extra goroutine or timer survives an accept retry. Closing a listener during
// the wait is observed by the next Accept, after at most one second.
func (srv *Server) acceptClientsWithRetry(listener net.Listener, wait func(time.Duration)) error {
	var nextID uint64
	var retryDelay time.Duration
	for {
		if srv.isStopping() {
			return net.ErrClosed
		}
		// Gate the serial acceptor before acquiring another socket. Kernel
		// backlog sockets are not server-owned; no overflow socket can push
		// physical ownership above the configured ceiling, even briefly.
		srv.lifecycleMu.Lock()
		budget, stop := srv.capacity, srv.stop
		srv.lifecycleMu.Unlock()
		if budget != nil {
			ready, changed := budget.acceptReady()
			if !ready {
				select {
				case <-changed:
				case <-stop:
					return net.ErrClosed
				}
				continue
			}
		}
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				srv.Log.Debug("Listener closed")
				return errors.Wrap(err, "Accept")
			}
			// Retry only known resource shortages, not the deprecated and
			// ambiguous net.Error.Temporary classification. TCP listeners
			// already handle interrupted/aborted connections internally.
			if isAcceptResourceError(err) {
				if retryDelay == 0 {
					srv.Log.WithError(err).Warn("Accept resource exhaustion; retrying")
					retryDelay = 5 * time.Millisecond
				} else {
					retryDelay *= 2
					if retryDelay > time.Second {
						retryDelay = time.Second
					}
				}
				wait(retryDelay)
				continue
			}
			srv.Log.WithError(err).Error("Error accepting connection")
			return errors.Wrap(err, "Accept")
		}
		if retryDelay != 0 {
			srv.Log.Debug("Accept resumed after resource exhaustion")
			retryDelay = 0
		}
		if err := configureTCPKeepAlive(conn); err != nil {
			srv.Log.WithError(err).Warn("Error configuring TCP keepalive")
		}

		// Peer identity is diagnostic only. Keep DNS work out of admission and
		// retain the address even for listeners without host:port addresses.
		remoteHost := conn.RemoteAddr().String()
		if host, _, err := net.SplitHostPort(remoteHost); err == nil {
			remoteHost = host
		}
		srv.serveClient(conn, nextID, remoteHost)
		nextID++
	}
}

// Serve runs the server once. An accept failure stops all accepted sessions and
// waits for their cleanup before returning the wrapped accept error.
// The caller owns listener: Serve does not close it on an accept failure.
// An explicit Shutdown closes it to unblock Accept; Serve then returns nil.
// All serving methods share the single-run restriction, including failed starts.
func (srv *Server) Serve(listener net.Listener) error {
	if err := srv.beginRun(); err != nil {
		return err
	}
	defer srv.finishRun()
	return srv.serve(listener)
}

func (srv *Server) beginRun() error {
	srv.lifecycleMu.Lock()
	defer srv.lifecycleMu.Unlock()
	if srv.started || srv.stopping {
		return ErrServerUsed
	}
	srv.started = true
	var err error
	srv.capacity, err = newCapacity(srv.Admission)
	if err != nil {
		return err
	}
	srv.stop = make(chan struct{})
	srv.done = make(chan struct{})
	srv.active = make(map[*client]struct{})
	now := time.Now()
	srv.registry.lock.Lock()
	srv.registry.clients = make(map[uint64]channelMember)
	srv.registry.channels = make(map[string]*channel)
	srv.registry.statsPassword = srv.StatsPassword
	srv.registry.capacity = srv.capacity
	srv.registry.createdTime = now
	srv.registry.maxChannelsTime = now
	srv.registry.maxClientsTime = now
	srv.registry.lock.Unlock()
	return nil
}

func (srv *Server) isStopping() bool {
	srv.lifecycleMu.Lock()
	defer srv.lifecycleMu.Unlock()
	return srv.stopping
}

func (srv *Server) stopRun() {
	srv.lifecycleMu.Lock()
	defer srv.lifecycleMu.Unlock()
	if !srv.stopping {
		srv.stopping = true
		if srv.capacity != nil {
			srv.capacity.stopAdmission()
		}
		close(srv.stop)
	}
}

// Shutdown stops admission and periodic pings, closes the serving listener and
// every accepted transport (including idle clients and TLS handshakes), and
// waits for accept, client, and channel cleanup and disconnect logging.
// It does not wait for peers to voluntarily leave or flush queued messages.
// The context bounds only this caller's wait. On cancellation it returns
// ctx.Err(); teardown continues, and a later Shutdown can wait for completion.
// Successful calls are idempotent. Before serving, it permanently shuts down
// the unused Server. Listener close errors are returned after cleanup; client
// close errors retain their existing diagnostic behavior and are not aggregated.
// Custom transports and logging hooks must allow teardown to finish.
func (srv *Server) Shutdown(ctx context.Context) error {
	srv.lifecycleMu.Lock()
	if srv.done == nil {
		srv.stop = make(chan struct{})
		srv.done = make(chan struct{})
		srv.stopping = true
		close(srv.stop)
		close(srv.done)
	} else if !srv.stopping {
		srv.stopping = true
		if srv.capacity != nil {
			srv.capacity.stopAdmission()
		}
		close(srv.stop)
	}
	done := srv.done
	srv.lifecycleMu.Unlock()
	// Prefer the completed result even with an already canceled context.
	select {
	case <-done:
		return srv.shutdownErr
	default:
	}
	select {
	case <-done:
		return srv.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (srv *Server) finishRun() {
	srv.stopRun()
	// Accept has ended before this point, and stopping prevents new admissions.
	srv.lifecycleMu.Lock()
	active := make([]*client, 0, len(srv.active))
	for c := range srv.active {
		active = append(active, c)
	}
	srv.lifecycleMu.Unlock()
	// Close independently: a slow/custom transport must not delay closing peers.
	var stops sync.WaitGroup
	for _, c := range active {
		stops.Add(1)
		go func(c *client) {
			defer stops.Done()
			c.stop("Server shutdown")
		}(c)
	}
	stops.Wait()
	srv.clients.Wait()
	// Client cleanup acknowledges channel removal before channel.start returns.
	srv.registry.workers.Wait()
	srv.capacity.shutdown()
	close(srv.done)
}

func (srv *Server) serve(listener net.Listener) error {
	expiry := time.NewTimer(time.Hour)
	defer expiry.Stop()
	var expiryCH <-chan time.Time
	resetExpiry := func() {
		if !expiry.Stop() {
			select {
			case <-expiry.C:
			default:
			}
		}
		next := srv.capacity.nextExpiry()
		if next.IsZero() {
			expiryCH = nil
			return
		}
		expiry.Reset(time.Until(next))
		expiryCH = expiry.C
	}
	srv.Log.WithFields(logrus.Fields{
		"time_between_pings":  srv.TimeBetweenPings,
		"pings_until_timeout": srv.PingsUntilTimeout,
	}).Info("Server started")
	acceptDone := make(chan error, 1)
	go func() { acceptDone <- srv.acceptClients(listener) }()

	var pingsCH <-chan time.Time
	if srv.TimeBetweenPings > 0 {
		ticker := time.NewTicker(srv.TimeBetweenPings)
		defer ticker.Stop()
		pingsCH = ticker.C
	}
	for {
		select {
		case <-srv.capacity.changed:
			resetExpiry()
		case now := <-expiryCH:
			srv.capacity.expire(now)
			resetExpiry()
		case err := <-acceptDone:
			if srv.isStopping() {
				srv.closeListener(listener)
				if errors.Is(err, net.ErrClosed) {
					return nil
				}
			}
			return err
		case <-srv.stop:
			srv.closeListener(listener)
			err := <-acceptDone
			if err != nil && !errors.Is(err, net.ErrClosed) {
				return err
			}
			return nil
		case <-pingsCH:
			if !srv.isStopping() {
				srv.dispatchPings()
			}
		}
	}
}

func (srv *Server) closeListener(listener net.Listener) {
	if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		srv.shutdownErr = errors.Wrap(err, "Close listener")
	}
}

const (
	defaultEventQueueSize      = 64
	defaultWriteTimeout        = 10 * time.Second
	defaultTLSHandshakeTimeout = 10 * time.Second
)

// Snapshot under the registry lock, then deliver without holding it. Queues
// remain open throughout cleanup, so a retained member is safe to enqueue to.
func (srv *Server) dispatchPings() {
	srv.registry.lock.RLock()
	members := make([]channelMember, 0, len(srv.registry.clients))
	for _, member := range srv.registry.clients {
		members = append(members, member)
	}
	srv.registry.lock.RUnlock()
	for _, member := range members {
		if srv.isStopping() {
			return
		}
		member.enqueue(pingMessage{})
	}
}

type pingMessage struct{}

func (pingMessage) Name() string {
	return "ping"
}
