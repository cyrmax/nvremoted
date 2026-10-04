// Copyright © 2023 Niko Carpenter <niko@nikocarpenter.com>
//
// This source code is governed by the MIT license, which can be found in the LICENSE file.

package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

// client represents a client on the server.
type client struct {
	capacity       *capacity
	startupDone    chan struct{}
	startupOnce    sync.Once
	rejected       atomic.Bool
	rejectionInput chan struct{}
	rejectionRole  string
	rejection      rejectionSchedule
	writeUntil     time.Time // protected by sendMTX; clips writes in rejection state
	overlap        bool
	overlapStart   chan time.Time
	lifecycleDone  chan struct{}
	id             uint64
	conn           net.Conn
	events         chan Message  // passes internal messages to a client
	recv           chan Message  // passes messages to a client from the network
	readNext       chan struct{} // Used by handleClient to ask readFromClient to read the next message
	channel        *channel      // active channel
	registry       *registry
	encoder        *json.Encoder
	sendMTX        sync.Mutex // Serializes the encoder and each write's deadline.
	writeTimeout   time.Duration
	stopMTX        sync.RWMutex // Protects stopped and stopReason
	stopped        bool
	stopReason     string
	log            *logrus.Logger
}

// serveClient handles events sent and received by a client.
func (srv *Server) serveClient(conn net.Conn, id uint64, remoteHost string) {
	// Allocate only minimal state until a physical pending permit is held.
	c := &client{id: id, conn: conn, log: srv.Log}
	srv.lifecycleMu.Lock()
	if srv.stopping {
		srv.lifecycleMu.Unlock()
		c.stop("Server shutdown")
		return
	}
	if srv.capacity == nil {
		var err error
		srv.capacity, err = newCapacity(srv.Admission)
		if err != nil {
			srv.lifecycleMu.Unlock()
			c.stop("Admission configuration error")
			return
		}
		srv.registry.lock.Lock()
		srv.registry.capacity = srv.capacity
		srv.registry.lock.Unlock()
	}
	c.capacity = srv.capacity
	_, isTLS := conn.(*tls.Conn)
	if !c.capacity.reserve(c, isTLS) {
		srv.lifecycleMu.Unlock()
		c.stop("Pre-join capacity exhausted")
		return
	}
	queueSize := srv.EventQueueSize
	if queueSize <= 0 {
		queueSize = defaultEventQueueSize
	}
	*c = client{
		capacity:       srv.capacity,
		startupDone:    make(chan struct{}),
		overlapStart:   make(chan time.Time, 1),
		lifecycleDone:  make(chan struct{}),
		rejectionInput: make(chan struct{}, 1),
		id:             id,
		conn:           conn,
		events:         make(chan Message, queueSize),
		recv:           make(chan Message),
		readNext:       make(chan struct{}),
		registry:       &srv.registry,
		encoder:        json.NewEncoder(conn),
		log:            srv.Log,
		writeTimeout:   srv.WriteTimeout,
	}

	if srv.active == nil {
		srv.active = make(map[*client]struct{})
	}
	srv.active[c] = struct{}{}
	srv.clients.Add(1)
	srv.lifecycleMu.Unlock()

	// Wait for both goroutines before removing channel and registry state.
	finished := make(chan struct{}, 2)

	srv.Log.WithFields(logrus.Fields{
		"id":          id,
		"remote_host": remoteHost,
	}).Info("Client connected")

	go func() {
		defer func() {
			c.capacity.release(c, time.Now())
			srv.lifecycleMu.Lock()
			delete(srv.active, c)
			srv.lifecycleMu.Unlock()
			srv.clients.Done()
		}()
		defer func() {
			c.capacity.cleaning(c)
			c.leaveChannel()
			conn.Close()
			c.stopMTX.RLock()
			reason := c.stopReason
			c.stopMTX.RUnlock()
			srv.Log.WithFields(logrus.Fields{
				"id":          id,
				"remote_host": remoteHost,
				"reason":      reason,
			}).Info("Client disconnected")
		}()

		if tlsConn, ok := conn.(*tls.Conn); ok {
			timeout := srv.TLSHandshakeTimeout
			if timeout <= 0 {
				timeout = defaultTLSHandshakeTimeout
			}
			// Keep handshake I/O out of the serial accept loop and ahead of
			// protocol reads/writes. Context cancellation closes the transport
			// during handshake; after success it cannot affect the connection.
			// No socket deadline is installed, so idle clients remain valid.
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			err := tlsConn.HandshakeContext(ctx)
			cancel()
			if err != nil {
				reason := "TLS handshake error"
				if err == context.DeadlineExceeded {
					reason = "TLS handshake timeout"
				}
				c.stop(reason)
				srv.Log.WithFields(logrus.Fields{
					"id":    id,
					"error": err,
				}).Debug("TLS handshake failed")
				return
			}
		}

		if isTLS && !c.capacity.established(c) {
			c.stop("Startup capacity exhausted")
			return
		}
		startupFinished := make(chan struct{})
		go c.watchStartup(startupFinished)
		overlapFinished := make(chan struct{})
		go c.watchOverlap(overlapFinished)
		defer func() { close(c.lifecycleDone); <-overlapFinished }()
		defer func() { c.finishStartup(); <-startupFinished }()
		go srv.readFromClient(c, finished)
		go srv.handleClient(c, finished)
		// Wait for both readFromClient and handleClient to finish.
		<-finished
		<-finished
	}()
}

// leaveChannel runs after both client goroutines have finished. Keep receiving
// events until membership cleanup completes. The queue is never closed, so even
// a registry snapshot retained across cleanup can enqueue safely.
// handleClient terminates via recv. Retained members hold the client through
// their stop callback until the snapshot or notification is released.
func (c *client) leaveChannel() {
	if c.channel == nil {
		return
	}
	left := make(chan struct{})
	go func() {
		c.channel.leave(c.id)
		close(left)
	}()
	for {
		select {
		case <-c.events:
		case <-left:
			return
		}
	}
}

// readFromClient reads data from the client socket, marshals it, and sends the resulting clientMessage to the client's events channel to be handled.
func (srv *Server) readFromClient(c *client, finished chan<- struct{}) {
	defer func() {
		close(c.recv)
		finished <- struct{}{}
	}()

	// NVDA Remote clients do not acknowledge server pings. Silence is valid;
	// transport errors detect dead peers, and stop closes the socket to unblock us.
	reader := newMessageReader(c.conn, srv.MaxMessageSize)

	for !c.isStopped() {
		if c.rejected.Load() {
			var buffer [4096]byte
			for !c.isStopped() {
				n, err := reader.Read(buffer[:])
				if n > 0 {
					c.noteRejectedInput()
				}
				if err != nil {
					c.stop("Rejected client disconnected")
					return
				}
			}
			return
		}
		raw, err := reader.read()
		var msg Message
		if err == nil {
			msg, err = unmarshalClientMessage(c.id, raw)
		}
		// handleClient could have finished while the above read was blocking.
		if err == nil {
			c.recv <- msg
			// Sending the unmarshaled message to handleClient might cause the client to be kicked.
			// But there would be no wayfor this goroutine to know that until the next read operation unblocks.
			// By waiting for handleClient to signal that it has finished processing the message,
			// we are able to see if the client was stopped before trying to read from the socket again.
			<-c.readNext
			continue
		}

		if c.isStopped() {
			return
		}
		if err == io.EOF {
			c.stop("Client disconnected")
			return
		}
		if err == errMessageTooLarge {
			// Close immediately: an unread/malicious peer must not delay stop
			// behind the serialized response write path.
			c.stop("Incoming message too large")
			return
		}
		if _, ok := err.(*json.UnmarshalTypeError); ok {
			c.sendError("malformed message")
			c.stop("client sent a malformed request")
			return
		}
		srv.Log.WithFields(logrus.Fields{
			"id":    c.id,
			"error": err,
		}).Warn("Error unmarshaling message from client")
		c.stop("Receive error")
		return
	}
}

// handleClient handles events sent on the client's events channel, serializes outgoing messages, and sends them to the client.
func (srv *Server) handleClient(c *client, finished chan<- struct{}) {
	defer func() { finished <- struct{}{} }()
	// Send the MOTD when the client connects
	if srv.MOTD != "" {
		c.send(ClientMOTDResponse{
			Type: "motd",
			MOTD: srv.MOTD,
		})
	}
	repeat := time.NewTimer(time.Hour)
	repeat.Stop()
	defer repeat.Stop()
	var repeatCH <-chan time.Time
	updateRejection := func() bool {
		now := time.Now()
		if !now.Before(c.rejection.deadline) {
			c.stop("Capacity rejection completed")
			return false
		}
		if c.rejection.announce(now) {
			c.announceCapacity()
		}
		if !repeat.Stop() {
			select {
			case <-repeat.C:
			default:
			}
		}
		repeat.Reset(c.rejection.next(time.Now()))
		repeatCH = repeat.C
		return true
	}
	for {
		select {
		case <-repeatCH:
			if !updateRejection() {
				repeatCH = nil
			}
		case <-c.rejectionInput:
			c.rejection.input = true
			if !updateRejection() {
				repeatCH = nil
			}
		case msg, ok := <-c.recv:
			if !ok {
				return
			}
			if c.isStopped() {
				c.readNext <- struct{}{}
				continue
			}
			if handlerFunc := clientMessageHandlers[msg.Name()]; handlerFunc == nil {
				c.log.WithFields(logrus.Fields{"id": c.id, "message_name": msg.Name()}).Warn("No handler found for client message")
				c.sendInternalError()
				c.stop("internal error")
			} else {
				handlerFunc(c, msg)
			}
			if c.rejected.Load() {
				repeat.Reset(c.rejection.next(time.Now()))
				repeatCH = repeat.C
			}
			c.readNext <- struct{}{}
		case msg := <-c.events:
			if c.rejected.Load() {
				continue
			}
			if handlerFunc := clientEventHandlers[msg.Name()]; handlerFunc == nil {
				c.log.WithFields(logrus.Fields{"id": c.id, "message_name": msg.Name()}).Warn("No handler found for client event")
				c.sendInternalError()
				c.stop("internal error")
			} else {
				handlerFunc(c, msg)
			}
		}
	}
}

// stop stops a client with the specified reason
// This method is safe to use concurrently.
func (c *client) stop(reason string) {
	c.stopMTX.Lock()
	if c.stopped {
		c.stopMTX.Unlock()
		return
	}
	c.stopped = true
	c.stopReason = reason
	c.stopMTX.Unlock()

	// Wake blocked reads and writes so cleanup does not need an idle timeout.
	// A backpressure disconnect must not wait for TLS close_notify delivery.
	if conn, ok := c.conn.(*tls.Conn); ok {
		conn.NetConn().Close()
	}
	c.conn.Close()
}

// isStopped checks to see if a client is stopped.
// This method is safe to use concurrently.
func (c *client) isStopped() bool {
	var stopped bool
	c.stopMTX.RLock()
	stopped = c.stopped
	c.stopMTX.RUnlock()
	return stopped
}

func (c *client) send(resp Message) {
	c.sendMTX.Lock()
	defer c.sendMTX.Unlock()
	if c.isStopped() {
		return
	}
	timeout := c.writeTimeout
	if timeout <= 0 {
		timeout = defaultWriteTimeout
	}
	deadline := time.Now().Add(timeout)
	if !c.writeUntil.IsZero() && c.writeUntil.Before(deadline) {
		deadline = c.writeUntil
	}
	err := c.conn.SetWriteDeadline(deadline)
	if err == nil {
		err = c.encoder.Encode(resp)
	}
	if err == nil {
		// Do not leave an expired deadline on an idle connection: TLS may also
		// write control records while reading from the peer.
		err = c.conn.SetWriteDeadline(time.Time{})
	}
	if err != nil {
		c.log.WithFields(logrus.Fields{
			"id":    c.id,
			"error": err,
		}).Warn("Error sending response to client")
		// In particular, a TLS write timeout makes further writes unsafe.
		c.stop("Send error")
	}
}

func (c *client) sendError(reason string) {
	c.send(ClientErrorResponse{
		Type:  "error",
		Error: reason,
	})
}

func (c *client) sendInternalError() {
	c.sendError("internal error")
}

func unmarshalClientMessage(id uint64, raw []byte) (Message, error) {
	// The raw JSON needs to be stored, because it will be unmarshalled twice,
	// first to a GenericClientMessage to get its type, then to the more specific Message type.
	// All returned messages will implement clientMessage, except for those of type message.ChannelMessage.
	var genericMSG GenericClientMessage
	if err := json.Unmarshal(raw, &genericMSG); err != nil {
		return nil, err
	}

	// If genericMSG.Type corresponds to a known clientMessage,
	// msgFunc will return a new empty message of that type into which the JSON will be unmarshalled.
	msgFunc := clientMessages[genericMSG.Type]
	var msg Message
	var err error
	if msgFunc == nil {
		// There is no clientMessage with the specified type.
		// Because the NVDA Remote protocol allows arbitrary messages to be sent on channels,
		// the JSON needs to be marshalled into a map.
		m := make(map[string]interface{})
		err = json.Unmarshal(raw, &m)
		msg = &channelMessage{
			origin: id,
			msg:    m,
		}
	} else {
		msg = msgFunc()
		err = json.Unmarshal(raw, &msg)
	}

	if err != nil {
		return nil, err
	}

	return msg, nil
}
