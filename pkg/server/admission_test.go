package server

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

type admissionConn struct {
	*handshakeConn
	addr net.Addr
}

func (c *admissionConn) RemoteAddr() net.Addr { return c.addr }

// These tests change DefaultResolver and must not run in parallel. The fake
// resolver never opens a socket; even a regression cannot reach external DNS.
func TestAdmissionDoesNotResolveDNS(t *testing.T) {
	for _, blocked := range []bool{true, false} {
		name := "failed"
		if blocked {
			name = "blocked"
		}
		t.Run(name, func(t *testing.T) {
			started := make(chan struct{}, 1)
			release := make(chan struct{})
			var calls atomic.Int32
			old := net.DefaultResolver
			net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(context.Context, string, string) (net.Conn, error) {
				calls.Add(1)
				select {
				case started <- struct{}{}:
				default:
				}
				if blocked {
					<-release
				}
				return nil, errors.New("controlled DNS failure")
			}}
			srv, log := handshakeServer(t)
			l := &handshakeListener{incoming: make(chan net.Conn), stop: make(chan struct{})}
			done := make(chan struct{})
			go func() { defer close(done); srv.acceptClients(l) }()
			t.Cleanup(func() {
				close(release)
				l.Close()
				awaitLivenessSignal(t, done)
				net.DefaultResolver = old
			})
			peers := make([]net.Conn, 2)
			for i := range peers {
				conn, peer := handshakePipe(t)
				peers[i] = peer
				select {
				case l.incoming <- &admissionConn{handshakeConn: conn, addr: &net.TCPAddr{IP: net.ParseIP("192.0.2.123"), Port: 4567}}:
				case <-started:
					t.Fatal("reverse DNS blocked accepting the next connection")
				case <-time.After(2 * time.Second):
					t.Fatal("accept did not advance to the next connection")
				}
				select {
				case <-conn.reads:
				case <-started:
					if blocked {
						t.Fatal("reverse DNS blocks admission before protocol lifecycle")
					}
					awaitLivenessSignal(t, conn.reads)
				case <-time.After(2 * time.Second):
					t.Fatal("accepted connection did not enter lifecycle")
				}
			}
			for _, peer := range peers {
				peer.Close()
			}
			connected, disconnected := 0, 0
			for disconnected < len(peers) {
				select {
				case e := <-log.entries:
					if e.Level <= logrus.WarnLevel {
						t.Fatalf("unexpected warning: %s", e.Message)
					}
					if e.Message == "Client connected" || e.Message == "Client disconnected" {
						if e.Data["remote_host"] != "192.0.2.123" {
							t.Fatalf("lost IP: %v", e.Data)
						}
						if e.Message == "Client connected" {
							connected++
						} else {
							disconnected++
						}
					}
				case <-time.After(2 * time.Second):
					t.Fatal("client cleanup did not finish")
				}
			}
			if connected != len(peers) {
				t.Fatalf("connected: %d", connected)
			}
			if calls.Load() != 0 {
				t.Fatalf("admission performed %d DNS calls", calls.Load())
			}
			assertLifecycleRegistryEmpty(t, &srv.registry)
		})
	}
}

func TestAdmissionPeerAddressLogs(t *testing.T) {
	for _, tc := range []struct {
		name string
		addr net.Addr
		want string
	}{
		{"ipv4", &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 1234}, "192.0.2.1"},
		{"ipv6", &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 1234}, "2001:db8::1"},
		{"ipv6_zone", &net.TCPAddr{IP: net.ParseIP("fe80::1"), Port: 1234, Zone: "test"}, "fe80::1%test"},
		{"non_tcp", &net.UnixAddr{Name: "test-peer", Net: "unix"}, "test-peer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, log := handshakeServer(t)
			l := &handshakeListener{incoming: make(chan net.Conn), stop: make(chan struct{})}
			done := make(chan struct{})
			go func() { defer close(done); srv.acceptClients(l) }()
			t.Cleanup(func() { l.Close(); awaitLivenessSignal(t, done) })
			conn, peer := handshakePipe(t)
			l.incoming <- &admissionConn{handshakeConn: conn, addr: tc.addr}
			awaitLivenessSignal(t, conn.reads)
			peer.Close()
			connected := false
			for {
				select {
				case e := <-log.entries:
					if e.Message != "Client connected" && e.Message != "Client disconnected" {
						continue
					}
					if e.Data["remote_host"] != tc.want {
						t.Fatalf("remote_host: %v, want %q", e.Data, tc.want)
					}
					if e.Message == "Client connected" {
						connected = true
					} else {
						if !connected {
							t.Fatal("missing connection log")
						}
						assertLifecycleRegistryEmpty(t, &srv.registry)
						return
					}
				case <-time.After(2 * time.Second):
					t.Fatal("client lifecycle did not finish")
				}
			}
		})
	}
}
