package server

import (
	"encoding/json"
	"net"
	"testing"
	"time"
)

type capacityPeer struct {
	net.Conn
	decoder *json.Decoder
}

func newCapacityPeer(t *testing.T, srv *Server, id uint64) *capacityPeer {
	t.Helper()
	conn, peer := net.Pipe()
	if err := peer.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	srv.serveClient(conn, id, "test")
	t.Cleanup(func() { peer.Close() })
	return &capacityPeer{peer, json.NewDecoder(peer)}
}
func (p *capacityPeer) send(t *testing.T, msg string) {
	t.Helper()
	if _, err := p.Write([]byte(msg)); err != nil {
		t.Fatal(err)
	}
}
func (p *capacityPeer) read(t *testing.T) map[string]interface{} {
	t.Helper()
	var m map[string]interface{}
	if err := p.decoder.Decode(&m); err != nil {
		t.Fatal(err)
	}
	return m
}
func capacityWireServer(t *testing.T, configure ...func(*Server)) *Server {
	t.Helper()
	srv := backpressureServer()
	srv.Admission = AdmissionConfig{HardLimit: 12, PendingLimit: 4, TLSLimit: 2, StartupLimit: 4, AdmittedLimit: 8, RejectionWindow: 100 * time.Millisecond, RejectionInterval: 20 * time.Millisecond}
	for _, f := range configure {
		f(srv)
	}
	if err := srv.beginRun(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.finishRun)
	return srv
}

func TestCapacityWireComplementAndHealthyRelay(t *testing.T) {
	for _, first := range []string{"master", "slave"} {
		t.Run(first, func(t *testing.T) {
			srv := capacityWireServer(t)
			p := newCapacityPeer(t, srv, 1)
			p.send(t, `{"type":"join","channel":"protected-by-admission","connection_type":"`+first+`"}`)
			if p.read(t)["type"] != "channel_joined" {
				t.Fatal("first rejected")
			}
			for i := 2; i <= 4; i++ {
				q := newCapacityPeer(t, srv, uint64(i))
				q.send(t, `{"type":"join","channel":"attacker`+string(rune('0'+i))+`","connection_type":"master"}`)
				if q.read(t)["type"] != "channel_joined" {
					t.Fatal("ordinary rejected early")
				}
			}
			r := newCapacityPeer(t, srv, 5)
			r.send(t, `{"type":"join","channel":"random","connection_type":"master"}`)
			if r.read(t)["type"] != "speak" {
				t.Fatal("missing capacity speech")
			}
			second := "slave"
			if first == "slave" {
				second = "master"
			}
			q := newCapacityPeer(t, srv, 6)
			q.send(t, `{"type":"join","channel":"protected-by-admission","connection_type":"`+second+`"}`)
			if q.read(t)["type"] != "channel_joined" || p.read(t)["type"] != "client_joined" {
				t.Fatal("complement rejected")
			}
			p.send(t, `{"type":"speak","sequence":["healthy"]}`)
			if q.read(t)["type"] != "speak" {
				t.Fatal("healthy relay blocked")
			}
		})
	}
}

func TestCapacityRejectionWireAndMOTD(t *testing.T) {
	srv := capacityWireServer(t, func(srv *Server) { srv.MOTD = "Independent maintenance notice" })
	p := newCapacityPeer(t, srv, 1)
	if m := p.read(t); m["type"] != "motd" || m["motd"] != srv.MOTD || m["force_display"] != false {
		t.Fatal(m)
	}
	p.send(t, `{"type":"join","channel":"a","connection_type":"master"}`)
	p.read(t)
	for i := 2; i <= 4; i++ {
		q := newCapacityPeer(t, srv, uint64(i))
		q.read(t)
		q.send(t, `{"type":"join","channel":"`+string(rune('a'+i))+`","connection_type":"master"}`)
		q.read(t)
	}
	r := newCapacityPeer(t, srv, 5)
	r.read(t)
	r.send(t, `{"type":"join","channel":"a","connection_type":"master"}`)
	m := r.read(t)
	if m["type"] != "speak" || m["origin"] != nil || m["sequence"].([]interface{})[0] != capacityAnnouncement {
		t.Fatal(m)
	}
	if r.read(t)["type"] != "error" {
		t.Fatal("missing diagnostic")
	}
	r.send(t, "     nonsense input")
	count := 1
	for {
		var m map[string]interface{}
		err := r.decoder.Decode(&m)
		if err != nil {
			break
		}
		if m["type"] != "speak" {
			t.Fatal("false membership or MOTD", m)
		}
		count++
	}
	if count < 4 || count > 20 {
		t.Fatalf("announcements=%d", count)
	}
	if srv.MOTD != "Independent maintenance notice" {
		t.Fatal("MOTD changed")
	}
	if stats := srv.registry.Stats(); stats.NumClients != 4 {
		t.Fatalf("fake membership: %+v", stats)
	}
}

func TestCapacityStartupAbsoluteDeadlineAndJoinedIdle(t *testing.T) {
	srv := capacityWireServer(t, func(srv *Server) { srv.Admission.StartupTimeout = 40 * time.Millisecond })
	joined := newCapacityPeer(t, srv, 1)
	joined.send(t, `{"type":"join","channel":"idle","connection_type":"slave"}`)
	joined.read(t)
	stalled := newCapacityPeer(t, srv, 2)
	stalled.send(t, `{"type":"protocol_version","version":2}`)
	var m map[string]interface{}
	if err := stalled.decoder.Decode(&m); err == nil {
		t.Fatal("startup not closed")
	}
	joined.send(t, `{"type":"protocol_version","version":2}`)
	if joinedStats := srv.registry.Stats(); joinedStats.NumClients != 1 {
		t.Fatal(joinedStats)
	}
}
