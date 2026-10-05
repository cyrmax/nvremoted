//go:build performance

package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/n0ot/nvremoted/internal/perf"
)

// relayPerfListener creates all key material before the measured interval. The
// client verifies the server leaf against this isolated in-memory root CA.
func relayPerfListener(transport string) (net.Listener, *tls.Config, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	if transport == "tcp" {
		return listener, nil, nil
	}
	if transport != "tls" {
		listener.Close()
		return nil, nil, fmt.Errorf("unknown transport %q", transport)
	}
	rootKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		listener.Close()
		return nil, nil, err
	}
	rootTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "NVRemoted local performance root"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	rootDER, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate, &rootKey.PublicKey, rootKey)
	if err != nil {
		listener.Close()
		return nil, nil, err
	}
	root, err := x509.ParseCertificate(rootDER)
	if err != nil {
		listener.Close()
		return nil, nil, err
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		listener.Close()
		return nil, nil, err
	}
	leafTemplate := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, root, &leafKey.PublicKey, rootKey)
	if err != nil {
		listener.Close()
		return nil, nil, err
	}
	leaf := tls.Certificate{Certificate: [][]byte{leafDER, rootDER}, PrivateKey: leafKey}
	roots := x509.NewCertPool()
	roots.AddCert(root)
	clientTLS := &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	// Return the wrapped listener through a forwarding wrapper while preserving
	// the original localhost address for dialing.
	return &relayPerfTLSListener{Listener: listener, tlsConfig: &tls.Config{Certificates: []tls.Certificate{leaf}, MinVersion: tls.VersionTLS12}}, clientTLS, nil
}

type relayPerfTLSListener struct {
	net.Listener
	tlsConfig *tls.Config
}

func (l *relayPerfTLSListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return tls.Server(conn, l.tlsConfig), nil
}

func relayPerfDial(addr string, clientTLS *tls.Config) (net.Conn, error) {
	dialer := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	if clientTLS == nil {
		return conn, nil
	}
	tlsConn := tls.Client(conn, clientTLS.Clone())
	if err := tlsConn.Handshake(); err != nil {
		conn.Close()
		return nil, err
	}
	return tlsConn, nil
}

func relayPerfSmokeConfig() perf.Config {
	return perf.Config{Duration: 80 * time.Millisecond, Warmup: 20 * time.Millisecond, Drain: 500 * time.Millisecond, Rate: 250, MaxInFlight: 64}
}

func TestPerformanceRelaySmoke(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec perf.Scenario
	}{
		{"open-tcp", perf.Scenario{Name: "smoke-open-tcp", Kind: "open", Transport: "tcp", Payload: "key", Channels: 1, Fanout: 2, Rate: 250}},
		{"closed-tls", perf.Scenario{Name: "smoke-closed-tls", Kind: "closed", Transport: "tls", Payload: "control", Channels: 1, Fanout: 1}},
		{"open-multichannel", perf.Scenario{Name: "smoke-open-multichannel", Kind: "open", Transport: "tcp", Payload: "arbitrary-nested", Channels: 2, Fanout: 2, Rate: 150}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := runRelayScenario(relayPerfSmokeConfig(), tc.spec)
			if err != nil {
				t.Fatal(err)
			}
			if res.Sent == 0 || res.Received == 0 || res.Missing != 0 || res.Expected != res.Received || res.Duplicates != 0 || res.OutOfOrder != 0 || res.Corrupted != 0 || res.Unexpected != 0 || res.Disconnects != 0 || res.ServerErrors != 0 {
				t.Fatalf("unexpected delivery summary: sent=%d received=%d missing=%d", res.Sent, res.Received, res.Missing)
			}
		})
	}
}

func TestPerformanceRelayShutdownCleanup(t *testing.T) {
	for _, transport := range []string{"tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			for i := 0; i < 3; i++ {
				res, err := runRelayScenario(perf.Config{Duration: 20 * time.Millisecond, Warmup: 0, Drain: 200 * time.Millisecond, Rate: 50, MaxInFlight: 8}, perf.Scenario{Name: "shutdown", Kind: "closed", Transport: transport, Payload: "key", Channels: 1, Fanout: 1})
				if err != nil {
					t.Fatal(err)
				}
				if res.Missing != 0 || res.Expected != res.Received || res.Corrupted != 0 || res.Disconnects != 0 || res.ServerErrors != 0 {
					t.Fatalf("invalid shutdown delivery summary: sent=%d received=%d expected=%d missing=%d corrupted=%d", res.Sent, res.Received, res.Expected, res.Missing, res.Corrupted)
				}
			}
		})
	}
}

func TestPerformanceRelayPayloadComparison(t *testing.T) {
	newValues := func() (map[string]any, map[string]any) {
		expected := map[string]any{
			"bench_seq": json.Number("0"),
			"type":      "arbitrary",
			"nullable":  nil,
			"enabled":   true,
			"count":     json.Number("12"),
			"text":      "sample",
			"nested": map[string]any{
				"values": []any{json.Number("1"), nil, false, "x"},
				"inner":  map[string]any{"active": true, "name": "node", "nullable": nil},
			},
		}
		received := relayPerfCloneJSON(expected).(map[string]any)
		received["bench_seq"] = json.Number("7")
		received["origin"] = json.Number("0") // Server client IDs legitimately start at zero.
		return expected, received
	}

	tests := []struct {
		name   string
		mutate func(map[string]any)
		want   bool
	}{
		{name: "matching nested JSON", want: true},
		{name: "missing top-level null replaced by extra null", mutate: func(m map[string]any) {
			delete(m, "nullable")
			m["extra"] = nil
		}},
		{name: "extra top-level field", mutate: func(m map[string]any) { m["extra"] = true }},
		{name: "null differs from boolean", mutate: func(m map[string]any) { m["nullable"] = false }},
		{name: "exact number spelling", mutate: func(m map[string]any) { m["count"] = json.Number("12.0") }},
		{name: "number type mismatch", mutate: func(m map[string]any) { m["count"] = float64(12) }},
		{name: "string type mismatch", mutate: func(m map[string]any) { m["text"] = []byte("sample") }},
		{name: "boolean type mismatch", mutate: func(m map[string]any) { m["enabled"] = "true" }},
		{name: "array order differs", mutate: func(m map[string]any) {
			nested := m["nested"].(map[string]any)
			nested["values"] = []any{nil, json.Number("1"), false, "x"}
		}},
		{name: "deep value differs", mutate: func(m map[string]any) {
			nested := m["nested"].(map[string]any)
			nested["inner"].(map[string]any)["name"] = "changed"
		}},
		{name: "missing nested null replaced by extra key", mutate: func(m map[string]any) {
			nested := m["nested"].(map[string]any)
			inner := nested["inner"].(map[string]any)
			delete(inner, "nullable")
			inner["extra"] = nil
		}},
		{name: "wrong sequence", mutate: func(m map[string]any) { m["bench_seq"] = json.Number("8") }},
		{name: "non-integer sequence", mutate: func(m map[string]any) { m["bench_seq"] = json.Number("7.0") }},
		{name: "missing origin", mutate: func(m map[string]any) { delete(m, "origin") }},
		{name: "unsupported JSON value fails closed", mutate: func(m map[string]any) {
			m["text"] = struct{ Value string }{"sample"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expected, received := newValues()
			if test.mutate != nil {
				test.mutate(received)
			}
			got := relayPerfPayloadMatches(expected, 7, received)
			if got != test.want {
				t.Fatalf("relayPerfPayloadMatches() = %v, want %v", got, test.want)
			}
		})
	}
	unsupported := struct{ Value string }{"same"}
	if relayPerfJSONEqual(unsupported, unsupported) {
		t.Fatal("unsupported non-JSON value compared equal")
	}

	expected, received := newValues()
	allocations := testing.AllocsPerRun(100, func() {
		if !relayPerfPayloadMatches(expected, 7, received) {
			t.Fatal("valid payload comparison failed")
		}
	})
	if allocations != 0 {
		t.Fatalf("payload comparison allocated %.2f times per call, want zero", allocations)
	}
}

func TestPerformanceRelayOriginValidation(t *testing.T) {
	for _, test := range []struct {
		name           string
		origin         any
		expectedOrigin uint64
		wantValid      bool
	}{
		{name: "zero client ID is valid", origin: json.Number("0"), expectedOrigin: 0, wantValid: true},
		{name: "matching client ID", origin: json.Number("5"), expectedOrigin: 5, wantValid: true},
		{name: "wrong client ID", origin: json.Number("6"), expectedOrigin: 5},
		{name: "missing client ID", origin: nil, expectedOrigin: 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload, received := relayPerfOriginFixture(test.origin)
			peer := &relayPerfPeer{index: 0}
			collector := newRelayPerfCollector([]*relayPerfPeer{peer}, 1)
			collector.start = perf.Now()
			collector.end = collector.start.Add(time.Second)
			collector.measurement = true
			collector.expectedOrigins[0] = test.expectedOrigin
			if !collector.reserve(7, collector.start, 0, 1, payload) {
				t.Fatal("could not reserve fixture message")
			}
			actual := collector.setActual(7)
			collector.markSent(7, 0, actual)
			collector.receive(0, 7, received, 1, perf.Now())
			if got := collector.corrupted == 0; got != test.wantValid {
				t.Fatalf("origin validation valid=%v, want %v", got, test.wantValid)
			}
		})
	}
}

func relayPerfOriginFixture(origin any) (map[string]any, map[string]any) {
	expected := map[string]any{"bench_seq": json.Number("0"), "type": "key"}
	received := map[string]any{"bench_seq": json.Number("7"), "type": "key"}
	if origin != nil {
		received["origin"] = origin
	}
	return expected, received
}

func relayPerfCloneJSON(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		clone := make(map[string]any, len(typed))
		for key, child := range typed {
			clone[key] = relayPerfCloneJSON(child)
		}
		return clone
	case []any:
		clone := make([]any, len(typed))
		for i, child := range typed {
			clone[i] = relayPerfCloneJSON(child)
		}
		return clone
	default:
		return typed
	}
}
