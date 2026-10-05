//go:build performance

package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
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
