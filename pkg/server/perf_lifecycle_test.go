//go:build performance

package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/n0ot/nvremoted/internal/perf"
	"github.com/sirupsen/logrus"
)

// runLifecycleScenario measures real Server.Serve sessions over loopback. Its
// scenarios intentionally use finite batches: admission decisions and cleanup
// are reported separately, while pending/rejection batches capture held-resource
// snapshots without serially waiting through the five-second rejection window.
func runLifecycleScenario(cfg perf.Config, spec perf.Scenario) (measured perf.Result, retErr error) {
	defer func() {
		measured.SentInWindow = measured.Sent
		measured.ReceivedInWindow = measured.Received
		measured.SendBytesInWindow = measured.SendBytes
		measured.ReceiveBytesInWindow = measured.ReceiveBytes
	}()
	if spec.Kind == "boundary" {
		return runLifecycleBoundary(cfg, spec)
	}
	if spec.Kind != "lifecycle" {
		return perf.Result{}, fmt.Errorf("unsupported lifecycle scenario kind %q", spec.Kind)
	}

	serverCfg, custom := lifecycleAdmissionConfig(spec, cfg)
	fixture, err := newPerfLifecycleServer(spec.Transport, serverCfg)
	if err != nil {
		return perf.Result{}, err
	}
	defer func() {
		closeErr := fixture.close()
		measured.ServerErrors = fixture.logHook.serverErrors.Load()
		if measured.ServerErrors != 0 {
			measured.Valid = false
		}
		if fixture.logHook.disconnects.Load() > 0 {
			if measured.Observations == nil {
				measured.Observations = make(map[string]any)
			}
			measured.Observations["server_disconnect_logs"] = fixture.logHook.disconnects.Load()
		}
		if closeErr != nil {
			measured.Valid = false
			retErr = errors.Join(retErr, fmt.Errorf("close lifecycle fixture: %w", closeErr))
		}
	}()

	result := lifecycleResult(spec, fixture.server)
	result.ServerConfig["admission_policy"] = "DefaultAdmissionConfig"
	if custom {
		result.ServerConfig["admission_policy"] = "finite scenario-specific AdmissionConfig"
		result.Notes = append(result.Notes, "Scenario uses explicit finite admission limits shown in server_configuration; production defaults are not changed.")
	}
	clients := lifecycleClientCount(cfg, spec)
	measured = result
	switch spec.Payload {
	case "tcp-connect", "tls-handshake", "startup-join":
		measured, err = lifecycleConnectionScenario(cfg, spec, fixture, result, clients)
	case "pending-tls", "pending-startup":
		measured, err = lifecyclePendingScenario(cfg, spec, fixture, result, clients)
	case "ordinary", "protected-complementary", "recovery", "protected-reconnect":
		measured, err = lifecycleAdmissionScenario(cfg, spec, fixture, result, clients)
	case "capacity-reject", "reject-burst":
		measured, err = lifecycleRejectScenario(cfg, spec, fixture, result, clients)
	case "oversized-offender":
		measured, err = lifecycleOversizedScenario(cfg, spec, fixture, result, clients)
	default:
		return perf.Result{}, fmt.Errorf("unknown lifecycle scenario %q", spec.Payload)
	}
	measured.ServerErrors = fixture.logHook.serverErrors.Load()
	if measured.ServerErrors != 0 {
		measured.Valid = false
	}
	if fixture.logHook.disconnects.Load() > 0 {
		if measured.Observations == nil {
			measured.Observations = make(map[string]any)
		}
		measured.Observations["server_disconnect_logs"] = fixture.logHook.disconnects.Load()
		measured.Observations["server_queue_overflow_logs"] = fixture.logHook.overflow.Load()
	}
	return measured, err
}

func lifecycleAdmissionConfig(spec perf.Scenario, cfg perf.Config) (AdmissionConfig, bool) {
	policy := DefaultAdmissionConfig()
	switch spec.Payload {
	case "ordinary", "recovery", "startup-join", "tcp-connect", "tls-handshake", "pending-startup", "pending-tls", "oversized-offender":
		return policy, false
	case "protected-complementary":
		count := lifecycleClientCount(cfg, spec)
		if count > 300 {
			count = 300
		}
		policy.AdmittedLimit = perfMaxInt(policy.AdmittedLimit, count*3)
		for i := 0; i < count; i++ {
			policy.ProtectedChannels = append(policy.ProtectedChannels, channelDigest(fmt.Sprintf("perf-protected-%d", i)))
		}
		return policy, true
	case "protected-reconnect":
		count := lifecycleClientCount(cfg, spec)
		policy.AdmittedLimit = count * 3
		for i := 0; i < count; i++ {
			policy.ProtectedChannels = append(policy.ProtectedChannels, channelDigest(fmt.Sprintf("perf-protected-overlap-%d", i)))
		}
		return policy, true
	case "capacity-reject", "reject-burst":
		policy.AdmittedLimit = 2
		// Preserve the default five-second user-visible rejection window.
		return policy, true
	default:
		return policy, false
	}
}

type perfLifecycleServer struct {
	server    *Server
	listener  net.Listener
	address   string
	tls       bool
	done      chan error
	logHook   *relayPerfLogHook
	clientTLS *tls.Config
}

func newPerfLifecycleServer(transport string, admission AdmissionConfig) (*perfLifecycleServer, error) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	log := logrus.New()
	hook := &relayPerfLogHook{}
	log.AddHook(hook)
	server := &Server{Admission: admission, Log: log}
	server.Log.SetOutput(io.Discard)
	server.Log.SetLevel(logrus.InfoLevel)
	listener := net.Listener(base)
	isTLS := transport == "tls"
	if isTLS {
		config, err := perfServerTLSConfig()
		if err != nil {
			_ = base.Close()
			return nil, err
		}
		server.TLSConfig = config
		listener = tls.NewListener(base, config)
	} else if transport != "" && transport != "tcp" {
		_ = base.Close()
		return nil, fmt.Errorf("unsupported lifecycle transport %q", transport)
	}
	fixture := &perfLifecycleServer{server: server, listener: listener, address: base.Addr().String(), tls: isTLS, done: make(chan error, 1), logHook: hook}
	if isTLS {
		fixture.clientTLS, err = fixture.clientTLSConfig()
		if err != nil {
			_ = listener.Close()
			return nil, err
		}
	}
	go func() { fixture.done <- server.Serve(listener) }()
	readyUntil := perf.Now().Add(2 * time.Second)
	for perf.Now().Before(readyUntil) {
		server.lifecycleMu.Lock()
		started := server.started && server.capacity != nil
		server.lifecycleMu.Unlock()
		if started {
			return fixture, nil
		}
		time.Sleep(time.Millisecond)
	}
	_ = listener.Close()
	return nil, fmt.Errorf("local server did not start before timeout")
}

func perfServerTLSConfig() (*tls.Config, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}
	now := time.Now()
	template := x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost"}}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}

func (f *perfLifecycleServer) close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shutdownErr := f.server.Shutdown(ctx)
	select {
	case serveErr := <-f.done:
		if shutdownErr != nil {
			return shutdownErr
		}
		if serveErr != nil {
			return fmt.Errorf("server Serve exited with error: %w", serveErr)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("server cleanup timed out: %w", ctx.Err())
	}
}

type perfLifecyclePeer struct {
	lastFrameAt time.Time
	connectedAt time.Time
	conn        net.Conn
	read        *bufio.Reader
	write       *json.Encoder
}

func TestPerformanceLifecycleHarness(t *testing.T) {
	for _, transport := range []string{"tcp", "tls"} {
		t.Run(transport, func(t *testing.T) {
			fixture, err := newPerfLifecycleServer(transport, DefaultAdmissionConfig())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := fixture.close(); err != nil {
					t.Error(err)
				}
			})
			peer, err := fixture.dial(5*time.Second, true)
			if err != nil {
				t.Fatal(err)
			}
			defer peer.close()
			kind, err := peer.join("performance-smoke", "master")
			if err != nil {
				t.Fatal(err)
			}
			if kind != "channel_joined" {
				t.Fatalf("join response type = %q", kind)
			}
		})
	}
}

func (f *perfLifecycleServer) dial(timeout time.Duration, handshake bool) (*perfLifecyclePeer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", f.address)
	if err != nil {
		return nil, err
	}
	if f.tls && handshake {
		tlsConn := tls.Client(conn, f.clientTLS.Clone())
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, err
		}
		conn = tlsConn
	}
	return &perfLifecyclePeer{connectedAt: perf.Now(), conn: conn, read: bufio.NewReader(conn), write: json.NewEncoder(conn)}, nil
}

func (f *perfLifecycleServer) clientTLSConfig() (*tls.Config, error) {
	certificate, err := x509.ParseCertificate(f.server.TLSConfig.Certificates[0].Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("invalid local benchmark certificate: %w", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "127.0.0.1"}, nil
}

func (p *perfLifecyclePeer) close() {
	if p != nil && p.conn != nil {
		_ = p.conn.Close()
	}
}

func (p *perfLifecyclePeer) join(channel, role string) (string, error) {
	request := ClientJoinMessage{GenericClientMessage: GenericClientMessage{Type: "join"}, Channel: channel, ConnectionType: role}
	if err := p.write.Encode(request); err != nil {
		return "", err
	}
	line, err := p.read.ReadBytes('\n')
	p.lastFrameAt = perf.Now()
	if err != nil {
		return "", err
	}
	var response struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(line, &response); err != nil {
		return "", err
	}
	return response.Type, nil
}

func lifecycleResult(spec perf.Scenario, server *Server) perf.Result {
	result := perf.Result{Scenario: spec, Valid: true, Before: perf.Capture(), ServerConfig: map[string]any{
		"max_message_size_bytes":   server.MaxMessageSize,
		"event_queue_size":         server.EventQueueSize,
		"write_timeout_ns":         server.WriteTimeout.Nanoseconds(),
		"tls_handshake_timeout_ns": server.TLSHandshakeTimeout.Nanoseconds(),
	}}
	return result
}

func lifecycleClientCount(cfg perf.Config, spec perf.Scenario) int {
	return perfMinInt(lifecycleRequestedClientCount(cfg, spec), 128)
}

func lifecycleRequestedClientCount(cfg perf.Config, spec perf.Scenario) int {
	if cfg.Clients > 0 {
		return cfg.Clients
	}
	if spec.Clients > 0 {
		return spec.Clients
	}
	return 16
}

func lifecycleAdmissionScenario(cfg perf.Config, spec perf.Scenario, fixture *perfLifecycleServer, result perf.Result, count int) (perf.Result, error) {
	count = perfMinInt(count, 128)
	if count < 1 {
		return result, fmt.Errorf("admission scenario has no samples within configured connection limits")
	}

	protected := strings.HasPrefix(spec.Payload, "protected-")
	if cfg.Warmup > 0 && !protected {
		warm, err := fixture.dial(5*time.Second, true)
		if err != nil {
			return result, err
		}
		kind, err := warm.join("perf-warmup", "master")
		warm.close()
		if err != nil || kind != "channel_joined" {
			if err == nil {
				err = fmt.Errorf("warmup join returned %q", kind)
			}
			return result, err
		}
		if !lifecycleWaitRegistryClients(&fixture.server.registry, 0, 2*time.Second) {
			return result, fmt.Errorf("warmup session cleanup timed out")
		}
	}

	var incumbents []*perfLifecyclePeer
	var incumbentCount int
	protectedKeys := make([]string, count)
	switch spec.Payload {
	case "protected-complementary":
		for i := 0; i < count; i++ {
			key := fmt.Sprintf("perf-protected-%d", i)
			protectedKeys[i] = key
			peer, err := fixture.dial(5*time.Second, true)
			if err != nil {
				return result, err
			}
			kind, err := peer.join(key, "master")
			if err != nil || kind != "channel_joined" {
				peer.close()
				if err == nil {
					err = fmt.Errorf("protected master setup returned %q", kind)
				}
				return result, err
			}
			incumbents = append(incumbents, peer)
		}
		incumbentCount = len(incumbents)
	case "protected-reconnect":
		for i := 0; i < count; i++ {
			key := fmt.Sprintf("perf-protected-overlap-%d", i)
			protectedKeys[i] = key
			for _, role := range []string{"master", "slave"} {
				peer, err := fixture.dial(5*time.Second, true)
				if err != nil {
					return result, err
				}
				kind, err := peer.join(key, role)
				if err != nil || kind != "channel_joined" {
					peer.close()
					if err == nil {
						err = fmt.Errorf("protected baseline returned %q", kind)
					}
					return result, err
				}
				incumbents = append(incumbents, peer)
			}
		}
		incumbentCount = len(incumbents)
	case "recovery":
		for i := 0; i < count; i++ {
			key := fmt.Sprintf("perf-generic-recovery-%d", i)
			protectedKeys[i] = key
			seed, err := fixture.dial(5*time.Second, true)
			if err != nil {
				return result, err
			}
			kind, err := seed.join(key, "master")
			seed.close()
			if err != nil || kind != "channel_joined" {
				if err == nil {
					err = fmt.Errorf("recovery seed returned %q", kind)
				}
				return result, err
			}
			if !lifecycleWaitRegistryClients(&fixture.server.registry, 0, 2*time.Second) {
				return result, fmt.Errorf("recovery seed cleanup timed out")
			}
		}
	}
	if cfg.Warmup > 0 && protected {
		warmKey, warmRole := "", "master"
		switch spec.Payload {
		case "protected-complementary":
			warmKey, warmRole = protectedKeys[0], "slave"
		case "protected-reconnect":
			warmKey, warmRole = protectedKeys[0], "master"
		case "recovery":
			warmKey = protectedKeys[0]
		}
		warm, err := fixture.dial(5*time.Second, true)
		if err != nil {
			return result, err
		}
		kind, err := warm.join(warmKey, warmRole)
		warm.close()
		if err != nil || kind != "channel_joined" {
			if err == nil {
				err = fmt.Errorf("protected warmup returned %q", kind)
			}
			return result, err
		}
		if !lifecycleWaitRegistryClients(&fixture.server.registry, incumbentCount, 2*time.Second) {
			return result, fmt.Errorf("protected warmup cleanup timed out")
		}
	}
	defer func() {
		for _, peer := range incumbents {
			peer.close()
		}
	}()

	resourcesBeforePending := perf.Capture()
	peers := make([]*perfLifecyclePeer, count)
	for i := range peers {
		peer, err := fixture.dial(5*time.Second, true)
		if err != nil {
			for _, opened := range peers {
				opened.close()
			}
			return result, err
		}
		peers[i] = peer
	}
	defer func() {
		for _, peer := range peers {
			peer.close()
		}
	}()
	hists := make([]perf.Histogram, len(peers))
	var setupErr error
	var setupErrMu sync.Mutex
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for i, peer := range peers {
		wg.Add(1)
		go func(i int, peer *perfLifecyclePeer) {
			defer wg.Done()
			<-gate
			key, role := fmt.Sprintf("perf-admission-%d", i), "master"
			switch spec.Payload {
			case "protected-complementary":
				key, role = protectedKeys[i], "slave"
			case "protected-reconnect", "recovery":
				key = protectedKeys[i]
			}
			begin := perf.Now()
			kind, err := peer.join(key, role)
			hists[i].Add(peer.lastFrameAt.Sub(begin))
			if err == nil && kind != "channel_joined" {
				err = fmt.Errorf("admission returned %q", kind)
			}
			if err != nil {
				setupErrMu.Lock()
				if setupErr == nil {
					setupErr = err
				}
				setupErrMu.Unlock()
			}
		}(i, peer)
	}
	result.Before = perf.Capture()
	decisionStarted := perf.Now()
	close(gate)
	wg.Wait()
	result.DurationNS = perf.Since(decisionStarted).Nanoseconds()
	if setupErr != nil {
		return result, setupErr
	}
	var decisionHist perf.Histogram
	for i := range hists {
		decisionHist.Merge(&hists[i])
	}
	result.Latency = decisionHist.Distribution()
	result.Offered, result.Sent, result.Received, result.Expected = uint64(len(peers)), uint64(len(peers)), uint64(len(peers)), uint64(len(peers))
	result.After = perf.Capture() // decision-only process resources
	result.Resources = perf.Delta(result.Before, result.After)
	activeCapacity := fixture.server.capacity.stats()
	if spec.Payload == "protected-reconnect" && activeCapacity.OverlapUsage < len(peers) {
		return result, fmt.Errorf("protected reconnect overlap usage %d, expected at least %d", activeCapacity.OverlapUsage, len(peers))
	}
	cleanupStart := perf.Now()
	cleanupHist := new(perf.Histogram)
	for i, peer := range peers {
		peer.close()
		want := incumbentCount + len(peers) - i - 1
		if !lifecycleWaitRegistryClients(&fixture.server.registry, want, 3*time.Second) {
			return result, fmt.Errorf("admission cleanup timed out after sample %d", i)
		}
		cleanupHist.Add(perf.Since(cleanupStart))
		cleanupStart = perf.Now()
	}
	for _, peer := range incumbents {
		peer.close()
	}
	baseCleanupStart := perf.Now()
	if !lifecycleWaitRegistryClients(&fixture.server.registry, 0, 3*time.Second) {
		return result, fmt.Errorf("admission baseline cleanup timed out")
	}
	if incumbentCount > 0 {
		cleanupHist.Add(perf.Since(baseCleanupStart))
	}
	if spec.Payload == "protected-reconnect" {
		stats := fixture.server.capacity.stats()
		if stats.OverlapUsage != 0 {
			return result, fmt.Errorf("protected overlap cleanup left overlap usage %d", stats.OverlapUsage)
		}
	}
	cleanupAfter := perf.Capture()
	result.Observations = map[string]any{
		"admission_decisions":              result.Received,
		"requested_clients":                lifecycleRequestedClientCount(cfg, spec),
		"resources_before_pending_clients": resourcesBeforePending,
		"decision_latency_semantics":       "join write through complete server response line; TCP/TLS setup excluded",
		"decision_resources_scope":         "from all measured sessions pending startup through completed admission responses, before cleanup",
		"cleanup_latency_ns":               cleanupHist.Distribution().Mean,
		"cleanup_latency_distribution":     cleanupHist.Distribution(),
		"cleanup_resources":                perf.Delta(result.After, cleanupAfter),
		"cleanup_completed":                true,
		"decisions_per_second":             perf.EventsPerSecond(result.Received, time.Duration(result.DurationNS)),
		"effective_client_samples":         len(peers),
		"capacity_stats_during_admission":  activeCapacity,
		"warmup":                           "one ordinary protocol join completed before measurement",
	}
	if protected {
		result.Observations["warmup"] = "scenario-specific protected join on the measured key and role, followed by full cleanup"
	}
	result.ServerConfig["admission"] = fixture.server.capacity.config
	return result, nil
}

func lifecycleWaitRegistryClients(reg *registry, want int, timeout time.Duration) bool {
	deadline := perf.Now().Add(timeout)
	for perf.Now().Before(deadline) {
		reg.lock.RLock()
		got := len(reg.clients)
		reg.lock.RUnlock()
		if got == want {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func lifecycleCleanup(fixture *perfLifecycleServer, peers []*perfLifecyclePeer) time.Duration {
	started := perf.Now()
	for _, peer := range peers {
		peer.close()
	}
	_ = lifecycleWaitRegistryClients(&fixture.server.registry, 0, 2*time.Second)
	return perf.Since(started)
}

func lifecycleRejectScenario(cfg perf.Config, spec perf.Scenario, fixture *perfLifecycleServer, result perf.Result, count int) (perf.Result, error) {
	if spec.Payload == "capacity-reject" {
		count = 1
	}
	count = perfMinInt(count, fixture.server.capacity.config.PendingLimit)
	if fixture.tls {
		count = perfMinInt(count, fixture.server.capacity.config.TLSLimit)
	}
	if count < 1 {
		return result, fmt.Errorf("rejection scenario has no samples within configured pending limit")
	}
	occupied := make([]*perfLifecyclePeer, 2)
	for i, role := range []string{"master", "slave"} {
		peer, err := fixture.dial(5*time.Second, true)
		if err != nil {
			return result, err
		}
		kind, err := peer.join("perf-full-ordinary", role)
		if err != nil || kind != "channel_joined" {
			peer.close()
			if err == nil {
				err = fmt.Errorf("capacity setup join returned %q", kind)
			}
			return result, err
		}
		occupied[i] = peer
	}
	defer func() {
		for _, peer := range occupied {
			peer.close()
		}
	}()

	resourcesBeforeRejectedPeers := perf.Capture()
	peers := make([]*perfLifecyclePeer, count)
	for i := range peers {
		peer, err := fixture.dial(5*time.Second, true)
		if err != nil {
			for _, opened := range peers {
				opened.close()
			}
			return result, err
		}
		peers[i] = peer
	}
	defer func() {
		for _, peer := range peers {
			peer.close()
		}
	}()
	result.ServerConfig["admission"] = fixture.server.capacity.config
	result.ServerConfig["rejection_window_ns"] = fixture.server.capacity.config.RejectionWindow.Nanoseconds()
	result.ServerConfig["rejection_interval_ns"] = fixture.server.capacity.config.RejectionInterval.Nanoseconds()
	decisionHists := make([]perf.Histogram, len(peers))
	windowHists := make([]perf.Histogram, len(peers))
	speechCounts := make([]uint64, len(peers))
	gate := make(chan struct{})
	firstDone := make(chan struct{}, len(peers))
	continueWindow := make(chan struct{})
	errCh := make(chan error, len(peers))
	var wg sync.WaitGroup
	for i, peer := range peers {
		wg.Add(1)
		go func(i int, peer *perfLifecyclePeer) {
			defer wg.Done()
			firstReported := false
			defer func() {
				if !firstReported {
					firstDone <- struct{}{}
				}
			}()
			<-gate
			request := ClientJoinMessage{GenericClientMessage: GenericClientMessage{Type: "join"}, Channel: fmt.Sprintf("rejected-%d", i), ConnectionType: "master"}
			begin := perf.Now()
			if err := peer.write.Encode(request); err != nil {
				errCh <- err
				return
			}
			line, err := peer.read.ReadBytes('\n')
			decisionHists[i].Add(perf.Since(begin))
			if err != nil {
				errCh <- err
				return
			}
			var response struct {
				Type     string   `json:"type"`
				Error    string   `json:"error"`
				Sequence []string `json:"sequence"`
			}
			if err := json.Unmarshal(line, &response); err != nil {
				errCh <- err
				return
			}
			if response.Type != "speak" || len(response.Sequence) != 1 || response.Sequence[0] != capacityAnnouncement {
				errCh <- fmt.Errorf("capacity response type %q, want speak", response.Type)
				return
			}
			windowStarted := perf.Now()
			speechCounts[i]++
			firstDone <- struct{}{}
			firstReported = true
			<-continueWindow
			errorResponses := 0
			for {
				line, err = peer.read.ReadBytes('\n')
				if err != nil {
					if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
						errCh <- err
						return
					}
					break
				}
				response.Type = ""
				response.Error = ""
				response.Sequence = nil
				if err = json.Unmarshal(line, &response); err != nil {
					errCh <- err
					return
				}
				if response.Type == "speak" {
					if len(response.Sequence) != 1 || response.Sequence[0] != capacityAnnouncement {
						errCh <- errors.New("rejection speech payload changed")
						return
					}
					speechCounts[i]++
				} else if response.Type == "error" && response.Error == "server at capacity" && errorResponses == 0 {
					errorResponses++
				} else {
					errCh <- fmt.Errorf("unexpected rejection lifecycle message %q", response.Type)
					return
				}
			}
			if errorResponses != 1 {
				errCh <- fmt.Errorf("rejection lifecycle received %d capacity error responses, want 1", errorResponses)
				return
			}
			windowHists[i].Add(perf.Since(windowStarted))
		}(i, peer)
	}
	result.Before = perf.Capture()
	started := perf.Now()
	close(gate)
	for i := 0; i < len(peers); i++ {
		<-firstDone
	}
	rejectionStats := fixture.server.capacity.stats()
	if rejectionStats.CapacityRejects < uint64(len(peers)) || rejectionStats.PendingRejection < len(peers) {
		close(continueWindow)
		wg.Wait()
		return result, fmt.Errorf("capacity rejection lifecycle stats: rejects=%d pending=%d, expected at least %d", rejectionStats.CapacityRejects, rejectionStats.PendingRejection, len(peers))
	}
	duringRejection := perf.Capture()
	decisionWall := perf.Since(started)
	close(continueWindow)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			lifecycleCleanup(fixture, peers)
			return result, err
		}
	}
	var decisionHist, windowHist perf.Histogram
	var speechTotal uint64
	for i := range decisionHists {
		decisionHist.Merge(&decisionHists[i])
		windowHist.Merge(&windowHists[i])
		speechTotal += speechCounts[i]
	}
	result.DurationNS = perf.Since(started).Nanoseconds()
	result.Offered, result.Sent, result.Received, result.Expected = uint64(len(peers)), uint64(len(peers)), speechTotal, speechTotal
	result.Latency = decisionHist.Distribution()
	result.Observations = map[string]any{"requested_clients": lifecycleRequestedClientCount(cfg, spec), "effective_clients": len(peers), "resources_before_rejected_clients": resourcesBeforeRejectedPeers, "rejection_decision_latency_semantics": "join write through first capacity speech line", "rejection_window_lifecycle_latency": windowHist.Distribution(), "rejection_speech_count": speechTotal, "rejection_window_live_while_clients_remain": true, "capacity_stats_during_rejection": rejectionStats, "resources_during_rejection": duringRejection}
	result.Observations["decision_wall_ns"] = decisionWall.Nanoseconds()
	result.Observations["decisions_per_second"] = perf.EventsPerSecond(uint64(len(peers)), decisionWall)
	result.Observations["decision_resources"] = perf.Delta(result.Before, duringRejection)
	cleanupStart := perf.Now()
	if !lifecycleWaitCapacityPhysical(fixture.server, 2, 2*time.Second) {
		return result, fmt.Errorf("rejected batch cleanup timed out")
	}
	result.Observations["rejection_cleanup_latency_ns"] = perf.Since(cleanupStart).Nanoseconds()
	result.After = perf.Capture()
	result.Resources = perf.Delta(result.Before, result.After)
	result.Observations["resource_snapshot_includes"] = "two admitted clients and rejected batch while default five-second window is active"
	return result, nil
}

func lifecycleWaitCapacityPhysical(server *Server, want int, timeout time.Duration) bool {
	deadline := perf.Now().Add(timeout)
	for perf.Now().Before(deadline) {
		server.lifecycleMu.Lock()
		budget := server.capacity
		server.lifecycleMu.Unlock()
		if budget != nil && budget.stats().PhysicalActive == want {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func lifecyclePendingScenario(cfg perf.Config, spec perf.Scenario, fixture *perfLifecycleServer, result perf.Result, count int) (perf.Result, error) {
	if spec.Payload == "pending-tls" && !fixture.tls {
		return result, fmt.Errorf("pending-tls requires TLS transport")
	}
	requested := lifecycleRequestedClientCount(cfg, spec)
	if spec.Payload == "pending-tls" {
		count = perfMinInt(count, fixture.server.capacity.config.TLSLimit)
	} else {
		count = perfMinInt(count, fixture.server.capacity.config.StartupLimit)
		if fixture.tls {
			count = perfMinInt(count, fixture.server.capacity.config.TLSLimit)
		}
	}
	count = perfMinInt(count, fixture.server.capacity.config.PendingLimit)
	resourcesBeforePending := perf.Capture()
	result.Before = resourcesBeforePending
	peers := make([]*perfLifecyclePeer, 0, count)
	conns := make([]net.Conn, 0, count)
	defer func() {
		for _, peer := range peers {
			peer.close()
		}
		for _, conn := range conns {
			_ = conn.Close()
		}
	}()
	for i := 0; i < count; i++ {
		if spec.Payload == "pending-tls" {
			conn, err := (&net.Dialer{}).Dial("tcp", fixture.address)
			if err != nil {
				return result, err
			}
			conns = append(conns, conn) // retain raw sockets before TLS handshake
		} else {
			peer, err := fixture.dial(5*time.Second, fixture.tls)
			if err != nil {
				return result, err
			}
			peers = append(peers, peer)
		}
	}
	deadline := perf.Now().Add(2 * time.Second)
	for perf.Now().Before(deadline) {
		stats := fixture.server.capacity.stats()
		wanted := len(peers)
		if spec.Payload == "pending-tls" {
			wanted = len(conns)
			if stats.PendingTLS >= wanted {
				break
			}
		} else if stats.PendingStartup >= wanted {
			break
		}
		time.Sleep(time.Millisecond)
	}
	stats := fixture.server.capacity.stats()
	observedPending := stats.PendingStartup
	if spec.Payload == "pending-tls" {
		observedPending = stats.PendingTLS
	}
	if observedPending < count {
		for _, peer := range peers {
			peer.close()
		}
		for _, conn := range conns {
			_ = conn.Close()
		}
		return result, fmt.Errorf("observed %d pending clients; expected %d", observedPending, count)
	}
	result.ServerConfig["admission"] = fixture.server.Admission
	result.Observations = map[string]any{"requested_clients": requested, "effective_clients": count, "observed_capacity": stats, "resources_during_pending": perf.Capture(), "gc_was_not_forced": true}
	measurementStart := perf.Now()
	if cfg.Duration > 0 {
		time.Sleep(perfMinDuration(cfg.Duration, 2*time.Second))
	}
	cleanupStart := perf.Now()
	for _, peer := range peers {
		peer.close()
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
	if !lifecycleWaitCapacityPhysical(fixture.server, 0, 5*time.Second) {
		return result, fmt.Errorf("pending client cleanup timed out")
	}
	result.Observations["cleanup_latency_ns"] = perf.Since(cleanupStart).Nanoseconds()
	result.DurationNS = perf.Since(measurementStart).Nanoseconds()
	result.Offered = uint64(count)
	result.Observations["pending_clients_observed"] = observedPending
	result.Observations["pending_cleanup_confirmed"] = fixture.server.capacity.stats().PhysicalActive == 0
	result.After = perf.Capture()
	result.Resources = perf.Delta(result.Before, result.After)
	return result, nil
}

func lifecycleConnectionScenario(cfg perf.Config, spec perf.Scenario, fixture *perfLifecycleServer, result perf.Result, count int) (perf.Result, error) {
	requested := lifecycleRequestedClientCount(cfg, spec)
	if fixture.tls && spec.Payload == "tls-handshake" {
		count = perfMinInt(count, fixture.server.capacity.config.TLSLimit)
	}
	var hist perf.Histogram
	started := perf.Now()
	for i := 0; i < count; i++ {
		begin := perf.Now()
		handshake := spec.Payload != "tcp-connect" || !fixture.tls
		peer, err := fixture.dial(10*time.Second, handshake)
		if err != nil {
			return result, err
		}
		if spec.Payload == "startup-join" {
			kind, err := peer.join(fmt.Sprintf("perf-startup-%d", i), "master")
			if err != nil || kind != "channel_joined" {
				peer.close()
				if err == nil {
					err = fmt.Errorf("startup join returned %q", kind)
				}
				return result, err
			}
		}
		elapsed := peer.connectedAt.Sub(begin)
		if spec.Payload == "startup-join" {
			elapsed = peer.lastFrameAt.Sub(begin)
		}
		peer.close()
		hist.Add(elapsed)
		if cfg.Duration > 0 && perf.Since(started) >= cfg.Duration {
			break
		}
	}
	result.DurationNS = perf.Since(started).Nanoseconds()
	result.Offered, result.Sent, result.Received, result.Expected = hist.Distribution().Count, hist.Distribution().Count, hist.Distribution().Count, hist.Distribution().Count
	result.Latency = hist.Distribution()
	result.After = perf.Capture()
	result.Resources = perf.Delta(result.Before, result.After)
	result.Observations = map[string]any{"requested_clients": requested, "effective_samples": result.Received, "latency_semantics": map[string]string{"tcp-connect": "TCP connect completion; socket close excluded", "tls-handshake": "TCP connect plus TLS handshake completion; socket close excluded", "startup-join": "TCP/TLS connect plus complete protocol join response; socket close excluded"}[spec.Payload], "decisions_per_second": perf.EventsPerSecond(result.Received, time.Duration(result.DurationNS))}
	return result, nil
}

func lifecycleOversizedScenario(cfg perf.Config, spec perf.Scenario, fixture *perfLifecycleServer, result perf.Result, count int) (perf.Result, error) {
	requested := lifecycleRequestedClientCount(cfg, spec)
	count = perfMinInt(count, fixture.server.capacity.config.PendingLimit)
	if fixture.tls {
		count = perfMinInt(count, fixture.server.capacity.config.TLSLimit)
	}
	if count < 1 {
		return result, fmt.Errorf("oversized offender scenario has no samples within configured client limits")
	}
	resourcesBeforeOffenders := perf.Capture()
	peers := make([]*perfLifecyclePeer, 0, count)
	for i := 0; i < count; i++ {
		peer, err := fixture.dial(5*time.Second, true)
		if err != nil {
			for _, opened := range peers {
				opened.close()
			}
			return result, err
		}
		peers = append(peers, peer)
	}
	defer func() {
		for _, peer := range peers {
			peer.close()
		}
	}()
	payload := perfOversizedMessage(defaultMaxMessageSize + 1)
	hists := make([]perf.Histogram, len(peers))
	errs := make(chan error, len(peers))
	gate := make(chan struct{})
	var wg sync.WaitGroup
	for i, peer := range peers {
		wg.Add(1)
		go func(i int, peer *perfLifecyclePeer) {
			defer wg.Done()
			<-gate
			begin := perf.Now()
			for sent := 0; sent < len(payload); {
				n, err := peer.conn.Write(payload[sent:])
				sent += n
				if err != nil {
					errs <- err
					return
				}
				if n == 0 {
					errs <- io.ErrShortWrite
					return
				}
			}
			_ = peer.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			_, err := peer.read.ReadByte()
			if err == nil {
				errs <- fmt.Errorf("oversized client %d remained readable", i)
				return
			}
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				errs <- fmt.Errorf("oversized client %d returned unexpected read error: %w", i, err)
				return
			}
			hists[i].Add(perf.Since(begin))
		}(i, peer)
	}
	result.Before = perf.Capture()
	result.Observations = map[string]any{
		"requested_offenders": requested, "effective_offenders": len(peers),
		"resources_before_offender_setup":                   resourcesBeforeOffenders,
		"resources_with_offenders_connected_before_payload": result.Before,
		"message_bytes": len(payload) - 1, "configured_max_message_bytes": defaultMaxMessageSize,
		"disconnect_reason_expected": "Incoming message too large", "expected_rejection": true,
	}
	started := perf.Now()
	close(gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return result, err
		}
	}
	cleanupStart := perf.Now()
	if !lifecycleWaitCapacityPhysical(fixture.server, 0, 10*time.Second) {
		return result, fmt.Errorf("oversized offender cleanup timed out")
	}
	var hist perf.Histogram
	for i := range hists {
		hist.Merge(&hists[i])
	}
	result.DurationNS = perf.Since(started).Nanoseconds()
	result.Offered, result.Sent = uint64(len(peers)), uint64(len(peers))
	result.Disconnects = fixture.logHook.disconnects.Load()
	actualOversized := fixture.logHook.oversized.Load()
	result.Observations["confirmed_oversized_disconnects"] = actualOversized
	if actualOversized != uint64(len(peers)) || result.Disconnects != uint64(len(peers)) {
		return result, fmt.Errorf("oversized rejection reasons: got %d oversized/%d total disconnects, want %d", actualOversized, result.Disconnects, len(peers))
	}
	result.Latency = hist.Distribution()
	result.Observations["cleanup_latency_ns"] = perf.Since(cleanupStart).Nanoseconds()
	result.Observations["cleanup_completed"] = fixture.server.capacity.stats().PhysicalActive == 0
	result.After = perf.Capture()
	result.Resources = perf.Delta(result.Before, result.After)
	return result, nil
}

func perfOversizedMessage(size int) []byte {
	prefix := `{"type":"bench","padding":"`
	suffix := `"}`
	return []byte(prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix + "\n")
}

func runLifecycleBoundary(cfg perf.Config, spec perf.Scenario) (measured perf.Result, retErr error) {
	fixture, err := newPerfLifecycleServer(spec.Transport, DefaultAdmissionConfig())
	if err != nil {
		return perf.Result{}, err
	}
	defer func() {
		closeErr := fixture.close()
		measured.ServerErrors = fixture.logHook.serverErrors.Load()
		if measured.ServerErrors != 0 {
			measured.Valid = false
		}
		if fixture.logHook.disconnects.Load() > 0 {
			if measured.Observations == nil {
				measured.Observations = make(map[string]any)
			}
			measured.Observations["server_disconnect_logs"] = fixture.logHook.disconnects.Load()
		}
		if closeErr != nil {
			measured.Valid = false
			retErr = errors.Join(retErr, fmt.Errorf("close boundary fixture: %w", closeErr))
		}
	}()
	result := lifecycleResult(spec, fixture.server)
	result.ServerConfig["admission_policy"] = "DefaultAdmissionConfig"
	if spec.Payload == "oversized" {
		return lifecycleOversizedScenario(cfg, spec, fixture, result, lifecycleClientCount(cfg, spec))
	}
	var size int
	var payload []byte
	switch spec.Payload {
	case "small":
		payload, err = perf.Payload("key")
	case "medium":
		payload, err = perf.Payload("clipboard-1KiB")
	case "1MiB":
		payload, err = perf.Payload("clipboard-1MiB")
	case "near-limit":
		size = defaultMaxMessageSize - 1024
	case "exact-limit":
		size = defaultMaxMessageSize
	default:
		return result, fmt.Errorf("unknown boundary profile %q", spec.Payload)
	}
	if err != nil {
		return result, err
	}
	if size > 0 {
		payload = perfSizedJSON(size, 1)
	}
	return lifecycleBoundaryForward(fixture, spec, result, payload)
}

func lifecycleBoundaryForward(fixture *perfLifecycleServer, spec perf.Scenario, result perf.Result, payload []byte) (perf.Result, error) {
	sender, err := fixture.dial(5*time.Second, true)
	if err != nil {
		return result, err
	}
	receiver, err := fixture.dial(5*time.Second, true)
	if err != nil {
		sender.close()
		return result, err
	}
	defer sender.close()
	defer receiver.close()
	channelKey := "perf-boundary"
	if kind, err := sender.join(channelKey, "master"); err != nil || kind != "channel_joined" {
		if err == nil {
			err = fmt.Errorf("sender join returned %q", kind)
		}
		return result, err
	}
	if kind, err := receiver.join(channelKey, "slave"); err != nil || kind != "channel_joined" {
		if err == nil {
			err = fmt.Errorf("receiver join returned %q", kind)
		}
		return result, err
	}
	// Drain the member-joined notification sent to the existing sender. The
	// newly joined receiver has already consumed its complete join response.
	joinEvent, err := sender.read.ReadBytes('\n')
	if err != nil {
		return result, err
	}
	var joined map[string]json.RawMessage
	if err := json.Unmarshal(joinEvent, &joined); err != nil || string(joined["type"]) != `"client_joined"` {
		return result, fmt.Errorf("unexpected sender join event %s", joinEvent)
	}
	warmup, err := perf.Payload("key")
	if err != nil {
		return result, err
	}
	warmup = perfAddSequence(warmup, 1)
	if _, err := sender.conn.Write(append(warmup, '\n')); err != nil {
		return result, err
	}
	warmReply, err := receiver.read.ReadBytes('\n')
	if err != nil || !bytes.Contains(warmReply, []byte(`"type":"key"`)) {
		if err == nil {
			err = fmt.Errorf("boundary warmup payload changed")
		}
		return result, err
	}
	wire := perfAddSequence(payload, 2)
	if spec.Payload == "near-limit" {
		wire = perfSizedJSON(defaultMaxMessageSize-1024, 2)
	}
	if spec.Payload == "exact-limit" {
		wire = perfSizedJSON(defaultMaxMessageSize, 2)
	}
	if !bytes.HasSuffix(wire, []byte("\n")) {
		wire = append(wire, '\n')
	}
	var expected map[string]any
	if err := json.Unmarshal(bytes.TrimSuffix(wire, []byte("\n")), &expected); err != nil {
		return result, err
	}
	var hist perf.Histogram
	result.Before = perf.Capture()
	started := perf.Now()
	if _, err := sender.conn.Write(wire); err != nil {
		return result, err
	}
	line, err := receiver.read.ReadBytes('\n')
	receivedAt := perf.Now()
	if err != nil {
		return result, err
	}
	var actual map[string]any
	if err := json.Unmarshal(line, &actual); err != nil {
		return result, err
	}
	origin, exists := actual["origin"]
	if !exists {
		return result, fmt.Errorf("boundary response has no origin: %s", line)
	}
	if originNumber, ok := origin.(float64); !ok || originNumber != 0 {
		return result, fmt.Errorf("boundary response origin is not sender ID 0: %#v", origin)
	}
	delete(actual, "origin")
	if !reflect.DeepEqual(actual, expected) {
		return result, fmt.Errorf("boundary response payload mismatch: got %v, want %v", actual, expected)
	}
	hist.Add(receivedAt.Sub(started))
	result.DurationNS = receivedAt.Sub(started).Nanoseconds()
	result.Offered, result.Sent, result.Received, result.Expected = 1, 1, 1, 1
	result.SendBytes, result.ReceiveBytes = uint64(len(wire)), uint64(len(line))
	result.Latency = hist.Distribution()
	result.Observations = map[string]any{"json_value_bytes": len(wire) - 1, "max_message_size_bytes": defaultMaxMessageSize, "latency_semantics": "sender write through receiver complete newline-delimited response; origin is validated and excluded from payload equality", "warmup_messages": 1}
	result.After = perf.Capture()
	result.Resources = perf.Delta(result.Before, result.After)
	return result, nil
}

func perfSizedJSON(size int, seq uint64) []byte {
	prefix := fmt.Sprintf(`{"type":"bench","bench_seq":%d,"padding":"`, seq)
	suffix := `"}`
	if size < len(prefix)+len(suffix) {
		return nil
	}
	return []byte(prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix)
}

func perfAddSequence(payload []byte, seq uint64) []byte {
	var message map[string]json.RawMessage
	if err := json.Unmarshal(bytes.TrimSpace(payload), &message); err != nil || message == nil {
		return append([]byte(nil), payload...)
	}
	sequence, _ := json.Marshal(seq)
	message["bench_seq"] = sequence
	encoded, err := json.Marshal(message)
	if err != nil {
		return append([]byte(nil), payload...)
	}
	return encoded
}

func perfMinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func perfMaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func perfMinDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
