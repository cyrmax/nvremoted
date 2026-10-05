//go:build performance

package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/n0ot/nvremoted/internal/perf"
	"github.com/sirupsen/logrus"
)

// runRelayScenario exercises the production Server. Only this tagged test file
// defines the runner; normal builds and tests never include performance runs.
func runRelayScenario(cfg perf.Config, spec perf.Scenario) (result perf.Result, err error) {
	result = perf.Result{Scenario: spec, QueueCapacity: defaultEventQueueSize}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 4096
	}
	if cfg.Drain <= 0 {
		cfg.Drain = 2 * time.Second
	}
	if cfg.Warmup < 0 || cfg.Duration < 0 || cfg.Drain < 0 {
		return result, errors.New("performance durations cannot be negative")
	}
	if spec.Transport != "tcp" && spec.Transport != "tls" {
		return result, fmt.Errorf("unsupported relay transport %q", spec.Transport)
	}
	if spec.Kind != "closed" && spec.Kind != "open" && spec.Kind != "idle" {
		return result, fmt.Errorf("unsupported relay workload kind %q", spec.Kind)
	}

	log := logrus.New()
	log.SetOutput(io.Discard)
	log.SetLevel(logrus.InfoLevel)
	hook := &relayPerfLogHook{}
	log.AddHook(hook)
	srv := &Server{Log: log, TimeBetweenPings: 0}
	listener, tlsClientConfig, err := relayPerfListener(spec.Transport)
	if err != nil {
		return result, err
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(listener) }()
	defer func() {
		_ = listener.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutdownErr := srv.Shutdown(ctx); shutdownErr != nil {
			result.Valid = false
			err = errors.Join(err, fmt.Errorf("server shutdown: %w", shutdownErr))
		}
		select {
		case serveErr := <-serveDone:
			if serveErr != nil && !errors.Is(serveErr, net.ErrClosed) {
				result.Valid = false
				err = errors.Join(err, fmt.Errorf("server Serve: %w", serveErr))
			}
		case <-time.After(5 * time.Second):
			result.Valid = false
			err = errors.Join(err, errors.New("server Serve did not exit during cleanup"))
		}
	}()

	if spec.Kind == "idle" {
		return runRelayIdle(cfg, spec, srv, listener.Addr().String(), tlsClientConfig, hook, result)
	}
	return runRelayTraffic(cfg, spec, srv, listener.Addr().String(), tlsClientConfig, hook, result)
}

type relayPerfLogHook struct {
	overflow      atomic.Uint64
	serverErrors  atomic.Uint64
	disconnects   atomic.Uint64
	oversized     atomic.Uint64
	measurement   atomic.Pointer[relayPerfCollector]
	firstOverflow atomic.Pointer[relayPerfOverflow]
}

type relayPerfOverflow struct {
	elapsedNS      int64
	sent, received uint64
}

func (h *relayPerfLogHook) Levels() []logrus.Level { return logrus.AllLevels }
func (h *relayPerfLogHook) Fire(entry *logrus.Entry) error {
	if entry.Message == "Client disconnected" {
		h.disconnects.Add(1)
		if entry.Data["reason"] == "Client event queue overflow" {
			if h.overflow.Add(1) == 1 {
				if c := h.measurement.Load(); c != nil {
					c.mu.Lock()
					elapsed := perf.Since(c.start)
					if c.measurement && elapsed > 0 {
						h.firstOverflow.Store(&relayPerfOverflow{elapsedNS: elapsed.Nanoseconds(), sent: c.sentInWindow, received: c.receivedWindow})
					}
					c.mu.Unlock()
				}
			}
		}
		if entry.Data["reason"] == "Incoming message too large" {
			h.oversized.Add(1)
		}
	}
	if entry.Level <= logrus.ErrorLevel {
		h.serverErrors.Add(1)
	}
	return nil
}

type relayPerfPeer struct {
	conn          net.Conn
	channel       int
	index         int
	channelIndex  int
	id            atomic.Uint64
	observed      chan uint64
	readerDone    chan struct{}
	joined        chan struct{}
	joinedNotices chan struct{}
	joinedOnce    sync.Once
	collector     atomic.Pointer[relayPerfCollector]
	readerErrors  atomic.Uint64
}

type relayPerfStamp struct {
	scheduled       time.Time
	actual          time.Time
	window          bool
	remaining       int
	seen            int
	last            time.Time
	lastRecipient   int
	recipientStart  int
	recipientCount  int
	seenBits        []uint64
	writerDone      bool
	expectedPayload map[string]any
}

type relayPerfCollector struct {
	mu                 sync.Mutex
	stamps             map[uint64]*relayPerfStamp
	peers              []*relayPerfPeer
	start              time.Time
	end                time.Time
	measurement        bool
	active             int
	changed            chan struct{}
	senders            []relayPerfSenderMetrics
	received           uint64
	receivedWindow     uint64
	receiveBytesWindow uint64
	missing            uint64
	duplicates         uint64
	outOfOrder         uint64
	corrupted          uint64
	unexpected         uint64
	timestampLost      uint64
	writeFailures      uint64
	queuedAfterWindow  uint64
	invalidSequence    uint64
	invalidOrigin      uint64
	invalidPayload     uint64
	invalidClock       uint64
	zeroLatency        uint64
	badOriginExpected  uint64
	badOriginActual    uint64
	bytesSent          uint64
	bytesSentWindow    uint64
	bytesReceived      uint64
	lastSeq            []uint64
	seqSeen            []bool
	closed             bool
	maxInFlight        int
	expected           uint64
	offered            uint64
	sent               uint64
	sentInWindow       uint64
	generatorMissed    uint64
	peerMetrics        []relayPerfPeerMetrics
	recipientCounts    []uint64
	recipientWindow    []uint64
	expectedByPeer     []uint64
	expectedOrigins    []uint64
}

type relayPerfSenderMetrics struct {
	write      perf.Histogram
	lag        perf.Histogram
	sent       uint64
	sentWindow uint64
	bytes      uint64
}

type relayPerfPeerMetrics struct {
	latency    perf.Histogram
	scheduled  perf.Histogram
	completion perf.Histogram
}

func newRelayPerfCollector(peers []*relayPerfPeer, maxInFlight int) *relayPerfCollector {
	if maxInFlight < 1 {
		maxInFlight = 1
	}
	c := &relayPerfCollector{
		stamps: make(map[uint64]*relayPerfStamp, maxInFlight), peers: peers, maxInFlight: maxInFlight,
		changed: make(chan struct{}), lastSeq: make([]uint64, len(peers)),
		seqSeen:     make([]bool, len(peers)),
		peerMetrics: make([]relayPerfPeerMetrics, len(peers)), senders: make([]relayPerfSenderMetrics, len(peers)),
		recipientCounts: make([]uint64, len(peers)), recipientWindow: make([]uint64, len(peers)), expectedOrigins: make([]uint64, len(peers)),
		expectedByPeer: make([]uint64, len(peers)),
	}
	return c
}

func (c *relayPerfCollector) signalLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

func (c *relayPerfCollector) reserve(seq uint64, scheduled time.Time, recipientStart, recipients int, expectedPayload map[string]any) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || len(c.stamps) >= c.maxInFlight {
		return false
	}
	c.stamps[seq] = &relayPerfStamp{scheduled: scheduled, remaining: recipients, lastRecipient: -1, recipientStart: recipientStart, recipientCount: recipients, seenBits: make([]uint64, (recipients+63)/64), expectedPayload: expectedPayload}
	c.active++
	c.offered++
	return true
}

func (c *relayPerfCollector) setActual(seq uint64) time.Time {
	c.mu.Lock()
	at := perf.Now()
	if stamp := c.stamps[seq]; stamp != nil {
		stamp.actual = at
	}
	c.mu.Unlock()
	return at
}

func (c *relayPerfCollector) sendFailed(seq uint64) {
	c.mu.Lock()
	if _, ok := c.stamps[seq]; ok {
		delete(c.stamps, seq)
		c.active--
		c.signalLocked()
	}
	c.mu.Unlock()
}

func (c *relayPerfCollector) writeFailed(seq uint64) {
	c.mu.Lock()
	c.writeFailures++
	if _, ok := c.stamps[seq]; ok {
		delete(c.stamps, seq)
		c.active--
		c.signalLocked()
	}
	c.mu.Unlock()
}

func (c *relayPerfCollector) recordWrite(sender int, d time.Duration, n int) {
	c.senders[sender].write.Add(d)
	c.senders[sender].bytes += uint64(n)
	c.mu.Lock()
	c.bytesSent += uint64(n)
	completed := perf.Now()
	if !completed.Before(c.start) && completed.Before(c.end) {
		c.bytesSentWindow += uint64(n)
	}
	c.mu.Unlock()
}

func (c *relayPerfCollector) markSent(seq uint64, sender int, completed time.Time) {
	c.mu.Lock()
	if stamp := c.stamps[seq]; stamp != nil {
		stamp.writerDone = true
		c.sent++
		c.senders[sender].sent++
		c.expected += uint64(stamp.recipientCount)
		for recipient := stamp.recipientStart; recipient < stamp.recipientStart+stamp.recipientCount; recipient++ {
			c.expectedByPeer[recipient]++
		}
		if !completed.Before(c.start) && completed.Before(c.end) {
			c.sentInWindow++
			c.senders[sender].sentWindow++
		}
		if stamp.remaining == 0 {
			delete(c.stamps, seq)
			c.active--
			c.signalLocked()
		}
	}
	c.mu.Unlock()
}

func (c *relayPerfCollector) receive(peerIndex int, seq uint64, raw map[string]any, n int, at time.Time) {
	c.mu.Lock()
	c.bytesReceived += uint64(n)
	if !at.Before(c.start) && at.Before(c.end) {
		c.receiveBytesWindow += uint64(n)
	}
	if peerIndex < 0 || peerIndex >= len(c.peers) {
		c.unexpected++
		c.mu.Unlock()
		return
	}
	if last := c.lastSeq[peerIndex]; c.seqSeen[peerIndex] {
		if seq == last {
			c.duplicates++
		} else if seq < last {
			c.outOfOrder++
		}
	}
	if !c.seqSeen[peerIndex] || seq > c.lastSeq[peerIndex] {
		c.lastSeq[peerIndex], c.seqSeen[peerIndex] = seq, true
	}
	stamp := c.stamps[seq]
	if stamp == nil {
		if c.measurement && !at.Before(c.start) && at.Before(c.end) {
			c.unexpected++
		}
		c.mu.Unlock()
		return
	}
	relativePeer := peerIndex - stamp.recipientStart
	if relativePeer < 0 || relativePeer >= stamp.recipientCount {
		c.unexpected++
		c.mu.Unlock()
		return
	}
	word, bit := relativePeer/64, uint(relativePeer%64)
	if stamp.seenBits[word]&(uint64(1)<<bit) != 0 {
		c.duplicates++
		c.mu.Unlock()
		return
	}
	stamp.seenBits[word] |= uint64(1) << bit
	actual, scheduled, expectedPayload := stamp.actual, stamp.scheduled, stamp.expectedPayload
	expectedOrigin := c.expectedOrigins[peerIndex]
	c.mu.Unlock()

	actualSeq, valid := relayPerfSequence(raw["bench_seq"])
	invalidSequence := !valid || actualSeq != seq
	origin, hasOrigin := relayPerfSequence(raw["origin"])
	invalidOrigin := !hasOrigin || origin != expectedOrigin
	valid = valid && actualSeq == seq && !invalidOrigin
	invalidPayload := false
	if expectedPayload != nil && !relayPerfPayloadMatches(expectedPayload, seq, raw) {
		valid = false
		invalidPayload = true
	}
	latency, scheduledLatency := time.Duration(-1), time.Duration(-1)
	invalidClock := false
	if !actual.IsZero() {
		latency = at.Sub(actual)
		scheduledLatency = at.Sub(scheduled)
		if latency < 0 {
			valid = false
			invalidClock = true
		}
	} else {
		c.mu.Lock()
		c.timestampLost++
		c.mu.Unlock()
	}

	c.mu.Lock()
	if !valid {
		c.corrupted++
	}
	if invalidSequence {
		c.invalidSequence++
	}
	if invalidOrigin {
		c.invalidOrigin++
		if c.invalidOrigin == 1 {
			c.badOriginExpected = expectedOrigin
			c.badOriginActual = origin
		}
	}
	if invalidPayload {
		c.invalidPayload++
	}
	if invalidClock {
		c.invalidClock++
	}
	if latency == 0 {
		c.zeroLatency++
	}
	c.received++
	c.recipientCounts[peerIndex]++
	if !at.Before(c.start) && at.Before(c.end) {
		c.receivedWindow++
		c.recipientWindow[peerIndex]++
	}
	stamp.seen++
	stamp.remaining--
	if at.After(stamp.last) {
		stamp.last = at
	}
	stamp.lastRecipient = peerIndex
	completion := time.Duration(-1)
	if stamp.remaining == 0 {
		if !stamp.last.IsZero() && !stamp.actual.IsZero() {
			completion = stamp.last.Sub(stamp.actual)
		}
		if stamp.writerDone {
			delete(c.stamps, seq)
			c.active--
			c.signalLocked()
		}
	}
	c.mu.Unlock()

	// Each receiver owns these histograms; only bounded stamp bookkeeping uses
	// the shared lock, so fan-out does not serialize histogram updates.
	if latency >= 0 {
		c.peerMetrics[peerIndex].latency.Add(latency)
	}
	if scheduledLatency >= 0 {
		c.peerMetrics[peerIndex].scheduled.Add(scheduledLatency)
	}
	if completion >= 0 {
		c.peerMetrics[peerIndex].completion.Add(completion)
	}
}

func relayPerfSequence(value any) (uint64, bool) {
	switch v := value.(type) {
	case json.Number:
		n, err := strconv.ParseUint(string(v), 10, 64)
		return n, err == nil
	case float64:
		if v < 0 || v != float64(uint64(v)) {
			return 0, false
		}
		return uint64(v), true
	case uint64:
		return v, true
	}
	return 0, false
}

func (p *relayPerfPeer) readLoop() {
	defer close(p.readerDone)
	reader := bufio.NewReaderSize(p.conn, 64<<10)
	for {
		frame, err := relayPerfReadFrame(reader)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
				p.readerErrors.Add(1)
			}
			return
		}
		receivedAt := perf.Now() // frame-complete time precedes benchmark validation.
		dec := json.NewDecoder(bytes.NewReader(frame))
		dec.UseNumber()
		var message map[string]any
		if err := dec.Decode(&message); err != nil {
			p.readerErrors.Add(1)
			return
		}
		if message["type"] == "channel_joined" {
			if id, ok := relayPerfSequence(message["origin"]); ok {
				p.id.Store(id)
			}
		}
		switch message["type"] {
		case "channel_joined":
			p.joinedOnce.Do(func() { close(p.joined) })
		case "client_joined":
			select {
			case p.joinedNotices <- struct{}{}:
			default:
			}
		}
		activeCollector := p.collector.Load()
		if seq, ok := relayPerfSequence(message["bench_seq"]); ok && activeCollector != nil {
			activeCollector.receive(p.index, seq, message, len(frame), receivedAt)
		} else if seq, ok := relayPerfSequence(message["bench_seq"]); ok {
			select {
			case p.observed <- seq:
			default:
			}
		} else if activeCollector != nil {
			switch message["type"] {
			case "channel_joined", "client_joined", "client_left", "ping":
			default:
				activeCollector.mu.Lock()
				activeCollector.unexpected++
				activeCollector.mu.Unlock()
			}
		}
	}
}

func relayPerfReadFrame(reader *bufio.Reader) ([]byte, error) {
	var frame []byte
	for {
		part, err := reader.ReadSlice('\n')
		if len(frame) == 0 && err != bufio.ErrBufferFull {
			return part, err
		}
		frame = append(frame, part...)
		if err == bufio.ErrBufferFull {
			continue
		}
		return frame, err
	}
}

func relayPerfPayload(name string) ([]byte, error) {
	data, err := perf.Payload(name)
	if err != nil {
		return nil, err
	}
	if !bytes.Contains(data, []byte(`"bench_seq":0`)) {
		return nil, fmt.Errorf("performance payload %q lacks exact numeric placeholder \"bench_seq\":0", name)
	}
	return data, nil
}

type relayPerfPayloadProfile struct {
	template perf.Template
	fields   map[string]any
}

func relayPerfPayloads(name string) ([]relayPerfPayloadProfile, error) {
	profiles := []string{name}
	if name == "mixed" {
		profiles = []string{"key", "control", "gesture", "speak-short", "braille-40", "clipboard-1KiB", "arbitrary-nested", "arbitrary-arrays"}
	}
	out := make([]relayPerfPayloadProfile, 0, len(profiles))
	for _, profile := range profiles {
		payload, err := relayPerfPayload(profile)
		if err != nil {
			return nil, err
		}
		template, err := perf.TemplateFromJSON(payload)
		if err != nil {
			return nil, err
		}
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.UseNumber()
		var fields map[string]any
		if err := decoder.Decode(&fields); err != nil {
			return nil, err
		}
		out = append(out, relayPerfPayloadProfile{template: template, fields: fields})
	}
	return out, nil
}

func relayPerfPayloadMatches(expected map[string]any, seq uint64, received map[string]any) bool {
	if len(received) != len(expected)+1 {
		return false
	}
	for key, expectedValue := range expected {
		actualValue, ok := received[key]
		if !ok {
			return false
		}
		if key == "bench_seq" {
			actual, ok := relayPerfSequence(actualValue)
			if !ok || actual != seq {
				return false
			}
			continue
		}
		if !relayPerfJSONEqual(actualValue, expectedValue) {
			return false
		}
	}
	_, hasOrigin := received["origin"]
	return hasOrigin
}

// relayPerfJSONEqual compares the JSON value subset produced by Decoder.UseNumber.
// JSON payloads are acyclic, so direct recursion needs no reflection or cycle tracking.
func relayPerfJSONEqual(actual, expected any) bool {
	switch expectedValue := expected.(type) {
	case nil:
		return actual == nil
	case bool:
		actualValue, ok := actual.(bool)
		return ok && actualValue == expectedValue
	case string:
		actualValue, ok := actual.(string)
		return ok && actualValue == expectedValue
	case json.Number:
		actualValue, ok := actual.(json.Number)
		return ok && actualValue == expectedValue
	case []any:
		actualValue, ok := actual.([]any)
		if !ok || len(actualValue) != len(expectedValue) {
			return false
		}
		for i := range expectedValue {
			if !relayPerfJSONEqual(actualValue[i], expectedValue[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		actualValue, ok := actual.(map[string]any)
		if !ok || len(actualValue) != len(expectedValue) {
			return false
		}
		for key, expectedField := range expectedValue {
			actualField, ok := actualValue[key]
			if !ok || !relayPerfJSONEqual(actualField, expectedField) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func relayPerfJoin(peer *relayPerfPeer, channel, role string) error {
	if _, err := io.WriteString(peer.conn, `{"type":"join","channel":"`+channel+`","connection_type":"`+role+`"}`+"\n"); err != nil {
		return err
	}
	select {
	case <-peer.joined:
		return nil
	case <-time.After(10 * time.Second):
		return errors.New("timed out waiting for channel_joined")
	}
}

func relayPerfWaitNotices(peers []*relayPerfPeer, counts []int) error {
	for i, p := range peers {
		for n := 0; n < counts[i]; n++ {
			select {
			case <-p.joinedNotices:
			case <-time.After(10 * time.Second):
				return fmt.Errorf("timed out waiting for peer %d join notice %d/%d", i, n+1, counts[i])
			}
		}
	}
	return nil
}

func relayPerfConnect(addr string, clientTLS *tls.Config, channel, role string, channelIndex, peerIndex, total int) (*relayPerfPeer, error) {
	conn, err := relayPerfDial(addr, clientTLS)
	if err != nil {
		return nil, err
	}
	p := &relayPerfPeer{conn: conn, channel: channelIndex, index: peerIndex, readerDone: make(chan struct{}), joined: make(chan struct{}), joinedNotices: make(chan struct{}, total), observed: make(chan uint64, 1)}
	go p.readLoop()
	if channel != "" {
		if err := relayPerfJoin(p, channel, role); err != nil {
			conn.Close()
			<-p.readerDone
			return nil, err
		}
	}
	return p, nil
}

func runRelayTraffic(cfg perf.Config, spec perf.Scenario, srv *Server, addr string, clientTLS *tls.Config, hook *relayPerfLogHook, result perf.Result) (perf.Result, error) {
	if spec.Channels <= 0 {
		spec.Channels = 1
	}
	if spec.Fanout < 1 {
		spec.Fanout = 1
	}
	if spec.Payload == "" {
		spec.Payload = "control"
	}
	if spec.Kind != "idle" && spec.Channels*(spec.Fanout+1) > DefaultAdmissionConfig().AdmittedLimit {
		return result, fmt.Errorf("relay topology needs %d admitted clients, exceeding the default admission limit %d", spec.Channels*(spec.Fanout+1), DefaultAdmissionConfig().AdmittedLimit)
	}
	result.Scenario = spec
	payloads, err := relayPerfPayloads(spec.Payload)
	if err != nil {
		return result, err
	}
	peers := make([]*relayPerfPeer, 0, spec.Channels*(spec.Fanout+1))
	senders := make([]*relayPerfPeer, spec.Channels)
	channelPeers := make([][]*relayPerfPeer, spec.Channels)
	channelNames := make([]string, spec.Channels)
	for ch := 0; ch < spec.Channels; ch++ {
		channelNames[ch] = fmt.Sprintf("perf_%d_%d", perf.Now().UnixNano(), ch)
		for i := 0; i <= spec.Fanout; i++ {
			peerIndex := len(peers)
			role := "master"
			if i > 0 {
				role = "slave"
			}
			p, e := relayPerfConnect(addr, clientTLS, channelNames[ch], role, ch, peerIndex, spec.Fanout+1)
			if e != nil {
				relayPerfClosePeers(peers)
				return result, e
			}
			p.channel = ch
			p.channelIndex = i
			peers = append(peers, p)
			channelPeers[ch] = append(channelPeers[ch], p)
			if i == 0 {
				senders[ch] = p
			}
		}
	}
	defer relayPerfClosePeers(peers)
	// Each existing member sees all members who joined after it. Wait for the
	// join-control traffic to be consumed before warming or measuring.
	counts := make([]int, len(peers))
	for i, p := range peers {
		counts[i] = len(channelPeers[p.channel]) - p.indexInChannel() - 1
	}
	if err := relayPerfWaitNotices(peers, counts); err != nil {
		return result, err
	}

	collector := newRelayPerfCollector(peers, cfg.MaxInFlight)
	// Warm-up is sequential closed-loop relay traffic and is excluded from every
	// measured distribution. It also verifies the payload path before timing.
	seq := uint64(1)
	warmEnd := perf.Now().Add(cfg.Warmup)
	for perf.Now().Before(warmEnd) {
		for ch, sender := range senders {
			payload := payloads[(seq-1)%uint64(len(payloads))]
			if err := relayPerfWarmup(sender, channelPeers[ch][1:], payload.template, seq); err != nil {
				return result, err
			}
			seq++
		}
	}
	for ch, members := range channelPeers {
		origin := senders[ch].id.Load()
		for _, p := range members[1:] {
			collector.expectedOrigins[p.index] = origin
		}
	}
	// Warm-up reader delivery was drained above, then measurement readers become active.
	for _, p := range peers {
		p.collector.Store(collector)
	}

	result.Before = perf.Capture()
	var stopProfiles func() error
	if cfg.Profile != "" {
		stopProfiles, err = perf.StartProfiles(cfg.Output, cfg.Profile)
		if err != nil {
			return result, err
		}
	}
	queueStop := make(chan struct{})
	var queueSampler sync.WaitGroup
	var queueSamples atomic.Uint64
	var queueMax atomic.Int64
	var queueCapacity atomic.Int64
	queueSampler.Add(1)
	go func() {
		defer queueSampler.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-queueStop:
				return
			case <-ticker.C:
				q, cap := relayPerfQueueSnapshot(srv)
				queueSamples.Add(1)
				if int64(q) > queueMax.Load() {
					queueMax.Store(int64(q))
				}
				queueCapacity.Store(int64(cap))
			}
		}
	}()
	defer func() {
		select {
		case <-queueStop:
		default:
			close(queueStop)
		}
		queueSampler.Wait()
	}()

	measureStart := perf.Now()
	collector.mu.Lock()
	collector.start = measureStart
	collector.end = collector.start.Add(cfg.Duration)
	collector.measurement = true
	collector.mu.Unlock()
	hook.measurement.Store(collector)
	defer hook.measurement.Store(nil)
	var sendersDone <-chan struct{}
	if spec.Kind == "closed" {
		relayPerfRunClosed(cfg, spec, senders, channelPeers, collector, payloads, &seq)
	} else {
		sendersDone = relayPerfRunOpen(cfg, spec, senders, collector, payloads, &seq)
	}
	measureEnd := collector.end
	if remaining := perf.Until(measureEnd); remaining > 0 {
		time.Sleep(remaining)
	}
	collector.mu.Lock()
	collector.measurement = false
	collector.mu.Unlock()

	result.After = perf.Capture()
	result.Resources = perf.Delta(result.Before, result.After)
	drainStart := perf.Now()
	drainComplete := relayPerfDrain(collector, cfg.Drain)
	if !drainComplete {
		collector.mu.Lock()
		result.Notes = append(result.Notes, "drain deadline expired with messages still in flight")
		collector.mu.Unlock()
	}
	writersDrained := true
	if sendersDone != nil {
		select {
		case <-sendersDone:
		case <-time.After(cfg.Drain):
			writersDrained = false
			result.Notes = append(result.Notes, "sender writes did not drain before the configured deadline")
		}
	}
	drainEnd := perf.Now()
	var profileStopErr error
	if stopProfiles != nil {
		if err := stopProfiles(); err != nil {
			profileStopErr = fmt.Errorf("profile stop: %w", err)
			result.Notes = append(result.Notes, "profile stop: "+err.Error())
		}
	}
	close(queueStop)
	queueSampler.Wait()
	result.Disconnects = hook.disconnects.Load()
	result.OverflowDisconnects = hook.overflow.Load()
	result.ServerErrors = hook.serverErrors.Load()
	for _, p := range peers {
		p.collector.Store(nil)
	}
	if !relayPerfClosePeers(peers) {
		return result, errors.New("receiver workers did not exit after connections closed")
	}
	if sendersDone != nil {
		select {
		case <-sendersDone:
		case <-time.After(5 * time.Second):
			return result, errors.New("sender workers did not exit after connections closed")
		}
	}
	result.DurationNS = measureEnd.Sub(measureStart).Nanoseconds()
	result.DrainNS = drainEnd.Sub(drainStart).Nanoseconds()
	result.SendBytes = collector.bytesSent
	result.SendBytesInWindow = collector.bytesSentWindow
	result.ReceiveBytes = collector.bytesReceived
	result.Offered = collector.offered
	result.Sent = collector.sent
	result.GeneratorMissed = collector.generatorMissed
	result.Received = collector.received
	result.ReceivedInWindow = collector.receivedWindow
	result.SentInWindow = collector.sentInWindow
	result.ReceiveBytesInWindow = collector.receiveBytesWindow
	result.Duplicates = collector.duplicates
	result.OutOfOrder = collector.outOfOrder
	result.Corrupted = collector.corrupted
	result.Unexpected = collector.unexpected
	result.TimestampLost = collector.timestampLost
	collector.mu.Lock()
	writeFailures := collector.writeFailures
	queuedAfterWindow := collector.queuedAfterWindow
	collector.mu.Unlock()
	var latency, scheduled, completion, sendLatency, generatorLag perf.Histogram
	for i := range collector.peerMetrics {
		latency.Merge(&collector.peerMetrics[i].latency)
		scheduled.Merge(&collector.peerMetrics[i].scheduled)
		completion.Merge(&collector.peerMetrics[i].completion)
	}
	for i := range collector.senders {
		sendLatency.Merge(&collector.senders[i].write)
		generatorLag.Merge(&collector.senders[i].lag)
	}
	result.Latency = latency.Distribution()
	result.ScheduledLatency = scheduled.Distribution()
	result.SendLatency = sendLatency.Distribution()
	result.Completion = completion.Distribution()
	result.GeneratorLag = generatorLag.Distribution()
	result.Expected = collector.expected
	collector.mu.Lock()
	result.Missing += collector.missing
	for _, stamp := range collector.stamps {
		result.Missing += uint64(max(stamp.remaining, 0))
	}
	collector.mu.Unlock()
	result.Recipients = relayPerfRecipientResults(spec, channelPeers, collector)
	result.Senders = make([]perf.SenderResult, 0, len(senders))
	for ch, sender := range senders {
		metrics := collector.senders[sender.index]
		result.Senders = append(result.Senders, perf.SenderResult{Channel: ch, Sent: metrics.sent, SentInWindow: metrics.sentWindow, Bytes: metrics.bytes, WriteLatency: metrics.write.Distribution(), GeneratorLag: metrics.lag.Distribution()})
	}
	result.QueueMax = int(queueMax.Load())
	result.QueueCapacity = int(queueCapacity.Load())
	result.QueueSamples = queueSamples.Load()
	result.ServerConfig = map[string]any{"admission": DefaultAdmissionConfig(), "event_queue_size": defaultEventQueueSize, "max_message_size": defaultMaxMessageSize, "channels": spec.Channels, "fanout": spec.Fanout}
	if conn, ok := senders[0].conn.(*tls.Conn); ok {
		state := conn.ConnectionState()
		result.ServerConfig["tls_version"] = state.Version
		result.ServerConfig["tls_cipher_suite"] = tls.CipherSuiteName(state.CipherSuite)
	}
	var readerErrors uint64
	for _, peer := range peers {
		readerErrors += peer.readerErrors.Load()
	}
	result.Observations = map[string]any{"sender_write_failures": writeFailures, "queued_writes_skipped_after_window": queuedAfterWindow, "invalid_sequence": collector.invalidSequence, "invalid_origin": collector.invalidOrigin, "invalid_origin_expected_sample": collector.badOriginExpected, "invalid_origin_actual_sample": collector.badOriginActual, "invalid_payload": collector.invalidPayload, "invalid_clock": collector.invalidClock, "zero_latency_samples": collector.zeroLatency}
	result.Observations["client_reader_errors"] = readerErrors
	result.Observations["drain_complete"] = drainComplete
	result.Observations["writers_drained"] = writersDrained
	if overflow := hook.firstOverflow.Load(); overflow != nil {
		seconds := float64(overflow.elapsedNS) / float64(time.Second)
		result.Observations["first_queue_overflow_elapsed_ns"] = overflow.elapsedNS
		result.Observations["sent_before_first_overflow"] = overflow.sent
		result.Observations["received_before_first_overflow"] = overflow.received
		result.Observations["pre_overflow_send_messages_per_second"] = float64(overflow.sent) / seconds
		result.Observations["pre_overflow_receive_deliveries_per_second"] = float64(overflow.received) / seconds
		result.Observations["pre_overflow_rate_semantics"] = "cumulative measurement-window counters through first observed queue-overflow log, divided by elapsed measurement time; completed sender writes may still have deliveries in flight; not an instantaneous last-bucket rate"
	}
	result.Valid = result.Missing == 0 && result.Duplicates == 0 && result.OutOfOrder == 0 && result.Corrupted == 0 && result.Unexpected == 0 && result.Disconnects == 0 && result.ServerErrors == 0 && result.TimestampLost == 0 && writeFailures == 0
	result.Valid = result.Valid && drainComplete && writersDrained && readerErrors == 0 && result.Sent > 0 && result.Expected == result.Received && result.Latency.Count == result.Received
	result.Valid = result.Valid && result.Expected == result.Sent*uint64(spec.Fanout)
	result.Observations["delivery_valid"] = result.Valid
	result.Observations["requested_load_fully_offered"] = result.GeneratorMissed == 0
	if !result.Valid {
		result.Errors = append(result.Errors, "relay workload failed delivery or lifecycle correctness checks")
	}
	if result.GeneratorMissed > 0 {
		result.Valid = false
		result.Errors = append(result.Errors, "generator did not sustain all requested offers; inspect achieved rate and generator lag")
	}
	if profileStopErr != nil {
		result.Valid = false
		return result, profileStopErr
	}
	return result, nil
}

func (p *relayPerfPeer) indexInChannel() int {
	return p.channelIndex
}

func relayPerfClosePeers(peers []*relayPerfPeer) bool {
	for _, p := range peers {
		if p != nil && p.conn != nil {
			_ = p.conn.Close()
		}
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for _, p := range peers {
		if p != nil && p.readerDone != nil {
			select {
			case <-p.readerDone:
			case <-deadline.C:
				return false
			}
		}
	}
	return true
}

func relayPerfRecipientResults(spec perf.Scenario, byChannel [][]*relayPerfPeer, c *relayPerfCollector) []perf.RecipientResult {
	results := make([]perf.RecipientResult, 0, spec.Channels*spec.Fanout)
	for ch, peers := range byChannel {
		for _, p := range peers[1:] {
			results = append(results, perf.RecipientResult{Channel: ch, Recipient: p.indexInChannel(), Received: c.recipientCounts[p.index], ReceivedInWindow: c.recipientWindow[p.index], Missing: uint64(max(int(c.expectedByPeer[p.index])-int(c.recipientCounts[p.index]), 0)), Latency: c.peerMetrics[p.index].latency.Distribution(), ScheduledLatency: c.peerMetrics[p.index].scheduled.Distribution()})
		}
	}
	return results
}

func relayPerfWaitStamp(c *relayPerfCollector, seq uint64, deadline time.Time) bool {
	for {
		c.mu.Lock()
		_, pending := c.stamps[seq]
		changed := c.changed
		c.mu.Unlock()
		if !pending {
			return true
		}
		remaining := perf.Until(deadline)
		if remaining <= 0 {
			return false
		}
		timer := time.NewTimer(remaining)
		select {
		case <-changed:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			return false
		}
	}
}

func relayPerfRunClosed(cfg perf.Config, spec perf.Scenario, senders []*relayPerfPeer, byChannel [][]*relayPerfPeer, c *relayPerfCollector, payloads []relayPerfPayloadProfile, seq *uint64) {
	var workers sync.WaitGroup
	for ch, sender := range senders {
		workers.Add(1)
		go func(ch int, sender *relayPerfPeer) {
			defer workers.Done()
			var wireBuffer []byte
			for perf.Now().Before(c.end) {
				id := atomic.AddUint64(seq, 1)
				payload := payloads[(id-1)%uint64(len(payloads))]
				if !c.reserve(id, perf.Now(), byChannel[ch][0].index+1, len(byChannel[ch])-1, payload.fields) {
					return
				}
				wire := payload.template.AppendWire(wireBuffer[:0], id)
				c.setActual(id)
				start := perf.Now()
				n, err := sender.conn.Write(wire)
				wireBuffer = wire[:0]
				c.recordWrite(sender.index, perf.Since(start), n)
				if err != nil {
					c.writeFailed(id)
					return
				}
				c.markSent(id, sender.index, perf.Now())
				if !relayPerfWaitStamp(c, id, c.end) {
					return
				}
			}
		}(ch, sender)
	}
	workers.Wait()
}

type relayPerfOffer struct {
	seq           uint64
	scheduled     time.Time
	templateIndex int
}

func relayPerfRunOpen(cfg perf.Config, spec perf.Scenario, senders []*relayPerfPeer, c *relayPerfCollector, payloads []relayPerfPayloadProfile, seq *uint64) <-chan struct{} {
	if spec.Rate <= 0 {
		spec.Rate = 1000
	}
	queues := make([]chan relayPerfOffer, len(senders))
	var workers sync.WaitGroup
	for ch, sender := range senders {
		queues[ch] = make(chan relayPerfOffer, max(1, cfg.MaxInFlight/len(senders)))
		workers.Add(1)
		go func(sender *relayPerfPeer, queue <-chan relayPerfOffer, channel int) {
			defer workers.Done()
			var wireBuffer []byte
			for offer := range queue {
				if !perf.Now().Before(c.end) {
					c.sendFailed(offer.seq)
					c.mu.Lock()
					c.generatorMissed++
					c.queuedAfterWindow++
					c.mu.Unlock()
					continue
				}
				template := payloads[offer.templateIndex].template
				wire := template.AppendWire(wireBuffer[:0], offer.seq)
				actual := c.setActual(offer.seq)
				start := perf.Now()
				n, err := sender.conn.Write(wire)
				wireBuffer = wire[:0]
				c.recordWrite(sender.index, perf.Since(start), n)
				if err != nil {
					c.writeFailed(offer.seq)
				}
				if err == nil {
					c.markSent(offer.seq, sender.index, perf.Now())
					c.senders[sender.index].lag.Add(actual.Sub(offer.scheduled))
				}
			}
		}(sender, queues[ch], ch)
	}
	period := time.Second / time.Duration(spec.Rate)
	if period <= 0 {
		period = time.Nanosecond
	}
	start := c.start
	end := c.end
	for scheduled, offered := start, uint64(0); scheduled.Before(end); scheduled, offered = scheduled.Add(period), offered+1 {
		if delay := perf.Until(scheduled); delay > 0 {
			timer := time.NewTimer(delay)
			<-timer.C
		}
		if !perf.Now().Before(end) {
			delta := end.Sub(scheduled)
			missed := uint64((delta + period - 1) / period)
			c.mu.Lock()
			c.offered += missed
			c.generatorMissed += missed
			c.mu.Unlock()
			break
		}
		channel := relayPerfChannelAt(offered, len(queues), spec.Mixed)
		id := atomic.AddUint64(seq, 1)
		ch := channel
		templateIndex := int((id - 1) % uint64(len(payloads)))
		payload := payloads[templateIndex]
		if !c.reserve(id, scheduled, c.peers[ch*(spec.Fanout+1)].index+1, spec.Fanout, payload.fields) {
			c.mu.Lock()
			c.offered++
			c.generatorMissed++
			c.mu.Unlock()
			continue
		}
		offer := relayPerfOffer{seq: id, scheduled: scheduled, templateIndex: templateIndex}
		select {
		case queues[channel] <- offer:
		default:
			c.sendFailed(id)
			c.mu.Lock()
			c.generatorMissed++
			c.mu.Unlock()
		}
	}
	for _, queue := range queues {
		close(queue)
	}
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	return done
}

func relayPerfChannelAt(offered uint64, channels int, mixed bool) int {
	if channels <= 1 {
		return 0
	}
	if !mixed || channels < 10 {
		return int(offered % uint64(channels))
	}
	hot := max(1, channels/10)
	if offered%5 < 4 {
		return int(((offered/5)*4 + offered%5) % uint64(hot))
	}
	cold := channels - hot
	return hot + int((offered/5)%uint64(cold))
}

func relayPerfWarmup(sender *relayPerfPeer, recipients []*relayPerfPeer, template perf.Template, seq uint64) error {
	wire := template.Wire(seq)
	if _, err := sender.conn.Write(wire); err != nil {
		return err
	}
	for _, recipient := range recipients {
		select {
		case got := <-recipient.observed:
			if got != seq {
				return fmt.Errorf("warmup sequence %d received as %d", seq, got)
			}
		case <-time.After(5 * time.Second):
			return errors.New("warmup delivery timed out")
		}
	}
	return nil
}

func relayPerfDrain(c *relayPerfCollector, duration time.Duration) bool {
	deadline := perf.Now().Add(duration)
	for {
		c.mu.Lock()
		pending := len(c.stamps)
		changed := c.changed
		c.mu.Unlock()
		if pending == 0 {
			return true
		}
		wait := perf.Until(deadline)
		if wait <= 0 {
			break
		}
		timer := time.NewTimer(wait)
		select {
		case <-changed:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			break
		}
	}
	c.mu.Lock()
	for _, stamp := range c.stamps {
		c.missing += uint64(max(stamp.remaining, 0))
	}
	c.stamps = make(map[uint64]*relayPerfStamp)
	c.active = 0
	c.signalLocked()
	c.mu.Unlock()
	return false
}

func relayPerfQueueSnapshot(srv *Server) (int, int) {
	maximum, capacity := 0, defaultEventQueueSize
	srv.registry.lock.RLock()
	for _, member := range srv.registry.clients {
		q := len(member.events)
		if q > maximum {
			maximum = q
		}
		if cap(member.events) > capacity {
			capacity = cap(member.events)
		}
	}
	srv.registry.lock.RUnlock()
	return maximum, capacity
}

func runRelayIdle(cfg perf.Config, spec perf.Scenario, srv *Server, addr string, clientTLS *tls.Config, hook *relayPerfLogHook, result perf.Result) (perf.Result, error) {
	requested := spec.Clients
	if requested < 1 {
		requested = 1
	}
	channels := spec.Channels
	if channels < 1 {
		channels = 1
	}
	if spec.Joined && spec.Channels == 0 {
		channels = max(1, (requested+1)/2)
	}
	attempted := requested
	unattempted := 0
	if !spec.Joined {
		attempted = min(requested, DefaultAdmissionConfig().StartupLimit)
		unattempted = requested - attempted
	}
	if err := relayPerfWaitServerReady(srv, 5*time.Second); err != nil {
		return result, err
	}
	result.Before = perf.Capture() // intentionally includes client setup in the resource delta.
	setupStart := perf.Now()
	peers := make([]*relayPerfPeer, 0, requested)
	byChannel := make([][]*relayPerfPeer, channels)
	channelNames := make([]string, channels)
	for i := range channelNames {
		channelNames[i] = fmt.Sprintf("perf_idle_%d_%d", perf.Now().UnixNano(), i)
	}
	setupFailures := 0
	for i := 0; i < attempted; i++ {
		ch := i % channels
		joinChannelName := ""
		if spec.Joined {
			joinChannelName = channelNames[ch]
		}
		role := "master"
		if spec.Joined && len(byChannel[ch])%2 == 1 {
			role = "slave"
		}
		noticeCapacity := 1
		if spec.Joined {
			noticeCapacity = 2
		}
		p, err := relayPerfConnect(addr, clientTLS, joinChannelName, role, ch, len(peers), noticeCapacity)
		if err != nil {
			setupFailures++
			continue
		}
		p.channelIndex = len(byChannel[ch])
		peers = append(peers, p)
		byChannel[ch] = append(byChannel[ch], p)
	}
	defer relayPerfClosePeers(peers)
	if spec.Joined {
		counts := make([]int, len(peers))
		for i, p := range peers {
			counts[i] = len(byChannel[p.channel]) - p.indexInChannel() - 1
		}
		if err := relayPerfWaitNotices(peers, counts); err != nil {
			return result, err
		}
	}
	setupDuration := perf.Since(setupStart)
	warmup := cfg.Warmup
	if !spec.Joined {
		warmup = min(warmup, DefaultAdmissionConfig().StartupTimeout/4)
	}
	if warmup > 0 {
		timer := time.NewTimer(warmup)
		<-timer.C
	}
	if cfg.Profile != "" {
		return result, errors.New("profiling is supported only for relay traffic scenarios")
	}
	beforeMeasurement := perf.Capture()
	duration := cfg.Duration
	if !spec.Joined {
		remainingLifetime := DefaultAdmissionConfig().StartupTimeout - perf.Since(setupStart) - time.Second
		duration = min(duration, remainingLifetime)
		if duration <= 0 {
			return result, errors.New("unjoined client setup exhausted default startup lifetime")
		}
	}
	if duration > 0 {
		timer := time.NewTimer(duration)
		<-timer.C
	}
	afterMeasurement := perf.Capture()
	result.After = afterMeasurement
	result.Resources = perf.Delta(result.Before, afterMeasurement)
	result.DurationNS = afterMeasurement.Wall.Sub(beforeMeasurement.Wall).Nanoseconds()
	stats := srv.capacity.statsSnapshot()
	result.Disconnects = hook.disconnects.Load()
	result.OverflowDisconnects = hook.overflow.Load()
	result.ServerErrors = hook.serverErrors.Load()
	result.QueueMax, result.QueueCapacity = relayPerfQueueSnapshot(srv)
	result.ServerConfig = map[string]any{"admission": DefaultAdmissionConfig(), "joined": spec.Joined, "channels": channels}
	result.Observations = map[string]any{
		"requested_clients":                     requested,
		"attempted_clients":                     attempted,
		"unattempted_due_default_startup_limit": unattempted,
		"server_owned_clients":                  stats.PhysicalActive,
		"pending_clients":                       stats.PendingTLS + stats.PendingStartup + stats.PendingAdmission + stats.PendingRejection,
		"joined_clients":                        stats.AdmittedConnections,
		"joined_channels":                       stats.ActiveChannels,
		"harness_open_sockets":                  len(peers),
		"setup_failures":                        setupFailures,
		"capacity_refusals":                     stats.CapacityRejects,
		"measurement_wall_seconds":              afterMeasurement.Wall.Sub(beforeMeasurement.Wall).Seconds(),
		"setup_wall_seconds":                    setupDuration.Seconds(),
		"requested_measurement_seconds":         cfg.Duration.Seconds(),
		"effective_warmup_seconds":              warmup.Seconds(),
		"idle_resources_include_harness":        true,
		"idle_resources_include_client_setup":   true,
		"steady_idle_resources":                 perf.Delta(beforeMeasurement, afterMeasurement),
	}
	if stats.PhysicalActive > 0 {
		result.Observations["whole_harness_heap_delta_bytes_per_owned_client"] = float64(int64(afterMeasurement.HeapAlloc)-int64(result.Before.HeapAlloc)) / float64(stats.PhysicalActive)
		if afterMeasurement.RSSBytes > 0 && result.Before.RSSBytes > 0 && afterMeasurement.RSSKind == result.Before.RSSKind && (afterMeasurement.RSSKind == "current_working_set" || afterMeasurement.RSSKind == "current_rss") {
			result.Observations["whole_harness_rss_delta_bytes_per_owned_client"] = float64(int64(afterMeasurement.RSSBytes)-int64(result.Before.RSSBytes)) / float64(stats.PhysicalActive)
		}
	}
	// Exceeding a default admission limit is itself useful capacity information.
	// It remains a valid observation as long as the server did not misbehave.
	result.Valid = result.OverflowDisconnects == 0 && result.ServerErrors == 0
	if setupFailures > 0 {
		result.Notes = append(result.Notes, "some requested clients were refused or could not complete setup under default admission limits")
	}
	if !relayPerfClosePeers(peers) {
		return result, errors.New("idle receiver workers did not exit after connections closed")
	}
	return result, nil
}

func relayPerfWaitServerReady(srv *Server, timeout time.Duration) error {
	deadline := perf.Now().Add(timeout)
	for perf.Now().Before(deadline) {
		srv.lifecycleMu.Lock()
		ready := srv.capacity != nil
		srv.lifecycleMu.Unlock()
		if ready {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return errors.New("timed out waiting for server startup")
}
