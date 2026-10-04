package server

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func testCapacity(t *testing.T, admitted int, protected ...string) *capacity {
	t.Helper()
	cfg := DefaultAdmissionConfig()
	cfg.AdmittedLimit = admitted
	cfg.PendingLimit = 8
	cfg.HardLimit = cfg.AdmittedLimit + cfg.PendingLimit
	cfg.TLSLimit = 4
	cfg.StartupLimit = 8
	for _, key := range protected {
		cfg.ProtectedChannels = append(cfg.ProtectedChannels, channelDigest(key))
	}
	c, err := newCapacity(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func admitTestClient(t *testing.T, c *capacity, key, role string, now time.Time) *client {
	t.Helper()
	conn := &client{}
	if !c.reserveAt(conn, false, now) {
		t.Fatalf("reserve failed for %q", role)
	}
	if !c.beginAdmissionAt(conn, now) {
		t.Fatal("beginAdmission failed")
	}
	if _, reason := c.admit(conn, key, role, now); reason != "" {
		t.Fatalf("admit failed: %s", reason)
	}
	c.markSuccessful(conn)
	return conn
}

func TestCapacityOrdinaryPairSurvivesFullOrdinaryCapacity(t *testing.T) {
	c := testCapacity(t, 4)
	now := time.Unix(100, 0)
	slave := admitTestClient(t, c, "channel-one", "slave", now)
	_ = admitTestClient(t, c, "channel-two", "master", now)
	master := admitTestClient(t, c, "channel-one", "master", now)
	if got := c.stats().GuaranteedEntitlementUsage; got != 4 {
		t.Fatalf("usage = %d, want 4", got)
	}
	extra := &client{}
	if !c.reserveAt(extra, false, now) {
		t.Fatal("physical pending reserve unexpectedly failed")
	} else {
		if !c.beginAdmissionAt(extra, now) {
			t.Fatal("beginAdmission failed")
		}
		if _, reason := c.admit(extra, "new-channel", "master", now); reason != capacityReasonFull {
			t.Fatalf("new channel reason = %q", reason)
		}
		c.release(extra, now)
	}
	c.release(master, now)
	c.release(slave, now)
}

func TestCapacityFirstMasterAlsoReceivesComplementarySlave(t *testing.T) {
	c := testCapacity(t, 4)
	now := time.Unix(200, 0)
	master := admitTestClient(t, c, "channel-one", "master", now)
	_ = admitTestClient(t, c, "channel-two", "slave", now)
	slave := admitTestClient(t, c, "channel-one", "slave", now)
	c.release(master, now)
	c.release(slave, now)
}

func TestCapacityExtrasNeedBestEffortCredit(t *testing.T) {
	c := testCapacity(t, 3)
	now := time.Unix(300, 0)
	_ = admitTestClient(t, c, "channel", "master", now)
	_ = admitTestClient(t, c, "channel", "slave", now)
	extra := &client{}
	if !c.reserveAt(extra, false, now) || !c.beginAdmissionAt(extra, now) {
		t.Fatal("pending setup failed")
	}
	if _, reason := c.admit(extra, "channel", "master", now); reason != "" {
		t.Fatalf("best-effort extra rejected with room: %s", reason)
	}
	if got := c.stats().GuaranteedEntitlementUsage; got != 2 {
		t.Fatalf("guaranteed usage = %d, want 2", got)
	}
	if got := c.stats().OrdinaryCapacityUsage; got != 1 {
		t.Fatalf("ordinary extra usage = %d, want 1", got)
	}
}

func TestCapacityRecoveryRetainsPairCreditsAgainstNewChannels(t *testing.T) {
	c := testCapacity(t, 2)
	now := time.Unix(400, 0)
	master := admitTestClient(t, c, "recover", "master", now)
	slave := admitTestClient(t, c, "recover", "slave", now)
	c.release(master, now)
	c.release(slave, now)
	attacker := &client{}
	if !c.reserveAt(attacker, false, now.Add(time.Second)) || !c.beginAdmissionAt(attacker, now.Add(time.Second)) {
		t.Fatal("pending setup failed")
	}
	if _, reason := c.admit(attacker, "attacker", "master", now.Add(time.Second)); reason != capacityReasonFull {
		t.Fatalf("attacker admitted: %q", reason)
	}
	c.release(attacker, now.Add(time.Second))
	reconnect := admitTestClient(t, c, "recover", "master", now.Add(2*time.Second))
	if got := c.stats().RecoveryAdmissions; got != 1 {
		t.Fatalf("recovery admissions = %d, want 1", got)
	}
	c.release(reconnect, now.Add(2*time.Second))
}

func TestCapacityRecoveryExpiresOnceAndFreesObligation(t *testing.T) {
	c := testCapacity(t, 2)
	now := time.Unix(500, 0)
	conn := admitTestClient(t, c, "recover", "master", now)
	c.release(conn, now)
	if removed := c.expire(now.Add(59 * time.Second)); removed != 0 {
		t.Fatalf("early expiration count = %d", removed)
	}
	if removed := c.expire(now.Add(time.Minute)); removed != 1 {
		t.Fatalf("expiration count = %d", removed)
	}
	if removed := c.expire(now.Add(time.Minute)); removed != 0 {
		t.Fatalf("second expiration count = %d", removed)
	}
	_ = admitTestClient(t, c, "new", "master", now.Add(time.Minute))
}

func TestCapacityConfiguredReservationAndOverlap(t *testing.T) {
	c := testCapacity(t, 3, "Protected Key")
	now := time.Unix(600, 0)
	master := admitTestClient(t, c, "Protected Key", "master", now)
	_ = admitTestClient(t, c, "Protected Key", "slave", now)
	ghost := admitTestClient(t, c, "Protected Key", "master", now)
	extra := &client{}
	if !c.reserveAt(extra, false, now) || !c.beginAdmissionAt(extra, now) {
		t.Fatal("pending setup failed")
	}
	if _, reason := c.admit(extra, "Protected Key", "master", now); reason != capacityReasonFull {
		t.Fatalf("second overlap got through: %q", reason)
	}
	c.release(extra, now)
	if got := c.stats().OverlapUsage; got != 1 {
		t.Fatalf("overlap usage = %d, want 1", got)
	}
	if c.overlapValid(ghost, now.Add(9*time.Second)) {
		t.Fatal("overlap expired early")
	}
	if !c.overlapValid(ghost, now.Add(10*time.Second)) {
		t.Fatal("expired duplicate overlap should require disconnect")
	}
	c.release(master, now.Add(10*time.Second))
	if c.overlapValid(ghost, now.Add(11*time.Second)) {
		t.Fatal("overlap remains after pair fits")
	}
	if got := c.stats().ConfiguredReservations; got != 1 {
		t.Fatalf("configured reservations = %d", got)
	}
	if removed := c.expire(now.Add(24 * time.Hour)); removed != 0 {
		t.Fatalf("configured reservation expired: %d", removed)
	}
}

func TestCapacityConfiguredOverlapUsesOrdinaryCreditFirst(t *testing.T) {
	c := testCapacity(t, 4, "protected")
	now := time.Unix(650, 0)
	_ = admitTestClient(t, c, "protected", "master", now)
	_ = admitTestClient(t, c, "protected", "slave", now)
	_ = admitTestClient(t, c, "protected", "master", now)
	if got := c.stats().OverlapUsage; got != 0 {
		t.Fatalf("overlap used despite ordinary credit: %d", got)
	}
	if got := c.stats().OrdinaryCapacityUsage; got != 1 {
		t.Fatalf("ordinary extra usage = %d, want 1", got)
	}
}

func TestCapacityFailedJoinRollsBackWithoutExtendingRecovery(t *testing.T) {
	c := testCapacity(t, 2)
	now := time.Unix(675, 0)
	first := admitTestClient(t, c, "recover", "master", now)
	c.release(first, now)
	expires := c.nextExpiry()

	failed := &client{}
	if !c.reserveAt(failed, false, now.Add(time.Second)) || !c.beginAdmissionAt(failed, now.Add(time.Second)) {
		t.Fatal("pending setup failed")
	}
	if _, reason := c.admit(failed, "recover", "monitor", now.Add(time.Second)); reason != capacityReasonFull {
		t.Fatalf("duplicate should need extra credit: %q", reason)
	}
	if got := c.stats().AdmittedConnections; got != 0 {
		t.Fatalf("failed admission leaked admitted permit: %d", got)
	}
	c.release(failed, now.Add(time.Second))
	if got := c.nextExpiry(); !got.Equal(expires) {
		t.Fatalf("failed attempt changed grace deadline from %v to %v", expires, got)
	}
	if got := c.stats().RecoveryAdmissions; got != 0 {
		t.Fatalf("failed recovery counted: %d", got)
	}

	recovered := admitTestClient(t, c, "recover", "slave", now.Add(2*time.Second))
	if got := c.stats().RecoveryAdmissions; got != 1 {
		t.Fatalf("successful recovery count = %d", got)
	}
	c.release(recovered, now.Add(2*time.Second))
}

func TestCapacityPrejoinStageBoundsAndStopAdmission(t *testing.T) {
	cfg := DefaultAdmissionConfig()
	cfg.HardLimit, cfg.PendingLimit, cfg.AdmittedLimit = 3, 2, 1
	cfg.TLSLimit, cfg.StartupLimit = 1, 2
	c, err := newCapacity(cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(680, 0)
	first, second := &client{}, &client{}
	if !c.reserveAt(first, true, now) {
		t.Fatal("first TLS permit rejected")
	}
	if c.reserveAt(second, true, now) {
		t.Fatal("TLS limit exceeded")
	}
	if got := c.stats().TLSCapacityRejects; got != 1 {
		t.Fatalf("TLS rejects = %d", got)
	}
	if !c.establishedAt(first, now) {
		t.Fatal("TLS establishment failed")
	}
	c.stopAdmission()
	if ready, wake := c.acceptReady(); ready {
		t.Fatal("accept remained enabled after stop")
	} else {
		select {
		case <-wake:
		default:
			t.Fatal("stop did not wake accept loop")
		}
	}
	if c.beginAdmissionAt(first, now) {
		t.Fatal("admission began after shutdown fence")
	}
	if c.reserveAt(second, false, now) {
		t.Fatal("reserve succeeded after shutdown fence")
	}
	c.release(first, now)
}

func TestCapacityStartupAndPhysicalBounds(t *testing.T) {
	cfg := DefaultAdmissionConfig()
	cfg.HardLimit, cfg.PendingLimit, cfg.AdmittedLimit = 3, 2, 1
	cfg.TLSLimit, cfg.StartupLimit = 2, 1
	c, err := newCapacity(cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(690, 0)
	tlsOne, tlsTwo := &client{}, &client{}
	if !c.reserveAt(tlsOne, true, now) || !c.reserveAt(tlsTwo, true, now) {
		t.Fatal("TLS reservation failed")
	}
	if !c.establishedAt(tlsOne, now) {
		t.Fatal("first startup failed")
	}
	if c.establishedAt(tlsTwo, now) {
		t.Fatal("startup limit exceeded")
	}
	if got := c.stats().StartupCapacityRejects; got != 1 {
		t.Fatalf("startup rejects = %d", got)
	}
	if got := c.stats().PhysicalActive; got != 2 {
		t.Fatalf("physical active = %d, want 2", got)
	}
	if c.reserveAt(&client{}, true, now) {
		t.Fatal("pending physical bound exceeded")
	}
	if got := c.stats().PhysicalRejects; got != 1 {
		t.Fatalf("physical rejects = %d", got)
	}
	c.release(tlsOne, now)
	c.release(tlsTwo, now)
}

func TestCapacityProtectedKeyUsesExactStringBytes(t *testing.T) {
	c := testCapacity(t, 3, "Ä Key")
	now := time.Unix(700, 0)
	if _, ok := c.configured[channelDigest("Ä Key")]; !ok {
		t.Fatal("exact key was not configured")
	}
	if _, ok := c.configured[channelDigest("ä Key")]; ok {
		t.Fatal("case-folded key unexpectedly configured")
	}
	if _, ok := c.configured[channelDigest("Ä Key ")]; ok {
		t.Fatal("trimmed key unexpectedly configured")
	}
	if got := len(c.channels); got != 1 {
		t.Fatalf("stored channel metadata = %d, want 1", got)
	}
	_ = admitTestClient(t, c, "Ä Key", "slave", now)
}

func TestCapacityStartupStagesAndDeadline(t *testing.T) {
	c := testCapacity(t, 4)
	now := time.Unix(800, 0)
	conn := &client{}
	if !c.reserveAt(conn, true, now) {
		t.Fatal("TLS reserve failed")
	}
	if got := c.stats().PendingTLS; got != 1 {
		t.Fatalf("pending TLS = %d", got)
	}
	if !c.establishedAt(conn, now) {
		t.Fatal("TLS establishment failed")
	}
	if !c.beginAdmissionAt(conn, now.Add(9*time.Second)) {
		t.Fatal("startup ended before deadline")
	}
	if _, reason := c.admit(conn, "idle", "master", now.Add(9*time.Second)); reason != "" {
		t.Fatalf("admit failed: %s", reason)
	}
	c.markSuccessful(conn)
	if c.timeoutAt(conn, now.Add(time.Hour)) {
		t.Fatal("joined client should not time out")
	}
	c.release(conn, now.Add(time.Hour))

	stall := &client{}
	if !c.reserveAt(stall, true, now) || !c.establishedAt(stall, now) {
		t.Fatal("stall setup failed")
	}
	if c.beginAdmissionAt(stall, now.Add(10*time.Second)) {
		t.Fatal("startup admitted at its deadline")
	}
	if got := c.stats().StartupTimeouts; got != 1 {
		t.Fatalf("startup timeouts = %d, want 1", got)
	}
	if _, reason := c.admit(stall, "no", "master", now.Add(10*time.Second)); reason != capacityReasonBadState {
		t.Fatalf("timed out admission reason = %q", reason)
	}
}

func TestCapacityConcurrentOrdinaryAdmissionCannotDoubleSpend(t *testing.T) {
	c := testCapacity(t, 2)
	now := time.Unix(900, 0)
	const contenders = 24
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := 0
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn := &client{}
			if !c.reserveAt(conn, false, now) || !c.beginAdmissionAt(conn, now) {
				return
			}
			if _, reason := c.admit(conn, strings.Repeat("x", i+1), "master", now); reason == "" {
				mu.Lock()
				accepted++
				mu.Unlock()
				c.markSuccessful(conn)
			}
			c.release(conn, now)
		}(i)
	}
	wg.Wait()
	if accepted != 1 {
		t.Fatalf("accepted channels = %d, want 1", accepted)
	}
	if got := c.stats().GuaranteedEntitlementUsage; got != 2 {
		t.Fatalf("usage after disconnect = %d, want 2", got)
	}
}

func TestCapacityComplementWinsAgainstConcurrentNewChannel(t *testing.T) {
	c := testCapacity(t, 4)
	now := time.Unix(925, 0)
	slave := admitTestClient(t, c, "paired", "slave", now)
	filler := admitTestClient(t, c, "filler", "master", now)
	var wg sync.WaitGroup
	results := make(chan string, 2)
	for _, candidate := range []struct{ key, role string }{{"paired", "master"}, {"attacker", "master"}} {
		wg.Add(1)
		go func(key, role string) {
			defer wg.Done()
			conn := &client{}
			if !c.reserveAt(conn, false, now) || !c.beginAdmissionAt(conn, now) {
				results <- "pending"
				return
			}
			_, reason := c.admit(conn, key, role, now)
			if reason == "" {
				c.markSuccessful(conn)
			}
			results <- reason
			c.release(conn, now)
		}(candidate.key, candidate.role)
	}
	wg.Wait()
	close(results)
	complementAccepted := false
	newRejected := false
	for reason := range results {
		if reason == "" {
			complementAccepted = true
		}
		if reason == capacityReasonFull {
			newRejected = true
		}
	}
	if !complementAccepted || !newRejected {
		t.Fatalf("complement accepted=%v new rejected=%v", complementAccepted, newRejected)
	}
	c.release(slave, now)
	c.release(filler, now)
}

func TestCapacityConcurrentRecoveryAndExpirationBoundary(t *testing.T) {
	c := testCapacity(t, 2)
	now := time.Unix(950, 0)
	first := admitTestClient(t, c, "recover", "master", now)
	c.release(first, now)
	expiry := c.nextExpiry()

	var wg sync.WaitGroup
	var accepted int
	var acceptedMu sync.Mutex
	var acceptedClients []*client
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := &client{}
			if !c.reserveAt(conn, false, expiry.Add(-time.Second)) || !c.beginAdmissionAt(conn, expiry.Add(-time.Second)) {
				return
			}
			if _, reason := c.admit(conn, "recover", "master", expiry.Add(-time.Second)); reason == "" {
				c.markSuccessful(conn)
				acceptedMu.Lock()
				accepted++
				acceptedClients = append(acceptedClients, conn)
				acceptedMu.Unlock()
			} else {
				c.release(conn, expiry.Add(-time.Second))
			}
		}()
	}
	wg.Wait()
	if accepted != 1 {
		t.Fatalf("concurrent recoveries accepted = %d, want 1", accepted)
	}
	for _, conn := range acceptedClients {
		c.release(conn, expiry.Add(-time.Second))
	}
	newExpiry := c.nextExpiry()
	if newExpiry.IsZero() || c.expire(newExpiry) != 1 {
		t.Fatal("recovered reservation did not expire at its exact boundary")
	}
	if got := c.stats().GenericReservations; got != 0 {
		t.Fatalf("generic reservations after expiration = %d", got)
	}
}

func TestCapacityStartupTimeoutRacingAdmission(t *testing.T) {
	c := testCapacity(t, 2)
	now := time.Unix(975, 0)
	conn := &client{}
	if !c.reserveAt(conn, true, now) || !c.establishedAt(conn, now) || !c.beginAdmissionAt(conn, now) {
		t.Fatal("setup failed")
	}
	deadline := now.Add(10 * time.Second)
	var wg sync.WaitGroup
	var admitted bool
	var timedOut bool
	var mu sync.Mutex
	start := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, reason := c.admit(conn, "race", "master", deadline)
		mu.Lock()
		admitted = reason == ""
		mu.Unlock()
	}()
	go func() { defer wg.Done(); <-start; timedOut = c.timeoutAt(conn, deadline) }()
	close(start)
	wg.Wait()
	if admitted {
		t.Fatal("admission succeeded at absolute startup deadline")
	}
	_ = timedOut // timeout may observe the admission path having already marked the deadline.
	if got := c.stats().AdmittedConnections; got != 0 {
		t.Fatalf("admitted after timeout race = %d", got)
	}
	c.release(conn, deadline)
}

func TestCapacityValidationAndCleanupIdempotence(t *testing.T) {
	for _, interval := range []time.Duration{time.Nanosecond, 2 * time.Nanosecond, 3 * time.Nanosecond} {
		cfg := DefaultAdmissionConfig()
		cfg.RejectionInterval = interval
		if _, err := newCapacity(cfg); err == nil {
			t.Fatal("zero response spacing accepted")
		}
	}
	cfg := DefaultAdmissionConfig()
	cfg.HardLimit = 8
	cfg.PendingLimit = 5
	cfg.AdmittedLimit = 4
	if _, err := newCapacity(cfg); err == nil {
		t.Fatal("inconsistent hard/pending/admitted limits accepted")
	}
	cfg = DefaultAdmissionConfig()
	cfg.AdmittedLimit = 2
	cfg.ProtectedChannels = []string{channelDigest("a")}
	if _, err := newCapacity(cfg); err == nil {
		t.Fatal("oversubscribed configured reservation accepted")
	}

	c := testCapacity(t, 2)
	now := time.Unix(1000, 0)
	conn := admitTestClient(t, c, "one", "master", now)
	c.cleaning(conn)
	if got := c.stats().AdmittedConnections; got != 1 {
		t.Fatalf("cleanup released admitted permit early: %d", got)
	}
	if !c.release(conn, now) || c.release(conn, now) {
		t.Fatal("release was not exactly once")
	}
	if got := c.stats().PhysicalActive; got != 0 {
		t.Fatalf("physical active after cleanup = %d", got)
	}
}

func TestCapacitySharedExpiryOwnsOnlyGenericReservations(t *testing.T) {
	c := testCapacity(t, 3)
	now := time.Unix(1100, 0)
	stalled := &client{}
	if !c.reserveAt(stalled, false, now) {
		t.Fatal("reserve")
	}
	if !c.nextExpiry().IsZero() {
		t.Fatal("shared timer would spin on startup deadline")
	}
	c.release(stalled, now)
	config := DefaultAdmissionConfig()
	config.AdmittedLimit = 3
	config.ProtectedChannels = []string{channelDigest("configured")}
	c, err := newCapacity(config)
	if err != nil {
		t.Fatal(err)
	}
	admitTestClient(t, c, "configured", "master", now)
	admitTestClient(t, c, "configured", "slave", now)
	overlap := admitTestClient(t, c, "configured", "master", now)
	if !c.overlapValid(overlap, now.Add(config.OverlapTimeout)) {
		t.Fatal("no overlap expiry")
	}
	c.cleaning(overlap)
	if !c.nextExpiry().IsZero() {
		t.Fatal("shared timer would spin while expired overlap cleans up")
	}
}
