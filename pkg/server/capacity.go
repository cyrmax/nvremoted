// Copyright © 2026
//
// This source code is governed by the MIT license that can be found in the LICENSE file.

package server

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

const (
	capacityReasonFull     = "capacity"
	capacityReasonPhysical = "physical_limit"
	capacityReasonTimedOut = "startup_timeout"
	capacityReasonBadState = "invalid_state"
)

// AdmissionConfig controls server resource policy. Protected channel values are
// lowercase or uppercase hexadecimal SHA-256 digests of the exact UTF-8
// channel keys. The keys themselves are never retained by the manager.
type AdmissionConfig struct {
	HardLimit         int
	PendingLimit      int
	TLSLimit          int
	StartupLimit      int
	AdmittedLimit     int
	RecoveryGrace     time.Duration
	StartupTimeout    time.Duration
	RejectionWindow   time.Duration
	RejectionInterval time.Duration
	OverlapTimeout    time.Duration
	ProtectedChannels []string
}

// DefaultAdmissionConfig returns the documented server policy defaults.
func DefaultAdmissionConfig() AdmissionConfig {
	return AdmissionConfig{
		HardLimit: 1152, PendingLimit: 128, TLSLimit: 64,
		StartupLimit: 128, AdmittedLimit: 1024,
		RecoveryGrace: 60 * time.Second, StartupTimeout: 10 * time.Second,
		RejectionWindow: 5 * time.Second, RejectionInterval: time.Second,
		OverlapTimeout: 10 * time.Second,
	}
}

type capacityState uint8

const (
	capacityTLS capacityState = iota + 1
	capacityStartup
	capacityAdmission
	capacityRejection
	capacityAdmitted
	capacityCleaning
)

type capacityClient struct {
	state       capacityState
	admitted    bool
	deadline    time.Time
	timedOut    bool
	key         string
	role        string
	successful  bool
	recovered   bool
	priorExpiry time.Time
	newEntry    bool
	overlap     bool
}

type channelEntitlement struct {
	configured     bool
	masters        int
	slaves         int
	unknown        int
	successful     int
	everSuccessful bool
	recoveryExpiry time.Time
	expires        time.Time
	overlap        *client
	overlapEnd     time.Time
}

// CapacityStats intentionally contains fixed, low-cardinality numeric fields.
type CapacityStats struct {
	PhysicalActive             int    `json:"physical_active"`
	PendingTLS                 int    `json:"pending_tls"`
	PendingStartup             int    `json:"pending_startup"`
	PendingAdmission           int    `json:"pending_admission"`
	PendingRejection           int    `json:"pending_rejection"`
	PendingCleanup             int    `json:"pending_cleanup"`
	AdmittedCleanup            int    `json:"admitted_cleanup"`
	AdmittedConnections        int    `json:"admitted_connections"`
	ActiveChannels             int    `json:"active_channels"`
	OrdinaryCapacityUsage      int    `json:"ordinary_capacity_usage"`
	GuaranteedEntitlementUsage int    `json:"guaranteed_entitlement_usage"`
	GenericReservations        int    `json:"generic_reservations"`
	ConfiguredReservations     int    `json:"configured_reservations"`
	OverlapUsage               int    `json:"overlap_usage"`
	CapacityRejects            uint64 `json:"capacity_rejects"`
	PhysicalRejects            uint64 `json:"physical_rejects"`
	TLSCapacityRejects         uint64 `json:"tls_capacity_rejects"`
	StartupCapacityRejects     uint64 `json:"startup_capacity_rejects"`
	StartupTimeouts            uint64 `json:"startup_timeouts"`
	RecoveryAdmissions         uint64 `json:"recovery_admissions"`
	PendingHighWater           int    `json:"pending_high_water"`
	AdmittedHighWater          int    `json:"admitted_high_water"`
	PhysicalHighWater          int    `json:"physical_high_water"`
}

type capacity struct {
	mu          sync.Mutex
	config      AdmissionConfig
	clients     map[*client]*capacityClient
	channels    map[string]*channelEntitlement
	configured  map[string]struct{}
	changed     chan struct{}
	ready       chan struct{}
	metrics     CapacityStats
	closed      bool
	stopping    bool
	readyClosed bool
}

func newCapacity(config AdmissionConfig) (*capacity, error) {
	defaults := DefaultAdmissionConfig()
	if config.HardLimit == 0 {
		config.HardLimit = defaults.HardLimit
	}
	if config.PendingLimit == 0 {
		config.PendingLimit = defaults.PendingLimit
	}
	if config.TLSLimit == 0 {
		config.TLSLimit = defaults.TLSLimit
	}
	if config.StartupLimit == 0 {
		config.StartupLimit = defaults.StartupLimit
	}
	if config.AdmittedLimit == 0 {
		config.AdmittedLimit = defaults.AdmittedLimit
	}
	if config.RecoveryGrace == 0 {
		config.RecoveryGrace = defaults.RecoveryGrace
	}
	if config.StartupTimeout == 0 {
		config.StartupTimeout = defaults.StartupTimeout
	}
	if config.RejectionWindow == 0 {
		config.RejectionWindow = defaults.RejectionWindow
	}
	if config.RejectionInterval == 0 {
		config.RejectionInterval = defaults.RejectionInterval
	}
	if config.OverlapTimeout == 0 {
		config.OverlapTimeout = defaults.OverlapTimeout
	}
	if config.HardLimit < 0 || config.PendingLimit < 0 || config.TLSLimit < 0 || config.StartupLimit < 0 || config.AdmittedLimit < 0 {
		return nil, errors.New("admission limits cannot be negative")
	}
	if config.RecoveryGrace < 0 || config.StartupTimeout < 0 || config.RejectionWindow < 0 || config.RejectionInterval < 0 || config.OverlapTimeout < 0 {
		return nil, errors.New("admission durations cannot be negative")
	}
	if config.TLSLimit > config.PendingLimit || config.StartupLimit > config.PendingLimit || config.AdmittedLimit > config.HardLimit || config.PendingLimit > config.HardLimit {
		return nil, errors.New("admission limits are inconsistent")
	}
	if config.PendingLimit > config.HardLimit-config.AdmittedLimit {
		return nil, errors.New("hard limit must cover pending plus admitted limits")
	}
	if config.RejectionWindow > 0 && (config.RejectionInterval <= 0 || config.RejectionInterval > config.RejectionWindow) {
		return nil, errors.New("rejection interval must be positive and no longer than the rejection window")
	}
	if config.RejectionInterval < 4*time.Nanosecond {
		return nil, errors.New("rejection interval must be at least 4ns to preserve nonzero response spacing")
	}
	if len(config.ProtectedChannels) > config.AdmittedLimit/3 {
		return nil, errors.New("configured protected channel entitlements exceed admitted capacity")
	}
	c := &capacity{
		config: config, clients: make(map[*client]*capacityClient),
		channels: make(map[string]*channelEntitlement), configured: make(map[string]struct{}),
		changed: make(chan struct{}, 1),
		ready:   make(chan struct{}),
	}
	for _, key := range config.ProtectedChannels {
		digestBytes, err := hex.DecodeString(key)
		if err != nil || len(digestBytes) != sha256.Size {
			return nil, errors.New("protected channel identifiers must be 64-character SHA-256 hex digests")
		}
		digest := hex.EncodeToString(digestBytes)
		if _, exists := c.configured[digest]; exists {
			return nil, errors.New("duplicate protected channel digest")
		}
		c.configured[digest] = struct{}{}
		c.channels[digest] = &channelEntitlement{configured: true}
	}
	c.config.ProtectedChannels = nil
	if len(c.configured) > config.AdmittedLimit/3 {
		return nil, errors.New("configured protected channel entitlements exceed admitted capacity")
	}
	return c, nil
}

// Validate checks the effective admission policy after applying defaults for
// fields left at zero in a TOML/API configuration.
func (config AdmissionConfig) Validate() error {
	_, err := newCapacity(config)
	return err
}

func channelDigest(key string) string {
	digest := sha256.Sum256([]byte(key))
	return hex.EncodeToString(digest[:])
}

func (c *capacity) reserve(conn *client, tls bool) bool {
	return c.reserveAt(conn, tls, time.Now())
}

func (c *capacity) reserveAt(conn *client, tls bool, now time.Time) bool {
	if conn == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.stopping || c.clients[conn] != nil {
		return false
	}
	if c.pendingLocked() >= c.config.PendingLimit || len(c.clients) >= c.config.HardLimit {
		c.metrics.PhysicalRejects++
		return false
	}
	state := capacityStartup
	if tls {
		if c.countStateLocked(capacityTLS) >= c.config.TLSLimit {
			c.metrics.TLSCapacityRejects++
			return false
		}
		state = capacityTLS
	} else if c.countStateLocked(capacityStartup) >= c.config.StartupLimit {
		c.metrics.StartupCapacityRejects++
		return false
	}
	clientState := &capacityClient{state: state}
	if state == capacityStartup {
		clientState.deadline = now.Add(c.config.StartupTimeout)
	}
	c.clients[conn] = clientState
	c.updateHighWaterLocked()
	if state == capacityStartup {
		c.signalLocked()
	}
	return true
}

func (c *capacity) established(conn *client) bool { return c.establishedAt(conn, time.Now()) }

func (c *capacity) establishedAt(conn *client, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.clients[conn]
	if state == nil || state.state != capacityTLS || c.countStateLocked(capacityStartup) >= c.config.StartupLimit {
		if state != nil && state.state == capacityTLS && c.countStateLocked(capacityStartup) >= c.config.StartupLimit {
			c.metrics.StartupCapacityRejects++
		}
		return false
	}
	state.state = capacityStartup
	state.deadline = now.Add(c.config.StartupTimeout)
	c.signalLocked()
	return true
}

func (c *capacity) beginAdmission(conn *client) bool { return c.beginAdmissionAt(conn, time.Now()) }

func (c *capacity) beginAdmissionAt(conn *client, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.clients[conn]
	if c.closed || c.stopping || state == nil || state.state != capacityStartup || state.timedOut || (!state.deadline.IsZero() && !now.Before(state.deadline)) {
		if state != nil && !state.timedOut && !state.deadline.IsZero() && !now.Before(state.deadline) {
			state.timedOut = true
			c.metrics.StartupTimeouts++
		}
		return false
	}
	state.state = capacityAdmission
	return true
}

// timeout atomically prevents a TLS-established startup client from entering a channel.
func (c *capacity) timeout(conn *client) bool { return c.timeoutAt(conn, time.Now()) }

func (c *capacity) startupDeadline(conn *client) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if state := c.clients[conn]; state != nil && !state.admitted {
		return state.deadline
	}
	return time.Time{}
}

func (c *capacity) timeoutAt(conn *client, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.clients[conn]
	if state == nil || (state.state != capacityStartup && state.state != capacityAdmission) || state.timedOut {
		return false
	}
	if !state.deadline.IsZero() && now.Before(state.deadline) {
		return false
	}
	state.timedOut = true
	c.metrics.StartupTimeouts++
	return true
}

// admit atomically transfers a pre-join permit to the admitted pool and reserves
// the channel entitlement before any channel membership is published.
func (c *capacity) admit(conn *client, key, role string, now time.Time) (bool, string) {
	digest := channelDigest(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.clients[conn]
	if c.closed || c.stopping || state == nil || state.state != capacityAdmission || state.timedOut {
		return false, capacityReasonBadState
	}
	if !state.deadline.IsZero() && !now.Before(state.deadline) {
		state.timedOut = true
		c.metrics.StartupTimeouts++
		return false, capacityReasonTimedOut
	}
	if countAdmitted(c.clients) >= c.config.AdmittedLimit {
		c.metrics.PhysicalRejects++
		c.metrics.CapacityRejects++
		return false, capacityReasonFull
	}
	c.expireLocked(now)
	entry := c.channels[digest]
	if entry == nil {
		entry = &channelEntitlement{configured: false}
		if _, configured := c.configured[digest]; configured {
			entry.configured = true
		}
		c.channels[digest] = entry
		state.newEntry = true
	} else if !entry.configured && entry.masters+entry.slaves+entry.unknown == 0 && !entry.expires.IsZero() {
		state.priorExpiry = entry.expires
		entry.recoveryExpiry = entry.expires
		entry.expires = time.Time{}
		state.recovered = true
	}
	state.key, state.role = digest, role
	state.state = capacityAdmitted
	countRole(entry, role, 1)
	state.overlap = false
	if entry.configured && duplicateRole(entry, role) && c.usageLocked() > c.config.AdmittedLimit && entry.overlap == nil {
		entry.overlap, entry.overlapEnd = conn, now.Add(c.config.OverlapTimeout)
		state.overlap = true
	}
	usage := c.usageLocked()
	if usage > c.config.AdmittedLimit {
		countRole(entry, role, -1)
		if state.overlap {
			entry.overlap = nil
			entry.overlapEnd = time.Time{}
		}
		state.state, state.key, state.role, state.overlap, state.admitted = capacityAdmission, "", "", false, false
		if state.recovered && entry.recoveryExpiry.IsZero() {
			entry.recoveryExpiry = state.priorExpiry
		}
		if !entry.configured && entry.masters+entry.slaves+entry.unknown == 0 {
			if !entry.recoveryExpiry.IsZero() {
				entry.expires, entry.recoveryExpiry = entry.recoveryExpiry, time.Time{}
				c.signalLocked()
			} else if state.newEntry {
				delete(c.channels, digest)
				state.newEntry = false
			}
		}
		c.metrics.CapacityRejects++
		return false, capacityReasonFull
	}
	state.admitted = true
	c.signalReadyLocked()
	c.updateHighWaterLocked()
	if state.overlap {
		c.signalLocked()
	}
	return state.overlap, ""
}

func (c *capacity) markSuccessful(conn *client) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if state := c.clients[conn]; state != nil && state.state == capacityAdmitted && !state.successful {
		state.successful = true
		if entry := c.channels[state.key]; entry != nil {
			entry.successful++
			entry.everSuccessful = true
			entry.expires = time.Time{}
			if state.recovered {
				c.metrics.RecoveryAdmissions++
				entry.recoveryExpiry = time.Time{}
				state.recovered = false
			}
		}
	}
}

func (c *capacity) rejecting(conn *client) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.clients[conn]
	if state == nil || state.state == capacityAdmitted || state.state == capacityCleaning {
		return false
	}
	state.state = capacityRejection
	return true
}

func (c *capacity) cleaning(conn *client) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if state := c.clients[conn]; state != nil {
		state.state = capacityCleaning
	}
}

// release is called only after client workers and channel cleanup have completed.
// It is idempotent and creates grace metadata only for a successfully admitted
// final member. Provisional failed joins restore, but never extend, prior grace.
func (c *capacity) release(conn *client, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.clients[conn]
	if state == nil {
		return false
	}
	if state.admitted {
		entry := c.channels[state.key]
		if entry != nil {
			countRole(entry, state.role, -1)
			if entry.overlap != nil {
				if overlapState := c.clients[entry.overlap]; overlapState != nil && !duplicateStillPresent(entry, overlapState.role) {
					overlapState.overlap = false
					entry.overlap, entry.overlapEnd = nil, time.Time{}
				}
			}
			if state.successful {
				entry.successful--
				if !entry.configured && entry.successful == 0 {
					entry.expires = now.Add(c.config.RecoveryGrace)
					c.signalLocked()
				}
			}
			if entry.overlap == conn {
				entry.overlap = nil
				entry.overlapEnd = time.Time{}
			}
			if !entry.configured && entry.masters+entry.slaves+entry.unknown == 0 && entry.successful == 0 {
				if entry.everSuccessful {
					if entry.expires.IsZero() && !entry.recoveryExpiry.IsZero() {
						entry.expires, entry.recoveryExpiry = entry.recoveryExpiry, time.Time{}
						c.signalLocked()
					}
				} else if !entry.recoveryExpiry.IsZero() {
					entry.expires, entry.recoveryExpiry = entry.recoveryExpiry, time.Time{}
					c.signalLocked()
				} else if !entry.expires.IsZero() {
					c.signalLocked()
				} else {
					delete(c.channels, state.key)
				}
			}
		}
	}
	delete(c.clients, conn)
	c.signalReadyLocked()
	return true
}

// overlapValid reports whether an expired configured recovery overlap still
// exceeds the 1-master/1-slave pair. A true result requires terminating conn.
func (c *capacity) overlapValid(conn *client, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.clients[conn]
	if state == nil || !state.overlap {
		return false
	}
	entry := c.channels[state.key]
	if entry == nil || entry.overlap != conn {
		state.overlap = false
		return false
	}
	if !duplicateStillPresent(entry, state.role) {
		entry.overlap = nil
		entry.overlapEnd = time.Time{}
		state.overlap = false
		return false
	}
	return !now.Before(entry.overlapEnd)
}

func (c *capacity) overlapDeadline(conn *client) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.clients[conn]
	if state == nil || !state.overlap {
		return time.Time{}
	}
	if entry := c.channels[state.key]; entry != nil && entry.overlap == conn {
		return entry.overlapEnd
	}
	return time.Time{}
}

func (c *capacity) expire(now time.Time) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.expireLocked(now)
}

func (c *capacity) expireLocked(now time.Time) int {
	removed := 0
	for key, entry := range c.channels {
		if entry.configured || entry.masters+entry.slaves+entry.unknown != 0 || entry.expires.IsZero() || now.Before(entry.expires) {
			continue
		}
		delete(c.channels, key)
		removed++
	}
	if removed > 0 {
		c.signalLocked()
	}
	return removed
}

// nextExpiry schedules only obligations processed by expire. Per-client startup
// and overlap watchdogs own their deadlines, including while cleanup is blocked.
func (c *capacity) nextExpiry() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	var next time.Time
	for _, entry := range c.channels {
		if entry.configured || entry.masters+entry.slaves+entry.unknown != 0 || entry.expires.IsZero() {
			continue
		}
		if next.IsZero() || entry.expires.Before(next) {
			next = entry.expires
		}
	}
	return next
}

func (c *capacity) shutdown() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopping = true
	c.closed = true
	c.signalReadyLocked()
	for key, entry := range c.channels {
		if !entry.configured {
			delete(c.channels, key)
		}
	}
	c.signalLocked()
}

// stopAdmission prevents new clients from progressing while allowing existing
// client cleanup and final stats collection to complete.
func (c *capacity) stopAdmission() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopping {
		return
	}
	c.stopping = true
	c.signalReadyLocked()
}

func (c *capacity) statsSnapshot() CapacityStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.metrics
	out.PhysicalActive = len(c.clients)
	for _, state := range c.clients {
		if state.admitted {
			out.AdmittedConnections++
		}
		switch state.state {
		case capacityTLS:
			out.PendingTLS++
		case capacityStartup:
			out.PendingStartup++
		case capacityAdmission:
			out.PendingAdmission++
		case capacityRejection:
			out.PendingRejection++
		case capacityCleaning:
			if state.admitted {
				out.AdmittedCleanup++
			} else {
				out.PendingCleanup++
			}
		}
	}
	out.ActiveChannels = 0
	out.OrdinaryCapacityUsage = 0
	out.GuaranteedEntitlementUsage = 0
	out.GenericReservations = 0
	out.ConfiguredReservations = 0
	out.OverlapUsage = 0
	for _, entry := range c.channels {
		if entry.masters+entry.slaves+entry.unknown > 0 {
			out.ActiveChannels++
		}
		cost := entitlementCost(entry)
		if entry.configured {
			out.GuaranteedEntitlementUsage += 3
			out.OrdinaryCapacityUsage += cost - 3
		} else {
			out.GuaranteedEntitlementUsage += 2
			out.OrdinaryCapacityUsage += cost - 2
		}
		if entry.configured {
			out.ConfiguredReservations++
		} else if entry.masters+entry.slaves+entry.unknown == 0 && !entry.expires.IsZero() {
			out.GenericReservations++
		}
		if entry.overlap != nil {
			out.OverlapUsage++
		}
	}
	return out
}

func (c *capacity) stats() CapacityStats { return c.statsSnapshot() }

// acceptReady is an accept-loop preflight. It prevents Accept from creating a
// server-owned connection above the absolute physical or pending ceiling.
// The returned channel is broadcast when capacity changes or shutdown starts.
func (c *capacity) acceptReady() (bool, <-chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.stopping {
		return false, c.ready
	}
	return c.pendingLocked() < c.config.PendingLimit && len(c.clients) < c.config.HardLimit, c.ready
}

func (c *capacity) pendingLocked() int { return len(c.clients) - countAdmitted(c.clients) }

func countAdmitted(clients map[*client]*capacityClient) int {
	n := 0
	for _, state := range clients {
		if state.admitted {
			n++
		}
	}
	return n
}

func (c *capacity) updateHighWaterLocked() {
	pending := c.pendingLocked()
	admitted := countAdmitted(c.clients)
	if pending > c.metrics.PendingHighWater {
		c.metrics.PendingHighWater = pending
	}
	if admitted > c.metrics.AdmittedHighWater {
		c.metrics.AdmittedHighWater = admitted
	}
	if len(c.clients) > c.metrics.PhysicalHighWater {
		c.metrics.PhysicalHighWater = len(c.clients)
	}
}

func (c *capacity) countStateLocked(want capacityState) int {
	n := 0
	for _, state := range c.clients {
		if state.state == want {
			n++
		}
	}
	return n
}

func (c *capacity) usageLocked() int {
	used := 0
	for _, entry := range c.channels {
		cost := entitlementCost(entry)
		if cost > c.config.AdmittedLimit-used {
			return c.config.AdmittedLimit + 1
		}
		used += cost
	}
	return used
}

func entitlementCost(entry *channelEntitlement) int {
	if entry.configured {
		extra := maxInt(entry.masters-1, 0) + maxInt(entry.slaves-1, 0) + entry.unknown
		if entry.overlap != nil && extra > 0 {
			extra--
		}
		return 3 + extra
	}
	return 2 + maxInt(entry.masters-1, 0) + maxInt(entry.slaves-1, 0) + entry.unknown
}

func countRole(entry *channelEntitlement, role string, delta int) {
	switch role {
	case "master":
		entry.masters += delta
	case "slave":
		entry.slaves += delta
	default:
		entry.unknown += delta
	}
}

func duplicateRole(entry *channelEntitlement, role string) bool {
	if role == "master" {
		return entry.masters > 1
	}
	if role == "slave" {
		return entry.slaves > 1
	}
	return false
}

func duplicateStillPresent(entry *channelEntitlement, role string) bool {
	return duplicateRole(entry, role)
}

func maxInt(value, floor int) int {
	if value > floor {
		return value
	}
	return floor
}

func (c *capacity) signalLocked() {
	select {
	case c.changed <- struct{}{}:
	default:
	}
}

func (c *capacity) signalReadyLocked() {
	if !c.readyClosed {
		close(c.ready)
		c.readyClosed = true
	}
	if !c.closed && !c.stopping {
		c.ready = make(chan struct{})
		c.readyClosed = false
	}
}
