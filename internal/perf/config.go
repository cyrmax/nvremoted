package perf

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func ConfigFromEnv(preset string) (Config, error) { return ParseConfig(preset, os.Getenv) }

func ParseConfig(preset string, get func(string) string) (Config, error) {
	c := Config{Preset: preset, Duration: time.Second, Warmup: 250 * time.Millisecond,
		Drain: 2 * time.Second, Transport: "both", Repetitions: 1, MaxInFlight: 4096}
	switch preset {
	case "quick":
	case "long":
		c.Duration = 10 * time.Second
		c.Warmup = time.Second
	case "profile":
		c.Duration = 30 * time.Second
		c.Warmup = time.Second
		c.Profile = "cpu"
	case "smoke":
		c.Duration = 30 * time.Millisecond
		c.Warmup = 10 * time.Millisecond
	default:
		return c, fmt.Errorf("unknown performance preset %q", preset)
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
		zero   bool
	}{
		{"DURATION", &c.Duration, false}, {"WARMUP", &c.Warmup, true}, {"DRAIN", &c.Drain, false},
	} {
		if value := get("NVREMOTED_BENCH_" + item.name); value != "" {
			d, err := time.ParseDuration(value)
			if err != nil || d < 0 || (!item.zero && d == 0) {
				return c, fmt.Errorf("NVREMOTED_BENCH_%s must be a %s duration", item.name, map[bool]string{true: "nonnegative", false: "positive"}[item.zero])
			}
			*item.target = d
		}
	}
	for _, item := range []struct {
		name    string
		target  *int
		maximum int
	}{
		{"RATE", &c.Rate, 10000000}, {"CHANNELS", &c.Channels, 500}, {"FANOUT", &c.Fanout, 1000},
		{"CLIENTS", &c.Clients, 1000}, {"REPETITIONS", &c.Repetitions, 100}, {"MAX_IN_FLIGHT", &c.MaxInFlight, 65536},
	} {
		if value := get("NVREMOTED_BENCH_" + item.name); value != "" {
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 || n > item.maximum {
				return c, fmt.Errorf("NVREMOTED_BENCH_%s must be between 1 and %d", item.name, item.maximum)
			}
			*item.target = n
		}
	}
	if value := get("NVREMOTED_BENCH_TRANSPORT"); value != "" {
		c.Transport = value
	}
	if c.Transport != "tcp" && c.Transport != "tls" && c.Transport != "both" {
		return c, fmt.Errorf("transport must be tcp, tls or both")
	}
	c.Payload = get("NVREMOTED_BENCH_PAYLOAD")
	if c.Payload != "" {
		if _, err := Payload(c.Payload); err != nil {
			return c, err
		}
	}
	c.Filter = get("NVREMOTED_BENCH_FILTER")
	c.Output = get("NVREMOTED_BENCH_OUTPUT")
	if value := get("NVREMOTED_BENCH_PROFILE"); value != "" {
		c.Profile = value
	}
	if c.Profile != "" {
		switch c.Profile {
		case "cpu", "heap", "allocs", "goroutine", "block", "mutex", "trace", "all":
		default:
			return c, fmt.Errorf("unknown profile mode %q", c.Profile)
		}
		if preset != "profile" {
			return c, fmt.Errorf("profiling is isolated: use mage benchProfile")
		}
	}
	return c, nil
}

// Scenarios deliberately vary one dimension at a time instead of creating a
// huge Cartesian product. Overrides form a custom relay scenario when no
// named filter is supplied, and otherwise override matching relay cases.
func Scenarios(c Config) []Scenario {
	var list []Scenario
	add := func(name, kind, payload string, channels, fanout, clients, rate int) {
		list = append(list, Scenario{Name: name, Kind: kind, Payload: payload, Channels: channels, Fanout: fanout, Clients: clients, Rate: rate})
	}
	if c.Preset == "profile" {
		add("representative-pgo", "open", "mixed", 10, 4, 0, 5000)
	} else if c.Preset == "smoke" {
		add("smoke-closed", "closed", "key", 1, 1, 0, 0)
		add("smoke-open", "open", "arbitrary-nested", 2, 2, 0, 100)
	} else if c.Filter == "" && (c.Rate > 0 || c.Channels > 0 || c.Fanout > 0 || c.Payload != "" || c.Clients > 0) {
		if c.Clients > 0 {
			add("custom-idle", "idle", "", 0, 0, c.Clients, 0)
			add("custom-joined", "idle", "", 0, 0, c.Clients, 0)
			list[len(list)-1].Joined = true
		} else {
			kind := "closed"
			if c.Rate > 0 {
				kind = "open"
			}
			add("custom-relay", kind, "key", 1, 1, 0, 0)
		}
	} else {
		payloads := []string{"key", "control", "gesture", "speak-short", "speak", "speak-large", "braille-20", "braille-80", "braille-160", "clipboard-1KiB", "clipboard-16KiB", "clipboard-64KiB", "clipboard-256KiB", "clipboard-1MiB", "arbitrary-flat", "arbitrary-nested", "arbitrary-strings", "arbitrary-arrays"}
		if c.Preset == "long" {
			payloads = PayloadNames()
		}
		for _, p := range payloads {
			add("payload/"+p, "closed", p, 1, 1, 0, 0)
		}
		fanouts := []int{1, 2, 4, 10, 32}
		if c.Preset == "long" {
			fanouts = append(fanouts, 100)
		}
		for _, n := range fanouts {
			add(fmt.Sprintf("fanout/%d", n), "closed", "key", 1, n, 0, 0)
			if c.Preset == "long" {
				add(fmt.Sprintf("fanout-load/%d", n), "open", "key", 1, n, 0, 1000)
			}
		}
		rates := []int{100, 1000, 5000, 10000, 25000}
		if c.Preset == "long" {
			rates = append(rates, 50000, 100000)
		}
		for _, rate := range rates {
			add(fmt.Sprintf("saturation/%d", rate), "open", "key", 1, 1, 0, rate)
		}
		for _, n := range []int{1, 10, 100} {
			add(fmt.Sprintf("channels/%d", n), "open", "key", n, 1, 0, 1000)
		}
		if c.Preset == "long" {
			add("clients/active/500", "open", "key", 250, 1, 0, 5000)
			add("clients/active/1000", "open", "key", 500, 1, 0, 5000)
		}
		add("channels/hot", "open", "mixed", 10, 2, 0, 5000)
		add("channels/hot-cold", "open", "mixed", 100, 1, 0, 1000)
		list[len(list)-1].Mixed = true
		counts := []int{100}
		if c.Preset == "long" {
			counts = []int{100, 500, 1000}
		}
		for _, n := range counts {
			add(fmt.Sprintf("clients/idle/%d", n), "idle", "", 0, 0, n, 0)
			add(fmt.Sprintf("clients/joined/%d", n), "idle", "", 0, 0, n, 0)
			list[len(list)-1].Joined = true
		}
		ids := []string{"ordinary", "protected-complementary", "recovery", "protected-reconnect", "capacity-reject", "reject-burst", "pending-tls", "pending-startup", "tcp-connect", "tls-handshake", "startup-join", "oversized-offender"}
		for _, id := range ids {
			batch := 16
			if c.Preset == "long" {
				batch = 64
			}
			add("lifecycle/"+id, "lifecycle", id, 0, 0, batch, 0)
		}
		for _, id := range []string{"small", "medium", "1MiB", "near-limit", "exact-limit", "oversized"} {
			add("boundary/"+id, "boundary", id, 0, 0, 4, 0)
		}
	}
	var out []Scenario
	transports := []string{"tcp", "tls"}
	if c.Transport != "both" {
		transports = []string{c.Transport}
	}
	for _, s := range list {
		if c.Filter != "" && !strings.Contains(s.Name, c.Filter) {
			continue
		}
		if s.Kind == "open" || s.Kind == "closed" {
			if c.Rate > 0 {
				s.Rate = c.Rate
				s.Kind = "open"
			}
			if c.Channels > 0 {
				s.Channels = c.Channels
			}
			if c.Fanout > 0 {
				s.Fanout = c.Fanout
			}
			if c.Payload != "" {
				s.Payload = c.Payload
			}
		}
		if c.Clients > 0 && (s.Kind == "idle" || s.Kind == "lifecycle" || s.Kind == "boundary") {
			s.Clients = c.Clients
		}
		for _, transport := range transports {
			if (s.Payload == "pending-tls" || s.Payload == "tls-handshake") && transport != "tls" {
				continue
			}
			if s.Payload == "tcp-connect" && transport != "tcp" {
				continue
			}
			for rep := 1; rep <= c.Repetitions; rep++ {
				copy := s
				copy.Transport = transport
				copy.Repetition = rep
				out = append(out, copy)
			}
		}
	}
	return out
}
