package perf

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
)

func comparisonKey(s Scenario) string { data, _ := json.Marshal(s); return string(data) }

// Compare prints descriptive deltas, never hardware-dependent pass/fail tests
// or claims of statistical significance from a single local run.
func Compare(w io.Writer, before, after Report) error {
	if before.SchemaVersion != SchemaVersion || after.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported comparison schema")
	}
	if before.Metadata.Instrumented || after.Metadata.Instrumented {
		return fmt.Errorf("profile runs are not baseline comparisons")
	}
	if before.Metadata.OS != after.Metadata.OS || before.Metadata.Architecture != after.Metadata.Architecture || before.Metadata.CPUModel != after.Metadata.CPUModel || before.Metadata.GOMAXPROCS != after.Metadata.GOMAXPROCS || before.Metadata.GoVersion != after.Metadata.GoVersion || before.Metadata.CGO != after.Metadata.CGO || before.Metadata.GOAMD64 != after.Metadata.GOAMD64 || before.Metadata.GOARM64 != after.Metadata.GOARM64 || before.Metadata.GOEXPERIMENT != after.Metadata.GOEXPERIMENT {
		fmt.Fprintln(w, "WARNING: environment/toolchain differs; deltas are descriptive and may not reflect code changes.")
	}
	if before.Config.Duration != after.Config.Duration || before.Config.Warmup != after.Config.Warmup || before.Config.Drain != after.Config.Drain || before.Config.MaxInFlight != after.Config.MaxInFlight {
		fmt.Fprintln(w, "WARNING: measurement/warmup/drain/window differs; workload conditions are not identical.")
	}
	if before.Metadata.Clock.Source != after.Metadata.Clock.Source || before.Metadata.Clock.FrequencyHz != after.Metadata.Clock.FrequencyHz {
		fmt.Fprintln(w, "WARNING: measurement clocks differ; sub-ms latency deltas may not be comparable.")
	}
	left := map[string]Result{}
	for _, r := range before.Results {
		left[comparisonKey(r.Scenario)] = r
	}
	ordered := append([]Result(nil), after.Results...)
	sort.Slice(ordered, func(i, j int) bool { return comparisonKey(ordered[i].Scenario) < comparisonKey(ordered[j].Scenario) })
	fmt.Fprintln(w, "Descriptive A/B deltas (candidate - baseline). Repeat runs before drawing conclusions.")
	matched := 0
	for _, candidate := range ordered {
		baseline, ok := left[comparisonKey(candidate.Scenario)]
		if !ok {
			fmt.Fprintf(w, "unmatched candidate: %s %s [%d]\n", candidate.Scenario.Name, candidate.Scenario.Transport, candidate.Scenario.Repetition)
			continue
		}
		matched++
		delete(left, comparisonKey(candidate.Scenario))
		oldServer, _ := json.Marshal(baseline.ServerConfig)
		newServer, _ := json.Marshal(candidate.ServerConfig)
		if string(oldServer) != string(newServer) {
			fmt.Fprintln(w, "WARNING: server configuration differs for", candidate.Scenario.Name)
		}
		fmt.Fprintf(w, "%s %s [%d] valid=%t/%t missing=%d/%d disconnects=%d/%d\n", candidate.Scenario.Name, candidate.Scenario.Transport, candidate.Scenario.Repetition, baseline.Valid, candidate.Valid, baseline.Missing, candidate.Missing, baseline.Disconnects, candidate.Disconnects)
		if !baseline.Valid || !candidate.Valid {
			fmt.Fprintln(w, "  INVALID delivery/pacing/resource result: no performance delta reported.")
			continue
		}
		for _, m := range []struct {
			name     string
			old, new float64
		}{
			{"p50_us", baseline.Latency.P50 / 1e3, candidate.Latency.P50 / 1e3},
			{"p95_us", baseline.Latency.P95 / 1e3, candidate.Latency.P95 / 1e3},
			{"p99_us", baseline.Latency.P99 / 1e3, candidate.Latency.P99 / 1e3},
			{"delivered_per_second", rate(baseline.ReceivedInWindow, seconds(baseline)), rate(candidate.ReceivedInWindow, seconds(candidate))},
			{"harness_alloc_bytes_per_sent", rate(baseline.Resources.AllocBytes, float64(baseline.Sent)), rate(candidate.Resources.AllocBytes, float64(candidate.Sent))},
			{"harness_allocs_per_sent", rate(baseline.Resources.Allocations, float64(baseline.Sent)), rate(candidate.Resources.Allocations, float64(candidate.Sent))},
			{"live_heap_bytes", float64(baseline.After.HeapAlloc), float64(candidate.After.HeapAlloc)},
			{"rss_bytes", float64(baseline.After.RSSBytes), float64(candidate.After.RSSBytes)},
			{"cpu_seconds", baseline.Resources.CPUSeconds, candidate.Resources.CPUSeconds},
			{"messages_per_cpu_second", rate(baseline.Sent, baseline.Resources.CPUSeconds), rate(candidate.Sent, candidate.Resources.CPUSeconds)},
		} {
			if m.old == 0 {
				fmt.Fprintf(w, "  %-28s %12.3f -> %12.3f delta=%+.3f (no relative baseline)\n", m.name, m.old, m.new, m.new-m.old)
			} else {
				fmt.Fprintf(w, "  %-28s %12.3f -> %12.3f delta=%+.3f (%+.2f%%)\n", m.name, m.old, m.new, m.new-m.old, 100*(m.new/m.old-1))
			}
		}
	}
	fmt.Fprintf(w, "Matched scenarios: %d; unmatched baseline: %d\n", matched, len(left))
	return nil
}
