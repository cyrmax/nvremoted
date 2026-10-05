//go:build performance

package server

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/n0ot/nvremoted/internal/perf"
)

// Explicit opt-in keeps an accidental `go test -tags=performance ./...` from
// launching an unattended suite. Ordinary CI does not compile this file.
func TestLocalPerformance(t *testing.T) {
	if os.Getenv("NVREMOTED_BENCH_RUN") != "1" {
		t.Skip("local performance suite is launched by Mage")
	}
	preset := os.Getenv("NVREMOTED_BENCH_PRESET")
	if preset == "" {
		preset = "quick"
	}
	cfg, err := perf.ConfigFromEnv(preset)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Output == "" {
		t.Fatal("Mage must provide a fresh result directory")
	}
	scenarios := perf.Scenarios(cfg)
	if len(scenarios) == 0 {
		t.Fatal("no scenarios selected")
	}
	report := perf.Report{SchemaVersion: perf.SchemaVersion, Metadata: perf.EnvironmentMetadata(cfg.Profile != ""), Config: cfg,
		Notes: []string{
			"Localhost latency includes the real relay plus client framing, TCP/TLS and OS scheduling. It is not isolated relay CPU time and not WAN/NVDA UI latency.",
			"CPU, allocations, heap and RSS are for the entire in-process harness: server, senders, receivers, validation and instrumentation. Component benchmarks localize relay costs.",
			"No GC is forced. Heap/RSS snapshots may contain retained or not-yet-collected objects from earlier scenarios; use isolated filtered runs for memory comparisons.",
			"Histogram quantiles have <=1% bin width; exact count/min/max/mean/stddev. p99.9 omitted below 10000 samples.",
			"Queue samples are periodic observations, not an exact time series. Overflow disconnects are counted separately from logs.",
			"Open-loop actual-send and scheduled latency differ. Generator lag/missed attempts invalidate claims that the target load was sustained.",
		}}
	for i, spec := range scenarios {
		fmt.Printf("[%d/%d] %s %s repetition=%d\n", i+1, len(scenarios), spec.Name, spec.Transport, spec.Repetition)
		runCfg := cfg
		if cfg.Profile != "" {
			runCfg.Output = filepath.Join(cfg.Output, "profiles", spec.Transport, fmt.Sprint(spec.Repetition))
		}
		var result perf.Result
		switch spec.Kind {
		case "open", "closed", "idle":
			result, err = runRelayScenario(runCfg, spec)
		case "lifecycle", "boundary":
			result, err = runLifecycleScenario(runCfg, spec)
		default:
			err = fmt.Errorf("unknown scenario kind %s", spec.Kind)
		}
		result.Scenario = spec
		if err != nil {
			result.Valid = false
			result.Errors = append(result.Errors, err.Error())
			t.Errorf("%s %s: %v", spec.Name, spec.Transport, err)
		}
		if result.Corrupted > 0 || result.Duplicates > 0 || result.OutOfOrder > 0 || result.Unexpected > 0 || result.TimestampLost > 0 {
			result.Valid = false
			t.Errorf("%s %s: delivery/harness integrity failure", spec.Name, spec.Transport)
		}
		if spec.Kind == "closed" && (result.Missing > 0 || result.Disconnects > 0) {
			result.Valid = false
			t.Errorf("%s %s: sequential delivery failed", spec.Name, spec.Transport)
		}
		report.Results = append(report.Results, result)
		if err := perf.WriteReport(cfg.Output, report); err != nil {
			t.Fatal(err)
		}
	}
	if err := perf.PrintSummary(os.Stdout, report); err != nil {
		t.Fatal(err)
	}
}
