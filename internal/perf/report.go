package perf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

func EnvironmentMetadata(instrumented bool) Metadata {
	m := Metadata{StartedUTC: time.Now().UTC(), OS: runtime.GOOS, Architecture: runtime.GOARCH,
		CPUModel: strings.TrimSpace(cpuModel()), LogicalCPUs: runtime.NumCPU(), GoVersion: runtime.Version(),
		GOMAXPROCS: runtime.GOMAXPROCS(0), CGO: os.Getenv("CGO_ENABLED"), Commit: "unknown", Instrumented: instrumented}
	m.Clock = InspectClock()
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "CGO_ENABLED":
				m.CGO = setting.Value
			case "GOAMD64":
				m.GOAMD64 = setting.Value
			case "GOARM64":
				m.GOARM64 = setting.Value
			case "GOEXPERIMENT":
				m.GOEXPERIMENT = setting.Value
			case "vcs.revision":
				if len(setting.Value) >= 8 {
					m.Commit = setting.Value[:8]
				}
			case "vcs.modified":
				m.Dirty = setting.Value == "true"
			}
		}
	}
	// Test executables normally lack VCS build info. This is a read-only local
	// fallback; no dependency on Git/network during measurement.
	if data, err := exec.Command("git", "rev-parse", "--short=8", "HEAD").Output(); err == nil {
		m.Commit = strings.TrimSpace(string(data))
	}
	if data, err := exec.Command("git", "status", "--porcelain").Output(); err == nil {
		m.Dirty = len(data) > 0
	}
	return m
}

func WriteReport(directory string, report Report) error {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	// Write each completed scenario atomically so an interrupted long run keeps
	// the last complete report rather than a partially written JSON document.
	tmp, err := os.CreateTemp(directory, ".results-*.json")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err = tmp.Write(append(data, '\n')); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(directory, "results.json")); err != nil {
		return err
	}
	summary, err := os.Create(filepath.Join(directory, "summary.txt"))
	if err != nil {
		return err
	}
	err = PrintSummary(summary, report)
	closeErr := summary.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func seconds(r Result) float64 { return float64(r.DurationNS) / 1e9 }
func rate(count uint64, duration float64) float64 {
	if duration <= 0 {
		return 0
	}
	return float64(count) / duration
}

func PrintSummary(w io.Writer, report Report) error {
	var rendered bytes.Buffer
	destination := w
	w = &rendered
	if _, err := fmt.Fprintf(w, "NVRemoted local performance schema=%d preset=%s profile=%s\n%s/%s | %s | %s | logical CPUs=%d GOMAXPROCS=%d CGO=%s | commit=%s dirty=%t\n",
		report.SchemaVersion, report.Config.Preset, report.Config.Profile, report.Metadata.OS, report.Metadata.Architecture,
		report.Metadata.CPUModel, report.Metadata.GoVersion, report.Metadata.LogicalCPUs, report.Metadata.GOMAXPROCS, report.Metadata.CGO, report.Metadata.Commit, report.Metadata.Dirty); err != nil {
		return err
	}
	if report.Metadata.Instrumented {
		fmt.Fprintln(w, "PROFILE RUN: latency/throughput are diagnostic, not a baseline comparison.")
	}
	fmt.Fprintf(w, "Clock: %s frequency=%dHz min observed step=%dns mean probe cost=%.1fns\n", report.Metadata.Clock.Source, report.Metadata.Clock.FrequencyHz, report.Metadata.Clock.ObservedMinStepNS, report.Metadata.Clock.MeanReadCostNS)
	fmt.Fprintln(w, "Latency in microseconds; recv/s counts deliveries inside measurement, NOT delayed drain deliveries.")
	fmt.Fprintln(w, "scenario | transport | sent/s | recv/s | p50 | p95 | p99 | p99.9 | missing | disc | generator missed | CPU cores | validity")
	for _, r := range report.Results {
		p999 := "insufficient"
		if r.Latency.P999 != nil {
			p999 = fmt.Sprintf("%.1f", *r.Latency.P999/1e3)
		}
		fmt.Fprintf(w, "%s [%d] | %s | %.0f | %.0f | %.1f | %.1f | %.1f | %s | %d | %d | %d | %.2f | %t\n", r.Scenario.Name, r.Scenario.Repetition, r.Scenario.Transport,
			rate(r.SentInWindow, seconds(r)), rate(r.ReceivedInWindow, seconds(r)), r.Latency.P50/1e3, r.Latency.P95/1e3, r.Latency.P99/1e3, p999, r.Missing, r.Disconnects, r.GeneratorMissed, r.Resources.CPUPercent/100, r.Valid)
		if r.Scenario.Kind == "open" {
			fmt.Fprintf(w, "  target=%d/s offered=%d actual-send p99=%.1fus schedule p99=%.1fus lag p99=%.1fus queue sampled=%d/%d overflows=%d\n", r.Scenario.Rate, r.Offered, r.Latency.P99/1e3, r.ScheduledLatency.P99/1e3, r.GeneratorLag.P99/1e3, r.QueueMax, r.QueueCapacity, r.OverflowDisconnects)
		}
		fmt.Fprintf(w, "  count=%d min=%.1fus mean=%.1fus p90=%.1fus max=%.1fus stddev=%.1fus alloc=%.2fMiB CPU=%.3fs heap=%.2fMiB RSS=%.2fMiB(%s) GC=%d GC-CPU=%.3fs\n",
			r.Latency.Count, r.Latency.Min/1e3, r.Latency.Mean/1e3, r.Latency.P90/1e3, r.Latency.Max/1e3, r.Latency.StdDev/1e3, float64(r.Resources.AllocBytes)/(1<<20), r.Resources.CPUSeconds,
			float64(r.After.HeapAlloc)/(1<<20), float64(r.After.RSSBytes)/(1<<20), r.After.RSSKind, r.Resources.GCCycles, r.Resources.GCCPUSeconds)
		if r.Sent > 0 {
			fmt.Fprintf(w, "  whole-harness alloc/message=%.1fB allocations/message=%.2f send=%.2fMiB/s receive=%.2fMiB/s messages/CPU-s=%.0f last-recipient p99=%.1fus\n",
				float64(r.Resources.AllocBytes)/float64(r.Sent), float64(r.Resources.Allocations)/float64(r.Sent), float64(r.SendBytesInWindow)/(1<<20)/positiveSeconds(r), float64(r.ReceiveBytesInWindow)/(1<<20)/positiveSeconds(r), rate(r.Sent, r.Resources.CPUSeconds), r.Completion.P99/1e3)
		}
		for _, note := range r.Notes {
			fmt.Fprintln(w, "  note:", note)
		}
		for _, err := range r.Errors {
			fmt.Fprintln(w, "  error:", err)
		}
	}
	printSaturation(w, report.Results)
	for _, note := range report.Notes {
		fmt.Fprintln(w, "Note:", note)
	}
	_, err := io.Copy(destination, &rendered)
	return err
}

func positiveSeconds(r Result) float64 {
	s := seconds(r)
	if s <= 0 {
		return 1
	}
	return s
}

func sustainable(r Result) bool {
	return r.Valid && r.Scenario.Kind == "open" && r.Sent > 0 && r.GeneratorMissed == 0 &&
		rate(r.SentInWindow, seconds(r)) >= float64(r.Scenario.Rate)*.95 && r.ReceivedInWindow >= uint64(float64(r.Expected)*.95)
}

func printSaturation(w io.Writer, results []Result) {
	for _, transport := range []string{"tcp", "tls"} {
		var good, firstInvalid, firstTail *Result
		var floor float64
		for i := range results {
			r := &results[i]
			if r.Scenario.Transport != transport || !strings.HasPrefix(r.Scenario.Name, "saturation/") {
				continue
			}
			if floor == 0 && r.Latency.Count > 0 {
				floor = r.Latency.P99
			}
			if sustainable(*r) && (good == nil || r.Scenario.Rate > good.Scenario.Rate) {
				good = r
			}
			if !sustainable(*r) && firstInvalid == nil {
				firstInvalid = r
			}
			if floor > 0 && r.Latency.P99 > floor*3 && firstTail == nil {
				firstTail = r
			}
		}
		if good == nil && firstInvalid == nil {
			continue
		}
		fmt.Fprintf(w, "Saturation %s (finite observation, not a guarantee): ", transport)
		if good != nil {
			fmt.Fprintf(w, "highest fully delivered tested rate with >=95%% pacing/window delivery: %d/s; ", good.Scenario.Rate)
		} else {
			fmt.Fprint(w, "no sustainable rate established; ")
		}
		if firstInvalid != nil {
			fmt.Fprintf(w, "first delivery/pacing failure at tested %d/s; ", firstInvalid.Scenario.Rate)
		}
		if firstTail != nil {
			fmt.Fprintf(w, "first >3x low-rate p99 candidate knee at %d/s", firstTail.Scenario.Rate)
		} else {
			fmt.Fprint(w, "no >3x p99 knee observed")
		}
		fmt.Fprintln(w)
	}
}

func ReadReport(path string) (Report, error) {
	var r Report
	f, err := os.Open(path)
	if err != nil {
		return r, err
	}
	defer f.Close()
	// Bounded input even for an accidentally selected huge/malformed file.
	d := json.NewDecoder(io.LimitReader(f, 64<<20))
	if err = d.Decode(&r); err != nil {
		return r, err
	}
	if r.SchemaVersion != SchemaVersion {
		return r, fmt.Errorf("unsupported result schema %d", r.SchemaVersion)
	}
	return r, nil
}
