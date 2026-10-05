package perf

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHistogramDistributionAndMerge(t *testing.T) {
	var all, left, right Histogram
	for i := 1; i <= 10000; i++ {
		d := time.Duration(i)
		all.Add(d)
		if i <= 5000 {
			left.Add(d)
		} else {
			right.Add(d)
		}
	}
	left.Merge(&right)
	a, b := all.Distribution(), left.Distribution()
	if a.Count != 10000 || a.Min != 1 || a.Max != 10000 || a.Mean != 5000.5 || a.P999 == nil {
		t.Fatalf("invalid moments: %+v", a)
	}
	for _, value := range []struct{ got, want float64 }{{a.P50, 5000}, {a.P90, 9000}, {a.P95, 9500}, {a.P99, 9900}, {*a.P999, 9990}} {
		if value.got < value.want || value.got > value.want*1.01 {
			t.Fatalf("quantile %v outside 1%% of %v", value.got, value.want)
		}
	}
	if a.P50 != b.P50 || a.P99 != b.P99 || math.Abs(a.StdDev-b.StdDev) > 1e-6 {
		t.Fatalf("merge changed distribution: %+v %+v", a, b)
	}
	var empty Histogram
	if empty.Distribution().P999 != nil || empty.Distribution().Count != 0 {
		t.Fatal("empty histogram populated")
	}
	empty.Merge(&all)
	if empty.Distribution().Mean != a.Mean || empty.Distribution().Count != a.Count {
		t.Fatal("empty merge failed")
	}
}

func TestConfigAndMatrix(t *testing.T) {
	get := func(string) string { return "" }
	c, err := ParseConfig("long", get)
	if err != nil {
		t.Fatal(err)
	}
	matrix := Scenarios(c)
	have := map[string]bool{}
	for _, s := range matrix {
		have[s.Name+"/"+s.Transport] = true
	}
	for _, name := range []string{"fanout/100/tcp", "clients/joined/1000/tls", "payload/clipboard-1MiB/tcp", "lifecycle/pending-tls/tls", "boundary/exact-limit/tcp"} {
		if !have[name] {
			t.Errorf("matrix missing %s", name)
		}
	}
	for _, env := range []map[string]string{{"NVREMOTED_BENCH_DURATION": "0s"}, {"NVREMOTED_BENCH_RATE": "0"}, {"NVREMOTED_BENCH_TRANSPORT": "udp"}, {"NVREMOTED_BENCH_PROFILE": "cpu"}, {"NVREMOTED_BENCH_MAX_IN_FLIGHT": "999999"}} {
		if _, err := ParseConfig("quick", func(k string) string { return env[k] }); err == nil {
			t.Fatalf("accepted invalid config %v", env)
		}
	}
	custom, err := ParseConfig("quick", func(k string) string {
		return map[string]string{"NVREMOTED_BENCH_RATE": "5000", "NVREMOTED_BENCH_PAYLOAD": "speak", "NVREMOTED_BENCH_TRANSPORT": "tls"}[k]
	})
	if err != nil {
		t.Fatal(err)
	}
	s := Scenarios(custom)
	if len(s) != 1 || s[0].Rate != 5000 || s[0].Payload != "speak" || s[0].Transport != "tls" {
		t.Fatalf("bad override: %+v", s)
	}
}

func TestPayloadTemplates(t *testing.T) {
	for _, name := range PayloadNames() {
		t.Run(name, func(t *testing.T) {
			template, err := NewTemplate(name)
			if err != nil {
				t.Fatal(err)
			}
			var m map[string]any
			wire := template.Wire(987654321)
			if err = json.Unmarshal(wire, &m); err != nil {
				t.Fatal(err)
			}
			if m["bench_seq"] != float64(987654321) || m["type"] == "" {
				t.Fatalf("invalid template %s", name)
			}
			if !bytes.Equal(wire, template.AppendWire(make([]byte, 0, len(wire)), 987654321)) {
				t.Fatal("reuse changed JSON")
			}
		})
	}
	if _, err := TemplateFromJSON([]byte(`{"bench_seq":0,"bench_seq":0}`)); err == nil {
		t.Fatal("duplicate placeholder accepted")
	}
}

func TestReportRoundTripAndComparison(t *testing.T) {
	directory := t.TempDir()
	r := Report{SchemaVersion: SchemaVersion, Config: Config{Preset: "quick"}, Results: []Result{{Scenario: Scenario{Name: "sample", Kind: "closed", Transport: "tcp"}, Valid: true, Sent: 10, ReceivedInWindow: 10, DurationNS: int64(time.Second), Latency: Distribution{P50: 1000, P95: 2000, P99: 3000}}}}
	if err := WriteReport(directory, r); err != nil {
		t.Fatal(err)
	}
	r.Results[0].Latency.P50 = 1200
	if err := WriteReport(directory, r); err != nil {
		t.Fatal(err)
	}
	loaded, err := ReadReport(filepath.Join(directory, "results.json"))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Results[0].Latency.P50 != 1200 {
		t.Fatal("report update lost")
	}
	var out bytes.Buffer
	if err := Compare(&out, r, loaded); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Matched scenarios: 1") {
		t.Fatal("not matched")
	}
	loaded.Results[0].Valid = false
	out.Reset()
	if err := Compare(&out, r, loaded); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "INVALID") || strings.Contains(out.String(), "p50_us") {
		t.Fatal("compared invalid result")
	}
	if err := os.WriteFile(filepath.Join(directory, "wrong.json"), []byte(`{"schema_version":999}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadReport(filepath.Join(directory, "wrong.json")); err == nil {
		t.Fatal("unknown schema accepted")
	}
}
