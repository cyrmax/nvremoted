package perf

import (
	"runtime"
	"runtime/metrics"
	"time"
)

type Resources struct {
	Wall             time.Time   `json:"-"`
	CPUUserSeconds   float64     `json:"cpu_user_seconds"`
	CPUSystemSeconds float64     `json:"cpu_system_seconds"`
	CPUAvailable     bool        `json:"cpu_available"`
	RSSBytes         uint64      `json:"rss_bytes"`
	RSSKind          string      `json:"rss_kind"`
	HeapAlloc        uint64      `json:"heap_alloc_bytes"`
	HeapSys          uint64      `json:"heap_reserved_bytes"`
	HeapObjects      uint64      `json:"heap_objects"`
	TotalAlloc       uint64      `json:"total_alloc_bytes"`
	Mallocs          uint64      `json:"total_mallocs"`
	Goroutines       int         `json:"goroutines"`
	NumGC            uint32      `json:"gc_cycles"`
	PauseTotalNS     uint64      `json:"gc_pause_total_ns"`
	GCCPUSeconds     float64     `json:"gc_cpu_seconds"`
	PauseNS          [256]uint64 `json:"-"`
}

type ResourceDelta struct {
	WallSeconds      float64 `json:"wall_seconds"`
	CPUSeconds       float64 `json:"cpu_seconds"`
	CPUUserSeconds   float64 `json:"cpu_user_seconds"`
	CPUSystemSeconds float64 `json:"cpu_system_seconds"`
	CPUAvailable     bool    `json:"cpu_available"`
	// 100% means one fully occupied logical CPU; this may exceed 100%.
	CPUPercent              float64      `json:"cpu_percent_one_core"`
	AllocBytes              uint64       `json:"allocated_bytes"`
	Allocations             uint64       `json:"allocations"`
	AllocBytesPerSecond     float64      `json:"allocated_bytes_per_second"`
	GCCycles                uint32       `json:"gc_cycles"`
	GCCPUSeconds            float64      `json:"gc_cpu_seconds"`
	GCPauseTotalNS          uint64       `json:"gc_pause_total_ns"`
	GCPauses                Distribution `json:"gc_pauses_retained"`
	GCPauseSamplesTruncated bool         `json:"gc_pause_samples_truncated"`
}

func Capture() Resources {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	samples := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}}
	metrics.Read(samples)
	gcCPU := float64(0)
	if samples[0].Value.Kind() == metrics.KindFloat64 {
		gcCPU = samples[0].Value.Float64()
	}
	user, system, rss, kind, available := processResources()
	return Resources{Wall: Now(), CPUUserSeconds: user, CPUSystemSeconds: system,
		CPUAvailable: available, RSSBytes: rss, RSSKind: kind, HeapAlloc: m.HeapAlloc,
		HeapSys: m.HeapSys, HeapObjects: m.HeapObjects, TotalAlloc: m.TotalAlloc,
		Mallocs: m.Mallocs, Goroutines: runtime.NumGoroutine(), NumGC: m.NumGC,
		PauseTotalNS: m.PauseTotalNs, GCCPUSeconds: gcCPU, PauseNS: m.PauseNs}
}

func Delta(before, after Resources) ResourceDelta {
	d := ResourceDelta{WallSeconds: after.Wall.Sub(before.Wall).Seconds(),
		CPUUserSeconds:   after.CPUUserSeconds - before.CPUUserSeconds,
		CPUSystemSeconds: after.CPUSystemSeconds - before.CPUSystemSeconds,
		CPUAvailable:     before.CPUAvailable && after.CPUAvailable,
		AllocBytes:       after.TotalAlloc - before.TotalAlloc, Allocations: after.Mallocs - before.Mallocs,
		GCCycles: after.NumGC - before.NumGC, GCCPUSeconds: after.GCCPUSeconds - before.GCCPUSeconds,
		GCPauseTotalNS: after.PauseTotalNS - before.PauseTotalNS}
	d.CPUSeconds = d.CPUUserSeconds + d.CPUSystemSeconds
	if d.WallSeconds > 0 {
		d.CPUPercent = 100 * d.CPUSeconds / d.WallSeconds
		d.AllocBytesPerSecond = float64(d.AllocBytes) / d.WallSeconds
	}
	var pauses Histogram
	start := before.NumGC
	if d.GCCycles > 256 {
		start = after.NumGC - 256
		d.GCPauseSamplesTruncated = true
	}
	for n := start; n != after.NumGC; n++ {
		pauses.Add(time.Duration(after.PauseNS[n%256]))
	}
	d.GCPauses = pauses.Distribution()
	return d
}
