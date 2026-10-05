package perf

import "time"

// Since and Until use the same process clock as local message timestamps.
func Since(t time.Time) time.Duration { return Now().Sub(t) }
func Until(t time.Time) time.Duration { return t.Sub(Now()) }

type ClockMetadata struct {
	Source            string  `json:"source"`
	FrequencyHz       int64   `json:"frequency_hz,omitempty"`
	ObservedMinStepNS int64   `json:"observed_min_positive_step_ns"`
	MeanReadCostNS    float64 `json:"mean_read_cost_ns"`
}

// InspectClock runs before scenarios, never inside a latency measurement.
func InspectClock() ClockMetadata {
	source, frequency := clockDetails()
	info := ClockMetadata{Source: source, FrequencyHz: frequency}
	start := Now()
	previous := start
	const probes = 10000
	for i := 0; i < probes; i++ {
		now := Now()
		step := now.Sub(previous).Nanoseconds()
		if step > 0 && (info.ObservedMinStepNS == 0 || step < info.ObservedMinStepNS) {
			info.ObservedMinStepNS = step
		}
		previous = now
	}
	info.MeanReadCostNS = float64(previous.Sub(start).Nanoseconds()) / probes
	return info
}

func EventsPerSecond(count uint64, interval time.Duration) float64 {
	if interval <= 0 {
		return 0
	}
	return float64(count) / interval.Seconds()
}
