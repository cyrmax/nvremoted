//go:build !linux && !darwin && !windows

package perf

func processResources() (float64, float64, uint64, string, bool) {
	return 0, 0, 0, "unavailable", false
}
func cpuModel() string { return "unknown" }
