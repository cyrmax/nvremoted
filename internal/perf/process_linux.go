//go:build linux

package perf

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

func processResources() (user, system float64, rss uint64, kind string, available bool) {
	var r syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &r) == nil {
		user = float64(r.Utime.Sec) + float64(r.Utime.Usec)/1e6
		system = float64(r.Stime.Sec) + float64(r.Stime.Usec)/1e6
		available = true
	}
	kind = "unavailable"
	if data, err := os.ReadFile("/proc/self/statm"); err == nil {
		fields := strings.Fields(string(data))
		if len(fields) > 1 {
			pages, err := strconv.ParseUint(fields[1], 10, 64)
			if err == nil {
				rss = pages * uint64(os.Getpagesize())
				kind = "current_rss"
			}
		}
	}
	return
}

func cpuModel() string {
	data, err := os.ReadFile("/proc/cpuinfo")
	if err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			key, value, ok := strings.Cut(line, ":")
			if ok && (strings.TrimSpace(key) == "model name" || strings.TrimSpace(key) == "Hardware") {
				return strings.TrimSpace(value)
			}
		}
	}
	return "unknown"
}
