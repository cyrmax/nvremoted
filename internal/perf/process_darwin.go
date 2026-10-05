//go:build darwin

package perf

import "syscall"

func processResources() (user, system float64, rss uint64, kind string, available bool) {
	var r syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &r) == nil {
		user = float64(r.Utime.Sec) + float64(r.Utime.Usec)/1e6
		system = float64(r.Stime.Sec) + float64(r.Stime.Usec)/1e6
		rss = uint64(r.Maxrss)
		kind = "peak_rss" // Distinct from current RSS; never label this memory/client.
		available = true
	} else {
		kind = "unavailable"
	}
	return
}

func cpuModel() string {
	value, err := syscall.Sysctl("machdep.cpu.brand_string")
	if err != nil {
		return "unknown"
	}
	return value
}
