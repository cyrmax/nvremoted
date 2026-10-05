//go:build windows

package perf

import (
	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
	"os"
	"unsafe"
)

var memoryInfo = windows.NewLazySystemDLL("psapi.dll").NewProc("GetProcessMemoryInfo")

type processMemoryCounters struct {
	Size                                                   uint32
	PageFaults                                             uint32
	PeakWorkingSet, WorkingSet, PeakPagedPool, PagedPool   uintptr
	PeakNonPagedPool, NonPagedPool, Pagefile, PeakPagefile uintptr
}

func processResources() (user, system float64, rss uint64, kind string, available bool) {
	process := windows.CurrentProcess()
	var created, exited, kernel, usr windows.Filetime
	err := windows.GetProcessTimes(process, &created, &exited, &kernel, &usr)
	if err == nil {
		user = float64(uint64(usr.HighDateTime)<<32|uint64(usr.LowDateTime)) / 1e7
		system = float64(uint64(kernel.HighDateTime)<<32|uint64(kernel.LowDateTime)) / 1e7
		available = true
	}
	c := processMemoryCounters{}
	c.Size = uint32(unsafe.Sizeof(c))
	ok, _, _ := memoryInfo.Call(uintptr(process), uintptr(unsafe.Pointer(&c)), uintptr(c.Size))
	if ok != 0 {
		rss = uint64(c.WorkingSet)
		kind = "current_working_set"
	} else {
		kind = "unavailable"
	}
	return
}

func cpuModel() string {
	key, err := registry.OpenKey(registry.LOCAL_MACHINE, `HARDWARE\DESCRIPTION\System\CentralProcessor\0`, registry.QUERY_VALUE)
	if err == nil {
		defer key.Close()
		value, _, err := key.GetStringValue("ProcessorNameString")
		if err == nil {
			return value
		}
	}
	return os.Getenv("PROCESSOR_IDENTIFIER")
}
