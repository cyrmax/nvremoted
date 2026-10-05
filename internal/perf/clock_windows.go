//go:build windows

package perf

import (
	"math/bits"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	qpcDLL                   = windows.NewLazySystemDLL("kernel32.dll")
	qpcCounter               = qpcDLL.NewProc("QueryPerformanceCounter")
	qpcAddress               = qpcCounter.Addr()
	qpcFrequency             = qpcDLL.NewProc("QueryPerformanceFrequency")
	qpcBuffers               = sync.Pool{New: func() any { return new(int64) }}
	qpcHz, qpcBase, qpcEpoch = initializeQPC()
)

func initializeQPC() (int64, int64, time.Time) {
	var frequency int64
	ok, _, _ := qpcFrequency.Call(uintptr(unsafe.Pointer(&frequency)))
	if ok == 0 || frequency <= 0 {
		panic("performance clock: QueryPerformanceFrequency failed")
	}
	epoch := time.Now()
	return frequency, readQPC(), epoch
}

func readQPC() int64 {
	// The syscall pointer escapes. Reuse a goroutine-local checked-out buffer
	// rather than allocating one on every clock read or serializing readers.
	buffer := qpcBuffers.Get().(*int64)
	ok, _, _ := syscall.SyscallN(qpcAddress, uintptr(unsafe.Pointer(buffer)))
	value := *buffer
	qpcBuffers.Put(buffer)
	if ok == 0 {
		panic("performance clock: QueryPerformanceCounter failed")
	}
	return value
}

func qpcElapsed(ticks, frequency int64) time.Duration {
	seconds, remainder := ticks/frequency, ticks%frequency
	hi, lo := bits.Mul64(uint64(remainder), uint64(time.Second))
	fraction, _ := bits.Div64(hi, lo, uint64(frequency))
	return time.Duration(seconds)*time.Second + time.Duration(fraction)
}

// Now preserves time.Time's monotonic component but derives elapsed time from
// one shared QPC epoch. Native time.Now on Windows can quantize sub-ms latency.
func Now() time.Time                { return qpcEpoch.Add(qpcElapsed(readQPC()-qpcBase, qpcHz)) }
func clockDetails() (string, int64) { return "Windows QueryPerformanceCounter", qpcHz }
