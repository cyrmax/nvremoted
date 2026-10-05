//go:build performance

package server

import (
	"testing"
	"time"

	"github.com/n0ot/nvremoted/internal/perf"
)

var perfClockSink time.Time

// The instrumentation clock is measured separately from production fragments.
func BenchmarkPerformanceClock(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		perfClockSink = perf.Now()
	}
}
