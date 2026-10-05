//go:build windows

package perf

import (
	"testing"
	"time"
)

func TestQPCElapsed(t *testing.T) {
	for _, sample := range []struct {
		ticks, frequency int64
		want             time.Duration
	}{
		{0, 10_000_000, 0}, {1, 10_000_000, 100},
		{15_000_000, 10_000_000, 1500 * time.Millisecond},
		{2_592_000_000_000, 10_000_000, 72 * time.Hour},
		{15_999_999_999, 8_000_000_000, 2*time.Second - time.Nanosecond},
	} {
		if got := qpcElapsed(sample.ticks, sample.frequency); got != sample.want {
			t.Errorf("ticks=%d frequency=%d: got %s want %s", sample.ticks, sample.frequency, got, sample.want)
		}
	}
}
