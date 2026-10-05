package perf

import (
	"sync"
	"testing"
	"time"
)

func TestClockMonotonic(t *testing.T) {
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			previous := Now()
			for j := 0; j < 1000; j++ {
				next := Now()
				if next.Before(previous) {
					t.Error("performance clock moved backwards")
					return
				}
				previous = next
			}
		}()
	}
	workers.Wait()
	if EventsPerSecond(10, 0) != 0 || EventsPerSecond(10, time.Second) != 10 {
		t.Error("invalid rate conversion")
	}
}
