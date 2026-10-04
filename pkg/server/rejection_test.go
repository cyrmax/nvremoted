package server

import (
	"testing"
	"time"
)

func TestRejectionScheduleAbsoluteWindowAndFlood(t *testing.T) {
	start := time.Unix(0, 0)
	s := newRejectionSchedule(start, 5*time.Second, time.Second)
	count := 1 // immediate announcement
	last := start
	for ms := 1; ms <= 6000; ms++ {
		now := start.Add(time.Duration(ms) * time.Millisecond)
		s.input = true
		if s.announce(now) {
			if now.Sub(last) < 250*time.Millisecond {
				t.Fatal("amplification")
			}
			last = now
			count++
		}
	}
	if count != 20 || s.deadline != start.Add(5*time.Second) {
		t.Fatalf("count=%d deadline=%v", count, s.deadline)
	}
}

func TestRejectionSchedulePeriodicAndCoalescing(t *testing.T) {
	start := time.Unix(0, 0)
	s := newRejectionSchedule(start, 5*time.Second, time.Second)
	for sec := 1; sec < 5; sec++ {
		if !s.announce(start.Add(time.Duration(sec) * time.Second)) {
			t.Fatal("missing periodic announcement")
		}
	}
	if s.announce(start.Add(5 * time.Second)) {
		t.Fatal("deadline response")
	}
	s = newRejectionSchedule(start, 5*time.Second, time.Second)
	s.input = true
	if s.announce(start.Add(249*time.Millisecond)) || s.next(start) != 250*time.Millisecond {
		t.Fatal("coalescing failed")
	}
	if !s.announce(start.Add(250*time.Millisecond)) || !s.announce(start.Add(time.Second)) {
		t.Fatal("input suppressed periodic")
	}
}
