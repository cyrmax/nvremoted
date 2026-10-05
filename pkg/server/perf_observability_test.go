//go:build performance

package server

import (
	"testing"
	"time"

	"github.com/n0ot/nvremoted/internal/perf"
	"github.com/sirupsen/logrus"
)

func TestPerformanceFirstOverflowSnapshot(t *testing.T) {
	c := &relayPerfCollector{start: perf.Now().Add(-time.Second), measurement: true, sentInWindow: 20, receivedWindow: 18}
	h := &relayPerfLogHook{}
	h.measurement.Store(c)
	entry := &logrus.Entry{Message: "Client disconnected", Level: logrus.InfoLevel, Data: logrus.Fields{"reason": "Client event queue overflow"}}
	if err := h.Fire(entry); err != nil {
		t.Fatal(err)
	}
	snapshot := h.firstOverflow.Load()
	if snapshot == nil || snapshot.sent != 20 || snapshot.received != 18 || snapshot.elapsedNS <= 0 {
		t.Fatalf("bad first-overflow snapshot: %+v", snapshot)
	}
	c.sentInWindow, c.receivedWindow = 30, 25
	if err := h.Fire(entry); err != nil {
		t.Fatal(err)
	}
	if h.firstOverflow.Load() != snapshot || h.overflow.Load() != 2 {
		t.Fatal("later overflow replaced first snapshot or lost counter")
	}

	h = &relayPerfLogHook{}
	c.measurement = false
	h.measurement.Store(c)
	if err := h.Fire(entry); err != nil {
		t.Fatal(err)
	}
	if h.firstOverflow.Load() != nil || h.overflow.Load() != 1 {
		t.Fatal("warm-up overflow must count without measurement rates")
	}
}
