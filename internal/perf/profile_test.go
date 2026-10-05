package perf

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestPerformanceProfilesRejectCombinedMode(t *testing.T) {
	// Rejection must precede filesystem writes and any global profiler changes.
	directory := filepath.Join(t.TempDir(), "not-created")
	oldMutex := runtime.SetMutexProfileFraction(7)
	defer runtime.SetMutexProfileFraction(oldMutex)
	stop, err := StartProfiles(directory, "all")
	if err == nil || stop != nil {
		t.Fatal("combined mode must fail before starting profiles")
	}
	if got := runtime.SetMutexProfileFraction(-1); got != 7 {
		t.Fatalf("rejected mode changed mutex sampling to %d", got)
	}
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatalf("rejected mode created output: %v", err)
	}
	config, err := ParseConfig("profile", func(key string) string {
		if key == "NVREMOTED_BENCH_PROFILE" {
			return "all"
		}
		return ""
	})
	if err != nil || config.Profile != "all" {
		t.Fatalf("Mage combined orchestration config rejected: %+v, %v", config, err)
	}
}
