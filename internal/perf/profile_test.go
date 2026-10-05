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

func TestPerformanceProfileSnapshotWriteFailure(t *testing.T) {
	for _, mode := range []string{"heap", "allocs", "goroutine"} {
		t.Run(mode, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "profiles")
			stop, err := StartProfiles(directory, mode)
			if err != nil {
				t.Fatal(err)
			}
			// Replace the empty output directory with a file to cause a portable
			// snapshot write failure without disk quotas or timing coordination.
			if err := os.Remove(directory); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(directory, []byte("occupied"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := stop(); err == nil {
				t.Fatal("snapshot write failure was not reported")
			}
		})
	}
}
