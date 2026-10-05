package perf

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
)

// StartProfiles is only called in dedicated profile runs, after network setup
// and warmup. It never enables an HTTP endpoint or changes relay semantics.
func StartProfiles(directory, mode string) (func() error, error) {
	if mode == "" {
		return func() error { return nil }, nil
	}
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}
	var stream *os.File
	if mode == "cpu" || mode == "all" || mode == "trace" {
		name := "cpu.pprof"
		if mode == "trace" {
			name = "execution.trace"
		}
		f, err := os.Create(filepath.Join(directory, name))
		if err != nil {
			return nil, err
		}
		stream = f
		if mode == "trace" {
			err = trace.Start(f)
		} else {
			err = pprof.StartCPUProfile(f)
		}
		if err != nil {
			f.Close()
			return nil, err
		}
	}
	if mode == "block" || mode == "all" {
		runtime.SetBlockProfileRate(1)
	}
	oldMutex := 0
	if mode == "mutex" || mode == "all" {
		oldMutex = runtime.SetMutexProfileFraction(1)
	}
	return func() error {
		var errs []error
		if stream != nil {
			if mode == "trace" {
				trace.Stop()
			} else {
				pprof.StopCPUProfile()
			}
			errs = append(errs, stream.Close())
		}
		if mode == "block" || mode == "all" {
			runtime.SetBlockProfileRate(0)
		}
		if mode == "mutex" || mode == "all" {
			runtime.SetMutexProfileFraction(oldMutex)
		}
		// CPU runs also preserve snapshot profiles for convenient investigation.
		names := []string{mode}
		if mode == "all" || mode == "cpu" {
			names = []string{"heap", "allocs", "goroutine", "block", "mutex"}
		}
		for _, name := range names {
			if name == "cpu" || name == "trace" {
				continue
			}
			p := pprof.Lookup(name)
			if p == nil {
				errs = append(errs, fmt.Errorf("profile %s unavailable", name))
				continue
			}
			f, err := os.Create(filepath.Join(directory, name+".pprof"))
			if err != nil {
				errs = append(errs, err)
				continue
			}
			errs = append(errs, p.WriteTo(f, 0), f.Close())
		}
		return errors.Join(errs...)
	}, nil
}
