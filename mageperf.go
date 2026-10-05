//go:build mage

package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/n0ot/nvremoted/internal/perf"
)

// Bench runs local component benchmarks and the quick TCP/TLS relay suite.
func Bench() error { return runPerformance("quick", true) }

// BenchLong runs the extended local payload/load/client/lifecycle matrix.
func BenchLong() error { return runPerformance("long", true) }

// BenchRelay runs only end-to-end scenarios (quick preset plus overrides).
func BenchRelay() error { return runPerformance("quick", false) }

// BenchProfile profiles a sustained mixed TCP/TLS workload separately from latency baselines.
func BenchProfile() error { return runPerformance("profile", false) }

// BenchMicro writes standard testing.B results compatible with benchstat.
func BenchMicro() error {
	c, err := perf.ConfigFromEnv("quick")
	if err != nil {
		return err
	}
	directory, err := performanceDirectory(c)
	if err != nil {
		return err
	}
	abs, err := filepath.Abs(directory)
	if err != nil {
		return err
	}
	fmt.Println("Local component output:", abs)
	return runComponentBenchmarks(abs, "quick")
}

// BenchSmoke checks tagged harness correctness, without performance thresholds.
func BenchSmoke() error {
	return runGo(runtime.GOOS, runtime.GOARCH, "0", "test", "-mod=readonly", "-tags=performance", "-run=^TestPerformance", "-count=1", "./pkg/server", "./internal/perf")
}

// BenchCheck vets the opt-in performance code without measuring performance.
func BenchCheck() error {
	return runGo(runtime.GOOS, runtime.GOARCH, "0", "vet", "-mod=readonly", "-tags=mage,performance", "./...")
}

// BenchSmokeRace checks the tagged harness with race detection. Caller supplies CC/PATH.
func BenchSmokeRace() error {
	return runGo(runtime.GOOS, runtime.GOARCH, "1", "test", "-mod=readonly", "-tags=performance", "-race", "-run=^TestPerformance", "-count=1", "./pkg/server", "./internal/perf")
}

// BenchCompileAll checks portability of the tagged harness without running it.
func BenchCompileAll() error {
	directory, err := performanceDirectory(perf.Config{Preset: "compile"})
	if err != nil {
		return err
	}
	for _, goos := range []string{"linux", "windows", "darwin"} {
		for _, arch := range []string{"amd64", "arm64"} {
			binary := filepath.Join(directory, goos+"-"+arch+".test")
			if goos == "windows" {
				binary += ".exe"
			}
			if err := runGo(goos, arch, "0", "test", "-mod=readonly", "-tags=performance", "-c", "-o", binary, "./pkg/server"); err != nil {
				return err
			}
		}
	}
	return nil
}

// BenchCompare prints descriptive deltas between two versioned JSON reports.
func BenchCompare(baseline, candidate string) error {
	before, err := perf.ReadReport(baseline)
	if err != nil {
		return err
	}
	after, err := perf.ReadReport(candidate)
	if err != nil {
		return err
	}
	return perf.Compare(os.Stdout, before, after)
}

func performanceDirectory(c perf.Config) (string, error) {
	if c.Output != "" {
		directory, err := filepath.Abs(c.Output)
		if err != nil {
			return "", err
		}
		// Do not overwrite an earlier run. The suite updates its own report as it
		// progresses; a new invocation always starts with a fresh directory.
		if err = os.Mkdir(directory, 0755); err != nil {
			return "", fmt.Errorf("new performance output directory: %w", err)
		}
		return directory, nil
	}
	root := "perf-results"
	if err := os.MkdirAll(root, 0755); err != nil {
		return "", err
	}
	return os.MkdirTemp(root, time.Now().UTC().Format("20060102T150405")+"-"+c.Preset+"-")
}

func runPerformance(preset string, components bool) error {
	c, err := perf.ConfigFromEnv(preset)
	if err != nil {
		return err
	}
	if len(perf.Scenarios(c)) == 0 {
		return fmt.Errorf("benchmark filter selects no scenarios")
	}
	timeout := 6 * time.Hour
	if value := os.Getenv("NVREMOTED_BENCH_TIMEOUT"); value != "" {
		timeout, err = time.ParseDuration(value)
		if err != nil || timeout < 0 {
			return fmt.Errorf("NVREMOTED_BENCH_TIMEOUT must be a nonnegative Go duration")
		}
	}
	directory, err := performanceDirectory(c)
	if err != nil {
		return err
	}
	abs, err := filepath.Abs(directory)
	if err != nil {
		return err
	}
	fmt.Println("Local performance output:", abs)
	if components {
		if err = runComponentBenchmarks(abs, preset); err != nil {
			return err
		}
	}
	if c.Profile == "all" {
		// CPU and contention samplers use separate fresh processes. Combining
		// them can amplify observer contention and hung in local Windows runs.
		for _, mode := range []string{"cpu", "block", "mutex"} {
			child := filepath.Join(abs, mode)
			if err := os.Mkdir(child, 0755); err != nil {
				return err
			}
			fmt.Println("Isolated profile:", mode, child)
			if err := runPerformanceScenarios(child, preset, mode, timeout); err != nil {
				return fmt.Errorf("%s profile run: %w", mode, err)
			}
		}
		return nil
	}
	return runPerformanceScenarios(abs, preset, c.Profile, timeout)
}

func runPerformanceScenarios(directory, preset, profile string, timeout time.Duration) error {
	file, err := os.Create(filepath.Join(directory, "runner.txt"))
	if err != nil {
		return err
	}
	defer file.Close()
	binary := filepath.Join(directory, "relay.test")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	args := []string{"test", "-mod=readonly", "-tags=performance", "-run=^TestLocalPerformance$", "-count=1", "-timeout=" + timeout.String(), "-v", "-o", binary, "./pkg/server"}
	return runPerformanceGo(io.MultiWriter(os.Stdout, file), []string{"NVREMOTED_BENCH_RUN=1", "NVREMOTED_BENCH_PRESET=" + preset, "NVREMOTED_BENCH_OUTPUT=" + directory, "NVREMOTED_BENCH_PROFILE=" + profile}, args...)
}

func runComponentBenchmarks(directory, preset string) error {
	filter := os.Getenv("NVREMOTED_BENCH_COMPONENT_FILTER")
	if filter == "" {
		filter = "^BenchmarkPerformance"
	}
	timeArg := "100ms"
	count := 1
	if preset == "long" {
		timeArg = "200ms"
		count = 5
	}
	if override := os.Getenv("NVREMOTED_BENCH_BENCHTIME"); override != "" {
		timeArg = override
	}
	if value := os.Getenv("NVREMOTED_BENCH_COMPONENT_COUNT"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > 100 {
			return fmt.Errorf("component count must be between 1 and 100")
		}
		count = n
	}
	f, err := os.Create(filepath.Join(directory, "components.txt"))
	if err != nil {
		return err
	}
	defer f.Close()
	return runPerformanceGo(io.MultiWriter(os.Stdout, f), nil, "test", "-mod=readonly", "-tags=performance", "-run=^$", "-bench="+filter, "-benchmem", "-benchtime="+timeArg, "-count="+strconv.Itoa(count), "-timeout=0", "./pkg/server")
}

func runPerformanceGo(output io.Writer, extraEnv []string, args ...string) error {
	goexe := os.Getenv("GOEXE")
	if goexe == "" {
		goexe = "go"
	}
	cmd := exec.Command(goexe, args...)
	cmd.Env = append(os.Environ(), "GOOS="+runtime.GOOS, "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0", "GO111MODULE=on", "GOWORK=off")
	cmd.Env = append(cmd.Env, extraEnv...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, output, output
	return cmd.Run()
}
