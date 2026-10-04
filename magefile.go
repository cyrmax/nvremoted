//go:build mage

// Copyright © 2023 Niko Carpenter <niko@nikocarpenter.com>
//
// This source code is governed by the MIT license, which can be found in the LICENSE file.

package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"go/format"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/magefile/mage/sh"
)

const (
	packageName = "github.com/n0ot/nvremoted/cmd/nvremoted"
	outDir      = "dist"
)

var Default = Build

// Build builds NVRemoted for the host OS and architecture, regardless of GOOS/GOARCH.
func Build() error { return buildPlatform(runtime.GOOS, runtime.GOARCH, "0", false) }

// BuildLinuxAmd64 builds the Linux amd64 distribution.
func BuildLinuxAmd64() error { return buildPlatform("linux", "amd64", "0", false) }

// BuildLinuxArm64 builds the Linux arm64 distribution.
func BuildLinuxArm64() error { return buildPlatform("linux", "arm64", "0", false) }

// BuildWindowsAmd64 builds the Windows amd64 distribution.
func BuildWindowsAmd64() error { return buildPlatform("windows", "amd64", "0", false) }

// BuildWindowsArm64 builds the Windows arm64 distribution.
func BuildWindowsArm64() error { return buildPlatform("windows", "arm64", "0", false) }

// BuildDarwinAmd64 builds the Darwin amd64 distribution.
func BuildDarwinAmd64() error { return buildPlatform("darwin", "amd64", "0", false) }

// BuildDarwinArm64 builds the Darwin arm64 distribution.
func BuildDarwinArm64() error { return buildPlatform("darwin", "arm64", "0", false) }

// BuildAll builds the complete distribution matrix sequentially.
func BuildAll() error {
	for _, build := range []func() error{
		BuildLinuxAmd64, BuildLinuxArm64, BuildWindowsAmd64,
		BuildWindowsArm64, BuildDarwinAmd64, BuildDarwinArm64,
	} {
		if err := build(); err != nil {
			return err
		}
	}
	return nil
}

// BuildRace retains the legacy host race build; the caller supplies a C compiler.
func BuildRace() error { return buildPlatform(runtime.GOOS, runtime.GOARCH, "1", true) }

// Install retains the legacy install target with the same version and CGO policy.
func Install() error {
	return runGo(runtime.GOOS, runtime.GOARCH, "0", "install", "-mod=readonly", "-ldflags", versionFlags(), packageName)
}

func binaryPath(goos, goarch string) string {
	name := "nvremoted"
	if goos == "windows" {
		name += ".exe"
	}
	return filepath.Join(outDir, goos+"-"+goarch, name)
}

func buildPlatform(goos, goarch, cgo string, race bool) error {
	output := binaryPath(goos, goarch)
	if err := os.MkdirAll(filepath.Dir(output), 0755); err != nil {
		return err
	}
	args := []string{"build", "-mod=readonly", "-ldflags", versionFlags(), "-o", output}
	if race {
		args = append(args, "-race")
	}
	args = append(args, packageName)
	if err := runGo(goos, goarch, cgo, args...); err != nil {
		return err
	}
	return writeChecksum(output)
}

func versionFlags() string {
	version, err := sh.Output("git", "describe", "--always", "--long", "--dirty")
	if err != nil {
		version = "unset"
	}
	return "-X " + packageName + "/commands.Version=" + version
}

func writeChecksum(output string) error {
	file, err := os.Open(output)
	if err != nil {
		return err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return err
	}
	line := fmt.Sprintf("%x  %s\n", hash.Sum(nil), filepath.Base(output))
	return os.WriteFile(output+".sha256", []byte(line), 0644)
}

// runGo isolates target settings to each child. Compiler and PATH settings belong
// to the caller. exec.Cmd resolves overrides without mutating Mage's environment.
func runGo(goos, goarch, cgo string, args ...string) error {
	goexe := os.Getenv("GOEXE")
	if goexe == "" {
		goexe = "go"
	}
	cmd := exec.Command(goexe, args...)
	cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch,
		"CGO_ENABLED="+cgo, "GO111MODULE=on", "GOWORK=off")
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	return cmd.Run()
}

func testCount() (string, error) {
	count := os.Getenv("NVREMOTED_TEST_COUNT")
	if count == "" {
		return "100", nil
	}
	n, err := strconv.Atoi(count)
	if err != nil || n < 1 {
		return "", fmt.Errorf("NVREMOTED_TEST_COUNT must be a positive integer, got %q", count)
	}
	return strconv.Itoa(n), nil
}

// Test runs all tests 100 times (override with NVREMOTED_TEST_COUNT).
func Test() error { return runTests(false) }

// TestRace runs the same repetitions with race detection and CGO enabled.
// Supply a suitable host C compiler in the calling environment.
func TestRace() error { return runTests(true) }

func runTests(race bool) error {
	count, err := testCount()
	if err != nil {
		return err
	}
	cgo := "0"
	args := []string{"test", "-mod=readonly", "-count=" + count}
	if race {
		cgo = "1"
		args = append(args, "-race")
	}
	return runGo(runtime.GOOS, runtime.GOARCH, cgo, append(args, "-tags=mage", "./...")...)
}

// Check checks Go formatting, module tidiness and vet without changing files.
// All three checks run, even if an earlier check fails; diffs stay visible.
func Check() error {
	return errors.Join(checkFormat(),
		runGo(runtime.GOOS, runtime.GOARCH, "0", "mod", "tidy", "-diff"),
		runGo(runtime.GOOS, runtime.GOARCH, "0", "vet", "-mod=readonly", "-tags=mage", "./..."))
}

func checkFormat() error {
	var unformatted []string
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", outDir, "bin", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || !entry.Type().IsRegular() {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		formatted, err := format.Source(source)
		if err != nil {
			return fmt.Errorf("gofmt %s: %w", path, err)
		}
		if !bytes.Equal(source, formatted) {
			unformatted = append(unformatted, path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(unformatted) == 0 {
		return nil
	}
	cmd := exec.Command("gofmt", append([]string{"-d"}, unformatted...)...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}
	return fmt.Errorf("gofmt required for %v", unformatted)
}

// Clean removes only generated distribution output and the legacy bin directory.
func Clean() error {
	// Inspect both trees first. Never traverse output redirected by a symlink or
	// Windows junction, including a nested link, during cleanup.
	for _, dir := range []string{outDir, "bin"} {
		if err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
			if os.IsNotExist(err) && path == dir {
				return nil
			}
			if err != nil {
				return err
			}
			if entry.Type()&(os.ModeSymlink|os.ModeIrregular) != 0 {
				return fmt.Errorf("refusing to clean linked output: %s", path)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return errors.Join(os.RemoveAll(outDir), os.RemoveAll("bin"))
}
