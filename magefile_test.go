//go:build mage

package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestRepeatPolicy(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"", "100"}, {"1", "1"}, {"007", "7"}, {"0", ""}, {"-1", ""}, {"oops", ""},
	} {
		t.Setenv("NVREMOTED_TEST_COUNT", tc.input)
		got, err := testCount()
		if got != tc.want || (err != nil) != (tc.want == "") {
			t.Fatalf("count %q: got %q, %v; want %q", tc.input, got, err, tc.want)
		}
	}
}

func TestDistributionNames(t *testing.T) {
	for _, goos := range []string{"linux", "windows", "darwin"} {
		for _, arch := range []string{"amd64", "arm64"} {
			name := "nvremoted"
			if goos == "windows" {
				name += ".exe"
			}
			if got := binaryPath(goos, arch); got != filepath.Join("dist", goos+"-"+arch, name) {
				t.Fatal(got)
			}
		}
	}
}

func inFixture(t *testing.T) string {
	t.Helper()
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(old); err != nil {
			t.Error(err)
		}
	})
	return dir
}

func putFixture(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(name, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

func TestFormatPreservesDirtySource(t *testing.T) {
	inFixture(t)
	source := "package main\nfunc dirty( ){ }\n"
	putFixture(t, "dirty.go", source)
	putFixture(t, "untracked.txt", "user content")
	if err := checkFormat(); err == nil {
		t.Fatal("unformatted source accepted")
	}
	got, err := os.ReadFile("dirty.go")
	if err != nil || string(got) != source {
		t.Fatalf("source changed: %q, %v", got, err)
	}
	got, err = os.ReadFile("untracked.txt")
	if err != nil || string(got) != "user content" {
		t.Fatal("untracked file changed")
	}
}

func TestChecksumsAndCleanup(t *testing.T) {
	inFixture(t)
	output := binaryPath("windows", "arm64")
	if err := os.MkdirAll(filepath.Dir(output), 0755); err != nil {
		t.Fatal(err)
	}
	putFixture(t, output, "binary fixture")
	putFixture(t, "source.txt", "keep me")
	if err := writeChecksum(output); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(output + ".sha256")
	want := fmt.Sprintf("%x  nvremoted.exe\n", sha256.Sum256([]byte("binary fixture")))
	if err != nil || string(got) != want {
		t.Fatalf("checksum: %q, %v", got, err)
	}
	if err := Clean(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat("dist"); !os.IsNotExist(err) {
		t.Fatal("output still exists", err)
	}
	if got, err := os.ReadFile("source.txt"); err != nil || string(got) != "keep me" {
		t.Fatal("source changed")
	}
	if err := Clean(); err != nil {
		t.Fatal("clean is not idempotent", err)
	}
}

// A real child process checks environment overrides and failure propagation.
// It runs the test executable in place of Go, without choosing a host compiler.
func TestGoEnvironmentIsolation(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOEXE", exe)
	t.Setenv("NVREMOTED_GO_CHILD", "1")
	output := filepath.Join(t.TempDir(), "env.json")
	t.Setenv("NVREMOTED_GO_CHILD_OUTPUT", output)
	t.Setenv("GOOS", "old-os")
	t.Setenv("GOARCH", "old-arch")
	t.Setenv("CGO_ENABLED", "old-cgo")
	for _, tc := range []struct{ os, arch, cgo string }{
		{"linux", "arm64", "0"}, {"windows", "amd64", "1"}, {"darwin", "arm64", "0"},
	} {
		if err := runGo(tc.os, tc.arch, tc.cgo, "-test.run=^TestGoChild$"); err == nil {
			t.Fatal("child failure did not propagate")
		}
		data, err := os.ReadFile(output)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatal(err)
		}
		want := []string{tc.os, tc.arch, tc.cgo, "on", "off", os.Getenv("CC"), os.Getenv("PATH")}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("child env: %q; want %q", got, want)
		}
		if os.Getenv("GOOS") != "old-os" || os.Getenv("GOARCH") != "old-arch" || os.Getenv("CGO_ENABLED") != "old-cgo" {
			t.Fatal("target settings leaked into parent")
		}
	}
}

func TestGoChild(t *testing.T) {
	if os.Getenv("NVREMOTED_GO_CHILD") != "1" {
		return
	}
	var values []string
	for _, key := range []string{"GOOS", "GOARCH", "CGO_ENABLED", "GO111MODULE", "GOWORK", "CC", "PATH"} {
		values = append(values, os.Getenv(key))
	}
	data, _ := json.Marshal(values)
	if err := os.WriteFile(os.Getenv("NVREMOTED_GO_CHILD_OUTPUT"), data, 0644); err != nil {
		os.Exit(2)
	}
	os.Exit(1)
}
