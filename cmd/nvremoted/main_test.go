package main

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestErrorReporting(t *testing.T) {
	var output bytes.Buffer
	reportError(&output, errors.New("startup failed"), time.Second)
	if output.String() != "startup failed\n" {
		t.Fatalf("error diagnostic: %q", output.String())
	}
}

func TestBlockedStderrCannotPreventExitBoundary(t *testing.T) {
	w := &blockedDiagnosticWriter{entered: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		reportError(w, errors.New("shutdown timed out"), 20*time.Millisecond)
		close(done)
	}()
	select {
	case <-w.entered:
	case <-time.After(time.Second):
		t.Fatal("diagnostic did not reach the blocked writer")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("blocked stderr prevented returning to process exit")
	}
	close(w.release)
	select {
	case <-w.finished:
	case <-time.After(time.Second):
		t.Fatal("released diagnostic worker leaked")
	}
}

type blockedDiagnosticWriter struct {
	entered  chan struct{}
	release  chan struct{}
	finished chan struct{}
}

func (w *blockedDiagnosticWriter) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	close(w.finished)
	return len(p), nil
}
