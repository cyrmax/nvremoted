//go:build unix || windows

package main

import (
	"os"
	"syscall"
)

func terminationSignals() []os.Signal {
	// On Windows SIGTERM represents console close/logoff/shutdown events,
	// not a Unix signal or a promise that the OS will wait for cleanup.
	return []os.Signal{os.Interrupt, syscall.SIGTERM}
}
