//go:build !unix && !windows

package main

import "os"

func terminationSignals() []os.Signal {
	return []os.Signal{os.Interrupt}
}
