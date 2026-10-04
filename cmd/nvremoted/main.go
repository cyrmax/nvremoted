// Copyright © 2023 Niko Carpenter <niko@nikocarpenter.com>
//
// This source code is governed by the MIT license, which can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/n0ot/nvremoted/cmd/nvremoted/commands"
)

func main() {
	// Process-owned subscription deliberately remains installed until Exit,
	// including the gap after command cleanup and before actual process exit.
	ctx, _ := signal.NotifyContext(context.Background(), terminationSignals()...)
	err := commands.Execute(ctx)
	code := 0
	if err != nil {
		reportError(os.Stderr, err, 100*time.Millisecond)
		code = 1
	}
	// All normal command defers have completed. At the bounded stop deadline,
	// Exit is the explicit boundary for work Go cannot forcibly interrupt.
	os.Exit(code)
}

// A full stderr pipe must not hold the process alive after the stop budget.
// Error diagnostics are best effort; the exit status remains authoritative.
func reportError(out io.Writer, err error, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		fmt.Fprintln(out, err)
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
}
