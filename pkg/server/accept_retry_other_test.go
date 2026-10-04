//go:build !windows && !plan9

package server

import (
	"syscall"
	"testing"
)

func TestAcceptResourceShortageRetries(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.ENFILE, syscall.ENOBUFS, syscall.ENOMEM} {
		t.Run(errno.Error(), func(t *testing.T) { testAcceptRetryBackoff(t, errno) })
	}
}
