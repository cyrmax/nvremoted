package server

import (
	"syscall"
	"testing"
)

func TestAcceptWindowsResourceRetries(t *testing.T) {
	// Use real Winsock codes, not syscall's synthetic POSIX errno values.
	for _, errno := range []syscall.Errno{10024, 10055} {
		t.Run(errno.Error(), func(t *testing.T) { testAcceptRetryBackoff(t, errno) })
	}
}
