//go:build !windows && !plan9

package server

import (
	"errors"
	"syscall"
)

func isAcceptResourceError(err error) bool {
	return errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) ||
		errors.Is(err, syscall.ENOBUFS) || errors.Is(err, syscall.ENOMEM)
}
