package server

import (
	"errors"
	"syscall"
)

// Winsock resource errors are not named by the frozen syscall package.
// https://learn.microsoft.com/en-us/windows/win32/winsock/windows-sockets-error-codes-2
const (
	wsaEMFILE  syscall.Errno = 10024
	wsaENOBUFS syscall.Errno = 10055
)

func isAcceptResourceError(err error) bool {
	return errors.Is(err, wsaEMFILE) || errors.Is(err, wsaENOBUFS) ||
		errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) ||
		errors.Is(err, syscall.ENOBUFS) || errors.Is(err, syscall.ENOMEM)
}
