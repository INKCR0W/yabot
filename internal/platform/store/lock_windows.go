//go:build windows

package store

import (
	"os"
	"syscall"
	"unsafe"
)

const (
	lockConflict        = syscall.Errno(33) // ERROR_LOCK_VIOLATION
	lockFailImmediately = 0x00000001
	lockExclusive       = 0x00000002
	lockLength          = 1
)

func lockFile(file *os.File) error {
	proc := syscall.NewLazyDLL("kernel32.dll").NewProc("LockFileEx")
	if err := proc.Find(); err != nil {
		return err
	}
	var offset syscall.Overlapped
	result, _, err := proc.Call(file.Fd(), lockExclusive|lockFailImmediately, 0, lockLength, 0, uintptr(unsafe.Pointer(&offset)))
	if result == 0 {
		return err
	}
	return nil
}

func unlockFile(file *os.File) error {
	proc := syscall.NewLazyDLL("kernel32.dll").NewProc("UnlockFileEx")
	if err := proc.Find(); err != nil {
		return err
	}
	var offset syscall.Overlapped
	result, _, err := proc.Call(file.Fd(), 0, lockLength, 0, uintptr(unsafe.Pointer(&offset)))
	if result == 0 {
		return err
	}
	return nil
}
