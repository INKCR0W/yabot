//go:build unix

package store

import (
	"os"
	"syscall"
)

const lockConflict = syscall.EWOULDBLOCK

func lockFile(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func unlockFile(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}
