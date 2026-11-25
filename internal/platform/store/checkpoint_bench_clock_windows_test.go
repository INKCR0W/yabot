//go:build bench

package store

import (
	"syscall"
	"testing"
	"time"
	"unsafe"
)

func storeBenchClock(t *testing.T) func() time.Duration {
	t.Helper()
	kernel := syscall.NewLazyDLL("kernel32.dll")
	frequencyProc := kernel.NewProc("QueryPerformanceFrequency")
	counterProc := kernel.NewProc("QueryPerformanceCounter")
	for _, proc := range []*syscall.LazyProc{frequencyProc, counterProc} {
		if err := proc.Find(); err != nil {
			t.Fatalf("find %s: %v", proc.Name, err)
		}
	}
	var frequency int64
	if ok, _, err := frequencyProc.Call(uintptr(unsafe.Pointer(&frequency))); ok == 0 || frequency <= 0 {
		t.Fatalf("read performance counter frequency: frequency %d, error %v", frequency, err)
	}
	read := func() int64 {
		var counter int64
		if ok, _, err := counterProc.Call(uintptr(unsafe.Pointer(&counter))); ok == 0 {
			t.Errorf("read performance counter: %v", err)
		}
		return counter
	}
	// Windows time.Now can quantize sub-millisecond latencies to zero.
	origin := read()
	nanosecondsPerCount := float64(time.Second) / float64(frequency)
	return func() time.Duration {
		return time.Duration(float64(read()-origin) * nanosecondsPerCount)
	}
}
