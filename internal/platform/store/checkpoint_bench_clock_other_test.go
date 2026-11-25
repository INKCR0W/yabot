//go:build bench && !windows

package store

import (
	"testing"
	"time"
)

func storeBenchClock(*testing.T) func() time.Duration {
	origin := time.Now()
	return func() time.Duration { return time.Since(origin) }
}
