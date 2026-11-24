package obs

import (
	"fmt"
	"maps"
	"slices"
	"sync"
)

// The zero value of Health is ready to use. Health must not be copied after first use.
type Health struct {
	mu     sync.RWMutex
	checks map[string]func() error
}

type Report struct {
	Healthy  bool
	Failures []Failure
}

type Failure struct {
	Name  string
	Error string
}

func NewHealth() *Health {
	return &Health{}
}

// Register panics for duplicate names and nil checks. Checks must be fast,
// non-blocking, and safe to call concurrently.
func (h *Health) Register(name string, check func() error) {
	if check == nil {
		panic(fmt.Sprintf("health check %q is nil", name))
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.checks[name]; exists {
		panic(fmt.Sprintf("health check %q is already registered", name))
	}
	if h.checks == nil {
		h.checks = make(map[string]func() error)
	}
	h.checks[name] = check
}

// Check evaluates a snapshot of registered checks and orders failures by name.
func (h *Health) Check() Report {
	h.mu.RLock()
	checks := make(map[string]func() error, len(h.checks))
	// Use map iteration so the race detector can observe the snapshot read.
	maps.Copy(checks, h.checks)
	h.mu.RUnlock()

	var failures []Failure
	for _, name := range slices.Sorted(maps.Keys(checks)) {
		if message, failed := runHealthCheck(checks[name]); failed {
			failures = append(failures, Failure{Name: name, Error: message})
		}
	}
	return Report{Healthy: len(failures) == 0, Failures: failures}
}

func runHealthCheck(check func() error) (message string, failed bool) {
	// Under GODEBUG=panicnil=1, recover returns nil for panic(nil).
	completed := false
	defer func() {
		if !completed {
			message = fmt.Sprintf("check panicked: %v", recover())
			failed = true
		}
	}()
	if err := check(); err != nil {
		message = err.Error()
		failed = true
	}
	completed = true
	return message, failed
}
