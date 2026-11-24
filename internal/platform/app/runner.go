package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync/atomic"
	"time"
)

type Component interface {
	Name() string
	// Start receives a child of Run's context that is canceled on the first Fatal.
	// It should return promptly on cancellation.
	Start(ctx context.Context) error
	// Stop gets a fresh deadline and may outlive Run if it ignores cancellation.
	Stop(ctx context.Context) error
}

type Fatal = func(component string, err error)

// Runner must be created with NewRunner and must not be copied.
type Runner struct {
	log         *slog.Logger
	stopTimeout time.Duration
	hasRun      atomic.Bool
	fatal       atomic.Pointer[fatalReport]
	fatalCause  chan error
	ignored     atomic.Int64
}

type fatalReport struct {
	component string
	err       error
}

// NewRunner uses a 10-second timeout per Stop. A nil logger discards logs.
func NewRunner(log *slog.Logger) *Runner {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Runner{log: log, stopTimeout: 10 * time.Second, fatalCause: make(chan error, 1)}
}

// Fatal can be obtained before building components and called concurrently at any time.
// The first report through shutdown is returned by Run. Reports are logged asynchronously at Error level.
// Ignored reports are grouped in the logs with counts and one example per group.
func (r *Runner) Fatal() Fatal {
	return func(component string, err error) {
		if r.fatal.CompareAndSwap(nil, &fatalReport{component, err}) {
			r.fatalCause <- err
			go r.log.Error("component reported a fatal error", "component", component, "error", err)
			return
		}
		// Count reports while one goroutine logs, even if the handler is slow.
		if r.ignored.Add(1) == 1 {
			go r.logIgnoredFatals(component, err)
		}
	}
}

func (r *Runner) logIgnoredFatals(component string, err error) {
	count := r.ignored.Load()
	r.log.Error("ignored fatal reports", "count", count, "example_component", component, "example_error", err)
	for count = r.ignored.Add(-count); count > 0; count = r.ignored.Add(-count) {
		r.log.Error("ignored additional fatal reports", "count", count)
	}
}

// Run may be called once and accepts Fatal reports until all Stop attempts finish or time out.
// It returns the first Fatal along with any startup error, or nil for cancellation alone.
func (r *Runner) Run(ctx context.Context, comps ...Component) error {
	if !r.hasRun.CompareAndSwap(false, true) {
		return errors.New("runner can only run once")
	}
	startCtx, cancelStart := context.WithCancelCause(ctx)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case cause := <-r.fatalCause:
			cancelStart(cause)
		case <-startCtx.Done():
		}
	}()
	defer func() {
		cancelStart(nil)
		<-watchDone
	}()
	started, runErr := r.startAndWait(startCtx, comps)
	r.stop(ctx, started)
	if report := r.fatal.Swap(&fatalReport{}); report != nil {
		cause := report.err
		if cause == nil {
			cause = errors.New("no cause provided")
		}
		fatalErr := fmt.Errorf("component %q reported a fatal error: %w", report.component, cause)
		if runErr == nil {
			runErr = fatalErr
		} else {
			runErr = errors.Join(fatalErr, runErr)
		}
	}
	return runErr
}

func (r *Runner) startAndWait(ctx context.Context, comps []Component) ([]namedComponent, error) {
	components, err := nameComponents(comps)
	if err != nil {
		return nil, err
	}
	for i, component := range components {
		if ctx.Err() != nil || r.fatal.Load() != nil {
			return components[:i], nil
		}
		begin := time.Now()
		err := invoke(ctx, component.Start)
		wasCanceled := err != nil && isCanceledStart(ctx, err)
		r.log.Info("component start finished", "component", component.name, "duration", time.Since(begin))
		if err != nil {
			if wasCanceled {
				r.log.Info("component start canceled", "component", component.name, "error", err)
				return components[:i], nil
			}
			r.logComponentFailure("component start failed", component.name, err)
			return components[:i], fmt.Errorf("start component %q: %w", component.name, err)
		}
	}

	<-ctx.Done()
	return components, nil
}

func isCanceledStart(ctx context.Context, err error) bool {
	if _, panicked := err.(*componentPanic); panicked {
		return false
	}
	ctxErr := ctx.Err()
	return ctxErr != nil && (errors.Is(err, ctxErr) || errors.Is(err, context.Cause(ctx)))
}

type namedComponent struct {
	Component
	name string
}

func nameComponents(comps []Component) ([]namedComponent, error) {
	names := make(map[string]struct{}, len(comps))
	components := make([]namedComponent, 0, len(comps))
	for _, component := range comps {
		name := component.Name()
		if _, exists := names[name]; exists {
			return nil, fmt.Errorf("duplicate component name %q", name)
		}
		names[name] = struct{}{}
		components = append(components, namedComponent{Component: component, name: name})
	}
	return components, nil
}

func (r *Runner) stop(ctx context.Context, started []namedComponent) {
	base := context.WithoutCancel(ctx)
	for i := len(started) - 1; i >= 0; i-- {
		r.stopComponent(base, started[i])
	}
}

func (r *Runner) stopComponent(base context.Context, component namedComponent) {
	begin := time.Now()
	ctx, cancel := context.WithTimeout(base, r.stopTimeout)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := invoke(ctx, component.Stop); err != nil {
			r.logComponentFailure("component stop failed", component.name, err)
		}
	}()
	select {
	case <-done:
	case <-ctx.Done():
		// A Stop that ignores cancellation can leave its goroutine behind.
		r.log.Error("component stop timed out", "component", component.name, "error", ctx.Err())
	}
	r.log.Info("component stop attempt finished", "component", component.name, "duration", time.Since(begin))
}

func (r *Runner) logComponentFailure(message, component string, err error) {
	fields := []any{"component", component, "error", err}
	if panicErr, ok := err.(*componentPanic); ok {
		fields = append(fields, "stack", string(panicErr.stack))
	}
	r.log.Error(message, fields...)
}

func invoke(ctx context.Context, operation func(context.Context) error) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = &componentPanic{value: value, stack: debug.Stack()}
		}
	}()
	return operation(ctx)
}

type componentPanic struct {
	value any
	stack []byte
}

func (e *componentPanic) Error() string { return fmt.Sprintf("panic: %v", e.value) }

func (e *componentPanic) Unwrap() error {
	err, _ := e.value.(error)
	return err
}
