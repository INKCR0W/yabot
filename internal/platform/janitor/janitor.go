package janitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/VictoriaMetrics/metrics"
)

const (
	FirstRunDelay = time.Minute
	ContinueDelay = 100 * time.Millisecond
)

// ErrBacklog signals that a successful task stopped with work potentially remaining.
var ErrBacklog = errors.New("janitor: backlog may remain")

type Deps struct {
	Log     *slog.Logger
	Metrics *metrics.Set
	Now     func() time.Time // Measures task durations. Defaults to time.Now and must be safe for concurrent calls.
}

// Janitor must be created with New and must not be copied.
type Janitor struct {
	deps Deps

	mu     sync.Mutex
	tasks  map[string]*task
	cancel context.CancelFunc
	done   chan struct{}
}

type task struct {
	name  string
	every time.Duration
	fn    func(context.Context) (int, error)

	runs     *metrics.Counter
	errors   *metrics.Counter
	rows     *metrics.Counter
	duration *metrics.Histogram
}

func New(deps Deps) *Janitor {
	if deps.Log == nil {
		deps.Log = slog.New(slog.DiscardHandler)
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	return &Janitor{deps: deps, tasks: make(map[string]*task)}
}

func (j *Janitor) Name() string { return "janitor" }

// Register panics after Start succeeds, or if name is empty or already registered,
// every is nonpositive, or fn is nil.
// An error with only ErrBacklog as its cause counts as success and schedules another run after ContinueDelay.
// The original cadence resumes at its next future tick once a run stops reporting backlog.
func (j *Janitor) Register(name string, every time.Duration, fn func(context.Context) (int, error)) {
	j.mu.Lock()
	defer j.mu.Unlock()
	switch {
	case j.done != nil:
		panic("janitor tasks must be registered before Start")
	case name == "":
		panic("janitor task name must not be empty")
	case j.tasks[name] != nil:
		panic(fmt.Sprintf("janitor task %q is already registered", name))
	case every <= 0:
		panic(fmt.Sprintf("janitor task %q interval must be positive", name))
	case fn == nil:
		panic(fmt.Sprintf("janitor task %q function must not be nil", name))
	}
	j.tasks[name] = &task{name: name, every: every, fn: fn}
}

// Start requires a metrics set. It succeeds at most once and runs tasks with a lifetime controlled by Stop.
func (j *Janitor) Start(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.done != nil {
		return errors.New("janitor has already started")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if j.deps.Metrics == nil {
		return errors.New("janitor metrics set must not be nil")
	}
	for _, task := range j.tasks {
		task.bindMetrics(j.deps.Metrics)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	taskCtx, cancel := context.WithCancel(context.Background())
	j.cancel = cancel
	done := make(chan struct{})
	j.done = done
	if len(j.tasks) == 0 {
		close(done)
		return nil
	}
	started := time.Now()
	var workers sync.WaitGroup
	for _, task := range j.tasks {
		workers.Go(func() { j.schedule(taskCtx, task, started) })
	}
	go func() {
		workers.Wait()
		close(done)
	}()
	return nil
}

// Stop cancels all tasks. If ctx ends before they exit, later calls can still wait for them.
func (j *Janitor) Stop(ctx context.Context) error {
	j.mu.Lock()
	cancel, done := j.cancel, j.done
	j.mu.Unlock()
	if done == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		select {
		case <-done:
			return nil
		default:
			return ctx.Err()
		}
	}
}

func (t *task) bindMetrics(set *metrics.Set) {
	escaped := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(t.name)
	label := `{task="` + escaped + `"}`
	t.runs = set.GetOrCreateCounter("yabot_janitor_runs_total" + label)
	t.errors = set.GetOrCreateCounter("yabot_janitor_errors_total" + label)
	t.rows = set.GetOrCreateCounter("yabot_janitor_rows_total" + label)
	t.duration = set.GetOrCreateHistogram("yabot_janitor_duration_seconds" + label)
}

func (j *Janitor) schedule(ctx context.Context, task *task, started time.Time) {
	next := started.Add(min(task.every, FirstRunDelay))
	timer := time.NewTimer(time.Until(next))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if ctx.Err() != nil {
			return
		}
		if j.run(ctx, task) {
			timer.Reset(ContinueDelay)
			continue
		}

		// Keep the original cadence while skipping periods that elapsed during the run.
		next = next.Add(task.every)
		if now := time.Now(); !next.After(now) {
			next = now.Add(task.every - now.Sub(next)%task.every)
		}
		timer.Reset(time.Until(next))
	}
}

func (j *Janitor) run(ctx context.Context, task *task) bool {
	started := j.deps.Now()
	task.runs.Inc()
	defer func() {
		task.duration.Update(max(0, j.deps.Now().Sub(started).Seconds()))
	}()

	completed := false
	defer func() {
		if !completed {
			value := recover()
			task.errors.Inc()
			j.deps.Log.Error("janitor task panicked", "task", task.name, "panic", value)
		}
	}()
	n, err := task.fn(ctx)
	completed = true
	if n > 0 {
		task.rows.Add(n)
	}
	hasBacklog := hasOnlyCause(err, ErrBacklog)
	if err == nil || hasBacklog {
		if n > 0 {
			j.deps.Log.Debug("janitor task processed rows", "task", task.name, "rows", n)
		}
		return hasBacklog
	}

	task.errors.Inc()
	switch {
	case hasOnlyCause(err, store.ErrBusy):
		j.deps.Log.Info("janitor task write queue is full", "task", task.name, "error", err)
	case !hasOnlyCause(err, ctx.Err()):
		j.deps.Log.Error("janitor task failed", "task", task.name, "error", err)
	}
	return false
}

func hasOnlyCause(err, target error) bool {
	if target == nil {
		return false
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		causes := wrapped.Unwrap()
		for _, cause := range causes {
			if !hasOnlyCause(cause, target) {
				return false
			}
		}
		return len(causes) > 0
	case interface{ Unwrap() error }:
		if cause := wrapped.Unwrap(); cause != nil {
			return hasOnlyCause(cause, target)
		}
	}
	return errors.Is(err, target)
}
