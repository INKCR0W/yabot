package store

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

func TestWriteAdmission(t *testing.T) {
	deps := newTestDeps(t)
	deps.WriteQueue = 1
	s := openTestStore(t, t.Context(), deps)
	op := func(*sql.Tx) error { return nil }
	var rejectedCallbacks atomic.Int64
	rejected := func(error) { rejectedCallbacks.Add(1) }
	if err := s.Write(op, rejected); !errors.Is(err, ErrClosed) {
		t.Fatalf("Write before Start = %v, want ErrClosed", err)
	}
	if err := s.WriteWait(t.Context(), op); !errors.Is(err, ErrClosed) {
		t.Fatalf("WriteWait before Start = %v, want ErrClosed", err)
	}
	startCtx, cancelStart := context.WithCancel(t.Context())
	defer cancelStart()
	if err := s.Start(startCtx); err != nil {
		t.Fatalf("start store: %v", err)
	}
	if err := s.Start(startCtx); err == nil {
		t.Fatal("second Start succeeded")
	}
	cancelStart()

	if err := s.Write(nil, rejected); err == nil {
		t.Error("Write accepted a nil op")
	}
	if err := s.Write(func(*sql.Tx) error {
		t.Error("Write executed an op with a nil done")
		return nil
	}, nil); err == nil {
		t.Error("Write accepted a nil done")
	}
	release, first := holdLifecycleWrite(t, s, op)
	queued := submitLifecycleWrite(t, s, op)
	ctx, cancel := context.WithTimeout(t.Context(), lifecycleWait)
	defer cancel()
	for _, call := range []struct {
		name string
		run  func() error
	}{
		{"Write", func() error { return s.Write(op, rejected) }},
		{"WriteWait", func() error { return s.WriteWait(ctx, op) }},
	} {
		result := make(chan error, 1)
		go func() { result <- call.run() }()
		if err := awaitLifecycleResult(t, result); !errors.Is(err, ErrBusy) {
			t.Errorf("%s with a full queue = %v, want ErrBusy", call.name, err)
		}
	}
	release()
	for _, result := range []<-chan error{first, queued} {
		if err := awaitLifecycleResult(t, result); err != nil {
			t.Errorf("accepted write failed after Start context cancellation: %v", err)
		}
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("stop store: %v", err)
	}
	if err := s.Write(op, rejected); !errors.Is(err, ErrClosed) {
		t.Errorf("Write after Stop = %v, want ErrClosed", err)
	}
	if err := s.WriteWait(ctx, op); !errors.Is(err, ErrClosed) {
		t.Errorf("WriteWait after Stop = %v, want ErrClosed", err)
	}
	if err := s.Start(ctx); err == nil {
		t.Error("Start after Stop succeeded")
	}
	if got := rejectedCallbacks.Load(); got != 0 {
		t.Errorf("rejected writes called done %d times", got)
	}
}

func TestConcurrentWrites(t *testing.T) {
	for _, test := range []struct {
		name       string
		perWorker  int
		stopDuring bool
	}{
		{"load", 50, false},
		{"stop during submissions", 32, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			const workers = 64
			total := workers * test.perWorker
			deps := newTestDeps(t)
			deps.WriteQueue = total
			s := startLifecycleStore(t, deps)
			var release func()
			var blockingDone <-chan error
			if test.stopDuring {
				release, blockingDone = holdLifecycleWrite(t, s, func(*sql.Tx) error { return nil })
			}

			type outcome struct {
				submitErr error
				doneErr   error
				calls     atomic.Int64
			}
			outcomes := make([]outcome, total)
			var ready, submitted sync.WaitGroup
			ready.Add(workers)
			submitted.Add(workers)
			start := make(chan struct{})
			continueSubmitting := make(chan struct{})
			stopWaiting := make(chan struct{})
			if !test.stopDuring {
				close(stopWaiting)
			}
			for worker := range workers {
				go func() {
					defer submitted.Done()
					<-start
					for i := range test.perWorker {
						if i == test.perWorker/2 {
							<-stopWaiting
						}
						id := worker*test.perWorker + i
						out := &outcomes[id]
						out.submitErr = s.Write(insertLifecycleItem(id), func(err error) {
							out.doneErr = err
							out.calls.Add(1)
						})
						if i == 0 {
							ready.Done()
							<-continueSubmitting
						}
					}
				}()
			}
			close(start)
			ready.Wait()
			ctx, cancel := context.WithTimeout(t.Context(), lifecycleWait)
			defer cancel()
			stopResult := make(chan error, 1)
			if test.stopDuring {
				go func() {
					<-continueSubmitting
					observed := lifecycleObservedContext{ctx, sync.OnceFunc(func() { close(stopWaiting) })}
					stopResult <- s.Stop(observed)
				}()
			}
			close(continueSubmitting)
			submitted.Wait()
			if test.stopDuring {
				release()
			} else {
				stopResult <- s.Stop(ctx)
			}
			if err := awaitLifecycleResult(t, stopResult); err != nil {
				t.Fatalf("stop store: %v", err)
			}
			if blockingDone != nil && completedLifecycleWrite(t, blockingDone) != nil {
				t.Fatal("blocking write failed")
			}

			wantRows := 0
			for id := range outcomes {
				out := &outcomes[id]
				calls := out.calls.Load()
				if out.submitErr == nil {
					if calls != 1 || out.doneErr != nil {
						t.Fatalf("accepted write %d called done %d times with %v", id, calls, out.doneErr)
					}
					wantRows++
					continue
				}
				if !test.stopDuring || !errors.Is(out.submitErr, ErrClosed) || calls != 0 {
					t.Fatalf("rejected write %d = %v, done calls = %d", id, out.submitErr, calls)
				}
			}
			if wantRows < workers {
				t.Fatalf("only %d writes accepted; want at least one per worker", wantRows)
			}
			if test.stopDuring && wantRows == total {
				t.Fatal("all writes were accepted despite Stop")
			}
			reopened := reopenLifecycleStore(t, deps)
			if rows := countLifecycleItems(t, reopened); rows != wantRows {
				t.Errorf("persisted %d rows, want %d rows with successful done", rows, wantRows)
			}
		})
	}
}

func TestStopDrainsAcceptedWrites(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{"waits for drain", nil},
		{"drain outlives expired deadline", context.DeadlineExceeded},
		{"drain outlives cancellation", context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			deps := newTestDeps(t)
			s := startLifecycleStore(t, deps)
			release, first := holdLifecycleWrite(t, s, insertLifecycleItem(1))
			results := []<-chan error{first,
				submitLifecycleWrite(t, s, insertLifecycleItem(2)),
				submitLifecycleWrite(t, s, insertLifecycleItem(3)),
			}
			var stopCtx context.Context
			var cancel context.CancelFunc
			if test.err == context.DeadlineExceeded {
				stopCtx, cancel = context.WithDeadline(t.Context(), time.Time{})
			} else {
				stopCtx, cancel = context.WithTimeout(t.Context(), lifecycleWait)
			}
			defer cancel()
			waiting := make(chan struct{})
			stopResult := make(chan error, 1)
			go func() {
				stopResult <- s.Stop(lifecycleObservedContext{stopCtx, sync.OnceFunc(func() { close(waiting) })})
			}()
			if test.err != context.DeadlineExceeded {
				awaitLifecycleResult(t, waiting)
			}
			if test.err == context.Canceled {
				cancel()
			}
			if test.err != nil {
				if err := awaitLifecycleResult(t, stopResult); !errors.Is(err, test.err) {
					t.Fatalf("Stop = %v, want %v", err, test.err)
				}
			}
			var rejectedCalls atomic.Int64
			if err := s.Write(insertLifecycleItem(4), func(error) { rejectedCalls.Add(1) }); !errors.Is(err, ErrClosed) {
				t.Fatalf("Write during Stop = %v, want ErrClosed", err)
			}
			for _, result := range results {
				select {
				case err := <-result:
					t.Fatalf("done called before the writer was released: %v", err)
				default:
				}
			}
			release()
			if test.err != nil {
				for _, result := range results {
					if err := awaitLifecycleResult(t, result); err != nil {
						t.Fatalf("drained write after timeout: %v", err)
					}
				}
				ctx, cancel := context.WithTimeout(t.Context(), lifecycleWait)
				defer cancel()
				if err := s.Stop(ctx); err != nil {
					t.Fatalf("wait for drained store: %v", err)
				}
			} else {
				if err := awaitLifecycleResult(t, stopResult); err != nil {
					t.Fatalf("stop store: %v", err)
				}
				for _, result := range results {
					if err := completedLifecycleWrite(t, result); err != nil {
						t.Fatalf("drained write: %v", err)
					}
				}
			}
			if got := rejectedCalls.Load(); got != 0 {
				t.Errorf("write rejected during Stop called done %d times", got)
			}
			reopened := reopenLifecycleStore(t, deps)
			if count := countLifecycleItems(t, reopened); count != len(results) {
				t.Errorf("persisted %d rows during Stop, want %d", count, len(results))
			}
		})
	}
}

func TestStopRetainsLockAndSharesCloseResult(t *testing.T) {
	for _, test := range []struct {
		name     string
		closeErr error
	}{
		{"success", nil},
		{"close failure", errors.New("injected pool close failure")},
	} {
		t.Run(test.name, func(t *testing.T) {
			deps := newTestDeps(t)
			s, err := Open(t.Context(), deps)
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			t.Cleanup(func() {
				if err := s.Stop(context.Background()); !errors.Is(err, test.closeErr) {
					t.Errorf("stop store: %v, want %v", err, test.closeErr)
				}
			})
			closingWriter := make(chan struct{})
			closeWriter := make(chan struct{})
			allowClose := sync.OnceFunc(func() { close(closeWriter) })
			t.Cleanup(allowClose)
			writerClosed := newBatchTestGate(t)
			observeWriterClosed := sync.OnceFunc(writerClosed.block)
			var writerCloses atomic.Int64
			s.hooks = &storeHooks{
				beforeClosePool: func(name string) error {
					if name == "writer" && writerCloses.Add(1) == 1 {
						close(closingWriter)
						<-closeWriter
						return test.closeErr
					}
					return nil
				},
				afterClosePool: func(name string) {
					if name == "writer" {
						observeWriterClosed()
					}
				},
			}
			const callers = 16
			ctx, cancel := context.WithTimeout(t.Context(), lifecycleWait)
			defer cancel()
			returned := make(chan error, callers)
			go func() { returned <- s.Stop(ctx) }()
			awaitLifecycleResult(t, closingWriter)

			waiting := make(chan struct{}, callers-1)
			for range callers - 1 {
				go func() {
					observed := lifecycleObservedContext{ctx, sync.OnceFunc(func() { waiting <- struct{}{} })}
					returned <- s.Stop(observed)
				}()
			}
			for range callers - 1 {
				awaitLifecycleResult(t, waiting)
			}
			var value int
			if err := s.write.QueryRowContext(ctx, "SELECT 1").Scan(&value); err != nil {
				t.Fatalf("writer closed before hook release: %v", err)
			}
			for _, pool := range []*sql.DB{s.Read(), s.checkpoint} {
				if err := pool.QueryRowContext(ctx, "SELECT 1").Scan(&value); err == nil {
					t.Fatal("reader or checkpoint pool remained open at writer close")
				}
			}
			if err := failTestOpen(t, ctx, freshLifecycleDeps(deps)); !errors.Is(err, lockConflict) {
				t.Fatalf("Open during pool closure = %v, want lock conflict", err)
			}
			select {
			case err := <-returned:
				t.Fatalf("Stop returned before pool closure: %v", err)
			default:
			}

			allowClose()
			awaitLifecycleResult(t, writerClosed.entered)
			if err := s.write.QueryRowContext(t.Context(), "SELECT 1").Scan(&value); err == nil {
				t.Fatal("writer accepted a query after pool Close")
			}
			if err := failTestOpen(t, ctx, freshLifecycleDeps(deps)); !errors.Is(err, lockConflict) {
				t.Fatalf("Open after writer pool Close = %v, want lock conflict", err)
			}
			writerClosed.release()

			first := awaitLifecycleResult(t, returned)
			if !errors.Is(first, test.closeErr) {
				t.Fatalf("Stop = %v, want %v", first, test.closeErr)
			}
			for range callers - 1 {
				if err := awaitLifecycleResult(t, returned); err != first {
					t.Errorf("concurrent Stop = %v, want shared result %v", err, first)
				}
			}
			if got := writerCloses.Load(); got != 1 {
				t.Errorf("writer pool close attempted %d times, want 1", got)
			}
			reopenLifecycleStore(t, deps)
		})
	}
}

func TestWriteWaitResults(t *testing.T) {
	writeErr := errors.New("business rule rejected write")
	for _, test := range []struct {
		name string
		err  error
	}{
		{"success", nil},
		{"op error", writeErr},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := startLifecycleStore(t, newTestDeps(t))
			ctx, cancel := context.WithTimeout(t.Context(), lifecycleWait)
			defer cancel()
			err := s.WriteWait(ctx, func(tx *sql.Tx) error {
				if err := insertLifecycleItem(1)(tx); err != nil {
					return err
				}
				return test.err
			})
			var opErr *OpError
			if !errors.Is(err, test.err) || errors.As(err, &opErr) != (test.err != nil) {
				t.Fatalf("WriteWait = %v, want result wrapping %v", err, test.err)
			}
			want := 1
			if test.err != nil {
				want = 0
			}
			if count := countLifecycleItems(t, s); count != want {
				t.Errorf("persisted %d rows, want %d", count, want)
			}
		})
	}

	t.Run("cancellation only ends the wait", func(t *testing.T) {
		s := startLifecycleStore(t, newTestDeps(t))
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		entered := make(chan struct{})
		resume := make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		t.Cleanup(release)
		result := make(chan error, 1)
		go func() {
			result <- s.WriteWait(ctx, func(tx *sql.Tx) error {
				close(entered)
				<-resume
				return insertLifecycleItem(1)(tx)
			})
		}()
		awaitLifecycleResult(t, entered)
		cancel()
		if err := awaitLifecycleResult(t, result); !errors.Is(err, context.Canceled) {
			t.Fatalf("WriteWait after cancellation = %v, want context.Canceled", err)
		}
		if count := countLifecycleItems(t, s); count != 0 {
			t.Fatalf("persisted %d rows before the op was released", count)
		}
		release()
		if err := awaitLifecycleResult(t, submitLifecycleWrite(t, s, func(*sql.Tx) error { return nil })); err != nil {
			t.Fatalf("wait for subsequent write: %v", err)
		}
		if count := countLifecycleItems(t, s); count != 1 {
			t.Errorf("persisted %d rows after release, want 1", count)
		}
	})
}

const lifecycleWait = 10 * time.Second

type lifecycleObservedContext struct {
	context.Context
	observe func()
}

func (ctx lifecycleObservedContext) Done() <-chan struct{} {
	ctx.observe()
	return ctx.Context.Done()
}

func startLifecycleStore(t *testing.T, deps Deps) *Store {
	t.Helper()
	s := openTestStore(t, t.Context(), deps)
	if err := s.Start(t.Context()); err != nil {
		t.Fatalf("start store: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), lifecycleWait)
	defer cancel()
	if err := s.WriteWait(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec("CREATE TABLE lifecycle_items (id INTEGER PRIMARY KEY)")
		return err
	}); err != nil {
		t.Fatalf("create lifecycle table: %v", err)
	}
	return s
}

func holdLifecycleWrite(t *testing.T, s *Store, op Op) (func(), <-chan error) {
	t.Helper()
	entered := make(chan struct{})
	resume := make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	t.Cleanup(release)
	result := submitLifecycleWrite(t, s, func(tx *sql.Tx) error {
		close(entered)
		<-resume
		return op(tx)
	})
	awaitLifecycleResult(t, entered)
	return release, result
}

func submitLifecycleWrite(t *testing.T, s *Store, op Op) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	var calls atomic.Int64
	if err := s.Write(op, func(err error) {
		if calls.Add(1) != 1 {
			t.Error("write called done more than once")
			return
		}
		result <- err
	}); err != nil {
		t.Fatalf("submit write: %v", err)
	}
	return result
}

func awaitLifecycleResult[T any](t *testing.T, result <-chan T) T {
	t.Helper()
	timer := time.NewTimer(lifecycleWait)
	defer timer.Stop()
	select {
	case got := <-result:
		return got
	case <-timer.C:
		t.Fatal("timed out waiting for store")
		var zero T
		return zero
	}
}

func completedLifecycleWrite(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	default:
		t.Fatal("Stop returned before calling done")
		return nil
	}
}

func insertLifecycleItem(id int) Op {
	return func(tx *sql.Tx) error {
		_, err := tx.Exec("INSERT INTO lifecycle_items (id) VALUES (?)", id)
		return err
	}
}

func countLifecycleItems(t *testing.T, s *Store) int {
	t.Helper()
	var count int
	if err := s.Read().QueryRowContext(t.Context(), "SELECT COUNT(*) FROM lifecycle_items").Scan(&count); err != nil {
		t.Fatalf("count persisted rows: %v", err)
	}
	return count
}

func freshLifecycleDeps(deps Deps) Deps {
	deps.Health = &testHealthRegistry{}
	deps.Metrics = metrics.NewSet()
	return deps
}

func reopenLifecycleStore(t *testing.T, deps Deps) *Store {
	t.Helper()
	return openTestStore(t, t.Context(), freshLifecycleDeps(deps))
}
