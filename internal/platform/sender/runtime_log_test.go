package sender

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-telegram/bot"
)

func newSenderWithLockCheckingLogger(t *testing.T) (*Sender, *bytes.Buffer) {
	t.Helper()
	var logs bytes.Buffer
	var s *Sender
	deps := workerRuntimeDeps(t)
	deps.Log = slog.New(slog.NewJSONHandler(senderTestLogWriter(func(p []byte) (int, error) {
		if s.mu.TryLock() {
			s.mu.Unlock()
		} else {
			t.Errorf("sender log writer ran while the sender mutex was held: %s", bytes.TrimSpace(p))
		}
		return logs.Write(p)
	}), nil))
	var err error
	s, err = New(deps)
	if err != nil {
		t.Fatal(err)
	}
	return s, &logs
}

func TestSenderWorkerLogsOutsideLock(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		panicDone bool
		noRetry   bool
		wantLog   string
	}{
		{"retry", context.DeadlineExceeded, false, false, "sender request retrying"},
		{"fatal", errors.New("unexpected response"), false, false, "sender request failed"},
		{"callback panic", nil, true, false, "sender Done callback panicked"},
		{"no retry", context.DeadlineExceeded, false, true, "sender request completed with retryable outcome"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, logs := newSenderWithLockCheckingLogger(t)
				now := time.Now()
				// Dispatch one worker without starting the scheduler, which could race the writer for the mutex.
				s.state = senderRunning
				s.base = t.Context()
				s.global = newTokenBucket(s.deps.GlobalRPS, s.deps.GlobalBurst, now)
				r := workerRuntimeRequest(Decision, 42, func(context.Context, *bot.Bot) error {
					return tc.err
				}, func(Result) {
					if tc.panicDone {
						panic("callback failure")
					}
				})
				r.NoRetry = tc.noRetry
				workerRuntimeEnqueue(t, s, r)
				s.mu.Lock()
				_, dispatched := s.dispatch(now)
				s.mu.Unlock()
				if !dispatched {
					t.Fatal("request was not dispatched")
				}
				synctest.Wait()
				if got := logs.String(); strings.Count(got, "\n") != 1 || !strings.Contains(got, tc.wantLog) {
					t.Errorf("logs = %q, want one %q record", got, tc.wantLog)
				}
			})
		})
	}
}

func TestSenderGlobalPauseLogsOutsideLock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, logs := newSenderWithLockCheckingLogger(t)
		const pause = 31 * time.Second
		// Apply the 429 before starting the scheduler so no worker can contend for the lock.
		s.record429(time.Now(), 42, methodOther, pause)
		if err := s.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer workerRuntimeStop(t, s)

		time.Sleep(pause)
		synctest.Wait()
		got := logs.String()
		const entered = "sender entered global pause"
		const left = "sender left global pause"
		if strings.Count(got, "\n") != 2 || strings.Count(got, entered) != 1 || strings.Count(got, left) != 1 ||
			strings.Index(got, entered) > strings.Index(got, left) {
			t.Errorf("logs = %q, want one pause entry followed by one exit", got)
		}
	})
}

func TestSenderSchedulerLogsCallbackPanicOutsideLock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, logs := newSenderWithLockCheckingLogger(t)
		if err := s.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer workerRuntimeStop(t, s)

		r := workerRuntimeRequest(Decision, 42, func(context.Context, *bot.Bot) error {
			t.Error("expired request reached a worker")
			return nil
		}, func(result Result) {
			if result.Dropped != Expired {
				t.Errorf("result = %+v, want Expired", result)
			}
			panic("callback failure")
		})
		r.Deadline = time.Now()
		workerRuntimeEnqueue(t, s, r)
		synctest.Wait()
		if got := logs.String(); strings.Count(got, "\n") != 1 || !strings.Contains(got, "sender Done callback panicked") {
			t.Errorf("logs = %q, want one callback panic record", got)
		}
	})
}
