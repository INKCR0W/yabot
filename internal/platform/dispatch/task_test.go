package dispatch

import (
	"context"
	"log/slog"
	"os"
	"runtime"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot/models"
)

func TestSettleOnPanic(t *testing.T) {
	for _, tc := range []struct {
		name         string
		run          func()
		legacyNil    bool
		wantReturn   bool
		wantSurvival bool
		wantPanic    any
	}{
		{name: "returns", run: func() {}, wantReturn: true, wantSurvival: true},
		{name: "panic value", run: func() { panic("task failed") }, wantPanic: "task failed", wantSurvival: true},
		{name: "legacy panic nil", run: func() { panic(nil) }, legacyNil: true, wantSurvival: true},
		{name: "Goexit", run: runtime.Goexit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			panicnil := "0"
			if tc.legacyNil {
				panicnil = "1"
			}
			t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",panicnil="+panicnil)
			d, err := New(testDeps())
			if err != nil {
				t.Fatal(err)
			}

			type outcome struct {
				calls      int
				settled    []error
				returned   bool
				survived   bool
				panicValue any
			}
			done := make(chan outcome, 1)
			go func() {
				var result outcome
				defer func() { done <- result }()
				task := SettleOnPanic(func(context.Context, *Shard) {
					result.calls++
					tc.run()
				}, func(err error) {
					result.settled = append(result.settled, err)
				})
				func() {
					defer func() { result.panicValue = recover() }()
					task(t.Context(), d.shards[0])
					result.returned = true
				}()
				result.survived = true
			}()

			got := await(t, done)
			if got.calls != 1 || got.returned != tc.wantReturn || got.survived != tc.wantSurvival || got.panicValue != tc.wantPanic {
				t.Errorf("task calls = %d, returned = %t, goroutine survived = %t, recovered = %v, want 1, %t, %t, %v",
					got.calls, got.returned, got.survived, got.panicValue, tc.wantReturn, tc.wantSurvival, tc.wantPanic)
			}
			if tc.wantReturn {
				if len(got.settled) != 0 {
					t.Errorf("settled on normal return: %v", got.settled)
				}
				return
			}
			if len(got.settled) != 1 {
				t.Fatalf("settled with %v, want exactly one error", got.settled)
			}
			if !IsPermanent(got.settled[0]) {
				t.Errorf("settled with %v, want a permanent error", got.settled[0])
			}
			assertWorkPanicError(t, got.settled[0], tc.wantPanic)
		})
	}
}

func TestSettleOnPanicReportsBothPanics(t *testing.T) {
	logs := &workPanicLog{Handler: slog.DiscardHandler}
	deps := testDeps()
	deps.Log = slog.New(logs)
	deps.Metrics = metrics.NewSet()
	d := startTestDispatcher(t, deps)
	task := SettleOnPanic(func(context.Context, *Shard) {
		panic("original panic")
	}, func(error) {
		panic("settle panic")
	})
	if err := d.Submit(Key{}, task); err != nil {
		t.Fatal(err)
	}
	waitForShardTask(t, d)
	if len(logs.errors) != 2 {
		t.Fatalf("logged panic errors = %v, want both panics", logs.errors)
	}
	assertWorkPanicError(t, logs.errors[0], "settle panic")
	assertWorkPanicError(t, logs.errors[1], "original panic")
	assertDispatchMetric(t, deps.Metrics, `yabot_dispatch_panics_total{route="none"}`, 2)
}

func TestRouterCompletionPanicSettlesCriticalAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := testDeps()
		deps.Metrics = metrics.NewSet()
		d := startTestDispatcher(t, deps)
		r := NewRouter(d, RouterDeps{})
		r.Register(routerTestRoute("critical", true, func(_ context.Context, sh *Shard, _ *models.Update, ack Ack) {
			ticket, ok := sh.Admit()
			if !ok {
				ack.Done(ErrBusy)
				return
			}
			go func() {
				ticket.Complete(SettleOnPanic(func(context.Context, *Shard) {
					panic("commit callback failed")
				}, ack.Done))
			}()
		}))

		rc := r.Accept(&models.Update{ID: 48, Message: &models.Message{}})
		err := rc.Wait(time.Second)
		if !IsPermanent(err) {
			t.Errorf("Wait = %v, want permanent error", err)
		}
		assertWorkPanicError(t, err, "commit callback failed")
		waitForShardTask(t, d)
		assertDispatchMetric(t, deps.Metrics, `yabot_dispatch_panics_total{route="none"}`, 1)
	})
}
