package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"
	"weak"

	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot/models"
)

func TestRouterFanOut(t *testing.T) {
	transient := errors.New("write queue full")
	permanent := errors.New("invalid update")
	otherPermanent := &PanicError{Value: "operation panicked"}
	for _, tc := range []struct {
		name          string
		first, second error
		timeout       bool
		wantPermanent bool
		causes        []error
	}{
		{name: "success"},
		{name: "mixed", first: transient, second: Permanent(permanent), causes: []error{transient, permanent}},
		{name: "permanent", first: Permanent(permanent), second: otherPermanent, wantPermanent: true, causes: []error{permanent, otherPermanent}},
		{name: "timeout", first: Permanent(permanent), timeout: true, causes: []error{ErrSettleTimeout}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d := startTestDispatcher(t, testDeps())
				r := NewRouter(d, RouterDeps{})
				u := &models.Update{ID: 42, Message: &models.Message{}}
				handled := make(chan string, 4)
				firstDone := make(chan struct{})
				for _, name := range []string{"first", "ignored", "optional", "second"} {
					rt := routerTestRoute(name, name != "optional", func(_ context.Context, _ *Shard, got *models.Update, ack Ack) {
						if got != u {
							t.Errorf("%s got update %p, want original %p", name, got, u)
						}
						switch name {
						case "first":
							ack.Done(tc.first)
							close(firstDone)
						case "second":
							if !tc.timeout {
								ack.Done(tc.second)
							}
						}
						handled <- name
					})
					switch name {
					case "ignored":
						rt.Match = func(*models.Update) bool { return false }
						rt.Key = func(*models.Update) Key {
							t.Error("Key ran for a route rejected by Match")
							return Key{}
						}
					case "second":
						rt.Match = func(*models.Update) bool {
							await(t, firstDone)
							return true
						}
					}
					r.Register(rt)
				}
				rc := r.Accept(u)
				waitForShardTask(t, d)
				close(handled)
				var order []string
				for name := range handled {
					order = append(order, name)
				}
				if want := []string{"first", "optional", "second"}; !slices.Equal(order, want) {
					t.Errorf("handler order = %v, want %v", order, want)
				}
				const deadline = time.Second
				start := time.Now()
				err := rc.Wait(deadline)
				var wantElapsed time.Duration
				if tc.timeout {
					wantElapsed = deadline
				}
				if elapsed := time.Since(start); elapsed != wantElapsed {
					t.Errorf("Wait returned after %s, want %s", elapsed, wantElapsed)
				}
				if len(tc.causes) == 0 && err != nil {
					t.Errorf("Wait = %v, want nil", err)
				}
				for _, cause := range tc.causes {
					if !errors.Is(err, cause) {
						t.Errorf("Wait = %v, want cause %v", err, cause)
					}
				}
				if got := IsPermanent(err); got != tc.wantPermanent {
					t.Errorf("IsPermanent(%v) = %t, want %t", err, got, tc.wantPermanent)
				}
				if tc.first != nil && !tc.timeout {
					assertRouterErrorRoute(t, err, "first")
				}
				if tc.second != nil || tc.timeout {
					assertRouterErrorRoute(t, err, "second")
				}
			})
		})
	}
}

func TestRouterFullInbox(t *testing.T) {
	var logs bytes.Buffer
	deps := testDeps()
	deps.Inbox = 1
	set := metrics.NewSet()
	d := startTestDispatcher(t, deps)
	unblock := holdShardForTicketTest(t, d)
	r := NewRouter(d, RouterDeps{Log: slog.New(slog.NewJSONHandler(&logs, nil)), Metrics: set})
	ran := make(chan int64, 2)
	optional := routerTestRoute("optional", false, func(_ context.Context, _ *Shard, u *models.Update, ack Ack) {
		ack.Done(errors.New("optional failure"))
		ack.Done(nil)
		ran <- u.ID
	})
	r.Register(optional)
	critical := routerTestRoute("critical", true, func(context.Context, *Shard, *models.Update, Ack) {
		t.Error("rejected critical route ran")
	})
	critical.Kinds = KindEditedMessage.Mask()
	r.Register(critical)

	for _, id := range []int64{1, 2} {
		rc := r.Accept(&models.Update{ID: id, Message: &models.Message{}})
		if err := rc.Wait(0); err != nil {
			t.Errorf("noncritical Wait before handlers ran for update %d = %v, want nil", id, err)
		}
	}
	select {
	case <-ran:
		t.Error("optional handler ran while the shard was blocked")
	default:
	}
	assertDispatchMetric(t, set, `yabot_dispatch_route_dropped_total{route="optional"}`, 1)
	rc := r.Accept(&models.Update{ID: 3, EditedMessage: &models.Message{}})
	err := rc.Wait(0)
	if err == ErrBusy || !errors.Is(err, ErrBusy) || IsPermanent(err) {
		t.Errorf("critical Wait with full inbox = %v, want wrapped transient ErrBusy", err)
	}
	assertRouterErrorRoute(t, err, "critical")
	assertRouterDropLog(t, &logs, "optional", 2, ErrBusy)

	unblock()
	if id := await(t, ran); id != 1 {
		t.Errorf("handled update %d, want 1", id)
	}
	waitForShardTask(t, d)
	select {
	case id := <-ran:
		t.Errorf("dropped update %d ran", id)
	default:
	}
	if got := set.GetOrCreateCounter(`yabot_dispatch_ack_duplicate_total{route="optional"}`).Get(); got != 0 {
		t.Errorf("noncritical duplicate Ack count = %d, want 0", got)
	}
}

func TestRouterClosedDispatcher(t *testing.T) {
	var logs bytes.Buffer
	d, err := New(testDeps())
	if err != nil {
		t.Fatal(err)
	}
	r := NewRouter(d, RouterDeps{Log: slog.New(slog.NewJSONHandler(&logs, nil))})
	r.Register(routerTestRoute("closed", true, func(context.Context, *Shard, *models.Update, Ack) {
		t.Error("rejected route ran")
	}))
	optional := routerTestRoute("optional", false, func(context.Context, *Shard, *models.Update, Ack) {
		t.Error("rejected noncritical route ran")
	})
	optional.Kinds = KindEditedMessage.Mask()
	r.Register(optional)
	rc := r.Accept(&models.Update{Message: &models.Message{}})
	err = rc.Wait(0)
	if err == ErrClosed || !errors.Is(err, ErrClosed) || IsPermanent(err) {
		t.Errorf("Wait on closed dispatcher = %v, want wrapped transient ErrClosed", err)
	}
	assertRouterErrorRoute(t, err, "closed")
	rc = r.Accept(&models.Update{ID: 4, EditedMessage: &models.Message{}})
	if err := rc.Wait(0); err != nil {
		t.Errorf("noncritical Wait on closed dispatcher = %v, want nil", err)
	}
	assertRouterDropLog(t, &logs, "optional", 4, ErrClosed)
}

func TestRouterHandlePanic(t *testing.T) {
	for _, tc := range []struct {
		name              string
		callsDone         bool
		dispatcherMetrics bool
	}{
		{name: "before Ack", dispatcherMetrics: true},
		{name: "after Ack", callsDone: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var dispatchLogs, routerLogs bytes.Buffer
			routerMetrics := metrics.NewSet()
			deps := testDeps()
			deps.Log = slog.New(slog.NewJSONHandler(&dispatchLogs, nil))
			if tc.dispatcherMetrics {
				deps.Metrics = metrics.NewSet()
			}
			d := startTestDispatcher(t, deps)
			r := NewRouter(d, RouterDeps{Log: slog.New(slog.NewJSONHandler(&routerLogs, nil)), Metrics: routerMetrics})
			key := Key{Space: 6, A: 12, B: -5}
			rt := routerTestRoute("handler", true, func(_ context.Context, _ *Shard, _ *models.Update, ack Ack) {
				if tc.callsDone {
					ack.Done(nil)
				}
				panic("handler failed")
			})
			rt.Key = func(*models.Update) Key { return key }
			r.Register(rt)
			if deps.Metrics != nil {
				assertDispatchMetric(t, deps.Metrics, `yabot_dispatch_panics_total{route="handler"}`, 0)
			}
			rc := r.Accept(&models.Update{ID: 21, Message: &models.Message{}})
			waitForShardTask(t, d)
			err := rc.Wait(0)
			if tc.callsDone {
				if err != nil {
					t.Errorf("Wait after Ack then panic = %v, want nil", err)
				}
				assertDispatchMetric(t, routerMetrics, `yabot_dispatch_ack_duplicate_total{route="handler"}`, 0)
			} else {
				var pe *PanicError
				if !IsPermanent(err) || !errors.As(err, &pe) || pe.Value != "handler failed" || len(pe.Stack) == 0 {
					t.Errorf("Wait after panic = %v, want permanent *PanicError with stack", err)
				}
			}
			if deps.Metrics != nil {
				assertDispatchMetric(t, deps.Metrics, `yabot_dispatch_panics_total{route="handler"}`, 1)
			}
			assertNoRouterPanicReports(t, routerMetrics, &routerLogs)
			record := readRouterLog(t, &dispatchLogs)
			if record.Level != "ERROR" || record.Key != key || record.Route != "handler" || !strings.Contains(record.Stack, "TestRouterHandlePanic") {
				t.Errorf("unexpected handler panic log: %s", dispatchLogs.String())
			}
		})
	}
}

func TestRouterPredicatePanic(t *testing.T) {
	for _, tc := range []struct {
		hook     string
		critical bool
	}{
		{hook: "key", critical: true},
		{hook: "match"},
	} {
		t.Run(tc.hook, func(t *testing.T) {
			var dispatchLogs, routerLogs bytes.Buffer
			routerMetrics := metrics.NewSet()
			deps := testDeps()
			deps.Log = slog.New(slog.NewJSONHandler(&dispatchLogs, nil))
			deps.Metrics = metrics.NewSet()
			d := startTestDispatcher(t, deps)
			r := NewRouter(d, RouterDeps{Log: slog.New(slog.NewJSONHandler(&routerLogs, nil)), Metrics: routerMetrics})
			rt := routerTestRoute(tc.hook, tc.critical, func(context.Context, *Shard, *models.Update, Ack) {
				t.Error("handle ran despite a routing panic")
			})
			if tc.hook == "key" {
				rt.Key = func(*models.Update) Key { panic("key failed") }
			} else {
				rt.Match = func(*models.Update) bool { panic("match failed") }
				rt.Key = func(*models.Update) Key {
					t.Error("Key ran after Match panicked")
					return Key{}
				}
			}
			r.Register(rt)
			ran := make(chan struct{})
			r.Register(routerTestRoute("healthy", false, func(context.Context, *Shard, *models.Update, Ack) { close(ran) }))
			rc := r.Accept(&models.Update{ID: 31, Message: &models.Message{}})
			err := rc.Wait(0)
			if tc.critical {
				var pe *PanicError
				if !IsPermanent(err) || !errors.As(err, &pe) || pe.Value != "key failed" || len(pe.Stack) == 0 {
					t.Errorf("Wait after Key panic = %v, want permanent *PanicError with stack", err)
				}
				assertRouterErrorRoute(t, err, tc.hook)
			} else {
				if err != nil {
					t.Errorf("Wait after optional Match panic = %v, want nil", err)
				}
				assertDispatchMetric(t, routerMetrics, fmt.Sprintf(`yabot_dispatch_route_dropped_total{route=%q}`, tc.hook), 1)
			}
			await(t, ran)
			assertDispatchMetric(t, deps.Metrics, fmt.Sprintf(`yabot_dispatch_panics_total{route=%q}`, tc.hook), 1)
			assertNoRouterPanicReports(t, routerMetrics, &routerLogs)
			record := readRouterLog(t, &dispatchLogs)
			if record.Level != "ERROR" || record.Route != tc.hook || record.UpdateID != 31 || !strings.Contains(record.Stack, "TestRouterPredicatePanic") {
				t.Errorf("unexpected routing panic log: %s", dispatchLogs.String())
			}
		})
	}
}

func TestRouterAckFromAnotherGoroutine(t *testing.T) {
	var logs bytes.Buffer
	set := metrics.NewSet()
	d := startTestDispatcher(t, testDeps())
	r := NewRouter(d, RouterDeps{Log: slog.New(slog.NewJSONHandler(&logs, nil)), Metrics: set})
	acks := make(chan Ack, 1)
	r.Register(routerTestRoute("async", true, func(_ context.Context, _ *Shard, _ *models.Update, ack Ack) {
		acks <- ack
	}))
	rc := r.Accept(&models.Update{ID: 51, Message: &models.Message{}})
	ack := await(t, acks)
	first := errors.New("async failure")
	second := errors.New("repeated result")
	done := make(chan struct{})
	go func() {
		ack.Done(first)
		ack.Done(Permanent(second))
		close(done)
	}()
	await(t, done)
	for range 2 {
		err := rc.Wait(0)
		if !errors.Is(err, first) || errors.Is(err, second) || IsPermanent(err) {
			t.Errorf("Wait = %v, want only the first Ack error", err)
		}
	}
	assertDispatchMetric(t, set, `yabot_dispatch_ack_duplicate_total{route="async"}`, 1)
	record := readRouterLog(t, &logs)
	if record.Level != "WARN" || record.Route != "async" || record.UpdateID != 51 {
		t.Errorf("unexpected duplicate Ack log: %s", logs.String())
	}
}

func TestRouterDuplicateAckWhileAnotherRouteIsPending(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var logs bytes.Buffer
		set := metrics.NewSet()
		d := startTestDispatcher(t, testDeps())
		r := NewRouter(d, RouterDeps{Log: slog.New(slog.NewJSONHandler(&logs, nil)), Metrics: set})
		first, second := make(chan Ack, 1), make(chan Ack, 1)
		r.Register(routerTestRoute("first", true, func(_ context.Context, _ *Shard, _ *models.Update, ack Ack) {
			ack.Done(nil)
			first <- ack
		}))
		r.Register(routerTestRoute("second", true, func(_ context.Context, _ *Shard, _ *models.Update, ack Ack) {
			second <- ack
		}))
		rc := r.Accept(&models.Update{ID: 52, Message: &models.Message{}})
		firstAck, secondAck := await(t, first), await(t, second)
		settled := make(chan error, 1)
		go func() { settled <- rc.Wait(time.Second) }()
		firstAck.Done(errors.New("repeated result"))
		synctest.Wait()
		select {
		case err := <-settled:
			t.Fatalf("Receipt settled with %v before the second route called Done", err)
		default:
		}
		secondAck.Done(nil)
		if err := await(t, settled); err != nil {
			t.Errorf("Wait after both routes completed = %v, want nil", err)
		}
		assertDispatchMetric(t, set, `yabot_dispatch_ack_duplicate_total{route="first"}`, 1)
		record := readRouterLog(t, &logs)
		if record.Level != "WARN" || record.Route != "first" || record.UpdateID != 52 {
			t.Errorf("unexpected duplicate Ack log: %s", logs.String())
		}
	})
}

func TestRouterReceiptTimeout(t *testing.T) {
	for _, timeout := range []time.Duration{-time.Nanosecond, 0, 20 * time.Millisecond} {
		t.Run(timeout.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var logs bytes.Buffer
				set := metrics.NewSet()
				d := startTestDispatcher(t, testDeps())
				r := NewRouter(d, RouterDeps{Log: slog.New(slog.NewJSONHandler(&logs, nil)), Metrics: set})
				firstErr := errors.New("invalid update")
				r.Register(routerTestRoute("completed", true, func(_ context.Context, _ *Shard, _ *models.Update, ack Ack) {
					ack.Done(Permanent(firstErr))
				}))
				acks := make(chan Ack, 1)
				r.Register(routerTestRoute("slow", true, func(_ context.Context, _ *Shard, _ *models.Update, ack Ack) {
					acks <- ack
				}))
				rc := r.Accept(&models.Update{Message: &models.Message{}})
				ack := await(t, acks)
				start := time.Now()
				timedOut := rc.Wait(timeout)
				if elapsed := time.Since(start); elapsed != max(0, timeout) {
					t.Errorf("Wait returned after %s for timeout %s", elapsed, timeout)
				}
				if timedOut == ErrSettleTimeout || !errors.Is(timedOut, ErrSettleTimeout) || IsPermanent(timedOut) {
					t.Fatalf("Wait = %v, want wrapped transient ErrSettleTimeout", timedOut)
				}
				if !errors.Is(timedOut, firstErr) {
					t.Errorf("Wait = %v, want the completed route's error as well", timedOut)
				}
				assertRouterErrorRoute(t, timedOut, "slow")
				late := errors.New("late failure")
				ack.Done(Permanent(late))
				ack.Done(nil)
				if err := rc.Wait(time.Second); err != timedOut {
					t.Errorf("Wait after late Ack = %v, want original %v", err, timedOut)
				}
				assertDispatchMetric(t, set, `yabot_dispatch_ack_duplicate_total{route="slow"}`, 0)
				if logs.Len() != 0 {
					t.Errorf("late Ack calls produced logs: %s", logs.String())
				}
			})
		})
	}
	if err := (Receipt{}).Wait(0); err != nil {
		t.Errorf("zero Receipt.Wait = %v, want nil", err)
	}
}

func TestRouterReceiptReleasesAcksAfterSettlement(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "completed"},
		{name: "timed out", err: ErrSettleTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := startTestDispatcher(t, testDeps())
			r := NewRouter(d, RouterDeps{})
			acks := make(chan weak.Pointer[routeAck], 1)
			r.Register(routerTestRoute("route", true, func(_ context.Context, _ *Shard, _ *models.Update, ack Ack) {
				if tc.err == nil {
					ack.Done(nil)
				}
				acks <- weak.Make(ack.(*routeAck))
			}))
			rc := r.Accept(&models.Update{Message: &models.Message{}})
			ack := await(t, acks)
			waitForShardTask(t, d)
			if err := rc.Wait(0); !errors.Is(err, tc.err) {
				t.Fatalf("Wait = %v, want %v", err, tc.err)
			}
			runtime.GC()
			if ack.Value() != nil {
				t.Error("settled Receipt retained its Ack")
			}
			runtime.KeepAlive(rc)
		})
	}
}

func TestRouterMailboxAck(t *testing.T) {
	for _, name := range []string{"run panic", "drop"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var dispatchLogs, routerLogs bytes.Buffer
				routerMetrics := metrics.NewSet()
				deps := testDeps()
				deps.Log = slog.New(slog.NewJSONHandler(&dispatchLogs, nil))
				deps.Metrics = metrics.NewSet()
				d := startTestDispatcher(t, deps)
				r := NewRouter(d, RouterDeps{Log: slog.New(slog.NewJSONHandler(&routerLogs, nil)), Metrics: routerMetrics})
				key := Key{Space: 8, A: 4, B: 2}
				dropReason := errors.New("entity removed")
				aborted := make(chan error, 1)
				deferred := make(chan struct{})
				rt := routerTestRoute("deferred", true, func(_ context.Context, sh *Shard, _ *models.Update, ack Ack) {
					sh.Hold(key)
					if !sh.Defer(key, Item{
						Name: "deferred",
						Run:  func(context.Context, *Shard) { panic("mailbox failed") },
						Abort: func(err error) {
							aborted <- err
							if err == dropReason {
								ack.Done(nil)
							} else {
								ack.Done(err)
							}
						},
					}) {
						t.Error("Defer rejected an empty mailbox")
						ack.Done(ErrBusy)
					}
					close(deferred)
				})
				rt.Key = func(*models.Update) Key { return key }
				r.Register(rt)
				rc := r.Accept(&models.Update{Message: &models.Message{}})
				await(t, deferred)
				settled := make(chan error, 1)
				go func() { settled <- rc.Wait(time.Second) }()
				synctest.Wait()
				select {
				case err := <-settled:
					t.Fatalf("Receipt settled with %v while mailbox was held", err)
				default:
				}
				if err := d.Submit(key, func(_ context.Context, sh *Shard) {
					if name == "drop" {
						sh.Drop(key, dropReason)
					} else {
						sh.Release(key)
					}
				}); err != nil {
					t.Fatal(err)
				}
				abortErr, err := await(t, aborted), await(t, settled)
				if name == "drop" {
					if abortErr != dropReason || err != nil {
						t.Errorf("Drop called Abort(%v), Wait = %v, want Abort(%v) and successful Ack", abortErr, err, dropReason)
					}
				} else {
					var pe *PanicError
					if !errors.As(abortErr, &pe) || pe.Value != "mailbox failed" || len(pe.Stack) == 0 || !IsPermanent(err) || !errors.Is(err, pe) {
						t.Errorf("Abort = %v, Wait = %v, want permanent mailbox *PanicError", abortErr, err)
					}
					assertDispatchMetric(t, deps.Metrics, `yabot_dispatch_panics_total{route="deferred"}`, 1)
					record := readRouterLog(t, &dispatchLogs)
					if record.Level != "ERROR" || record.Key != key || record.Route != "deferred" || !strings.Contains(record.Stack, "TestRouterMailboxAck") {
						t.Errorf("unexpected mailbox panic log: %s", dispatchLogs.String())
					}
				}
				assertNoRouterPanicReports(t, routerMetrics, &routerLogs)
			})
		})
	}
}

func TestRouterAllowedUpdates(t *testing.T) {
	for _, tc := range []struct {
		name  string
		kinds []KindMask
		want  []string
	}{
		{name: "no routes"},
		{
			name:  "overlap",
			kinds: []KindMask{KindMyChatMember.Mask(), KindMessage.Mask() | KindChatJoinRequest.Mask(), KindChatJoinRequest.Mask()},
			want:  []string{"message", "chat_join_request", "my_chat_member"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := New(testDeps())
			if err != nil {
				t.Fatal(err)
			}
			r := NewRouter(d, RouterDeps{})
			for i, kinds := range tc.kinds {
				rt := routerTestRoute(fmt.Sprintf("route-%d", i), false, func(context.Context, *Shard, *models.Update, Ack) {})
				rt.Kinds = kinds
				r.Register(rt)
			}
			for range 2 {
				if got := r.AllowedUpdates(); got == nil || !slices.Equal(got, tc.want) {
					t.Errorf("AllowedUpdates = %#v, want non-nil %v", got, tc.want)
				}
			}
		})
	}
}

func TestRouterRegisterPanics(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(*Router, *Route)
	}{
		{name: "empty name", prepare: func(_ *Router, rt *Route) { rt.Name = "" }},
		{name: "reserved name", prepare: func(_ *Router, rt *Route) { rt.Name = "none" }},
		{name: "duplicate name", prepare: func(r *Router, rt *Route) { r.Register(*rt) }},
		{name: "empty kinds", prepare: func(_ *Router, rt *Route) { rt.Kinds = 0 }},
		{name: "unknown kind", prepare: func(_ *Router, rt *Route) { rt.Kinds |= KindMask(1) << 63 }},
		{name: "nil Key", prepare: func(_ *Router, rt *Route) { rt.Key = nil }},
		{name: "nil Handle", prepare: func(_ *Router, rt *Route) { rt.Handle = nil }},
		{name: "after Accept", prepare: func(r *Router, _ *Route) { r.Accept(&models.Update{ID: 11}) }},
		{name: "after AllowedUpdates", prepare: func(r *Router, _ *Route) { r.AllowedUpdates() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := New(testDeps())
			if err != nil {
				t.Fatal(err)
			}
			r := NewRouter(d, RouterDeps{})
			rt := routerTestRoute("route", true, func(context.Context, *Shard, *models.Update, Ack) {})
			tc.prepare(r, &rt)
			defer func() {
				if recover() == nil {
					t.Error("Register did not panic")
				}
			}()
			r.Register(rt)
		})
	}
}

func TestRouterUnroutedUpdate(t *testing.T) {
	d := startTestDispatcher(t, testDeps())
	set := metrics.NewSet()
	r := NewRouter(d, RouterDeps{Metrics: set})
	r.Register(routerTestRoute("message", true, func(context.Context, *Shard, *models.Update, Ack) {
		t.Error("message handler ran for an unrouted update")
	}))
	for _, u := range []*models.Update{{ID: 13}, {ID: 14, EditedMessage: &models.Message{}}} {
		if err := r.Accept(u).Wait(0); err != nil {
			t.Errorf("Wait for unrouted update %d = %v, want nil", u.ID, err)
		}
	}
	assertDispatchMetric(t, set, "yabot_dispatch_unrouted_total", 2)
}

func routerTestRoute(name string, critical bool, handle func(context.Context, *Shard, *models.Update, Ack)) Route {
	return Route{
		Name:     name,
		Kinds:    KindMessage.Mask(),
		Key:      func(*models.Update) Key { return Key{} },
		Critical: critical,
		Handle:   handle,
	}
}

func assertRouterErrorRoute(t *testing.T, err error, route string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), route) {
		t.Errorf("Wait error %v is missing route %q", err, route)
	}
}

func assertNoRouterPanicReports(t *testing.T, set *metrics.Set, logs *bytes.Buffer) {
	t.Helper()
	for _, name := range set.ListMetricNames() {
		if strings.HasPrefix(name, "yabot_dispatch_panics_total") {
			t.Errorf("Router metrics contain unexpected panic counter %s", name)
		}
	}
	if logs.Len() != 0 {
		t.Errorf("Router logger received unexpected records: %s", logs.String())
	}
}

type routerLogRecord struct {
	Level    string
	Route    string
	Key      Key
	UpdateID int64 `json:"update_id"`
	Stack    string
	Error    string
}

func assertRouterDropLog(t *testing.T, logs *bytes.Buffer, route string, updateID int64, err error) {
	t.Helper()
	record := readRouterLog(t, logs)
	if record.Level != "INFO" || record.Route != route || record.UpdateID != updateID || record.Error != err.Error() {
		t.Errorf("unexpected drop log: %s", logs.String())
	}
}

func readRouterLog(t *testing.T, logs *bytes.Buffer) routerLogRecord {
	t.Helper()
	var record routerLogRecord
	if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
		t.Fatalf("expected one JSON log, got %q: %v", logs.String(), err)
	}
	return record
}

func TestRouterDeliversEachRouteToItsKeyShard(t *testing.T) {
	deps := testDeps()
	deps.Shards = 4
	d := startTestDispatcher(t, deps)
	keys := make([]Key, 0, 2)
	for a := int64(1); len(keys) < 2; a++ {
		k := Key{Space: 7, A: a}
		if d.shardIndex(k) != 0 && (len(keys) == 0 || d.shardIndex(k) != d.shardIndex(keys[0])) {
			keys = append(keys, k)
		}
	}
	r := NewRouter(d, RouterDeps{})
	type delivery struct {
		route int
		shard int
	}
	ran := make(chan delivery, len(keys))
	for i, k := range keys {
		rt := routerTestRoute(fmt.Sprintf("route-%d", i), true, func(_ context.Context, sh *Shard, _ *models.Update, ack Ack) {
			ran <- delivery{route: i, shard: sh.Index()}
			ack.Done(nil)
		})
		rt.Key = func(*models.Update) Key { return k }
		r.Register(rt)
	}
	if err := r.Accept(&models.Update{ID: 1, Message: &models.Message{}}).Wait(5 * time.Second); err != nil {
		t.Fatalf("Wait = %v, want nil", err)
	}
	for range keys {
		got := await(t, ran)
		if want := d.shardIndex(keys[got.route]); got.shard != want {
			t.Errorf("route %d ran on shard %d, want %d", got.route, got.shard, want)
		}
	}
}
