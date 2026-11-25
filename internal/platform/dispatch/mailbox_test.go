package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/maphash"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

func TestMailboxReadyItemsCheckBusyAndKeepOrder(t *testing.T) {
	d := startTestDispatcher(t, testDeps())
	key := Key{A: 1}
	var trace mailboxTestTrace
	runMailboxTestTask(t, d, func(sh *Shard) {
		sh.DeferInternal(key, trace.item("hold", func(_ context.Context, sh *Shard) {
			sh.Hold(key)
			sh.DeferInternal(key, trace.item("newer", nil))
		}))
		sh.DeferInternal(key, trace.item("older", nil))
		trace = append(trace, "submitted")
	})
	trace.check(t, "submitted", "hold")

	runMailboxTestTask(t, d, func(sh *Shard) {
		if !sh.Busy(key) {
			t.Error("entity is idle after Hold")
		}
		sh.Release(key)
	})
	trace.check(t, "submitted", "hold", "older", "newer")
}

func TestMailboxIdleExternalItemRunsAfterCurrentTask(t *testing.T) {
	d := startTestDispatcher(t, testDeps())
	key := Key{A: 1}
	var trace mailboxTestTrace
	runMailboxTestTask(t, d, func(sh *Shard) {
		if !sh.Defer(key, trace.item("external", nil)) {
			t.Error("Defer rejected an idle entity")
		}
		trace = append(trace, "submitted")
	})
	trace.check(t, "submitted", "external")
}

func TestMailboxReleaseStopsAtHoldAndKeepsQueuedOrder(t *testing.T) {
	d := startTestDispatcher(t, testDeps())
	key := Key{A: 1}
	var trace mailboxTestTrace
	runMailboxTestTask(t, d, func(sh *Shard) {
		sh.Hold(key)
		sh.DeferInternal(key, trace.item("rehold", func(_ context.Context, sh *Shard) {
			sh.Hold(key)
		}))
		sh.DeferInternal(key, trace.item("older item", nil))
	})
	trace.check(t)

	runMailboxTestTask(t, d, func(sh *Shard) {
		sh.Release(key)
		if !sh.Defer(key, trace.item("external after Release", nil)) {
			t.Error("Defer rejected an entity with space")
		}
		sh.DeferInternal(key, trace.item("internal after Release", nil))
	})
	trace.check(t, "rehold")

	runMailboxTestTask(t, d, func(sh *Shard) {
		if !sh.Busy(key) {
			t.Error("entity is idle after a mailbox item called Hold")
		}
		sh.Release(key)
		sh.Release(key)
	})
	trace.check(t, "rehold", "older item", "external after Release", "internal after Release")
}

func TestMailboxAppendsWorkDuringDrain(t *testing.T) {
	d := startTestDispatcher(t, testDeps())
	key := Key{A: 1}
	var trace mailboxTestTrace
	runMailboxTestTask(t, d, func(sh *Shard) {
		sh.Hold(key)
		sh.DeferInternal(key, trace.item("first", func(_ context.Context, sh *Shard) {
			sh.Release(key)
			sh.DeferInternal(key, trace.item("enqueued during drain", nil))
			trace = append(trace, "first returned")
		}))
		sh.DeferInternal(key, trace.item("already queued", nil))
		sh.Release(key)
	})
	trace.check(t, "first", "first returned", "already queued", "enqueued during drain")
}

func TestMailboxLimitCountsOnlyExternalItems(t *testing.T) {
	deps := testDeps()
	deps.Metrics = metrics.NewSet()
	d := startTestDispatcher(t, deps)
	key := Key{A: 1}
	var trace mailboxTestTrace
	want := []string{"first internal"}
	for i := range deps.MailboxPerEntity {
		want = append(want, fmt.Sprintf("external %d", i))
	}
	want = append(want, "last internal", "external after drain")

	runMailboxTestTask(t, d, func(sh *Shard) {
		sh.Hold(key)
		sh.DeferInternal(key, trace.item(want[0], func(_ context.Context, sh *Shard) {
			if sh.Defer(key, trace.item("overflow during Release", nil)) {
				t.Error("Defer accepted an external item while the released mailbox was still full")
			}
		}))
		for _, name := range want[1 : 1+deps.MailboxPerEntity] {
			if !sh.Defer(key, trace.item(name, nil)) {
				t.Errorf("Defer rejected %s before the mailbox was full", name)
			}
		}
		if sh.Defer(key, trace.item("overflow", nil)) {
			t.Error("Defer accepted an external item with a full mailbox")
		}
		sh.DeferInternal(key, trace.item("last internal", nil))
	})
	trace.check(t)
	assertDispatchMetric(t, deps.Metrics, "yabot_dispatch_mailbox_overflow_total", 1)

	runMailboxTestTask(t, d, func(sh *Shard) { sh.Release(key) })
	runMailboxTestTask(t, d, func(sh *Shard) {
		if !sh.Defer(key, trace.item("external after drain", nil)) {
			t.Error("Defer rejected an item after the mailbox was drained")
		}
	})
	trace.check(t, want...)
	assertDispatchMetric(t, deps.Metrics, "yabot_dispatch_mailbox_overflow_total", 2)
}

func TestMailboxExternalCapacityReturnsBeforeDrain(t *testing.T) {
	for _, capacity := range []int{2, 16} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			deps := testDeps()
			deps.MailboxPerEntity = capacity
			d := startTestDispatcher(t, deps)
			key := Key{A: 1}
			var trace mailboxTestTrace
			want := make([]string, capacity+1)
			for i := range want {
				want[i] = fmt.Sprintf("external %d", i)
			}
			runMailboxTestTask(t, d, func(sh *Shard) {
				sh.Hold(key)
				first := trace.item(want[0], func(_ context.Context, sh *Shard) { sh.Hold(key) })
				if !sh.Defer(key, first) {
					t.Error("Defer rejected the first item")
				}
				for _, name := range want[1:capacity] {
					if !sh.Defer(key, trace.item(name, nil)) {
						t.Errorf("Defer rejected %s before reaching capacity", name)
					}
				}
				sh.Release(key)
			})
			trace.check(t, want[0])

			runMailboxTestTask(t, d, func(sh *Shard) {
				if !sh.Defer(key, trace.item(want[capacity], nil)) {
					t.Error("Defer did not reuse the slot freed by the first item")
				}
				if sh.Defer(key, trace.item("overflow", nil)) {
					t.Error("Defer accepted more than one item into the freed slot")
				}
				sh.Release(key)
			})
			trace.check(t, want...)
		})
	}
}

func TestMailboxInternalItemsExceedExternalCapacity(t *testing.T) {
	for _, tc := range []struct {
		name string
		hold bool
	}{
		{name: "held", hold: true},
		{name: "ready"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := testDeps()
			deps.MailboxPerEntity = 16
			d := startTestDispatcher(t, deps)
			key := Key{A: 1}
			var trace mailboxTestTrace
			want := make([]string, 3*deps.MailboxPerEntity)
			for i := range want {
				want[i] = fmt.Sprintf("internal %d", i)
			}

			runMailboxTestTask(t, d, func(sh *Shard) {
				if tc.hold {
					sh.Hold(key)
				}
				for _, name := range want {
					sh.DeferInternal(key, trace.item(name, nil))
				}
				if tc.hold && len(sh.ready) != 0 {
					t.Error("internal items for the held entity were registered as ready")
				}
			})
			if tc.hold {
				trace.check(t)
				runMailboxTestTask(t, d, func(sh *Shard) { sh.Release(key) })
			}
			trace.check(t, want...)
		})
	}
}

func TestMailboxDropAbortsInOrderAndRecovers(t *testing.T) {
	for _, tc := range []struct {
		name string
		hold bool
	}{
		{name: "held", hold: true},
		{name: "ready"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := testDeps()
			deps.Metrics = metrics.NewSet()
			logs := &ticketTestLogs{}
			deps.Log = slog.New(logs)
			d := startTestDispatcher(t, deps)
			key := Key{A: 1}
			reason := errors.New("entity deleted")
			var trace mailboxTestTrace
			runMailboxTestTask(t, d, func(sh *Shard) {
				if tc.hold {
					sh.Hold(key)
				}
				for i, name := range []string{"first", "second", "third"} {
					item := trace.item(name, nil)
					abort := item.Abort
					item.Abort = func(got error) {
						abort(got)
						if got != reason {
							t.Errorf("Abort(%s) reason = %v, want the original %v", name, got, reason)
						}
						if i == 1 {
							panic("abort failed")
						}
					}
					sh.DeferInternal(key, item)
				}
				sh.Drop(key, reason)
				if sh.Busy(key) || len(sh.mailboxes) != 0 {
					t.Error("Drop left entity state behind")
				}
				sh.DeferInternal(key, trace.item("new event", nil))
			})
			trace.check(t, "abort first", "abort second", "abort third", "new event")
			assertDispatchMetric(t, deps.Metrics, `yabot_dispatch_panics_total{route="mailbox"}`, 1)
			if got := logs.warnings.Load(); got != 1 {
				t.Errorf("Abort panic produced %d warning or error logs, want 1", got)
			}
		})
	}
}

func TestMailboxClearsIdleEntities(t *testing.T) {
	d := startTestDispatcher(t, testDeps())
	var trace mailboxTestTrace
	runMailboxTestTask(t, d, func(sh *Shard) {
		for i := range 32 {
			key := Key{A: int64(i)}
			sh.Release(key)
			sh.Drop(key, ErrClosed)
			sh.Hold(key)
			if got := mailboxTestPanicValue(func() { sh.Hold(key) }); !strings.Contains(fmt.Sprint(got), fmt.Sprint(key)) {
				t.Errorf("duplicate Hold panic = %v, want key %v in the message", got, key)
			}
			sh.Release(key)
		}
		if got := len(sh.mailboxes); got != 0 {
			t.Errorf("releasing empty entities left %d mailbox entries", got)
		}
		sh.DeferInternal(Key{A: 1}, trace.item("ready", nil))
		sh.Hold(Key{A: 2})
		sh.DeferInternal(Key{A: 2}, trace.item("released", nil))
		sh.Release(Key{A: 2})
	})
	slices.Sort(trace)
	trace.check(t, "ready", "released")
	runMailboxTestTask(t, d, func(sh *Shard) {
		if got := len(sh.mailboxes); got != 0 {
			t.Errorf("draining entities left %d mailbox entries", got)
		}
	})
}

func TestMailboxRunPanicPreservesHold(t *testing.T) {
	for _, tc := range []struct {
		name string
		hold bool
	}{
		{name: "held", hold: true},
		{name: "ready"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := testDeps()
			deps.Metrics = metrics.NewSet()
			d := startTestDispatcher(t, deps)
			key := Key{A: 1}
			var trace mailboxTestTrace
			runMailboxTestTask(t, d, func(sh *Shard) {
				if tc.hold {
					sh.Hold(key)
				}
				sh.DeferInternal(key, trace.item("panicking task", func(_ context.Context, sh *Shard) {
					sh.Hold(key)
					panic("run failed")
				}))
				sh.DeferInternal(key, trace.item("following task", nil))
				if tc.hold {
					sh.Release(key)
				}
			})
			trace.check(t, "panicking task", "abort panicking task")
			assertDispatchMetric(t, deps.Metrics, `yabot_dispatch_panics_total{route="mailbox"}`, 1)
			runMailboxTestTask(t, d, func(sh *Shard) {
				if !sh.Busy(key) {
					t.Error("panic cleared the Hold")
				}
				sh.Release(key)
			})
			trace.check(t, "panicking task", "abort panicking task", "following task")
		})
	}
}

func TestMailboxRunPanicAborts(t *testing.T) {
	for _, path := range []string{"held", "ready", "replay"} {
		for _, tc := range []struct {
			name       string
			itemName   string
			route      string
			abortPanic bool
		}{
			{name: "named", itemName: "join-request", route: "join-request"},
			{name: "unnamed", route: "mailbox"},
			{name: "panicking Abort", itemName: "join-request", route: "join-request", abortPanic: true},
		} {
			t.Run(path+"/"+tc.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					const runFailure = "run failed"
					const abortFailure = "abort failed"
					deps := testDeps()
					deps.Metrics = metrics.NewSet()
					var logged bytes.Buffer
					deps.Log = slog.New(slog.NewJSONHandler(&logged, nil))
					clock := newMailboxLifecycleClock(&deps)
					d := startTestDispatcher(t, deps)
					key := Key{Space: 17, A: -12, B: 19}
					var trace mailboxTestTrace
					var abortErr error
					item := Item{
						Name: tc.itemName,
						Run: func(context.Context, *Shard) {
							trace = append(trace, "run")
							panic(runFailure)
						},
						Abort: func(err error) {
							trace = append(trace, "abort run")
							abortErr = err
							if tc.abortPanic {
								panic(abortFailure)
							}
						},
					}
					runMailboxTestTask(t, d, func(sh *Shard) {
						switch path {
						case "held":
							sh.Hold(key)
							if !sh.Defer(key, item) {
								t.Error("Defer rejected an item in an empty mailbox")
							}
						case "ready":
							sh.DeferInternal(key, item)
						case "replay":
							sh.Requeue(key, item, 0)
						}
						sh.DeferInternal(key, trace.item("next", nil))
					})
					switch path {
					case "held":
						trace.check(t)
						runMailboxTestTask(t, d, func(sh *Shard) { sh.Release(key) })
					case "replay":
						trace.check(t)
						clock.tick(100 * time.Millisecond)
						waitForShardTask(t, d)
					}
					trace.check(t, "run", "abort run", "next")
					var panicErr *PanicError
					if !errors.As(abortErr, &panicErr) || panicErr.Value != runFailure ||
						!strings.Contains(panicErr.Error(), runFailure) || len(panicErr.Stack) == 0 {
						t.Errorf("Abort reason = %v, want *PanicError with value %q and a stack", abortErr, runFailure)
					}
					wantPanics := []string{runFailure}
					if tc.abortPanic {
						wantPanics = append(wantPanics, abortFailure)
					}
					assertDispatchMetric(t, deps.Metrics, fmt.Sprintf(`yabot_dispatch_panics_total{route=%q}`, tc.route), len(wantPanics))
					assertMailboxPanicLogs(t, &logged, key, tc.route, wantPanics)
				})
			})
		}
	}
}

func assertMailboxPanicLogs(t *testing.T, logged *bytes.Buffer, key Key, route string, values []string) {
	t.Helper()
	decoder := json.NewDecoder(logged)
	for _, value := range values {
		var record struct {
			Level string
			Shard *int
			Key   Key
			Route string
			Panic string
			Stack string
		}
		if err := decoder.Decode(&record); err != nil {
			t.Fatalf("decoding panic log for %q: %v", value, err)
		}
		if record.Level != "ERROR" || record.Shard == nil || *record.Shard != 0 || record.Key != key ||
			record.Route != route || record.Panic != value || !strings.Contains(record.Stack, "TestMailboxRunPanicAborts") {
			t.Errorf("panic log = %+v, want ERROR for shard 0, key %v, route %q, value %q and a stack", record, key, route, value)
		}
	}
	if decoder.More() {
		t.Error("unexpected extra panic log")
	}
}

func TestMailboxOperationsRejectForeignKey(t *testing.T) {
	deps := testDeps()
	deps.Shards = 2
	d := startTestDispatcher(t, deps)
	keys := [2]Key{}
	for i := int64(1); keys[0] == (Key{}) || keys[1] == (Key{}); i++ {
		key := Key{A: i}
		keys[maphash.Comparable(d.seed, key)%uint64(len(keys))] = key
	}
	item := Item{Run: func(context.Context, *Shard) {}, Abort: func(error) {}}
	for _, tc := range []struct {
		name string
		call func(*Shard, Key)
	}{
		{"Hold", func(sh *Shard, key Key) { sh.Hold(key) }},
		{"Busy", func(sh *Shard, key Key) { sh.Busy(key) }},
		{"Defer", func(sh *Shard, key Key) { sh.Defer(key, item) }},
		{"DeferInternal", func(sh *Shard, key Key) { sh.DeferInternal(key, item) }},
		{"Release", func(sh *Shard, key Key) { sh.Release(key) }},
		{"Requeue", func(sh *Shard, key Key) { sh.Requeue(key, item, 0) }},
		{"Drop", func(sh *Shard, key Key) { sh.Drop(key, ErrClosed) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			err := d.RunOnAll(ctx, func(_ context.Context, sh *Shard, done func(error)) {
				foreign := keys[1-sh.Index()]
				if got := mailboxTestPanicValue(func() { tc.call(sh, foreign) }); !strings.Contains(fmt.Sprint(got), fmt.Sprint(foreign)) {
					t.Errorf("%s panic on shard %d = %v, want foreign key %v", tc.name, sh.Index(), got, foreign)
				}
				done(nil)
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMailboxOperationsRejectInvalidItems(t *testing.T) {
	d := startTestDispatcher(t, testDeps())
	key := Key{A: 1}
	for _, op := range []struct {
		name string
		call func(*Shard, Item)
	}{
		{"Defer", func(sh *Shard, item Item) { sh.Defer(key, item) }},
		{"DeferInternal", func(sh *Shard, item Item) { sh.DeferInternal(key, item) }},
		{"Requeue", func(sh *Shard, item Item) { sh.Requeue(key, item, 0) }},
	} {
		for _, input := range []struct {
			name string
			item Item
		}{
			{"nil Run", Item{Abort: func(error) {}}},
			{"nil Abort", Item{Run: func(context.Context, *Shard) {}}},
		} {
			t.Run(op.name+"/"+input.name, func(t *testing.T) {
				runMailboxTestTask(t, d, func(sh *Shard) {
					if mailboxTestPanicValue(func() { op.call(sh, input.item) }) == nil {
						t.Errorf("%s accepted %s", op.name, input.name)
					}
					if sh.Busy(key) || len(sh.mailboxes) != 0 {
						t.Errorf("%s changed the mailbox with %s", op.name, input.name)
					}
				})
			})
		}
	}
}

type mailboxTestTrace []string

func (trace *mailboxTestTrace) item(name string, action Task) Item {
	return Item{
		Run: func(ctx context.Context, sh *Shard) {
			*trace = append(*trace, name)
			if action != nil {
				action(ctx, sh)
			}
		},
		Abort: func(error) { *trace = append(*trace, "abort "+name) },
	}
}

func (trace mailboxTestTrace) check(t *testing.T, want ...string) {
	t.Helper()
	if !slices.Equal(trace, want) {
		t.Fatalf("mailbox events = %q, want %q", trace, want)
	}
}

func runMailboxTestTask(t *testing.T, d *Dispatcher, fn func(*Shard)) {
	t.Helper()
	submitTicketTestTask(t, d, func(_ context.Context, sh *Shard) { fn(sh) })
	waitForShardTask(t, d)
}

func mailboxTestPanicValue(fn func()) (value any) {
	defer func() { value = recover() }()
	fn()
	return nil
}
