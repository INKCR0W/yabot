package ingress

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot/models"
)

func TestNewRejectsInvalidDeps(t *testing.T) {
	source := stubSource{}
	skew := telegram.NewTimeSkew(nil, nil)
	deps := testDeps(source, newTestRouter(t), skew, nil)
	for _, tc := range []struct {
		name   string
		change func(*Deps)
	}{
		{"source", func(d *Deps) { d.Source = nil }},
		{"router", func(d *Deps) { d.Router = (*dispatch.Router)(nil) }},
		{"store", func(d *Deps) { d.Store = (*store.Store)(nil) }},
		{"skew", func(d *Deps) { d.Skew = (*telegram.TimeSkew)(nil) }},
		{"fatal", func(d *Deps) { d.Fatal = nil }},
		{"poll timeout", func(d *Deps) { d.PollTimeout = 0 }},
		{"settle timeout", func(d *Deps) { d.SettleTimeout = -time.Second }},
		{"transient budget", func(d *Deps) { d.TransientBudget = 0 }},
		{"poison threshold", func(d *Deps) { d.PoisonThreshold = 0 }},
		{"retry delay", func(d *Deps) { d.RetryDelay = -time.Second }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invalid := deps
			tc.change(&invalid)
			if got, err := New(invalid); got != nil || err == nil {
				t.Fatalf("New returned (%v, %v), want dependency error", got, err)
			}
		})
	}
}

func TestDedupeEvictsOldest(t *testing.T) {
	ring := dedupeRing{seen: make(map[int64]struct{})}
	for id := int64(0); id < DedupeCapacity; id++ {
		ring.add(id)
	}
	ring.add(0)
	ring.add(DedupeCapacity)
	if ring.has(0) || !ring.has(1) || !ring.has(DedupeCapacity) {
		t.Fatal("adding an ID to a full ring did not evict only the oldest ID")
	}
	ring.add(0)
	if !ring.has(0) || ring.has(1) || !ring.has(2) {
		t.Fatal("re-adding an evicted ID did not advance the ring")
	}
}

func TestBatchSharesSettleDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type pollRecord struct {
			offset   int64
			at       time.Time
			deadline time.Time
		}
		requests := make(chan pollRecord, 2)
		acks := make(chan dispatch.Ack, 2)
		fatals := make(chan fatalCall, 1)
		router := newTestRouter(t)
		const routingDelay = 750 * time.Millisecond
		router.Register(dispatch.Route{
			Name:     "unsettled",
			Kinds:    dispatch.KindMessage.Mask(),
			Critical: true,
			Match: func(u *models.Update) bool {
				if u.ID == 2 {
					time.Sleep(routingDelay)
				}
				return true
			},
			Key: func(*models.Update) dispatch.Key { return dispatch.Key{Space: 1} },
			Handle: func(_ context.Context, _ *dispatch.Shard, _ *models.Update, _ time.Time, ack dispatch.Ack) {
				acks <- ack
			},
		})
		calls := 0
		source := stubSource{poll: func(ctx context.Context, offset int64, _ int, _ time.Duration, _ []string) ([]models.Update, error) {
			deadline, _ := ctx.Deadline()
			requests <- pollRecord{offset: offset, at: time.Now(), deadline: deadline}
			calls++
			if calls == 1 {
				return []models.Update{{ID: 1, Message: &models.Message{Date: 1}}, {ID: 2, Message: &models.Message{}}}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		skew := pollSkewFunc(func(time.Time, int64) time.Duration {
			// Lag measurement uses part of the batch's settle budget.
			time.Sleep(250 * time.Millisecond)
			return 0
		})
		deps := testDeps(source, router, skew, fatals)
		deps.Metrics = metrics.NewSet()
		ingress := startTestIngress(t, deps)
		first := awaitValue(t, requests)
		firstAck, secondAck := awaitValue(t, acks), awaitValue(t, acks)
		second := awaitValue(t, requests)
		if got := first.deadline.Sub(first.at); got != deps.PollTimeout+10*time.Second {
			t.Errorf("poll context timeout = %v, want poll timeout plus 10 seconds", got)
		}
		if got, want := second.at.Sub(first.at), deps.SettleTimeout+deps.RetryDelay; got != want || second.offset != first.offset {
			t.Errorf("retry at +%v after response with offset %d, want +%v with offset %d", got, second.offset, want, first.offset)
		}
		firstAck.Done(nil)
		secondAck.Done(nil)
		if err := ingress.Stop(t.Context()); err != nil {
			t.Fatalf("stop ingress: %v", err)
		}
		select {
		case fatal := <-fatals:
			t.Fatalf("received unexpected Fatal: %v", fatal.err)
		default:
		}
	})
}

func TestPollRoutesBatchReceiveTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type response struct {
			offset int64
			at     time.Time
		}
		type delivery struct {
			id     int64
			recvAt time.Time
			at     time.Time
		}
		polls := make(chan response, 3)
		deliveries := make(chan delivery, 4)
		fatals := make(chan fatalCall, 1)
		router := newTestRouter(t)
		retry := errors.New("retry update")
		failed := false
		registerRoute(router, "message", dispatch.KindMessage.Mask(), true, func(u *models.Update, ack dispatch.Ack) {
			if u.ID == 2 && !failed {
				failed = true
				ack.Done(retry)
				return
			}
			ack.Done(nil)
		})
		observed := observingRouter{Router: router, onAccept: func(u *models.Update, recvAt time.Time) {
			deliveries <- delivery{id: u.ID, recvAt: recvAt, at: time.Now()}
		}}
		calls := 0
		source := stubSource{poll: func(ctx context.Context, offset int64, _ int, timeout time.Duration, _ []string) ([]models.Update, error) {
			if timeout == 0 {
				return nil, nil
			}
			calls++
			if calls > 2 {
				polls <- response{offset: offset}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			time.Sleep(10 * time.Millisecond)
			polls <- response{offset: offset, at: time.Now()}
			return []models.Update{{ID: 1, Message: &models.Message{Date: 1}}, {ID: 2, Message: &models.Message{Date: 1}}}, nil
		}}
		skew := pollSkewFunc(func(time.Time, int64) time.Duration {
			// Advance the fake clock between the response and routing.
			time.Sleep(time.Millisecond)
			return 0
		})
		deps := testDeps(source, observed, skew, fatals)
		deps.Metrics = metrics.NewSet()
		ingress := startTestIngress(t, deps)

		var previous time.Time
		for _, ids := range [][]int64{{1, 2}, {2}} {
			poll := awaitValue(t, polls)
			if poll.offset != 0 || !poll.at.After(previous) {
				t.Errorf("response at %v with offset %d, want a later response than %v with offset 0", poll.at, poll.offset, previous)
			}
			for _, id := range ids {
				got := awaitValue(t, deliveries)
				if got.id != id || !got.recvAt.Equal(poll.at) || !got.recvAt.Before(got.at) {
					t.Errorf("update %d received at %v and routed at %v, want update %d received at %v before routing",
						got.id, got.recvAt, got.at, id, poll.at)
				}
			}
			previous = poll.at
		}
		if got := awaitValue(t, polls).offset; got != 3 {
			t.Errorf("offset after the retried batch = %d, want 3", got)
		}
		if err := ingress.Stop(t.Context()); err != nil {
			t.Fatalf("stop ingress: %v", err)
		}
		select {
		case got := <-deliveries:
			t.Errorf("unexpected delivery of update %d", got.id)
		case fatal := <-fatals:
			t.Errorf("unexpected Fatal: %v", fatal.err)
		default:
		}
	})
}

func TestStopBeforeStart(t *testing.T) {
	source := stubSource{}
	deps := testDeps(source, newTestRouter(t), telegram.NewTimeSkew(nil, nil), nil)
	ingress, err := New(deps)
	if err != nil {
		t.Fatalf("create ingress: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := ingress.Stop(ctx); err != nil {
		t.Fatalf("stop before Start: %v", err)
	}
}

type stubSource struct {
	poll func(context.Context, int64, int, time.Duration, []string) ([]models.Update, error)
}

func (s stubSource) GetUpdates(ctx context.Context, offset int64, limit int, timeout time.Duration, allowed []string) ([]models.Update, error) {
	if s.poll == nil {
		return nil, errors.New("unexpected poll")
	}
	return s.poll(ctx, offset, limit, timeout, allowed)
}

func (stubSource) DeleteWebhook(context.Context) error { return nil }

type observingRouter struct {
	Router
	onAccept func(*models.Update, time.Time)
}

func (r observingRouter) Accept(u *models.Update, recvAt time.Time) dispatch.Receipt {
	r.onAccept(u, recvAt)
	return r.Router.Accept(u, recvAt)
}
