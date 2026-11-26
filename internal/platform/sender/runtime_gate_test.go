package sender

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math"
	"runtime"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot"

	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

func gateRuntimeDeps(t *testing.T) Deps {
	t.Helper()
	b, err := bot.New("123456:test-token", bot.WithSkipGetMe())
	if err != nil {
		t.Fatal(err)
	}
	return Deps{
		Bot: b, Workers: 1, CapInteractive: 1, CapBackground: 1,
		GlobalRPS: 100, GlobalBurst: 10, ClassWeights: [4]int{8, 4, 2, 1}, ChatQueue: 32,
	}
}

func gateRuntimeStart(t *testing.T, deps Deps) *Sender {
	t.Helper()
	s, err := New(deps)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	return s
}

func gateRuntimeStop(t *testing.T, s *Sender) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Error(err)
	}
}

func gateRuntimeRequest(class Class, chatID int64, method string, deadline time.Time) (Request, <-chan Result) {
	done := make(chan Result, 2)
	return Request{
		Class: class, ChatID: chatID, Method: method, Deadline: deadline,
		Do: func(context.Context, *bot.Bot) error { return nil }, Done: func(r Result) { done <- r },
	}, done
}

func gateRuntimeEnqueue(t *testing.T, s *Sender, r Request) {
	t.Helper()
	if err := s.Enqueue(r); err != nil {
		t.Fatal(err)
	}
}

func gateRuntimeResult(t *testing.T, done <-chan Result) Result {
	t.Helper()
	select {
	case r := <-done:
		return r
	default:
		t.Fatal("request did not complete")
		return Result{}
	}
}

func gateRuntimePending(t *testing.T, done <-chan Result) {
	t.Helper()
	select {
	case r := <-done:
		t.Fatalf("request completed early: %+v", r)
	default:
	}
}

func gateRuntimeExpired(t *testing.T, done <-chan Result, attempts int) {
	t.Helper()
	if r := gateRuntimeResult(t, done); r.Dropped != Expired || r.Outcome.Class != telegram.Retry || r.Attempts != attempts || r.Ambiguous {
		t.Fatalf("result = %+v, want unambiguous expiry after %d attempts", r, attempts)
	}
}

func TestSenderGateRuntimeConfig(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Deps)
		field  string
	}{
		{"bot", func(d *Deps) { d.Bot = nil }, "Bot"},
		{"workers", func(d *Deps) { d.Workers = 0 }, "Workers"},
		{"negative deadline reserve", func(d *Deps) { d.ReservedDeadline = -1 }, "reserved worker"},
		{"negative decision reserve", func(d *Deps) { d.ReservedDecision = -1 }, "reserved worker"},
		{"deadline reserve leaves no shared workers", func(d *Deps) { d.ReservedDeadline = 1 }, "reservations"},
		{"decision reserve leaves no shared workers", func(d *Deps) { d.ReservedDecision = 1 }, "reservations"},
		{"overflowing reserves", func(d *Deps) { d.ReservedDeadline, d.ReservedDecision = math.MaxInt, math.MaxInt }, "reservations"},
		{"interactive cap missing", func(d *Deps) { d.CapInteractive = 0 }, "CapInteractive"},
		{"background cap missing", func(d *Deps) { d.CapBackground = 0 }, "CapBackground"},
		{"interactive cap exceeds shared workers", func(d *Deps) { d.CapInteractive = 2 }, "CapInteractive"},
		{"background cap exceeds shared workers", func(d *Deps) { d.CapBackground = 2 }, "CapBackground"},
		{"zero rate", func(d *Deps) { d.GlobalRPS = 0 }, "GlobalRPS"},
		{"negative rate", func(d *Deps) { d.GlobalRPS = -1 }, "GlobalRPS"},
		{"infinite rate", func(d *Deps) { d.GlobalRPS = math.Inf(1) }, "GlobalRPS"},
		{"NaN rate", func(d *Deps) { d.GlobalRPS = math.NaN() }, "GlobalRPS"},
		{"burst", func(d *Deps) { d.GlobalBurst = 0 }, "GlobalBurst"},
		{"deadline weight", func(d *Deps) { d.ClassWeights[0] = 0 }, "deadline"},
		{"decision weight", func(d *Deps) { d.ClassWeights[1] = -1 }, "decision"},
		{"interactive weight", func(d *Deps) { d.ClassWeights[2] = 0 }, "interactive"},
		{"background weight", func(d *Deps) { d.ClassWeights[3] = -1 }, "background"},
		{"chat queue", func(d *Deps) { d.ChatQueue = 0 }, "ChatQueue"},
		{"retries", func(d *Deps) { d.MaxRetries = -1 }, "MaxRetries"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := gateRuntimeDeps(t)
			tc.change(&d)
			if s, err := New(d); s != nil || err == nil || !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("New returned (%v, %v), want an error identifying %s", s, err, tc.field)
			}
		})
	}
}

func TestSenderGateRuntimeEnqueueErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := gateRuntimeDeps(t)
		d.ChatQueue = 1
		s, err := New(d)
		if err != nil {
			t.Fatal(err)
		}
		rejected, rejectedDone := gateRuntimeRequest(Background, 1, "getChat", time.Time{})
		check := func(r Request, want error) {
			t.Helper()
			if err := s.Enqueue(r); !errors.Is(err, want) {
				t.Fatalf("Enqueue error = %v, want %v", err, want)
			}
		}
		check(rejected, ErrClosed)
		invalid := rejected
		invalid.Method = ""
		check(invalid, ErrInvalidRequest)
		if err := s.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer gateRuntimeStop(t, s)
		for _, change := range []func(*Request){
			func(r *Request) { r.Class = 0 },
			func(r *Request) { r.Class = Background + 1 },
			func(r *Request) { r.Do = nil },
			func(r *Request) { r.Done = nil },
			func(r *Request) { r.Method = "" },
			func(r *Request) { r.Class = Deadline },
			func(r *Request) { r.Class = Interactive },
		} {
			r := rejected
			change(&r)
			check(r, ErrInvalidRequest)
		}
		blocked, blockedDone := gateRuntimeRequest(Background, 99, "getChat", time.Time{})
		release := make(chan struct{})
		blocked.Do = func(ctx context.Context, _ *bot.Bot) error {
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		gateRuntimeEnqueue(t, s, blocked)
		synctest.Wait()
		queued, queuedDone := gateRuntimeRequest(Background, 1, "getChat", time.Time{})
		gateRuntimeEnqueue(t, s, queued)
		check(rejected, ErrBusy)
		gateRuntimePending(t, queuedDone)
		fullDeadline, fullDeadlineDone := gateRuntimeRequest(Deadline, 1, "answerChatJoinRequestQuery", time.Now().Add(time.Second))
		gateRuntimeEnqueue(t, s, fullDeadline)
		miss := rejected
		miss.Class, miss.Method, miss.Deadline = fullDeadline.Class, fullDeadline.Method, time.Now()
		check(miss, ErrWouldMiss)
		miss.Deadline = fullDeadline.Deadline
		check(miss, ErrBusy)
		close(release)
		synctest.Wait()
		for _, done := range []<-chan Result{blockedDone, queuedDone, fullDeadlineDone} {
			if r := gateRuntimeResult(t, done); r.Outcome.Class != telegram.OK {
				t.Fatalf("accepted result = %+v", r)
			}
		}
		gateRuntimeStop(t, s)
		check(rejected, ErrClosed)
		check(invalid, ErrInvalidRequest)
		gateRuntimePending(t, rejectedDone)
	})
}

func TestSenderRuntimeLifecycle(t *testing.T) {
	for _, action := range []string{"canceled start", "stop before start"} {
		t.Run(action, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, err := New(gateRuntimeDeps(t))
				if err != nil {
					t.Fatal(err)
				}
				defer gateRuntimeStop(t, s)
				r, done := gateRuntimeRequest(Background, 1, "getChat", time.Time{})
				if action == "stop before start" {
					gateRuntimeStop(t, s)
					if err := s.Start(t.Context()); err == nil {
						t.Error("Start succeeded after Stop")
					}
					if err := s.Enqueue(r); !errors.Is(err, ErrClosed) {
						t.Errorf("Enqueue after Stop = %v, want ErrClosed", err)
					}
					gateRuntimePending(t, done)
					return
				}
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				if err := s.Start(ctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("Start with canceled context = %v, want context.Canceled", err)
				}
				if err := s.Enqueue(r); !errors.Is(err, ErrClosed) {
					t.Errorf("Enqueue after failed Start = %v, want ErrClosed", err)
				}
				if err := s.Start(t.Context()); err != nil {
					t.Fatalf("Start after cancellation = %v", err)
				}
				if err := s.Start(t.Context()); err == nil {
					t.Error("repeated Start succeeded")
				}
				gateRuntimeEnqueue(t, s, r)
				synctest.Wait()
				if got := gateRuntimeResult(t, done); got.Outcome.Class != telegram.OK || got.Attempts != 1 {
					t.Errorf("request after successful Start = %+v, want one successful attempt", got)
				}
				gateRuntimePending(t, done)
			})
		})
	}
}

func TestSenderRuntimePredictionInputs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := gateRuntimeStart(t, gateRuntimeDeps(t))
		defer gateRuntimeStop(t, s)
		for _, class := range []Class{0, Background + 1} {
			if s.WouldMiss(class, 1, time.Now()) || s.EstimateWait(class, 1) != 0 || s.ClassWait(class) != 0 {
				t.Errorf("invalid class %d produced a prediction", class)
			}
		}
		r, done := gateRuntimeRequest(Background, 0, "getChat", time.Time{})
		r.Do = func(context.Context, *bot.Bot) error { return &bot.TooManyRequestsError{RetryAfter: 2} }
		gateRuntimeEnqueue(t, s, r)
		synctest.Wait()
		gateRuntimeResult(t, done)
		if s.WouldMiss(Decision, 1, time.Time{}) {
			t.Error("request without a deadline would miss")
		}
		if got := s.EstimateWait(Decision, 1); got != 2*time.Second {
			t.Errorf("Decision estimate during other-method pause = %v, want 2s", got)
		}
		if !s.WouldMiss(Decision, 1, time.Now().Add(time.Second)) {
			t.Error("Decision estimate excluded its method pause")
		}
		if got := s.EstimateWait(Deadline, 1); got != 10*time.Millisecond {
			t.Errorf("Deadline estimate during other-method pause = %v, want 10ms", got)
		}
	})
}

func TestSenderRuntimeConcurrentWeightsAndPrediction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := gateRuntimeStart(t, gateRuntimeDeps(t))
		defer gateRuntimeStop(t, s)
		until := time.Now().Add(time.Second)
		var callers sync.WaitGroup
		for range 3 {
			callers.Go(func() {
				for j := range 50 {
					s.SetChatWeight(int64(j+1), .25, until)
				}
			})
		}
		for chat := int64(1); chat <= 50; chat++ {
			if got := s.EstimateWait(Deadline, chat); got != 10*time.Millisecond {
				t.Errorf("solo chat %d wait = %v, want 10ms", chat, got)
			}
		}
		callers.Wait()
	})
}

func TestSenderGateRuntimePrediction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := gateRuntimeDeps(t)
		s := gateRuntimeStart(t, d)
		defer gateRuntimeStop(t, s)
		now := time.Now()
		const method = "answerChatJoinRequestQuery"
		if got := s.EstimateWait(Deadline, 4); got != 10*time.Millisecond {
			t.Fatalf("idle wait = %v, want 10ms", got)
		}
		trigger, triggerDone := gateRuntimeRequest(Decision, 0, method, time.Time{})
		trigger.Do = func(context.Context, *bot.Bot) error { return &bot.TooManyRequestsError{RetryAfter: 2} }
		gateRuntimeEnqueue(t, s, trigger)
		synctest.Wait()
		if r := gateRuntimeResult(t, triggerDone); r.Outcome.RetryAfter != 2*time.Second {
			t.Fatalf("trigger result = %+v", r)
		}
		if got := s.EstimateWait(Deadline, 4); got != 2*time.Second {
			t.Fatalf("paused wait = %v, want 2s", got)
		}
		for _, tc := range []struct {
			delay time.Duration
			miss  bool
		}{
			{time.Second, true},
			{2 * time.Second, true},
			{2*time.Second + time.Nanosecond, false},
		} {
			r, done := gateRuntimeRequest(Deadline, 4, method, now.Add(tc.delay))
			synctest.Wait()
			s.mu.Lock()
			loops, tokens := s.loops, s.global
			s.mu.Unlock()
			for range 5 {
				if got := s.WouldMiss(r.Class, r.ChatID, r.Deadline); got != tc.miss {
					t.Fatalf("WouldMiss at %v = %t, want %t", tc.delay, got, tc.miss)
				}
			}
			synctest.Wait()
			s.mu.Lock()
			afterLoops, afterTokens := s.loops, s.global
			s.mu.Unlock()
			if afterLoops != loops || afterTokens != tokens || s.ClassWait(Deadline) != 0 {
				t.Fatal("WouldMiss changed sender state")
			}
			err := s.Enqueue(r)
			if tc.miss && !errors.Is(err, ErrWouldMiss) || !tc.miss && err != nil {
				t.Fatalf("Enqueue at %v returned %v with WouldMiss=%t", tc.delay, err, tc.miss)
			}
			gateRuntimePending(t, done)
		}
		r, done := gateRuntimeRequest(Deadline, 5, "getChat", now.Add(time.Second))
		gateRuntimeEnqueue(t, s, r)
		synctest.Wait()
		if r := gateRuntimeResult(t, done); r.Outcome.Class != telegram.OK {
			t.Fatalf("request using another method class = %+v", r)
		}
		paused, _ := gateRuntimeRequest(Decision, 8, method, time.Time{})
		gateRuntimeEnqueue(t, s, paused)
		synctest.Wait()
		if got := s.ClassWait(Deadline); got != 15*time.Millisecond {
			t.Errorf("Deadline class wait = %v, want 15ms", got)
		}
		if got := s.ClassWait(Decision); got != 30*time.Millisecond {
			t.Errorf("Decision class wait = %v, want 30ms", got)
		}
	})
}

func TestSenderGateRuntimeExpiredDoneReentry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := gateRuntimeStart(t, gateRuntimeDeps(t))
		defer gateRuntimeStop(t, s)
		first, firstDone := gateRuntimeRequest(Background, 7, "sendMessage", time.Time{})
		gateRuntimeEnqueue(t, s, first)
		synctest.Wait()
		gateRuntimeResult(t, firstDone)
		reentered, reenteredDone := gateRuntimeRequest(Decision, 9, "getChat", time.Time{})
		expired, _ := gateRuntimeRequest(Background, 7, "sendMessage", time.Now().Add(100*time.Millisecond))
		attempted := make(chan struct{}, 1)
		expired.Do = func(context.Context, *bot.Bot) error { attempted <- struct{}{}; return nil }
		done := make(chan Result, 2)
		admission := make(chan error, 1)
		expired.Done = func(r Result) {
			admission <- s.Enqueue(reentered)
			done <- r
		}
		gateRuntimeEnqueue(t, s, expired)
		synctest.Wait()
		gateRuntimePending(t, done)
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		gateRuntimeExpired(t, done, 0)
		if err := <-admission; err != nil {
			t.Fatalf("Enqueue from Done returned %v", err)
		}
		if r := gateRuntimeResult(t, reenteredDone); r.Outcome.Class != telegram.OK {
			t.Fatalf("reentered result = %+v", r)
		}
		if len(attempted) != 0 {
			t.Fatal("expired request attempted Do")
		}
	})
}

func TestSenderGateRuntimeChatPause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := gateRuntimeDeps(t)
		d.Metrics = metrics.NewSet()
		s := gateRuntimeStart(t, d)
		defer gateRuntimeStop(t, s)
		began := time.Now()
		release := make(chan struct{})
		trigger, triggerDone := gateRuntimeRequest(Background, 4, "getChat", time.Time{})
		trigger.Do = func(ctx context.Context, _ *bot.Bot) error {
			select {
			case <-release:
				return &bot.TooManyRequestsError{RetryAfter: 2}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		gateRuntimeEnqueue(t, s, trigger)
		synctest.Wait()
		long, longDone := gateRuntimeRequest(Background, 4, "approveChatJoinRequest", time.Time{})
		short, shortDone := gateRuntimeRequest(Background, 4, "approveChatJoinRequest", began.Add(2*time.Second))
		other, otherDone := gateRuntimeRequest(Background, 5, "getChat", time.Time{})
		gateRuntimeEnqueue(t, s, long)
		gateRuntimeEnqueue(t, s, short)
		gateRuntimeEnqueue(t, s, other)
		synctest.Wait()
		close(release)
		synctest.Wait()
		if r := gateRuntimeResult(t, triggerDone); r.Attempts != 1 || r.Dropped != 0 || r.Ambiguous || r.Outcome.RetryAfter != 2*time.Second {
			t.Fatalf("429 trigger result = %+v", r)
		}
		gateRuntimeExpired(t, shortDone, 0)
		gateRuntimePending(t, longDone)
		if r := gateRuntimeResult(t, otherDone); r.Outcome.Class != telegram.OK {
			t.Fatalf("unaffected chat result = %+v", r)
		}
		pausedChats := d.Metrics.GetOrCreateGauge(`yabot_sender_gate_paused{scope="chat"}`, nil)
		if got := pausedChats.Get(); got != 1 {
			t.Errorf("paused chats = %v, want 1", got)
		}
		late, _ := gateRuntimeRequest(Interactive, 4, "getChat", began.Add(2*time.Second))
		lateDone := make(chan Result, 2)
		late.Done = func(r Result) {
			lateDone <- r
			runtime.Goexit()
		}
		enqueued := make(chan error, 1)
		callerExited := make(chan struct{})
		go func() {
			defer close(callerExited)
			enqueued <- s.Enqueue(late)
		}()
		<-callerExited
		select {
		case err := <-enqueued:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("Done exited the Enqueue caller's goroutine")
		}
		synctest.Wait()
		gateRuntimeExpired(t, lateDone, 0)
		time.Sleep(2*time.Second - time.Nanosecond)
		synctest.Wait()
		gateRuntimePending(t, longDone)
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if r := gateRuntimeResult(t, longDone); r.Outcome.Class != telegram.OK {
			t.Fatalf("recovered chat result = %+v", r)
		}
		if got := pausedChats.Get(); got != 0 {
			t.Errorf("paused chats after recovery = %v, want 0", got)
		}
	})
}

func TestSenderRuntimePausedChatDoesNotShareDeadlineRate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := gateRuntimeStart(t, gateRuntimeDeps(t))
		defer gateRuntimeStop(t, s)
		trigger, done := gateRuntimeRequest(Decision, 1, "getChat", time.Time{})
		trigger.Do = func(context.Context, *bot.Bot) error { return &bot.TooManyRequestsError{RetryAfter: 1} }
		gateRuntimeEnqueue(t, s, trigger)
		synctest.Wait()
		gateRuntimeResult(t, done)
		queued, queuedDone := gateRuntimeRequest(Deadline, 1, "approveChatJoinRequest", time.Now().Add(5*time.Second))
		gateRuntimeEnqueue(t, s, queued)
		synctest.Wait()
		gateRuntimePending(t, queuedDone)
		if got := s.EstimateWait(Deadline, 2); got != 10*time.Millisecond {
			t.Errorf("unpaused chat predicted wait = %v, want 10ms", got)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if got := gateRuntimeResult(t, queuedDone); got.Outcome.Class != telegram.OK || got.Attempts != 1 || got.Dropped != 0 {
			t.Errorf("paused chat result = %+v, want success after recovery", got)
		}
	})
}

func TestSenderGateRuntimeMethodPause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := gateRuntimeStart(t, gateRuntimeDeps(t))
		defer gateRuntimeStop(t, s)
		for chatID, seconds := range []int{3, 5} {
			trigger, done := gateRuntimeRequest(Decision, int64(chatID+1), "sendMessage", time.Time{})
			trigger.Do = func(context.Context, *bot.Bot) error { return &bot.TooManyRequestsError{RetryAfter: seconds} }
			gateRuntimeEnqueue(t, s, trigger)
			synctest.Wait()
			if r := gateRuntimeResult(t, done); r.Outcome.RetryAfter != time.Duration(seconds)*time.Second {
				t.Fatalf("trigger %d result = %+v", chatID, r)
			}
		}
		release := make(chan struct{})
		trigger, done := gateRuntimeRequest(Decision, 3, "sendMessage", time.Time{})
		trigger.Do = func(ctx context.Context, _ *bot.Bot) error {
			select {
			case <-release:
				return &bot.TooManyRequestsError{RetryAfter: 4}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		gateRuntimeEnqueue(t, s, trigger)
		synctest.Wait()
		chatOnly, chatOnlyDone := gateRuntimeRequest(Background, 3, "getChat", time.Now().Add(time.Second))
		gateRuntimeEnqueue(t, s, chatOnly)
		close(release)
		synctest.Wait()
		if got := gateRuntimeResult(t, done); got.Outcome.RetryAfter != 4*time.Second {
			t.Fatalf("escalating 429 = %+v, want 4s RetryAfter", got)
		}
		gateRuntimeExpired(t, chatOnlyDone, 0)
		blocked, blockedDone := gateRuntimeRequest(Background, 9, "editMessageText", time.Time{})
		other, otherDone := gateRuntimeRequest(Decision, 9, "getChat", time.Time{})
		gateRuntimeEnqueue(t, s, blocked)
		gateRuntimeEnqueue(t, s, other)
		synctest.Wait()
		gateRuntimePending(t, blockedDone)
		if r := gateRuntimeResult(t, otherDone); r.Outcome.Class != telegram.OK {
			t.Fatalf("unpaused method class result = %+v", r)
		}
		time.Sleep(5*time.Second - time.Nanosecond)
		synctest.Wait()
		gateRuntimePending(t, blockedDone)
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if r := gateRuntimeResult(t, blockedDone); r.Outcome.Class != telegram.OK {
			t.Fatalf("recovered method class result = %+v", r)
		}
	})
}

func TestSenderGateRuntimeMethodPauseExpiry(t *testing.T) {
	for _, tc := range []struct {
		name             string
		queueBeforePause bool
	}{
		{name: "queued", queueBeforePause: true},
		{name: "admission", queueBeforePause: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := gateRuntimeStart(t, gateRuntimeDeps(t))
				defer gateRuntimeStop(t, s)
				release := make(chan struct{})
				trigger, triggerDone := gateRuntimeRequest(Decision, 0, "approveChatJoinRequest", time.Time{})
				trigger.Do = func(ctx context.Context, _ *bot.Bot) error {
					select {
					case <-release:
						return &bot.TooManyRequestsError{RetryAfter: 5}
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				gateRuntimeEnqueue(t, s, trigger)
				synctest.Wait()
				now := time.Now()
				short, shortDone := gateRuntimeRequest(Interactive, 9, "answerChatJoinRequestQuery", now.Add(time.Second))
				long, longDone := gateRuntimeRequest(Interactive, 10, "answerChatJoinRequestQuery", now.Add(6*time.Second))
				unaffected, unaffectedDone := gateRuntimeRequest(Interactive, 11, "getChat", now.Add(time.Second))
				if tc.queueBeforePause {
					gateRuntimeEnqueue(t, s, short)
					gateRuntimeEnqueue(t, s, long)
					gateRuntimeEnqueue(t, s, unaffected)
					synctest.Wait()
					gateRuntimePending(t, shortDone)
				}
				close(release)
				synctest.Wait()
				if r := gateRuntimeResult(t, triggerDone); r.Outcome.RetryAfter != 5*time.Second {
					t.Fatalf("429 trigger result = %+v", r)
				}
				if !tc.queueBeforePause {
					gateRuntimeEnqueue(t, s, short)
					gateRuntimeEnqueue(t, s, long)
					gateRuntimeEnqueue(t, s, unaffected)
					synctest.Wait()
				}
				gateRuntimeExpired(t, shortDone, 0)
				if !tc.queueBeforePause {
					s.mu.Lock()
					pending := len(s.expireChats)
					s.mu.Unlock()
					if pending != 0 {
						t.Errorf("%d expired admissions retained after their callbacks", pending)
					}
				}
				gateRuntimePending(t, longDone)
				if got := gateRuntimeResult(t, unaffectedDone); got.Outcome.Class != telegram.OK || got.Attempts != 1 {
					t.Errorf("other method during pause = %+v, want one successful attempt", got)
				}
				time.Sleep(5 * time.Second)
				synctest.Wait()
				if got := gateRuntimeResult(t, longDone); got.Outcome.Class != telegram.OK || got.Attempts != 1 {
					t.Errorf("request with time remaining after recovery = %+v, want one successful attempt", got)
				}
			})
		})
	}
}

func TestSenderGateRuntimeGlobalPause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := gateRuntimeDeps(t)
		d.GlobalRPS, d.GlobalBurst = 1, 3
		s := gateRuntimeStart(t, d)
		defer gateRuntimeStop(t, s)
		began := time.Now()
		trigger, triggerDone := gateRuntimeRequest(Background, 4, "getChat", time.Time{})
		trigger.Do = func(context.Context, *bot.Bot) error { return &bot.TooManyRequestsError{RetryAfter: 31} }
		gateRuntimeEnqueue(t, s, trigger)
		synctest.Wait()
		gateRuntimeResult(t, triggerDone)
		s.mu.Lock()
		pausedBucket := s.global
		s.mu.Unlock()
		time.Sleep(5 * time.Second)
		var results []<-chan Result
		for i, method := range []string{"approveChatJoinRequest", "getChat", "sendMessage"} {
			r, done := gateRuntimeRequest(Decision, int64(i+10), method, time.Time{})
			gateRuntimeEnqueue(t, s, r)
			results = append(results, done)
		}
		synctest.Wait()
		time.Sleep(26*time.Second - time.Nanosecond)
		synctest.Wait()
		for _, done := range results {
			gateRuntimePending(t, done)
		}
		s.mu.Lock()
		actualBucket := s.global
		tokens, _ := s.global.balance(time.Now())
		s.mu.Unlock()
		if actualBucket != pausedBucket || tokens != d.GlobalBurst {
			t.Fatalf("global bucket changed during pause: before %+v, after %+v, available %d", pausedBucket, actualBucket, tokens)
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		for _, done := range results {
			if r := gateRuntimeResult(t, done); r.Outcome.Class != telegram.OK || r.Attempts != 1 {
				t.Fatalf("first burst after pause: %+v", r)
			}
		}
		if elapsed := time.Since(began); elapsed != 31*time.Second {
			t.Fatalf("first burst elapsed = %v, want 31s", elapsed)
		}
	})
}

func TestSenderGateRuntimeMetrics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := gateRuntimeDeps(t)
		d.MaxRetries = 1
		d.Metrics = metrics.NewSet()
		var logs bytes.Buffer
		d.Log = slog.New(slog.NewJSONHandler(&logs, nil))
		s := gateRuntimeStart(t, d)
		defer gateRuntimeStop(t, s)
		release := make(chan struct{})
		request, done := gateRuntimeRequest(Decision, 0, "getChat", time.Time{})
		var calls int
		request.Do = func(ctx context.Context, _ *bot.Bot) error {
			calls++
			if calls > 1 {
				return nil
			}
			select {
			case <-release:
				return &bot.TooManyRequestsError{RetryAfter: 31}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		gateRuntimeEnqueue(t, s, request)
		synctest.Wait()
		short, shortDone := gateRuntimeRequest(Deadline, 8, "answerChatJoinRequestQuery", time.Now().Add(20*time.Second))
		gateRuntimeEnqueue(t, s, short)
		synctest.Wait()
		scrape := func(want ...string) string {
			t.Helper()
			var body bytes.Buffer
			d.Metrics.WritePrometheus(&body)
			exposition := body.String()
			for _, line := range want {
				if !strings.Contains("\n"+exposition, "\n"+line+"\n") {
					t.Errorf("missing metric %q in %s", line, exposition)
				}
			}
			return exposition
		}
		scrape(`yabot_sender_queue_depth{class="deadline"} 1`, `yabot_sender_workers_busy{class="decision"} 1`)
		time.Sleep(time.Second)
		close(release)
		synctest.Wait()
		gateRuntimeExpired(t, shortDone, 0)
		gateRuntimePending(t, done)
		scrape(
			`yabot_sender_queue_depth{class="deadline"} 0`, `yabot_sender_workers_busy{class="decision"} 0`,
			`yabot_sender_dropped_total{class="deadline",reason="expired"} 1`,
			`yabot_sender_requests_total{method="getChat",class="decision",outcome="retry"} 1`,
			`yabot_sender_gate_paused{scope="class"} 1`, `yabot_sender_gate_paused{scope="global"} 1`,
		)
		unknown, unknownDone := gateRuntimeRequest(Background, 9, "unrecognized\"method\n", time.Time{})
		gateRuntimeEnqueue(t, s, unknown)
		time.Sleep(31 * time.Second)
		synctest.Wait()
		gateRuntimeResult(t, done)
		gateRuntimeResult(t, unknownDone)
		exposition := scrape(
			`yabot_sender_requests_total{method="getChat",class="decision",outcome="ok"} 1`,
			`yabot_sender_requests_total{method="other",class="background",outcome="ok"} 1`,
			`yabot_sender_gate_paused{scope="class"} 0`, `yabot_sender_gate_paused{scope="global"} 0`,
			`yabot_sender_queue_depth{class="decision"} 0`, `yabot_sender_queue_depth{class="background"} 0`,
			`yabot_sender_workers_busy{class="decision"} 0`, `yabot_sender_workers_busy{class="background"} 0`,
			`yabot_sender_latency_seconds_count{class="decision"} 1`, `yabot_sender_latency_seconds_sum{class="decision"} 32`,
		)
		if strings.Contains(exposition, "unrecognized") || strings.Contains(exposition, `yabot_sender_latency_seconds_count{class="deadline"}`) {
			t.Fatalf("unexpected method label or latency for a dropped request in %s", exposition)
		}
		logLines := logs.String()
		entered := `"level":"WARN","msg":"sender entered global pause"`
		left := `"level":"INFO","msg":"sender left global pause"`
		if strings.Count(logLines, entered) != 1 || strings.Count(logLines, left) != 1 || strings.Index(logLines, entered) > strings.Index(logLines, left) {
			t.Fatalf("global pause transition logs: %s", logLines)
		}
	})
}

func TestSenderGateRuntimePauseExtension(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := gateRuntimeDeps(t)
		d.Workers = 2
		s := gateRuntimeStart(t, d)
		defer gateRuntimeStop(t, s)
		began := time.Now()
		releases := []chan struct{}{make(chan struct{}), make(chan struct{})}
		var results []<-chan Result
		for i, seconds := range []int{31, 35} {
			trigger, done := gateRuntimeRequest(Decision, 4, "getChat", time.Time{})
			trigger.Do = func(ctx context.Context, _ *bot.Bot) error {
				select {
				case <-releases[i]:
					return &bot.TooManyRequestsError{RetryAfter: seconds}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			gateRuntimeEnqueue(t, s, trigger)
			results = append(results, done)
		}
		synctest.Wait()
		close(releases[0])
		synctest.Wait()
		gateRuntimeResult(t, results[0])
		short, shortDone := gateRuntimeRequest(Interactive, 8, "sendMessage", began.Add(33*time.Second))
		gateRuntimeEnqueue(t, s, short)
		synctest.Wait()
		gateRuntimePending(t, shortDone)
		close(releases[1])
		synctest.Wait()
		gateRuntimeResult(t, results[1])
		gateRuntimeExpired(t, shortDone, 0)
	})
}

func TestSenderGateRuntimeDeadline429(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := gateRuntimeDeps(t)
		d.MaxRetries = 2
		s := gateRuntimeStart(t, d)
		defer gateRuntimeStop(t, s)
		r, done := gateRuntimeRequest(Deadline, 6, "approveChatJoinRequest", time.Now().Add(time.Second))
		r.Do = func(context.Context, *bot.Bot) error { return &bot.TooManyRequestsError{RetryAfter: 2} }
		gateRuntimeEnqueue(t, s, r)
		synctest.Wait()
		gateRuntimeExpired(t, done, 1)
	})
}

func TestSenderGateRuntimeDeadline429Escalation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := gateRuntimeDeps(t)
		d.MaxRetries = 1
		s := gateRuntimeStart(t, d)
		defer gateRuntimeStop(t, s)
		for i, tc := range []struct {
			retryAfterSeconds int
			remaining         time.Duration
		}{
			{8, 500 * time.Millisecond},
			{1, 500 * time.Millisecond},
			{1, 2 * time.Second},
		} {
			r, done := gateRuntimeRequest(Deadline, int64(i+1), "approveChatJoinRequest", time.Now().Add(tc.remaining))
			r.Do = func(context.Context, *bot.Bot) error {
				return &bot.TooManyRequestsError{RetryAfter: tc.retryAfterSeconds}
			}
			gateRuntimeEnqueue(t, s, r)
			synctest.Wait()
			gateRuntimeExpired(t, done, 1)
		}
	})
}

func TestSenderGateRuntimeRefundBeforeAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := gateRuntimeDeps(t)
		d.GlobalRPS, d.GlobalBurst = 0.1, 1
		s := gateRuntimeStart(t, d)
		defer gateRuntimeStop(t, s)
		began := time.Now()
		r, done := gateRuntimeRequest(Background, -7, "sendMessage", time.Time{})
		calledAt := make(chan time.Time, 1)
		r.Do = func(context.Context, *bot.Bot) error {
			calledAt <- time.Now()
			return nil
		}
		s.mu.Lock()
		s.onDispatch = func(now time.Time, entry *item) {
			s.onDispatch = nil
			s.record429(now, entry.request.ChatID, entry.method, time.Second)
		}
		s.mu.Unlock()
		gateRuntimeEnqueue(t, s, r)
		synctest.Wait()
		gateRuntimePending(t, done)
		s.mu.Lock()
		globalAvailable := s.global.has(time.Now())
		chatAvailable := s.chatBuckets.has(time.Now(), r.ChatID, classifyMethod(r.Method))
		s.mu.Unlock()
		if !globalAvailable || !chatAvailable {
			t.Errorf("tokens available during pause: global=%t, chat=%t; want both", globalAvailable, chatAvailable)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if r := gateRuntimeResult(t, done); r.Outcome.Class != telegram.OK || r.Attempts != 1 {
			t.Fatalf("request after recovery = %+v", r)
		}
		select {
		case at := <-calledAt:
			if elapsed := at.Sub(began); elapsed != time.Second {
				t.Fatalf("Do started after %v, want 1s", elapsed)
			}
		default:
			t.Fatal("Do was not called")
		}
	})
}

func TestSenderGateRuntime429Requeues(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := gateRuntimeDeps(t)
		d.MaxRetries, d.GlobalRPS, d.GlobalBurst = 1, 1, 1
		s := gateRuntimeStart(t, d)
		defer gateRuntimeStop(t, s)
		release := make(chan struct{})
		first, firstDone := gateRuntimeRequest(Decision, 4, "getChat", time.Time{})
		var attempts int
		first.Do = func(ctx context.Context, _ *bot.Bot) error {
			attempts++
			if attempts > 1 {
				return nil
			}
			select {
			case <-release:
				return &bot.TooManyRequestsError{RetryAfter: 2}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		gateRuntimeEnqueue(t, s, first)
		synctest.Wait()
		second, secondDone := gateRuntimeRequest(Decision, 4, "getChat", time.Time{})
		gateRuntimeEnqueue(t, s, second)
		close(release)
		synctest.Wait()
		if got := s.ClassWait(Decision); got != 2*time.Second {
			t.Errorf("Decision backlog during the 429 pause = %v, want 2s", got)
		}
		time.Sleep(2*time.Second - time.Nanosecond)
		synctest.Wait()
		gateRuntimePending(t, firstDone)
		gateRuntimePending(t, secondDone)
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if r := gateRuntimeResult(t, firstDone); r.Outcome.Class != telegram.OK || r.Attempts != 2 || r.Ambiguous {
			t.Fatalf("retried head = %+v", r)
		}
		gateRuntimePending(t, secondDone)
		time.Sleep(time.Second)
		synctest.Wait()
		if r := gateRuntimeResult(t, secondDone); r.Outcome.Class != telegram.OK || r.Attempts != 1 {
			t.Fatalf("request behind retried head = %+v", r)
		}
	})
}

func TestSenderGateRuntimeNoSpin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := gateRuntimeDeps(t)
		d.GlobalRPS, d.GlobalBurst = 0.1, 5
		s := gateRuntimeStart(t, d)
		defer gateRuntimeStop(t, s)
		now := time.Now()
		first, firstDone := gateRuntimeRequest(Background, -7, "sendMessage", time.Time{})
		gateRuntimeEnqueue(t, s, first)
		synctest.Wait()
		gateRuntimeResult(t, firstDone)
		s.mu.Lock()
		loops := s.loops
		s.mu.Unlock()
		second, secondDone := gateRuntimeRequest(Background, -7, "editMessageText", time.Time{})
		gateRuntimeEnqueue(t, s, second)
		synctest.Wait()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		gateRuntimePending(t, secondDone)
		s.mu.Lock()
		blockedLoops := s.loops - loops
		tokens, _ := s.global.balance(time.Now())
		s.mu.Unlock()
		if blockedLoops == 0 || blockedLoops > 4 || tokens != d.GlobalBurst-1 {
			t.Fatalf("blocked across %v: loops=%d, tokens=%d; want 1–4 loops, %d tokens", time.Since(now), blockedLoops, tokens, d.GlobalBurst-1)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if r := gateRuntimeResult(t, secondDone); r.Outcome.Class != telegram.OK {
			t.Fatalf("chat bucket recovery result = %+v", r)
		}
	})
}

type senderTestLogWriter func([]byte) (int, error)

func (w senderTestLogWriter) Write(p []byte) (int, error) { return w(p) }

func TestSenderGateRuntimeRaidWeight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := gateRuntimeDeps(t)
		d.GlobalBurst, d.ClassWeights = 1, [4]int{1, 1, 1, 1}
		var weightLogs bytes.Buffer
		var s *Sender
		d.Log = slog.New(slog.NewJSONHandler(senderTestLogWriter(func(p []byte) (int, error) {
			s.EstimateWait(Deadline, 1)
			return weightLogs.Write(p)
		}), nil))
		s = gateRuntimeStart(t, d)
		defer gateRuntimeStop(t, s)
		release := make(chan struct{})
		blocker, _ := gateRuntimeRequest(Background, 8, "getChat", time.Time{})
		blocker.Do = func(ctx context.Context, _ *bot.Bot) error {
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		gateRuntimeEnqueue(t, s, blocker)
		synctest.Wait()
		until := time.Now().Add(time.Minute)
		s.SetChatWeight(1, 0.25, until)
		if weightLogs.Len() != 0 {
			t.Errorf("valid chat weight produced a warning: %s", weightLogs.String())
		}
		s.SetChatWeight(1, math.NaN(), until)
		logs := weightLogs.String()
		if strings.Count(logs, "\"level\":\"WARN\"") != 1 || !strings.Contains(logs, "invalid chat weight") {
			t.Errorf("chat weight logs = %s, want one warning for the invalid weight", logs)
		}
		deadlineIDs := make(chan int64, 16)
		interactiveIDs := make(chan int64, 16)
		for _, tc := range []struct {
			class  Class
			method string
			ids    chan int64
		}{
			{Deadline, "approveChatJoinRequest", deadlineIDs},
			{Interactive, "getChat", interactiveIDs},
		} {
			for _, chatID := range []int64{1, 2} {
				for range 8 {
					r, _ := gateRuntimeRequest(tc.class, chatID, tc.method, until)
					r.Do = func(context.Context, *bot.Bot) error { tc.ids <- chatID; return nil }
					gateRuntimeEnqueue(t, s, r)
				}
			}
		}
		close(release)
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		firstFourRaid := func(ids chan int64) int {
			t.Helper()
			if len(ids) < 4 {
				t.Fatalf("got %d attempts, want at least 4", len(ids))
			}
			raid := 0
			for range 4 {
				if <-ids == 1 {
					raid++
				}
			}
			return raid
		}
		if raid := firstFourRaid(deadlineIDs); raid > 1 {
			t.Errorf("raid chat got %d of first 4 deadline attempts, want at most 1", raid)
		}
		if raid := firstFourRaid(interactiveIDs); raid != 2 {
			t.Errorf("raid chat got %d of first 4 interactive attempts, want 2", raid)
		}
	})
}
