package sender

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-telegram/bot"
)

func TestSenderHealthCheck(t *testing.T) {
	type rateLimit struct {
		method  string
		seconds int
	}
	for _, tc := range []struct {
		name       string
		rateLimits []rateLimit
		recoverIn  time.Duration
	}{
		{
			name:       "explicit global pause",
			rateLimits: []rateLimit{{"getChat", 31}},
			recoverIn:  31 * time.Second,
		},
		{
			name: "all method classes paused",
			rateLimits: []rateLimit{
				{"approveChatJoinRequest", 9},
				{"sendMessage", 5},
				{"getChat", 7},
			},
			recoverIn: 5 * time.Second,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := gateRuntimeStart(t, gateRuntimeDeps(t))
				defer gateRuntimeStop(t, s)
				requireSenderHealthy(t, s)
				time.Sleep(125 * time.Millisecond)
				recovery := time.Now().Add(tc.recoverIn)
				for i, limit := range tc.rateLimits {
					enqueueHealthRateLimit(t, s, limit.method, limit.seconds)
					if i < len(tc.rateLimits)-1 {
						requireSenderHealthy(t, s)
					}
				}
				requireSenderDegraded(t, s.HealthCheck(), recovery)
				time.Sleep(tc.recoverIn - time.Nanosecond)
				requireSenderDegraded(t, s.HealthCheck(), recovery)
				time.Sleep(time.Nanosecond)
				requireSenderHealthy(t, s)
				gateRuntimeStop(t, s)
				requireSenderHealthy(t, s)
				t.Logf("global recovery after %v at %s", tc.recoverIn, recovery.UTC().Format(time.RFC3339Nano))
			})
		})
	}
}

func enqueueHealthRateLimit(t *testing.T, s *Sender, method string, retryAfterSeconds int) {
	t.Helper()
	request, done := gateRuntimeRequest(Decision, 0, method, time.Time{})
	request.Do = func(context.Context, *bot.Bot) error {
		return &bot.TooManyRequestsError{RetryAfter: retryAfterSeconds}
	}
	gateRuntimeEnqueue(t, s, request)
	synctest.Wait()
	if result := gateRuntimeResult(t, done); result.Outcome.RetryAfter != time.Duration(retryAfterSeconds)*time.Second {
		t.Fatalf("%s result = %+v, want a %ds 429", method, result, retryAfterSeconds)
	}
}

func TestSenderHealthCheckExtendedDerivedGlobalPause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := gateRuntimeDeps(t)
		deps.Workers = 2
		s := gateRuntimeStart(t, deps)
		defer gateRuntimeStop(t, s)
		start := time.Now()
		const messageMethod = "sendMessage"
		const extensionRetryAfter = 8
		started, release := make(chan struct{}), make(chan struct{})
		request, done := gateRuntimeRequest(Decision, 0, messageMethod, time.Time{})
		request.Do = func(ctx context.Context, _ *bot.Bot) error {
			close(started)
			select {
			case <-release:
				return &bot.TooManyRequestsError{RetryAfter: extensionRetryAfter}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		gateRuntimeEnqueue(t, s, request)
		synctest.Wait()
		select {
		case <-started:
		default:
			t.Fatal("message request did not start before its method class was paused")
		}

		enqueueHealthRateLimit(t, s, "approveChatJoinRequest", 9)
		enqueueHealthRateLimit(t, s, messageMethod, 5)
		enqueueHealthRateLimit(t, s, "getChat", 7)
		firstRecovery := start.Add(5 * time.Second)
		extendedRecovery := start.Add(7 * time.Second)
		requireSenderDegraded(t, s.HealthCheck(), firstRecovery)

		time.Sleep(time.Until(start.Add(time.Second)))
		close(release)
		synctest.Wait()
		if result := gateRuntimeResult(t, done); result.Outcome.RetryAfter != extensionRetryAfter*time.Second {
			t.Fatalf("in-flight message result = %+v, want a %ds 429", result, extensionRetryAfter)
		}
		requireSenderDegraded(t, s.HealthCheck(), extendedRecovery)
		time.Sleep(time.Until(firstRecovery))
		requireSenderDegraded(t, s.HealthCheck(), extendedRecovery)
		time.Sleep(time.Until(extendedRecovery))
		requireSenderHealthy(t, s)
		t.Logf("derived global recovery extended from %s to %s", firstRecovery.UTC().Format(time.RFC3339), extendedRecovery.UTC().Format(time.RFC3339))
	})
}

func TestSenderHealthCheckExtendedGlobalPause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := gateRuntimeDeps(t)
		deps.Workers = 2
		s := gateRuntimeStart(t, deps)
		defer gateRuntimeStop(t, s)
		start := time.Now()
		replies := []struct {
			after             time.Duration
			retryAfterSeconds int
		}{
			{0, 31},
			{time.Second, 40},
		}
		started := make(chan struct{}, len(replies))
		release := make(chan struct{})
		completed := make([]<-chan Result, len(replies))
		for i, reply := range replies {
			request, done := gateRuntimeRequest(Decision, int64(i+1), "getChat", time.Time{})
			request.Do = func(ctx context.Context, _ *bot.Bot) error {
				started <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
				time.Sleep(reply.after)
				return &bot.TooManyRequestsError{RetryAfter: reply.retryAfterSeconds}
			}
			completed[i] = done
			gateRuntimeEnqueue(t, s, request)
		}
		synctest.Wait()
		if got := len(started); got != len(replies) {
			t.Fatalf("%d requests started, want %d in flight before the first 429", got, len(replies))
		}
		close(release)
		for i, reply := range replies {
			receivedAt := start.Add(reply.after)
			time.Sleep(time.Until(receivedAt))
			synctest.Wait()
			retryAfter := time.Duration(reply.retryAfterSeconds) * time.Second
			if result := gateRuntimeResult(t, completed[i]); result.Outcome.RetryAfter != retryAfter {
				t.Fatalf("result at %v = %+v, want a 429 with retry_after=%v", reply.after, result, retryAfter)
			}
			requireSenderDegraded(t, s.HealthCheck(), receivedAt.Add(retryAfter))
		}

		firstRecovery := start.Add(31 * time.Second)
		extendedRecovery := start.Add(41 * time.Second)
		time.Sleep(time.Until(firstRecovery))
		requireSenderDegraded(t, s.HealthCheck(), extendedRecovery)
		time.Sleep(time.Until(extendedRecovery))
		requireSenderHealthy(t, s)
	})
}

func TestSenderHealthCheckConcurrentGlobalPause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := gateRuntimeStart(t, gateRuntimeDeps(t))
		defer gateRuntimeStop(t, s)
		const retryAfterSeconds = 31
		recovery := time.Now().Add(retryAfterSeconds * time.Second)
		release := make(chan struct{})
		request, done := gateRuntimeRequest(Decision, 1, "getChat", time.Time{})
		request.Do = func(ctx context.Context, _ *bot.Bot) error {
			select {
			case <-release:
				return &bot.TooManyRequestsError{RetryAfter: retryAfterSeconds}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		gateRuntimeEnqueue(t, s, request)
		checked := make(chan error, 1)
		go func() {
			<-release
			checked <- s.HealthCheck()
		}()
		synctest.Wait()
		close(release)
		synctest.Wait()

		if err := <-checked; err != nil {
			requireSenderDegraded(t, err, recovery)
		}
		if result := gateRuntimeResult(t, done); result.Outcome.RetryAfter != retryAfterSeconds*time.Second {
			t.Fatalf("result = %+v, want a %ds 429", result, retryAfterSeconds)
		}
		requireSenderDegraded(t, s.HealthCheck(), recovery)
	})
}

func requireSenderHealthy(t *testing.T, s *Sender) {
	t.Helper()
	if err := s.HealthCheck(); err != nil {
		t.Fatalf("HealthCheck() = %v, want nil", err)
	}
}

func requireSenderDegraded(t *testing.T, err error, recovery time.Time) {
	t.Helper()
	timestamp := recovery.UTC().Format(time.RFC3339Nano)
	if err == nil || !strings.HasPrefix(err.Error(), "degraded:") || !strings.Contains(err.Error(), timestamp) {
		t.Fatalf("HealthCheck() = %v, want degraded with UTC recovery %s", err, timestamp)
	}
}
