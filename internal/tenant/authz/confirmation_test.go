package authz_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot"

	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
	"github.com/INKCR0W/yabot/internal/tenant/authz"
	"github.com/INKCR0W/yabot/internal/tenant/chats"
)

type confirmation struct {
	creator, anonymous bool
	err                error
}

func startConfirmation(z *authz.Authz, ctx context.Context) <-chan confirmation {
	result := make(chan confirmation, 1)
	go func() {
		creator, anonymous, err := z.ConfirmOwner(ctx, userID, chatID)
		result <- confirmation{creator, anonymous, err}
	}()
	return result
}

func checkConfirmation(t *testing.T, ch <-chan confirmation, want confirmation) {
	t.Helper()
	got := receive(t, ch)
	if got.creator != want.creator || got.anonymous != want.anonymous || !errors.Is(got.err, want.err) {
		t.Fatalf("ConfirmOwner = %+v, want %+v", got, want)
	}
}

func TestConcurrentEditsAcrossGroupsShareCheck(t *testing.T) {
	for _, tt := range []struct {
		name string
		drop bool
		err  error
	}{
		{name: "creator"},
		{name: "unavailable", drop: true, err: authz.ErrUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				first := startEdit(f.check, t.Context(), userID, chatID, "general")
				request := f.request(t, chatID)
				second := startEdit(f.check, t.Context(), userID, chatID, "")
				synctest.Wait()
				if len(first) != 0 || len(second) != 0 {
					t.Error("Edit returned before the shared sender result")
				}
				requests := []sender.Request{request}
				for len(f.pending) > 0 {
					requests = append(requests, f.request(t, chatID))
				}
				if len(requests) != 1 {
					t.Errorf("submitted %d ownership checks, want 1", len(requests))
				}
				for _, request := range requests {
					if tt.drop {
						request.Done(sender.DroppedResult(sender.Expired, nil))
					} else if err := complete(t, request, userID, chatID, creatorBody(userID, false)); err != nil {
						t.Error(err)
					}
				}
				checkEdit(t, first, !tt.drop, tt.err)
				checkEdit(t, second, !tt.drop, tt.err)
				decision, check := "ok", "creator"
				if tt.drop {
					decision, check = "unavailable", "expired"
				}
				assertCounter(t, f.metrics, fmt.Sprintf(`yabot_authz_owner_checks_total{outcome=%q}`, check), 1)
				assertCounter(t, f.metrics, fmt.Sprintf(`yabot_authz_decisions_total{op="edit",outcome=%q}`, decision), 2)

				next := startEdit(f.check, t.Context(), userID, chatID, "general")
				if err := complete(t, f.request(t, chatID), userID, chatID, creatorBody(userID, true)); err != nil {
					t.Fatal(err)
				}
				checkEdit(t, next, false, nil)
				f.assertIdle(t)
			})
		})
	}
}

func TestEditsForDifferentUsersOrChatsUseSeparateChecks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.snapshots[chatID-1] = chats.ChatSnapshot{State: chats.Active, OwnerID: userID}
		first := startEdit(f.check, t.Context(), userID, chatID, "general")
		firstRequest := f.request(t, chatID)
		otherChat := startEdit(f.check, t.Context(), userID, chatID-1, "general")
		otherChatRequest := f.request(t, chatID-1)

		f.snapshots[chatID] = chats.ChatSnapshot{State: chats.Active, OwnerID: userID + 1}
		newOwner := startEdit(f.check, t.Context(), userID+1, chatID, "general")
		newOwnerRequest := f.request(t, chatID)

		if err := complete(t, newOwnerRequest, userID+1, chatID, creatorBody(userID+1, true)); err != nil {
			t.Fatal(err)
		}
		if err := complete(t, otherChatRequest, userID, chatID-1, creatorBody(userID, false)); err != nil {
			t.Fatal(err)
		}
		if err := complete(t, firstRequest, userID, chatID, administratorBody); err != nil {
			t.Fatal(err)
		}
		checkEdit(t, newOwner, false, nil)
		checkEdit(t, otherChat, true, nil)
		checkEdit(t, first, false, authz.ErrForbidden)
		if id := receive(t, f.stale); id != chatID {
			t.Fatalf("stale owner report for %d, want %d", id, chatID)
		}
		f.assertIdle(t)
	})
}

func TestEditCancellationLeavesSharedCheckRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		first := startEdit(f.check, ctx, userID, chatID, "general")
		request := f.request(t, chatID)
		second := startEdit(f.check, t.Context(), userID, chatID, "general")
		f.assertIdle(t)
		cancel()
		checkEdit(t, first, false, context.Canceled)
		if len(second) != 0 {
			t.Fatal("the second Edit returned before the sender completed")
		}
		if err := complete(t, request, userID, chatID, creatorBody(userID, false)); err != nil {
			t.Fatal(err)
		}
		checkEdit(t, second, true, nil)

		ctx, cancelExpired := context.WithTimeout(t.Context(), 0)
		defer cancelExpired()
		checkEdit(t, startEdit(f.check, ctx, userID, chatID, "general"), false, context.DeadlineExceeded)
		f.assertIdle(t)
		assertCounter(t, f.metrics, `yabot_authz_decisions_total{op="edit",outcome="canceled"}`, 2)
	})
}

func TestConfirmOwnerUsesIndependentChecks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		edit := startEdit(f.check, t.Context(), userID, chatID, "general")
		editRequest := f.request(t, chatID)
		delete(f.snapshots, chatID)
		claim := startConfirmation(f.check, t.Context())
		claimRequest := f.request(t, chatID)
		otherClaim := startConfirmation(f.check, t.Context())
		otherRequest := f.request(t, chatID)
		if err := complete(t, claimRequest, userID, chatID, administratorBody); err != nil {
			t.Fatal(err)
		}
		checkConfirmation(t, claim, confirmation{})
		if err := complete(t, otherRequest, userID, chatID, creatorBody(userID, true)); err != nil {
			t.Fatal(err)
		}
		checkConfirmation(t, otherClaim, confirmation{creator: true, anonymous: true})
		if err := complete(t, editRequest, userID, chatID, creatorBody(userID, false)); err != nil {
			t.Fatal(err)
		}
		checkEdit(t, edit, true, nil)

		claim = startConfirmation(f.check, t.Context())
		if err := complete(t, f.request(t, chatID), userID, chatID, creatorBody(userID, false)); err != nil {
			t.Fatal(err)
		}
		checkConfirmation(t, claim, confirmation{creator: true})
		f.assertIdle(t)
	})
}

func TestCanceledConfirmationStillObservesSenderExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := startConfirmation(f.check, ctx)
		request := f.request(t, chatID)
		cancel()
		checkConfirmation(t, done, confirmation{err: context.Canceled})
		assertCounter(t, f.metrics, `yabot_authz_owner_checks_total{outcome="expired"}`, 0)
		request.Done(sender.DroppedResult(sender.Expired, nil))
		f.assertIdle(t)
		assertCounter(t, f.metrics, `yabot_authz_owner_checks_total{outcome="canceled"}`, 1)
		assertCounter(t, f.metrics, `yabot_authz_owner_checks_total{outcome="expired"}`, 1)
		var expiryLogs int
		for line := range strings.SplitSeq(f.logs.String(), "\n") {
			if strings.Contains(line, `"level":"INFO"`) && strings.Contains(line, `"outcome":"expired"`) {
				expiryLogs++
			}
		}
		if expiryLogs != 1 {
			t.Errorf("expected one expiry INFO log, got %d in %s", expiryLogs, &f.logs)
		}
	})
}

type loggerFunc func()

func (f loggerFunc) Log(context.Context, slog.Level, string, ...any) { f() }

func TestEditSettlesBeforeOwnershipLoggerReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		requests := make(chan sender.Request, 2)
		logging := make(chan struct{}, 4)
		releaseLog := make(chan struct{})
		defer close(releaseLog)
		counters := metrics.NewSet()
		z := newAuthz(t, authz.Deps{
			Chats:    chatSnapshots{chatID: {State: chats.Active, OwnerID: userID}},
			Reporter: reporterFunc(func(id int64) { t.Errorf("unexpected stale report for %d", id) }),
			Sender:   senderFunc(func(r sender.Request) error { requests <- r; return nil }),
			Metrics:  counters,
			Log: loggerFunc(func() {
				logging <- struct{}{}
				<-releaseLog
			}),
		})
		first := startEdit(z, t.Context(), userID, chatID, "general")
		request := receive(t, requests)
		go request.Done(sender.DroppedResult(sender.Expired, nil))
		receive(t, logging)
		checkEdit(t, first, false, authz.ErrUnavailable)

		second := startEdit(z, t.Context(), userID, chatID, "general")
		request = receive(t, requests)
		if err := complete(t, request, userID, chatID, creatorBody(userID, false)); err != nil {
			t.Fatal(err)
		}
		checkEdit(t, second, true, nil)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		canceled := startEdit(z, ctx, userID, chatID, "general")
		request = receive(t, requests)
		cancel()
		receive(t, logging)
		checkEdit(t, canceled, false, context.Canceled)
		request.Done(sender.DroppedResult(sender.Shutdown, nil))
		receive(t, logging)

		assertCounter(t, counters, `yabot_authz_owner_checks_total{outcome="expired"}`, 1)
		assertCounter(t, counters, `yabot_authz_owner_checks_total{outcome="creator"}`, 1)
		assertCounter(t, counters, `yabot_authz_owner_checks_total{outcome="shutdown"}`, 1)
		assertCounter(t, counters, `yabot_authz_decisions_total{op="edit",outcome="canceled"}`, 1)
		if len(logging) != 0 || len(requests) != 0 {
			t.Fatal("unexpected additional ownership log or request")
		}
	})
}

func TestConfirmOwnerSettlesWhileLoggerIsBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		requests := make(chan sender.Request, 1)
		logging := make(chan struct{}, 4)
		releaseLog := make(chan struct{})
		defer close(releaseLog)
		counters := metrics.NewSet()
		z := newAuthz(t, authz.Deps{
			Chats:    chatSnapshots{},
			Reporter: reporterFunc(func(id int64) { t.Errorf("unexpected stale report for %d", id) }),
			Sender:   senderFunc(func(r sender.Request) error { requests <- r; return nil }),
			Metrics:  counters,
			Log: loggerFunc(func() {
				logging <- struct{}{}
				<-releaseLog
			}),
		})
		check := startConfirmation(z, t.Context())
		request := receive(t, requests)
		returned := make(chan struct{})
		go func() {
			request.Done(sender.DroppedResult(sender.Expired, nil))
			close(returned)
		}()
		receive(t, logging)
		receive(t, returned)
		checkConfirmation(t, check, confirmation{err: authz.ErrUnavailable})

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		check = startConfirmation(z, ctx)
		request = receive(t, requests)
		cancel()
		receive(t, logging)
		checkConfirmation(t, check, confirmation{err: context.Canceled})
		request.Done(sender.DroppedResult(sender.Expired, nil))
		receive(t, logging)
		assertCounter(t, counters, `yabot_authz_owner_checks_total{outcome="expired"}`, 2)
		assertCounter(t, counters, `yabot_authz_owner_checks_total{outcome="canceled"}`, 1)
		if len(logging) != 0 || len(requests) != 0 {
			t.Fatal("unexpected additional ownership log or request")
		}
	})
}

func TestCanceledConfirmationStillSubmitsOnce(t *testing.T) {
	for _, enqueueErr := range []error{nil, sender.ErrClosed} {
		calls := 0
		z := newAuthz(t, authz.Deps{
			Chats:    chatSnapshots{},
			Reporter: reporterFunc(func(id int64) { t.Errorf("unexpected stale report for %d", id) }),
			Sender: senderFunc(func(r sender.Request) error {
				calls++
				if enqueueErr == nil {
					r.Done(sender.DroppedResult(sender.Expired, nil))
				}
				return enqueueErr
			}),
		})
		ctx, cancel := context.WithTimeout(t.Context(), 0)
		creator, anonymous, err := z.ConfirmOwner(ctx, userID, chatID)
		cancel()
		if creator || anonymous || !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
			t.Errorf("ConfirmOwner with enqueue error %v = (%t, %t, %v), submissions = %d", enqueueErr, creator, anonymous, err, calls)
		}
	}
}

func TestNonOKChecksAreUnavailable(t *testing.T) {
	for _, tt := range []struct {
		result  sender.Result
		outcome string
		level   string
	}{
		{sender.Result{Outcome: telegram.Outcome{Class: telegram.Retry}}, "retry", "INFO"},
		{sender.DroppedResult(sender.Shutdown, nil), "shutdown", "INFO"},
		{sender.Result{Outcome: telegram.Outcome{Class: telegram.Done, Reason: telegram.ChatGone}}, "chat_gone", "INFO"},
		{sender.Result{Outcome: telegram.Outcome{Class: telegram.Forbidden}}, "forbidden", "INFO"},
		{sender.Result{Outcome: telegram.Outcome{Class: telegram.Permission}}, "permission", "INFO"},
		{sender.Result{Outcome: telegram.Outcome{Class: telegram.Migrated}}, "migrated", "INFO"},
		{sender.Result{Outcome: telegram.Outcome{Class: telegram.Fatal}}, "fatal", "ERROR"},
		{sender.Result{Outcome: telegram.Outcome{Class: telegram.Conflict}}, "conflict", "ERROR"},
		{sender.Result{Outcome: telegram.Outcome{Class: telegram.Unauthorized}}, "unauthorized", "ERROR"},
		{sender.Result{Outcome: telegram.Outcome{Class: telegram.Unknown}}, "unknown", "ERROR"},
	} {
		t.Run(tt.outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				edit := startEdit(f.check, t.Context(), userID, chatID, "general")
				f.request(t, chatID).Done(tt.result)
				checkEdit(t, edit, false, authz.ErrUnavailable)
				claim := startConfirmation(f.check, t.Context())
				f.request(t, chatID).Done(tt.result)
				checkConfirmation(t, claim, confirmation{err: authz.ErrUnavailable})
				assertCounter(t, f.metrics, fmt.Sprintf(`yabot_authz_owner_checks_total{outcome=%q}`, tt.outcome), 2)
				f.assertIdle(t)
				if got := strings.Count(f.logs.String(), fmt.Sprintf(`"level":%q`, tt.level)); got != 2 {
					t.Errorf("expected two %s logs, got %d in %s", tt.level, got, &f.logs)
				}
			})
		})
	}
}

func TestRejectedOwnerCheckLogsSafeOutcome(t *testing.T) {
	for _, tt := range []struct {
		err     error
		outcome string
		level   string
	}{
		{sender.ErrBusy, "busy", "INFO"},
		{sender.ErrWouldMiss, "would_miss", "INFO"},
		{sender.ErrClosed, "shutdown", "INFO"},
		{errors.New("private request data"), "enqueue_error", "ERROR"},
	} {
		t.Run(tt.outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var logs bytes.Buffer
				counters := metrics.NewSet()
				z := newAuthz(t, authz.Deps{
					Chats:    chatSnapshots{chatID: {State: chats.Active, OwnerID: userID}},
					Reporter: reporterFunc(func(id int64) { t.Errorf("unexpected stale report for %d", id) }),
					Sender:   senderFunc(func(sender.Request) error { return tt.err }),
					Log:      slog.New(slog.NewJSONHandler(&logs, nil)),
					Metrics:  counters,
				})
				checkEdit(t, startEdit(z, t.Context(), userID, chatID, ""), false, authz.ErrUnavailable)
				log := logs.String()
				for _, field := range []string{
					fmt.Sprintf(`"level":%q`, tt.level), fmt.Sprintf(`"outcome":%q`, tt.outcome),
					fmt.Sprintf(`"user_id":%d`, userID), fmt.Sprintf(`"chat_id":%d`, chatID),
				} {
					if !strings.Contains(log, field) {
						t.Errorf("missing %s in %s", field, log)
					}
				}
				if strings.Contains(log, tt.err.Error()) {
					t.Errorf("log contains the raw enqueue error: %s", log)
				}
				assertCounter(t, counters, fmt.Sprintf(`yabot_authz_owner_checks_total{outcome=%q}`, tt.outcome), 1)
			})
		})
	}
}

func TestSDKFailureKeepsClassification(t *testing.T) {
	for _, tt := range []struct {
		body       string
		want       telegram.Outcome
		diagnostic string
	}{
		{`{"ok":false,"error_code":429,"description":"API rate limit","parameters":{"retry_after":2}}`,
			telegram.Outcome{Class: telegram.Retry, RetryAfter: 2 * time.Second}, "API rate limit"},
		{`{"ok":false,"error_code":400,"description":"API chat migration","parameters":{"migrate_to_chat_id":-123}}`,
			telegram.Outcome{Class: telegram.Migrated, MigrateTo: -123}, "API chat migration"},
		{`{"ok":false,"error_code":500,"description":"private response fragment"}`, telegram.Outcome{Class: telegram.Retry},
			"getChatMember failed (*errors.errorString)"},
		{`{"ok":true,"result":null}`, telegram.Outcome{Class: telegram.Fatal}, "getChatMember returned no member"},
	} {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(t)
			check := startConfirmation(f.check, t.Context())
			err := complete(t, f.request(t, chatID), userID, chatID, tt.body)
			if got := telegram.Classify(err); got != tt.want {
				t.Fatalf("SDK result = %v, classified as %+v, want %+v", err, got, tt.want)
			}
			if err == nil || err.Error() != tt.diagnostic {
				t.Errorf("SDK result = %v, want %q", err, tt.diagnostic)
			}
			checkConfirmation(t, check, confirmation{err: authz.ErrUnavailable})
			f.assertIdle(t)
		})
	}
}

func TestSenderLogsAPIErrorDetailsWithoutResponseBodiesOrTokens(t *testing.T) {
	const token = "123:fake-token"
	const rawBodyMarker = "private response fragment"
	const displayName = "test display name"
	for _, tt := range []struct {
		name         string
		body         string
		want         string
		networkError error
	}{
		{name: "bad request", body: fmt.Sprintf(`{"ok":false,"error_code":400,"description":"Bad Request: USER_ID_INVALID (%s, %s, %s)"}`,
			token, url.QueryEscape(token), strings.ReplaceAll(url.QueryEscape(token), "%3A", "%3a")),
			want: "Bad Request: USER_ID_INVALID (***, ***, ***)"},
		{name: "not found", body: `{"ok":false,"error_code":404,"description":"Not Found: method unavailable"}`,
			want: "Not Found: method unavailable"},
		{name: "other API code", body: `{"ok":false,"error_code":418,"description":"` + rawBodyMarker + `"}`,
			want: "getChatMember failed (*errors.errorString)"},
		{name: "missing API code", body: `{"description":"` + displayName + `"}`, want: "getChatMember failed (*errors.errorString)"},
		{name: "zero API code", body: `{"error_code":0,"description":"` + displayName + `"}`, want: "getChatMember failed (*errors.errorString)"},
		{name: "truncated envelope", body: `{"ok":false,"description":"` + rawBodyMarker, want: "getChatMember failed (*json.SyntaxError)"},
		{name: "invalid result", body: `{"ok":true,"result":{"status":"creator","is_anonymous":"` + rawBodyMarker + `"}}`,
			want: "getChatMember failed (*json.UnmarshalTypeError)"},
		{name: "network error with API-like text", networkError: errors.New("error response from telegram for method getChatMember, 418 " + rawBodyMarker),
			want: "getChatMember failed (*url.Error)"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
					if tt.networkError != nil {
						return nil, tt.networkError
					}
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(tt.body))}, nil
				})}
				b, err := bot.New(token, bot.WithSkipGetMe(), bot.WithHTTPClient(time.Second, client))
				if err != nil {
					t.Fatal(err)
				}
				var logs bytes.Buffer
				s, err := sender.New(sender.Deps{
					Bot: b, Log: slog.New(slog.NewJSONHandler(&logs, nil)),
					Workers: 1, CapInteractive: 1, CapBackground: 1,
					GlobalRPS: 25, GlobalBurst: 1, ChatQueue: 1, ClassWeights: [4]int{8, 4, 2, 1},
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Start(t.Context()); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := s.Stop(context.Background()); err != nil {
						t.Error(err)
					}
				}()
				z := newAuthz(t, authz.Deps{
					Chats: chatSnapshots{}, Sender: s,
					Reporter: reporterFunc(func(id int64) { t.Errorf("unexpected stale report for %d", id) }),
				})
				checkConfirmation(t, startConfirmation(z, t.Context()), confirmation{err: authz.ErrUnavailable})
				log := logs.String()
				if !strings.Contains(log, `"level":"ERROR"`) || !strings.Contains(log, tt.want) {
					t.Errorf("sender logs = %s, want ERROR with %q", log, tt.want)
				}
				for _, private := range []string{token, url.QueryEscape(token), strings.ReplaceAll(url.QueryEscape(token), "%3A", "%3a"), rawBodyMarker, displayName} {
					if strings.Contains(log, private) {
						t.Errorf("sender logs contain unsanitized text %q: %s", private, log)
					}
				}
			})
		})
	}
}
