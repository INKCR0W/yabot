package authz_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
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

const (
	userID int64 = 42
	chatID int64 = -1001234567890
)

type chatSnapshots map[int64]chats.ChatSnapshot

func (s chatSnapshots) State(id int64) (chats.ChatSnapshot, bool) {
	chat, exists := s[id]
	if !exists {
		return chats.ChatSnapshot{State: chats.Active, OwnerID: userID}, false
	}
	return chat, exists
}

func (s chatSnapshots) OwnedBy(int64) []chats.ChatRef { return nil }

type senderFunc func(sender.Request) error

func (f senderFunc) Enqueue(r sender.Request) error { return f(r) }

type reporterFunc func(int64)

func (f reporterFunc) ReportOwnerStale(id int64) { f(id) }

type editDecision struct {
	grant authz.Grant
	err   error
}

func startEdit(z *authz.Authz, ctx context.Context, user, chat int64, group string) <-chan editDecision {
	result := make(chan editDecision, 1)
	go func() {
		grant, err := z.Authorize(ctx, user, chat, group, authz.Edit)
		result <- editDecision{grant, err}
	}()
	return result
}

func checkEdit(t *testing.T, ch <-chan editDecision, public bool, wantErr error) {
	t.Helper()
	got := receive(t, ch)
	if got.grant.Public != public || !errors.Is(got.err, wantErr) {
		t.Fatalf("Edit = (%+v, %v), want (Public=%t, %v)", got.grant, got.err, public, wantErr)
	}
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	synctest.Wait()
	select {
	case result := <-ch:
		return result
	default:
		t.Fatal("expected a result before all goroutines blocked")
		var zero T
		return zero
	}
}

type fixture struct {
	check     *authz.Authz
	snapshots chatSnapshots
	pending   chan sender.Request
	stale     chan int64
	metrics   *metrics.Set
	logs      bytes.Buffer
	now       time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{
		snapshots: chatSnapshots{chatID: {State: chats.Active, OwnerID: userID}},
		pending:   make(chan sender.Request, 8),
		stale:     make(chan int64, 8),
		metrics:   metrics.NewSet(),
		now:       time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC),
	}
	f.check = newAuthz(t, authz.Deps{
		Chats: f.snapshots, Metrics: f.metrics, Now: func() time.Time { return f.now },
		Log:      slog.New(slog.NewJSONHandler(&f.logs, nil)),
		Reporter: reporterFunc(func(id int64) { f.stale <- id }),
		Sender:   senderFunc(func(r sender.Request) error { f.pending <- r; return nil }),
	})
	return f
}

func newAuthz(t *testing.T, deps authz.Deps) *authz.Authz {
	t.Helper()
	z, err := authz.New(deps)
	if err != nil {
		t.Fatal(err)
	}
	return z
}

func (f *fixture) request(t *testing.T, chat int64) sender.Request {
	t.Helper()
	r := receive(t, f.pending)
	if r.Method != "getChatMember" || r.ChatID != chat || r.Class != sender.Interactive || r.NoRetry ||
		!r.Deadline.Equal(f.now.Add(5*time.Second)) {
		t.Fatalf("invalid ownership check request: %+v", r)
	}
	return r
}

func (f *fixture) assertIdle(t *testing.T) {
	t.Helper()
	synctest.Wait()
	if len(f.pending) != 0 || len(f.stale) != 0 {
		t.Fatalf("unexpected requests=%d or stale reports=%d", len(f.pending), len(f.stale))
	}
}

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func complete(t *testing.T, r sender.Request, user, chat int64, body string) error {
	t.Helper()
	calls := 0
	client := &http.Client{Transport: transportFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if req.Method != http.MethodPost || !strings.HasSuffix(req.URL.Path, "/getChatMember") {
			t.Errorf("unexpected API request for %s %s", req.Method, req.URL.Path)
		}
		if err := req.ParseMultipartForm(0); err != nil {
			t.Errorf("parse getChatMember request: %v", err)
			return nil, err
		}
		if req.FormValue("user_id") != strconv.FormatInt(user, 10) || req.FormValue("chat_id") != strconv.FormatInt(chat, 10) {
			t.Errorf("getChatMember form = %v, want user_id=%d and chat_id=%d", req.Form, user, chat)
		}
		if err := req.Context().Err(); err != nil {
			t.Errorf("getChatMember received a canceled context: %v", err)
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	b, err := bot.New("123:test-token", bot.WithSkipGetMe(), bot.WithHTTPClient(time.Second, client))
	if err != nil {
		t.Fatal(err)
	}
	err = r.Do(context.Background(), b)
	if calls != 1 {
		t.Errorf("sent %d Bot API calls, want 1", calls)
	}
	r.Done(sender.Result{Outcome: telegram.Classify(err), Err: err, Attempts: 1})
	return err
}

func creatorBody(user int64, anonymous bool) string {
	return fmt.Sprintf(`{"ok":true,"result":{"status":"creator","user":{"id":%d},"is_anonymous":%t}}`, user, anonymous)
}

const administratorBody = `{"ok":true,"result":{"status":"administrator","user":{"id":42},"is_anonymous":false}}`

func TestAuthorizeSnapshot(t *testing.T) {
	tests := []struct {
		name   string
		state  chats.ChatState
		owner  int64
		actor  int64
		ops    []authz.Op
		want   error
		result string
	}{
		{"no record", chats.NoRecord, 0, userID, []authz.Op{authz.View, authz.Edit}, authz.ErrForbidden, "forbidden"},
		{"removed owner", chats.Removed, userID, userID, []authz.Op{authz.View, authz.Edit}, authz.ErrForbidden, "forbidden"},
		{"unknown state", "unknown", userID, userID, []authz.Op{authz.View, authz.Edit}, authz.ErrForbidden, "forbidden"},
		{"no owner", chats.Active, 0, userID, []authz.Op{authz.View, authz.Edit}, authz.ErrForbidden, "forbidden"},
		{"zero is not an owner", chats.Active, 0, 0, []authz.Op{authz.View, authz.Edit}, authz.ErrForbidden, "forbidden"},
		{"non-owner", chats.Active, userID, userID + 1, []authz.Op{authz.View, authz.Edit}, authz.ErrForbidden, "forbidden"},
		{"onboarding owner", chats.Onboarding, userID, userID, []authz.Op{authz.View, authz.Edit}, authz.ErrBotNotAdmin, "bot_not_admin"},
		{"onboarding other user", chats.Onboarding, userID, userID + 1, []authz.Op{authz.View, authz.Edit}, authz.ErrForbidden, "forbidden"},
		{"active view", chats.Active, userID, userID, []authz.Op{authz.View}, nil, "ok"},
		{"degraded view", chats.Degraded, userID, userID, []authz.Op{authz.View}, nil, "ok"},
		{"unknown operation", chats.Active, userID, userID, []authz.Op{0}, authz.ErrForbidden, "forbidden"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshots := chatSnapshots{}
			if tt.state != chats.NoRecord {
				snapshots[chatID] = chats.ChatSnapshot{State: tt.state, OwnerID: tt.owner}
			}
			counters := metrics.NewSet()
			z := newAuthz(t, authz.Deps{
				Chats: snapshots, Metrics: counters,
				Reporter: reporterFunc(func(id int64) { t.Errorf("unexpected stale report for %d", id) }),
				Sender: senderFunc(func(r sender.Request) error {
					t.Errorf("unexpected %s for chat %d", r.Method, r.ChatID)
					return sender.ErrBusy
				}),
			})
			for _, op := range tt.ops {
				grant, err := z.Authorize(context.Background(), tt.actor, chatID, "future_group", op)
				if grant != (authz.Grant{}) || !errors.Is(err, tt.want) {
					t.Errorf("Authorize(%v) = (%+v, %v), want (zero Grant, %v)", op, grant, err, tt.want)
				}
				opLabel := map[authz.Op]string{authz.View: "view", authz.Edit: "edit", 0: "unknown"}[op]
				assertCounter(t, counters, fmt.Sprintf(`yabot_authz_decisions_total{op=%q,outcome=%q}`, opLabel, tt.result), 1)
			}
		})
	}
}

func TestEditChecksCurrentOwnership(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t)
		f.snapshots[chatID] = chats.ChatSnapshot{State: chats.Degraded, OwnerID: userID}
		for _, step := range []struct {
			body   string
			public bool
			want   error
		}{
			{creatorBody(userID, false), true, nil},
			{creatorBody(userID, true), false, nil},
			{administratorBody, false, authz.ErrForbidden},
			{creatorBody(userID, false), true, nil},
		} {
			done := startEdit(f.check, t.Context(), userID, chatID, "general")
			if err := complete(t, f.request(t, chatID), userID, chatID, step.body); err != nil {
				t.Fatal(err)
			}
			checkEdit(t, done, step.public, step.want)
			if step.want == authz.ErrForbidden {
				if id := receive(t, f.stale); id != chatID {
					t.Fatalf("stale owner report for %d, want %d", id, chatID)
				}
			}
			f.assertIdle(t)
		}
		assertCounter(t, f.metrics, `yabot_authz_owner_checks_total{outcome="creator"}`, 3)
		assertCounter(t, f.metrics, `yabot_authz_owner_checks_total{outcome="not_creator"}`, 1)
	})
}

func assertCounter(t *testing.T, set *metrics.Set, name string, want uint64) {
	t.Helper()
	if got := set.GetOrCreateCounter(name).Get(); got != want {
		t.Errorf("%s = %d, want %d", name, got, want)
	}
}
