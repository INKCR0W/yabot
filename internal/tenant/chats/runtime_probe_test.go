package chats

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

const (
	runtimeMigrationTarget   int64 = -10111
	runtimeMigrationEnvelope       = `{"ok":false,"error_code":400,"description":"group chat was upgraded to a supergroup chat","parameters":{"migrate_to_chat_id":-10111}}`
	runtimeChatGoneEnvelope        = `{"ok":false,"error_code":400,"description":"chat not found"}`
	runtimeForbiddenEnvelope       = `{"ok":false,"error_code":403,"description":"forbidden"}`

	runtimeProbeMember = `{"status":"member","user":{"id":44,"is_bot":true}}`
	runtimeProbeAdmin  = `{"status":"administrator","user":{"id":44,"is_bot":true},"can_invite_users":true}`
	runtimeProbeLeft   = `{"status":"left","user":{"id":44,"is_bot":true}}`
	runtimeProbeOwner  = `[{"status":"administrator","user":{"id":21}},{"status":"creator","user":{"id":555},"is_anonymous":true}]`
)

func TestRuntimeProbeSDKMigrationAtEachStage(t *testing.T) {
	t.Parallel()
	stages := []struct{ method, payload string }{
		{"getChat", `{"type":"group","title":"Initial title"}`},
		{"getChatMember", runtimeProbeAdmin},
		{"getChatAdministrators", runtimeProbeOwner},
	}
	const firstChatID = -100720
	h := newRuntimeHarness(t, runtimeOptions{tickets: 1, beforeStart: func(h *runtimeHarness) {
		for i := range stages {
			h.seedActive(firstChatID-int64(i), 1)
		}
	}})
	for i, stage := range stages {
		t.Run(stage.method, func(t *testing.T) {
			id := firstChatID - int64(i)
			h.chats.ReportUnknown(id)
			h.tick()
			for j, request := range stages[:i+1] {
				req := h.nextRequest(request.method)
				if j == i {
					h.finishRequestEnvelope(req, runtimeMigrationEnvelope)
				} else {
					h.finishRequest(req, request.payload)
				}
			}
			h.settleEffects(id)
			h.assertNoRequest()
			h.assertProbeTicket(0)
			if got := h.snapshot(id); got.State != Removed || got.RemovedReason != RemovedMigrated ||
				got.MigratedTo != runtimeMigrationTarget || got.PurgeAfter != h.now().Add(720*time.Hour).Unix() ||
				got.OwnerID != 81 || h.water(id).ID != 1 {
				t.Fatalf("migration from %s changed the snapshot to %+v", stage.method, got)
			}
		})
	}
	h.assertCounter(`yabot_chats_probe_total{trigger="report_unknown",outcome="migrated"}`, uint64(len(stages)))
	if _, exists := h.chats.State(runtimeMigrationTarget); exists {
		t.Fatal("Probe migration created a record for the destination")
	}
}

type runtimeTransport func(*http.Request) (*http.Response, error)

func (rt runtimeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return rt(r) }

func (h *runtimeHarness) finishRequest(req sender.Request, result string) map[string]json.RawMessage {
	h.t.Helper()
	return h.finishRequestEnvelope(req, `{"ok":true,"result":`+result+`}`)
}

func (h *runtimeHarness) finishRequestEnvelope(req sender.Request, envelope string) map[string]json.RawMessage {
	h.t.Helper()
	return h.finishRequestStatus(req, http.StatusOK, envelope)
}

func (h *runtimeHarness) finishRequestStatus(req sender.Request, status int, envelope string) map[string]json.RawMessage {
	h.t.Helper()
	var payload map[string]json.RawMessage
	transport := runtimeTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/"+req.Method) {
			h.t.Errorf("SDK sent %s %s, want POST %s", r.Method, r.URL.Path, req.Method)
		}
		if err := errors.Join(r.ParseMultipartForm(1<<20), r.Body.Close()); err != nil {
			return nil, err
		}
		if err := r.MultipartForm.RemoveAll(); err != nil {
			return nil, err
		}
		payload = make(map[string]json.RawMessage, len(r.MultipartForm.Value))
		for name, values := range r.MultipartForm.Value {
			field := []byte(values[0])
			if !json.Valid(field) {
				var err error
				field, err = json.Marshal(values[0])
				if err != nil {
					return nil, err
				}
			}
			payload[name] = field
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(envelope)), Header: make(http.Header)}, nil
	})
	upstream, err := bot.New("12345:test", bot.WithSkipGetMe(), bot.WithServerURL("https://telegram.test"),
		bot.WithHTTPClient(0, &http.Client{Transport: transport}))
	if err != nil {
		h.t.Fatal(err)
	}
	err = req.Do(h.t.Context(), upstream)
	if payload == nil {
		h.t.Fatalf("%s did not make its SDK request: %v", req.Method, err)
	}
	req.Done(sender.Result{Outcome: telegram.Classify(err), Err: err, Attempts: 1})
	return payload
}

func (h *runtimeHarness) runtimeTickets() float64 {
	return h.metrics.GetOrCreateGauge(`yabot_dispatch_tickets_inuse{shard="0"}`, nil).Get()
}

func (h *runtimeHarness) assertProbeTicket(want float64) {
	h.t.Helper()
	if got := h.runtimeTickets(); got != want {
		h.t.Fatalf("tickets in use = %v, want %v", got, want)
	}
	if got := h.metrics.GetOrCreateCounter(`yabot_dispatch_ticket_late_total{op="complete"}`).Get(); got != 0 {
		h.t.Fatalf("late ticket completions = %d", got)
	}
}

func (h *runtimeHarness) startReportedProbe(chatID int64) sender.Request {
	h.t.Helper()
	h.chats.ReportUnknown(chatID)
	h.tick()
	return h.nextRequest("getChat")
}

func (h *runtimeHarness) assertChatOperation(chatID int64, operation MetricKind, outcome string, level slog.Level) {
	h.t.Helper()
	message := "chat operation result"
	if level == slog.LevelError {
		message = "chat operation failed"
	}
	h.assertLog(level, message, "chat_id", chatID, "operation", string(operation), "outcome", outcome)
}

func (h *runtimeHarness) assertProbeOutcome(chatID int64, trigger ProbeTrigger, outcome string, level slog.Level) {
	h.t.Helper()
	h.assertCounter(fmt.Sprintf(`yabot_chats_probe_total{trigger=%q,outcome=%q}`, trigger, outcome), 1)
	h.assertChatOperation(chatID, ProbeOutcome, outcome, level)
}

func (h *runtimeHarness) inspectProbeRequest(req sender.Request, chatID int64, result string) {
	h.t.Helper()
	if req.ChatID != chatID || req.Class != sender.Background || req.NoRetry {
		h.t.Fatalf("invalid %s sender contract: chat %d, class %v, no retry %v", req.Method, req.ChatID, req.Class, req.NoRetry)
	}
	payload := h.finishRequest(req, result)
	if got := string(payload["chat_id"]); got != strconv.FormatInt(chatID, 10) {
		h.t.Fatalf("%s payload chat_id = %s, want %d", req.Method, got, chatID)
	}
	if req.Method == "getChatMember" && string(payload["user_id"]) != strconv.Itoa(runtimeBotID) {
		h.t.Fatalf("getChatMember user id = %s, want %d", payload["user_id"], runtimeBotID)
	}
}

func TestRuntimeProbeChainAndGuardVisibility(t *testing.T) {
	t.Parallel()
	const id = -10070
	for _, tc := range []struct {
		name        string
		guardBot    string
		member      string
		state       ChatState
		known       bool
		assigned    bool
		admins      string
		wantCreator int64
	}{
		{"visible and assigned", `,"guard_bot":{"id":44,"is_bot":true}`, runtimeProbeMember, Onboarding, true, true, runtimeProbeOwner, 555},
		{"visible and another bot", `,"guard_bot":{"id":89,"is_bot":true}`, runtimeProbeMember, Onboarding, true, false, runtimeProbeOwner, 555},
		{"absent as administrator", "", runtimeProbeAdmin, Active, true, false, runtimeProbeOwner, 555},
		{"absent as member", "", runtimeProbeMember, Onboarding, false, false, runtimeProbeOwner, 555},
		{"empty administrator list", "", runtimeProbeMember, Onboarding, false, false, `[]`, 0},
		{"no creator", "", runtimeProbeMember, Onboarding, false, false, `[{"status":"administrator","user":{"id":555}}]`, 0},
		{"creator followed by a duplicate", "", runtimeProbeAdmin, Active, true, false,
			`[{"status":"creator","user":{"id":555}},{"status":"creator","user":{"id":556}}]`, 555},
		{"bot already left", "", runtimeProbeLeft, Removed, false, false, "", 81},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRuntimeHarness(t, runtimeOptions{tickets: 1, beforeStart: func(h *runtimeHarness) {
				h.seedActive(id, 1)
				h.exec(`UPDATE chats SET state='onboarding',bot_rights=0 WHERE chat_id=?`, id)
			}})
			getChat := h.startReportedProbe(id)
			h.assertProbeTicket(1)
			h.assertNoRequest()
			h.inspectProbeRequest(getChat, id, `{"id":-10070,"type":"supergroup","title":"Observed title","join_by_request":true`+tc.guardBot+`}`)
			member := h.nextRequest("getChatMember")
			h.assertProbeTicket(1)
			h.assertNoRequest()
			h.inspectProbeRequest(member, id, tc.member)
			if tc.admins != "" {
				admins := h.nextRequest("getChatAdministrators")
				h.assertProbeTicket(1)
				h.assertNoRequest()
				h.inspectProbeRequest(admins, id, tc.admins)
			}
			h.settleEffects(id)
			h.assertProbeTicket(0)
			h.assertNoRequest()
			snap := h.snapshot(id)
			if snap.State != tc.state || snap.OwnerID != tc.wantCreator || snap.GuardKnown != tc.known || snap.GuardAssigned != tc.assigned ||
				!snap.JoinByRequest || snap.Title != "Observed title" {
				t.Fatalf("unexpected Probe projection: %+v", snap)
			}
			h.assertCounter(`yabot_chats_probe_total{trigger="report_unknown",outcome="ok"}`, 1)
			h.assertNoLog(slog.LevelInfo, "chat operation result", "chat_id", int64(id), "operation", string(ProbeOutcome), "outcome", "ok")
			if h.water(id).ID != 1 {
				t.Fatal("Probe changed the membership watermark")
			}
		})
	}
}

func TestRuntimeProbeStopsOnPartialFailure(t *testing.T) {
	t.Parallel()
	const id = -10071
	for _, tc := range []struct {
		name    string
		fail    string
		reject  bool
		panics  bool
		result  sender.Result
		want    ChatState
		migrate int64
		outcome string
		level   slog.Level
	}{
		{name: "getChat retry", fail: "getChat", result: sender.Result{Outcome: telegram.Outcome{Class: telegram.Retry}}, want: Active, outcome: "retry"},
		{name: "member dropped", fail: "getChatMember", result: sender.DroppedResult(sender.Expired, sender.ErrWouldMiss), want: Active, outcome: "expired"},
		{name: "admins unexpected", fail: "getChatAdministrators", result: sender.Result{Outcome: telegram.Outcome{Class: telegram.Fatal}}, want: Active, outcome: "fatal", level: slog.LevelError},
		{name: "getChat enqueue busy", fail: "getChat", reject: true, want: Active, outcome: "busy"},
		{name: "member enqueue busy", fail: "getChatMember", reject: true, want: Active, outcome: "busy"},
		{name: "admins enqueue busy", fail: "getChatAdministrators", reject: true, want: Active, outcome: "busy"},
		{name: "getChat enqueue panic", fail: "getChat", panics: true, want: Active, outcome: "fatal", level: slog.LevelError},
		{name: "member enqueue panic", fail: "getChatMember", panics: true, want: Active, outcome: "fatal", level: slog.LevelError},
		{name: "admins enqueue panic", fail: "getChatAdministrators", panics: true, want: Active, outcome: "fatal", level: slog.LevelError},
		{name: "admins forbidden", fail: "getChatAdministrators", result: sender.Result{Outcome: telegram.Outcome{Class: telegram.Forbidden}}, want: Removed, outcome: "forbidden"},
		{name: "admins permission", fail: "getChatAdministrators", result: sender.Result{Outcome: telegram.Outcome{Class: telegram.Permission}}, want: Active, outcome: "permission"},
		{name: "member gone", fail: "getChatMember", result: sender.Result{Outcome: telegram.Outcome{Class: telegram.Done, Reason: telegram.ChatGone}}, want: Removed, outcome: "chat_gone"},
		{name: "member unexpected done", fail: "getChatMember", result: sender.Result{Outcome: telegram.Outcome{Class: telegram.Done, Reason: telegram.RequestMissing}}, want: Active, outcome: "request_missing", level: slog.LevelError},
		{name: "admins migrated", fail: "getChatAdministrators", result: sender.Result{Outcome: telegram.Outcome{Class: telegram.Migrated, MigrateTo: -10111}}, want: Removed, migrate: -10111, outcome: "migrated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRuntimeHarness(t, runtimeOptions{tickets: 1, beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) }})
			if tc.reject {
				h.sender.mu.Lock()
				h.sender.reject = tc.fail
				h.sender.mu.Unlock()
			}
			if tc.panics {
				h.sender.mu.Lock()
				h.sender.panicOn = tc.fail
				h.sender.mu.Unlock()
			}
			h.chats.ReportUnknown(id)
			h.tick()
			stages := []struct{ method, payload string }{
				{"getChat", `{"type":"supergroup","title":"Title from successful getChat"}`},
				{"getChatMember", runtimeProbeAdmin},
				{"getChatAdministrators", runtimeProbeOwner},
			}
			for _, step := range stages {
				if step.method == tc.fail && (tc.reject || tc.panics) {
					break
				}
				req := h.nextRequest(step.method)
				if step.method == tc.fail {
					req.Done(tc.result)
					break
				}
				h.finishRequest(req, step.payload)
			}
			h.settleEffects(id)
			h.assertProbeTicket(0)
			if tc.panics {
				h.assertCounter(`yabot_dispatch_panics_total{route="none"}`, 0)
				h.assertCounter(`yabot_dispatch_ticket_late_total{op="cancel"}`, 0)
			}
			h.assertNoRequest()
			snap := h.snapshot(id)
			wantTitle := "Title from successful getChat"
			if tc.fail == "getChat" {
				wantTitle = "Initial title"
			}
			if snap.State != tc.want || snap.OwnerID != 81 || snap.MigratedTo != tc.migrate || snap.Title != wantTitle {
				t.Fatalf("partial Probe projection: %+v", snap)
			}
			h.assertProbeOutcome(id, ProbeUnknown, tc.outcome, tc.level)
		})
	}
}

func TestRuntimeProbeReportsSDKFailures(t *testing.T) {
	t.Parallel()
	const id int64 = -100711
	const apiFailure = `{"ok":false,"error_code":500,"description":"temporary upstream failure"}`
	for _, tc := range []struct {
		name     string
		method   string
		envelope string
		outcome  string
		level    slog.Level
	}{
		{"getChat API error", "getChat", apiFailure, "retry", slog.LevelInfo},
		{"getChat missing result", "getChat", `{"ok":true,"result":null}`, "fatal", slog.LevelError},
		{"getChatMember API error", "getChatMember", apiFailure, "retry", slog.LevelInfo},
		{"getChatMember null result", "getChatMember", `{"ok":true,"result":null}`, "fatal", slog.LevelError},
		{"getChatAdministrators API error", "getChatAdministrators", apiFailure, "retry", slog.LevelInfo},
		{"getChatAdministrators null result", "getChatAdministrators", `{"ok":true,"result":null}`, "fatal", slog.LevelError},
		{"creator without user", "getChatAdministrators", `{"ok":true,"result":[{"status":"creator"}]}`, "fatal", slog.LevelError},
		{"creator with zero user ID", "getChatAdministrators", `{"ok":true,"result":[{"status":"creator","user":{"id":0}}]}`, "fatal", slog.LevelError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRuntimeHarness(t, runtimeOptions{tickets: 1, beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) }})
			h.chats.ReportUnknown(id)
			h.tick()
			stages := []struct{ method, payload string }{
				{"getChat", `{"type":"supergroup","title":"Title from successful getChat"}`},
				{"getChatMember", runtimeProbeAdmin},
				{"getChatAdministrators", runtimeProbeOwner},
			}
			for _, stage := range stages {
				request := h.nextRequest(stage.method)
				if stage.method == tc.method {
					h.finishRequestEnvelope(request, tc.envelope)
					break
				}
				h.finishRequest(request, stage.payload)
			}
			h.settleEffects(id)
			h.assertNoRequest()
			h.assertProbeTicket(0)
			h.assertProbeOutcome(id, ProbeUnknown, tc.outcome, tc.level)
			wantTitle := "Title from successful getChat"
			if tc.method == "getChat" {
				wantTitle = "Initial title"
			}
			if got := h.snapshot(id); got.State != Active || got.OwnerID != 81 || got.Title != wantTitle {
				t.Fatalf("snapshot after %s = %+v, want active with unchanged owner and title %q", tc.name, got, wantTitle)
			}
		})
	}
}

func TestRuntimeProbeReportsUnknownMembershipWithoutChangingState(t *testing.T) {
	t.Parallel()
	const id int64 = -100712
	h := newRuntimeHarness(t, runtimeOptions{tickets: 1, beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) }})
	h.finishRequest(h.startReportedProbe(id), `{"type":"supergroup","title":"Observed title"}`)
	h.finishRequest(h.nextRequest("getChatMember"), `{"status":"unrecognized","user":{"id":44}}`)
	h.settleEffects(id)
	h.assertNoRequest()
	h.assertProbeTicket(0)
	h.assertCounter(`yabot_chats_events_total{outcome="invalid_member"}`, 1)
	h.assertChatOperation(id, InvalidMember, "invalid_member", slog.LevelError)
	if got := h.snapshot(id); got.State != Active || got.OwnerID != 81 || got.Title != "Observed title" {
		t.Fatalf("unknown membership changed committed membership: %+v", got)
	}
}

func TestRuntimeGetChatServerFailureKeepsConfirmedSnapshot(t *testing.T) {
	t.Parallel()
	const id int64 = -100713
	h := newRuntimeHarness(t, runtimeOptions{tickets: 1, beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) }})
	h.finishRequest(h.startReportedProbe(id), deliveryProbeChat(id, models.ChatTypeSupergroup))
	h.finishRequest(h.nextRequest("getChatMember"), runtimeProbeAdmin)
	h.finishRequest(h.nextRequest("getChatAdministrators"), runtimeProbeOwner)
	h.settleEffects(id)
	before := h.snapshot(id)
	if !before.GuardKnown || !before.GuardAssigned || !before.JoinByRequest {
		t.Fatalf("successful Probe did not confirm metadata: %+v", before)
	}
	version := h.chats.Version()

	h.advance(ProbeInterval)
	h.finishRequestStatus(h.startReportedProbe(id), http.StatusInternalServerError,
		`{"ok":false,"error_code":500,"description":"temporary upstream failure"}`)
	h.settleEffects(id)
	if after := h.snapshot(id); after != before || h.chats.Version() != version {
		t.Fatalf("getChat server failure changed snapshot or version: before %+v, after %+v", before, after)
	}
	h.assertProbeOutcome(id, ProbeUnknown, "retry", slog.LevelInfo)
	h.assertNoRequest()
	h.assertProbeTicket(0)
}

func TestRuntimeProbeReportsCoalescingAndRateLimit(t *testing.T) {
	t.Parallel()
	const id int64 = -100713
	h := newRuntimeHarness(t, runtimeOptions{tickets: 1, beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) }})
	request := h.startReportedProbe(id)
	h.chats.ReportPermissionLost(id)
	h.tick()
	h.barrier(id)
	h.assertNoRequest()
	h.assertCounter(`yabot_chats_probe_total{trigger="report_permission_lost",outcome="in_flight"}`, 1)
	h.assertChatOperation(id, ProbeInFlight, "in_flight", slog.LevelInfo)

	request.Done(sender.DroppedResult(sender.Expired, sender.ErrWouldMiss))
	h.settleEffects(id)
	h.advance(ProbeInterval - time.Nanosecond)
	h.chats.ReportOwnerStale(id)
	h.tick()
	h.barrier(id)
	h.assertNoRequest()
	h.assertCounter(`yabot_chats_probe_total{trigger="report_owner_stale",outcome="rate_limited"}`, 1)
	h.assertChatOperation(id, ProbeLimited, "rate_limited", slog.LevelInfo)

	h.advance(time.Nanosecond)
	h.chats.ReportOwnerStale(id)
	h.tick()
	if request := h.nextRequest("getChat"); request.ChatID != id {
		t.Fatalf("Probe after the rate-limit interval targeted %d, want %d", request.ChatID, id)
	}
}

type runtimeCountedStore struct {
	*store.Store
	opErrors *atomic.Int32
}

func (s runtimeCountedStore) Write(op store.Op, done func(error)) error {
	return s.Store.Write(op, func(err error) {
		if _, ok := errors.AsType[*store.OpError](err); ok {
			s.opErrors.Add(1)
		}
		done(err)
	})
}

func TestRuntimeProbeAbandonmentFreesSlotAndStartsPendingTransition(t *testing.T) {
	t.Parallel()
	const id = -10072
	var opErrors atomic.Int32
	h := newRuntimeHarness(t, runtimeOptions{
		beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) },
		configure: func(d *Deps) {
			d.Store = runtimeCountedStore{Store: d.Store.(*store.Store), opErrors: &opErrors}
		},
	})
	h.exec(`CREATE TRIGGER test_reject_probed_owner BEFORE UPDATE ON chats WHEN NEW.owner_id = 555
		BEGIN SELECT raise(ABORT, 'forced Probe failure'); END`)
	first := h.startReportedProbe(id)
	h.apply(runtimeUpdate(id, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, false))
	h.nextRequest("sendMessage").Done(sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}})
	h.assertNoRequest()
	h.finishRequest(first, `{"type":"supergroup","title":"Old Probe title"}`)
	h.finishRequest(h.nextRequest("getChatMember"), runtimeProbeAdmin)
	h.finishRequest(h.nextRequest("getChatAdministrators"), runtimeProbeOwner)
	h.settleEffects(id)
	for attempt := int32(1); attempt <= 4; attempt++ {
		if got := opErrors.Load(); got != attempt {
			t.Fatalf("operation errors after attempt %d = %d", attempt, got)
		}
		if snap := h.snapshot(id); snap.State != Degraded || snap.OwnerID != 81 || snap.Title != "Initial title" {
			t.Fatalf("a failed Probe changed snapshot: %+v", snap)
		}
		if attempt < 4 {
			h.assertNoRequest()
			h.advance(5 * time.Second)
			h.tick()
			h.settleEffects(id)
		}
	}
	h.assertCounter(`yabot_chats_write_failures_total{outcome="op_error"}`, 4)
	h.assertCounter("yabot_chats_write_abandoned_total", 1)
	h.assertLog(slog.LevelError, "chat event abandoned after write failures", "chat_id", int64(id))
	replacement := h.nextRequest("getChat")
	if replacement.ChatID != id || h.runtimeTickets() != 1 {
		t.Fatalf("replacement Probe chat = %d, in-use tickets = %v", replacement.ChatID, h.runtimeTickets())
	}
	replacement.Done(sender.DroppedResult(sender.Shutdown, sender.ErrClosed))
	h.settleEffects(id)
	h.assertProbeTicket(0)
}

func TestRuntimeAbandonedJoinProbeGuidesFromCommittedStateAndFreesSlot(t *testing.T) {
	t.Parallel()
	const id = -10079
	var opErrors atomic.Int32
	h := newRuntimeHarness(t, runtimeOptions{
		configure: func(d *Deps) {
			d.Store = runtimeCountedStore{Store: d.Store.(*store.Store), opErrors: &opErrors}
		},
	})
	h.exec(`CREATE TRIGGER test_reject_owner BEFORE UPDATE ON chats WHEN NEW.owner_id = 555
		BEGIN SELECT raise(ABORT, 'forced Probe failure'); END`)
	h.apply(runtimeUpdate(id, 1, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false))
	committed := h.snapshot(id)
	h.finishRequest(h.nextRequest("getChat"), `{"type":"supergroup","title":"Probe title","guard_bot":{"id":44,"is_bot":true},"join_by_request":true}`)
	h.finishRequest(h.nextRequest("getChatMember"), runtimeProbeAdmin)
	h.finishRequest(h.nextRequest("getChatAdministrators"), runtimeProbeOwner)
	h.settleEffects(id)
	for range 3 {
		h.assertNoRequest()
		h.advance(5 * time.Second)
		h.tick()
		h.settleEffects(id)
	}
	if got := opErrors.Load(); got != 4 {
		t.Fatalf("Probe attempts = %d, want 4", got)
	}
	if got := h.snapshot(id); got != committed || got.State != Onboarding {
		t.Fatalf("abandoned Probe changed the committed join: %+v", got)
	}
	guide := h.nextRequest("sendMessage")
	payload := h.finishRequest(guide, `{"message_id":1}`)
	var body string
	if err := json.Unmarshal(payload["text"], &body); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"chats.onboarding.guard_unknown", "chats.onboarding.missing_invite"} {
		if want := h.catalog.HTML("zh-Hans", key, nil); !strings.Contains(body, want) {
			t.Errorf("guide after abandonment omitted %q: %q", want, body)
		}
	}
	if stale := h.catalog.HTML("zh-Hans", "chats.onboarding.join_by_request", nil); strings.Contains(body, stale) {
		t.Fatalf("guide included a fact from the abandoned Probe: %q", body)
	}
	h.settleEffects(id)
	h.assertProbeTicket(0)
	h.assertNoRequest()
	h.advance(ProbeInterval)
	if req := h.startReportedProbe(id); req.ChatID != id {
		t.Fatalf("abandoned Probe blocked the next trigger: chat=%d", req.ChatID)
	}
}

func TestRuntimeProbeStaleFactsAndOwnerClaim(t *testing.T) {
	t.Parallel()
	const id = -10073
	for _, tc := range []struct {
		name      string
		makeClaim bool
		wantOwner int64
	}{
		{"stale facts still allow owner", false, 555},
		{"claim prevents stale owner", true, 42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) }})
			first := h.startReportedProbe(id)
			h.apply(runtimeUpdate(id, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, true))
			if tc.makeClaim {
				ctx, cancel := context.WithTimeout(t.Context(), runtimeWait)
				defer cancel()
				if err := h.setOwner(ctx, id, tc.wantOwner); err != nil {
					t.Fatal(err)
				}
				if snap := h.snapshot(id); snap.OwnerID != tc.wantOwner {
					t.Fatalf("claim returned before snapshot updated: %+v", snap)
				}
			}
			h.finishRequest(first, `{"type":"supergroup","title":"Stale title"}`)
			h.finishRequest(h.nextRequest("getChatMember"), runtimeProbeAdmin)
			h.finishRequest(h.nextRequest("getChatAdministrators"), runtimeProbeOwner)
			h.settleEffects(id)
			h.assertCounter(`yabot_chats_probe_total{trigger="report_unknown",outcome="stale"}`, 1)
			h.assertChatOperation(id, ProbeStaleResult, "stale", slog.LevelInfo)
			if snap := h.snapshot(id); snap.OwnerID != tc.wantOwner || snap.Title != "Initial title" {
				t.Fatalf("stale Probe snapshot = %+v, expected owner %d", snap, tc.wantOwner)
			}
			var owner int64
			if err := h.store.Read().QueryRowContext(t.Context(), `SELECT owner_id FROM chats WHERE chat_id=?`, id).Scan(&owner); err != nil || owner != tc.wantOwner {
				t.Fatalf("owner on disk = %d, expected %d: %v", owner, tc.wantOwner, err)
			}
		})
	}
}

func TestRuntimeErasureScrubsProbeReplayAfterWriteFailure(t *testing.T) {
	t.Parallel()
	const id, userID = -10076, 555
	for _, tc := range []struct {
		name     string
		previous ChatState
		sqlVerb  string
	}{
		{"recordless", NoRecord, "INSERT"},
		{"removed", Removed, "UPDATE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writes := make(chan struct{}, 4)
			h := newRuntimeHarness(t, runtimeOptions{
				configure: func(d *Deps) {
					actual := d.Store
					d.Store = maintenanceStore{Store: actual, write: func(op store.Op, done func(error)) error {
						err := actual.Write(op, done)
						select {
						case writes <- struct{}{}:
						default:
						}
						return err
					}}
				},
				beforeStart: func(h *runtimeHarness) {
					if tc.previous == Removed {
						h.seedActive(id, 1)
						h.exec(`UPDATE chats SET state='removed',bot_rights=0,removed_at=?,removed_reason='left',purge_after=? WHERE chat_id=?`,
							h.now().Unix(), h.now().Add(720*time.Hour).Unix(), id)
					}
				},
			})
			h.exec(`CREATE TRIGGER test_reject_recovery BEFORE ` + tc.sqlVerb + ` ON chats WHEN NEW.owner_id = 555
				BEGIN SELECT raise(ABORT, 'forced recovery failure'); END`)
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			t.Cleanup(unblock)
			blocked := make(chan error, 1)
			if err := h.store.Write(func(*sql.Tx) error {
				close(entered)
				<-release
				return nil
			}, func(err error) { blocked <- err }); err != nil {
				t.Fatal(err)
			}
			runtimeReceive(t, entered)
			h.finishRequest(h.startReportedProbe(id), `{"type":"supergroup","title":"Recovered title"}`)
			h.finishRequest(h.nextRequest("getChatMember"), runtimeProbeAdmin)
			h.finishRequest(h.nextRequest("getChatAdministrators"), runtimeProbeOwner)
			runtimeReceive(t, writes)
			erased := make(chan error, 1)
			go func() { erased <- h.chats.DeleteUser(t.Context(), userID) }()
			runtimeReceive(t, writes)
			unblock()
			if err := errors.Join(runtimeReceive(t, blocked), runtimeReceive(t, erased)); err != nil {
				t.Fatal(err)
			}
			h.settleEffects(id)
			prior, present := h.chats.State(id)
			if prior.State != tc.previous || present != (tc.previous != NoRecord) {
				t.Fatalf("failed recovery published state: %+v, %t", prior, present)
			}
			h.exec(`DROP TRIGGER test_reject_recovery`)
			h.advance(5 * time.Second)
			h.tick()
			h.settleEffects(id)
			if after := h.snapshot(id); after.State != Active || after.OwnerID != 0 || after.Title != "Recovered title" {
				t.Fatalf("recovery replay restored erased owner or lost the transition: %+v", after)
			}
			h.nextRequest("sendMessage").Done(sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}})
			assertNoStoredChatIdentity(t, h, userID)
		})
	}
}
