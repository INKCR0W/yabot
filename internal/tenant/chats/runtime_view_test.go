package chats

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/store"
)

func TestRuntimeReporterDeduplicatesAndConsumesReportsOnTick(t *testing.T) {
	t.Parallel()
	const oldID, newID = -11006, -11005
	h := newRuntimeHarness(t, runtimeOptions{shards: 2, beforeStart: func(h *runtimeHarness) {
		h.seedActive(oldID, 1)
	}})
	before := h.chats.Version()
	h.chats.ReportMigrated(oldID, newID)
	h.chats.ReportMigrated(oldID, newID-10)
	h.barrier(oldID)
	if h.snapshot(oldID).State != Active {
		t.Fatal("migration ran before dispatcher tick")
	}
	h.tick()
	h.settleEffects(oldID)
	changes, now, full := h.chats.ChangesSince(before)
	if got := h.snapshot(oldID); got.State != Removed || got.RemovedReason != RemovedMigrated || got.MigratedTo != newID {
		t.Fatalf("migration report did not produce the first removal: %+v", got)
	}
	if _, ok := h.chats.State(newID); ok || full || now != before+1 || !slices.Equal(changes, []Change{
		{Version: now, ChatID: oldID, State: Removed},
	}) {
		t.Fatalf("migration view delta = %v, %d, %v", changes, now, full)
	}
	for _, report := range []struct {
		chatID int64
		send   func(int64)
	}{
		{newID, h.chats.ReportUnknown},
		{newID - 1, h.chats.ReportPermissionLost},
		{newID - 2, h.chats.ReportOwnerStale},
	} {
		report.send(report.chatID)
		report.send(report.chatID)
		h.tick()
		req := h.nextRequest("getChat")
		if req.ChatID != report.chatID || req.Class != sender.Background {
			t.Fatalf("report got request on wrong chat or class: id=%d class=%v", req.ChatID, req.Class)
		}
		req.Done(sender.DroppedResult(sender.Busy, sender.ErrBusy))
		h.barrier(report.chatID)
		h.assertNoRequest()
	}
	h.advance(ProbeInterval)
	h.chats.ReportUnknown(newID)
	h.tick()
	if req := h.nextRequest("getChat"); req.ChatID != newID {
		t.Fatalf("consumed report was not replaceable: chat=%d", req.ChatID)
	}
}

func TestRuntimeReporterCountsUniqueOverflow(t *testing.T) {
	t.Parallel()
	h := newRuntimeHarness(t, runtimeOptions{tickets: 128})
	const first int64 = -190001
	for i := range 10_000 {
		h.chats.ReportUnknown(first - int64(i))
	}
	h.chats.ReportUnknown(first)
	h.chats.ReportOwnerStale(first)
	h.chats.ReportUnknown(first - 10_000)
	h.assertCounter(`yabot_chats_reports_dropped_total{reason="capacity"}`, 2)
	h.assertNoRequest()

	dispatched := make(map[int64]bool)
	for range 2 {
		h.tick()
		h.barrier(first)
		batch := len(h.sender.requests)
		if batch != 64 {
			t.Fatalf("one tick dispatched %d of the pending reports, want 64", batch)
		}
		for range batch {
			req := h.nextRequest("getChat")
			if req.ChatID > first || req.ChatID <= first-10_000 || dispatched[req.ChatID] {
				t.Fatalf("one tick dispatched a dropped or duplicate report for chat %d", req.ChatID)
			}
			dispatched[req.ChatID] = true
			req.Done(sender.DroppedResult(sender.Busy, sender.ErrBusy))
		}
		h.barrier(first)
	}
	h.assertLog(slog.LevelWarn, "chat report queue is full", "dropped", uint64(2))
	if got := h.logCount(slog.LevelWarn, "chat report queue is full"); got != 1 {
		t.Fatalf("two ticks reported one overflow %d times", got)
	}
}

type rejectReportDispatcher struct {
	Dispatcher
	key       dispatch.Key
	err       error
	rejection atomic.Bool
}

func (d *rejectReportDispatcher) Submit(k dispatch.Key, task dispatch.Task) error {
	if k == d.key && d.rejection.CompareAndSwap(false, true) {
		return d.err
	}
	return d.Dispatcher.Submit(k, task)
}

func TestRuntimeReporterRetriesOnlyBusySubmissions(t *testing.T) {
	t.Parallel()
	const id = -2001
	for _, tc := range []struct {
		name   string
		err    error
		busy   uint64
		closed uint64
	}{
		{name: "busy", err: dispatch.ErrBusy, busy: 1},
		{name: "closed", err: dispatch.ErrClosed, closed: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRuntimeHarness(t, runtimeOptions{configure: func(deps *Deps) {
				deps.Dispatcher = &rejectReportDispatcher{Dispatcher: deps.Dispatcher, key: keyOf(id), err: tc.err}
			}})
			h.chats.ReportMigrated(id, id-1)
			h.tick()
			h.settleEffects(id)
			if _, ok := h.chats.State(id); ok {
				t.Fatal("report applied on a rejected submission")
			}
			h.assertCounter(`yabot_chats_overload_total{source="report_inbox"}`, tc.busy)
			h.assertCounter(`yabot_chats_reports_dropped_total{reason="dispatch_closed"}`, tc.closed)
			if tc.busy != 0 {
				h.assertLog(slog.LevelInfo, "chat report deferred because shard inbox is full", "chat_id", int64(id), "kind", "migrated")
			} else {
				h.assertLog(slog.LevelInfo, "chat report could not be submitted", "chat_id", int64(id))
				h.assertNoLog(slog.LevelInfo, "chat report deferred because shard inbox is full")
			}

			h.chats.ReportMigrated(id, id-2)
			h.tick()
			h.settleEffects(id)
			want := int64(id - 1)
			if tc.closed != 0 {
				want = id - 2
			}
			if got := h.snapshot(id); got.State != Removed || got.MigratedTo != want {
				t.Fatalf("migration after a rejected submission = %+v, want target %d", got, want)
			}
		})
	}
}

func TestRuntimeReporterDoesNotResubmitAnAcceptedReportStillInMailbox(t *testing.T) {
	t.Parallel()
	const id = -2002
	h := newRuntimeHarness(t, runtimeOptions{})
	if err := h.dispatcher.Submit(keyOf(id), func(_ context.Context, sh *dispatch.Shard) {
		sh.Hold(keyOf(id))
	}); err != nil {
		t.Fatal(err)
	}
	h.barrier(id)
	h.chats.ReportUnknown(id)
	for range 3 {
		h.tick()
		h.barrier(id)
		h.assertNoRequest()
	}
	if err := h.dispatcher.Submit(keyOf(id), func(_ context.Context, sh *dispatch.Shard) {
		sh.Release(keyOf(id))
	}); err != nil {
		t.Fatal(err)
	}
	req := h.nextRequest("getChat")
	h.barrier(id)
	h.assertCounter(`yabot_chats_probe_total{trigger="report_unknown",outcome="in_flight"}`, 0)
	h.assertNoRequest()
	req.Done(sender.DroppedResult(sender.Busy, sender.ErrBusy))
	h.barrier(id)
	h.assertCounter(`yabot_chats_probe_total{trigger="report_unknown",outcome="busy"}`, 1)
}

func TestRuntimeReporterDoesNotDeliverDuringCancellationOrStop(t *testing.T) {
	t.Parallel()
	const id = -2003
	h := newRuntimeHarness(t, runtimeOptions{})
	h.chats.ReportMigrated(id, id-1)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := h.dispatcher.Submit(keyOf(id), func(_ context.Context, sh *dispatch.Shard) {
		h.chats.tick(ctx, sh)
	}); err != nil {
		t.Fatal(err)
	}
	h.settleEffects(id)
	if _, ok := h.chats.State(id); ok {
		t.Fatal("canceled tick delivered a report")
	}
	h.tick()
	h.settleEffects(id)
	if got := h.snapshot(id); got.MigratedTo != id-1 {
		t.Fatalf("the next live tick did not deliver the report: %+v", got)
	}

	h.chats.ReportMigrated(id, id-2)
	if err := h.chats.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	h.tick()
	h.settleEffects(id)
	if got := h.snapshot(id); got.MigratedTo != id-1 {
		t.Fatalf("tick delivered another report after stop: %+v", got)
	}
}

func TestRuntimeTickExpiresRecordlessWatermarksWithoutTraffic(t *testing.T) {
	t.Parallel()
	const id = -2004
	h := newRuntimeHarness(t, runtimeOptions{})
	h.apply(runtimeUpdate(id, 1, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeLeft, false))
	h.advance(10*time.Minute - time.Nanosecond)
	h.tick()
	h.assertCounter(`yabot_chats_memory_evictions_total{reason="ttl"}`, 0)
	h.advance(time.Nanosecond)
	h.tick()
	h.assertCounter(`yabot_chats_memory_evictions_total{reason="ttl"}`, 1)
}

func TestRuntimeRecordlessProbePinsMemoryUntilCompletion(t *testing.T) {
	t.Parallel()
	const probing, idle = -2005, -2006
	h := newRuntimeHarness(t, runtimeOptions{})
	h.apply(runtimeUpdate(probing, 1, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeLeft, false))
	request := h.startReportedProbe(probing)
	h.apply(runtimeUpdate(idle, 2, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeLeft, false))
	h.advance(memoryTTL)
	h.tick()
	h.assertCounter(`yabot_chats_memory_evictions_total{reason="ttl"}`, 1)
	h.chats.ReportUnknown(probing)
	h.tick()
	h.barrier(probing)
	h.assertNoRequest()
	h.assertCounter(`yabot_chats_probe_total{trigger="report_unknown",outcome="in_flight"}`, 1)
	request.Done(sender.DroppedResult(sender.Busy, sender.ErrBusy))
	h.settleEffects(probing)
	h.assertCounter(`yabot_chats_probe_total{trigger="report_unknown",outcome="stale"}`, 0)
	h.advance(memoryTTL - time.Nanosecond)
	h.tick()
	h.assertCounter(`yabot_chats_memory_evictions_total{reason="ttl"}`, 1)
	h.advance(time.Nanosecond)
	h.tick()
	h.assertCounter(`yabot_chats_memory_evictions_total{reason="ttl"}`, 2)
}

func TestRuntimeMalformedMemberUpdateIsPermanentWithoutAdvancingWatermark(t *testing.T) {
	t.Parallel()
	const chatID = -13102
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) { h.seedActive(chatID, 1) }})
	before := h.snapshot(chatID)
	for _, tc := range []struct {
		name        string
		breakUpdate func(*models.Update)
	}{
		{"missing chat id", func(u *models.Update) { u.MyChatMember.Chat.ID = 0 }},
		{"missing update id", func(u *models.Update) { u.ID = 0 }},
		{"missing update date", func(u *models.Update) { u.MyChatMember.Date = 0 }},
		{"missing old member status", func(u *models.Update) { u.MyChatMember.OldChatMember = models.ChatMember{} }},
		{"missing new member status", func(u *models.Update) { u.MyChatMember.NewChatMember = models.ChatMember{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			malformed := runtimeUpdate(chatID, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false)
			tc.breakUpdate(malformed)
			if err := h.update(malformed).Wait(runtimeWait); !dispatch.IsPermanent(err) {
				t.Fatalf("malformed update receipt = %v, want permanent error", err)
			}
		})
	}
	h.assertCounter(`yabot_chats_events_total{outcome="invalid_update"}`, 5)
	h.assertLog(slog.LevelError, "malformed chat membership update", "update_id", 2)
	if got := h.snapshot(chatID); got != before || h.water(chatID).ID != 1 {
		t.Fatalf("malformed update changed committed state: %+v", got)
	}
	h.assertNoRequest()
}

func TestRuntimeUnknownNewMemberStatusKeepsEarlierValidUpdatesEligible(t *testing.T) {
	t.Parallel()
	const recorded, recordless = -13103, -13104
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) { h.seedActive(recorded, 1) }})
	before := h.snapshot(recorded)
	version := h.chats.Version()
	var unknown models.ChatMember
	if err := json.Unmarshal([]byte(`{"status":"future_status","user":{"id":44,"is_bot":true}}`), &unknown); err != nil {
		t.Fatal(err)
	}
	for i, id := range []int64{recorded, recordless} {
		malformed := runtimeUpdate(id, int64(3+2*i), h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false)
		malformed.MyChatMember.NewChatMember = unknown
		if err := h.update(malformed).Wait(runtimeWait); !dispatch.IsPermanent(err) {
			t.Fatalf("unknown member status for chat %d receipt = %v, want permanent error", id, err)
		}
	}
	h.assertCounter(`yabot_chats_events_total{outcome="invalid_update"}`, 2)
	if got := h.snapshot(recorded); got != before || h.water(recorded) != before.Watermark || h.chats.Version() != version {
		t.Fatalf("unknown member status changed the committed record: %+v", got)
	}
	if _, ok := h.chats.State(recordless); ok {
		t.Fatal("unknown member status created a record")
	}
	h.assertNoRequest()

	h.apply(runtimeUpdate(recorded, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
	if got := h.snapshot(recorded); got.State != Removed || h.water(recorded).ID != 2 {
		t.Fatalf("earlier valid departure did not apply: %+v", got)
	}
	h.apply(runtimeUpdate(recordless, 4, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false))
	if got := h.snapshot(recordless); got.State != Onboarding || h.water(recordless).ID != 4 || got.EverActive {
		t.Fatalf("earlier valid join did not apply cleanly: %+v", got)
	}
}

func TestRuntimeAdmissionSwitchRejectsJoinsAndProbeRecovery(t *testing.T) {
	t.Parallel()
	const absentID, removedID = -13110, -13111
	h := newRuntimeHarness(t, runtimeOptions{
		beforeStart: func(h *runtimeHarness) {
			h.seedActive(removedID, 1)
			h.exec(`UPDATE chats SET state = 'removed', removed_at = ? WHERE chat_id = ?`, h.now().Unix(), removedID)
		},
		configure: func(deps *Deps) { deps.AcceptNewChats = false },
	})
	original := h.snapshot(removedID)
	version := h.chats.Version()
	for _, id := range []int64{absentID, removedID} {
		update := runtimeUpdate(id, 2, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeAdministrator, true)
		h.apply(update)
		leave := h.nextRequest("leaveChat")
		if leave.ChatID != id || leave.Class != sender.Background || leave.NoRetry {
			t.Fatalf("rejecting chat %d used class %v, chat %d, NoRetry %v", id, leave.Class, leave.ChatID, leave.NoRetry)
		}
		h.finishRequest(leave, "true")
		h.settleEffects(id)
		h.apply(update)
		h.assertNoRequest()
	}
	if _, exists := h.chats.State(absentID); exists {
		t.Fatal("emergency rejection created a chat record")
	}
	if got := h.snapshot(removedID); got.State != Removed || got.MemberEpoch != original.MemberEpoch ||
		h.water(removedID).ID != 2 || h.chats.Version() != version {
		t.Fatalf("emergency rejection did not preserve the removed state and advance its watermark: %+v", got)
	}
	h.finishRequest(h.startReportedProbe(absentID), `{"type":"supergroup","title":"Probe observed chat"}`)
	h.finishRequest(h.nextRequest("getChatMember"), runtimeProbeAdmin)
	h.finishRequest(h.nextRequest("getChatAdministrators"), runtimeProbeOwner)
	leave := h.nextRequest("leaveChat")
	if leave.ChatID != absentID || leave.Class != sender.Background {
		t.Fatalf("unexpected rejection after Probe: chat=%d class=%v", leave.ChatID, leave.Class)
	}
	h.finishRequest(leave, "true")
	h.settleEffects(absentID)
	if _, exists := h.chats.State(absentID); exists {
		t.Fatal("emergency rejection allowed Probe to restore a chat")
	}
}

func TestRuntimeViewChangeRingReturnsNewestCompleteWindow(t *testing.T) {
	t.Parallel()
	var v view
	snapshots := make([]Change, 0, 4097)
	states := [...]ChatState{Active, Removed}
	for i := range 4097 {
		id := int64(-2004 - i%3)
		state := states[i/3%2]
		v.publish(&chatEntity{state: State{Record: Record{ChatID: id, State: state}}})
		snapshots = append(snapshots, Change{Version: uint64(i + 1), ChatID: id, State: state})
	}
	if got := v.Version(); got != 4097 {
		t.Fatalf("view version = %d, want 4097", got)
	}
	for _, version := range []uint64{0, 4098} {
		if changes, now, full := v.ChangesSince(version); !full || now != 4097 || changes != nil {
			t.Fatalf("unavailable view version %d returned %d changes, version %d, full %v", version, len(changes), now, full)
		}
	}
	changes, now, full := v.ChangesSince(1)
	if full || now != 4097 || len(changes) != 4096 {
		t.Fatalf("boundary version returned %d changes, version %d, full %v", len(changes), now, full)
	}
	for i, got := range changes {
		if want := snapshots[i+1]; got != want {
			t.Fatalf("change %d = %+v, want %+v", i, got, want)
		}
	}
	if delta, current, full := v.ChangesSince(now); delta != nil || current != now || full {
		t.Fatalf("current version returned %v, %d, %v", delta, current, full)
	}
}

func TestRuntimeViewCensusAndOwnershipTrackCommittedChanges(t *testing.T) {
	t.Parallel()
	const activeID, onboardingID, ownerID = -2010, -2011, 81
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
		h.seedActive(activeID, 1)
		h.exec(`INSERT INTO chats (chat_id, type, title, state, updated_at)
			VALUES (?, 'supergroup', 'Onboarding', 'onboarding', ?),
				(-2012, 'channel', 'Degraded', 'degraded', ?),
				(-2013, 'group', 'Removed', 'removed', ?)`,
			onboardingID, h.now().Unix(), h.now().Unix(), h.now().Unix())
	}})

	names := []string{
		`yabot_chats{state="onboarding"}`, `yabot_chats{state="active"}`, `yabot_chats{state="degraded"}`,
		`yabot_chats{state="removed"}`, "yabot_chats_owner_missing",
	}
	registered := h.metrics.ListMetricNames()
	for _, name := range names {
		if !slices.Contains(registered, name) {
			t.Fatalf("missing gauge %s", name)
		}
	}
	assertCensus := func(want ...float64) {
		t.Helper()
		for i, name := range names {
			if got := h.metrics.GetOrCreateGauge(name, nil).Get(); got != want[i] {
				t.Fatalf("%s = %g, want %g", name, got, want[i])
			}
		}
	}
	assertCensus(1, 1, 1, 1, 2)
	if got := h.chats.OwnedBy(0); len(got) != 0 {
		t.Fatalf("owner query with no user ID includes ownerless chats: %+v", got)
	}
	version := h.chats.Version()
	for range 2 {
		if err := h.setOwner(t.Context(), onboardingID, ownerID); err != nil {
			t.Fatal(err)
		}
		if got := h.chats.Version(); got != version {
			t.Fatalf("owner claim advanced version from %d to %d", version, got)
		}
		assertCensus(1, 1, 1, 1, 1)
	}
	h.apply(runtimeUpdate(activeID, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
	assertCensus(1, 0, 1, 2, 1)
	refs := h.chats.OwnedBy(ownerID)
	if len(refs) != 1 || refs[0].ID != onboardingID || refs[0].State != Onboarding {
		t.Fatalf("owner chats after an owned chat was removed = %+v", refs)
	}
	h.chats.ReportMigrated(-2012, -2014)
	h.tick()
	h.settleEffects(-2012)
	assertCensus(1, 0, 0, 3, 0)
}

func TestRuntimeViewAndReporterWorkWithoutMetrics(t *testing.T) {
	t.Parallel()
	const id = -2015
	h := newRuntimeHarness(t, runtimeOptions{
		beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) },
		configure:   func(d *Deps) { d.Metrics = nil },
	})
	before := h.chats.Version()
	h.chats.ReportMigrated(id, id-1)
	h.tick()
	h.settleEffects(id)
	if got := h.snapshot(id); got.State != Removed || got.MigratedTo != id-1 {
		t.Fatalf("migration without metrics = %+v", got)
	}
	if got, now, full := h.chats.ChangesSince(before); full || now != before+1 ||
		!slices.Equal(got, []Change{{Version: now, ChatID: id, State: Removed}}) {
		t.Fatalf("migration without metrics returned changes=%+v, version=%d, full=%v", got, now, full)
	}
	for i := range 10_001 {
		h.chats.ReportUnknown(int64(-2100 - i))
	}
}

func TestRuntimePeriodicCursorSelectsNextDueChatAcrossBuckets(t *testing.T) {
	t.Parallel()
	ids := []int64{254, 255, 257}
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
		for _, id := range ids {
			h.seedActive(id, 1)
		}
	}})
	job := h.janitor["chats_probe"]
	for _, id := range []int64{254, 255, 257, 254} {
		if n, err := job.run(t.Context()); err != nil || n != 1 {
			t.Fatalf("periodic selection for chat %d returned %d, %v", id, n, err)
		}
		probe := h.nextRequest("getChat")
		if probe.ChatID != id {
			t.Fatalf("selected chat %d, want %d", probe.ChatID, id)
		}
		probe.Done(sender.DroppedResult(sender.Busy, sender.ErrBusy))
		h.settleEffects(id)
		h.advance(24 * time.Hour)
	}
}

func TestRuntimeClearAndRecreateNeverReusesMemberEpoch(t *testing.T) {
	t.Parallel()
	const chatID = -12004
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) {
		h.seedActive(chatID, 10)
	}})
	original := h.snapshot(chatID)
	active := h.metrics.GetOrCreateGauge(`yabot_chats{state="active"}`, nil)
	removedCount := h.metrics.GetOrCreateGauge(`yabot_chats{state="removed"}`, nil)
	ownerMissing := h.metrics.GetOrCreateGauge("yabot_chats_owner_missing", nil)
	if active.Get() != 1 || removedCount.Get() != 0 || ownerMissing.Get() != 0 {
		t.Fatal("preload census did not count the active chat and its owner")
	}
	h.apply(runtimeUpdate(chatID, 11, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
	removed := h.snapshot(chatID)
	if removed.MemberEpoch != original.MemberEpoch {
		t.Fatalf("leaving changed member epoch: before=%d after=%d", original.MemberEpoch, removed.MemberEpoch)
	}
	if active.Get() != 0 || removedCount.Get() != 1 || ownerMissing.Get() != 0 {
		t.Fatal("removal did not update the census")
	}
	beforeDelete := h.chats.Version()
	if err := h.dispatcher.Submit(keyOf(chatID), func(_ context.Context, sh *dispatch.Shard) {
		h.chats.enqueueInternal(sh, keyOf(chatID), ClearFinished{RemovedAt: removed.RemovedAt, MemberEpoch: removed.MemberEpoch})
	}); err != nil {
		t.Fatal(err)
	}
	h.settleEffects(chatID)
	if _, ok := h.chats.State(chatID); ok {
		t.Fatal("clear completion left a view entry")
	}
	records := make(chan int, 1)
	if err := h.dispatcher.Submit(keyOf(chatID), func(_ context.Context, sh *dispatch.Shard) {
		records <- len(h.chats.local(sh).records)
	}); err != nil {
		t.Fatal(err)
	}
	if count := runtimeReceive(t, records); count != 0 {
		t.Fatalf("clear completion retained %d registered records on shard", count)
	}
	var count int
	if err := h.store.Read().QueryRowContext(t.Context(), "SELECT count(*) FROM chats WHERE chat_id = ?", chatID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("cleared row count = %d (%v)", count, err)
	}
	h.apply(runtimeUpdate(chatID, 12, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeAdministrator, true))
	recreated := h.snapshot(chatID)
	if recreated.MemberEpoch <= removed.MemberEpoch || recreated.State != Active {
		t.Fatalf("recreated chat = %+v, old epoch %d", recreated, removed.MemberEpoch)
	}
	if active.Get() != 1 || removedCount.Get() != 0 || ownerMissing.Get() != 1 {
		t.Fatal("deletion and reentry did not update the census and missing owner count")
	}
	changes, now, full := h.chats.ChangesSince(beforeDelete)
	if full || len(changes) != 2 || changes[0].State != NoRecord || changes[1].State != Active || now != beforeDelete+2 {
		t.Fatalf("delete/recreate changes = %v, %d, %v", changes, now, full)
	}
}

func TestRuntimeClearPublishesDeletionWithPinnedRecordlessMemory(t *testing.T) {
	t.Parallel()
	const chatID, pinnedID = -12005, -12006
	const evictions = `yabot_chats_memory_evictions_total{reason="capacity"}`
	for _, tc := range []struct {
		name     string
		inFlight bool
	}{
		{name: "drops unpinned facts"},
		{name: "keeps in-flight Probe until completion", inFlight: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newRuntimeHarness(t, runtimeOptions{memoryCapacity: 1, beforeStart: func(h *runtimeHarness) {
				h.seedActive(chatID, 1)
			}})
			h.apply(runtimeUpdate(chatID, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
			removed, version := h.snapshot(chatID), h.chats.Version()
			pinned := h.startReportedProbe(pinnedID)
			if tc.inFlight {
				probe := h.startReportedProbe(chatID)
				submitRuntimeClear(t, h, removed)
				h.settleEffects(chatID)
				h.assertCounter(evictions, 0)
				probe.Done(sender.DroppedResult(sender.Expired, sender.ErrWouldMiss))
				h.settleEffects(chatID)
				h.assertCounter(`yabot_chats_probe_total{trigger="report_unknown",outcome="expired"}`, 1)
			} else {
				submitRuntimeClear(t, h, removed)
				h.settleEffects(chatID)
			}
			if got, ok := h.chats.State(chatID); ok {
				t.Fatalf("clear left a snapshot: %+v", got)
			}
			var count int
			err := h.store.Read().QueryRowContext(t.Context(), "SELECT count(*) FROM chats WHERE chat_id = ?", chatID).Scan(&count)
			if err != nil || count != 0 {
				t.Fatalf("cleared row count = %d (%v)", count, err)
			}
			changes, now, full := h.chats.ChangesSince(version)
			if full || !slices.Equal(changes, []Change{{Version: version + 1, ChatID: chatID, State: NoRecord}}) || now != version+1 {
				t.Fatalf("clear changes = %v, %d, %v", changes, now, full)
			}
			h.assertCounter(evictions, 1)
			pinned.Done(sender.DroppedResult(sender.Busy, sender.ErrBusy))
			h.settleEffects(pinnedID)
			h.assertCounter(`yabot_chats_probe_total{trigger="report_unknown",outcome="busy"}`, 1)
			h.assertProbeTicket(0)
		})
	}
}

func TestRuntimeClearWriteFailurePreservesRecordAndMemoryCapacity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		async bool
	}{
		{name: "submission rejected"},
		{name: "delete rolled back", async: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const chatID, otherID = -12008, -12009
			var fail atomic.Bool
			h := newRuntimeHarness(t, runtimeOptions{
				memoryCapacity: 1,
				beforeStart:    func(h *runtimeHarness) { h.seedActive(chatID, 1) },
				configure: func(d *Deps) {
					actual := d.Store
					d.Store = maintenanceStore{Store: actual, write: func(op store.Op, done func(error)) error {
						if fail.Swap(false) {
							if !tc.async {
								return store.ErrBusy
							}
							op = func(*sql.Tx) error { return store.ErrRollbackOp }
						}
						return actual.Write(op, done)
					}}
				},
			})
			h.apply(runtimeUpdate(chatID, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
			removed, version := h.snapshot(chatID), h.chats.Version()
			fail.Store(true)
			submitRuntimeClear(t, h, removed)
			h.settleEffects(chatID)
			if got := h.snapshot(chatID); got != removed || h.water(chatID) != removed.Watermark || h.chats.Version() != version {
				t.Fatalf("failed clear changed the row or view: %+v", got)
			}
			req := h.startReportedProbe(otherID)
			if req.ChatID != otherID {
				t.Fatalf("probe targeted chat %d, want %d", req.ChatID, otherID)
			}
			req.Done(sender.DroppedResult(sender.Busy, sender.ErrBusy))
			h.settleEffects(otherID)
		})
	}
}

func submitRuntimeClear(t *testing.T, h *runtimeHarness, removed ChatSnapshot) {
	t.Helper()
	k := keyOf(removed.ChatID)
	if err := h.dispatcher.Submit(k, func(_ context.Context, sh *dispatch.Shard) {
		h.chats.enqueueInternal(sh, k, ClearFinished{RemovedAt: removed.RemovedAt, MemberEpoch: removed.MemberEpoch})
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStoreCompletionPanicReleasesHoldAndUnusedTicket(t *testing.T) {
	t.Parallel()
	const chatID = -12100
	var panicOnProbeStart atomic.Bool
	h := newRuntimeHarness(t, runtimeOptions{
		tickets: 1,
		beforeStart: func(h *runtimeHarness) {
			h.seedActive(chatID, 1)
		},
		configure: func(deps *Deps) {
			now, actual := deps.Now, deps.Store
			deps.Now = func() time.Time {
				if panicOnProbeStart.Swap(false) {
					panic("injected probe start panic")
				}
				return now()
			}
			firstWrite := true
			deps.Store = maintenanceStore{Store: actual, write: func(op store.Op, done func(error)) error {
				inject := firstWrite
				firstWrite = false
				return actual.Write(op, func(err error) {
					if inject && err == nil {
						panicOnProbeStart.Store(true)
					}
					done(err)
				})
			}}
		},
	})
	update := runtimeUpdate(chatID, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, false)
	err := h.update(update).Wait(runtimeWait)
	var panicErr *dispatch.PanicError
	if !errors.As(err, &panicErr) || !dispatch.IsPermanent(err) {
		t.Fatalf("receipt after commit callback panic = %v", err)
	}
	h.settleEffects(chatID)
	h.assertNoRequest()
	if got := h.snapshot(chatID); got.State != Degraded || h.water(chatID).ID != 2 {
		t.Fatalf("committed state after completion panic = %+v", got)
	}
	if got := h.metrics.GetOrCreateGauge(`yabot_dispatch_tickets_inuse{shard="0"}`, nil).Get(); got != 0 {
		t.Fatalf("in-use tickets after completion panic = %g", got)
	}
	h.apply(runtimeUpdate(chatID, 3, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
	if got := h.snapshot(chatID); got.State != Removed || h.water(chatID).ID != 3 {
		t.Fatalf("later update did not run after completion panic: %+v", got)
	}
}

type rejectFirstChatWrite struct {
	Store
	reject error
}

func (s *rejectFirstChatWrite) Write(op store.Op, done func(error)) error {
	if s.reject != nil {
		err := s.reject
		s.reject = nil
		return err
	}
	return s.Store.Write(op, done)
}

func TestRuntimeSynchronousWriteRejectionCancelsTicketBeforeRetry(t *testing.T) {
	t.Parallel()
	const chatID = -12101
	h := newRuntimeHarness(t, runtimeOptions{
		tickets: 1,
		configure: func(deps *Deps) {
			deps.Store = &rejectFirstChatWrite{Store: deps.Store, reject: store.ErrBusy}
		},
	})
	update := runtimeUpdate(chatID, 1, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false)
	if err := h.update(update).Wait(runtimeWait); !errors.Is(err, store.ErrBusy) || dispatch.IsPermanent(err) {
		t.Fatalf("sync failure receipt = %v", err)
	}
	if _, exists := h.chats.State(chatID); exists {
		t.Fatal("sync failure published uncommitted membership")
	}
	h.apply(update)
	req := h.nextRequest("getChat")
	req.Done(sender.DroppedResult(sender.Shutdown, sender.ErrClosed))
	h.settleEffects(chatID)
}

func TestRuntimeInternalTicketExhaustionReplaysAtHead(t *testing.T) {
	t.Parallel()
	const chatID = -12102
	h := newRuntimeHarness(t, runtimeOptions{
		tickets:     1,
		beforeStart: func(h *runtimeHarness) { h.seedActive(chatID, 1) },
	})
	tickets := make(chan dispatch.Ticket, 1)
	if err := h.dispatcher.Submit(keyOf(chatID), func(_ context.Context, sh *dispatch.Shard) {
		ticket, _ := sh.Admit()
		tickets <- ticket
	}); err != nil {
		t.Fatal(err)
	}
	ticket := runtimeReceive(t, tickets)
	cancel := true
	t.Cleanup(func() {
		if cancel {
			ticket.Cancel()
		}
	})
	h.chats.ReportMigrated(chatID, chatID-1)
	h.tick()
	h.barrier(chatID)
	rc := h.update(runtimeUpdate(chatID, 2, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false))
	h.barrier(chatID)
	if got := h.snapshot(chatID).State; got != Active {
		t.Fatalf("event bypassed held migration: %s", got)
	}
	ticket.Cancel()
	cancel = false
	h.advance(time.Second)
	h.tick()
	if err := rc.Wait(runtimeWait); err != nil {
		t.Fatal(err)
	}
	if got := h.snapshot(chatID); got.State != Onboarding || got.Watermark.ID != 2 {
		t.Fatalf("deferred membership after migration replay = %+v", got)
	}
	h.assertNoLog(slog.LevelError, "dispatch task panicked")
}
