package chats

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot/models"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/i18n"
	"github.com/INKCR0W/yabot/internal/platform/obs"
	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
	"github.com/INKCR0W/yabot/internal/tenant/settings"
)

const (
	runtimeWait  = 5 * time.Second
	runtimeBotID = 44
)

type runtimeOptions struct {
	shards, tickets int
	memoryCapacity  int
	beforeStart     func(*runtimeHarness)
	configure       func(*Deps)
	deferChatsStart bool
	migrate         bool
}

type runtimeHarness struct {
	t          *testing.T
	chats      *Chats
	store      *store.Store
	dispatcher *dispatch.Dispatcher
	router     *dispatch.Router
	sender     *runtimeSender
	metrics    *metrics.Set
	logs       runtimeLogs
	settings   *settings.Registry
	catalog    *i18n.Catalog
	janitor    runtimeJanitor
	clock      atomic.Int64
	ticks      []chan time.Time
	ticked     []chan struct{}
}

type runtimeJanitor map[string]struct {
	every time.Duration
	run   func(context.Context) (int, error)
}

type runtimeLogs struct {
	mu      sync.Mutex
	records bytes.Buffer
}

func (l *runtimeLogs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.records.Write(p)
}

func (h *runtimeHarness) assertLog(level slog.Level, message string, attrs ...any) {
	h.t.Helper()
	if !h.hasLog(level, message, attrs...) {
		h.t.Fatalf("missing %s log %q with %v", level, message, attrs)
	}
}

func (h *runtimeHarness) assertNoLog(level slog.Level, message string, attrs ...any) {
	h.t.Helper()
	if h.hasLog(level, message, attrs...) {
		h.t.Fatalf("unexpected %s log %q with %v", level, message, attrs)
	}
}

func (h *runtimeHarness) hasLog(level slog.Level, message string, attrs ...any) bool {
	h.t.Helper()
	return h.logCount(level, message, attrs...) != 0
}

func (h *runtimeHarness) logCount(level slog.Level, message string, attrs ...any) int {
	h.t.Helper()
	count := 0
	h.logs.mu.Lock()
	lines := strings.Split(h.logs.records.String(), "\n")
	h.logs.mu.Unlock()
	for _, line := range lines {
		if line == "" {
			continue
		}
		var record map[string]any
		decoder := json.NewDecoder(strings.NewReader(line))
		decoder.UseNumber()
		if err := decoder.Decode(&record); err != nil {
			h.t.Fatalf("decode log: %v", err)
		}
		if record["msg"] != message {
			continue
		}
		matches := true
		for i := 0; i < len(attrs); i += 2 {
			if fmt.Sprint(record[attrs[i].(string)]) != fmt.Sprint(attrs[i+1]) {
				matches = false
				break
			}
		}
		if matches && record["level"] == level.String() {
			count++
		}
	}
	return count
}

func (h *runtimeHarness) assertCounter(name string, value uint64) {
	h.t.Helper()
	if got := h.metrics.GetOrCreateCounter(name).Get(); got != value {
		h.t.Fatalf("%s = %d, want %d", name, got, value)
	}
}

func (j runtimeJanitor) Register(name string, every time.Duration, run func(context.Context) (int, error)) {
	j[name] = struct {
		every time.Duration
		run   func(context.Context) (int, error)
	}{every, run}
}

type runtimeSender struct {
	requests chan sender.Request
	mu       sync.Mutex
	pending  map[uint64]func(sender.Result)
	last     uint64
	closed   bool
	reject   string
	panicOn  string
}

func (s *runtimeSender) Enqueue(req sender.Request) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.closed:
		return sender.ErrClosed
	case req.Method == s.reject:
		return sender.ErrBusy
	case req.Method == s.panicOn:
		panic("injected sender enqueue panic")
	}
	s.last++
	id, done := s.last, req.Done
	req.Done = func(result sender.Result) {
		s.mu.Lock()
		_, pending := s.pending[id]
		delete(s.pending, id)
		s.mu.Unlock()
		if pending {
			done(result)
		}
	}
	s.pending[id] = req.Done
	select {
	case s.requests <- req:
		return nil
	default:
		delete(s.pending, id)
		return sender.ErrBusy
	}
}

func (s *runtimeSender) stop() {
	s.mu.Lock()
	s.closed = true
	dones := make([]func(sender.Result), 0, len(s.pending))
	for _, done := range s.pending {
		dones = append(dones, done)
	}
	s.mu.Unlock()
	for _, done := range dones {
		done(sender.DroppedResult(sender.Shutdown, sender.ErrClosed))
	}
}

func newRuntimeHarness(t *testing.T, opts runtimeOptions) *runtimeHarness {
	t.Helper()
	if opts.shards == 0 {
		opts.shards = 1
	}
	if opts.tickets == 0 {
		opts.tickets = 16
	}
	h := &runtimeHarness{
		t: t, metrics: metrics.NewSet(), janitor: make(runtimeJanitor),
		ticks: make([]chan time.Time, opts.shards), ticked: make([]chan struct{}, opts.shards),
		sender: &runtimeSender{requests: make(chan sender.Request, 128), pending: make(map[uint64]func(sender.Result))},
	}
	h.clock.Store(time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC).UnixNano())
	logger := slog.New(slog.NewJSONHandler(&h.logs, nil))
	for i := range h.ticks {
		h.ticks[i], h.ticked[i] = make(chan time.Time), make(chan struct{}, 1)
	}
	var err error
	h.store, err = store.Open(t.Context(), store.Deps{
		Dir: runtimeStoreDir(t), CacheKiB: 8000, WriteQueue: 256, CheckpointInterval: time.Hour, BackupKeep: 1,
		Metrics: h.metrics, Health: obs.NewHealth(), Fatal: func(component string, err error) { t.Errorf("%s fatal: %v", component, err) },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { runtimeStop(t, h.store.Stop) })
	migrations := [][]store.Migration{settings.Migrations(), Migrations()}
	if opts.migrate {
		if err = h.store.Migrate(t.Context(), migrations...); err != nil {
			t.Fatal(err)
		}
	}
	if err = h.store.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !opts.migrate {
		err = h.store.WriteWait(t.Context(), func(tx *sql.Tx) error {
			for _, group := range migrations {
				for _, migration := range group {
					if _, err := tx.Exec(migration.SQL); err != nil {
						return fmt.Errorf("create %s test schema: %w", migration.Component, err)
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	h.dispatcher, err = dispatch.New(dispatch.Deps{
		Shards: opts.shards, Inbox: 512, TicketsPerShard: opts.tickets, MailboxPerEntity: 16,
		Metrics: h.metrics, Now: h.now, Log: logger,
		NewTicker: func(shard int, _ time.Duration) (<-chan time.Time, func()) { return h.ticks[shard], nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	h.router = dispatch.NewRouter(h.dispatcher, dispatch.RouterDeps{Metrics: h.metrics})
	h.catalog = i18n.New(i18n.Deps{Metrics: h.metrics})
	h.settings, err = settings.New(settings.Deps{
		Store: h.store, Janitor: h.janitor, Catalog: h.catalog, AuditRetention: time.Hour, Metrics: h.metrics, Now: h.now,
	})
	if err != nil {
		t.Fatal(err)
	}
	deps := Deps{
		Store: h.store, Dispatcher: h.dispatcher, Router: h.router, Sender: h.sender, Janitor: h.janitor,
		Settings: h.settings, Catalog: h.catalog, Metrics: h.metrics, Log: logger, Now: h.now,
		BotID: runtimeBotID, BotUsername: "ExampleGuardBot",
		AcceptNewChats: true, RemovedRetention: 720 * time.Hour, IdleLeaveAfter: 168 * time.Hour,
		ProbePeriodic: 24 * time.Hour, ProbePeriodicRPS: 0.2,
	}
	if opts.configure != nil {
		opts.configure(&deps)
	}
	h.chats, err = New(deps)
	if err != nil {
		t.Fatal(err)
	}
	if opts.memoryCapacity != 0 {
		for i := range opts.shards {
			h.chats.shards.Store(i, newShardState(opts.memoryCapacity))
		}
	}
	h.dispatcher.OnTick(func(_ context.Context, sh *dispatch.Shard) { h.ticked[sh.Index()] <- struct{}{} })
	if opts.beforeStart != nil {
		opts.beforeStart(h)
	}
	if err = h.dispatcher.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { runtimeStop(t, h.dispatcher.Stop) })
	t.Cleanup(h.sender.stop)
	if err = h.settings.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !opts.deferChatsStart {
		if err = h.chats.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { runtimeStop(t, h.chats.Stop) })
	}
	return h
}

func runtimeStoreDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if runtime.GOOS != "windows" {
		return dir
	}
	dir = filepath.Join(dir, "store")
	t.Cleanup(func() {
		// SQLite migration files can still be pending deletion after their last handle closes.
		deadline := time.Now().Add(2 * time.Second)
		for {
			err := os.RemoveAll(dir)
			if err == nil {
				return
			}
			if time.Now().After(deadline) {
				t.Errorf("remove temporary SQLite files: %v", err)
				return
			}
			<-time.After(10 * time.Millisecond)
		}
	})
	return dir
}

func runtimeStop(t *testing.T, stop func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), runtimeWait)
	defer cancel()
	if err := stop(ctx); err != nil {
		t.Error(err)
	}
}

func runtimeReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case item := <-ch:
		return item
	case <-time.After(runtimeWait):
		t.Fatal("timed out waiting for test handshake")
		var zero T
		return zero
	}
}

func (h *runtimeHarness) now() time.Time { return time.Unix(0, h.clock.Load()) }

func (h *runtimeHarness) advance(d time.Duration) { h.clock.Add(int64(d)) }

func (h *runtimeHarness) tick() {
	h.t.Helper()
	for i, ticks := range h.ticks {
		select {
		case ticks <- h.now():
		case <-time.After(runtimeWait):
			h.t.Fatal("timed out delivering dispatcher tick")
		}
		runtimeReceive(h.t, h.ticked[i])
	}
}

func (h *runtimeHarness) update(update *models.Update) dispatch.Receipt {
	return h.router.Accept(update, h.now())
}

func (h *runtimeHarness) apply(update *models.Update) {
	h.t.Helper()
	if err := h.update(update).Wait(runtimeWait); err != nil {
		h.t.Fatalf("settle member update %d: %v", update.ID, err)
	}
}

func (h *runtimeHarness) setOwner(ctx context.Context, chatID, userID int64) error {
	token, err := h.chats.ClaimToken(userID, chatID)
	if err != nil {
		return err
	}
	return h.chats.SetOwner(ctx, chatID, userID, token)
}

func (h *runtimeHarness) barrier(chatID int64) {
	h.t.Helper()
	done := make(chan struct{})
	deadline := time.Now().Add(runtimeWait)
	for {
		err := h.dispatcher.Submit(keyOf(chatID), func(context.Context, *dispatch.Shard) { close(done) })
		if err == nil {
			runtimeReceive(h.t, done)
			return
		}
		if !errors.Is(err, dispatch.ErrBusy) || time.Now().After(deadline) {
			h.t.Fatalf("submit shard barrier: %v", err)
		}
		runtime.Gosched()
	}
}

func (h *runtimeHarness) storeBarrier() {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.t.Context(), runtimeWait)
	defer cancel()
	if err := h.store.WriteWait(ctx, func(*sql.Tx) error { return nil }); err != nil {
		h.t.Fatalf("settle store barrier: %v", err)
	}
}

func (h *runtimeHarness) settleEffects(chatID int64) {
	h.t.Helper()
	h.barrier(chatID)
	h.storeBarrier()
	h.barrier(chatID)
}

func (h *runtimeHarness) nextRequest(method string) sender.Request {
	h.t.Helper()
	req := runtimeReceive(h.t, h.sender.requests)
	if req.Method != method {
		h.t.Fatalf("sender method = %q, want %q", req.Method, method)
	}
	return req
}

func (h *runtimeHarness) assertNoRequest() {
	h.t.Helper()
	select {
	case req := <-h.sender.requests:
		h.t.Fatalf("unexpected sender request: %s", req.Method)
	default:
	}
}

func (h *runtimeHarness) exec(query string, args ...any) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.t.Context(), runtimeWait)
	defer cancel()
	if err := h.store.WriteWait(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, query, args...)
		return err
	}); err != nil {
		h.t.Fatalf("execute test SQL: %v", err)
	}
}

func (h *runtimeHarness) seedActive(id, watermark int64) {
	h.t.Helper()
	h.exec(`INSERT INTO chats (chat_id, type, title, state, bot_rights, owner_id, added_at, updated_at, ever_active,
		member_event_date, member_event_update_id) VALUES (?, 'supergroup', 'Initial title', 'active', ?, 81, ?, ?, 1, ?, ?)`,
		id, InviteUsers, h.now().Unix(), h.now().Unix(), h.now().Unix(), watermark)
}

func (h *runtimeHarness) water(chatID int64) dispatch.Order {
	h.t.Helper()
	var w dispatch.Order
	if err := h.store.Read().QueryRowContext(h.t.Context(), `SELECT member_event_date, member_event_update_id FROM chats WHERE chat_id=?`, chatID).Scan(&w.Date, &w.ID); err != nil {
		h.t.Fatal(err)
	}
	return w
}

func (h *runtimeHarness) snapshot(chatID int64) ChatSnapshot {
	h.t.Helper()
	snap, ok := h.chats.State(chatID)
	if !ok {
		h.t.Fatalf("chat %d missing from the snapshot", chatID)
	}
	return snap
}

func runtimeUpdate(chatID, id, date int64, old, next models.ChatMemberType, invite bool) *models.Update {
	return &models.Update{ID: id, MyChatMember: &models.ChatMemberUpdated{
		Chat: models.Chat{ID: chatID, Type: models.ChatTypeSupergroup, Title: "Initial title"},
		From: models.User{ID: 83, LanguageCode: "zh-CN"}, Date: int(date),
		OldChatMember: runtimeMember(old, true), NewChatMember: runtimeMember(next, invite),
	}}
}

func runtimeMember(kind models.ChatMemberType, invite bool) models.ChatMember {
	m := models.ChatMember{Type: kind}
	if kind == models.ChatMemberTypeAdministrator {
		m.Administrator = &models.ChatMemberAdministrator{CanInviteUsers: invite}
	}
	return m
}

func TestRuntimePreloadAndMemberOrder(t *testing.T) {
	t.Parallel()
	const startID int64 = -10060
	h := newRuntimeHarness(t, runtimeOptions{shards: 4, beforeStart: func(h *runtimeHarness) {
		for i := range int64(16) {
			h.seedActive(startID+i, 10)
		}
	}})
	if got := h.router.AllowedUpdates(); !slices.Equal(got, []string{"my_chat_member"}) {
		t.Fatalf("allowed updates = %v", got)
	}
	startVersion := h.chats.Version()
	for i := range int64(16) {
		id := startID + i
		before := h.snapshot(id)
		if before.State != Active || before.OwnerID != 81 || before.MemberEpoch == 0 {
			t.Fatalf("preloaded snapshot for %d = %+v", id, before)
		}
		old := runtimeUpdate(id, 9, h.now().Unix()+1, models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false)
		h.apply(old)
		if got := h.snapshot(id); got != before || h.water(id).ID != 10 {
			t.Fatalf("old update changed preloaded record: %+v", got)
		}
	}
	newer := runtimeUpdate(startID, 12, h.now().Unix()-1, models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, true)
	h.apply(newer)
	if w := h.water(startID); w.ID != 12 || w.Date != h.now().Unix()-1 || h.chats.Version() != startVersion {
		t.Fatalf("unchanged state did not persist order without publishing a change: %+v, version %d", w, h.chats.Version())
	}
	h.apply(runtimeUpdate(startID, 11, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
	h.apply(newer)
	if h.water(startID).ID != 12 || h.snapshot(startID).State != Active {
		t.Fatal("older and duplicate updates changed the record")
	}
	private := runtimeUpdate(startID, 13, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false)
	private.MyChatMember.Chat.Type = models.ChatTypePrivate
	h.apply(private)
	h.barrier(startID)
	if h.water(startID).ID != 12 || h.snapshot(startID).State != Active {
		t.Fatal("private my_chat_member changed a group record")
	}
	if got := h.metrics.GetOrCreateCounter(`yabot_dispatch_stale_updates_total{route="my_chat_member"}`).Get(); got != 18 {
		t.Fatalf("stale update count = %d, want 18", got)
	}
	h.assertCounter(`yabot_chats_events_total{outcome="stale_completion"}`, 0)
	h.assertLog(slog.LevelInfo, "stale chat membership update", "chat_id", startID, "update_id", 12)
	h.assertNoRequest()
}

func TestRuntimeEvaluatesWatermarkAfterHold(t *testing.T) {
	t.Parallel()
	const id = -10066
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) }})
	entered, release := make(chan struct{}), make(chan struct{})
	releaseWriter := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseWriter)
	done := make(chan error, 1)
	if err := h.store.Write(func(*sql.Tx) error {
		close(entered)
		<-release
		return nil
	}, func(err error) { done <- err }); err != nil {
		t.Fatal(err)
	}
	runtimeReceive(t, entered)
	newer := h.update(runtimeUpdate(id, 3, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
	older := h.update(runtimeUpdate(id, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeAdministrator, true))
	h.barrier(id)
	if got := h.snapshot(id).State; got != Active {
		t.Fatalf("uncommitted member update became visible: %s", got)
	}
	releaseWriter()
	if err := errors.Join(runtimeReceive(t, done), newer.Wait(runtimeWait), older.Wait(runtimeWait)); err != nil {
		t.Fatal(err)
	}
	if got := h.snapshot(id); got.State != Removed || h.water(id).ID != 3 {
		t.Fatalf("deferred stale update regressed chat: %+v", got)
	}
	if got := h.metrics.GetOrCreateCounter(`yabot_dispatch_stale_updates_total{route="my_chat_member"}`).Get(); got != 1 {
		t.Fatalf("stale count = %d, want 1", got)
	}
}

func TestRuntimeCommitFailureDoesNotPublishEpochOrderOrEffects(t *testing.T) {
	t.Parallel()
	const id = -10067
	h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) }})
	before, version := h.snapshot(id), h.chats.Version()
	h.exec(`CREATE TABLE test_parent (id INTEGER PRIMARY KEY)`)
	h.exec(`CREATE TABLE test_child (id INTEGER REFERENCES test_parent (id) DEFERRABLE INITIALLY DEFERRED)`)
	h.exec(`CREATE TRIGGER test_fail_commit AFTER UPDATE ON chats WHEN NEW.member_event_update_id = 2
		BEGIN INSERT INTO test_child VALUES (1); END`)
	reentry := runtimeUpdate(id, 2, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false)
	err := h.update(reentry).Wait(runtimeWait)
	if !errors.Is(err, store.ErrAborted) || dispatch.IsPermanent(err) {
		t.Fatalf("failed commit receipt = %v, want transient batch abort", err)
	}
	h.barrier(id)
	h.assertCounter(`yabot_chats_write_failures_total{outcome="retry"}`, 1)
	h.assertCounter(`yabot_chats_write_failures_total{outcome="op_error"}`, 0)
	h.assertLog(slog.LevelInfo, "chat write will be retried or rejected", "chat_id", id)
	if got := h.snapshot(id); got != before || h.water(id).ID != 1 || h.chats.Version() != version {
		t.Fatalf("failed commit changed state, order, epoch, or view version: %+v", got)
	}
	h.assertNoRequest()
	h.exec(`DROP TRIGGER test_fail_commit`)
	h.apply(reentry)
	if got := h.snapshot(id); got.State != Onboarding || got.MemberEpoch != before.MemberEpoch+1 || got.OwnerID != 0 ||
		h.water(id).ID != 2 || h.chats.Version() != version+1 {
		t.Fatalf("retried update did not commit the expected reentry: %+v", got)
	}
	if req := h.nextRequest("getChat"); req.ChatID != id {
		t.Fatalf("probe chat id = %d, want %d", req.ChatID, id)
	}
}

func TestRuntimeMembershipWriteReceiptRedactsSQLiteErrors(t *testing.T) {
	t.Parallel()
	const id = -10068
	const triggerDetail = "private SQLite trigger detail"
	for _, tc := range []struct {
		name       string
		raise      string
		permanent  bool
		sqliteCode int
	}{
		{name: "operation rollback", raise: "ABORT", permanent: true, sqliteCode: sqlite3.SQLITE_CONSTRAINT_TRIGGER},
		{name: "batch rollback", raise: "ROLLBACK"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRuntimeHarness(t, runtimeOptions{beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) }})
			h.exec(fmt.Sprintf(`CREATE TRIGGER test_fail_op BEFORE UPDATE ON chats WHEN NEW.member_event_update_id = 2
				BEGIN SELECT raise(%s, '%s'); END`, tc.raise, triggerDetail))
			u := runtimeUpdate(id, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false)
			err := h.update(u).Wait(runtimeWait)
			if err == nil || dispatch.IsPermanent(err) != tc.permanent {
				t.Fatalf("write failure receipt = %v, want permanent = %t", err, tc.permanent)
			}
			if _, ok := errors.AsType[*store.OpError](err); ok != tc.permanent || errors.Is(err, store.ErrAborted) != !tc.permanent {
				t.Fatalf("write failure receipt has wrong store classification: %v", err)
			}
			summary := fmt.Sprintf("upsert_chat failed (sqlite_code=%d)", tc.sqliteCode)
			if got := err.Error(); strings.Contains(got, triggerDetail) || !strings.Contains(got, summary) {
				t.Fatalf("write failure receipt did not give a redacted operation and SQLite code: %s", got)
			}
			if _, ok := errors.AsType[*sqlite.Error](err); ok {
				t.Fatal("write failure receipt wraps the raw SQLite error")
			}
			if tc.permanent {
				h.assertCounter(`yabot_chats_write_failures_total{outcome="op_error"}`, 1)
				h.assertCounter(`yabot_chats_write_failures_total{outcome="retry"}`, 0)
				h.assertLog(slog.LevelError, "chat write failed", "chat_id", id)
			} else {
				h.assertCounter(`yabot_chats_write_failures_total{outcome="op_error"}`, 0)
				h.assertCounter(`yabot_chats_write_failures_total{outcome="retry"}`, 1)
				h.assertLog(slog.LevelInfo, "chat write will be retried or rejected", "chat_id", id)
			}
			if h.water(id).ID != 1 || h.snapshot(id).State != Active {
				t.Fatal("failed write advanced the committed record")
			}
			h.exec(`DROP TRIGGER test_fail_op`)
			h.apply(u)
			if h.water(id).ID != 2 || h.snapshot(id).State != Removed {
				t.Fatal("replayed update did not apply")
			}
		})
	}
}

type runtimePanicLog struct{ slog.Handler }

func (runtimePanicLog) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelError
}

func (runtimePanicLog) Handle(context.Context, slog.Record) error { panic("injected completion panic") }

func TestRuntimeCommitCallbackPanicSettlesAckAndReleasesHold(t *testing.T) {
	t.Parallel()
	const id = -10075
	h := newRuntimeHarness(t, runtimeOptions{
		tickets:     1,
		beforeStart: func(h *runtimeHarness) { h.seedActive(id, 1) },
		configure:   func(d *Deps) { d.Log = slog.New(runtimePanicLog{Handler: slog.DiscardHandler}) },
	})
	h.exec(`CREATE TRIGGER test_callback_panic BEFORE UPDATE ON chats WHEN NEW.member_event_update_id = 2
		BEGIN SELECT raise(ABORT, 'forced operation failure'); END`)
	u := runtimeUpdate(id, 2, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false)
	err := h.update(u).Wait(runtimeWait)
	if _, ok := errors.AsType[*dispatch.PanicError](err); !ok || !dispatch.IsPermanent(err) {
		t.Fatalf("commit callback receipt = %v, want permanent panic", err)
	}
	h.barrier(id)
	h.assertProbeTicket(0)
	h.exec(`DROP TRIGGER test_callback_panic`)
	h.apply(u)
	if h.water(id).ID != 2 || h.snapshot(id).State != Removed {
		t.Fatal("commit callback panic left entity held")
	}
}

func TestRuntimeOldAdministratorEvidenceSurvivesRecordlessWatermark(t *testing.T) {
	t.Parallel()
	const id = -10069
	h := newRuntimeHarness(t, runtimeOptions{})
	h.apply(runtimeUpdate(id, 4, h.now().Unix(), models.ChatMemberTypeAdministrator, models.ChatMemberTypeLeft, false))
	h.apply(runtimeUpdate(id, 3, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false))
	if _, ok := h.chats.State(id); ok {
		t.Fatal("recordless stale update created a chat")
	}
	h.apply(runtimeUpdate(id, 5, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false))
	if got := h.snapshot(id); got.State != Onboarding || !got.EverActive || h.water(id).ID != 5 {
		t.Fatalf("created record lost old administrator evidence: %+v", got)
	}
	h.advance(10 * time.Minute)
	h.tick()
	h.barrier(id)
	h.assertCounter(`yabot_chats_memory_evictions_total{reason="ttl"}`, 0)
}

func TestRuntimeRecordlessWatermarksEvictOldestAndExpire(t *testing.T) {
	t.Parallel()
	const capacity, first = 64, -900000
	h := newRuntimeHarness(t, runtimeOptions{memoryCapacity: capacity, configure: func(d *Deps) {
		d.Store = &rejectFirstChatWrite{Store: d.Store, reject: store.ErrBusy}
	}})
	now := h.now()
	if err := h.dispatcher.Submit(keyOf(first), func(_ context.Context, sh *dispatch.Shard) {
		local := h.chats.local(sh)
		for i := range int64(capacity) {
			entry := &chatEntity{state: State{
				Record:          Record{ChatID: first + i},
				MemoryWatermark: dispatch.Order{Date: now.Unix(), ID: capacity + i + 2},
			}}
			h.chats.retainMemory(sh, local, entry, now)
		}
	}); err != nil {
		t.Fatal(err)
	}
	h.barrier(first)
	h.advance(time.Second)
	attempt := h.update(runtimeUpdate(first, 2*capacity+2, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false))
	if err := attempt.Wait(runtimeWait); !errors.Is(err, store.ErrBusy) {
		t.Fatalf("first record creation did not reach the rejected write: %v", err)
	}
	h.apply(runtimeUpdate(first+capacity, 2*capacity+2, h.now().Unix(), models.ChatMemberTypeMember, models.ChatMemberTypeLeft, false))
	h.assertCounter(`yabot_chats_memory_evictions_total{reason="capacity"}`, 1)
	for _, tc := range []struct {
		chatID int64
		wait   time.Duration
		want   bool
	}{
		{first + 1, 0, true},
		{first + capacity - 1, 10*time.Minute - 2*time.Second, false},
		{first, time.Second, false},
		{first + capacity, time.Second, true},
	} {
		h.advance(tc.wait)
		h.apply(runtimeUpdate(tc.chatID, 1, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false))
		if _, ok := h.chats.State(tc.chatID); ok != tc.want {
			t.Fatalf("record present for %d = %v, want %v", tc.chatID, ok, tc.want)
		}
	}
	h.assertCounter(`yabot_chats_memory_evictions_total{reason="capacity"}`, 2)
	h.assertCounter(`yabot_chats_memory_evictions_total{reason="ttl"}`, capacity-3)
}

func TestRuntimeRecordlessCacheRefreshesReplacedWatermarkBeforeExpiration(t *testing.T) {
	t.Parallel()
	const retained, expired, newID = -61100, -61101, -61102
	h := newRuntimeHarness(t, runtimeOptions{})
	h.apply(runtimeUpdate(retained, 10, h.now().Unix(), models.ChatMemberTypeMember, models.ChatMemberTypeLeft, false))
	h.apply(runtimeUpdate(expired, 10, h.now().Unix(), models.ChatMemberTypeMember, models.ChatMemberTypeLeft, false))
	h.advance(10*time.Minute - time.Second)
	remember := func(id, updateID int64) {
		if err := h.dispatcher.Submit(keyOf(id), func(_ context.Context, sh *dispatch.Shard) {
			now := h.now()
			h.chats.retainMemory(sh, h.chats.local(sh), &chatEntity{state: State{
				Record:          Record{ChatID: id},
				MemoryWatermark: dispatch.Order{Date: now.Unix(), ID: updateID},
			}}, now)
		}); err != nil {
			t.Fatal(err)
		}
		h.barrier(id)
	}
	remember(retained, 20)
	h.advance(time.Second)
	remember(newID, 10)
	h.assertCounter(`yabot_chats_memory_evictions_total{reason="ttl"}`, 1)
	h.apply(runtimeUpdate(retained, 15, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false))
	if _, exists := h.chats.State(retained); exists {
		t.Fatal("stale event overrode the newer cached watermark")
	}
	h.apply(runtimeUpdate(expired, 5, h.now().Unix(), models.ChatMemberTypeLeft, models.ChatMemberTypeMember, false))
	if got := h.snapshot(expired).Watermark.ID; got != 5 {
		t.Fatalf("expired watermark prevented new join, current id %d", got)
	}
}

func TestRuntimeRecordlessCapacityPreservesInFlightProbe(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		allInFlight bool
	}{
		{name: "evicts oldest idle entry"},
		{name: "rejects new probe when all are in flight", allInFlight: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const capacity = 64
			const oldest int64 = -800000
			const incoming = oldest + capacity
			h := newRuntimeHarness(t, runtimeOptions{memoryCapacity: capacity})
			h.chats.ReportUnknown(oldest)
			h.tick()
			firstProbe := h.nextRequest("getChat")
			now := h.now()
			if err := h.dispatcher.Submit(keyOf(oldest), func(_ context.Context, sh *dispatch.Shard) {
				local := h.chats.local(sh)
				for id := oldest + 1; id < incoming; id++ {
					state := State{Record: Record{ChatID: id}, MemoryWatermark: dispatch.Order{Date: now.Unix(), ID: 5}}
					if tc.allInFlight {
						state.Probe.InFlight = ProbeRequest{Trigger: ProbeUnknown}
					}
					h.chats.retainMemory(sh, local, &chatEntity{state: state}, now)
				}
			}); err != nil {
				t.Fatal(err)
			}
			h.barrier(oldest)
			h.chats.ReportUnknown(incoming)
			h.tick()
			h.settleEffects(incoming)
			if tc.allInFlight {
				h.assertNoRequest()
				h.assertProbeTicket(1)
				h.assertLog(slog.LevelWarn, "recordless chat memory is full of pinned entries", "chat_id", incoming)
				h.assertCounter(`yabot_chats_overload_total{source="recordless_memory"}`, 1)
				h.assertCounter(`yabot_chats_memory_evictions_total{reason="capacity"}`, 0)
			} else {
				req := h.nextRequest("getChat")
				if req.ChatID != incoming {
					t.Fatalf("new probe is for chat %d, want %d", req.ChatID, incoming)
				}
				req.Done(sender.DroppedResult(sender.Shutdown, sender.ErrClosed))
				h.assertLog(slog.LevelWarn, "recordless chat memory is full", "chat_id", oldest+1)
				h.assertCounter(`yabot_chats_memory_evictions_total{reason="capacity"}`, 1)
			}
			h.finishRequest(firstProbe, deliveryProbeChat(oldest, models.ChatTypeSupergroup))
			finishDeliveryMembership(h, models.ChatMemberTypeAdministrator, true)
			h.settleEffects(oldest)
			if got := h.snapshot(oldest); got.State != Active || got.OwnerID != 71 {
				t.Fatalf("retained probe did not recover its chat: %+v", got)
			}
			h.nextRequest("sendMessage").Done(sender.Result{Outcome: telegram.Outcome{Class: telegram.OK}})
			h.settleEffects(oldest)
			h.assertProbeTicket(0)
		})
	}
}

func TestRuntimeRecordlessCapacityDefaultsToProductionLimit(t *testing.T) {
	t.Parallel()
	d, err := dispatch.New(dispatch.Deps{Shards: 2, Inbox: 1, TicketsPerShard: 1, MailboxPerEntity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { runtimeStop(t, d.Stop) })
	var chats Chats
	err = d.RunOnAll(t.Context(), func(_ context.Context, sh *dispatch.Shard, done func(error)) {
		if got := chats.local(sh).memoryCapacity; got != 100_000 {
			done(fmt.Errorf("recordless capacity for shard %d = %d, want 100000", sh.Index(), got))
			return
		}
		done(nil)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeMigrationCreatesRequiredIndexes(t *testing.T) {
	t.Parallel()
	h := newRuntimeHarness(t, runtimeOptions{migrate: true})
	var count int
	if err := h.store.Read().QueryRowContext(t.Context(),
		`SELECT count(*) FROM pragma_table_info('chats') WHERE name='added_by'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("adder column count = %d, want 0: %v", count, err)
	}
	if err := h.store.Read().QueryRowContext(t.Context(),
		`SELECT count(*) FROM pragma_index_list('chats')`).Scan(&count); err != nil || count != 3 {
		t.Fatalf("chat index count = %d, want 3: %v", count, err)
	}
	for _, query := range []struct {
		sql, index string
		columns    []string
	}{
		{`SELECT chat_id FROM chats WHERE owner_id=41`, "chats_owner_id", []string{"owner_id"}},
		{`SELECT chat_id FROM chats WHERE state='removed' AND purge_after<=100 ORDER BY purge_after`, "chats_state_purge_after", []string{"state", "purge_after"}},
		{`SELECT chat_id FROM chats WHERE state='degraded' AND updated_at<=100 ORDER BY updated_at`, "chats_state_updated_at", []string{"state", "updated_at"}},
	} {
		var id, parent, aux int
		var detail string
		err := h.store.Read().QueryRowContext(t.Context(), fmt.Sprintf("EXPLAIN QUERY PLAN %s", query.sql)).Scan(&id, &parent, &aux, &detail)
		if err != nil || !strings.Contains(detail, query.index) {
			t.Errorf("query %q uses %q, want %s: %v", query.sql, detail, query.index, err)
		}
		rows, err := h.store.Read().QueryContext(t.Context(), `SELECT name FROM pragma_index_info(?) ORDER BY seqno`, query.index)
		if err != nil {
			t.Fatal(err)
		}
		var columns []string
		for rows.Next() {
			var column string
			if err := rows.Scan(&column); err != nil {
				t.Fatal(errors.Join(err, rows.Close()))
			}
			columns = append(columns, column)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(columns, query.columns) {
			t.Errorf("index %s columns = %v, want %v", query.index, columns, query.columns)
		}
	}
}
