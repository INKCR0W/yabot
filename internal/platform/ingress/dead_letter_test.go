package ingress

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot/models"
)

const (
	poisonMetricName      = "yabot_ingress_poison_total"
	deadLettersMetricName = "yabot_ingress_dead_letters_total"
	privateJoinBio        = "private join request bio"
)

func TestPoisonDeadLetterThreshold(t *testing.T) {
	h := newPollHarness(t)
	s := openDeadLetterStore(t)
	const newID, existingID = 71, 72
	const oldError = "old summary"
	now := time.Date(2026, 11, 25, 12, 0, 1, 234_567_890, time.UTC)
	execDeadLetterSQL(t, s, `INSERT INTO dead_letter (update_id, kind, error, created_at) VALUES (?, ?, ?, ?)`,
		existingID, "old kind", oldError, now.Add(-time.Hour).UnixMilli())

	h.deps.Store = s
	h.deps.Now = func() time.Time { return now }
	h.deps.PoisonThreshold = 3
	cause := dispatch.Permanent(errors.New("invalid join request " + strings.Repeat("雪", 220)))
	registerDeadLetterRoute(h, cause)
	h.start(t)
	h.webhookOK(t)

	call := h.next(t, "getUpdates")
	var offset int64
	var poisons uint64
	for _, tc := range []struct {
		id               int64
		priorSummary     string
		priorDeadLetters uint64
	}{
		{id: newID},
		{id: existingID, priorSummary: oldError, priorDeadLetters: 1},
	} {
		for attempt := 1; attempt <= 3; attempt++ {
			if call.offset != offset {
				t.Fatalf("update %d attempt %d offset = %d, want %d", tc.id, attempt, call.offset, offset)
			}
			assertDeadLetterMetrics(t, h.metrics, poisons, tc.priorDeadLetters)
			var summary string
			err := s.Read().QueryRowContext(t.Context(), `SELECT error FROM dead_letter WHERE update_id = ?`, tc.id).Scan(&summary)
			if tc.priorSummary == "" {
				if !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("update %d attempt %d found dead letter %q, err = %v", tc.id, attempt, summary, err)
				}
			} else if err != nil || summary != tc.priorSummary {
				t.Fatalf("update %d attempt %d summary = %q, err = %v, want %q", tc.id, attempt, summary, err, tc.priorSummary)
			}
			call.ok(poisonBatch(tc.id))
			call = h.next(t, "getUpdates")
			poisons++
		}
		offset = tc.id + 1
		if call.offset != offset {
			t.Fatalf("offset after third failure of update %d = %d, want %d", tc.id, call.offset, offset)
		}
		assertDeadLetterMetrics(t, h.metrics, poisons, tc.priorDeadLetters+1)
		assertDeadLetterRow(t, s, tc.id, now)
	}
	assertDeadLetterColumns(t, s)
}

func TestDeadLetterTransientWriteRetries(t *testing.T) {
	h := newPollHarness(t)
	s := openDeadLetterStore(t)
	writeFailures := []error{
		store.ErrBusy,
		fmt.Errorf("begin write batch: %w", store.ErrAborted),
		store.ErrFaulted,
		errors.New("commit write batch failed"),
	}
	var writes atomic.Int64
	h.deps.Store = testWriter(func(ctx context.Context, op store.Op) error {
		attempt := int(writes.Add(1)) - 1
		if attempt < len(writeFailures) {
			return writeFailures[attempt]
		}
		return s.WriteWait(ctx, op)
	})
	h.deps.PoisonThreshold = 1
	registerDeadLetterRoute(h, dispatch.Permanent(errors.New("invalid join request")))
	h.start(t)
	h.webhookOK(t)

	const id = 81
	for attempt := range len(writeFailures) + 1 {
		call := h.next(t, "getUpdates")
		if call.offset != 0 || writes.Load() != int64(attempt) {
			t.Fatalf("attempt %d offset = %d, writes = %d, want offset 0 and %d writes", attempt+1, call.offset, writes.Load(), attempt)
		}
		assertDeadLetterMetrics(t, h.metrics, uint64(attempt), 0)
		assertDeadLetterCount(t, s, 0)
		call.ok(poisonBatch(id))
	}
	if call := h.next(t, "getUpdates"); call.offset != id+1 {
		t.Fatalf("offset after successful write = %d, want %d", call.offset, id+1)
	}
	assertDeadLetterCount(t, s, 1)
	assertDeadLetterMetrics(t, h.metrics, uint64(len(writeFailures)+1), 1)
}

func TestTransientFailureBudget(t *testing.T) {
	const budget = time.Minute
	transient := errors.New("temporary handler failure")
	for _, tc := range []struct {
		name       string
		receiptErr error
		writeErr   error
	}{
		{name: "receipt", receiptErr: transient},
		{name: "dead_letter_write", receiptErr: dispatch.Permanent(errors.New("invalid join request")), writeErr: store.ErrBusy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newPollHarness(t)
			start := time.Unix(1_700_000_000, 0)
			var elapsed atomic.Int64
			h.deps.Now = func() time.Time { return start.Add(time.Duration(elapsed.Load())) }
			h.deps.TransientBudget = budget
			h.deps.PoisonThreshold = 1
			wantErr := tc.receiptErr
			var wantPoison uint64
			if tc.writeErr != nil {
				h.deps.Store = testWriter(func(context.Context, store.Op) error { return tc.writeErr })
				wantErr = tc.writeErr
				wantPoison = 3
			}
			registerDeadLetterRoute(h, tc.receiptErr)
			i := h.start(t)
			h.webhookOK(t)

			const id = 82
			for _, failedAt := range []time.Duration{0, budget, budget + time.Nanosecond} {
				call := h.next(t, "getUpdates")
				if call.offset != 0 {
					t.Fatalf("retry at %s used offset %d, want 0", failedAt, call.offset)
				}
				elapsed.Store(int64(failedAt))
				call.ok(poisonBatch(id))
			}
			if err := expectDeadLetterFatal(t, h, i, id); !errors.Is(err, wantErr) {
				t.Errorf("Fatal error = %v, want %v", err, wantErr)
			}
			assertDeadLetterMetrics(t, h.metrics, wantPoison, 0)
		})
	}
}

func TestDeadLetterOpErrorIsFatal(t *testing.T) {
	h := newPollHarness(t)
	s := openDeadLetterStore(t)
	execDeadLetterSQL(t, s, `CREATE TRIGGER reject_dead_letter BEFORE INSERT ON dead_letter
		BEGIN SELECT RAISE(ABORT, 'dead letter write rejected'); END`)
	h.deps.Store = s
	h.deps.PoisonThreshold = 1
	registerDeadLetterRoute(h, dispatch.Permanent(errors.New("invalid join request")))
	i := h.start(t)
	h.webhookOK(t)

	const id = 83
	call := h.next(t, "getUpdates")
	if call.offset != 0 {
		t.Fatalf("first offset = %d, want 0", call.offset)
	}
	call.ok(poisonBatch(id))
	err := expectDeadLetterFatal(t, h, i, id)
	var opErr *store.OpError
	if !errors.As(err, &opErr) || !strings.Contains(err.Error(), "dead letter write rejected") {
		t.Errorf("Fatal error = %v, want OpError from SQLite trigger", err)
	}
	assertDeadLetterCount(t, s, 0)
	assertDeadLetterMetrics(t, h.metrics, 1, 0)
}

func TestDeadLetterWriteSurvivesStop(t *testing.T) {
	h := newPollHarness(t)
	s := openDeadLetterStore(t)
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	h.deps.Store = testWriter(func(ctx context.Context, op store.Op) error {
		started <- ctx
		select {
		case <-release:
			return s.WriteWait(ctx, op)
		case <-ctx.Done():
			return ctx.Err()
		case <-t.Context().Done():
			return t.Context().Err()
		}
	})
	h.deps.PoisonThreshold = 1
	registerDeadLetterRoute(h, dispatch.Permanent(errors.New("invalid join request")))
	i := h.start(t)
	h.webhookOK(t)
	h.next(t, "getUpdates").ok(poisonBatch(84))
	ctx := awaitValue(t, started)
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		t.Error("dead letter write has no deadline")
	}
	expired, cancel := context.WithDeadline(t.Context(), time.Time{})
	defer cancel()
	if err := i.Stop(expired); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Stop during dead letter write = %v, want deadline exceeded", err)
	}
	if err := ctx.Err(); err != nil {
		t.Errorf("Stop canceled dead letter write: %v", err)
	}
	close(release)
	call := h.next(t, "getUpdates")
	if call.offset != 85 || call.limit != 1 || call.timeout != 0 {
		t.Errorf("shutdown request offset = %d, limit = %d, timeout = %d, want (85, 1, 0)", call.offset, call.limit, call.timeout)
	}
	assertDeadLetterCount(t, s, 1)
	call.ok(`[]`)
}

func TestDeadLetterSweeper(t *testing.T) {
	s := openDeadLetterStore(t)
	now := time.UnixMilli(1_700_000_000_123)
	const retention = 30 * 24 * time.Hour
	cutoff := now.Add(-retention).UnixMilli()
	for _, entry := range []struct {
		id, createdAt int64
	}{
		{1, cutoff - 1},
		{2, cutoff - 2},
		{3, cutoff - 3},
		{4, cutoff},
		{5, cutoff + 1},
	} {
		execDeadLetterSQL(t, s, `INSERT INTO dead_letter (update_id, kind, error, created_at) VALUES (?, 'message', 'summary', ?)`,
			entry.id, entry.createdAt)
	}
	del := DeadLetterSweeper(retention, func() time.Time { return now })
	for _, step := range []struct {
		advance       time.Duration
		wantDeleted   int
		wantRemaining []int64
	}{
		{wantDeleted: 2, wantRemaining: []int64{1, 4, 5}},
		{wantDeleted: 1, wantRemaining: []int64{4, 5}},
		{wantDeleted: 0, wantRemaining: []int64{4, 5}},
		{advance: time.Millisecond, wantDeleted: 1, wantRemaining: []int64{5}},
	} {
		now = now.Add(step.advance)
		var deleted int
		if err := s.WriteWait(t.Context(), func(tx *sql.Tx) error {
			var err error
			deleted, err = del(tx, 2)
			return err
		}); err != nil || deleted != step.wantDeleted {
			t.Fatalf("sweep deleted %d rows, err = %v, want %d rows", deleted, err, step.wantDeleted)
		}
		got, err := readDeadLetterColumn[int64](t.Context(), s.Read(), `SELECT update_id FROM dead_letter ORDER BY update_id`)
		if err != nil || !slices.Equal(got, step.wantRemaining) {
			t.Fatalf("remaining dead letters = %v, err = %v, want %v", got, err, step.wantRemaining)
		}
	}
}

func TestDeadLetterSweeperRejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name      string
		retention time.Duration
		now       func() time.Time
	}{
		{"zero retention", 0, time.Now},
		{"negative retention", -time.Second, time.Now},
		{"nil clock", time.Second, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("DeadLetterSweeper did not panic")
				}
			}()
			DeadLetterSweeper(tc.retention, tc.now)
		})
	}
}

func registerDeadLetterRoute(h *pollHarness, err error) {
	registerRoute(h.router, "poison", dispatch.KindChatJoinRequest.Mask(), true, func(_ *models.Update, ack dispatch.Ack) {
		ack.Done(err)
	})
}

func poisonBatch(id int64) string {
	return fmt.Sprintf(`[{"update_id":%d,"chat_join_request":{"date":1700000000,"bio":%q}}]`, id, privateJoinBio)
}

func expectDeadLetterFatal(t *testing.T, h *pollHarness, i *Ingress, id int64) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testWait)
	defer cancel()
	var report fatalCall
	select {
	case report = <-h.fatals:
	case call := <-h.calls:
		t.Fatalf("next request was %s with offset %d, want Fatal", call.method, call.offset)
	case <-ctx.Done():
		t.Fatal("timed out waiting for Fatal")
	}
	if report.component != "ingress" || report.err == nil || !strings.Contains(report.err.Error(), fmt.Sprint(id)) {
		t.Fatalf("Fatal(%q, %v), want ingress error mentioning update %d", report.component, report.err, id)
	}
	h.awaitExit(t, i)
	return report.err
}

func assertDeadLetterMetrics(t *testing.T, set *metrics.Set, wantPoison, wantDeadLetters uint64) {
	t.Helper()
	if got := set.GetOrCreateCounter(poisonMetricName).Get(); got != wantPoison {
		t.Errorf("poison counter = %d, want %d", got, wantPoison)
	}
	if got := set.GetOrCreateCounter(deadLettersMetricName).Get(); got != wantDeadLetters {
		t.Errorf("dead letters counter = %d, want %d", got, wantDeadLetters)
	}
}

func assertDeadLetterCount(t *testing.T, s *store.Store, want int) {
	t.Helper()
	var got int
	if err := s.Read().QueryRowContext(t.Context(), `SELECT count(*) FROM dead_letter`).Scan(&got); err != nil || got != want {
		t.Fatalf("dead letter count = %d, err = %v, want %d", got, err, want)
	}
}

func assertDeadLetterRow(t *testing.T, s *store.Store, id int64, now time.Time) {
	t.Helper()
	var kind, summary string
	var createdAt int64
	err := s.Read().QueryRowContext(t.Context(), `SELECT kind, error, created_at FROM dead_letter WHERE update_id = ?`, id).Scan(&kind, &summary, &createdAt)
	if err != nil || kind != "chat_join_request" || createdAt != now.UnixMilli() {
		t.Fatalf("dead letter %d kind = %q, created_at = %d, err = %v, want chat_join_request and %d", id, kind, createdAt, err, now.UnixMilli())
	}
	if !strings.Contains(summary, "invalid join request") || !utf8.ValidString(summary) || len(summary) < 510 || len(summary) > 512 || strings.Contains(summary, privateJoinBio) {
		t.Errorf("dead letter %d has unexpected summary of %d bytes: %q", id, len(summary), summary)
	}
}

func assertDeadLetterColumns(t *testing.T, s *store.Store) {
	t.Helper()
	want := []string{"created_at", "error", "kind", "update_id"}
	got, err := readDeadLetterColumn[string](t.Context(), s.Read(), `SELECT name FROM pragma_table_info('dead_letter') ORDER BY name`)
	if err != nil || !slices.Equal(got, want) {
		t.Fatalf("dead letter columns = %v, err = %v, want %v", got, err, want)
	}
}

func readDeadLetterColumn[T any](ctx context.Context, db *sql.DB, query string) (_ []T, err error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	var values []T
	for rows.Next() {
		var value T
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func execDeadLetterSQL(t *testing.T, s *store.Store, query string, args ...any) {
	t.Helper()
	if err := s.WriteWait(t.Context(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(t.Context(), query, args...)
		return err
	}); err != nil {
		t.Fatalf("execute dead letter fixture SQL: %v", err)
	}
}

type deadLetterTestHealth struct{}

func (deadLetterTestHealth) Register(string, func() error) {}

func openDeadLetterStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.Context(), store.Deps{
		Dir:                deadLetterTempDir(t),
		CacheKiB:           8000,
		WriteQueue:         8,
		CheckpointInterval: time.Hour,
		BackupKeep:         7,
		Metrics:            metrics.NewSet(),
		Health:             deadLetterTestHealth{},
		Fatal:              func(component string, err error) { t.Errorf("unexpected Fatal(%q, %v)", component, err) },
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		defer cancel()
		if err := s.Stop(ctx); err != nil {
			t.Errorf("stop store: %v", err)
		}
	})
	for attempt := range 2 {
		if err := s.Migrate(t.Context(), Migrations()); err != nil {
			t.Fatalf("migrate store on attempt %d: %v", attempt+1, err)
		}
	}
	var version int
	if err := s.Read().QueryRowContext(t.Context(), `SELECT version FROM schema_migrations WHERE component = 'ingress'`).Scan(&version); err != nil || version != 1 {
		t.Fatalf("ingress migration version = %d, err = %v, want 1", version, err)
	}
	if err := s.Start(t.Context()); err != nil {
		t.Fatalf("start store: %v", err)
	}
	return s
}

func deadLetterTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "yabot-ingress-")
	if err != nil {
		t.Fatalf("create store directory: %v", err)
	}
	t.Cleanup(func() {
		// Windows may still be removing the SQLite WAL files when Stop returns.
		deadline := time.Now().Add(2 * time.Second)
		for {
			err := os.RemoveAll(dir)
			if err == nil {
				return
			}
			if time.Now().After(deadline) {
				t.Errorf("remove store directory: %v", err)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	return dir
}
