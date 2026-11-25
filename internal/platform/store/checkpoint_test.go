package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func TestCheckpointReclaimsIdleWAL(t *testing.T) {
	deps := newTestDeps(t)
	s := openBatchTestStore(t, deps)
	checkpointCalls := 0
	ticks := startCheckpointTest(t, s, func(context.Context, string) error {
		checkpointCalls++
		return nil
	})
	for _, step := range []struct {
		name  string
		mode  string
		calls int
		write bool
	}{
		{"before first commit", checkpointTruncate, 1, false},
		{"after commit", checkpointPassive, 2, true},
		{"idle with WAL pages", checkpointTruncate, 3, false},
		{"idle with empty WAL", "", 3, false},
	} {
		t.Run(step.name, func(t *testing.T) {
			if step.write {
				writeCheckpointItem(t, s, 1)
			}
			result := ticks.run(t, step.mode)
			if checkpointCalls != step.calls {
				t.Errorf("checkpoint statements so far = %d, want %d", checkpointCalls, step.calls)
			}
			switch step.mode {
			case checkpointPassive:
				if result.busy != 0 || result.logPages <= 0 || result.checkpointed != result.logPages {
					t.Errorf("PASSIVE result = %+v, want a nonempty WAL fully backfilled", result)
				}
				assertBatchMetric(t, deps.Metrics, "yabot_store_wal_pages", result.logPages)
			case checkpointTruncate:
				if result != (checkpointResult{}) {
					t.Errorf("TRUNCATE result = %+v, want all zeroes", result)
				}
				if got := checkpointWALSize(t, deps.Dir); got != 0 {
					t.Errorf("WAL size after TRUNCATE = %d bytes, want 0", got)
				}
				assertBatchMetric(t, deps.Metrics, "yabot_store_wal_pages", 0)
			}
		})
	}
}

func TestCheckpointReclaimsWALAfterFailedCommit(t *testing.T) {
	deps := newTestDeps(t)
	// Constrain the page cache so the transaction spills to the WAL before COMMIT.
	deps.CacheKiB = 64
	s := openBatchTestStore(t, deps)
	createBatchChildTable(t, s)
	ticks := startCheckpointTest(t, s, nil)
	ticks.run(t, checkpointTruncate)
	if size := checkpointWALSize(t, deps.Dir); size != 0 {
		t.Fatalf("WAL size before the failed batch = %d bytes, want 0", size)
	}
	ticks.run(t, "")

	ctx, cancel := context.WithTimeout(t.Context(), lifecycleWait)
	defer cancel()
	const payloadBytes = 512 * 1024
	err := s.WriteWait(ctx, func(tx *sql.Tx) error {
		if _, err := tx.Exec("INSERT INTO items(id, payload) VALUES (1, zeroblob(?))", payloadBytes); err != nil {
			return err
		}
		return insertBatchOrphan(tx)
	})
	if !errors.Is(err, ErrAborted) || !strings.Contains(err.Error(), "during commit") || !strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
		t.Fatalf("WriteWait = %v, want a deferred foreign key violation at COMMIT", err)
	}
	assertBatchIDs(t, s)
	if size := checkpointWALSize(t, deps.Dir); size == 0 {
		t.Fatal("WAL size after the failed batch = 0 bytes, want residual WAL data")
	}

	if result := ticks.run(t, checkpointPassive); result.logPages != 0 {
		t.Errorf("PASSIVE after rollback reported %d WAL pages, want 0 committed pages", result.logPages)
	}
	ticks.run(t, checkpointTruncate)
	if size := checkpointWALSize(t, deps.Dir); size != 0 {
		t.Errorf("WAL size after idle TRUNCATE = %d bytes, want 0", size)
	}
	ticks.run(t, "")
}

func TestCheckpointTickerReclaimsIdleWAL(t *testing.T) {
	deps := newTestDeps(t)
	deps.CheckpointInterval = 10 * time.Millisecond
	s := openBatchTestStore(t, deps)
	startBatchTestStore(t, s)
	writeCheckpointItem(t, s, 1)
	// The old snapshot keeps the WAL from being truncated before the page count is observed.
	_, release := pinCheckpointSnapshot(t, s)
	writeCheckpointItem(t, s, 2)
	writeCheckpointItem(t, s, 3)

	walPages := deps.Metrics.GetOrCreateGauge("yabot_store_wal_pages", nil)
	waitForWAL := func(want string, ready func(pages float64, size int64) bool) {
		t.Helper()
		poll := time.NewTicker(10 * time.Millisecond)
		defer poll.Stop()
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		for {
			pages, size := walPages.Get(), checkpointWALSize(t, deps.Dir)
			if ready(pages, size) {
				return
			}
			select {
			case <-poll.C:
			case <-deadline.C:
				t.Fatalf("timed out waiting for %s: WAL page gauge = %g, file size = %d bytes", want, pages, size)
			}
		}
	}
	waitForWAL("a sampled nonempty WAL", func(pages float64, size int64) bool {
		return pages > 0 && size > 0
	})
	release()
	waitForWAL("an empty WAL", func(pages float64, size int64) bool {
		return pages == 0 && size == 0
	})
}

func TestCheckpointReclaimsCrashWALWithoutNewWrites(t *testing.T) {
	deps := newTestDeps(t)
	crashStoreAfterCommit(t, deps.Dir)
	s := openTestStore(t, t.Context(), deps)
	if size := checkpointWALSize(t, deps.Dir); size <= 0 {
		t.Fatalf("reopened WAL size = %d bytes, want a nonempty WAL", size)
	}
	ticks := startCheckpointTest(t, s, nil)
	ticks.run(t, checkpointTruncate)
	if size := checkpointWALSize(t, deps.Dir); size != 0 {
		t.Errorf("WAL size after idle tick = %d bytes, want 0", size)
	}
	assertBatchIDs(t, s, 42)
}

func TestCheckpointRetriesIdleAfterUnsampledWAL(t *testing.T) {
	s := openBatchTestStore(t, newTestDeps(t))
	noWAL, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open database without WAL: %v", err)
	}
	t.Cleanup(func() {
		if err := noWAL.Close(); err != nil {
			t.Errorf("close database without WAL: %v", err)
		}
	})
	checkpoint := s.checkpoint
	s.checkpoint = noWAL
	var state checkpointState
	mode, result, err := s.checkpointTick(t.Context(), &state)
	s.checkpoint = checkpoint
	if mode != checkpointTruncate || err != nil || result.logPages != -1 {
		t.Fatalf("idle tick without WAL = %q, %+v, %v, want TRUNCATE with unavailable WAL pages", mode, result, err)
	}
	if mode, _, err := s.checkpointTick(t.Context(), &state); mode != checkpointTruncate || err != nil {
		t.Errorf("idle tick after unavailable WAL sample = %q, %v, want successful TRUNCATE", mode, err)
	}
}

func checkpointWALSize(t *testing.T, dir string) int64 {
	t.Helper()
	stat, err := os.Stat(filepath.Join(dir, "yabot.db-wal"))
	if err != nil {
		t.Fatalf("stat WAL: %v", err)
	}
	return stat.Size()
}

func TestCheckpointBusyTruncateDoesNotBlockWrites(t *testing.T) {
	const operationLimit = 2 * time.Second
	deps := newTestDeps(t)
	var logs bytes.Buffer
	deps.Log = slog.New(slog.NewJSONHandler(&logs, nil))
	s := openBatchTestStore(t, deps)
	startWrite := make(chan struct{})
	truncates := 0
	ticks := startCheckpointTest(t, s, func(_ context.Context, mode string) error {
		if mode == checkpointTruncate {
			truncates++
			if truncates == 2 {
				close(startWrite)
			}
		}
		return nil
	})
	writeCheckpointItem(t, s, 1)
	reader, release := pinCheckpointSnapshot(t, s)
	writeCheckpointItem(t, s, 2)
	result := ticks.run(t, checkpointPassive)
	if result.logPages <= result.checkpointed {
		t.Fatalf("PASSIVE result = %+v, want frames beyond the reader's snapshot", result)
	}

	startedAt := time.Now()
	if result := ticks.run(t, checkpointTruncate); result.busy != 1 {
		t.Fatalf("TRUNCATE with pinned snapshot = %+v, want busy=1", result)
	}
	truncateElapsed := time.Since(startedAt)
	if truncateElapsed >= operationLimit {
		t.Errorf("TRUNCATE with pinned snapshot took %s, want under %s", truncateElapsed, operationLimit)
	}
	assertBatchMetric(t, deps.Metrics, "yabot_store_wal_pages", result.logPages)

	type writeOutcome struct {
		elapsed time.Duration
		err     error
	}
	writeResult := make(chan writeOutcome, 1)
	go func() {
		select {
		case <-startWrite:
		case <-t.Context().Done():
			return
		}
		ctx, cancel := context.WithTimeout(t.Context(), operationLimit)
		defer cancel()
		startedAt := time.Now()
		err := s.WriteWait(ctx, insertBatchItem(3))
		writeResult <- writeOutcome{time.Since(startedAt), err}
	}()
	if result := ticks.run(t, checkpointTruncate); result.busy != 1 {
		t.Errorf("next idle tick TRUNCATE = %+v, want busy=1", result)
	}
	write := awaitLifecycleResult(t, writeResult)
	t.Logf("busy TRUNCATE took %s, concurrent WriteWait took %s", truncateElapsed, write.elapsed)
	if write.err != nil || write.elapsed >= operationLimit {
		t.Errorf("WriteWait during TRUNCATE finished in %s with %v, want success under %s", write.elapsed, write.err, operationLimit)
	}
	assertCheckpointSnapshot(t, reader)
	release()
	if write.err == nil {
		assertBatchIDs(t, s, 1, 2, 3)
	}
	if err := s.Stop(t.Context()); err != nil {
		t.Fatalf("stop store: %v", err)
	}
	assertBatchMetric(t, deps.Metrics, "yabot_store_checkpoint_errors_total", 0)
	if records := readCheckpointLogs(t, &logs); len(records) != 0 {
		t.Errorf("expected busy checkpoints without logs, got %+v", records)
	}
}

func TestObserveWALGrowth(t *testing.T) {
	type observation struct {
		pages       int
		err         error
		wantGrowing bool
	}
	const firstPages = 100
	firstAlertPages := firstPages + walGrowthThreshold
	beforeAlert := make([]observation, walGrowthThreshold)
	for i := range beforeAlert {
		beforeAlert[i] = observation{pages: firstPages + i}
	}
	withPrefix := func(tail ...observation) []observation {
		return slices.Concat(beforeAlert, tail)
	}
	alert := observation{pages: firstAlertPages, wantGrowing: true}
	flat := observation{pages: firstAlertPages - 1}
	alternating := []observation{{pages: firstPages}}
	for i := 1; i <= 2*walGrowthThreshold; i++ {
		sample := observation{pages: firstPages + i}
		alternating = append(alternating, sample, sample)
	}
	checkpointErr := errors.New("checkpoint failed")

	for _, test := range []struct {
		name         string
		observations []observation
	}{
		{"first observation establishes baseline", beforeAlert[:1]},
		{"reaches threshold", withPrefix(alert)},
		{"flat before threshold resets count", withPrefix(flat, observation{pages: firstAlertPages})},
		{"alternating increases and plateaus", alternating},
		{"flat ends alert", withPrefix(alert, observation{pages: firstAlertPages}, observation{pages: firstAlertPages + 1})},
		{"decrease ends alert", withPrefix(alert, observation{pages: firstPages}, observation{pages: firstPages + 1})},
		{"continued growth keeps alert active", withPrefix(alert,
			observation{pages: firstAlertPages + 1, wantGrowing: true},
			observation{pages: firstAlertPages + 2, wantGrowing: true})},
		{"unavailable pages preserve streak and alert", withPrefix(observation{pages: -1}, alert,
			observation{pages: -1, wantGrowing: true}, observation{pages: firstAlertPages + 1, wantGrowing: true})},
		{"checkpoint errors preserve streak and alert", withPrefix(observation{pages: firstAlertPages + 1, err: checkpointErr}, alert,
			observation{err: checkpointErr, wantGrowing: true}, observation{pages: firstAlertPages + 1, wantGrowing: true})},
	} {
		t.Run(test.name, func(t *testing.T) {
			var state walGrowthState
			for i, sample := range test.observations {
				state, _ = observeWALGrowth(state, sample.pages, sample.err)
				if got := state.isGrowing(); got != sample.wantGrowing {
					t.Fatalf("observation %d (%d pages, error %v): alert = %t, want %t", i+1, sample.pages, sample.err, got, sample.wantGrowing)
				}
			}
		})
	}
}

func TestCheckpointReportsSustainedWALGrowth(t *testing.T) {
	for _, test := range []struct {
		name         string
		keepSnapshot bool
	}{
		{"WAL pages decrease", false},
		{"WAL pages stay flat with old reader", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			testCheckpointWALGrowth(t, test.keepSnapshot)
		})
	}
}

func TestCheckpointClearsStaleGrowthGaugeOnReopen(t *testing.T) {
	const gaugeName = "yabot_store_wal_growing"
	deps := newTestDeps(t)
	var logs bytes.Buffer
	deps.Log = slog.New(slog.NewJSONHandler(&logs, nil))
	previous := openTestStore(t, t.Context(), deps)
	deps.Metrics.GetOrCreateGauge(gaugeName, nil).Set(1)
	if err := previous.Stop(t.Context()); err != nil {
		t.Fatalf("stop previous store: %v", err)
	}

	s := openTestStore(t, t.Context(), deps)
	ticks := startCheckpointTest(t, s, nil)
	ticks.run(t, checkpointTruncate)
	assertBatchMetric(t, deps.Metrics, gaugeName, 0)
	if records := readCheckpointLogs(t, &logs); len(records) != 0 {
		t.Errorf("expected no WAL growth transition logs after reopen, got %+v", records)
	}
}

func testCheckpointWALGrowth(t *testing.T, keepSnapshot bool) {
	t.Helper()
	deps := newTestDeps(t)
	var logs bytes.Buffer
	deps.Log = slog.New(slog.NewJSONHandler(&logs, nil))
	s := openBatchTestStore(t, deps)
	ticks := startCheckpointTest(t, s, nil)
	writeCheckpointItem(t, s, 1)
	reader, release := pinCheckpointSnapshot(t, s)
	previous := ticks.run(t, checkpointPassive).logPages
	var warningPages int
	for i := 1; i <= walGrowthThreshold+2; i++ {
		writeCheckpointItem(t, s, i+1)
		result := ticks.run(t, checkpointPassive)
		if result.logPages <= previous {
			t.Fatalf("WAL pages after write %d = %d, want more than %d", i, result.logPages, previous)
		}
		previous = result.logPages
		if i == walGrowthThreshold {
			warningPages = result.logPages
		}
		wantGrowing := 0
		if i >= walGrowthThreshold {
			wantGrowing = 1
		}
		assertBatchMetric(t, deps.Metrics, "yabot_store_wal_growing", wantGrowing)
	}
	assertCheckpointSnapshot(t, reader)
	wantBusy, wantPages := 0, 0
	if keepSnapshot {
		wantBusy, wantPages = 1, previous
	} else {
		release()
	}
	result := ticks.run(t, checkpointTruncate)
	if result.busy != wantBusy || result.logPages != wantPages {
		t.Fatalf("recovery TRUNCATE = %+v, want busy=%d and logPages=%d", result, wantBusy, wantPages)
	}
	assertBatchMetric(t, deps.Metrics, "yabot_store_wal_growing", 0)
	if keepSnapshot {
		assertCheckpointSnapshot(t, reader)
		release()
		ticks.run(t, checkpointTruncate)
	}
	ticks.run(t, "")
	if err := s.Stop(t.Context()); err != nil {
		t.Fatalf("stop store: %v", err)
	}
	records := readCheckpointLogs(t, &logs)
	if len(records) != 2 || records[0].Level != "WARN" || records[1].Level != "INFO" || records[1].Message != "store WAL stopped growing" {
		t.Fatalf("expected one WAL growth warning and one recovery info log, got %+v", records)
	}
	if got := records[0].WALPages; got == nil || *got != warningPages {
		t.Errorf("expected WAL growth warning with wal_pages=%d, got %+v", warningPages, records[0])
	}
	if got := records[1].WALPages; got == nil || *got != wantPages {
		t.Errorf("expected WAL recovery info with wal_pages=%d, got %+v", wantPages, records[1])
	}
}

func TestCheckpointReportsStatementErrorsAndRetriesIdle(t *testing.T) {
	deps := newTestDeps(t)
	var logs bytes.Buffer
	deps.Log = slog.New(slog.NewJSONHandler(&logs, nil))
	s := openBatchTestStore(t, deps)
	ticks := startCheckpointTest(t, s, nil)
	ticks.run(t, checkpointTruncate)

	// The single-connection pool keeps the read transaction on the checkpoint connection.
	if _, err := s.checkpoint.ExecContext(t.Context(), "BEGIN"); err != nil {
		t.Fatalf("begin read transaction on checkpoint connection: %v", err)
	}
	release := sync.OnceFunc(func() {
		if _, err := s.checkpoint.ExecContext(t.Context(), "ROLLBACK"); err != nil {
			t.Errorf("release read transaction on checkpoint connection: %v", err)
		}
	})
	defer release()
	var count int
	if err := s.checkpoint.QueryRowContext(t.Context(), "SELECT count(*) FROM items").Scan(&count); err != nil {
		t.Fatalf("pin read transaction on checkpoint connection: %v", err)
	}

	for i := 1; i <= 3; i++ {
		writeCheckpointItem(t, s, i)
		ticks.send(t)
		got := awaitLifecycleResult(t, ticks.results)
		var sqliteErr *sqlite.Error
		if got.mode != checkpointPassive || !errors.As(got.err, &sqliteErr) || sqliteErr.Code() != sqlite3.SQLITE_LOCKED {
			t.Errorf("checkpoint failed attempt = %+v, want PASSIVE with SQLITE_LOCKED", got)
		}
		assertBatchMetric(t, deps.Metrics, "yabot_store_checkpoint_errors_total", i)
		records := readCheckpointLogs(t, &logs)
		if len(records) != 1 || records[0].Level != "WARN" || !strings.Contains(records[0].Error, "database table is locked") {
			t.Errorf("expected one checkpoint warning carrying SQLITE_LOCKED, got %+v", records)
		}
		if err := deps.Health.(*testHealthRegistry).check(); err != nil {
			t.Errorf("store health after checkpoint failure = %v", err)
		}
	}
	release()
	if size := checkpointWALSize(t, deps.Dir); size <= 0 {
		t.Fatalf("WAL size after PASSIVE errors = %d bytes, want a nonempty WAL", size)
	}
	ticks.run(t, checkpointTruncate)
	if size := checkpointWALSize(t, deps.Dir); size != 0 {
		t.Errorf("WAL size after idle retry = %d bytes, want 0", size)
	}
	writeCheckpointItem(t, s, 4)
	ticks.run(t, checkpointPassive)
	assertBatchIDs(t, s, 1, 2, 3, 4)
	if err := s.Stop(t.Context()); err != nil {
		t.Fatalf("stop store: %v", err)
	}
	assertBatchMetric(t, deps.Metrics, "yabot_store_checkpoint_errors_total", 3)
	if records := readCheckpointLogs(t, &logs); len(records) != 0 {
		t.Errorf("expected successful checkpoints without logs, got %+v", records)
	}
}

type checkpointTestObservation struct {
	mode   string
	result checkpointResult
	err    error
}

type checkpointTestTicks struct {
	ticks   chan time.Time
	results chan checkpointTestObservation
}

func startCheckpointTest(t *testing.T, s *Store, before func(context.Context, string) error) checkpointTestTicks {
	t.Helper()
	ticks := checkpointTestTicks{make(chan time.Time), make(chan checkpointTestObservation, 1)}
	s.hooks = &storeHooks{
		checkpointTicks:  ticks.ticks,
		beforeCheckpoint: before,
		afterCheckpointTick: func(mode string, result checkpointResult, err error) {
			ticks.results <- checkpointTestObservation{mode, result, err}
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("start store: %v", err)
	}
	return ticks
}

func (ticks checkpointTestTicks) send(t *testing.T) {
	t.Helper()
	select {
	case ticks.ticks <- time.Now():
	case <-time.After(lifecycleWait):
		t.Fatal("timed out sending checkpoint tick")
	}
}

func (ticks checkpointTestTicks) run(t *testing.T, mode string) checkpointResult {
	t.Helper()
	ticks.send(t)
	got := awaitLifecycleResult(t, ticks.results)
	if got.mode != mode || got.err != nil {
		t.Fatalf("checkpoint tick = %+v, want mode %q and no error", got, mode)
	}
	return got.result
}

func writeCheckpointItem(t *testing.T, s *Store, id int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), lifecycleWait)
	defer cancel()
	if err := s.WriteWait(ctx, insertBatchItem(id)); err != nil {
		t.Fatalf("write item %d: %v", id, err)
	}
}

func pinCheckpointSnapshot(t *testing.T, s *Store) (*sql.Tx, func()) {
	t.Helper()
	reader, err := s.Read().BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("begin read transaction: %v", err)
	}
	release := sync.OnceFunc(func() {
		if err := reader.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("release reader snapshot: %v", err)
		}
	})
	t.Cleanup(release)
	assertCheckpointSnapshot(t, reader)
	return reader, release
}

func assertCheckpointSnapshot(t *testing.T, reader *sql.Tx) {
	t.Helper()
	var count int
	if err := reader.QueryRowContext(t.Context(), "SELECT count(*) FROM items").Scan(&count); err != nil {
		t.Fatalf("read pinned snapshot: %v", err)
	}
	if count != 1 {
		t.Errorf("pinned snapshot has %d rows, want 1", count)
	}
}

type checkpointTestLog struct {
	Level    string `json:"level"`
	Message  string `json:"msg"`
	Error    string `json:"error"`
	WALPages *int   `json:"wal_pages"`
}

func readCheckpointLogs(t *testing.T, logs io.Reader) []checkpointTestLog {
	t.Helper()
	var entries []checkpointTestLog
	decoder := json.NewDecoder(logs)
	for {
		var entry checkpointTestLog
		err := decoder.Decode(&entry)
		if err == io.EOF {
			return entries
		}
		if err != nil {
			t.Fatalf("decode checkpoint log: %v", err)
		}
		entries = append(entries, entry)
	}
}
