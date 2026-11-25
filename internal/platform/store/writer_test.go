package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func TestWriterIsolatesOpFailures(t *testing.T) {
	businessErr := errors.New("invalid item")
	for _, test := range []struct {
		name string
		fail func() error
		want error
		text string
	}{
		{"business error", func() error { return businessErr }, businessErr, ""},
		{"requested rollback", func() error { return ErrRollbackOp }, ErrRollbackOp, ""},
		{"panic", func() error { panic("item panic") }, nil, "item panic"},
	} {
		t.Run(test.name, func(t *testing.T) {
			deps := newTestDeps(t)
			s := openBatchTestStore(t, deps)
			startBatchTestStore(t, s)
			gate, gateResults := blockBatchWriter(t, s)
			results := queueBatchTestOps(t, s,
				insertBatchItem(1),
				func(tx *sql.Tx) error {
					if err := insertBatchItem(2)(tx); err != nil {
						return err
					}
					return test.fail()
				},
				insertBatchItem(3),
			)
			gate.release()
			requireBatchSuccess(t, gateResults, 0)
			requireBatchSuccess(t, results, 0)
			err := nextBatchResult(t, results, 1)
			var opErr *OpError
			if !errors.As(err, &opErr) || (test.want != nil && !errors.Is(err, test.want)) ||
				(test.text != "" && !strings.Contains(err.Error(), test.text)) {
				t.Errorf("failed operation completion = %v", err)
			}
			requireBatchSuccess(t, results, 2)
			assertBatchIDs(t, s, 1, 3)
			assertBatchMetric(t, deps.Metrics, "yabot_store_batch_size_count", 2)
			assertBatchMetric(t, deps.Metrics, "yabot_store_batch_size_sum", 4)
			assertBatchMetric(t, deps.Metrics, "yabot_store_commit_seconds_count", 2)
			assertBatchAbortCount(t, deps.Metrics, 0)
		})
	}
}

func TestWriterCommitFailureAbortsBatch(t *testing.T) {
	for _, test := range []struct {
		name      string
		hook      bool
		wantCause string
	}{
		{"injected", true, "injected commit failure"},
		{"deferred foreign key", false, "foreign key constraint failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			deps := newTestDeps(t)
			s := openBatchTestStore(t, deps)
			if test.hook {
				attempts := 0
				s.hooks = &storeHooks{beforeCommit: func(*sql.Tx) error {
					attempts++
					if attempts == 2 {
						return errors.New("injected commit failure")
					}
					return nil
				}}
			} else {
				createBatchChildTable(t, s)
			}
			startBatchTestStore(t, s)
			gate, gateResults := blockBatchWriter(t, s)
			commitOp := insertBatchOrphan
			if test.hook {
				commitOp = insertBatchItem(2)
			}
			results := queueBatchTestOps(t, s, insertBatchItem(1), rejectBatchItem, commitOp, insertBatchItem(3))
			gate.release()
			requireBatchSuccess(t, gateResults, 0)
			assertBatchAborted(t, results, 4, test.wantCause)
			assertBatchIDs(t, s)
			assertBatchAbortCount(t, deps.Metrics, 1)
			if !test.hook {
				assertBatchMetric(t, deps.Metrics, "yabot_store_commit_seconds_count", 2)
			}
			if err := s.WriteWait(t.Context(), insertBatchItem(4)); err != nil {
				t.Fatalf("write after failed commit: %v", err)
			}
			assertBatchIDs(t, s, 4)
		})
	}
}

func TestWriterAbortsBatchAfterSQLiteFailure(t *testing.T) {
	for _, test := range []struct {
		name        string
		op          Op
		wantCause   string
		wantSQLCode int
	}{
		{"database full", fillBatchDatabase, "database or disk is full", sqlite3.SQLITE_FULL},
		{"transaction already rolled back", rollBackBatchTransaction, "transaction rolled back inside operation", 0},
		{"savepoint release fails after successful operation", func(tx *sql.Tx) error {
			_, err := tx.Exec("ROLLBACK")
			return err
		}, "release savepoint", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			deps := newTestDeps(t)
			s := openBatchTestStore(t, deps)
			startBatchTestStore(t, s)
			gate, gateResults := blockBatchWriter(t, s)
			cause := make(chan error, 1)
			results := queueBatchTestOps(t, s, insertBatchItem(1), rejectBatchItem, func(tx *sql.Tx) error {
				err := test.op(tx)
				cause <- err
				return err
			}, insertBatchItem(3))
			gate.release()
			requireBatchSuccess(t, gateResults, 0)
			assertBatchAborted(t, results, 4, test.wantCause)
			if err := receiveBatchValue(t, cause); test.wantSQLCode != 0 {
				var sqliteErr *sqlite.Error
				if !errors.As(err, &sqliteErr) || sqliteErr.Code()&0xff != test.wantSQLCode {
					t.Errorf("SQLite operation error = %v, want primary code %d", err, test.wantSQLCode)
				}
			}
			assertBatchIDs(t, s)
			assertBatchAbortCount(t, deps.Metrics, 1)
		})
	}
}

func TestWriterAbortsOnEngineErrorWithLiveSavepoint(t *testing.T) {
	source := openBatchTestStore(t, newTestDeps(t))
	constraint := batchConstraintError(t, source)
	full := batchFullError(t, source)

	deps := newTestDeps(t)
	s := openBatchTestStore(t, deps)
	startBatchTestStore(t, s)
	gate, gateResults := blockBatchWriter(t, s)
	results := queueBatchTestOps(t, s, insertBatchItem(1), func(tx *sql.Tx) error {
		if err := insertBatchItem(2)(tx); err != nil {
			return err
		}
		return errors.Join(constraint, &OpError{Err: full})
	}, insertBatchItem(3))
	gate.release()
	requireBatchSuccess(t, gateResults, 0)
	assertBatchAborted(t, results, 3, "database or disk is full")
	assertBatchIDs(t, s)
	assertBatchAbortCount(t, deps.Metrics, 1)
}

func TestIsEngineCode(t *testing.T) {
	for _, test := range []struct {
		name string
		code int
		want bool
	}{
		{"SQLITE_FULL", sqlite3.SQLITE_FULL, true},
		{"SQLITE_IOERR", sqlite3.SQLITE_IOERR, true},
		{"SQLITE_NOMEM", sqlite3.SQLITE_NOMEM, true},
		{"SQLITE_CORRUPT", sqlite3.SQLITE_CORRUPT, true},
		{"SQLITE_NOTADB", sqlite3.SQLITE_NOTADB, true},
		{"SQLITE_IOERR_READ", sqlite3.SQLITE_IOERR_READ, true},
		{"SQLITE_IOERR_WRITE", sqlite3.SQLITE_IOERR_WRITE, true},
		{"SQLITE_IOERR_FSYNC", sqlite3.SQLITE_IOERR_FSYNC, true},
		{"SQLITE_CORRUPT_VTAB", sqlite3.SQLITE_CORRUPT_VTAB, true},
		{"SQLITE_CONSTRAINT", sqlite3.SQLITE_CONSTRAINT, false},
		{"SQLITE_CONSTRAINT_UNIQUE", sqlite3.SQLITE_CONSTRAINT_UNIQUE, false},
		{"SQLITE_BUSY", sqlite3.SQLITE_BUSY, false},
		{"SQLITE_BUSY_SNAPSHOT", sqlite3.SQLITE_BUSY_SNAPSHOT, false},
		{"SQLITE_READONLY", sqlite3.SQLITE_READONLY, false},
		{"SQLITE_READONLY_CANTLOCK", sqlite3.SQLITE_READONLY_CANTLOCK, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := isEngineCode(test.code); got != test.want {
				t.Errorf("isEngineCode(%d) = %v, want %v", test.code, got, test.want)
			}
		})
	}
}

func TestIsEngineError(t *testing.T) {
	source := openBatchTestStore(t, newTestDeps(t))
	constraint := batchConstraintError(t, source)
	full := batchFullError(t, source)
	other := errors.New("unrelated error")
	var nilSQLiteError *sqlite.Error
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"typed nil", nilSQLiteError, false},
		{"engine code", full, true},
		{"wrapped engine code", fmt.Errorf("operation: %w", full), true},
		{"engine in later joined branch", errors.Join(constraint, full), true},
		{"engine in nested multi-wrap", fmt.Errorf("operation: %w; %w", constraint, errors.Join(other, full)), true},
		{"engine after typed nil", errors.Join(nilSQLiteError, full), true},
		{"constraint code", constraint, false},
		{"no engine in tree", errors.Join(other, fmt.Errorf("operation: %w", constraint)), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := isEngineError(test.err); got != test.want {
				t.Errorf("isEngineError = %v, want %v", got, test.want)
			}
		})
	}
}

func TestWriterEntersFaultState(t *testing.T) {
	for _, test := range []struct {
		name        string
		resetStreak bool
	}{
		{"three consecutive mixed failures", false},
		{"commit resets abort streak", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			type fatalReport struct {
				component string
				err       error
			}
			deps := newTestDeps(t)
			var logs bytes.Buffer
			deps.Log = slog.New(slog.NewJSONHandler(&logs, nil))
			health := &testHealthRegistry{}
			deps.Health = health
			fatals := make(chan fatalReport, 4)
			deps.Fatal = func(component string, err error) { fatals <- fatalReport{component, err} }
			s := openBatchTestStore(t, deps)
			if health.name != "store" || health.check == nil {
				t.Fatal("Open did not register the store health check")
			}
			if _, err := s.write.ExecContext(t.Context(), "PRAGMA busy_timeout=0"); err != nil {
				t.Fatalf("disable writer busy timeout: %v", err)
			}
			createBatchChildTable(t, s)
			startBatchTestStore(t, s)
			assertBatchBeginFailure(t, s)
			assertBatchAborted(t, queueBatchTestOps(t, s, insertBatchOrphan), 1, "foreign key constraint failed")
			if err := health.check(); err != nil {
				t.Fatalf("health after two failures = %v", err)
			}
			if test.resetStreak {
				if err := s.WriteWait(t.Context(), insertBatchItem(1)); err != nil {
					t.Fatalf("write to reset the failure streak: %v", err)
				}
			}
			gate := newBatchTestGate(t)
			third := queueBatchTestOps(t, s, func(tx *sql.Tx) error {
				gate.block()
				return rollBackBatchTransaction(tx)
			})
			receiveBatchValue(t, gate.entered)
			ran := make(chan struct{}, 2)
			pending := queueBatchTestOps(t, s,
				func(tx *sql.Tx) error { ran <- struct{}{}; return insertBatchItem(2)(tx) },
				func(tx *sql.Tx) error { ran <- struct{}{}; return insertBatchItem(3)(tx) },
			)
			gate.release()
			assertBatchAborted(t, third, 1, "transaction rolled back inside operation")
			rejected := make(chan error, 1)
			if test.resetStreak {
				requireBatchSuccess(t, pending, 0)
				requireBatchSuccess(t, pending, 1)
				if err := health.check(); err != nil {
					t.Errorf("health after failure streak reset = %v", err)
				}
				assertBatchIDs(t, s, 1, 2, 3)
			} else {
				for index := range 2 {
					err := nextBatchResult(t, pending, index)
					var opErr *OpError
					if !errors.Is(err, ErrFaulted) || errors.As(err, &opErr) {
						t.Errorf("queued operation %d completion = %v, want ErrFaulted", index, err)
					}
				}
				assertNoBatchValue(t, ran)
				if err := health.check(); err == nil {
					t.Error("faulted store reported healthy")
				}
				if err := s.Write(insertBatchItem(4), func(err error) { rejected <- err }); !errors.Is(err, ErrFaulted) {
					t.Errorf("subsequent Write = %v, want ErrFaulted", err)
				}
				assertBatchIDs(t, s)
			}
			if err := s.Stop(t.Context()); err != nil {
				t.Fatalf("stop store: %v", err)
			}
			if !test.resetStreak {
				assertNoBatchValue(t, rejected)
				select {
				case report := <-fatals:
					if report.component != "store" || !errors.Is(report.err, ErrFaulted) {
						t.Errorf("Fatal(%q, %v), want store and ErrFaulted", report.component, report.err)
					}
				default:
					t.Error("faulted store did not call Fatal")
				}
			}
			assertNoBatchValue(t, fatals)
			assertBatchAbortCount(t, deps.Metrics, 3)
			assertBatchFailureLogs(t, &logs, test.resetStreak)
		})
	}
}

func assertBatchBeginFailure(t *testing.T, s *Store) {
	t.Helper()
	ctx := t.Context()
	conn, err := s.checkpoint.Conn(ctx)
	if err != nil {
		t.Fatalf("take checkpoint connection: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close checkpoint connection: %v", err)
		}
	}()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("hold SQLite write lock: %v", err)
	}
	defer func() {
		if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
			t.Errorf("release SQLite write lock: %v", err)
		}
	}()
	results := queueBatchTestOps(t, s, func(*sql.Tx) error {
		t.Error("operation ran after BEGIN IMMEDIATE failed")
		return nil
	})
	assertBatchAborted(t, results, 1, "begin immediate")
}

func assertBatchFailureLogs(t *testing.T, logs io.Reader, resetStreak bool) {
	t.Helper()
	type batchLog struct {
		Level       string `json:"level"`
		Message     string `json:"msg"`
		Cause       string `json:"error"`
		Consecutive int    `json:"consecutive_failures"`
	}
	wantLogs := []batchLog{
		{"ERROR", "store batch failed", "database is locked", 1},
		{"ERROR", "store batch failed", "foreign key constraint failed", 2},
		{"ERROR", "store batch failed", "transaction rolled back inside operation", 3},
	}
	if resetStreak {
		wantLogs[2].Consecutive = 1
	} else {
		wantLogs = append(wantLogs, batchLog{"ERROR", "store entered fault state", ErrFaulted.Error(), 3})
	}
	decoder := json.NewDecoder(logs)
	for _, want := range wantLogs {
		var got batchLog
		if err := decoder.Decode(&got); err != nil {
			t.Fatalf("read log for %q: %v", want.Cause, err)
		}
		if got.Level != want.Level || got.Message != want.Message || got.Consecutive != want.Consecutive ||
			!strings.Contains(strings.ToLower(got.Cause), want.Cause) {
			t.Errorf("log = %+v, want %+v with matching cause text", got, want)
		}
	}
	var extra batchLog
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Errorf("unexpected extra log: %+v (decode error: %v)", extra, err)
	}
}

func TestWriterRepeatedBusinessFailureDoesNotFault(t *testing.T) {
	deps := newTestDeps(t)
	health := &testHealthRegistry{}
	deps.Health = health
	fatals := make(chan error, 1)
	deps.Fatal = func(_ string, err error) { fatals <- err }
	s := openBatchTestStore(t, deps)
	startBatchTestStore(t, s)
	businessErr := errors.New("persistent business error")
	for range 25 {
		err := s.WriteWait(t.Context(), func(tx *sql.Tx) error {
			if err := insertBatchItem(1)(tx); err != nil {
				return err
			}
			return businessErr
		})
		var opErr *OpError
		if !errors.As(err, &opErr) || !errors.Is(err, businessErr) {
			t.Fatalf("operation completion = %v, want OpError wrapping %v", err, businessErr)
		}
	}
	if err := health.check(); err != nil {
		t.Errorf("health after business errors = %v", err)
	}
	assertNoBatchValue(t, fatals)
	assertBatchIDs(t, s)
	assertBatchAbortCount(t, deps.Metrics, 0)
	assertBatchMetric(t, deps.Metrics, "yabot_store_batch_size_count", 25)
}

func TestWriterCallsDoneAfterCommitAndRecoversDonePanic(t *testing.T) {
	deps := newTestDeps(t)
	var logs bytes.Buffer
	deps.Log = slog.New(slog.NewTextHandler(&logs, nil))
	s := openBatchTestStore(t, deps)
	beforeCommit := newBatchTestGate(t)
	afterCommit := newBatchTestGate(t)
	commits := 0
	s.hooks = &storeHooks{
		beforeCommit: func(*sql.Tx) error {
			commits++
			if commits == 2 {
				beforeCommit.block()
			}
			return nil
		},
		afterCommit: func() {
			if commits == 2 {
				afterCommit.block()
			}
		},
	}
	startBatchTestStore(t, s)
	gate, gateResults := blockBatchWriter(t, s)
	results := make(chan batchTestResult, 4)
	for index := range 2 {
		if err := s.Write(insertBatchItem(index+1), func(err error) {
			results <- batchTestResult{index, err}
			if index == 0 {
				panic("done panic marker")
			}
		}); err != nil {
			t.Fatalf("queue operation %d: %v", index, err)
		}
	}
	gate.release()
	requireBatchSuccess(t, gateResults, 0)
	receiveBatchValue(t, beforeCommit.entered)
	assertNoBatchValue(t, results)
	assertBatchIDs(t, s)
	beforeCommit.release()
	receiveBatchValue(t, afterCommit.entered)
	assertNoBatchValue(t, results)
	assertBatchIDs(t, s, 1, 2)
	afterCommit.release()
	requireBatchSuccess(t, results, 0)
	requireBatchSuccess(t, results, 1)
	if err := s.WriteWait(t.Context(), insertBatchItem(3)); err != nil {
		t.Fatalf("write after callback panic: %v", err)
	}
	if err := s.Stop(t.Context()); err != nil {
		t.Fatalf("stop store: %v", err)
	}
	assertNoBatchValue(t, results)
	if log := logs.String(); !strings.Contains(log, "level=ERROR") || !strings.Contains(log, "done panic marker") {
		t.Errorf("expected an Error log carrying callback panic, got %q", log)
	}
}

func TestWriterDoesNotWaitForMoreRequests(t *testing.T) {
	deps := newTestDeps(t)
	synctest.Test(t, func(t *testing.T) {
		s := openBatchTestStore(t, deps)
		startBatchTestStore(t, s)
		synctest.Wait()

		results := queueBatchTestOps(t, s, insertBatchItem(1))
		synctest.Wait()
		select {
		case result := <-results:
			if result.err != nil {
				t.Fatalf("single write failed: %v", result.err)
			}
		default:
			t.Fatal("single write did not complete without advancing the virtual clock")
		}
	})
}

func TestWriterCapsDrainBatchAt128(t *testing.T) {
	deps := newTestDeps(t)
	deps.WriteQueue = 129
	s := openBatchTestStore(t, deps)
	rowsAtCommit := make(chan int, deps.WriteQueue+1)
	s.hooks = &storeHooks{beforeCommit: func(tx *sql.Tx) error {
		var rows int
		if err := tx.QueryRow("SELECT count(*) FROM items").Scan(&rows); err != nil {
			return err
		}
		rowsAtCommit <- rows
		return nil
	}}
	startBatchTestStore(t, s)
	gate, gateResults := blockBatchWriter(t, s)
	ops := make([]Op, 129)
	for index := range ops {
		ops[index] = insertBatchItem(index + 1)
	}
	results := queueBatchTestOps(t, s, ops...)
	gate.release()
	requireBatchSuccess(t, gateResults, 0)
	for index := range ops {
		requireBatchSuccess(t, results, index)
	}
	for _, want := range []int{0, 128, 129} {
		if got := receiveBatchValue(t, rowsAtCommit); got != want {
			t.Errorf("rows at commit = %d, want %d", got, want)
		}
	}
	assertNoBatchValue(t, rowsAtCommit)
}

const (
	commitCrashDirEnv = "YABOT_STORE_TEST_CRASH_DIR"
	commitCrashExit   = 73
)

func TestWriterCommitSurvivesCrashBeforeDone(t *testing.T) {
	if dir := os.Getenv(commitCrashDirEnv); dir != "" {
		deps := Deps{
			Dir:                dir,
			CacheKiB:           8000,
			WriteQueue:         16,
			CheckpointInterval: 500 * time.Millisecond,
			BackupKeep:         7,
			Metrics:            metrics.NewSet(),
			Health:             &testHealthRegistry{},
			Fatal:              func(_ string, err error) { t.Errorf("store faulted: %v", err) },
		}
		s := openBatchTestStore(t, deps)
		s.hooks = &storeHooks{
			afterCommit:     func() { os.Exit(commitCrashExit) },
			checkpointTicks: make(chan time.Time),
		}
		startBatchTestStore(t, s)
		results := queueBatchTestOps(t, s, insertBatchItem(42))
		t.Fatalf("done ran before the simulated crash: %v", nextBatchResult(t, results, 0))
	}

	deps := newTestDeps(t)
	crashStoreAfterCommit(t, deps.Dir)
	s := openTestStore(t, t.Context(), deps)
	assertBatchIDs(t, s, 42)
}

func crashStoreAfterCommit(t *testing.T, dir string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("find test executable: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, exe, "-test.run=^TestWriterCommitSurvivesCrashBeforeDone$", "-test.count=1")
	child.Env = append(child.Environ(), commitCrashDirEnv+"="+dir)
	output, err := child.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != commitCrashExit {
		t.Fatalf("crash test subprocess = %v, want exit code %d\n%s", err, commitCrashExit, output)
	}
}

func openBatchTestStore(t *testing.T, deps Deps) *Store {
	t.Helper()
	s := openTestStore(t, t.Context(), deps)
	if _, err := s.write.ExecContext(t.Context(), "CREATE TABLE items (id INTEGER PRIMARY KEY, payload BLOB)"); err != nil {
		t.Fatalf("create items table: %v", err)
	}
	return s
}

func startBatchTestStore(t *testing.T, s *Store) {
	t.Helper()
	if err := s.Start(t.Context()); err != nil {
		t.Fatalf("start store: %v", err)
	}
}

func insertBatchItem(id int) Op {
	return func(tx *sql.Tx) error {
		_, err := tx.Exec("INSERT INTO items(id) VALUES (?)", id)
		return err
	}
}

func rejectBatchItem(tx *sql.Tx) error {
	if err := insertBatchItem(5)(tx); err != nil {
		return err
	}
	return errors.New("item rejected by business rule")
}

func createBatchChildTable(t *testing.T, s *Store) {
	t.Helper()
	if _, err := s.write.ExecContext(t.Context(), `CREATE TABLE child (
		parent_id INTEGER REFERENCES items(id) DEFERRABLE INITIALLY DEFERRED
	)`); err != nil {
		t.Fatalf("create child table: %v", err)
	}
}

func insertBatchOrphan(tx *sql.Tx) error {
	_, err := tx.Exec("INSERT INTO child(parent_id) VALUES (99)")
	return err
}

func fillBatchDatabase(tx *sql.Tx) error {
	var pageCount int
	if err := tx.QueryRow("PRAGMA page_count").Scan(&pageCount); err != nil {
		return err
	}
	if _, err := tx.Exec("PRAGMA max_page_count = " + strconv.Itoa(pageCount)); err != nil {
		return err
	}
	_, err := tx.Exec("INSERT INTO items(id, payload) VALUES (2, zeroblob(1048576))")
	return err
}

func batchConstraintError(t *testing.T, s *Store) *sqlite.Error {
	t.Helper()
	_, err := s.write.ExecContext(t.Context(), "INSERT INTO items(id) VALUES (1), (1)")
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) || sqliteErr.Code() != sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY {
		t.Fatalf("duplicate item error = %v, want SQLITE_CONSTRAINT_PRIMARYKEY", err)
	}
	return sqliteErr
}

func batchFullError(t *testing.T, s *Store) *sqlite.Error {
	t.Helper()
	startBatchTestStore(t, s)
	observed := make(chan error, 1)
	if err := s.WriteWait(t.Context(), func(tx *sql.Tx) error {
		cause := fillBatchDatabase(tx)
		observed <- cause
		return cause
	}); !errors.Is(err, ErrAborted) {
		t.Fatalf("trigger SQLite full: %v", err)
	}
	cause := receiveBatchValue(t, observed)
	var sqliteErr *sqlite.Error
	if !errors.As(cause, &sqliteErr) || sqliteErr.Code()&0xff != sqlite3.SQLITE_FULL {
		t.Fatalf("source database error = %v, want SQLITE_FULL", cause)
	}
	return sqliteErr
}

func rollBackBatchTransaction(tx *sql.Tx) error {
	if _, err := tx.Exec("ROLLBACK"); err != nil {
		return err
	}
	return errors.New("transaction rolled back inside operation")
}

type batchTestGate struct {
	entered chan struct{}
	resume  chan struct{}
	release func()
}

func newBatchTestGate(t *testing.T) batchTestGate {
	t.Helper()
	resume := make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	t.Cleanup(release)
	return batchTestGate{make(chan struct{}), resume, release}
}

func (g batchTestGate) block() {
	close(g.entered)
	<-g.resume
}

func blockBatchWriter(t *testing.T, s *Store) (batchTestGate, <-chan batchTestResult) {
	t.Helper()
	gate := newBatchTestGate(t)
	results := queueBatchTestOps(t, s, func(*sql.Tx) error {
		gate.block()
		return nil
	})
	receiveBatchValue(t, gate.entered)
	return gate, results
}

type batchTestResult struct {
	index int
	err   error
}

func queueBatchTestOps(t *testing.T, s *Store, ops ...Op) <-chan batchTestResult {
	t.Helper()
	results := make(chan batchTestResult, len(ops))
	for index, op := range ops {
		if err := s.Write(op, func(err error) {
			select {
			case results <- batchTestResult{index, err}:
			default:
				t.Errorf("unexpected callback for operation %d", index)
			}
		}); err != nil {
			t.Fatalf("queue operation %d: %v", index, err)
		}
	}
	return results
}

func nextBatchResult(t *testing.T, results <-chan batchTestResult, index int) error {
	t.Helper()
	result := receiveBatchValue(t, results)
	if result.index != index {
		t.Errorf("operation %d completed out of order, want operation %d", result.index, index)
	}
	return result.err
}

func requireBatchSuccess(t *testing.T, results <-chan batchTestResult, index int) {
	t.Helper()
	if err := nextBatchResult(t, results, index); err != nil {
		t.Fatalf("operation %d failed: %v", index, err)
	}
}

func assertBatchAborted(t *testing.T, results <-chan batchTestResult, size int, reason string) {
	t.Helper()
	var batchErr error
	for index := range size {
		err := nextBatchResult(t, results, index)
		var opErr *OpError
		if !errors.Is(err, ErrAborted) || errors.As(err, &opErr) || !strings.Contains(strings.ToLower(err.Error()), reason) {
			t.Errorf("operation %d completion = %v, want batch abort identifying %q", index, err, reason)
		}
		if index == 0 {
			batchErr = err
		} else if err != batchErr {
			t.Errorf("operation %d received %v, want the same error as operation 0 (%v)", index, err, batchErr)
		}
	}
}

func assertBatchIDs(t *testing.T, s *Store, want ...int) {
	t.Helper()
	rows, err := s.Read().QueryContext(t.Context(), "SELECT id FROM items ORDER BY id")
	if err != nil {
		t.Fatalf("read item IDs: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close item rows: %v", err)
		}
	}()
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan item ID: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate item IDs: %v", err)
	}
	if !slices.Equal(ids, want) {
		t.Errorf("committed item IDs = %v, want %v", ids, want)
	}
}

func assertBatchMetric(t *testing.T, set *metrics.Set, name string, want int) {
	t.Helper()
	var exposition bytes.Buffer
	set.WritePrometheus(&exposition)
	line := fmt.Sprintf("%s %d", name, want)
	if !slices.Contains(strings.Split(exposition.String(), "\n"), line) {
		t.Errorf("metrics missing %q:\n%s", line, &exposition)
	}
}

func assertBatchAbortCount(t *testing.T, set *metrics.Set, want int) {
	t.Helper()
	assertBatchMetric(t, set, "yabot_store_batch_aborts_total", want)
}

func receiveBatchValue[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for writer")
		var zero T
		return zero
	}
}

func assertNoBatchValue[T any](t *testing.T, values <-chan T) {
	t.Helper()
	select {
	case value := <-values:
		t.Errorf("unexpected writer notification: %v", value)
	default:
	}
}
