package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func TestOpenConnectionPragmas(t *testing.T) {
	const cacheKiB = 3067
	deps := newTestDeps(t)
	deps.CacheKiB = cacheKiB
	s := openTestStore(t, t.Context(), deps)
	for _, pool := range []struct {
		name string
		db   *sql.DB
		want int
	}{
		{"writer", s.write, 1},
		{"reader", s.Read(), 2},
		{"checkpoint", s.checkpoint, 1},
	} {
		if got := pool.db.Stats().MaxOpenConnections; got != pool.want {
			t.Fatalf("%s pool allows %d connections, want %d", pool.name, got, pool.want)
		}
	}
	connections := []struct {
		name      string
		conn      *sql.Conn
		queryOnly string
	}{
		{"writer", takeTestConn(t, s.write), "0"},
		{"reader 1", takeTestConn(t, s.Read()), "1"},
		{"reader 2", takeTestConn(t, s.Read()), "1"},
		{"checkpoint", takeTestConn(t, s.checkpoint), "0"},
	}
	for _, connection := range connections {
		t.Run(connection.name, func(t *testing.T) {
			pragmas := []struct {
				name string
				want string
			}{
				{"journal_mode", "wal"},
				{"synchronous", "1"},
				{"busy_timeout", "5000"},
				{"foreign_keys", "1"},
				{"wal_autocheckpoint", "0"},
				{"journal_size_limit", "67108864"},
				{"cache_size", strconv.Itoa(-cacheKiB)},
				{"query_only", connection.queryOnly},
			}
			for _, pragma := range pragmas {
				var got string
				if err := connection.conn.QueryRowContext(t.Context(), "PRAGMA "+pragma.name).Scan(&got); err != nil {
					t.Errorf("read PRAGMA %s: %v", pragma.name, err)
					continue
				}
				if got != pragma.want {
					t.Errorf("PRAGMA %s = %q, want %q", pragma.name, got, pragma.want)
				}
			}
		})
	}
}

func TestReadPoolRejectsWrites(t *testing.T) {
	ctx := t.Context()
	s := openTestStore(t, ctx, newTestDeps(t))
	if _, err := s.write.ExecContext(ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create table through write connection: %v", err)
	}
	if _, err := s.write.ExecContext(ctx, "INSERT INTO items (id) VALUES (?)", 42); err != nil {
		t.Fatalf("insert through write connection: %v", err)
	}
	var got int
	if err := s.Read().QueryRowContext(ctx, "SELECT id FROM items").Scan(&got); err != nil {
		t.Fatalf("query through read pool: %v", err)
	}
	if got != 42 {
		t.Fatalf("item ID = %d, want 42", got)
	}
	for _, statement := range []string{
		"INSERT INTO items (id) VALUES (43)",
		"CREATE TABLE other_items (id INTEGER PRIMARY KEY)",
	} {
		if _, err := s.Read().ExecContext(ctx, statement); err == nil {
			t.Errorf("read pool accepted %q", statement)
		}
	}

	t.Run("query_only disabled", func(t *testing.T) {
		conn := takeTestConn(t, s.Read())
		if _, err := conn.ExecContext(t.Context(), "PRAGMA query_only=OFF"); err != nil {
			t.Fatalf("disable query_only: %v", err)
		}
		_, err := conn.ExecContext(t.Context(), "INSERT INTO items (id) VALUES (44)")
		var sqliteErr *sqlite.Error
		if !errors.As(err, &sqliteErr) || sqliteErr.Code()&0xff != sqlite3.SQLITE_READONLY {
			t.Errorf("write with query_only disabled = %v, want SQLITE_READONLY", err)
		}
	})
}

func TestWriterBeginsImmediateTransaction(t *testing.T) {
	ctx := t.Context()
	s := openTestStore(t, ctx, newTestDeps(t))
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin writer transaction: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			t.Errorf("roll back writer transaction: %v", err)
		}
	}()

	conn := takeTestConn(t, s.checkpoint)
	if _, err := conn.ExecContext(ctx, "PRAGMA busy_timeout=0"); err != nil {
		t.Fatalf("disable checkpoint connection busy timeout: %v", err)
	}
	_, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE")
	if err == nil {
		if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
			t.Errorf("roll back checkpoint connection transaction: %v", err)
		}
		t.Fatal("checkpoint connection acquired the write lock while the writer transaction was open")
	}
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) || sqliteErr.Code()&0xff != sqlite3.SQLITE_BUSY {
		t.Errorf("begin immediate on checkpoint connection = %v, want SQLITE_BUSY", err)
	}
}

func TestWriterAbortsBatchWhenBeginFails(t *testing.T) {
	ctx := t.Context()
	deps := newTestDeps(t)
	s := openTestStore(t, ctx, deps)
	if _, err := s.write.ExecContext(ctx, "PRAGMA busy_timeout=0"); err != nil {
		t.Fatalf("disable writer connection busy timeout: %v", err)
	}
	conn := takeTestConn(t, s.checkpoint)
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("hold SQLite write lock: %v", err)
	}
	if err := s.Start(ctx); err != nil {
		t.Fatalf("start store: %v", err)
	}
	var ran bool
	op := func(*sql.Tx) error {
		ran = true
		return nil
	}
	err := s.WriteWait(ctx, op)
	var opErr *OpError
	if !errors.Is(err, ErrAborted) || errors.As(err, &opErr) || !strings.Contains(err.Error(), "begin immediate") {
		t.Errorf("WriteWait = %v, want a batch error from BEGIN IMMEDIATE", err)
	}
	if ran {
		t.Error("operation ran after BEGIN IMMEDIATE failed")
	}
	if got := deps.Metrics.GetOrCreateCounter("yabot_store_batch_aborts_total").Get(); got != 1 {
		t.Errorf("batch aborts = %d, want 1", got)
	}
	if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("release SQLite write lock: %v", err)
	}
	if err := s.WriteWait(ctx, op); err != nil || !ran {
		t.Errorf("next WriteWait = %v, operation ran = %v, want success", err, ran)
	}
}

func TestOpenLocksDirectoryUntilStop(t *testing.T) {
	ctx := t.Context()
	deps := newTestDeps(t)
	s := openTestStore(t, ctx, deps)

	lockPath := filepath.Join(deps.Dir, "yabot.lock")
	err := failTestOpen(t, ctx, deps)
	if !errors.Is(err, lockConflict) {
		t.Fatalf("second Open error = %v, want a lock conflict", err)
	}
	message := err.Error()
	if !strings.Contains(message, "data directory is locked by another process") ||
		(!strings.Contains(message, lockPath) && !strings.Contains(message, strconv.Quote(lockPath))) {
		t.Errorf("lock conflict error = %q, want a message identifying the locked path %q", message, lockPath)
	}

	if err := s.Stop(ctx); err != nil {
		t.Fatalf("release lock: %v", err)
	}
	deps.Health = &testHealthRegistry{}
	openTestStore(t, ctx, deps)
}

func TestStopClosesConnections(t *testing.T) {
	ctx := t.Context()
	s := openTestStore(t, ctx, newTestDeps(t))
	for range 2 {
		if err := s.Stop(ctx); err != nil {
			t.Fatalf("stop store: %v", err)
		}
	}
	for _, connection := range []struct {
		name string
		db   *sql.DB
	}{
		{"writer", s.write},
		{"reader", s.Read()},
		{"checkpoint", s.checkpoint},
	} {
		var value int
		if err := connection.db.QueryRowContext(ctx, "SELECT 1").Scan(&value); err == nil {
			t.Errorf("%s accepted a query after Stop", connection.name)
		}
	}
}

func TestOpenValidatesDeps(t *testing.T) {
	type testCase struct {
		name    string
		invalid func(*Deps)
		want    string
	}
	tests := []testCase{
		{"empty directory", func(deps *Deps) { deps.Dir = "" }, "directory"},
		{"zero cache", func(deps *Deps) { deps.CacheKiB = 0 }, "cache size"},
		{"negative cache", func(deps *Deps) { deps.CacheKiB = -1 }, "cache size"},
		{"zero queue", func(deps *Deps) { deps.WriteQueue = 0 }, "queue capacity"},
		{"negative queue", func(deps *Deps) { deps.WriteQueue = -1 }, "queue capacity"},
		{"queue exceeds limit", func(deps *Deps) { deps.WriteQueue = 1<<20 + 1 }, "queue capacity"},
		{"nil metrics", func(deps *Deps) { deps.Metrics = nil }, "metrics set"},
		{"nil health", func(deps *Deps) { deps.Health = nil }, "health registry"},
		{"typed nil health", func(deps *Deps) { deps.Health = (*testHealthRegistry)(nil) }, "health registry"},
		{"nil fatal", func(deps *Deps) { deps.Fatal = nil }, "fatal callback"},
	}
	if strconv.IntSize == 64 {
		oversizedCacheKiB := int64(1<<31 + 1)
		tests = append(tests, testCase{"cache exceeds SQLite range", func(deps *Deps) {
			deps.CacheKiB = int(oversizedCacheKiB)
		}, "cache size"})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			deps := newTestDeps(t)
			deps.Dir = filepath.Join(deps.Dir, "data")
			test.invalid(&deps)
			if err := failTestOpen(t, t.Context(), deps); !strings.Contains(err.Error(), test.want) {
				t.Errorf("Open error = %v, want %q", err, test.want)
			}
			if deps.Dir != "" {
				if _, err := os.Stat(deps.Dir); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("stat data directory after invalid Open = %v, want not exist", err)
				}
			}
		})
	}
}

func TestOpenAcceptsMaximumCacheSize(t *testing.T) {
	if strconv.IntSize != 64 {
		t.Skip("maximum SQLite cache size in KiB requires a 64-bit int")
	}
	cacheKiB := int64(1 << 31)
	deps := newTestDeps(t)
	deps.CacheKiB = int(cacheKiB)
	s := openTestStore(t, t.Context(), deps)
	var got int64
	if err := s.Read().QueryRowContext(t.Context(), "PRAGMA cache_size").Scan(&got); err != nil {
		t.Fatalf("read PRAGMA cache_size: %v", err)
	}
	if got != -cacheKiB {
		t.Errorf("PRAGMA cache_size = %d, want %d", got, -cacheKiB)
	}
}

func TestOpenCreatesDirectory(t *testing.T) {
	deps := newTestDeps(t)
	deps.Dir = filepath.Join(deps.Dir, "data #%&", "store")
	openTestStore(t, t.Context(), deps)
	for _, filename := range []string{"yabot.db", "yabot.lock"} {
		path := filepath.Join(deps.Dir, filename)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %q: %v", path, err)
		}
		if !info.Mode().IsRegular() {
			t.Errorf("%q has mode %s, want a regular file", path, info.Mode())
		}
	}
}

func TestOpenFailureReleasesDirectoryLock(t *testing.T) {
	ctx := t.Context()
	deps := newTestDeps(t)
	dbPath := filepath.Join(deps.Dir, "yabot.db")
	if err := os.Mkdir(dbPath, 0o700); err != nil {
		t.Fatalf("create directory at database path: %v", err)
	}
	failTestOpen(t, ctx, deps)
	if err := os.Remove(dbPath); err != nil {
		t.Fatalf("remove directory at database path: %v", err)
	}
	openTestStore(t, ctx, deps)
}

func TestOpenContextDoesNotOwnStore(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s := openTestStore(t, ctx, newTestDeps(t))
	cancel()
	var value int
	if err := s.Read().QueryRowContext(t.Context(), "SELECT 1").Scan(&value); err != nil {
		t.Fatalf("query after Open context cancellation: %v", err)
	}
	if value != 1 {
		t.Errorf("query returned %d, want 1", value)
	}
}

type testHealthRegistry struct {
	name  string
	check func() error
}

func (h *testHealthRegistry) Register(name string, check func() error) {
	h.name = name
	h.check = check
}

func newTestDeps(t *testing.T) Deps {
	t.Helper()
	return Deps{
		Dir:        testDir(t),
		CacheKiB:   8000,
		WriteQueue: 4096,
		Metrics:    metrics.NewSet(),
		Health:     &testHealthRegistry{},
		Fatal: func(name string, err error) {
			t.Errorf("unexpected fatal error from %s: %v", name, err)
		},
	}
}

func openTestStore(t *testing.T, ctx context.Context, deps Deps) *Store {
	t.Helper()
	s, err := Open(ctx, deps)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Stop(context.Background()); err != nil {
			t.Errorf("stop store: %v", err)
		}
	})
	return s
}

func failTestOpen(t *testing.T, ctx context.Context, deps Deps) error {
	t.Helper()
	s, err := Open(ctx, deps)
	if s != nil {
		if stopErr := s.Stop(ctx); stopErr != nil {
			t.Errorf("stop unexpected store: %v", stopErr)
		}
		t.Fatal("Open returned a store, want nil")
	}
	if err == nil {
		t.Fatal("Open returned no error")
	}
	return err
}

func takeTestConn(t *testing.T, db *sql.DB) *sql.Conn {
	t.Helper()
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatalf("take connection: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close connection: %v", err)
		}
	})
	return conn
}

// Windows may briefly report a directory as non-empty after SQLite deletes its WAL and SHM files.
func testDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "yabot-store-")
	if err != nil {
		t.Fatalf("create test directory: %v", err)
	}
	t.Cleanup(func() {
		deadline := time.Now().Add(2 * time.Second)
		for {
			err := os.RemoveAll(dir)
			if err == nil {
				return
			}
			if time.Now().After(deadline) {
				t.Errorf("remove test directory: %v", err)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	return dir
}
