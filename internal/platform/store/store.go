package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	_ "modernc.org/sqlite"
)

const maxCacheKiB int64 = 1 << 31

type Deps struct {
	Dir      string
	CacheKiB int // SQLite stores the negated value as a signed 32-bit integer.
}

// Store owns the database pools and data directory lock. It must be opened with Open and must not be copied.
type Store struct {
	write      *sql.DB
	read       *sql.DB
	checkpoint *sql.DB
	lock       *os.File
	stopOnce   sync.Once
	stopErr    error
}

func Open(ctx context.Context, deps Deps) (_ *Store, err error) {
	if deps.Dir == "" {
		return nil, errors.New("store data directory must not be empty")
	}
	if deps.CacheKiB <= 0 || int64(deps.CacheKiB) > maxCacheKiB {
		return nil, fmt.Errorf("store cache size must be between 1 and %d KiB: %d", maxCacheKiB, deps.CacheKiB)
	}
	dir, err := filepath.Abs(deps.Dir)
	if err != nil {
		return nil, fmt.Errorf("resolve store data directory %q: %w", deps.Dir, err)
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create store data directory %q: %w", dir, err)
	}
	lockPath := filepath.Join(dir, "yabot.lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open data directory lock %q: %w", lockPath, err)
	}
	if err := lockFile(lock); err != nil {
		if errors.Is(err, lockConflict) {
			err = fmt.Errorf("data directory is locked by another process: %q: %w", lockPath, err)
		} else {
			err = fmt.Errorf("lock data directory at %q: %w", lockPath, err)
		}
		return nil, errors.Join(err, closeLockFile(lock))
	}

	s := &Store{lock: lock}
	defer func() {
		if err != nil {
			err = errors.Join(err, s.close())
		}
	}()

	dbPath := filepath.Join(dir, "yabot.db")
	params := url.Values{"_pragma": {
		"journal_mode(WAL)",
		"synchronous(NORMAL)",
		"busy_timeout(5000)",
		"foreign_keys(ON)",
		"wal_autocheckpoint(0)",
		"journal_size_limit(67108864)",
		"cache_size(" + strconv.Itoa(-deps.CacheKiB) + ")",
	}}
	// The writer creates the database and enables WAL before any read-only connection opens.
	params.Set("_txlock", "immediate")
	if s.write, err = openPool(ctx, dbPath, params, 1); err != nil {
		return nil, fmt.Errorf("open store writer at %q: %w", dbPath, err)
	}
	params.Del("_txlock")
	if s.checkpoint, err = openPool(ctx, dbPath, params, 1); err != nil {
		return nil, fmt.Errorf("open store checkpoint connection at %q: %w", dbPath, err)
	}
	params.Set("mode", "ro")
	params.Add("_pragma", "query_only(1)")
	if s.read, err = openPool(ctx, dbPath, params, 2); err != nil {
		return nil, fmt.Errorf("open store reader at %q: %w", dbPath, err)
	}
	return s, nil
}

func openPool(ctx context.Context, path string, params url.Values, limit int) (*sql.DB, error) {
	dsn := url.URL{Scheme: "file", OmitHost: true, Path: filepath.ToSlash(path), RawQuery: params.Encode()}
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(limit)
	db.SetMaxIdleConns(limit)
	var mode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		return db, fmt.Errorf("read journal mode: %w", err)
	}
	if mode != "wal" {
		return db, fmt.Errorf("journal mode is %q, want wal", mode)
	}
	return db, nil
}

// Read returns a store-owned pool. Callers must release its rows, transactions and borrowed connections before Stop.
func (s *Store) Read() *sql.DB { return s.read }

func (s *Store) Name() string { return "store" }

func (s *Store) Start(context.Context) error { return nil }

// Stop closes the pools and releases the data directory lock. Concurrent calls return the same result.
func (s *Store) Stop(context.Context) error {
	s.stopOnce.Do(func() { s.stopErr = s.close() })
	return s.stopErr
}

func (s *Store) close() error {
	var errs []error
	for _, pool := range []struct {
		name string
		db   *sql.DB
	}{
		{"reader", s.read},
		{"checkpoint connection", s.checkpoint},
		{"writer", s.write},
	} {
		if pool.db == nil {
			continue
		}
		if err := pool.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close store %s: %w", pool.name, err))
		}
	}
	if err := unlockFile(s.lock); err != nil {
		errs = append(errs, fmt.Errorf("unlock data directory at %q: %w", s.lock.Name(), err))
	}
	return errors.Join(append(errs, closeLockFile(s.lock))...)
}

func closeLockFile(lock *os.File) error {
	if err := lock.Close(); err != nil {
		return fmt.Errorf("close data directory lock %q: %w", lock.Name(), err)
	}
	return nil
}
