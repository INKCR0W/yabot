package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

const (
	maxCacheKiB   int64 = 1 << 31
	maxWriteQueue       = 1 << 20
)

type Deps struct {
	Dir                string
	CacheKiB           int // SQLite stores the negated value as a signed 32-bit integer.
	Log                *slog.Logger
	WriteQueue         int              // Valid range: 1 to 1<<20.
	CheckpointInterval time.Duration    // Must be positive. The configuration default is 500 ms.
	BackupKeep         int              // Pre-migration snapshots to keep; daily retention is Backup's keep. Must be positive.
	Now                func() time.Time // Supplies backup timestamps. Defaults to time.Now.
	Metrics            *metrics.Set
	Health             HealthRegistry
	Fatal              func(component string, err error)
}

type HealthRegistry interface {
	Register(name string, check func() error)
}

// Store owns the database pools and data directory lock. It must be opened with Open and must not be copied.
type Store struct {
	write      *sql.DB
	read       *sql.DB
	checkpoint *sql.DB
	lock       *os.File
	log        *slog.Logger
	fatal      func(string, error)
	metrics    writerMetrics
	queue      chan writeRequest
	fault      atomic.Pointer[writerFault]
	hooks      *storeHooks

	checkpointInterval time.Duration
	checkpointMetrics  checkpointMetrics
	startedBatches     atomic.Uint64
	backupDir          string
	backupKeep         int
	now                func() time.Time
	maintenance        chan struct{}
	closing            chan struct{}

	mu               sync.Mutex
	migrating        bool
	writerDone       chan struct{}
	checkpointDone   chan struct{}
	cancelCheckpoint context.CancelFunc
	isStopping       bool
	stopped          chan struct{}
	stopErr          error
}

type storeHooks struct {
	beforeCommit        func(*sql.Tx) error
	afterCommit         func()
	checkpointTicks     <-chan time.Time
	beforeCheckpoint    func(context.Context, string) error
	afterCheckpointTick func(string, checkpointResult, error)
	syncBackupDirectory func(string) error
	beforeClosePool     func(name string) error
	afterClosePool      func(name string)
}

func Open(ctx context.Context, deps Deps) (opened *Store, err error) {
	if err := validateDeps(deps); err != nil {
		return nil, err
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

	log := deps.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	s := &Store{
		lock:               lock,
		log:                log,
		fatal:              deps.Fatal,
		queue:              make(chan writeRequest, deps.WriteQueue),
		checkpointInterval: deps.CheckpointInterval,
		backupDir:          filepath.Join(dir, "backup"),
		backupKeep:         deps.BackupKeep,
		now:                now,
		maintenance:        make(chan struct{}, 1),
		closing:            make(chan struct{}),
		stopped:            make(chan struct{}),
	}
	defer func() {
		if opened != nil {
			return
		}
		if closeErr := s.close(); closeErr != nil {
			if err == nil {
				log.Error("store cleanup failed during open", "error", closeErr)
			} else {
				err = errors.Join(err, closeErr)
			}
		}
	}()

	dbPath := filepath.Join(dir, "yabot.db")
	params := url.Values{
		"_busy_timeout": {"5000"},
		"_pragma": {
			"journal_mode(WAL)",
			"synchronous(NORMAL)",
			"foreign_keys(ON)",
			"wal_autocheckpoint(0)",
			"journal_size_limit(67108864)",
			"cache_size(" + strconv.Itoa(-deps.CacheKiB) + ")",
		},
	}
	// The writer creates the database and enables WAL before any read-only connection opens.
	params.Set("_txlock", "immediate")
	if s.write, err = openPool(ctx, dbPath, params, 1); err != nil {
		return nil, fmt.Errorf("open store writer at %q: %w", dbPath, err)
	}
	params.Del("_txlock")
	params.Set("_busy_timeout", "0")
	if s.checkpoint, err = openPool(ctx, dbPath, params, 1); err != nil {
		return nil, fmt.Errorf("open store checkpoint connection at %q: %w", dbPath, err)
	}
	params.Set("_busy_timeout", "5000")
	params.Set("mode", "ro")
	params.Add("_pragma", "query_only(1)")
	if s.read, err = openPool(ctx, dbPath, params, 2); err != nil {
		return nil, fmt.Errorf("open store reader at %q: %w", dbPath, err)
	}
	s.metrics = writerMetrics{
		commitSeconds: deps.Metrics.GetOrCreateHistogram("yabot_store_commit_seconds"),
		batchSize:     deps.Metrics.GetOrCreateHistogram("yabot_store_batch_size"),
		batchAborts:   deps.Metrics.GetOrCreateCounter("yabot_store_batch_aborts_total"),
	}
	s.checkpointMetrics = checkpointMetrics{
		walPages:   deps.Metrics.GetOrCreateGauge("yabot_store_wal_pages", nil),
		walGrowing: deps.Metrics.GetOrCreateGauge("yabot_store_wal_growing", nil),
		errors:     deps.Metrics.GetOrCreateCounter("yabot_store_checkpoint_errors_total"),
	}
	deps.Health.Register(s.Name(), s.checkHealth)
	return s, nil
}

func validateDeps(deps Deps) error {
	switch {
	case deps.Dir == "":
		return errors.New("store data directory must not be empty")
	case deps.CacheKiB <= 0 || int64(deps.CacheKiB) > maxCacheKiB:
		return fmt.Errorf("store cache size must be between 1 and %d KiB: %d", maxCacheKiB, deps.CacheKiB)
	case deps.WriteQueue <= 0 || deps.WriteQueue > maxWriteQueue:
		return fmt.Errorf("store write queue capacity must be between 1 and %d: %d", maxWriteQueue, deps.WriteQueue)
	case deps.CheckpointInterval <= 0:
		return fmt.Errorf("store checkpoint interval must be positive: %s", deps.CheckpointInterval)
	case deps.BackupKeep <= 0:
		return fmt.Errorf("store backup retention must be positive: %d", deps.BackupKeep)
	case deps.Metrics == nil:
		return errors.New("store metrics set must not be nil")
	case isNilHealthRegistry(deps.Health):
		return errors.New("store health registry must not be nil")
	case deps.Fatal == nil:
		return errors.New("store fatal callback must not be nil")
	default:
		return nil
	}
}

func isNilHealthRegistry(registry HealthRegistry) bool {
	value := reflect.ValueOf(registry)
	switch value.Kind() {
	case reflect.Invalid:
		return true
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
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

// Start runs the writer and checkpointer until Stop, independently of the startup context.
func (s *Store) Start(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isStopping {
		return ErrClosed
	}
	if s.writerDone != nil {
		return errors.New("store has already started")
	}
	if s.migrating {
		return errors.New("store migration is in progress")
	}
	checkpointCtx, cancelCheckpoint := context.WithCancel(context.Background())
	s.cancelCheckpoint = cancelCheckpoint
	s.writerDone = make(chan struct{})
	s.checkpointDone = make(chan struct{})
	go s.runWriter()
	go s.runCheckpoints(checkpointCtx)
	return nil
}

// Stop drains accepted writes, stops the checkpointer and waits for active backup or migration work before closing the pools.
// It releases the directory lock last. If ctx ends first, cleanup continues in the background.
func (s *Store) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.isStopping {
		s.isStopping = true
		close(s.closing)
		close(s.queue)
		go s.finishStop(s.writerDone)
	}
	s.mu.Unlock()

	select {
	case <-s.stopped:
		return s.stopErr
	case <-ctx.Done():
		select {
		case <-s.stopped:
			return s.stopErr
		default:
			return fmt.Errorf("stop store: %w", ctx.Err())
		}
	}
}

func (s *Store) finishStop(writerDone <-chan struct{}) {
	if writerDone != nil {
		<-writerDone
		s.cancelCheckpoint()
		<-s.checkpointDone
	}
	s.maintenance <- struct{}{}
	s.stopErr = s.close()
	<-s.maintenance
	close(s.stopped)
}

func (s *Store) beginMaintenance(ctx context.Context) error {
	if s == nil || s.maintenance == nil {
		return errors.New("store is not open")
	}
	select {
	case <-s.closing:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	case s.maintenance <- struct{}{}:
	}
	// Recheck after acquiring the slot because select can choose any ready case.
	select {
	case <-s.closing:
		s.endMaintenance()
		return ErrClosed
	default:
	}
	if err := ctx.Err(); err != nil {
		s.endMaintenance()
		return err
	}
	return nil
}

func (s *Store) endMaintenance() {
	<-s.maintenance
}

func (s *Store) checkHealth() error {
	if fault := s.fault.Load(); fault != nil {
		return fault
	}
	return nil
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
		if s.hooks != nil && s.hooks.beforeClosePool != nil {
			if err := s.hooks.beforeClosePool(pool.name); err != nil {
				errs = append(errs, fmt.Errorf("close store %s: %w", pool.name, err))
			}
		}
		if err := pool.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close store %s: %w", pool.name, err))
		}
		if s.hooks != nil && s.hooks.afterClosePool != nil {
			s.hooks.afterClosePool(pool.name)
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
