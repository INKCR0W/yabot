//go:build bench

package store

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	benchExistingSessions = 100_000
	benchSessionKeys      = 120_000
	benchUpsertSession    = `INSERT INTO sessions(chat_id, user_id, sid, state, version, query_id, deadline, payload)
		VALUES (?, ?, '0123456789abcdef', 0, 1, 'fedcba9876543210', ?, zeroblob(152))
		ON CONFLICT(chat_id, user_id) DO UPDATE SET
			state = 1 - sessions.state,
			version = sessions.version + 1,
			deadline = excluded.deadline`
)

func TestStoreOpenLoopBenchmark(t *testing.T) {
	rate := benchPositiveEnv(t, "YABOT_STORE_BENCH_RATE", 1000, strconv.Atoi)
	warmup := benchPositiveEnv(t, "YABOT_STORE_BENCH_WARMUP", time.Second, time.ParseDuration)
	duration := benchPositiveEnv(t, "YABOT_STORE_BENCH_DURATION", 4*time.Second, time.ParseDuration)
	rounds := benchPositiveEnv(t, "YABOT_STORE_BENCH_ROUNDS", 2, strconv.Atoi)
	deps := newTestDeps(t)
	s := openTestStore(t, t.Context(), deps)
	peak := &benchWALPeak{pages: -1}
	s.hooks = &storeHooks{afterCheckpointTick: peak.observe}
	upsert := prepareBenchSessions(t, s)
	defer func() {
		if err := upsert.Close(); err != nil {
			t.Errorf("close session statement: %v", err)
		}
	}()
	// Start measuring from an empty WAL so the preload does not inflate WAL pages and latency.
	var busy, logPages, checkpointed int
	if err := s.checkpoint.QueryRowContext(t.Context(), "PRAGMA wal_checkpoint(TRUNCATE)").
		Scan(&busy, &logPages, &checkpointed); err != nil || busy != 0 || logPages != 0 {
		t.Fatalf("truncate preload WAL: busy=%d log=%d err=%v", busy, logPages, err)
	}
	load := benchStoreLoad{
		store: s, upsert: upsert, rng: rand.New(rand.NewPCG(1, 2)), now: storeBenchClock(t),
		deadline: 1_700_000_000_000,
	}
	startBatchTestStore(t, s)
	t.Logf("rate=%d writes/s warmup=%v duration=%v rounds=%d GOMAXPROCS=%d checkpoint_interval=%v",
		rate, warmup, duration, rounds, runtime.GOMAXPROCS(0), deps.CheckpointInterval)
	if _, err := load.run(warmup, rate); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	for round := range rounds {
		s.metrics.batchSize.Reset()
		peak.take()
		latencies, err := load.run(duration, rate)
		pages := peak.take()
		if err != nil {
			t.Fatalf("round %d: %v", round+1, err)
		}
		var batches uint64
		s.metrics.batchSize.VisitNonZeroBuckets(func(_ string, count uint64) { batches += count })
		if batches == 0 {
			t.Fatal("no batch size observations")
		}
		slices.Sort(latencies)
		maxPages := "unobserved"
		if pages >= 0 {
			maxPages = strconv.Itoa(pages)
		}
		t.Logf("round=%d writes=%d p50=%v p90=%v p99=%v p99.9=%v avg_batch_size=%.2f max_observed_wal_pages=%s",
			round+1, len(latencies), benchPercentile(latencies, 0.50), benchPercentile(latencies, 0.90),
			benchPercentile(latencies, 0.99), benchPercentile(latencies, 0.999),
			float64(len(latencies))/float64(batches), maxPages)
	}
}

func benchPositiveEnv[T ~int | ~int64](t *testing.T, name string, fallback T, parse func(string) (T, error)) T {
	t.Helper()
	if value, ok := os.LookupEnv(name); ok {
		parsed, err := parse(value)
		if err != nil || parsed <= 0 {
			t.Fatalf("%s must be positive, got %q", name, value)
		}
		return parsed
	}
	return fallback
}

func prepareBenchSessions(t *testing.T, s *Store) *sql.Stmt {
	t.Helper()
	ctx := t.Context()
	if _, err := s.write.ExecContext(ctx, `CREATE TABLE sessions (
		chat_id INTEGER NOT NULL,
		user_id INTEGER NOT NULL,
		sid TEXT NOT NULL,
		state INTEGER NOT NULL,
		version INTEGER NOT NULL,
		query_id TEXT NOT NULL,
		deadline INTEGER NOT NULL,
		payload BLOB NOT NULL,
		PRIMARY KEY (chat_id, user_id)
	)`); err != nil {
		t.Fatalf("create sessions table: %v", err)
	}
	upsert, err := s.write.PrepareContext(ctx, benchUpsertSession)
	if err != nil {
		t.Fatalf("prepare session upsert: %v", err)
	}
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin session preload: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("roll back session preload: %v", err)
		}
	}()
	statement := tx.Stmt(upsert)
	for key := range benchExistingSessions {
		chatID, userID := benchSessionKey(key)
		if _, err := statement.Exec(chatID, userID, int64(0)); err != nil {
			t.Fatalf("preload session %d: %v", key, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit session preload: %v", err)
	}
	return upsert
}

func benchSessionKey(key int) (chatID, userID int64) {
	return -1_000_000_000_000 - int64(key/100), 1_000_000_000 + int64(key%100)
}

type benchStoreLoad struct {
	store    *Store
	upsert   *sql.Stmt
	rng      *rand.Rand
	now      func() time.Duration
	deadline int64
}

type benchStoreSample struct {
	latency time.Duration
	err     error
}

func (l *benchStoreLoad) run(duration time.Duration, rate int) ([]time.Duration, error) {
	planned := duration.Seconds() * float64(rate)
	if planned < 1 || planned >= float64(math.MaxInt) {
		return nil, fmt.Errorf("duration %v at %d writes/s yields an invalid sample count", duration, rate)
	}
	total := int(planned)
	samples := make(chan benchStoreSample, total)
	start := l.now()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	var firstErr error
	sent := 0
	for sent < total && firstErr == nil {
		<-ticker.C
		due := int(min((l.now()-start).Seconds()*float64(rate), planned))
		for ; sent < due; sent++ {
			if err := l.submit(samples); err != nil {
				firstErr = fmt.Errorf("enqueue write %d: %w", sent+1, err)
				break
			}
		}
	}
	latencies := make([]time.Duration, sent)
	for i := range latencies {
		sample := <-samples
		latencies[i] = sample.latency
		if sample.err != nil && firstErr == nil {
			firstErr = fmt.Errorf("complete write %d: %w", i+1, sample.err)
		}
	}
	return latencies, firstErr
}

func (l *benchStoreLoad) submit(samples chan<- benchStoreSample) error {
	chatID, userID := benchSessionKey(l.rng.IntN(benchSessionKeys))
	l.deadline++
	deadline := l.deadline
	op := func(tx *sql.Tx) error {
		_, err := tx.Stmt(l.upsert).Exec(chatID, userID, deadline)
		return err
	}
	start := l.now()
	return l.store.Write(op, func(err error) {
		samples <- benchStoreSample{l.now() - start, err}
	})
}

type benchWALPeak struct {
	mu    sync.Mutex
	pages int
}

func (p *benchWALPeak) observe(mode string, result checkpointResult, err error) {
	if mode == "" || err != nil || result.logPages < 0 {
		return
	}
	p.mu.Lock()
	p.pages = max(p.pages, result.logPages)
	p.mu.Unlock()
}

func (p *benchWALPeak) take() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	pages := p.pages
	p.pages = -1
	return pages
}

func benchPercentile(sorted []time.Duration, quantile float64) time.Duration {
	return sorted[int(math.Ceil(quantile*float64(len(sorted))))-1]
}
