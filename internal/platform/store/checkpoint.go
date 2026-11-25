package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/VictoriaMetrics/metrics"
)

const (
	checkpointPassive  = "PASSIVE"
	checkpointTruncate = "TRUNCATE"
	walGrowthThreshold = 20
)

type checkpointMetrics struct {
	walPages   *metrics.Gauge
	walGrowing *metrics.Gauge
	errors     *metrics.Counter
}

type checkpointResult struct {
	busy         int
	logPages     int
	checkpointed int
}

type checkpointState struct {
	lastStartedBatches uint64
	isWALEmpty         bool
	walGrowth          walGrowthState
}

type walGrowthState struct {
	lastPages int
	increases int
	hasSample bool
}

func (state walGrowthState) isGrowing() bool {
	return state.increases == walGrowthThreshold
}

func observeWALGrowth(state walGrowthState, pages int, err error) (walGrowthState, bool) {
	// SQLite reports -1 when the checkpoint could not sample the WAL.
	if err != nil || pages < 0 {
		return state, false
	}
	switch {
	case !state.hasSample || pages < state.lastPages:
		state.increases = 0
	case pages > state.lastPages && state.increases < walGrowthThreshold:
		state.increases++
	}
	state.lastPages = pages
	state.hasSample = true
	return state, true
}

func (s *Store) runCheckpoints(ctx context.Context) {
	defer close(s.checkpointDone)
	var ticks <-chan time.Time
	if s.hooks != nil {
		ticks = s.hooks.checkpointTicks
	}
	if ticks == nil {
		ticker := time.NewTicker(s.checkpointInterval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	var state checkpointState
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ticks:
			if !ok || ctx.Err() != nil {
				return
			}
			mode, result, err := s.checkpointTick(ctx, &state)
			if s.hooks != nil && s.hooks.afterCheckpointTick != nil {
				s.hooks.afterCheckpointTick(mode, result, err)
			}
		}
	}
}

func (s *Store) checkpointTick(ctx context.Context, state *checkpointState) (string, checkpointResult, error) {
	if state.isWALEmpty && s.startedBatches.Load() == state.lastStartedBatches {
		return "", checkpointResult{}, nil
	}

	mode, result, err := s.executeCheckpoint(ctx, state)
	// PASSIVE can report zero valid frames while the WAL file still holds rolled-back pages.
	state.isWALEmpty = err == nil && mode == checkpointTruncate && result.busy == 0 && result.logPages == 0
	s.recordCheckpointResult(&state.walGrowth, result.logPages, err)
	if err != nil && !errors.Is(err, ctx.Err()) {
		s.checkpointMetrics.errors.Inc()
		s.log.Error("store checkpoint failed", "mode", mode, "error", err)
	}
	return mode, result, err
}

func (s *Store) executeCheckpoint(ctx context.Context, state *checkpointState) (string, checkpointResult, error) {
	conn, err := s.checkpoint.Conn(ctx)
	if err != nil {
		return "", checkpointResult{}, err
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil && !errors.Is(closeErr, sql.ErrConnDone) {
			s.log.Error("store failed to return checkpoint connection", "error", closeErr)
		}
	}()

	// Backup can occupy this connection while the writer keeps committing.
	mode := checkpointTruncate
	startedBatches := s.startedBatches.Load()
	if startedBatches != state.lastStartedBatches {
		mode = checkpointPassive
		state.lastStartedBatches = startedBatches
	}

	if s.hooks != nil && s.hooks.beforeCheckpoint != nil {
		if err := s.hooks.beforeCheckpoint(ctx, mode); err != nil {
			return mode, checkpointResult{}, err
		}
	}
	var result checkpointResult
	err = conn.QueryRowContext(ctx, "PRAGMA wal_checkpoint("+mode+")").
		Scan(&result.busy, &result.logPages, &result.checkpointed)
	return mode, result, err
}

func (s *Store) recordCheckpointResult(state *walGrowthState, pages int, err error) {
	next, hasSample := observeWALGrowth(*state, pages, err)
	if !hasSample {
		return
	}
	wasGrowing, isGrowing := state.isGrowing(), next.isGrowing()
	*state = next
	s.checkpointMetrics.walPages.Set(float64(pages))
	var growing float64
	if isGrowing {
		growing = 1
	}
	s.checkpointMetrics.walGrowing.Set(growing)
	switch {
	case isGrowing && !wasGrowing:
		s.log.Warn("store WAL keeps growing", "wal_pages", pages)
	case !isGrowing && wasGrowing:
		s.log.Info("store WAL stopped growing", "wal_pages", pages)
	}
}
