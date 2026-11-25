package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	maxBatchSize = 128
	faultAfter   = 3
)

var (
	ErrRollbackOp = errors.New("store: rollback op")
	ErrBusy       = errors.New("store: write queue is full")
	ErrClosed     = errors.New("store: writer is closed")
	ErrFaulted    = errors.New("store: writer is faulted")
	ErrAborted    = errors.New("store: batch aborted")
)

// Op runs on the writer goroutine. It must finish its queries before returning and leave transaction control to the store.
// If a statement on tx returns an error, return it immediately (possibly wrapped) without executing any more statements on tx,
// because SQLite may have rolled back the transaction, letting subsequent statements autocommit despite a batch abort.
// Do not call runtime.Goexit (including via t.Fatal), which can leave the writer stuck in connection cleanup.
type Op func(tx *sql.Tx) error

// OpError reports a deterministic failure whose operation was individually rolled back.
type OpError struct{ Err error }

func (e *OpError) Error() string { return fmt.Sprintf("store operation rolled back: %v", e.Err) }
func (e *OpError) Unwrap() error { return e.Err }

type writeRequest struct {
	op   Op
	done func(error)
}

type writerMetrics struct {
	commitSeconds *metrics.Histogram
	batchSize     *metrics.Histogram
	batchAborts   *metrics.Counter
}

type writerFault struct{ cause error }

func (e *writerFault) Error() string { return fmt.Sprintf("%v: %v", ErrFaulted, e.cause) }
func (e *writerFault) Unwrap() error { return ErrFaulted }

// Write tries to enqueue op. On success, the writer calls done exactly once after the batch finishes.
// done runs synchronously on the writer goroutine; it must be non-blocking and must not call runtime.Goexit.
func (s *Store) Write(op Op, done func(error)) error {
	if op == nil || done == nil {
		return errors.New("store: write requires a non-nil operation and callback")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.fault.Load() != nil:
		return ErrFaulted
	case s.writerDone == nil || s.isStopping:
		return ErrClosed
	}
	select {
	case s.queue <- writeRequest{op: op, done: done}:
		return nil
	default:
		return ErrBusy
	}
}

// WriteWait tries to enqueue op even if ctx is already done. Once op is queued,
// it can still commit if the wait returns ctx.Err().
func (s *Store) WriteWait(ctx context.Context, op Op) error {
	result := make(chan error, 1)
	if err := s.Write(op, func(err error) { result <- err }); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) runWriter() {
	defer close(s.writerDone)
	var batch [maxBatchSize]writeRequest
	var results [maxBatchSize]error
	consecutiveFailures := 0
	for first := range s.queue {
		batch[0] = first
		size := s.drainBatch(batch[:])
		var batchErr error
		if fault := s.fault.Load(); fault != nil {
			batchErr = fault
		} else {
			s.metrics.batchSize.Update(float64(size))
			batchErr = s.writeBatch(batch[:size], results[:size])
			if batchErr != nil {
				consecutiveFailures++
				s.reportBatchFailure(batchErr, consecutiveFailures)
			} else {
				consecutiveFailures = 0
			}
		}
		for i, request := range batch[:size] {
			result := batchErr
			if result == nil {
				result = results[i]
			}
			s.completeWrite(request, result)
		}
		clear(batch[:size])
		clear(results[:size])
	}
}

func (s *Store) drainBatch(batch []writeRequest) int {
	for i := 1; i < len(batch); i++ {
		select {
		case request, ok := <-s.queue:
			if !ok {
				return i
			}
			batch[i] = request
		default:
			return i
		}
	}
	return len(batch)
}

func (s *Store) reportBatchFailure(err error, consecutive int) {
	s.metrics.batchAborts.Inc()
	var fault *writerFault
	if consecutive >= faultAfter {
		fault = &writerFault{cause: err}
		s.mu.Lock()
		s.fault.Store(fault)
		s.mu.Unlock()
	}
	s.log.Error("store batch failed", "error", err, "consecutive_failures", consecutive)
	if fault != nil {
		s.log.Error("store entered fault state", "error", fault, "consecutive_failures", consecutive)
		s.fatal(s.Name(), fault)
	}
}

func (s *Store) writeBatch(batch []writeRequest, results []error) error {
	ctx := context.Background()
	conn, err := s.write.Conn(ctx)
	if err != nil {
		return abortBatch("acquire writer connection", err)
	}
	defer func() {
		if err := conn.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
			s.log.Error("store failed to return writer connection", "error", err)
		}
	}()

	start := time.Now()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		s.rollbackConnection(ctx, conn)
		return abortBatch("begin immediate", err)
	}
	// A rolled-back batch can leave pages in the WAL, so report activity after the transaction ends.
	defer s.startedBatches.Add(1)
	for i, request := range batch {
		opErr, err := executeOp(tx, request.op)
		if err != nil {
			s.logRollbackError(tx.Rollback())
			return abortBatch("execute operation", err)
		}
		results[i] = opErr
	}

	if s.hooks != nil && s.hooks.beforeCommit != nil {
		if err := s.hooks.beforeCommit(tx); err != nil {
			s.logRollbackError(tx.Rollback())
			return abortBatch("commit", err)
		}
	}
	err = tx.Commit()
	s.metrics.commitSeconds.Update(time.Since(start).Seconds())
	if err != nil {
		// The driver rolls back after a failed COMMIT; this ROLLBACK is a fallback on the same connection.
		s.rollbackConnection(ctx, conn)
		return abortBatch("commit", err)
	}
	if s.hooks != nil && s.hooks.afterCommit != nil {
		s.hooks.afterCommit()
	}
	return nil
}

func executeOp(tx *sql.Tx, op Op) (opErr, batchErr error) {
	if _, err := tx.Exec("SAVEPOINT store_op"); err != nil {
		return nil, fmt.Errorf("open savepoint: %w", err)
	}
	err := callOp(tx, op)
	if err != nil {
		if isEngineError(err) {
			return nil, err
		}
		if _, rollbackErr := tx.Exec("ROLLBACK TO store_op"); rollbackErr != nil {
			return nil, fmt.Errorf("roll back savepoint after operation failed (%v): %w", err, rollbackErr)
		}
	}
	if _, releaseErr := tx.Exec("RELEASE store_op"); releaseErr != nil {
		if err != nil {
			return nil, fmt.Errorf("release savepoint after operation failed (%v): %w", err, releaseErr)
		}
		return nil, fmt.Errorf("release savepoint: %w", releaseErr)
	}
	if err != nil {
		return &OpError{Err: err}, nil
	}
	return nil, nil
}

func isEngineError(err error) bool {
	for err != nil {
		if hasEngineCode(err) {
			return true
		}
		switch wrapped := err.(type) {
		case interface{ Unwrap() error }:
			err = wrapped.Unwrap()
		case interface{ Unwrap() []error }:
			for _, child := range wrapped.Unwrap() {
				if isEngineError(child) {
					return true
				}
			}
			return false
		default:
			return false
		}
	}
	return false
}

func hasEngineCode(err error) bool {
	sqliteErr, ok := err.(*sqlite.Error)
	if !ok {
		matcher, ok := err.(interface{ As(any) bool })
		if !ok || !matcher.As(&sqliteErr) {
			return false
		}
	}
	if sqliteErr == nil {
		return false
	}
	return isEngineCode(sqliteErr.Code())
}

func isEngineCode(code int) bool {
	const primaryCodeMask = 0xff
	switch code & primaryCodeMask {
	case sqlite3.SQLITE_FULL, sqlite3.SQLITE_IOERR, sqlite3.SQLITE_NOMEM, sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB:
		return true
	default:
		return false
	}
}

func abortBatch(stage string, cause error) error {
	// A cause returned by an Op may itself wrap OpError. Batch failures must remain transient.
	return fmt.Errorf("%w during %s: %v", ErrAborted, stage, cause)
}

func (s *Store) rollbackConnection(ctx context.Context, conn *sql.Conn) {
	_, err := conn.ExecContext(ctx, "ROLLBACK")
	s.logRollbackError(err)
}

func (s *Store) logRollbackError(err error) {
	if err == nil || errors.Is(err, sql.ErrTxDone) || isNoActiveTransactionError(err) {
		return
	}
	s.log.Error("store failed to roll back batch", "error", err)
}

func isNoActiveTransactionError(err error) bool {
	var sqliteErr *sqlite.Error
	return errors.As(err, &sqliteErr) && sqliteErr.Code() == sqlite3.SQLITE_ERROR &&
		strings.Contains(sqliteErr.Error(), "cannot rollback - no transaction is active")
}

func callOp(tx *sql.Tx, op Op) (err error) {
	completed := false
	defer func() {
		if !completed {
			value := recover()
			if cause, ok := value.(error); ok {
				err = fmt.Errorf("store operation panicked: %w", cause)
			} else {
				err = fmt.Errorf("store operation panicked: %v", value)
			}
		}
	}()
	err = op(tx)
	completed = true
	return err
}

func (s *Store) completeWrite(request writeRequest, err error) {
	completed := false
	defer func() {
		if !completed {
			s.log.Error("store completion callback panicked", "panic", recover())
		}
	}()
	request.done(err)
	completed = true
}
