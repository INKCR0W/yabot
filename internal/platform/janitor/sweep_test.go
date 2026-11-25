package janitor_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/janitor"
	"github.com/INKCR0W/yabot/internal/platform/obs"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/VictoriaMetrics/metrics"
)

func TestSweep(t *testing.T) {
	delErr := errors.New("delete failed")
	for _, tc := range []struct {
		name        string
		initial     int
		delay       time.Duration
		failBatch   int
		want        int
		wantErr     error
		wantBatches int
	}{
		{name: "row limit with rows left", initial: 450, want: 200, wantErr: janitor.ErrBacklog},
		{name: "exact row limit", initial: 200, want: 200, wantErr: janitor.ErrBacklog},
		{name: "empty", wantBatches: 1},
		{name: "short batch", initial: 30, want: 30, wantBatches: 1},
		{name: "short second batch", initial: 130, want: 130, wantBatches: 2},
		{name: "time budget exceeded", initial: 450, delay: 6 * time.Millisecond, want: 100, wantErr: janitor.ErrBacklog, wantBatches: 1},
		{name: "time budget reached", initial: 450, delay: 5 * time.Millisecond, want: 100, wantErr: janitor.ErrBacklog, wantBatches: 1},
		{name: "short batch at time budget", initial: 30, delay: 5 * time.Millisecond, want: 30, wantBatches: 1},
		{name: "rollback failed batch", initial: 450, failBatch: 2, want: 100, wantErr: delErr, wantBatches: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := sweepDataDir(t)
			synctest.Test(t, func(t *testing.T) {
				s := openSweepStore(t, dir, tc.initial)
				defer stopSweepStore(t, s)

				calls := 0
				got, err := janitor.Sweep(t.Context(), s, func(tx *sql.Tx, limit int) (int, error) {
					calls++
					if tc.delay > 0 {
						time.Sleep(tc.delay)
					}
					deleted, err := deleteSweepRows(tx, limit)
					if err != nil {
						return 0, err
					}
					if calls == tc.failBatch {
						return deleted, delErr
					}
					return deleted, nil
				})
				if got != tc.want || !errors.Is(err, tc.wantErr) {
					t.Errorf("Sweep = (%d, %v), want (%d, %v)", got, err, tc.want, tc.wantErr)
				}
				if tc.wantBatches > 0 && calls != tc.wantBatches {
					t.Errorf("delete callback called %d times, want %d", calls, tc.wantBatches)
				}
				assertSweepRows(t, s, tc.initial-tc.want)
			})
		})
	}
}

func TestSweepCanceledWhileQueued(t *testing.T) {
	dir := sweepDataDir(t)
	synctest.Test(t, func(t *testing.T) {
		s := openSweepStore(t, dir, 30)
		defer stopSweepStore(t, s)
		block := make(chan struct{})
		release := sync.OnceFunc(func() { close(block) })
		defer release()
		entered := make(chan struct{})
		done := make(chan error, 1)
		if err := s.Write(func(*sql.Tx) error {
			close(entered)
			<-block
			return nil
		}, func(err error) { done <- err }); err != nil {
			t.Fatal(err)
		}
		<-entered

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		swept := make(chan error, 1)
		go func() {
			rows, err := janitor.Sweep(ctx, s, deleteSweepRows)
			if rows != 0 {
				t.Errorf("Sweep reported %d rows before the writer completed, want 0", rows)
			}
			swept <- err
		}()
		synctest.Wait()
		cancel()
		if err := <-swept; !errors.Is(err, context.Canceled) {
			t.Errorf("Sweep error = %v, want context.Canceled", err)
		}
		release()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if err := s.WriteWait(t.Context(), func(*sql.Tx) error { return nil }); err != nil {
			t.Fatal(err)
		}
		assertSweepRows(t, s, 30)
	})
}

func openSweepStore(t *testing.T, dir string, rows int) *store.Store {
	t.Helper()
	ctx := t.Context()
	s, err := store.Open(ctx, store.Deps{
		Dir:                dir,
		CacheKiB:           8000,
		WriteQueue:         8,
		CheckpointInterval: time.Hour,
		BackupKeep:         7,
		Metrics:            metrics.NewSet(),
		Health:             obs.NewHealth(),
		Fatal:              func(component string, err error) { t.Errorf("%s fatal: %v", component, err) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(ctx); err != nil {
		stopSweepStore(t, s)
		t.Fatal(err)
	}
	if err := s.WriteWait(ctx, func(tx *sql.Tx) error {
		if _, err := tx.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY)"); err != nil {
			return err
		}
		if rows == 0 {
			return nil
		}
		_, err := tx.Exec(`WITH RECURSIVE ids(id) AS (VALUES (1) UNION ALL SELECT id + 1 FROM ids WHERE id < ?)
			INSERT INTO items SELECT id FROM ids`, rows)
		return err
	}); err != nil {
		stopSweepStore(t, s)
		t.Fatal(err)
	}
	return s
}

func stopSweepStore(t *testing.T, s *store.Store) {
	t.Helper()
	if err := s.Stop(context.Background()); err != nil {
		t.Errorf("stop store: %v", err)
	}
}

func sweepDataDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	if runtime.GOOS == "windows" {
		// Windows may briefly report deleted SQLite WAL and SHM files as present.
		t.Cleanup(func() {
			deadline := time.Now().Add(2 * time.Second)
			for {
				err := os.RemoveAll(dir)
				if err == nil {
					return
				}
				if time.Now().After(deadline) {
					t.Errorf("remove store data directory: %v", err)
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
	return dir
}

func deleteSweepRows(tx *sql.Tx, limit int) (int, error) {
	result, err := tx.Exec("DELETE FROM items WHERE id IN (SELECT id FROM items ORDER BY id LIMIT ?)", limit)
	if err != nil {
		return 0, err
	}
	rows, err := result.RowsAffected()
	return int(rows), err
}

func assertSweepRows(t *testing.T, s *store.Store, want int) {
	t.Helper()
	var got int
	if err := s.Read().QueryRowContext(t.Context(), "SELECT count(*) FROM items").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("rows remaining = %d, want %d", got, want)
	}
}
