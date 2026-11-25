package janitor

import (
	"context"
	"database/sql"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/store"
)

const (
	SweepRows   = 200
	SweepBudget = 5 * time.Millisecond
)

// Keep the batch limit below SweepRows so the budget can stop the next submission.
const sweepBatchRows = 100

type Writer interface {
	WriteWait(ctx context.Context, op store.Op) error
}

// Sweep counts deletions from every successful WriteWait, even after SweepBudget,
// while canceling the wait can leave a later commit uncounted.
// del must delete up to the supplied limit and report fewer rows only when no eligible rows remain,
// so Sweep ends successfully on a short batch and returns ErrBacklog when either limit prevents another submission.
func Sweep(ctx context.Context, w Writer, del func(tx *sql.Tx, limit int) (int, error)) (int, error) {
	start := time.Now()
	total := 0
	for total < SweepRows {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		if time.Since(start) >= SweepBudget {
			return total, ErrBacklog
		}
		limit := min(sweepBatchRows, SweepRows-total)
		var rows int
		if err := w.WriteWait(ctx, func(tx *sql.Tx) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			var err error
			rows, err = del(tx, limit)
			return err
		}); err != nil {
			return total, err
		}
		total += rows
		if rows < limit {
			return total, nil
		}
	}
	return total, ErrBacklog
}
