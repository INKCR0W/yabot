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

type Writer interface {
	WriteWait(ctx context.Context, op store.Op) error
}

// Sweep runs del in separate write operations. Each del must bound its SQL with the supplied LIMIT and return the deleted row count.
// The time budget controls submission of the next batch. A submitted batch can finish after the budget or cancellation, and only confirmed commits are counted.
func Sweep(ctx context.Context, w Writer, del func(tx *sql.Tx, limit int) (int, error)) (int, error) {
	start := time.Now()
	total := 0
	for total < SweepRows {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		if time.Since(start) >= SweepBudget {
			break
		}
		limit := SweepRows - total
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
		if rows == 0 {
			break
		}
		total += rows
	}
	return total, nil
}
