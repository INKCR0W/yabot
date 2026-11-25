package ingress

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/INKCR0W/yabot/internal/platform/store"
)

func Migrations() []store.Migration {
	return []store.Migration{{
		Component: "ingress",
		Version:   1,
		SQL: `CREATE TABLE dead_letter (
	update_id INTEGER PRIMARY KEY,
	kind TEXT NOT NULL,
	error TEXT NOT NULL,
	created_at INTEGER NOT NULL
);
CREATE INDEX dead_letter_created_at ON dead_letter (created_at);`,
	}}
}

func (i *Ingress) writeDeadLetter(ctx context.Context, id int64, kind, summary string) error {
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), i.deps.SettleTimeout)
	defer cancel()
	createdAt := i.deps.Now().UnixMilli()
	return i.deps.Store.WriteWait(writeCtx, func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO dead_letter (update_id, kind, error, created_at) VALUES (?, ?, ?, ?)
ON CONFLICT (update_id) DO UPDATE SET kind = excluded.kind, error = excluded.error, created_at = excluded.created_at`,
			id, kind, summary, createdAt)
		return err
	})
}

func errorSummary(err error) string {
	const maxSummaryBytes = 512
	summary := strings.ToValidUTF8(err.Error(), "\uFFFD")
	if len(summary) <= maxSummaryBytes {
		return summary
	}
	end := maxSummaryBytes
	for !utf8.RuneStart(summary[end]) {
		end--
	}
	return summary[:end]
}

// DeadLetterSweeper deletes up to limit expired rows per call. The caller owns the transaction.
func DeadLetterSweeper(retention time.Duration, now func() time.Time) func(tx *sql.Tx, limit int) (int, error) {
	if retention <= 0 || now == nil {
		panic("ingress: dead letter sweeper requires positive retention and a clock")
	}
	return func(tx *sql.Tx, limit int) (int, error) {
		if limit < 0 {
			return 0, errors.New("ingress: dead letter sweep limit must not be negative")
		}
		result, err := tx.Exec(`DELETE FROM dead_letter WHERE rowid IN (
	SELECT rowid FROM dead_letter WHERE created_at < ? ORDER BY created_at LIMIT ?
)`, now().Add(-retention).UnixMilli(), limit)
		if err != nil {
			return 0, err
		}
		rows, err := result.RowsAffected()
		return int(rows), err
	}
}
