package settings

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/janitor"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/INKCR0W/yabot/internal/tenant/contract"
)

const privacyBatchRows = 500

var (
	_ contract.UserEraser = (*Registry)(nil)
	_ contract.ChatPurger = (*Registry)(nil)
)

func (r *Registry) DeleteUser(ctx context.Context, userID int64) error {
	r.requireFrozen()
	if err := ctx.Err(); err != nil {
		return err
	}
	bucket := r.deletionBucket(userID)
	if !bucket.begin() {
		if r.metrics.deleteBusy != nil {
			r.metrics.deleteBusy.Inc()
		}
		if r.log != nil {
			r.log.InfoContext(ctx, "settings user erasure concurrency limit reached")
		}
		return fmt.Errorf("erase settings user data: %w", store.ErrBusy)
	}
	defer bucket.finish()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var deleted int
		err := r.store.WriteWait(context.WithoutCancel(ctx), func(tx *sql.Tx) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			settingsRows, err := affectedRows(tx.Exec(`UPDATE chat_settings SET updated_by = NULL
				WHERE rowid IN (SELECT rowid FROM chat_settings WHERE updated_by = ? LIMIT ?)`, userID, privacyBatchRows))
			if err != nil {
				return err
			}
			deleted = settingsRows
			if deleted == privacyBatchRows {
				return nil
			}
			auditRows, err := affectedRows(tx.Exec(`UPDATE settings_audit SET actor = NULL
				WHERE id IN (SELECT id FROM settings_audit WHERE actor = ? LIMIT ?)`, userID, privacyBatchRows-deleted))
			deleted += auditRows
			return err
		})
		if err != nil {
			return fmt.Errorf("erase settings user data: %w", err)
		}
		if deleted < privacyBatchRows {
			return nil
		}
	}
}

// PurgeChat calls valid inside each batch's transaction on the store writer goroutine,
// so valid must return quickly and must never wait for a store write.
func (r *Registry) PurgeChat(ctx context.Context, chatID int64, valid func() bool) error {
	r.requireFrozen()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		deleted, err := r.purgeChatBatch(ctx, chatID, valid)
		if errors.Is(err, contract.ErrAborted) {
			return contract.ErrAborted
		}
		if err != nil {
			return fmt.Errorf("purge chat settings: %w", err)
		}
		if deleted < privacyBatchRows {
			return nil
		}
	}
}

func (r *Registry) purgeChatBatch(ctx context.Context, chatID int64, valid func() bool) (int, error) {
	var features []string
	var auditRows int
	done := make(chan error, 1)
	err := r.store.Write(func(tx *sql.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !valid() {
			return contract.ErrAborted
		}
		var err error
		features, err = deleteChatSettings(tx, chatID)
		if err != nil || len(features) == privacyBatchRows {
			return err
		}
		auditRows, err = affectedRows(tx.Exec(`DELETE FROM settings_audit
			WHERE id IN (SELECT id FROM settings_audit WHERE chat_id = ? LIMIT ?)`, chatID, privacyBatchRows-len(features)))
		return err
	}, func(err error) {
		if err == nil {
			r.forgetFeatures(chatID, features)
		}
		done <- err
	})
	if err != nil {
		return 0, err
	}
	// A submitted batch can commit after cancellation. Wait for its eviction before returning.
	if err := <-done; err != nil {
		return 0, err
	}
	return len(features) + auditRows, nil
}

func deleteChatSettings(tx *sql.Tx, chatID int64) (features []string, err error) {
	rows, err := tx.Query(`DELETE FROM chat_settings
		WHERE rowid IN (SELECT rowid FROM chat_settings WHERE chat_id = ? LIMIT ?) RETURNING feature`, chatID, privacyBatchRows)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	for rows.Next() {
		var feature string
		if err := rows.Scan(&feature); err != nil {
			return nil, err
		}
		features = append(features, feature)
	}
	return features, rows.Err()
}

func (r *Registry) sweepAudit(ctx context.Context, retention time.Duration) (int, error) {
	cutoff := r.now().Add(-retention).Unix()
	return janitor.Sweep(ctx, r.store, func(tx *sql.Tx, limit int) (int, error) {
		return affectedRows(tx.Exec(`DELETE FROM settings_audit
			WHERE id IN (SELECT id FROM settings_audit WHERE created_at < ? LIMIT ?)`, cutoff, limit))
	})
}

func affectedRows(result sql.Result, err error) (int, error) {
	if err != nil {
		return 0, err
	}
	rows, err := result.RowsAffected()
	return int(rows), err
}
