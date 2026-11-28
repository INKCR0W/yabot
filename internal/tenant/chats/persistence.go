package chats

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"

	"github.com/INKCR0W/yabot/internal/platform/store"
)

//go:embed schema.sql
var schemaSQL string

func Migrations() []store.Migration {
	return []store.Migration{{Component: "chats", Version: 1, SQL: schemaSQL}}
}

const chatColumns = `chat_id, type, title, state, bot_rights, guard_assigned, owner_id, added_at,
updated_at, ever_active, removed_at, removed_reason, migrated_to, purge_after, member_event_date, member_event_update_id`

const upsertChat = `INSERT INTO chats (` + chatColumns + `)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(chat_id) DO UPDATE SET
    type = excluded.type,
    title = excluded.title,
    state = excluded.state,
    bot_rights = excluded.bot_rights,
    guard_assigned = excluded.guard_assigned,
    owner_id = excluded.owner_id,
    added_at = excluded.added_at,
    updated_at = excluded.updated_at,
    ever_active = excluded.ever_active,
    removed_at = excluded.removed_at,
    removed_reason = excluded.removed_reason,
    migrated_to = excluded.migrated_to,
    purge_after = excluded.purge_after,
    member_event_date = excluded.member_event_date,
    member_event_update_id = excluded.member_event_update_id`

func writeRecord(record Record) store.Op {
	return func(tx *sql.Tx) error {
		_, err := tx.Exec(upsertChat, record.ChatID, record.Type, record.Title, record.State, record.BotRights, record.GuardAssigned,
			optionalChatValue(record.OwnerID), optionalChatValue(record.AddedAt), record.UpdatedAt,
			record.EverActive, optionalChatValue(record.RemovedAt), optionalChatValue(record.RemovedReason), optionalChatValue(record.MigratedTo),
			optionalChatValue(record.PurgeAfter), record.Watermark.Date, record.Watermark.ID)
		return err
	}
}

func optionalChatValue[T comparable](v T) any {
	var zero T
	if v == zero {
		return nil
	}
	return v
}

func deleteRecord(chatID int64) store.Op {
	return func(tx *sql.Tx) error {
		_, err := tx.Exec(`DELETE FROM chats WHERE chat_id = ?`, chatID)
		return err
	}
}

func readRecords(ctx context.Context, db *sql.DB, onRecord func(Record) error) (err error) {
	rows, err := db.QueryContext(ctx, `SELECT `+chatColumns+` FROM chats`)
	if err != nil {
		return fmt.Errorf("query chats for preload: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close chats preload rows: %w", closeErr))
		}
	}()
	for rows.Next() {
		record, err := scanChatRecord(rows)
		if err != nil {
			return err
		}
		if err := onRecord(record); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read chats for preload: %w", err)
	}
	return nil
}

func scanChatRecord(rows *sql.Rows) (Record, error) {
	var record Record
	var ownerID, addedAt, removedAt, migratedTo, purgeAfter sql.NullInt64
	var removedReason sql.NullString
	err := rows.Scan(&record.ChatID, &record.Type, &record.Title, &record.State, &record.BotRights, &record.GuardAssigned,
		&ownerID, &addedAt, &record.UpdatedAt, &record.EverActive, &removedAt, &removedReason, &migratedTo,
		&purgeAfter, &record.Watermark.Date, &record.Watermark.ID)
	if err != nil {
		// Scan errors may include the raw column value.
		return Record{}, fmt.Errorf("decode stored chat record %d", record.ChatID)
	}
	if !record.State.isPresent() && record.State != Removed {
		return Record{}, fmt.Errorf("invalid state for stored chat record %d", record.ChatID)
	}
	record.OwnerID, record.AddedAt = ownerID.Int64, addedAt.Int64
	record.RemovedAt, record.RemovedReason = removedAt.Int64, RemovedReason(removedReason.String)
	record.MigratedTo, record.PurgeAfter = migratedTo.Int64, purgeAfter.Int64
	return record, nil
}
