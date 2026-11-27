package settings

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/janitor"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/INKCR0W/yabot/internal/tenant/contract"
)

func TestDeleteUserAnonymizesCommittedBatches(t *testing.T) {
	watched := &privacyBatchStore{}
	r, s := newTestRegistry(t, func(d *Deps) {
		watched.Store = d.Store
		d.Store = watched
	})
	r.Freeze()
	privacyExec(t, s, `WITH RECURSIVE ids(id) AS (VALUES (1) UNION ALL SELECT id + 1 FROM ids WHERE id < 511)
		INSERT INTO chat_settings (chat_id, feature, version, rev, data, etag, updated_by, updated_at)
		SELECT id, 'example', 1, 1, '{}', 'etag', 42, 0 FROM ids`)
	privacyExec(t, s, `INSERT INTO chat_settings VALUES (600, 'example', 1, 1, '{}', 'etag', 43, 0)`)
	seedPrivacyAudit(t, s, 1, 42, 0, 510)
	seedPrivacyAudit(t, s, 2, 43, 0, 1)

	const settingsCount = `SELECT count(*) FROM chat_settings WHERE updated_by = ?`
	const auditCount = `SELECT count(*) FROM settings_audit WHERE actor = ?`
	remaining := 1021
	var batches []int
	watched.afterCommit = func() {
		current := privacyCount(t, s, settingsCount, 42) + privacyCount(t, s, auditCount, 42)
		batches = append(batches, remaining-current)
		remaining = current
	}
	if err := r.DeleteUser(t.Context(), 42); err != nil {
		t.Fatal(err)
	}
	if len(batches) < 3 || remaining != 0 {
		t.Errorf("committed batch sizes = %v, with %d rows remaining", batches, remaining)
	}
	for _, size := range batches {
		if size > 500 {
			t.Errorf("committed batch has %d rows, want at most 500", size)
		}
	}
	if got := privacyCount(t, s, `SELECT count(*) FROM chat_settings WHERE updated_by IS NULL`); got != 511 {
		t.Errorf("anonymous settings rows = %d, want 511", got)
	}
	if got := privacyCount(t, s, `SELECT count(*) FROM settings_audit WHERE actor IS NULL`); got != 510 {
		t.Errorf("anonymous audit rows = %d, want 510", got)
	}
	if counts := [2]int{privacyCount(t, s, settingsCount, 43), privacyCount(t, s, auditCount, 43)}; counts != [2]int{1, 1} {
		t.Errorf("unrelated user's rows = %v, want [1 1]", counts)
	}
}

func TestPurgeChatChecksEveryBatchAndClearsCommittedSettings(t *testing.T) {
	for _, tc := range []struct {
		name         string
		abortAt      int
		cancelAt     int
		wantChecks   int
		wantSettings bool
		wantAudit    int
		wantErr      error
	}{
		{name: "complete", wantChecks: 3},
		{name: "abort before first batch", abortAt: 1, wantChecks: 1, wantSettings: true, wantAudit: 1002, wantErr: contract.ErrAborted},
		{name: "abort before second batch", abortAt: 2, wantChecks: 2, wantAudit: 503, wantErr: contract.ErrAborted},
		{name: "cancel inside first batch", cancelAt: 1, wantChecks: 1, wantAudit: 503, wantErr: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type privacySettings struct {
				Enabled bool `json:"enabled"`
			}
			r, s := newTestRegistry(t, nil)
			h := Register[privacySettings](r, Spec{
				Feature: "privacy_test", Version: 1,
				Groups: []GroupMeta{{ID: "general"}},
				Fields: []Field{{Key: "enabled", Type: FieldBool, Group: "general", Default: json.RawMessage("false")}},
			})
			r.Freeze()
			for _, chatID := range []int64{1, 2} {
				_, err := h.Patch(t.Context(), chatID, Actor{}, map[string]json.RawMessage{"enabled": json.RawMessage("true")}, `"0"`)
				if err != nil {
					t.Fatal(err)
				}
			}
			seedPrivacyAudit(t, s, 1, 42, 0, 1001)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			checks := 0
			err := r.PurgeChat(ctx, 1, func() bool {
				checks++
				if checks == tc.cancelAt {
					cancel()
				}
				return checks != tc.abortAt
			})
			if !errors.Is(err, tc.wantErr) || checks != tc.wantChecks {
				t.Errorf("PurgeChat = %v with %d validity checks, want %v with %d", err, checks, tc.wantErr, tc.wantChecks)
			}
			if got := h.Get(1).Enabled; got != tc.wantSettings {
				t.Errorf("cached setting = %v, want %v", got, tc.wantSettings)
			}
			if exists := privacyCount(t, s, `SELECT count(*) FROM chat_settings WHERE chat_id = 1`) != 0; exists != tc.wantSettings {
				t.Errorf("stored setting exists = %v, want %v", exists, tc.wantSettings)
			}
			if got := privacyCount(t, s, `SELECT count(*) FROM settings_audit WHERE chat_id = 1`); got != tc.wantAudit {
				t.Errorf("audit rows = %d, want %d", got, tc.wantAudit)
			}
			if !h.Get(2).Enabled || privacyCount(t, s, `SELECT count(*) FROM settings_audit WHERE chat_id = 2`) != 1 {
				t.Error("unrelated chat lost its settings or audit")
			}
			if !tc.wantSettings {
				result, err := h.Patch(t.Context(), 1, Actor{}, map[string]json.RawMessage{"enabled": json.RawMessage("false")}, `"0"`)
				if err != nil || result.ETag != `"0"` {
					t.Errorf("patch after purge = etag %q, error %v, want default etag and success", result.ETag, err)
				}
			}
		})
	}
}

func TestSweepAuditRespectsCutoffAndBatchLimit(t *testing.T) {
	for _, tc := range []struct {
		name     string
		old      int
		wantRows int
		wantErr  error
	}{
		{name: "expires only older rows", old: 17, wantRows: 17},
		{name: "signals backlog", old: 250, wantRows: janitor.SweepRows, wantErr: janitor.ErrBacklog},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				now := time.Unix(1000000, 0)
				retention := 24 * time.Hour
				cutoff := now.Add(-retention).Unix()
				r, s := newTestRegistry(t, func(d *Deps) { d.Now = func() time.Time { return now } })
				seedPrivacyAudit(t, s, 1, 42, cutoff-1, tc.old)
				seedPrivacyAudit(t, s, 1, 42, cutoff, 1)
				seedPrivacyAudit(t, s, 1, 42, cutoff+1, 1)
				n, err := r.sweepAudit(t.Context(), retention)
				if n != tc.wantRows || err != tc.wantErr {
					t.Errorf("sweepAudit = (%d, %v), want (%d, %v)", n, err, tc.wantRows, tc.wantErr)
				}
				if got := privacyCount(t, s, `SELECT count(*) FROM settings_audit WHERE created_at < ?`, cutoff); got != tc.old-tc.wantRows {
					t.Errorf("expired rows remaining = %d, want %d", got, tc.old-tc.wantRows)
				}
				if got := privacyCount(t, s, `SELECT count(*) FROM settings_audit WHERE created_at >= ?`, cutoff); got != 2 {
					t.Errorf("retained rows = %d, want 2", got)
				}
			})
		})
	}
}

func TestPrivacyQueriesHaveIndexes(t *testing.T) {
	_, s := newTestRegistry(t, nil)
	for _, tc := range []struct{ table, column string }{
		{"chat_settings", "chat_id"},
		{"chat_settings", "updated_by"},
		{"settings_audit", "chat_id"},
		{"settings_audit", "actor"},
		{"settings_audit", "created_at"},
	} {
		t.Run(tc.table+"."+tc.column, func(t *testing.T) {
			if n := privacyCount(t, s, `SELECT count(*) FROM pragma_index_list(?) AS indexes
				JOIN pragma_index_info(indexes.name) AS columns ON columns.seqno = 0 WHERE columns.name = ?`, tc.table, tc.column); n == 0 {
				t.Errorf("no index starts with %s.%s", tc.table, tc.column)
			}
		})
	}
}

type privacyBatchStore struct {
	Store
	afterCommit func()
}

func (s *privacyBatchStore) WriteWait(ctx context.Context, op store.Op) error {
	err := s.Store.WriteWait(ctx, op)
	if err == nil && s.afterCommit != nil {
		s.afterCommit()
	}
	return err
}

func seedPrivacyAudit(t *testing.T, s *store.Store, chatID, userID, createdAt int64, count int) {
	t.Helper()
	privacyExec(t, s, `WITH RECURSIVE ids(id) AS (VALUES (1) UNION ALL SELECT id + 1 FROM ids WHERE id < ?)
		INSERT INTO settings_audit (chat_id, feature, actor, diff, created_at) SELECT ?, 'example', ?, '{}', ? FROM ids`, count, chatID, userID, createdAt)
}

func privacyExec(t *testing.T, s *store.Store, query string, args ...any) {
	t.Helper()
	if err := s.WriteWait(t.Context(), func(tx *sql.Tx) error {
		_, err := tx.Exec(query, args...)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func privacyCount(t *testing.T, s *store.Store, query string, args ...any) int {
	t.Helper()
	var n int
	if err := s.Read().QueryRowContext(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
