package settings

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/metrics"
)

type migrationSettings struct {
	Count int64 `json:"count"`
}

func TestPreloadMigratesOlderRowsAndPreservesFailedRowETags(t *testing.T) {
	type migrationCall struct {
		from int
		raw  string
	}
	var calls []migrationCall
	var logs bytes.Buffer
	set := metrics.NewSet()
	r, s := newTestRegistry(t, func(d *Deps) {
		d.Metrics = set
		d.Log = slog.New(slog.NewTextHandler(&logs, nil))
	})
	h := Register[migrationSettings](r, Spec{
		Feature: "migration", Version: 2,
		Groups: []GroupMeta{{ID: "basic"}},
		Fields: []Field{{Key: "count", Type: FieldInt, Group: "basic", Default: json.RawMessage(`3`)}},
		Migrate: func(from int, raw json.RawMessage) (json.RawMessage, error) {
			calls = append(calls, migrationCall{from, string(raw)})
			switch string(raw) {
			case `{"legacy_count":7}`:
				return json.RawMessage(`{"count":7}`), nil
			case `{"legacy_count":"broken"}`:
				return nil, errors.New("unreadable legacy count")
			default:
				return nil, errors.New("unexpected input to migration")
			}
		},
	})
	table := []struct {
		name, raw, etag string
		version         any
		want            int64
		bad             bool
	}{
		{"current", `{"count":9}`, `"current"`, 2, 9, false},
		{"legacy", `{"legacy_count":7}`, `"migrate"`, 1, 7, false},
		{"migration failed", `{"legacy_count":"broken"}`, `"migration-failed"`, 1, 3, true},
		{"future version", `{"count":18}`, `"future"`, 3, 3, true},
		{"bad field value", `{"count":1.0}`, `"bad-field"`, 2, 3, true},
		{"bad object", `null`, `"bad-object"`, 2, 3, true},
		{"invalid version", `{"count":8}`, `"bad-version"`, "unreadable", 3, true},
	}
	ctx := t.Context()
	if err := s.WriteWait(ctx, func(tx *sql.Tx) error {
		for i, row := range table {
			if _, err := tx.Exec(`INSERT INTO chat_settings (chat_id, feature, version, rev, data, etag, updated_at)
				VALUES (?, 'migration', ?, 7, ?, ?, 0)`, i+1, row.version, row.raw, row.etag); err != nil {
				return err
			}
		}
		_, err := tx.Exec(`INSERT INTO chat_settings (chat_id, feature, version, rev, data, etag, updated_at)
			VALUES (1, 'unregistered', 1, 1, '{}', '"ignored"', 0)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	wantCalls := []migrationCall{{1, `{"legacy_count":7}`}, {1, `{"legacy_count":"broken"}`}}
	slices.SortFunc(calls, func(a, b migrationCall) int { return strings.Compare(b.raw, a.raw) })
	if !slices.Equal(calls, wantCalls) {
		t.Errorf("migration calls = %+v, want %+v", calls, wantCalls)
	}
	if got := h.Get(0); got.Count != 3 {
		t.Errorf("Get for missing chat = %+v, want default", got)
	}
	if got := set.GetOrCreateCounter("yabot_settings_load_errors_total").Get(); got != 5 {
		t.Errorf("load errors = %d, want 5", got)
	}
	if !strings.Contains(logs.String(), "level=INFO msg=\"ignored settings for unregistered feature\"") || strings.Count(logs.String(), "level=ERROR") != 5 {
		t.Errorf("unexpected preload log output: %s", &logs)
	}
	for i, row := range table {
		t.Run(row.name, func(t *testing.T) {
			chatID := int64(i + 1)
			if got := h.Get(chatID); got.Count != row.want {
				t.Errorf("Get = %+v, want count %d", got, row.want)
			}
			result, err := h.Patch(ctx, chatID, Actor{}, nil, row.etag)
			if err != nil || result.ETag != row.etag {
				t.Fatalf("no-op Patch returned %q, %v; want %q", result.ETag, err, row.etag)
			}
			if row.bad {
				result, err := h.Patch(ctx, chatID, Actor{}, parsePatch(t, `{"count":11}`), row.etag)
				if err != nil || result.ETag == row.etag || h.Get(chatID).Count != 11 {
					t.Fatalf("overwrite returned %q, %v, Get = %+v", result.ETag, err, h.Get(chatID))
				}
				var version, rev int
				if err := s.Read().QueryRowContext(ctx, `SELECT version, rev FROM chat_settings WHERE chat_id = ? AND feature = 'migration'`, chatID).Scan(&version, &rev); err != nil || version != 2 || rev != 8 {
					t.Errorf("stored version=%d rev=%d err=%v; want 2, 8, nil", version, rev, err)
				}
			}
		})
	}
}

func TestSettingsSnapshotNeverRegresses(t *testing.T) {
	r, h, _ := newSampleRegistry(t, nil)
	for i, tc := range []struct {
		name    string
		lateRev int64
	}{
		{name: "older revision", lateRev: 1},
		{name: "equal revision", lateRev: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := rowKey{chatID: int64(i + 1), feature: "sample"}
			newDone := make(chan struct{})
			lateDone := make(chan struct{})
			latest := sampleSettings{Enabled: true, Count: 14}
			go func() {
				<-newDone
				r.publish(key, entry{value: sampleSettings{Count: 6}, etag: `"late"`, rev: tc.lateRev})
				close(lateDone)
			}()
			r.publish(key, entry{value: latest, etag: `"latest"`, rev: 2})
			if got := h.Get(key.chatID); got != latest {
				t.Errorf("Get after new publication = %+v, want %+v", got, latest)
			}
			close(newDone)
			<-lateDone
			if got := h.Get(key.chatID); got != latest {
				t.Errorf("Get after late publication = %+v, want %+v", got, latest)
			}
		})
	}
}
