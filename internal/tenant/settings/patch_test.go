package settings

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/store"
)

type sampleSettings struct {
	Enabled bool    `json:"enabled"`
	Mode    string  `json:"mode"`
	Count   int64   `json:"count"`
	Delay   Seconds `json:"delay"`
}

func sampleSpec() Spec {
	return Spec{
		Feature: "sample", PermGroup: "sample", Version: 1,
		Groups: []GroupMeta{{ID: "basic"}},
		Fields: []Field{
			{Key: "enabled", Type: FieldBool, Group: "basic", Default: json.RawMessage(`false`)},
			{Key: "mode", Type: FieldEnum, Group: "basic", Default: json.RawMessage(`"normal"`),
				Options: []Option{{Value: "normal"}, {Value: "fast"}, {Value: "off"}}},
			{Key: "count", Type: FieldInt, Group: "basic", Default: json.RawMessage(`2`), Min: new(int64(0)), Max: new(int64(20)), Step: 2,
				VisibleIf: &Cond{All: []Clause{{Field: "enabled", Eq: json.RawMessage(`true`)}, {Field: "mode", Ne: json.RawMessage(`"off"`)}}}},
			{Key: "delay", Type: FieldDuration, Group: "basic", Default: json.RawMessage(`300`), Min: new(int64(60)), Max: new(int64(1800)), Step: 60,
				VisibleIf: &Cond{Any: []Clause{{Field: "mode", In: []json.RawMessage{json.RawMessage(`"fast"`)}}, {Field: "count", Eq: json.RawMessage(`8`)}}}},
		},
	}
}

func newSampleRegistry(t *testing.T, setup func(*Deps)) (*Registry, *Handle[sampleSettings], *store.Store) {
	t.Helper()
	r, s := newTestRegistry(t, setup)
	h := Register[sampleSettings](r, sampleSpec())
	if err := r.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	return r, h, s
}

func parsePatch(t *testing.T, raw string) map[string]json.RawMessage {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		t.Fatal(err)
	}
	return fields
}

func patchSample(t *testing.T, h *Handle[sampleSettings], chatID int64, raw, etag string) PatchResult {
	t.Helper()
	res, err := h.Patch(t.Context(), chatID, Actor{}, parsePatch(t, raw), etag)
	if err != nil {
		t.Fatalf("patch sample: %v", err)
	}
	return res
}

func storedSample(t *testing.T, s *store.Store, chatID int64) sampleSettings {
	t.Helper()
	var raw string
	if err := s.Read().QueryRowContext(t.Context(), `SELECT data FROM chat_settings WHERE chat_id = ? AND feature = 'sample'`, chatID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var values sampleSettings
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		t.Fatal(err)
	}
	return values
}

func countRows(t *testing.T, s *store.Store, query string, args ...any) int {
	t.Helper()
	var count int
	if err := s.Read().QueryRowContext(t.Context(), query, args...).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func requireFieldCodes(t *testing.T, result PatchResult, want map[string]FieldCode) {
	t.Helper()
	if len(result.Fields) != len(want) {
		t.Fatalf("fields = %+v, want codes %v", result.Fields, want)
	}
	for key, code := range want {
		if result.Fields[key].Code != code {
			t.Errorf("code for %s = %q, want %q", key, result.Fields[key].Code, code)
		}
	}
}

func TestPatchVisibilityUsesAllAcceptedInputsOnce(t *testing.T) {
	_, h, s := newSampleRegistry(t, nil)
	for i, tc := range []struct {
		name, seed, patch string
		want              sampleSettings
	}{
		{"hide invalid value", `{"enabled":true,"count":6}`, `{"enabled":false,"count":"bad"}`, sampleSettings{false, "normal", 6, 300}},
		{"hide valid value", `{"enabled":true,"count":6}`, `{"enabled":false,"count":8}`, sampleSettings{false, "normal", 6, 300}},
		{"open and set in one patch", "", `{"enabled":true,"count":8,"delay":420}`, sampleSettings{true, "normal", 8, 420}},
		{"hide if enum equals excluded value", `{"enabled":true,"count":6}`, `{"mode":"off","count":4}`, sampleSettings{true, "off", 6, 300}},
		{"show when enum is in set", "", `{"mode":"fast","delay":60}`, sampleSettings{false, "fast", 2, 60}},
		{"candidate includes hidden valid inputs", "", `{"count":8,"delay":120}`, sampleSettings{false, "normal", 2, 120}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chatID := int64(i + 1)
			etag := defaultETag
			if tc.seed != "" {
				etag = patchSample(t, h, chatID, tc.seed, etag).ETag
			}
			patch := parsePatch(t, tc.patch)
			res, err := h.Patch(t.Context(), chatID, Actor{}, patch, etag)
			if err != nil {
				t.Fatalf("Patch = %v (%+v)", err, res.Fields)
			}
			wantCodes := make(map[string]FieldCode, len(patch))
			for key := range patch {
				wantCodes[key] = CodeOK
			}
			requireFieldCodes(t, res, wantCodes)
			if got := h.Get(chatID); got != tc.want {
				t.Errorf("snapshot = %+v, want %+v", got, tc.want)
			}
			if got := storedSample(t, s, chatID); got != tc.want {
				t.Errorf("stored = %+v, want %+v", got, tc.want)
			}
			if raw := string(res.Values["delay"]); raw != fmt.Sprint(tc.want.Delay) || tc.want.Delay.Duration() != time.Duration(tc.want.Delay)*time.Second {
				t.Errorf("delay = %s seconds, want %d", raw, tc.want.Delay)
			}
		})
	}
}

func TestInvalidPatchDoesNotWriteAnyFields(t *testing.T) {
	_, h, s := newSampleRegistry(t, nil)
	for i, seed := range []struct {
		name string
		raw  string
	}{{"no row", ""}, {"existing row", `{"enabled":true,"count":4}`}} {
		t.Run(seed.name, func(t *testing.T) {
			chatID := int64(i + 1)
			etag := defaultETag
			rowCount := 0
			if seed.raw != "" {
				etag = patchSample(t, h, chatID, seed.raw, etag).ETag
				rowCount = 1
			}
			before := h.Get(chatID)
			res, err := h.Patch(t.Context(), chatID, Actor{}, parsePatch(t, `{"enabled":true,"mode":"fast","count":5,"delay":1e3,"missing":1}`), etag)
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("Patch = %v, want ErrInvalid", err)
			}
			requireFieldCodes(t, res, map[string]FieldCode{
				"enabled": CodeOK, "mode": CodeOK, "count": CodeInvalidValue, "delay": CodeInvalidValue, "missing": CodeUnknownKey,
			})
			if got := h.Get(chatID); got != before {
				t.Errorf("snapshot = %+v, want %+v", got, before)
			}
			if got := countRows(t, s, `SELECT count(*) FROM settings_audit WHERE chat_id = ?`, chatID); got != rowCount {
				t.Errorf("audit rows = %d, want %d", got, rowCount)
			}
			if got := countRows(t, s, `SELECT count(*) FROM chat_settings WHERE chat_id = ?`, chatID); got != rowCount {
				t.Errorf("settings rows = %d, want %d", got, rowCount)
			}
			if rowCount == 1 && storedSample(t, s, chatID) != before {
				t.Error("invalid patch changed stored values")
			}
		})
	}
}

func TestPatchRejectsCrossFieldErrorOnHiddenField(t *testing.T) {
	r, s := newTestRegistry(t, nil)
	spec := sampleSpec()
	spec.Validate = func(v any) []FieldError {
		cfg := v.(*sampleSettings)
		if cfg.Mode == "fast" && cfg.Count < 4 {
			return []FieldError{{Key: "count"}}
		}
		return nil
	}
	h := Register[sampleSettings](r, spec)
	r.Freeze()
	initial := patchSample(t, h, 1, `{"mode":"off"}`, defaultETag)
	before := h.Get(1)
	result, err := h.Patch(t.Context(), 1, Actor{}, parsePatch(t, `{"mode":"fast"}`), initial.ETag)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Patch = %v, want ErrInvalid", err)
	}
	requireFieldCodes(t, result, map[string]FieldCode{"mode": CodeOK, "count": CodeInvalidValue})
	if h.Get(1) != before || storedSample(t, s, 1) != before || storedETag(t, s, 1) != initial.ETag ||
		r.Render(1, "en").Specs[0].ETag != initial.ETag {
		t.Error("invalid patch changed settings or their etag")
	}
	if got := countRows(t, s, `SELECT count(*) FROM settings_audit`); got != 1 {
		t.Errorf("audit rows = %d, want 1", got)
	}

	// The hidden count input must not reach Validate: it checks the values that would be saved.
	result, err = h.Patch(t.Context(), 1, Actor{}, parsePatch(t, `{"mode":"fast","count":8}`), initial.ETag)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Patch with hidden count = %v, want ErrInvalid", err)
	}
	requireFieldCodes(t, result, map[string]FieldCode{"mode": CodeOK, "count": CodeInvalidValue})
	if h.Get(1) != before || storedSample(t, s, 1) != before {
		t.Error("patch validated against a hidden input changed settings")
	}
	if got := countRows(t, s, `SELECT count(*) FROM settings_audit`); got != 1 {
		t.Errorf("audit rows = %d, want 1", got)
	}
}

func TestPatchChecksExactETagIncludingNoOp(t *testing.T) {
	_, h, s := newSampleRegistry(t, nil)
	ctx := t.Context()
	created := patchSample(t, h, 2, `{"enabled":true}`, defaultETag)
	if !regexp.MustCompile(`^"[0-9a-f]{32}"$`).MatchString(created.ETag) {
		t.Fatalf("generated etag = %q", created.ETag)
	}
	for _, tc := range []struct {
		name       string
		chatID     int64
		etag       string
		wantErr    error
		wantETag   string
		wantAudits int
	}{
		{"empty row wrong etag", 1, created.ETag, ErrConflict, "", 0},
		{"empty row default etag", 1, defaultETag, nil, defaultETag, 0},
		{"explicit wildcard", 2, "*", ErrConflict, "", 1},
		{"existing row default etag", 2, defaultETag, ErrConflict, "", 1},
		{"wrong etag", 2, `"ffffffffffffffffffffffffffffffff"`, ErrConflict, "", 1},
		{"weak etag", 2, "W/" + created.ETag, ErrConflict, "", 1},
		{"suffix", 2, created.ETag + ", other", ErrConflict, "", 1},
		{"prefix", 2, strings.TrimSuffix(created.ETag, `"`), ErrConflict, "", 1},
		{"surrounding whitespace", 2, " \t" + created.ETag + " \n", nil, created.ETag, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := h.Patch(ctx, tc.chatID, Actor{}, nil, tc.etag)
			if !errors.Is(err, tc.wantErr) || res.ETag != tc.wantETag {
				t.Errorf("Patch = %q, %v; want %q, %v", res.ETag, err, tc.wantETag, tc.wantErr)
			}
			if tc.wantErr == nil {
				want := h.registry.Render(tc.chatID, "en").Specs[0].Values
				if len(res.Values) == 0 || !reflect.DeepEqual(res.Values, want) {
					t.Errorf("Patch values = %s, want every current value %s", res.Values, want)
				}
			}
			if got := countRows(t, s, `SELECT count(*) FROM settings_audit WHERE chat_id = ?`, tc.chatID); got != tc.wantAudits {
				t.Errorf("audit rows = %d, want %d", got, tc.wantAudits)
			}
		})
	}
}

func TestPatchActorAndAuditDiff(t *testing.T) {
	now := time.Unix(560, 234)
	_, h, s := newSampleRegistry(t, func(d *Deps) { d.Now = func() time.Time { return now } })
	etag := defaultETag
	for _, tc := range []struct {
		name, patch, diff string
		actor             Actor
		wantActor         sql.NullInt64
	}{
		{"public", `{"enabled":true}`, `{"enabled":{"old":false,"new":true}}`, Actor{UserID: 42, Public: true}, sql.NullInt64{Int64: 42, Valid: true}},
		{"private", `{"enabled":true,"mode":"fast","delay":720}`, `{"delay":{"old":300,"new":720},"mode":{"old":"normal","new":"fast"}}`, Actor{UserID: 73}, sql.NullInt64{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := h.Patch(t.Context(), 1, tc.actor, parsePatch(t, tc.patch), etag)
			if err != nil {
				t.Fatal(err)
			}
			if result.ETag == etag {
				t.Fatal("changed patch reused etag")
			}
			etag = result.ETag
			var updatedBy, actor sql.NullInt64
			var updatedAt, createdAt int64
			var diff string
			if err := s.Read().QueryRowContext(t.Context(), `SELECT updated_by, updated_at FROM chat_settings WHERE chat_id = 1`).Scan(&updatedBy, &updatedAt); err != nil {
				t.Fatal(err)
			}
			if err := s.Read().QueryRowContext(t.Context(), `SELECT actor, diff, created_at FROM settings_audit WHERE chat_id = 1 ORDER BY id DESC LIMIT 1`).Scan(&actor, &diff, &createdAt); err != nil {
				t.Fatal(err)
			}
			if updatedBy != tc.wantActor || actor != tc.wantActor || diff != tc.diff || updatedAt != now.Unix() || createdAt != now.Unix() {
				t.Errorf("stored updated_by=%v actor=%v diff=%s updated_at=%d created_at=%d", updatedBy, actor, diff, updatedAt, createdAt)
			}
		})
	}
}

func TestPatchRollsBackIfAuditWriteFails(t *testing.T) {
	_, h, s := newSampleRegistry(t, nil)
	ctx := t.Context()
	if err := s.WriteWait(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(`CREATE TRIGGER fail_audit BEFORE INSERT ON settings_audit BEGIN SELECT RAISE(ABORT, 'audit failed'); END`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	before := h.Get(1)
	_, err := h.Patch(ctx, 1, Actor{}, parsePatch(t, `{"enabled":true}`), defaultETag)
	var opErr *store.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("Patch = %v, want a store operation failure", err)
	}
	if h.Get(1) != before || countRows(t, s, `SELECT count(*) FROM chat_settings`) != 0 || countRows(t, s, `SELECT count(*) FROM settings_audit`) != 0 {
		t.Fatal("failed audit left a partial write")
	}
}

func TestConcurrentPatchesSharingETagCommitOnlyOnce(t *testing.T) {
	for _, tc := range []struct {
		name          string
		initialAudits uint64
	}{
		{name: "insert"},
		{name: "update", initialAudits: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pause, reached, resume := pauseWrite()
				defer resume()
				_, h, s := newSampleRegistry(t, interceptWrite(tc.initialAudits+1, func(st Store, op store.Op, done func(error)) error {
					pause()
					return st.Write(op, done)
				}))
				etag := defaultETag
				if tc.initialAudits != 0 {
					etag = patchSample(t, h, 1, `{"enabled":true}`, etag).ETag
				}
				off := parsePatch(t, `{"mode":"off"}`)
				late := make(chan error, 1)
				go func() {
					_, err := h.Patch(t.Context(), 1, Actor{}, off, etag)
					late <- err
				}()
				<-reached
				fast := parsePatch(t, `{"mode":"fast"}`)
				committed := make(chan error, 1)
				go func() {
					_, err := h.Patch(t.Context(), 1, Actor{}, fast, etag)
					committed <- err
				}()
				synctest.Wait()
				select {
				case err := <-committed:
					if err != nil {
						t.Fatal(err)
					}
				default:
					t.Fatal("a Patch that has yet to submit blocked another Patch")
				}
				resume()
				if err := <-late; !errors.Is(err, ErrConflict) {
					t.Fatalf("second commit = %v, want ErrConflict", err)
				}
				if got := countRows(t, s, `SELECT count(*) FROM settings_audit WHERE chat_id = 1`); got != int(tc.initialAudits)+1 {
					t.Errorf("audit rows = %d, want one additional row", got)
				}
				if got := h.Get(1); got != storedSample(t, s, 1) || got.Mode != "fast" {
					t.Errorf("snapshot = %+v, want the first committed patch", got)
				}
			})
		})
	}
}

func TestRealStoreConcurrentPatchesAndSnapshots(t *testing.T) {
	r, h, s := newSampleRegistry(t, nil)
	const writers, writes = 6, 8
	start := make(chan struct{})
	var group sync.WaitGroup
	for writer := range writers {
		group.Go(func() {
			<-start
			chatID := int64(1 + writer)
			etag := defaultETag
			for i := range writes {
				patch := map[string]json.RawMessage{"enabled": json.RawMessage(`true`), "count": fmt.Append(nil, 2*(i+2))}
				result, err := r.Patch(t.Context(), "sample", chatID, Actor{}, patch, etag)
				if err != nil {
					t.Errorf("Patch(%d) = %v", chatID, err)
					return
				}
				etag = result.ETag
				if got := h.Get(chatID); got.Count != int64(2*(i+2)) || !got.Enabled {
					t.Errorf("Get(%d) after Patch = %+v", chatID, got)
				}
				other := h.Get(int64(1 + (writer+1)%writers))
				if other.Enabled != (other.Count > 2) {
					t.Errorf("read inconsistent snapshot %+v", other)
				}
			}
		})
	}
	close(start)
	group.Wait()
	if got := countRows(t, s, `SELECT count(*) FROM settings_audit`); got != writers*writes {
		t.Errorf("audit rows = %d, want %d", got, writers*writes)
	}
}

func TestPatchCanceledBeforeSubmission(t *testing.T) {
	_, h, s := newSampleRegistry(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := h.Patch(ctx, 1, Actor{}, parsePatch(t, `{"enabled":true}`), defaultETag)
	if !errors.Is(err, context.Canceled) || countRows(t, s, `SELECT count(*) FROM chat_settings`) != 0 {
		t.Fatalf("canceled Patch = %v, want cancellation with no write", err)
	}
}
