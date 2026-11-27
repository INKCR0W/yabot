package settings

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/VictoriaMetrics/metrics"
)

func TestCanceledPatchReportsWriterResultOnce(t *testing.T) {
	for _, tc := range []struct {
		name, setup, outcome string
		log                  string
		wantAudits           int
	}{
		{name: "committed", outcome: "ok", wantAudits: 1},
		{
			name: "conflict", outcome: "conflict", log: `level=INFO msg="settings patch rejected"`,
			setup: `INSERT INTO chat_settings (chat_id, feature, version, rev, data, etag, updated_at)
				VALUES (1, 'sample', 1, 1, '{}', '"already-written"', 0)`,
		},
		{
			name: "operation error", outcome: "error", log: `level=ERROR msg="settings patch failed"`,
			setup: `CREATE TRIGGER fail_audit BEFORE INSERT ON settings_audit BEGIN SELECT RAISE(ABORT, 'audit failed'); END`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pause, opRan, resume := pauseWrite()
				defer resume()
				writerResult := make(chan error, 1)
				var logs bytes.Buffer
				set := metrics.NewSet()
				_, h, s := newSampleRegistry(t, func(d *Deps) {
					d.Log = slog.New(slog.NewTextHandler(&logs, nil))
					d.Metrics = set
					interceptWrite(1, func(st Store, op store.Op, done func(error)) error {
						return st.Write(afterOp(op, pause), func(err error) {
							done(err)
							writerResult <- err
						})
					})(d)
				})
				if tc.setup != "" {
					privacyExec(t, s, tc.setup)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				patch := parsePatch(t, `{"enabled":true}`)
				callerResult := make(chan error, 1)
				go func() {
					_, err := h.Patch(ctx, 1, Actor{}, patch, defaultETag)
					callerResult <- err
				}()
				<-opRan
				cancel()
				synctest.Wait()
				select {
				case err := <-callerResult:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("Patch = %v, want context.Canceled", err)
					}
				default:
					t.Fatal("Patch was still waiting for the writer")
				}

				resume()
				err := <-writerResult
				var opErr *store.OpError
				switch tc.outcome {
				case "ok":
					if err != nil || !h.Get(1).Enabled {
						t.Fatalf("writer result = %v, snapshot = %+v, want committed update", err, h.Get(1))
					}
				case "conflict":
					if !errors.Is(err, ErrConflict) {
						t.Fatalf("writer result = %v, want ErrConflict", err)
					}
				case "error":
					if !errors.As(err, &opErr) || !strings.Contains(err.Error(), "audit failed") {
						t.Fatalf("writer result = %v, want audit OpError", err)
					}
				}
				synctest.Wait()
				for _, outcome := range []string{"ok", "invalid", "conflict", "error"} {
					var want uint64
					if outcome == tc.outcome {
						want = 1
					}
					if got := set.GetOrCreateCounter(`yabot_settings_patch_total{outcome="` + outcome + `"}`).Get(); got != want {
						t.Errorf("%s Patch count = %d, want %d", outcome, got, want)
					}
				}
				if output := logs.String(); tc.log == "" {
					if output != "" {
						t.Errorf("committed Patch logs = %q, want no rejection or failure log", output)
					}
				} else if strings.Count(output, "\n") != 1 || !strings.Contains(output, tc.log) ||
					!strings.Contains(output, err.Error()) || strings.Contains(output, context.Canceled.Error()) {
					t.Errorf("Patch logs = %q, want one %q record with the writer error", output, tc.log)
				}
				if got := countRows(t, s, `SELECT count(*) FROM settings_audit`); got != tc.wantAudits {
					t.Errorf("audit rows = %d, want %d", got, tc.wantAudits)
				}
			})
		})
	}
}

func TestPatchFailureLoggingDoesNotBlockStoreWriter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pause, logging, resume := pauseWrite()
		defer resume()
		_, h, s := newSampleRegistry(t, func(d *Deps) {
			d.Log = slog.New(slog.NewTextHandler(patchLogWriter(func(p []byte) (int, error) {
				pause()
				return len(p), nil
			}), nil))
		})
		privacyExec(t, s, `CREATE TRIGGER fail_audit BEFORE INSERT ON settings_audit BEGIN SELECT RAISE(ABORT, 'audit failed'); END`)
		patch := startPublicPatch(t, h, 1, 42, `{"enabled":true}`, defaultETag)
		<-logging

		writerCompleted := make(chan error, 1)
		if err := s.Write(func(*sql.Tx) error { return nil }, func(err error) { writerCompleted <- err }); err != nil {
			t.Fatal(err)
		}
		requireFinished(t, "unrelated store Write", writerCompleted)
		resume()
		var opErr *store.OpError
		if err := <-patch; !errors.As(err, &opErr) {
			t.Fatalf("Patch = %v, want OpError", err)
		}
	})
}

type patchLogWriter func([]byte) (int, error)

func (w patchLogWriter) Write(p []byte) (int, error) { return w(p) }

func TestPatchOutcomeClassification(t *testing.T) {
	r := &Registry{}
	for _, tc := range []struct {
		name     string
		err      error
		expected bool
	}{
		{"canceled in writer", &store.OpError{Err: context.Canceled}, true},
		{"deadline in writer", &store.OpError{Err: context.DeadlineExceeded}, true},
		{"conflict in writer", &store.OpError{Err: ErrConflict}, true},
		{"operation failure", &store.OpError{Err: errors.New("audit failed")}, false},
		{"canceled before submit", context.Canceled, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := r.countPatch(tc.err); got != tc.expected {
				t.Fatalf("countPatch(%v) expected = %t, want %t", tc.err, got, tc.expected)
			}
		})
	}
}

func TestPatchRejectionsAreNotCountedAsErrors(t *testing.T) {
	set := metrics.NewSet()
	r := &Registry{metrics: newRegistryMetrics(set)}
	for _, err := range []error{context.Canceled, &store.OpError{Err: context.Canceled}, ErrUnknownFeature, store.ErrBusy} {
		r.countPatch(err)
	}
	r.countPatch(&store.OpError{Err: errors.New("audit failed")})
	for outcome, want := range map[string]uint64{"rejected": 4, "error": 1} {
		if got := set.GetOrCreateCounter(`yabot_settings_patch_total{outcome="` + outcome + `"}`).Get(); got != want {
			t.Errorf("outcome %s = %d, want %d", outcome, got, want)
		}
	}
}
