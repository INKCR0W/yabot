package janitor_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/janitor"
)

func TestBackupTask(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	backupErr := errors.New("backup failed")
	for _, tc := range []struct {
		name     string
		files    []string
		mtime    time.Time
		fail     error
		wantCall bool
		wantRows int
	}{
		{name: "no backup", wantCall: true, wantRows: 1},
		{name: "23 hours old", files: []string{
			"yabot-20261124T100000.000Z.db",
			"yabot-20261124T130000.000Z.db",
		}, mtime: now.Add(-48 * time.Hour)},
		{name: "24 hours old", files: []string{"yabot-20261124T120000.000Z.db"}, mtime: now, wantCall: true, wantRows: 1},
		{name: "25 hours old", files: []string{"yabot-20261124T110000.000Z.db"}, mtime: now, wantCall: true, wantRows: 1},
		{name: "pre-migration only", files: []string{"yabot-premigrate-20261125T110000.000Z-0123456789ab.db"}, mtime: now, wantCall: true, wantRows: 1},
		{name: "backup failed", fail: backupErr, wantCall: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, name := range tc.files {
				path := writeSnapshot(t, dir, name)
				if err := os.Chtimes(path, tc.mtime, tc.mtime); err != nil {
					t.Fatal(err)
				}
			}
			const keep = 7
			calls := 0
			b := testBackuper{dir, func(ctx context.Context, backupDir string, backupKeep int) error {
				calls++
				if ctx != t.Context() || backupDir != dir || backupKeep != keep {
					t.Errorf("Backup received different context or arguments (dir %q, keep %d)", backupDir, backupKeep)
				}
				return tc.fail
			}}
			run := janitor.BackupTask(b, keep, func() time.Time { return now })
			n, err := run(t.Context())
			if n != tc.wantRows || !errors.Is(err, tc.fail) {
				t.Errorf("BackupTask = (%d, %v), want (%d, %v)", n, err, tc.wantRows, tc.fail)
			}
			wantCalls := 0
			if tc.wantCall {
				wantCalls = 1
			}
			if calls != wantCalls {
				t.Errorf("Backup called %d times, want %d", calls, wantCalls)
			}
		})
	}
}

func TestBackupTaskAcrossDailyInterval(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 11, 25, 12, 0, 0, 123_000_000, time.UTC)
	now := start
	calls := 0
	b := testBackuper{dir, func(_ context.Context, backupDir string, _ int) error {
		calls++
		name := "yabot-" + now.UTC().Format("20060102T150405.000Z") + ".db"
		return os.WriteFile(filepath.Join(backupDir, name), nil, 0600)
	}}
	run := janitor.BackupTask(b, 7, func() time.Time { return now })

	for _, step := range []struct {
		name     string
		elapsed  time.Duration
		wantRows int
	}{
		{name: "initial backup", wantRows: 1},
		{name: "23 hours later", elapsed: 23 * time.Hour},
		{name: "24 hours later", elapsed: 24 * time.Hour, wantRows: 1},
		{name: "one hour after the second backup", elapsed: 25 * time.Hour},
	} {
		t.Run(step.name, func(t *testing.T) {
			now = start.Add(step.elapsed)
			previousCalls := calls
			if n, err := run(t.Context()); n != step.wantRows || err != nil {
				t.Errorf("BackupTask = (%d, %v), want (%d, nil)", n, err, step.wantRows)
			}
			if got := calls - previousCalls; got != step.wantRows {
				t.Errorf("Backup called %d times, want %d", got, step.wantRows)
			}
		})
	}
}

func TestBackupTaskPublishedSnapshotBeforeError(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	backupErr := errors.New("sync backup directory failed")
	calls := 0
	b := testBackuper{dir, func(context.Context, string, int) error {
		calls++
		writeSnapshot(t, dir, "yabot-20261125T120000.000Z.db")
		return backupErr
	}}
	run := janitor.BackupTask(b, 7, func() time.Time { return now })
	if n, err := run(t.Context()); n != 0 || !errors.Is(err, backupErr) {
		t.Fatalf("initial BackupTask run = (%d, %v), want (0, %v)", n, err, backupErr)
	}
	if n, err := run(t.Context()); n != 0 || err != nil {
		t.Fatalf("subsequent BackupTask run = (%d, %v), want (0, nil)", n, err)
	}
	if calls != 1 {
		t.Errorf("Backup called %d times, want 1", calls)
	}
}

func TestBackupTaskReadError(t *testing.T) {
	file := writeSnapshot(t, t.TempDir(), "file")
	b := testBackuper{file, func(context.Context, string, int) error {
		t.Fatal("Backup called after reading the directory failed")
		return nil
	}}
	if n, err := janitor.BackupTask(b, 7, nil)(t.Context()); n != 0 || err == nil {
		t.Errorf("BackupTask = (%d, %v), want (0, non-nil error)", n, err)
	}
}

type testBackuper struct {
	dir    string
	backup func(context.Context, string, int) error
}

func (b testBackuper) BackupDir() string { return b.dir }

func (b testBackuper) Backup(ctx context.Context, dir string, keep int) error {
	return b.backup(ctx, dir, keep)
}

func writeSnapshot(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
