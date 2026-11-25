package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"modernc.org/sqlite"
)

const backupTestName = "yabot-20261125T153000.123Z.db"

func TestLatestBackup(t *testing.T) {
	stamp := time.Date(2026, time.November, 25, 15, 30, 0, 123_000_000, time.UTC)
	for _, tc := range []struct {
		name    string
		setup   func(t *testing.T, dir string)
		want    time.Time
		wantErr bool
	}{
		{name: "missing directory"},
		{name: "empty directory", setup: func(t *testing.T, dir string) {
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "regular file", wantErr: true, setup: func(t *testing.T, dir string) {
			if err := os.WriteFile(dir, nil, 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "mixed files", want: stamp, setup: func(t *testing.T, dir string) {
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			const older = "yabot-20261125T153000.122Z.db"
			for _, name := range []string{
				older,
				backupTestName,
				"yabot-premigrate-20261126T153000.123Z-0123456789ab.db",
				"yabot-20261126T153000.123Z.db.tmp",
				"yabot-20261126T153000.123Z.db.tmp-journal",
				"yabot-20261126T153000.1234Z.db",
				"yabot-20261131T153000.123Z.db",
			} {
				if err := os.WriteFile(filepath.Join(dir, name), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Mkdir(filepath.Join(dir, "yabot-20261127T153000.123Z.db"), 0700); err != nil {
				t.Fatal(err)
			}
			newerMtime := stamp.Add(time.Hour)
			if err := os.Chtimes(filepath.Join(dir, older), newerMtime, newerMtime); err != nil {
				t.Fatal(err)
			}
			olderMtime := stamp.Add(-time.Hour)
			if err := os.Chtimes(filepath.Join(dir, backupTestName), olderMtime, olderMtime); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(testDir(t), "backup")
			if tc.setup != nil {
				tc.setup(t, dir)
			}
			got, ok, err := LatestBackup(dir)
			if wantOK := !tc.want.IsZero(); (err != nil) != tc.wantErr || ok != wantOK || got != tc.want {
				t.Errorf("LatestBackup = (%v, %t, %v), want time %v, found %t, error present %t", got, ok, err, tc.want, wantOK, tc.wantErr)
			}
		})
	}
}

func TestBackupSnapshot(t *testing.T) {
	deps := newTestDeps(t)
	deps.Now = backupTestTime
	s := openBatchTestStore(t, deps)
	if got, want := s.BackupDir(), filepath.Join(deps.Dir, "backup"); got != want || !filepath.IsAbs(got) {
		t.Errorf("BackupDir() = %q, want absolute path %q", got, want)
	}
	startBatchTestStore(t, s)
	writeCheckpointItem(t, s, 42)

	dir := filepath.Join(deps.Dir, "backup 'quoted'; #")
	if err := s.Backup(t.Context(), dir, 7); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if got := backupTestEntries(t, dir); !slices.Equal(got, []string{backupTestName}) {
		t.Fatalf("backup directory entries = %v, want [%s]", got, backupTestName)
	}
	assertBackupIDs(t, filepath.Join(dir, backupTestName), 42)
}

func TestBackupSyncsDirectoryAndParent(t *testing.T) {
	for _, tc := range []struct {
		name          string
		dirExists     bool
		cancelPublish bool
	}{
		{name: "new directory"},
		{name: "existing directory", dirExists: true},
		{name: "publish canceled in new directory", cancelPublish: true},
		{name: "publish canceled in existing directory", dirExists: true, cancelPublish: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			deps := newTestDeps(t)
			deps.Now = backupTestTime
			if tc.cancelPublish {
				deps.Now = func() time.Time {
					cancel()
					return backupTestTime()
				}
			}
			s := openBatchTestStore(t, deps)
			dir := s.BackupDir()
			if tc.dirExists {
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatalf("create backup directory fixture: %v", err)
				}
			}

			parent := filepath.Dir(dir)
			want := []string{dir, parent}
			var paths []string
			s.hooks = &storeHooks{syncBackupDirectory: func(path string) error {
				paths = append(paths, path)
				if _, err := os.Stat(filepath.Join(dir, backupTestName)); err != nil {
					t.Errorf("stat published backup during directory sync: %v", err)
				}
				return nil
			}}
			var wantErr error
			if tc.cancelPublish {
				want = nil
				wantErr = context.Canceled
			}
			if err := s.Backup(ctx, dir, 1); !errors.Is(err, wantErr) {
				t.Fatalf("Backup returned %v, want %v", err, wantErr)
			}
			if !slices.Equal(paths, want) {
				t.Errorf("synced directories = %v, want %v", paths, want)
			}
			if tc.cancelPublish {
				if got := backupTestEntries(t, dir); len(got) != 0 {
					t.Fatalf("canceled backup left files: %v", got)
				}
			} else {
				assertBackupIDs(t, filepath.Join(dir, backupTestName))
			}
		})
	}
}

func TestBackupRequiresParentDirectory(t *testing.T) {
	s := openTestStore(t, t.Context(), newTestDeps(t))
	for _, tc := range []struct {
		name         string
		parentIsFile bool
		reason       string
	}{
		{name: "missing parent", reason: "must exist"},
		{name: "parent is a file", parentIsFile: true, reason: "not a directory"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := testDir(t)
			parent := filepath.Join(root, "snapshots")
			var want []string
			if tc.parentIsFile {
				if err := os.WriteFile(parent, nil, 0600); err != nil {
					t.Fatalf("create parent fixture: %v", err)
				}
				want = []string{filepath.Base(parent)}
			}
			err := s.Backup(t.Context(), filepath.Join(parent, "backup"), 1)
			if err == nil || !strings.Contains(err.Error(), tc.reason) || !strings.Contains(err.Error(), filepath.Base(parent)) {
				t.Errorf("Backup returned %v, want error identifying unavailable parent %q", err, parent)
			}
			if got := backupTestEntries(t, root); !slices.Equal(got, want) {
				t.Errorf("parent directory fixtures after failed backup = %v, want %v", got, want)
			}
		})
	}
}

func TestBackupRetention(t *testing.T) {
	deps := newTestDeps(t)
	deps.Now = backupTestTime
	s := openTestStore(t, t.Context(), deps)
	dir := testDir(t)
	fixtures := []struct {
		name   string
		retain bool
	}{
		{"yabot-20261122T153000.123Z.db", false},
		{"yabot-20261123T153000.123Z.db", false},
		{"yabot-20261124T153000.123Z.db", true},
		{"yabot-premigrate-20261121T153000.123Z-0123456789ab.db", true},
		{"yabot-premigrate-20261122T153000.123Z-123456789abc.db", true},
		{"yabot-20261122T153001.123Z.db.tmp", false},
		{"yabot-20260230T153000.123Z.db", true},
		{"yabot-20260230T153000.123Z.db.tmp", true},
		{"yabot-20261122T153000.123Z.db.manual", true},
		{"manual.db.tmp", true},
	}
	want := []string{backupTestName}
	for _, fixture := range fixtures {
		path := filepath.Join(dir, fixture.name)
		if err := os.WriteFile(path, []byte(fixture.name), 0600); err != nil {
			t.Fatalf("write backup fixture %q: %v", path, err)
		}
		if fixture.retain {
			want = append(want, fixture.name)
		}
	}
	newerMtime := backupTestTime().Add(time.Hour)
	if err := os.Chtimes(filepath.Join(dir, fixtures[0].name), newerMtime, newerMtime); err != nil {
		t.Fatalf("set misleading modification time on oldest backup: %v", err)
	}
	slices.Sort(want)
	if err := s.Backup(t.Context(), dir, 2); err != nil {
		t.Fatalf("backup with retention: %v", err)
	}
	if got := backupTestEntries(t, dir); !slices.Equal(got, want) {
		t.Fatalf("backup directory entries = %v, want %v", got, want)
	}
	for _, fixture := range fixtures {
		if fixture.retain {
			contents, err := os.ReadFile(filepath.Join(dir, fixture.name))
			if err != nil || string(contents) != fixture.name {
				t.Errorf("backup fixture %q = %q, %v, want original contents", fixture.name, contents, err)
			}
		}
	}
}

func TestBackupDirectorySyncFailure(t *testing.T) {
	for _, tc := range []struct {
		name      string
		oldBackup string
		run       func(*Store, context.Context, []Migration) error
		isMigrate bool
	}{
		{
			name:      "Backup",
			oldBackup: "yabot-20261124T153000.123Z.db",
			run: func(s *Store, ctx context.Context, _ []Migration) error {
				return s.Backup(ctx, s.BackupDir(), 1)
			},
		},
		{
			name:      "Migrate",
			oldBackup: "yabot-premigrate-20261124T153000.123Z-0123456789ab.db",
			run: func(s *Store, ctx context.Context, upgrade []Migration) error {
				return s.Migrate(ctx, upgrade)
			},
			isMigrate: true,
		},
	} {
		for _, failDir := range []string{"backup directory", "parent directory"} {
			t.Run(tc.name+"/"+failDir, func(t *testing.T) {
				deps := newTestDeps(t)
				stamp := backupTestTime().Add(-time.Second)
				deps.Now = func() time.Time { return stamp }
				deps.BackupKeep = 1
				s := openTestStore(t, t.Context(), deps)
				initial, upgrade := migrationUpgradeSteps()
				mustMigrate(t, s, initial)

				dir := s.BackupDir()
				initialBackup := findMigrationSnapshot(t, dir, stamp)
				stamp = backupTestTime()
				fixtures := []string{tc.oldBackup, tc.oldBackup + ".tmp-journal"}
				for _, name := range fixtures {
					if err := os.WriteFile(filepath.Join(dir, name), nil, 0600); err != nil {
						t.Fatalf("write backup fixture %q: %v", name, err)
					}
				}

				failPath := dir
				if failDir == "parent directory" {
					failPath = filepath.Dir(dir)
				}
				syncErr := errors.New("injected directory sync failure")
				s.hooks = &storeHooks{syncBackupDirectory: func(path string) error {
					if path == failPath {
						return syncErr
					}
					return nil
				}}
				want := []string{tc.oldBackup, initialBackup}
				attempts := 1
				if tc.isMigrate {
					attempts = 2
				}
				for i := range attempts {
					if err := tc.run(s, t.Context(), upgrade); !errors.Is(err, syncErr) {
						t.Fatalf("%s attempt %d returned %v, want directory sync error", tc.name, i+1, err)
					}
					published := backupTestName
					if tc.isMigrate {
						published = findMigrationSnapshot(t, dir, stamp.Add(time.Duration(i)*time.Millisecond))
					}
					want = append(want, published)
					slices.Sort(want)
					assertMigrationBackupNames(t, dir, want...)
					assertBackupIDs(t, filepath.Join(dir, published))
				}
				if !tc.isMigrate {
					return
				}

				assertMigrationRecords(t, s.Read(), migrationVersion{"items", 1})
				assertMigrationTable(t, s.Read(), "upgraded_items", false)
				assertUpgradeBackup(t, s.Read(), "")

				s.hooks = nil
				mustMigrate(t, s, upgrade)
				published := findMigrationSnapshot(t, dir, stamp.Add(2*time.Millisecond))
				assertMigrationBackupNames(t, dir, published)
				assertMigrationRecords(t, s.Read(), migrationVersion{"items", 1}, migrationVersion{"items", 2})
			})
		}
	}
}

func TestBackupKeepsNewSnapshotAfterClockRollback(t *testing.T) {
	deps := newTestDeps(t)
	deps.Now = backupTestTime
	s := openBatchTestStore(t, deps)
	dir := testDir(t)
	newer := []string{"yabot-20261126T153000.123Z.db", "yabot-20261127T153000.123Z.db"}
	for _, name := range newer {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0600); err != nil {
			t.Fatalf("write newer backup %q: %v", name, err)
		}
	}
	if err := s.Backup(t.Context(), dir, len(newer)); err != nil {
		t.Fatalf("backup after clock rollback: %v", err)
	}
	if got, want := backupTestEntries(t, dir), []string{backupTestName, newer[1]}; !slices.Equal(got, want) {
		t.Fatalf("backup directory entries = %v, want %v", got, want)
	}
	assertBackupIDs(t, filepath.Join(dir, backupTestName))
}

func TestBackupPrunesInterruptedVacuumFiles(t *testing.T) {
	deps := newTestDeps(t)
	deps.Now = backupTestTime
	s := openBatchTestStore(t, deps)
	dir := testDir(t)
	const old = "yabot-20261124T153000.123Z.db"
	fixtures := []struct {
		name   string
		retain bool
	}{
		{old + ".tmp", false},
		{old + ".tmp-journal", false},
		{"yabot-premigrate-20261124T153000.123Z-0123456789ab.db.tmp-journal", false},
		{old + "-journal", true},
		{old + ".tmp-journal.bak", true},
		{old + ".tmp-wal", true},
		{old + ".tmp-shm", true},
		{"manual.db.tmp-journal", true},
		{"yabot-20260230T153000.123Z.db.tmp-journal", true},
	}
	want := []string{backupTestName}
	for _, fixture := range fixtures {
		if err := os.WriteFile(filepath.Join(dir, fixture.name), nil, 0600); err != nil {
			t.Fatalf("write backup fixture %q: %v", fixture.name, err)
		}
		if fixture.retain {
			want = append(want, fixture.name)
		}
	}
	checked := false
	// SQLite authorizes VACUUM INTO's output attachment before opening the output database.
	setStoreTestAuthorizer(t, s.checkpoint, func(action sqlite.AuthorizerActionCode, _, _, _, _ string) sqlite.AuthorizerReturnCode {
		if action != sqlite.AuthAttach {
			return sqlite.AuthorizerOK
		}
		checked = true
		for _, fixture := range fixtures {
			if fixture.retain {
				continue
			}
			if _, err := os.Lstat(filepath.Join(dir, fixture.name)); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("stat %q at vacuum attachment returned %v, want not exist", fixture.name, err)
			}
		}
		return sqlite.AuthorizerOK
	})
	if err := s.Backup(t.Context(), dir, 1); err != nil {
		t.Fatalf("backup with abandoned vacuum files: %v", err)
	}
	if !checked {
		t.Fatal("backup did not authorize the vacuum attachment")
	}
	slices.Sort(want)
	if got := backupTestEntries(t, dir); !slices.Equal(got, want) {
		t.Fatalf("backup directory entries = %v, want %v", got, want)
	}
}

func TestBackupRejectsNonPositiveKeep(t *testing.T) {
	s := openTestStore(t, t.Context(), newTestDeps(t))
	for _, keep := range []int{0, -1} {
		t.Run(fmt.Sprint(keep), func(t *testing.T) {
			dir := s.BackupDir()
			if err := s.Backup(t.Context(), dir, keep); err == nil {
				t.Fatalf("Backup(_, _, %d) succeeded, want error", keep)
			}
			if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("stat backup directory after keep=%d: %v, want not exist", keep, err)
			}
		})
	}
}

func TestBackupRejectsUnopenedAndStoppedStore(t *testing.T) {
	var unopened Store
	stopped := openTestStore(t, t.Context(), newTestDeps(t))
	if err := stopped.Stop(t.Context()); err != nil {
		t.Fatalf("stop store: %v", err)
	}
	for _, tc := range []struct {
		name string
		s    *Store
	}{
		{"unopened", &unopened},
		{"stopped", stopped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(testDir(t), "backup")
			if err := tc.s.Backup(t.Context(), dir, 7); err == nil {
				t.Fatal("Backup succeeded, want error")
			}
			if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("stat backup directory: %v, want not exist", err)
			}
		})
	}
}

func TestBackupRemovesPartialFileOnCancellation(t *testing.T) {
	deps := newTestDeps(t)
	deps.Now = backupTestTime
	s := openBatchTestStore(t, deps)
	reached, release := pauseBackupCopy(t, s)
	defer release()
	startBatchTestStore(t, s)
	writeCheckpointItem(t, s, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dir := s.BackupDir()
	result := make(chan error, 1)
	go func() { result <- s.Backup(ctx, dir, 7) }()
	awaitBackupCopy(t, reached, result)
	if _, err := os.Stat(filepath.Join(dir, backupTestName+".tmp")); err != nil {
		t.Fatalf("stat temporary backup mid-copy: %v", err)
	}
	cancel()
	release()
	if err := awaitLifecycleResult(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled backup returned %v, want context.Canceled", err)
	}
	// On Windows SQLite's journal deletion can leave a renamed file for a moment, as with testDir.
	deadline := time.Now().Add(2 * time.Second)
	for got := backupTestEntries(t, dir); len(got) != 0; got = backupTestEntries(t, dir) {
		if time.Now().After(deadline) {
			t.Fatalf("canceled backup left files: %v", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := s.Backup(t.Context(), dir, 7); err != nil {
		t.Fatalf("retry backup after cancellation: %v", err)
	}
	assertBackupIDs(t, filepath.Join(dir, backupTestName), 1)
}

func TestWriterCommitsDuringBackup(t *testing.T) {
	deps := newTestDeps(t)
	deps.Now = backupTestTime
	s := openBatchTestStore(t, deps)
	reached, release := pauseBackupCopy(t, s)
	defer release()
	startBatchTestStore(t, s)
	writeCheckpointItem(t, s, 1)

	dir := s.BackupDir()
	ctx := t.Context()
	result := make(chan error, 1)
	go func() { result <- s.Backup(ctx, dir, 7) }()
	awaitBackupCopy(t, reached, result)
	writeCheckpointItem(t, s, 2)
	release()

	if err := awaitLifecycleResult(t, result); err != nil {
		t.Fatalf("backup with concurrent writer: %v", err)
	}
	assertBatchIDs(t, s, 1, 2)
	assertBackupIDs(t, filepath.Join(dir, backupTestName), 1)
}

func TestConcurrentBackupsUseDifferentPaths(t *testing.T) {
	deps := newTestDeps(t)
	synctest.Test(t, func(t *testing.T) {
		var clockCalls atomic.Int32
		deps.Now = func() time.Time {
			clockCalls.Add(1)
			return backupTestTime()
		}
		s := openBatchTestStore(t, deps)
		reached, release := pauseBackupCopy(t, s)
		defer release()
		dir := s.BackupDir()
		ctx := t.Context()
		first, second := make(chan error, 1), make(chan error, 1)
		go func() { first <- s.Backup(ctx, dir, 7) }()
		awaitBackupCopy(t, reached, first)
		go func() { second <- s.Backup(ctx, dir, 7) }()
		synctest.Wait()
		if got := clockCalls.Load(); got != 1 {
			t.Errorf("second backup reached the clock before first backup finished: %d calls", got)
		}
		release()
		for _, result := range []<-chan error{first, second} {
			if err := awaitLifecycleResult(t, result); err != nil {
				t.Fatalf("concurrent backup: %v", err)
			}
		}
		names := backupTestEntries(t, dir)
		if want := []string{backupTestName, "yabot-20261125T153000.124Z.db"}; !slices.Equal(names, want) {
			t.Fatalf("concurrent backup paths = %v, want %v", names, want)
		}
		for _, name := range names {
			assertBackupIDs(t, filepath.Join(dir, name))
		}
	})
}

func TestStopWaitsForActiveBackup(t *testing.T) {
	deps := newTestDeps(t)
	deps.Now = backupTestTime
	synctest.Test(t, func(t *testing.T) {
		s := openBatchTestStore(t, deps)
		reached, release := pauseBackupCopy(t, s)
		defer release()
		ctx := t.Context()
		dir := s.BackupDir()
		active := make(chan error, 1)
		go func() { active <- s.Backup(ctx, dir, 7) }()
		awaitBackupCopy(t, reached, active)

		queuedCtx, cancelQueued := context.WithCancel(ctx)
		defer cancelQueued()
		queued := make(chan error, 1)
		queuedDir := filepath.Join(dir, "queued")
		go func() { queued <- s.Backup(queuedCtx, queuedDir, 7) }()
		synctest.Wait()
		cancelQueued()
		if err := awaitLifecycleResult(t, queued); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel queued Backup: %v, want context.Canceled", err)
		}
		if _, err := os.Lstat(queuedDir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat canceled queued backup directory: %v, want not exist", err)
		}

		stopCtx, cancelStop := context.WithCancel(ctx)
		defer cancelStop()
		stopping := make(chan error, 1)
		go func() { stopping <- s.Stop(stopCtx) }()
		synctest.Wait()
		cancelStop()
		if err := awaitLifecycleResult(t, stopping); !errors.Is(err, context.Canceled) {
			t.Fatalf("Stop while backup is active: %v, want context.Canceled", err)
		}
		if err := failTestOpen(t, ctx, deps); !errors.Is(err, lockConflict) {
			t.Errorf("Open while backup is active: %v, want lock conflict", err)
		}
		if err := s.Backup(ctx, dir, 7); !errors.Is(err, ErrClosed) {
			t.Errorf("Backup after Stop began: %v, want ErrClosed", err)
		}
		release()
		if err := awaitLifecycleResult(t, active); err != nil {
			t.Fatalf("active backup during shutdown: %v", err)
		}
		if err := s.Stop(ctx); err != nil {
			t.Fatalf("Stop after backup finished: %v", err)
		}
		openTestStore(t, ctx, deps)
		assertBackupIDs(t, filepath.Join(dir, backupTestName))
	})
}

func backupTestTime() time.Time {
	return time.Date(2026, time.November, 25, 23, 30, 0, 123456789, time.FixedZone("UTC+8", 8*60*60))
}

func backupTestEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read backup directory %q: %v", dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func pauseBackupCopy(t *testing.T, s *Store) (<-chan struct{}, func()) {
	t.Helper()
	reached, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	pause := sync.OnceFunc(func() {
		close(reached)
		<-resume
	})
	// VACUUM INTO opens the source snapshot before compiling its table copy.
	authorizer := func(action sqlite.AuthorizerActionCode, table, _, database, _ string) sqlite.AuthorizerReturnCode {
		if action == sqlite.AuthRead && table == "items" && database == "main" {
			pause()
		}
		return sqlite.AuthorizerOK
	}
	setStoreTestAuthorizer(t, s.checkpoint, authorizer)
	setStoreTestAuthorizer(t, s.write, authorizer)
	return reached, release
}

func awaitBackupCopy(t *testing.T, reached <-chan struct{}, result <-chan error) {
	t.Helper()
	select {
	case <-reached:
	case err := <-result:
		t.Fatalf("backup returned before copying the source table: %v", err)
	case <-time.After(lifecycleWait):
		t.Fatal("backup did not reach the source table copy")
	}
}

func assertBackupIDs(t *testing.T, path string, want ...int) {
	t.Helper()
	dsn := url.URL{Scheme: "file", OmitHost: true, Path: filepath.ToSlash(path), RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		t.Fatalf("open backup %q: %v", path, err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close backup %q: %v", path, err)
		}
	}()
	var got string
	if err := db.QueryRowContext(t.Context(), "PRAGMA integrity_check").Scan(&got); err != nil || got != "ok" {
		t.Fatalf("backup integrity = %q, %v, want ok", got, err)
	}
	rows, err := db.QueryContext(t.Context(), "SELECT id FROM items ORDER BY id")
	if err != nil {
		t.Fatalf("read items from backup: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close backup rows: %v", err)
		}
	}()
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan backup item: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate backup items: %v", err)
	}
	if !slices.Equal(ids, want) {
		t.Errorf("backup item IDs = %v, want %v", ids, want)
	}
}

func setStoreTestAuthorizer(t *testing.T, db *sql.DB, fn sqlite.AuthorizerFn) {
	t.Helper()
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatalf("borrow SQLite connection: %v", err)
	}
	err = conn.Raw(func(driverConn any) error {
		registerer, ok := driverConn.(sqlite.AuthorizerRegisterer)
		if !ok {
			return fmt.Errorf("SQLite driver connection does not support an authorizer")
		}
		return registerer.RegisterAuthorizer(fn)
	})
	if err := errors.Join(err, conn.Close()); err != nil {
		t.Fatalf("set SQLite authorizer: %v", err)
	}
}
