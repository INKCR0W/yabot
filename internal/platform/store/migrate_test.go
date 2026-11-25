package store

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func TestMigrateAppliesComponentsOnce(t *testing.T) {
	deps := newTestDeps(t)
	deps.Now = backupTestTime
	s := openTestStore(t, t.Context(), deps)
	sets := [][]Migration{
		{
			{Component: "zeta", Version: 2, SQL: `INSERT INTO items VALUES (2)`},
			{Component: "alpha", Version: 1, SQL: `CREATE TABLE copies (id INTEGER PRIMARY KEY); INSERT INTO copies SELECT id FROM items;`},
			{Component: "zeta", Version: 1, SQL: `CREATE TABLE items (id INTEGER PRIMARY KEY); INSERT INTO items VALUES (1);`},
		},
		{{Component: "beta", Version: 1, SQL: `CREATE TABLE flags (id INTEGER PRIMARY KEY)`}},
	}
	before := time.Now().UnixMilli()
	mustMigrate(t, s, sets...)
	after := time.Now().UnixMilli()
	assertBatchIDs(t, s, 1, 2)
	var copies int
	if err := s.Read().QueryRowContext(t.Context(), `SELECT count(*) FROM copies`).Scan(&copies); err != nil || copies != 2 {
		t.Fatalf("copied rows = %d, error = %v, want 2 and no error", copies, err)
	}
	assertMigrationTable(t, s.Read(), "flags", true)
	versions := []migrationVersion{{"alpha", 1}, {"beta", 1}, {"zeta", 1}, {"zeta", 2}}
	records := assertMigrationRecords(t, s.Read(), versions...)
	for _, record := range records {
		if record.appliedAt < before || record.appliedAt > after {
			t.Errorf("%s version %d applied_at = %d, want Unix milliseconds in [%d, %d]", record.component, record.version, record.appliedAt, before, after)
		}
	}
	name := findMigrationSnapshot(t, s.BackupDir(), backupTestTime())
	assertMigrationBackupNames(t, s.BackupDir(), name)
	snapshot := openMigrationSnapshot(t, filepath.Join(s.BackupDir(), name))
	assertMigrationTable(t, snapshot, "schema_migrations", false)
	assertMigrationTable(t, snapshot, "items", false)
	assertUpgradeBackup(t, snapshot, "")
	assertUpgradeBackup(t, s.Read(), "")

	mustMigrate(t, s, sets...)
	if got := assertMigrationRecords(t, s.Read(), versions...); !slices.Equal(got, records) {
		t.Errorf("migration records after repeat = %v, want %v", got, records)
	}
	assertBatchIDs(t, s, 1, 2)
	assertMigrationBackupNames(t, s.BackupDir(), name)
}

func TestMigrateBacksUpDataWithoutMigrationHistory(t *testing.T) {
	deps := newTestDeps(t)
	deps.Now = backupTestTime
	s := openTestStore(t, t.Context(), deps)
	if _, err := s.write.ExecContext(t.Context(), `
		CREATE TABLE items (id INTEGER PRIMARY KEY);
		INSERT INTO items VALUES (42);
	`); err != nil {
		t.Fatalf("seed database without migration history: %v", err)
	}
	steps := []Migration{{Component: "items", Version: 1, SQL: `UPDATE items SET id = 43 WHERE id = 42`}}
	mustMigrate(t, s, steps)
	assertBatchIDs(t, s, 43)
	assertMigrationRecords(t, s.Read(), migrationVersion{"items", 1})

	name := findMigrationSnapshot(t, s.BackupDir(), backupTestTime())
	path := filepath.Join(s.BackupDir(), name)
	assertBackupIDs(t, path, 42)
	snapshot := openMigrationSnapshot(t, path)
	assertMigrationTable(t, snapshot, "schema_migrations", false)
	assertUpgradeBackup(t, snapshot, "")
}

func TestMigrateRejectsInvalidSetsBeforeChangingDatabase(t *testing.T) {
	const create = `CREATE TABLE invalid_items (id INTEGER PRIMARY KEY)`
	for _, tc := range []struct {
		name string
		sets [][]Migration
	}{
		{"initial gap", [][]Migration{{{Component: "broken", Version: 2, SQL: create}}}},
		{"later gap", [][]Migration{{{Component: "broken", Version: 1, SQL: create}, {Component: "broken", Version: 3, SQL: create}}}},
		{"repeated version", [][]Migration{{{Component: "broken", Version: 1, SQL: create}, {Component: "broken", Version: 1, SQL: create}}}},
		{"zero version", [][]Migration{{{Component: "broken", Version: 0, SQL: create}}}},
		{"empty component", [][]Migration{{{Component: "", Version: 1, SQL: create}}}},
		{"empty SQL", [][]Migration{{{Component: "broken", Version: 1, SQL: ""}}}},
		{"NUL in SQL", [][]Migration{{{Component: "broken", Version: 1, SQL: create + ";\x00INSERT INTO invalid_items VALUES (1)"}}}},
		{"component in two sets", [][]Migration{
			{{Component: "broken", Version: 1, SQL: create}},
			{{Component: "broken", Version: 2, SQL: create}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openTestStore(t, t.Context(), newTestDeps(t))
			valid := []Migration{{Component: "valid", Version: 1, SQL: `CREATE TABLE valid_items (id INTEGER PRIMARY KEY)`}}
			if err := s.Migrate(t.Context(), append([][]Migration{valid}, tc.sets...)...); err == nil {
				t.Fatal("Migrate accepted invalid input")
			}
			for _, table := range []string{"schema_migrations", "valid_items", "invalid_items"} {
				assertMigrationTable(t, s.Read(), table, false)
			}
			assertMigrationBackupNames(t, s.BackupDir())
		})
	}
}

func TestMigrateRejectsNewerOrUnknownComponents(t *testing.T) {
	for _, tc := range []struct {
		name  string
		known int
	}{
		{"newer version", 1},
		{"unknown component", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openTestStore(t, t.Context(), newTestDeps(t))
			installed := []Migration{
				{Component: "existing", Version: 1, SQL: `CREATE TABLE installed (id INTEGER PRIMARY KEY)`},
				{Component: "existing", Version: 2, SQL: `INSERT INTO installed VALUES (10)`},
			}
			mustMigrate(t, s, installed)
			names := backupTestEntries(t, s.BackupDir())
			versions := []migrationVersion{{"existing", 1}, {"existing", 2}}
			before := assertMigrationRecords(t, s.Read(), versions...)
			pending := []Migration{{Component: "pending", Version: 1, SQL: `CREATE TABLE pending_items (id INTEGER PRIMARY KEY)`}}
			err := s.Migrate(t.Context(), pending, installed[:tc.known])
			assertMigrationErrorContains(t, err, "existing", "2", strconv.Itoa(tc.known))
			if got := assertMigrationRecords(t, s.Read(), versions...); !slices.Equal(got, before) {
				t.Errorf("migration records after rejection = %v, want %v", got, before)
			}
			assertMigrationTable(t, s.Read(), "pending_items", false)
			assertMigrationBackupNames(t, s.BackupDir(), names...)
		})
	}
}

func TestMigrateRollsBackOnlyFailedComponentAndCanResume(t *testing.T) {
	deps := newTestDeps(t)
	deps.Now = backupTestTime
	s := openTestStore(t, t.Context(), deps)
	committed := []Migration{{Component: "zeta", Version: 1, SQL: `CREATE TABLE completed (id INTEGER PRIMARY KEY)`}}
	failing := []Migration{
		{Component: "alpha", Version: 1, SQL: `CREATE TABLE items (id INTEGER PRIMARY KEY)`},
		{Component: "alpha", Version: 2, SQL: `INSERT INTO items VALUES (7); INSERT INTO missing_table VALUES (8);`},
	}
	later := []Migration{{Component: "beta", Version: 1, SQL: `CREATE TABLE later_items (id INTEGER PRIMARY KEY)`}}
	err := s.Migrate(t.Context(), committed, failing, later)
	assertMigrationErrorContains(t, err, "alpha", "2")
	assertMigrationRecords(t, s.Read(), migrationVersion{"zeta", 1})
	assertMigrationTable(t, s.Read(), "completed", true)
	assertMigrationTable(t, s.Read(), "items", false)
	assertMigrationTable(t, s.Read(), "later_items", false)
	name := findMigrationSnapshot(t, s.BackupDir(), backupTestTime())
	assertMigrationBackupNames(t, s.BackupDir(), name)
	assertUpgradeBackup(t, s.Read(), name)

	failing[1].SQL = `INSERT INTO items VALUES (7); INSERT INTO items VALUES (8);`
	mustMigrate(t, s, committed, failing, later)
	assertMigrationRecords(t, s.Read(), migrationVersion{"alpha", 1}, migrationVersion{"alpha", 2}, migrationVersion{"beta", 1}, migrationVersion{"zeta", 1})
	assertMigrationTable(t, s.Read(), "later_items", true)
	assertBatchIDs(t, s, 7, 8)
	assertMigrationBackupNames(t, s.BackupDir(), name)
	assertUpgradeBackup(t, s.Read(), "")
}

func TestMigratePreservesConstraintErrorAfterRollback(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"store rolls back", `INSERT INTO items VALUES (2); INSERT OR ABORT INTO items VALUES (1);`},
		{"SQLite rolls back", `INSERT INTO items VALUES (2); INSERT OR ROLLBACK INTO items VALUES (1);`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openTestStore(t, t.Context(), newTestDeps(t))
			steps := []Migration{
				{Component: "items", Version: 1, SQL: `CREATE TABLE items (id INTEGER PRIMARY KEY); INSERT INTO items VALUES (1);`},
				{Component: "items", Version: 2, SQL: tc.sql},
			}
			mustMigrate(t, s, steps[:1])

			err := s.Migrate(t.Context(), steps)
			assertMigrationErrorContains(t, err, "UNIQUE constraint failed: items.id")
			var sqliteErr *sqlite.Error
			if !errors.As(err, &sqliteErr) || sqliteErr.Code() != sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY {
				t.Errorf("Migrate error = %v, want SQLITE_CONSTRAINT_PRIMARYKEY", err)
			}
			if strings.Contains(err.Error(), "no transaction is active") {
				t.Errorf("Migrate error includes rollback noise: %v", err)
			}
			assertBatchIDs(t, s, 1)
			assertMigrationRecords(t, s.Read(), migrationVersion{"items", 1})
		})
	}
}

func TestMigrateForeignKeys(t *testing.T) {
	const initial = `
		CREATE TABLE parent (id INTEGER PRIMARY KEY);
		CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE);
		INSERT INTO parent VALUES (1);
		INSERT INTO child VALUES (10, 1), (11, 1);
	`
	for _, tc := range []struct {
		name string
		sql  string
		want []migrationVersion
	}{
		{
			name: "preserve children when rebuilding parent",
			sql: `
				PRAGMA foreign_keys = OFF;
				CREATE TABLE new_parent (id INTEGER PRIMARY KEY, label TEXT NOT NULL);
				INSERT INTO new_parent SELECT id, 'retained' FROM parent;
				DROP TABLE parent;
				ALTER TABLE new_parent RENAME TO parent;
			`,
			want: []migrationVersion{{"constraints", 1}, {"constraints", 2}},
		},
		{
			name: "roll back foreign key violation",
			sql: `
				INSERT INTO parent VALUES (2);
				INSERT INTO child VALUES (12, 99);
			`,
			want: []migrationVersion{{"constraints", 1}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openTestStore(t, t.Context(), newTestDeps(t))
			steps := []Migration{
				{Component: "constraints", Version: 1, SQL: initial},
				{Component: "constraints", Version: 2, SQL: tc.sql},
			}
			mustMigrate(t, s, steps[:1])
			err := s.Migrate(t.Context(), steps)
			if len(tc.want) == 1 {
				assertMigrationErrorContains(t, err, "constraints", "version 2", "foreign key violations", "child")
			} else {
				if err != nil {
					t.Fatalf("Migrate: %v", err)
				}
				var label string
				if err := s.Read().QueryRowContext(t.Context(), `SELECT label FROM parent WHERE id = 1`).Scan(&label); err != nil || label != "retained" {
					t.Fatalf("parent label = %q, error = %v, want retained and no error", label, err)
				}
			}
			assertMigrationRecords(t, s.Read(), tc.want...)
			var parents, children string
			if err := s.Read().QueryRowContext(t.Context(), `SELECT
				(SELECT coalesce(group_concat(id ORDER BY id), '') FROM parent),
				(SELECT coalesce(group_concat(id || ':' || parent_id ORDER BY id), '') FROM child)
			`).Scan(&parents, &children); err != nil {
				t.Fatal(err)
			}
			if parents != "1" || children != "10:1,11:1" {
				t.Errorf("parents = %q, children = %q, want 1 and 10:1,11:1", parents, children)
			}
			assertWriterForeignKeys(t, s)
		})
	}
}

func TestMigrateDiscardsConnectionAfterFailedScript(t *testing.T) {
	for _, tc := range []struct {
		name, broken, fixed string
	}{
		{
			name:   "attached database",
			broken: `ATTACH DATABASE ':memory:' AS staging; INSERT INTO missing_table VALUES (1);`,
			fixed:  `ATTACH DATABASE ':memory:' AS staging; DETACH DATABASE staging; INSERT INTO items VALUES (1);`,
		},
		{
			name:   "query only",
			broken: `PRAGMA query_only = ON; INSERT INTO items VALUES (1);`,
			fixed:  `INSERT INTO items VALUES (1);`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openTestStore(t, t.Context(), newTestDeps(t))
			steps := []Migration{
				{Component: "items", Version: 1, SQL: `CREATE TABLE items (id INTEGER PRIMARY KEY)`},
				{Component: "items", Version: 2, SQL: tc.broken},
			}
			mustMigrate(t, s, steps[:1])
			assertMigrationErrorContains(t, s.Migrate(t.Context(), steps), "execute store migration", "items", "version 2")
			assertMigrationRecords(t, s.Read(), migrationVersion{"items", 1})
			assertWriterForeignKeys(t, s)
			steps[1].SQL = tc.fixed
			mustMigrate(t, s, steps)
			assertMigrationRecords(t, s.Read(), migrationVersion{"items", 1}, migrationVersion{"items", 2})
			assertBatchIDs(t, s, 1)
		})
	}
}

func TestMigrateDoesNotExposeTemporaryTableToWriter(t *testing.T) {
	s := openTestStore(t, t.Context(), newTestDeps(t))
	steps := []Migration{
		{Component: "items", Version: 1, SQL: `
			CREATE TABLE items (id INTEGER PRIMARY KEY);
			INSERT INTO items VALUES (1);
		`},
		{Component: "items", Version: 2, SQL: `
			CREATE TEMP TABLE items AS SELECT id FROM main.items;
			DROP TABLE main.items;
			CREATE TABLE main.items (id INTEGER PRIMARY KEY, label TEXT NOT NULL DEFAULT '');
			INSERT INTO main.items (id) SELECT id FROM temp.items;
		`},
	}
	mustMigrate(t, s, steps[:1])
	mustMigrate(t, s, steps)
	startBatchTestStore(t, s)
	if err := s.WriteWait(t.Context(), insertBatchItem(2)); err != nil {
		t.Fatalf("write item after migration: %v", err)
	}
	assertBatchIDs(t, s, 1, 2)
}

func assertWriterForeignKeys(t *testing.T, s *Store) {
	t.Helper()
	var enabled int
	if err := s.write.QueryRowContext(t.Context(), `PRAGMA foreign_keys`).Scan(&enabled); err != nil || enabled != 1 {
		t.Fatalf("writer foreign_keys = %d, error = %v, want 1 and no error", enabled, err)
	}
}

func TestMigrateScriptCannotCommitItsComponent(t *testing.T) {
	s := openTestStore(t, t.Context(), newTestDeps(t))
	steps := []Migration{
		{Component: "items", Version: 1, SQL: `CREATE TABLE items (id INTEGER PRIMARY KEY)`},
		{Component: "items", Version: 2, SQL: `INSERT INTO items VALUES (1); COMMIT; INSERT INTO missing_table VALUES (2);`},
	}
	err := s.Migrate(t.Context(), steps)
	assertMigrationErrorContains(t, err, "items", "2")
	assertMigrationTable(t, s.Read(), "items", false)

	steps[1].SQL = `INSERT INTO items VALUES (1);`
	mustMigrate(t, s, steps)
	assertMigrationRecords(t, s.Read(), migrationVersion{"items", 1}, migrationVersion{"items", 2})
	assertBatchIDs(t, s, 1)
}

func TestMigrateKeepsSnapshotBeforeAllComponentUpgrades(t *testing.T) {
	deps := newTestDeps(t)
	stamp := backupTestTime()
	deps.Now = func() time.Time { return stamp }
	deps.BackupKeep = 1
	s := openTestStore(t, t.Context(), deps)
	if want := filepath.Join(deps.Dir, "backup"); s.BackupDir() != want {
		t.Fatalf("BackupDir() = %q, want %q", s.BackupDir(), want)
	}
	initial, upgrade := migrationUpgradeSteps()
	flags := []Migration{
		{Component: "flags", Version: 1, SQL: `CREATE TABLE flags (id INTEGER PRIMARY KEY)`},
		{Component: "flags", Version: 2, SQL: `CREATE TABLE upgraded_flags (id INTEGER PRIMARY KEY)`},
	}
	mustMigrate(t, s, initial, flags[:1])
	stamp = stamp.Add(time.Second)
	mustMigrate(t, s, upgrade, flags)
	assertMigrationRecords(t, s.Read(), migrationVersion{"flags", 1}, migrationVersion{"flags", 2}, migrationVersion{"items", 1}, migrationVersion{"items", 2})

	name := findMigrationSnapshot(t, s.BackupDir(), stamp)
	assertMigrationBackupNames(t, s.BackupDir(), name)
	snapshot := openMigrationSnapshot(t, filepath.Join(s.BackupDir(), name))
	for _, table := range []string{"items", "flags"} {
		assertMigrationTable(t, snapshot, table, true)
		assertMigrationTable(t, snapshot, "upgraded_"+table, false)
		assertMigrationTable(t, s.Read(), "upgraded_"+table, true)
	}
	assertMigrationRecords(t, snapshot, migrationVersion{"flags", 1}, migrationVersion{"items", 1})
	mustMigrate(t, s, upgrade, flags)
	assertMigrationBackupNames(t, s.BackupDir(), name)
}

func TestMigrateReusesSnapshotAcrossPartialUpgradeRestarts(t *testing.T) {
	ctx := t.Context()
	deps := newTestDeps(t)
	deps.BackupKeep = 2
	stamp := backupTestTime()
	deps.Now = func() time.Time { return stamp }
	var log bytes.Buffer
	deps.Log = slog.New(slog.NewTextHandler(&log, nil))
	s := openTestStore(t, ctx, deps)
	dir := s.BackupDir()
	initial, upgrade := migrationUpgradeSteps()
	flags := failingFlagsUpgrade()
	mustMigrate(t, s, initial, flags[:1])
	first := findMigrationSnapshot(t, dir, stamp)
	stamp = stamp.Add(time.Second)
	assertMigrationErrorContains(t, s.Migrate(ctx, upgrade, flags), "flags", "version 2", "missing_table")
	name := findMigrationSnapshot(t, dir, stamp)
	assertUpgradeBackup(t, s.Read(), name)

	for attempt := 0; attempt <= deps.BackupKeep; attempt++ {
		if err := s.Stop(ctx); err != nil {
			t.Fatalf("stop store before retry %d: %v", attempt+1, err)
		}
		stamp = stamp.Add(time.Second)
		log.Reset()
		s = openTestStore(t, ctx, deps)
		assertMigrationErrorContains(t, s.Migrate(ctx, upgrade, flags), "flags", "version 2", "missing_table")
		if !slices.ContainsFunc(strings.Split(log.String(), "\n"), func(line string) bool {
			return strings.Contains(line, "level=INFO ") && strings.Contains(line, "reusing") && strings.Contains(line, name)
		}) {
			t.Errorf("retry %d did not log reuse of %q at Info level: %s", attempt+1, name, &log)
		}
	}
	assertMigrationBackupNames(t, dir, first, name)
	assertUpgradeBackup(t, s.Read(), name)
	assertMigrationRecords(t, s.Read(), migrationVersion{"flags", 1}, migrationVersion{"items", 1}, migrationVersion{"items", 2})
	assertMigrationTable(t, s.Read(), "upgraded_items", true)
	assertMigrationTable(t, s.Read(), "upgraded_flags", false)

	snapshot := openMigrationSnapshot(t, filepath.Join(dir, name))
	assertMigrationRecords(t, snapshot, migrationVersion{"flags", 1}, migrationVersion{"items", 1})
	assertMigrationTable(t, snapshot, "upgraded_items", false)
	assertMigrationTable(t, snapshot, "upgraded_flags", false)
	assertUpgradeBackup(t, snapshot, "")

	flags[1].SQL = "CREATE TABLE upgraded_flags (id INTEGER PRIMARY KEY)"
	mustMigrate(t, s, upgrade, flags)
	assertMigrationRecords(t, s.Read(), migrationVersion{"flags", 1}, migrationVersion{"flags", 2}, migrationVersion{"items", 1}, migrationVersion{"items", 2})
	assertUpgradeBackup(t, s.Read(), "")
	assertMigrationBackupNames(t, dir, first, name)
}

func TestMigrateCreatesNewSnapshotAfterRestoringDifferentTarget(t *testing.T) {
	ctx := t.Context()
	deps := newTestDeps(t)
	deps.BackupKeep = 3
	stamp := backupTestTime()
	deps.Now = func() time.Time { return stamp }
	s := openTestStore(t, ctx, deps)
	dir := s.BackupDir()
	initial, upgrade := migrationUpgradeSteps()
	flags := failingFlagsUpgrade()
	mustMigrate(t, s, initial, flags[:1])
	upgrade[1].SQL += `; INSERT INTO items VALUES (42)`
	stamp = stamp.Add(time.Second)
	assertMigrationErrorContains(t, s.Migrate(ctx, upgrade, flags), "flags", "version 2", "missing_table")
	targetA := findMigrationSnapshot(t, dir, stamp)
	assertUpgradeBackup(t, s.Read(), targetA)

	flags[1].SQL = "CREATE TABLE upgraded_flags (id INTEGER PRIMARY KEY)"
	targetBFlags := append(slices.Clone(flags), Migration{Component: "flags", Version: 3, SQL: "INSERT INTO upgraded_flags VALUES (1)"})
	stamp = stamp.Add(time.Second)
	mustMigrate(t, s, upgrade, targetBFlags)
	targetB := findMigrationSnapshot(t, dir, stamp)
	path := filepath.Join(dir, targetB)
	t.Run("snapshot has no upgrade marker", func(t *testing.T) {
		assertUpgradeBackup(t, openMigrationSnapshot(t, path), "")
	})

	if err := s.Stop(ctx); err != nil {
		t.Fatalf("stop store before restore: %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read snapshot for restore: %v", err)
	}
	if err := os.WriteFile(filepath.Join(deps.Dir, "yabot.db"), contents, 0600); err != nil {
		t.Fatalf("restore database: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, targetA)); err != nil {
		t.Fatalf("check original target A snapshot still exists: %v", err)
	}
	stamp = stamp.Add(time.Second)
	s = openTestStore(t, ctx, deps)
	mustMigrate(t, s, upgrade, flags)
	retry := findMigrationSnapshot(t, dir, stamp)
	assertMigrationBackupNames(t, dir, targetA, targetB, retry)
	assertBackupIDs(t, filepath.Join(dir, targetA))
	assertBackupIDs(t, filepath.Join(dir, retry), 42)
	assertMigrationRecords(t, s.Read(), migrationVersion{"flags", 1}, migrationVersion{"flags", 2}, migrationVersion{"items", 1}, migrationVersion{"items", 2})
}

func TestMigrateCreatesNewSnapshotAfterRestoringSameTarget(t *testing.T) {
	ctx := t.Context()
	deps := newTestDeps(t)
	stamp := backupTestTime()
	deps.Now = func() time.Time { return stamp }
	s := openTestStore(t, ctx, deps)
	dir := s.BackupDir()
	initial, upgrade := migrationUpgradeSteps()
	upgrade[1].SQL += "; INSERT INTO items VALUES (2)"
	flags := failingFlagsUpgrade()
	mustMigrate(t, s, initial, flags[:1])
	baseline := findMigrationSnapshot(t, dir, stamp)

	stamp = stamp.Add(time.Second)
	assertMigrationErrorContains(t, s.Migrate(ctx, upgrade, flags), "flags", "version 2", "missing_table")
	assertMigrationRecords(t, s.Read(), migrationVersion{"flags", 1}, migrationVersion{"items", 1}, migrationVersion{"items", 2})
	oldSnapshot := findMigrationSnapshot(t, dir, stamp)
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("stop store before restore: %v", err)
	}
	oldPath := filepath.Join(dir, oldSnapshot)
	contents, err := os.ReadFile(oldPath)
	if err != nil {
		t.Fatalf("read snapshot for restore: %v", err)
	}
	dbPath := filepath.Join(deps.Dir, "yabot.db")
	if err := os.WriteFile(dbPath, contents, 0600); err != nil {
		t.Fatalf("restore database: %v", err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := os.Remove(dbPath + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("remove stale database file %q: %v", dbPath+suffix, err)
		}
	}

	s = openTestStore(t, ctx, deps)
	mustMigrate(t, s, initial, flags[:1])
	assertMigrationRecords(t, s.Read(), migrationVersion{"flags", 1}, migrationVersion{"items", 1})
	assertMigrationBackupNames(t, dir, baseline, oldSnapshot)
	startBatchTestStore(t, s)
	const recoveredItemID = 42
	if err := s.WriteWait(ctx, insertBatchItem(recoveredItemID)); err != nil {
		t.Fatalf("write item after restore: %v", err)
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("stop restored store before upgrade: %v", err)
	}

	stamp = stamp.Add(time.Second)
	s = openTestStore(t, ctx, deps)
	flags[1].SQL = "CREATE TABLE upgraded_flags (id INTEGER PRIMARY KEY)"
	mustMigrate(t, s, upgrade, flags)
	newSnapshot := findMigrationSnapshot(t, dir, stamp)
	assertMigrationBackupNames(t, dir, baseline, oldSnapshot, newSnapshot)
	assertBackupIDs(t, oldPath)
	assertBackupIDs(t, filepath.Join(dir, newSnapshot), recoveredItemID)
	assertMigrationRecords(t, s.Read(), migrationVersion{"flags", 1}, migrationVersion{"flags", 2}, migrationVersion{"items", 1}, migrationVersion{"items", 2})
	assertBatchIDs(t, s, 2, recoveredItemID)
}

func TestMigrateRecreatesMissingMarkedSnapshot(t *testing.T) {
	deps := newTestDeps(t)
	stamp := backupTestTime()
	deps.Now = func() time.Time { return stamp }
	s := openTestStore(t, t.Context(), deps)
	dir := s.BackupDir()
	initial, upgrade := migrationUpgradeSteps()
	flags := failingFlagsUpgrade()
	mustMigrate(t, s, initial, flags[:1])
	baseline := findMigrationSnapshot(t, dir, stamp)
	stamp = stamp.Add(time.Second)
	assertMigrationErrorContains(t, s.Migrate(t.Context(), upgrade, flags), "flags", "version 2", "missing_table")
	first := findMigrationSnapshot(t, dir, stamp)
	assertUpgradeBackup(t, s.Read(), first)
	if err := os.Remove(filepath.Join(dir, first)); err != nil {
		t.Fatalf("remove marked snapshot: %v", err)
	}

	stamp = stamp.Add(time.Second)
	assertMigrationErrorContains(t, s.Migrate(t.Context(), upgrade, flags), "flags", "version 2", "missing_table")
	second := findMigrationSnapshot(t, dir, stamp)
	assertMigrationBackupNames(t, dir, baseline, second)
	assertUpgradeBackup(t, s.Read(), second)
	snapshot := openMigrationSnapshot(t, filepath.Join(dir, second))
	assertUpgradeBackup(t, snapshot, "")
	assertMigrationRecords(t, snapshot, migrationVersion{"flags", 1}, migrationVersion{"items", 1}, migrationVersion{"items", 2})
}

func TestMigrateRotationKeepsMarkedSnapshot(t *testing.T) {
	deps := newTestDeps(t)
	stamp := backupTestTime()
	deps.Now = func() time.Time { return stamp }
	deps.BackupKeep = 1
	s := openTestStore(t, t.Context(), deps)
	dir := s.BackupDir()
	initial, upgrade := migrationUpgradeSteps()
	flags := failingFlagsUpgrade()
	mustMigrate(t, s, initial, flags[:1])
	stamp = stamp.Add(time.Second)
	assertMigrationErrorContains(t, s.Migrate(t.Context(), upgrade, flags), "flags", "version 2", "missing_table")
	marked := findMigrationSnapshot(t, dir, stamp)
	newer := strings.Replace(marked, "20261125", "20261126", 1)
	if err := os.WriteFile(filepath.Join(dir, newer), nil, 0600); err != nil {
		t.Fatalf("write newer snapshot fixture: %v", err)
	}

	assertMigrationErrorContains(t, s.Migrate(t.Context(), upgrade, flags), "flags", "version 2", "missing_table")
	assertMigrationBackupNames(t, dir, marked)
	assertUpgradeBackup(t, s.Read(), marked)
	assertMigrationRecords(t, openMigrationSnapshot(t, filepath.Join(dir, marked)), migrationVersion{"flags", 1}, migrationVersion{"items", 1})
}

func TestMigrateClearsMarkerAfterFinalComponentCommitted(t *testing.T) {
	s := openTestStore(t, t.Context(), newTestDeps(t))
	initial, upgrade := migrationUpgradeSteps()
	mustMigrate(t, s, initial)
	mustMigrate(t, s, upgrade)
	names := backupTestEntries(t, s.BackupDir())
	if len(names) != 2 {
		t.Fatalf("backup directory entries = %v, want two snapshots", names)
	}
	if _, err := s.write.ExecContext(t.Context(), `INSERT INTO store_upgrade (id, target, backup, started_at) VALUES (1, ?, ?, ?)`,
		"0123456789ab", names[1], time.Now().UnixMilli()); err != nil {
		t.Fatalf("simulate interruption before clearing upgrade marker: %v", err)
	}

	mustMigrate(t, s, upgrade)
	assertUpgradeBackup(t, s.Read(), "")
	assertMigrationBackupNames(t, s.BackupDir(), names...)
}

func TestMigrateAddsMarkerTableToExistingDatabase(t *testing.T) {
	s := openTestStore(t, t.Context(), newTestDeps(t))
	initial, upgrade := migrationUpgradeSteps()
	mustMigrate(t, s, initial)
	if _, err := s.write.ExecContext(t.Context(), "DROP TABLE store_upgrade"); err != nil {
		t.Fatalf("simulate database created before upgrade tracking: %v", err)
	}

	mustMigrate(t, s, upgrade)
	assertMigrationRecords(t, s.Read(), migrationVersion{"items", 1}, migrationVersion{"items", 2})
	assertUpgradeBackup(t, s.Read(), "")
}

func TestMigrateRotatesOnlyPremigrationSnapshots(t *testing.T) {
	deps := newTestDeps(t)
	deps.BackupKeep = 2
	stamp := backupTestTime()
	deps.Now = func() time.Time { return stamp }
	s := openTestStore(t, t.Context(), deps)
	dir := s.BackupDir()
	initial, upgrade := migrationUpgradeSteps()
	mustMigrate(t, s, initial)
	if err := s.Backup(t.Context(), dir, 1); err != nil {
		t.Fatalf("create daily backup: %v", err)
	}

	const old = "yabot-premigrate-20261124T153000.123Z-0123456789ab.db"
	fixtures := []struct {
		name string
		keep bool
	}{
		{old + ".tmp", false},
		{old + ".tmp-journal", false},
		{old + ".tmp-journal.bak", true},
		{old + "-journal", true},
		{"yabot-premigrate-20260230T153000.123Z-0123456789ab.db.tmp", true},
		{"yabot-premigrate-20261124T153000.123Z-0123456789ag.db.tmp-journal", true},
		{"yabot-premigrate-20261124T153000.123Z-0123456789a.db.tmp", true},
	}
	want := []string{backupTestName}
	for _, fixture := range fixtures {
		if err := os.WriteFile(filepath.Join(dir, fixture.name), nil, 0600); err != nil {
			t.Fatalf("write interrupted backup fixture %q: %v", fixture.name, err)
		}
		if fixture.keep {
			want = append(want, fixture.name)
		}
	}

	snapshots := []string{findMigrationSnapshot(t, dir, stamp)}
	for _, migrations := range [][]Migration{
		upgrade,
		append(slices.Clone(upgrade), Migration{Component: "items", Version: 3, SQL: "INSERT INTO items VALUES (3)"}),
		append(slices.Clone(upgrade), Migration{Component: "items", Version: 3, SQL: "INSERT INTO items VALUES (3)"}, Migration{Component: "items", Version: 4, SQL: "INSERT INTO items VALUES (4)"}),
	} {
		stamp = stamp.Add(time.Second)
		mustMigrate(t, s, migrations)
		snapshots = append(snapshots, findMigrationSnapshot(t, dir, stamp))
		retained := append(slices.Clone(want), snapshots[max(len(snapshots)-deps.BackupKeep, 0):]...)
		slices.Sort(retained)
		assertMigrationBackupNames(t, dir, retained...)
	}
	assertMigrationRecords(t, openMigrationSnapshot(t, filepath.Join(dir, backupTestName)), migrationVersion{"items", 1})
	assertMigrationRecords(t, openMigrationSnapshot(t, filepath.Join(dir, snapshots[2])), migrationVersion{"items", 1}, migrationVersion{"items", 2})
	assertMigrationRecords(t, openMigrationSnapshot(t, filepath.Join(dir, snapshots[3])), migrationVersion{"items", 1}, migrationVersion{"items", 2}, migrationVersion{"items", 3})
}

func TestMigrationTarget(t *testing.T) {
	zeta := []Migration{
		{Component: "zeta", Version: 1, SQL: "SELECT 1"},
		{Component: "zeta", Version: 2, SQL: "SELECT 2"},
	}
	alpha := []Migration{{Component: "alpha", Version: 1, SQL: "SELECT 1"}}
	target := func(t *testing.T, sets ...[]Migration) string {
		t.Helper()
		components, err := validateMigrations(sets)
		if err != nil {
			t.Fatal(err)
		}
		return migrationTarget(components)
	}
	base := target(t, zeta, alpha)
	if !regexp.MustCompile("^[0-9a-f]{12}$").MatchString(base) {
		t.Fatalf("migration target = %q, want 12 lowercase hexadecimal digits", base)
	}
	for _, tc := range []struct {
		name string
		sets [][]Migration
		same bool
	}{
		{"reordered components and versions", [][]Migration{alpha, {zeta[1], zeta[0]}}, true},
		{"different maximum version", [][]Migration{zeta[:1], alpha}, false},
		{"different component at the same version", [][]Migration{zeta, {{Component: "beta", Version: 1, SQL: "SELECT 1"}}}, false},
		{"different SQL at the same version", [][]Migration{zeta, {{Component: "alpha", Version: 1, SQL: "SELECT 3"}}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := target(t, tc.sets...)
			if (got == base) != tc.same {
				t.Errorf("migration target = %q, base = %q, want equal: %t", got, base, tc.same)
			}
		})
	}
}

func TestMigrateStopsIfPreflightBackupFails(t *testing.T) {
	initial, upgrade := migrationUpgradeSteps()
	for _, tc := range []struct {
		name    string
		applied []Migration
		pending []Migration
	}{
		{name: "first migration", pending: initial},
		{name: "upgrade", applied: initial, pending: upgrade},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openTestStore(t, t.Context(), newTestDeps(t))
			if len(tc.applied) != 0 {
				mustMigrate(t, s, tc.applied)
				if err := os.Rename(s.BackupDir(), s.BackupDir()+".original"); err != nil {
					t.Fatalf("move existing backup directory: %v", err)
				}
			}
			if err := os.WriteFile(s.BackupDir(), []byte("not a directory"), 0600); err != nil {
				t.Fatal(err)
			}
			assertMigrationErrorContains(t, s.Migrate(t.Context(), tc.pending), "back up store before migration")
			if len(tc.applied) == 0 {
				assertMigrationTable(t, s.Read(), "schema_migrations", false)
				assertMigrationTable(t, s.Read(), "items", false)
			} else {
				assertMigrationRecords(t, s.Read(), migrationVersion{"items", 1})
				assertMigrationTable(t, s.Read(), "upgraded_items", false)
			}
		})
	}
}

func TestMigrateRejectsInvalidLifecycle(t *testing.T) {
	t.Run("unopened", func(t *testing.T) {
		var s Store
		initial, _ := migrationUpgradeSteps()
		if err := s.Migrate(t.Context(), initial); err == nil {
			t.Fatal("Migrate succeeded before Open")
		}
	})
	for _, tc := range []struct {
		name       string
		transition func(*Store, context.Context) error
	}{
		{"started", (*Store).Start},
		{"stopped", (*Store).Stop},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openTestStore(t, t.Context(), newTestDeps(t))
			if err := tc.transition(s, t.Context()); err != nil {
				t.Fatal(err)
			}
			initial, _ := migrationUpgradeSteps()
			if err := s.Migrate(t.Context(), initial); err == nil {
				t.Fatal("Migrate succeeded outside its lifecycle")
			}
		})
	}
}

func TestStartRejectsActiveMigration(t *testing.T) {
	deps := newTestDeps(t)
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		s := openTestStore(t, ctx, deps)
		ticks := make(chan time.Time, 1)
		checkpoints := make(chan error, 1)
		s.hooks = &storeHooks{
			checkpointTicks: ticks,
			afterCheckpointTick: func(_ string, _ checkpointResult, err error) {
				checkpoints <- err
			},
		}
		gate := newBatchTestGate(t)
		setStoreTestAuthorizer(t, s.write, func(action sqlite.AuthorizerActionCode, table, _, _, _ string) sqlite.AuthorizerReturnCode {
			if action == sqlite.AuthCreateTable && table == "schema_migrations" {
				gate.block()
			}
			return sqlite.AuthorizerOK
		})
		initial, _ := migrationUpgradeSteps()
		result := startPausedMigration(t, s, ctx, initial, gate)

		if err := s.Start(ctx); err == nil {
			t.Error("Start succeeded while migration was active")
		}
		ticks <- time.Now()
		synctest.Wait()
		select {
		case err := <-checkpoints:
			t.Errorf("checkpoint ran while migration was active: %v", err)
		default:
		}
		rejected := func(err error) { t.Errorf("rejected Write called its callback: %v", err) }
		if err := s.Write(insertBatchItem(1), rejected); !errors.Is(err, ErrClosed) {
			t.Errorf("Write while migration was active = %v, want ErrClosed", err)
		}

		gate.release()
		if err := awaitLifecycleResult(t, result); err != nil {
			t.Fatalf("Migrate after release: %v", err)
		}
		if err := s.Start(ctx); err != nil {
			t.Fatalf("Start after migration: %v", err)
		}
		if err := awaitLifecycleResult(t, checkpoints); err != nil {
			t.Fatalf("checkpoint after Start: %v", err)
		}
		writeCtx, cancelWrite := context.WithTimeout(ctx, lifecycleWait)
		defer cancelWrite()
		if err := s.WriteWait(writeCtx, insertBatchItem(1)); err != nil {
			t.Fatalf("WriteWait after Start: %v", err)
		}
		assertBatchIDs(t, s, 1)
	})
}

func TestMigrateRespectsCanceledContext(t *testing.T) {
	for _, tc := range []struct {
		name         string
		cancelDuring bool
	}{
		{name: "before migration"},
		{name: "inside migration transaction", cancelDuring: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openTestStore(t, t.Context(), newTestDeps(t))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancelDuring {
				setStoreTestAuthorizer(t, s.write, func(action sqlite.AuthorizerActionCode, table, _, _, _ string) sqlite.AuthorizerReturnCode {
					if action == sqlite.AuthCreateTable && table == "schema_migrations" {
						cancel()
					}
					return sqlite.AuthorizerOK
				})
			} else {
				cancel()
			}
			initial, _ := migrationUpgradeSteps()
			if err := s.Migrate(ctx, initial); !errors.Is(err, context.Canceled) {
				t.Fatalf("Migrate error = %v, want context cancellation", err)
			}
			assertMigrationTable(t, s.Read(), "schema_migrations", false)
			assertMigrationTable(t, s.Read(), "items", false)
			assertWriterForeignKeys(t, s)
			mustMigrate(t, s, initial)
		})
	}
}

func TestMigrateCancelsDuringComponentSQL(t *testing.T) {
	deps := newTestDeps(t)
	synctest.Test(t, func(t *testing.T) {
		s := openTestStore(t, t.Context(), deps)
		steps := []Migration{
			{Component: "items", Version: 1, SQL: `CREATE TABLE items (id INTEGER PRIMARY KEY); INSERT INTO items VALUES (1);`},
			{Component: "items", Version: 2, SQL: `INSERT INTO items VALUES (2)`},
			{Component: "items", Version: 3, SQL: `
				INSERT INTO items VALUES (pause_migration());
				INSERT INTO items VALUES (observe_second_statement());
			`},
		}
		mustMigrate(t, s, steps[:1])

		gate := newBatchTestGate(t)
		var secondCalls int
		sqliteDriver := new(sqlite.Driver)
		if err := sqliteDriver.RegisterScalarFunction("pause_migration", 0, func(*sqlite.FunctionContext, []driver.Value) (driver.Value, error) {
			gate.block()
			return int64(3), nil
		}); err != nil {
			t.Fatalf("register migration pause function: %v", err)
		}
		if err := sqliteDriver.RegisterScalarFunction("observe_second_statement", 0, func(*sqlite.FunctionContext, []driver.Value) (driver.Value, error) {
			secondCalls++
			return int64(4), nil
		}); err != nil {
			t.Fatalf("register migration observer function: %v", err)
		}
		if err := s.write.Close(); err != nil {
			t.Fatalf("close original writer pool: %v", err)
		}
		dsn := url.URL{Scheme: "file", OmitHost: true, Path: filepath.ToSlash(filepath.Join(deps.Dir, "yabot.db")), RawQuery: "mode=rw&_pragma=foreign_keys(ON)"}
		s.write = sql.OpenDB(migrationTestConnector{sqliteDriver, dsn.String()})
		s.write.SetMaxOpenConns(1)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := startPausedMigration(t, s, ctx, steps, gate)
		cancel()
		// Let the driver's cancellation watcher run while the first statement is still blocked.
		synctest.Wait()
		gate.release()

		if err := awaitLifecycleResult(t, result); !errors.Is(err, context.Canceled) {
			t.Errorf("Migrate during canceled SQL = %v, want context.Canceled", err)
		}
		if secondCalls != 0 {
			t.Errorf("second SQL statement ran %d times, want 0", secondCalls)
		}
		assertBatchIDs(t, s, 1)
		assertMigrationRecords(t, s.Read(), migrationVersion{"items", 1})
	})
}

func startPausedMigration(t *testing.T, s *Store, ctx context.Context, steps []Migration, gate batchTestGate) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- s.Migrate(ctx, steps) }()
	select {
	case <-gate.entered:
		return result
	case err := <-result:
		t.Fatalf("Migrate returned before the SQL pause: %v", err)
	case <-time.After(lifecycleWait):
		t.Fatal("Migrate did not reach the SQL pause")
	}
	return nil
}

type migrationTestConnector struct {
	sqlite *sqlite.Driver
	dsn    string
}

func (c migrationTestConnector) Connect(context.Context) (driver.Conn, error) {
	return c.sqlite.Open(c.dsn)
}

func (c migrationTestConnector) Driver() driver.Driver { return c.sqlite }

type migrationVersion struct {
	component string
	version   int
}

type migrationRecord struct {
	migrationVersion
	appliedAt int64
}

func migrationUpgradeSteps() (initial, upgrade []Migration) {
	initial = []Migration{{Component: "items", Version: 1, SQL: `CREATE TABLE items (id INTEGER PRIMARY KEY)`}}
	upgrade = append(slices.Clone(initial), Migration{Component: "items", Version: 2, SQL: `CREATE TABLE upgraded_items (id INTEGER PRIMARY KEY)`})
	return initial, upgrade
}

func failingFlagsUpgrade() []Migration {
	return []Migration{
		{Component: "flags", Version: 1, SQL: "CREATE TABLE flags (id INTEGER PRIMARY KEY)"},
		{Component: "flags", Version: 2, SQL: "CREATE TABLE upgraded_flags (id INTEGER PRIMARY KEY); INSERT INTO missing_table VALUES (1);"},
	}
}

func mustMigrate(t *testing.T, s *Store, sets ...[]Migration) {
	t.Helper()
	if err := s.Migrate(t.Context(), sets...); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
}

func assertMigrationRecords(t *testing.T, db *sql.DB, want ...migrationVersion) []migrationRecord {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT component, version, applied_at FROM schema_migrations ORDER BY component, version`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close migration rows: %v", err)
		}
	}()
	var records []migrationRecord
	var versions []migrationVersion
	for rows.Next() {
		var record migrationRecord
		if err := rows.Scan(&record.component, &record.version, &record.appliedAt); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
		versions = append(versions, record.migrationVersion)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(versions, want) {
		t.Fatalf("migration versions = %v, want %v", versions, want)
	}
	return records
}

func assertMigrationTable(t *testing.T, db *sql.DB, name string, want bool) {
	t.Helper()
	var exists bool
	if err := db.QueryRowContext(t.Context(), `SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?)`, name).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists != want {
		t.Errorf("table %q exists = %t, want %t", name, exists, want)
	}
}

func assertUpgradeBackup(t *testing.T, db *sql.DB, want string) {
	t.Helper()
	var backup string
	err := db.QueryRowContext(t.Context(), "SELECT backup FROM store_upgrade").Scan(&backup)
	if want == "" {
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("upgrade marker backup = %q, error = %v, want no marker", backup, err)
		}
	} else if err != nil || backup != want {
		t.Fatalf("upgrade marker backup = %q, error = %v, want %q", backup, err, want)
	}
}

func assertMigrationBackupNames(t *testing.T, dir string, want ...string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	if !slices.Equal(names, want) {
		t.Fatalf("backup directory entries = %v, want %v", names, want)
	}
}

func findMigrationSnapshot(t *testing.T, dir string, stamp time.Time) string {
	t.Helper()
	prefix := "yabot-premigrate-" + stamp.UTC().Format("20060102T150405.000Z") + "-"
	pattern := regexp.MustCompile("^" + regexp.QuoteMeta(prefix) + "[0-9a-f]{12}\\.db$")
	names := backupTestEntries(t, dir)
	var matches []string
	for _, name := range names {
		if pattern.MatchString(name) {
			matches = append(matches, name)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("backup directory entries = %v, want one snapshot matching %s", names, pattern)
	}
	return matches[0]
}

func openMigrationSnapshot(t *testing.T, path string) *sql.DB {
	t.Helper()
	dsn := url.URL{Scheme: "file", OmitHost: true, Path: filepath.ToSlash(path), RawQuery: "mode=ro"}
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close migration snapshot: %v", err)
		}
	})
	return db
}

func assertMigrationErrorContains(t *testing.T, err error, parts ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("Migrate succeeded, want an error containing %v", parts)
	}
	for _, part := range parts {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("Migrate error %q does not contain %q", err, part)
		}
	}
}
