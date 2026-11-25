package store

import (
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"modernc.org/sqlite"
)

// Migration SQL may contain multiple statements and must leave transaction control to the store.
type Migration struct {
	Component string
	Version   int
	SQL       string
}

type migrationComponent struct {
	name       string
	migrations []Migration
}

const createUpgradeTable = `CREATE TABLE IF NOT EXISTS main.store_upgrade (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	target TEXT NOT NULL,
	backup TEXT NOT NULL,
	started_at INTEGER NOT NULL
)`

type upgradeMarker struct {
	target string
	backup string
}

// Migrate applies registered components between Open and Start, committing each component independently.
func (s *Store) Migrate(ctx context.Context, sets ...[]Migration) error {
	if err := s.beginMaintenance(ctx); err != nil {
		return err
	}
	defer s.endMaintenance()
	if err := s.beginMigration(); err != nil {
		return err
	}
	defer func() {
		s.mu.Lock()
		s.migrating = false
		s.mu.Unlock()
	}()

	components, err := validateMigrations(sets)
	if err != nil {
		return err
	}
	current, hasTable, err := s.migrationVersions(ctx, components)
	if err != nil {
		return err
	}
	needsMigration := slices.ContainsFunc(components, func(component migrationComponent) bool {
		return current[component.name] < len(component.migrations)
	})
	var marker *upgradeMarker
	if hasTable || needsMigration {
		if _, err := s.write.ExecContext(ctx, createUpgradeTable); err != nil {
			return fmt.Errorf("create store upgrade table: %w", err)
		}
		marker, err = s.readUpgradeMarker(ctx)
		if err != nil {
			return err
		}
	}
	if needsMigration {
		if err := s.prepareUpgrade(ctx, migrationTarget(components), marker); err != nil {
			return fmt.Errorf("back up store before migration: %w", err)
		}
	}
	for _, component := range components {
		from := current[component.name]
		if from == len(component.migrations) {
			continue
		}
		if err := s.migrateComponent(ctx, component, from, hasTable); err != nil {
			return err
		}
		hasTable = true
		s.log.Info("store component migrated", "component", component.name, "from_version", from, "to_version", len(component.migrations))
	}
	if marker != nil || needsMigration {
		return s.clearUpgradeMarker(ctx)
	}
	return nil
}

func (s *Store) readUpgradeMarker(ctx context.Context) (*upgradeMarker, error) {
	var marker upgradeMarker
	err := s.write.QueryRowContext(ctx, `SELECT target, backup FROM main.store_upgrade WHERE id = 1`).Scan(&marker.target, &marker.backup)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read store upgrade marker: %w", err)
	}
	return &marker, nil
}

func (s *Store) clearUpgradeMarker(ctx context.Context) error {
	if _, err := s.write.ExecContext(ctx, `DELETE FROM main.store_upgrade WHERE id = 1`); err != nil {
		return fmt.Errorf("clear store upgrade marker: %w", err)
	}
	return nil
}

func (s *Store) prepareUpgrade(ctx context.Context, target string, marker *upgradeMarker) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	start := time.Now()
	dir := s.BackupDir()
	files, err := prepareBackup(dir, true)
	if err != nil {
		return err
	}
	path, err := markedBackupPath(dir, target, marker)
	if err != nil {
		return err
	}
	if path != "" {
		s.log.Info("reusing store backup before migration", "path", path, "target", target)
		_, err := files.prune(dir, marker.backup, s.backupKeep)
		return err
	}
	// Commit the deletion before VACUUM INTO can copy a stale marker into the snapshot.
	if marker != nil {
		if err := s.clearUpgradeMarker(ctx); err != nil {
			return err
		}
	}
	path, size, err := s.createBackup(ctx, dir, target)
	if err != nil {
		return err
	}
	name := filepath.Base(path)
	if _, err := s.write.ExecContext(ctx, `INSERT INTO main.store_upgrade (id, target, backup, started_at)
		VALUES (1, ?, ?, ?)`,
		target, name, time.Now().UnixMilli()); err != nil {
		return fmt.Errorf("record store upgrade marker: %w", err)
	}
	deleted, err := files.prune(dir, name, s.backupKeep)
	s.log.Info("store backup published", "path", path, "size_bytes", size, "duration", time.Since(start), "deleted", deleted)
	return err
}

func markedBackupPath(dir, target string, marker *upgradeMarker) (string, error) {
	if marker == nil || marker.target != target || !filepath.IsLocal(marker.backup) || filepath.Base(marker.backup) != marker.backup {
		return "", nil
	}
	path := filepath.Join(dir, marker.backup)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("check marked store backup %q: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return "", nil
	}
	return path, nil
}

func (s *Store) beginMigration() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isStopping {
		return ErrClosed
	}
	if s.writerDone != nil {
		return errors.New("store migrations must run before Start")
	}
	s.migrating = true
	return nil
}

func validateMigrations(sets [][]Migration) ([]migrationComponent, error) {
	var components []migrationComponent
	positions := make(map[string]int)
	for _, set := range sets {
		firstInSet := len(components)
		for _, migration := range set {
			switch {
			case strings.TrimSpace(migration.Component) == "":
				return nil, errors.New("store migration component must not be empty")
			case migration.Version < 1:
				return nil, fmt.Errorf("store migration %q has invalid version %d", migration.Component, migration.Version)
			case strings.TrimSpace(migration.SQL) == "":
				return nil, fmt.Errorf("store migration %q version %d has empty SQL", migration.Component, migration.Version)
			case strings.ContainsRune(migration.SQL, '\x00'):
				return nil, fmt.Errorf("store migration %q version %d has a NUL byte in SQL", migration.Component, migration.Version)
			}
			position, exists := positions[migration.Component]
			if !exists {
				position = len(components)
				positions[migration.Component] = position
				components = append(components, migrationComponent{name: migration.Component})
			} else if position < firstInSet {
				return nil, fmt.Errorf("store migration component %q is registered in multiple sets", migration.Component)
			}
			components[position].migrations = append(components[position].migrations, migration)
		}
	}
	for _, component := range components {
		slices.SortFunc(component.migrations, func(a, b Migration) int {
			return cmp.Compare(a.Version, b.Version)
		})
		for i, migration := range component.migrations {
			if want := i + 1; migration.Version != want {
				return nil, fmt.Errorf("store migration component %q has version %d, expected %d without gaps or duplicates", component.name, migration.Version, want)
			}
		}
	}
	return components, nil
}

func migrationTarget(components []migrationComponent) string {
	ordered := slices.Clone(components)
	slices.SortFunc(ordered, func(a, b migrationComponent) int {
		return cmp.Compare(a.name, b.name)
	})
	var key []byte
	for _, component := range ordered {
		key = fmt.Appendf(key, "%q:%d\n", component.name, len(component.migrations))
	}
	sum := sha256.Sum256(key)
	return hex.EncodeToString(sum[:backupTargetHexLen/2])
}

func (s *Store) migrationVersions(ctx context.Context, components []migrationComponent) (map[string]int, bool, error) {
	var hasTable bool
	err := s.write.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM main.sqlite_schema WHERE type = 'table' AND name = 'schema_migrations'
	)`).Scan(&hasTable)
	if err != nil {
		return nil, false, fmt.Errorf("check store migration table: %w", err)
	}
	if !hasTable {
		return nil, false, nil
	}
	known := make(map[string]int, len(components))
	for _, component := range components {
		known[component.name] = len(component.migrations)
	}
	rows, err := s.write.QueryContext(ctx, `SELECT component, MAX(version)
		FROM main.schema_migrations GROUP BY component ORDER BY component`)
	if err != nil {
		return nil, true, fmt.Errorf("query store migration versions: %w", err)
	}
	versions, err := readMigrationVersions(rows, known)
	return versions, true, errors.Join(err, rows.Close())
}

func readMigrationVersions(rows *sql.Rows, known map[string]int) (map[string]int, error) {
	versions := make(map[string]int, len(known))
	for rows.Next() {
		var component string
		var version int
		if err := rows.Scan(&component, &version); err != nil {
			return nil, fmt.Errorf("read store migration version: %w", err)
		}
		maxVersion, registered := known[component]
		if !registered || version > maxVersion {
			return nil, fmt.Errorf("store migration component %q has database version %d, binary knows version %d", component, version, maxVersion)
		}
		if version < 1 {
			return nil, fmt.Errorf("store migration component %q has invalid database version %d", component, version)
		}
		versions[component] = version
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read store migration versions: %w", err)
	}
	return versions, nil
}

func (s *Store) migrateComponent(ctx context.Context, component migrationComponent, from int, hasTable bool) (err error) {
	conn, err := s.write.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire writer for store migration %q version %d: %w", component.name, from+1, err)
	}
	cleanupCtx := context.WithoutCancel(ctx)
	var inTransaction, committed bool
	defer func() {
		cleanupErr := finishMigrationConnection(cleanupCtx, conn, inTransaction)
		if cleanupErr != nil {
			cleanupErr = fmt.Errorf("clean up writer after store migration %q through version %d: %w", component.name, len(component.migrations), cleanupErr)
			if !committed && err == nil {
				s.log.Error("store migration cleanup failed during unwinding", "error", cleanupErr)
			}
			err = errors.Join(err, cleanupErr)
		}
	}()
	if err := setMigrationForeignKeys(ctx, conn, false); err != nil {
		return fmt.Errorf("disable foreign keys for store migration %q version %d: %w", component.name, from+1, err)
	}
	// Own the transaction on conn so cancellation cannot race rollback with foreign-key restoration.
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("begin store migration %q version %d: %w", component.name, from+1, err)
	}
	inTransaction = true
	if !hasTable {
		if _, err := conn.ExecContext(ctx, `CREATE TABLE main.schema_migrations (
			component TEXT NOT NULL,
			version INTEGER NOT NULL,
			applied_at INTEGER NOT NULL,
			PRIMARY KEY (component, version)
		)`); err != nil {
			return fmt.Errorf("create store migration table for %q version %d: %w", component.name, from+1, err)
		}
	}
	for _, migration := range component.migrations[from:] {
		if err := executeMigrationSQL(ctx, conn, migration.SQL); err != nil {
			return fmt.Errorf("execute store migration %q version %d: %w", component.name, migration.Version, err)
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO main.schema_migrations (component, version, applied_at) VALUES (?, ?, ?)`,
			component.name, migration.Version, time.Now().UnixMilli()); err != nil {
			return fmt.Errorf("record store migration %q version %d: %w", component.name, migration.Version, err)
		}
	}
	if err := checkMigrationForeignKeys(ctx, conn); err != nil {
		return fmt.Errorf("check foreign keys for store migration %q through version %d: %w", component.name, len(component.migrations), err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("commit store migration %q through version %d: %w", component.name, len(component.migrations), err)
	}
	// Finish a started COMMIT without an interrupt that could obscure its outcome.
	if _, err := conn.ExecContext(cleanupCtx, "COMMIT"); err != nil {
		return fmt.Errorf("commit store migration %q through version %d: %w", component.name, len(component.migrations), err)
	}
	inTransaction = false
	committed = true
	return nil
}

func finishMigrationConnection(ctx context.Context, conn *sql.Conn, inTransaction bool) (err error) {
	defer func() {
		if closeErr := conn.Close(); closeErr != nil && !errors.Is(closeErr, sql.ErrConnDone) {
			err = errors.Join(err, fmt.Errorf("return writer connection: %w", closeErr))
		}
	}()
	if inTransaction {
		if _, rollbackErr := conn.ExecContext(ctx, "ROLLBACK"); rollbackErr != nil && !isNoActiveTransactionError(rollbackErr) {
			err = fmt.Errorf("roll back store migration: %w", rollbackErr)
		}
	}
	if restoreErr := setMigrationForeignKeys(ctx, conn, true); restoreErr != nil {
		err = errors.Join(err, fmt.Errorf("restore writer foreign keys: %w", restoreErr))
	}
	// TEMP objects and attached databases can outlive the migration transaction.
	discardErr := conn.Raw(func(any) error { return driver.ErrBadConn })
	if discardErr != nil && !errors.Is(discardErr, driver.ErrBadConn) && !errors.Is(discardErr, sql.ErrConnDone) {
		err = errors.Join(err, fmt.Errorf("discard writer connection: %w", discardErr))
	}
	return err
}

func setMigrationForeignKeys(ctx context.Context, conn *sql.Conn, enabled bool) error {
	pragma := "PRAGMA foreign_keys = OFF"
	if enabled {
		pragma = "PRAGMA foreign_keys = ON"
	}
	if _, err := conn.ExecContext(ctx, pragma); err != nil {
		return err
	}
	var actual bool
	if err := conn.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&actual); err != nil {
		return err
	}
	if actual != enabled {
		return fmt.Errorf("foreign_keys = %t, want %t", actual, enabled)
	}
	return nil
}

func checkMigrationForeignKeys(ctx context.Context, conn *sql.Conn) (err error) {
	rows, err := conn.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	const maxReported = 5
	var tables []string
	for len(tables) < maxReported && rows.Next() {
		var table, parent string
		var rowID sql.NullInt64
		var foreignKeyID int
		if err := rows.Scan(&table, &rowID, &parent, &foreignKeyID); err != nil {
			return err
		}
		tables = append(tables, table)
	}
	if len(tables) > 0 {
		return errors.Join(fmt.Errorf("foreign key violations in tables %q", tables), rows.Err())
	}
	return rows.Err()
}

func executeMigrationSQL(ctx context.Context, conn *sql.Conn, query string) error {
	return conn.Raw(func(raw any) (err error) {
		sqliteConn, ok := raw.(interface {
			sqlite.AuthorizerRegisterer
			driver.ExecerContext
		})
		if !ok {
			return errors.New("store writer connection does not support migration SQL authorization")
		}
		if err := sqliteConn.RegisterAuthorizer(authorizeMigrationSQL); err != nil {
			return fmt.Errorf("authorize store migration SQL: %w", err)
		}
		// The authorizer also blocks the store's COMMIT and ROLLBACK.
		defer func() {
			if clearErr := sqliteConn.RegisterAuthorizer(nil); clearErr != nil {
				err = errors.Join(err, fmt.Errorf("clear store migration SQL authorizer: %w", clearErr))
			}
		}()
		_, err = sqliteConn.ExecContext(ctx, query, nil)
		return err
	})
}

func authorizeMigrationSQL(action sqlite.AuthorizerActionCode, _, _, _, _ string) sqlite.AuthorizerReturnCode {
	if action == sqlite.AuthTransaction {
		return sqlite.AuthorizerDeny
	}
	return sqlite.AuthorizerOK
}
