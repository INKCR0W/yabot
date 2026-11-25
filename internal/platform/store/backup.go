package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"
)

const (
	backupStampLayout  = "20060102T150405.000Z"
	dailyBackupPrefix  = "yabot-"
	preMigrationPrefix = "yabot-premigrate-"
	backupTargetHexLen = 12
)

type backupFiles struct {
	daily        []backupSnapshot
	preMigration []backupSnapshot
}

type backupSnapshot struct {
	name  string
	stamp time.Time
}

// BackupDir returns the absolute path to the store's default backup directory.
func (s *Store) BackupDir() string { return s.backupDir }

// LatestBackup returns the UTC timestamp in the newest published daily snapshot's filename.
// A missing directory or a directory without daily snapshots yields false and a nil error.
func LatestBackup(dir string) (time.Time, bool, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		// On Windows, ReadDir can report ErrNotExist for an existing file.
		info, statErr := os.Stat(dir)
		if errors.Is(statErr, os.ErrNotExist) {
			return time.Time{}, false, nil
		}
		if statErr != nil {
			return time.Time{}, false, fmt.Errorf("check store backup directory: %w", statErr)
		}
		if !info.IsDir() {
			return time.Time{}, false, fmt.Errorf("store backup path %q is not a directory", dir)
		}
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("list store backups: %w", err)
	}
	// ReadDir sorts daily filenames in timestamp order.
	for _, entry := range slices.Backward(entries) {
		if !entry.Type().IsRegular() {
			continue
		}
		stamp, target, ok := parseBackupName(entry.Name())
		if ok && target == "" {
			return stamp, true, nil
		}
	}
	return time.Time{}, false, nil
}

// Backup retains up to keep daily snapshots including the new one and expires unmarked migration snapshots older than keep 24-hour days.
// Calls are serialized, require a positive keep and an existing parent of dir, and should run after migration succeeds to avoid copying an unfinished upgrade marker.
func (s *Store) Backup(ctx context.Context, dir string, keep int) error {
	if keep < 1 {
		return fmt.Errorf("store backup retention must be positive: %d", keep)
	}
	if err := s.beginMaintenance(ctx); err != nil {
		return err
	}
	defer s.endMaintenance()
	start := time.Now()
	if dir == "" {
		return errors.New("store backup directory must not be empty")
	}
	dir, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("resolve store backup directory: %w", err)
	}
	files, err := prepareBackup(dir)
	if err != nil {
		return err
	}
	path, size, err := s.createBackup(ctx, dir, "")
	if err != nil {
		return err
	}
	deleted, pruneErr := pruneBackups(files.daily, dir, filepath.Base(path), keep)
	expired, expireErr := s.pruneExpiredMigrationBackups(ctx, files.preMigration, dir, keep)
	s.log.Info("store backup published", "path", path, "size_bytes", size, "duration", time.Since(start), "deleted", deleted+expired)
	return errors.Join(pruneErr, expireErr)
}

func (s *Store) pruneExpiredMigrationBackups(ctx context.Context, files []backupSnapshot, dir string, keep int) (int, error) {
	// Backup filenames have four-digit years. A longer retention cannot expire a snapshot.
	if len(files) == 0 || keep >= 10_000*366 {
		return 0, nil
	}
	cutoff := s.now().UTC().AddDate(0, 0, -keep)
	if !files[0].stamp.Before(cutoff) {
		return 0, nil
	}
	var hasTable bool
	if err := s.checkpoint.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM main.sqlite_schema WHERE type IN ('table', 'view') AND name = 'store_upgrade' COLLATE NOCASE
	)`).Scan(&hasTable); err != nil {
		return 0, fmt.Errorf("check store upgrade marker table: %w", err)
	}
	var retainedName string
	if hasTable {
		marker, err := readUpgradeMarker(ctx, s.checkpoint)
		if err != nil {
			return 0, err
		}
		if marker != nil {
			retainedName = marker.backup
		}
	}
	deleted := 0
	var errs []error
	for _, file := range files {
		if !file.stamp.Before(cutoff) {
			break
		}
		if file.name == retainedName {
			continue
		}
		if err := os.Remove(filepath.Join(dir, file.name)); err != nil {
			errs = append(errs, fmt.Errorf("remove expired store migration backup: %w", err))
			continue
		}
		deleted++
	}
	return deleted, errors.Join(errs...)
}

func (s *Store) createBackup(ctx context.Context, dir, target string) (string, int64, error) {
	path, size, err := s.publishBackup(ctx, dir, target)
	if err != nil {
		return "", 0, err
	}
	if err := s.syncBackupDirectories(dir); err != nil {
		return "", 0, err
	}
	return path, size, nil
}

func prepareBackupDirectory(dir string) error {
	parent := filepath.Dir(dir)
	info, err := os.Stat(parent)
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("store backup parent directory %q must exist: %w", parent, err)
	}
	if err != nil {
		return fmt.Errorf("check store backup parent directory %q: %w", parent, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("store backup parent path %q is not a directory", parent)
	}
	info, err = os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("create store backup directory %q: %w", dir, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("check store backup directory %q: %w", dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("store backup path %q is not a directory", dir)
	}
	return nil
}

func (s *Store) publishBackup(ctx context.Context, dir, target string) (path string, size int64, err error) {
	path, err = reserveBackupPath(ctx, dir, s.now().UTC(), target)
	if err != nil {
		return "", 0, err
	}
	tmp := path + ".tmp"
	published := false
	defer func() {
		if !published {
			err = errors.Join(err, removeBackupTemp(tmp))
		}
	}()
	// query_only also blocks writes to VACUUM's output. The one-connection pool serializes this with checkpoints.
	if _, err := s.checkpoint.ExecContext(ctx, "VACUUM INTO ?", tmp); err != nil {
		return "", 0, fmt.Errorf("write store backup %q: %w", tmp, err)
	}
	if err := ctx.Err(); err != nil {
		return "", 0, err
	}
	size, err = syncBackupFile(tmp)
	if err != nil {
		return "", 0, fmt.Errorf("sync store backup %q: %w", tmp, err)
	}
	if err := ctx.Err(); err != nil {
		return "", 0, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", 0, fmt.Errorf("publish store backup %q: %w", path, err)
	}
	published = true
	return path, size, nil
}

func reserveBackupPath(ctx context.Context, dir string, stamp time.Time, target string) (string, error) {
	for ; ; stamp = stamp.Add(time.Millisecond) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		timestamp := stamp.Format(backupStampLayout)
		if len(timestamp) != len(backupStampLayout) {
			return "", fmt.Errorf("store backup timestamp is out of range: %v", stamp)
		}
		name := dailyBackupPrefix + timestamp + ".db"
		if target != "" {
			name = preMigrationPrefix + timestamp + "-" + target + ".db"
		}
		path := filepath.Join(dir, name)
		if _, err := os.Lstat(path); err == nil {
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("check store backup path: %w", err)
		}
		tmp := path + ".tmp"
		file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("reserve store backup path: %w", err)
		}
		if err := file.Close(); err != nil {
			return "", errors.Join(fmt.Errorf("close store backup temporary file: %w", err), removeBackupTemp(tmp))
		}
		return path, nil
	}
}

func removeBackupTemp(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove store backup temporary file: %w", err)
	}
	return nil
}

func syncBackupFile(path string) (size int64, err error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	if err := file.Sync(); err != nil {
		return 0, err
	}
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (s *Store) syncBackupDirectories(dir string) error {
	var errs []error
	paths := []string{dir}
	if parent := filepath.Dir(dir); parent != dir {
		paths = append(paths, parent)
	}
	for _, path := range paths {
		if err := s.syncBackupDirectory(path); err != nil {
			errs = append(errs, fmt.Errorf("sync store backup directory %q: %w", path, err))
		}
	}
	return errors.Join(errs...)
}

func (s *Store) syncBackupDirectory(path string) error {
	if s.hooks != nil && s.hooks.syncBackupDirectory != nil {
		return s.hooks.syncBackupDirectory(path)
	}
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func prepareBackup(dir string) (backupFiles, error) {
	if err := prepareBackupDirectory(dir); err != nil {
		return backupFiles{}, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return backupFiles{}, fmt.Errorf("list store backups: %w", err)
	}
	var files backupFiles
	var errs []error
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		name, isTemp := strings.CutSuffix(entry.Name(), ".tmp-journal")
		if !isTemp {
			name, isTemp = strings.CutSuffix(name, ".tmp")
		}
		stamp, target, ok := parseBackupName(name)
		if !ok {
			continue
		}
		if isTemp {
			if err := removeBackupTemp(filepath.Join(dir, entry.Name())); err != nil {
				errs = append(errs, err)
			}
		} else if target != "" {
			files.preMigration = append(files.preMigration, backupSnapshot{name: name, stamp: stamp})
		} else {
			files.daily = append(files.daily, backupSnapshot{name: name, stamp: stamp})
		}
	}
	return files, errors.Join(errs...)
}

func parseBackupName(name string) (stamp time.Time, target string, ok bool) {
	base, ok := strings.CutSuffix(name, ".db")
	if !ok {
		return time.Time{}, "", false
	}
	var timestamp string
	if suffix, isPreMigration := strings.CutPrefix(base, preMigrationPrefix); isPreMigration {
		timestamp, target, ok = strings.Cut(suffix, "-")
		if !ok || len(target) != backupTargetHexLen ||
			strings.IndexFunc(target, func(r rune) bool {
				return !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f')
			}) >= 0 {
			return time.Time{}, "", false
		}
	} else {
		timestamp, ok = strings.CutPrefix(base, dailyBackupPrefix)
		if !ok {
			return time.Time{}, "", false
		}
	}
	stamp, err := time.Parse(backupStampLayout, timestamp)
	return stamp, target, err == nil && stamp.Format(backupStampLayout) == timestamp
}

func pruneBackups(files []backupSnapshot, dir, retainedName string, keep int) (int, error) {
	var errs []error
	deleted := 0
	remaining := keep - 1
	// ReadDir name order preserves the chronological order within each backup type.
	for _, file := range slices.Backward(files) {
		if file.name == retainedName {
			continue
		}
		if remaining > 0 {
			remaining--
			continue
		}
		if err := os.Remove(filepath.Join(dir, file.name)); err != nil {
			errs = append(errs, fmt.Errorf("remove old store backup: %w", err))
			continue
		}
		deleted++
	}
	return deleted, errors.Join(errs...)
}
