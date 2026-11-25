package janitor

import (
	"context"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/store"
)

// BackupCheckEvery is the interval to use when registering a BackupTask with the janitor.
const BackupCheckEvery = time.Hour

type Backuper interface {
	Backup(ctx context.Context, dir string, keep int) error
	BackupDir() string
}

// BackupTask creates a snapshot when no daily snapshot was published within the last 24 hours.
// It reports 1 when Backup succeeds. A nil clock uses time.Now.
func BackupTask(b Backuper, keep int, now func() time.Time) func(context.Context) (int, error) {
	if now == nil {
		now = time.Now
	}
	return func(ctx context.Context) (int, error) {
		dir := b.BackupDir()
		stamp, found, err := store.LatestBackup(dir)
		if err != nil {
			return 0, err
		}
		// The snapshot is stamped slightly after the check that triggered it; the slack keeps
		// the next day's check from missing the boundary by milliseconds.
		const dailyInterval = 24*time.Hour - time.Minute
		if found && now().Sub(stamp) < dailyInterval {
			return 0, nil
		}
		if err := b.Backup(ctx, dir, keep); err != nil {
			return 0, err
		}
		return 1, nil
	}
}
