package settings

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/i18n"
	"github.com/INKCR0W/yabot/internal/platform/obs"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/VictoriaMetrics/metrics"
)

type testJanitor struct {
	name  string
	every time.Duration
	run   func(context.Context) (int, error)
}

func (j *testJanitor) Register(name string, every time.Duration, run func(context.Context) (int, error)) {
	j.name, j.every, j.run = name, every, run
}

type testETagSource struct{ seq atomic.Uint64 }

func (r *testETagSource) Read(p []byte) (int, error) {
	var tag [16]byte
	binary.BigEndian.PutUint64(tag[8:], r.seq.Add(1))
	return copy(p, tag[:]), nil
}

func newTestRegistry(t *testing.T, setup func(*Deps)) (*Registry, *store.Store) {
	t.Helper()
	ctx := t.Context()
	dir := filepath.Join(t.TempDir(), "store")
	if runtime.GOOS == "windows" {
		t.Cleanup(func() {
			deadline := time.Now().Add(2 * time.Second)
			for {
				err := os.RemoveAll(dir)
				if err == nil {
					return
				}
				if time.Now().After(deadline) {
					t.Errorf("remove SQLite test data: %v", err)
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
	set := metrics.NewSet()
	s, err := store.Open(ctx, store.Deps{
		Dir: dir, CacheKiB: 8000, WriteQueue: 256, CheckpointInterval: time.Hour, BackupKeep: 7,
		Metrics: set, Health: obs.NewHealth(), Fatal: func(component string, err error) { t.Errorf("%s fatal: %v", component, err) },
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Stop(context.Background()); err != nil {
			t.Errorf("stop store: %v", err)
		}
	})
	if err := s.Migrate(ctx, Migrations()); err != nil {
		t.Fatalf("migrate settings: %v", err)
	}
	if err := s.Start(ctx); err != nil {
		t.Fatalf("start store: %v", err)
	}
	deps := Deps{Store: s, Catalog: i18n.New(i18n.Deps{}), Janitor: &testJanitor{}, AuditRetention: 90 * 24 * time.Hour, Metrics: set, ETagRandom: &testETagSource{}}
	if setup != nil {
		setup(&deps)
	}
	r, err := New(deps)
	if err != nil {
		t.Fatalf("create settings registry: %v", err)
	}
	return r, s
}

func TestRegisterRejectsDuplicateAndFrozenFeatures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		freeze bool
	}{
		{name: "duplicate feature"},
		{name: "registration after freeze", freeze: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := newTestRegistry(t, nil)
			spec := sampleSpec()
			Register[sampleSettings](r, spec)
			if tc.freeze {
				r.Freeze()
				spec.Feature = "fresh"
			}
			defer func() {
				if recover() == nil {
					t.Error("Register did not panic")
				}
			}()
			Register[sampleSettings](r, spec)
		})
	}
}
