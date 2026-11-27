package settings

import (
	"context"
	"crypto/rand"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/i18n"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/VictoriaMetrics/metrics"
)

//go:embed zh-Hans.toml en.toml
var translations embed.FS

type Store interface {
	Write(op store.Op, done func(error)) error
	WriteWait(ctx context.Context, op store.Op) error
	Read() *sql.DB
}

type Janitor interface {
	Register(name string, every time.Duration, fn func(context.Context) (int, error))
}

type Deps struct {
	Store          Store
	Catalog        *i18n.Catalog
	Janitor        Janitor
	AuditRetention time.Duration
	Metrics        *metrics.Set
	Log            *slog.Logger
	Now            func() time.Time
	// ETagRandom defaults to crypto/rand.Reader and must support concurrent use.
	ETagRandom io.Reader
}

type Registry struct {
	store         Store
	catalog       *i18n.Catalog
	log           *slog.Logger
	now           func() time.Time
	random        io.Reader
	metrics       registryMetrics
	patchLogSlots chan struct{}

	registration sync.Mutex
	frozen       atomic.Bool
	specs        []*registeredSpec
	features     map[string]*registeredSpec
	shards       [64]snapshotShard
	deletions    [deletionBuckets]deletionBucket
}

type Handle[T any] struct {
	registry *Registry
	spec     *registeredSpec
}

func New(d Deps) (*Registry, error) {
	if d.Store == nil || d.Catalog == nil || d.Janitor == nil || d.AuditRetention <= 0 {
		return nil, errors.New("settings: store, catalog, janitor and positive audit retention are required")
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.ETagRandom == nil {
		d.ETagRandom = rand.Reader
	}
	if err := d.Catalog.Register("settings", translations); err != nil {
		return nil, fmt.Errorf("register settings translations: %w", err)
	}
	r := &Registry{
		store: d.Store, catalog: d.Catalog, log: d.Log, now: d.Now, random: d.ETagRandom,
		metrics: newRegistryMetrics(d.Metrics), features: make(map[string]*registeredSpec),
	}
	if d.Log != nil {
		r.patchLogSlots = make(chan struct{}, maxPendingPatchLogs)
	}
	d.Janitor.Register("settings_audit", time.Hour, func(ctx context.Context) (int, error) {
		return r.sweepAudit(ctx, d.AuditRetention)
	})
	return r, nil
}

// Register panics on invalid schemas, duplicate features, or registration after Freeze.
func Register[T any](r *Registry, spec Spec) *Handle[T] {
	r.registration.Lock()
	defer r.registration.Unlock()
	if r.frozen.Load() {
		panic("settings: registry is frozen")
	}
	if _, exists := r.features[spec.Feature]; exists {
		panic("settings: feature already registered: " + spec.Feature)
	}
	compiled := compileSpec(reflect.TypeFor[T](), spec)
	r.features[spec.Feature] = compiled
	r.specs = append(r.specs, compiled)
	return &Handle[T]{registry: r, spec: compiled}
}

func (r *Registry) Freeze() {
	r.registration.Lock()
	defer r.registration.Unlock()
	r.frozen.Store(true)
}

func (r *Registry) requireFrozen() {
	if !r.frozen.Load() {
		panic("settings: freeze the registry before use")
	}
}

func (r *Registry) Name() string { return "settings" }

func (r *Registry) Start(ctx context.Context) error {
	r.Freeze()
	return r.Preload(ctx)
}

func (r *Registry) Stop(context.Context) error { return nil }

// PermGroup returns false for an unregistered feature.
func (r *Registry) PermGroup(feature string) (string, bool) {
	r.requireFrozen()
	if spec := r.features[feature]; spec != nil {
		return spec.PermGroup, true
	}
	return "", false
}

// Get returns an independent value without taking a lock.
func (h *Handle[T]) Get(chatID int64) T {
	h.registry.requireFrozen()
	return h.registry.current(h.spec, chatID).value.(T)
}

func (h *Handle[T]) Patch(ctx context.Context, chatID int64, actor Actor, patch map[string]json.RawMessage, ifMatch string) (PatchResult, error) {
	return h.registry.Patch(ctx, h.spec.Feature, chatID, actor, patch, ifMatch)
}
