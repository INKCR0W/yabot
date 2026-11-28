package chats

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/i18n"
	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/INKCR0W/yabot/internal/tenant/contract"
	"github.com/INKCR0W/yabot/internal/tenant/settings"
)

const SpaceChat dispatch.Space = 1

const (
	ErrBusy              chatError = "chats: capacity exhausted"
	ErrForbidden         chatError = "chats: bot is not in the chat"
	ErrMembershipChanged chatError = "chats: bot membership changed"

	memberRoute = "my_chat_member"
)

type chatError string

func (e chatError) Error() string { return string(e) }

type Store interface {
	Read() *sql.DB
	Write(store.Op, func(error)) error
}

type Dispatcher interface {
	RegisterSpace(string, dispatch.Space)
	OnTick(func(context.Context, *dispatch.Shard))
	Submit(dispatch.Key, dispatch.Task) error
	RunOnAll(context.Context, dispatch.AsyncTask) error
}

type Router interface {
	Register(dispatch.Route)
}

type Sender interface {
	Enqueue(sender.Request) error
}

type Janitor interface {
	Register(string, time.Duration, func(context.Context) (int, error))
}

type Catalog interface {
	Register(string, fs.FS) error
	T(string, string, i18n.Args) string
	HTML(string, string, i18n.Args) string
}

type Deps struct {
	Store            Store
	Dispatcher       Dispatcher
	Router           Router
	Sender           Sender
	Janitor          Janitor
	Catalog          Catalog
	Settings         *settings.Registry
	Log              *slog.Logger
	Metrics          *metrics.Set
	Now              func() time.Time
	BotID            int64
	BotUsername      string
	AcceptNewChats   bool
	RemovedRetention time.Duration
	IdleLeaveAfter   time.Duration
	ProbePeriodic    time.Duration
	ProbePeriodicRPS float64
}

type General struct {
	Language string `json:"language"`
}

type Chats struct {
	view
	maintenance
	store         Store
	dispatch      Dispatcher
	sender        Sender
	catalog       Catalog
	general       *settings.Handle[General]
	log           *slog.Logger
	metrics       *metrics.Set
	now           func() time.Time
	botID         int64
	botUsername   string
	policy        Policy
	probePeriodic time.Duration
	periodicStart periodicProbeGate
	shards        sync.Map
	ready         atomic.Bool
	reports       reportQueue
}

var (
	_ View                = (*Chats)(nil)
	_ Reporter            = (*Chats)(nil)
	_ contract.UserEraser = (*Chats)(nil)
)

//go:embed en.toml zh-Hans.toml
var chatTranslations embed.FS

func New(d Deps) (*Chats, error) {
	if d.Store == nil || d.Dispatcher == nil || d.Router == nil || d.Sender == nil ||
		d.Janitor == nil || d.Catalog == nil || d.Settings == nil {
		return nil, errors.New("chats: store, dispatcher, router, sender, janitor, catalog and settings are required")
	}
	if d.BotID <= 0 || !validUsername(d.BotUsername) {
		return nil, errors.New("chats: a bot ID and username are required")
	}
	removed, idle, periodic := d.RemovedRetention, d.IdleLeaveAfter, d.ProbePeriodic
	if removed == 0 {
		removed = 30 * 24 * time.Hour
	}
	if idle == 0 {
		idle = 7 * 24 * time.Hour
	}
	if periodic == 0 {
		periodic = 24 * time.Hour
	}
	if removed < time.Second || idle < time.Second || periodic < time.Second {
		return nil, errors.New("chats: retention and probe intervals must be at least one second")
	}
	rps := d.ProbePeriodicRPS
	if rps == 0 {
		rps = 0.2
	}
	interval := math.Ceil(float64(time.Second) / rps)
	if !(interval >= 1 && interval < float64(math.MaxInt64)) {
		return nil, errors.New("chats: invalid periodic probe rate")
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Log == nil {
		d.Log = slog.New(slog.DiscardHandler)
	}
	if err := d.Catalog.Register("chats", chatTranslations); err != nil {
		return nil, fmt.Errorf("register chats translations: %w", err)
	}
	c := &Chats{
		store: d.Store, dispatch: d.Dispatcher, sender: d.Sender, catalog: d.Catalog,
		log: d.Log, metrics: d.Metrics, now: d.Now, botID: d.BotID, botUsername: d.BotUsername,
		policy:        Policy{AcceptNewChats: d.AcceptNewChats, RemovedRetention: removed, IdleLeaveAfter: idle},
		probePeriodic: periodic,
		periodicStart: periodicProbeGate{interval: time.Duration(interval)},
		reports:       reportQueue{entries: make(map[reportKey]reportEntry)},
	}
	c.view.init(d.Metrics)
	if d.Metrics != nil {
		c.reports.dropped = d.Metrics.GetOrCreateCounter(`yabot_chats_reports_dropped_total{reason="capacity"}`)
	}
	c.general = registerGeneral(d.Settings)
	d.Dispatcher.RegisterSpace("chat", SpaceChat)
	d.Dispatcher.OnTick(c.tick)
	d.Router.Register(dispatch.Route{
		Name: memberRoute, Kinds: dispatch.KindMyChatMember.Mask(), Critical: true,
		Match: func(u *models.Update) bool {
			_, ok := MemberUpdateOf(u)
			return ok
		},
		Key:    func(u *models.Update) dispatch.Key { return keyOf(u.MyChatMember.Chat.ID) },
		Handle: c.handleMemberUpdate,
	})
	d.Janitor.Register("chats_probe", c.periodicStart.interval, c.periodicProbe)
	d.Janitor.Register("chats_idle", time.Hour, c.idleSweep)
	return c, nil
}

func registerGeneral(registry *settings.Registry) *settings.Handle[General] {
	return settings.Register[General](registry, settings.Spec{
		Feature: "general", Version: 1, PermGroup: "general", Title: "chats.general.title",
		Groups: []settings.GroupMeta{{ID: "general", Title: "chats.general.group"}},
		Fields: []settings.Field{{
			Key: "language", Type: settings.FieldEnum, Group: "general", Label: "chats.general.language",
			Default: json.RawMessage(`"auto"`),
			Options: []settings.Option{
				{Value: "auto", Label: "chats.general.auto"},
				{Value: "zh-Hans", Label: "chats.general.zh_hans"},
				{Value: "en", Label: "chats.general.en"},
			},
		}},
	})
}

func validUsername(name string) bool {
	if name == "" {
		return false
	}
	for _, ch := range name {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_') {
			return false
		}
	}
	return true
}

func keyOf(id int64) dispatch.Key { return dispatch.Key{Space: SpaceChat, A: id} }

func (c *Chats) Name() string { return "chats" }

// Start requires the store and dispatcher to be running and the schema to be migrated.
func (c *Chats) Start(ctx context.Context) error {
	err := readRecords(ctx, c.store.Read(), func(row Record) error {
		return c.submitPreload(ctx, row)
	})
	if err == nil {
		// The barrier is behind all keyed preload tasks in each shard's inbox.
		err = c.dispatch.RunOnAll(ctx, func(_ context.Context, sh *dispatch.Shard, done func(error)) {
			local := c.local(sh)
			for _, entity := range local.records {
				local.epoch++
				entity.state.MemberEpoch = local.epoch
				c.publish(entity)
			}
			done(nil)
		})
	}
	if err != nil {
		c.count("yabot_chats_load_errors_total")
		c.log.Error("chats preload failed", "error", err)
		return fmt.Errorf("preload chats: %w", err)
	}
	c.ready.Store(true)
	return nil
}

func (c *Chats) submitPreload(ctx context.Context, row Record) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := c.dispatch.Submit(keyOf(row.ChatID), func(_ context.Context, sh *dispatch.Shard) {
			c.local(sh).records[row.ChatID] = &chatEntity{state: State{Record: row}}
		})
		if !errors.Is(err, dispatch.ErrBusy) {
			return err
		}
		c.count("yabot_chats_preload_busy_total")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func (c *Chats) Stop(context.Context) error {
	c.ready.Store(false)
	return nil
}
