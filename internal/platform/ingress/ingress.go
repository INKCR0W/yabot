package ingress

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot/models"
)

const (
	BatchLimit     = 100
	DedupeCapacity = 4096

	pollRequestGrace = 10 * time.Second
	confirmTimeout   = 2 * time.Second
)

type Source interface {
	// GetUpdates returns an ordered batch whose updates won't be mutated or reused by the source.
	GetUpdates(ctx context.Context, offset int64, limit int, timeout time.Duration, allowed []string) ([]models.Update, error)
	DeleteWebhook(ctx context.Context) error
}

type Router interface {
	// Receipt error text is logged and stored in dead_letter, so it must not carry user data such as bios.
	Accept(u *models.Update, recvAt time.Time) dispatch.Receipt
	AllowedUpdates() []string
}

type Writer interface {
	WriteWait(ctx context.Context, op store.Op) error
}

type Skew interface {
	Age(recvWall time.Time, tgDate int64) time.Duration
}

type Deps struct {
	Source          Source
	Router          Router
	Store           Writer
	Skew            Skew
	Fatal           func(component string, err error)
	PollTimeout     time.Duration
	SettleTimeout   time.Duration
	TransientBudget time.Duration
	PoisonThreshold int
	Log             *slog.Logger
	Metrics         *metrics.Set
	Now             func() time.Time
	RetryDelay      time.Duration
}

type Ingress struct {
	deps    Deps
	metrics ingressMetrics

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func New(deps Deps) (*Ingress, error) {
	switch {
	case isNil(deps.Source):
		return nil, errors.New("ingress: source is required")
	case isNil(deps.Router):
		return nil, errors.New("ingress: router is required")
	case isNil(deps.Store):
		return nil, errors.New("ingress: store is required")
	case isNil(deps.Skew):
		return nil, errors.New("ingress: skew estimator is required")
	case deps.Fatal == nil:
		return nil, errors.New("ingress: fatal callback is required")
	case deps.PollTimeout <= 0:
		return nil, errors.New("ingress: poll timeout must be positive")
	case deps.SettleTimeout <= 0:
		return nil, errors.New("ingress: settle timeout must be positive")
	case deps.TransientBudget <= 0:
		return nil, errors.New("ingress: transient budget must be positive")
	case deps.PoisonThreshold <= 0:
		return nil, errors.New("ingress: poison threshold must be positive")
	case deps.RetryDelay < 0:
		return nil, errors.New("ingress: retry delay must not be negative")
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.Log == nil {
		deps.Log = slog.New(slog.DiscardHandler)
	}
	if deps.RetryDelay == 0 {
		deps.RetryDelay = time.Second
	}
	return &Ingress{deps: deps, metrics: newIngressMetrics(deps.Metrics)}, nil
}

func isNil(dependency any) bool {
	value := reflect.ValueOf(dependency)
	switch value.Kind() {
	case reflect.Invalid:
		return true
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (*Ingress) Name() string { return "ingress" }

// Start launches polling once. Polling runs independently of ctx until Stop or until it reports Fatal and exits.
func (i *Ingress) Start(ctx context.Context) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.done != nil {
		return errors.New("ingress: already started")
	}
	allowed := append([]string{}, i.deps.Router.AllowedUpdates()...)
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	i.cancel = cancel
	i.done = make(chan struct{})
	go func() {
		defer close(i.done)
		defer cancel()
		i.run(runCtx, allowed)
	}()
	return nil
}

// Stop cancels polling and waits for any received batch. A timed-out Stop can be retried.
func (i *Ingress) Stop(ctx context.Context) error {
	i.mu.Lock()
	cancel, done := i.cancel, i.done
	i.mu.Unlock()
	if done == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (i *Ingress) run(ctx context.Context, allowed []string) {
	if !i.deleteWebhook(ctx) {
		return
	}
	state := newBatchState()
	var offset int64
	var needsConfirmation bool
	const maxDuration = time.Duration(1<<63 - 1)
	requestTimeout := min(i.deps.PollTimeout, maxDuration-pollRequestGrace) + pollRequestGrace
	for ctx.Err() == nil {
		requestCtx, cancel := context.WithTimeout(ctx, requestTimeout)
		updates, err := i.deps.Source.GetUpdates(requestCtx, offset, BatchLimit, i.deps.PollTimeout, allowed)
		received := time.Now()
		cancel()
		if err != nil {
			outcome := telegram.Classify(err)
			if outcome.Class == telegram.Conflict || outcome.Class == telegram.Unauthorized {
				i.deps.Fatal(i.Name(), err)
				return
			}
			if ctx.Err() != nil {
				break
			}
			waitRetry(ctx, i.logPollError(ctx, err, outcome))
			continue
		}

		// Only a completed request is known to have confirmed the offset with Telegram.
		needsConfirmation = false
		i.metrics.observeBatch(updates, i.deps.Now, i.deps.Skew)
		if len(updates) == 0 {
			continue
		}
		ok, err := i.settleBatch(ctx, state, updates, received)
		if i.metrics.settleSeconds != nil {
			i.metrics.settleSeconds.Update(time.Since(received).Seconds())
		}
		if err != nil {
			i.deps.Fatal(i.Name(), err)
			return
		}
		if !ok {
			waitRetry(ctx, i.deps.RetryDelay)
			continue
		}
		offset = updates[len(updates)-1].ID + 1
		state.prune(offset)
		needsConfirmation = true
	}
	if needsConfirmation {
		i.confirmOffset(ctx, offset, allowed)
	}
}

func (i *Ingress) deleteWebhook(ctx context.Context) bool {
	for ctx.Err() == nil {
		requestCtx, cancel := context.WithTimeout(ctx, pollRequestGrace)
		err := i.deps.Source.DeleteWebhook(requestCtx)
		cancel()
		if err == nil {
			return true
		}
		outcome := telegram.Classify(err)
		if outcome.Class == telegram.Unauthorized {
			i.deps.Fatal(i.Name(), err)
			return false
		}
		if ctx.Err() != nil {
			return false
		}
		delay := i.deps.RetryDelay
		level := slog.LevelError
		if outcome.Class == telegram.Retry {
			delay = max(delay, outcome.RetryAfter)
			if i.metrics.webhookRetries != nil {
				i.metrics.webhookRetries.Inc()
			}
			level = slog.LevelInfo
		}
		i.deps.Log.Log(ctx, level, "retry deleting Telegram webhook", "class", outcome.Class, "retry_delay", delay, "error", err)
		waitRetry(ctx, delay)
	}
	return false
}

func (i *Ingress) logPollError(ctx context.Context, err error, outcome telegram.Outcome) time.Duration {
	delay := i.deps.RetryDelay
	if outcome.Class == telegram.Retry {
		delay = max(delay, outcome.RetryAfter)
		if i.metrics.pollRetries != nil {
			i.metrics.pollRetries.Inc()
		}
		i.deps.Log.InfoContext(ctx, "retry Telegram getUpdates", "retry_delay", delay, "error", err)
	} else {
		i.deps.Log.ErrorContext(ctx, "Telegram getUpdates failed", "class", outcome.Class, "error", err)
	}
	return delay
}

func (i *Ingress) confirmOffset(ctx context.Context, offset int64, allowed []string) {
	confirmCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), confirmTimeout)
	defer cancel()
	if _, err := i.deps.Source.GetUpdates(confirmCtx, offset, 1, 0, allowed); err != nil {
		i.deps.Log.InfoContext(confirmCtx, "could not confirm Telegram offset on shutdown", "offset", offset, "error", err)
	}
}

func waitRetry(ctx context.Context, delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}
