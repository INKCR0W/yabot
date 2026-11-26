package dispatch

import (
	"context"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot/models"
)

// Ack.Done may be called from any goroutine. Only the first call takes effect.
type Ack interface{ Done(err error) }

type Route struct {
	Name  string
	Kinds KindMask
	// Match is optional. Match and Key run on the caller of Accept and must return promptly.
	Match    func(*models.Update) bool
	Key      func(*models.Update) Key
	Critical bool
	// Handle runs on the owning shard and must treat u and all its nested data as read-only.
	// Critical handlers must call ack.Done once. The Ack for a noncritical route ignores all calls.
	// Wrap completion callbacks that settle a critical Ack with SettleOnPanic.
	Handle func(ctx context.Context, sh *Shard, u *models.Update, recvAt time.Time, ack Ack)
}

// RouterDeps configures Router-specific logging and metrics.
// The Dispatcher owns panic logs and counters, including those for Match and Key.
type RouterDeps struct {
	Log     *slog.Logger
	Metrics *metrics.Set
}

type registeredRoute struct {
	Route
	ackLog     *slog.Logger
	dropped    *metrics.Counter
	duplicates *metrics.Counter
}

type Router struct {
	d        *Dispatcher
	deps     RouterDeps
	unrouted *metrics.Counter

	mu     sync.Mutex
	frozen bool
	names  map[string]struct{}
	kinds  KindMask
	routes [kindCount][]*registeredRoute
}

// NewRouter panics on a nil Dispatcher. A nil Log uses the Dispatcher's logger.
func NewRouter(d *Dispatcher, deps RouterDeps) *Router {
	if d == nil {
		panic("dispatch: router requires a dispatcher")
	}
	if deps.Log == nil {
		deps.Log = d.deps.Log
	}
	r := &Router{d: d, deps: deps, names: make(map[string]struct{})}
	if deps.Metrics != nil {
		r.unrouted = deps.Metrics.GetOrCreateCounter("yabot_dispatch_unrouted_total")
	}
	return r
}

// Register is valid until the first call to Accept or AllowedUpdates.
func (r *Router) Register(rt Route) {
	r.mu.Lock()
	defer r.mu.Unlock()

	const knownKinds = KindMask(1<<kindCount - 1)
	_, duplicate := r.names[rt.Name]
	switch {
	case r.frozen:
		panic("dispatch: routes must be registered before Accept or AllowedUpdates")
	case rt.Name == "" || rt.Name == "none":
		// "none" labels panics of tasks submitted without a route.
		panic("dispatch: route name must not be empty or \"none\"")
	case duplicate:
		panic("dispatch: duplicate route name: " + rt.Name)
	case rt.Kinds == 0 || rt.Kinds&^knownKinds != 0:
		panic("dispatch: invalid kinds for route " + rt.Name)
	case rt.Key == nil || rt.Handle == nil:
		panic("dispatch: route requires Key and Handle: " + rt.Name)
	}

	entry := &registeredRoute{Route: rt, ackLog: r.deps.Log}
	// Expose a zero-valued panic series before the route sees any work.
	r.d.panicCounter(rt.Name)
	if set := r.deps.Metrics; set != nil {
		label := routeMetricLabel(rt.Name)
		if rt.Critical {
			entry.duplicates = set.GetOrCreateCounter("yabot_dispatch_ack_duplicate_total" + label)
		} else {
			entry.dropped = set.GetOrCreateCounter("yabot_dispatch_route_dropped_total" + label)
		}
	}
	r.names[rt.Name] = struct{}{}
	r.kinds |= rt.Kinds
	for kind := range kindCount {
		if rt.Kinds&kind.Mask() != 0 {
			r.routes[kind] = append(r.routes[kind], entry)
		}
	}
}

func (r *Router) freeze() {
	r.mu.Lock()
	r.frozen = true
	r.mu.Unlock()
}

// AllowedUpdates returns a non-nil slice for explicit inclusion in getUpdates requests.
func (r *Router) AllowedUpdates() []string {
	r.freeze()
	allowed := make([]string, 0)
	for kind := range kindCount {
		if r.kinds&kind.Mask() != 0 {
			allowed = append(allowed, kind.String())
		}
	}
	return allowed
}

// Accept forwards u and recvAt unchanged without waiting for handlers. The caller must not
// mutate or reuse u or its nested data from this call onward, even after the receipt settles.
func (r *Router) Accept(u *models.Update, recvAt time.Time) Receipt {
	r.freeze()
	var state *receiptState
	routed := false
	if kind, ok := KindOf(u); ok {
		for _, rt := range r.routes[kind] {
			key, matched, panicErr := rt.keyFor(u)
			if !matched && panicErr == nil {
				continue
			}
			routed = true
			var ack *routeAck
			if rt.Critical {
				if state == nil {
					state = newReceiptState()
				}
				ack = state.add(rt, u.ID)
			}
			if panicErr != nil {
				r.d.reportRoutingPanic(rt.Name, u.ID, panicErr)
				rt.deliveryFailed(ack, panicErr)
				continue
			}
			r.submit(rt, key, u, recvAt, ack)
		}
	}
	if !routed && r.unrouted != nil {
		r.unrouted.Inc()
	}
	if state != nil {
		state.finishRouting()
	}
	return Receipt{state: state}
}

func (rt *registeredRoute) keyFor(u *models.Update) (key Key, matched bool, err *PanicError) {
	returned := false
	defer func() {
		if !returned {
			err = &PanicError{Shard: ingressPanicShard, Value: recover(), Stack: debug.Stack()}
		}
	}()
	if rt.Match == nil || rt.Match(u) {
		key, matched = rt.Key(u), true
	}
	returned = true
	return
}

func (r *Router) submit(rt *registeredRoute, key Key, u *models.Update, recvAt time.Time, ack *routeAck) {
	var handlerAck Ack = discardAck{}
	var onPanic func(error)
	if ack != nil {
		handlerAck = ack
		onPanic = func(err error) { ack.complete(err) }
	}
	task := func(ctx context.Context, sh *Shard) {
		rt.Handle(ctx, sh, u, recvAt, handlerAck)
	}
	if err := r.d.submit(key, rt.Name, task, onPanic); err != nil {
		rt.deliveryFailed(ack, err)
		if ack == nil {
			r.deps.Log.Info("dispatch dropped noncritical route", "route", rt.Name, "update_id", u.ID, "error", err)
		}
	}
}

func (rt *registeredRoute) deliveryFailed(ack *routeAck, err error) {
	if ack != nil {
		ack.complete(err)
	} else if rt.dropped != nil {
		rt.dropped.Inc()
	}
}

type discardAck struct{}

func (discardAck) Done(error) {}
