package api

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/httpx"
	"github.com/INKCR0W/yabot/internal/platform/i18n"
	"github.com/VictoriaMetrics/metrics"
)

const apiPrefix = "/api/v1"

type Deps struct {
	Server interface {
		Handle(pattern string, h http.Handler)
		SetErrorMessages(func(r *http.Request, code string) string)
	}
	BotToken  string
	MaxAge    time.Duration
	UserRPS   float64
	UserBurst int
	Catalog   *i18n.Catalog
	Metrics   *metrics.Set
	Log       *slog.Logger
	Now       func() time.Time
}

type API struct {
	server interface {
		Handle(pattern string, h http.Handler)
	}
	botToken       string
	maxAge         time.Duration
	catalog        *i18n.Catalog
	log            *slog.Logger
	now            func() time.Time
	users          *httpx.KeyedLimiter[int64]
	authFailures   map[string]*metrics.Counter
	authErrors     *metrics.Counter
	rateLimited    *metrics.Counter
	encodingErrors *metrics.Counter
}

func New(d Deps) (*API, error) {
	switch {
	case d.Server == nil:
		return nil, errors.New("miniapp API requires a route registrar")
	case d.BotToken == "":
		return nil, errors.New("miniapp API requires a bot token")
	case d.MaxAge <= 0:
		return nil, errors.New("miniapp API requires a positive initData maximum age")
	case !(d.UserRPS > 0) || d.UserRPS >= math.MaxFloat64:
		return nil, errors.New("miniapp API user rate must be positive and less than the unlimited rate")
	case d.UserBurst <= 0:
		return nil, errors.New("miniapp API requires a positive user burst")
	case d.Catalog == nil:
		return nil, errors.New("miniapp API requires a translation catalog")
	case d.Metrics == nil:
		return nil, errors.New("miniapp API requires a metrics set")
	case d.Log == nil:
		return nil, errors.New("miniapp API requires a logger")
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	if err := d.Catalog.Register("api", translations); err != nil {
		return nil, fmt.Errorf("register miniapp API translations: %w", err)
	}

	const userLimiterCapacity = 10_000
	evictions := d.Metrics.GetOrCreateCounter("yabot_miniapp_user_limiter_evictions_total")
	failures := make(map[string]*metrics.Counter)
	for _, reason := range [...]string{authMissing, authMalformed, authBadHash, authExpired, authFuture, authNoUser} {
		name := "yabot_miniapp_auth_failures_total{reason=" + strconv.Quote(reason) + "}"
		failures[reason] = d.Metrics.GetOrCreateCounter(name)
	}
	a := &API{
		server:         d.Server,
		botToken:       d.BotToken,
		maxAge:         d.MaxAge,
		catalog:        d.Catalog,
		log:            d.Log,
		now:            d.Now,
		users:          httpx.NewKeyedLimiter[int64](d.UserRPS, d.UserBurst, userLimiterCapacity, evictions.Inc),
		authFailures:   failures,
		authErrors:     d.Metrics.GetOrCreateCounter(`yabot_miniapp_internal_errors_total{reason="auth"}`),
		rateLimited:    d.Metrics.GetOrCreateCounter("yabot_miniapp_rate_limited_total"),
		encodingErrors: d.Metrics.GetOrCreateCounter(`yabot_miniapp_internal_errors_total{reason="encoding"}`),
	}
	d.Server.SetErrorMessages(func(r *http.Request, code string) string {
		return a.errorBody(requestHeaderLanguage(r), code).Message
	})
	a.server.Handle(apiPrefix+"/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.Error(w, r, http.StatusNotFound, CodeNotFound)
	}))
	return a, nil
}

type Route struct {
	Method  string
	Path    string
	Handler http.Handler
}

type Routes interface {
	Routes() []Route
}

// Mount registers paths relative to the API prefix before serving. Invalid or conflicting routes panic.
func (a *API) Mount(r Routes) {
	for _, route := range r.Routes() {
		if route.Method == "" || strings.ContainsAny(route.Method, " \t") {
			panic("miniapp API route requires an HTTP method")
		}
		if !strings.HasPrefix(route.Path, "/") {
			panic("miniapp API route path must begin with a slash")
		}
		if route.Handler == nil {
			panic("miniapp API route requires a handler")
		}
		pattern := route.Method + " " + apiPrefix + route.Path
		a.server.Handle(pattern, a.authenticate(a.limitUser(route.Handler)))
	}
}

func (a *API) limitUser(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, _ := IdentityFrom(r.Context())
		if !a.users.Allow(identity.UserID, a.now()) {
			a.rateLimited.Inc()
			w.Header().Set("Retry-After", "1")
			a.Error(w, r, http.StatusTooManyRequests, CodeRateLimited)
			return
		}
		next.ServeHTTP(w, r)
	})
}
