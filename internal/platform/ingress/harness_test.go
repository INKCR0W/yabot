package ingress

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/store"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot/models"
)

const (
	testBotToken = "12345:test-secret-token"
	testWait     = 5 * time.Second
)

type fatalCall struct {
	component string
	err       error
}

type pollHarness struct {
	deps    Deps
	router  *dispatch.Router
	metrics *metrics.Set
	calls   <-chan *botCall
	fatals  <-chan fatalCall
}

type botCall struct {
	method      string
	offset      int64
	limit       int
	timeout     int64
	allowed     []string
	dropPending bool
	at          time.Time
	reply       chan botReply
}

type botReply struct {
	status    int
	body      string
	truncated bool
}

func (call *botCall) ok(result string) {
	call.respond(http.StatusOK, `{"ok":true,"result":`+result+`}`, false)
}

func (call *botCall) respond(status int, body string, truncated bool) {
	call.reply <- botReply{status: status, body: body, truncated: truncated}
}

func newPollHarness(t *testing.T) *pollHarness {
	t.Helper()
	router := newTestRouter(t)
	calls := make(chan *botCall, 16)
	fatals := make(chan fatalCall, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveBotCall(t, w, r, calls)
	}))
	t.Cleanup(server.Close)
	skew := telegram.NewTimeSkew(nil, nil)
	client, err := telegram.New(telegram.Config{Token: testBotToken, ServerURL: server.URL, Skew: skew})
	if err != nil {
		t.Fatalf("create Telegram client: %v", err)
	}
	set := metrics.NewSet()
	deps := testDeps(TelegramSource(client), router, skew, fatals)
	deps.Metrics = set
	return &pollHarness{deps: deps, router: router, metrics: set, calls: calls, fatals: fatals}
}

func serveBotCall(t *testing.T, w http.ResponseWriter, r *http.Request, calls chan<- *botCall) {
	t.Helper()
	method, ok := strings.CutPrefix(r.URL.Path, "/bot"+testBotToken+"/")
	if !ok || r.Method != http.MethodPost {
		t.Error("unexpected Telegram request")
		http.NotFound(w, r)
		return
	}
	call := &botCall{method: method, at: time.Now(), reply: make(chan botReply, 1)}
	switch method {
	case "deleteWebhook":
		if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
			if err := r.ParseMultipartForm(1024); err != nil {
				t.Errorf("parse deleteWebhook request: %v", err)
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
		}
		if raw := r.FormValue("drop_pending_updates"); raw != "" {
			var err error
			call.dropPending, err = strconv.ParseBool(raw)
			if err != nil {
				t.Errorf("parse drop_pending_updates: %v", err)
			}
		}
	case "getUpdates":
		var params struct {
			Offset  int64    `json:"offset"`
			Limit   int      `json:"limit"`
			Timeout int64    `json:"timeout"`
			Allowed []string `json:"allowed_updates"`
		}
		if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
			t.Errorf("parse getUpdates request: %v", err)
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		call.offset, call.limit, call.timeout, call.allowed = params.Offset, params.Limit, params.Timeout, params.Allowed
	default:
		t.Errorf("unexpected Telegram method %q", method)
		http.NotFound(w, r)
		return
	}

	select {
	case calls <- call:
	case <-r.Context().Done():
		return
	}
	select {
	case reply := <-call.reply:
		w.Header().Set("Content-Type", "application/json")
		if reply.truncated {
			w.Header().Set("Content-Length", strconv.Itoa(len(reply.body)+32))
			w.Header().Set("Connection", "close")
		}
		w.WriteHeader(reply.status)
		if _, err := w.Write([]byte(reply.body)); err != nil && r.Context().Err() == nil {
			t.Errorf("send Telegram response: %v", err)
		}
	case <-r.Context().Done():
	}
}

func (h *pollHarness) start(t *testing.T) *Ingress {
	t.Helper()
	i := startTestIngress(t, h.deps)
	// Registered after startTestIngress, so it runs first and answers any shutdown confirmation.
	t.Cleanup(func() { h.stop(t, i) })
	return i
}

// stop answers the shutdown offset confirmation, which is the only request allowed during Stop.
func (h *pollHarness) stop(t *testing.T, i *Ingress) {
	t.Helper()
	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		defer cancel()
		stopped <- i.Stop(ctx)
	}()
	for {
		select {
		case err := <-stopped:
			if err != nil {
				t.Errorf("Stop: %v", err)
			}
			return
		case call := <-h.calls:
			if call.method != "getUpdates" || call.limit != 1 || call.timeout != 0 {
				t.Errorf("unexpected request during Stop: %s limit %d timeout %d", call.method, call.limit, call.timeout)
			}
			call.ok(`[]`)
		}
	}
}

func (h *pollHarness) next(t *testing.T, method string) *botCall {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testWait)
	defer cancel()
	select {
	case call := <-h.calls:
		if call.method != method {
			t.Fatalf("Telegram method = %s, want %s", call.method, method)
		}
		return call
	case fatal := <-h.fatals:
		t.Fatalf("received Fatal(%s, %v), want %s", fatal.component, fatal.err, method)
	case <-ctx.Done():
		t.Fatalf("waiting for Telegram %s: %v", method, ctx.Err())
	}
	return nil
}

func (h *pollHarness) webhookOK(t *testing.T) {
	t.Helper()
	call := h.next(t, "deleteWebhook")
	if call.dropPending {
		t.Error("deleteWebhook requested dropping pending updates")
	}
	call.ok("true")
}

func (h *pollHarness) awaitExit(t *testing.T, i *Ingress) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testWait)
	defer cancel()
	select {
	case <-i.done:
		assertPollExit(t, h)
	case call := <-h.calls:
		t.Fatalf("unexpected %s with offset %d while waiting for ingress to exit", call.method, call.offset)
	case <-ctx.Done():
		t.Fatal("ingress did not exit on its own")
	}
}

func newTestRouter(t *testing.T) *dispatch.Router {
	t.Helper()
	d, err := dispatch.New(dispatch.Deps{Shards: 1, Inbox: 512, TicketsPerShard: 4, MailboxPerEntity: 4})
	if err != nil {
		t.Fatalf("create dispatcher: %v", err)
	}
	d.RegisterSpace("ingress test", 1)
	if err := d.Start(t.Context()); err != nil {
		t.Fatalf("start dispatcher: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		defer cancel()
		if err := d.Stop(ctx); err != nil {
			t.Errorf("stop dispatcher: %v", err)
		}
	})
	return dispatch.NewRouter(d, dispatch.RouterDeps{})
}

func registerRoute(r *dispatch.Router, name string, kinds dispatch.KindMask, critical bool, handle func(*models.Update, dispatch.Ack)) {
	r.Register(dispatch.Route{
		Name:     name,
		Kinds:    kinds,
		Critical: critical,
		Key:      func(*models.Update) dispatch.Key { return dispatch.Key{Space: 1} },
		Handle: func(_ context.Context, _ *dispatch.Shard, u *models.Update, _ time.Time, ack dispatch.Ack) {
			handle(u, ack)
		},
	})
}

type testWriter func(context.Context, store.Op) error

func (w testWriter) WriteWait(ctx context.Context, op store.Op) error { return w(ctx, op) }

func testDeps(source Source, router Router, skew Skew, fatals chan<- fatalCall) Deps {
	return Deps{
		Source: source,
		Router: router,
		Store: testWriter(func(context.Context, store.Op) error {
			return &store.OpError{Err: errors.New("unexpected store write")}
		}),
		Skew:            skew,
		Fatal:           func(name string, err error) { fatals <- fatalCall{component: name, err: err} },
		PollTimeout:     25 * time.Second,
		SettleTimeout:   2 * time.Second,
		TransientBudget: 2 * time.Minute,
		PoisonThreshold: 3,
		RetryDelay:      time.Millisecond,
	}
}

func startTestIngress(t *testing.T, deps Deps) *Ingress {
	t.Helper()
	ingress, err := New(deps)
	if err != nil {
		t.Fatalf("create ingress: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	err = ingress.Start(ctx)
	cancel()
	if err != nil {
		t.Fatalf("start ingress: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		defer cancel()
		if err := ingress.Stop(ctx); err != nil {
			t.Errorf("stop ingress: %v", err)
		}
	})
	return ingress
}

func awaitValue[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testWait)
	defer cancel()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatalf("waiting for channel value: %v", ctx.Err())
		var zero T
		return zero
	}
}
