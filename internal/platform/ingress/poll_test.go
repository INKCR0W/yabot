package ingress

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
	"github.com/VictoriaMetrics/metrics"
	"github.com/go-telegram/bot/models"
)

func TestPollSettlesWholeBatch(t *testing.T) {
	h := newPollHarness(t)
	type delivery struct {
		id  int64
		ack dispatch.Ack
	}
	deliveries := make(chan delivery, 16)
	record := func(u *models.Update, ack dispatch.Ack) {
		deliveries <- delivery{u.ID, ack}
	}
	registerRoute(h.router, "join", dispatch.KindChatJoinRequest.Mask(), true, record)
	registerRoute(h.router, "member", dispatch.KindMyChatMember.Mask(), false, record)

	i := h.start(t)
	if err := i.Start(t.Context()); err == nil {
		t.Fatal("second Start succeeded")
	}
	h.webhookOK(t)
	first := h.next(t, "getUpdates")
	wantAllowed := []string{"chat_join_request", "my_chat_member"}
	assertPollRequest(t, first, 0, BatchLimit, int64(h.deps.PollTimeout/time.Second), wantAllowed)

	const batch = `[{"update_id":10,"chat_join_request":{"date":100}},
		{"update_id":11,"my_chat_member":{"date":100}},
		{"update_id":12,"chat_join_request":{"date":100}},
		{"update_id":13,"chat_join_request":{"date":100}}]`
	first.ok(batch)
	delivered := func(wantID int64) dispatch.Ack {
		t.Helper()
		select {
		case got := <-deliveries:
			if got.id != wantID {
				t.Fatalf("delivered update %d, want %d", got.id, wantID)
			}
			return got.ack
		case call := <-h.calls:
			t.Fatalf("requested %s with offset %d before delivery of update %d", call.method, call.offset, wantID)
		case <-time.After(testWait):
			t.Fatalf("no delivery for update %d", wantID)
		}
		return nil
	}
	delivered(10).Done(nil)
	delivered(11)
	pending := delivered(12)
	delivered(13).Done(nil)
	select {
	case call := <-h.calls:
		t.Fatalf("requested %s with offset %d while the ack for update 12 was held", call.method, call.offset)
	default:
	}
	released := time.Now()
	pending.Done(errors.New("retry update"))

	retry := h.next(t, "getUpdates")
	if retry.at.Before(released) {
		t.Fatal("requested another batch before the last ack was released")
	}
	assertPollRequest(t, retry, 0, BatchLimit, int64(h.deps.PollTimeout/time.Second), wantAllowed)
	retry.ok(batch)
	delivered(12).Done(nil)
	select {
	case got := <-deliveries:
		t.Fatalf("redelivered settled update %d", got.id)
	case call := <-h.calls:
		if call.method != "getUpdates" {
			t.Fatalf("Telegram method = %s after retry, want getUpdates", call.method)
		}
		assertPollRequest(t, call, 14, BatchLimit, int64(h.deps.PollTimeout/time.Second), wantAllowed)
	case fatal := <-h.fatals:
		t.Fatalf("received Fatal(%s, %v) after retry", fatal.component, fatal.err)
	case <-time.After(testWait):
		t.Fatal("no poll after the retried update settled")
	}
	// The canceled long poll may not have reached Telegram, so Stop confirms the offset again.
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		defer cancel()
		if err := i.Stop(ctx); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()
	confirmation := h.next(t, "getUpdates")
	assertPollRequest(t, confirmation, 14, 1, 0, wantAllowed)
	confirmation.ok(`[]`)
	<-stopped
	assertPollExit(t, h)
}

func TestPollStopWaitsAndConfirmsOffset(t *testing.T) {
	h := newPollHarness(t)
	acks := make(chan dispatch.Ack, 4)
	matched := make(chan int64, 4)
	h.router.Register(dispatch.Route{
		Name:     "join",
		Kinds:    dispatch.KindChatJoinRequest.Mask(),
		Critical: true,
		Match: func(u *models.Update) bool {
			matched <- u.ID
			return true
		},
		Key: func(*models.Update) dispatch.Key { return dispatch.Key{} },
		Handle: func(_ context.Context, _ *dispatch.Shard, _ *models.Update, ack dispatch.Ack) {
			acks <- ack
		},
	})

	i := h.start(t)
	h.webhookOK(t)
	h.next(t, "getUpdates").ok(`[{"update_id":30,"chat_join_request":{"date":100}},{"update_id":31,"chat_join_request":{"date":100}}]`)
	awaitValue(t, acks).Done(nil)
	last := awaitValue(t, acks)
	expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	if err := i.Stop(expired); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop with an expired context and pending ack = %v", err)
	}
	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		defer cancel()
		stopped <- i.Stop(ctx)
	}()
	last.Done(nil)

	confirmation := h.next(t, "getUpdates")
	assertPollRequest(t, confirmation, 32, 1, 0, []string{"chat_join_request"})
	select {
	case err := <-stopped:
		t.Fatalf("Stop returned %v before the offset confirmation completed", err)
	default:
	}
	if err := i.Stop(expired); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop with an expired context and pending confirmation = %v", err)
	}
	confirmation.ok(`[{"update_id":32,"chat_join_request":{"date":100}}]`)
	if err := awaitValue(t, stopped); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	h.stop(t, i)
	h.stop(t, i)

	for _, want := range []int64{30, 31} {
		if got := awaitValue(t, matched); got != want {
			t.Fatalf("matched update %d, want %d", got, want)
		}
	}
	select {
	case id := <-matched:
		t.Fatalf("routed update %d from the confirmation response", id)
	default:
	}
	assertPollExit(t, h)
}

func TestPollStopAfterEmptyResponse(t *testing.T) {
	h := newPollHarness(t)
	i := h.start(t)
	h.webhookOK(t)
	h.next(t, "getUpdates").ok(`[{"update_id":70}]`)
	poll := h.next(t, "getUpdates")
	assertPollRequest(t, poll, 71, BatchLimit, int64(h.deps.PollTimeout/time.Second), []string{})
	poll.ok(`[]`)
	assertPollRequest(t, h.next(t, "getUpdates"), 71, BatchLimit, int64(h.deps.PollTimeout/time.Second), []string{})

	ctx, cancel := context.WithTimeout(t.Context(), testWait)
	defer cancel()
	if err := i.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	assertPollExit(t, h)
}

func TestPollFatalResponses(t *testing.T) {
	for _, tc := range []struct {
		name, method string
		status       int
		truncated    bool
		wantClass    telegram.Class
	}{
		{"conflict", "getUpdates", http.StatusConflict, false, telegram.Conflict},
		{"unauthorized", "getUpdates", http.StatusUnauthorized, false, telegram.Unauthorized},
		{"truncated conflict", "getUpdates", http.StatusConflict, true, telegram.Conflict},
		{"truncated unauthorized", "getUpdates", http.StatusUnauthorized, true, telegram.Unauthorized},
		{"webhook unauthorized", "deleteWebhook", http.StatusUnauthorized, false, telegram.Unauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newPollHarness(t)
			i := h.start(t)
			if tc.method == "getUpdates" {
				h.webhookOK(t)
			}
			body := fmt.Sprintf(`{"ok":false,"error_code":%d,"description":%q}`, tc.status, http.StatusText(tc.status)+" "+testBotToken)
			if tc.truncated {
				body = `{"ok":`
			}
			h.next(t, tc.method).respond(tc.status, body, tc.truncated)

			select {
			case got := <-h.fatals:
				if got.component != "ingress" || telegram.Classify(got.err).Class != tc.wantClass {
					t.Fatalf("Fatal(%q, %v), want ingress with class %v", got.component, got.err, tc.wantClass)
				}
				if strings.Contains(got.err.Error(), testBotToken) {
					t.Fatal("fatal error contains the bot token")
				}
				if tc.method == "deleteWebhook" &&
					(!strings.Contains(got.err.Error(), "401") || !strings.Contains(got.err.Error(), "Unauthorized")) {
					t.Errorf("Fatal error = %q, want error code and description", got.err)
				}
			case call := <-h.calls:
				t.Fatalf("requested %s with offset %d after status %d", call.method, call.offset, tc.status)
			case <-time.After(testWait):
				t.Fatalf("no Fatal after status %d", tc.status)
			}
			h.awaitExit(t, i)
		})
	}
}

func TestPollRetryResponses(t *testing.T) {
	h := newPollHarness(t)
	h.deps.RetryDelay = 20 * time.Millisecond
	h.start(t)
	h.webhookOK(t)
	poll := h.next(t, "getUpdates")
	for _, tc := range []struct {
		status  int
		body    string
		minWait time.Duration
	}{
		{
			http.StatusTooManyRequests,
			`{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":1}}`,
			time.Second,
		},
		{
			http.StatusBadGateway,
			`{"ok":false,"error_code":502,"description":"Bad Gateway"}`,
			h.deps.RetryDelay,
		},
	} {
		sentAt := time.Now()
		poll.respond(tc.status, tc.body, false)
		poll = h.next(t, "getUpdates")
		if elapsed := poll.at.Sub(sentAt); elapsed < tc.minWait || poll.offset != 0 {
			t.Errorf("retry after HTTP %d used offset %d after %v, want offset 0 after at least %v",
				tc.status, poll.offset, elapsed, tc.minWait)
		}
	}
	poll.ok(`[{"update_id":40}]`)
	if got := h.next(t, "getUpdates").offset; got != 41 {
		t.Fatalf("offset after successful retry = %d, want 41", got)
	}
}

func TestPollStartupRetriesUnsuccessfulWebhook(t *testing.T) {
	h := newPollHarness(t)
	h.deps.RetryDelay = 20 * time.Millisecond
	h.start(t)
	call := h.next(t, "deleteWebhook")
	started := time.Now()
	call.ok("false")
	call = h.next(t, "deleteWebhook")
	if elapsed := call.at.Sub(started); elapsed < h.deps.RetryDelay {
		t.Errorf("webhook retry took %v, want at least %v", elapsed, h.deps.RetryDelay)
	}
	call.respond(http.StatusConflict, `{"ok":false,"error_code":409,"description":"Conflict"}`, false)
	h.webhookOK(t)
	first := h.next(t, "getUpdates")
	assertPollRequest(t, first, 0, BatchLimit, int64(h.deps.PollTimeout/time.Second), []string{})
}

func TestPollWebhookRetryDiagnostics(t *testing.T) {
	h := newPollHarness(t)
	var logs bytes.Buffer
	h.deps.Log = slog.New(slog.NewTextHandler(&logs, nil))
	i := h.start(t)
	const responseMarker = "raw-response-marker"
	encodedToken := url.QueryEscape(testBotToken)
	description := "webhook refused " + testBotToken + " " + encodedToken
	cases := []struct {
		status    int
		body      string
		truncated bool
		want      []string
	}{
		{http.StatusBadRequest, fmt.Sprintf(`{"ok":false,"error_code":400,"description":%q}`, description), false, []string{"error code 400", "webhook refused"}},
		{http.StatusNotFound, fmt.Sprintf(`{"ok":false,"error_code":404,"description":%q}`, description), false, []string{"error code 404", "webhook refused"}},
		{http.StatusBadGateway, fmt.Sprintf(`{"ok":false,"error_code":502,"description":%q}`, description), false, []string{"502 webhook refused"}},
		{http.StatusBadRequest, responseMarker + " " + testBotToken, false, []string{"decode Telegram deleteWebhook response body"}},
		{http.StatusOK, `{"ok":`, true, []string{"unexpected EOF"}},
	}
	for _, tc := range cases {
		h.next(t, "deleteWebhook").respond(tc.status, tc.body, tc.truncated)
	}
	h.webhookOK(t)
	h.next(t, "getUpdates")
	h.stop(t, i)

	logText := logs.String()
	for _, sensitive := range []string{testBotToken, encodedToken, responseMarker} {
		if strings.Contains(logText, sensitive) {
			t.Fatal("webhook retry logs contain sensitive response data")
		}
	}
	lines := strings.Split(strings.TrimSpace(logText), "\n")
	if len(lines) != len(cases) {
		t.Fatalf("got %d log lines, want %d: %s", len(lines), len(cases), logText)
	}
	for idx, tc := range cases {
		for _, detail := range tc.want {
			if !strings.Contains(lines[idx], detail) {
				t.Errorf("retry after HTTP %d: log %q missing %q", tc.status, lines[idx], detail)
			}
		}
	}
}

func TestPollMalformedUpdate(t *testing.T) {
	h := newPollHarness(t)
	registerRoute(h.router, "join", dispatch.KindChatJoinRequest.Mask(), true, func(u *models.Update, ack dispatch.Ack) {
		ack.Done(errors.New("malformed update was routed"))
		t.Errorf("routed undecodable update %d", u.ID)
	})
	h.start(t)
	h.webhookOK(t)
	h.next(t, "getUpdates").ok(`[{"update_id":50,"chat_join_request":{"date":"invalid"}}]`)
	if got := h.next(t, "getUpdates").offset; got != 51 {
		t.Fatalf("offset = %d, want 51", got)
	}
	assertPollMetric(t, h.metrics, `yabot_updates_total{kind="unknown"}`, 1)
}

func TestPollBatchMetrics(t *testing.T) {
	h := newPollHarness(t)
	received := time.Unix(105, 0)
	h.deps.Now = func() time.Time { return received }
	type sample struct {
		wall time.Time
		date int64
	}
	ages := make(chan sample, BatchLimit)
	h.deps.Skew = pollSkewFunc(func(wall time.Time, date int64) time.Duration {
		ages <- sample{wall, date}
		return wall.Sub(time.Unix(date, 0)) - 3*time.Second
	})

	h.start(t)
	h.webhookOK(t)
	full := []string{
		`{"update_id":1,"chat_join_request":{"date":95}}`,
		`{"update_id":2,"my_chat_member":{"date":96}}`,
		`{"update_id":3,"message":{"date":97}}`,
		`{"update_id":4,"inline_query":{"id":"undated"}}`,
	}
	for id := 5; id <= BatchLimit; id++ {
		full = append(full, fmt.Sprintf(`{"update_id":%d}`, id))
	}
	h.next(t, "getUpdates").ok("[" + strings.Join(full, ",") + "]")
	poll := h.next(t, "getUpdates")
	if poll.offset != BatchLimit+1 {
		t.Fatalf("offset after full batch = %d, want %d", poll.offset, BatchLimit+1)
	}
	assertPollMetric(t, h.metrics, "yabot_ingress_full_batch_ratio", 1)
	assertPollMetric(t, h.metrics, "yabot_ingress_batch_size_count", 1)
	assertPollMetric(t, h.metrics, "yabot_ingress_batch_size_sum", BatchLimit)
	assertPollMetric(t, h.metrics, "yabot_ingress_settle_seconds_count", 1)
	assertPollMetric(t, h.metrics, "yabot_ingress_lag_seconds_count", 3)
	assertPollMetric(t, h.metrics, "yabot_ingress_lag_seconds_sum", 18)
	assertPollMetric(t, h.metrics, `yabot_updates_total{kind="unknown"}`, BatchLimit-4)
	for _, kind := range []string{"chat_join_request", "my_chat_member", "message", "inline_query"} {
		assertPollMetric(t, h.metrics, fmt.Sprintf(`yabot_updates_total{kind=%q}`, kind), 1)
	}
	for _, date := range []int64{95, 96, 97} {
		if got := awaitValue(t, ages); got != (sample{received, date}) {
			t.Errorf("Age called with %+v, want wall=%v, date=%d", got, received, date)
		}
	}
	select {
	case got := <-ages:
		t.Errorf("unexpected Age sample %+v", got)
	default:
	}

	poll.ok("[]")
	poll = h.next(t, "getUpdates")
	if poll.offset != BatchLimit+1 {
		t.Fatalf("offset after empty batch = %d, want %d", poll.offset, BatchLimit+1)
	}
	assertPollMetric(t, h.metrics, "yabot_ingress_full_batch_ratio", 0.5)
	assertPollMetric(t, h.metrics, "yabot_ingress_batch_size_count", 2)
	assertPollMetric(t, h.metrics, "yabot_ingress_batch_size_sum", BatchLimit)
	assertPollMetric(t, h.metrics, "yabot_ingress_settle_seconds_count", 1)

	for id := BatchLimit + 1; id < BatchLimit+99; id++ {
		poll.ok(fmt.Sprintf(`[{"update_id":%d}]`, id))
		poll = h.next(t, "getUpdates")
		if poll.offset != int64(id+1) {
			t.Fatalf("offset after update %d = %d", id, poll.offset)
		}
	}
	assertPollMetric(t, h.metrics, "yabot_ingress_full_batch_ratio", 0.01)
	poll.ok(fmt.Sprintf(`[{"update_id":%d}]`, BatchLimit+99))
	h.next(t, "getUpdates")
	assertPollMetric(t, h.metrics, "yabot_ingress_full_batch_ratio", 0)
	assertPollMetric(t, h.metrics, "yabot_ingress_batch_size_count", 101)
	assertPollMetric(t, h.metrics, "yabot_ingress_batch_size_sum", BatchLimit+99)
	assertPollMetric(t, h.metrics, "yabot_ingress_settle_seconds_count", 100)
}

type pollSkewFunc func(time.Time, int64) time.Duration

func (age pollSkewFunc) Age(wall time.Time, date int64) time.Duration {
	return age(wall, date)
}

func assertPollRequest(t *testing.T, call *botCall, offset int64, limit int, timeout int64, allowed []string) {
	t.Helper()
	if call.offset != offset || call.limit != limit || call.timeout != timeout || call.allowed == nil || !slices.Equal(call.allowed, allowed) {
		t.Errorf("getUpdates(offset=%d, limit=%d, timeout=%d, allowed=%#v), want (%d, %d, %d, %#v)",
			call.offset, call.limit, call.timeout, call.allowed, offset, limit, timeout, allowed)
	}
}

func assertPollExit(t *testing.T, h *pollHarness) {
	t.Helper()
	select {
	case call := <-h.calls:
		t.Errorf("unexpected request after exit: %s with offset %d", call.method, call.offset)
	default:
	}
	select {
	case fatal := <-h.fatals:
		t.Errorf("unexpected fatal after exit: %s: %v", fatal.component, fatal.err)
	default:
	}
}

func assertPollMetric(t *testing.T, set *metrics.Set, name string, want float64) {
	t.Helper()
	var exposition bytes.Buffer
	set.WritePrometheus(&exposition)
	for line := range strings.SplitSeq(exposition.String(), "\n") {
		metric, value, ok := strings.Cut(line, " ")
		if !ok || metric != name {
			continue
		}
		got, err := strconv.ParseFloat(value, 64)
		if err != nil || got != want {
			t.Errorf("%s = %q, want %v", name, value, want)
		}
		return
	}
	t.Errorf("metric %s missing, want %v", name, want)
}
