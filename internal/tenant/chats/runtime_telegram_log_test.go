package chats

import (
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"

	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

func TestRuntimeSenderLogsOmitTelegramResponseBodies(t *testing.T) {
	t.Parallel()
	const (
		id    int64 = -100714
		title       = "Private Fixture Chat Title"
		body        = `{"ok":true,"result":{"title":"` + title + `",`
	)
	probe := []struct{ method, payload string }{
		{"getChat", deliveryProbeChat(id, models.ChatTypeSupergroup)},
		{"getChatMember", runtimeProbeAdmin},
		{"getChatAdministrators", runtimeProbeOwner},
	}
	for _, method := range []string{"getChat", "getChatMember", "getChatAdministrators", "leaveChat", "sendMessage"} {
		t.Run(method, func(t *testing.T) {
			h := newRuntimeHarness(t, runtimeOptions{configure: func(d *Deps) { d.AcceptNewChats = method != "leaveChat" }})
			joinDeliveryChat(h, id, 1, models.ChatTypeSupergroup, true)
			if method != "leaveChat" {
				for _, stage := range probe {
					if stage.method == method {
						break
					}
					h.finishRequest(h.nextRequest(stage.method), stage.payload)
				}
			}
			request := h.nextRequest(method)
			upstream, err := bot.New("12345:test", bot.WithSkipGetMe(), bot.WithServerURL("https://telegram.test"),
				bot.WithHTTPClient(0, &http.Client{Transport: runtimeTransport(func(r *http.Request) (*http.Response, error) {
					if !strings.HasSuffix(r.URL.Path, "/"+method) {
						t.Errorf("SDK made an unexpected request for %s", method)
					}
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
				})}))
			if err != nil {
				t.Fatal(err)
			}
			outbound, err := sender.New(sender.Deps{
				Bot: upstream, Log: slog.New(slog.NewJSONHandler(&h.logs, nil)),
				Workers: 1, CapInteractive: 1, CapBackground: 1, GlobalRPS: 10, GlobalBurst: 1,
				ClassWeights: [4]int{1, 1, 1, 1}, ChatQueue: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := outbound.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { runtimeStop(t, outbound.Stop) })
			finished := make(chan sender.Result, 1)
			done := request.Done
			request.Done = func(result sender.Result) {
				done(result)
				finished <- result
			}
			if err := outbound.Enqueue(request); err != nil {
				t.Fatal(err)
			}
			result := runtimeReceive(t, finished)
			h.logs.mu.Lock()
			logs := h.logs.records.String()
			h.logs.mu.Unlock()
			if strings.Contains(logs, title) {
				t.Fatal("sender log contains the chat title from the malformed response")
			}
			if result.Outcome.Class != telegram.Fatal || result.Err == nil || strings.Contains(result.Err.Error(), title) {
				t.Fatalf("malformed response returned an unsafe error or wrong outcome (%s)", result.Outcome.Class)
			}
			h.assertLog(slog.LevelError, "sender request failed", "method", method, "chat_id", id,
				"error", "telegram request failed (fatal, *json.SyntaxError)")
		})
	}
}
