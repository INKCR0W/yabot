package ingress

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/dispatch"
	"github.com/INKCR0W/yabot/internal/platform/telegram"
	"github.com/go-telegram/bot/models"
)

func TestStopSettlesSuccessfulPollAfterCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const updateID = 10
		type delivery struct {
			id  int64
			ack dispatch.Ack
		}
		pollStarted := make(chan struct{})
		confirmations := make(chan int64, 1)
		delivered := make(chan delivery, 1)
		router := newTestRouter(t)
		registerRoute(router, "join", dispatch.KindChatJoinRequest.Mask(), true, func(u *models.Update, ack dispatch.Ack) {
			delivered <- delivery{u.ID, ack}
		})
		source := stubSource{poll: func(ctx context.Context, offset int64, limit int, _ time.Duration, _ []string) ([]models.Update, error) {
			if limit == 1 {
				confirmations <- offset
				return nil, nil
			}
			close(pollStarted)
			<-ctx.Done()
			// The result is successful even when cancellation wins the race to the caller.
			return []models.Update{{ID: updateID, ChatJoinRequest: &models.ChatJoinRequest{}}}, nil
		}}
		i := startTestIngress(t, testDeps(source, router, telegram.NewTimeSkew(nil, nil), make(chan fatalCall, 1)))
		awaitValue(t, pollStarted)
		ctx, cancel := context.WithTimeout(t.Context(), testWait)
		defer cancel()
		stopped := make(chan error, 1)
		go func() { stopped <- i.Stop(ctx) }()
		synctest.Wait()

		var got delivery
		select {
		case got = <-delivered:
			if got.id != updateID {
				t.Fatalf("delivered update %d, want %d", got.id, updateID)
			}
		default:
			t.Fatalf("successful poll did not deliver update %d after cancellation", updateID)
		}
		select {
		case offset := <-confirmations:
			t.Fatalf("confirmed offset %d before update %d settled", offset, updateID)
		case err := <-stopped:
			t.Fatalf("Stop returned %v before update %d settled", err, updateID)
		default:
		}
		got.ack.Done(nil)
		if err := awaitValue(t, stopped); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		select {
		case offset := <-confirmations:
			if offset != updateID+1 {
				t.Errorf("confirmation offset = %d, want %d", offset, updateID+1)
			}
		default:
			t.Fatal("Stop returned without confirming the offset")
		}
	})
}

func TestStopConfirmationTimesOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const wantTimeout = 2 * time.Second
		acks := make(chan dispatch.Ack, 1)
		confirmationStarted := make(chan time.Time, 1)
		releaseConfirmation := make(chan struct{})
		defer close(releaseConfirmation)
		router := newTestRouter(t)
		registerRoute(router, "join", dispatch.KindChatJoinRequest.Mask(), true, func(_ *models.Update, ack dispatch.Ack) {
			acks <- ack
		})
		source := stubSource{poll: func(ctx context.Context, _ int64, limit int, _ time.Duration, _ []string) ([]models.Update, error) {
			if limit != 1 {
				return []models.Update{{ID: 10, ChatJoinRequest: &models.ChatJoinRequest{}}}, nil
			}
			confirmationStarted <- time.Now()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-releaseConfirmation:
				return nil, errors.New("test ended while confirmation was pending")
			}
		}}
		i := startTestIngress(t, testDeps(source, router, telegram.NewTimeSkew(nil, nil), make(chan fatalCall, 1)))
		ack := awaitValue(t, acks)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		stopped := make(chan error, 1)
		go func() { stopped <- i.Stop(ctx) }()
		synctest.Wait()
		ack.Done(nil)
		started := awaitValue(t, confirmationStarted)
		// Read the Stop result directly so an independent test deadline cannot mask it.
		if err := <-stopped; err != nil {
			t.Fatalf("Stop returned %v after %v of confirmation, want nil after %v", err, time.Since(started), wantTimeout)
		}
		if elapsed := time.Since(started); elapsed != wantTimeout {
			t.Errorf("Stop returned %v after confirmation started, want %v", elapsed, wantTimeout)
		}
	})
}

func TestStopConfirmationLogsTelegramErrors(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusConflict} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			h := newPollHarness(t)
			var logs bytes.Buffer
			h.deps.Log = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			acks := make(chan dispatch.Ack, 1)
			registerRoute(h.router, "join", dispatch.KindChatJoinRequest.Mask(), true, func(_ *models.Update, ack dispatch.Ack) {
				acks <- ack
			})

			i := h.start(t)
			h.webhookOK(t)
			h.next(t, "getUpdates").ok(`[{"update_id":10,"chat_join_request":{}}]`)
			ack := awaitValue(t, acks)
			canceled, cancelStop := context.WithCancel(t.Context())
			cancelStop()
			if err := i.Stop(canceled); !errors.Is(err, context.Canceled) {
				t.Fatalf("Stop with canceled context and pending ack = %v, want context canceled", err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), testWait)
			defer cancel()
			stopped := make(chan error, 1)
			go func() { stopped <- i.Stop(ctx) }()
			ack.Done(nil)
			confirmation := h.next(t, "getUpdates")
			assertPollRequest(t, confirmation, 11, 1, 0, []string{"chat_join_request"})
			description := "confirmation rejected: " + http.StatusText(status)
			confirmation.respond(status, fmt.Sprintf(`{"ok":false,"error_code":%d,"description":%q}`, status, description), false)
			if err := awaitValue(t, stopped); err != nil {
				t.Fatalf("Stop: %v", err)
			}
			assertPollExit(t, h)

			var record struct {
				Level  string `json:"level"`
				Offset int64  `json:"offset"`
				Error  string `json:"error"`
			}
			if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
				t.Fatalf("want one confirmation log record, got %q: %v", logs.String(), err)
			}
			if record.Level != slog.LevelInfo.String() || record.Offset != 11 || !strings.Contains(record.Error, description) {
				t.Errorf("confirmation log = %s, want one Info record with offset 11 and error containing %q", logs.String(), description)
			}
		})
	}
}
