package authz

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"golang.org/x/sync/singleflight"

	"github.com/INKCR0W/yabot/internal/platform/sender"
	"github.com/INKCR0W/yabot/internal/tenant/chats"
)

type Op uint8

const (
	View Op = iota + 1
	Edit
)

const (
	ErrForbidden   authzError = "authz: forbidden"
	ErrBotNotAdmin authzError = "authz: bot is not an administrator"
	ErrUnavailable authzError = "authz: ownership check unavailable"
)

type authzError string

func (e authzError) Error() string { return string(e) }

// Grant.Public is true only for an Edit confirmed with is_anonymous=false.
type Grant struct{ Public bool }

type ChatRef = chats.ChatRef

type Deps struct {
	Chats interface {
		State(chatID int64) (chats.ChatSnapshot, bool)
		OwnedBy(userID int64) []ChatRef
	}
	Reporter interface{ ReportOwnerStale(chatID int64) }
	Sender   interface{ Enqueue(sender.Request) error }
	Log      interface {
		Log(context.Context, slog.Level, string, ...any)
	}
	Metrics interface{ GetOrCreateCounter(string) *metrics.Counter }
	Now     func() time.Time
}

type Authz struct {
	deps  Deps
	edits singleflight.Group
}

func New(deps Deps) (*Authz, error) {
	if deps.Chats == nil || deps.Reporter == nil || deps.Sender == nil {
		return nil, errors.New("authz: chats, reporter and sender are required")
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.Log == nil {
		deps.Log = slog.New(slog.DiscardHandler)
	}
	return &Authz{deps: deps}, nil
}

// Authorize reserves group for future permission rules. View returns a zero Grant.
func (z *Authz) Authorize(ctx context.Context, userID, chatID int64, group string, op Op) (grant Grant, err error) {
	var liveEdit bool
	defer func() {
		if liveEdit {
			go z.observeDecision(context.WithoutCancel(ctx), userID, chatID, op, err)
			return
		}
		z.observeDecision(ctx, userID, chatID, op, err)
	}()

	snapshot, exists := z.deps.Chats.State(chatID)
	if !exists || snapshot.OwnerID == 0 || snapshot.OwnerID != userID {
		return Grant{}, ErrForbidden
	}
	switch snapshot.State {
	case chats.Onboarding:
		return Grant{}, ErrBotNotAdmin
	case chats.Active, chats.Degraded:
	default:
		return Grant{}, ErrForbidden
	}
	switch op {
	case View:
		return Grant{}, nil
	case Edit:
		liveEdit = true
		return z.authorizeEdit(ctx, userID, chatID)
	default:
		return Grant{}, ErrForbidden
	}
}

func (z *Authz) ChatsOf(userID int64) []ChatRef {
	return z.deps.Chats.OwnedBy(userID)
}

type sharedOwnerCheck struct {
	owner   ownership
	observe func()
}

func (z *Authz) authorizeEdit(ctx context.Context, userID, chatID int64) (Grant, error) {
	if err := ctx.Err(); err != nil {
		return Grant{}, err
	}
	key := strconv.FormatInt(chatID, 10) + ":" + strconv.FormatInt(userID, 10)
	result := z.edits.DoChan(key, func() (any, error) {
		done, err := z.submitOwnerCheck(userID, chatID)
		var checked ownerCheck
		if err != nil {
			checked = rejectedOwnerCheck(err)
		} else {
			checked = <-done
		}
		// DoChan publishes the result after removing the completed query from the group.
		observe := sync.OnceFunc(func() { go z.observeOwnerCheck(context.Background(), userID, chatID, checked) })
		owner, err := checked.result()
		return sharedOwnerCheck{owner: owner, observe: observe}, err
	})
	select {
	case <-ctx.Done():
		go func() {
			checked := <-result
			checked.Val.(sharedOwnerCheck).observe()
		}()
		return Grant{}, ctx.Err()
	case checked := <-result:
		shared := checked.Val.(sharedOwnerCheck)
		shared.observe()
		if err := ctx.Err(); err != nil {
			return Grant{}, err
		}
		if checked.Err != nil {
			return Grant{}, checked.Err
		}
		owner := shared.owner
		if !owner.creator {
			z.deps.Reporter.ReportOwnerStale(chatID)
			return Grant{}, ErrForbidden
		}
		return Grant{Public: !owner.anonymous}, nil
	}
}

func (z *Authz) observeDecision(ctx context.Context, userID, chatID int64, op Op, err error) {
	operation := "unknown"
	switch op {
	case View:
		operation = "view"
	case Edit:
		operation = "edit"
	}
	var outcome string
	switch err {
	case nil:
		outcome = "ok"
	case ErrForbidden:
		outcome = "forbidden"
	case ErrBotNotAdmin:
		outcome = "bot_not_admin"
	case ErrUnavailable:
		outcome = "unavailable"
	default:
		outcome = "canceled"
	}
	z.count(fmt.Sprintf(`yabot_authz_decisions_total{op=%q,outcome=%q}`, operation, outcome))
	if err != nil && err != ErrUnavailable {
		z.deps.Log.Log(ctx, slog.LevelInfo, "authorization denied", "user_id", userID, "chat_id", chatID,
			"op", operation, "outcome", outcome)
	}
}

func (z *Authz) count(name string) {
	if z.deps.Metrics != nil {
		z.deps.Metrics.GetOrCreateCounter(name).Inc()
	}
}
