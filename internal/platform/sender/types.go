package sender

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-telegram/bot"

	"github.com/INKCR0W/yabot/internal/platform/telegram"
)

// Class identifies a scheduling class. Its zero value is invalid.
// Higher weights increase the DRR share without imposing strict priority.
type Class uint8

const (
	Deadline Class = iota + 1
	Decision
	Interactive
	Background
)

func (c Class) String() string {
	switch c {
	case Deadline:
		return "deadline"
	case Decision:
		return "decision"
	case Interactive:
		return "interactive"
	case Background:
		return "background"
	default:
		return "unknown"
	}
}

func (c Class) valid() bool {
	return c >= Deadline && c <= Background
}

// DropReason is zero when the request was not dropped.
type DropReason uint8

const (
	Expired DropReason = iota + 1
	Shutdown
	// Busy is synthesized by callers when admission fails.
	Busy
)

func (r DropReason) String() string {
	switch r {
	case 0:
		return "none"
	case Expired:
		return "expired"
	case Shutdown:
		return "shutdown"
	case Busy:
		return "busy"
	default:
		return "unknown"
	}
}

type Request struct {
	Class Class
	// ChatID is zero for requests with no associated chat.
	ChatID int64
	Method string
	// Deadline is required for Deadline and Interactive requests. Otherwise zero means no deadline.
	Deadline time.Time
	Do       func(ctx context.Context, b *bot.Bot) error
	// Done runs on a sender goroutine and must not block.
	Done func(Result)
}

type Result struct {
	Outcome  telegram.Outcome
	Err      error
	Attempts int
	// Ambiguous reports whether any attempt was classified as Retry with zero RetryAfter.
	Ambiguous bool
	// Sender results with a nonzero Dropped have Outcome.Class set to telegram.Retry.
	Dropped DropReason
}

// DroppedResult constructs a dropped result with a Retry outcome.
// Enqueue already counts ErrBusy and ErrWouldMiss rejections.
func DroppedResult(reason DropReason, err error) Result {
	return Result{Outcome: telegram.Outcome{Class: telegram.Retry}, Dropped: reason, Err: err}
}

var (
	ErrWouldMiss      = errors.New("sender: request would miss its deadline")
	ErrBusy           = errors.New("sender: chat queue is full")
	ErrClosed         = errors.New("sender: sender is not accepting requests")
	ErrInvalidRequest = errors.New("sender: invalid request")
)

func validateRequest(r Request) error {
	switch {
	case !r.Class.valid():
		return fmt.Errorf("%w: invalid class %d", ErrInvalidRequest, r.Class)
	case r.Do == nil:
		return fmt.Errorf("%w: Do is required", ErrInvalidRequest)
	case r.Done == nil:
		return fmt.Errorf("%w: Done is required", ErrInvalidRequest)
	case r.Deadline.IsZero() && (r.Class == Deadline || r.Class == Interactive):
		return fmt.Errorf("%w: Deadline is required for class %s", ErrInvalidRequest, r.Class)
	case r.Method == "":
		return fmt.Errorf("%w: Method is required", ErrInvalidRequest)
	default:
		return nil
	}
}
