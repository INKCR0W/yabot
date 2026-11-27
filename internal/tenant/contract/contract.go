package contract

import (
	"context"
	"errors"
)

var ErrAborted = errors.New("chat purge aborted")

type Summarizer interface {
	Summary(ctx context.Context, chatID int64, lang string) (Summary, bool)
}

type Summary struct {
	Feature string
	Title   string
	Lines   []string
}

type UserEraser interface {
	// A nil return means the changes have committed.
	DeleteUser(ctx context.Context, userID int64) error
}

type ChatPurger interface {
	// Calls valid before each batch and returns ErrAborted if it is false.
	// A nil return means the deletions have committed.
	PurgeChat(ctx context.Context, chatID int64, valid func() bool) error
}
