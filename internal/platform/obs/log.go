package obs

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
)

const (
	FieldChatID   = "chat_id"
	FieldUserID   = "user_id"
	FieldQuery    = "query"
	FieldUpdateID = "update_id"
)

func NewLogger(w io.Writer, level, format string) (*slog.Logger, error) {
	if w == nil {
		return nil, errors.New("log writer is required")
	}
	var opts slog.HandlerOptions
	switch level {
	case "debug":
		opts.Level = slog.LevelDebug
	case "info":
		opts.Level = slog.LevelInfo
	case "warn":
		opts.Level = slog.LevelWarn
	case "error":
		opts.Level = slog.LevelError
	default:
		return nil, errors.New("invalid log level: must be debug, info, warn, or error")
	}

	switch format {
	case "json":
		return slog.New(slog.NewJSONHandler(w, &opts)), nil
	case "text":
		return slog.New(slog.NewTextHandler(w, &opts)), nil
	default:
		return nil, errors.New("invalid log format: must be json or text")
	}
}

// QueryTag returns the first eight lowercase hex digits of the query ID's SHA-256 digest.
func QueryTag(queryID string) string {
	sum := sha256.Sum256([]byte(queryID))
	return hex.EncodeToString(sum[:4])
}
