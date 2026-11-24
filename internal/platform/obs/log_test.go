package obs

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestNewLoggerFormats(t *testing.T) {
	type logEntry struct {
		Time     time.Time `json:"time"`
		Level    string    `json:"level"`
		Message  string    `json:"msg"`
		ChatID   int64     `json:"chat_id"`
		UserID   int64     `json:"user_id"`
		Query    string    `json:"query"`
		UpdateID int64     `json:"update_id"`
	}

	tests := []struct {
		format string
		check  func(*testing.T, string)
	}{
		{"json", func(t *testing.T, output string) {
			var got logEntry
			if err := json.Unmarshal([]byte(output), &got); err != nil {
				t.Fatalf("decode log entry: %v", err)
			}
			if got.Time.IsZero() {
				t.Error("log entry has no timestamp")
			}
			got.Time = time.Time{}
			want := logEntry{
				Level: "INFO", Message: "query received", ChatID: -15,
				UserID: 17, Query: "8d26786e", UpdateID: 21,
			}
			if got != want {
				t.Errorf("log entry = %+v, want %+v", got, want)
			}
		}},
		{"text", func(t *testing.T, output string) {
			for _, field := range []string{
				"time=", "level=INFO", `msg="query received"`,
				"chat_id=-15", "user_id=17", "query=8d26786e", "update_id=21",
			} {
				if !strings.Contains(output, field) {
					t.Errorf("log output %q is missing %q", output, field)
				}
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			var output bytes.Buffer
			logger, err := NewLogger(&output, "info", tt.format)
			if err != nil {
				t.Fatalf("NewLogger: %v", err)
			}
			logger.Info("query received",
				slog.Int64(FieldChatID, -15),
				slog.Int64(FieldUserID, 17),
				slog.String(FieldQuery, QueryTag("query-1")),
				slog.Int64(FieldUpdateID, 21),
			)
			if strings.Contains(output.String(), "query-1") {
				t.Error("log output contains the raw query ID")
			}
			tt.check(t, output.String())
		})
	}
}

func TestNewLoggerFiltersLevel(t *testing.T) {
	tests := []struct {
		level string
		want  string
	}{
		{"debug", "debug-message info-message warn-message error-message"},
		{"info", "info-message warn-message error-message"},
		{"warn", "warn-message error-message"},
		{"error", "error-message"},
	}
	for _, format := range []string{"json", "text"} {
		for _, tt := range tests {
			t.Run(format+"/"+tt.level, func(t *testing.T) {
				var output bytes.Buffer
				logger, err := NewLogger(&output, tt.level, format)
				if err != nil {
					t.Fatalf("NewLogger: %v", err)
				}
				logger.Debug("debug-message")
				logger.Info("info-message")
				logger.Warn("warn-message")
				logger.Error("error-message")

				lines := strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n")
				want := strings.Fields(tt.want)
				if len(lines) != len(want) {
					t.Fatalf("log output = %q, want messages %q", output.String(), want)
				}
				for i, message := range want {
					if !strings.Contains(lines[i], message) {
						t.Errorf("log entry %q is missing message %q", lines[i], message)
					}
				}
			})
		}
	}
}

func TestNewLoggerRejectsInvalidOptions(t *testing.T) {
	tests := []struct {
		name   string
		writer io.Writer
		level  string
		format string
	}{
		{"writer", nil, "info", "json"},
		{"level", io.Discard, "INFO", "json"},
		{"format", io.Discard, "info", "yaml"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, err := NewLogger(tt.writer, tt.level, tt.format)
			if logger != nil || err == nil {
				t.Fatalf("NewLogger = (%v, %v), want a nil logger and an error", logger, err)
			}
			if !strings.Contains(err.Error(), tt.name) {
				t.Errorf("NewLogger error = %q, want it to identify the invalid %s", err, tt.name)
			}
		})
	}
}

func TestQueryTag(t *testing.T) {
	tests := []struct {
		queryID string
		want    string
	}{
		{"", "e3b0c442"},
		{"query-1", "8d26786e"},
		{"query-2", "4719075b"},
		{"abc", "ba7816bf"},
	}
	for _, tt := range tests {
		for range 2 {
			if got := QueryTag(tt.queryID); got != tt.want {
				t.Errorf("QueryTag(%q) = %q, want %q", tt.queryID, got, tt.want)
			}
		}
	}
}
