package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestDurationText(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		want     time.Duration
		wantText string
		wantErr  bool
	}{
		{name: "seconds", text: "25s", want: 25 * time.Second, wantText: "25s"},
		{name: "fractional seconds", text: "0.5s", want: 500 * time.Millisecond, wantText: "500ms"},
		{name: "missing unit", text: "25", wantErr: true},
		{name: "overflow", text: "999999999999999999h", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var duration Duration
			err := duration.UnmarshalText([]byte(tc.text))
			if tc.wantErr {
				if err == nil {
					t.Error("UnmarshalText succeeded, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("UnmarshalText: %v", err)
			}
			if got := duration.Std(); got != tc.want {
				t.Errorf("Std() = %v, want %v", got, tc.want)
			}
			text, err := duration.MarshalText()
			if err != nil {
				t.Fatalf("MarshalText: %v", err)
			}
			if got := string(text); got != tc.wantText {
				t.Errorf("MarshalText() = %q, want %q", got, tc.wantText)
			}
		})
	}
}

func TestSecretFormatting(t *testing.T) {
	const raw = "credential-for-redaction-test"
	var secret Secret
	if err := secret.UnmarshalText([]byte(raw)); err != nil {
		t.Fatalf("UnmarshalText: %v", err)
	}
	if secret.Reveal() != raw {
		t.Fatal("Reveal did not return the input")
	}
	wrapped := struct{ Credential Secret }{Credential: secret}
	formats := []struct {
		name     string
		format   string
		arg      any
		wantMask bool
	}{
		{"value", "%v", secret, true},
		{"Go syntax", "%#v", secret, true},
		{"named struct field", "%+v", wrapped, true},
		{"nested Go syntax", "%#v", wrapped, true},
		{"pointer diagnostic", "%p", secret, false},
		{"unexported struct field", "%+v", struct{ credential Secret }{credential: secret}, false},
	}
	for _, tc := range formats {
		t.Run(tc.name, func(t *testing.T) {
			got := fmt.Sprintf(tc.format, tc.arg)
			if strings.Contains(got, raw) {
				t.Error("fmt output contained the original credential")
			}
			if tc.wantMask && !strings.Contains(got, "[REDACTED]") {
				t.Error("fmt output did not contain the mask")
			}
		})
	}

	t.Run("text encoding", func(t *testing.T) {
		got, err := secret.MarshalText()
		if err != nil {
			t.Fatalf("MarshalText: %v", err)
		}
		if string(got) != "[REDACTED]" {
			t.Error("MarshalText did not return the mask")
		}
	})
	t.Run("JSON encoding", func(t *testing.T) {
		got, err := json.Marshal(wrapped)
		if err != nil {
			t.Fatalf("json.Marshal: %v", err)
		}
		if string(got) != `{"Credential":"[REDACTED]"}` {
			t.Error("JSON did not contain the masked credential")
		}
	})

	handlers := []struct {
		name string
		make func(*bytes.Buffer) slog.Handler
	}{
		{"slog text", func(out *bytes.Buffer) slog.Handler { return slog.NewTextHandler(out, nil) }},
		{"slog JSON", func(out *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(out, nil) }},
	}
	for _, tc := range handlers {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			slog.New(tc.make(&out)).Info("redaction", "secret", secret, "wrapped", wrapped)
			if strings.Contains(out.String(), raw) {
				t.Error("slog output contained the original credential")
			}
			if strings.Count(out.String(), "[REDACTED]") != 2 {
				t.Error("slog did not mask both credentials")
			}
		})
	}
}
