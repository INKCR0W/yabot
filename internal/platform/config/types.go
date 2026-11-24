package config

import (
	"errors"
	"log/slog"
	"time"
)

// Duration accepts Go duration strings such as "25s" in TOML.
type Duration time.Duration

const (
	invalidDurationText = "must be a valid duration string such as 25s"
	durationTOMLType    = "must be a TOML string such as 25s"
	secretTOMLType      = "must be a TOML string"
)

func (d Duration) Std() time.Duration {
	return time.Duration(d)
}

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.Std().String()), nil
}

func (d *Duration) UnmarshalText(text []byte) error {
	value, err := time.ParseDuration(string(text))
	if err != nil {
		return errors.New(invalidDurationText)
	}
	*d = Duration(value)
	return nil
}

func (d *Duration) UnmarshalTOML(value any) error {
	text, ok := value.(string)
	if !ok {
		return errors.New(durationTOMLType)
	}
	return d.UnmarshalText([]byte(text))
}

const redactedSecret = "[REDACTED]"

// Secret redacts its value in formatted output, logs, and text encoding. Its zero value is unset.
type Secret struct {
	// A pointer hides the raw value when fmt cannot call methods on an unexported field.
	value *string
}

func newSecret(value string) Secret {
	if value == "" {
		return Secret{}
	}
	return Secret{value: &value}
}

// Reveal returns the raw credential or an empty string if unset.
func (s Secret) Reveal() string {
	if s.value == nil {
		return ""
	}
	return *s.value
}

func (s Secret) String() string {
	return redactedSecret
}

func (s Secret) GoString() string {
	return redactedSecret
}

func (s Secret) LogValue() slog.Value {
	return slog.StringValue(redactedSecret)
}

func (s Secret) MarshalText() ([]byte, error) {
	return []byte(redactedSecret), nil
}

func (s *Secret) UnmarshalText(text []byte) error {
	*s = newSecret(string(text))
	return nil
}

func (s *Secret) UnmarshalTOML(value any) error {
	text, ok := value.(string)
	if !ok {
		return errors.New(secretTOMLType)
	}
	return s.UnmarshalText([]byte(text))
}
