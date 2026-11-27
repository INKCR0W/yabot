package settings

import (
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/INKCR0W/yabot/internal/platform/i18n"
)

var (
	ErrInvalid        = errors.New("settings: invalid patch")
	ErrConflict       = errors.New("settings: etag conflict")
	ErrUnknownFeature = errors.New("settings: unknown feature")
)

type FieldType string

const (
	FieldBool       FieldType = "bool"
	FieldInt        FieldType = "int"
	FieldEnum       FieldType = "enum"
	FieldDuration   FieldType = "duration"
	FieldText       FieldType = "text"
	FieldStringList FieldType = "string_list"
	FieldMultiEnum  FieldType = "multi_enum"
	FieldInfo       FieldType = "info"
	FieldPermRule   FieldType = "perm_rule"
)

type Field struct {
	Key       string
	Type      FieldType
	Label     i18n.Key
	Help      i18n.Key
	Default   json.RawMessage
	Min, Max  *int64
	Step      int64
	Options   []Option
	VisibleIf *Cond
	Group     string
}

type Option struct {
	Value string
	Label i18n.Key
}

type GroupMeta struct {
	ID    string
	Title i18n.Key
}

type Cond struct {
	All []Clause `json:"all,omitzero"`
	Any []Clause `json:"any,omitzero"`
}

type Clause struct {
	Field string            `json:"field"`
	Eq    json.RawMessage   `json:"eq,omitempty"`
	Ne    json.RawMessage   `json:"ne,omitempty"`
	In    []json.RawMessage `json:"in,omitzero"`
}

type Spec struct {
	Feature   string
	PermGroup string
	Version   int
	Title     i18n.Key
	Groups    []GroupMeta
	Fields    []Field
	// Validate receives a private *T and reports errors by schema field key. Calls may overlap.
	Validate func(v any) []FieldError
	Migrate  func(from int, raw json.RawMessage) (json.RawMessage, error)
}

type FieldError struct {
	Key     string
	Message i18n.Key
	Args    i18n.Args
}

type Actor struct {
	UserID int64
	Public bool
}

// Seconds stores an integer number of seconds, including in JSON.
type Seconds int64

// Duration saturates at the bounds of time.Duration.
func (s Seconds) Duration() time.Duration {
	if s > Seconds(math.MaxInt64/int64(time.Second)) {
		return time.Duration(math.MaxInt64)
	}
	if s < Seconds(math.MinInt64/int64(time.Second)) {
		return time.Duration(math.MinInt64)
	}
	return time.Duration(s) * time.Second
}

type FieldCode string

const (
	CodeOK           FieldCode = "ok"
	CodeInvalidValue FieldCode = "invalid_value"
	CodeUnknownKey   FieldCode = "unknown_key"
)

type FieldResult struct {
	Code FieldCode
	Key  i18n.Key
	Args i18n.Args
}

type PatchResult struct {
	ETag   string                     `json:"etag"`
	Values map[string]json.RawMessage `json:"values"`
	Fields map[string]FieldResult     `json:"-"`
}
