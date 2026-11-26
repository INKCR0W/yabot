package sender

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-telegram/bot"
)

func TestClassString(t *testing.T) {
	cases := []struct {
		class Class
		want  string
	}{
		{0, "unknown"},
		{Deadline, "deadline"},
		{Decision, "decision"},
		{Interactive, "interactive"},
		{Background, "background"},
		{Background + 1, "unknown"},
		{255, "unknown"},
	}
	for _, testCase := range cases {
		if got := testCase.class.String(); got != testCase.want {
			t.Errorf("Class(%d).String() = %q, want %q", testCase.class, got, testCase.want)
		}
	}
}

func TestDropReasonString(t *testing.T) {
	cases := []struct {
		reason DropReason
		want   string
	}{
		{0, "none"},
		{Expired, "expired"},
		{Shutdown, "shutdown"},
		{Busy, "busy"},
		{Busy + 1, "unknown"},
	}
	for _, testCase := range cases {
		if got := testCase.reason.String(); got != testCase.want {
			t.Errorf("DropReason(%d).String() = %q, want %q", testCase.reason, got, testCase.want)
		}
	}
}

func TestValidateRequest(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Request)
		want   error
	}{
		{"deadline class with deadline", func(r *Request) {}, nil},
		{"decision class with deadline", func(r *Request) { r.Class = Decision }, nil},
		{"interactive class with deadline", func(r *Request) { r.Class = Interactive }, nil},
		{"background class with deadline", func(r *Request) { r.Class = Background }, nil},
		{"decision class without deadline", func(r *Request) { r.Class, r.Deadline = Decision, time.Time{} }, nil},
		{"background class without deadline", func(r *Request) { r.Class, r.Deadline = Background, time.Time{} }, nil},
		{"zero class", func(r *Request) { r.Class = 0 }, ErrInvalidRequest},
		{"out of range class", func(r *Request) { r.Class = Background + 1 }, ErrInvalidRequest},
		{"nil Do", func(r *Request) { r.Do = nil }, ErrInvalidRequest},
		{"nil Done", func(r *Request) { r.Done = nil }, ErrInvalidRequest},
		{"deadline class without deadline", func(r *Request) { r.Deadline = time.Time{} }, ErrInvalidRequest},
		{"interactive class without deadline", func(r *Request) { r.Class, r.Deadline = Interactive, time.Time{} }, ErrInvalidRequest},
		{"empty method", func(r *Request) { r.Method = "" }, ErrInvalidRequest},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			r := Request{
				Class: Deadline, Method: "methodOutsideTable", Deadline: time.Unix(100, 0),
				Do: func(context.Context, *bot.Bot) error {
					t.Error("validateRequest called Do")
					return nil
				},
				Done: func(Result) { t.Error("validateRequest called Done") },
			}
			testCase.change(&r)
			if err := validateRequest(r); !errors.Is(err, testCase.want) {
				t.Errorf("validateRequest() = %v, want %v", err, testCase.want)
			}
		})
	}
}
