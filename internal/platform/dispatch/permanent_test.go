package dispatch

import (
	"errors"
	"fmt"
	"io/fs"
	"testing"
)

func TestPermanent(t *testing.T) {
	cause := errors.New("invalid entity")
	fileErr := &fs.PathError{Op: "read", Path: "entity", Err: cause}
	marked := Permanent(fileErr)
	panicErr := &PanicError{Value: "failed task"}
	mixed := errors.Join(marked, ErrBusy)
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"marked nil", Permanent(nil), false},
		{"plain", cause, false},
		{"wrapped plain", fmt.Errorf("wrapped: %w", cause), false},
		{"queue full", ErrBusy, false},
		{"settle timeout", ErrSettleTimeout, false},
		{"marked", marked, true},
		{"wrapped mark", fmt.Errorf("wrapped: %w", marked), true},
		{"panic", panicErr, true},
		{"wrapped panic", fmt.Errorf("wrapped: %w", panicErr), true},
		{"all permanent", errors.Join(marked, panicErr), true},
		{"mixed", mixed, false},
		{"mixed with panic", errors.Join(panicErr, ErrBusy), false},
		{"wrapped mixed", fmt.Errorf("wrapped: %w", mixed), false},
		{"nested mixed", errors.Join(panicErr, mixed), false},
		{"explicitly marked mixed", Permanent(mixed), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsPermanent(tc.err); got != tc.want {
				t.Errorf("IsPermanent(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
	if err := Permanent(nil); err != nil {
		t.Errorf("Permanent(nil) = %v, want nil", err)
	}
	var got *fs.PathError
	if !errors.Is(marked, cause) || !errors.As(marked, &got) || got != fileErr {
		t.Errorf("Permanent(%v) did not preserve its error chain", fileErr)
	}
}
