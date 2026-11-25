package dispatch

import "errors"

type permanentError struct{ cause error }

func (e *permanentError) Error() string { return e.cause.Error() }
func (e *permanentError) Unwrap() error { return e.cause }

// Permanent marks err for dead-letter handling while preserving its error chain. Nil stays nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{cause: err}
}

// IsPermanent recognizes explicit marks and recovered panics. A joined error is permanent
// only when every branch is permanent, unless the join itself is explicitly marked.
func IsPermanent(err error) bool {
	for err != nil {
		switch current := err.(type) {
		case *permanentError, *PanicError:
			return true
		case interface{ Unwrap() []error }:
			causes := current.Unwrap()
			if len(causes) == 0 {
				return false
			}
			for _, cause := range causes {
				if !IsPermanent(cause) {
					return false
				}
			}
			return true
		default:
			err = errors.Unwrap(current)
		}
	}
	return false
}
