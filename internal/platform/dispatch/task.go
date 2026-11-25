package dispatch

import (
	"context"
	"runtime/debug"
)

// SettleOnPanic passes a *PanicError to settle before re-panicking, including for panic(nil),
// or before Goexit continues unwinding. Dispatch reports a panic in settle separately.
func SettleOnPanic(t Task, settle func(error)) Task {
	return func(ctx context.Context, sh *Shard) {
		returned := false
		func() {
			defer func() {
				if returned {
					return
				}
				value := recover()
				err := &PanicError{Shard: sh.index, Value: value, Stack: debug.Stack()}
				sh.execute(ctx, work{task: func(context.Context, *Shard) { settle(err) }})
				if value != nil {
					panic(value)
				}
			}()
			t(ctx, sh)
			returned = true
		}()
		// A recovered legacy panic(nil) reaches here. Goexit keeps unwinding.
		if !returned {
			panic(nil)
		}
	}
}
