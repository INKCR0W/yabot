package dispatch

type Local[T any] struct {
	values []T
}

// NewLocal calls init once per shard before returning. Register locals before Start.
func NewLocal[T any](d *Dispatcher, init func() T) *Local[T] {
	d.mu.RLock()
	canRegister := d.state == stateCreated
	d.mu.RUnlock()
	if !canRegister {
		panic("dispatch: locals must be created before Start")
	}
	if init == nil {
		panic("dispatch: local initializer must not be nil")
	}
	local := &Local[T]{values: make([]T, len(d.shards))}
	for i := range local.values {
		local.values[i] = init()
	}
	return local
}

// Get requires a Shard from the dispatcher's own goroutine.
func (l *Local[T]) Get(sh *Shard) T { return l.values[sh.Index()] }
