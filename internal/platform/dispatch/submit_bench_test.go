package dispatch

import (
	"context"
	"errors"
	"runtime"
	"testing"
)

func BenchmarkSubmit(b *testing.B) {
	d, err := New(Deps{Shards: 4, Inbox: 512, TicketsPerShard: 1024, MailboxPerEntity: 16})
	if err != nil {
		b.Fatal(err)
	}
	type entity struct {
		events  int64
		balance int64
	}
	entities := NewLocal(d, func() map[Key]*entity { return make(map[Key]*entity) })
	if err := d.Start(b.Context()); err != nil {
		b.Fatal(err)
	}
	const keys = 4096
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		id := int64(i % keys)
		key := Key{Space: 1, A: id, B: id % 29}
		task := func(_ context.Context, sh *Shard) {
			state := entities.Get(sh)
			item := state[key]
			if item == nil {
				item = new(entity)
				state[key] = item
			}
			item.events++
			item.balance += key.A
		}
		for {
			err := d.Submit(key, task)
			if err == nil {
				break
			}
			if !errors.Is(err, ErrBusy) {
				b.Fatal(err)
			}
			runtime.Gosched()
		}
	}
	if err := d.Stop(b.Context()); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tasks/s")
}
