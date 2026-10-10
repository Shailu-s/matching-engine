package engine

import (
	"context"
	"math/rand"
	"sync"
	"testing"
)

// orderFlow builds a realistic mix: limits clustered around a mid price so a
// large share cross and trade, plus some market orders.
func orderFlow(n int, seed int64) []*Order {
	r := rand.New(rand.NewSource(seed))
	out := make([]*Order, n)
	for i := range out {
		o := &Order{ID: uint64(i + 1), Side: Side(r.Intn(2)), Kind: Limit, Price: 10000 + int64(r.Intn(100)) - 50, Qty: 1 + int64(r.Intn(50))}
		if r.Intn(20) == 0 {
			o.Kind = Market
		}
		out[i] = o
	}
	return out
}

// Raw book: single core, no channels. This is the matching cost itself.
func BenchmarkBookSubmit(b *testing.B) {
	flow := orderFlow(b.N, 42)
	bk := NewBook("X")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bk.Submit(flow[i])
	}
}

// Full engine path: concurrent clients -> channel -> owner goroutine -> reply.
func BenchmarkEngineSubmitParallel(b *testing.B) {
	e := New("X")
	ctx := context.Background()
	flow := orderFlow(b.N, 7)
	var idx uint64
	var mu sync.Mutex
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			o := flow[idx]
			idx++
			mu.Unlock()
			e.Submit(ctx, "X", o)
		}
	})
}
