package engine

import (
	"context"
	"sync"
	"testing"
)

// The hook must see maker fills too, including ones triggered by someone
// else's order, and must run before the submitter gets its reply.
func TestHookSeesEveryFillBeforeReply(t *testing.T) {
	var mu sync.Mutex
	var seen []Trade
	e := NewWithHook(func(sym string, tr []Trade) {
		mu.Lock()
		seen = append(seen, tr...)
		mu.Unlock()
	}, "X")
	ctx := context.Background()
	e.Submit(ctx, "X", &Order{ID: 1, Side: Sell, Price: 100, Qty: 5, Owner: 3})
	e.Submit(ctx, "X", &Order{ID: 2, Side: Buy, Price: 100, Qty: 2})
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || seen[0].MakerOwner != 3 || seen[0].Qty != 2 {
		t.Fatalf("hook saw %+v", seen)
	}
}
