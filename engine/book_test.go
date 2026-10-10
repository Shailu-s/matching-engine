package engine

import (
	"math/rand"
	"testing"
)

func lim(id uint64, s Side, px, qty int64) *Order {
	return &Order{ID: id, Side: s, Kind: Limit, Price: px, Qty: qty}
}

func TestRestsWhenNoCross(t *testing.T) {
	b := NewBook("X")
	tr, _ := b.Submit(lim(1, Buy, 100, 5))
	if len(tr) != 0 || b.RestingOrders() != 1 {
		t.Fatalf("expected resting order, got trades=%v", tr)
	}
	d := b.Depth(5)
	if d.Bids[0] != (Level{100, 5, 1}) {
		t.Fatalf("bad depth %+v", d)
	}
}

func TestPriceThenTimePriority(t *testing.T) {
	b := NewBook("X")
	b.Submit(lim(1, Sell, 101, 5))
	b.Submit(lim(2, Sell, 100, 5)) // better price, later
	b.Submit(lim(3, Sell, 100, 5)) // same price, even later
	tr, _ := b.Submit(lim(4, Buy, 101, 12))
	want := []Trade{{2, 4, 100, 5, "buy"}, {3, 4, 100, 5, "buy"}, {1, 4, 101, 2, "buy"}}
	if len(tr) != len(want) {
		t.Fatalf("got %v", tr)
	}
	for i := range want {
		if tr[i] != want[i] {
			t.Fatalf("trade %d: got %+v want %+v", i, tr[i], want[i])
		}
	}
	d := b.Depth(5)
	if len(d.Bids) != 0 || d.Asks[0] != (Level{101, 3, 1}) {
		t.Fatalf("bad depth after match %+v", d)
	}
}

func TestTradesAtMakerPrice(t *testing.T) {
	b := NewBook("X")
	b.Submit(lim(1, Buy, 105, 3))
	tr, _ := b.Submit(lim(2, Sell, 100, 3))
	if len(tr) != 1 || tr[0].Price != 105 {
		t.Fatalf("taker must get maker's price, got %v", tr)
	}
}

func TestMarketOrderDoesNotRest(t *testing.T) {
	b := NewBook("X")
	b.Submit(lim(1, Sell, 100, 2))
	tr, _ := b.Submit(&Order{ID: 2, Side: Buy, Kind: Market, Qty: 5})
	if len(tr) != 1 || tr[0].Qty != 2 || b.RestingOrders() != 0 {
		t.Fatalf("market remainder must be dropped: %v resting=%d", tr, b.RestingOrders())
	}
}

func TestCancel(t *testing.T) {
	b := NewBook("X")
	b.Submit(lim(1, Buy, 100, 5))
	b.Submit(lim(2, Buy, 100, 7))
	if _, err := b.Cancel(1); err != nil {
		t.Fatal(err)
	}
	if d := b.Depth(1); d.Bids[0] != (Level{100, 7, 1}) {
		t.Fatalf("bad depth %+v", d)
	}
	b.Cancel(2)
	if len(b.Depth(1).Bids) != 0 {
		t.Fatal("empty level must be removed")
	}
	if _, err := b.Cancel(2); err != ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestRejects(t *testing.T) {
	b := NewBook("X")
	if _, err := b.Submit(lim(1, Buy, 100, 0)); err != ErrBadQty {
		t.Fatal(err)
	}
	if _, err := b.Submit(lim(1, Buy, 0, 1)); err != ErrBadPrice {
		t.Fatal(err)
	}
	b.Submit(lim(1, Buy, 100, 1))
	if _, err := b.Submit(lim(1, Buy, 100, 1)); err != ErrDuplicateID {
		t.Fatal(err)
	}
}

// Conservation: every lot submitted is either traded (counted on both sides),
// still resting, cancelled, or a dropped market remainder. Also the book must
// never be crossed after a submit.
func TestRandomisedInvariants(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	b := NewBook("X")
	var in, traded, cancelled, dropped int64
	for id := uint64(1); id <= 50000; id++ {
		if r.Intn(10) == 0 && b.RestingOrders() > 0 {
			for oid := range b.orders {
				o, _ := b.Cancel(oid)
				cancelled += o.Qty
				break
			}
		}
		o := lim(id, Side(r.Intn(2)), 1000+int64(r.Intn(40)), 1+int64(r.Intn(20)))
		if r.Intn(20) == 0 {
			o.Kind = Market
		}
		in += o.Qty
		tr, err := b.Submit(o)
		if err != nil {
			t.Fatal(err)
		}
		for _, x := range tr {
			traded += 2 * x.Qty
		}
		if o.Kind == Market {
			dropped += o.Qty
		}
		if bb, ba := b.bids.best(), b.asks.best(); bb != nil && ba != nil && bb.price >= ba.price {
			t.Fatalf("crossed book: bid %d >= ask %d", bb.price, ba.price)
		}
	}
	var resting int64
	for _, o := range b.orders {
		resting += o.Qty
	}
	if in != traded+resting+cancelled+dropped {
		t.Fatalf("lots not conserved: in=%d traded=%d resting=%d cancelled=%d dropped=%d", in, traded, resting, cancelled, dropped)
	}
}
