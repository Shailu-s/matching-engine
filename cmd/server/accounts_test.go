package main

import (
	"context"
	"math/rand"
	"testing"

	"github.com/Shailu-s/matching-engine/engine"
)

// harness wires Accounts to a real engine the same way main does.
type harness struct {
	t *testing.T
	a *Accounts
	e *engine.Engine
	s *server
}

func newHarness(t *testing.T) *harness {
	a := NewAccounts(symbols)
	e := engine.NewWithHook(a.Settle, symbols...)
	return &harness{t: t, a: a, e: e, s: &server{e: e, a: a}}
}

// place mirrors server.submit without HTTP.
func (h *harness) place(tok, sym, side string, price, qty int64, ioc bool) error {
	id := h.e.NextID()
	oo := &openOrder{ID: id, Symbol: sym, Side: side, Kind: "limit", Price: price, Qty: qty, Remaining: qty, ioc: ioc}
	owner, err := h.a.Reserve(tok, oo)
	if err != nil {
		return err
	}
	o := &engine.Order{ID: id, Price: price, Qty: qty, Owner: owner, IOC: ioc}
	if side == "sell" {
		o.Side = engine.Sell
	}
	if _, err := h.e.Submit(context.Background(), sym, o); err != nil {
		return err
	}
	h.a.Done(tok, id)
	return nil
}

// totals sums every account's cash and coins, held or not.
func (h *harness) totals() (cash int64, coins map[string]int64) {
	h.a.mu.Lock()
	defer h.a.mu.Unlock()
	coins = map[string]int64{}
	for _, acc := range h.a.byID {
		cash += acc.cash + acc.cashHeld
		for s, n := range acc.coins {
			coins[s] += n
		}
		for s, n := range acc.coinsHeld {
			coins[s] += n
		}
	}
	return
}

func TestBuyFillsAtMakerPriceAndRefundsHold(t *testing.T) {
	h := newHarness(t)
	seller, _ := h.a.Create("s")
	buyer, _ := h.a.Create("b")
	seller.coins["BTC-USD"] = 10 // 0.01 BTC
	if err := h.place(seller.token, "BTC-USD", "sell", 6_000_000, 10, false); err != nil {
		t.Fatal(err)
	}
	// buyer bids $61,000 but the maker asked $60,000: pays $600, not $610
	if err := h.place(buyer.token, "BTC-USD", "buy", 6_100_000, 10, false); err != nil {
		t.Fatal(err)
	}
	v, _ := h.a.View(buyer.token)
	if v.Cash != 9400 || v.CashHeld != 0 || v.Positions[0].Coins != 0.01 || len(v.Open) != 0 {
		t.Fatalf("buyer after fill: %+v", v)
	}
	sv, _ := h.a.View(seller.token)
	if sv.Cash != 10600 || sv.Positions[0].Coins != 0 {
		t.Fatalf("seller after fill: %+v", sv)
	}
}

func TestCannotOverspendOrOversell(t *testing.T) {
	h := newHarness(t)
	acc, _ := h.a.Create("x")
	// 0.2 BTC at $60,000 = $12,000 > $10,000
	if err := h.place(acc.token, "BTC-USD", "buy", 6_000_000, 200, false); err != errNoCash {
		t.Fatalf("want errNoCash, got %v", err)
	}
	if err := h.place(acc.token, "ETH-USD", "sell", 300_000, 1, false); err != errNoCoins {
		t.Fatalf("want errNoCoins, got %v", err)
	}
	// two resting bids that each fit alone but not together
	if err := h.place(acc.token, "BTC-USD", "buy", 5_000_000, 150, false); err != nil {
		t.Fatal(err)
	}
	if err := h.place(acc.token, "BTC-USD", "buy", 5_000_000, 150, false); err != errNoCash {
		t.Fatalf("second bid should be refused, got %v", err)
	}
}

func TestIOCReleasesUnfilledAndCancelReleasesResting(t *testing.T) {
	h := newHarness(t)
	acc, _ := h.a.Create("x")
	// nothing to match: IOC fills 0 and must give the whole hold back
	if err := h.place(acc.token, "SOL-USD", "buy", 15_000, 50, true); err != nil {
		t.Fatal(err)
	}
	if v, _ := h.a.View(acc.token); v.Cash != 10000 || v.CashHeld != 0 || len(v.Open) != 0 {
		t.Fatalf("after IOC: %+v", v)
	}
	if err := h.place(acc.token, "SOL-USD", "buy", 15_000, 50, false); err != nil {
		t.Fatal(err)
	}
	v, _ := h.a.View(acc.token)
	if v.CashHeld != 750 || len(v.Open) != 1 {
		t.Fatalf("resting bid should hold $750: %+v", v)
	}
	id := v.Open[0].ID
	if _, err := h.e.Cancel(context.Background(), "SOL-USD", id); err != nil {
		t.Fatal(err)
	}
	h.a.Cancelled(acc.token, id)
	if v, _ := h.a.View(acc.token); v.Cash != 10000 || v.CashHeld != 0 {
		t.Fatalf("after cancel: %+v", v)
	}
}

// Random trading between accounts: cash and coins only ever move between
// accounts, never appear or vanish, and nothing goes negative.
func TestRandomTradingConservesMoney(t *testing.T) {
	h := newHarness(t)
	rng := rand.New(rand.NewSource(1))
	var toks []string
	for i := 0; i < 20; i++ {
		acc, _ := h.a.Create("")
		acc.coins["ETH-USD"] = 100 // 1 ETH each
		toks = append(toks, acc.token)
	}
	cash0, coins0 := h.totals()
	for i := 0; i < 5000; i++ {
		side := "buy"
		if rng.Intn(2) == 0 {
			side = "sell"
		}
		h.place(toks[rng.Intn(len(toks))], "ETH-USD", side, 300_000+int64(rng.Intn(2000))-1000, 1+int64(rng.Intn(20)), rng.Intn(4) == 0)
	}
	cash1, coins1 := h.totals()
	if cash1 != cash0 || coins1["ETH-USD"] != coins0["ETH-USD"] {
		t.Fatalf("money not conserved: cash %d -> %d, eth %d -> %d", cash0, cash1, coins0["ETH-USD"], coins1["ETH-USD"])
	}
	h.a.mu.Lock()
	defer h.a.mu.Unlock()
	for _, acc := range h.a.byID {
		if acc.cash < 0 || acc.cashHeld < 0 || acc.coins["ETH-USD"] < 0 || acc.coinsHeld["ETH-USD"] < 0 {
			t.Fatalf("negative balance: %+v", acc)
		}
	}
}
