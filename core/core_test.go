package core_test

import (
	"bytes"
	"testing"

	"github.com/Shailu-s/matching-engine/core"
	"github.com/Shailu-s/matching-engine/loadtest"
)

// stream builds a deterministic command stream with seqs stamped, using
// results from a scratch core so cancels hit real resting orders.
func stream(n int, seed int64) []core.Cmd {
	g := loadtest.NewGen(seed, 1, 50)
	scratch := core.New()
	var out []core.Cmd
	seq := uint64(0)
	add := func(c core.Cmd) {
		seq++
		c.Seq = seq
		res := scratch.Apply(&c)
		g.Observe(&c, res)
		out = append(out, c)
	}
	for _, c := range g.Setup() {
		add(c)
	}
	for i := 0; i < n; i++ {
		add(g.Next())
	}
	return out
}

func applyAll(c *core.Core, cmds []core.Cmd) {
	for i := range cmds {
		c.Apply(&cmds[i])
	}
}

func TestDeterministic(t *testing.T) {
	cmds := stream(50_000, 1)
	a, b := core.New(), core.New()
	applyAll(a, cmds)
	applyAll(b, cmds)
	if a.Hash() != b.Hash() {
		t.Fatal("same input produced different state")
	}
	if a.Trades() == 0 || a.Resting() == 0 {
		t.Fatalf("stream too quiet: trades=%d resting=%d", a.Trades(), a.Resting())
	}
}

// Snapshot at the midpoint, restore, apply the tail: must equal applying
// everything from scratch. This is the recovery path in miniature.
func TestSnapshotPlusReplayEqualsFullReplay(t *testing.T) {
	cmds := stream(60_000, 2)
	full := core.New()
	applyAll(full, cmds)

	half := core.New()
	applyAll(half, cmds[:len(cmds)/2])
	var buf bytes.Buffer
	if err := half.WriteSnapshot(&buf); err != nil {
		t.Fatal(err)
	}
	restored, err := core.ReadSnapshot(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Hash() != half.Hash() {
		t.Fatal("snapshot round trip changed state")
	}
	applyAll(restored, cmds[len(cmds)/2:])
	if restored.Hash() != full.Hash() {
		t.Fatal("snapshot + replay != full replay")
	}
}

func TestTradingConservesValue(t *testing.T) {
	cmds := stream(80_000, 3)
	c := core.New()
	// deposits come first in the stream; totals after them must never move
	nDep := 0
	for nDep < len(cmds) && cmds[nDep].Kind == core.CmdDeposit {
		nDep++
	}
	applyAll(c, cmds[:nDep])
	cash0, coins0 := c.Totals()
	applyAll(c, cmds[nDep:])
	cash1, coins1 := c.Totals()
	if cash0 != cash1 || coins0 != coins1 {
		t.Fatalf("value not conserved: cash %d->%d coins %v->%v", cash0, cash1, coins0, coins1)
	}
}

func TestRiskRejectsAndRefunds(t *testing.T) {
	c := core.New()
	seq := uint64(0)
	do := func(cmd core.Cmd) core.Result { seq++; cmd.Seq = seq; return c.Apply(&cmd) }

	do(core.Cmd{Kind: core.CmdDeposit, Account: 1, Asset: core.AssetUSD, Qty: 1_000_000_000}) // $10,000
	do(core.Cmd{Kind: core.CmdDeposit, Account: 2, Asset: 1, Qty: 10})                        // 0.01 BTC

	// $10,000 cannot buy 0.2 BTC at $60,000
	if r := do(core.Cmd{Kind: core.CmdPlace, Account: 1, Market: 0, Side: core.Buy, Price: 6_000_000, Qty: 200}); r.Status != core.ErrNoFunds {
		t.Fatalf("want ErrNoFunds, got %v", r.Status)
	}
	if r := do(core.Cmd{Kind: core.CmdPlace, Account: 1, Market: 0, Side: core.Sell, Price: 6_000_000, Qty: 1}); r.Status != core.ErrNoCoins {
		t.Fatalf("want ErrNoCoins, got %v", r.Status)
	}
	ask := do(core.Cmd{Kind: core.CmdPlace, Account: 2, Market: 0, Side: core.Sell, Price: 6_000_000, Qty: 10})
	if ask.Resting != 10 {
		t.Fatalf("ask should rest: %+v", ask)
	}
	// bid $61,000 for 0.01 BTC, fills at the maker's $60,000: pays $600
	if r := do(core.Cmd{Kind: core.CmdPlace, Account: 1, Market: 0, Side: core.Buy, Price: 6_100_000, Qty: 10}); r.Filled != 10 || r.Trades != 1 {
		t.Fatalf("bid should fill: %+v", r)
	}
	cash, held, coins, _, _ := c.Balance(1)
	if cash != 940_000_000 || held != 0 || coins[0] != 10 {
		t.Fatalf("buyer: cash=%d held=%d btc=%d", cash, held, coins[0])
	}
	cash2, _, coins2, coinsHeld2, _ := c.Balance(2)
	if cash2 != 60_000_000 || coins2[0] != 0 || coinsHeld2[0] != 0 {
		t.Fatalf("seller: cash=%d btc=%d held=%d", cash2, coins2[0], coinsHeld2[0])
	}
	// IOC with nothing to hit gives the whole hold back
	if r := do(core.Cmd{Kind: core.CmdPlace, Account: 1, Market: 0, Side: core.Buy, Price: 5_000_000, Qty: 10, IOC: true}); r.Resting != 0 {
		t.Fatalf("IOC rested: %+v", r)
	}
	if _, held, _, _, _ := c.Balance(1); held != 0 {
		t.Fatalf("IOC left a hold of %d", held)
	}
	// cancel: only the owner, and it releases the hold
	bid := do(core.Cmd{Kind: core.CmdPlace, Account: 1, Market: 0, Side: core.Buy, Price: 5_000_000, Qty: 10})
	if r := do(core.Cmd{Kind: core.CmdCancel, Account: 2, OrderID: bid.Seq}); r.Status != core.ErrNotOwner {
		t.Fatalf("want ErrNotOwner, got %v", r.Status)
	}
	if r := do(core.Cmd{Kind: core.CmdCancel, Account: 1, OrderID: bid.Seq}); r.Status != core.OK {
		t.Fatalf("cancel failed: %v", r.Status)
	}
	if _, held, _, _, _ := c.Balance(1); held != 0 {
		t.Fatalf("cancel left a hold of %d", held)
	}
}

// Risk + match cost per command, single thread: the second rung of the ladder.
func BenchmarkCoreApply(b *testing.B) {
	g := loadtest.NewGen(9, 1, 1000)
	c := core.New()
	seq := uint64(0)
	for _, cmd := range g.Setup() {
		seq++
		cmd.Seq = seq
		c.Apply(&cmd)
	}
	cmds := make([]core.Cmd, b.N)
	for i := range cmds {
		cmds[i] = g.Next()
	}
	b.ResetTimer()
	for i := range cmds {
		seq++
		cmds[i].Seq = seq
		res := c.Apply(&cmds[i])
		g.Observe(&cmds[i], res)
	}
}
