// Package loadtest generates realistic order flow and drives it through any
// transport (in-process or TCP), measuring throughput and latency. The same
// generator feeds the core tests, the load generator and the ladder.
package loadtest

import (
	"math/rand"

	"github.com/Shailu-s/matching-engine/core"
)

var mids = [...]int64{6_500_000, 320_000, 15_000} // cents per coin

type live struct {
	id   uint64
	acct uint32
}

// Gen produces a mix of limit orders clustered around mid (so many cross),
// some IOC orders, and cancels of orders it saw rest. Not safe for
// concurrent use; give each sender its own Gen.
type Gen struct {
	rng       *rand.Rand
	first     uint32
	accounts  uint32
	CancelPct int
	IOCPct    int
	live      []live
}

func NewGen(seed int64, firstAccount, accounts uint32) *Gen {
	return &Gen{rng: rand.New(rand.NewSource(seed)), first: firstAccount, accounts: accounts, CancelPct: 10, IOCPct: 10}
}

// Setup funds every account this generator trades with: plenty of cash and
// coins so most orders pass risk, as on a real venue.
func (g *Gen) Setup() []core.Cmd {
	var out []core.Cmd
	for a := g.first; a < g.first+g.accounts; a++ {
		out = append(out, core.Cmd{Kind: core.CmdDeposit, Account: a, Asset: core.AssetUSD, Qty: 1 << 46})
		for m := range core.Markets {
			out = append(out, core.Cmd{Kind: core.CmdDeposit, Account: a, Asset: uint8(m + 1), Qty: 1 << 32})
		}
	}
	return out
}

func (g *Gen) Next() core.Cmd {
	r := g.rng
	if len(g.live) > 0 && r.Intn(100) < g.CancelPct {
		i := r.Intn(len(g.live))
		l := g.live[i]
		g.live[i] = g.live[len(g.live)-1]
		g.live = g.live[:len(g.live)-1]
		return core.Cmd{Kind: core.CmdCancel, Account: l.acct, OrderID: l.id}
	}
	m := r.Intn(len(core.Markets))
	tick := mids[m] / 10_000
	return core.Cmd{
		Kind:    core.CmdPlace,
		Account: g.first + uint32(r.Intn(int(g.accounts))),
		Market:  uint8(m),
		Side:    uint8(r.Intn(2)),
		IOC:     r.Intn(100) < g.IOCPct,
		Price:   mids[m] + int64(r.Intn(100)-50)*tick,
		Qty:     1 + int64(r.Intn(50)),
	}
}

// Observe feeds results back so later cancels target orders that rested.
func (g *Gen) Observe(c *core.Cmd, res core.Result) {
	if c.Kind == core.CmdPlace && res.Resting > 0 {
		if len(g.live) >= 4096 {
			g.live[g.rng.Intn(len(g.live))] = live{res.Seq, c.Account}
			return
		}
		g.live = append(g.live, live{res.Seq, c.Account})
	}
}
