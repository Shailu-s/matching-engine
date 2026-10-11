// Package core is the exchange's business logic: pre-trade risk, matching
// and post-trade settlement, as one deterministic state machine.
//
// Core is single-threaded and has no clock, no randomness and no I/O. Its
// state is a pure function of the ordered command stream, so replaying the
// journal (optionally on top of a snapshot) rebuilds exactly the same state.
// That property is what makes crash recovery possible, and Hash() is how it
// is checked.
package core

import (
	"github.com/Shailu-s/matching-engine/engine"
)

// MarketSpec fixes a market's lot size. Prices are integer cents per whole
// coin, quantities integer lots.
type MarketSpec struct {
	Symbol      string
	LotsPerCoin int64
}

var Markets = []MarketSpec{{"BTC-USD", 1000}, {"ETH-USD", 100}, {"SOL-USD", 10}}

const nm = 3 // len(Markets), as a constant so account arrays need no allocation

// Cash is held in milli-cents so price x lots is exact for every market:
// notional = priceCents * lots * (1000 / lotsPerCoin).
func notional(m uint8, priceCents, lots int64) int64 {
	return priceCents * lots * (1000 / Markets[m].LotsPerCoin)
}

type Kind uint8

const (
	CmdDeposit Kind = 1
	CmdPlace   Kind = 2
	CmdCancel  Kind = 3
)

const (
	Buy  uint8 = 0
	Sell uint8 = 1
)

// AssetUSD is the deposit asset for cash; coin of market m is AssetUSD+1+m.
const AssetUSD uint8 = 0

// Cmd is one journaled input. Seq is stamped by the sequencer; a placed
// order's ID is its Seq, so IDs are deterministic too.
type Cmd struct {
	Seq     uint64
	Kind    Kind
	Account uint32
	Market  uint8
	Side    uint8
	IOC     bool
	Asset   uint8
	Price   int64  // place: limit price, cents per coin
	Qty     int64  // place: lots; deposit: milli-cents (USD) or lots (coin)
	OrderID uint64 // cancel
}

type Status uint8

const (
	OK Status = iota
	ErrNoFunds
	ErrNoCoins
	ErrBadQty
	ErrBadPrice
	ErrBadMarket
	ErrNotFound
	ErrNotOwner
	ErrBadCmd
)

var statusText = [...]string{"ok", "insufficient funds", "insufficient coins", "bad quantity", "bad price", "unknown market", "order not found", "not your order", "bad command"}

func (s Status) String() string {
	if int(s) < len(statusText) {
		return statusText[s]
	}
	return "unknown"
}

// Result answers the command's sender. Fills to resting makers settle
// balances too but are not part of the taker's result.
type Result struct {
	Seq     uint64
	Status  Status
	Filled  int64  // lots filled immediately
	Resting int64  // lots left on the book
	Trades  uint32 // number of fills this command produced
}

type account struct {
	cash, cashHeld   int64
	coins, coinsHeld [nm]int64
}

// order is risk's view of a resting order: who owns it and what it holds.
type order struct {
	account   uint32
	market    uint8
	side      uint8
	price     int64
	remaining int64
	held      int64 // milli-cents for a buy, lots for a sell
}

type Core struct {
	seq      uint64
	accounts map[uint32]*account
	orders   map[uint64]*order
	books    [nm]*engine.Book
	trades   uint64
}

func New() *Core {
	c := &Core{accounts: map[uint32]*account{}, orders: map[uint64]*order{}}
	for i, m := range Markets {
		c.books[i] = engine.NewBook(m.Symbol)
	}
	return c
}

func (c *Core) Seq() uint64    { return c.seq }
func (c *Core) Trades() uint64 { return c.trades }
func (c *Core) Resting() int   { return len(c.orders) }
func (c *Core) Accounts() int  { return len(c.accounts) }

const (
	maxQty   = 1_000_000_000
	maxPrice = 10_000_000_000
	maxDep   = 1 << 50
)

// Apply executes one command. Commands must arrive in Seq order with no
// gaps; the sequencer guarantees it and recovery checks it.
func (c *Core) Apply(cmd *Cmd) Result {
	c.seq = cmd.Seq
	r := Result{Seq: cmd.Seq}
	switch cmd.Kind {
	case CmdDeposit:
		r.Status = c.deposit(cmd)
	case CmdPlace:
		r = c.place(cmd)
	case CmdCancel:
		r.Status = c.cancel(cmd)
	default:
		r.Status = ErrBadCmd
	}
	return r
}

func (c *Core) deposit(cmd *Cmd) Status {
	if cmd.Qty <= 0 || cmd.Qty > maxDep {
		return ErrBadQty
	}
	if cmd.Asset > nm {
		return ErrBadMarket
	}
	a := c.accounts[cmd.Account]
	if a == nil {
		a = &account{}
		c.accounts[cmd.Account] = a
	}
	if cmd.Asset == AssetUSD {
		a.cash += cmd.Qty
	} else {
		a.coins[cmd.Asset-1] += cmd.Qty
	}
	return OK
}

func (c *Core) place(cmd *Cmd) Result {
	r := Result{Seq: cmd.Seq}
	if int(cmd.Market) >= nm {
		r.Status = ErrBadMarket
		return r
	}
	if cmd.Qty <= 0 || cmd.Qty > maxQty {
		r.Status = ErrBadQty
		return r
	}
	if cmd.Price <= 0 || cmd.Price > maxPrice {
		r.Status = ErrBadPrice
		return r
	}
	if cmd.Side != Buy && cmd.Side != Sell {
		r.Status = ErrBadCmd
		return r
	}
	a := c.accounts[cmd.Account]
	if a == nil {
		r.Status = ErrNoFunds
		return r
	}
	m := cmd.Market

	// risk #1: hold what the order could use, before it touches the book
	o := &order{account: cmd.Account, market: m, side: cmd.Side, price: cmd.Price, remaining: cmd.Qty}
	if cmd.Side == Buy {
		need := notional(m, cmd.Price, cmd.Qty)
		if a.cash < need {
			r.Status = ErrNoFunds
			return r
		}
		a.cash -= need
		a.cashHeld += need
		o.held = need
	} else {
		if a.coins[m] < cmd.Qty {
			r.Status = ErrNoCoins
			return r
		}
		a.coins[m] -= cmd.Qty
		a.coinsHeld[m] += cmd.Qty
		o.held = cmd.Qty
	}
	c.orders[cmd.Seq] = o

	eo := &engine.Order{ID: cmd.Seq, Side: engine.Side(cmd.Side), Price: cmd.Price, Qty: cmd.Qty, IOC: cmd.IOC, Owner: uint64(cmd.Account)}
	trades, _ := c.books[m].Submit(eo) // inputs already validated; IDs are unique seqs

	// risk #2: settle both sides of every fill
	for i := range trades {
		t := &trades[i]
		c.fill(t.MakerID, t.Price, t.Qty)
		c.fill(t.TakerID, t.Price, t.Qty)
		r.Filled += t.Qty
	}
	c.trades += uint64(len(trades))
	r.Trades = uint32(len(trades))

	if o.remaining > 0 {
		if cmd.IOC {
			c.release(cmd.Seq, o) // never rests
		} else {
			r.Resting = o.remaining
		}
	}
	return r
}

// fill moves money for one side of a trade.
func (c *Core) fill(id uint64, price, qty int64) {
	o := c.orders[id]
	a := c.accounts[o.account]
	m := o.market
	cost := notional(m, price, qty)
	if o.side == Buy {
		// held at the order's limit; filled at the maker's price, never
		// worse, so the difference goes back to available cash
		part := notional(m, o.price, qty)
		a.cashHeld -= part
		o.held -= part
		a.cash += part - cost
		a.coins[m] += qty
	} else {
		a.coinsHeld[m] -= qty
		o.held -= qty
		a.cash += cost
	}
	o.remaining -= qty
	if o.remaining == 0 {
		c.release(id, o)
	}
}

// release returns whatever an order still holds and forgets it.
func (c *Core) release(id uint64, o *order) {
	a := c.accounts[o.account]
	if o.side == Buy {
		a.cashHeld -= o.held
		a.cash += o.held
	} else {
		a.coinsHeld[o.market] -= o.held
		a.coins[o.market] += o.held
	}
	o.held = 0
	delete(c.orders, id)
}

func (c *Core) cancel(cmd *Cmd) Status {
	o, ok := c.orders[cmd.OrderID]
	if !ok {
		return ErrNotFound
	}
	if o.account != cmd.Account {
		return ErrNotOwner
	}
	c.books[o.market].Cancel(cmd.OrderID)
	c.release(cmd.OrderID, o)
	return OK
}

// Totals sums every account's cash and coins, available plus held. Trading
// only moves value between accounts, so these change only on deposits.
func (c *Core) Totals() (cash int64, coins [nm]int64) {
	for _, a := range c.accounts {
		cash += a.cash + a.cashHeld
		for m := 0; m < nm; m++ {
			coins[m] += a.coins[m] + a.coinsHeld[m]
		}
	}
	return
}

// Balance reports one account, for tests and the admin endpoint.
func (c *Core) Balance(acct uint32) (cash, cashHeld int64, coins, coinsHeld [nm]int64, ok bool) {
	a, ok := c.accounts[acct]
	if !ok {
		return
	}
	return a.cash, a.cashHeld, a.coins, a.coinsHeld, true
}
