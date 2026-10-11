package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shailu-s/matching-engine/engine"
)

// Units. Prices are integer cents per whole coin. Quantities are integer
// lots, and each market has its own lot size (0.001 BTC, 0.01 ETH, 0.1 SOL)
// so a $10,000 account can trade sensible sizes. Cash is held in
// milli-cents, which makes price x lots exact for every market:
//
//	cost = priceCents * lots * (1000 / lotsPerCoin)
const (
	milliPerCent = 1000
	startCash    = 10_000 * 100 * milliPerCent // $10,000
	maxOpen      = 50
	maxAccounts  = 5000
)

var lotsPerCoin = map[string]int64{"BTC-USD": 1000, "ETH-USD": 100, "SOL-USD": 10}

func notional(sym string, priceCents, lots int64) int64 {
	return priceCents * lots * (milliPerCent / lotsPerCoin[sym])
}

var (
	errNoSession   = errors.New("no session: pick a username or continue as guest")
	errNoCash      = errors.New("not enough USD")
	errNoCoins     = errors.New("not enough coins to sell")
	errTooMany     = errors.New("too many open orders")
	errNotYours    = errors.New("order not found")
	errNoLiquidity = errors.New("no liquidity on the other side")
)

type openOrder struct {
	ID        uint64 `json:"id"`
	Symbol    string `json:"symbol"`
	Side      string `json:"side"`
	Kind      string `json:"type"`
	Price     int64  `json:"price"`
	Qty       int64  `json:"qty"`
	Remaining int64  `json:"remaining"`
	At        int64  `json:"at"`
	held      int64  // milli-cents for a buy, lots for a sell
	ioc       bool
}

type fill struct {
	At      int64  `json:"at"`
	Symbol  string `json:"symbol"`
	Side    string `json:"side"`
	Price   int64  `json:"price"`
	Qty     int64  `json:"qty"`
	Role    string `json:"role"`
	OrderID uint64 `json:"order_id"`
}

type account struct {
	id        uint64
	name      string
	token     string
	cash      int64            // available milli-cents
	cashHeld  int64            // reserved by open buy orders
	coins     map[string]int64 // available lots
	coinsHeld map[string]int64 // reserved by open sell orders
	open      map[uint64]*openOrder
	fills     []fill
	seen      time.Time
}

// Accounts is the demo's tiny risk engine: it reserves funds before an order
// reaches the book and settles fills from the engine's trade hook. One mutex
// is plenty at demo scale; the matching hot path never touches it for the
// liquidity bot's orders.
type Accounts struct {
	mu      sync.Mutex
	byToken map[string]*account
	byID    map[uint64]*account
	nextID  uint64
	last    map[string]*atomic.Int64 // last trade price per symbol, cents
}

func NewAccounts(symbols []string) *Accounts {
	a := &Accounts{byToken: map[string]*account{}, byID: map[uint64]*account{}, last: map[string]*atomic.Int64{}}
	for _, s := range symbols {
		a.last[s] = new(atomic.Int64)
		a.last[s].Store(mids[s])
	}
	return a
}

// Create opens a session. Returns orders of evicted idle accounts so the
// caller can cancel them outside the lock.
func (a *Accounts) Create(name string) (*account, []*openOrder) {
	name = strings.TrimSpace(name)
	if len(name) > 24 {
		name = name[:24]
	}
	b := make([]byte, 16)
	rand.Read(b)
	a.mu.Lock()
	defer a.mu.Unlock()
	var stale []*openOrder
	if len(a.byID) >= maxAccounts {
		for id, acc := range a.byID {
			if time.Since(acc.seen) > 30*time.Minute {
				for _, o := range acc.open {
					stale = append(stale, o)
				}
				delete(a.byID, id)
				delete(a.byToken, acc.token)
			}
		}
	}
	a.nextID++
	if name == "" {
		name = fmt.Sprintf("guest-%04d", a.nextID%10000)
	}
	acc := &account{
		id: a.nextID, name: name, token: hex.EncodeToString(b), cash: startCash,
		coins: map[string]int64{}, coinsHeld: map[string]int64{}, open: map[uint64]*openOrder{}, seen: time.Now(),
	}
	a.byToken[acc.token] = acc
	a.byID[acc.id] = acc
	return acc, stale
}

func (a *Accounts) lookup(token string) (*account, error) {
	acc, ok := a.byToken[token]
	if !ok {
		return nil, errNoSession
	}
	acc.seen = time.Now()
	return acc, nil
}

// Reserve is risk check #1: hold the cash or coins the order could use, and
// register the order before it reaches the book, so a fill can never arrive
// for an order the account layer has not seen.
func (a *Accounts) Reserve(token string, o *openOrder) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	acc, err := a.lookup(token)
	if err != nil {
		return 0, err
	}
	if len(acc.open) >= maxOpen {
		return 0, errTooMany
	}
	if o.Side == "buy" {
		need := notional(o.Symbol, o.Price, o.Qty)
		if acc.cash < need {
			return 0, errNoCash
		}
		acc.cash -= need
		acc.cashHeld += need
		o.held = need
	} else {
		if acc.coins[o.Symbol] < o.Qty {
			return 0, errNoCoins
		}
		acc.coins[o.Symbol] -= o.Qty
		acc.coinsHeld[o.Symbol] += o.Qty
		o.held = o.Qty
	}
	acc.open[o.ID] = o
	return acc.id, nil
}

// Settle is the engine's trade hook (risk check #2). It runs on the symbol's
// owner goroutine, sees every fill once and in order, and moves money for
// whichever side belongs to a real account.
func (a *Accounts) Settle(sym string, trades []engine.Trade) {
	a.last[sym].Store(trades[len(trades)-1].Price)
	var mine bool
	for _, t := range trades {
		if t.MakerOwner != 0 || t.TakerOwner != 0 {
			mine = true
			break
		}
	}
	if !mine {
		return
	}
	now := time.Now().UnixMilli()
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, t := range trades {
		makerSide := "sell"
		if t.TakerSide == "sell" {
			makerSide = "buy"
		}
		a.apply(t.MakerOwner, t.MakerID, sym, makerSide, "maker", t, now)
		a.apply(t.TakerOwner, t.TakerID, sym, t.TakerSide, "taker", t, now)
	}
}

func (a *Accounts) apply(owner, orderID uint64, sym, side, role string, t engine.Trade, now int64) {
	if owner == 0 {
		return
	}
	acc, ok := a.byID[owner]
	if !ok {
		return // account evicted; its resting orders are being cancelled
	}
	o, ok := acc.open[orderID]
	if !ok {
		return
	}
	cost := notional(sym, t.Price, t.Qty)
	if side == "buy" {
		// the hold was taken at the order's limit price; the fill happened at
		// the maker's price, which is never worse, so refund the difference
		part := notional(sym, o.Price, t.Qty)
		acc.cashHeld -= part
		o.held -= part
		acc.cash += part - cost
		acc.coins[sym] += t.Qty
	} else {
		acc.coinsHeld[sym] -= t.Qty
		o.held -= t.Qty
		acc.cash += cost
	}
	o.Remaining -= t.Qty
	if o.Remaining == 0 {
		a.release(acc, o)
	}
	acc.fills = append(acc.fills, fill{At: now, Symbol: sym, Side: side, Price: t.Price, Qty: t.Qty, Role: role, OrderID: orderID})
	if len(acc.fills) > 100 {
		acc.fills = acc.fills[len(acc.fills)-100:]
	}
}

// release returns whatever an order still holds and forgets it.
func (a *Accounts) release(acc *account, o *openOrder) {
	if o.Side == "buy" {
		acc.cashHeld -= o.held
		acc.cash += o.held
	} else {
		acc.coinsHeld[o.Symbol] -= o.held
		acc.coins[o.Symbol] += o.held
	}
	o.held = 0
	delete(acc.open, o.ID)
}

// Done is called after the engine answers. IOC and market orders never rest,
// so anything they did not fill is released now. Engine fills for this order
// all ran in Settle before the engine replied, so there is no race here.
func (a *Accounts) Done(token string, id uint64) *openOrder {
	a.mu.Lock()
	defer a.mu.Unlock()
	acc, ok := a.byToken[token]
	if !ok {
		return nil
	}
	o, ok := acc.open[id]
	if !ok {
		return nil
	}
	cp := *o
	if o.ioc {
		a.release(acc, o)
	}
	return &cp
}

func (a *Accounts) Has(token string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.byToken[token]
	return ok
}

// Owns checks the order belongs to this session before a cancel.
func (a *Accounts) Owns(token string, id uint64) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	acc, err := a.lookup(token)
	if err != nil {
		return "", err
	}
	o, ok := acc.open[id]
	if !ok {
		return "", errNotYours
	}
	return o.Symbol, nil
}

// Cancelled releases the hold once the engine has removed the order.
func (a *Accounts) Cancelled(token string, id uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if acc, ok := a.byToken[token]; ok {
		if o, ok := acc.open[id]; ok {
			a.release(acc, o)
		}
	}
}

type position struct {
	Symbol string  `json:"symbol"`
	Coins  float64 `json:"coins"`
	Held   float64 `json:"held"`
	Last   float64 `json:"last"`
	Value  float64 `json:"value"`
}

type accountView struct {
	Name      string      `json:"name"`
	Cash      float64     `json:"cash"`
	CashHeld  float64     `json:"cash_held"`
	Equity    float64     `json:"equity"`
	PnL       float64     `json:"pnl"`
	PnLPct    float64     `json:"pnl_pct"`
	Positions []position  `json:"positions"`
	Open      []openOrder `json:"open"`
	Fills     []fill      `json:"fills"`
}

func usd(milli int64) float64 { return float64(milli) / (100 * milliPerCent) }

func (a *Accounts) View(token string) (*accountView, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	acc, err := a.lookup(token)
	if err != nil {
		return nil, err
	}
	v := &accountView{Name: acc.name, Cash: usd(acc.cash), CashHeld: usd(acc.cashHeld), Positions: []position{}, Open: []openOrder{}, Fills: []fill{}}
	equity := acc.cash + acc.cashHeld
	for _, s := range symbols {
		lots := acc.coins[s] + acc.coinsHeld[s]
		last := a.last[s].Load()
		val := notional(s, last, lots)
		equity += val
		v.Positions = append(v.Positions, position{
			Symbol: s, Coins: float64(acc.coins[s]) / float64(lotsPerCoin[s]), Held: float64(acc.coinsHeld[s]) / float64(lotsPerCoin[s]),
			Last: float64(last) / 100, Value: usd(val),
		})
	}
	v.Equity = usd(equity)
	v.PnL = usd(equity - startCash)
	v.PnLPct = v.PnL / usd(startCash) * 100
	for _, o := range acc.open {
		v.Open = append(v.Open, *o)
	}
	sort.Slice(v.Open, func(i, j int) bool { return v.Open[i].ID > v.Open[j].ID })
	for i := len(acc.fills) - 1; i >= 0 && len(v.Fills) < 50; i-- {
		v.Fills = append(v.Fills, acc.fills[i])
	}
	return v, nil
}

func (a *Accounts) Count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.byID)
}
