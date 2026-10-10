// Package engine implements a price-time priority limit order book.
//
// Prices and quantities are int64 ticks/lots so matching never touches
// floating point. A Book is NOT safe for concurrent use: the Engine gives
// each symbol a single owner goroutine, which is what makes matching
// deterministic without locks on the hot path.
package engine

import (
	"errors"
	"sort"
)

type Side uint8

const (
	Buy Side = iota
	Sell
)

func (s Side) String() string {
	if s == Buy {
		return "buy"
	}
	return "sell"
}

type Kind uint8

const (
	Limit Kind = iota
	Market
)

type Order struct {
	ID       uint64
	Side     Side
	Kind     Kind
	Price    int64 // ignored for market orders
	Qty      int64 // remaining quantity
	OrigQty  int64
	Seq      uint64 // arrival sequence, the "time" in price-time priority
	level    *level
	prev     *Order
	next     *Order
	resting  bool
}

type Trade struct {
	MakerID uint64 `json:"maker_id"`
	TakerID uint64 `json:"taker_id"`
	Price   int64  `json:"price"`
	Qty     int64  `json:"qty"`
	TakerSide string `json:"taker_side"`
}

// level is a FIFO queue of resting orders at one price, as an intrusive
// doubly linked list so cancel is O(1) once the order is found by ID.
type level struct {
	price int64
	total int64
	head  *Order
	tail  *Order
	count int
}

func (l *level) push(o *Order) {
	o.level = l
	o.prev = l.tail
	o.next = nil
	if l.tail != nil {
		l.tail.next = o
	} else {
		l.head = o
	}
	l.tail = o
	l.total += o.Qty
	l.count++
}

func (l *level) remove(o *Order) {
	if o.prev != nil {
		o.prev.next = o.next
	} else {
		l.head = o.next
	}
	if o.next != nil {
		o.next.prev = o.prev
	} else {
		l.tail = o.prev
	}
	l.total -= o.Qty
	l.count--
	o.prev, o.next, o.level = nil, nil, nil
}

// side keeps price levels sorted best-first: bids descending, asks ascending.
// Levels are few compared with orders, so a sorted slice with binary search
// beats a tree on cache behaviour; the best level is always index 0.
type side struct {
	better func(a, b int64) bool
	levels []*level
	byPx   map[int64]*level
}

func newSide(better func(a, b int64) bool) *side {
	return &side{better: better, byPx: make(map[int64]*level)}
}

func (s *side) best() *level {
	if len(s.levels) == 0 {
		return nil
	}
	return s.levels[0]
}

func (s *side) getOrCreate(px int64) *level {
	if l, ok := s.byPx[px]; ok {
		return l
	}
	l := &level{price: px}
	i := sort.Search(len(s.levels), func(i int) bool { return !s.better(s.levels[i].price, px) })
	s.levels = append(s.levels, nil)
	copy(s.levels[i+1:], s.levels[i:])
	s.levels[i] = l
	s.byPx[px] = l
	return l
}

func (s *side) drop(l *level) {
	delete(s.byPx, l.price)
	i := sort.Search(len(s.levels), func(i int) bool { return !s.better(s.levels[i].price, l.price) })
	if i < len(s.levels) && s.levels[i] == l {
		s.levels = append(s.levels[:i], s.levels[i+1:]...)
	}
}

type Book struct {
	Symbol string
	bids   *side
	asks   *side
	orders map[uint64]*Order
	seq    uint64
	trades []Trade // reused buffer, valid until the next call
}

func NewBook(symbol string) *Book {
	return &Book{
		Symbol: symbol,
		bids:   newSide(func(a, b int64) bool { return a > b }),
		asks:   newSide(func(a, b int64) bool { return a < b }),
		orders: make(map[uint64]*Order),
	}
}

var (
	ErrDuplicateID = errors.New("duplicate order id")
	ErrBadQty      = errors.New("quantity must be positive")
	ErrBadPrice    = errors.New("limit price must be positive")
	ErrNotFound    = errors.New("order not found")
)

// Submit matches an incoming order against the opposite side and rests any
// limit remainder. Market order remainders are discarded (IOC semantics).
// The returned trade slice is only valid until the next Submit.
func (b *Book) Submit(o *Order) ([]Trade, error) {
	if o.Qty <= 0 {
		return nil, ErrBadQty
	}
	if o.Kind == Limit && o.Price <= 0 {
		return nil, ErrBadPrice
	}
	if _, dup := b.orders[o.ID]; dup {
		return nil, ErrDuplicateID
	}
	b.seq++
	o.Seq = b.seq
	o.OrigQty = o.Qty
	b.trades = b.trades[:0]

	opp, own := b.asks, b.bids
	if o.Side == Sell {
		opp, own = b.bids, b.asks
	}
	for o.Qty > 0 {
		best := opp.best()
		if best == nil || (o.Kind == Limit && !crosses(o, best.price)) {
			break
		}
		for o.Qty > 0 && best.head != nil {
			m := best.head
			q := min(o.Qty, m.Qty)
			b.trades = append(b.trades, Trade{MakerID: m.ID, TakerID: o.ID, Price: best.price, Qty: q, TakerSide: o.Side.String()})
			o.Qty -= q
			m.Qty -= q
			best.total -= q
			if m.Qty == 0 {
				// level total was already reduced by q; remove() subtracts m.Qty, now 0
				best.remove(m)
				m.resting = false
				delete(b.orders, m.ID)
			}
		}
		if best.head == nil {
			opp.drop(best)
		}
	}
	if o.Qty > 0 && o.Kind == Limit {
		own.getOrCreate(o.Price).push(o)
		o.resting = true
		b.orders[o.ID] = o
	}
	return b.trades, nil
}

func crosses(o *Order, px int64) bool {
	if o.Side == Buy {
		return o.Price >= px
	}
	return o.Price <= px
}

func (b *Book) Cancel(id uint64) (*Order, error) {
	o, ok := b.orders[id]
	if !ok {
		return nil, ErrNotFound
	}
	l := o.level
	l.remove(o)
	if l.head == nil {
		if o.Side == Buy {
			b.bids.drop(l)
		} else {
			b.asks.drop(l)
		}
	}
	o.resting = false
	delete(b.orders, id)
	return o, nil
}

type Level struct {
	Price  int64 `json:"price"`
	Qty    int64 `json:"qty"`
	Orders int   `json:"orders"`
}

type Depth struct {
	Symbol string  `json:"symbol"`
	Bids   []Level `json:"bids"`
	Asks   []Level `json:"asks"`
}

func (b *Book) Depth(n int) Depth {
	d := Depth{Symbol: b.Symbol, Bids: []Level{}, Asks: []Level{}}
	for i := 0; i < len(b.bids.levels) && i < n; i++ {
		l := b.bids.levels[i]
		d.Bids = append(d.Bids, Level{l.price, l.total, l.count})
	}
	for i := 0; i < len(b.asks.levels) && i < n; i++ {
		l := b.asks.levels[i]
		d.Asks = append(d.Asks, Level{l.price, l.total, l.count})
	}
	return d
}

func (b *Book) RestingOrders() int { return len(b.orders) }

var ErrUnknownSymbol = errors.New("unknown symbol")
