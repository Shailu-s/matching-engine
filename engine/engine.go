package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Engine owns one Book per symbol, each driven by a single goroutine that
// drains a command channel. Symbols match in parallel; within a symbol every
// command is applied in arrival order, so no locks touch the book itself.
type Engine struct {
	mu      sync.RWMutex
	books   map[string]*runner
	nextID  atomic.Uint64
	Matched atomic.Uint64 // total orders processed, for the stats endpoint
	Trades  atomic.Uint64
}

type cmdKind uint8

const (
	cmdSubmit cmdKind = iota
	cmdCancel
	cmdDepth
)

type cmd struct {
	kind  cmdKind
	order *Order
	id    uint64
	depth int
	reply chan result
}

type result struct {
	trades []Trade
	order  *Order
	depth  Depth
	err    error
}

type runner struct {
	book *Book
	in   chan cmd
	subs map[chan Event]struct{}
	smu  sync.Mutex
}

// Event is pushed to subscribers after every state change on a symbol.
type Event struct {
	Symbol string  `json:"symbol"`
	Trades []Trade `json:"trades"`
	Depth  Depth   `json:"depth"`
	At     int64   `json:"at"`
}

func New(symbols ...string) *Engine {
	e := &Engine{books: make(map[string]*runner)}
	for _, s := range symbols {
		r := &runner{book: NewBook(s), in: make(chan cmd, 4096), subs: map[chan Event]struct{}{}}
		e.books[s] = r
		go e.loop(r)
	}
	return e
}

func (e *Engine) loop(r *runner) {
	for c := range r.in {
		var res result
		switch c.kind {
		case cmdSubmit:
			tr, err := r.book.Submit(c.order)
			if err == nil {
				res.trades = append([]Trade(nil), tr...) // book reuses its buffer
				e.Matched.Add(1)
				e.Trades.Add(uint64(len(tr)))
			}
			res.err = err
		case cmdCancel:
			res.order, res.err = r.book.Cancel(c.id)
		case cmdDepth:
			res.depth = r.book.Depth(c.depth)
		}
		if c.kind != cmdDepth && res.err == nil {
			r.publish(Event{Symbol: r.book.Symbol, Trades: res.trades, Depth: r.book.Depth(10), At: time.Now().UnixMilli()})
		}
		if c.reply != nil {
			c.reply <- res
		}
	}
}

// publish never blocks the matching loop: a slow subscriber misses events
// and resyncs from the depth snapshot carried by the next one.
func (r *runner) publish(ev Event) {
	r.smu.Lock()
	defer r.smu.Unlock()
	for ch := range r.subs {
		select {
		case ch <- ev:
		default:
		}
	}
}

func (e *Engine) runner(sym string) (*runner, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	r, ok := e.books[sym]
	if !ok {
		return nil, ErrUnknownSymbol
	}
	return r, nil
}

func (e *Engine) Symbols() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]string, 0, len(e.books))
	for s := range e.books {
		out = append(out, s)
	}
	return out
}

func (e *Engine) NextID() uint64 { return e.nextID.Add(1) }

func (e *Engine) do(ctx context.Context, sym string, c cmd) (result, error) {
	r, err := e.runner(sym)
	if err != nil {
		return result{}, err
	}
	c.reply = make(chan result, 1)
	select {
	case r.in <- c:
	case <-ctx.Done():
		return result{}, ctx.Err()
	}
	select {
	case res := <-c.reply:
		return res, res.err
	case <-ctx.Done():
		return result{}, ctx.Err()
	}
}

func (e *Engine) Submit(ctx context.Context, sym string, o *Order) ([]Trade, error) {
	res, err := e.do(ctx, sym, cmd{kind: cmdSubmit, order: o})
	return res.trades, err
}

func (e *Engine) Cancel(ctx context.Context, sym string, id uint64) (*Order, error) {
	res, err := e.do(ctx, sym, cmd{kind: cmdCancel, id: id})
	return res.order, err
}

func (e *Engine) Depth(ctx context.Context, sym string, n int) (Depth, error) {
	res, err := e.do(ctx, sym, cmd{kind: cmdDepth, depth: n})
	return res.depth, err
}

func (e *Engine) Subscribe(sym string) (chan Event, func(), error) {
	r, err := e.runner(sym)
	if err != nil {
		return nil, nil, err
	}
	ch := make(chan Event, 64)
	r.smu.Lock()
	r.subs[ch] = struct{}{}
	r.smu.Unlock()
	return ch, func() {
		r.smu.Lock()
		delete(r.subs, ch)
		r.smu.Unlock()
	}, nil
}
