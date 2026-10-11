package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"log"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shailu-s/matching-engine/engine"
)

//go:embed web
var webFS embed.FS

var symbols = []string{"BTC-USD", "ETH-USD", "SOL-USD"}

// mid prices in ticks (cents) used by the demo liquidity bot
var mids = map[string]int64{"BTC-USD": 6500000, "ETH-USD": 320000, "SOL-USD": 15000}

func main() {
	accts := NewAccounts(symbols)
	e := engine.NewWithHook(accts.Settle, symbols...)
	for _, s := range symbols {
		go liquidityBot(e, s)
	}
	srv := &server{e: e, a: accts}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/session", srv.session)
	mux.HandleFunc("GET /api/account", srv.account)
	mux.HandleFunc("GET /api/markets", srv.markets)
	mux.HandleFunc("POST /api/orders", srv.submit)
	mux.HandleFunc("DELETE /api/orders/{id}", srv.cancel)
	mux.HandleFunc("GET /api/depth/{sym}", func(w http.ResponseWriter, r *http.Request) { depth(e, w, r) })
	mux.HandleFunc("GET /api/stream/{sym}", func(w http.ResponseWriter, r *http.Request) { stream(e, w, r) })
	mux.HandleFunc("GET /api/stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"orders": e.Matched.Load(), "trades": e.Trades.Load(), "symbols": symbols, "cpus": runtime.NumCPU(), "accounts": accts.Count()})
	})
	mux.HandleFunc("GET /api/benchmark", benchmark)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	sub, _ := fs.Sub(webFS, "web")
	mux.Handle("/", http.FileServer(http.FS(sub)))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("matching engine listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

type server struct {
	e *engine.Engine
	a *Accounts
}

func token(r *http.Request) string { return r.Header.Get("X-Session") }

func (s *server) session(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&req)
	acc, stale := s.a.Create(req.Name)
	for _, o := range stale {
		s.e.Cancel(r.Context(), o.Symbol, o.ID)
	}
	v, _ := s.a.View(acc.token)
	writeJSON(w, 201, map[string]any{"token": acc.token, "account": v})
}

func (s *server) account(w http.ResponseWriter, r *http.Request) {
	v, err := s.a.View(token(r))
	if err != nil {
		writeErr(w, 401, err.Error())
		return
	}
	writeJSON(w, 200, v)
}

func (s *server) markets(w http.ResponseWriter, r *http.Request) {
	out := []map[string]any{}
	for _, sym := range symbols {
		out = append(out, map[string]any{"symbol": sym, "lots_per_coin": lotsPerCoin[sym]})
	}
	writeJSON(w, 200, out)
}

type orderReq struct {
	Symbol string `json:"symbol"`
	Side   string `json:"side"`
	Type   string `json:"type"`
	Price  int64  `json:"price"` // cents per coin, limit orders only
	Qty    int64  `json:"qty"`   // lots
}

// submit: reserve funds (risk #1) -> engine -> fills settle in the trade hook
// (risk #2) -> release whatever an IOC/market order did not use.
func (s *server) submit(w http.ResponseWriter, r *http.Request) {
	var req orderReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		writeErr(w, 400, "bad json")
		return
	}
	if !s.a.Has(token(r)) {
		writeErr(w, 401, errNoSession.Error())
		return
	}
	if _, ok := lotsPerCoin[req.Symbol]; !ok {
		writeErr(w, 404, engine.ErrUnknownSymbol.Error())
		return
	}
	side := strings.ToLower(req.Side)
	if side != "buy" && side != "sell" {
		writeErr(w, 400, "side must be buy or sell")
		return
	}
	if req.Qty <= 0 || req.Qty > 1_000_000 {
		writeErr(w, 400, engine.ErrBadQty.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	kind := "limit"
	price := req.Price
	if strings.ToLower(req.Type) == "market" {
		// a market order becomes an immediate-or-cancel limit 5% through the
		// touch, so the funds to hold are bounded and a thin book can't fill
		// it at an absurd price
		kind = "market"
		d, err := s.e.Depth(ctx, req.Symbol, 1)
		if err != nil {
			writeErr(w, statusFor(err), err.Error())
			return
		}
		if side == "buy" && len(d.Asks) > 0 {
			price = d.Asks[0].Price * 105 / 100
		} else if side == "sell" && len(d.Bids) > 0 {
			price = d.Bids[0].Price * 95 / 100
		} else {
			writeErr(w, 422, errNoLiquidity.Error())
			return
		}
	}
	if price <= 0 || price > 1_000_000_000 {
		writeErr(w, 400, engine.ErrBadPrice.Error())
		return
	}
	id := s.e.NextID()
	oo := &openOrder{ID: id, Symbol: req.Symbol, Side: side, Kind: kind, Price: price, Qty: req.Qty, Remaining: req.Qty, At: time.Now().UnixMilli(), ioc: kind == "market"}
	owner, err := s.a.Reserve(token(r), oo)
	if err != nil {
		code := 422
		if errors.Is(err, errNoSession) {
			code = 401
		}
		writeErr(w, code, err.Error())
		return
	}
	o := &engine.Order{ID: id, Price: price, Qty: req.Qty, Owner: owner, IOC: oo.ioc, Side: engine.Buy}
	if side == "sell" {
		o.Side = engine.Sell
	}
	trades, err := s.e.Submit(ctx, req.Symbol, o)
	if err != nil {
		// the order never reached the book; give the hold back
		s.a.Cancelled(token(r), id)
		writeErr(w, statusFor(err), err.Error())
		return
	}
	s.a.Done(token(r), id)
	if trades == nil {
		trades = []engine.Trade{}
	}
	var filled int64
	for _, t := range trades {
		filled += t.Qty
	}
	writeJSON(w, 201, map[string]any{"order_id": id, "filled": filled, "resting": kind == "limit" && filled < req.Qty, "trades": trades})
}

func (s *server) cancel(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		writeErr(w, 400, "bad id")
		return
	}
	sym, err := s.a.Owns(token(r), id)
	if err != nil {
		code := 404
		if errors.Is(err, errNoSession) {
			code = 401
		}
		writeErr(w, code, err.Error())
		return
	}
	o, err := s.e.Cancel(r.Context(), sym, id)
	if err != nil {
		writeErr(w, statusFor(err), err.Error())
		return
	}
	s.a.Cancelled(token(r), id)
	writeJSON(w, 200, map[string]any{"order_id": o.ID, "cancelled_qty": o.Qty})
}

func depth(e *engine.Engine, w http.ResponseWriter, r *http.Request) {
	d, err := e.Depth(r.Context(), r.PathValue("sym"), 15)
	if err != nil {
		writeErr(w, statusFor(err), err.Error())
		return
	}
	writeJSON(w, 200, d)
}

// stream pushes book + trade events over Server-Sent Events.
func stream(e *engine.Engine, w http.ResponseWriter, r *http.Request) {
	ch, unsub, err := e.Subscribe(r.PathValue("sym"))
	if err != nil {
		writeErr(w, statusFor(err), err.Error())
		return
	}
	defer unsub()
	fl, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, 500, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	// coalesce bursts: send at most ~10 frames/sec per client
	var pending *engine.Event
	var trades []engine.Trade
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-ch:
			trades = append(trades, ev.Trades...)
			pending = &ev
		case <-tick.C:
			if pending == nil {
				continue
			}
			if len(trades) > 50 {
				trades = trades[len(trades)-50:]
			}
			pending.Trades = trades
			b, _ := json.Marshal(pending)
			w.Write([]byte("data: "))
			w.Write(b)
			w.Write([]byte("\n\n"))
			fl.Flush()
			pending, trades = nil, nil
		case <-ping.C:
			w.Write([]byte(": ping\n\n"))
			fl.Flush()
		}
	}
}

var (
	benchMu   sync.Mutex
	lastBench atomic.Pointer[benchSummary]
)

type benchChunk struct {
	Chunk  int   `json:"chunk"`
	Orders int   `json:"orders"`
	Trades int   `json:"trades"`
	Ns     int64 `json:"ns"`
	OPS    int64 `json:"orders_per_sec"`
}

type benchSummary struct {
	Done      bool    `json:"done"`
	Chunks    int     `json:"chunks"`
	Orders    int     `json:"orders"`
	Trades    int     `json:"trades"`
	MedianOPS int64   `json:"median_orders_per_sec"`
	BestOPS   int64   `json:"best_orders_per_sec"`
	NsPerOrd  int64   `json:"median_ns_per_order"`
	Resting   int     `json:"resting_after"`
	CPUs      int     `json:"cpus"`
	GoVersion string  `json:"go_version"`
	At        int64   `json:"at"`
	Cached    bool    `json:"cached,omitempty"`
	AgeSec    float64 `json:"age_sec,omitempty"`
}

// benchmark streams a live run as NDJSON: one line per timed chunk of orders
// through a single fresh, isolated book, then a summary. Each chunk is timed
// on its own and the short pause between chunks is outside the timing, so a
// throttled shared CPU shows up as slow bars instead of silently skewing one
// big number. The median chunk is the headline figure.
func benchmark(w http.ResponseWriter, r *http.Request) {
	const chunks, size = 10, 50_000
	if !benchMu.TryLock() {
		if last := lastBench.Load(); last != nil {
			c := *last
			c.Cached = true
			c.AgeSec = time.Since(time.UnixMilli(c.At)).Seconds()
			writeJSON(w, 200, c)
			return
		}
		writeErr(w, 429, "a benchmark is already running, try again in a second")
		return
	}
	defer benchMu.Unlock()
	fl, _ := w.(http.Flusher)
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	enc := json.NewEncoder(w)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	flow := make([]engine.Order, chunks*size)
	for i := range flow {
		flow[i] = engine.Order{ID: uint64(i + 1), Side: engine.Side(rng.Intn(2)), Price: 10000 + int64(rng.Intn(100)) - 50, Qty: 1 + int64(rng.Intn(50))}
		if rng.Intn(20) == 0 {
			flow[i].Kind = engine.Market
		}
	}
	b := engine.NewBook("BENCH")
	ops := make([]int64, 0, chunks)
	sum := benchSummary{Done: true, Chunks: chunks, CPUs: runtime.NumCPU(), GoVersion: runtime.Version()}
	for c := 0; c < chunks; c++ {
		if r.Context().Err() != nil {
			return
		}
		part := flow[c*size : (c+1)*size]
		trades := 0
		start := time.Now()
		for i := range part {
			tr, _ := b.Submit(&part[i])
			trades += len(tr)
		}
		el := time.Since(start)
		ch := benchChunk{Chunk: c + 1, Orders: size, Trades: trades, Ns: el.Nanoseconds(), OPS: int64(float64(size) / el.Seconds())}
		ops = append(ops, ch.OPS)
		sum.Orders += size
		sum.Trades += trades
		enc.Encode(ch)
		if fl != nil {
			fl.Flush()
		}
		time.Sleep(150 * time.Millisecond)
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i] < ops[j] })
	sum.MedianOPS = (ops[chunks/2-1] + ops[chunks/2]) / 2
	sum.BestOPS = ops[chunks-1]
	sum.NsPerOrd = int64(1e9 / float64(sum.MedianOPS))
	sum.Resting = b.RestingOrders()
	sum.At = time.Now().UnixMilli()
	lastBench.Store(&sum)
	enc.Encode(sum)
}

// liquidityBot keeps each demo book populated with a two-sided market and a
// trickle of crossing orders so the UI is never empty.
func liquidityBot(e *engine.Engine, sym string) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(len(sym))))
	mid := mids[sym]
	tickSz := max(mid/10000, 1)
	ctx := context.Background()
	var live []uint64
	for i := 0; ; i++ {
		seeding := i < 120 // fill both sides fast after a restart, passive only
		mid += int64(rng.Intn(5)-2) * tickSz
		s := engine.Side(rng.Intn(2))
		off := int64(1+rng.Intn(12)) * tickSz
		px := mid - off
		if s == engine.Sell {
			px = mid + off
		}
		if !seeding && rng.Intn(6) == 0 { // aggressive order that trades
			px = mid + 15*tickSz
			if s == engine.Sell {
				px = mid - 15*tickSz
			}
		}
		o := &engine.Order{ID: e.NextID(), Side: s, Price: px, Qty: 1 + int64(rng.Intn(40))}
		qty := o.Qty
		// don't read o after Submit: once resting, the engine goroutine owns it
		if tr, err := e.Submit(ctx, sym, o); err == nil {
			var filled int64
			for _, t := range tr {
				filled += t.Qty
			}
			if filled < qty {
				live = append(live, o.ID)
			}
		}
		if len(live) > 300 {
			e.Cancel(ctx, sym, live[0])
			live = live[1:]
		}
		if !seeding {
			time.Sleep(time.Duration(150+rng.Intn(250)) * time.Millisecond)
		}
	}
}

func statusFor(err error) int {
	switch {
	case errors.Is(err, engine.ErrUnknownSymbol), errors.Is(err, engine.ErrNotFound):
		return 404
	case errors.Is(err, context.DeadlineExceeded):
		return 503
	default:
		return 400
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
