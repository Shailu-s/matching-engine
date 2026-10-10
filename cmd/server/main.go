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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shailu-s/matching-engine/engine"
)

//go:embed web
var webFS embed.FS

var symbols = []string{"BTC-USD", "ETH-USD", "SOL-USD"}

// mid prices in ticks (cents) used by the demo liquidity bot
var mids = map[string]int64{"BTC-USD": 6500000, "ETH-USD": 320000, "SOL-USD": 15000}

func main() {
	e := engine.New(symbols...)
	for _, s := range symbols {
		go liquidityBot(e, s)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/orders", func(w http.ResponseWriter, r *http.Request) { submit(e, w, r) })
	mux.HandleFunc("DELETE /api/orders/{sym}/{id}", func(w http.ResponseWriter, r *http.Request) { cancel(e, w, r) })
	mux.HandleFunc("GET /api/depth/{sym}", func(w http.ResponseWriter, r *http.Request) { depth(e, w, r) })
	mux.HandleFunc("GET /api/stream/{sym}", func(w http.ResponseWriter, r *http.Request) { stream(e, w, r) })
	mux.HandleFunc("GET /api/stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"orders": e.Matched.Load(), "trades": e.Trades.Load(), "symbols": symbols, "cpus": runtime.NumCPU()})
	})
	mux.HandleFunc("POST /api/benchmark", benchmark)
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

type orderReq struct {
	Symbol string `json:"symbol"`
	Side   string `json:"side"`
	Type   string `json:"type"`
	Price  int64  `json:"price"`
	Qty    int64  `json:"qty"`
}

func submit(e *engine.Engine, w http.ResponseWriter, r *http.Request) {
	var req orderReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		writeErr(w, 400, "bad json")
		return
	}
	o := &engine.Order{ID: e.NextID(), Price: req.Price, Qty: req.Qty}
	switch strings.ToLower(req.Side) {
	case "buy":
		o.Side = engine.Buy
	case "sell":
		o.Side = engine.Sell
	default:
		writeErr(w, 400, "side must be buy or sell")
		return
	}
	if strings.ToLower(req.Type) == "market" {
		o.Kind = engine.Market
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	trades, err := e.Submit(ctx, req.Symbol, o)
	if err != nil {
		writeErr(w, statusFor(err), err.Error())
		return
	}
	var filled int64
	for _, t := range trades {
		filled += t.Qty
	}
	writeJSON(w, 201, map[string]any{"order_id": o.ID, "filled": filled, "resting": o.Kind == engine.Limit && filled < req.Qty, "trades": trades})
}

func cancel(e *engine.Engine, w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		writeErr(w, 400, "bad id")
		return
	}
	o, err := e.Cancel(r.Context(), r.PathValue("sym"), id)
	if err != nil {
		writeErr(w, statusFor(err), err.Error())
		return
	}
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

var benchMu sync.Mutex

// benchmark runs a fresh, isolated book through N synthetic orders on this
// server and reports measured throughput. One at a time so concurrent clicks
// don't skew each other's numbers.
func benchmark(w http.ResponseWriter, r *http.Request) {
	n, _ := strconv.Atoi(r.URL.Query().Get("n"))
	if n <= 0 || n > 500000 {
		n = 200000
	}
	if !benchMu.TryLock() {
		writeErr(w, 429, "a benchmark is already running, try again in a second")
		return
	}
	defer benchMu.Unlock()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	flow := make([]engine.Order, n)
	for i := range flow {
		flow[i] = engine.Order{ID: uint64(i + 1), Side: engine.Side(rng.Intn(2)), Price: 10000 + int64(rng.Intn(100)) - 50, Qty: 1 + int64(rng.Intn(50))}
		if rng.Intn(20) == 0 {
			flow[i].Kind = engine.Market
		}
	}
	b := engine.NewBook("BENCH")
	var trades int
	start := time.Now()
	for i := range flow {
		tr, _ := b.Submit(&flow[i])
		trades += len(tr)
	}
	el := time.Since(start)
	writeJSON(w, 200, map[string]any{
		"orders": n, "trades": trades, "elapsed_ms": float64(el.Microseconds()) / 1000,
		"orders_per_sec": int64(float64(n) / el.Seconds()), "ns_per_order": el.Nanoseconds() / int64(n),
		"resting_after": b.RestingOrders(), "cpus": runtime.NumCPU(),
	})
}

// liquidityBot keeps each demo book populated with a two-sided market and a
// trickle of crossing orders so the UI is never empty.
func liquidityBot(e *engine.Engine, sym string) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(len(sym))))
	mid := mids[sym]
	tickSz := max(mid/10000, 1)
	ctx := context.Background()
	var live []uint64
	for {
		mid += int64(rng.Intn(5)-2) * tickSz
		s := engine.Side(rng.Intn(2))
		off := int64(1+rng.Intn(12)) * tickSz
		px := mid - off
		if s == engine.Sell {
			px = mid + off
		}
		if rng.Intn(6) == 0 { // aggressive order that trades
			px = mid + 15*tickSz
			if s == engine.Sell {
				px = mid - 15*tickSz
			}
		}
		o := &engine.Order{ID: e.NextID(), Side: s, Price: px, Qty: 1 + int64(rng.Intn(25))}
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
		time.Sleep(time.Duration(150+rng.Intn(250)) * time.Millisecond)
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
