package loadtest

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shailu-s/matching-engine/core"
	"github.com/Shailu-s/matching-engine/exchange"
	"github.com/Shailu-s/matching-engine/wire"
)

// Sender is one pipelined connection to the exchange. Results arrive, in
// send order, on the callback given to the Dialer.
type Sender interface {
	Send(id uint64, c *core.Cmd) error
	Flush() error
	Close() error
}

type Dialer func(onResult func(id uint64, r core.Result)) (Sender, error)

// TCP dials the gateway: real sockets, real framing, separate process.
func TCP(addr string) Dialer {
	return func(on func(uint64, core.Result)) (Sender, error) { return wire.Dial(addr, on) }
}

// InProcess calls the pipeline directly: sequencer, journal and core, but
// no network. Used by the ladder to isolate what TCP costs.
func InProcess(x *exchange.Exchange) Dialer {
	return func(on func(uint64, core.Result)) (Sender, error) { return &inproc{x: x, on: on}, nil }
}

type inproc struct {
	x  *exchange.Exchange
	on func(uint64, core.Result)
}

func (p *inproc) Send(id uint64, c *core.Cmd) error {
	return p.x.Submit(*c, func(r core.Result) { p.on(id, r) })
}
func (p *inproc) Flush() error { return nil }
func (p *inproc) Close() error { return nil }

type Config struct {
	Conns    int           // parallel connections
	Window   int           // max unanswered requests per connection
	Rate     int           // total orders/sec; 0 = as fast as acks allow
	Duration time.Duration // measured period
	Warmup   time.Duration // run before measuring
	Accounts int           // per connection
	Seed     int64
}

func (c *Config) defaults() {
	if c.Conns <= 0 {
		c.Conns = 16
	}
	if c.Window <= 0 {
		c.Window = 256
	}
	if c.Duration <= 0 {
		c.Duration = 5 * time.Second
	}
	if c.Accounts <= 0 {
		c.Accounts = 50
	}
}

type Report struct {
	Conns      int               `json:"conns"`
	Window     int               `json:"window"`
	TargetRate int               `json:"target_rate"`
	Seconds    float64           `json:"seconds"`
	Measured   int               `json:"measured_requests"`
	Throughput float64           `json:"orders_per_sec"`
	Trades     uint64            `json:"trades"`
	Statuses   map[string]uint64 `json:"statuses"`
	P50        time.Duration     `json:"p50_ns"`
	P90        time.Duration     `json:"p90_ns"`
	P99        time.Duration     `json:"p99_ns"`
	P999       time.Duration     `json:"p999_ns"`
	Max        time.Duration     `json:"max_ns"`
}

func (r Report) String() string {
	return fmt.Sprintf("%10.0f orders/sec   p50 %-9v p99 %-9v p99.9 %-9v max %v", r.Throughput, r.P50.Round(time.Microsecond), r.P99.Round(time.Microsecond), r.P999.Round(time.Microsecond), r.Max.Round(time.Microsecond))
}

type pending struct {
	at  int64 // when it was sent, or for fixed-rate runs when it was SCHEDULED
	cmd core.Cmd
}

type worker struct {
	gen   *Gen
	gmu   sync.Mutex
	pend  chan pending
	lat   []int64
	stat  [16]uint64
	trade uint64
	from  int64 // measure requests scheduled in [from, to)
	to    int64
	done  atomic.Uint64 // results fully processed; lets drive know lat is safe to read
	errs  atomic.Value
}

// onResult runs on the connection's reader (or the core goroutine for
// in-process). Results are in send order, so the oldest pending matches.
func (w *worker) onResult(_ uint64, r core.Result) {
	p := <-w.pend
	now := time.Now().UnixNano()
	if p.at >= w.from && p.at < w.to {
		w.lat = append(w.lat, now-p.at)
		w.stat[r.Status&15]++
		w.trade += uint64(r.Trades)
	}
	if r.Resting > 0 {
		w.gmu.Lock()
		w.gen.Observe(&p.cmd, r)
		w.gmu.Unlock()
	}
	w.done.Add(1)
}

// Setup funds the accounts every connection will trade with.
func Setup(d Dialer, cfg Config) error {
	cfg.defaults()
	var acked atomic.Int64
	done := make(chan struct{})
	var total int64
	s, err := d(func(_ uint64, r core.Result) {
		if acked.Add(1) == total {
			close(done)
		}
	})
	if err != nil {
		return err
	}
	defer s.Close()
	var cmds []core.Cmd
	for i := 0; i < cfg.Conns; i++ {
		cmds = append(cmds, NewGen(cfg.Seed+int64(i), firstAccount(i, cfg), uint32(cfg.Accounts)).Setup()...)
	}
	total = int64(len(cmds))
	for i := range cmds {
		if err := s.Send(uint64(i), &cmds[i]); err != nil {
			return err
		}
	}
	s.Flush()
	select {
	case <-done:
		return nil
	case <-time.After(30 * time.Second):
		return errors.New("setup: deposits not acknowledged")
	}
}

func firstAccount(conn int, cfg Config) uint32 { return 1 + uint32(conn*cfg.Accounts) }

// Run drives load and measures. Closed loop (Rate 0) keeps each connection's
// window full and measures from send. Fixed rate schedules sends on a clock
// and measures from the scheduled time, so a stalled server can't hide its
// stall by slowing the client down (coordinated omission).
func Run(d Dialer, cfg Config) (Report, error) {
	cfg.defaults()
	start := time.Now().UnixNano()
	from := start + int64(cfg.Warmup)
	to := from + int64(cfg.Duration)
	ws := make([]*worker, cfg.Conns)
	senders := make([]Sender, cfg.Conns)
	for i := range ws {
		w := &worker{gen: NewGen(cfg.Seed+int64(i), firstAccount(i, cfg), uint32(cfg.Accounts)), pend: make(chan pending, cfg.Window), from: from, to: to}
		ws[i] = w
		s, err := d(w.onResult)
		if err != nil {
			return Report{}, err
		}
		senders[i] = s
	}
	var wg sync.WaitGroup
	for i := range ws {
		wg.Add(1)
		go func(w *worker, s Sender) {
			defer wg.Done()
			if err := drive(w, s, cfg, start, to); err != nil {
				w.errs.Store(err)
			}
		}(ws[i], senders[i])
	}
	wg.Wait()
	for _, s := range senders {
		s.Close()
	}

	rep := Report{Conns: cfg.Conns, Window: cfg.Window, TargetRate: cfg.Rate, Seconds: cfg.Duration.Seconds(), Statuses: map[string]uint64{}}
	var all []int64
	for _, w := range ws {
		if e, ok := w.errs.Load().(error); ok {
			return rep, e
		}
		all = append(all, w.lat...)
		rep.Trades += w.trade
		for s, n := range w.stat {
			if n > 0 {
				rep.Statuses[core.Status(s).String()] += n
			}
		}
	}
	if len(all) == 0 {
		return rep, errors.New("no requests completed in the measured window")
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	q := func(p float64) time.Duration { return time.Duration(all[min(len(all)-1, int(p*float64(len(all))))]) }
	rep.Measured = len(all)
	rep.Throughput = float64(len(all)) / cfg.Duration.Seconds()
	rep.P50, rep.P90, rep.P99, rep.P999, rep.Max = q(.50), q(.90), q(.99), q(.999), time.Duration(all[len(all)-1])
	return rep, nil
}

func drive(w *worker, s Sender, cfg Config, start, to int64) error {
	var id uint64
	var interval, next int64
	if cfg.Rate > 0 {
		interval = int64(time.Second) * int64(cfg.Conns) / int64(cfg.Rate)
		next = start
	}
	for {
		now := time.Now().UnixNano()
		if now >= to {
			break
		}
		at := now
		if cfg.Rate > 0 {
			if next > now {
				// nothing due yet: push out what we have and sleep. No spinning:
				// busy-waiting senders would steal CPU from the server. Waking
				// late is fine, the lateness counts toward measured latency.
				s.Flush()
				time.Sleep(time.Duration(next - now))
			}
			at = next
			next += interval
		}
		w.gmu.Lock()
		cmd := w.gen.Next()
		w.gmu.Unlock()
		p := pending{at: at, cmd: cmd}
		select {
		case w.pend <- p:
		default:
			s.Flush() // window full: flush before waiting for acks
			w.pend <- p
		}
		id++
		if err := s.Send(id, &cmd); err != nil {
			return err
		}
	}
	s.Flush()
	deadline := time.Now().Add(15 * time.Second)
	for w.done.Load() < id {
		if time.Now().After(deadline) {
			return fmt.Errorf("%d requests never answered", id-w.done.Load())
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}
