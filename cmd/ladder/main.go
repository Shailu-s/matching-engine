// Command ladder measures the hot path one layer at a time, so every number
// says exactly what it includes:
//
//  1. matching only             engine.Book, one thread, orders already in memory
//
//  2. + risk                    reserve before, settle after (core.Apply)
//
//  3. + sequencer + journal     batching, sequence numbers, file append; no fsync
//
//  4. + fsync (group commit)    acknowledged only after the batch is on disk
//
//  5. + TCP gateway             separate server process, binary protocol, loopback
//
//  6. fixed-rate latency        same as 5 at set rates, latency from scheduled send
//
//     go run ./cmd/ladder                    # full ladder
//     go run ./cmd/ladder -md docs/ladder.md # also write a markdown table
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/Shailu-s/matching-engine/core"
	"github.com/Shailu-s/matching-engine/engine"
	"github.com/Shailu-s/matching-engine/exchange"
	"github.com/Shailu-s/matching-engine/loadtest"
)

type row struct {
	name, what string
	ops        float64
	nsPerOp    float64
	rep        *loadtest.Report
	server     exchange.Latency // Submit -> reply, measured inside the server
	fsync      exchange.Latency // one journal write + fsync
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "serve" {
		serve(os.Args[2:])
		return
	}
	dur := flag.Duration("duration", 5*time.Second, "measured time per networked rung")
	warm := flag.Duration("warmup", time.Second, "warmup per networked rung")
	n := flag.Int("n", 3_000_000, "commands for the single-thread rungs")
	conns := flag.Int("conns", 16, "connections")
	window := flag.Int("window", 256, "in-flight requests per connection")
	rates := flag.String("rates", "10000,50000,100000,200000", "comma-separated fixed rates for rung 6, or auto (25%, 50%, 75% of rung 5)")
	md := flag.String("md", "", "write a markdown table to this file")
	flag.Parse()

	host := hostInfo()
	fmt.Printf("%s\n\n", host)
	cfg := loadtest.Config{Conns: *conns, Window: *window, Duration: *dur, Warmup: *warm, Accounts: 50, Seed: 42}
	var rows []row

	rows = append(rows, rung1(*n))
	printRow(rows[len(rows)-1])
	rows = append(rows, rung2(*n))
	printRow(rows[len(rows)-1])
	rows = append(rows, inProcess("3", "+ sequencer + journal, no fsync", false, cfg))
	printRow(rows[len(rows)-1])
	rows = append(rows, inProcess("4", "+ fsync, group commit", true, cfg))
	printRow(rows[len(rows)-1])

	addr, admin, stop := spawnServer()
	if err := loadtest.Setup(loadtest.TCP(addr), cfg); err != nil {
		log.Fatal(err)
	}
	r5 := network("5", "+ TCP gateway, separate process", addr, admin, cfg)
	rows = append(rows, r5)
	printRow(r5)

	var rs []int
	if *rates == "auto" {
		for _, f := range []float64{0.25, 0.5, 0.75} {
			rs = append(rs, roundRate(r5.ops*f))
		}
	} else {
		for _, s := range strings.Split(*rates, ",") {
			var v int
			fmt.Sscan(strings.TrimSpace(s), &v)
			rs = append(rs, v)
		}
	}
	for _, rate := range rs {
		c := cfg
		c.Rate = rate
		r := network("6", fmt.Sprintf("  at a fixed %s orders/sec", comma(float64(rate))), addr, admin, c)
		rows = append(rows, r)
		printRow(r)
	}
	stop()

	if *md != "" {
		if err := os.WriteFile(*md, []byte(markdown(host, rows, cfg)), 0o644); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("\nwrote %s\n", *md)
	}
}

// ---- single-thread rungs ----

func stream(n int) (setup, cmds []core.Cmd, gen *loadtest.Gen) {
	gen = loadtest.NewGen(7, 1, 1000)
	setup = gen.Setup()
	cmds = make([]core.Cmd, n)
	for i := range cmds {
		cmds[i] = gen.Next()
	}
	return
}

// rung1: the same order flow straight into the books, no risk.
func rung1(n int) row {
	_, cmds, gen := stream(n)
	books := make([]*engine.Book, len(core.Markets))
	for i, m := range core.Markets {
		books[i] = engine.NewBook(m.Symbol)
	}
	market := map[uint64]uint8{}
	orders := make([]engine.Order, n)
	runtime.GC()
	t0 := time.Now()
	for i := range cmds {
		c := &cmds[i]
		id := uint64(i + 1)
		if c.Kind == core.CmdCancel {
			if m, ok := market[c.OrderID]; ok {
				books[m].Cancel(c.OrderID)
				delete(market, c.OrderID)
			}
			continue
		}
		o := &orders[i]
		*o = engine.Order{ID: id, Side: engine.Side(c.Side), Price: c.Price, Qty: c.Qty, IOC: c.IOC}
		books[c.Market].Submit(o)
		if o.Qty > 0 && !c.IOC {
			market[id] = c.Market
			gen.Observe(c, core.Result{Seq: id, Resting: o.Qty})
		}
	}
	el := time.Since(t0)
	return row{name: "1", what: "matching only, 1 thread, orders in memory", ops: float64(n) / el.Seconds(), nsPerOp: float64(el.Nanoseconds()) / float64(n)}
}

// rung2: risk reserve + match + settle, still one thread, no I/O.
func rung2(n int) row {
	setup, cmds, gen := stream(n)
	c := core.New()
	seq := uint64(0)
	for i := range setup {
		seq++
		setup[i].Seq = seq
		c.Apply(&setup[i])
	}
	runtime.GC()
	t0 := time.Now()
	for i := range cmds {
		seq++
		cmds[i].Seq = seq
		r := c.Apply(&cmds[i])
		gen.Observe(&cmds[i], r)
	}
	el := time.Since(t0)
	return row{name: "2", what: "+ risk: reserve before, settle after", ops: float64(n) / el.Seconds(), nsPerOp: float64(el.Nanoseconds()) / float64(n)}
}

// ---- pipeline rungs ----

func inProcess(name, what string, fsync bool, cfg loadtest.Config) row {
	dir, _ := os.MkdirTemp("", "ladder-*")
	defer os.RemoveAll(dir)
	x, err := exchange.Open(exchange.Config{Dir: dir, Fsync: fsync})
	if err != nil {
		log.Fatal(err)
	}
	x.Start()
	defer x.Close()
	d := loadtest.InProcess(x)
	if err := loadtest.Setup(d, cfg); err != nil {
		log.Fatal(err)
	}
	r := measured(d, cfg, x.ResetLatency)
	st, _ := x.State()
	return row{name: name, what: what, ops: r.Throughput, rep: &r, server: st.ServerLat, fsync: st.AppendLat}
}

func network(name, what, addr, admin string, cfg loadtest.Config) row {
	r := measured(loadtest.TCP(addr), cfg, func() {
		if resp, err := http.Post("http://"+admin+"/reset-latency", "", nil); err == nil {
			resp.Body.Close()
		}
	})
	var st exchange.State
	if resp, err := http.Get("http://" + admin + "/state"); err == nil {
		json.NewDecoder(resp.Body).Decode(&st)
		resp.Body.Close()
	}
	return row{name: name, what: what, ops: r.Throughput, rep: &r, server: st.ServerLat, fsync: st.AppendLat}
}

// measured runs an unmeasured warmup, clears the server's histograms, then
// runs the measured period, so client and server numbers cover the same span.
func measured(d loadtest.Dialer, cfg loadtest.Config, reset func()) loadtest.Report {
	if cfg.Warmup > 0 {
		w := cfg
		w.Duration, w.Warmup, w.Seed = cfg.Warmup, 0, cfg.Seed+1000
		if _, err := loadtest.Run(d, w); err != nil {
			log.Fatal(err)
		}
	}
	reset()
	cfg.Warmup = 0
	r, err := loadtest.Run(d, cfg)
	if err != nil {
		log.Fatal(err)
	}
	return r
}

// spawnServer runs this binary again as a separate server process, so the
// gateway rungs cross a real process and socket boundary.
func spawnServer() (string, string, func()) {
	dir, _ := os.MkdirTemp("", "ladder-net-*")
	addr, admin := freeAddr(), freeAddr()
	self, _ := os.Executable()
	cmd := exec.Command(self, "serve", dir, addr, admin)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		if resp, err := http.Get("http://" + admin + "/state"); err == nil {
			resp.Body.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	return addr, admin, func() {
		cmd.Process.Signal(syscall.SIGTERM)
		cmd.Wait()
		os.RemoveAll(dir)
	}
}

func serve(args []string) {
	dir, addr, admin := args[0], args[1], args[2]
	x, err := exchange.Open(exchange.Config{Dir: dir, Fsync: true})
	if err != nil {
		log.Fatal(err)
	}
	x.Start()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	go http.ListenAndServe(admin, x.AdminHandler())
	go x.Serve(ln)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	ln.Close()
	x.Close()
}

func freeAddr() string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// ---- output ----

func hostInfo() string {
	cpu := runtime.GOARCH
	if out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
		cpu = strings.TrimSpace(string(out))
	} else if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, l := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(l, "model name") {
				cpu = strings.TrimSpace(strings.SplitN(l, ":", 2)[1])
				break
			}
		}
	}
	return fmt.Sprintf("host: %s, %d cores, %s/%s, %s", cpu, runtime.NumCPU(), runtime.GOOS, runtime.GOARCH, runtime.Version())
}

func printRow(r row) {
	if r.rep == nil {
		fmt.Printf("%s %-48s %12s orders/sec   %6.0f ns/order\n", r.name, r.what, comma(r.ops), r.nsPerOp)
		return
	}
	p := r.rep
	if r.name == "3" { // no fsync on this rung; the histogram only timed the write
		r.fsync = exchange.Latency{}
	}
	fmt.Printf("%s %-48s %12s orders/sec   client p50 %-7s p99 %-7s | server p99 %-7s | fsync p50 %-6s p99 %s\n",
		r.name, r.what, comma(r.ops), dur(p.P50), dur(p.P99), dur(r.server.P99), dur(r.fsync.P50), dur(r.fsync.P99))
}

func markdown(host string, rows []row, cfg loadtest.Config) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s. Pipeline rungs: %d connections x %d in flight, %v measured after %v warmup.\n\n", host, cfg.Conns, cfg.Window, cfg.Duration, cfg.Warmup)
	b.WriteString("| # | Layer | Orders/sec | Client p50 | Client p99 | Server p99 | fsync p50 | fsync p99 |\n|---|---|---|---|---|---|---|---|\n")
	for _, r := range rows {
		if r.rep == nil {
			fmt.Fprintf(&b, "| %s | %s | %s | %.0f ns/order | | | | |\n", r.name, r.what, comma(r.ops), r.nsPerOp)
		} else {
			fs50, fs99 := "–", "–"
			if r.fsync.Count > 0 && r.name != "3" {
				fs50, fs99 = dur(r.fsync.P50), dur(r.fsync.P99)
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s | %s |\n", r.name, strings.TrimSpace(r.what), comma(r.ops), dur(r.rep.P50), dur(r.rep.P99), dur(r.server.P99), fs50, fs99)
		}
	}
	return b.String()
}

func dur(d time.Duration) string {
	switch {
	case d >= time.Millisecond:
		return fmt.Sprintf("%.1fms", float64(d)/1e6)
	case d == 0:
		return "–"
	default:
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
}

func comma(v float64) string {
	s := fmt.Sprintf("%d", int64(v))
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func roundRate(v float64) int {
	switch {
	case v >= 100_000:
		return int(v/50_000) * 50_000
	case v >= 10_000:
		return int(v/5_000) * 5_000
	default:
		return int(v/1_000)*1_000 + 1_000
	}
}
