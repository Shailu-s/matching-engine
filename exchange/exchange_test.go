package exchange_test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Shailu-s/matching-engine/core"
	"github.com/Shailu-s/matching-engine/exchange"
	"github.com/Shailu-s/matching-engine/loadtest"
	"github.com/Shailu-s/matching-engine/wire"
)

// Orders over a real TCP connection get the results the core computes.
func TestGatewayRoundTrip(t *testing.T) {
	x, err := exchange.Open(exchange.Config{Dir: t.TempDir(), Fsync: true})
	if err != nil {
		t.Fatal(err)
	}
	x.Start()
	defer x.Close()
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go x.Serve(ln)

	results := make(chan core.Result, 16)
	c, err := wire.Dial(ln.Addr().String(), func(_ uint64, r core.Result) { results <- r })
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	send := []core.Cmd{
		{Kind: core.CmdDeposit, Account: 1, Asset: core.AssetUSD, Qty: 1_000_000_000},
		{Kind: core.CmdDeposit, Account: 2, Asset: 1, Qty: 10},
		{Kind: core.CmdPlace, Account: 2, Market: 0, Side: core.Sell, Price: 6_000_000, Qty: 10},
		{Kind: core.CmdPlace, Account: 1, Market: 0, Side: core.Buy, Price: 6_100_000, Qty: 4},
		{Kind: core.CmdCancel, Account: 1, OrderID: 3},
		{Kind: core.CmdCancel, Account: 2, OrderID: 3},
	}
	for i := range send {
		c.Send(uint64(i), &send[i])
	}
	c.Flush()
	got := make([]core.Result, len(send))
	for i := range got {
		select {
		case got[i] = <-results:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout")
		}
	}
	if got[2].Resting != 10 || got[3].Filled != 4 || got[3].Trades != 1 {
		t.Fatalf("fill wrong: %+v %+v", got[2], got[3])
	}
	if got[4].Status != core.ErrNotOwner || got[5].Status != core.OK {
		t.Fatalf("cancel wrong: %v %v", got[4].Status, got[5].Status)
	}
	for i, r := range got {
		if r.Seq != uint64(i+1) {
			t.Fatalf("result %d has seq %d", i, r.Seq)
		}
	}
}

// Graceful restart: Close snapshots, Open restores the same state.
func TestRestartRestoresState(t *testing.T) {
	dir := t.TempDir()
	x, _ := exchange.Open(exchange.Config{Dir: dir, Fsync: false, SnapshotEvery: 20_000})
	x.Start()
	cfg := loadtest.Config{Conns: 4, Window: 64, Duration: 300 * time.Millisecond, Accounts: 10, Seed: 1}
	if err := loadtest.Setup(loadtest.InProcess(x), cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := loadtest.Run(loadtest.InProcess(x), cfg); err != nil {
		t.Fatal(err)
	}
	before, _ := x.State()
	if err := x.Close(); err != nil {
		t.Fatal(err)
	}
	y, err := exchange.Open(exchange.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	y.Start()
	defer y.Close()
	after, _ := y.State()
	if after.Seq != before.Seq || after.Hash != before.Hash {
		t.Fatalf("restart changed state: %d %s -> %d %s", before.Seq, before.Hash, after.Seq, after.Hash)
	}
}

// ---- crash test against the real binary ----

func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "exchange")
	out, err := exec.Command("go", "build", "-o", bin, "github.com/Shailu-s/matching-engine/cmd/exchange").CombinedOutput()
	if err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return bin
}

func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

type proc struct {
	cmd   *exec.Cmd
	addr  string
	admin string
}

func start(t *testing.T, bin, dir string) *proc {
	t.Helper()
	p := &proc{addr: freeAddr(t), admin: freeAddr(t)}
	p.cmd = exec.Command(bin, "-dir", dir, "-addr", p.addr, "-admin", p.admin, "-snapshot-every", "40000")
	p.cmd.Stderr = os.Stderr
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if s, err := p.state(); err == nil && s.Seq >= 0 {
			return p
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("exchange did not come up")
	return nil
}

func (p *proc) state() (exchange.State, error) {
	var s exchange.State
	resp, err := http.Get("http://" + p.admin + "/state")
	if err != nil {
		return s, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return s, fmt.Errorf("status %d", resp.StatusCode)
	}
	return s, json.NewDecoder(resp.Body).Decode(&s)
}

// traffic sends orders until stop is closed and records the highest seq the
// exchange acknowledged. An ack means "journaled and applied".
func traffic(t *testing.T, addr string, seed int64, stop <-chan struct{}) *atomic.Uint64 {
	var maxAcked atomic.Uint64
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			g := loadtest.NewGen(seed+int64(i), 1+uint32(i*20), 20)
			var gmu sync.Mutex
			type sent struct{ cmd core.Cmd }
			inflight := make(chan sent, 128)
			c, err := wire.Dial(addr, func(_ uint64, r core.Result) {
				s := <-inflight
				for {
					m := maxAcked.Load()
					if r.Seq <= m || maxAcked.CompareAndSwap(m, r.Seq) {
						break
					}
				}
				gmu.Lock()
				g.Observe(&s.cmd, r)
				gmu.Unlock()
			})
			if err != nil {
				return
			}
			defer c.Close()
			setup := g.Setup()
			for _, cmd := range setup {
				inflight <- sent{cmd}
				c.Send(0, &cmd)
			}
			c.Flush()
			for {
				select {
				case <-stop:
					return
				case <-c.Done:
					return // server died under us
				default:
				}
				gmu.Lock()
				cmd := g.Next()
				gmu.Unlock()
				select {
				case inflight <- sent{cmd}:
				default:
					c.Flush()
					select {
					case inflight <- sent{cmd}:
					case <-c.Done:
						return
					}
				}
				if c.Send(0, &cmd) != nil {
					return
				}
			}
		}(i)
	}
	go func() { <-stop; wg.Wait() }()
	return &maxAcked
}

func verify(t *testing.T, bin, dir string) {
	t.Helper()
	out, err := exec.Command(bin, "-dir", dir, "-verify").Output()
	if err != nil {
		t.Fatalf("verify failed: %v\n%s", err, out)
	}
	var v struct{ Match bool }
	json.Unmarshal(out, &v)
	if !v.Match {
		t.Fatalf("snapshot+replay != journal-only replay:\n%s", out)
	}
}

// Kill -9 the exchange mid-traffic, several times, sometimes leaving a torn
// record at the end of the journal. After every restart:
//   - every order the exchange acknowledged is still there (seq >= max acked)
//   - snapshot + journal replay equals a replay of the whole journal
func TestKill9Recovery(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and kills a real process")
	}
	bin := buildBinary(t)
	dir := t.TempDir()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for round := 0; round < 4; round++ {
		p := start(t, bin, dir)
		s0, _ := p.state()
		stop := make(chan struct{})
		acked := traffic(t, p.addr, int64(round*100), stop)
		time.Sleep(time.Duration(300+rng.Intn(700)) * time.Millisecond)

		p.cmd.Process.Signal(syscall.SIGKILL) // no shutdown hook, no final snapshot
		p.cmd.Wait()
		close(stop)
		maxAcked := acked.Load()
		if maxAcked <= s0.Seq {
			t.Fatalf("round %d: no traffic was acknowledged", round)
		}
		if round%2 == 1 {
			// simulate a torn write: half a record of garbage at the tail
			f, _ := os.OpenFile(filepath.Join(dir, "journal.log"), os.O_APPEND|os.O_WRONLY, 0)
			f.Write([]byte("torn-write-garbage"))
			f.Close()
		}

		verify(t, bin, dir)
		p = start(t, bin, dir)
		s, err := p.state()
		if err != nil {
			t.Fatal(err)
		}
		if s.Seq < maxAcked {
			t.Fatalf("round %d: lost acknowledged orders: max acked seq %d, recovered seq %d", round, maxAcked, s.Seq)
		}
		if round%2 == 1 && s.Recovery.TruncatedBytes == 0 {
			t.Fatalf("round %d: torn tail was not truncated", round)
		}
		t.Logf("round %d: killed after acking seq %d; recovered seq %d from snapshot@%d + %d records in %.0f ms, truncated %d bytes",
			round, maxAcked, s.Seq, s.Recovery.SnapshotSeq, s.Recovery.Replayed, s.Recovery.Ms, s.Recovery.TruncatedBytes)
		p.cmd.Process.Signal(syscall.SIGKILL)
		p.cmd.Wait()
	}
}
