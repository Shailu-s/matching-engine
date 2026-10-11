// Package exchange wires the hot path:
//
//	gateway -> sequencer (stamp seq, batch, append, fsync) -> core (risk, match, settle) -> reply
//
// Two goroutines, connected by a channel, so the journal can flush batch
// N+1 while the core is still applying batch N. A command is answered only
// after it is durable in the journal AND applied, so an acknowledged order
// survives a crash.
package exchange

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shailu-s/matching-engine/core"
	"github.com/Shailu-s/matching-engine/journal"
)

type Config struct {
	Dir           string
	Fsync         bool   // false only for benchmarking the cost of fsync
	MaxBatch      int    // commands per journal write; default 8192
	SnapshotEvery uint64 // take a snapshot every N commands; 0 = never (except on Close)
}

// Reply is called on the core goroutine. It must not block.
type Reply func(core.Result)

type req struct {
	cmd   core.Cmd
	reply Reply
	at    int64 // when Submit accepted it, for server-side latency
}

type Exchange struct {
	cfg   Config
	core  *core.Core
	jw    *journal.Writer
	in    chan req
	coreQ chan any // []req batches, or func() control calls, in order
	done  chan struct{}
	seq   uint64 // last seq stamped; sequencer goroutine only

	closeMu  sync.RWMutex
	closed   bool
	lastSnap uint64

	Recovery RecoveryInfo

	// stats
	batches  atomic.Uint64
	entries  atomic.Uint64
	maxBatch atomic.Uint64
	syncNs   atomic.Uint64

	// server-side latency, Submit -> reply, and journal append+fsync time
	lat    hist
	append hist
}

type RecoveryInfo struct {
	SnapshotSeq    uint64  `json:"snapshot_seq"`
	Replayed       int     `json:"replayed_from_journal"`
	TruncatedBytes int64   `json:"torn_tail_bytes_truncated"`
	Seq            uint64  `json:"seq_after_recovery"`
	Ms             float64 `json:"took_ms"`
}

const journalFile = "journal.log"

// Open recovers state from dir: newest valid snapshot, then every journal
// record after it, then truncates any torn tail. Nothing runs until Start.
func Open(cfg Config) (*Exchange, error) {
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 8192
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	t0 := time.Now()
	c, info, err := Recover(cfg.Dir)
	if err != nil {
		return nil, err
	}
	jw, err := journal.OpenWriter(filepath.Join(cfg.Dir, journalFile), info.good, cfg.Fsync)
	if err != nil {
		return nil, err
	}
	info.ri.Ms = float64(time.Since(t0).Microseconds()) / 1000
	x := &Exchange{
		cfg: cfg, core: c, jw: jw, seq: c.Seq(), lastSnap: info.ri.SnapshotSeq, Recovery: info.ri,
		in: make(chan req, 1<<16), coreQ: make(chan any, 64), done: make(chan struct{}),
	}
	return x, nil
}

type recovered struct {
	ri   RecoveryInfo
	good int64
}

func (r recovered) SnapshotSeq() uint64 { return r.ri.SnapshotSeq }
func (r recovered) Replayed() int       { return r.ri.Replayed }

// Recover rebuilds the core from dir without opening anything for writing.
// It is also what the offline verifier uses.
func Recover(dir string) (*core.Core, recovered, error) {
	var out recovered
	c, snapSeq, err := loadLatestSnapshot(dir)
	if err != nil {
		return nil, out, err
	}
	out.ri.SnapshotSeq = snapSeq
	path := filepath.Join(dir, journalFile)
	good, err := journal.Read(path, func(cmd core.Cmd) error {
		if cmd.Seq <= c.Seq() {
			return nil // already in the snapshot
		}
		if cmd.Seq != c.Seq()+1 {
			return fmt.Errorf("journal gap: have seq %d, next record is %d", c.Seq(), cmd.Seq)
		}
		c.Apply(&cmd)
		out.ri.Replayed++
		return nil
	})
	if err != nil {
		return nil, out, err
	}
	if st, err := os.Stat(path); err == nil {
		out.ri.TruncatedBytes = st.Size() - good
	}
	out.good = good
	out.ri.Seq = c.Seq()
	return c, out, nil
}

// ReplayJournalOnly ignores snapshots and rebuilds from the first record.
// Comparing its hash with the live state proves snapshot + replay is exact.
func ReplayJournalOnly(dir string) (*core.Core, error) {
	c := core.New()
	_, err := journal.Read(filepath.Join(dir, journalFile), func(cmd core.Cmd) error {
		if cmd.Seq != c.Seq()+1 {
			return fmt.Errorf("journal gap at %d", cmd.Seq)
		}
		c.Apply(&cmd)
		return nil
	})
	return c, err
}

func (x *Exchange) Start() {
	go x.sequencer()
	go x.processor()
}

// Submit queues a command. It blocks only if the inbound queue is full,
// which is the backpressure that keeps memory bounded.
func (x *Exchange) Submit(cmd core.Cmd, reply Reply) error {
	x.closeMu.RLock()
	defer x.closeMu.RUnlock()
	if x.closed {
		return errors.New("exchange closed")
	}
	x.in <- req{cmd, reply, time.Now().UnixNano()}
	return nil
}

// sequencer is the single writer: it alone assigns sequence numbers and
// appends to the journal, so the journal order IS the processing order.
func (x *Exchange) sequencer() {
	defer close(x.coreQ)
	cmds := make([]core.Cmd, 0, x.cfg.MaxBatch)
	for {
		r, ok := <-x.in
		if !ok {
			return
		}
		batch := make([]req, 0, 64)
		batch = append(batch, r)
		// take whatever else is already waiting: under load batches grow
		// on their own while the previous fsync is in flight
	drain:
		for len(batch) < x.cfg.MaxBatch {
			select {
			case r, ok := <-x.in:
				if !ok {
					break drain
				}
				batch = append(batch, r)
			default:
				break drain
			}
		}
		cmds = cmds[:0]
		for i := range batch {
			x.seq++
			batch[i].cmd.Seq = x.seq
			cmds = append(cmds, batch[i].cmd)
		}
		t0 := time.Now()
		if err := x.jw.Append(cmds); err != nil {
			// a journal we cannot write is a journal we cannot trust
			log.Fatalf("journal append failed at seq %d: %v", x.seq, err)
		}
		took := time.Since(t0).Nanoseconds()
		x.syncNs.Add(uint64(took))
		x.append.record(took)
		x.batches.Add(1)
		x.entries.Add(uint64(len(batch)))
		if n := uint64(len(batch)); n > x.maxBatch.Load() {
			x.maxBatch.Store(n)
		}
		x.coreQ <- batch
	}
}

// processor owns the core. Only this goroutine touches it, so risk and
// matching need no locks.
func (x *Exchange) processor() {
	defer close(x.done)
	for item := range x.coreQ {
		switch v := item.(type) {
		case []req:
			for i := range v {
				res := x.core.Apply(&v[i].cmd)
				if v[i].reply != nil {
					v[i].reply(res)
				}
			}
			now := time.Now().UnixNano()
			for i := range v {
				x.lat.record(now - v[i].at)
			}
			if x.cfg.SnapshotEvery > 0 && x.core.Seq()-x.lastSnap >= x.cfg.SnapshotEvery {
				if err := x.snapshot(); err != nil {
					log.Printf("snapshot failed: %v", err)
				}
			}
		case func():
			v()
		}
	}
}

// do runs f on the core goroutine between batches, where state is
// consistent with everything journaled so far.
func (x *Exchange) do(f func()) error {
	x.closeMu.RLock()
	if x.closed {
		x.closeMu.RUnlock()
		return errors.New("exchange closed")
	}
	done := make(chan struct{})
	x.coreQ <- func() { f(); close(done) }
	x.closeMu.RUnlock()
	<-done
	return nil
}

type State struct {
	Seq          uint64       `json:"seq"`
	Hash         string       `json:"state_hash"`
	Accounts     int          `json:"accounts"`
	Resting      int          `json:"resting_orders"`
	Trades       uint64       `json:"trades"`
	JournalBytes int64        `json:"journal_bytes"`
	Batches      uint64       `json:"journal_batches"`
	AvgBatch     float64      `json:"avg_batch"`
	MaxBatch     uint64       `json:"max_batch"`
	AvgAppendUs  float64      `json:"avg_append_us"`
	Fsync        bool         `json:"fsync"`
	LastSnapshot uint64       `json:"last_snapshot_seq"`
	Recovery     RecoveryInfo `json:"recovery"`
	ServerLat    Latency      `json:"server_latency"`
	AppendLat    Latency      `json:"journal_append_latency"`
}

// ResetLatency clears the latency histograms, e.g. after warmup.
func (x *Exchange) ResetLatency() { x.lat.reset(); x.append.reset() }

func (x *Exchange) State() (State, error) {
	var s State
	err := x.do(func() {
		s = State{
			Seq: x.core.Seq(), Hash: x.core.Hash(), Accounts: x.core.Accounts(), Resting: x.core.Resting(),
			Trades: x.core.Trades(), LastSnapshot: x.lastSnap, Recovery: x.Recovery, Fsync: x.cfg.Fsync,
		}
	})
	b, e := x.batches.Load(), x.entries.Load()
	s.Batches, s.MaxBatch = b, x.maxBatch.Load()
	if b > 0 {
		s.AvgBatch = float64(e) / float64(b)
		s.AvgAppendUs = float64(x.syncNs.Load()) / float64(b) / 1000
	}
	s.JournalBytes = x.jw.Size()
	s.ServerLat, s.AppendLat = x.lat.snapshot(), x.append.snapshot()
	return s, err
}

func (x *Exchange) Snapshot() error {
	var err error
	if e := x.do(func() { err = x.snapshot() }); e != nil {
		return e
	}
	return err
}

// Close stops intake, drains everything already accepted, takes a final
// snapshot and closes the journal. kill -9 skips all of this; recovery
// handles that case.
func (x *Exchange) Close() error {
	x.closeMu.Lock()
	if x.closed {
		x.closeMu.Unlock()
		return nil
	}
	x.closed = true
	close(x.in)
	x.closeMu.Unlock()
	<-x.done
	err := x.snapshot()
	if e := x.jw.Close(); err == nil {
		err = e
	}
	return err
}

// ---- snapshots ----

func snapName(seq uint64) string { return fmt.Sprintf("snapshot-%020d.snap", seq) }

// snapshot writes the state atomically: temp file, fsync, rename, fsync the
// directory. A crash at any point leaves either the old or the new file.
func (x *Exchange) snapshot() error {
	seq := x.core.Seq()
	if seq == x.lastSnap {
		return nil
	}
	final := filepath.Join(x.cfg.Dir, snapName(seq))
	tmp := final + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := x.core.WriteSnapshot(f); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	if d, err := os.Open(x.cfg.Dir); err == nil {
		d.Sync()
		d.Close()
	}
	x.lastSnap = seq
	// keep the two newest; older ones are covered by them plus the journal
	snaps, _ := listSnapshots(x.cfg.Dir)
	for i := 0; i+2 < len(snaps); i++ {
		os.Remove(filepath.Join(x.cfg.Dir, snaps[i]))
	}
	return nil
}

func listSnapshots(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range ents {
		if n := e.Name(); strings.HasPrefix(n, "snapshot-") && strings.HasSuffix(n, ".snap") {
			out = append(out, n)
		}
	}
	sort.Strings(out) // zero-padded seq, so lexical = numeric
	return out, nil
}

// loadLatestSnapshot tries snapshots newest first and falls back to an
// empty core, so one damaged snapshot only costs a longer replay.
func loadLatestSnapshot(dir string) (*core.Core, uint64, error) {
	snaps, err := listSnapshots(dir)
	if err != nil {
		return nil, 0, err
	}
	for i := len(snaps) - 1; i >= 0; i-- {
		f, err := os.Open(filepath.Join(dir, snaps[i]))
		if err != nil {
			continue
		}
		c, err := core.ReadSnapshot(f)
		f.Close()
		if err != nil {
			log.Printf("skipping %s: %v", snaps[i], err)
			continue
		}
		return c, c.Seq(), nil
	}
	return core.New(), 0, nil
}
