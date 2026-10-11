// Command exchange runs the durable hot path: TCP gateway -> sequencer +
// journal -> risk + matching core, with an admin HTTP endpoint.
//
//	exchange -dir data                 serve (recovers from data/ first)
//	exchange -dir data -verify         offline check: snapshot+replay vs journal-only replay
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shailu-s/matching-engine/exchange"
)

func main() {
	dir := flag.String("dir", "data", "journal and snapshot directory")
	addr := flag.String("addr", "127.0.0.1:9100", "gateway TCP address")
	admin := flag.String("admin", "127.0.0.1:9101", "admin HTTP address (GET /state, POST /snapshot)")
	fsync := flag.Bool("fsync", true, "fsync each journal batch before acknowledging")
	snapEvery := flag.Uint64("snapshot-every", 2_000_000, "snapshot every N commands (0 = only on shutdown)")
	verify := flag.Bool("verify", false, "recover offline, print both hashes, exit non-zero if they differ")
	flag.Parse()

	if *verify {
		os.Exit(runVerify(*dir))
	}

	x, err := exchange.Open(exchange.Config{Dir: *dir, Fsync: *fsync, SnapshotEvery: *snapEvery})
	if err != nil {
		log.Fatal(err)
	}
	r := x.Recovery
	log.Printf("recovered: snapshot@%d + %d journal records -> seq %d in %.1f ms (torn tail truncated: %d bytes)",
		r.SnapshotSeq, r.Replayed, r.Seq, r.Ms, r.TruncatedBytes)
	x.Start()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	go func() { log.Fatal(http.ListenAndServe(*admin, x.AdminHandler())) }()
	go x.Serve(ln)
	log.Printf("gateway on %s, admin on %s, fsync=%v", ln.Addr(), *admin, *fsync)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down: draining and taking a final snapshot")
	ln.Close()
	if err := x.Close(); err != nil {
		log.Fatal(err)
	}
}

// runVerify rebuilds the state two independent ways and compares hashes.
func runVerify(dir string) int {
	a, info, err := exchange.Recover(dir)
	if err != nil {
		log.Fatal(err)
	}
	b, err := exchange.ReplayJournalOnly(dir)
	if err != nil {
		log.Fatal(err)
	}
	out := map[string]any{
		"snapshot_plus_replay": map[string]any{"snapshot_seq": info.SnapshotSeq(), "replayed": info.Replayed(), "seq": a.Seq(), "hash": a.Hash()},
		"journal_only_replay":  map[string]any{"seq": b.Seq(), "hash": b.Hash()},
		"match":                a.Hash() == b.Hash() && a.Seq() == b.Seq(),
		"resting_orders":       a.Resting(),
		"accounts":             a.Accounts(),
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(out)
	if a.Hash() != b.Hash() {
		fmt.Fprintln(os.Stderr, "MISMATCH")
		return 1
	}
	return 0
}
