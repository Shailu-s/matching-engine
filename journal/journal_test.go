package journal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Shailu-s/matching-engine/core"
)

func cmds(from, n int) []core.Cmd {
	out := make([]core.Cmd, n)
	for i := range out {
		out[i] = core.Cmd{Seq: uint64(from + i), Kind: core.CmdPlace, Account: uint32(i), Market: 1, Side: 1, IOC: i%2 == 0, Price: 320_000 + int64(i), Qty: 7, OrderID: 99}
	}
	return out
}

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j.log")
	w, err := OpenWriter(path, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	in := append(cmds(1, 100), cmds(101, 50)...)
	w.Append(in[:100])
	w.Append(in[100:])
	w.Close()
	var got []core.Cmd
	good, err := Read(path, func(c core.Cmd) error { got = append(got, c); return nil })
	if err != nil || good != int64(len(in)*RecordSize) || len(got) != len(in) {
		t.Fatalf("good=%d n=%d err=%v", good, len(got), err)
	}
	for i := range in {
		if got[i] != in[i] {
			t.Fatalf("record %d: got %+v want %+v", i, got[i], in[i])
		}
	}
}

// A crash mid-write leaves a partial record, or a full-length one with
// garbage. Read must stop at the last good record, and reopening at that
// offset must drop the tail so new appends follow cleanly.
func TestTornTailIsTruncated(t *testing.T) {
	for name, tail := range map[string][]byte{
		"partial": make([]byte, RecordSize/2),
		"corrupt": func() []byte { b := make([]byte, RecordSize); encode(b, &core.Cmd{Seq: 11}); b[20] ^= 0xff; return b }(),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "j.log")
			w, _ := OpenWriter(path, 0, false)
			w.Append(cmds(1, 10))
			w.Close()
			f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
			f.Write(tail)
			f.Close()

			n := 0
			good, _ := Read(path, func(core.Cmd) error { n++; return nil })
			if n != 10 || good != 10*RecordSize {
				t.Fatalf("read %d records, good=%d", n, good)
			}
			w, _ = OpenWriter(path, good, false)
			w.Append(cmds(11, 5))
			w.Close()
			var last core.Cmd
			n = 0
			Read(path, func(c core.Cmd) error { n++; last = c; return nil })
			if n != 15 || last.Seq != 15 {
				t.Fatalf("after reopen: %d records, last seq %d", n, last.Seq)
			}
		})
	}
}

func TestMissingFileIsEmpty(t *testing.T) {
	good, err := Read(filepath.Join(t.TempDir(), "nope"), func(core.Cmd) error { t.Fatal("called"); return nil })
	if good != 0 || err != nil {
		t.Fatalf("good=%d err=%v", good, err)
	}
}
