package wire

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/Shailu-s/matching-engine/core"
)

func TestRequestRoundTrip(t *testing.T) {
	in := []core.Cmd{
		{Kind: core.CmdPlace, Account: 7, Market: 2, Side: core.Sell, IOC: true, Price: 15_123, Qty: 42},
		{Kind: core.CmdCancel, Account: 9, OrderID: 1 << 40},
		{Kind: core.CmdDeposit, Account: 3, Asset: 2, Qty: 1 << 45},
	}
	var buf []byte
	for i := range in {
		buf = AppendRequest(buf, uint64(100+i), &in[i])
	}
	r := bufio.NewReader(bytes.NewReader(buf))
	scratch := make([]byte, MaxReqLen)
	for i := range in {
		id, got, err := ReadRequest(r, scratch)
		if err != nil || id != uint64(100+i) || got != in[i] {
			t.Fatalf("frame %d: id=%d got=%+v err=%v want %+v", i, id, got, err, in[i])
		}
	}
	if _, _, err := ReadRequest(bufio.NewReader(bytes.NewReader([]byte{9})), scratch); err != ErrBadFrame {
		t.Fatalf("want ErrBadFrame, got %v", err)
	}
}

func TestResultRoundTrip(t *testing.T) {
	want := core.Result{Seq: 1 << 33, Status: core.ErrNoCoins, Filled: 5, Resting: 9, Trades: 3}
	b := AppendResult(nil, 77, want)
	if len(b) != ResultLen {
		t.Fatalf("len %d", len(b))
	}
	id, got := DecodeResult(b)
	if id != 77 || got != want {
		t.Fatalf("got %d %+v", id, got)
	}
}
