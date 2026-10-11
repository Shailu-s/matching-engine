package core

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/Shailu-s/matching-engine/engine"
)

// Snapshot format, little endian:
//
//	magic "MESNAP01" | seq | trades | nAccounts | accounts sorted by id |
//	per market: nResting | resting orders in book priority order
//
// Every resting order is written once, from the book, in priority order, so
// restoring is just re-submitting them into empty books.
var snapMagic = [8]byte{'M', 'E', 'S', 'N', 'A', 'P', '0', '1'}

func (c *Core) WriteSnapshot(w io.Writer) error {
	bw := bufio.NewWriterSize(w, 1<<20)
	le := binary.LittleEndian
	var b [8]byte
	u64 := func(v uint64) { le.PutUint64(b[:], v); bw.Write(b[:8]) }
	u32 := func(v uint32) { le.PutUint32(b[:], v); bw.Write(b[:4]) }

	bw.Write(snapMagic[:])
	u64(c.seq)
	u64(c.trades)
	ids := c.sortedAccounts()
	u32(uint32(len(ids)))
	for _, id := range ids {
		a := c.accounts[id]
		u32(id)
		u64(uint64(a.cash))
		u64(uint64(a.cashHeld))
		for m := 0; m < nm; m++ {
			u64(uint64(a.coins[m]))
			u64(uint64(a.coinsHeld[m]))
		}
	}
	for m := 0; m < nm; m++ {
		u32(uint32(c.books[m].RestingOrders()))
		c.books[m].EachResting(func(eo *engine.Order) {
			o := c.orders[eo.ID]
			u64(eo.ID)
			u32(o.account)
			bw.WriteByte(o.side)
			u64(uint64(o.price))
			u64(uint64(o.remaining))
			u64(uint64(o.held))
		})
	}
	return bw.Flush()
}

func ReadSnapshot(r io.Reader) (*Core, error) {
	br := bufio.NewReaderSize(r, 1<<20)
	le := binary.LittleEndian
	var b [8]byte
	var err error
	u64 := func() uint64 {
		if err == nil {
			_, err = io.ReadFull(br, b[:8])
		}
		return le.Uint64(b[:8])
	}
	u32 := func() uint32 {
		if err == nil {
			_, err = io.ReadFull(br, b[:4])
		}
		return le.Uint32(b[:4])
	}
	var magic [8]byte
	if _, err := io.ReadFull(br, magic[:]); err != nil || magic != snapMagic {
		return nil, errors.New("not a snapshot file")
	}
	c := New()
	c.seq = u64()
	c.trades = u64()
	n := u32()
	for i := uint32(0); i < n && err == nil; i++ {
		id := u32()
		a := &account{cash: int64(u64()), cashHeld: int64(u64())}
		for m := 0; m < nm; m++ {
			a.coins[m] = int64(u64())
			a.coinsHeld[m] = int64(u64())
		}
		c.accounts[id] = a
	}
	for m := 0; m < nm && err == nil; m++ {
		k := u32()
		for i := uint32(0); i < k && err == nil; i++ {
			id := u64()
			o := &order{account: u32(), market: uint8(m)}
			var sb byte
			if err == nil {
				sb, err = br.ReadByte()
			}
			o.side = sb
			o.price = int64(u64())
			o.remaining = int64(u64())
			o.held = int64(u64())
			if err != nil {
				break
			}
			c.orders[id] = o
			eo := &engine.Order{ID: id, Side: engine.Side(o.side), Price: o.price, Qty: o.remaining, Owner: uint64(o.account)}
			if tr, e := c.books[m].Submit(eo); e != nil || len(tr) != 0 {
				return nil, fmt.Errorf("snapshot order %d does not restore cleanly", id)
			}
		}
	}
	if err != nil {
		return nil, fmt.Errorf("truncated snapshot: %w", err)
	}
	return c, nil
}

func (c *Core) sortedAccounts() []uint32 {
	ids := make([]uint32, 0, len(c.accounts))
	for id := range c.accounts {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// Hash is a SHA-256 over the whole state in a canonical order: the snapshot
// encoding itself. Two cores with the same hash hold identical balances,
// reservations and books, down to FIFO position within each price level.
func (c *Core) Hash() string {
	h := sha256.New()
	c.WriteSnapshot(h)
	return hex.EncodeToString(h.Sum(nil))
}
