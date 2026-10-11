// Package journal is the append-only input log: every command, in sequence
// order, written to disk before it is processed.
//
// Record layout (little endian), fixed size:
//
//	length u32 | crc32c(payload) u32 | payload (41 bytes)
//	payload = seq u64 | kind u8 | account u32 | market u8 | side u8 |
//	          ioc u8 | asset u8 | price i64 | qty i64 | orderID u64
//
// A crash can leave a half-written last record. The CRC catches it, and
// recovery truncates the file back to the last good record.
package journal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync/atomic"

	"github.com/Shailu-s/matching-engine/core"
)

const (
	payloadSize = 41
	RecordSize  = 8 + payloadSize
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func encode(dst []byte, c *core.Cmd) {
	le := binary.LittleEndian
	p := dst[8:]
	le.PutUint64(p[0:], c.Seq)
	p[8] = byte(c.Kind)
	le.PutUint32(p[9:], c.Account)
	p[13] = c.Market
	p[14] = c.Side
	if c.IOC {
		p[15] = 1
	} else {
		p[15] = 0
	}
	p[16] = c.Asset
	le.PutUint64(p[17:], uint64(c.Price))
	le.PutUint64(p[25:], uint64(c.Qty))
	le.PutUint64(p[33:], c.OrderID)
	le.PutUint32(dst[0:], payloadSize)
	le.PutUint32(dst[4:], crc32.Checksum(p[:payloadSize], castagnoli))
}

func decode(rec []byte) (core.Cmd, bool) {
	le := binary.LittleEndian
	if le.Uint32(rec[0:]) != payloadSize {
		return core.Cmd{}, false
	}
	p := rec[8:RecordSize]
	if crc32.Checksum(p, castagnoli) != le.Uint32(rec[4:]) {
		return core.Cmd{}, false
	}
	return core.Cmd{
		Seq: le.Uint64(p[0:]), Kind: core.Kind(p[8]), Account: le.Uint32(p[9:]),
		Market: p[13], Side: p[14], IOC: p[15] == 1, Asset: p[16],
		Price: int64(le.Uint64(p[17:])), Qty: int64(le.Uint64(p[25:])), OrderID: le.Uint64(p[33:]),
	}, true
}

// Writer appends batches. It is used by exactly one goroutine, the
// sequencer, so it needs no locking.
type Writer struct {
	f     *os.File
	fsync bool
	buf   []byte
	size  atomic.Int64 // read by stats from other goroutines
}

// OpenWriter opens path for appending at offset (the end of the last good
// record, as returned by Read), truncating anything after it.
func OpenWriter(path string, offset int64, fsync bool) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(offset); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	w := &Writer{f: f, fsync: fsync}
	w.size.Store(offset)
	return w, nil
}

// Append writes a whole batch with one write call and, if enabled, one
// fsync. That single fsync covering many commands is group commit: the
// disk's flush latency is paid once per batch, not once per order.
func (w *Writer) Append(cmds []core.Cmd) error {
	need := len(cmds) * RecordSize
	if cap(w.buf) < need {
		w.buf = make([]byte, need)
	}
	b := w.buf[:need]
	for i := range cmds {
		encode(b[i*RecordSize:], &cmds[i])
	}
	if _, err := w.f.Write(b); err != nil {
		return err
	}
	w.size.Add(int64(need))
	if w.fsync {
		return w.f.Sync() // on macOS Go issues F_FULLFSYNC, a real flush to media
	}
	return nil
}

func (w *Writer) Size() int64  { return w.size.Load() }
func (w *Writer) Close() error { return w.f.Close() }

// Read calls fn for every intact record in order and returns the offset just
// past the last good one. A short or corrupt record ends the log: everything
// from there on is a torn write and should be truncated.
func Read(path string, fn func(core.Cmd) error) (good int64, err error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 1<<20)
	rec := make([]byte, RecordSize)
	for {
		if _, err := io.ReadFull(br, rec); err != nil {
			return good, nil // EOF or torn tail
		}
		c, ok := decode(rec)
		if !ok {
			return good, nil
		}
		if err := fn(c); err != nil {
			return good, err
		}
		good += RecordSize
	}
}
