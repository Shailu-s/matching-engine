// Package wire is the gateway's binary protocol over TCP. Fixed-size,
// little-endian frames: no JSON parsing on the hot path.
//
// Requests start with a type byte:
//
//	1 place   clientID u64 | account u32 | market u8 | side u8 | ioc u8 | price i64 | qty i64
//	2 cancel  clientID u64 | account u32 | orderID u64
//	3 deposit clientID u64 | account u32 | asset u8 | amount i64
//
// Every request gets exactly one response, in order per connection:
//
//	clientID u64 | seq u64 | status u8 | filled i64 | resting i64 | trades u32
//
// clientID is the sender's own correlation number; seq is the exchange's
// sequence number, which is also the order ID for cancels.
package wire

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/Shailu-s/matching-engine/core"
)

const (
	tPlace   = 1
	tCancel  = 2
	tDeposit = 3

	placeLen   = 31
	cancelLen  = 20
	depositLen = 21
	ResultLen  = 37
	MaxReqLen  = 1 + placeLen
)

var le = binary.LittleEndian

var ErrBadFrame = errors.New("wire: unknown frame type")

func AppendRequest(dst []byte, clientID uint64, c *core.Cmd) []byte {
	var b [MaxReqLen]byte
	le.PutUint64(b[1:], clientID)
	le.PutUint32(b[9:], c.Account)
	switch c.Kind {
	case core.CmdPlace:
		b[0] = tPlace
		b[13], b[14] = c.Market, c.Side
		if c.IOC {
			b[15] = 1
		}
		le.PutUint64(b[16:], uint64(c.Price))
		le.PutUint64(b[24:], uint64(c.Qty))
		return append(dst, b[:1+placeLen]...)
	case core.CmdCancel:
		b[0] = tCancel
		le.PutUint64(b[13:], c.OrderID)
		return append(dst, b[:1+cancelLen]...)
	default:
		b[0] = tDeposit
		b[13] = c.Asset
		le.PutUint64(b[14:], uint64(c.Qty))
		return append(dst, b[:1+depositLen]...)
	}
}

// ReadRequest reads one request. buf must be at least MaxReqLen bytes.
func ReadRequest(r *bufio.Reader, buf []byte) (uint64, core.Cmd, error) {
	t, err := r.ReadByte()
	if err != nil {
		return 0, core.Cmd{}, err
	}
	var n int
	switch t {
	case tPlace:
		n = placeLen
	case tCancel:
		n = cancelLen
	case tDeposit:
		n = depositLen
	default:
		return 0, core.Cmd{}, ErrBadFrame
	}
	b := buf[:n]
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, core.Cmd{}, err
	}
	id := le.Uint64(b[0:])
	c := core.Cmd{Account: le.Uint32(b[8:])}
	switch t {
	case tPlace:
		c.Kind = core.CmdPlace
		c.Market, c.Side, c.IOC = b[12], b[13], b[14] == 1
		c.Price = int64(le.Uint64(b[15:]))
		c.Qty = int64(le.Uint64(b[23:]))
	case tCancel:
		c.Kind = core.CmdCancel
		c.OrderID = le.Uint64(b[12:])
	case tDeposit:
		c.Kind = core.CmdDeposit
		c.Asset = b[12]
		c.Qty = int64(le.Uint64(b[13:]))
	}
	return id, c, nil
}

func AppendResult(dst []byte, clientID uint64, r core.Result) []byte {
	var b [ResultLen]byte
	le.PutUint64(b[0:], clientID)
	le.PutUint64(b[8:], r.Seq)
	b[16] = byte(r.Status)
	le.PutUint64(b[17:], uint64(r.Filled))
	le.PutUint64(b[25:], uint64(r.Resting))
	le.PutUint32(b[33:], r.Trades)
	return append(dst, b[:]...)
}

func DecodeResult(b []byte) (uint64, core.Result) {
	return le.Uint64(b[0:]), core.Result{
		Seq: le.Uint64(b[8:]), Status: core.Status(b[16]), Filled: int64(le.Uint64(b[17:])),
		Resting: int64(le.Uint64(b[25:])), Trades: le.Uint32(b[33:]),
	}
}

// Client is a pipelined connection: Send many requests, results arrive on
// the callback in the same order. Send is not safe for concurrent use.
type Client struct {
	conn net.Conn
	bw   *bufio.Writer
	buf  []byte
	Done chan struct{} // closed when the reader stops
	Err  error
	once sync.Once
}

func Dial(addr string, onResult func(clientID uint64, r core.Result)) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	c := &Client{conn: conn, bw: bufio.NewWriterSize(conn, 64<<10), Done: make(chan struct{})}
	go func() {
		defer close(c.Done)
		br := bufio.NewReaderSize(conn, 64<<10)
		b := make([]byte, ResultLen)
		for {
			if _, err := io.ReadFull(br, b); err != nil {
				c.Err = err
				return
			}
			onResult(DecodeResult(b))
		}
	}()
	return c, nil
}

func (c *Client) Send(clientID uint64, cmd *core.Cmd) error {
	c.buf = AppendRequest(c.buf[:0], clientID, cmd)
	_, err := c.bw.Write(c.buf)
	return err
}

func (c *Client) Flush() error { return c.bw.Flush() }

func (c *Client) Close() error {
	var err error
	c.once.Do(func() { err = c.conn.Close() })
	return err
}
