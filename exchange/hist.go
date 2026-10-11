package exchange

import (
	"math/bits"
	"sync/atomic"
	"time"
)

// hist is a lock-free latency histogram: log2 buckets split into 8 linear
// sub-buckets (~12% resolution) over nanoseconds. Cheap enough to record
// every command on the core goroutine.
type hist struct {
	b   [64 * 8]atomic.Uint64
	n   atomic.Uint64
	max atomic.Int64
}

func bucket(ns int64) int {
	if ns < 8 {
		return int(ns)
	}
	e := bits.Len64(uint64(ns)) - 1 // ns in [2^e, 2^(e+1))
	sub := int((uint64(ns) >> (e - 3)) & 7)
	return e*8 + sub
}

// lower bound of a bucket, for reporting
func bucketValue(i int) int64 {
	if i < 8 {
		return int64(i)
	}
	e, sub := i/8, i%8
	return (int64(8) + int64(sub)) << (e - 3)
}

func (h *hist) record(ns int64) {
	if ns < 0 {
		ns = 0
	}
	h.b[bucket(ns)].Add(1)
	h.n.Add(1)
	for {
		m := h.max.Load()
		if ns <= m || h.max.CompareAndSwap(m, ns) {
			return
		}
	}
}

func (h *hist) quantile(q float64) time.Duration {
	n := h.n.Load()
	if n == 0 {
		return 0
	}
	target := uint64(q * float64(n))
	var seen uint64
	for i := range h.b {
		seen += h.b[i].Load()
		if seen > target {
			return time.Duration(bucketValue(i))
		}
	}
	return time.Duration(h.max.Load())
}

func (h *hist) reset() {
	for i := range h.b {
		h.b[i].Store(0)
	}
	h.n.Store(0)
	h.max.Store(0)
}

type Latency struct {
	Count uint64        `json:"count"`
	P50   time.Duration `json:"p50_ns"`
	P99   time.Duration `json:"p99_ns"`
	P999  time.Duration `json:"p999_ns"`
	Max   time.Duration `json:"max_ns"`
}

func (h *hist) snapshot() Latency {
	return Latency{Count: h.n.Load(), P50: h.quantile(.5), P99: h.quantile(.99), P999: h.quantile(.999), Max: time.Duration(h.max.Load())}
}
