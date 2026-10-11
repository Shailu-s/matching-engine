package exchange

import (
	"testing"
	"time"
)

func TestHistQuantiles(t *testing.T) {
	var h hist
	for i := 1; i <= 1000; i++ {
		h.record(int64(i) * int64(time.Microsecond))
	}
	for _, c := range []struct {
		q    float64
		want time.Duration
	}{{.5, 500 * time.Microsecond}, {.99, 990 * time.Microsecond}} {
		got := h.quantile(c.q)
		if got < c.want*85/100 || got > c.want*110/100 {
			t.Fatalf("q%.2f = %v, want ~%v", c.q, got, c.want)
		}
	}
	if h.snapshot().Max != time.Millisecond {
		t.Fatalf("max %v", h.snapshot().Max)
	}
}
