package exchange

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"

	"github.com/Shailu-s/matching-engine/core"
	"github.com/Shailu-s/matching-engine/wire"
)

// MaxInflight caps unanswered requests per connection. The reader stops
// reading once a connection has this many in flight, and each connection's
// reply queue has exactly this capacity, so the core goroutine never blocks
// on a slow client.
const MaxInflight = 4096

// Serve accepts gateway connections until ln is closed.
func (x *Exchange) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go x.handle(conn)
	}
}

func (x *Exchange) handle(conn net.Conn) {
	defer conn.Close()
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	out := make(chan []byte, MaxInflight)
	slots := make(chan struct{}, MaxInflight)
	go writer(conn, out, slots)

	defer func() {
		// wait for every accepted request to be answered (the writer frees a
		// slot per reply), then stop the writer
		for i := 0; i < MaxInflight; i++ {
			slots <- struct{}{}
		}
		close(out)
	}()

	br := bufio.NewReaderSize(conn, 64<<10)
	buf := make([]byte, wire.MaxReqLen)
	for {
		id, cmd, err := wire.ReadRequest(br, buf)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Printf("gateway %s: %v", conn.RemoteAddr(), err)
			}
			return
		}
		slots <- struct{}{} // blocks at MaxInflight: per-connection backpressure
		if err := x.Submit(cmd, func(r core.Result) { out <- wire.AppendResult(nil, id, r) }); err != nil {
			<-slots
			return
		}
	}
}

// writer batches replies: it writes whatever is queued, then flushes once,
// so one syscall carries many acks under load.
func writer(conn net.Conn, out <-chan []byte, slots <-chan struct{}) {
	bw := bufio.NewWriterSize(conn, 64<<10)
	for b := range out {
		bw.Write(b)
		<-slots
		for n := len(out); n > 0; n-- {
			bw.Write(<-out)
			<-slots
		}
		if err := bw.Flush(); err != nil {
			// keep draining so the core never blocks on this connection
			for range out {
				<-slots
			}
			return
		}
	}
}

// AdminHandler exposes state for checks and demos: GET /state, POST /snapshot.
func (x *Exchange) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /state", func(w http.ResponseWriter, r *http.Request) {
		s, err := x.State()
		if err != nil {
			http.Error(w, err.Error(), 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(s)
	})
	mux.HandleFunc("POST /reset-latency", func(w http.ResponseWriter, r *http.Request) {
		x.ResetLatency()
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("POST /snapshot", func(w http.ResponseWriter, r *http.Request) {
		if err := x.Snapshot(); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Write([]byte("ok\n"))
	})
	return mux
}
