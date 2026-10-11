// Command loadgen drives the exchange gateway over TCP from a separate
// process and reports throughput and latency percentiles.
//
//	loadgen -addr 127.0.0.1:9100                 closed loop: as fast as acks allow
//	loadgen -addr 127.0.0.1:9100 -rate 200000    fixed rate, latency from scheduled send time
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Shailu-s/matching-engine/loadtest"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9100", "gateway address")
	conns := flag.Int("conns", 16, "connections")
	window := flag.Int("window", 256, "max in-flight requests per connection")
	rate := flag.Int("rate", 0, "total orders/sec (0 = closed loop)")
	dur := flag.Duration("duration", 10*time.Second, "measured duration")
	warm := flag.Duration("warmup", 2*time.Second, "unmeasured warmup")
	accts := flag.Int("accounts", 50, "accounts per connection")
	seed := flag.Int64("seed", time.Now().UnixNano(), "random seed")
	setup := flag.Bool("setup", true, "fund the accounts first")
	asJSON := flag.Bool("json", false, "print the report as JSON")
	flag.Parse()

	cfg := loadtest.Config{Conns: *conns, Window: *window, Rate: *rate, Duration: *dur, Warmup: *warm, Accounts: *accts, Seed: *seed}
	d := loadtest.TCP(*addr)
	if *setup {
		if err := loadtest.Setup(d, cfg); err != nil {
			log.Fatal(err)
		}
	}
	rep, err := loadtest.Run(d, cfg)
	if err != nil {
		log.Fatal(err)
	}
	if *asJSON {
		json.NewEncoder(os.Stdout).Encode(rep)
		return
	}
	mode := "closed loop"
	if *rate > 0 {
		mode = fmt.Sprintf("fixed rate %d/s", *rate)
	}
	fmt.Printf("%s, %d conns x window %d, %v measured\n%s\nrequests %d, trades %d, statuses %v\n",
		mode, *conns, *window, *dur, rep, rep.Measured, rep.Trades, rep.Statuses)
}
