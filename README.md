# Matching Engine

In-memory limit order book and matching engine in Go. Price-time priority,
limit and market orders, O(1) cancel, one owner goroutine per symbol, live
order book and trade stream over Server-Sent Events.

## Benchmarks (Apple M4, Go 1.25)

| Path | Cost | Throughput |
|---|---|---|
| Raw book `Submit` (single core, 0 allocs/op) | 120-175 ns/order | ~5.7-8M orders/sec |
| Full engine path (concurrent clients -> channel -> owner goroutine -> reply) | 0.8-1.0 µs/order | ~1M orders/sec |

Order flow: random limits clustered +/-50 ticks around mid (most cross and
trade) plus 5% market orders. Reproduce with:

```
go test -bench . -benchmem ./engine/
```

The live demo has a **Run benchmark** button that pushes 200,000 orders
through a fresh book on the server it is running on and reports the measured
orders/sec.

## Design

- **Integer ticks and lots.** No floating point anywhere in matching.
- **Price levels:** sorted slice per side (bids descending, asks ascending),
  best level always at index 0, plus a price -> level map. Levels are few
  compared with orders, so a contiguous slice beats a tree on cache behaviour.
- **Queue per level:** intrusive doubly linked list, FIFO for time priority,
  O(1) removal on cancel via an order-ID map.
- **Concurrency:** one goroutine owns each book and drains a command channel.
  No locks on the hot path; symbols match in parallel; matching is
  deterministic per symbol.
- **Trades execute at the maker's price.** Market order remainders are dropped
  (IOC semantics).
- **Fan-out never blocks matching:** subscribers get a buffered channel; a slow
  client misses events and resyncs from the depth snapshot in the next one.
  The SSE handler coalesces bursts to ~10 frames/sec per client.

## Tests

`go test -race ./...` covers price priority, time priority, maker-price
execution, market orders, cancel, rejects, and a 50,000-order randomised run
that checks lot conservation and that the book is never left crossed.

## API

| Method | Path | |
|---|---|---|
| POST | `/api/orders` | `{"symbol","side":"buy|sell","type":"limit|market","price":<cents>,"qty"}` |
| DELETE | `/api/orders/{symbol}/{id}` | cancel |
| GET | `/api/depth/{symbol}` | top 15 levels |
| GET | `/api/stream/{symbol}` | SSE: depth + trades |
| POST | `/api/benchmark?n=200000` | in-process throughput run |

## Run

```
go run ./cmd/server      # http://localhost:8080
docker build -t me . && docker run -p 8080:8080 me
```
