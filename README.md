# Matching Engine

In-memory limit order book and matching engine in Go. Price-time priority,
limit, market and IOC orders, O(1) cancel, one owner goroutine per symbol,
live order book and trade stream over Server-Sent Events, and demo accounts
that start with $10,000 and trade against a liquidity bot.

**Live demo:** https://matching-engine-ydj6.onrender.com (free tier, the
first load after idle takes ~50 s). Opening the page runs a live benchmark on
the server.

## Benchmarks (Apple M4, 10 cores, Go 1.25.7)

![go test -bench output](docs/benchmark.png)

| Path | Median of 5 runs | Range | Throughput |
|---|---|---|---|
| Raw book `Submit`, one thread, 0 allocs/op | 136 ns/order | 132-213 ns | **~7.4M orders/sec** |
| Full engine path: concurrent clients -> channel -> book goroutine -> reply | 983 ns/order | 829-1155 ns | **~1.0M orders/sec** |

Order flow: random limits clustered +/-50 ticks around mid (most cross and
trade) plus 5% market orders. Reproduce with:

```
go test -run '^$' -bench . -benchmem -count=5 ./engine/
```

**Live benchmark on the demo:** on page load the server pushes 500,000 random
orders through a fresh book in 10 batches of 50,000, times each batch on its
own and streams the results to the page. The headline is the median batch.
The free host is a shared fraction of one core, so its number is lower than
the laptop's.

**Scaling:** one market is single-threaded by design, because matching order
N+1 depends on order N. More cores do not make one book faster; they let more
markets run side by side, so total throughput grows with cores.

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

## Demo accounts

Pick a username or continue as guest and you get $10,000 of demo USD. Lot
sizes are 0.001 BTC, 0.01 ETH and 0.1 SOL so small accounts can trade.

- **Before the book (risk #1):** `Reserve` holds the cash or coins an order
  could use and registers it, so a fill can never arrive for an unknown order.
- **After matching (risk #2):** the engine calls a `TradeHook` on the symbol's
  goroutine for every fill, maker and taker, before replying. Unlike the
  browser broadcast it never drops events, so it is where balances settle.
- Buys are held at the limit price and refunded the difference when they fill
  at the maker's better price. Market orders become IOC limits 5% through the
  touch, so the hold is bounded.
- Cash is integer milli-cents, so price x lots is exact in every market.
- Accounts live in memory and reset on restart. The benchmark uses a bare
  book, so the account layer is not in the measured path.

## Tests

`go test -race ./...` covers price priority, time priority, maker-price
execution, market and IOC orders, cancel, rejects, a 50,000-order randomised
run that checks lot conservation and that the book is never left crossed,
that the trade hook sees every fill before the reply, and account tests:
maker-price refunds, no overspending or overselling, IOC and cancel releasing
holds, and 5,000 random orders across 20 accounts with total cash and coins
unchanged.

## API

| Method | Path | |
|---|---|---|
| POST | `/api/session` | `{"name"}` -> `{"token","account"}`; send the token as `X-Session` |
| GET | `/api/account` | balances, positions, open orders, fills, P/L |
| POST | `/api/orders` | `{"symbol","side":"buy|sell","type":"limit|market","price":<cents per coin>,"qty":<lots>}` |
| DELETE | `/api/orders/{id}` | cancel one of your orders |
| GET | `/api/depth/{symbol}` | top 15 levels |
| GET | `/api/stream/{symbol}` | SSE: depth + trades |
| GET | `/api/markets` | symbols and lot sizes |
| GET | `/api/benchmark` | live run, streamed as NDJSON (one line per batch, then a summary) |

## Run

```
go run ./cmd/server      # http://localhost:8080
docker build -t me . && docker run -p 8080:8080 me
```
