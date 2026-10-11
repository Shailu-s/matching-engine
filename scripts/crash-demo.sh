#!/usr/bin/env bash
# Kill -9 the exchange in the middle of live traffic, restart it, and prove
# the recovered state is exact.
set -euo pipefail
cd "$(dirname "$0")/.."
BIN=$(mktemp -d)
DATA=$(mktemp -d)
trap 'pkill -f "$BIN/exchange" 2>/dev/null || true; rm -rf "$BIN" "$DATA"' EXIT
go build -o "$BIN/exchange" ./cmd/exchange
go build -o "$BIN/loadgen" ./cmd/loadgen

wait_up() { for _ in $(seq 1 200); do curl -sf -o /dev/null 127.0.0.1:9101/state && return; sleep 0.05; done; echo "exchange did not start"; exit 1; }

echo "== start exchange (fsync on, snapshot every 1M commands)"
"$BIN/exchange" -dir "$DATA" -snapshot-every 1000000 2>&1 | sed 's/^/   [exchange] /' &
wait_up

echo "== send load from a separate process, then kill -9 the exchange mid-stream"
"$BIN/loadgen" -duration 30s -warmup 0s >/dev/null 2>&1 &
sleep 4
SEQ=$(curl -s 127.0.0.1:9101/state | sed -n 's/.*"seq": \([0-9]*\).*/\1/p')
pkill -9 -f "$BIN/exchange"
echo "   killed with SIGKILL at about seq $SEQ: no shutdown hook, no final snapshot"
pkill -f "$BIN/loadgen" 2>/dev/null || true
sleep 0.5

echo "== offline check on the crashed data: newest snapshot + journal tail vs the whole journal from zero"
"$BIN/exchange" -dir "$DATA" -verify

echo "== restart: newest snapshot + journal tail"
"$BIN/exchange" -dir "$DATA" -snapshot-every 1000000 2>&1 | sed 's/^/   [exchange] /' &
wait_up
curl -s 127.0.0.1:9101/state | grep -E '"(seq|state_hash|resting_orders|snapshot_seq|replayed_from_journal|torn_tail_bytes_truncated|took_ms)"'
pkill -f "$BIN/exchange"; sleep 1

