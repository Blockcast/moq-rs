#!/usr/bin/env bash
# SPDX-License-Identifier: MIT OR Apache-2.0

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
TMP=$(mktemp -d)
RELAY_PID=
PUB_PID=
SUB_PID=

cleanup() {
  kill "$RELAY_PID" "$PUB_PID" "$SUB_PID" 2>/dev/null || true
  rm -f "$TMP"/*
  rmdir "$TMP"
}
trap cleanup EXIT

fail() {
  printf '%s\n' "$1" >&2
  for log in relay publisher subscriber; do
    printf '\n--- %s.log ---\n' "$log" >&2
    test ! -f "$TMP/$log.log" || command cat "$TMP/$log.log" >&2
  done
  exit 1
}

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$TMP/key.pem" -out "$TMP/cert.pem" -days 1 \
  -subj '/CN=localhost' -addext 'subjectAltName=DNS:localhost' \
  >/dev/null 2>&1
dd if=/dev/zero of="$TMP/input.bin" bs=1228 count=1 status=none

"$ROOT/target/debug/moq-relay-ietf" \
  --bind 127.0.0.1:4443 \
  --tls-cert "$TMP/cert.pem" \
  --tls-key "$TMP/key.pem" \
  --coordinator-file "$TMP/coordinator.json" \
  >"$TMP/relay.log" 2>&1 &
RELAY_PID=$!
sleep 1

"$ROOT/target/debug/moq-pub-mmtp" moqt://localhost:4443 \
  --name solana-shreds \
  --catalog-json "$ROOT/moq-pub-mmtp/tests/assets/shred-datagram.json" \
  --mmtp-input udp \
  --mmtp-udp-bind 127.0.0.1:5004 \
  --bind 127.0.0.1:0 \
  --tls-disable-verify \
  >"$TMP/publisher.log" 2>&1 &
PUB_PID=$!
sleep 1

"$ROOT/target/debug/moq-sub-raw" moqt://localhost:4443 \
  --name solana-shreds \
  --track shreds \
  --output "$TMP/output.bin" \
  --bind 127.0.0.1:0 \
  --tls-disable-verify \
  >"$TMP/subscriber.log" 2>&1 &
SUB_PID=$!
sleep 2

# One dd output block is one UDP datagram. Smaller default blocks would create
# multiple datagrams and test latest-wins loss rather than the 1,228-byte MTU.
dd if="$TMP/input.bin" bs=1228 count=1 status=none > /dev/udp/127.0.0.1/5004

for _ in $(seq 1 50); do
  if test -f "$TMP/output.bin" && test "$(stat -c %s "$TMP/output.bin")" -ge 1228; then
    break
  fi
  kill -0 "$RELAY_PID" "$PUB_PID" "$SUB_PID" 2>/dev/null \
    || fail "a pipeline process exited before delivery"
  sleep 0.1
done

cmp "$TMP/input.bin" "$TMP/output.bin" \
  || fail "subscriber output did not equal the 1,228-byte input"
if grep -q 'datagram exceeds QUIC limit' "$TMP/publisher.log" "$TMP/relay.log"; then
  fail "pipeline emitted datagram exceeds QUIC limit"
fi

printf 'shred datagram E2E passed: input=1228 bytes output=1228 bytes\n'
