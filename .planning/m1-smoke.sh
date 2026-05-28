#!/usr/bin/env bash
# M.1 end-to-end smoke test (BLO-4020 / ADR T9).
#
# Pipeline:
#   synth_mmtp (deterministic MPU sequences for 2 tracks)
#     | moq-pub-mmtp --mmtp-input stdin
#     → moq-relay-ietf --dev --mlog-dir
#     → moq-sub-raw (writes per-track files)
#     → sha256 compared per-track against synth's expected output
#
# Env knobs:
#   SMOKE=/tmp/m1-smoke   working dir for outputs
#   NAME=smoke            broadcast namespace
#   GROUPS=8              MPUs per track
#   PORT=4443             relay port
#   PACKET_DELAY_MS=50    pacing so SubgroupsReader's latest-only
#                         semantics don't lose intermediate MPUs
#
# Exits non-zero if any per-track sha256 mismatches.

set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"

SMOKE="${SMOKE:-/tmp/m1-smoke}"
NAME="${NAME:-smoke}"
GROUPS="${GROUPS:-8}"
PORT="${PORT:-4443}"
PACKET_DELAY_MS="${PACKET_DELAY_MS:-50}"
URL="https://localhost:$PORT"

rm -rf "$SMOKE"
mkdir -p "$SMOKE/mlog"

echo "[1/7] Building binaries..."
cargo build --release \
    -p moq-pub-mmtp -p moq-sub-raw -p moq-relay-ietf 2>&1 | tail -3
cargo build --release --example synth_mmtp -p moq-pub-mmtp 2>&1 | tail -2

echo "[2/7] Writing catalog..."
cat > "$SMOKE/catalog.json" <<EOF
{
  "version": 1,
  "streamingFormat": 1,
  "streamingFormatVersion": "0.2",
  "supportsDeltaUpdates": true,
  "commonTrackFields": {"namespace": "$NAME"},
  "tracks": [
    {"name": "v", "container": "mmtp", "selectionParams": {"codec": "hev1.synth"}},
    {"name": "a", "container": "mmtp", "selectionParams": {"codec": "mp4a.synth"}}
  ],
  "multicast": {
    "endpoints": [{
      "groupAddress": "239.255.1.1",
      "port": 5004,
      "tracks": [
        {"name": "v", "packetId": 1},
        {"name": "a", "packetId": 2}
      ]
    }]
  }
}
EOF

# Ensure TLS certs exist (dev/cert is idempotent).
./dev/cert >/dev/null 2>&1 || true

cleanup() {
    local code=$?
    [[ -n "${SUB_PID-}" ]] && kill -TERM "$SUB_PID" 2>/dev/null || true
    [[ -n "${PUB_PID-}" ]] && kill -TERM "$PUB_PID" 2>/dev/null || true
    [[ -n "${RELAY_PID-}" ]] && kill -TERM "$RELAY_PID" 2>/dev/null || true
    wait 2>/dev/null || true
    rm -f /tmp/moq-coordinator.json 2>/dev/null || true
    exit $code
}
trap cleanup EXIT INT TERM

# moq-rs --dev mode uses /tmp/moq-coordinator.json which can hold
# stale entries from a prior aborted run; start clean.
rm -f /tmp/moq-coordinator.json

echo "[3/7] Starting relay (--dev --mlog-dir $SMOKE/mlog)..."
"$REPO_ROOT/target/release/moq-relay-ietf" \
    --bind "[::]:$PORT" \
    --tls-cert dev/localhost.crt --tls-key dev/localhost.key \
    --dev --mlog-dir "$SMOKE/mlog" \
    > "$SMOKE/relay.log" 2>&1 &
RELAY_PID=$!
sleep 2

UDP_PORT="${UDP_PORT:-5004}"
UDP_TARGET="127.0.0.1:$UDP_PORT"

echo "[4/7] Starting moq-pub-mmtp (UDP listener on $UDP_TARGET)..."
"$REPO_ROOT/target/release/moq-pub-mmtp" \
    --mmtp-input udp --mmtp-udp-bind "$UDP_TARGET" \
    --catalog-json "$SMOKE/catalog.json" \
    --name "$NAME" \
    --tls-disable-verify \
    "$URL" \
    > "$SMOKE/pub.log" 2>&1 &
PUB_PID=$!
# Give pub time to connect + announce the namespace.
sleep 3

echo "[5/7] Starting moq-sub-raw..."
"$REPO_ROOT/target/release/moq-sub-raw" \
    --name "$NAME" \
    --track v --output "$SMOKE/sub-out-1.bin" \
    --track a --output "$SMOKE/sub-out-2.bin" \
    --tls-disable-verify \
    "$URL" \
    > "$SMOKE/sub.log" 2>&1 &
SUB_PID=$!
# Give sub time to subscribe.
sleep 2

echo "[6/7] Sending synth_mmtp UDP packets to $UDP_TARGET ($GROUPS groups, ${PACKET_DELAY_MS}ms per packet)..."
"$REPO_ROOT/target/release/examples/synth_mmtp" \
    --output-dir "$SMOKE" \
    --groups "$GROUPS" \
    --packet-delay-ms "$PACKET_DELAY_MS" \
    --udp "$UDP_TARGET" \
    > "$SMOKE/synth.log" 2>&1

# Give moq-pub-mmtp time to publish the trailing packets, then
# moq-sub-raw time to drain them before we cut the relay.
sleep 3

echo "[7/7] Stopping subscriber + publisher + relay..."
kill -TERM "$SUB_PID" 2>/dev/null || true
wait "$SUB_PID" 2>/dev/null || true
SUB_PID=
kill -TERM "$PUB_PID" 2>/dev/null || true
wait "$PUB_PID" 2>/dev/null || true
PUB_PID=
kill -TERM "$RELAY_PID" 2>/dev/null || true
wait "$RELAY_PID" 2>/dev/null || true
RELAY_PID=

echo
echo "=== Per-track sha256 comparison ==="
EXIT=0
for track in 1 2; do
    expected="$SMOKE/expected-$track.bin"
    actual="$SMOKE/sub-out-$track.bin"
    if [[ ! -s "$actual" ]]; then
        echo "Track $track: FAIL (sub-out file missing/empty: $actual)"
        EXIT=1
        continue
    fi
    EXP_HASH=$(sha256sum "$expected" | awk '{print $1}')
    ACT_HASH=$(sha256sum "$actual"   | awk '{print $1}')
    EXP_BYTES=$(stat -c%s "$expected")
    ACT_BYTES=$(stat -c%s "$actual")
    if [[ "$EXP_HASH" = "$ACT_HASH" ]]; then
        echo "Track $track: MATCH ($EXP_BYTES bytes) $EXP_HASH"
    else
        echo "Track $track: MISMATCH"
        echo "  expected ($EXP_BYTES bytes): $EXP_HASH"
        echo "  got      ($ACT_BYTES bytes): $ACT_HASH"
        EXIT=1
    fi
done

echo
echo "=== mlog files (proves --mlog-dir wrote, NOT qlog) ==="
ls -la "$SMOKE/mlog/" 2>/dev/null || echo "(no mlog dir)"

echo
echo "=== Working dir contents ==="
ls -la "$SMOKE/"

trap - EXIT INT TERM
if [[ $EXIT -ne 0 ]]; then
    echo
    echo "SMOKE FAIL"
    exit 1
fi
echo
echo "SMOKE PASS"
