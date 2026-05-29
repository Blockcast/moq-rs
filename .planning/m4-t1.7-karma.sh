#!/usr/bin/env bash
# M.4 Track 1 / T1.7 stage 3 — Karma vehicle for the live Shaka MMTP observe E2E.
#
# Same live pipe as m4-t1.7-e2e.sh, but the Shaka side runs as a Karma
# integration spec (test/msf/mmtp_live_e2e_integration.js) instead of a
# standalone page. This wrapper starts the relay + publisher + control server
# OUTSIDE Karma, then invokes the shaka-player test runner filtered to that one
# spec. The spec fetches /__fingerprint + /__replay from the control server
# (CORS-enabled) and asserts the Mapping-B observe contract in jasmine.
#
# Env knobs: SHAKA_ROOT, CAPTURE, PORT (relay), CTRL_PORT, UDP_PORT.
set -euo pipefail

cd "$(dirname "$0")/.."
MOQ_ROOT="$(pwd)"
SHAKA_ROOT="${SHAKA_ROOT:-$MOQ_ROOT/../shaka-player}"
CAPTURE="${CAPTURE:-$MOQ_ROOT/.planning/m4-t1.7-e2e/moq_mmt_capture_full.json}"
PORT="${PORT:-4443}"
CTRL_PORT="${CTRL_PORT:-8097}"
UDP_PORT="${UDP_PORT:-5004}"
NAME=smoke
URL="https://localhost:$PORT"
R="$MOQ_ROOT/target/release"

WORK=/tmp/m4-t1.7-karma
rm -rf "$WORK"; mkdir -p "$WORK"
FP_FILE="$WORK/fingerprint.hex"

[[ -d "$SHAKA_ROOT" ]] || { echo "SHAKA_ROOT not found: $SHAKA_ROOT"; exit 1; }
[[ -s "$CAPTURE" ]]    || { echo "capture not found: $CAPTURE"; exit 1; }

echo "[1/6] Build binaries..."
cargo build --release -p moq-pub-mmtp -p moq-relay-ietf 2>&1 | tail -2

echo "[2/6] Cert + local fingerprint..."
./dev/cert >/dev/null 2>&1 || true
openssl x509 -in dev/localhost.crt -outform DER | sha256sum | awk '{print $1}' > "$FP_FILE"

echo "[3/6] Catalog..."
cat > "$WORK/catalog.json" <<EOF
{
  "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2", "supportsDeltaUpdates": true,
  "commonTrackFields": {"namespace": "$NAME"},
  "tracks": [{"name": "v", "container": "mmtp", "selectionParams": {"codec": "avc1.synth"}}],
  "multicast": {
    "subgroupHistoryGroups": 8,
    "endpoints": [{"groupAddress": "239.1.1.1", "port": 5000, "tracks": [{"name": "v", "packetId": 1}]}]
  }
}
EOF

PIDS=()
cleanup() {
  local code=$?
  for p in "${PIDS[@]:-}"; do [[ -n "$p" ]] && kill -TERM "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -f /tmp/moq-coordinator.json 2>/dev/null || true
  exit $code
}
trap cleanup EXIT INT TERM
rm -f /tmp/moq-coordinator.json

echo "[4/6] Start relay + publisher + control server..."
"$R/moq-relay-ietf" --bind "[::]:$PORT" \
  --tls-cert dev/localhost.crt --tls-key dev/localhost.key --dev \
  > "$WORK/relay.log" 2>&1 &
PIDS+=($!); sleep 2
"$R/moq-pub-mmtp" --mmtp-input udp --mmtp-udp-bind "127.0.0.1:$UDP_PORT" \
  --catalog-json "$WORK/catalog.json" --name "$NAME" --tls-disable-verify "$URL" \
  > "$WORK/pub.log" 2>&1 &
PIDS+=($!); sleep 3
E2E_REPO_ROOT="$SHAKA_ROOT" E2E_CAPTURE="$CAPTURE" E2E_RESULT="$WORK/unused.json" \
  E2E_FINGERPRINT="$FP_FILE" E2E_PUB_HOST=127.0.0.1 E2E_PUB_PORT="$UDP_PORT" \
  E2E_PORT="$CTRL_PORT" \
  python3 "$MOQ_ROOT/.planning/m4-t1.7-e2e/serve.py" > "$WORK/serve.log" 2>&1 &
PIDS+=($!); sleep 1

echo "[5/6] Run Karma spec (shaka-player; integration tests, filtered)..."
# No --quick: integration specs must load. --filter selects only the live spec.
( cd "$SHAKA_ROOT" && timeout 300 python3 build/test.py --no-build \
    --browsers ChromeHeadless --filter 'MMTP live E2E' ) 2>&1 | tee "$WORK/karma.log" | tail -25
RC=${PIPESTATUS[0]}

echo "[6/6] Karma exit: $RC"
trap - EXIT INT TERM
cleanup_rc=0
for p in "${PIDS[@]:-}"; do [[ -n "$p" ]] && kill -TERM "$p" 2>/dev/null || true; done
wait 2>/dev/null || true
rm -f /tmp/moq-coordinator.json 2>/dev/null || true
exit "$RC"
