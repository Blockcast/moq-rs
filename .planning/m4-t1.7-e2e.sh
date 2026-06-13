#!/usr/bin/env bash
# M.4 Track 1 / T1.2 — live single-pipe PLAYBACK E2E (evolves the T1.7 harness):
#   real moq_mmt capture  -> moq-pub-mmtp -> moq-relay-ietf
#                         -> Shaka (headless Chrome, real WebTransport)
#                         -> MSE SourceBuffer -> assert video playback.
#
# The harness page (default demo/play-mmtp.html; override via HARNESS=) loads
# the MMTP stream into an MSE pipeline (hand-rolled or production Player.load)
# and POSTs {status, bufferedSeconds, currentTime} to the control server
# (.planning/m4-t1.7-e2e/serve.py), which replays the capture into the publisher
# on the page's cue — with optional adversarial shaping (start=mid-GOP join,
# drop=frame loss, reorder=out-of-order) selected via the Player.load harness
# query string. PASS = MSE buffered >0s and the video advanced past t=0.
#
# Env knobs:
#   SHAKA_ROOT   shaka-player repo (default ../shaka-player rel to moq-rs)
#   CAPTURE      full-payload capture json (default /tmp/moq_mmt_capture_full.json)
#   PORT         relay quic+web bind port (default 4443)
#   CTRL_PORT    control/static server port (default 8097)
#   UDP_PORT     publisher UDP listener port (default 5004)
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

WORK=/tmp/m4-t1.7-e2e
rm -rf "$WORK"; mkdir -p "$WORK"
RESULT="$WORK/result.json"
FP_FILE="$WORK/fingerprint.hex"

[[ -d "$SHAKA_ROOT" ]] || { echo "SHAKA_ROOT not found: $SHAKA_ROOT"; exit 1; }
[[ -s "$CAPTURE" ]]    || { echo "capture not found: $CAPTURE"; exit 1; }

echo "[1/8] Build binaries..."
cargo build --release -p moq-pub-mmtp -p moq-relay-ietf 2>&1 | tail -2

echo "[2/8] Ensure dev cert + compute fingerprint locally (no TLS connection)..."
./dev/cert >/dev/null 2>&1 || true
openssl x509 -in dev/localhost.crt -outform DER | sha256sum | awk '{print $1}' > "$FP_FILE"
echo "      fingerprint: $(cat "$FP_FILE")"

echo "[3/8] Write catalog (track v, packetId 1)..."
cat > "$WORK/catalog.json" <<EOF
{
  "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2", "supportsDeltaUpdates": true,
  "commonTrackFields": {"namespace": "$NAME"},
  "tracks": [{"name": "v", "packaging": "mmtp", "framerate": 15, "selectionParams": {"codec": "avc1.42c01e"}}],
  "multicast": {
    "subgroupHistoryGroups": 8,
    "endpoints": [{"groupAddress": "239.1.1.1", "port": 5000, "tracks": [{"name": "v", "packetId": 1}]}]
  }
}
EOF

PIDS=()
cleanup() {
  local code=$?
  [[ -n "${CHROME_PID-}" ]] && kill -TERM "$CHROME_PID" 2>/dev/null || true
  for p in "${PIDS[@]:-}"; do [[ -n "$p" ]] && kill -TERM "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -f /tmp/moq-coordinator.json 2>/dev/null || true
  exit $code
}
trap cleanup EXIT INT TERM
rm -f /tmp/moq-coordinator.json

echo "[4/8] Start relay (--dev) on $URL ..."
RUST_LOG="${RUST_LOG:-info,moq_relay_ietf=debug,moq_transport=debug,quinn=warn}" \
"$R/moq-relay-ietf" --bind "[::]:$PORT" \
  --tls-cert dev/localhost.crt --tls-key dev/localhost.key --dev \
  > "$WORK/relay.log" 2>&1 &
PIDS+=($!); sleep 2

echo "[5/8] Start publisher (UDP 127.0.0.1:$UDP_PORT) ..."
"$R/moq-pub-mmtp" --mmtp-input udp --mmtp-udp-bind "127.0.0.1:$UDP_PORT" \
  --catalog-json "$WORK/catalog.json" --name "$NAME" --tls-disable-verify "$URL" \
  > "$WORK/pub.log" 2>&1 &
PIDS+=($!); sleep 3

echo "[6/8] Start control+static server (root=$SHAKA_ROOT, port=$CTRL_PORT) ..."
E2E_REPO_ROOT="$SHAKA_ROOT" E2E_CAPTURE="$CAPTURE" E2E_RESULT="$RESULT" \
  E2E_FINGERPRINT="$FP_FILE" E2E_PUB_HOST=127.0.0.1 E2E_PUB_PORT="$UDP_PORT" \
  E2E_PORT="$CTRL_PORT" \
  python3 "$MOQ_ROOT/.planning/m4-t1.7-e2e/serve.py" > "$WORK/serve.log" 2>&1 &
PIDS+=($!); sleep 1

echo "[7/8] Launch headless Chrome at the playback harness page ..."
HARNESS="${HARNESS:-demo/play-mmtp.html}"
echo "      harness: $HARNESS"
google-chrome --headless=new --no-sandbox --disable-gpu --no-first-run \
  --user-data-dir="$WORK/chrome" \
  --autoplay-policy=no-user-gesture-required \
  --enable-logging=stderr \
  "http://127.0.0.1:$CTRL_PORT/$HARNESS" \
  > "$WORK/chrome.log" 2>&1 &
CHROME_PID=$!

echo "      waiting for result (up to 45s) ..."
for i in $(seq 1 90); do
  [[ -s "$RESULT" ]] && break
  sleep 0.5
done

kill -TERM "$CHROME_PID" 2>/dev/null || true; CHROME_PID=

echo "[8/8] Result:"
if [[ ! -s "$RESULT" ]]; then
  echo "  NO RESULT — harness did not POST. Tails:"
  echo "  --- serve.log ---"; tail -6 "$WORK/serve.log" | sed 's/^/    /'
  echo "  --- pub.log ---";   tail -6 "$WORK/pub.log"   | sed 's/^/    /'
  echo "  --- relay.log ---"; tail -4 "$WORK/relay.log" | sed 's/^/    /'
  echo "  --- chrome.log (tail) ---"; tail -10 "$WORK/chrome.log" | sed 's/^/    /'
  exit 1
fi
python3 - "$RESULT" <<'PY'
# Assert the T1.2 playback contract end-to-end.
#   PASS requires:
#     - harness status == "done", no error
#     - bufferedSeconds > 0  (MSE SourceBuffer received fMP4 data)
#     - currentTime > 0      (video element advanced past t=0)
import json,sys
r=json.load(open(sys.argv[1]))
status=r.get("status")
err=r.get("error")
buf=float(r.get("bufferedSeconds",0))
ct=float(r.get("currentTime",0))
print("  status        :", status)
print("  error         :", (err or "")[:300])
print("  bufferedSeconds:", round(buf,3))
print("  currentTime   :", round(ct,3))

fails=[]
if status!="done": fails.append(f"status={status!r} (expected 'done')")
if err: fails.append("harness error: " + (err or "")[:200])
if buf<=0: fails.append(f"bufferedSeconds={buf} (must be >0; MSE did not buffer any data)")
if ct<=0: fails.append(f"currentTime={ct} (must be >0; video did not advance)")

print()
if fails:
    print("  E2E ASSERT: FAIL")
    for f in fails: print("    -", f)
    sys.exit(1)
print(f"  E2E ASSERT: PASS — MMTP playback verified: MSE buffered={buf:.3f}s, currentTime={ct:.3f}s advanced (harness via HARNESS=: hand-rolled MSE pipe or production Player.load)")
PY
ASSERT_RC=$?

if [[ $ASSERT_RC -ne 0 ]]; then
  echo "  --- chrome.log (last 30) ---"; tail -30 "$WORK/chrome.log" | sed 's/^/    /'
  echo "  --- serve.log (last 10) ---"; tail -10 "$WORK/serve.log" | sed 's/^/    /'
  echo "  --- relay.log (tail) ---"; tail -6 "$WORK/relay.log" | sed 's/^/    /'
  exit 1
fi
