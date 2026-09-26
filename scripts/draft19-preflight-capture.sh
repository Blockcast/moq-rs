#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blockcast Inc.
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Phase 3 draft-19 preflight capture for one moq-rs server role (BLO-35570).
#
# Two independent pieces of evidence end up in one artifact:
#
#   1. Negotiation, attested by the SHIPPED BINARIES. A real `moq-relay-ietf`
#      and a real `moq-pub-mmtp`, both `--wire-profile draft19`, complete a
#      draft-19 SETUP over real QUIC. The role's own log line -- the one
#      carrying `selected_version=moqt-19` -- is lifted verbatim into the
#      artifact. This is what makes the row about the binaries and not about a
#      library call.
#
#   2. GOAWAY, captured at the session layer by `draft19-preflight`. No shipped
#      binary emits a draft-19 GOAWAY today, so this half is explicitly
#      caveated in the artifact rather than dressed up as a production drain.
#
# Usage: scripts/draft19-preflight-capture.sh <publisher|relay-root|relay-leaf> <output.json>
set -euo pipefail

ROLE="${1:?usage: $0 <publisher|relay-root|relay-leaf> <output.json>}"
OUTPUT="${2:?usage: $0 <publisher|relay-root|relay-leaf> <output.json>}"

case "$ROLE" in
  publisher|relay-root|relay-leaf) ;;
  *) echo "unknown role: $ROLE" >&2; exit 2 ;;
esac

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD)"
BIN_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}/debug"
WORK="$(mktemp -d)"
trap 'kill "${RELAY_PID:-}" 2>/dev/null || true; rm -rf "$WORK"' EXIT

cargo build -p moq-relay-ietf -p moq-pub-mmtp \
  --bin moq-relay-ietf --bin moq-pub-mmtp --bin draft19-preflight

# 127.0.0.1 throughout: `localhost` can resolve to ::1, and the relay binds a
# single-stack v4 socket, so the publisher would then never reach it.
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$WORK/key.pem" -out "$WORK/cert.pem" \
  -subj "/CN=localhost" -addext "subjectAltName=IP:127.0.0.1,DNS:localhost" 2>/dev/null

# Any valid MMTP catalog will do: the publisher parses it before connecting and
# then exits on the draft-19 branch without ever announcing a track.
cp "$REPO_ROOT/moq-catalog/tests/fixtures/positive/flat-av.json" "$WORK/catalog.json"

PORT="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
RELAY_URL="https://127.0.0.1:$PORT"

# Root advertises itself as an origin to the coordinator; leaf only consumes
# the coordinator. Both run the identical draft-19 accept path, because
# `serve_draft19_control_plane` runs before any scope or coordinator lookup --
# the artifact says so rather than implying two different negotiations.
RELAY_ARGS=(--bind "127.0.0.1:$PORT" --wire-profile draft19
            --tls-cert "$WORK/cert.pem" --tls-key "$WORK/key.pem" --tls-disable-verify
            --coordinator-file "$WORK/coordinator-$ROLE.json")
if [ "$ROLE" = "relay-root" ]; then
  RELAY_ARGS+=(--node "$RELAY_URL")
fi

RUST_LOG=info "$BIN_DIR/moq-relay-ietf" "${RELAY_ARGS[@]}" >"$WORK/relay.log" 2>&1 &
RELAY_PID=$!

for _ in $(seq 1 60); do
  grep -q "listening on 127.0.0.1:$PORT" "$WORK/relay.log" && break
  kill -0 "$RELAY_PID" 2>/dev/null || { cat "$WORK/relay.log" >&2; echo "relay exited early" >&2; exit 1; }
  sleep 0.5
done
grep -q "listening on 127.0.0.1:$PORT" "$WORK/relay.log" || { cat "$WORK/relay.log" >&2; echo "relay never listened" >&2; exit 1; }

PUB_ARGS=("$RELAY_URL" --name draft19-preflight --catalog-json "$WORK/catalog.json"
          --wire-profile draft19 --tls-disable-verify)
# Expected to exit non-zero: the publisher reports the negotiated profile and
# then refuses to put draft-16 MMTP bytes on a moqt-19 connection.
RUST_LOG=info "$BIN_DIR/moq-pub-mmtp" "${PUB_ARGS[@]}" >"$WORK/pub.log" 2>&1 || true

grep -q "negotiated moqt-19, but MMTP publish is draft-16 only" "$WORK/pub.log" || {
  cat "$WORK/pub.log" >&2; echo "publisher did not report a moqt-19 negotiation" >&2; exit 1
}

# Strip ANSI styling so the quoted evidence is the log line, not its colouring.
strip_ansi() { sed -r 's/\x1b\[[0-9;]*m//g'; }
if [ "$ROLE" = "publisher" ]; then
  EVIDENCE="$(strip_ansi <"$WORK/pub.log" | grep -F 'draft-19 session established; MMTP publish over draft-19 is not implemented' | tail -1)"
  INVOCATION="moq-pub-mmtp ${PUB_ARGS[*]}"
else
  EVIDENCE="$(strip_ansi <"$WORK/relay.log" | grep -F 'draft-19 session established: control plane only' | tail -1)"
  INVOCATION="moq-relay-ietf ${RELAY_ARGS[*]}"
fi
[ -n "$EVIDENCE" ] || { echo "no draft-19 establishment line for role $ROLE" >&2; exit 1; }

# Fail closed rather than attesting a version the binary did not report.
case "$EVIDENCE" in
  *selected_version=moqt-19*) ;;
  *) echo "evidence line does not carry selected_version=moqt-19: $EVIDENCE" >&2; exit 1 ;;
esac

kill "$RELAY_PID" 2>/dev/null || true
wait "$RELAY_PID" 2>/dev/null || true
unset RELAY_PID

"$BIN_DIR/draft19-preflight" \
  --role "$ROLE" \
  --source-commit "$SOURCE_COMMIT" \
  --binary-evidence "$EVIDENCE" \
  --binary-invocation "$INVOCATION" \
  --captured-at "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" \
  --output "$WORK/attestation.json" \
  --tls-cert "$WORK/cert.pem" --tls-key "$WORK/key.pem" --tls-disable-verify

# Digest the canonical payload exactly as a consumer recomputes it: compact
# JSON in the emitted key order, one trailing LF. `jq -c` preserves input key
# order, which is what makes this reproducible from the artifact alone.
DIGEST="$(jq -c '.canonical_payload' "$WORK/attestation.json" | sha256sum | cut -d' ' -f1)"
mkdir -p "$(dirname "$OUTPUT")"
jq --arg sha256 "$DIGEST" '. + {sha256: $sha256}' "$WORK/attestation.json" >"$OUTPUT"

echo "captured $ROLE at $SOURCE_COMMIT -> $OUTPUT (canonical sha256 $DIGEST)"
