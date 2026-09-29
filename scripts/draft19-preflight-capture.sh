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
#   2. GOAWAY, also attested by the SHIPPED RELAY BINARY. The relay is launched
#      with a drain configured, a `draft19-preflight` client establishes a
#      draft-19 session against it, and the relay is then signalled. The GOAWAY
#      the client decodes is the one `serve_draft19_control_plane` emitted, and
#      the relay's own log line for that send is lifted into the artifact as
#      `goaway.binary_evidence`. No session-level stand-in is involved.
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
# The artifact attests source_commit, so a dirty tree would emit a commit that
# did not build these binaries. `--porcelain` so an untracked source file counts
# too. The workflow checks out clean, so this only catches a local run.
[ -z "$(git -C "$REPO_ROOT" status --porcelain)" ] || {
  echo "working tree is dirty; $SOURCE_COMMIT would not describe these binaries" >&2; exit 1
}
BIN_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}/debug"
WORK="$(mktemp -d)"
# SIGKILL, not the default SIGTERM: with the drain armed below, SIGTERM starts a
# graceful drain rather than stopping the relay, so an error path would leave it
# running. The happy path unsets these before the trap can see them.
trap 'kill -KILL "${PREFLIGHT_PID:-}" 2>/dev/null || true; kill -KILL "${RELAY_PID:-}" 2>/dev/null || true; rm -rf "$WORK"' EXIT

# Must match `GOAWAY_URI` and `GOAWAY_TIMEOUT_MS` in
# moq-relay-ietf/src/bin/draft19-preflight/main.rs, which fails closed if the
# GOAWAY the relay sends carries anything else.
GOAWAY_URI="moqt://next.example"
GOAWAY_TIMEOUT_MS=250

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
# the coordinator. Both runs are the same observation: `serve_draft19_control_plane`
# runs before any scope or coordinator lookup, `RelayConfig.node` is written and
# never read, and the `--node` value below recomputes the byte-identical
# `relay_url` the `None` fallback already produces. The artifact records that in
# the digest-covered `canonical_payload.path_equivalent_to`.
RELAY_ARGS=(--bind "127.0.0.1:$PORT" --wire-profile draft19
            --tls-cert "$WORK/cert.pem" --tls-key "$WORK/key.pem" --tls-disable-verify
            --coordinator-file "$WORK/coordinator-$ROLE.json"
            --draft19-goaway-uri "$GOAWAY_URI"
            --draft19-goaway-timeout-ms "$GOAWAY_TIMEOUT_MS")
if [ "$ROLE" = "relay-root" ]; then
  RELAY_ARGS+=(--node "$RELAY_URL")
fi

# Strip ANSI styling so every log check reads the line, not its colouring.
# `grep -q` would exit early and SIGPIPE the sed, which `set -o pipefail` would
# then report as a failed check, so these greps discard stdout instead.
strip_ansi() { sed -r 's/\x1b\[[0-9;]*m//g'; }

RUST_LOG=info "$BIN_DIR/moq-relay-ietf" "${RELAY_ARGS[@]}" >"$WORK/relay.log" 2>&1 &
RELAY_PID=$!

relay_listening() { strip_ansi <"$WORK/relay.log" | grep -F "listening on 127.0.0.1:$PORT" >/dev/null; }
# A dead relay is the likelier explanation for any line that never arrives, so
# all three waits rule it out before reporting the line's own absence.
relay_alive_or_die() { kill -0 "$RELAY_PID" 2>/dev/null || { cat "$WORK/relay.log" >&2; echo "relay exited early" >&2; exit 1; }; }

# `wait` has no timeout, and every wait here is on a peer that can wedge. Left
# unbounded they terminate only when a QUIC idle timeout eventually trips, which
# on CI means spending the job timeout and losing the log dump with it. Reap
# under a watchdog instead: an overrun becomes a normal non-zero wait, so the
# caller's failure branch still runs and still dumps `relay.log`.
WAIT_BUDGET_SECS=30
wait_bounded() {
  local pid=$1 rc=0 watchdog
  ( sleep "$WAIT_BUDGET_SECS"; kill -KILL "$pid" 2>/dev/null ) &
  watchdog=$!
  wait "$pid" || rc=$?
  kill "$watchdog" 2>/dev/null || true
  wait "$watchdog" 2>/dev/null || true
  return "$rc"
}

for _ in $(seq 1 60); do
  relay_listening && break
  relay_alive_or_die
  sleep 0.5
done
relay_listening || { cat "$WORK/relay.log" >&2; echo "relay never listened" >&2; exit 1; }

PUB_ARGS=("$RELAY_URL" --name draft19-preflight --catalog-json "$WORK/catalog.json"
          --wire-profile draft19 --tls-disable-verify)

if [ "$ROLE" = "publisher" ]; then
  EVIDENCE_LOG="$WORK/pub.log"
  MARKER='draft-19 session established; MMTP publish over draft-19 is not implemented'
  INVOCATION="moq-pub-mmtp ${PUB_ARGS[*]}"
else
  EVIDENCE_LOG="$WORK/relay.log"
  MARKER='draft-19 session established: control plane only'
  INVOCATION="moq-relay-ietf ${RELAY_ARGS[*]}"
fi
# `|| true` is load-bearing: under `set -o pipefail` a grep that matches nothing
# makes the whole pipeline exit 1, which `set -e` turns into a bare exit before
# the named diagnostic below can run.
evidence() { strip_ansi <"$EVIDENCE_LOG" | grep -F "$MARKER" | tail -1; }

# Each side returns from `establish` independently, and the publisher logs its
# negotiation and then immediately exits. That tears the QUIC connection down
# under the relay, which can still be inside `Draft19Session::establish`: it
# then logs `failed to read capsule` / `connection error: closed` and never
# writes an establishment line at all. Polling cannot recover that connection,
# so reconnect. Each attempt is a fresh connection to the same still-running
# relay, and the inner poll covers the narrower case where the relay's line is
# merely late. Killing the relay to flush it would not help either: SIGTERM can
# land before the relay task reaches the log call, losing the line.
EVIDENCE=""
ATTEMPTS=8
for _ in $(seq 1 "$ATTEMPTS"); do
  # Expected to exit non-zero: the publisher reports the negotiated profile and
  # then refuses to put draft-16 MMTP bytes on a moqt-19 connection.
  RUST_LOG=info "$BIN_DIR/moq-pub-mmtp" "${PUB_ARGS[@]}" >"$WORK/pub.log" 2>&1 || true
  strip_ansi <"$WORK/pub.log" | grep -F "negotiated moqt-19, but MMTP publish is draft-16 only" >/dev/null || {
    # The relay is only exercised by this connection, so a relay that dies does
    # so during the run above and takes the publisher's negotiation down with
    # it. Rule that out before blaming the publisher, or a dead relay reports
    # as a negotiation regression with only `pub.log` to read.
    relay_alive_or_die
    cat "$WORK/pub.log" >&2; echo "publisher did not report a moqt-19 negotiation" >&2; exit 1
  }
  for _ in $(seq 1 10); do
    EVIDENCE="$(evidence)" || true
    if [ -n "$EVIDENCE" ]; then break; fi
    sleep 0.5
  done
  if [ -n "$EVIDENCE" ]; then break; fi
  relay_alive_or_die
done
[ -n "$EVIDENCE" ] || {
  cat "$EVIDENCE_LOG" >&2
  echo "no draft-19 establishment line for role $ROLE after $ATTEMPTS attempts" >&2; exit 1
}

# Fail closed rather than attesting a version the binary did not report.
case "$EVIDENCE" in
  *selected_version=moqt-19*) ;;
  *) echo "evidence line does not carry selected_version=moqt-19: $EVIDENCE" >&2; exit 1 ;;
esac

# The GOAWAY half. The preflight client must be established -- and so admitted
# to the relay's drain -- before the signal: a drain that starts with no live
# draft-19 session has nothing to redirect and the relay exits immediately.
READY="$WORK/preflight.ready"
"$BIN_DIR/draft19-preflight" \
  --role "$ROLE" \
  --source-commit "$SOURCE_COMMIT" \
  --binary-evidence "$EVIDENCE" \
  --binary-invocation "$INVOCATION" \
  --captured-at "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" \
  --output "$WORK/attestation.json" \
  --relay-url "$RELAY_URL" \
  --ready-file "$READY" \
  --tls-cert "$WORK/cert.pem" --tls-key "$WORK/key.pem" --tls-disable-verify &
PREFLIGHT_PID=$!

for _ in $(seq 1 60); do
  [ -f "$READY" ] && break
  kill -0 "$PREFLIGHT_PID" 2>/dev/null || break
  relay_alive_or_die
  sleep 0.5
done
[ -f "$READY" ] || {
  wait_bounded "$PREFLIGHT_PID" 2>/dev/null || true
  cat "$WORK/relay.log" >&2
  echo "preflight never established a draft-19 session against the relay" >&2; exit 1
}

# Arm the drain. SIGTERM is what the relay turns into a GOAWAY broadcast. A
# failure here means the relay died between the ready-file check and now, so it
# gets the same log dump every other failure path in this file gets.
kill -TERM "$RELAY_PID" || {
  cat "$WORK/relay.log" >&2
  echo "relay was gone before the drain could be armed" >&2; exit 1
}
wait_bounded "$PREFLIGHT_PID" || {
  cat "$WORK/relay.log" >&2
  echo "preflight did not capture a GOAWAY from the relay" >&2; exit 1
}
unset PREFLIGHT_PID
# The relay stops itself once the drain is over, so this reaps it rather than
# killing it: an exit here is the drain completing, not a signal landing.
wait_bounded "$RELAY_PID" 2>/dev/null || true
unset RELAY_PID

# Quote the relay's own line for the send, the same way the SETUP half is
# quoted. Validated before it is attested: a line that names a different URI or
# Timeout would describe a different GOAWAY than the one captured above.
GOAWAY_EVIDENCE="$(strip_ansi <"$WORK/relay.log" | grep -F 'draft-19 drain: sent GOAWAY' | tail -1)" || true
[ -n "$GOAWAY_EVIDENCE" ] || {
  cat "$WORK/relay.log" >&2
  echo "relay logged no draft-19 GOAWAY send" >&2; exit 1
}
case "$GOAWAY_EVIDENCE" in
  *"new_session_uri=$GOAWAY_URI"*"timeout_ms=$GOAWAY_TIMEOUT_MS"*) ;;
  *) echo "relay GOAWAY line does not carry the configured URI and Timeout: $GOAWAY_EVIDENCE" >&2; exit 1 ;;
esac

# Digest the canonical payload exactly as a consumer recomputes it: compact
# JSON in the emitted key order, one trailing LF. `jq -c` preserves input key
# order, which is what makes this reproducible from the artifact alone.
DIGEST="$(jq -c '.canonical_payload' "$WORK/attestation.json" | sha256sum | cut -d' ' -f1)"
mkdir -p "$(dirname "$OUTPUT")"
# Both additions are provenance the capture observes after `draft19-preflight`
# exits, so both sit outside the digest, which is computed above.
jq --arg sha256 "$DIGEST" --arg goaway_evidence "$GOAWAY_EVIDENCE" \
  '.goaway += {binary_evidence: $goaway_evidence} | . + {sha256: $sha256}' \
  "$WORK/attestation.json" >"$OUTPUT"

echo "captured $ROLE at $SOURCE_COMMIT -> $OUTPUT (canonical sha256 $DIGEST)"
