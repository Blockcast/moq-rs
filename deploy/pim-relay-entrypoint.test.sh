#!/bin/sh
# SPDX-FileCopyrightText: 2026 Blockcast Inc.
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Runs pim-relay-entrypoint.sh against a fake relay that prints what it got.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cat >"$tmp/relay" <<'EOF'
#!/bin/sh
echo "RUST_LOG=$RUST_LOG"
for a; do echo "$a"; done
EOF
chmod +x "$tmp/relay"

run() { env -u RUST_LOG RELAY_BIN="$tmp/relay" sh "$here/pim-relay-entrypoint.sh" "$@" 2>"$tmp/err"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

# The harness launch line (pim interop-matrix.test.mjs startRelay).
out=$(run --bind '[::]:4443' --http-port 4444 --protocols ietf-wt,ietf-raw \
	--catalog-probe-deadline-ms 5000 --log-level debug)
echo "$out" | grep -qx 'RUST_LOG=debug' || fail "--log-level not mapped: $out"
echo "$out" | grep -q 'catalog-probe' && fail "catalog probe flag leaked: $out"
echo "$out" | grep -qx 5000 && fail "catalog probe value leaked: $out"
grep -q 'ignoring --catalog-probe-deadline-ms 5000' "$tmp/err" || fail "no warning for dropped flag"
for want in --bind '[::]:4443' --http-port 4444 --protocols ietf-wt,ietf-raw --tls-cert --tls-key; do
	echo "$out" | grep -qxF -- "$want" || fail "missing $want: $out"
done
cert=$(echo "$out" | grep -A1 -x -- --tls-cert | tail -1)
openssl x509 -in "$cert" -noout -text | grep -q 'id-ecPublicKey' || fail "cert is not ECDSA"
days=$(( ($(date -d "$(openssl x509 -in "$cert" -noout -enddate | cut -d= -f2)" +%s) - $(date +%s)) / 86400 ))
[ "$days" -le 14 ] || fail "cert valid $days days; WebTransport hashes allow 14"

# An operator cert is kept, RUST_LOG wins over --log-level, moq-lite is passed through.
out=$(RUST_LOG=warn RELAY_BIN="$tmp/relay" sh "$here/pim-relay-entrypoint.sh" \
	--tls-cert /c.pem --tls-key /k.pem --log-level debug --generate-cert=true --protocols moq-lite)
echo "$out" | grep -qx 'RUST_LOG=warn' || fail "RUST_LOG overridden: $out"
[ "$(echo "$out" | grep -cx -- --tls-cert)" = 1 ] || fail "cert generated despite --tls-cert: $out"
echo "$out" | grep -q generate-cert && fail "--generate-cert leaked: $out"
echo "$out" | grep -qx moq-lite || fail "moq-lite must reach the relay so it refuses it: $out"

run --log-level >/dev/null && fail "--log-level without a value accepted"
echo "pim-relay-entrypoint: ok"
