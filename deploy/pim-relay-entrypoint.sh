#!/bin/sh
# SPDX-FileCopyrightText: 2026 Blockcast Inc.
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# Adapts the dual-stack-relay launch line used by the PIM harness to
# moq-relay-ietf. The harness runs the image with relay flags only, so this is
# the ENTRYPOINT.
#
#   --log-level <f>                   becomes RUST_LOG unless RUST_LOG is set
#   --generate-cert[=<b>]             accepted; a cert is generated anyway when
#                                     no --tls-cert is given
#   --catalog-probe-deadline-ms <n>   dropped with a warning: dual-stack's
#                                     catalog probe does not exist here
#
# Everything else is passed through unchanged. That includes --protocols: a
# list containing moq-lite is refused by the relay, not stripped here.
set -eu

relay="${RELAY_BIN:-moq-relay-ietf}"
have_cert=false
n=$#
i=0
while [ "$i" -lt "$n" ]; do
	a=$1
	shift
	i=$((i + 1))
	case "$a" in
	--log-level)
		[ "$i" -lt "$n" ] || { echo "pim-relay-entrypoint: --log-level needs a value" >&2; exit 2; }
		: "${RUST_LOG:=$1}"
		shift
		i=$((i + 1))
		;;
	--log-level=*) : "${RUST_LOG:=${a#*=}}" ;;
	--generate-cert | --generate-cert=*) ;;
	--catalog-probe-deadline-ms)
		[ "$i" -lt "$n" ] || { echo "pim-relay-entrypoint: $a needs a value" >&2; exit 2; }
		echo "pim-relay-entrypoint: ignoring $a $1: no catalog probe in moq-relay-ietf" >&2
		shift
		i=$((i + 1))
		;;
	--catalog-probe-deadline-ms=*)
		echo "pim-relay-entrypoint: ignoring $a: no catalog probe in moq-relay-ietf" >&2
		;;
	*)
		case "$a" in --tls-cert | --tls-cert=*) have_cert=true ;; esac
		set -- "$@" "$a"
		;;
	esac
done
export RUST_LOG="${RUST_LOG:-info,quinn=warn}"

if [ "$have_cert" = false ]; then
	# WebTransport serverCertificateHashes accepts only ECDSA certificates
	# valid for at most 14 days, which is why dual-stack generates one too.
	dir=$(mktemp -d)
	openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
		-days 10 -subj "/CN=localhost" \
		-addext "subjectAltName=DNS:localhost,DNS:$(hostname),IP:127.0.0.1,IP:::1" \
		-keyout "$dir/key.pem" -out "$dir/cert.pem" 2>"$dir/openssl.err" ||
		{ echo "pim-relay-entrypoint: cert generation failed" >&2; cat "$dir/openssl.err" >&2; exit 1; }
	set -- "$@" --tls-cert "$dir/cert.pem" --tls-key "$dir/key.pem"
fi

exec "$relay" "$@"
