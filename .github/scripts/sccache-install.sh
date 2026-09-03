#!/bin/sh
# SPDX-FileCopyrightText: 2026 Blockcast Inc.
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# ---------------------------------------------------------------------------
# VENDORED COPY -- do not edit here first.
# Source of truth: Blockcast/pim-multicast-gateway .github/scripts/sccache-install.sh
#
# moq-rs is built by that repository's docker-build.yml with `context: moq-rs`
# (the build-images matrix entry for moq-pub-mmtp), so the parent's copy sits
# outside this image's build context and cannot be COPY'd into it. Vendoring
# is the same pattern hang-mmt-fec and libmmt already use for their
# .github/actions/setup-rust-sccache copies.
#
# Apart from this block the file is byte-identical to the parent's. Keep it
# that way: the in-image contract it implements (single verdict per stage,
# EXIT trap for stats + upload drain, fail-open probes) is pinned by the
# parent's packages/moq-server/tests/sccache-workflow-contract.test.mjs,
# which cannot see this copy. Check for drift with:
#
#   diff <(sed '2,21d' .github/scripts/sccache-install.sh) \
#        <pim-multicast-gateway>/.github/scripts/sccache-install.sh
# ---------------------------------------------------------------------------
# Install the sccache binary into a Rust builder stage.
#
# Run as its OWN RUN layer, not inside the cargo RUN: that layer is then cached
# and the ~10MB download happens once per base-image change instead of on every
# source edit. Pair it with sccache-env.sh, which is sourced by the cargo RUN
# and does the credential/probe/env work.
#
# FAILS OPEN, deliberately, and that is the whole contract: a cache is not a
# dependency, so nothing here may fail an image build. If the download, the
# archive, or the apt prerequisite install fails, we exit 0 with no binary
# installed; sccache-env.sh then hits its `command -v sccache` branch and the
# build compiles exactly as it does today, just slower.

set -u

SCCACHE_VERSION="${SCCACHE_VERSION:-0.8.2}"

# musl build: statically linked, so one artifact works on every base here
# (rust:*-slim, rust:*-bookworm, cargo-chef's slim-bookworm) with no glibc
# version coupling.
case "$(uname -m)" in
  x86_64)  arch=x86_64-unknown-linux-musl ;;
  aarch64) arch=aarch64-unknown-linux-musl ;;
  *)
    echo "sccache install skipped (unsupported arch $(uname -m))"
    exit 0
    ;;
esac

if ! command -v curl >/dev/null 2>&1; then
  # Several stages install curl themselves; only pay for apt when they didn't.
  if command -v apt-get >/dev/null 2>&1; then
    apt-get update >/dev/null 2>&1 &&
      apt-get install -y --no-install-recommends curl ca-certificates >/dev/null 2>&1
    rm -rf /var/lib/apt/lists/*
  fi
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "sccache install skipped (no curl available)"
  exit 0
fi

url="https://github.com/mozilla/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-${arch}.tar.gz"

if curl -sfL --retry 3 --retry-delay 2 --connect-timeout 30 "$url" |
   tar xz -C /usr/local/bin --strip-components=1 --wildcards "*/sccache" 2>/dev/null &&
   sccache --version; then
  exit 0
fi

echo "sccache install failed for $url; continuing without a compile cache"
exit 0
