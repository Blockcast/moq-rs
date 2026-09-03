#!/bin/sh
# SPDX-FileCopyrightText: 2026 Blockcast Inc.
# SPDX-License-Identifier: MIT OR Apache-2.0
#
# ---------------------------------------------------------------------------
# VENDORED COPY -- do not edit here first.
# Source of truth: Blockcast/pim-multicast-gateway .github/scripts/sccache-env.sh
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
#   diff <(sed '2,21d' .github/scripts/sccache-env.sh) \
#        <pim-multicast-gateway>/.github/scripts/sccache-env.sh
# ---------------------------------------------------------------------------
# Turn on sccache for a Rust image build when CI supplies ceph RGW credentials.
#
# SOURCED, not executed:  . /usr/local/bin/sccache-env.sh && cargo build ...
# It has to be sourced because it exports RUSTC_WRAPPER into the shell that
# then runs cargo; a subprocess wrapper would not survive into the compound
# `find && cargo clean && cargo build && ...` commands these Dockerfiles use.
#
# Why an object-level cache at all, when the images already use BuildKit
# `--mount=type=cache` for the cargo registry and target dir: those two are
# LOCAL STATE ON ONE BUILDKIT DAEMON, and the registry layer cache is
# all-or-nothing per RUN and only exports on success. A submodule pin bump —
# routine in this repo — changes a COPY layer and invalidates every RUN after
# it, so the layer cache misses completely and every third-party crate
# recompiles. sccache is keyed on the compilation itself, so unchanged crates
# hit regardless of which layer got invalidated, and a killed or timed-out
# build still warms the next attempt.
#
# Mirrors .github/chromium-image/Dockerfile.build, which uses the same
# endpoint / BuildKit-secret / probe shape for the Chromium builder. Secrets
# arrive as `--mount=type=secret`, so they never land in a layer or a cache key.
#
# FAILS OPEN by design: no endpoint, no credentials, or an unreachable RGW all
# leave RUSTC_WRAPPER unset and compile exactly as before. A local `docker
# build` with no build-args therefore behaves identically to today. It must
# never fail the build — a cache is not a dependency.
#
# But note what failing open costs when it happens PART WAY THROUGH a stage.
# RUSTC_WRAPPER participates in cargo's fingerprint, so a stage whose steps
# disagree about it invalidates everything the earlier steps cooked. In
# packages/dual-stack-relay/Dockerfile that is four independent decision points
# (chef cook, chef cook --tests, build, test) sharing one target/ tree: if the
# endpoint probe succeeds for the cooks and then fails for the build, the build
# does not merely lose the cache, it discards the whole cooked dependency graph
# and recompiles it — strictly slower than never having enabled sccache at all.
# The verdict file below makes the first decision in a stage bind the rest of
# it, so a mid-stage flap cannot do that.

# One decision per stage. Written on the first sourcing and honored by every
# later one: a file written by a RUN persists into the layer, so subsequent
# RUNs in the same stage — and stages derived from it via `FROM <stage>`, as
# dual-stack-relay's `test` is from `builder` — read the same verdict.
#
# This is also what keeps a cache-hit layer coherent. If the cook layers were
# built with no credentials they carry verdict `off`, and the build step honors
# it rather than switching the wrapper on and invalidating the cooked artifacts
# it was about to reuse.
SCCACHE_VERDICT_FILE="${SCCACHE_VERDICT_FILE:-/var/lib/sccache-verdict}"
sccache_pinned=""
if [ -r "$SCCACHE_VERDICT_FILE" ]; then
  sccache_pinned="$(cat "$SCCACHE_VERDICT_FILE" 2>/dev/null || true)"
fi
sccache_verdict=off

if [ "$sccache_pinned" = "off" ]; then
  echo "sccache disabled (pinned off by an earlier step in this stage; enabling it here would change RUSTC_WRAPPER mid-stage and invalidate everything already compiled)"
elif [ -z "${SCCACHE_RGW_ENDPOINT:-}" ]; then
  echo "sccache disabled (no SCCACHE_RGW_ENDPOINT build-arg)"
elif [ ! -s /run/secrets/ceph_s3_key ] || [ ! -s /run/secrets/ceph_s3_secret ]; then
  echo "sccache disabled (RGW credentials not mounted)"
elif ! curl -sf --max-time 5 "${SCCACHE_RGW_ENDPOINT}" >/dev/null 2>&1; then
  echo "sccache disabled (RGW endpoint probe failed: ${SCCACHE_RGW_ENDPOINT})"
elif ! sccache --version >/dev/null 2>&1; then
  # `sccache --version`, not `command -v sccache`: sccache-install.sh fails open
  # and can leave no binary at all, and an arch-mismatched or truncated download
  # would still satisfy a PATH lookup while failing to execute.
  echo "sccache disabled (binary not installed or not runnable in this stage)"
else
  AWS_ACCESS_KEY_ID="$(cat /run/secrets/ceph_s3_key)"
  AWS_SECRET_ACCESS_KEY="$(cat /run/secrets/ceph_s3_secret)"
  export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
  export SCCACHE_BUCKET="${SCCACHE_BUCKET:-pim-rust-sccache}"
  # sccache wants host[:port], not a URL.
  SCCACHE_ENDPOINT="${SCCACHE_RGW_ENDPOINT#http://}"
  export SCCACHE_ENDPOINT="${SCCACHE_ENDPOINT#https://}"
  export SCCACHE_S3_USE_SSL=off
  export SCCACHE_REGION=us-east-1
  # 0 = never idle-shutdown; the server must outlive gaps between cargo
  # invocations inside one RUN.
  export SCCACHE_IDLE_TIMEOUT=0
  export RUSTC_WRAPPER=sccache
  # Incremental artifacts are machine-local and defeat a shared object cache;
  # .github/actions/setup-rust-sccache disables it for the same reason.
  export CARGO_INCREMENTAL=0

  if sccache --start-server >/dev/null 2>&1; then
    echo "sccache enabled (bucket ${SCCACHE_BUCKET}, endpoint ${SCCACHE_ENDPOINT})"
    sccache_verdict=on

    # Hit-rate telemetry, and a clean shutdown, for every in-image build —
    # matching the `sccache --show-stats` step that every workflow using
    # .github/actions/setup-rust-sccache already runs (enforced by
    # packages/moq-server/tests/sccache-workflow-contract.test.mjs). An EXIT
    # trap rather than an explicit call after each cargo line: this script is
    # sourced into the RUN's shell, so one trap covers all eleven call sites
    # across five Dockerfiles and cannot be forgotten when a new one is added.
    #
    # It fires on failure too, which is when the numbers matter most: a build
    # reporting 0 hits over hundreds of compilations is a broken cache, and
    # without this it looked exactly like a working one.
    #
    # --stop-server is not cosmetic. sccache uploads objects from the server
    # process asynchronously, so ending the RUN with writes still in flight
    # discards them; stopping the server drains that queue, which is the
    # difference between warming the next build and only paying for this one.
    # Both calls are `|| true` — a cache is not a dependency, and a stats or
    # shutdown error must not change the RUN's exit status.
    trap 'sccache --show-stats 2>&1 || true; sccache --stop-server >/dev/null 2>&1 || true' EXIT
  else
    # Server refused to start: unset everything rather than handing cargo a
    # wrapper that will fail on every single invocation.
    unset RUSTC_WRAPPER
    echo "sccache disabled (server failed to start)"
  fi
fi

if [ -z "$sccache_pinned" ]; then
  # First sourcing in this stage — record the decision for the rest of it.
  # Best-effort: a stage with a read-only /var must still build. Both the
  # mkdir and the write are wrapped in a stderr-redirected group rather than
  # each carrying `2>/dev/null`: redirections are set up before the command
  # runs, so a failing `> "$file"` reports on the shell's own stderr and its
  # own `2>/dev/null` never gets the chance to suppress it — which put a line
  # that reads like a build error into the log of a build that was fine.
  { mkdir -p "$(dirname "$SCCACHE_VERDICT_FILE")" \
      && printf '%s\n' "$sccache_verdict" > "$SCCACHE_VERDICT_FILE"; } 2>/dev/null || true
elif [ "$sccache_pinned" = "on" ] && [ -z "${RUSTC_WRAPPER:-}" ]; then
  # The mirror image of the case the verdict file prevents, and the one it
  # cannot fix: an earlier layer compiled WITH the wrapper (or was restored
  # from a cache that did) and it is unavailable now. Proceeding without it is
  # correct but not free — cargo's fingerprint changed, so the cooked graph
  # gets rebuilt. Say so, because the only symptom is a long build.
  echo "warning: this stage was cooked with sccache but it is unavailable now; cargo will recompile the cached dependency graph"
fi
