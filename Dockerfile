# SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
# SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
# SPDX-License-Identifier: MIT OR Apache-2.0

# Rust 1.96.0 (2026-05-28). Keep this aligned with the parent repository's
# other Rust image pins instead of silently advancing on every build.
FROM rust:1.96-bookworm AS builder

WORKDIR /build

# sccache: object-level compile cache in the cluster's ceph RGW, mirroring
# packages/dual-stack-relay/Dockerfile:17-33 in the parent repository
# (Blockcast/pim-multicast-gateway), which builds this image.
#
# The three cargo steps below already use BuildKit `--mount=type=cache` for the
# registry and target/ dir, and on a warm daemon those make this a no-op. But
# they are LOCAL STATE ON ONE BUILDKIT DAEMON, and this image is built against
# two persistent daemons holding separate RWO caches -- a random pick
# cold-missed ~50% of builds, which is why the parent's docker-build.yml pins a
# daemon per service (`pin-key`). sccache is keyed on the compilation itself
# and its objects live in shared object storage, so it is the fallback for
# exactly the builds where the pin flipped to the peer daemon or the mount was
# evicted. Tail-risk insurance, not a steady-state speedup.
#
# The scripts are vendored under .github/scripts/ rather than COPY'd from the
# parent because this image is built with `context: moq-rs` -- see the header
# in each script for the source of truth and the drift check.
#
# Both fail open: no credentials, no endpoint, an unreachable RGW, or a failed
# sccache download all compile exactly as before. That is what keeps this repo's
# own `docker build` (.github/workflows/pr.yml, heap-profile-image.yml), which
# passes none of these, byte-for-byte unchanged in behaviour. A cache is not a
# dependency.
COPY .github/scripts/sccache-install.sh .github/scripts/sccache-env.sh /usr/local/bin/
RUN /usr/local/bin/sccache-install.sh

# Empty for local builds, which then compile without sccache exactly as before.
# The parent's .github/scripts/docker-buildx-with-session-retry.sh:83-99 appends
# this build-arg and the two ceph_s3_* BuildKit secrets to every buildx
# invocation in the build-images job, so no workflow change is needed.
ARG SCCACHE_RGW_ENDPOINT=
ARG SCCACHE_BUCKET=pim-rust-sccache
ENV SCCACHE_RGW_ENDPOINT=${SCCACHE_RGW_ENDPOINT} \
    SCCACHE_BUCKET=${SCCACHE_BUCKET}

# Copy only manifests first so application source changes retain the compiled
# dependency layer. Dummy targets make every workspace package buildable.
COPY Cargo.toml Cargo.lock ./
COPY moq-api/Cargo.toml moq-api/Cargo.toml
COPY moq-canary/Cargo.toml moq-canary/Cargo.toml
COPY moq-catalog/Cargo.toml moq-catalog/Cargo.toml
COPY moq-clock-ietf/Cargo.toml moq-clock-ietf/Cargo.toml
COPY moq-native-ietf/Cargo.toml moq-native-ietf/Cargo.toml
COPY moq-pub/Cargo.toml moq-pub/Cargo.toml
COPY moq-pub-mmtp/Cargo.toml moq-pub-mmtp/Cargo.toml
COPY moq-pub-mmtp/vendor/mmt-core/Cargo.toml moq-pub-mmtp/vendor/mmt-core/Cargo.toml
COPY moq-relay-ietf/Cargo.toml moq-relay-ietf/Cargo.toml
COPY moq-sub/Cargo.toml moq-sub/Cargo.toml
COPY moq-sub-raw/Cargo.toml moq-sub-raw/Cargo.toml
COPY moq-test-client/Cargo.toml moq-test-client/Cargo.toml
COPY moq-transport/Cargo.toml moq-transport/Cargo.toml

RUN mkdir -p \
      moq-api/src moq-canary/src moq-catalog/src moq-clock-ietf/src \
      moq-native-ietf/src moq-pub/src moq-pub-mmtp/src \
      moq-pub-mmtp/vendor/mmt-core/src moq-pub-mmtp/vendor/mmt-core/benches \
      moq-relay-ietf/src/bin/moq-relay-ietf \
      moq-sub/src moq-sub-raw/src moq-test-client/src moq-transport/src && \
    for crate in moq-api moq-catalog moq-native-ietf moq-pub moq-relay-ietf moq-sub moq-transport; do \
      echo "" > "$crate/src/lib.rs"; \
    done && \
    echo "" > moq-pub-mmtp/vendor/mmt-core/src/lib.rs && \
    echo "fn main() {}" > moq-pub-mmtp/vendor/mmt-core/benches/header_bench.rs && \
    for crate in moq-api moq-canary moq-clock-ietf moq-pub moq-pub-mmtp moq-sub moq-sub-raw moq-test-client; do \
      echo "fn main() {}" > "$crate/src/main.rs"; \
    done && \
    echo "fn main() {}" > moq-relay-ietf/src/bin/moq-relay-ietf/main.rs

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    --mount=type=secret,id=ceph_s3_key \
    --mount=type=secret,id=ceph_s3_secret \
    . /usr/local/bin/sccache-env.sh; \
    cargo build --release --features moq-pub-mmtp/metrics-prometheus

COPY . ./

# Reuse a cache between builds.
# I tried to `cargo install`, but it doesn't seem to work with workspaces.
# There's also issues with the cache mount since it builds into /usr/local/cargo/bin
# We can't mount that without clobbering cargo itself.
# We instead we build the binaries and copy them to the cargo bin directory.
#
# moq-pub-mmtp/metrics-prometheus (BLO-22882): compiled in by default so the
# MOQ_PUB_METRICS_ADDR env var (set via Helm) is live without a separate
# opt-in image variant — unlike profiling/heap-profiling below, the exporter
# itself stays inert (no listener bound) until that env var is set, so this
# does not change default runtime behavior.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    --mount=type=secret,id=ceph_s3_key \
    --mount=type=secret,id=ceph_s3_secret \
    . /usr/local/bin/sccache-env.sh; \
    find . -path '*/src/*.rs' -o -path '*/src/**/*.rs' | xargs touch && \
    cargo build --release --features moq-pub-mmtp/metrics-prometheus && \
    cp /build/target/release/moq-* /usr/local/cargo/bin

# Optional: overwrite moq-pub-mmtp with a profiling-enabled build. CPU profiling
# uses PROFILING=1; retained-allocation profiling uses HEAP_PROFILING=1 and the
# jemalloc-backed `heap-profiling` feature. Both remain runtime-gated by
# MOQ_PUB_PROFILE_ADDR; the default image is unchanged.
ARG PROFILING=""
ARG HEAP_PROFILING=""
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    --mount=type=secret,id=ceph_s3_key \
    --mount=type=secret,id=ceph_s3_secret \
    . /usr/local/bin/sccache-env.sh; \
    if [ -n "$HEAP_PROFILING" ]; then \
      JEMALLOC_SYS_WITH_MALLOC_CONF="prof:true,prof_active:false,lg_prof_sample:19" \
        RUSTFLAGS="-C force-frame-pointers=yes" \
        cargo rustc --release -p moq-pub-mmtp --features heap-profiling,metrics-prometheus -- \
          -C link-arg=-no-pie && \
      cp /build/target/release/moq-pub-mmtp /usr/local/cargo/bin/moq-pub-mmtp; \
    elif [ -n "$PROFILING" ]; then \
      cargo build --release -p moq-pub-mmtp --features profiling,metrics-prometheus && \
      cp /build/target/release/moq-pub-mmtp /usr/local/cargo/bin/moq-pub-mmtp; \
    fi

# Create a pub image that also contains ffmpeg and a helper script
FROM debian:bookworm-slim as moq-pub

# Install required utilities and ffmpeg
RUN apt-get update && \
    apt-get install -y ffmpeg wget

# Copy the publish script into the image
COPY ./deploy/publish /usr/local/bin/publish

# Copy over the built binaries.
COPY --from=builder /usr/local/cargo/bin/moq-* /usr/local/bin

# Use our publish script
CMD [ "publish" ]

# Validate image provenance in an independent stage so the contract can be
# tested without compiling the Rust workspace.
FROM debian:bookworm-slim AS image-provenance
ARG SOURCE_REVISION
ARG BASE_REVISION

# BLO-22346: provenance is mandatory, not best-effort. A silent "unknown"
# default let every published image ship unattributable to any source
# commit; refuse to build rather than fall back to a value nothing can
# trace back to a revision.
RUN for pair in "SOURCE_REVISION=$SOURCE_REVISION" "BASE_REVISION=$BASE_REVISION"; do \
	name=${pair%%=*}; value=${pair#*=}; \
	if ! printf '%s' "$value" | grep -Eq '^[0-9a-f]{40}$'; then \
		echo "error: $name build-arg must be a 40-hex git commit sha (got '$value'). Pass --build-arg $name=<sha> so this image is traceable to source." >&2; \
		exit 1; \
	fi; \
    done

LABEL org.opencontainers.image.revision=$SOURCE_REVISION
LABEL org.opencontainers.image.base.revision=$BASE_REVISION

# Create an image with just the binaries.
FROM image-provenance

ARG PROFILE_KIND="default"

RUN apt-get update && \
	apt-get install -y --no-install-recommends ca-certificates curl libssl3 && \
	rm -rf /var/lib/apt/lists/*

LABEL org.opencontainers.image.source=https://github.com/Blockcast/moq-rs
LABEL org.opencontainers.image.licenses="MIT OR Apache-2.0"
LABEL org.opencontainers.image.description="moq-rs binaries"
LABEL org.blockcast.profile.kind=$PROFILE_KIND

COPY --from=builder /usr/local/cargo/bin/moq-* /usr/local/bin

# Entrypoint to load relay TLS config in Fly
# TODO remove this; it should be specific to the fly deployment.
COPY deploy/fly-relay.sh .

# Default to moq-relay
CMD ["moq-relay"]
