# SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
# SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
# SPDX-License-Identifier: MIT OR Apache-2.0

# Rust 1.96.0 (2026-05-28). Keep this aligned with the parent repository's
# other Rust image pins instead of silently advancing on every build.
FROM rust:1.96-bookworm AS builder

WORKDIR /build

# Copy only manifests first so application source changes retain the compiled
# dependency layer. Dummy targets make every workspace package buildable.
COPY Cargo.toml Cargo.lock ./
COPY moq-api/Cargo.toml moq-api/Cargo.toml
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
      moq-api/src moq-catalog/src moq-clock-ietf/src \
      moq-native-ietf/src moq-pub/src moq-pub-mmtp/src \
      moq-pub-mmtp/vendor/mmt-core/src moq-pub-mmtp/vendor/mmt-core/benches \
      moq-relay-ietf/src/bin/moq-relay-ietf \
      moq-sub/src moq-sub-raw/src moq-test-client/src moq-transport/src && \
    for crate in moq-api moq-catalog moq-native-ietf moq-pub moq-relay-ietf moq-sub moq-transport; do \
      echo "" > "$crate/src/lib.rs"; \
    done && \
    echo "" > moq-pub-mmtp/vendor/mmt-core/src/lib.rs && \
    echo "fn main() {}" > moq-pub-mmtp/vendor/mmt-core/benches/header_bench.rs && \
    for crate in moq-api moq-clock-ietf moq-pub moq-pub-mmtp moq-sub moq-sub-raw moq-test-client; do \
      echo "fn main() {}" > "$crate/src/main.rs"; \
    done && \
    echo "fn main() {}" > moq-relay-ietf/src/bin/moq-relay-ietf/main.rs

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    cargo build --release

COPY . ./

# Reuse a cache between builds.
# I tried to `cargo install`, but it doesn't seem to work with workspaces.
# There's also issues with the cache mount since it builds into /usr/local/cargo/bin
# We can't mount that without clobbering cargo itself.
# We instead we build the binaries and copy them to the cargo bin directory.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    find . -path '*/src/*.rs' -o -path '*/src/**/*.rs' | xargs touch && \
    cargo build --release && cp /build/target/release/moq-* /usr/local/cargo/bin

# Optional: overwrite moq-pub-mmtp with a profiling-enabled build (feature
# `profiling` = on-demand pprof endpoint, itself inert unless MOQ_PUB_PROFILE_ADDR
# is set at runtime). `--build-arg PROFILING=1` swaps in the profiling binary;
# unset (the default) makes this a no-op and the default binary is unchanged.
ARG PROFILING=""
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    if [ -n "$PROFILING" ]; then \
      cargo build --release -p moq-pub-mmtp --features profiling && \
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

# Create an image with just the binaries
FROM debian:bookworm-slim

RUN apt-get update && \
	apt-get install -y --no-install-recommends ca-certificates curl libssl3 && \
	rm -rf /var/lib/apt/lists/*

LABEL org.opencontainers.image.source=https://github.com/kixelated/moq-rs
LABEL org.opencontainers.image.licenses="MIT OR Apache-2.0"

COPY --from=builder /usr/local/cargo/bin/moq-* /usr/local/bin

# Entrypoint to load relay TLS config in Fly
# TODO remove this; it should be specific to the fly deployment.
COPY deploy/fly-relay.sh .

# Default to moq-relay
CMD ["moq-relay"]
