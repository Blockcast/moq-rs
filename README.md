# moq-rs

An implementation of the Media over QUIC Transport (MoQT) protocol for live media delivery over QUIC, as specified by the IETF MoQ working group.

This codebase was originally created by [Luke Curley (@kixelated)](https://github.com/kixelated). [Mike English (@englishm)](https://github.com/englishm) contributed to early design and has maintained this IETF-aligned fork. The project is now maintained by Cloudflare. The implementation targets [draft-ietf-moq-transport-16](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/16/).

> **Note:** This repository includes example client and server applications (moq-pub, moq-sub, moq-clock-ietf, moq-relay-ietf) intended to demonstrate usage of the moq-transport library. These are meant for testing and development purposes and have not been optimized for production use.

## Protocol Support

The `main` branch targets **draft-16** of the MoQT specification. For draft-07 compatibility (used in [Cloudflare's current production deployment](https://developers.cloudflare.com/moq/)), see the [`draft-ietf-moq-transport-07`](https://github.com/cloudflare/moq-rs/tree/draft-ietf-moq-transport-07) branch.

### What's Included

This repository provides:
- **moq-transport**: A complete MoQT protocol library implementation
- **moq-relay-ietf**: A production-ready relay server
- **Sample clients**: An fMP4 publisher (moq-pub) and a demonstration clock application (moq-clock-ietf)

### Protocol Feature Support

**Supported:**
- CLIENT_SETUP / SERVER_SETUP
- PUBLISH_NAMESPACE
- PUBLISH / PUBLISH_OK / PUBLISH_DONE
- SUBSCRIBE
- Standalone FETCH passthrough for local and remote PUBLISH_NAMESPACE origins
- WebTransport and raw QUIC transport layers
- Both stream ("subgroup") and datagram delivery modes

**Not Supported:**
- SUBSCRIBE_NAMESPACE (Soon)
- Joining FETCH, exact PUBLISH origins, and cached FETCH responses

### Wire Profiles

Transport negotiation selects an exact wire profile: the profile name is the
ALPN on native QUIC, and the same value travels in
WT-Available-Protocols / WT-Protocol on WebTransport. `moqt-16` is the default
and is always offered. `moq-relay-ietf` and `moq-pub-mmtp` take
`--wire-profile <name>` to additively offer one more:

| Profile | ALPN | Status |
| --- | --- | --- |
| draft-16 | `moqt-16` | Default. Full media publish/subscribe/relay. |
| `blockcast01` | `moqt-blockcast-01` | Draft-16 framing with mandatory bounded subgroup history. Full media path. |
| `draft19` | `moqt-19` | **Control plane only.** |

Draft-19 is a different wire from draft-16: two unidirectional control streams
instead of one bidirectional, and SETUP type `0x2f00` with delta-coded
`SetupOptions` instead of `CLIENT_SETUP`/`SERVER_SETUP`. The implementation
covers SETUP, GOAWAY (New Session URI plus millisecond timeout), and request
stream lifecycle. It has **no draft-19 publisher or subscriber**, so:

- `moq-relay-ietf --wire-profile draft19` completes the draft-19 handshake and
  then refuses every other control message out loud. It routes no media.
- `moq-pub-mmtp --wire-profile draft19` completes the handshake, logs the
  negotiated profile, and exits. MMTP publish is draft-16 only.

A draft-16 session refuses a draft-19 negotiation with a typed
`SessionError::ProfileFramingMismatch`, so no session can report
`selected_version() == moqt-19` while writing draft-16 bytes.

## Interoperability

A public relay instance running the latest `main` branch is available for interop testing at:
```
https://interop-relay.cloudflare.mediaoverquic.com:443
```

As an implementation targeting the IETF specification, this codebase should be compatible with other implementations targeting the same draft version. See the [moq-wg/moq-transport wiki](https://github.com/moq-wg/moq-transport/wiki/Interop) for a list of other implementations.

For streaming format compatibility:
- **[video-dev/moq-js](https://github.com/video-dev/moq-js)**: A TypeScript player implementation compatible with moq-pub's fMP4 output. Check draft version compatibility when selecting branches.
- **[gst-moq-pub](https://github.com/rafaelcaricio/gst-moq-pub)**: A GStreamer publisher plugin built on moq-pub, also compatible with moq-js.

## Components

This repository contains several crates:

- **moq-transport**: A media-agnostic library implementing the core MoQT protocol.
  - **moq-native-ietf**: QUIC and TLS utilities for native transport.
- **moq-relay-ietf**: A relay server that forwards content from publishers to subscribers, with caching and deduplication.
  - **moq-api**: An HTTP API server for origin discovery and relay coordination, backed by Redis.
- **moq-pub**: A publisher client that broadcasts fMP4 streams over MoQT.
  - **moq-catalog**: Catalog format handling.
  - **moq-sub**: A subscriber client for consuming MoQT streams.
- **moq-clock-ietf**: A simple time publisher/subscriber demonstrating non-media use cases.

## Development

Typical development workflow:

1. Start a local relay in one terminal:
   ```bash
   ./dev/relay
   ```

2. Start a publisher in another terminal:
   ```bash
   ./dev/pub
   ```
   Add `--tls-disable-verify` if you prefer not to install local certificates.

3. For playback, clone and run [moq-js](https://github.com/video-dev/moq-js) locally, then open it in a browser pointed at your local relay.

See the [dev helper scripts](dev/README.md) for more options and workflows.

## Usage

For detailed usage information, see the README in each crate directory:
- [moq-transport](moq-transport/README.md) - Protocol library documentation
- [moq-relay-ietf](moq-relay-ietf/README.md) - Relay server configuration
- [moq-pub](moq-pub/README.md) - Publisher client usage

The moq-transport crate is also published on [crates.io](https://crates.io/crates/moq-transport) with [API documentation](https://docs.rs/moq-transport/latest/moq_transport/).

## License

Licensed under either:
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
