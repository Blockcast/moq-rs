// SPDX-License-Identifier: MIT OR Apache-2.0

use clap::Parser;
use url::Url;

/// Where the publisher reads MMTP packets from.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum MmtpInput {
    /// Length-prefixed framing on stdin: each frame is `[u32 BE length][payload]`.
    Stdin,
    /// Bound UDP socket — each datagram is one MMTP packet (per RFC 8551 framing).
    Udp,
}

#[derive(Parser, Clone)]
#[command(version, about = "MMTP publisher for IETF moq-transport (draft-14+)", long_about = None)]
pub struct Args {
    /// Connect URL of the relay (e.g. https://localhost:4443). No trailing path
    /// — pass the broadcast name via --name to keep the relay's tenant scope
    /// aligned with the subscriber (see .planning/moq-rs-m0-results.md for the
    /// scope-mismatch note).
    pub url: Url,

    /// Broadcast name (becomes the MoQT track namespace).
    #[arg(long)]
    pub name: String,

    /// Path to a catalog JSON file matching moq_catalog::Root. The publisher
    /// announces the listed tracks (each with TrackPackaging::Mmtp) and routes
    /// incoming MMTP packets by packet_id per the catalog's
    /// multicast.endpoints[].tracks[] map.
    #[arg(long = "catalog-json", value_name = "PATH")]
    pub catalog_json: std::path::PathBuf,

    /// Where MMTP packets come from.
    #[arg(long = "mmtp-input", value_enum, default_value = "stdin")]
    pub mmtp_input: MmtpInput,

    /// UDP bind address when --mmtp-input=udp. Each received datagram
    /// is one MMTP packet (no length prefix — the datagram boundary IS
    /// the packet boundary).
    #[arg(long = "mmtp-udp-bind", default_value = "0.0.0.0:0")]
    pub mmtp_udp_bind: std::net::SocketAddr,

    /// Client-side UDP bind for the QUIC/WebTransport connection to the relay.
    #[arg(long, default_value = "[::]:0")]
    pub bind: std::net::SocketAddr,

    /// TLS configuration shared with moq-pub / moq-relay-ietf:
    /// `--tls-cert`, `--tls-key`, `--tls-root`, `--tls-disable-verify`.
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,
}
