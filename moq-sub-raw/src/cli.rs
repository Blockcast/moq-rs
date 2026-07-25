// SPDX-License-Identifier: MIT OR Apache-2.0

use std::path::PathBuf;

use clap::Parser;
use url::Url;

/// Subscribe to named tracks on a MoQ broadcast and dump each track's
/// raw object payloads to its own file. Paired `--track`/`--output`
/// arguments — repeat for multiple tracks (e.g.
/// `--track v --output v.bin --track a --output a.bin`).
#[derive(Parser, Clone)]
#[command(version, about = "Raw object-payload subscriber for IETF moq-transport (draft-14+)", long_about = None)]
pub struct Args {
    /// Connect URL of the relay (e.g. https://localhost:4443). No
    /// trailing path — pass the broadcast name via --name to keep the
    /// relay's tenant scope aligned with the publisher (see
    /// .planning/moq-rs-m0-results.md).
    pub url: Url,

    /// Broadcast name (the MoQT track namespace).
    #[arg(long)]
    pub name: String,

    /// Track names to subscribe to. Pair 1:1 with --output values
    /// (positional pairing).
    #[arg(long = "track", value_name = "NAME")]
    pub track: Vec<String>,

    /// Output file paths receiving the raw concatenated object
    /// payloads of each track. Pair 1:1 with --track values.
    #[arg(long = "output", value_name = "PATH")]
    pub output: Vec<PathBuf>,

    /// Client-side UDP bind for the QUIC/WebTransport connection.
    #[arg(long, default_value = "[::]:0")]
    pub bind: std::net::SocketAddr,

    /// TLS configuration shared with moq-pub-mmtp / moq-pub /
    /// moq-relay-ietf: `--tls-cert`, `--tls-key`, `--tls-root`,
    /// `--tls-disable-verify`.
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,
}
