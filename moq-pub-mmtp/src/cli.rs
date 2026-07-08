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

    /// Source-Specific Multicast (SSM) source address (IPv4 or IPv6). When set
    /// AND the --mmtp-udp-bind target is multicast of the same family, the
    /// receiver issues a source-specific (S,G) join instead of an any-source
    /// (*,G) join. REQUIRED for SSM groups (232.0.0.0/8, ff3x::/... source-
    /// specific): the fabric only forwards SSM traffic to receivers that name
    /// the source, so a plain (*,G) join receives nothing. Omit for ASM groups
    /// or loopback smoke tests.
    #[arg(long = "mmtp-udp-source")]
    pub mmtp_udp_source: Option<std::net::IpAddr>,

    /// Local interface IPv4 address (imr_interface) for an IPv4 SSM join. Omit
    /// to let the route to the group pick the NIC. IPv6 uses
    /// --mmtp-udp-iface-index instead.
    #[arg(long = "mmtp-udp-iface")]
    pub mmtp_udp_iface: Option<std::net::Ipv4Addr>,

    /// Local interface index for an IPv6 SSM join (0 = pick via route). IPv4
    /// uses --mmtp-udp-iface instead.
    #[arg(long = "mmtp-udp-iface-index")]
    pub mmtp_udp_iface_index: Option<u32>,

    /// Seconds between periodic SSM membership re-reports. REQUIRED whenever a
    /// source-specific join is in effect (--mmtp-udp-source + multicast target):
    /// the receive fabric has no IGMP/MLD querier, so nothing prompts the host
    /// to re-report and the switch's snooping entry ages out (~260s, = IGMP
    /// Group Membership Interval), silently killing forwarding on a live stream.
    /// The receiver must self-refresh below the fabric's IGMP Query Interval
    /// (default 125s per RFC 3376) — e.g. 60. No default: an unset value with
    /// SSM enabled is a hard error, not a silent guess. Ignored for ASM/unicast.
    #[arg(long = "mmtp-membership-refresh-secs")]
    pub mmtp_membership_refresh_secs: Option<u64>,

    /// Client-side UDP bind for the QUIC/WebTransport connection to the relay.
    #[arg(long, default_value = "[::]:0")]
    pub bind: std::net::SocketAddr,

    /// TLS configuration shared with moq-pub / moq-relay-ietf:
    /// `--tls-cert`, `--tls-key`, `--tls-root`, `--tls-disable-verify`.
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,
}
