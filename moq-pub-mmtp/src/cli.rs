// SPDX-License-Identifier: MIT OR Apache-2.0

use clap::{Parser, ValueEnum};
use moq_transport::profile::WireProfile;
use url::Url;

/// Where the publisher reads MMTP packets from.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum MmtpInput {
    /// Length-prefixed framing on stdin: each frame is `[u32 BE length][payload]`.
    Stdin,
    /// Bound UDP socket — each datagram is one MMTP packet (per RFC 8551 framing).
    Udp,
}

/// Optional non-default MoQT wire profile selected by the publisher.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum WireProfileArg {
    /// Negotiate the Blockcast profile requiring bounded subgroup history.
    Blockcast01,
}

impl From<WireProfileArg> for WireProfile {
    fn from(profile: WireProfileArg) -> Self {
        match profile {
            WireProfileArg::Blockcast01 => Self::Blockcast01,
        }
    }
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

    /// Source-Specific Multicast (SSM) source address. When set AND the
    /// --mmtp-udp-bind target is multicast, the receiver issues a
    /// source-specific (S,G) join (IP_ADD_SOURCE_MEMBERSHIP) instead of an
    /// any-source (*,G) join. REQUIRED for SSM groups (232.0.0.0/8): the
    /// multicast fabric only forwards SSM traffic to receivers that name the
    /// source, so a plain (*,G) join receives nothing. Omit for ASM groups
    /// or loopback smoke tests.
    #[arg(long = "mmtp-udp-source")]
    pub mmtp_udp_source: Option<std::net::Ipv4Addr>,

    /// Local interface IPv4 address to join the multicast group on
    /// (imr_interface). Omit to let the kernel pick via the route to the
    /// group — pair that with a `ip route … dev <iface>` route so the join
    /// lands on the multicast-bearing interface (e.g. a Multus secondary).
    #[arg(long = "mmtp-udp-iface")]
    pub mmtp_udp_iface: Option<std::net::Ipv4Addr>,

    /// Client-side UDP bind for the QUIC/WebTransport connection to the relay.
    #[arg(long, default_value = "[::]:0")]
    pub bind: std::net::SocketAddr,

    /// Opt into a non-default MoQT wire profile. When omitted, the existing
    /// moqt-16 negotiation path is unchanged.
    #[arg(long, value_enum)]
    pub wire_profile: Option<WireProfileArg>,

    /// Address to expose Prometheus metrics on (e.g., "127.0.0.1:9090").
    /// Requires the `metrics-prometheus` feature to be enabled.
    /// When set, serves metrics — including `moq_negotiation_total`, emitted
    /// by moq-native-ietf on every connect attempt — at http://<addr>/metrics.
    ///
    /// Precedence vs. `MOQ_PUB_METRICS_ADDR` (BLO-26174): the env var is read
    /// before this flag is even parsed, and the Prometheus recorder can only
    /// be installed once per process. If `MOQ_PUB_METRICS_ADDR` is set, it
    /// always wins — this flag is logged as ignored, not attempted, and the
    /// process keeps running on the env-var listener. Set only this flag (no
    /// env var) to use it. See metrics_endpoint::install_flag_exporter_if_needed.
    #[arg(long)]
    pub metrics_addr: Option<std::net::SocketAddr>,

    /// Minimum backoff before the first reconnect attempt after the relay
    /// session is lost. Doubles on each consecutive loss up to
    /// --reconnect-backoff-max-ms (BLO-26173: the publisher reconnects
    /// in-process instead of exiting on relay session timeout).
    #[arg(long = "reconnect-backoff-min-ms", default_value_t = 500)]
    pub reconnect_backoff_min_ms: u64,

    /// Reconnect backoff ceiling — consecutive relay session losses never
    /// wait longer than this between attempts.
    ///
    /// Doubles as the "healthy session" threshold: a session that stays up at
    /// least this long clears the consecutive-failure streak, so the next loss
    /// backs off from the floor again (see `attempt_after_session`). Raising
    /// this to be gentler on the relay therefore also raises the bar for what
    /// counts as healthy — the two are deliberately coupled today, but they are
    /// conceptually independent and may be split later.
    #[arg(long = "reconnect-backoff-max-ms", default_value_t = 30_000)]
    pub reconnect_backoff_max_ms: u64,

    /// TLS configuration shared with moq-pub / moq-relay-ietf:
    /// `--tls-cert`, `--tls-key`, `--tls-root`, `--tls-disable-verify`.
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn required_args() -> [&'static str; 6] {
        [
            "moq-pub-mmtp",
            "moqt://localhost:4443",
            "--name",
            "example",
            "--catalog-json",
            "catalog.json",
        ]
    }

    #[test]
    fn wire_profile_is_opt_in() {
        let args = Args::try_parse_from(required_args()).unwrap();
        assert!(args.wire_profile.is_none());

        let args = Args::try_parse_from(
            required_args()
                .into_iter()
                .chain(["--wire-profile", "blockcast01"]),
        )
        .unwrap();
        assert_eq!(
            args.wire_profile.map(WireProfile::from),
            Some(WireProfile::Blockcast01)
        );
    }

    #[test]
    fn metrics_addr_is_opt_in() {
        let args = Args::try_parse_from(required_args()).unwrap();
        assert!(args.metrics_addr.is_none());

        let args = Args::try_parse_from(
            required_args()
                .into_iter()
                .chain(["--metrics-addr", "127.0.0.1:9090"]),
        )
        .unwrap();
        assert_eq!(args.metrics_addr, Some("127.0.0.1:9090".parse().unwrap()));
    }

    #[test]
    fn reconnect_backoff_has_sane_defaults_and_is_overridable() {
        let args = Args::try_parse_from(required_args()).unwrap();
        assert_eq!(args.reconnect_backoff_min_ms, 500);
        assert_eq!(args.reconnect_backoff_max_ms, 30_000);

        let args = Args::try_parse_from(required_args().into_iter().chain([
            "--reconnect-backoff-min-ms",
            "100",
            "--reconnect-backoff-max-ms",
            "5000",
        ]))
        .unwrap();
        assert_eq!(args.reconnect_backoff_min_ms, 100);
        assert_eq!(args.reconnect_backoff_max_ms, 5000);
    }
}
