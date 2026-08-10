// SPDX-License-Identifier: MIT OR Apache-2.0

use clap::Parser;
use url::Url;

/// Continuously subscribe to a track under the negotiated Blockcast wire
/// profile (moqt-blockcast-01) and decode SUBSCRIBE_OK key 0x40
/// (bounded subgroup history window) on every cycle.
///
/// Each cycle opens a fresh connection rather than reusing one for the
/// whole run: this exercises real negotiation repeatedly, which is the
/// thing BLO-22255's verifying signal actually needs evidence of, not just
/// that one session happened to negotiate the profile once.
#[derive(Parser, Clone)]
#[command(
    version,
    about = "Blockcast wire-profile negotiation canary for IETF moq-transport",
    long_about = None
)]
pub struct Args {
    /// Connect URL of the relay/publisher (e.g. https://localhost:4443 or
    /// moqt://localhost:4443). No trailing path — pass the broadcast name
    /// via --name.
    pub url: Url,

    /// Broadcast name (the MoQT track namespace) to subscribe under.
    #[arg(long)]
    pub name: String,

    /// Track name to subscribe to. Any track the target announces works —
    /// the probe only needs a SUBSCRIBE_OK, not the track's payload.
    #[arg(long)]
    pub track: String,

    /// Client-side UDP bind for the QUIC/WebTransport connection.
    #[arg(long, default_value = "[::]:0")]
    pub bind: std::net::SocketAddr,

    /// Seconds to wait between probe cycles.
    #[arg(long = "interval-seconds", default_value_t = 30)]
    pub interval_seconds: u64,

    /// Seconds to wait for a single probe (connect + SUBSCRIBE_OK) before
    /// treating it as a failure and moving on to the next cycle.
    #[arg(long = "probe-timeout-seconds", default_value_t = 10)]
    pub probe_timeout_seconds: u64,

    /// Run exactly one probe cycle and exit with its result instead of
    /// looping. Used for CI smoke tests and manual one-shot checks.
    #[arg(long)]
    pub once: bool,

    /// Address to expose Prometheus metrics on (e.g., "0.0.0.0:9091").
    /// Requires the `metrics-prometheus` feature to be enabled. When set,
    /// serves `moq_canary_probe_total` / `moq_canary_history_window_groups`
    /// at http://<addr>/metrics.
    #[arg(long)]
    pub metrics_addr: Option<std::net::SocketAddr>,

    /// TLS configuration shared with moq-pub-mmtp / moq-sub-raw /
    /// moq-relay-ietf: `--tls-cert`, `--tls-key`, `--tls-root`,
    /// `--tls-disable-verify`.
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn required_args() -> [&'static str; 5] {
        [
            "moq-canary",
            "moqt://localhost:4443",
            "--name",
            "example",
            "--track",
        ]
    }

    #[test]
    fn defaults_are_a_bounded_periodic_loop() {
        let args = Args::try_parse_from(required_args().into_iter().chain(["catalog"])).unwrap();
        assert_eq!(args.interval_seconds, 30);
        assert_eq!(args.probe_timeout_seconds, 10);
        assert!(!args.once);
        assert!(args.metrics_addr.is_none());
    }

    #[test]
    fn once_and_metrics_addr_are_opt_in() {
        let args = Args::try_parse_from(required_args().into_iter().chain([
            "catalog",
            "--once",
            "--metrics-addr",
            "127.0.0.1:9091",
        ]))
        .unwrap();
        assert!(args.once);
        assert_eq!(args.metrics_addr, Some("127.0.0.1:9091".parse().unwrap()));
    }
}
