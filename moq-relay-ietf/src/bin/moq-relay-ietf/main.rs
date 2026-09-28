// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

mod api_coordinator;
mod file_coordinator;

use std::sync::Arc;
use std::{net, path::PathBuf};

use anyhow::Context;
use clap::{Parser, ValueEnum};
use moq_transport::profile::WireProfile;
use url::Url;

use api_coordinator::{ApiCoordinator, ApiCoordinatorConfig};
use file_coordinator::FileCoordinator;
use moq_relay_ietf::{
    Coordinator, Draft19Drain, Draft19DrainEnd, Relay, RelayConfig, SessionConfig, Web, WebConfig,
};
use moq_transport::profile::draft19::SessionErrorCode;

/// SIGINT, and SIGTERM where the platform has it.
///
/// Registering replaces the platform's default terminate action for the rest
/// of the process, not just until the first signal. Whoever holds these must
/// act on every signal, or the process can only be stopped with SIGKILL.
struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl ShutdownSignals {
    fn register() -> std::io::Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            Ok(Self {
                interrupt: signal(SignalKind::interrupt())?,
                terminate: signal(SignalKind::terminate())?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    /// Wait for the next shutdown signal.
    ///
    /// tokio coalesces a signal kind: several deliveries before this is polled
    /// yield one notification, so a very fast double Ctrl-C can count as one.
    /// Supervisors send one SIGTERM and then wait, which is unaffected.
    async fn recv(&mut self) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            // `None` means the stream closed, not that a signal arrived.
            // Reporting it as a signal would stop the relay with exit 0 and
            // look like a clean shutdown.
            tokio::select! {
                Some(()) = self.interrupt.recv() => Ok(()),
                Some(()) = self.terminate.recv() => Ok(()),
                else => Err(std::io::Error::other("shutdown signal streams closed")),
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum WireProfileArg {
    /// Accept the Blockcast profile requiring bounded subgroup history.
    Blockcast01,
    /// Accept draft-19 (ALPN moqt-19). CONTROL PLANE ONLY: the relay performs
    /// the draft-19 SETUP exchange and GOAWAY handling, and refuses every other
    /// draft-19 control message. Media routing over draft-19 is not implemented.
    Draft19,
}

impl From<WireProfileArg> for WireProfile {
    fn from(profile: WireProfileArg) -> Self {
        match profile {
            WireProfileArg::Blockcast01 => Self::Blockcast01,
            WireProfileArg::Draft19 => Self::Draft19,
        }
    }
}

#[derive(Parser, Clone)]
pub struct Cli {
    /// Listen on this address
    #[arg(long, default_value = "[::]:443")]
    pub bind: net::SocketAddr,

    /// Accept a non-default MoQT wire profile in addition to the default moqt-16 profile.
    #[arg(long, value_enum)]
    wire_profile: Option<WireProfileArg>,

    /// The TLS configuration.
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,

    /// Directory to write qlog files (one per connection)
    #[arg(long)]
    pub qlog_dir: Option<PathBuf>,

    /// Directory to write mlog files (one per connection)
    #[arg(long)]
    pub mlog_dir: Option<PathBuf>,

    /// Maximum request ID plus one advertised in MoQT setup.
    #[arg(long, default_value_t = 100)]
    pub max_request_id: u64,

    /// Retain the Objects of this many most recently arrived groups of every
    /// track the relay receives, and answer a standalone FETCH from them when
    /// every Object of its range is retained. Memory is bounded by
    /// --fetch-retention-track-bytes and --fetch-retention-bytes, which are
    /// required with it. Without this flag nothing is retained and every FETCH
    /// is forwarded upstream.
    #[arg(
        long,
        requires = "fetch_retention_track_bytes",
        requires = "fetch_retention_bytes"
    )]
    pub fetch_retention_groups: Option<std::num::NonZeroU64>,

    /// Most bytes retained for one track, counting each Object's payload,
    /// extension headers and per-Object overhead. Older groups are dropped to
    /// make room; an Object that does not fit even then is not retained.
    #[arg(long, requires = "fetch_retention_groups")]
    pub fetch_retention_track_bytes: Option<std::num::NonZeroUsize>,

    /// Most bytes retained across all tracks, counted as for
    /// --fetch-retention-track-bytes.
    #[arg(long, requires = "fetch_retention_groups")]
    pub fetch_retention_bytes: Option<std::num::NonZeroUsize>,

    /// Forward all PUBLISH_NAMESPACE messages to the provided server for auth/routing.
    /// If not provided, the relay accepts every unique namespace publish.
    #[arg(long)]
    pub announce: Option<Url>,

    /// The URL of the moq-api server in order to run a cluster.
    /// Must be used in conjunction with --node to advertise the origin
    #[arg(long)]
    pub api: Option<Url>,

    /// The hostname that we advertise to other origins.
    /// The provided certificate must be valid for this address.
    #[arg(long)]
    pub node: Option<Url>,

    /// Enable development mode.
    /// This hosts a HTTPS web server via TCP to serve the fingerprint of the certificate.
    #[arg(long)]
    pub dev: bool,

    /// Serve qlog files over HTTPS at /qlog/:cid
    /// Requires --dev to enable the web server. Only serves files by exact CID - no index.
    #[arg(long)]
    pub qlog_serve: bool,

    /// Serve mlog files over HTTPS at /mlog/:cid
    /// Requires --dev to enable the web server. Only serves files by exact CID - no index.
    #[arg(long)]
    pub mlog_serve: bool,

    /// Path to the shared coordinator file for multi-relay coordination.
    /// Multiple relay instances can share namespace/track registration via this file.
    /// User doesn't have to explicitly create and populate anything. This path will be
    /// used by file coordinator to store namespace/track registration information.
    /// User need to make sure if multiple relay's are being used all of them have same path
    /// to this file.
    #[arg(long, default_value = "/tmp/moq-coordinator.json")]
    pub coordinator_file: PathBuf,

    /// URL of the moq-api server for coordination (e.g., "http://localhost:8080").
    /// When specified, uses moq-api HTTP server instead of file-based coordination.
    /// This is useful when running a cluster of relays with a centralized API server.
    #[arg(long)]
    pub api_url: Option<Url>,

    /// TTL in seconds for namespace registrations in the API.
    /// Only used when --api-url is specified.
    #[arg(long, default_value = "600")]
    pub api_ttl: u64,

    /// Address to expose Prometheus metrics on (e.g., "127.0.0.1:9090").
    /// Requires the `metrics-prometheus` feature to be enabled.
    /// When set, serves metrics at http://<addr>/metrics
    #[arg(long)]
    pub metrics_addr: Option<net::SocketAddr>,

    /// Redirect draining draft-19 sessions to this URL.
    ///
    /// Setting it enables the draft-19 graceful drain: on SIGINT/SIGTERM the
    /// relay sends every draft-19 session a GOAWAY carrying this New Session
    /// URI, and redirects new draft-19 sessions the same way instead of
    /// serving them. Draft-16 sessions are unaffected.
    ///
    /// The process keeps running until every draft-19 session has closed,
    /// either by its peer or with GOAWAY_TIMEOUT once the Timeout counted from
    /// that session's GOAWAY elapses, then exits cleanly. Draft-19 sessions
    /// that arrive during the drain are redirected and waited for too, but only
    /// until the Timeout has run out from the last GOAWAY sent to a session
    /// that was live when the drain began: a steady stream of arrivals cannot
    /// hold the process up past that, and any still open are closed at exit.
    /// A second SIGINT or SIGTERM stops it early. With a zero Timeout, sessions
    /// close only when their peers do, and the drain has no such bound.
    #[arg(long)]
    pub draft19_goaway_uri: Option<Url>,

    /// Milliseconds advertised in the draft-19 GOAWAY Timeout.
    ///
    /// Once it elapses with the peer still connected, the relay closes that
    /// session with GOAWAY_TIMEOUT (0x10). Zero advertises no deadline and
    /// waits for the peer instead. Only used with --draft19-goaway-uri.
    #[arg(long, default_value_t = 30_000)]
    pub draft19_goaway_timeout_ms: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing with env filter (respects RUST_LOG environment variable)
    // Default to info level, but suppress quinn's verbose output
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,quinn=warn")),
        )
        .init();

    let cli = Cli::parse();

    // Initialize Prometheus metrics exporter if --metrics-addr is provided
    #[cfg(feature = "metrics-prometheus")]
    if let Some(metrics_addr) = cli.metrics_addr {
        use metrics_exporter_prometheus::PrometheusBuilder;

        // Configure histogram buckets for subscribe latency (1ms to 10s)
        let subscribe_latency_buckets = vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0,
        ];

        PrometheusBuilder::new()
            .with_http_listener(metrics_addr)
            .set_buckets_for_metric(
                metrics_exporter_prometheus::Matcher::Full(
                    "moq_relay_subscribe_latency_seconds".to_string(),
                ),
                &subscribe_latency_buckets,
            )?
            .install()
            .expect("failed to install Prometheus metrics exporter");

        // Register metric descriptions (shows as # HELP in Prometheus output)
        moq_relay_ietf::metrics::describe_metrics();

        tracing::info!(
            "metrics exporter listening on http://{}/metrics",
            metrics_addr
        );
    }

    #[cfg(not(feature = "metrics-prometheus"))]
    if cli.metrics_addr.is_some() {
        tracing::warn!(
            "--metrics-addr was provided but the metrics-prometheus feature is not enabled. \
             Rebuild with --features metrics-prometheus to enable the Prometheus exporter."
        );
    }

    let tls = cli.tls.load()?;

    if tls.server.is_none() {
        anyhow::bail!("missing TLS certificates");
    }

    // Determine qlog directory for both relay and web server
    let qlog_dir_for_relay = cli.qlog_dir.clone();
    let qlog_dir_for_web = if cli.qlog_serve {
        cli.qlog_dir.clone()
    } else {
        None
    };

    // Determine mlog directory for both relay and web server
    let mlog_dir_for_relay = cli.mlog_dir.clone();
    let mlog_dir_for_web = if cli.mlog_serve {
        cli.mlog_dir.clone()
    } else {
        None
    };

    let media_endpoint = moq_native_ietf::quic::Endpoint::new(
        moq_native_ietf::quic::Config::new(cli.bind, qlog_dir_for_relay.clone(), tls.clone())?
            .with_wire_profiles(enabled_wire_profiles(cli.wire_profile)),
    )?;
    let media_closer = media_endpoint.closer();

    // Build the relay URL from the node or bind address
    let relay_url = cli
        .node
        .clone()
        .unwrap_or_else(|| Url::parse(&format!("https://{}", cli.bind)).unwrap());

    // Create the coordinator based on CLI arguments
    // Priority: api-url > file coordinator
    let coordinator: Arc<dyn Coordinator> = if let Some(api_url) = &cli.api_url {
        let config = ApiCoordinatorConfig::new(api_url.clone(), relay_url).with_ttl(cli.api_ttl);
        let api_coordinator = ApiCoordinator::new(config);
        tracing::info!("using API coordinator: {}", api_url);
        Arc::new(api_coordinator)
    } else {
        tracing::info!("using file coordinator: {}", cli.coordinator_file.display());
        Arc::new(FileCoordinator::new(&cli.coordinator_file, relay_url))
    };

    // Create a QUIC server for media.
    let draft19_drain = cli
        .draft19_goaway_uri
        .clone()
        .map(|uri| Draft19Drain::new(uri, cli.draft19_goaway_timeout_ms));

    let relay = Relay::new(RelayConfig {
        tls: tls.clone(),
        bind: None,
        endpoints: vec![media_endpoint],
        qlog_dir: qlog_dir_for_relay,
        mlog_dir: mlog_dir_for_relay,
        node: cli.node,
        announce: cli.announce,
        coordinator,
        session: SessionConfig {
            max_request_id: cli.max_request_id,
            // The relay forwards whatever catalog its upstream publisher
            // delivers; it declares no capabilities of its own (BLO-22575).
            ..SessionConfig::default()
        },
        // No connection tagger: the default binary treats every inbound
        // connection as a public client. Embedders that run relay-to-relay
        // meshes supply a tagger to mark internal peers.
        connection_tagger: None,
        draft19_drain: draft19_drain.clone(),
    })?;

    // Start the draft-19 drain on the first shutdown signal and stop once it is
    // finished, or on the second signal, when a drain is configured. Without
    // one no handler is installed and the platform's default terminate action
    // still applies.
    let drain_then_stop = match draft19_drain {
        Some(drain) => {
            let mut signals =
                ShutdownSignals::register().context("failed to watch for shutdown signals")?;
            tracing::info!(
                new_session_uri = %drain.new_session_uri,
                timeout_ms = drain.timeout_ms,
                "draft-19 graceful drain armed: SIGINT/SIGTERM sends GOAWAY"
            );
            Some(async move {
                signals.recv().await?;
                tracing::info!("shutdown signal received: draining draft-19 sessions");
                drain.signal.cancel();
                // Wait for the sessions themselves, not a timer started here.
                // Each session's Timeout runs from its own GOAWAY send, which
                // is later than this signal, so such a timer would stop the
                // process before any session could be closed with
                // GOAWAY_TIMEOUT. The drain's own ceiling waits those out.
                tokio::select! {
                    result = signals.recv() => {
                        result?;
                        tracing::info!("second shutdown signal received: stopping");
                    }
                    end = drain.finished() => match end {
                        Draft19DrainEnd::Drained => tracing::info!(
                            "draft-19 drain complete: every session has closed, stopping"
                        ),
                        Draft19DrainEnd::Ceiling => tracing::info!(
                            timeout_ms = drain.timeout_ms,
                            "draft-19 drain ceiling reached: every session live at the signal \
                             has closed and the Timeout has run out since its last GOAWAY, stopping"
                        ),
                    },
                }
                Ok::<_, std::io::Error>(())
            })
        }
        None => None,
    };

    if cli.dev {
        // Create a web server too.
        // Currently this only contains the certificate fingerprint (for development only).
        let web = Web::new(WebConfig {
            bind: cli.bind,
            tls,
            qlog_dir: qlog_dir_for_web,
            mlog_dir: mlog_dir_for_web,
        });

        tokio::spawn(async move {
            web.run().await.expect("failed to run web server");
        });
    }

    let relay = match (
        cli.fetch_retention_groups,
        cli.fetch_retention_track_bytes,
        cli.fetch_retention_bytes,
    ) {
        (Some(groups), Some(track_bytes), Some(total_bytes)) => relay.with_fetch_retention(
            moq_relay_ietf::FetchRetention::new(groups, track_bytes, total_bytes),
        ),
        (None, None, None) => relay,
        _ => anyhow::bail!(
            "--fetch-retention-groups, --fetch-retention-track-bytes and \
             --fetch-retention-bytes must be set together"
        ),
    };

    match drain_then_stop {
        Some(drain_then_stop) => {
            let stopped = tokio::select! {
                result = relay.run() => return result,
                result = drain_then_stop => result.context("failed to watch for shutdown signals"),
            };
            // The relay and its sessions are dropped by now, but every close
            // they made is only queued. Returning would end the process before
            // quinn sends them, so a peer closed with GOAWAY_TIMEOUT would see
            // no close at all and wait out its idle timeout instead. Close
            // whatever is left with NO_ERROR, the code quinn itself uses when a
            // connection's last handle drops, and wait for every close to be
            // sent.
            media_closer
                .close_and_wait_idle(SessionErrorCode::NoError as u32, b"relay shutting down")
                .await;
            stopped
        }
        None => relay.run().await,
    }
}

fn enabled_wire_profiles(wire_profile: Option<WireProfileArg>) -> Vec<WireProfile> {
    let mut profiles = vec![WireProfile::Draft16];
    if let Some(profile) = wire_profile {
        profiles.push(profile.into());
    }
    profiles
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_request_id_flag_overrides_default() {
        let cli = Cli::try_parse_from(["moq-relay-ietf", "--max-request-id", "7"]).unwrap();

        assert_eq!(cli.max_request_id, 7);
    }

    #[test]
    fn blockcast_profile_is_additive_and_opt_in() {
        let default = Cli::try_parse_from(["moq-relay-ietf"]).unwrap();
        assert_eq!(
            enabled_wire_profiles(default.wire_profile),
            [WireProfile::Draft16]
        );

        let enabled =
            Cli::try_parse_from(["moq-relay-ietf", "--wire-profile", "blockcast01"]).unwrap();
        assert_eq!(
            enabled_wire_profiles(enabled.wire_profile),
            [WireProfile::Draft16, WireProfile::Blockcast01]
        );
    }

    #[test]
    fn draft19_profile_is_additive_and_opt_in() {
        let enabled = Cli::try_parse_from(["moq-relay-ietf", "--wire-profile", "draft19"]).unwrap();
        assert_eq!(
            enabled_wire_profiles(enabled.wire_profile),
            [WireProfile::Draft16, WireProfile::Draft19]
        );
        assert_eq!(WireProfile::Draft19.name(), "moqt-19");
    }

    #[test]
    fn fetch_retention_is_opt_in_and_rejects_zero() {
        let default = Cli::try_parse_from(["moq-relay-ietf"]).unwrap();
        assert_eq!(default.fetch_retention_groups, None);

        let retention = |groups: &str, track: &str, total: &str| {
            Cli::try_parse_from([
                "moq-relay-ietf",
                "--fetch-retention-groups",
                groups,
                "--fetch-retention-track-bytes",
                track,
                "--fetch-retention-bytes",
                total,
            ])
        };
        let set = retention("4", "1000", "5000").unwrap();
        assert_eq!(
            set.fetch_retention_groups.map(|groups| groups.get()),
            Some(4)
        );
        assert_eq!(
            set.fetch_retention_track_bytes.map(|bytes| bytes.get()),
            Some(1000)
        );
        assert_eq!(
            set.fetch_retention_bytes.map(|bytes| bytes.get()),
            Some(5000)
        );

        assert!(retention("0", "1000", "5000").is_err());
        assert!(retention("4", "0", "5000").is_err());
        assert!(retention("4", "1000", "0").is_err());
    }

    #[test]
    fn fetch_retention_requires_both_byte_budgets() {
        assert!(Cli::try_parse_from(["moq-relay-ietf", "--fetch-retention-groups", "4"]).is_err());
        assert!(Cli::try_parse_from([
            "moq-relay-ietf",
            "--fetch-retention-groups",
            "4",
            "--fetch-retention-track-bytes",
            "1000",
        ])
        .is_err());
        assert!(
            Cli::try_parse_from(["moq-relay-ietf", "--fetch-retention-bytes", "5000"]).is_err()
        );
    }
}
