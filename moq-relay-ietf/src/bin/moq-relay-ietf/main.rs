// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

mod api_coordinator;
mod file_coordinator;

use std::sync::Arc;
use std::{net, path::PathBuf};

use anyhow::Context;
use clap::Parser;
use url::Url;

use api_coordinator::{ApiCoordinator, ApiCoordinatorConfig};
use file_coordinator::FileCoordinator;
use moq_relay_ietf::{
    admin, Coordinator, Relay, RelayConfig, SessionConfig, Transports, Web, WebConfig,
};
use std::time::Duration;

#[derive(Parser, Clone)]
pub struct Cli {
    /// Listen on this address
    #[arg(long, default_value = "[::]:443")]
    pub bind: net::SocketAddr,

    /// The TLS configuration.
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,

    /// Directory to write qlog files (one per connection)
    #[arg(long)]
    pub qlog_dir: Option<PathBuf>,

    /// Directory to write mlog files (one per connection)
    #[arg(long)]
    pub mlog_dir: Option<PathBuf>,

    /// Accepted for compatibility; ignored.
    ///
    /// Draft-18 removed MAX_REQUEST_ID, so there is no request budget to
    /// advertise in SETUP. The flag remains so existing invocations keep
    /// working.
    #[arg(long, default_value_t = 100)]
    pub max_request_id: u64,

    /// Seconds to keep a cached track with no subscribers before releasing its
    /// upstream subscription. 0 disables eviction, holding upstream
    /// subscriptions for the lifetime of the upstream session.
    #[arg(long, default_value_t = 30)]
    pub cache_idle_timeout: u64,

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

    /// Serve the plain-HTTP operator surface on this TCP port: /certificate.sha256,
    /// /livez, /readyz, POST /_drain and /_astats.
    ///
    /// The drain endpoint answers 503 unless BLOCKCAST_RELAY_DRAIN_TOKEN is set;
    /// callers send that secret in the X-Blockcast-Drain-Token header.
    #[arg(long, env = "RELAY_HTTP_PORT")]
    pub http_port: Option<u16>,

    /// Traffic Router base URL, asked for the GOAWAY target when a drain request
    /// does not name one.
    #[arg(long, env = "BLOCKCAST_RELAY_TR_URL")]
    pub tr_url: Option<Url>,

    /// Comma-separated transports to accept: `ietf-wt` (WebTransport),
    /// `ietf-raw` (raw QUIC), or `ietf` for both.
    #[arg(long, default_value = "ietf-wt,ietf-raw", value_parser = parse_protocols)]
    pub protocols: Transports,
}

fn parse_protocols(value: &str) -> Result<Transports, String> {
    let mut transports = Transports {
        web_transport: false,
        raw_quic: false,
    };
    for token in value.split(',').map(str::trim).filter(|t| !t.is_empty()) {
        match token {
            "ietf-wt" => transports.web_transport = true,
            "ietf-raw" => transports.raw_quic = true,
            "ietf" => transports = Transports::default(),
            other => return Err(format!("unsupported protocol family {other:?}")),
        }
    }
    if !transports.web_transport && !transports.raw_quic {
        return Err("no protocol family enabled".to_string());
    }
    Ok(transports)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing with env filter (respects RUST_LOG environment variable)
    // Default to info level, but suppress quinn's verbose output
    //
    // Logs go to stderr, per convention and to keep stdout clean.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
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
    let relay = Relay::new_with_cache_idle_timeout(
        RelayConfig {
            tls: tls.clone(),
            bind: Some(cli.bind),
            endpoints: vec![],
            qlog_dir: qlog_dir_for_relay,
            mlog_dir: mlog_dir_for_relay,
            node: cli.node,
            announce: cli.announce,
            coordinator,
            session: SessionConfig {
                max_request_id: cli.max_request_id,
            },
            // No connection tagger: the default binary treats every inbound
            // connection as a public client. Embedders that run relay-to-relay
            // meshes supply a tagger to mark internal peers.
            connection_tagger: None,
        },
        Duration::from_secs(cli.cache_idle_timeout),
    )?
    .with_transports(cli.protocols);

    if let Some(port) = cli.http_port {
        let fingerprint = tls
            .fingerprints
            .first()
            .context("missing certificate fingerprint")?
            .clone();
        let router = admin::router(admin::AdminConfig {
            fingerprint,
            drain: relay.drain(),
            drain_token: std::env::var("BLOCKCAST_RELAY_DRAIN_TOKEN").ok(),
            tr_url: cli.tr_url.clone(),
        });
        let listener = tokio::net::TcpListener::bind((net::Ipv6Addr::UNSPECIFIED, port))
            .await
            .with_context(|| format!("failed to bind --http-port {port}"))?;
        tracing::info!("operator HTTP listening on {}", listener.local_addr()?);
        tokio::spawn(async move {
            if let Err(err) = axum::serve(listener, router).await {
                tracing::error!(error = %err, "operator HTTP server failed");
            }
        });
    }

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

    relay.run().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocols_select_transports() {
        let parse = |v: &str| Cli::try_parse_from(["moq-relay-ietf", "--protocols", v]);
        assert_eq!(
            parse("ietf-raw").unwrap().protocols,
            Transports {
                web_transport: false,
                raw_quic: true
            }
        );
        assert_eq!(
            parse("ietf-wt").unwrap().protocols,
            Transports {
                web_transport: true,
                raw_quic: false
            }
        );
        assert_eq!(parse("ietf").unwrap().protocols, Transports::default());
        assert_eq!(
            Cli::try_parse_from(["moq-relay-ietf"]).unwrap().protocols,
            Transports::default()
        );
        // moq-lite is a different protocol; refuse rather than silently ignore it.
        assert!(parse("ietf-wt,ietf-raw,moq-lite").is_err());
        assert!(parse("").is_err());
    }

    #[test]
    fn max_request_id_flag_overrides_default() {
        let cli = Cli::try_parse_from(["moq-relay-ietf", "--max-request-id", "7"]).unwrap();

        assert_eq!(cli.max_request_id, 7);
    }
}
