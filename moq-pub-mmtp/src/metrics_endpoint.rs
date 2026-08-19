// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Prometheus metrics endpoint. Mirrors moq-relay-ietf's split (see
// moq-relay-ietf/src/metrics.rs + src/bin/moq-relay-ietf/main.rs): the
// `metrics` crate facade is always compiled in (near-zero overhead with no
// recorder installed, same as the `log` crate with no logger configured);
// the optional `metrics-prometheus` feature adds the HTTP exporter.
//
// Activation is env-var-only — MOQ_PUB_METRICS_ADDR, e.g. "0.0.0.0:9091" —
// the same runtime-gating pattern MOQ_PUB_PROFILE_ADDR uses in profiling.rs,
// so a binary built with the feature compiled in stays inert until an
// operator opts in, and there is no separate clap flag to keep in sync with
// the env var.
//
// # Available Metrics
//
// | Name | Description |
// |------|-------------|
// | `moq_pub_mmtp_dropped_datagrams_total` | Datagrams dropped by the publisher-side ring buffer (ring-superseded by a lagging subscriber, or over-MTU payloads skipped) — see moq-transport/src/session/subscribed.rs |

/// Register metric descriptions (Prometheus `# HELP` text).
pub fn describe_metrics() {
    metrics::describe_counter!(
        "moq_pub_mmtp_dropped_datagrams_total",
        "Datagrams dropped by the publisher-side ring buffer: ring-superseded \
         (lagging subscriber) or over-MTU payloads skipped"
    );
}

/// Install the Prometheus exporter iff `MOQ_PUB_METRICS_ADDR` is set. No-op
/// otherwise, and a no-op (with a warning) when the `metrics-prometheus`
/// feature was not compiled in.
pub fn spawn_if_enabled() {
    let Ok(addr) = std::env::var("MOQ_PUB_METRICS_ADDR") else {
        return;
    };

    // Registered whenever an exporter address is configured, independent of
    // whether the metrics-prometheus feature was compiled in: describe_counter!
    // is a no-op against the facade when no recorder is installed, and calling
    // it here (rather than only from the feature-gated install-success arm
    // below) keeps this function reachable under every feature combination.
    describe_metrics();

    #[cfg(feature = "metrics-prometheus")]
    {
        let parsed: Result<std::net::SocketAddr, _> = addr.parse();
        match parsed {
            Ok(socket_addr) => {
                match metrics_exporter_prometheus::PrometheusBuilder::new()
                    .with_http_listener(socket_addr)
                    .install()
                {
                    Ok(()) => {
                        tracing::info!(
                            addr = %socket_addr,
                            "metrics exporter listening on http://{socket_addr}/metrics"
                        );
                    }
                    Err(error) => {
                        // Observability plumbing must not take down a live
                        // publisher: log and keep running without the
                        // exporter rather than failing the process.
                        tracing::warn!(%error, "failed to install Prometheus metrics exporter; continuing without it");
                    }
                }
            }
            Err(error) => {
                tracing::warn!(
                    %addr,
                    %error,
                    "MOQ_PUB_METRICS_ADDR is not a valid socket address; metrics exporter disabled"
                );
            }
        }
    }

    #[cfg(not(feature = "metrics-prometheus"))]
    {
        tracing::warn!(
            %addr,
            "MOQ_PUB_METRICS_ADDR was set but the metrics-prometheus feature is not enabled. \
             Rebuild with --features metrics-prometheus to enable the Prometheus exporter."
        );
    }
}
