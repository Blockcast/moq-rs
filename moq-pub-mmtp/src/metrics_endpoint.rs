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
///
/// Deliberately NOT under `#[cfg(feature = "metrics-prometheus")]`. It was,
/// to stop a feature-off build seeing it as dead code under `-D warnings`
/// (#71), but BLO-26174 moved the call in `spawn_if_enabled` out of the
/// gated block so descriptions register whenever an exporter address is
/// configured. That made the function live under every feature combination —
/// so the cfg stopped suppressing a warning and started breaking the build:
/// `cargo test -p moq-pub-mmtp` failed to compile at 54d855f with
/// `cannot find function describe_metrics`, because the default (feature-off)
/// test profile reached the call with no definition in scope. Release builds
/// stayed green, which is why CI missed it.
///
/// Safe unconditionally: `metrics` is an unconditional dependency and only
/// `metrics-exporter-prometheus` is optional, so `describe_counter!` compiles
/// either way and is a no-op against the facade when no recorder is installed.
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
///
/// Returns the address the exporter ended up listening on, so the caller can
/// reconcile this env-gated activation against the CLI `--metrics-addr` flag
/// (see [`install_flag_exporter_if_needed`]) — the process-global recorder
/// can only be installed once, and this path runs first (before
/// `Args::parse()`), so `Some` here means the flag must not attempt a second
/// install (BLO-26174).
pub fn spawn_if_enabled() -> Option<std::net::SocketAddr> {
    let Ok(addr) = std::env::var("MOQ_PUB_METRICS_ADDR") else {
        return None;
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
                        Some(socket_addr)
                    }
                    Err(error) => {
                        // Observability plumbing must not take down a live
                        // publisher: log and keep running without the
                        // exporter rather than failing the process.
                        tracing::warn!(%error, "failed to install Prometheus metrics exporter; continuing without it");
                        None
                    }
                }
            }
            Err(error) => {
                tracing::warn!(
                    %addr,
                    %error,
                    "MOQ_PUB_METRICS_ADDR is not a valid socket address; metrics exporter disabled"
                );
                None
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
        None
    }
}

/// Reconcile the CLI `--metrics-addr` flag against whatever
/// [`spawn_if_enabled`] already did with `MOQ_PUB_METRICS_ADDR`. The
/// process-global Prometheus recorder can be installed exactly once
/// (BLO-26174): if the env-gated path already claimed it (`env_addr` is
/// `Some`), the flag is logged as ignored rather than attempted — this is
/// the ONLY path that runs, so a second `install()` call (and the panic it
/// used to cause via `.expect()`) never happens. If the env-gated path did
/// not install anything, the flag installs on its own and any failure is
/// logged, never `.expect()`ed/`.unwrap()`ed.
///
/// No-op when `flag_addr` is `None`, and (like `spawn_if_enabled`) a no-op
/// with a warning when the `metrics-prometheus` feature is not compiled in.
pub fn install_flag_exporter_if_needed(
    flag_addr: Option<std::net::SocketAddr>,
    env_addr: Option<std::net::SocketAddr>,
) {
    let Some(flag_addr) = flag_addr else {
        return;
    };

    #[cfg(feature = "metrics-prometheus")]
    {
        if let Some(env_addr) = env_addr {
            tracing::warn!(
                flag_addr = %flag_addr,
                env_addr = %env_addr,
                "--metrics-addr is ignored: MOQ_PUB_METRICS_ADDR already installed the \
                 Prometheus exporter on {env_addr}"
            );
            return;
        }

        match metrics_exporter_prometheus::PrometheusBuilder::new()
            .with_http_listener(flag_addr)
            .install()
        {
            Ok(()) => {
                describe_metrics();
                tracing::info!(
                    addr = %flag_addr,
                    "metrics exporter listening on http://{flag_addr}/metrics"
                );
            }
            Err(error) => {
                // Same rule as spawn_if_enabled: never take the process down
                // over exporter install failure.
                tracing::warn!(%error, "failed to install Prometheus metrics exporter; continuing without it");
            }
        }
    }

    #[cfg(not(feature = "metrics-prometheus"))]
    {
        let _ = env_addr;
        tracing::warn!(
            addr = %flag_addr,
            "--metrics-addr was provided but the metrics-prometheus feature is not enabled. \
             Rebuild with --features metrics-prometheus to enable the Prometheus exporter."
        );
    }
}

#[cfg(all(test, feature = "metrics-prometheus"))]
mod tests {
    use super::*;

    /// BLO-26174 regression: both activations set at once must yield a
    /// single listener (the env path, since it runs first) plus a warning
    /// about the ignored flag — no panic.
    #[tokio::test]
    async fn ignores_flag_when_env_already_installed() {
        install_flag_exporter_if_needed(
            Some("127.0.0.1:0".parse().unwrap()),
            Some("127.0.0.1:0".parse().unwrap()),
        );
    }

    /// BLO-26174 regression: the historical panic was `install()` failing
    /// because the process-global recorder was already claimed. Drive that
    /// exact failure for real (two live install() calls, same process) and
    /// assert the second one is handled rather than `.expect()`ed.
    #[tokio::test]
    async fn flag_path_survives_an_already_claimed_recorder() {
        install_flag_exporter_if_needed(Some("127.0.0.1:0".parse().unwrap()), None);
        // The recorder is now already installed (by the call above, or by a
        // prior test in this binary). This call's own install() attempt
        // must fail gracefully, not panic.
        install_flag_exporter_if_needed(Some("127.0.0.1:0".parse().unwrap()), None);
    }

    #[tokio::test]
    async fn none_flag_addr_is_a_no_op() {
        install_flag_exporter_if_needed(None, Some("127.0.0.1:0".parse().unwrap()));
        install_flag_exporter_if_needed(None, None);
    }
}
