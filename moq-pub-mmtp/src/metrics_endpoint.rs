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
// | `moq_pub_mmtp_dropped_datagrams_total{reason}` | Datagrams dropped by the publisher-side ring buffer. `reason="ring_superseded"` (lagging subscriber) or `reason="over_mtu"` (above the live QUIC datagram limit) — distinct causes with distinct remedies, so they are not summed. See moq-transport/src/session/subscribed.rs |
// | `moq_pub_mmtp_sent_datagrams_total` | Datagrams successfully sent. The denominator: loss fraction is `rate(dropped) / (rate(dropped) + rate(sent))` |
//
// All three series are materialized at zero on startup, so an absent series
// means the exporter is down — never "no loss" (BLO-41602).

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
/// test profile reached the call with no definition in scope. CI did not catch
/// it because the call was hoisted out of the gated block in the squash-merge
/// commit itself: #77's tested head (9582b153) had the call inside the block
/// and its `build` job was green, and `pr.yml` runs only on `pull_request`, so
/// no job ever built the merged tree — 54d855f has zero check runs.
///
/// Safe unconditionally: `metrics` is an unconditional dependency and only
/// `metrics-exporter-prometheus` is optional, so `describe_counter!` compiles
/// either way and is a no-op against the facade when no recorder is installed.
pub fn describe_metrics() {
    use moq_transport::session::{
        DROPPED_DATAGRAMS_METRIC, DROP_REASON_OVER_MTU, DROP_REASON_RING_SUPERSEDED,
        SENT_DATAGRAMS_METRIC,
    };

    metrics::describe_counter!(
        DROPPED_DATAGRAMS_METRIC,
        "Datagrams dropped by the publisher-side ring buffer, by `reason`: \
         ring_superseded (lagging subscriber) or over_mtu (payload above the \
         live QUIC datagram limit)"
    );
    metrics::describe_counter!(
        SENT_DATAGRAMS_METRIC,
        "Datagrams successfully sent to the subscriber. Denominator for the \
         drop counters: a drop rate alone cannot distinguish a lossy path from \
         a busy one"
    );

    // Materialize all three series at zero (BLO-41602). Without this a process
    // that has never dropped a datagram exports no drop series at all, and
    // PromQL cannot tell "loss-free" from "publishing nothing" — measured
    // 2026-10-08, staging-blockcastd scraped up=1 with no drop series while
    // production carried 132M, and neither reading was interpretable.
    // `increment(0)` registers without perturbing the value.
    metrics::counter!(DROPPED_DATAGRAMS_METRIC, "reason" => DROP_REASON_RING_SUPERSEDED)
        .increment(0);
    metrics::counter!(DROPPED_DATAGRAMS_METRIC, "reason" => DROP_REASON_OVER_MTU).increment(0);
    metrics::counter!(SENT_DATAGRAMS_METRIC).increment(0);
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

    // Called whenever an exporter address is configured, independent of
    // whether the metrics-prometheus feature was compiled in, so this function
    // stays reachable under every feature combination. It registers NOTHING
    // here: `metrics` resolves `counter!`/`describe_counter!` against the
    // global recorder at call time, which is still the NoopRecorder until
    // `install()` below runs, and there is no replay. The registration that
    // actually lands is the second call in the install-success arm.
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
                        // Must follow install(): before it the series were
                        // discarded by the NoopRecorder, which on this (the
                        // production) path left an absent drop series meaning
                        // "loss-free or not publishing" (BLO-41602). Idempotent
                        // with the pre-install call above. Mirrors the flag
                        // path in install_flag_exporter_if_needed.
                        describe_metrics();
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

    /// BLO-41602: the whole point of materializing at zero is that a
    /// loss-free publisher still EXPORTS the drop series, so PromQL can tell
    /// "no loss" from "not running". That rests entirely on `increment(0)`
    /// producing a rendered line — assert it against the real Prometheus
    /// renderer rather than assuming it.
    #[test]
    fn describe_metrics_materializes_every_series_at_zero() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, describe_metrics);

        let rendered = handle.render();
        for expected in [
            "moq_pub_mmtp_dropped_datagrams_total{reason=\"ring_superseded\"} 0",
            "moq_pub_mmtp_dropped_datagrams_total{reason=\"over_mtu\"} 0",
            "moq_pub_mmtp_sent_datagrams_total 0",
        ] {
            assert!(
                rendered.contains(expected),
                "missing {expected:?} in rendered exposition:\n{rendered}"
            );
        }
    }

    const ENV_CHILD_MARKER: &str = "MOQ_PUB_METRICS_ENV_ACTIVATION_CHILD";
    const ENV_CHILD_PASSED: &str = "ENV_ACTIVATION_CHILD_PASSED";

    /// BLO-41602: `describe_metrics_materializes_every_series_at_zero` installs
    /// a recorder and THEN describes, the inverse of the production sequence,
    /// so it cannot see a registration that runs before `install()`. This
    /// drives the real `MOQ_PUB_METRICS_ADDR` path (`spawn_if_enabled`) and
    /// scrapes the live exporter. The global recorder is install-once per
    /// process and other tests in this binary claim it, so the scenario runs
    /// in a fresh child process (`env_activation_child`).
    #[test]
    fn env_activation_materializes_every_series_at_zero() {
        // ponytail: free port picked then released before the child binds it;
        // a collision fails loudly (install Err -> expect in the child).
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "metrics_endpoint::tests::env_activation_child",
                "--ignored",
                "--nocapture",
            ])
            .env("MOQ_PUB_METRICS_ADDR", format!("127.0.0.1:{port}"))
            .env(ENV_CHILD_MARKER, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        // The sentinel guards against a filter that matched no test, which
        // the harness reports as a successful run.
        assert!(
            out.status.success() && stdout.contains(ENV_CHILD_PASSED),
            "env-activation child failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
    }

    #[tokio::test]
    #[ignore = "child process of env_activation_materializes_every_series_at_zero"]
    async fn env_activation_child() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        if std::env::var_os(ENV_CHILD_MARKER).is_none() {
            return;
        }
        let addr = spawn_if_enabled().expect("exporter must install in a fresh process");

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();

        for expected in [
            "moq_pub_mmtp_dropped_datagrams_total{reason=\"ring_superseded\"} 0",
            "moq_pub_mmtp_dropped_datagrams_total{reason=\"over_mtu\"} 0",
            "moq_pub_mmtp_sent_datagrams_total 0",
        ] {
            assert!(
                response.contains(expected),
                "missing {expected:?} in scraped exposition:\n{response}"
            );
        }
        println!("{ENV_CHILD_PASSED}");
    }
}
