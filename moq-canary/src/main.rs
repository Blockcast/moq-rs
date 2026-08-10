// SPDX-License-Identifier: MIT OR Apache-2.0

use std::num::NonZeroU64;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use moq_native_ietf::quic;
use moq_transport::{
    coding::TrackNamespace, profile::WireProfile, serve::Tracks, session::Subscriber,
};

mod cli;

use cli::Args;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,quinn=warn")),
        )
        .init();

    let args = Args::parse();

    // Optional Prometheus metrics exporter (feature `metrics-prometheus` +
    // --metrics-addr), mirroring moq-relay-ietf / moq-pub-mmtp.
    #[cfg(feature = "metrics-prometheus")]
    if let Some(metrics_addr) = args.metrics_addr {
        use metrics_exporter_prometheus::PrometheusBuilder;
        PrometheusBuilder::new()
            .with_http_listener(metrics_addr)
            .install()
            .expect("failed to install Prometheus metrics exporter");
        tracing::info!(
            "metrics exporter listening on http://{}/metrics",
            metrics_addr
        );
    }
    #[cfg(not(feature = "metrics-prometheus"))]
    if args.metrics_addr.is_some() {
        tracing::warn!(
            "--metrics-addr was provided but the metrics-prometheus feature is not enabled. \
             Rebuild with --features metrics-prometheus to enable the Prometheus exporter."
        );
    }

    let tls = args.tls.load()?;
    let probe_timeout = Duration::from_secs(args.probe_timeout_seconds);
    let interval = Duration::from_secs(args.interval_seconds);

    loop {
        let cycle_started = tokio::time::Instant::now();
        let outcome = await_probe_task(
            probe_timeout,
            args.probe_timeout_seconds,
            tokio::spawn({
                let args = args.clone();
                let tls = tls.clone();
                async move { probe_once(&args, &tls).await }
            }),
        )
        .await;

        match &outcome {
            Ok(window) => {
                tracing::info!(
                    track = %args.track,
                    history_window_groups = window.get(),
                    "canary probe: decoded SUBSCRIBE_OK key 0x40 (moqt-blockcast-01)"
                );
                metrics::gauge!("moq_canary_history_window_groups").set(window.get() as f64);
                metrics::counter!("moq_canary_probe_total", "outcome" => "decoded").increment(1);
            }
            Err(err) => {
                tracing::warn!(track = %args.track, error = %err, "canary probe failed");
                metrics::counter!(
                    "moq_canary_probe_total",
                    "outcome" => probe_error_outcome(err),
                )
                .increment(1);
            }
        }

        if args.once {
            return outcome.map(|_| ());
        }

        let elapsed = cycle_started.elapsed();
        let sleep_for = interval.saturating_sub(elapsed);
        tokio::time::sleep(sleep_for).await;
    }
}

/// Await one owned probe task. On deadline expiry, abort and join it before a
/// later cycle begins so its session cannot survive as detached background work.
async fn await_probe_task<T>(
    probe_timeout: Duration,
    timeout_seconds: u64,
    mut probe_task: tokio::task::JoinHandle<Result<T>>,
) -> Result<T> {
    match tokio::time::timeout(probe_timeout, &mut probe_task).await {
        Ok(Ok(result)) => result,
        Ok(Err(join_err)) => Err(anyhow!("probe task terminated abnormally: {join_err}")),
        Err(_elapsed) => {
            probe_task.abort();
            match probe_task.await {
                Ok(_) => {}
                Err(join_err) if join_err.is_cancelled() => {}
                Err(join_err) => {
                    return Err(anyhow!("probe task terminated abnormally: {join_err}"))
                }
            }
            Err(anyhow!("probe did not complete within {timeout_seconds}s"))
        }
    }
}

/// Run one connect → SUBSCRIBE → decode cycle. Every cycle opens a fresh
/// connection so a 24h run is evidence of repeated negotiation, not one
/// long-lived session that happened to negotiate the profile once.
async fn probe_once(args: &Args, tls: &moq_native_ietf::tls::Config) -> Result<NonZeroU64> {
    let quic_endpoint = quic::Endpoint::new(
        quic::Config::new(args.bind, None, tls.clone())?
            .with_wire_profiles([WireProfile::Draft16, WireProfile::Blockcast01]),
    )?;

    let (session, connection_id, transport, selected_version) = quic_endpoint
        .client
        .connect_with_profile(&args.url, None, WireProfile::Blockcast01)
        .await
        .context("connect_with_profile(moqt-blockcast-01) failed")?;
    tracing::debug!(%connection_id, "canary connected");

    let (session, mut subscriber) =
        Subscriber::connect_negotiated(session, transport, selected_version)
            .await
            .context("failed to create MoQ Transport subscriber session")?;

    let namespace = TrackNamespace::from_utf8_path(&args.name);
    let (mut tracks_writer, _request, mut tracks_reader) = Tracks::new(namespace.clone()).produce();
    let track_writer = tracks_writer
        .create(args.track.as_str())
        .ok_or_else(|| anyhow!("TracksWriter::create returned None for `{}`", args.track))?;

    let probe = async {
        // Keep the handle alive until we've read the history window: `Subscribe`
        // unsubscribes on drop, so discarding it immediately (as opposed to
        // binding it) would tear down the subscription before SUBSCRIBE_OK's
        // key 0x40 can be read back off `track_reader`.
        let _subscribe = subscriber
            .subscribe_open(track_writer)
            .await
            .map_err(|err| anyhow!("SUBSCRIBE failed: {err:?}"))?;

        let track_reader = tracks_reader
            .subscribe(namespace.clone(), args.track.as_str())
            .ok_or_else(|| anyhow!("TracksReader::subscribe returned None for `{}`", args.track))?;

        track_reader.history_window().ok_or_else(|| {
            anyhow!(
                "SUBSCRIBE_OK for `{}` carried no key 0x40 (history window) under \
                 moqt-blockcast-01 — either the peer is not the profile-capable build \
                 or producer-side enforcement regressed",
                args.track
            )
        })
    };

    let result = tokio::select! {
        r = probe => r,
        r = session.run() => match r {
            Ok(()) => Err(anyhow!("session ended before SUBSCRIBE_OK arrived")),
            Err(err) => Err(anyhow!("session failed while probe was pending: {err}")),
        },
    };

    result
}

/// Coarse metric label for a probe failure. Kept small and closed-set so the
/// `moq_canary_probe_total{outcome=...}` cardinality stays bounded — the
/// full error detail goes to the log line, not the label.
fn probe_error_outcome(err: &anyhow::Error) -> &'static str {
    let msg = err.to_string();
    if msg.contains("did not complete within") {
        "timeout"
    } else if msg.contains("connect_with_profile") {
        "connect_failed"
    } else if msg.contains("SUBSCRIBE failed") {
        "subscribe_failed"
    } else if msg.contains("carried no key 0x40") {
        "missing_history_window"
    } else if msg.contains("session") {
        "session_failed"
    } else {
        "other"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(signal) = self.0.take() {
                let _ = signal.send(());
            }
        }
    }

    #[tokio::test]
    async fn probe_timeout_aborts_and_awaits_the_owned_task() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = DropSignal(Some(dropped_tx));
            let _ = started_tx.send(());
            std::future::pending::<Result<NonZeroU64>>().await
        });
        started_rx
            .await
            .expect("probe task starts before its deadline");

        let err = await_probe_task(Duration::from_millis(10), 1, task)
            .await
            .expect_err("pending probe times out");
        assert!(err.to_string().contains("did not complete within 1s"));
        tokio::time::timeout(Duration::from_secs(1), dropped_rx)
            .await
            .expect("aborted task is awaited")
            .expect("task drop guard ran");
    }

    #[test]
    fn probe_error_outcome_classifies_known_failure_modes() {
        assert_eq!(
            probe_error_outcome(&anyhow!("probe did not complete within 10s")),
            "timeout"
        );
        assert_eq!(
            probe_error_outcome(&anyhow!(
                "connect_with_profile(moqt-blockcast-01) failed: refused"
            )),
            "connect_failed"
        );
        assert_eq!(
            probe_error_outcome(&anyhow!("SUBSCRIBE failed: Cancel")),
            "subscribe_failed"
        );
        assert_eq!(
            probe_error_outcome(&anyhow!(
                "SUBSCRIBE_OK for `v` carried no key 0x40 (history window) under moqt-blockcast-01"
            )),
            "missing_history_window"
        );
        assert_eq!(
            probe_error_outcome(&anyhow!("session ended before SUBSCRIBE_OK arrived")),
            "session_failed"
        );
        assert_eq!(probe_error_outcome(&anyhow!("something else")), "other");
    }
}
