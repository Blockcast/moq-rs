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
        let outcome = match tokio::time::timeout(probe_timeout, probe_once(&args, &tls)).await {
            Ok(inner) => inner,
            Err(_elapsed) => Err(anyhow!(
                "probe did not complete within {}s",
                args.probe_timeout_seconds
            )),
        };

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

    let mut session_task =
        tokio::spawn(async move { session.run().await.context("session error") });

    let namespace = TrackNamespace::from_utf8_path(&args.name);
    let (mut tracks_writer, _request, mut tracks_reader) = Tracks::new(namespace.clone()).produce();
    let track_writer = tracks_writer
        .create(args.track.as_str())
        .ok_or_else(|| anyhow!("TracksWriter::create returned None for `{}`", args.track))?;

    let probe = async {
        subscriber
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
        r = &mut session_task => match r {
            Ok(Ok(())) => Err(anyhow!("session ended before SUBSCRIBE_OK arrived")),
            Ok(Err(err)) => Err(err.context("session task failed while probe was pending")),
            Err(join_err) => Err(anyhow!("session task terminated abnormally: {join_err}")),
        },
    };

    session_task.abort();
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
