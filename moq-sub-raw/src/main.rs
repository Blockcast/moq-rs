// SPDX-License-Identifier: MIT OR Apache-2.0

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use moq_native_ietf::quic;
use moq_transport::{coding::TrackNamespace, serve::Tracks, session::Subscriber};

mod cli;
mod subscribe;

use cli::Args;
use subscribe::{drain_track_to_writer, validate_track_output_pairs};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,quinn=warn")),
        )
        .init();

    let args = Args::parse();
    validate_track_output_pairs(&args.track, &args.output)?;

    // ---- moq-transport session ----

    let tls = args.tls.load()?;
    let quic_endpoint = quic::Endpoint::new(quic::Config::new(args.bind, None, tls.clone())?)?;

    tracing::info!(url = %args.url, "connecting to relay");
    let (session, connection_id, transport) =
        quic_endpoint.client.connect(&args.url, None).await?;
    tracing::info!(%connection_id, "connected to relay");

    let (session, subscriber) = Subscriber::connect(session, transport)
        .await
        .context("failed to create MoQ Transport subscriber session")?;

    // Per the M.0 finding (.planning/moq-rs-m0-results.md): the
    // namespace lives in the URL-path-derived tenant scope on the
    // relay, not as a connect-URL path. Stay on the root path.
    let namespace = TrackNamespace::from_utf8_path(&args.name);
    let (mut tracks_writer, _request, mut tracks_reader) =
        Tracks::new(namespace.clone()).produce();

    // For each (track, output) pair: create the producer-side
    // TrackWriter, hand it to a clone of the subscriber to wire up
    // the actual MoQ subscription, then spawn a drain task that
    // reads the cached TrackReader and dumps payloads into the file.
    let mut tasks = tokio::task::JoinSet::new();
    for (name, path) in args.track.iter().zip(args.output.iter()) {
        let track_writer = tracks_writer.create(name).ok_or_else(|| {
            anyhow!("TracksWriter::create returned None for `{name}` (broadcast closed?)")
        })?;
        {
            let mut sub = subscriber.clone();
            let name = name.clone();
            tokio::spawn(async move {
                if let Err(err) = sub.subscribe(track_writer).await {
                    tracing::warn!(track = %name, "subscribe failed: {err:?}");
                }
            });
        }
        let track_reader = tracks_reader
            .subscribe(namespace.clone(), name)
            .ok_or_else(|| anyhow!("TracksReader::subscribe returned None for `{name}`"))?;
        let path = path.clone();
        let name = name.clone();
        tasks.spawn(async move {
            let mut file = tokio::fs::File::create(&path)
                .await
                .with_context(|| format!("creating output `{}`", path.display()))?;
            let n = drain_track_to_writer(track_reader, &mut file)
                .await
                .with_context(|| format!("draining track `{name}`"))?;
            tracing::info!(
                track = %name,
                bytes = n,
                output = %path.display(),
                "track drained"
            );
            Ok::<(), anyhow::Error>(())
        });
    }

    tokio::select! {
        res = session.run() => res.context("session error")?,
        _ = wait_tasks(&mut tasks) => {
            tracing::info!("all drain tasks finished");
        }
    }

    Ok(())
}

/// Wait for all spawned drain tasks to complete, logging any errors.
async fn wait_tasks(tasks: &mut tokio::task::JoinSet<Result<()>>) {
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(err)) => tracing::warn!("drain task error: {err:?}"),
            Err(join_err) => tracing::warn!("drain task panicked: {join_err:?}"),
        }
    }
}
