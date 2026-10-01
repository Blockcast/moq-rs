// SPDX-License-Identifier: MIT OR Apache-2.0

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use moq_native_ietf::quic;
use moq_transport::{
    coding::{KeyValuePairs, TrackNamespace, TrackNamespacePrefix},
    message::SubscribeOptions,
    serve::Tracks,
    session::{SubscribeNamespace, Subscriber},
};

mod cli;
mod subscribe;

use cli::Args;
use subscribe::{drain_track_to_writer, namespace_published, validate_track_output_pairs};

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
    let (session, connection_id, transport, selected_version) =
        quic_endpoint.client.connect(&args.url, None).await?;
    tracing::info!(%connection_id, "connected to relay");

    let (session, subscriber) =
        Subscriber::connect_negotiated(session, transport, selected_version)
            .await
            .context("failed to create MoQ Transport subscriber session")?;
    // The session's run loop carries every request below, so each wait for a
    // reply has to run alongside it.
    let session = session.run();
    tokio::pin!(session);

    // Per the M.0 finding (.planning/moq-rs-m0-results.md): the
    // namespace lives in the URL-path-derived tenant scope on the
    // relay, not as a connect-URL path. Stay on the root path.
    let namespace = TrackNamespace::from_utf8_path(&args.name);

    // Held until the process exits: dropping it cancels the SUBSCRIBE_NAMESPACE.
    let _namespace_subscription = if args.await_namespace {
        tokio::select! {
            res = &mut session => {
                res.context("session error")?;
                bail!("session closed before the relay published `{namespace}`");
            }
            res = await_namespace(subscriber.clone(), &namespace) => Some(res?),
        }
    } else {
        None
    };

    let (mut tracks_writer, _request, mut tracks_reader) = Tracks::new(namespace.clone()).produce();

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
        res = &mut session => res.context("session error")?,
        res = wait_tasks(&mut tasks) => {
            res?;
            tracing::info!("all drain tasks finished");
        }
    }

    Ok(())
}

/// Wait until the relay reports `namespace` as published.
///
/// Sends SUBSCRIBE_NAMESPACE with the namespace itself as the prefix and
/// returns once the relay answers with NAMESPACE for exactly that namespace.
/// moq-relay-ietf sends NAMESPACE only for namespaces in its registry, and a
/// publisher's PUBLISH_NAMESPACE enters the registry together with the route
/// that forwards a SUBSCRIBE to that publisher. A SUBSCRIBE sent after this
/// returns is therefore routed rather than refused as not found. The wait has
/// no deadline of its own; the caller bounds it. The returned request cancels
/// the SUBSCRIBE_NAMESPACE when dropped.
async fn await_namespace(
    mut subscriber: Subscriber,
    namespace: &TrackNamespace,
) -> Result<SubscribeNamespace> {
    let prefix = TrackNamespacePrefix {
        fields: namespace.fields.clone(),
    };
    let request = subscriber
        .subscribe_namespace(
            prefix,
            SubscribeOptions::Namespace,
            KeyValuePairs::default(),
        )
        .await
        .with_context(|| format!("sending SUBSCRIBE_NAMESPACE for `{namespace}`"))?;
    request
        .ok()
        .await
        .with_context(|| format!("relay refused SUBSCRIBE_NAMESPACE for `{namespace}`"))?;
    tracing::info!(%namespace, "waiting for the relay to publish the namespace");

    while let Some(event) = request
        .next()
        .await
        .with_context(|| format!("reading SUBSCRIBE_NAMESPACE responses for `{namespace}`"))?
    {
        if namespace_published(&event, namespace) {
            tracing::info!(%namespace, "relay published the namespace");
            return Ok(request);
        }
    }
    bail!("SUBSCRIBE_NAMESPACE for `{namespace}` ended before the relay published it")
}

/// Wait for all spawned drain tasks to complete, returning the first drain
/// error or join panic. A raw-capture run whose tracks fail to drain (bad
/// output path, non-subgroup track, read/write error, panic) MUST exit
/// non-zero so smoke/E2E wrappers observe the failure instead of treating an
/// empty capture as success (repo rule: no silent fallbacks).
async fn wait_tasks(tasks: &mut tokio::task::JoinSet<Result<()>>) -> Result<()> {
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err.context("drain task failed")),
            Err(join_err) => return Err(anyhow!("drain task panicked: {join_err:?}")),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // wait_tasks must surface the first drain error, not swallow it — otherwise
    // a capture run that produced no valid output still exits 0.
    #[tokio::test]
    async fn wait_tasks_propagates_first_drain_error() {
        let mut tasks: tokio::task::JoinSet<Result<()>> = tokio::task::JoinSet::new();
        tasks.spawn(async { Ok(()) });
        tasks.spawn(async { Err(anyhow!("drain boom")) });
        let res = wait_tasks(&mut tasks).await;
        assert!(res.is_err(), "a failing drain task must propagate an error");
    }

    #[tokio::test]
    async fn wait_tasks_ok_when_all_succeed() {
        let mut tasks: tokio::task::JoinSet<Result<()>> = tokio::task::JoinSet::new();
        tasks.spawn(async { Ok(()) });
        tasks.spawn(async { Ok(()) });
        assert!(wait_tasks(&mut tasks).await.is_ok());
    }
}
