// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use clap::Parser;
use moq_catalog::{Root, TrackPackaging};
use moq_native_ietf::quic;
use moq_transport::{
    coding::TrackNamespace,
    profile::WireProfile,
    serve::{DatagramsWriter, SubgroupsWriter, Tracks, TracksWriter},
    session::Publisher,
};
use tokio::io::AsyncWriteExt;

mod cli;
mod datagram;
mod framing;
mod metrics_endpoint;
mod mmtp_parse;
#[cfg(feature = "profiling")]
mod profiling;
mod publish;
mod udp;

use cli::{Args, MmtpInput};
use datagram::DatagramState;
use mmtp_parse::route;
use publish::{
    dispatch, RepairSink, SharedPresentationEpoch, TrackState, CONTROL_PRIORITY, SOURCE_PRIORITY,
};

#[cfg(feature = "heap-profiling")]
#[global_allocator]
static GLOBAL_ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// The canonical MSF catalog has no publisher-retention field. Keep the policy
// explicit and bounded in the runtime until it is configurable outside the
// wire schema.
const PUBLISHER_HISTORY_WINDOW: NonZeroU64 = match NonZeroU64::new(32) {
    Some(value) => value,
    None => panic!("publisher history window must be nonzero"),
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,quinn=warn")),
        )
        .init();

    // Optional on-demand CPU profiler (feature `profiling` + MOQ_PUB_PROFILE_ADDR).
    // No-op unless both the compile feature and the env var are set.
    #[cfg(feature = "profiling")]
    profiling::spawn_if_enabled();

    // Optional Prometheus metrics exporter (feature `metrics-prometheus` +
    // MOQ_PUB_METRICS_ADDR). No-op unless the env var is set; see
    // metrics_endpoint.rs for the activation pattern. Runs before
    // Args::parse() so this path always gets first claim on the
    // process-global recorder — see install_flag_exporter_if_needed below
    // and the precedence note on Args::metrics_addr in cli.rs (BLO-26174).
    let env_metrics_addr = metrics_endpoint::spawn_if_enabled();

    let args = Args::parse();

    // Reconcile the legacy --metrics-addr flag against whatever the env-var
    // path above already did. Serves whatever the process has already
    // recorded via the `metrics` facade — notably moq-native-ietf's
    // `moq_negotiation_total`, emitted on every connect attempt regardless of
    // this flag. Never panics: see metrics_endpoint::install_flag_exporter_if_needed.
    metrics_endpoint::install_flag_exporter_if_needed(args.metrics_addr, env_metrics_addr);

    // ---- catalog ----

    let catalog_bytes = tokio::fs::read(&args.catalog_json)
        .await
        .with_context(|| format!("reading catalog JSON {}", args.catalog_json.display()))?;
    let catalog: Root = serde_json::from_slice(&catalog_bytes)
        .with_context(|| format!("parsing catalog JSON {}", args.catalog_json.display()))?;

    // T5: library-level catalog validation (defense in depth — build_state_map
    // re-checks the publisher-relevant invariants at runtime).
    catalog
        .validate()
        .map_err(|e| anyhow::anyhow!("catalog validation failed: {e}"))?;
    check_namespace_consistency(&catalog, &args.name)?;

    let namespace = TrackNamespace::from_utf8_path(&args.name);

    // ---- input source ----
    //
    // Opened ONCE and held here for the life of the process — every relay
    // reconnect below only ever *borrows* it. For --mmtp-input=udp this is
    // what keeps the SSM `(S,G)` join alive across a relay session loss
    // (BLO-26173): the join is a property of the socket, not of the
    // moq-transport session, so a reconnect that rebuilds the session never
    // touches it.
    let udp_socket = match args.mmtp_input {
        MmtpInput::Udp => {
            let socket = udp::open_udp_socket(
                args.mmtp_udp_bind,
                args.mmtp_udp_source,
                args.mmtp_udp_iface,
            )
            .await?;
            tracing::info!(addr = %socket.local_addr()?, "listening for datagrams");
            Some(socket)
        }
        MmtpInput::Stdin => None,
    };
    let mut stdin = matches!(args.mmtp_input, MmtpInput::Stdin).then(tokio::io::stdin);
    let mut input_buf = vec![0u8; 65_536];
    let mut packet_count: u64 = 0;

    // ---- moq-transport session, retried with backoff on relay session loss ----

    let tls = args.tls.load()?;
    let mut quic_config = quic::Config::new(args.bind, None, tls.clone())?;
    let wire_profile = args.wire_profile.map(WireProfile::from);
    if let Some(profile) = wire_profile {
        quic_config = quic_config.with_wire_profiles([WireProfile::Draft16, profile]);
    }
    let quic_endpoint = quic::Endpoint::new(quic_config)?;

    let backoff_min = Duration::from_millis(args.reconnect_backoff_min_ms);
    let backoff_max = Duration::from_millis(args.reconnect_backoff_max_ms);
    let mut attempt: u32 = 0;

    loop {
        tracing::info!(url = %args.url, attempt, "connecting to relay");
        let connected = match wire_profile {
            Some(profile) => {
                quic_endpoint
                    .client
                    .connect_with_profile(&args.url, None, profile)
                    .await
            }
            None => quic_endpoint.client.connect(&args.url, None).await,
        };
        let (session, connection_id, transport, selected_version) = match connected {
            Ok(v) => v,
            Err(error) => {
                reconnect_after_failure(
                    SessionLost {
                        task: "connect",
                        error: Some(error),
                    },
                    &mut attempt,
                    backoff_min,
                    backoff_max,
                )
                .await;
                continue;
            }
        };
        let negotiated = Publisher::connect_negotiated(session, transport, selected_version)
            .await
            .context("failed to create MoQ Transport publisher");
        let (session, mut publisher) = match negotiated {
            Ok(v) => v,
            Err(error) => {
                reconnect_after_failure(
                    SessionLost {
                        task: "connect",
                        error: Some(error),
                    },
                    &mut attempt,
                    backoff_min,
                    backoff_max,
                )
                .await;
                continue;
            }
        };
        tracing::info!(%connection_id, attempt, "connected to relay");
        let connected_at = Instant::now();

        // A MoQ session's TracksReader is single-use (handed to exactly one
        // Publisher::publish_namespace call), so the catalog-derived router
        // and catalog-track writers are rebuilt fresh every reconnect. This
        // is pure, catalog-only construction with no I/O — the catalog was
        // already validated once above, so a failure here would be a
        // catalog/schema defect rather than a transient network condition,
        // and is treated as fatal rather than retried.
        let (mut tracks_writer, _request, tracks_reader) = Tracks::new(namespace.clone()).produce();
        let mut router = build_router(&mut tracks_writer, &catalog)?;
        tracing::info!(
            router = router.kind(),
            attempt,
            "built publisher router from catalog"
        );
        let _catalog_subgroups = publish_catalog_track(&mut tracks_writer, &catalog_bytes)?;

        // Run the session and namespace-publish halves on separate tokio
        // tasks (see historical note: this keeps ingest and egress off a
        // single core). Packet ingest is driven inline below via
        // `next_input_event` so the input source stays owned by this
        // function across every reconnect instead of being consumed by a
        // per-session task.
        let mut session_task =
            tokio::spawn(async move { session.run().await.context("session error") });
        let mut publish_namespace_task = tokio::spawn(async move {
            publisher
                .publish_namespace(tracks_reader)
                .await
                .context("publisher error")
        });

        let lost = loop {
            tokio::select! {
                r = &mut session_task => break describe_task_end("session", r),
                r = &mut publish_namespace_task => break describe_task_end("publish_namespace", r),
                event = next_input_event(udp_socket.as_ref(), stdin.as_mut(), &mut input_buf) => {
                    match event? {
                        InputEvent::Packet(packet) => {
                            router.handle(packet)?;
                            packet_count = packet_count.wrapping_add(1);
                            // `% ==` kept over `u64::is_multiple_of` (stable
                            // only since Rust 1.87) to honor the repo's 1.70+ MSRV.
                            #[allow(clippy::manual_is_multiple_of)]
                            if packet_count % 1000 == 0 {
                                tracing::debug!(packet_count, "packets dispatched");
                            }
                        }
                        InputEvent::Skip => {}
                        InputEvent::Exhausted => {
                            // Clean stdin EOF: the finite input is done, not
                            // the relay session — exit the whole process
                            // successfully rather than reconnecting forever.
                            // Flush a final ack so any wrappers know we're done.
                            let _ = tokio::io::stdout().flush().await;
                            session_task.abort();
                            publish_namespace_task.abort();
                            tracing::info!(packet_count, "input exhausted — publisher done");
                            return Ok(());
                        }
                    }
                }
            }
        };
        session_task.abort();
        publish_namespace_task.abort();
        drop(router);
        drop(_catalog_subgroups);
        drop(tracks_writer);
        // Fold this session's uptime into the streak before backing off, so
        // `attempt` stays a count of *consecutive* failures rather than a
        // lifetime total. See `attempt_after_session`.
        let uptime = connected_at.elapsed();
        let recovered = attempt_after_session(attempt, uptime, backoff_max);
        if recovered != attempt {
            tracing::info!(
                previous_attempt = attempt,
                uptime_ms = uptime.as_millis() as u64,
                healthy_after_ms = backoff_max.as_millis() as u64,
                "session outlived the backoff ceiling; resetting reconnect streak"
            );
        }
        attempt = recovered;
        reconnect_after_failure(lost, &mut attempt, backoff_min, backoff_max).await;
    }
}

/// Which task ended a session attempt, and why. `error: None` means the task
/// finished without an `Err` (e.g. the relay closed the session cleanly) —
/// still a reason to reconnect, just not a failure worth escalating past
/// `info`.
struct SessionLost {
    task: &'static str,
    error: Option<anyhow::Error>,
}

fn describe_task_end(
    task: &'static str,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> SessionLost {
    match result {
        Ok(Ok(())) => SessionLost { task, error: None },
        Ok(Err(error)) => SessionLost {
            task,
            error: Some(error),
        },
        Err(join_error) => SessionLost {
            task,
            error: Some(anyhow::anyhow!("task terminated abnormally: {join_error}")),
        },
    }
}

/// Exponential backoff before the next reconnect attempt: doubles from
/// `min` on each consecutive loss, capped at `max`.
fn backoff_duration(attempt: u32, min: Duration, max: Duration) -> Duration {
    let factor = 1u32.checked_shl(attempt).unwrap_or(u32::MAX);
    min.checked_mul(factor).unwrap_or(max).min(max)
}

/// The attempt counter to carry into the next reconnect, given how long the
/// session that just ended stayed up.
///
/// `backoff_duration` doubles on `attempt`, so the counter must describe a
/// *consecutive* failure streak. Without this, `attempt` only ever rises: a
/// publisher that reconnects, streams cleanly for hours, then drops again
/// resumes with the accumulated backoff, and after six lifetime losses —
/// however far apart — every later recovery stalls the full `backoff_max`.
/// The input source is drained only inside the session `select!`, so that
/// sleep is unread UDP, turning an instantly-recoverable blip into a visible
/// outage.
///
/// A session that stayed up for at least `healthy_after` is evidence the prior
/// streak is not predictive, so the streak resets. Resetting on the *connect*
/// path instead would be the shorter change, but it reintroduces hot-looping
/// when a session dies immediately after connecting — precisely what the
/// backoff exists to damp. `backoff_max` is the natural threshold: a session
/// that outlived the longest backoff was not part of the storm the backoff is
/// there to slow.
fn attempt_after_session(attempt: u32, uptime: Duration, healthy_after: Duration) -> u32 {
    if uptime >= healthy_after {
        0
    } else {
        attempt
    }
}

/// Record, log, and sleep out a lost session before the caller retries.
///
/// The `moq_pub_session_reconnect_total` counter (via the `metrics` facade —
/// same pattern as moq-native-ietf's unconditional `moq_negotiation_total`)
/// plus this log line are what make a reconnect storm distinguishable from a
/// healthy long session, per BLO-26173's acceptance criteria.
async fn reconnect_after_failure(
    lost: SessionLost,
    attempt: &mut u32,
    backoff_min: Duration,
    backoff_max: Duration,
) {
    metrics::counter!("moq_pub_session_reconnect_total", "task" => lost.task).increment(1);
    let backoff = backoff_duration(*attempt, backoff_min, backoff_max);
    match &lost.error {
        Some(error) => tracing::warn!(
            task = lost.task,
            %error,
            attempt = *attempt,
            backoff_ms = backoff.as_millis() as u64,
            "relay session lost; reconnecting"
        ),
        None => tracing::info!(
            task = lost.task,
            attempt = *attempt,
            backoff_ms = backoff.as_millis() as u64,
            "relay session ended; reconnecting"
        ),
    }
    tokio::time::sleep(backoff).await;
    *attempt = attempt.saturating_add(1);
}

/// One event yielded by the configured input source (UDP socket or stdin).
enum InputEvent {
    /// One MMTP packet/datagram ready to route.
    Packet(Bytes),
    /// A benign zero-length UDP datagram — caller should just poll again.
    Skip,
    /// The finite input (stdin) reached clean EOF. UDP never yields this.
    Exhausted,
}

/// Poll the configured input source for the next packet.
///
/// Exactly one of `udp_socket`/`stdin` is `Some`, matching `--mmtp-input`.
/// Callers hold the source in their own scope across reconnects rather than
/// handing it to a per-session task — see the `udp_socket` comment in
/// `main`.
async fn next_input_event<R: tokio::io::AsyncRead + Unpin>(
    udp_socket: Option<&tokio::net::UdpSocket>,
    stdin: Option<&mut R>,
    buf: &mut [u8],
) -> Result<InputEvent> {
    match (udp_socket, stdin) {
        (Some(socket), None) => {
            let (n, _addr) = socket.recv_from(buf).await.context("UDP recv_from error")?;
            if n == 0 {
                return Ok(InputEvent::Skip);
            }
            Ok(InputEvent::Packet(Bytes::copy_from_slice(&buf[..n])))
        }
        (None, Some(stdin)) => {
            match framing::read_one_frame(stdin)
                .await
                .context("stdin framing error")?
            {
                Some(frame) => Ok(InputEvent::Packet(Bytes::from(frame))),
                None => Ok(InputEvent::Exhausted),
            }
        }
        _ => unreachable!("exactly one input source is configured, matching --mmtp-input"),
    }
}

/// Build per-track state from the catalog's `multicast.endpoints[].tracks[]`.
///
/// Each entry produces:
///   - one new MoQ track on the broadcast (via TracksWriter::create)
///   - one transition into Subgroups mode (TrackWriter::subgroups)
///   - one TrackState keyed by MMTP packet_id
///
/// Errors:
///   - catalog has no `multicast` extension
///   - duplicate packet_id across endpoints
///   - referenced track name not found in `catalog.tracks`
///   - TracksWriter::create returns None (all readers dropped)
fn build_state_map(
    tracks_writer: &mut TracksWriter,
    catalog: &Root,
) -> Result<HashMap<u16, TrackState<SubgroupsWriter>>> {
    let multicast = catalog.multicast.as_ref().ok_or_else(|| {
        anyhow::anyhow!("catalog has no `multicast` extension — required for moq-pub-mmtp")
    })?;
    let endpoints = multicast
        .endpoints
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("catalog.multicast.endpoints is missing"))?;

    let mut map: HashMap<u16, TrackState<SubgroupsWriter>> = HashMap::new();
    let presentation_epoch: SharedPresentationEpoch = Default::default();
    for endpoint in endpoints {
        for track_ref in &endpoint.tracks {
            if map.contains_key(&track_ref.packet_id) {
                bail!(
                    "duplicate packet_id {} (used by track `{}` and a prior endpoint)",
                    track_ref.packet_id,
                    track_ref.name
                );
            }
            let catalog_track = catalog
                .tracks
                .iter()
                .find(|t| t.name == track_ref.name)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "multicast endpoint references track `{}` not present in catalog.tracks",
                        track_ref.name
                    )
                })?;
            if matches!(catalog_track.packaging, Some(TrackPackaging::FecRepair)) {
                tracing::debug!(
                    track = %track_ref.name,
                    packet_id = track_ref.packet_id,
                    "skipping catalog-declared FEC repair track; publisher creates repair siblings"
                );
                continue;
            }
            let timescale = catalog_track.timescale.ok_or_else(|| {
                anyhow::anyhow!(
                    "catalog track `{}` has no effective timescale",
                    track_ref.name
                )
            })?;
            if timescale == 0 {
                bail!(
                    "catalog track `{}` has invalid effective timescale 0; expected > 0",
                    track_ref.name
                );
            }
            let group_duration_ms = catalog_track.group_duration_ms;
            let group_duration_ticks = catalog_track
                .group_duration_ticks
                .or_else(|| group_duration_ms.map(|ms| ms as u64 * timescale as u64 / 1000))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "catalog track `{}` has no effective group duration",
                        track_ref.name
                    )
                })?;
            if group_duration_ticks == 0 {
                bail!(
                    "catalog track `{}` has invalid effective group duration 0 ticks; expected > 0",
                    track_ref.name
                );
            }
            let repair_group_depth = catalog_track
                .fec
                .as_ref()
                .and_then(|fec| fec.interleave_depth_ms)
                .map(|depth_ms| {
                    let numerator = depth_ms as u128 * timescale as u128;
                    let denominator = group_duration_ticks as u128 * 1000;
                    ceil_div_u128(numerator, denominator).max(1) as u64
                })
                .unwrap_or(1);

            let track_writer = tracks_writer.create(&track_ref.name).ok_or_else(|| {
                anyhow::anyhow!(
                    "TracksWriter::create returned None for `{}` (broadcast already closed?)",
                    track_ref.name
                )
            })?;

            // Mapping B opens many concurrent subgroups per group (Init + one
            // per MFU), so retained history must remain bounded. Retention is a
            // fixed publisher policy because it is not part of the MSF schema.
            let history_window = PUBLISHER_HISTORY_WINDOW;
            let subgroups = track_writer
                .subgroups_with_history(history_window)
                .with_context(|| format!("track `{}`: subgroups() failed", track_ref.name))?;

            let repair = if let Some(fec) = &catalog_track.fec {
                let repair_track = catalog
                    .tracks
                    .iter()
                    .find(|candidate| candidate.name == fec.repair_track)
                    .expect("Root::validate resolved fec.repairTrack");
                let priority = repair_track
                    .priority
                    .expect("Root::validate requires repair priority");
                let repair_writer = tracks_writer.create(&fec.repair_track).ok_or_else(|| {
                    anyhow::anyhow!(
                        "TracksWriter::create returned None for `{}` (broadcast already closed?)",
                        fec.repair_track
                    )
                })?;
                let repair_subgroups = repair_writer
                    .subgroups_with_history(history_window)
                    .with_context(|| format!("track `{}`: subgroups() failed", fec.repair_track))?;
                Some(RepairSink {
                    sink: repair_subgroups,
                    priority,
                    current_group: None,
                    current_group_id: None,
                })
            } else {
                None
            };

            map.insert(
                track_ref.packet_id,
                TrackState::new(
                    track_ref.name.clone(),
                    SOURCE_PRIORITY,
                    timescale,
                    group_duration_ticks,
                    repair_group_depth,
                    presentation_epoch.clone(),
                    subgroups,
                    repair,
                ),
            );
        }
    }
    Ok(map)
}

#[allow(clippy::manual_div_ceil)]
fn ceil_div_u128(numerator: u128, denominator: u128) -> u128 {
    (numerator + denominator - 1) / denominator
}

/// Catalog track name required by draft-ietf-moq-msf-00 §5.2.
const CATALOG_TRACK_NAMES: [&str; 1] = ["catalog"];

/// Publish the broadcast's catalog JSON on each catalog track name.
///
/// The JSON body is posted as a single object on group 0 in the control band.
///
/// Returns one `SubgroupsWriter` per catalog track name so the caller can
/// retain them for the session's lifetime; dropping one would close that
/// track and surface as "catalog gone" to subscribers using that name.
fn publish_catalog_track(
    tracks_writer: &mut TracksWriter,
    catalog_bytes: &[u8],
) -> Result<Vec<SubgroupsWriter>> {
    let mut writers = Vec::with_capacity(CATALOG_TRACK_NAMES.len());
    for name in CATALOG_TRACK_NAMES {
        let track = tracks_writer
            .create(name)
            .ok_or_else(|| anyhow::anyhow!("TracksWriter::create returned None for `{name}`"))?;
        let mut subgroups = track
            .subgroups()
            .with_context(|| format!("`{name}` track: subgroups() failed"))?;
        let mut subgroup = subgroups
            .create(moq_transport::serve::Subgroup {
                group_id: 0,
                subgroup_id: 0,
                priority: CONTROL_PRIORITY,
            })
            .with_context(|| format!("`{name}` SubgroupsWriter::create failed"))?;
        subgroup
            .write(Bytes::copy_from_slice(catalog_bytes))
            .with_context(|| format!("writing catalog JSON object failed for `{name}`"))?;
        // Dropping `subgroup` here is intentional — the SubgroupObjectWriter
        // it produced internally has remain==0 (full payload written) so the
        // reader sees a complete object.
        drop(subgroup);
        writers.push(subgroups);
    }
    Ok(writers)
}

/// Check that the catalog's embedded namespace (if any) matches the
/// broadcast name from the `--name` CLI flag.
///
/// Catches publisher misconfiguration where a track namespace disagrees with
/// the broadcast name announced to the relay.
fn check_namespace_consistency(catalog: &Root, name: &str) -> Result<()> {
    for track in &catalog.tracks {
        if let Some(ns) = &track.namespace {
            if ns == name {
                continue;
            }
            bail!(
                "catalog track `{}` namespace `{ns}` disagrees with broadcast --name `{name}`",
                track.name
            );
        }
    }
    Ok(())
}

/// Publisher router: the per-protocol dispatch chosen from the catalog.
///
/// `Mmtp` interprets MMTP MPU/MFU structure (Mapping B subgrouping + AL-FEC
/// repair siblings, see `publish::dispatch`); `Datagram` carries each UDP
/// datagram as one opaque native MoQ datagram (see `datagram::DatagramState`).
enum Router {
    Mmtp(HashMap<u16, TrackState<SubgroupsWriter>>),
    Datagram(DatagramState<DatagramsWriter>),
}

impl Router {
    fn kind(&self) -> &'static str {
        match self {
            Router::Mmtp(_) => "mmtp",
            Router::Datagram(_) => "datagram",
        }
    }

    /// Publish one received packet/datagram to its track(s).
    fn handle(&mut self, packet: Bytes) -> Result<()> {
        match self {
            Router::Mmtp(state_map) => {
                let routing = route(&packet).context("MMTP header parse error")?;
                dispatch(state_map, &routing, packet)
            }
            Router::Datagram(state) => state.handle(packet),
        }
    }
}

/// Choose the publisher router from the catalog's track packaging.
///
/// A catalog with a `packaging=datagram` source track selects the opaque
/// datagram router; anything else is MMTP (the default, preserving prior
/// behavior). `expand_common_fields` has already run, so each track's
/// `packaging` is its effective (track-or-common) value.
fn build_router(tracks_writer: &mut TracksWriter, catalog: &Root) -> Result<Router> {
    let has_datagram = catalog
        .tracks
        .iter()
        .any(|t| matches!(t.packaging, Some(TrackPackaging::Datagram)));
    if has_datagram {
        Ok(Router::Datagram(build_datagram_state(
            tracks_writer,
            catalog,
        )?))
    } else {
        Ok(Router::Mmtp(build_state_map(tracks_writer, catalog)?))
    }
}

/// Build opaque datagram state from a catalog with exactly one
/// `packaging=datagram` source track.
///
/// Errors:
///   - no, or more than one, datagram track (the router maps a single stream)
///   - TracksWriter::create returns None (broadcast already closed)
fn build_datagram_state(
    tracks_writer: &mut TracksWriter,
    catalog: &Root,
) -> Result<DatagramState<DatagramsWriter>> {
    let mut datagram_tracks = catalog
        .tracks
        .iter()
        .filter(|t| matches!(t.packaging, Some(TrackPackaging::Datagram)));
    let track = datagram_tracks.next().ok_or_else(|| {
        anyhow::anyhow!("datagram router selected but no packaging=datagram track present")
    })?;
    if datagram_tracks.next().is_some() {
        bail!("datagram router supports exactly one packaging=datagram track");
    }

    // The datagram ring retains a fixed number of payloads for lagging
    // subscribers. Shred-style bursts land faster than a reader wakes, so the
    // runtime policy must stay explicit and bounded even though the canonical
    // MSF catalog does not carry a retention field.
    let _multicast = catalog.multicast.as_ref().ok_or_else(|| {
        anyhow::anyhow!("catalog has no `multicast` extension — required for datagram publishing")
    })?;
    let history_window = PUBLISHER_HISTORY_WINDOW;

    let mut track_writer = tracks_writer.create(&track.name).ok_or_else(|| {
        anyhow::anyhow!(
            "TracksWriter::create returned None for `{}` (broadcast already closed?)",
            track.name
        )
    })?;
    // Set on the Track BEFORE `.datagrams()` consumes it: datagrams() inherits
    // the window as the bounded ring depth (publisher memory = window × payload
    // size, raw-lossy supersession beyond it) AND the session advertises it in
    // SUBSCRIBE_OK (BLO-10339) so a downstream relay mirror bounds its own
    // retention.
    track_writer.set_history_window(history_window)?;
    let datagrams = track_writer
        .datagrams()
        .with_context(|| format!("track `{}`: datagrams() failed", track.name))?;

    // Datagram source objects retain their existing priority-0 policy.
    Ok(DatagramState::new(track.name.clone(), 0, datagrams))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mmtp_parse::{MfuIdentity, PacketRouting};
    use mmt_core::header::{FragmentType, PacketType};
    use moq_catalog::multicast::{MulticastConfig, MulticastEndpoint, MulticastTrackRef};
    use moq_catalog::{FecAlgorithm, FecDescriptor, MmtpMode, Track};
    use moq_transport::serve::{TrackReaderMode, Tracks};

    fn ns() -> TrackNamespace {
        TrackNamespace::from_utf8_path("test-broadcast")
    }

    fn track(name: &str, packaging: Option<TrackPackaging>) -> Track {
        let mmtp_mode = if matches!(packaging, Some(TrackPackaging::Mmtp)) {
            Some(MmtpMode::Mpu)
        } else {
            None
        };
        Track {
            name: name.into(),
            packaging,
            mmtp_mode,
            timescale: mmtp_mode.map(|_| 90_000),
            group_duration_ms: mmtp_mode.map(|_| 1_000),
            ..Default::default()
        }
    }

    fn catalog_with(tracks: Vec<Track>, multicast: Option<MulticastConfig>) -> Root {
        Root {
            version: 1,
            streaming_format: "mmtp".into(),
            streaming_format_version: "0.2".into(),
            supports_delta_updates: Some(true),
            tracks,
            multicast,
        }
    }

    fn endpoint(track_refs: Vec<(&str, u16)>) -> MulticastEndpoint {
        MulticastEndpoint {
            protocol: None,
            source_address: None,
            group_address: "232.0.1.1".into(),
            port: 5004,
            tracks: track_refs
                .into_iter()
                .map(|(name, packet_id)| MulticastTrackRef {
                    name: name.into(),
                    packet_id,
                })
                .collect(),
            bandwidth: None,
        }
    }

    fn expect_err(r: Result<HashMap<u16, TrackState<SubgroupsWriter>>>) -> anyhow::Error {
        match r {
            Err(e) => e,
            Ok(_) => panic!("expected Err, got Ok"),
        }
    }

    #[test]
    fn build_state_map_errors_when_no_multicast_extension() {
        let cat = catalog_with(vec![track("v", Some(TrackPackaging::Mmtp))], None);
        let (mut tw, _r, _rd) = Tracks::new(ns()).produce();
        let err = expect_err(build_state_map(&mut tw, &cat));
        assert!(
            err.to_string().contains("no `multicast` extension"),
            "got: {err}"
        );
    }

    #[test]
    fn build_state_map_errors_when_endpoints_missing() {
        let cat = catalog_with(
            vec![track("v", Some(TrackPackaging::Mmtp))],
            Some(MulticastConfig::default()),
        );
        let (mut tw, _r, _rd) = Tracks::new(ns()).produce();
        let err = expect_err(build_state_map(&mut tw, &cat));
        assert!(
            err.to_string().contains("endpoints is missing"),
            "got: {err}"
        );
    }

    #[test]
    fn build_state_map_errors_on_duplicate_packet_id() {
        let cat = catalog_with(
            vec![
                track("v", Some(TrackPackaging::Mmtp)),
                track("a", Some(TrackPackaging::Mmtp)),
            ],
            Some(MulticastConfig {
                endpoints: Some(vec![endpoint(vec![("v", 1), ("a", 1)])]),
                network_source: None,
            }),
        );
        let (mut tw, _r, _rd) = Tracks::new(ns()).produce();
        let err = expect_err(build_state_map(&mut tw, &cat));
        assert!(
            err.to_string().contains("duplicate packet_id"),
            "got: {err}"
        );
    }

    #[test]
    fn build_state_map_errors_on_missing_track_reference() {
        let cat = catalog_with(
            vec![track("v", Some(TrackPackaging::Mmtp))],
            Some(MulticastConfig {
                endpoints: Some(vec![endpoint(vec![("does-not-exist", 1)])]),
                network_source: None,
            }),
        );
        let (mut tw, _r, _rd) = Tracks::new(ns()).produce();
        let err = expect_err(build_state_map(&mut tw, &cat));
        assert!(
            err.to_string().contains("not present in catalog.tracks"),
            "got: {err}"
        );
    }

    #[test]
    fn build_state_map_uses_fixed_runtime_history() {
        let cat = catalog_with(
            vec![track("v", Some(TrackPackaging::Mmtp))],
            Some(MulticastConfig {
                endpoints: Some(vec![endpoint(vec![("v", 1)])]),
                network_source: None,
            }),
        );
        let (mut tw, _r, _rd) = Tracks::new(ns()).produce();
        let map = build_state_map(&mut tw, &cat).expect("schema catalog uses runtime history");
        assert!(map.contains_key(&1));
    }

    #[test]
    fn build_state_map_rejects_zero_timescale() {
        let mut source = track("v", Some(TrackPackaging::Mmtp));
        source.timescale = Some(0);
        let cat = catalog_with(
            vec![source],
            Some(MulticastConfig {
                endpoints: Some(vec![endpoint(vec![("v", 1)])]),
                network_source: None,
            }),
        );
        let (mut tw, _r, _rd) = Tracks::new(ns()).produce();
        let err = expect_err(build_state_map(&mut tw, &cat));
        assert!(
            err.to_string().contains("effective timescale 0"),
            "got: {err}"
        );
    }

    #[test]
    fn build_state_map_rejects_zero_effective_group_duration() {
        let mut source = track("v", Some(TrackPackaging::Mmtp));
        source.group_duration_ticks = None;
        source.group_duration_ms = Some(1);
        source.timescale = Some(1);
        let cat = catalog_with(
            vec![source],
            Some(MulticastConfig {
                endpoints: Some(vec![endpoint(vec![("v", 1)])]),
                network_source: None,
            }),
        );
        let (mut tw, _r, _rd) = Tracks::new(ns()).produce();
        let err = expect_err(build_state_map(&mut tw, &cat));
        assert!(
            err.to_string().contains("effective group duration 0 ticks"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn datagram_router_uses_bounded_ring_transport_mode() {
        let cat = catalog_with(
            vec![track("shreds", Some(TrackPackaging::Datagram))],
            Some(MulticastConfig {
                endpoints: None,
                network_source: None,
            }),
        );
        let (mut tracks, _requests, mut readers) = Tracks::new(ns()).produce();

        let mut state = build_datagram_state(&mut tracks, &cat).unwrap();
        let reader = readers
            .get_track_reader(&ns(), "shreds")
            .expect("datagram track registered");

        let TrackReaderMode::Datagrams(mut datagrams) = reader.mode().await.unwrap() else {
            panic!("datagram track must resolve to TrackReaderMode::Datagrams");
        };

        // The schema no longer carries deployment retention policy; the runtime
        // default is large enough to retain this ten-datagram burst.
        for value in 0..10u8 {
            state.handle(Bytes::from(vec![value; 8])).unwrap();
        }
        drop(state); // close the writer so the drain below terminates

        let mut got = Vec::new();
        while let Some(datagram) = datagrams.read().await.unwrap() {
            got.push(datagram.group_id);
        }
        assert_eq!(got, (0..10).collect::<Vec<_>>());
        assert_eq!(datagrams.dropped(), 0);
    }

    #[tokio::test]
    async fn datagram_router_requires_multicast_config() {
        // Datagram publishing is only valid for a multicast catalog. The
        // retention depth itself is fixed runtime policy, not catalog data.
        let cat = catalog_with(vec![track("shreds", Some(TrackPackaging::Datagram))], None);
        let (mut tracks, _requests, _readers) = Tracks::new(ns()).produce();

        let err = match build_datagram_state(&mut tracks, &cat) {
            Ok(_) => panic!("expected startup error for missing multicast config"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("multicast"), "got: {err}");
    }

    #[test]
    fn publish_catalog_track_registers_only_canonical_name() {
        // On startup, the publisher posts the full catalog JSON as a single
        // object on group 0 at control priority 32.
        let cat = catalog_with(
            vec![track("v", Some(TrackPackaging::Mmtp))],
            Some(MulticastConfig {
                endpoints: Some(vec![endpoint(vec![("v", 1)])]),
                network_source: None,
            }),
        );
        let catalog_bytes = serde_json::to_vec(&cat).unwrap();
        let (mut tw, _r, mut tr) = Tracks::new(ns()).produce();
        // Retain the returned subgroups writers so the tracks stay open during
        // the assertion (TrackReader::is_closed would otherwise observe
        // writer-drop as stale).
        let _retained = publish_catalog_track(&mut tw, &catalog_bytes)
            .expect("publish_catalog_track returns Ok");

        let reader = tr
            .get_track_reader(&ns(), "catalog")
            .expect("canonical catalog track is registered");
        assert!(!reader.is_closed());
        assert!(tr.get_track_reader(&ns(), "catalog.json").is_none());
        assert!(tr.get_track_reader(&ns(), ".catalog").is_none());
    }

    #[test]
    fn build_state_map_happy_path_with_two_tracks() {
        let cat = catalog_with(
            vec![
                track("v", Some(TrackPackaging::Mmtp)),
                track("a", Some(TrackPackaging::Mmtp)),
            ],
            Some(MulticastConfig {
                endpoints: Some(vec![endpoint(vec![("v", 17), ("a", 18)])]),
                network_source: None,
            }),
        );
        let (mut tw, _r, _rd) = Tracks::new(ns()).produce();
        let map = build_state_map(&mut tw, &cat).unwrap();
        assert_eq!(map.len(), 2);
        let v = map.get(&17).expect("packet_id 17 present");
        assert_eq!(v.name, "v");
        assert_eq!(v.priority, SOURCE_PRIORITY);
        assert!(v.last_seen_mpu_seq.is_none());
        assert!(v.repair.is_none(), "tracks without fec have no repair sink");
        let a = map.get(&18).expect("packet_id 18 present");
        assert_eq!(a.name, "a");
        assert_eq!(a.priority, SOURCE_PRIORITY);
        assert!(a.repair.is_none(), "tracks without fec have no repair sink");
    }

    #[test]
    fn build_state_map_skips_catalog_declared_fec_repair_tracks() {
        let cat = catalog_with(
            vec![
                track("v", Some(TrackPackaging::Mmtp)),
                track("v/repair", Some(TrackPackaging::FecRepair)),
            ],
            Some(MulticastConfig {
                endpoints: Some(vec![endpoint(vec![("v", 17), ("v/repair", 18)])]),
                network_source: None,
            }),
        );
        let (mut tw, _r, _rd) = Tracks::new(ns()).produce();
        let map = build_state_map(&mut tw, &cat).unwrap();
        assert_eq!(map.len(), 1);
        assert!(map.contains_key(&17));
        assert!(!map.contains_key(&18));
    }

    #[test]
    fn build_state_map_uses_catalog_fec_repair_track_name() {
        // When the source track declares `fec`, the repair sibling is registered
        // under the catalog's fec.repairTrack name (draft-ramadan-moq-fec §5.1),
        // not the `<source>/repair` convention.
        let mut v = track("v", Some(TrackPackaging::Mmtp));
        v.fec = Some(FecDescriptor {
            algorithm: FecAlgorithm::RaptorQ,
            source_symbols: 32,
            repair_symbols: 8,
            symbol_size: 1312,
            interleave_depth_ms: None,
            repair_track: "v/fec-custom".into(),
            mode: None,
        });
        let mut repair = track("v/fec-custom", Some(TrackPackaging::FecRepair));
        repair.priority = Some(240);
        let cat = catalog_with(
            vec![v, repair],
            Some(MulticastConfig {
                endpoints: Some(vec![endpoint(vec![("v", 17)])]),
                network_source: None,
            }),
        );
        let (mut tw, _r, mut tr) = Tracks::new(ns()).produce();
        let map = build_state_map(&mut tw, &cat).unwrap();
        assert!(
            map.get(&17).expect("source track present").repair.is_some(),
            "a fec-declaring track still gets a repair sibling"
        );
        assert!(
            tr.get_track_reader(&ns(), "v/fec-custom").is_some(),
            "repair sibling is registered under the catalog fec.repairTrack name"
        );
        assert!(
            tr.get_track_reader(&ns(), "v/repair").is_none(),
            "the `<source>/repair` convention name is not used when fec names one"
        );
    }

    #[test]
    fn build_state_map_does_not_invent_repair_track_without_fec() {
        let cat = catalog_with(
            vec![track("v", Some(TrackPackaging::Mmtp))],
            Some(MulticastConfig {
                endpoints: Some(vec![endpoint(vec![("v", 17)])]),
                network_source: None,
            }),
        );
        let (mut tw, _r, mut tr) = Tracks::new(ns()).produce();
        let _map = build_state_map(&mut tw, &cat).unwrap();
        assert!(tr.get_track_reader(&ns(), "v/repair").is_none());
    }

    #[tokio::test]
    async fn udp_recv_dispatches_one_packet() {
        // T4: each UDP datagram is one MMTP packet (no length prefix — the
        // datagram boundary IS the packet boundary). next_input_event must
        // read one datagram and hand it to the caller for dispatch (the
        // production loop then calls Router::handle, exercised here too).
        let cat = catalog_with(
            vec![track("v", Some(TrackPackaging::Mmtp))],
            Some(MulticastConfig {
                endpoints: Some(vec![endpoint(vec![("v", 1)])]),
                network_source: None,
            }),
        );
        let (mut tw, _r, _rd) = Tracks::new(ns()).produce();
        let state_map = build_state_map(&mut tw, &cat).unwrap();
        let mut router = Router::Mmtp(state_map);

        let recv_sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_addr = recv_sock.local_addr().unwrap();
        let send_sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();

        // One MPU Init packet for packet_id=1, mpu_seq=42.
        let pkt = synth_mpu_init_packet(1, 42);
        send_sock.send_to(&pkt, recv_addr).await.unwrap();

        let mut buf = vec![0u8; 65_536];
        let event = next_input_event::<tokio::io::Stdin>(Some(&recv_sock), None, &mut buf)
            .await
            .unwrap();
        let InputEvent::Packet(packet) = event else {
            panic!("expected InputEvent::Packet for a non-empty UDP datagram");
        };
        router.handle(packet).unwrap();

        let Router::Mmtp(state_map) = &router else {
            panic!("expected Mmtp router");
        };
        let s = state_map.get(&1).expect("packet_id 1 present");
        assert_eq!(s.last_seen_mpu_seq, Some(42));
        assert_eq!(
            s.current_group_id,
            Some(0),
            "MPU sequence 42 must not be copied into the formula-derived Group"
        );
    }

    #[tokio::test]
    async fn stdin_input_reports_exhausted_on_clean_eof() {
        // An empty Cursor mimics stdin closed with zero bytes — next_input_event
        // must distinguish this cleanly from a packet or a framing error so
        // main() can exit(0) instead of reconnecting.
        let mut empty = std::io::Cursor::new(Vec::<u8>::new());
        let mut buf = vec![0u8; 64];
        let event = next_input_event::<std::io::Cursor<Vec<u8>>>(None, Some(&mut empty), &mut buf)
            .await
            .unwrap();
        assert!(matches!(event, InputEvent::Exhausted));
    }

    #[tokio::test]
    async fn stdin_input_yields_packet_for_one_frame() {
        let payload = b"mmtp-frame";
        let mut wire = Vec::new();
        wire.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        wire.extend_from_slice(payload);
        let mut reader = std::io::Cursor::new(wire);
        let mut buf = vec![0u8; 64];
        let event = next_input_event::<std::io::Cursor<Vec<u8>>>(None, Some(&mut reader), &mut buf)
            .await
            .unwrap();
        let InputEvent::Packet(packet) = event else {
            panic!("expected InputEvent::Packet for one length-prefixed frame");
        };
        assert_eq!(&packet[..], payload);
    }

    #[test]
    fn backoff_duration_doubles_and_caps() {
        let min = Duration::from_millis(500);
        let max = Duration::from_millis(30_000);
        assert_eq!(backoff_duration(0, min, max), Duration::from_millis(500));
        assert_eq!(backoff_duration(1, min, max), Duration::from_millis(1_000));
        assert_eq!(backoff_duration(2, min, max), Duration::from_millis(2_000));
        assert_eq!(backoff_duration(6, min, max), Duration::from_millis(30_000));
        // Cap holds even for attempt counts far past where 2^attempt would
        // overflow a u32 shift.
        assert_eq!(backoff_duration(1_000, min, max), max);
    }

    #[test]
    fn healthy_session_resets_the_reconnect_streak() {
        let max = Duration::from_millis(30_000);

        // A session that outlived the backoff ceiling clears the streak, so the
        // next recovery starts from backoff_min instead of the accumulated cap.
        assert_eq!(attempt_after_session(6, max, max), 0);
        assert_eq!(attempt_after_session(6, Duration::from_secs(3_600), max), 0);
        // Boundary: `healthy_after` itself counts as healthy.
        assert_eq!(attempt_after_session(3, max, max), 0);

        // A session that died inside the ceiling is part of the same storm, so
        // the streak carries and the backoff keeps doubling.
        assert_eq!(
            attempt_after_session(3, Duration::from_millis(29_999), max),
            3
        );
        assert_eq!(attempt_after_session(1, Duration::ZERO, max), 1);
    }

    #[test]
    fn reconnect_backoff_recovers_after_a_healthy_session() {
        let min = Duration::from_millis(500);
        let max = Duration::from_millis(30_000);

        // Six consecutive losses pin the backoff at the ceiling.
        assert_eq!(backoff_duration(6, min, max), max);

        // One healthy session is enough to return the next recovery to
        // backoff_min — the regression this guards is a publisher that has
        // dropped six times over its lifetime stalling 30s on every later
        // blip, however far apart those blips were.
        let attempt = attempt_after_session(6, Duration::from_secs(3_600), max);
        assert_eq!(backoff_duration(attempt, min, max), min);
    }

    /// Pins the ordering that the reset's payoff depends on.
    ///
    /// `reconnect_after_failure` binds `backoff` from `*attempt` at the top of
    /// the function and increments only after sleeping, which is precisely why
    /// resetting `attempt` to 0 yields a `backoff_min` sleep on the next loss.
    /// Hoisting the increment above that binding turns `backoff_duration(0)`
    /// into `backoff_duration(1)` and silently restores the stall this change
    /// fixes — the tests above would not notice, because they call
    /// `backoff_duration` directly and never observe the ordering.
    ///
    /// Note the seam is the increment's position relative to the *binding*, not
    /// relative to the sleep: swapping `sleep` and the increment is inert, since
    /// `backoff` is already bound by then. Verified by mutation — the swap keeps
    /// this test green, the hoist fails it with `left: 1s, right: 500ms`.
    ///
    /// Time is paused, so the assertion is on virtual elapsed time and the test
    /// costs no wall-clock.
    #[tokio::test(start_paused = true)]
    async fn reconnect_sleeps_the_current_attempt_then_increments() {
        let min = Duration::from_millis(500);
        let max = Duration::from_millis(30_000);

        let mut attempt = 0;
        let start = tokio::time::Instant::now();
        reconnect_after_failure(
            SessionLost {
                task: "session",
                error: None,
            },
            &mut attempt,
            min,
            max,
        )
        .await;
        assert_eq!(
            start.elapsed(),
            min,
            "a reset attempt must sleep backoff_min, not the next step"
        );
        assert_eq!(attempt, 1, "attempt must increment after the sleep");

        // And the step after that is the doubled one, not the one just slept.
        let start = tokio::time::Instant::now();
        reconnect_after_failure(
            SessionLost {
                task: "session",
                error: None,
            },
            &mut attempt,
            min,
            max,
        )
        .await;
        assert_eq!(start.elapsed(), backoff_duration(1, min, max));
        assert_eq!(attempt, 2);
    }

    #[tokio::test]
    async fn describe_task_end_distinguishes_clean_end_from_error_and_panic() {
        let clean = describe_task_end("session", Ok(Ok(())));
        assert_eq!(clean.task, "session");
        assert!(clean.error.is_none());

        let errored = describe_task_end("session", Ok(Err(anyhow::anyhow!("boom"))));
        assert!(errored.error.is_some());

        // A JoinError can't be constructed directly outside tokio internals;
        // exercise it via an aborted task instead.
        let handle: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async {
            std::future::pending::<()>().await;
            Ok(())
        });
        handle.abort();
        let join_result = handle.await;
        let aborted = describe_task_end("session", join_result);
        assert!(aborted.error.is_some());
    }

    #[tokio::test]
    async fn receiver_observes_formula_groups_and_per_mfu_subgroups() {
        let cat = catalog_with(
            vec![track("v", Some(TrackPackaging::Mmtp))],
            Some(MulticastConfig {
                endpoints: Some(vec![endpoint(vec![("v", 1)])]),
                network_source: None,
            }),
        );
        let (mut tracks, _requests, mut readers) = Tracks::new(ns()).produce();
        let mut state_map = build_state_map(&mut tracks, &cat).unwrap();
        let reader = readers
            .get_track_reader(&ns(), "v")
            .expect("source track reader");

        let packet = |fragment_type, timestamp, identity| PacketRouting {
            packet_id: 1,
            packet_type: PacketType::Mpu,
            fec_type: 0,
            rap_flag: false,
            mpu_sequence: Some(90_000),
            fragment_type: Some(fragment_type),
            timestamp,
            timed: true,
            fragmentation_indicator: 0,
            fragment_counter: 0,
            mfu_identity: identity,
            aggregation: false,
        };
        dispatch(
            &mut state_map,
            &packet(FragmentType::Init, 0, None),
            Bytes::from_static(b"init"),
        )
        .unwrap();
        for sample_number in [19, 20] {
            dispatch(
                &mut state_map,
                &packet(
                    FragmentType::Mfu,
                    65_536,
                    Some(MfuIdentity::Timed {
                        movie_fragment_sequence_number: 7,
                        sample_number,
                    }),
                ),
                Bytes::from_static(b"mfu"),
            )
            .unwrap();
        }
        drop(state_map);

        let TrackReaderMode::Subgroups(mut subgroups) = reader.mode().await.unwrap() else {
            panic!("source track must use subgroup mode");
        };
        let mut observed = Vec::new();
        while let Some(subgroup) = subgroups.next().await.unwrap() {
            observed.push((subgroup.group_id, subgroup.subgroup_id, subgroup.priority));
        }
        assert_eq!(
            observed,
            vec![
                (0, 0, SOURCE_PRIORITY),
                (1, 1, SOURCE_PRIORITY),
                (1, 2, SOURCE_PRIORITY),
            ]
        );
    }

    fn synth_mpu_init_packet(packet_id: u16, mpu_seq: u32) -> Vec<u8> {
        use bytes::BufMut;
        use mmt_core::header::{FragmentType, MmtpHeader, MpuHeader, PacketType};
        let hdr = MmtpHeader::new(packet_id, PacketType::Mpu);
        let mut buf = bytes::BytesMut::with_capacity(64);
        hdr.write_to(&mut buf).unwrap();
        let mpu = MpuHeader::new(FragmentType::Init, mpu_seq);
        mpu.write_to(&mut buf).unwrap();
        buf.put_slice(&[0xAA, 0xBB]); // tiny payload
        buf.to_vec()
    }

    #[test]
    fn check_namespace_consistency_passes_when_common_namespace_matches() {
        // commonTrackFields.namespace = "bbb" matches --name=bbb → OK.
        let mut cat = catalog_with(vec![track("v", Some(TrackPackaging::Mmtp))], None);
        cat.tracks[0].namespace = Some("bbb".into());
        check_namespace_consistency(&cat, "bbb").expect("matching namespace is OK");
    }

    #[test]
    fn check_namespace_consistency_passes_when_no_common_namespace() {
        // Common has no namespace → publisher sets it from --name; OK.
        let cat = catalog_with(vec![track("v", Some(TrackPackaging::Mmtp))], None);
        check_namespace_consistency(&cat, "anything").expect("no common namespace is OK");
    }

    #[test]
    fn check_namespace_consistency_errors_on_mismatch() {
        // commonTrackFields.namespace = "foo" but --name=bar → hard error.
        // Catches publisher misconfiguration where the broadcast name
        // and the embedded catalog namespace disagree.
        let mut cat = catalog_with(vec![track("v", Some(TrackPackaging::Mmtp))], None);
        cat.tracks[0].namespace = Some("foo".into());
        let err = match check_namespace_consistency(&cat, "bar") {
            Err(e) => e,
            Ok(()) => panic!("expected Err on mismatched namespace"),
        };
        assert!(
            err.to_string().contains("namespace"),
            "expected namespace mismatch err, got: {err}"
        );
    }

    #[test]
    fn build_state_map_registers_declared_repair_tracks_on_broadcast() {
        let mut source = track("v", Some(TrackPackaging::Mmtp));
        source.fec = Some(FecDescriptor {
            algorithm: FecAlgorithm::RaptorQ,
            source_symbols: 32,
            repair_symbols: 8,
            symbol_size: 1312,
            interleave_depth_ms: None,
            repair_track: "v/repair".into(),
            mode: None,
        });
        let mut repair = track("v/repair", Some(TrackPackaging::FecRepair));
        repair.priority = Some(240);
        let cat = catalog_with(
            vec![source, repair],
            Some(MulticastConfig {
                endpoints: Some(vec![endpoint(vec![("v", 17)])]),
                network_source: None,
            }),
        );
        let (mut tw, _r, mut tr) = Tracks::new(ns()).produce();
        let _map = build_state_map(&mut tw, &cat).unwrap();
        let v = tr
            .get_track_reader(&ns(), "v")
            .expect("source track `v` registered");
        assert_eq!(v.name, "v".into());
        let v_repair = tr
            .get_track_reader(&ns(), "v/repair")
            .expect("repair track `v/repair` registered");
        assert_eq!(v_repair.name, "v/repair".into());
        assert!(!v_repair.is_closed(), "repair track is alive");
    }
}
