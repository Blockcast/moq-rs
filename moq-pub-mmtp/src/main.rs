// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;
use std::num::NonZeroU64;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use clap::Parser;
use moq_catalog::{Root, TrackPackaging};
use moq_native_ietf::quic;
use moq_transport::{
    coding::TrackNamespace,
    serve::{DatagramsWriter, SubgroupsWriter, Tracks, TracksWriter},
    session::Publisher,
};
use tokio::io::AsyncWriteExt;

mod cli;
mod datagram;
mod framing;
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

    let args = Args::parse();

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

    // ---- moq-transport session ----

    let namespace = TrackNamespace::from_utf8_path(&args.name);
    let (mut tracks_writer, _request, tracks_reader) = Tracks::new(namespace).produce();

    // Select the publisher router from the catalog's track packaging: MMTP
    // (per-packet_id MPU/MFU dispatch) or opaque datagram pass-through.
    let router = build_router(&mut tracks_writer, &catalog)?;
    tracing::info!(
        router = router.kind(),
        "built publisher router from catalog"
    );

    // Publish the catalog JSON on the canonical track. The returned writer is
    // retained for the session lifetime so subscribers do not observe a closed
    // catalog track.
    let _catalog_subgroups = publish_catalog_track(&mut tracks_writer, &catalog_bytes)?;
    tracing::info!(
        bytes = catalog_bytes.len(),
        tracks = ?CATALOG_TRACK_NAMES,
        "posted catalog on catalog tracks"
    );

    let tls = args.tls.load()?;
    let quic_endpoint = quic::Endpoint::new(quic::Config::new(args.bind, None, tls.clone())?)?;

    tracing::info!(url = %args.url, "connecting to relay");
    let (session, connection_id, transport, selected_version) =
        quic_endpoint.client.connect(&args.url, None).await?;
    tracing::info!(%connection_id, "connected to relay");

    let (session, mut publisher) =
        Publisher::connect_negotiated(session, transport, selected_version)
            .await
            .context("failed to create MoQ Transport publisher")?;

    // Run the three long-lived halves on SEPARATE tokio tasks rather than as
    // three branches of one `select!`. A single `select!` is one future = one
    // task, and tokio never parallelizes one task across workers — so ingest
    // (run_publisher: recv_from -> route -> create_group/put_object) and egress
    // (publisher.publish_namespace -> serve_subgroup: open_uni -> encode ->
    // quinn write) serialize on ONE core (the ~0.97-core, 90%-userspace ceiling
    // that gates the publish-latency floor; raising the CPU limit only removed
    // CFS throttling because the work was one task, not because it needed more
    // workers). Spawning lets the multi-thread runtime place ingest and egress
    // on different workers. They already communicate through the Arc-backed
    // `watch::State` behind SubgroupsWriter/SubgroupsReader, so the wire output
    // (objects, groups, subgroups, framing, ordering) is byte-identical — the
    // relay is unaffected. run_publisher stays a SINGLE task, so the monotonic
    // group_id assignment (datagram.rs) remains a single ordered point.
    //
    // Teardown is preserved: race the three JoinHandles and propagate the first
    // to finish (Ok or Err) exactly as the old `select!` did, so the first error
    // still returns immediately and the external watchdog respawns us. The other
    // tasks are aborted (the process is exiting regardless).
    let mmtp_input = args.mmtp_input;
    let udp_bind = args.mmtp_udp_bind;
    let udp_source = args.mmtp_udp_source;
    let udp_iface = args.mmtp_udp_iface;

    let mut session_task =
        tokio::spawn(async move { session.run().await.context("session error") });
    let mut publish_namespace_task = tokio::spawn(async move {
        publisher
            .publish_namespace(tracks_reader)
            .await
            .context("publisher error")
    });
    let mut publish_task = tokio::spawn(async move {
        run_publisher(
            mmtp_input,
            udp_bind,
            udp_source,
            udp_iface,
            router,
            tracks_writer,
        )
        .await
        .context("publisher loop error")
    });

    let first = tokio::select! {
        r = &mut session_task => r,
        r = &mut publish_namespace_task => r,
        r = &mut publish_task => r,
    };
    session_task.abort();
    publish_namespace_task.abort();
    publish_task.abort();

    // Unwrap the JoinHandle layer: a JoinError (panic/abort of the winning task)
    // is itself fatal and must drive respawn, same as any branch error would.
    match first {
        Ok(inner) => inner?,
        Err(join_err) => bail!("publisher task terminated abnormally: {join_err}"),
    }

    Ok(())
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

            let mut track_writer = tracks_writer.create(&track_ref.name).ok_or_else(|| {
                anyhow::anyhow!(
                    "TracksWriter::create returned None for `{}` (broadcast already closed?)",
                    track_ref.name
                )
            })?;

            // Mapping B opens many concurrent subgroups per group (Init + one
            // per MFU), so retained history must remain bounded. Retention is a
            // fixed publisher policy because it is not part of the MSF schema.
            let history_window = PUBLISHER_HISTORY_WINDOW;
            // Set on the Track BEFORE `.subgroups()` consumes it: `subgroups()`
            // inherits the window to bound local pruning, AND the publisher
            // session advertises it in SUBSCRIBE_OK (BLO-10339) so a downstream
            // relay mirror bounds its own retention to the same window.
            track_writer.set_history_window(history_window)?;
            let subgroups = track_writer
                .subgroups()
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
                let mut repair_writer =
                    tracks_writer.create(&fec.repair_track).ok_or_else(|| {
                        anyhow::anyhow!(
                        "TracksWriter::create returned None for `{}` (broadcast already closed?)",
                        fec.repair_track
                    )
                    })?;
                repair_writer.set_history_window(history_window)?;
                let repair_subgroups = repair_writer
                    .subgroups()
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

/// Drive the publisher loop until the input ends.
///
/// `tracks_writer` is held here only to keep the broadcast alive — once
/// dropped, TracksReader (held by `publisher.announce`) would see "done"
/// and close the session early.
async fn run_publisher(
    input: MmtpInput,
    udp_bind: std::net::SocketAddr,
    udp_source: Option<std::net::Ipv4Addr>,
    udp_iface: Option<std::net::Ipv4Addr>,
    mut router: Router,
    _tracks_writer: TracksWriter,
) -> Result<()> {
    match input {
        MmtpInput::Stdin => run_stdin_loop(&mut router).await,
        MmtpInput::Udp => run_udp_loop(udp_bind, udp_source, udp_iface, &mut router).await,
    }
}

/// Drive the publisher loop reading one packet/datagram per UDP datagram.
/// Per T4: the datagram boundary IS the framing — no length prefix. The
/// `Router` interprets each datagram per the catalog's packaging.
async fn run_udp_loop(
    bind: std::net::SocketAddr,
    source: Option<std::net::Ipv4Addr>,
    iface: Option<std::net::Ipv4Addr>,
    router: &mut Router,
) -> Result<()> {
    // open_udp_socket binds + (for multicast targets) joins the group
    // and enables loopback so cast/ffmpeg's multicast emission via
    // `moqenc_mmt` lands here without a separate flag. `source` selects a
    // source-specific (S,G) join for SSM groups (232.0.0.0/8); `iface`
    // (or a matching route) pins the join to the multicast-bearing NIC.
    let socket = udp::open_udp_socket(bind, source, iface).await?;
    tracing::info!(addr = %socket.local_addr()?, "listening for datagrams");
    // 65536 covers any IPv4/IPv6 MTU; oversized datagrams get truncated.
    let mut buf = vec![0u8; 65_536];
    let mut packet_count: u64 = 0;
    loop {
        recv_one_udp_packet(&socket, router, &mut buf).await?;
        packet_count = packet_count.wrapping_add(1);
        // `% == 0` is kept over `u64::is_multiple_of` (stable only since Rust
        // 1.87) to honor the repo's documented 1.70+ MSRV — same rationale as
        // moq-catalog's group-duration exactness check.
        #[allow(clippy::manual_is_multiple_of)]
        if packet_count % 1000 == 0 {
            tracing::debug!(packet_count, "UDP packets dispatched");
        }
    }
}

/// Receive one UDP datagram and hand it to the router. Extracted from the
/// loop body so unit tests can drive it with a single synthetic packet
/// rather than spawning the full loop.
async fn recv_one_udp_packet(
    socket: &tokio::net::UdpSocket,
    router: &mut Router,
    buf: &mut [u8],
) -> Result<()> {
    let (n, _addr) = socket.recv_from(buf).await.context("UDP recv_from error")?;
    if n == 0 {
        return Ok(());
    }
    router.handle(Bytes::copy_from_slice(&buf[..n]))?;
    Ok(())
}

async fn run_stdin_loop(router: &mut Router) -> Result<()> {
    let mut stdin = tokio::io::stdin();
    let mut packet_count: u64 = 0;
    loop {
        let frame = framing::read_one_frame(&mut stdin)
            .await
            .context("stdin framing error")?;
        let Some(packet) = frame else {
            // Clean EOF — flush a final ack so any wrappers know we're done.
            let _ = tokio::io::stdout().flush().await;
            tracing::info!(packet_count, "stdin EOF — publisher loop done");
            return Ok(());
        };
        // Move the frame body into Bytes so SubgroupWriter::write avoids a copy.
        router.handle(Bytes::from(packet))?;
        packet_count += 1;
    }
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
        // T4: each UDP datagram is one MMTP packet (no length prefix —
        // the datagram boundary IS the packet boundary). recv_one_udp_packet
        // must read one datagram and pass it through to dispatch.
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
        recv_one_udp_packet(&recv_sock, &mut router, &mut buf)
            .await
            .unwrap();

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
