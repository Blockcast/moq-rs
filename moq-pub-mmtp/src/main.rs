// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use clap::Parser;
use moq_catalog::{Root, TrackPackaging};
use moq_native_ietf::quic;
use moq_transport::{
    coding::TrackNamespace,
    serve::{SubgroupsWriter, Tracks, TracksWriter},
    session::Publisher,
};
use tokio::io::AsyncWriteExt;

mod cli;
mod framing;
mod mmtp_parse;
mod publish;
mod udp;

use cli::{Args, MmtpInput};
use mmtp_parse::route;
use publish::{dispatch, RepairSink, TrackState};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,quinn=warn")),
        )
        .init();

    let args = Args::parse();

    // ---- catalog ----

    let catalog_bytes = tokio::fs::read(&args.catalog_json)
        .await
        .with_context(|| format!("reading catalog JSON {}", args.catalog_json.display()))?;
    let mut catalog: Root = serde_json::from_slice(&catalog_bytes)
        .with_context(|| format!("parsing catalog JSON {}", args.catalog_json.display()))?;

    // T5: library-level catalog validation (defense in depth — build_state_map
    // re-checks the publisher-relevant invariants at runtime).
    catalog
        .validate()
        .map_err(|e| anyhow::anyhow!("catalog validation failed: {e}"))?;
    check_namespace_consistency(&catalog, &args.name)?;
    catalog.expand_common_fields();

    // ---- moq-transport session ----

    let namespace = TrackNamespace::from_utf8_path(&args.name);
    let (mut tracks_writer, _request, tracks_reader) = Tracks::new(namespace).produce();

    // Build per-packet_id state map from the catalog's multicast extension.
    let state_map = build_state_map(&mut tracks_writer, &catalog)?;
    tracing::info!(
        track_count = state_map.len(),
        "built per-track state map from catalog"
    );

    // Publish the catalog JSON on the catalog tracks (canonical `catalog` per
    // draft-ietf-moq-msf-00 §5.2, plus the legacy `.catalog` alias). The
    // returned SubgroupsWriters are bound for the session's lifetime — dropping
    // one would surface as "catalog gone" to subscribers using that name.
    let _catalog_subgroups = publish_catalog_track(&mut tracks_writer, &catalog_bytes)?;
    tracing::info!(
        bytes = catalog_bytes.len(),
        tracks = ?CATALOG_TRACK_NAMES,
        "posted catalog on catalog tracks"
    );

    let tls = args.tls.load()?;
    let quic_endpoint = quic::Endpoint::new(quic::Config::new(args.bind, None, tls.clone())?)?;

    tracing::info!(url = %args.url, "connecting to relay");
    let (session, connection_id, transport) = quic_endpoint.client.connect(&args.url, None).await?;
    tracing::info!(%connection_id, "connected to relay");

    let (session, mut publisher) = Publisher::connect(session, transport)
        .await
        .context("failed to create MoQ Transport publisher")?;

    tokio::select! {
        res = session.run() => res.context("session error")?,
        res = publisher.announce(tracks_reader) => res.context("publisher error")?,
        res = run_publisher(args.mmtp_input, args.mmtp_udp_bind, state_map, tracks_writer) => res.context("publisher loop error")?,
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

            let track_writer = tracks_writer.create(&track_ref.name).ok_or_else(|| {
                anyhow::anyhow!(
                    "TracksWriter::create returned None for `{}` (broadcast already closed?)",
                    track_ref.name
                )
            })?;
            let mut subgroups = track_writer
                .subgroups()
                .with_context(|| format!("track `{}`: subgroups() failed", track_ref.name))?;

            // Config-or-throw: MMTP publishing under Mapping B opens many
            // concurrent subgroups per group (Init + one per MFU). The publisher
            // MUST bound retained history; there is no silent unbounded default.
            let history_window = multicast.subgroup_history_groups.ok_or_else(|| {
                anyhow::anyhow!(
                    "catalog multicast.subgroupHistoryGroups is required for MMTP publishing \
                     (config-or-throw): it bounds per-track subgroup memory under Mapping B"
                )
            })?;
            if history_window < 1 {
                bail!("multicast.subgroupHistoryGroups must be >= 1 (got {history_window})");
            }
            subgroups.set_history_window(history_window)?;

            // Auto-create the AL-FEC repair sibling. The track name
            // convention `<source>/repair` is publisher-internal per
            // draft-ramadan-moq-mmt §8.2 / draft-ramadan-moq-fec §6.1.
            let repair_name = format!("{}/repair", track_ref.name);
            let repair_writer = tracks_writer.create(&repair_name).ok_or_else(|| {
                anyhow::anyhow!(
                    "TracksWriter::create returned None for `{}` (broadcast already closed?)",
                    repair_name
                )
            })?;
            let mut repair_subgroups = repair_writer
                .subgroups()
                .with_context(|| format!("track `{repair_name}`: subgroups() failed"))?;
            repair_subgroups.set_history_window(history_window)?;

            map.insert(
                track_ref.packet_id,
                // Source tracks publish at priority 0; the old
                // priority_for_container() indirection died with the catalog
                // `container` field (repair siblings hardcode 7 in publish.rs).
                TrackState::new(
                    track_ref.name.clone(),
                    0,
                    subgroups,
                    Some(RepairSink {
                        sink: repair_subgroups,
                        current_group: None,
                        current_group_id: None,
                    }),
                ),
            );
        }
    }
    Ok(map)
}

/// Catalog track names. The broadcast's catalog JSON is published under each.
///
///   - `catalog` — the canonical, REQUIRED name. draft-ietf-moq-msf-00 §5.2:
///     "The catalog track MUST have a case-sensitive Track Name of `catalog`."
///     Shaka MSF subscribes to it (`lib/msf/msf_parser.js` `subscribeToCatalog_`).
///   - `.catalog` — a legacy compatibility alias for the moq.dev/hang
///     (WARP-lineage) ecosystem (moq-pub, moq-sub, gst-moq-pub subscribe to
///     `.catalog`). The dot-prefix is reserved at the *namespace* level by
///     moq-transport §3.2.1, so it is not idiomatic for a media track name; it
///     is kept only so non-MSF consumers still resolve the catalog.
///
/// `catalog` is listed first as the conformant name; drop `.catalog` once the
/// non-MSF consumers migrate.
const CATALOG_TRACK_NAMES: [&str; 2] = ["catalog", ".catalog"];

/// Publish the broadcast's catalog JSON on each catalog track name.
///
/// The JSON body is posted as a single object on group 0. Priority 127 is the
/// lowest non-control value — receivers fetch it eagerly on JOIN but it must
/// not preempt media tracks.
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
                priority: 127,
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
/// Catches a class of publisher misconfigurations where the catalog's
/// `commonTrackFields.namespace` disagrees with the broadcast name the
/// relay is announcing. If the common namespace is `None`, the
/// publisher is the source of truth and any name is acceptable.
fn check_namespace_consistency(catalog: &Root, name: &str) -> Result<()> {
    if let Some(ns) = &catalog.common_track_fields.namespace {
        if ns != name {
            bail!(
                "catalog namespace `{ns}` disagrees with broadcast --name `{name}`; \
                 either align commonTrackFields.namespace with --name or omit it from the catalog"
            );
        }
    }
    Ok(())
}

/// Drive the publisher dispatch loop until the input ends.
///
/// `tracks_writer` is held here only to keep the broadcast alive — once
/// dropped, TracksReader (held by `publisher.announce`) would see "done"
/// and close the session early.
async fn run_publisher(
    input: MmtpInput,
    udp_bind: std::net::SocketAddr,
    mut state_map: HashMap<u16, TrackState<SubgroupsWriter>>,
    _tracks_writer: TracksWriter,
) -> Result<()> {
    match input {
        MmtpInput::Stdin => run_stdin_loop(&mut state_map).await,
        MmtpInput::Udp => run_udp_loop(udp_bind, &mut state_map).await,
    }
}

/// Drive the publisher dispatch loop reading one MMTP packet per UDP
/// datagram. Per T4: each datagram is one MMTP packet — no length
/// prefix, because the datagram boundary IS the packet framing.
async fn run_udp_loop(
    bind: std::net::SocketAddr,
    state_map: &mut HashMap<u16, TrackState<SubgroupsWriter>>,
) -> Result<()> {
    // open_udp_socket binds + (for multicast targets) joins the group
    // and enables loopback so cast/ffmpeg's multicast emission via
    // `moqenc_mmt` lands here without a separate flag.
    let socket = udp::open_udp_socket(bind).await?;
    tracing::info!(addr = %socket.local_addr()?, "listening for MMTP datagrams");
    // 65536 covers any IPv4/IPv6 MTU; oversized datagrams get truncated.
    let mut buf = vec![0u8; 65_536];
    let mut packet_count: u64 = 0;
    loop {
        recv_one_udp_packet(&socket, state_map, &mut buf).await?;
        packet_count = packet_count.wrapping_add(1);
        if packet_count % 1000 == 0 {
            tracing::debug!(packet_count, "UDP packets dispatched");
        }
    }
}

/// Receive one UDP datagram, parse its MMTP header, dispatch to the
/// matching track. Extracted from the loop body so unit tests can
/// drive it with a single synthetic packet rather than spawning the
/// full loop.
async fn recv_one_udp_packet(
    socket: &tokio::net::UdpSocket,
    state_map: &mut HashMap<u16, TrackState<SubgroupsWriter>>,
    buf: &mut [u8],
) -> Result<()> {
    let (n, _addr) = socket.recv_from(buf).await.context("UDP recv_from error")?;
    if n == 0 {
        return Ok(());
    }
    let packet = &buf[..n];
    let routing = route(packet).context("MMTP header parse error (UDP)")?;
    dispatch(state_map, &routing, Bytes::copy_from_slice(packet))?;
    Ok(())
}

async fn run_stdin_loop(state_map: &mut HashMap<u16, TrackState<SubgroupsWriter>>) -> Result<()> {
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
        let routing = route(&packet).context("MMTP header parse error")?;
        // Move the frame body into Bytes so SubgroupWriter::write avoids a copy.
        let payload = Bytes::from(packet);
        dispatch(state_map, &routing, payload)?;
        packet_count += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moq_catalog::multicast::{MulticastConfig, MulticastEndpoint, MulticastTrackRef};
    use moq_catalog::{CommonTrackFields, MmtpMode, SelectionParam, Track};
    use moq_transport::serve::Tracks;

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
            selection_params: SelectionParam::default(),
            ..Default::default()
        }
    }

    fn catalog_with(tracks: Vec<Track>, multicast: Option<MulticastConfig>) -> Root {
        Root {
            version: 1,
            streaming_format: 1,
            streaming_format_version: "0.2".into(),
            streaming_delta_updates: true,
            common_track_fields: CommonTrackFields::default(),
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
            network_source: None,
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
                subgroup_history_groups: Some(8),
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
                subgroup_history_groups: Some(8),
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
    fn build_state_map_errors_when_subgroup_history_window_absent() {
        // config-or-throw: MMTP publishing requires multicast.subgroupHistoryGroups.
        // Mapping B opens many subgroups per group; there is no silent unbounded
        // default.
        let cat = catalog_with(
            vec![track("v", Some(TrackPackaging::Mmtp))],
            Some(MulticastConfig {
                endpoints: Some(vec![endpoint(vec![("v", 1)])]),
                network_source: None,
                subgroup_history_groups: None,
            }),
        );
        let (mut tw, _r, _rd) = Tracks::new(ns()).produce();
        let err = expect_err(build_state_map(&mut tw, &cat));
        assert!(
            err.to_string().contains("subgroupHistoryGroups"),
            "got: {err}"
        );
    }

    #[test]
    fn publish_catalog_track_registers_both_catalog_track_names() {
        // T2: on startup, the publisher MUST post the full catalog JSON as a
        // single object on group 0 at priority 127, under BOTH catalog track
        // names: `catalog` (canonical/REQUIRED per draft-ietf-moq-msf-00 §5.2;
        // what Shaka MSF subscribes to) and `.catalog` (legacy WARP-lineage
        // alias for non-MSF consumers: moq-pub, moq-sub, gst-moq-pub). This test
        // pins both registrations; the byte-for-byte write contract is covered
        // in T9 smoke.
        let cat = catalog_with(
            vec![track("v", Some(TrackPackaging::Mmtp))],
            Some(MulticastConfig {
                endpoints: Some(vec![endpoint(vec![("v", 1)])]),
                network_source: None,
                subgroup_history_groups: Some(8),
            }),
        );
        let catalog_bytes = serde_json::to_vec(&cat).unwrap();
        let (mut tw, _r, mut tr) = Tracks::new(ns()).produce();
        // Retain the returned subgroups writers so the tracks stay open during
        // the assertion (TrackReader::is_closed would otherwise observe
        // writer-drop as stale).
        let _retained = publish_catalog_track(&mut tw, &catalog_bytes)
            .expect("publish_catalog_track returns Ok");

        for name in [".catalog", "catalog"] {
            let reader = tr
                .get_track_reader(&ns(), name)
                .unwrap_or_else(|| panic!("`{name}` track is registered on the broadcast"));
            assert_eq!(reader.name, name);
            assert!(!reader.is_closed(), "`{name}` track is alive");
        }
    }

    #[test]
    fn build_state_map_happy_path_with_two_tracks() {
        let cat = catalog_with(
            vec![track("v", Some(TrackPackaging::Mmtp)), track("a", None)],
            Some(MulticastConfig {
                endpoints: Some(vec![endpoint(vec![("v", 17), ("a", 18)])]),
                network_source: None,
                subgroup_history_groups: Some(8),
            }),
        );
        let (mut tw, _r, _rd) = Tracks::new(ns()).produce();
        let map = build_state_map(&mut tw, &cat).unwrap();
        assert_eq!(map.len(), 2);
        let v = map.get(&17).expect("packet_id 17 present");
        assert_eq!(v.name, "v");
        assert_eq!(v.priority, 0);
        assert!(v.last_seen_mpu_seq.is_none());
        // T3: every source track gets an auto-created /repair sibling.
        assert!(v.repair.is_some(), "track `v` must have a repair sibling");
        let a = map.get(&18).expect("packet_id 18 present");
        assert_eq!(a.name, "a");
        assert_eq!(a.priority, 0);
        assert!(a.repair.is_some(), "track `a` must have a repair sibling");
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
                subgroup_history_groups: Some(8),
            }),
        );
        let (mut tw, _r, _rd) = Tracks::new(ns()).produce();
        let map = build_state_map(&mut tw, &cat).unwrap();
        assert_eq!(map.len(), 1);
        assert!(map.contains_key(&17));
        assert!(!map.contains_key(&18));
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
                subgroup_history_groups: Some(8),
            }),
        );
        let (mut tw, _r, _rd) = Tracks::new(ns()).produce();
        let mut state_map = build_state_map(&mut tw, &cat).unwrap();

        let recv_sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let recv_addr = recv_sock.local_addr().unwrap();
        let send_sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();

        // One MPU Init packet for packet_id=1, mpu_seq=42.
        let pkt = synth_mpu_init_packet(1, 42);
        send_sock.send_to(&pkt, recv_addr).await.unwrap();

        let mut buf = vec![0u8; 65_536];
        recv_one_udp_packet(&recv_sock, &mut state_map, &mut buf)
            .await
            .unwrap();

        let s = state_map.get(&1).expect("packet_id 1 present");
        assert_eq!(s.last_seen_mpu_seq, Some(42));
        assert_eq!(s.current_group_id, Some(42));
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
        cat.common_track_fields.namespace = Some("bbb".into());
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
        cat.common_track_fields.namespace = Some("foo".into());
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
    fn build_state_map_registers_repair_tracks_on_broadcast() {
        // Pin the naming convention: source `v` → repair track named
        // `v/repair`, reachable via TracksReader.get_track_reader.
        let cat = catalog_with(
            vec![track("v", Some(TrackPackaging::Mmtp))],
            Some(MulticastConfig {
                endpoints: Some(vec![endpoint(vec![("v", 17)])]),
                network_source: None,
                subgroup_history_groups: Some(8),
            }),
        );
        let (mut tw, _r, mut tr) = Tracks::new(ns()).produce();
        let _map = build_state_map(&mut tw, &cat).unwrap();
        let v = tr
            .get_track_reader(&ns(), "v")
            .expect("source track `v` registered");
        assert_eq!(v.name, "v");
        let v_repair = tr
            .get_track_reader(&ns(), "v/repair")
            .expect("repair track `v/repair` registered");
        assert_eq!(v_repair.name, "v/repair");
        assert!(!v_repair.is_closed(), "repair track is alive");
    }
}
