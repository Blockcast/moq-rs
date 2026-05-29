// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use clap::Parser;
use moq_catalog::{Container, Root};
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

    // Publish the catalog JSON on the `.catalog` track. The returned
    // SubgroupsWriter is bound for the session's lifetime — dropping
    // it would surface as "catalog gone" to subscribers.
    let _catalog_subgroups = publish_catalog_track(&mut tracks_writer, &catalog_bytes)?;
    tracing::info!(
        bytes = catalog_bytes.len(),
        "posted catalog on `.catalog` track"
    );

    let tls = args.tls.load()?;
    let quic_endpoint = quic::Endpoint::new(quic::Config::new(args.bind, None, tls.clone())?)?;

    tracing::info!(url = %args.url, "connecting to relay");
    let (session, connection_id, transport) =
        quic_endpoint.client.connect(&args.url, None).await?;
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
    let multicast = catalog
        .multicast
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("catalog has no `multicast` extension — required for moq-pub-mmtp"))?;
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
            let priority = priority_for_container(catalog_track.container.as_ref());

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
            subgroups.set_history_window(history_window);

            // Auto-create the AL-FEC repair sibling. The track name
            // convention `<source>/repair` is publisher-internal per
            // draft-ramadan-moq-mmt §7.2; the catalog does not list it.
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
            repair_subgroups.set_history_window(history_window);

            map.insert(
                track_ref.packet_id,
                TrackState::new(
                    track_ref.name.clone(),
                    priority,
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

/// Publish the broadcast's catalog JSON on the `.catalog` track.
///
/// Per the IETF moq-catalog-format draft + Codex review #3: the
/// catalog is itself a MoQ track named `.catalog`, with the JSON body
/// posted as a single object on group 0. Priority 127 is the lowest
/// non-control value — receivers fetch it eagerly on JOIN but it must
/// not preempt media tracks.
///
/// Returns the `SubgroupsWriter` so the caller can retain it for the
/// session's lifetime; dropping it would close the track and surface
/// as "catalog gone" to subscribers.
fn publish_catalog_track(
    tracks_writer: &mut TracksWriter,
    catalog_bytes: &[u8],
) -> Result<SubgroupsWriter> {
    let track = tracks_writer
        .create(".catalog")
        .ok_or_else(|| anyhow::anyhow!("TracksWriter::create returned None for `.catalog`"))?;
    let mut subgroups = track
        .subgroups()
        .context("`.catalog` track: subgroups() failed")?;
    let mut subgroup = subgroups
        .create(moq_transport::serve::Subgroup {
            group_id: 0,
            subgroup_id: 0,
            priority: 127,
        })
        .context("`.catalog` SubgroupsWriter::create failed")?;
    subgroup
        .write(Bytes::copy_from_slice(catalog_bytes))
        .context("writing catalog JSON object failed")?;
    // Dropping `subgroup` here is intentional — the SubgroupObjectWriter
    // it produced internally has remain==0 (full payload written) so the
    // reader sees a complete object.
    drop(subgroup);
    Ok(subgroups)
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

/// Object-level priority for a source track keyed by container type.
///
/// Per draft-ramadan-moq-mmt §7.2:
///   - Source media (mmtp / mfu / isobmff / unset) → priority 0 (highest).
///   - FEC repair tracks → priority 7 (lower than source).
/// Repair sibling tracks (created in T3) bypass this fn and use 7 directly.
fn priority_for_container(container: Option<&Container>) -> u8 {
    match container {
        Some(Container::FecRepair) => 7,
        _ => 0,
    }
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
    let (n, _addr) = socket
        .recv_from(buf)
        .await
        .context("UDP recv_from error")?;
    if n == 0 {
        return Ok(());
    }
    let packet = &buf[..n];
    let routing = route(packet).context("MMTP header parse error (UDP)")?;
    dispatch(
        state_map,
        &routing,
        Bytes::copy_from_slice(packet),
    )?;
    Ok(())
}

async fn run_stdin_loop(
    state_map: &mut HashMap<u16, TrackState<SubgroupsWriter>>,
) -> Result<()> {
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
    use moq_catalog::{CommonTrackFields, SelectionParam, Track};
    use moq_transport::serve::Tracks;

    fn ns() -> TrackNamespace {
        TrackNamespace::from_utf8_path("test-broadcast")
    }

    fn track(name: &str, container: Option<Container>) -> Track {
        Track {
            name: name.into(),
            container,
            selection_params: SelectionParam::default(),
            ..Default::default()
        }
    }

    fn catalog_with(
        tracks: Vec<Track>,
        multicast: Option<MulticastConfig>,
    ) -> Root {
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

    #[test]
    fn priority_for_container_pins_spec() {
        // Per draft-ramadan-moq-mmt §7.2: source media gets priority 0,
        // FEC repair tracks get priority 7. If anyone moves these
        // numbers, the smoke test mlog will reveal it — pin them here.
        assert_eq!(priority_for_container(None), 0);
        assert_eq!(priority_for_container(Some(&Container::Mmtp)), 0);
        assert_eq!(priority_for_container(Some(&Container::Mfu)), 0);
        assert_eq!(priority_for_container(Some(&Container::Isobmff)), 0);
        assert_eq!(priority_for_container(Some(&Container::FecRepair)), 7);
    }

    fn expect_err(r: Result<HashMap<u16, TrackState<SubgroupsWriter>>>) -> anyhow::Error {
        match r {
            Err(e) => e,
            Ok(_) => panic!("expected Err, got Ok"),
        }
    }

    #[test]
    fn build_state_map_errors_when_no_multicast_extension() {
        let cat = catalog_with(vec![track("v", Some(Container::Mmtp))], None);
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
            vec![track("v", Some(Container::Mmtp))],
            Some(MulticastConfig::default()),
        );
        let (mut tw, _r, _rd) = Tracks::new(ns()).produce();
        let err = expect_err(build_state_map(&mut tw, &cat));
        assert!(err.to_string().contains("endpoints is missing"), "got: {err}");
    }

    #[test]
    fn build_state_map_errors_on_duplicate_packet_id() {
        let cat = catalog_with(
            vec![
                track("v", Some(Container::Mmtp)),
                track("a", Some(Container::Mmtp)),
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
            vec![track("v", Some(Container::Mmtp))],
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
            vec![track("v", Some(Container::Mmtp))],
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
    fn publish_catalog_track_registers_dotcatalog_track() {
        // T2: on startup, the publisher MUST create the special
        // `.catalog` track and post the full catalog JSON as a single
        // object on group 0 at priority 127. This test pins the track
        // registration; the byte-for-byte write contract is covered
        // end-to-end in T9 smoke.
        let cat = catalog_with(
            vec![track("v", Some(Container::Mmtp))],
            Some(MulticastConfig {
                endpoints: Some(vec![endpoint(vec![("v", 1)])]),
                network_source: None,
                subgroup_history_groups: Some(8),
            }),
        );
        let catalog_bytes = serde_json::to_vec(&cat).unwrap();
        let (mut tw, _r, mut tr) = Tracks::new(ns()).produce();
        // Retain the returned subgroups writer so the track stays open
        // during the assertion (TrackReader::is_closed would otherwise
        // observe writer-drop as stale).
        let _retained = publish_catalog_track(&mut tw, &catalog_bytes)
            .expect("publish_catalog_track returns Ok");

        let reader = tr
            .get_track_reader(&ns(), ".catalog")
            .expect(".catalog track is registered on the broadcast");
        assert_eq!(reader.name, ".catalog");
        assert!(!reader.is_closed(), ".catalog track is alive");
    }

    #[test]
    fn build_state_map_happy_path_with_two_tracks() {
        let cat = catalog_with(
            vec![
                track("v", Some(Container::Mmtp)),
                track("a", None), // no container → defaults to 0
            ],
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

    #[tokio::test]
    async fn udp_recv_dispatches_one_packet() {
        // T4: each UDP datagram is one MMTP packet (no length prefix —
        // the datagram boundary IS the packet boundary). recv_one_udp_packet
        // must read one datagram and pass it through to dispatch.
        let cat = catalog_with(
            vec![track("v", Some(Container::Mmtp))],
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
        let mut cat = catalog_with(vec![track("v", Some(Container::Mmtp))], None);
        cat.common_track_fields.namespace = Some("bbb".into());
        check_namespace_consistency(&cat, "bbb").expect("matching namespace is OK");
    }

    #[test]
    fn check_namespace_consistency_passes_when_no_common_namespace() {
        // Common has no namespace → publisher sets it from --name; OK.
        let cat = catalog_with(vec![track("v", Some(Container::Mmtp))], None);
        check_namespace_consistency(&cat, "anything").expect("no common namespace is OK");
    }

    #[test]
    fn check_namespace_consistency_errors_on_mismatch() {
        // commonTrackFields.namespace = "foo" but --name=bar → hard error.
        // Catches publisher misconfiguration where the broadcast name
        // and the embedded catalog namespace disagree.
        let mut cat = catalog_with(vec![track("v", Some(Container::Mmtp))], None);
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
            vec![track("v", Some(Container::Mmtp))],
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
