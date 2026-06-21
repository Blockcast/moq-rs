// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Publisher core: per-track state and MMTP packet dispatch.
//
// The dispatch logic is extracted as a pure function abstracted over the
// moq-transport writer surface via the `TrackSubgroups` + `SubgroupWrite`
// traits. Tests use in-memory mocks; the runtime wires the real
// `moq_transport::serve::SubgroupsWriter` via the adapter impls at the
// bottom of this file.
//
// Spec invariants enforced (see .planning/moq-rs-m1-adr.md):
//   A1 — first packet of each new MPU MUST be `FragmentType::Init`
//        (the MPU metadata box). Caller's responsibility; we error if
//        violated rather than synthesize.
//   A2 — MPU sequence numbers within a track MUST be monotonically
//        non-decreasing. Regression is a hard error because
//        `moq_transport::serve::SubgroupsWriter::create` silently drops
//        subgroups whose group_id ≤ latest (subgroup.rs:116-128), which
//        would otherwise mask the bug.
//   A3 — `packet_id` MUST appear in the catalog's
//        `multicast.endpoints[].tracks[]` map; unknown ids hard-error.

use std::collections::HashMap;

use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use mmt_core::header::{FragmentType, PacketType};

/// AL-FEC source block size: K source symbols per FEC block.
///
/// K=32 × ~1416 B MTU ≈ 45 KB per block. Matches the encoder constant in
/// moq-rs/mmtp-fec and the receiver's mmt-core::FecDecoder::new(K).
const FEC_K: u32 = 32;

use crate::mmtp_parse::PacketRouting;

/// Object-level writer for one MoQ subgroup (≈ one MPU group).
///
/// Real impl: `moq_transport::serve::SubgroupWriter`. Test impl: an
/// in-memory `Vec<Bytes>` recorder.
pub trait SubgroupWrite {
    /// Append one full MMTP packet as a MoQ object on this subgroup.
    fn put_object(&mut self, payload: Bytes) -> Result<()>;
}

/// Subgroup factory for one MoQ track (≈ one MMTP packet_id).
///
/// Real impl: `moq_transport::serve::SubgroupsWriter`. Test impl: a
/// recorder of `(group_id, subgroup_id, priority)` create calls.
pub trait TrackSubgroups {
    /// The object-writer handed back by `create_group`.
    type Group: SubgroupWrite;

    /// Open a new subgroup with the given identity. For MMTP-on-MoQ
    /// we always pass `subgroup_id = 0` (one subgroup per MPU group).
    fn create_group(
        &mut self,
        group_id: u64,
        subgroup_id: u64,
        priority: u8,
    ) -> Result<Self::Group>;
}

/// Per-track runtime state.
///
/// One instance per catalog track keyed by MMTP packet_id. Holds the
/// open SubgroupsWriter (long-lived, owns the moq-transport track) plus
/// the currently open group (short-lived, replaced on each MPU advance).
pub struct TrackState<T: TrackSubgroups> {
    /// Catalog track name (informational, used for log context).
    pub name: String,
    /// Object-level priority (typically derived from track container).
    pub priority: u8,
    /// Subgroup factory wired to one moq-transport TrackWriter::subgroups().
    pub sink: T,
    /// The subgroup currently open for writes — None until the first
    /// MPU on this track. Replaced when `last_seen_mpu_seq` advances.
    pub current_group: Option<T::Group>,
    /// MPU sequence backing the currently open group; matches the
    /// group_id we passed to `create_group`.
    pub current_group_id: Option<u64>,
    /// Last MPU sequence seen on this track; used for the A2
    /// monotonicity check.
    pub last_seen_mpu_seq: Option<u32>,
    /// FEC Source Block Number of the most recently seen source packet
    /// that carried a SourceFecPayloadId (fec_type=1). Derived as
    /// `SourceFecPayloadId::sbn(FEC_K)`. None when the track does not
    /// carry FEC (fec_type=0 only). Used by the repair dispatcher (B2).
    pub current_sbn: Option<u32>,
    /// Sibling `<name>/repair` track for AL-FEC repair packets. Per
    /// draft-ramadan-moq-mmt §7.2 repair tracks run at priority 7 and
    /// inherit the source track's group_id so the receiver can
    /// correlate source/repair by MPU sequence. None means no FEC is
    /// configured for this packet_id (subscriber gets no recovery).
    pub repair: Option<RepairSink<T>>,
}

/// Repair sibling state for one source packet_id.
///
/// B2: repair subgroups are now keyed by FEC Source Block Number (SBN),
/// not by source MPU group_id. One repair subgroup per SBN so the receiver
/// can correlate repair symbols to exactly the K source symbols they protect
/// (draft-ramadan-moq-fec §6.1; K=`FEC_K`=32 per constants above).
pub struct RepairSink<T: TrackSubgroups> {
    /// Subgroup factory for the `<source>/repair` MoQ track.
    pub sink: T,
    /// Currently open repair subgroup — None before first repair on
    /// this track. Replaced when the SBN advances.
    pub current_group: Option<T::Group>,
    /// SBN of the currently open repair subgroup. Updated when a new
    /// SBN is seen (i.e. the FEC encoder moved to the next block).
    pub current_sbn: Option<u32>,
}

/// Dispatch one MMTP packet to its owning track.
///
/// `payload` is the full MMTP packet (header + body) — exactly what
/// the receiver will see when this lands as a MoQ object payload, per
/// the raw-passthrough container mode (see .planning/moq-rs-m1-adr.md
/// "MMTP framing — already done").
pub fn dispatch<T: TrackSubgroups>(
    state_map: &mut HashMap<u16, TrackState<T>>,
    routing: &PacketRouting,
    payload: Bytes,
) -> Result<()> {
    let state = state_map.get_mut(&routing.packet_id).ok_or_else(|| {
        anyhow!(
            "unknown packet_id {}: not in catalog multicast.endpoints[].tracks[]",
            routing.packet_id
        )
    })?;

    match routing.packet_type {
        PacketType::Mpu => {
            let mpu_seq = routing.mpu_sequence.ok_or_else(|| {
                anyhow!(
                    "packet_id {} is Mpu but mmtp_parse::route did not populate mpu_sequence",
                    routing.packet_id
                )
            })?;
            let frag = routing.fragment_type.ok_or_else(|| {
                anyhow!(
                    "packet_id {} is Mpu but mmtp_parse::route did not populate fragment_type",
                    routing.packet_id
                )
            })?;

            match state.last_seen_mpu_seq {
                Some(last) if mpu_seq < last => {
                    bail!(
                        "MPU sequence regression on packet_id {}: got mpu_seq={}, last seen {} \
                         (A2: moq-transport's SubgroupsWriter would silently drop this)",
                        routing.packet_id,
                        mpu_seq,
                        last
                    );
                }
                Some(last) if mpu_seq == last => {
                    let group = state.current_group.as_mut().ok_or_else(|| {
                        anyhow!(
                            "internal invariant: packet_id {} has last_seen_mpu_seq={} but no \
                             current_group",
                            routing.packet_id,
                            last
                        )
                    })?;
                    group.put_object(payload)?;
                }
                _ => {
                    // First-ever packet on the track OR mpu_seq > last.
                    // Either way, A1 requires FragmentType::Init for the
                    // first packet of a new MPU.
                    if frag != FragmentType::Init {
                        bail!(
                            "first packet of MPU mpu_seq={} on packet_id {} has \
                             fragment_type={:?}, expected Init (A1: MPU metadata must be \
                             object 0 of the new group)",
                            mpu_seq,
                            routing.packet_id,
                            frag
                        );
                    }
                    let group =
                        state
                            .sink
                            .create_group(mpu_seq as u64, 0, state.priority)?;
                    state.current_group = Some(group);
                    state.current_group_id = Some(mpu_seq as u64);
                    state.last_seen_mpu_seq = Some(mpu_seq);
                    state
                        .current_group
                        .as_mut()
                        .expect("just assigned")
                        .put_object(payload)?;
                }
            }
            // B2: update current_sbn for every source packet (both new-group
            // and continuation). The SBN can advance mid-MPU when a FEC block
            // boundary does not align with MPU boundaries (rare but spec-legal).
            // This single update covers both paths; the new-group branch above
            // does not need a separate update.
            if let Some(ref fec_id) = routing.source_fec_payload_id {
                state.current_sbn = Some(fec_id.sbn(FEC_K));
            }
        }
        PacketType::Repair => {
            // T3 / B2: route AL-FEC repair to the `<name>/repair` sibling
            // track at priority 7. B2: repair group_id = SBN (not source
            // MPU group_id), so the receiver can match repair symbols to
            // exactly the K source symbols in one FEC source block
            // (draft-ramadan-moq-fec §6.1, K=FEC_K=32).
            let repair_sbn = state.current_sbn.ok_or_else(|| {
                anyhow!(
                    "repair packet for packet_id {} arrived before any source FEC block \
                     (no SourceFecPayloadId seen; FEC encoder must emit source packets \
                     with fec_type=1 before repair symbols)",
                    routing.packet_id
                )
            })?;
            let repair = state.repair.as_mut().ok_or_else(|| {
                anyhow!(
                    "repair packet for packet_id {} but no /repair sibling track \
                     was registered for `{}` (build_state_map misconfiguration?)",
                    routing.packet_id,
                    state.name
                )
            })?;
            // Advance the repair subgroup when the SBN advances.
            if repair.current_sbn != Some(repair_sbn) {
                let group = repair.sink.create_group(repair_sbn as u64, 0, 7)?;
                repair.current_group = Some(group);
                repair.current_sbn = Some(repair_sbn);
            }
            repair
                .current_group
                .as_mut()
                .expect("just assigned or already open")
                .put_object(payload)?;
        }
        PacketType::Generic | PacketType::Control => {
            tracing::debug!(
                packet_id = routing.packet_id,
                packet_type = ?routing.packet_type,
                track = %state.name,
                "skipping non-media packet"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mmt_core::header::{FecType, SourceFecPayloadId};

    // ---- in-memory mocks ----

    #[derive(Default)]
    struct MockGroup {
        writes: Vec<Bytes>,
    }

    impl SubgroupWrite for MockGroup {
        fn put_object(&mut self, payload: Bytes) -> Result<()> {
            self.writes.push(payload);
            Ok(())
        }
    }

    #[derive(Default)]
    struct MockSubgroups {
        /// (group_id, subgroup_id, priority) recorded for each `create_group` call.
        groups_created: Vec<(u64, u64, u8)>,
    }

    impl TrackSubgroups for MockSubgroups {
        type Group = MockGroup;

        fn create_group(
            &mut self,
            group_id: u64,
            subgroup_id: u64,
            priority: u8,
        ) -> Result<Self::Group> {
            self.groups_created.push((group_id, subgroup_id, priority));
            Ok(MockGroup::default())
        }
    }

    // ---- test helpers ----

    fn make_state_map(packet_id: u16, priority: u8) -> HashMap<u16, TrackState<MockSubgroups>> {
        make_state_map_with_repair(packet_id, priority, false)
    }

    fn make_state_map_with_repair(
        packet_id: u16,
        priority: u8,
        with_repair: bool,
    ) -> HashMap<u16, TrackState<MockSubgroups>> {
        let repair = if with_repair {
            Some(RepairSink {
                sink: MockSubgroups::default(),
                current_group: None,
                current_sbn: None,
            })
        } else {
            None
        };
        let mut map = HashMap::new();
        map.insert(
            packet_id,
            TrackState {
                name: format!("track-{packet_id}"),
                priority,
                sink: MockSubgroups::default(),
                current_group: None,
                current_group_id: None,
                last_seen_mpu_seq: None,
                current_sbn: None,
                repair,
            },
        );
        map
    }

    /// Source MPU packet without FEC (fec_type=0). Use for non-FEC tests
    /// and for A1/A2/A3 invariant tests where FEC is not under test.
    fn mpu(packet_id: u16, mpu_seq: u32, frag: FragmentType) -> PacketRouting {
        PacketRouting {
            packet_id,
            packet_type: PacketType::Mpu,
            fec_type: 0,
            rap_flag: false,
            mpu_sequence: Some(mpu_seq),
            fragment_type: Some(frag),
            source_fec_payload_id: None,
        }
    }

    /// Source MPU packet with FEC (fec_type=1, SourceFecPayloadId.ss_id=`ss_id`).
    /// Use for B2 repair tests where SBN-keyed grouping is under test.
    fn mpu_with_fec(packet_id: u16, mpu_seq: u32, frag: FragmentType, ss_id: u32) -> PacketRouting {
        PacketRouting {
            packet_id,
            packet_type: PacketType::Mpu,
            fec_type: FecType::WithSourcePayloadId as u8,
            rap_flag: false,
            mpu_sequence: Some(mpu_seq),
            fragment_type: Some(frag),
            source_fec_payload_id: Some(SourceFecPayloadId { ss_id }),
        }
    }

    fn repair(packet_id: u16) -> PacketRouting {
        PacketRouting {
            packet_id,
            packet_type: PacketType::Repair,
            // Per ISO/IEC 23008-1 Table 8: FEC repair packets carry
            // fec_type = 2 (RepairMode0) or 3 (RepairMode1).
            fec_type: 2,
            rap_flag: false,
            mpu_sequence: None,
            fragment_type: None,
            source_fec_payload_id: None,
        }
    }

    // ---- 5 RED tests (per .planning/m1-next-session-prompt.md T1 STEP 3) ----

    #[test]
    fn unknown_packet_id_hard_errors() {
        // A3: packet_id not in catalog → hard error (no silent drop).
        let mut map = make_state_map(1, 5);
        let routing = mpu(99, 1, FragmentType::Init);
        let err = dispatch(&mut map, &routing, Bytes::from_static(b"x")).unwrap_err();
        assert!(
            err.to_string().contains("unknown packet_id"),
            "expected unknown packet_id err, got: {err}"
        );
    }

    #[test]
    fn first_packet_on_track_must_be_init() {
        // A1: the first MMTP packet of an MPU is FragmentType::Init
        // (MPU metadata box). Anything else is a caller bug.
        let mut map = make_state_map(1, 5);
        let routing = mpu(1, 1, FragmentType::Mfu);
        let err = dispatch(&mut map, &routing, Bytes::from_static(b"x")).unwrap_err();
        assert!(
            err.to_string().contains("Init"),
            "expected Init-required err, got: {err}"
        );
    }

    #[test]
    fn mpu_sequence_regression_hard_errors() {
        // A2: MPU sequence MUST be monotonically non-decreasing per
        // track. Regression is a hard error (the underlying writer
        // would silently drop, hiding the bug).
        let mut map = make_state_map(1, 5);
        dispatch(
            &mut map,
            &mpu(1, 10, FragmentType::Init),
            Bytes::from_static(b"init"),
        )
        .unwrap();
        let err = dispatch(
            &mut map,
            &mpu(1, 5, FragmentType::Init),
            Bytes::from_static(b"oops"),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("regression"),
            "expected MPU regression err, got: {err}"
        );
    }

    #[test]
    fn mpu_advance_opens_new_subgroup() {
        // On strictly-increasing MPU seq, dispatch MUST call
        // SubgroupsWriter::create({group_id: mpu_seq, subgroup_id: 0,
        // priority}) — NOT append() — per Codex #5.
        let mut map = make_state_map(1, 5);
        dispatch(
            &mut map,
            &mpu(1, 10, FragmentType::Init),
            Bytes::from_static(b"a"),
        )
        .unwrap();
        dispatch(
            &mut map,
            &mpu(1, 11, FragmentType::Init),
            Bytes::from_static(b"b"),
        )
        .unwrap();
        let state = map.get(&1).unwrap();
        assert_eq!(
            state.sink.groups_created,
            vec![(10, 0, 5), (11, 0, 5)],
            "expected two create_group calls with explicit group_ids"
        );
        assert_eq!(state.current_group_id, Some(11));
        assert_eq!(state.last_seen_mpu_seq, Some(11));
    }

    // ---- T3: FEC repair routing RED tests ----

    #[test]
    fn repair_packet_for_unknown_packet_id_errors() {
        // Repair for a packet_id not in the source map → hard error.
        let mut map = make_state_map_with_repair(1, 5, true);
        let err = dispatch(&mut map, &repair(99), Bytes::from_static(b"r")).unwrap_err();
        assert!(
            err.to_string().contains("unknown packet_id"),
            "got: {err}"
        );
    }

    #[test]
    fn repair_packet_for_track_without_repair_sibling_errors() {
        // packet_id is mapped, but the TrackState has no repair sibling.
        // M.1 design: catalog declares all source tracks, build_state_map
        // auto-creates `<name>/repair`. A missing repair sibling means a
        // misconfigured build_state_map call — surface it loudly.
        let mut map = make_state_map_with_repair(1, 5, false);
        // Open the source MPU with FEC first so we pass the "no FEC block"
        // error path and isolate the missing-repair-sibling check.
        dispatch(
            &mut map,
            &mpu_with_fec(1, 10, FragmentType::Init, 0),
            Bytes::from_static(b"i"),
        )
        .unwrap();
        let err = dispatch(&mut map, &repair(1), Bytes::from_static(b"r")).unwrap_err();
        assert!(
            err.to_string().contains("no /repair sibling")
                || err.to_string().contains("repair sibling"),
            "got: {err}"
        );
    }

    #[test]
    fn repair_before_source_fec_block_errors() {
        // Repair packet arrives before any source FEC block was seen on this
        // packet_id. B2: the publisher errors rather than write to an
        // ambiguous group — the receiver would have nothing to repair.
        let mut map = make_state_map_with_repair(1, 5, true);
        let err = dispatch(&mut map, &repair(1), Bytes::from_static(b"r")).unwrap_err();
        assert!(
            err.to_string().contains("before any source FEC block"),
            "got: {err}"
        );
    }

    #[test]
    fn repair_packet_routes_to_repair_sink_at_priority_7() {
        // Repair packets MUST land on the repair sibling sink (not the
        // source sink), with priority 7 per draft-ramadan-moq-mmt §7.2.
        // B2: repair group_id = SBN (ss_id=0 → SBN=0 with K=32).
        let mut map = make_state_map_with_repair(1, 5, true);
        // Source MPU 10, ss_id=0 → SBN=0.
        dispatch(
            &mut map,
            &mpu_with_fec(1, 10, FragmentType::Init, 0),
            Bytes::from_static(b"i"),
        )
        .unwrap();
        // Now a repair packet for the same packet_id.
        dispatch(&mut map, &repair(1), Bytes::from_static(b"r1")).unwrap();
        let state = map.get(&1).unwrap();
        // Source sink: 1 create_group at priority 5.
        assert_eq!(state.sink.groups_created, vec![(10, 0, 5)]);
        // Repair sink: 1 create_group at priority 7, group_id=SBN=0.
        let r = state.repair.as_ref().expect("repair sibling exists");
        assert_eq!(
            r.sink.groups_created,
            vec![(0, 0, 7)],
            "repair group_id is SBN (0); priority is 7"
        );
        assert_eq!(r.current_sbn, Some(0));
        // The repair payload landed on the repair group, not the source.
        let rg = r.current_group.as_ref().expect("repair group open");
        assert_eq!(rg.writes, vec![Bytes::from_static(b"r1")]);
    }

    #[test]
    fn repair_group_advances_with_source_block() {
        // B2: when the FEC source block advances (ss_id crosses a K=32
        // boundary), the next repair packet MUST open a new repair subgroup
        // keyed by the new SBN, NOT by the source MPU.
        //   ss_id 0  → SBN 0 (block 0: ss_ids 0..31)
        //   ss_id 32 → SBN 1 (block 1: ss_ids 32..63)
        let mut map = make_state_map_with_repair(1, 5, true);
        // Source packet in FEC block 0 (ss_id=0 → SBN=0).
        dispatch(
            &mut map,
            &mpu_with_fec(1, 10, FragmentType::Init, 0),
            Bytes::from_static(b"i10"),
        )
        .unwrap();
        dispatch(&mut map, &repair(1), Bytes::from_static(b"r_sbn0")).unwrap();
        // ss_id=31 is the last symbol of FEC block 0; SBN must still be 0.
        // Exercises the off-by-one boundary: 31 / 32 = 0 (integer division), not 1.
        dispatch(
            &mut map,
            &mpu_with_fec(1, 10, FragmentType::Mfu, 31),
            Bytes::from_static(b"m10"),
        )
        .unwrap();
        assert_eq!(
            map.get(&1).unwrap().current_sbn,
            Some(0),
            "ss_id=31 must remain in SBN=0 (off-by-one guard)"
        );
        // Source advances to FEC block 1 (ss_id=32 → SBN=1), new MPU 11.
        dispatch(
            &mut map,
            &mpu_with_fec(1, 11, FragmentType::Init, 32),
            Bytes::from_static(b"i11"),
        )
        .unwrap();
        dispatch(&mut map, &repair(1), Bytes::from_static(b"r_sbn1")).unwrap();
        let r = map.get(&1).unwrap().repair.as_ref().unwrap();
        assert_eq!(
            r.sink.groups_created,
            vec![(0, 0, 7), (1, 0, 7)],
            "repair opens a new group when SBN advances (0 → 1), keyed by SBN not MPU"
        );
        assert_eq!(r.current_sbn, Some(1));
    }

    #[test]
    fn repair_group_keyed_by_sbn_not_mpu_group() {
        // B2 RED→GREEN: repair group_id MUST be the SBN, not the source
        // MPU group_id. Two MPUs in the same FEC block (same SBN) MUST
        // share one repair subgroup; two MPUs in different FEC blocks MUST
        // each open their own repair subgroup.
        //
        // Scenario:
        //   MPU 10 → ss_id=5  → SBN=0 (5/32=0)
        //   MPU 11 → ss_id=10 → SBN=0 (10/32=0)  ← same block as MPU 10
        //   MPU 12 → ss_id=32 → SBN=1 (32/32=1)  ← new block
        //
        // Expected repair groups:  [(0,0,7), (1,0,7)]   (2 groups, by SBN)
        // Wrong old MPU behaviour: [(10,0,7),(11,0,7),(12,0,7)] (3 groups)
        let mut map = make_state_map_with_repair(1, 5, true);

        dispatch(
            &mut map,
            &mpu_with_fec(1, 10, FragmentType::Init, 5),
            Bytes::from_static(b"i10"),
        )
        .unwrap();
        dispatch(&mut map, &repair(1), Bytes::from_static(b"r_mpu10")).unwrap();

        dispatch(
            &mut map,
            &mpu_with_fec(1, 11, FragmentType::Init, 10),
            Bytes::from_static(b"i11"),
        )
        .unwrap();
        dispatch(&mut map, &repair(1), Bytes::from_static(b"r_mpu11")).unwrap();

        dispatch(
            &mut map,
            &mpu_with_fec(1, 12, FragmentType::Init, 32),
            Bytes::from_static(b"i12"),
        )
        .unwrap();
        dispatch(&mut map, &repair(1), Bytes::from_static(b"r_mpu12")).unwrap();

        let r = map.get(&1).unwrap().repair.as_ref().unwrap();
        assert_eq!(
            r.sink.groups_created,
            vec![(0, 0, 7), (1, 0, 7)],
            "repair subgroups keyed by SBN (2 groups), not by MPU sequence (3 groups)"
        );
        assert_eq!(r.current_sbn, Some(1), "current SBN is 1 after block advance");
    }

    #[test]
    fn equal_mpu_appends_to_current_subgroup() {
        // Same MPU sequence number → continue writing into the open
        // subgroup. No new create_group call.
        let mut map = make_state_map(1, 5);
        dispatch(
            &mut map,
            &mpu(1, 10, FragmentType::Init),
            Bytes::from_static(b"init"),
        )
        .unwrap();
        dispatch(
            &mut map,
            &mpu(1, 10, FragmentType::Mfu),
            Bytes::from_static(b"frame1"),
        )
        .unwrap();
        dispatch(
            &mut map,
            &mpu(1, 10, FragmentType::Mfu),
            Bytes::from_static(b"frame2"),
        )
        .unwrap();
        let state = map.get(&1).unwrap();
        assert_eq!(
            state.sink.groups_created,
            vec![(10, 0, 5)],
            "exactly one subgroup created for the single MPU"
        );
        let group = state.current_group.as_ref().expect("current_group is open");
        assert_eq!(group.writes.len(), 3);
        assert_eq!(group.writes[0], Bytes::from_static(b"init"));
        assert_eq!(group.writes[1], Bytes::from_static(b"frame1"));
        assert_eq!(group.writes[2], Bytes::from_static(b"frame2"));
    }

    #[test]
    fn fragmented_mfu_packets_share_one_subgroup_raw_passthrough() {
        // B1=C raw-passthrough fragmentation contract (BLO-8047 §B1):
        //
        // A logical MFU larger than the path MTU is split by the source
        // into N MMTP packets (ISO/IEC 23008-1:2023 §9.2.3.3). On the
        // wire we see: one Init packet carrying the MPU metadata box,
        // then N MFU packets with FragmentType=Mfu and
        // fragmentation_indicator ∈ {1=first, 2=middle, 3=last}, all
        // sharing the same mpu_sequence and an incrementing
        // fragment_counter.
        //
        // The PUBLISHER does NOT reassemble. Each MMTP packet — Init
        // included — lands as its own MoQ object in the single subgroup
        // keyed by (packet_id, mpu_sequence). The receiver reassembles
        // using `mmt-core::MfuReassembler` (vendored at
        // moq-pub-mmtp/vendor/mmt-core/src/reassembler.rs).
        //
        // Erroring on fragmentation here would reject every video stream
        // above 1080p audio (4K I-frames at MTU=1416 need 220-1100
        // fragments; 8K needs 750-2900). See BLO-8047 description for
        // the full MTU/I-frame fragmentation math.
        //
        // This test pins the contract end-to-end on the dispatch layer:
        // 4 packets at the same (packet_id, mpu_seq) → 1 create_group
        // call, 4 put_object writes preserved verbatim.
        let mut map = make_state_map(1, 5);
        let pkts: &[(FragmentType, &[u8])] = &[
            // Init: MPU metadata box (FI=0 implicit; PacketRouting carries
            // only fragment_type, not FI, by design).
            (FragmentType::Init, b"init-mpu-10"),
            // First MFU fragment (would carry FI=1 on the wire).
            (FragmentType::Mfu, b"mfu-fragment-1"),
            // Middle MFU fragment (FI=2 on the wire).
            (FragmentType::Mfu, b"mfu-fragment-2"),
            // Last MFU fragment (FI=3 on the wire). Reassembled by the
            // receiver into one logical MFU together with fragments 1-2.
            (FragmentType::Mfu, b"mfu-fragment-3"),
        ];
        for (frag, payload) in pkts {
            dispatch(&mut map, &mpu(1, 10, *frag), Bytes::copy_from_slice(payload))
                .expect("raw-passthrough must accept all fragments of one MPU");
        }
        let state = map.get(&1).unwrap();
        assert_eq!(
            state.sink.groups_created,
            vec![(10, 0, 5)],
            "exactly one subgroup created for the single MPU (group_id=mpu_seq=10, subgroup_id=0, priority=5)"
        );
        let group = state.current_group.as_ref().expect("current_group is open");
        assert_eq!(
            group.writes.len(),
            4,
            "all 4 MMTP packets (Init + 3 MFU fragments) land as separate MoQ objects"
        );
        // Payloads preserved verbatim — raw-passthrough means no
        // transformation between the inbound MMTP packet bytes and the
        // outbound MoQ object payload.
        assert_eq!(group.writes[0], Bytes::from_static(b"init-mpu-10"));
        assert_eq!(group.writes[1], Bytes::from_static(b"mfu-fragment-1"));
        assert_eq!(group.writes[2], Bytes::from_static(b"mfu-fragment-2"));
        assert_eq!(group.writes[3], Bytes::from_static(b"mfu-fragment-3"));
    }
}

// ---- moq-transport adapter (wired in main.rs, not used by tests) ----
//
// These impls bridge our generic dispatch fn to the concrete
// moq-transport writer types. They are deliberately thin — the
// transport-level error type is wrapped in anyhow so the dispatch
// fn signature stays generic across mocks and runtime.

impl SubgroupWrite for moq_transport::serve::SubgroupWriter {
    fn put_object(&mut self, payload: Bytes) -> Result<()> {
        moq_transport::serve::SubgroupWriter::write(self, payload)
            .map_err(|e| anyhow!("SubgroupWriter::write failed: {e}"))
    }
}

impl TrackSubgroups for moq_transport::serve::SubgroupsWriter {
    type Group = moq_transport::serve::SubgroupWriter;

    fn create_group(
        &mut self,
        group_id: u64,
        subgroup_id: u64,
        priority: u8,
    ) -> Result<Self::Group> {
        moq_transport::serve::SubgroupsWriter::create(
            self,
            moq_transport::serve::Subgroup {
                group_id,
                subgroup_id,
                priority,
            },
        )
        .map_err(|e| anyhow!("SubgroupsWriter::create failed: {e}"))
    }
}

// Suppress dead-code warnings for the bail!/anyhow! helpers that the
// stub doesn't yet use. Remove once dispatch is implemented.
#[allow(dead_code)]
const _: fn() = || {
    let _ = anyhow!("");
    let _: Result<()> = (|| bail!("x"))();
};
