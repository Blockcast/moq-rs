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
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use mmt_core::header::{FragmentType, PacketType};

use crate::mmtp_parse::{MfuIdentity, PacketRouting};

pub const CONTROL_PRIORITY: u8 = 32;
pub const SOURCE_PRIORITY: u8 = 128;

pub type SharedPresentationEpoch = Arc<Mutex<Option<u32>>>;

#[derive(Clone, Copy)]
struct ActiveMfu {
    identity: MfuIdentity,
    last_fragment_counter: u8,
}

pub fn group_id_for_ticks(ticks: i64, group_duration_ticks: u64) -> u64 {
    if ticks < 0 {
        0
    } else {
        ticks as u64 / group_duration_ticks
    }
}

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
    /// Object-level priority for source objects on this track.
    pub priority: u8,
    /// Subgroup factory wired to one moq-transport TrackWriter::subgroups().
    pub sink: T,
    /// Formula-derived currently open MoQ group.
    pub current_group_id: Option<u64>,
    /// Last MPU sequence seen on this track; used for the A2
    /// monotonicity check.
    pub last_seen_mpu_seq: Option<u32>,
    /// Subgroup 0 of the current group: the FT=Init (MPU metadata) object,
    /// per draft-ramadan-moq-mmt §4.3. Opened lazily when the Init packet of
    /// the current group arrives. Reset on group advance.
    init_group: Option<T::Group>,
    /// MFU subgroups keyed by normative DU-header identity (§5.2.2).
    mfu_groups: HashMap<MfuIdentity, T::Group>,
    /// Fragment chains whose continuation packets omit the DU header. Timestamp
    /// is only a lookup for one active chain; collisions and counter gaps fail
    /// closed instead of guessing identity.
    active_mfus: HashMap<u32, ActiveMfu>,
    /// Next MFU subgroup_id to assign within the current group. Starts at 1
    /// (subgroup 0 is reserved for Init) and increments as new MFU timestamps
    /// are seen. Reset on group advance.
    next_mfu_subgroup_id: u64,
    timescale: u32,
    group_duration_ticks: u64,
    repair_group_depth: u64,
    presentation_epoch: SharedPresentationEpoch,
    max_unwrapped_ntp_ticks: Option<i64>,
    /// Catalog-declared repair track for AL-FEC repair packets. Per
    /// draft-ramadan-moq-fec §6.1 repair tracks run at priority 240 and
    /// use formula-group/interleave alignment so receivers can correlate the
    /// symbols with the source data they protect. None means no FEC is
    /// configured for this packet_id.
    pub repair: Option<RepairSink<T>>,
}

impl<T: TrackSubgroups> TrackState<T> {
    /// Create per-track state with no group open yet.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        priority: u8,
        timescale: u32,
        group_duration_ticks: u64,
        repair_group_depth: u64,
        presentation_epoch: SharedPresentationEpoch,
        sink: T,
        repair: Option<RepairSink<T>>,
    ) -> Self {
        Self {
            name,
            priority,
            sink,
            current_group_id: None,
            last_seen_mpu_seq: None,
            init_group: None,
            mfu_groups: HashMap::new(),
            active_mfus: HashMap::new(),
            next_mfu_subgroup_id: 1,
            timescale,
            group_duration_ticks,
            repair_group_depth,
            presentation_epoch,
            max_unwrapped_ntp_ticks: None,
            repair,
        }
    }

    fn formula_group_id(&mut self, timestamp: u32) -> Result<u64> {
        let mut epoch = self
            .presentation_epoch
            .lock()
            .map_err(|_| anyhow!("shared presentation epoch lock poisoned"))?;
        let start = *epoch.get_or_insert(timestamp);
        drop(epoch);

        const NTP_SHORT_MODULUS: i64 = 1_i64 << 32;
        let base = timestamp.wrapping_sub(start) as i64;
        let unwrapped = if let Some(reference) = self.max_unwrapped_ntp_ticks {
            let era = reference.div_euclid(NTP_SHORT_MODULUS);
            [era - 1, era, era + 1]
                .into_iter()
                .map(|candidate_era| base + candidate_era * NTP_SHORT_MODULUS)
                .min_by_key(|candidate| candidate.abs_diff(reference))
                .expect("three NTP-short unwrap candidates")
        } else {
            timestamp.wrapping_sub(start) as i32 as i64
        };
        self.max_unwrapped_ntp_ticks = Some(
            self.max_unwrapped_ntp_ticks
                .map_or(unwrapped, |current| current.max(unwrapped)),
        );
        let presentation_ticks = if unwrapped < 0 {
            0
        } else {
            ((unwrapped as u128 * self.timescale as u128) / 65_536) as i64
        };
        Ok(group_id_for_ticks(
            presentation_ticks,
            self.group_duration_ticks,
        ))
    }

    /// Advance to a new formula group: reset all per-group subgroup state. Dropping
    /// the old subgroup writers closes those subgroups (they are complete).
    fn advance_to_group(&mut self, group_id: u64) {
        self.current_group_id = Some(group_id);
        self.init_group = None;
        self.mfu_groups.clear();
        self.active_mfus.clear();
        self.next_mfu_subgroup_id = 1;
    }
}

/// Repair sibling state for one source packet_id.
///
/// Repair groups align with the source formula group and interleave depth D:
/// repair_group = floor(source_group / D).
pub struct RepairSink<T: TrackSubgroups> {
    /// Subgroup factory for the `<source>/repair` MoQ track.
    pub sink: T,
    /// Catalog priority for repair objects. The canonical profile requires 240.
    pub priority: u8,
    /// Currently open repair subgroup — None before first repair on
    /// this track. Replaced when source MPU advances.
    pub current_group: Option<T::Group>,
    /// Repair group_id mirroring the source track's current MPU group.
    pub current_group_id: Option<u64>,
}

/// Dispatch one MMTP packet to its owning track.
///
/// `payload` is the full MMTP packet (header + body) — exactly what
/// the receiver will see when this lands as a MoQ object payload, per
/// the raw MMTP passthrough mode (see .planning/moq-rs-m1-adr.md
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

            // Refuse aggregated MPU packets (multiple data units in one payload).
            // The moq_mmt muxer does not emit aggregation under Mapping B (one MFU
            // per packet), and dispatch keys a whole packet to a single subgroup by
            // its timestamp — an aggregate would silently mis-route its inner DUs.
            // Mirror the FT=Fragment refusal: error rather than guess.
            if routing.aggregation {
                bail!(
                    "packet_id {} carries an aggregated MPU (multiple data units in one \
                     payload), which the moq_mmt muxer does not emit under Mapping B; \
                     refusing to guess subgroup boundaries",
                    routing.packet_id
                );
            }

            // MPU sequence remains an independent monotonicity invariant. It is
            // explicitly forbidden as the MoQ Group number (§4.3).
            match state.last_seen_mpu_seq {
                Some(last) if mpu_seq < last => {
                    bail!(
                        "MPU sequence regression on packet_id {}: got mpu_seq={}, last seen {} \
                         (A2: MPU sequence must be monotonically non-decreasing per track)",
                        routing.packet_id,
                        mpu_seq,
                        last
                    );
                }
                _ => state.last_seen_mpu_seq = Some(mpu_seq),
            }

            let formula_group_id = state.formula_group_id(routing.timestamp)?;
            if state
                .current_group_id
                .is_some_and(|current| formula_group_id < current)
            {
                tracing::warn!(
                    packet_id = routing.packet_id,
                    formula_group_id,
                    current_group_id = state.current_group_id,
                    "dropping stale packet from a prior formula group"
                );
                return Ok(());
            }
            if state.current_group_id != Some(formula_group_id) {
                state.advance_to_group(formula_group_id);
            }

            let group_id = state
                .current_group_id
                .expect("advance_to_group sets current_group_id for every MPU group");

            // §5.2.2: subgroup 0 = MPU metadata; each MFU subgroup is keyed by
            // its timed (movie_fragment_sequence_number, sample_number) or
            // non-timed Item_ID identity from the DU header.
            match frag {
                FragmentType::Init => {
                    if state.init_group.is_none() {
                        let g = state.sink.create_group(group_id, 0, state.priority)?;
                        state.init_group = Some(g);
                    }
                    state
                        .init_group
                        .as_mut()
                        .expect("init_group just set")
                        .put_object(payload)?;
                }
                FragmentType::Mfu => {
                    let identity = match (routing.fragmentation_indicator, routing.mfu_identity) {
                        (0, Some(identity)) => identity,
                        (1, Some(identity)) => {
                            if state.active_mfus.contains_key(&routing.timestamp) {
                                bail!(
                                    "packet_id {} starts overlapping MFUs at timestamp {}; DU identity would be ambiguous",
                                    routing.packet_id,
                                    routing.timestamp
                                );
                            }
                            state.active_mfus.insert(
                                routing.timestamp,
                                ActiveMfu {
                                    identity,
                                    last_fragment_counter: routing.fragment_counter,
                                },
                            );
                            identity
                        }
                        (2 | 3, None) => {
                            let active = state.active_mfus.get_mut(&routing.timestamp).ok_or_else(|| {
                                anyhow!(
                                    "packet_id {} continuation MFU (FI={}) has no active DU-header identity for timestamp {}",
                                    routing.packet_id,
                                    routing.fragmentation_indicator,
                                    routing.timestamp
                                )
                            })?;
                            let expected = active.last_fragment_counter.wrapping_add(1);
                            if routing.fragment_counter != expected {
                                bail!(
                                    "packet_id {} continuation MFU counter gap at timestamp {}: got {}, expected {}",
                                    routing.packet_id,
                                    routing.timestamp,
                                    routing.fragment_counter,
                                    expected
                                );
                            }
                            active.last_fragment_counter = routing.fragment_counter;
                            let identity = active.identity;
                            if routing.fragmentation_indicator == 3 {
                                state.active_mfus.remove(&routing.timestamp);
                            }
                            identity
                        }
                        _ => {
                            bail!(
                                "packet_id {} MFU has inconsistent FI={} and DU-header identity presence",
                                routing.packet_id,
                                routing.fragmentation_indicator
                            );
                        }
                    };
                    if !state.mfu_groups.contains_key(&identity) {
                        let subgroup_id = state.next_mfu_subgroup_id;
                        let g = state
                            .sink
                            .create_group(group_id, subgroup_id, state.priority)?;
                        state.next_mfu_subgroup_id += 1;
                        state.mfu_groups.insert(identity, g);
                    }
                    state
                        .mfu_groups
                        .get_mut(&identity)
                        .expect("mfu subgroup just inserted")
                        .put_object(payload)?;
                }
                FragmentType::Fragment => {
                    bail!(
                        "packet_id {} carries MPU fragment_type=Fragment (moof), which the \
                         moq_mmt muxer does not emit on the multicast wire under Mapping B \
                         (only Init + MFU); refusing to guess its subgroup",
                        routing.packet_id
                    );
                }
            }
        }
        PacketType::Repair => {
            // Route AL-FEC repair to the catalog-declared track and priority.
            // D consecutive formula groups share one repair group, preserving
            // the SBN=floor(Group/D) alignment.
            let source_group_id = state.current_group_id.ok_or_else(|| {
                anyhow!(
                    "repair packet for packet_id {} arrived before any source MPU \
                     (publisher cannot pick a group_id; FEC encoder should emit \
                     source MPUs first)",
                    routing.packet_id
                )
            })?;
            let repair_group_id = source_group_id / state.repair_group_depth;
            let repair = state.repair.as_mut().ok_or_else(|| {
                anyhow!(
                    "repair packet for packet_id {} but no /repair sibling track \
                     was registered for `{}` (build_state_map misconfiguration?)",
                    routing.packet_id,
                    state.name
                )
            })?;
            if repair.current_group_id != Some(repair_group_id) {
                let group = repair
                    .sink
                    .create_group(repair_group_id, 0, repair.priority)?;
                repair.current_group = Some(group);
                repair.current_group_id = Some(repair_group_id);
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
                priority: 240,
                current_group: None,
                current_group_id: None,
            })
        } else {
            None
        };
        let mut map = HashMap::new();
        map.insert(
            packet_id,
            TrackState::new(
                format!("track-{packet_id}"),
                priority,
                65_536,
                65_536,
                1,
                Arc::new(Mutex::new(Some(0))),
                MockSubgroups::default(),
                repair,
            ),
        );
        map
    }

    fn mpu(packet_id: u16, mpu_seq: u32, frag: FragmentType) -> PacketRouting {
        mpu_ts(packet_id, mpu_seq, frag, 0)
    }

    /// Like `mpu` but with an explicit MMTP timestamp (the Mapping-B MFU key).
    fn mpu_ts(packet_id: u16, mpu_seq: u32, frag: FragmentType, timestamp: u32) -> PacketRouting {
        PacketRouting {
            packet_id,
            packet_type: PacketType::Mpu,
            fec_type: 0,
            rap_flag: false,
            mpu_sequence: Some(mpu_seq),
            fragment_type: Some(frag),
            timestamp: mpu_seq.wrapping_shl(16).wrapping_add(timestamp),
            timed: true,
            fragmentation_indicator: 0,
            fragment_counter: 0,
            mfu_identity: (frag == FragmentType::Mfu).then_some(MfuIdentity::Timed {
                movie_fragment_sequence_number: mpu_seq,
                sample_number: timestamp,
            }),
            aggregation: false,
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
            timestamp: 0,
            timed: false,
            fragmentation_indicator: 0,
            fragment_counter: 0,
            mfu_identity: None,
            aggregation: false,
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
    fn mfu_first_group_is_allowed_and_opens_mfu_subgroup() {
        // A1 relaxed under Mapping B: a group may start with an MFU (its Init may
        // be lost on the multicast leg and also rides the reliable catalog). The
        // MFU opens an MFU subgroup (>=1), NOT subgroup 0.
        let mut map = make_state_map(1, 5);
        dispatch(
            &mut map,
            &mpu_ts(1, 1, FragmentType::Mfu, 0x100),
            Bytes::from_static(b"frame"),
        )
        .unwrap();
        let state = map.get(&1).unwrap();
        assert_eq!(
            state.sink.groups_created,
            vec![(1, 1, 5)],
            "MFU opens subgroup 1 of group 1 (0 reserved for Init)"
        );
        assert!(
            state.init_group.is_none(),
            "no Init seen → subgroup 0 not opened"
        );
    }

    #[test]
    fn fragment_type_moof_hard_errors() {
        // The moq_mmt muxer never emits FT=Fragment (moof) on the multicast wire
        // under Mapping B; refuse rather than guess a subgroup.
        let mut map = make_state_map(1, 5);
        let err = dispatch(
            &mut map,
            &mpu_ts(1, 1, FragmentType::Fragment, 0x100),
            Bytes::from_static(b"moof"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("Fragment"), "got: {err}");
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
    fn init_subgroup_zero_mfus_subgroups_by_timestamp() {
        // Mapping B: Init -> subgroup 0; each distinct MFU timestamp -> a new
        // subgroup (1, 2, ...); the same timestamp reuses its subgroup.
        let mut map = make_state_map(1, 5);
        dispatch(
            &mut map,
            &mpu_ts(1, 10, FragmentType::Init, 0),
            Bytes::from_static(b"init"),
        )
        .unwrap();
        dispatch(
            &mut map,
            &mpu_ts(1, 10, FragmentType::Mfu, 0xA),
            Bytes::from_static(b"a0"),
        )
        .unwrap();
        dispatch(
            &mut map,
            &mpu_ts(1, 10, FragmentType::Mfu, 0xB),
            Bytes::from_static(b"b0"),
        )
        .unwrap();
        dispatch(
            &mut map,
            &mpu_ts(1, 10, FragmentType::Mfu, 0xA),
            Bytes::from_static(b"a1"),
        )
        .unwrap();
        let state = map.get(&1).unwrap();
        assert_eq!(
            state.sink.groups_created,
            vec![(10, 0, 5), (10, 1, 5), (10, 2, 5)],
            "Init=subgroup 0; MFU ts 0xA=subgroup 1; MFU ts 0xB=subgroup 2; ts 0xA reused"
        );
        assert_eq!(
            state.init_group.as_ref().unwrap().writes,
            vec![Bytes::from_static(b"init")]
        );
        assert_eq!(
            state.mfu_groups[&MfuIdentity::Timed {
                movie_fragment_sequence_number: 10,
                sample_number: 0xA,
            }]
                .writes,
            vec![Bytes::from_static(b"a0"), Bytes::from_static(b"a1")]
        );
        assert_eq!(
            state.mfu_groups[&MfuIdentity::Timed {
                movie_fragment_sequence_number: 10,
                sample_number: 0xB,
            }]
                .writes,
            vec![Bytes::from_static(b"b0")]
        );
    }

    #[test]
    fn mpu_advance_resets_subgroup_indexing() {
        // Advancing to a new group resets the MFU subgroup counter to 1 and
        // clears the previous group's subgroups.
        let mut map = make_state_map(1, 5);
        dispatch(
            &mut map,
            &mpu_ts(1, 10, FragmentType::Init, 0),
            Bytes::from_static(b"i10"),
        )
        .unwrap();
        dispatch(
            &mut map,
            &mpu_ts(1, 10, FragmentType::Mfu, 0xA),
            Bytes::from_static(b"a"),
        )
        .unwrap();
        dispatch(
            &mut map,
            &mpu_ts(1, 11, FragmentType::Init, 0),
            Bytes::from_static(b"i11"),
        )
        .unwrap();
        dispatch(
            &mut map,
            &mpu_ts(1, 11, FragmentType::Mfu, 0xC),
            Bytes::from_static(b"c"),
        )
        .unwrap();
        let state = map.get(&1).unwrap();
        assert_eq!(
            state.sink.groups_created,
            vec![(10, 0, 5), (10, 1, 5), (11, 0, 5), (11, 1, 5)],
            "each group restarts MFU subgroups at 1"
        );
        assert_eq!(state.current_group_id, Some(11));
        assert_eq!(state.last_seen_mpu_seq, Some(11));
        assert!(state.mfu_groups.contains_key(&MfuIdentity::Timed {
            movie_fragment_sequence_number: 11,
            sample_number: 0xC,
        }));
        assert!(
            !state.mfu_groups.contains_key(&MfuIdentity::Timed {
                movie_fragment_sequence_number: 10,
                sample_number: 0xA,
            }),
            "group 10's MFU subgroups cleared on advance to group 11"
        );
    }

    // ---- T3: FEC repair routing RED tests ----

    #[test]
    fn repair_packet_for_unknown_packet_id_errors() {
        // Repair for a packet_id not in the source map → hard error.
        let mut map = make_state_map_with_repair(1, 5, true);
        let err = dispatch(&mut map, &repair(99), Bytes::from_static(b"r")).unwrap_err();
        assert!(err.to_string().contains("unknown packet_id"), "got: {err}");
    }

    #[test]
    fn repair_packet_for_track_without_repair_sibling_errors() {
        // packet_id is mapped, but the TrackState has no repair sibling.
        // M.1 design: catalog declares all source tracks, build_state_map
        // auto-creates `<name>/repair`. A missing repair sibling means a
        // misconfigured build_state_map call — surface it loudly.
        let mut map = make_state_map_with_repair(1, 5, false);
        // Open the source MPU first so we know we are past the "no MPU"
        // error path — this isolates the missing-repair-sibling check.
        dispatch(
            &mut map,
            &mpu(1, 10, FragmentType::Init),
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
    fn repair_before_source_mpu_errors() {
        // Repair packet arrives before any source MPU was seen on this
        // packet_id. The publisher should error rather than write to an
        // ambiguous group — the receiver would have nothing to repair.
        let mut map = make_state_map_with_repair(1, 5, true);
        let err = dispatch(&mut map, &repair(1), Bytes::from_static(b"r")).unwrap_err();
        assert!(
            err.to_string().contains("before any source MPU")
                || err.to_string().contains("source MPU"),
            "got: {err}"
        );
    }

    #[test]
    fn repair_packet_routes_to_repair_sink_at_priority_240() {
        // Repair packets MUST land on the repair sibling sink (not the
        // source sink), with the canonical repair priority 240.
        let mut map = make_state_map_with_repair(1, 5, true);
        // Open source MPU 10 first.
        dispatch(
            &mut map,
            &mpu(1, 10, FragmentType::Init),
            Bytes::from_static(b"i"),
        )
        .unwrap();
        // Now a repair packet for the same packet_id.
        dispatch(&mut map, &repair(1), Bytes::from_static(b"r1")).unwrap();
        let state = map.get(&1).unwrap();
        // Source sink: 1 create_group at priority 5.
        assert_eq!(state.sink.groups_created, vec![(10, 0, 5)]);
        // Repair sink aligns with the source formula group at priority 240.
        let r = state.repair.as_ref().expect("repair sibling exists");
        assert_eq!(
            r.sink.groups_created,
            vec![(10, 0, 240)],
            "repair group aligns with source; priority is 240"
        );
        // The repair payload landed on the repair group, not the source.
        let rg = r.current_group.as_ref().expect("repair group open");
        assert_eq!(rg.writes, vec![Bytes::from_static(b"r1")]);
    }

    #[test]
    fn repair_group_advances_with_source_mpu() {
        // When the source MPU advances from 10 to 11, the next repair
        // packet on that packet_id MUST open a new repair group at 11.
        let mut map = make_state_map_with_repair(1, 5, true);
        dispatch(
            &mut map,
            &mpu(1, 10, FragmentType::Init),
            Bytes::from_static(b"i10"),
        )
        .unwrap();
        dispatch(&mut map, &repair(1), Bytes::from_static(b"r10")).unwrap();
        dispatch(
            &mut map,
            &mpu(1, 11, FragmentType::Init),
            Bytes::from_static(b"i11"),
        )
        .unwrap();
        dispatch(&mut map, &repair(1), Bytes::from_static(b"r11")).unwrap();
        let r = map.get(&1).unwrap().repair.as_ref().unwrap();
        assert_eq!(
            r.sink.groups_created,
            vec![(10, 0, 240), (11, 0, 240)],
            "repair opens a new group each time source MPU advances"
        );
        assert_eq!(r.current_group_id, Some(11));
    }

    #[test]
    fn mfu_fragments_same_timestamp_share_one_subgroup() {
        // A single MFU fragmented across MMTP packets (FI=1,2,3) carries the same
        // per-sample timestamp on every fragment, so all fragments land as
        // ordered objects in one subgroup — even though the publisher never reads
        // the Fragmentation Indicator (§5.1).
        let mut map = make_state_map(1, 5);
        dispatch(
            &mut map,
            &mpu_ts(1, 10, FragmentType::Init, 0),
            Bytes::from_static(b"init"),
        )
        .unwrap();
        dispatch(
            &mut map,
            &mpu_ts(1, 10, FragmentType::Mfu, 0xABCD),
            Bytes::from_static(b"f1"),
        )
        .unwrap();
        dispatch(
            &mut map,
            &mpu_ts(1, 10, FragmentType::Mfu, 0xABCD),
            Bytes::from_static(b"f2"),
        )
        .unwrap();
        dispatch(
            &mut map,
            &mpu_ts(1, 10, FragmentType::Mfu, 0xABCD),
            Bytes::from_static(b"f3"),
        )
        .unwrap();
        let state = map.get(&1).unwrap();
        assert_eq!(
            state.sink.groups_created,
            vec![(10, 0, 5), (10, 1, 5)],
            "one Init subgroup + one MFU subgroup for the fragmented frame"
        );
        let mfu = &state.mfu_groups[&MfuIdentity::Timed {
            movie_fragment_sequence_number: 10,
            sample_number: 0xABCD,
        }];
        assert_eq!(
            mfu.writes,
            vec![
                Bytes::from_static(b"f1"),
                Bytes::from_static(b"f2"),
                Bytes::from_static(b"f3"),
            ]
        );
    }

    #[test]
    fn mfu_without_du_header_identity_fails_closed() {
        // A continuation fragment cannot invent identity from its timestamp. If
        // the first fragment carrying the DU header was lost, fail closed.
        let mut map = make_state_map(1, 5);
        let mut continuation = mpu_ts(1, 10, FragmentType::Mfu, 0xABCD);
        continuation.fragmentation_indicator = 2;
        continuation.mfu_identity = None;
        let err = dispatch(&mut map, &continuation, Bytes::from_static(b"f2")).unwrap_err();
        assert!(err.to_string().contains("no active DU-header identity"));
    }

    #[test]
    fn aggregated_mpu_hard_errors() {
        // Aggregated MPU packets (multiple data units in one payload) are refused:
        // dispatch keys a whole packet to one timestamp subgroup, so an aggregate
        // would silently mis-route its inner DUs. Mirror the FT=Fragment refusal.
        let mut map = make_state_map(1, 5);
        let mut routing = mpu_ts(1, 1, FragmentType::Mfu, 0x100);
        routing.aggregation = true;
        let err = dispatch(&mut map, &routing, Bytes::from_static(b"agg")).unwrap_err();
        assert!(
            err.to_string().contains("aggregat"),
            "expected aggregated-MPU refusal, got: {err}"
        );
    }

    #[test]
    fn canonical_alignment_vectors_match_libmmt() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/assets/align.json");
        let raw = std::fs::read_to_string(path).expect("canonical align fixture present");
        let doc: serde_json::Value = serde_json::from_str(&raw).expect("valid align fixture");
        for vector in doc["group_id"].as_array().unwrap() {
            let actual = group_id_for_ticks(
                vector["ticks"].as_i64().unwrap(),
                vector["group_duration_ticks"].as_u64().unwrap(),
            );
            assert_eq!(
                actual,
                vector["expected_group_id"].as_u64().unwrap(),
                "{}",
                vector["name"].as_str().unwrap()
            );
        }
        for vector in doc["subgroup_key"].as_array().unwrap() {
            let actual = match vector["kind"].as_str().unwrap() {
                "mpu-metadata" => "metadata:0".to_string(),
                "timed" => format!(
                    "timed:{}:{}",
                    vector["movie_fragment_sequence_number"].as_u64().unwrap(),
                    vector["sample_number"].as_u64().unwrap()
                ),
                "non-timed" => {
                    format!("non-timed:{}", vector["item_id"].as_u64().unwrap())
                }
                kind => panic!("unknown subgroup vector kind {kind}"),
            };
            assert_eq!(
                actual,
                vector["expected_key"].as_str().unwrap(),
                "{}",
                vector["name"].as_str().unwrap()
            );
        }
    }

    #[test]
    fn continuation_fragments_repeat_and_reuse_du_header_identity() {
        let mut map = make_state_map(1, 5);
        let mut first = mpu_ts(1, 10, FragmentType::Mfu, 0xABCD);
        first.fragmentation_indicator = 1;
        dispatch(&mut map, &first, Bytes::from_static(b"first")).unwrap();
        let mut continuation = first;
        continuation.fragmentation_indicator = 3;
        continuation.fragment_counter = 1;
        continuation.mfu_identity = None;
        dispatch(&mut map, &continuation, Bytes::from_static(b"last")).unwrap();
        let group = &map.get(&1).unwrap().mfu_groups[&MfuIdentity::Timed {
            movie_fragment_sequence_number: 10,
            sample_number: 0xABCD,
        }];
        assert_eq!(
            group.writes,
            vec![Bytes::from_static(b"first"), Bytes::from_static(b"last")]
        );
    }

    #[test]
    fn distinct_du_identities_do_not_alias_equal_timestamps() {
        let mut map = make_state_map(1, 5);
        let mut first = mpu_ts(1, 10, FragmentType::Mfu, 0xABCD);
        first.mfu_identity = Some(MfuIdentity::Timed {
            movie_fragment_sequence_number: 7,
            sample_number: 19,
        });
        let mut second = first.clone();
        second.mfu_identity = Some(MfuIdentity::Timed {
            movie_fragment_sequence_number: 8,
            sample_number: 1,
        });
        dispatch(&mut map, &first, Bytes::from_static(b"a")).unwrap();
        dispatch(&mut map, &second, Bytes::from_static(b"b")).unwrap();
        assert_eq!(map.get(&1).unwrap().sink.groups_created.len(), 2);
    }

    #[test]
    fn overlapping_fragment_chains_at_equal_timestamp_fail_closed() {
        let mut map = make_state_map(1, 5);
        let mut first = mpu_ts(1, 10, FragmentType::Mfu, 0xABCD);
        first.fragmentation_indicator = 1;
        dispatch(&mut map, &first, Bytes::from_static(b"a-first")).unwrap();

        let mut overlapping = first.clone();
        overlapping.mfu_identity = Some(MfuIdentity::Timed {
            movie_fragment_sequence_number: 10,
            sample_number: 99,
        });
        let err = dispatch(&mut map, &overlapping, Bytes::from_static(b"b-first")).unwrap_err();
        assert!(err.to_string().contains("overlapping MFUs"));
    }

    #[test]
    fn repair_groups_follow_floor_group_over_depth() {
        let mut map = make_state_map_with_repair(1, 5, true);
        map.get_mut(&1).unwrap().repair_group_depth = 2;
        for group in [10, 11, 12] {
            dispatch(
                &mut map,
                &mpu(1, group, FragmentType::Init),
                Bytes::from_static(b"i"),
            )
            .unwrap();
            dispatch(&mut map, &repair(1), Bytes::from_static(b"r")).unwrap();
        }
        assert_eq!(
            map.get(&1)
                .unwrap()
                .repair
                .as_ref()
                .unwrap()
                .sink
                .groups_created,
            vec![(5, 0, 240), (6, 0, 240)]
        );
    }

    #[test]
    fn publisher_restart_with_high_mpu_sequence_starts_formula_at_zero() {
        let mut map = make_state_map(1, SOURCE_PRIORITY);
        let mut routing = mpu(1, 90_000, FragmentType::Init);
        routing.timestamp = 0;
        dispatch(&mut map, &routing, Bytes::from_static(b"init")).unwrap();
        assert_eq!(
            map.get(&1).unwrap().sink.groups_created,
            vec![(0, 0, SOURCE_PRIORITY)],
            "a publisher restart does not copy a retained high MPU sequence into Group"
        );
    }

    #[test]
    fn reordered_prior_group_packet_does_not_regress_writer_state() {
        let mut map = make_state_map(1, SOURCE_PRIORITY);
        let mut current = mpu_ts(1, 11, FragmentType::Mfu, 1);
        current.timestamp = 11 << 16;
        dispatch(&mut map, &current, Bytes::from_static(b"current")).unwrap();

        let mut stale = mpu_ts(1, 11, FragmentType::Mfu, 2);
        stale.timestamp = 10 << 16;
        dispatch(&mut map, &stale, Bytes::from_static(b"stale")).unwrap();

        let state = map.get(&1).unwrap();
        assert_eq!(state.current_group_id, Some(11));
        assert_eq!(state.sink.groups_created, vec![(11, 1, SOURCE_PRIORITY)]);
    }

    #[test]
    fn ntp_short_wrap_preserves_formula_progression() {
        let mut map = make_state_map(1, SOURCE_PRIORITY);
        let epoch = map.get(&1).unwrap().presentation_epoch.clone();
        *epoch.lock().unwrap() = Some(0xffff_0000);

        let mut before_wrap = mpu(1, 1, FragmentType::Init);
        before_wrap.timestamp = 0xffff_0000;
        dispatch(&mut map, &before_wrap, Bytes::from_static(b"before")).unwrap();

        let mut after_wrap = mpu_ts(1, 2, FragmentType::Mfu, 1);
        after_wrap.timestamp = 0x0001_0000;
        dispatch(&mut map, &after_wrap, Bytes::from_static(b"after")).unwrap();

        assert_eq!(
            map.get(&1).unwrap().sink.groups_created,
            vec![(0, 0, SOURCE_PRIORITY), (2, 1, SOURCE_PRIORITY)]
        );
    }

    fn hex_to_bytes(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn bytes_to_hex(bytes: &[u8]) -> String {
        let mut out = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            use std::fmt::Write;
            write!(&mut out, "{b:02x}").unwrap();
        }
        out
    }

    fn load_capture(path: &str) -> (serde_json::Value, Vec<Vec<u8>>) {
        let raw = std::fs::read_to_string(path).expect("capture fixture present");
        let doc: serde_json::Value = serde_json::from_str(&raw).expect("valid fixture JSON");
        let packets = doc["packets_hex"]
            .as_array()
            .expect("packets_hex array")
            .iter()
            .map(|h| hex_to_bytes(h.as_str().unwrap()))
            .collect();
        (doc, packets)
    }

    fn expected_groups(doc: &serde_json::Value, key: &str) -> Vec<(u64, u64, u8)> {
        doc["expected"][key]
            .as_array()
            .expect("expected group array")
            .iter()
            .map(|row| {
                let row = row.as_array().expect("group tuple");
                (
                    row[0].as_u64().unwrap(),
                    row[1].as_u64().unwrap(),
                    row[2].as_u64().unwrap() as u8,
                )
            })
            .collect()
    }

    // T1.7 stage 2: real FFmpeg moq_mmt multicast packets (captured on loopback;
    // MMTP+MPU headers verbatim) MUST flow through the Mapping-B dispatch without
    // error and produce the expected subgroup structure — subgroup 0 = Init,
    // subgroups 1..M = one MFU each, keyed by the per-sample MMTP timestamp. This
    // validates the dispatch against real muxer output, not just synthetic vectors.
    #[test]
    fn replays_real_moq_mmt_capture_into_mapping_b_subgroups() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/assets/moq_mmt_capture.json"
        );
        let (doc, packets) = load_capture(path);
        assert!(
            packets.len() >= 100,
            "expected the real capture (~120 packets), got {}",
            packets.len()
        );

        // The moq_mmt muxer's video packet_id is 1.
        let mut map = make_state_map(1, 5);
        for (i, pkt) in packets.iter().enumerate() {
            let routing = crate::mmtp_parse::route(pkt)
                .unwrap_or_else(|e| panic!("route() failed on real packet {i}: {e}"));
            dispatch(&mut map, &routing, Bytes::copy_from_slice(pkt))
                .unwrap_or_else(|e| panic!("dispatch() failed on real packet {i}: {e}"));
        }

        let state = map.get(&1).expect("packet_id 1 present");

        // Frozen oracle: the exact (group, subgroup, priority) sequence this real
        // capture must produce. Unlike the self-derived structural checks below,
        // this is a true regression gate — a muxer framing shift (different MFU or
        // group boundaries) changes these tuples and fails the test rather than
        // re-deriving a new "expected" from the shifted bytes.
        assert_eq!(
            state.sink.groups_created,
            expected_groups(&doc, "source_groups_created"),
            "FEC-off capture must produce the frozen group/subgroup structure"
        );

        // Group every create_group call by group_id. Per group, subgroup 0 (Init)
        // appears at most once; the MFU subgroups are contiguous 1..=M.
        use std::collections::BTreeMap;
        let mut by_group: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        for (g, sg, prio) in &state.sink.groups_created {
            assert_eq!(*prio, 5, "subgroups inherit the track priority");
            by_group.entry(*g).or_default().push(*sg);
        }
        assert!(
            by_group.len() >= 2,
            "expected >=2 MPU groups (group advance exercised), got {}",
            by_group.len()
        );

        let mut saw_init_subgroup_zero = false;
        let mut saw_multi_mfu_group = false;
        for (g, subs) in &by_group {
            let init_count = subs.iter().filter(|x| **x == 0).count();
            assert!(
                init_count <= 1,
                "group {g}: Init subgroup 0 created more than once"
            );
            if init_count == 1 {
                saw_init_subgroup_zero = true;
            }
            let mut mfus: Vec<u64> = subs.iter().copied().filter(|x| *x != 0).collect();
            mfus.sort_unstable();
            let expected: Vec<u64> = (1..=mfus.len() as u64).collect();
            assert_eq!(
                mfus, expected,
                "group {g}: MFU subgroups must be contiguous 1..=M"
            );
            if mfus.len() >= 2 {
                saw_multi_mfu_group = true;
            }
        }
        assert!(
            saw_init_subgroup_zero,
            "expected an Init object on subgroup 0"
        );
        assert!(
            saw_multi_mfu_group,
            "expected a group with multiple MFU subgroups"
        );

        // A fragmented MFU: some MFU subgroup of the final (still-open) group
        // received more than one object (its FI=1,2,..,3 fragments).
        let max_objs = state
            .mfu_groups
            .values()
            .map(|g| g.writes.len())
            .max()
            .unwrap_or(0);
        assert!(
            max_objs >= 2,
            "expected a fragmented MFU (>1 object in one subgroup), got max {max_objs}"
        );
    }

    #[test]
    fn fec_off_capture_has_no_source_fec_or_repair_packets() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/assets/moq_mmt_capture.json"
        );
        let (_doc, packets) = load_capture(path);
        assert!(
            packets.len() >= 100,
            "expected the real FEC-off capture (~120 packets), got {}",
            packets.len()
        );

        for (i, pkt) in packets.iter().enumerate() {
            let routing = crate::mmtp_parse::route(pkt)
                .unwrap_or_else(|e| panic!("route() failed on FEC-off packet {i}: {e}"));
            assert_ne!(
                routing.fec_type, 1,
                "FEC-off guard packet {i} unexpectedly carries source FEC"
            );
            assert_ne!(
                routing.packet_type,
                PacketType::Repair,
                "FEC-off guard packet {i} unexpectedly routes as repair"
            );
        }
    }

    #[test]
    fn replays_fec_on_capture_into_source_and_repair_tracks() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/assets/moq_mmt_fec_on_capture.json"
        );
        let (doc, packets) = load_capture(path);
        assert_eq!(packets.len(), 6, "fixture shape changed");

        let mut map = make_state_map_with_repair(1, 5, true);
        let mut source_trailers = Vec::new();
        let mut repair_payloads = Vec::new();

        for (i, pkt) in packets.iter().enumerate() {
            let routing = crate::mmtp_parse::route(pkt)
                .unwrap_or_else(|e| panic!("route() failed on FEC-on packet {i}: {e}"));
            match routing.packet_type {
                PacketType::Mpu => {
                    assert_eq!(
                        routing.fec_type, 1,
                        "FEC-on source packet {i} must carry source FEC"
                    );
                    let trailer = &pkt[pkt.len() - 4..];
                    source_trailers.push(bytes_to_hex(trailer));
                    assert_ne!(
                        &pkt[12..16],
                        trailer,
                        "SourceFecPayloadId must be a trailer, not an MPU prefix"
                    );
                }
                PacketType::Repair => {
                    assert_eq!(
                        routing.fec_type, 2,
                        "repair packet {i} must use RepairMode0 fec_type"
                    );
                    repair_payloads.push(bytes_to_hex(&pkt[12..]));
                }
                other => panic!("unexpected FEC-on packet type at {i}: {other:?}"),
            }
            dispatch(&mut map, &routing, Bytes::copy_from_slice(pkt))
                .unwrap_or_else(|e| panic!("dispatch() failed on FEC-on packet {i}: {e}"));
        }

        let state = map.get(&1).expect("packet_id 1 present");
        assert_eq!(
            state.sink.groups_created,
            expected_groups(&doc, "source_groups_created")
        );

        let repair = state.repair.as_ref().expect("repair sibling exists");
        assert_eq!(
            repair.sink.groups_created,
            expected_groups(&doc, "repair_groups_created")
        );
        assert_eq!(repair.current_group_id, Some(0));

        let expected_trailers: Vec<String> = doc["expected"]["source_trailers_hex"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(source_trailers, expected_trailers);

        let expected_repair_payloads: Vec<String> = doc["expected"]["repair_payloads_hex"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(repair_payloads, expected_repair_payloads);
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
