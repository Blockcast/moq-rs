// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! This module contains the structs and functions for the MoQ catalog format
/// The catalog format is a JSON file that describes the tracks available in a broadcast.
///
/// The current version of the catalog format is draft-01.
/// https://www.ietf.org/archive/id/draft-ietf-moq-catalogformat-01.html
use serde::{Deserialize, Serialize};

pub mod multicast;
pub use multicast::{MulticastConfig, MulticastEndpoint, MulticastTrackRef, NetworkSource};

#[derive(Serialize, Deserialize, Debug)]
pub struct Root {
    pub version: u16,

    #[serde(rename = "streamingFormat")]
    pub streaming_format: u16,

    #[serde(rename = "streamingFormatVersion")]
    pub streaming_format_version: String,

    #[serde(rename = "supportsDeltaUpdates")]
    pub streaming_delta_updates: bool,

    #[serde(rename = "commonTrackFields")]
    pub common_track_fields: CommonTrackFields,

    pub tracks: Vec<Track>,

    /// Optional multicast transport descriptor per draft-ramadan-moq-multicast.
    /// Omitted from serialized output when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multicast: Option<MulticastConfig>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Track {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    pub name: String,

    #[serde(rename = "initTrack", skip_serializing_if = "Option::is_none")]
    pub init_track: Option<String>,

    #[serde(rename = "initData", skip_serializing_if = "Option::is_none")]
    pub init_data: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub packaging: Option<TrackPackaging>,

    /// MMTP packetization mode per draft-ramadan-moq-mmt §12.1.
    #[serde(rename = "mmtpMode", skip_serializing_if = "Option::is_none")]
    pub mmtp_mode: Option<MmtpMode>,

    /// Media timescale in Hz (§4.4.2). REQUIRED for mmtp tracks — the catalog
    /// does not infer it. Inheritable via commonTrackFields.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timescale: Option<u32>,

    /// Group (GOP) duration in integer milliseconds (§4.4.2). REQUIRED for mmtp
    /// tracks unless `groupDurationTicks` is given; must satisfy
    /// `(groupDurationMs * timescale) % 1000 == 0`. Inheritable.
    #[serde(rename = "groupDurationMs", skip_serializing_if = "Option::is_none")]
    pub group_duration_ms: Option<u32>,

    /// Exact group duration in timescale ticks (§4.4.2). Optional override used
    /// when the integer-millisecond form cannot be exact; wins over
    /// `groupDurationMs` when both are present. Inheritable.
    #[serde(rename = "groupDurationTicks", skip_serializing_if = "Option::is_none")]
    pub group_duration_ticks: Option<u64>,

    /// Keyframe (GOP) repair cadence in integer milliseconds. The interval
    /// between consecutive video keyframes, used by receivers to derive a
    /// keyframe-loss repair timeout. Advisory and OPTIONAL. DISTINCT from
    /// `groupDurationMs`, which is the per-MFU MMTP media-group duration (one
    /// frame for frame-grouped streams), not a keyframe cadence. Inheritable.
    #[serde(rename = "keyframeIntervalMs", skip_serializing_if = "Option::is_none")]
    pub keyframe_interval_ms: Option<u32>,

    /// Exact keyframe interval in timescale ticks. Optional override used when the
    /// integer-millisecond form cannot be exact; wins over `keyframeIntervalMs`
    /// when both are present. Advisory; inheritable.
    #[serde(rename = "keyframeIntervalTicks", skip_serializing_if = "Option::is_none")]
    pub keyframe_interval_ticks: Option<u64>,

    /// MPU metadata delivery mode (§4.5); effective default is `inline`.
    /// Inheritable.
    #[serde(rename = "initMode", skip_serializing_if = "Option::is_none")]
    pub init_mode: Option<InitMode>,

    /// Per-track AL-FEC descriptor (§8.3 / moq-fec §5.1). When present, its
    /// `repairTrack` must name a catalog track with `fec-repair` packaging.
    /// Per-track; not inherited.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fec: Option<FecDescriptor>,

    /// MoQ delivery priority (base catalog field). Repair tracks SHOULD use a
    /// lower priority (e.g. 7) so repair data is dropped first under congestion
    /// (moq-fec §6.1). Per-track; not inherited.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,

    #[serde(rename = "renderGroup", skip_serializing_if = "Option::is_none")]
    pub render_group: Option<u16>,

    #[serde(rename = "altGroup", skip_serializing_if = "Option::is_none")]
    pub alt_group: Option<u16>,

    #[serde(rename = "selectionParams")]
    pub selection_params: SelectionParam,

    #[serde(rename = "temporalId", skip_serializing_if = "Option::is_none")]
    pub temporal_id: Option<u32>,

    #[serde(rename = "spatialId", skip_serializing_if = "Option::is_none")]
    pub spatial_id: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub depends: Option<Vec<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn track_packaging_default_is_cmaf() {
        let pkg = TrackPackaging::default();
        assert_eq!(pkg, TrackPackaging::Cmaf);
    }

    #[test]
    fn track_packaging_serializes_mmtp_values() {
        for (v, expected) in [
            (TrackPackaging::Cmaf, "\"cmaf\""),
            (TrackPackaging::Loc, "\"loc\""),
            (TrackPackaging::Mmtp, "\"mmtp\""),
            (TrackPackaging::FecRepair, "\"fec-repair\""),
            (TrackPackaging::Datagram, "\"datagram\""),
        ] {
            let json = serde_json::to_string(&v).unwrap();
            assert_eq!(json, expected, "serialize {v:?}");
            let back: TrackPackaging = serde_json::from_str(&json).unwrap();
            assert_eq!(back, v, "round-trip {v:?}");
        }
    }

    #[test]
    fn mmtp_mode_serializes_spec_values() {
        for (v, expected) in [(MmtpMode::Mpu, "\"mpu\""), (MmtpMode::Mfu, "\"mfu\"")] {
            let json = serde_json::to_string(&v).unwrap();
            assert_eq!(json, expected, "serialize {v:?}");
            let back: MmtpMode = serde_json::from_str(&json).unwrap();
            assert_eq!(back, v, "round-trip {v:?}");
        }
    }

    #[test]
    fn track_accepts_packaging_and_mmtp_mode_fields() {
        let json = r#"{
            "name": "video",
            "packaging": "mmtp",
            "mmtpMode": "mpu",
            "selectionParams": {"codec": "avc1.64001f"}
        }"#;
        let track: Track = serde_json::from_str(json).unwrap();
        assert_eq!(track.packaging, Some(TrackPackaging::Mmtp));
        assert_eq!(track.mmtp_mode, Some(MmtpMode::Mpu));
    }

    #[test]
    fn track_without_mmtp_mode_round_trips() {
        let t = Track {
            name: "v".into(),
            ..Default::default()
        };
        let json = serde_json::to_string(&t).unwrap();
        assert!(!json.contains("mmtpMode"), "json = {json}");
    }

    #[test]
    fn root_accepts_optional_multicast_field() {
        let json = r#"{
            "version": 1,
            "streamingFormat": 1,
            "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {},
            "tracks": [],
            "multicast": {
                "endpoints": [{
                    "protocol": "ssm",
                    "sourceAddress": "69.25.95.10",
                    "groupAddress": "232.0.10.1",
                    "port": 5004,
                    "tracks": [{"name":"v","packetId":1}]
                }]
            }
        }"#;
        let root: Root = serde_json::from_str(json).unwrap();
        let mc = root.multicast.expect("multicast field present");
        let eps = mc.endpoints.expect("endpoints present");
        assert_eq!(eps.len(), 1);
        assert_eq!(eps[0].group_address, "232.0.10.1");
        assert_eq!(eps[0].tracks[0].packet_id, 1);
    }

    #[test]
    fn root_without_multicast_round_trips() {
        // A pre-multicast-extension catalog must still round-trip cleanly.
        let json = r#"{
            "version": 1,
            "streamingFormat": 1,
            "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {},
            "tracks": []
        }"#;
        let root: Root = serde_json::from_str(json).unwrap();
        assert!(root.multicast.is_none());
        let back = serde_json::to_string(&root).unwrap();
        // Optional multicast must NOT appear when None.
        assert!(!back.contains("multicast"), "back = {back}");
    }

    #[test]
    fn track_packaging_round_trips_all_variants() {
        for (v, expected) in [
            (TrackPackaging::Cmaf, "\"cmaf\""),
            (TrackPackaging::Loc, "\"loc\""),
            (TrackPackaging::Mmtp, "\"mmtp\""),
            (TrackPackaging::FecRepair, "\"fec-repair\""),
            (TrackPackaging::Datagram, "\"datagram\""),
        ] {
            let json = serde_json::to_string(&v).unwrap();
            assert_eq!(json, expected, "serialize {v:?}");
            let back: TrackPackaging = serde_json::from_str(&json).unwrap();
            assert_eq!(back, v, "round-trip {v:?}");
        }
    }

    // ---- T5: Root::validate() tests ----
    //
    // Promotes the publisher-side guards in moq-pub-mmtp::build_state_map
    // to library-level validation so subscribers can reject malformed
    // catalogs without re-implementing the same checks. Publisher keeps
    // its own runtime guards as defense in depth.
    #[test]
    fn expand_common_fields_inherits_when_track_field_is_none() {
        // commonTrackFields supplies namespace + packaging + renderGroup.
        // The single track has none of those set; after expansion it
        // should carry all three from common.
        let json = r#"{
            "version": 1,
            "streamingFormat": 1,
            "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {
                "namespace": "ns-common",
                "packaging": "cmaf",
                "renderGroup": 1
            },
            "tracks": [
                {"name":"v","selectionParams":{}}
            ]
        }"#;
        let mut root: Root = serde_json::from_str(json).unwrap();
        assert_eq!(root.tracks[0].namespace, None);
        assert_eq!(root.tracks[0].packaging, None);
        assert_eq!(root.tracks[0].render_group, None);
        root.expand_common_fields();
        assert_eq!(root.tracks[0].namespace.as_deref(), Some("ns-common"));
        assert_eq!(root.tracks[0].packaging, Some(TrackPackaging::Cmaf));
        assert_eq!(root.tracks[0].render_group, Some(1));
    }

    #[test]
    fn expand_common_fields_preserves_track_level_overrides() {
        // Track-level value wins over common when both are set.
        let json = r#"{
            "version": 1,
            "streamingFormat": 1,
            "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {
                "namespace": "ns-common",
                "packaging": "cmaf"
            },
            "tracks": [
                {"name":"v","namespace":"ns-track","packaging":"loc","selectionParams":{}}
            ]
        }"#;
        let mut root: Root = serde_json::from_str(json).unwrap();
        root.expand_common_fields();
        assert_eq!(root.tracks[0].namespace.as_deref(), Some("ns-track"));
        assert_eq!(root.tracks[0].packaging, Some(TrackPackaging::Loc));
    }

    #[test]
    fn validate_accepts_fec_repair_in_catalog_tracks() {
        // Per draft-ramadan-moq-fec §5.2: repair tracks are catalog-signaled
        // for multicast. The publisher may still derive its own repair sibling.
        let json = r#"{
            "version": 1,
            "streamingFormat": 1,
            "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {},
            "tracks": [
                {"name":"v","packaging":"mmtp","mmtpMode":"mpu","timescale":90000,"groupDurationMs":1000,"selectionParams":{}},
                {"name":"v/repair","packaging":"fec-repair","selectionParams":{}}
            ]
        }"#;
        let root: Root = serde_json::from_str(json).unwrap();
        root.validate()
            .expect("FEC repair catalog tracks are valid");
    }

    #[test]
    fn validate_rejects_mmtp_track_without_mmtp_mode() {
        let json = r#"{
            "version": 1,
            "streamingFormat": 1,
            "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {},
            "tracks": [
                {"name":"v","packaging":"mmtp","selectionParams":{}}
            ]
        }"#;
        let root: Root = serde_json::from_str(json).unwrap();
        let err = root
            .validate()
            .expect_err("mmtpMode is required for mmtp tracks");
        assert!(
            matches!(&err, CatalogValidationError::MissingMmtpMode { track_name } if track_name == "v"),
            "expected MissingMmtpMode(v), got: {err:?}"
        );
    }

    #[test]
    fn validate_rejects_inherited_mmtp_packaging_without_mmtp_mode() {
        // 0a02292: packaging may be INHERITED from commonTrackFields. A track
        // with no per-track packaging is still effectively mmtp when common
        // says so, and §12.1's REQUIRED mmtpMode must apply to it — validate()
        // runs before expand_common_fields(), so it must consider the
        // effective (track-or-common) packaging, not just the track field.
        let json = r#"{
            "version": 1,
            "streamingFormat": 1,
            "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {"packaging": "mmtp"},
            "tracks": [
                {"name":"v","selectionParams":{}}
            ]
        }"#;
        let root: Root = serde_json::from_str(json).unwrap();
        let err = root
            .validate()
            .expect_err("inherited mmtp packaging still requires mmtpMode");
        assert!(
            matches!(&err, CatalogValidationError::MissingMmtpMode { track_name } if track_name == "v"),
            "expected MissingMmtpMode(v), got: {err:?}"
        );
    }

    #[test]
    fn validate_accepts_inherited_mmtp_packaging_with_track_mmtp_mode() {
        // Counterpart: inherited mmtp packaging + per-track mmtpMode is valid.
        let json = r#"{
            "version": 1,
            "streamingFormat": 1,
            "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {"packaging": "mmtp"},
            "tracks": [
                {"name":"v","mmtpMode":"mpu","timescale":90000,"groupDurationMs":1000,"selectionParams":{}}
            ]
        }"#;
        let root: Root = serde_json::from_str(json).unwrap();
        root.validate()
            .expect("inherited mmtp packaging with mmtpMode is valid");
    }

    #[test]
    fn expand_common_fields_inherits_mmtp_mode() {
        // mmtpMode set once in commonTrackFields is pushed down to a track that
        // omits it — the twin of packaging inheritance (BLO-10312).
        let json = r#"{
            "version": 1,
            "streamingFormat": 1,
            "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {"packaging": "mmtp", "mmtpMode": "mfu"},
            "tracks": [
                {"name":"v","selectionParams":{}}
            ]
        }"#;
        let mut root: Root = serde_json::from_str(json).unwrap();
        assert_eq!(root.tracks[0].mmtp_mode, None);
        root.expand_common_fields();
        assert_eq!(root.tracks[0].mmtp_mode, Some(MmtpMode::Mfu));
        assert_eq!(root.tracks[0].packaging, Some(TrackPackaging::Mmtp));
    }

    #[test]
    fn validate_accepts_track_inheriting_both_packaging_and_mmtp_mode() {
        // The point of BLO-10312: a single-mode catalog declares packaging +
        // mmtpMode once in common; the bare track validates on effective values.
        let json = r#"{
            "version": 1,
            "streamingFormat": 1,
            "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {"packaging": "mmtp", "mmtpMode": "mpu", "timescale": 90000, "groupDurationMs": 1000},
            "tracks": [
                {"name":"v","selectionParams":{}}
            ]
        }"#;
        let root: Root = serde_json::from_str(json).unwrap();
        root.validate()
            .expect("track inheriting both packaging and mmtpMode from common is valid");
    }

    #[test]
    fn from_tracks_keeps_heterogeneous_mmtp_mode() {
        // A real catalog mixes modes (video=mpu, audio=mfu). from_tracks must not
        // hoist mmtpMode to common (the values differ) and must leave each track's
        // value intact — otherwise the field is lost and validate() would then
        // reject the mmtp tracks for a missing mmtpMode.
        let json = r#"{
            "version": 1,
            "streamingFormat": 1,
            "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {},
            "tracks": [
                {"name":"v","packaging":"mmtp","mmtpMode":"mpu","selectionParams":{}},
                {"name":"a","packaging":"mmtp","mmtpMode":"mfu","selectionParams":{}}
            ]
        }"#;
        let mut root: Root = serde_json::from_str(json).unwrap();
        let common = CommonTrackFields::from_tracks(&mut root.tracks);
        assert_eq!(
            common.mmtp_mode, None,
            "differing mmtpMode must not be hoisted to common"
        );
        assert_eq!(
            root.tracks[0].mmtp_mode,
            Some(MmtpMode::Mpu),
            "video keeps mpu"
        );
        assert_eq!(
            root.tracks[1].mmtp_mode,
            Some(MmtpMode::Mfu),
            "audio keeps mfu"
        );
    }

    #[test]
    fn common_track_fields_round_trips_mmtp_mode() {
        let common: CommonTrackFields =
            serde_json::from_str(r#"{"packaging":"mmtp","mmtpMode":"mpu"}"#).unwrap();
        assert_eq!(common.mmtp_mode, Some(MmtpMode::Mpu));
        let back = serde_json::to_string(&common).unwrap();
        assert!(back.contains(r#""mmtpMode":"mpu""#), "json = {back}");
    }

    #[test]
    fn from_tracks_keeps_heterogeneous_packaging() {
        // Sibling of from_tracks_keeps_heterogeneous_mmtp_mode. A catalog mixing
        // packaging (video=mmtp, audio=cmaf) must not hoist packaging to common
        // (the values differ) and must leave each track's value intact. Before the
        // uniform `common.is_some()` strip guard, the per-track packaging was
        // stripped anyway and silently lost — never restored by expand. The same
        // guard now covers renderGroup/altGroup.
        let json = r#"{
            "version": 1,
            "streamingFormat": 1,
            "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {},
            "tracks": [
                {"name":"v","packaging":"mmtp","mmtpMode":"mpu","selectionParams":{}},
                {"name":"a","packaging":"cmaf","selectionParams":{}}
            ]
        }"#;
        let mut root: Root = serde_json::from_str(json).unwrap();
        let common = CommonTrackFields::from_tracks(&mut root.tracks);
        assert_eq!(
            common.packaging, None,
            "differing packaging must not be hoisted to common"
        );
        assert_eq!(
            root.tracks[0].packaging,
            Some(TrackPackaging::Mmtp),
            "video keeps mmtp"
        );
        assert_eq!(
            root.tracks[1].packaging,
            Some(TrackPackaging::Cmaf),
            "audio keeps cmaf"
        );
    }

    #[test]
    fn expand_common_fields_does_not_override_track_mmtp_mode() {
        // Track-level mmtpMode wins over common: expand_common_fields must NOT
        // clobber a track that declares its own value (the is_none() guard in
        // with_common). common=mpu, one track=mfu → that track keeps mfu.
        let json = r#"{
            "version": 1,
            "streamingFormat": 1,
            "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {"packaging": "mmtp", "mmtpMode": "mpu"},
            "tracks": [
                {"name":"v","selectionParams":{}},
                {"name":"a","mmtpMode":"mfu","selectionParams":{}}
            ]
        }"#;
        let mut root: Root = serde_json::from_str(json).unwrap();
        root.expand_common_fields();
        assert_eq!(
            root.tracks[0].mmtp_mode,
            Some(MmtpMode::Mpu),
            "v inherits common mpu"
        );
        assert_eq!(
            root.tracks[1].mmtp_mode,
            Some(MmtpMode::Mfu),
            "a keeps its own mfu (override wins over common)"
        );
    }

    #[test]
    fn from_tracks_then_expand_round_trips_mmtp_mode() {
        // Homogeneous hoist → expand is lossless: from_tracks strips a shared
        // mmtpMode into common (both tracks → None), and expand_common_fields
        // restores it per-track. Exercises the strip branch + the round-trip the
        // whole inheritance design relies on.
        let json = r#"{
            "version": 1,
            "streamingFormat": 1,
            "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {},
            "tracks": [
                {"name":"v","packaging":"mmtp","mmtpMode":"mpu","selectionParams":{}},
                {"name":"a","packaging":"mmtp","mmtpMode":"mpu","selectionParams":{}}
            ]
        }"#;
        let mut root: Root = serde_json::from_str(json).unwrap();
        root.common_track_fields = CommonTrackFields::from_tracks(&mut root.tracks);
        assert_eq!(
            root.common_track_fields.mmtp_mode,
            Some(MmtpMode::Mpu),
            "shared mpu hoisted to common"
        );
        assert_eq!(root.tracks[0].mmtp_mode, None, "stripped from track v");
        assert_eq!(root.tracks[1].mmtp_mode, None, "stripped from track a");
        root.expand_common_fields();
        assert_eq!(
            root.tracks[0].mmtp_mode,
            Some(MmtpMode::Mpu),
            "restored on v"
        );
        assert_eq!(
            root.tracks[1].mmtp_mode,
            Some(MmtpMode::Mpu),
            "restored on a"
        );
    }

    #[test]
    fn validate_rejects_unknown_track_reference() {
        // multicast.endpoints[].tracks[] references "video" but no
        // catalog.tracks[] entry has that name. The receiver would
        // be unable to find selectionParams; reject at the catalog
        // level.
        let json = r#"{
            "version": 1,
            "streamingFormat": 1,
            "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {},
            "tracks": [
                {"name":"v","selectionParams":{}}
            ],
            "multicast": {
                "endpoints": [{
                    "groupAddress": "232.0.1.1",
                    "port": 5004,
                    "tracks": [
                        {"name":"video","packetId":1}
                    ]
                }]
            }
        }"#;
        let root: Root = serde_json::from_str(json).unwrap();
        let err = root.validate().expect_err("unknown track ref must fail");
        assert!(
            matches!(&err, CatalogValidationError::UnknownTrackReference { track_name } if track_name == "video"),
            "expected UnknownTrackReference(video), got: {err:?}"
        );
    }

    #[test]
    fn validate_rejects_duplicate_packet_id() {
        // Two multicast track refs share packet_id=1. Per
        // draft-ramadan-moq-multicast §4.1: "Values MUST be unique
        // within an (sourceAddress, groupAddress, port) tuple."
        let json = r#"{
            "version": 1,
            "streamingFormat": 1,
            "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {},
            "tracks": [
                {"name":"v","selectionParams":{}},
                {"name":"a","selectionParams":{}}
            ],
            "multicast": {
                "endpoints": [{
                    "groupAddress": "232.0.1.1",
                    "port": 5004,
                    "tracks": [
                        {"name":"v","packetId":1},
                        {"name":"a","packetId":1}
                    ]
                }]
            }
        }"#;
        let root: Root = serde_json::from_str(json).unwrap();
        let err = root.validate().expect_err("duplicate packet_id must fail");
        assert!(
            matches!(
                err,
                CatalogValidationError::DuplicatePacketId { packet_id: 1, .. }
            ),
            "expected DuplicatePacketId, got: {err:?}"
        );
    }

    #[test]
    fn init_mode_defaults_to_inline() {
        assert_eq!(InitMode::default(), InitMode::Inline);
    }

    #[test]
    fn track_round_trips_full_mmt_fields() {
        let json = r#"{
            "name":"v","packaging":"mmtp","mmtpMode":"mfu",
            "timescale":90000,"groupDurationMs":2000,"groupDurationTicks":180000,
            "initMode":"track","priority":3,
            "fec":{"algorithm":"raptorq","sourceSymbols":32,"repairSymbols":8,
                   "interleaveDepthMs":4000,"symbolSize":1312,"repairTrack":"v/repair"},
            "selectionParams":{"codec":"avc1.42c01e"}
        }"#;
        let t: Track = serde_json::from_str(json).unwrap();
        assert_eq!(t.timescale, Some(90000));
        assert_eq!(t.group_duration_ms, Some(2000));
        assert_eq!(t.group_duration_ticks, Some(180000));
        assert_eq!(t.init_mode, Some(InitMode::Track));
        assert_eq!(t.priority, Some(3));
        let fec = t.fec.as_ref().expect("fec present");
        assert_eq!(fec.algorithm, FecAlgorithm::RaptorQ);
        assert_eq!(fec.source_symbols, 32);
        assert_eq!(fec.repair_symbols, 8);
        assert_eq!(fec.symbol_size, 1312);
        assert_eq!(fec.interleave_depth_ms, Some(4000));
        assert_eq!(fec.repair_track, "v/repair");
        // Round-trips back to the camelCase wire form.
        let back = serde_json::to_string(&t).unwrap();
        assert!(
            back.contains(r#""groupDurationTicks":180000"#),
            "json = {back}"
        );
        assert!(
            back.contains(r#""repairTrack":"v/repair""#),
            "json = {back}"
        );
    }

    // Serialization emits exactly the canonical ms key — and never the retired
    // pre-rename key — checked as serde_json::Value map keys, not substrings.
    #[test]
    fn fec_serializes_canonical_interleave_depth_ms_key_only() {
        let fec = FecDescriptor {
            algorithm: FecAlgorithm::RaptorQ,
            source_symbols: 32,
            repair_symbols: 8,
            symbol_size: 1312,
            interleave_depth_ms: Some(4000),
            repair_track: "v/repair".into(),
        };
        let v = serde_json::to_value(&fec).unwrap();
        let obj = v.as_object().expect("fec serializes to a JSON object");
        assert_eq!(
            obj.get("interleaveDepthMs"),
            Some(&serde_json::json!(4000)),
            "canonical key present, json = {v}"
        );
        assert!(
            !obj.contains_key("interleaveDepth"),
            "retired key must not be emitted, json = {v}"
        );
    }

    // None emits neither the canonical key nor the retired one.
    #[test]
    fn fec_none_interleave_depth_ms_emits_no_key() {
        let fec = FecDescriptor {
            algorithm: FecAlgorithm::RaptorQ,
            source_symbols: 32,
            repair_symbols: 8,
            symbol_size: 1312,
            interleave_depth_ms: None,
            repair_track: "v/repair".into(),
        };
        let v = serde_json::to_value(&fec).unwrap();
        let obj = v.as_object().expect("fec serializes to a JSON object");
        assert!(
            !obj.contains_key("interleaveDepthMs") && !obj.contains_key("interleaveDepth"),
            "absent interleave depth emits no key at all, json = {v}"
        );
    }

    #[test]
    fn expand_common_fields_inherits_new_mmt_fields() {
        // timescale + groupDurationMs + initMode declared once in common are
        // pushed down to a bare track (BLO-10313, same pattern as mmtpMode).
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {"timescale": 48000, "groupDurationMs": 500, "initMode": "track"},
            "tracks": [{"name":"a","selectionParams":{}}]
        }"#;
        let mut root: Root = serde_json::from_str(json).unwrap();
        root.expand_common_fields();
        assert_eq!(root.tracks[0].timescale, Some(48000));
        assert_eq!(root.tracks[0].group_duration_ms, Some(500));
        assert_eq!(root.tracks[0].init_mode, Some(InitMode::Track));
    }

    #[test]
    fn track_round_trips_keyframe_interval() {
        // keyframeIntervalMs is the keyframe/GOP repair cadence, DISTINCT from the
        // per-frame groupDurationMs (here 33 ms ≈ one frame at 30 fps vs a 1 s GOP).
        let json = r#"{
            "name":"v","packaging":"mmtp","mmtpMode":"mfu",
            "timescale":90000,"groupDurationMs":33,"groupDurationTicks":3000,
            "keyframeIntervalMs":1000,"keyframeIntervalTicks":90000,
            "selectionParams":{"codec":"avc1.42c01e"}
        }"#;
        let t: Track = serde_json::from_str(json).unwrap();
        assert_eq!(t.keyframe_interval_ms, Some(1000));
        assert_eq!(t.keyframe_interval_ticks, Some(90000));
        assert_eq!(t.group_duration_ms, Some(33));
        let back = serde_json::to_string(&t).unwrap();
        assert!(back.contains(r#""keyframeIntervalMs":1000"#), "json = {back}");
        assert!(back.contains(r#""keyframeIntervalTicks":90000"#), "json = {back}");
    }

    #[test]
    fn expand_common_fields_inherits_keyframe_interval() {
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {"timescale": 90000, "groupDurationMs": 33, "keyframeIntervalMs": 1000},
            "tracks": [{"name":"v","selectionParams":{}}]
        }"#;
        let mut root: Root = serde_json::from_str(json).unwrap();
        root.expand_common_fields();
        assert_eq!(root.tracks[0].keyframe_interval_ms, Some(1000));
    }

    #[test]
    fn validate_rejects_zero_keyframe_interval() {
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [{"name":"v","packaging":"mmtp","mmtpMode":"mpu","timescale":90000,"groupDurationMs":1000,"keyframeIntervalMs":0,"selectionParams":{}}]
        }"#;
        let err = serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect_err("zero keyframe interval is bad init");
        assert!(
            matches!(&err, CatalogValidationError::ZeroKeyframeInterval { track_name } if track_name == "v"),
            "got: {err:?}"
        );
    }

    #[test]
    fn keyframe_interval_is_optional() {
        // Advisory field: an mmtp track with no keyframe cadence still validates
        // (status quo — keyframe repair simply stays disabled receiver-side).
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [{"name":"v","packaging":"mmtp","mmtpMode":"mpu","timescale":90000,"groupDurationMs":1000,"selectionParams":{}}]
        }"#;
        serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect("keyframe interval is optional");
    }

    #[test]
    fn keyframe_interval_round_trips_fractional_cadence() {
        // Fractional cadence: 14700 ticks / 44100 Hz = 333.333… ms, which has no
        // exact integer-ms form. The advisory keyframeIntervalMs carries the
        // rounded value (333) while keyframeIntervalTicks pins the exact cadence.
        // Unlike groupDurationMs, the keyframe interval has NO §4.4.2 exactness
        // rule, so this validates as-is — the rounded ms never has to be exact.
        // Pins the serialize/round path so the rounded ms + exact ticks both ship.
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [{"name":"v","packaging":"mmtp","mmtpMode":"mfu","timescale":44100,"groupDurationMs":333,"groupDurationTicks":14700,"keyframeIntervalMs":333,"keyframeIntervalTicks":14700,"selectionParams":{}}]
        }"#;
        let root: Root = serde_json::from_str(json).unwrap();
        root.validate()
            .expect("fractional keyframe cadence has no exactness rule");
        let t = &root.tracks[0];
        assert_eq!(t.keyframe_interval_ms, Some(333), "rounded ms preserved");
        assert_eq!(
            t.keyframe_interval_ticks,
            Some(14700),
            "exact tick cadence preserved"
        );
        let back = serde_json::to_string(t).unwrap();
        assert!(
            back.contains(r#""keyframeIntervalMs":333"#),
            "rounded ms round-trips: {back}"
        );
        assert!(
            back.contains(r#""keyframeIntervalTicks":14700"#),
            "exact ticks round-trip: {back}"
        );
    }

    #[test]
    fn validate_rejects_mmtp_without_timescale() {
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [{"name":"v","packaging":"mmtp","mmtpMode":"mpu","groupDurationMs":1000,"selectionParams":{}}]
        }"#;
        let err = serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect_err("mmtp requires timescale");
        assert!(
            matches!(&err, CatalogValidationError::MissingTimescale { track_name } if track_name == "v"),
            "got: {err:?}"
        );
    }

    #[test]
    fn validate_rejects_mmtp_without_group_duration() {
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [{"name":"v","packaging":"mmtp","mmtpMode":"mpu","timescale":90000,"selectionParams":{}}]
        }"#;
        let err = serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect_err("mmtp requires a group duration");
        assert!(
            matches!(&err, CatalogValidationError::MissingGroupDuration { track_name } if track_name == "v"),
            "got: {err:?}"
        );
    }

    #[test]
    fn validate_rejects_inexact_group_duration() {
        // 333ms * 44100Hz = 14_685_300 ticks → not a whole-ms tick count.
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [{"name":"a","packaging":"mmtp","mmtpMode":"mfu","timescale":44100,"groupDurationMs":333,"selectionParams":{}}]
        }"#;
        let err = serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect_err("inexact groupDurationMs must be rejected");
        assert!(
            matches!(&err, CatalogValidationError::GroupDurationNotExact { track_name, .. } if track_name == "a"),
            "got: {err:?}"
        );
    }

    #[test]
    fn validate_accepts_inexact_group_duration_with_ticks_override() {
        // Same inexact ms, but an explicit groupDurationTicks override is valid.
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [{"name":"a","packaging":"mmtp","mmtpMode":"mfu","timescale":44100,"groupDurationMs":333,"groupDurationTicks":14700,"selectionParams":{}}]
        }"#;
        serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect("ticks override bypasses the ms exactness rule");
    }

    #[test]
    fn validate_accepts_fec_with_valid_repair_track() {
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [
                {"name":"v","packaging":"mmtp","mmtpMode":"mpu","timescale":90000,"groupDurationMs":1000,
                 "fec":{"algorithm":"raptorq","sourceSymbols":32,"repairSymbols":8,"symbolSize":1312,"repairTrack":"v/repair"},
                 "selectionParams":{}},
                {"name":"v/repair","packaging":"fec-repair","selectionParams":{}}
            ]
        }"#;
        serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect("fec referencing a fec-repair track is valid");
    }

    #[test]
    fn validate_rejects_fec_with_dangling_repair_track() {
        // repairTrack names a track that is not present / not fec-repair.
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [
                {"name":"v","packaging":"mmtp","mmtpMode":"mpu","timescale":90000,"groupDurationMs":1000,
                 "fec":{"algorithm":"raptorq","sourceSymbols":32,"repairSymbols":8,"symbolSize":1312,"repairTrack":"v/missing"},
                 "selectionParams":{}}
            ]
        }"#;
        let err = serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect_err("dangling fec.repairTrack must be rejected");
        assert!(
            matches!(&err, CatalogValidationError::UnknownRepairTrack { repair_track, .. } if repair_track == "v/missing"),
            "got: {err:?}"
        );
    }

    #[test]
    fn validate_rejects_zero_timescale() {
        // A zero timescale is bad init: tick math is undefined and (ms × 0) % 1000
        // is always 0, which would silently neutralize the exactness check.
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [{"name":"v","packaging":"mmtp","mmtpMode":"mpu","timescale":0,"groupDurationMs":1000,"selectionParams":{}}]
        }"#;
        let err = serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect_err("timescale 0 must be rejected");
        assert!(
            matches!(&err, CatalogValidationError::ZeroTimescale { track_name } if track_name == "v"),
            "got: {err:?}"
        );
    }

    #[test]
    fn validate_rejects_zero_group_duration() {
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [{"name":"v","packaging":"mmtp","mmtpMode":"mpu","timescale":90000,"groupDurationMs":0,"selectionParams":{}}]
        }"#;
        let err = serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect_err("zero groupDurationMs must be rejected");
        assert!(
            matches!(&err, CatalogValidationError::ZeroGroupDuration { track_name } if track_name == "v"),
            "got: {err:?}"
        );
    }

    #[test]
    fn validate_rejects_fec_zero_symbols() {
        // An active FEC scheme with zero repair symbols repairs nothing, and a
        // multicast receiver has no back channel to renegotiate — reject it.
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [
                {"name":"v","packaging":"mmtp","mmtpMode":"mpu","timescale":90000,"groupDurationMs":1000,
                 "fec":{"algorithm":"raptorq","sourceSymbols":32,"repairSymbols":0,"symbolSize":1312,"repairTrack":"v/repair"},
                 "selectionParams":{}},
                {"name":"v/repair","packaging":"fec-repair","selectionParams":{}}
            ]
        }"#;
        let err = serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect_err("zero repairSymbols on an active scheme must be rejected");
        assert!(
            matches!(&err, CatalogValidationError::InvalidFecParams { track_name, .. } if track_name == "v"),
            "got: {err:?}"
        );
    }

    #[test]
    fn from_tracks_keeps_heterogeneous_timescale() {
        // Sibling of from_tracks_keeps_heterogeneous_mmtp_mode for the §4.4.2
        // fields: tracks disagree on timescale → not hoisted, each kept (the
        // uniform common.is_some() strip guard, BLO-10446).
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [
                {"name":"v","timescale":90000,"selectionParams":{}},
                {"name":"a","timescale":48000,"selectionParams":{}}
            ]
        }"#;
        let mut root: Root = serde_json::from_str(json).unwrap();
        let common = CommonTrackFields::from_tracks(&mut root.tracks);
        assert_eq!(
            common.timescale, None,
            "differing timescale must not be hoisted"
        );
        assert_eq!(root.tracks[0].timescale, Some(90000), "video keeps 90000");
        assert_eq!(root.tracks[1].timescale, Some(48000), "audio keeps 48000");
    }

    #[test]
    fn validate_accepts_fec_with_inherited_repair_packaging() {
        // The repair track's `fec-repair` packaging is inherited from common; the
        // fec check uses effective packaging, so this must validate (covers the
        // .or(common) arm on the accept path).
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {"packaging": "fec-repair"},
            "tracks": [
                {"name":"v","packaging":"mmtp","mmtpMode":"mpu","timescale":90000,"groupDurationMs":1000,
                 "fec":{"algorithm":"raptorq","sourceSymbols":32,"repairSymbols":8,"symbolSize":1312,"repairTrack":"v/repair"},
                 "selectionParams":{}},
                {"name":"v/repair","selectionParams":{}}
            ]
        }"#;
        serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect("repair track inheriting fec-repair packaging from common is valid");
    }

    #[test]
    fn validate_accepts_aligned_switching_set() {
        // Two mmtp renditions in altGroup 1, same timescale + group duration:
        // their MoQ group numbers stay media-time-aligned. §4.4.2.
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [
                {"name":"v720","packaging":"mmtp","mmtpMode":"mfu","timescale":90000,"groupDurationMs":1000,"altGroup":1,"selectionParams":{}},
                {"name":"v1080","packaging":"mmtp","mmtpMode":"mfu","timescale":90000,"groupDurationMs":1000,"altGroup":1,"selectionParams":{}}
            ]
        }"#;
        serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect("an aligned switching set is valid");
    }

    #[test]
    fn validate_rejects_switching_set_group_duration_mismatch() {
        // Same altGroup, same timescale, but 1000 ms vs 2000 ms group durations:
        // group numbers drift, so ABR switches would land off a group boundary.
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [
                {"name":"v720","packaging":"mmtp","mmtpMode":"mfu","timescale":90000,"groupDurationMs":1000,"altGroup":1,"selectionParams":{}},
                {"name":"v1080","packaging":"mmtp","mmtpMode":"mfu","timescale":90000,"groupDurationMs":2000,"altGroup":1,"selectionParams":{}}
            ]
        }"#;
        let err = serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect_err("mismatched group durations in a switching set must be rejected");
        assert!(
            matches!(
                &err,
                CatalogValidationError::SwitchingSetGroupDurationMismatch { alt_group: 1, track_name, other_track }
                    if track_name == "v1080" && other_track == "v720"
            ),
            "got: {err:?}"
        );
    }

    #[test]
    fn validate_accepts_mixed_timescale_switching_set_when_wall_clock_equal() {
        // Different timescales but equal wall-clock group duration (both 1 s):
        // cross-multiplication makes raw tick counts (90000 vs 48000) agree.
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [
                {"name":"hi","packaging":"mmtp","mmtpMode":"mfu","timescale":90000,"groupDurationTicks":90000,"altGroup":2,"selectionParams":{}},
                {"name":"lo","packaging":"mmtp","mmtpMode":"mfu","timescale":48000,"groupDurationTicks":48000,"altGroup":2,"selectionParams":{}}
            ]
        }"#;
        serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect("equal wall-clock group durations agree across timescales");
    }

    #[test]
    fn validate_accepts_lone_alt_group_member() {
        // A single track carrying an altGroup has nothing to disagree with.
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [
                {"name":"v","packaging":"mmtp","mmtpMode":"mfu","timescale":90000,"groupDurationMs":1000,"altGroup":7,"selectionParams":{}}
            ]
        }"#;
        serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect("a lone switching-set member is valid");
    }

    #[test]
    fn validate_rejects_switching_set_mismatch_with_inherited_alt_group() {
        // altGroup is hoisted into commonTrackFields (the compaction real catalogs
        // use via from_tracks); the switching-set check must still read it through
        // `.or(common)` and reject the group-duration mismatch. Locks the
        // inheritance path that the four direct-altGroup tests above never take.
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {"altGroup": 1},
            "tracks": [
                {"name":"v720","packaging":"mmtp","mmtpMode":"mfu","timescale":90000,"groupDurationMs":1000,"selectionParams":{}},
                {"name":"v1080","packaging":"mmtp","mmtpMode":"mfu","timescale":90000,"groupDurationMs":2000,"selectionParams":{}}
            ]
        }"#;
        let err = serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect_err("a mismatch under an inherited altGroup must still be rejected");
        assert!(
            matches!(
                &err,
                CatalogValidationError::SwitchingSetGroupDurationMismatch { alt_group: 1, track_name, other_track }
                    if track_name == "v1080" && other_track == "v720"
            ),
            "got: {err:?}"
        );
    }

    #[test]
    fn validate_rejects_third_switching_set_member_against_anchor() {
        // Members 1+2 agree (1000 ms); member 3 (2000 ms) disagrees. The error
        // must name the anchor (v1, first-seen), not the predecessor (v2) — the
        // comparison is always against the sticky anchor, not the prior member.
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [
                {"name":"v1","packaging":"mmtp","mmtpMode":"mfu","timescale":90000,"groupDurationMs":1000,"altGroup":1,"selectionParams":{}},
                {"name":"v2","packaging":"mmtp","mmtpMode":"mfu","timescale":90000,"groupDurationMs":1000,"altGroup":1,"selectionParams":{}},
                {"name":"v3","packaging":"mmtp","mmtpMode":"mfu","timescale":90000,"groupDurationMs":2000,"altGroup":1,"selectionParams":{}}
            ]
        }"#;
        let err = serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect_err("third member disagreeing with the anchor must be rejected");
        assert!(
            matches!(
                &err,
                CatalogValidationError::SwitchingSetGroupDurationMismatch { alt_group: 1, track_name, other_track }
                    if track_name == "v3" && other_track == "v1"
            ),
            "the error must name the anchor (v1), not the predecessor (v2); got: {err:?}"
        );
    }

    #[test]
    fn validate_skips_switching_set_check_for_non_mmtp_tracks() {
        // §4.4.2 group-number alignment is an MMTP-over-MoQ concept. Non-mmtp
        // renditions (e.g. cmaf) in a switching set are out of its scope and must
        // NOT be subjected to the group-duration agreement check — which would
        // also hit the ms→tick truncation the per-track loop exactness-guards only
        // for mmtp. Differing (and non-exact) cmaf group durations validate.
        let json = r#"{
            "version": 1, "streamingFormat": 1, "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true, "commonTrackFields": {},
            "tracks": [
                {"name":"c720","packaging":"cmaf","timescale":44100,"groupDurationMs":1001,"altGroup":1,"selectionParams":{}},
                {"name":"c1080","packaging":"cmaf","timescale":44100,"groupDurationMs":2002,"altGroup":1,"selectionParams":{}}
            ]
        }"#;
        serde_json::from_str::<Root>(json)
            .unwrap()
            .validate()
            .expect("non-mmtp switching-set members are out of §4.4.2 scope");
    }
}

/// Library-level validation errors raised by `Root::validate`.
///
/// Promotes guards that publishers and subscribers both need so
/// they don't re-implement them. Variants are stable and matchable
/// so callers can branch on the specific failure.
#[derive(Debug, Clone, PartialEq)]
pub enum CatalogValidationError {
    /// Two `multicast.endpoints[].tracks[]` entries share the same
    /// `packet_id` — violates draft-ramadan-moq-multicast §4.1
    /// uniqueness requirement.
    DuplicatePacketId {
        packet_id: u16,
        first_track: String,
        duplicate_track: String,
    },
    /// A `multicast.endpoints[].tracks[].name` does not appear in
    /// `tracks[]`. Receivers cannot resolve selectionParams for an
    /// unknown track.
    UnknownTrackReference { track_name: String },
    /// A `tracks[]` entry declares MMTP packaging without the required
    /// `mmtpMode` signal.
    MissingMmtpMode { track_name: String },
    /// An MMTP track has no effective `timescale` (§4.4.2 requires one;
    /// the catalog never infers it).
    MissingTimescale { track_name: String },
    /// An MMTP track has neither `groupDurationMs` nor `groupDurationTicks`
    /// (§4.4.2 requires a group duration).
    MissingGroupDuration { track_name: String },
    /// `groupDurationMs * timescale` is not a whole number of ticks and no
    /// `groupDurationTicks` override is given (§4.4.2 exactness).
    GroupDurationNotExact {
        track_name: String,
        group_duration_ms: u32,
        timescale: u32,
    },
    /// A track's `fec.repairTrack` does not name a catalog track whose
    /// effective packaging is `fec-repair` (moq-fec §5.1).
    UnknownRepairTrack {
        track_name: String,
        repair_track: String,
    },
    /// An MMTP track declares `timescale: 0` — bad init; the §4.4.2 tick math is
    /// undefined and would silently neutralize the exactness check.
    ZeroTimescale { track_name: String },
    /// An MMTP track declares a zero `groupDurationMs` or `groupDurationTicks`
    /// (a zero-length group is bad init; §4.4.2).
    ZeroGroupDuration { track_name: String },
    /// An MMTP track declares a zero `keyframeIntervalMs` or
    /// `keyframeIntervalTicks`. A zero cadence would drive the receiver's
    /// keyframe-loss repair timeout to zero — bad init.
    ZeroKeyframeInterval { track_name: String },
    /// A track's `fec` descriptor has a degenerate parameter for an active scheme
    /// (zero source/repair symbols or symbol size; moq-fec §5.1). FEC params are
    /// out-of-band for multicast, so a degenerate descriptor silently fails recovery.
    InvalidFecParams { track_name: String, reason: String },
    /// Two MMTP tracks in the same switching set (same effective `altGroup`)
    /// publish different effective group durations, so their MoQ group
    /// numbers cannot stay media-time-aligned (draft-ramadan-moq-mmt §4.4.2).
    SwitchingSetGroupDurationMismatch {
        /// The switching set (effective `altGroup`) the two tracks share.
        alt_group: u16,
        /// The offending track — the one whose group duration differs.
        track_name: String,
        /// The first-seen member of the set; the anchor compared against.
        other_track: String,
    },
}

impl std::fmt::Display for CatalogValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicatePacketId {
                packet_id,
                first_track,
                duplicate_track,
            } => write!(
                f,
                "duplicate packet_id {packet_id} used by both `{first_track}` and `{duplicate_track}` \
                 (draft-ramadan-moq-multicast §4.1 requires packet_id uniqueness within an endpoint tuple)"
            ),
            Self::UnknownTrackReference { track_name } => write!(
                f,
                "multicast endpoint references track `{track_name}` not present in catalog.tracks[]"
            ),
            Self::MissingMmtpMode { track_name } => write!(
                f,
                "catalog.tracks[] entry `{track_name}` has packaging=mmtp but no required mmtpMode"
            ),
            Self::MissingTimescale { track_name } => write!(
                f,
                "catalog.tracks[] entry `{track_name}` has packaging=mmtp but no required timescale (draft-ramadan-moq-mmt §4.4.2)"
            ),
            Self::MissingGroupDuration { track_name } => write!(
                f,
                "catalog.tracks[] entry `{track_name}` has packaging=mmtp but no groupDurationMs or groupDurationTicks (§4.4.2)"
            ),
            Self::GroupDurationNotExact {
                track_name,
                group_duration_ms,
                timescale,
            } => write!(
                f,
                "catalog.tracks[] entry `{track_name}`: groupDurationMs {group_duration_ms} × timescale {timescale} \
                 is not a whole number of ticks — supply groupDurationTicks (§4.4.2)"
            ),
            Self::UnknownRepairTrack {
                track_name,
                repair_track,
            } => write!(
                f,
                "catalog.tracks[] entry `{track_name}` fec.repairTrack `{repair_track}` does not name a fec-repair track (moq-fec §5.1)"
            ),
            Self::ZeroTimescale { track_name } => write!(
                f,
                "catalog.tracks[] entry `{track_name}` has timescale 0 (§4.4.2 requires a positive timescale)"
            ),
            Self::ZeroGroupDuration { track_name } => write!(
                f,
                "catalog.tracks[] entry `{track_name}` has a zero groupDurationMs/groupDurationTicks (§4.4.2 requires a positive group duration)"
            ),
            Self::ZeroKeyframeInterval { track_name } => write!(
                f,
                "catalog.tracks[] entry `{track_name}` has a zero keyframeIntervalMs/keyframeIntervalTicks (a positive keyframe cadence is required when present)"
            ),
            Self::InvalidFecParams { track_name, reason } => write!(
                f,
                "catalog.tracks[] entry `{track_name}` has an invalid fec descriptor: {reason} (moq-fec §5.1)"
            ),
            Self::SwitchingSetGroupDurationMismatch {
                alt_group,
                track_name,
                other_track,
            } => write!(
                f,
                "switching set altGroup {alt_group}: track `{track_name}` and `{other_track}` publish \
                 different effective group durations — MoQ group numbers cannot stay media-time-aligned \
                 (draft-ramadan-moq-mmt §4.4.2)"
            ),
        }
    }
}

impl std::error::Error for CatalogValidationError {}

impl Root {
    /// Validate library-level invariants on the catalog.
    ///
    /// First-error-wins. Run after `serde_json::from_str` parsing
    /// to reject malformed catalogs before downstream code touches
    /// them. Publishers run this in addition to their own runtime
    /// guards (defense in depth); subscribers run it standalone.
    pub fn validate(&self) -> Result<(), CatalogValidationError> {
        // draft-ramadan-moq-mmt §12.1 requires mmtpMode for MMTP packaging.
        // validate() runs before expand_common_fields(), so both packaging and
        // mmtpMode are taken as effective values (the track's own field OR the
        // commonTrackFields default; track-level wins). A track may inherit
        // either or both from common — the requirement is on the effective pair.
        for t in &self.tracks {
            let effective_packaging = t
                .packaging
                .as_ref()
                .or(self.common_track_fields.packaging.as_ref());
            if !matches!(effective_packaging, Some(TrackPackaging::Mmtp)) {
                continue;
            }
            // §12.1: mmtpMode REQUIRED (effective: track OR common).
            if t.mmtp_mode
                .as_ref()
                .or(self.common_track_fields.mmtp_mode.as_ref())
                .is_none()
            {
                return Err(CatalogValidationError::MissingMmtpMode {
                    track_name: t.name.clone(),
                });
            }
            // §4.4.2: mmtp tracks MUST publish an explicit timescale; the catalog
            // never infers one.
            let timescale = match t.timescale.or(self.common_track_fields.timescale) {
                Some(ts) => ts,
                None => {
                    return Err(CatalogValidationError::MissingTimescale {
                        track_name: t.name.clone(),
                    })
                }
            };
            // §4.4.2: a zero timescale is bad init — the tick math is undefined and
            // would silently neutralize the exactness check below (`ms × 0 % 1000`
            // is always 0).
            if timescale == 0 {
                return Err(CatalogValidationError::ZeroTimescale {
                    track_name: t.name.clone(),
                });
            }
            // §4.4.2: group duration REQUIRED, as ms or an explicit tick override.
            let gd_ms = t
                .group_duration_ms
                .or(self.common_track_fields.group_duration_ms);
            let gd_ticks = t
                .group_duration_ticks
                .or(self.common_track_fields.group_duration_ticks);
            if gd_ms.is_none() && gd_ticks.is_none() {
                return Err(CatalogValidationError::MissingGroupDuration {
                    track_name: t.name.clone(),
                });
            }
            // A present-but-zero group duration is bad init (zero-length group).
            if matches!(gd_ms, Some(0)) || matches!(gd_ticks, Some(0)) {
                return Err(CatalogValidationError::ZeroGroupDuration {
                    track_name: t.name.clone(),
                });
            }
            // A present-but-zero keyframe interval would zero the receiver's
            // repair timeout — bad init. Advisory field, so not required.
            let kf_ms = t
                .keyframe_interval_ms
                .or(self.common_track_fields.keyframe_interval_ms);
            let kf_ticks = t
                .keyframe_interval_ticks
                .or(self.common_track_fields.keyframe_interval_ticks);
            if matches!(kf_ms, Some(0)) || matches!(kf_ticks, Some(0)) {
                return Err(CatalogValidationError::ZeroKeyframeInterval {
                    track_name: t.name.clone(),
                });
            }
            // §4.4.2 exactness: the ms form must convert to a whole tick count
            // unless an explicit ticks override is supplied. `% == 0` is kept over
            // `u64::is_multiple_of` (stable only since Rust 1.87) to honor the
            // repo's documented 1.70+ MSRV.
            #[allow(clippy::manual_is_multiple_of)]
            if let (Some(ms), None) = (gd_ms, gd_ticks) {
                if (ms as u64 * timescale as u64) % 1000 != 0 {
                    return Err(CatalogValidationError::GroupDurationNotExact {
                        track_name: t.name.clone(),
                        group_duration_ms: ms,
                        timescale,
                    });
                }
            }
        }
        // moq-fec §5.1: a track's fec.repairTrack must reference a catalog track
        // whose effective packaging is fec-repair.
        for t in &self.tracks {
            if let Some(fec) = &t.fec {
                // FEC params are out-of-band for multicast (no back channel to
                // renegotiate), so a degenerate descriptor silently breaks recovery
                // for every receiver. Reject zero counts for an active scheme.
                if !matches!(fec.algorithm, FecAlgorithm::None) {
                    let bad = if fec.source_symbols == 0 {
                        Some("sourceSymbols (K) must be >= 1")
                    } else if fec.repair_symbols == 0 {
                        Some("repairSymbols (P) must be >= 1")
                    } else if fec.symbol_size == 0 {
                        Some("symbolSize (T) must be >= 1")
                    } else {
                        None
                    };
                    if let Some(reason) = bad {
                        return Err(CatalogValidationError::InvalidFecParams {
                            track_name: t.name.clone(),
                            reason: reason.to_string(),
                        });
                    }
                }
                let target_is_repair = match self.tracks.iter().find(|x| x.name == fec.repair_track)
                {
                    Some(x) => matches!(
                        x.packaging
                            .as_ref()
                            .or(self.common_track_fields.packaging.as_ref()),
                        Some(TrackPackaging::FecRepair)
                    ),
                    None => false,
                };
                if !target_is_repair {
                    return Err(CatalogValidationError::UnknownRepairTrack {
                        track_name: t.name.clone(),
                        repair_track: fec.repair_track.clone(),
                    });
                }
            }
        }
        // draft-ramadan-moq-mmt §4.4.2 "Switching Sets": every MMTP track in the
        // same switching set — identified by the IETF-catalog `altGroup` (multicast
        // draft §4.1) — MUST publish the same effective group duration. The group
        // number is `floor(media_ticks / group_duration_ticks)`, and `media_ticks`
        // scales with the track's own `timescale`, so two tracks produce the same
        // group number for a given media time iff their group durations are equal
        // in WALL-CLOCK terms (ticks / timescale), not as raw tick counts. We
        // compare wall-clock equality in the integer domain via cross-
        // multiplication (`ticks_a * ts_b == ticks_b * ts_a`); for the common case
        // where set members share a timescale this reduces to plain tick equality.
        // u128 products avoid overflow (each operand is a u64-bounded field).
        //
        // Scoped to effective-MMTP tracks: §4.4.2 group numbering is an
        // MMTP-over-MoQ concept, and the per-track loop above already requires —
        // and exactness-checks — timescale + group duration for exactly these
        // tracks, so the `continue` skips below can only fire for a non-MMTP set
        // member, which is out of scope. (Without this packaging gate a non-MMTP
        // altGroup member carrying timescale + groupDurationMs would reach the
        // unguarded ms→tick truncation and could be mis-compared.)
        let mut anchor: std::collections::HashMap<u16, (u128, u128, String)> =
            std::collections::HashMap::new();
        for t in &self.tracks {
            if !matches!(
                t.packaging
                    .as_ref()
                    .or(self.common_track_fields.packaging.as_ref()),
                Some(TrackPackaging::Mmtp)
            ) {
                continue;
            }
            let alt = match t.alt_group.or(self.common_track_fields.alt_group) {
                Some(alt) => alt,
                None => continue,
            };
            let ts = match t.timescale.or(self.common_track_fields.timescale) {
                Some(ts) => ts as u128,
                None => continue,
            };
            let ticks = match t
                .group_duration_ticks
                .or(self.common_track_fields.group_duration_ticks)
            {
                Some(ticks) => ticks as u128,
                None => match t
                    .group_duration_ms
                    .or(self.common_track_fields.group_duration_ms)
                {
                    Some(ms) => ms as u128 * ts / 1000,
                    None => continue,
                },
            };
            match anchor.get(&alt) {
                None => {
                    anchor.insert(alt, (ticks, ts, t.name.clone()));
                }
                Some((a_ticks, a_ts, a_name)) => {
                    if ticks * a_ts != *a_ticks * ts {
                        return Err(CatalogValidationError::SwitchingSetGroupDurationMismatch {
                            alt_group: alt,
                            track_name: t.name.clone(),
                            other_track: a_name.clone(),
                        });
                    }
                }
            }
        }
        if let Some(mc) = &self.multicast {
            if let Some(endpoints) = &mc.endpoints {
                let mut seen: std::collections::HashMap<u16, String> =
                    std::collections::HashMap::new();
                for endpoint in endpoints {
                    for tref in &endpoint.tracks {
                        // T5b — every multicast track ref must resolve
                        // to a catalog.tracks[] entry by name.
                        if !self.tracks.iter().any(|t| t.name == tref.name) {
                            return Err(CatalogValidationError::UnknownTrackReference {
                                track_name: tref.name.clone(),
                            });
                        }
                        // T5a — packet_id uniqueness across endpoints.
                        if let Some(first) = seen.get(&tref.packet_id) {
                            return Err(CatalogValidationError::DuplicatePacketId {
                                packet_id: tref.packet_id,
                                first_track: first.clone(),
                                duplicate_track: tref.name.clone(),
                            });
                        }
                        seen.insert(tref.packet_id, tref.name.clone());
                    }
                }
            }
        }
        Ok(())
    }

    /// Apply `commonTrackFields` defaults to every track entry.
    ///
    /// For each track, any field that is `None` at the track level is
    /// populated from the matching field on `common_track_fields`.
    /// Track-level values always win. This is the inverse of
    /// `CommonTrackFields::from_tracks` (which extracts common values
    /// for serialization compactness); callers that consume a freshly
    /// parsed catalog typically run `expand_common_fields` before
    /// reading per-track values so they don't need to check both
    /// locations themselves.
    pub fn expand_common_fields(&mut self) {
        let common = self.common_track_fields.clone();
        for track in &mut self.tracks {
            track.with_common(&common);
        }
    }
}

impl Track {
    fn with_common(&mut self, common: &CommonTrackFields) {
        if self.namespace.is_none() {
            self.namespace.clone_from(&common.namespace);
        }
        if self.packaging.is_none() {
            self.packaging.clone_from(&common.packaging);
        }
        if self.mmtp_mode.is_none() {
            self.mmtp_mode.clone_from(&common.mmtp_mode);
        }
        if self.timescale.is_none() {
            self.timescale = common.timescale;
        }
        if self.group_duration_ms.is_none() {
            self.group_duration_ms = common.group_duration_ms;
        }
        if self.group_duration_ticks.is_none() {
            self.group_duration_ticks = common.group_duration_ticks;
        }
        if self.keyframe_interval_ms.is_none() {
            self.keyframe_interval_ms = common.keyframe_interval_ms;
        }
        if self.keyframe_interval_ticks.is_none() {
            self.keyframe_interval_ticks = common.keyframe_interval_ticks;
        }
        if self.init_mode.is_none() {
            self.init_mode.clone_from(&common.init_mode);
        }
        if self.render_group.is_none() {
            self.render_group = common.render_group;
        }
        if self.alt_group.is_none() {
            self.alt_group = common.alt_group;
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
pub enum TrackPackaging {
    #[serde(rename = "cmaf")]
    #[default]
    Cmaf,

    #[serde(rename = "loc")]
    Loc,

    /// MMTP packaging. Defined by draft-ramadan-moq-mmt §12.1 as an additional
    /// value of the IETF catalog `packaging` field (draft-ietf-moq-catalogformat
    /// defines `cmaf` and `loc`). This is the MSF-facing packaging value
    /// (`shaka.msf` keys track selection on `packaging === "mmtp"`); the
    /// REQUIRED per-track `mmtpMode` field carries the finer MPU-vs-MFU
    /// encapsulation detail (the legacy `container` extension is gone).
    #[serde(rename = "mmtp")]
    Mmtp,

    /// AL-FEC repair track packaging, per draft-ramadan-moq-fec §5.2 (repair
    /// tracks are catalog-signaled; REQUIRED for multicast where no back
    /// channel exists). The publisher skips these when building its source
    /// track map and derives its own `<name>/repair` siblings.
    #[serde(rename = "fec-repair")]
    FecRepair,

    /// Opaque datagram packaging: each ingested UDP datagram becomes exactly
    /// one MoQ object, carried verbatim with no protocol parsing. The publisher
    /// maps one track per datagram stream, advancing the MoQ group on every
    /// datagram (one object per group), so `multicast.subgroupHistoryGroups`
    /// bounds retained datagrams. Used for non-MMTP payloads whose framing the
    /// MoQ layer must not interpret (e.g. Solana shred frames, which the
    /// receiver reassembles from the frames' own headers). Unlike `mmtp`, it
    /// requires no `mmtpMode` and derives no `/repair` sibling.
    #[serde(rename = "datagram")]
    Datagram,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum MmtpMode {
    #[serde(rename = "mpu")]
    Mpu,

    #[serde(rename = "mfu")]
    Mfu,
}

/// MPU metadata delivery mode per draft-ramadan-moq-mmt §4.5. `inline` (default)
/// carries the MPU metadata box as subgroup 0 of each group; `track` delivers it
/// on a separate init track.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
pub enum InitMode {
    #[serde(rename = "inline")]
    #[default]
    Inline,

    #[serde(rename = "track")]
    Track,
}

/// AL-FEC scheme identifier per draft-ramadan-moq-fec §5.1.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum FecAlgorithm {
    #[serde(rename = "none")]
    None,

    #[serde(rename = "raptorq")]
    RaptorQ,

    #[serde(rename = "reed-solomon")]
    ReedSolomon,
}

/// Per-track AL-FEC descriptor (draft-ramadan-moq-fec §5.1, referenced by
/// draft-ramadan-moq-mmt §8.3). Out-of-band FEC_CONFIG for multicast, where no
/// back channel exists to negotiate it. Per-track; not inherited via common.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct FecDescriptor {
    /// FEC scheme.
    pub algorithm: FecAlgorithm,

    /// K — source symbols per FEC block.
    #[serde(rename = "sourceSymbols")]
    pub source_symbols: u32,

    /// P — repair symbols per FEC block.
    #[serde(rename = "repairSymbols")]
    pub repair_symbols: u32,

    /// T — bytes per symbol.
    #[serde(rename = "symbolSize")]
    pub symbol_size: u32,

    /// Canonical AL-FEC interleave block span in MILLISECONDS
    /// (draft-ramadan-moq-fec §5.1). The full FEC block spans
    /// `interleaveDepthMs`; per-sub-block timeout = `interleaveDepthMs × K_sub / K`.
    /// OPTIONAL; absent ⇒ no interleaving (D=1), resolved by the consumer —
    /// the catalog never materializes a `0`.
    #[serde(rename = "interleaveDepthMs", skip_serializing_if = "Option::is_none")]
    pub interleave_depth_ms: Option<u32>,

    /// Repair track name, convention `[namespace, track_name, "repair"]`. Must
    /// reference a catalog track whose effective packaging is `fec-repair`.
    #[serde(rename = "repairTrack")]
    pub repair_track: String,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct SelectionParam {
    pub codec: Option<String>,

    #[serde(rename = "mimeType")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub framerate: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub bitrate: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub samplerate: Option<u32>,

    #[serde(rename = "channelConfig", skip_serializing_if = "Option::is_none")]
    pub channel_config: Option<String>,

    #[serde(rename = "displayWidth", skip_serializing_if = "Option::is_none")]
    pub display_width: Option<u16>,

    #[serde(rename = "displayHeight", skip_serializing_if = "Option::is_none")]
    pub display_height: Option<u16>,

    #[serde(rename = "lang", skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct CommonTrackFields {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub packaging: Option<TrackPackaging>,

    /// MMTP packetization mode, inheritable per-catalog like `packaging`.
    /// draft-ramadan-moq-mmt §12.1 marks `mmtpMode` REQUIRED per-track; setting
    /// it here lets a single-mode catalog declare it once. `validate()` enforces
    /// the requirement on the *effective* value (track OR common), and
    /// `expand_common_fields()` pushes it down so consumers see it per-track.
    #[serde(rename = "mmtpMode", skip_serializing_if = "Option::is_none")]
    pub mmtp_mode: Option<MmtpMode>,

    /// Media timescale (Hz), inheritable like `mmtpMode`. §4.4.2.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timescale: Option<u32>,

    /// Group duration (integer ms), inheritable. §4.4.2.
    #[serde(rename = "groupDurationMs", skip_serializing_if = "Option::is_none")]
    pub group_duration_ms: Option<u32>,

    /// Exact group-duration tick override, inheritable. §4.4.2.
    #[serde(rename = "groupDurationTicks", skip_serializing_if = "Option::is_none")]
    pub group_duration_ticks: Option<u64>,

    /// Keyframe interval (integer ms), inheritable. Advisory repair cadence.
    #[serde(rename = "keyframeIntervalMs", skip_serializing_if = "Option::is_none")]
    pub keyframe_interval_ms: Option<u32>,

    /// Exact keyframe-interval tick override, inheritable.
    #[serde(rename = "keyframeIntervalTicks", skip_serializing_if = "Option::is_none")]
    pub keyframe_interval_ticks: Option<u64>,

    /// MPU metadata delivery mode, inheritable. §4.5.
    #[serde(rename = "initMode", skip_serializing_if = "Option::is_none")]
    pub init_mode: Option<InitMode>,

    #[serde(rename = "renderGroup", skip_serializing_if = "Option::is_none")]
    pub render_group: Option<u16>,

    #[serde(rename = "altGroup", skip_serializing_if = "Option::is_none")]
    pub alt_group: Option<u16>,
}

impl CommonTrackFields {
    /// Serialize function to conditionally include fields based on their commonality amoung tracks
    pub fn from_tracks(tracks: &mut [Track]) -> Self {
        if tracks.is_empty() {
            return Default::default();
        }

        // Use the first track as the basis
        let mut common = Self {
            namespace: tracks[0].namespace.clone(),
            packaging: tracks[0].packaging.clone(),
            mmtp_mode: tracks[0].mmtp_mode.clone(),
            timescale: tracks[0].timescale,
            group_duration_ms: tracks[0].group_duration_ms,
            group_duration_ticks: tracks[0].group_duration_ticks,
            keyframe_interval_ms: tracks[0].keyframe_interval_ms,
            keyframe_interval_ticks: tracks[0].keyframe_interval_ticks,
            init_mode: tracks[0].init_mode.clone(),
            render_group: tracks[0].render_group,
            alt_group: tracks[0].alt_group,
        };

        // Loop over the other tracks to check if they have the same values
        for track in &mut tracks[1..] {
            if track.namespace != common.namespace {
                common.namespace = None;
            }
            if track.packaging != common.packaging {
                common.packaging = None;
            }
            if track.mmtp_mode != common.mmtp_mode {
                common.mmtp_mode = None;
            }
            if track.timescale != common.timescale {
                common.timescale = None;
            }
            if track.group_duration_ms != common.group_duration_ms {
                common.group_duration_ms = None;
            }
            if track.group_duration_ticks != common.group_duration_ticks {
                common.group_duration_ticks = None;
            }
            if track.keyframe_interval_ms != common.keyframe_interval_ms {
                common.keyframe_interval_ms = None;
            }
            if track.keyframe_interval_ticks != common.keyframe_interval_ticks {
                common.keyframe_interval_ticks = None;
            }
            if track.init_mode != common.init_mode {
                common.init_mode = None;
            }
            if track.render_group != common.render_group {
                common.render_group = None
            }
            if track.alt_group != common.alt_group {
                common.alt_group = None;
            }
        }

        // Loop again to remove the common fields from the tracks. Strip a field
        // only when it was actually hoisted to common (`common.<field>.is_some()`).
        // For a heterogeneous catalog the disagreeing field stays None in common
        // (set above), so each track keeps its own value instead of losing it —
        // e.g. video=mpu/audio=mfu mmtpMode, or video=mmtp/audio=cmaf packaging.
        // expand_common_fields() is the exact inverse: it only pushes down values
        // that are Some in common.
        for track in tracks {
            if common.namespace.is_some() {
                track.namespace = None;
            }
            if common.packaging.is_some() {
                track.packaging = None;
            }
            if common.mmtp_mode.is_some() {
                track.mmtp_mode = None;
            }
            // New §12.1 fields use the correct `common.is_some()` strip form too.
            if common.timescale.is_some() {
                track.timescale = None;
            }
            if common.group_duration_ms.is_some() {
                track.group_duration_ms = None;
            }
            if common.group_duration_ticks.is_some() {
                track.group_duration_ticks = None;
            }
            if common.keyframe_interval_ms.is_some() {
                track.keyframe_interval_ms = None;
            }
            if common.keyframe_interval_ticks.is_some() {
                track.keyframe_interval_ticks = None;
            }
            if common.init_mode.is_some() {
                track.init_mode = None;
            }
            if common.render_group.is_some() {
                track.render_group = None;
            }
            if common.alt_group.is_some() {
                track.alt_group = None;
            }
        }

        common
    }
}
