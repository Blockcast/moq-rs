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
                {"name":"v","packaging":"mmtp","mmtpMode":"mpu","selectionParams":{}},
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
                {"name":"v","mmtpMode":"mpu","selectionParams":{}}
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
            "commonTrackFields": {"packaging": "mmtp", "mmtpMode": "mpu"},
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
            let effective_mmtp_mode = t
                .mmtp_mode
                .as_ref()
                .or(self.common_track_fields.mmtp_mode.as_ref());
            if matches!(effective_packaging, Some(TrackPackaging::Mmtp))
                && effective_mmtp_mode.is_none()
            {
                return Err(CatalogValidationError::MissingMmtpMode {
                    track_name: t.name.clone(),
                });
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
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum MmtpMode {
    #[serde(rename = "mpu")]
    Mpu,

    #[serde(rename = "mfu")]
    Mfu,
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
