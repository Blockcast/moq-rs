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

    /// Container format per draft-ramadan-moq-mmt §11.1. Optional extension
    /// to the IETF draft-01 catalog format; parsers that don't understand it
    /// MUST ignore it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container: Option<Container>,

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
    fn container_serializes_spec_values() {
        // Per draft-ramadan-moq-mmt §11.1: container values are
        // "isobmff" | "mmtp" | "mfu" | "fec-repair".
        for (v, expected) in [
            (Container::Isobmff, "\"isobmff\""),
            (Container::Mmtp, "\"mmtp\""),
            (Container::Mfu, "\"mfu\""),
            (Container::FecRepair, "\"fec-repair\""),
        ] {
            let json = serde_json::to_string(&v).unwrap();
            assert_eq!(json, expected, "serialize {v:?}");
            let back: Container = serde_json::from_str(&json).unwrap();
            assert_eq!(back, v, "round-trip {v:?}");
        }
    }

    #[test]
    fn track_accepts_container_field() {
        let json = r#"{
            "name": "video",
            "container": "mmtp",
            "selectionParams": {"codec": "avc1.64001f"}
        }"#;
        let track: Track = serde_json::from_str(json).unwrap();
        assert_eq!(track.container, Some(Container::Mmtp));
    }

    #[test]
    fn track_without_container_round_trips() {
        // Containers field is optional — older catalogs that don't carry it
        // must still parse, and serialization must not emit "container":null.
        let t = Track {
            name: "v".into(),
            ..Default::default()
        };
        let json = serde_json::to_string(&t).unwrap();
        assert!(!json.contains("container"), "json = {json}");
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
    fn validate_rejects_fec_repair_in_catalog_tracks() {
        // Per ADR T3: repair tracks are publisher-derived (auto-named
        // `<source>/repair`) — they MUST NOT appear in catalog.tracks[].
        // A catalog that pre-declares a Container::FecRepair entry is
        // either misconfigured or relying on M.1b receiver semantics.
        let json = r#"{
            "version": 1,
            "streamingFormat": 1,
            "streamingFormatVersion": "0.2",
            "supportsDeltaUpdates": true,
            "commonTrackFields": {},
            "tracks": [
                {"name":"v","container":"mmtp","selectionParams":{}},
                {"name":"v/repair","container":"fec-repair","selectionParams":{}}
            ]
        }"#;
        let root: Root = serde_json::from_str(json).unwrap();
        let err = root.validate().expect_err("FecRepair in catalog.tracks must fail");
        assert!(
            matches!(&err, CatalogValidationError::FecRepairInCatalog { track_name } if track_name == "v/repair"),
            "expected FecRepairInCatalog(v/repair), got: {err:?}"
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
            matches!(err, CatalogValidationError::DuplicatePacketId { packet_id: 1, .. }),
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
    /// A `tracks[]` entry carries `container: fec-repair`. Repair
    /// tracks are publisher-derived (auto-named `<source>/repair`
    /// per ADR T3) and must not appear in the catalog directly.
    /// M.1b will revisit if catalog-declared FEC tracks become
    /// useful for receiver-side semantics.
    FecRepairInCatalog { track_name: String },
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
            Self::FecRepairInCatalog { track_name } => write!(
                f,
                "catalog.tracks[] entry `{track_name}` has container=fec-repair; \
                 repair tracks are publisher-derived in M.1 and must not appear in the catalog"
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
        // T5e — FecRepair must not appear in catalog.tracks (repair
        // tracks are publisher-derived).
        for t in &self.tracks {
            if matches!(t.container, Some(Container::FecRepair)) {
                return Err(CatalogValidationError::FecRepairInCatalog {
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

    /// MMTP packaging. Defined by draft-ramadan-moq-mmt as an additional value
    /// of the IETF catalog `packaging` field (draft-ietf-moq-catalogformat
    /// defines `cmaf` and `loc`). This is the coarse, MSF-facing packaging value
    /// (`shaka.msf` keys track selection on `packaging === "mmtp"`); the
    /// optional `container` extension carries finer MMTP encapsulation detail.
    #[serde(rename = "mmtp")]
    Mmtp,
}

/// Container format per draft-ramadan-moq-mmt §11.1.
///
/// Identifies how media is encapsulated inside MoQ objects. Distinct from
/// `TrackPackaging` (which describes the streaming format CMAF vs LOC at the
/// IETF draft-01 catalog level). Tracks MAY carry both fields independently.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Container {
    /// Raw ISOBMFF/MPU with the MMTP header stripped. Default when MoQ
    /// transport provides timestamps and FEC out-of-band.
    #[serde(rename = "isobmff")]
    Isobmff,

    /// MMTP-encapsulated MPU (Standard MPU mode). Object 0 of each group
    /// carries the MPU metadata (mmpu+moov+moof). Subsequent objects carry
    /// MFU payloads.
    #[serde(rename = "mmtp")]
    Mmtp,

    /// MMTP MFU mode. Object 0 carries MPU metadata only; each subsequent
    /// object carries one complete MFU (one coded frame). Enables per-frame
    /// FEC protection and frame-level prioritization.
    #[serde(rename = "mfu")]
    Mfu,

    /// FEC repair track per draft-ramadan-moq-fec.
    #[serde(rename = "fec-repair")]
    FecRepair,
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
            if track.render_group != common.render_group {
                common.render_group = None
            }
            if track.alt_group != common.alt_group {
                common.alt_group = None;
            }
        }

        // Loop again to remove the common fields from the tracks
        for track in tracks {
            if common.namespace.is_some() {
                track.namespace = None;
            }
            if track.packaging.is_some() {
                track.packaging = None;
            }
            if track.render_group.is_some() {
                track.render_group = None;
            }
            if track.alt_group.is_some() {
                track.alt_group = None;
            }
        }

        common
    }
}
