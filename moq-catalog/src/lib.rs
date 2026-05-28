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
        ] {
            let json = serde_json::to_string(&v).unwrap();
            assert_eq!(json, expected, "serialize {v:?}");
            let back: TrackPackaging = serde_json::from_str(&json).unwrap();
            assert_eq!(back, v, "round-trip {v:?}");
        }
    }
}

impl Track {
    #[allow(dead_code)] // TODO use
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

#[derive(Serialize, Deserialize, Debug, Default)]
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
