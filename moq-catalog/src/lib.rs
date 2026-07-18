// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Canonical Rust model and validator for the MoQ MSF catalog used by MMTP.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub mod multicast;
pub use multicast::{
    AmtDiscovery, MulticastConfig, MulticastEndpoint, MulticastProtocol, MulticastTrackRef,
    NetworkSource,
};

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Root {
    pub version: u16,
    #[serde(rename = "streamingFormat")]
    pub streaming_format: String,
    #[serde(rename = "streamingFormatVersion")]
    pub streaming_format_version: String,
    #[serde(
        rename = "supportsDeltaUpdates",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub supports_delta_updates: Option<bool>,
    pub tracks: Vec<Track>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multicast: Option<MulticastConfig>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
#[serde(deny_unknown_fields)]
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
    #[serde(rename = "isLive", skip_serializing_if = "Option::is_none")]
    pub is_live: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    #[serde(rename = "mimeType", skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub framerate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bitrate: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub samplerate: Option<u64>,
    #[serde(rename = "channelConfig", skip_serializing_if = "Option::is_none")]
    pub channel_config: Option<String>,
    #[serde(rename = "mmtpMode", skip_serializing_if = "Option::is_none")]
    pub mmtp_mode: Option<MmtpMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timescale: Option<u32>,
    #[serde(rename = "groupDurationMs", skip_serializing_if = "Option::is_none")]
    pub group_duration_ms: Option<u32>,
    #[serde(rename = "groupDurationTicks", skip_serializing_if = "Option::is_none")]
    pub group_duration_ticks: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fec: Option<FecDescriptor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,
    #[serde(rename = "renderGroup", skip_serializing_if = "Option::is_none")]
    pub render_group: Option<u32>,
    #[serde(rename = "altGroup", skip_serializing_if = "Option::is_none")]
    pub alt_group: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depends: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Default)]
#[serde(rename_all = "kebab-case")]
pub enum TrackPackaging {
    #[default]
    Cmaf,
    Loc,
    Mmtp,
    FecRepair,
    Datagram,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum MmtpMode {
    Mpu,
    Mfu,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
pub enum FecAlgorithm {
    #[serde(rename = "raptorq")]
    RaptorQ,
    #[serde(rename = "reed-solomon")]
    ReedSolomon,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum FecMode {
    Interleaved,
    Object,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct FecDescriptor {
    pub algorithm: FecAlgorithm,
    #[serde(rename = "sourceSymbols")]
    pub source_symbols: u32,
    #[serde(rename = "repairSymbols")]
    pub repair_symbols: u32,
    #[serde(rename = "symbolSize")]
    pub symbol_size: u32,
    #[serde(rename = "interleaveDepthMs", skip_serializing_if = "Option::is_none")]
    pub interleave_depth_ms: Option<u32>,
    #[serde(rename = "repairTrack")]
    pub repair_track: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<FecMode>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CatalogValidationError {
    InvalidRoot(&'static str),
    InvalidTrack {
        track_name: String,
        reason: &'static str,
    },
    MissingMmtpMode {
        track_name: String,
    },
    MissingTimescale {
        track_name: String,
    },
    MissingGroupDuration {
        track_name: String,
    },
    GroupDurationNotExact {
        track_name: String,
        group_duration_ms: u32,
        timescale: u32,
    },
    ZeroTimescale {
        track_name: String,
    },
    ZeroGroupDuration {
        track_name: String,
    },
    InvalidFecParams {
        track_name: String,
        reason: &'static str,
    },
    UnknownRepairTrack {
        track_name: String,
        repair_track: String,
    },
    InvalidRepairTrack {
        track_name: String,
        reason: &'static str,
    },
    SwitchingSetGroupDurationMismatch {
        alt_group: u32,
        track_name: String,
        other_track: String,
    },
    InvalidMulticast(&'static str),
    UnknownTrackReference {
        track_name: String,
    },
    DuplicatePacketId {
        packet_id: u16,
        first_track: String,
        duplicate_track: String,
    },
}

impl std::fmt::Display for CatalogValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRoot(reason) => write!(f, "invalid catalog root: {reason}"),
            Self::InvalidTrack { track_name, reason } => write!(f, "invalid track `{track_name}`: {reason}"),
            Self::MissingMmtpMode { track_name } => write!(f, "MMTP track `{track_name}` is missing mmtpMode"),
            Self::MissingTimescale { track_name } => write!(f, "MMTP track `{track_name}` is missing timescale"),
            Self::MissingGroupDuration { track_name } => write!(f, "MMTP track `{track_name}` is missing group duration"),
            Self::GroupDurationNotExact { track_name, group_duration_ms, timescale } => write!(f, "track `{track_name}` groupDurationMs {group_duration_ms} is not exact at timescale {timescale}"),
            Self::ZeroTimescale { track_name } => write!(f, "track `{track_name}` has zero timescale"),
            Self::ZeroGroupDuration { track_name } => write!(f, "track `{track_name}` has zero group duration"),
            Self::InvalidFecParams { track_name, reason } => write!(f, "track `{track_name}` has invalid FEC parameters: {reason}"),
            Self::UnknownRepairTrack { track_name, repair_track } => write!(f, "track `{track_name}` references unknown repair track `{repair_track}`"),
            Self::InvalidRepairTrack { track_name, reason } => write!(f, "repair track `{track_name}` is invalid: {reason}"),
            Self::SwitchingSetGroupDurationMismatch { alt_group, track_name, other_track } => write!(f, "switching set {alt_group} has mismatched durations on `{track_name}` and `{other_track}`"),
            Self::InvalidMulticast(reason) => write!(f, "invalid multicast configuration: {reason}"),
            Self::UnknownTrackReference { track_name } => write!(f, "multicast references unknown track `{track_name}`"),
            Self::DuplicatePacketId { packet_id, first_track, duplicate_track } => write!(f, "packetId {packet_id} is shared by `{first_track}` and `{duplicate_track}` in one endpoint"),
        }
    }
}

impl std::error::Error for CatalogValidationError {}

impl Root {
    pub fn validate(&self) -> Result<(), CatalogValidationError> {
        if self.version != 1 {
            return Err(CatalogValidationError::InvalidRoot("version must be 1"));
        }
        if self.streaming_format != "mmtp" {
            return Err(CatalogValidationError::InvalidRoot(
                "streamingFormat must be mmtp",
            ));
        }
        if self.streaming_format_version.is_empty() {
            return Err(CatalogValidationError::InvalidRoot(
                "streamingFormatVersion must not be empty",
            ));
        }
        if self.tracks.is_empty() {
            return Err(CatalogValidationError::InvalidRoot(
                "tracks must not be empty",
            ));
        }

        for track in &self.tracks {
            if track.name.is_empty() {
                return Err(CatalogValidationError::InvalidTrack {
                    track_name: track.name.clone(),
                    reason: "name must not be empty",
                });
            }
            let packaging =
                track
                    .packaging
                    .as_ref()
                    .ok_or_else(|| CatalogValidationError::InvalidTrack {
                        track_name: track.name.clone(),
                        reason: "packaging is required",
                    })?;
            match packaging {
                TrackPackaging::Mmtp => self.validate_mmtp_track(track)?,
                TrackPackaging::FecRepair => self.validate_repair_track(track)?,
                _ => {}
            }
        }
        self.validate_fec_references()?;
        self.validate_switching_sets()?;
        if let Some(multicast) = &self.multicast {
            self.validate_multicast(multicast)?;
        }
        Ok(())
    }

    fn validate_mmtp_track(&self, track: &Track) -> Result<(), CatalogValidationError> {
        if track.mmtp_mode.is_none() {
            return Err(CatalogValidationError::MissingMmtpMode {
                track_name: track.name.clone(),
            });
        }
        let timescale =
            track
                .timescale
                .ok_or_else(|| CatalogValidationError::MissingTimescale {
                    track_name: track.name.clone(),
                })?;
        if timescale == 0 {
            return Err(CatalogValidationError::ZeroTimescale {
                track_name: track.name.clone(),
            });
        }
        if track.group_duration_ms.is_none() && track.group_duration_ticks.is_none() {
            return Err(CatalogValidationError::MissingGroupDuration {
                track_name: track.name.clone(),
            });
        }
        if matches!(track.group_duration_ms, Some(0))
            || matches!(track.group_duration_ticks, Some(0))
        {
            return Err(CatalogValidationError::ZeroGroupDuration {
                track_name: track.name.clone(),
            });
        }
        if let (Some(ms), None) = (track.group_duration_ms, track.group_duration_ticks) {
            #[allow(clippy::manual_is_multiple_of)] // Keep the crate's Rust 1.70 MSRV.
            if (ms as u64 * timescale as u64) % 1000 != 0 {
                return Err(CatalogValidationError::GroupDurationNotExact {
                    track_name: track.name.clone(),
                    group_duration_ms: ms,
                    timescale,
                });
            }
        }
        if let Some(fec) = &track.fec {
            if fec.source_symbols == 0 || fec.repair_symbols == 0 || fec.symbol_size == 0 {
                return Err(CatalogValidationError::InvalidFecParams {
                    track_name: track.name.clone(),
                    reason: "symbol counts and symbolSize must be positive",
                });
            }
            if fec.algorithm == FecAlgorithm::RaptorQ && fec.symbol_size % 8 != 0 {
                return Err(CatalogValidationError::InvalidFecParams {
                    track_name: track.name.clone(),
                    reason: "RaptorQ symbolSize must be divisible by 8",
                });
            }
            if matches!(fec.interleave_depth_ms, Some(0)) {
                return Err(CatalogValidationError::InvalidFecParams {
                    track_name: track.name.clone(),
                    reason: "interleaveDepthMs must be positive when present",
                });
            }
        }
        Ok(())
    }

    fn validate_repair_track(&self, track: &Track) -> Result<(), CatalogValidationError> {
        if track.priority != Some(240) {
            return Err(CatalogValidationError::InvalidRepairTrack {
                track_name: track.name.clone(),
                reason: "priority must be 240",
            });
        }
        if track.mmtp_mode.is_some()
            || track.timescale.is_some()
            || track.group_duration_ms.is_some()
            || track.group_duration_ticks.is_some()
            || track.fec.is_some()
        {
            return Err(CatalogValidationError::InvalidRepairTrack {
                track_name: track.name.clone(),
                reason: "MMTP timing and fec fields are forbidden",
            });
        }
        Ok(())
    }

    fn validate_fec_references(&self) -> Result<(), CatalogValidationError> {
        for track in &self.tracks {
            let Some(fec) = &track.fec else { continue };
            let valid = self.tracks.iter().any(|candidate| {
                candidate.name == fec.repair_track
                    && candidate.packaging == Some(TrackPackaging::FecRepair)
            });
            if !valid {
                return Err(CatalogValidationError::UnknownRepairTrack {
                    track_name: track.name.clone(),
                    repair_track: fec.repair_track.clone(),
                });
            }
        }
        Ok(())
    }

    fn validate_switching_sets(&self) -> Result<(), CatalogValidationError> {
        let mut anchors: HashMap<u32, (u128, u128, String)> = HashMap::new();
        for track in &self.tracks {
            if track.packaging != Some(TrackPackaging::Mmtp) {
                continue;
            }
            let Some(alt_group) = track.alt_group else {
                continue;
            };
            let timescale = track.timescale.expect("MMTP validation requires timescale") as u128;
            let ticks = track
                .group_duration_ticks
                .map(u128::from)
                .unwrap_or_else(|| {
                    u128::from(
                        track
                            .group_duration_ms
                            .expect("MMTP validation requires duration"),
                    ) * timescale
                        / 1000
                });
            if let Some((anchor_ticks, anchor_timescale, anchor_name)) = anchors.get(&alt_group) {
                if ticks * anchor_timescale != anchor_ticks * timescale {
                    return Err(CatalogValidationError::SwitchingSetGroupDurationMismatch {
                        alt_group,
                        track_name: track.name.clone(),
                        other_track: anchor_name.clone(),
                    });
                }
            } else {
                anchors.insert(alt_group, (ticks, timescale, track.name.clone()));
            }
        }
        Ok(())
    }

    fn validate_multicast(
        &self,
        multicast: &MulticastConfig,
    ) -> Result<(), CatalogValidationError> {
        let endpoints = multicast
            .endpoints
            .as_ref()
            .filter(|value| !value.is_empty())
            .ok_or(CatalogValidationError::InvalidMulticast(
                "endpoints must not be empty",
            ))?;
        if let Some(network_sources) = &multicast.network_source {
            if network_sources.is_empty() {
                return Err(CatalogValidationError::InvalidMulticast(
                    "networkSource must not be empty when present",
                ));
            }
            for source in network_sources {
                match source {
                    NetworkSource::Amt { relay, port, .. } => {
                        if relay.as_deref() == Some("") || matches!(port, Some(0)) {
                            return Err(CatalogValidationError::InvalidMulticast(
                                "AMT relay must be nonempty and port must be positive",
                            ));
                        }
                    }
                    NetworkSource::Atsc3 {
                        frequency,
                        plp_id,
                        sls_uri,
                        ..
                    } => {
                        if *frequency == 0 || *plp_id > 63 || url::Url::parse(sls_uri).is_err() {
                            return Err(CatalogValidationError::InvalidMulticast(
                                "invalid ATSC3 frequency, plpId, or slsUri",
                            ));
                        }
                    }
                }
            }
        }
        for endpoint in endpoints {
            if endpoint.group_address.is_empty() || endpoint.port == 0 || endpoint.tracks.is_empty()
            {
                return Err(CatalogValidationError::InvalidMulticast(
                    "endpoint groupAddress, port, and tracks are required",
                ));
            }
            if endpoint.source_address.as_deref() == Some("") {
                return Err(CatalogValidationError::InvalidMulticast(
                    "sourceAddress must not be empty",
                ));
            }
            if endpoint.protocol.is_none() && endpoint.source_address.is_none() {
                return Err(CatalogValidationError::InvalidMulticast(
                    "endpoint requires protocol or sourceAddress",
                ));
            }
            if endpoint.protocol == Some(MulticastProtocol::Ssm)
                && endpoint.source_address.is_none()
            {
                return Err(CatalogValidationError::InvalidMulticast(
                    "SSM endpoint requires sourceAddress",
                ));
            }
            if matches!(endpoint.bandwidth, Some(0)) {
                return Err(CatalogValidationError::InvalidMulticast(
                    "bandwidth must be positive",
                ));
            }
            let mut packet_ids: HashMap<u16, &str> = HashMap::new();
            for track_ref in &endpoint.tracks {
                if track_ref.name.is_empty()
                    || !self.tracks.iter().any(|track| track.name == track_ref.name)
                {
                    return Err(CatalogValidationError::UnknownTrackReference {
                        track_name: track_ref.name.clone(),
                    });
                }
                if let Some(first) = packet_ids.insert(track_ref.packet_id, &track_ref.name) {
                    return Err(CatalogValidationError::DuplicatePacketId {
                        packet_id: track_ref.packet_id,
                        first_track: first.to_string(),
                        duplicate_track: track_ref.name.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_and_validate(json: &str) -> Root {
        let root: Root = serde_json::from_str(json).unwrap();
        root.validate().unwrap();
        root
    }

    #[test]
    fn accepts_flat_msf_track() {
        let root = parse_and_validate(
            r#"{
            "version":1,"streamingFormat":"mmtp","streamingFormatVersion":"draft-ramadan-moq-mmt-00",
            "tracks":[{"name":"video","packaging":"mmtp","codec":"avc1.640028","framerate":29.97,"mmtpMode":"mfu","timescale":90000,"groupDurationTicks":3000}]
        }"#,
        );
        assert_eq!(root.tracks[0].codec.as_deref(), Some("avc1.640028"));
        assert_eq!(root.tracks[0].framerate, Some(29.97));
    }

    #[test]
    fn rejects_nested_selection_params() {
        let json = r#"{"version":1,"streamingFormat":"mmtp","streamingFormatVersion":"x","tracks":[{"name":"v","packaging":"mmtp","selectionParams":{"codec":"avc1"},"mmtpMode":"mfu","timescale":90000,"groupDurationMs":1000}]}"#;
        assert!(serde_json::from_str::<Root>(json).is_err());
    }

    #[test]
    fn validates_repair_priority_and_reference() {
        parse_and_validate(
            r#"{
            "version":1,"streamingFormat":"mmtp","streamingFormatVersion":"x",
            "tracks":[
              {"name":"v","packaging":"mmtp","mmtpMode":"mfu","timescale":90000,"groupDurationMs":1000,"fec":{"algorithm":"raptorq","sourceSymbols":8,"repairSymbols":4,"symbolSize":1312,"repairTrack":"v-repair"}},
              {"name":"v-repair","packaging":"fec-repair","priority":240}
            ]
        }"#,
        );
    }

    #[test]
    fn accepts_equal_wall_clock_switching_set_durations() {
        parse_and_validate(
            r#"{
            "version":1,"streamingFormat":"mmtp","streamingFormatVersion":"x",
            "tracks":[
              {"name":"v","packaging":"mmtp","mmtpMode":"mfu","timescale":90000,"groupDurationTicks":360000,"altGroup":2},
              {"name":"a","packaging":"mmtp","mmtpMode":"mfu","timescale":48000,"groupDurationTicks":192000,"altGroup":2}
            ]
        }"#,
        );
    }

    #[test]
    fn omitted_protocol_defaults_to_ssm_when_source_is_present() {
        parse_and_validate(
            r#"{
            "version":1,"streamingFormat":"mmtp","streamingFormatVersion":"x",
            "tracks":[{"name":"v","packaging":"mmtp","mmtpMode":"mfu","timescale":90000,"groupDurationMs":1000}],
            "multicast":{"endpoints":[{"sourceAddress":"192.0.2.1","groupAddress":"232.1.1.1","port":5000,"tracks":[{"name":"v","packetId":1}]}],"networkSource":[{"type":"amt","discovery":"driad"}]}
        }"#,
        );
    }

    #[test]
    fn multicast_network_source_is_optional() {
        parse_and_validate(
            r#"{
            "version":1,"streamingFormat":"mmtp","streamingFormatVersion":"x",
            "tracks":[{"name":"v","packaging":"mmtp","mmtpMode":"mfu","timescale":90000,"groupDurationMs":1000}],
            "multicast":{"endpoints":[{"sourceAddress":"192.0.2.1","groupAddress":"232.1.1.1","port":5000,"tracks":[{"name":"v","packetId":1}]}]}
        }"#,
        );
    }
}
