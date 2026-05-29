// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Multicast extension to the MoQ catalog format.
// Per draft-ramadan-moq-multicast-00 §4.1, §4.2.
// Field shape matches the multicast draft normatively (catalog draft is
// silent on multicast; multicast draft is the source of truth for these
// field names and semantics).

use serde::{Deserialize, Serialize};

/// JSON-compatible "one object OR array" carrier.
///
/// Used for fields the spec defines as accepting either a single object
/// or an array of objects (e.g. `networkSource` per §4.2.3). Round-trips
/// preserve the input form (a single object does NOT become a one-element
/// array on re-serialize).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

/// Network source descriptor for multicast transport discovery.
///
/// Per draft-ramadan-moq-multicast §4.2. The `type` field is the
/// discriminator; type-specific fields vary by source kind. This struct
/// carries the union of fields used by the AMT type (§4.2.1) — the most
/// common bridge case. Future types (atsc3, etc.) can extend this struct
/// with `#[serde(skip_serializing_if = "Option::is_none")]` optional fields.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct NetworkSource {
    /// Source type identifier. Defined values per §4.2:
    /// "amt", "atsc3".
    #[serde(rename = "type")]
    pub source_type: String,

    /// AMT relay address (IP or hostname). Per §4.2.1, OPTIONAL — subscribers
    /// fall back to discovery when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relay: Option<String>,

    /// Relay discovery method. Per §4.2.1: "driad" (RFC 8777) or "manual".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discovery: Option<String>,
}

/// A track reference in a multicast endpoint.
///
/// Per draft-ramadan-moq-multicast §4.1: each `tracks[]` entry is an object
/// with `name` (REQUIRED) and `packetId` (REQUIRED).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct MulticastTrackRef {
    /// Track name corresponding to the track identifier used in the catalog.
    pub name: String,

    /// MMTP packet_id used for packet-level track routing on multicast.
    /// Maps directly to the Packet ID field in the MMTP header
    /// (draft-ramadan-moq-mmt §3.1). Values MUST be unique within an
    /// (sourceAddress, groupAddress, port) tuple.
    #[serde(rename = "packetId")]
    pub packet_id: u16,
}

/// A multicast endpoint in the extended catalog format.
///
/// Each endpoint maps a set of tracks to a specific (S,G,port) multicast
/// group. Per draft-ramadan-moq-multicast §4.1.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct MulticastEndpoint {
    /// Transport protocol. OPTIONAL — defaults to "ssm" when sourceAddress
    /// is present, "asm" otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,

    /// SSM source IP address (RFC 4607). Required for SSM; if omitted,
    /// implies ASM.
    #[serde(rename = "sourceAddress", skip_serializing_if = "Option::is_none")]
    pub source_address: Option<String>,

    /// Multicast group address. REQUIRED.
    /// SSM range: 232.0.0.0/8 (IPv4), ff3x::/32 (IPv6).
    /// ASM range: 224.0.0.0/4 (IPv4), ff0x::/16 (IPv6).
    #[serde(rename = "groupAddress")]
    pub group_address: String,

    /// UDP port number. REQUIRED.
    pub port: u16,

    /// Tracks available on this endpoint with their MMTP packet_ids. REQUIRED.
    pub tracks: Vec<MulticastTrackRef>,

    /// Aggregate endpoint bandwidth in bits per second. RECOMMENDED so that
    /// subscribers can do capacity-aware join decisions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bandwidth: Option<u64>,

    /// Per-endpoint network source override. OPTIONAL. May also appear at
    /// the top-level MulticastConfig to apply to all endpoints.
    #[serde(rename = "networkSource", skip_serializing_if = "Option::is_none")]
    pub network_source: Option<OneOrMany<NetworkSource>>,
}

/// Top-level multicast transport configuration.
///
/// Per draft-ramadan-moq-multicast §4.1: only `endpoints[]` and the
/// top-level `networkSource` are defined. Earlier hang-mmt-fec versions
/// carried "simple form" fields (top-level groupAddress/port/sourceAddress)
/// as a one-endpoint shortcut; those are NOT in the spec and are not carried
/// here. Publishers needing single-endpoint shapes use a one-element
/// `endpoints` array.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(default)]
pub struct MulticastConfig {
    /// Per-endpoint multicast groups.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoints: Option<Vec<MulticastEndpoint>>,

    /// Default network source applied to all endpoints. Single object or
    /// array per §4.2.3.
    #[serde(rename = "networkSource", skip_serializing_if = "Option::is_none")]
    pub network_source: Option<OneOrMany<NetworkSource>>,

    /// Subgroup history window, in MoQ groups, the publisher retains per track
    /// to bound memory (and serve catching-up subscribers). Under Mapping B a
    /// group holds many concurrent subgroups (Init + one per MFU); the publisher
    /// prunes subgroups of groups older than this window. REQUIRED for MMTP
    /// publishing — the publisher errors if it is absent (config-or-throw, no
    /// silent unbounded default). Applies to source and `<name>/repair` tracks
    /// alike. Local deployment-policy extension; not in
    /// draft-ramadan-moq-multicast.
    #[serde(rename = "subgroupHistoryGroups", skip_serializing_if = "Option::is_none")]
    pub subgroup_history_groups: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- spec-alignment tests (draft-ramadan-moq-multicast-00 §4.1, §4.2.3) ----

    #[test]
    fn endpoint_protocol_is_optional() {
        // Per §4.1: protocol is OPTIONAL, defaults to "ssm" when sourceAddress present.
        let json = r#"{
            "sourceAddress": "10.0.0.1",
            "groupAddress": "232.1.1.1",
            "port": 5004,
            "tracks": [{"name": "v", "packetId": 1}]
        }"#;
        let ep: MulticastEndpoint = serde_json::from_str(json).unwrap();
        assert!(ep.protocol.is_none(), "protocol must be Option");
    }

    #[test]
    fn endpoint_omits_protocol_when_none() {
        let ep = MulticastEndpoint {
            protocol: None,
            source_address: Some("10.0.0.1".into()),
            group_address: "232.1.1.1".into(),
            port: 5004,
            tracks: vec![],
            bandwidth: None,
            network_source: None,
        };
        let json = serde_json::to_string(&ep).unwrap();
        assert!(!json.contains("protocol"), "json = {json}");
    }

    #[test]
    fn endpoint_accepts_network_source_per_endpoint() {
        // Per §4.1 last paragraph: networkSource may appear on individual endpoints.
        let json = r#"{
            "groupAddress": "232.1.1.1",
            "port": 5004,
            "tracks": [],
            "networkSource": {"type": "amt", "relay": "amt.example.com", "discovery": "driad"}
        }"#;
        let ep: MulticastEndpoint = serde_json::from_str(json).unwrap();
        assert!(ep.network_source.is_some());
    }

    #[test]
    fn config_has_no_simple_form_fields() {
        // The spec defines only `endpoints[]` and `networkSource` at the
        // top level. Simple-form fields (groupAddress/port/sourceAddress
        // on the MulticastConfig itself) were a hang-mmt-fec extension and
        // are not normative. A pure spec parse with extended-form input
        // must populate `endpoints`.
        let json = r#"{
            "endpoints": [{
                "groupAddress": "232.1.1.1",
                "port": 5004,
                "tracks": []
            }]
        }"#;
        let cfg: MulticastConfig = serde_json::from_str(json).unwrap();
        let eps = cfg.endpoints.expect("endpoints present");
        assert_eq!(eps.len(), 1);
    }

    #[test]
    fn network_source_accepts_single_object() {
        // Per §4.2.3: networkSource MAY be a single object.
        let json = r#"{"type":"amt","relay":"amt.example.com","discovery":"driad"}"#;
        let ns: OneOrMany<NetworkSource> = serde_json::from_str(json).unwrap();
        match ns {
            OneOrMany::One(n) => assert_eq!(n.source_type, "amt"),
            OneOrMany::Many(_) => panic!("expected One"),
        }
    }

    #[test]
    fn network_source_accepts_array() {
        // Per §4.2.3: networkSource MAY be an array.
        let json = r#"[
            {"type":"amt","relay":"a","discovery":"driad"},
            {"type":"amt","relay":"b","discovery":"driad"}
        ]"#;
        let ns: OneOrMany<NetworkSource> = serde_json::from_str(json).unwrap();
        match ns {
            OneOrMany::One(_) => panic!("expected Many"),
            OneOrMany::Many(arr) => assert_eq!(arr.len(), 2),
        }
    }

    #[test]
    fn one_or_many_round_trips() {
        let single = OneOrMany::One(NetworkSource {
            source_type: "amt".into(),
            relay: Some("amt.example.com".into()),
            discovery: Some("driad".into()),
        });
        let json = serde_json::to_string(&single).unwrap();
        // Single form must serialize as a bare object, not a one-element array.
        assert!(json.starts_with('{'), "json = {json}");
        let back: OneOrMany<NetworkSource> = serde_json::from_str(&json).unwrap();
        match back {
            OneOrMany::One(n) => assert_eq!(n.source_type, "amt"),
            OneOrMany::Many(_) => panic!("round-trip lost single form"),
        }
    }

    // ---- serde round-trip tests for individual structs ----

    #[test]
    fn track_ref_serde_round_trip() {
        let r = MulticastTrackRef {
            name: "video-1080p".into(),
            packet_id: 17,
        };
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(json, r#"{"name":"video-1080p","packetId":17}"#);
        let back: MulticastTrackRef = serde_json::from_str(&json).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn endpoint_extended_form_round_trips() {
        let ep = MulticastEndpoint {
            protocol: Some("ssm".into()),
            source_address: Some("69.25.95.10".into()),
            group_address: "232.0.10.1".into(),
            port: 5004,
            tracks: vec![
                MulticastTrackRef {
                    name: "audio".into(),
                    packet_id: 1,
                },
                MulticastTrackRef {
                    name: "video".into(),
                    packet_id: 2,
                },
            ],
            bandwidth: Some(5_000_000),
            network_source: None,
        };
        let json = serde_json::to_string(&ep).unwrap();
        let back: MulticastEndpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ep);
    }

    #[test]
    fn endpoint_omits_optional_fields_when_none() {
        let ep = MulticastEndpoint {
            protocol: None,
            source_address: None,
            group_address: "239.1.2.3".into(),
            port: 1234,
            tracks: vec![],
            bandwidth: None,
            network_source: None,
        };
        let json = serde_json::to_string(&ep).unwrap();
        assert!(!json.contains("sourceAddress"), "json = {json}");
        assert!(!json.contains("bandwidth"), "json = {json}");
        assert!(!json.contains("networkSource"), "json = {json}");
    }

    #[test]
    fn config_default_is_all_none() {
        let cfg = MulticastConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        // An empty MulticastConfig must serialize to {} — both fields optional.
        assert_eq!(json, "{}");
    }

    #[test]
    fn network_source_amt_round_trip() {
        let ns = NetworkSource {
            source_type: "amt".into(),
            relay: Some("amt-relay.example.com".into()),
            discovery: Some("driad".into()),
        };
        let json = serde_json::to_string(&ns).unwrap();
        let back: NetworkSource = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ns);
        assert!(json.contains("\"type\":\"amt\""), "json = {json}");
        assert!(json.contains("\"discovery\":\"driad\""), "json = {json}");
    }
}
