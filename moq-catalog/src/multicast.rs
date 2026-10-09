// SPDX-License-Identifier: MIT OR Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MulticastProtocol {
    Ssm,
    Asm,
    Amt,
    Atsc3,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AmtDiscovery {
    Driad,
    Manual,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum NetworkSource {
    #[serde(rename = "amt")]
    Amt {
        /// AMT relay discovery method. OPTIONAL per
        /// draft-ramadan-moq-multicast-00 §7.4 — an omitted value means the
        /// same thing as `"driad"` in the subscriber's discovery order, but is
        /// kept absent here so a catalog round-trips without gaining a field
        /// its publisher never emitted.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        discovery: Option<AmtDiscovery>,
        #[serde(skip_serializing_if = "Option::is_none")]
        relay: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        port: Option<u16>,
    },
    #[serde(rename = "atsc3")]
    Atsc3 {
        frequency: u64,
        #[serde(rename = "plpId")]
        plp_id: u8,
        #[serde(rename = "serviceId")]
        service_id: u16,
        #[serde(rename = "slsUri")]
        sls_uri: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MulticastTrackRef {
    pub name: String,
    #[serde(rename = "packetId")]
    pub packet_id: u16,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MulticastEndpoint {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<MulticastProtocol>,
    #[serde(rename = "sourceAddress", skip_serializing_if = "Option::is_none")]
    pub source_address: Option<String>,
    #[serde(rename = "groupAddress")]
    pub group_address: String,
    pub port: u16,
    pub tracks: Vec<MulticastTrackRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bandwidth: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct MulticastConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoints: Option<Vec<MulticastEndpoint>>,
    #[serde(rename = "networkSource", skip_serializing_if = "Option::is_none")]
    pub network_source: Option<Vec<NetworkSource>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_source_always_serializes_as_array() {
        let cfg = MulticastConfig {
            endpoints: Some(vec![]),
            network_source: Some(vec![NetworkSource::Amt {
                discovery: Some(AmtDiscovery::Driad),
                relay: None,
                port: None,
            }]),
        };
        let json = serde_json::to_value(cfg).unwrap();
        assert!(json["networkSource"].is_array());
    }

    #[test]
    fn absent_optional_fields_are_omitted() {
        let json = serde_json::to_value(MulticastConfig::default()).unwrap();
        assert_eq!(json, serde_json::json!({}));
    }

    #[test]
    fn atsc3_round_trips_without_dropping_fields() {
        let json = r#"{
            "type":"atsc3",
            "frequency":587000000,
            "plpId":0,
            "serviceId":101,
            "slsUri":"https://example.test/service/101/sls.xml"
        }"#;
        let source: NetworkSource = serde_json::from_str(json).unwrap();
        let emitted = serde_json::to_value(source).unwrap();
        assert_eq!(emitted["frequency"], 587000000_u64);
        assert_eq!(emitted["plpId"], 0);
        assert_eq!(emitted["serviceId"], 101);
        assert_eq!(
            emitted["slsUri"],
            "https://example.test/service/101/sls.xml"
        );
    }

    #[test]
    fn amt_round_trips_port() {
        let json = r#"{"type":"amt","discovery":"manual","relay":"relay.test","port":2268}"#;
        let source: NetworkSource = serde_json::from_str(json).unwrap();
        assert_eq!(serde_json::to_value(source).unwrap()["port"], 2268);
    }

    /// BLO-17758: `discovery` is OPTIONAL per draft-ramadan-moq-multicast-00
    /// §7.4, and no producer in the tree emits it. Requiring it here made
    /// `moq-sub --catalog` fail with `missing field 'discovery'` against the
    /// live `nasa/iss/a` broadcast, so no media was ever requested. This is
    /// the exact wire form the deployed publisher emits.
    #[test]
    fn amt_parses_without_discovery_and_does_not_invent_one() {
        let json = r#"{"relay":"69.25.95.128","type":"amt"}"#;
        let source: NetworkSource = serde_json::from_str(json).unwrap();
        assert!(
            matches!(
                source,
                NetworkSource::Amt {
                    discovery: None,
                    ..
                }
            ),
            "omitted discovery must parse as None"
        );
        let emitted = serde_json::to_value(&source).unwrap();
        assert!(
            emitted.as_object().unwrap().get("discovery").is_none(),
            "absent discovery must stay absent on re-serialize: {emitted}"
        );
    }

    /// The whole `multicast` object as served live, not just one source — this
    /// is what actually failed to deserialize at column 498.
    #[test]
    fn live_multicast_config_deserializes() {
        let json = r#"{"endpoints":[{"protocol":"ssm","sourceAddress":"69.25.95.192","groupAddress":"232.1.1.60","port":8000,"tracks":[{"name":"video/720p","packetId":1}]}],"networkSource":[{"relay":"69.25.95.128","type":"amt"}]}"#;
        let cfg: MulticastConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.network_source.as_ref().unwrap().len(), 1);
    }
}
