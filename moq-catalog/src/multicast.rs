// SPDX-License-Identifier: MIT OR Apache-2.0

//! Multicast catalog extension (draft-ramadan-moq-multicast-00 §4).
//!
//! Two documents define this wire format and they do not always agree: the
//! draft, and libmmt's `packages/container/schemas/catalog.schema.json`, which
//! `tests/fixtures/README.md` names as the other implementation. When they
//! disagree:
//!
//! - Parsing follows the draft. This crate is a reader, so a form the draft
//!   allows is accepted even where libmmt's schema rejects it: AMT without
//!   `discovery`, ATSC 3.0 without `plpId`/`serviceId`/`slsUri`,
//!   endpoint-level `networkSource`, and `auth` nested in `multicast`. Each is
//!   a deliberate divergence from libmmt.
//! - libmmt governs the shared corpus, and so what a producer should emit.
//!   That README's contract is that every `positive/` file validates in both
//!   and every `negative/` file is rejected by both. A draft-only form above
//!   therefore cannot be a `positive/` fixture (unit tests here pin it
//!   instead), and a form libmmt rejects with a shared `negative/` vector
//!   stays rejected here whatever the draft says. No live instance of that
//!   second case: the object form of `MulticastConfig::network_source` was
//!   one until the draft itself went array-only on 2026-10-05.

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
        /// draft-ramadan-moq-multicast-00 §4.2.1 — an omitted value means the
        /// same thing as `"driad"` in the subscriber's discovery order, but is
        /// kept absent here so a catalog round-trips without gaining a field
        /// its publisher never emitted.
        ///
        /// `Option` alone is what makes the field optional on the wire; serde
        /// deserializes a missing `Option` as `None` with no `default` needed.
        /// `skip_serializing_if` is the attribute doing the round-trip work.
        #[serde(skip_serializing_if = "Option::is_none")]
        discovery: Option<AmtDiscovery>,
        #[serde(skip_serializing_if = "Option::is_none")]
        relay: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        port: Option<u16>,
    },
    /// ATSC 3.0 broadcast source (§4.2.2).
    ///
    /// libmmt's `$defs/networkSource` atsc3 branch requires `plpId`,
    /// `serviceId` and `slsUri`; the draft makes all three OPTIONAL, and so
    /// does this crate. A source omitting any of them parses here and fails
    /// libmmt's schema: a deliberate divergence from libmmt, the same one
    /// `discovery` above has carried since BLO-17758. See the module doc.
    #[serde(rename = "atsc3")]
    Atsc3 {
        /// RF center frequency. The one REQUIRED ATSC 3.0 field per
        /// draft-ramadan-moq-multicast-00 §4.2.2.
        ///
        /// No unit is claimed here because the authorities disagree: the
        /// draft says kHz (its example is `533000`), libmmt types it as a bare
        /// `positiveInteger`, and libmmt's corpus vector `fec-multicast.json`
        /// carries `587000000`, a UHF channel only in Hz. Nothing in `moq-rs`
        /// interprets the value and validation checks only that it is
        /// non-zero, so the unit is a cross-repo question. This crate's tests
        /// follow the corpus.
        frequency: u64,
        /// Physical Layer Pipe ID. OPTIONAL per §4.2.2, which gives it a
        /// default of 0 (base PLP).
        ///
        /// That default is applied where the value is *used*, not at parse:
        /// `#[serde(default)]` on a bare `u8` would materialise `"plpId": 0`
        /// on re-serialize for a publisher that never emitted it, which the
        /// golden round-trip harness reads as an invented field. Same reason
        /// `discovery` above carries no default. Read it as
        /// `plp_id.unwrap_or(0)`.
        #[serde(rename = "plpId", skip_serializing_if = "Option::is_none")]
        plp_id: Option<u8>,
        /// Service ID within the broadcast multiplex. OPTIONAL per §4.2.2.
        #[serde(rename = "serviceId", skip_serializing_if = "Option::is_none")]
        service_id: Option<u16>,
        /// Service Layer Signaling bootstrap URI. OPTIONAL per §4.2.2.
        #[serde(rename = "slsUri", skip_serializing_if = "Option::is_none")]
        sls_uri: Option<String>,
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
    /// Per-endpoint network delivery configuration. `(array of objects,
    /// OPTIONAL)` per draft-ramadan-moq-multicast-00 §4.1, which places it
    /// either here or on the enclosing `multicast` object: §4.2 says it "MAY
    /// appear at the `multicast` level (applying to all endpoints) or on
    /// individual endpoints; a single source is expressed as a one-element
    /// array."
    ///
    /// Without this field `deny_unknown_fields` above turned the per-endpoint
    /// placement into a hard `unknown field` parse failure — the same class of
    /// break as the `missing field 'discovery'` one that cost BLO-17758 a run,
    /// with the sign flipped.
    ///
    /// Array-only, matching both the draft and `MulticastConfig::network_source`.
    ///
    /// libmmt's `$defs/endpoint` is `additionalProperties: false` without
    /// `networkSource`, so it rejects an endpoint carrying one. Accepting it
    /// here is a deliberate divergence from libmmt; see the module doc.
    #[serde(rename = "networkSource", skip_serializing_if = "Option::is_none")]
    pub network_source: Option<Vec<NetworkSource>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct MulticastConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoints: Option<Vec<MulticastEndpoint>>,
    /// Network delivery configuration applying to all endpoints. `(array of
    /// objects, OPTIONAL)` per draft-ramadan-moq-multicast-00 §4.1; §4.2 adds
    /// that "a single source is expressed as a one-element array."
    ///
    /// Array-only here agrees with both authorities: libmmt pins an array, and
    /// the draft has typed this an array since the 2026-10-05 sync
    /// (moqcast-draft `9088406f`). The bare-object form earlier drafts allowed
    /// is held as a byte-identical libmmt mirror in the golden negative fixture
    /// `network-source-object` (`Reject::Parse`). Tracked on BLO-41985.
    #[serde(rename = "networkSource", skip_serializing_if = "Option::is_none")]
    pub network_source: Option<Vec<NetworkSource>>,
    /// Multicast content-authentication block, OPTIONAL per
    /// draft-ramadan-moq-multicast-00, which lists it among the `multicast`
    /// object's members in §4.1 — "**auth** (object, OPTIONAL): Content
    /// authentication configuration for the bc-provenance profile; see
    /// Section 7.2" — i.e. nested inside `multicast`:
    /// `{"multicast": {"auth": {"scheme": "bc-provenance"}, ...}}`.
    ///
    /// Distinct from `Root::multicast_auth`, which is the *literal dotted root
    /// key* `"multicast.auth"` sitting beside `multicast` — libmmt's shape, and
    /// what the fleet actually emits (BLO-39866, #103). They are different
    /// JSON; carrying one did not carry the other, so the draft-shaped nesting
    /// still hard-failed `deny_unknown_fields`.
    ///
    /// Only the dotted key is legal in libmmt: its `$defs/multicast` is
    /// `additionalProperties: false` over `endpoints` and `networkSource`, so
    /// it rejects this nested form. Accepting it here is a deliberate
    /// divergence from libmmt; see the module doc for which authority wins.
    ///
    /// Untyped and unvalidated for the same reason `Root::multicast_auth` is:
    /// nothing in `moq-rs` verifies provenance, and a second partial definition
    /// of a security-relevant schema that nothing here consumes is the exact
    /// failure BLO-37534 was. §7.2's "scheme REQUIRED if auth present" is for
    /// whoever verifies the signature, against libmmt's schema.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<serde_json::Value>,
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
            ..Default::default()
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
    /// §4.2.1, and no producer in the tree emits it. Requiring it here made
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
        let sources = cfg.network_source.as_ref().unwrap();
        assert_eq!(sources.len(), 1);
        assert_eq!(
            sources[0],
            NetworkSource::Amt {
                discovery: None,
                relay: Some("69.25.95.128".to_string()),
                port: None,
            }
        );
    }

    /// BLO-41985 gap (a): `plpId`, `serviceId` and `slsUri` are all OPTIONAL
    /// per draft-ramadan-moq-multicast-00 §4.2.2 — only `frequency` is
    /// REQUIRED. Requiring them here made a spec-legal minimal ATSC 3.0 source
    /// fail with `missing field 'plpId'`, the same hard-parse break BLO-17758
    /// hit from the AMT side.
    #[test]
    fn atsc3_parses_with_only_frequency_and_does_not_invent_fields() {
        let json = r#"{"type":"atsc3","frequency":533000000}"#;
        let source: NetworkSource = serde_json::from_str(json).unwrap();
        assert!(
            matches!(
                source,
                NetworkSource::Atsc3 {
                    frequency: 533000000,
                    plp_id: None,
                    service_id: None,
                    sls_uri: None,
                }
            ),
            "omitted optional ATSC3 fields must parse as None, got {source:?}"
        );
        // §4.2.2's "Defaults to 0 (base PLP)" is a reader-side semantic, not a
        // field the catalog gains.
        let NetworkSource::Atsc3 { plp_id, .. } = &source else {
            unreachable!()
        };
        assert_eq!(plp_id.unwrap_or(0), 0, "absent plpId reads as base PLP 0");

        let emitted = serde_json::to_value(&source).unwrap();
        let obj = emitted.as_object().unwrap();
        for absent in ["plpId", "serviceId", "slsUri"] {
            assert!(
                obj.get(absent).is_none(),
                "absent {absent} must stay absent on re-serialize: {emitted}"
            );
        }
        assert_eq!(obj["frequency"], 533000000_u64);
    }

    /// BLO-41985 gap (b1): §4.1 types `networkSource` as OPTIONAL and says it
    /// "May appear on individual endpoints or at the top-level `multicast`
    /// object". `deny_unknown_fields` on `MulticastEndpoint` turned the
    /// per-endpoint placement into a hard `unknown field` failure.
    #[test]
    fn endpoint_level_network_source_parses_and_round_trips() {
        let json = r#"{"groupAddress":"232.1.1.60","port":8000,"tracks":[{"name":"video","packetId":1}],"networkSource":[{"type":"amt","relay":"69.25.95.128"}]}"#;
        let endpoint: MulticastEndpoint =
            serde_json::from_str(json).expect("per-endpoint networkSource must parse");
        assert_eq!(
            endpoint.network_source.as_deref(),
            Some(
                [NetworkSource::Amt {
                    discovery: None,
                    relay: Some("69.25.95.128".to_string()),
                    port: None,
                }]
                .as_slice()
            )
        );
        assert_eq!(
            serde_json::to_value(&endpoint).unwrap(),
            serde_json::from_str::<serde_json::Value>(json).unwrap(),
            "endpoint round-trip must be lossless"
        );
    }

    /// An endpoint that never carried `networkSource` must not gain one.
    #[test]
    fn endpoint_without_network_source_does_not_invent_one() {
        let json =
            r#"{"groupAddress":"232.1.1.60","port":8000,"tracks":[{"name":"video","packetId":1}]}"#;
        let endpoint: MulticastEndpoint = serde_json::from_str(json).unwrap();
        assert!(endpoint.network_source.is_none());
        let emitted = serde_json::to_value(&endpoint).unwrap();
        assert!(
            emitted.as_object().unwrap().get("networkSource").is_none(),
            "absent networkSource must stay absent: {emitted}"
        );
    }

    /// BLO-41985 gap (b2): §7.2 nests the auth block *inside* `multicast`
    /// (`{"multicast":{"auth":{...},"endpoints":[...]}}`). That is a different
    /// JSON shape from the literal dotted root key `"multicast.auth"` that
    /// `Root::multicast_auth` carries for libmmt (BLO-39866, #103), so adding
    /// that one did not make this one parse — `deny_unknown_fields` still
    /// rejected it.
    #[test]
    fn nested_multicast_auth_parses_and_round_trips() {
        let json = r#"{"endpoints":[{"groupAddress":"232.1.1.60","port":8000,"tracks":[{"name":"video","packetId":1}]}],"auth":{"scheme":"bc-provenance"}}"#;
        let cfg: MulticastConfig =
            serde_json::from_str(json).expect("nested multicast.auth must parse");
        assert_eq!(
            cfg.auth,
            Some(serde_json::json!({"scheme": "bc-provenance"})),
            "auth block must survive parse intact"
        );
        assert_eq!(
            serde_json::to_value(&cfg).unwrap(),
            serde_json::from_str::<serde_json::Value>(json).unwrap(),
            "auth round-trip must be lossless"
        );
    }

    /// A config that never carried `auth` must not gain one.
    #[test]
    fn multicast_config_without_auth_does_not_invent_one() {
        let json = r#"{"endpoints":[{"groupAddress":"232.1.1.60","port":8000,"tracks":[{"name":"video","packetId":1}]}]}"#;
        let cfg: MulticastConfig = serde_json::from_str(json).unwrap();
        assert!(cfg.auth.is_none());
        let emitted = serde_json::to_value(&cfg).unwrap();
        assert!(
            emitted.as_object().unwrap().get("auth").is_none(),
            "absent auth must stay absent: {emitted}"
        );
    }

    /// The bare-object `networkSource` form that drafts before the 2026-10-05
    /// sync allowed. Both authorities now reject it: libmmt pins an array and
    /// the current draft types it "array of objects" (§4.1, §4.2). The golden
    /// negative fixture `network-source-object` mirrors libmmt's rejection
    /// byte-for-byte; this pins the parse side so a future widening has to be
    /// an explicit cross-repo decision rather than an accident. See BLO-41985.
    #[test]
    fn object_form_network_source_is_deliberately_rejected() {
        let json = r#"{"endpoints":[],"networkSource":{"type":"amt","relay":"69.25.95.128"}}"#;
        assert!(
            serde_json::from_str::<MulticastConfig>(json).is_err(),
            "object-form networkSource stays rejected: both libmmt and the \
             current draft type it as an array"
        );
    }
}
