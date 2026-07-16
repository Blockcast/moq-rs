// SPDX-FileCopyrightText: 2026 Blockcast Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::collections::BTreeMap;

use bytes::{Buf, Bytes};
use moq_transport::coding::{
    Decode, Encode, KeyValuePairs, Location, ReasonPhrase, SessionUri, TrackNamespace,
    TrackNamespacePrefix,
};
use moq_transport::data::{
    ExtensionHeaders, ObjectStatus, StreamHeader, StreamHeaderType, SubgroupHeader, SubgroupObject,
    SubgroupObjectExt,
};
use moq_transport::message::*;
use moq_transport::setup::{Client, Server};
use moq_transport::MOQ_WIRE_FIXTURE_VERSION;
use serde::Deserialize;

#[derive(Deserialize)]
struct Manifest {
    version: String,
    vectors: BTreeMap<String, String>,
}

fn namespace() -> TrackNamespace {
    TrackNamespace::from_utf8_path("live/video")
}

fn prefix() -> TrackNamespacePrefix {
    TrackNamespacePrefix::from_utf8_path("live")
}

fn encode<T: Encode>(value: &T) -> Vec<u8> {
    let mut wire = Vec::new();
    value.encode(&mut wire).unwrap();
    wire
}

fn decode_hex(hex: &str) -> Vec<u8> {
    assert!(hex.len().is_multiple_of(2), "fixture has odd-length hex");
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(pair, 16).unwrap()
        })
        .collect()
}

fn assert_fixture<T>(manifest: &Manifest, name: &str, value: T)
where
    T: Decode + Encode,
{
    let expected = decode_hex(
        manifest
            .vectors
            .get(name)
            .unwrap_or_else(|| panic!("missing wire fixture {name}")),
    );
    assert_eq!(encode(&value), expected, "encoded bytes changed for {name}");

    let mut wire = Bytes::copy_from_slice(&expected);
    let decoded = T::decode(&mut wire).unwrap();
    assert!(!wire.has_remaining(), "decoder left bytes for {name}");
    assert_eq!(encode(&decoded), expected, "round trip changed {name}");
}

fn control_messages() -> Vec<(&'static str, Message)> {
    let empty = KeyValuePairs::default;
    let mut unknown_parameter = KeyValuePairs::new();
    unknown_parameter.set_bytesvalue(0x3f, vec![0xde, 0xad]);

    let mut unknown_track_extension = TrackExtensions::new();
    unknown_track_extension.set_bytes_extension(0x3f, vec![0xca, 0xfe]);

    vec![
        (
            "REQUEST_UPDATE",
            RequestUpdate {
                id: 2,
                existing_request_id: 0,
                params: empty(),
            }
            .into(),
        ),
        (
            "REQUEST_ERROR",
            RequestError {
                id: 2,
                error_code: RequestErrorCode::DoesNotExist.into(),
                retry_interval: 0,
                reason: ReasonPhrase("missing".into()),
            }
            .into(),
        ),
        (
            "REQUEST_OK",
            RequestOk {
                id: 2,
                params: empty(),
            }
            .into(),
        ),
        (
            "SUBSCRIBE",
            Subscribe {
                id: 4,
                track_namespace: namespace(),
                track_name: "main".into(),
                params: unknown_parameter,
            }
            .into(),
        ),
        (
            "SUBSCRIBE_OK_UNKNOWN_EXTENSION",
            SubscribeOk {
                id: 4,
                track_alias: 6,
                params: empty(),
                track_extensions: unknown_track_extension,
            }
            .into(),
        ),
        ("UNSUBSCRIBE", Unsubscribe { id: 4 }.into()),
        (
            "PUBLISH_NAMESPACE",
            PublishNamespace {
                id: 6,
                track_namespace: namespace(),
                params: empty(),
            }
            .into(),
        ),
        (
            "NAMESPACE",
            Namespace {
                track_namespace_suffix: prefix(),
            }
            .into(),
        ),
        (
            "PUBLISH_NAMESPACE_DONE",
            PublishNamespaceDone { id: 6 }.into(),
        ),
        (
            "NAMESPACE_DONE",
            NamespaceDone {
                track_namespace_suffix: prefix(),
            }
            .into(),
        ),
        (
            "PUBLISH_NAMESPACE_CANCEL",
            PublishNamespaceCancel {
                id: 6,
                error_code: 1,
                reason_phrase: ReasonPhrase("expired".into()),
            }
            .into(),
        ),
        (
            "TRACK_STATUS",
            TrackStatus {
                id: 8,
                track_namespace: namespace(),
                track_name: "main".into(),
                params: empty(),
            }
            .into(),
        ),
        (
            "PUBLISH",
            Publish {
                id: 10,
                track_namespace: namespace(),
                track_name: "main".into(),
                track_alias: 12,
                params: empty(),
                track_extensions: TrackExtensions::default(),
            }
            .into(),
        ),
        (
            "PUBLISH_DONE",
            PublishDone {
                id: 10,
                status_code: PublishDoneCode::TrackEnded.into(),
                stream_count: 1,
                reason: ReasonPhrase("ended".into()),
            }
            .into(),
        ),
        (
            "PUBLISH_OK",
            PublishOk {
                id: 10,
                params: empty(),
            }
            .into(),
        ),
        (
            "FETCH",
            Fetch {
                id: 12,
                fetch_type: FetchType::Standalone,
                standalone_fetch: Some(StandaloneFetch {
                    track_namespace: namespace(),
                    track_name: "main".into(),
                    start_location: Location::new(1, 2),
                    end_location: Location::new(3, 4),
                }),
                joining_fetch: None,
                params: empty(),
            }
            .into(),
        ),
        ("FETCH_CANCEL", FetchCancel { id: 12 }.into()),
        (
            "FETCH_OK",
            FetchOk {
                id: 12,
                end_of_track: true,
                end_location: Location::new(3, 4),
                params: empty(),
                track_extensions: TrackExtensions::default(),
            }
            .into(),
        ),
        (
            "SUBSCRIBE_NAMESPACE",
            SubscribeNamespace {
                id: 14,
                track_namespace_prefix: prefix(),
                subscribe_options: SubscribeOptions::Both,
                params: empty(),
            }
            .into(),
        ),
        (
            "GO_AWAY",
            GoAway {
                uri: SessionUri("moq://next.example".into()),
            }
            .into(),
        ),
        ("MAX_REQUEST_ID", MaxRequestId { request_id: 64 }.into()),
        (
            "REQUESTS_BLOCKED",
            RequestsBlocked { max_request_id: 64 }.into(),
        ),
        (
            "UNKNOWN_CONTROL",
            Message::Unknown(Unknown {
                message_type: 0x3e,
                payload: Bytes::from_static(&[0xaa, 0xbb, 0xcc]),
            }),
        ),
    ]
}

#[test]
fn moqt_16_wire_vectors_are_byte_exact_and_round_trip() {
    let manifest: Manifest =
        serde_json::from_str(include_str!("fixtures/moqt-16-v1.json")).unwrap();
    assert_eq!(manifest.version, MOQ_WIRE_FIXTURE_VERSION);

    let messages = control_messages();
    for (name, message) in messages {
        assert_fixture(&manifest, name, message);
    }

    let mut client_params = KeyValuePairs::new();
    client_params.set_bytesvalue(1, b"/demo".to_vec());
    assert_fixture(
        &manifest,
        "CLIENT_SETUP",
        Client {
            params: client_params,
        },
    );

    let mut server_params = KeyValuePairs::new();
    server_params.set_intvalue(2, 64);
    assert_fixture(
        &manifest,
        "SERVER_SETUP",
        Server {
            params: server_params,
        },
    );

    assert_fixture(
        &manifest,
        "SUBGROUP_HEADER",
        StreamHeader {
            header_type: StreamHeaderType::SubgroupIdExt,
            subgroup_header: Some(SubgroupHeader {
                header_type: StreamHeaderType::SubgroupIdExt,
                track_alias: 6,
                group_id: 7,
                subgroup_id: Some(8),
                publisher_priority: 9,
            }),
            fetch_header: None,
        },
    );
    assert_fixture(
        &manifest,
        "SUBGROUP_OBJECT",
        SubgroupObject {
            object_id_delta: 1,
            payload_length: 3,
            status: None,
        },
    );

    let mut extensions = ExtensionHeaders::new();
    extensions.set_bytesvalue(0x3f, vec![0x11, 0x22]);
    assert_fixture(
        &manifest,
        "SUBGROUP_OBJECT_UNKNOWN_EXTENSION",
        SubgroupObjectExt {
            object_id_delta: 2,
            extension_headers: extensions,
            payload_length: 0,
            status: Some(ObjectStatus::NormalObject),
        },
    );

    assert_eq!(manifest.vectors.len(), 28);
}
