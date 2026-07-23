// SPDX-FileCopyrightText: 2025-2026 Blockcast Inc. and contributors
// SPDX-License-Identifier: Apache-2.0

use bytes::Bytes;
use mmtp_moq_carriage::{AlFecMetadata, CarriageObject, CarriageWriter, WIRE_CONTRACT_VERSION};
use moq_transport::{
    coding::Value,
    data::ExtensionHeaders,
    serve::{Subgroups, Track},
};
use std::sync::Arc;

const TEST_FEC_EXTENSION_ID: u64 = 0x3d;

fn object(group_id: u64, subgroup_id: u64, payload: &'static [u8]) -> CarriageObject {
    CarriageObject {
        group_id,
        subgroup_id,
        priority: 128,
        payload: Bytes::from_static(payload),
        extension_headers: ExtensionHeaders::new(),
    }
}

#[test]
fn consumes_frozen_moqt_16_contract() {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    const MOQ_T16_V1_FNV1A: u64 = 0x292174d7067d066d;

    let fixture = include_bytes!("fixtures/moqt-16-v1.json");
    let parsed: serde_json::Value = serde_json::from_slice(fixture).unwrap();
    let digest = fixture.iter().fold(FNV_OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    });

    assert_eq!(parsed["version"], WIRE_CONTRACT_VERSION);
    assert_eq!(digest, MOQ_T16_V1_FNV1A, "moqt-16-v1 fixture changed");
}

#[test]
fn fec_metadata_round_trips_for_source_and_repair() {
    for repair in [false, true] {
        let metadata = AlFecMetadata {
            scheme: 6,
            source_block_number: 0x1020_3040,
            encoding_symbol_id: if repair { 48 } else { 7 },
            repair,
        };
        let mut carried = object(9, u64::from(repair), b"opaque MMTP packet");
        carried
            .set_fec_metadata(TEST_FEC_EXTENSION_ID, metadata)
            .unwrap();
        assert_eq!(
            carried.fec_metadata(TEST_FEC_EXTENSION_ID).unwrap(),
            Some(metadata)
        );
    }
}

#[tokio::test]
async fn publisher_and_relay_model_preserve_payload_fec_and_unknown_extensions() {
    let track = Arc::new(Track {
        namespace: "live".try_into().unwrap(),
        name: "mmtp".into(),
    });
    let (writer, mut reader) = Subgroups { track }.produce();
    let mut publisher = CarriageWriter::new(writer);

    let metadata = AlFecMetadata {
        scheme: 6,
        source_block_number: 12,
        encoding_symbol_id: 3,
        repair: false,
    };
    let mut source = object(4, 0, b"DSR/libmmt MMTP source vector");
    source
        .extension_headers
        .set_bytesvalue(0x3f, vec![0xca, 0xfe]);
    source
        .set_fec_metadata(TEST_FEC_EXTENSION_ID, metadata)
        .unwrap();
    publisher.publish(&source).unwrap();

    let mut subgroup = reader.next().await.unwrap().unwrap();
    let mut relayed = subgroup.next().await.unwrap().unwrap();
    assert_eq!(relayed.read_all().await.unwrap(), source.payload);
    assert_eq!(
        relayed.extension_headers.get(0x3f).unwrap().value,
        Value::BytesValue(vec![0xca, 0xfe])
    );
    let fec = match &relayed
        .extension_headers
        .get(TEST_FEC_EXTENSION_ID)
        .unwrap()
        .value
    {
        Value::BytesValue(value) => AlFecMetadata::decode(value).unwrap(),
        Value::IntValue(_) => panic!("FEC extension changed type"),
    };
    assert_eq!(fec, metadata);
}

#[test]
fn rejects_subgroup_regression_before_transport_silently_drops_it() {
    let track = Arc::new(Track {
        namespace: "live".try_into().unwrap(),
        name: "mmtp".into(),
    });
    let (writer, _reader) = Subgroups { track }.produce();
    let mut publisher = CarriageWriter::new(writer);

    publisher.publish(&object(5, 1, b"new")).unwrap();
    let error = publisher.publish(&object(5, 0, b"stale")).unwrap_err();
    assert!(error.to_string().contains("regressed"));
}
