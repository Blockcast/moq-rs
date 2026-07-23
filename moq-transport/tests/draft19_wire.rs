// SPDX-FileCopyrightText: 2026 Blockcast Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::time::{Duration, Instant};

use bytes::{Buf, Bytes, BytesMut};
use moq_transport::coding::{Decode, Encode, SessionUri, Vi64};
use moq_transport::profile::draft19::{
    ControlStreamPair, Frame, GoAway, GoAwayState, RequestKind, RequestStream, RequestStreamRole,
    SessionErrorCode, Setup, SetupOption, SetupOptions, StreamAction, StreamErrorCode,
    StreamProtocolError, GOAWAY_TYPE,
};
use moq_transport::profile::WireProfile;
use moq_transport::session::Draft19SessionRole;

fn encode<T: Encode>(value: &T) -> Vec<u8> {
    let mut wire = Vec::new();
    value.encode(&mut wire).unwrap();
    wire
}

#[test]
fn draft19_vi64_matches_official_vectors_and_accepts_non_minimal_values() {
    let vectors = [
        (25, vec![0x19]),
        (37, vec![0x25]),
        (15_293, vec![0xbb, 0xbd]),
        (226_442_877, vec![0xed, 0x7f, 0x3e, 0x7d]),
        (2_893_212_287_960, vec![0xfa, 0xa1, 0xa0, 0xe4, 0x03, 0xd8]),
        (
            151_288_809_941_952,
            vec![0xfc, 0x89, 0x98, 0xab, 0xc6, 0x6b, 0xc0],
        ),
        (
            70_423_237_261_249_041,
            vec![0xfe, 0xfa, 0x31, 0x8f, 0xa8, 0xe3, 0xca, 0x11],
        ),
        (u64::MAX, vec![0xff; 9]),
    ];

    for (value, expected) in vectors {
        assert_eq!(encode(&Vi64::new(value)), expected);
        let mut wire = Bytes::from(expected);
        assert_eq!(Vi64::decode(&mut wire).unwrap().into_inner(), value);
        assert!(!wire.has_remaining());
    }

    let mut non_minimal = Bytes::from_static(&[0x80, 0x25]);
    assert_eq!(Vi64::decode(&mut non_minimal).unwrap().into_inner(), 37);
}

#[test]
fn draft19_vi64_boundary_lengths_cover_the_full_u64_domain() {
    let vectors = [
        (0, vec![0x00]),
        (127, vec![0x7f]),
        (128, vec![0x80, 0x80]),
        (16_383, vec![0xbf, 0xff]),
        (16_384, vec![0xc0, 0x40, 0x00]),
        (u64::MAX, vec![0xff; 9]),
    ];
    for (value, expected) in vectors {
        assert_eq!(encode(&Vi64::new(value)), expected);
    }
}

#[test]
fn draft19_setup_is_unified_length_bounded_and_has_no_option_count() {
    let setup = Setup {
        options: SetupOptions(vec![SetupOption::bytes(0x01, b"/")]),
    };
    let expected = vec![0xaf, 0x00, 0x00, 0x03, 0x01, 0x01, b'/'];
    assert_eq!(encode(&setup), expected);

    let mut wire = Bytes::from(expected);
    assert_eq!(Setup::decode(&mut wire).unwrap(), setup);
    assert!(!wire.has_remaining());
}

#[test]
fn draft19_setup_accepts_non_minimal_type_and_unknown_duplicate_options() {
    let mut wire = Bytes::from_static(&[
        0xc0, 0x2f, 0x00, 0x00, 0x06, // non-minimal SETUP type and body length
        0x09, 0x01, b'a', // unknown odd option 9
        0x00, 0x01, b'b', // duplicate unknown option 9
    ]);
    let setup = Setup::decode(&mut wire).unwrap();
    assert_eq!(setup.options.0.len(), 2);
    assert!(!wire.has_remaining());

    let mut unregistered_two = Bytes::from_static(&[
        0xaf, 0x00, 0x00, 0x04, // SETUP and body length
        0x02, 0x00, // unknown even option 2
        0x00, 0x01, // duplicate unknown option 2
    ]);
    assert_eq!(
        Setup::decode(&mut unregistered_two)
            .unwrap()
            .options
            .0
            .len(),
        2
    );
}

#[test]
fn draft19_decoder_rejects_moqt16_setup_without_consuming_fixture_profile() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/moqt-16-v1.json")).unwrap();
    assert_eq!(fixture["version"], "moqt-16-v1");
    let hex = fixture["vectors"]["CLIENT_SETUP"].as_str().unwrap();
    let bytes = hex
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect::<Vec<_>>();
    let mut wire = Bytes::from(bytes);
    let remaining = wire.len();
    assert!(matches!(
        Setup::decode_for_profile(WireProfile::Draft16, &mut wire),
        Err(moq_transport::coding::DecodeError::InvalidMessage(0x2f00))
    ));
    assert_eq!(
        wire.len(),
        remaining,
        "profile rejection consumed fixture bytes"
    );
}

#[test]
fn draft19_frame_uses_vi64_type_and_exact_u16_payload_length() {
    let frame = Frame {
        message_type: 0x2f00,
        payload: Bytes::from_static(b"ok"),
    };
    assert_eq!(encode(&frame), vec![0xaf, 0x00, 0x00, 0x02, b'o', b'k']);

    let mut truncated = Bytes::from_static(&[0x03, 0x00, 0x02, 0xff]);
    assert!(Frame::decode(&mut truncated).is_err());
}

#[test]
fn draft19_goaway_is_byte_exact_and_round_trips_uri_then_timeout() {
    let goaway = GoAway {
        new_session_uri: SessionUri("moqt://next.example".into()),
        timeout_ms: 250,
    };
    let frame = goaway.clone().into_frame().unwrap();
    assert_eq!(frame.message_type, GOAWAY_TYPE);
    assert_eq!(
        encode(&frame),
        [
            vec![0x10, 0x00, 0x16, 0x13],
            b"moqt://next.example".to_vec(),
            vec![0x80, 0xfa],
        ]
        .concat()
    );
    assert_eq!(GoAway::from_frame(&frame).unwrap(), goaway);

    let no_redirect = GoAway {
        new_session_uri: SessionUri(String::new()),
        timeout_ms: 0,
    };
    assert_eq!(
        encode(&no_redirect.into_frame().unwrap()),
        vec![0x10, 0, 2, 0, 0]
    );
}

#[test]
fn draft19_goaway_enforces_the_8192_byte_uri_cap_on_both_paths() {
    let maximum = GoAway {
        new_session_uri: SessionUri("x".repeat(8_192)),
        timeout_ms: 1,
    };
    assert_eq!(
        GoAway::from_frame(&maximum.clone().into_frame().unwrap()).unwrap(),
        maximum
    );

    let oversized = GoAway {
        new_session_uri: SessionUri("x".repeat(8_193)),
        timeout_ms: 1,
    };
    assert!(oversized.into_frame().is_err());

    let mut payload = Vec::new();
    Vi64::new(8_193).encode(&mut payload).unwrap();
    let oversized_frame = Frame {
        message_type: GOAWAY_TYPE,
        payload: payload.into(),
    };
    assert!(GoAway::from_frame(&oversized_frame).is_err());
}

#[test]
fn draft19_goaway_new_session_uri_is_role_aware_at_the_session_boundary() {
    let empty = GoAway {
        new_session_uri: SessionUri(String::new()),
        timeout_ms: 0,
    };
    Draft19SessionRole::Server
        .validate_received_goaway(&empty)
        .unwrap();
    Draft19SessionRole::Client
        .validate_received_goaway(&empty)
        .unwrap();

    let maximum = GoAway::from_frame(
        &GoAway {
            new_session_uri: SessionUri("x".repeat(8_192)),
            timeout_ms: 1,
        }
        .into_frame()
        .unwrap(),
    )
    .unwrap();
    Draft19SessionRole::Client
        .validate_received_goaway(&maximum)
        .unwrap();

    let error = Draft19SessionRole::Server
        .validate_received_goaway(&maximum)
        .unwrap_err();
    assert_eq!(error, StreamProtocolError::ServerGoAwayWithNewSessionUri);
    assert_eq!(
        error.session_code(),
        Some(SessionErrorCode::ProtocolViolation)
    );
}

#[test]
fn draft19_goaway_duplicate_tracking_is_scoped_per_stream() {
    let mut control = GoAwayState::default();
    let mut request_a = GoAwayState::default();
    let mut request_b = GoAwayState::default();

    control.record_received().unwrap();
    request_a.record_received().unwrap();
    request_b.record_received().unwrap();
    assert_eq!(
        request_a.record_received().unwrap_err(),
        StreamProtocolError::DuplicateGoAway
    );
    assert!(control.received());
    assert!(request_b.received());
}

#[test]
fn draft19_goaway_record_sent_rejects_a_second_send_on_one_stream() {
    let now = Instant::now();
    let goaway = GoAway {
        new_session_uri: SessionUri(String::new()),
        timeout_ms: 1,
    };
    let mut state = GoAwayState::default();
    state.record_sent(&goaway, now).unwrap();
    assert_eq!(
        state.record_sent(&goaway, now).unwrap_err(),
        StreamProtocolError::DuplicateGoAway
    );
}

#[test]
fn draft19_goaway_sent_and_received_are_tracked_independently() {
    let now = Instant::now();
    let goaway = GoAway {
        new_session_uri: SessionUri(String::new()),
        timeout_ms: 1,
    };

    // Recording a sent GOAWAY must not mark the stream as having received one.
    let mut sender = GoAwayState::default();
    sender.record_sent(&goaway, now).unwrap();
    assert!(sender.sent());
    assert!(!sender.received());

    // ...and vice versa: a received GOAWAY leaves the sent flag clear.
    let mut receiver = GoAwayState::default();
    receiver.record_received().unwrap();
    assert!(receiver.received());
    assert!(!receiver.sent());
}

#[test]
fn draft19_goaway_decode_rejects_a_within_cap_length_overrunning_the_payload() {
    // A URI length that is under the 8,192 cap but larger than the bytes that
    // actually follow must be a clean decode error, never a panic on the
    // fixed-size copy that materializes the URI.
    let mut payload = Vec::new();
    Vi64::new(64).encode(&mut payload).unwrap();
    payload.extend_from_slice(b"short");
    let truncated = Frame {
        message_type: GOAWAY_TYPE,
        payload: payload.into(),
    };
    assert!(GoAway::from_frame(&truncated).is_err());
}

#[test]
fn draft19_goaway_timeout_zero_never_expires_and_nonzero_is_an_upper_hint() {
    let now = Instant::now();
    let mut no_deadline = GoAwayState::default();
    no_deadline
        .record_sent(
            &GoAway {
                new_session_uri: SessionUri(String::new()),
                timeout_ms: 0,
            },
            now,
        )
        .unwrap();
    assert!(!no_deadline.timeout_expired(now + Duration::from_secs(86_400)));

    let mut bounded = GoAwayState::default();
    bounded
        .record_sent(
            &GoAway {
                new_session_uri: SessionUri(String::new()),
                timeout_ms: 50,
            },
            now,
        )
        .unwrap();
    assert!(!bounded.timeout_expired(now + Duration::from_millis(49)));
    assert!(bounded.timeout_expired(now + Duration::from_millis(50)));
}

#[test]
fn draft19_request_goaway_allows_early_fin_and_uses_going_away_reset() {
    let mut early = RequestStream::new(WireProfile::Draft19, 0x1d).unwrap();
    early.finish_sending_for_goaway().unwrap();

    let mut expired = RequestStream::new(WireProfile::Draft19, 0x03).unwrap();
    assert_eq!(
        expired.reset_sending(StreamErrorCode::GoingAway).unwrap(),
        StreamAction::Reset(StreamErrorCode::GoingAway)
    );
    assert_eq!(StreamErrorCode::GoingAway as u64, 0x04);
    assert_eq!(SessionErrorCode::GoAwayTimeout as u64, 0x10);
}

#[test]
fn draft19_control_streams_are_paired_unidirectional_and_never_finish() {
    let mut pair = ControlStreamPair::new(WireProfile::Draft19).unwrap();
    assert!(!pair.is_ready());
    pair.sent_setup().unwrap();
    pair.received_setup().unwrap();
    assert!(pair.is_ready());

    let error = pair.on_fin_or_reset().unwrap_err();
    assert_eq!(
        error.session_code(),
        Some(SessionErrorCode::ProtocolViolation)
    );
    assert!(ControlStreamPair::new(WireProfile::Draft16).is_err());
}

#[test]
fn draft19_request_stream_rejects_invalid_first_messages_and_premature_fin() {
    let invalid = RequestStream::new(WireProfile::Draft19, 0x07).unwrap_err();
    assert_eq!(
        invalid.session_code(),
        Some(SessionErrorCode::ProtocolViolation)
    );

    let mut request = RequestStream::new(WireProfile::Draft19, 0x03).unwrap();
    assert_eq!(request.kind(), RequestKind::Subscribe);
    let premature = request.receive_fin().unwrap_err();
    assert_eq!(premature.stream_code(), Some(StreamErrorCode::Cancelled));

    request.receive_first_response(0x04).unwrap();
    let missing_done = request.receive_fin().unwrap_err();
    assert_eq!(missing_done.stream_code(), Some(StreamErrorCode::Cancelled));
    request.receive_publish_done().unwrap();
    request.receive_fin().unwrap();

    let mut publish = RequestStream::new(WireProfile::Draft19, 0x1d).unwrap();
    assert_eq!(
        publish.finish_sending().unwrap_err().stream_code(),
        Some(StreamErrorCode::Cancelled)
    );
    assert!(publish.send_publish_done().is_err());
    publish.receive_first_response(0x07).unwrap();
    publish.send_publish_done().unwrap();
    publish.finish_sending().unwrap();

    let mut responder =
        RequestStream::new_for_role(WireProfile::Draft19, 0x03, RequestStreamRole::Responder)
            .unwrap();
    assert!(responder.send_publish_done().is_err());
    assert!(!responder.can_send_followup());
    responder.send_first_response(0x04).unwrap();
    assert!(responder.can_send_followup());
    responder.send_publish_done().unwrap();
    responder.finish_sending().unwrap();
}

#[test]
fn draft19_request_reset_and_stop_sending_transitions_use_registry_codes() {
    let mut request = RequestStream::new(WireProfile::Draft19, 0x16).unwrap();
    assert_eq!(
        request
            .receive_stop_sending(StreamErrorCode::DeliveryTimeout)
            .unwrap(),
        StreamAction::Reset(StreamErrorCode::DeliveryTimeout)
    );
    let repeated = request
        .receive_stop_sending(StreamErrorCode::DeliveryTimeout)
        .unwrap_err();
    assert_eq!(repeated.stream_code(), Some(StreamErrorCode::InternalError));

    let mut cancelled = RequestStream::new(WireProfile::Draft19, 0x0d).unwrap();
    assert_eq!(
        cancelled.cancel(StreamErrorCode::Cancelled).unwrap(),
        StreamAction::ResetAndStop(StreamErrorCode::Cancelled)
    );
    assert!(matches!(
        cancelled.cancel(StreamErrorCode::Cancelled),
        Err(StreamProtocolError::InvalidTransition)
    ));
}

#[test]
fn draft19_validates_the_first_response_for_each_request_family() {
    for (request_type, response_type) in [
        (0x03, 0x04),
        (0x16, 0x18),
        (0x1d, 0x07),
        (0x0d, 0x07),
        (0x06, 0x07),
        (0x50, 0x07),
        (0x51, 0x07),
    ] {
        let mut request = RequestStream::new(WireProfile::Draft19, request_type).unwrap();
        request.receive_first_response(response_type).unwrap();
    }

    let mut request = RequestStream::new(WireProfile::Draft19, 0x03).unwrap();
    assert!(matches!(
        request.receive_first_response(0x07),
        Err(StreamProtocolError::InvalidFirstResponse { .. })
    ));
}

#[test]
fn draft19_setup_round_trip_preserves_pinned_legacy_fixture_digest() {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    const MOQ_T16_V1_FNV1A: u64 = 0x292174d7067d066d;

    let before = include_bytes!("fixtures/moqt-16-v1.json");
    let digest = before.iter().fold(FNV_OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    });
    assert_eq!(digest, MOQ_T16_V1_FNV1A, "moqt-16-v1 fixture changed");

    let setup = Setup {
        options: SetupOptions(vec![
            SetupOption::bytes(0x01, b"/demo"),
            SetupOption::integer(0x04, 16),
        ]),
    };
    let mut wire = BytesMut::from(encode(&setup).as_slice());
    let decoded = Setup::decode(&mut wire).unwrap();
    assert_eq!(decoded, setup);
}

#[test]
fn draft19_error_registries_use_draft19_values() {
    assert_eq!(SessionErrorCode::InternalError as u64, 0x01);
    assert_eq!(SessionErrorCode::ProtocolViolation as u64, 0x03);
    assert_eq!(SessionErrorCode::KeyValueFormattingError as u64, 0x06);
    assert_eq!(StreamErrorCode::UnknownObjectStatus as u64, 0x06);
    assert_eq!(StreamErrorCode::MalformedTrack as u64, 0x12);
}
