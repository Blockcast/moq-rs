// SPDX-FileCopyrightText: 2026 Blockcast Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Draft-19 graceful drain, driven over a real QUIC connection.
//!
//! These exercise the relay's own serve path ([`serve_draft19_control_plane`])
//! rather than an in-process session pair, so the GOAWAY a client observes here
//! is the one the relay binary emits.

use std::time::{Duration, Instant};

use moq_native_ietf::quic::{Config, Endpoint};
use moq_native_ietf::tls;
use moq_relay_ietf::{serve_draft19_control_plane, Draft19Drain};
use moq_transport::profile::draft19::{GoAway, Setup, GOAWAY_TYPE};
use moq_transport::profile::WireProfile;
use moq_transport::session::{Draft19Session, Draft19SessionRole};
use tokio_util::sync::CancellationToken;
use url::Url;

/// The relay advertises this as the GOAWAY New Session URI.
const NEW_SESSION_URI: &str = "https://relay-b.example.net/moq/";
/// Short enough to keep the timeout case quick, long enough that the redirect
/// case is not racing it.
const TIMEOUT_MS: u64 = 400;
/// `SessionErrorCode::GoAwayTimeout`, as the client sees it on the close.
const GOAWAY_TIMEOUT_CODE: u32 = 0x10;

/// Self-signed TLS for localhost, written to disk so `tls::Args` can load it.
///
/// Going through `tls::Args` rather than building a `rustls::ServerConfig` by
/// hand keeps `rcgen` the only dev-dependency this test needs.
fn tls_config(tag: &str) -> tls::Config {
    let dir =
        std::env::temp_dir().join(format!("moq-draft19-drain-{}-{}", std::process::id(), tag));
    std::fs::create_dir_all(&dir).expect("temp dir is creatable");
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");

    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("self-signed cert generates");
    std::fs::write(&cert_path, certified.cert.pem()).expect("cert is writable");
    std::fs::write(&key_path, certified.key_pair.serialize_pem()).expect("key is writable");

    tls::Args {
        cert: vec![cert_path],
        key: vec![key_path],
        disable_verify: true,
        ..Default::default()
    }
    .load()
    .expect("TLS config loads")
}

fn endpoint(tag: &str) -> Endpoint {
    Endpoint::new(
        Config::new("127.0.0.1:0".parse().unwrap(), None, tls_config(tag))
            .unwrap()
            .with_wire_profiles([WireProfile::Draft19]),
    )
    .expect("endpoint binds")
}

/// Stand the relay's draft-19 serve path up on a real QUIC listener and connect
/// a draft-19 client to it.
///
/// The bare `web_transport::Session` is handed back alongside the draft-19
/// session because the session-level close code is only observable there. A
/// control-stream read surfaces the relay's close as `ControlStreamClosed`,
/// which is identical whether the relay closed deliberately or just dropped the
/// connection.
async fn connect(
    tag: &str,
    drain: Draft19Drain,
) -> (
    Draft19Session,
    web_transport::Session,
    tokio::task::JoinHandle<()>,
) {
    let server_endpoint = endpoint(tag);
    let mut server = server_endpoint.server.expect("server side is configured");
    let addr = server.local_addr().expect("listener has an address");

    let serve = tokio::spawn(async move {
        let (conn, _info) = server.accept().await.expect("relay accepts a connection");
        serve_draft19_control_plane(conn, Some(drain)).await;
    });

    let url = Url::parse(&format!("https://localhost:{}/", addr.port())).unwrap();
    let (conn, _cid, _transport, selected) = endpoint(&format!("{tag}-client"))
        .client
        .connect_with_profile(&url, Some(addr), WireProfile::Draft19)
        .await
        .expect("client connects");
    assert_eq!(selected, WireProfile::Draft19, "negotiated draft-19");

    let raw = conn.clone();
    let session = Draft19Session::establish(
        conn,
        Draft19SessionRole::Client,
        WireProfile::Draft19,
        Setup::default(),
    )
    .await
    .expect("client establishes a draft-19 session");

    (session, raw, serve)
}

/// Read control frames until a GOAWAY arrives, and decode it.
async fn await_goaway(session: &mut Draft19Session) -> GoAway {
    loop {
        let frame = session
            .receive_control()
            .await
            .expect("control stream stays open until GOAWAY");
        if frame.message_type == GOAWAY_TYPE {
            return GoAway::from_frame(&frame).expect("GOAWAY decodes");
        }
    }
}

/// AC1 + AC3: draining the relay sends a GOAWAY on the control stream, and the
/// client decodes exactly the URI and Timeout the relay was configured with.
#[tokio::test]
async fn drain_sends_goaway_with_configured_uri_and_timeout() {
    let signal = CancellationToken::new();
    let (mut session, _raw, serve) = connect(
        "redirect",
        Draft19Drain {
            new_session_uri: Url::parse(NEW_SESSION_URI).unwrap(),
            timeout_ms: TIMEOUT_MS,
            signal: signal.clone(),
        },
    )
    .await;

    signal.cancel();

    let goaway = tokio::time::timeout(Duration::from_secs(10), await_goaway(&mut session))
        .await
        .expect("GOAWAY arrives promptly after the drain starts");

    assert_eq!(
        goaway.new_session_uri.0, NEW_SESSION_URI,
        "client observes the configured New Session URI"
    );
    assert_eq!(
        goaway.timeout_ms, TIMEOUT_MS,
        "client observes the configured millisecond Timeout"
    );

    // Closing promptly is the cooperative path, so the relay must not reach for
    // GOAWAY_TIMEOUT.
    session
        .close_after_goaway()
        .expect("client closes after GOAWAY");
    tokio::time::timeout(Duration::from_secs(10), serve)
        .await
        .expect("relay finishes the session once the peer closes")
        .expect("serve task does not panic");
}

/// AC1, second half: a draft-19 session established after the drain has started
/// is redirected rather than served. The GOAWAY arrives without the client
/// sending anything.
#[tokio::test]
async fn session_opened_during_drain_is_redirected_immediately() {
    let signal = CancellationToken::new();
    // Already draining before the client ever connects.
    signal.cancel();

    let (mut session, _raw, serve) = connect(
        "late-arrival",
        Draft19Drain {
            new_session_uri: Url::parse(NEW_SESSION_URI).unwrap(),
            timeout_ms: TIMEOUT_MS,
            signal,
        },
    )
    .await;

    let goaway = tokio::time::timeout(Duration::from_secs(10), await_goaway(&mut session))
        .await
        .expect("a session opened mid-drain is sent GOAWAY without asking");
    assert_eq!(goaway.new_session_uri.0, NEW_SESSION_URI);
    assert_eq!(goaway.timeout_ms, TIMEOUT_MS);

    session
        .close_after_goaway()
        .expect("client closes after GOAWAY");
    let _ = tokio::time::timeout(Duration::from_secs(10), serve).await;
}

/// AC2: a client that receives the GOAWAY and then does nothing is closed by
/// the relay with `GOAWAY_TIMEOUT` (0x10) once the advertised Timeout expires.
#[tokio::test]
async fn goaway_timeout_closes_the_session_with_no_open_requests() {
    let signal = CancellationToken::new();
    let (mut session, raw, serve) = connect(
        "timeout",
        Draft19Drain {
            new_session_uri: Url::parse(NEW_SESSION_URI).unwrap(),
            timeout_ms: TIMEOUT_MS,
            signal: signal.clone(),
        },
    )
    .await;

    signal.cancel();
    let goaway = tokio::time::timeout(Duration::from_secs(10), await_goaway(&mut session))
        .await
        .expect("GOAWAY arrives");
    assert_eq!(goaway.timeout_ms, TIMEOUT_MS);

    // Deliberately do not close. This session never opened a request stream, so
    // the relay has to be enforcing the second arm of draft-19 section 3.6
    // rather than the open-subscriptions one.
    let started = Instant::now();
    let error = tokio::time::timeout(Duration::from_secs(10), raw.closed())
        .await
        .expect("relay closes the idle session rather than lingering");

    assert!(
        started.elapsed() >= Duration::from_millis(TIMEOUT_MS),
        "relay waited out the Timeout it advertised before closing, took {:?}",
        started.elapsed()
    );

    // Assert on the session close code, not merely that the session ended.
    // Letting the relay task return would also tear the connection down, so a
    // bare "it closed" assertion passes even with timeout enforcement removed.
    let rendered = format!("{error}");
    assert!(
        rendered.contains(&format!("code={}", GOAWAY_TIMEOUT_CODE)),
        "relay closed with GOAWAY_TIMEOUT (0x10), got: {rendered}"
    );

    tokio::time::timeout(Duration::from_secs(10), serve)
        .await
        .expect("serve task finishes after enforcing the timeout")
        .expect("serve task does not panic");
}
