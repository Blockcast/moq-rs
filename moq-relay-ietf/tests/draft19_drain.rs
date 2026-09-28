// SPDX-FileCopyrightText: 2026 Blockcast Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Draft-19 graceful drain, driven over a real QUIC connection.
//!
//! These exercise the relay's own serve path ([`serve_draft19_control_plane`])
//! rather than an in-process session pair, so the GOAWAY a client observes here
//! is the one the relay binary emits. The binary's shutdown-signal handling is
//! driven through the built executable itself.

use std::time::{Duration, Instant};

use moq_native_ietf::quic::{Config, Endpoint};
use moq_native_ietf::tls;
use moq_relay_ietf::{serve_draft19_control_plane, Draft19Drain};
use moq_transport::profile::draft19::{GoAway, Setup, GOAWAY_TYPE};
use moq_transport::profile::WireProfile;
use moq_transport::session::{Draft19Session, Draft19SessionRole};
use url::Url;

/// The relay advertises this as the GOAWAY New Session URI.
const NEW_SESSION_URI: &str = "https://relay-b.example.net/moq/";
/// Short enough to keep the timeout case quick, long enough that the redirect
/// case is not racing it.
const TIMEOUT_MS: u64 = 400;
/// `SessionErrorCode::GoAwayTimeout`, as the client sees it on the close.
const GOAWAY_TIMEOUT_CODE: u32 = 0x10;

/// Self-signed TLS for localhost, written as `cert.pem` and `key.pem` into a
/// scratch directory that is removed when the returned guard drops.
fn tls_files(tag: &str) -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix(&format!("moq-draft19-drain-{tag}-"))
        .tempdir()
        .expect("temp dir is creatable");

    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("self-signed cert generates");
    std::fs::write(dir.path().join("cert.pem"), certified.cert.pem()).expect("cert is writable");
    std::fs::write(
        dir.path().join("key.pem"),
        certified.key_pair.serialize_pem(),
    )
    .expect("key is writable");

    dir
}

/// Self-signed TLS for localhost, loaded through `tls::Args`.
///
/// Going through `tls::Args` rather than building a `rustls::ServerConfig` by
/// hand keeps `rcgen` the only dev-dependency this test needs.
fn tls_config(tag: &str) -> tls::Config {
    // Loading reads the files into memory, so the directory can go afterwards.
    let dir = tls_files(tag);
    tls::Args {
        cert: vec![dir.path().join("cert.pem")],
        key: vec![dir.path().join("key.pem")],
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
        // Admitted at accept time, as the relay's accept loop does.
        serve_draft19_control_plane(conn, Some(drain.admit())).await;
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
    let drain = Draft19Drain::new(Url::parse(NEW_SESSION_URI).unwrap(), TIMEOUT_MS);
    let signal = drain.signal.clone();
    let (mut session, _raw, serve) = connect("redirect", drain).await;

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
    let drain = Draft19Drain::new(Url::parse(NEW_SESSION_URI).unwrap(), TIMEOUT_MS);
    let signal = drain.signal.clone();
    // Already draining before the client ever connects.
    signal.cancel();

    let (mut session, _raw, serve) = connect("late-arrival", drain).await;

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
    let drain = Draft19Drain::new(Url::parse(NEW_SESSION_URI).unwrap(), TIMEOUT_MS);
    let signal = drain.signal.clone();
    let (mut session, raw, serve) = connect("timeout", drain).await;

    // Start the clock before the drain, not on GOAWAY receipt. The relay times
    // its deadline from sending the GOAWAY, which is after this point but ahead
    // of the client decoding it, so only this start is a sound lower bound.
    let started = Instant::now();
    signal.cancel();
    let goaway = tokio::time::timeout(Duration::from_secs(10), await_goaway(&mut session))
        .await
        .expect("GOAWAY arrives");
    assert_eq!(goaway.timeout_ms, TIMEOUT_MS);

    // Deliberately do not close. This session never opened a request stream, so
    // this pins that the relay enforces the Timeout whether or not any request
    // is open, as draft-19 sections 3.5 and 3.6 key it to the peer alone.
    tokio::time::timeout(Duration::from_secs(10), serve)
        .await
        .expect("relay closes the idle session rather than lingering")
        .expect("serve task does not panic");

    assert!(
        started.elapsed() >= Duration::from_millis(TIMEOUT_MS),
        "relay waited out the Timeout it advertised before closing, took {:?}",
        started.elapsed()
    );

    // The relay's close still has to cross loopback and be processed by this
    // client's endpoint driver, so give it a bounded wait rather than reading
    // it the instant the serve task returns. A relay that released the session
    // without ever sending the close still fails the code assertion below.
    let error = tokio::time::timeout(Duration::from_secs(1), raw.closed())
        .await
        .expect("the client sees the relay's close");

    // Assert on the session close code, not merely that the session ended.
    // Letting the relay task return would also tear the connection down, so a
    // bare "it closed" assertion passes even with timeout enforcement removed.
    let rendered = format!("{error}");
    assert!(
        rendered.contains(&format!("code={}", GOAWAY_TIMEOUT_CODE)),
        "relay closed with GOAWAY_TIMEOUT (0x10), got: {rendered}"
    );
}

/// Read the relay's log output until a line containing `needle` appears.
#[cfg(unix)]
async fn await_log<R>(lines: &mut tokio::io::Lines<R>, needle: &str)
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    await_log_line(lines, needle).await;
}

/// Read the relay's log output until a line containing `needle` appears, and
/// return that line.
#[cfg(unix)]
async fn await_log_line<R>(lines: &mut tokio::io::Lines<R>, needle: &str) -> String
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let wait = async {
        while let Some(line) = lines.next_line().await.expect("relay log is readable") {
            if line.contains(needle) {
                return line;
            }
        }
        panic!("relay exited before logging {needle:?}");
    };
    tokio::time::timeout(Duration::from_secs(10), wait)
        .await
        .unwrap_or_else(|_| panic!("relay did not log {needle:?}"))
}

/// Deliver SIGTERM to the relay process.
#[cfg(unix)]
fn sigterm(relay: &tokio::process::Child) {
    let pid = relay.id().expect("relay is still running").to_string();
    let status = std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .expect("kill runs");
    assert!(status.success(), "SIGTERM delivered to the relay");
}

/// The relay binary, its log, and the scratch directory holding its
/// certificate and coordinator file. The directory is removed when this drops,
/// after `kill_on_drop` has stopped the relay.
#[cfg(unix)]
struct DrainingRelay {
    process: tokio::process::Child,
    lines: tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStdout>>,
    _dir: tempfile::TempDir,
}

/// Start the relay binary with a draft-19 drain configured, and return it with
/// its log once the shutdown handlers are installed.
#[cfg(unix)]
async fn spawn_draining_relay(tag: &str, extra_args: &[String]) -> DrainingRelay {
    use tokio::io::AsyncBufReadExt;

    let dir = tls_files(tag);
    let mut process = tokio::process::Command::new(env!("CARGO_BIN_EXE_moq-relay-ietf"))
        .arg("--bind=127.0.0.1:0")
        .arg("--tls-cert")
        .arg(dir.path().join("cert.pem"))
        .arg("--tls-key")
        .arg(dir.path().join("key.pem"))
        .arg("--coordinator-file")
        .arg(dir.path().join("coordinator.json"))
        .arg(format!("--draft19-goaway-uri={NEW_SESSION_URI}"))
        .args(extra_args)
        .env("RUST_LOG", "info")
        .env("NO_COLOR", "1")
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("relay binary starts");
    let lines = tokio::io::BufReader::new(process.stdout.take().expect("stdout is piped")).lines();
    let mut relay = DrainingRelay {
        process,
        lines,
        _dir: dir,
    };

    // Logged only once the handlers are installed, so SIGTERM is caught from here.
    await_log(&mut relay.lines, "graceful drain armed").await;
    relay
}

/// Read the bound address from the relay binary's `listening on` log line.
#[cfg(unix)]
async fn await_listen_addr<R>(lines: &mut tokio::io::Lines<R>) -> std::net::SocketAddr
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let listening = await_log_line(lines, "listening on").await;
    listening
        .rsplit(' ')
        .next()
        .and_then(|addr| addr.parse().ok())
        .unwrap_or_else(|| panic!("relay logs its bound address, got: {listening}"))
}

/// Connect a draft-19 client to the relay binary at `addr`, returning the
/// session and the bare connection that carries the session close code.
#[cfg(unix)]
async fn connect_client(
    addr: std::net::SocketAddr,
    tag: &str,
) -> (Draft19Session, web_transport::Session) {
    let url = Url::parse(&format!("https://localhost:{}/", addr.port())).unwrap();
    let (conn, _cid, _transport, selected) = endpoint(tag)
        .client
        .connect_with_profile(&url, Some(addr), WireProfile::Draft19)
        .await
        .expect("client connects to the relay binary");
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
    (session, raw)
}

/// The first SIGTERM starts the drain and the second stops the process.
///
/// Installing the tokio signal handlers replaces the default terminate action
/// for the rest of the process, so a draining relay that ignored the second
/// signal could only be stopped with SIGKILL.
///
/// tokio coalesces two SIGTERMs delivered before the first is observed into
/// one notification, so this waits for the drain log between them. It does not
/// cover that window, which only a very fast double Ctrl-C can hit.
#[cfg(unix)]
#[tokio::test]
async fn second_shutdown_signal_stops_a_draining_relay() {
    // Keep one client connected under the default 30 s Timeout, so the drain
    // cannot complete within this test and the exit can only come from the
    // second signal.
    let DrainingRelay {
        process: mut relay,
        mut lines,
        _dir,
    } = spawn_draining_relay("signals", &["--wire-profile=draft19".to_string()]).await;
    let addr = await_listen_addr(&mut lines).await;
    let (mut session, _raw) = connect_client(addr, "signals-client").await;

    sigterm(&relay);
    await_log(&mut lines, "draining draft-19 sessions").await;
    tokio::time::timeout(Duration::from_secs(10), await_goaway(&mut session))
        .await
        .expect("GOAWAY arrives");

    sigterm(&relay);
    let status = tokio::time::timeout(Duration::from_secs(10), relay.wait())
        .await
        .expect("relay exits on the second SIGTERM instead of swallowing it")
        .expect("relay exit status is readable");
    assert!(status.success(), "relay stops cleanly, got {status}");
}

/// With no draft-19 session to drain, one SIGTERM (a supervisor's) stops the
/// relay at once and cleanly, rather than waiting to be SIGKILLed at the end
/// of the grace period.
#[cfg(unix)]
#[tokio::test]
async fn single_shutdown_signal_stops_a_relay_with_no_sessions() {
    let DrainingRelay {
        process: mut relay,
        mut lines,
        _dir,
    } = spawn_draining_relay("single-signal", &[]).await;

    sigterm(&relay);
    await_log(&mut lines, "draft-19 drain complete").await;
    let status = tokio::time::timeout(Duration::from_secs(10), relay.wait())
        .await
        .expect("relay exits without a second signal")
        .expect("relay exit status is readable");
    assert!(status.success(), "relay stops cleanly, got {status}");
}

/// The supervisor path must still close an idle draft-19 client with
/// `GOAWAY_TIMEOUT` (0x10). One SIGTERM drains the binary; the client takes the
/// GOAWAY and stays silent. The relay may only exit once that session has been
/// closed with the code, not when a timer started at the signal runs out: each
/// session's Timeout runs from its own GOAWAY send, which is later.
#[cfg(unix)]
#[tokio::test]
async fn single_shutdown_signal_closes_an_idle_client_with_goaway_timeout_before_exit() {
    let DrainingRelay {
        process: mut relay,
        mut lines,
        _dir,
    } = spawn_draining_relay(
        "idle-client",
        &[
            "--wire-profile=draft19".to_string(),
            format!("--draft19-goaway-timeout-ms={TIMEOUT_MS}"),
        ],
    )
    .await;
    let addr = await_listen_addr(&mut lines).await;
    let (mut session, raw) = connect_client(addr, "idle-client-client").await;

    let signalled = Instant::now();
    sigterm(&relay);
    let goaway = tokio::time::timeout(Duration::from_secs(10), await_goaway(&mut session))
        .await
        .expect("GOAWAY arrives");
    assert_eq!(goaway.timeout_ms, TIMEOUT_MS);

    // Stay silent. The relay has to close this session itself, and the close
    // has to reach this client before the relay exits: closing only queues the
    // CONNECTION_CLOSE frame, so a relay that exited as soon as it had closed
    // could leave this client seeing no close at all and waiting out its idle
    // timeout. Observing the code proves the frame was sent before exit.
    let error = tokio::time::timeout(Duration::from_secs(10), raw.closed())
        .await
        .expect("relay closes the idle session");
    let rendered = format!("{error}");
    assert!(
        rendered.contains(&format!("code={}", GOAWAY_TIMEOUT_CODE)),
        "relay closed with GOAWAY_TIMEOUT (0x10) before exiting, got: {rendered}"
    );
    assert!(
        signalled.elapsed() >= Duration::from_millis(TIMEOUT_MS),
        "relay waited out the advertised Timeout, took {:?}",
        signalled.elapsed()
    );

    let status = tokio::time::timeout(Duration::from_secs(10), relay.wait())
        .await
        .expect("relay exits once its draining session is closed")
        .expect("relay exit status is readable");
    assert!(status.success(), "relay stops cleanly, got {status}");
}

/// Record how `raw` closes, as it closes.
///
/// Read it at once rather than after the relay exits. Each test client owns its
/// own QUIC endpoint, and once the closed connection has drained that endpoint
/// goes away and quinn reports the connection as locally closed from then on,
/// hiding the peer's close code.
#[cfg(unix)]
fn watch_close(raw: web_transport::Session) -> tokio::task::JoinHandle<String> {
    tokio::spawn(async move { raw.closed().await.to_string() })
}

/// Connect a draft-19 client to the relay binary at `addr` and take its GOAWAY,
/// or return `None` once the relay no longer accepts or redirects connections.
#[cfg(unix)]
async fn try_connect_redirected(
    addr: std::net::SocketAddr,
    tag: &str,
) -> Option<(Draft19Session, web_transport::Session)> {
    let url = Url::parse(&format!("https://localhost:{}/", addr.port())).unwrap();
    let (conn, _cid, _transport, _selected) = endpoint(tag)
        .client
        .connect_with_profile(&url, Some(addr), WireProfile::Draft19)
        .await
        .ok()?;
    let raw = conn.clone();
    let mut session = Draft19Session::establish(
        conn,
        Draft19SessionRole::Client,
        WireProfile::Draft19,
        Setup::default(),
    )
    .await
    .ok()?;
    loop {
        if session.receive_control().await.ok()?.message_type == GOAWAY_TYPE {
            return Some((session, raw));
        }
    }
}

/// A draft-19 connection accepted while the drain is under way is sent the
/// redirect, rather than being dropped when the relay exits. The drain's
/// default 30 s Timeout means nothing here waits on a timer: both clients take
/// the redirect and close, and that completes the drain.
#[cfg(unix)]
#[tokio::test]
async fn connection_accepted_during_the_drain_is_redirected_not_dropped() {
    let DrainingRelay {
        process: mut relay,
        mut lines,
        _dir,
    } = spawn_draining_relay("arrival", &["--wire-profile=draft19".to_string()]).await;
    let addr = await_listen_addr(&mut lines).await;
    let (mut first, _first_raw) = connect_client(addr, "arrival-first").await;

    sigterm(&relay);
    tokio::time::timeout(Duration::from_secs(10), await_goaway(&mut first))
        .await
        .expect("GOAWAY arrives");

    // The drain is under way, and `first` holds it open.
    let (mut arrival, _arrival_raw) = connect_client(addr, "arrival-late").await;
    let goaway = tokio::time::timeout(Duration::from_secs(10), await_goaway(&mut arrival))
        .await
        .expect("a connection accepted mid-drain is redirected with GOAWAY");
    assert_eq!(goaway.new_session_uri.0, NEW_SESSION_URI);

    first
        .close_after_goaway()
        .expect("first client closes after GOAWAY");
    arrival
        .close_after_goaway()
        .expect("arriving client closes after GOAWAY");
    let status = tokio::time::timeout(Duration::from_secs(10), relay.wait())
        .await
        .expect("relay exits once both redirected clients have closed")
        .expect("relay exit status is readable");
    assert!(status.success(), "relay stops cleanly, got {status}");
}

/// Arrivals during the drain cannot hold it open. A client live at the signal
/// takes the GOAWAY and stays silent; after it, a new client arrives every
/// quarter Timeout and does the same, so some session is always open. The
/// relay still exits, once that first client has been closed with
/// GOAWAY_TIMEOUT and the Timeout has run out from its GOAWAY, and the arrival
/// still open then is closed at exit rather than left to time out.
#[cfg(unix)]
#[tokio::test]
async fn drain_ends_at_the_ceiling_under_continuous_arrivals() {
    let DrainingRelay {
        process: mut relay,
        mut lines,
        _dir,
    } = spawn_draining_relay(
        "ceiling",
        &[
            "--wire-profile=draft19".to_string(),
            format!("--draft19-goaway-timeout-ms={TIMEOUT_MS}"),
        ],
    )
    .await;
    let addr = await_listen_addr(&mut lines).await;
    let (mut live, live_raw) = connect_client(addr, "ceiling-live").await;
    let live_closed = watch_close(live_raw);

    sigterm(&relay);
    tokio::time::timeout(Duration::from_secs(10), await_goaway(&mut live))
        .await
        .expect("GOAWAY arrives");

    let mut arrivals = Vec::new();
    let status = {
        let arrive = async {
            for i in 0u32.. {
                if let Some((session, raw)) =
                    try_connect_redirected(addr, &format!("ceiling-arrival-{i}")).await
                {
                    arrivals.push((session, watch_close(raw)));
                }
                tokio::time::sleep(Duration::from_millis(TIMEOUT_MS / 4)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                status = relay.wait() => status,
                () = arrive => unreachable!("arrivals continue until the relay exits"),
            }
        })
        .await
        .expect("the drain ends at its ceiling while arrivals continue")
        .expect("relay exit status is readable")
    };
    assert!(status.success(), "relay stops cleanly, got {status}");

    let rendered = live_closed.await.expect("close watcher does not panic");
    assert!(
        rendered.contains(&format!("code={GOAWAY_TIMEOUT_CODE}")),
        "the client live at the signal was closed with GOAWAY_TIMEOUT, got: {rendered}"
    );

    // Each arrival's own Timeout runs out after the ceiling, so the latest one
    // was still open when the relay exited. Its session was ended by the exit,
    // not by GOAWAY_TIMEOUT, and that reached it before the process was gone: a
    // relay that exited without flushing would leave it to its idle timeout.
    let (_session, arrival_closed) = arrivals
        .pop()
        .expect("at least one client arrived during the drain");
    let rendered = tokio::time::timeout(Duration::from_secs(10), arrival_closed)
        .await
        .expect("the relay closed the arrival still open at exit")
        .expect("close watcher does not panic");
    assert!(
        !rendered.contains(&format!("code={GOAWAY_TIMEOUT_CODE}"))
            && !rendered.contains("timed out"),
        "the arrival still open at exit was closed by the exit, got: {rendered}"
    );
}
