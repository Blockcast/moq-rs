// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! A draft-20 client against a running relay: SETUP, SUBSCRIBE, media
//! forwarding, then an operator drain that delivers GOAWAY to every session.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use async_trait::async_trait;
use moq_native_ietf::{quic, tls};
use moq_relay_ietf::{
    admin, Coordinator, CoordinatorContext, CoordinatorError, CoordinatorResult, NamespaceOrigin,
    NamespaceRegistration, Relay, RelayConfig, SessionConfig, Transports,
};
use moq_transport::{
    coding::TrackNamespace,
    message::GoAway,
    serve::{self, Subgroup, TrackReaderMode},
    session::{Publisher, Subscriber},
};
use tokio::sync::watch;

/// Every namespace is local; nothing is ever found elsewhere.
struct LocalOnly;

#[async_trait]
impl Coordinator for LocalOnly {
    async fn register_namespace(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
        _context: &CoordinatorContext,
    ) -> CoordinatorResult<NamespaceRegistration> {
        Ok(NamespaceRegistration::new(()))
    }

    async fn unregister_namespace(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
    ) -> CoordinatorResult<()> {
        Ok(())
    }

    async fn lookup(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
    ) -> CoordinatorResult<(NamespaceOrigin, Option<quic::Client>)> {
        Err(CoordinatorError::NamespaceNotFound)
    }
}

const TOKEN: &str = "drain-secret";
const NEXT: &str = "https://relay-b.example.net:4443/live";

struct Harness {
    relay_port: u16,
    admin: SocketAddr,
    client: quic::Endpoint,
    _dir: TempDir,
}

struct TempDir(std::path::PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn start() -> Harness {
    start_with(Transports::default()).await
}

async fn start_with(transports: Transports) -> Harness {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let dir = std::env::temp_dir().join(format!(
        "moq-drain-e2e-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    std::fs::write(dir.join("cert.pem"), cert.cert.pem()).unwrap();
    std::fs::write(dir.join("key.pem"), cert.key_pair.serialize_pem()).unwrap();
    let server_tls = tls::Args {
        cert: vec![dir.join("cert.pem")],
        key: vec![dir.join("key.pem")],
        root: vec![],
        disable_verify: true,
    }
    .load()
    .unwrap();
    let client_tls = tls::Args {
        disable_verify: true,
        ..Default::default()
    }
    .load()
    .unwrap();

    // The relay's run future is not Send, so it gets its own runtime thread.
    let (tx, rx) = std::sync::mpsc::channel();
    let relay_tls = server_tls.clone();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            let endpoint = quic::Endpoint::new(
                quic::Config::new("127.0.0.1:0".parse().unwrap(), None, relay_tls.clone()).unwrap(),
            )
            .unwrap();
            let port = endpoint
                .server
                .as_ref()
                .unwrap()
                .local_addr()
                .unwrap()
                .port();
            let relay = Relay::new(RelayConfig {
                bind: None,
                endpoints: vec![endpoint],
                tls: relay_tls,
                qlog_dir: None,
                mlog_dir: None,
                announce: None,
                node: None,
                coordinator: Arc::new(LocalOnly),
                session: SessionConfig::default(),
                connection_tagger: None,
            })
            .unwrap()
            .with_transports(transports);
            tx.send((port, relay.drain())).unwrap();
            let _ = relay.run().await;
        });
    });
    let (relay_port, drain) = rx.recv().unwrap();

    let router = admin::router(admin::AdminConfig {
        fingerprint: server_tls.fingerprints[0].clone(),
        drain,
        drain_token: Some(TOKEN.to_string()),
        tr_url: None,
        moq_port: Some(relay_port),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await });

    let client = quic::Endpoint::new(
        quic::Config::new("127.0.0.1:0".parse().unwrap(), None, client_tls).unwrap(),
    )
    .unwrap();
    Harness {
        relay_port,
        admin,
        client,
        _dir: TempDir(dir),
    }
}

impl Harness {
    async fn connect(
        &self,
        scheme: &str,
    ) -> (web_transport::Session, moq_transport::session::Transport) {
        let url = url::Url::parse(&format!("{scheme}://localhost:{}/", self.relay_port)).unwrap();
        let (session, _cid, transport) = self.client.client.connect(&url, None).await.unwrap();
        (session, transport)
    }

    fn admin(&self, path: &str) -> String {
        format!("http://{}{path}", self.admin)
    }
}

async fn wait_go_away(mut rx: watch::Receiver<Option<GoAway>>) -> GoAway {
    tokio::time::timeout(Duration::from_secs(5), rx.wait_for(Option::is_some))
        .await
        .expect("no GOAWAY")
        .unwrap()
        .clone()
        .unwrap()
}

async fn relay_media_then_drain(scheme: &str) {
    let h = start().await;
    let namespace = TrackNamespace::from_utf8_path("live");

    // Publisher: PUBLISH_NAMESPACE, then a new group every 50 ms.
    let (conn, transport) = h.connect(scheme).await;
    let (session, mut publisher) = Publisher::connect(conn, transport).await.unwrap();
    let publisher_go_away = session.go_away_received();
    tokio::spawn(session.run());
    let (mut tracks, _, tracks_reader) = serve::Tracks::new(namespace.clone()).produce();
    let mut groups = tracks.create("video").unwrap().subgroups().unwrap();
    tokio::spawn(async move { publisher.publish_namespace(tracks_reader).await });
    tokio::spawn(async move {
        for group_id in 0..u64::MAX {
            let mut subgroup = groups
                .create(Subgroup {
                    group_id,
                    subgroup_id: 0,
                    priority: 0,
                })
                .unwrap();
            subgroup
                .write(bytes::Bytes::from(format!("object {group_id}")))
                .unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });

    // Subscriber: retry SUBSCRIBE until the namespace is registered.
    let (conn, transport) = h.connect(scheme).await;
    let (session, subscriber) = Subscriber::connect(conn, transport).await.unwrap();
    let subscriber_go_away = session.go_away_received();
    tokio::spawn(session.run());
    let payload = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let (writer, reader) = serve::Track::new(namespace.clone(), "video").produce();
            let mut subscriber = subscriber.clone();
            tokio::spawn(async move { subscriber.subscribe(writer).await });
            let read = async {
                let TrackReaderMode::Subgroups(mut subgroups) = reader.mode().await? else {
                    anyhow::bail!("expected subgroups");
                };
                let mut subgroup = subgroups.next().await?.expect("subgroup");
                Ok(subgroup.read_next().await?.expect("object"))
            };
            match tokio::time::timeout(Duration::from_secs(1), read).await {
                Ok(Ok(payload)) => break payload,
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .expect("no media through the relay");
    assert!(payload.starts_with(b"object "), "{payload:?}");

    let http = reqwest::Client::new();
    let ready = http.get(h.admin("/readyz")).send().await.unwrap();
    assert_eq!(ready.status(), 200);
    let fingerprint = http
        .get(h.admin("/certificate.sha256"))
        .send()
        .await
        .unwrap();
    assert_eq!(fingerprint.text().await.unwrap().len(), 64);

    // Unauthenticated drains are refused and change nothing.
    let drain = |token: Option<&str>| {
        let mut req = http
            .post(h.admin("/_drain"))
            .json(&serde_json::json!({"newSessionUri": NEXT, "timeoutMs": 5000}));
        if let Some(token) = token {
            req = req.header(admin::DRAIN_TOKEN_HEADER, token);
        }
        req.send()
    };
    assert_eq!(drain(None).await.unwrap().status(), 401);
    assert_eq!(drain(Some("wrong")).await.unwrap().status(), 401);
    assert!(publisher_go_away.borrow().is_none());

    let response = drain(Some(TOKEN)).await.unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["newSessionUri"], NEXT);
    assert_eq!(body["sessions"], 2);

    for rx in [publisher_go_away, subscriber_go_away] {
        let go_away = wait_go_away(rx).await;
        assert_eq!(go_away.uri.0, NEXT);
        assert_eq!(go_away.timeout, 5000);
    }

    // A session accepted while draining is told at once.
    let (conn, transport) = h.connect(scheme).await;
    let (session, _subscriber) = Subscriber::connect(conn, transport).await.unwrap();
    let late = session.go_away_received();
    tokio::spawn(session.run());
    assert_eq!(wait_go_away(late).await.uri.0, NEXT);

    let ready = http.get(h.admin("/readyz")).send().await.unwrap();
    assert_eq!(ready.status(), 503);
    let astats: serde_json::Value = http
        .get(h.admin("/_astats?inf.name=lo"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(astats["ats"]["plugin.relay_stats.draining"], 1);
    assert_eq!(
        astats["ats"]["plugin.relay_stats.drain_new_session_uri"],
        NEXT
    );
    assert_eq!(astats["system"]["notAvailable"], true);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_quic_client_receives_media_then_goaway() {
    relay_media_then_drain("moqt").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn webtransport_client_receives_media_then_goaway() {
    relay_media_then_drain("https").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn goaway_timeout_closes_the_session() {
    let h = start().await;
    let (conn, transport) = h.connect("moqt").await;
    let (session, _subscriber) = Subscriber::connect(conn, transport).await.unwrap();
    let run = tokio::spawn(session.run());
    // Let the relay register the session before draining.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let response = reqwest::Client::new()
        .post(h.admin("/_drain"))
        .header(admin::DRAIN_TOKEN_HEADER, TOKEN)
        .query(&[("newSessionUri", NEXT), ("timeoutMs", "200")])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    // The client's session ends because the relay closed it with
    // GOAWAY_TIMEOUT (0x10).
    let result = tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("relay did not close after the GOAWAY timeout")
        .unwrap();
    let err = format!("{result:?}");
    assert!(
        err.contains("ApplicationClosed") && err.contains("error_code: 16"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_disabled_transport_is_refused() {
    let h = start_with(Transports {
        web_transport: false,
        raw_quic: true,
    })
    .await;
    let (conn, transport) = h.connect("https").await;
    let refused =
        tokio::time::timeout(Duration::from_secs(5), Subscriber::connect(conn, transport))
            .await
            .expect("SETUP neither completed nor failed");
    assert!(refused.is_err(), "WebTransport must be refused");

    let (conn, transport) = h.connect("moqt").await;
    assert!(Subscriber::connect(conn, transport).await.is_ok());
}
