// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    future::Future,
    net,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;

use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_native_ietf::quic::{self, Endpoint};
use moq_transport::profile::WireProfile;
use moq_transport::session::SessionConfig;
use tokio_util::sync::CancellationToken;
use tokio_util::task::{task_tracker::TaskTrackerToken, TaskTracker};
use url::Url;

use crate::upstream_namespaces::{UpstreamNamespaces, UpstreamNamespacesRunner};
use crate::{
    metrics::GaugeGuard, ConnectionMeta, ConnectionTagger, Consumer, Coordinator, Locals, Producer,
    RelayInfo, RemoteManager, Session, SessionContext,
};

// A type alias for boxed future
type ServerFuture = Pin<
    Box<
        dyn Future<
            Output = (
                anyhow::Result<(web_transport::Session, quic::ConnInfo)>,
                quic::Server,
            ),
        >,
    >,
>;

/// Configuration for the relay.
pub struct RelayConfig {
    /// Listen on this address
    pub bind: Option<net::SocketAddr>,

    /// Optional list of endpoints if provided, we won't use bind
    pub endpoints: Vec<Endpoint>,

    /// The TLS configuration.
    pub tls: moq_native_ietf::tls::Config,

    /// Directory to write qlog files (one per connection)
    pub qlog_dir: Option<PathBuf>,

    /// Directory to write mlog files (one per connection)
    pub mlog_dir: Option<PathBuf>,

    /// Forward all PUBLISH_NAMESPACE messages to the (optional) upstream URL.
    pub announce: Option<Url>,

    /// Our hostname which we advertise to other origins.
    /// We use QUIC, so the certificate must be valid for this address.
    pub node: Option<Url>,

    /// The coordinator for namespace/track registration and discovery.
    pub coordinator: Arc<dyn Coordinator>,

    /// MoQT session configuration used for inbound and relay-to-relay sessions.
    pub session: SessionConfig,

    /// Classifies inbound connections as public clients or internal relay
    /// peers via connection tags (the well-known `interface` tag). Consulted
    /// once per accepted connection with the peer's socket address and
    /// connection path.
    ///
    /// When `None`, every inbound connection is treated as a public client.
    /// Outbound connections the relay dials itself (`--announce`,
    /// [`RemoteManager`]) are always tagged internal and bypass this.
    pub connection_tagger: Option<Arc<dyn ConnectionTagger>>,

    /// Graceful-drain parameters for draft-19 sessions.
    ///
    /// When `None` the relay never drains draft-19 sessions and they run until
    /// the peer disconnects.
    pub draft19_drain: Option<Draft19Drain>,
}

/// Graceful-drain parameters for draft-19 sessions.
///
/// Cancelling [`Self::signal`] starts the drain. Every live draft-19 session
/// is sent a GOAWAY carrying [`Self::new_session_uri`] and
/// [`Self::timeout_ms`], and every draft-19 session established afterwards is
/// sent the same GOAWAY immediately after SETUP. That redirect is how
/// draft-ietf-moq-transport-19 §3.6 expects a draining server to turn new
/// arrivals away: the peer learns where to reconnect instead of seeing a bare
/// close.
///
/// Draining is scoped to draft-19. Draft-16 media routing is unaffected.
///
/// Each accepted draft-19 connection is [admitted](Self::admit) before it is
/// served, and [`Self::finished`] reports when the drain is over.
#[derive(Clone, Debug)]
pub struct Draft19Drain {
    /// Advertised as the GOAWAY New Session URI. Clients redial here.
    pub new_session_uri: Url,

    /// Advertised as the GOAWAY millisecond Timeout, and enforced: once it
    /// elapses with the peer still connected, the relay closes the session
    /// with `GOAWAY_TIMEOUT` (`0x10`). It also bounds the drain; see
    /// [`Self::finished`].
    ///
    /// Zero advertises no deadline, so the relay waits for the peer instead.
    pub timeout_ms: u64,

    /// Cancel to begin draining.
    pub signal: CancellationToken,

    /// Every admitted draft-19 connection, from accept until its session ends.
    sessions: TaskTracker,

    /// The admitted connections that were live when the drain began: the ones
    /// the drain's GOAWAY broadcast redirects. Arrivals during the drain are
    /// redirected too, but are not part of the broadcast.
    broadcast: TaskTracker,

    /// When the broadcast's most recent GOAWAY was sent.
    last_broadcast_goaway: Arc<std::sync::Mutex<Option<Instant>>>,
}

/// Why [`Draft19Drain::finished`] resolved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Draft19DrainEnd {
    /// Every admitted draft-19 session has ended.
    Drained,

    /// Every session live when the drain began has been redirected and has
    /// ended, and the GOAWAY Timeout has since run out from the broadcast's
    /// last GOAWAY. Sessions that arrived during the drain may still be open.
    Ceiling,
}

impl Draft19Drain {
    pub fn new(new_session_uri: Url, timeout_ms: u64) -> Self {
        Self {
            new_session_uri,
            timeout_ms,
            signal: CancellationToken::new(),
            sessions: TaskTracker::new(),
            broadcast: TaskTracker::new(),
            last_broadcast_goaway: Default::default(),
        }
    }

    /// Admit an accepted draft-19 connection to the drain.
    ///
    /// Call at accept time, before the connection's task is spawned or polled,
    /// and hand the result to [`serve_draft19_control_plane`]. The drain cannot
    /// be [finished](Self::finished) while the admission is alive, so a
    /// connection accepted mid-drain is redirected rather than dropped at exit.
    pub fn admit(&self) -> Draft19Admission {
        Draft19Admission {
            drain: self.clone(),
            _session: self.sessions.token(),
            broadcast: (!self.signal.is_cancelled()).then(|| self.broadcast.token()),
        }
    }

    /// Resolve once the drain is over. Call after cancelling [`Self::signal`].
    ///
    /// The drain is over when every admitted session has ended
    /// ([`Draft19DrainEnd::Drained`]), or at the ceiling
    /// ([`Draft19DrainEnd::Ceiling`]), whichever comes first. The ceiling is
    /// [`Self::timeout_ms`] after the last GOAWAY the broadcast sent, but never
    /// earlier than [`Self::timeout_ms`] after this call, so a broadcast whose
    /// last GOAWAY predates it still gets the full window. It waits for every
    /// broadcast session to end first. Each of those closes by its own Timeout,
    /// so none is cut short, and each keeps its `GOAWAY_TIMEOUT` close unless
    /// the close flush itself expires first; see `GOAWAY_CLOSE_FLUSH_MS`.
    /// Arrivals during the drain cannot move the ceiling, so a steady stream of
    /// them cannot hold the drain open.
    ///
    /// A zero Timeout advertises no deadline, so there is no ceiling and the
    /// drain ends only once every session, arrivals included, has ended.
    pub async fn finished(&self) -> Draft19DrainEnd {
        let began = Instant::now();
        self.sessions.close();
        self.broadcast.close();

        let ceiling = async {
            self.broadcast.wait().await;
            if self.timeout_ms == 0 {
                return std::future::pending().await;
            }
            let last_goaway = self
                .last_broadcast_goaway
                .lock()
                .expect("drain GOAWAY clock is never poisoned")
                .map_or(began, |sent| sent.max(began));
            let deadline = last_goaway + Duration::from_millis(self.timeout_ms);
            tokio::time::sleep_until(deadline.into()).await;
        };

        tokio::select! {
            () = self.sessions.wait() => Draft19DrainEnd::Drained,
            () = ceiling => Draft19DrainEnd::Ceiling,
        }
    }
}

/// A draft-19 connection's place in a [`Draft19Drain`], from accept until its
/// session ends. See [`Draft19Drain::admit`].
#[derive(Debug)]
pub struct Draft19Admission {
    drain: Draft19Drain,
    _session: TaskTrackerToken,
    /// Held when admitted before the drain began.
    broadcast: Option<TaskTrackerToken>,
}

impl Draft19Admission {
    /// Record that this session's drain GOAWAY has been sent.
    fn goaway_sent(&self, sent: Instant) {
        if self.broadcast.is_some() {
            let mut last = self
                .drain
                .last_broadcast_goaway
                .lock()
                .expect("drain GOAWAY clock is never poisoned");
            *last = Some(last.map_or(sent, |last| last.max(sent)));
        }
    }
}

impl RelayConfig {
    /// Build a relay from this configuration.
    pub fn build(self) -> anyhow::Result<Relay> {
        Relay::new(self)
    }
}

/// MoQ Relay server.
pub struct Relay {
    config: RelayConfig,
    locals: Locals,
    remotes: RemoteManager,
    upstream_namespaces: UpstreamNamespaces,
    upstream_namespaces_runner: UpstreamNamespacesRunner,
}

impl Relay {
    pub fn new(mut config: RelayConfig) -> anyhow::Result<Self> {
        if config.bind.is_some() && !config.endpoints.is_empty() {
            anyhow::bail!("cannot specify both bind and endpoints");
        }

        if let Some(bind) = config.bind.take() {
            let endpoint = quic::Endpoint::new(quic::Config::new(
                bind,
                config.qlog_dir.clone(),
                config.tls.clone(),
            )?)?;
            config.endpoints = vec![endpoint];
        }

        if config.endpoints.is_empty() {
            anyhow::bail!("no endpoints available to start the server");
        }

        // Validate mlog directory if provided
        if let Some(mlog_dir) = &config.mlog_dir {
            if !mlog_dir.exists() {
                anyhow::bail!("mlog directory does not exist: {}", mlog_dir.display());
            }
            if !mlog_dir.is_dir() {
                anyhow::bail!("mlog path is not a directory: {}", mlog_dir.display());
            }
            tracing::info!("mlog output enabled: {}", mlog_dir.display());
        }

        let locals = Locals::new();

        // FIXME(itzmanish): have a generic filter to find endpoints for forward, remote etc.
        let remote_clients = config
            .endpoints
            .iter()
            .map(|endpoint| endpoint.client.clone())
            .collect::<Vec<_>>();

        // Create remote manager - uses coordinator for namespace lookups
        let remotes = RemoteManager::new_with_session_config(
            config.coordinator.clone(),
            remote_clients,
            config.session,
        );
        let (upstream_namespaces, upstream_namespaces_runner) =
            UpstreamNamespaces::new(locals.clone(), remotes.clone(), config.coordinator.clone());

        Ok(Self {
            config,
            locals,
            remotes,
            upstream_namespaces,
            upstream_namespaces_runner,
        })
    }

    /// Retain the recent groups of every track this relay receives, so a
    /// standalone FETCH for them is answered locally instead of being
    /// forwarded upstream (draft-ietf-moq-transport-16 §9.16.3).
    ///
    /// Call before [`Self::run`]. The relay-local and remote track registries
    /// are replaced, and the namespace manager rebuilt over them, so every
    /// track registered from then on is retained.
    pub fn with_fetch_retention(mut self, fetch_retention: crate::FetchRetention) -> Self {
        self.locals = self.locals.with_fetch_retention(fetch_retention.clone());
        self.remotes = self.remotes.with_fetch_retention(fetch_retention);
        let (upstream_namespaces, upstream_namespaces_runner) = UpstreamNamespaces::new(
            self.locals.clone(),
            self.remotes.clone(),
            self.config.coordinator.clone(),
        );
        self.upstream_namespaces = upstream_namespaces;
        self.upstream_namespaces_runner = upstream_namespaces_runner;
        self
    }

    /// Run the relay server.
    pub async fn run(self) -> anyhow::Result<()> {
        let Self {
            config,
            locals,
            remotes,
            upstream_namespaces,
            upstream_namespaces_runner,
        } = self;

        let RelayConfig {
            endpoints: quic_endpoints,
            announce: announce_url,
            mlog_dir,
            coordinator,
            session: session_config,
            connection_tagger,
            draft19_drain,
            ..
        } = config;

        let run_result = async {
            let mut tasks = FuturesUnordered::new();
            tasks.push(
                async move {
                    upstream_namespaces_runner.run().await;
                    Ok::<(), anyhow::Error>(())
                }
                .boxed(),
            );

            // Use the remote manager for routing to remote relays.
            let remote_manager = remotes.clone();

            // Start the forwarder, if any
            let forward_producer = if let Some(url) = &announce_url {
                tracing::info!("forwarding PUBLISH_NAMESPACE messages to {}", url);

                // Establish a QUIC connection to the forward URL
                let (session, _quic_client_initial_cid, transport, selected_version) = quic_endpoints[0]
                    .client
                    .connect(url, None)
                    .await
                    .context("failed to establish forward connection")?;

                // Create the MoQ session over the connection
                let (session, publisher, subscriber) = moq_transport::session::Session::connect_with_profile(
                    session,
                    None,
                    transport,
                    selected_version,
                    session_config,
                )
                .await
                .context("failed to establish forward session")?;

                // Use the connection path already validated and stored by Session::connect().
                // The forward session is scoped to whatever path the announce URL specifies.
                //
                // Note: the forward connection intentionally does not call
                // coordinator.resolve_scope(). The announce URL is operator-configured
                // (via --announce), not client-supplied, so it doesn't need the same
                // auth/permission checks that incoming client connections get. The
                // forward session always gets both Producer and Consumer (full
                // read-write) since it's acting as a relay peer, not a client.
                //
                // Limitation: all incoming scopes are forwarded to this single upstream scope.
                // Multi-scope forwarding (routing different incoming scopes to different
                // upstream paths) would require per-scope forward connections.
                let forward_scope = session.connection_path().map(|s| s.to_string());
                let forward_context = SessionContext::internal(
                    forward_scope,
                    Some(RelayInfo::new(url.clone())),
                );

                let forward_coordinator = coordinator.clone();
                let session = Session {
                    session,
                    producer: Some(Producer::new_with_upstream_namespaces(
                        publisher,
                        locals.clone(),
                        remote_manager.clone(),
                        upstream_namespaces.clone(),
                        forward_context.clone(),
                    )),
                    consumer: Some(Consumer::new(
                        subscriber,
                        locals.clone(),
                        forward_coordinator,
                        remote_manager.clone(),
                        None,
                        forward_context,
                    )),
                    // Forward connections are always full read-write relay peers,
                    // so no reject loops needed.
                    reject_publishes: None,
                    reject_subscribes: None,
                };

                let forward_producer = session.producer.clone();

                tasks.push(async move { session.run().await.context("forwarding failed") }.boxed());

                forward_producer
            } else {
                None
            };

            let servers: Vec<quic::Server> = quic_endpoints
                .into_iter()
                .map(|endpoint| endpoint.server.context("missing TLS certificate for server"))
                .collect::<anyhow::Result<_>>()?;

            // This will hold the futures for all our listening servers.
            let mut accepts: FuturesUnordered<ServerFuture> = FuturesUnordered::new();
            for mut server in servers {
                tracing::info!("listening on {}", server.local_addr()?);

                // Create a future, box it, and push it to the collection.
                accepts.push(
                    async move {
                        let conn = server.accept().await.context("accept failed");
                        (conn, server)
                    }
                    .boxed(),
                );
            }

            loop {
                tokio::select! {
                    // This branch polls all the `accept` futures concurrently.
                    Some((conn_result, mut server)) = accepts.next() => {
                        // An accept operation has completed.
                        // First, immediately queue up the next accept() call for this server.
                        accepts.push(
                            async move {
                                let conn = server.accept().await.context("accept failed");
                                (conn, server)
                            }
                            .boxed(),
                        );

                        let (conn, info) = conn_result.context("failed to accept QUIC connection")?;
                        let quic::ConnInfo {
                            id: connection_id,
                            transport,
                            selected_version,
                            remote_address: remote_addr,
                            // The local IP the connection was accepted on
                            // (destination IP the peer targeted); forwarded to the
                            // connection tagger for inbound-interface classification.
                            local_ip,
                            server_name,
                        } = info;

                        metrics::counter!(
                            "moq_relay_connections_total",
                            "transport" => match transport {
                                moq_transport::session::Transport::WebTransport => "webtransport",
                                moq_transport::session::Transport::RawQuic => "raw_quic",
                            },
                            "selected_version" => selected_version.name(),
                        )
                        .increment(1);

                        // Construct mlog path from connection ID if mlog directory is configured
                        let mlog_path = mlog_dir.as_ref()
                            .map(|dir| dir.join(format!("{}_server.mlog", connection_id)));

                        let locals = locals.clone();
                        let remotes = remote_manager.clone();
                        let forward = forward_producer.clone();
                        let coordinator = coordinator.clone();
                        let upstream_namespaces = upstream_namespaces.clone();
                        let connection_tagger = connection_tagger.clone();
                        // Admit draft-19 connections to the drain here, before
                        // their task is pushed or polled, so a connection
                        // accepted while the drain is momentarily empty holds
                        // it open instead of being dropped at exit.
                        let draft19_admission = (selected_version == WireProfile::Draft19)
                            .then(|| draft19_drain.as_ref().map(Draft19Drain::admit))
                            .flatten();

                        // Spawn a new task to handle the connection
                        tasks.push(async move {
                            // Track active connections - decrements when task completes
                            let _conn_guard = GaugeGuard::new("moq_relay_active_connections");

                            // Clone the raw connection so we can close it with a proper
                            // error code if scope resolution fails after the MoQ handshake.
                            let raw_conn = conn.clone();

                            // Draft-19 is a different wire (two unidirectional
                            // control streams, SETUP 0x2f00) and the relay's
                            // media routing is draft-16 only. Serve the control
                            // plane and say so, rather than handing the
                            // connection to a session that would misframe it.
                            if selected_version == WireProfile::Draft19 {
                                serve_draft19_control_plane(conn, draft19_admission).await;
                                metrics::counter!("moq_relay_connections_closed_total").increment(1);
                                return Ok(());
                            }

                            // Create the MoQ session over the connection (setup handshake etc)
                            let (session, publisher, subscriber) = match moq_transport::session::Session::accept_with_profile(conn, mlog_path, transport, selected_version, session_config).await {
                                Ok(session) => session,
                                Err(err) => {
                                    tracing::warn!(error = %err, "failed to accept MoQ session: {}", err);
                                    metrics::counter!("moq_relay_connection_errors_total", "stage" => "session_accept").increment(1);
                                    // Maintain invariant: connections_total - connections_closed_total == active_connections
                                    metrics::counter!("moq_relay_connections_closed_total").increment(1);
                                    return Ok(());
                                }
                            };

                            // Create our MoQ relay session
                            let moq_session = session;

                            // Resolve the connection path to a scope (identity + permissions).
                            // This translates the raw transport-level path into an application-level
                            // scope_id and determines what the connection is allowed to do.
                            let scope_info = match coordinator.resolve_scope(moq_session.connection_path()).await {
                                Ok(info) => info,
                                Err(err) => {
                                    tracing::warn!(
                                        connection_path = moq_session.connection_path(),
                                        error = %err,
                                        "scope resolution failed, rejecting session"
                                    );
                                    // Close with PROTOCOL_VIOLATION (0x3) so the client
                                    // gets a meaningful error instead of an abrupt reset.
                                    // This is a QUIC APPLICATION_CLOSE, not a MoQT SESSION_CLOSE
                                    // control message. Sending a proper SESSION_CLOSE would require
                                    // running the MoQ session's send loop, which is not warranted
                                    // for a pre-session rejection. The QUIC close code and reason
                                    // string are visible to the client's transport layer.
                                    raw_conn.close(0x3, "scope resolution failed");
                                    metrics::counter!("moq_relay_connection_errors_total", "stage" => "scope_resolve").increment(1);
                                    metrics::counter!("moq_relay_connections_closed_total").increment(1);
                                    return Ok(());
                                }
                            };

                            let can_publish = scope_info.as_ref().is_none_or(|s| s.permissions.can_publish());
                            let can_subscribe = scope_info.as_ref().is_none_or(|s| s.permissions.can_subscribe());

                            // Classify the connection interface (public client vs internal
                            // relay peer). This is deliberately separate from scope
                            // resolution above: resolve_scope() returns identity +
                            // permissions, while the embedder-supplied tagger decides the
                            // transport interface from the peer socket address, TLS SNI, and
                            // connection path. With no tagger configured every inbound
                            // connection is treated as a public client. For connections
                            // classified internal, the peer relay identity is derived from
                            // the inbound socket address (see RelayInfo::from_socket_addr).
                            let scope = scope_info.as_ref().map(|info| info.scope_id.clone());
                            let context = match connection_tagger.as_ref() {
                                Some(tagger) => {
                                    let meta = ConnectionMeta::new(
                                        Some(remote_addr),
                                        server_name,
                                        moq_session.connection_path().map(str::to_string),
                                    )
                                    .with_local_ip(local_ip);
                                    let tags = tagger.tag(&meta);
                                    SessionContext::from_tags(scope, &tags, Some(remote_addr))
                                }
                                None => SessionContext::public(scope),
                            };

                            if let Some(ref info) = scope_info {
                                tracing::debug!(
                                    connection_path = moq_session.connection_path(),
                                    scope_id = %info.scope_id,
                                    permissions = ?info.permissions,
                                    "scope resolved"
                                );
                            }

                            // Gate Producer/Consumer creation on permissions.
                            // Note the intentional inversion:
                            // - Producer serves SUBSCRIBEs → gated on can_subscribe
                            // - Consumer handles PUBLISH_NAMESPACEs → gated on can_publish
                            //
                            // When a half is disabled, we pass its transport counterpart
                            // to the Session's reject fields so unauthorized messages get
                            // an explicit error response instead of being silently ignored.
                            let (producer, reject_subscribes) = if can_subscribe {
                                (publisher.map(|publisher| Producer::new_with_upstream_namespaces(publisher, locals.clone(), remotes.clone(), upstream_namespaces, context.clone())), None)
                            } else {
                                (None, publisher)
                            };

                            let (consumer, reject_publishes) = if can_publish {
                                (subscriber.map(|subscriber| Consumer::new(subscriber, locals, coordinator, remotes.clone(), forward, context)), None)
                            } else {
                                (None, subscriber)
                            };

                            let session = Session {
                                session: moq_session,
                                producer,
                                consumer,
                                reject_publishes,
                                reject_subscribes,
                            };

                            match session.run().await {
                                Ok(()) => {
                                    // Session ended cleanly (uncommon - usually ends via close)
                                    metrics::counter!("moq_relay_connections_closed_total").increment(1);
                                }
                                Err(err) if err.is_graceful_close() => {
                                    // Graceful close - peer sent APPLICATION_CLOSE with code 0
                                    tracing::debug!("MoQ session closed gracefully");
                                    metrics::counter!("moq_relay_connections_closed_total").increment(1);
                                }
                                Err(err) => {
                                    // Actual error - protocol violation, timeout, etc.
                                    tracing::warn!(error = %err, "MoQ session error: {}", err);
                                    metrics::counter!("moq_relay_connection_errors_total", "stage" => "session_run").increment(1);
                                    metrics::counter!("moq_relay_connections_closed_total").increment(1);
                                }
                            }

                            Ok(())
                        }.boxed());
                    },
                    res = tasks.next(), if !tasks.is_empty() => res.unwrap()?,
                }
            }
        }
        .await;

        remotes.shutdown().await;
        run_result
    }
}

/// How long a `GOAWAY_TIMEOUT` close gets to reach the peer.
///
/// Deliberately not the drain's advertised Timeout. That is the deadline given
/// to the *peer to act on the GOAWAY*; this is the deadline for *our own close
/// to flush*, an RTT-scale quantity unrelated to it. Reusing the advertised one
/// would hand any deployment that advertises a sub-RTT Timeout a close that
/// expires before it can land.
///
/// Chosen to sit above web-transport-quinn's own `max(3 * RTT, 100ms)` close
/// budget at ordinary RTTs, so on the paths that library bounds it is the
/// library that fires first. The two cross at RTT ~= 333ms; above that this
/// bound fires first and releases the admission while the library's spawned
/// close is still running, which is benign because that task still force-closes
/// with the right code afterwards.
const GOAWAY_CLOSE_FLUSH_MS: u64 = 1_000;

/// Serve a draft-19 session's control plane, and only that.
///
/// The relay routes media through the draft-16 `Publisher`/`Subscriber` pair,
/// which has no draft-19 counterpart. A draft-19 connection therefore gets a
/// conformant SETUP exchange and GOAWAY handling, and every other control
/// message is refused out loud. Media routing over draft-19 is out of scope;
/// see the `--wire-profile` help text.
///
/// When `admission` is supplied and its drain's signal is cancelled, the
/// session is sent a GOAWAY carrying the configured New Session URI and
/// millisecond Timeout, and then closed: gracefully once the peer closes, or
/// with `GOAWAY_TIMEOUT` (`0x10`) once the advertised Timeout elapses. A
/// session that starts after the signal already fired takes the same path
/// immediately after SETUP. The admission is held until this returns, which
/// for a `GOAWAY_TIMEOUT` close is until the session reports itself closed or
/// `GOAWAY_CLOSE_FLUSH_MS` elapses, whichever is first. So
/// [`Draft19DrainEnd::Drained`] means every session ended, not that every peer
/// acknowledged its close.
pub async fn serve_draft19_control_plane(
    conn: web_transport::Session,
    admission: Option<Draft19Admission>,
) {
    use moq_transport::profile::draft19::Setup;
    use moq_transport::session::{Draft19Session, Draft19SessionRole};

    let drain = admission.as_ref().map(|admission| &admission.drain);
    let raw_conn = conn.clone();

    let mut session = match Draft19Session::establish(
        conn,
        Draft19SessionRole::Server,
        WireProfile::Draft19,
        Setup::default(),
    )
    .await
    {
        Ok(session) => session,
        Err(err) => {
            tracing::warn!(error = %err, "failed to establish draft-19 session");
            metrics::counter!("moq_relay_connection_errors_total", "stage" => "session_accept")
                .increment(1);
            return;
        }
    };

    tracing::info!(
        selected_version = %session.selected_version(),
        "draft-19 session established: control plane only, relay media routing is not implemented"
    );

    // Serve until the peer goes away or the relay begins draining.
    loop {
        tokio::select! {
            result = session.receive_control() => match result {
                Ok(frame) => tracing::warn!(
                    message_type = frame.message_type,
                    "refusing draft-19 control message: relay media routing is not implemented"
                ),
                Err(err) => {
                    tracing::info!(error = %err, "draft-19 session closed");
                    return;
                }
            },
            () = drain_signalled(drain) => break,
        }
    }

    let (Some(admission), Some(drain)) = (admission.as_ref(), drain) else {
        // `drain_signalled` never completes without a drain configured, so the
        // loop above cannot break here.
        unreachable!("drain loop broke without a configured drain");
    };

    let goaway = moq_transport::profile::draft19::GoAway {
        new_session_uri: moq_transport::coding::SessionUri(drain.new_session_uri.to_string()),
        timeout_ms: drain.timeout_ms,
    };
    let frame = match goaway.into_frame() {
        Ok(frame) => frame,
        Err(err) => {
            tracing::error!(error = %err, "failed to encode draft-19 drain GOAWAY");
            return;
        }
    };
    if let Err(err) = session.send_control(&frame).await {
        tracing::warn!(error = %err, "failed to send draft-19 drain GOAWAY");
        return;
    }
    admission.goaway_sent(Instant::now());
    metrics::counter!("moq_relay_draft19_goaway_sent_total").increment(1);
    tracing::info!(
        new_session_uri = %drain.new_session_uri,
        timeout_ms = drain.timeout_ms,
        "draft-19 drain: sent GOAWAY"
    );

    // Wait for the peer to close, bounded by the Timeout we just advertised.
    let peer_close = async {
        loop {
            match session.receive_control().await {
                Ok(frame) => tracing::warn!(
                    message_type = frame.message_type,
                    "refusing draft-19 control message: draining"
                ),
                Err(err) => return err,
            }
        }
    };

    if drain.timeout_ms == 0 {
        // Timeout=0 advertises no deadline, so there is nothing to enforce.
        let err = peer_close.await;
        tracing::info!(error = %err, "draft-19 session closed after GOAWAY");
        return;
    }

    let outcome = tokio::time::timeout(Duration::from_millis(drain.timeout_ms), peer_close).await;

    match outcome {
        Ok(err) => tracing::info!(error = %err, "draft-19 session closed after GOAWAY"),
        Err(_elapsed) => {
            // A control-plane-only session never opens a request stream.
            if session.enforce_control_goaway_timeout(Instant::now(), false) {
                metrics::counter!("moq_relay_draft19_goaway_timeout_total").increment(1);
                tracing::warn!(
                    timeout_ms = drain.timeout_ms,
                    "draft-19 GOAWAY timeout elapsed without peer close: closing with GOAWAY_TIMEOUT"
                );
                // `close` only records the code. Wait for the session to
                // report itself closed before the admission is released. Over
                // WebTransport that is once the close capsule has been
                // delivered; over raw QUIC it is at once, and the
                // CONNECTION_CLOSE frame itself is flushed by the process
                // waiting for its endpoint to go idle before it exits.
                //
                // Bounded, because this wait gates the admission, which gates
                // both `Drained` and `Ceiling`. A peer that simply never
                // answers the capsule is already bounded inside
                // web-transport-quinn: `close` spawns a task that writes the
                // capsule, waits `max(3 * RTT, 100ms)` for the connection to
                // close, and then force-closes it with the
                // `GOAWAY_TIMEOUT`-derived code. The residual unbounded path is
                // narrower than that: the capsule's `write_all` stalled on
                // flow-control credit never errors, so none of that task's
                // force-close branches is reached and its own timeout is never
                // armed. That stall is what this bound catches.
                //
                // So expiry costs nothing observable on the paths the library
                // bounds, only an early admission release. On the stall path it
                // costs the close code, because the connection is still open
                // and the endpoint closer then closes it with its own. Raw QUIC
                // reaches none of this, since `closed()` is already resolved by
                // the time it is awaited.
                let _ = tokio::time::timeout(
                    Duration::from_millis(GOAWAY_CLOSE_FLUSH_MS),
                    raw_conn.closed(),
                )
                .await;
            }
        }
    }
}

/// Resolve once the drain signal fires, or never when no drain is configured.
async fn drain_signalled(drain: Option<&Draft19Drain>) {
    match drain {
        Some(drain) => drain.signal.cancelled().await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drain(timeout_ms: u64) -> Draft19Drain {
        Draft19Drain::new(
            Url::parse("https://relay-b.example.net/moq/").unwrap(),
            timeout_ms,
        )
    }

    /// A connection admitted at accept time holds the drain open before its
    /// task is ever polled, so it cannot be dropped at exit in that window.
    #[tokio::test]
    async fn an_admission_holds_the_drain_open_before_its_connection_is_served() {
        let drain = drain(60_000);
        drain.signal.cancel();

        let admission = drain.admit();
        assert_eq!(
            drain.finished().now_or_never(),
            None,
            "an admitted but unserved connection keeps the drain open"
        );

        drop(admission);
        assert_eq!(
            drain.finished().now_or_never(),
            Some(Draft19DrainEnd::Drained),
            "the drain is over once its last admission is released"
        );
    }

    /// A steady stream of arrivals, each still open when the next comes, cannot
    /// hold the drain open past the Timeout counted from the broadcast's last
    /// GOAWAY.
    #[tokio::test]
    async fn arrivals_cannot_hold_the_drain_open_past_the_ceiling() {
        const TIMEOUT_MS: u64 = 200;
        let drain = drain(TIMEOUT_MS);
        let live = drain.admit();
        drain.signal.cancel();

        let sent = Instant::now();
        live.goaway_sent(sent);
        drop(live);

        // The first arrival is admitted before the drain is polled, so the
        // drain is never empty from here on.
        let mut open = vec![drain.admit()];
        let arrive = async {
            loop {
                tokio::time::sleep(Duration::from_millis(TIMEOUT_MS / 4)).await;
                open.push(drain.admit());
            }
        };
        let end = tokio::select! {
            end = drain.finished() => end,
            () = arrive => unreachable!("arrivals never stop"),
        };

        assert_eq!(end, Draft19DrainEnd::Ceiling);
        assert!(
            sent.elapsed() >= Duration::from_millis(TIMEOUT_MS),
            "the ceiling is the Timeout after the last broadcast GOAWAY, took {:?}",
            sent.elapsed()
        );
    }
}
