// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::HashSet,
    fmt,
    fs::File,
    io::BufWriter,
    net::{self, IpAddr},
    path::PathBuf,
    sync::{Arc, Mutex},
    time,
};

use anyhow::Context;
use clap::Parser;
use socket2::{Domain, Protocol, Socket, Type};
use url::Url;

use moq_transport::{profile::WireProfile, session::Transport};

use crate::tls;

use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;

type AcceptedSession = (web_transport::Session, String, Transport, WireProfile);
type AcceptFuture = BoxFuture<'static, anyhow::Result<AcceptedSession>>;

/// Represents the address family of the local QUIC socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressFamily {
    Ipv4,
    Ipv6,
    /// IPv6 with dual-stack support (IPV6_V6ONLY=false)
    Ipv6DualStack,
}

pub enum Host {
    Ip(IpAddr),
    Name(String),
}

impl fmt::Display for AddressFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AddressFamily::Ipv4 => write!(f, "IPv4"),
            AddressFamily::Ipv6 => write!(f, "IPv6"),
            AddressFamily::Ipv6DualStack => write!(f, "IPv6 (dual stack)"),
        }
    }
}

/// Bind a UDP socket, attempting dual-stack if the address is IPv6.
///
/// For IPv6 addresses, attempts to set `IPV6_V6ONLY = false` to enable
/// dual-stack operation (accepting both IPv4 and IPv6 traffic). This is
/// the default on Linux but must be explicitly requested on macOS/Windows.
///
/// Returns `(socket, is_dual_stack)` where `is_dual_stack` indicates
/// whether the socket can handle both IPv4 and IPv6 destinations.
fn bind_smart(addr: net::SocketAddr) -> anyhow::Result<(net::UdpSocket, bool)> {
    let domain = if addr.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))
        .context("failed to create UDP socket")?;

    let mut is_dual_stack = false;

    if addr.is_ipv6() {
        match socket.set_only_v6(false) {
            Ok(()) => {
                is_dual_stack = true;
                tracing::debug!(addr = %addr, "IPv6 dual-stack enabled (IPV6_V6ONLY=false)");
            }
            Err(e) => {
                tracing::warn!(
                    addr = %addr,
                    error = %e,
                    "Could not enable dual-stack on IPv6 socket; \
                     IPv4-only destinations may be unreachable"
                );
            }
        }
    }

    socket
        .bind(&addr.into())
        .with_context(|| format!("failed to bind UDP socket to {}", addr))?;

    let local_addr = match socket.local_addr() {
        Ok(a) => a
            .as_socket()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "<non-IP address>".to_string()),
        Err(e) => {
            tracing::warn!(error = %e, "failed to get local address after successful bind");
            "<unknown>".to_string()
        }
    };

    tracing::info!(
        bind = %addr,
        local = %local_addr,
        dual_stack = is_dual_stack,
        "UDP socket bound"
    );

    Ok((socket.into(), is_dual_stack))
}

/// Build a TransportConfig with our standard settings
///
/// This is used both for the base endpoint config and when creating
/// per-connection configs with qlog enabled.
fn build_transport_config() -> quinn::TransportConfig {
    // A 1,228-byte Solana shred needs up to 1,234 bytes after MoQ framing.
    // Quinn consumes another 39 bytes at the current raw-QUIC profile, so the
    // controlled shred path must support at least a 1,280-byte UDP payload
    // (1,328-byte IPv6 PMTU). DPLPMTUD can raise this on Ethernet paths and
    // black-hole detection can lower it when the path contract is violated.
    const INITIAL_UDP_PAYLOAD_MTU: u16 = 1_280;

    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(time::Duration::from_secs(10).try_into().unwrap()));
    transport.keep_alive_interval(Some(time::Duration::from_secs(4))); // TODO make this smarter
    transport.congestion_controller_factory(Arc::new(quinn::congestion::BbrConfig::default()));
    transport.initial_mtu(INITIAL_UDP_PAYLOAD_MTU);
    transport.mtu_discovery_config(Some(quinn::MtuDiscoveryConfig::default()));
    transport
}

fn select_wire_profile(offered: &[String], supported: &[WireProfile]) -> Option<WireProfile> {
    supported.iter().copied().find(|profile| {
        offered
            .iter()
            .any(|offered| offered.as_str() == profile.name())
    })
}

fn validate_selected_profile(
    required: WireProfile,
    selected: Option<&str>,
) -> anyhow::Result<WireProfile> {
    anyhow::ensure!(
        selected == Some(required.name()),
        "WebTransport protocol mismatch: required={} selected={}",
        required,
        selected.unwrap_or("<none>")
    );
    Ok(required)
}

fn offered_profiles_label(offered: &[String]) -> &'static str {
    let draft19 = offered.iter().any(|profile| profile == "moqt-19");
    let draft16 = offered.iter().any(|profile| profile == "moqt-16");
    let unknown = offered
        .iter()
        .any(|profile| WireProfile::from_name(profile).is_none());
    match (draft19, draft16, unknown) {
        (false, false, false) => "none",
        (true, false, false) => "moqt-19",
        (false, true, false) => "moqt-16",
        (true, true, false) => "moqt-19+moqt-16",
        (false, false, true) => "unknown",
        _ => "known+unknown",
    }
}

fn supported_profiles_label(supported: &[WireProfile]) -> &'static str {
    let draft19 = supported.contains(&WireProfile::Draft19);
    let draft16 = supported.contains(&WireProfile::Draft16);
    match (draft19, draft16) {
        (false, false) => "none",
        (true, false) => "moqt-19",
        (false, true) => "moqt-16",
        (true, true) => "moqt-19+moqt-16",
    }
}

fn required_profile_label(offered: &[String]) -> &'static str {
    if offered.len() != 1 {
        return if offered.is_empty() {
            "none"
        } else {
            "multiple"
        };
    }
    WireProfile::from_name(&offered[0]).map_or("unknown", WireProfile::name)
}

fn connect_error_outcome(error: &quinn::ConnectionError) -> &'static str {
    const NO_APPLICATION_PROTOCOL: u8 = 120;

    match error {
        quinn::ConnectionError::TransportError(error)
            if error.code == quinn::TransportErrorCode::crypto(NO_APPLICATION_PROTOCOL) =>
        {
            "mismatch"
        }
        _ => "connect_error",
    }
}

#[derive(Parser, Clone)]
pub struct Args {
    /// Listen for UDP packets on the given address.
    ///
    /// Defaults to [::]:0 (IPv6 with dual-stack). If the default IPv6 bind
    /// fails, automatically falls back to 0.0.0.0 (IPv4-only) with a warning.
    /// Explicitly provided IPv6 addresses will not fall back.
    #[arg(long, default_value = Args::DEFAULT_BIND)]
    pub bind: net::SocketAddr,

    /// Directory to write qlog files (one per connection)
    #[arg(long)]
    pub qlog_dir: Option<PathBuf>,

    #[command(flatten)]
    pub tls: tls::Args,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            bind: Self::DEFAULT_BIND.parse().unwrap(),
            qlog_dir: None,
            tls: Default::default(),
        }
    }
}

impl Args {
    /// The default bind address used when `--bind` is not explicitly provided.
    const DEFAULT_BIND: &str = "[::]:0";

    pub fn load(&self) -> anyhow::Result<Config> {
        let tls = self.tls.load()?;

        match Config::new(self.bind, self.qlog_dir.clone(), tls.clone()) {
            Ok(config) => Ok(config),
            Err(e) if self.bind.to_string() == Self::DEFAULT_BIND => {
                // IPv6 default bind failed -- try falling back to IPv4.
                // Only do this for the default; if the user explicitly
                // requested an IPv6 address, respect that and propagate
                // the error.
                let fallback = net::SocketAddr::new(
                    net::IpAddr::V4(net::Ipv4Addr::UNSPECIFIED),
                    self.bind.port(),
                );
                tracing::warn!(
                    requested = %self.bind,
                    fallback = %fallback,
                    error = %e,
                    "IPv6 bind failed, falling back to IPv4"
                );
                Config::new(fallback, self.qlog_dir.clone(), tls).with_context(|| {
                    format!("IPv4 fallback also failed (original IPv6 error: {})", e)
                })
            }
            Err(e) => Err(e),
        }
    }
}

/// A hook to wrap the endpoint's [`quinn::AsyncUdpSocket`] before it is handed
/// to quinn.
///
/// This lets callers interpose custom socket behavior — for example,
/// byte-counting for metrics — without this crate depending on the caller's
/// instrumentation. The closure receives the runtime-wrapped socket and
/// returns the socket quinn should actually use (typically the input wrapped
/// in a decorator).
///
/// Construct one via [`Config::with_socket_wrapper`], which accepts any
/// matching closure; this boxed alias is the stored form. `Box` (rather than
/// `Arc`) is sufficient because `Config` is not `Clone` and the wrapper is
/// invoked exactly once, during [`Endpoint::new`].
pub type SocketWrapperFn = Box<
    dyn Fn(Arc<dyn quinn::AsyncUdpSocket>) -> Arc<dyn quinn::AsyncUdpSocket>
        + Send
        + Sync
        + 'static,
>;

pub struct Config {
    pub bind: Option<net::SocketAddr>,
    pub socket: net::UdpSocket,
    pub is_dual_stack: bool,
    pub qlog_dir: Option<PathBuf>,
    pub tls: tls::Config,
    pub tags: HashSet<String>,
    /// Wire profiles accepted by the endpoint, in server preference order.
    pub wire_profiles: Vec<WireProfile>,
    /// Optional hook to wrap the [`quinn::AsyncUdpSocket`] before endpoint
    /// creation. Defaults to `None` (no wrapping). See [`SocketWrapperFn`].
    pub socket_wrapper: Option<SocketWrapperFn>,
}

impl Config {
    pub fn new(
        bind: net::SocketAddr,
        qlog_dir: Option<PathBuf>,
        tls: tls::Config,
    ) -> anyhow::Result<Self> {
        let (socket, is_dual_stack) = bind_smart(bind)?;
        Ok(Self {
            bind: Some(bind),
            socket,
            is_dual_stack,
            qlog_dir,
            tls,
            tags: HashSet::new(),
            wire_profiles: vec![WireProfile::Draft16],
            socket_wrapper: None,
        })
    }

    pub fn with_socket(
        socket: net::UdpSocket,
        qlog_dir: Option<PathBuf>,
        tls: tls::Config,
    ) -> Self {
        // Probe the socket to detect dual-stack capability rather than assuming.
        let is_dual_stack = socket.local_addr().is_ok_and(|addr| {
            addr.is_ipv6() && {
                let sock_ref = socket2::SockRef::from(&socket);
                sock_ref.only_v6().map(|v6only| !v6only).unwrap_or(false)
            }
        });

        Self {
            bind: None,
            socket,
            is_dual_stack,
            qlog_dir,
            tls,
            tags: HashSet::new(),
            wire_profiles: vec![WireProfile::Draft16],
            socket_wrapper: None,
        }
    }

    pub fn with_tag(mut self, tag: String) -> Self {
        self.tags.insert(tag);
        self
    }

    pub fn with_wire_profiles(mut self, profiles: impl IntoIterator<Item = WireProfile>) -> Self {
        self.wire_profiles.clear();
        for profile in profiles {
            if !self.wire_profiles.contains(&profile) {
                self.wire_profiles.push(profile);
            }
        }
        self
    }

    /// Attach a closure that wraps the endpoint's [`quinn::AsyncUdpSocket`]
    /// before it is handed to quinn. See [`SocketWrapperFn`].
    pub fn with_socket_wrapper<F>(mut self, wrapper: F) -> Self
    where
        F: Fn(Arc<dyn quinn::AsyncUdpSocket>) -> Arc<dyn quinn::AsyncUdpSocket>
            + Send
            + Sync
            + 'static,
    {
        self.socket_wrapper = Some(Box::new(wrapper));
        self
    }
}

pub struct Endpoint {
    pub client: Client,
    pub server: Option<Server>,
    /// Tags associated with this endpoint
    /// These are used to filter endpoints for different purposes, for eg-
    /// "server" tag is used to filter endpoints for relay server
    /// "forward" tag is used to filter endpoints for forwarder
    /// This is upto the user to define and use
    pub tags: HashSet<String>,
}

impl Endpoint {
    pub fn new(config: Config) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !config.wire_profiles.is_empty(),
            "at least one MoQT wire profile must be configured"
        );
        // Validate qlog directory if provided

        if let Some(qlog_dir) = &config.qlog_dir {
            if !qlog_dir.exists() {
                anyhow::bail!("qlog directory does not exist: {}", qlog_dir.display());
            }
            if !qlog_dir.is_dir() {
                anyhow::bail!("qlog path is not a directory: {}", qlog_dir.display());
            }
            tracing::info!("qlog output enabled: {}", qlog_dir.display());
        }

        // Build transport config with our standard settings
        let transport = Arc::new(build_transport_config());
        let wire_profiles = config.wire_profiles.clone();

        let mut server_config = None;

        if let Some(mut config) = config.tls.server {
            config.alpn_protocols = std::iter::once(web_transport_quinn::ALPN.as_bytes().to_vec())
                .chain(wire_profiles.iter().map(|profile| profile.alpn().to_vec()))
                .collect();
            config.key_log = Arc::new(rustls::KeyLogFile::new());

            let config: quinn::crypto::rustls::QuicServerConfig = config.try_into()?;
            let mut config = quinn::ServerConfig::with_crypto(Arc::new(config));
            config.transport_config(transport.clone());

            server_config = Some(config);
        }

        // There's a bit more boilerplate to make a generic endpoint.
        let runtime = quinn::default_runtime().context("no async runtime")?;
        let endpoint_config = quinn::EndpointConfig::default();
        let socket = config.socket;

        // Create the generic QUIC endpoint. When a socket wrapper is configured,
        // wrap the std socket into quinn's AsyncUdpSocket and let the caller
        // interpose (e.g. for byte-counting metrics) before quinn takes it.
        let quic = match config.socket_wrapper {
            Some(wrap) => {
                let async_socket = runtime
                    .wrap_udp_socket(socket)
                    .context("failed to wrap UDP socket")?;
                let wrapped = wrap(async_socket);
                quinn::Endpoint::new_with_abstract_socket(
                    endpoint_config,
                    server_config.clone(),
                    wrapped,
                    runtime,
                )
                .context("failed to create QUIC endpoint")?
            }
            None => quinn::Endpoint::new(endpoint_config, server_config.clone(), socket, runtime)
                .context("failed to create QUIC endpoint")?,
        };

        let server = server_config.map(|base_server_config| Server {
            quic: quic.clone(),
            accept: Default::default(),
            qlog_dir: config.qlog_dir.map(Arc::new),
            base_server_config: Arc::new(base_server_config),
            wire_profiles: wire_profiles.clone(),
        });

        let client = Client {
            quic,
            config: config.tls.client,
            transport,
            is_dual_stack: config.is_dual_stack,
            wire_profiles,
        };

        Ok(Self {
            client,
            server,
            tags: config.tags,
        })
    }
}

pub struct Server {
    quic: quinn::Endpoint,
    accept: FuturesUnordered<AcceptFuture>,
    qlog_dir: Option<Arc<PathBuf>>,
    base_server_config: Arc<quinn::ServerConfig>,
    wire_profiles: Vec<WireProfile>,
}

impl Server {
    pub async fn accept(&mut self) -> Option<AcceptedSession> {
        loop {
            tokio::select! {
                res = self.quic.accept() => {
                    let conn = res?;
                    let qlog_dir = self.qlog_dir.clone();
                    let base_server_config = self.base_server_config.clone();
                    let wire_profiles = self.wire_profiles.clone();
                    self.accept.push(Self::accept_session(conn, qlog_dir, base_server_config, wire_profiles).boxed());
                },
                res = self.accept.next(), if !self.accept.is_empty() => {
                    match res? {
                        Ok(result) => return Some(result),
                        Err(err) => {
                            tracing::warn!("failed to accept QUIC connection: {}", err.root_cause());
                            continue;
                        }
                    }
                }
            }
        }
    }

    async fn accept_session(
        conn: quinn::Incoming,
        qlog_dir: Option<Arc<PathBuf>>,
        base_server_config: Arc<quinn::ServerConfig>,
        wire_profiles: Vec<WireProfile>,
    ) -> anyhow::Result<AcceptedSession> {
        // Capture the original destination connection ID BEFORE accepting
        // This is the actual QUIC CID that can be used for qlog/mlog correlation
        let orig_dst_cid = conn.orig_dst_cid();
        let connection_id_hex = orig_dst_cid.to_string();

        // Configure per-connection qlog if enabled
        let mut conn = if let Some(qlog_dir) = qlog_dir {
            // Create qlog file path using connection ID
            let qlog_path = qlog_dir.join(format!("{}_server.qlog", connection_id_hex));

            // Create transport config with our standard settings plus qlog
            let mut transport = build_transport_config();

            let file = File::create(&qlog_path).context("failed to create qlog file")?;
            let writer = BufWriter::new(file);

            let mut qlog = quinn::QlogConfig::default();
            qlog.writer(Box::new(writer))
                .title(Some("moq-relay".into()));
            transport.qlog_stream(qlog.into_stream());

            // Create custom server config with qlog-enabled transport
            let mut server_config = (*base_server_config).clone();
            server_config.transport_config(Arc::new(transport));

            tracing::debug!(
                "qlog enabled: cid={} path={}",
                connection_id_hex,
                qlog_path.display()
            );

            // Accept with custom config
            conn.accept_with(Arc::new(server_config))?
        } else {
            // No qlog - use default config
            conn.accept()?
        };

        let handshake = conn
            .handshake_data()
            .await?
            .downcast::<quinn::crypto::rustls::HandshakeData>()
            .unwrap();

        let alpn = handshake.protocol.context("missing ALPN")?;
        let alpn_display = String::from_utf8_lossy(&alpn);
        let server_name = handshake.server_name.unwrap_or_default();

        tracing::debug!(
            "received QUIC handshake: cid={} ip={} alpn={} server={}",
            connection_id_hex,
            conn.remote_address(),
            alpn_display,
            server_name,
        );

        // Wait for the QUIC connection to be established.
        let conn = conn.await.context("failed to establish QUIC connection")?;

        tracing::debug!(
            "established QUIC connection: cid={} stable_id={} ip={} alpn={} server={}",
            connection_id_hex,
            conn.stable_id(),
            conn.remote_address(),
            alpn_display,
            server_name,
        );

        let (session, transport, selected_version) = if alpn == web_transport_quinn::ALPN.as_bytes()
        {
            // Wait for the WebTransport CONNECT request (includes H3 SETTINGS exchange).
            let request = web_transport_quinn::Request::accept(conn)
                .await
                .context("failed to receive WebTransport request")?;

            let selected_version = select_wire_profile(&request.protocols, &wire_profiles);
            let Some(selected_version) = selected_version else {
                let offered = request.protocols.join(",");
                let supported = wire_profiles
                    .iter()
                    .map(|profile| profile.name())
                    .collect::<Vec<_>>()
                    .join(",");
                metrics::counter!(
                    "moq_negotiation_total",
                    "transport" => "webtransport",
                    "outcome" => "mismatch",
                    "offered" => offered_profiles_label(&request.protocols),
                    "supported" => supported_profiles_label(&wire_profiles),
                    "required" => required_profile_label(&request.protocols),
                )
                .increment(1);
                request
                    .reject(web_transport_quinn::http::StatusCode::BAD_REQUEST)
                    .await
                    .context("failed to reject WebTransport protocol mismatch")?;
                anyhow::bail!(
                    "WebTransport protocol mismatch: offered=[{}] supported=[{}]",
                    offered,
                    supported
                );
            };

            // Accept the CONNECT request.
            let session = request
                .respond(
                    web_transport_quinn::proto::ConnectResponse::OK
                        .with_protocol(selected_version.name()),
                )
                .await
                .context("failed to respond to WebTransport request")?;
            (session, Transport::WebTransport, selected_version)
        } else if let Some(selected_version) = WireProfile::from_alpn(&alpn) {
            // Raw QUIC mode — create a "fake" WebTransport session with no H3 framing.
            let request = url::Url::parse("moqt://localhost").unwrap();
            let session = web_transport_quinn::Session::raw(
                conn,
                request,
                web_transport_quinn::proto::ConnectResponse::default(),
            );
            (session, Transport::RawQuic, selected_version)
        } else {
            anyhow::bail!("unsupported ALPN: {}", alpn_display)
        };

        Ok((
            session.into(),
            connection_id_hex,
            transport,
            selected_version,
        ))
    }

    pub fn local_addr(&self) -> anyhow::Result<net::SocketAddr> {
        self.quic
            .local_addr()
            .context("failed to get local address")
    }
}

#[derive(Clone)]
pub struct Client {
    quic: quinn::Endpoint,
    config: rustls::ClientConfig,
    transport: Arc<quinn::TransportConfig>,
    is_dual_stack: bool,
    wire_profiles: Vec<WireProfile>,
}

impl Client {
    /// Returns the local address of the QUIC socket.
    pub fn local_addr(&self) -> anyhow::Result<net::SocketAddr> {
        self.quic
            .local_addr()
            .context("failed to get local address")
    }

    /// Returns the address family of the local QUIC socket.
    ///
    /// Uses the dual-stack state determined at bind time rather than
    /// compile-time platform assumptions.
    pub fn address_family(&self) -> anyhow::Result<AddressFamily> {
        let local_addr = self
            .quic
            .local_addr()
            .context("failed to get local socket address")?;

        if local_addr.is_ipv4() {
            Ok(AddressFamily::Ipv4)
        } else if self.is_dual_stack {
            Ok(AddressFamily::Ipv6DualStack)
        } else {
            Ok(AddressFamily::Ipv6)
        }
    }

    pub async fn connect(
        &self,
        url: &Url,
        socket_addr: Option<net::SocketAddr>,
    ) -> anyhow::Result<(web_transport::Session, String, Transport, WireProfile)> {
        self.connect_with_profile(url, socket_addr, WireProfile::Draft16)
            .await
    }

    pub async fn connect_with_profile(
        &self,
        url: &Url,
        socket_addr: Option<net::SocketAddr>,
        required: WireProfile,
    ) -> anyhow::Result<(web_transport::Session, String, Transport, WireProfile)> {
        anyhow::ensure!(
            self.wire_profiles.contains(&required),
            "required MoQT profile {} is not enabled; supported=[{}]",
            required,
            self.wire_profiles
                .iter()
                .map(|profile| profile.name())
                .collect::<Vec<_>>()
                .join(",")
        );
        let mut config = self.config.clone();

        // TODO support connecting to both ALPNs at the same time
        config.alpn_protocols = vec![match url.scheme() {
            "https" => web_transport_quinn::ALPN.as_bytes().to_vec(),
            "moqt" => required.alpn().to_vec(),
            _ => anyhow::bail!("url scheme must be 'https' or 'moqt'"),
        }];

        config.key_log = Arc::new(rustls::KeyLogFile::new());

        let config: quinn::crypto::rustls::QuicClientConfig = config.try_into()?;
        let mut config = quinn::ClientConfig::new(Arc::new(config));
        config.transport_config(self.transport.clone());

        // Capture the initial destination CID that will be sent to the server
        // This is the CID used for qlog/mlog correlation on the server side
        let cid_capture: Arc<Mutex<Option<quinn::ConnectionId>>> = Arc::new(Mutex::new(None));
        let cid_capture_clone = cid_capture.clone();
        config.initial_dst_cid_provider(Arc::new(move || {
            // Generate a random CID (Quinn's default behavior)
            use rand::Rng;
            let mut rng = rand::thread_rng();
            let random_bytes: [u8; 16] = rng.gen();
            let cid = quinn::ConnectionId::new(&random_bytes);
            *cid_capture_clone.lock().unwrap() = Some(cid);
            cid
        }));

        let host = match url.host().context("missing host")? {
            url::Host::Domain(d) => d.to_string(),
            url::Host::Ipv4(ip) => ip.to_string(),
            url::Host::Ipv6(ip) => ip.to_string(), // No brackets
        };
        let port = url.port().unwrap_or(443);

        // Look up the DNS entry and filter by socket address family.
        let addr = match socket_addr {
            Some(addr) => addr,
            None => {
                // Default DNS resolution logic
                self.resolve_dns(&host, port, self.address_family()?)
                    .await?
            }
        };

        let connection = match self.quic.connect_with(config, addr, &host)?.await {
            Ok(connection) => connection,
            Err(error) => {
                metrics::counter!(
                    "moq_negotiation_total",
                    "transport" => match url.scheme() {
                        "https" => "webtransport",
                        "moqt" => "raw_quic",
                        _ => "unknown",
                    },
                    "outcome" => connect_error_outcome(&error),
                    "offered" => required.name(),
                    "supported" => supported_profiles_label(&self.wire_profiles),
                    "required" => required.name(),
                )
                .increment(1);
                return Err(error.into());
            }
        };

        // Extract the CID that was used
        let connection_id_hex = cid_capture
            .lock()
            .unwrap()
            .as_ref()
            .context("CID not captured")?
            .to_string();

        let (session, transport, selected_version) = match url.scheme() {
            "https" => {
                let request = web_transport_quinn::proto::ConnectRequest::new(url.clone())
                    .with_protocol(required.name());
                let session = web_transport_quinn::Session::connect(connection, request).await?;
                let selected_version =
                    validate_selected_profile(required, session.response().protocol.as_deref())?;
                (session, Transport::WebTransport, selected_version)
            }
            "moqt" => {
                let handshake = connection
                    .handshake_data()
                    .context("missing QUIC handshake data")?
                    .downcast::<quinn::crypto::rustls::HandshakeData>()
                    .map_err(|_| anyhow::anyhow!("invalid QUIC handshake data"))?;
                let selected = handshake.protocol.context("missing ALPN")?;
                let selected_version = WireProfile::from_alpn(&selected)
                    .context("server selected an unsupported MoQT ALPN")?;
                anyhow::ensure!(
                    selected_version == required,
                    "native QUIC protocol mismatch: required={} selected={}",
                    required,
                    selected_version
                );
                (
                    web_transport_quinn::Session::raw(
                        connection,
                        url.clone(),
                        web_transport_quinn::proto::ConnectResponse::default(),
                    ),
                    Transport::RawQuic,
                    selected_version,
                )
            }
            _ => unreachable!(),
        };

        metrics::counter!(
            "moq_negotiation_total",
            "transport" => match transport {
                Transport::WebTransport => "webtransport",
                Transport::RawQuic => "raw_quic",
            },
            "outcome" => "selected",
            "offered" => required.name(),
            "supported" => supported_profiles_label(&self.wire_profiles),
            "required" => required.name(),
        )
        .increment(1);

        Ok((
            session.into(),
            connection_id_hex,
            transport,
            selected_version,
        ))
    }

    /// Default DNS resolution logic that filters results by address family.
    async fn resolve_dns(
        &self,
        host: &str,
        port: u16,
        address_family: AddressFamily,
    ) -> anyhow::Result<net::SocketAddr> {
        let local_addr = self.local_addr()?;

        // Collect all DNS results
        let addrs: Vec<net::SocketAddr> = match Self::parse_socket_addr(host, port) {
            Ok(addr) => {
                vec![addr]
            }
            Err(_) => tokio::net::lookup_host((host, port))
                .await
                .context("failed DNS lookup")?
                .collect(),
        };

        if addrs.is_empty() {
            anyhow::bail!("DNS lookup for host '{}' returned no addresses", host);
        }

        // Log all DNS results for debugging
        tracing::debug!(
            "DNS lookup for {}, family {:?}: found {} results",
            host,
            address_family,
            addrs.len()
        );
        for (i, addr) in addrs.iter().enumerate() {
            tracing::debug!(
                "  DNS[{}]: {} ({})",
                i,
                addr,
                if addr.is_ipv4() { "IPv4" } else { "IPv6" }
            );
        }

        // Filter DNS results to match our local socket's address family
        let compatible_addr = match address_family {
            AddressFamily::Ipv4 => {
                // IPv4 socket: filter to IPv4 addresses
                addrs
                    .iter()
                    .find(|a| a.is_ipv4())
                    .cloned()
                    .context(format!(
                        "No IPv4 address found for host '{}' (local socket is IPv4: {})",
                        host, local_addr
                    ))?
            }
            AddressFamily::Ipv6DualStack => {
                // Dual-stack socket: any address family works, use first result
                tracing::debug!("Using first DNS result (IPv6 dual-stack): {}", addrs[0]);
                addrs[0]
            }
            AddressFamily::Ipv6 => {
                // IPv6-only socket: filter to IPv6 addresses
                addrs
                    .iter()
                    .find(|a| a.is_ipv6())
                    .cloned()
                    .context(format!(
                        "No IPv6 address found for host '{}' (local socket is IPv6: {})",
                        host, local_addr
                    ))?
            }
        };

        tracing::debug!(
            "Connecting from {} to {} (selected from {} DNS results)",
            local_addr,
            compatible_addr,
            addrs.len()
        );

        Ok(compatible_addr)
    }

    fn parse_socket_addr(host: &str, port: u16) -> Result<net::SocketAddr, net::AddrParseError> {
        let host = format!("{}:{}", host, port);
        host.parse::<net::SocketAddr>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use std::sync::atomic::{AtomicBool, Ordering};

    fn tls_config() -> tls::Config {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert = CertificateDer::from(certified.cert.der().to_vec());
        let key =
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der()));
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let server = rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .unwrap();
        let mut config = tls::Args {
            disable_verify: true,
            ..Default::default()
        }
        .load()
        .unwrap();
        config.server = Some(server);
        config
    }

    fn endpoint(profiles: &[WireProfile]) -> Endpoint {
        Endpoint::new(
            Config::new("127.0.0.1:0".parse().unwrap(), None, tls_config())
                .unwrap()
                .with_wire_profiles(profiles.iter().copied()),
        )
        .unwrap()
    }

    async fn negotiate(
        scheme: &str,
        server_profiles: &[WireProfile],
        required: WireProfile,
    ) -> (
        anyhow::Result<(web_transport::Session, String, Transport, WireProfile)>,
        tokio::task::JoinHandle<Option<(web_transport::Session, String, Transport, WireProfile)>>,
    ) {
        let mut server = endpoint(server_profiles).server.unwrap();
        let addr = server.local_addr().unwrap();
        let client = endpoint(&[required]).client;
        let accept = tokio::spawn(async move { server.accept().await });
        let url = Url::parse(&format!("{scheme}://localhost:{}/", addr.port())).unwrap();
        let connected = client
            .connect_with_profile(&url, Some(addr), required)
            .await;
        (connected, accept)
    }

    #[tokio::test]
    async fn production_shred_datagram_fits_initial_quic_mtu() {
        const ENCODED_SHRED_LEN: usize = 1_234;

        let (connected, accept) =
            negotiate("moqt", &[WireProfile::Draft19], WireProfile::Draft19).await;
        let (client, ..) = connected.expect("client connects");
        let (server, ..) = accept.await.unwrap().expect("server accepts");
        let payload = vec![0x5a; ENCODED_SHRED_LEN];

        assert!(client.max_datagram_size().await >= ENCODED_SHRED_LEN);
        client
            .send_datagram(payload.clone().into())
            .await
            .expect("production-sized shred datagram sends");
        assert_eq!(server.recv_datagram().await.unwrap(), payload);
    }

    /// Installing a pass-through socket wrapper must still produce a working
    /// endpoint. Exercises the `socket_wrapper` branch of `Endpoint::new`,
    /// including `wrap_udp_socket` and `new_with_abstract_socket`, and verifies
    /// the wrapper closure is actually invoked.
    #[tokio::test]
    async fn socket_wrapper_passthrough_builds_endpoint() {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind UDP socket");

        // Client-only TLS config: no cert/key, so `tls.server` is `None` and
        // `Endpoint::new` builds a client without needing certificates.
        let tls = tls::Args {
            disable_verify: true,
            ..Default::default()
        }
        .load()
        .expect("load client TLS config");

        let called = Arc::new(AtomicBool::new(false));
        let called_in_wrapper = called.clone();
        let config = Config::with_socket(socket, None, tls).with_socket_wrapper(move |inner| {
            called_in_wrapper.store(true, Ordering::SeqCst);
            inner // pass through unchanged
        });

        let endpoint = Endpoint::new(config).expect("endpoint builds with wrapper");

        assert!(
            called.load(Ordering::SeqCst),
            "the socket wrapper closure should have been invoked"
        );
        assert!(
            endpoint.server.is_none(),
            "client-only TLS config should yield no server"
        );
    }

    #[test]
    fn webtransport_requires_exact_selected_protocol() {
        assert_eq!(
            validate_selected_profile(WireProfile::Draft19, Some("moqt-19")).unwrap(),
            WireProfile::Draft19
        );
        assert!(validate_selected_profile(WireProfile::Draft19, Some("moqt-16")).is_err());
        assert!(validate_selected_profile(WireProfile::Draft19, None).is_err());
    }

    #[test]
    fn webtransport_selects_only_an_exact_common_protocol() {
        let offered = vec!["moqt-19-preview".to_string(), "moqt-16".to_string()];
        assert_eq!(
            select_wire_profile(&offered, &[WireProfile::Draft19, WireProfile::Draft16]),
            Some(WireProfile::Draft16)
        );
        assert_eq!(
            select_wire_profile(&["moqt-19-preview".to_string()], &[WireProfile::Draft19]),
            None
        );
    }

    #[test]
    fn wire_profiles_are_unique_in_preference_order() {
        let config = Config::new("127.0.0.1:0".parse().unwrap(), None, tls_config())
            .unwrap()
            .with_wire_profiles([
                WireProfile::Draft19,
                WireProfile::Draft16,
                WireProfile::Draft19,
            ]);

        assert_eq!(
            config.wire_profiles,
            vec![WireProfile::Draft19, WireProfile::Draft16]
        );
    }

    #[test]
    fn only_no_application_protocol_is_a_negotiation_mismatch() {
        let mismatch =
            quinn::ConnectionError::TransportError(quinn::TransportErrorCode::crypto(120).into());

        assert_eq!(connect_error_outcome(&mismatch), "mismatch");
        assert_eq!(
            connect_error_outcome(&quinn::ConnectionError::TimedOut),
            "connect_error"
        );
    }

    #[tokio::test]
    async fn native_quic_selects_exact_moqt_19() {
        let (client, server) =
            negotiate("moqt", &[WireProfile::Draft19], WireProfile::Draft19).await;
        let (_, _, transport, selected) = client.unwrap();
        assert_eq!(transport, Transport::RawQuic);
        assert_eq!(selected, WireProfile::Draft19);
        let (_, _, transport, selected) = server.await.unwrap().unwrap();
        assert_eq!(transport, Transport::RawQuic);
        assert_eq!(selected, WireProfile::Draft19);
    }

    #[tokio::test]
    async fn native_quic_rejects_moqt_19_to_moqt_16() {
        let (client, server) =
            negotiate("moqt", &[WireProfile::Draft16], WireProfile::Draft19).await;
        let error = match client {
            Ok(_) => panic!("mismatched ALPN unexpectedly connected"),
            Err(error) => error.to_string().to_ascii_lowercase(),
        };
        assert!(
            error.contains("application protocol") || error.contains("peer doesn't support"),
            "unexpected TLS error: {error}"
        );
        server.abort();
    }

    #[tokio::test]
    async fn webtransport_selects_and_echoes_exact_moqt_19() {
        let (client, server) =
            negotiate("https", &[WireProfile::Draft19], WireProfile::Draft19).await;
        let (session, _, transport, selected) = client.unwrap();
        assert_eq!(session.protocol(), Some("moqt-19"));
        assert_eq!(transport, Transport::WebTransport);
        assert_eq!(selected, WireProfile::Draft19);
        let (session, _, transport, selected) = server.await.unwrap().unwrap();
        assert_eq!(session.protocol(), Some("moqt-19"));
        assert_eq!(transport, Transport::WebTransport);
        assert_eq!(selected, WireProfile::Draft19);
    }

    #[tokio::test]
    async fn webtransport_rejects_without_an_exact_common_protocol() {
        let (client, server) =
            negotiate("https", &[WireProfile::Draft16], WireProfile::Draft19).await;
        assert!(client.is_err());
        server.abort();
    }
}
