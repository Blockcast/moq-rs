// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Coordinator data and error types, re-exported from `crate::coordinator`.

use std::net::SocketAddr;

use moq_transport::coding::TrackNamespace;
use url::Url;

use crate::session::SessionInterface;

#[cfg(doc)]
use super::Coordinator;

#[derive(Debug, thiserror::Error)]
pub enum CoordinatorError {
    #[error("namespace not found")]
    NamespaceNotFound,

    #[error("namespace already registered")]
    NamespaceAlreadyRegistered,

    #[error("Internal Error: {0}")]
    Other(anyhow::Error),
}

impl From<anyhow::Error> for CoordinatorError {
    fn from(err: anyhow::Error) -> Self {
        Self::Other(err)
    }
}

impl From<tokio::task::JoinError> for CoordinatorError {
    fn from(err: tokio::task::JoinError) -> Self {
        Self::Other(err.into())
    }
}

impl From<std::io::Error> for CoordinatorError {
    fn from(err: std::io::Error) -> Self {
        Self::Other(err.into())
    }
}

pub type CoordinatorResult<T> = std::result::Result<T, CoordinatorError>;

/// Handle returned when a namespace is registered with the coordinator.
///
/// Dropping this handle automatically unregisters the namespace.
/// This provides RAII-based cleanup - when the publisher disconnects
/// or the namespace is no longer served, cleanup happens automatically.
pub struct NamespaceRegistration {
    _inner: Box<dyn Send + Sync>,
    _metadata: Option<Vec<(String, String)>>,
}

impl NamespaceRegistration {
    /// Create a new registration handle wrapping any Send + Sync type.
    ///
    /// The wrapped value's `Drop` implementation will be called when
    /// this registration is dropped.
    pub fn new<T: Send + Sync + 'static>(inner: T) -> Self {
        Self {
            _inner: Box::new(inner),
            _metadata: None,
        }
    }

    /// Add metadata as list of key value pair of string: string
    pub fn with_metadata(mut self, metadata: Vec<(String, String)>) -> Self {
        self._metadata = Some(metadata);
        self
    }
}

/// Result of a namespace lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceOrigin {
    /// The namespace of the track belongs to
    namespace: TrackNamespace,
    /// The URL of the relay serving this namespace
    /// If the relay is not discoverable via this URL, use `socket_addr`
    /// But you still have to pass a valid URL because the TLS verification
    /// happens for hostname
    url: Url,
    /// The socket address of the relay if the relay is not approachable
    /// via DNS lookup, This is to bypass DNS lookups.
    socket_addr: Option<SocketAddr>,
    /// Additional metadata associated with this namespace
    metadata: Option<Vec<(String, String)>>,
}

impl NamespaceOrigin {
    /// Create a new NamespaceOrigin.
    pub fn new(namespace: TrackNamespace, url: Url, addr: Option<SocketAddr>) -> Self {
        Self {
            namespace,
            url,
            socket_addr: addr,
            metadata: None,
        }
    }
    pub fn with_metadata(mut self, values: (String, String)) -> Self {
        if let Some(metadata) = &mut self.metadata {
            metadata.push(values);
        } else {
            self.metadata = Some(vec![values]);
        }
        self
    }

    /// Get the namespace.
    pub fn namespace(&self) -> &TrackNamespace {
        &self.namespace
    }

    /// Get the URL of the relay serving this namespace.
    pub fn url(&self) -> Url {
        self.url.clone()
    }

    pub fn addr(&self) -> Option<SocketAddr> {
        self.socket_addr
    }

    /// Get the metadata associated with this namespace.
    pub fn metadata(&self) -> Option<Vec<(String, String)>> {
        self.metadata.clone()
    }
}

/// Information about the resolved scope for a connection.
///
/// Returned by [`Coordinator::resolve_scope()`] to tell the relay:
/// - Which scope this connection belongs to (for routing and namespace isolation)
/// - What the connection is allowed to do (for permission enforcement)
///
/// Multiple connection paths can map to the same `scope_id` — for example,
/// a publisher path and a subscriber path that share a scope but have
/// different permissions.
#[derive(Debug, Clone)]
pub struct ScopeInfo {
    /// The resolved scope identity. Used as the key for namespace
    /// registration and lookup in all subsequent coordinator operations.
    ///
    /// Multiple connection paths can map to the same `scope_id`.
    pub scope_id: String,

    /// What this connection is allowed to do within the scope.
    pub permissions: ScopePermissions,
}

/// Permissions granted to a connection within its scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopePermissions {
    /// Can both publish (PUBLISH_NAMESPACE) and subscribe (SUBSCRIBE/FETCH).
    ReadWrite,
    /// Can subscribe/fetch only. Publishing attempts will be rejected
    /// by the relay (the Consumer side of the session will not be created).
    ReadOnly,
}

impl ScopePermissions {
    /// Whether this permission level allows publishing (PUBLISH_NAMESPACE).
    pub fn can_publish(&self) -> bool {
        matches!(self, Self::ReadWrite)
    }

    /// Whether this permission level allows subscribing (SUBSCRIBE/FETCH).
    ///
    /// Always returns `true` — both `ReadWrite` and `ReadOnly` connections
    /// can subscribe. This is intentional: the asymmetry with [`can_publish()`]
    /// reflects that subscribing is the baseline capability, while publishing
    /// requires elevated permissions. If a future permission level needs to
    /// deny subscribing, a new variant should be added.
    ///
    /// [`can_publish()`]: ScopePermissions::can_publish
    pub fn can_subscribe(&self) -> bool {
        true
    }
}

// ============================================================================
// Types for extended Coordinator functionality
// ============================================================================

/// Per-scope configuration retrieved from the coordinator.
///
/// Called after [`Coordinator::resolve_scope()`] to get operational parameters
/// for the scope. This configuration applies to all sessions within the scope.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ScopeConfig {
    /// Origin server to fall back to when namespace not found locally or on
    /// other relays. The relay will attempt to subscribe from this origin
    /// before returning "not found" to the subscriber.
    pub origin_fallback: Option<Url>,

    /// Whether to pre-register subscriber interest for tracks that don't exist
    /// yet. When true, enables "subscriber-first" workflows where subscribers
    /// can wait for publishers that haven't connected yet.
    ///
    /// This corresponds to the "rendezvous" concept in the MoQT specification
    /// (`RENDEZVOUS_TIMEOUT` parameter, see moq-transport PR #1447). The
    /// "lingering subscribe" terminology from moq-transport issue #1402 is
    /// used here for consistency with existing implementations.
    ///
    /// Future: A `rendezvous_timeout` field may be added to control how long
    /// the relay waits for a publisher before giving up.
    pub lingering_subscribe: bool,
}

/// Result of subscribing to a namespace prefix via SUBSCRIBE_NAMESPACE.
///
/// The subscription remains active until this handle is dropped.
/// On drop, cleanup is performed (e.g., unregistering from the coordinator).
pub struct NamespaceSubscription {
    /// Peer relays that currently host namespaces matching the subscribed
    /// prefix. The subscribing relay opens upstream SUBSCRIBE_NAMESPACE
    /// sessions to these to receive live NAMESPACE / NAMESPACE_DONE updates.
    /// The coordinator excludes the caller and the inbound source peer, so
    /// every entry is a distinct upstream that is safe to pull from.
    ///
    /// The matching namespaces themselves are not returned here: they are
    /// delivered live over the upstream pull sessions (cross-relay) and served
    /// from relay-local state (same-relay), so a static snapshot would be both
    /// redundant and unable to reflect later withdrawals.
    pub upstream_relays: Vec<RelayInfo>,

    /// RAII handle — drop triggers unsubscription cleanup.
    _registration: Box<dyn Send + Sync>,
}

impl Default for NamespaceSubscription {
    fn default() -> Self {
        Self {
            upstream_relays: vec![],
            _registration: Box::new(()),
        }
    }
}

impl NamespaceSubscription {
    /// Create a new subscription with upstream relays and a cleanup handle.
    pub fn new<T: Send + Sync + 'static>(upstream_relays: Vec<RelayInfo>, inner: T) -> Self {
        Self {
            upstream_relays,
            _registration: Box::new(inner),
        }
    }
}

/// Information about a relay to forward messages to.
///
/// Returned by [`Coordinator::lookup_namespace_subscribers()`] and
/// [`Coordinator::lookup_track_subscribers()`] to tell the relay where
/// to forward PUBLISH_NAMESPACE or track availability notifications.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RelayInfo {
    /// Relay URL (used for TLS SNI and connection establishment).
    pub url: Url,

    /// Optional direct socket address (bypasses DNS resolution).
    pub addr: Option<SocketAddr>,
}

impl RelayInfo {
    /// Create a new RelayInfo with URL only.
    pub fn new(url: Url) -> Self {
        Self { url, addr: None }
    }

    /// Create a new RelayInfo with URL and direct socket address.
    pub fn with_addr(url: Url, addr: SocketAddr) -> Self {
        Self {
            url,
            addr: Some(addr),
        }
    }

    /// Create a `RelayInfo` identity from an inbound peer's socket address.
    ///
    /// Used when a relay accepts an internal connection and only knows the
    /// peer by its transport-level source address — it trusts the incoming
    /// socket address rather than asking the coordinator to resolve the
    /// peer's canonical URL. The [`url`](Self::url) is a synthetic
    /// `https://{addr}` identity; callers should treat [`addr`](Self::addr)
    /// as the authoritative field and must not assume the URL is dialable.
    pub fn from_socket_addr(addr: SocketAddr) -> Self {
        // A `SocketAddr` always renders as a valid `host:port` authority
        // (IPv6 is bracketed by its `Display`), so this parse is infallible.
        let url = Url::parse(&format!("https://{addr}"))
            .expect("socket address forms a valid https authority");
        Self {
            url,
            addr: Some(addr),
        }
    }
}

/// Per-call context describing how a coordinator operation reached this relay.
///
/// Passed alongside `scope` to routing/registration methods so a coordinator
/// can distinguish public client traffic from internal relay-to-relay traffic
/// and, for the latter, know which peer relay it came from.
///
/// The relay's own URL (the "caller") is intentionally **not** part of this
/// context: each coordinator instance is constructed knowing its own relay
/// URL, so it does not need to be told on every call.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct CoordinatorContext {
    /// Whether the originating session is public client-facing or internal
    /// relay-to-relay. Defaults to [`SessionInterface::Public`].
    pub interface: SessionInterface,

    /// The peer relay this operation was received from, for internal
    /// sessions. `None` for public client sessions.
    ///
    /// Derived by the relay from the inbound socket address (see
    /// [`RelayInfo::from_socket_addr`]); the coordinator is not asked to
    /// resolve peer identity.
    pub source: Option<RelayInfo>,
}

impl CoordinatorContext {
    /// A public, client-facing context with no peer relay.
    pub fn public() -> Self {
        Self {
            interface: SessionInterface::Public,
            source: None,
        }
    }

    /// An internal, relay-to-relay context originating from `source`.
    pub fn internal(source: Option<RelayInfo>) -> Self {
        Self {
            interface: SessionInterface::Internal,
            source,
        }
    }
}

/// Handle returned when a track is registered with the coordinator.
///
/// Dropping this handle automatically unregisters the track.
/// This provides RAII-based cleanup for track-level PUBLISH.
pub struct TrackRegistration {
    _registration: Box<dyn Send + Sync>,
}

impl Default for TrackRegistration {
    fn default() -> Self {
        Self {
            _registration: Box::new(()),
        }
    }
}

impl TrackRegistration {
    /// Create a new track registration handle wrapping any Send + Sync type.
    pub fn new<T: Send + Sync + 'static>(inner: T) -> Self {
        Self {
            _registration: Box::new(inner),
        }
    }
}

/// Information about a registered track.
///
/// Returned by [`Coordinator::list_tracks()`] to describe tracks
/// registered under a namespace.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TrackEntry {
    /// The namespace this track belongs to.
    pub namespace: TrackNamespace,

    /// The track name within the namespace.
    pub name: String,
}

impl TrackEntry {
    /// Create a new TrackEntry.
    pub fn new(namespace: TrackNamespace, name: String) -> Self {
        Self { namespace, name }
    }
}

/// Handle returned when subscribing to a track for rendezvous/lingering subscriber support.
///
/// Dropping this handle automatically unregisters the track subscription.
/// This provides RAII-based cleanup for pre-registered subscriber interest
/// (the "rendezvous" concept from MoQT's `RENDEZVOUS_TIMEOUT` parameter).
pub struct TrackSubscription {
    _registration: Box<dyn Send + Sync>,
}

impl Default for TrackSubscription {
    fn default() -> Self {
        Self {
            _registration: Box::new(()),
        }
    }
}

impl TrackSubscription {
    /// Create a new track subscription handle wrapping any Send + Sync type.
    pub fn new<T: Send + Sync + 'static>(inner: T) -> Self {
        Self {
            _registration: Box::new(inner),
        }
    }
}
