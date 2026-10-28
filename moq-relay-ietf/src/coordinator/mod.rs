// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use async_trait::async_trait;
use moq_native_ietf::quic;
use moq_transport::coding::{TrackNamespace, TrackNamespacePrefix};

mod types;

pub use types::*;

/// Coordinator handles namespace registration/discovery across relays.
///
/// Implementations are responsible for:
/// - Resolving connection paths to scopes (identity + permissions)
/// - Tracking which namespaces are served locally
/// - Caching remote namespace lookups
/// - Communicating with external registries (HTTP API, Redis, etc.)
/// - Periodic refresh/heartbeat of registrations
/// - Cleanup when registrations are dropped
///
/// # Thread Safety
///
/// All methods take `&self` and implementations must be thread-safe.
/// Multiple tasks will call these methods concurrently.
///
/// ## Scope Resolution
///
/// When a new session is accepted, the relay calls [`resolve_scope()`] with
/// the raw connection path (from WebTransport URL or CLIENT_SETUP PATH
/// parameter). The coordinator returns a [`ScopeInfo`] containing:
///
/// - **`scope_id`**: The resolved scope identity, used as the key for all
///   subsequent `register_namespace()` and `lookup()` calls. This is
///   intentionally separate from the raw connection path — multiple paths
///   can map to the same scope.
///
/// - **`permissions`**: What the connection is allowed to do. The relay
///   enforces permissions by selectively enabling the publish and/or
///   subscribe sides of the session.
///
/// If `resolve_scope()` returns `None`, the session is unscoped — all
/// subsequent operations use `scope: None` and both publish and subscribe
/// are allowed.
///
/// [`resolve_scope()`]: Coordinator::resolve_scope
#[async_trait]
pub trait Coordinator: Send + Sync {
    /// Resolve a connection path to scope information.
    ///
    /// Called once per accepted session, before any register/lookup calls.
    /// The relay uses the returned [`ScopeInfo`] to:
    /// - Scope all subsequent coordinator operations to `scope_id`
    /// - Enforce permissions (e.g., skip creating the publish side for
    ///   `ReadOnly` connections)
    ///
    /// # Arguments
    ///
    /// * `connection_path` - The raw connection path from the WebTransport
    ///   URL or CLIENT_SETUP PATH parameter. `None` if no path was present.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(ScopeInfo))` - Connection is scoped with the given
    ///   identity and permissions.
    /// - `Ok(None)` - Connection is unscoped. The relay will pass
    ///   `scope: None` to all subsequent coordinator calls and allow
    ///   both publish and subscribe.
    /// - `Err(...)` - Connection should be rejected (e.g., unrecognized
    ///   path, unauthorized).
    ///
    /// # Default Implementation
    ///
    /// Passes through the connection path as the `scope_id` with
    /// `ReadWrite` permissions. Connections without a path are unscoped.
    async fn resolve_scope(
        &self,
        connection_path: Option<&str>,
    ) -> CoordinatorResult<Option<ScopeInfo>> {
        Ok(connection_path.map(|path| ScopeInfo {
            scope_id: path.to_string(),
            permissions: ScopePermissions::ReadWrite,
        }))
    }

    /// Register a namespace as locally available on this relay.
    ///
    /// Called when a publisher sends PUBLISH_NAMESPACE.
    /// The coordinator should:
    /// 1. Record the namespace as locally available
    /// 2. Advertise to external registry if configured
    /// 3. Start any refresh/heartbeat tasks
    /// 4. Return a handle that unregisters on drop
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity from [`resolve_scope()`],
    ///   or `None` for unscoped sessions. Used to isolate namespace
    ///   registrations — the same namespace in different scopes may
    ///   route independently.
    /// * `namespace` - The namespace being registered
    /// * `context` - Whether the registering session is a public client or an
    ///   internal relay peer (and which peer). Coordinators may use this to
    ///   avoid, for example, re-advertising registrations that arrived from
    ///   another relay.
    ///
    /// # Returns
    ///
    /// A `NamespaceRegistration` handle. The namespace remains registered
    /// as long as this handle is held. Dropping it unregisters the namespace.
    ///
    /// [`resolve_scope()`]: Coordinator::resolve_scope
    async fn register_namespace(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
        context: &CoordinatorContext,
    ) -> CoordinatorResult<NamespaceRegistration>;

    /// Unregister a namespace.
    ///
    /// Called when a publisher sends PUBLISH_NAMESPACE_DONE.
    /// This is an explicit unregistration - the registration handle may still exist
    /// but the namespace should be removed from the registry.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `namespace` - The namespace to unregister
    async fn unregister_namespace(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<()>;

    /// Lookup where a namespace is served from.
    ///
    /// Called when a subscriber requests a namespace.
    /// The coordinator should check in order:
    /// 1. Local registrations (return `Local`)
    /// 2. Cached remote lookups (return `Remote(url)` if not expired)
    /// 3. External registry (cache and return result)
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped
    ///   sessions. Coordinators use this to scope lookups (e.g., to route
    ///   to the correct origin for a particular application).
    /// * `namespace` - The namespace to look up
    ///
    /// # Returns
    ///
    /// - `Ok(NamespaceOrigin, Option<quic::Client>)` - Namespace origin and optional client if available
    /// - `Err` - Namespace not found anywhere
    async fn lookup(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
    ) -> CoordinatorResult<(NamespaceOrigin, Option<quic::Client>)>;

    /// Lookup where a specific track is served from.
    ///
    /// Called when a subscriber requests a full track name. Implementations
    /// should prefer exact track registrations created by PUBLISH, then fall
    /// back to namespace routing created by PUBLISH_NAMESPACE. This keeps the
    /// common caller ergonomic while still allowing a single PUBLISH track to be
    /// routed without pretending the whole namespace was published.
    ///
    /// # Default Implementation
    ///
    /// Falls back to [`lookup`], so existing coordinators that only implement
    /// namespace-level routing retain their current behavior.
    async fn lookup_track(
        &self,
        scope: Option<&str>,
        namespace: &TrackNamespace,
        _track: &str,
    ) -> CoordinatorResult<(NamespaceOrigin, Option<quic::Client>)> {
        self.lookup(scope, namespace).await
    }

    /// Graceful shutdown of the coordinator.
    ///
    /// Called when the relay is shutting down. Implementations should:
    /// - Unregister all local namespaces and tracks
    /// - Cancel refresh tasks
    /// - Close connections to external registries
    async fn shutdown(&self) -> CoordinatorResult<()> {
        Ok(())
    }

    // ========================================================================
    // Scope configuration
    // ========================================================================

    /// Get configuration for a resolved scope.
    ///
    /// Called after [`resolve_scope()`] to retrieve operational parameters
    /// for the scope, such as origin fallback URLs and lingering subscriber
    /// settings.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity from [`resolve_scope()`],
    ///   or `None` for unscoped sessions.
    ///
    /// # Default Implementation
    ///
    /// Returns default configuration (no origin fallback, lingering subscribe
    /// disabled).
    ///
    /// [`resolve_scope()`]: Coordinator::resolve_scope
    async fn get_scope_config(&self, _scope: Option<&str>) -> CoordinatorResult<ScopeConfig> {
        Ok(ScopeConfig::default())
    }

    // ========================================================================
    // SUBSCRIBE_NAMESPACE support
    // ========================================================================

    /// Register interest in a namespace prefix (SUBSCRIBE_NAMESPACE).
    ///
    /// Called when a subscriber sends SUBSCRIBE_NAMESPACE. The coordinator
    /// should:
    /// 1. Record that this relay is interested in the prefix
    /// 2. Return the peer relays that host currently-matching namespaces (the
    ///    upstream pull targets)
    /// 3. Return an RAII handle for cleanup on disconnect
    ///
    /// The subscribing relay opens upstream SUBSCRIBE_NAMESPACE sessions to the
    /// returned [`NamespaceSubscription::upstream_relays`] to receive live
    /// NAMESPACE / NAMESPACE_DONE updates for namespaces that already exist
    /// (the publish-before-subscribe case). Separately, when publishers *later*
    /// register namespaces matching this prefix, the origin relay uses
    /// [`lookup_namespace_subscribers()`] to find interested relays and push
    /// PUBLISH_NAMESPACE to them (the subscribe-before-publish case).
    ///
    /// The subscribing relay applies **no** routing or topology policy of its
    /// own and does not interpret the session interface: it simply opens an
    /// upstream pull to each relay in `upstream_relays`, and an empty list means
    /// "serve from local state only, do not connect upstream". All routing
    /// policy therefore lives in the coordinator.
    ///
    /// To decide what to return, the coordinator MAY use the informational
    /// [`CoordinatorContext`] — in particular the session
    /// [`interface`][CoordinatorContext::interface] (public client vs. internal
    /// relay peer) and the [`source`][CoordinatorContext::source] peer. For
    /// example, the built-in implementations exclude both the caller (this
    /// relay's own endpoint) and the source peer, so a relay is never told to
    /// pull from itself or from the peer the SUBSCRIBE_NAMESPACE just arrived
    /// from; and they return an empty list for internal sessions, so a pull
    /// opened on another relay's behalf does not itself fan out upstream (which
    /// would otherwise make relays recurse — and, in a mesh, cycle). Those are
    /// policy choices of the built-in implementations, not requirements of this
    /// trait. Interest is still recorded for the push path regardless of
    /// interface.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `prefix` - The namespace prefix to subscribe to.
    /// * `context` - Whether the subscribing session is a public client or an
    ///   internal relay peer (and which peer).
    ///
    /// # Default Implementation
    ///
    /// Returns an empty subscription (no upstream relays, no-op cleanup).
    ///
    /// [`lookup_namespace_subscribers()`]: Coordinator::lookup_namespace_subscribers
    async fn subscribe_namespace(
        &self,
        _scope: Option<&str>,
        _prefix: &TrackNamespacePrefix,
        _context: &CoordinatorContext,
    ) -> CoordinatorResult<NamespaceSubscription> {
        Ok(NamespaceSubscription::default())
    }

    /// Unregister interest in a namespace prefix (UNSUBSCRIBE_NAMESPACE).
    ///
    /// Called when a subscriber sends UNSUBSCRIBE_NAMESPACE or disconnects.
    /// This is an explicit unregistration — the subscription handle may still
    /// exist but interest should be removed from the registry.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `prefix` - The namespace prefix to unsubscribe from.
    ///
    /// # Default Implementation
    ///
    /// No-op (returns success).
    async fn unsubscribe_namespace(
        &self,
        _scope: Option<&str>,
        _prefix: &TrackNamespacePrefix,
    ) -> CoordinatorResult<()> {
        Ok(())
    }

    /// Find relays interested in a namespace (reverse lookup).
    ///
    /// Called when a publisher registers a new namespace. The relay uses this
    /// to find other relays that have active SUBSCRIBE_NAMESPACE subscriptions
    /// matching this namespace, then forwards PUBLISH_NAMESPACE to them.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `namespace` - The newly-registered namespace.
    /// * `context` - Origin of the PUBLISH_NAMESPACE that triggered this lookup.
    ///   Implementations MUST use this (together with their own relay identity)
    ///   to exclude relays that would create forwarding loops:
    ///   - the **caller** (this relay's own endpoint), so a relay never forwards
    ///     PUBLISH_NAMESPACE to itself; and
    ///   - the **source** ([`CoordinatorContext::source`]), so a PUBLISH_NAMESPACE
    ///     is never echoed back to the peer relay it just arrived from.
    ///
    /// # Returns
    ///
    /// List of peer relay endpoints to forward PUBLISH_NAMESPACE to, with the
    /// caller and source excluded.
    ///
    /// # Default Implementation
    ///
    /// Returns an empty list (no subscribers).
    async fn lookup_namespace_subscribers(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
        _context: &CoordinatorContext,
    ) -> CoordinatorResult<Vec<RelayInfo>> {
        Ok(vec![])
    }

    // ========================================================================
    // Track-level PUBLISH support
    // ========================================================================

    /// Register a track as available on this relay (track-level PUBLISH).
    ///
    /// Called when a publisher sends PUBLISH for a specific track. This
    /// provides finer-grained routing than namespace-level registration.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `namespace` - The namespace the track belongs to.
    /// * `track` - The track name within the namespace.
    ///
    /// # Returns
    ///
    /// A `TrackRegistration` handle. The track remains registered as long as
    /// this handle is held. Dropping it unregisters the track.
    ///
    /// # Default Implementation
    ///
    /// Returns a no-op registration handle.
    async fn register_track(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
        _track: &str,
    ) -> CoordinatorResult<TrackRegistration> {
        Ok(TrackRegistration::default())
    }

    /// Unregister a track.
    ///
    /// Called when a publisher sends PUBLISH_DONE or disconnects.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `namespace` - The namespace the track belongs to.
    /// * `track` - The track name to unregister.
    ///
    /// # Default Implementation
    ///
    /// No-op (returns success).
    async fn unregister_track(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
        _track: &str,
    ) -> CoordinatorResult<()> {
        Ok(())
    }

    /// List tracks registered under a namespace.
    ///
    /// Used for track discovery within a namespace, supporting SUBSCRIBE_NAMESPACE
    /// workflows where subscribers need to know what tracks are available.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `namespace` - The namespace to list tracks from.
    ///
    /// # Default Implementation
    ///
    /// Returns an empty list.
    async fn list_tracks(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
    ) -> CoordinatorResult<Vec<TrackEntry>> {
        Ok(vec![])
    }

    // ========================================================================
    // Lingering subscriber / rendezvous support
    // ========================================================================
    //
    // These methods implement the "rendezvous" concept from the MoQT
    // specification (RENDEZVOUS_TIMEOUT parameter, moq-transport PR #1447),
    // also known as "lingering subscribe" (moq-transport issue #1402) or
    // "early media" (Cisco's original framing at IETF 122).
    //
    // The relay uses these to pre-register subscriber interest before a
    // publisher exists, enabling subscriber-first workflows.
    //
    // Future: timeout handling for rendezvous — how long to wait before
    // giving up on a publisher.

    /// Pre-register interest in a track that may not exist yet (rendezvous).
    ///
    /// Enables "subscriber-first" workflows where a subscriber can wait for a
    /// publisher that hasn't connected yet. Called when
    /// [`ScopeConfig::lingering_subscribe`] is true and a subscriber requests
    /// a track that doesn't exist.
    ///
    /// Also known as "lingering subscribe" (moq-transport issue #1402) or
    /// "rendezvous" (MoQT spec's `RENDEZVOUS_TIMEOUT` parameter).
    ///
    /// When a publisher later registers the track, the relay uses
    /// [`lookup_track_subscribers()`] to find waiting subscribers.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `namespace` - The namespace the track would belong to.
    /// * `track` - The track name to pre-register interest in.
    ///
    /// # Returns
    ///
    /// A `TrackSubscription` handle. Interest remains registered as long as
    /// this handle is held. Dropping it removes the interest.
    ///
    /// # Default Implementation
    ///
    /// Returns a no-op subscription handle.
    ///
    /// [`lookup_track_subscribers()`]: Coordinator::lookup_track_subscribers
    async fn subscribe_track(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
        _track: &str,
    ) -> CoordinatorResult<TrackSubscription> {
        Ok(TrackSubscription::default())
    }

    /// Unregister track subscription interest.
    ///
    /// Called when a subscriber disconnects or no longer needs the track.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `namespace` - The namespace the track belongs to.
    /// * `track` - The track name to unsubscribe from.
    ///
    /// # Default Implementation
    ///
    /// No-op (returns success).
    async fn unsubscribe_track(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
        _track: &str,
    ) -> CoordinatorResult<()> {
        Ok(())
    }

    /// Find relays with subscribers waiting for a track (reverse lookup).
    ///
    /// Called when a publisher registers a track, to notify lingering
    /// subscribers that the track is now available.
    ///
    /// # Arguments
    ///
    /// * `scope` - The resolved scope identity, or `None` for unscoped sessions.
    /// * `namespace` - The namespace the track belongs to.
    /// * `track` - The track name.
    ///
    /// # Returns
    ///
    /// List of relay endpoints with waiting subscribers.
    ///
    /// # Default Implementation
    ///
    /// Returns an empty list.
    async fn lookup_track_subscribers(
        &self,
        _scope: Option<&str>,
        _namespace: &TrackNamespace,
        _track: &str,
    ) -> CoordinatorResult<Vec<RelayInfo>> {
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests;
