// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::{hash_map, HashMap, HashSet},
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::sync::Notify;

use crate::{
    coding::{Decode, TrackName, TrackNamespace},
    data,
    message::{self, Message, RequestErrorCode},
    mlog,
    serve::{self, FullTrackName, ServeError, TrackReader},
};

use crate::watch::Queue;

use super::{
    PendingPublishUpdate, PublishedNamespace, PublishedNamespaceRecv, PublishedTrack,
    PublishedTrackRecv, Reader, RequestId, RequestIdAllocation, Session, SessionConfig,
    SessionError, Subscribe, SubscribeRecv,
};

// Default timeout for waiting for subscribe aliases to become available via SUBSCRIBE_OK (1 second)
const DEFAULT_ALIAS_WAIT_TIME_MS: u64 = 1000;

// TODO remove Clone.
#[derive(Clone)]
pub struct Subscriber {
    /// Active inbound PUBLISH_NAMESPACE messages, keyed by namespace.
    published_namespaces: Arc<Mutex<HashMap<TrackNamespace, PublishedNamespaceRecv>>>,

    /// Queue of inbound PUBLISH_NAMESPACE events waiting to be consumed by the application.
    published_namespace_queue: Queue<PublishedNamespace>,

    /// The currently active outbound subscribes, keyed by request id.
    subscribes: Arc<Mutex<HashMap<u64, SubscribeRecv>>>,

    /// Outbound TRACK_STATUS requests awaiting a shared REQUEST_OK / REQUEST_ERROR response.
    track_statuses: Arc<Mutex<HashSet<u64>>>,

    /// Map of track alias → SUBSCRIBE request id.
    /// Populated on SUBSCRIBE_OK.  Both alias maps must be collision-free:
    /// Track Aliases are session-scoped (§10.1), so SUBSCRIBE and PUBLISH aliases
    /// share the same namespace.
    subscribe_alias_map: Arc<Mutex<HashMap<u64, u64>>>,

    /// Notify when subscribe alias map is updated.
    subscribe_alias_notify: Arc<Notify>,

    /// Active inbound PUBLISH subscriptions, keyed by PUBLISH request id.
    /// The transport writes Objects into the TrackWriter stored here;
    /// the application receives the TrackReader from PublishedTrack::ok.
    published_tracks: Arc<Mutex<HashMap<u64, PublishedTrackRecv>>>,

    /// Map of track alias → PUBLISH request id.
    /// Track Aliases are session-scoped (§10.1), so both alias maps must be
    /// checked together when validating incoming alias usage.
    publish_alias_map: Arc<Mutex<HashMap<u64, u64>>>,

    /// Notify when publish alias map is updated.
    publish_alias_notify: Arc<Notify>,

    /// Queue of inbound PUBLISH events waiting for the application to accept.
    published_track_queue: Queue<PublishedTrack>,

    /// Tracks which (namespace, name) pairs this endpoint is subscribed to
    /// (as subscriber role) — covers both outbound SUBSCRIBE and inbound PUBLISH.
    /// Used for §5.1 same-role duplicate-subscription enforcement.
    subscriber_names: Arc<Mutex<HashMap<FullTrackName, u64>>>,

    /// Pending REQUEST_UPDATE messages sent by PublishedTrack::set_forward,
    /// keyed by the REQUEST_UPDATE's own request id.
    /// When REQUEST_OK / REQUEST_ERROR arrives for one of these ids, the
    /// corresponding oneshot sender is notified.
    pending_publish_updates: Arc<Mutex<HashMap<u64, PendingPublishUpdate>>>,

    /// The queue we will write any outbound control messages we want to send, the session run_send task
    /// will process the queue and send the message on the control stream.
    outgoing: Queue<Message>,

    /// Shared with Publisher so all requests within a session use unique IDs.
    /// When we need a new Request Id for sending a request, we can get it from here.
    /// The manager is shared with the Publisher, so the session uses unique request ids
    /// for all requests generated.  If we initiated the QUIC connection then request
    /// IDs start at 0 and increment by 2 (even numbers).  If we accepted an inbound
    /// QUIC connection then request IDs start at 1 and increment by 2 (odd numbers).
    request_id: RequestId,

    /// Optional mlog writer for logging transport events
    mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
}

/// Outcome of resolving a Track Alias: which subscription kind owns it.
///
/// Track Aliases are session-scoped (§10.1).  SUBSCRIBE and PUBLISH share the
/// same alias namespace, so lookup must check both maps.
enum AliasBinding {
    /// Alias belongs to an outbound SUBSCRIBE; carries the subscribe request id.
    Subscribe(u64),
    /// Alias belongs to an inbound PUBLISH; carries the PUBLISH request id.
    Publish(u64),
}

impl Subscriber {
    pub(super) fn new(
        outgoing: Queue<Message>,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
        request_id: RequestId,
    ) -> Self {
        Self {
            published_namespaces: Default::default(),
            published_namespace_queue: Default::default(),
            subscribes: Default::default(),
            track_statuses: Default::default(),
            subscribe_alias_map: Default::default(),
            subscribe_alias_notify: Arc::new(Notify::new()),
            published_tracks: Default::default(),
            publish_alias_map: Default::default(),
            publish_alias_notify: Arc::new(Notify::new()),
            published_track_queue: Default::default(),
            subscriber_names: Default::default(),
            pending_publish_updates: Default::default(),
            outgoing,
            request_id,
            mlog,
        }
    }

    /// Create an inbound/server QUIC connection, by accepting a bi-directional QUIC stream for control messages.
    pub async fn accept(
        session: web_transport::Session,
        transport: super::Transport,
    ) -> Result<(Session, Self), SessionError> {
        Self::accept_with_config(session, transport, SessionConfig::default()).await
    }

    pub async fn accept_with_config(
        session: web_transport::Session,
        transport: super::Transport,
        config: SessionConfig,
    ) -> Result<(Session, Self), SessionError> {
        let (session, _, subscriber) =
            Session::accept_with_config(session, None, transport, config).await?;
        let subscriber = subscriber.ok_or(SessionError::Internal)?;
        Ok((session, subscriber))
    }

    /// Create an outbound/client QUIC connection, by opening a bi-directional QUIC stream for control messages.
    pub async fn connect(
        session: web_transport::Session,
        transport: super::Transport,
    ) -> Result<(Session, Self), SessionError> {
        Self::connect_with_config(session, transport, SessionConfig::default()).await
    }

    pub async fn connect_with_config(
        session: web_transport::Session,
        transport: super::Transport,
        config: SessionConfig,
    ) -> Result<(Session, Self), SessionError> {
        let (session, _, subscriber) =
            Session::connect_with_config(session, None, transport, config).await?;
        Ok((session, subscriber))
    }

    /// Wait for the next inbound PUBLISH_NAMESPACE from the peer, if any.
    pub async fn published_namespace(&mut self) -> Option<PublishedNamespace> {
        self.published_namespace_queue.pop().await
    }

    /// Wait for the next inbound PUBLISH from the peer, if any.
    ///
    /// The returned [`PublishedTrack`] must be accepted with
    /// [`PublishedTrack::ok`] or dropped to reject.
    pub async fn published_track(&mut self) -> Option<PublishedTrack> {
        self.published_track_queue.pop().await
    }

    /// Take the [`TrackReader`] stored for a PUBLISH request id.
    ///
    /// Called by [`PublishedTrack::ok`] exactly once per subscription.
    pub(super) fn take_published_track_reader(&self, request_id: u64) -> Option<TrackReader> {
        self.published_tracks
            .lock()
            .ok()?
            .get_mut(&request_id)?
            .take_reader()
    }

    /// Remove all subscriber-side state for an inbound PUBLISH.
    ///
    /// Called by `PublishedTrack::drop` when the app did not call `ok()`.
    pub(super) fn remove_published_track(&self, request_id: u64) {
        if let Ok(mut tracks) = self.published_tracks.lock() {
            if let Some(recv) = tracks.remove(&request_id) {
                // Recover the alias so we can clean up the alias map too.
                drop(recv);
            }
        }
        // Alias cleanup: scan publish_alias_map for entries pointing at this
        // request id and remove them.
        // TODO(itzmanish): maintain a reverse map to make this O(1).
        if let Ok(mut aliases) = self.publish_alias_map.lock() {
            aliases.retain(|_alias, id| *id != request_id);
        }
        // Clean up subscriber_names for this request id.
        if let Ok(mut names) = self.subscriber_names.lock() {
            names.retain(|_name, id| *id != request_id);
        }
    }

    /// Send REQUEST_UPDATE to toggle Forward state on an established PUBLISH
    /// subscription (draft-16 §9.11), then wait for REQUEST_OK / REQUEST_ERROR.
    ///
    /// Called by [`PublishedTrack::set_forward`].
    pub(super) async fn send_request_update_for_publish(
        &mut self,
        publish_request_id: u64,
        forward: bool,
    ) -> Result<(), ServeError> {
        let update_id = self
            .get_next_request_id()
            .map_err(|e| ServeError::internal_ctx(format!("request ID limit: {e}")))?;

        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut updates = self
                .pending_publish_updates
                .lock()
                .map_err(|_| ServeError::internal_ctx("pending_publish_updates lock poisoned"))?;
            updates.insert(update_id, PendingPublishUpdate { result_tx: tx });
        }

        let mut params = crate::coding::KeyValuePairs::default();
        params.set_forward(forward);

        self.send_message(message::RequestUpdate {
            id: update_id,
            existing_request_id: publish_request_id,
            params,
        });

        rx.await
            .unwrap_or(Err(ServeError::internal_ctx("set_forward sender dropped")))
    }

    fn add_mlog_event<F>(&self, make_event: F)
    where
        F: FnOnce(f64) -> mlog::Event,
    {
        if let Some(ref mlog) = self.mlog {
            if let Ok(mut mlog) = mlog.lock() {
                let event = make_event(mlog.elapsed_ms());
                let _ = mlog.add_event(event);
            }
        }
    }

    fn log_request_ok_parsed(&self, request_kind: &str, msg: &message::RequestOk) {
        self.add_mlog_event(|time| mlog::events::request_ok_parsed(time, 0, request_kind, msg));
    }

    fn log_request_error_parsed(&self, request_kind: &str, msg: &message::RequestError) {
        self.add_mlog_event(|time| mlog::events::request_error_parsed(time, 0, request_kind, msg));
    }

    fn log_request_error_created(&self, request_kind: &str, msg: &message::RequestError) {
        self.add_mlog_event(|time| mlog::events::request_error_created(time, 0, request_kind, msg));
    }

    pub(super) fn send_request_ok(&mut self, request_kind: &str, msg: message::RequestOk) {
        self.add_mlog_event(|time| mlog::events::request_ok_created(time, 0, request_kind, &msg));
        self.send_message(msg);
    }

    pub(super) fn send_request_error(&mut self, request_kind: &str, msg: message::RequestError) {
        self.log_request_error_created(request_kind, &msg);
        self.send_message(msg);
    }

    /// Allocate the next outbound request ID, enforcing the peer-advertised maximum.
    ///
    /// Returns `Err(TooManyRequests)` if no budget remains and also sends
    /// REQUESTS_BLOCKED if not already sent for this limit.
    fn get_next_request_id(&mut self) -> Result<u64, SessionError> {
        match self.request_id.allocate()? {
            RequestIdAllocation::Allocated(id) => Ok(id),
            blocked @ RequestIdAllocation::Blocked { .. } => {
                if let Some(msg) = blocked.requests_blocked() {
                    let _ = self.outgoing.push(msg.into());
                }
                Err(SessionError::TooManyRequests)
            }
        }
    }

    pub fn track_status(
        &mut self,
        track_namespace: &TrackNamespace,
        track_name: impl Into<TrackName>,
    ) {
        let id = match self.get_next_request_id() {
            Ok(id) => id,
            Err(e) => {
                tracing::warn!(error = %e, "could not send TRACK_STATUS: request ID limit reached");
                return;
            }
        };
        if let Ok(mut track_statuses) = self.track_statuses.lock() {
            track_statuses.insert(id);
        } else {
            tracing::warn!("could not track outbound TRACK_STATUS: lock poisoned");
            return;
        }
        self.send_message(message::TrackStatus {
            id,
            track_namespace: track_namespace.clone(),
            track_name: track_name.into(),
            params: Default::default(),
        });
        // TODO(itzmanish): make async and wait for response?
    }

    /// Subscribe to a track by creating a new subscribe request to the publisher.  Block until subscription is closed.
    pub async fn subscribe(&mut self, track: serve::TrackWriter) -> Result<(), ServeError> {
        let subscribe = self.subscribe_open(track).await?;
        subscribe.closed().await
    }

    /// Subscribe to a track and wait until the publisher acknowledges it.
    pub async fn subscribe_open(
        &mut self,
        track: serve::TrackWriter,
    ) -> Result<Subscribe, ServeError> {
        let request_id = self
            .get_next_request_id()
            .map_err(|e| ServeError::internal_ctx(format!("request ID limit: {}", e)))?;

        // §5.1: enforce single subscriber-role subscription per track.
        // Both outbound SUBSCRIBE and inbound PUBLISH make this endpoint the
        // subscriber for the track.
        let full_name = FullTrackName {
            namespace: track.namespace.clone(),
            name: track.name.clone(),
        };
        {
            let mut names = self
                .subscriber_names
                .lock()
                .map_err(|_| ServeError::internal_ctx("subscriber_names lock poisoned"))?;
            if names.contains_key(&full_name) {
                return Err(ServeError::Duplicate);
            }
            names.insert(full_name, request_id);
        }

        let (send, recv) = Subscribe::new(self.clone(), request_id, track);
        self.subscribes
            .lock()
            .map_err(|_| ServeError::internal_ctx("subscribe lock poisoned"))?
            .insert(request_id, recv);
        send.ok().await?;
        Ok(send)
    }

    /// Send a message to the publisher via the control stream.
    pub(super) fn send_message<M: Into<message::Subscriber>>(&mut self, msg: M) {
        let msg = msg.into();

        // Remove our entry on terminal state.
        // Draft-16: PUBLISH_NAMESPACE_CANCEL carries Request ID, so look up
        // the namespace by iterating the map.
        if let message::Subscriber::PublishNamespaceCancel(msg) = &msg {
            let _ = self.drop_publish_namespace(msg.id);
        }

        // TODO report dropped messages?
        let _ = self.outgoing.push(msg.into());
    }

    /// Receive a message from the publisher via the control stream.
    pub(super) fn recv_message(&mut self, msg: message::Publisher) -> Result<(), SessionError> {
        match &msg {
            message::Publisher::PublishNamespace(msg) => self.recv_publish_namespace(msg)?,
            message::Publisher::PublishNamespaceDone(msg) => {
                self.recv_publish_namespace_done(msg)?;
            }
            // PUBLISH: publisher-initiated subscription (draft-16 §9.13).
            message::Publisher::Publish(msg) => self.recv_publish(msg)?,
            // PUBLISH_DONE terminates either a SUBSCRIBE-created or PUBLISH-created
            // subscription.  The request id alone does not tell us which map to look
            // in; we check both (§9.15).
            message::Publisher::PublishDone(msg) => self.recv_publish_done(msg)?,
            message::Publisher::SubscribeOk(msg) => self.recv_subscribe_ok(msg)?,
            // Draft-16 shared responses (REQUEST_OK / REQUEST_ERROR).
            message::Publisher::RequestOk(msg) => self.recv_request_ok(msg)?,
            message::Publisher::RequestError(msg) => self.recv_request_error(msg)?,
            // FETCH_OK is part of draft-16, but FETCH is not implemented here yet.
            message::Publisher::FetchOk(msg) => {
                tracing::debug!(
                    target: "moq_transport::control",
                    request_id = msg.id,
                    "received FETCH_OK for unsupported FETCH — ignoring"
                );
            }
        }

        Ok(())
    }

    /// Handle reception of an inbound PUBLISH_NAMESPACE from the publisher.
    fn recv_publish_namespace(
        &mut self,
        msg: &message::PublishNamespace,
    ) -> Result<(), SessionError> {
        let mut published_namespaces = self
            .published_namespaces
            .lock()
            .map_err(|_| SessionError::Internal)?;

        // Duplicate PUBLISH_NAMESPACE for the same namespace within a session is invalid.
        let entry = match published_namespaces.entry(msg.track_namespace.clone()) {
            hash_map::Entry::Occupied(_) => return Err(SessionError::Duplicate),
            hash_map::Entry::Vacant(entry) => entry,
        };

        let (published_ns, recv) =
            PublishedNamespace::new(self.clone(), msg.id, msg.track_namespace.clone());
        if let Err(published_ns) = self.published_namespace_queue.push(published_ns) {
            published_ns.close(ServeError::Cancel)?;
            return Ok(());
        }
        entry.insert(recv);

        Ok(())
    }

    /// Handle reception of PUBLISH_NAMESPACE_DONE from the publisher.
    fn recv_publish_namespace_done(
        &mut self,
        msg: &message::PublishNamespaceDone,
    ) -> Result<(), SessionError> {
        // Draft-16 §9.22: PUBLISH_NAMESPACE_DONE carries Request ID, not namespace.
        if let Some(recv) = self.drop_publish_namespace(msg.id) {
            recv.recv_done()?;
        }
        Ok(())
    }

    /// Handle the reception of a SUBSCRIBE_OK message from the publisher.
    fn recv_subscribe_ok(&mut self, msg: &message::SubscribeOk) -> Result<(), SessionError> {
        if let Some(subscribe) = self
            .subscribes
            .lock()
            .map_err(|_| SessionError::Internal)?
            .get_mut(&msg.id)
        {
            // Track Aliases are session-scoped (§10.1).  The SUBSCRIBE_OK alias
            // must not collide with any alias already bound by a SUBSCRIBE or a
            // PUBLISH subscription.
            {
                let sub_aliases = self
                    .subscribe_alias_map
                    .lock()
                    .map_err(|_| SessionError::Internal)?;
                let pub_aliases = self
                    .publish_alias_map
                    .lock()
                    .map_err(|_| SessionError::Internal)?;
                if sub_aliases.contains_key(&msg.track_alias)
                    || pub_aliases.contains_key(&msg.track_alias)
                {
                    return Err(SessionError::Duplicate);
                }
            }

            // Map track alias → subscription id for quick lookup when receiving
            // streams/datagrams.
            self.subscribe_alias_map
                .lock()
                .map_err(|_| SessionError::Internal)?
                .insert(msg.track_alias, msg.id);

            // Notify waiting tasks that the alias map has been updated.
            self.subscribe_alias_notify.notify_waiters();

            // Notify the subscribe of the successful subscription.
            subscribe.ok(msg.track_alias)?;
        }

        Ok(())
    }

    /// Remove a subscribe from our map of active subscribes, the alias map, and subscriber_names.
    pub(super) fn remove_subscribe(&mut self, id: u64) -> Option<SubscribeRecv> {
        let subscribe = self.subscribes.lock().ok().and_then(|mut s| s.remove(&id));
        if let Some(ref sub) = subscribe {
            if let Some(track_alias) = sub.track_alias() {
                if let Ok(mut alias_map) = self.subscribe_alias_map.lock() {
                    alias_map.remove(&track_alias);
                }
            }
        }
        // Clean up the subscriber_names entry for this request id.
        // TODO(itzmanish): maintain a reverse map to make this O(1).
        if let Ok(mut names) = self.subscriber_names.lock() {
            names.retain(|_name, req_id| *req_id != id);
        }
        subscribe
    }

    /// Handle an inbound PUBLISH from the publisher (draft-16 §9.13).
    ///
    /// This establishes a publisher-initiated subscription.  The endpoint
    /// becomes the subscriber for this track.
    fn recv_publish(&mut self, msg: &message::Publish) -> Result<(), SessionError> {
        // First-cut policy: reject non-empty TrackExtensions.
        // We do not yet propagate track extensions through the serve model, so
        // accepting them would silently drop relay-visible metadata (§8.6).
        // TODO(itzmanish): lift this restriction once TrackExtensions are
        // carried through TrackReader/TrackWriter.
        if !msg.track_extensions.is_empty() {
            self.send_request_error(
                "publish",
                message::RequestError {
                    id: msg.id,
                    error_code: RequestErrorCode::NotSupported as u64,
                    retry_interval: 0,
                    reason: crate::coding::ReasonPhrase(
                        "track extensions not supported".to_string(),
                    ),
                },
            );
            return Ok(());
        }

        // Track Aliases are session-scoped (§10.1).  Both alias maps share the
        // namespace, so we must check both for collisions.
        {
            let sub_aliases = self
                .subscribe_alias_map
                .lock()
                .map_err(|_| SessionError::Internal)?;
            let pub_aliases = self
                .publish_alias_map
                .lock()
                .map_err(|_| SessionError::Internal)?;
            if sub_aliases.contains_key(&msg.track_alias)
                || pub_aliases.contains_key(&msg.track_alias)
            {
                // §9.13: duplicate Track Alias closes the session.
                return Err(SessionError::Duplicate);
            }
        }

        // §5.1: enforce single subscriber-role subscription per track.
        // Both outbound SUBSCRIBE and inbound PUBLISH make this endpoint the
        // subscriber for the given (namespace, name).
        let full_name = FullTrackName {
            namespace: msg.track_namespace.clone(),
            name: msg.track_name.clone(),
        };
        let duplicate_subscription = {
            self.subscriber_names
                .lock()
                .map_err(|_| SessionError::Internal)?
                .contains_key(&full_name)
        };
        if duplicate_subscription {
            self.send_request_error(
                "publish",
                message::RequestError {
                    id: msg.id,
                    error_code: RequestErrorCode::DuplicateSubscription as u64,
                    retry_interval: 0,
                    reason: crate::coding::ReasonPhrase("duplicate subscription".to_string()),
                },
            );
            return Ok(());
        }

        // Parse FORWARD and LARGEST_OBJECT from the PUBLISH params.
        let initial_forward = msg
            .params
            .forward()
            .map_err(SessionError::Decode)?
            .unwrap_or(true);
        let largest_location = msg.params.largest_object().map_err(SessionError::Decode)?;

        // Allocate the track.  The transport owns the writer; the application
        // receives the reader via PublishedTrack::ok.
        let (writer, reader) =
            crate::serve::Track::new(msg.track_namespace.clone(), msg.track_name.clone()).produce();

        // Build both handles sharing the same state.
        let (published_track, recv) = PublishedTrackRecv::produce(
            self.clone(),
            msg.id,
            msg.track_alias,
            msg.track_namespace.clone(),
            msg.track_name.clone(),
            initial_forward,
            largest_location,
            writer,
            reader,
        );

        // Register the alias BEFORE queueing the PublishedTrack so that Object
        // streams racing the PUBLISH (§5.1 allows pre-PUBLISH_OK delivery) can
        // be resolved immediately.
        self.publish_alias_map
            .lock()
            .map_err(|_| SessionError::Internal)?
            .insert(msg.track_alias, msg.id);
        self.publish_alias_notify.notify_waiters();

        // Register subscriber_names so a duplicate SUBSCRIBE or PUBLISH for the
        // same track is rejected with DUPLICATE_SUBSCRIPTION (§5.1).
        self.subscriber_names
            .lock()
            .map_err(|_| SessionError::Internal)?
            .insert(full_name, msg.id);

        // Store the transport recv handle keyed by request id.
        self.published_tracks
            .lock()
            .map_err(|_| SessionError::Internal)?
            .insert(msg.id, recv);

        tracing::debug!(
            target: "moq_transport::control",
            request_id = msg.id,
            track_alias = msg.track_alias,
            namespace = %msg.track_namespace,
            name = %msg.track_name,
            forward = initial_forward,
            "received PUBLISH"
        );

        // If the application is no longer listening, drop the PublishedTrack
        // which sends REQUEST_ERROR back to the publisher.
        if self.published_track_queue.push(published_track).is_err() {
            // Queue is closed; clean up state we just inserted.
            self.publish_alias_map
                .lock()
                .ok()
                .map(|mut m| m.remove(&msg.track_alias));
            self.published_tracks
                .lock()
                .ok()
                .map(|mut m| m.remove(&msg.id));
            self.subscriber_names
                .lock()
                .ok()
                .map(|mut m| m.retain(|_, id| *id != msg.id));
        }

        Ok(())
    }

    /// Handle PUBLISH_DONE from the publisher (draft-16 §9.15).
    ///
    /// PUBLISH_DONE terminates either a SUBSCRIBE-created subscription
    /// (publisher sends PUBLISH_DONE after SUBSCRIBE was sent to it) or a
    /// PUBLISH-created subscription (publisher sends PUBLISH_DONE to end its
    /// push).  The request id alone does not tell us which map to look in,
    /// so we check both.
    fn recv_publish_done(&mut self, msg: &message::PublishDone) -> Result<(), SessionError> {
        // Check SUBSCRIBE-initiated subscriptions first.
        if let Some(subscribe) = self.remove_subscribe(msg.id) {
            subscribe.error(ServeError::Closed(msg.status_code))?;
            return Ok(());
        }

        // Then check PUBLISH-initiated subscriptions.
        let recv = self
            .published_tracks
            .lock()
            .map_err(|_| SessionError::Internal)?
            .remove(&msg.id);
        if let Some(mut recv) = recv {
            recv.recv_done(msg.status_code);
            // Clean up alias and name maps.
            // TODO(itzmanish): maintain reverse maps to make these O(1).
            if let Ok(mut aliases) = self.publish_alias_map.lock() {
                aliases.retain(|_alias, id| *id != msg.id);
            }
            if let Ok(mut names) = self.subscriber_names.lock() {
                names.retain(|_name, id| *id != msg.id);
            }
        } else {
            tracing::debug!(
                target: "moq_transport::control",
                request_id = msg.id,
                "received PUBLISH_DONE for unknown subscription — ignoring"
            );
        }

        Ok(())
    }

    /// Handle REQUEST_OK from the publisher (draft-16 §9.7).
    ///
    /// REQUEST_OK is the shared positive response for REQUEST_UPDATE, TRACK_STATUS,
    /// SUBSCRIBE_NAMESPACE, and PUBLISH_NAMESPACE. SUBSCRIBE uses its own dedicated
    /// SUBSCRIBE_OK message (§9.10) and does not come through this handler.
    fn recv_request_ok(&mut self, msg: &message::RequestOk) -> Result<(), SessionError> {
        if self.drop_track_status(msg.id)? {
            let request_kind = "track_status";
            self.log_request_ok_parsed(request_kind, msg);
            tracing::debug!(
                target: "moq_transport::control",
                request_id = msg.id,
                request_kind,
                "received REQUEST_OK"
            );
            return Ok(());
        }

        // Check if this is a response to a pending REQUEST_UPDATE from set_forward.
        let pending = self
            .pending_publish_updates
            .lock()
            .map_err(|_| SessionError::Internal)?
            .remove(&msg.id);

        if let Some(update) = pending {
            let request_kind = "request_update";
            self.log_request_ok_parsed(request_kind, msg);
            tracing::debug!(
                target: "moq_transport::control",
                request_id = msg.id,
                request_kind,
                "received REQUEST_OK"
            );
            // Wake the set_forward caller with success.
            let _ = update.result_tx.send(Ok(()));
            return Ok(());
        }

        let request_kind = "unknown";
        self.log_request_ok_parsed(request_kind, msg);
        tracing::debug!(
            target: "moq_transport::control",
            request_id = msg.id,
            request_kind,
            "received REQUEST_OK"
        );
        Ok(())
    }

    /// Handle REQUEST_ERROR from the publisher (draft-16 §9.8).
    ///
    /// Routes to: active SUBSCRIBE (by request id), pending REQUEST_UPDATE
    /// from set_forward, or logs and ignores.
    fn recv_request_error(&mut self, msg: &message::RequestError) -> Result<(), SessionError> {
        // Route to a matching SUBSCRIBE if present.
        if let Some(subscribe) = self.remove_subscribe(msg.id) {
            self.log_request_error_parsed("subscribe", msg);
            subscribe.error(ServeError::Closed(msg.error_code))?;
            tracing::debug!(
                target: "moq_transport::control",
                request_id = msg.id,
                request_kind = "subscribe",
                error_code = msg.error_code,
                retry_interval = msg.retry_interval,
                reason = %msg.reason.0,
                "received REQUEST_ERROR"
            );
            return Ok(());
        } else if self.drop_track_status(msg.id)? {
            self.log_request_error_parsed("track_status", msg);
            tracing::debug!(
                target: "moq_transport::control",
                request_id = msg.id,
                request_kind = "track_status",
                error_code = msg.error_code,
                retry_interval = msg.retry_interval,
                reason = %msg.reason.0,
                "received REQUEST_ERROR"
            );
            return Ok(());
        }

        // Route to a pending REQUEST_UPDATE from PublishedTrack::set_forward.
        let pending = self
            .pending_publish_updates
            .lock()
            .map_err(|_| SessionError::Internal)?
            .remove(&msg.id);
        if let Some(update) = pending {
            self.log_request_error_parsed("request_update", msg);
            tracing::debug!(
                target: "moq_transport::control",
                request_id = msg.id,
                request_kind = "request_update",
                error_code = msg.error_code,
                retry_interval = msg.retry_interval,
                reason = %msg.reason.0,
                "received REQUEST_ERROR"
            );
            let _ = update
                .result_tx
                .send(Err(ServeError::Closed(msg.error_code)));
            return Ok(());
        }

        self.log_request_error_parsed("unknown", msg);
        tracing::debug!(
            target: "moq_transport::control",
            request_id = msg.id,
            request_kind = "unknown",
            error_code = msg.error_code,
            retry_interval = msg.retry_interval,
            reason = %msg.reason.0,
            "received REQUEST_ERROR"
        );
        Ok(())
    }

    fn drop_track_status(&mut self, id: u64) -> Result<bool, SessionError> {
        Ok(self
            .track_statuses
            .lock()
            .map_err(|_| SessionError::Internal)?
            .remove(&id))
    }

    fn drop_publish_namespace(&mut self, id: u64) -> Option<PublishedNamespaceRecv> {
        if let Ok(mut ns) = self.published_namespaces.lock() {
            let key = ns
                .iter()
                .find(|(_k, v)| v.request_id == id)
                .map(|(k, _)| k.clone());
            if let Some(key) = key {
                return ns.remove(&key);
            }
        }
        None
    }

    /// Resolve a Track Alias to the request id and subscription kind.
    ///
    /// Checks both alias maps in order:
    ///   1. `subscribe_alias_map` (populated by SUBSCRIBE_OK).
    ///   2. `publish_alias_map`   (populated eagerly by recv_publish before PUBLISH_OK).
    ///
    /// PUBLISH aliases are registered before PUBLISH_OK so that Object streams
    /// racing the control message (§5.1 allows pre-PUBLISH_OK delivery) route
    /// correctly.  The SUBSCRIBE map still uses a wait-with-timeout because
    /// SUBSCRIBE_OK may arrive after the stream due to buffering.
    ///
    /// Returns `None` if the alias is not found within the timeout.
    async fn resolve_alias(
        &self,
        track_alias: u64,
        timeout_ms: Option<u64>,
    ) -> Result<Option<AliasBinding>, SessionError> {
        // Check the PUBLISH alias map first (no wait needed: inserted eagerly).
        {
            let pub_map = self
                .publish_alias_map
                .lock()
                .map_err(|_| SessionError::Internal)?;
            if let Some(&req_id) = pub_map.get(&track_alias) {
                return Ok(Some(AliasBinding::Publish(req_id)));
            }
        }

        // Fall through to the SUBSCRIBE alias map, which may need a brief wait.
        let timeout_ms = match timeout_ms {
            Some(ms) => ms,
            None => {
                // Caller does not want to wait — check once and return.
                return match self.subscribe_alias_map.lock() {
                    Ok(aliases) => Ok(aliases
                        .get(&track_alias)
                        .copied()
                        .map(AliasBinding::Subscribe)),
                    Err(_) => {
                        tracing::error!(
                            target: "moq_transport::control",
                            track_alias,
                            "subscribe_alias_map lock poisoned"
                        );
                        Err(SessionError::Internal)
                    }
                };
            }
        };

        // Wait for the alias to appear in the SUBSCRIBE map.
        let timeout_duration = Duration::from_millis(timeout_ms);
        tokio::time::timeout(timeout_duration, async {
            loop {
                // Register notification before checking to avoid a TOCTOU gap.
                let notified = self.subscribe_alias_notify.notified();

                // Re-check PUBLISH map first in case it was just inserted.
                {
                    let pub_map = match self.publish_alias_map.lock() {
                        Ok(m) => m,
                        Err(_) => return Err(SessionError::Internal),
                    };
                    if let Some(&req_id) = pub_map.get(&track_alias) {
                        return Ok(Some(AliasBinding::Publish(req_id)));
                    }
                }

                let sub_id = match self.subscribe_alias_map.lock() {
                    Ok(aliases) => aliases.get(&track_alias).copied(),
                    Err(_) => {
                        tracing::error!(
                            target: "moq_transport::control",
                            track_alias,
                            "subscribe_alias_map lock poisoned"
                        );
                        return Err(SessionError::Internal);
                    }
                };

                if let Some(id) = sub_id {
                    return Ok(Some(AliasBinding::Subscribe(id)));
                }

                notified.await;
            }
        })
        .await
        .unwrap_or(Ok(None))
    }

    /// Legacy helper — kept for the error-recovery path in `recv_stream`.
    /// Returns the SUBSCRIBE request id only.
    async fn get_subscribe_id_by_alias(
        &self,
        track_alias: u64,
        timeout_ms: Option<u64>,
    ) -> Result<Option<u64>, SessionError> {
        match self.resolve_alias(track_alias, timeout_ms).await? {
            Some(AliasBinding::Subscribe(id)) => Ok(Some(id)),
            Some(AliasBinding::Publish(_)) | None => Ok(None),
        }
    }

    /// Handle reception of a new stream from the QUIC session.
    pub(super) async fn recv_stream(
        mut self,
        stream: web_transport::RecvStream,
    ) -> Result<(), SessionError> {
        tracing::trace!("[SUBSCRIBER] recv_stream: new stream received, decoding header");
        let mut reader = Reader::new(stream);

        // Decode the stream header
        let stream_header: data::StreamHeader = reader.decode().await?;
        tracing::trace!(
            "[SUBSCRIBER] recv_stream: decoded stream header type={:?}",
            stream_header.header_type
        );

        // No fetch support yet
        if !stream_header.header_type.is_subgroup() {
            return Err(SessionError::unimplemented("non-SUBGROUP stream types"));
        }

        let subgroup_header = stream_header
            .subgroup_header
            .ok_or(SessionError::Internal)?;

        // Log subgroup header parsed/received
        if let Some(ref mlog) = self.mlog {
            if let Ok(mut mlog_guard) = mlog.lock() {
                let time = mlog_guard.elapsed_ms();
                let stream_id = 0; // TODO: Placeholder, need actual QUIC stream ID
                let event = mlog::subgroup_header_parsed(time, stream_id, &subgroup_header);
                let _ = mlog_guard.add_event(event);
            }
        }

        let track_alias = subgroup_header.track_alias;
        tracing::trace!(
            "[SUBSCRIBER] recv_stream: stream for subscription track_alias={}",
            track_alias
        );

        let mlog = self.mlog.clone();
        let res = self
            .recv_stream_inner(reader, stream_header.header_type, subgroup_header, mlog)
            .await;
        if let Err(SessionError::Serve(err)) = &res {
            tracing::warn!(
                "[SUBSCRIBER] recv_stream: stream processing error for track_alias={}: {:?}",
                track_alias,
                err
            );
            // The writer is closed, so we should terminate.
            // TODO it would be nice to do this immediately when the Writer is closed.
            if let Some(subscribe_id) = self.get_subscribe_id_by_alias(track_alias, None).await? {
                if let Some(subscribe) = self.remove_subscribe(subscribe_id) {
                    subscribe.error(err.clone())?;
                }
            }
        }

        res
    }

    /// Continue handling the reception of a new stream from the QUIC session.
    async fn recv_stream_inner(
        &mut self,
        reader: Reader,
        stream_header_type: data::StreamHeaderType,
        subgroup_header: data::SubgroupHeader,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> Result<(), SessionError> {
        let track_alias = subgroup_header.track_alias;
        tracing::trace!(
            "[SUBSCRIBER] recv_stream_inner: processing stream for track_alias={}",
            track_alias
        );

        let binding = self
            .resolve_alias(track_alias, Some(DEFAULT_ALIAS_WAIT_TIME_MS))
            .await?;

        match binding {
            Some(AliasBinding::Subscribe(subscribe_id)) => {
                tracing::trace!(
                    "[SUBSCRIBER] recv_stream_inner: receiving subgroup data (SUBSCRIBE)"
                );
                self.recv_subgroup(
                    stream_header_type,
                    subgroup_header,
                    subscribe_id,
                    reader,
                    mlog,
                )
                .await?;
            }
            Some(AliasBinding::Publish(publish_id)) => {
                tracing::trace!(
                    "[SUBSCRIBER] recv_stream_inner: receiving subgroup data (PUBLISH)"
                );
                self.recv_subgroup_publish(
                    stream_header_type,
                    subgroup_header,
                    publish_id,
                    reader,
                    mlog,
                )
                .await?;
            }
            None => {
                return Err(SessionError::Serve(ServeError::not_found_ctx(format!(
                    "track_alias={} not found in subscribe or publish maps",
                    track_alias
                ))));
            }
        }

        tracing::trace!(
            "[SUBSCRIBER] recv_stream_inner: completed processing stream for track_alias={}",
            track_alias
        );
        Ok(())
    }

    /// Decode subgroup objects from a stream and write them via `get_writer`.
    ///
    /// This is the single implementation of the subgroup receive loop shared
    /// by both SUBSCRIBE-initiated and PUBLISH-initiated subscriptions.
    ///
    /// `get_writer` is called once — on the first object in the subgroup — to
    /// obtain a `SubgroupWriter`.  It receives the (possibly updated) subgroup
    /// header and must open the writer against the correct map entry (either
    /// `subscribes` for outbound SUBSCRIBE, or `published_tracks` for inbound
    /// PUBLISH).  Keeping this as a closure avoids duplicating ~100 lines of
    /// object decoding, ID tracking, validation, logging, and payload reading.
    async fn recv_subgroup_objects(
        stream_header_type: data::StreamHeaderType,
        mut subgroup_header: data::SubgroupHeader,
        mut reader: Reader,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
        mut get_writer: impl FnMut(data::SubgroupHeader) -> Result<serve::SubgroupWriter, SessionError>,
    ) -> Result<(), SessionError> {
        tracing::trace!(
            "[SUBSCRIBER] recv_subgroup_objects: starting - group_id={}, subgroup_id={:?}, priority={}",
            subgroup_header.group_id,
            subgroup_header.subgroup_id,
            subgroup_header.publisher_priority
        );

        let mut object_count = 0;
        let mut previous_object_id: Option<u64> = None;
        let mut subgroup_writer: Option<serve::SubgroupWriter> = None;

        while !reader.done().await? {
            // Decode the object header.  Extension-header variant carries extra
            // relay-visible fields; plain variant does not.
            let (mut remaining_bytes, object_id_delta, status, decoded_object) =
                if stream_header_type.has_extension_headers() {
                    let object = reader.decode::<data::SubgroupObjectExt>().await?;
                    tracing::trace!(
                        "[SUBSCRIBER] recv_subgroup_objects: object #{} with ext headers \
                         object_id_delta={} payload={} status={:?} ext={:?}",
                        object_count + 1,
                        object.object_id_delta,
                        object.payload_length,
                        object.status,
                        object.extension_headers
                    );
                    // Check for known draft-14 extension types
                    if object.extension_headers.has(0xB) {
                        tracing::trace!(
                            "[SUBSCRIBER] recv_subgroup_objects: object #{} has IMMUTABLE EXTENSIONS (0xB)",
                            object_count + 1
                        );
                    }
                    if object.extension_headers.has(0x3C) {
                        tracing::trace!(
                            "[SUBSCRIBER] recv_subgroup_objects: object #{} has PRIOR GROUP ID GAP (0x3C)",
                            object_count + 1
                        );
                    }
                    let obj_copy = object.clone();
                    (
                        object.payload_length,
                        object.object_id_delta,
                        object.status,
                        Some(obj_copy),
                    )
                } else {
                    let object = reader.decode::<data::SubgroupObject>().await?;
                    tracing::trace!(
                        "[SUBSCRIBER] recv_subgroup_objects: object #{} \
                         object_id_delta={} payload={} status={:?}",
                        object_count + 1,
                        object.object_id_delta,
                        object.payload_length,
                        object.status
                    );
                    (
                        object.payload_length,
                        object.object_id_delta,
                        object.status,
                        None,
                    )
                };

            // Compute the absolute object ID from the delta.
            let current_object_id = match previous_object_id {
                Some(prev) => prev
                    .checked_add(object_id_delta)
                    .and_then(|v| v.checked_add(1))
                    .ok_or_else(|| {
                        SessionError::ProtocolViolation("subgroup object id overflow".to_string())
                    })?,
                None => object_id_delta,
            };
            previous_object_id = Some(current_object_id);

            let extension_headers = decoded_object.as_ref().map(|o| o.extension_headers.clone());

            // Non-normal status with extension headers is a protocol violation.
            if status.is_some_and(|s| s != data::ObjectStatus::NormalObject)
                && extension_headers.as_ref().is_some_and(|h| !h.is_empty())
            {
                return Err(SessionError::ProtocolViolation(
                    "non-normal object status with extension headers".to_string(),
                ));
            }

            // Open the subgroup writer on the first object.
            if subgroup_writer.is_none() {
                if stream_header_type.uses_first_object_id_as_subgroup_id() {
                    subgroup_header.subgroup_id = Some(current_object_id);
                }
                subgroup_writer = Some(get_writer(subgroup_header.clone())?);
            }

            // Log the object.
            if let Some(ref mlog) = mlog {
                if let Ok(mut mlog_guard) = mlog.lock() {
                    let time = mlog_guard.elapsed_ms();
                    let stream_id = 0; // TODO: Placeholder, need actual QUIC stream ID
                    let event = if let Some(obj_ext) = decoded_object {
                        mlog::subgroup_object_ext_parsed(
                            time,
                            stream_id,
                            subgroup_header.group_id,
                            subgroup_header.subgroup_id.unwrap_or(0),
                            current_object_id,
                            &obj_ext,
                        )
                    } else {
                        // For non-extension objects, create a temporary SubgroupObject for logging
                        let temp_obj = data::SubgroupObject {
                            object_id_delta,
                            payload_length: remaining_bytes,
                            status,
                        };
                        mlog::subgroup_object_parsed(
                            time,
                            stream_id,
                            subgroup_header.group_id,
                            subgroup_header.subgroup_id.unwrap_or(0),
                            current_object_id,
                            &temp_obj,
                        )
                    };
                    let _ = mlog_guard.add_event(event);
                }
            }

            // Write the object payload.
            // TODO SLG - object_id_delta and object status are still being ignored
            let subgroup_writer = subgroup_writer.as_mut().ok_or(SessionError::Internal)?;
            let mut object_writer = subgroup_writer.create(remaining_bytes, extension_headers)?;

            while remaining_bytes > 0 {
                let chunk = reader.read_chunk(remaining_bytes).await?.ok_or_else(|| {
                    tracing::error!(
                        "[SUBSCRIBER] recv_subgroup_objects: stream ended with {} bytes remaining",
                        remaining_bytes
                    );
                    SessionError::WrongSize
                })?;
                remaining_bytes -= chunk.len();
                object_writer.write(chunk)?;
            }

            object_count += 1;
        }

        tracing::trace!(
            "[SUBSCRIBER] recv_subgroup_objects: completed (group_id={}, subgroup_id={}, {} objects)",
            subgroup_header.group_id,
            subgroup_header.subgroup_id.unwrap_or(0),
            object_count
        );
        Ok(())
    }

    /// Handle subgroup stream data for a SUBSCRIBE-initiated subscription.
    async fn recv_subgroup(
        &mut self,
        stream_header_type: data::StreamHeaderType,
        subgroup_header: data::SubgroupHeader,
        subscribe_id: u64,
        reader: Reader,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> Result<(), SessionError> {
        let subscribes = self.subscribes.clone();
        Self::recv_subgroup_objects(
            stream_header_type,
            subgroup_header,
            reader,
            mlog,
            move |header| {
                let mut map = subscribes.lock().map_err(|_| SessionError::Internal)?;
                let recv = map.get_mut(&subscribe_id).ok_or_else(|| {
                    SessionError::Serve(ServeError::not_found_ctx(format!(
                        "subscribe_id={} not found for track_alias={}",
                        subscribe_id, header.track_alias
                    )))
                })?;
                Ok(recv.subgroup(header)?)
            },
        )
        .await
    }

    /// Handle subgroup stream data for a PUBLISH-initiated subscription.
    async fn recv_subgroup_publish(
        &mut self,
        stream_header_type: data::StreamHeaderType,
        subgroup_header: data::SubgroupHeader,
        publish_id: u64,
        reader: Reader,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> Result<(), SessionError> {
        let published_tracks = self.published_tracks.clone();
        Self::recv_subgroup_objects(
            stream_header_type,
            subgroup_header,
            reader,
            mlog,
            move |header| {
                let mut map = published_tracks
                    .lock()
                    .map_err(|_| SessionError::Internal)?;
                let recv = map.get_mut(&publish_id).ok_or_else(|| {
                    SessionError::Serve(ServeError::not_found_ctx(format!(
                        "publish_id={} not found for track_alias={}",
                        publish_id, header.track_alias
                    )))
                })?;
                Ok(recv.subgroup(header)?)
            },
        )
        .await
    }

    /// Handle reception of a datagram from the QUIC session.
    pub async fn recv_datagram(&mut self, datagram: bytes::Bytes) -> Result<(), SessionError> {
        let mut cursor = io::Cursor::new(datagram);
        let datagram = data::Datagram::decode(&mut cursor)?;

        if let Some(ref mlog) = self.mlog {
            if let Ok(mut mlog_guard) = mlog.lock() {
                let time = mlog_guard.elapsed_ms();
                let stream_id = 0; // TODO: Placeholder, need actual QUIC stream ID
                let _ =
                    mlog_guard.add_event(mlog::object_datagram_parsed(time, stream_id, &datagram));
            }
        }

        // Check for extension headers in the datagram
        if let Some(ref ext_headers) = datagram.extension_headers {
            tracing::trace!(
                "[SUBSCRIBER] recv_datagram: datagram contains extension headers: {:?}",
                ext_headers
            );

            // Check for known draft-14 extension types

            // Check for Immutable Extensions (type 0xB = 11)
            if ext_headers.has(0xB) {
                tracing::trace!(
                    "[SUBSCRIBER] recv_datagram: datagram contains IMMUTABLE EXTENSIONS (type 0xB)"
                );
                if let Some(immutable_ext) = ext_headers.get(0xB) {
                    tracing::trace!(
                        "[SUBSCRIBER] recv_datagram: immutable extension details: {:?}",
                        immutable_ext
                    );
                }
            }

            // Check for Prior Group ID Gap (type 0x3C = 60)
            if ext_headers.has(0x3C) {
                tracing::trace!(
                    "[SUBSCRIBER] recv_datagram: datagram contains PRIOR GROUP ID GAP (type 0x3C)"
                );
                if let Some(gap_ext) = ext_headers.get(0x3C) {
                    tracing::trace!(
                        "[SUBSCRIBER] recv_datagram: prior group id gap details: {:?}",
                        gap_ext
                    );
                }
            }
        }

        let track_alias = datagram.track_alias;
        let binding = self
            .resolve_alias(track_alias, Some(DEFAULT_ALIAS_WAIT_TIME_MS))
            .await?;

        match binding {
            Some(AliasBinding::Subscribe(subscribe_id)) => {
                if let Some(subscribe) = self
                    .subscribes
                    .lock()
                    .ok()
                    .as_mut()
                    .and_then(|s| s.get_mut(&subscribe_id))
                {
                    tracing::trace!(
                        "[SUBSCRIBER] recv_datagram (SUBSCRIBE): track_alias={}, group_id={}, object_id={}",
                        track_alias,
                        datagram.group_id,
                        datagram.object_id.unwrap_or(0)
                    );
                    subscribe.datagram(datagram)?;
                }
            }
            Some(AliasBinding::Publish(publish_id)) => {
                if let Some(recv) = self
                    .published_tracks
                    .lock()
                    .ok()
                    .as_mut()
                    .and_then(|m| m.get_mut(&publish_id))
                {
                    tracing::trace!(
                        "[SUBSCRIBER] recv_datagram (PUBLISH): track_alias={}, group_id={}, object_id={}",
                        track_alias,
                        datagram.group_id,
                        datagram.object_id.unwrap_or(0)
                    );
                    recv.datagram(datagram)?;
                }
            }
            None => {
                tracing::warn!(
                    "[SUBSCRIBER] recv_datagram: discarded due to unknown track_alias={}, group_id={}, object_id={}",
                    track_alias,
                    datagram.group_id,
                    datagram.object_id.unwrap_or(0)
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::task::Poll;

    use super::*;
    use crate::{message, serve::Track};

    fn subscriber() -> Subscriber {
        let request_id = RequestId::new(0, 100, 100, 0);
        Subscriber::new(Queue::default(), None, request_id)
    }

    #[test]
    fn request_ok_routes_to_pending_track_status() {
        let mut subscriber = subscriber();
        let namespace = TrackNamespace::from_utf8_path("test");

        subscriber.track_status(&namespace, "0.mp4");
        assert!(subscriber.track_statuses.lock().unwrap().contains(&0));

        subscriber
            .recv_request_ok(&message::RequestOk {
                id: 0,
                params: Default::default(),
            })
            .unwrap();

        assert!(subscriber.track_statuses.lock().unwrap().is_empty());
    }

    #[test]
    fn request_error_routes_to_pending_track_status() {
        let mut subscriber = subscriber();
        let namespace = TrackNamespace::from_utf8_path("test");

        subscriber.track_status(&namespace, "0.mp4");
        assert!(subscriber.track_statuses.lock().unwrap().contains(&0));

        subscriber
            .recv_request_error(&message::RequestError {
                id: 0,
                error_code: message::RequestErrorCode::DoesNotExist as u64,
                retry_interval: 0,
                reason: crate::coding::ReasonPhrase("not found".to_string()),
            })
            .unwrap();

        assert!(subscriber.track_statuses.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn subscribe_open_cleans_up_when_cancelled_before_ok() {
        let mut subscriber = subscriber();
        let observer = subscriber.clone();
        let (writer, _reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "0.mp4").produce();

        {
            let subscribe = subscriber.subscribe_open(writer);
            futures::pin_mut!(subscribe);

            assert!(matches!(futures::poll!(&mut subscribe), Poll::Pending));
            assert_eq!(observer.subscribes.lock().unwrap().len(), 1);
        }

        assert!(observer.subscribes.lock().unwrap().is_empty());
        assert!(observer.subscribe_alias_map.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dropping_open_subscribe_removes_recv_state() {
        let mut subscriber = subscriber();
        let observer = subscriber.clone();
        let (writer, _reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "0.mp4").produce();

        let subscribe = subscriber.subscribe_open(writer);
        futures::pin_mut!(subscribe);

        assert!(matches!(futures::poll!(&mut subscribe), Poll::Pending));
        assert_eq!(observer.subscribes.lock().unwrap().len(), 1);

        let mut receiver = observer.clone();
        receiver
            .recv_subscribe_ok(&message::SubscribeOk {
                id: 0,
                track_alias: 10,
                params: Default::default(),
                track_extensions: Default::default(),
            })
            .unwrap();

        let subscribe = match futures::poll!(&mut subscribe) {
            Poll::Ready(Ok(subscribe)) => subscribe,
            Poll::Ready(Err(err)) => panic!("subscribe failed: {err}"),
            Poll::Pending => panic!("subscribe remained pending after SubscribeOk"),
        };

        assert_eq!(observer.subscribes.lock().unwrap().len(), 1);
        assert_eq!(
            observer
                .subscribe_alias_map
                .lock()
                .unwrap()
                .get(&10)
                .copied(),
            Some(0)
        );

        drop(subscribe);

        assert!(observer.subscribes.lock().unwrap().is_empty());
        assert!(observer.subscribe_alias_map.lock().unwrap().is_empty());
    }
}
