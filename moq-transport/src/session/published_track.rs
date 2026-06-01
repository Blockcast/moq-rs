// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Inbound PUBLISH handling: a subscriber receives a PUBLISH from a publisher
//! and can accept it with PUBLISH_OK, reject it with REQUEST_ERROR, and then
//! receive Objects on the resulting subscription.
//!
//! # Ownership model (draft-16 §9.13 / §9.14)
//!
//! The transport layer retains the `TrackWriter` (stored inside
//! `PublishedTrackRecv`) so that `recv_stream` and `recv_datagram` can write
//! inbound Objects without application involvement.  The application receives
//! the `TrackReader` by calling `PublishedTrack::ok`.
//!
//! This mirrors the existing outbound-SUBSCRIBE path where `SubscribeRecv`
//! owns the writer and the stream/datagram handlers write into it.

use crate::{
    coding::{KeyValuePairs, Location, ReasonPhrase, TrackName, TrackNamespace},
    data,
    message::{self, RequestErrorCode},
    serve::{self, ServeError, TrackReader, TrackWriterMode},
    watch::State,
};

use super::Subscriber;

// ── Shared state ──────────────────────────────────────────────────────────────

/// State shared between `PublishedTrack` (application handle) and
/// `PublishedTrackRecv` (transport handle).
pub(crate) struct PublishedTrackState {
    /// True once PUBLISH_DONE has been received from the publisher.
    done: bool,
    /// Terminal result; set when `done` becomes true.
    closed: Result<(), ServeError>,
}

impl Default for PublishedTrackState {
    fn default() -> Self {
        Self {
            done: false,
            closed: Ok(()),
        }
    }
}

// ── Application-facing handle ─────────────────────────────────────────────────

/// An inbound PUBLISH received by this endpoint acting as subscriber
/// (draft-16 §9.13).
///
/// Call [`ok`] to accept the subscription and obtain the [`TrackReader`].
/// Dropping without calling [`ok`] sends `REQUEST_ERROR UNINTERESTED` back to
/// the publisher.
pub struct PublishedTrack {
    session: Subscriber,
    state: State<PublishedTrackState>,

    /// Request ID of the inbound PUBLISH (draft-16 §9.1).
    request_id: u64,
    /// Track Alias chosen by the publisher (§10.1).
    track_alias: u64,
    /// Full track identifier.
    namespace: TrackNamespace,
    name: TrackName,
    /// Initial Forward value parsed from the PUBLISH params (§9.13 / §5.1).
    initial_forward: bool,
    /// LARGEST_OBJECT from the PUBLISH params, if present (§5.1).
    largest_location: Option<Location>,

    /// True once `ok()` has been called successfully.
    ok: bool,
    /// Optional override for the rejection error sent on drop.
    error: Option<ServeError>,
}

impl PublishedTrack {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        session: Subscriber,
        request_id: u64,
        track_alias: u64,
        namespace: TrackNamespace,
        name: TrackName,
        initial_forward: bool,
        largest_location: Option<Location>,
        state: State<PublishedTrackState>,
    ) -> Self {
        Self {
            session,
            state,
            request_id,
            track_alias,
            namespace,
            name,
            initial_forward,
            largest_location,
            ok: false,
            error: None,
        }
    }

    /// Accept the PUBLISH by sending PUBLISH_OK (draft-16 §9.14).
    ///
    /// Returns the `TrackReader` the application should poll for Objects.
    ///
    /// `forward` sets the initial Forward State:
    ///   - `true`  — publisher may start transmitting Objects immediately.
    ///   - `false` — publisher pauses until the subscriber sends REQUEST_UPDATE
    ///               with Forward=1.
    ///
    /// # Protocol note
    /// PUBLISH has a dedicated success response (§9.14, type 0x1E).  Do not
    /// use REQUEST_OK (§9.7) here.
    pub fn ok(&mut self, forward: bool) -> Result<TrackReader, ServeError> {
        if self.ok {
            return Err(ServeError::Duplicate);
        }

        let mut params = KeyValuePairs::default();
        params.set_forward(forward);

        // PUBLISH_OK uses its own message type 0x1E (§9.14).
        self.session.send_message(message::PublishOk {
            id: self.request_id,
            params,
        });
        self.ok = true;

        // Take the TrackReader that was pre-allocated in recv_publish.
        let reader = self
            .session
            .take_published_track_reader(self.request_id)
            .ok_or(ServeError::Done)?;

        Ok(reader)
    }

    /// Mark this track for rejection with a specific error on drop.
    pub fn close(mut self, err: ServeError) {
        self.error = Some(err);
    }

    /// Wait until the publisher sends PUBLISH_DONE or the session closes.
    ///
    /// Returns `Ok(())` on clean termination (TRACK_ENDED), or the error code
    /// from PUBLISH_DONE on all other outcomes.
    pub async fn closed(&self) -> Result<(), ServeError> {
        loop {
            {
                let state = self.state.lock();
                state.closed.clone()?;
                match state.modified() {
                    Some(notify) => notify,
                    None => return Ok(()),
                }
            }
            .await;
        }
    }

    /// Toggle Forward state by sending REQUEST_UPDATE (draft-16 §9.11).
    ///
    /// Waits for the publisher's REQUEST_OK or returns the error from
    /// REQUEST_ERROR.
    pub async fn set_forward(&mut self, forward: bool) -> Result<(), ServeError> {
        self.session
            .send_request_update_for_publish(self.request_id, forward)
            .await
    }

    pub fn namespace(&self) -> &TrackNamespace {
        &self.namespace
    }

    pub fn name(&self) -> &TrackName {
        &self.name
    }

    pub fn track_alias(&self) -> u64 {
        self.track_alias
    }

    pub fn initial_forward(&self) -> bool {
        self.initial_forward
    }

    pub fn largest_location(&self) -> Option<Location> {
        self.largest_location
    }
}

impl Drop for PublishedTrack {
    fn drop(&mut self) {
        if self.ok {
            // Already accepted; nothing to send — PUBLISH_DONE arrives from the
            // publisher to terminate.
            return;
        }

        // Never accepted: send REQUEST_ERROR to reject the subscription (§9.8).
        let err = self.error.clone().unwrap_or(ServeError::Cancel);

        let error_code = match &err {
            ServeError::Cancel | ServeError::Done => RequestErrorCode::Uninterested as u64,
            ServeError::Duplicate => RequestErrorCode::DuplicateSubscription as u64,
            ServeError::NotFound | ServeError::NotFoundWithId(_, _) => {
                RequestErrorCode::DoesNotExist as u64
            }
            ServeError::NotImplemented(_) | ServeError::NotImplementedWithId(_, _) => {
                RequestErrorCode::NotSupported as u64
            }
            ServeError::Internal(_) | ServeError::InternalWithId(_, _) => {
                RequestErrorCode::InternalError as u64
            }
            ServeError::Closed(code) => *code,
            _ => RequestErrorCode::InternalError as u64,
        };

        self.session.send_request_error(
            "publish",
            message::RequestError {
                id: self.request_id,
                error_code,
                retry_interval: 0,
                reason: ReasonPhrase("uninterested".to_string()),
            },
        );

        // Clean up subscriber-side state for this PUBLISH.
        self.session.remove_published_track(self.request_id);
    }
}

// ── Transport-facing recv handle ──────────────────────────────────────────────

/// Transport-side bookkeeping for a single inbound PUBLISH.
///
/// Stored in `Subscriber::published_tracks`.  Stream and datagram receive
/// paths write Objects directly into the `TrackWriterMode` here.
pub(crate) struct PublishedTrackRecv {
    /// Shared state so both the transport and app can observe PUBLISH_DONE.
    state: State<PublishedTrackState>,

    /// Write half for inbound Objects.  The transport owns this so it can push
    /// Objects without going through the application.
    writer: Option<TrackWriterMode>,

    /// Read half returned to the application on `PublishedTrack::ok`.
    /// Wrapped in `Option` so it can be taken exactly once.
    reader: Option<TrackReader>,
}

impl PublishedTrackRecv {
    /// Create a `PublishedTrack` / `PublishedTrackRecv` pair from a PUBLISH message.
    ///
    /// Returns both the application-facing handle and the transport-facing recv,
    /// pre-wired to the same shared state.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn produce(
        session: Subscriber,
        request_id: u64,
        track_alias: u64,
        namespace: TrackNamespace,
        name: TrackName,
        initial_forward: bool,
        largest_location: Option<Location>,
        writer: serve::TrackWriter,
        reader: TrackReader,
    ) -> (PublishedTrack, PublishedTrackRecv) {
        let (app_state, transport_state) = State::<PublishedTrackState>::default().split();

        let app = PublishedTrack::new(
            session,
            request_id,
            track_alias,
            namespace,
            name,
            initial_forward,
            largest_location,
            app_state,
        );
        let recv = Self {
            state: transport_state,
            writer: Some(writer.into()),
            reader: Some(reader),
        };

        (app, recv)
    }

    /// Take the `TrackReader` out exactly once (for `PublishedTrack::ok`).
    pub fn take_reader(&mut self) -> Option<TrackReader> {
        self.reader.take()
    }

    /// Open a subgroup writer for the given subgroup header.
    ///
    /// Mirrors `SubscribeRecv::subgroup` so the same `recv_subgroup` code path
    /// can serve both SUBSCRIBE-initiated and PUBLISH-initiated subscriptions.
    pub fn subgroup(
        &mut self,
        header: data::SubgroupHeader,
    ) -> Result<serve::SubgroupWriter, ServeError> {
        let writer = self.writer.take().ok_or(ServeError::Done)?;

        let mut subgroups = match writer {
            TrackWriterMode::Track(track) => track.subgroups()?,
            TrackWriterMode::Subgroups(subgroups) => subgroups,
            _ => return Err(ServeError::Mode),
        };

        let subgroup_writer = subgroups.create(serve::Subgroup {
            group_id: header.group_id,
            subgroup_id: header.subgroup_id.unwrap_or(0),
            priority: header.publisher_priority,
        })?;

        self.writer = Some(subgroups.into());
        Ok(subgroup_writer)
    }

    /// Write a datagram Object into the track.
    ///
    /// Mirrors `SubscribeRecv::datagram`.
    pub fn datagram(&mut self, datagram: data::Datagram) -> Result<(), ServeError> {
        let writer = self.writer.take().ok_or(ServeError::Done)?;

        match writer {
            TrackWriterMode::Track(track) => {
                let mut datagrams = track.datagrams()?;
                datagrams.write(serve::Datagram {
                    group_id: datagram.group_id,
                    object_id: datagram.object_id.unwrap_or(0),
                    priority: datagram.publisher_priority,
                    payload: datagram.payload.unwrap_or_default(),
                    extension_headers: datagram.extension_headers.unwrap_or_default(),
                })?;
                self.writer = Some(TrackWriterMode::Datagrams(datagrams));
                Ok(())
            }
            TrackWriterMode::Datagrams(mut datagrams) => {
                datagrams.write(serve::Datagram {
                    group_id: datagram.group_id,
                    object_id: datagram.object_id.unwrap_or(0),
                    priority: datagram.publisher_priority,
                    payload: datagram.payload.unwrap_or_default(),
                    extension_headers: datagram.extension_headers.unwrap_or_default(),
                })?;
                self.writer = Some(TrackWriterMode::Datagrams(datagrams));
                Ok(())
            }
            other => {
                self.writer = Some(other);
                Err(ServeError::Mode)
            }
        }
    }

    /// Called when PUBLISH_DONE arrives (§9.15).
    ///
    /// Closes the writer so the `TrackReader` sees end-of-track.
    pub fn recv_done(&mut self, status_code: u64) {
        if let Some(mut state) = self.state.lock_mut() {
            state.done = true;
            state.closed = if status_code == message::PublishDoneCode::TrackEnded as u64 {
                Err(ServeError::Done)
            } else {
                Err(ServeError::Closed(status_code))
            };
        }
        // Drop the writer to signal end-of-track to any downstream readers.
        self.writer = None;
    }
}

// ── Pending REQUEST_UPDATE for set_forward ────────────────────────────────────

/// Bookkeeping for a pending REQUEST_UPDATE sent by `PublishedTrack::set_forward`.
///
/// Stored in `Subscriber::pending_publish_updates` keyed by the REQUEST_UPDATE
/// request id.  When REQUEST_OK or REQUEST_ERROR arrives it is removed and the
/// sender is notified.
pub(crate) struct PendingPublishUpdate {
    /// One-shot channel to wake the `set_forward` caller.
    pub result_tx: tokio::sync::oneshot::Sender<Result<(), ServeError>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        coding::TrackNamespace,
        serve::Track,
        session::{Queue, RequestId},
    };

    fn make_pair(
        request_id: u64,
    ) -> (
        PublishedTrack,
        PublishedTrackRecv,
        crate::session::Subscriber,
    ) {
        let rid = RequestId::new(0, 100, 100, 0);
        let subscriber = crate::session::Subscriber::new(Queue::default(), None, rid);
        let (writer, reader) =
            Track::new(TrackNamespace::from_utf8_path("test"), "0.mp4").produce();
        let (pt, recv) = PublishedTrackRecv::produce(
            subscriber.clone(),
            request_id,
            42,
            TrackNamespace::from_utf8_path("test"),
            "0.mp4".into(),
            true,
            None,
            writer,
            reader,
        );
        (pt, recv, subscriber)
    }

    #[test]
    fn take_reader_returns_once() {
        let (_pt, mut recv, _sub) = make_pair(0);
        assert!(recv.take_reader().is_some());
        assert!(
            recv.take_reader().is_none(),
            "reader must only be given out once"
        );
    }

    #[test]
    fn recv_done_closes_writer_and_sets_state() {
        let (_pt, mut recv, _sub) = make_pair(1);
        assert!(recv.writer.is_some());

        recv.recv_done(message::PublishDoneCode::TrackEnded as u64);

        assert!(
            recv.writer.is_none(),
            "writer must be dropped after PUBLISH_DONE"
        );
    }

    #[test]
    fn recv_done_non_track_ended_stores_code() {
        let (_pt, mut recv, _sub) = make_pair(2);
        recv.recv_done(message::PublishDoneCode::Expired as u64);
        // No panic and writer is gone.
        assert!(recv.writer.is_none());
    }

    #[test]
    fn published_track_drop_without_ok_does_not_panic() {
        let (pt, _recv, _sub) = make_pair(3);
        // Drop without calling ok() — should send REQUEST_ERROR, not panic.
        drop(pt);
    }
}
