// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{num::NonZeroU64, ops};

use crate::{
    coding::{KeyValuePairs, Location, TrackNamespace},
    data,
    message::{self, FilterType, GroupOrder},
    serve::{self, ServeError, TrackWriter, TrackWriterMode},
};

use crate::watch::State;

use super::Subscriber;

// TODO rename to SubscriptionInfo when used for Publishes as well?
#[derive(Debug, Clone)]
pub struct SubscribeInfo {
    pub id: u64,
    pub track_namespace: TrackNamespace,
    pub track_name: String,

    /// Subscriber Priority
    pub subscriber_priority: u8,
    pub group_order: GroupOrder,

    /// Forward Flag
    pub forward: bool,

    /// Filter type
    pub filter_type: FilterType,

    /// The starting location for this subscription. Only present for "AbsoluteStart" and "AbsoluteRange" filter types.
    pub start_location: Option<Location>,
    /// End group id, inclusive, for the subscription, if applicable. Only present for "AbsoluteRange" filter type.
    pub end_group_id: Option<u64>,

    /// Optional parameters
    pub params: KeyValuePairs,

    // Set to true if this is a track_status request only
    pub track_status: bool,
}

impl SubscribeInfo {
    pub fn new_from_subscribe(msg: &message::Subscribe) -> Self {
        Self {
            id: msg.id,
            track_namespace: msg.track_namespace.clone(),
            track_name: msg.track_name.clone(),
            subscriber_priority: msg.subscriber_priority,
            group_order: msg.group_order,
            forward: msg.forward,
            filter_type: msg.filter_type,
            start_location: msg.start_location,
            end_group_id: msg.end_group_id,
            params: msg.params.clone(),
            track_status: false,
        }
    }
}

struct SubscribeState {
    ok: bool,
    track_alias: Option<u64>,
    closed: Result<(), ServeError>,
}

impl Default for SubscribeState {
    fn default() -> Self {
        Self {
            ok: Default::default(),
            track_alias: None,
            closed: Ok(()),
        }
    }
}

// Held by the application
#[must_use = "unsubscribe on drop"]
pub struct Subscribe {
    state: State<SubscribeState>,
    subscriber: Subscriber,

    pub info: SubscribeInfo,
}

impl Subscribe {
    pub(super) fn new(
        mut subscriber: Subscriber,
        request_id: u64,
        track: TrackWriter,
    ) -> (Subscribe, SubscribeRecv) {
        let subscribe_message = message::Subscribe {
            id: request_id,
            track_namespace: track.namespace.clone(),
            track_name: track.name.clone(),
            // TODO add prioritization logic on the publisher side
            subscriber_priority: 127, // default to mid value, see: https://github.com/moq-wg/moq-transport/issues/504
            group_order: GroupOrder::Publisher, // defer to publisher send order
            forward: true,            // default to forwarding objects
            filter_type: FilterType::LargestObject,
            start_location: None,
            end_group_id: None,
            params: Default::default(),
        };
        let info = SubscribeInfo::new_from_subscribe(&subscribe_message);

        subscriber.send_message(subscribe_message);

        let (send, recv) = State::default().split();

        let send = Subscribe {
            state: send,
            subscriber,
            info,
        };

        let recv = SubscribeRecv {
            state: recv,
            writer: Some(track.into()),
        };

        (send, recv)
    }

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

    pub async fn ok(&self) -> Result<(), ServeError> {
        loop {
            {
                let state = self.state.lock();
                state.closed.clone()?;

                if state.ok {
                    return Ok(());
                }

                match state.modified() {
                    Some(notify) => notify,
                    None => return Err(ServeError::Done),
                }
            }
            .await;
        }
    }
}

impl Drop for Subscribe {
    fn drop(&mut self) {
        self.subscriber
            .send_message(message::Unsubscribe { id: self.info.id });
        self.subscriber.remove_subscribe(self.info.id);
    }
}

impl ops::Deref for Subscribe {
    type Target = SubscribeInfo;

    fn deref(&self) -> &SubscribeInfo {
        &self.info
    }
}

pub(super) struct SubscribeRecv {
    state: State<SubscribeState>,
    writer: Option<TrackWriterMode>,
}

impl SubscribeRecv {
    pub fn ok(
        &mut self,
        alias: u64,
        history_window_groups: Option<NonZeroU64>,
    ) -> Result<(), ServeError> {
        // Acknowledge first: reject a duplicate OK and record the alias. The
        // history window is applied AFTER, best-effort — it is an advisory
        // retention optimization (BLO-10339/10419), so a failure to apply it
        // must not fail an otherwise-valid subscription (and must not route into
        // the session-level Serve-error swallow that would leave it un-acked).
        {
            let state = self.state.lock();
            if state.ok {
                return Err(ServeError::Duplicate);
            }

            if let Some(mut state) = state.into_mut() {
                state.ok = true;
                state.track_alias = Some(alias);
            }
        }

        // Apply the publisher's subgroup history window to the local mirror's
        // Track. Setting it on the shared TrackState both bounds the mirror
        // (TrackWriter::subgroups() inherits it) AND makes the downstream-facing
        // TrackReader::history_window() expose it, so a relay re-serving this
        // track re-advertises the window in its own SUBSCRIBE_OK (serve_inner
        // reads TrackReader::history_window). The window thus propagates
        // transitively across relay hops (BLO-10419). The writer is still in
        // Track mode here: SUBSCRIBE_OK precedes any subgroup stream
        // (subscribed.rs sends it via send_message_and_wait).
        if let Some(window) = history_window_groups {
            match self.writer.as_mut() {
                Some(TrackWriterMode::Track(track)) => {
                    if let Err(err) = track.set_history_window(window) {
                        tracing::warn!(
                            ?err,
                            "subgroup history window not applied to mirror track; retention unbounded"
                        );
                    }
                }
                Some(TrackWriterMode::Subgroups(subgroups)) => {
                    if let Err(err) = subgroups.set_history_window(window.get()) {
                        tracing::warn!(
                            ?err,
                            "subgroup history window not applied to mirror subgroups; retention unbounded"
                        );
                    }
                }
                // Datagram tracks carry no subgroups, so there is no retention
                // window to bound. Any other mode would mean SUBSCRIBE_OK arrived
                // after the writer was converted/taken — unreachable today (OK
                // precedes stream data); logged so a future ordering regression
                // surfaces instead of silently dropping the window.
                _ => tracing::debug!(
                    "subgroup history window ignored: mirror writer not in Track/Subgroups mode"
                ),
            }
        }

        Ok(())
    }

    pub fn track_alias(&self) -> Option<u64> {
        let state = self.state.lock();
        state.track_alias
    }

    pub fn error(mut self, err: ServeError) -> Result<(), ServeError> {
        if let Some(writer) = self.writer.take() {
            writer.close(err.clone())?;
        }

        let state = self.state.lock();
        state.closed.clone()?;

        let mut state = state.into_mut().ok_or(ServeError::Cancel)?;
        state.closed = Err(err);

        Ok(())
    }

    pub fn subgroup(
        &mut self,
        header: data::SubgroupHeader,
    ) -> Result<serve::SubgroupWriter, ServeError> {
        let writer = self.writer.take().ok_or(ServeError::Done)?;

        let mut subgroups = match writer {
            // TODO SLG - understand why both of these are needed, clock demo won't run if I comment out TrackWriteMode::Track
            // The local mirror Track carries the publisher's history window (set
            // in ok(), BLO-10339); subgroups() inherits it to bound retention.
            TrackWriterMode::Track(track) => track.subgroups()?,
            TrackWriterMode::Subgroups(subgroups) => subgroups,
            _ => return Err(ServeError::Mode),
        };

        let writer = subgroups.create(serve::Subgroup {
            group_id: header.group_id,
            // When subgroup_id is not present in the header type, it implicitly means subgroup 0
            subgroup_id: header.subgroup_id.unwrap_or(0),
            priority: header.publisher_priority,
        })?;

        self.writer = Some(subgroups.into());

        Ok(writer)
    }

    pub fn datagram(&mut self, datagram: data::Datagram) -> Result<(), ServeError> {
        let writer = self.writer.take().ok_or(ServeError::Done)?;

        match writer {
            TrackWriterMode::Track(track) => {
                // convert Track -> Datagrams writer, write, then put Datagrams back
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
                // preserve whatever unexpected mode was present, then report error
                self.writer = Some(other);
                Err(ServeError::Mode)
            }
        }
    }
}
