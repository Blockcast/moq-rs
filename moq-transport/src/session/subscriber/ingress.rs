// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Subscriber data-plane ingress: subgroup streams and object datagrams,
//! resolved to their owning SUBSCRIBE/PUBLISH via the track alias registry.
//! Control-message response routing stays in the parent module.

use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{
    coding::Decode,
    data, mlog,
    serve::{self, ServeError},
};

use super::super::{Reader, SessionError};
use super::{registry::TrackOrigin, Subscriber};

// Default timeout for waiting for subscribe aliases to become available via SUBSCRIBE_OK (1 second)
const DEFAULT_ALIAS_WAIT_TIME_MS: u64 = 1000;

impl Subscriber {
    /// Resolve a Track Alias to its owning subscription (draft-16 §10.1).
    ///
    /// PUBLISH aliases are registered eagerly in `recv_publish` (before
    /// PUBLISH_OK), and SUBSCRIBE aliases in `recv_subscribe_ok`. A stream or
    /// datagram can still race ahead of SUBSCRIBE_OK, so when `timeout_ms` is
    /// set we wait up to that long for the alias to appear.
    ///
    /// Returns `None` if the alias is not found (immediately when `timeout_ms`
    /// is `None`, or after the timeout otherwise).
    async fn get_track_origin_by_alias(
        &self,
        track_alias: u64,
        timeout_ms: Option<u64>,
    ) -> Result<Option<TrackOrigin>, SessionError> {
        // Fast path: already present.
        {
            let aliases = self
                .track_alias_map
                .lock()
                .map_err(|_| SessionError::Internal)?;
            if let Some(origin) = aliases.get(track_alias) {
                return Ok(Some(origin));
            }
        }

        let timeout_ms = match timeout_ms {
            Some(ms) => ms,
            None => return Ok(None),
        };

        let timeout_duration = Duration::from_millis(timeout_ms);
        tokio::time::timeout(timeout_duration, async {
            loop {
                // Register notification before checking to avoid a TOCTOU gap.
                let notified = self.track_alias_notify.notified();

                {
                    let aliases = self
                        .track_alias_map
                        .lock()
                        .map_err(|_| SessionError::Internal)?;
                    if let Some(origin) = aliases.get(track_alias) {
                        return Ok(Some(origin));
                    }
                }

                notified.await;
            }
        })
        .await
        .unwrap_or(Ok(None))
    }

    /// Handle reception of a new stream from the QUIC session.
    pub(in crate::session) async fn recv_stream(
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

        if stream_header.header_type.is_fetch() {
            let fetch = stream_header.fetch_header.ok_or(SessionError::Internal)?;
            let mut fetches = self.fetches.lock().map_err(|_| SessionError::Internal)?;
            let recv = fetches.get_mut(&fetch.request_id).ok_or_else(|| {
                SessionError::Serve(ServeError::not_found_ctx(format!(
                    "fetch request {} not found",
                    fetch.request_id
                )))
            })?;
            recv.recv_stream(reader)?;
            return Ok(());
        }

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
            if let Some(TrackOrigin::Subscribe(subscribe_id)) =
                self.get_track_origin_by_alias(track_alias, None).await?
            {
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

        let origin = self
            .get_track_origin_by_alias(track_alias, Some(DEFAULT_ALIAS_WAIT_TIME_MS))
            .await?
            .ok_or_else(|| {
                SessionError::Serve(ServeError::not_found_ctx(format!(
                    "track_alias={} not found",
                    track_alias
                )))
            })?;

        self.recv_subgroup(stream_header_type, subgroup_header, origin, reader, mlog)
            .await?;

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
    /// `subscribes` for outbound SUBSCRIBE, or `publishes_received` for inbound
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

            // Write the object payload under the Object ID decoded from the
            // stream, so relays cache and forward the publisher's IDs
            // (draft-16 §8.1, §8.7).
            // TODO SLG - object status is still being ignored
            let subgroup_writer = subgroup_writer.as_mut().ok_or(SessionError::Internal)?;
            let mut object_writer = subgroup_writer.create_with_id(
                current_object_id,
                remaining_bytes,
                extension_headers,
            )?;

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

    /// Handle subgroup stream data, routing the writer through the owning
    /// subscription (SUBSCRIBE or PUBLISH) identified by `origin`.
    ///
    /// The shared object loop (`recv_subgroup_objects`) opens the writer lazily
    /// on the first object via the closure, which preserves the case where the
    /// subgroup id is derived from the first object id.
    async fn recv_subgroup(
        &mut self,
        stream_header_type: data::StreamHeaderType,
        subgroup_header: data::SubgroupHeader,
        origin: TrackOrigin,
        reader: Reader,
        mlog: Option<Arc<Mutex<mlog::MlogWriter>>>,
    ) -> Result<(), SessionError> {
        let subscribes = self.subscribes.clone();
        let publishes_received = self.publishes_received.clone();
        Self::recv_subgroup_objects(
            stream_header_type,
            subgroup_header,
            reader,
            mlog,
            move |header| match origin {
                TrackOrigin::Subscribe(subscribe_id) => {
                    let mut map = subscribes.lock().map_err(|_| SessionError::Internal)?;
                    let recv = map.get_mut(&subscribe_id).ok_or_else(|| {
                        SessionError::Serve(ServeError::not_found_ctx(format!(
                            "subscribe_id={} not found for track_alias={}",
                            subscribe_id, header.track_alias
                        )))
                    })?;
                    Ok(recv.subgroup(header)?)
                }
                TrackOrigin::Publish(publish_id) => {
                    let mut map = publishes_received
                        .lock()
                        .map_err(|_| SessionError::Internal)?;
                    let recv = map.get_mut(&publish_id).ok_or_else(|| {
                        SessionError::Serve(ServeError::not_found_ctx(format!(
                            "publish_id={} not found for track_alias={}",
                            publish_id, header.track_alias
                        )))
                    })?;
                    Ok(recv.subgroup(header)?)
                }
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
        let origin = self
            .get_track_origin_by_alias(track_alias, Some(DEFAULT_ALIAS_WAIT_TIME_MS))
            .await?;

        match origin {
            Some(TrackOrigin::Subscribe(subscribe_id)) => {
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
            Some(TrackOrigin::Publish(publish_id)) => {
                if let Some(recv) = self
                    .publishes_received
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
