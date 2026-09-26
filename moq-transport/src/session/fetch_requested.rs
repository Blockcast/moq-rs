// SPDX-FileCopyrightText: 2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use bytes::{Bytes, BytesMut};

use crate::{
    coding::{KeyValuePairs, Location, VarInt},
    data::{
        DataStreamResetCode, FetchEndOfRange, FetchEntry, FetchHeader, FetchObject,
        FetchObjectDecoder, FetchObjectEncoder, StreamHeaderType,
    },
    message::{self, GroupOrder, Message, RequestErrorCode, TrackExtensions},
    serve::ServeError,
    watch::{Queue, State},
};

use super::{send_order, Fetch, SessionError, SessionId, Writer, DEFAULT_SUBSCRIBER_PRIORITY};

const COPY_CHUNK_SIZE: usize = 64 * 1024;

struct FetchRequestedState {
    closed: Result<(), ServeError>,
}

impl Default for FetchRequestedState {
    fn default() -> Self {
        Self { closed: Ok(()) }
    }
}

/// An Object a publisher supplies from its own storage in a FETCH response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchResponseObject {
    /// The Object's properties as serialized on the FETCH stream (§10.4.4).
    pub object: FetchObject,

    /// The Object Payload, as the chunks it is stored in. Their lengths add up
    /// to `object.payload_length`; they are written back to back.
    pub payload: Vec<Bytes>,
}

impl FetchResponseObject {
    pub fn location(&self) -> Location {
        self.object.location()
    }
}

/// How a FETCH response continues after the Objects a publisher supplies from
/// its own storage (draft-16 §9.16.3).
pub enum FetchRest {
    /// The supplied Objects are every Object in the requested range. The stream
    /// ends with a FIN and FETCH_OK carries these fields (§9.17).
    Complete {
        end_of_track: bool,
        end_location: Location,
    },

    /// The rest of the range is answered by an upstream FETCH of exactly that
    /// rest: its stream is appended to the supplied Objects, and its FETCH_OK or
    /// REQUEST_ERROR answers this request (§9.16.3: a relay that meets an Object
    /// it has not cached confirms its status upstream).
    Upstream {
        fetch: Box<Fetch>,
        timeout: Duration,
    },

    /// Every Object after the supplied ones, up to and including
    /// `last_location`, has unknown status and there is nothing to ask. The
    /// stream carries an End of Unknown Range marker for them (§10.4.4.2) and a
    /// FIN; FETCH_OK carries `end_location`. `last_location` is the last
    /// Location of the requested range in the order the response is sent.
    Unknown {
        last_location: Location,
        end_location: Location,
    },
}

/// An inbound standalone FETCH waiting for application routing.
#[must_use = "serve, proxy, reject, or drop the FETCH request"]
pub struct FetchRequested {
    webtransport: Option<web_transport::Session>,
    session_id: SessionId,
    outgoing: Queue<Message>,
    active: Arc<Mutex<HashMap<u64, FetchRequestedRecv>>>,
    state: State<FetchRequestedState>,
    id: u64,
    subscriber_priority: u8,
    group_order: GroupOrder,
    #[cfg(test)]
    capture: Option<Arc<Mutex<FetchCapture>>>,
    pub request: message::Fetch,
}

pub(crate) struct FetchRequestedRecv {
    state: State<FetchRequestedState>,
}

impl FetchRequested {
    pub(super) fn new(
        webtransport: Option<web_transport::Session>,
        session_id: SessionId,
        outgoing: Queue<Message>,
        active: Arc<Mutex<HashMap<u64, FetchRequestedRecv>>>,
        request: message::Fetch,
    ) -> Result<(Self, FetchRequestedRecv), SessionError> {
        // §9.2.2.3: "If omitted from ... FETCH, the publisher uses the value 128."
        let subscriber_priority = request
            .params
            .subscriber_priority()?
            .unwrap_or(DEFAULT_SUBSCRIBER_PRIORITY);
        // §9.2.2.4: "If omitted from FETCH, the receiver uses Ascending (0x1)."
        let group_order = request
            .params
            .group_order()?
            .unwrap_or(GroupOrder::Ascending);
        let id = request.id;
        let (send, recv) = State::default().split();
        Ok((
            Self {
                webtransport,
                session_id,
                outgoing,
                active,
                state: send,
                id,
                subscriber_priority,
                group_order,
                #[cfg(test)]
                capture: None,
                request,
            },
            FetchRequestedRecv { state: recv },
        ))
    }

    /// Subscriber Priority of this FETCH (draft-16 §9.2.2.3), 128 when the
    /// parameter was omitted. Every Object of the response is scheduled under it
    /// first (§7.2).
    pub fn subscriber_priority(&self) -> u8 {
        self.subscriber_priority
    }

    /// The order in which the response must send groups (draft-16 §9.2.2.4),
    /// Ascending when the parameter was omitted.
    pub fn group_order(&self) -> GroupOrder {
        self.group_order
    }

    pub async fn closed(&self) -> Result<(), ServeError> {
        loop {
            let notify = {
                let state = self.state.lock();
                state.closed.clone()?;
                state.modified()
            };
            match notify {
                Some(notify) => notify.await,
                None => return Ok(()),
            }
        }
    }

    pub fn reject(
        self,
        code: RequestErrorCode,
        reason: impl Into<String>,
    ) -> Result<(), ServeError> {
        self.claim_response()?;
        self.send_error(code, reason);
        Ok(())
    }

    /// Answer entirely from an upstream FETCH of the same range.
    pub async fn proxy(self, upstream: Fetch, timeout: Duration) -> Result<(), SessionError> {
        self.serve(
            Vec::new(),
            FetchRest::Upstream {
                fetch: Box::new(upstream),
                timeout,
            },
        )
        .await
    }

    /// Answer with `objects`, which the caller holds itself, followed by `rest`.
    ///
    /// `objects` must be the leading Objects of the requested range, in the
    /// order of [`Self::group_order`] and, within a group, in Object ID order
    /// (§9.16.3), with nothing of unknown status between them. They are written
    /// to one unidirectional stream, each scheduled at the request's Subscriber
    /// Priority and then its own Publisher Priority (§7.2).
    ///
    /// A FETCH_CANCEL resets the stream with CANCELLED (§5.2).
    pub async fn serve(
        self,
        objects: Vec<FetchResponseObject>,
        rest: FetchRest,
    ) -> Result<(), SessionError> {
        match rest {
            FetchRest::Upstream { fetch, timeout } => {
                self.serve_with_upstream(objects, *fetch, timeout).await
            }
            FetchRest::Complete {
                end_of_track,
                end_location,
            } => {
                self.serve_local(objects, None, end_of_track, end_location)
                    .await
            }
            FetchRest::Unknown {
                last_location,
                end_location,
            } => {
                self.serve_local(objects, Some(last_location), false, end_location)
                    .await
            }
        }
    }

    async fn serve_local(
        self,
        objects: Vec<FetchResponseObject>,
        unknown_through: Option<Location>,
        end_of_track: bool,
        end_location: Location,
    ) -> Result<(), SessionError> {
        let reset = FetchReset::default();
        let result = {
            let operation = self.write_local(&objects, unknown_through, reset.clone());
            tokio::pin!(operation);
            tokio::select! {
                biased;
                closed = self.closed() => {
                    reset.set(DataStreamResetCode::Cancelled);
                    return Err(closed.err().unwrap_or(ServeError::Done).into());
                },
                result = &mut operation => result,
            }
        };

        match result {
            Ok(()) => {
                let id = self.id;
                self.respond(message::FetchOk {
                    id,
                    end_of_track,
                    end_location,
                    params: KeyValuePairs::default(),
                    track_extensions: TrackExtensions::default(),
                })
            }
            Err(err) => {
                self.reject(RequestErrorCode::InternalError, "fetch response failed")?;
                Err(err)
            }
        }
    }

    async fn write_local(
        &self,
        objects: &[FetchResponseObject],
        unknown_through: Option<Location>,
        reset: FetchReset,
    ) -> Result<(), SessionError> {
        let mut stream = self.open_stream(reset).await?;
        for object in objects {
            stream.write_object(object).await?;
        }
        if let Some(location) = unknown_through {
            stream
                .write_end_of_range(FetchEndOfRange::Unknown, location)
                .await?;
        }
        stream.finish().await
    }

    async fn serve_with_upstream(
        self,
        objects: Vec<FetchResponseObject>,
        mut upstream: Fetch,
        timeout: Duration,
    ) -> Result<(), SessionError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let reset = FetchReset::default();
        let result = {
            let operation = self.serve_with_upstream_inner(&objects, &mut upstream, reset.clone());
            tokio::pin!(operation);
            tokio::select! {
                biased;
                closed = self.closed() => {
                    reset.set(DataStreamResetCode::Cancelled);
                    return Err(closed.err().unwrap_or(ServeError::Done).into());
                },
                _ = tokio::time::sleep_until(deadline) => {
                    reset.set(DataStreamResetCode::DeliveryTimeout);
                    None
                },
                result = &mut operation => Some(result),
            }
        };

        match result {
            Some(Ok(response)) => self.respond(response),
            None => {
                self.reject(RequestErrorCode::Timeout, "fetch proxy timed out")?;
                Err(ServeError::Cancel.into())
            }
            Some(Err(err)) => {
                if let Some(error) = self.wait_for_request_error(&upstream, deadline).await? {
                    let request_id = self.id;
                    self.respond(proxied_error(error, request_id))?;
                    return Err(err);
                }
                self.reject(RequestErrorCode::InternalError, "fetch proxy failed")?;
                Err(err)
            }
        }
    }

    async fn serve_with_upstream_inner(
        &self,
        objects: &[FetchResponseObject],
        upstream: &mut Fetch,
        reset: FetchReset,
    ) -> Result<message::FetchOk, SessionError> {
        let mut stream = self.open_stream(reset).await?;
        for object in objects {
            stream.write_object(object).await?;
        }
        stream.copy_from(upstream).await?;

        let response = upstream.ok().await?;
        stream.finish().await?;
        Ok(proxied_response(response, self.id))
    }

    async fn open_stream(&self, reset: FetchReset) -> Result<FetchStreamWriter, SessionError> {
        let sink = match self.webtransport.as_ref() {
            Some(webtransport) => FetchSink::Stream(Writer::new(
                self.session_id.clone(),
                webtransport.open_uni().await?,
            )),
            #[cfg(test)]
            None if self.capture.is_some() => {
                FetchSink::Buffer(self.capture.clone().ok_or(SessionError::Internal)?)
            }
            None => return Err(SessionError::Internal),
        };
        Ok(FetchStreamWriter::new(
            sink,
            self.id,
            self.subscriber_priority,
            reset,
        ))
    }

    async fn wait_for_request_error(
        &self,
        upstream: &Fetch,
        deadline: tokio::time::Instant,
    ) -> Result<Option<message::RequestError>, ServeError> {
        if let Some(error) = upstream.request_error() {
            return Ok(Some(error));
        }
        tokio::select! {
            closed = self.closed() => {
                closed?;
                Ok(None)
            },
            _ = tokio::time::sleep_until(deadline) => Ok(None),
            _ = upstream.ok() => Ok(upstream.request_error()),
        }
    }

    fn respond(self, response: impl Into<Message>) -> Result<(), SessionError> {
        self.claim_response()?;
        let _ = self.outgoing.clone().push(response.into());
        Ok(())
    }

    fn claim_response(&self) -> Result<(), ServeError> {
        let state = self.state.lock();
        state.closed.clone()?;
        let mut state = state.into_mut().ok_or(ServeError::Done)?;
        state.closed = Err(ServeError::Done);
        Ok(())
    }

    fn send_error(&self, code: RequestErrorCode, reason: impl Into<String>) {
        let reason = reason.into();
        let _ = self
            .outgoing
            .clone()
            .push(message::RequestError::new(self.id, code, 0, &reason).into());
    }

    fn remove_active(&self) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&self.id);
        }
    }
}

impl Drop for FetchRequested {
    fn drop(&mut self) {
        if self.claim_response().is_ok() {
            self.send_error(RequestErrorCode::InternalError, "fetch request dropped");
        }
        self.remove_active();
    }
}

impl FetchRequestedRecv {
    pub fn cancel(&mut self) -> Result<(), ServeError> {
        let state = self.state.lock();
        if state.closed.is_err() {
            return Ok(());
        }
        let Some(mut state) = state.into_mut() else {
            return Ok(());
        };
        state.closed = Err(ServeError::Cancel);
        Ok(())
    }
}

pub(super) fn inclusive_end(end: Location) -> Location {
    if end.object_id == 0 {
        Location::new(end.group_id, VarInt::MAX.into_inner())
    } else {
        Location::new(end.group_id, end.object_id - 1)
    }
}

fn proxied_response(mut response: message::FetchOk, request_id: u64) -> message::FetchOk {
    response.id = request_id;
    response.params = KeyValuePairs::default();
    response
}

fn proxied_error(mut response: message::RequestError, request_id: u64) -> message::RequestError {
    response.id = request_id;
    response
}

#[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
fn upstream_reset_code(err: &SessionError) -> Option<u32> {
    match err {
        SessionError::WebTransport(web_transport::Error::Read(
            web_transport::quinn::ReadError::Reset(code),
        )) => Some(*code),
        _ => None,
    }
}

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
fn upstream_reset_code(_err: &SessionError) -> Option<u32> {
    None
}

/// Where a FETCH response is written.
enum FetchSink {
    Stream(Writer),
    /// In-memory stand-in for a QUIC stream, for unit tests.
    #[cfg(test)]
    Buffer(Arc<Mutex<FetchCapture>>),
}

/// What a test sink observed: the bytes written, each send order applied with
/// the byte offset from which it took effect, and how the stream ended.
#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct FetchCapture {
    pub bytes: BytesMut,
    pub send_orders: Vec<(usize, i32)>,
    pub finished: bool,
    pub reset: Option<u32>,
}

impl FetchSink {
    async fn write(&mut self, buf: &[u8]) -> Result<(), SessionError> {
        match self {
            Self::Stream(writer) => writer.write(buf).await,
            #[cfg(test)]
            Self::Buffer(capture) => {
                capture
                    .lock()
                    .map_err(|_| SessionError::Internal)?
                    .bytes
                    .extend_from_slice(buf);
                Ok(())
            }
        }
    }

    fn set_priority(&mut self, order: i32) {
        match self {
            Self::Stream(writer) => writer.set_priority(order),
            #[cfg(test)]
            Self::Buffer(capture) => {
                if let Ok(mut capture) = capture.lock() {
                    let offset = capture.bytes.len();
                    capture.send_orders.push((offset, order));
                }
            }
        }
    }

    fn finish(&mut self) -> Result<(), SessionError> {
        match self {
            Self::Stream(writer) => writer.finish(),
            #[cfg(test)]
            Self::Buffer(capture) => {
                capture.lock().map_err(|_| SessionError::Internal)?.finished = true;
                Ok(())
            }
        }
    }

    fn reset(&mut self, code: u32) {
        match self {
            Self::Stream(writer) => writer.reset(code),
            #[cfg(test)]
            Self::Buffer(capture) => {
                if let Ok(mut capture) = capture.lock() {
                    capture.reset.get_or_insert(code);
                }
            }
        }
    }
}

/// The unidirectional stream of one FETCH response (draft-16 §10.4.4).
///
/// The stream is reset with the code in its [`FetchReset`] unless it was
/// finished, so an abandoned response is never mistaken for a complete one.
struct FetchStreamWriter {
    sink: FetchSink,
    request_id: u64,
    header_written: bool,
    subscriber_priority: u8,
    /// The send order last applied to the stream.
    send_order: Option<i32>,
    encoder: FetchObjectEncoder,
    reset: FetchReset,
    finished: bool,
}

impl FetchStreamWriter {
    fn new(sink: FetchSink, request_id: u64, subscriber_priority: u8, reset: FetchReset) -> Self {
        Self {
            sink,
            request_id,
            header_written: false,
            subscriber_priority,
            send_order: None,
            encoder: FetchObjectEncoder::new(),
            reset,
            finished: false,
        }
    }

    /// Schedule what is written next at the request's Subscriber Priority and
    /// then `publisher_priority` (§7.2). A FETCH response "can contain objects
    /// with different publisher priorities" (§7.1), so this is re-evaluated for
    /// every Object; quinn applies it to data not yet transmitted.
    fn prioritize(&mut self, publisher_priority: u8) {
        let order = send_order(self.subscriber_priority, publisher_priority);
        if self.send_order != Some(order) {
            self.sink.set_priority(order);
            self.send_order = Some(order);
        }
    }

    /// Write the FETCH_HEADER if nothing has been written yet. It is deferred
    /// until the first entry so that it goes out under that entry's priority.
    async fn write_header(&mut self) -> Result<(), SessionError> {
        if self.header_written {
            return Ok(());
        }
        let mut buf = BytesMut::new();
        crate::coding::Encode::encode(
            &FetchHeader {
                header_type: StreamHeaderType::Fetch,
                request_id: self.request_id,
            },
            &mut buf,
        )?;
        self.sink.write(&buf).await?;
        self.header_written = true;
        Ok(())
    }

    async fn write_object(&mut self, object: &FetchResponseObject) -> Result<(), SessionError> {
        let payload_length: usize = object.payload.iter().map(Bytes::len).sum();
        if payload_length != object.object.payload_length {
            return Err(ServeError::Size.into());
        }

        self.prioritize(object.object.publisher_priority);
        self.write_header().await?;

        let mut buf = BytesMut::new();
        self.encoder.encode_object(&object.object, &mut buf)?;
        self.sink.write(&buf).await?;
        for chunk in &object.payload {
            self.sink.write(chunk).await?;
        }
        Ok(())
    }

    async fn write_end_of_range(
        &mut self,
        kind: FetchEndOfRange,
        location: Location,
    ) -> Result<(), SessionError> {
        self.write_header().await?;
        let mut buf = BytesMut::new();
        self.encoder.encode_end_of_range(kind, location, &mut buf)?;
        self.sink.write(&buf).await
    }

    /// Append the Objects of an upstream FETCH response.
    ///
    /// The bytes are forwarded unchanged. Entry headers are parsed only to find
    /// each Object's Publisher Priority for scheduling and to recognise where
    /// its payload ends, so a truncated or malformed upstream stream resets this
    /// one with MALFORMED_TRACK instead of being finished. An upstream reset is
    /// passed on with the upstream's code.
    async fn copy_from(&mut self, upstream: &mut Fetch) -> Result<(), SessionError> {
        let mut decoder = FetchObjectDecoder::new();
        // Bytes of an entry header that straddles upstream chunks.
        let mut partial = BytesMut::new();
        let mut payload_remaining = 0usize;

        loop {
            let mut chunk = match upstream.read_stream_chunk(COPY_CHUNK_SIZE).await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(err) => {
                    if let Some(code) = upstream_reset_code(&err) {
                        self.reset.set_raw(code);
                    }
                    return Err(err);
                }
            };

            while !chunk.is_empty() {
                if payload_remaining > 0 {
                    let part = chunk.split_to(payload_remaining.min(chunk.len()));
                    payload_remaining -= part.len();
                    self.sink.write(&part).await?;
                    continue;
                }

                let decoded = if partial.is_empty() {
                    decoder.decode(&chunk)
                } else {
                    decoder.decode(&partial)
                };
                match decoded {
                    Ok((entry, consumed)) => {
                        if let FetchEntry::Object(object) = &entry {
                            self.prioritize(object.publisher_priority);
                            payload_remaining = object.payload_length;
                        }
                        self.write_header().await?;
                        if partial.is_empty() {
                            let header = chunk.split_to(consumed);
                            self.sink.write(&header).await?;
                        } else {
                            // `partial` holds exactly this header: bytes are only
                            // moved into it as the decoder asks for them.
                            let header = partial.split_to(consumed).freeze();
                            self.sink.write(&header).await?;
                            let leftover = partial.split().freeze();
                            payload_remaining = payload_remaining
                                .checked_sub(leftover.len())
                                .ok_or_else(|| SessionError::from(ServeError::Size))?;
                            self.sink.write(&leftover).await?;
                        }
                    }
                    Err(crate::coding::DecodeError::More(needed)) => {
                        if partial.is_empty() {
                            partial.extend_from_slice(&chunk);
                            chunk.clear();
                        } else {
                            let take = needed.max(1).min(chunk.len());
                            partial.extend_from_slice(&chunk.split_to(take));
                        }
                    }
                    Err(err) => {
                        self.reset.set(DataStreamResetCode::MalformedTrack);
                        return Err(err.into());
                    }
                }
            }
        }

        if payload_remaining > 0 || !partial.is_empty() {
            // §10.4: a FIN in the middle of a serialized Object.
            self.reset.set(DataStreamResetCode::MalformedTrack);
            return Err(ServeError::Size.into());
        }
        Ok(())
    }

    async fn finish(mut self) -> Result<(), SessionError> {
        self.write_header().await?;
        self.sink.finish()?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for FetchStreamWriter {
    fn drop(&mut self) {
        if !self.finished {
            self.sink.reset(self.reset.code());
        }
    }
}

#[derive(Clone)]
struct FetchReset(Arc<AtomicU32>);

impl Default for FetchReset {
    fn default() -> Self {
        Self(Arc::new(AtomicU32::new(
            DataStreamResetCode::InternalError.into(),
        )))
    }
}

impl FetchReset {
    fn set(&self, code: DataStreamResetCode) {
        self.set_raw(code.into());
    }

    fn set_raw(&self, code: u32) {
        self.0.store(code, Ordering::Release);
    }

    fn code(&self) -> u32 {
        self.0.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        coding::{Location, TrackNamespace},
        message::{Fetch, FetchType, StandaloneFetch},
    };

    use super::*;

    fn request(id: u64) -> Fetch {
        Fetch {
            id,
            fetch_type: FetchType::Standalone,
            standalone_fetch: Some(StandaloneFetch {
                track_namespace: TrackNamespace::from_utf8_path("test"),
                track_name: "video".into(),
                start_location: Location::new(0, 0),
                end_location: Location::new(1, 0),
            }),
            joining_fetch: None,
            params: Default::default(),
        }
    }

    struct Handles {
        request: FetchRequested,
        recv: FetchRequestedRecv,
        _keepalive: Queue<Message>,
        outgoing: Queue<Message>,
        active: Arc<Mutex<HashMap<u64, FetchRequestedRecv>>>,
    }

    fn handles(id: u64) -> Handles {
        let (outgoing, receiver) = Queue::default().split();
        let keepalive = outgoing.clone();
        let active = Arc::new(Mutex::new(HashMap::new()));
        let (request, recv) = FetchRequested::new(
            None,
            SessionId::generate(),
            outgoing,
            active.clone(),
            request(id),
        )
        .unwrap();
        Handles {
            request,
            recv,
            _keepalive: keepalive,
            outgoing: receiver,
            active,
        }
    }

    #[test]
    fn cancel_after_fetch_request_drop_is_benign() {
        let (request, state) = State::<FetchRequestedState>::default().split();
        let mut recv = FetchRequestedRecv { state };
        drop(request);

        assert!(recv.cancel().is_ok());
    }

    #[tokio::test]
    async fn reject_sends_one_error_and_removes_active_state() {
        let Handles {
            request,
            recv,
            _keepalive,
            mut outgoing,
            active,
        } = handles(7);
        active.lock().unwrap().insert(7, recv);

        request
            .reject(RequestErrorCode::NotSupported, "not supported")
            .unwrap();

        let Message::RequestError(error) = outgoing.pop().await.unwrap() else {
            panic!("expected REQUEST_ERROR");
        };
        assert_eq!(error.id, 7);
        assert_eq!(error.error_code, RequestErrorCode::NotSupported as u64);
        assert!(active.lock().unwrap().is_empty());
        assert!(outgoing.close().is_empty());
    }

    #[tokio::test]
    async fn cancellation_wakes_request_and_suppresses_drop_error() {
        let Handles {
            request,
            mut recv,
            _keepalive,
            outgoing,
            active,
        } = handles(9);
        active.lock().unwrap().insert(
            9,
            FetchRequestedRecv {
                state: recv.state.clone(),
            },
        );

        recv.cancel().unwrap();
        assert!(matches!(request.closed().await, Err(ServeError::Cancel)));
        drop(request);

        assert!(active.lock().unwrap().is_empty());
        assert!(outgoing.close().is_empty());
    }

    #[tokio::test]
    async fn dropping_unanswered_request_sends_internal_error() {
        let Handles {
            mut request,
            recv,
            _keepalive,
            mut outgoing,
            active,
        } = handles(11);
        active.lock().unwrap().insert(11, recv);
        request.request.id = 99;

        drop(request);

        let Message::RequestError(error) = outgoing.pop().await.unwrap() else {
            panic!("expected REQUEST_ERROR");
        };
        assert_eq!(error.id, 11);
        assert_eq!(error.error_code, RequestErrorCode::InternalError as u64);
        assert!(active.lock().unwrap().is_empty());
    }

    #[test]
    fn proxied_terminal_messages_remap_only_request_id() {
        let mut ok = message::FetchOk {
            id: 1,
            end_of_track: true,
            end_location: Location::new(4, 8),
            params: KeyValuePairs::default(),
            track_extensions: Default::default(),
        };
        ok.params.set_intvalue(2, 7);
        ok.track_extensions.set_delivery_timeout(10);
        let mapped = proxied_response(ok.clone(), 99);
        assert_eq!(mapped.id, 99);
        assert_eq!(mapped.end_of_track, ok.end_of_track);
        assert_eq!(mapped.end_location, ok.end_location);
        assert_eq!(mapped.track_extensions, ok.track_extensions);
        assert!(mapped.params.0.is_empty());

        let error = message::RequestError::new(1, RequestErrorCode::DoesNotExist, 42, "not here");
        let mapped = proxied_error(error.clone(), 99);
        assert_eq!(mapped.id, 99);
        assert_eq!(mapped.error_code, error.error_code);
        assert_eq!(mapped.retry_interval, error.retry_interval);
        assert_eq!(mapped.reason, error.reason);
    }

    #[cfg(any(not(target_arch = "wasm32"), target_os = "wasi"))]
    #[test]
    fn upstream_reset_codes_are_preserved() {
        for code in [0, 1, 2, 3, 4, 0x12, 0xdead_beef] {
            let err = SessionError::WebTransport(web_transport::Error::Read(
                web_transport::quinn::ReadError::Reset(code),
            ));
            assert_eq!(upstream_reset_code(&err), Some(code));
        }
    }

    #[tokio::test]
    async fn proxy_waits_for_request_error_after_stream_failure() {
        let subscriber = super::super::Subscriber::new(
            Queue::default(),
            Queue::default(),
            None,
            super::super::RequestId::new(0, 100, 100, 0),
            super::super::PendingRequests::default(),
            super::super::SessionId::generate(),
        );
        let (upstream, mut upstream_recv) = super::super::Fetch::new(subscriber, request(64));
        let Handles {
            request,
            recv: _recv,
            _keepalive,
            outgoing: _,
            active: _,
        } = handles(7);
        let expected =
            message::RequestError::new(64, RequestErrorCode::DoesNotExist, 42, "origin failed");
        let delivered = expected.clone();
        let deliver_error = async {
            tokio::task::yield_now().await;
            upstream_recv.recv_error(&delivered).unwrap();
        };

        let (result, ()) = tokio::join!(
            request.wait_for_request_error(
                &upstream,
                tokio::time::Instant::now() + Duration::from_secs(1),
            ),
            deliver_error,
        );

        assert_eq!(result.unwrap(), Some(expected));
    }

    // ---------------------------------------------------------------
    // Serving from supplied Objects
    // ---------------------------------------------------------------

    fn handles_with_params(id: u64, params: KeyValuePairs) -> (Handles, Arc<Mutex<FetchCapture>>) {
        let (outgoing, receiver) = Queue::default().split();
        let keepalive = outgoing.clone();
        let active = Arc::new(Mutex::new(HashMap::new()));
        let mut fetch = request(id);
        fetch.params = params;
        let (mut request, recv) =
            FetchRequested::new(None, SessionId::generate(), outgoing, active.clone(), fetch)
                .unwrap();
        let capture = Arc::new(Mutex::new(FetchCapture::default()));
        request.capture = Some(capture.clone());
        (
            Handles {
                request,
                recv,
                _keepalive: keepalive,
                outgoing: receiver,
                active,
            },
            capture,
        )
    }

    fn supplied(
        group_id: u64,
        object_id: u64,
        priority: u8,
        payload: &'static [u8],
    ) -> FetchResponseObject {
        FetchResponseObject {
            object: FetchObject {
                group_id,
                subgroup_id: Some(0),
                object_id,
                publisher_priority: priority,
                extension_headers: Default::default(),
                payload_length: payload.len(),
            },
            // Split each payload so chunking is exercised.
            payload: payload.chunks(2).map(Bytes::from_static).collect(),
        }
    }

    /// Decode a captured FETCH stream into (request id, entries with payloads).
    fn decode_stream(bytes: &[u8]) -> (u64, Vec<(FetchEntry, Vec<u8>)>) {
        use crate::coding::Decode;

        let mut cursor = bytes;
        let header_type = StreamHeaderType::decode(&mut cursor).unwrap();
        assert!(header_type.is_fetch());
        let header = FetchHeader::decode(header_type, &mut cursor).unwrap();

        let mut decoder = FetchObjectDecoder::new();
        let mut entries = Vec::new();
        while !cursor.is_empty() {
            let (entry, consumed) = decoder.decode(cursor).unwrap();
            cursor = &cursor[consumed..];
            let payload = match &entry {
                FetchEntry::Object(object) => {
                    let (payload, rest) = cursor.split_at(object.payload_length);
                    cursor = rest;
                    payload.to_vec()
                }
                FetchEntry::EndOfRange { .. } => Vec::new(),
            };
            entries.push((entry, payload));
        }
        (header.request_id, entries)
    }

    async fn next_message(outgoing: &mut Queue<Message>) -> Message {
        outgoing.pop().await.expect("expected a control message")
    }

    #[tokio::test]
    async fn serves_supplied_objects_in_order_then_fin_then_fetch_ok() {
        let (handles, capture) = handles_with_params(21, KeyValuePairs::default());
        let Handles {
            request,
            recv,
            _keepalive,
            mut outgoing,
            active,
        } = handles;
        active.lock().unwrap().insert(21, recv);
        let objects = vec![
            supplied(5, 0, 240, b"repair-0"),
            supplied(5, 1, 240, b"repair-1"),
            supplied(5, 2, 240, b"r2"),
        ];

        request
            .serve(
                objects.clone(),
                FetchRest::Complete {
                    end_of_track: false,
                    end_location: Location::new(5, 3),
                },
            )
            .await
            .unwrap();

        let capture = capture.lock().unwrap().clone();
        assert!(capture.finished);
        assert_eq!(capture.reset, None);
        let (request_id, entries) = decode_stream(&capture.bytes);
        assert_eq!(request_id, 21);
        let expected: Vec<_> = objects
            .iter()
            .map(|o| {
                (
                    FetchEntry::Object(o.object.clone()),
                    o.payload.concat().to_vec(),
                )
            })
            .collect();
        assert_eq!(entries, expected);

        let Message::FetchOk(ok) = next_message(&mut outgoing).await else {
            panic!("expected FETCH_OK");
        };
        assert_eq!(ok.id, 21);
        assert!(!ok.end_of_track);
        assert_eq!(ok.end_location, Location::new(5, 3));
        assert!(active.lock().unwrap().is_empty());
        assert!(outgoing.close().is_empty());
    }

    #[tokio::test]
    async fn unknown_rest_is_marked_with_end_of_unknown_range() {
        let (handles, capture) = handles_with_params(23, KeyValuePairs::default());
        let Handles {
            request,
            recv: _recv,
            _keepalive,
            mut outgoing,
            active: _,
        } = handles;

        request
            .serve(
                vec![supplied(8, 0, 240, b"a"), supplied(8, 1, 240, b"b")],
                FetchRest::Unknown {
                    last_location: Location::new(8, 9),
                    end_location: Location::new(8, 10),
                },
            )
            .await
            .unwrap();

        let capture = capture.lock().unwrap().clone();
        assert!(capture.finished);
        let (_, entries) = decode_stream(&capture.bytes);
        assert_eq!(entries.len(), 3);
        assert_eq!(
            entries[2].0,
            FetchEntry::EndOfRange {
                kind: FetchEndOfRange::Unknown,
                location: Location::new(8, 9),
            }
        );
        let Message::FetchOk(ok) = next_message(&mut outgoing).await else {
            panic!("expected FETCH_OK");
        };
        assert_eq!(ok.end_location, Location::new(8, 10));
    }

    #[tokio::test]
    async fn empty_response_is_a_header_and_a_fin() {
        let (handles, capture) = handles_with_params(25, KeyValuePairs::default());
        let Handles {
            request,
            recv: _recv,
            _keepalive,
            mut outgoing,
            active: _,
        } = handles;

        request
            .serve(
                Vec::new(),
                FetchRest::Complete {
                    end_of_track: false,
                    end_location: Location::new(3, 0),
                },
            )
            .await
            .unwrap();

        let capture = capture.lock().unwrap().clone();
        assert!(capture.finished);
        assert_eq!(decode_stream(&capture.bytes), (25, Vec::new()));
        assert!(matches!(
            next_message(&mut outgoing).await,
            Message::FetchOk(_)
        ));
    }

    #[tokio::test]
    async fn payload_length_mismatch_resets_and_rejects() {
        let (handles, capture) = handles_with_params(27, KeyValuePairs::default());
        let Handles {
            request,
            recv: _recv,
            _keepalive,
            mut outgoing,
            active: _,
        } = handles;
        let mut object = supplied(1, 0, 128, b"four");
        object.object.payload_length = 5;

        assert!(request
            .serve(
                vec![object],
                FetchRest::Complete {
                    end_of_track: false,
                    end_location: Location::new(1, 1),
                },
            )
            .await
            .is_err());

        let capture = capture.lock().unwrap().clone();
        assert!(!capture.finished);
        assert_eq!(
            capture.reset,
            Some(DataStreamResetCode::InternalError.into())
        );
        let Message::RequestError(error) = next_message(&mut outgoing).await else {
            panic!("expected REQUEST_ERROR");
        };
        assert_eq!(error.id, 27);
        assert_eq!(error.error_code, RequestErrorCode::InternalError as u64);
    }

    #[tokio::test]
    async fn cancelled_fetch_sends_no_response() {
        let (handles, _capture) = handles_with_params(29, KeyValuePairs::default());
        let Handles {
            request,
            mut recv,
            _keepalive,
            outgoing,
            active: _,
        } = handles;
        recv.cancel().unwrap();

        let result = request
            .serve(
                vec![supplied(1, 0, 128, b"x")],
                FetchRest::Complete {
                    end_of_track: false,
                    end_location: Location::new(1, 1),
                },
            )
            .await;

        assert!(matches!(
            result,
            Err(SessionError::Serve(ServeError::Cancel))
        ));
        assert!(outgoing.close().is_empty());
    }

    #[test]
    fn omitted_parameters_use_the_draft_defaults() {
        let Handles { request, .. } = handles(31);
        // §9.2.2.3 and §9.2.2.4.
        assert_eq!(request.subscriber_priority(), 128);
        assert_eq!(request.group_order(), GroupOrder::Ascending);
    }

    #[test]
    fn invalid_parameters_are_rejected_at_construction() {
        let mut fetch = request(33);
        fetch
            .params
            .set_intvalue(message::parameter_type::SUBSCRIBER_PRIORITY, 256);
        assert!(FetchRequested::new(
            None,
            SessionId::generate(),
            Queue::default(),
            Default::default(),
            fetch
        )
        .is_err());
    }

    /// Send order the response stream carried when each Object started.
    fn send_orders_per_object(capture: &FetchCapture) -> Vec<i32> {
        use crate::coding::Decode;

        let mut cursor = &capture.bytes[..];
        let header_type = StreamHeaderType::decode(&mut cursor).unwrap();
        FetchHeader::decode(header_type, &mut cursor).unwrap();
        let mut offset = capture.bytes.len() - cursor.len();

        let mut decoder = FetchObjectDecoder::new();
        let mut orders = Vec::new();
        while offset < capture.bytes.len() {
            let (entry, consumed) = decoder.decode(&capture.bytes[offset..]).unwrap();
            if let FetchEntry::Object(object) = &entry {
                let order = capture
                    .send_orders
                    .iter()
                    .rev()
                    .find(|(applied_at, _)| *applied_at <= offset)
                    .map(|(_, order)| *order)
                    .expect("no send order applied before the object");
                orders.push(order);
                offset += object.payload_length;
            }
            offset += consumed;
        }
        orders
    }

    async fn served_send_orders(params: KeyValuePairs, priorities: &[u8]) -> Vec<i32> {
        let (handles, capture) = handles_with_params(41, params);
        let Handles {
            request,
            recv: _recv,
            _keepalive,
            outgoing: _outgoing,
            active: _,
        } = handles;
        let objects = priorities
            .iter()
            .enumerate()
            .map(|(object_id, priority)| supplied(9, object_id as u64, *priority, b"sym"))
            .collect();
        request
            .serve(
                objects,
                FetchRest::Complete {
                    end_of_track: false,
                    end_location: Location::new(9, priorities.len() as u64),
                },
            )
            .await
            .unwrap();
        let capture = capture.lock().unwrap().clone();
        send_orders_per_object(&capture)
    }

    fn params_with_subscriber_priority(priority: u8) -> KeyValuePairs {
        let mut params = KeyValuePairs::default();
        params.set_subscriber_priority(priority);
        params
    }

    #[tokio::test]
    async fn each_object_is_sent_at_subscriber_then_its_publisher_priority() {
        let orders =
            served_send_orders(params_with_subscriber_priority(64), &[240, 240, 100]).await;
        assert_eq!(
            orders,
            vec![
                send_order(64, 240),
                send_order(64, 240),
                send_order(64, 100)
            ]
        );
    }

    /// The keyframe-repair case of draft-ramadan-moq-fec §11.4.5: a repair
    /// FETCH at a more urgent Subscriber Priority is scheduled ahead of a
    /// concurrent subscription stream of a less urgent track and ahead of a less
    /// urgent FETCH on the same session, in quinn's higher-is-first order.
    #[tokio::test]
    async fn urgent_fetch_outranks_less_urgent_subscription_and_fetch() {
        use crate::session::SubscribeInfo;

        // Repair symbols carry the repair track's Publisher Priority, 240.
        let urgent_fetch = served_send_orders(params_with_subscriber_priority(64), &[240]).await[0];
        let lax_fetch = served_send_orders(params_with_subscriber_priority(200), &[240]).await[0];

        // A subscription to a less urgent track: subscribed at Subscriber
        // Priority 128 (the SUBSCRIBE carries no parameter), Publisher Priority
        // 192.
        let subscribe = message::Subscribe {
            id: 0,
            track_namespace: TrackNamespace::from_utf8_path("test"),
            track_name: "captions".into(),
            params: KeyValuePairs::default(),
        };
        let info = SubscribeInfo::new_from_subscribe(&subscribe).unwrap();
        let subscription = send_order(info.subscriber_priority, 192);

        assert!(urgent_fetch > subscription);
        assert!(urgent_fetch > lax_fetch);
        assert!(subscription > lax_fetch);
    }

    #[tokio::test]
    async fn fetch_without_subscriber_priority_ties_on_publisher_priority() {
        // Omitted Subscriber Priority is 128, the same as a default SUBSCRIBE,
        // so §7.2 falls through to Publisher Priority: the repair object (240)
        // yields to source media (128) of the default subscription.
        let fetch = served_send_orders(KeyValuePairs::default(), &[240]).await[0];
        assert!(send_order(DEFAULT_SUBSCRIBER_PRIORITY, 128) > fetch);
        assert_eq!(send_order(DEFAULT_SUBSCRIBER_PRIORITY, 240), fetch);
    }
}
