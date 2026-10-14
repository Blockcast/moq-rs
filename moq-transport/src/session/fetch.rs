// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! An outbound FETCH and the data stream that answers it (draft-20 §10.13,
//! §5.2).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use tokio::sync::oneshot;

use super::{Reader, Session, SessionError};
use crate::coding::Location;
use crate::data::{FetchItem, FetchStreamState};
use crate::message::{self, GroupOrder, LocationFilter, Message};

/// Requests waiting for their FETCH data stream, keyed by Request ID.
pub(super) type FetchStreams = Arc<Mutex<HashMap<u64, oneshot::Sender<Reader>>>>;

/// Stream reset code CANCELLED (§3.3.4).
pub(super) const CANCELLED: u32 = 0x1;

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    /// The publisher answered REQUEST_ERROR.
    #[error("fetch rejected: code={code} reason={reason}")]
    Rejected { code: u64, reason: String },

    #[error(transparent)]
    Session(#[from] SessionError),
}

enum Data {
    Pending(oneshot::Receiver<Reader>),
    Open(Reader, FetchStreamState),
    Done,
}

/// An outbound FETCH. Dropping it before both the data stream and the
/// request stream have ended cancels the FETCH (§5.2).
pub struct Fetch {
    id: u64,
    session: web_transport::Session,
    streams: FetchStreams,
    /// The response half of the request stream; ours is FINed after FETCH.
    request: Option<Reader>,
    /// Start Location of an Absolute filter, checked against FETCH_OK.
    start: Option<Location>,
    descending: bool,
    data: Data,
    ok: Option<message::FetchOk>,
}

impl Fetch {
    pub(super) fn new(
        session: web_transport::Session,
        streams: FetchStreams,
        msg: &message::Fetch,
        request: Reader,
        data: oneshot::Receiver<Reader>,
    ) -> Result<Self, SessionError> {
        let start = match msg.params.location_filter()? {
            Some(LocationFilter::Absolute { start, .. }) => Some(start),
            _ => None,
        };
        Ok(Self {
            id: msg.id,
            session,
            streams,
            request: Some(request),
            start,
            // §10.2.8: an omitted GROUP_ORDER is Ascending.
            descending: msg.params.group_order()? == Some(GroupOrder::Descending),
            data: Data::Pending(data),
            ok: None,
        })
    }

    pub fn request_id(&self) -> u64 {
        self.id
    }

    /// FETCH_OK, once it has arrived.
    pub fn fetch_ok(&self) -> Option<&message::FetchOk> {
        self.ok.as_ref()
    }

    /// The next Object or End of Range on the data stream, with the Object's
    /// payload (empty for an End of Range). `None` after the stream's FIN.
    pub async fn next(&mut self) -> Result<Option<(FetchItem, Bytes)>, FetchError> {
        loop {
            match &mut self.data {
                Data::Done => return Ok(None),
                Data::Open(reader, state) => {
                    if violation(&self.session, reader.done().await)? {
                        self.data = Data::Done;
                        return Ok(None);
                    }
                    let decoded = reader
                        .decode_with(|r| {
                            let mut next = state.clone();
                            Ok((next.decode(r)?, next))
                        })
                        .await;
                    let (item, next) = violation(&self.session, decoded)?;
                    *state = next;
                    let payload = match &item {
                        FetchItem::Object(object) => violation(
                            &self.session,
                            read_payload(reader, object.payload_length).await,
                        )?,
                        FetchItem::EndOfRange { .. } => Bytes::new(),
                    };
                    return Ok(Some((item, payload)));
                }
                Data::Pending(pending) => {
                    // The data stream and the response arrive in either order.
                    let reader = match &mut self.request {
                        Some(response) if self.ok.is_none() => tokio::select! {
                            reader = pending => reader,
                            response = Session::decode_bidi_response(response, self.id) => {
                                self.recv_response(response)?;
                                continue;
                            }
                        },
                        _ => pending.await,
                    };
                    self.data = match reader {
                        Ok(reader) => Data::Open(reader, FetchStreamState::new(self.descending)),
                        // The registration went away with the session.
                        Err(_) => Data::Done,
                    };
                }
            }
        }
    }

    /// Wait for FETCH_OK, which can arrive before or after the Objects.
    pub async fn ok(&mut self) -> Result<&message::FetchOk, FetchError> {
        if self.ok.is_none() {
            let response = self.request.as_mut().ok_or(SessionError::Internal)?;
            let response = Session::decode_bidi_response(response, self.id).await;
            self.recv_response(response)?;
        }
        Ok(self.ok.as_ref().expect("set above"))
    }

    fn recv_response(&mut self, response: Result<Message, SessionError>) -> Result<(), FetchError> {
        // FETCH_OK and REQUEST_ERROR are each the last message on the stream.
        self.request = None;
        match violation(&self.session, response)? {
            Message::FetchOk(ok) => {
                // §10.14: an End Location before the Start Location.
                if self.start.is_some_and(|start| ok.end_location < start) {
                    return Err(close(
                        &self.session,
                        SessionError::ProtocolViolation(
                            "FETCH_OK End Location precedes the Start Location".to_string(),
                        ),
                    ));
                }
                self.ok = Some(ok);
                Ok(())
            }
            Message::RequestError(err) => {
                self.data = Data::Done;
                Err(FetchError::Rejected {
                    code: err.error_code,
                    reason: err.reason.0,
                })
            }
            other => Err(close(
                &self.session,
                SessionError::ProtocolViolation(format!(
                    "{} on a FETCH request stream",
                    other.name()
                )),
            )),
        }
    }
}

/// A malformed response or data stream closes the session (§11.4).
fn violation<T>(
    session: &web_transport::Session,
    result: Result<T, SessionError>,
) -> Result<T, FetchError> {
    result.map_err(|err| {
        if err.is_session_fatal() {
            close(session, err)
        } else {
            err.into()
        }
    })
}

fn close(session: &web_transport::Session, err: SessionError) -> FetchError {
    let code = u32::try_from(err.code()).unwrap_or(0x1);
    session.close(code, "fetch protocol violation");
    err.into()
}

async fn read_payload(reader: &mut Reader, len: usize) -> Result<Bytes, SessionError> {
    let mut payload = BytesMut::with_capacity(len);
    while payload.len() < len {
        match reader.read_chunk(len - payload.len()).await? {
            Some(chunk) => payload.extend_from_slice(&chunk),
            // A FIN inside an Object (§11.4).
            None => {
                return Err(SessionError::ProtocolViolation(
                    "FETCH stream ended inside an Object".to_string(),
                ))
            }
        }
    }
    Ok(payload.freeze())
}

impl Drop for Fetch {
    fn drop(&mut self) {
        if let Ok(mut streams) = self.streams.lock() {
            streams.remove(&self.id);
        }
        // §5.2: cancelling MUST send STOP_SENDING on the request stream, and
        // MAY on an open data stream.
        if let Some(mut response) = self.request.take() {
            response.stop(CANCELLED);
        }
        if let Data::Open(reader, _) = &mut self.data {
            reader.stop(CANCELLED);
        }
    }
}
