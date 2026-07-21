// SPDX-FileCopyrightText: 2026 Blockcast Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Draft-19 framing and stream-lifecycle foundation.

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use bytes::{Buf, BufMut, Bytes};
use thiserror::Error;

use crate::coding::{Decode, DecodeError, Encode, EncodeError, SessionUri, Vi64};
use crate::profile::WireProfile;

pub const SETUP_TYPE: u64 = 0x2f00;
pub const GOAWAY_TYPE: u64 = 0x10;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u64)]
pub enum SetupOptionType {
    Path = 0x01,
    AuthorizationToken = 0x03,
    MaxAuthTokenCacheSize = 0x04,
    Authority = 0x05,
    MaxFilterRanges = 0x06,
    MoqtImplementation = 0x07,
    MaxRequestUpdates = 0x08,
}

impl SetupOptionType {
    const fn is_known(value: u64) -> bool {
        // AUTHORIZATION_TOKEN (0x03) remains opaque until its nested token
        // structure is implemented; treating it as unknown is safer than
        // accepting malformed values as a recognized option.
        matches!(value, 0x01 | 0x04 | 0x05 | 0x06 | 0x07 | 0x08)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SetupOptionValue {
    Integer(u64),
    Bytes(Vec<u8>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetupOption {
    pub option_type: u64,
    pub value: SetupOptionValue,
}

impl SetupOption {
    pub fn integer(option_type: u64, value: u64) -> Self {
        Self {
            option_type,
            value: SetupOptionValue::Integer(value),
        }
    }

    pub fn bytes(option_type: u64, value: impl Into<Vec<u8>>) -> Self {
        Self {
            option_type,
            value: SetupOptionValue::Bytes(value.into()),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SetupOptions(pub Vec<SetupOption>);

impl SetupOptions {
    fn decode_body<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let mut options = Vec::new();
        let mut previous = 0u64;
        let mut known = HashSet::new();

        while buf.has_remaining() {
            let delta = Vi64::decode(buf)?.into_inner();
            let option_type = previous
                .checked_add(delta)
                .ok_or(DecodeError::KvpTypeOverflow)?;
            if SetupOptionType::is_known(option_type) && !known.insert(option_type) {
                return Err(DecodeError::DuplicateParameter(option_type));
            }

            let value = if option_type.is_multiple_of(2) {
                SetupOptionValue::Integer(Vi64::decode(buf)?.into_inner())
            } else {
                let len = Vi64::decode(buf)?.into_inner();
                let len =
                    usize::try_from(len).map_err(|_| DecodeError::KeyValuePairLengthExceeded())?;
                if len > u16::MAX as usize {
                    return Err(DecodeError::KeyValuePairLengthExceeded());
                }
                <Vi64 as Decode>::decode_remaining(buf, len)?;
                let mut value = vec![0; len];
                buf.copy_to_slice(&mut value);
                SetupOptionValue::Bytes(value)
            };

            options.push(SetupOption { option_type, value });
            previous = option_type;
        }

        Ok(Self(options))
    }

    fn encode_body<W: BufMut>(&self, buf: &mut W) -> Result<(), EncodeError> {
        let mut previous = 0u64;
        let mut known = HashSet::new();

        for option in &self.0 {
            if SetupOptionType::is_known(option.option_type) && !known.insert(option.option_type) {
                return Err(EncodeError::InvalidValue);
            }
            let delta = option
                .option_type
                .checked_sub(previous)
                .ok_or(EncodeError::KvpKeyOrder)?;
            Vi64::new(delta).encode(buf)?;

            match &option.value {
                SetupOptionValue::Integer(value) if option.option_type.is_multiple_of(2) => {
                    Vi64::new(*value).encode(buf)?;
                }
                SetupOptionValue::Bytes(value) if !option.option_type.is_multiple_of(2) => {
                    if value.len() > u16::MAX as usize {
                        return Err(EncodeError::KeyValuePairLengthExceeded);
                    }
                    Vi64::new(value.len() as u64).encode(buf)?;
                    <Vi64 as Encode>::encode_remaining(buf, value.len())?;
                    buf.put_slice(value);
                }
                _ => return Err(EncodeError::InvalidValue),
            }
            previous = option.option_type;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Setup {
    pub options: SetupOptions,
}

impl Decode for Setup {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let message_type = Vi64::decode(buf)?.into_inner();
        if message_type != SETUP_TYPE {
            return Err(DecodeError::InvalidMessage(message_type));
        }
        let len = u16::decode(buf)? as usize;
        <Vi64 as Decode>::decode_remaining(buf, len)?;
        let mut body = buf.copy_to_bytes(len);
        let options = SetupOptions::decode_body(&mut body)?;
        if body.has_remaining() {
            return Err(DecodeError::InvalidMessage(SETUP_TYPE));
        }
        Ok(Self { options })
    }
}

impl Setup {
    pub fn decode_for_profile<B: Buf>(
        profile: WireProfile,
        buf: &mut B,
    ) -> Result<Self, DecodeError> {
        if profile != WireProfile::Draft19 {
            return Err(DecodeError::InvalidMessage(SETUP_TYPE));
        }
        Self::decode(buf)
    }
}

impl Encode for Setup {
    fn encode<W: BufMut>(&self, buf: &mut W) -> Result<(), EncodeError> {
        let mut body = Vec::new();
        self.options.encode_body(&mut body)?;
        if body.len() > u16::MAX as usize {
            return Err(EncodeError::MsgBoundsExceeded);
        }

        Vi64::new(SETUP_TYPE).encode(buf)?;
        (body.len() as u16).encode(buf)?;
        <Vi64 as Encode>::encode_remaining(buf, body.len())?;
        buf.put_slice(&body);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    pub message_type: u64,
    pub payload: Bytes,
}

impl Decode for Frame {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let message_type = Vi64::decode(buf)?.into_inner();
        let len = u16::decode(buf)? as usize;
        <Vi64 as Decode>::decode_remaining(buf, len)?;
        Ok(Self {
            message_type,
            payload: buf.copy_to_bytes(len),
        })
    }
}

impl Frame {
    pub fn decode_for_profile<B: Buf>(
        profile: WireProfile,
        buf: &mut B,
    ) -> Result<Self, DecodeError> {
        if profile != WireProfile::Draft19 {
            return Err(DecodeError::InvalidMessage(SETUP_TYPE));
        }
        Self::decode(buf)
    }
}

impl Encode for Frame {
    fn encode<W: BufMut>(&self, buf: &mut W) -> Result<(), EncodeError> {
        if self.payload.len() > u16::MAX as usize {
            return Err(EncodeError::MsgBoundsExceeded);
        }
        Vi64::new(self.message_type).encode(buf)?;
        (self.payload.len() as u16).encode(buf)?;
        <Vi64 as Encode>::encode_remaining(buf, self.payload.len())?;
        buf.put_slice(&self.payload);
        Ok(())
    }
}

/// Draft-19 GOAWAY body, usable on either a control or request stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoAway {
    pub new_session_uri: SessionUri,
    pub timeout_ms: u64,
}

impl GoAway {
    pub fn into_frame(self) -> Result<Frame, EncodeError> {
        if self.new_session_uri.0.len() > SessionUri::MAX_LEN {
            return Err(EncodeError::FieldBoundsExceeded("SessionUri".to_string()));
        }
        let mut payload = Vec::new();
        Vi64::new(self.new_session_uri.0.len() as u64).encode(&mut payload)?;
        payload.put_slice(self.new_session_uri.0.as_bytes());
        Vi64::new(self.timeout_ms).encode(&mut payload)?;
        Ok(Frame {
            message_type: GOAWAY_TYPE,
            payload: payload.into(),
        })
    }

    pub fn from_frame(frame: &Frame) -> Result<Self, DecodeError> {
        if frame.message_type != GOAWAY_TYPE {
            return Err(DecodeError::InvalidMessage(frame.message_type));
        }

        let mut payload = frame.payload.clone();
        let uri_len = Vi64::decode(&mut payload)?.into_inner();
        let uri_len = usize::try_from(uri_len)
            .map_err(|_| DecodeError::FieldBoundsExceeded("SessionUri".to_string()))?;
        if uri_len > SessionUri::MAX_LEN {
            return Err(DecodeError::FieldBoundsExceeded("SessionUri".to_string()));
        }
        <Vi64 as Decode>::decode_remaining(&mut payload, uri_len)?;
        let mut uri = vec![0; uri_len];
        payload.copy_to_slice(&mut uri);
        let new_session_uri = SessionUri(String::from_utf8(uri)?);
        let timeout_ms = Vi64::decode(&mut payload)?.into_inner();
        if payload.has_remaining() {
            return Err(DecodeError::InvalidLength(
                frame.payload.len(),
                frame.payload.len() - payload.remaining(),
            ));
        }

        Ok(Self {
            new_session_uri,
            timeout_ms,
        })
    }
}

/// GOAWAY tracking is deliberately per stream so request migration cannot
/// consume the control stream's single-GOAWAY allowance (or vice versa).
#[derive(Debug, Default)]
pub struct GoAwayState {
    sent: bool,
    received: bool,
    deadline: Option<Instant>,
}

impl GoAwayState {
    pub const fn sent(&self) -> bool {
        self.sent
    }

    pub const fn received(&self) -> bool {
        self.received
    }

    pub const fn active(&self) -> bool {
        self.sent || self.received
    }

    pub fn record_sent(
        &mut self,
        goaway: &GoAway,
        now: Instant,
    ) -> Result<(), StreamProtocolError> {
        if self.sent {
            return Err(StreamProtocolError::DuplicateGoAway);
        }
        self.sent = true;
        self.deadline = (goaway.timeout_ms != 0)
            .then(|| now.checked_add(Duration::from_millis(goaway.timeout_ms)))
            .flatten();
        Ok(())
    }

    pub fn record_received(&mut self) -> Result<(), StreamProtocolError> {
        if self.received {
            return Err(StreamProtocolError::DuplicateGoAway);
        }
        self.received = true;
        Ok(())
    }

    pub fn timeout_expired(&self, now: Instant) -> bool {
        self.deadline.is_some_and(|deadline| now >= deadline)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u64)]
pub enum SessionErrorCode {
    NoError = 0x0,
    InternalError = 0x1,
    Unauthorized = 0x2,
    ProtocolViolation = 0x3,
    KeyValueFormattingError = 0x6,
    GoAwayTimeout = 0x10,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u64)]
pub enum StreamErrorCode {
    InternalError = 0x00,
    Cancelled = 0x01,
    DeliveryTimeout = 0x02,
    SessionClosed = 0x03,
    GoingAway = 0x04,
    TooFarBehind = 0x05,
    UnknownObjectStatus = 0x06,
    ExpiredAuthToken = 0x07,
    ExcessiveLoad = 0x09,
    MalformedTrack = 0x12,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum StreamProtocolError {
    #[error("wire profile {0:?} cannot decode draft-19 framing")]
    ProfileMismatch(WireProfile),
    #[error("invalid first request message {message_type:#x}")]
    InvalidFirstMessage { message_type: u64 },
    #[error("request stream received an invalid first response {message_type:#x}")]
    InvalidFirstResponse { message_type: u64 },
    #[error("stream finished before its required messages")]
    PrematureFin,
    #[error("invalid stream transition")]
    InvalidTransition,
    #[error("control stream closed")]
    ControlStreamClosed,
    #[error("received or sent multiple GOAWAY messages on one stream")]
    DuplicateGoAway,
}

impl StreamProtocolError {
    pub const fn session_code(&self) -> Option<SessionErrorCode> {
        match self {
            Self::ProfileMismatch(_)
            | Self::InvalidFirstMessage { .. }
            | Self::InvalidFirstResponse { .. }
            | Self::ControlStreamClosed
            | Self::DuplicateGoAway => Some(SessionErrorCode::ProtocolViolation),
            Self::PrematureFin | Self::InvalidTransition => None,
        }
    }

    pub const fn stream_code(&self) -> Option<StreamErrorCode> {
        match self {
            Self::PrematureFin => Some(StreamErrorCode::Cancelled),
            Self::InvalidTransition => Some(StreamErrorCode::InternalError),
            _ => None,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ControlStreamPair {
    local_setup_sent: bool,
    remote_setup_received: bool,
}

impl ControlStreamPair {
    pub fn new(profile: WireProfile) -> Result<Self, StreamProtocolError> {
        if profile != WireProfile::Draft19 {
            return Err(StreamProtocolError::ProfileMismatch(profile));
        }
        Ok(Self {
            local_setup_sent: false,
            remote_setup_received: false,
        })
    }

    pub fn sent_setup(&mut self) -> Result<(), StreamProtocolError> {
        if self.local_setup_sent {
            return Err(StreamProtocolError::InvalidTransition);
        }
        self.local_setup_sent = true;
        Ok(())
    }

    pub fn received_setup(&mut self) -> Result<(), StreamProtocolError> {
        if self.remote_setup_received {
            return Err(StreamProtocolError::InvalidTransition);
        }
        self.remote_setup_received = true;
        Ok(())
    }

    pub const fn is_ready(&self) -> bool {
        self.local_setup_sent && self.remote_setup_received
    }

    pub const fn on_fin_or_reset(&self) -> Result<(), StreamProtocolError> {
        Err(StreamProtocolError::ControlStreamClosed)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RequestKind {
    Subscribe,
    Publish,
    Fetch,
    TrackStatus,
    PublishNamespace,
    SubscribeNamespace,
    SubscribeTracks,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RequestStreamRole {
    Requester,
    Responder,
}

impl RequestKind {
    pub fn from_first_message(message_type: u64) -> Result<Self, StreamProtocolError> {
        match message_type {
            0x03 => Ok(Self::Subscribe),
            0x1d => Ok(Self::Publish),
            0x16 => Ok(Self::Fetch),
            0x0d => Ok(Self::TrackStatus),
            0x06 => Ok(Self::PublishNamespace),
            0x50 => Ok(Self::SubscribeNamespace),
            0x51 => Ok(Self::SubscribeTracks),
            _ => Err(StreamProtocolError::InvalidFirstMessage { message_type }),
        }
    }

    const fn success_response(self) -> u64 {
        match self {
            Self::Subscribe => 0x04,
            Self::Fetch => 0x18,
            // Draft-19 uses REQUEST_OK (0x07) for the remaining request families.
            _ => 0x07,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum SendState {
    Open,
    Finished,
    Reset(StreamErrorCode),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ReceiveState {
    Open,
    Finished,
    StopSent(StreamErrorCode),
    Reset(StreamErrorCode),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum StreamAction {
    None,
    Reset(StreamErrorCode),
    StopSending(StreamErrorCode),
    ResetAndStop(StreamErrorCode),
}

#[derive(Debug, Eq, PartialEq)]
pub struct RequestStream {
    kind: RequestKind,
    role: RequestStreamRole,
    send: SendState,
    receive: ReceiveState,
    first_response_complete: bool,
    send_required_before_fin: bool,
    receive_required_before_fin: bool,
}

impl RequestStream {
    pub fn new(profile: WireProfile, first_message_type: u64) -> Result<Self, StreamProtocolError> {
        Self::new_for_role(profile, first_message_type, RequestStreamRole::Requester)
    }

    pub fn new_for_role(
        profile: WireProfile,
        first_message_type: u64,
        role: RequestStreamRole,
    ) -> Result<Self, StreamProtocolError> {
        if profile != WireProfile::Draft19 {
            return Err(StreamProtocolError::ProfileMismatch(profile));
        }
        let kind = RequestKind::from_first_message(first_message_type)?;
        Ok(Self {
            kind,
            role,
            send: SendState::Open,
            receive: ReceiveState::Open,
            first_response_complete: false,
            send_required_before_fin: match role {
                RequestStreamRole::Requester => kind == RequestKind::Publish,
                RequestStreamRole::Responder => true,
            },
            receive_required_before_fin: match role {
                RequestStreamRole::Requester => true,
                RequestStreamRole::Responder => kind == RequestKind::Publish,
            },
        })
    }

    pub const fn kind(&self) -> RequestKind {
        self.kind
    }

    pub const fn role(&self) -> RequestStreamRole {
        self.role
    }

    pub const fn first_response_complete(&self) -> bool {
        self.first_response_complete
    }

    pub const fn can_send_followup(&self) -> bool {
        matches!(self.role, RequestStreamRole::Requester) || self.first_response_complete
    }

    pub const fn can_receive_followup(&self) -> bool {
        matches!(self.role, RequestStreamRole::Responder) || self.first_response_complete
    }

    pub fn receive_first_response(&mut self, message_type: u64) -> Result<(), StreamProtocolError> {
        if self.role != RequestStreamRole::Requester || self.first_response_complete {
            return Err(StreamProtocolError::InvalidTransition);
        }
        let successful = message_type == self.kind.success_response();
        if message_type != 0x05 && !successful {
            return Err(StreamProtocolError::InvalidFirstResponse { message_type });
        }
        self.first_response_complete = true;
        self.receive_required_before_fin = successful && self.kind == RequestKind::Subscribe;
        Ok(())
    }

    pub fn send_first_response(&mut self, message_type: u64) -> Result<(), StreamProtocolError> {
        if self.role != RequestStreamRole::Responder || self.first_response_complete {
            return Err(StreamProtocolError::InvalidTransition);
        }
        let successful = message_type == self.kind.success_response();
        if message_type != 0x05 && !successful {
            return Err(StreamProtocolError::InvalidFirstResponse { message_type });
        }
        self.first_response_complete = true;
        self.send_required_before_fin = successful && self.kind == RequestKind::Subscribe;
        Ok(())
    }

    pub fn send_publish_done(&mut self) -> Result<(), StreamProtocolError> {
        let local_is_publisher = matches!(
            (self.role, self.kind),
            (RequestStreamRole::Requester, RequestKind::Publish)
                | (RequestStreamRole::Responder, RequestKind::Subscribe)
        );
        if !self.first_response_complete || !local_is_publisher || !self.send_required_before_fin {
            return Err(StreamProtocolError::InvalidTransition);
        }
        self.send_required_before_fin = false;
        Ok(())
    }

    pub fn receive_publish_done(&mut self) -> Result<(), StreamProtocolError> {
        let remote_is_publisher = matches!(
            (self.role, self.kind),
            (RequestStreamRole::Requester, RequestKind::Subscribe)
                | (RequestStreamRole::Responder, RequestKind::Publish)
        );
        if !self.first_response_complete
            || !remote_is_publisher
            || !self.receive_required_before_fin
        {
            return Err(StreamProtocolError::InvalidTransition);
        }
        self.receive_required_before_fin = false;
        Ok(())
    }

    pub fn receive_fin(&mut self) -> Result<(), StreamProtocolError> {
        if self.receive != ReceiveState::Open {
            return Err(StreamProtocolError::InvalidTransition);
        }
        if self.receive_required_before_fin {
            return Err(StreamProtocolError::PrematureFin);
        }
        self.receive = ReceiveState::Finished;
        Ok(())
    }

    pub fn finish_sending(&mut self) -> Result<(), StreamProtocolError> {
        if self.send != SendState::Open {
            return Err(StreamProtocolError::InvalidTransition);
        }
        if self.send_required_before_fin {
            return Err(StreamProtocolError::PrematureFin);
        }
        self.send = SendState::Finished;
        Ok(())
    }

    pub fn finish_sending_for_goaway(&mut self) -> Result<(), StreamProtocolError> {
        if self.send != SendState::Open {
            return Err(StreamProtocolError::InvalidTransition);
        }
        self.send_required_before_fin = false;
        self.send = SendState::Finished;
        Ok(())
    }

    pub fn reset_sending(
        &mut self,
        code: StreamErrorCode,
    ) -> Result<StreamAction, StreamProtocolError> {
        if self.send != SendState::Open {
            return Err(StreamProtocolError::InvalidTransition);
        }
        self.send = SendState::Reset(code);
        Ok(StreamAction::Reset(code))
    }

    pub fn receive_stop_sending(
        &mut self,
        code: StreamErrorCode,
    ) -> Result<StreamAction, StreamProtocolError> {
        match self.send {
            SendState::Open => {
                self.send = SendState::Reset(code);
                Ok(StreamAction::Reset(code))
            }
            SendState::Finished => Ok(StreamAction::None),
            SendState::Reset(_) => Err(StreamProtocolError::InvalidTransition),
        }
    }

    pub fn receive_reset(
        &mut self,
        code: StreamErrorCode,
    ) -> Result<StreamAction, StreamProtocolError> {
        match self.receive {
            ReceiveState::Open | ReceiveState::StopSent(_) => {
                self.receive = ReceiveState::Reset(code);
                Ok(StreamAction::None)
            }
            ReceiveState::Finished => Ok(StreamAction::None),
            ReceiveState::Reset(_) => Err(StreamProtocolError::InvalidTransition),
        }
    }

    pub fn cancel(&mut self, code: StreamErrorCode) -> Result<StreamAction, StreamProtocolError> {
        match (self.send, self.receive) {
            (SendState::Open, ReceiveState::Open) => {
                self.send = SendState::Reset(code);
                self.receive = ReceiveState::StopSent(code);
                Ok(StreamAction::ResetAndStop(code))
            }
            (SendState::Finished, ReceiveState::Open) => {
                self.receive = ReceiveState::StopSent(code);
                Ok(StreamAction::StopSending(code))
            }
            (SendState::Open, ReceiveState::Finished) => {
                self.send = SendState::Reset(code);
                Ok(StreamAction::Reset(code))
            }
            _ => Err(StreamProtocolError::InvalidTransition),
        }
    }
}
