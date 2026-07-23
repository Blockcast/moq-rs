// SPDX-FileCopyrightText: 2026 Blockcast Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::time::Instant;

use thiserror::Error;

use crate::profile::draft19::{
    ControlStreamPair, Frame, GoAway, GoAwayState, RequestStream, RequestStreamRole,
    SessionErrorCode, Setup, StreamAction, StreamErrorCode, StreamProtocolError, GOAWAY_TYPE,
};
use crate::profile::WireProfile;

use super::{Reader, SessionError, Writer};

fn transport_error(session: &web_transport::Session, error: SessionError) -> Draft19SessionError {
    if matches!(error, SessionError::Decode(_)) {
        session.close(
            crate::profile::draft19::SessionErrorCode::ProtocolViolation as u32,
            &error.to_string(),
        );
    }
    error.into()
}

fn control_error(session: &web_transport::Session, error: SessionError) -> Draft19SessionError {
    session.close(
        crate::profile::draft19::SessionErrorCode::ProtocolViolation as u32,
        &error.to_string(),
    );
    error.into()
}

fn first_response_fin_error(lifecycle: &mut RequestStream) -> StreamProtocolError {
    match lifecycle.receive_fin() {
        Ok(()) => StreamProtocolError::InvalidTransition,
        Err(error) => error,
    }
}

fn request_goaway_timeout_action(
    goaway: &GoAwayState,
    lifecycle: &mut RequestStream,
    now: Instant,
) -> Result<Option<StreamAction>, StreamProtocolError> {
    if !goaway.timeout_expired(now) {
        return Ok(None);
    }

    match lifecycle.reset_sending(StreamErrorCode::GoingAway) {
        Ok(action) => Ok(Some(action)),
        Err(StreamProtocolError::InvalidTransition) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Pure decision for control-stream GOAWAY timeout enforcement: yields the
/// session error code the sender closes with once its advertised timeout has
/// elapsed while requests are still open, or `None` to leave the session open.
/// Mirrors `request_goaway_timeout_action` so the close decision is testable
/// without a live transport.
fn control_goaway_timeout_action(
    goaway: &GoAwayState,
    now: Instant,
    has_open_requests: bool,
) -> Option<SessionErrorCode> {
    (has_open_requests && goaway.timeout_expired(now)).then_some(SessionErrorCode::GoAwayTimeout)
}

#[derive(Debug, Error)]
pub enum Draft19SessionError {
    #[error(transparent)]
    Transport(#[from] SessionError),
    #[error(transparent)]
    Protocol(#[from] StreamProtocolError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Draft19SessionRole {
    Client,
    Server,
}

impl Draft19SessionRole {
    pub fn validate_received_goaway(self, goaway: &GoAway) -> Result<(), StreamProtocolError> {
        if self == Self::Server && !goaway.new_session_uri.0.is_empty() {
            return Err(StreamProtocolError::ServerGoAwayWithNewSessionUri);
        }
        Ok(())
    }
}

/// Operational draft-19 stream core.
///
/// Construction opens the local unidirectional control stream, sends SETUP,
/// accepts the peer's unidirectional control stream, and decodes its SETUP.
/// Callers may only construct it after exact transport negotiation selected
/// the explicit draft-19 profile.
pub struct Draft19Session {
    session: web_transport::Session,
    role: Draft19SessionRole,
    control_sender: Writer,
    control_receiver: Reader,
    control_state: ControlStreamPair,
    control_goaway: GoAwayState,
    peer_setup: Setup,
    selected_version: WireProfile,
}

impl Draft19Session {
    fn protocol_error(&self, error: StreamProtocolError) -> Draft19SessionError {
        if let Some(code) = error.session_code() {
            self.session.close(code as u32, &error.to_string());
        }
        error.into()
    }

    pub async fn establish(
        session: web_transport::Session,
        role: Draft19SessionRole,
        profile: WireProfile,
        local_setup: Setup,
    ) -> Result<Self, Draft19SessionError> {
        let mut control_state = ControlStreamPair::new(profile)?;

        let mut control_sender = Writer::new(session.open_uni().await.map_err(SessionError::from)?);
        control_sender.encode(&local_setup).await?;
        control_state.sent_setup()?;

        let mut control_receiver =
            Reader::new(session.accept_uni().await.map_err(SessionError::from)?);
        let peer_setup = control_receiver
            .decode::<Setup>()
            .await
            .map_err(|error| control_error(&session, error))?;
        control_state.received_setup()?;

        Ok(Self {
            session,
            role,
            control_sender,
            control_receiver,
            control_state,
            control_goaway: GoAwayState::default(),
            peer_setup,
            selected_version: profile,
        })
    }

    pub const fn peer_setup(&self) -> &Setup {
        &self.peer_setup
    }

    pub const fn selected_version(&self) -> WireProfile {
        self.selected_version
    }

    pub const fn control_ready(&self) -> bool {
        self.control_state.is_ready()
    }

    pub async fn send_control(&mut self, frame: &Frame) -> Result<(), Draft19SessionError> {
        let goaway = (frame.message_type == GOAWAY_TYPE)
            .then(|| GoAway::from_frame(frame))
            .transpose()
            .map_err(SessionError::from)?;
        if goaway.is_some() && self.control_goaway.sent() {
            return Err(self.protocol_error(StreamProtocolError::DuplicateGoAway));
        }
        self.control_sender.encode(frame).await?;
        if let Some(goaway) = goaway {
            self.control_goaway.record_sent(&goaway, Instant::now())?;
        }
        Ok(())
    }

    pub async fn receive_control(&mut self) -> Result<Frame, Draft19SessionError> {
        if self
            .control_receiver
            .done()
            .await
            .map_err(|error| control_error(&self.session, error))?
        {
            return Err(self.protocol_error(StreamProtocolError::ControlStreamClosed));
        }
        let frame: Frame = self
            .control_receiver
            .decode()
            .await
            .map_err(|error| control_error(&self.session, error))?;
        if frame.message_type == GOAWAY_TYPE {
            let goaway = GoAway::from_frame(&frame)
                .map_err(SessionError::from)
                .map_err(|error| control_error(&self.session, error))?;
            if let Err(error) = self.role.validate_received_goaway(&goaway) {
                return Err(self.protocol_error(error));
            }
            if let Err(error) = self.control_goaway.record_received() {
                return Err(self.protocol_error(error));
            }
        }
        Ok(frame)
    }

    pub fn control_goaway_timeout_expired(&self, now: Instant) -> bool {
        self.control_goaway.timeout_expired(now)
    }

    pub fn enforce_control_goaway_timeout(&self, now: Instant, has_open_requests: bool) -> bool {
        match control_goaway_timeout_action(&self.control_goaway, now, has_open_requests) {
            Some(code) => {
                self.session
                    .close(code as u32, "GOAWAY timeout elapsed with open requests");
                true
            }
            None => false,
        }
    }

    pub fn close_after_goaway(&self) -> Result<(), Draft19SessionError> {
        if !self.control_goaway.active() {
            return Err(StreamProtocolError::InvalidTransition.into());
        }
        self.session.close(
            crate::profile::draft19::SessionErrorCode::NoError as u32,
            "GOAWAY graceful close",
        );
        Ok(())
    }

    pub async fn open_request(
        &self,
        first: Frame,
    ) -> Result<Draft19RequestStream, Draft19SessionError> {
        let lifecycle = RequestStream::new_for_role(
            WireProfile::Draft19,
            first.message_type,
            RequestStreamRole::Requester,
        )?;
        let (send, recv) = self.session.open_bi().await.map_err(SessionError::from)?;
        let mut stream = Draft19RequestStream {
            session: self.session.clone(),
            role: self.role,
            sender: Writer::new(send),
            receiver: Reader::new(recv),
            lifecycle,
            goaway: GoAwayState::default(),
        };
        stream.sender.encode(&first).await?;
        Ok(stream)
    }

    pub async fn accept_request(&self) -> Result<Draft19RequestStream, Draft19SessionError> {
        let (send, recv) = self.session.accept_bi().await.map_err(SessionError::from)?;
        let mut receiver = Reader::new(recv);
        let first = receiver
            .decode::<Frame>()
            .await
            .map_err(|error| transport_error(&self.session, error))?;
        let lifecycle = RequestStream::new_for_role(
            WireProfile::Draft19,
            first.message_type,
            RequestStreamRole::Responder,
        )
        .map_err(|error| self.protocol_error(error))?;
        Ok(Draft19RequestStream {
            session: self.session.clone(),
            role: self.role,
            sender: Writer::new(send),
            receiver,
            lifecycle,
            goaway: GoAwayState::default(),
        })
    }
}

pub struct Draft19RequestStream {
    session: web_transport::Session,
    role: Draft19SessionRole,
    sender: Writer,
    receiver: Reader,
    lifecycle: RequestStream,
    goaway: GoAwayState,
}

impl Draft19RequestStream {
    fn protocol_error(&self, error: StreamProtocolError) -> Draft19SessionError {
        if let Some(code) = error.session_code() {
            self.session.close(code as u32, &error.to_string());
        }
        error.into()
    }

    pub const fn lifecycle(&self) -> &RequestStream {
        &self.lifecycle
    }

    pub async fn send_first_response(&mut self, frame: &Frame) -> Result<(), Draft19SessionError> {
        if frame.message_type == GOAWAY_TYPE {
            return self
                .send_goaway(GoAway::from_frame(frame).map_err(SessionError::from)?)
                .await;
        }
        if let Err(error) = self.lifecycle.send_first_response(frame.message_type) {
            return Err(self.protocol_error(error));
        }
        self.sender.encode(frame).await?;
        Ok(())
    }

    pub async fn receive_first_response(&mut self) -> Result<Frame, Draft19SessionError> {
        if self.receiver.done().await? {
            let error = first_response_fin_error(&mut self.lifecycle);
            return Err(self.protocol_error(error));
        }
        let frame: Frame = self
            .receiver
            .decode::<Frame>()
            .await
            .map_err(|error| transport_error(&self.session, error))?;
        if frame.message_type == GOAWAY_TYPE {
            let goaway = GoAway::from_frame(&frame)
                .map_err(SessionError::from)
                .map_err(|error| transport_error(&self.session, error))?;
            if let Err(error) = self.role.validate_received_goaway(&goaway) {
                return Err(self.protocol_error(error));
            }
            if let Err(error) = self.goaway.record_received() {
                return Err(self.protocol_error(error));
            }
            return Ok(frame);
        }
        if let Err(error) = self.lifecycle.receive_first_response(frame.message_type) {
            return Err(self.protocol_error(error));
        }
        Ok(frame)
    }

    pub async fn send_message(&mut self, frame: &Frame) -> Result<(), Draft19SessionError> {
        if frame.message_type == GOAWAY_TYPE {
            return self
                .send_goaway(GoAway::from_frame(frame).map_err(SessionError::from)?)
                .await;
        }
        if !self.lifecycle.can_send_followup() {
            return Err(
                self.protocol_error(StreamProtocolError::InvalidFirstResponse {
                    message_type: frame.message_type,
                }),
            );
        }
        self.sender.encode(frame).await?;
        Ok(())
    }

    pub async fn receive_message(&mut self) -> Result<Option<Frame>, Draft19SessionError> {
        if !self.lifecycle.can_receive_followup() {
            return Err(
                self.protocol_error(StreamProtocolError::InvalidFirstResponse { message_type: 0 })
            );
        }
        if self.receiver.done().await? {
            self.lifecycle.receive_fin()?;
            return Ok(None);
        }
        let frame: Frame = self
            .receiver
            .decode()
            .await
            .map_err(|error| transport_error(&self.session, error))?;
        if frame.message_type == GOAWAY_TYPE {
            let goaway = GoAway::from_frame(&frame)
                .map_err(SessionError::from)
                .map_err(|error| transport_error(&self.session, error))?;
            if let Err(error) = self.role.validate_received_goaway(&goaway) {
                return Err(self.protocol_error(error));
            }
            if let Err(error) = self.goaway.record_received() {
                return Err(self.protocol_error(error));
            }
        }
        Ok(Some(frame))
    }

    pub async fn send_publish_done(&mut self, frame: &Frame) -> Result<(), Draft19SessionError> {
        if frame.message_type != 0x0b {
            return Err(StreamProtocolError::InvalidTransition.into());
        }
        if let Err(error) = self.lifecycle.send_publish_done() {
            return Err(self.protocol_error(error));
        }
        self.sender.encode(frame).await?;
        Ok(())
    }

    pub async fn receive_publish_done(&mut self) -> Result<Frame, Draft19SessionError> {
        let frame = self
            .receive_message()
            .await?
            .ok_or(StreamProtocolError::PrematureFin)?;
        if frame.message_type != 0x0b {
            return Err(StreamProtocolError::InvalidTransition.into());
        }
        if let Err(error) = self.lifecycle.receive_publish_done() {
            return Err(self.protocol_error(error));
        }
        Ok(frame)
    }

    pub fn finish_sending(&mut self) -> Result<(), Draft19SessionError> {
        self.lifecycle.finish_sending()?;
        self.sender.finish()?;
        Ok(())
    }

    pub async fn send_goaway(&mut self, goaway: GoAway) -> Result<(), Draft19SessionError> {
        if self.goaway.sent() {
            return Err(self.protocol_error(StreamProtocolError::DuplicateGoAway));
        }
        let frame = goaway.clone().into_frame().map_err(SessionError::from)?;
        self.sender.encode(&frame).await?;
        self.goaway.record_sent(&goaway, Instant::now())?;
        Ok(())
    }

    pub fn finish_after_goaway(&mut self) -> Result<(), Draft19SessionError> {
        if !self.goaway.active() {
            return Err(StreamProtocolError::InvalidTransition.into());
        }
        self.lifecycle.finish_sending_for_goaway()?;
        self.sender.finish()?;
        Ok(())
    }

    pub fn enforce_goaway_timeout(&mut self, now: Instant) -> Result<bool, Draft19SessionError> {
        match request_goaway_timeout_action(&self.goaway, &mut self.lifecycle, now)? {
            None => return Ok(false),
            Some(StreamAction::Reset(code)) => self.sender.reset(code as u32),
            Some(_) => unreachable!("reset_sending always returns a reset action"),
        }
        Ok(true)
    }

    pub fn cancel(&mut self, code: StreamErrorCode) -> Result<(), Draft19SessionError> {
        match self.lifecycle.cancel(code)? {
            StreamAction::Reset(code) => self.sender.reset(code as u32),
            StreamAction::StopSending(code) => self.receiver.stop(code as u32),
            StreamAction::ResetAndStop(code) => {
                self.sender.reset(code as u32);
                self.receiver.stop(code as u32);
            }
            StreamAction::None => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn active_goaway(now: Instant) -> GoAwayState {
        let mut goaway = GoAwayState::default();
        goaway
            .record_sent(
                &GoAway {
                    new_session_uri: crate::coding::SessionUri(String::new()),
                    timeout_ms: 1,
                },
                now,
            )
            .unwrap();
        goaway
    }

    #[test]
    fn first_response_fin_is_an_error_before_and_after_a_response() {
        let mut waiting = RequestStream::new(WireProfile::Draft19, 0x16).unwrap();
        assert_eq!(
            first_response_fin_error(&mut waiting),
            StreamProtocolError::PrematureFin
        );

        let mut complete = RequestStream::new(WireProfile::Draft19, 0x16).unwrap();
        complete.receive_first_response(0x18).unwrap();
        assert_eq!(
            first_response_fin_error(&mut complete),
            StreamProtocolError::InvalidTransition
        );
    }

    #[test]
    fn request_goaway_timeout_enforcement_is_idempotent() {
        let now = Instant::now();
        let goaway = active_goaway(now);
        let expired = now + Duration::from_millis(1);
        let mut lifecycle = RequestStream::new(WireProfile::Draft19, 0x03).unwrap();

        assert_eq!(
            request_goaway_timeout_action(&goaway, &mut lifecycle, expired).unwrap(),
            Some(StreamAction::Reset(StreamErrorCode::GoingAway))
        );
        assert_eq!(
            request_goaway_timeout_action(&goaway, &mut lifecycle, expired).unwrap(),
            None
        );
    }

    #[test]
    fn request_goaway_timeout_after_early_finish_is_a_noop() {
        let now = Instant::now();
        let goaway = active_goaway(now);
        let mut lifecycle = RequestStream::new(WireProfile::Draft19, 0x03).unwrap();
        lifecycle.finish_sending_for_goaway().unwrap();

        assert_eq!(
            request_goaway_timeout_action(&goaway, &mut lifecycle, now + Duration::from_millis(1),)
                .unwrap(),
            None
        );
    }

    #[test]
    fn control_goaway_timeout_closes_with_goaway_timeout_only_after_expiry_with_open_requests() {
        let now = Instant::now();
        let goaway = active_goaway(now); // timeout_ms: 1
        let expired = now + Duration::from_millis(1);

        // Not yet expired: no close even with open requests.
        assert_eq!(control_goaway_timeout_action(&goaway, now, true), None);
        // Expired but nothing left to drain: no close.
        assert_eq!(control_goaway_timeout_action(&goaway, expired, false), None);
        // Expired with open requests: force close, and specifically with
        // GOAWAY_TIMEOUT (not any other session code).
        assert_eq!(
            control_goaway_timeout_action(&goaway, expired, true),
            Some(SessionErrorCode::GoAwayTimeout)
        );
    }
}
