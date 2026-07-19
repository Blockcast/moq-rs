// SPDX-FileCopyrightText: 2026 Blockcast Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use thiserror::Error;

use crate::profile::draft19::{
    ControlStreamPair, Frame, RequestStream, RequestStreamRole, Setup, StreamAction,
    StreamErrorCode, StreamProtocolError,
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

#[derive(Debug, Error)]
pub enum Draft19SessionError {
    #[error(transparent)]
    Transport(#[from] SessionError),
    #[error(transparent)]
    Protocol(#[from] StreamProtocolError),
}

/// Operational draft-19 stream core.
///
/// Construction opens the local unidirectional control stream, sends SETUP,
/// accepts the peer's unidirectional control stream, and decodes its SETUP.
/// It intentionally does not advertise `moqt-19`; callers may only construct
/// it after selecting the explicit draft-19 profile out of band.
pub struct Draft19Session {
    session: web_transport::Session,
    control_sender: Writer,
    control_receiver: Reader,
    control_state: ControlStreamPair,
    peer_setup: Setup,
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
            control_sender,
            control_receiver,
            control_state,
            peer_setup,
        })
    }

    pub const fn peer_setup(&self) -> &Setup {
        &self.peer_setup
    }

    pub const fn control_ready(&self) -> bool {
        self.control_state.is_ready()
    }

    pub async fn send_control(&mut self, frame: &Frame) -> Result<(), Draft19SessionError> {
        self.control_sender.encode(frame).await?;
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
        self.control_receiver
            .decode()
            .await
            .map_err(|error| control_error(&self.session, error))
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
            sender: Writer::new(send),
            receiver: Reader::new(recv),
            lifecycle,
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
            sender: Writer::new(send),
            receiver,
            lifecycle,
        })
    }
}

pub struct Draft19RequestStream {
    session: web_transport::Session,
    sender: Writer,
    receiver: Reader,
    lifecycle: RequestStream,
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
        let frame = self
            .receiver
            .decode::<Frame>()
            .await
            .map_err(|error| transport_error(&self.session, error))?;
        if let Err(error) = self.lifecycle.receive_first_response(frame.message_type) {
            return Err(self.protocol_error(error));
        }
        Ok(frame)
    }

    pub async fn send_message(&mut self, frame: &Frame) -> Result<(), Draft19SessionError> {
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
        Ok(Some(
            self.receiver
                .decode()
                .await
                .map_err(|error| transport_error(&self.session, error))?,
        ))
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
}
