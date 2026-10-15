// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use crate::{
    coding::SessionUri,
    message::{self, Message},
    watch::Queue,
};

use super::SessionError;

/// Sends this session's control-stream GOAWAY (draft-20 §3.6, §10.4).
///
/// Cloneable so an application can keep one per session (a relay drain, for
/// example) while the session runs. A session sends at most one: a peer that
/// receives a second must close with PROTOCOL_VIOLATION.
#[derive(Clone)]
pub struct GoAwaySender {
    outgoing: Queue<Message>,
    sent: Arc<AtomicBool>,
    server: bool,
}

impl GoAwaySender {
    pub(super) fn new(outgoing: Queue<Message>, server: bool) -> Self {
        Self {
            outgoing,
            sent: Default::default(),
            server,
        }
    }

    /// Queue GOAWAY carrying `uri` (empty reuses the current URI) and the
    /// `timeout_ms` this side will wait before closing with GOAWAY_TIMEOUT.
    ///
    /// Returns `Ok(false)` without sending if this session already sent one.
    pub fn send(&self, uri: &str, timeout_ms: u64) -> Result<bool, SessionError> {
        if !self.server && !uri.is_empty() {
            return Err(SessionError::ProtocolViolation(
                "a client GOAWAY must carry an empty New Session URI".to_string(),
            ));
        }
        if uri.len() > SessionUri::MAX_LEN {
            return Err(SessionError::ProtocolViolation(
                "GOAWAY New Session URI exceeds 8192 bytes".to_string(),
            ));
        }
        if self.sent.swap(true, Ordering::SeqCst) {
            return Ok(false);
        }
        self.outgoing
            .clone()
            .push(Message::GoAway(message::GoAway {
                uri: SessionUri(uri.to_string()),
                timeout: timeout_ms,
            }))
            .map_err(|_| SessionError::Internal)?;
        Ok(true)
    }
}
