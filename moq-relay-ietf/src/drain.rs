// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Relay-wide drain: one operator action sends GOAWAY to every session.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use moq_transport::session::GoAwaySender;

/// GOAWAY_TIMEOUT (draft-20 §3.5): the peer did not close in time.
const GOAWAY_TIMEOUT: u32 = 0x10;

/// The GOAWAY a draining relay sends.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DrainState {
    pub new_session_uri: String,
    pub timeout_ms: u64,
}

#[derive(Default)]
struct Inner {
    next_id: u64,
    sessions: HashMap<u64, (GoAwaySender, web_transport::Session)>,
    state: Option<DrainState>,
}

/// Tracks live sessions so a drain can reach all of them.
///
/// Draining is one-way for the life of the process: the relay is being
/// replaced, and a session accepted after the drain gets GOAWAY at once.
#[derive(Clone, Default)]
pub struct Drain {
    inner: Arc<Mutex<Inner>>,
}

/// Unregisters a session when its handler ends.
pub struct DrainGuard {
    id: u64,
    inner: Arc<Mutex<Inner>>,
}

impl Drop for DrainGuard {
    fn drop(&mut self) {
        self.inner.lock().unwrap().sessions.remove(&self.id);
    }
}

impl Drain {
    /// Register an accepted session. GOAWAY goes out now if already draining.
    pub fn register(&self, go_away: GoAwaySender, conn: web_transport::Session) -> DrainGuard {
        let mut inner = self.inner.lock().unwrap();
        let id = inner.next_id;
        inner.next_id += 1;
        if let Some(state) = inner.state.clone() {
            send(&go_away, conn.clone(), &state);
        }
        inner.sessions.insert(id, (go_away, conn));
        DrainGuard {
            id,
            inner: self.inner.clone(),
        }
    }

    /// Send GOAWAY to every live session; returns how many were reached.
    ///
    /// A second call keeps the first target: each session sends one GOAWAY.
    pub fn start(&self, state: DrainState) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let state = inner.state.get_or_insert(state).clone();
        inner
            .sessions
            .values()
            .filter(|(go_away, conn)| send(go_away, conn.clone(), &state))
            .count()
    }

    pub fn state(&self) -> Option<DrainState> {
        self.inner.lock().unwrap().state.clone()
    }

    pub fn session_count(&self) -> usize {
        self.inner.lock().unwrap().sessions.len()
    }
}

/// Queue GOAWAY and, for a non-zero timeout, close with GOAWAY_TIMEOUT once it
/// passes (§3.6). Closing a connection the client already left is a no-op.
fn send(go_away: &GoAwaySender, conn: web_transport::Session, state: &DrainState) -> bool {
    match go_away.send(&state.new_session_uri, state.timeout_ms) {
        Ok(true) => {
            if state.timeout_ms > 0 {
                let timeout = Duration::from_millis(state.timeout_ms);
                tokio::spawn(async move {
                    tokio::time::sleep(timeout).await;
                    conn.close(GOAWAY_TIMEOUT, "GOAWAY timeout");
                });
            }
            true
        }
        Ok(false) => false,
        Err(err) => {
            tracing::warn!(error = %err, "failed to queue GOAWAY");
            false
        }
    }
}
