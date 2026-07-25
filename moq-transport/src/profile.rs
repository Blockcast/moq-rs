// SPDX-FileCopyrightText: 2026 Blockcast Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Explicit MOQT wire profiles.
//!
//! Draft-19 is deliberately not wired into ALPN advertisement yet. Consumers
//! must opt into this transport-core profile explicitly while the remaining
//! application codecs are ported.

pub mod draft19;

#[derive(Default, Copy, Clone, Debug, Eq, PartialEq)]
pub enum WireProfile {
    #[default]
    Draft16,
    Draft19,
}

impl WireProfile {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Draft16 => "moqt-16",
            Self::Draft19 => "moqt-19",
        }
    }
}
