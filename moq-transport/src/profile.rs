// SPDX-FileCopyrightText: 2026 Blockcast Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Explicit MOQT wire profiles.
//!
//! A profile may only be selected through exact transport negotiation. Native
//! QUIC uses the profile name as its ALPN while WebTransport carries the same
//! value in WT-Available-Protocols / WT-Protocol.

pub mod draft19;

#[derive(Default, Copy, Clone, Debug, Eq, PartialEq)]
pub enum WireProfile {
    #[default]
    Draft16,
    Draft19,
    /// Blockcast's versioned draft-16 profile with mandatory bounded history.
    Blockcast01,
}

impl WireProfile {
    pub const ALL: [Self; 3] = [Self::Blockcast01, Self::Draft19, Self::Draft16];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Draft16 => "moqt-16",
            Self::Draft19 => "moqt-19",
            Self::Blockcast01 => "moqt-blockcast-01",
        }
    }

    pub const fn alpn(self) -> &'static [u8] {
        self.name().as_bytes()
    }

    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|profile| profile.name() == name)
    }

    pub fn from_alpn(alpn: &[u8]) -> Option<Self> {
        Self::ALL.into_iter().find(|profile| profile.alpn() == alpn)
    }
}

impl std::fmt::Display for WireProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blockcast_profile_has_distinct_exact_negotiation_name() {
        assert_eq!(WireProfile::Blockcast01.name(), "moqt-blockcast-01");
        assert_eq!(
            WireProfile::from_name("moqt-blockcast-01"),
            Some(WireProfile::Blockcast01)
        );
        assert_eq!(
            WireProfile::from_alpn(b"moqt-blockcast-01"),
            Some(WireProfile::Blockcast01)
        );
        assert_eq!(WireProfile::from_name("moqt-blockcast"), None);
        assert_eq!(WireProfile::from_name("moqt-blockcast-01-preview"), None);
    }
}
