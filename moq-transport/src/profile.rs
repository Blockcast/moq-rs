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

    /// Exact WebTransport subprotocol token (WT-Available-Protocols / WT-Protocol).
    ///
    /// This is NOT the ALPN. ROOT-13 splits the draft-16 family because bare
    /// `moqt-16` was shared by writers needing different media handlers: the
    /// dual-stack relay now reads `moqt-wt-16` (MOQT object delivery over
    /// WebTransport) and `moqt-raw-mmtp-16` (MOQT control, raw MMTP media).
    /// Every moq-rs call site delivers MOQT objects — including `moq-pub-mmtp`,
    /// whose MMTP and opaque-datagram routers both publish through a
    /// `TrackWriter` as MOQT subgroup/datagram objects — so draft-16 maps to
    /// `moqt-wt-16` here and `moqt-raw-mmtp-16` is unreachable from this crate.
    ///
    /// `moqt-19` and `moqt-blockcast-01` are outside the ROOT-13 vocabulary by
    /// design: they are moq-rs-to-moq-rs profiles, already exact and
    /// single-valued, and the dual-stack relay is correct to reject them.
    pub const fn webtransport_protocol(self) -> &'static str {
        match self {
            Self::Draft16 => "moqt-wt-16",
            Self::Draft19 => "moqt-19",
            Self::Blockcast01 => "moqt-blockcast-01",
        }
    }

    /// Legacy WebTransport token this profile answered to before ROOT-13.
    ///
    /// Readers accept it; writers never offer it. Delete at ROOT-13 cutover
    /// step 5, once the strict reader is proven (see
    /// `docs/g0/root-13-subprotocol-token-matrix.md`).
    const fn legacy_webtransport_protocol(self) -> Option<&'static str> {
        match self {
            Self::Draft16 => Some("moqt-16"),
            Self::Draft19 | Self::Blockcast01 => None,
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|profile| profile.name() == name)
    }

    pub fn from_alpn(alpn: &[u8]) -> Option<Self> {
        Self::ALL.into_iter().find(|profile| profile.alpn() == alpn)
    }

    /// Server-side match for an offered WebTransport subprotocol token.
    ///
    /// Reader-lenient during the ROOT-13 writer-first window: it also accepts
    /// the legacy token so an un-migrated writer keeps working and writers can
    /// be rolled back before the reader.
    pub fn from_webtransport_protocol(protocol: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|profile| {
            profile.webtransport_protocol() == protocol
                || profile.legacy_webtransport_protocol() == Some(protocol)
        })
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

    #[test]
    fn webtransport_token_is_separate_from_alpn_for_draft16() {
        // ROOT-13: the WT subprotocol disambiguates the draft-16 family; the
        // native QUIC ALPN deliberately does not move.
        assert_eq!(WireProfile::Draft16.webtransport_protocol(), "moqt-wt-16");
        assert_eq!(WireProfile::Draft16.alpn(), b"moqt-16");
        assert_eq!(WireProfile::Draft19.webtransport_protocol(), "moqt-19");
        assert_eq!(
            WireProfile::Blockcast01.webtransport_protocol(),
            "moqt-blockcast-01"
        );
        // No moq-rs profile offers the raw-MMTP media contract.
        assert!(WireProfile::ALL
            .iter()
            .all(|p| p.webtransport_protocol() != "moqt-raw-mmtp-16"));
    }

    #[test]
    fn reader_accepts_target_and_legacy_tokens_but_nothing_else() {
        assert_eq!(
            WireProfile::from_webtransport_protocol("moqt-wt-16"),
            Some(WireProfile::Draft16)
        );
        // Lenient reader window: delete with ROOT-13 cutover step 5.
        assert_eq!(
            WireProfile::from_webtransport_protocol("moqt-16"),
            Some(WireProfile::Draft16)
        );
        assert_eq!(
            WireProfile::from_webtransport_protocol("moqt-19"),
            Some(WireProfile::Draft19)
        );
        assert_eq!(WireProfile::from_webtransport_protocol("moqt-wt-17"), None);
        assert_eq!(
            WireProfile::from_webtransport_protocol("moqt-raw-mmtp-16"),
            None
        );
        assert_eq!(WireProfile::from_webtransport_protocol("moqtail"), None);
        assert_eq!(
            WireProfile::from_webtransport_protocol(" moqt-wt-16 "),
            None
        );
    }
}
