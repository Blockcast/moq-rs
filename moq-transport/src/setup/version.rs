// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{Decode, DecodeError, Encode, EncodeError, VarInt};

use std::fmt;
use std::ops::Deref;

/// A version number negotiated during the setup.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version(pub u32);

impl Version {
    // Note: older draft versions are NOT included here, as we will no longer
    //       handle the old SETUP message type numbers of (0x40 and 0x41)

    /// First version we might see in CLIENT_SETUP (0x20) or SERVER_SETUP (0x21)
    /// https://www.ietf.org/archive/id/draft-ietf-moq-transport-11.html
    pub const DRAFT_11: Version = Version(0xff00000b);

    /// https://www.ietf.org/archive/id/draft-ietf-moq-transport-12.html
    pub const DRAFT_12: Version = Version(0xff00000c);

    /// https://www.ietf.org/archive/id/draft-ietf-moq-transport-13.html
    pub const DRAFT_13: Version = Version(0xff00000d);

    /// https://www.ietf.org/archive/id/draft-ietf-moq-transport-14.html
    pub const DRAFT_14: Version = Version(0xff00000e);

    /// https://www.ietf.org/archive/id/draft-ietf-moq-transport-15.html
    ///
    /// draft-15 is additive over draft-14 at the data plane: it preserves
    /// the 0x10-0x1d stream-type range (priority present) and adds 0x30-0x3d
    /// (priority absent, inherits from control message). moq-transport's
    /// encoder only emits the 0x10-0x1d range today, which is valid
    /// draft-15 framing.
    pub const DRAFT_15: Version = Version(0xff00000f);

    /// https://www.ietf.org/archive/id/draft-ietf-moq-transport-16.html
    ///
    /// draft-16 continues the additive evolution from draft-14/15. The
    /// 0x10-0x1d "priority present" stream-type range remains valid, so
    /// moq-transport's current encoder produces wire bytes that decode
    /// as conformant draft-16 framing. Required for interop with
    /// receivers that only speak draft-16 (e.g., moqtail).
    pub const DRAFT_16: Version = Version(0xff000010);
}

impl From<u32> for Version {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

impl From<Version> for u32 {
    fn from(v: Version) -> Self {
        v.0
    }
}

impl Decode for Version {
    /// Decode the version number.
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let v = VarInt::decode(r)?;
        Ok(Self(u32::try_from(v).map_err(DecodeError::BoundsExceeded)?))
    }
}

impl Encode for Version {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        VarInt::from_u32(self.0).encode(w)?;
        Ok(())
    }
}

impl fmt::Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Just reuse the Display formatting
        write!(f, "{self}")
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 > 0xff000000 {
            write!(f, "DRAFT_{:02}", self.0 & 0x00ffffff)
        } else {
            self.0.fmt(f)
        }
    }
}

/// A list of versions in arbitrary order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Versions(pub Vec<Version>);

impl Decode for Versions {
    /// Decode the version list.
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let count = usize::decode(r)?;
        let mut vs = Vec::new();

        for _ in 0..count {
            let v = Version::decode(r)?;
            vs.push(v);
        }

        Ok(Self(vs))
    }
}

impl Encode for Versions {
    /// Encode the version list.
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.0.len().encode(w)?;

        for v in &self.0 {
            v.encode(w)?;
        }

        Ok(())
    }
}

impl Deref for Versions {
    type Target = Vec<Version>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Vec<Version>> for Versions {
    fn from(vs: Vec<Version>) -> Self {
        Self(vs)
    }
}

impl<const N: usize> From<[Version; N]> for Versions {
    fn from(vs: [Version; N]) -> Self {
        Self(vs.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn encode_decode() {
        let mut buf = BytesMut::new();
        let versions = Versions(vec![Version(1), Version::DRAFT_12, Version::DRAFT_13]);

        versions.encode(&mut buf).unwrap();
        #[rustfmt::skip]
        assert_eq!(
            buf.to_vec(),
            vec![
                0x03, // 3 Versions
                // Version 1
                0x01,
                // Version DRAFT_12 (0xff00000c)
                0xC0, 0x00, 0x00, 0x00, 0xFF, 0x00, 0x00, 0x0C,
                // Version DRAFT_13 (0xff00000d)
                0xC0, 0x00, 0x00, 0x00, 0xFF, 0x00, 0x00, 0x0D,
            ]
        );

        let decoded = Versions::decode(&mut buf).unwrap();
        assert_eq!(decoded, versions);
    }

    /// Pins the IETF moq-transport draft-15 + draft-16 version constants per
    /// `https://www.ietf.org/archive/id/draft-ietf-moq-transport-{15,16}.html`.
    /// Added 2026-05-28 for M.4 T0: moqtail (`moqtail-private/libs/moqtail-ts/`)
    /// speaks `0xff000010` and the Blockcast Shaka fork's MSF integration speaks
    /// both `0xff00000e` and `0xff000010` via ALPN negotiation. The Display
    /// formatting must continue to render `"DRAFT_16"` (lowercase hex byte → 16)
    /// so log lines + mlog events remain readable.
    #[test]
    fn draft_15_and_16_constants_pin_ietf_codepoints() {
        assert_eq!(Version::DRAFT_15.0, 0xff00000f);
        assert_eq!(Version::DRAFT_16.0, 0xff000010);
        assert_eq!(format!("{}", Version::DRAFT_15), "DRAFT_15");
        assert_eq!(format!("{}", Version::DRAFT_16), "DRAFT_16");

        // Round-trip both as a list (matches the actual setup wire shape).
        let mut buf = BytesMut::new();
        let versions = Versions(vec![Version::DRAFT_16, Version::DRAFT_14]);
        versions.encode(&mut buf).unwrap();
        let decoded = Versions::decode(&mut buf).unwrap();
        assert_eq!(decoded, versions);
    }
}
