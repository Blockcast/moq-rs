// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{Decode, DecodeError, Encode, EncodeError, SessionUri};

/// Sent to tell the peer the session (or one request) is going away (draft-20 §10.4).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GoAway {
    /// Where to reconnect; empty reuses the current URI. Always empty from a client.
    pub uri: SessionUri,
    /// Milliseconds the sender waits for graceful closure; 0 means no specific timeout.
    pub timeout: u64,
}

impl Decode for GoAway {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let uri = SessionUri::decode(r)?;
        let timeout = u64::decode(r)?;
        Ok(Self { uri, timeout })
    }
}

impl Encode for GoAway {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.uri.encode(w)?;
        self.timeout.encode(w)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn encode_decode() {
        let mut buf = BytesMut::new();

        let msg = GoAway {
            uri: SessionUri("moq://example.com:1234".to_string()),
            timeout: 5000,
        };
        msg.encode(&mut buf).unwrap();
        let decoded = GoAway::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    /// Golden vector: URI length, URI bytes, then the Timeout vi64 (§10.4 Figure 7).
    #[test]
    fn golden_body() {
        let msg = GoAway {
            uri: SessionUri("moqt://r".to_string()),
            timeout: 300,
        };
        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        assert_eq!(
            buf.to_vec(),
            [&[0x08][..], b"moqt://r", &[0x81, 0x2c]].concat()
        );
    }

    /// A body that stops after the URI is missing the mandatory Timeout field.
    #[test]
    fn missing_timeout_is_rejected() {
        let mut buf = bytes::Bytes::from_static(&[0x00]);
        assert!(GoAway::decode(&mut buf).is_err());
    }
}
