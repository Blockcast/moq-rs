// SPDX-FileCopyrightText: 2026 Cloudflare Inc. and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{Decode, DecodeError, Encode, EncodeError, KeyValuePairs};

/// Unilateral notice from a publisher that a subscription's state changed for
/// a reason other than a REQUEST_UPDATE (draft-20 §10.10). It carries only
/// the parameters whose values changed and is never answered.
///
/// Sent on the subscription's request stream, so it carries no Request ID.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PublishStateNotify {
    pub params: KeyValuePairs,
}

impl Decode for PublishStateNotify {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        Ok(Self {
            params: KeyValuePairs::decode_message_params(r)?,
        })
    }
}

impl Encode for PublishStateNotify {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.params.encode_message_params(w)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coding::Location;
    use bytes::BytesMut;

    /// Golden vector: parameter count, then LARGEST_OBJECT {7, 2} as two
    /// inline vi64s (§10.2.17), which the publisher MUST include when known.
    #[test]
    fn golden_body_with_largest_object() {
        let mut params = KeyValuePairs::default();
        params.set_largest_object(Location::new(7, 2)).unwrap();
        let msg = PublishStateNotify { params };

        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x01, 0x09, 0x07, 0x02]);
        assert_eq!(PublishStateNotify::decode(&mut buf).unwrap(), msg);
    }

    #[test]
    fn empty_body_is_rejected() {
        let mut buf = bytes::Bytes::new();
        assert!(PublishStateNotify::decode(&mut buf).is_err());
    }
}
