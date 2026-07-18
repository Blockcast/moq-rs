// SPDX-FileCopyrightText: 2026 Blockcast Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

//! The variable-length 64-bit integer from draft-ietf-moq-transport-19.
//!
//! This is intentionally separate from [`super::VarInt`], which remains the
//! QUIC variable-length integer used by the immutable draft-16 profile.

use bytes::{Buf, BufMut};

use super::{Decode, DecodeError, Encode, EncodeError};

#[derive(Default, Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Vi64(u64);

impl Vi64 {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn into_inner(self) -> u64 {
        self.0
    }

    pub const fn encoded_len(self) -> usize {
        let bits = 64 - self.0.leading_zeros() as usize;
        if bits == 0 {
            1
        } else if bits <= 56 {
            bits.div_ceil(7)
        } else {
            9
        }
    }
}

impl From<u64> for Vi64 {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Vi64> for u64 {
    fn from(value: Vi64) -> Self {
        value.0
    }
}

impl Decode for Vi64 {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        Self::decode_remaining(buf, 1)?;
        let first = buf.get_u8();
        let leading = first.leading_ones() as usize;
        let len = if leading == 8 { 9 } else { leading + 1 };
        Self::decode_remaining(buf, len - 1)?;

        let mut value = if len >= 8 {
            0
        } else {
            u64::from(first & (0xff >> len))
        };
        for _ in 1..len {
            value = (value << 8) | u64::from(buf.get_u8());
        }

        Ok(Self(value))
    }
}

impl Encode for Vi64 {
    fn encode<W: BufMut>(&self, buf: &mut W) -> Result<(), EncodeError> {
        let len = self.encoded_len();
        Self::encode_remaining(buf, len)?;

        if len == 9 {
            buf.put_u8(0xff);
            buf.put_u64(self.0);
            return Ok(());
        }

        let prefix = if len == 1 { 0 } else { 0xff << (9 - len) };
        let shift = 8 * (len - 1);
        let value_mask = if len == 8 { 0 } else { 0xff >> len };
        let first = prefix | ((self.0 >> shift) as u8 & value_mask);
        buf.put_u8(first);

        let bytes = self.0.to_be_bytes();
        if len > 1 {
            buf.put_slice(&bytes[9 - len..]);
        }
        Ok(())
    }
}
