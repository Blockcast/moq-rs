// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

/// Setup Parameter Types
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u64)]
pub enum ParameterType {
    Path = 0x1,
    MaxRequestId = 0x2,
    AuthorizationToken = 0x3,
    MaxAuthTokenCacheSize = 0x4,
    Authority = 0x5,
    MOQTImplementation = 0x7,
}

impl From<ParameterType> for u64 {
    fn from(value: ParameterType) -> Self {
        value as u64
    }
}

/// CatalogSubscriptionCapabilities SETUP parameter (Blockcast extension,
/// BLO-22575). Odd key → length-prefixed bytes value.
///
/// A publisher declares how its catalog track is delivered so the peer can
/// pick the matching SUBSCRIBE filter without keying on a negotiated draft
/// version. Consumed by `blockcastd-shred-relay`
/// (`packages/dual-stack-relay/src/protocol/messages.rs`,
/// `resolve_catalog_subscription_capabilities`).
pub const CATALOG_SUBSCRIPTION_CAPABILITIES: u64 = 0x4243;

/// Declaration body for a publisher that writes its catalog once at
/// group 0 / object 0 and retains it — `moq-pub-mmtp`'s
/// `publish_catalog_track`. Canonical (shortest-form) varints, in order:
///
/// ```text
/// contract_version = 1
/// catalog_delivery = 1  (RetainedAtOrigin; 0 = Live)
/// filter_count     = 2
/// filters          = 0x02 (LargestObject), 0x03 (AbsoluteStart)
/// ```
///
/// Filters MUST be strictly increasing and MUST include the one required by
/// the declared delivery (`AbsoluteStart` for RetainedAtOrigin) — the relay
/// rejects the session otherwise. It selects `AbsoluteStart(0,0)`, which is
/// what actually delivers the retained catalog object.
pub const RETAINED_CATALOG_CAPABILITIES: &[u8] = &[1, 1, 2, 0x02, 0x03];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coding::{Decode, Encode};

    /// Pins the wire contract the relay decodes: the declared field values,
    /// shortest-form varints (the relay rejects overlong ones), and no
    /// trailing bytes.
    #[test]
    fn retained_catalog_capabilities_is_canonical() {
        let mut cursor = RETAINED_CATALOG_CAPABILITIES;
        let fields: Vec<u64> = std::iter::from_fn(|| {
            (!cursor.is_empty()).then(|| u64::decode(&mut cursor).expect("varint decodes"))
        })
        .collect();

        assert_eq!(fields, vec![1, 1, 2, 0x02, 0x03]);
        assert!(cursor.is_empty(), "declaration has trailing bytes");

        // Re-encoding with the canonical encoder must reproduce the constant
        // byte-for-byte, proving no field is overlong.
        let mut round_trip = Vec::new();
        for field in &fields {
            field.encode(&mut round_trip).expect("varint encodes");
        }
        assert_eq!(round_trip, RETAINED_CATALOG_CAPABILITIES);
    }

    /// Odd key → the KVP layer encodes a length-prefixed bytes value, which is
    /// the encoding the relay requires (`WrongParameterEncoding` otherwise).
    #[test]
    fn catalog_capabilities_key_is_odd() {
        assert_eq!(CATALOG_SUBSCRIPTION_CAPABILITIES % 2, 1);
    }
}
