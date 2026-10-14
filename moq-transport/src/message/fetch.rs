// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{
    validate_full_track_name, Decode, DecodeError, Encode, EncodeError, KeyValuePairs, TrackName,
    TrackNamespace,
};

/// Sent by the subscriber to request a range of already published objects
/// within a track (draft-20 §10.13). The range travels in the LOCATION_FILTER
/// parameter; without one the fetch covers {0, 0} through Largest Object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Fetch {
    /// The fetch request ID
    pub id: u64,
    pub track_namespace: TrackNamespace,
    pub track_name: TrackName,

    /// Optional parameters
    pub params: KeyValuePairs,
}

impl Decode for Fetch {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let id = u64::decode(r)?;
        let track_namespace = TrackNamespace::decode(r)?;
        let track_name = TrackName::decode(r)?;
        validate_full_track_name(&track_namespace, track_name.as_bytes())?;
        let params = KeyValuePairs::decode_message_params(r)?;

        Ok(Self {
            id,
            track_namespace,
            track_name,
            params,
        })
    }
}

impl Encode for Fetch {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.id.encode(w)?;
        self.track_namespace.encode(w)?;
        self.track_name.encode(w)?;
        self.params.encode_message_params(w)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coding::Location;
    use crate::message::LocationFilter;
    use bytes::BytesMut;

    #[test]
    fn encode_decode() {
        let mut params = KeyValuePairs::new();
        params
            .set_location_filter(&LocationFilter::Absolute {
                start: Location::new(34, 53),
                end_group: Some(34),
                end_object: Some(60),
            })
            .unwrap();
        let msg = Fetch {
            id: 12345,
            track_namespace: TrackNamespace::from_utf8_path("test/path/to/resource"),
            track_name: "audiotrack".into(),
            params,
        };

        let mut buf = BytesMut::new();
        msg.encode(&mut buf).unwrap();
        assert_eq!(Fetch::decode(&mut buf).unwrap(), msg);
    }

    /// A draft-18 Standalone body (Fetch Type 0x1 before the namespace) is not
    /// a draft-20 FETCH: the type byte reads as a namespace tuple count of 1
    /// and the remaining fields no longer line up.
    #[test]
    fn draft18_fetch_type_prefix_is_rejected() {
        // id=0, FetchType=1, ns=("a"), name="t", start {0,0}, end {0,1}, 0 params
        let mut buf =
            bytes::Bytes::from_static(&[0x00, 0x01, 0x01, 0x01, b'a', 0x01, b't', 0, 0, 0, 1, 0]);
        assert!(Fetch::decode(&mut buf).is_err());
    }
}
