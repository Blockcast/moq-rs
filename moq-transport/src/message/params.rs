// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use bytes::Buf as _;

use crate::coding::{
    Decode, DecodeError, Encode, EncodeError, KeyValuePair, KeyValuePairs, Location, Value,
};
use crate::message::{wire_id, GroupOrder};

/// Message-parameter type IDs (draft-20 §15.7).
pub mod parameter_type {
    /// OBJECT_DELIVERY_TIMEOUT.
    pub const DELIVERY_TIMEOUT: u64 = 0x02;
    pub const AUTHORIZATION_TOKEN: u64 = 0x03;
    /// draft-18 §10.2.6. Milliseconds a subscriber will wait for a publisher to
    /// appear. Valid on SUBSCRIBE only, unlike the delivery timeouts.
    pub const RENDEZVOUS_TIMEOUT: u64 = 0x04;
    pub const SUBGROUP_DELIVERY_TIMEOUT: u64 = 0x06;
    pub const EXPIRES: u64 = 0x08;
    pub const LARGEST_OBJECT: u64 = 0x09;
    pub const FILL_TIMEOUT: u64 = 0x0A;
    pub const FORWARD: u64 = 0x10;
    pub const SUBSCRIBER_PRIORITY: u64 = 0x20;
    pub const LOCATION_FILTER: u64 = 0x21;
    pub const GROUP_ORDER: u64 = 0x22;
    pub const FILL_PARAMETERS: u64 = 0x23;
    pub const SUBGROUP_FILTER: u64 = 0x25;
    pub const OBJECTID_FILTER: u64 = 0x26;
    pub const PRIORITY_FILTER: u64 = 0x27;
    pub const OBJECT_PROPERTY_FILTER: u64 = 0x28;
    pub const TRACK_PROPERTY_FILTER: u64 = 0x29;
    pub const NEW_GROUP_REQUEST: u64 = 0x32;
    pub const TRACK_NAMESPACE_PREFIX: u64 = 0x34;
    pub const INCLUDE_PROPERTIES: u64 = 0x35;
}

/// Message-parameter type IDs whose value is a single raw `uint8` byte rather
/// than a varint (draft-18 §10.2).
///
/// Draft-17 replaced the generic even=varint / odd=length-prefixed KVP rule for
/// message parameters with per-parameter value kinds. These three are the
/// byte-valued parameters carried by SUBSCRIBE / FETCH. Notably
/// SUBSCRIBER_PRIORITY defaults to 128, which a varint encodes in two bytes —
/// desynchronising the parameter block for peers (imquic, moq-go) that expect a
/// single byte. LOCATION_FILTER (0x21) stays length-prefixed bytes and the
/// varint-valued parameters (timeouts, EXPIRES, NEW_GROUP_REQUEST) are
/// unaffected, so they are intentionally absent here.
pub const U8_VALUE_PARAMETER_TYPES: &[u64] = &[
    parameter_type::FORWARD,             // 0x10
    parameter_type::SUBSCRIBER_PRIORITY, // 0x20
    parameter_type::GROUP_ORDER,         // 0x22
];

const TWO_VARINT_VALUE_PARAMETER_TYPES: &[u64] = &[parameter_type::LARGEST_OBJECT];

/// Message scopes enforced for supported parameters with restricted use.
/// Unknown parameter types remain available for negotiated extensions.
const MESSAGE_PARAMETER_SCOPES: &[(u64, &[u64])] =
    &[(parameter_type::RENDEZVOUS_TIMEOUT, &[wire_id::Subscribe])];

/// Draft-16 extension-header type IDs.
pub mod extension_type {
    pub const DELIVERY_TIMEOUT: u64 = 0x02;
    pub const MAX_CACHE_DURATION: u64 = 0x04;
    pub const IMMUTABLE_EXTENSIONS: u64 = 0x0B;
    pub const DEFAULT_PUBLISHER_PRIORITY: u64 = 0x0E;
    pub const DEFAULT_PUBLISHER_GROUP_ORDER: u64 = 0x22;
    pub const DYNAMIC_GROUPS: u64 = 0x30;
    pub const PRIOR_GROUP_ID_GAP: u64 = 0x3C;
    pub const PRIOR_OBJECT_ID_GAP: u64 = 0x3E;
}

/// LOCATION_FILTER value (draft-20 §5.1.2). The number of vi64 fields present
/// in the length-prefixed value selects the form.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocationFilter {
    /// Zero fields: no filter. In REQUEST_UPDATE this removes the current filter.
    Unfiltered,
    /// One field (StartGroup): start at {Largest.Group + 1 - n, 0}; open-ended.
    /// `RelativeGroup(0)` is the Next Group, `RelativeGroup(1)` the current one.
    RelativeGroup(u64),
    /// Two zero fields: start at the Next Object; open-ended.
    NextObject,
    /// Absolute start, optional inclusive end group, optional inclusive end
    /// object within the end group. `end_object` requires `end_group`.
    Absolute {
        start: Location,
        end_group: Option<u64>,
        end_object: Option<u64>,
    },
}

impl Decode for LocationFilter {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let mut fields = [0u64; 4];
        let mut n = 0;
        while r.has_remaining() {
            if n == fields.len() {
                return Err(DecodeError::InvalidParameter);
            }
            fields[n] = u64::decode(r)?;
            n += 1;
        }

        Ok(match n {
            0 => Self::Unfiltered,
            1 => Self::RelativeGroup(fields[0]),
            2 if fields[0] == 0 && fields[1] == 0 => Self::NextObject,
            _ => Self::Absolute {
                start: Location::new(fields[0], fields[1]),
                end_group: if n >= 3 {
                    // StartGroup + EndGroupDelta overflowing is a PROTOCOL_VIOLATION.
                    Some(
                        fields[0]
                            .checked_add(fields[2])
                            .ok_or(DecodeError::InvalidParameter)?,
                    )
                } else {
                    None
                },
                end_object: (n == 4).then_some(fields[3]),
            },
        })
    }
}

impl Encode for LocationFilter {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        match *self {
            Self::Unfiltered => {}
            Self::RelativeGroup(groups) => groups.encode(w)?,
            Self::NextObject => {
                0u64.encode(w)?;
                0u64.encode(w)?;
            }
            Self::Absolute {
                start,
                end_group,
                end_object,
            } => {
                // An open-ended filter from {0, 0} is unfiltered, and as two
                // fields it would read back as NextObject.
                if start == Location::new(0, 0) && end_group.is_none() {
                    return Ok(());
                }
                start.group_id.encode(w)?;
                start.object_id.encode(w)?;
                match (end_group, end_object) {
                    (None, None) => {}
                    (None, Some(_)) => {
                        return Err(EncodeError::MissingField("EndGroup".to_string()))
                    }
                    (Some(end_group), end_object) => {
                        end_group
                            .checked_sub(start.group_id)
                            .ok_or(EncodeError::InvalidValue)?
                            .encode(w)?;
                        if let Some(end_object) = end_object {
                            end_object.encode(w)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Draft-16 Track Extensions are a trailing sequence of KVPs with no count or length prefix.
#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct TrackExtensions(pub Vec<KeyValuePair>);

impl TrackExtensions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_extension(&mut self, kvp: KeyValuePair) {
        if let Some(existing) = self.0.iter_mut().find(|k| k.key == kvp.key) {
            *existing = kvp;
        } else {
            self.0.push(kvp);
        }
    }

    pub fn set_int_extension(&mut self, key: u64, value: u64) {
        self.set_extension(KeyValuePair::new_int(key, value));
    }

    pub fn set_bytes_extension(&mut self, key: u64, value: Vec<u8>) {
        self.set_extension(KeyValuePair::new_bytes(key, value));
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn contains_only(&self, supported_types: &[u64]) -> bool {
        self.0
            .iter()
            .all(|extension| supported_types.contains(&extension.key))
    }

    pub fn delivery_timeout(&self) -> Result<Option<u64>, DecodeError> {
        get_kvp_int(&self.0, extension_type::DELIVERY_TIMEOUT)
    }

    pub fn set_delivery_timeout(&mut self, timeout: u64) {
        self.set_int_extension(extension_type::DELIVERY_TIMEOUT, timeout);
    }

    pub fn max_cache_duration(&self) -> Result<Option<u64>, DecodeError> {
        get_kvp_int(&self.0, extension_type::MAX_CACHE_DURATION)
    }

    pub fn set_max_cache_duration(&mut self, duration: u64) {
        self.set_int_extension(extension_type::MAX_CACHE_DURATION, duration);
    }

    pub fn default_publisher_priority(&self) -> Result<Option<u8>, DecodeError> {
        get_kvp_u8(&self.0, extension_type::DEFAULT_PUBLISHER_PRIORITY)
    }

    pub fn set_default_publisher_priority(&mut self, priority: u8) {
        self.set_int_extension(extension_type::DEFAULT_PUBLISHER_PRIORITY, priority.into());
    }

    pub fn default_publisher_group_order(&self) -> Result<Option<GroupOrder>, DecodeError> {
        match get_kvp_int(&self.0, extension_type::DEFAULT_PUBLISHER_GROUP_ORDER)? {
            Some(1) => Ok(Some(GroupOrder::Ascending)),
            Some(2) => Ok(Some(GroupOrder::Descending)),
            Some(_) => Err(DecodeError::InvalidGroupOrder),
            None => Ok(None),
        }
    }

    pub fn set_default_publisher_group_order(
        &mut self,
        group_order: GroupOrder,
    ) -> Result<(), EncodeError> {
        match group_order {
            GroupOrder::Ascending | GroupOrder::Descending => {
                self.set_int_extension(
                    extension_type::DEFAULT_PUBLISHER_GROUP_ORDER,
                    group_order as u64,
                );
                Ok(())
            }
            GroupOrder::Publisher => Err(EncodeError::InvalidValue),
        }
    }

    pub fn dynamic_groups(&self) -> Result<Option<bool>, DecodeError> {
        match get_kvp_int(&self.0, extension_type::DYNAMIC_GROUPS)? {
            Some(0) => Ok(Some(false)),
            Some(1) => Ok(Some(true)),
            Some(_) => Err(DecodeError::InvalidParameter),
            None => Ok(None),
        }
    }

    pub fn set_dynamic_groups(&mut self, enabled: bool) {
        self.set_int_extension(extension_type::DYNAMIC_GROUPS, if enabled { 1 } else { 0 });
    }
}

impl Decode for TrackExtensions {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        let mut extensions = Vec::new();
        let mut prev = 0u64;

        while r.has_remaining() {
            let (pair, new_prev) = KeyValuePair::decode_with_prev(r, prev)?;
            prev = new_prev;
            extensions.push(pair);
        }

        Ok(Self(extensions))
    }
}

impl Encode for TrackExtensions {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        let mut sorted: Vec<&KeyValuePair> = self.0.iter().collect();
        sorted.sort_by_key(|k| k.key);

        let mut prev = 0u64;
        for kvp in &sorted {
            prev = kvp.encode_with_prev(w, prev)?;
        }

        Ok(())
    }
}

fn encode_bytes_value<T: Encode>(value: &T) -> Result<Vec<u8>, EncodeError> {
    let mut buf = Vec::new();
    value.encode(&mut buf)?;
    Ok(buf)
}

fn decode_bytes_value<T: Decode>(value: &[u8]) -> Result<T, DecodeError> {
    let mut payload = bytes::Bytes::copy_from_slice(value);
    let decoded = T::decode(&mut payload)?;
    if payload.has_remaining() {
        return Err(DecodeError::InvalidParameter);
    }
    Ok(decoded)
}

fn get_kvp_int(pairs: &[KeyValuePair], key: u64) -> Result<Option<u64>, DecodeError> {
    match pairs
        .iter()
        .find(|kvp| kvp.key == key)
        .map(|kvp| &kvp.value)
    {
        Some(Value::IntValue(value)) => Ok(Some(*value)),
        Some(Value::BytesValue(_)) => Err(DecodeError::InvalidParameter),
        None => Ok(None),
    }
}

fn get_kvp_u8(pairs: &[KeyValuePair], key: u64) -> Result<Option<u8>, DecodeError> {
    match get_kvp_int(pairs, key)? {
        Some(value) => u8::try_from(value)
            .map(Some)
            .map_err(|_| DecodeError::InvalidParameter),
        None => Ok(None),
    }
}

impl KeyValuePairs {
    /// Decode a draft-18 §10.2 message-parameter block (SUBSCRIBE, FETCH,
    /// PUBLISH, PUBLISH_OK, REQUEST_OK, REQUEST_UPDATE, …).
    ///
    /// This is the count-prefixed KVP decode with the draft-17 byte-valued
    /// parameter kinds applied uniformly (see [`U8_VALUE_PARAMETER_TYPES`]).
    /// Every message carrying §10.2 parameters MUST use this rather than the
    /// generic [`KeyValuePairs::decode`], otherwise a peer's single-byte
    /// SUBSCRIBER_PRIORITY (default 128) desynchronises the whole block.
    pub fn decode_message_params<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        Self::decode_with_value_types(
            r,
            U8_VALUE_PARAMETER_TYPES,
            TWO_VARINT_VALUE_PARAMETER_TYPES,
        )
    }

    /// Encode a draft-18 §10.2 message-parameter block. Counterpart to
    /// [`decode_message_params`](KeyValuePairs::decode_message_params).
    pub fn encode_message_params<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.encode_with_value_types(
            w,
            U8_VALUE_PARAMETER_TYPES,
            TWO_VARINT_VALUE_PARAMETER_TYPES,
        )
    }

    pub(crate) fn parameters_allowed_on(&self, message_type: u64) -> bool {
        if message_type == wire_id::SubscribeTracks {
            return self.0.iter().all(|parameter| {
                matches!(
                    parameter.key,
                    parameter_type::AUTHORIZATION_TOKEN | parameter_type::FORWARD
                )
            });
        }
        self.0.iter().all(|parameter| {
            MESSAGE_PARAMETER_SCOPES
                .iter()
                .find(|(parameter_type, _)| *parameter_type == parameter.key)
                .is_none_or(|(_, message_types)| message_types.contains(&message_type))
        })
    }

    fn int_parameter(&self, key: u64) -> Result<Option<u64>, DecodeError> {
        get_kvp_int(&self.0, key)
    }

    pub fn set_forward(&mut self, forward: bool) {
        self.set_intvalue(parameter_type::FORWARD, if forward { 1 } else { 0 });
    }

    pub fn forward(&self) -> Result<Option<bool>, DecodeError> {
        match self.int_parameter(parameter_type::FORWARD)? {
            Some(0) => Ok(Some(false)),
            Some(1) => Ok(Some(true)),
            Some(_) => Err(DecodeError::InvalidParameter),
            None => Ok(None),
        }
    }

    pub fn set_subscriber_priority(&mut self, priority: u8) {
        self.set_intvalue(parameter_type::SUBSCRIBER_PRIORITY, priority.into());
    }

    pub fn subscriber_priority(&self) -> Result<Option<u8>, DecodeError> {
        get_kvp_u8(&self.0, parameter_type::SUBSCRIBER_PRIORITY)
    }

    pub fn set_group_order(&mut self, group_order: GroupOrder) {
        if group_order != GroupOrder::Publisher {
            self.set_intvalue(parameter_type::GROUP_ORDER, group_order as u64);
        }
    }

    pub fn group_order(&self) -> Result<Option<GroupOrder>, DecodeError> {
        match self.int_parameter(parameter_type::GROUP_ORDER)? {
            Some(0) => Err(DecodeError::InvalidGroupOrder),
            Some(1) => Ok(Some(GroupOrder::Ascending)),
            Some(2) => Ok(Some(GroupOrder::Descending)),
            Some(_) => Err(DecodeError::InvalidGroupOrder),
            None => Ok(None),
        }
    }

    pub fn set_location_filter(&mut self, filter: &LocationFilter) -> Result<(), EncodeError> {
        self.set_bytesvalue(parameter_type::LOCATION_FILTER, encode_bytes_value(filter)?);
        Ok(())
    }

    /// `None` means the parameter was omitted; `Some(Unfiltered)` means it was
    /// present with length 0 (an explicit clear in REQUEST_UPDATE).
    pub fn location_filter(&self) -> Result<Option<LocationFilter>, DecodeError> {
        match self
            .get(parameter_type::LOCATION_FILTER)
            .map(|kvp| &kvp.value)
        {
            Some(Value::BytesValue(value)) => decode_bytes_value(value).map(Some),
            Some(Value::IntValue(_)) => Err(DecodeError::InvalidParameter),
            None => Ok(None),
        }
    }

    pub fn set_largest_object(&mut self, location: Location) -> Result<(), EncodeError> {
        self.set_bytesvalue(
            parameter_type::LARGEST_OBJECT,
            encode_bytes_value(&location)?,
        );
        Ok(())
    }

    pub fn largest_object(&self) -> Result<Option<Location>, DecodeError> {
        match self
            .get(parameter_type::LARGEST_OBJECT)
            .map(|kvp| &kvp.value)
        {
            Some(Value::BytesValue(value)) => decode_bytes_value(value).map(Some),
            Some(Value::IntValue(_)) => Err(DecodeError::InvalidParameter),
            None => Ok(None),
        }
    }

    pub fn set_expires(&mut self, expires: u64) {
        self.set_intvalue(parameter_type::EXPIRES, expires);
    }

    pub fn expires(&self) -> Result<Option<u64>, DecodeError> {
        self.int_parameter(parameter_type::EXPIRES)
    }

    pub fn set_delivery_timeout(&mut self, timeout: u64) {
        self.set_intvalue(parameter_type::DELIVERY_TIMEOUT, timeout);
    }

    pub fn delivery_timeout(&self) -> Result<Option<u64>, DecodeError> {
        self.int_parameter(parameter_type::DELIVERY_TIMEOUT)
    }

    pub fn set_rendezvous_timeout(&mut self, timeout_ms: u64) {
        self.set_intvalue(parameter_type::RENDEZVOUS_TIMEOUT, timeout_ms);
    }

    /// Milliseconds the subscriber will wait for a publisher to become available
    /// (draft-18 §10.2.6). `None` and `Some(0)` are equivalent in effect: the
    /// spec's default is 0, meaning respond immediately rather than hold.
    pub fn rendezvous_timeout(&self) -> Result<Option<u64>, DecodeError> {
        self.int_parameter(parameter_type::RENDEZVOUS_TIMEOUT)
    }

    pub fn set_new_group_request(&mut self, group_id: u64) {
        self.set_intvalue(parameter_type::NEW_GROUP_REQUEST, group_id);
    }

    pub fn new_group_request(&self) -> Result<Option<u64>, DecodeError> {
        self.int_parameter(parameter_type::NEW_GROUP_REQUEST)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn key_value_pairs_message_parameter_methods_round_trip_typed_values() {
        let mut params = KeyValuePairs::default();
        let filter = LocationFilter::Absolute {
            start: Location::new(1, 2),
            end_group: Some(3),
            end_object: Some(4),
        };

        params.set_forward(false);
        params.set_subscriber_priority(7);
        params.set_group_order(GroupOrder::Descending);
        params.set_location_filter(&filter).unwrap();
        params.set_largest_object(Location::new(4, 5)).unwrap();
        params.set_expires(6);
        params.set_delivery_timeout(8);
        params.set_new_group_request(9);
        params.set_rendezvous_timeout(30_000);

        assert_eq!(params.forward().unwrap(), Some(false));
        assert_eq!(params.subscriber_priority().unwrap(), Some(7));
        assert_eq!(params.group_order().unwrap(), Some(GroupOrder::Descending));
        assert_eq!(params.location_filter().unwrap(), Some(filter));
        assert_eq!(params.largest_object().unwrap(), Some(Location::new(4, 5)));
        assert_eq!(params.expires().unwrap(), Some(6));
        assert_eq!(params.delivery_timeout().unwrap(), Some(8));
        assert_eq!(params.new_group_request().unwrap(), Some(9));
        assert_eq!(params.rendezvous_timeout().unwrap(), Some(30_000));
    }

    /// 0x04 is even, so the KVP codec carries it as a varint. A peer sending a
    /// byte string under that key is malformed rather than something to coerce.
    fn decode_filter(bytes: &[u8]) -> Result<LocationFilter, DecodeError> {
        decode_bytes_value(bytes)
    }

    /// Golden vectors for every LOCATION_FILTER form (draft-20 §5.1.2).
    #[test]
    fn location_filter_golden_vectors() {
        let cases: &[(&[u8], LocationFilter)] = &[
            (&[], LocationFilter::Unfiltered),
            (&[0x05], LocationFilter::RelativeGroup(5)),
            (&[0x00, 0x00], LocationFilter::NextObject),
            (
                &[0x0a, 0x03],
                LocationFilter::Absolute {
                    start: Location::new(10, 3),
                    end_group: None,
                    end_object: None,
                },
            ),
            (
                &[0x0a, 0x03, 0x02],
                LocationFilter::Absolute {
                    start: Location::new(10, 3),
                    end_group: Some(12),
                    end_object: None,
                },
            ),
            (
                &[0x0a, 0x03, 0x02, 0x07],
                LocationFilter::Absolute {
                    start: Location::new(10, 3),
                    end_group: Some(12),
                    end_object: Some(7),
                },
            ),
            // Three or four fields are absolute even when the start is {0, 0}.
            (
                &[0x00, 0x00, 0x00],
                LocationFilter::Absolute {
                    start: Location::new(0, 0),
                    end_group: Some(0),
                    end_object: None,
                },
            ),
        ];
        for (bytes, filter) in cases {
            assert_eq!(
                decode_filter(bytes).unwrap(),
                *filter,
                "decode {bytes:02x?}"
            );
            assert_eq!(
                encode_bytes_value(filter).unwrap(),
                bytes.to_vec(),
                "encode {filter:?}"
            );
        }
    }

    /// The whole parameter inside a message-parameter block: count, type 0x21,
    /// byte length, then the fields.
    #[test]
    fn location_filter_parameter_block_golden() {
        let mut params = KeyValuePairs::default();
        params
            .set_location_filter(&LocationFilter::Absolute {
                start: Location::new(10, 3),
                end_group: Some(12),
                end_object: None,
            })
            .unwrap();
        let mut buf = BytesMut::new();
        params.encode_message_params(&mut buf).unwrap();
        assert_eq!(buf.to_vec(), vec![0x01, 0x21, 0x03, 0x0a, 0x03, 0x02]);

        let decoded = KeyValuePairs::decode_message_params(&mut buf.freeze()).unwrap();
        assert_eq!(
            decoded.location_filter().unwrap(),
            params.location_filter().unwrap()
        );
    }

    #[test]
    fn location_filter_rejects_malformed_values() {
        // More than four fields.
        assert!(decode_filter(&[1, 2, 3, 4, 5]).is_err());
        // StartGroup + EndGroupDelta overflows 2^64 - 1.
        let mut overflow = vec![0xff; 9];
        overflow.extend_from_slice(&[0x00, 0x01]);
        assert!(decode_filter(&overflow).is_err());
        // A truncated vi64 inside the length.
        assert!(decode_filter(&[0x0a, 0x80]).is_err());
        // An integer-typed value where a length-prefixed one is required.
        let mut params = KeyValuePairs::default();
        params.set_intvalue(parameter_type::LOCATION_FILTER, 1);
        assert!(params.location_filter().is_err());
    }

    #[test]
    fn location_filter_encoding_edge_cases() {
        // Open-ended from {0, 0} is unfiltered, not NextObject.
        let from_origin = LocationFilter::Absolute {
            start: Location::new(0, 0),
            end_group: None,
            end_object: None,
        };
        assert!(encode_bytes_value(&from_origin).unwrap().is_empty());
        // EndObject needs an end group, and the end group cannot precede the start.
        assert!(encode_bytes_value(&LocationFilter::Absolute {
            start: Location::new(5, 0),
            end_group: None,
            end_object: Some(1),
        })
        .is_err());
        assert!(encode_bytes_value(&LocationFilter::Absolute {
            start: Location::new(5, 0),
            end_group: Some(4),
            end_object: None,
        })
        .is_err());
    }

    #[test]
    fn rendezvous_timeout_rejects_a_bytes_value() {
        let mut params = KeyValuePairs::default();
        params.set_bytesvalue(parameter_type::RENDEZVOUS_TIMEOUT, vec![1, 2, 3]);

        assert!(params.rendezvous_timeout().is_err());
    }

    /// Absent means the spec default of 0: do not hold, answer immediately.
    #[test]
    fn rendezvous_timeout_absent_is_none() {
        assert_eq!(KeyValuePairs::default().rendezvous_timeout().unwrap(), None);
    }

    #[test]
    fn largest_object_uses_two_inline_varints() {
        let mut params = KeyValuePairs::default();
        params.set_largest_object(Location::new(4, 5)).unwrap();
        let mut encoded = BytesMut::new();

        params.encode_message_params(&mut encoded).unwrap();

        assert_eq!(
            encoded.as_ref(),
            &[1, parameter_type::LARGEST_OBJECT as u8, 4, 5]
        );
        assert_eq!(
            KeyValuePairs::decode_message_params(&mut encoded)
                .unwrap()
                .largest_object()
                .unwrap(),
            Some(Location::new(4, 5))
        );
    }

    #[test]
    fn track_extensions_methods_round_trip_typed_values() {
        let mut extensions = TrackExtensions::default();

        extensions.set_delivery_timeout(10);
        extensions.set_max_cache_duration(20);
        extensions.set_default_publisher_priority(30);
        extensions
            .set_default_publisher_group_order(GroupOrder::Ascending)
            .unwrap();
        extensions.set_dynamic_groups(true);

        assert_eq!(extensions.delivery_timeout().unwrap(), Some(10));
        assert_eq!(extensions.max_cache_duration().unwrap(), Some(20));
        assert_eq!(extensions.default_publisher_priority().unwrap(), Some(30));
        assert_eq!(
            extensions.default_publisher_group_order().unwrap(),
            Some(GroupOrder::Ascending)
        );
        assert_eq!(extensions.dynamic_groups().unwrap(), Some(true));
    }

    #[test]
    fn track_extensions_rejects_publisher_group_order_sentinel() {
        let mut extensions = TrackExtensions::default();

        assert!(matches!(
            extensions.set_default_publisher_group_order(GroupOrder::Publisher),
            Err(EncodeError::InvalidValue)
        ));
    }

    #[test]
    fn track_extensions_contains_only_supported_types() {
        let mut extensions = TrackExtensions::default();
        let supported = [extension_type::DEFAULT_PUBLISHER_PRIORITY];

        assert!(extensions.contains_only(&supported));
        extensions.set_default_publisher_priority(30);
        assert!(extensions.contains_only(&supported));
        extensions.set_delivery_timeout(10);
        assert!(!extensions.contains_only(&supported));
    }
}
