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
    parameter_type::INCLUDE_PROPERTIES,  // 0x35
];

const TWO_VARINT_VALUE_PARAMETER_TYPES: &[u64] = &[parameter_type::LARGEST_OBJECT];

/// Even-typed parameters that are length-prefixed anyway (draft-20 §5.1.4).
/// Every Range Filter carries a Length, so reading these two as varints would
/// desynchronise the rest of the block.
const BYTES_VALUE_PARAMETER_TYPES: &[u64] = &[
    parameter_type::OBJECTID_FILTER,
    parameter_type::OBJECT_PROPERTY_FILTER,
];

/// The five Range Filter parameter types (draft-20 §5.1.4).
pub const RANGE_FILTER_TYPES: &[u64] = &[
    parameter_type::SUBGROUP_FILTER,
    parameter_type::OBJECTID_FILTER,
    parameter_type::PRIORITY_FILTER,
    parameter_type::OBJECT_PROPERTY_FILTER,
    parameter_type::TRACK_PROPERTY_FILTER,
];

/// The MAX_FILTER_RANGES this endpoint grants its peer. Nothing evaluates Range
/// Filters yet, so this endpoint does not send the option, and the default of 0
/// obliges the peer not to send any (draft-20 §10.3.1.6).
pub const LOCAL_MAX_FILTER_RANGES: u64 = 0;

const OBJECT_FILTER_MESSAGES: &[u64] = &[
    wire_id::Subscribe,
    wire_id::Fetch,
    wire_id::SubscribeTracks,
    wire_id::RequestUpdate,
];

/// Message scopes enforced for supported parameters with restricted use.
/// Unknown parameter types remain available for negotiated extensions.
const MESSAGE_PARAMETER_SCOPES: &[(u64, &[u64])] = &[
    (parameter_type::RENDEZVOUS_TIMEOUT, &[wire_id::Subscribe]),
    (parameter_type::SUBGROUP_FILTER, OBJECT_FILTER_MESSAGES),
    (parameter_type::OBJECTID_FILTER, OBJECT_FILTER_MESSAGES),
    (parameter_type::PRIORITY_FILTER, OBJECT_FILTER_MESSAGES),
    (
        parameter_type::OBJECT_PROPERTY_FILTER,
        OBJECT_FILTER_MESSAGES,
    ),
    (
        parameter_type::TRACK_PROPERTY_FILTER,
        &[wire_id::SubscribeTracks, wire_id::RequestUpdate],
    ),
    (
        parameter_type::INCLUDE_PROPERTIES,
        &[
            wire_id::Subscribe,
            wire_id::TrackStatus,
            wire_id::Fetch,
            wire_id::SubscribeTracks,
        ],
    ),
];

/// One Range Filter parameter value (draft-20 §5.1.4). A zero Length means "no
/// filter" and decodes to `None` rather than to a value of this type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeFilter {
    pub set_id: u8,
    /// The even Property Type of OBJECT_/TRACK_PROPERTY_FILTER; `None` for the
    /// other three types, which carry no such field.
    pub property_type: Option<u64>,
    /// Inclusive, absolute `(start, end)` pairs, OR'd. Only the last may have
    /// `end == None`, meaning open-ended.
    pub ranges: Vec<(u64, Option<u64>)>,
}

impl RangeFilter {
    fn has_property_type(filter_type: u64) -> bool {
        matches!(
            filter_type,
            parameter_type::OBJECT_PROPERTY_FILTER | parameter_type::TRACK_PROPERTY_FILTER
        )
    }

    /// Decode the value of Range Filter parameter `filter_type`. Every error is
    /// an INVALID_FILTER: the Length already bounds the value, so a malformed
    /// filter does not desynchronise the parameter block.
    pub fn decode_value(filter_type: u64, value: &[u8]) -> Result<Option<Self>, DecodeError> {
        let mut r = bytes::Bytes::copy_from_slice(value);
        if !r.has_remaining() {
            return Ok(None);
        }
        let set_id = u8::decode(&mut r)?;
        let property_type = if Self::has_property_type(filter_type) {
            let property_type = u64::decode(&mut r)?;
            // §10.2.13-14: only even (integer-valued) Properties can be ranged.
            if property_type % 2 != 0 {
                return Err(DecodeError::InvalidFilterType);
            }
            Some(property_type)
        } else {
            None
        };

        let mut ranges = Vec::new();
        let mut prev_end = 0u64;
        while r.has_remaining() {
            // Start is a delta from the prior End (or 0); End from its own Start.
            let start = prev_end
                .checked_add(u64::decode(&mut r)?)
                .ok_or(DecodeError::InvalidFilterType)?;
            let end = if r.has_remaining() {
                let end = start
                    .checked_add(u64::decode(&mut r)?)
                    .ok_or(DecodeError::InvalidFilterType)?;
                prev_end = end;
                Some(end)
            } else {
                None
            };
            // §10.2.12: Publisher Priority is 8 bits.
            if filter_type == parameter_type::PRIORITY_FILTER
                && end.unwrap_or(start) > u8::MAX.into()
            {
                return Err(DecodeError::InvalidFilterType);
            }
            ranges.push((start, end));
        }

        Ok(Some(Self {
            set_id,
            property_type,
            ranges,
        }))
    }

    pub fn encode_value(&self, filter_type: u64) -> Result<Vec<u8>, EncodeError> {
        if self.property_type.is_some() != Self::has_property_type(filter_type) {
            return Err(EncodeError::InvalidValue);
        }
        let mut w = Vec::new();
        self.set_id.encode(&mut w)?;
        if let Some(property_type) = self.property_type {
            property_type.encode(&mut w)?;
        }
        let mut prev_end = 0u64;
        for (i, &(start, end)) in self.ranges.iter().enumerate() {
            start
                .checked_sub(prev_end)
                .ok_or(EncodeError::InvalidValue)?
                .encode(&mut w)?;
            match end {
                Some(end) => {
                    end.checked_sub(start)
                        .ok_or(EncodeError::InvalidValue)?
                        .encode(&mut w)?;
                    prev_end = end;
                }
                None if i + 1 == self.ranges.len() => {}
                None => return Err(EncodeError::InvalidValue),
            }
        }
        Ok(w)
    }
}

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
                if end_group.is_none() && end_object.is_some() {
                    return Err(EncodeError::MissingField("EndGroup".to_string()));
                }
                // An open-ended filter from {0, 0} is unfiltered, and as two
                // fields it would read back as NextObject.
                if start == Location::new(0, 0) && end_group.is_none() {
                    return Ok(());
                }
                start.group_id.encode(w)?;
                start.object_id.encode(w)?;
                match (end_group, end_object) {
                    (None, _) => {}
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
            BYTES_VALUE_PARAMETER_TYPES,
        )
    }

    /// Encode a draft-18 §10.2 message-parameter block. Counterpart to
    /// [`decode_message_params`](KeyValuePairs::decode_message_params).
    pub fn encode_message_params<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.encode_with_value_types(
            w,
            U8_VALUE_PARAMETER_TYPES,
            TWO_VARINT_VALUE_PARAMETER_TYPES,
            BYTES_VALUE_PARAMETER_TYPES,
        )
    }

    /// Whether every parameter may appear on `message_type`, and every value
    /// whose range violation is a PROTOCOL_VIOLATION is in range.
    pub(crate) fn parameters_allowed_on(&self, message_type: u64) -> bool {
        let scoped = if message_type == wire_id::SubscribeTracks {
            self.0.iter().all(|parameter| {
                matches!(
                    parameter.key,
                    parameter_type::AUTHORIZATION_TOKEN
                        | parameter_type::FORWARD
                        | parameter_type::INCLUDE_PROPERTIES
                ) || RANGE_FILTER_TYPES.contains(&parameter.key)
            })
        } else {
            self.0.iter().all(|parameter| {
                MESSAGE_PARAMETER_SCOPES
                    .iter()
                    .find(|(parameter_type, _)| *parameter_type == parameter.key)
                    .is_none_or(|(_, message_types)| message_types.contains(&message_type))
            })
        };
        scoped && self.include_properties().is_ok()
    }

    /// INCLUDE_PROPERTIES (draft-20 §10.2.21). A value other than 0 or 1 closes
    /// the session with PROTOCOL_VIOLATION.
    pub fn include_properties(&self) -> Result<Option<bool>, DecodeError> {
        match self.int_parameter(parameter_type::INCLUDE_PROPERTIES)? {
            Some(0) => Ok(Some(false)),
            Some(1) => Ok(Some(true)),
            Some(_) => Err(DecodeError::InvalidParameter),
            None => Ok(None),
        }
    }

    pub fn set_include_properties(&mut self, include: bool) {
        self.set_intvalue(parameter_type::INCLUDE_PROPERTIES, include.into());
    }

    /// Append one Range Filter parameter; `None` encodes the zero-Length form.
    /// Filters may repeat, so this never replaces an existing one.
    pub fn push_range_filter(
        &mut self,
        filter_type: u64,
        filter: Option<&RangeFilter>,
    ) -> Result<(), EncodeError> {
        if !RANGE_FILTER_TYPES.contains(&filter_type) {
            return Err(EncodeError::InvalidValue);
        }
        let value = match filter {
            Some(filter) => filter.encode_value(filter_type)?,
            None => Vec::new(),
        };
        self.0.push(KeyValuePair::new_bytes(filter_type, value));
        Ok(())
    }

    /// Every Range Filter in the block as `(type, filter)`, in wire order.
    ///
    /// An error means REQUEST_ERROR with INVALID_FILTER (draft-20 §5.1.4): a
    /// malformed value, a repeated (Type, SetID, Property Type), or more
    /// Ranges than `max_ranges`, the MAX_FILTER_RANGES this endpoint granted.
    /// With `max_ranges == 0` the peer may send no filter parameter at all.
    pub fn range_filters(
        &self,
        max_ranges: u64,
    ) -> Result<Vec<(u64, Option<RangeFilter>)>, DecodeError> {
        let mut filters = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut total_ranges = 0u64;
        for parameter in self
            .0
            .iter()
            .filter(|parameter| RANGE_FILTER_TYPES.contains(&parameter.key))
        {
            if max_ranges == 0 {
                return Err(DecodeError::InvalidFilterType);
            }
            let Value::BytesValue(value) = &parameter.value else {
                return Err(DecodeError::InvalidFilterType);
            };
            let filter = RangeFilter::decode_value(parameter.key, value)
                .map_err(|_| DecodeError::InvalidFilterType)?;
            if let Some(filter) = &filter {
                if !seen.insert((parameter.key, filter.set_id, filter.property_type)) {
                    return Err(DecodeError::InvalidFilterType);
                }
                total_ranges += filter.ranges.len() as u64;
                if total_ranges > max_ranges {
                    return Err(DecodeError::InvalidFilterType);
                }
            }
            filters.push((parameter.key, filter));
        }
        Ok(filters)
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

    fn range(set_id: u8, property_type: Option<u64>, ranges: &[(u64, Option<u64>)]) -> RangeFilter {
        RangeFilter {
            set_id,
            property_type,
            ranges: ranges.to_vec(),
        }
    }

    /// Golden vectors for Range Filter values (draft-20 §5.1.4), including the
    /// spec's own example: ranges 3-5 and 10-15 encode as 3, 2, 5, 5.
    #[test]
    fn range_filter_golden_vectors() {
        use parameter_type::*;
        let cases: &[(u64, &[u8], RangeFilter)] = &[
            (
                SUBGROUP_FILTER,
                &[0x00, 0x03, 0x02, 0x05, 0x05],
                range(0, None, &[(3, Some(5)), (10, Some(15))]),
            ),
            // An omitted End leaves the last Range open-ended.
            (OBJECTID_FILTER, &[0x07, 0x04], range(7, None, &[(4, None)])),
            (
                PRIORITY_FILTER,
                &[0x01, 0x40, 0x3f],
                range(1, None, &[(64, Some(127))]),
            ),
            // Property Type follows SetID.
            (
                OBJECT_PROPERTY_FILTER,
                &[0x02, 0x08, 0x01, 0x00],
                range(2, Some(8), &[(1, Some(1))]),
            ),
            (
                TRACK_PROPERTY_FILTER,
                &[0xff, 0x0a, 0x00, 0x0a, 0x05],
                range(255, Some(10), &[(0, Some(10)), (15, None)]),
            ),
        ];
        for (filter_type, bytes, filter) in cases {
            assert_eq!(
                RangeFilter::decode_value(*filter_type, bytes).unwrap(),
                Some(filter.clone()),
                "decode {filter_type:#x} {bytes:02x?}"
            );
            assert_eq!(
                filter.encode_value(*filter_type).unwrap(),
                bytes.to_vec(),
                "encode {filter_type:#x} {filter:?}"
            );
        }
        // Length 0 is no filter.
        assert_eq!(
            RangeFilter::decode_value(SUBGROUP_FILTER, &[]).unwrap(),
            None
        );
    }

    #[test]
    fn range_filter_rejects_malformed_values() {
        use parameter_type::*;
        let vi = |v: u64| {
            let mut w = Vec::new();
            v.encode(&mut w).unwrap();
            w
        };
        let max = vi(u64::MAX);
        let cases: &[(u64, Vec<u8>)] = &[
            // Start + End delta overflows 2^64 - 1.
            (
                SUBGROUP_FILTER,
                [vec![0x00], max.clone(), vec![0x01]].concat(),
            ),
            // The next Start delta overflows from the prior End.
            (
                OBJECTID_FILTER,
                [vec![0x00], max.clone(), vec![0x00, 0x01]].concat(),
            ),
            // Publisher Priority is 8 bits: a Start, or an End, of 256.
            (PRIORITY_FILTER, [vec![0x00], vi(256)].concat()),
            (PRIORITY_FILTER, [vec![0x00, 0x00], vi(256)].concat()),
            // Property Type must be even.
            (OBJECT_PROPERTY_FILTER, vec![0x00, 0x09, 0x01]),
            (TRACK_PROPERTY_FILTER, vec![0x00, 0x0b]),
            // A truncated vi64 inside the Length.
            (SUBGROUP_FILTER, vec![0x00, 0x80]),
        ];
        for (filter_type, bytes) in cases {
            assert!(
                RangeFilter::decode_value(*filter_type, bytes).is_err(),
                "{filter_type:#x} {bytes:02x?} must be INVALID_FILTER"
            );
        }
        // An open-ended Range anywhere but last is not encodable.
        assert!(range(0, None, &[(1, None), (5, Some(6))])
            .encode_value(SUBGROUP_FILTER)
            .is_err());
        // Property Type present exactly on the property filters.
        assert!(range(0, Some(2), &[])
            .encode_value(SUBGROUP_FILTER)
            .is_err());
        assert!(range(0, None, &[])
            .encode_value(TRACK_PROPERTY_FILTER)
            .is_err());
    }

    /// 0x26 and 0x28 are even but length-prefixed. Read as varints, the
    /// OBJECTID_FILTER below would consume its Length as the value and parse the
    /// SetID as the next parameter's Type Delta.
    #[test]
    fn even_range_filters_are_length_prefixed_in_the_block() {
        let mut params = KeyValuePairs::default();
        params
            .push_range_filter(
                parameter_type::OBJECTID_FILTER,
                Some(&range(3, None, &[(4, Some(6))])),
            )
            .unwrap();
        params.set_include_properties(false);
        let mut buf = BytesMut::new();
        params.encode_message_params(&mut buf).unwrap();
        // count 2; 0x26 len 3 [03 04 02]; delta 0x0f -> 0x35, uint8 0.
        assert_eq!(
            buf.to_vec(),
            vec![0x02, 0x26, 0x03, 0x03, 0x04, 0x02, 0x0f, 0x00]
        );

        let decoded = KeyValuePairs::decode_message_params(&mut buf.freeze()).unwrap();
        assert_eq!(decoded.include_properties().unwrap(), Some(false));
        assert_eq!(
            decoded.range_filters(16).unwrap(),
            vec![(
                parameter_type::OBJECTID_FILTER,
                Some(range(3, None, &[(4, Some(6))]))
            )]
        );
    }

    #[test]
    fn range_filters_enforce_limit_and_uniqueness() {
        use parameter_type::*;
        let mut params = KeyValuePairs::default();
        params
            .push_range_filter(SUBGROUP_FILTER, Some(&range(0, None, &[(1, Some(2))])))
            .unwrap();
        params
            .push_range_filter(
                SUBGROUP_FILTER,
                Some(&range(1, None, &[(3, Some(4)), (9, None)])),
            )
            .unwrap();
        // Same type, distinct SetIDs: allowed, three Ranges in total.
        assert_eq!(params.range_filters(3).unwrap().len(), 2);
        // MAX_FILTER_RANGES exceeded.
        assert!(params.range_filters(2).is_err());
        // Not granted at all: no filter parameter may be sent.
        assert!(params.range_filters(0).is_err());
        let mut empty = KeyValuePairs::default();
        empty.push_range_filter(SUBGROUP_FILTER, None).unwrap();
        assert!(empty.range_filters(0).is_err());
        assert_eq!(
            empty.range_filters(1).unwrap(),
            vec![(SUBGROUP_FILTER, None)]
        );

        // A repeated (Type, SetID) is INVALID_FILTER.
        params
            .push_range_filter(SUBGROUP_FILTER, Some(&range(1, None, &[(7, Some(7))])))
            .unwrap();
        assert!(params.range_filters(16).is_err());

        // For property filters the Property Type is part of the key.
        let mut props = KeyValuePairs::default();
        for property_type in [2, 4] {
            props
                .push_range_filter(
                    OBJECT_PROPERTY_FILTER,
                    Some(&range(0, Some(property_type), &[(1, None)])),
                )
                .unwrap();
        }
        assert_eq!(props.range_filters(16).unwrap().len(), 2);
        props
            .push_range_filter(
                OBJECT_PROPERTY_FILTER,
                Some(&range(0, Some(4), &[(5, None)])),
            )
            .unwrap();
        assert!(props.range_filters(16).is_err());
    }

    #[test]
    fn include_properties_outside_zero_or_one_is_a_protocol_violation() {
        let mut params = KeyValuePairs::default();
        params.set_include_properties(true);
        assert_eq!(params.include_properties().unwrap(), Some(true));
        assert!(params.parameters_allowed_on(wire_id::Subscribe));

        params.set_intvalue(parameter_type::INCLUDE_PROPERTIES, 2);
        assert!(params.include_properties().is_err());
        assert!(!params.parameters_allowed_on(wire_id::Subscribe));
    }

    #[test]
    fn filter_and_include_properties_scopes() {
        use parameter_type::*;
        let only = |key: u64| {
            let mut params = KeyValuePairs::default();
            if key == INCLUDE_PROPERTIES {
                params.set_include_properties(true);
            } else {
                params.push_range_filter(key, None).unwrap();
            }
            params
        };
        for key in [
            SUBGROUP_FILTER,
            OBJECTID_FILTER,
            PRIORITY_FILTER,
            OBJECT_PROPERTY_FILTER,
        ] {
            for message in [
                wire_id::Subscribe,
                wire_id::Fetch,
                wire_id::SubscribeTracks,
                wire_id::RequestUpdate,
            ] {
                assert!(
                    only(key).parameters_allowed_on(message),
                    "{key:#x} on {message:#x}"
                );
            }
            assert!(!only(key).parameters_allowed_on(wire_id::Publish));
        }
        // TRACK_PROPERTY_FILTER filters PUBLISHes, so only SUBSCRIBE_TRACKS (and
        // its REQUEST_UPDATE) carries it.
        assert!(only(TRACK_PROPERTY_FILTER).parameters_allowed_on(wire_id::SubscribeTracks));
        assert!(only(TRACK_PROPERTY_FILTER).parameters_allowed_on(wire_id::RequestUpdate));
        assert!(!only(TRACK_PROPERTY_FILTER).parameters_allowed_on(wire_id::Subscribe));
        for message in [
            wire_id::Subscribe,
            wire_id::TrackStatus,
            wire_id::Fetch,
            wire_id::SubscribeTracks,
        ] {
            assert!(only(INCLUDE_PROPERTIES).parameters_allowed_on(message));
        }
        assert!(!only(INCLUDE_PROPERTIES).parameters_allowed_on(wire_id::Publish));
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
        for start in [Location::new(5, 0), Location::new(0, 0)] {
            assert!(encode_bytes_value(&LocationFilter::Absolute {
                start,
                end_group: None,
                end_object: Some(1),
            })
            .is_err());
        }
        assert!(encode_bytes_value(&LocationFilter::Absolute {
            start: Location::new(5, 0),
            end_group: Some(4),
            end_object: None,
        })
        .is_err());
    }

    /// 0x04 is even, so the KVP codec carries it as a varint. A peer sending a
    /// byte string under that key is malformed rather than something to coerce.
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
