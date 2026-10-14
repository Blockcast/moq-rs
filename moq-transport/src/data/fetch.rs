// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{Decode, DecodeError, Encode, EncodeError};
use crate::data::{ExtensionHeaders, StreamHeaderType};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FetchHeader {
    /// Subgroup Header Type
    pub header_type: StreamHeaderType,

    /// The fetch request Id number
    pub request_id: u64,
}

// Note:  Not using the Decode trait, since we need to know the header_type to properly parse this, and it
//        is read before knowing we need to decode this.
impl FetchHeader {
    pub fn decode<R: bytes::Buf>(
        header_type: StreamHeaderType,
        r: &mut R,
    ) -> Result<Self, DecodeError> {
        let request_id = u64::decode(r)?;

        Ok(Self {
            header_type,
            request_id,
        })
    }
}

impl Encode for FetchHeader {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        self.header_type.encode(w)?;
        self.request_id.encode(w)?;

        Ok(())
    }
}

/// One Object on a FETCH stream after the FETCH_HEADER (draft-20 §11.4.4).
/// The payload follows on the stream and is not part of this struct.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FetchObject {
    pub group_id: u64,

    /// `None` for an Object with the Datagram forwarding preference (flag 0x40).
    pub subgroup_id: Option<u64>,

    pub object_id: u64,

    /// Publisher priority, where **smaller** values are sent first.
    pub publisher_priority: u8,

    pub properties: ExtensionHeaders,

    pub payload_length: usize,
}

/// Status of a range of Objects that the FETCH stream does not serialize
/// (draft-20 §11.4.4.2, Table 7).
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum FetchRangeKind {
    NonExistent = 0x8C,
    Unknown = 0x10C,
    TimedOut = 0x20C,
}

/// An item on a FETCH stream.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FetchItem {
    Object(FetchObject),
    /// All Objects after the last serialized one, up to and including this
    /// Location, have the given status.
    EndOfRange {
        kind: FetchRangeKind,
        group_id: u64,
        object_id: u64,
    },
}

const FLAG_SUBGROUP_MASK: u64 = 0x03;
const FLAG_OBJECT_ID: u64 = 0x04;
const FLAG_GROUP_ID: u64 = 0x08;
const FLAG_PRIORITY: u64 = 0x10;
const FLAG_PROPERTIES: u64 = 0x20;
const FLAG_DATAGRAM: u64 = 0x40;

/// The fields a FETCH stream item can reference from the prior Object.
/// Every field is delta-coded against this, so the codec is stateful and one
/// instance serves one stream.
#[derive(Debug, Clone, Default)]
pub struct FetchStreamState {
    descending: bool,
    /// Group and Object ID of the prior Object or End of Range.
    location: Option<(u64, u64)>,
    /// Subgroup ID of the last actual (non-datagram) Object.
    subgroup_id: Option<u64>,
    /// Priority of the last actual Object.
    priority: Option<u8>,
}

impl FetchStreamState {
    /// `descending` is the Group Order of the fetch (§10.2.8).
    pub fn new(descending: bool) -> Self {
        Self {
            descending,
            ..Self::default()
        }
    }

    fn group_from_delta(&self, prior: u64, delta: u64) -> Option<u64> {
        let step = delta.checked_add(1)?;
        if self.descending {
            prior.checked_sub(step)
        } else {
            prior.checked_add(step)
        }
    }

    fn group_to_delta(&self, prior: u64, group_id: u64) -> Option<u64> {
        let step = if self.descending {
            prior.checked_sub(group_id)?
        } else {
            group_id.checked_sub(prior)?
        };
        step.checked_sub(1)
    }

    /// Decode the next item. Every error is a PROTOCOL_VIOLATION.
    pub fn decode<R: bytes::Buf>(&mut self, r: &mut R) -> Result<FetchItem, DecodeError> {
        let flags = u64::decode(r)?;
        let range = match flags {
            0x8C => Some(FetchRangeKind::NonExistent),
            0x10C => Some(FetchRangeKind::Unknown),
            0x20C => Some(FetchRangeKind::TimedOut),
            f if f >= 128 => return Err(DecodeError::InvalidValue),
            _ => None,
        };

        // End of Range values are not flags (Table 7); its Group and Object
        // IDs are read as absolute, as in the draft-20 reference relay. Only
        // that reading can express a range ending in the prior Object's group.
        if let Some(kind) = range {
            let group_id = u64::decode(r)?;
            let object_id = u64::decode(r)?;
            // §11.4.4.2: the next Object's prior Group and Object IDs are these.
            self.location = Some((group_id, object_id));
            return Ok(FetchItem::EndOfRange {
                kind,
                group_id,
                object_id,
            });
        }

        // The first item must carry absolute Group and Object IDs.
        let has_group = flags & FLAG_GROUP_ID != 0;
        let has_object = flags & FLAG_OBJECT_ID != 0;
        if self.location.is_none() && !(has_group && has_object) {
            return Err(DecodeError::InvalidValue);
        }

        let group_delta = if has_group {
            Some(u64::decode(r)?)
        } else {
            None
        };

        // A datagram Object has no Subgroup ID.
        let subgroup_id = if flags & FLAG_DATAGRAM != 0 {
            None
        } else {
            Some(match flags & FLAG_SUBGROUP_MASK {
                0x00 => 0,
                0x01 => self.subgroup_id.ok_or(DecodeError::InvalidValue)?,
                0x02 => self
                    .subgroup_id
                    .ok_or(DecodeError::InvalidValue)?
                    .checked_add(1)
                    .ok_or(DecodeError::InvalidValue)?,
                _ => u64::decode(r)?,
            })
        };

        let object_delta = if has_object {
            Some(u64::decode(r)?)
        } else {
            None
        };

        let (group_id, object_id) = match (self.location, group_delta) {
            (None, Some(group)) => (group, object_delta.expect("checked above")),
            (Some((prior_group, prior_object)), Some(delta)) => {
                let group = self
                    .group_from_delta(prior_group, delta)
                    .ok_or(DecodeError::InvalidValue)?;
                let object = match object_delta {
                    Some(object) => object,
                    None => prior_object
                        .checked_add(1)
                        .ok_or(DecodeError::InvalidValue)?,
                };
                (group, object)
            }
            (Some((prior_group, prior_object)), None) => (
                prior_group,
                prior_object
                    .checked_add(object_delta.unwrap_or(1))
                    .ok_or(DecodeError::InvalidValue)?,
            ),
            (None, None) => unreachable!("checked above"),
        };
        self.location = Some((group_id, object_id));

        let publisher_priority = if flags & FLAG_PRIORITY != 0 {
            u8::decode(r)?
        } else {
            self.priority.ok_or(DecodeError::InvalidValue)?
        };
        let properties = if flags & FLAG_PROPERTIES != 0 {
            ExtensionHeaders::decode(r)?
        } else {
            ExtensionHeaders::new()
        };
        let payload_length = usize::decode(r)?;

        // ponytail: a datagram Object leaves the prior Subgroup ID as is; the
        // spec does not say what a later 0x01/0x02 refers to after one.
        if subgroup_id.is_some() {
            self.subgroup_id = subgroup_id;
        }
        self.priority = Some(publisher_priority);

        Ok(FetchItem::Object(FetchObject {
            group_id,
            subgroup_id,
            object_id,
            publisher_priority,
            properties,
            payload_length,
        }))
    }

    /// Encode the next item with the shortest flags the prior Object allows.
    /// Groups must follow the stream's Group Order and Object IDs must not
    /// decrease within a group.
    pub fn encode<W: bytes::BufMut>(
        &mut self,
        item: &FetchItem,
        w: &mut W,
    ) -> Result<(), EncodeError> {
        let object = match item {
            FetchItem::EndOfRange {
                kind,
                group_id,
                object_id,
            } => {
                (*kind as u64).encode(w)?;
                group_id.encode(w)?;
                object_id.encode(w)?;
                self.location = Some((*group_id, *object_id));
                return Ok(());
            }
            FetchItem::Object(o) => o,
        };
        let (group_id, object_id) = (object.group_id, object.object_id);

        let (group_delta, object_delta) = match self.location {
            None => (Some(group_id), Some(object_id)),
            Some((prior_group, prior_object)) if prior_group == group_id => {
                let delta = object_id
                    .checked_sub(prior_object)
                    .ok_or(EncodeError::InvalidValue)?;
                (None, (delta != 1).then_some(delta))
            }
            Some((prior_group, prior_object)) => {
                let delta = self
                    .group_to_delta(prior_group, group_id)
                    .ok_or(EncodeError::InvalidValue)?;
                let object = (prior_object.checked_add(1) != Some(object_id)).then_some(object_id);
                (Some(delta), object)
            }
        };

        let mut flags = 0;
        if group_delta.is_some() {
            flags |= FLAG_GROUP_ID;
        }
        if object_delta.is_some() {
            flags |= FLAG_OBJECT_ID;
        }

        let subgroup = match object.subgroup_id {
            None => {
                flags |= FLAG_DATAGRAM;
                None
            }
            Some(0) => None,
            Some(id) if Some(id) == self.subgroup_id => {
                flags |= 0x01;
                None
            }
            Some(id) if self.subgroup_id.and_then(|s| s.checked_add(1)) == Some(id) => {
                flags |= 0x02;
                None
            }
            Some(id) => {
                flags |= 0x03;
                Some(id)
            }
        };
        if self.priority != Some(object.publisher_priority) {
            flags |= FLAG_PRIORITY;
        }
        if !object.properties.is_empty() {
            flags |= FLAG_PROPERTIES;
        }

        flags.encode(w)?;
        if let Some(delta) = group_delta {
            delta.encode(w)?;
        }
        if let Some(id) = subgroup {
            id.encode(w)?;
        }
        if let Some(delta) = object_delta {
            delta.encode(w)?;
        }
        if flags & FLAG_PRIORITY != 0 {
            object.publisher_priority.encode(w)?;
        }
        if flags & FLAG_PROPERTIES != 0 {
            object.properties.encode(w)?;
        }
        object.payload_length.encode(w)?;

        self.location = Some((group_id, object_id));
        if object.subgroup_id.is_some() {
            self.subgroup_id = object.subgroup_id;
        }
        self.priority = Some(object.publisher_priority);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Bytes, BytesMut};

    fn obj(group_id: u64, subgroup_id: Option<u64>, object_id: u64, prio: u8) -> FetchItem {
        FetchItem::Object(FetchObject {
            group_id,
            subgroup_id,
            object_id,
            publisher_priority: prio,
            properties: ExtensionHeaders::new(),
            payload_length: 4,
        })
    }

    fn vi(v: u64) -> Vec<u8> {
        let mut b = BytesMut::new();
        v.encode(&mut b).unwrap();
        b.to_vec()
    }

    fn decode_all(descending: bool, bytes: &[u8]) -> Result<Vec<FetchItem>, DecodeError> {
        let mut state = FetchStreamState::new(descending);
        let mut buf = Bytes::copy_from_slice(bytes);
        let mut out = vec![];
        while buf.has_remaining() {
            out.push(state.decode(&mut buf)?);
        }
        Ok(out)
    }

    use bytes::Buf;

    /// Hand-built §11.4.4 stream: absolute first Object, implicit next Object,
    /// a Group ID Delta, an End of Range, and an explicit Subgroup ID after it.
    #[test]
    fn golden_ascending_stream() {
        let items = vec![
            obj(5, Some(0), 0, 7),
            obj(5, Some(0), 1, 7),
            obj(6, Some(0), 0, 7),
            FetchItem::EndOfRange {
                kind: FetchRangeKind::NonExistent,
                group_id: 8,
                object_id: 0,
            },
            obj(9, Some(2), 3, 7),
        ];
        let mut wire = vec![0x1C, 5, 0, 7, 4]; // G|O|P, group 5, object 0, prio 7
        wire.extend([0x00, 4]); // subgroup 0, object prior+1, priority prior
        wire.extend([0x0C, 0, 0, 4]); // G|O: group 5+0+1, object 0
        wire.extend(vi(0x8C));
        wire.extend([8, 0]); // absolute End of Range
        wire.extend([0x0F, 0, 2, 3, 4]); // G|O|subgroup field: group 8+0+1, subgroup 2, object 3

        let mut state = FetchStreamState::new(false);
        let mut buf = BytesMut::new();
        for item in &items {
            state.encode(item, &mut buf).unwrap();
        }
        assert_eq!(buf.to_vec(), wire);
        assert_eq!(decode_all(false, &wire).unwrap(), items);
    }

    #[test]
    fn subgroup_modes_priority_and_properties_roundtrip() {
        let mut props = ExtensionHeaders::new();
        props.set_intvalue(4, 9);
        let with_props = FetchItem::Object(FetchObject {
            group_id: 1,
            subgroup_id: Some(4),
            object_id: 7,
            publisher_priority: 200,
            properties: props,
            payload_length: 0,
        });
        let items = vec![
            obj(1, Some(3), 2, 1),
            obj(1, Some(3), 4, 1), // 0x01, object delta 2
            obj(1, Some(4), 5, 9), // 0x02, new priority
            with_props,
            obj(1, None, 9, 9), // datagram
        ];
        let mut state = FetchStreamState::new(false);
        let mut buf = BytesMut::new();
        for item in &items {
            state.encode(item, &mut buf).unwrap();
        }
        assert_eq!(decode_all(false, &buf).unwrap(), items);
    }

    #[test]
    fn descending_group_order_roundtrip() {
        let items = vec![
            obj(10, Some(0), 0, 1),
            obj(7, Some(0), 1, 1),
            obj(0, Some(0), 5, 1),
        ];
        let mut state = FetchStreamState::new(true);
        let mut buf = BytesMut::new();
        for item in &items {
            state.encode(item, &mut buf).unwrap();
        }
        // group 10 -> 7 is delta 2 (10 - (2+1)), object 1 is prior+1
        assert_eq!(&buf[5..7], &[0x08, 2]);
        assert_eq!(decode_all(true, &buf).unwrap(), items);
        // The same bytes in ascending order decode to different groups.
        assert_ne!(decode_all(false, &buf).ok(), Some(items));
    }

    #[test]
    fn datagram_flag_ignores_subgroup_bits() {
        let FetchItem::Object(o) = &decode_all(false, &[0x5F, 1, 2, 3, 0]).unwrap()[0] else {
            panic!()
        };
        assert_eq!((o.group_id, o.subgroup_id, o.object_id), (1, None, 2));
    }

    #[test]
    fn malformed_streams_are_protocol_violations() {
        let cases: &[(&str, bool, Vec<u8>)] = &[
            ("first Object without Group ID", false, vec![0x14, 0, 1, 0]),
            ("first Object without Object ID", false, vec![0x18, 0, 1, 0]),
            (
                "first Object refers to prior Subgroup",
                false,
                vec![0x1D, 0, 0, 1, 0],
            ),
            (
                "first Object refers to prior Priority",
                false,
                vec![0x0C, 0, 0, 0],
            ),
            // Each would decode as an Object if its value were read as flags.
            (
                "undefined value above 127",
                false,
                [vec![0x1C, 0, 0, 1, 0], vi(0x80), vec![0]].concat(),
            ),
            (
                "undefined value near 0x8C",
                false,
                [vec![0x1C, 0, 0, 1, 0], vi(0x8D), vec![0, 0, 0]].concat(),
            ),
            // After an End of Range with no prior Object (§11.4.4.2).
            (
                "prior Subgroup after leading End of Range",
                false,
                [vi(0x8C), vec![0, 0, 0x11, 1, 0]].concat(),
            ),
            (
                "prior Subgroup plus one after leading End of Range",
                false,
                [vi(0x8C), vec![0, 0, 0x12, 1, 0]].concat(),
            ),
            (
                "prior Priority after leading End of Range",
                false,
                [vi(0x8C), vec![0, 0, 0x00, 0]].concat(),
            ),
            (
                "descending group below 0",
                true,
                vec![0x1C, 1, 0, 1, 0, 0x08, 1, 0],
            ),
        ];
        for (name, descending, wire) in cases {
            assert!(
                decode_all(*descending, wire).is_err(),
                "{name} must be rejected"
            );
        }

        let mut overflow = vec![0x1C];
        overflow.extend(vi(u64::MAX));
        overflow.extend([0, 1, 0, 0x08, 0]);
        assert!(
            decode_all(false, &overflow).is_err(),
            "ascending group overflow"
        );

        let mut obj_overflow = vec![0x1C, 0];
        obj_overflow.extend(vi(u64::MAX));
        obj_overflow.extend([1, 0, 0x00, 0]);
        assert!(
            decode_all(false, &obj_overflow).is_err(),
            "object ID overflow"
        );
    }

    #[test]
    fn encoder_rejects_out_of_order_groups() {
        let mut state = FetchStreamState::new(false);
        let mut buf = BytesMut::new();
        state.encode(&obj(5, Some(0), 0, 1), &mut buf).unwrap();
        assert!(state.encode(&obj(4, Some(0), 0, 1), &mut buf).is_err());
        assert!(state.encode(&obj(5, Some(0), 0, 1), &mut buf).is_ok());
    }
}
