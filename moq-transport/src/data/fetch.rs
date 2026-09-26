// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! FETCH data stream framing (draft-ietf-moq-transport-16 §10.4.4).
//!
//! A FETCH stream starts with a [`FetchHeader`] and is followed by a sequence
//! of entries, each introduced by a Serialization Flags varint. The flags say
//! which fields are present and which are inherited from the *prior* Object on
//! the same stream, so encoding and decoding are stateful: use one
//! [`FetchObjectEncoder`] or [`FetchObjectDecoder`] per stream.

use crate::coding::{Decode, DecodeError, Encode, EncodeError, Location};
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

/// Serialization Flags bits (§10.4.4.1, Tables 5 and 6).
mod flags {
    /// Two-bit Subgroup ID encoding field.
    pub const SUBGROUP_MASK: u64 = 0x03;
    /// Subgroup ID is zero.
    pub const SUBGROUP_ZERO: u64 = 0x00;
    /// Subgroup ID is the prior Object's Subgroup ID.
    pub const SUBGROUP_PRIOR: u64 = 0x01;
    /// Subgroup ID is the prior Object's Subgroup ID plus one.
    pub const SUBGROUP_PRIOR_PLUS_ONE: u64 = 0x02;
    /// The Subgroup ID field is present.
    pub const SUBGROUP_PRESENT: u64 = 0x03;
    /// Object ID field is present (otherwise the prior Object ID plus one).
    pub const OBJECT_ID: u64 = 0x04;
    /// Group ID field is present (otherwise the prior Object's Group ID).
    pub const GROUP_ID: u64 = 0x08;
    /// Priority field is present (otherwise the prior Object's Priority).
    pub const PRIORITY: u64 = 0x10;
    /// Extensions field is present.
    pub const EXTENSIONS: u64 = 0x20;
    /// Datagram Forwarding Preference: ignore the two least significant bits.
    pub const DATAGRAM: u64 = 0x40;
    /// Values at or above this are not flag bitmasks (§10.4.4, Table 4).
    pub const FIRST_NON_FLAG_VALUE: u64 = 0x80;
    /// End of Non-Existent Range (§10.4.4, Table 4).
    pub const END_OF_NON_EXISTENT_RANGE: u64 = 0x8c;
    /// End of Unknown Range (§10.4.4, Table 4).
    pub const END_OF_UNKNOWN_RANGE: u64 = 0x10c;
}

/// The properties of one Object carried on a FETCH stream (§10.4.4).
///
/// Object Status is absent from FETCH (§10.2.1.1), so every Object here is a
/// Normal Object; a zero `payload_length` is a zero-length Normal Object.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FetchObject {
    pub group_id: u64,

    /// `None` for an Object whose Forwarding Preference is Datagram, which has
    /// no Subgroup ID (§10.2.1, §10.4.4.1).
    pub subgroup_id: Option<u64>,

    pub object_id: u64,

    /// Publisher priority, where **smaller** values are sent first (§7.1).
    pub publisher_priority: u8,

    pub extension_headers: ExtensionHeaders,

    /// Length of the Object Payload that follows the serialized fields.
    pub payload_length: usize,
}

impl FetchObject {
    pub fn location(&self) -> Location {
        Location::new(self.group_id, self.object_id)
    }
}

/// The two End of Range markers (§10.4.4.2).
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum FetchEndOfRange {
    /// Every Object after the prior entry, up to and including the location,
    /// does not exist (0x8C).
    NonExistent,
    /// Every Object after the prior entry, up to and including the location,
    /// has unknown status (0x10C).
    Unknown,
}

/// One entry on a FETCH stream after the FETCH_HEADER.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FetchEntry {
    Object(FetchObject),
    EndOfRange {
        kind: FetchEndOfRange,
        location: Location,
    },
}

/// The fields a later Object may inherit from the prior Object (§10.4.4.1).
#[derive(Debug, Clone, Copy)]
struct PriorObject {
    group_id: u64,
    subgroup_id: Option<u64>,
    object_id: u64,
    publisher_priority: u8,
}

impl PriorObject {
    fn of(object: &FetchObject) -> Self {
        Self {
            group_id: object.group_id,
            subgroup_id: object.subgroup_id,
            object_id: object.object_id,
            publisher_priority: object.publisher_priority,
        }
    }
}

/// Serializes the entries of one FETCH stream (§10.4.4).
///
/// Fields equal to what a receiver would infer from the prior Object are
/// omitted. The first Object, and the first Object after an End of Range
/// marker, carries every field explicitly: §10.4.4.1 makes a first Object that
/// references the prior Object a PROTOCOL_VIOLATION, and draft-16 does not say
/// whether an End of Range marker counts as the prior Object.
#[derive(Debug, Default)]
pub struct FetchObjectEncoder {
    prior: Option<PriorObject>,
}

impl FetchObjectEncoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Encode an Object's serialized fields, up to and including the Object
    /// Payload Length. The caller writes the payload itself.
    pub fn encode_object<W: bytes::BufMut>(
        &mut self,
        object: &FetchObject,
        w: &mut W,
    ) -> Result<(), EncodeError> {
        let prior = self.prior;

        let mut serialization_flags = match (object.subgroup_id, prior) {
            (None, _) => flags::DATAGRAM,
            (Some(0), _) => flags::SUBGROUP_ZERO,
            (
                Some(subgroup_id),
                Some(PriorObject {
                    subgroup_id: Some(prior_subgroup_id),
                    ..
                }),
            ) if subgroup_id == prior_subgroup_id => flags::SUBGROUP_PRIOR,
            (
                Some(subgroup_id),
                Some(PriorObject {
                    subgroup_id: Some(prior_subgroup_id),
                    ..
                }),
            ) if Some(subgroup_id) == prior_subgroup_id.checked_add(1) => {
                flags::SUBGROUP_PRIOR_PLUS_ONE
            }
            (Some(_), _) => flags::SUBGROUP_PRESENT,
        };

        let group_present = prior.is_none_or(|prior| prior.group_id != object.group_id);
        if group_present {
            serialization_flags |= flags::GROUP_ID;
        }
        // An Object ID is inferred as the prior Object ID plus one. Spell it out
        // whenever the Group ID changes, so no receiver has to carry an Object ID
        // across a group boundary.
        let object_present = group_present
            || prior.is_none_or(|prior| prior.object_id.checked_add(1) != Some(object.object_id));
        if object_present {
            serialization_flags |= flags::OBJECT_ID;
        }
        let priority_present =
            prior.is_none_or(|prior| prior.publisher_priority != object.publisher_priority);
        if priority_present {
            serialization_flags |= flags::PRIORITY;
        }
        let extensions_present = !object.extension_headers.is_empty();
        if extensions_present {
            serialization_flags |= flags::EXTENSIONS;
        }

        serialization_flags.encode(w)?;
        if group_present {
            object.group_id.encode(w)?;
        }
        if serialization_flags & flags::DATAGRAM == 0
            && serialization_flags & flags::SUBGROUP_MASK == flags::SUBGROUP_PRESENT
        {
            object
                .subgroup_id
                .ok_or_else(|| EncodeError::MissingField("SubgroupId".to_string()))?
                .encode(w)?;
        }
        if object_present {
            object.object_id.encode(w)?;
        }
        if priority_present {
            object.publisher_priority.encode(w)?;
        }
        if extensions_present {
            object.extension_headers.encode(w)?;
        }
        object.payload_length.encode(w)?;

        self.prior = Some(PriorObject::of(object));
        Ok(())
    }

    /// Encode an End of Range marker (§10.4.4.2): the Serialization Flags
    /// value followed by the Group ID and Object ID of `location`.
    pub fn encode_end_of_range<W: bytes::BufMut>(
        &mut self,
        kind: FetchEndOfRange,
        location: Location,
        w: &mut W,
    ) -> Result<(), EncodeError> {
        let serialization_flags = match kind {
            FetchEndOfRange::NonExistent => flags::END_OF_NON_EXISTENT_RANGE,
            FetchEndOfRange::Unknown => flags::END_OF_UNKNOWN_RANGE,
        };
        serialization_flags.encode(w)?;
        location.group_id.encode(w)?;
        location.object_id.encode(w)?;

        self.prior = None;
        Ok(())
    }
}

/// Parses the entries of one FETCH stream (§10.4.4).
///
/// [`Self::decode`] leaves the decoder unchanged when it fails, including with
/// [`DecodeError::More`], so a caller that buffers a byte stream can retry the
/// same bytes once more have arrived.
#[derive(Debug, Default)]
pub struct FetchObjectDecoder {
    prior: Option<PriorObject>,
}

impl FetchObjectDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Decode one entry from the front of `buf`, returning it with the number
    /// of bytes it occupied. For an Object the Object Payload is *not* part of
    /// that count: the next `payload_length` bytes of the stream belong to it.
    pub fn decode(&mut self, buf: &[u8]) -> Result<(FetchEntry, usize), DecodeError> {
        let mut cursor = buf;
        self.decode_contiguous(&mut cursor)
    }

    fn decode_contiguous(&mut self, r: &mut &[u8]) -> Result<(FetchEntry, usize), DecodeError> {
        let start = r.len();
        let serialization_flags = u64::decode(r)?;

        if serialization_flags >= flags::FIRST_NON_FLAG_VALUE {
            let kind = match serialization_flags {
                flags::END_OF_NON_EXISTENT_RANGE => FetchEndOfRange::NonExistent,
                flags::END_OF_UNKNOWN_RANGE => FetchEndOfRange::Unknown,
                // §10.4.4: any other value is a PROTOCOL_VIOLATION.
                _ => return Err(DecodeError::InvalidValue),
            };
            let group_id = u64::decode(r)?;
            let object_id = u64::decode(r)?;
            let location = Location::new(group_id, object_id);
            // Keep the location as the reference for a following Object that
            // omits its Group or Object ID; the other inheritable fields still
            // come from the last Object.
            if let Some(prior) = &mut self.prior {
                prior.group_id = group_id;
                prior.object_id = object_id;
            }
            return Ok((FetchEntry::EndOfRange { kind, location }, start - r.len()));
        }

        let prior = self.prior;
        // §10.4.4.1: the first Object must not reference fields of a prior one.
        let prior_or_violation = || prior.ok_or(DecodeError::InvalidValue);

        let group_id = if serialization_flags & flags::GROUP_ID != 0 {
            u64::decode(r)?
        } else {
            prior_or_violation()?.group_id
        };

        let subgroup_id = if serialization_flags & flags::DATAGRAM != 0 {
            None
        } else {
            match serialization_flags & flags::SUBGROUP_MASK {
                flags::SUBGROUP_ZERO => Some(0),
                flags::SUBGROUP_PRIOR => Some(
                    prior_or_violation()?
                        .subgroup_id
                        .ok_or(DecodeError::InvalidValue)?,
                ),
                flags::SUBGROUP_PRIOR_PLUS_ONE => Some(
                    prior_or_violation()?
                        .subgroup_id
                        .ok_or(DecodeError::InvalidValue)?
                        .checked_add(1)
                        .ok_or(DecodeError::InvalidValue)?,
                ),
                _ => Some(u64::decode(r)?),
            }
        };

        let object_id = if serialization_flags & flags::OBJECT_ID != 0 {
            u64::decode(r)?
        } else {
            prior_or_violation()?
                .object_id
                .checked_add(1)
                .ok_or(DecodeError::InvalidValue)?
        };

        let publisher_priority = if serialization_flags & flags::PRIORITY != 0 {
            u8::decode(r)?
        } else {
            prior_or_violation()?.publisher_priority
        };

        let extension_headers = if serialization_flags & flags::EXTENSIONS != 0 {
            ExtensionHeaders::decode(r)?
        } else {
            ExtensionHeaders::default()
        };

        let payload_length = usize::decode(r)?;

        let object = FetchObject {
            group_id,
            subgroup_id,
            object_id,
            publisher_priority,
            extension_headers,
            payload_length,
        };
        self.prior = Some(PriorObject::of(&object));
        Ok((FetchEntry::Object(object), start - r.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Buf, BytesMut};

    fn object(
        group_id: u64,
        subgroup_id: Option<u64>,
        object_id: u64,
        priority: u8,
    ) -> FetchObject {
        FetchObject {
            group_id,
            subgroup_id,
            object_id,
            publisher_priority: priority,
            extension_headers: ExtensionHeaders::default(),
            payload_length: 3,
        }
    }

    fn round_trip(entries: &[FetchEntry]) -> BytesMut {
        let mut encoder = FetchObjectEncoder::new();
        let mut buf = BytesMut::new();
        for entry in entries {
            match entry {
                FetchEntry::Object(object) => {
                    encoder.encode_object(object, &mut buf).unwrap();
                    buf.extend_from_slice(&vec![0xab; object.payload_length]);
                }
                FetchEntry::EndOfRange { kind, location } => {
                    encoder
                        .encode_end_of_range(*kind, *location, &mut buf)
                        .unwrap();
                }
            }
        }

        let mut decoder = FetchObjectDecoder::new();
        let mut wire = buf.clone().freeze();
        for expected in entries {
            let (decoded, consumed) = decoder.decode(&wire).unwrap();
            wire.advance(consumed);
            assert_eq!(&decoded, expected);
            if let FetchEntry::Object(object) = decoded {
                wire.advance(object.payload_length);
            }
        }
        assert!(!wire.has_remaining());
        buf
    }

    #[test]
    fn first_object_spells_out_every_inheritable_field() {
        let mut buf = BytesMut::new();
        FetchObjectEncoder::new()
            .encode_object(&object(5, Some(2), 7, 240), &mut buf)
            .unwrap();
        // Group, Object, Priority present; Subgroup ID present (0x03).
        assert_eq!(buf[0], 0x1f);
        assert_eq!(&buf[1..], &[5, 2, 7, 240, 3]);
    }

    #[test]
    fn contiguous_objects_inherit_group_subgroup_and_priority() {
        let buf = round_trip(&[
            FetchEntry::Object(object(9, Some(0), 0, 240)),
            FetchEntry::Object(object(9, Some(0), 1, 240)),
            FetchEntry::Object(object(9, Some(0), 2, 240)),
        ]);
        // First object: flags, group, object, priority, length (5 bytes) plus a
        // 3-byte payload. The second object inherits everything but Subgroup ID
        // zero, so it is a flags byte and a payload length.
        let second = 5 + 3;
        assert_eq!(&buf[second..second + 2], &[0x00, 3]);
    }

    #[test]
    fn subgroup_prior_and_prior_plus_one_round_trip() {
        round_trip(&[
            FetchEntry::Object(object(1, Some(4), 0, 128)),
            FetchEntry::Object(object(1, Some(4), 1, 128)),
            FetchEntry::Object(object(1, Some(5), 2, 128)),
            FetchEntry::Object(object(1, Some(9), 3, 64)),
        ]);
    }

    #[test]
    fn group_change_and_object_gap_are_explicit() {
        round_trip(&[
            FetchEntry::Object(object(1, Some(0), 3, 128)),
            FetchEntry::Object(object(1, Some(0), 7, 128)),
            FetchEntry::Object(object(2, Some(0), 8, 128)),
        ]);
    }

    #[test]
    fn datagram_objects_carry_the_datagram_flag_and_no_subgroup() {
        let mut buf = BytesMut::new();
        FetchObjectEncoder::new()
            .encode_object(&object(3, None, 1, 200), &mut buf)
            .unwrap();
        // 0x5C is above 63, so the flags take a two-byte varint.
        let serialization_flags = u64::decode(&mut &buf[..]).unwrap();
        assert_eq!(serialization_flags, flags::DATAGRAM | 0x1c);
        round_trip(&[
            FetchEntry::Object(object(3, None, 1, 200)),
            FetchEntry::Object(object(3, None, 2, 200)),
        ]);
    }

    #[test]
    fn extensions_are_carried_when_present() {
        let mut with_extensions = object(1, Some(0), 0, 128);
        with_extensions.extension_headers.set_intvalue(0x3c, 2);
        round_trip(&[
            FetchEntry::Object(with_extensions),
            FetchEntry::Object(object(1, Some(0), 1, 128)),
        ]);
    }

    #[test]
    fn end_of_range_markers_round_trip_and_reset_the_prior_object() {
        let buf = round_trip(&[
            FetchEntry::Object(object(4, Some(0), 0, 128)),
            FetchEntry::EndOfRange {
                kind: FetchEndOfRange::Unknown,
                location: Location::new(4, 6),
            },
            FetchEntry::Object(object(4, Some(0), 7, 128)),
            FetchEntry::EndOfRange {
                kind: FetchEndOfRange::NonExistent,
                location: Location::new(4, 9),
            },
        ]);
        // First object: 5 header bytes and a 3-byte payload. Marker: 0x10C is a
        // 2-byte varint, then group and object. The object after the marker
        // repeats group, object and priority.
        let after_marker = (5 + 3) + (2 + 1 + 1);
        assert_eq!(buf[after_marker] & 0x1c, 0x1c);
    }

    #[test]
    fn first_object_referencing_a_prior_object_is_rejected() {
        for serialization_flags in [0x00u8, 0x01, 0x02, 0x10 | 0x08, 0x04 | 0x08] {
            // Each value omits at least one inheritable field.
            let wire = [serialization_flags, 1, 1, 1, 0];
            assert!(
                FetchObjectDecoder::new().decode(&wire).is_err(),
                "flags {serialization_flags:#x} must not be accepted first"
            );
        }
    }

    #[test]
    fn non_flag_values_other_than_end_of_range_are_rejected() {
        let mut buf = BytesMut::new();
        0x80u64.encode(&mut buf).unwrap();
        buf.extend_from_slice(&[1, 1]);
        assert!(matches!(
            FetchObjectDecoder::new().decode(&buf),
            Err(DecodeError::InvalidValue)
        ));
    }

    #[test]
    fn short_buffer_leaves_cursor_and_state_untouched() {
        let mut encoder = FetchObjectEncoder::new();
        let mut buf = BytesMut::new();
        encoder
            .encode_object(&object(300, Some(0), 70_000, 128), &mut buf)
            .unwrap();
        let full = buf.freeze();

        let mut decoder = FetchObjectDecoder::new();
        for len in 0..full.len() {
            assert!(matches!(
                decoder.decode(&full[..len]),
                Err(DecodeError::More(_))
            ));
        }
        // A failed decode left no prior Object behind, so this still parses as
        // a first Object.
        assert_eq!(
            decoder.decode(&full).unwrap(),
            (
                FetchEntry::Object(object(300, Some(0), 70_000, 128)),
                full.len()
            )
        );
    }
}
