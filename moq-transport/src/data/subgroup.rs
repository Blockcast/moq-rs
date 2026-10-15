// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::coding::{Decode, DecodeError, Encode, EncodeError};
use crate::data::{ExtensionHeaders, ObjectStatus, StreamHeaderType, DEFAULT_PUBLISHER_PRIORITY};

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub(crate) enum ObjectIdDeltaError {
    #[error("object ID delta overflow")]
    Overflow,

    #[error("object IDs must increase within a subgroup stream")]
    NonMonotonic,
}

/// Decode an Object ID Delta and advance the stream's Object ID state.
pub(crate) fn decode_object_id_delta(
    previous_object_id: &mut Option<u64>,
    object_id_delta: u64,
) -> Result<u64, ObjectIdDeltaError> {
    let object_id = match *previous_object_id {
        Some(previous) => previous
            .checked_add(object_id_delta)
            .and_then(|value| value.checked_add(1))
            .ok_or(ObjectIdDeltaError::Overflow)?,
        None => object_id_delta,
    };
    *previous_object_id = Some(object_id);
    Ok(object_id)
}

/// Encode an absolute Object ID and advance the stream's Object ID state.
pub(crate) fn encode_object_id_delta(
    previous_object_id: &mut Option<u64>,
    object_id: u64,
) -> Result<u64, ObjectIdDeltaError> {
    let object_id_delta = match *previous_object_id {
        Some(previous) => object_id
            .checked_sub(previous)
            .and_then(|difference| difference.checked_sub(1))
            .ok_or(ObjectIdDeltaError::NonMonotonic)?,
        None => object_id,
    };
    *previous_object_id = Some(object_id);
    Ok(object_id_delta)
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SubgroupHeader {
    /// Subgroup Header Type
    pub header_type: StreamHeaderType,

    /// The track alias.
    pub track_alias: u64,

    /// The group sequence number
    pub group_id: u64,

    /// The subgroup sequence number
    pub subgroup_id: Option<u64>,

    /// Publisher priority, where **smaller** values are sent first.
    pub publisher_priority: u8,
}

// Note:  Not using the Decode trait, since we need to know the header_type to properly parse this, and it
//        is read before knowing we need to decode this.
impl SubgroupHeader {
    pub fn decode<R: bytes::Buf>(
        header_type: StreamHeaderType,
        r: &mut R,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "[DECODE] SubgroupHeader: starting decode with header_type={:?}, buffer_remaining={} bytes",
            header_type,
            r.remaining()
        );

        let track_alias = u64::decode(r)?;
        tracing::trace!("[DECODE] SubgroupHeader: track_alias={}", track_alias);

        let group_id = u64::decode(r)?;
        tracing::trace!("[DECODE] SubgroupHeader: group_id={}", group_id);

        let subgroup_id = match header_type.has_subgroup_id() {
            true => {
                let id = u64::decode(r)?;
                tracing::trace!("[DECODE] SubgroupHeader: subgroup_id={}", id);
                Some(id)
            }
            false => {
                tracing::trace!(
                    "[DECODE] SubgroupHeader: subgroup_id=None (not present for this header type)"
                );
                None
            }
        };

        let publisher_priority = if header_type.uses_default_priority() {
            DEFAULT_PUBLISHER_PRIORITY
        } else {
            u8::decode(r)?
        };
        tracing::trace!(
            "[DECODE] SubgroupHeader: publisher_priority={}, buffer_remaining={} bytes",
            publisher_priority,
            r.remaining()
        );

        let result = Self {
            header_type,
            track_alias,
            group_id,
            subgroup_id,
            publisher_priority,
        };

        tracing::trace!(
            "[DECODE] SubgroupHeader complete: track_alias={}, group_id={}, subgroup_id={:?}, priority={}",
            result.track_alias,
            result.group_id,
            result.subgroup_id,
            result.publisher_priority
        );

        Ok(result)
    }
}

impl Encode for SubgroupHeader {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        tracing::trace!(
            "[ENCODE] SubgroupHeader: starting encode - track_alias={}, group_id={}, subgroup_id={:?}, priority={}, header_type={:?}",
            self.track_alias,
            self.group_id,
            self.subgroup_id,
            self.publisher_priority,
            self.header_type
        );

        let start_pos = w.remaining_mut();

        self.header_type.encode(w)?;
        tracing::trace!("[ENCODE] SubgroupHeader: encoded header_type");

        self.track_alias.encode(w)?;
        tracing::trace!(
            "[ENCODE] SubgroupHeader: encoded track_alias={}",
            self.track_alias
        );

        self.group_id.encode(w)?;
        tracing::trace!(
            "[ENCODE] SubgroupHeader: encoded group_id={}",
            self.group_id
        );

        if self.header_type.has_subgroup_id() {
            if let Some(subgroup_id) = self.subgroup_id {
                subgroup_id.encode(w)?;
                tracing::trace!(
                    "[ENCODE] SubgroupHeader: encoded subgroup_id={}",
                    subgroup_id
                );
            } else {
                tracing::error!(
                    "[ENCODE] SubgroupHeader: MISSING subgroup_id for header_type={:?}",
                    self.header_type
                );
                return Err(EncodeError::MissingField("SubgroupId".to_string()));
            }
        } else {
            tracing::trace!("[ENCODE] SubgroupHeader: subgroup_id not encoded (not required for this header type)");
        }

        if !self.header_type.uses_default_priority() {
            self.publisher_priority.encode(w)?;
            tracing::trace!(
                "[ENCODE] SubgroupHeader: encoded publisher_priority={}",
                self.publisher_priority
            );
        }

        let bytes_written = start_pos - w.remaining_mut();
        tracing::trace!(
            "[ENCODE] SubgroupHeader complete: wrote {} bytes",
            bytes_written
        );

        Ok(())
    }
}

// Subgroup Object without Extension headers (version with ExtensionHeaders is below)
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SubgroupObject {
    pub object_id_delta: u64,
    pub payload_length: usize,
    pub status: Option<ObjectStatus>,
    //pub payload: bytes::Bytes,  // TODO SLG - payload is sent outside this right now - decide which way to go
}

impl Decode for SubgroupObject {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        tracing::trace!(
            "[DECODE] SubgroupObject: starting decode, buffer_remaining={} bytes",
            r.remaining()
        );

        let object_id_delta = u64::decode(r)?;
        tracing::trace!(
            "[DECODE] SubgroupObject: object_id_delta={}",
            object_id_delta
        );

        let payload_length = usize::decode(r)?;
        tracing::trace!("[DECODE] SubgroupObject: payload_length={}", payload_length);

        let status = match payload_length {
            0 => {
                let s = ObjectStatus::decode(r)?;
                tracing::trace!("[DECODE] SubgroupObject: status={:?} (payload_length=0)", s);
                Some(s)
            }
            _ => {
                tracing::trace!("[DECODE] SubgroupObject: status=None (payload_length > 0)");
                None
            }
        };

        //Self::decode_remaining(r, payload_length);
        //let payload = r.copy_to_bytes(payload_length);

        tracing::trace!(
            "[DECODE] SubgroupObject complete: object_id_delta={}, payload_length={}, status={:?}, buffer_remaining={} bytes",
            object_id_delta,
            payload_length,
            status,
            r.remaining()
        );

        Ok(Self {
            object_id_delta,
            payload_length,
            status,
            //payload,
        })
    }
}

impl Encode for SubgroupObject {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        tracing::trace!(
            "[ENCODE] SubgroupObject: starting encode - object_id_delta={}, payload_length={}, status={:?}",
            self.object_id_delta,
            self.payload_length,
            self.status
        );

        self.object_id_delta.encode(w)?;
        tracing::trace!(
            "[ENCODE] SubgroupObject: encoded object_id_delta={}",
            self.object_id_delta
        );

        self.payload_length.encode(w)?;
        tracing::trace!(
            "[ENCODE] SubgroupObject: encoded payload_length={}",
            self.payload_length
        );

        if self.payload_length == 0 {
            if let Some(status) = self.status {
                status.encode(w)?;
                tracing::trace!("[ENCODE] SubgroupObject: encoded status={:?}", status);
            } else {
                tracing::error!("[ENCODE] SubgroupObject: MISSING status for payload_length=0");
                return Err(EncodeError::MissingField("Status".to_string()));
            }
        }
        //Self::encode_remaining(w, self.payload.len())?;
        //w.put_slice(&self.payload);

        tracing::trace!("[ENCODE] SubgroupObject complete");

        Ok(())
    }
}

// Subgroup Object with Extension headers
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SubgroupObjectExt {
    pub object_id_delta: u64,
    pub extension_headers: ExtensionHeaders,
    pub payload_length: usize,
    pub status: Option<ObjectStatus>,
    //pub payload: bytes::Bytes,  // TODO SLG - payload is sent outside this right now - decide which way to go
}

impl Decode for SubgroupObjectExt {
    fn decode<R: bytes::Buf>(r: &mut R) -> Result<Self, DecodeError> {
        tracing::trace!(
            "[DECODE] SubgroupObjectExt: starting decode, buffer_remaining={} bytes",
            r.remaining()
        );

        let object_id_delta = u64::decode(r)?;
        tracing::trace!(
            "[DECODE] SubgroupObjectExt: object_id_delta={}",
            object_id_delta
        );

        let extension_headers = ExtensionHeaders::decode(r)?;
        tracing::trace!(
            "[DECODE] SubgroupObjectExt: extension_headers={:?}",
            extension_headers
        );

        let payload_length = usize::decode(r)?;
        tracing::trace!(
            "[DECODE] SubgroupObjectExt: payload_length={}",
            payload_length
        );

        let status = match payload_length {
            0 => {
                let s = ObjectStatus::decode(r)?;
                tracing::trace!(
                    "[DECODE] SubgroupObjectExt: status={:?} (payload_length=0)",
                    s
                );
                Some(s)
            }
            _ => {
                tracing::trace!("[DECODE] SubgroupObjectExt: status=None (payload_length > 0)");
                None
            }
        };

        if status.is_some_and(|status| status != ObjectStatus::NormalObject)
            && !extension_headers.is_empty()
        {
            return Err(DecodeError::InvalidValue);
        }

        //Self::decode_remaining(r, payload_length);
        //let payload = r.copy_to_bytes(payload_length);

        tracing::trace!(
            "[DECODE] SubgroupObjectExt complete: object_id_delta={}, payload_length={}, status={:?}, buffer_remaining={} bytes",
            object_id_delta,
            payload_length,
            status,
            r.remaining()
        );

        Ok(Self {
            object_id_delta,
            extension_headers,
            payload_length,
            status,
            //payload,
        })
    }
}

impl Encode for SubgroupObjectExt {
    fn encode<W: bytes::BufMut>(&self, w: &mut W) -> Result<(), EncodeError> {
        tracing::trace!(
            "[ENCODE] SubgroupObjectExt: starting encode - object_id_delta={}, payload_length={}, status={:?}, extension_headers={:?}",
            self.object_id_delta,
            self.payload_length,
            self.status,
            self.extension_headers
        );

        self.object_id_delta.encode(w)?;
        tracing::trace!(
            "[ENCODE] SubgroupObjectExt: encoded object_id_delta={}",
            self.object_id_delta
        );

        self.extension_headers.encode(w)?;
        tracing::trace!("[ENCODE] SubgroupObjectExt: encoded extension_headers");

        self.payload_length.encode(w)?;
        tracing::trace!(
            "[ENCODE] SubgroupObjectExt: encoded payload_length={}",
            self.payload_length
        );

        if self.payload_length == 0 {
            if let Some(status) = self.status {
                if status != ObjectStatus::NormalObject && !self.extension_headers.is_empty() {
                    return Err(EncodeError::InvalidValue);
                }
                status.encode(w)?;
                tracing::trace!("[ENCODE] SubgroupObjectExt: encoded status={:?}", status);
            } else {
                tracing::error!("[ENCODE] SubgroupObjectExt: MISSING status for payload_length=0");
                return Err(EncodeError::MissingField("Status".to_string()));
            }
        }
        //Self::encode_remaining(w, self.payload.len())?;
        //w.put_slice(&self.payload);

        tracing::trace!("[ENCODE] SubgroupObjectExt complete");

        Ok(())
    }
}

// TODO SLG - add more unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{StreamHeader, SubgroupIdMode};
    use bytes::Bytes;
    use bytes::BytesMut;

    #[test]
    fn object_id_delta_round_trips_absolute_first_and_relative_followups() {
        for (object_ids, expected_deltas) in [([0, 1, 2], [0, 0, 0]), ([7, 9, 12], [7, 1, 2])] {
            let mut encode_previous = None;
            let deltas = object_ids
                .map(|object_id| encode_object_id_delta(&mut encode_previous, object_id).unwrap());
            assert_eq!(deltas, expected_deltas);

            let mut decode_previous = None;
            let decoded =
                deltas.map(|delta| decode_object_id_delta(&mut decode_previous, delta).unwrap());
            assert_eq!(decoded, object_ids);
        }
    }

    #[test]
    fn type_78_omits_priority_without_consuming_the_first_object() {
        let header = SubgroupHeader {
            header_type: StreamHeaderType::subgroup(SubgroupIdMode::Zero, false, true, true, true),
            track_alias: 0,
            group_id: 0,
            subgroup_id: None,
            publisher_priority: 200,
        };
        let mut encoded = BytesMut::new();
        header.encode(&mut encoded).unwrap();
        assert_eq!(encoded.as_ref(), &[0x78, 0x00, 0x00]);
        encoded.extend_from_slice(&[0x00, 0x01, b'x']);

        let mut encoded = encoded.freeze();
        let decoded = StreamHeader::decode(&mut encoded)
            .unwrap()
            .subgroup_header
            .unwrap();
        assert_eq!(decoded.header_type.value(), 0x78);
        assert_eq!(decoded.publisher_priority, 128);
        assert_eq!(encoded.as_ref(), &[0x00, 0x01, b'x']);

        let object = SubgroupObject::decode(&mut encoded).unwrap();
        assert_eq!(object.object_id_delta, 0);
        assert_eq!(object.payload_length, 1);
        assert_eq!(encoded.as_ref(), b"x");
    }

    #[test]
    fn subgroup_id_modes_consume_only_their_declared_fields() {
        for (mode, subgroup_id, expected_id) in [
            (SubgroupIdMode::Zero, None, None),
            (SubgroupIdMode::FirstObject, None, None),
            (SubgroupIdMode::Explicit, Some(9), Some(9)),
        ] {
            let header = SubgroupHeader {
                header_type: StreamHeaderType::subgroup(mode, false, false, true, false),
                track_alias: 7,
                group_id: 8,
                subgroup_id,
                publisher_priority: 200,
            };
            let mut encoded = BytesMut::new();
            header.encode(&mut encoded).unwrap();
            encoded.extend_from_slice(&[0x0b, 0x01, b'x']);

            let mut encoded = encoded.freeze();
            let decoded = StreamHeader::decode(&mut encoded)
                .unwrap()
                .subgroup_header
                .unwrap();
            assert_eq!(decoded.subgroup_id, expected_id);
            assert_eq!(decoded.publisher_priority, 128);
            assert_eq!(encoded.as_ref(), &[0x0b, 0x01, b'x']);
        }
    }

    #[test]
    fn object_id_delta_rejects_overflow_and_non_monotonic_ids() {
        let mut previous = Some(u64::MAX);
        assert_eq!(
            decode_object_id_delta(&mut previous, 0),
            Err(ObjectIdDeltaError::Overflow)
        );
        assert_eq!(previous, Some(u64::MAX));

        for object_id in [9, 8] {
            let mut previous = Some(9);
            assert_eq!(
                encode_object_id_delta(&mut previous, object_id),
                Err(ObjectIdDeltaError::NonMonotonic)
            );
            assert_eq!(previous, Some(9));
        }
    }

    #[test]
    fn filtered_object_ids_use_the_previous_transmitted_object() {
        let mut previous = None;
        let deltas = [0, 1, 5, 6, 9]
            .into_iter()
            .filter(|object_id| *object_id > 4)
            .map(|object_id| encode_object_id_delta(&mut previous, object_id).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(deltas, [5, 0, 2]);
    }

    #[test]
    fn encode_decode_object() {
        let mut buf = BytesMut::new();

        let msg = SubgroupObject {
            object_id_delta: 0,
            payload_length: 7,
            status: None,
        };
        msg.encode(&mut buf).unwrap();
        let decoded = SubgroupObject::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn encode_decode_object_ext() {
        let mut buf = BytesMut::new();

        // One ExtensionHeader for testing
        let mut ext_hdrs = ExtensionHeaders::new();
        ext_hdrs.set_bytesvalue(123, vec![0x00, 0x01, 0x02, 0x03]);

        let msg = SubgroupObjectExt {
            object_id_delta: 0,
            extension_headers: ext_hdrs,
            payload_length: 7,
            status: None,
        };
        msg.encode(&mut buf).unwrap();
        let decoded = SubgroupObjectExt::decode(&mut buf).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn decode_rejects_non_normal_status_with_extension_headers() {
        let data = vec![
            0x00, // object id delta
            0x02, // extension headers byte length
            0x00, // extension delta type
            0x01, // extension value
            0x00, // payload length
            0x04, // EndOfTrack
        ];
        let mut buf: Bytes = data.into();

        assert!(matches!(
            SubgroupObjectExt::decode(&mut buf).unwrap_err(),
            DecodeError::InvalidValue
        ));
    }

    #[test]
    fn encode_rejects_non_normal_status_with_extension_headers() {
        let mut ext_hdrs = ExtensionHeaders::new();
        ext_hdrs.set_intvalue(0, 1);
        let msg = SubgroupObjectExt {
            object_id_delta: 0,
            extension_headers: ext_hdrs,
            payload_length: 0,
            status: Some(ObjectStatus::EndOfTrack),
        };
        let mut buf = BytesMut::new();

        assert!(matches!(
            msg.encode(&mut buf).unwrap_err(),
            EncodeError::InvalidValue
        ));
    }

    /// The first draft-20 §11.6 example: one subgroup, Type Flags 0x14.
    #[test]
    fn decodes_draft_20_example_subgroup_with_explicit_id_and_priority() {
        let wire: &[u8] = &[
            0x14, 0x02, 0x00, 0x00, 0x00, // header: alias 2, group 0, subgroup 0, priority 0
            0x00, 0x04, b'a', b'b', b'c', b'd', // Object ID 0
            0x00, 0x04, b'e', b'f', b'g', b'h', // Object ID 1
        ];
        let mut r = Bytes::from_static(wire);
        let header = StreamHeader::decode(&mut r)
            .unwrap()
            .subgroup_header
            .unwrap();
        assert_eq!(header.track_alias, 2);
        assert_eq!(header.group_id, 0);
        assert_eq!(header.subgroup_id, Some(0));
        assert_eq!(header.publisher_priority, 0);

        let mut previous = None;
        for (expected_id, payload) in [(0, b"abcd"), (1, b"efgh")] {
            let object = SubgroupObject::decode(&mut r).unwrap();
            let id = decode_object_id_delta(&mut previous, object.object_id_delta).unwrap();
            assert_eq!(id, expected_id);
            assert_eq!(r.split_to(object.payload_length).as_ref(), payload);
        }
        assert!(r.is_empty());

        let mut encoded = BytesMut::new();
        header.encode(&mut encoded).unwrap();
        assert_eq!(&encoded[..], &wire[..5]);
    }

    /// The second draft-20 §11.6 example: Type Flags 0x35 (properties, explicit
    /// Subgroup ID, default priority). The draft's printed lengths (Properties
    /// Length 33, a 21-byte value) do not match its 14-byte "traceID:123456",
    /// so the lengths here are derived from the printed values. 33 is the
    /// pre-vi64 arithmetic with absolute types (1 + 8 + 2 + 1 + 21); do not
    /// "fix" this vector toward the printed lengths.
    #[test]
    fn decodes_draft_20_example_subgroup_with_properties() {
        let wire: &[u8] = &[
            0x35, 0x02, 0x00, 0x00, // header: alias 2, group 0, subgroup 0; no priority
            0x00, 0x16, // Object ID 0, Properties Length 22
            0x04, 0xf0, 0x82, 0x57, 0xdc, 0xd3, // Type 4 = 2186796243
            0x49, 0x0e, b't', b'r', b'a', b'c', b'e', b'I', b'D', b':', b'1', b'2', b'3', b'4',
            b'5', b'6', // Type 77, 14 bytes
            0x04, b'a', b'b', b'c', b'd', 0x00, 0x00, 0x04, b'e', b'f', b'g',
            b'h', // Object ID 1, no properties
        ];
        let mut r = Bytes::from_static(wire);
        let header = StreamHeader::decode(&mut r)
            .unwrap()
            .subgroup_header
            .unwrap();
        assert_eq!(header.header_type.value(), 0x35);
        assert_eq!(header.subgroup_id, Some(0));
        assert_eq!(header.publisher_priority, DEFAULT_PUBLISHER_PRIORITY);

        let first = SubgroupObjectExt::decode(&mut r).unwrap();
        assert_eq!(first.object_id_delta, 0);
        assert_eq!(
            first.extension_headers.get(4).map(|kvp| &kvp.value),
            Some(&crate::coding::Value::IntValue(2_186_796_243))
        );
        assert_eq!(
            first.extension_headers.get(77).map(|kvp| &kvp.value),
            Some(&crate::coding::Value::BytesValue(
                b"traceID:123456".to_vec()
            ))
        );
        assert_eq!(r.split_to(first.payload_length).as_ref(), b"abcd");

        let second = SubgroupObjectExt::decode(&mut r).unwrap();
        assert_eq!(second.object_id_delta, 0);
        assert!(second.extension_headers.is_empty());
        assert_eq!(r.split_to(second.payload_length).as_ref(), b"efgh");
        assert!(r.is_empty());

        let mut encoded = BytesMut::new();
        header.encode(&mut encoded).unwrap();
        first.encode(&mut encoded).unwrap();
        encoded.extend_from_slice(b"abcd");
        second.encode(&mut encoded).unwrap();
        encoded.extend_from_slice(b"efgh");
        assert_eq!(&encoded[..], wire);
    }

    /// FEC Source Info (draft-ramadan-moq-fec §5.3) is odd Property 0x11 with a
    /// fixed 4-byte SS_ID. Next to it is an unknown odd Property whose Delta
    /// Type needs 2 bytes. Neither is interpreted here; both must round-trip
    /// byte for byte so a relay forwards them unmodified (§5.4).
    #[test]
    fn fec_source_info_and_unknown_odd_property_round_trip() {
        let wire: &[u8] = &[
            0x00, 0x0c, // Object ID delta 0, Properties Length 12
            0x11, 0x04, 0x00, 0x00, 0x01, 0x00, // Type 0x11, SS_ID 256
            0x83, 0xee, 0x03, 0xaa, 0xbb, 0xcc, // Type 0x3ff (delta 0x3ee), 3 bytes
            0x02, 0xde, 0xad, // Payload Length 2, payload
        ];
        let mut r = Bytes::from_static(wire);
        let object = SubgroupObjectExt::decode(&mut r).unwrap();
        assert_eq!(object.object_id_delta, 0);
        assert_eq!(
            object.extension_headers.get(0x11).map(|kvp| &kvp.value),
            Some(&crate::coding::Value::BytesValue(vec![0, 0, 1, 0]))
        );
        assert_eq!(
            object.extension_headers.get(0x3ff).map(|kvp| &kvp.value),
            Some(&crate::coding::Value::BytesValue(vec![0xaa, 0xbb, 0xcc]))
        );
        assert_eq!(r.as_ref(), &[0xde, 0xad]);

        let mut encoded = BytesMut::new();
        object.encode(&mut encoded).unwrap();
        encoded.extend_from_slice(&r);
        assert_eq!(&encoded[..], wire);
    }
}
