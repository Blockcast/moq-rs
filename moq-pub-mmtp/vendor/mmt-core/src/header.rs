//! MMT header types for zero-copy parsing and generation
//!
//! Implements headers as specified in ISO/IEC 23008-1:
//! - MMTP Header (12 bytes base)
//! - MPU Header (8 bytes)
//! - MFU Data Unit Header

use crate::error::{MmtError, Result};
use bytes::{Buf, BufMut};

/// MMTP base header size in bytes
pub const MMTP_HEADER_SIZE: usize = 12;

/// MPU header size in bytes
pub const MPU_HEADER_SIZE: usize = 8;

/// MFU data unit header size in bytes.
///
/// `MfuDataUnit::read_from` parses 14 bytes:
/// `movie_fragment_sequence(4) + sample_number(4) + offset(4) + priority(1) + dep_counter(1)`.
/// The previous value (12) contradicted the parser and disagreed with
/// `mmt-ffi::MMT_MFU_HEADER_SIZE = 14`, which is what C consumers see.
pub const MFU_HEADER_SIZE: usize = 14;

/// MMTP Packet Header (ISO/IEC 23008-1:2023 Table 6)
///
/// ```text
/// Byte 0: V(2) | C(1) | FEC(2) | r(1) | X(1) | R(1)
/// Byte 1: RES(2) | type(6)
/// Bytes 2-3: packet_id (16 bits)
/// Bytes 4-7: timestamp (32 bits)
/// Bytes 8-11: packet_sequence_number (32 bits)
/// ```
///
/// Where:
/// - V = version (2 bits)
/// - C = payload_type_extension_flag (1 bit)
/// - FEC = FEC_type (2 bits) per Table 8
/// - r = reserved (1 bit)
/// - X = extension_flag (1 bit)
/// - R = RAP_flag (1 bit)
/// - RES = reserved (2 bits)
/// - type = packet type (6 bits)
#[derive(Debug, Clone, PartialEq)]
pub struct MmtpHeader {
    /// Version (2 bits) - always 0 for current spec
    pub version: u8,
    /// Payload type extension flag (1 bit)
    pub payload_type_extension_flag: bool,
    /// FEC type (2 bits) per Table 8
    pub fec_type: u8,
    /// Extension flag (1 bit) - indicates header extension present
    pub extension_flag: bool,
    /// Random Access Point flag (1 bit)
    pub rap_flag: bool,
    /// Packet type (6 bits)
    pub packet_type: PacketType,
    /// Packet ID (asset identifier)
    pub packet_id: u16,
    /// Timestamp (NTP-based)
    pub timestamp: u32,
    /// Packet sequence number
    pub packet_sequence: u32,
    /// Optional extension header data (when extension_flag = 1)
    pub extension: Option<Vec<u8>>,
}

impl MmtpHeader {
    /// Create a new MMTP header with default values
    pub fn new(packet_id: u16, packet_type: PacketType) -> Self {
        Self {
            version: 0,
            payload_type_extension_flag: false,
            fec_type: 0,
            extension_flag: false,
            rap_flag: false,
            packet_type,
            packet_id,
            timestamp: 0,
            packet_sequence: 0,
            extension: None,
        }
    }

    /// Write header to buffer (zero-copy)
    ///
    /// Format per ISO/IEC 23008-1:2023 Table 6:
    /// - Byte 0: V(2) | C(1) | FEC(2) | r(1) | X(1) | R(1)
    /// - Byte 1: RES(2) | type(6)
    ///
    /// Returns the number of bytes written.
    #[inline]
    pub fn write_to<B: BufMut>(&self, buf: &mut B) -> Result<usize> {
        let ext_len = self.extension.as_ref().map(|e| e.len()).unwrap_or(0);
        let total_size = MMTP_HEADER_SIZE + ext_len;

        if buf.remaining_mut() < total_size {
            return Err(MmtError::BufferTooSmall {
                need: total_size,
                have: buf.remaining_mut(),
            });
        }

        // Byte 0: V(2) | C(1) | FEC(2) | r(1) | X(1) | R(1)
        let byte0 = ((self.version & 0x03) << 6)
            | ((self.payload_type_extension_flag as u8) << 5)
            | ((self.fec_type & 0x03) << 3)
            // r (reserved) = 0
            | ((self.extension_flag as u8) << 1)
            | (self.rap_flag as u8);
        buf.put_u8(byte0);

        // Byte 1: RES(2) | type(6)
        let byte1 = self.packet_type as u8 & 0x3F;
        buf.put_u8(byte1);

        // Bytes 2-3: packet_id
        buf.put_u16(self.packet_id);

        // Bytes 4-7: timestamp
        buf.put_u32(self.timestamp);

        // Bytes 8-11: packet_sequence
        buf.put_u32(self.packet_sequence);

        let mut written = MMTP_HEADER_SIZE;

        // Extension headers if present (when X=1)
        if let Some(ext) = &self.extension {
            buf.put_slice(ext);
            written += ext.len();
        }

        Ok(written)
    }

    /// Read header from buffer (zero-copy)
    ///
    /// Parses per ISO/IEC 23008-1:2023 Table 6
    #[inline]
    pub fn read_from<B: Buf>(buf: &mut B) -> Result<Self> {
        if buf.remaining() < MMTP_HEADER_SIZE {
            return Err(MmtError::BufferTooSmall {
                need: MMTP_HEADER_SIZE,
                have: buf.remaining(),
            });
        }

        // Byte 0: V(2) | C(1) | FEC(2) | r(1) | X(1) | R(1)
        let byte0 = buf.get_u8();
        let version = (byte0 >> 6) & 0x03;
        let payload_type_extension_flag = (byte0 & 0x20) != 0;
        let fec_type = (byte0 >> 3) & 0x03;
        // r (reserved) is ignored
        let extension_flag = (byte0 & 0x02) != 0;
        let rap_flag = (byte0 & 0x01) != 0;

        // Byte 1: RES(2) | type(6)
        let byte1 = buf.get_u8();
        let packet_type = PacketType::from_u8(byte1 & 0x3F)?;

        let packet_id = buf.get_u16();
        let timestamp = buf.get_u32();
        let packet_sequence = buf.get_u32();

        Ok(MmtpHeader {
            version,
            payload_type_extension_flag,
            fec_type,
            extension_flag,
            rap_flag,
            packet_type,
            packet_id,
            timestamp,
            packet_sequence,
            extension: None,
        })
    }

    /// Get the header size including extensions
    #[inline]
    pub fn size(&self) -> usize {
        MMTP_HEADER_SIZE + self.extension.as_ref().map(|e| e.len()).unwrap_or(0)
    }
}

/// MMTP Packet Type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    /// MPU (Media Processing Unit)
    Mpu = 0x00,
    /// Generic object
    Generic = 0x01,
    /// Control message (signaling)
    Control = 0x02,
    /// FEC repair symbol (AL-FEC)
    Repair = 0x03,
}

impl PacketType {
    /// Convert from raw u8 value
    #[inline]
    pub fn from_u8(val: u8) -> Result<Self> {
        match val {
            0x00 => Ok(PacketType::Mpu),
            0x01 => Ok(PacketType::Generic),
            0x02 => Ok(PacketType::Control),
            0x03 => Ok(PacketType::Repair),
            _ => Err(MmtError::InvalidPacketType(val)),
        }
    }
}

/// FEC type values per Table 8 of ISO/IEC 23008-1
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FecType {
    /// No FEC or FEC without source_FEC_payload_ID
    None = 0x00,
    /// MMTP packet with source_FEC_payload_ID field
    WithSourcePayloadId = 0x01,
    /// FEC repair packet for FEC Payload Mode 0
    RepairMode0 = 0x02,
    /// FEC repair packet for FEC Payload Mode 1
    RepairMode1 = 0x03,
}

impl FecType {
    /// Convert from raw u8 value
    #[inline]
    pub fn from_u8(val: u8) -> Result<Self> {
        match val {
            0x00 => Ok(FecType::None),
            0x01 => Ok(FecType::WithSourcePayloadId),
            0x02 => Ok(FecType::RepairMode0),
            0x03 => Ok(FecType::RepairMode1),
            _ => Err(MmtError::InvalidFecType(val)),
        }
    }

    /// Check if this FEC type includes source_FEC_payload_ID
    #[inline]
    pub fn has_source_payload_id(&self) -> bool {
        matches!(self, FecType::WithSourcePayloadId)
    }

    /// Check if this is a repair packet
    #[inline]
    pub fn is_repair(&self) -> bool {
        matches!(self, FecType::RepairMode0 | FecType::RepairMode1)
    }
}

/// Source FEC Payload ID per ISO/IEC 23008-1:2023 Section C.5.2.
/// SS_ID (32 bits) — flat monotonic counter, incremented per source packet.
/// For constant K: SBN = floor(SS_ID / K), ESI = SS_ID % K.
/// For variable K: block membership from repair SS_Start + SSB_length.
/// Appended after MMTP payload when fec_type = 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceFecPayloadId {
    /// SS_ID: source symbol sequence number (32 bits)
    pub ss_id: u32,
}

impl SourceFecPayloadId {
    /// Size in bytes
    pub const SIZE: usize = 4;

    /// Create from SS_ID
    pub fn new(ss_id: u32) -> Self {
        Self { ss_id }
    }

    /// Derive SBN for constant K: floor(SS_ID / K)
    pub fn sbn(&self, k: u32) -> u32 {
        if k == 0 {
            0
        } else {
            self.ss_id / k
        }
    }

    /// Derive ESI for constant K: SS_ID % K
    pub fn esi(&self, k: u32) -> u32 {
        if k == 0 {
            0
        } else {
            self.ss_id % k
        }
    }

    /// Write to buffer
    #[inline]
    pub fn write_to<B: BufMut>(&self, buf: &mut B) -> Result<usize> {
        if buf.remaining_mut() < Self::SIZE {
            return Err(MmtError::BufferTooSmall {
                need: Self::SIZE,
                have: buf.remaining_mut(),
            });
        }
        buf.put_u32(self.ss_id);
        Ok(Self::SIZE)
    }

    /// Read from buffer
    #[inline]
    pub fn read_from<B: Buf>(buf: &mut B) -> Result<Self> {
        if buf.remaining() < Self::SIZE {
            return Err(MmtError::BufferTooSmall {
                need: Self::SIZE,
                have: buf.remaining(),
            });
        }
        Ok(Self {
            ss_id: buf.get_u32(),
        })
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> [u8; 4] {
        self.ss_id.to_be_bytes()
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8; 4]) -> Self {
        Self {
            ss_id: u32::from_be_bytes(*bytes),
        }
    }

    /// Read a Source FEC Payload ID from the final four bytes of a packet
    /// payload/trailer span.
    #[inline]
    pub fn read_trailer(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < Self::SIZE {
            return Err(MmtError::BufferTooSmall {
                need: Self::SIZE,
                have: bytes.len(),
            });
        }

        let off = bytes.len() - Self::SIZE;
        Ok(Self {
            ss_id: u32::from_be_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]]),
        })
    }

    /// Split bytes into `(payload_without_trailer, source_fec_payload_id)`.
    ///
    /// The Source FEC Payload ID is carried as a trailer after the MPU payload
    /// when `fec_type = 1`; it is not part of the MMTP header prefix.
    #[inline]
    pub fn split_payload_and_trailer(bytes: &[u8]) -> Result<(&[u8], Self)> {
        let trailer = Self::read_trailer(bytes)?;
        let payload_end = bytes.len() - Self::SIZE;
        Ok((&bytes[..payload_end], trailer))
    }
}

/// Extended MMTP Header with FEC support
///
/// `fec_type = 1` advertises a Source FEC Payload ID, but the SS_ID is a
/// four-byte trailer appended after the MPU payload. It is not serialized as a
/// prefix immediately after the MMTP header.
#[derive(Debug, Clone, PartialEq)]
pub struct MmtpHeaderExt {
    /// Base header fields
    pub base: MmtpHeader,
    /// Source FEC Payload ID to append as a trailer when serializing a full
    /// packet payload.
    pub source_fec_payload_id: Option<SourceFecPayloadId>,
}

impl MmtpHeaderExt {
    /// Create a new extended header without FEC
    pub fn new(packet_id: u16, packet_type: PacketType) -> Self {
        Self {
            base: MmtpHeader::new(packet_id, packet_type),
            source_fec_payload_id: None,
        }
    }

    /// Create a new extended header with FEC payload ID
    pub fn with_fec(
        packet_id: u16,
        packet_type: PacketType,
        source_fec_payload_id: SourceFecPayloadId,
    ) -> Self {
        Self {
            base: MmtpHeader {
                version: 0,
                payload_type_extension_flag: false,
                fec_type: FecType::WithSourcePayloadId as u8,
                extension_flag: false,
                rap_flag: false,
                packet_type,
                packet_id,
                timestamp: 0,
                packet_sequence: 0,
                extension: None,
            },
            source_fec_payload_id: Some(source_fec_payload_id),
        }
    }

    /// Get MMTP header size. Source FEC Payload ID bytes are trailer bytes and
    /// are intentionally excluded.
    pub fn size(&self) -> usize {
        self.base.size()
    }

    /// Whether this packet carries a Source FEC Payload ID trailer.
    ///
    /// Derived solely from the header `fec_type`, this is the single source of
    /// truth for trailer presence used by both the serialize path
    /// ([`Self::write_source_fec_payload_id_trailer`]) and the parse path
    /// ([`Self::split_payload_and_source_fec_trailer`]), so the two cannot
    /// disagree about whether a trailer exists.
    pub fn has_source_fec_trailer(&self) -> bool {
        self.base.fec_type == FecType::WithSourcePayloadId as u8
    }

    /// Write the MMTP header to buffer.
    ///
    /// This does not write the Source FEC Payload ID. Call
    /// [`Self::write_source_fec_payload_id_trailer`] after writing the MPU
    /// payload when serializing a full source packet.
    pub fn write_to<B: BufMut>(&self, buf: &mut B) -> Result<usize> {
        self.base.write_to(buf)
    }

    /// Write the Source FEC Payload ID trailer after the packet payload.
    pub fn write_source_fec_payload_id_trailer<B: BufMut>(&self, buf: &mut B) -> Result<usize> {
        debug_assert_eq!(
            self.has_source_fec_trailer(),
            self.source_fec_payload_id.is_some(),
            "fec_type and source_fec_payload_id disagree on trailer presence"
        );
        if let Some(ref fec_id) = self.source_fec_payload_id {
            fec_id.write_to(buf)
        } else {
            Ok(0)
        }
    }

    /// Read the base MMTP header from the buffer.
    ///
    /// Does **not** populate `source_fec_payload_id`: when `fec_type = 1` the
    /// SS_ID is a payload *trailer*, not a header prefix, and cannot be parsed
    /// without the full payload span. The field is therefore always `None` after
    /// a read — recover the SS_ID from the payload via
    /// [`Self::split_payload_and_source_fec_trailer`].
    pub fn read_from<B: Buf>(buf: &mut B) -> Result<Self> {
        let base = MmtpHeader::read_from(buf)?;

        Ok(Self {
            base,
            source_fec_payload_id: None,
        })
    }

    /// Split a payload/trailer span according to the header FEC type.
    ///
    /// When `fec_type = 1`, the final four bytes are returned as the Source FEC
    /// Payload ID and the first slice is bounded at `len - 4`.
    pub fn split_payload_and_source_fec_trailer<'a>(
        &self,
        bytes: &'a [u8],
    ) -> Result<(&'a [u8], Option<SourceFecPayloadId>)> {
        if self.has_source_fec_trailer() {
            let (payload, fec_id) = SourceFecPayloadId::split_payload_and_trailer(bytes)?;
            Ok((payload, Some(fec_id)))
        } else {
            Ok((bytes, None))
        }
    }
}

/// MPU (Media Processing Unit) Header
///
/// Per ISO/IEC 23008-1 Table 14:
/// ```text
/// +--------+--------+--------+--------+--------+--------+--------+--------+
/// |  length (16)    |FT|T|FI|A|frag_cnt|      mpu_sequence (32)           |
/// +--------+--------+--------+--------+--------+--------+--------+--------+
/// ```
/// Byte 0-1: Payload length (16-bit big-endian)
/// Byte 2: Fragment type (4 bits) | Timed (1 bit) | Fragmentation indicator (2 bits) | Aggregation (1 bit)
/// Byte 3: Fragment counter (8-bit)
/// Byte 4-7: MPU sequence number (32-bit big-endian)
#[derive(Debug, Clone, PartialEq)]
pub struct MpuHeader {
    /// Payload length (16-bit)
    pub payload_length: u16,
    /// Fragment type
    pub fragment_type: FragmentType,
    /// Timed media flag
    pub timed: bool,
    /// Fragmentation indicator (0=complete, 1=first, 2=middle, 3=last)
    pub fragmentation_indicator: u8,
    /// Aggregation flag (multiple data units in payload)
    pub aggregation: bool,
    /// Fragment counter
    pub fragment_counter: u8,
    /// MPU sequence number (32 bits)
    pub mpu_sequence: u32,
}

impl MpuHeader {
    /// Create a new MPU header
    pub fn new(fragment_type: FragmentType, mpu_sequence: u32) -> Self {
        Self {
            payload_length: 0,
            fragment_type,
            timed: true,
            fragmentation_indicator: 0, // 0 = complete (not fragmented)
            aggregation: false,
            fragment_counter: 0,
            mpu_sequence,
        }
    }

    /// Write header to buffer per ISO/IEC 23008-1
    #[inline]
    pub fn write_to<B: BufMut>(&self, buf: &mut B) -> Result<usize> {
        if buf.remaining_mut() < MPU_HEADER_SIZE {
            return Err(MmtError::BufferTooSmall {
                need: MPU_HEADER_SIZE,
                have: buf.remaining_mut(),
            });
        }

        // Bytes 0-1: payload_length (16-bit big-endian)
        buf.put_u16(self.payload_length);

        // Byte 2: fragment_type (4 bits) | timed (1 bit) | fragmentation_indicator (2 bits) | aggregation (1 bit)
        let byte2 = ((self.fragment_type as u8 & 0x0F) << 4)
            | ((self.timed as u8) << 3)
            | ((self.fragmentation_indicator & 0x03) << 1)
            | (self.aggregation as u8);
        buf.put_u8(byte2);

        // Byte 3: fragment_counter (8-bit)
        buf.put_u8(self.fragment_counter);

        // Bytes 4-7: mpu_sequence (32-bit big-endian)
        buf.put_u32(self.mpu_sequence);

        Ok(MPU_HEADER_SIZE)
    }

    /// Read header from buffer per ISO/IEC 23008-1
    #[inline]
    pub fn read_from<B: Buf>(buf: &mut B) -> Result<(Self, u32)> {
        if buf.remaining() < MPU_HEADER_SIZE {
            return Err(MmtError::BufferTooSmall {
                need: MPU_HEADER_SIZE,
                have: buf.remaining(),
            });
        }

        // Bytes 0-1: payload_length (16-bit)
        let payload_length = buf.get_u16();

        // Byte 2: flags
        let byte2 = buf.get_u8();
        let fragment_type = FragmentType::from_u8((byte2 >> 4) & 0x0F)?;
        let timed = (byte2 & 0x08) != 0;
        let fragmentation_indicator = (byte2 >> 1) & 0x03;
        let aggregation = (byte2 & 0x01) != 0;

        // Byte 3: fragment_counter
        let fragment_counter = buf.get_u8();

        // Bytes 4-7: mpu_sequence (32-bit)
        let mpu_sequence = buf.get_u32();

        Ok((
            MpuHeader {
                payload_length,
                fragment_type,
                timed,
                fragmentation_indicator,
                aggregation,
                fragment_counter,
                mpu_sequence,
            },
            payload_length as u32,
        ))
    }

    /// Get the header size
    #[inline]
    pub const fn size() -> usize {
        MPU_HEADER_SIZE
    }
}

/// Fragment type for MPU
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FragmentType {
    /// Initialization segment (ftyp + moov)
    Init = 0x00,
    /// Movie fragment (moof + mdat)
    Fragment = 0x01,
    /// Media Fragment Unit (individual samples/NAL units)
    Mfu = 0x02,
}

impl FragmentType {
    /// Convert from raw u8 value
    #[inline]
    pub fn from_u8(val: u8) -> Result<Self> {
        match val {
            0x00 => Ok(FragmentType::Init),
            0x01 => Ok(FragmentType::Fragment),
            0x02 => Ok(FragmentType::Mfu),
            _ => Err(MmtError::InvalidFragmentType(val)),
        }
    }
}

/// MFU (Media Fragment Unit) Data Unit Header
///
/// Used when fragment_type is Mfu (0x02) for individual sample delivery.
///
/// ```text
/// +--------+--------+--------+--------+
/// |     movie_fragment_sequence       |
/// +--------+--------+--------+--------+
/// |          sample_number            |
/// +--------+--------+--------+--------+
/// |            offset                 |
/// +--------+--------+--------+--------+
/// |priority|dep_cnt |   (payload)     |
/// +--------+--------+
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct MfuDataUnit {
    /// Fragment type (should be Mfu)
    pub fragment_type: FragmentType,
    /// Movie fragment sequence number
    pub movie_fragment_sequence: u32,
    /// Sample number within the movie fragment
    pub sample_number: u32,
    /// Byte offset within the sample
    pub offset: u32,
    /// Priority (0 = highest)
    pub priority: u8,
    /// Dependency counter
    pub dep_counter: u8,
}

impl MfuDataUnit {
    /// Create a new MFU data unit header
    pub fn new(movie_fragment_sequence: u32, sample_number: u32) -> Self {
        Self {
            fragment_type: FragmentType::Mfu,
            movie_fragment_sequence,
            sample_number,
            offset: 0,
            priority: 0,
            dep_counter: 0,
        }
    }

    /// Write header to buffer (zero-copy)
    #[inline]
    pub fn write_to<B: BufMut>(&self, buf: &mut B) -> Result<usize> {
        // MFU header is 14 bytes when including length field
        const MFU_WRITE_SIZE: usize = 14;

        if buf.remaining_mut() < MFU_WRITE_SIZE {
            return Err(MmtError::BufferTooSmall {
                need: MFU_WRITE_SIZE,
                have: buf.remaining_mut(),
            });
        }

        // Bytes 0-3: movie_fragment_sequence
        buf.put_u32(self.movie_fragment_sequence);

        // Bytes 4-7: sample_number
        buf.put_u32(self.sample_number);

        // Bytes 8-11: offset
        buf.put_u32(self.offset);

        // Byte 12: priority
        buf.put_u8(self.priority);

        // Byte 13: dep_counter
        buf.put_u8(self.dep_counter);

        Ok(MFU_WRITE_SIZE)
    }

    /// Read header from buffer (zero-copy)
    #[inline]
    pub fn read_from<B: Buf>(buf: &mut B) -> Result<Self> {
        const MFU_READ_SIZE: usize = 14;

        if buf.remaining() < MFU_READ_SIZE {
            return Err(MmtError::BufferTooSmall {
                need: MFU_READ_SIZE,
                have: buf.remaining(),
            });
        }

        let movie_fragment_sequence = buf.get_u32();
        let sample_number = buf.get_u32();
        let offset = buf.get_u32();
        let priority = buf.get_u8();
        let dep_counter = buf.get_u8();

        Ok(MfuDataUnit {
            fragment_type: FragmentType::Mfu,
            movie_fragment_sequence,
            sample_number,
            offset,
            priority,
            dep_counter,
        })
    }

    /// Get the header size
    #[inline]
    pub const fn size() -> usize {
        14
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mmtp_header_roundtrip() {
        // ISO/IEC 23008-1:2023 Table 6 format
        let header = MmtpHeader {
            version: 0,
            payload_type_extension_flag: true,
            fec_type: 1,
            extension_flag: false,
            rap_flag: true,
            packet_type: PacketType::Mpu,
            packet_id: 0x1234,
            timestamp: 0xABCDEF01,
            packet_sequence: 0x12345678,
            extension: None,
        };

        let mut buf = vec![0u8; 64];
        let written = header.write_to(&mut buf.as_mut_slice()).unwrap();
        assert_eq!(written, MMTP_HEADER_SIZE);

        let mut read_buf = &buf[..written];
        let parsed = MmtpHeader::read_from(&mut read_buf).unwrap();

        assert_eq!(header.version, parsed.version);
        assert_eq!(
            header.payload_type_extension_flag,
            parsed.payload_type_extension_flag
        );
        assert_eq!(header.fec_type, parsed.fec_type);
        assert_eq!(header.extension_flag, parsed.extension_flag);
        assert_eq!(header.rap_flag, parsed.rap_flag);
        assert_eq!(header.packet_type, parsed.packet_type);
        assert_eq!(header.packet_id, parsed.packet_id);
        assert_eq!(header.timestamp, parsed.timestamp);
        assert_eq!(header.packet_sequence, parsed.packet_sequence);
    }

    #[test]
    fn test_mmtp_header_byte_layout() {
        // Test ISO/IEC 23008-1:2023 Table 6 byte layout:
        // Byte 0: V(2) | C(1) | FEC(2) | r(1) | X(1) | R(1)
        // Byte 1: RES(2) | type(6)
        let header = MmtpHeader {
            version: 0,                         // V = 00
            payload_type_extension_flag: false, // C = 0
            fec_type: 0,                        // FEC = 00
            extension_flag: false,              // X = 0
            rap_flag: true,                     // R = 1
            packet_type: PacketType::Mpu,       // type = 0x00
            packet_id: 0x0001,
            timestamp: 0x00000001,
            packet_sequence: 0x00000001,
            extension: None,
        };

        let mut buf = vec![0u8; 64];
        header.write_to(&mut buf.as_mut_slice()).unwrap();

        // Byte 0: V=00, C=0, FEC=00, r=0, X=0, R=1 -> 0x01
        assert_eq!(buf[0], 0x01);
        // Byte 1: RES=00, type=000000 -> 0x00
        assert_eq!(buf[1], 0x00);
        // Bytes 2-3: packet_id = 0x0001
        assert_eq!(buf[2], 0x00);
        assert_eq!(buf[3], 0x01);
    }

    #[test]
    fn test_mmtp_header_fec_type() {
        // Test FEC type encoding in byte 0
        let header = MmtpHeader {
            version: 0,
            payload_type_extension_flag: false,
            fec_type: FecType::WithSourcePayloadId as u8, // FEC = 01
            extension_flag: false,
            rap_flag: false,
            packet_type: PacketType::Mpu,
            packet_id: 0x0001,
            timestamp: 0x00000001,
            packet_sequence: 0x00000001,
            extension: None,
        };

        let mut buf = vec![0u8; 64];
        header.write_to(&mut buf.as_mut_slice()).unwrap();

        // Byte 0: V=00, C=0, FEC=01, r=0, X=0, R=0 -> 0x08
        assert_eq!(buf[0], 0x08);

        let mut read_buf = &buf[..MMTP_HEADER_SIZE];
        let parsed = MmtpHeader::read_from(&mut read_buf).unwrap();
        assert_eq!(parsed.fec_type, FecType::WithSourcePayloadId as u8);
    }

    #[test]
    fn test_mmtp_header_ext_source_fec_id_is_trailer_not_prefix() {
        let fec_id = SourceFecPayloadId::new(0x01020304);
        let header = MmtpHeaderExt::with_fec(0x1234, PacketType::Mpu, fec_id);

        let mut buf = vec![0u8; 64];
        let written = header.write_to(&mut buf.as_mut_slice()).unwrap();

        // fec_type=1 advertises a Source FEC Payload ID, but the SS_ID is not
        // part of the MMTP header. It is appended after the MPU payload.
        assert_eq!(written, MMTP_HEADER_SIZE);
        assert_eq!(&buf[written..written + SourceFecPayloadId::SIZE], &[0; 4]);
    }

    #[test]
    fn test_mmtp_header_ext_read_does_not_consume_payload_as_fec_prefix() {
        let fec_id = SourceFecPayloadId::new(0x01020304);
        let header = MmtpHeaderExt::with_fec(0x1234, PacketType::Mpu, fec_id);
        let payload = [0xAA, 0xBB, 0xCC, 0xDD, 0xEE];

        let mut packet = Vec::new();
        header.write_to(&mut packet).unwrap();
        packet.extend_from_slice(&payload);
        packet.extend_from_slice(&fec_id.to_bytes());

        let mut read_buf = &packet[..];
        let parsed = MmtpHeaderExt::read_from(&mut read_buf).unwrap();

        assert_eq!(parsed.base.fec_type, FecType::WithSourcePayloadId as u8);
        assert!(parsed.source_fec_payload_id.is_none());
        assert_eq!(read_buf.len(), payload.len() + SourceFecPayloadId::SIZE);
        assert_eq!(read_buf, [&payload[..], &fec_id.to_bytes()].concat());
    }

    #[test]
    fn test_source_fec_payload_id_split_uses_payload_end_len_minus_4() {
        let header =
            MmtpHeaderExt::with_fec(0x1234, PacketType::Mpu, SourceFecPayloadId::new(0x01020304));
        let mut payload_with_trailer = b"video-data".to_vec();
        payload_with_trailer.extend_from_slice(&0x01020304u32.to_be_bytes());

        let (payload, parsed_fec_id) = header
            .split_payload_and_source_fec_trailer(&payload_with_trailer)
            .unwrap();

        assert_eq!(payload, b"video-data");
        assert_eq!(
            payload.len(),
            payload_with_trailer.len() - SourceFecPayloadId::SIZE
        );
        assert_eq!(parsed_fec_id.unwrap().ss_id, 0x01020304);
    }

    #[test]
    fn test_mpu_header_roundtrip() {
        let header = MpuHeader {
            payload_length: 0,
            fragment_type: FragmentType::Fragment,
            timed: true,
            fragmentation_indicator: 0,
            aggregation: false,
            fragment_counter: 0,
            mpu_sequence: 0x123456,
        };

        let mut buf = vec![0u8; 64];
        let written = header.write_to(&mut buf.as_mut_slice()).unwrap();
        assert_eq!(written, MPU_HEADER_SIZE);

        let mut read_buf = &buf[..written];
        let (parsed, _payload_len) = MpuHeader::read_from(&mut read_buf).unwrap();

        assert_eq!(header.fragment_type, parsed.fragment_type);
        assert_eq!(header.timed, parsed.timed);
        assert_eq!(header.aggregation, parsed.aggregation);
        assert_eq!(header.mpu_sequence, parsed.mpu_sequence);
    }

    #[test]
    fn test_mfu_data_unit_roundtrip() {
        let mfu = MfuDataUnit {
            fragment_type: FragmentType::Mfu,
            movie_fragment_sequence: 0x12345678,
            sample_number: 0x00000042,
            offset: 0x00001000,
            priority: 5,
            dep_counter: 3,
        };

        let mut buf = vec![0u8; 64];
        let written = mfu.write_to(&mut buf.as_mut_slice()).unwrap();
        assert_eq!(written, 14);

        let mut read_buf = &buf[..written];
        let parsed = MfuDataUnit::read_from(&mut read_buf).unwrap();

        assert_eq!(mfu.movie_fragment_sequence, parsed.movie_fragment_sequence);
        assert_eq!(mfu.sample_number, parsed.sample_number);
        assert_eq!(mfu.offset, parsed.offset);
        assert_eq!(mfu.priority, parsed.priority);
        assert_eq!(mfu.dep_counter, parsed.dep_counter);
    }

    #[test]
    fn test_mfu_header_size_matches_runtime() {
        // Pins MFU_HEADER_SIZE against MfuDataUnit::size() and the actual byte
        // count produced by write_to(). The previous value (12) drifted from
        // the parser/serializer (14) and this test prevents a re-regression.
        assert_eq!(MFU_HEADER_SIZE, 14);
        assert_eq!(MfuDataUnit::size(), MFU_HEADER_SIZE);

        let mfu = MfuDataUnit {
            fragment_type: FragmentType::Mfu,
            movie_fragment_sequence: 0,
            sample_number: 0,
            offset: 0,
            priority: 0,
            dep_counter: 0,
        };
        let mut buf = vec![0u8; 32];
        let written = mfu.write_to(&mut buf.as_mut_slice()).unwrap();
        assert_eq!(written, MFU_HEADER_SIZE);
    }

    #[test]
    fn test_buffer_too_small() {
        let header = MmtpHeader::new(1, PacketType::Mpu);
        let mut buf = vec![0u8; 4]; // Too small

        let result = header.write_to(&mut buf.as_mut_slice());
        assert!(matches!(result, Err(MmtError::BufferTooSmall { .. })));
    }

    #[test]
    fn test_invalid_packet_type() {
        // ISO/IEC 23008-1:2023 format: Byte 1 = RES(2) | type(6)
        let mut buf: &[u8] = &[
            0x00, // Byte 0: V=0, C=0, FEC=0, r=0, X=0, R=0
            0x0F, // Byte 1: RES=00, type=001111 = 15 (invalid)
            0x00, 0x01, // packet_id
            0x00, 0x00, 0x00, 0x01, // timestamp
            0x00, 0x00, 0x00, 0x01, // packet_sequence
        ];

        let result = MmtpHeader::read_from(&mut buf);
        assert!(matches!(result, Err(MmtError::InvalidPacketType(15))));
    }

    #[test]
    fn test_packet_type_values() {
        assert_eq!(PacketType::Mpu as u8, 0x00);
        assert_eq!(PacketType::Generic as u8, 0x01);
        assert_eq!(PacketType::Control as u8, 0x02);
    }

    #[test]
    fn test_fragment_type_values() {
        assert_eq!(FragmentType::Init as u8, 0x00);
        assert_eq!(FragmentType::Fragment as u8, 0x01);
        assert_eq!(FragmentType::Mfu as u8, 0x02);
    }
}
