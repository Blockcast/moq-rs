//! Packet assembly and building for MMT
//!
//! Provides zero-copy packet building with pre-allocated buffers.
//! Handles fragmentation for payloads larger than MTU.

use crate::error::{MmtError, Result};
use crate::header::{
    FragmentType, MfuDataUnit, MmtpHeader, MpuHeader, PacketType, MMTP_HEADER_SIZE, MPU_HEADER_SIZE,
};
use bytes::BytesMut;

/// Default MTU size (typical UDP payload limit)
pub const DEFAULT_MTU: usize = 1400;

/// Maximum packet size
pub const MAX_PACKET_SIZE: usize = 65535;

/// Packet builder with zero-copy operations
///
/// Uses a pre-allocated buffer to avoid allocations in the hot path.
/// Supports building complete MMTP packets with headers and payload.
pub struct PacketBuilder {
    /// Maximum transmission unit
    mtu: usize,
    /// Pre-allocated buffer
    buffer: BytesMut,
    /// Current write position
    current_pos: usize,
    /// Packet sequence counter
    sequence: u32,
}

impl PacketBuilder {
    /// Create a new packet builder with specified MTU
    pub fn new(mtu: usize) -> Self {
        let mtu = mtu.min(MAX_PACKET_SIZE);
        Self {
            mtu,
            buffer: BytesMut::with_capacity(mtu),
            current_pos: 0,
            sequence: 0,
        }
    }

    /// Create a new packet builder with default MTU
    pub fn with_default_mtu() -> Self {
        Self::new(DEFAULT_MTU)
    }

    /// Reset the builder for a new packet
    #[inline]
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.current_pos = 0;
    }

    /// Get the current MTU
    #[inline]
    pub fn mtu(&self) -> usize {
        self.mtu
    }

    /// Get the current sequence number
    #[inline]
    pub fn sequence(&self) -> u32 {
        self.sequence
    }

    /// Increment and return the next sequence number
    #[inline]
    pub fn next_sequence(&mut self) -> u32 {
        let seq = self.sequence;
        self.sequence = self.sequence.wrapping_add(1);
        seq
    }

    /// Get remaining capacity in the buffer
    #[inline]
    pub fn remaining(&self) -> usize {
        self.mtu.saturating_sub(self.current_pos)
    }

    /// Get the current packet data
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.buffer[..self.current_pos]
    }

    /// Get the current packet length
    #[inline]
    pub fn len(&self) -> usize {
        self.current_pos
    }

    /// Check if the buffer is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.current_pos == 0
    }

    /// Add MMTP header to the packet
    #[inline]
    pub fn add_mmtp_header(&mut self, header: &MmtpHeader) -> Result<usize> {
        if self.remaining() < header.size() {
            return Err(MmtError::PacketTooLarge {
                size: self.current_pos + header.size(),
                mtu: self.mtu,
            });
        }

        // Ensure buffer has capacity
        if self.buffer.len() < self.current_pos + header.size() {
            self.buffer.resize(self.current_pos + header.size(), 0);
        }

        let mut slice = &mut self.buffer[self.current_pos..];
        let written = header.write_to(&mut slice)?;
        self.current_pos += written;
        Ok(written)
    }

    /// Add MPU header to the packet
    #[inline]
    pub fn add_mpu_header(&mut self, header: &MpuHeader) -> Result<usize> {
        if self.remaining() < MPU_HEADER_SIZE {
            return Err(MmtError::PacketTooLarge {
                size: self.current_pos + MPU_HEADER_SIZE,
                mtu: self.mtu,
            });
        }

        if self.buffer.len() < self.current_pos + MPU_HEADER_SIZE {
            self.buffer.resize(self.current_pos + MPU_HEADER_SIZE, 0);
        }

        let mut slice = &mut self.buffer[self.current_pos..];
        let written = header.write_to(&mut slice)?;
        self.current_pos += written;
        Ok(written)
    }

    /// Add MFU data unit header to the packet
    #[inline]
    pub fn add_mfu_header(&mut self, mfu: &MfuDataUnit) -> Result<usize> {
        let size = MfuDataUnit::size();
        if self.remaining() < size {
            return Err(MmtError::PacketTooLarge {
                size: self.current_pos + size,
                mtu: self.mtu,
            });
        }

        if self.buffer.len() < self.current_pos + size {
            self.buffer.resize(self.current_pos + size, 0);
        }

        let mut slice = &mut self.buffer[self.current_pos..];
        let written = mfu.write_to(&mut slice)?;
        self.current_pos += written;
        Ok(written)
    }

    /// Add raw payload data to the packet
    #[inline]
    pub fn add_payload(&mut self, payload: &[u8]) -> Result<usize> {
        if self.remaining() < payload.len() {
            return Err(MmtError::PacketTooLarge {
                size: self.current_pos + payload.len(),
                mtu: self.mtu,
            });
        }

        if self.buffer.len() < self.current_pos + payload.len() {
            self.buffer.resize(self.current_pos + payload.len(), 0);
        }

        self.buffer[self.current_pos..self.current_pos + payload.len()].copy_from_slice(payload);
        self.current_pos += payload.len();
        Ok(payload.len())
    }

    /// Update the payload length field in the MPU header
    ///
    /// This should be called after adding the payload to update the length field.
    /// The `mpu_header_offset` is the position where the MPU header starts.
    #[inline]
    pub fn update_mpu_length(&mut self, mpu_header_offset: usize) -> Result<()> {
        if mpu_header_offset + 4 > self.current_pos {
            return Err(MmtError::BufferTooSmall {
                need: 4,
                have: self.current_pos.saturating_sub(mpu_header_offset),
            });
        }

        // Payload length is from after MPU header to end of packet
        let payload_len = (self.current_pos - mpu_header_offset - MPU_HEADER_SIZE) as u32;
        let len_bytes = payload_len.to_be_bytes();
        self.buffer[mpu_header_offset..mpu_header_offset + 4].copy_from_slice(&len_bytes);
        Ok(())
    }

    /// Build a complete MPU packet with headers and payload
    ///
    /// This is a convenience method that builds a complete packet.
    pub fn build_mpu_packet(
        &mut self,
        packet_id: u16,
        mpu_sequence: u32,
        fragment_type: FragmentType,
        payload: &[u8],
        rap_flag: bool,
    ) -> Result<&[u8]> {
        self.reset();

        // Create headers
        let mmtp_header = MmtpHeader {
            version: 0,
            payload_type_extension_flag: false,
            fec_type: 0,
            extension_flag: false,
            rap_flag,
            packet_type: PacketType::Mpu,
            packet_id,
            timestamp: 0, // Caller should set this
            packet_sequence: self.next_sequence(),
            extension: None,
        };

        let mpu_header = MpuHeader::new(fragment_type, mpu_sequence);

        // Add headers
        self.add_mmtp_header(&mmtp_header)?;
        let mpu_offset = self.current_pos;
        self.add_mpu_header(&mpu_header)?;

        // Add payload
        self.add_payload(payload)?;

        // Update MPU length
        self.update_mpu_length(mpu_offset)?;

        Ok(self.data())
    }

    /// Build an MFU packet with sample data
    pub fn build_mfu_packet(
        &mut self,
        packet_id: u16,
        mpu_sequence: u32,
        movie_fragment_sequence: u32,
        sample_number: u32,
        sample_data: &[u8],
        rap_flag: bool,
    ) -> Result<&[u8]> {
        self.reset();

        let mmtp_header = MmtpHeader {
            version: 0,
            payload_type_extension_flag: false,
            fec_type: 0,
            extension_flag: false,
            rap_flag,
            packet_type: PacketType::Mpu,
            packet_id,
            timestamp: 0,
            packet_sequence: self.next_sequence(),
            extension: None,
        };

        let mpu_header = MpuHeader::new(FragmentType::Mfu, mpu_sequence);

        let mfu = MfuDataUnit::new(movie_fragment_sequence, sample_number);

        // Add all headers
        self.add_mmtp_header(&mmtp_header)?;
        let mpu_offset = self.current_pos;
        self.add_mpu_header(&mpu_header)?;
        self.add_mfu_header(&mfu)?;

        // Add sample data
        self.add_payload(sample_data)?;

        // Update MPU length
        self.update_mpu_length(mpu_offset)?;

        Ok(self.data())
    }

    /// Take ownership of the built packet data
    pub fn take(&mut self) -> BytesMut {
        let data = self.buffer.split_to(self.current_pos);
        self.current_pos = 0;
        data
    }
}

/// Packet fragmenter for splitting large payloads across multiple packets
pub struct PacketFragmenter {
    mtu: usize,
    max_payload_per_packet: usize,
}

impl PacketFragmenter {
    /// Create a new fragmenter with the given MTU
    pub fn new(mtu: usize) -> Self {
        // Reserve space for headers
        let header_overhead = MMTP_HEADER_SIZE + MPU_HEADER_SIZE + MfuDataUnit::size();
        let max_payload = mtu.saturating_sub(header_overhead);

        Self {
            mtu,
            max_payload_per_packet: max_payload,
        }
    }

    /// Get the configured MTU
    #[inline]
    pub fn mtu(&self) -> usize {
        self.mtu
    }

    /// Get the maximum payload size per packet
    #[inline]
    pub fn max_payload(&self) -> usize {
        self.max_payload_per_packet
    }

    /// Calculate the number of packets needed for a payload
    #[inline]
    pub fn packets_needed(&self, payload_len: usize) -> usize {
        if payload_len == 0 {
            return 1;
        }
        // A zero max_payload (MTU ≤ header overhead) can't carry any payload —
        // return 0 instead of dividing by zero. Mirrors fragment() yielding no
        // fragments for the same degenerate config.
        if self.max_payload_per_packet == 0 {
            return 0;
        }
        payload_len.div_ceil(self.max_payload_per_packet)
    }

    /// Fragment a payload into multiple packet payloads
    ///
    /// Returns an iterator over (offset, fragment) tuples.
    pub fn fragment<'a>(&'a self, payload: &'a [u8]) -> FragmentIterator<'a> {
        FragmentIterator {
            payload,
            max_size: self.max_payload_per_packet,
            offset: 0,
        }
    }
}

/// Iterator over payload fragments
pub struct FragmentIterator<'a> {
    payload: &'a [u8],
    max_size: usize,
    offset: usize,
}

impl<'a> Iterator for FragmentIterator<'a> {
    /// (offset, fragment_data)
    type Item = (usize, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        // A zero max_size (MTU ≤ header overhead) would leave `offset` unable to
        // advance — without this guard the iterator loops forever, growing any
        // collector unbounded. Treat "no room for payload" as "no fragments".
        if self.max_size == 0 || self.offset >= self.payload.len() {
            return None;
        }

        let start = self.offset;
        let end = (start + self.max_size).min(self.payload.len());
        let fragment = &self.payload[start..end];

        self.offset = end;
        Some((start, fragment))
    }
}

/// Pre-built packet pool for zero-allocation packet building
pub struct PacketPool {
    builders: Vec<PacketBuilder>,
    mtu: usize,
}

impl PacketPool {
    /// Create a new packet pool with the given capacity
    pub fn new(capacity: usize, mtu: usize) -> Self {
        let builders = (0..capacity).map(|_| PacketBuilder::new(mtu)).collect();

        Self { builders, mtu }
    }

    /// Get the configured MTU for this pool
    #[inline]
    pub fn mtu(&self) -> usize {
        self.mtu
    }

    /// Get a packet builder from the pool
    pub fn get(&mut self) -> Option<PacketBuilder> {
        self.builders.pop()
    }

    /// Return a packet builder to the pool
    pub fn put(&mut self, mut builder: PacketBuilder) {
        builder.reset();
        self.builders.push(builder);
    }

    /// Get the pool size
    pub fn len(&self) -> usize {
        self.builders.len()
    }

    /// Check if the pool is empty
    pub fn is_empty(&self) -> bool {
        self.builders.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fragmenter_minimal_viable_mtu_boundary() {
        // Lock the off-by-one around header_overhead: find the MTU where
        // max_payload transitions 0 → 1 without hardcoding the overhead.
        let overhead = (0..512)
            .find(|&mtu| PacketFragmenter::new(mtu).max_payload() == 1)
            .map(|mtu| mtu - 1)
            .expect("a minimal viable MTU exists below 512");

        assert_eq!(
            PacketFragmenter::new(overhead).max_payload(),
            0,
            "mtu == overhead → max_payload 0"
        );
        let frag = PacketFragmenter::new(overhead + 1);
        assert_eq!(frag.max_payload(), 1, "mtu == overhead+1 → max_payload 1");

        // With max_payload == 1, a 3-byte payload yields exactly 3 one-byte
        // fragments and packets_needed agrees.
        let frags: Vec<_> = frag.fragment(&[1u8, 2, 3]).collect();
        assert_eq!(frags.len(), 3);
        assert!(frags.iter().all(|(_, s)| s.len() == 1));
        assert_eq!(frag.packets_needed(3), 3);
    }

    #[test]
    fn test_fragmenter_zero_max_payload_terminates() {
        // MTU ≤ header overhead → max_payload == 0. Regression: fragment() must
        // terminate (return no fragments) rather than loop forever on a
        // non-advancing offset.
        let frag = PacketFragmenter::new(4);
        assert_eq!(frag.max_payload(), 0);
        assert_eq!(frag.fragment(&[1u8, 2, 3, 4, 5]).count(), 0);
        // packets_needed must not divide by zero on the same degenerate config.
        assert_eq!(frag.packets_needed(5), 0);
    }

    #[test]
    fn test_packet_builder_basic() {
        let builder = PacketBuilder::new(1400);
        assert_eq!(builder.mtu(), 1400);
        assert!(builder.is_empty());
    }

    #[test]
    fn test_add_mmtp_header() {
        let mut builder = PacketBuilder::new(1400);
        let header = MmtpHeader::new(1, PacketType::Mpu);

        let written = builder.add_mmtp_header(&header).unwrap();
        assert_eq!(written, MMTP_HEADER_SIZE);
        assert_eq!(builder.len(), MMTP_HEADER_SIZE);
    }

    #[test]
    fn test_add_payload() {
        let mut builder = PacketBuilder::new(1400);
        let payload = [0x01, 0x02, 0x03, 0x04];

        let header = MmtpHeader::new(1, PacketType::Mpu);
        builder.add_mmtp_header(&header).unwrap();

        let written = builder.add_payload(&payload).unwrap();
        assert_eq!(written, 4);
        assert_eq!(builder.len(), MMTP_HEADER_SIZE + 4);
    }

    #[test]
    fn test_build_mpu_packet() {
        let mut builder = PacketBuilder::new(1400);
        let payload = [0x01, 0x02, 0x03, 0x04];

        let packet = builder
            .build_mpu_packet(
                1,                  // packet_id
                0,                  // mpu_sequence
                FragmentType::Init, // fragment_type
                &payload,           // payload
                true,               // rap_flag
            )
            .unwrap();

        // Check packet structure
        assert!(packet.len() >= MMTP_HEADER_SIZE + MPU_HEADER_SIZE + 4);

        // Verify MMTP header per ISO/IEC 23008-1:2023 Table 6
        // Byte 0: V(2) | C(1) | FEC(2) | r(1) | X(1) | R(1)
        // RAP flag (R) is in bit 0 of byte 0
        assert_eq!(packet[0] & 0x01, 0x01); // rap_flag = true
    }

    #[test]
    fn test_build_mfu_packet() {
        let mut builder = PacketBuilder::new(1400);
        let sample_data = [0xAB, 0xCD, 0xEF];

        let packet = builder
            .build_mfu_packet(
                1,            // packet_id
                0,            // mpu_sequence
                1,            // movie_fragment_sequence
                1,            // sample_number
                &sample_data, // sample_data
                false,        // rap_flag
            )
            .unwrap();

        // Should have all headers plus sample data
        let expected_min = MMTP_HEADER_SIZE + MPU_HEADER_SIZE + MfuDataUnit::size() + 3;
        assert!(packet.len() >= expected_min);
    }

    #[test]
    fn test_packet_too_large() {
        let mut builder = PacketBuilder::new(20); // Very small MTU
        let large_payload = [0u8; 100];

        let result = builder.add_payload(&large_payload);
        assert!(matches!(result, Err(MmtError::PacketTooLarge { .. })));
    }

    #[test]
    fn test_sequence_increment() {
        let mut builder = PacketBuilder::new(1400);
        assert_eq!(builder.next_sequence(), 0);
        assert_eq!(builder.next_sequence(), 1);
        assert_eq!(builder.next_sequence(), 2);
        assert_eq!(builder.sequence(), 3);
    }

    #[test]
    fn test_reset() {
        let mut builder = PacketBuilder::new(1400);
        let payload = [0x01, 0x02, 0x03];

        builder.add_payload(&payload).unwrap();
        assert_eq!(builder.len(), 3);

        builder.reset();
        assert!(builder.is_empty());
    }

    #[test]
    fn test_fragmenter_basic() {
        let fragmenter = PacketFragmenter::new(1400);
        assert!(fragmenter.max_payload() > 0);
        assert!(fragmenter.max_payload() < 1400);
    }

    #[test]
    fn test_fragmenter_packets_needed() {
        let fragmenter = PacketFragmenter::new(1400);
        let max_payload = fragmenter.max_payload();

        assert_eq!(fragmenter.packets_needed(0), 1);
        assert_eq!(fragmenter.packets_needed(max_payload), 1);
        assert_eq!(fragmenter.packets_needed(max_payload + 1), 2);
        assert_eq!(fragmenter.packets_needed(max_payload * 3), 3);
    }

    #[test]
    fn test_fragmenter_iteration() {
        let fragmenter = PacketFragmenter::new(100); // Small MTU for testing
        let max_payload = fragmenter.max_payload();

        // Create payload that needs 3 fragments
        let payload = vec![0xAB; max_payload * 2 + 10];

        let fragments: Vec<_> = fragmenter.fragment(&payload).collect();
        assert_eq!(fragments.len(), 3);

        // Verify offsets
        assert_eq!(fragments[0].0, 0);
        assert_eq!(fragments[1].0, max_payload);
        assert_eq!(fragments[2].0, max_payload * 2);

        // Verify fragment sizes
        assert_eq!(fragments[0].1.len(), max_payload);
        assert_eq!(fragments[1].1.len(), max_payload);
        assert_eq!(fragments[2].1.len(), 10);
    }

    #[test]
    fn test_packet_pool() {
        let mut pool = PacketPool::new(3, 1400);
        assert_eq!(pool.len(), 3);

        let builder1 = pool.get().unwrap();
        assert_eq!(pool.len(), 2);

        let builder2 = pool.get().unwrap();
        assert_eq!(pool.len(), 1);

        pool.put(builder1);
        assert_eq!(pool.len(), 2);

        pool.put(builder2);
        assert_eq!(pool.len(), 3);
    }

    #[test]
    fn test_take_ownership() {
        let mut builder = PacketBuilder::new(1400);
        let payload = [0x01, 0x02, 0x03, 0x04];
        builder.add_payload(&payload).unwrap();

        let data = builder.take();
        assert_eq!(data.as_ref(), &payload);
        assert!(builder.is_empty());
    }
}
