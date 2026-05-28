// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Thin wrappers around libmmt's mmt-core parsers. Centralizes the
// "given a raw MMTP packet, give me (packet_id, mpu_sequence)" extraction
// the publisher needs to route packets to MoQ (track, group).
//
// libmmt/mmt-core implements ISO/IEC 23008-1 §9.2.2 (MMTP) and §A.3 (MPU)
// header decode. We do not re-implement the bit-twiddling.

use anyhow::{anyhow, Context, Result};
use mmt_core::header::{FragmentType, MmtpHeader, MpuHeader, PacketType, MMTP_HEADER_SIZE};

/// Routing key extracted from one MMTP packet's headers.
#[derive(Debug, Clone, PartialEq)]
pub struct PacketRouting {
    /// MMTP packet_id → MoQ track (per draft-ramadan-moq-mmt §4.1).
    pub packet_id: u16,
    /// MMTP packet type — Mpu, Generic, Control, or Repair.
    pub packet_type: PacketType,
    /// FEC type byte (0=none, 1=source, 2=repair, 3=reserved) per §3.1.
    pub fec_type: u8,
    /// Random Access Point flag — set on the first MMTP packet of a new
    /// MPU when the underlying media segment starts at a keyframe.
    pub rap_flag: bool,
    /// MPU sequence number → MoQ group_id (per §4.1). Present only when
    /// `packet_type == PacketType::Mpu` and the payload includes an MPU
    /// header (which §A.3 of the spec mandates for Mpu packets).
    pub mpu_sequence: Option<u32>,
    /// MPU fragment type (Init / Fragment / Mfu). Required for the
    /// publisher's A1 invariant: the first MMTP packet of a new MPU
    /// MUST carry FragmentType::Init (the MPU metadata box). Present
    /// only when `packet_type == PacketType::Mpu`.
    pub fragment_type: Option<FragmentType>,
}

/// Parse routing info from a raw MMTP packet. Does not copy the packet.
///
/// Returns `Err` if the buffer is shorter than the MMTP header (12 bytes),
/// or if `packet_type == Mpu` and the payload is shorter than the MPU
/// header (8 bytes).
pub fn route(packet: &[u8]) -> Result<PacketRouting> {
    if packet.len() < MMTP_HEADER_SIZE {
        return Err(anyhow!(
            "short MMTP packet: {} bytes (need ≥{})",
            packet.len(),
            MMTP_HEADER_SIZE
        ));
    }
    let mut cursor: &[u8] = packet;
    let hdr = MmtpHeader::read_from(&mut cursor)
        .map_err(|e| anyhow!("MMTP header decode failed: {e:?}"))?;
    let (mpu_sequence, fragment_type) = if hdr.packet_type == PacketType::Mpu {
        let mut payload: &[u8] = &packet[MMTP_HEADER_SIZE..];
        let (mpu, _payload_len) = MpuHeader::read_from(&mut payload)
            .map_err(|e| anyhow!("MPU header decode failed: {e:?}"))
            .context("MMTP packet_type=Mpu but MPU header decode failed")?;
        (Some(mpu.mpu_sequence), Some(mpu.fragment_type))
    } else {
        (None, None)
    };
    Ok(PacketRouting {
        packet_id: hdr.packet_id,
        packet_type: hdr.packet_type,
        fec_type: hdr.fec_type,
        rap_flag: hdr.rap_flag,
        mpu_sequence,
        fragment_type,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    /// Build a synthetic MMTP packet for tests.
    fn synth_mmtp_packet(
        packet_id: u16,
        packet_type: PacketType,
        rap_flag: bool,
        fec_type: u8,
        mpu_sequence: Option<u32>,
    ) -> Vec<u8> {
        let mut hdr = MmtpHeader::new(packet_id, packet_type);
        hdr.rap_flag = rap_flag;
        hdr.fec_type = fec_type;
        let mut buf = bytes::BytesMut::with_capacity(64);
        hdr.write_to(&mut buf).unwrap();
        if let Some(seq) = mpu_sequence {
            let mut mpu = MpuHeader::new(mmt_core::header::FragmentType::Init, seq);
            mpu.payload_length = 0;
            mpu.write_to(&mut buf).unwrap();
        }
        // Trail a couple of payload bytes for realism.
        buf.put_slice(&[0xAA, 0xBB]);
        buf.to_vec()
    }

    #[test]
    fn rejects_short_packet() {
        let err = route(&[0u8; 8]).unwrap_err();
        assert!(err.to_string().contains("short MMTP"));
    }

    #[test]
    fn parses_mpu_packet_with_sequence() {
        let pkt = synth_mmtp_packet(17, PacketType::Mpu, true, 0, Some(42));
        let r = route(&pkt).unwrap();
        assert_eq!(r.packet_id, 17);
        assert_eq!(r.packet_type, PacketType::Mpu);
        assert!(r.rap_flag);
        assert_eq!(r.fec_type, 0);
        assert_eq!(r.mpu_sequence, Some(42));
        // fragment_type is required for the publisher's A1 Init-only-first-packet check.
        assert_eq!(
            r.fragment_type,
            Some(mmt_core::header::FragmentType::Init)
        );
    }

    #[test]
    fn parses_repair_packet_without_mpu() {
        let pkt = synth_mmtp_packet(18, PacketType::Repair, false, 2, None);
        let r = route(&pkt).unwrap();
        assert_eq!(r.packet_id, 18);
        assert_eq!(r.packet_type, PacketType::Repair);
        assert_eq!(r.fec_type, 2);
        assert_eq!(r.mpu_sequence, None);
        // Non-MPU packets have no MPU header, so no fragment_type.
        assert_eq!(r.fragment_type, None);
    }

    #[test]
    fn fails_when_mpu_packet_lacks_mpu_header() {
        // packet_type = Mpu but only 12 bytes total (no MPU header following).
        let mut pkt = synth_mmtp_packet(1, PacketType::Mpu, false, 0, Some(0));
        // Truncate everything after the 12-byte MMTP header.
        pkt.truncate(MMTP_HEADER_SIZE);
        let err = route(&pkt).unwrap_err();
        assert!(
            err.to_string().contains("MPU header decode failed")
                || err.to_string().contains("MMTP packet_type=Mpu"),
            "unexpected error: {err}"
        );
    }

    /// Build an MMTP packet carrying one MFU fragment with the given
    /// fragmentation_indicator (FI) and fragment_counter. Used by the
    /// raw-passthrough contract tests (B1=C) — see
    /// `accepts_fragmented_mfu_packets_at_fi_1_2_3` below.
    fn synth_mfu_fragment_packet(
        packet_id: u16,
        mpu_sequence: u32,
        fi: u8,
        fragment_counter: u8,
        payload: &[u8],
    ) -> Vec<u8> {
        let hdr = MmtpHeader::new(packet_id, PacketType::Mpu);
        let mut mpu = MpuHeader::new(FragmentType::Mfu, mpu_sequence);
        mpu.fragmentation_indicator = fi;
        mpu.fragment_counter = fragment_counter;
        mpu.payload_length = payload.len() as u16;
        let mut buf = bytes::BytesMut::with_capacity(64);
        hdr.write_to(&mut buf).unwrap();
        mpu.write_to(&mut buf).unwrap();
        buf.put_slice(payload);
        buf.to_vec()
    }

    #[test]
    fn accepts_fragmented_mfu_packets_at_fi_1_2_3() {
        // B1=C raw-passthrough contract: the parser MUST accept MFU
        // fragments (FragmentType=Mfu, fragmentation_indicator ∈ {1,2,3})
        // without error. The publisher does not reassemble; FI is
        // intentionally absent from PacketRouting because each fragment
        // is forwarded as its own MoQ object and the receiver
        // reassembles via mmt-core::MfuReassembler.
        //
        // Pinning this contract here prevents future regressions where
        // the parser starts rejecting FI != 0 (which would reject every
        // video stream above 1080p audio — see BLO-8047 §B1 for the
        // MTU/I-frame fragmentation math).
        for (fi, counter) in [(1u8, 0u8), (2, 1), (3, 2)] {
            let pkt = synth_mfu_fragment_packet(7, 42, fi, counter, b"frag");
            let r = route(&pkt)
                .unwrap_or_else(|e| panic!("route() rejected FI={fi}, counter={counter}: {e}"));
            assert_eq!(r.packet_id, 7);
            assert_eq!(r.packet_type, PacketType::Mpu);
            assert_eq!(r.mpu_sequence, Some(42));
            assert_eq!(
                r.fragment_type,
                Some(FragmentType::Mfu),
                "FI={fi}: routing key must carry FragmentType::Mfu",
            );
        }
    }
}
