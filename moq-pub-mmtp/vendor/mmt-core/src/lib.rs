//! mmt-core: MMT protocol implementation
//!
//! This library provides zero-copy header parsing and generation for
//! MMTP (MPEG Media Transport Protocol) as specified in ISO/IEC 23008-1.
//!
//! ## Spec Compliance
//!
//! - **ISO/IEC 23008-1:2023** - MPEG Media Transport (MMT)
//!   - Section 9.2.2 - MMTP packet structure (Table 6)
//!   - Section 9.3.2 - MPU mode payload (Table 13, 14)
//!   - Section 7 - ISOBMFF-based MPU
//! - **ATSC A/331:2019** - Signaling, Delivery, Synchronization, and Error Protection
//!   - MMTUSD-1.0 schema (MPUComponent, mmtPackageId)
//!   - S-TSID-1.0 schema (SrcFlow, Payload formatId/srcFecPayloadId)
//!
//! ## Features
//!
//! - Zero-copy header parsing and generation
//! - Codec conversion (Annex B ↔ HVCC/AVCC)
//! - Packet assembly and fragmentation
//! - MFU reassembly with timeout handling
//! - Pre-allocated buffer pools for hot path
//!
//! ## Example
//!
//! ```rust
//! use mmt_core::{MmtpHeader, PacketType, PacketBuilder, FragmentType};
//!
//! // Create an MMTP header
//! let header = MmtpHeader::new(1, PacketType::Mpu);
//!
//! // Build a complete packet
//! let mut builder = PacketBuilder::new(1400);
//! let packet = builder.build_mpu_packet(
//!     1,                      // packet_id
//!     0,                      // mpu_sequence
//!     FragmentType::Init,     // fragment_type
//!     &[0x01, 0x02, 0x03],   // payload
//!     true,                   // rap_flag
//! ).unwrap();
//! ```

pub mod codec;
pub mod error;
pub mod header;
pub mod packet;
#[cfg(feature = "reassembler")]
pub mod reassembler;

// Re-export main types
pub use codec::{AnnexBNalIterator, AvcNalType, CodecConverter, HevcNalType, HvccNalIterator};
pub use error::{MmtError, Result};
pub use header::{
    FecType, FragmentType, MfuDataUnit, MmtpHeader, MmtpHeaderExt, MpuHeader, PacketType,
    SourceFecPayloadId, MFU_HEADER_SIZE, MMTP_HEADER_SIZE, MPU_HEADER_SIZE,
};
pub use packet::{
    FragmentIterator, PacketBuilder, PacketFragmenter, PacketPool, DEFAULT_MTU, MAX_PACKET_SIZE,
};
#[cfg(feature = "reassembler")]
pub use reassembler::{MfuFragment, MfuReassembler, ReassembledMfu, ReassemblerStats};
