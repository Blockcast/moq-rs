//! Error types for mmt-core

use thiserror::Error;

#[derive(Error, Debug)]
pub enum MmtError {
    #[error("Buffer too small: need {need} bytes, have {have}")]
    BufferTooSmall { need: usize, have: usize },

    #[error("Invalid packet type: {0}")]
    InvalidPacketType(u8),

    #[error("Invalid fragment type: {0}")]
    InvalidFragmentType(u8),

    #[error("Invalid FEC type: {0}")]
    InvalidFecType(u8),

    #[error("Packet too large: {size} bytes exceeds MTU {mtu}")]
    PacketTooLarge { size: usize, mtu: usize },

    #[error("Invalid start code in Annex B stream")]
    InvalidStartCode,

    #[error("Invalid FEC payload ID")]
    InvalidFecPayloadId,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, MmtError>;
