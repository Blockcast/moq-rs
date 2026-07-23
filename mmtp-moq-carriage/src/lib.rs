// SPDX-FileCopyrightText: 2025-2026 Blockcast Inc. and contributors
// SPDX-License-Identifier: Apache-2.0

//! Native MMTP object carriage over MoQT draft-16.

use bytes::Bytes;
use moq_transport::{
    coding::Value,
    data::ExtensionHeaders,
    serve::{ServeError, Subgroup, SubgroupWriter, SubgroupsWriter},
};

/// Immutable shared codec/vector contract consumed by this adapter.
pub const WIRE_CONTRACT_VERSION: &str = "moqt-16-v1";

const FEC_METADATA_VERSION: u8 = 1;
const FEC_METADATA_LEN: usize = 12;

/// AL-FEC identity carried with an MMTP source or repair object.
///
/// This is carriage metadata only. It does not encode or decode repair symbols.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlFecMetadata {
    pub scheme: u8,
    pub source_block_number: u32,
    pub encoding_symbol_id: u32,
    pub repair: bool,
}

impl AlFecMetadata {
    pub fn encode(self) -> [u8; FEC_METADATA_LEN] {
        let mut encoded = [0; FEC_METADATA_LEN];
        encoded[0] = FEC_METADATA_VERSION;
        encoded[1] = self.scheme;
        encoded[2] = u8::from(self.repair);
        encoded[4..8].copy_from_slice(&self.source_block_number.to_be_bytes());
        encoded[8..12].copy_from_slice(&self.encoding_symbol_id.to_be_bytes());
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, CarriageError> {
        if encoded.len() != FEC_METADATA_LEN {
            return Err(CarriageError::InvalidFecMetadataLength(encoded.len()));
        }
        if encoded[0] != FEC_METADATA_VERSION {
            return Err(CarriageError::UnsupportedFecMetadataVersion(encoded[0]));
        }
        if encoded[2] > 1 || encoded[3] != 0 {
            return Err(CarriageError::InvalidFecMetadataFlags);
        }

        Ok(Self {
            scheme: encoded[1],
            repair: encoded[2] == 1,
            source_block_number: u32::from_be_bytes(encoded[4..8].try_into().unwrap()),
            encoding_symbol_id: u32::from_be_bytes(encoded[8..12].try_into().unwrap()),
        })
    }
}

/// One opaque MMTP packet and its MoQT object mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CarriageObject {
    pub group_id: u64,
    pub subgroup_id: u64,
    pub priority: u8,
    pub payload: Bytes,
    pub extension_headers: ExtensionHeaders,
}

impl CarriageObject {
    /// Attach AL-FEC metadata under a bytes-valued extension selected by the
    /// negotiated application profile. Unknown existing extensions are retained.
    pub fn set_fec_metadata(
        &mut self,
        extension_id: u64,
        metadata: AlFecMetadata,
    ) -> Result<(), CarriageError> {
        if extension_id.is_multiple_of(2) {
            return Err(CarriageError::FecExtensionMustBeBytes(extension_id));
        }
        self.extension_headers
            .set_bytesvalue(extension_id, metadata.encode().to_vec());
        Ok(())
    }

    /// Decode AL-FEC metadata without consuming or rewriting any extension.
    pub fn fec_metadata(&self, extension_id: u64) -> Result<Option<AlFecMetadata>, CarriageError> {
        let Some(extension) = self.extension_headers.get(extension_id) else {
            return Ok(None);
        };
        match &extension.value {
            Value::BytesValue(value) => AlFecMetadata::decode(value).map(Some),
            Value::IntValue(_) => Err(CarriageError::FecExtensionMustBeBytes(extension_id)),
        }
    }

    fn write_to(&self, subgroup: &mut SubgroupWriter) -> Result<(), CarriageError> {
        let mut object =
            subgroup.create(self.payload.len(), Some(self.extension_headers.clone()))?;
        object.write(self.payload.clone())?;
        Ok(())
    }
}

/// Ordered publisher adapter for a single MoQT track.
pub struct CarriageWriter {
    subgroups: SubgroupsWriter,
    current: Option<((u64, u64), SubgroupWriter)>,
}

impl CarriageWriter {
    pub fn new(subgroups: SubgroupsWriter) -> Self {
        Self {
            subgroups,
            current: None,
        }
    }

    pub fn publish(&mut self, object: &CarriageObject) -> Result<(), CarriageError> {
        let next = (object.group_id, object.subgroup_id);
        let current = self.current.as_ref().map(|(identity, _)| *identity);

        if current != Some(next) {
            if current.is_some_and(|identity| next <= identity) {
                return Err(CarriageError::NonMonotonicSubgroup {
                    current: current.unwrap(),
                    next,
                });
            }
            let writer = self.subgroups.create(Subgroup {
                group_id: object.group_id,
                subgroup_id: object.subgroup_id,
                priority: object.priority,
            })?;
            self.current = Some((next, writer));
        }

        object.write_to(&mut self.current.as_mut().unwrap().1)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CarriageError {
    #[error("AL-FEC extension {0:#x} must be bytes-valued (odd type)")]
    FecExtensionMustBeBytes(u64),
    #[error("AL-FEC metadata has {0} bytes; expected {FEC_METADATA_LEN}")]
    InvalidFecMetadataLength(usize),
    #[error("unsupported AL-FEC metadata version {0}")]
    UnsupportedFecMetadataVersion(u8),
    #[error("invalid AL-FEC metadata flags or reserved bits")]
    InvalidFecMetadataFlags,
    #[error("subgroup identity regressed from {current:?} to {next:?}")]
    NonMonotonicSubgroup {
        current: (u64, u64),
        next: (u64, u64),
    },
    #[error(transparent)]
    Transport(#[from] ServeError),
}
