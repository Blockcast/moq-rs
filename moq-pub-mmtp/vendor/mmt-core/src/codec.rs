//! Codec conversion utilities for H.264/H.265 NAL unit handling
//!
//! Provides conversion between:
//! - Annex B format (start code prefixed: 0x00000001 or 0x000001)
//! - HVCC/AVCC format (length prefixed)
//!
//! Zero-copy where possible, with pre-allocated output buffers.

use crate::error::{MmtError, Result};

/// NAL unit types for H.265/HEVC
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HevcNalType {
    TrailN = 0,
    TrailR = 1,
    TsaN = 2,
    TsaR = 3,
    StsaN = 4,
    StsaR = 5,
    RadlN = 6,
    RadlR = 7,
    RaslN = 8,
    RaslR = 9,
    BlaWLp = 16,
    BlaWRadl = 17,
    BlaNLp = 18,
    IdrWRadl = 19,
    IdrNLp = 20,
    CraNut = 21,
    VpsNut = 32,
    SpsNut = 33,
    PpsNut = 34,
    AudNut = 35,
    EosNut = 36,
    EobNut = 37,
    FdNut = 38,
    PrefixSeiNut = 39,
    SuffixSeiNut = 40,
}

impl HevcNalType {
    /// Check if this NAL type is a VCL (Video Coding Layer) unit
    #[inline]
    pub fn is_vcl(self) -> bool {
        (self as u8) < 32
    }

    /// Check if this NAL type is an IDR picture
    #[inline]
    pub fn is_idr(self) -> bool {
        matches!(self, HevcNalType::IdrWRadl | HevcNalType::IdrNLp)
    }

    /// Check if this NAL type is a random access point
    #[inline]
    pub fn is_rap(self) -> bool {
        matches!(
            self,
            HevcNalType::BlaWLp
                | HevcNalType::BlaWRadl
                | HevcNalType::BlaNLp
                | HevcNalType::IdrWRadl
                | HevcNalType::IdrNLp
                | HevcNalType::CraNut
        )
    }
}

/// NAL unit types for H.264/AVC
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AvcNalType {
    Unspecified = 0,
    Slice = 1,
    Dpa = 2,
    Dpb = 3,
    Dpc = 4,
    IdrSlice = 5,
    Sei = 6,
    Sps = 7,
    Pps = 8,
    Aud = 9,
    EndSequence = 10,
    EndStream = 11,
    Filler = 12,
}

impl AvcNalType {
    /// Check if this NAL type is an IDR slice
    #[inline]
    pub fn is_idr(self) -> bool {
        matches!(self, AvcNalType::IdrSlice)
    }
}

/// Codec converter for NAL unit format conversion
pub struct CodecConverter;

impl CodecConverter {
    /// Convert H.264/H.265 Annex B (start codes) to HVCC/AVCC (length prefixes)
    ///
    /// Input: NAL units separated by start codes (0x00000001 or 0x000001)
    /// Output: NAL units with 4-byte length prefixes
    ///
    /// # Example
    /// ```
    /// use mmt_core::CodecConverter;
    ///
    /// let annexb = vec![
    ///     0x00, 0x00, 0x00, 0x01,  // Start code
    ///     0x67, 0x42, 0x00, 0x1e,  // SPS NAL
    /// ];
    /// let hvcc = CodecConverter::annexb_to_hvcc(&annexb).unwrap();
    /// assert_eq!(&hvcc[0..4], &[0x00, 0x00, 0x00, 0x04]); // Length = 4
    /// ```
    pub fn annexb_to_hvcc(annexb: &[u8]) -> Result<Vec<u8>> {
        if annexb.is_empty() {
            return Ok(Vec::new());
        }

        // Pre-allocate output (same size or smaller)
        let mut output = Vec::with_capacity(annexb.len());
        let mut pos = 0;

        // Count leading zeros
        let zero_count_start = pos;
        while pos < annexb.len() && annexb[pos] == 0 {
            pos += 1;
        }
        let zero_count = pos - zero_count_start;

        // Need at least the start code indicator (0x01 after zeros)
        if pos >= annexb.len() {
            return Err(MmtError::InvalidStartCode);
        }

        // Verify we have a valid start code: need at least 2 zeros followed by 0x01
        // Valid: 0x000001 or 0x00000001
        if annexb[pos] != 1 || zero_count < 2 {
            return Err(MmtError::InvalidStartCode);
        }
        pos += 1;

        while pos < annexb.len() {
            // Find next start code
            let nal_start = pos;
            let nal_end = Self::find_next_start_code_position(&annexb[pos..])
                .map(|offset| pos + offset)
                .unwrap_or(annexb.len());

            let nal_len = nal_end - nal_start;
            if nal_len > 0 {
                // Reserve exact space needed (avoids potential reallocation)
                output.reserve(4 + nal_len);
                // Write 4-byte length prefix
                let len_bytes = (nal_len as u32).to_be_bytes();
                output.push(len_bytes[0]);
                output.push(len_bytes[1]);
                output.push(len_bytes[2]);
                output.push(len_bytes[3]);
                // Write NAL unit data
                output.extend_from_slice(&annexb[nal_start..nal_end]);
            }

            if nal_end >= annexb.len() {
                break;
            }

            // Skip the start code
            pos = nal_end;
            while pos < annexb.len() && annexb[pos] == 0 {
                pos += 1;
            }
            if pos < annexb.len() && annexb[pos] == 1 {
                pos += 1;
            } else {
                break;
            }
        }

        Ok(output)
    }

    /// Convert HVCC/AVCC (length prefixes) to Annex B (start codes)
    ///
    /// Input: NAL units with 4-byte length prefixes
    /// Output: NAL units separated by 4-byte start codes (0x00000001)
    pub fn hvcc_to_annexb(hvcc: &[u8]) -> Result<Vec<u8>> {
        if hvcc.is_empty() {
            return Ok(Vec::new());
        }

        // Pre-allocate output (same size, start codes same length as length fields)
        let mut output = Vec::with_capacity(hvcc.len());
        let mut pos = 0;

        while pos + 4 <= hvcc.len() {
            // Read 4-byte length prefix
            let nal_len =
                u32::from_be_bytes([hvcc[pos], hvcc[pos + 1], hvcc[pos + 2], hvcc[pos + 3]])
                    as usize;

            pos += 4;

            if pos + nal_len > hvcc.len() {
                return Err(MmtError::BufferTooSmall {
                    need: pos + nal_len,
                    have: hvcc.len(),
                });
            }

            // Reserve exact space needed (avoids potential reallocation)
            output.reserve(4 + nal_len);
            // Write 4-byte start code
            output.push(0x00);
            output.push(0x00);
            output.push(0x00);
            output.push(0x01);
            // Write NAL unit data
            output.extend_from_slice(&hvcc[pos..pos + nal_len]);

            pos += nal_len;
        }

        Ok(output)
    }

    /// Convert Annex B to HVCC in-place within a provided buffer
    ///
    /// Returns the number of bytes written to the output buffer.
    /// This is a zero-allocation version for hot paths.
    pub fn annexb_to_hvcc_inplace(annexb: &[u8], output: &mut [u8]) -> Result<usize> {
        if annexb.is_empty() {
            return Ok(0);
        }

        let mut out_pos = 0;
        let mut pos = 0;

        // Count leading zeros to find first start code
        let zero_count_start = pos;
        while pos < annexb.len() && annexb[pos] == 0 {
            pos += 1;
        }
        let zero_count = pos - zero_count_start;

        // Need at least 2 zeros followed by 0x01 for a valid start code
        if pos >= annexb.len() || annexb[pos] != 1 || zero_count < 2 {
            return Err(MmtError::InvalidStartCode);
        }
        pos += 1;

        while pos < annexb.len() {
            let nal_start = pos;
            let nal_end = Self::find_next_start_code_position(&annexb[pos..])
                .map(|offset| pos + offset)
                .unwrap_or(annexb.len());

            let nal_len = nal_end - nal_start;
            if nal_len > 0 {
                // Check output buffer space
                if out_pos + 4 + nal_len > output.len() {
                    return Err(MmtError::BufferTooSmall {
                        need: out_pos + 4 + nal_len,
                        have: output.len(),
                    });
                }

                // Write 4-byte length prefix
                output[out_pos..out_pos + 4].copy_from_slice(&(nal_len as u32).to_be_bytes());
                out_pos += 4;

                // Write NAL unit data
                output[out_pos..out_pos + nal_len].copy_from_slice(&annexb[nal_start..nal_end]);
                out_pos += nal_len;
            }

            if nal_end >= annexb.len() {
                break;
            }

            // Skip the start code
            pos = nal_end;
            while pos < annexb.len() && annexb[pos] == 0 {
                pos += 1;
            }
            if pos < annexb.len() && annexb[pos] == 1 {
                pos += 1;
            } else {
                break;
            }
        }

        Ok(out_pos)
    }

    /// Find the position of the next start code in the data
    ///
    /// Returns the offset from the start of `data` where the next start code begins,
    /// or None if no start code is found.
    fn find_next_start_code_position(data: &[u8]) -> Option<usize> {
        if data.len() < 3 {
            return None;
        }

        for i in 0..data.len() - 2 {
            // Check for 0x000001 or 0x00000001
            if data[i] == 0 && data[i + 1] == 0 {
                if data[i + 2] == 1 {
                    return Some(i);
                }
                if i + 3 < data.len() && data[i + 2] == 0 && data[i + 3] == 1 {
                    return Some(i);
                }
            }
        }

        None
    }

    /// Extract the HEVC NAL unit type from a NAL unit header
    ///
    /// The NAL unit type is in bits 1-6 of the first byte (after removing forbidden_zero_bit).
    #[inline]
    pub fn get_hevc_nal_type(nal_header: &[u8]) -> Option<u8> {
        if nal_header.is_empty() {
            return None;
        }
        // HEVC NAL header: forbidden_zero_bit (1) | nal_unit_type (6) | layer_id (6) | temporal_id (3)
        Some((nal_header[0] >> 1) & 0x3F)
    }

    /// Extract the AVC NAL unit type from a NAL unit header
    ///
    /// The NAL unit type is in bits 0-4 of the first byte.
    #[inline]
    pub fn get_avc_nal_type(nal_header: &[u8]) -> Option<u8> {
        if nal_header.is_empty() {
            return None;
        }
        // AVC NAL header: forbidden_zero_bit (1) | nal_ref_idc (2) | nal_unit_type (5)
        Some(nal_header[0] & 0x1F)
    }

    /// Check if an HEVC NAL unit is a random access point
    pub fn is_hevc_rap(nal_data: &[u8]) -> bool {
        if let Some(nal_type) = Self::get_hevc_nal_type(nal_data) {
            // BLA, IDR, CRA are RAP types (16-21)
            (16..=21).contains(&nal_type)
        } else {
            false
        }
    }

    /// Check if an AVC NAL unit is an IDR
    pub fn is_avc_idr(nal_data: &[u8]) -> bool {
        Self::get_avc_nal_type(nal_data) == Some(5)
    }

    /// Parse NAL units from HVCC format and return iterator
    pub fn parse_hvcc_nals(hvcc: &[u8]) -> HvccNalIterator<'_> {
        HvccNalIterator { data: hvcc, pos: 0 }
    }

    /// Parse NAL units from Annex B format and return iterator
    pub fn parse_annexb_nals(annexb: &[u8]) -> AnnexBNalIterator<'_> {
        AnnexBNalIterator {
            data: annexb,
            pos: 0,
        }
    }
}

/// Iterator over NAL units in HVCC format
pub struct HvccNalIterator<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for HvccNalIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos + 4 > self.data.len() {
            return None;
        }

        let nal_len = u32::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]) as usize;

        self.pos += 4;

        if self.pos + nal_len > self.data.len() {
            return None;
        }

        let nal = &self.data[self.pos..self.pos + nal_len];
        self.pos += nal_len;
        Some(nal)
    }
}

/// Iterator over NAL units in Annex B format
pub struct AnnexBNalIterator<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for AnnexBNalIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.data.len() {
            return None;
        }

        // Count leading zeros
        let zero_start = self.pos;
        while self.pos < self.data.len() && self.data[self.pos] == 0 {
            self.pos += 1;
        }
        let zero_count = self.pos - zero_start;

        // Check for valid start code: at least 2 zeros followed by 0x01
        if self.pos >= self.data.len() || self.data[self.pos] != 1 || zero_count < 2 {
            return None;
        }
        self.pos += 1;

        if self.pos >= self.data.len() {
            return None;
        }

        let nal_start = self.pos;

        // Find next start code
        let nal_end = CodecConverter::find_next_start_code_position(&self.data[self.pos..])
            .map(|offset| self.pos + offset)
            .unwrap_or(self.data.len());

        self.pos = nal_end;
        Some(&self.data[nal_start..nal_end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_annexb_to_hvcc_simple() {
        // Annex B: 0x00000001 + NAL
        let annexb = vec![
            0x00, 0x00, 0x00, 0x01, // Start code (4 bytes)
            0x67, 0x42, 0x00, 0x1e, // SPS NAL (4 bytes)
        ];

        let hvcc = CodecConverter::annexb_to_hvcc(&annexb).unwrap();

        // Expected: length prefix + NAL
        assert_eq!(hvcc.len(), 8);
        assert_eq!(&hvcc[0..4], &[0x00, 0x00, 0x00, 0x04]); // Length = 4
        assert_eq!(&hvcc[4..8], &[0x67, 0x42, 0x00, 0x1e]); // NAL data
    }

    #[test]
    fn test_annexb_to_hvcc_multiple_nals() {
        let annexb = vec![
            0x00, 0x00, 0x00, 0x01, // Start code
            0x67, 0x42, 0x00, 0x1e, // SPS NAL (4 bytes)
            0x00, 0x00, 0x00, 0x01, // Start code
            0x68, 0xce, 0x3c, 0x80, // PPS NAL (4 bytes)
        ];

        let hvcc = CodecConverter::annexb_to_hvcc(&annexb).unwrap();

        assert_eq!(hvcc.len(), 16);
        // First NAL
        assert_eq!(&hvcc[0..4], &[0x00, 0x00, 0x00, 0x04]); // Length = 4
        assert_eq!(&hvcc[4..8], &[0x67, 0x42, 0x00, 0x1e]); // SPS
                                                            // Second NAL
        assert_eq!(&hvcc[8..12], &[0x00, 0x00, 0x00, 0x04]); // Length = 4
        assert_eq!(&hvcc[12..16], &[0x68, 0xce, 0x3c, 0x80]); // PPS
    }

    #[test]
    fn test_annexb_to_hvcc_3byte_start_code() {
        // 3-byte start code (0x000001)
        let annexb = vec![
            0x00, 0x00, 0x01, // Start code (3 bytes)
            0x67, 0x42, 0x00, 0x1e, // SPS NAL
        ];

        let hvcc = CodecConverter::annexb_to_hvcc(&annexb).unwrap();

        assert_eq!(&hvcc[0..4], &[0x00, 0x00, 0x00, 0x04]); // Length = 4
        assert_eq!(&hvcc[4..8], &[0x67, 0x42, 0x00, 0x1e]); // NAL data
    }

    #[test]
    fn test_hvcc_to_annexb() {
        let hvcc = vec![
            0x00, 0x00, 0x00, 0x04, // Length = 4
            0x67, 0x42, 0x00, 0x1e, // SPS NAL
            0x00, 0x00, 0x00, 0x04, // Length = 4
            0x68, 0xce, 0x3c, 0x80, // PPS NAL
        ];

        let annexb = CodecConverter::hvcc_to_annexb(&hvcc).unwrap();

        assert_eq!(annexb.len(), 16);
        // First NAL
        assert_eq!(&annexb[0..4], &[0x00, 0x00, 0x00, 0x01]); // Start code
        assert_eq!(&annexb[4..8], &[0x67, 0x42, 0x00, 0x1e]); // SPS
                                                              // Second NAL
        assert_eq!(&annexb[8..12], &[0x00, 0x00, 0x00, 0x01]); // Start code
        assert_eq!(&annexb[12..16], &[0x68, 0xce, 0x3c, 0x80]); // PPS
    }

    #[test]
    fn test_roundtrip_annexb_hvcc_annexb() {
        let original = vec![
            0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1e, 0xab, 0xcd, 0x00, 0x00, 0x00, 0x01,
            0x68, 0xce, 0x3c, 0x80,
        ];

        let hvcc = CodecConverter::annexb_to_hvcc(&original).unwrap();
        let back = CodecConverter::hvcc_to_annexb(&hvcc).unwrap();

        assert_eq!(original, back);
    }

    #[test]
    fn test_annexb_to_hvcc_inplace() {
        let annexb = vec![0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1e];

        let mut output = vec![0u8; 64];
        let written = CodecConverter::annexb_to_hvcc_inplace(&annexb, &mut output).unwrap();

        assert_eq!(written, 8);
        assert_eq!(&output[0..4], &[0x00, 0x00, 0x00, 0x04]);
        assert_eq!(&output[4..8], &[0x67, 0x42, 0x00, 0x1e]);
    }

    #[test]
    fn test_empty_input() {
        assert_eq!(
            CodecConverter::annexb_to_hvcc(&[]).unwrap(),
            Vec::<u8>::new()
        );
        assert_eq!(
            CodecConverter::hvcc_to_annexb(&[]).unwrap(),
            Vec::<u8>::new()
        );
    }

    #[test]
    fn test_get_hevc_nal_type() {
        // HEVC VPS (nal_type = 32): forbidden_zero_bit(0) | nal_type(100000) | layer_id | tid
        // First byte: 0 | 100000 | 0 = 0x40
        let vps = [0x40, 0x01]; // VPS NAL
        assert_eq!(CodecConverter::get_hevc_nal_type(&vps), Some(32));

        // HEVC SPS (nal_type = 33): 0 | 100001 | 0 = 0x42
        let sps = [0x42, 0x01];
        assert_eq!(CodecConverter::get_hevc_nal_type(&sps), Some(33));

        // HEVC IDR (nal_type = 19): 0 | 010011 | 0 = 0x26
        let idr = [0x26, 0x01];
        assert_eq!(CodecConverter::get_hevc_nal_type(&idr), Some(19));
    }

    #[test]
    fn test_get_avc_nal_type() {
        // AVC SPS (nal_type = 7): forbidden(0) | ref_idc(11) | type(00111) = 0x67
        let sps = [0x67];
        assert_eq!(CodecConverter::get_avc_nal_type(&sps), Some(7));

        // AVC PPS (nal_type = 8): forbidden(0) | ref_idc(11) | type(01000) = 0x68
        let pps = [0x68];
        assert_eq!(CodecConverter::get_avc_nal_type(&pps), Some(8));

        // AVC IDR (nal_type = 5): forbidden(0) | ref_idc(11) | type(00101) = 0x65
        let idr = [0x65];
        assert_eq!(CodecConverter::get_avc_nal_type(&idr), Some(5));
    }

    #[test]
    fn test_is_hevc_rap() {
        // IDR NAL (type 19)
        let idr = [0x26, 0x01];
        assert!(CodecConverter::is_hevc_rap(&idr));

        // Trail NAL (type 1) - not RAP
        let trail = [0x02, 0x01];
        assert!(!CodecConverter::is_hevc_rap(&trail));
    }

    #[test]
    fn test_is_avc_idr() {
        let idr = [0x65];
        assert!(CodecConverter::is_avc_idr(&idr));

        let non_idr = [0x41];
        assert!(!CodecConverter::is_avc_idr(&non_idr));
    }

    #[test]
    fn test_hvcc_nal_iterator() {
        let hvcc = vec![
            0x00, 0x00, 0x00, 0x04, // Length = 4
            0x67, 0x42, 0x00, 0x1e, // SPS
            0x00, 0x00, 0x00, 0x02, // Length = 2
            0x68, 0xce, // PPS
        ];

        let nals: Vec<_> = CodecConverter::parse_hvcc_nals(&hvcc).collect();
        assert_eq!(nals.len(), 2);
        assert_eq!(nals[0], &[0x67, 0x42, 0x00, 0x1e]);
        assert_eq!(nals[1], &[0x68, 0xce]);
    }

    #[test]
    fn test_annexb_nal_iterator() {
        let annexb = vec![
            0x00, 0x00, 0x00, 0x01, // Start code
            0x67, 0x42, 0x00, 0x1e, // SPS
            0x00, 0x00, 0x01, // 3-byte start code
            0x68, 0xce, // PPS
        ];

        let nals: Vec<_> = CodecConverter::parse_annexb_nals(&annexb).collect();
        assert_eq!(nals.len(), 2);
        assert_eq!(nals[0], &[0x67, 0x42, 0x00, 0x1e]);
        assert_eq!(nals[1], &[0x68, 0xce]);
    }

    #[test]
    fn test_invalid_start_code() {
        let invalid = vec![0x01, 0x02, 0x03]; // No start code
        assert!(CodecConverter::annexb_to_hvcc(&invalid).is_err());
    }
}
