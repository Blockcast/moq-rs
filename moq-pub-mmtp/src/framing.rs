// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Stdin framing for MMTP packets. Each frame on the wire is `[u32 BE length][payload]`.

use anyhow::{bail, Context, Result};
use tokio::io::{AsyncRead, AsyncReadExt};

/// Maximum MMTP payload accepted per frame. The MMTP packet header has a
/// 16-bit length field, so any single packet on the wire is ≤ 65535 bytes.
/// We allow a small headroom for callers who concatenate FEC repair into
/// the same length-prefix frame.
pub const MAX_FRAME_BYTES: usize = 128 * 1024;

/// Read one length-prefixed MMTP frame from `reader`.
/// Returns `Ok(None)` on clean EOF before any bytes of the length prefix
/// are read. Returns `Err` on partial frame, oversize, or zero-length frame.
pub async fn read_one_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Vec<u8>>> {
    // Read the first byte separately so we can distinguish clean EOF (0 bytes
    // read) from a partial-prefix corruption (1-3 bytes read).
    let mut first = [0u8; 1];
    let n = reader
        .read(&mut first)
        .await
        .context("reading length prefix")?;
    if n == 0 {
        return Ok(None);
    }
    let mut rest = [0u8; 3];
    reader
        .read_exact(&mut rest)
        .await
        .context("reading length prefix")?;
    let prefix = [first[0], rest[0], rest[1], rest[2]];
    let len = u32::from_be_bytes(prefix) as usize;
    if len == 0 {
        bail!("zero-length MMTP frame on stdin (framing desync?)");
    }
    if len > MAX_FRAME_BYTES {
        bail!("MMTP frame length {len} exceeds MAX_FRAME_BYTES={MAX_FRAME_BYTES}");
    }
    let mut buf = vec![0u8; len];
    reader
        .read_exact(&mut buf)
        .await
        .with_context(|| format!("reading {len} payload bytes after length prefix"))?;
    Ok(Some(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn cursor(bytes: &[u8]) -> Cursor<Vec<u8>> {
        Cursor::new(bytes.to_vec())
    }

    #[tokio::test]
    async fn reads_single_frame() {
        let payload = b"hello mmtp";
        let mut wire = Vec::new();
        wire.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        wire.extend_from_slice(payload);
        let mut r = cursor(&wire);
        let f = read_one_frame(&mut r).await.unwrap().unwrap();
        assert_eq!(f, payload);
    }

    #[tokio::test]
    async fn reads_two_frames_in_sequence() {
        let mut wire = Vec::new();
        for p in [b"AAAA".as_slice(), b"BB".as_slice()] {
            wire.extend_from_slice(&(p.len() as u32).to_be_bytes());
            wire.extend_from_slice(p);
        }
        let mut r = cursor(&wire);
        assert_eq!(read_one_frame(&mut r).await.unwrap().unwrap(), b"AAAA");
        assert_eq!(read_one_frame(&mut r).await.unwrap().unwrap(), b"BB");
        // Stream now drained; next read is clean EOF.
        assert!(read_one_frame(&mut r).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn returns_none_on_clean_eof_before_prefix() {
        let mut r = cursor(b"");
        assert!(read_one_frame(&mut r).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn errors_on_partial_length_prefix() {
        // 3 bytes — incomplete prefix.
        let mut r = cursor(&[0x00, 0x00, 0x05]);
        let err = read_one_frame(&mut r).await.unwrap_err();
        assert!(
            err.to_string().contains("length prefix"),
            "err = {err:?}"
        );
    }

    #[tokio::test]
    async fn errors_on_partial_payload() {
        // length says 10, payload only 4 bytes.
        let mut wire = Vec::new();
        wire.extend_from_slice(&10u32.to_be_bytes());
        wire.extend_from_slice(b"abcd");
        let mut r = cursor(&wire);
        let err = read_one_frame(&mut r).await.unwrap_err();
        assert!(err.to_string().contains("payload bytes"), "err = {err:?}");
    }

    #[tokio::test]
    async fn rejects_zero_length_frame() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&0u32.to_be_bytes());
        let mut r = cursor(&wire);
        let err = read_one_frame(&mut r).await.unwrap_err();
        assert!(err.to_string().contains("zero-length"), "err = {err:?}");
    }

    #[tokio::test]
    async fn rejects_oversize_frame() {
        let oversize = (MAX_FRAME_BYTES as u32) + 1;
        let mut wire = Vec::new();
        wire.extend_from_slice(&oversize.to_be_bytes());
        let mut r = cursor(&wire);
        let err = read_one_frame(&mut r).await.unwrap_err();
        assert!(err.to_string().contains("MAX_FRAME_BYTES"), "err = {err:?}");
    }
}
