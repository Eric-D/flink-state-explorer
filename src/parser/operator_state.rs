//! Managed **operator** state streams (the delegate of an `OperatorStreamStateHandle`): the
//! header's compression flag, and the bytes of each element the handle's offsets point at.
//!
//! Layout, verified against Flink `release-1.20` (`flink-runtime/src/main/java/org/apache/flink/
//! runtime/state/`) and snappy-java 1.1.10.4, the version Flink 1.20 pins (root `pom.xml`):
//!
//! - `DefaultOperatorStateBackendSnapshotStrategy.java:165-212` writes the
//!   `OperatorBackendSerializationProxy` header straight to the checkpoint stream, then opens
//!   **one** `CompressibleFSDataOutputStream` over the rest of the stream and writes every list
//!   state's elements (`PartitionableListState.write`, :110-123), then every broadcast state's
//!   map (`HeapBroadcastState.write`, :109-120: `writeInt(size)` then the key/value pairs). Each
//!   recorded offset is `out.getPos()`, taken just before one element (or one broadcast map).
//! - The header (`OperatorBackendSerializationProxy.write`, :97-103) starts with
//!   `writeInt(VERSION = 6)` (`VersionedIOReadableWritable.write`) then
//!   `writeBoolean(usingStateCompression)`; `read` (:114-130) reads that flag only for version 6
//!   (meta-info snapshot version >= 7), so older streams are never compressed.
//! - With compression the wrapper is a `SnappyFramedOutputStream`
//!   (`SnappyStreamCompressionDecorator.java:45-48`). Its constructor writes the Snappy stream
//!   identifier once (`SnappyFramedOutputStream.java:253`, `:263-267`): right after the header,
//!   before the first offset. `CompressibleFSDataOutputStream.getPos` (:43-49) **flushes** it
//!   before returning the position, so every offset is a chunk boundary and the chunks between
//!   one offset and the next hold exactly that element.
//! - Restore mirrors this (`OperatorStateRestoreOperation.java:182-209`): one decompressing
//!   stream over the rest, then `seek(offset)` per element, which drops whatever was buffered
//!   and resumes reading chunks at that raw position (`CompressibleFSDataInputStream.java:45-53`).
//!
//! So the stream is compressed (or not) as a whole, but each element can be inflated on its own.

use std::borrow::Cow;

use crate::parser::state_handle::PartitionOffsetEntry;

/// `OperatorBackendSerializationProxy.VERSION` in Flink 1.20.
pub const OPERATOR_BACKEND_VERSION: i32 = 6;

/// The leading fields of an operator-state stream's `OperatorBackendSerializationProxy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperatorStreamHeader {
    pub version: i32,
    /// Everything after the header is one Snappy-framed stream.
    pub using_state_compression: bool,
}

/// Read the version and the compression flag. `None` if `data` does not start like an operator
/// backend stream (Flink accepts versions 1 to 6: `getCompatibleVersions`, :91-94).
pub fn read_operator_stream_header(data: &[u8]) -> Option<OperatorStreamHeader> {
    let version = i32::from_be_bytes(data.get(..4)?.try_into().ok()?);
    if !(1..=OPERATOR_BACKEND_VERSION).contains(&version) {
        return None;
    }
    // Proxy version 6 maps to meta-info snapshot version 7, the first with the flag (:126-130).
    let using_state_compression = version >= 6 && *data.get(4)? != 0;
    Some(OperatorStreamHeader {
        version,
        using_state_compression,
    })
}

/// The elements of one operator-state stream, addressed by the offsets its handle records.
pub struct OperatorStreamElements<'a> {
    stream: &'a [u8],
    compressed: bool,
    /// Every offset of every state in the stream, plus its end, sorted and deduplicated. States
    /// share the stream, so an element ends where the next element of *any* state begins.
    boundaries: Vec<u64>,
}

impl<'a> OperatorStreamElements<'a> {
    /// Read the header of `stream` and index the offsets of all its `partitions`.
    pub fn new(stream: &'a [u8], partitions: &[PartitionOffsetEntry]) -> Result<Self, String> {
        let header = read_operator_stream_header(stream).ok_or_else(|| {
            format!(
                "not an operator-state stream: the {}-byte stream does not start with an \
                 OperatorBackendSerializationProxy version 1..={}",
                stream.len(),
                OPERATOR_BACKEND_VERSION
            )
        })?;
        let len = stream.len() as u64;
        let mut boundaries: Vec<u64> = partitions
            .iter()
            .flat_map(|p| p.offsets.iter())
            .filter_map(|&o| u64::try_from(o).ok())
            .filter(|&o| o < len)
            .collect();
        boundaries.push(len);
        boundaries.sort_unstable();
        boundaries.dedup();
        Ok(OperatorStreamElements {
            stream,
            compressed: header.using_state_compression,
            boundaries,
        })
    }

    pub fn compressed(&self) -> bool {
        self.compressed
    }

    /// The bytes of the element written at `offset`, inflated if the stream is compressed.
    pub fn element(&self, offset: i64) -> Result<Cow<'a, [u8]>, String> {
        let len = self.stream.len() as u64;
        let start = u64::try_from(offset)
            .ok()
            .filter(|&s| s <= len)
            .ok_or_else(|| format!("offset {} is outside the {}-byte stream", offset, len))?;
        // The first boundary past `start` (binary search: a stream can hold millions of
        // elements). `len` is the last boundary, so only `start == len` finds none: an empty
        // element at the very end.
        let end = self
            .boundaries
            .get(self.boundaries.partition_point(|&b| b <= start))
            .copied()
            .unwrap_or(len);
        let raw = &self.stream[start as usize..end as usize];
        if !self.compressed {
            return Ok(Cow::Borrowed(raw));
        }
        let mut out = Vec::new();
        inflate_snappy_chunks(raw, &mut out)
            .map_err(|e| format!("Snappy chunks at offset {}: {}", offset, e))?;
        Ok(Cow::Owned(out))
    }
}

/// Decode a run of Snappy framing-format chunks and append their data to `out`. No stream
/// identifier is needed in front: in an operator-state stream it is written once, before the
/// first element.
///
/// Mirrors `SnappyFramedInputStream` (snappy-java 1.1.10.4) as Flink opens it,
/// `new SnappyFramedInputStream(stream, false)` (`SnappyStreamCompressionDecorator.java:51-54`),
/// i.e. **without** checksum verification (`ensureBuffer`, :509-585). Chunk header = type byte +
/// 3-byte little-endian length (`getFrameMetaData`, :609-660): `0x00` = 4-byte CRC then
/// Snappy-compressed data, `0x01` = CRC then raw data (both at least 5 bytes), `0xff` = stream
/// identifier (exactly 6 bytes, skipped), `0x80..=0xfe` skipped, `0x02..=0x7f` an error.
///
/// Not `snap::read::FrameDecoder`: it zero-fills ~140 KiB of buffers when created, which one
/// decoder per element turns into minutes for a million elements.
pub fn inflate_snappy_chunks(mut raw: &[u8], out: &mut Vec<u8>) -> Result<(), String> {
    let mut decoder = snap::raw::Decoder::new();
    while !raw.is_empty() {
        let Some(header) = raw.get(..4) else {
            return Err(format!("truncated chunk header ({} bytes)", raw.len()));
        };
        let flag = header[0];
        let len = header[1] as usize | (header[2] as usize) << 8 | (header[3] as usize) << 16;
        let Some(body) = raw.get(4..4 + len) else {
            return Err(format!(
                "chunk 0x{:02x} of {} bytes truncated to {}",
                flag,
                len,
                raw.len() - 4
            ));
        };
        raw = &raw[4 + len..];
        match flag {
            0x00 | 0x01 => {
                if len < 5 {
                    return Err(format!("invalid length {} for chunk 0x{:02x}", len, flag));
                }
                let data = &body[4..]; // after the masked CRC-32C, which Flink doesn't check
                if flag == 0x01 {
                    out.extend_from_slice(data);
                } else {
                    let n = snap::raw::decompress_len(data).map_err(|e| e.to_string())?;
                    // The framing format caps a chunk at 65 536 uncompressed bytes; trusting a
                    // corrupt length would zero-fill up to 4 GiB before decompression fails.
                    if n > 1 << 16 {
                        return Err(format!(
                            "chunk inflates to {} bytes, over the 65536-byte limit",
                            n
                        ));
                    }
                    let at = out.len();
                    out.resize(at + n, 0);
                    decoder
                        .decompress(data, &mut out[at..])
                        .map_err(|e| e.to_string())?;
                }
            }
            0xff if len != 6 => {
                return Err(format!(
                    "stream identifier chunk with invalid length: {}",
                    len
                ))
            }
            0x02..=0x7f => return Err(format!("unsupported unskippable chunk: 0x{:02x}", flag)),
            _ => {} // stream identifier, padding, reserved skippable
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn partition(offsets: &[i64]) -> PartitionOffsetEntry {
        PartitionOffsetEntry {
            state_name: "s".to_string(),
            mode: 0,
            offsets: offsets.to_vec(),
        }
    }

    /// `[version][flag]` then `rest`.
    fn stream(version: i32, compressed: bool, rest: &[u8]) -> Vec<u8> {
        let mut s = version.to_be_bytes().to_vec();
        s.push(compressed as u8);
        s.extend_from_slice(rest);
        s
    }

    /// `data` as Snappy chunks, without the stream identifier `snap` puts in front.
    fn chunks(data: &[u8]) -> Vec<u8> {
        let mut enc = snap::write::FrameEncoder::new(Vec::new());
        enc.write_all(data).unwrap();
        let framed = enc.into_inner().unwrap();
        assert_eq!(
            &framed[..4],
            &[0xff, 6, 0, 0],
            "snap starts with the identifier"
        );
        framed[10..].to_vec()
    }

    #[test]
    fn header_flag_only_from_version_6() {
        let h = read_operator_stream_header(&stream(6, true, &[])).unwrap();
        assert!(h.using_state_compression);
        assert!(
            !read_operator_stream_header(&stream(6, false, &[]))
                .unwrap()
                .using_state_compression
        );
        // Version 5 has no flag: the byte after the version is already the state count.
        assert!(
            !read_operator_stream_header(&stream(5, true, &[]))
                .unwrap()
                .using_state_compression
        );
        assert!(read_operator_stream_header(&stream(7, false, &[])).is_none());
        assert!(read_operator_stream_header(&[0, 0, 0]).is_none());
    }

    #[test]
    fn element_runs_to_the_next_offset_of_any_state() {
        // Two states interleaved in offset order: an element must stop at the other state's
        // next element, not at its own state's next offset.
        let s = stream(6, false, b"aaBBccDD");
        let parts = [partition(&[5, 9]), partition(&[7, 11])];
        let e = OperatorStreamElements::new(&s, &parts).unwrap();
        assert_eq!(&*e.element(5).unwrap(), b"aa");
        assert_eq!(&*e.element(7).unwrap(), b"BB");
        assert_eq!(&*e.element(9).unwrap(), b"cc");
        assert_eq!(&*e.element(11).unwrap(), b"DD");
        assert_eq!(
            &*e.element(13).unwrap(),
            b"",
            "empty element at the very end"
        );
        assert!(e
            .element(14)
            .unwrap_err()
            .contains("outside the 13-byte stream"));
        assert!(e.element(-1).is_err());
    }

    #[test]
    fn compressed_elements_inflate_on_their_own() {
        let big: Vec<u8> = (0..150_000u32)
            .map(|i| b"abcdefgh"[i as usize % 8])
            .collect();
        let mut rest = vec![0xff, 6, 0, 0, b's', b'N', b'a', b'P', b'p', b'Y'];
        let first = 5 + rest.len() as i64;
        rest.extend(chunks(b"tiny"));
        let second = 5 + rest.len() as i64;
        rest.extend(chunks(&big));
        let s = stream(6, true, &rest);
        let e = OperatorStreamElements::new(&s, &[partition(&[first, second])]).unwrap();
        assert!(e.compressed());
        assert_eq!(&*e.element(first).unwrap(), b"tiny");
        assert_eq!(&*e.element(second).unwrap(), big.as_slice());
        // More than one 64 KiB block, and compressible: Snappy-compressed (0x00) chunks.
        assert!(chunks(&big).first() == Some(&0x00));
    }

    #[test]
    fn malformed_chunks_are_errors() {
        let mut out = Vec::new();
        assert!(inflate_snappy_chunks(&[0x01, 9, 0, 0, 1, 2], &mut out).is_err());
        assert!(inflate_snappy_chunks(&[0x02, 0, 0, 0], &mut out).is_err());
        assert!(inflate_snappy_chunks(&[0x01, 4, 0, 0, 1, 2, 3, 4], &mut out).is_err());
        assert!(inflate_snappy_chunks(&[0xff, 5, 0, 0, 1, 2, 3, 4, 5], &mut out).is_err());
        // A compressed chunk whose varint claims ~4 GiB: refused before any allocation.
        let huge = [0x00, 9, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0x0f];
        let e = inflate_snappy_chunks(&huge, &mut out).unwrap_err();
        assert!(e.contains("65536-byte limit"), "{}", e);
        // Padding and reserved skippable chunks are skipped.
        inflate_snappy_chunks(
            &[0xfe, 2, 0, 0, 9, 9, 0x01, 5, 0, 0, 0, 0, 0, 0, b'x'],
            &mut out,
        )
        .unwrap();
        assert_eq!(out, b"x");
    }
}
