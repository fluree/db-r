//! Error types for commit format v2.

use std::fmt;

#[derive(Debug)]
pub enum CommitCodecError {
    /// First 4 bytes are not b"FCV2".
    InvalidMagic,
    /// Version byte is not supported.
    UnsupportedVersion(u8),
    /// Blob is smaller than the minimum valid size.
    TooSmall { got: usize, min: usize },
    /// Dictionary data is malformed.
    InvalidDictionary(String),
    /// Op data is malformed.
    InvalidOp(String),
    /// Unknown o_tag value.
    InvalidOpTag(u8),
    /// Zstd decompression failed (reader).
    DecompressionFailed(std::io::Error),
    /// Zstd compression failed (writer).
    CompressionFailed(std::io::Error),
    /// Envelope decoding failed (reader).
    EnvelopeDecode(String),
    /// Envelope encoding failed (writer).
    EnvelopeEncode(String),
    /// Unexpected end of data while reading.
    UnexpectedEof,
    /// FlakeValue variant not supported in commit codec format.
    UnsupportedValue(String),
    /// Non-default graph encountered; Phase 1 only supports default graph.
    NonDefaultGraph { ns_code: u16, name_id: u32 },
    /// Size limit exceeded for envelope data.
    LimitExceeded(String),
    /// Transaction number out of range for u32 encoding.
    TOutOfRange(i64),
    /// Graph ID out of range for u16 encoding.
    GIdOutOfRange(u64),
    /// Negative list index (not supported).
    NegativeListIndex(i32),
}

impl fmt::Display for CommitCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => write!(f, "commit-codec: invalid magic bytes (expected FCV2)"),
            Self::UnsupportedVersion(v) => {
                write!(f, "commit-codec: unsupported version {}", v)
            }
            Self::TooSmall { got, min } => {
                write!(
                    f,
                    "commit-codec: blob too small ({} bytes, need >= {})",
                    got, min
                )
            }
            Self::InvalidDictionary(msg) => write!(f, "commit-codec: invalid dictionary: {}", msg),
            Self::InvalidOp(msg) => write!(f, "commit-codec: invalid op: {}", msg),
            Self::InvalidOpTag(tag) => write!(f, "commit-codec: invalid op tag: {}", tag),
            Self::DecompressionFailed(e) => {
                write!(f, "commit-codec: zstd decompression failed: {}", e)
            }
            Self::CompressionFailed(e) => {
                write!(f, "commit-codec: zstd compression failed: {}", e)
            }
            Self::EnvelopeDecode(msg) => {
                write!(f, "commit-codec: envelope decode failed: {}", msg)
            }
            Self::EnvelopeEncode(msg) => {
                write!(f, "commit-codec: envelope encode failed: {}", msg)
            }
            Self::UnexpectedEof => write!(f, "commit-codec: unexpected end of data"),
            Self::UnsupportedValue(desc) => {
                write!(f, "commit-codec: unsupported FlakeValue variant: {}", desc)
            }
            Self::NonDefaultGraph { ns_code, name_id } => {
                write!(
                    f,
                    "commit-codec: non-default graph (ns_code={}, name_id={}); Phase 1 only supports default graph",
                    ns_code, name_id
                )
            }
            Self::LimitExceeded(msg) => {
                write!(f, "commit-codec: limit exceeded: {}", msg)
            }
            Self::TOutOfRange(t) => {
                write!(f, "commit-codec: t value {} out of u32 range", t)
            }
            Self::GIdOutOfRange(g) => {
                write!(f, "commit-codec: graph_delta key {} exceeds u16::MAX", g)
            }
            Self::NegativeListIndex(i) => {
                write!(f, "commit-codec: negative list index {}", i)
            }
        }
    }
}

impl std::error::Error for CommitCodecError {}
