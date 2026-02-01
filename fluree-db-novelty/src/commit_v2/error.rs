//! Error types for commit format v2.

use std::fmt;

#[derive(Debug)]
pub enum CommitV2Error {
    /// First 4 bytes are not b"FCV2".
    InvalidMagic,
    /// Version byte is not supported.
    UnsupportedVersion(u8),
    /// Blob is smaller than the minimum valid size.
    TooSmall { got: usize, min: usize },
    /// SHA-256 of blob content does not match the trailing hash.
    HashMismatch {
        expected: [u8; 32],
        actual: [u8; 32],
    },
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
    /// FlakeValue variant not supported in commit-v2 format.
    UnsupportedValue(String),
    /// Non-default graph encountered; Phase 1 only supports default graph.
    NonDefaultGraph { ns_code: i32, name_id: u32 },
}

impl fmt::Display for CommitV2Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => write!(f, "commit-v2: invalid magic bytes (expected FCV2)"),
            Self::UnsupportedVersion(v) => {
                write!(f, "commit-v2: unsupported version {}", v)
            }
            Self::TooSmall { got, min } => {
                write!(f, "commit-v2: blob too small ({} bytes, need >= {})", got, min)
            }
            Self::HashMismatch { expected, actual } => {
                write!(
                    f,
                    "commit-v2: hash mismatch (expected {:?}, got {:?})",
                    &expected[..4],
                    &actual[..4]
                )
            }
            Self::InvalidDictionary(msg) => write!(f, "commit-v2: invalid dictionary: {}", msg),
            Self::InvalidOp(msg) => write!(f, "commit-v2: invalid op: {}", msg),
            Self::InvalidOpTag(tag) => write!(f, "commit-v2: invalid op tag: {}", tag),
            Self::DecompressionFailed(e) => {
                write!(f, "commit-v2: zstd decompression failed: {}", e)
            }
            Self::CompressionFailed(e) => {
                write!(f, "commit-v2: zstd compression failed: {}", e)
            }
            Self::EnvelopeDecode(msg) => {
                write!(f, "commit-v2: envelope decode failed: {}", msg)
            }
            Self::EnvelopeEncode(msg) => {
                write!(f, "commit-v2: envelope encode failed: {}", msg)
            }
            Self::UnexpectedEof => write!(f, "commit-v2: unexpected end of data"),
            Self::UnsupportedValue(desc) => {
                write!(f, "commit-v2: unsupported FlakeValue variant: {}", desc)
            }
            Self::NonDefaultGraph { ns_code, name_id } => {
                write!(
                    f,
                    "commit-v2: non-default graph (ns_code={}, name_id={}); Phase 1 only supports default graph",
                    ns_code, name_id
                )
            }
        }
    }
}

impl std::error::Error for CommitV2Error {}
