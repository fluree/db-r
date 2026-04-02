//! Binary commit codec.
//!
//! Encodes and decodes [`Commit`](super::Commit) values to/from a compact
//! binary format (v4). The envelope section supports multi-parent references
//! for merge commits.
//!
//! # Layout
//!
//! ```text
//! [Header 32B][Envelope (binary)][Ops section][Dictionaries][Footer 64B][optional signature block]
//! ```
//!
//! See [`format`] for constants and layout details.

pub mod envelope;
mod error;
pub mod format;
pub mod op_codec;
pub mod raw_reader;
mod reader;
pub mod string_dict;
pub mod varint;
#[cfg(feature = "credential")]
mod writer;

pub use envelope::CommitV2Envelope;
pub use error::CommitV2Error;
pub use format::{CommitSignature, ALGO_ED25519, MAGIC};
pub use raw_reader::{load_commit_ops, CommitOps, RawObject, RawOp};
pub use reader::{read_commit, read_commit_envelope, verify_commit_blob};
#[cfg(feature = "credential")]
pub use writer::{write_commit, CommitWriteResult};
