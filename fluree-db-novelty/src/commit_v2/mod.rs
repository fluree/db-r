//! Commit format v2: binary commit reader and codec.
//!
//! The v2 format replaces the JSON serialization of `Commit` with a binary
//! layout: binary envelope (non-flake metadata) + binary-encoded ops
//! (flakes with commit-local Sid name dictionaries).
//!
//! # Layout
//!
//! ```text
//! [Header 32B][Envelope (binary)][Ops section][Dictionaries][Footer 64B][Hash 32B]
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

pub use envelope::CommitV2Envelope;
pub use error::CommitV2Error;
pub use format::MAGIC;
pub use raw_reader::{load_commit_ops, CommitOps, RawObject, RawOp};
pub use reader::{read_commit, read_commit_envelope};
