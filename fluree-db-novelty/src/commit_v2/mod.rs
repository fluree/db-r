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

mod error;
pub mod envelope;
pub mod format;
pub mod op_codec;
mod reader;
pub mod string_dict;
pub mod varint;

pub use envelope::CommitV2Envelope;
pub use error::CommitV2Error;
pub use format::MAGIC;
pub use reader::{read_commit, read_commit_envelope};
