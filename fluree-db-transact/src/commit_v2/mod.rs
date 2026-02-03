//! Commit format v2: binary commit writer.
//!
//! The codec (format, varint, string_dict, op_codec) and reader live in
//! `fluree_db_novelty::commit_v2`. This module provides only the writer,
//! which is used by the commit pipeline in `fluree-db-transact`.

mod streaming;
mod writer;

// Re-export core types from novelty for convenience
pub use fluree_db_novelty::commit_v2::CommitV2Envelope;
pub use fluree_db_novelty::commit_v2::CommitV2Error;
pub use fluree_db_novelty::commit_v2::MAGIC;
pub use streaming::StreamingCommitWriter;
pub use writer::{write_commit, CommitWriteResult};
