//! # Fluree DB Transact
//!
//! Transaction support for Fluree DB, including staging and committing changes.
//!
//! This crate provides:
//! - Transaction parsing (JSON-LD → IR)
//! - Flake generation from templates
//! - Staging (creates a `LedgerView` with uncommitted changes)
//! - Commit (persists to storage and publishes to nameservice)
//!
//! ## Transaction Types
//!
//! - **Insert**: Add new triples (fails if subject already exists)
//! - **Upsert**: Insert or update (deletes existing values for provided predicates)
//! - **Update**: SPARQL-style conditional update with WHERE/DELETE/INSERT
//!
//! ## Example
//!
//! ```ignore
//! use fluree_db_transact::{stage_update, commit};
//! use serde_json::json;
//!
//! // Stage an update
//! let view = stage_update(ledger, &json!({
//!     "where": { "@id": "?s", "ex:name": "?name" },
//!     "delete": { "@id": "?s", "ex:name": "?name" },
//!     "insert": { "@id": "?s", "ex:name": "New Name" }
//! }), opts).await?;
//!
//! // Commit the changes
//! let (receipt, new_state) = commit(view, &storage, &nameservice, commit_opts).await?;
//! ```

pub mod address;
pub mod commit;
pub mod commit_flakes;
pub mod error;
pub mod generate;
pub mod ir;
pub mod namespace;
pub mod parse;
pub mod stage;

// Re-exports
pub use address::parse_commit_id;
pub use commit::{commit, CommitOpts, CommitReceipt};
pub use commit_flakes::generate_commit_flakes;
pub use error::{Result, TransactError};
pub use generate::{apply_cancellation, FlakeGenerator};
pub use ir::{InlineValues, TemplateTerm, TripleTemplate, Txn, TxnOpts, TxnType};
pub use namespace::{NamespaceRegistry, BLANK_NODE_PREFIX};
pub use parse::parse_transaction;
pub use stage::{stage, StageOptions};

#[cfg(feature = "shacl")]
pub use stage::stage_with_shacl;
