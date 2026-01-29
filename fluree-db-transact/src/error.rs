//! Transaction error types

use thiserror::Error;

/// Transaction errors
#[derive(Error, Debug)]
pub enum TransactError {
    /// Core database error
    #[error("Core error: {0}")]
    Core(#[from] fluree_db_core::Error),

    /// Query error
    #[error("Query error: {0}")]
    Query(#[from] fluree_db_query::QueryError),

    /// Novelty error
    #[error("Novelty error: {0}")]
    Novelty(#[from] fluree_db_novelty::NoveltyError),

    /// Ledger error
    #[error("Ledger error: {0}")]
    Ledger(#[from] fluree_db_ledger::LedgerError),

    /// Nameservice error
    #[error("Nameservice error: {0}")]
    Nameservice(#[from] fluree_db_nameservice::NameServiceError),

    /// JSON serialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// JSON-LD processing error
    #[error("JSON-LD error: {0}")]
    JsonLd(#[from] fluree_graph_json_ld::JsonLdError),

    /// Transaction parsing error
    #[error("Parse error: {0}")]
    Parse(String),

    /// Flake generation error
    #[error("Flake generation error: {0}")]
    FlakeGeneration(String),

    /// Commit conflict (concurrent modification)
    #[error("Commit conflict: expected t={expected_t}, head_t={head_t}")]
    CommitConflict { expected_t: i64, head_t: i64 },

    /// Address mismatch during commit
    #[error("Commit address mismatch: expected {expected}, found {found}")]
    AddressMismatch { expected: String, found: String },

    /// Empty transaction (no flakes to commit)
    #[error("Empty transaction: no flakes to commit")]
    EmptyTransaction,

    /// Novelty at maximum size (backpressure)
    #[error("Novelty at maximum size, reindexing required")]
    NoveltyAtMax,

    /// Transaction would exceed maximum novelty size
    #[error("Transaction would exceed novelty limit: current={current_bytes}, delta={delta_bytes}, max={max_bytes}")]
    NoveltyWouldExceed {
        current_bytes: usize,
        delta_bytes: usize,
        max_bytes: usize,
    },

    /// Invalid template term
    #[error("Invalid template term: {0}")]
    InvalidTerm(String),

    /// Unbound variable in template
    #[error("Unbound variable in template: {0}")]
    UnboundVariable(String),

    /// Policy violation
    #[error("{0}")]
    PolicyViolation(#[from] fluree_db_policy::PolicyError),

    /// SHACL validation error (only available with `shacl` feature)
    #[cfg(feature = "shacl")]
    #[error("SHACL error: {0}")]
    Shacl(#[from] fluree_db_shacl::ShaclError),

    /// SHACL validation violation (only available with `shacl` feature)
    #[cfg(feature = "shacl")]
    #[error("{0}")]
    ShaclViolation(String),
}

/// Result type for transaction operations
pub type Result<T> = std::result::Result<T, TransactError>;
