//! Error types for the ledger crate

use thiserror::Error;

/// Result type for ledger operations
pub type Result<T> = std::result::Result<T, LedgerError>;

/// Errors that can occur in ledger operations
#[derive(Error, Debug)]
pub enum LedgerError {
    /// Ledger not found in nameservice
    #[error("Ledger not found: {0}")]
    NotFound(String),

    /// Core error wrapper
    #[error("Core error: {0}")]
    Core(#[from] fluree_db_core::Error),

    /// Novelty error wrapper
    #[error("Novelty error: {0}")]
    Novelty(#[from] fluree_db_novelty::NoveltyError),

    /// Nameservice error wrapper
    #[error("Nameservice error: {0}")]
    Nameservice(#[from] fluree_db_nameservice::NameServiceError),

    /// Backpressure - novelty at max capacity
    #[error("Novelty at max capacity, waiting for indexer")]
    MaxNovelty,

    /// Alias mismatch when applying index
    #[error("Index alias '{new}' does not match ledger alias '{expected}'")]
    AliasMismatch { new: String, expected: String },

    /// Stale index (older than current)
    #[error("Index at t={index_t} is older than current index at t={current_t}")]
    StaleIndex { index_t: i64, current_t: i64 },

    /// Missing index address in nameservice record
    #[error("Nameservice has index_t={index_t} for '{alias}' but no index_address")]
    MissingIndexAddress { alias: String, index_t: i64 },

    /// No index exists at or before the requested time
    #[error("No index available at or before t={target_t} for '{alias}' (earliest index at t={earliest_t})")]
    NoIndexAtTime {
        alias: String,
        target_t: i64,
        earliest_t: i64,
    },

    /// Target time is in the future (beyond current head)
    #[error("Target t={target_t} is beyond current head t={head_t} for '{alias}'")]
    FutureTime {
        alias: String,
        target_t: i64,
        head_t: i64,
    },
}

impl LedgerError {
    /// Create a not found error
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::NotFound(msg.into())
    }

    /// Create an alias mismatch error
    pub fn alias_mismatch(new: impl Into<String>, expected: impl Into<String>) -> Self {
        Self::AliasMismatch {
            new: new.into(),
            expected: expected.into(),
        }
    }

    /// Create a stale index error
    pub fn stale_index(index_t: i64, current_t: i64) -> Self {
        Self::StaleIndex { index_t, current_t }
    }

    /// Create a missing index address error
    pub fn missing_index_address(alias: impl Into<String>, index_t: i64) -> Self {
        Self::MissingIndexAddress {
            alias: alias.into(),
            index_t,
        }
    }

    /// Create a no index at time error
    pub fn no_index_at_time(
        alias: impl Into<String>,
        target_t: i64,
        earliest_t: i64,
    ) -> Self {
        Self::NoIndexAtTime {
            alias: alias.into(),
            target_t,
            earliest_t,
        }
    }

    /// Create a future time error
    pub fn future_time(alias: impl Into<String>, target_t: i64, head_t: i64) -> Self {
        Self::FutureTime {
            alias: alias.into(),
            target_t,
            head_t,
        }
    }
}
