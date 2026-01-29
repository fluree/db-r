//! Error types for the nameservice crate

use thiserror::Error;

/// Result type for nameservice operations
pub type Result<T> = std::result::Result<T, NameServiceError>;

/// Errors that can occur in nameservice operations
#[derive(Error, Debug)]
pub enum NameServiceError {
    /// Ledger not found
    #[error("Ledger not found: {0}")]
    NotFound(String),

    /// Invalid alias format
    #[error("Invalid alias format: {0}")]
    InvalidAlias(String),

    /// Storage/IO error
    #[error("Storage error: {0}")]
    Storage(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// The ledger has been retracted
    #[error("Ledger has been retracted: {0}")]
    Retracted(String),

    /// Ledger already exists (cannot create)
    #[error("Ledger already exists: {0}")]
    LedgerAlreadyExists(String),
}

impl NameServiceError {
    /// Create a not found error
    pub fn not_found(alias: impl Into<String>) -> Self {
        Self::NotFound(alias.into())
    }

    /// Create an invalid alias error
    pub fn invalid_alias(msg: impl Into<String>) -> Self {
        Self::InvalidAlias(msg.into())
    }

    /// Create a storage error
    pub fn storage(msg: impl Into<String>) -> Self {
        Self::Storage(msg.into())
    }

    /// Create a ledger already exists error
    pub fn ledger_already_exists(alias: impl Into<String>) -> Self {
        Self::LedgerAlreadyExists(alias.into())
    }
}
