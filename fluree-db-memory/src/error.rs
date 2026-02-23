use thiserror::Error;

pub type Result<T> = std::result::Result<T, MemoryError>;

#[derive(Error, Debug)]
pub enum MemoryError {
    /// Memory store has not been initialized (no __memory ledger).
    #[error("memory store not initialized — run `fluree memory init` first")]
    NotInitialized,

    /// Memory with the given ID was not found.
    #[error("memory not found: {0}")]
    NotFound(String),

    /// Secret detected in content (API keys, passwords, etc.).
    #[error("secret detected in content: {0}")]
    SecretDetected(String),

    /// Error from the Fluree API layer.
    #[error("API error: {0}")]
    Api(#[from] fluree_db_api::ApiError),

    /// JSON serialization/deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Generic error.
    #[error("{0}")]
    Other(String),
}
