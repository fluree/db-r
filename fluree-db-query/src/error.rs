//! Error types for query execution

use crate::binding::BatchError;
use thiserror::Error;

/// Query execution errors
#[derive(Error, Debug)]
pub enum QueryError {
    /// Error from fluree-db-core
    #[error("Core error: {0}")]
    Core(#[from] fluree_db_core::Error),

    /// Batch construction error
    #[error("Batch error: {0}")]
    Batch(#[from] BatchError),

    /// R2RML materialization error
    #[error("R2RML error: {0}")]
    R2rml(#[from] fluree_db_r2rml::R2rmlError),

    /// Operator not opened
    #[error("Operator not opened - call open() before next_batch()")]
    OperatorNotOpened,

    /// Operator already opened
    #[error("Operator already opened")]
    OperatorAlreadyOpened,

    /// Operator is closed
    #[error("Operator is closed")]
    OperatorClosed,

    /// Variable not found
    #[error("Variable not found: {0}")]
    VariableNotFound(String),

    /// Index selection failed
    #[error("No suitable index for query pattern")]
    NoSuitableIndex,

    /// Invalid query
    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    /// Invalid filter expression
    #[error("Invalid filter: {0}")]
    InvalidFilter(String),

    /// Resource limit exceeded
    #[error("Resource limit exceeded: {0}")]
    ResourceLimit(String),

    /// Fuel limit exceeded (Clojure parity)
    #[error(transparent)]
    FuelLimitExceeded(#[from] fluree_db_core::FuelExceededError),

    /// Internal error (should not happen in normal operation)
    #[error("Internal error: {0}")]
    Internal(String),

    /// Policy evaluation error
    #[error("Policy error: {0}")]
    Policy(String),
}

/// Result type for query operations
pub type Result<T> = std::result::Result<T, QueryError>;
