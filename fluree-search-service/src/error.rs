//! Service-level error types for the search service.
//!
//! These errors are internal to the service and are converted to
//! protocol `SearchError` types before being returned to clients.

use fluree_search_protocol::ErrorCode;
use std::time::Duration;
use thiserror::Error;

/// Internal service errors.
#[derive(Debug, Error)]
pub enum ServiceError {
    /// Virtual graph not found in nameservice.
    #[error("virtual graph not found: {alias}")]
    VgNotFound { alias: String },

    /// No snapshot available for the requested as_of_t.
    #[error("no snapshot available for as_of_t={as_of_t}")]
    NoSnapshotForAsOfT { as_of_t: i64 },

    /// Index has never been built for this virtual graph.
    #[error("index not built for virtual graph: {alias}")]
    IndexNotBuilt { alias: String },

    /// Sync timeout - index didn't reach expected head in time.
    #[error("sync timeout after {elapsed:?} waiting for t={target_t:?}")]
    SyncTimeout {
        target_t: Option<i64>,
        elapsed: Duration,
    },

    /// Request timeout.
    #[error("request timeout after {elapsed:?}")]
    Timeout { elapsed: Duration },

    /// Invalid request parameters.
    #[error("invalid request: {message}")]
    InvalidRequest { message: String },

    /// Unsupported protocol version.
    #[error("unsupported protocol version: {version}")]
    UnsupportedProtocolVersion { version: String },

    /// Storage error loading index.
    #[error("storage error: {message}")]
    StorageError { message: String },

    /// Nameservice error.
    #[error("nameservice error: {message}")]
    NameserviceError { message: String },

    /// Internal error.
    #[error("internal error: {message}")]
    Internal { message: String },
}

impl ServiceError {
    /// Convert to protocol error code.
    pub fn error_code(&self) -> ErrorCode {
        match self {
            ServiceError::VgNotFound { .. } => ErrorCode::VgNotFound,
            ServiceError::NoSnapshotForAsOfT { .. } => ErrorCode::NoSnapshotForAsOfT,
            ServiceError::IndexNotBuilt { .. } => ErrorCode::IndexNotBuilt,
            ServiceError::SyncTimeout { .. } => ErrorCode::SyncTimeout,
            ServiceError::Timeout { .. } => ErrorCode::Timeout,
            ServiceError::InvalidRequest { .. } => ErrorCode::InvalidRequest,
            ServiceError::UnsupportedProtocolVersion { .. } => {
                ErrorCode::UnsupportedProtocolVersion
            }
            ServiceError::StorageError { .. }
            | ServiceError::NameserviceError { .. }
            | ServiceError::Internal { .. } => ErrorCode::Internal,
        }
    }
}

/// Result type alias for service operations.
pub type Result<T> = std::result::Result<T, ServiceError>;
