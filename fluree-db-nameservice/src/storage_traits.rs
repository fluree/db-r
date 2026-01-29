//! Extended storage traits for advanced operations
//!
//! These traits extend the base `Storage` and `StorageWrite` traits from `fluree-db-core`
//! with capabilities needed for storage-backed nameservice implementations:
//!
//! - [`StorageDelete`]: Delete stored objects
//! - [`StorageList`]: List objects by prefix
//! - [`StorageCas`]: Compare-and-swap operations using ETags
//!
//! These traits are implemented by cloud storage backends like S3 but are intentionally
//! not required by the core storage traits to keep the base interface minimal.

use async_trait::async_trait;
use std::fmt::Debug;
use thiserror::Error;

/// Error type for extended storage operations
///
/// These errors have specific semantics important for nameservice operations:
/// - `PreconditionFailed` indicates CAS conflict (retry is appropriate)
/// - `Throttled` indicates rate limiting (back off and retry)
/// - Others are generally fatal for the operation
#[derive(Debug, Error)]
pub enum StorageExtError {
    /// I/O or network error
    #[error("I/O error: {0}")]
    Io(String),

    /// Resource not found
    #[error("Not found: {0}")]
    NotFound(String),

    /// Precondition failed (CAS conflict - 412)
    ///
    /// This is expected under contention and should trigger retry logic,
    /// not be treated as a fatal error.
    #[error("Precondition failed (CAS conflict)")]
    PreconditionFailed,

    /// Unauthorized - invalid credentials
    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    /// Forbidden - insufficient permissions
    #[error("Forbidden: {0}")]
    Forbidden(String),

    /// Throttled - rate limited
    ///
    /// Indicates the caller should back off and retry.
    #[error("Throttled: {0}")]
    Throttled(String),

    /// Other error
    #[error("{0}")]
    Other(String),
}

impl StorageExtError {
    pub fn io(msg: impl Into<String>) -> Self {
        Self::Io(msg.into())
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::NotFound(msg.into())
    }

    pub fn unauthorized(msg: impl Into<String>) -> Self {
        Self::Unauthorized(msg.into())
    }

    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self::Forbidden(msg.into())
    }

    pub fn throttled(msg: impl Into<String>) -> Self {
        Self::Throttled(msg.into())
    }

    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
}

/// Result type for extended storage operations
pub type StorageExtResult<T> = std::result::Result<T, StorageExtError>;

/// Result of a paginated list operation
#[derive(Debug, Clone)]
pub struct ListResult {
    /// Object keys/addresses relative to storage root
    ///
    /// These are storage keys, not full Fluree addresses.
    /// The caller is responsible for building Fluree addresses from context.
    pub keys: Vec<String>,

    /// Continuation token for fetching the next page
    ///
    /// `None` if there are no more results.
    pub continuation_token: Option<String>,

    /// Whether there are more results available
    pub is_truncated: bool,
}

impl ListResult {
    /// Create a new list result
    pub fn new(keys: Vec<String>, continuation_token: Option<String>, is_truncated: bool) -> Self {
        Self {
            keys,
            continuation_token,
            is_truncated,
        }
    }

    /// Create an empty list result
    pub fn empty() -> Self {
        Self {
            keys: Vec::new(),
            continuation_token: None,
            is_truncated: false,
        }
    }
}

/// Delete stored objects
///
/// This trait is separate from `StorageWrite` because:
/// 1. Not all storage backends support deletion
/// 2. Deletion is rarely needed in normal operation (append-only data model)
/// 3. Deletion has security implications that warrant separate consideration
#[async_trait]
pub trait StorageDelete: Debug + Send + Sync {
    /// Delete an object by address
    ///
    /// Returns `Ok(())` if the object was deleted or did not exist.
    /// Only returns an error for actual failures (network, permissions, etc).
    async fn delete(&self, address: &str) -> StorageExtResult<()>;
}

/// List objects by prefix
///
/// This trait provides listing capabilities for storage backends.
///
/// # Warning
///
/// `list_prefix` can be expensive for large prefixes. Use only for:
/// - Development/debugging
/// - Admin operations
/// - Small, bounded prefixes
///
/// For production iteration over large datasets, use `list_prefix_paginated`.
#[async_trait]
pub trait StorageList: Debug + Send + Sync {
    /// List all objects under a prefix
    ///
    /// Returns all matching keys. May be expensive for large prefixes.
    ///
    /// # Warning
    ///
    /// This method loads all results into memory. For large prefixes,
    /// use `list_prefix_paginated` instead.
    async fn list_prefix(&self, prefix: &str) -> StorageExtResult<Vec<String>>;

    /// Paginated listing for production use
    ///
    /// Returns up to `max_keys` addresses and a continuation token.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix to list under
    /// * `continuation_token` - Token from a previous call to continue listing
    /// * `max_keys` - Maximum number of keys to return (capped by backend limits)
    async fn list_prefix_paginated(
        &self,
        prefix: &str,
        continuation_token: Option<String>,
        max_keys: usize,
    ) -> StorageExtResult<ListResult>;
}

/// Compare-and-swap operations using ETags
///
/// This trait provides atomic conditional writes using HTTP ETags.
/// It enables building concurrent-safe data structures on eventually-consistent storage.
///
/// # ETag Semantics
///
/// ETags are opaque strings that change whenever the object content changes.
/// They are typically MD5 hashes for single-part uploads, but the exact format
/// is implementation-dependent.
///
/// ETags should be normalized (quotes stripped) before comparison.
#[async_trait]
pub trait StorageCas: Debug + Send + Sync {
    /// Write only if the object does not exist
    ///
    /// Uses `If-None-Match: *` semantics.
    ///
    /// # Returns
    ///
    /// - `Ok(true)` if the write succeeded (object was created)
    /// - `Ok(false)` if the object already existed
    /// - `Err(StorageExtError::PreconditionFailed)` on conflict (equivalent to Ok(false))
    async fn write_if_absent(&self, address: &str, bytes: &[u8]) -> StorageExtResult<bool>;

    /// Write only if the object's ETag matches
    ///
    /// Uses `If-Match: <etag>` semantics for atomic update.
    ///
    /// # Returns
    ///
    /// - `Ok(new_etag)` if the write succeeded
    /// - `Err(StorageExtError::PreconditionFailed)` if the ETag didn't match
    /// - `Err(StorageExtError::NotFound)` if the object doesn't exist
    async fn write_if_match(
        &self,
        address: &str,
        bytes: &[u8],
        expected_etag: &str,
    ) -> StorageExtResult<String>;

    /// Read an object with its ETag
    ///
    /// Used to get the current state for subsequent `write_if_match` operations.
    ///
    /// # Returns
    ///
    /// - `Ok((bytes, etag))` with the object content and its ETag
    /// - `Err(StorageExtError::NotFound)` if the object doesn't exist
    async fn read_with_etag(&self, address: &str) -> StorageExtResult<(Vec<u8>, String)>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_result_empty() {
        let result = ListResult::empty();
        assert!(result.keys.is_empty());
        assert!(result.continuation_token.is_none());
        assert!(!result.is_truncated);
    }

    #[test]
    fn test_list_result_new() {
        let result = ListResult::new(
            vec!["key1".to_string(), "key2".to_string()],
            Some("token".to_string()),
            true,
        );
        assert_eq!(result.keys.len(), 2);
        assert_eq!(result.continuation_token, Some("token".to_string()));
        assert!(result.is_truncated);
    }

    #[test]
    fn test_storage_ext_error_constructors() {
        assert!(matches!(StorageExtError::io("test"), StorageExtError::Io(_)));
        assert!(matches!(
            StorageExtError::not_found("test"),
            StorageExtError::NotFound(_)
        ));
        assert!(matches!(
            StorageExtError::unauthorized("test"),
            StorageExtError::Unauthorized(_)
        ));
        assert!(matches!(
            StorageExtError::forbidden("test"),
            StorageExtError::Forbidden(_)
        ));
        assert!(matches!(
            StorageExtError::throttled("test"),
            StorageExtError::Throttled(_)
        ));
        assert!(matches!(
            StorageExtError::other("test"),
            StorageExtError::Other(_)
        ));
    }
}
