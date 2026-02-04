//! Proxy storage implementation for peer mode
//!
//! Fetches index data via the transaction server's `/fluree/storage/block` endpoint
//! instead of direct storage access. This allows peers to operate without storage
//! credentials (no S3 access, no filesystem mount).
//!
//! ## Content Negotiation
//!
//! ProxyStorage supports content negotiation for leaf nodes via `read_bytes_hint()`:
//! - `ReadHint::AnyBytes` - Always requests `application/octet-stream` (raw bytes)
//! - `ReadHint::PreferLeafFlakes` - First tries `application/x-fluree-flakes`, falls back
//!   to octet-stream on 406 Not Acceptable
//!
//! This allows policy-filtered flakes to be returned for leaf nodes while avoiding
//! double HTTP requests for branch nodes.

use async_trait::async_trait;
use fluree_db_core::error::{Error as CoreError, Result};
use fluree_db_core::storage::ReadHint;
use fluree_db_core::{
    ContentAddressedWrite, ContentKind, ContentWriteResult, StorageRead, StorageWrite,
};
use reqwest::{Client, StatusCode};
use serde::Serialize;
use std::fmt::Debug;
use std::time::Duration;

/// Storage implementation that proxies reads through the transaction server
#[derive(Clone)]
pub struct ProxyStorage {
    client: Client,
    base_url: String,
    token: String,
}

impl Debug for ProxyStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyStorage")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

/// Request body for block fetch endpoint
#[derive(Debug, Serialize)]
struct BlockRequest<'a> {
    address: &'a str,
}

/// Internal result type for fetch operations
///
/// This avoids using CoreError for the 406 case, which is an internal
/// retry condition rather than a user-facing error.
enum FetchOutcome {
    /// Successfully fetched bytes
    Success(Vec<u8>),
    /// Server returned 406 Not Acceptable (format not available)
    NotAcceptable,
    /// Fetch failed with an error
    Error(CoreError),
}

impl ProxyStorage {
    /// Create a new proxy storage client
    ///
    /// # Arguments
    ///
    /// * `base_url` - Base URL of the transaction server (e.g., `https://tx.fluree.internal:8090`)
    /// * `token` - Bearer token for authentication (with `fluree.storage.*` claims)
    pub fn new(base_url: String, token: String) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(60)) // 1 minute for block reads
            .build()
            .expect("Failed to create proxy storage client");

        // Normalize base_url by trimming trailing slashes
        let base_url = base_url.trim_end_matches('/').to_string();

        Self {
            client,
            base_url,
            token,
        }
    }

    /// Build the storage block endpoint URL
    fn block_url(&self) -> String {
        format!("{}/fluree/storage/block", self.base_url)
    }

    /// Fetch with flakes-first content negotiation
    ///
    /// Tries `application/x-fluree-flakes` first. If the server returns 406
    /// (format not available for this block type), falls back to raw bytes.
    async fn fetch_prefer_flakes(&self, address: &str) -> Result<Vec<u8>> {
        // Try flakes format first
        match self
            .fetch_with_accept(address, "application/x-fluree-flakes")
            .await
        {
            FetchOutcome::Success(bytes) => Ok(bytes),
            FetchOutcome::NotAcceptable => {
                // 406 = not a leaf or policy filtering not applicable
                // Fall back to raw bytes
                tracing::debug!(
                    address = %address,
                    "Flakes format not available, falling back to raw bytes"
                );
                match self
                    .fetch_with_accept(address, "application/octet-stream")
                    .await
                {
                    FetchOutcome::Success(bytes) => Ok(bytes),
                    FetchOutcome::NotAcceptable => {
                        // Should not happen for octet-stream, but handle anyway
                        Err(CoreError::storage(format!(
                            "Storage proxy: octet-stream also rejected for {}",
                            address
                        )))
                    }
                    FetchOutcome::Error(e) => Err(e),
                }
            }
            FetchOutcome::Error(e) => Err(e),
        }
    }

    /// Internal fetch with a specific Accept header
    ///
    /// Returns `FetchOutcome` to distinguish between success, 406, and errors
    /// without using string-based error detection.
    async fn fetch_with_accept(&self, address: &str, accept: &str) -> FetchOutcome {
        let url = self.block_url();
        let body = BlockRequest { address };

        let response = match self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Accept", accept)
            .json(&body)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                let err = if e.is_timeout() {
                    CoreError::io(format!("Storage proxy timeout for {}: {}", address, e))
                } else if e.is_connect() {
                    CoreError::io(format!(
                        "Storage proxy connection failed for {}: {}",
                        address, e
                    ))
                } else {
                    CoreError::io(format!(
                        "Storage proxy request failed for {}: {}",
                        address, e
                    ))
                };
                return FetchOutcome::Error(err);
            }
        };

        let status = response.status();

        match status {
            StatusCode::OK => match response.bytes().await {
                Ok(bytes) => FetchOutcome::Success(bytes.to_vec()),
                Err(e) => FetchOutcome::Error(CoreError::io(format!(
                    "Failed to read response body for {}: {}",
                    address, e
                ))),
            },
            StatusCode::NOT_FOUND => FetchOutcome::Error(CoreError::not_found(address)),
            StatusCode::NOT_ACCEPTABLE => {
                // 406 - format not available, signal for retry with different Accept
                FetchOutcome::NotAcceptable
            }
            StatusCode::UNAUTHORIZED => FetchOutcome::Error(CoreError::storage(format!(
                "Storage proxy authentication failed for {}: check token validity",
                address
            ))),
            StatusCode::FORBIDDEN => {
                // Address not in token scope - treat as not found (no existence leak)
                FetchOutcome::Error(CoreError::not_found(address))
            }
            s if s.is_server_error() => FetchOutcome::Error(CoreError::io(format!(
                "Storage proxy server error for {}: {}",
                address, status
            ))),
            _ => FetchOutcome::Error(CoreError::storage(format!(
                "Storage proxy unexpected status {} for {}",
                status, address
            ))),
        }
    }
}

#[async_trait]
impl StorageRead for ProxyStorage {
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>> {
        // Always request raw bytes (no content negotiation)
        match self
            .fetch_with_accept(address, "application/octet-stream")
            .await
        {
            FetchOutcome::Success(bytes) => Ok(bytes),
            FetchOutcome::NotAcceptable => {
                // Should not happen for octet-stream
                Err(CoreError::storage(format!(
                    "Storage proxy: octet-stream rejected for {}",
                    address
                )))
            }
            FetchOutcome::Error(e) => Err(e),
        }
    }

    async fn read_bytes_hint(&self, address: &str, hint: ReadHint) -> Result<Vec<u8>> {
        match hint {
            ReadHint::AnyBytes => self.read_bytes(address).await,
            ReadHint::PreferLeafFlakes => self.fetch_prefer_flakes(address).await,
            // Future ReadHint variants fall back to raw bytes
            _ => self.read_bytes(address).await,
        }
    }

    async fn exists(&self, address: &str) -> Result<bool> {
        // In v1, implement exists as try-read
        // This is correct but slightly less efficient than a HEAD request
        // The server currently only supports POST for blocks anyway
        match self.read_bytes(address).await {
            Ok(_) => Ok(true),
            Err(CoreError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn list_prefix(&self, _prefix: &str) -> Result<Vec<String>> {
        // ProxyStorage is read-only and doesn't support listing
        Err(CoreError::storage(
            "ProxyStorage does not support list_prefix".to_string(),
        ))
    }
}

#[async_trait]
impl StorageWrite for ProxyStorage {
    async fn write_bytes(&self, _address: &str, _bytes: &[u8]) -> Result<()> {
        Err(CoreError::storage(
            "ProxyStorage is read-only (writes must go to the transaction server)".to_string(),
        ))
    }

    async fn delete(&self, _address: &str) -> Result<()> {
        Err(CoreError::storage(
            "ProxyStorage is read-only (deletes must go to the transaction server)".to_string(),
        ))
    }
}

#[async_trait]
impl ContentAddressedWrite for ProxyStorage {
    async fn content_write_bytes_with_hash(
        &self,
        _kind: ContentKind,
        _ledger_alias: &str,
        _content_hash_hex: &str,
        _bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        Err(CoreError::storage(
            "ProxyStorage is read-only (writes must go to the transaction server)".to_string(),
        ))
    }

    async fn content_write_bytes(
        &self,
        _kind: ContentKind,
        _ledger_alias: &str,
        _bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        Err(CoreError::storage(
            "ProxyStorage is read-only (writes must go to the transaction server)".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proxy_storage_debug() {
        let storage = ProxyStorage::new(
            "http://localhost:8090".to_string(),
            "test-token".to_string(),
        );
        let debug = format!("{:?}", storage);
        assert!(debug.contains("ProxyStorage"));
        assert!(debug.contains("localhost:8090"));
        // Token should NOT be in debug output
        assert!(!debug.contains("test-token"));
    }

    #[test]
    fn test_block_url() {
        let storage = ProxyStorage::new(
            "http://localhost:8090".to_string(),
            "test-token".to_string(),
        );
        assert_eq!(
            storage.block_url(),
            "http://localhost:8090/fluree/storage/block"
        );
    }

    #[test]
    fn test_block_url_with_trailing_slash() {
        let storage = ProxyStorage::new(
            "http://localhost:8090/".to_string(),
            "test-token".to_string(),
        );
        // Should work but might have double slash - that's okay for URLs
        assert!(storage.block_url().contains("/fluree/storage/block"));
    }
}
