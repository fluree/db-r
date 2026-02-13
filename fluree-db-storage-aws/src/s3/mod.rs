//! S3 storage backend implementation
//!
//! Provides `S3Storage` which implements the core `Storage` and `StorageWrite` traits
//! for reading and writing data to Amazon S3.
//!
//! Also implements the extended storage traits from `fluree-db-nameservice`:
//! - `StorageDelete`
//! - `StorageList`
//! - `StorageCas`
//!
//! ## S3 Express One Zone Support
//!
//! S3 Express One Zone (directory buckets) is expected to be supported natively by
//! the AWS SDK (v1.x), which should automatically handle session-based authentication
//! for Express buckets. Use an Express bucket name (format: `bucket-name--zone-id--x-s3`).
//!
//! **Important**: This native Express support should be validated with real AWS
//! integration tests before production use. LocalStack does not fully emulate
//! Express session authentication.
//!
//! ## Timeout Configuration
//!
//! The `timeout_ms` setting controls the total operation timeout, which **includes
//! SDK retry time**. For Lambda environments, ensure this value accounts for your
//! function's remaining execution time.

pub mod address;

use crate::error::{AwsStorageError, Result};
use address::{address_to_key, key_to_address, normalize_etag};
use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use aws_smithy_types::retry::RetryConfig;
use aws_smithy_types::timeout::TimeoutConfig;
use fluree_db_core::error::Error as CoreError;
use fluree_db_core::{
    content_address, sha256_hex, ContentAddressedWrite, ContentKind, ContentWriteResult,
    StorageRead, StorageWrite,
};
use fluree_db_nameservice::{
    ListResult as NsListResult, StorageCas, StorageDelete, StorageExtError, StorageExtResult,
    StorageList,
};
use std::fmt::Debug;
use std::time::Duration;

/// S3 storage configuration
#[derive(Debug, Clone, Default)]
pub struct S3Config {
    /// S3 bucket name (supports both standard S3 and S3 Express directory buckets)
    pub bucket: String,
    /// Optional key prefix
    pub prefix: Option<String>,
    /// Optional endpoint override (e.g. LocalStack/MinIO, or custom AWS endpoint)
    pub endpoint: Option<String>,
    /// Operation timeout in milliseconds (optional)
    pub timeout_ms: Option<u64>,
    /// Max retries (Clojure parity: retries *after* the initial attempt)
    pub max_retries: Option<u32>,
    /// Initial backoff for retries in milliseconds (randomized with jitter by SDK)
    pub retry_base_delay_ms: Option<u64>,
    /// Max backoff for retries in milliseconds
    pub retry_max_delay_ms: Option<u64>,
}

/// S3-based storage backend
///
/// Implements `Storage` and `StorageWrite` traits for Amazon S3.
/// Supports both standard S3 and S3 Express One Zone (directory buckets).
///
/// S3 Express One Zone authentication is handled automatically by the SDK
/// when using directory bucket names (format: `bucket-name--zone-id--x-s3`).
#[derive(Clone)]
pub struct S3Storage {
    /// S3 client (handles both standard and Express buckets)
    client: Client,
    /// S3 bucket name
    bucket: String,
    /// Optional key prefix
    prefix: Option<String>,
}

impl Debug for S3Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Storage")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("is_express", &Self::is_express_bucket(&self.bucket))
            .finish()
    }
}

impl S3Storage {
    /// Create a new S3 storage backend
    ///
    /// For S3 Express buckets (detected by `--x-s3` suffix), the SDK
    /// automatically handles session-based authentication.
    ///
    /// # Arguments
    ///
    /// * `sdk_config` - AWS SDK configuration (from `aws_config::load_defaults()`)
    /// * `config` - S3-specific configuration (bucket, prefix, timeout)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    /// let s3_config = S3Config {
    ///     bucket: "my-bucket".to_string(),
    ///     prefix: Some("data".to_string()),
    ///     timeout_ms: Some(30000),
    /// };
    /// let storage = S3Storage::new(&sdk_config, s3_config).await?;
    /// ```
    pub async fn new(sdk_config: &aws_config::SdkConfig, config: S3Config) -> Result<Self> {
        // Verify region is configured
        if sdk_config.region().is_none() {
            return Err(AwsStorageError::MissingRegion);
        }

        // Build S3 config by inheriting from SdkConfig (preserves HTTP client, retry config,
        // endpoints, sleep impl, etc.) then apply our overrides
        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(sdk_config);

        // Apply endpoint override if configured (e.g. LocalStack/MinIO)
        if let Some(endpoint) = &config.endpoint {
            s3_config_builder = s3_config_builder.endpoint_url(endpoint);
        }

        // Apply retry overrides (Clojure parity)
        if config.max_retries.is_some()
            || config.retry_base_delay_ms.is_some()
            || config.retry_max_delay_ms.is_some()
        {
            // AWS SDK uses "max attempts" = initial attempt + retries
            let max_attempts = config.max_retries.unwrap_or(0).saturating_add(1).max(1);

            let mut retry_config = RetryConfig::standard().with_max_attempts(max_attempts);

            if let Some(ms) = config.retry_base_delay_ms {
                retry_config = retry_config.with_initial_backoff(Duration::from_millis(ms));
            }
            if let Some(ms) = config.retry_max_delay_ms {
                retry_config = retry_config.with_max_backoff(Duration::from_millis(ms));
            }

            s3_config_builder = s3_config_builder.retry_config(retry_config);
        }

        // Apply timeout if configured
        if let Some(timeout_ms) = config.timeout_ms {
            let timeout_config = TimeoutConfig::builder()
                .operation_timeout(Duration::from_millis(timeout_ms))
                .build();
            s3_config_builder = s3_config_builder.timeout_config(timeout_config);
        }

        let client = Client::from_conf(s3_config_builder.build());

        Ok(Self {
            client,
            bucket: config.bucket,
            prefix: config.prefix,
        })
    }

    /// Create S3Storage from pre-built client (for testing)
    pub fn from_client(client: Client, bucket: String, prefix: Option<String>) -> Self {
        Self {
            client,
            bucket,
            prefix,
        }
    }

    /// Detect S3 Express directory bucket by naming convention
    ///
    /// Pattern: `*--{region-az}-az{digit}--x-s3`
    /// Examples: `my-bucket--use1-az1--x-s3`, `foo--apne1-az2--x-s3`
    pub fn is_express_bucket(bucket: &str) -> bool {
        // Must end with --x-s3 and have -az{digit}-- before it
        if !bucket.ends_with("--x-s3") {
            return false;
        }
        // Find the second-to-last "--" delimiter
        let without_suffix = &bucket[..bucket.len() - 6]; // Remove "--x-s3"
        if let Some(pos) = without_suffix.rfind("--") {
            let az_part = &without_suffix[pos + 2..];
            // Must contain "-az" followed by a digit
            if let Some(az_pos) = az_part.find("-az") {
                let after_az = &az_part[az_pos + 3..];
                return after_az
                    .chars()
                    .next()
                    .map(|c| c.is_ascii_digit())
                    .unwrap_or(false);
            }
        }
        false
    }

    /// Check if this storage is using an Express bucket
    pub fn is_express(&self) -> bool {
        Self::is_express_bucket(&self.bucket)
    }

    /// Get the bucket name
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Get the key prefix
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// Convert a Fluree address to an S3 key
    fn to_key(&self, address: &str) -> std::result::Result<String, CoreError> {
        address_to_key(address, self.prefix.as_deref())
            .map_err(|e| CoreError::storage(format!("Invalid address: {}", e)))
    }

    /// Convert an S3 key to a Fluree address
    fn to_address(&self, key: &str) -> String {
        key_to_address(key, self.prefix.as_deref())
    }
}

#[async_trait]
impl StorageRead for S3Storage {
    async fn read_bytes(&self, address: &str) -> std::result::Result<Vec<u8>, CoreError> {
        let key = self.to_key(address)?;

        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| map_s3_error_core(e, &key))?;

        let bytes = response
            .body
            .collect()
            .await
            .map_err(|e| CoreError::io(format!("Failed to read S3 body: {}", e)))?
            .into_bytes()
            .to_vec();

        Ok(bytes)
    }

    async fn exists(&self, address: &str) -> std::result::Result<bool, CoreError> {
        let key = self.to_key(address)?;

        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                // Pattern match on SdkError to avoid panic from into_service_error()
                use aws_sdk_s3::error::SdkError;
                match &e {
                    SdkError::ServiceError(service_err) => {
                        // Check HTTP status code for 404
                        if service_err.raw().status().as_u16() == 404 {
                            Ok(false)
                        } else {
                            Err(map_s3_error_core(e, &key))
                        }
                    }
                    // For non-service errors (timeout, dispatch, etc), propagate as storage error
                    _ => Err(map_s3_error_core(e, &key)),
                }
            }
        }
    }

    async fn list_prefix(&self, prefix: &str) -> std::result::Result<Vec<String>, CoreError> {
        // Delegate to the nameservice trait impl and convert the error
        StorageList::list_prefix(self, prefix)
            .await
            .map_err(ext_error_to_core)
    }
}

#[async_trait]
impl StorageWrite for S3Storage {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> std::result::Result<(), CoreError> {
        let key = self.to_key(address)?;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(bytes.to_vec()))
            .send()
            .await
            .map_err(|e| map_s3_error_core(e, &key))?;

        Ok(())
    }

    async fn delete(&self, address: &str) -> std::result::Result<(), CoreError> {
        // Delegate to the nameservice trait impl and convert the error
        StorageDelete::delete(self, address)
            .await
            .map_err(ext_error_to_core)
    }
}

impl fluree_db_core::StorageMethod for S3Storage {
    fn storage_method(&self) -> &str {
        fluree_db_core::STORAGE_METHOD_S3
    }
}

#[async_trait]
impl ContentAddressedWrite for S3Storage {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> std::result::Result<ContentWriteResult, CoreError> {
        let address = content_address(
            fluree_db_core::STORAGE_METHOD_S3,
            kind,
            ledger_id,
            content_hash_hex,
        );
        self.write_bytes(&address, bytes).await?;
        Ok(ContentWriteResult {
            address,
            content_hash: content_hash_hex.to_string(),
            size_bytes: bytes.len(),
        })
    }

    async fn content_write_bytes(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        bytes: &[u8],
    ) -> std::result::Result<ContentWriteResult, CoreError> {
        let hash_hex = sha256_hex(bytes);
        self.content_write_bytes_with_hash(kind, ledger_id, &hash_hex, bytes)
            .await
    }
}

// Extended storage trait implementations

#[async_trait]
impl StorageDelete for S3Storage {
    async fn delete(&self, address: &str) -> StorageExtResult<()> {
        let key = address_to_key(address, self.prefix.as_deref())
            .map_err(|e| StorageExtError::io(format!("Invalid address: {}", e)))?;

        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| map_s3_error_ext(e, &key))?;

        Ok(())
    }
}

#[async_trait]
impl StorageList for S3Storage {
    async fn list_prefix(&self, prefix: &str) -> StorageExtResult<Vec<String>> {
        let mut addresses = Vec::new();
        let mut continuation_token = None;

        // Build the full prefix including any configured prefix
        let full_prefix = match &self.prefix {
            Some(p) => format!("{}/{}", p.trim_end_matches('/'), prefix),
            None => prefix.to_string(),
        };

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full_prefix);

            if let Some(token) = continuation_token.take() {
                request = request.continuation_token(token);
            }

            let response = request
                .send()
                .await
                .map_err(|e| map_s3_error_ext(e, &full_prefix))?;

            for object in response.contents() {
                if let Some(key) = object.key() {
                    addresses.push(self.to_address(key));
                }
            }

            match response.next_continuation_token() {
                Some(token) => continuation_token = Some(token.to_string()),
                None => break,
            }
        }

        Ok(addresses)
    }

    async fn list_prefix_paginated(
        &self,
        prefix: &str,
        continuation_token: Option<String>,
        max_keys: usize,
    ) -> StorageExtResult<NsListResult> {
        // Build the full prefix including any configured prefix
        let full_prefix = match &self.prefix {
            Some(p) => format!("{}/{}", p.trim_end_matches('/'), prefix),
            None => prefix.to_string(),
        };

        let mut request = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&full_prefix)
            .max_keys(max_keys as i32);

        if let Some(token) = continuation_token {
            request = request.continuation_token(token);
        }

        let response = request
            .send()
            .await
            .map_err(|e| map_s3_error_ext(e, &full_prefix))?;

        let addresses: Vec<String> = response
            .contents()
            .iter()
            .filter_map(|obj| obj.key().map(|k| self.to_address(k)))
            .collect();

        Ok(NsListResult {
            keys: addresses,
            continuation_token: response.next_continuation_token().map(|s| s.to_string()),
            is_truncated: response.is_truncated().unwrap_or(false),
        })
    }
}

#[async_trait]
impl StorageCas for S3Storage {
    async fn write_if_absent(&self, address: &str, bytes: &[u8]) -> StorageExtResult<bool> {
        let key = address_to_key(address, self.prefix.as_deref())
            .map_err(|e| StorageExtError::io(format!("Invalid address: {}", e)))?;

        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(bytes.to_vec()))
            .if_none_match("*")
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(e) if is_precondition_failed_sdk(&e) => Ok(false),
            Err(e) => Err(map_s3_error_ext(e, &key)),
        }
    }

    async fn write_if_match(
        &self,
        address: &str,
        bytes: &[u8],
        expected_etag: &str,
    ) -> StorageExtResult<String> {
        let key = address_to_key(address, self.prefix.as_deref())
            .map_err(|e| StorageExtError::io(format!("Invalid address: {}", e)))?;

        // AWS expects ETags to be quoted
        let etag_quoted = if expected_etag.starts_with('"') {
            expected_etag.to_string()
        } else {
            format!("\"{}\"", expected_etag)
        };

        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(bytes.to_vec()))
            .if_match(&etag_quoted)
            .send()
            .await;

        match result {
            Ok(output) => {
                let new_etag = output.e_tag().map(normalize_etag).unwrap_or_default();
                Ok(new_etag)
            }
            Err(e) if is_precondition_failed_sdk(&e) => Err(StorageExtError::PreconditionFailed),
            Err(e) => Err(map_s3_error_ext(e, &key)),
        }
    }

    async fn read_with_etag(&self, address: &str) -> StorageExtResult<(Vec<u8>, String)> {
        let key = address_to_key(address, self.prefix.as_deref())
            .map_err(|e| StorageExtError::io(format!("Invalid address: {}", e)))?;

        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| map_s3_error_ext(e, &key))?;

        let etag = response.e_tag().map(normalize_etag).unwrap_or_default();

        let bytes = response
            .body
            .collect()
            .await
            .map_err(|e| StorageExtError::io(format!("Failed to read S3 body: {}", e)))?
            .into_bytes()
            .to_vec();

        Ok((bytes, etag))
    }
}

/// Result of a list operation (re-export for convenience)
pub type ListResult = NsListResult;

// Error mapping helpers

/// Map an SDK error to CoreError, properly handling 404 as NotFound
fn map_s3_error_core<E: std::fmt::Debug>(
    err: aws_sdk_s3::error::SdkError<E>,
    key: &str,
) -> CoreError {
    use aws_sdk_s3::error::SdkError;

    match &err {
        SdkError::ServiceError(service_err) => {
            let status = service_err.raw().status().as_u16();
            match status {
                404 => CoreError::not_found(format!("Key not found: {}", key)),
                403 => CoreError::storage(format!("Access denied for key '{}': {:?}", key, err)),
                _ => CoreError::storage(format!(
                    "S3 error for key '{}' (HTTP {}): {:?}",
                    key, status, err
                )),
            }
        }
        SdkError::TimeoutError(_) => {
            CoreError::io(format!("S3 timeout for key '{}': {:?}", key, err))
        }
        SdkError::DispatchFailure(_) => {
            CoreError::io(format!("S3 connection error for key '{}': {:?}", key, err))
        }
        _ => CoreError::storage(format!("S3 error for key '{}': {:?}", key, err)),
    }
}

/// Map an SDK error to StorageExtError with proper HTTP status classification
fn map_s3_error_ext<E: std::fmt::Debug>(
    err: aws_sdk_s3::error::SdkError<E>,
    key: &str,
) -> StorageExtError {
    use aws_sdk_s3::error::SdkError;

    match &err {
        SdkError::ServiceError(service_err) => {
            let status = service_err.raw().status().as_u16();
            match status {
                404 => StorageExtError::not_found(format!("Key not found: {}", key)),
                401 => StorageExtError::unauthorized(format!("Unauthorized for key: {}", key)),
                403 => StorageExtError::forbidden(format!("Access denied for key: {}", key)),
                412 => StorageExtError::PreconditionFailed,
                // Retryable server errors: throttling (429), server errors (500/502/503/504)
                429 | 500 | 502 | 503 | 504 => StorageExtError::throttled(format!(
                    "Retryable error for key '{}' (HTTP {})",
                    key, status
                )),
                _ => StorageExtError::io(format!(
                    "S3 error for key '{}' (HTTP {}): {:?}",
                    key, status, err
                )),
            }
        }
        SdkError::TimeoutError(_) => {
            StorageExtError::io(format!("S3 timeout for key '{}': {:?}", key, err))
        }
        SdkError::DispatchFailure(_) => {
            StorageExtError::io(format!("S3 connection error for key '{}': {:?}", key, err))
        }
        _ => StorageExtError::io(format!("S3 error for key '{}': {:?}", key, err)),
    }
}

/// Check if an SDK error is a 412 Precondition Failed response
fn is_precondition_failed_sdk<E: std::fmt::Debug>(err: &aws_sdk_s3::error::SdkError<E>) -> bool {
    use aws_sdk_s3::error::SdkError;

    match err {
        SdkError::ServiceError(service_err) => service_err.raw().status().as_u16() == 412,
        _ => false,
    }
}

/// Convert StorageExtError to CoreError
fn ext_error_to_core(err: StorageExtError) -> CoreError {
    match err {
        StorageExtError::Io(msg) => CoreError::io(msg),
        StorageExtError::NotFound(msg) => CoreError::not_found(msg),
        StorageExtError::Unauthorized(msg) => CoreError::storage(format!("Unauthorized: {}", msg)),
        StorageExtError::Forbidden(msg) => CoreError::storage(format!("Forbidden: {}", msg)),
        StorageExtError::Throttled(msg) => CoreError::io(format!("Throttled: {}", msg)),
        StorageExtError::PreconditionFailed => CoreError::storage("Precondition failed"),
        StorageExtError::Other(msg) => CoreError::other(msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_express_bucket() {
        // Valid Express bucket names
        assert!(S3Storage::is_express_bucket("my-bucket--use1-az1--x-s3"));
        assert!(S3Storage::is_express_bucket("my-bucket--usw2-az2--x-s3"));
        assert!(S3Storage::is_express_bucket("test--apne1-az3--x-s3"));

        // Invalid - not Express buckets
        assert!(!S3Storage::is_express_bucket("my-bucket"));
        assert!(!S3Storage::is_express_bucket("my-bucket--x-s3")); // Missing az pattern
        assert!(!S3Storage::is_express_bucket("my-bucket-az1--x-s3")); // Missing --

        // Edge cases
        assert!(!S3Storage::is_express_bucket(""));
        assert!(!S3Storage::is_express_bucket("--x-s3"));
    }

    #[test]
    fn test_s3_config_default() {
        let config = S3Config::default();
        assert!(config.bucket.is_empty());
        assert!(config.prefix.is_none());
        assert!(config.endpoint.is_none());
        assert!(config.timeout_ms.is_none());
    }
}
