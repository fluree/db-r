//! Direct catalog client — bypasses REST catalog, reads metadata from S3 directly.
//!
//! This client resolves the current Iceberg table version via `version-hint.text`
//! in the table's metadata directory, then reads the corresponding
//! `vN.metadata.json` file. This is the same pattern used by Iceberg's
//! Hadoop file-based catalog.
//!
//! # Usage
//!
//! ```ignore
//! use fluree_db_iceberg::catalog::direct::DirectCatalogClient;
//! use fluree_db_iceberg::io::S3IcebergStorage;
//!
//! let storage = S3IcebergStorage::from_default_chain(Some("us-east-1"), None, false).await?;
//! let client = DirectCatalogClient::new(
//!     "s3://bucket/warehouse/ns/table".to_string(),
//!     Arc::new(storage),
//! );
//!
//! let response = client.load_table(&table_id, false).await?;
//! // response.metadata_location → "s3://bucket/warehouse/ns/table/metadata/v42.metadata.json"
//! ```

use crate::catalog::{CatalogClient, LoadTableResponse, TableIdentifier};
use crate::error::{IcebergError, Result};
use crate::io::IcebergStorage;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

/// Catalog client that reads Iceberg metadata directly from a known S3 table location.
///
/// Instead of querying a REST catalog API, this client:
/// 1. Reads `{table_location}/metadata/version-hint.text` (one small S3 GET)
/// 2. Parses the version number from the hint file
/// 3. Constructs the metadata path: `{table_location}/metadata/v{N}.metadata.json`
///
/// This is ideal for use cases where the writer (e.g., `iceberg-rust`) already
/// knows the table location and a REST catalog adds unnecessary overhead.
pub struct DirectCatalogClient<S: IcebergStorage> {
    table_location: String,
    storage: Arc<S>,
}

impl<S: IcebergStorage> DirectCatalogClient<S> {
    /// Create a new direct catalog client.
    ///
    /// `table_location` should be the S3 prefix for the table root directory
    /// (e.g., `s3://bucket/warehouse/ns/table`). It must contain a `metadata/`
    /// subdirectory with Iceberg metadata files.
    pub fn new(table_location: String, storage: Arc<S>) -> Self {
        Self {
            table_location,
            storage,
        }
    }

    /// Resolve the current metadata version via `version-hint.text`.
    ///
    /// Returns the full S3 path to the current metadata JSON file.
    async fn resolve_metadata_location(&self) -> Result<String> {
        let hint_path = format!("{}/metadata/version-hint.text", self.table_location);
        let hint_bytes = self.storage.read(&hint_path).await.map_err(|e| {
            IcebergError::Metadata(format!(
                "Failed to read version-hint.text at {}: {}",
                hint_path, e
            ))
        })?;

        let version_str = std::str::from_utf8(&hint_bytes)
            .map_err(|e| IcebergError::Metadata(format!("Invalid version-hint.text: {}", e)))?
            .trim();

        let version: u32 = version_str.parse().map_err(|e| {
            IcebergError::Metadata(format!(
                "version-hint.text contains invalid version '{}': {}",
                version_str, e
            ))
        })?;

        Ok(format!(
            "{}/metadata/v{}.metadata.json",
            self.table_location, version
        ))
    }
}

impl<S: IcebergStorage> std::fmt::Debug for DirectCatalogClient<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectCatalogClient")
            .field("table_location", &self.table_location)
            .finish()
    }
}

#[async_trait(?Send)]
impl<S: IcebergStorage> CatalogClient for DirectCatalogClient<S> {
    /// Not supported for direct catalogs — returns an error.
    async fn list_namespaces(&self) -> Result<Vec<String>> {
        Err(IcebergError::Catalog(
            "Direct catalog does not support namespace listing".to_string(),
        ))
    }

    /// Not supported for direct catalogs — returns an error.
    async fn list_tables(&self, _namespace: &str) -> Result<Vec<String>> {
        Err(IcebergError::Catalog(
            "Direct catalog does not support table listing".to_string(),
        ))
    }

    /// Load table metadata by resolving `version-hint.text` from the table location.
    ///
    /// The `request_credentials` parameter is ignored — direct mode uses the
    /// storage client's own credentials (IAM role, env vars, etc.).
    async fn load_table(
        &self,
        _table_id: &TableIdentifier,
        _request_credentials: bool,
    ) -> Result<LoadTableResponse> {
        let metadata_location = self.resolve_metadata_location().await?;

        Ok(LoadTableResponse {
            metadata_location,
            config: HashMap::new(),
            credentials: None, // Direct mode uses ambient credentials
        })
    }
}

// ---------------------------------------------------------------------------
// Send-safe variant for server-side usage
// ---------------------------------------------------------------------------

#[cfg(feature = "aws")]
use crate::catalog::SendCatalogClient;
#[cfg(feature = "aws")]
use crate::io::SendIcebergStorage;

/// Send-safe direct catalog client for server-side usage with `tokio::spawn`.
#[cfg(feature = "aws")]
pub struct SendDirectCatalogClient<S: SendIcebergStorage> {
    table_location: String,
    storage: Arc<S>,
}

#[cfg(feature = "aws")]
impl<S: SendIcebergStorage> SendDirectCatalogClient<S> {
    /// Create a new send-safe direct catalog client.
    pub fn new(table_location: String, storage: Arc<S>) -> Self {
        Self {
            table_location,
            storage,
        }
    }

    /// Resolve the current metadata version via `version-hint.text`.
    async fn resolve_metadata_location(&self) -> Result<String> {
        let hint_path = format!("{}/metadata/version-hint.text", self.table_location);
        let hint_bytes = self.storage.read(&hint_path).await.map_err(|e| {
            IcebergError::Metadata(format!(
                "Failed to read version-hint.text at {}: {}",
                hint_path, e
            ))
        })?;

        let version_str = std::str::from_utf8(&hint_bytes)
            .map_err(|e| IcebergError::Metadata(format!("Invalid version-hint.text: {}", e)))?
            .trim();

        let version: u32 = version_str.parse().map_err(|e| {
            IcebergError::Metadata(format!(
                "version-hint.text contains invalid version '{}': {}",
                version_str, e
            ))
        })?;

        Ok(format!(
            "{}/metadata/v{}.metadata.json",
            self.table_location, version
        ))
    }
}

#[cfg(feature = "aws")]
impl<S: SendIcebergStorage> std::fmt::Debug for SendDirectCatalogClient<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendDirectCatalogClient")
            .field("table_location", &self.table_location)
            .finish()
    }
}

#[cfg(feature = "aws")]
#[async_trait]
impl<S: SendIcebergStorage + 'static> SendCatalogClient for SendDirectCatalogClient<S> {
    async fn list_namespaces(&self) -> Result<Vec<String>> {
        Err(IcebergError::Catalog(
            "Direct catalog does not support namespace listing".to_string(),
        ))
    }

    async fn list_tables(&self, _namespace: &str) -> Result<Vec<String>> {
        Err(IcebergError::Catalog(
            "Direct catalog does not support table listing".to_string(),
        ))
    }

    async fn load_table(
        &self,
        _table_id: &TableIdentifier,
        _request_credentials: bool,
    ) -> Result<LoadTableResponse> {
        let metadata_location = self.resolve_metadata_location().await?;

        Ok(LoadTableResponse {
            metadata_location,
            config: HashMap::new(),
            credentials: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::MemoryStorage;

    #[tokio::test]
    async fn test_direct_catalog_resolves_version_hint() {
        let mut storage = MemoryStorage::new();
        storage.add_file("s3://bucket/table/metadata/version-hint.text", "5");
        // We don't need the actual metadata file for load_table — just the location
        let client = DirectCatalogClient::new("s3://bucket/table".to_string(), Arc::new(storage));

        let table_id = TableIdentifier {
            namespace: "ns".to_string(),
            table: "table".to_string(),
        };

        let response = client.load_table(&table_id, false).await.unwrap();
        assert_eq!(
            response.metadata_location,
            "s3://bucket/table/metadata/v5.metadata.json"
        );
        assert!(response.credentials.is_none());
    }

    #[tokio::test]
    async fn test_direct_catalog_missing_version_hint() {
        let storage = MemoryStorage::new();
        let client = DirectCatalogClient::new("s3://bucket/table".to_string(), Arc::new(storage));

        let table_id = TableIdentifier {
            namespace: "ns".to_string(),
            table: "table".to_string(),
        };

        let result = client.load_table(&table_id, false).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("version-hint.text"), "Error: {}", err_msg);
    }

    #[tokio::test]
    async fn test_direct_catalog_malformed_version_hint() {
        let mut storage = MemoryStorage::new();
        storage.add_file(
            "s3://bucket/table/metadata/version-hint.text",
            "not-a-number",
        );

        let client = DirectCatalogClient::new("s3://bucket/table".to_string(), Arc::new(storage));

        let table_id = TableIdentifier {
            namespace: "ns".to_string(),
            table: "table".to_string(),
        };

        let result = client.load_table(&table_id, false).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("invalid version"), "Error: {}", err_msg);
    }

    #[tokio::test]
    async fn test_direct_catalog_version_hint_with_whitespace() {
        let mut storage = MemoryStorage::new();
        storage.add_file("s3://bucket/table/metadata/version-hint.text", "42\n");

        let client = DirectCatalogClient::new("s3://bucket/table".to_string(), Arc::new(storage));

        let table_id = TableIdentifier {
            namespace: "ns".to_string(),
            table: "table".to_string(),
        };

        let response = client.load_table(&table_id, false).await.unwrap();
        assert_eq!(
            response.metadata_location,
            "s3://bucket/table/metadata/v42.metadata.json"
        );
    }

    #[tokio::test]
    async fn test_direct_catalog_list_namespaces_unsupported() {
        let storage = MemoryStorage::new();
        let client = DirectCatalogClient::new("s3://bucket/table".to_string(), Arc::new(storage));

        let result = client.list_namespaces().await;
        assert!(result.is_err());
    }
}
