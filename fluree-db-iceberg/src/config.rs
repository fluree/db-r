//! Configuration schemas for Iceberg virtual graphs.
//!
//! This module defines the JSON-serializable configuration structures
//! stored in `VgNsRecord.config` for Iceberg virtual graphs.

use crate::auth::AuthConfig;
use crate::catalog::parse_table_identifier;
use crate::catalog::TableIdentifier;
use crate::error::{IcebergError, Result};
use serde::{Deserialize, Serialize};

/// Configuration for an Iceberg virtual graph.
///
/// This is stored as JSON in `VgNsRecord.config` for VGs with type `VgType::Iceberg`.
///
/// # Example JSON
///
/// ```json
/// {
///     "catalog": {
///         "uri": "https://polaris.example.com",
///         "auth": {
///             "type": "bearer",
///             "token": {"env_var": "POLARIS_TOKEN"}
///         }
///     },
///     "table": "openflights.airlines",
///     "io": {
///         "vended_credentials": true
///     }
/// }
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IcebergVgConfig {
    /// Catalog configuration
    pub catalog: CatalogConfig,
    /// Table identifier
    pub table: TableConfig,
    /// Storage/IO configuration
    #[serde(default)]
    pub io: IoConfig,
    /// R2RML mapping source (format-agnostic, used in Phase 3)
    #[serde(default)]
    pub mapping: Option<MappingSource>,
}

impl IcebergVgConfig {
    /// Parse from JSON string (stored in VgNsRecord.config).
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json)
            .map_err(|e| IcebergError::Config(format!("Failed to parse Iceberg VG config: {}", e)))
    }

    /// Serialize to JSON string.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|e| IcebergError::Config(format!("Failed to serialize config: {}", e)))
    }

    /// Serialize to pretty-printed JSON string.
    pub fn to_json_pretty(&self) -> Result<String> {
        serde_json::to_string_pretty(self)
            .map_err(|e| IcebergError::Config(format!("Failed to serialize config: {}", e)))
    }

    /// Get the table identifier.
    pub fn table_identifier(&self) -> Result<TableIdentifier> {
        parse_table_identifier(&self.table.identifier())
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.catalog.uri.is_empty() {
            return Err(IcebergError::Config("catalog.uri is required".to_string()));
        }

        // Validate table identifier can be parsed
        self.table_identifier()?;

        Ok(())
    }
}

/// Catalog connection configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CatalogConfig {
    /// Catalog type (currently only "polaris"/"rest" supported)
    #[serde(default = "default_catalog_type")]
    pub catalog_type: String,
    /// REST catalog base URI
    pub uri: String,
    /// Authentication configuration
    #[serde(default)]
    pub auth: AuthConfig,
    /// Optional warehouse identifier
    #[serde(default)]
    pub warehouse: Option<String>,
}

fn default_catalog_type() -> String {
    "polaris".to_string()
}

/// Table identification configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum TableConfig {
    /// Full table identifier string (e.g., "openflights.airlines")
    Identifier(String),
    /// Structured namespace + table
    Structured { namespace: String, name: String },
}

impl TableConfig {
    /// Get the canonical table identifier string.
    pub fn identifier(&self) -> String {
        match self {
            TableConfig::Identifier(id) => id.clone(),
            TableConfig::Structured { namespace, name } => format!("{}.{}", namespace, name),
        }
    }
}

/// Storage I/O configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IoConfig {
    /// Whether to use vended credentials from the catalog (default: true)
    #[serde(default = "default_vended_credentials")]
    pub vended_credentials: bool,
    /// S3 region override
    #[serde(default)]
    pub s3_region: Option<String>,
    /// S3 endpoint override (for MinIO, LocalStack, etc.)
    #[serde(default)]
    pub s3_endpoint: Option<String>,
    /// Use path-style S3 URLs (for MinIO, LocalStack)
    #[serde(default)]
    pub s3_path_style: bool,
}

fn default_vended_credentials() -> bool {
    true
}

impl Default for IoConfig {
    fn default() -> Self {
        Self {
            vended_credentials: true, // Default to using vended credentials
            s3_region: None,
            s3_endpoint: None,
            s3_path_style: false,
        }
    }
}

/// R2RML mapping source (format-agnostic).
///
/// Phase 3 will use this to load mappings without depending on
/// the serialization format (Turtle, JSON-LD, etc.).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MappingSource {
    /// Storage address, URL, or inline content
    pub source: String,
    /// Media type hint (optional, inferred from source extension if omitted)
    /// Examples: "text/turtle", "application/ld+json"
    #[serde(default)]
    pub media_type: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let json = r#"{
            "catalog": {
                "uri": "https://polaris.example.com"
            },
            "table": "openflights.airlines"
        }"#;

        let config: IcebergVgConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.catalog.uri, "https://polaris.example.com");
        assert_eq!(config.catalog.catalog_type, "polaris");
        assert_eq!(config.table.identifier(), "openflights.airlines");
        assert!(config.io.vended_credentials); // default
        assert!(config.mapping.is_none());
    }

    #[test]
    fn test_parse_full_config() {
        let json = r#"{
            "catalog": {
                "uri": "https://polaris.example.com",
                "catalog_type": "rest",
                "auth": {
                    "type": "bearer",
                    "token": "my-token"
                },
                "warehouse": "my-warehouse"
            },
            "table": {
                "namespace": "db.schema",
                "name": "events"
            },
            "io": {
                "vended_credentials": false,
                "s3_region": "us-west-2",
                "s3_endpoint": "http://localhost:9000"
            },
            "mapping": {
                "source": "s3://bucket/mapping.ttl",
                "media_type": "text/turtle"
            }
        }"#;

        let config: IcebergVgConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.catalog.catalog_type, "rest");
        assert_eq!(config.catalog.warehouse, Some("my-warehouse".to_string()));
        assert_eq!(config.table.identifier(), "db.schema.events");
        assert!(!config.io.vended_credentials);
        assert_eq!(config.io.s3_region, Some("us-west-2".to_string()));
        assert!(config.mapping.is_some());
        let mapping = config.mapping.unwrap();
        assert_eq!(mapping.source, "s3://bucket/mapping.ttl");
        assert_eq!(mapping.media_type, Some("text/turtle".to_string()));
    }

    #[test]
    fn test_validate_missing_uri() {
        let json = r#"{
            "catalog": {
                "uri": ""
            },
            "table": "ns.table"
        }"#;

        let config: IcebergVgConfig = serde_json::from_str(json).unwrap();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("uri"));
    }

    #[test]
    fn test_validate_invalid_table_id() {
        let json = r#"{
            "catalog": {
                "uri": "https://polaris.example.com"
            },
            "table": "invalid"
        }"#;

        let config: IcebergVgConfig = serde_json::from_str(json).unwrap();
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_roundtrip_serialization() {
        let original = IcebergVgConfig {
            catalog: CatalogConfig {
                catalog_type: "polaris".to_string(),
                uri: "https://polaris.example.com".to_string(),
                auth: AuthConfig::None,
                warehouse: None,
            },
            table: TableConfig::Identifier("ns.table".to_string()),
            io: IoConfig::default(),
            mapping: None,
        };

        let json = original.to_json().unwrap();
        let parsed = IcebergVgConfig::from_json(&json).unwrap();

        assert_eq!(parsed.catalog.uri, original.catalog.uri);
        assert_eq!(parsed.table.identifier(), original.table.identifier());
    }
}
