//! Configuration types for virtual graph creation.
//!
//! This module contains builder-style configuration structs for creating
//! different types of virtual graphs (BM25, Vector, Iceberg, R2RML).

use fluree_db_core::alias as core_alias;
use fluree_db_query::bm25::Bm25Config;
use serde_json::Value as JsonValue;

#[cfg(feature = "iceberg")]
use fluree_db_iceberg::IcebergVgConfig;

// =============================================================================
// BM25 Configuration
// =============================================================================

/// Configuration for creating a BM25 full-text search index.
#[derive(Debug, Clone)]
pub struct Bm25CreateConfig {
    /// Name for the virtual graph (e.g., "my-search")
    pub name: String,

    /// Branch name (defaults to "main")
    pub branch: Option<String>,

    /// Source ledger alias (e.g., "docs:main")
    pub ledger: String,

    /// Indexing query that defines what to index.
    ///
    /// The query must:
    /// - Include `@id` in the select to identify documents
    /// - Select properties whose text content should be indexed
    ///
    /// Example:
    /// ```json
    /// {
    ///   "@context": {"ex": "http://example.org/"},
    ///   "where": [{"@id": "?x", "@type": "ex:Article"}],
    ///   "select": {"?x": ["@id", "ex:title", "ex:content"]}
    /// }
    /// ```
    pub query: JsonValue,

    /// BM25 k1 parameter (term frequency saturation). Default: 1.2
    pub k1: Option<f64>,

    /// BM25 b parameter (document length normalization). Default: 0.75
    pub b: Option<f64>,
}

impl Bm25CreateConfig {
    /// Create a new config with minimal required fields.
    pub fn new(name: impl Into<String>, ledger: impl Into<String>, query: JsonValue) -> Self {
        Self {
            name: name.into(),
            branch: None,
            ledger: ledger.into(),
            query,
            k1: None,
            b: None,
        }
    }

    /// Set the branch name.
    pub fn with_branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = Some(branch.into());
        self
    }

    /// Set BM25 k1 parameter.
    pub fn with_k1(mut self, k1: f64) -> Self {
        self.k1 = Some(k1);
        self
    }

    /// Set BM25 b parameter.
    pub fn with_b(mut self, b: f64) -> Self {
        self.b = Some(b);
        self
    }

    /// Get the effective branch name.
    pub fn effective_branch(&self) -> &str {
        self.branch.as_deref().unwrap_or(core_alias::DEFAULT_BRANCH)
    }

    /// Get the virtual graph alias (name:branch).
    pub fn vg_alias(&self) -> String {
        format!("{}:{}", self.name, self.effective_branch())
    }

    /// Build BM25Config from the options.
    pub fn bm25_config(&self) -> Bm25Config {
        Bm25Config::new(self.k1.unwrap_or(1.2), self.b.unwrap_or(0.75))
    }

    /// Validate the configuration.
    ///
    /// Returns an error if any configuration values are invalid.
    ///
    /// # Validation Rules
    ///
    /// - `name` must not be empty
    /// - `ledger` must not be empty
    /// - `k1` must be positive (if specified)
    /// - `b` must be between 0 and 1 (if specified)
    /// - `query` must have a "select" clause
    pub fn validate(&self) -> crate::Result<()> {
        // Validate name
        if self.name.trim().is_empty() {
            return Err(crate::ApiError::config("VG name cannot be empty"));
        }

        // Validate name format (no colons allowed - reserved for alias)
        if self.name.contains(':') {
            return Err(crate::ApiError::config(
                "VG name cannot contain ':' (use branch for versioning)",
            ));
        }

        // Validate ledger alias
        if self.ledger.trim().is_empty() {
            return Err(crate::ApiError::config("Source ledger cannot be empty"));
        }

        // Validate k1
        if let Some(k1) = self.k1 {
            if k1 <= 0.0 {
                return Err(crate::ApiError::config(format!(
                    "k1 must be positive, got {}",
                    k1
                )));
            }
            if k1 > 10.0 {
                // Warn but don't error - unusual but valid
                tracing::warn!(k1 = k1, "Unusually high k1 value (typical: 1.2-2.0)");
            }
        }

        // Validate b
        if let Some(b) = self.b {
            if !(0.0..=1.0).contains(&b) {
                return Err(crate::ApiError::config(format!(
                    "b must be between 0 and 1, got {}",
                    b
                )));
            }
        }

        // Validate query structure
        if self.query.get("select").is_none() && self.query.get("selectOne").is_none() {
            return Err(crate::ApiError::config(
                "Indexing query must have a 'select' or 'selectOne' clause",
            ));
        }

        Ok(())
    }
}

// =============================================================================
// Vector Search Configuration
// =============================================================================

/// Configuration for creating a vector similarity search index.
///
/// Vector VGs provide approximate nearest neighbor search using embedding vectors.
/// The index is built using HNSW and supports cosine, dot product,
/// and Euclidean distance metrics.
///
/// # Example
///
/// ```ignore
/// use fluree_db_api::VectorCreateConfig;
/// use fluree_db_query::vector::DistanceMetric;
///
/// let config = VectorCreateConfig::new(
///     "embeddings",
///     "docs:main",
///     json!({
///         "@context": {"ex": "http://example.org/"},
///         "where": [{"@id": "?doc", "@type": "ex:Article"}],
///         "select": {"?doc": ["@id", "ex:embedding"]}
///     }),
///     "ex:embedding",
///     768,
/// )
/// .with_metric(DistanceMetric::Cosine);
///
/// let result = fluree.create_vector_index(config).await?;
/// ```
#[cfg(feature = "vector")]
#[derive(Debug, Clone)]
pub struct VectorCreateConfig {
    /// Name for the virtual graph (e.g., "embeddings")
    pub name: String,

    /// Branch name (defaults to "main")
    pub branch: Option<String>,

    /// Source ledger alias (e.g., "docs:main")
    pub ledger: String,

    /// Indexing query that defines what to index.
    ///
    /// The query must:
    /// - Include `@id` in the select to identify documents
    /// - Select the embedding property
    pub query: JsonValue,

    /// Property path to the embedding vector (e.g., "ex:embedding")
    pub embedding_property: String,

    /// Expected vector dimensions (e.g., 768 for sentence transformers)
    pub dimensions: usize,

    /// Distance metric for similarity search. Default: Cosine
    pub metric: Option<DistanceMetric>,

    /// HNSW connectivity parameter (default: 16)
    /// Higher values give better recall but slower indexing
    pub connectivity: Option<usize>,

    /// Expansion factor during index construction (default: 128)
    pub expansion_add: Option<usize>,

    /// Expansion factor during search (default: 64)
    /// Higher values give better recall but slower search
    pub expansion_search: Option<usize>,

    /// Deployment configuration (embedded or remote).
    ///
    /// If `None`, defaults to embedded mode. Set to remote mode to delegate
    /// vector search to a remote search service via HTTP.
    pub deployment: Option<SearchDeploymentConfig>,
}

#[cfg(feature = "vector")]
impl VectorCreateConfig {
    /// Create a new config with minimal required fields.
    pub fn new(
        name: impl Into<String>,
        ledger: impl Into<String>,
        query: JsonValue,
        embedding_property: impl Into<String>,
        dimensions: usize,
    ) -> Self {
        Self {
            name: name.into(),
            branch: None,
            ledger: ledger.into(),
            query,
            embedding_property: embedding_property.into(),
            dimensions,
            metric: None,
            connectivity: None,
            expansion_add: None,
            expansion_search: None,
            deployment: None,
        }
    }

    /// Set the branch name.
    pub fn with_branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = Some(branch.into());
        self
    }

    /// Set the distance metric.
    pub fn with_metric(mut self, metric: DistanceMetric) -> Self {
        self.metric = Some(metric);
        self
    }

    /// Set HNSW connectivity parameter.
    pub fn with_connectivity(mut self, connectivity: usize) -> Self {
        self.connectivity = Some(connectivity);
        self
    }

    /// Set expansion factor for index construction.
    pub fn with_expansion_add(mut self, expansion_add: usize) -> Self {
        self.expansion_add = Some(expansion_add);
        self
    }

    /// Set expansion factor for search.
    pub fn with_expansion_search(mut self, expansion_search: usize) -> Self {
        self.expansion_search = Some(expansion_search);
        self
    }

    /// Set the deployment configuration (embedded or remote).
    pub fn with_deployment(mut self, deployment: SearchDeploymentConfig) -> Self {
        self.deployment = Some(deployment);
        self
    }

    /// Get the effective branch name.
    pub fn effective_branch(&self) -> &str {
        self.branch.as_deref().unwrap_or(core_alias::DEFAULT_BRANCH)
    }

    /// Get the virtual graph alias (name:branch).
    pub fn vg_alias(&self) -> String {
        format!("{}:{}", self.name, self.effective_branch())
    }

    /// Get the effective distance metric.
    pub fn effective_metric(&self) -> DistanceMetric {
        self.metric.unwrap_or(DistanceMetric::Cosine)
    }

    /// Validate the configuration.
    ///
    /// # Validation Rules
    ///
    /// - `name` must not be empty
    /// - `name` must not contain ':'
    /// - `ledger` must not be empty
    /// - `embedding_property` must not be empty
    /// - `dimensions` must be positive
    /// - `query` must have a "select" clause
    pub fn validate(&self) -> crate::Result<()> {
        // Validate name
        if self.name.trim().is_empty() {
            return Err(crate::ApiError::config("VG name cannot be empty"));
        }

        if self.name.contains(':') {
            return Err(crate::ApiError::config(
                "VG name cannot contain ':' (use branch for versioning)",
            ));
        }

        // Validate ledger alias
        if self.ledger.trim().is_empty() {
            return Err(crate::ApiError::config("Source ledger cannot be empty"));
        }

        // Validate embedding property
        if self.embedding_property.trim().is_empty() {
            return Err(crate::ApiError::config(
                "Embedding property cannot be empty",
            ));
        }

        // Validate dimensions
        if self.dimensions == 0 {
            return Err(crate::ApiError::config(
                "Vector dimensions must be positive",
            ));
        }

        // Validate query structure
        if self.query.get("select").is_none() && self.query.get("selectOne").is_none() {
            return Err(crate::ApiError::config(
                "Indexing query must have a 'select' or 'selectOne' clause",
            ));
        }

        Ok(())
    }
}

// =============================================================================
// Iceberg Configuration
// =============================================================================

/// Configuration for creating an Iceberg virtual graph.
///
/// Iceberg VGs provide access to Apache Iceberg tables stored in data lakes
/// (S3, GCS, etc.) via REST catalogs like Apache Polaris.
///
/// # Example
///
/// ```ignore
/// use fluree_db_api::IcebergCreateConfig;
///
/// let config = IcebergCreateConfig::new(
///     "openflights-vg",
///     "https://polaris.example.com",
///     "openflights.airlines",
/// )
/// .with_auth_bearer("my-token")
/// .with_warehouse("my-warehouse");
///
/// let result = fluree.create_iceberg_vg(config).await?;
/// ```
#[cfg(feature = "iceberg")]
#[derive(Debug, Clone)]
pub struct IcebergCreateConfig {
    /// Name for the virtual graph (e.g., "openflights-vg")
    pub name: String,

    /// Branch name (defaults to "main")
    pub branch: Option<String>,

    /// REST catalog URI
    pub catalog_uri: String,

    /// Table identifier (e.g., "openflights.airlines" or "namespace.table")
    pub table_identifier: String,

    /// Optional warehouse identifier
    pub warehouse: Option<String>,

    /// Authentication configuration
    pub auth: fluree_db_iceberg::auth::AuthConfig,

    /// Whether to use vended credentials (default: true)
    pub vended_credentials: bool,

    /// S3 region override
    pub s3_region: Option<String>,

    /// S3 endpoint override (for MinIO, LocalStack)
    pub s3_endpoint: Option<String>,

    /// Use path-style S3 URLs
    pub s3_path_style: bool,
}

#[cfg(feature = "iceberg")]
impl IcebergCreateConfig {
    /// Create a new Iceberg VG config with minimal required fields.
    pub fn new(
        name: impl Into<String>,
        catalog_uri: impl Into<String>,
        table_identifier: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            branch: None,
            catalog_uri: catalog_uri.into(),
            table_identifier: table_identifier.into(),
            warehouse: None,
            auth: fluree_db_iceberg::auth::AuthConfig::None,
            vended_credentials: true,
            s3_region: None,
            s3_endpoint: None,
            s3_path_style: false,
        }
    }

    /// Set the branch name.
    pub fn with_branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = Some(branch.into());
        self
    }

    /// Set bearer token authentication.
    pub fn with_auth_bearer(mut self, token: impl Into<String>) -> Self {
        self.auth = fluree_db_iceberg::auth::AuthConfig::Bearer {
            token: fluree_db_iceberg::ConfigValue::literal(token.into()),
        };
        self
    }

    /// Set OAuth2 client credentials authentication.
    pub fn with_auth_oauth2(
        mut self,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Self {
        self.auth = fluree_db_iceberg::auth::AuthConfig::OAuth2ClientCredentials {
            token_url: token_url.into(),
            client_id: fluree_db_iceberg::ConfigValue::literal(client_id.into()),
            client_secret: fluree_db_iceberg::ConfigValue::literal(client_secret.into()),
            scope: None,
            audience: None,
        };
        self
    }

    /// Set the warehouse identifier.
    pub fn with_warehouse(mut self, warehouse: impl Into<String>) -> Self {
        self.warehouse = Some(warehouse.into());
        self
    }

    /// Enable or disable vended credentials.
    pub fn with_vended_credentials(mut self, enabled: bool) -> Self {
        self.vended_credentials = enabled;
        self
    }

    /// Set S3 region.
    pub fn with_s3_region(mut self, region: impl Into<String>) -> Self {
        self.s3_region = Some(region.into());
        self
    }

    /// Set S3 endpoint (for MinIO, LocalStack).
    pub fn with_s3_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.s3_endpoint = Some(endpoint.into());
        self
    }

    /// Enable path-style S3 URLs.
    pub fn with_s3_path_style(mut self, enabled: bool) -> Self {
        self.s3_path_style = enabled;
        self
    }

    /// Get the effective branch name.
    pub fn effective_branch(&self) -> &str {
        self.branch.as_deref().unwrap_or("main")
    }

    /// Get the virtual graph alias (name:branch).
    pub fn vg_alias(&self) -> String {
        format!("{}:{}", self.name, self.effective_branch())
    }

    /// Convert to the internal IcebergVgConfig structure for storage.
    pub fn to_iceberg_vg_config(&self) -> IcebergVgConfig {
        use fluree_db_iceberg::config::{CatalogConfig, IoConfig, TableConfig};

        IcebergVgConfig {
            catalog: CatalogConfig {
                catalog_type: "polaris".to_string(),
                uri: self.catalog_uri.clone(),
                auth: self.auth.clone(),
                warehouse: self.warehouse.clone(),
            },
            table: TableConfig::Identifier(self.table_identifier.clone()),
            io: IoConfig {
                vended_credentials: self.vended_credentials,
                s3_region: self.s3_region.clone(),
                s3_endpoint: self.s3_endpoint.clone(),
                s3_path_style: self.s3_path_style,
            },
            mapping: None,
        }
    }

    /// Validate the configuration.
    pub fn validate(&self) -> crate::Result<()> {
        if self.name.trim().is_empty() {
            return Err(crate::ApiError::config("VG name cannot be empty"));
        }

        if self.name.contains(':') {
            return Err(crate::ApiError::config(
                "VG name cannot contain ':' (use branch for versioning)",
            ));
        }

        if self.catalog_uri.trim().is_empty() {
            return Err(crate::ApiError::config("Catalog URI cannot be empty"));
        }

        if self.table_identifier.trim().is_empty() {
            return Err(crate::ApiError::config("Table identifier cannot be empty"));
        }

        // Validate table identifier format
        use fluree_db_iceberg::catalog::parse_table_identifier;
        parse_table_identifier(&self.table_identifier).map_err(|e| {
            crate::ApiError::config(format!("Invalid table identifier: {}", e))
        })?;

        Ok(())
    }
}

// =============================================================================
// R2RML Configuration
// =============================================================================

/// Configuration for creating an R2RML virtual graph.
///
/// R2RML VGs combine Iceberg table access with R2RML mappings to expose
/// relational data as RDF triples. The R2RML mapping defines how table
/// rows are transformed into triples.
///
/// # Example
///
/// ```ignore
/// use fluree_db_api::R2rmlCreateConfig;
///
/// let config = R2rmlCreateConfig::new(
///     "airlines-rdf",
///     "https://polaris.example.com",
///     "openflights.airlines",
///     "fluree:file://mappings/airlines.ttl",
/// )
/// .with_auth_bearer("my-token");
///
/// let result = fluree.create_r2rml_vg(config).await?;
/// ```
#[cfg(feature = "iceberg")]
#[derive(Debug, Clone)]
pub struct R2rmlCreateConfig {
    /// Underlying Iceberg configuration
    pub iceberg: IcebergCreateConfig,

    /// R2RML mapping source (storage address or URL)
    pub mapping_source: String,

    /// R2RML mapping media type (optional, inferred from extension if omitted)
    pub mapping_media_type: Option<String>,
}

#[cfg(feature = "iceberg")]
impl R2rmlCreateConfig {
    /// Create a new R2RML VG config with minimal required fields.
    pub fn new(
        name: impl Into<String>,
        catalog_uri: impl Into<String>,
        table_identifier: impl Into<String>,
        mapping_source: impl Into<String>,
    ) -> Self {
        Self {
            iceberg: IcebergCreateConfig::new(name, catalog_uri, table_identifier),
            mapping_source: mapping_source.into(),
            mapping_media_type: None,
        }
    }

    /// Set the branch name.
    pub fn with_branch(mut self, branch: impl Into<String>) -> Self {
        self.iceberg = self.iceberg.with_branch(branch);
        self
    }

    /// Set bearer token authentication.
    pub fn with_auth_bearer(mut self, token: impl Into<String>) -> Self {
        self.iceberg = self.iceberg.with_auth_bearer(token);
        self
    }

    /// Set OAuth2 client credentials authentication.
    pub fn with_auth_oauth2(
        mut self,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Self {
        self.iceberg = self.iceberg.with_auth_oauth2(token_url, client_id, client_secret);
        self
    }

    /// Set the warehouse identifier.
    pub fn with_warehouse(mut self, warehouse: impl Into<String>) -> Self {
        self.iceberg = self.iceberg.with_warehouse(warehouse);
        self
    }

    /// Set the mapping media type (e.g., "text/turtle").
    pub fn with_mapping_media_type(mut self, media_type: impl Into<String>) -> Self {
        self.mapping_media_type = Some(media_type.into());
        self
    }

    /// Enable or disable vended credentials.
    pub fn with_vended_credentials(mut self, enabled: bool) -> Self {
        self.iceberg = self.iceberg.with_vended_credentials(enabled);
        self
    }

    /// Set S3 region.
    pub fn with_s3_region(mut self, region: impl Into<String>) -> Self {
        self.iceberg = self.iceberg.with_s3_region(region);
        self
    }

    /// Set S3 endpoint (for MinIO, LocalStack).
    pub fn with_s3_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.iceberg = self.iceberg.with_s3_endpoint(endpoint);
        self
    }

    /// Enable path-style S3 URLs.
    pub fn with_s3_path_style(mut self, enabled: bool) -> Self {
        self.iceberg = self.iceberg.with_s3_path_style(enabled);
        self
    }

    /// Get the virtual graph alias (name:branch).
    pub fn vg_alias(&self) -> String {
        self.iceberg.vg_alias()
    }

    /// Convert to the internal IcebergVgConfig structure with mapping for storage.
    pub fn to_iceberg_vg_config(&self) -> IcebergVgConfig {
        let mut config = self.iceberg.to_iceberg_vg_config();
        config.mapping = Some(fluree_db_iceberg::config::MappingSource {
            source: self.mapping_source.clone(),
            media_type: self.mapping_media_type.clone(),
        });
        config
    }

    /// Validate the configuration.
    pub fn validate(&self) -> crate::Result<()> {
        // Validate the underlying Iceberg config
        self.iceberg.validate()?;

        // Validate mapping source
        if self.mapping_source.trim().is_empty() {
            return Err(crate::ApiError::config("R2RML mapping source cannot be empty"));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_bm25_config_defaults() {
        let config = Bm25CreateConfig::new("search", "docs:main", json!({"select": ["?x"]}));

        assert_eq!(config.name, "search");
        assert_eq!(config.ledger, "docs:main");
        assert_eq!(config.effective_branch(), "main");
        assert_eq!(config.vg_alias(), "search:main");

        let bm25 = config.bm25_config();
        assert!((bm25.k1 - 1.2).abs() < 0.001);
        assert!((bm25.b - 0.75).abs() < 0.001);
    }

    #[test]
    fn test_bm25_config_with_options() {
        let config = Bm25CreateConfig::new("search", "docs:main", json!({}))
            .with_branch("dev")
            .with_k1(1.5)
            .with_b(0.5);

        assert_eq!(config.effective_branch(), "dev");
        assert_eq!(config.vg_alias(), "search:dev");

        let bm25 = config.bm25_config();
        assert!((bm25.k1 - 1.5).abs() < 0.001);
        assert!((bm25.b - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_bm25_config_validation_valid() {
        let config = Bm25CreateConfig::new("search", "docs:main", json!({"select": ["?x"]}));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_bm25_config_validation_empty_name() {
        let config = Bm25CreateConfig::new("", "docs:main", json!({"select": ["?x"]}));
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().to_string().contains("name"));
    }

    #[test]
    fn test_bm25_config_validation_name_with_colon() {
        let config = Bm25CreateConfig::new("search:index", "docs:main", json!({"select": ["?x"]}));
        assert!(config.validate().is_err());
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("colon") || err.contains("':'"));
    }

    #[test]
    fn test_bm25_config_validation_empty_ledger() {
        let config = Bm25CreateConfig::new("search", "", json!({"select": ["?x"]}));
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().to_string().contains("ledger"));
    }

    #[test]
    fn test_bm25_config_validation_negative_k1() {
        let config =
            Bm25CreateConfig::new("search", "docs:main", json!({"select": ["?x"]})).with_k1(-1.0);
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().to_string().contains("k1"));
    }

    #[test]
    fn test_bm25_config_validation_invalid_b() {
        let config =
            Bm25CreateConfig::new("search", "docs:main", json!({"select": ["?x"]})).with_b(1.5);
        assert!(config.validate().is_err());
        assert!(config.validate().unwrap_err().to_string().contains("b"));
    }

    #[test]
    fn test_bm25_config_validation_no_select() {
        let config = Bm25CreateConfig::new("search", "docs:main", json!({"where": []}));
        assert!(config.validate().is_err());
        assert!(config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("select"));
    }

    #[test]
    fn test_bm25_config_validation_select_one() {
        // selectOne is also valid
        let config = Bm25CreateConfig::new("search", "docs:main", json!({"selectOne": ["?x"]}));
        assert!(config.validate().is_ok());
    }
}
