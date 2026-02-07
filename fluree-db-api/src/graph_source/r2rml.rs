//! R2RML graph source operations and provider.
//!
//! This module provides APIs for creating R2RML graph sources and implements
//! the R2RML provider traits for query execution against Iceberg tables.
//!
//! This module is only available with the `iceberg` feature.

use crate::graph_source::cache::R2rmlCache;
use crate::graph_source::config::{IcebergCreateConfig, R2rmlCreateConfig};
use crate::graph_source::result::{IcebergCreateResult, R2rmlCreateResult};
use crate::Result;
use async_trait::async_trait;
use fluree_db_core::Storage;
use fluree_db_iceberg::{
    catalog::{RestCatalogClient, RestCatalogConfig, SendCatalogClient},
    io::{ColumnBatch, S3IcebergStorage, SendIcebergStorage, SendParquetReader},
    metadata::TableMetadata,
    scan::{ScanConfig, SendScanPlanner},
    IcebergGsConfig,
};
use fluree_db_nameservice::{GraphSourcePublisher, GraphSourceType, NameService, Publisher};
use fluree_db_query::error::{QueryError, Result as QueryResult};
use fluree_db_query::r2rml::{R2rmlProvider, R2rmlTableProvider};
use fluree_db_r2rml::mapping::CompiledR2rmlMapping;
use std::sync::Arc;
use tracing::{debug, info, warn};

// =============================================================================
// Iceberg/R2RML Graph Source Creation
// =============================================================================

impl<S, N> crate::Fluree<S, N>
where
    S: Storage + fluree_db_core::StorageWrite + Clone + 'static,
    N: NameService + Publisher + GraphSourcePublisher,
{
    /// Create an Iceberg graph source.
    ///
    /// This operation:
    /// 1. Validates the configuration
    /// 2. Optionally tests the catalog connection
    /// 3. Publishes the graph source record to the nameservice
    pub async fn create_iceberg_graph_source(
        &self,
        config: IcebergCreateConfig,
    ) -> Result<IcebergCreateResult> {
        let graph_source_address = config.graph_source_address();
        info!(
            graph_source_address = %graph_source_address,
            catalog_uri = %config.catalog_uri,
            table = %config.table_identifier,
            "Creating Iceberg graph source"
        );

        // 1. Validate configuration
        config.validate()?;

        // 2. Test catalog connection (optional but recommended)
        let connection_tested = self.test_iceberg_connection(&config).await.is_ok();
        if !connection_tested {
            warn!(
                graph_source_address = %graph_source_address,
                "Could not verify catalog connection - graph source will be created but may fail at query time"
            );
        }

        // 3. Convert config to storage format
        let iceberg_config = config.to_iceberg_gs_config();
        let config_json = iceberg_config
            .to_json()
            .map_err(|e| crate::ApiError::Config(format!("Failed to serialize config: {}", e)))?;

        // 4. Publish graph source record to nameservice
        self.nameservice
            .publish_graph_source(
                &config.name,
                config.effective_branch(),
                GraphSourceType::Iceberg,
                &config_json,
                &[], // No ledger dependencies for Iceberg graph sources
            )
            .await?;

        info!(
            graph_source_address = %graph_source_address,
            connection_tested = connection_tested,
            "Created Iceberg graph source"
        );

        Ok(IcebergCreateResult {
            graph_source_address,
            table_identifier: config.table_identifier.clone(),
            catalog_uri: config.catalog_uri.clone(),
            connection_tested,
        })
    }

    /// Create an R2RML graph source (Iceberg table with R2RML mapping).
    ///
    /// This operation:
    /// 1. Validates the configuration
    /// 2. Loads and validates the R2RML mapping
    /// 3. Optionally tests the catalog connection
    /// 4. Publishes the graph source record to the nameservice
    pub async fn create_r2rml_graph_source(
        &self,
        config: R2rmlCreateConfig,
    ) -> Result<R2rmlCreateResult> {
        let graph_source_address = config.graph_source_address();
        info!(
            graph_source_address = %graph_source_address,
            catalog_uri = %config.iceberg.catalog_uri,
            table = %config.iceberg.table_identifier,
            mapping = %config.mapping_source,
            "Creating R2RML graph source"
        );

        // 1. Validate configuration
        config.validate()?;

        // 2. Load and validate the R2RML mapping
        let (triples_map_count, mapping_validated) = self
            .validate_r2rml_mapping(&config)
            .await
            .map(|count| (count, true))
            .unwrap_or_else(|e| {
                warn!(
                    graph_source_address = %graph_source_address,
                    error = %e,
                    "Could not validate R2RML mapping - graph source will be created but may fail at query time"
                );
                (0, false)
            });

        // 3. Test catalog connection (optional but recommended)
        let connection_tested = self.test_iceberg_connection(&config.iceberg).await.is_ok();
        if !connection_tested {
            warn!(
                graph_source_address = %graph_source_address,
                "Could not verify catalog connection - graph source will be created but may fail at query time"
            );
        }

        // 4. Convert config to storage format
        let iceberg_config = config.to_iceberg_gs_config();
        let config_json = iceberg_config
            .to_json()
            .map_err(|e| crate::ApiError::Config(format!("Failed to serialize config: {}", e)))?;

        // 5. Publish graph source record to nameservice
        self.nameservice
            .publish_graph_source(
                &config.iceberg.name,
                config.iceberg.effective_branch(),
                GraphSourceType::Iceberg,
                &config_json,
                &[], // No ledger dependencies for Iceberg/R2RML graph sources
            )
            .await?;

        info!(
            graph_source_address = %graph_source_address,
            triples_map_count = triples_map_count,
            connection_tested = connection_tested,
            mapping_validated = mapping_validated,
            "Created R2RML graph source"
        );

        Ok(R2rmlCreateResult {
            graph_source_address,
            table_identifier: config.iceberg.table_identifier.clone(),
            catalog_uri: config.iceberg.catalog_uri.clone(),
            mapping_source: config.mapping_source.clone(),
            triples_map_count,
            connection_tested,
            mapping_validated,
        })
    }

    /// Test connection to an Iceberg catalog.
    ///
    /// This performs a lightweight connection test by attempting to load
    /// the table metadata from the catalog.
    async fn test_iceberg_connection(&self, config: &IcebergCreateConfig) -> Result<()> {
        use fluree_db_iceberg::catalog::parse_table_identifier;

        // Create auth provider
        let auth = config.auth.create_provider_arc().map_err(|e| {
            crate::ApiError::Config(format!("Failed to create auth provider: {}", e))
        })?;

        // Create catalog client
        let catalog_config = RestCatalogConfig {
            uri: config.catalog_uri.clone(),
            warehouse: config.warehouse.clone(),
            ..Default::default()
        };

        let catalog = RestCatalogClient::new(catalog_config, auth).map_err(|e| {
            crate::ApiError::Config(format!("Failed to create catalog client: {}", e))
        })?;

        // Parse table identifier
        let table_id = parse_table_identifier(&config.table_identifier)
            .map_err(|e| crate::ApiError::Config(format!("Invalid table identifier: {}", e)))?;

        // Attempt to load table metadata (this tests the connection)
        catalog
            .load_table(&table_id, config.vended_credentials)
            .await
            .map_err(|e| {
                crate::ApiError::Config(format!("Failed to load table from catalog: {}", e))
            })?;

        Ok(())
    }

    /// Validate an R2RML mapping by loading and compiling it.
    ///
    /// Returns the number of TriplesMap definitions in the mapping.
    async fn validate_r2rml_mapping(&self, config: &R2rmlCreateConfig) -> Result<usize> {
        // Load the mapping content from storage
        let mapping_bytes = self
            .storage()
            .read_bytes(&config.mapping_source)
            .await
            .map_err(|e| {
                crate::ApiError::Config(format!(
                    "Failed to load R2RML mapping from '{}': {}",
                    config.mapping_source, e
                ))
            })?;

        let mapping_content = String::from_utf8(mapping_bytes).map_err(|e| {
            crate::ApiError::Config(format!("R2RML mapping is not valid UTF-8: {}", e))
        })?;

        // Determine format by extension or media type
        let is_turtle = config.mapping_media_type.as_ref().map_or_else(
            || {
                config.mapping_source.ends_with(".ttl")
                    || config.mapping_source.ends_with(".turtle")
            },
            |mt| mt.contains("turtle"),
        );

        // Parse and compile the mapping
        let compiled = if is_turtle {
            fluree_db_r2rml::loader::R2rmlLoader::from_turtle(&mapping_content)
                .map_err(|e| {
                    crate::ApiError::Config(format!("Failed to parse R2RML Turtle: {}", e))
                })?
                .compile()
                .map_err(|e| {
                    crate::ApiError::Config(format!("Failed to compile R2RML mapping: {}", e))
                })?
        } else {
            return Err(crate::ApiError::Config(
                "R2RML mapping must be in Turtle format (.ttl). JSON-LD is not yet supported."
                    .to_string(),
            ));
        };

        Ok(compiled.len())
    }
}

// =============================================================================
// R2RML Provider Implementation
// =============================================================================

/// Provider for R2RML graph source query integration.
///
/// This provider implements the `R2rmlProvider` and `R2rmlTableProvider` traits
/// required by the query engine to execute R2RML-backed queries against
/// Iceberg tables.
///
/// # Usage
///
/// ```ignore
/// use fluree_db_api::FlureeR2rmlProvider;
///
/// let provider = FlureeR2rmlProvider::new(&fluree);
/// let ctx = ExecutionContext::new(&db, &vars)
///     .with_r2rml_providers(&provider, &provider);
/// ```
pub struct FlureeR2rmlProvider<'a, S: Storage + 'static, N> {
    fluree: &'a crate::Fluree<S, N>,
}

impl<'a, S: Storage + 'static, N> FlureeR2rmlProvider<'a, S, N> {
    /// Create a new R2RML provider wrapping a Fluree instance.
    pub fn new(fluree: &'a crate::Fluree<S, N>) -> Self {
        Self { fluree }
    }
}

impl<S: Storage + 'static, N> std::fmt::Debug for FlureeR2rmlProvider<'_, S, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlureeR2rmlProvider")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<S, N> R2rmlProvider for FlureeR2rmlProvider<'_, S, N>
where
    S: Storage + Clone + 'static,
    N: NameService + GraphSourcePublisher,
{
    /// Check if a graph source has an R2RML mapping.
    async fn has_r2rml_mapping(&self, graph_source_address: &str) -> bool {
        match self
            .fluree
            .nameservice()
            .lookup_graph_source(graph_source_address)
            .await
        {
            Ok(Some(record)) => {
                // First check if this is an R2RML or Iceberg graph source type
                if !matches!(
                    record.source_type,
                    GraphSourceType::R2rml | GraphSourceType::Iceberg
                ) {
                    return false;
                }

                // Parse into typed config to stay aligned with real config schema
                if let Ok(config) = IcebergGsConfig::from_json(&record.config) {
                    config.mapping.is_some()
                } else {
                    false
                }
            }
            Ok(None) => false,
            Err(_) => false,
        }
    }

    /// Get the compiled R2RML mapping for a graph source.
    ///
    /// This method uses the R2RML cache to avoid repeated parsing and compilation.
    async fn compiled_mapping(
        &self,
        graph_source_address: &str,
        _as_of_t: Option<i64>,
    ) -> QueryResult<Arc<CompiledR2rmlMapping>> {
        // Look up the graph source record
        let record = self
            .fluree
            .nameservice()
            .lookup_graph_source(graph_source_address)
            .await
            .map_err(|e| QueryError::Internal(format!("Nameservice error: {}", e)))?
            .ok_or_else(|| {
                QueryError::InvalidQuery(format!(
                    "Graph source '{}' not found",
                    graph_source_address
                ))
            })?;

        // Verify it's an R2RML or Iceberg graph source
        if !matches!(
            record.source_type,
            GraphSourceType::R2rml | GraphSourceType::Iceberg
        ) {
            return Err(QueryError::InvalidQuery(format!(
                "Graph source '{}' is not an R2RML graph source (type: {:?})",
                graph_source_address, record.source_type
            )));
        }

        // Parse into typed config
        let iceberg_config = IcebergGsConfig::from_json(&record.config).map_err(|e| {
            QueryError::Internal(format!(
                "Failed to parse graph source config for '{}': {}",
                graph_source_address, e
            ))
        })?;

        let mapping_config = iceberg_config.mapping.as_ref().ok_or_else(|| {
            QueryError::InvalidQuery(format!(
                "Graph source '{}' is missing 'mapping' in config",
                graph_source_address
            ))
        })?;

        let mapping_source = &mapping_config.source;
        let media_type = mapping_config.media_type.as_deref();

        // Check cache first
        let cache = self.fluree.r2rml_cache();
        let cache_key =
            R2rmlCache::mapping_cache_key(graph_source_address, mapping_source, media_type);

        if let Some(cached) = cache.get_mapping(&cache_key).await {
            debug!(
                graph_source_address = %graph_source_address,
                cache_key = %cache_key,
                "R2RML mapping cache hit"
            );
            return Ok(cached);
        }

        debug!(
            graph_source_address = %graph_source_address,
            cache_key = %cache_key,
            "R2RML mapping cache miss - loading from storage"
        );

        // Cache miss - load the mapping content
        let mapping_bytes = self
            .fluree
            .storage()
            .read_bytes(mapping_source)
            .await
            .map_err(|e| {
                QueryError::InvalidQuery(format!(
                    "Failed to load R2RML mapping from '{}': {}",
                    mapping_source, e
                ))
            })?;

        let mapping_content = String::from_utf8(mapping_bytes).map_err(|e| {
            QueryError::InvalidQuery(format!(
                "R2RML mapping at '{}' is not valid UTF-8: {}",
                mapping_source, e
            ))
        })?;

        // Parse and compile the mapping
        let media_type = mapping_config.media_type.as_deref();

        let is_turtle = media_type.map_or_else(
            || mapping_source.ends_with(".ttl") || mapping_source.ends_with(".turtle"),
            |mt| mt.contains("turtle"),
        );

        let compiled = if is_turtle {
            fluree_db_r2rml::loader::R2rmlLoader::from_turtle(&mapping_content)
                .map_err(|e| {
                    QueryError::InvalidQuery(format!(
                        "Failed to parse R2RML Turtle from '{}': {}",
                        mapping_source, e
                    ))
                })?
                .compile()
                .map_err(|e| {
                    QueryError::InvalidQuery(format!(
                        "Failed to compile R2RML mapping from '{}': {}",
                        mapping_source, e
                    ))
                })?
        } else {
            return Err(QueryError::InvalidQuery(format!(
                "R2RML mapping for '{}' uses JSON-LD format, which is not yet supported. \
                 Please use Turtle format (.ttl).",
                graph_source_address
            )));
        };

        let compiled = Arc::new(compiled);

        // Cache the compiled mapping
        cache
            .put_mapping(cache_key.clone(), Arc::clone(&compiled))
            .await;

        info!(
            graph_source_address = %graph_source_address,
            cache_key = %cache_key,
            triples_maps = compiled.triples_maps.len(),
            "Loaded, compiled, and cached R2RML mapping"
        );

        Ok(compiled)
    }
}

#[async_trait]
impl<S, N> R2rmlTableProvider for FlureeR2rmlProvider<'_, S, N>
where
    S: Storage + Clone + 'static,
    N: NameService + GraphSourcePublisher,
{
    /// Scan an Iceberg table and return column batches.
    ///
    /// This method connects to the Iceberg catalog, executes a scan with
    /// the specified projection, and returns the results as column batches.
    async fn scan_table(
        &self,
        graph_source_address: &str,
        table_name: &str,
        projection: &[String],
        _as_of_t: Option<i64>,
    ) -> QueryResult<Vec<ColumnBatch>> {
        // Look up the graph source record to get Iceberg connection info
        let record = self
            .fluree
            .nameservice()
            .lookup_graph_source(graph_source_address)
            .await
            .map_err(|e| QueryError::Internal(format!("Nameservice error: {}", e)))?
            .ok_or_else(|| {
                QueryError::InvalidQuery(format!(
                    "Graph source '{}' not found",
                    graph_source_address
                ))
            })?;

        // Parse the Iceberg graph source config
        let iceberg_config = IcebergGsConfig::from_json(&record.config).map_err(|e| {
            QueryError::Internal(format!(
                "Failed to parse Iceberg graph source config for '{}': {}",
                graph_source_address, e
            ))
        })?;

        // Validate the config
        iceberg_config.validate().map_err(|e| {
            QueryError::InvalidQuery(format!(
                "Invalid Iceberg graph source config for '{}': {}",
                graph_source_address, e
            ))
        })?;

        info!(
            graph_source_address = %graph_source_address,
            table_name = %table_name,
            catalog_uri = %iceberg_config.catalog.uri,
            projection = ?projection,
            "Starting Iceberg table scan"
        );

        // Create auth provider
        let auth = iceberg_config
            .catalog
            .auth
            .create_provider_arc()
            .map_err(|e| QueryError::Internal(format!("Failed to create auth provider: {}", e)))?;

        // Create REST catalog client
        let catalog_config = RestCatalogConfig {
            uri: iceberg_config.catalog.uri.clone(),
            warehouse: iceberg_config.catalog.warehouse.clone(),
            ..Default::default()
        };

        let catalog = RestCatalogClient::new(catalog_config, auth)
            .map_err(|e| QueryError::Internal(format!("Failed to create catalog client: {}", e)))?;

        // Parse the table identifier
        use fluree_db_iceberg::catalog::parse_table_identifier;
        let table_id = if table_name.is_empty() {
            iceberg_config.table_identifier().map_err(|e| {
                QueryError::Internal(format!("Failed to parse table identifier: {}", e))
            })?
        } else {
            parse_table_identifier(table_name).map_err(|e| {
                QueryError::Internal(format!(
                    "Failed to parse table identifier '{}': {}",
                    table_name, e
                ))
            })?
        };

        info!(
            namespace = %table_id.namespace,
            table = %table_id.table,
            "Loading table from catalog"
        );

        // Load table metadata from catalog
        let load_response = catalog
            .load_table(&table_id, iceberg_config.io.vended_credentials)
            .await
            .map_err(|e| {
                QueryError::Internal(format!("Failed to load table from catalog: {}", e))
            })?;

        info!(
            metadata_location = %load_response.metadata_location,
            has_credentials = load_response.credentials.is_some(),
            "Loaded table metadata location"
        );

        // Create S3 storage
        let storage = if let Some(credentials) = load_response.credentials {
            info!("Using vended credentials from catalog");
            S3IcebergStorage::from_vended_credentials(&credentials)
                .await
                .map_err(|e| QueryError::Internal(format!("Failed to create S3 storage: {}", e)))?
        } else {
            info!(
                region = ?iceberg_config.io.s3_region,
                endpoint = ?iceberg_config.io.s3_endpoint,
                "Using ambient AWS credentials"
            );
            S3IcebergStorage::from_default_chain(
                iceberg_config.io.s3_region.as_deref(),
                iceberg_config.io.s3_endpoint.as_deref(),
                iceberg_config.io.s3_path_style,
            )
            .await
            .map_err(|e| QueryError::Internal(format!("Failed to create S3 storage: {}", e)))?
        };

        // Check cache for table metadata
        let cache = self.fluree.r2rml_cache();
        let metadata_location = &load_response.metadata_location;

        let metadata = if let Some(cached) = cache.get_metadata(metadata_location).await {
            debug!(metadata_location = %metadata_location, "Table metadata cache hit");
            cached
        } else {
            debug!(metadata_location = %metadata_location, "Table metadata cache miss");

            let metadata_bytes = storage.read(metadata_location).await.map_err(|e| {
                QueryError::Internal(format!("Failed to read table metadata: {}", e))
            })?;

            let parsed = TableMetadata::from_json(&metadata_bytes).map_err(|e| {
                QueryError::Internal(format!("Failed to parse table metadata: {}", e))
            })?;

            let metadata = Arc::new(parsed);
            cache
                .put_metadata(metadata_location.clone(), Arc::clone(&metadata))
                .await;

            info!(
                metadata_location = %metadata_location,
                format_version = metadata.format_version,
                "Loaded and cached table metadata"
            );

            metadata
        };

        let schema = metadata
            .current_schema()
            .ok_or_else(|| QueryError::Internal("Table has no current schema".to_string()))?;

        info!(
            format_version = metadata.format_version,
            schema_id = schema.schema_id,
            field_count = schema.fields.len(),
            "Parsed table metadata"
        );

        // Resolve column names to field IDs for projection
        let projected_field_ids: Vec<i32> = if projection.is_empty() {
            schema
                .fields
                .iter()
                .filter(|f| !f.is_nested())
                .map(|f| f.id)
                .collect()
        } else {
            projection
                .iter()
                .filter_map(|col_name| schema.field_by_name(col_name).map(|f| f.id))
                .collect()
        };

        if projected_field_ids.is_empty() && !projection.is_empty() {
            return Err(QueryError::InvalidQuery(format!(
                "None of the projected columns {:?} exist in table schema. Available: {:?}",
                projection,
                schema.field_names()
            )));
        }

        // Create scan configuration with projection
        let scan_config = ScanConfig::new().with_projection(projected_field_ids.clone());

        // Create scan planner and generate scan plan
        let planner = SendScanPlanner::new(&storage, &metadata, scan_config);
        let plan = planner
            .plan_scan()
            .await
            .map_err(|e| QueryError::Internal(format!("Failed to plan scan: {}", e)))?;

        info!(
            files_selected = plan.files_selected,
            files_pruned = plan.files_pruned,
            estimated_rows = plan.estimated_row_count,
            "Scan plan created"
        );

        if plan.is_empty() {
            info!("Scan plan has no files - returning empty result");
            return Ok(Vec::new());
        }

        // Read data files
        let reader = SendParquetReader::new(&storage);
        let mut all_batches = Vec::new();

        for task in &plan.tasks {
            info!(
                file_path = %task.data_file.file_path,
                record_count = task.data_file.record_count,
                "Reading Parquet file"
            );

            let batches = reader.read_task(task).await.map_err(|e| {
                QueryError::Internal(format!(
                    "Failed to read Parquet file '{}': {}",
                    task.data_file.file_path, e
                ))
            })?;

            all_batches.extend(batches);
        }

        info!(
            total_batches = all_batches.len(),
            total_rows = all_batches.iter().map(|b| b.num_rows).sum::<usize>(),
            "Iceberg scan complete"
        );

        Ok(all_batches)
    }
}
