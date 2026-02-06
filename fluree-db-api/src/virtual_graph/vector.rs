//! Embedded vector similarity search index operations.
//!
//! This module provides APIs for creating, loading, syncing, and dropping
//! embedded vector similarity search indexes.

#[cfg(feature = "vector")]
use crate::virtual_graph::config::VectorCreateConfig;
#[cfg(feature = "vector")]
use crate::virtual_graph::helpers::{
    expand_ids_in_results, expand_prefixed_iri, expand_properties_in_results, extract_prefix_map,
};
#[cfg(feature = "vector")]
use crate::virtual_graph::result::{
    VectorCreateResult, VectorDropResult, VectorStalenessCheck, VectorSyncResult,
};
#[cfg(feature = "vector")]
use crate::{QueryResult as ApiQueryResult, Result};
#[cfg(feature = "vector")]
use fluree_db_core::{alias as core_alias, Storage, StorageWrite};
#[cfg(feature = "vector")]
use fluree_db_ledger::LedgerState;
#[cfg(feature = "vector")]
use fluree_db_nameservice::{NameService, Publisher, VgType, VirtualGraphPublisher};
#[cfg(feature = "vector")]
use fluree_db_query::parse::parse_query;
#[cfg(feature = "vector")]
use fluree_db_query::vector::usearch::{
    IncrementalVectorUpdater, VectorIndex, VectorIndexBuilder, VectorPropertyDeps,
};
#[cfg(feature = "vector")]
use fluree_db_query::{execute_with_overlay, DataSource, ExecutableQuery, SelectMode, VarRegistry};
#[cfg(feature = "vector")]
use serde_json::Value as JsonValue;
#[cfg(feature = "vector")]
use std::collections::HashSet;
#[cfg(feature = "vector")]
use std::sync::Arc;
#[cfg(feature = "vector")]
use tracing::{info, warn};

// =============================================================================
// Vector Index Creation
// =============================================================================

#[cfg(feature = "vector")]
impl<S, N> crate::Fluree<S, N>
where
    S: Storage + StorageWrite + Clone + 'static,
    N: NameService + Publisher + VirtualGraphPublisher,
{
    /// Create a vector similarity search index.
    ///
    /// This operation:
    /// 1. Loads the source ledger
    /// 2. Executes the indexing query to get documents
    /// 3. Extracts embedding vectors from each document
    /// 4. Builds the vector index using usearch HNSW
    /// 5. Persists the index snapshot to storage
    /// 6. Publishes the VG record to the nameservice
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration specifying the index name, source ledger, query, and embedding property
    ///
    /// # Returns
    ///
    /// Result containing the created index metadata
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
    ///         "where": [{"@id": "?x", "@type": "Article"}],
    ///         "select": {"?x": ["@id", "embedding"]}
    ///     }),
    ///     "embedding",
    ///     768,
    /// ).with_metric(DistanceMetric::Cosine);
    ///
    /// let result = fluree.create_vector_index(config).await?;
    /// ```
    pub async fn create_vector_index(
        &self,
        config: VectorCreateConfig,
    ) -> Result<VectorCreateResult> {
        let vg_alias = config.vg_alias();
        info!(
            vg_alias = %vg_alias,
            ledger = %config.ledger,
            dimensions = config.dimensions,
            "Creating vector similarity search index"
        );

        // Check if VG already exists (prevent duplicates)
        if let Some(existing) = self.nameservice.lookup_vg(&vg_alias).await? {
            if !existing.retracted {
                return Err(crate::ApiError::Config(format!(
                    "Virtual graph '{}' already exists",
                    vg_alias
                )));
            }
        }

        // 1. Load source ledger
        let ledger = self.ledger(&config.ledger).await?;
        let source_t = ledger.t();

        info!(
            ledger = %config.ledger,
            t = source_t,
            "Loaded source ledger"
        );

        // 2. Execute indexing query
        let results = self
            .execute_vector_indexing_query(&ledger, &config.query)
            .await?;

        info!(result_count = results.len(), "Executed indexing query");

        // 2b. Expand prefixed IRIs in @id fields and property names to full IRIs
        let context = config
            .query
            .get("@context")
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let prefix_map = extract_prefix_map(&context);
        let results = expand_ids_in_results(results, &prefix_map);
        let results = expand_properties_in_results(results, &prefix_map);

        // 2c. Expand embedding property name if prefixed
        let embedding_property = expand_prefixed_iri(&config.embedding_property, &prefix_map)
            .unwrap_or_else(|| config.embedding_property.clone());

        // 3. Build vector index
        let property_deps = VectorPropertyDeps::from_query(&embedding_property, &config.query);
        let metric = config
            .metric
            .unwrap_or(fluree_db_query::vector::DistanceMetric::Cosine);

        let mut builder =
            VectorIndexBuilder::new(config.ledger.as_str(), config.dimensions, metric)?
                .with_embedding_property(&embedding_property)
                .with_property_deps(property_deps)
                .with_watermark(source_t);

        // Reserve capacity if we know the result count
        if !results.is_empty() {
            builder = builder.with_capacity(results.len())?;
        }

        for result in &results {
            builder.add_result(result)?;
        }

        let vector_count = builder.indexed_count();
        let skipped_count = builder.skipped_count();
        let index = builder.build();

        info!(
            vector_count = vector_count,
            skipped_count = skipped_count,
            dimensions = config.dimensions,
            "Built vector index"
        );

        // 4. Persist index snapshot (versioned for time-travel support)
        let index_address = self
            .persist_vector_index_versioned(&vg_alias, &index, source_t)
            .await?;

        info!(
            index_address = %index_address,
            index_t = source_t,
            "Persisted versioned index snapshot"
        );

        // 5. Publish VG record to nameservice
        let config_json = serde_json::to_string(&serde_json::json!({
            "embedding_property": config.embedding_property,
            "dimensions": config.dimensions,
            "metric": format!("{:?}", metric),
            "query": config.query,
            "usearch": {
                "connectivity": config.connectivity,
                "expansion_add": config.expansion_add,
                "expansion_search": config.expansion_search,
            }
        }))?;

        self.nameservice
            .publish_vg(
                &config.name,
                config.effective_branch(),
                VgType::Vector,
                &config_json,
                std::slice::from_ref(&config.ledger),
            )
            .await?;

        // Publish index location and watermark
        self.nameservice
            .publish_vg_index(
                &config.name,
                config.effective_branch(),
                &index_address,
                source_t,
            )
            .await?;

        info!(
            vg_alias = %vg_alias,
            vector_count = vector_count,
            index_t = source_t,
            "Created vector similarity search index"
        );

        Ok(VectorCreateResult {
            vg_alias,
            vector_count,
            skipped_count,
            dimensions: config.dimensions,
            index_t: source_t,
            index_address: Some(index_address),
        })
    }

    /// Execute the indexing query and return JSON-LD results.
    ///
    /// Executes the query and formats results as JSON-LD objects suitable for indexing.
    /// Each result object will have an `@id` field identifying the document.
    pub(crate) async fn execute_vector_indexing_query(
        &self,
        ledger: &LedgerState<S>,
        query_json: &JsonValue,
    ) -> Result<Vec<JsonValue>> {
        // Parse the query
        let mut vars = VarRegistry::new();
        let parsed = parse_query(query_json, &ledger.db, &mut vars)?;

        // Execute with a wildcard select so the operator pipeline does not project away
        // bindings we need for indexing
        let mut parsed_for_exec = parsed.clone();
        parsed_for_exec.select_mode = SelectMode::Wildcard;
        parsed_for_exec.select.clear();
        parsed_for_exec.graph_select = None;

        let executable = ExecutableQuery::simple(parsed_for_exec);

        let source = DataSource::new(&ledger.db, ledger.novelty.as_ref(), ledger.t());
        let batches = execute_with_overlay(source, &vars, &executable).await?;

        // Format using the standard JSON-LD formatter
        let result = ApiQueryResult {
            vars,
            t: ledger.t(),
            novelty: Some(ledger.novelty.clone()),
            context: parsed.context,
            orig_context: parsed.orig_context,
            select: parsed.select,
            select_mode: parsed.select_mode,
            batches,
            binary_store: None,
            construct_template: parsed.construct_template,
            graph_select: parsed.graph_select,
        };

        let json = result.to_jsonld_async(&ledger.db).await?;
        match json {
            JsonValue::Array(arr) => Ok(arr),
            JsonValue::Object(_) => Ok(vec![json]),
            _ => Ok(Vec::new()),
        }
    }

    /// Persist a vector index snapshot to storage with versioned path.
    ///
    /// Creates a snapshot at `virtual-graphs/{name}/{branch}/vector/t{index_t}/snapshot.bin`
    /// and publishes the snapshot to the history file for time-travel support.
    pub(crate) async fn persist_vector_index_versioned(
        &self,
        vg_alias: &str,
        index: &VectorIndex,
        index_t: i64,
    ) -> Result<String> {
        use fluree_db_query::vector::usearch::serialize;

        // Serialize the index
        let bytes = serialize(index)?;

        // Build versioned storage address
        let (name, branch) = core_alias::split_alias(vg_alias).map_err(|e| {
            crate::ApiError::config(format!("Invalid virtual graph alias '{}': {}", vg_alias, e))
        })?;
        let address = format!(
            "fluree:file://virtual-graphs/{}/{}/vector/t{}/snapshot.bin",
            name, branch, index_t
        );

        // Write to storage
        self.storage().write_bytes(&address, &bytes).await?;

        // Publish snapshot to history (for time-travel selection)
        self.nameservice
            .publish_vg_snapshot(&name, &branch, &address, index_t)
            .await?;

        Ok(address)
    }
}

// =============================================================================
// Vector Index Loading (for queries)
// =============================================================================

#[cfg(feature = "vector")]
impl<S, N> crate::Fluree<S, N>
where
    S: Storage + Clone + 'static,
    N: NameService + VirtualGraphPublisher,
{
    /// Load a vector index for a specific `as_of_t` using snapshot selection.
    ///
    /// This is the time-travel aware version of `load_vector_index`.
    pub async fn load_vector_index_at(
        &self,
        vg_alias: &str,
        as_of_t: i64,
    ) -> Result<(Arc<VectorIndex>, i64)> {
        use fluree_db_query::vector::usearch::deserialize;

        // Look up snapshot history
        let history = self.nameservice.lookup_vg_snapshots(vg_alias).await?;

        // Select appropriate snapshot
        let selection = history.select_snapshot(as_of_t).ok_or_else(|| {
            crate::ApiError::NotFound(format!(
                "No vector snapshot available for {} at t={}",
                vg_alias, as_of_t
            ))
        })?;

        // Load from storage
        let bytes = self.storage().read_bytes(&selection.index_address).await?;

        // Deserialize
        let index = deserialize(&bytes)?;

        Ok((Arc::new(index), selection.index_t))
    }

    /// Load a vector index from storage (head snapshot).
    ///
    /// For time-travel queries, use `load_vector_index_at` instead.
    pub async fn load_vector_index(&self, vg_alias: &str) -> Result<Arc<VectorIndex>> {
        use fluree_db_query::vector::usearch::deserialize;

        // Look up VG record
        let record = self.nameservice.lookup_vg(vg_alias).await?.ok_or_else(|| {
            crate::ApiError::NotFound(format!("Virtual graph not found: {}", vg_alias))
        })?;

        // Get index address
        let index_address = record.index_address.ok_or_else(|| {
            crate::ApiError::NotFound(format!("No index for virtual graph: {}", vg_alias))
        })?;

        // Load from storage
        let bytes = self.storage().read_bytes(&index_address).await?;

        // Deserialize
        let index = deserialize(&bytes)?;

        Ok(Arc::new(index))
    }

    /// Check if a vector index is stale relative to its source ledger.
    ///
    /// This is a lightweight check that only looks up nameservice records.
    pub async fn check_vector_staleness(&self, vg_alias: &str) -> Result<VectorStalenessCheck> {
        // Look up VG record
        let record = self.nameservice.lookup_vg(vg_alias).await?.ok_or_else(|| {
            crate::ApiError::NotFound(format!("Virtual graph not found: {}", vg_alias))
        })?;

        // Get source ledger from dependencies
        let source_ledger = record
            .dependencies
            .first()
            .ok_or_else(|| crate::ApiError::Config("VG has no source ledger".to_string()))?
            .clone();

        // Check minimum head across all dependencies
        let mut ledger_t: Option<i64> = None;
        for dep in &record.dependencies {
            let ledger_record = self.nameservice.lookup(dep).await?.ok_or_else(|| {
                crate::ApiError::NotFound(format!("Source ledger not found: {}", dep))
            })?;
            ledger_t = Some(match ledger_t {
                Some(cur) => cur.min(ledger_record.commit_t),
                None => ledger_record.commit_t,
            });
        }
        let ledger_t = ledger_t.unwrap_or(0);

        let index_t = record.index_t;
        let is_stale = index_t < ledger_t;
        let lag = ledger_t - index_t;

        Ok(VectorStalenessCheck {
            vg_alias: vg_alias.to_string(),
            source_ledger,
            index_t,
            ledger_t,
            is_stale,
            lag,
        })
    }
}

// =============================================================================
// Vector Index Sync (Maintenance)
// =============================================================================

#[cfg(feature = "vector")]
impl<S, N> crate::Fluree<S, N>
where
    S: Storage + StorageWrite + Clone + 'static,
    N: NameService + Publisher + VirtualGraphPublisher,
{
    /// Sync a vector index to catch up with ledger updates.
    ///
    /// This operation performs incremental updates when possible,
    /// falling back to full resync if needed.
    pub async fn sync_vector_index(&self, vg_alias: &str) -> Result<VectorSyncResult> {
        use fluree_db_novelty::trace_commits;
        use fluree_db_query::bm25::CompiledPropertyDeps;
        use fluree_db_query::vector::usearch::{deserialize, serialize};
        use futures::StreamExt;

        info!(vg_alias = %vg_alias, "Starting vector index sync");

        // 1. Look up VG record to get config and index address
        let record = self.nameservice.lookup_vg(vg_alias).await?.ok_or_else(|| {
            crate::ApiError::NotFound(format!("Virtual graph not found: {}", vg_alias))
        })?;

        // Check if VG has been dropped
        if record.retracted {
            return Err(crate::ApiError::Drop(format!(
                "Cannot sync retracted virtual graph: {}",
                vg_alias
            )));
        }

        let index_address = match &record.index_address {
            Some(addr) => addr.clone(),
            None => {
                // No index yet - need full resync
                return self.resync_vector_index(vg_alias).await;
            }
        };

        // Parse config to get query
        let config: JsonValue = serde_json::from_str(&record.config)?;
        let query = config
            .get("query")
            .cloned()
            .unwrap_or(serde_json::json!({}));

        // Get source ledger alias from dependencies
        let source_ledger_alias = record
            .dependencies
            .first()
            .ok_or_else(|| crate::ApiError::Config("VG has no source ledger".to_string()))?
            .clone();

        // 2. Load source ledger to get current state
        let ledger = self.ledger(&source_ledger_alias).await?;
        let ledger_t = ledger.t();

        // 3. Load existing index
        let bytes = self.storage().read_bytes(&index_address).await?;
        let mut index = deserialize(&bytes)?;
        let old_watermark = index.watermark.get(&source_ledger_alias).unwrap_or(0);

        // Already up to date?
        if ledger_t <= old_watermark {
            info!(vg_alias = %vg_alias, ledger_t = ledger_t, "Index already up to date");
            return Ok(VectorSyncResult {
                vg_alias: vg_alias.to_string(),
                upserted: 0,
                removed: 0,
                skipped: 0,
                old_watermark,
                new_watermark: old_watermark,
                was_full_resync: false,
            });
        }

        // 4. Get head commit address for tracing
        let head_commit_address = ledger
            .ns_record
            .as_ref()
            .and_then(|r| r.commit_address.clone())
            .ok_or_else(|| crate::ApiError::NotFound("No commit address for ledger".to_string()))?;

        // 5. Compile property deps for this ledger's namespace
        // Convert VectorPropertyDeps to PropertyDeps for compilation
        let bm25_property_deps = index.property_deps.query_deps.clone();
        let compiled_deps = CompiledPropertyDeps::compile(&bm25_property_deps, |iri: &str| {
            ledger.db.encode_iri(iri)
        });

        // 6. Trace commits and collect affected subjects
        let mut affected_sids: HashSet<fluree_db_core::Sid> = HashSet::new();
        let stream = trace_commits(
            self.storage().clone(),
            head_commit_address.clone(),
            old_watermark,
        );
        futures::pin_mut!(stream);

        while let Some(result) = stream.next().await {
            let commit = result?;
            let subjects = compiled_deps.affected_subjects(&commit.flakes);
            affected_sids.extend(subjects);
        }

        // If no subjects affected, fall back to full resync
        if affected_sids.is_empty() {
            warn!(
                vg_alias = %vg_alias,
                old_watermark = old_watermark,
                ledger_t = ledger_t,
                "No affected subjects detected, falling back to full resync"
            );
            return self.resync_vector_index(vg_alias).await;
        }

        // 7. Convert affected Sids to IRIs
        let affected_iris: HashSet<Arc<str>> = affected_sids
            .into_iter()
            .filter_map(|sid| ledger.db.decode_sid(&sid).map(|s| Arc::from(s.as_str())))
            .collect();

        info!(
            vg_alias = %vg_alias,
            affected_count = affected_iris.len(),
            "Found affected subjects for incremental update"
        );

        // 8. Re-run indexing query and filter to affected subjects
        let results = self.execute_vector_indexing_query(&ledger, &query).await?;

        // Expand prefix map for @id fields and property names
        let context = query
            .get("@context")
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let prefix_map = extract_prefix_map(&context);
        let results = expand_ids_in_results(results, &prefix_map);
        let results = expand_properties_in_results(results, &prefix_map);

        let mut affected_iris_expanded = affected_iris.clone();
        for full_iri in &affected_iris {
            for (prefix, ns) in &prefix_map {
                if full_iri.starts_with(ns.as_str()) {
                    let local = &full_iri[ns.len()..];
                    let prefixed = format!("{}:{}", prefix, local);
                    affected_iris_expanded.insert(Arc::from(prefixed));
                }
            }
        }

        // 9. Apply incremental update
        let mut updater = IncrementalVectorUpdater::new(source_ledger_alias.as_str(), &mut index);
        let update_result = updater.apply_update(&results, &affected_iris_expanded, ledger_t);

        info!(
            vg_alias = %vg_alias,
            upserted = update_result.upserted,
            removed = update_result.removed,
            skipped = update_result.skipped,
            "Applied incremental update"
        );

        // 10. Persist updated index
        let (name, branch) = core_alias::split_alias(vg_alias).map_err(|e| {
            crate::ApiError::config(format!("Invalid virtual graph alias '{}': {}", vg_alias, e))
        })?;
        let new_address = format!(
            "fluree:file://virtual-graphs/{}/{}/vector/t{}/snapshot.bin",
            name, branch, ledger_t
        );
        let bytes = serialize(&index)?;
        self.storage().write_bytes(&new_address, &bytes).await?;

        // 11. Publish snapshot to history
        self.nameservice
            .publish_vg_snapshot(&name, &branch, &new_address, ledger_t)
            .await?;

        // 12. Update VG index record (head pointer)
        self.nameservice
            .publish_vg_index(&name, &branch, &new_address, ledger_t)
            .await?;

        info!(
            vg_alias = %vg_alias,
            new_address = %new_address,
            ledger_t = ledger_t,
            "Persisted synced vector index"
        );

        Ok(VectorSyncResult {
            vg_alias: vg_alias.to_string(),
            upserted: update_result.upserted,
            removed: update_result.removed,
            skipped: update_result.skipped,
            old_watermark,
            new_watermark: ledger_t,
            was_full_resync: false,
        })
    }

    /// Full resync of a vector index.
    ///
    /// Rebuilds the entire index from scratch by re-running the indexing query.
    pub async fn resync_vector_index(&self, vg_alias: &str) -> Result<VectorSyncResult> {
        use fluree_db_query::vector::usearch::{deserialize, serialize};

        info!(vg_alias = %vg_alias, "Starting full vector index resync");

        // 1. Look up VG record
        let record = self.nameservice.lookup_vg(vg_alias).await?.ok_or_else(|| {
            crate::ApiError::NotFound(format!("Virtual graph not found: {}", vg_alias))
        })?;

        if record.retracted {
            return Err(crate::ApiError::Drop(format!(
                "Cannot resync retracted virtual graph: {}",
                vg_alias
            )));
        }

        // Parse config
        let config: JsonValue = serde_json::from_str(&record.config)?;
        let query = config
            .get("query")
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let embedding_property = config
            .get("embedding_property")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                crate::ApiError::Config("Missing embedding_property in VG config".to_string())
            })?;
        let dimensions = config
            .get("dimensions")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| crate::ApiError::Config("Missing dimensions in VG config".to_string()))?
            as usize;

        // Get source ledger
        let source_ledger_alias = record
            .dependencies
            .first()
            .ok_or_else(|| crate::ApiError::Config("VG has no source ledger".to_string()))?
            .clone();

        // 2. Load source ledger
        let ledger = self.ledger(&source_ledger_alias).await?;
        let ledger_t = ledger.t();

        // 3. Load existing index to get old watermark
        let old_watermark = if let Some(addr) = &record.index_address {
            let bytes = self.storage().read_bytes(addr).await?;
            let old_index = deserialize(&bytes)?;
            old_index.watermark.get(&source_ledger_alias).unwrap_or(0)
        } else {
            0
        };

        // 4. Execute indexing query
        let results = self.execute_vector_indexing_query(&ledger, &query).await?;

        // Expand prefix map for @id fields and property names
        let context = query
            .get("@context")
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let prefix_map = extract_prefix_map(&context);
        let results = expand_ids_in_results(results, &prefix_map);
        let results = expand_properties_in_results(results, &prefix_map);

        // Expand embedding property name if prefixed
        let embedding_property_expanded = expand_prefixed_iri(embedding_property, &prefix_map)
            .unwrap_or_else(|| embedding_property.to_string());

        // 5. Build new index
        let property_deps = VectorPropertyDeps::from_query(&embedding_property_expanded, &query);

        // Parse metric from config
        let metric_str = config
            .get("metric")
            .and_then(|v| v.as_str())
            .unwrap_or("Cosine");
        let metric = match metric_str {
            "Dot" => fluree_db_query::vector::DistanceMetric::Dot,
            "Euclidean" => fluree_db_query::vector::DistanceMetric::Euclidean,
            _ => fluree_db_query::vector::DistanceMetric::Cosine,
        };

        let mut builder =
            VectorIndexBuilder::new(source_ledger_alias.as_str(), dimensions, metric)?
                .with_embedding_property(&embedding_property_expanded)
                .with_property_deps(property_deps)
                .with_watermark(ledger_t);

        if !results.is_empty() {
            builder = builder.with_capacity(results.len())?;
        }

        for result in &results {
            builder.add_result(result)?;
        }

        let upserted = builder.indexed_count();
        let skipped = builder.skipped_count();
        let index = builder.build();

        info!(
            vg_alias = %vg_alias,
            upserted = upserted,
            skipped = skipped,
            "Built new vector index"
        );

        // 6. Persist new index
        let (name, branch) = core_alias::split_alias(vg_alias).map_err(|e| {
            crate::ApiError::config(format!("Invalid virtual graph alias '{}': {}", vg_alias, e))
        })?;
        let new_address = format!(
            "fluree:file://virtual-graphs/{}/{}/vector/t{}/snapshot.bin",
            name, branch, ledger_t
        );
        let bytes = serialize(&index)?;
        self.storage().write_bytes(&new_address, &bytes).await?;

        // 7. Publish snapshot to history
        self.nameservice
            .publish_vg_snapshot(&name, &branch, &new_address, ledger_t)
            .await?;

        // 8. Update VG index record
        self.nameservice
            .publish_vg_index(&name, &branch, &new_address, ledger_t)
            .await?;

        info!(
            vg_alias = %vg_alias,
            new_address = %new_address,
            ledger_t = ledger_t,
            "Completed full vector index resync"
        );

        Ok(VectorSyncResult {
            vg_alias: vg_alias.to_string(),
            upserted,
            removed: 0, // Full resync doesn't track removals
            skipped,
            old_watermark,
            new_watermark: ledger_t,
            was_full_resync: true,
        })
    }

    /// Drop a vector index.
    ///
    /// This marks the VG as retracted in the nameservice but does not
    /// immediately delete snapshot files (they may be needed for time-travel).
    pub async fn drop_vector_index(&self, vg_alias: &str) -> Result<VectorDropResult> {
        info!(vg_alias = %vg_alias, "Dropping vector index");

        // Look up VG record
        let record = self.nameservice.lookup_vg(vg_alias).await?.ok_or_else(|| {
            crate::ApiError::NotFound(format!("Virtual graph not found: {}", vg_alias))
        })?;

        if record.retracted {
            return Ok(VectorDropResult {
                vg_alias: vg_alias.to_string(),
                deleted_snapshots: 0,
                was_already_retracted: true,
            });
        }

        // Mark as retracted
        let (name, branch) = core_alias::split_alias(vg_alias).map_err(|e| {
            crate::ApiError::config(format!("Invalid virtual graph alias '{}': {}", vg_alias, e))
        })?;

        self.nameservice.retract_vg(&name, &branch).await?;

        info!(vg_alias = %vg_alias, "Marked vector index as retracted");

        Ok(VectorDropResult {
            vg_alias: vg_alias.to_string(),
            deleted_snapshots: 0,
            was_already_retracted: false,
        })
    }
}
