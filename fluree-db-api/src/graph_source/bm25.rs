//! BM25 full-text search index operations.
//!
//! This module provides APIs for creating, loading, syncing, and dropping
//! BM25 full-text search indexes.

use crate::graph_source::config::Bm25CreateConfig;
use crate::graph_source::helpers::{expand_ids_in_results, extract_prefix_map};
use crate::graph_source::result::{
    Bm25CreateResult, Bm25DropResult, Bm25StalenessCheck, Bm25SyncResult, SnapshotSelection,
};
use crate::{QueryResult as ApiQueryResult, Result};
use fluree_db_core::{
    alias as core_alias, ContentId, ContentStore, OverlayProvider, Storage, StorageWrite,
};
use fluree_db_ledger::LedgerState;
use fluree_db_nameservice::{
    GraphSourcePublisher, GraphSourceType, NameService, Publisher, STORAGE_SEGMENT_GRAPH_SOURCES,
};
use fluree_db_query::bm25::{Bm25IndexBuilder, Bm25Manifest, Bm25SnapshotEntry, PropertyDeps};
use fluree_db_query::parse::parse_query;
use fluree_db_query::{execute_with_overlay, DataSource, ExecutableQuery, SelectMode, VarRegistry};
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{info, warn};

/// Best-effort deletion of old snapshot blobs from storage.
/// Logs warnings on failure but does not propagate errors.
async fn delete_old_snapshots<S: Storage>(storage: &S, addresses: &[String]) {
    for addr in addresses {
        if let Err(e) = storage.delete(addr).await {
            warn!(address = %addr, error = %e, "failed to delete old BM25 snapshot");
        }
    }
}

/// Default snapshot retention for BM25 manifests.
/// Uses the same default as index GC (`gc_max_old_indexes` + 1 for current).
fn snapshot_retention() -> usize {
    (fluree_db_indexer::DEFAULT_MAX_OLD_INDEXES as usize) + 1
}

// =============================================================================
// BM25 Index Creation
// =============================================================================

impl<S, N> crate::Fluree<S, N>
where
    S: Storage + StorageWrite + Clone + 'static,
    N: NameService + Publisher + GraphSourcePublisher,
{
    /// Create a BM25 full-text search index.
    ///
    /// This operation:
    /// 1. Loads the source ledger
    /// 2. Executes the indexing query to get documents
    /// 3. Builds the BM25 index
    /// 4. Persists the index snapshot to storage
    /// 5. Publishes the graph source record to the nameservice
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration specifying the index name, source ledger, and query
    ///
    /// # Returns
    ///
    /// Result containing the created index metadata
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = Bm25CreateConfig::new("search", "docs:main", json!({
    ///     "where": [{"@id": "?x", "@type": "Article"}],
    ///     "select": {"?x": ["@id", "title", "content"]}
    /// }));
    ///
    /// let result = fluree.create_full_text_index(config).await?;
    /// ```
    pub async fn create_full_text_index(
        &self,
        config: Bm25CreateConfig,
    ) -> Result<Bm25CreateResult> {
        let graph_source_id = config.graph_source_id();
        info!(
            graph_source_id = %graph_source_id,
            ledger = %config.ledger,
            "Creating BM25 full-text index"
        );

        // Check if graph source already exists (prevent duplicates)
        if let Some(existing) = self
            .nameservice
            .lookup_graph_source(&graph_source_id)
            .await?
        {
            if !existing.retracted {
                return Err(crate::ApiError::Config(format!(
                    "Graph source '{}' already exists",
                    graph_source_id
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
            .execute_bm25_indexing_query(&ledger, &config.query)
            .await?;

        info!(result_count = results.len(), "Executed indexing query");

        // 2b. Expand prefixed IRIs in @id fields to full IRIs
        let context = config
            .query
            .get("@context")
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let prefix_map = extract_prefix_map(&context);
        let results = expand_ids_in_results(results, &prefix_map);

        // 3. Build BM25 index
        let property_deps = PropertyDeps::from_indexing_query(&config.query);
        let mut builder = Bm25IndexBuilder::new(config.ledger.as_str(), config.bm25_config())
            .with_property_deps(property_deps)
            .with_watermark(source_t);

        builder.add_results(&results)?;

        let doc_count = builder.indexed_count();
        let skipped = builder.skipped_count();
        let index = builder.build();
        let term_count = index.num_terms();

        info!(
            doc_count = doc_count,
            skipped = skipped,
            term_count = term_count,
            "Built BM25 index"
        );

        // 4. Persist index snapshot blob to CAS
        let snapshot_address = self
            .write_bm25_snapshot_blob(&graph_source_id, &index, source_t)
            .await?;

        info!(
            snapshot_address = %snapshot_address,
            index_t = source_t,
            "Persisted versioned index snapshot"
        );

        // 5. Build manifest with initial snapshot entry
        let mut manifest = Bm25Manifest::new(&graph_source_id);
        manifest.append(Bm25SnapshotEntry::new(source_t, &snapshot_address));

        // 6. Publish graph source config record to nameservice
        let config_json = serde_json::to_string(&serde_json::json!({
            "k1": config.k1.unwrap_or(1.2),
            "b": config.b.unwrap_or(0.75),
            "query": config.query,
        }))?;

        self.nameservice
            .publish_graph_source(
                &config.name,
                config.effective_branch(),
                GraphSourceType::Bm25,
                &config_json,
                std::slice::from_ref(&config.ledger),
            )
            .await?;

        // 7. Publish manifest to CAS and head pointer to nameservice
        let index_id = self
            .publish_bm25_manifest(&graph_source_id, &manifest, source_t)
            .await?;

        info!(
            graph_source_id = %graph_source_id,
            doc_count = doc_count,
            index_t = source_t,
            "Created BM25 full-text index"
        );

        Ok(Bm25CreateResult {
            graph_source_id,
            doc_count,
            term_count,
            index_t: source_t,
            index_id: Some(index_id),
        })
    }

    /// Execute the indexing query and return JSON-LD results.
    ///
    /// Executes the query and formats results as JSON-LD objects suitable for indexing.
    /// Each result object will have an `@id` field identifying the document.
    pub(crate) async fn execute_bm25_indexing_query(
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
            construct_template: parsed.construct_template,
            graph_select: parsed.graph_select,
            binary_store: None,
        };

        let json = result.to_jsonld_async(&ledger.db).await?;
        match json {
            JsonValue::Array(arr) => Ok(arr),
            JsonValue::Object(_) => Ok(vec![json]),
            _ => Ok(Vec::new()),
        }
    }

    /// Execute an indexing query against a historical ledger view.
    ///
    /// This is used for building BM25 indexes at historical points in time.
    pub(crate) async fn execute_bm25_indexing_query_historical(
        &self,
        view: &crate::HistoricalLedgerView<S>,
        query_json: &JsonValue,
    ) -> Result<Vec<JsonValue>> {
        // Parse the query
        let mut vars = VarRegistry::new();
        let parsed = parse_query(query_json, &view.db, &mut vars)?;

        // Execute with a wildcard select
        let mut parsed_for_exec = parsed.clone();
        parsed_for_exec.select_mode = SelectMode::Wildcard;
        parsed_for_exec.select.clear();
        parsed_for_exec.graph_select = None;

        let executable = ExecutableQuery::simple(parsed_for_exec);

        let overlay: &dyn fluree_db_core::OverlayProvider = view
            .overlay()
            .map(|n| n.as_ref() as &dyn fluree_db_core::OverlayProvider)
            .unwrap_or(&fluree_db_core::NoOverlay);
        let source = DataSource::new(&view.db, overlay, view.to_t());
        let batches = execute_with_overlay(source, &vars, &executable).await?;

        // Format using the standard JSON-LD formatter
        let result = ApiQueryResult {
            vars,
            t: view.to_t(),
            novelty: view
                .overlay()
                .map(|n| Arc::clone(n) as Arc<dyn OverlayProvider>),
            context: parsed.context,
            orig_context: parsed.orig_context,
            select: parsed.select,
            select_mode: parsed.select_mode,
            batches,
            construct_template: parsed.construct_template,
            graph_select: parsed.graph_select,
            binary_store: None,
        };

        let json = result.to_jsonld_async(&view.db).await?;
        match json {
            JsonValue::Array(arr) => Ok(arr),
            JsonValue::Object(_) => Ok(vec![json]),
            _ => Ok(Vec::new()),
        }
    }

    /// Write a BM25 index snapshot blob to storage with versioned path.
    ///
    /// Creates a snapshot at `graph-sources/{name}/{branch}/bm25/t{index_t}/snapshot.bin`.
    /// Does NOT update nameservice or the manifest -- callers handle that.
    pub(crate) async fn write_bm25_snapshot_blob(
        &self,
        graph_source_id: &str,
        index: &fluree_db_query::bm25::Bm25Index,
        index_t: i64,
    ) -> Result<String> {
        use fluree_db_query::bm25::serialize;

        let bytes = serialize(index)?;

        let (name, branch) = core_alias::split_alias(graph_source_id).map_err(|e| {
            crate::ApiError::config(format!(
                "Invalid graph source alias '{}': {}",
                graph_source_id, e
            ))
        })?;
        let address = format!(
            "fluree:file://{STORAGE_SEGMENT_GRAPH_SOURCES}/{}/{}/bm25/t{}/snapshot.bin",
            name, branch, index_t
        );

        self.storage().write_bytes(&address, &bytes).await?;
        Ok(address)
    }

    /// Write a BM25 manifest to CAS and publish the manifest address as
    /// the graph source head pointer in nameservice.
    ///
    /// The manifest is content-addressed (keyed by `index_t`), so each
    /// publish creates a new immutable object in storage.
    pub(crate) async fn publish_bm25_manifest(
        &self,
        graph_source_id: &str,
        manifest: &Bm25Manifest,
        index_t: i64,
    ) -> Result<ContentId> {
        let (name, branch) = core_alias::split_alias(graph_source_id).map_err(|e| {
            crate::ApiError::config(format!(
                "Invalid graph source alias '{}': {}",
                graph_source_id, e
            ))
        })?;

        let bytes = serde_json::to_vec(manifest)?;

        // Write through the content store so it's stored at the CID-mapped address
        let cs = fluree_db_core::content_store_for(self.storage().clone(), graph_source_id);
        let index_id = cs
            .put(fluree_db_core::ContentKind::IndexRoot, &bytes)
            .await?;

        self.nameservice
            .publish_graph_source_index(&name, &branch, &index_id, index_t)
            .await?;

        Ok(index_id)
    }
}

// =============================================================================
// BM25 Manifest Loading (read-only helpers)
// =============================================================================

impl<S, N> crate::Fluree<S, N>
where
    S: Storage + Clone + 'static,
    N: NameService + GraphSourcePublisher,
{
    /// Load the current BM25 manifest from CAS, or create a new empty one.
    ///
    /// Reads the manifest address from the nameservice head pointer,
    /// then loads the manifest JSON from CAS. Returns an empty manifest
    /// if the graph source has no index yet (e.g., during initial create).
    pub(crate) async fn load_or_create_bm25_manifest(
        &self,
        graph_source_id: &str,
    ) -> Result<Bm25Manifest> {
        match self
            .nameservice
            .lookup_graph_source(graph_source_id)
            .await?
        {
            Some(record) if record.index_id.is_some() => {
                let index_cid = record.index_id.as_ref().unwrap();
                let cs = fluree_db_core::content_store_for(self.storage().clone(), graph_source_id);
                let bytes = cs.get(index_cid).await?;
                let manifest: Bm25Manifest = serde_json::from_slice(&bytes)?;
                Ok(manifest)
            }
            _ => Ok(Bm25Manifest::new(graph_source_id)),
        }
    }

    /// Load the current BM25 manifest from CAS.
    ///
    /// Returns an error if the graph source is not found or has no index.
    pub(crate) async fn load_bm25_manifest(&self, graph_source_id: &str) -> Result<Bm25Manifest> {
        let record = self
            .nameservice
            .lookup_graph_source(graph_source_id)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!("Graph source not found: {}", graph_source_id))
            })?;

        let index_cid = record.index_id.ok_or_else(|| {
            crate::ApiError::NotFound(format!("No index for graph source: {}", graph_source_id))
        })?;

        let cs = fluree_db_core::content_store_for(self.storage().clone(), graph_source_id);
        let bytes = cs.get(&index_cid).await?;
        let manifest: Bm25Manifest = serde_json::from_slice(&bytes)?;
        Ok(manifest)
    }
}

// =============================================================================
// BM25 Index Loading (for queries)
// =============================================================================

impl<S, N> crate::Fluree<S, N>
where
    S: Storage + Clone + 'static,
    N: NameService + GraphSourcePublisher,
{
    /// Select the best BM25 snapshot for a given `as_of_t`.
    ///
    /// Loads the BM25 manifest from CAS and selects the snapshot with the
    /// largest `index_t` that is <= `as_of_t`.
    pub async fn select_bm25_snapshot(
        &self,
        graph_source_id: &str,
        as_of_t: i64,
    ) -> Result<Option<SnapshotSelection>> {
        let manifest = self.load_bm25_manifest(graph_source_id).await?;

        match manifest.select_snapshot(as_of_t) {
            Some(entry) => Ok(Some(SnapshotSelection {
                graph_source_id: graph_source_id.to_string(),
                snapshot_t: entry.index_t,
                snapshot_address: entry.snapshot_address.clone(),
            })),
            None => Ok(None),
        }
    }

    /// Load a BM25 index for a specific `as_of_t` using snapshot selection.
    ///
    /// This is the time-travel aware version of `load_bm25_index`.
    pub async fn load_bm25_index_at(
        &self,
        graph_source_id: &str,
        as_of_t: i64,
    ) -> Result<(Arc<fluree_db_query::bm25::Bm25Index>, i64)> {
        use fluree_db_query::bm25::deserialize;

        let selection = self
            .select_bm25_snapshot(graph_source_id, as_of_t)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!(
                    "No BM25 snapshot available for {} at t={}",
                    graph_source_id, as_of_t
                ))
            })?;

        let bytes = self
            .storage()
            .read_bytes(&selection.snapshot_address)
            .await?;

        let index = deserialize(&bytes)?;
        Ok((Arc::new(index), selection.snapshot_t))
    }

    /// Load a BM25 index from storage (head snapshot).
    ///
    /// Loads the manifest, resolves the head snapshot, and deserializes.
    /// For time-travel queries, use `load_bm25_index_at` instead.
    pub async fn load_bm25_index(
        &self,
        graph_source_id: &str,
    ) -> Result<Arc<fluree_db_query::bm25::Bm25Index>> {
        use fluree_db_query::bm25::deserialize;

        let manifest = self.load_bm25_manifest(graph_source_id).await?;
        let head = manifest.head().ok_or_else(|| {
            crate::ApiError::NotFound(format!("No snapshots in manifest for: {}", graph_source_id))
        })?;

        let bytes = self.storage().read_bytes(&head.snapshot_address).await?;
        let index = deserialize(&bytes)?;
        Ok(Arc::new(index))
    }

    /// Check if a BM25 index is stale relative to its source ledger.
    ///
    /// This is a lightweight check that only looks up nameservice records.
    pub async fn check_bm25_staleness(&self, graph_source_id: &str) -> Result<Bm25StalenessCheck> {
        // Look up graph source record
        let record = self
            .nameservice
            .lookup_graph_source(graph_source_id)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!("Graph source not found: {}", graph_source_id))
            })?;

        // Get source ledger from dependencies
        let source_ledger = record
            .dependencies
            .first()
            .ok_or_else(|| {
                crate::ApiError::Config("Graph source has no source ledger".to_string())
            })?
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

        Ok(Bm25StalenessCheck {
            graph_source_id: graph_source_id.to_string(),
            source_ledger,
            index_t,
            ledger_t,
            is_stale,
            lag,
        })
    }
}

// =============================================================================
// BM25 Index Sync (Maintenance)
// =============================================================================

impl<S, N> crate::Fluree<S, N>
where
    S: Storage + StorageWrite + Clone + 'static,
    N: NameService + Publisher + GraphSourcePublisher,
{
    /// Sync a BM25 index to catch up with ledger updates.
    ///
    /// This operation performs incremental updates when possible,
    /// falling back to full resync if needed.
    pub async fn sync_bm25_index(&self, graph_source_id: &str) -> Result<Bm25SyncResult> {
        use fluree_db_novelty::trace_commits_by_id;
        use fluree_db_query::bm25::{deserialize, CompiledPropertyDeps, IncrementalUpdater};
        use futures::StreamExt;

        info!(graph_source_id = %graph_source_id, "Starting BM25 index sync");

        // 1. Look up graph source record to get config and index address
        let record = self
            .nameservice
            .lookup_graph_source(graph_source_id)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!("Graph source not found: {}", graph_source_id))
            })?;

        // Check if graph source has been dropped
        if record.retracted {
            return Err(crate::ApiError::Drop(format!(
                "Cannot sync retracted graph source: {}",
                graph_source_id
            )));
        }

        if record.index_id.is_none() {
            // No index yet - need full resync
            return self.resync_bm25_index(graph_source_id).await;
        }

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
            .ok_or_else(|| {
                crate::ApiError::Config("Graph source has no source ledger".to_string())
            })?
            .clone();

        // 2. Load source ledger to get current state
        let ledger = self.ledger(&source_ledger_alias).await?;
        let ledger_t = ledger.t();

        // 3. Load existing index via manifest head
        let manifest = self.load_bm25_manifest(graph_source_id).await?;
        let head = manifest.head().ok_or_else(|| {
            crate::ApiError::NotFound(format!("No snapshots in manifest for: {}", graph_source_id))
        })?;
        let bytes = self.storage().read_bytes(&head.snapshot_address).await?;
        let mut index = deserialize(&bytes)?;
        let old_watermark = index.watermark.get(&source_ledger_alias).unwrap_or(0);

        // Already up to date?
        if ledger_t <= old_watermark {
            info!(graph_source_id = %graph_source_id, ledger_t = ledger_t, "Index already up to date");
            return Ok(Bm25SyncResult {
                graph_source_id: graph_source_id.to_string(),
                upserted: 0,
                removed: 0,
                affected_subjects: 0,
                old_watermark,
                new_watermark: old_watermark,
                was_full_resync: false,
            });
        }

        // 4. Get head commit CID for tracing
        let head_commit_id = ledger
            .ns_record
            .as_ref()
            .and_then(|r| r.commit_head_id.clone())
            .ok_or_else(|| crate::ApiError::NotFound("No commit head for ledger".to_string()))?;

        // 5. Compile property deps for this ledger's namespace
        let compiled_deps = CompiledPropertyDeps::compile(&index.property_deps, |iri: &str| {
            ledger.db.encode_iri(iri)
        });

        // 6. Trace commits and collect affected subjects
        let mut affected_sids: HashSet<fluree_db_core::Sid> = HashSet::new();
        let store = fluree_db_core::content_store_for(self.storage().clone(), &ledger.db.ledger_id);
        let stream = trace_commits_by_id(store, head_commit_id.clone(), old_watermark);
        futures::pin_mut!(stream);

        while let Some(result) = stream.next().await {
            let commit = result?;
            let subjects = compiled_deps.affected_subjects(&commit.flakes);
            affected_sids.extend(subjects);
        }

        // If no subjects affected, fall back to full resync
        if affected_sids.is_empty() {
            warn!(
                graph_source_id = %graph_source_id,
                old_watermark = old_watermark,
                ledger_t = ledger_t,
                "No affected subjects detected, falling back to full resync"
            );
            return self.resync_bm25_index(graph_source_id).await;
        }

        // 7. Convert affected Sids to IRIs
        let affected_iris: HashSet<Arc<str>> = affected_sids
            .into_iter()
            .filter_map(|sid| ledger.db.decode_sid(&sid).map(|s| Arc::from(s.as_str())))
            .collect();

        info!(
            graph_source_id = %graph_source_id,
            affected_count = affected_iris.len(),
            "Found affected subjects for incremental update"
        );

        // 8. Re-run indexing query and filter to affected subjects
        let results = self.execute_bm25_indexing_query(&ledger, &query).await?;

        // Expand prefix map for matching
        let context = query
            .get("@context")
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let prefix_map = extract_prefix_map(&context);

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
        let mut updater = IncrementalUpdater::new(source_ledger_alias.as_str(), &mut index);
        let update_result = updater.apply_update(&results, &affected_iris_expanded, ledger_t);

        info!(
            graph_source_id = %graph_source_id,
            upserted = update_result.upserted,
            removed = update_result.removed,
            "Applied incremental update"
        );

        // 10. Persist updated index blob
        let new_address = self
            .write_bm25_snapshot_blob(graph_source_id, &index, ledger_t)
            .await?;

        // 11. Update manifest, trim old snapshots, and publish
        let mut manifest = manifest;
        manifest.append(Bm25SnapshotEntry::new(ledger_t, &new_address));
        let removed = manifest.trim(snapshot_retention());
        self.publish_bm25_manifest(graph_source_id, &manifest, ledger_t)
            .await?;

        // Best-effort cleanup of old snapshot blobs
        delete_old_snapshots(self.storage(), &removed).await;

        info!(
            graph_source_id = %graph_source_id,
            new_address = %new_address,
            trimmed = removed.len(),
            ledger_t = ledger_t,
            "Incremental sync complete"
        );

        Ok(Bm25SyncResult {
            graph_source_id: graph_source_id.to_string(),
            upserted: update_result.upserted,
            removed: update_result.removed,
            affected_subjects: affected_iris.len(),
            old_watermark,
            new_watermark: ledger_t,
            was_full_resync: false,
        })
    }

    /// Force a full resync of a BM25 index.
    ///
    /// Unlike `sync_bm25_index`, this re-runs the entire indexing query
    /// and rebuilds the index from scratch.
    pub async fn resync_bm25_index(&self, graph_source_id: &str) -> Result<Bm25SyncResult> {
        use fluree_db_query::bm25::{deserialize, IncrementalUpdater};

        info!(graph_source_id = %graph_source_id, "Starting BM25 full resync");

        // 1. Look up graph source record
        let record = self
            .nameservice
            .lookup_graph_source(graph_source_id)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!("Graph source not found: {}", graph_source_id))
            })?;

        if record.retracted {
            return Err(crate::ApiError::Drop(format!(
                "Cannot sync retracted graph source: {}",
                graph_source_id
            )));
        }

        if record.index_id.is_none() {
            return Err(crate::ApiError::NotFound(format!(
                "No index for graph source: {}",
                graph_source_id
            )));
        }

        let config: JsonValue = serde_json::from_str(&record.config)?;
        let query = config
            .get("query")
            .cloned()
            .unwrap_or(serde_json::json!({}));

        let source_ledger = record
            .dependencies
            .first()
            .ok_or_else(|| {
                crate::ApiError::Config("Graph source has no source ledger".to_string())
            })?
            .clone();

        // 2. Load existing index via manifest head (to preserve config and property deps)
        let manifest = self.load_bm25_manifest(graph_source_id).await?;
        let head = manifest.head().ok_or_else(|| {
            crate::ApiError::NotFound(format!("No snapshots in manifest for: {}", graph_source_id))
        })?;
        let bytes = self.storage().read_bytes(&head.snapshot_address).await?;
        let mut index = deserialize(&bytes)?;
        let old_watermark = index.watermark.get(&source_ledger).unwrap_or(0);

        // 3. Load source ledger
        let ledger = self.ledger(&source_ledger).await?;
        let ledger_t = ledger.t();

        // 4. Re-run indexing query
        let results = self.execute_bm25_indexing_query(&ledger, &query).await?;

        info!(
            graph_source_id = %graph_source_id,
            result_count = results.len(),
            ledger_t = ledger_t,
            "Executed full indexing query"
        );

        // 5. Apply full sync (replaces all documents)
        let mut updater = IncrementalUpdater::new(source_ledger.as_str(), &mut index);
        let update_result = updater.apply_full_sync(&results, ledger_t);

        // 6. Persist updated index blob
        let new_address = self
            .write_bm25_snapshot_blob(graph_source_id, &index, ledger_t)
            .await?;

        // 7. Update manifest, trim old snapshots, and publish
        let mut manifest = manifest;
        manifest.append(Bm25SnapshotEntry::new(ledger_t, &new_address));
        let removed = manifest.trim(snapshot_retention());
        self.publish_bm25_manifest(graph_source_id, &manifest, ledger_t)
            .await?;

        // Best-effort cleanup of old snapshot blobs
        delete_old_snapshots(self.storage(), &removed).await;

        info!(
            graph_source_id = %graph_source_id,
            new_address = %new_address,
            trimmed = removed.len(),
            ledger_t = ledger_t,
            "Full resync complete"
        );

        Ok(Bm25SyncResult {
            graph_source_id: graph_source_id.to_string(),
            upserted: update_result.upserted,
            removed: update_result.removed,
            affected_subjects: update_result.upserted + update_result.removed,
            old_watermark,
            new_watermark: ledger_t,
            was_full_resync: true,
        })
    }

    /// Load a BM25 index, optionally syncing if stale.
    ///
    /// This implements the "on-query catch-up" pattern.
    pub async fn load_bm25_index_with_sync(
        &self,
        graph_source_id: &str,
        auto_sync: bool,
    ) -> Result<(
        Arc<fluree_db_query::bm25::Bm25Index>,
        Option<Bm25SyncResult>,
    )> {
        use fluree_db_query::bm25::deserialize;

        // Look up graph source record
        let record = self
            .nameservice
            .lookup_graph_source(graph_source_id)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!("Graph source not found: {}", graph_source_id))
            })?;

        // Get source ledger to check staleness
        let source_ledger = record
            .dependencies
            .first()
            .ok_or_else(|| {
                crate::ApiError::Config("Graph source has no source ledger".to_string())
            })?
            .clone();

        // Look up source ledger record
        let ledger_record = self
            .nameservice
            .lookup(&source_ledger)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!("Source ledger not found: {}", source_ledger))
            })?;

        let index_t = record.index_t;
        let ledger_t = ledger_record.commit_t;
        let is_stale = index_t < ledger_t;

        // Sync if stale and auto_sync is enabled
        let sync_result = if is_stale && auto_sync {
            info!(
                graph_source_id = %graph_source_id,
                index_t = index_t,
                ledger_t = ledger_t,
                "Index is stale, syncing before load"
            );
            Some(self.sync_bm25_index(graph_source_id).await?)
        } else {
            None
        };

        // Load the (possibly updated) index via manifest head
        let manifest = self.load_bm25_manifest(graph_source_id).await?;
        let head = manifest.head().ok_or_else(|| {
            crate::ApiError::NotFound(format!("No snapshots in manifest for: {}", graph_source_id))
        })?;

        let bytes = self.storage().read_bytes(&head.snapshot_address).await?;
        let index = deserialize(&bytes)?;

        Ok((Arc::new(index), sync_result))
    }

    /// Sync a BM25 index to a specific target time.
    ///
    /// This builds a BM25 snapshot at exactly `target_t` by loading
    /// the source ledger at that historical point.
    pub async fn sync_bm25_index_to(
        &self,
        graph_source_id: &str,
        target_t: i64,
        timeout_ms: Option<u64>,
    ) -> Result<Bm25SyncResult> {
        use fluree_db_query::bm25::{Bm25IndexBuilder, IncrementalUpdater, PropertyDeps};

        info!(
            graph_source_id = %graph_source_id,
            target_t = target_t,
            timeout_ms = ?timeout_ms,
            "Starting BM25 index sync to specific t"
        );

        let _ = timeout_ms; // Reserved for future timeout support

        // 1. Look up graph source record to get config
        let record = self
            .nameservice
            .lookup_graph_source(graph_source_id)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!("Graph source not found: {}", graph_source_id))
            })?;

        let config: JsonValue = serde_json::from_str(&record.config)?;
        let query = config
            .get("query")
            .cloned()
            .unwrap_or(serde_json::json!({}));
        let k1 = config.get("k1").and_then(|v| v.as_f64()).unwrap_or(1.2);
        let b = config.get("b").and_then(|v| v.as_f64()).unwrap_or(0.75);

        let source_ledger = record
            .dependencies
            .first()
            .ok_or_else(|| {
                crate::ApiError::Config("Graph source has no source ledger".to_string())
            })?
            .clone();

        // 2. Check if we already have a snapshot at target_t
        let manifest = self.load_or_create_bm25_manifest(graph_source_id).await?;
        if manifest.has_snapshot_at(target_t) {
            info!(graph_source_id = %graph_source_id, target_t = target_t, "Snapshot already exists");
            return Ok(Bm25SyncResult {
                graph_source_id: graph_source_id.to_string(),
                upserted: 0,
                removed: 0,
                affected_subjects: 0,
                old_watermark: target_t,
                new_watermark: target_t,
                was_full_resync: false,
            });
        }

        // 3. Load source ledger at target_t using time-travel
        let view = self.ledger_view_at(&source_ledger, target_t).await?;

        // 4. Execute indexing query at target_t
        let results = self
            .execute_bm25_indexing_query_historical(&view, &query)
            .await?;

        info!(
            graph_source_id = %graph_source_id,
            target_t = target_t,
            result_count = results.len(),
            "Executed indexing query at historical t"
        );

        // 5. Build BM25 index
        let property_deps = PropertyDeps::from_indexing_query(&query);
        let bm25_config = fluree_db_query::bm25::Bm25Config::new(k1, b);
        let mut builder = Bm25IndexBuilder::new(source_ledger.as_str(), bm25_config)
            .with_property_deps(property_deps)
            .with_watermark(target_t);

        builder.add_results(&results)?;
        let mut index = builder.build();

        // Apply as full sync to set watermarks correctly
        let mut updater = IncrementalUpdater::new(source_ledger.as_str(), &mut index);
        let update_result = updater.apply_full_sync(&results, target_t);

        // 6. Persist versioned snapshot blob
        let address = self
            .write_bm25_snapshot_blob(graph_source_id, &index, target_t)
            .await?;

        // 7. Update manifest, trim old snapshots, and publish
        let mut manifest = manifest;
        manifest.append(Bm25SnapshotEntry::new(target_t, &address));
        let removed = manifest.trim(snapshot_retention());
        let effective_t = manifest.head().map(|h| h.index_t).unwrap_or(target_t);
        self.publish_bm25_manifest(graph_source_id, &manifest, effective_t)
            .await?;

        // Best-effort cleanup of old snapshot blobs
        delete_old_snapshots(self.storage(), &removed).await;

        info!(
            graph_source_id = %graph_source_id,
            target_t = target_t,
            trimmed = removed.len(),
            upserted = update_result.upserted,
            "Sync to specific t complete"
        );

        Ok(Bm25SyncResult {
            graph_source_id: graph_source_id.to_string(),
            upserted: update_result.upserted,
            removed: update_result.removed,
            affected_subjects: update_result.upserted + update_result.removed,
            old_watermark: record.index_t,
            new_watermark: target_t,
            was_full_resync: true,
        })
    }

    /// Sync multiple BM25 indexes.
    pub async fn sync_bm25_indexes(
        &self,
        graph_source_ides: &[&str],
    ) -> Vec<Result<Bm25SyncResult>> {
        let mut results = Vec::with_capacity(graph_source_ides.len());
        for alias in graph_source_ides {
            results.push(self.sync_bm25_index(alias).await);
        }
        results
    }

    /// Check staleness for multiple BM25 indexes.
    pub async fn check_bm25_staleness_batch(
        &self,
        graph_source_ides: &[&str],
    ) -> Vec<Result<Bm25StalenessCheck>> {
        let mut results = Vec::with_capacity(graph_source_ides.len());
        for alias in graph_source_ides {
            results.push(self.check_bm25_staleness(alias).await);
        }
        results
    }

    /// Drop a BM25 full-text index.
    ///
    /// This operation:
    /// 1. Marks the graph source as retracted in nameservice
    /// 2. Deletes all snapshot files from storage
    pub async fn drop_full_text_index(&self, graph_source_id: &str) -> Result<Bm25DropResult>
    where
        S: StorageWrite,
    {
        info!(graph_source_id = %graph_source_id, "Dropping BM25 full-text index");

        // 1. Look up graph source record to verify it exists
        let record = self
            .nameservice
            .lookup_graph_source(graph_source_id)
            .await?;

        let record = match record {
            Some(r) => r,
            None => {
                return Err(crate::ApiError::NotFound(format!(
                    "Graph source not found: {}",
                    graph_source_id
                )));
            }
        };

        // If already retracted, return early (idempotent)
        if record.retracted {
            info!(graph_source_id = %graph_source_id, "Graph source already retracted");
            return Ok(Bm25DropResult {
                graph_source_id: graph_source_id.to_string(),
                deleted_snapshots: 0,
                was_already_retracted: true,
            });
        }

        // 2. Load manifest for cleanup (get all snapshot addresses)
        let manifest = self.load_or_create_bm25_manifest(graph_source_id).await?;

        // 3. Retract graph source in nameservice
        self.nameservice
            .retract_graph_source(&record.name, &record.branch)
            .await?;

        info!(
            graph_source_id = %graph_source_id,
            snapshot_count = manifest.snapshots.len(),
            "Graph source retracted, cleaning up storage"
        );

        // 4. Collect all addresses to delete (snapshot blobs + manifest itself)
        let addresses_to_delete: HashSet<String> = manifest
            .all_snapshot_addresses()
            .into_iter()
            .map(|s| s.to_string())
            .collect();

        // Note: the manifest CID is tracked in the nameservice, but the
        // storage address is derivable from the manifest snapshots already collected.

        // 5. Delete all snapshot files
        let mut deleted_snapshots = 0;
        for addr in &addresses_to_delete {
            match self.storage().delete(addr).await {
                Ok(()) => {
                    deleted_snapshots += 1;
                }
                Err(e) => {
                    warn!(
                        graph_source_id = %graph_source_id,
                        address = %addr,
                        error = %e,
                        "Failed to delete snapshot file"
                    );
                }
            }
        }

        info!(
            graph_source_id = %graph_source_id,
            deleted = deleted_snapshots,
            total = addresses_to_delete.len(),
            "Drop complete"
        );

        Ok(Bm25DropResult {
            graph_source_id: graph_source_id.to_string(),
            deleted_snapshots,
            was_already_retracted: false,
        })
    }
}
