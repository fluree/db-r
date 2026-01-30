//! BM25 full-text search index operations.
//!
//! This module provides APIs for creating, loading, syncing, and dropping
//! BM25 full-text search indexes.

use crate::virtual_graph::config::Bm25CreateConfig;
use crate::virtual_graph::helpers::{expand_ids_in_results, extract_prefix_map};
use crate::virtual_graph::result::{
    Bm25CreateResult, Bm25DropResult, Bm25StalenessCheck, Bm25SyncResult, SnapshotSelection,
};
use crate::{QueryResult as ApiQueryResult, Result, SimpleCache};
use fluree_db_core::{alias as core_alias, OverlayProvider, Storage, StorageWrite};
use fluree_db_ledger::LedgerState;
use fluree_db_nameservice::{NameService, Publisher, VgType, VirtualGraphPublisher};
use fluree_db_query::bm25::{Bm25IndexBuilder, PropertyDeps};
use fluree_db_query::parse::parse_query;
use fluree_db_query::{execute_with_overlay_at, ExecutableQuery, SelectMode, VarRegistry};
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{info, warn};

// =============================================================================
// BM25 Index Creation
// =============================================================================

impl<S, N> crate::Fluree<S, SimpleCache, N>
where
    S: Storage + StorageWrite + Clone + 'static,
    N: NameService + Publisher + VirtualGraphPublisher,
{
    /// Create a BM25 full-text search index.
    ///
    /// This operation:
    /// 1. Loads the source ledger
    /// 2. Executes the indexing query to get documents
    /// 3. Builds the BM25 index
    /// 4. Persists the index snapshot to storage
    /// 5. Publishes the VG record to the nameservice
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
    pub async fn create_full_text_index(&self, config: Bm25CreateConfig) -> Result<Bm25CreateResult> {
        let vg_alias = config.vg_alias();
        info!(
            vg_alias = %vg_alias,
            ledger = %config.ledger,
            "Creating BM25 full-text index"
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
        let results = self.execute_bm25_indexing_query(&ledger, &config.query).await?;

        info!(
            result_count = results.len(),
            "Executed indexing query"
        );

        // 2b. Expand prefixed IRIs in @id fields to full IRIs
        let context = config.query.get("@context").cloned().unwrap_or(serde_json::json!({}));
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

        // 4. Persist index snapshot (versioned for time-travel support)
        let index_address = self
            .persist_bm25_index_versioned(&vg_alias, &index, source_t)
            .await?;

        info!(
            index_address = %index_address,
            index_t = source_t,
            "Persisted versioned index snapshot"
        );

        // 5. Publish VG record to nameservice
        let config_json = serde_json::to_string(&serde_json::json!({
            "k1": config.k1.unwrap_or(1.2),
            "b": config.b.unwrap_or(0.75),
            "query": config.query,
        }))?;

        self.nameservice
            .publish_vg(
                &config.name,
                config.effective_branch(),
                VgType::Bm25,
                &config_json,
                &[config.ledger.clone()],
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
            doc_count = doc_count,
            index_t = source_t,
            "Created BM25 full-text index"
        );

        Ok(Bm25CreateResult {
            vg_alias,
            doc_count,
            term_count,
            index_t: source_t,
            index_address: Some(index_address),
        })
    }

    /// Execute the indexing query and return JSON-LD results.
    ///
    /// Executes the query and formats results as JSON-LD objects suitable for indexing.
    /// Each result object will have an `@id` field identifying the document.
    pub(crate) async fn execute_bm25_indexing_query(
        &self,
        ledger: &LedgerState<S, SimpleCache>,
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

        let batches = execute_with_overlay_at(
            &ledger.db,
            ledger.novelty.as_ref(),
            &vars,
            &executable,
            ledger.t(),
            None,
        )
        .await?;

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
        view: &crate::HistoricalLedgerView<S, SimpleCache>,
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

        let batches = if let Some(novelty) = view.overlay() {
            execute_with_overlay_at(
                &view.db,
                novelty.as_ref(),
                &vars,
                &executable,
                view.to_t(),
                None,
            )
            .await?
        } else {
            execute_with_overlay_at(
                &view.db,
                &fluree_db_core::NoOverlay,
                &vars,
                &executable,
                view.to_t(),
                None,
            )
            .await?
        };

        // Format using the standard JSON-LD formatter
        let result = ApiQueryResult {
            vars,
            t: view.to_t(),
            novelty: view.overlay().map(|n| Arc::clone(n) as Arc<dyn OverlayProvider>),
            context: parsed.context,
            orig_context: parsed.orig_context,
            select: parsed.select,
            select_mode: parsed.select_mode,
            batches,
            construct_template: parsed.construct_template,
            graph_select: parsed.graph_select,
        };

        let json = result.to_jsonld_async(&view.db).await?;
        match json {
            JsonValue::Array(arr) => Ok(arr),
            JsonValue::Object(_) => Ok(vec![json]),
            _ => Ok(Vec::new()),
        }
    }

    /// Persist a BM25 index snapshot to storage with versioned path.
    ///
    /// Creates a snapshot at `virtual-graphs/{name}/{branch}/bm25/t{index_t}/snapshot.bin`
    /// and publishes the snapshot to the history file for time-travel support.
    pub(crate) async fn persist_bm25_index_versioned(
        &self,
        vg_alias: &str,
        index: &fluree_db_query::bm25::Bm25Index,
        index_t: i64,
    ) -> Result<String> {
        use fluree_db_query::bm25::serialize;

        // Serialize the index
        let bytes = serialize(index)?;

        // Build versioned storage address
        let (name, branch) = core_alias::split_alias(vg_alias).map_err(|e| {
            crate::ApiError::config(format!(
                "Invalid virtual graph alias '{}': {}",
                vg_alias, e
            ))
        })?;
        let address = format!(
            "fluree:file://virtual-graphs/{}/{}/bm25/t{}/snapshot.bin",
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
// BM25 Index Loading (for queries)
// =============================================================================

impl<S, N> crate::Fluree<S, SimpleCache, N>
where
    S: Storage + Clone + 'static,
    N: NameService + VirtualGraphPublisher,
{
    /// Select the best BM25 snapshot for a given `as_of_t`.
    ///
    /// This queries the snapshot history to find the snapshot with the
    /// largest `index_t` that is <= `as_of_t`.
    pub async fn select_bm25_snapshot(
        &self,
        vg_alias: &str,
        as_of_t: i64,
    ) -> Result<Option<SnapshotSelection>> {
        // Look up snapshot history
        let history = self.nameservice.lookup_vg_snapshots(vg_alias).await?;

        // Select best snapshot for as_of_t
        match history.select_snapshot(as_of_t) {
            Some(entry) => Ok(Some(SnapshotSelection {
                vg_alias: vg_alias.to_string(),
                snapshot_t: entry.index_t,
                snapshot_address: entry.index_address.clone(),
            })),
            None => Ok(None),
        }
    }

    /// Load a BM25 index for a specific `as_of_t` using snapshot selection.
    ///
    /// This is the time-travel aware version of `load_bm25_index`.
    pub async fn load_bm25_index_at(
        &self,
        vg_alias: &str,
        as_of_t: i64,
    ) -> Result<(Arc<fluree_db_query::bm25::Bm25Index>, i64)> {
        use fluree_db_query::bm25::deserialize;

        // Select appropriate snapshot
        let selection = self
            .select_bm25_snapshot(vg_alias, as_of_t)
            .await?
            .ok_or_else(|| {
                crate::ApiError::NotFound(format!(
                    "No BM25 snapshot available for {} at t={}",
                    vg_alias, as_of_t
                ))
            })?;

        // Load from storage
        let bytes = self.storage().read_bytes(&selection.snapshot_address).await?;

        // Deserialize
        let index = deserialize(&bytes)?;

        Ok((Arc::new(index), selection.snapshot_t))
    }

    /// Load a BM25 index from storage (head snapshot).
    ///
    /// For time-travel queries, use `load_bm25_index_at` instead.
    pub async fn load_bm25_index(
        &self,
        vg_alias: &str,
    ) -> Result<Arc<fluree_db_query::bm25::Bm25Index>> {
        use fluree_db_query::bm25::deserialize;

        // Look up VG record
        let record = self
            .nameservice
            .lookup_vg(vg_alias)
            .await?
            .ok_or_else(|| crate::ApiError::NotFound(format!("Virtual graph not found: {}", vg_alias)))?;

        // Get index address
        let index_address = record
            .index_address
            .ok_or_else(|| crate::ApiError::NotFound(format!("No index for virtual graph: {}", vg_alias)))?;

        // Load from storage
        let bytes = self.storage().read_bytes(&index_address).await?;

        // Deserialize
        let index = deserialize(&bytes)?;

        Ok(Arc::new(index))
    }

    /// Check if a BM25 index is stale relative to its source ledger.
    ///
    /// This is a lightweight check that only looks up nameservice records.
    pub async fn check_bm25_staleness(&self, vg_alias: &str) -> Result<Bm25StalenessCheck> {
        // Look up VG record
        let record = self
            .nameservice
            .lookup_vg(vg_alias)
            .await?
            .ok_or_else(|| crate::ApiError::NotFound(format!("Virtual graph not found: {}", vg_alias)))?;

        // Get source ledger from dependencies
        let source_ledger = record
            .dependencies
            .first()
            .ok_or_else(|| crate::ApiError::Config("VG has no source ledger".to_string()))?
            .clone();

        // Check minimum head across all dependencies
        let mut ledger_t: Option<i64> = None;
        for dep in &record.dependencies {
            let ledger_record = self
                .nameservice
                .lookup(dep)
                .await?
                .ok_or_else(|| crate::ApiError::NotFound(format!("Source ledger not found: {}", dep)))?;
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
// BM25 Index Sync (Maintenance)
// =============================================================================

impl<S, N> crate::Fluree<S, SimpleCache, N>
where
    S: Storage + StorageWrite + Clone + 'static,
    N: NameService + Publisher + VirtualGraphPublisher,
{
    /// Sync a BM25 index to catch up with ledger updates.
    ///
    /// This operation performs incremental updates when possible,
    /// falling back to full resync if needed.
    pub async fn sync_bm25_index(&self, vg_alias: &str) -> Result<Bm25SyncResult> {
        use fluree_db_novelty::trace_commits;
        use fluree_db_query::bm25::{deserialize, serialize, CompiledPropertyDeps, IncrementalUpdater};
        use futures::StreamExt;

        info!(vg_alias = %vg_alias, "Starting BM25 index sync");

        // 1. Look up VG record to get config and index address
        let record = self
            .nameservice
            .lookup_vg(vg_alias)
            .await?
            .ok_or_else(|| crate::ApiError::NotFound(format!("Virtual graph not found: {}", vg_alias)))?;

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
                return self.resync_bm25_index(vg_alias).await;
            }
        };

        // Parse config to get query
        let config: JsonValue = serde_json::from_str(&record.config)?;
        let query = config.get("query").cloned().unwrap_or(serde_json::json!({}));

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
            return Ok(Bm25SyncResult {
                vg_alias: vg_alias.to_string(),
                upserted: 0,
                removed: 0,
                affected_subjects: 0,
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
            .ok_or_else(|| {
                crate::ApiError::NotFound("No commit address for ledger".to_string())
            })?;

        // 5. Compile property deps for this ledger's namespace
        let compiled_deps = CompiledPropertyDeps::compile(&index.property_deps, |iri: &str| {
            ledger.db.encode_iri(iri)
        });

        // 6. Trace commits and collect affected subjects
        let mut affected_sids: HashSet<fluree_db_core::Sid> = HashSet::new();
        let stream = trace_commits(self.storage().clone(), head_commit_address.clone(), old_watermark);
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
            return self.resync_bm25_index(vg_alias).await;
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
        let results = self.execute_bm25_indexing_query(&ledger, &query).await?;

        // Expand prefix map for matching
        let context = query.get("@context").cloned().unwrap_or(serde_json::json!({}));
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
            vg_alias = %vg_alias,
            upserted = update_result.upserted,
            removed = update_result.removed,
            "Applied incremental update"
        );

        // 10. Persist updated index
        let (name, branch) = core_alias::split_alias(vg_alias).map_err(|e| {
            crate::ApiError::config(format!("Invalid virtual graph alias '{}': {}", vg_alias, e))
        })?;
        let new_address = format!(
            "fluree:file://virtual-graphs/{}/{}/bm25/t{}/snapshot.bin",
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
            "Incremental sync complete"
        );

        Ok(Bm25SyncResult {
            vg_alias: vg_alias.to_string(),
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
    pub async fn resync_bm25_index(&self, vg_alias: &str) -> Result<Bm25SyncResult> {
        use fluree_db_query::bm25::{deserialize, serialize, IncrementalUpdater};

        info!(vg_alias = %vg_alias, "Starting BM25 full resync");

        // 1. Look up VG record
        let record = self
            .nameservice
            .lookup_vg(vg_alias)
            .await?
            .ok_or_else(|| crate::ApiError::NotFound(format!("Virtual graph not found: {}", vg_alias)))?;

        if record.retracted {
            return Err(crate::ApiError::Drop(format!(
                "Cannot sync retracted virtual graph: {}",
                vg_alias
            )));
        }

        let index_address = record
            .index_address
            .ok_or_else(|| crate::ApiError::NotFound(format!("No index for virtual graph: {}", vg_alias)))?;

        let config: JsonValue = serde_json::from_str(&record.config)?;
        let query = config.get("query").cloned().unwrap_or(serde_json::json!({}));

        let source_ledger = record
            .dependencies
            .first()
            .ok_or_else(|| crate::ApiError::Config("VG has no source ledger".to_string()))?
            .clone();

        // 2. Load existing index (to preserve config and property deps)
        let bytes = self.storage().read_bytes(&index_address).await?;
        let mut index = deserialize(&bytes)?;
        let old_watermark = index.watermark.get(&source_ledger).unwrap_or(0);

        // 3. Load source ledger
        let ledger = self.ledger(&source_ledger).await?;
        let ledger_t = ledger.t();

        // 4. Re-run indexing query
        let results = self.execute_bm25_indexing_query(&ledger, &query).await?;

        info!(
            vg_alias = %vg_alias,
            result_count = results.len(),
            ledger_t = ledger_t,
            "Executed full indexing query"
        );

        // 5. Apply full sync (replaces all documents)
        let mut updater = IncrementalUpdater::new(source_ledger.as_str(), &mut index);
        let update_result = updater.apply_full_sync(&results, ledger_t);

        // 6. Persist updated index
        let (name, branch) = core_alias::split_alias(vg_alias).map_err(|e| {
            crate::ApiError::config(format!("Invalid virtual graph alias '{}': {}", vg_alias, e))
        })?;
        let new_address = format!(
            "fluree:file://virtual-graphs/{}/{}/bm25/t{}/snapshot.bin",
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
            "Full resync complete"
        );

        Ok(Bm25SyncResult {
            vg_alias: vg_alias.to_string(),
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
        vg_alias: &str,
        auto_sync: bool,
    ) -> Result<(Arc<fluree_db_query::bm25::Bm25Index>, Option<Bm25SyncResult>)> {
        use fluree_db_query::bm25::deserialize;

        // Look up VG record
        let record = self
            .nameservice
            .lookup_vg(vg_alias)
            .await?
            .ok_or_else(|| crate::ApiError::NotFound(format!("Virtual graph not found: {}", vg_alias)))?;

        // Get source ledger to check staleness
        let source_ledger = record
            .dependencies
            .first()
            .ok_or_else(|| crate::ApiError::Config("VG has no source ledger".to_string()))?
            .clone();

        // Look up source ledger record
        let ledger_record = self
            .nameservice
            .lookup(&source_ledger)
            .await?
            .ok_or_else(|| crate::ApiError::NotFound(format!("Source ledger not found: {}", source_ledger)))?;

        let index_t = record.index_t;
        let ledger_t = ledger_record.commit_t;
        let is_stale = index_t < ledger_t;

        // Sync if stale and auto_sync is enabled
        let sync_result = if is_stale && auto_sync {
            info!(
                vg_alias = %vg_alias,
                index_t = index_t,
                ledger_t = ledger_t,
                "Index is stale, syncing before load"
            );
            Some(self.sync_bm25_index(vg_alias).await?)
        } else {
            None
        };

        // Load the (possibly updated) index
        let record = self
            .nameservice
            .lookup_vg(vg_alias)
            .await?
            .ok_or_else(|| crate::ApiError::NotFound(format!("Virtual graph not found: {}", vg_alias)))?;

        let index_address = record
            .index_address
            .ok_or_else(|| crate::ApiError::NotFound(format!("No index for virtual graph: {}", vg_alias)))?;

        let bytes = self.storage().read_bytes(&index_address).await?;
        let index = deserialize(&bytes)?;

        Ok((Arc::new(index), sync_result))
    }

    /// Sync a BM25 index to a specific target time.
    ///
    /// This builds a BM25 snapshot at exactly `target_t` by loading
    /// the source ledger at that historical point.
    pub async fn sync_bm25_index_to(
        &self,
        vg_alias: &str,
        target_t: i64,
        timeout_ms: Option<u64>,
    ) -> Result<Bm25SyncResult> {
        use fluree_db_query::bm25::{Bm25IndexBuilder, IncrementalUpdater, PropertyDeps, serialize};

        info!(
            vg_alias = %vg_alias,
            target_t = target_t,
            timeout_ms = ?timeout_ms,
            "Starting BM25 index sync to specific t"
        );

        let _ = timeout_ms; // Reserved for future timeout support

        // 1. Look up VG record to get config
        let record = self
            .nameservice
            .lookup_vg(vg_alias)
            .await?
            .ok_or_else(|| crate::ApiError::NotFound(format!("Virtual graph not found: {}", vg_alias)))?;

        let config: JsonValue = serde_json::from_str(&record.config)?;
        let query = config.get("query").cloned().unwrap_or(serde_json::json!({}));
        let k1 = config.get("k1").and_then(|v| v.as_f64()).unwrap_or(1.2);
        let b = config.get("b").and_then(|v| v.as_f64()).unwrap_or(0.75);

        let source_ledger = record
            .dependencies
            .first()
            .ok_or_else(|| crate::ApiError::Config("VG has no source ledger".to_string()))?
            .clone();

        // 2. Check if we already have a snapshot at target_t
        let history = self.nameservice.lookup_vg_snapshots(vg_alias).await?;
        if history.has_snapshot_at(target_t) {
            info!(vg_alias = %vg_alias, target_t = target_t, "Snapshot already exists");
            return Ok(Bm25SyncResult {
                vg_alias: vg_alias.to_string(),
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
        let results = self.execute_bm25_indexing_query_historical(&view, &query).await?;

        info!(
            vg_alias = %vg_alias,
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

        // 6. Persist versioned snapshot
        let (name, branch) = core_alias::split_alias(vg_alias).map_err(|e| {
            crate::ApiError::config(format!("Invalid virtual graph alias '{}': {}", vg_alias, e))
        })?;
        let address = format!(
            "fluree:file://virtual-graphs/{}/{}/bm25/t{}/snapshot.bin",
            name, branch, target_t
        );

        let bytes = serialize(&index)?;
        self.storage().write_bytes(&address, &bytes).await?;

        // 7. Publish snapshot to history
        self.nameservice
            .publish_vg_snapshot(&name, &branch, &address, target_t)
            .await?;

        // 8. Update head if this is newer
        if let Some(head) = history.head() {
            if target_t > head.index_t {
                self.nameservice
                    .publish_vg_index(&name, &branch, &address, target_t)
                    .await?;
            }
        } else {
            self.nameservice
                .publish_vg_index(&name, &branch, &address, target_t)
                .await?;
        }

        info!(
            vg_alias = %vg_alias,
            target_t = target_t,
            upserted = update_result.upserted,
            "Sync to specific t complete"
        );

        Ok(Bm25SyncResult {
            vg_alias: vg_alias.to_string(),
            upserted: update_result.upserted,
            removed: update_result.removed,
            affected_subjects: update_result.upserted + update_result.removed,
            old_watermark: record.index_t,
            new_watermark: target_t,
            was_full_resync: true,
        })
    }

    /// Sync multiple BM25 indexes.
    pub async fn sync_bm25_indexes(&self, vg_aliases: &[&str]) -> Vec<Result<Bm25SyncResult>> {
        let mut results = Vec::with_capacity(vg_aliases.len());
        for alias in vg_aliases {
            results.push(self.sync_bm25_index(alias).await);
        }
        results
    }

    /// Check staleness for multiple BM25 indexes.
    pub async fn check_bm25_staleness_batch(
        &self,
        vg_aliases: &[&str],
    ) -> Vec<Result<Bm25StalenessCheck>> {
        let mut results = Vec::with_capacity(vg_aliases.len());
        for alias in vg_aliases {
            results.push(self.check_bm25_staleness(alias).await);
        }
        results
    }

    /// Drop a BM25 full-text index.
    ///
    /// This operation:
    /// 1. Marks the virtual graph as retracted in nameservice
    /// 2. Deletes all snapshot files from storage
    pub async fn drop_full_text_index(&self, vg_alias: &str) -> Result<Bm25DropResult>
    where
        S: StorageWrite,
    {
        info!(vg_alias = %vg_alias, "Dropping BM25 full-text index");

        // 1. Look up VG record to verify it exists
        let record = self.nameservice.lookup_vg(vg_alias).await?;

        let record = match record {
            Some(r) => r,
            None => {
                return Err(crate::ApiError::NotFound(format!(
                    "Virtual graph not found: {}",
                    vg_alias
                )));
            }
        };

        // If already retracted, return early (idempotent)
        if record.retracted {
            info!(vg_alias = %vg_alias, "VG already retracted");
            return Ok(Bm25DropResult {
                vg_alias: vg_alias.to_string(),
                deleted_snapshots: 0,
                was_already_retracted: true,
            });
        }

        // 2. Get snapshot history for cleanup
        let history = self.nameservice.lookup_vg_snapshots(vg_alias).await?;

        // 3. Retract VG in nameservice
        self.nameservice
            .retract_vg(&record.name, &record.branch)
            .await?;

        info!(
            vg_alias = %vg_alias,
            snapshot_count = history.snapshots.len(),
            "VG retracted, cleaning up storage"
        );

        // 4. Collect all addresses to delete
        let mut addresses_to_delete: HashSet<String> = history
            .snapshots
            .iter()
            .map(|s| s.index_address.clone())
            .collect();

        if let Some(index_addr) = &record.index_address {
            addresses_to_delete.insert(index_addr.clone());
        }

        // 5. Delete all snapshot files
        let mut deleted_snapshots = 0;
        for addr in &addresses_to_delete {
            match self.storage().delete(addr).await {
                Ok(()) => {
                    deleted_snapshots += 1;
                }
                Err(e) => {
                    warn!(
                        vg_alias = %vg_alias,
                        address = %addr,
                        error = %e,
                        "Failed to delete snapshot file"
                    );
                }
            }
        }

        info!(
            vg_alias = %vg_alias,
            deleted = deleted_snapshots,
            total = addresses_to_delete.len(),
            "Drop complete"
        );

        Ok(Bm25DropResult {
            vg_alias: vg_alias.to_string(),
            deleted_snapshots,
            was_already_retracted: false,
        })
    }
}
