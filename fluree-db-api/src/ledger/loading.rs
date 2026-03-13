use std::sync::Arc;

use crate::{
    ApiError, Fluree, HistoricalLedgerView, LedgerState, NameService, Result, Storage,
    TypeErasedStore,
};
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::ContentStore;
use fluree_db_nameservice::{BranchPoint, NameServiceError, NsRecord, Publisher};
use fluree_db_query::BinaryRangeProvider;

impl<S, N> Fluree<S, N>
where
    S: Storage + Clone + 'static,
    N: NameService,
{
    /// Load a ledger by address (e.g., "mydb:main")
    ///
    /// This loads the ledger state using the connection-wide cache.
    /// The ledger state combines the indexed database with any uncommitted novelty transactions.
    pub async fn ledger(&self, ledger_id: &str) -> Result<LedgerState> {
        let mut state = LedgerState::load(
            &self.nameservice,
            ledger_id,
            self.connection.storage().clone(),
        )
        .await?;

        // If nameservice has an index address, require that the binary index root is
        // readable and loadable. This ensures `fluree.ledger()` always returns a
        // queryable, indexed LedgerSnapshot after (re)indexing.
        //
        // Note: we may already have a `LedgerSnapshot.range_provider` (e.g. attached after `load_ledger_snapshot`),
        // but we still want `binary_store` so query execution can use `BinaryScanOperator`.
        if let Some(index_cid) = state
            .ns_record
            .as_ref()
            .and_then(|r| r.index_head_id.as_ref())
            .cloned()
        {
            if state.snapshot.range_provider.is_none() || state.binary_store.is_none() {
                let storage = self.connection.storage();
                let cs = fluree_db_core::content_store_for(
                    storage.clone(),
                    state.snapshot.ledger_id.as_str(),
                );
                let bytes = cs.get(&index_cid).await.map_err(|e| {
                    ApiError::internal(format!(
                        "failed to read binary index root for {}: {}",
                        index_cid, e
                    ))
                })?;

                let cache_dir = std::env::temp_dir().join("fluree-cache");
                let cs = std::sync::Arc::new(cs);
                let mut store = BinaryIndexStore::load_from_root_bytes(
                    cs,
                    &bytes,
                    &cache_dir,
                    Some(Arc::clone(&self.leaflet_cache)),
                )
                .await
                .map_err(|e| {
                    ApiError::internal(format!(
                        "failed to load binary index store for {}: {}",
                        index_cid, e
                    ))
                })?;

                // Vector shards are truly lazy — loaded on demand per-shard
                // when decode_value hits a VECTOR_ID, using the same sync→async
                // bridge as index leaflets (thread + block_on).

                // Augment namespace codes with entries from novelty commits.
                // The index root only contains namespaces known at index time, but
                // subsequent transactions may introduce new namespace prefixes.
                // LedgerSnapshot.namespace_codes already has the merged set (index + novelty).
                store.augment_namespace_codes(&state.snapshot.namespace_codes);

                let arc_store = Arc::new(store);
                if state.snapshot.range_provider.is_none() {
                    let provider = BinaryRangeProvider::new(
                        Arc::clone(&arc_store),
                        state.dict_novelty.clone(),
                    );
                    state.snapshot.range_provider = Some(Arc::new(provider));
                }
                state.binary_store = Some(TypeErasedStore(arc_store));
            }
        }

        // Load default context from CAS if the nameservice record has one.
        if let Some(ref ctx_id) = state
            .ns_record
            .as_ref()
            .and_then(|r| r.default_context.as_ref())
        {
            let cs = fluree_db_core::content_store_for(
                self.connection.storage().clone(),
                state.snapshot.ledger_id.as_str(),
            );
            match cs.get(ctx_id).await {
                Ok(bytes) => match serde_json::from_slice(&bytes) {
                    Ok(ctx) => state.default_context = Some(ctx),
                    Err(e) => tracing::warn!(%e, "failed to parse default context JSON"),
                },
                Err(e) => tracing::debug!(%e, cid = %ctx_id, "could not load default context"),
            }
        }

        Ok(state)
    }

    /// Load a historical view of a ledger at a specific time
    ///
    /// This provides time-travel capability by loading the ledger state
    /// as it existed at `target_t`. The view is read-only and time-bounded.
    pub async fn ledger_view_at(
        &self,
        ledger_id: &str,
        target_t: i64,
    ) -> Result<HistoricalLedgerView> {
        let view = HistoricalLedgerView::load_at(
            &self.nameservice,
            ledger_id,
            self.connection.storage().clone(),
            target_t,
        )
        .await?;

        Ok(view)
    }
}

// =============================================================================
// Ledger Creation
// =============================================================================

impl<S, N> Fluree<S, N>
where
    S: Storage + Clone + 'static,
    N: NameService + Publisher,
{
    /// Create a new empty ledger with genesis state
    ///
    /// This operation:
    /// 1. Normalizes the ledger ID (ensures branch suffix like `:main`)
    /// 2. Registers the ledger in the nameservice (fails if already exists)
    /// 3. Creates a genesis database with t=0 (no transactions yet)
    /// 4. Returns the new LedgerState ready for transactions
    ///
    /// # Arguments
    ///
    /// * `ledger_id` - Ledger ID (e.g., "mydb" or "mydb:main")
    ///
    /// # Errors
    ///
    /// Returns `ApiError::LedgerExists` (HTTP 409) if:
    /// - The ledger already exists
    /// - The ledger was previously dropped (retracted) - must use hard drop to reuse address
    ///
    /// # Example
    ///
    /// ```ignore
    /// let ledger = fluree.create_ledger("mydb").await?;
    /// // Now you can transact: fluree.insert(ledger, &data).await?
    /// ```
    pub async fn create_ledger(&self, ledger_id: &str) -> Result<LedgerState> {
        use fluree_db_core::ledger_id::normalize_ledger_id;
        use fluree_db_novelty::Novelty;
        use tracing::info;

        // 1. Normalize ledger_id (ensure branch suffix)
        let ledger_id = normalize_ledger_id(ledger_id).unwrap_or_else(|_| ledger_id.to_string());
        info!(ledger_id = %ledger_id, "Creating ledger");

        // 2. Register in nameservice via Publisher (fails if already exists)
        match self.nameservice.publish_ledger_init(&ledger_id).await {
            Ok(()) => {}
            Err(NameServiceError::LedgerAlreadyExists(a)) => {
                return Err(ApiError::ledger_exists(a));
            }
            Err(e) => {
                return Err(e.into());
            }
        }

        // 3. Create genesis LedgerSnapshot with empty state at t=0
        let db = fluree_db_core::LedgerSnapshot::genesis(&ledger_id);

        // 4. Create LedgerState with empty Novelty (t=0)
        let ledger = LedgerState::new(db, Novelty::new(0));

        info!(ledger_id = %ledger_id, "Ledger created successfully");
        Ok(ledger)
    }

    /// Create a new branch for a ledger.
    ///
    /// Looks up the source branch to capture its current commit state, copies
    /// the source's commit chain into the new branch's storage namespace, then
    /// creates a new [`NsRecord`] for `ledger_name:new_branch` with the commit
    /// head copied from the source.
    ///
    /// # Errors
    ///
    /// - [`ApiError::LedgerExists`] if the branch already exists
    /// - [`ApiError::NotFound`] if the source branch does not exist
    pub async fn create_branch(
        &self,
        ledger_name: &str,
        new_branch: &str,
        source_branch: Option<&str>,
    ) -> Result<NsRecord> {
        use fluree_db_core::ledger_id::format_ledger_id;
        use tracing::info;

        let source = source_branch.unwrap_or("main");
        let source_id = format_ledger_id(ledger_name, source);
        let new_id = format_ledger_id(ledger_name, new_branch);

        info!(ledger_name, new_branch, source, "Creating branch");

        // Look up the source branch to capture its commit state
        let source_record = self
            .nameservice
            .lookup(&source_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(source_id.clone()))?;

        let commit_id = source_record.commit_head_id.clone().ok_or_else(|| {
            ApiError::internal(format!("Source branch {} has no commit head", source_id))
        })?;

        let branch_point = BranchPoint {
            source: source.to_string(),
            commit_id,
            t: source_record.commit_t,
        };

        match self
            .nameservice
            .create_branch(ledger_name, new_branch, branch_point)
            .await
        {
            Ok(()) => {}
            Err(NameServiceError::LedgerAlreadyExists(a)) => {
                return Err(ApiError::ledger_exists(a));
            }
            Err(e) => {
                return Err(e.into());
            }
        }

        let record = self.nameservice.lookup(&new_id).await?.ok_or_else(|| {
            ApiError::internal(format!(
                "Branch {} was created but not found in nameservice",
                new_id
            ))
        })?;

        info!(
            ledger_name,
            new_branch, source, "Branch created successfully"
        );
        Ok(record)
    }
}

impl<S, N> Fluree<S, N>
where
    S: Storage + Clone + 'static,
    N: NameService,
{
    /// List all non-retracted branches for a ledger.
    pub async fn list_branches(&self, ledger_name: &str) -> Result<Vec<NsRecord>> {
        Ok(self.nameservice.list_branches(ledger_name).await?)
    }
}
