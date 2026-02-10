use std::sync::Arc;

use crate::{
    ApiError, Fluree, HistoricalLedgerView, LedgerState, NameService, Result, Storage,
    TypeErasedStore,
};
use fluree_db_core::ContentStore;
use fluree_db_indexer::run_index::{BinaryIndexRoot, BinaryIndexStore, BINARY_INDEX_ROOT_VERSION};
use fluree_db_nameservice::{NameServiceError, Publisher};
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
    pub async fn ledger(&self, ledger_id: &str) -> Result<LedgerState<S>> {
        let mut state = LedgerState::load(
            &self.nameservice,
            ledger_id,
            self.connection.storage().clone(),
        )
        .await?;

        // If nameservice has an index address, require that the binary index root is
        // readable and loadable. This ensures `fluree.ledger()` always returns a
        // queryable, indexed Db after (re)indexing.
        //
        // Note: we may already have a `Db.range_provider` (e.g. created during Db::load),
        // but we still want `binary_store` so query execution can use `BinaryScanOperator`.
        if let Some(index_cid) = state
            .ns_record
            .as_ref()
            .and_then(|r| r.index_head_id.as_ref())
            .cloned()
        {
            if state.db.range_provider.is_none() || state.binary_store.is_none() {
                let storage = self.connection.storage();
                let cs =
                    fluree_db_core::content_store_for(storage.clone(), state.db.ledger_id.as_str());
                let bytes = cs.get(&index_cid).await.map_err(|e| {
                    ApiError::internal(format!(
                        "failed to read binary index root for {}: {}",
                        index_cid, e
                    ))
                })?;

                let root = serde_json::from_slice::<BinaryIndexRoot>(&bytes).map_err(|e| {
                    ApiError::internal(format!(
                        "failed to parse binary index root for {}: {}",
                        index_cid, e
                    ))
                })?;
                if root.version != BINARY_INDEX_ROOT_VERSION {
                    return Err(ApiError::internal(format!(
                        "unsupported binary index root version {} for {} (expected {})",
                        root.version, index_cid, BINARY_INDEX_ROOT_VERSION
                    )));
                }

                let cache_dir = std::env::temp_dir().join("fluree-cache");
                let store = BinaryIndexStore::load_from_root_default(&cs, &root, &cache_dir)
                    .await
                    .map_err(|e| {
                        ApiError::internal(format!(
                            "failed to load binary index store for {}: {}",
                            index_cid, e
                        ))
                    })?;

                let arc_store = Arc::new(store);
                if state.db.range_provider.is_none() {
                    let provider = BinaryRangeProvider::new(
                        Arc::clone(&arc_store),
                        state.dict_novelty.clone(),
                        0,
                    );
                    state.db.range_provider = Some(Arc::new(provider));
                }
                state.binary_store = Some(TypeErasedStore(arc_store));
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
    ) -> Result<HistoricalLedgerView<S>> {
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
    pub async fn create_ledger(&self, ledger_id: &str) -> Result<LedgerState<S>> {
        use fluree_db_core::alias::normalize_alias;
        use fluree_db_novelty::Novelty;
        use tracing::info;

        // 1. Normalize address (ensure branch suffix)
        let ledger_id = normalize_alias(ledger_id).unwrap_or_else(|_| ledger_id.to_string());
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

        // 3. Create genesis Db with empty state at t=0
        let db = fluree_db_core::Db::genesis(self.connection.storage().clone(), &ledger_id);

        // 4. Create LedgerState with empty Novelty (t=0)
        let ledger = LedgerState::new(db, Novelty::new(0));

        info!(ledger_id = %ledger_id, "Ledger created successfully");
        Ok(ledger)
    }
}
