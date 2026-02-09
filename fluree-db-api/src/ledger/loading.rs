use std::sync::Arc;

use crate::{
    ApiError, Fluree, HistoricalLedgerView, LedgerState, NameService, Result, Storage,
    TypeErasedStore,
};
use fluree_db_indexer::run_index::{
    BinaryIndexRootV2, BinaryIndexStore, BINARY_INDEX_ROOT_VERSION_V2,
};
use fluree_db_nameservice::{NameServiceError, Publisher};
use fluree_db_query::BinaryRangeProvider;

impl<S, N> Fluree<S, N>
where
    S: Storage + Clone + 'static,
    N: NameService,
{
    /// Resolve a datetime to a `t` value by walking commits (fallback implementation).
    ///
    /// Note: The primary implementation now uses index-based lookup via `time_resolve::datetime_to_t`.
    /// This walking implementation is kept as a fallback for edge cases or testing.
    #[allow(dead_code)]
    async fn resolve_datetime_to_t_walking(
        &self,
        head_commit_address: &str,
        target_iso: &str,
        default_t: i64,
    ) -> Result<i64> {
        use chrono::DateTime;
        use futures::StreamExt;

        let target = DateTime::parse_from_rfc3339(target_iso).map_err(|e| {
            ApiError::internal(format!(
                "Invalid ISO-8601 timestamp for time travel: {} ({})",
                target_iso, e
            ))
        })?;

        // Walk commits newest -> oldest. Return the first commit whose timestamp is <= target.
        //
        // This matches Clojure behavior: find the first commit after the target, then use its
        // previous t. Returning latest <= target is equivalent under monotonic t/time.
        let mut saw_any_timestamp = false;
        let stream = fluree_db_novelty::trace_commits(
            self.connection.storage().clone(),
            head_commit_address.to_string(),
            0,
        );
        futures::pin_mut!(stream);

        while let Some(result) = stream.next().await {
            let commit = result.map_err(|e| {
                ApiError::internal(format!(
                    "Failed to read commit during datetime->t resolution: {}",
                    e
                ))
            })?;
            let Some(commit_time) = commit.time.as_deref() else {
                continue;
            };

            let Ok(dt) = DateTime::parse_from_rfc3339(commit_time) else {
                // Skip invalid timestamps rather than failing dataset load.
                continue;
            };
            saw_any_timestamp = true;

            if dt <= target {
                return Ok(commit.t);
            }
        }

        if !saw_any_timestamp {
            // If commits have no timestamps (or all timestamps are invalid), fall back to head.
            return Ok(default_t);
        }

        Err(ApiError::internal(format!(
            "There is no data as of {}",
            target_iso
        )))
    }

    /// Resolve a SHA prefix to a `t` value by walking commits (fallback implementation).
    ///
    /// This walks through the commit chain looking for a commit whose ID
    /// matches the given SHA prefix.
    ///
    /// Note: The primary implementation now uses index-based lookup via `time_resolve::sha_to_t`.
    /// This walking implementation is kept as a fallback for edge cases or testing.
    ///
    /// # Arguments
    /// * `head_commit_address` - The address of the head commit to start from
    /// * `sha_prefix` - The SHA prefix to match (with or without 'sha256:' prefix)
    ///
    /// # Returns
    /// * `Ok(t)` - The `t` value of the matching commit
    /// * `Err` - If no commit matches, or if multiple commits match (ambiguous)
    #[allow(dead_code)]
    async fn resolve_sha_to_t_walking(
        &self,
        head_commit_address: &str,
        sha_prefix: &str,
    ) -> Result<i64> {
        use futures::StreamExt;

        // Normalize the SHA prefix:
        // - Strip "fluree:commit:" prefix if present
        // - Strip "sha256:" prefix if present
        // - Result should be the hex hash (or prefix thereof)
        let sha_normalized = sha_prefix
            .strip_prefix("fluree:commit:")
            .unwrap_or(sha_prefix);
        let sha_normalized = sha_normalized
            .strip_prefix("sha256:")
            .unwrap_or(sha_normalized);

        // Validation: minimum 6 characters for useful prefix matching
        if sha_normalized.len() < 6 {
            return Err(ApiError::query(format!(
                "SHA prefix must be at least 6 characters, got {}",
                sha_normalized.len()
            )));
        }

        // SHA-256 in hex is 64 characters
        if sha_normalized.len() > 64 {
            return Err(ApiError::query(format!(
                "SHA prefix too long ({} chars). SHA-256 in hex is 64 characters.",
                sha_normalized.len()
            )));
        }

        // Walk commits looking for matching SHA
        let mut matching_commits: Vec<(String, i64)> = Vec::new();
        let stream = fluree_db_novelty::trace_commits(
            self.connection.storage().clone(),
            head_commit_address.to_string(),
            0,
        );
        futures::pin_mut!(stream);

        while let Some(result) = stream.next().await {
            let commit = result.map_err(|e| {
                ApiError::internal(format!(
                    "Failed to read commit during sha->t resolution: {}",
                    e
                ))
            })?;

            // Check if this commit's ID matches the prefix
            if let Some(commit_id) = &commit.id {
                // commit_id is now a ContentId; use digest_hex() to get the SHA-256 hex
                let commit_sha = commit_id.digest_hex();

                if commit_sha.starts_with(sha_normalized) {
                    matching_commits.push((commit_id.to_string(), commit.t));

                    // If we have an exact match (full SHA), we're done
                    if commit_sha == sha_normalized {
                        break;
                    }
                }
            }

            // Stop after finding multiple matches (prefix is ambiguous)
            if matching_commits.len() > 1 {
                break;
            }
        }

        match matching_commits.len() {
            0 => Err(ApiError::query(format!(
                "No commit found with SHA prefix: {}",
                sha_normalized
            ))),
            1 => {
                let (_, t) = &matching_commits[0];
                Ok(*t)
            }
            _ => {
                // Multiple matches - ambiguous prefix
                let commit_ids: Vec<&str> = matching_commits
                    .iter()
                    .take(5)
                    .map(|(id, _)| id.as_str())
                    .collect();
                Err(ApiError::query(format!(
                    "Ambiguous SHA prefix: {}. Multiple commits match: {:?}{}",
                    sha_normalized,
                    commit_ids,
                    if matching_commits.len() > 5 {
                        " ..."
                    } else {
                        ""
                    }
                )))
            }
        }
    }

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
        if let Some(index_addr) = state
            .ns_record
            .as_ref()
            .and_then(|r| r.index_address.as_ref())
            .cloned()
        {
            if state.db.range_provider.is_none() || state.binary_store.is_none() {
                let storage = self.connection.storage();
                let bytes = storage.read_bytes(&index_addr).await.map_err(|e| {
                    ApiError::internal(format!(
                        "failed to read binary index root at {}: {}",
                        index_addr, e
                    ))
                })?;

                let root = serde_json::from_slice::<BinaryIndexRootV2>(&bytes).map_err(|e| {
                    ApiError::internal(format!(
                        "failed to parse binary index root at {}: {}",
                        index_addr, e
                    ))
                })?;
                if root.version != BINARY_INDEX_ROOT_VERSION_V2 {
                    return Err(ApiError::internal(format!(
                        "unsupported binary index root version {} at {} (expected {})",
                        root.version, index_addr, BINARY_INDEX_ROOT_VERSION_V2
                    )));
                }

                let cache_dir = std::env::temp_dir().join("fluree-cache");
                let store = BinaryIndexStore::load_from_root_default(storage, &root, &cache_dir)
                    .await
                    .map_err(|e| {
                        ApiError::internal(format!(
                            "failed to load binary index store from {}: {}",
                            index_addr, e
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
