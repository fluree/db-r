//! Ledger state combining indexed Db with novelty overlay
//!
//! This crate provides `LedgerState` which combines:
//! - A persisted `Db` (the latest indexed state)
//! - A `Novelty` overlay (uncommitted transactions since the last index)
//!
//! Together they provide a consistent view of the ledger at a specific point in time.
//!
//! # Types
//!
//! - [`LedgerState`] - Live ledger state (mutable, has novelty)
//! - [`HistoricalLedgerView`] - Read-only view at a specific time (for time-travel)
//! - [`LedgerView`] - Staged transactions (uncommitted changes)
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_ledger::{LedgerState, HistoricalLedgerView};
//!
//! // Load current ledger state
//! let state = LedgerState::load(&nameservice, "mydb:main", storage, cache).await?;
//! println!("Ledger at t={}", state.t());
//!
//! // Load historical view at t=50
//! let view = HistoricalLedgerView::load_at(&ns, "mydb:main", storage, cache, 50).await?;
//! ```

mod error;
mod historical;
mod staged;

pub use error::{LedgerError, Result};
pub use historical::HistoricalLedgerView;
pub use staged::LedgerView;

use fluree_db_core::{Db, NodeCache, Storage};
use fluree_db_nameservice::{NameService, NsRecord};
use fluree_db_novelty::{generate_commit_flakes, trace_commits, Novelty};
use futures::StreamExt;
use std::sync::Arc;

/// Configuration for novelty backpressure
#[derive(Clone, Debug)]
pub struct IndexConfig {
    /// Soft threshold - trigger background indexing (default 100KB)
    pub reindex_min_bytes: usize,
    /// Hard threshold - block new commits until indexed (default 1MB)
    pub reindex_max_bytes: usize,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            // Clojure parity defaults (see fluree/db `add-reindex-thresholds`):
            // - reindex-min-bytes: 100000  (100 kb, decimal)
            // - reindex-max-bytes: 1000000 (1 mb, decimal)
            reindex_min_bytes: 100_000,
            reindex_max_bytes: 1_000_000,
        }
    }
}

/// Ledger state combining indexed Db with novelty overlay
///
/// Provides a consistent view of the ledger by combining:
/// - The persisted index (Db)
/// - In-memory uncommitted changes (Novelty)
#[derive(Debug, Clone)]
pub struct LedgerState<S, C> {
    /// The indexed database
    pub db: Db<S, C>,
    /// In-memory overlay of uncommitted transactions
    pub novelty: Arc<Novelty>,
    /// Current head commit address
    pub head_commit: Option<String>,
    /// Nameservice record (if loaded via nameservice)
    pub ns_record: Option<NsRecord>,
}

impl<S: Storage + Clone + 'static, C: NodeCache> LedgerState<S, C> {
    /// Load a ledger from nameservice
    ///
    /// This is resilient to missing index - if the nameservice has commits
    /// but no index yet, it creates a genesis Db and loads all commits as novelty.
    pub async fn load<N: NameService>(
        ns: &N,
        ledger_address: &str,
        storage: S,
        cache: impl Into<Arc<C>>,
    ) -> Result<Self> {
        let cache = cache.into();
        let record = ns
            .lookup(ledger_address)
            .await?
            .ok_or_else(|| LedgerError::not_found(ledger_address))?;

        // Handle missing index (genesis fallback)
        // Use record.address which includes the branch (e.g., "test:main")
        let mut db = match &record.index_address {
            Some(addr) => Db::load(storage.clone(), Arc::clone(&cache), addr).await?,
            None => Db::genesis(storage.clone(), Arc::clone(&cache), &record.address),
        };

        // Load novelty from commits since index_t
        let novelty = match &record.commit_address {
            Some(addr) if record.commit_t > db.t => {
                let (novelty, ns_delta) =
                    Self::load_novelty(storage, addr, db.t, &record.address).await?;
                // Apply namespace deltas from commits
                for (code, prefix) in ns_delta {
                    db.namespace_codes.insert(code, prefix);
                }
                novelty
            }
            _ => Novelty::new(db.t),
        };

        Ok(Self {
            db,
            novelty: Arc::new(novelty),
            head_commit: record.commit_address.clone(),
            ns_record: Some(record),
        })
    }

    /// Load novelty from commits since a given index_t
    ///
    /// Returns the novelty overlay and a merged map of namespace deltas
    /// from all loaded commits.
    async fn load_novelty(
        storage: S,
        head_address: &str,
        index_t: i64,
        ledger_alias: &str,
    ) -> Result<(Novelty, std::collections::HashMap<i32, String>)> {
        use std::collections::HashMap;

        let mut novelty = Novelty::new(index_t);
        let mut merged_ns_delta: HashMap<i32, String> = HashMap::new();

        let stream = trace_commits(storage, head_address.to_string(), index_t);
        futures::pin_mut!(stream);

        while let Some(result) = stream.next().await {
            let commit = result?;
            let meta_flakes = generate_commit_flakes(&commit, ledger_alias, commit.t);
            let mut all_flakes = commit.flakes;
            all_flakes.extend(meta_flakes);
            novelty.apply_commit(all_flakes, commit.t)?;
            // Merge namespace deltas.
            //
            // IMPORTANT: trace_commits streams from HEAD backwards (newest -> oldest),
            // so we must ensure newer commits win. Only insert if the code hasn't
            // already been set by a newer commit.
            for (code, prefix) in commit.namespace_delta {
                merged_ns_delta.entry(code).or_insert(prefix);
            }
        }

        Ok((novelty, merged_ns_delta))
    }

    /// Create a new ledger state from components
    pub fn new(db: Db<S, C>, novelty: Novelty) -> Self {
        Self {
            db,
            novelty: Arc::new(novelty),
            head_commit: None,
            ns_record: None,
        }
    }

    /// Get the current transaction time (max of index and novelty)
    pub fn t(&self) -> i64 {
        self.novelty.t.max(self.db.t)
    }

    /// Get the indexed transaction time
    pub fn index_t(&self) -> i64 {
        self.db.t
    }

    /// Get the ledger alias
    pub fn alias(&self) -> &str {
        &self.db.alias
    }

    /// Check if novelty is at max capacity (should block new commits)
    pub fn at_max_novelty(&self, config: &IndexConfig) -> bool {
        self.novelty.size >= config.reindex_max_bytes
    }

    /// Check if novelty should trigger background indexing
    pub fn should_reindex(&self, config: &IndexConfig) -> bool {
        self.novelty.size >= config.reindex_min_bytes
    }

    /// Get the novelty size in bytes
    pub fn novelty_size(&self) -> usize {
        self.novelty.size
    }

    /// Get the novelty epoch
    pub fn epoch(&self) -> u64 {
        self.novelty.epoch
    }

    /// Get a reference to the novelty overlay
    pub fn novelty(&self) -> &Arc<Novelty> {
        &self.novelty
    }

    /// Get current stats (indexed + novelty merged)
    ///
    /// Always returns an IndexStats, even for genesis/no-index ledgers.
    /// Falls back to default stats and applies novelty deltas.
    ///
    /// This is the canonical way to get up-to-date statistics for a ledger,
    /// as it includes both the indexed stats and any uncommitted changes
    /// from the novelty layer.
    pub fn current_stats(&self) -> fluree_db_core::IndexStats {
        let indexed = self
            .db
            .stats
            .as_ref()
            .cloned()
            .unwrap_or_default(); // IndexStats::default() for genesis/no-index
        fluree_db_novelty::current_stats(&indexed, self.novelty.as_ref())
    }

    /// Apply a new index, updating Db and pruning novelty
    ///
    /// # Semantics
    ///
    /// - The loaded Db's `t` represents `index_t` (time the index is current through)
    /// - Accepts if `new_index_t > current_index_t` (forward progress)
    /// - Allows `new_index_t <= commit_t` (index catching up to commits)
    /// - Equal-t with different address: ignored (no-op) for now
    /// - Prunes novelty up to `new_index_t`
    ///
    /// # Errors
    ///
    /// - `AliasMismatch` if the new index is for a different ledger
    /// - `StaleIndex` if the new index is older than the current index
    /// - `Core` errors from loading the index
    pub async fn apply_index(&mut self, index_address: &str) -> Result<()>
    where
        C: Clone,
    {
        let new_db = Db::load(
            self.db.storage.clone(),
            self.db.cache.clone(),
            index_address,
        )
        .await?;

        // Verify alias matches
        if new_db.alias != self.db.alias {
            return Err(LedgerError::alias_mismatch(&new_db.alias, &self.db.alias));
        }

        // Verify forward progress on index
        let current_index_t = self.db.t;
        if new_db.t < current_index_t {
            return Err(LedgerError::stale_index(new_db.t, current_index_t));
        }
        if new_db.t == current_index_t {
            // Equal-t: ignore (defer tie-break by hash to multi-indexer phase)
            return Ok(());
        }

        // Clear novelty up to new index_t
        let mut new_novelty = (*self.novelty).clone();
        new_novelty.clear_up_to(new_db.t);

        // Update state
        self.db = new_db;
        self.novelty = Arc::new(new_novelty);

        // Update ns_record
        if let Some(ref mut record) = self.ns_record {
            record.index_address = Some(index_address.to_string());
            record.index_t = self.db.t;
        }

        Ok(())
    }

    /// Check nameservice for newer index and apply if available
    ///
    /// Returns `true` if a newer index was applied, `false` otherwise.
    ///
    /// # Errors
    ///
    /// - `NotFound` if the ledger is not in the nameservice
    /// - `MissingIndexAddress` if nameservice has index_t but no address
    /// - Other errors from `apply_index`
    pub async fn maybe_apply_newer_index<N: NameService>(&mut self, ns: &N) -> Result<bool>
    where
        C: Clone,
    {
        let record = ns
            .lookup(&self.db.alias)
            .await?
            .ok_or_else(|| LedgerError::not_found(&self.db.alias))?;

        // Only apply if there's a newer index AND it has an address
        if record.index_t > self.db.t {
            let index_address = record.index_address.as_ref().ok_or_else(|| {
                LedgerError::missing_index_address(&self.db.alias, record.index_t)
            })?;
            self.apply_index(index_address).await?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Check if indexing should be triggered and return the alias if so
    ///
    /// This is a convenience method for use after committing transactions.
    /// It checks if novelty has exceeded the soft threshold (reindex_min_bytes)
    /// and returns `Some(alias)` if indexing should be triggered.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // After committing a transaction:
    /// if let Some(alias) = ledger.maybe_trigger_index(&index_config) {
    ///     // Trigger background indexing
    ///     indexer_handle.trigger(alias);
    /// }
    /// ```
    ///
    /// For blocking scenarios where indexing must complete before proceeding,
    /// check `at_max_novelty()` instead and wait for indexing to complete.
    pub fn maybe_trigger_index(&self, config: &IndexConfig) -> Option<&str> {
        if self.should_reindex(config) {
            Some(self.alias())
        } else {
            None
        }
    }

    /// Check if at max novelty and return alias for blocking scenarios
    ///
    /// This is for use in blocking scenarios where a commit should wait
    /// for indexing to complete before proceeding.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Before committing when novelty is at max:
    /// if let Some(alias) = ledger.require_index(&index_config) {
    ///     // Trigger and wait for indexing
    ///     let result = indexer_handle.trigger_and_wait(alias).await?;
    ///     ledger.apply_index(&result.root_address).await?;
    /// }
    /// ```
    pub fn require_index(&self, config: &IndexConfig) -> Option<&str> {
        if self.at_max_novelty(config) {
            Some(self.alias())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{Flake, FlakeValue, MemoryStorage, NoCache, Sid};
    use fluree_db_nameservice::memory::MemoryNameService;

    fn make_flake(s: i32, p: i32, o: i64, t: i64) -> Flake {
        Flake::new(
            Sid::new(s, format!("s{}", s)),
            Sid::new(p, format!("p{}", p)),
            FlakeValue::Long(o),
            Sid::new(2, "long"),
            t,
            true,
            None,
        )
    }

    #[tokio::test]
    async fn test_ledger_state_new() {
        let storage = MemoryStorage::new();
        let cache = NoCache::new();
        let db = Db::genesis(storage, cache, "test:main");

        let mut novelty = Novelty::new(0);
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 1)], 1)
            .unwrap();

        let state = LedgerState::new(db, novelty);

        assert_eq!(state.alias(), "test:main");
        assert_eq!(state.index_t(), 0);
        assert_eq!(state.t(), 1);
        assert_eq!(state.epoch(), 1);
    }

    #[tokio::test]
    async fn test_ledger_state_backpressure() {
        let storage = MemoryStorage::new();
        let cache = NoCache::new();
        let db = Db::genesis(storage, cache, "test:main");

        let mut novelty = Novelty::new(0);
        // Add some flakes to increase size
        for i in 0..100 {
            novelty
                .apply_commit(vec![make_flake(i, 1, i as i64, 1)], 1)
                .unwrap();
        }

        let state = LedgerState::new(db, novelty);

        let small_config = IndexConfig {
            reindex_min_bytes: 100,
            reindex_max_bytes: 1000,
        };

        assert!(state.novelty_size() > 0);
        assert!(state.should_reindex(&small_config));
        assert!(state.at_max_novelty(&small_config));
    }

    #[tokio::test]
    async fn test_ledger_state_load_genesis() {
        use fluree_db_nameservice::Publisher;

        let ns = MemoryNameService::new();
        let storage = MemoryStorage::new();
        let cache = NoCache::new();

        // Create a ledger in nameservice with no index
        ns.publish_commit("test:main", "commit-1", 1).await.unwrap();

        // Store the commit
        let commit = fluree_db_novelty::Commit::new("commit-1", 1, vec![make_flake(1, 1, 100, 1)]);
        let storage = storage;
        storage.insert("commit-1", serde_json::to_vec(&commit).unwrap());

        // Load ledger - should use genesis since no index exists
        let state = LedgerState::load(&ns, "test:main", storage, cache)
            .await
            .unwrap();

        assert_eq!(state.alias(), "test:main");
        assert_eq!(state.index_t(), 0); // Genesis
        assert_eq!(state.t(), 1); // From commit
        assert_eq!(state.novelty.len(), 1);
    }

    #[tokio::test]
    async fn test_apply_index_success() {
        use fluree_db_core::serde::json::{serialize_db_root, DbRoot};
        use fluree_db_core::index::ChildRef;
        use fluree_db_core::IndexType;
        use std::collections::HashMap;

        let storage = MemoryStorage::new();
        let cache = NoCache::new();
        let db = Db::genesis(storage.clone(), cache, "test:main");

        // Create novelty with flakes at t=1 and t=2
        let mut novelty = Novelty::new(0);
        novelty.apply_commit(vec![make_flake(1, 1, 100, 1)], 1).unwrap();
        novelty.apply_commit(vec![make_flake(2, 1, 200, 2)], 2).unwrap();

        let mut state = LedgerState::new(db, novelty);
        assert_eq!(state.index_t(), 0);
        // Check active flakes via index iterator (arena has 2, and 2 are active)
        assert_eq!(state.novelty.iter_index(IndexType::Spot).count(), 2);

        // Create a new index at t=1
        let mut namespace_codes = HashMap::new();
        namespace_codes.insert(0, "".to_string());
        namespace_codes.insert(1, "@".to_string());

        let empty_root = ChildRef {
            id: "empty".to_string(),
            leaf: true,
            first: None,
            rhs: None,
            size: 0,
            bytes: Some(0),
            leftmost: true,
        };

        let db_root = DbRoot {
            alias: "test:main".to_string(),
            t: 1,
            version: 2,
            namespace_codes,
            spot: Some(empty_root.clone()),
            psot: Some(empty_root.clone()),
            post: Some(empty_root.clone()),
            opst: Some(empty_root.clone()),
            tspo: Some(empty_root),
            timestamp: None,
            stats: None,
            config: None,
            prev_index: None,
            schema: None,
            garbage: None,
        };

        let root_bytes = serialize_db_root(&db_root).unwrap();
        let storage = state.db.storage.clone();
        storage.insert("index-root-1", root_bytes);

        // Apply the index
        state.apply_index("index-root-1").await.unwrap();

        // Index should now be at t=1
        assert_eq!(state.index_t(), 1);
        // Novelty at t=1 should be cleared, only t=2 remains in the index vectors
        // (arena still has 2 flakes but only 1 is active)
        assert_eq!(state.novelty.iter_index(IndexType::Spot).count(), 1);
    }

    #[tokio::test]
    async fn test_apply_index_alias_mismatch() {
        use fluree_db_core::serde::json::{serialize_db_root, DbRoot};
        use fluree_db_core::index::ChildRef;
        use std::collections::HashMap;

        let storage = MemoryStorage::new();
        let cache = NoCache::new();
        let db = Db::genesis(storage.clone(), cache, "test:main");
        let novelty = Novelty::new(0);

        let mut state = LedgerState::new(db, novelty);

        // Create an index for a different ledger
        let mut namespace_codes = HashMap::new();
        namespace_codes.insert(0, "".to_string());

        let empty_root = ChildRef {
            id: "empty".to_string(),
            leaf: true,
            first: None,
            rhs: None,
            size: 0,
            bytes: Some(0),
            leftmost: true,
        };

        let db_root = DbRoot {
            alias: "other:ledger".to_string(), // Different alias!
            t: 1,
            version: 2,
            namespace_codes,
            spot: Some(empty_root.clone()),
            psot: Some(empty_root.clone()),
            post: Some(empty_root.clone()),
            opst: Some(empty_root.clone()),
            tspo: Some(empty_root),
            timestamp: None,
            stats: None,
            config: None,
            prev_index: None,
            schema: None,
            garbage: None,
        };

        let root_bytes = serialize_db_root(&db_root).unwrap();
        let storage = state.db.storage.clone();
        storage.insert("bad-index", root_bytes);

        // Should fail with alias mismatch
        let result = state.apply_index("bad-index").await;
        assert!(matches!(result, Err(LedgerError::AliasMismatch { .. })));
    }

    #[tokio::test]
    async fn test_apply_index_stale() {
        use fluree_db_core::serde::json::{serialize_db_root, DbRoot};
        use fluree_db_core::index::ChildRef;
        use std::collections::HashMap;

        let storage = MemoryStorage::new();
        let cache = NoCache::new();

        // Start with a db at t=2
        let mut namespace_codes = HashMap::new();
        namespace_codes.insert(0, "".to_string());

        let empty_root = ChildRef {
            id: "empty".to_string(),
            leaf: true,
            first: None,
            rhs: None,
            size: 0,
            bytes: Some(0),
            leftmost: true,
        };

        let db_root_t2 = DbRoot {
            alias: "test:main".to_string(),
            t: 2,
            version: 2,
            namespace_codes: namespace_codes.clone(),
            spot: Some(empty_root.clone()),
            psot: Some(empty_root.clone()),
            post: Some(empty_root.clone()),
            opst: Some(empty_root.clone()),
            tspo: Some(empty_root.clone()),
            timestamp: None,
            stats: None,
            config: None,
            prev_index: None,
            schema: None,
            garbage: None,
        };

        let root_bytes_t2 = serialize_db_root(&db_root_t2).unwrap();
        let storage = storage;
        storage.insert("index-root-t2", root_bytes_t2);

        let db = Db::load(storage.clone(), cache, "index-root-t2").await.unwrap();
        let novelty = Novelty::new(2);
        let mut state = LedgerState::new(db, novelty);
        assert_eq!(state.index_t(), 2);

        // Create an older index at t=1
        let db_root_t1 = DbRoot {
            alias: "test:main".to_string(),
            t: 1,
            version: 2,
            namespace_codes,
            spot: Some(empty_root.clone()),
            psot: Some(empty_root.clone()),
            post: Some(empty_root.clone()),
            opst: Some(empty_root.clone()),
            tspo: Some(empty_root),
            timestamp: None,
            stats: None,
            config: None,
            prev_index: None,
            schema: None,
            garbage: None,
        };

        let root_bytes_t1 = serialize_db_root(&db_root_t1).unwrap();
        storage.insert("index-root-t1", root_bytes_t1);

        // Should fail with stale index error
        let result = state.apply_index("index-root-t1").await;
        assert!(matches!(result, Err(LedgerError::StaleIndex { .. })));
    }

    #[tokio::test]
    async fn test_apply_index_equal_t_noop() {
        use fluree_db_core::serde::json::{serialize_db_root, DbRoot};
        use fluree_db_core::index::ChildRef;
        use std::collections::HashMap;

        let storage = MemoryStorage::new();
        let cache = NoCache::new();

        let mut namespace_codes = HashMap::new();
        namespace_codes.insert(0, "".to_string());

        let empty_root = ChildRef {
            id: "empty".to_string(),
            leaf: true,
            first: None,
            rhs: None,
            size: 0,
            bytes: Some(0),
            leftmost: true,
        };

        let db_root = DbRoot {
            alias: "test:main".to_string(),
            t: 1,
            version: 2,
            namespace_codes: namespace_codes.clone(),
            spot: Some(empty_root.clone()),
            psot: Some(empty_root.clone()),
            post: Some(empty_root.clone()),
            opst: Some(empty_root.clone()),
            tspo: Some(empty_root.clone()),
            timestamp: None,
            stats: None,
            config: None,
            prev_index: None,
            schema: None,
            garbage: None,
        };

        let root_bytes = serialize_db_root(&db_root).unwrap();
        let storage = storage;
        storage.insert("index-root", root_bytes);

        let db = Db::load(storage.clone(), cache, "index-root").await.unwrap();
        let novelty = Novelty::new(1);
        let mut state = LedgerState::new(db, novelty);

        // Create another index at same t
        let db_root_same_t = DbRoot {
            alias: "test:main".to_string(),
            t: 1, // Same t
            version: 2,
            namespace_codes,
            spot: Some(empty_root.clone()),
            psot: Some(empty_root.clone()),
            post: Some(empty_root.clone()),
            opst: Some(empty_root.clone()),
            tspo: Some(empty_root),
            timestamp: None,
            stats: None,
            config: None,
            prev_index: None,
            schema: None,
            garbage: None,
        };

        let root_bytes_same = serialize_db_root(&db_root_same_t).unwrap();
        storage.insert("index-root-same", root_bytes_same);

        // Should succeed as no-op
        let result = state.apply_index("index-root-same").await;
        assert!(result.is_ok());
        // Index_t should still be 1
        assert_eq!(state.index_t(), 1);
    }

    #[tokio::test]
    async fn test_maybe_trigger_index_below_threshold() {
        let storage = MemoryStorage::new();
        let cache = NoCache::new();
        let db = Db::genesis(storage, cache, "test:main");
        let novelty = Novelty::new(0);

        let state = LedgerState::new(db, novelty);

        let config = IndexConfig {
            reindex_min_bytes: 1000,
            reindex_max_bytes: 10000,
        };

        // No data, so below threshold
        let result = state.maybe_trigger_index(&config);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_maybe_trigger_index_above_threshold() {
        let storage = MemoryStorage::new();
        let cache = NoCache::new();
        let db = Db::genesis(storage, cache, "test:main");

        // Add some flakes to increase size
        let mut novelty = Novelty::new(0);
        for i in 0..100 {
            novelty
                .apply_commit(vec![make_flake(i, 1, i as i64, 1)], 1)
                .unwrap();
        }

        let state = LedgerState::new(db, novelty);

        let config = IndexConfig {
            reindex_min_bytes: 100, // Low threshold to trigger
            reindex_max_bytes: 10000,
        };

        // Above min threshold
        let result = state.maybe_trigger_index(&config);
        assert_eq!(result, Some("test:main"));
    }

    #[tokio::test]
    async fn test_require_index_below_max() {
        let storage = MemoryStorage::new();
        let cache = NoCache::new();
        let db = Db::genesis(storage, cache, "test:main");

        let mut novelty = Novelty::new(0);
        for i in 0..10 {
            novelty
                .apply_commit(vec![make_flake(i, 1, i as i64, 1)], 1)
                .unwrap();
        }

        let state = LedgerState::new(db, novelty);

        let config = IndexConfig {
            reindex_min_bytes: 100,
            reindex_max_bytes: 10000, // High max threshold
        };

        // Below max threshold - should not require
        let result = state.require_index(&config);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_require_index_at_max() {
        let storage = MemoryStorage::new();
        let cache = NoCache::new();
        let db = Db::genesis(storage, cache, "test:main");

        let mut novelty = Novelty::new(0);
        for i in 0..100 {
            novelty
                .apply_commit(vec![make_flake(i, 1, i as i64, 1)], 1)
                .unwrap();
        }

        let state = LedgerState::new(db, novelty);

        let config = IndexConfig {
            reindex_min_bytes: 100,
            reindex_max_bytes: 100, // Low max threshold to trigger
        };

        // Above max threshold - should require
        let result = state.require_index(&config);
        assert_eq!(result, Some("test:main"));
    }
}
