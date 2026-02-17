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
//! let state = LedgerState::load(&nameservice, "mydb:main", storage).await?;
//! println!("Ledger at t={}", state.t());
//!
//! // Load historical view at t=50
//! let view = HistoricalLedgerView::load_at(&ns, "mydb:main", storage, 50).await?;
//! ```

mod error;
mod historical;
mod staged;

pub use error::{LedgerError, Result};
pub use historical::HistoricalLedgerView;
pub use staged::LedgerView;

use fluree_db_core::{content_store_for, ContentId, ContentStore, Db, DictNovelty, Storage};
use fluree_db_nameservice::{NameService, NsRecord};
use fluree_db_novelty::{generate_commit_flakes, trace_commits_by_id, Novelty};
use futures::StreamExt;
use std::sync::Arc;

/// Type-erased binary index store for query engine access.
///
/// Allows `LedgerState` to carry a `BinaryIndexStore` without
/// depending on `fluree-db-indexer`. The API layer downcasts to
/// the concrete type when building `ContextConfig` for queries.
#[derive(Clone)]
pub struct TypeErasedStore(pub Arc<dyn std::any::Any + Send + Sync>);

impl std::fmt::Debug for TypeErasedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeErasedStore").finish()
    }
}

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
pub struct LedgerState {
    /// The indexed database
    pub db: Db,
    /// In-memory overlay of uncommitted transactions
    pub novelty: Arc<Novelty>,
    /// Dictionary novelty layer for subjects and strings.
    ///
    /// Tracks novel dictionary entries introduced since the last index build.
    /// Populated during commit, read during queries, reset at index application.
    pub dict_novelty: Arc<DictNovelty>,
    /// Content identifier of the head commit (identity).
    ///
    /// Set during commit (from the computed CID) and during ledger load
    /// (derived from the commit blob hash).
    pub head_commit_id: Option<ContentId>,
    /// Content identifier of the current index root (identity).
    ///
    /// Set when an index is applied (from `IndexResult.root_id`) and during
    /// ledger load (from `NsRecord.index_head_id`).
    pub head_index_id: Option<ContentId>,
    /// Nameservice record (if loaded via nameservice)
    pub ns_record: Option<NsRecord>,
    /// Type-erased binary index store (concrete type: `Arc<BinaryIndexStore>`).
    ///
    /// Set by `Fluree::ledger()` when a binary index is available. Used by
    /// the query engine to enable `BinaryScanOperator` for IRI resolution.
    pub binary_store: Option<TypeErasedStore>,
    /// Default JSON-LD @context for this ledger.
    ///
    /// Captured from turtle @prefix declarations during import and augmented
    /// with built-in namespace prefixes. Applied to queries that don't supply
    /// their own @context. Loaded from CAS via `NsRecord.default_context`.
    pub default_context: Option<serde_json::Value>,
    /// Type-erased spatial index providers, keyed by predicate IRI.
    ///
    /// Each entry is `Arc<dyn SpatialIndexProvider>`. Set by `Fluree::ledger()`
    /// when spatial indexes are available in the binary index root.
    pub spatial_indexes: Option<TypeErasedStore>,
}

impl LedgerState {
    /// Load a ledger from nameservice
    ///
    /// This is resilient to missing index - if the nameservice has commits
    /// but no index yet, it creates a genesis Db and loads all commits as novelty.
    pub async fn load<S: Storage + Clone + 'static, N: NameService>(
        ns: &N,
        ledger_id: &str,
        storage: S,
    ) -> Result<Self> {
        let record = ns
            .lookup(ledger_id)
            .await?
            .ok_or_else(|| LedgerError::not_found(ledger_id))?;

        let store = content_store_for(storage.clone(), &record.ledger_id);

        // Handle missing index (genesis fallback)
        let (mut db, dict_novelty) = match &record.index_head_id {
            Some(index_cid) => {
                let root_bytes = store.get(index_cid).await?;
                let loaded = Db::from_root_bytes(&root_bytes)?;
                let dn = DictNovelty::with_watermarks(
                    loaded.subject_watermarks.clone(),
                    loaded.string_watermark,
                );
                (loaded, dn)
            }
            None => (Db::genesis(&record.ledger_id), DictNovelty::new_genesis()),
        };

        // Load novelty from commits since index_t
        let head_commit_id = match &record.commit_head_id {
            Some(head_cid) if record.commit_t > db.t => {
                let (novelty_overlay, ns_delta, head_id) =
                    Self::load_novelty(store, head_cid, db.t, &record.ledger_id).await?;
                // Apply namespace deltas from commits
                for (code, prefix) in ns_delta {
                    db.namespace_codes.insert(code, prefix);
                }
                // Replace empty novelty with loaded overlay below
                let head_commit_id = head_id;
                let head_index_id = record.index_head_id.clone();
                return Ok(Self {
                    db,
                    novelty: Arc::new(novelty_overlay),
                    dict_novelty: Arc::new(dict_novelty),
                    head_commit_id,
                    head_index_id,
                    ns_record: Some(record),
                    binary_store: None,
                    default_context: None,
                    spatial_indexes: None,
                });
            }
            _ => record.commit_head_id.clone(),
        };

        let head_index_id = record.index_head_id.clone();
        let novelty_t = db.t;
        Ok(Self {
            db,
            novelty: Arc::new(Novelty::new(novelty_t)),
            dict_novelty: Arc::new(dict_novelty),
            head_commit_id,
            head_index_id,
            ns_record: Some(record),
            binary_store: None,
            default_context: None,
            spatial_indexes: None,
        })
    }

    /// Load novelty from commits since a given index_t
    ///
    /// Walks the commit chain backwards from `head_cid` using the content store,
    /// collecting flakes for all commits with `t > index_t`.
    ///
    /// Returns the novelty overlay, a merged map of namespace deltas
    /// from all loaded commits, and the head commit's ContentId.
    async fn load_novelty<C: ContentStore + Clone + 'static>(
        store: C,
        head_cid: &ContentId,
        index_t: i64,
        ledger_id: &str,
    ) -> Result<(
        Novelty,
        std::collections::HashMap<u16, String>,
        Option<ContentId>,
    )> {
        use std::collections::HashMap;

        let mut novelty = Novelty::new(index_t);
        let mut merged_ns_delta: HashMap<u16, String> = HashMap::new();
        let head_commit_id: Option<ContentId> = Some(head_cid.clone());

        let stream = trace_commits_by_id(store, head_cid.clone(), index_t);
        futures::pin_mut!(stream);

        while let Some(result) = stream.next().await {
            let commit = result?;
            Self::apply_commit_to_novelty(&mut novelty, &mut merged_ns_delta, commit, ledger_id)?;
        }

        Ok((novelty, merged_ns_delta, head_commit_id))
    }

    /// Apply a single commit to the novelty overlay and merge namespace deltas.
    fn apply_commit_to_novelty(
        novelty: &mut Novelty,
        merged_ns_delta: &mut std::collections::HashMap<u16, String>,
        commit: fluree_db_novelty::Commit,
        ledger_id: &str,
    ) -> Result<()> {
        let meta_flakes = generate_commit_flakes(&commit, ledger_id, commit.t);
        let mut all_flakes = commit.flakes;
        all_flakes.extend(meta_flakes);
        novelty.apply_commit(all_flakes, commit.t)?;
        // Merge namespace deltas.
        // IMPORTANT: trace_commits streams from HEAD backwards (newest -> oldest),
        // so we must ensure newer commits win. Only insert if the code hasn't
        // already been set by a newer commit.
        for (code, prefix) in commit.namespace_delta {
            merged_ns_delta.entry(code).or_insert(prefix);
        }
        Ok(())
    }

    /// Create a new ledger state from components
    pub fn new(db: Db, novelty: Novelty) -> Self {
        let dict_novelty =
            DictNovelty::with_watermarks(db.subject_watermarks.clone(), db.string_watermark);
        Self {
            db,
            novelty: Arc::new(novelty),
            dict_novelty: Arc::new(dict_novelty),
            head_commit_id: None,
            head_index_id: None,
            ns_record: None,
            binary_store: None,
            default_context: None,
            spatial_indexes: None,
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

    /// Get the ledger ID
    pub fn ledger_id(&self) -> &str {
        &self.db.ledger_id
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
        let indexed = self.db.stats.as_ref().cloned().unwrap_or_default(); // IndexStats::default() for genesis/no-index
        fluree_db_novelty::current_stats(&indexed, self.novelty.as_ref())
    }

    /// Apply a new index, updating Db and pruning novelty
    ///
    /// # Semantics
    ///
    /// - The loaded Db's `t` represents `index_t` (time the index is current through)
    /// - Accepts if `new_index_t > current_index_t` (forward progress)
    /// - Allows `new_index_t <= commit_t` (index catching up to commits)
    /// - Equal-t with different CID: ignored (no-op) for now
    /// - Prunes novelty up to `new_index_t`
    ///
    /// # Arguments
    ///
    /// * `index_id` - Content identifier of the new index root
    ///
    /// # Errors
    ///
    /// - `LedgerIdMismatch` if the new index is for a different ledger
    /// - `StaleIndex` if the new index is older than the current index
    /// - `Core` errors from loading the index
    pub async fn apply_index(&mut self, index_id: &ContentId, cs: &dyn ContentStore) -> Result<()> {
        let root_bytes = cs.get(index_id).await?;
        let new_db = Db::from_root_bytes(&root_bytes)?;

        // Verify ledger ID matches
        if new_db.ledger_id != self.db.ledger_id {
            return Err(LedgerError::ledger_id_mismatch(
                &new_db.ledger_id,
                &self.db.ledger_id,
            ));
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

        // Reset dict_novelty with new watermarks from the index root
        let new_dict_novelty = DictNovelty::with_watermarks(
            new_db.subject_watermarks.clone(),
            new_db.string_watermark,
        );

        // Update state
        self.db = new_db;
        self.novelty = Arc::new(new_novelty);
        self.dict_novelty = Arc::new(new_dict_novelty);
        self.head_index_id = Some(index_id.clone());

        // Update ns_record
        if let Some(ref mut record) = self.ns_record {
            record.index_head_id = Some(index_id.clone());
            record.index_t = self.db.t;
        }

        Ok(())
    }

    /// Apply a pre-loaded Db as the new index.
    ///
    /// Same validation as `apply_index()` but takes an already-loaded Db,
    /// avoiding the storage I/O call. This enables the API level to:
    /// 1. Read root bytes once
    /// 2. Load `BinaryIndexStore` and attach `BinaryRangeProvider` to the Db
    /// 3. Apply the enriched Db here in a brief, non-async swap
    ///
    /// The caller is responsible for ensuring the Db has `range_provider` set
    /// if it's a binary-only (v2) Db.
    pub fn apply_loaded_db(&mut self, new_db: Db, index_id: Option<&ContentId>) -> Result<()> {
        // Verify ledger ID matches
        if new_db.ledger_id != self.db.ledger_id {
            return Err(LedgerError::ledger_id_mismatch(
                &new_db.ledger_id,
                &self.db.ledger_id,
            ));
        }

        // Verify forward progress on index
        let current_index_t = self.db.t;
        if new_db.t < current_index_t {
            return Err(LedgerError::stale_index(new_db.t, current_index_t));
        }
        if new_db.t == current_index_t {
            return Ok(());
        }

        // Clear novelty up to new index_t
        let mut new_novelty = (*self.novelty).clone();
        new_novelty.clear_up_to(new_db.t);

        // Reset dict_novelty with new watermarks from the index root
        let new_dict_novelty = DictNovelty::with_watermarks(
            new_db.subject_watermarks.clone(),
            new_db.string_watermark,
        );

        // Update state
        self.db = new_db;
        self.novelty = Arc::new(new_novelty);
        self.dict_novelty = Arc::new(new_dict_novelty);
        self.head_index_id = index_id.cloned();

        // Update ns_record
        if let Some(ref mut record) = self.ns_record {
            record.index_head_id = index_id.cloned();
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
    /// - `MissingIndexAddress` if nameservice has index_t but no index CID
    /// - Other errors from `apply_index`
    pub async fn maybe_apply_newer_index<N: NameService>(
        &mut self,
        ns: &N,
        cs: &dyn ContentStore,
    ) -> Result<bool> {
        let record = ns
            .lookup(&self.db.ledger_id)
            .await?
            .ok_or_else(|| LedgerError::not_found(&self.db.ledger_id))?;

        // Only apply if there's a newer index AND it has a CID
        if record.index_t > self.db.t {
            let index_id = record
                .index_head_id
                .as_ref()
                .ok_or_else(|| LedgerError::missing_index_id(&self.db.ledger_id, record.index_t))?;
            self.apply_index(index_id, cs).await?;
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
            Some(self.ledger_id())
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
    ///     ledger.apply_index(&result.root_id, &content_store).await?;
    /// }
    /// ```
    pub fn require_index(&self, config: &IndexConfig) -> Option<&str> {
        if self.at_max_novelty(config) {
            Some(self.ledger_id())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{ContentId, ContentKind, Flake, FlakeValue, MemoryStorage, Sid};
    use fluree_db_nameservice::memory::MemoryNameService;

    fn make_flake(s: u16, p: u16, o: i64, t: i64) -> Flake {
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

    /// Helper: build minimal IRB1 root bytes for testing.
    ///
    /// Only populates ledger_id, index_t, and namespace_codes.
    /// All other sections are empty/zero.
    fn build_test_irb1(ledger_id: &str, index_t: i64, ns_codes: &[(u16, &str)]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);
        buf.extend_from_slice(b"IRB1"); // magic
        buf.push(1); // version
        buf.push(0); // flags (no optional sections)
        buf.extend_from_slice(&0u16.to_le_bytes()); // pad
        buf.extend_from_slice(&index_t.to_le_bytes()); // index_t
        buf.extend_from_slice(&0i64.to_le_bytes()); // base_t

        // Ledger ID
        let lid = ledger_id.as_bytes();
        buf.extend_from_slice(&(lid.len() as u16).to_le_bytes());
        buf.extend_from_slice(lid);

        buf.push(0); // subject_id_encoding = Narrow

        // Namespace codes
        buf.extend_from_slice(&(ns_codes.len() as u16).to_le_bytes());
        for &(code, prefix) in ns_codes {
            buf.extend_from_slice(&code.to_le_bytes());
            let pb = prefix.as_bytes();
            buf.extend_from_slice(&(pb.len() as u16).to_le_bytes());
            buf.extend_from_slice(pb);
        }

        // Predicate SIDs (empty)
        buf.extend_from_slice(&0u32.to_le_bytes());

        // Small dict inlines: graph_iris, datatype_iris, language_tags (all empty)
        for _ in 0..3 {
            buf.extend_from_slice(&0u16.to_le_bytes());
        }

        // Dict refs: 4 trees (each: dummy CID + 0 leaves)
        // Use a minimal valid CID (all zeros isn't valid, so we create one)
        let dummy_cid = ContentId::new(ContentKind::IndexRoot, b"dummy");
        let cid_bytes = dummy_cid.to_bytes();
        for _ in 0..4 {
            buf.extend_from_slice(&(cid_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(&cid_bytes);
            buf.extend_from_slice(&0u32.to_le_bytes()); // 0 leaves
        }

        // Numbig (empty)
        buf.extend_from_slice(&0u16.to_le_bytes());
        // Vectors (empty)
        buf.extend_from_slice(&0u16.to_le_bytes());

        // Watermarks (empty)
        buf.extend_from_slice(&0u16.to_le_bytes()); // 0 subject watermarks
        buf.extend_from_slice(&0u32.to_le_bytes()); // string_watermark = 0

        // Cumulative commit stats (3x u64 = 0)
        for _ in 0..3 {
            buf.extend_from_slice(&0u64.to_le_bytes());
        }

        // Default graph routing (0 orders)
        buf.push(0);

        // Named graph routing (0 graphs)
        buf.extend_from_slice(&0u16.to_le_bytes());

        buf
    }

    /// Helper: store IRB1 root bytes via the content store and return the CID.
    async fn store_index_root(
        storage: &MemoryStorage,
        ledger_id: &str,
        index_t: i64,
        ns_codes: &[(u16, &str)],
    ) -> ContentId {
        let store = content_store_for(storage.clone(), ledger_id);
        let bytes = build_test_irb1(ledger_id, index_t, ns_codes);
        store.put(ContentKind::IndexRoot, &bytes).await.unwrap()
    }

    /// Helper: store a commit blob via the content store and return the CID.
    async fn store_commit(
        storage: &MemoryStorage,
        ledger_id: &str,
        commit: &fluree_db_novelty::Commit,
    ) -> ContentId {
        let store = content_store_for(storage.clone(), ledger_id);
        let blob = fluree_db_novelty::commit_v2::write_commit(commit, false, None).unwrap();
        store.put(ContentKind::Commit, &blob.bytes).await.unwrap()
    }

    #[tokio::test]
    async fn test_ledger_state_new() {
        let db = Db::genesis("test:main");

        let mut novelty = Novelty::new(0);
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 1)], 1)
            .unwrap();

        let state = LedgerState::new(db, novelty);

        assert_eq!(state.ledger_id(), "test:main");
        assert_eq!(state.index_t(), 0);
        assert_eq!(state.t(), 1);
        assert_eq!(state.epoch(), 1);
    }

    #[tokio::test]
    async fn test_ledger_state_backpressure() {
        let db = Db::genesis("test:main");

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

        // Create a commit and store it via CAS
        let commit = fluree_db_novelty::Commit::new(1, vec![make_flake(1, 1, 100, 1)]);
        let cid = store_commit(&storage, "test:main", &commit).await;

        // Publish to nameservice (no index)
        ns.publish_commit("test:main", 1, &cid).await.unwrap();

        // Load ledger - should use genesis since no index exists
        let state = LedgerState::load(&ns, "test:main", storage).await.unwrap();

        assert_eq!(state.ledger_id(), "test:main");
        assert_eq!(state.index_t(), 0); // Genesis
        assert_eq!(state.t(), 1); // From commit
                                  // 1 data flake + 3 commit metadata flakes (db#address, db#alias, db#t)
        assert_eq!(state.novelty.len(), 4);
    }

    #[tokio::test]
    async fn test_apply_index_success() {
        use fluree_db_core::IndexType;

        let storage = MemoryStorage::new();
        let db = Db::genesis("test:main");

        // Create novelty with flakes at t=1 and t=2
        let mut novelty = Novelty::new(0);
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 1)], 1)
            .unwrap();
        novelty
            .apply_commit(vec![make_flake(2, 1, 200, 2)], 2)
            .unwrap();

        let mut state = LedgerState::new(db, novelty);
        assert_eq!(state.index_t(), 0);
        // Check active flakes via index iterator (arena has 2, and 2 are active)
        assert_eq!(state.novelty.iter_index(IndexType::Spot).count(), 2);

        // Create an IRB1 index root at t=1 and store via CAS
        let index_cid = store_index_root(&storage, "test:main", 1, &[(0, ""), (1, "@")]).await;
        let store = content_store_for(storage.clone(), "test:main");

        // Apply the index
        state.apply_index(&index_cid, &store).await.unwrap();

        // Index should now be at t=1
        assert_eq!(state.index_t(), 1);
        // Novelty at t=1 should be cleared, only t=2 remains in the index vectors
        // (arena still has 2 flakes but only 1 is active)
        assert_eq!(state.novelty.iter_index(IndexType::Spot).count(), 1);
    }

    #[tokio::test]
    async fn test_apply_index_address_mismatch() {
        let storage = MemoryStorage::new();
        let db = Db::genesis("test:main");
        let novelty = Novelty::new(0);

        let mut state = LedgerState::new(db, novelty);

        // Create an IRB1 root for a different ledger, but store under test:main's CAS space
        let bytes = build_test_irb1("other:ledger", 1, &[(0, "")]);
        let store = content_store_for(storage.clone(), "test:main");
        let index_cid = store.put(ContentKind::IndexRoot, &bytes).await.unwrap();

        // Should fail with ledger ID mismatch
        let result = state.apply_index(&index_cid, &store).await;
        assert!(matches!(result, Err(LedgerError::LedgerIdMismatch { .. })));
    }

    #[tokio::test]
    async fn test_apply_index_stale() {
        let storage = MemoryStorage::new();

        // Create an IRB1 root at t=2
        let index_cid_t2 = store_index_root(&storage, "test:main", 2, &[(0, "")]).await;

        // Load the Db from CAS for current state
        let store = content_store_for(storage.clone(), "test:main");
        let root_bytes = store.get(&index_cid_t2).await.unwrap();
        let db = Db::from_root_bytes(&root_bytes).unwrap();
        let novelty = Novelty::new(2);
        let mut state = LedgerState::new(db, novelty);
        assert_eq!(state.index_t(), 2);

        // Create an older IRB1 root at t=1
        let index_cid_t1 = store_index_root(&storage, "test:main", 1, &[(0, "")]).await;

        // Should fail with stale index error
        let cs = content_store_for(storage.clone(), "test:main");
        let result = state.apply_index(&index_cid_t1, &cs).await;
        assert!(matches!(result, Err(LedgerError::StaleIndex { .. })));
    }

    #[tokio::test]
    async fn test_apply_index_equal_t_noop() {
        let storage = MemoryStorage::new();

        // Create an IRB1 root at t=1
        let index_cid = store_index_root(&storage, "test:main", 1, &[(0, "")]).await;

        // Load Db from CAS
        let store = content_store_for(storage.clone(), "test:main");
        let root_bytes = store.get(&index_cid).await.unwrap();
        let db = Db::from_root_bytes(&root_bytes).unwrap();
        let novelty = Novelty::new(1);
        let mut state = LedgerState::new(db, novelty);

        // Create another IRB1 root at same t (different bytes produce different CID)
        let index_cid_same =
            store_index_root(&storage, "test:main", 1, &[(0, ""), (99, "extra")]).await;

        // Should succeed as no-op (equal t)
        let cs = content_store_for(storage.clone(), "test:main");
        let result = state.apply_index(&index_cid_same, &cs).await;
        assert!(result.is_ok());
        // Index_t should still be 1
        assert_eq!(state.index_t(), 1);
    }

    #[tokio::test]
    async fn test_maybe_trigger_index_below_threshold() {
        let db = Db::genesis("test:main");
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
        let db = Db::genesis("test:main");

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
        let db = Db::genesis("test:main");

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
        let db = Db::genesis("test:main");

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
