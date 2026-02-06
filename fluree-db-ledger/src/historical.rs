//! Historical ledger view for time-travel queries
//!
//! This module provides `HistoricalLedgerView` for querying a ledger at a specific
//! point in time. Unlike `LedgerState` which represents the live, mutable head,
//! `HistoricalLedgerView` is read-only and time-bounded.
//!
//! # Design
//!
//! A historical view consists of:
//! - A `Db` loaded from the head index (or genesis if no index exists)
//! - An optional `Novelty` overlay for commits between `index_t` and `target_t`
//! - A `to_t` field that bounds all queries
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_ledger::HistoricalLedgerView;
//!
//! // Load a view at t=50
//! let view = HistoricalLedgerView::load_at(
//!     &ns, "mydb:main", storage, 50
//! ).await?;
//!
//! // Query using the view as an overlay provider
//! execute_pattern_with_overlay(&view.db, view.overlay(), &vars, &pattern, view.to_t()).await?;
//! ```

use crate::error::{LedgerError, Result};
use fluree_db_core::{Db, Flake, IndexType, OverlayProvider, Storage};
use fluree_db_nameservice::NameService;
use fluree_db_novelty::{generate_commit_flakes, trace_commits, Novelty};
use futures::StreamExt;
use std::sync::Arc;

/// Read-only ledger view for time-bounded historical queries
///
/// This struct provides a consistent view of a ledger at a specific point in time.
/// It combines:
/// - The head index (or genesis if no index exists yet)
/// - An optional novelty overlay (commits between `index_t` and `to_t`)
///
/// Indexes are cumulative and contain all historical data, so the head index
/// is always valid for any query. The `to_t` field bounds query results.
///
/// Unlike `LedgerState`, this is immutable and cannot be updated.
#[derive(Debug)]
pub struct HistoricalLedgerView<S> {
    /// The indexed database (head index or genesis)
    pub db: Db<S>,
    /// Optional novelty overlay (commits between index_t and to_t)
    overlay: Option<Arc<Novelty>>,
    /// Time bound for all queries
    to_t: i64,
}

impl<S: Storage + Clone + 'static> HistoricalLedgerView<S> {
    /// Load a historical view of a ledger at a specific time
    ///
    /// # Algorithm
    ///
    /// 1. Use head index if available (indexes are cumulative, contain all historical data)
    /// 2. Fall back to genesis only if no index exists
    /// 3. Build novelty overlay from commits in `(index_t, target_t]` if needed
    ///
    /// # Arguments
    ///
    /// * `ns` - Nameservice for ledger discovery
    /// * `alias` - Ledger alias (e.g., "mydb:main")
    /// * `storage` - Storage backend
    /// * `target_t` - The time to load the view at
    ///
    /// # Errors
    ///
    /// - `NotFound` if the ledger doesn't exist
    /// - `FutureTime` if `target_t` is beyond the current head
    pub async fn load_at<N: NameService>(
        ns: &N,
        alias: &str,
        storage: S,
        target_t: i64,
    ) -> Result<Self> {
        let record = ns
            .lookup(alias)
            .await?
            .ok_or_else(|| LedgerError::not_found(alias))?;

        // Check if target_t is in the future
        if target_t > record.commit_t {
            return Err(LedgerError::future_time(alias, target_t, record.commit_t));
        }

        // Always use head index if available (indexes are cumulative, contain all historical data)
        // Fall back to genesis only if no index exists yet
        let (db, index_t) = if let Some(addr) = &record.index_address {
            let db = Db::load(storage.clone(), addr).await?;
            (db, record.index_t)
        } else {
            let db = Db::genesis(storage.clone(), &record.address);
            (db, 0)
        };

        // Build novelty from commits between index_t and target_t
        let overlay = if let Some(commit_addr) = &record.commit_address {
            if target_t > index_t {
                let (novelty, ns_delta) = Self::load_novelty_range(
                    storage,
                    commit_addr,
                    index_t,
                    target_t,
                    &record.address,
                )
                .await?;

                // Apply namespace deltas to db (we need a mutable copy)
                let mut db = db;
                for (code, prefix) in ns_delta {
                    db.namespace_codes.insert(code, prefix);
                }

                if novelty.is_empty() {
                    return Ok(Self {
                        db,
                        overlay: None,
                        to_t: target_t,
                    });
                }

                return Ok(Self {
                    db,
                    overlay: Some(Arc::new(novelty)),
                    to_t: target_t,
                });
            }
            None
        } else {
            None
        };

        Ok(Self {
            db,
            overlay,
            to_t: target_t,
        })
    }

    /// Load novelty from commits within a specific range
    ///
    /// Loads commits from `head_address` backwards, including only commits
    /// where `index_t < commit.t <= target_t`.
    async fn load_novelty_range(
        storage: S,
        head_address: &str,
        index_t: i64,
        target_t: i64,
        ledger_alias: &str,
    ) -> Result<(Novelty, std::collections::HashMap<u16, String>)> {
        use std::collections::HashMap;

        let mut novelty = Novelty::new(index_t);
        let mut merged_ns_delta: HashMap<u16, String> = HashMap::new();

        // trace_commits streams from HEAD backwards, stopping at index_t
        let stream = trace_commits(storage, head_address.to_string(), index_t);
        futures::pin_mut!(stream);

        while let Some(result) = stream.next().await {
            let commit = result?;

            // Skip commits beyond target_t
            if commit.t > target_t {
                continue;
            }

            let meta_flakes = generate_commit_flakes(&commit, ledger_alias, commit.t);
            let mut all_flakes = commit.flakes;
            all_flakes.extend(meta_flakes);
            novelty.apply_commit(all_flakes, commit.t)?;

            // Merge namespace deltas (newer wins - trace_commits is newest first)
            for (code, prefix) in commit.namespace_delta {
                merged_ns_delta.entry(code).or_insert(prefix);
            }
        }

        Ok((novelty, merged_ns_delta))
    }

    /// Create a historical view directly from components
    ///
    /// This is useful for testing or when you've already loaded the components.
    pub fn new(db: Db<S>, overlay: Option<Arc<Novelty>>, to_t: i64) -> Self {
        Self { db, overlay, to_t }
    }

    /// Get the time bound for this view
    pub fn to_t(&self) -> i64 {
        self.to_t
    }

    /// Get the index time (when the db was indexed)
    pub fn index_t(&self) -> i64 {
        self.db.t
    }

    /// Get the ledger alias
    pub fn alias(&self) -> &str {
        &self.db.alias
    }

    /// Get the overlay if present
    pub fn overlay(&self) -> Option<&Arc<Novelty>> {
        self.overlay.as_ref()
    }

    /// Get the overlay as an OverlayProvider reference
    ///
    /// Returns the novelty overlay if present, which can be used with
    /// `execute_pattern_with_overlay` and similar functions.
    pub fn overlay_provider(&self) -> Option<&dyn OverlayProvider> {
        self.overlay
            .as_ref()
            .map(|n| n.as_ref() as &dyn OverlayProvider)
    }
}

/// OverlayProvider implementation for HistoricalLedgerView
///
/// This allows the view to be used directly as an overlay provider in queries.
/// The `to_t` filtering is handled automatically.
impl<S: Storage> OverlayProvider for HistoricalLedgerView<S> {
    fn epoch(&self) -> u64 {
        self.overlay.as_ref().map(|n| n.epoch).unwrap_or(0)
    }

    fn for_each_overlay_flake(
        &self,
        index: IndexType,
        first: Option<&Flake>,
        rhs: Option<&Flake>,
        leftmost: bool,
        to_t: i64,
        callback: &mut dyn FnMut(&Flake),
    ) {
        if let Some(novelty) = &self.overlay {
            // Use the minimum of the requested to_t and our view's to_t
            let effective_to_t = to_t.min(self.to_t);
            novelty.for_each_overlay_flake(index, first, rhs, leftmost, effective_to_t, callback);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeValue, MemoryStorage, Sid};
    use fluree_db_nameservice::memory::MemoryNameService;
    use fluree_db_nameservice::Publisher;

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

    #[tokio::test]
    async fn test_historical_view_new() {
        let storage = MemoryStorage::new();
        let db = Db::genesis(storage, "test:main");

        let view = HistoricalLedgerView::new(db, None, 10);

        assert_eq!(view.alias(), "test:main");
        assert_eq!(view.to_t(), 10);
        assert_eq!(view.index_t(), 0);
        assert!(view.overlay().is_none());
    }

    #[tokio::test]
    async fn test_historical_view_with_overlay() {
        let storage = MemoryStorage::new();
        let db = Db::genesis(storage, "test:main");

        let mut novelty = Novelty::new(0);
        novelty
            .apply_commit(vec![make_flake(1, 1, 100, 1)], 1)
            .unwrap();

        let view = HistoricalLedgerView::new(db, Some(Arc::new(novelty)), 10);

        assert_eq!(view.to_t(), 10);
        assert!(view.overlay().is_some());
        assert_eq!(view.epoch(), 1);
    }

    #[tokio::test]
    async fn test_historical_view_overlay_provider() {
        let storage = MemoryStorage::new();
        let db = Db::genesis(storage, "test:main");

        let mut novelty = Novelty::new(0);
        novelty
            .apply_commit(
                vec![
                    make_flake(1, 1, 100, 1),
                    make_flake(2, 1, 200, 5),
                    make_flake(3, 1, 300, 10),
                ],
                10,
            )
            .unwrap();

        // View at t=5 should only see flakes with t <= 5
        let view = HistoricalLedgerView::new(db, Some(Arc::new(novelty)), 5);

        let mut collected = Vec::new();
        view.for_each_overlay_flake(IndexType::Spot, None, None, true, 100, &mut |f| {
            collected.push(f.s.namespace_code);
        });

        // Should only see flakes at t=1 and t=5, not t=10
        assert_eq!(collected, vec![1, 2]);
    }

    #[tokio::test]
    async fn test_load_at_not_found() {
        let ns = MemoryNameService::new();
        let storage = MemoryStorage::new();

        let result = HistoricalLedgerView::load_at(&ns, "nonexistent:main", storage, 10).await;

        assert!(matches!(result, Err(LedgerError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_load_at_future_time() {
        let ns = MemoryNameService::new();
        let storage = MemoryStorage::new();

        // Create a ledger with commits up to t=5
        ns.publish_commit("test:main", "commit-5", 5).await.unwrap();

        // Store the commit as v2 binary
        let commit = fluree_db_novelty::Commit::new("commit-5", 5, vec![make_flake(1, 1, 100, 5)]);
        let blob = fluree_db_novelty::commit_v2::write_commit(&commit, false, None).unwrap();
        storage.insert("commit-5", blob.bytes);

        // Try to load at t=10 (future)
        let result = HistoricalLedgerView::load_at(&ns, "test:main", storage, 10).await;

        assert!(matches!(result, Err(LedgerError::FutureTime { .. })));
    }

    #[tokio::test]
    async fn test_load_at_genesis_fallback() {
        let ns = MemoryNameService::new();
        let storage = MemoryStorage::new();

        // Create a ledger with commits but no index
        ns.publish_commit("test:main", "commit-5", 5).await.unwrap();

        // Store the commit as v2 binary
        let commit = fluree_db_novelty::Commit::new("commit-5", 5, vec![make_flake(1, 1, 100, 5)]);
        let blob = fluree_db_novelty::commit_v2::write_commit(&commit, false, None).unwrap();
        storage.insert("commit-5", blob.bytes);

        // Load at t=5 - should use genesis db since no index exists
        let view = HistoricalLedgerView::load_at(&ns, "test:main", storage, 5)
            .await
            .unwrap();

        assert_eq!(view.alias(), "test:main");
        assert_eq!(view.to_t(), 5);
        assert_eq!(view.index_t(), 0); // Genesis
        assert!(view.overlay().is_some()); // Should have novelty from commit
    }
}
