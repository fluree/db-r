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
use fluree_db_core::{
    content_store_for, ContentId, ContentStore, Db, Flake, FlakeMeta, FlakeValue, IndexType,
    OverlayProvider, Sid, Storage,
};
use fluree_db_nameservice::NameService;
use fluree_db_novelty::{generate_commit_flakes, trace_commits_by_id, Novelty};
use fluree_vocab::namespaces::{FLUREE_COMMIT, FLUREE_DB, JSON_LD, RDF, XSD};
use fluree_vocab::{rdf_names, xsd_names};
use futures::StreamExt;
use std::sync::Arc;

/// Reserved local name for the txn-meta named graph.
const TXN_META_GRAPH_LOCAL_NAME: &str = "txn-meta";

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
    /// * `ledger_id` - Ledger ID (e.g., "mydb:main")
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

        let store = content_store_for(storage.clone(), &record.ledger_id);

        // If the requested time is *before* the latest index_t, we cannot assume the
        // binary index can answer time-travel purely via the index. Until the index
        // format guarantees history coverage for all updates, we fall back to an
        // overlay-only reconstruction (genesis Db + commit replay up to target_t).
        //
        // When target_t >= index_t, we can use the head index and only replay commits
        // after index_t (normal fast path).
        let use_index = record.index_head_id.is_some() && target_t >= record.index_t;

        // Base db + baseline index_t for overlay range.
        let (mut db, index_t) = if use_index {
            let index_cid = record.index_head_id.as_ref().unwrap();
            let root_bytes = store.get(index_cid).await?;
            let root_json: serde_json::Value =
                serde_json::from_slice(&root_bytes).map_err(|e| {
                    fluree_db_core::Error::invalid_index(format!("invalid root JSON: {}", e))
                })?;
            let loaded = Db::from_v2_json(storage.clone(), &root_json)?;
            (loaded, record.index_t)
        } else {
            (Db::genesis(storage.clone(), &record.ledger_id), 0)
        };

        // Build novelty from commits between index_t and target_t.
        // When we are in overlay-only mode (use_index=false), this replays *all*
        // commits up to target_t (index_t=0), producing a correct time-travel snapshot
        // without relying on index history coverage.
        let overlay = if let Some(head_cid) = &record.commit_head_id {
            if target_t > index_t {
                tracing::trace!(target_t, index_t, "HistoricalLedgerView: loading novelty");
                let (novelty, ns_delta) =
                    Self::load_novelty_range(store, head_cid, index_t, target_t, &record.ledger_id)
                        .await?;

                // Apply namespace deltas to db
                for (code, prefix) in ns_delta {
                    db.namespace_codes.insert(code, prefix);
                }

                if novelty.is_empty() {
                    tracing::trace!("HistoricalLedgerView: novelty is empty");
                    None
                } else {
                    tracing::trace!(
                        epoch = novelty.epoch,
                        "HistoricalLedgerView: returning with overlay"
                    );
                    Some(Arc::new(novelty))
                }
            } else {
                tracing::trace!(target_t, index_t, "HistoricalLedgerView: no novelty needed");
                None
            }
        } else {
            tracing::trace!("HistoricalLedgerView: no commit_head_id, no novelty");
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
    /// Walks the commit chain backwards from `head_cid` using the content store,
    /// including only commits where `index_t < commit.t <= target_t`.
    async fn load_novelty_range<C: ContentStore + Clone + 'static>(
        store: C,
        head_cid: &ContentId,
        index_t: i64,
        target_t: i64,
        ledger_id: &str,
    ) -> Result<(Novelty, std::collections::HashMap<u16, String>)> {
        use std::collections::HashMap;

        tracing::trace!(
            %head_cid,
            index_t,
            target_t,
            "load_novelty_range: starting"
        );

        let mut novelty = Novelty::new(index_t);
        let mut merged_ns_delta: HashMap<u16, String> = HashMap::new();

        let stream = trace_commits_by_id(store, head_cid.clone(), index_t);
        futures::pin_mut!(stream);

        let mut commit_count = 0;
        while let Some(result) = stream.next().await {
            let commit = result?;
            commit_count += 1;
            tracing::trace!(
                commit_count,
                t = commit.t,
                flakes = commit.flakes.len(),
                "load_novelty_range: processing commit"
            );

            // Skip commits beyond target_t
            if commit.t > target_t {
                tracing::trace!(
                    t = commit.t,
                    target_t,
                    "load_novelty_range: skipping future commit"
                );
                continue;
            }

            // Derive a commit subject SID for txn-meta flakes from the CID's digest hex.
            let commit_subject = commit
                .id
                .as_ref()
                .map(|cid| Sid::new(FLUREE_COMMIT, cid.digest_hex()));

            // Derive user-provided txn-meta entries as flakes in the txn-meta named graph.
            // These are stored in the commit envelope (not in commit.flakes) and must be
            // replayed to support historical `ledger#txn-meta` views.
            let txn_meta_flakes = commit_subject.as_ref().map(|commit_sid| {
                let txn_meta_graph = Sid::new(FLUREE_DB, TXN_META_GRAPH_LOCAL_NAME);
                commit
                    .txn_meta
                    .iter()
                    .map(|entry| {
                        let p = Sid::new(entry.predicate_ns, &entry.predicate_name);
                        let (o, dt, m) = match &entry.value {
                            fluree_db_novelty::TxnMetaValue::String(s) => (
                                FlakeValue::String(s.clone()),
                                Sid::new(XSD, xsd_names::STRING),
                                None,
                            ),
                            fluree_db_novelty::TxnMetaValue::Long(n) => {
                                (FlakeValue::Long(*n), Sid::new(XSD, xsd_names::LONG), None)
                            }
                            fluree_db_novelty::TxnMetaValue::Double(n) => (
                                FlakeValue::Double(*n),
                                Sid::new(XSD, xsd_names::DOUBLE),
                                None,
                            ),
                            fluree_db_novelty::TxnMetaValue::Boolean(b) => (
                                FlakeValue::Boolean(*b),
                                Sid::new(XSD, xsd_names::BOOLEAN),
                                None,
                            ),
                            fluree_db_novelty::TxnMetaValue::Ref { ns, name } => (
                                FlakeValue::Ref(Sid::new(*ns, name)),
                                Sid::new(JSON_LD, "id"),
                                None,
                            ),
                            fluree_db_novelty::TxnMetaValue::LangString { value, lang } => (
                                FlakeValue::String(value.clone()),
                                Sid::new(RDF, rdf_names::LANG_STRING),
                                Some(FlakeMeta::with_lang(lang.clone())),
                            ),
                            fluree_db_novelty::TxnMetaValue::TypedLiteral {
                                value,
                                dt_ns,
                                dt_name,
                            } => (
                                FlakeValue::String(value.clone()),
                                Sid::new(*dt_ns, dt_name),
                                None,
                            ),
                        };

                        Flake::new_in_graph(
                            txn_meta_graph.clone(),
                            commit_sid.clone(),
                            p,
                            o,
                            dt,
                            commit.t,
                            true,
                            m,
                        )
                    })
                    .collect::<Vec<Flake>>()
            });

            let meta_flakes = generate_commit_flakes(&commit, ledger_id, commit.t);
            let meta_len = meta_flakes.len();
            let mut all_flakes = commit.flakes;
            all_flakes.extend(meta_flakes);
            if let Some(mut flakes) = txn_meta_flakes {
                all_flakes.append(&mut flakes);
            }
            tracing::trace!(
                total_flakes = all_flakes.len(),
                meta_flakes = meta_len,
                "load_novelty_range: applying commit"
            );
            novelty.apply_commit(all_flakes, commit.t)?;

            // Merge namespace deltas (newer wins - trace_commits is newest first)
            for (code, prefix) in commit.namespace_delta {
                merged_ns_delta.entry(code).or_insert(prefix);
            }
        }

        tracing::trace!(
            commit_count,
            novelty_empty = novelty.is_empty(),
            "load_novelty_range: completed"
        );
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

    /// Get the ledger ID
    pub fn ledger_id(&self) -> &str {
        &self.db.ledger_id
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
    use fluree_db_core::{
        content_store_for, ContentKind, ContentStore, FlakeValue, MemoryStorage, Sid,
    };
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

        assert_eq!(view.ledger_id(), "test:main");
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

    /// Helper: serialize a commit, store via content store, and publish to nameservice.
    /// Returns the CID of the stored commit.
    async fn store_and_publish_commit(
        storage: &MemoryStorage,
        ns: &MemoryNameService,
        ledger_id: &str,
        commit: &fluree_db_novelty::Commit,
    ) -> ContentId {
        let store = content_store_for(storage.clone(), ledger_id);
        let blob = fluree_db_novelty::commit_v2::write_commit(commit, false, None).unwrap();
        let cid = store.put(ContentKind::Commit, &blob.bytes).await.unwrap();
        ns.publish_commit(ledger_id, commit.t, &cid).await.unwrap();
        cid
    }

    #[tokio::test]
    async fn test_load_at_future_time() {
        let ns = MemoryNameService::new();
        let storage = MemoryStorage::new();

        // Create a ledger with commits up to t=5
        let commit = fluree_db_novelty::Commit::new(5, vec![make_flake(1, 1, 100, 5)]);
        store_and_publish_commit(&storage, &ns, "test:main", &commit).await;

        // Try to load at t=10 (future)
        let result = HistoricalLedgerView::load_at(&ns, "test:main", storage, 10).await;

        assert!(matches!(result, Err(LedgerError::FutureTime { .. })));
    }

    #[tokio::test]
    async fn test_load_at_genesis_fallback() {
        let ns = MemoryNameService::new();
        let storage = MemoryStorage::new();

        // Create a ledger with commits but no index
        let commit = fluree_db_novelty::Commit::new(5, vec![make_flake(1, 1, 100, 5)]);
        store_and_publish_commit(&storage, &ns, "test:main", &commit).await;

        // Load at t=5 - should use genesis db since no index exists
        let view = HistoricalLedgerView::load_at(&ns, "test:main", storage, 5)
            .await
            .unwrap();

        assert_eq!(view.ledger_id(), "test:main");
        assert_eq!(view.to_t(), 5);
        assert_eq!(view.index_t(), 0); // Genesis
        assert!(view.overlay().is_some()); // Should have novelty from commit
    }
}
