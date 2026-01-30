//! Transaction commit
//!
//! This module provides the `commit` function that persists staged changes
//! to storage and publishes to the nameservice.

use crate::address::parse_commit_id;
use crate::error::{Result, TransactError};
use crate::namespace::NamespaceRegistry;
use chrono::Utc;
use fluree_db_core::{ContentAddressedWrite, ContentKind, NodeCache, Storage};
use fluree_db_ledger::{IndexConfig, LedgerState, LedgerView};
use fluree_db_nameservice::{NameService, Publisher};
use fluree_db_novelty::generate_commit_flakes;
use fluree_db_novelty::{Commit, CommitData, CommitRef};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::Instrument;

/// Hashable commit representation (excludes address/id/time to avoid self-reference and nondeterminism)
#[derive(Serialize)]
struct CommitForHash<'a> {
    t: i64,
    flakes: &'a [fluree_db_core::Flake],
    previous: &'a Option<String>,
    namespace_delta: &'a HashMap<i32, String>,
    /// Transaction address - included in hash for content-address integrity
    txn: &'a Option<String>,
}

fn compute_commit_id(commit: &Commit) -> String {
    let previous = commit.previous_address().map(|s| s.to_string());
    let for_hash = CommitForHash {
        t: commit.t,
        flakes: &commit.flakes,
        previous: &previous,
        namespace_delta: &commit.namespace_delta,
        txn: &commit.txn,
    };

    // Use postcard for canonical binary encoding
    let canonical = postcard::to_stdvec(&for_hash).expect("postcard serialization should not fail");
    let hash = Sha256::digest(&canonical);
    format!("sha256:{}", hex::encode(hash))
}

/// Receipt returned after a successful commit
#[derive(Debug, Clone)]
pub struct CommitReceipt {
    /// Content address of the commit
    pub address: String,
    /// Commit ID (sha256 hash)
    pub commit_id: String,
    /// Transaction time of this commit
    pub t: i64,
    /// Number of flakes in the commit
    pub flake_count: usize,
}

/// Options for commit operation
#[derive(Debug, Clone, Default)]
pub struct CommitOpts {
    /// Optional commit message
    pub message: Option<String>,
    /// Optional author identity
    pub author: Option<String>,
    /// Original transaction JSON for storage (Clojure parity)
    /// When present, the raw transaction JSON is stored separately and
    /// can be retrieved via history queries with `txn: true`.
    pub raw_txn: Option<serde_json::Value>,
}

impl CommitOpts {
    /// Create new commit options with a message
    pub fn with_message(message: impl Into<String>) -> Self {
        Self {
            message: Some(message.into()),
            ..Default::default()
        }
    }

    /// Set the author
    pub fn author(mut self, author: impl Into<String>) -> Self {
        self.author = Some(author.into());
        self
    }

    /// Set the raw transaction JSON for storage (Clojure parity)
    pub fn with_raw_txn(mut self, txn: serde_json::Value) -> Self {
        self.raw_txn = Some(txn);
        self
    }
}

/// Commit a staged transaction
///
/// This function:
/// 1. Extracts flakes from the view
/// 2. Checks backpressure (novelty not at max)
/// 3. Verifies sequencing (t and previous match expected)
/// 4. Builds and content-addresses the commit record
/// 5. Writes to storage
/// 6. Publishes to nameservice
/// 7. Returns new LedgerState with updated novelty
///
/// # Arguments
///
/// * `view` - The staged ledger view
/// * `ns_registry` - Namespace registry with any new allocations
/// * `storage` - Storage backend
/// * `nameservice` - Nameservice for lookup and publishing
/// * `index_config` - Configuration for backpressure limits
/// * `opts` - Commit options (message, author, raw_txn for storage)
///
/// # Returns
///
/// A tuple of (CommitReceipt, new LedgerState)
pub async fn commit<S, C, N>(
    view: LedgerView<S, C>,
    mut ns_registry: NamespaceRegistry,
    storage: &S,
    nameservice: &N,
    index_config: &IndexConfig,
    opts: CommitOpts,
) -> Result<(CommitReceipt, LedgerState<S, C>)>
where
    S: Storage + ContentAddressedWrite + Clone + 'static,
    C: NodeCache,
    N: NameService + Publisher,
{
    let span = tracing::debug_span!(
        "commit",
        flake_count = tracing::field::Empty,
        bytes_written = tracing::field::Empty,
    );
    async {
        // 1. Extract flakes from view
        let (mut base, flakes) = view.into_parts();

        // 2. Check for empty transaction
        if flakes.is_empty() {
            return Err(TransactError::EmptyTransaction);
        }

        // 3. Check backpressure - current novelty at max
        if base.at_max_novelty(index_config) {
            return Err(TransactError::NoveltyAtMax);
        }

        // 4. Predictive sizing - would these flakes reach or exceed max?
        let delta_bytes: usize = flakes.iter().map(|f| f.size_bytes()).sum();
        let current_bytes = base.novelty_size();
        let max_bytes = index_config.reindex_max_bytes;
        if current_bytes + delta_bytes >= max_bytes {
            return Err(TransactError::NoveltyWouldExceed {
                current_bytes,
                delta_bytes,
                max_bytes,
            });
        }

        // 5. Verify sequencing
        async {
            let current = nameservice.lookup(base.alias()).await?;
            verify_sequencing(&base, current.as_ref())?;
            Ok::<_, TransactError>(())
        }
        .instrument(tracing::debug_span!("verify_seq"))
        .await?;

        // 6. Build commit record
        let new_t = base.t() + 1;
        let flake_count = flakes.len();
        tracing::Span::current().record("flake_count", flake_count);

        // Capture namespace delta once:
        // - write into commit record for persistence
        // - apply to returned in-memory Db so subsequent operations (e.g., SPARQL/JSON-LD queries)
        //   can encode IRIs without requiring a reload.
        let ns_delta = ns_registry.take_delta();

        // Apply namespace delta to the in-memory Db immediately (Clojure parity).
        for (code, prefix) in &ns_delta {
            base.db.namespace_codes.insert(*code, prefix.clone());
        }

        // Generate ISO 8601 timestamp
        let timestamp = Utc::now().to_rfc3339();

        // Store original transaction JSON if provided (Clojure parity)
        let txn_address = async {
            if let Some(txn_json) = &opts.raw_txn {
                let txn_bytes = serde_json::to_vec(txn_json)?;
                let res = storage
                    .content_write_bytes(ContentKind::Txn, base.alias(), &txn_bytes)
                    .await?;
                Ok::<_, TransactError>(Some(res.address))
            } else {
                Ok(None)
            }
        }
        .instrument(tracing::debug_span!("raw_txn_write"))
        .await?;

        let mut commit_record = Commit::new("", new_t, flakes.clone())
            .with_namespace_delta(ns_delta)
            .with_time(timestamp);

        // Add txn address to commit record (must be before computing commit ID)
        if let Some(txn_addr) = &txn_address {
            commit_record = commit_record.with_txn(txn_addr.clone());
        }

        // Build previous commit reference with id and address
        if let Some(prev_addr) = &base.head_commit {
            // Try to extract commit ID from previous address
            let prev_id = parse_commit_id(prev_addr).map(|id| format!("fluree:commit:{}", id));
            let mut prev_ref = CommitRef::new(prev_addr.clone());
            if let Some(id) = prev_id {
                prev_ref = prev_ref.with_id(id);
            }
            commit_record = commit_record.with_previous_ref(prev_ref);
        }

        // Build cumulative DB metadata
        // Get current stats if available, otherwise use defaults
        let (cumulative_flakes, cumulative_size) = if let Some(stats) = &base.db.stats {
            (
                stats.flakes + flake_count as u64,
                stats.size + delta_bytes as u64,
            )
        } else {
            // No indexed stats yet - use novelty as baseline
            let novelty_flakes = base.novelty.len() as u64;
            (
                novelty_flakes + flake_count as u64,
                (base.novelty_size() + delta_bytes) as u64,
            )
        };

        let commit_data = CommitData {
            id: None,      // Will be set after content-addressing
            address: None, // DB address not tracked separately in Rust yet
            flakes: cumulative_flakes,
            size: cumulative_size,
            previous: None, // Previous DB reference not tracked yet
        };
        commit_record = commit_record.with_data(commit_data);

        // 7. Content-address + write (storage-owned)
        //
        // Important: the on-disk commit blob is written *without* `address` / `id`
        // set (to avoid self-reference). We inject them into the in-memory struct
        // after the write for downstream logic (metadata flakes, receipt, etc.).
        // Compute stable commit ID (intentionally excludes wall-clock timestamp).
        let commit_id = compute_commit_id(&commit_record);
        let commit_hash_hex = commit_id
            .strip_prefix("sha256:")
            .unwrap_or(commit_id.as_str());

        let bytes = serde_json::to_vec(&commit_record)?;
        let storage_write_bytes = bytes.len();
        let address = async {
            let write_res = storage
                .content_write_bytes_with_hash(
                    ContentKind::Commit,
                    base.alias(),
                    commit_hash_hex,
                    &bytes,
                )
                .await?;
            Ok::<_, TransactError>(write_res.address)
        }
        .instrument(tracing::debug_span!(
            "storage_write",
            bytes_written = storage_write_bytes,
        ))
        .await?;

        // Update in-memory commit with its address and content ID
        commit_record.address = address.clone();
        commit_record.id = Some(format!("fluree:commit:{}", commit_id));

        // 8. Publish to nameservice
        async {
            nameservice
                .publish_commit(base.alias(), &address, new_t)
                .await
        }
        .instrument(tracing::debug_span!("ns_publish"))
        .await?;

        // 9. Generate commit metadata flakes + build new state
        let new_state = {
            let state_build_span = tracing::debug_span!("state_build");
            let _guard = state_build_span.enter();

            // Generate commit metadata flakes (Clojure parity)
            // Note: We merge these into novelty only, not into commit_record.flakes
            // (matching Clojure's behavior where metadata flakes are derived separately)
            let commit_metadata_flakes =
                generate_commit_flakes(&commit_record, base.alias(), new_t);

            // Build new state - merge commit_metadata_flakes with transaction flakes
            let mut all_flakes = flakes;
            all_flakes.extend(commit_metadata_flakes);
            let flake_count = all_flakes.len();

            let mut new_novelty = {
                let _guard = tracing::debug_span!("novelty_clone").entered();
                (*base.novelty).clone()
            };
            {
                let span = tracing::debug_span!("novelty_apply", flake_count);
                let _guard = span.enter();
                new_novelty.apply_commit(all_flakes, new_t)?;
            }

            LedgerState {
                db: base.db,
                novelty: Arc::new(new_novelty),
                head_commit: Some(address.clone()),
                ns_record: base.ns_record,
            }
        };

        let receipt = CommitReceipt {
            address,
            commit_id,
            t: new_t,
            flake_count,
        };

        Ok((receipt, new_state))
    }
    .instrument(span)
    .await
}

/// Verify that this commit follows the expected sequence
fn verify_sequencing<S, C>(
    base: &LedgerState<S, C>,
    current: Option<&fluree_db_nameservice::NsRecord>,
) -> Result<()>
where
    S: Storage + Clone + 'static,
    C: NodeCache,
{
    match current {
        None => {
            // Genesis case: no record exists yet
            // Base should have no head_commit and t=0
            if base.head_commit.is_some() {
                return Err(TransactError::CommitConflict {
                    expected_t: 0,
                    head_t: base.t(),
                });
            }
            if base.t() != 0 {
                return Err(TransactError::CommitConflict {
                    expected_t: 0,
                    head_t: base.t(),
                });
            }
            Ok(())
        }
        Some(record) => {
            // Normal case: verify both t and previous
            if base.t() != record.commit_t {
                return Err(TransactError::CommitConflict {
                    expected_t: record.commit_t,
                    head_t: base.t(),
                });
            }

            // Verify previous address matches
            match (&base.head_commit, &record.commit_address) {
                (Some(base_prev), Some(record_prev)) if base_prev != record_prev => {
                    return Err(TransactError::AddressMismatch {
                        expected: record_prev.clone(),
                        found: base_prev.clone(),
                    });
                }
                (None, Some(record_prev)) => {
                    return Err(TransactError::AddressMismatch {
                        expected: record_prev.clone(),
                        found: "None".to_string(),
                    });
                }
                (Some(base_prev), None) => {
                    return Err(TransactError::AddressMismatch {
                        expected: "None".to_string(),
                        found: base_prev.clone(),
                    });
                }
                _ => {}
            }

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{TemplateTerm, TripleTemplate, Txn};
    use crate::stage::{stage, StageOptions};
    use fluree_db_core::{Db, FlakeValue, MemoryStorage, NoCache, Sid};
    use fluree_db_nameservice::memory::MemoryNameService;
    use fluree_db_novelty::Novelty;

    #[tokio::test]
    async fn test_commit_simple_insert() {
        let storage = MemoryStorage::new();
        let cache = NoCache::new();
        let db = Db::genesis(storage.clone(), cache, "test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();

        // Stage an insert
        let txn = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.db);
        let (view, ns_registry) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        // Commit
        let config = IndexConfig::default();
        let (receipt, new_state) = commit(
            view,
            ns_registry,
            &storage,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        assert_eq!(receipt.t, 1);
        assert_eq!(receipt.flake_count, 1);
        assert!(receipt.commit_id.starts_with("sha256:"));
        assert_eq!(new_state.t(), 1);
        assert!(new_state.head_commit.is_some());
    }

    #[tokio::test]
    async fn test_commit_empty_transaction() {
        let storage = MemoryStorage::new();
        let cache = NoCache::new();
        let db = Db::genesis(storage.clone(), cache, "test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();

        // Stage an empty transaction (no inserts)
        let txn = Txn::insert();
        let ns_registry = NamespaceRegistry::from_db(&ledger.db);
        let (view, ns_registry) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        // Commit should fail
        let config = IndexConfig::default();
        let result = commit(
            view,
            ns_registry,
            &storage,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await;

        assert!(matches!(result, Err(TransactError::EmptyTransaction)));
    }

    #[tokio::test]
    async fn test_commit_sequence() {
        let storage = MemoryStorage::new();
        let cache = NoCache::new();
        let db = Db::genesis(storage.clone(), cache, "test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();
        let config = IndexConfig::default();

        // First commit
        let txn1 = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.db);
        let (view1, ns_registry1) = stage(ledger, txn1, ns_registry, StageOptions::default())
            .await
            .unwrap();
        let (receipt1, state1) = commit(
            view1,
            ns_registry1,
            &storage,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        assert_eq!(receipt1.t, 1);

        // Second commit
        let txn2 = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:bob")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Bob".to_string())),
        ));

        let ns_registry2 = NamespaceRegistry::from_db(&state1.db);
        let (view2, ns_registry2) = stage(state1, txn2, ns_registry2, StageOptions::default())
            .await
            .unwrap();
        let (receipt2, state2) = commit(
            view2,
            ns_registry2,
            &storage,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        assert_eq!(receipt2.t, 2);
        assert_eq!(state2.t(), 2);
        // Novelty includes transaction flakes + commit metadata flakes
        // 2 txn flakes + 8 metadata (commit 1, no previous) + 9 metadata (commit 2, has previous) = 19
        assert!(
            state2.novelty.len() >= 2,
            "novelty should include at least 2 transaction flakes"
        );
    }

    #[tokio::test]
    async fn test_commit_predictive_sizing() {
        let storage = MemoryStorage::new();
        let cache = NoCache::new();
        let db = Db::genesis(storage.clone(), cache, "test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();

        // Create a transaction with a large string value
        let big_value = "x".repeat(1000);
        let txn = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:bio")),
            TemplateTerm::Value(FlakeValue::String(big_value)),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.db);
        let (view, ns_registry) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        // Use a very small max to trigger predictive sizing error
        let config = IndexConfig {
            reindex_min_bytes: 50,
            reindex_max_bytes: 100, // Smaller than the big flake
        };

        let result = commit(
            view,
            ns_registry,
            &storage,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await;

        // Should fail with NoveltyWouldExceed
        assert!(matches!(
            result,
            Err(TransactError::NoveltyWouldExceed { .. })
        ));
    }
}
