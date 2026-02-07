//! Transaction commit
//!
//! This module provides the `commit` function that persists staged changes
//! to storage and publishes to the nameservice.

use crate::address::parse_commit_id;
use crate::error::{Result, TransactError};
use crate::namespace::NamespaceRegistry;
use chrono::Utc;
use fluree_db_core::{ContentAddressedWrite, ContentKind, DictNovelty, Flake, FlakeValue, Storage};
use fluree_db_ledger::{IndexConfig, LedgerState, LedgerView};
use fluree_db_nameservice::{NameService, Publisher};
use fluree_db_novelty::generate_commit_flakes;
use fluree_db_novelty::{Commit, CommitData, CommitRef, SigningKey, TxnMetaEntry, TxnSignature};
use std::sync::Arc;
use tracing::Instrument;

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
#[derive(Clone, Default)]
pub struct CommitOpts {
    /// Optional commit message
    pub message: Option<String>,
    /// Optional author identity
    pub author: Option<String>,
    /// Original transaction JSON for storage (Clojure parity)
    /// When present, the raw transaction JSON is stored separately and
    /// can be retrieved via history queries with `txn: true`.
    pub raw_txn: Option<serde_json::Value>,
    /// Ed25519 signing key for commit signatures (opt-in).
    /// When set, the commit blob includes a trailing signature block.
    pub signing_key: Option<Arc<SigningKey>>,
    /// Transaction signature (audit metadata: who submitted the transaction).
    pub txn_signature: Option<TxnSignature>,
    /// User-provided transaction metadata.
    ///
    /// Stored in the commit envelope and emitted to the txn-meta graph (`g_id=1`)
    /// during indexing. Each entry becomes a triple with the commit as subject.
    pub txn_meta: Vec<TxnMetaEntry>,
    /// Named graph IRI to g_id mappings introduced by this transaction.
    ///
    /// Stored in the commit envelope for replay-safe persistence. The indexer
    /// uses this to resolve graph IRIs to dictionary IDs when building the index.
    pub graph_delta: std::collections::HashMap<u32, String>,
}

impl std::fmt::Debug for CommitOpts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitOpts")
            .field("message", &self.message)
            .field("author", &self.author)
            .field("raw_txn", &self.raw_txn.is_some())
            .field("signing_key", &self.signing_key.is_some())
            .field(
                "txn_signature",
                &self.txn_signature.as_ref().map(|s| &s.signer),
            )
            .field("txn_meta_count", &self.txn_meta.len())
            .field("graph_delta_count", &self.graph_delta.len())
            .finish()
    }
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

    /// Set the signing key for commit signatures
    pub fn with_signing_key(mut self, key: Arc<SigningKey>) -> Self {
        self.signing_key = Some(key);
        self
    }

    /// Set the transaction signature (audit metadata)
    pub fn with_txn_signature(mut self, sig: TxnSignature) -> Self {
        self.txn_signature = Some(sig);
        self
    }

    /// Set the user-provided transaction metadata
    pub fn with_txn_meta(mut self, txn_meta: Vec<TxnMetaEntry>) -> Self {
        self.txn_meta = txn_meta;
        self
    }

    /// Set the named graph delta (g_id -> IRI mappings)
    pub fn with_graph_delta(mut self, graph_delta: std::collections::HashMap<u32, String>) -> Self {
        self.graph_delta = graph_delta;
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
pub async fn commit<S, N>(
    view: LedgerView<S>,
    mut ns_registry: NamespaceRegistry,
    storage: &S,
    nameservice: &N,
    index_config: &IndexConfig,
    opts: CommitOpts,
) -> Result<(CommitReceipt, LedgerState<S>)>
where
    S: Storage + ContentAddressedWrite + Clone + 'static,
    N: NameService + Publisher,
{
    // 1. Extract flakes from view
    let (mut base, flakes) = view.into_parts();

    let commit_span = tracing::info_span!(
        "txn_commit",
        alias = base.alias(),
        base_t = base.t(),
        flake_count = tracing::field::Empty,
        delta_bytes = tracing::field::Empty,
        current_novelty_bytes = tracing::field::Empty,
        max_novelty_bytes = index_config.reindex_max_bytes,
        has_raw_txn = opts.raw_txn.is_some(),
    );

    async {
        let span = tracing::Span::current();

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
        span.record("flake_count", flakes.len());
        span.record("delta_bytes", delta_bytes);
        span.record("current_novelty_bytes", current_bytes);
        if current_bytes + delta_bytes >= max_bytes {
            return Err(TransactError::NoveltyWouldExceed {
                current_bytes,
                delta_bytes,
                max_bytes,
            });
        }

        // 5. Verify sequencing
        let current = nameservice
            .lookup(base.alias())
            .instrument(tracing::info_span!("commit_nameservice_lookup"))
            .await?;
        {
            let span = tracing::info_span!("commit_verify_sequencing");
            let _g = span.enter();
            verify_sequencing(&base, current.as_ref())?;
        }

        // 6. Build commit record
        let new_t = base.t() + 1;
        let flake_count = flakes.len();

        // Capture namespace delta once:
        // - write into commit record for persistence
        // - apply to returned in-memory Db so subsequent operations (e.g., SPARQL/JSON-LD queries)
        //   can encode IRIs without requiring a reload.
        let ns_delta = {
            let span = tracing::info_span!("commit_namespace_delta");
            let _g = span.enter();
            ns_registry.take_delta()
        };

        // Apply namespace delta to the in-memory Db immediately (Clojure parity).
        for (code, prefix) in &ns_delta {
            base.db.namespace_codes.insert(*code, prefix.clone());
        }

        // Generate ISO 8601 timestamp
        // TODO: Refactor to accept an optional timestamp via CommitOpts instead of calling
        // Utc::now() directly. This would enable deterministic commit hashes for testing
        // (see fluree-db-api/tests/it_stable_hashes.rs) and allow callers to provide
        // externally-sourced timestamps when needed.
        let timestamp = Utc::now().to_rfc3339();

        // Store original transaction JSON if provided (Clojure parity)
        let txn_address = if let Some(txn_json) = &opts.raw_txn {
            let txn_bytes = serde_json::to_vec(txn_json)?;
            let res = storage
                .content_write_bytes(ContentKind::Txn, base.alias(), &txn_bytes)
                .instrument(tracing::info_span!("commit_write_raw_txn"))
                .await?;
            tracing::info!(raw_txn_bytes = txn_bytes.len(), "raw txn stored");
            Some(res.address)
        } else {
            None
        };

        let mut commit_record = {
            let span = tracing::info_span!("commit_build_record");
            let _g = span.enter();
            Commit::new("", new_t, flakes.clone())
                .with_namespace_delta(ns_delta)
                .with_time(timestamp)
        };

        // Add txn address to commit record (must be before computing commit ID)
        if let Some(txn_addr) = &txn_address {
            commit_record = commit_record.with_txn(txn_addr.clone());
        }

        // Add txn signature if provided (audit metadata)
        if let Some(txn_sig) = opts.txn_signature {
            commit_record = commit_record.with_txn_signature(txn_sig);
        }

        // Add user-provided transaction metadata
        if !opts.txn_meta.is_empty() {
            commit_record = commit_record.with_txn_meta(opts.txn_meta.clone());
        }

        // Add named graph delta (g_id -> IRI mappings)
        if !opts.graph_delta.is_empty() {
            commit_record.graph_delta = opts.graph_delta.clone();
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

        // The blob's trailing SHA-256 IS the content address.
        let (commit_id, commit_hash_hex, bytes) = {
            let span = tracing::info_span!("commit_write_commit_blob");
            let _g = span.enter();
            let signing = opts
                .signing_key
                .as_ref()
                .map(|key| (key.as_ref(), base.alias()));
            let result = crate::commit_v2::write_commit(&commit_record, true, signing)?;
            let commit_id = format!("sha256:{}", &result.content_hash_hex);
            (commit_id, result.content_hash_hex, result.bytes)
        };

        let write_res = storage
            .content_write_bytes_with_hash(
                ContentKind::Commit,
                base.alias(),
                &commit_hash_hex,
                &bytes,
            )
            .await?;
        tracing::info!(commit_bytes = bytes.len(), "commit blob stored");
        let address = write_res.address;

        // Update in-memory commit with its address and content ID
        commit_record.address = address.clone();
        commit_record.id = Some(format!("fluree:commit:{}", commit_id));

        // 8. Publish to nameservice
        nameservice
            .publish_commit(base.alias(), &address, new_t)
            .instrument(tracing::info_span!("commit_publish_nameservice"))
            .await?;

        // 9. Generate commit metadata flakes (Clojure parity)
        // Note: We merge these into novelty only, not into commit_record.flakes
        // (matching Clojure's behavior where metadata flakes are derived separately)
        let commit_metadata_flakes = {
            let span = tracing::info_span!("commit_generate_metadata_flakes");
            let _g = span.enter();
            generate_commit_flakes(&commit_record, base.alias(), new_t)
        };
        tracing::info!(
            metadata_flakes = commit_metadata_flakes.len(),
            "commit metadata flakes generated"
        );

        // 10. Build new state - merge commit_metadata_flakes with transaction flakes
        let mut all_flakes = flakes;
        all_flakes.extend(commit_metadata_flakes);

        // 10.1 Populate DictNovelty with subjects/strings from this commit
        let mut dict_novelty = base.dict_novelty.clone();
        {
            let span = tracing::info_span!("commit_populate_dict_novelty");
            let _g = span.enter();
            populate_dict_novelty(Arc::make_mut(&mut dict_novelty), &all_flakes);
        }

        let mut new_novelty = (*base.novelty).clone();
        {
            let span = tracing::info_span!("commit_apply_to_novelty");
            let _g = span.enter();
            new_novelty.apply_commit(all_flakes, new_t)?;
        }

        let new_state = LedgerState {
            db: base.db,
            novelty: Arc::new(new_novelty),
            dict_novelty,
            head_commit: Some(address.clone()),
            ns_record: base.ns_record,
            binary_store: base.binary_store,
        };

        let receipt = CommitReceipt {
            address,
            commit_id,
            t: new_t,
            flake_count,
        };

        Ok((receipt, new_state))
    }
    .instrument(commit_span)
    .await
}

/// Populate DictNovelty with subjects and strings from committed flakes.
///
/// Scans each flake for:
/// - Subject IDs (`flake.s`) — registered as novel subjects
/// - Object references (`FlakeValue::Ref`) — registered as novel subjects
/// - String values (`FlakeValue::String`, `FlakeValue::Json`) — registered as novel strings
///
/// Does NOT check the persisted tree — some entries may shadow persisted subjects.
/// This is safe because `DictOverlay` checks the persisted tree first for reverse
/// lookups (canonical ID wins).
fn populate_dict_novelty(dict_novelty: &mut DictNovelty, flakes: &[Flake]) {
    dict_novelty.ensure_initialized();

    for flake in flakes {
        // Subject
        dict_novelty
            .subjects
            .assign_or_lookup(flake.s.namespace_code, &flake.s.name);

        // Object references
        if let FlakeValue::Ref(ref sid) = flake.o {
            dict_novelty
                .subjects
                .assign_or_lookup(sid.namespace_code, &sid.name);
        }

        // String values
        match &flake.o {
            FlakeValue::String(s) | FlakeValue::Json(s) => {
                dict_novelty.strings.assign_or_lookup(s);
            }
            _ => {}
        }
    }
}

/// Verify that this commit follows the expected sequence
fn verify_sequencing<S>(
    base: &LedgerState<S>,
    current: Option<&fluree_db_nameservice::NsRecord>,
) -> Result<()>
where
    S: Storage + Clone + 'static,
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
    use fluree_db_core::{Db, FlakeValue, MemoryStorage, Sid};
    use fluree_db_nameservice::memory::MemoryNameService;
    use fluree_db_novelty::Novelty;

    #[tokio::test]
    async fn test_commit_simple_insert() {
        let storage = MemoryStorage::new();
        let db = Db::genesis(storage.clone(), "test:main");
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
        let db = Db::genesis(storage.clone(), "test:main");
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
        let db = Db::genesis(storage.clone(), "test:main");
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
        let db = Db::genesis(storage.clone(), "test:main");
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
