//! Transaction commit
//!
//! This module provides the `commit` function that persists staged changes
//! to storage and publishes to the nameservice.

use crate::error::{Result, TransactError};
use crate::namespace::NamespaceRegistry;
use chrono::Utc;
use fluree_db_core::{
    ContentAddressedWrite, ContentId, ContentKind, DictNovelty, Flake, FlakeValue, Storage,
    CODEC_FLUREE_COMMIT, CODEC_FLUREE_TXN,
};
use fluree_db_ledger::{IndexConfig, LedgerState, LedgerView};
use fluree_db_nameservice::{NameService, Publisher};
use fluree_db_novelty::generate_commit_flakes;
use fluree_db_novelty::{Commit, CommitRef, SigningKey, TxnMetaEntry, TxnSignature};
use std::sync::Arc;
use tracing::Instrument;

/// Receipt returned after a successful commit
#[derive(Debug, Clone)]
pub struct CommitReceipt {
    /// Content identifier (CIDv1) — primary identity
    pub commit_id: ContentId,
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
    pub graph_delta: std::collections::HashMap<u16, String>,
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
    pub fn with_graph_delta(mut self, graph_delta: std::collections::HashMap<u16, String>) -> Self {
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
    view: LedgerView,
    mut ns_registry: NamespaceRegistry,
    storage: &S,
    nameservice: &N,
    index_config: &IndexConfig,
    opts: CommitOpts,
) -> Result<(CommitReceipt, LedgerState)>
where
    S: Storage + ContentAddressedWrite + Clone + 'static,
    N: NameService + Publisher,
{
    // 1. Extract flakes from view
    let (mut base, flakes) = view.into_parts();

    // Move commit options into locals so we can pass ownership where useful
    // (e.g., txn_meta) without forcing clones, while still using other fields later.
    let CommitOpts {
        message: _message,
        author: _author,
        raw_txn,
        signing_key,
        txn_signature,
        txn_meta,
        graph_delta,
    } = opts;

    let commit_span = tracing::debug_span!(
        "txn_commit",
        alias = base.ledger_id(),
        base_t = base.t(),
        flake_count = tracing::field::Empty,
        delta_bytes = tracing::field::Empty,
        current_novelty_bytes = tracing::field::Empty,
        max_novelty_bytes = index_config.reindex_max_bytes,
        has_raw_txn = raw_txn.is_some(),
    );
    async move {
        let commit_span = tracing::Span::current();

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
        commit_span.record("flake_count", flakes.len());
        commit_span.record("delta_bytes", delta_bytes);
        commit_span.record("current_novelty_bytes", current_bytes);
        if current_bytes + delta_bytes >= max_bytes {
            return Err(TransactError::NoveltyWouldExceed {
                current_bytes,
                delta_bytes,
                max_bytes,
            });
        }

        // 5. Verify sequencing
        let current = nameservice
            .lookup(base.ledger_id())
            .instrument(tracing::debug_span!("commit_nameservice_lookup"))
            .await?;
        {
            let span = tracing::debug_span!("commit_verify_sequencing");
            let _g = span.enter();
            verify_sequencing(&base, current.as_ref())?;
        }

        // 6. Build commit record
        let new_t = base.t() + 1;
        let flake_count = flakes.len();

        // Capture namespace delta once:
        // - write into commit record for persistence
        // - apply to returned in-memory LedgerSnapshot so subsequent operations (e.g., SPARQL/JSON-LD queries)
        //   can encode IRIs without requiring a reload.
        let ns_delta = {
            let span = tracing::debug_span!("commit_namespace_delta");
            let _g = span.enter();
            ns_registry.take_delta()
        };

        // Apply envelope deltas (namespace + graph) to the in-memory LedgerSnapshot.
        // This must happen before novelty apply so encode_iri() works for graph routing.
        base.snapshot.apply_envelope_deltas(&ns_delta, graph_delta.values().map(|s| s.as_str()));

        // Generate ISO 8601 timestamp
        // TODO: Refactor to accept an optional timestamp via CommitOpts instead of calling
        // Utc::now() directly. This would enable deterministic commit hashes for testing
        // (see fluree-db-api/tests/it_stable_hashes.rs) and allow callers to provide
        // externally-sourced timestamps when needed.
        let timestamp = Utc::now().to_rfc3339();

        // Store original transaction JSON if provided (Clojure parity)
        let txn_id: Option<ContentId> = if let Some(txn_json) = &raw_txn {
            let (txn_bytes, res) = async {
                let txn_bytes = serde_json::to_vec(txn_json)?;
                let res = storage
                    .content_write_bytes(ContentKind::Txn, base.ledger_id(), &txn_bytes)
                    .await?;
                Ok::<_, TransactError>((txn_bytes, res))
            }
            .instrument(tracing::debug_span!("commit_write_raw_txn"))
            .await?;
            tracing::info!(raw_txn_bytes = txn_bytes.len(), "raw txn stored");
            // Derive ContentId from the content hash in the write result
            ContentId::from_hex_digest(CODEC_FLUREE_TXN, &res.content_hash)
        } else {
            None
        };

        let mut commit_record = {
            let span = tracing::debug_span!("commit_build_record");
            let _g = span.enter();
            Commit::new(new_t, flakes)
                .with_namespace_delta(ns_delta)
                .with_time(timestamp)
        };

        // Add txn CID to commit record (must be before computing commit ID)
        if let Some(txn_cid) = txn_id {
            commit_record = commit_record.with_txn(txn_cid);
        }

        // Add txn signature if provided (audit metadata)
        if let Some(txn_sig) = txn_signature {
            commit_record = commit_record.with_txn_signature(txn_sig);
        }

        // Add user-provided transaction metadata
        if !txn_meta.is_empty() {
            commit_record = commit_record.with_txn_meta(txn_meta);
        }

        // Add named graph delta (g_id -> IRI mappings)
        if !graph_delta.is_empty() {
            commit_record.graph_delta = graph_delta;
        }

        // Build previous commit reference from the head commit's ContentId.
        if let Some(cid) = base.head_commit_id.clone() {
            commit_record = commit_record.with_previous_ref(CommitRef::new(cid));
        }

        // 7. Content-address + write (storage-owned)
        //
        // The on-disk commit blob is written *without* `id` set (to avoid
        // self-reference). We derive the ContentId from the blob's trailing
        // SHA-256 hash after writing.

        let (commit_cid, commit_hash_hex, bytes) = {
            let span = tracing::debug_span!("commit_write_commit_blob");
            let _g = span.enter();
            let signing = signing_key
                .as_ref()
                .map(|key| (key.as_ref(), base.ledger_id()));
            let result = crate::commit_v2::write_commit(&commit_record, true, signing)?;
            let commit_cid =
                ContentId::from_hex_digest(CODEC_FLUREE_COMMIT, &result.content_hash_hex)
                    .expect("valid SHA-256 hex from commit writer");
            (commit_cid, result.content_hash_hex, result.bytes)
        };

        storage
            .content_write_bytes_with_hash(
                ContentKind::Commit,
                base.ledger_id(),
                &commit_hash_hex,
                &bytes,
            )
            .await?;
        tracing::info!(commit_bytes = bytes.len(), "commit blob stored");

        // Update in-memory commit with its ContentId
        commit_record.id = Some(commit_cid.clone());

        // 8. Publish to nameservice
        nameservice
            .publish_commit(base.ledger_id(), new_t, &commit_cid)
            .instrument(tracing::debug_span!("commit_publish_nameservice"))
            .await?;

        // 9. Generate commit metadata flakes (Clojure parity)
        // Note: We merge these into novelty only, not into commit_record.flakes
        // (matching Clojure's behavior where metadata flakes are derived separately)
        let commit_metadata_flakes = {
            let span = tracing::debug_span!("commit_generate_metadata_flakes");
            let _g = span.enter();
            generate_commit_flakes(&commit_record, base.ledger_id(), new_t)
        };
        tracing::info!(
            metadata_flakes = commit_metadata_flakes.len(),
            "commit metadata flakes generated"
        );

        // 10. Build new state - merge commit_metadata_flakes with transaction flakes
        let mut all_flakes = std::mem::take(&mut commit_record.flakes);
        all_flakes.extend(commit_metadata_flakes);

        // 10.1 Populate DictNovelty with subjects/strings from this commit
        let mut dict_novelty = base.dict_novelty.clone();
        {
            let span = tracing::debug_span!("commit_populate_dict_novelty");
            let _g = span.enter();
            populate_dict_novelty(Arc::make_mut(&mut dict_novelty), &all_flakes);
        }

        let mut new_novelty = Arc::clone(&base.novelty);
        {
            let span = tracing::debug_span!("commit_apply_to_novelty");
            let _g = span.enter();
            Arc::make_mut(&mut new_novelty).apply_commit(all_flakes, new_t)?;
        }

        let new_state = LedgerState {
            snapshot: base.snapshot,
            novelty: new_novelty,
            dict_novelty,
            head_commit_id: Some(commit_cid.clone()),
            head_index_id: base.head_index_id,
            ns_record: base.ns_record,
            binary_store: base.binary_store,
            default_context: base.default_context,
            spatial_indexes: base.spatial_indexes,
        };

        let receipt = CommitReceipt {
            commit_id: commit_cid,
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
fn verify_sequencing(
    base: &LedgerState,
    current: Option<&fluree_db_nameservice::NsRecord>,
) -> Result<()> {
    match current {
        None => {
            // Genesis case: no record exists yet
            // Base should have no head_commit_id and t=0
            if base.head_commit_id.is_some() {
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

            // Verify previous commit identity matches via CID comparison.
            match (&base.head_commit_id, &record.commit_head_id) {
                // Both have CIDs: compare directly
                (Some(base_cid), Some(record_cid)) => {
                    if base_cid != record_cid {
                        return Err(TransactError::CommitIdMismatch {
                            expected: record_cid.to_string(),
                            found: base_cid.to_string(),
                        });
                    }
                }
                // Neither has CID: genesis edge case, both are at t=0 with no commits
                (None, None) => {}
                // Mixed state: one side has CID, the other doesn't
                (Some(base_cid), None) => {
                    return Err(TransactError::CommitIdMismatch {
                        expected: "None".to_string(),
                        found: base_cid.to_string(),
                    });
                }
                (None, Some(record_cid)) => {
                    return Err(TransactError::CommitIdMismatch {
                        expected: record_cid.to_string(),
                        found: "None".to_string(),
                    });
                }
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
    use fluree_db_core::{FlakeValue, LedgerSnapshot, MemoryStorage, Sid};
    use fluree_db_nameservice::memory::MemoryNameService;
    use fluree_db_novelty::Novelty;

    #[tokio::test]
    async fn test_commit_simple_insert() {
        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();

        // Stage an insert
        let txn = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
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
        // commit_id is now a ContentId with CODEC_FLUREE_COMMIT
        assert_eq!(receipt.commit_id.codec(), CODEC_FLUREE_COMMIT);
        assert_eq!(new_state.t(), 1);
        assert!(new_state.head_commit_id.is_some());
    }

    #[tokio::test]
    async fn test_commit_empty_transaction() {
        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();

        // Stage an empty transaction (no inserts)
        let txn = Txn::insert();
        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
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
        let db = LedgerSnapshot::genesis("test:main");
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

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
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

        let ns_registry2 = NamespaceRegistry::from_db(&state1.snapshot);
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
        let db = LedgerSnapshot::genesis("test:main");
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

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
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
