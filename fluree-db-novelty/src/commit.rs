//! Commit type and DAG-aware commit traversal.
//!
//! This module provides the [`Commit`] type representing a single transaction,
//! [`CommitEnvelope`] for lightweight metadata-only access, and streaming
//! utilities for walking commit history backwards from a HEAD.
//!
//! Commits form a directed acyclic graph (DAG): normal commits have one
//! parent, merge commits have two or more. The traversal functions use
//! BFS with a visited set to handle the DAG correctly, yielding each
//! commit exactly once in reverse-topological order (highest `t` first).
//!
//! The `stop_at_t` parameter stops traversal when all remaining commits
//! have `t <= stop_at_t`, which is typically the index `t` — commits at
//! or below that point are already captured in the index.

use crate::commit_v2::format::CommitSignature;
use crate::{NoveltyError, Result};
use fluree_db_core::{ContentId, ContentStore, Flake};
use futures::stream::{self, Stream};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Reference to a commit (for linking previous commits)
///
/// In the CID-based architecture, the content identifier IS the identity.
/// Storage location is resolved by the `ContentStore` implementation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitRef {
    /// Content identifier (CIDv1). This IS the identity.
    pub id: ContentId,
}

impl CommitRef {
    /// Create a new commit reference from a ContentId
    pub fn new(id: ContentId) -> Self {
        Self { id }
    }
}

/// Transaction signature — audit record of who submitted a transaction.
///
/// The raw signed transaction (JWS/VC) is stored separately via content-addressed
/// storage. The `txn_id` provides a content-addressed reference to retrieve and
/// re-verify the original signed transaction.
#[derive(Clone, Debug)]
pub struct TxnSignature {
    /// Verified signer identity (did:key:z6Mk...)
    pub signer: String,
    /// Content-addressed transaction ID.
    /// References the original signed transaction stored in CAS.
    pub txn_id: Option<String>,
}

// =============================================================================
// Transaction Metadata Types
// =============================================================================

/// Maximum number of txn-meta entries per transaction.
pub const MAX_TXN_META_ENTRIES: usize = 256;

/// Maximum encoded size of txn-meta in bytes (64KB).
pub const MAX_TXN_META_BYTES: usize = 65536;

/// A predicate/object pair for user-provided transaction metadata.
///
/// Uses ns_code + name (like Sid) for compact encoding and resolver compatibility.
/// The subject is implicit — always the commit itself.
///
/// Stored in the commit envelope for replay-safe persistence, then emitted to
/// the txn-meta graph (`g_id=1`) during indexing.
#[derive(Clone, Debug, PartialEq)]
pub struct TxnMetaEntry {
    /// Predicate namespace code
    pub predicate_ns: u16,
    /// Predicate local name
    pub predicate_name: String,
    /// Object value
    pub value: TxnMetaValue,
}

impl TxnMetaEntry {
    /// Create a new txn-meta entry
    pub fn new(predicate_ns: u16, predicate_name: impl Into<String>, value: TxnMetaValue) -> Self {
        Self {
            predicate_ns,
            predicate_name: predicate_name.into(),
            value,
        }
    }
}

/// Object value for a transaction metadata entry.
///
/// Supports the same value types as normal RDF literals and references,
/// using ns_code + name for compact encoding.
#[derive(Clone, Debug, PartialEq)]
pub enum TxnMetaValue {
    /// Plain string literal (xsd:string)
    String(String),

    /// Typed literal with explicit datatype (ns_code + name)
    TypedLiteral {
        value: String,
        dt_ns: u16,
        dt_name: String,
    },

    /// Language-tagged string (rdf:langString)
    LangString { value: String, lang: String },

    /// IRI reference (ns_code + name)
    Ref { ns: u16, name: String },

    /// Integer value (xsd:long)
    Long(i64),

    /// Double value (xsd:double)
    ///
    /// Must be finite — NaN, +Inf, -Inf are rejected at parse time.
    Double(f64),

    /// Boolean value (xsd:boolean)
    Boolean(bool),
}

impl TxnMetaValue {
    /// Create a string value
    pub fn string(s: impl Into<String>) -> Self {
        Self::String(s.into())
    }

    /// Create an integer value
    pub fn long(n: i64) -> Self {
        Self::Long(n)
    }

    /// Create a boolean value
    pub fn boolean(b: bool) -> Self {
        Self::Boolean(b)
    }

    /// Create an IRI reference value
    pub fn reference(ns: u16, name: impl Into<String>) -> Self {
        Self::Ref {
            ns,
            name: name.into(),
        }
    }

    /// Create a language-tagged string
    pub fn lang_string(value: impl Into<String>, lang: impl Into<String>) -> Self {
        Self::LangString {
            value: value.into(),
            lang: lang.into(),
        }
    }

    /// Create a typed literal
    pub fn typed_literal(value: impl Into<String>, dt_ns: u16, dt_name: impl Into<String>) -> Self {
        Self::TypedLiteral {
            value: value.into(),
            dt_ns,
            dt_name: dt_name.into(),
        }
    }

    /// Create a double value, returning None if not finite
    pub fn double(n: f64) -> Option<Self> {
        if n.is_finite() {
            Some(Self::Double(n))
        } else {
            None
        }
    }
}

/// A commit represents a single transaction in the ledger
#[derive(Clone, Debug)]
pub struct Commit {
    /// Content identifier (CIDv1). `None` before the commit is serialized
    /// and hashed; `Some(cid)` after hashing or when loaded from storage.
    pub id: Option<ContentId>,

    /// Transaction time (monotonically increasing)
    pub t: i64,

    /// ISO 8601 timestamp of when the commit was created
    pub time: Option<String>,

    /// Flakes in this commit (assertions and retractions)
    pub flakes: Vec<Flake>,

    /// Parent commit references (CID-based).
    /// Empty for genesis, one element for normal commits, two+ for merge commits.
    pub previous_refs: Vec<CommitRef>,

    /// Transaction blob CID (content-addressed reference to original txn JSON).
    /// When present, the raw transaction JSON can be loaded from this CID.
    pub txn: Option<ContentId>,

    /// New namespace codes introduced by this commit (code → prefix)
    ///
    /// When transactions introduce new IRIs with prefixes not yet in the
    /// database's namespace table, new codes are allocated and recorded here.
    /// This allows ledger loading to apply namespace updates from commit history.
    pub namespace_delta: HashMap<u16, String>,

    /// Transaction signature (audit metadata: who submitted the transaction)
    pub txn_signature: Option<TxnSignature>,

    /// Commit signatures (cryptographic proof of which node(s) wrote this commit)
    pub commit_signatures: Vec<CommitSignature>,

    /// User-provided transaction metadata (replay-safe).
    ///
    /// Stored in the commit envelope and emitted to the txn-meta graph (`g_id=1`)
    /// during indexing. Each entry becomes a triple with the commit subject.
    pub txn_meta: Vec<TxnMetaEntry>,

    /// Named graph IRI to g_id mappings introduced by this commit.
    ///
    /// When a transaction references named graphs (via TriG GRAPH blocks or
    /// JSON-LD with graph IRIs), this map stores the g_id assignment for each
    /// graph IRI introduced in this commit. This is necessary for:
    ///
    /// 1. **Replay safety**: Commits must be self-contained so that replaying
    ///    the commit chain produces the same g_id assignments.
    /// 2. **Index independence**: g_id assignments in the commit do not depend
    ///    on the current index state, so re-indexing is deterministic.
    ///
    /// Reserved g_ids:
    /// - `0`: default graph
    /// - `1`: txn-meta graph (`#txn-meta`)
    /// - `2+`: user-defined named graphs
    pub graph_delta: HashMap<u16, String>,

    /// Ledger-fixed split mode for canonical IRI encoding.
    /// Set in the genesis commit; absent in subsequent commits.
    pub ns_split_mode: Option<fluree_db_core::ns_encoding::NsSplitMode>,
}

impl Commit {
    /// Create a new commit (id is set to `None` until serialized and hashed)
    pub fn new(t: i64, flakes: Vec<Flake>) -> Self {
        Self {
            id: None,
            t,
            time: None,
            flakes,
            previous_refs: Vec::new(),
            txn: None,
            namespace_delta: HashMap::new(),
            txn_signature: None,
            commit_signatures: Vec::new(),
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
            ns_split_mode: None,
        }
    }

    /// Set the content identifier
    pub fn with_id(mut self, id: ContentId) -> Self {
        self.id = Some(id);
        self
    }

    /// Set the commit timestamp (ISO 8601)
    pub fn with_time(mut self, time: impl Into<String>) -> Self {
        self.time = Some(time.into());
        self
    }

    /// Add a parent commit reference.
    ///
    /// For normal commits, call once. For merge commits, call multiple times
    /// or use [`with_merge_parents`](Self::with_merge_parents).
    pub fn with_previous_ref(mut self, prev_ref: CommitRef) -> Self {
        self.previous_refs.push(prev_ref);
        self
    }

    /// Set all parent commit references at once (for merge commits).
    pub fn with_merge_parents(mut self, refs: Vec<CommitRef>) -> Self {
        self.previous_refs = refs;
        self
    }

    /// Set the transaction CID
    pub fn with_txn(mut self, txn_id: ContentId) -> Self {
        self.txn = Some(txn_id);
        self
    }

    /// Set the namespace delta (new namespace codes introduced by this commit)
    pub fn with_namespace_delta(mut self, delta: HashMap<u16, String>) -> Self {
        self.namespace_delta = delta;
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

    /// Iterate over all parent commit CIDs.
    pub fn parent_ids(&self) -> impl Iterator<Item = &ContentId> {
        self.previous_refs.iter().map(|r| &r.id)
    }
}

// =============================================================================
// Loading
// =============================================================================

/// Load a single commit from a content store by CID.
pub async fn load_commit_by_id<C: ContentStore>(store: &C, id: &ContentId) -> Result<Commit> {
    let data = store
        .get(id)
        .await
        .map_err(|e| NoveltyError::storage(format!("Failed to read commit {}: {}", id, e)))?;

    let _span = tracing::debug_span!("load_commit", blob_bytes = data.len()).entered();
    let mut commit = crate::commit_v2::read_commit(&data)
        .map_err(|e| NoveltyError::invalid_commit(e.to_string()))?;

    // Set the commit's id from the CID we used to load it
    commit.id = Some(id.clone());

    Ok(commit)
}

// =============================================================================
// CommitEnvelope - Lightweight commit metadata without flakes
// =============================================================================

/// Lightweight commit metadata without flakes
///
/// Used for scanning commit history without loading all flake data into memory.
/// This enables memory-bounded batched reindex by allowing a metadata-only
/// backwards scan before forward flake processing.
///
/// Decoded from the binary envelope section of a v2/v3 commit blob.
#[derive(Clone, Debug)]
pub struct CommitEnvelope {
    /// Transaction time (monotonically increasing)
    pub t: i64,

    /// Parent commit references (CID-based).
    /// Empty for genesis, one element for normal commits, two+ for merge commits.
    pub previous_refs: Vec<CommitRef>,

    /// Transaction blob CID (content-addressed reference to original txn JSON)
    pub txn: Option<ContentId>,

    /// New namespace codes introduced by this commit (code → prefix)
    pub namespace_delta: HashMap<u16, String>,

    /// User-provided transaction metadata (replay-safe)
    pub txn_meta: Vec<TxnMetaEntry>,

    /// Ledger-fixed split mode for canonical IRI encoding.
    /// Set once in the genesis commit; absent in subsequent commits.
    pub ns_split_mode: Option<fluree_db_core::ns_encoding::NsSplitMode>,
}

impl CommitEnvelope {
    /// Iterate over all parent commit CIDs.
    pub fn parent_ids(&self) -> impl Iterator<Item = &ContentId> {
        self.previous_refs.iter().map(|r| &r.id)
    }
}

/// Load a commit envelope (metadata only, no flakes) from a content store by CID.
///
/// More memory-efficient than [`load_commit_by_id`] when you only need
/// metadata for scanning.
pub async fn load_commit_envelope_by_id<C: ContentStore + ?Sized>(
    store: &C,
    id: &ContentId,
) -> Result<CommitEnvelope> {
    let data = store.get(id).await.map_err(|e| {
        NoveltyError::storage(format!("Failed to read commit envelope {}: {}", id, e))
    })?;

    let envelope = {
        let _span =
            tracing::debug_span!("load_commit_envelope_by_id", blob_bytes = data.len()).entered();
        crate::commit_v2::read_commit_envelope(&data)
            .map_err(|e| NoveltyError::invalid_commit(e.to_string()))?
    };

    Ok(envelope)
}

/// Walk a commit DAG from a head CID, collecting `(t, ContentId)` pairs for
/// all commits with `t > stop_at_t`, sorted by `t` descending.
///
/// Each commit is visited exactly once. This is the building block for
/// [`trace_commit_envelopes_by_id`] and [`trace_commits_by_id`].
pub async fn collect_dag_cids<C: ContentStore + ?Sized>(
    store: &C,
    head_id: &ContentId,
    stop_at_t: i64,
) -> Result<Vec<(i64, ContentId)>> {
    let mut result = Vec::new();
    let mut frontier = vec![head_id.clone()];
    let mut visited = std::collections::HashSet::new();

    while let Some(cid) = frontier.pop() {
        if !visited.insert(cid.clone()) {
            continue;
        }
        let envelope = load_commit_envelope_by_id(store, &cid).await?;
        if envelope.t <= stop_at_t {
            continue;
        }
        for parent_id in envelope.parent_ids() {
            frontier.push(parent_id.clone());
        }
        result.push((envelope.t, cid));
    }

    // Sort by t descending (highest first = reverse-topological order).
    result.sort_by(|a, b| b.0.cmp(&a.0));
    Ok(result)
}

/// Stream commit envelopes from head backwards in reverse-topological order.
///
/// Walks the commit DAG, yielding `(ContentId, CommitEnvelope)` pairs ordered
/// by descending `t`. Each commit is yielded exactly once. Handles merge
/// commits with multiple parents.
pub fn trace_commit_envelopes_by_id<C: ContentStore + Clone + 'static>(
    store: C,
    head_id: ContentId,
    stop_at_t: i64,
) -> impl Stream<Item = Result<(ContentId, CommitEnvelope)>> {
    // Collect all CIDs first, then stream them.
    stream::unfold(
        None::<std::result::Result<std::vec::IntoIter<(i64, ContentId)>, ()>>,
        move |state| {
            let store = store.clone();
            let head_id = head_id.clone();
            async move {
                let mut iter = match state {
                    Some(Ok(iter)) => iter,
                    Some(Err(())) => return None,
                    None => {
                        // First call: walk the DAG and collect all CIDs.
                        match collect_dag_cids(&store, &head_id, stop_at_t).await {
                            Ok(cids) => cids.into_iter(),
                            Err(e) => return Some((Err(e), Some(Err(())))),
                        }
                    }
                };

                // Yield next envelope.
                let (_t, cid) = iter.next()?;
                match load_commit_envelope_by_id(&store, &cid).await {
                    Ok(env) => Some((Ok((cid, env)), Some(Ok(iter)))),
                    Err(e) => Some((Err(e), Some(Err(())))),
                }
            }
        },
    )
}

/// Stream commits from head backwards in reverse-topological order.
///
/// Walks the commit DAG, yielding full [`Commit`] values ordered by
/// descending `t`. Each commit is yielded exactly once. Handles merge
/// commits with multiple parents.
pub fn trace_commits_by_id<C: ContentStore + Clone + 'static>(
    store: C,
    head_id: ContentId,
    stop_at_t: i64,
) -> impl Stream<Item = Result<Commit>> {
    stream::unfold(
        None::<std::result::Result<std::vec::IntoIter<(i64, ContentId)>, ()>>,
        move |state| {
            let store = store.clone();
            let head_id = head_id.clone();
            async move {
                let mut iter = match state {
                    Some(Ok(iter)) => iter,
                    Some(Err(())) => return None,
                    None => match collect_dag_cids(&store, &head_id, stop_at_t).await {
                        Ok(cids) => cids.into_iter(),
                        Err(e) => return Some((Err(e), Some(Err(())))),
                    },
                };

                let (_t, cid) = iter.next()?;
                match load_commit_by_id(&store, &cid).await {
                    Ok(commit) => Some((Ok(commit), Some(Ok(iter)))),
                    Err(e) => Some((Err(e), Some(Err(())))),
                }
            }
        },
    )
}

// =============================================================================
// Common Ancestor
// =============================================================================

/// The most recent common ancestor between two commit chains.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommonAncestor {
    /// CID of the common commit.
    pub commit_id: ContentId,
    /// Transaction time of the common commit.
    pub t: i64,
}

/// Find the most recent common ancestor of two commit DAGs.
///
/// Uses dual-frontier BFS: expands both frontiers by following all parents,
/// and stops when a CID visited from one side appears in the other's visited
/// set. Handles merge commits with multiple parents correctly.
///
/// Returns an error if the DAGs share no common ancestor (should not happen
/// for branches within the same ledger, since they share a genesis commit).
pub async fn find_common_ancestor<C: ContentStore>(
    store: &C,
    head_a: &ContentId,
    head_b: &ContentId,
) -> Result<CommonAncestor> {
    if head_a == head_b {
        let envelope = load_commit_envelope_by_id(store, head_a).await?;
        return Ok(CommonAncestor {
            commit_id: head_a.clone(),
            t: envelope.t,
        });
    }

    let mut visited_a = std::collections::HashSet::new();
    let mut visited_b = std::collections::HashSet::new();
    // Frontier entries: (t, cid). We advance whichever frontier has the higher max-t.
    let mut frontier_a = Vec::new();
    let mut frontier_b = Vec::new();

    // Seed both frontiers.
    let env_a = load_commit_envelope_by_id(store, head_a).await?;
    visited_a.insert(head_a.clone());
    frontier_a.push((env_a.t, head_a.clone()));

    let env_b = load_commit_envelope_by_id(store, head_b).await?;
    visited_b.insert(head_b.clone());
    frontier_b.push((env_b.t, head_b.clone()));

    // Check initial overlap.
    if visited_a.contains(head_b) {
        return Ok(CommonAncestor {
            commit_id: head_b.clone(),
            t: env_b.t,
        });
    }
    if visited_b.contains(head_a) {
        return Ok(CommonAncestor {
            commit_id: head_a.clone(),
            t: env_a.t,
        });
    }

    loop {
        let max_a = frontier_a.iter().map(|(t, _)| *t).max();
        let max_b = frontier_b.iter().map(|(t, _)| *t).max();

        match (max_a, max_b) {
            (None, None) => {
                return Err(NoveltyError::invalid_commit(
                    "commit chains have no common ancestor".to_string(),
                ));
            }
            (Some(ta), Some(tb)) if ta >= tb => {
                // Advance frontier A: pop highest-t entry, expand its parents.
                if let Some(ancestor) =
                    advance_frontier(store, &mut frontier_a, &mut visited_a, &visited_b).await?
                {
                    return Ok(ancestor);
                }
            }
            _ => {
                // Advance frontier B.
                if let Some(ancestor) =
                    advance_frontier(store, &mut frontier_b, &mut visited_b, &visited_a).await?
                {
                    return Ok(ancestor);
                }
            }
        }
    }
}

/// Pop the highest-t entry from a frontier, load its parents, and check if
/// any newly-visited CID appears in the other side's visited set.
async fn advance_frontier<C: ContentStore>(
    store: &C,
    frontier: &mut Vec<(i64, ContentId)>,
    visited: &mut std::collections::HashSet<ContentId>,
    other_visited: &std::collections::HashSet<ContentId>,
) -> Result<Option<CommonAncestor>> {
    if frontier.is_empty() {
        return Ok(None);
    }
    // Find and remove the highest-t entry.
    let max_idx = frontier
        .iter()
        .enumerate()
        .max_by_key(|(_, (t, _))| *t)
        .map(|(i, _)| i)
        .unwrap();
    let (_t, cid) = frontier.swap_remove(max_idx);

    let envelope = load_commit_envelope_by_id(store, &cid).await?;
    for parent_id in envelope.parent_ids() {
        if visited.insert(parent_id.clone()) {
            let parent_env = load_commit_envelope_by_id(store, parent_id).await?;
            if other_visited.contains(parent_id) {
                return Ok(Some(CommonAncestor {
                    commit_id: parent_id.clone(),
                    t: parent_env.t,
                }));
            }
            frontier.push((parent_env.t, parent_id.clone()));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{ContentKind, Flake, FlakeValue, MemoryContentStore, Sid};

    fn make_test_content_id(kind: ContentKind, label: &str) -> ContentId {
        ContentId::new(kind, label.as_bytes())
    }

    fn make_test_flake(s: i64, p: i64, o: i64, t: i64) -> Flake {
        Flake::new(
            Sid::new(s as u16, format!("s{}", s)),
            Sid::new(p as u16, format!("p{}", p)),
            FlakeValue::Long(o),
            Sid::new(2, "long"),
            t,
            true,
            None,
        )
    }

    #[tokio::test]
    async fn test_commit_creation() {
        let flakes = vec![make_test_flake(1, 2, 42, 1)];
        let commit = Commit::new(1, flakes);

        assert_eq!(commit.t, 1);
        assert_eq!(commit.flakes.len(), 1);
        assert!(commit.previous_refs.is_empty());
        assert!(commit.id.is_none());
    }

    #[tokio::test]
    async fn test_commit_chain() {
        let id1 = make_test_content_id(ContentKind::Commit, "commit-1");
        let id2 = make_test_content_id(ContentKind::Commit, "commit-2");

        let commit1 = Commit::new(1, vec![]);
        let commit2 = Commit::new(2, vec![]).with_previous_ref(CommitRef::new(id1.clone()));
        let commit3 = Commit::new(3, vec![]).with_previous_ref(CommitRef::new(id2.clone()));

        assert_eq!(commit1.parent_ids().next(), None);
        assert_eq!(commit2.parent_ids().next(), Some(&id1));
        assert_eq!(commit3.parent_ids().next(), Some(&id2));
    }

    // =========================================================================
    // CommitEnvelope tests
    // =========================================================================

    #[test]
    fn test_commit_envelope_fields() {
        let prev_id = make_test_content_id(ContentKind::Commit, "commit-0");
        let envelope = CommitEnvelope {
            t: 5,
            previous_refs: vec![CommitRef::new(prev_id.clone())],
            txn: None,
            namespace_delta: HashMap::from([(100, "ex:".to_string())]),
            txn_meta: Vec::new(),
            ns_split_mode: None,
        };

        assert_eq!(envelope.t, 5);
        assert_eq!(envelope.parent_ids().next(), Some(&prev_id));
        assert_eq!(envelope.namespace_delta.get(&100), Some(&"ex:".to_string()));
    }

    // =========================================================================
    // TxnMetaEntry / TxnMetaValue tests
    // =========================================================================

    #[test]
    fn test_txn_meta_entry_creation() {
        let entry = TxnMetaEntry::new(100, "machine", TxnMetaValue::string("10.2.3.4"));
        assert_eq!(entry.predicate_ns, 100);
        assert_eq!(entry.predicate_name, "machine");
        assert_eq!(entry.value, TxnMetaValue::String("10.2.3.4".to_string()));
    }

    #[test]
    fn test_txn_meta_value_constructors() {
        assert_eq!(
            TxnMetaValue::string("hello"),
            TxnMetaValue::String("hello".to_string())
        );
        assert_eq!(TxnMetaValue::long(42), TxnMetaValue::Long(42));
        assert_eq!(TxnMetaValue::boolean(true), TxnMetaValue::Boolean(true));
        assert_eq!(
            TxnMetaValue::reference(50, "Alice"),
            TxnMetaValue::Ref {
                ns: 50,
                name: "Alice".to_string()
            }
        );
        assert_eq!(
            TxnMetaValue::lang_string("hello", "en"),
            TxnMetaValue::LangString {
                value: "hello".to_string(),
                lang: "en".to_string()
            }
        );
        assert_eq!(
            TxnMetaValue::typed_literal("2025-01-01", 2, "date"),
            TxnMetaValue::TypedLiteral {
                value: "2025-01-01".to_string(),
                dt_ns: 2,
                dt_name: "date".to_string()
            }
        );
    }

    #[test]
    fn test_txn_meta_value_double_finite() {
        assert_eq!(TxnMetaValue::double(2.72), Some(TxnMetaValue::Double(2.72)));
        assert_eq!(TxnMetaValue::double(-0.5), Some(TxnMetaValue::Double(-0.5)));
    }

    #[test]
    fn test_txn_meta_value_double_non_finite() {
        assert_eq!(TxnMetaValue::double(f64::NAN), None);
        assert_eq!(TxnMetaValue::double(f64::INFINITY), None);
        assert_eq!(TxnMetaValue::double(f64::NEG_INFINITY), None);
    }

    // =========================================================================
    // trace_commits_by_id tests
    // =========================================================================

    /// Helper: serialize a commit to v2 binary, store in a MemoryContentStore,
    /// and return the CID.
    async fn store_commit(store: &MemoryContentStore, commit: &Commit) -> ContentId {
        let result = crate::commit_v2::write_commit(commit, false, None).unwrap();
        store.put(ContentKind::Commit, &result.bytes).await.unwrap()
    }

    #[tokio::test]
    async fn test_trace_commits_by_id_single_commit() {
        use futures::StreamExt;

        let store = MemoryContentStore::new();
        let commit = Commit::new(1, vec![make_test_flake(1, 2, 42, 1)]);
        let cid = store_commit(&store, &commit).await;

        let commits: Vec<_> = trace_commits_by_id(store, cid, 0)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].t, 1);
    }

    #[tokio::test]
    async fn test_trace_commits_by_id_chain() {
        use futures::StreamExt;

        let store = MemoryContentStore::new();

        // Build a 3-commit chain: c1 <- c2 <- c3
        let c1 = Commit::new(1, vec![make_test_flake(1, 2, 10, 1)]);
        let c1_id = store_commit(&store, &c1).await;

        let c2 = Commit::new(2, vec![make_test_flake(2, 3, 20, 2)])
            .with_previous_ref(CommitRef::new(c1_id.clone()));
        let c2_id = store_commit(&store, &c2).await;

        let c3 = Commit::new(3, vec![make_test_flake(3, 4, 30, 3)])
            .with_previous_ref(CommitRef::new(c2_id.clone()));
        let c3_id = store_commit(&store, &c3).await;

        // Trace from head (c3), stop_at_t=0 → all 3 commits
        let commits: Vec<_> = trace_commits_by_id(store.clone(), c3_id.clone(), 0)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(commits.len(), 3);
        assert_eq!(commits[0].t, 3);
        assert_eq!(commits[1].t, 2);
        assert_eq!(commits[2].t, 1);
    }

    #[tokio::test]
    async fn test_trace_commits_by_id_stop_at_t() {
        use futures::StreamExt;

        let store = MemoryContentStore::new();

        let c1 = Commit::new(1, vec![]);
        let c1_id = store_commit(&store, &c1).await;

        let c2 = Commit::new(2, vec![]).with_previous_ref(CommitRef::new(c1_id.clone()));
        let c2_id = store_commit(&store, &c2).await;

        let c3 = Commit::new(3, vec![]).with_previous_ref(CommitRef::new(c2_id.clone()));
        let c3_id = store_commit(&store, &c3).await;

        // stop_at_t=1 → only c3 and c2 (c1 has t=1, excluded by t <= stop_at_t)
        let commits: Vec<_> = trace_commits_by_id(store, c3_id, 1)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].t, 3);
        assert_eq!(commits[1].t, 2);
    }
}
