//! Commit type and streaming commit trace
//!
//! This module provides the Commit type representing a single transaction
//! and utilities for streaming commits from head backwards.
//!
//! # Linear History Assumption
//!
//! The commit tracing utilities assume a **linear commit history** where:
//! - Each commit has at most one previous commit (single-parent chain)
//! - Transaction times (`t`) are strictly monotonically increasing
//! - No branching, rebasing, or merge commits
//!
//! This matches Fluree's current commit model. The stop condition for
//! [`trace_commits`] uses `t <= stop_at_t`, which relies on `t` being
//! monotonic to correctly identify which commits are already indexed.
//!
//! For future support of git-like branching semantics, the stop condition
//! would need to be ancestry-based (e.g., comparing commit CIDs or
//! using Merkle ancestry proofs) rather than `t`-based.

use crate::commit_v2::format::CommitSignature;
use crate::{NoveltyError, Result};
use fluree_db_core::{ContentId, ContentStore, Flake, Storage, CODEC_FLUREE_COMMIT};
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

/// Index reference embedded in a commit
///
/// When a commit is created at an index point, this records the index CID
/// and the transaction time the index covers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexRef {
    /// Content identifier for the index root
    pub id: ContentId,

    /// Transaction time the index covers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub t: Option<i64>,
}

impl IndexRef {
    /// Create a new index reference
    pub fn new(id: ContentId) -> Self {
        Self { id, t: None }
    }

    /// Set the transaction time
    pub fn with_t(mut self, t: i64) -> Self {
        self.t = Some(t);
        self
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

    /// Previous commit reference (CID-based)
    pub previous_ref: Option<CommitRef>,

    /// Index reference (if indexed at this commit)
    pub index: Option<IndexRef>,

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
    pub graph_delta: HashMap<u32, String>,
}

impl Commit {
    /// Create a new commit (id is set to `None` until serialized and hashed)
    pub fn new(t: i64, flakes: Vec<Flake>) -> Self {
        Self {
            id: None,
            t,
            time: None,
            flakes,
            previous_ref: None,
            index: None,
            txn: None,
            namespace_delta: HashMap::new(),
            txn_signature: None,
            commit_signatures: Vec::new(),
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
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

    /// Set the previous commit reference
    pub fn with_previous_ref(mut self, prev_ref: CommitRef) -> Self {
        self.previous_ref = Some(prev_ref);
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

    /// Get the previous commit CID (if any)
    pub fn previous_id(&self) -> Option<&ContentId> {
        self.previous_ref.as_ref().map(|r| &r.id)
    }

    /// Get the index CID (if indexed at this commit)
    pub fn index_id(&self) -> Option<&ContentId> {
        self.index.as_ref().map(|r| &r.id)
    }
}

// =============================================================================
// V2 backward compatibility helpers
// =============================================================================

/// Extract the SHA-256 hex hash from a legacy storage address.
///
/// Supports formats like `fluree:file:///path/to/HASH.fcv2` (current) and
/// `fluree:s3://bucket/path/to/HASH.json` (legacy, backward compat).
fn hash_hex_from_address(address: &str) -> Option<&str> {
    // Extract path portion after :// if present
    let path = if let Some(rest) = address.strip_prefix("fluree:") {
        let pos = rest.find("://")?;
        &rest[pos + 3..]
    } else if let Some(pos) = address.find("://") {
        &address[pos + 3..]
    } else {
        return None;
    };

    let last_segment = path.rsplit('/').next()?;
    let hash = last_segment
        .strip_suffix(".fcv2")
        .or_else(|| last_segment.strip_suffix(".json"))?;
    if hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(hash)
    } else {
        None
    }
}

/// Derive a `ContentId` from a legacy v2 commit address or id string.
///
/// Tries the `id` string first (format: `fluree:commit:sha256:HEX`),
/// then falls back to extracting the hash from the `address`.
#[cfg(test)]
fn content_id_from_v2_commit_ref(
    id_str: Option<&str>,
    address: &str,
    codec: u64,
) -> Option<ContentId> {
    // Try the id string first: "fluree:commit:sha256:HEX"
    if let Some(id) = id_str {
        let hex = id
            .strip_prefix("fluree:commit:sha256:")
            .or_else(|| id.strip_prefix("fluree:commit:"))
            .or_else(|| id.strip_prefix("fluree:index:sha256:"))
            .or_else(|| id.strip_prefix("fluree:index:"));
        if let Some(hex) = hex {
            if let Some(cid) = ContentId::from_hex_digest(codec, hex) {
                return Some(cid);
            }
        }
    }

    // Fall back to extracting hash from address
    let hex = hash_hex_from_address(address)?;
    ContentId::from_hex_digest(codec, hex)
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

/// Load a single commit from legacy storage by address string.
///
/// This is a backward-compatible entry point for code that still uses
/// address-based storage. The commit's `id` is derived from the v2
/// blob hash.
pub async fn load_commit<S: Storage>(storage: &S, address: &str) -> Result<Commit> {
    let data = storage
        .read_bytes(address)
        .await
        .map_err(|e| NoveltyError::storage(format!("Failed to read commit {}: {}", address, e)))?;

    let _span = tracing::debug_span!("load_commit_v2", blob_bytes = data.len()).entered();
    let mut commit = crate::commit_v2::read_commit(&data)
        .map_err(|e| NoveltyError::invalid_commit(e.to_string()))?;

    // Derive id from address hash if not already set
    if commit.id.is_none() {
        if let Some(hex) = hash_hex_from_address(address) {
            commit.id = ContentId::from_hex_digest(CODEC_FLUREE_COMMIT, hex);
        }
    }

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
/// Decoded from the binary envelope section of a v2 commit blob.
#[derive(Clone, Debug)]
pub struct CommitEnvelope {
    /// Transaction time (monotonically increasing)
    pub t: i64,

    /// Previous commit reference (CID-based)
    pub previous_ref: Option<CommitRef>,

    /// Index reference (if indexed at this commit)
    pub index: Option<IndexRef>,

    /// New namespace codes introduced by this commit (code → prefix)
    pub namespace_delta: HashMap<u16, String>,

    /// User-provided transaction metadata (replay-safe)
    pub txn_meta: Vec<TxnMetaEntry>,
}

impl CommitEnvelope {
    /// Get the previous commit CID (if any)
    pub fn previous_id(&self) -> Option<&ContentId> {
        self.previous_ref.as_ref().map(|r| &r.id)
    }

    /// Get the index reference (if indexed at this commit)
    pub fn index_ref(&self) -> Option<&IndexRef> {
        self.index.as_ref()
    }

    /// Get the index CID (if indexed at this commit)
    pub fn index_id(&self) -> Option<&ContentId> {
        self.index.as_ref().map(|r| &r.id)
    }
}

/// Load a commit envelope (metadata only, no flakes) from storage
///
/// More memory-efficient than `load_commit` when you only need metadata
/// for scanning commit history. Only reads the header + envelope section
/// of the v2 binary blob.
pub async fn load_commit_envelope<S: Storage>(
    storage: &S,
    address: &str,
) -> Result<CommitEnvelope> {
    let data = storage.read_bytes(address).await.map_err(|e| {
        NoveltyError::storage(format!("Failed to read commit envelope {}: {}", address, e))
    })?;

    let envelope = {
        let _span =
            tracing::debug_span!("load_commit_envelope_v2", blob_bytes = data.len()).entered();
        crate::commit_v2::read_commit_envelope(&data)
            .map_err(|e| NoveltyError::invalid_commit(e.to_string()))?
    };

    Ok(envelope)
}

/// Stream commit envelopes from head backwards, stopping when t <= stop_at_t
///
/// Like `trace_commits` but only loads metadata (no flakes), making it more
/// memory-efficient for scanning large commit histories.
///
/// Returns `(address, envelope)` pairs so callers know the address of each commit.
///
/// # Arguments
///
/// * `storage` - Storage backend (must implement Clone for async stream)
/// * `head_address` - Address of the most recent commit to start from
/// * `stop_at_t` - Stop when `commit.t <= stop_at_t` (typically 0 for full scan)
pub fn trace_commit_envelopes<S: Storage + Clone + 'static>(
    storage: S,
    head_address: String,
    stop_at_t: i64,
) -> impl Stream<Item = Result<(String, CommitEnvelope)>> {
    stream::unfold(Some(head_address), move |addr| {
        let storage = storage.clone();
        async move {
            let addr = addr?;
            let envelope = match load_commit_envelope(&storage, &addr).await {
                Ok(e) => e,
                Err(e) => return Some((Err(e), None)),
            };

            // Stop if this commit's t is at or below the stop point
            if envelope.t <= stop_at_t {
                return None;
            }

            // Transition: storage is still address-based, but commit chain references are CID-based.
            // Derive the previous commit's legacy address by swapping the filename stem in-place.
            let next = envelope
                .previous_id()
                .and_then(|cid| replace_hash_in_address(&addr, &cid.digest_hex()));
            Some((Ok((addr, envelope)), next))
        }
    })
}

/// Stream commits from head backwards, stopping when t <= stop_at_t
///
/// This is used to load novelty incrementally - we trace commits backwards
/// from the head commit until we reach commits that are already in the index.
///
/// # Linear History Required
///
/// This function assumes linear commit history with monotonically increasing `t`.
/// See [module documentation](self) for details on this constraint.
///
/// # Arguments
///
/// * `storage` - Storage backend (must implement Clone for async stream)
/// * `head_address` - Address of the most recent commit to start from
/// * `stop_at_t` - Stop when `commit.t <= stop_at_t` (typically the index t)
pub fn trace_commits<S: Storage + Clone + 'static>(
    storage: S,
    head_address: String,
    stop_at_t: i64,
) -> impl Stream<Item = Result<Commit>> {
    stream::unfold(Some(head_address), move |addr| {
        let storage = storage.clone();
        async move {
            let addr = addr?;
            let commit = match load_commit(&storage, &addr).await {
                Ok(c) => c,
                Err(e) => return Some((Err(e), None)),
            };

            // Stop if this commit's t is at or below the index t
            if commit.t <= stop_at_t {
                return None;
            }

            // Transition: storage is still address-based, but commit chain references are CID-based.
            // Derive the previous commit's legacy address by swapping the filename stem in-place.
            let next = commit
                .previous_id()
                .and_then(|cid| replace_hash_in_address(&addr, &cid.digest_hex()));
            Some((Ok(commit), next))
        }
    })
}

/// Replace the filename stem (between the last `/` and last `.`) with `new_hash_hex`.
///
/// Used to derive the previous commit address from the current commit address
/// during the transition where storage is still legacy-address-based.
fn replace_hash_in_address(address: &str, new_hash_hex: &str) -> Option<String> {
    let slash = address.rfind('/')?;
    let dot = address.rfind('.')?;
    if dot <= slash + 1 {
        return None;
    }

    let mut out = String::with_capacity(address.len() - (dot - (slash + 1)) + new_hash_hex.len());
    out.push_str(&address[..slash + 1]);
    out.push_str(new_hash_hex);
    out.push_str(&address[dot..]);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{
        ContentKind, Flake, FlakeValue, Sid, CODEC_FLUREE_COMMIT, CODEC_FLUREE_INDEX_ROOT,
    };

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
        assert!(commit.previous_ref.is_none());
        assert!(commit.id.is_none());
    }

    #[tokio::test]
    async fn test_commit_chain() {
        let id1 = make_test_content_id(ContentKind::Commit, "commit-1");
        let id2 = make_test_content_id(ContentKind::Commit, "commit-2");

        let commit1 = Commit::new(1, vec![]);
        let commit2 = Commit::new(2, vec![]).with_previous_ref(CommitRef::new(id1.clone()));
        let commit3 = Commit::new(3, vec![]).with_previous_ref(CommitRef::new(id2.clone()));

        assert!(commit1.previous_id().is_none());
        assert_eq!(commit2.previous_id(), Some(&id1));
        assert_eq!(commit3.previous_id(), Some(&id2));
    }

    // =========================================================================
    // CommitEnvelope tests
    // =========================================================================

    #[test]
    fn test_commit_envelope_fields() {
        let prev_id = make_test_content_id(ContentKind::Commit, "commit-0");
        let envelope = CommitEnvelope {
            t: 5,
            previous_ref: Some(CommitRef::new(prev_id.clone())),
            index: None,
            namespace_delta: HashMap::from([(100, "ex:".to_string())]),
            txn_meta: Vec::new(),
        };

        assert_eq!(envelope.t, 5);
        assert_eq!(envelope.previous_id(), Some(&prev_id));
        assert_eq!(envelope.namespace_delta.get(&100), Some(&"ex:".to_string()));
    }

    #[test]
    fn test_commit_envelope_index_ref() {
        let idx_id = make_test_content_id(ContentKind::IndexRoot, "index-1");
        let envelope = CommitEnvelope {
            t: 10,
            previous_ref: None,
            index: Some(IndexRef::new(idx_id.clone()).with_t(10)),
            namespace_delta: HashMap::new(),
            txn_meta: Vec::new(),
        };

        let idx = envelope.index_ref().unwrap();
        assert_eq!(idx.id, idx_id);
        assert_eq!(idx.t, Some(10));
    }

    #[test]
    fn test_commit_envelope_no_index() {
        let envelope = CommitEnvelope {
            t: 5,
            previous_ref: None,
            index: None,
            namespace_delta: HashMap::new(),
            txn_meta: Vec::new(),
        };
        assert!(envelope.index_ref().is_none());
        assert!(envelope.index_id().is_none());
    }

    // =========================================================================
    // V2 backward compat helpers
    // =========================================================================

    #[test]
    fn test_hash_hex_from_address_fcv2() {
        let hash = "a".repeat(64);
        let addr = format!("fluree:file:///data/ledger/commit/{}.fcv2", hash);
        assert_eq!(hash_hex_from_address(&addr), Some(hash.as_str()));

        let s3_addr = format!("fluree:s3://bucket/path/commit/{}.fcv2", hash);
        assert_eq!(hash_hex_from_address(&s3_addr), Some(hash.as_str()));

        assert_eq!(hash_hex_from_address("not-an-address"), None);
        assert_eq!(hash_hex_from_address("fluree:file:///short.fcv2"), None);
    }

    #[test]
    fn test_hash_hex_from_address_json_backward_compat() {
        // Backward compat: .json commit addresses should still parse
        let hash = "a".repeat(64);
        let addr = format!("fluree:file:///data/ledger/commit/{}.json", hash);
        assert_eq!(hash_hex_from_address(&addr), Some(hash.as_str()));

        let s3_addr = format!("fluree:s3://bucket/path/commit/{}.json", hash);
        assert_eq!(hash_hex_from_address(&s3_addr), Some(hash.as_str()));

        assert_eq!(hash_hex_from_address("fluree:file:///short.json"), None);
    }

    #[test]
    fn test_content_id_from_v2_commit_ref() {
        let hash = "a".repeat(64);
        let id_str = format!("fluree:commit:sha256:{}", hash);
        let address = format!("fluree:file:///path/commit/{}.fcv2", hash);

        // From id string
        let cid =
            content_id_from_v2_commit_ref(Some(&id_str), &address, CODEC_FLUREE_COMMIT).unwrap();
        assert_eq!(cid.digest_hex(), hash);
        assert_eq!(cid.codec(), CODEC_FLUREE_COMMIT);

        // From address only (.fcv2)
        let cid = content_id_from_v2_commit_ref(None, &address, CODEC_FLUREE_COMMIT).unwrap();
        assert_eq!(cid.digest_hex(), hash);

        // From address only (.json backward compat)
        let json_address = format!("fluree:file:///path/commit/{}.json", hash);
        let cid = content_id_from_v2_commit_ref(None, &json_address, CODEC_FLUREE_COMMIT).unwrap();
        assert_eq!(cid.digest_hex(), hash);

        // Index ref
        let idx_id_str = format!("fluree:index:sha256:{}", hash);
        let cid =
            content_id_from_v2_commit_ref(Some(&idx_id_str), &address, CODEC_FLUREE_INDEX_ROOT)
                .unwrap();
        assert_eq!(cid.codec(), CODEC_FLUREE_INDEX_ROOT);
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
}
