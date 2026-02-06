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
//! would need to be ancestry-based (e.g., comparing commit addresses or
//! using Merkle ancestry proofs) rather than `t`-based.

use crate::commit_v2::format::CommitSignature;
use crate::{NoveltyError, Result};
use fluree_db_core::{Flake, Storage};
use futures::stream::{self, Stream};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Reference to a commit (for linking previous commits)
///
/// Used in commit chains to reference the previous commit with both
/// content-address IRI and storage address.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitRef {
    /// Content-address IRI (e.g., "fluree:commit:sha256:...")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    /// Storage address (e.g., "fluree:file://...")
    pub address: String,
}

impl CommitRef {
    /// Create a new commit reference
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            id: None,
            address: address.into(),
        }
    }

    /// Create a commit reference with content-address IRI
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }
}

/// Embedded DB metadata in a commit
///
/// Records the cumulative state of the database at this commit point.
/// This is NOT the flake count of the commit itself, but the total DB state.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct CommitData {
    /// Content-address IRI (legacy / optional).
    ///
    /// Historically this could reference a separate DB snapshot subject. The Rust
    /// pipeline now treats commit metadata as keyed by commit identifiers, so this
    /// field is typically `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    /// Storage address
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,

    /// Total flake count in the database (cumulative)
    #[serde(default)]
    pub flakes: u64,

    /// Total size in bytes (cumulative)
    #[serde(default)]
    pub size: u64,

    /// Previous DB reference (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous: Option<Box<CommitData>>,
}

/// Index reference embedded in a commit
///
/// When a commit is created at an index point, this records the index metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexRef {
    /// Content-address IRI (e.g., "fluree:index:sha256:...")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    /// Storage address
    pub address: String,

    /// Index version
    #[serde(default = "default_version")]
    pub v: i32,

    /// Transaction time the index covers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub t: Option<i64>,
}

fn default_version() -> i32 {
    2
}

impl IndexRef {
    /// Create a new index reference
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            id: None,
            address: address.into(),
            v: 2,
            t: None,
        }
    }

    /// Set the content-address IRI
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
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
    /// Content-addressed transaction ID (e.g., "fluree:tx:sha256:...")
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
/// The subject is implicit — always the commit itself (`fluree:commit:sha256:<hex>`).
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
    pub fn typed_literal(
        value: impl Into<String>,
        dt_ns: u16,
        dt_name: impl Into<String>,
    ) -> Self {
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
    /// Content address of this commit (storage address)
    pub address: String,

    /// Content-address IRI (e.g., "fluree:commit:sha256:...")
    pub id: Option<String>,

    /// Transaction time (monotonically increasing)
    pub t: i64,

    /// Commit version (default 2)
    pub v: i32,

    /// ISO 8601 timestamp of when the commit was created
    pub time: Option<String>,

    /// Flakes in this commit (assertions and retractions)
    pub flakes: Vec<Flake>,

    /// Previous commit reference with id and address
    pub previous_ref: Option<CommitRef>,

    /// Embedded DB metadata (cumulative state)
    pub data: Option<CommitData>,

    /// Index reference (if indexed at this commit)
    pub index: Option<IndexRef>,

    /// Transaction address (storage address of the original transaction JSON)
    /// When present, the raw transaction JSON can be loaded from this address.
    /// Only set when the transaction included a txn payload (Clojure parity).
    pub txn: Option<String>,

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
    /// - `1`: txn-meta graph (`https://ns.flur.ee/ledger#transactions`)
    /// - `2+`: user-defined named graphs
    pub graph_delta: HashMap<u32, String>,
}

impl Commit {
    /// Create a new commit
    pub fn new(address: impl Into<String>, t: i64, flakes: Vec<Flake>) -> Self {
        Self {
            address: address.into(),
            id: None,
            t,
            v: 2,
            time: None,
            flakes,
            previous_ref: None,
            data: None,
            index: None,
            txn: None,
            namespace_delta: HashMap::new(),
            txn_signature: None,
            commit_signatures: Vec::new(),
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
        }
    }

    /// Set the content-address IRI
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
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

    /// Set the embedded DB metadata
    pub fn with_data(mut self, data: CommitData) -> Self {
        self.data = Some(data);
        self
    }

    /// Set the transaction address (storage address of the original transaction JSON)
    pub fn with_txn(mut self, txn_addr: impl Into<String>) -> Self {
        self.txn = Some(txn_addr.into());
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

    /// Get the previous commit address (if any)
    pub fn previous_address(&self) -> Option<&str> {
        self.previous_ref.as_ref().map(|r| r.address.as_str())
    }

    /// Get the previous commit content-address IRI (if any)
    pub fn previous_id(&self) -> Option<&str> {
        self.previous_ref.as_ref().and_then(|r| r.id.as_deref())
    }

    /// Get the index address (if indexed at this commit)
    pub fn index_address(&self) -> Option<&str> {
        self.index.as_ref().map(|r| r.address.as_str())
    }
}

/// Load a single commit from storage
///
/// The commit blob must be in v2 binary format (magic header `FLv2`).
pub async fn load_commit<S: Storage>(storage: &S, address: &str) -> Result<Commit> {
    let data = storage
        .read_bytes(address)
        .await
        .map_err(|e| NoveltyError::storage(format!("Failed to read commit {}: {}", address, e)))?;

    let _span = tracing::debug_span!("load_commit_v2", blob_bytes = data.len()).entered();
    let mut commit = crate::commit_v2::read_commit(&data)
        .map_err(|e| NoveltyError::invalid_commit(e.to_string()))?;

    // Inject derived metadata that may be omitted from on-disk commit blobs.
    //
    // This matches Clojure behavior where `id`/`address` are injected at read time
    // from the storage address, rather than being self-referential in the blob.
    commit.address = address.to_string();
    if commit.id.is_none() {
        if let Some(hex) = commit_hash_hex_from_address(address) {
            commit.id = Some(format!("fluree:commit:sha256:{}", hex));
        }
    }

    Ok(commit)
}

fn commit_hash_hex_from_address(address: &str) -> Option<&str> {
    // Extract path portion after :// if present (supports `fluree:*://...` and raw `*://...`)
    let path = if let Some(rest) = address.strip_prefix("fluree:") {
        let pos = rest.find("://")?;
        &rest[pos + 3..]
    } else if let Some(pos) = address.find("://") {
        &address[pos + 3..]
    } else {
        return None;
    };

    let commit_part = path.rsplit('/').next()?;
    let hash = commit_part.strip_suffix(".json")?;
    if hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(hash)
    } else {
        None
    }
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

    /// Commit version (default 2)
    pub v: i32,

    /// Previous commit reference with id and address
    pub previous_ref: Option<CommitRef>,

    /// Index reference (if indexed at this commit)
    pub index: Option<IndexRef>,

    /// New namespace codes introduced by this commit (code → prefix)
    pub namespace_delta: HashMap<u16, String>,

    /// User-provided transaction metadata (replay-safe)
    pub txn_meta: Vec<TxnMetaEntry>,
}

impl CommitEnvelope {
    /// Get the previous commit address (if any)
    pub fn previous_address(&self) -> Option<&str> {
        self.previous_ref.as_ref().map(|r| r.address.as_str())
    }

    /// Get the index reference (if indexed at this commit)
    pub fn index_ref(&self) -> Option<&IndexRef> {
        self.index.as_ref()
    }

    /// Get the index address (if indexed at this commit)
    pub fn index_address(&self) -> Option<&str> {
        self.index.as_ref().map(|r| r.address.as_str())
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

            let next = envelope.previous_address().map(String::from);
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

            let next = commit.previous_address().map(String::from);
            Some((Ok(commit), next))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{Flake, FlakeValue, Sid};

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
        let commit = Commit::new("commit-1", 1, flakes);

        assert_eq!(commit.address, "commit-1");
        assert_eq!(commit.t, 1);
        assert_eq!(commit.flakes.len(), 1);
        assert!(commit.previous_ref.is_none());
    }

    #[tokio::test]
    async fn test_commit_chain() {
        let commit1 = Commit::new("commit-1", 1, vec![]);
        let commit2 =
            Commit::new("commit-2", 2, vec![]).with_previous_ref(CommitRef::new("commit-1"));
        let commit3 =
            Commit::new("commit-3", 3, vec![]).with_previous_ref(CommitRef::new("commit-2"));

        assert!(commit1.previous_address().is_none());
        assert_eq!(commit2.previous_address(), Some("commit-1"));
        assert_eq!(commit3.previous_address(), Some("commit-2"));
    }

    // =========================================================================
    // CommitEnvelope tests
    // =========================================================================

    #[test]
    fn test_commit_envelope_fields() {
        let envelope = CommitEnvelope {
            t: 5,
            v: 2,
            previous_ref: Some(CommitRef::new("commit-0")),
            index: None,
            namespace_delta: HashMap::from([(100, "ex:".to_string())]),
            txn_meta: Vec::new(),
        };

        assert_eq!(envelope.t, 5);
        assert_eq!(envelope.v, 2);
        assert_eq!(envelope.previous_address(), Some("commit-0"));
        assert_eq!(envelope.namespace_delta.get(&100), Some(&"ex:".to_string()));
    }

    #[test]
    fn test_commit_envelope_index_ref() {
        let envelope = CommitEnvelope {
            t: 10,
            v: 2,
            previous_ref: None,
            index: Some(IndexRef::new("index-addr").with_t(10)),
            namespace_delta: HashMap::new(),
            txn_meta: Vec::new(),
        };

        let idx = envelope.index_ref().unwrap();
        assert_eq!(idx.address, "index-addr");
        assert_eq!(idx.v, 2);
        assert_eq!(idx.t, Some(10));
    }

    #[test]
    fn test_commit_envelope_no_index() {
        let envelope = CommitEnvelope {
            t: 5,
            v: 2,
            previous_ref: None,
            index: None,
            namespace_delta: HashMap::new(),
            txn_meta: Vec::new(),
        };
        assert!(envelope.index_ref().is_none());
        assert!(envelope.index_address().is_none());
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
        assert_eq!(TxnMetaValue::string("hello"), TxnMetaValue::String("hello".to_string()));
        assert_eq!(TxnMetaValue::long(42), TxnMetaValue::Long(42));
        assert_eq!(TxnMetaValue::boolean(true), TxnMetaValue::Boolean(true));
        assert_eq!(
            TxnMetaValue::reference(50, "Alice"),
            TxnMetaValue::Ref { ns: 50, name: "Alice".to_string() }
        );
        assert_eq!(
            TxnMetaValue::lang_string("hello", "en"),
            TxnMetaValue::LangString { value: "hello".to_string(), lang: "en".to_string() }
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
