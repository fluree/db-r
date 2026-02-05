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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitData {
    /// Content-address IRI (e.g., "fluree:db:sha256:...")
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

impl Default for CommitData {
    fn default() -> Self {
        Self {
            id: None,
            address: None,
            flakes: 0,
            size: 0,
            previous: None,
        }
    }
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
pub async fn load_commit_envelope<S: Storage>(storage: &S, address: &str) -> Result<CommitEnvelope> {
    let data = storage
        .read_bytes(address)
        .await
        .map_err(|e| NoveltyError::storage(format!("Failed to read commit envelope {}: {}", address, e)))?;

    let envelope = {
        let _span = tracing::debug_span!("load_commit_envelope_v2", blob_bytes = data.len()).entered();
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
        let commit2 = Commit::new("commit-2", 2, vec![])
            .with_previous_ref(CommitRef::new("commit-1"));
        let commit3 = Commit::new("commit-3", 3, vec![])
            .with_previous_ref(CommitRef::new("commit-2"));

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
        };
        assert!(envelope.index_ref().is_none());
        assert!(envelope.index_address().is_none());
    }
}
