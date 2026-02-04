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

/// A commit represents a single transaction in the ledger
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Commit {
    /// Content address of this commit (storage address)
    pub address: String,

    /// Content-address IRI (e.g., "fluree:commit:sha256:...")
    /// Optional for backward compatibility with older commit files.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    /// Transaction time (monotonically increasing)
    pub t: i64,

    /// Commit version (default 2)
    #[serde(default = "default_version")]
    pub v: i32,

    /// ISO 8601 timestamp of when the commit was created
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,

    /// Flakes in this commit (assertions and retractions)
    #[serde(default)]
    pub flakes: Vec<Flake>,

    /// Previous commit address (None for genesis)
    /// Legacy field - use `previous_ref` for new commits
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous: Option<String>,

    /// Previous commit reference with id and address (new format)
    #[serde(rename = "previousRef", skip_serializing_if = "Option::is_none")]
    pub previous_ref: Option<CommitRef>,

    /// Embedded DB metadata (cumulative state)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<CommitData>,

    /// Index reference (if indexed at this commit)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<IndexRef>,

    /// Index address at time of commit (if reindexed at this point)
    /// Legacy field - use `index` for new commits
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed_at: Option<String>,

    /// Transaction address (storage address of the original transaction JSON)
    /// When present, the raw transaction JSON can be loaded from this address.
    /// Only set when the transaction included a txn payload (Clojure parity).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txn: Option<String>,

    /// New namespace codes introduced by this commit (code → prefix)
    ///
    /// When transactions introduce new IRIs with prefixes not yet in the
    /// database's namespace table, new codes are allocated and recorded here.
    /// This allows ledger loading to apply namespace updates from commit history.
    #[serde(default)]
    pub namespace_delta: HashMap<u16, String>,
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
            previous: None,
            previous_ref: None,
            data: None,
            index: None,
            indexed_at: None,
            txn: None,
            namespace_delta: HashMap::new(),
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

    /// Set the previous commit address (legacy format)
    pub fn with_previous(mut self, prev: impl Into<String>) -> Self {
        self.previous = Some(prev.into());
        self
    }

    /// Set the previous commit reference (new format with id and address)
    pub fn with_previous_ref(mut self, prev_ref: CommitRef) -> Self {
        self.previous_ref = Some(prev_ref);
        // Also set legacy field for backward compatibility
        self.previous = Some(self.previous_ref.as_ref().unwrap().address.clone());
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

    /// Get the effective previous commit address
    ///
    /// Returns the address from `previous_ref` if present, otherwise falls back to `previous`.
    pub fn previous_address(&self) -> Option<&str> {
        self.previous_ref
            .as_ref()
            .map(|r| r.address.as_str())
            .or(self.previous.as_deref())
    }

    /// Get the effective previous commit content-address IRI
    ///
    /// Returns the id from `previous_ref` if present.
    pub fn previous_id(&self) -> Option<&str> {
        self.previous_ref.as_ref().and_then(|r| r.id.as_deref())
    }

    /// Get the effective index address
    ///
    /// Returns the address from `index` if present, otherwise falls back to `indexed_at`.
    pub fn index_address(&self) -> Option<&str> {
        self.index
            .as_ref()
            .map(|r| r.address.as_str())
            .or(self.indexed_at.as_deref())
    }
}

/// Load a single commit from storage
pub async fn load_commit<S: Storage>(storage: &S, address: &str) -> Result<Commit> {
    let data = storage
        .read_bytes(address)
        .await
        .map_err(|e| NoveltyError::storage(format!("Failed to read commit {}: {}", address, e)))?;

    let mut commit = if data.len() >= 4 && data[0..4] == crate::commit_v2::format::MAGIC {
        let _span = tracing::debug_span!("load_commit_v2", blob_bytes = data.len()).entered();
        crate::commit_v2::read_commit(&data)
            .map_err(|e| NoveltyError::invalid_commit(e.to_string()))?
    } else {
        serde_json::from_slice(&data)
            .map_err(|e| NoveltyError::invalid_commit(e.to_string()))?
    };

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
/// # Memory vs CPU
///
/// This deserializes from the same JSON as `Commit` but omits the `flakes` field.
/// While this avoids allocating memory for flake data, serde still parses all
/// JSON tokens including the flakes array. For very large commits, consider
/// whether the CPU cost of parsing is acceptable for your use case.
///
/// # Deserialize-only
///
/// This struct is designed for reading existing commits, not creating new ones.
/// Use `Commit` when you need to serialize commit data.
#[derive(Clone, Debug, Deserialize)]
pub struct CommitEnvelope {
    /// Transaction time (monotonically increasing)
    pub t: i64,

    /// Commit version (default 2)
    #[serde(default = "default_version")]
    pub v: i32,

    /// Previous commit address (None for genesis)
    /// Legacy field - check `previous_ref` first
    #[serde(default)]
    pub previous: Option<String>,

    /// Previous commit reference with id and address (new format)
    #[serde(rename = "previousRef", default)]
    pub previous_ref: Option<CommitRef>,

    /// Index reference (if indexed at this commit)
    /// Includes version and t for validation without extra storage reads
    #[serde(default)]
    pub index: Option<IndexRef>,

    /// Index address at time of commit (if reindexed at this point)
    /// Legacy field - check `index` first
    #[serde(default)]
    pub indexed_at: Option<String>,

    /// New namespace codes introduced by this commit (code → prefix)
    #[serde(default)]
    pub namespace_delta: HashMap<u16, String>,
}

/// Sentinel value for unknown index version (legacy `indexed_at` field)
pub const INDEX_VERSION_UNKNOWN: i32 = 0;

impl CommitEnvelope {
    /// Get the effective previous commit address
    ///
    /// Returns the address from `previous_ref` if present, otherwise falls back to `previous`.
    pub fn previous_address(&self) -> Option<&str> {
        self.previous_ref
            .as_ref()
            .map(|r| r.address.as_str())
            .or(self.previous.as_deref())
    }

    /// Get the effective index reference
    ///
    /// Prefers `index` (new format with version info) over `indexed_at` (legacy address-only).
    /// If only `indexed_at` is present, creates an IndexRef with `v=INDEX_VERSION_UNKNOWN` (0)
    /// to indicate the version is not known from the commit metadata.
    ///
    /// Callers should check for `v == INDEX_VERSION_UNKNOWN` and load the db-root
    /// to determine the actual version when validation is needed.
    pub fn index_ref(&self) -> Option<IndexRef> {
        if let Some(ref idx) = self.index {
            Some(idx.clone())
        } else if let Some(ref addr) = self.indexed_at {
            // Legacy: create IndexRef from address-only field
            // Use sentinel value to indicate unknown version
            Some(IndexRef {
                id: None,
                address: addr.clone(),
                v: INDEX_VERSION_UNKNOWN,
                t: None,
            })
        } else {
            None
        }
    }

    /// Get the effective index address
    ///
    /// Returns the address from `index` if present, otherwise falls back to `indexed_at`.
    pub fn index_address(&self) -> Option<&str> {
        self.index
            .as_ref()
            .map(|r| r.address.as_str())
            .or(self.indexed_at.as_deref())
    }
}

/// Load a commit envelope (metadata only, no flakes) from storage
///
/// This is more memory-efficient than `load_commit` when you only need
/// metadata for scanning commit history.
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
        assert!(commit.previous.is_none());
    }

    #[tokio::test]
    async fn test_commit_chain() {
        let commit1 = Commit::new("commit-1", 1, vec![]);
        let commit2 = Commit::new("commit-2", 2, vec![]).with_previous("commit-1");
        let commit3 = Commit::new("commit-3", 3, vec![]).with_previous("commit-2");

        assert!(commit1.previous.is_none());
        assert_eq!(commit2.previous, Some("commit-1".to_string()));
        assert_eq!(commit3.previous, Some("commit-2".to_string()));
    }

    // =========================================================================
    // CommitEnvelope tests
    // =========================================================================

    #[tokio::test]
    async fn test_commit_envelope_deserializes_without_flakes() {
        // Commit JSON with flakes - envelope should ignore the flakes field
        let json = r#"{
            "address": "commit-1",
            "t": 5,
            "v": 2,
            "flakes": [[1, 2, 3, 4, 5, true, null], [6, 7, 8, 9, 10, false, null]],
            "previous": "commit-0",
            "namespace_delta": {"100": "ex:"}
        }"#;

        let envelope: CommitEnvelope = serde_json::from_str(json).unwrap();
        assert_eq!(envelope.t, 5);
        assert_eq!(envelope.v, 2);
        assert_eq!(envelope.previous_address(), Some("commit-0"));
        assert_eq!(envelope.namespace_delta.get(&100), Some(&"ex:".to_string()));
    }

    #[tokio::test]
    async fn test_commit_envelope_previous_ref_takes_precedence() {
        let json = r#"{
            "t": 3,
            "v": 2,
            "previous": "old-addr",
            "previousRef": {"address": "new-addr", "id": "fluree:commit:sha256:abc"}
        }"#;

        let envelope: CommitEnvelope = serde_json::from_str(json).unwrap();
        // previous_ref should take precedence over legacy previous field
        assert_eq!(envelope.previous_address(), Some("new-addr"));
    }

    #[tokio::test]
    async fn test_commit_envelope_index_ref() {
        // With new index format
        let json_new = r#"{
            "t": 10,
            "v": 2,
            "index": {"address": "index-addr", "v": 2, "t": 10}
        }"#;

        let envelope: CommitEnvelope = serde_json::from_str(json_new).unwrap();
        let idx = envelope.index_ref().unwrap();
        assert_eq!(idx.address, "index-addr");
        assert_eq!(idx.v, 2);
        assert_eq!(idx.t, Some(10));

        // With legacy indexed_at format
        let json_legacy = r#"{
            "t": 10,
            "v": 2,
            "indexed_at": "legacy-index-addr"
        }"#;

        let envelope: CommitEnvelope = serde_json::from_str(json_legacy).unwrap();
        let idx = envelope.index_ref().unwrap();
        assert_eq!(idx.address, "legacy-index-addr");
        // Legacy format uses INDEX_VERSION_UNKNOWN (0) to indicate unknown version
        assert_eq!(idx.v, INDEX_VERSION_UNKNOWN);
        assert_eq!(idx.t, None);
    }

    #[tokio::test]
    async fn test_commit_envelope_no_index() {
        let json = r#"{"t": 5, "v": 2}"#;
        let envelope: CommitEnvelope = serde_json::from_str(json).unwrap();
        assert!(envelope.index_ref().is_none());
        assert!(envelope.index_address().is_none());
    }

}
