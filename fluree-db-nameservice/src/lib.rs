//! Nameservice traits and implementations for Fluree DB
//!
//! This crate provides the core abstractions for ledger discovery, publishing,
//! and subscription. It defines four main traits:
//!
//! - [`NameService`]: Read-only lookup of ledger metadata
//! - [`Publisher`]: Publishing commit and index updates
//! - [`RefPublisher`]: Explicit compare-and-set ref operations for sync
//! - [`Publication`]: Optional subscription support for reactive updates
//!
//! # Extended Storage Traits
//!
//! For storage-backed nameservice implementations (e.g., S3), this crate also
//! provides extended storage traits:
//!
//! - [`StorageDelete`]: Delete stored objects
//! - [`StorageList`]: List objects by prefix
//! - [`StorageCas`]: Compare-and-swap operations using ETags
//!
//! # Implementations
//!
//! - [`MemoryNameService`]: In-memory implementation for testing
//! - [`FileNameService`]: File-based implementation using ns@v2 format
//! - [`StorageNameService`]: Storage-backed implementation using CAS operations

mod error;
#[cfg(feature = "native")]
pub mod file;
pub mod memory;
pub mod storage_ns;
pub mod storage_traits;
pub mod tracking;
#[cfg(feature = "native")]
pub mod tracking_file;

pub use error::{NameServiceError, Result};
pub use storage_ns::StorageNameService;
pub use storage_traits::{
    ListResult, StorageCas, StorageDelete, StorageExtError, StorageExtResult, StorageList,
};
pub use tracking::{MemoryTrackingStore, RemoteName, RemoteTrackingStore, TrackingRecord};
#[cfg(feature = "native")]
pub use tracking_file::FileTrackingStore;

use async_trait::async_trait;
use fluree_db_core::alias;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tokio::sync::broadcast;

/// Nameservice record containing ledger metadata
///
/// This struct preserves the distinction between the address (canonical ledger:branch)
/// and the ledger name (without branch suffix).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NsRecord {
    /// Canonical ledger address with branch (e.g., "mydb:main")
    ///
    /// This is the primary cache key and the fully-qualified identifier.
    /// Use this for cache lookups and as the canonical form.
    pub address: String,

    /// Ledger name without branch suffix (e.g., "mydb")
    ///
    /// This matches Clojure's distinction where `alias` is the base name
    /// and `address` includes the branch. For cache keys, use `address`.
    pub alias: String,

    /// Branch name (e.g., "main")
    pub branch: String,

    /// Latest commit address
    pub commit_address: Option<String>,

    /// Transaction time of latest commit
    pub commit_t: i64,

    /// Latest index address (may lag behind commit)
    pub index_address: Option<String>,

    /// Transaction time of latest index
    pub index_t: i64,

    /// Default context address for JSON-LD
    pub default_context_address: Option<String>,

    /// Whether this ledger has been retracted
    pub retracted: bool,
}

impl NsRecord {
    /// Create a new NsRecord with minimal required fields
    pub fn new(alias: impl Into<String>, branch: impl Into<String>) -> Self {
        let alias = alias.into();
        let branch = branch.into();
        let address = format!("{}:{}", alias, branch);

        Self {
            address,
            alias,
            branch,
            commit_address: None,
            commit_t: 0,
            index_address: None,
            index_t: 0,
            default_context_address: None,
            retracted: false,
        }
    }

    /// Check if this record has an index
    pub fn has_index(&self) -> bool {
        self.index_address.is_some()
    }

    /// Check if there are commits newer than the index
    pub fn has_novelty(&self) -> bool {
        self.commit_t > self.index_t
    }
}

/// Virtual graph type enumeration
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VgType {
    /// BM25 full-text search index
    Bm25,
    /// Vector similarity search index
    Vector,
    /// R2RML relational mapping (future)
    R2rml,
    /// Apache Iceberg table (future)
    Iceberg,
    /// Unknown/custom virtual graph type
    Unknown(String),
}

impl VgType {
    /// Convert VgType to the JSON-LD @type string
    pub fn to_type_string(&self) -> String {
        match self {
            VgType::Bm25 => "fidx:BM25".to_string(),
            VgType::Vector => "fidx:Vector".to_string(),
            VgType::R2rml => "fidx:R2RML".to_string(),
            VgType::Iceberg => "fidx:Iceberg".to_string(),
            VgType::Unknown(s) => s.clone(),
        }
    }

    /// Parse from a JSON-LD @type string
    pub fn from_type_string(s: &str) -> Self {
        match s {
            "fidx:BM25" => VgType::Bm25,
            "fidx:Vector" => VgType::Vector,
            "fidx:R2RML" => VgType::R2rml,
            "fidx:Iceberg" => VgType::Iceberg,
            _ if s.contains("BM25") || s.contains("bm25") => VgType::Bm25,
            _ if s.contains("Vector") || s.contains("vector") => VgType::Vector,
            _ if s.contains("R2RML") || s.contains("r2rml") => VgType::R2rml,
            _ if s.contains("Iceberg") || s.contains("iceberg") => VgType::Iceberg,
            _ => VgType::Unknown(s.to_string()),
        }
    }
}

/// Virtual graph nameservice record
///
/// This struct holds metadata for virtual graphs (BM25, R2RML, Iceberg, etc.)
/// stored in the nameservice. Virtual graphs are separate from ledgers but
/// follow a similar ns@v2 storage pattern.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VgNsRecord {
    /// The address used to look up this record (e.g., "my-search:main")
    pub address: String,

    /// Base name of the virtual graph (e.g., "my-search")
    pub name: String,

    /// Branch name (e.g., "main")
    pub branch: String,

    /// Virtual graph type (BM25, R2RML, Iceberg, etc.)
    pub vg_type: VgType,

    /// Configuration as JSON string (parsed by VG implementation)
    pub config: String,

    /// Dependent ledger aliases (e.g., ["source-ledger:main"])
    pub dependencies: Vec<String>,

    /// Index snapshot address (if any)
    pub index_address: Option<String>,

    /// Index watermark (transaction time of indexed data)
    pub index_t: i64,

    /// Whether this virtual graph has been retracted
    pub retracted: bool,
}

impl VgNsRecord {
    /// Create a new VgNsRecord with required fields
    pub fn new(
        name: impl Into<String>,
        branch: impl Into<String>,
        vg_type: VgType,
        config: impl Into<String>,
        dependencies: Vec<String>,
    ) -> Self {
        let name = name.into();
        let branch = branch.into();
        let address = format!("{}:{}", name, branch);

        Self {
            address,
            name,
            branch,
            vg_type,
            config: config.into(),
            dependencies,
            index_address: None,
            index_t: 0,
            retracted: false,
        }
    }

    /// Check if this is a BM25 virtual graph
    pub fn is_bm25(&self) -> bool {
        matches!(self.vg_type, VgType::Bm25)
    }

    /// Check if this is a Vector virtual graph
    pub fn is_vector(&self) -> bool {
        matches!(self.vg_type, VgType::Vector)
    }

    /// Check if this virtual graph has an index
    pub fn has_index(&self) -> bool {
        self.index_address.is_some()
    }

    /// Get the canonical alias (name:branch)
    pub fn alias(&self) -> String {
        format!("{}:{}", self.name, self.branch)
    }
}

/// A single snapshot entry in VG snapshot history
///
/// Represents a versioned BM25 (or other VG type) index snapshot at a specific `t`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VgSnapshotEntry {
    /// Transaction time (watermark) for this snapshot
    pub index_t: i64,

    /// Storage address of the snapshot
    pub index_address: String,
}

impl VgSnapshotEntry {
    /// Create a new snapshot entry
    pub fn new(index_t: i64, index_address: impl Into<String>) -> Self {
        Self {
            index_t,
            index_address: index_address.into(),
        }
    }
}

/// VG snapshot history for time-travel queries
///
/// Contains an ordered list of snapshot entries (monotonically increasing by `index_t`).
/// Used to select the appropriate snapshot for historical queries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct VgSnapshotHistory {
    /// VG alias (e.g., "my-search:main")
    pub alias: String,

    /// Ordered list of snapshots (sorted by index_t ascending)
    pub snapshots: Vec<VgSnapshotEntry>,
}

impl VgSnapshotHistory {
    /// Create a new empty snapshot history
    pub fn new(alias: impl Into<String>) -> Self {
        Self {
            alias: alias.into(),
            snapshots: Vec::new(),
        }
    }

    /// Select the best snapshot for a given `as_of_t`
    ///
    /// Returns the snapshot with `index_t = max { t | t <= as_of_t }`,
    /// or `None` if no suitable snapshot exists.
    pub fn select_snapshot(&self, as_of_t: i64) -> Option<&VgSnapshotEntry> {
        // Find the largest index_t <= as_of_t
        // Since snapshots are sorted ascending, we can iterate in reverse
        self.snapshots.iter().rev().find(|s| s.index_t <= as_of_t)
    }

    /// Get the most recent snapshot (head)
    pub fn head(&self) -> Option<&VgSnapshotEntry> {
        self.snapshots.last()
    }

    /// Check if a snapshot exists at exactly the given `t`
    pub fn has_snapshot_at(&self, t: i64) -> bool {
        self.snapshots.iter().any(|s| s.index_t == t)
    }

    /// Append a snapshot entry (must be monotonically increasing)
    ///
    /// Returns `true` if the snapshot was added, `false` if it was rejected
    /// (duplicate or lower `t` than existing).
    pub fn append(&mut self, entry: VgSnapshotEntry) -> bool {
        if let Some(last) = self.snapshots.last() {
            if entry.index_t <= last.index_t {
                return false; // Reject: not strictly increasing
            }
        }
        self.snapshots.push(entry);
        true
    }
}

/// Result of looking up a nameservice record
///
/// Can be either a ledger record or a virtual graph record.
#[derive(Clone, Debug)]
pub enum NsLookupResult {
    /// A ledger record
    Ledger(NsRecord),
    /// A virtual graph record
    VirtualGraph(VgNsRecord),
    /// Record not found
    NotFound,
}

/// Read-only nameservice lookup trait
///
/// Implementations provide ledger discovery by alias or address.
#[async_trait]
pub trait NameService: Debug + Send + Sync {
    /// Look up a ledger by address (may be alias or IRI)
    ///
    /// Returns `None` if the ledger is not found.
    async fn lookup(&self, ledger_address: &str) -> Result<Option<NsRecord>>;

    /// Get the alias for a ledger address (if known)
    ///
    /// This is the reverse lookup - given an address, find the alias.
    async fn alias(&self, ledger_address: &str) -> Result<Option<String>>;

    /// Get all known ledger records
    ///
    /// Used for building in-memory query indexes over the nameservice.
    async fn all_records(&self) -> Result<Vec<NsRecord>>;
}

/// Publisher trait for writing nameservice records
///
/// Implementations handle publishing commit and index updates with
/// monotonic guarantees.
#[async_trait]
pub trait Publisher: Debug + Send + Sync {
    /// Initialize a new ledger in the nameservice
    ///
    /// Creates a minimal NsRecord for a new ledger with no commits yet.
    /// Only succeeds if no record exists for this alias.
    ///
    /// # Arguments
    /// * `alias` - The normalized ledger alias (e.g., "mydb:main")
    ///
    /// # Errors
    /// Returns an error if a record already exists (including retracted records).
    async fn publish_ledger_init(&self, alias: &str) -> Result<()>;

    /// Publish a new commit
    ///
    /// Only updates if: `(not exists) OR (new_t > existing_t)`
    ///
    /// This is called by the transactor after each successful commit.
    async fn publish_commit(&self, alias: &str, commit_addr: &str, commit_t: i64) -> Result<()>;

    /// Publish a new index
    ///
    /// Only updates if: `(not exists) OR (new_t > existing_t)` - STRICTLY monotonic.
    ///
    /// This is called by the indexer after successfully writing new index roots.
    /// The index is published to a separate file/attribute to avoid contention
    /// with commit publishing.
    ///
    /// Note: "equal t prefers index file" is a READ-TIME merge rule, not a write rule.
    async fn publish_index(&self, alias: &str, index_addr: &str, index_t: i64) -> Result<()>;

    /// Retract a ledger
    ///
    /// Marks the ledger as retracted. Future lookups will return the record
    /// with `retracted: true`.
    async fn retract(&self, alias: &str) -> Result<()>;

    /// Get the publishing address for an alias
    ///
    /// Returns `None` for "private" publishing (don't write ns field to commit).
    /// Returns `Some(address)` for the value to write into commit's ns field.
    fn publishing_address(&self, alias: &str) -> Option<String>;
}

/// Admin-level publisher operations
///
/// Unlike `Publisher`, these methods allow non-monotonic updates
/// for admin operations like reindexing.
#[async_trait]
pub trait AdminPublisher: Publisher {
    /// Publish index, allowing overwrite when t == existing_t
    ///
    /// Unlike `publish_index()` which enforces strict monotonicity (new_t > existing_t),
    /// this method allows overwriting when t == existing_t. This is needed for admin
    /// operations like `reindex()` where we rebuild to the same t with a new root address.
    ///
    /// Note: This does NOT allow t < existing_t to preserve invariants for time-travel
    /// and snapshot history.
    async fn publish_index_allow_equal(
        &self,
        alias: &str,
        index_addr: &str,
        index_t: i64,
    ) -> Result<()>;
}

/// Virtual graph publisher trait
///
/// Implementations handle publishing virtual graph config and index updates.
/// Virtual graph records are stored separately from ledger records.
#[async_trait]
pub trait VirtualGraphPublisher: Debug + Send + Sync {
    /// Publish a virtual graph configuration record
    ///
    /// Creates or updates the VG config in nameservice. This stores the VG
    /// definition (type, config, dependencies) but NOT the index state.
    ///
    /// The config record is stored at `ns@v2/{vg-name}/{branch}.json`.
    async fn publish_vg(
        &self,
        name: &str,
        branch: &str,
        vg_type: VgType,
        config: &str,
        dependencies: &[String],
    ) -> Result<()>;

    /// Update virtual graph index address (head pointer)
    ///
    /// Only updates if: `new_index_t > existing_index_t` (strictly monotonic).
    ///
    /// The index record is stored at `ns@v2/{vg-name}/{branch}.index.json`,
    /// separate from the config record to avoid contention.
    ///
    /// Config updates must NOT reset index watermark.
    /// Index updates must NOT rewrite config.
    ///
    /// NOTE: This updates the "head" pointer only. For time-travel support,
    /// also call `publish_vg_snapshot` to record the snapshot in history.
    async fn publish_vg_index(
        &self,
        name: &str,
        branch: &str,
        index_addr: &str,
        index_t: i64,
    ) -> Result<()>;

    /// Publish a snapshot to the VG snapshot history
    ///
    /// Appends a snapshot entry to the history file. Only succeeds if
    /// `index_t > max(existing snapshot t)` (strictly monotonic).
    ///
    /// The snapshot history is stored at `ns@v2/{vg-name}/{branch}.snapshots.json`.
    ///
    /// This is separate from `publish_vg_index` to allow:
    /// 1. Building historical snapshots without moving head
    /// 2. Recording all snapshots for time-travel queries
    async fn publish_vg_snapshot(
        &self,
        name: &str,
        branch: &str,
        index_addr: &str,
        index_t: i64,
    ) -> Result<()>;

    /// Look up the snapshot history for a virtual graph
    ///
    /// Returns the full snapshot history for time-travel snapshot selection.
    /// Returns an empty history if no snapshots have been published.
    async fn lookup_vg_snapshots(&self, alias: &str) -> Result<VgSnapshotHistory>;

    /// Retract a virtual graph
    ///
    /// Marks the VG as retracted. Future lookups will return the record
    /// with `retracted: true`.
    async fn retract_vg(&self, name: &str, branch: &str) -> Result<()>;

    /// Look up a virtual graph by alias
    ///
    /// Returns `None` if not found or if the record is a ledger (not a VG).
    async fn lookup_vg(&self, alias: &str) -> Result<Option<VgNsRecord>>;

    /// Look up any record (ledger or VG) and return unified result
    ///
    /// This is useful when you don't know if an alias refers to a ledger or VG.
    async fn lookup_any(&self, alias: &str) -> Result<NsLookupResult>;

    /// Get all known virtual graph records
    ///
    /// Used for building in-memory query indexes over the nameservice.
    /// Returns all VG records including retracted ones (callers can filter by status).
    async fn all_vg_records(&self) -> Result<Vec<VgNsRecord>>;
}

/// Subscription scope for filtering nameservice events.
///
/// Determines which events a subscriber will receive:
/// - `Alias(String)` - Only events matching this specific alias
/// - `All` - All events from any ledger or virtual graph
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscriptionScope {
    /// Subscribe to events for a specific alias (ledger or VG)
    Alias(String),
    /// Subscribe to all events (all ledgers and VGs)
    All,
}

impl SubscriptionScope {
    /// Create a scope for a specific alias
    pub fn alias(alias: impl Into<String>) -> Self {
        Self::Alias(alias.into())
    }

    /// Create a scope for all events
    pub fn all() -> Self {
        Self::All
    }

    /// Check if this scope matches a given event alias
    pub fn matches(&self, event_alias: &str) -> bool {
        match self {
            Self::All => true,
            Self::Alias(a) => a == event_alias,
        }
    }
}

/// Nameservice event emitted when records change.
///
/// These events are **in-process only** (they are not persisted, and they do not
/// automatically propagate across multiple processes/machines even if the
/// nameservice backend is file/storage based).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NameServiceEvent {
    /// A ledger commit head was advanced.
    LedgerCommitPublished {
        alias: String,
        commit_address: String,
        commit_t: i64,
    },
    /// A ledger index head was advanced.
    LedgerIndexPublished {
        alias: String,
        index_address: String,
        index_t: i64,
    },
    /// A ledger was retracted.
    LedgerRetracted { alias: String },
    /// A virtual graph config was published/updated.
    VgConfigPublished {
        alias: String,
        vg_type: VgType,
        dependencies: Vec<String>,
    },
    /// A virtual graph head index pointer was advanced.
    VgIndexPublished {
        alias: String,
        index_address: String,
        index_t: i64,
    },
    /// A virtual graph snapshot was appended to history.
    VgSnapshotPublished {
        alias: String,
        index_address: String,
        index_t: i64,
    },
    /// A virtual graph was retracted.
    VgRetracted { alias: String },
}

/// Subscription handle for receiving ledger updates
#[derive(Debug)]
pub struct Subscription {
    /// The subscription scope (alias or all)
    pub scope: SubscriptionScope,
    /// Receiver for nameservice events (in-process).
    pub receiver: broadcast::Receiver<NameServiceEvent>,
}

/// Optional publication trait for reactive updates
///
/// This trait is only implemented where the backend supports pubsub.
/// Not all nameservice implementations support subscriptions.
#[async_trait]
pub trait Publication: Debug + Send + Sync {
    /// Subscribe to nameservice events with a given scope
    ///
    /// Returns a subscription handle that can be used to receive events.
    /// The receiver will receive all events; filtering by scope is the
    /// caller's responsibility (allows uniform broadcast channel usage).
    async fn subscribe(&self, scope: SubscriptionScope) -> Result<Subscription>;

    /// Unsubscribe from updates (no-op for stateless implementations)
    async fn unsubscribe(&self, scope: &SubscriptionScope) -> Result<()>;

    /// Get all known addresses for an alias (commit history)
    async fn known_addresses(&self, alias: &str) -> Result<Vec<String>>;
}

// ---------------------------------------------------------------------------
// Ref-level CAS (compare-and-set) types and trait
// ---------------------------------------------------------------------------

/// Which ref is being read or updated.
///
/// `Copy` — small enum, pass by value at call sites.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RefKind {
    /// The commit head pointer (`f:commit` + `f:t` in ns@v2).
    CommitHead,
    /// The index head pointer (`f:index` + `f:indexT` in ns@v2).
    IndexHead,
}

/// A ref value: an optional address + transaction-time watermark.
///
/// Semantics when returned from [`RefPublisher::get_ref`]:
/// - `Some(RefValue { address: None, t: 0 })` — ref exists but is "unborn"
///   (ledger initialised, no commit yet — analogous to git's unborn HEAD).
/// - `Some(RefValue { address: Some(..), t })` — ref exists with a value.
/// - `None` (at the `Option` level) — alias/ref is completely unknown.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefValue {
    /// Address of the commit or index root. `None` means "unborn".
    pub address: Option<String>,
    /// Monotonic watermark (transaction time).
    pub t: i64,
}

/// Outcome of a compare-and-set operation.
///
/// Conflicts are **not errors** — they are expected outcomes of concurrent
/// writes and must be handled by the caller (retry, report, etc.).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CasResult {
    /// CAS succeeded — the ref was updated to the new value.
    Updated,
    /// CAS failed — `expected` did not match the current value.
    /// `actual` carries the current ref (if any) so the caller can decide
    /// what to do next (retry, diverge, etc.).
    Conflict { actual: Option<RefValue> },
}

/// Explicit ref-level CAS operations for sync.
///
/// CAS compares on **address** (the ref identity — like git's "expected old
/// OID"). `t` serves as a **kind-dependent monotonic guard**:
///
/// | Kind         | Guard             | Rationale                          |
/// |--------------|-------------------|------------------------------------|
/// | `CommitHead` | `new.t > cur.t`   | No two commits share a `t`.        |
/// | `IndexHead`  | `new.t >= cur.t`  | Re-index at same `t` is allowed.   |
///
/// "Fast-forward" in Fluree is defined by `t`-ordering, **not** commit
/// ancestry.  If ancestry-based FF is ever needed, commit parent links and a
/// graph walk would be required — that is out of scope here.
#[async_trait]
pub trait RefPublisher: Debug + Send + Sync {
    /// Read the current ref value for an alias + kind.
    ///
    /// Returns:
    /// - `Some(RefValue { address: None, .. })` — ref exists, unborn
    /// - `Some(RefValue { address: Some(..), .. })` — ref exists with value
    /// - `None` — alias/ref completely unknown
    async fn get_ref(&self, alias: &str, kind: RefKind) -> Result<Option<RefValue>>;

    /// Atomic compare-and-set.
    ///
    /// Updates the ref **only if** the current address matches `expected`.
    /// Pass `expected = None` for initial creation (ref must not exist).
    ///
    /// The kind-dependent monotonic guard is also checked:
    /// - `CommitHead`: `new.t > current.t`
    /// - `IndexHead`: `new.t >= current.t`
    ///
    /// Returns [`CasResult::Conflict`] (with the actual value) on mismatch.
    async fn compare_and_set_ref(
        &self,
        alias: &str,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> Result<CasResult>;

    /// Fast-forward the commit head with a retry loop.
    ///
    /// Succeeds only when `new.t > current.t` (strict monotonicity).
    /// On CAS conflict from a concurrent writer the method re-reads the
    /// current ref and retries if the update is still a fast-forward.
    /// Returns [`CasResult::Conflict`] once it determines the ref has
    /// diverged (`current.t >= new.t` after re-read).
    async fn fast_forward_commit(
        &self,
        alias: &str,
        new: &RefValue,
        max_retries: usize,
    ) -> Result<CasResult> {
        for _ in 0..max_retries {
            let current = self.get_ref(alias, RefKind::CommitHead).await?;

            // Check whether fast-forward is still possible.
            if let Some(ref cur) = current {
                if new.t <= cur.t {
                    return Ok(CasResult::Conflict { actual: current });
                }
            }

            match self
                .compare_and_set_ref(alias, RefKind::CommitHead, current.as_ref(), new)
                .await?
            {
                CasResult::Updated => return Ok(CasResult::Updated),
                CasResult::Conflict { actual } => {
                    // Another writer advanced the ref — still FF-able?
                    if let Some(ref a) = actual {
                        if new.t <= a.t {
                            return Ok(CasResult::Conflict { actual });
                        }
                    }
                    // Retry — next iteration re-reads current.
                    continue;
                }
            }
        }
        // Exhausted retries — return latest known state.
        let current = self.get_ref(alias, RefKind::CommitHead).await?;
        Ok(CasResult::Conflict { actual: current })
    }
}

/// Parse a ledger alias into (ledger_name, branch) components
///
/// Alias format: `ledger-name:branch` (e.g., "mydb:main")
/// If no branch is specified, defaults to the core default branch.
pub fn parse_alias(alias: &str) -> Result<(String, String)> {
    alias::split_alias(alias)
        .map_err(|e| NameServiceError::invalid_alias(format!("{}", e)))
}

// ---------------------------------------------------------------------------
// V2 Concern Types (Status and Config extensions)
// ---------------------------------------------------------------------------

/// Which concern is being read or updated (v2 extension).
///
/// Extends the concept of `RefKind` to include Status and Config concerns.
/// Head and Index concerns map directly to `RefKind::CommitHead` and
/// `RefKind::IndexHead` respectively.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ConcernKind {
    /// Commit head pointer - equivalent to RefKind::CommitHead
    Head,
    /// Index state - equivalent to RefKind::IndexHead
    Index,
    /// Status state (queue depth, locks, progress, etc.)
    Status,
    /// Config state (default context, settings)
    Config,
}

impl ConcernKind {
    /// Convert to RefKind if applicable (Head/Index only).
    ///
    /// Returns `None` for Status and Config since they don't map to RefKind.
    pub fn as_ref_kind(&self) -> Option<RefKind> {
        match self {
            ConcernKind::Head => Some(RefKind::CommitHead),
            ConcernKind::Index => Some(RefKind::IndexHead),
            ConcernKind::Status | ConcernKind::Config => None,
        }
    }
}

/// Status payload with extensible metadata.
///
/// The `state` field contains the primary status (e.g., "ready", "indexing", "error").
/// Additional metadata can be stored in `extra` using `#[serde(flatten)]`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusPayload {
    /// Primary state value (e.g., "ready", "init", "indexing", "error", "retracted")
    pub state: String,

    /// Extensible metadata (queue_depth, locks, progress, error messages, etc.)
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

impl StatusPayload {
    /// Create a new status payload with just a state
    pub fn new(state: impl Into<String>) -> Self {
        Self {
            state: state.into(),
            extra: std::collections::HashMap::new(),
        }
    }

    /// Create a status payload with state and extra metadata
    pub fn with_extra(
        state: impl Into<String>,
        extra: std::collections::HashMap<String, serde_json::Value>,
    ) -> Self {
        Self {
            state: state.into(),
            extra,
        }
    }

    /// Check if the status indicates "ready" state
    pub fn is_ready(&self) -> bool {
        self.state == "ready"
    }

    /// Check if the status indicates "retracted" state
    pub fn is_retracted(&self) -> bool {
        self.state == "retracted"
    }
}

impl Default for StatusPayload {
    fn default() -> Self {
        Self::new("ready")
    }
}

/// Config payload with known fields + extensibility.
///
/// Contains common config fields like `default_context`, with additional
/// settings stored in `extra`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConfigPayload {
    /// Default JSON-LD context address
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_context: Option<String>,

    /// Additional config (index_threshold, replication settings, etc.)
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

impl ConfigPayload {
    /// Create a new empty config payload
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a config payload with a default context
    pub fn with_default_context(context: impl Into<String>) -> Self {
        Self {
            default_context: Some(context.into()),
            extra: std::collections::HashMap::new(),
        }
    }
}

/// Status concern value (watermark + payload).
///
/// The watermark `v` is a monotonically increasing counter that changes
/// on every status update. Status always has a payload (never unborn).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatusValue {
    /// Watermark (monotonically increasing version counter)
    pub v: i64,
    /// Status payload (always present)
    pub payload: StatusPayload,
}

impl StatusValue {
    /// Create a new status value
    pub fn new(v: i64, payload: StatusPayload) -> Self {
        Self { v, payload }
    }

    /// Create initial status value (v=1, state="ready")
    pub fn initial() -> Self {
        Self {
            v: 1,
            payload: StatusPayload::default(),
        }
    }
}

/// Config concern value (watermark + optional payload).
///
/// The watermark `v` is a monotonically increasing counter. Config can be
/// "unborn" (v=0, payload=None) if no config has been set yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigValue {
    /// Watermark (monotonically increasing version counter)
    pub v: i64,
    /// Config payload (None if unborn)
    pub payload: Option<ConfigPayload>,
}

impl ConfigValue {
    /// Create a new config value
    pub fn new(v: i64, payload: Option<ConfigPayload>) -> Self {
        Self { v, payload }
    }

    /// Create an unborn config value (v=0, no payload)
    pub fn unborn() -> Self {
        Self {
            v: 0,
            payload: None,
        }
    }

    /// Check if this config is unborn (no config set yet)
    pub fn is_unborn(&self) -> bool {
        self.v == 0 && self.payload.is_none()
    }
}

/// Result of a compare-and-set operation for status.
///
/// Conflicts are NOT errors — they indicate the expected value didn't match
/// the current value. The caller should handle conflicts by examining `actual`
/// and deciding whether to retry or report divergence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StatusCasResult {
    /// CAS succeeded — status was updated to the new value
    Updated,
    /// CAS failed — expected didn't match current.
    /// `actual` contains the current status value (if record exists).
    Conflict { actual: Option<StatusValue> },
}

/// Result of a compare-and-set operation for config.
///
/// Conflicts are NOT errors — they indicate the expected value didn't match
/// the current value. The caller should handle conflicts by examining `actual`
/// and deciding whether to retry or report divergence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigCasResult {
    /// CAS succeeded — config was updated to the new value
    Updated,
    /// CAS failed — expected didn't match current.
    /// `actual` contains the current config value (if record exists).
    Conflict { actual: Option<ConfigValue> },
}

// ---------------------------------------------------------------------------
// V2 Publisher Traits (Status and Config)
// ---------------------------------------------------------------------------

/// Publisher for status concern (v2 extension).
///
/// Status tracks operational metadata like queue depth, locks, progress,
/// and error states. It uses a monotonically increasing watermark and
/// CAS semantics for coordination.
///
/// Status always exists once a record is created (initial state is "ready" with v=1).
#[async_trait]
pub trait StatusPublisher: Debug + Send + Sync {
    /// Get current status for an alias.
    ///
    /// Returns:
    /// - `Some(StatusValue)` — record exists with status
    /// - `None` — record doesn't exist at all
    async fn get_status(&self, alias: &str) -> Result<Option<StatusValue>>;

    /// Push status with CAS semantics.
    ///
    /// Updates only if current matches expected. Returns conflict with actual on mismatch.
    ///
    /// # Arguments
    /// * `alias` - The ledger/entity alias
    /// * `expected` - The expected current status (`None` for initial creation)
    /// * `new` - The new status to set (must have `new.v > expected.v`)
    ///
    /// # Returns
    /// - `Updated` — successfully updated
    /// - `Conflict { actual }` — current didn't match expected
    async fn push_status(
        &self,
        alias: &str,
        expected: Option<&StatusValue>,
        new: &StatusValue,
    ) -> Result<StatusCasResult>;
}

/// Publisher for config concern (v2 extension).
///
/// Config tracks settings like default context, index thresholds, and other
/// configuration options. It uses a monotonically increasing watermark and
/// CAS semantics.
///
/// Config can be "unborn" (v=0, payload=None) if no config has been set yet.
#[async_trait]
pub trait ConfigPublisher: Debug + Send + Sync {
    /// Get current config for an alias.
    ///
    /// Returns:
    /// - `Some(ConfigValue)` — record exists (may be unborn with v=0)
    /// - `None` — record doesn't exist at all
    async fn get_config(&self, alias: &str) -> Result<Option<ConfigValue>>;

    /// Push config with CAS semantics.
    ///
    /// Updates only if current matches expected. Returns conflict with actual on mismatch.
    ///
    /// # Arguments
    /// * `alias` - The ledger/entity alias
    /// * `expected` - The expected current config (`None` for initial creation)
    /// * `new` - The new config to set (must have `new.v > expected.v`)
    ///
    /// # Returns
    /// - `Updated` — successfully updated
    /// - `Conflict { actual }` — current didn't match expected
    async fn push_config(
        &self,
        alias: &str,
        expected: Option<&ConfigValue>,
        new: &ConfigValue,
    ) -> Result<ConfigCasResult>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_alias_with_branch() {
        let (ledger, branch) = parse_alias("mydb:main").unwrap();
        assert_eq!(ledger, "mydb");
        assert_eq!(branch, "main");
    }

    #[test]
    fn test_parse_alias_without_branch() {
        let (ledger, branch) = parse_alias("mydb").unwrap();
        assert_eq!(ledger, "mydb");
        assert_eq!(branch, "main");
    }

    #[test]
    fn test_parse_alias_with_slashes() {
        let (ledger, branch) = parse_alias("tenant/customers:dev").unwrap();
        assert_eq!(ledger, "tenant/customers");
        assert_eq!(branch, "dev");
    }

    #[test]
    fn test_parse_alias_empty() {
        assert!(parse_alias("").is_err());
        assert!(parse_alias(":main").is_err());
        assert!(parse_alias("ledger:").is_err());
    }

    #[test]
    fn test_ns_record_new() {
        let record = NsRecord::new("mydb", "main");
        assert_eq!(record.alias, "mydb");
        assert_eq!(record.branch, "main");
        assert_eq!(record.address, "mydb:main");
        assert_eq!(record.commit_t, 0);
        assert_eq!(record.index_t, 0);
        assert!(!record.retracted);
    }

    #[test]
    fn test_ns_record_has_novelty() {
        let mut record = NsRecord::new("mydb", "main");
        assert!(!record.has_novelty());

        record.commit_t = 10;
        record.index_t = 5;
        assert!(record.has_novelty());

        record.index_t = 10;
        assert!(!record.has_novelty());
    }

    #[test]
    fn test_vg_type_to_string() {
        assert_eq!(VgType::Bm25.to_type_string(), "fidx:BM25");
        assert_eq!(VgType::Vector.to_type_string(), "fidx:Vector");
        assert_eq!(VgType::R2rml.to_type_string(), "fidx:R2RML");
        assert_eq!(VgType::Iceberg.to_type_string(), "fidx:Iceberg");
        assert_eq!(
            VgType::Unknown("fidx:Custom".to_string()).to_type_string(),
            "fidx:Custom"
        );
    }

    #[test]
    fn test_vg_type_from_string() {
        assert_eq!(VgType::from_type_string("fidx:BM25"), VgType::Bm25);
        assert_eq!(VgType::from_type_string("fidx:Vector"), VgType::Vector);
        assert_eq!(VgType::from_type_string("fidx:R2RML"), VgType::R2rml);
        assert_eq!(VgType::from_type_string("fidx:Iceberg"), VgType::Iceberg);
        // Fuzzy matching
        assert_eq!(
            VgType::from_type_string("https://ns.flur.ee/index#BM25"),
            VgType::Bm25
        );
        assert_eq!(
            VgType::from_type_string("https://ns.flur.ee/index#Vector"),
            VgType::Vector
        );
        assert_eq!(
            VgType::from_type_string("fidx:Custom"),
            VgType::Unknown("fidx:Custom".to_string())
        );
    }

    #[test]
    fn test_vg_ns_record_new() {
        let record = VgNsRecord::new(
            "my-search",
            "main",
            VgType::Bm25,
            r#"{"k1": 1.2, "b": 0.75}"#,
            vec!["source-ledger:main".to_string()],
        );

        assert_eq!(record.name, "my-search");
        assert_eq!(record.branch, "main");
        assert_eq!(record.address, "my-search:main");
        assert_eq!(record.vg_type, VgType::Bm25);
        assert_eq!(record.config, r#"{"k1": 1.2, "b": 0.75}"#);
        assert_eq!(record.dependencies, vec!["source-ledger:main".to_string()]);
        assert_eq!(record.index_address, None);
        assert_eq!(record.index_t, 0);
        assert!(!record.retracted);
    }

    #[test]
    fn test_vg_ns_record_is_bm25() {
        let bm25 = VgNsRecord::new("search", "main", VgType::Bm25, "{}", vec![]);
        let r2rml = VgNsRecord::new("mapping", "main", VgType::R2rml, "{}", vec![]);

        assert!(bm25.is_bm25());
        assert!(!r2rml.is_bm25());
    }

    #[test]
    fn test_vg_ns_record_alias() {
        let record = VgNsRecord::new("my-search", "dev", VgType::Bm25, "{}", vec![]);
        assert_eq!(record.alias(), "my-search:dev");
    }

    #[test]
    fn test_vg_ns_record_has_index() {
        let mut record = VgNsRecord::new("search", "main", VgType::Bm25, "{}", vec![]);
        assert!(!record.has_index());

        record.index_address = Some("fluree:file://vg/search/snapshot.bin".to_string());
        record.index_t = 42;
        assert!(record.has_index());
    }

    // ========== VgSnapshotHistory Tests ==========

    #[test]
    fn test_vg_snapshot_history_new() {
        let history = VgSnapshotHistory::new("my-search:main");
        assert_eq!(history.alias, "my-search:main");
        assert!(history.snapshots.is_empty());
        assert!(history.head().is_none());
    }

    #[test]
    fn test_vg_snapshot_history_append_monotonic() {
        let mut history = VgSnapshotHistory::new("vg:main");

        // First append should succeed
        assert!(history.append(VgSnapshotEntry::new(5, "addr-5")));
        assert_eq!(history.snapshots.len(), 1);

        // Strictly increasing should succeed
        assert!(history.append(VgSnapshotEntry::new(10, "addr-10")));
        assert_eq!(history.snapshots.len(), 2);

        // Equal t should be rejected
        assert!(!history.append(VgSnapshotEntry::new(10, "addr-10-dup")));
        assert_eq!(history.snapshots.len(), 2);

        // Lower t should be rejected
        assert!(!history.append(VgSnapshotEntry::new(7, "addr-7")));
        assert_eq!(history.snapshots.len(), 2);

        // Higher t should succeed
        assert!(history.append(VgSnapshotEntry::new(20, "addr-20")));
        assert_eq!(history.snapshots.len(), 3);
    }

    #[test]
    fn test_vg_snapshot_history_select_snapshot() {
        let mut history = VgSnapshotHistory::new("vg:main");
        history.append(VgSnapshotEntry::new(5, "addr-5"));
        history.append(VgSnapshotEntry::new(10, "addr-10"));
        history.append(VgSnapshotEntry::new(20, "addr-20"));

        // Exact match
        let snap = history.select_snapshot(10).unwrap();
        assert_eq!(snap.index_t, 10);
        assert_eq!(snap.index_address, "addr-10");

        // Should select best <= as_of_t
        let snap = history.select_snapshot(15).unwrap();
        assert_eq!(snap.index_t, 10); // Largest t <= 15

        // at as_of_t = 5, select t=5
        let snap = history.select_snapshot(5).unwrap();
        assert_eq!(snap.index_t, 5);

        // at as_of_t = 25, select t=20 (highest)
        let snap = history.select_snapshot(25).unwrap();
        assert_eq!(snap.index_t, 20);

        // at as_of_t = 3, no suitable snapshot
        assert!(history.select_snapshot(3).is_none());
    }

    #[test]
    fn test_vg_snapshot_history_head() {
        let mut history = VgSnapshotHistory::new("vg:main");
        assert!(history.head().is_none());

        history.append(VgSnapshotEntry::new(10, "addr-10"));
        assert_eq!(history.head().unwrap().index_t, 10);

        history.append(VgSnapshotEntry::new(20, "addr-20"));
        assert_eq!(history.head().unwrap().index_t, 20);
    }

    #[test]
    fn test_vg_snapshot_history_has_snapshot_at() {
        let mut history = VgSnapshotHistory::new("vg:main");
        history.append(VgSnapshotEntry::new(10, "addr-10"));
        history.append(VgSnapshotEntry::new(20, "addr-20"));

        assert!(history.has_snapshot_at(10));
        assert!(history.has_snapshot_at(20));
        assert!(!history.has_snapshot_at(15));
        assert!(!history.has_snapshot_at(5));
    }

    #[test]
    fn test_vg_snapshot_entry_new() {
        let entry = VgSnapshotEntry::new(42, "fluree:file://vg/snapshot.bin");
        assert_eq!(entry.index_t, 42);
        assert_eq!(entry.index_address, "fluree:file://vg/snapshot.bin");
    }

    // ========== V2 Concern Type Tests ==========

    #[test]
    fn test_concern_kind_as_ref_kind() {
        assert_eq!(ConcernKind::Head.as_ref_kind(), Some(RefKind::CommitHead));
        assert_eq!(ConcernKind::Index.as_ref_kind(), Some(RefKind::IndexHead));
        assert_eq!(ConcernKind::Status.as_ref_kind(), None);
        assert_eq!(ConcernKind::Config.as_ref_kind(), None);
    }

    #[test]
    fn test_status_payload_new() {
        let status = StatusPayload::new("ready");
        assert_eq!(status.state, "ready");
        assert!(status.extra.is_empty());
        assert!(status.is_ready());
        assert!(!status.is_retracted());
    }

    #[test]
    fn test_status_payload_with_extra() {
        let mut extra = std::collections::HashMap::new();
        extra.insert("queue_depth".to_string(), serde_json::json!(5));
        extra.insert("last_commit_ms".to_string(), serde_json::json!(42));

        let status = StatusPayload::with_extra("indexing", extra);
        assert_eq!(status.state, "indexing");
        assert_eq!(status.extra.get("queue_depth"), Some(&serde_json::json!(5)));
        assert!(!status.is_ready());
    }

    #[test]
    fn test_status_payload_default() {
        let status = StatusPayload::default();
        assert_eq!(status.state, "ready");
        assert!(status.extra.is_empty());
    }

    #[test]
    fn test_config_payload_new() {
        let config = ConfigPayload::new();
        assert_eq!(config.default_context, None);
        assert!(config.extra.is_empty());
    }

    #[test]
    fn test_config_payload_with_default_context() {
        let config = ConfigPayload::with_default_context("fluree:file://contexts/v1.json");
        assert_eq!(
            config.default_context,
            Some("fluree:file://contexts/v1.json".to_string())
        );
    }

    #[test]
    fn test_status_value_new() {
        let status = StatusValue::new(42, StatusPayload::new("ready"));
        assert_eq!(status.v, 42);
        assert_eq!(status.payload.state, "ready");
    }

    #[test]
    fn test_status_value_initial() {
        let status = StatusValue::initial();
        assert_eq!(status.v, 1);
        assert_eq!(status.payload.state, "ready");
    }

    #[test]
    fn test_config_value_new() {
        let config = ConfigValue::new(5, Some(ConfigPayload::with_default_context("ctx")));
        assert_eq!(config.v, 5);
        assert!(config.payload.is_some());
        assert!(!config.is_unborn());
    }

    #[test]
    fn test_config_value_unborn() {
        let config = ConfigValue::unborn();
        assert_eq!(config.v, 0);
        assert!(config.payload.is_none());
        assert!(config.is_unborn());
    }

    #[test]
    fn test_status_cas_result() {
        let updated = StatusCasResult::Updated;
        let conflict = StatusCasResult::Conflict {
            actual: Some(StatusValue::initial()),
        };

        assert!(matches!(updated, StatusCasResult::Updated));
        assert!(matches!(conflict, StatusCasResult::Conflict { .. }));
    }

    #[test]
    fn test_config_cas_result() {
        let updated = ConfigCasResult::Updated;
        let conflict = ConfigCasResult::Conflict {
            actual: Some(ConfigValue::unborn()),
        };

        assert!(matches!(updated, ConfigCasResult::Updated));
        assert!(matches!(conflict, ConfigCasResult::Conflict { .. }));
    }
}
