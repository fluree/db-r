//! File-based nameservice implementation using ns@v2 format
//!
//! This implementation stores records as JSON files following the ns@v2 format:
//! - `data/ns@v2/{ledger-name}/{branch}.json` - Main record (commit info)
//! - `data/ns@v2/{ledger-name}/{branch}.index.json` - Index record (separate for indexer)
//!
//! The separation of commit and index files allows transactors and indexers
//! to update independently without contention.
//!
//! # Concurrency Warning
//!
//! When built with the `native` feature on Unix platforms, this implementation uses
//! an OS-level file lock (flock/fcntl semantics via `libc::flock`) to make the
//! read-modify-write cycle atomic across processes.
//!
//! For single-writer scenarios (one transactor per ledger, one indexer per ledger),
//! this implementation is safe. The separation of commit and index files enables
//! a transactor and indexer to operate independently on the same ledger.
//!
//! For multi-writer scenarios across machines (or filesystems where OS locks are not
//! reliable, e.g. some networked FS), use a nameservice backend with CAS semantics:
//! - S3 with ETag conditional writes
//! - DynamoDB with conditional expressions
//! - A database with transactions
//!
//! Note: this file-locking approach provides mutual exclusion, not a distributed CAS.

use crate::{
    parse_alias, AdminPublisher, CasResult, ConfigCasResult, ConfigPayload, ConfigPublisher,
    ConfigValue, NameService, NameServiceError, NameServiceEvent, NsLookupResult, NsRecord,
    Publication, Publisher, RefKind, RefPublisher, RefValue, Result, StatusCasResult,
    StatusPayload, StatusPublisher, StatusValue, Subscription, VgNsRecord, VgSnapshotEntry,
    VgSnapshotHistory, VgType, VirtualGraphPublisher,
};
use async_trait::async_trait;
use fluree_db_core::alias as core_alias;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fs::OpenOptions;
use std::io::Write;
#[cfg(all(feature = "native", unix))]
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::broadcast;

/// File-based nameservice using ns@v2 format
#[derive(Clone)]
pub struct FileNameService {
    /// Base path for storage
    base_path: PathBuf,
    /// In-process event sender for reactive subscriptions.
    event_tx: broadcast::Sender<NameServiceEvent>,
}

impl Debug for FileNameService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileNameService")
            .field("base_path", &self.base_path)
            .finish()
    }
}

/// JSON structure for main ns@v2 record file
#[derive(Debug, Serialize, Deserialize)]
struct NsFileV2 {
    /// Context can be either a string or an object with prefix mappings
    #[serde(rename = "@context")]
    context: serde_json::Value,

    #[serde(rename = "@id")]
    id: String,

    #[serde(rename = "@type")]
    record_type: Vec<String>,

    #[serde(rename = "f:ledger")]
    ledger: LedgerRef,

    #[serde(rename = "f:branch")]
    branch: String,

    #[serde(rename = "f:commit", skip_serializing_if = "Option::is_none")]
    commit: Option<AddressRef>,

    #[serde(rename = "f:t")]
    t: i64,

    #[serde(rename = "f:index", skip_serializing_if = "Option::is_none")]
    index: Option<IndexRef>,

    #[serde(rename = "f:status")]
    status: String,

    #[serde(rename = "f:defaultContext", skip_serializing_if = "Option::is_none")]
    default_context: Option<AddressRef>,

    // V2 extension fields (optional for backward compatibility)
    /// Status watermark (v2 extension) - defaults to 1 if missing
    #[serde(rename = "f:statusV", skip_serializing_if = "Option::is_none")]
    status_v: Option<i64>,

    /// Status metadata beyond the state field (v2 extension)
    #[serde(rename = "f:statusMeta", skip_serializing_if = "Option::is_none")]
    status_meta: Option<std::collections::HashMap<String, serde_json::Value>>,

    /// Config watermark (v2 extension) - defaults to 0 (unborn) if missing
    #[serde(rename = "f:configV", skip_serializing_if = "Option::is_none")]
    config_v: Option<i64>,

    /// Config metadata beyond default_context (v2 extension)
    #[serde(rename = "f:configMeta", skip_serializing_if = "Option::is_none")]
    config_meta: Option<std::collections::HashMap<String, serde_json::Value>>,
}

/// JSON structure for index-only ns@v2 file
#[derive(Debug, Serialize, Deserialize)]
struct NsIndexFileV2 {
    /// Context can be either a string or an object with prefix mappings
    #[serde(rename = "@context")]
    context: serde_json::Value,

    #[serde(rename = "f:index")]
    index: IndexRef,
}

#[derive(Debug, Serialize, Deserialize)]
struct LedgerRef {
    #[serde(rename = "@id")]
    id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct AddressRef {
    #[serde(rename = "@id")]
    id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexRef {
    #[serde(rename = "@id")]
    id: String,

    #[serde(rename = "f:t")]
    t: i64,
}

/// JSON structure for VG ns@v2 config record file
///
/// VG records use the same ns@v2 path pattern but have different fields:
/// - `@type` includes "f:VirtualGraphDatabase" and a VG-specific type (fidx:BM25, etc.)
/// - `fidx:config` contains the VG configuration as a JSON string
/// - `fidx:dependencies` lists dependent ledger aliases
#[derive(Debug, Serialize, Deserialize)]
struct VgNsFileV2 {
    /// Context includes both f: (ledger) and fidx: (index) namespaces
    #[serde(rename = "@context")]
    context: serde_json::Value,

    #[serde(rename = "@id")]
    id: String,

    /// Type array includes "f:VirtualGraphDatabase" and VG-specific type
    #[serde(rename = "@type")]
    record_type: Vec<String>,

    /// Base name of the VG
    #[serde(rename = "f:name")]
    name: String,

    /// Branch name
    #[serde(rename = "f:branch")]
    branch: String,

    /// VG configuration as JSON string
    #[serde(rename = "fidx:config")]
    config: ConfigRef,

    /// Dependent ledger aliases
    #[serde(rename = "fidx:dependencies")]
    dependencies: Vec<String>,

    /// Status (ready/retracted)
    #[serde(rename = "f:status")]
    status: String,
}

/// Config stored as JSON string with @value wrapper
#[derive(Debug, Serialize, Deserialize)]
struct ConfigRef {
    #[serde(rename = "@value")]
    value: String,
}

/// Reference to a VG index address
#[derive(Debug, Serialize, Deserialize)]
struct VgIndexRef {
    #[serde(rename = "@type")]
    ref_type: String,

    #[serde(rename = "@value")]
    address: String,
}

/// JSON structure for VG index record (separate from config)
///
/// Stored at `ns@v2/{vg-name}/{branch}.index.json` to avoid contention
/// between config updates and index updates. Uses monotonic update rule:
/// only write if new index_t > existing index_t.
#[derive(Debug, Serialize, Deserialize)]
struct VgIndexFileV2WithT {
    #[serde(rename = "@context")]
    context: serde_json::Value,

    #[serde(rename = "@id")]
    id: String,

    #[serde(rename = "fidx:index")]
    index: VgIndexRef,

    #[serde(rename = "fidx:indexT")]
    index_t: i64,
}

/// JSON entry for a single snapshot in the history
#[derive(Debug, Serialize, Deserialize)]
struct VgSnapshotEntryV2 {
    #[serde(rename = "fidx:indexT")]
    index_t: i64,

    #[serde(rename = "fidx:indexAddress")]
    index_address: String,
}

/// JSON structure for VG snapshot history file
///
/// Stored at `ns@v2/{vg-name}/{branch}.snapshots.json` for time-travel support.
/// Contains an ordered list of snapshots (monotonically increasing by index_t).
#[derive(Debug, Serialize, Deserialize)]
struct VgSnapshotsFileV2 {
    #[serde(rename = "@context")]
    context: serde_json::Value,

    #[serde(rename = "@id")]
    id: String,

    #[serde(rename = "fidx:snapshots")]
    snapshots: Vec<VgSnapshotEntryV2>,
}

const NS_CONTEXT_IRI: &str = "https://ns.flur.ee/ledger#";
const FIDX_CONTEXT_IRI: &str = "https://ns.flur.ee/index#";
const NS_VERSION: &str = "ns@v2";

/// Create the standard ns@v2 context as JSON value
/// Uses object format `{"f": "https://ns.flur.ee/ledger#"}` for Clojure compatibility
fn ns_context() -> serde_json::Value {
    serde_json::json!({"f": NS_CONTEXT_IRI})
}

/// Create VG ns@v2 context including both f: and fidx: namespaces
fn vg_context() -> serde_json::Value {
    serde_json::json!({
        "f": NS_CONTEXT_IRI,
        "fidx": FIDX_CONTEXT_IRI
    })
}

#[cfg(all(feature = "native", unix))]
struct FlockGuard {
    file: std::fs::File,
}

#[cfg(all(feature = "native", unix))]
impl FlockGuard {
    fn lock_exclusive(path: &Path) -> Result<Self> {
        // Ensure parent directory exists for the lock file.
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(NameServiceError::Io)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(NameServiceError::Io)?;

        let fd = file.as_raw_fd();
        let rc = unsafe { libc::flock(fd, libc::LOCK_EX) };
        if rc != 0 {
            return Err(NameServiceError::storage(format!(
                "Failed to acquire file lock on {:?}",
                path
            )));
        }

        Ok(Self { file })
    }
}

#[cfg(all(feature = "native", unix))]
impl Drop for FlockGuard {
    fn drop(&mut self) {
        let fd = self.file.as_raw_fd();
        unsafe {
            libc::flock(fd, libc::LOCK_UN);
        }
    }
}

impl FileNameService {
    /// Create a new file-based nameservice
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        // Small buffer; consumers should treat this as best-effort.
        let (event_tx, _event_rx) = broadcast::channel(128);
        Self {
            base_path: base_path.into(),
            event_tx,
        }
    }

    /// Get the path for the main ns record
    fn ns_path(&self, ledger_name: &str, branch: &str) -> PathBuf {
        self.base_path
            .join(NS_VERSION)
            .join(ledger_name)
            .join(format!("{}.json", branch))
    }

    /// Get the path for the index-only ns record
    fn index_path(&self, ledger_name: &str, branch: &str) -> PathBuf {
        self.base_path
            .join(NS_VERSION)
            .join(ledger_name)
            .join(format!("{}.index.json", branch))
    }

    /// Get the path for the VG snapshots history file
    fn snapshots_path(&self, name: &str, branch: &str) -> PathBuf {
        self.base_path
            .join(NS_VERSION)
            .join(name)
            .join(format!("{}.snapshots.json", branch))
    }

    /// Read and parse a JSON file
    async fn read_json<T: for<'de> Deserialize<'de>>(&self, path: &Path) -> Result<Option<T>> {
        if !path.exists() {
            return Ok(None);
        }

        let content = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| NameServiceError::storage(format!("Failed to read {:?}: {}", path, e)))?;

        let parsed = serde_json::from_str(&content)?;
        Ok(Some(parsed))
    }

    /// Write a JSON file atomically (write to .tmp then rename)
    ///
    /// Used in non-Unix / non-locking fallback builds.
    #[cfg(not(all(feature = "native", unix)))]
    async fn write_json_atomic<T: Serialize>(&self, path: &Path, value: &T) -> Result<()> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                NameServiceError::storage(format!("Failed to create directory {:?}: {}", parent, e))
            })?;
        }

        let content = serde_json::to_string_pretty(value)?;

        // Write to temp file
        let tmp_path = path.with_extension("json.tmp");
        tokio::fs::write(&tmp_path, &content).await.map_err(|e| {
            NameServiceError::storage(format!("Failed to write {:?}: {}", tmp_path, e))
        })?;

        // Rename to final path (atomic on most filesystems)
        tokio::fs::rename(&tmp_path, path).await.map_err(|e| {
            NameServiceError::storage(format!(
                "Failed to rename {:?} to {:?}: {}",
                tmp_path, path, e
            ))
        })?;

        Ok(())
    }

    /// Atomically update a JSON file under an OS-level lock (native, unix only).
    ///
    /// This mirrors Clojure's `swap-json` behavior on FileStore: lock + read + transform + write.
    ///
    /// The update function receives the current parsed value (or None if missing) and
    /// returns Some(new_value) to write, or None to perform a no-op.
    #[cfg(all(feature = "native", unix))]
    async fn swap_json_locked<T, F>(&self, path: PathBuf, update: F) -> Result<()>
    where
        T: DeserializeOwned + Serialize + Send + 'static,
        F: FnOnce(Option<T>) -> Result<Option<T>> + Send + 'static,
    {
        tokio::task::spawn_blocking(move || -> Result<()> {
            // Lock file lives alongside the target.
            let lock_path = {
                let mut p = path.clone();
                let file_name = p
                    .file_name()
                    .and_then(|s| s.to_str())
                    .ok_or_else(|| NameServiceError::storage("Invalid file name".to_string()))?;
                p.set_file_name(format!("{}.lock", file_name));
                p
            };

            let _guard = FlockGuard::lock_exclusive(&lock_path)?;

            // Read existing (if any)
            let existing: Option<T> = match std::fs::read_to_string(&path) {
                Ok(s) => Some(serde_json::from_str(&s)?),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => return Err(NameServiceError::Io(e)),
            };

            let new_value = match update(existing)? {
                Some(v) => v,
                None => return Ok(()),
            };

            // Ensure parent exists
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(NameServiceError::Io)?;
            }

            // Write to a temp file then rename (atomic on most local filesystems).
            let tmp_path = {
                let mut p = path.clone();
                let pid = std::process::id();
                let nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0);
                let file_name = p
                    .file_name()
                    .and_then(|s| s.to_str())
                    .ok_or_else(|| NameServiceError::storage("Invalid file name".to_string()))?;
                p.set_file_name(format!("{}.tmp.{}.{}", file_name, pid, nanos));
                p
            };

            let content = serde_json::to_string_pretty(&new_value)?;
            {
                let mut f = std::fs::File::create(&tmp_path).map_err(NameServiceError::Io)?;
                f.write_all(content.as_bytes())
                    .map_err(NameServiceError::Io)?;
                // Optional: f.sync_all() for stronger durability; skipped for perf.
            }

            std::fs::rename(&tmp_path, &path).map_err(NameServiceError::Io)?;
            Ok(())
        })
        .await
        .map_err(|e| NameServiceError::storage(format!("Join error in swap_json_locked: {}", e)))?
    }

    /// Load and merge main record with index file
    async fn load_record(&self, ledger_name: &str, branch: &str) -> Result<Option<NsRecord>> {
        let main_path = self.ns_path(ledger_name, branch);
        let index_path = self.index_path(ledger_name, branch);

        // Read main record
        let main_file: Option<NsFileV2> = self.read_json(&main_path).await?;

        let Some(main) = main_file else {
            return Ok(None);
        };

        // Read index file (if exists)
        let index_file: Option<NsIndexFileV2> = self.read_json(&index_path).await?;

        // Convert to NsRecord
        let mut record = NsRecord {
            address: core_alias::format_alias(ledger_name, branch),
            alias: main.ledger.id.clone(),
            branch: main.branch,
            commit_address: main.commit.map(|c| c.id),
            commit_t: main.t,
            index_address: main.index.as_ref().map(|i| i.id.clone()),
            index_t: main.index.as_ref().map(|i| i.t).unwrap_or(0),
            default_context_address: main.default_context.map(|c| c.id),
            retracted: main.status == "retracted",
        };

        // Merge index file if it has equal or higher t (READ-TIME merge rule)
        if let Some(index_data) = index_file {
            if index_data.index.t >= record.index_t {
                record.index_address = Some(index_data.index.id);
                record.index_t = index_data.index.t;
            }
        }

        Ok(Some(record))
    }

    /// Check if a record file is a VG record (based on @type).
    /// Uses exact match for "f:VirtualGraphDatabase" or full IRI.
    async fn is_vg_record(&self, name: &str, branch: &str) -> Result<bool> {
        let main_path = self.ns_path(name, branch);
        if !main_path.exists() {
            return Ok(false);
        }

        let content = tokio::fs::read_to_string(&main_path).await.map_err(|e| {
            NameServiceError::storage(format!("Failed to read {:?}: {}", main_path, e))
        })?;

        // Parse just enough to check @type
        let parsed: serde_json::Value = serde_json::from_str(&content)?;
        Ok(Self::is_vg_from_json(&parsed))
    }

    /// Check if parsed JSON represents a VG record (exact match).
    fn is_vg_from_json(parsed: &serde_json::Value) -> bool {
        if let Some(types) = parsed.get("@type").and_then(|t| t.as_array()) {
            for t in types {
                if let Some(s) = t.as_str() {
                    // Exact match: either prefixed or full IRI
                    if s == "f:VirtualGraphDatabase"
                        || s == "https://ns.flur.ee/ledger#VirtualGraphDatabase"
                    {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Load a VG config record and merge with index file
    async fn load_vg_record(&self, name: &str, branch: &str) -> Result<Option<VgNsRecord>> {
        let main_path = self.ns_path(name, branch);
        let index_path = self.index_path(name, branch);

        // Read main record
        let main_file: Option<VgNsFileV2> = self.read_json(&main_path).await?;

        let Some(main) = main_file else {
            return Ok(None);
        };

        // Determine VG type from @type array (exact match, excluding VirtualGraphDatabase)
        let vg_type = main
            .record_type
            .iter()
            .find(|t| {
                *t != "f:VirtualGraphDatabase"
                    && *t != "https://ns.flur.ee/ledger#VirtualGraphDatabase"
            })
            .map(|t| VgType::from_type_string(t))
            .unwrap_or(VgType::Unknown("unknown".to_string()));

        // Convert to VgNsRecord
        let mut record = VgNsRecord {
            address: core_alias::format_alias(name, branch),
            name: main.name,
            branch: main.branch,
            vg_type,
            config: main.config.value,
            dependencies: main.dependencies,
            index_address: None,
            index_t: 0,
            retracted: main.status == "retracted",
        };

        // Read and merge VG index file (if exists)
        let index_file: Option<VgIndexFileV2WithT> = self.read_json(&index_path).await?;
        if let Some(index_data) = index_file {
            if index_data.index_t > record.index_t {
                record.index_address = Some(index_data.index.address);
                record.index_t = index_data.index_t;
            }
        }

        Ok(Some(record))
    }

    /// Create NsFileV2 from record
    ///
    /// Used in non-Unix / non-locking fallback builds.
    #[cfg(not(all(feature = "native", unix)))]
    fn record_to_file(&self, record: &NsRecord) -> NsFileV2 {
        NsFileV2 {
            context: ns_context(),
            id: record.address.clone(),
            record_type: vec!["f:Database".to_string(), "f:PhysicalDatabase".to_string()],
            ledger: LedgerRef {
                id: record.alias.clone(),
            },
            branch: record.branch.clone(),
            commit: record
                .commit_address
                .as_ref()
                .map(|a| AddressRef { id: a.clone() }),
            t: record.commit_t,
            index: record.index_address.as_ref().map(|a| IndexRef {
                id: a.clone(),
                t: record.index_t,
            }),
            status: if record.retracted {
                "retracted".to_string()
            } else {
                "ready".to_string()
            },
            default_context: record
                .default_context_address
                .as_ref()
                .map(|a| AddressRef { id: a.clone() }),
            // v2 extension fields (not set by record_to_file - preserved by swap_json_locked)
            status_v: None,
            status_meta: None,
            config_v: None,
            config_meta: None,
        }
    }
}

#[async_trait]
impl NameService for FileNameService {
    async fn lookup(&self, ledger_address: &str) -> Result<Option<NsRecord>> {
        let (ledger_name, branch) = parse_alias(ledger_address)?;
        self.load_record(&ledger_name, &branch).await
    }

    async fn alias(&self, ledger_address: &str) -> Result<Option<String>> {
        Ok(self.lookup(ledger_address).await?.map(|r| r.alias))
    }

    async fn all_records(&self) -> Result<Vec<NsRecord>> {
        let ns_dir = self.base_path.join(NS_VERSION);

        if !ns_dir.exists() {
            return Ok(vec![]);
        }

        let mut records = Vec::new();

        // Walk the ns@v2 directory recursively so ledger names that contain '/'
        // (e.g., "tenant1/customers") are discovered (Clojure parity).
        let mut stack = vec![ns_dir];
        let ns_dir_base = self.base_path.join(NS_VERSION);

        while let Some(current_dir) = stack.pop() {
            let mut dir_entries = tokio::fs::read_dir(&current_dir).await.map_err(|e| {
                NameServiceError::storage(format!(
                    "Failed to read directory {:?}: {}",
                    current_dir, e
                ))
            })?;

            while let Some(entry) = dir_entries.next_entry().await.map_err(|e| {
                NameServiceError::storage(format!("Failed to read directory entry: {}", e))
            })? {
                let path = entry.path();

                if path.is_dir() {
                    stack.push(path);
                    continue;
                }

                if !path.is_file() {
                    continue;
                }

                let file_name = entry.file_name().to_string_lossy().to_string();

                // Skip index files and snapshot files, only process main .json files
                if file_name.ends_with(".index.json") || file_name.ends_with(".snapshots.json") {
                    continue;
                }

                if !file_name.ends_with(".json") {
                    continue;
                }

                let branch = file_name.trim_end_matches(".json");
                let Ok(relative_path) = path.strip_prefix(&ns_dir_base) else {
                    continue;
                };
                let parent = relative_path
                    .parent()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_default();
                if parent.is_empty() {
                    continue;
                }

                // Exclude virtual graph records from ledger records.
                if self.is_vg_record(&parent, branch).await? {
                    continue;
                }

                if let Ok(Some(record)) = self.load_record(&parent, branch).await {
                    records.push(record);
                }
            }
        }

        Ok(records)
    }
}

#[async_trait]
impl Publisher for FileNameService {
    async fn publish_ledger_init(&self, alias: &str) -> Result<()> {
        let (ledger_name, branch) = parse_alias(alias)?;
        let main_path = self.ns_path(&ledger_name, &branch);
        let normalized_alias = core_alias::format_alias(&ledger_name, &branch);

        #[cfg(all(feature = "native", unix))]
        {
            let path = main_path.clone();
            let ledger_name_for_file = ledger_name.clone();
            let branch_for_file = branch.clone();
            let normalized_alias_for_error = normalized_alias.clone();

            // Use swap_json_locked to atomically check-and-create
            return self
                .swap_json_locked::<NsFileV2, _>(path, move |existing| {
                    if existing.is_some() {
                        // Record already exists (including retracted) - return error
                        return Err(NameServiceError::ledger_already_exists(
                            normalized_alias_for_error,
                        ));
                    }

                    // Create minimal record with no commits
                    let file = NsFileV2 {
                        context: ns_context(),
                        id: core_alias::format_alias(&ledger_name_for_file, &branch_for_file),
                        record_type: vec![
                            "f:Database".to_string(),
                            "f:PhysicalDatabase".to_string(),
                        ],
                        ledger: LedgerRef {
                            id: ledger_name_for_file.clone(),
                        },
                        branch: branch_for_file.clone(),
                        commit: None,
                        t: 0,
                        index: None,
                        status: "ready".to_string(),
                        default_context: None,
                        // v2 extension fields - initialize with defaults
                        status_v: Some(1), // Initial status
                        status_meta: None,
                        config_v: Some(0), // Unborn config
                        config_meta: None,
                    };
                    Ok(Some(file))
                })
                .await;
        }

        #[cfg(not(all(feature = "native", unix)))]
        {
            // Non-locking fallback: check if file exists, then create
            if main_path.exists() {
                return Err(NameServiceError::ledger_already_exists(normalized_alias));
            }

            let file = NsFileV2 {
                context: ns_context(),
                id: normalized_alias.clone(),
                record_type: vec!["f:Database".to_string(), "f:PhysicalDatabase".to_string()],
                ledger: LedgerRef {
                    id: ledger_name.clone(),
                },
                branch: branch.clone(),
                commit: None,
                t: 0,
                index: None,
                status: "ready".to_string(),
                default_context: None,
                // v2 extension fields - initialize with defaults
                status_v: Some(1), // Initial status
                status_meta: None,
                config_v: Some(0), // Unborn config
                config_meta: None,
            };
            self.write_json_atomic(&main_path, &file).await?;
            Ok(())
        }
    }

    async fn publish_commit(&self, alias: &str, commit_addr: &str, commit_t: i64) -> Result<()> {
        let (ledger_name, branch) = parse_alias(alias)?;
        let main_path = self.ns_path(&ledger_name, &branch);

        #[cfg(all(feature = "native", unix))]
        {
            let path = main_path.clone();
            let commit_addr = commit_addr.to_string();
            let commit_addr_for_event = commit_addr.clone();
            let ledger_name_for_file = ledger_name.clone();
            let branch_for_file = branch.clone();
            let alias_for_event = core_alias::format_alias(&ledger_name, &branch);

            let did_update = Arc::new(AtomicBool::new(false));
            let did_update2 = did_update.clone();

            let res = self
                .swap_json_locked::<NsFileV2, _>(path, move |existing| {
                    match existing {
                        Some(mut file) => {
                            // Strictly monotonic update
                            if commit_t > file.t {
                                file.commit = Some(AddressRef { id: commit_addr });
                                file.t = commit_t;
                                did_update2.store(true, Ordering::SeqCst);
                                Ok(Some(file))
                            } else {
                                Ok(None)
                            }
                        }
                        None => {
                            // Create new record (always write)
                            let file = NsFileV2 {
                                context: ns_context(),
                                id: core_alias::format_alias(
                                    &ledger_name_for_file,
                                    &branch_for_file,
                                ),
                                record_type: vec![
                                    "f:Database".to_string(),
                                    "f:PhysicalDatabase".to_string(),
                                ],
                                ledger: LedgerRef {
                                    id: ledger_name_for_file.clone(),
                                },
                                branch: branch_for_file.clone(),
                                commit: Some(AddressRef { id: commit_addr }),
                                t: commit_t,
                                index: None,
                                status: "ready".to_string(),
                                default_context: None,
                                // v2 extension fields - initialize with defaults
                                status_v: Some(1),
                                status_meta: None,
                                config_v: Some(0),
                                config_meta: None,
                            };
                            did_update2.store(true, Ordering::SeqCst);
                            Ok(Some(file))
                        }
                    }
                })
                .await;

            if res.is_ok() && did_update.load(Ordering::SeqCst) {
                let _ = self.event_tx.send(NameServiceEvent::LedgerCommitPublished {
                    alias: alias_for_event,
                    commit_address: commit_addr_for_event,
                    commit_t,
                });
            }

            return res;
        }

        #[cfg(not(all(feature = "native", unix)))]
        {
            // Fallback: no cross-process lock available.
            // Keep behavior for testing/dev, but this is not safe for concurrent writers.
            let existing: Option<NsFileV2> = self.read_json(&main_path).await?;
            let mut did_update = false;

            let file = if let Some(mut file) = existing {
                if commit_t > file.t {
                    file.commit = Some(AddressRef {
                        id: commit_addr.to_string(),
                    });
                    file.t = commit_t;
                    did_update = true;
                }
                file
            } else {
                let record = NsRecord {
                    address: core_alias::format_alias(&ledger_name, &branch),
                    alias: ledger_name.clone(),
                    branch: branch.clone(),
                    commit_address: Some(commit_addr.to_string()),
                    commit_t,
                    index_address: None,
                    index_t: 0,
                    default_context_address: None,
                    retracted: false,
                };
                did_update = true;
                self.record_to_file(&record)
            };

            self.write_json_atomic(&main_path, &file).await?;
            if did_update {
                let _ = self.event_tx.send(NameServiceEvent::LedgerCommitPublished {
                    alias: core_alias::format_alias(&ledger_name, &branch),
                    commit_address: commit_addr.to_string(),
                    commit_t,
                });
            }
            Ok(())
        }
    }

    async fn publish_index(&self, alias: &str, index_addr: &str, index_t: i64) -> Result<()> {
        let (ledger_name, branch) = parse_alias(alias)?;
        let index_path = self.index_path(&ledger_name, &branch);

        #[cfg(all(feature = "native", unix))]
        {
            let path = index_path.clone();
            let index_addr = index_addr.to_string();
            let index_addr_for_event = index_addr.clone();
            let alias_for_event = core_alias::format_alias(&ledger_name, &branch);
            let did_update = Arc::new(AtomicBool::new(false));
            let did_update2 = did_update.clone();

            let res = self
                .swap_json_locked::<NsIndexFileV2, _>(path, move |existing| {
                    if let Some(existing_file) = &existing {
                        if index_t <= existing_file.index.t {
                            return Ok(None);
                        }
                    }

                    let file = NsIndexFileV2 {
                        context: ns_context(),
                        index: IndexRef {
                            id: index_addr,
                            t: index_t,
                        },
                    };
                    did_update2.store(true, Ordering::SeqCst);
                    Ok(Some(file))
                })
                .await;

            if res.is_ok() && did_update.load(Ordering::SeqCst) {
                let _ = self.event_tx.send(NameServiceEvent::LedgerIndexPublished {
                    alias: alias_for_event,
                    index_address: index_addr_for_event,
                    index_t,
                });
            }

            return res;
        }

        #[cfg(not(all(feature = "native", unix)))]
        {
            let existing: Option<NsIndexFileV2> = self.read_json(&index_path).await?;
            if let Some(existing_file) = &existing {
                if index_t <= existing_file.index.t {
                    return Ok(());
                }
            }

            let file = NsIndexFileV2 {
                context: ns_context(),
                index: IndexRef {
                    id: index_addr.to_string(),
                    t: index_t,
                },
            };
            self.write_json_atomic(&index_path, &file).await?;
            let _ = self.event_tx.send(NameServiceEvent::LedgerIndexPublished {
                alias: core_alias::format_alias(&ledger_name, &branch),
                index_address: index_addr.to_string(),
                index_t,
            });
            Ok(())
        }
    }

    async fn retract(&self, alias: &str) -> Result<()> {
        let (ledger_name, branch) = parse_alias(alias)?;
        let main_path = self.ns_path(&ledger_name, &branch);

        #[cfg(all(feature = "native", unix))]
        {
            let path = main_path.clone();
            let alias_for_event = core_alias::format_alias(&ledger_name, &branch);
            let did_update = Arc::new(AtomicBool::new(false));
            let did_update2 = did_update.clone();

            let res = self
                .swap_json_locked::<NsFileV2, _>(path, move |existing| {
                    let mut file = match existing {
                        Some(f) => f,
                        None => return Ok(None),
                    };
                    if file.status == "retracted" {
                        return Ok(None);
                    }
                    file.status = "retracted".to_string();
                    // Advance status_v when retracting
                    let current_v = file.status_v.unwrap_or(1);
                    file.status_v = Some(current_v + 1);
                    did_update2.store(true, Ordering::SeqCst);
                    Ok(Some(file))
                })
                .await;

            if res.is_ok() && did_update.load(Ordering::SeqCst) {
                let _ = self.event_tx.send(NameServiceEvent::LedgerRetracted {
                    alias: alias_for_event,
                });
            }

            return res;
        }

        #[cfg(not(all(feature = "native", unix)))]
        {
            let existing: Option<NsFileV2> = self.read_json(&main_path).await?;
            if let Some(mut file) = existing {
                if file.status != "retracted" {
                    file.status = "retracted".to_string();
                    // Advance status_v when retracting
                    let current_v = file.status_v.unwrap_or(1);
                    file.status_v = Some(current_v + 1);
                    self.write_json_atomic(&main_path, &file).await?;
                    let _ = self.event_tx.send(NameServiceEvent::LedgerRetracted {
                        alias: core_alias::format_alias(&ledger_name, &branch),
                    });
                }
            }
            Ok(())
        }
    }

    fn publishing_address(&self, alias: &str) -> Option<String> {
        // File nameservice returns the alias as the publishing address
        Some(core_alias::normalize_alias(alias).unwrap_or_else(|_| alias.to_string()))
    }
}

#[async_trait]
impl AdminPublisher for FileNameService {
    async fn publish_index_allow_equal(
        &self,
        alias: &str,
        index_addr: &str,
        index_t: i64,
    ) -> Result<()> {
        let (ledger_name, branch) = parse_alias(alias)?;
        let index_path = self.index_path(&ledger_name, &branch);
        let alias_for_event = core_alias::format_alias(&ledger_name, &branch);

        #[cfg(all(feature = "native", unix))]
        {
            let index_addr = index_addr.to_string();
            let index_addr_for_event = index_addr.clone();
            let did_update = Arc::new(AtomicBool::new(false));
            let did_update2 = did_update.clone();

            let res = self
                .swap_json_locked::<NsIndexFileV2, _>(index_path, move |existing| {
                    let should_update = match &existing {
                        Some(file) => index_t >= file.index.t, // Allow equal
                        None => true,
                    };

                    if should_update {
                        did_update2.store(true, Ordering::SeqCst);
                        // Note: closure must return Result<Option<T>>
                        Ok(Some(NsIndexFileV2 {
                            context: ns_context(),
                            index: IndexRef {
                                id: index_addr.clone(),
                                t: index_t,
                            },
                        }))
                    } else {
                        Ok(None)
                    }
                })
                .await;

            // Only emit event if update actually happened
            if res.is_ok() && did_update.load(Ordering::SeqCst) {
                let _ = self.event_tx.send(NameServiceEvent::LedgerIndexPublished {
                    alias: alias_for_event,
                    index_address: index_addr_for_event,
                    index_t,
                });
            }

            return res;
        }

        #[cfg(not(all(feature = "native", unix)))]
        {
            // Fallback for non-Unix
            let existing: Option<NsIndexFileV2> = self.read_json(&index_path).await?;
            let should_update = match &existing {
                Some(file) => index_t >= file.index.t,
                None => true,
            };

            if should_update {
                let file = NsIndexFileV2 {
                    context: ns_context(),
                    index: IndexRef {
                        id: index_addr.to_string(),
                        t: index_t,
                    },
                };
                self.write_json_atomic(&index_path, &file).await?;

                let _ = self.event_tx.send(NameServiceEvent::LedgerIndexPublished {
                    alias: alias_for_event,
                    index_address: index_addr.to_string(),
                    index_t,
                });
            }

            Ok(())
        }
    }
}

#[async_trait]
impl VirtualGraphPublisher for FileNameService {
    async fn publish_vg(
        &self,
        name: &str,
        branch: &str,
        vg_type: VgType,
        config: &str,
        dependencies: &[String],
    ) -> Result<()> {
        let main_path = self.ns_path(name, branch);

        #[cfg(all(feature = "native", unix))]
        {
            let path = main_path.clone();
            let name = name.to_string();
            let branch = branch.to_string();
            let config = config.to_string();
            let dependencies_for_file = dependencies.to_vec();
            let dependencies_for_event = dependencies_for_file.clone();
            let vg_type_for_event = vg_type.clone();
            let vg_type_str = vg_type.to_type_string();
            let name_for_file = name.clone();
            let branch_for_file = branch.clone();
            let alias_for_event = core_alias::format_alias(&name, &branch);

            let did_update = Arc::new(AtomicBool::new(false));
            let did_update2 = did_update.clone();

            let res = self
                .swap_json_locked::<VgNsFileV2, _>(path, move |existing| {
                    // For VG config, we always update (config changes are allowed)
                    // Only preserve retracted status if already set
                    let status = existing
                        .as_ref()
                        .map(|f| f.status.clone())
                        .filter(|s| s == "retracted")
                        .unwrap_or_else(|| "ready".to_string());

                    let file = VgNsFileV2 {
                        context: vg_context(),
                        id: core_alias::format_alias(&name_for_file, &branch_for_file),
                        record_type: vec!["f:VirtualGraphDatabase".to_string(), vg_type_str],
                        name: name_for_file,
                        branch: branch_for_file,
                        config: ConfigRef { value: config },
                        dependencies: dependencies_for_file,
                        status,
                    };
                    did_update2.store(true, Ordering::SeqCst);
                    Ok(Some(file))
                })
                .await;

            if res.is_ok() && did_update.load(Ordering::SeqCst) {
                let _ = self.event_tx.send(NameServiceEvent::VgConfigPublished {
                    alias: alias_for_event,
                    vg_type: vg_type_for_event,
                    dependencies: dependencies_for_event,
                });
            }

            return res;
        }

        #[cfg(not(all(feature = "native", unix)))]
        {
            let file = VgNsFileV2 {
                context: vg_context(),
                id: core_alias::format_alias(&name, &branch),
                record_type: vec![
                    "f:VirtualGraphDatabase".to_string(),
                    vg_type.to_type_string(),
                ],
                name: name.to_string(),
                branch: branch.to_string(),
                config: ConfigRef {
                    value: config.to_string(),
                },
                dependencies: dependencies.to_vec(),
                status: "ready".to_string(),
            };
            self.write_json_atomic(&main_path, &file).await?;
            let _ = self.event_tx.send(NameServiceEvent::VgConfigPublished {
                alias: core_alias::format_alias(&name, &branch),
                vg_type,
                dependencies: dependencies.to_vec(),
            });
            Ok(())
        }
    }

    async fn publish_vg_index(
        &self,
        name: &str,
        branch: &str,
        index_addr: &str,
        index_t: i64,
    ) -> Result<()> {
        let index_path = self.index_path(name, branch);

        #[cfg(all(feature = "native", unix))]
        {
            let path = index_path.clone();
            let index_addr = index_addr.to_string();
            let name = name.to_string();
            let branch = branch.to_string();
            let index_addr_for_event = index_addr.clone();
            let alias_for_event = core_alias::format_alias(&name, &branch);
            let did_update = Arc::new(AtomicBool::new(false));
            let did_update2 = did_update.clone();

            let res = self
                .swap_json_locked::<VgIndexFileV2WithT, _>(path, move |existing| {
                    // Strictly monotonic: only update if new_t > existing_t
                    if let Some(existing_file) = &existing {
                        if index_t <= existing_file.index_t {
                            return Ok(None);
                        }
                    }

                    let file = VgIndexFileV2WithT {
                        context: vg_context(),
                        id: core_alias::format_alias(&name, &branch),
                        index: VgIndexRef {
                            ref_type: "f:Address".to_string(),
                            address: index_addr,
                        },
                        index_t,
                    };
                    did_update2.store(true, Ordering::SeqCst);
                    Ok(Some(file))
                })
                .await;

            if res.is_ok() && did_update.load(Ordering::SeqCst) {
                let _ = self.event_tx.send(NameServiceEvent::VgIndexPublished {
                    alias: alias_for_event,
                    index_address: index_addr_for_event,
                    index_t,
                });
            }

            return res;
        }

        #[cfg(not(all(feature = "native", unix)))]
        {
            let existing: Option<VgIndexFileV2WithT> = self.read_json(&index_path).await?;
            if let Some(existing_file) = &existing {
                if index_t <= existing_file.index_t {
                    return Ok(());
                }
            }

            let file = VgIndexFileV2WithT {
                context: vg_context(),
                id: core_alias::format_alias(&name, &branch),
                index: VgIndexRef {
                    ref_type: "f:Address".to_string(),
                    address: index_addr.to_string(),
                },
                index_t,
            };
            self.write_json_atomic(&index_path, &file).await?;
            let _ = self.event_tx.send(NameServiceEvent::VgIndexPublished {
                alias: core_alias::format_alias(&name, &branch),
                index_address: index_addr.to_string(),
                index_t,
            });
            Ok(())
        }
    }

    async fn publish_vg_snapshot(
        &self,
        name: &str,
        branch: &str,
        index_addr: &str,
        index_t: i64,
    ) -> Result<()> {
        let snapshots_path = self.snapshots_path(name, branch);

        #[cfg(all(feature = "native", unix))]
        {
            let path = snapshots_path.clone();
            let index_addr = index_addr.to_string();
            let name = name.to_string();
            let branch = branch.to_string();
            let index_addr_for_event = index_addr.clone();
            let alias_for_event = core_alias::format_alias(&name, &branch);
            let did_update = Arc::new(AtomicBool::new(false));
            let did_update2 = did_update.clone();

            let res = self
                .swap_json_locked::<VgSnapshotsFileV2, _>(path, move |existing| {
                    let mut file = existing.unwrap_or_else(|| VgSnapshotsFileV2 {
                        context: vg_context(),
                        id: core_alias::format_alias(&name, &branch),
                        snapshots: Vec::new(),
                    });

                    // Strictly monotonic: only append if new_t > max existing t
                    if let Some(last) = file.snapshots.last() {
                        if index_t <= last.index_t {
                            return Ok(None); // Reject: not strictly increasing
                        }
                    }

                    file.snapshots.push(VgSnapshotEntryV2 {
                        index_t,
                        index_address: index_addr,
                    });
                    did_update2.store(true, Ordering::SeqCst);
                    Ok(Some(file))
                })
                .await;

            if res.is_ok() && did_update.load(Ordering::SeqCst) {
                let _ = self.event_tx.send(NameServiceEvent::VgSnapshotPublished {
                    alias: alias_for_event,
                    index_address: index_addr_for_event,
                    index_t,
                });
            }

            return res;
        }

        #[cfg(not(all(feature = "native", unix)))]
        {
            let existing: Option<VgSnapshotsFileV2> = self.read_json(&snapshots_path).await?;
            let mut file = existing.unwrap_or_else(|| VgSnapshotsFileV2 {
                context: vg_context(),
                id: core_alias::format_alias(&name, &branch),
                snapshots: Vec::new(),
            });

            // Strictly monotonic: only append if new_t > max existing t
            if let Some(last) = file.snapshots.last() {
                if index_t <= last.index_t {
                    return Ok(()); // Reject: not strictly increasing
                }
            }

            file.snapshots.push(VgSnapshotEntryV2 {
                index_t,
                index_address: index_addr.to_string(),
            });
            self.write_json_atomic(&snapshots_path, &file).await?;
            let _ = self.event_tx.send(NameServiceEvent::VgSnapshotPublished {
                alias: core_alias::format_alias(&name, &branch),
                index_address: index_addr.to_string(),
                index_t,
            });
            Ok(())
        }
    }

    async fn lookup_vg_snapshots(&self, alias: &str) -> Result<VgSnapshotHistory> {
        let (name, branch) = parse_alias(alias)?;
        let snapshots_path = self.snapshots_path(&name, &branch);

        let file: Option<VgSnapshotsFileV2> = self.read_json(&snapshots_path).await?;

        match file {
            Some(f) => {
                let snapshots = f
                    .snapshots
                    .into_iter()
                    .map(|e| VgSnapshotEntry::new(e.index_t, e.index_address))
                    .collect();
                Ok(VgSnapshotHistory {
                    alias: core_alias::format_alias(&name, &branch),
                    snapshots,
                })
            }
            None => Ok(VgSnapshotHistory::new(core_alias::format_alias(
                &name, &branch,
            ))),
        }
    }

    async fn retract_vg(&self, name: &str, branch: &str) -> Result<()> {
        let main_path = self.ns_path(name, branch);

        #[cfg(all(feature = "native", unix))]
        {
            let path = main_path.clone();
            let alias_for_event = core_alias::format_alias(name, branch);
            let did_update = Arc::new(AtomicBool::new(false));
            let did_update2 = did_update.clone();

            let res = self
                .swap_json_locked::<VgNsFileV2, _>(path, move |existing| {
                    let mut file = match existing {
                        Some(f) => f,
                        None => return Ok(None),
                    };
                    if file.status == "retracted" {
                        return Ok(None);
                    }
                    file.status = "retracted".to_string();
                    did_update2.store(true, Ordering::SeqCst);
                    Ok(Some(file))
                })
                .await;

            if res.is_ok() && did_update.load(Ordering::SeqCst) {
                let _ = self.event_tx.send(NameServiceEvent::VgRetracted {
                    alias: alias_for_event,
                });
            }

            return res;
        }

        #[cfg(not(all(feature = "native", unix)))]
        {
            let existing: Option<VgNsFileV2> = self.read_json(&main_path).await?;
            if let Some(mut file) = existing {
                file.status = "retracted".to_string();
                self.write_json_atomic(&main_path, &file).await?;
                let _ = self.event_tx.send(NameServiceEvent::VgRetracted {
                    alias: core_alias::format_alias(&name, &branch),
                });
            }
            Ok(())
        }
    }

    async fn lookup_vg(&self, alias: &str) -> Result<Option<VgNsRecord>> {
        let (name, branch) = parse_alias(alias)?;

        // First check if it's a VG record
        if !self.is_vg_record(&name, &branch).await? {
            return Ok(None);
        }

        self.load_vg_record(&name, &branch).await
    }

    async fn lookup_any(&self, alias: &str) -> Result<NsLookupResult> {
        let (name, branch) = parse_alias(alias)?;
        let main_path = self.ns_path(&name, &branch);

        if !main_path.exists() {
            return Ok(NsLookupResult::NotFound);
        }

        // Check if it's a VG record
        if self.is_vg_record(&name, &branch).await? {
            match self.load_vg_record(&name, &branch).await? {
                Some(record) => Ok(NsLookupResult::VirtualGraph(record)),
                None => Ok(NsLookupResult::NotFound),
            }
        } else {
            // It's a ledger record
            match self.load_record(&name, &branch).await? {
                Some(record) => Ok(NsLookupResult::Ledger(record)),
                None => Ok(NsLookupResult::NotFound),
            }
        }
    }

    async fn all_vg_records(&self) -> Result<Vec<VgNsRecord>> {
        let ns_dir = self.base_path.join(NS_VERSION);

        if !ns_dir.exists() {
            return Ok(vec![]);
        }

        let mut records = Vec::new();

        // Walk the ns@v2 directory recursively
        let mut stack = vec![ns_dir];

        while let Some(current_dir) = stack.pop() {
            let mut dir_entries = tokio::fs::read_dir(&current_dir).await.map_err(|e| {
                NameServiceError::storage(format!(
                    "Failed to read directory {:?}: {}",
                    current_dir, e
                ))
            })?;

            while let Some(entry) = dir_entries.next_entry().await.map_err(|e| {
                NameServiceError::storage(format!("Failed to read directory entry: {}", e))
            })? {
                let path = entry.path();

                if path.is_dir() {
                    // Add subdirectory to stack for recursive processing
                    stack.push(path);
                } else if path.is_file() {
                    let file_name = entry.file_name().to_string_lossy().to_string();

                    // Skip index files and snapshot files, only process main .json files
                    if file_name.ends_with(".index.json") || file_name.ends_with(".snapshots.json")
                    {
                        continue;
                    }

                    if file_name.ends_with(".json") {
                        // Extract name and branch from path
                        // Path structure: ns@v2/{name}/{branch}.json or ns@v2/{name}/{subdir}/.../{branch}.json
                        let ns_dir_base = self.base_path.join(NS_VERSION);
                        if let Ok(relative_path) = path.strip_prefix(&ns_dir_base) {
                            // relative_path is like "vg-name/main.json" or "tenant/vg/main.json"
                            let parent = relative_path
                                .parent()
                                .map(|p| p.to_string_lossy().to_string())
                                .unwrap_or_default();
                            let branch = file_name.trim_end_matches(".json");

                            // Check if this is a VG record
                            if self.is_vg_record(&parent, branch).await? {
                                if let Ok(Some(record)) = self.load_vg_record(&parent, branch).await
                                {
                                    records.push(record);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(records)
    }
}

#[async_trait]
impl RefPublisher for FileNameService {
    async fn get_ref(&self, alias: &str, kind: RefKind) -> Result<Option<RefValue>> {
        let (ledger_name, branch) = parse_alias(alias)?;
        match kind {
            RefKind::CommitHead => {
                let main_path = self.ns_path(&ledger_name, &branch);
                let main_file: Option<NsFileV2> = self.read_json(&main_path).await?;
                match main_file {
                    None => Ok(None),
                    Some(f) => Ok(Some(RefValue {
                        address: f.commit.map(|c| c.id),
                        t: f.t,
                    })),
                }
            }
            RefKind::IndexHead => {
                // Check separate index file first, then fall back to main file.
                let index_path = self.index_path(&ledger_name, &branch);
                let index_file: Option<NsIndexFileV2> = self.read_json(&index_path).await?;

                if let Some(idx) = index_file {
                    return Ok(Some(RefValue {
                        address: Some(idx.index.id),
                        t: idx.index.t,
                    }));
                }

                // Fall back to main file's inline index.
                let main_path = self.ns_path(&ledger_name, &branch);
                let main_file: Option<NsFileV2> = self.read_json(&main_path).await?;
                match main_file {
                    None => Ok(None),
                    Some(f) => Ok(Some(RefValue {
                        address: f.index.as_ref().map(|i| i.id.clone()),
                        t: f.index.as_ref().map(|i| i.t).unwrap_or(0),
                    })),
                }
            }
        }
    }

    async fn compare_and_set_ref(
        &self,
        alias: &str,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> Result<CasResult> {
        let (ledger_name, branch) = parse_alias(alias)?;
        let expected_clone = expected.cloned();
        let new_clone = new.clone();
        let event_tx = self.event_tx.clone();
        let normalized_alias = core_alias::format_alias(&ledger_name, &branch);

        match kind {
            RefKind::CommitHead => {
                let path = self.ns_path(&ledger_name, &branch);
                let ledger_name_c = ledger_name.clone();
                let branch_c = branch.clone();
                let alias_c = normalized_alias.clone();

                // Use a shared cell to communicate the CAS result out of the closure.
                let result_cell = Arc::new(std::sync::Mutex::new(CasResult::Updated));
                let result_cell2 = result_cell.clone();

                self.swap_json_locked(path, move |existing: Option<NsFileV2>| {
                    let current_ref = existing.as_ref().map(|f| RefValue {
                        address: f.commit.as_ref().map(|c| c.id.clone()),
                        t: f.t,
                    });

                    // Validate expected matches current.
                    match (&expected_clone, &current_ref) {
                        (None, None) => {} // OK: creating new
                        (None, Some(actual)) => {
                            *result_cell2.lock().unwrap() = CasResult::Conflict {
                                actual: Some(actual.clone()),
                            };
                            return Ok(None); // no-op write
                        }
                        (Some(_), None) => {
                            *result_cell2.lock().unwrap() = CasResult::Conflict { actual: None };
                            return Ok(None);
                        }
                        (Some(exp), Some(actual)) => {
                            if exp.address != actual.address {
                                *result_cell2.lock().unwrap() = CasResult::Conflict {
                                    actual: Some(actual.clone()),
                                };
                                return Ok(None);
                            }
                        }
                    }

                    // Monotonic guard: CommitHead requires strict new.t > current.t
                    if let Some(ref cur) = current_ref {
                        if new_clone.t <= cur.t {
                            *result_cell2.lock().unwrap() = CasResult::Conflict {
                                actual: Some(cur.clone()),
                            };
                            return Ok(None);
                        }
                    }

                    // Apply update.
                    let mut file = existing.unwrap_or_else(|| {
                        NsFileV2 {
                            context: ns_context(),
                            id: alias_c.clone(),
                            record_type: vec![
                                "f:Database".to_string(),
                                "f:PhysicalDatabase".to_string(),
                            ],
                            ledger: LedgerRef {
                                id: ledger_name_c.clone(),
                            },
                            branch: branch_c.clone(),
                            commit: None,
                            t: 0,
                            index: None,
                            status: "ready".to_string(),
                            default_context: None,
                            // v2 extension fields
                            status_v: Some(1),
                            status_meta: None,
                            config_v: Some(0),
                            config_meta: None,
                        }
                    });

                    file.commit = new_clone
                        .address
                        .as_ref()
                        .map(|a| AddressRef { id: a.clone() });
                    file.t = new_clone.t;

                    Ok(Some(file))
                })
                .await?;

                let result = result_cell.lock().unwrap().clone();
                if result == CasResult::Updated {
                    if let Some(addr) = &new.address {
                        let _ = event_tx.send(NameServiceEvent::LedgerCommitPublished {
                            alias: core_alias::format_alias(&ledger_name, &branch),
                            commit_address: addr.clone(),
                            commit_t: new.t,
                        });
                    }
                }
                Ok(result)
            }

            RefKind::IndexHead => {
                let path = self.index_path(&ledger_name, &branch);
                let result_cell = Arc::new(std::sync::Mutex::new(CasResult::Updated));
                let result_cell2 = result_cell.clone();

                self.swap_json_locked(path, move |existing: Option<NsIndexFileV2>| {
                    let current_ref = existing.as_ref().map(|f| RefValue {
                        address: Some(f.index.id.clone()),
                        t: f.index.t,
                    });

                    // Validate expected matches current.
                    match (&expected_clone, &current_ref) {
                        (None, None) => {}
                        (None, Some(actual)) => {
                            *result_cell2.lock().unwrap() = CasResult::Conflict {
                                actual: Some(actual.clone()),
                            };
                            return Ok(None);
                        }
                        (Some(_), None) => {
                            *result_cell2.lock().unwrap() = CasResult::Conflict { actual: None };
                            return Ok(None);
                        }
                        (Some(exp), Some(actual)) => {
                            if exp.address != actual.address {
                                *result_cell2.lock().unwrap() = CasResult::Conflict {
                                    actual: Some(actual.clone()),
                                };
                                return Ok(None);
                            }
                        }
                    }

                    // Monotonic guard: IndexHead allows new.t >= current.t
                    if let Some(ref cur) = current_ref {
                        if new_clone.t < cur.t {
                            *result_cell2.lock().unwrap() = CasResult::Conflict {
                                actual: Some(cur.clone()),
                            };
                            return Ok(None);
                        }
                    }

                    // Apply update.
                    let new_addr = new_clone
                        .address
                        .as_ref()
                        .expect("IndexHead address must be Some for write");

                    let file = NsIndexFileV2 {
                        context: ns_context(),
                        index: IndexRef {
                            id: new_addr.clone(),
                            t: new_clone.t,
                        },
                    };
                    Ok(Some(file))
                })
                .await?;

                let result = result_cell.lock().unwrap().clone();
                if result == CasResult::Updated {
                    if let Some(addr) = &new.address {
                        let _ = event_tx.send(NameServiceEvent::LedgerIndexPublished {
                            alias: core_alias::format_alias(&ledger_name, &branch),
                            index_address: addr.clone(),
                            index_t: new.t,
                        });
                    }
                }
                Ok(result)
            }
        }
    }
}

#[async_trait]
impl Publication for FileNameService {
    async fn subscribe(&self, scope: crate::SubscriptionScope) -> Result<Subscription> {
        Ok(Subscription {
            scope,
            receiver: self.event_tx.subscribe(),
        })
    }

    async fn unsubscribe(&self, _scope: &crate::SubscriptionScope) -> Result<()> {
        Ok(())
    }

    async fn known_addresses(&self, alias: &str) -> Result<Vec<String>> {
        let (name, branch) = parse_alias(alias)?;
        match self.load_record(&name, &branch).await? {
            Some(record) => {
                let mut addresses = Vec::new();
                if let Some(addr) = record.commit_address {
                    addresses.push(addr);
                }
                if let Some(addr) = record.index_address {
                    addresses.push(addr);
                }
                Ok(addresses)
            }
            None => Ok(vec![]),
        }
    }
}

// ---------------------------------------------------------------------------
// V2 Extension: StatusPublisher and ConfigPublisher
// ---------------------------------------------------------------------------

#[cfg(all(feature = "native", unix))]
#[async_trait]
impl StatusPublisher for FileNameService {
    async fn get_status(&self, alias: &str) -> Result<Option<StatusValue>> {
        let (ledger_name, branch) = parse_alias(alias)?;
        let main_path = self.ns_path(&ledger_name, &branch);

        let main_file: Option<NsFileV2> = self.read_json(&main_path).await?;

        match main_file {
            None => Ok(None),
            Some(f) => {
                // Build StatusPayload from status field and status_meta
                let extra = f.status_meta.unwrap_or_default();
                let payload = StatusPayload {
                    state: f.status.clone(),
                    extra,
                };

                // status_v defaults to 1 if missing (for backward compatibility)
                let v = f.status_v.unwrap_or(1);

                Ok(Some(StatusValue { v, payload }))
            }
        }
    }

    async fn push_status(
        &self,
        alias: &str,
        expected: Option<&StatusValue>,
        new: &StatusValue,
    ) -> Result<StatusCasResult> {
        let (ledger_name, branch) = parse_alias(alias)?;
        let path = self.ns_path(&ledger_name, &branch);

        // Clone values for the closure
        let expected_clone = expected.cloned();
        let new_clone = new.clone();

        let result_cell = std::sync::Arc::new(std::sync::Mutex::new(StatusCasResult::Updated));
        let result_cell2 = result_cell.clone();

        self.swap_json_locked::<NsFileV2, _>(path, move |existing| {
            let current = match &existing {
                None => None,
                Some(f) => {
                    let extra = f.status_meta.clone().unwrap_or_default();
                    let payload = StatusPayload {
                        state: f.status.clone(),
                        extra,
                    };
                    let v = f.status_v.unwrap_or(1);
                    Some(StatusValue { v, payload })
                }
            };

            // Compare expected with current
            match (&expected_clone, &current) {
                (None, None) => {
                    *result_cell2.lock().unwrap() = StatusCasResult::Conflict { actual: None };
                    return Ok(None);
                }
                (None, Some(actual)) => {
                    *result_cell2.lock().unwrap() = StatusCasResult::Conflict {
                        actual: Some(actual.clone()),
                    };
                    return Ok(None);
                }
                (Some(_), None) => {
                    *result_cell2.lock().unwrap() = StatusCasResult::Conflict { actual: None };
                    return Ok(None);
                }
                (Some(exp), Some(actual)) => {
                    if exp.v != actual.v || exp.payload != actual.payload {
                        *result_cell2.lock().unwrap() = StatusCasResult::Conflict {
                            actual: Some(actual.clone()),
                        };
                        return Ok(None);
                    }
                }
            }

            // Monotonic guard: new.v > current.v
            let current_v = current.as_ref().map(|c| c.v).unwrap_or(0);
            if new_clone.v <= current_v {
                *result_cell2.lock().unwrap() = StatusCasResult::Conflict { actual: current };
                return Ok(None);
            }

            // Apply update
            let mut file = existing.unwrap();
            file.status = new_clone.payload.state.clone();
            file.status_v = Some(new_clone.v);
            file.status_meta = if new_clone.payload.extra.is_empty() {
                None
            } else {
                Some(new_clone.payload.extra.clone())
            };

            Ok(Some(file))
        })
        .await?;

        let result = result_cell.lock().unwrap().clone();
        Ok(result)
    }
}

#[cfg(all(feature = "native", unix))]
#[async_trait]
impl ConfigPublisher for FileNameService {
    async fn get_config(&self, alias: &str) -> Result<Option<ConfigValue>> {
        let (ledger_name, branch) = parse_alias(alias)?;
        let main_path = self.ns_path(&ledger_name, &branch);

        let main_file: Option<NsFileV2> = self.read_json(&main_path).await?;

        match main_file {
            None => Ok(None),
            Some(f) => {
                // config_v defaults based on whether default_context exists:
                // - If default_context exists but config_v is missing, treat as v=1 (legacy record with config)
                // - If neither exists, treat as v=0 (unborn)
                let v = f.config_v.unwrap_or_else(|| {
                    if f.default_context.is_some() || f.config_meta.is_some() {
                        1 // Legacy record with config data
                    } else {
                        0 // Unborn
                    }
                });

                // Build ConfigPayload if we have any config data
                let payload = if v == 0 && f.default_context.is_none() && f.config_meta.is_none() {
                    None
                } else {
                    let extra = f.config_meta.unwrap_or_default();
                    Some(ConfigPayload {
                        default_context: f.default_context.map(|c| c.id),
                        extra,
                    })
                };

                Ok(Some(ConfigValue { v, payload }))
            }
        }
    }

    async fn push_config(
        &self,
        alias: &str,
        expected: Option<&ConfigValue>,
        new: &ConfigValue,
    ) -> Result<ConfigCasResult> {
        let (ledger_name, branch) = parse_alias(alias)?;
        let path = self.ns_path(&ledger_name, &branch);

        // Clone values for the closure
        let expected_clone = expected.cloned();
        let new_clone = new.clone();

        let result_cell = std::sync::Arc::new(std::sync::Mutex::new(ConfigCasResult::Updated));
        let result_cell2 = result_cell.clone();

        self.swap_json_locked::<NsFileV2, _>(path, move |existing| {
            let current = match &existing {
                None => None,
                Some(f) => {
                    // config_v defaults based on whether default_context exists:
                    // - If default_context exists but config_v is missing, treat as v=1 (legacy record)
                    // - If neither exists, treat as v=0 (unborn)
                    let v = f.config_v.unwrap_or_else(|| {
                        if f.default_context.is_some() || f.config_meta.is_some() {
                            1 // Legacy record with config data
                        } else {
                            0 // Unborn
                        }
                    });
                    let payload =
                        if v == 0 && f.default_context.is_none() && f.config_meta.is_none() {
                            None
                        } else {
                            let extra = f.config_meta.clone().unwrap_or_default();
                            Some(ConfigPayload {
                                default_context: f.default_context.as_ref().map(|c| c.id.clone()),
                                extra,
                            })
                        };
                    Some(ConfigValue { v, payload })
                }
            };

            // Compare expected with current
            match (&expected_clone, &current) {
                (None, None) => {
                    *result_cell2.lock().unwrap() = ConfigCasResult::Conflict { actual: None };
                    return Ok(None);
                }
                (None, Some(actual)) => {
                    *result_cell2.lock().unwrap() = ConfigCasResult::Conflict {
                        actual: Some(actual.clone()),
                    };
                    return Ok(None);
                }
                (Some(_), None) => {
                    *result_cell2.lock().unwrap() = ConfigCasResult::Conflict { actual: None };
                    return Ok(None);
                }
                (Some(exp), Some(actual)) => {
                    if exp.v != actual.v || exp.payload != actual.payload {
                        *result_cell2.lock().unwrap() = ConfigCasResult::Conflict {
                            actual: Some(actual.clone()),
                        };
                        return Ok(None);
                    }
                }
            }

            // Monotonic guard: new.v > current.v
            let current_v = current.as_ref().map(|c| c.v).unwrap_or(0);
            if new_clone.v <= current_v {
                *result_cell2.lock().unwrap() = ConfigCasResult::Conflict { actual: current };
                return Ok(None);
            }

            // Apply update
            let mut file = existing.unwrap();
            file.config_v = Some(new_clone.v);

            if let Some(ref payload) = new_clone.payload {
                file.default_context = payload
                    .default_context
                    .as_ref()
                    .map(|c| AddressRef { id: c.clone() });
                file.config_meta = if payload.extra.is_empty() {
                    None
                } else {
                    Some(payload.extra.clone())
                };
            } else {
                file.default_context = None;
                file.config_meta = None;
            }

            Ok(Some(file))
        })
        .await?;

        let result = result_cell.lock().unwrap().clone();
        Ok(result)
    }
}

// Non-Unix fallback implementations
#[cfg(not(all(feature = "native", unix)))]
#[async_trait]
impl StatusPublisher for FileNameService {
    async fn get_status(&self, alias: &str) -> Result<Option<StatusValue>> {
        let (ledger_name, branch) = parse_alias(alias)?;
        let main_path = self.ns_path(&ledger_name, &branch);

        let main_file: Option<NsFileV2> = self.read_json(&main_path).await?;

        match main_file {
            None => Ok(None),
            Some(f) => {
                let extra = f.status_meta.unwrap_or_default();
                let payload = StatusPayload {
                    state: f.status.clone(),
                    extra,
                };
                let v = f.status_v.unwrap_or(1);
                Ok(Some(StatusValue { v, payload }))
            }
        }
    }

    async fn push_status(
        &self,
        _alias: &str,
        _expected: Option<&StatusValue>,
        _new: &StatusValue,
    ) -> Result<StatusCasResult> {
        // Non-Unix platforms don't support file locking for CAS
        Err(NameServiceError::storage(
            "StatusPublisher push_status requires native Unix file locking".to_string(),
        ))
    }
}

#[cfg(not(all(feature = "native", unix)))]
#[async_trait]
impl ConfigPublisher for FileNameService {
    async fn get_config(&self, alias: &str) -> Result<Option<ConfigValue>> {
        let (ledger_name, branch) = parse_alias(alias)?;
        let main_path = self.ns_path(&ledger_name, &branch);

        let main_file: Option<NsFileV2> = self.read_json(&main_path).await?;

        match main_file {
            None => Ok(None),
            Some(f) => {
                // config_v defaults based on whether default_context exists:
                // - If default_context exists but config_v is missing, treat as v=1 (legacy record)
                // - If neither exists, treat as v=0 (unborn)
                let v = f.config_v.unwrap_or_else(|| {
                    if f.default_context.is_some() || f.config_meta.is_some() {
                        1 // Legacy record with config data
                    } else {
                        0 // Unborn
                    }
                });
                let payload = if v == 0 && f.default_context.is_none() && f.config_meta.is_none() {
                    None
                } else {
                    let extra = f.config_meta.unwrap_or_default();
                    Some(ConfigPayload {
                        default_context: f.default_context.map(|c| c.id),
                        extra,
                    })
                };
                Ok(Some(ConfigValue { v, payload }))
            }
        }
    }

    async fn push_config(
        &self,
        _alias: &str,
        _expected: Option<&ConfigValue>,
        _new: &ConfigValue,
    ) -> Result<ConfigCasResult> {
        // Non-Unix platforms don't support file locking for CAS
        Err(NameServiceError::storage(
            "ConfigPublisher push_config requires native Unix file locking".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::sync::broadcast::error::TryRecvError;

    async fn setup() -> (TempDir, FileNameService) {
        let temp_dir = TempDir::new().unwrap();
        let ns = FileNameService::new(temp_dir.path());
        (temp_dir, ns)
    }

    #[tokio::test]
    async fn test_file_ns_emits_events_on_publish_commit_monotonic() {
        let (_temp, ns) = setup().await;
        let mut sub = ns
            .subscribe(crate::SubscriptionScope::alias("mydb:main"))
            .await
            .unwrap();

        ns.publish_commit("mydb:main", "commit-1", 1).await.unwrap();
        let evt = sub.receiver.recv().await.unwrap();
        assert_eq!(
            evt,
            NameServiceEvent::LedgerCommitPublished {
                alias: "mydb:main".to_string(),
                commit_address: "commit-1".to_string(),
                commit_t: 1
            }
        );

        // Lower t should not emit a new event.
        ns.publish_commit("mydb:main", "commit-old", 0)
            .await
            .unwrap();
        assert!(matches!(sub.receiver.try_recv(), Err(TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn test_file_ns_publish_commit() {
        let (_temp, ns) = setup().await;

        // First publish
        ns.publish_commit("mydb:main", "commit-1", 1).await.unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_address, Some("commit-1".to_string()));
        assert_eq!(record.commit_t, 1);

        // Higher t should update
        ns.publish_commit("mydb:main", "commit-2", 5).await.unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_address, Some("commit-2".to_string()));
        assert_eq!(record.commit_t, 5);

        // Lower t should be ignored
        ns.publish_commit("mydb:main", "commit-old", 3)
            .await
            .unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_address, Some("commit-2".to_string()));
        assert_eq!(record.commit_t, 5);
    }

    #[tokio::test]
    async fn test_file_ns_separate_index_file() {
        let (_temp, ns) = setup().await;

        // Publish commit
        ns.publish_commit("mydb:main", "commit-1", 10)
            .await
            .unwrap();

        // Publish index (written to separate file)
        ns.publish_index("mydb:main", "index-1", 5).await.unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_t, 10);
        assert_eq!(record.index_t, 5);
        assert_eq!(record.index_address, Some("index-1".to_string()));
        assert!(record.has_novelty());
    }

    #[tokio::test]
    async fn test_file_ns_index_merge_rule() {
        let (temp, ns) = setup().await;

        // Publish commit with embedded index
        ns.publish_commit("mydb:main", "commit-1", 10)
            .await
            .unwrap();

        // Manually add index to main file
        let main_path = temp.path().join("ns@v2/mydb/main.json");
        let mut content: NsFileV2 =
            serde_json::from_str(&tokio::fs::read_to_string(&main_path).await.unwrap()).unwrap();
        content.index = Some(IndexRef {
            id: "index-old".to_string(),
            t: 5,
        });
        tokio::fs::write(&main_path, serde_json::to_string_pretty(&content).unwrap())
            .await
            .unwrap();

        // Publish newer index to separate file
        ns.publish_index("mydb:main", "index-new", 8).await.unwrap();

        // Lookup should prefer the index file (8 >= 5)
        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.index_address, Some("index-new".to_string()));
        assert_eq!(record.index_t, 8);
    }

    #[tokio::test]
    async fn test_file_ns_all_records() {
        let (_temp, ns) = setup().await;

        ns.publish_commit("db1:main", "commit-1", 1).await.unwrap();
        ns.publish_commit("db2:main", "commit-2", 1).await.unwrap();
        ns.publish_commit("db3:dev", "commit-3", 1).await.unwrap();

        let records = ns.all_records().await.unwrap();
        assert_eq!(records.len(), 3);
    }

    #[tokio::test]
    async fn test_file_ns_ledger_with_slash() {
        let (_temp, ns) = setup().await;

        ns.publish_commit("tenant/customers:main", "commit-1", 1)
            .await
            .unwrap();

        let record = ns.lookup("tenant/customers:main").await.unwrap().unwrap();
        assert_eq!(record.alias, "tenant/customers");
        assert_eq!(record.branch, "main");
    }

    // ========== Virtual Graph Tests ==========

    #[tokio::test]
    async fn test_vg_publish_and_lookup() {
        let (_temp, ns) = setup().await;

        let config = r#"{"k1":1.2,"b":0.75}"#;
        let deps = vec!["source-ledger:main".to_string()];

        ns.publish_vg("my-search", "main", VgType::Bm25, config, &deps)
            .await
            .unwrap();

        let record = ns.lookup_vg("my-search:main").await.unwrap().unwrap();
        assert_eq!(record.name, "my-search");
        assert_eq!(record.branch, "main");
        assert_eq!(record.vg_type, VgType::Bm25);
        assert_eq!(record.config, config);
        assert_eq!(record.dependencies, deps);
        assert_eq!(record.index_address, None);
        assert_eq!(record.index_t, 0);
        assert!(!record.retracted);
    }

    #[tokio::test]
    async fn test_vg_publish_index_merge() {
        let (_temp, ns) = setup().await;

        let config = r#"{"k1":1.2}"#;
        let deps = vec!["source:main".to_string()];

        // Publish VG config
        ns.publish_vg("my-vg", "main", VgType::Bm25, config, &deps)
            .await
            .unwrap();

        // Publish VG index
        ns.publish_vg_index("my-vg", "main", "fluree:file://index/snapshot.bin", 42)
            .await
            .unwrap();

        // Lookup should merge config + index
        let record = ns.lookup_vg("my-vg:main").await.unwrap().unwrap();
        assert_eq!(record.config, config);
        assert_eq!(
            record.index_address,
            Some("fluree:file://index/snapshot.bin".to_string())
        );
        assert_eq!(record.index_t, 42);
    }

    #[tokio::test]
    async fn test_vg_index_monotonic_update() {
        let (_temp, ns) = setup().await;

        let config = r#"{}"#;
        ns.publish_vg("vg", "main", VgType::Bm25, config, &[])
            .await
            .unwrap();

        // First index publish
        ns.publish_vg_index("vg", "main", "index-v1", 10)
            .await
            .unwrap();

        // Higher t should update
        ns.publish_vg_index("vg", "main", "index-v2", 20)
            .await
            .unwrap();

        let record = ns.lookup_vg("vg:main").await.unwrap().unwrap();
        assert_eq!(record.index_address, Some("index-v2".to_string()));
        assert_eq!(record.index_t, 20);

        // Lower t should be ignored (monotonic rule)
        ns.publish_vg_index("vg", "main", "index-old", 15)
            .await
            .unwrap();

        let record = ns.lookup_vg("vg:main").await.unwrap().unwrap();
        assert_eq!(record.index_address, Some("index-v2".to_string()));
        assert_eq!(record.index_t, 20);

        // Equal t should also be ignored
        ns.publish_vg_index("vg", "main", "index-same", 20)
            .await
            .unwrap();

        let record = ns.lookup_vg("vg:main").await.unwrap().unwrap();
        assert_eq!(record.index_address, Some("index-v2".to_string()));
    }

    #[tokio::test]
    async fn test_vg_retract() {
        let (_temp, ns) = setup().await;

        ns.publish_vg("vg", "main", VgType::Bm25, "{}", &[])
            .await
            .unwrap();

        let record = ns.lookup_vg("vg:main").await.unwrap().unwrap();
        assert!(!record.retracted);

        ns.retract_vg("vg", "main").await.unwrap();

        let record = ns.lookup_vg("vg:main").await.unwrap().unwrap();
        assert!(record.retracted);
    }

    #[tokio::test]
    async fn test_vg_lookup_any_distinguishes_types() {
        let (_temp, ns) = setup().await;

        // Create a regular ledger
        ns.publish_commit("ledger:main", "commit-1", 1)
            .await
            .unwrap();

        // Create a VG
        ns.publish_vg("vg", "main", VgType::Bm25, "{}", &[])
            .await
            .unwrap();

        // lookup_any should return correct type
        match ns.lookup_any("ledger:main").await.unwrap() {
            NsLookupResult::Ledger(r) => assert_eq!(r.alias, "ledger"),
            other => panic!("Expected Ledger, got {:?}", other),
        }

        match ns.lookup_any("vg:main").await.unwrap() {
            NsLookupResult::VirtualGraph(r) => assert_eq!(r.name, "vg"),
            other => panic!("Expected VirtualGraph, got {:?}", other),
        }

        // Non-existent should return NotFound
        match ns.lookup_any("nonexistent:main").await.unwrap() {
            NsLookupResult::NotFound => {}
            other => panic!("Expected NotFound, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_vg_lookup_returns_none_for_ledger() {
        let (_temp, ns) = setup().await;

        // Create a regular ledger
        ns.publish_commit("ledger:main", "commit-1", 1)
            .await
            .unwrap();

        // lookup_vg should return None for a ledger
        let result = ns.lookup_vg("ledger:main").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_vg_config_update_preserves_index() {
        let (_temp, ns) = setup().await;

        // Publish initial config
        ns.publish_vg("vg", "main", VgType::Bm25, r#"{"v":1}"#, &[])
            .await
            .unwrap();

        // Publish index
        ns.publish_vg_index("vg", "main", "index-1", 10)
            .await
            .unwrap();

        // Update config (should not affect index)
        ns.publish_vg("vg", "main", VgType::Bm25, r#"{"v":2}"#, &[])
            .await
            .unwrap();

        let record = ns.lookup_vg("vg:main").await.unwrap().unwrap();
        assert_eq!(record.config, r#"{"v":2}"#);
        // Index should still be present (from separate file)
        assert_eq!(record.index_address, Some("index-1".to_string()));
        assert_eq!(record.index_t, 10);
    }

    #[tokio::test]
    async fn test_vg_type_variants() {
        let (_temp, ns) = setup().await;

        // Test different VG types
        ns.publish_vg("bm25-vg", "main", VgType::Bm25, "{}", &[])
            .await
            .unwrap();
        ns.publish_vg("r2rml-vg", "main", VgType::R2rml, "{}", &[])
            .await
            .unwrap();
        ns.publish_vg("iceberg-vg", "main", VgType::Iceberg, "{}", &[])
            .await
            .unwrap();
        ns.publish_vg(
            "custom-vg",
            "main",
            VgType::Unknown("fidx:CustomType".to_string()),
            "{}",
            &[],
        )
        .await
        .unwrap();

        assert_eq!(
            ns.lookup_vg("bm25-vg:main").await.unwrap().unwrap().vg_type,
            VgType::Bm25
        );
        assert_eq!(
            ns.lookup_vg("r2rml-vg:main")
                .await
                .unwrap()
                .unwrap()
                .vg_type,
            VgType::R2rml
        );
        assert_eq!(
            ns.lookup_vg("iceberg-vg:main")
                .await
                .unwrap()
                .unwrap()
                .vg_type,
            VgType::Iceberg
        );
        assert_eq!(
            ns.lookup_vg("custom-vg:main")
                .await
                .unwrap()
                .unwrap()
                .vg_type,
            VgType::Unknown("fidx:CustomType".to_string())
        );
    }

    // ========== VG Snapshot History Tests ==========

    #[tokio::test]
    async fn test_file_vg_snapshot_publish_and_lookup() {
        let (_temp, ns) = setup().await;

        ns.publish_vg("vg", "main", VgType::Bm25, "{}", &[])
            .await
            .unwrap();

        // Publish snapshots
        ns.publish_vg_snapshot("vg", "main", "addr-t5", 5)
            .await
            .unwrap();
        ns.publish_vg_snapshot("vg", "main", "addr-t10", 10)
            .await
            .unwrap();
        ns.publish_vg_snapshot("vg", "main", "addr-t20", 20)
            .await
            .unwrap();

        // Lookup and verify
        let history = ns.lookup_vg_snapshots("vg:main").await.unwrap();
        assert_eq!(history.alias, "vg:main");
        assert_eq!(history.snapshots.len(), 3);
        assert_eq!(history.snapshots[0].index_t, 5);
        assert_eq!(history.snapshots[1].index_t, 10);
        assert_eq!(history.snapshots[2].index_t, 20);
    }

    #[tokio::test]
    async fn test_file_vg_snapshot_monotonic() {
        let (_temp, ns) = setup().await;

        ns.publish_vg("vg", "main", VgType::Bm25, "{}", &[])
            .await
            .unwrap();

        ns.publish_vg_snapshot("vg", "main", "addr-t10", 10)
            .await
            .unwrap();
        ns.publish_vg_snapshot("vg", "main", "addr-t20", 20)
            .await
            .unwrap();

        // Lower t should be ignored
        ns.publish_vg_snapshot("vg", "main", "addr-t15", 15)
            .await
            .unwrap();

        // Equal t should be ignored
        ns.publish_vg_snapshot("vg", "main", "addr-t20-dup", 20)
            .await
            .unwrap();

        let history = ns.lookup_vg_snapshots("vg:main").await.unwrap();
        assert_eq!(history.snapshots.len(), 2);
        assert_eq!(history.snapshots[0].index_t, 10);
        assert_eq!(history.snapshots[1].index_t, 20);
        assert_eq!(history.snapshots[1].index_address, "addr-t20");
    }

    #[tokio::test]
    async fn test_file_vg_snapshot_lookup_empty() {
        let (_temp, ns) = setup().await;

        // Lookup nonexistent VG snapshots returns empty history
        let history = ns.lookup_vg_snapshots("nonexistent:main").await.unwrap();
        assert_eq!(history.alias, "nonexistent:main");
        assert!(history.snapshots.is_empty());
    }

    #[tokio::test]
    async fn test_file_vg_snapshot_selection() {
        let (_temp, ns) = setup().await;

        ns.publish_vg("vg", "main", VgType::Bm25, "{}", &[])
            .await
            .unwrap();

        ns.publish_vg_snapshot("vg", "main", "addr-t5", 5)
            .await
            .unwrap();
        ns.publish_vg_snapshot("vg", "main", "addr-t10", 10)
            .await
            .unwrap();
        ns.publish_vg_snapshot("vg", "main", "addr-t20", 20)
            .await
            .unwrap();

        let history = ns.lookup_vg_snapshots("vg:main").await.unwrap();

        // Select best snapshot for as_of_t=12 should be t=10
        let snap = history.select_snapshot(12).unwrap();
        assert_eq!(snap.index_t, 10);

        // Select best snapshot for as_of_t=25 should be t=20
        let snap = history.select_snapshot(25).unwrap();
        assert_eq!(snap.index_t, 20);

        // Select best snapshot for as_of_t=3 should be None
        assert!(history.select_snapshot(3).is_none());
    }

    #[tokio::test]
    async fn test_file_vg_snapshot_file_format() {
        let (temp, ns) = setup().await;

        ns.publish_vg("vg", "main", VgType::Bm25, "{}", &[])
            .await
            .unwrap();

        ns.publish_vg_snapshot("vg", "main", "fluree:file://vg/t10.bin", 10)
            .await
            .unwrap();

        // Verify the file was created
        let snapshots_path = temp.path().join("ns@v2/vg/main.snapshots.json");
        assert!(snapshots_path.exists());

        // Read and verify JSON structure
        let content = tokio::fs::read_to_string(&snapshots_path).await.unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();

        assert!(parsed.get("@context").is_some());
        assert_eq!(parsed.get("@id").unwrap(), "vg:main");

        let snapshots = parsed.get("fidx:snapshots").unwrap().as_array().unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].get("fidx:indexT").unwrap(), 10);
        assert_eq!(
            snapshots[0].get("fidx:indexAddress").unwrap(),
            "fluree:file://vg/t10.bin"
        );
    }

    // =========================================================================
    // RefPublisher tests
    // =========================================================================

    #[tokio::test]
    async fn test_file_ref_get_ref_unknown_alias() {
        let (_dir, ns) = setup().await;
        let result = ns
            .get_ref("nonexistent:main", RefKind::CommitHead)
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_file_ref_get_ref_after_publish() {
        let (_dir, ns) = setup().await;
        ns.publish_commit("mydb:main", "commit-1", 5).await.unwrap();

        let commit = ns
            .get_ref("mydb:main", RefKind::CommitHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(commit.address, Some("commit-1".to_string()));
        assert_eq!(commit.t, 5);
    }

    #[tokio::test]
    async fn test_file_ref_cas_create_new() {
        let (_dir, ns) = setup().await;
        let new_ref = RefValue {
            address: Some("commit-1".to_string()),
            t: 1,
        };

        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, None, &new_ref)
            .await
            .unwrap();
        assert_eq!(result, CasResult::Updated);

        let current = ns
            .get_ref("mydb:main", RefKind::CommitHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.address, Some("commit-1".to_string()));
        assert_eq!(current.t, 1);
    }

    #[tokio::test]
    async fn test_file_ref_cas_conflict_already_exists() {
        let (_dir, ns) = setup().await;
        ns.publish_commit("mydb:main", "commit-1", 1).await.unwrap();

        let new_ref = RefValue {
            address: Some("commit-2".to_string()),
            t: 2,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, None, &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { actual } => {
                let a = actual.unwrap();
                assert_eq!(a.address, Some("commit-1".to_string()));
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_file_ref_cas_address_mismatch() {
        let (_dir, ns) = setup().await;
        ns.publish_commit("mydb:main", "commit-1", 1).await.unwrap();

        let expected = RefValue {
            address: Some("wrong".to_string()),
            t: 1,
        };
        let new_ref = RefValue {
            address: Some("commit-2".to_string()),
            t: 2,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { .. } => {}
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_file_ref_cas_success() {
        let (_dir, ns) = setup().await;
        ns.publish_commit("mydb:main", "commit-1", 1).await.unwrap();

        let expected = RefValue {
            address: Some("commit-1".to_string()),
            t: 1,
        };
        let new_ref = RefValue {
            address: Some("commit-2".to_string()),
            t: 2,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        assert_eq!(result, CasResult::Updated);

        let current = ns
            .get_ref("mydb:main", RefKind::CommitHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.address, Some("commit-2".to_string()));
        assert_eq!(current.t, 2);
    }

    #[tokio::test]
    async fn test_file_ref_cas_commit_strict_monotonic() {
        let (_dir, ns) = setup().await;
        ns.publish_commit("mydb:main", "commit-1", 5).await.unwrap();

        let expected = RefValue {
            address: Some("commit-1".to_string()),
            t: 5,
        };
        let new_ref = RefValue {
            address: Some("commit-2".to_string()),
            t: 5,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { .. } => {}
            _ => panic!("expected conflict for same t"),
        }
    }

    #[tokio::test]
    async fn test_file_ref_cas_index_allows_equal_t() {
        let (_dir, ns) = setup().await;
        ns.publish_commit("mydb:main", "commit-1", 5).await.unwrap();
        ns.publish_index("mydb:main", "index-1", 5).await.unwrap();

        let expected = RefValue {
            address: Some("index-1".to_string()),
            t: 5,
        };
        let new_ref = RefValue {
            address: Some("index-2".to_string()),
            t: 5,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::IndexHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        assert_eq!(result, CasResult::Updated);
    }

    #[tokio::test]
    async fn test_file_ref_fast_forward_commit() {
        let (_dir, ns) = setup().await;
        ns.publish_commit("mydb:main", "commit-1", 1).await.unwrap();

        let new_ref = RefValue {
            address: Some("commit-5".to_string()),
            t: 5,
        };
        let result = ns
            .fast_forward_commit("mydb:main", &new_ref, 3)
            .await
            .unwrap();
        assert_eq!(result, CasResult::Updated);

        let current = ns
            .get_ref("mydb:main", RefKind::CommitHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.t, 5);
    }

    #[tokio::test]
    async fn test_file_ref_fast_forward_rejected_stale() {
        let (_dir, ns) = setup().await;
        ns.publish_commit("mydb:main", "commit-1", 10)
            .await
            .unwrap();

        let new_ref = RefValue {
            address: Some("old".to_string()),
            t: 5,
        };
        let result = ns
            .fast_forward_commit("mydb:main", &new_ref, 3)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { actual } => {
                assert_eq!(actual.unwrap().t, 10);
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_file_ref_cas_index_get_ref() {
        let (_dir, ns) = setup().await;
        ns.publish_commit("mydb:main", "commit-1", 5).await.unwrap();
        ns.publish_index("mydb:main", "index-1", 3).await.unwrap();

        let index = ns
            .get_ref("mydb:main", RefKind::IndexHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(index.address, Some("index-1".to_string()));
        assert_eq!(index.t, 3);
    }

    // =========================================================================
    // StatusPublisher tests
    // =========================================================================

    #[tokio::test]
    async fn test_file_status_get_nonexistent() {
        let (_dir, ns) = setup().await;
        let result = ns.get_status("nonexistent:main").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_file_status_get_initial() {
        let (_dir, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let status = ns.get_status("mydb:main").await.unwrap().unwrap();
        assert_eq!(status.v, 1);
        assert_eq!(status.payload.state, "ready");
    }

    #[tokio::test]
    async fn test_file_status_push_update() {
        let (_dir, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let initial = ns.get_status("mydb:main").await.unwrap().unwrap();

        // Push new status
        let new_status = crate::StatusValue::new(2, crate::StatusPayload::new("indexing"));
        let result = ns
            .push_status("mydb:main", Some(&initial), &new_status)
            .await
            .unwrap();
        assert!(matches!(result, crate::StatusCasResult::Updated));

        // Verify update
        let current = ns.get_status("mydb:main").await.unwrap().unwrap();
        assert_eq!(current.v, 2);
        assert_eq!(current.payload.state, "indexing");
    }

    #[tokio::test]
    async fn test_file_status_push_conflict() {
        let (_dir, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();

        // Try to push with wrong expected value
        let wrong_expected = crate::StatusValue::new(5, crate::StatusPayload::new("wrong"));
        let new_status = crate::StatusValue::new(6, crate::StatusPayload::new("indexing"));
        let result = ns
            .push_status("mydb:main", Some(&wrong_expected), &new_status)
            .await
            .unwrap();

        match result {
            crate::StatusCasResult::Conflict { actual } => {
                let a = actual.unwrap();
                assert_eq!(a.v, 1);
                assert_eq!(a.payload.state, "ready");
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_file_retract_bumps_status_v() {
        let (_dir, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();

        // Get initial status (v=1, state="ready")
        let initial = ns.get_status("mydb:main").await.unwrap().unwrap();
        assert_eq!(initial.v, 1);
        assert_eq!(initial.payload.state, "ready");

        // Retract the ledger
        ns.retract("mydb:main").await.unwrap();

        // Verify status_v was incremented and state changed to "retracted"
        let after_retract = ns.get_status("mydb:main").await.unwrap().unwrap();
        assert_eq!(
            after_retract.v, 2,
            "status_v should be incremented on retract"
        );
        assert_eq!(after_retract.payload.state, "retracted");
    }

    // =========================================================================
    // ConfigPublisher tests
    // =========================================================================

    #[tokio::test]
    async fn test_file_config_get_nonexistent() {
        let (_dir, ns) = setup().await;
        let result = ns.get_config("nonexistent:main").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_file_config_get_unborn() {
        let (_dir, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let config = ns.get_config("mydb:main").await.unwrap().unwrap();
        assert!(config.is_unborn());
        assert_eq!(config.v, 0);
    }

    #[tokio::test]
    async fn test_file_config_push_from_unborn() {
        let (_dir, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let unborn = ns.get_config("mydb:main").await.unwrap().unwrap();
        assert!(unborn.is_unborn());

        // Push first config
        let new_config = crate::ConfigValue::new(
            1,
            Some(crate::ConfigPayload::with_default_context("fluree:ctx/v1")),
        );
        let result = ns
            .push_config("mydb:main", Some(&unborn), &new_config)
            .await
            .unwrap();
        assert!(matches!(result, crate::ConfigCasResult::Updated));

        // Verify update
        let current = ns.get_config("mydb:main").await.unwrap().unwrap();
        assert!(!current.is_unborn());
        assert_eq!(current.v, 1);
        assert_eq!(
            current.payload.as_ref().unwrap().default_context,
            Some("fluree:ctx/v1".to_string())
        );
    }

    #[tokio::test]
    async fn test_file_config_push_conflict() {
        let (_dir, ns) = setup().await;
        ns.publish_ledger_init("mydb:main").await.unwrap();

        // Try to push with wrong expected value
        let wrong_expected = crate::ConfigValue::new(5, Some(crate::ConfigPayload::new()));
        let new_config = crate::ConfigValue::new(6, Some(crate::ConfigPayload::new()));
        let result = ns
            .push_config("mydb:main", Some(&wrong_expected), &new_config)
            .await
            .unwrap();

        match result {
            crate::ConfigCasResult::Conflict { actual } => {
                let a = actual.unwrap();
                assert!(a.is_unborn());
            }
            _ => panic!("expected conflict"),
        }
    }
}
