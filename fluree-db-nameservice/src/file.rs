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
    parse_address, AdminPublisher, CasResult, ConfigCasResult, ConfigPayload, ConfigPublisher,
    ConfigValue, GraphSourcePublisher, GraphSourceRecord, GraphSourceType, NameService,
    NameServiceError, NameServiceEvent, NsLookupResult, NsRecord, Publication, Publisher, RefKind,
    RefPublisher, RefValue, Result, StatusCasResult, StatusPayload, StatusPublisher, StatusValue,
    Subscription,
};
use async_trait::async_trait;
use fluree_db_core::alias as core_alias;
use fluree_db_core::ContentId;
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

    /// Content identifier for the head commit (CID string, e.g. "bafy...").
    /// This is the authoritative identity for the commit head pointer.
    #[serde(
        rename = "f:commitCid",
        skip_serializing_if = "Option::is_none",
        default
    )]
    commit_cid: Option<String>,

    #[serde(rename = "f:t")]
    t: i64,

    #[serde(rename = "f:ledgerIndex", skip_serializing_if = "Option::is_none")]
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

    #[serde(rename = "f:ledgerIndex")]
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
    /// Content identifier for this index root (CID string).
    #[serde(rename = "f:cid", skip_serializing_if = "Option::is_none", default)]
    cid: Option<String>,

    #[serde(rename = "f:t")]
    t: i64,
}

/// JSON structure for graph source ns@v2 config record file
///
/// Graph source records use the same ns@v2 path pattern but have different fields:
/// - `@type` includes "f:IndexSource" (or "f:MappedSource") and a source-specific type
/// - `f:graphSourceConfig` contains the graph source configuration as a JSON string
/// - `f:graphSourceDependencies` lists dependent ledger IDs
#[derive(Debug, Serialize, Deserialize)]
struct GraphSourceNsFileV2 {
    /// Context uses f: namespace
    #[serde(rename = "@context")]
    context: serde_json::Value,

    #[serde(rename = "@id")]
    id: String,

    /// Type array includes kind type and source-specific type
    #[serde(rename = "@type")]
    record_type: Vec<String>,

    /// Base name of the graph source
    #[serde(rename = "f:name")]
    name: String,

    /// Branch name
    #[serde(rename = "f:branch")]
    branch: String,

    /// Graph source configuration as JSON string
    #[serde(rename = "f:graphSourceConfig")]
    config: ConfigRef,

    /// Dependent ledger IDs
    #[serde(rename = "f:graphSourceDependencies")]
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

/// Reference to a graph source index CID
#[derive(Debug, Serialize, Deserialize)]
struct GraphSourceIndexRef {
    /// Content identifier string for the graph source index snapshot
    #[serde(rename = "f:graphSourceIndexCid")]
    cid: String,
}

/// JSON structure for graph source index record (separate from config)
///
/// Stored at `ns@v2/{graph-source-name}/{branch}.index.json` to avoid contention
/// between config updates and index updates. Uses monotonic update rule:
/// only write if new index_t > existing index_t.
#[derive(Debug, Serialize, Deserialize)]
struct GraphSourceIndexFileV2WithT {
    #[serde(rename = "@context")]
    context: serde_json::Value,

    #[serde(rename = "@id")]
    id: String,

    #[serde(rename = "f:graphSourceIndex")]
    index: GraphSourceIndexRef,

    #[serde(rename = "f:graphSourceIndexT")]
    index_t: i64,
}

const NS_VERSION: &str = "ns@v2";

/// Create the standard ns@v2 context as JSON value.
/// Uses object format with the `"f"` prefix mapping to the Fluree DB namespace.
fn ns_context() -> serde_json::Value {
    serde_json::json!({"f": fluree_vocab::fluree::DB})
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
            .truncate(true)
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
            ledger_id: core_alias::format_alias(ledger_name, branch),
            name: main.ledger.id.clone(),
            branch: main.branch,
            commit_head_id: main
                .commit_cid
                .as_deref()
                .and_then(|s| s.parse::<ContentId>().ok()),
            commit_t: main.t,
            index_head_id: main
                .index
                .as_ref()
                .and_then(|i| i.cid.as_deref())
                .and_then(|s| s.parse::<ContentId>().ok()),
            index_t: main.index.as_ref().map(|i| i.t).unwrap_or(0),
            default_context: main.default_context.map(|c| c.id),
            retracted: main.status == "retracted",
        };

        // Merge index file if it has equal or higher t (READ-TIME merge rule)
        if let Some(index_data) = index_file {
            if index_data.index.t >= record.index_t {
                record.index_head_id = index_data
                    .index
                    .cid
                    .as_deref()
                    .and_then(|s| s.parse::<ContentId>().ok());
                record.index_t = index_data.index.t;
            }
        }

        Ok(Some(record))
    }

    /// Check if a record file is a graph source record (based on @type).
    /// Matches "f:IndexSource"/"f:MappedSource" compact prefixes and full IRIs.
    async fn is_graph_source_record(&self, name: &str, branch: &str) -> Result<bool> {
        let main_path = self.ns_path(name, branch);
        if !main_path.exists() {
            return Ok(false);
        }

        let content = tokio::fs::read_to_string(&main_path).await.map_err(|e| {
            NameServiceError::storage(format!("Failed to read {:?}: {}", main_path, e))
        })?;

        // Parse just enough to check @type
        let parsed: serde_json::Value = serde_json::from_str(&content)?;
        Ok(Self::is_graph_source_from_json(&parsed))
    }

    /// Check if parsed JSON represents a graph source record (exact match).
    /// Matches `"f:"` compact prefixes and full IRIs.
    fn is_graph_source_from_json(parsed: &serde_json::Value) -> bool {
        if let Some(types) = parsed.get("@type").and_then(|t| t.as_array()) {
            for t in types {
                if let Some(s) = t.as_str() {
                    // Match on kind types (f: compact prefix and full IRIs)
                    if s == "f:IndexSource"
                        || s == "f:MappedSource"
                        || s == fluree_vocab::ns_types::INDEX_SOURCE
                        || s == fluree_vocab::ns_types::MAPPED_SOURCE
                    {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Load a graph source config record and merge with index file
    async fn load_graph_source_record(
        &self,
        name: &str,
        branch: &str,
    ) -> Result<Option<GraphSourceRecord>> {
        let main_path = self.ns_path(name, branch);
        let index_path = self.index_path(name, branch);

        // Read main record
        let main_file: Option<GraphSourceNsFileV2> = self.read_json(&main_path).await?;

        let Some(main) = main_file else {
            return Ok(None);
        };

        // Determine graph source type from @type array (exclude the kind types).
        let source_type = main
            .record_type
            .iter()
            .find(|t| {
                !matches!(
                    t.as_str(),
                    "f:IndexSource"
                        | "f:MappedSource"
                        | fluree_vocab::ns_types::INDEX_SOURCE
                        | fluree_vocab::ns_types::MAPPED_SOURCE
                )
            })
            .map(|t| GraphSourceType::from_type_string(t))
            .unwrap_or(GraphSourceType::Unknown("unknown".to_string()));

        // Convert to GraphSourceRecord
        let mut record = GraphSourceRecord {
            address: core_alias::format_alias(name, branch),
            name: main.name,
            branch: main.branch,
            source_type,
            config: main.config.value,
            dependencies: main.dependencies,
            index_id: None,
            index_t: 0,
            retracted: main.status == "retracted",
        };

        // Read and merge graph source index file (if exists)
        let index_file: Option<GraphSourceIndexFileV2WithT> = self.read_json(&index_path).await?;
        if let Some(index_data) = index_file {
            if index_data.index_t > record.index_t {
                record.index_id = index_data.index.cid.parse::<ContentId>().ok();
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
            id: record.ledger_id.clone(),
            record_type: vec!["f:LedgerSource".to_string()],
            ledger: LedgerRef {
                id: record.name.clone(),
            },
            branch: record.branch.clone(),
            commit_cid: record.commit_head_id.as_ref().map(|cid| cid.to_string()),
            t: record.commit_t,
            index: if record.index_head_id.is_some() {
                Some(IndexRef {
                    cid: record.index_head_id.as_ref().map(|cid| cid.to_string()),
                    t: record.index_t,
                })
            } else {
                None
            },
            status: if record.retracted {
                "retracted".to_string()
            } else {
                "ready".to_string()
            },
            default_context: record
                .default_context
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
    async fn lookup(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
        let (ledger_name, branch) = parse_address(ledger_id)?;
        self.load_record(&ledger_name, &branch).await
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

                // Exclude graph source records from ledger records.
                if self.is_graph_source_record(&parent, branch).await? {
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
    async fn publish_ledger_init(&self, ledger_id: &str) -> Result<()> {
        let (ledger_name, branch) = parse_address(ledger_id)?;
        let main_path = self.ns_path(&ledger_name, &branch);
        let normalized_address = core_alias::format_alias(&ledger_name, &branch);

        #[cfg(all(feature = "native", unix))]
        {
            let path = main_path.clone();
            let ledger_name_for_file = ledger_name.clone();
            let branch_for_file = branch.clone();
            let normalized_address_for_error = normalized_address.clone();

            // Use swap_json_locked to atomically check-and-create
            return self
                .swap_json_locked::<NsFileV2, _>(path, move |existing| {
                    if existing.is_some() {
                        // Record already exists (including retracted) - return error
                        return Err(NameServiceError::ledger_already_exists(
                            normalized_address_for_error,
                        ));
                    }

                    // Create minimal record with no commits
                    let file = NsFileV2 {
                        context: ns_context(),
                        id: core_alias::format_alias(&ledger_name_for_file, &branch_for_file),
                        record_type: vec!["f:LedgerSource".to_string()],
                        ledger: LedgerRef {
                            id: ledger_name_for_file.clone(),
                        },
                        branch: branch_for_file.clone(),
                        commit_cid: None,
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
                return Err(NameServiceError::ledger_already_exists(normalized_address));
            }

            let file = NsFileV2 {
                context: ns_context(),
                id: normalized_address.clone(),
                record_type: vec!["f:LedgerSource".to_string()],
                ledger: LedgerRef {
                    id: ledger_name.clone(),
                },
                branch: branch.clone(),
                commit_cid: None,
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

    async fn publish_commit(
        &self,
        ledger_id: &str,
        commit_t: i64,
        commit_id: &ContentId,
    ) -> Result<()> {
        let (ledger_name, branch) = parse_address(ledger_id)?;
        let main_path = self.ns_path(&ledger_name, &branch);

        #[cfg(all(feature = "native", unix))]
        {
            let path = main_path.clone();
            let commit_id_for_file = commit_id.clone();
            let commit_id_for_event = commit_id.clone();
            let ledger_name_for_file = ledger_name.clone();
            let branch_for_file = branch.clone();
            let address_for_event = core_alias::format_alias(&ledger_name, &branch);

            let did_update = Arc::new(AtomicBool::new(false));
            let did_update2 = did_update.clone();

            let res = self
                .swap_json_locked::<NsFileV2, _>(path, move |existing| {
                    let cid_str = Some(commit_id_for_file.to_string());

                    match existing {
                        Some(mut file) => {
                            // Strictly monotonic update
                            if commit_t > file.t {
                                file.commit_cid = cid_str;
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
                                record_type: vec!["f:LedgerSource".to_string()],
                                ledger: LedgerRef {
                                    id: ledger_name_for_file.clone(),
                                },
                                branch: branch_for_file.clone(),
                                commit_cid: cid_str,
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
                    ledger_id: address_for_event,
                    commit_id: commit_id_for_event,
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
                    file.commit_cid = Some(commit_id.to_string());
                    file.t = commit_t;
                    did_update = true;
                }
                file
            } else {
                let record = NsRecord {
                    ledger_id: core_alias::format_alias(&ledger_name, &branch),
                    name: ledger_name.clone(),
                    branch: branch.clone(),
                    commit_head_id: Some(commit_id.clone()),
                    commit_t,
                    index_head_id: None,
                    index_t: 0,
                    default_context: None,
                    retracted: false,
                };
                did_update = true;
                self.record_to_file(&record)
            };

            self.write_json_atomic(&main_path, &file).await?;
            if did_update {
                let _ = self.event_tx.send(NameServiceEvent::LedgerCommitPublished {
                    ledger_id: core_alias::format_alias(&ledger_name, &branch),
                    commit_id: commit_id.clone(),
                    commit_t,
                });
            }
            Ok(())
        }
    }

    async fn publish_index(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        let (ledger_name, branch) = parse_address(ledger_id)?;
        let index_path = self.index_path(&ledger_name, &branch);

        #[cfg(all(feature = "native", unix))]
        {
            let path = index_path.clone();
            let index_id_for_event = index_id.clone();
            let cid_str = index_id.to_string();
            let address_for_event = core_alias::format_alias(&ledger_name, &branch);
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
                            cid: Some(cid_str.clone()),
                            t: index_t,
                        },
                    };
                    did_update2.store(true, Ordering::SeqCst);
                    Ok(Some(file))
                })
                .await;

            if res.is_ok() && did_update.load(Ordering::SeqCst) {
                let _ = self.event_tx.send(NameServiceEvent::LedgerIndexPublished {
                    ledger_id: address_for_event,
                    index_id: index_id_for_event,
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
                    cid: Some(index_id.to_string()),
                    t: index_t,
                },
            };
            self.write_json_atomic(&index_path, &file).await?;
            let _ = self.event_tx.send(NameServiceEvent::LedgerIndexPublished {
                ledger_id: core_alias::format_alias(&ledger_name, &branch),
                index_id: index_id.clone(),
                index_t,
            });
            Ok(())
        }
    }

    async fn retract(&self, ledger_id: &str) -> Result<()> {
        let (ledger_name, branch) = parse_address(ledger_id)?;
        let main_path = self.ns_path(&ledger_name, &branch);

        #[cfg(all(feature = "native", unix))]
        {
            let path = main_path.clone();
            let address_for_event = core_alias::format_alias(&ledger_name, &branch);
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
                    ledger_id: address_for_event,
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
                        ledger_id: core_alias::format_alias(&ledger_name, &branch),
                    });
                }
            }
            Ok(())
        }
    }

    fn publishing_address(&self, ledger_id: &str) -> Option<String> {
        // File nameservice returns the ledger ID as the publishing address
        Some(core_alias::normalize_alias(ledger_id).unwrap_or_else(|_| ledger_id.to_string()))
    }
}

#[async_trait]
impl AdminPublisher for FileNameService {
    async fn publish_index_allow_equal(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        let (ledger_name, branch) = parse_address(ledger_id)?;
        let index_path = self.index_path(&ledger_name, &branch);
        let address_for_event = core_alias::format_alias(&ledger_name, &branch);

        #[cfg(all(feature = "native", unix))]
        {
            let index_id_for_event = index_id.clone();
            let cid_str = index_id.to_string();
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
                        Ok(Some(NsIndexFileV2 {
                            context: ns_context(),
                            index: IndexRef {
                                cid: Some(cid_str.clone()),
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
                    ledger_id: address_for_event,
                    index_id: index_id_for_event,
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
                        cid: Some(index_id.to_string()),
                        t: index_t,
                    },
                };
                self.write_json_atomic(&index_path, &file).await?;

                let _ = self.event_tx.send(NameServiceEvent::LedgerIndexPublished {
                    ledger_id: address_for_event,
                    index_id: index_id.clone(),
                    index_t,
                });
            }

            Ok(())
        }
    }
}

#[async_trait]
impl GraphSourcePublisher for FileNameService {
    async fn publish_graph_source(
        &self,
        name: &str,
        branch: &str,
        source_type: GraphSourceType,
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
            let source_type_for_event = source_type.clone();
            let kind_type_str = match source_type.kind() {
                crate::GraphSourceKind::Index => "f:IndexSource".to_string(),
                crate::GraphSourceKind::Mapped => "f:MappedSource".to_string(),
                crate::GraphSourceKind::Ledger => "f:LedgerSource".to_string(),
            };
            let source_type_str = source_type.to_type_string();
            let name_for_file = name.clone();
            let branch_for_file = branch.clone();
            let address_for_event = core_alias::format_alias(&name, &branch);

            let did_update = Arc::new(AtomicBool::new(false));
            let did_update2 = did_update.clone();

            let res = self
                .swap_json_locked::<GraphSourceNsFileV2, _>(path, move |existing| {
                    // For graph source config, we always update (config changes are allowed)
                    // Only preserve retracted status if already set
                    let status = existing
                        .as_ref()
                        .map(|f| f.status.clone())
                        .filter(|s| s == "retracted")
                        .unwrap_or_else(|| "ready".to_string());

                    let file = GraphSourceNsFileV2 {
                        context: ns_context(),
                        id: core_alias::format_alias(&name_for_file, &branch_for_file),
                        record_type: vec![kind_type_str, source_type_str],
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
                let _ = self
                    .event_tx
                    .send(NameServiceEvent::GraphSourceConfigPublished {
                        address: address_for_event,
                        source_type: source_type_for_event,
                        dependencies: dependencies_for_event,
                    });
            }

            return res;
        }

        #[cfg(not(all(feature = "native", unix)))]
        {
            let kind_type_str = match source_type.kind() {
                crate::GraphSourceKind::Index => "f:IndexSource".to_string(),
                crate::GraphSourceKind::Mapped => "f:MappedSource".to_string(),
                crate::GraphSourceKind::Ledger => "f:LedgerSource".to_string(),
            };
            let file = GraphSourceNsFileV2 {
                context: ns_context(),
                id: core_alias::format_alias(&name, &branch),
                record_type: vec![kind_type_str, source_type.to_type_string()],
                name: name.to_string(),
                branch: branch.to_string(),
                config: ConfigRef {
                    value: config.to_string(),
                },
                dependencies: dependencies.to_vec(),
                status: "ready".to_string(),
            };
            self.write_json_atomic(&main_path, &file).await?;
            let _ = self
                .event_tx
                .send(NameServiceEvent::GraphSourceConfigPublished {
                    address: core_alias::format_alias(&name, &branch),
                    source_type,
                    dependencies: dependencies.to_vec(),
                });
            Ok(())
        }
    }

    async fn publish_graph_source_index(
        &self,
        name: &str,
        branch: &str,
        index_id: &ContentId,
        index_t: i64,
    ) -> Result<()> {
        let index_path = self.index_path(name, branch);

        #[cfg(all(feature = "native", unix))]
        {
            let path = index_path.clone();
            let cid_str = index_id.to_string();
            let index_id_for_event = index_id.clone();
            let name = name.to_string();
            let branch = branch.to_string();
            let address_for_event = core_alias::format_alias(&name, &branch);
            let did_update = Arc::new(AtomicBool::new(false));
            let did_update2 = did_update.clone();

            let res = self
                .swap_json_locked::<GraphSourceIndexFileV2WithT, _>(path, move |existing| {
                    // Strictly monotonic: only update if new_t > existing_t
                    if let Some(existing_file) = &existing {
                        if index_t <= existing_file.index_t {
                            return Ok(None);
                        }
                    }

                    let file = GraphSourceIndexFileV2WithT {
                        context: ns_context(),
                        id: core_alias::format_alias(&name, &branch),
                        index: GraphSourceIndexRef { cid: cid_str },
                        index_t,
                    };
                    did_update2.store(true, Ordering::SeqCst);
                    Ok(Some(file))
                })
                .await;

            if res.is_ok() && did_update.load(Ordering::SeqCst) {
                let _ = self
                    .event_tx
                    .send(NameServiceEvent::GraphSourceIndexPublished {
                        address: address_for_event,
                        index_id: index_id_for_event,
                        index_t,
                    });
            }

            return res;
        }

        #[cfg(not(all(feature = "native", unix)))]
        {
            let existing: Option<GraphSourceIndexFileV2WithT> = self.read_json(&index_path).await?;
            if let Some(existing_file) = &existing {
                if index_t <= existing_file.index_t {
                    return Ok(());
                }
            }

            let file = GraphSourceIndexFileV2WithT {
                context: ns_context(),
                id: core_alias::format_alias(name, branch),
                index: GraphSourceIndexRef {
                    cid: index_id.to_string(),
                },
                index_t,
            };
            self.write_json_atomic(&index_path, &file).await?;
            let _ = self
                .event_tx
                .send(NameServiceEvent::GraphSourceIndexPublished {
                    address: core_alias::format_alias(name, branch),
                    index_id: index_id.clone(),
                    index_t,
                });
            Ok(())
        }
    }

    async fn retract_graph_source(&self, name: &str, branch: &str) -> Result<()> {
        let main_path = self.ns_path(name, branch);

        #[cfg(all(feature = "native", unix))]
        {
            let path = main_path.clone();
            let address_for_event = core_alias::format_alias(name, branch);
            let did_update = Arc::new(AtomicBool::new(false));
            let did_update2 = did_update.clone();

            let res = self
                .swap_json_locked::<GraphSourceNsFileV2, _>(path, move |existing| {
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
                let _ = self.event_tx.send(NameServiceEvent::GraphSourceRetracted {
                    address: address_for_event,
                });
            }

            return res;
        }

        #[cfg(not(all(feature = "native", unix)))]
        {
            let existing: Option<GraphSourceNsFileV2> = self.read_json(&main_path).await?;
            if let Some(mut file) = existing {
                file.status = "retracted".to_string();
                self.write_json_atomic(&main_path, &file).await?;
                let _ = self.event_tx.send(NameServiceEvent::GraphSourceRetracted {
                    address: core_alias::format_alias(&name, &branch),
                });
            }
            Ok(())
        }
    }

    async fn lookup_graph_source(&self, address: &str) -> Result<Option<GraphSourceRecord>> {
        let (name, branch) = parse_address(address)?;

        // First check if it's a graph source record
        if !self.is_graph_source_record(&name, &branch).await? {
            return Ok(None);
        }

        self.load_graph_source_record(&name, &branch).await
    }

    async fn lookup_any(&self, address: &str) -> Result<NsLookupResult> {
        let (name, branch) = parse_address(address)?;
        let main_path = self.ns_path(&name, &branch);

        if !main_path.exists() {
            return Ok(NsLookupResult::NotFound);
        }

        // Check if it's a graph source record
        if self.is_graph_source_record(&name, &branch).await? {
            match self.load_graph_source_record(&name, &branch).await? {
                Some(record) => Ok(NsLookupResult::GraphSource(record)),
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

    async fn all_graph_source_records(&self) -> Result<Vec<GraphSourceRecord>> {
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
                            // relative_path is like "gs-name/main.json" or "tenant/gs/main.json"
                            let parent = relative_path
                                .parent()
                                .map(|p| p.to_string_lossy().to_string())
                                .unwrap_or_default();
                            let branch = file_name.trim_end_matches(".json");

                            // Check if this is a graph source record
                            if self.is_graph_source_record(&parent, branch).await? {
                                if let Ok(Some(record)) =
                                    self.load_graph_source_record(&parent, branch).await
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
    async fn get_ref(&self, ledger_id: &str, kind: RefKind) -> Result<Option<RefValue>> {
        let (ledger_name, branch) = parse_address(ledger_id)?;
        match kind {
            RefKind::CommitHead => {
                let main_path = self.ns_path(&ledger_name, &branch);
                let main_file: Option<NsFileV2> = self.read_json(&main_path).await?;
                match main_file {
                    None => Ok(None),
                    Some(f) => Ok(Some(RefValue {
                        id: f
                            .commit_cid
                            .as_deref()
                            .and_then(|s| s.parse::<ContentId>().ok()),
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
                        id: idx
                            .index
                            .cid
                            .as_deref()
                            .and_then(|s| s.parse::<ContentId>().ok()),
                        t: idx.index.t,
                    }));
                }

                // Fall back to main file's inline index.
                let main_path = self.ns_path(&ledger_name, &branch);
                let main_file: Option<NsFileV2> = self.read_json(&main_path).await?;
                match main_file {
                    None => Ok(None),
                    Some(f) => Ok(Some(RefValue {
                        id: f
                            .index
                            .as_ref()
                            .and_then(|i| i.cid.as_deref())
                            .and_then(|s| s.parse::<ContentId>().ok()),
                        t: f.index.as_ref().map(|i| i.t).unwrap_or(0),
                    })),
                }
            }
        }
    }

    async fn compare_and_set_ref(
        &self,
        ledger_id: &str,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> Result<CasResult> {
        let (ledger_name, branch) = parse_address(ledger_id)?;
        let expected_clone = expected.cloned();
        let new_clone = new.clone();
        let event_tx = self.event_tx.clone();
        let normalized_address = core_alias::format_alias(&ledger_name, &branch);

        match kind {
            RefKind::CommitHead => {
                let path = self.ns_path(&ledger_name, &branch);
                let ledger_name_c = ledger_name.clone();
                let branch_c = branch.clone();
                let address_c = normalized_address.clone();

                // Use a shared cell to communicate the CAS result out of the closure.
                let result_cell = Arc::new(std::sync::Mutex::new(CasResult::Updated));
                let result_cell2 = result_cell.clone();

                self.swap_json_locked(path, move |existing: Option<NsFileV2>| {
                    let current_ref = existing.as_ref().map(|f| RefValue {
                        id: f
                            .commit_cid
                            .as_deref()
                            .and_then(|s| s.parse::<ContentId>().ok()),
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
                            // Compare on ContentId identity.
                            let identity_matches = match (&exp.id, &actual.id) {
                                (Some(a), Some(b)) => a == b,
                                (None, None) => true,
                                _ => false,
                            };
                            if !identity_matches {
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
                            id: address_c.clone(),
                            record_type: vec!["f:LedgerSource".to_string()],
                            ledger: LedgerRef {
                                id: ledger_name_c.clone(),
                            },
                            branch: branch_c.clone(),
                            commit_cid: None,
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

                    // CID goes into the commit_cid field.
                    file.commit_cid = new_clone.id.as_ref().map(|cid| cid.to_string());
                    file.t = new_clone.t;

                    Ok(Some(file))
                })
                .await?;

                let result = result_cell.lock().unwrap().clone();
                if result == CasResult::Updated {
                    if let Some(ref cid) = new.id {
                        let _ = event_tx.send(NameServiceEvent::LedgerCommitPublished {
                            ledger_id: core_alias::format_alias(&ledger_name, &branch),
                            commit_id: cid.clone(),
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
                        id: f
                            .index
                            .cid
                            .as_deref()
                            .and_then(|s| s.parse::<ContentId>().ok()),
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
                            // Compare on ContentId identity.
                            let identity_matches = match (&exp.id, &actual.id) {
                                (Some(a), Some(b)) => a == b,
                                (None, None) => true,
                                _ => false,
                            };
                            if !identity_matches {
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

                    // Apply update: CID goes into IndexRef.cid.
                    let file = NsIndexFileV2 {
                        context: ns_context(),
                        index: IndexRef {
                            cid: new_clone.id.as_ref().map(|cid| cid.to_string()),
                            t: new_clone.t,
                        },
                    };
                    Ok(Some(file))
                })
                .await?;

                let result = result_cell.lock().unwrap().clone();
                if result == CasResult::Updated {
                    if let Some(ref cid) = new.id {
                        let _ = event_tx.send(NameServiceEvent::LedgerIndexPublished {
                            ledger_id: core_alias::format_alias(&ledger_name, &branch),
                            index_id: cid.clone(),
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

    async fn known_addresses(&self, _ledger_id: &str) -> Result<Vec<String>> {
        // CID-only nameservice no longer stores storage addresses.
        Ok(vec![])
    }
}

// ---------------------------------------------------------------------------
// V2 Extension: StatusPublisher and ConfigPublisher
// ---------------------------------------------------------------------------

#[cfg(all(feature = "native", unix))]
#[async_trait]
impl StatusPublisher for FileNameService {
    async fn get_status(&self, ledger_id: &str) -> Result<Option<StatusValue>> {
        let (ledger_name, branch) = parse_address(ledger_id)?;
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
        ledger_id: &str,
        expected: Option<&StatusValue>,
        new: &StatusValue,
    ) -> Result<StatusCasResult> {
        let (ledger_name, branch) = parse_address(ledger_id)?;
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
    async fn get_config(&self, ledger_id: &str) -> Result<Option<ConfigValue>> {
        let (ledger_name, branch) = parse_address(ledger_id)?;
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
        ledger_id: &str,
        expected: Option<&ConfigValue>,
        new: &ConfigValue,
    ) -> Result<ConfigCasResult> {
        let (ledger_name, branch) = parse_address(ledger_id)?;
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
    async fn get_status(&self, ledger_id: &str) -> Result<Option<StatusValue>> {
        let (ledger_name, branch) = parse_address(ledger_id)?;
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
        _ledger_id: &str,
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
    async fn get_config(&self, ledger_id: &str) -> Result<Option<ConfigValue>> {
        let (ledger_name, branch) = parse_address(ledger_id)?;
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
        _ledger_id: &str,
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
    use fluree_db_core::storage::ContentKind;
    use tempfile::TempDir;
    use tokio::sync::broadcast::error::TryRecvError;

    /// Create a test ContentId from a label string (deterministic, reproducible).
    fn test_cid(label: &str) -> ContentId {
        ContentId::new(ContentKind::Commit, label.as_bytes())
    }

    async fn setup() -> (TempDir, FileNameService) {
        let temp_dir = TempDir::new().unwrap();
        let ns = FileNameService::new(temp_dir.path());
        (temp_dir, ns)
    }

    #[tokio::test]
    async fn test_file_ns_emits_events_on_publish_commit_monotonic() {
        let (_temp, ns) = setup().await;
        let mut sub = ns
            .subscribe(crate::SubscriptionScope::address("mydb:main"))
            .await
            .unwrap();

        let cid1 = test_cid("commit-1");
        ns.publish_commit("mydb:main", 1, &cid1).await.unwrap();
        let evt = sub.receiver.recv().await.unwrap();
        assert_eq!(
            evt,
            NameServiceEvent::LedgerCommitPublished {
                ledger_id: "mydb:main".to_string(),
                commit_id: cid1.clone(),
                commit_t: 1
            }
        );

        // Lower t should not emit a new event.
        let cid_old = test_cid("commit-old");
        ns.publish_commit("mydb:main", 0, &cid_old).await.unwrap();
        assert!(matches!(sub.receiver.try_recv(), Err(TryRecvError::Empty)));
    }

    #[tokio::test]
    async fn test_file_ns_publish_commit() {
        let (_temp, ns) = setup().await;

        let cid1 = test_cid("commit-1");
        let cid2 = test_cid("commit-2");
        let cid_old = test_cid("commit-old");

        // First publish
        ns.publish_commit("mydb:main", 1, &cid1).await.unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_head_id, Some(cid1.clone()));
        assert_eq!(record.commit_t, 1);

        // Higher t should update
        ns.publish_commit("mydb:main", 5, &cid2).await.unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_head_id, Some(cid2.clone()));
        assert_eq!(record.commit_t, 5);

        // Lower t should be ignored
        ns.publish_commit("mydb:main", 3, &cid_old).await.unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_head_id, Some(cid2.clone()));
        assert_eq!(record.commit_t, 5);
    }

    #[tokio::test]
    async fn test_file_ns_separate_index_file() {
        let (_temp, ns) = setup().await;

        let commit_cid = test_cid("commit-1");
        let index_cid = ContentId::new(ContentKind::IndexRoot, b"index-1");

        // Publish commit
        ns.publish_commit("mydb:main", 10, &commit_cid)
            .await
            .unwrap();

        // Publish index (written to separate file)
        ns.publish_index("mydb:main", 5, &index_cid).await.unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_t, 10);
        assert_eq!(record.index_t, 5);
        assert_eq!(record.index_head_id, Some(index_cid));
        assert!(record.has_novelty());
    }

    #[tokio::test]
    async fn test_file_ns_index_merge_rule() {
        let (temp, ns) = setup().await;

        let commit_cid = test_cid("commit-1");
        let index_new_cid = ContentId::new(ContentKind::IndexRoot, b"index-new");

        // Publish commit with embedded index
        ns.publish_commit("mydb:main", 10, &commit_cid)
            .await
            .unwrap();

        // Manually add index to main file
        let main_path = temp.path().join("ns@v2/mydb/main.json");
        let index_old_cid = ContentId::new(ContentKind::IndexRoot, b"index-old");
        let mut content: NsFileV2 =
            serde_json::from_str(&tokio::fs::read_to_string(&main_path).await.unwrap()).unwrap();
        content.index = Some(IndexRef {
            cid: Some(index_old_cid.to_string()),
            t: 5,
        });
        tokio::fs::write(&main_path, serde_json::to_string_pretty(&content).unwrap())
            .await
            .unwrap();

        // Publish newer index to separate file
        ns.publish_index("mydb:main", 8, &index_new_cid)
            .await
            .unwrap();

        // Lookup should prefer the index file (8 >= 5)
        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.index_head_id, Some(index_new_cid));
        assert_eq!(record.index_t, 8);
    }

    #[tokio::test]
    async fn test_file_ns_all_records() {
        let (_temp, ns) = setup().await;

        ns.publish_commit("db1:main", 1, &test_cid("commit-1"))
            .await
            .unwrap();
        ns.publish_commit("db2:main", 1, &test_cid("commit-2"))
            .await
            .unwrap();
        ns.publish_commit("db3:dev", 1, &test_cid("commit-3"))
            .await
            .unwrap();

        let records = ns.all_records().await.unwrap();
        assert_eq!(records.len(), 3);
    }

    #[tokio::test]
    async fn test_file_ns_ledger_with_slash() {
        let (_temp, ns) = setup().await;

        ns.publish_commit("tenant/customers:main", 1, &test_cid("commit-1"))
            .await
            .unwrap();

        let record = ns.lookup("tenant/customers:main").await.unwrap().unwrap();
        assert_eq!(record.name, "tenant/customers");
        assert_eq!(record.branch, "main");
    }

    // ========== Graph Source Tests ==========

    #[tokio::test]
    async fn test_graph_source_publish_and_lookup() {
        let (_temp, ns) = setup().await;

        let config = r#"{"k1":1.2,"b":0.75}"#;
        let deps = vec!["source-ledger:main".to_string()];

        ns.publish_graph_source("my-search", "main", GraphSourceType::Bm25, config, &deps)
            .await
            .unwrap();

        let record = ns
            .lookup_graph_source("my-search:main")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.name, "my-search");
        assert_eq!(record.branch, "main");
        assert_eq!(record.source_type, GraphSourceType::Bm25);
        assert_eq!(record.config, config);
        assert_eq!(record.dependencies, deps);
        assert_eq!(record.index_id, None);
        assert_eq!(record.index_t, 0);
        assert!(!record.retracted);
    }

    #[tokio::test]
    async fn test_graph_source_publish_index_merge() {
        let (_temp, ns) = setup().await;

        let config = r#"{"k1":1.2}"#;
        let deps = vec!["source:main".to_string()];

        // Publish graph source config
        ns.publish_graph_source("my-gs", "main", GraphSourceType::Bm25, config, &deps)
            .await
            .unwrap();

        // Publish graph source index
        let gs_index_cid = ContentId::new(ContentKind::IndexRoot, b"gs-index-1");
        ns.publish_graph_source_index("my-gs", "main", &gs_index_cid, 42)
            .await
            .unwrap();

        // Lookup should merge config + index
        let record = ns.lookup_graph_source("my-gs:main").await.unwrap().unwrap();
        assert_eq!(record.config, config);
        assert_eq!(record.index_id, Some(gs_index_cid));
        assert_eq!(record.index_t, 42);
    }

    #[tokio::test]
    async fn test_graph_source_index_monotonic_update() {
        let (_temp, ns) = setup().await;

        let config = r#"{}"#;
        ns.publish_graph_source("gs", "main", GraphSourceType::Bm25, config, &[])
            .await
            .unwrap();

        // First index publish
        let gs_cid_v1 = ContentId::new(ContentKind::IndexRoot, b"index-v1");
        let gs_cid_v2 = ContentId::new(ContentKind::IndexRoot, b"index-v2");
        let gs_cid_old = ContentId::new(ContentKind::IndexRoot, b"index-old");
        let gs_cid_same = ContentId::new(ContentKind::IndexRoot, b"index-same");

        ns.publish_graph_source_index("gs", "main", &gs_cid_v1, 10)
            .await
            .unwrap();

        // Higher t should update
        ns.publish_graph_source_index("gs", "main", &gs_cid_v2, 20)
            .await
            .unwrap();

        let record = ns.lookup_graph_source("gs:main").await.unwrap().unwrap();
        assert_eq!(record.index_id, Some(gs_cid_v2.clone()));
        assert_eq!(record.index_t, 20);

        // Lower t should be ignored (monotonic rule)
        ns.publish_graph_source_index("gs", "main", &gs_cid_old, 15)
            .await
            .unwrap();

        let record = ns.lookup_graph_source("gs:main").await.unwrap().unwrap();
        assert_eq!(record.index_id, Some(gs_cid_v2.clone()));
        assert_eq!(record.index_t, 20);

        // Equal t should also be ignored
        ns.publish_graph_source_index("gs", "main", &gs_cid_same, 20)
            .await
            .unwrap();

        let record = ns.lookup_graph_source("gs:main").await.unwrap().unwrap();
        assert_eq!(record.index_id, Some(gs_cid_v2));
    }

    #[tokio::test]
    async fn test_graph_source_retract() {
        let (_temp, ns) = setup().await;

        ns.publish_graph_source("gs", "main", GraphSourceType::Bm25, "{}", &[])
            .await
            .unwrap();

        let record = ns.lookup_graph_source("gs:main").await.unwrap().unwrap();
        assert!(!record.retracted);

        ns.retract_graph_source("gs", "main").await.unwrap();

        let record = ns.lookup_graph_source("gs:main").await.unwrap().unwrap();
        assert!(record.retracted);
    }

    #[tokio::test]
    async fn test_graph_source_lookup_any_distinguishes_types() {
        let (_temp, ns) = setup().await;

        // Create a regular ledger
        ns.publish_commit("ledger:main", 1, &test_cid("commit-1"))
            .await
            .unwrap();

        // Create a graph source
        ns.publish_graph_source("gs", "main", GraphSourceType::Bm25, "{}", &[])
            .await
            .unwrap();

        // lookup_any should return correct type
        match ns.lookup_any("ledger:main").await.unwrap() {
            NsLookupResult::Ledger(r) => assert_eq!(r.name, "ledger"),
            other => panic!("Expected Ledger, got {:?}", other),
        }

        match ns.lookup_any("gs:main").await.unwrap() {
            NsLookupResult::GraphSource(r) => assert_eq!(r.name, "gs"),
            other => panic!("Expected GraphSource, got {:?}", other),
        }

        // Non-existent should return NotFound
        match ns.lookup_any("nonexistent:main").await.unwrap() {
            NsLookupResult::NotFound => {}
            other => panic!("Expected NotFound, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_graph_source_lookup_returns_none_for_ledger() {
        let (_temp, ns) = setup().await;

        // Create a regular ledger
        ns.publish_commit("ledger:main", 1, &test_cid("commit-1"))
            .await
            .unwrap();

        // lookup_graph_source should return None for a ledger
        let result = ns.lookup_graph_source("ledger:main").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_graph_source_config_update_preserves_index() {
        let (_temp, ns) = setup().await;

        // Publish initial config
        ns.publish_graph_source("gs", "main", GraphSourceType::Bm25, r#"{"v":1}"#, &[])
            .await
            .unwrap();

        // Publish index
        let gs_idx_cid = ContentId::new(ContentKind::IndexRoot, b"gs-index-1");
        ns.publish_graph_source_index("gs", "main", &gs_idx_cid, 10)
            .await
            .unwrap();

        // Update config (should not affect index)
        ns.publish_graph_source("gs", "main", GraphSourceType::Bm25, r#"{"v":2}"#, &[])
            .await
            .unwrap();

        let record = ns.lookup_graph_source("gs:main").await.unwrap().unwrap();
        assert_eq!(record.config, r#"{"v":2}"#);
        // Index should still be present (from separate file)
        assert_eq!(record.index_id, Some(gs_idx_cid));
        assert_eq!(record.index_t, 10);
    }

    #[tokio::test]
    async fn test_graph_source_type_variants() {
        let (_temp, ns) = setup().await;

        // Test different graph source types
        ns.publish_graph_source("bm25-gs", "main", GraphSourceType::Bm25, "{}", &[])
            .await
            .unwrap();
        ns.publish_graph_source("r2rml-gs", "main", GraphSourceType::R2rml, "{}", &[])
            .await
            .unwrap();
        ns.publish_graph_source("iceberg-gs", "main", GraphSourceType::Iceberg, "{}", &[])
            .await
            .unwrap();
        ns.publish_graph_source(
            "custom-gs",
            "main",
            GraphSourceType::Unknown("f:CustomType".to_string()),
            "{}",
            &[],
        )
        .await
        .unwrap();

        assert_eq!(
            ns.lookup_graph_source("bm25-gs:main")
                .await
                .unwrap()
                .unwrap()
                .source_type,
            GraphSourceType::Bm25
        );
        assert_eq!(
            ns.lookup_graph_source("r2rml-gs:main")
                .await
                .unwrap()
                .unwrap()
                .source_type,
            GraphSourceType::R2rml
        );
        assert_eq!(
            ns.lookup_graph_source("iceberg-gs:main")
                .await
                .unwrap()
                .unwrap()
                .source_type,
            GraphSourceType::Iceberg
        );
        assert_eq!(
            ns.lookup_graph_source("custom-gs:main")
                .await
                .unwrap()
                .unwrap()
                .source_type,
            GraphSourceType::Unknown("f:CustomType".to_string())
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
        let cid1 = test_cid("commit-1");
        ns.publish_commit("mydb:main", 5, &cid1).await.unwrap();

        let commit = ns
            .get_ref("mydb:main", RefKind::CommitHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(commit.id, Some(cid1));
        assert_eq!(commit.t, 5);
    }

    #[tokio::test]
    async fn test_file_ref_cas_create_new() {
        let (_dir, ns) = setup().await;
        let new_ref = RefValue { id: None, t: 1 };

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
        assert_eq!(current.id, None);
        assert_eq!(current.t, 1);
    }

    #[tokio::test]
    async fn test_file_ref_cas_conflict_already_exists() {
        let (_dir, ns) = setup().await;
        let cid1 = test_cid("commit-1");
        ns.publish_commit("mydb:main", 1, &cid1).await.unwrap();

        let new_ref = RefValue { id: None, t: 2 };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, None, &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { actual } => {
                let a = actual.unwrap();
                assert_eq!(a.id, Some(cid1));
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_file_ref_cas_cid_mismatch() {
        let (_dir, ns) = setup().await;
        let cid1 = test_cid("commit-1");
        ns.publish_commit("mydb:main", 1, &cid1).await.unwrap();

        let wrong_cid = test_cid("wrong");
        let expected = RefValue {
            id: Some(wrong_cid),
            t: 1,
        };
        let new_ref = RefValue {
            id: Some(test_cid("commit-2")),
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
        let cid1 = test_cid("commit-1");
        ns.publish_commit("mydb:main", 1, &cid1).await.unwrap();

        // Expected must match what's stored: id from CID
        let expected = RefValue {
            id: Some(cid1.clone()),
            t: 1,
        };
        let cid2 = test_cid("commit-2");
        let new_ref = RefValue {
            id: Some(cid2.clone()),
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
        assert_eq!(current.id, Some(cid2));
        assert_eq!(current.t, 2);
    }

    #[tokio::test]
    async fn test_file_ref_cas_commit_strict_monotonic() {
        let (_dir, ns) = setup().await;
        let cid1 = test_cid("commit-1");
        ns.publish_commit("mydb:main", 5, &cid1).await.unwrap();

        let expected = RefValue {
            id: Some(cid1),
            t: 5,
        };
        let new_ref = RefValue {
            id: Some(test_cid("commit-2")),
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
        let commit_cid = test_cid("commit-1");
        let index_cid = ContentId::new(ContentKind::IndexRoot, b"index-1");
        ns.publish_commit("mydb:main", 5, &commit_cid)
            .await
            .unwrap();
        ns.publish_index("mydb:main", 5, &index_cid).await.unwrap();

        // Expected must match stored: id from CID
        let expected = RefValue {
            id: Some(index_cid),
            t: 5,
        };
        let index_cid2 = ContentId::new(ContentKind::IndexRoot, b"index-2");
        let new_ref = RefValue {
            id: Some(index_cid2),
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
        let cid1 = test_cid("commit-1");
        ns.publish_commit("mydb:main", 1, &cid1).await.unwrap();

        let new_ref = RefValue {
            id: Some(test_cid("commit-5")),
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
        let cid1 = test_cid("commit-1");
        ns.publish_commit("mydb:main", 10, &cid1).await.unwrap();

        let new_ref = RefValue {
            id: Some(test_cid("old")),
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
        let commit_cid = test_cid("commit-1");
        let index_cid = ContentId::new(ContentKind::IndexRoot, b"index-1");
        ns.publish_commit("mydb:main", 5, &commit_cid)
            .await
            .unwrap();
        ns.publish_index("mydb:main", 3, &index_cid).await.unwrap();

        let index = ns
            .get_ref("mydb:main", RefKind::IndexHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(index.id, Some(index_cid));
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
