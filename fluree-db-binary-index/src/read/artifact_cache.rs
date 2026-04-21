use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use fluree_db_core::{ContentId, ContentStore};
use once_cell::sync::Lazy;
use parking_lot::Mutex;

use super::leaflet_cache::LeafletCache;

const CACHE_BUDGET_NUMERATOR: u64 = 9;
const CACHE_BUDGET_DENOMINATOR: u64 = 10;
const CACHE_EVICT_NUMERATOR: u64 = 8;
const CACHE_EVICT_DENOMINATOR: u64 = 10;
const DEFAULT_LAMBDA_TMP_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_LAMBDA_TMP_WARN_SLACK_BYTES: u64 = 64 * 1024 * 1024;

static CACHE_REGISTRY: Lazy<Mutex<HashMap<PathBuf, Weak<DiskArtifactCache>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Debug)]
pub(crate) struct DiskArtifactCache {
    root: PathBuf,
    budget_bytes: u64,
    state: Mutex<DiskArtifactCacheState>,
}

#[derive(Debug, Default)]
struct DiskArtifactCacheState {
    tracked_bytes: Option<u64>,
}

#[derive(Debug)]
struct CacheEntry {
    path: PathBuf,
    bytes: u64,
    modified: std::time::SystemTime,
}

fn storage_to_io_error(e: fluree_db_core::error::Error) -> io::Error {
    let kind = match &e {
        fluree_db_core::error::Error::NotFound(_) => io::ErrorKind::NotFound,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, e.to_string())
}

fn is_cache_temp_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with(".cas_") && name.ends_with(".tmp"))
}

fn is_disk_full(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::StorageFull || err.raw_os_error() == Some(28)
}

fn try_read_cached_bytes(path: &Path) -> io::Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

fn scan_cache_entries(root: &Path) -> io::Result<Vec<CacheEntry>> {
    let mut stack = vec![root.to_path_buf()];
    let mut entries = Vec::new();

    while let Some(dir) = stack.pop() {
        let read_dir = match fs::read_dir(&dir) {
            Ok(read_dir) => read_dir,
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };

        for child in read_dir {
            let child = child?;
            let path = child.path();
            let file_type = child.file_type()?;
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if !file_type.is_file() || is_cache_temp_file(&path) {
                continue;
            }

            let meta = child.metadata()?;
            entries.push(CacheEntry {
                path,
                bytes: meta.len(),
                modified: meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH),
            });
        }
    }

    Ok(entries)
}

impl DiskArtifactCache {
    pub(crate) fn for_dir(cache_dir: &Path) -> Arc<Self> {
        let root = cache_dir.to_path_buf();
        let mut registry = CACHE_REGISTRY.lock();
        if let Some(existing) = registry.get(&root).and_then(Weak::upgrade) {
            return existing;
        }

        let cache = Arc::new(Self::new(root.clone()));
        registry.insert(root, Arc::downgrade(&cache));
        cache
    }

    fn new(root: PathBuf) -> Self {
        if let Err(err) = fs::create_dir_all(&root) {
            tracing::warn!(
                cache_dir = %root.display(),
                error = %err,
                "failed to create disk artifact cache directory; cache writes disabled"
            );
            return Self {
                root,
                budget_bytes: 0,
                state: Mutex::new(DiskArtifactCacheState::default()),
            };
        }

        let available = fs2::available_space(&root).unwrap_or_else(|err| {
            tracing::warn!(
                cache_dir = %root.display(),
                error = %err,
                "failed to inspect available disk space; disk cache writes disabled"
            );
            0
        });
        let budget_bytes = match std::env::var("FLUREE_DISK_CACHE_BUDGET_BYTES") {
            Ok(val) => match val.parse::<u64>() {
                Ok(0) => {
                    tracing::info!(
                        cache_dir = %root.display(),
                        "FLUREE_DISK_CACHE_BUDGET_BYTES=0; disk cache writes disabled"
                    );
                    0
                }
                Ok(bytes) => {
                    tracing::info!(
                        cache_dir = %root.display(),
                        budget_bytes = bytes,
                        "using FLUREE_DISK_CACHE_BUDGET_BYTES override"
                    );
                    bytes
                }
                Err(err) => {
                    tracing::warn!(
                        cache_dir = %root.display(),
                        value = %val,
                        error = %err,
                        "invalid FLUREE_DISK_CACHE_BUDGET_BYTES; falling back to auto-detect"
                    );
                    available
                        .saturating_mul(CACHE_BUDGET_NUMERATOR)
                        .saturating_div(CACHE_BUDGET_DENOMINATOR)
                }
            },
            Err(_) => available
                .saturating_mul(CACHE_BUDGET_NUMERATOR)
                .saturating_div(CACHE_BUDGET_DENOMINATOR),
        };

        if available > 0
            && available
                <= DEFAULT_LAMBDA_TMP_BYTES.saturating_add(DEFAULT_LAMBDA_TMP_WARN_SLACK_BYTES)
        {
            tracing::warn!(
                cache_dir = %root.display(),
                available_tmp_bytes = available,
                cache_budget_bytes = budget_bytes,
                "disk cache is using near-default ephemeral storage; consider increasing Lambda /tmp"
            );
        }

        Self {
            root,
            budget_bytes,
            state: Mutex::new(DiskArtifactCacheState::default()),
        }
    }

    #[cfg(test)]
    fn with_budget(root: PathBuf, budget_bytes: u64) -> Self {
        fs::create_dir_all(&root).expect("create test cache dir");
        Self {
            root,
            budget_bytes,
            state: Mutex::new(DiskArtifactCacheState::default()),
        }
    }

    fn low_water_mark(&self) -> u64 {
        self.budget_bytes
            .saturating_mul(CACHE_EVICT_NUMERATOR)
            .saturating_div(CACHE_EVICT_DENOMINATOR)
    }

    fn current_bytes(&self) -> io::Result<u64> {
        let mut state = self.state.lock();
        if let Some(bytes) = state.tracked_bytes {
            return Ok(bytes);
        }
        let bytes = scan_cache_entries(&self.root)?
            .into_iter()
            .fold(0u64, |acc, entry| acc.saturating_add(entry.bytes));
        state.tracked_bytes = Some(bytes);
        Ok(bytes)
    }

    fn set_current_bytes(&self, bytes: u64) {
        self.state.lock().tracked_bytes = Some(bytes);
    }

    fn note_write(&self, bytes: u64) {
        let mut state = self.state.lock();
        let current = state.tracked_bytes.unwrap_or(0);
        state.tracked_bytes = Some(current.saturating_add(bytes));
    }

    fn evict_until(&self, target_bytes: u64) -> io::Result<()> {
        let mut entries = scan_cache_entries(&self.root)?;
        let mut current = entries
            .iter()
            .fold(0u64, |acc, entry| acc.saturating_add(entry.bytes));
        if current <= target_bytes {
            self.set_current_bytes(current);
            return Ok(());
        }

        entries.sort_by_key(|entry| entry.modified);
        for entry in entries {
            if current <= target_bytes {
                break;
            }
            match fs::remove_file(&entry.path) {
                Ok(()) => {
                    current = current.saturating_sub(entry.bytes);
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    current = current.saturating_sub(entry.bytes);
                }
                Err(err) => {
                    tracing::debug!(
                        cache_dir = %self.root.display(),
                        path = %entry.path.display(),
                        error = %err,
                        "failed to evict cache file"
                    );
                }
            }
        }

        self.set_current_bytes(current);
        Ok(())
    }

    fn ensure_capacity(&self, incoming_bytes: u64) -> io::Result<()> {
        if self.budget_bytes == 0 {
            return Ok(());
        }

        let current = self.current_bytes()?;
        if current.saturating_add(incoming_bytes) <= self.budget_bytes {
            return Ok(());
        }

        let target = self
            .low_water_mark()
            .min(self.budget_bytes.saturating_sub(incoming_bytes));
        self.evict_until(target)
    }

    fn write_atomic(target: &Path, bytes: &[u8]) -> io::Result<bool> {
        if target.exists() {
            return Ok(false);
        }

        let parent = target
            .parent()
            .ok_or_else(|| io::Error::other("cache target has no parent dir"))?;
        fs::create_dir_all(parent)?;

        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp = parent.join(format!(".cas_{}_{}.tmp", std::process::id(), seq));
        fs::write(&tmp, bytes)?;
        if let Err(_rename_err) = fs::rename(&tmp, target) {
            let _ = fs::remove_file(&tmp);
            if !target.exists() {
                return Err(io::Error::other(format!(
                    "failed to cache bytes to {:?}",
                    target
                )));
            }
            return Ok(false);
        }
        Ok(true)
    }

    pub(crate) fn best_effort_write(&self, target: &Path, bytes: &[u8]) {
        if self.budget_bytes == 0 {
            return;
        }

        if let Err(err) = self.ensure_capacity(bytes.len() as u64) {
            tracing::warn!(
                cache_dir = %self.root.display(),
                error = %err,
                "failed to enforce disk cache budget; skipping cache write"
            );
            return;
        }

        match Self::write_atomic(target, bytes) {
            Ok(true) => self.note_write(bytes.len() as u64),
            Ok(false) => {}
            Err(err) if is_disk_full(&err) => {
                if let Err(evict_err) = self.evict_until(self.low_water_mark()) {
                    tracing::warn!(
                        cache_dir = %self.root.display(),
                        error = %evict_err,
                        "failed to evict cache files after disk-full error"
                    );
                    return;
                }
                match Self::write_atomic(target, bytes) {
                    Ok(true) => self.note_write(bytes.len() as u64),
                    Ok(false) => {}
                    Err(retry_err) => tracing::warn!(
                        cache_dir = %self.root.display(),
                        target = %target.display(),
                        error = %retry_err,
                        "disk cache write failed after eviction; continuing without cache"
                    ),
                }
            }
            Err(err) => tracing::warn!(
                cache_dir = %self.root.display(),
                target = %target.display(),
                error = %err,
                "disk cache write failed; continuing without cache"
            ),
        }
    }
}

pub fn best_effort_cache_bytes_to_path(cache_dir: &Path, target: &Path, bytes: &[u8]) {
    DiskArtifactCache::for_dir(cache_dir).best_effort_write(target, bytes);
}

/// Fetch bytes for a content-addressed artifact, layering an optional
/// in-memory [`LeafletCache`] on top of the existing local-CAS / disk /
/// CAS lookup chain.
///
/// Lookup order when `leaflet_cache` is `Some`:
/// 1. In-memory `LeafletCache::LoadArtifact` slot — cheapest.
/// 2. Local CAS path (memory-mapped or direct file read).
/// 3. Disk cache under `cache_dir` (survives process restarts).
/// 4. `cs.get(id)` — the network/CAS backend.
///
/// Every intermediate miss populates the in-memory cache so that a
/// subsequent reload of the same index root pays zero S3 latency for
/// unchanged arenas and dict blobs (fluree/db-r#155 follow-up — the
/// incident report observed `cache_budget_pct = 0.12%` because this
/// helper didn't consult the cache at all).
pub async fn fetch_cached_bytes(
    cs: &dyn ContentStore,
    id: &ContentId,
    cache_dir: &Path,
    ext: &str,
    leaflet_cache: Option<&Arc<LeafletCache>>,
) -> io::Result<Vec<u8>> {
    if let Some(cache) = leaflet_cache {
        let key = LeafletCache::cid_cache_key(&id.to_bytes());
        let arc = cache
            .get_or_load_artifact(key, || async {
                fetch_cached_bytes_uncached(cs, id, cache_dir, ext).await
            })
            .await?;
        return Ok(arc.to_vec());
    }
    fetch_cached_bytes_uncached(cs, id, cache_dir, ext).await
}

/// Pre-cache implementation. Retained as its own function so the cached
/// path can use it as the `load_fn` closure.
async fn fetch_cached_bytes_uncached(
    cs: &dyn ContentStore,
    id: &ContentId,
    cache_dir: &Path,
    ext: &str,
) -> io::Result<Vec<u8>> {
    if let Some(local_path) = cs.resolve_local_path(id) {
        return match try_read_cached_bytes(&local_path)? {
            Some(bytes) => Ok(bytes),
            None => {
                tracing::debug!(
                    path = %local_path.display(),
                    "local artifact path disappeared during read; falling back to remote fetch"
                );
                let bytes = cs.get(id).await.map_err(storage_to_io_error)?;
                best_effort_cache_bytes_to_path(
                    cache_dir,
                    &cache_dir.join(format!("{}.{}", id.digest_hex(), ext)),
                    &bytes,
                );
                Ok(bytes)
            }
        };
    }
    let hash = id.digest_hex();
    let cached = cache_dir.join(format!("{}.{}", hash, ext));
    if let Some(bytes) = try_read_cached_bytes(&cached)? {
        return Ok(bytes);
    }
    let bytes = cs.get(id).await.map_err(storage_to_io_error)?;
    best_effort_cache_bytes_to_path(cache_dir, &cached, &bytes);
    Ok(bytes)
}

/// `fetch_cached_bytes` variant that stores on disk using the CID
/// address directly rather than a digest-hex-plus-extension filename.
/// See that function for the full lookup order and caching rationale.
pub async fn fetch_cached_bytes_cid(
    cs: &dyn ContentStore,
    id: &ContentId,
    cache_dir: &Path,
    leaflet_cache: Option<&Arc<LeafletCache>>,
) -> io::Result<Vec<u8>> {
    if let Some(cache) = leaflet_cache {
        let key = LeafletCache::cid_cache_key(&id.to_bytes());
        let arc = cache
            .get_or_load_artifact(key, || async {
                fetch_cached_bytes_cid_uncached(cs, id, cache_dir).await
            })
            .await?;
        return Ok(arc.to_vec());
    }
    fetch_cached_bytes_cid_uncached(cs, id, cache_dir).await
}

async fn fetch_cached_bytes_cid_uncached(
    cs: &dyn ContentStore,
    id: &ContentId,
    cache_dir: &Path,
) -> io::Result<Vec<u8>> {
    if let Some(local_path) = cs.resolve_local_path(id) {
        return match try_read_cached_bytes(&local_path)? {
            Some(bytes) => Ok(bytes),
            None => {
                tracing::debug!(
                    path = %local_path.display(),
                    "local artifact path disappeared during read; falling back to remote fetch"
                );
                let bytes = cs.get(id).await.map_err(storage_to_io_error)?;
                best_effort_cache_bytes_to_path(cache_dir, &cache_dir.join(id.to_string()), &bytes);
                Ok(bytes)
            }
        };
    }
    let cached = cache_dir.join(id.to_string());
    if let Some(bytes) = try_read_cached_bytes(&cached)? {
        return Ok(bytes);
    }
    let bytes = cs.get(id).await.map_err(storage_to_io_error)?;
    best_effort_cache_bytes_to_path(cache_dir, &cached, &bytes);
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_cache_dir(label: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "fluree-artifact-cache-test-{}-{}-{}",
            label,
            std::process::id(),
            n
        ));
        let _ = fs::remove_dir_all(&path);
        path
    }

    #[test]
    fn write_and_read_back() {
        let dir = temp_cache_dir("write-read");
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        let target = dir.join("abc123.leaf");
        let data = b"hello world";

        cache.best_effort_write(&target, data);
        assert!(target.exists());
        assert_eq!(fs::read(&target).unwrap(), data);
    }

    #[test]
    fn write_skipped_when_budget_is_zero() {
        let dir = temp_cache_dir("zero-budget");
        let cache = DiskArtifactCache::with_budget(dir.clone(), 0);
        let target = dir.join("should-not-exist.leaf");

        cache.best_effort_write(&target, b"data");
        assert!(!target.exists());
    }

    #[test]
    fn duplicate_write_is_idempotent() {
        let dir = temp_cache_dir("dup-write");
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        let target = dir.join("dup.leaf");
        let data = b"first write";

        cache.best_effort_write(&target, data);
        cache.best_effort_write(&target, b"second write attempt");
        // First write wins — content unchanged.
        assert_eq!(fs::read(&target).unwrap(), data);
    }

    #[test]
    fn tracked_bytes_updated_on_write() {
        let dir = temp_cache_dir("tracked");
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);

        cache.best_effort_write(&dir.join("a.leaf"), &[0u8; 100]);
        cache.best_effort_write(&dir.join("b.leaf"), &[0u8; 200]);

        assert_eq!(cache.current_bytes().unwrap(), 300);
    }

    #[test]
    fn eviction_removes_oldest_files() {
        let dir = temp_cache_dir("eviction");
        // Budget: 500 bytes, low water mark = 500 * 8/10 = 400.
        let cache = DiskArtifactCache::with_budget(dir.clone(), 500);

        // Write three 150-byte files (total 450, under budget).
        for name in &["old.leaf", "mid.leaf", "new.leaf"] {
            let target = dir.join(name);
            cache.best_effort_write(&target, &[0u8; 150]);
            // Ensure distinct modification times for deterministic eviction order.
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert_eq!(cache.current_bytes().unwrap(), 450);

        // Writing another 150 bytes pushes total to 600 > 500 budget.
        // ensure_capacity should evict oldest files down to low water mark (400)
        // or budget - incoming (350), whichever is lower → 350.
        cache.best_effort_write(&dir.join("trigger.leaf"), &[0u8; 150]);

        // The oldest file(s) should have been evicted.
        assert!(
            !dir.join("old.leaf").exists(),
            "oldest file should be evicted"
        );
        // The newest files + trigger should survive.
        assert!(dir.join("trigger.leaf").exists());
    }

    #[test]
    fn scan_ignores_temp_files() {
        let dir = temp_cache_dir("scan-temp");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("real.leaf"), [0u8; 10]).unwrap();
        fs::write(dir.join(".cas_123_0.tmp"), [0u8; 20]).unwrap();

        let entries = scan_cache_entries(&dir).unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].path.ends_with("real.leaf"));
    }

    #[test]
    fn scan_walks_subdirectories() {
        let dir = temp_cache_dir("scan-subdir");
        let sub = dir.join("sub");
        fs::create_dir_all(&sub).unwrap();
        fs::write(dir.join("a.leaf"), [0u8; 10]).unwrap();
        fs::write(sub.join("b.leaf"), [0u8; 20]).unwrap();

        let entries = scan_cache_entries(&dir).unwrap();
        assert_eq!(entries.len(), 2);
        let total: u64 = entries.iter().map(|e| e.bytes).sum();
        assert_eq!(total, 30);
    }

    #[test]
    fn for_dir_returns_singleton() {
        let dir = temp_cache_dir("singleton");
        let a = DiskArtifactCache::for_dir(&dir);
        let b = DiskArtifactCache::for_dir(&dir);
        assert!(Arc::ptr_eq(&a, &b), "same dir should return same Arc");
    }

    #[test]
    fn singleton_dropped_when_no_strong_refs() {
        let dir = temp_cache_dir("singleton-drop");
        let a = DiskArtifactCache::for_dir(&dir);
        let ptr1 = Arc::as_ptr(&a);
        drop(a);

        // After dropping the only strong ref, a new call should create a fresh instance.
        let b = DiskArtifactCache::for_dir(&dir);
        let ptr2 = Arc::as_ptr(&b);
        assert_ne!(ptr1, ptr2, "should be a new instance after drop");
    }

    #[test]
    fn current_bytes_scans_on_first_call() {
        let dir = temp_cache_dir("initial-scan");
        fs::create_dir_all(&dir).unwrap();
        // Pre-populate some files before creating the cache.
        fs::write(dir.join("pre1.leaf"), [0u8; 100]).unwrap();
        fs::write(dir.join("pre2.leaf"), [0u8; 200]).unwrap();

        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        assert_eq!(cache.current_bytes().unwrap(), 300);
    }

    #[test]
    fn write_creates_parent_dirs() {
        let dir = temp_cache_dir("nested-write");
        let cache = DiskArtifactCache::with_budget(dir.clone(), 1024 * 1024);
        let target = dir.join("deep").join("nested").join("file.leaf");

        cache.best_effort_write(&target, b"nested data");
        assert_eq!(fs::read(&target).unwrap(), b"nested data");
    }

    // ========================================================================
    // LeafletCache integration (fluree/db-r#155 follow-up — Change 4)
    // ========================================================================

    use async_trait::async_trait;
    use fluree_db_core::{content_kind::ContentKind, Error as CoreError, Result as CoreResult};

    /// Minimal `ContentStore` that delegates to an in-memory map and counts
    /// every `get()` call. Used by the Change-4 validation tests to prove
    /// that a second `fetch_cached_bytes*` against the same CID serves from
    /// `LeafletCache` (in-memory) without re-invoking the backend.
    #[derive(Debug)]
    struct CountingCs {
        data: std::sync::RwLock<std::collections::HashMap<ContentId, Vec<u8>>>,
        get_count: AtomicU64,
    }

    impl CountingCs {
        fn new() -> Self {
            Self {
                data: std::sync::RwLock::new(std::collections::HashMap::new()),
                get_count: AtomicU64::new(0),
            }
        }

        fn insert_with_cid(&self, id: ContentId, bytes: Vec<u8>) {
            self.data.write().unwrap().insert(id, bytes);
        }

        fn get_count(&self) -> u64 {
            self.get_count.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl ContentStore for CountingCs {
        async fn has(&self, id: &ContentId) -> CoreResult<bool> {
            Ok(self.data.read().unwrap().contains_key(id))
        }

        async fn get(&self, id: &ContentId) -> CoreResult<Vec<u8>> {
            self.get_count.fetch_add(1, Ordering::Relaxed);
            self.data
                .read()
                .unwrap()
                .get(id)
                .cloned()
                .ok_or_else(|| CoreError::not_found(id.to_string()))
        }

        async fn put(&self, kind: ContentKind, bytes: &[u8]) -> CoreResult<ContentId> {
            let id = ContentId::new(kind, bytes);
            self.data
                .write()
                .unwrap()
                .insert(id.clone(), bytes.to_vec());
            Ok(id)
        }

        async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> CoreResult<()> {
            self.data
                .write()
                .unwrap()
                .insert(id.clone(), bytes.to_vec());
            Ok(())
        }

        async fn release(&self, _id: &ContentId) -> CoreResult<()> {
            Ok(())
        }
    }

    /// A `LeafletCache`-backed `fetch_cached_bytes` call serves the second
    /// request from in-memory cache — the backend's `get()` is NOT invoked
    /// the second time. Validates the observable effect of Change 4
    /// (fluree/db-r#155 follow-up): the incident report's
    /// `cache_budget_pct = 0.12%` was caused by the load-time fetch path
    /// bypassing LeafletCache; this proves the path now participates.
    #[tokio::test]
    async fn fetch_cached_bytes_hits_leaflet_cache_on_repeat() {
        let dir = temp_cache_dir("leaflet-cache-repeat");
        let payload = b"arena-blob-payload".to_vec();
        let id = ContentId::new(ContentKind::IndexBranch, &payload);

        let cs = Arc::new(CountingCs::new());
        cs.insert_with_cid(id.clone(), payload.clone());

        let leaflet_cache = Arc::new(LeafletCache::with_max_mb(4));

        // First call: cache miss → backend invoked once.
        let b1 = fetch_cached_bytes(cs.as_ref(), &id, &dir, "blob", Some(&leaflet_cache))
            .await
            .expect("first fetch");
        assert_eq!(b1, payload);
        assert_eq!(cs.get_count(), 1, "first call should invoke backend once");

        // Second call: should be served from LeafletCache (LoadArtifact slot).
        // `get()` count must NOT increase.
        let b2 = fetch_cached_bytes(cs.as_ref(), &id, &dir, "blob", Some(&leaflet_cache))
            .await
            .expect("second fetch");
        assert_eq!(b2, payload);
        assert_eq!(
            cs.get_count(),
            1,
            "second call must hit in-memory cache, not backend"
        );

        // Without a LeafletCache the second call DOES invoke the backend
        // again (depending on disk cache presence the disk tier serves it,
        // but `get_count` only tracks backend calls). Sanity: baseline.
        let b3 = fetch_cached_bytes(cs.as_ref(), &id, &dir, "blob", None)
            .await
            .expect("third fetch without cache");
        assert_eq!(b3, payload);
        // Disk cache should still serve it — backend get count stays at 1.
        assert_eq!(
            cs.get_count(),
            1,
            "disk cache served without backend re-fetch"
        );
    }

    /// Same check for the CID-path variant (`fetch_cached_bytes_cid`)
    /// because it has its own body and its own cache-wrapper — regression
    /// proof that both branches thread LeafletCache correctly.
    #[tokio::test]
    async fn fetch_cached_bytes_cid_hits_leaflet_cache_on_repeat() {
        let dir = temp_cache_dir("leaflet-cache-cid-repeat");
        let payload = b"branch-manifest-payload".to_vec();
        let id = ContentId::new(ContentKind::IndexBranch, &payload);

        let cs = Arc::new(CountingCs::new());
        cs.insert_with_cid(id.clone(), payload.clone());

        let leaflet_cache = Arc::new(LeafletCache::with_max_mb(4));

        let b1 = fetch_cached_bytes_cid(cs.as_ref(), &id, &dir, Some(&leaflet_cache))
            .await
            .expect("first fetch");
        assert_eq!(b1, payload);
        assert_eq!(cs.get_count(), 1);

        let b2 = fetch_cached_bytes_cid(cs.as_ref(), &id, &dir, Some(&leaflet_cache))
            .await
            .expect("second fetch");
        assert_eq!(b2, payload);
        assert_eq!(
            cs.get_count(),
            1,
            "CID variant must also hit in-memory cache on repeat"
        );
    }
}
