use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use fluree_db_core::{ContentId, ContentStore};
use once_cell::sync::Lazy;
use parking_lot::Mutex;

const CACHE_BUDGET_NUMERATOR: u64 = 9;
const CACHE_BUDGET_DENOMINATOR: u64 = 10;
const CACHE_EVICT_NUMERATOR: u64 = 8;
const CACHE_EVICT_DENOMINATOR: u64 = 10;
const DEFAULT_LAMBDA_TMP_BYTES: u64 = 512 * 1024 * 1024;
const DEFAULT_LAMBDA_TMP_WARN_SLACK_BYTES: u64 = 64 * 1024 * 1024;

static CACHE_REGISTRY: Lazy<Mutex<HashMap<PathBuf, Weak<DiskArtifactCache>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Debug)]
struct DiskArtifactCache {
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
    fn for_dir(cache_dir: &Path) -> Arc<Self> {
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
        let budget_bytes = available
            .saturating_mul(CACHE_BUDGET_NUMERATOR)
            .saturating_div(CACHE_BUDGET_DENOMINATOR);

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

    fn best_effort_write(&self, target: &Path, bytes: &[u8]) {
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

pub async fn fetch_cached_bytes(
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

pub async fn fetch_cached_bytes_cid(
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
