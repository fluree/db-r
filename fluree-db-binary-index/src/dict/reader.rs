//! Demand-loading read interface for dictionary trees.
//!
//! A `DictTreeReader` holds a decoded branch manifest and resolves lookups
//! by loading the appropriate leaf on demand. Leaf data is read from local
//! files or provided in memory.
//!
//! When a shared `LeafletCache` is provided, `LocalFiles` lookups go through
//! the global LRU cache (respecting the customer's memory budget). Without
//! a cache, leaves are read directly from disk on each lookup.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::branch::DictBranch;
use super::reverse_leaf::ReverseLeaf;

use crate::read::leaflet_cache::LeafletCache;
use fluree_db_core::{ContentId, ContentStore};

/// Leaf data source for demand-loading.
#[derive(Debug)]
pub enum LeafSource {
    /// Read leaves from local files. Maps CAS address → file path.
    LocalFiles(HashMap<String, PathBuf>),
    /// Fetch leaves from a CAS content store on demand (remote storages), with optional
    /// local file fallbacks for any leaves that are already locally resolvable.
    ///
    /// This avoids pre-downloading entire dictionaries during store construction
    /// (critical for Lambda + S3 cold starts). Leaf bytes are cached in `LeafletCache`
    /// when configured.
    CasOnDemand {
        cs: Arc<dyn ContentStore>,
        local_files: HashMap<String, PathBuf>,
        remote_cids: HashMap<String, ContentId>,
    },
    /// Leaves are provided inline (for testing or small dictionaries).
    InMemory(HashMap<String, Arc<[u8]>>),
}

/// A demand-loading reader for dictionary trees (forward or reverse).
///
/// For `LocalFiles` sources, uses the global `LeafletCache` (if provided)
/// to avoid repeated disk reads. Dict tree leaves are content-addressed
/// and immutable, so the CAS address hash has astronomically unlikely
/// collisions with no epoch/staging dimension.
pub struct DictTreeReader {
    branch: DictBranch,
    leaf_source: LeafSource,
    /// Shared global cache for dict leaf blobs. Content-addressed leaves
    /// use `xxh3_128(cas_address)` as the cache key — immutable, no
    /// epoch/time dimension needed.
    global_cache: Option<Arc<LeafletCache>>,
    /// Performance counters (atomic for shared access).
    disk_reads: AtomicU64,
    cache_hits: AtomicU64,
}

impl DictTreeReader {
    /// Create a reader from a decoded branch and leaf source.
    pub fn new(branch: DictBranch, leaf_source: LeafSource) -> Self {
        Self {
            branch,
            leaf_source,
            global_cache: None,
            disk_reads: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
        }
    }

    /// Create a reader backed by the global leaflet cache.
    ///
    /// The cache is shared across all stores and readers, giving one
    /// global memory budget. Dict leaves are cached by their CAS address
    /// hash (content-addressed → immutable, astronomically unlikely collisions).
    pub fn with_cache(
        branch: DictBranch,
        leaf_source: LeafSource,
        cache: Arc<LeafletCache>,
    ) -> Self {
        Self {
            branch,
            leaf_source,
            global_cache: Some(cache),
            disk_reads: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
        }
    }

    /// Create a reader with all leaves pre-loaded in memory.
    pub fn from_memory(branch: DictBranch, leaves: HashMap<String, Vec<u8>>) -> Self {
        let arc_leaves: HashMap<String, Arc<[u8]>> = leaves
            .into_iter()
            .map(|(k, v)| (k, Arc::from(v.into_boxed_slice())))
            .collect();
        Self {
            branch,
            leaf_source: LeafSource::InMemory(arc_leaves),
            global_cache: None,
            disk_reads: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
        }
    }

    /// Load a `DictTreeReader` from CAS-stored [`DictTreeRefs`].
    ///
    /// Fetches and decodes the branch, resolves each leaf CID to a local
    /// file path or keeps it as a remote CID for on-demand fetching, then
    /// constructs the reader with an optional leaflet cache.
    pub async fn from_refs(
        cs: &Arc<dyn ContentStore>,
        refs: &crate::format::wire_helpers::DictTreeRefs,
        leaflet_cache: Option<&Arc<LeafletCache>>,
    ) -> io::Result<Self> {
        let branch_bytes = cs.get(&refs.branch).await.map_err(|e| {
            io::Error::other(format!("failed to load branch: {e}"))
        })?;
        let branch = DictBranch::decode(&branch_bytes)?;

        let mut local_files = HashMap::with_capacity(branch.leaves.len());
        let mut remote_cids = HashMap::new();

        for (cid, bl) in refs.leaves.iter().zip(branch.leaves.iter()) {
            if let Some(local_path) = cs.resolve_local_path(cid) {
                local_files.insert(bl.address.clone(), local_path);
            } else {
                remote_cids.insert(bl.address.clone(), cid.clone());
            }
        }

        let leaf_source = if remote_cids.is_empty() {
            LeafSource::LocalFiles(local_files)
        } else {
            LeafSource::CasOnDemand {
                cs: Arc::clone(cs),
                local_files,
                remote_cids,
            }
        };

        match leaflet_cache {
            Some(cache) => Ok(Self::with_cache(branch, leaf_source, Arc::clone(cache))),
            None => Ok(Self::new(branch, leaf_source)),
        }
    }

    /// Attach a global cache to this reader.
    pub fn set_cache(&mut self, cache: Option<Arc<LeafletCache>>) {
        self.global_cache = cache;
    }

    /// The underlying branch manifest.
    pub fn branch(&self) -> &DictBranch {
        &self.branch
    }
    /// Reverse lookup: find ID by key bytes.
    pub fn reverse_lookup(&self, key: &[u8]) -> io::Result<Option<u64>> {
        let leaf_idx = match self.branch.find_leaf(key) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        let address = &self.branch.leaves[leaf_idx].address;
        let leaf_data = self.load_leaf(address)?;
        let leaf = ReverseLeaf::from_bytes(&leaf_data)?;

        Ok(leaf.lookup(key))
    }

    /// Range scan: find all entries whose key is in `[start_key, end_key)`.
    ///
    /// Scans across multiple B-tree leaves as needed. Returns `(key_bytes, id)` pairs
    /// in sorted key order. Used for subject prefix scans (e.g., commit SHA lookup).
    pub fn reverse_range_scan(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> io::Result<Vec<(Vec<u8>, u64)>> {
        if self.branch.leaves.is_empty() {
            return Ok(Vec::new());
        }

        // Find the first leaf that might contain start_key.
        let start_leaf = match self.branch.find_leaf(start_key) {
            Some(idx) => idx,
            None => {
                // start_key is before the first leaf or after the last.
                // If the first leaf's first_key >= start_key, it might have matches.
                if self.branch.leaves[0].first_key.as_slice() >= start_key {
                    0
                } else {
                    return Ok(Vec::new());
                }
            }
        };

        let mut results = Vec::new();

        for leaf_idx in start_leaf..self.branch.leaves.len() {
            let leaf_entry = &self.branch.leaves[leaf_idx];

            // If this leaf's first_key >= end_key, no more matches possible.
            if leaf_entry.first_key.as_slice() >= end_key {
                break;
            }

            let leaf_data = self.load_leaf(&leaf_entry.address)?;
            let leaf = ReverseLeaf::from_bytes(&leaf_data)?;

            for (key, id) in leaf.scan_range(start_key, end_key) {
                results.push((key.to_vec(), id));
            }
        }

        Ok(results)
    }

    /// Load leaf bytes from the configured source.
    ///
    /// For `LocalFiles` with a global cache: uses the cache (keyed by
    /// `xxh3_128(cas_address)`) to avoid repeated disk reads.
    /// Without a cache: reads directly from disk.
    fn load_leaf(&self, address: &str) -> io::Result<Arc<[u8]>> {
        fn fetch_remote_leaf_bytes(
            cs: Arc<dyn ContentStore>,
            cid: ContentId,
        ) -> io::Result<Vec<u8>> {
            // DictTreeReader is sync, but ContentStore::get is async.
            //
            // We intentionally avoid `block_in_place` here because this code can run on
            // runtimes that don't support it (e.g., current-thread). Instead we spawn a
            // short-lived OS thread and block on the Tokio handle there.
            let handle = tokio::runtime::Handle::try_current().map_err(|_| {
                io::Error::other("dict tree: remote leaf fetch requires a Tokio runtime")
            })?;

            let (tx, rx) = std::sync::mpsc::sync_channel::<Result<Vec<u8>, String>>(1);
            std::thread::spawn(move || {
                let res = handle
                    .block_on(async { cs.get(&cid).await })
                    .map_err(|e| e.to_string());
                let _ = tx.send(res);
            });

            let res = rx
                .recv()
                .map_err(|_| io::Error::other("dict tree: fetch thread died"))?;
            res.map_err(io::Error::other)
        }

        match &self.leaf_source {
            LeafSource::LocalFiles(map) => {
                let path = map.get(address).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("dict tree: no local file for leaf {}", address),
                    )
                })?;

                if let Some(cache) = &self.global_cache {
                    let cache_key = xxhash_rust::xxh3::xxh3_128(address.as_bytes());
                    if let Some(bytes) = cache.get_dict_leaf(cache_key) {
                        self.cache_hits.fetch_add(1, Ordering::Relaxed);
                        return Ok(bytes);
                    }
                    let path = path.clone();
                    let disk_reads = &self.disk_reads;
                    cache.try_get_or_load_dict_leaf(cache_key, || {
                        disk_reads.fetch_add(1, Ordering::Relaxed);
                        let bytes = std::fs::read(&path)?;
                        Ok(Arc::from(bytes.into_boxed_slice()))
                    })
                } else {
                    self.disk_reads.fetch_add(1, Ordering::Relaxed);
                    let bytes = std::fs::read(path)?;
                    Ok(Arc::from(bytes.into_boxed_slice()))
                }
            }
            LeafSource::CasOnDemand {
                cs,
                local_files,
                remote_cids,
            } => {
                // Local file fast-path when available (e.g., file storage or mixed backends).
                if let Some(path) = local_files.get(address) {
                    if let Some(cache) = &self.global_cache {
                        let cache_key = xxhash_rust::xxh3::xxh3_128(address.as_bytes());
                        if let Some(bytes) = cache.get_dict_leaf(cache_key) {
                            self.cache_hits.fetch_add(1, Ordering::Relaxed);
                            return Ok(bytes);
                        }
                        let path = path.clone();
                        let disk_reads = &self.disk_reads;
                        return cache.try_get_or_load_dict_leaf(cache_key, || {
                            disk_reads.fetch_add(1, Ordering::Relaxed);
                            let bytes = std::fs::read(&path)?;
                            Ok(Arc::from(bytes.into_boxed_slice()))
                        });
                    }
                    self.disk_reads.fetch_add(1, Ordering::Relaxed);
                    let bytes = std::fs::read(path)?;
                    return Ok(Arc::from(bytes.into_boxed_slice()));
                }

                let cid = remote_cids.get(address).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("dict tree: no CID mapping for leaf {}", address),
                    )
                })?;

                // Remote fetch path: cache bytes in LeafletCache (keyed by CAS address).
                if let Some(cache) = &self.global_cache {
                    let cache_key = xxhash_rust::xxh3::xxh3_128(address.as_bytes());
                    if let Some(bytes) = cache.get_dict_leaf(cache_key) {
                        self.cache_hits.fetch_add(1, Ordering::Relaxed);
                        return Ok(bytes);
                    }
                    let cs = Arc::clone(cs);
                    let cid = cid.clone();
                    let disk_reads = &self.disk_reads;
                    cache.try_get_or_load_dict_leaf(cache_key, || {
                        disk_reads.fetch_add(1, Ordering::Relaxed);
                        let bytes = fetch_remote_leaf_bytes(cs, cid)?;
                        Ok(Arc::from(bytes.into_boxed_slice()))
                    })
                } else {
                    self.disk_reads.fetch_add(1, Ordering::Relaxed);
                    let bytes = fetch_remote_leaf_bytes(Arc::clone(cs), cid.clone())?;
                    Ok(Arc::from(bytes.into_boxed_slice()))
                }
            }
            LeafSource::InMemory(map) => {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                map.get(address).cloned().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("dict tree: no in-memory leaf for {}", address),
                    )
                })
            }
        }
    }

    /// Total entries across all leaves.
    pub fn total_entries(&self) -> u64 {
        self.branch.total_entries()
    }

    /// Number of disk reads performed since creation.
    pub fn disk_reads(&self) -> u64 {
        self.disk_reads.load(Ordering::Relaxed)
    }

    /// Number of cache hits since creation (InMemory always counts as hit).
    pub fn cache_hits(&self) -> u64 {
        self.cache_hits.load(Ordering::Relaxed)
    }

    /// Preload all leaves into the global cache (or just read them into OS page cache).
    ///
    /// Returns the number of leaves loaded. This is useful for warming caches
    /// at server startup so the first query doesn't pay cold-start I/O penalties.
    pub fn preload_all_leaves(&self) -> io::Result<usize> {
        let mut loaded = 0;
        for entry in &self.branch.leaves {
            let _ = self.load_leaf(&entry.address)?;
            loaded += 1;
        }
        Ok(loaded)
    }
}

impl std::fmt::Debug for DictTreeReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DictTreeReader")
            .field("leaf_count", &self.branch.leaves.len())
            .field("total_entries", &self.total_entries())
            .field("has_global_cache", &self.global_cache.is_some())
            .field("disk_reads", &self.disk_reads())
            .field("cache_hits", &self.cache_hits())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dict::builder;
    use crate::dict::reverse_leaf::ReverseEntry;

    fn build_reverse_reader(entries: Vec<ReverseEntry>) -> DictTreeReader {
        let result =
            builder::build_reverse_tree(entries, builder::DEFAULT_TARGET_LEAF_BYTES).unwrap();

        let mut leaf_map = HashMap::new();
        for (leaf_artifact, branch_leaf) in result.leaves.iter().zip(result.branch.leaves.iter()) {
            leaf_map.insert(branch_leaf.address.clone(), leaf_artifact.bytes.clone());
        }

        DictTreeReader::from_memory(result.branch, leaf_map)
    }

    #[test]
    fn test_reverse_lookup() {
        let mut entries: Vec<ReverseEntry> = (0..100)
            .map(|i| ReverseEntry {
                key: format!("key_{:04}", i).into_bytes(),
                id: i as u64,
            })
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));

        let reader = build_reverse_reader(entries);

        assert_eq!(reader.reverse_lookup(b"key_0050").unwrap(), Some(50));
        assert_eq!(reader.reverse_lookup(b"nonexistent").unwrap(), None);
    }
}
