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
use std::sync::Arc;

use super::branch::DictBranch;
use super::forward_leaf::ForwardLeaf;
use super::reverse_leaf::ReverseLeaf;

use crate::run_index::leaflet_cache::LeafletCache;

/// Leaf data source for demand-loading.
#[derive(Debug)]
pub enum LeafSource {
    /// Read leaves from local files. Maps CAS address → file path.
    LocalFiles(HashMap<String, PathBuf>),
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
}

impl DictTreeReader {
    /// Create a reader from a decoded branch and leaf source.
    pub fn new(branch: DictBranch, leaf_source: LeafSource) -> Self {
        Self {
            branch,
            leaf_source,
            global_cache: None,
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

    /// Forward lookup: find value by u64 ID.
    pub fn forward_lookup(&self, id: u64) -> io::Result<Option<Vec<u8>>> {
        let leaf_idx = match self.branch.find_leaf_by_id(id) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        let address = &self.branch.leaves[leaf_idx].address;
        let leaf_data = self.load_leaf(address)?;
        let leaf = ForwardLeaf::from_bytes(&leaf_data)?;

        Ok(leaf.lookup(id).map(|v| v.to_vec()))
    }

    /// Forward lookup returning a string (convenience for IRI/value resolution).
    pub fn forward_lookup_str(&self, id: u64) -> io::Result<Option<String>> {
        match self.forward_lookup(id)? {
            Some(bytes) => {
                let s = String::from_utf8(bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(Some(s))
            }
            None => Ok(None),
        }
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

    /// Load leaf bytes from the configured source.
    ///
    /// For `LocalFiles` with a global cache: uses the cache (keyed by
    /// `xxh3_128(cas_address)`) to avoid repeated disk reads.
    /// Without a cache: reads directly from disk.
    fn load_leaf(&self, address: &str) -> io::Result<Arc<[u8]>> {
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
                    let path = path.clone();
                    cache.try_get_or_load_dict_leaf(cache_key, || {
                        let bytes = std::fs::read(&path)?;
                        Ok(Arc::from(bytes.into_boxed_slice()))
                    })
                } else {
                    let bytes = std::fs::read(path)?;
                    Ok(Arc::from(bytes.into_boxed_slice()))
                }
            }
            LeafSource::InMemory(map) => map.get(address).cloned().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("dict tree: no in-memory leaf for {}", address),
                )
            }),
        }
    }

    /// Total entries across all leaves.
    pub fn total_entries(&self) -> u64 {
        self.branch.total_entries()
    }
}

impl std::fmt::Debug for DictTreeReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DictTreeReader")
            .field("leaf_count", &self.branch.leaves.len())
            .field("total_entries", &self.total_entries())
            .field("has_global_cache", &self.global_cache.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dict_tree::builder;
    use crate::dict_tree::forward_leaf::ForwardEntry;
    use crate::dict_tree::reverse_leaf::ReverseEntry;

    fn build_forward_reader(entries: Vec<ForwardEntry>) -> DictTreeReader {
        let result =
            builder::build_forward_tree(entries, builder::DEFAULT_TARGET_LEAF_BYTES).unwrap();

        // Simulate CAS: use pending hash as the address key
        let mut leaf_map = HashMap::new();
        for (leaf_artifact, branch_leaf) in result.leaves.iter().zip(result.branch.leaves.iter()) {
            leaf_map.insert(branch_leaf.address.clone(), leaf_artifact.bytes.clone());
        }

        DictTreeReader::from_memory(result.branch, leaf_map)
    }

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
    fn test_forward_lookup() {
        let entries: Vec<ForwardEntry> = (0..100)
            .map(|i| ForwardEntry {
                id: i * 10,
                value: format!("http://example.org/{}", i).into_bytes(),
            })
            .collect();

        let reader = build_forward_reader(entries);

        assert_eq!(
            reader.forward_lookup_str(0).unwrap(),
            Some("http://example.org/0".to_string())
        );
        assert_eq!(
            reader.forward_lookup_str(500).unwrap(),
            Some("http://example.org/50".to_string())
        );
        assert_eq!(reader.forward_lookup_str(5).unwrap(), None);
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

    #[test]
    fn test_forward_multi_leaf() {
        // Force multiple leaves with small target
        let entries: Vec<ForwardEntry> = (0..2000)
            .map(|i| ForwardEntry {
                id: i,
                value: format!("http://example.org/entity/long/path/{}", i).into_bytes(),
            })
            .collect();

        let result = builder::build_forward_tree(entries, 4096).unwrap();
        assert!(result.leaves.len() > 1);

        let mut leaf_map = HashMap::new();
        for (artifact, bl) in result.leaves.iter().zip(result.branch.leaves.iter()) {
            leaf_map.insert(bl.address.clone(), artifact.bytes.clone());
        }
        let reader = DictTreeReader::from_memory(result.branch, leaf_map);

        // Look up entries that span different leaves
        for id in [0, 500, 999, 1500, 1999] {
            let val = reader.forward_lookup_str(id).unwrap();
            assert!(val.is_some(), "should find id={}", id);
            assert!(val.unwrap().contains(&id.to_string()));
        }
    }

    #[test]
    fn test_empty_tree() {
        let reader = build_forward_reader(vec![]);
        assert_eq!(reader.forward_lookup(0).unwrap(), None);
        assert_eq!(reader.total_entries(), 0);
    }
}
