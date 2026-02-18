//! Multi-pack reader with mmap support for forward dictionary packs.
//!
//! `ForwardPackReader` manages one or more `FPK1` packs and routes lookups
//! to the correct pack via binary search on ID ranges.
//!
//! ## Loading
//!
//! - **`from_pack_refs`**: Async constructor. Resolves packs from local CAS paths,
//!   on-disk cache, or remote CAS fetch. All packs are eagerly loaded and validated.
//! - **`from_memory`**: In-memory constructor for testing.

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::forward_pack::{lookup_in_pack, parse_pack_meta, ParsedPackMeta};
use crate::run_index::index_root::PackBranchEntry;
use fluree_db_core::ContentStore;

/// Global atomic counter for unique temp file names (avoids collisions
/// across concurrent pack fetches within the same process).
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

// ============================================================================
// PackHandle — owns routing info + backing store for a single pack
// ============================================================================

struct PackHandle {
    first_id: u64,
    last_id: u64,
    meta: ParsedPackMeta,
    backing: LoadedBacking,
}

enum LoadedBacking {
    Mmap(memmap2::Mmap),
    InMemory(Arc<[u8]>),
}

impl LoadedBacking {
    fn bytes(&self) -> &[u8] {
        match self {
            LoadedBacking::Mmap(mmap) => mmap.as_ref(),
            LoadedBacking::InMemory(bytes) => bytes.as_ref(),
        }
    }
}

// ============================================================================
// ForwardPackReader
// ============================================================================

/// Multi-pack reader for forward dictionary lookups.
///
/// Manages one or more `FPK1` packs, sorted by ID range. Lookups binary-search
/// the packs, then use pre-parsed metadata for zero-alloc page navigation.
pub struct ForwardPackReader {
    packs: Vec<PackHandle>,
}

impl ForwardPackReader {
    /// Load packs from CAS, eagerly fetching all of them.
    ///
    /// For each `PackBranchEntry`:
    /// 1. If `cs.resolve_local_path(&cid)` returns a path → mmap directly.
    /// 2. Else if cache file exists → mmap from cache.
    /// 3. Else → fetch from CAS, write to cache, mmap the cache file.
    ///
    /// After loading, each pack's header is validated: ID range must match the
    /// routing entry, and `kind`/`ns_code` must match `expected_kind`/`expected_ns_code`.
    pub async fn from_pack_refs(
        cs: Arc<dyn ContentStore>,
        cache_dir: &Path,
        refs: &[PackBranchEntry],
        expected_kind: u8,
        expected_ns_code: u16,
    ) -> io::Result<Self> {
        // Pre-create cache directory once.
        if !refs.is_empty() {
            std::fs::create_dir_all(cache_dir).map_err(|e| {
                io::Error::other(format!("create cache dir {}: {}", cache_dir.display(), e))
            })?;
        }

        let mut packs = Vec::with_capacity(refs.len());

        for entry in refs {
            let cache_name = format!("{}.fpk", entry.pack_cid.digest_hex());
            let cache_path = cache_dir.join(&cache_name);

            let local_path = cs.resolve_local_path(&entry.pack_cid);

            let (mmap, meta) = if let Some(path) = local_path {
                // Local CAS path — mmap directly.
                let mmap = mmap_file(&path)?;
                let meta = parse_pack_meta(mmap.as_ref())?;
                (mmap, meta)
            } else if cache_path.exists() {
                // Cached on disk — mmap.
                let mmap = mmap_file(&cache_path)?;
                let meta = parse_pack_meta(mmap.as_ref())?;
                (mmap, meta)
            } else {
                // Remote — fetch, write to cache, mmap the cache file.
                let bytes = cs
                    .get(&entry.pack_cid)
                    .await
                    .map_err(|e| io::Error::other(format!("pack fetch: {e}")))?;

                atomic_write_to_cache(&cache_path, &bytes)?;

                let mmap = mmap_file(&cache_path)?;
                let meta = parse_pack_meta(mmap.as_ref())?;
                (mmap, meta)
            };

            validate_meta(&meta, entry, expected_kind, expected_ns_code)?;
            packs.push(PackHandle {
                first_id: entry.first_id,
                last_id: entry.last_id,
                meta,
                backing: LoadedBacking::Mmap(mmap),
            });
        }

        // Sort by first_id (should already be sorted, but enforce).
        packs.sort_by_key(|p| p.first_id);
        validate_pack_routing(&packs)?;

        Ok(Self { packs })
    }

    /// Create from pre-built in-memory pack bytes (for testing).
    pub fn from_memory(pack_bytes_list: Vec<Arc<[u8]>>) -> io::Result<Self> {
        let mut packs = Vec::with_capacity(pack_bytes_list.len());

        for bytes in pack_bytes_list {
            let meta = parse_pack_meta(&bytes)?;
            packs.push(PackHandle {
                first_id: meta.first_id,
                last_id: meta.last_id,
                meta,
                backing: LoadedBacking::InMemory(bytes),
            });
        }

        packs.sort_by_key(|p| p.first_id);
        validate_pack_routing(&packs)?;

        Ok(Self { packs })
    }

    /// Create an empty reader (no packs).
    pub fn empty() -> Self {
        Self { packs: Vec::new() }
    }

    /// Number of packs in this reader.
    pub fn pack_count(&self) -> usize {
        self.packs.len()
    }

    /// Hot-path: append value bytes to `out`. Returns `true` if the ID was found.
    ///
    /// Zero-alloc steady state: uses pre-parsed page directory for binary search,
    /// then O(1) offset indexing within the page.
    pub fn forward_lookup_into(&self, id: u64, out: &mut Vec<u8>) -> io::Result<bool> {
        let Some(handle) = self.find_pack(id) else {
            return Ok(false);
        };
        match lookup_in_pack(handle.backing.bytes(), &handle.meta, id) {
            Some(value) => {
                out.extend_from_slice(value);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Convenience: look up and return as a `String`.
    pub fn forward_lookup_str(&self, id: u64) -> io::Result<Option<String>> {
        let Some(handle) = self.find_pack(id) else {
            return Ok(None);
        };
        match lookup_in_pack(handle.backing.bytes(), &handle.meta, id) {
            Some(value) => {
                let s = std::str::from_utf8(value)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(Some(s.to_string()))
            }
            None => Ok(None),
        }
    }

    /// Binary search packs by ID to find the one containing `id`.
    fn find_pack(&self, id: u64) -> Option<&PackHandle> {
        let idx = self.packs.partition_point(|p| p.first_id <= id);
        if idx == 0 {
            return None;
        }
        let candidate = &self.packs[idx - 1];
        if id <= candidate.last_id {
            Some(candidate)
        } else {
            None
        }
    }
}

impl std::fmt::Debug for ForwardPackReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForwardPackReader")
            .field("pack_count", &self.packs.len())
            .finish()
    }
}

// ============================================================================
// Validation
// ============================================================================

/// Validate that pack handles have strictly increasing, non-overlapping ID ranges.
fn validate_pack_routing(packs: &[PackHandle]) -> io::Result<()> {
    for i in 1..packs.len() {
        let prev_last = packs[i - 1].last_id;
        let curr_first = packs[i].first_id;
        if curr_first <= prev_last {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "pack routing: pack {} first_id {} overlaps with previous last_id {}",
                    i, curr_first, prev_last
                ),
            ));
        }
    }
    Ok(())
}

/// Validate parsed pack metadata against the root routing entry and expected kind/ns_code.
fn validate_meta(
    meta: &ParsedPackMeta,
    entry: &PackBranchEntry,
    expected_kind: u8,
    expected_ns_code: u16,
) -> io::Result<()> {
    if meta.first_id != entry.first_id || meta.last_id != entry.last_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "pack header range [{}, {}] doesn't match root routing entry [{}, {}]",
                meta.first_id, meta.last_id, entry.first_id, entry.last_id,
            ),
        ));
    }
    if meta.kind != expected_kind {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "pack kind {} doesn't match expected {}",
                meta.kind, expected_kind,
            ),
        ));
    }
    if meta.ns_code != expected_ns_code {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "pack ns_code {} doesn't match expected {}",
                meta.ns_code, expected_ns_code,
            ),
        ));
    }
    Ok(())
}

// ============================================================================
// Helpers
// ============================================================================

fn mmap_file(path: &Path) -> io::Result<memmap2::Mmap> {
    let file = std::fs::File::open(path).map_err(|e| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("open pack file {}: {}", path.display(), e),
        )
    })?;
    // SAFETY: The file is an immutable CAS artifact, not concurrently modified.
    unsafe { memmap2::Mmap::map(&file) }
}

/// Write bytes to a cache file atomically (temp file + rename).
fn atomic_write_to_cache(cache_path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = cache_path.with_extension(format!(
        "tmp.{}.{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&tmp, bytes)?;
    match std::fs::rename(&tmp, cache_path) {
        Ok(()) => Ok(()),
        Err(_) if cache_path.exists() => {
            // Another process won the race — discard our tmp and use theirs.
            let _ = std::fs::remove_file(&tmp);
            Ok(())
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            Err(e)
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dict_tree::forward_pack::{encode_forward_pack, KIND_STRING_FWD};

    fn make_pack_bytes(first: u64, count: usize) -> Vec<u8> {
        let entries: Vec<(u64, Vec<u8>)> = (0..count)
            .map(|i| {
                let id = first + i as u64;
                (id, format!("val_{}", id).into_bytes())
            })
            .collect();
        let refs: Vec<(u64, &[u8])> = entries.iter().map(|(id, v)| (*id, v.as_slice())).collect();
        encode_forward_pack(&refs, KIND_STRING_FWD, 0, 2 * 1024 * 1024).unwrap()
    }

    #[test]
    fn test_single_pack_memory() {
        let bytes = make_pack_bytes(0, 100);
        let reader =
            ForwardPackReader::from_memory(vec![Arc::from(bytes.into_boxed_slice())]).unwrap();

        assert_eq!(reader.pack_count(), 1);

        // Hit
        assert_eq!(
            reader.forward_lookup_str(50).unwrap(),
            Some("val_50".to_string())
        );

        // Miss
        assert_eq!(reader.forward_lookup_str(100).unwrap(), None);
    }

    #[test]
    fn test_multi_pack_memory() {
        let pack1 = make_pack_bytes(0, 100);
        let pack2 = make_pack_bytes(100, 100);
        let pack3 = make_pack_bytes(200, 50);

        let reader = ForwardPackReader::from_memory(vec![
            Arc::from(pack1.into_boxed_slice()),
            Arc::from(pack2.into_boxed_slice()),
            Arc::from(pack3.into_boxed_slice()),
        ])
        .unwrap();

        assert_eq!(reader.pack_count(), 3);

        // First pack
        assert_eq!(
            reader.forward_lookup_str(0).unwrap(),
            Some("val_0".to_string())
        );
        assert_eq!(
            reader.forward_lookup_str(99).unwrap(),
            Some("val_99".to_string())
        );

        // Second pack
        assert_eq!(
            reader.forward_lookup_str(100).unwrap(),
            Some("val_100".to_string())
        );

        // Third pack
        assert_eq!(
            reader.forward_lookup_str(249).unwrap(),
            Some("val_249".to_string())
        );

        // Out of range
        assert_eq!(reader.forward_lookup_str(250).unwrap(), None);
    }

    #[test]
    fn test_lookup_into() {
        let bytes = make_pack_bytes(0, 10);
        let reader =
            ForwardPackReader::from_memory(vec![Arc::from(bytes.into_boxed_slice())]).unwrap();

        let mut out = Vec::new();
        assert!(reader.forward_lookup_into(5, &mut out).unwrap());
        assert_eq!(out, b"val_5");

        assert!(!reader.forward_lookup_into(999, &mut out).unwrap());
        assert_eq!(out.len(), 5); // unchanged
    }

    #[test]
    fn test_empty_reader() {
        let reader = ForwardPackReader::empty();
        assert_eq!(reader.pack_count(), 0);
        assert_eq!(reader.forward_lookup_str(0).unwrap(), None);
    }

    #[test]
    fn test_gap_between_packs() {
        // Packs covering [0..99] and [200..299] — gap at [100..199]
        let pack1 = make_pack_bytes(0, 100);
        let pack2 = make_pack_bytes(200, 100);

        let reader = ForwardPackReader::from_memory(vec![
            Arc::from(pack1.into_boxed_slice()),
            Arc::from(pack2.into_boxed_slice()),
        ])
        .unwrap();

        assert_eq!(
            reader.forward_lookup_str(50).unwrap(),
            Some("val_50".to_string())
        );
        assert_eq!(reader.forward_lookup_str(150).unwrap(), None); // in the gap
        assert_eq!(
            reader.forward_lookup_str(250).unwrap(),
            Some("val_250".to_string())
        );
    }

    #[test]
    fn test_overlapping_packs_rejected() {
        // Packs [0..99] and [50..149] overlap — should be rejected.
        let pack1 = make_pack_bytes(0, 100);
        let pack2 = make_pack_bytes(50, 100);

        let result = ForwardPackReader::from_memory(vec![
            Arc::from(pack1.into_boxed_slice()),
            Arc::from(pack2.into_boxed_slice()),
        ]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("overlaps"));
    }
}
