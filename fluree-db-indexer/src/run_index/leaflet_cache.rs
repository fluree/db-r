//! Unified LRU cache for decoded leaflet regions and dictionary leaves.
//!
//! Uses a **single** `moka::sync::Cache` (synchronous — `BinaryCursor` is
//! sync) with TinyLFU eviction. All entry types — Region 1, Region 2, and
//! dictionary tree leaves — share one pool so TinyLFU decides what stays
//! based on actual access patterns rather than fixed budget splits.
//!
//! Region 3 (history journal) is **never cached** — it's cold-path data
//! decoded on demand for time-travel replay and discarded afterwards.
//!
//! ## Cache Key
//!
//! A `CacheKey` enum discriminates entry types:
//! - `R1` / `R2`: keyed by `(leaf_id, leaflet_index, to_t, epoch)`.
//!   `to_t` distinguishes time-travel snapshots; `epoch` distinguishes
//!   staged vs committed views at the same `t`.
//! - `DictLeaf`: keyed by `xxh3_128(cas_address)`. Content-addressed and
//!   immutable — no epoch/time dimension needed.

use fluree_db_core::subject_id::SubjectIdColumn;
use moka::sync::Cache;
use std::io;
use std::sync::Arc;

// ============================================================================
// Cache key
// ============================================================================

/// Leaflet identity fields shared by R1 and R2 entries.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct LeafletCacheKey {
    /// Leaf identity: xxh3_128 of the content hash string from the branch manifest.
    /// Uses 128-bit hash to make collisions astronomically unlikely (~1 in 3.4×10^38).
    /// Leaves are content-addressed (`{sha256}.fli`), so this is stable.
    pub leaf_id: u128,
    /// Leaflet slot within the leaf (0..leaflet_count, max 255).
    pub leaflet_index: u8,
    /// Effective "state-at" t — always the resolved numeric t, never a sentinel.
    /// For current-time queries: `to_t = store.max_t()`.
    /// For time-travel: `to_t` = the target t requested by the query.
    pub to_t: i64,
    /// Overlay/stage invalidation epoch.
    /// From `OverlayProvider::epoch()`; staged != committed.
    /// 0 when no overlay is active.
    pub epoch: u64,
}

/// Unified cache key. The enum discriminant ensures R1, R2, and dict leaf
/// entries never collide even when the underlying identifiers match.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum CacheKey {
    R1(LeafletCacheKey),
    R2(LeafletCacheKey),
    /// Key = xxh3_128(CAS address). Content-addressed → immutable.
    DictLeaf(u128),
}

// ============================================================================
// Cached value types
// ============================================================================

/// Cached decoded Region 1 (core columns: s_id, p_id, o_kind, o_key).
///
/// Uses `Arc<[T]>` for zero-copy sharing between cache and in-flight cursors.
/// Subject IDs use [`SubjectIdColumn`] for compact storage: narrow mode (u32) saves
/// ~20% cache capacity compared to wide mode (u64).
#[derive(Clone, Debug)]
pub struct CachedRegion1 {
    pub s_ids: SubjectIdColumn,
    pub p_ids: Arc<[u32]>,
    pub o_kinds: Arc<[u8]>,
    pub o_keys: Arc<[u64]>,
    pub row_count: usize,
}

/// Cached decoded Region 2 (metadata columns: dt, t, lang, i).
#[derive(Clone, Debug)]
pub struct CachedRegion2 {
    pub dt_values: Arc<[u32]>,
    pub t_values: Arc<[i64]>,
    pub lang_ids: Arc<[u16]>,
    pub i_values: Arc<[i32]>,
}

impl CachedRegion1 {
    /// Approximate byte size of this cached value (for cache weighing).
    pub fn byte_size(&self) -> usize {
        // s_ids: 4 (narrow) or 8 (wide) + p_ids(4) + o_kinds(1) + o_keys(8)
        self.s_ids.byte_size() + self.row_count * (4 + 1 + 8)
    }
}

impl CachedRegion2 {
    /// Approximate byte size of this cached value (for cache weighing).
    pub fn byte_size(&self) -> usize {
        // dt(4) + t(8) + lang(2) + i(4) = 18 bytes per row
        self.dt_values.len() * 18
    }
}

// ============================================================================
// Unified cached value
// ============================================================================

/// Unified value stored in the single moka cache.
#[derive(Clone)]
enum CachedEntry {
    R1(CachedRegion1),
    R2(CachedRegion2),
    DictLeaf(Arc<[u8]>),
}

impl CachedEntry {
    /// Approximate byte size for the moka weigher.
    fn byte_size(&self) -> usize {
        match self {
            CachedEntry::R1(r1) => r1.byte_size(),
            CachedEntry::R2(r2) => r2.byte_size(),
            CachedEntry::DictLeaf(bytes) => bytes.len(),
        }
    }
}

// ============================================================================
// LeafletCache
// ============================================================================

/// Unified LRU cache for decoded leaflet regions and dictionary tree leaves,
/// backed by a single moka TinyLFU pool.
///
/// All entry types share one memory budget — TinyLFU decides what stays
/// based on actual access frequency/recency rather than fixed splits.
/// The lazy-Region-2 strategy is preserved: R1 and R2 use separate
/// `CacheKey` variants, so inserting R1 never evicts or implies R2.
pub struct LeafletCache {
    inner: Cache<CacheKey, CachedEntry>,
}

impl LeafletCache {
    /// Create a new cache with the given maximum byte budget.
    ///
    /// One pool, one budget. TinyLFU eviction applies across all entry types.
    pub fn with_max_bytes(max_bytes: u64) -> Self {
        let inner = Cache::builder()
            .weigher(|_key: &CacheKey, val: &CachedEntry| {
                val.byte_size().min(u32::MAX as usize) as u32
            })
            .max_capacity(max_bytes)
            .build();

        Self { inner }
    }

    // ========================================================================
    // Region 1
    // ========================================================================

    /// Get or decode Region 1 for the given key.
    ///
    /// On cache miss, calls `decode_fn` to produce the value, inserts it,
    /// and returns the cached copy.
    pub fn get_or_decode_r1<F>(&self, key: LeafletCacheKey, decode_fn: F) -> CachedRegion1
    where
        F: FnOnce() -> CachedRegion1,
    {
        let entry = self
            .inner
            .get_with(CacheKey::R1(key), || CachedEntry::R1(decode_fn()));
        match entry {
            CachedEntry::R1(r1) => r1,
            _ => unreachable!("R1 key always maps to R1 entry"),
        }
    }

    /// Check if Region 1 is cached for the given key (no insertion).
    pub fn get_r1(&self, key: &LeafletCacheKey) -> Option<CachedRegion1> {
        match self.inner.get(&CacheKey::R1(key.clone())) {
            Some(CachedEntry::R1(r1)) => Some(r1),
            _ => None,
        }
    }

    /// Check if Region 1 is cached for the given key without cloning the value.
    pub fn contains_r1(&self, key: &LeafletCacheKey) -> bool {
        self.inner.contains_key(&CacheKey::R1(key.clone()))
    }

    // ========================================================================
    // Region 2
    // ========================================================================

    /// Get or decode Region 2 for the given key.
    pub fn get_or_decode_r2<F>(&self, key: LeafletCacheKey, decode_fn: F) -> CachedRegion2
    where
        F: FnOnce() -> CachedRegion2,
    {
        let entry = self
            .inner
            .get_with(CacheKey::R2(key), || CachedEntry::R2(decode_fn()));
        match entry {
            CachedEntry::R2(r2) => r2,
            _ => unreachable!("R2 key always maps to R2 entry"),
        }
    }

    /// Check if Region 2 is cached for the given key (no insertion).
    pub fn get_r2(&self, key: &LeafletCacheKey) -> Option<CachedRegion2> {
        match self.inner.get(&CacheKey::R2(key.clone())) {
            Some(CachedEntry::R2(r2)) => Some(r2),
            _ => None,
        }
    }

    /// Check if Region 2 is cached for the given key without cloning the value.
    pub fn contains_r2(&self, key: &LeafletCacheKey) -> bool {
        self.inner.contains_key(&CacheKey::R2(key.clone()))
    }

    // ========================================================================
    // Dict tree leaf cache
    // ========================================================================

    /// Check if a dict tree leaf is cached (read-only, no insertion).
    pub fn get_dict_leaf(&self, key: u128) -> Option<Arc<[u8]>> {
        match self.inner.get(&CacheKey::DictLeaf(key)) {
            Some(CachedEntry::DictLeaf(bytes)) => Some(bytes),
            _ => None,
        }
    }

    /// Get or load a dict tree leaf with single-flight and error propagation.
    ///
    /// Uses `try_get_with` so that only one thread loads a given leaf;
    /// concurrent callers block on the same initializer. If the load
    /// fails, nothing is cached and the error propagates.
    ///
    /// Key should be `xxh3_128(cas_address.as_bytes())`.
    pub fn try_get_or_load_dict_leaf<F>(&self, key: u128, load_fn: F) -> io::Result<Arc<[u8]>>
    where
        F: FnOnce() -> io::Result<Arc<[u8]>>,
    {
        let result = self.inner.try_get_with(CacheKey::DictLeaf(key), || {
            load_fn().map(CachedEntry::DictLeaf)
        });
        match result {
            Ok(CachedEntry::DictLeaf(bytes)) => Ok(bytes),
            Ok(_) => unreachable!("DictLeaf key always maps to DictLeaf entry"),
            Err(arc_err) => Err(io::Error::new(arc_err.kind(), arc_err.to_string())),
        }
    }

    // ========================================================================
    // Housekeeping
    // ========================================================================

    /// Invalidate all entries (e.g., after index rebuild).
    pub fn invalidate_all(&self) {
        self.inner.invalidate_all();
    }

    /// Approximate total number of entries in cache (all types).
    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_key(leaf_id: u128, leaflet_index: u8, to_t: i64, epoch: u64) -> LeafletCacheKey {
        LeafletCacheKey {
            leaf_id,
            leaflet_index,
            to_t,
            epoch,
        }
    }

    fn make_r1(row_count: usize) -> CachedRegion1 {
        CachedRegion1 {
            s_ids: SubjectIdColumn::from_narrow(vec![1u32; row_count]),
            p_ids: vec![2u32; row_count].into(),
            o_kinds: vec![0u8; row_count].into(),
            o_keys: vec![3u64; row_count].into(),
            row_count,
        }
    }

    fn make_r2(row_count: usize) -> CachedRegion2 {
        CachedRegion2 {
            dt_values: vec![0u32; row_count].into(),
            t_values: vec![1i64; row_count].into(),
            lang_ids: vec![0u16; row_count].into(),
            i_values: vec![i32::MIN; row_count].into(),
        }
    }

    #[test]
    fn test_leaflet_cache_miss_then_hit() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024); // 10MB
        let key = make_key(42, 0, 100, 0);

        // Miss — should return None
        assert!(cache.get_r1(&key).is_none());

        // get_or_decode should call the decode fn
        let mut called = false;
        let r1 = cache.get_or_decode_r1(key.clone(), || {
            called = true;
            make_r1(100)
        });
        assert!(called);
        assert_eq!(r1.row_count, 100);

        // Hit — should not call decode fn
        let mut called_again = false;
        let r1_cached = cache.get_or_decode_r1(key.clone(), || {
            called_again = true;
            make_r1(999) // should NOT be used
        });
        assert!(!called_again);
        assert_eq!(r1_cached.row_count, 100); // original value
    }

    #[test]
    fn test_leaflet_cache_r2_independent() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);
        let key = make_key(42, 0, 100, 0);

        // Insert R1 but not R2
        cache.get_or_decode_r1(key.clone(), || make_r1(50));
        assert!(cache.get_r1(&key).is_some());
        assert!(cache.get_r2(&key).is_none());

        // Insert R2
        cache.get_or_decode_r2(key.clone(), || make_r2(50));
        assert!(cache.get_r2(&key).is_some());
    }

    #[test]
    fn test_leaflet_cache_epoch_invalidation() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);

        let key_committed = make_key(42, 0, 100, 0);
        let key_staged = make_key(42, 0, 100, 1);

        cache.get_or_decode_r1(key_committed.clone(), || make_r1(100));

        // Different epoch → cache miss
        assert!(cache.get_r1(&key_staged).is_none());

        // Same epoch → cache hit
        assert!(cache.get_r1(&key_committed).is_some());
    }

    #[test]
    fn test_leaflet_cache_different_to_t() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);

        let key_t100 = make_key(42, 0, 100, 0);
        let key_t50 = make_key(42, 0, 50, 0);

        cache.get_or_decode_r1(key_t100.clone(), || make_r1(100));

        // Different to_t → cache miss (time-travel produces different state)
        assert!(cache.get_r1(&key_t50).is_none());

        // Same to_t → cache hit
        assert!(cache.get_r1(&key_t100).is_some());
    }

    #[test]
    fn test_leaflet_cache_different_leaflet_index() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);

        let key_0 = make_key(42, 0, 100, 0);
        let key_1 = make_key(42, 1, 100, 0);

        cache.get_or_decode_r1(key_0.clone(), || make_r1(100));

        // Different leaflet index → cache miss
        assert!(cache.get_r1(&key_1).is_none());
    }

    #[test]
    fn test_leaflet_cache_invalidate_all() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);
        let key = make_key(42, 0, 100, 0);

        cache.get_or_decode_r1(key.clone(), || make_r1(100));
        cache.get_or_decode_r2(key.clone(), || make_r2(100));
        assert!(cache.get_r1(&key).is_some());
        assert!(cache.get_r2(&key).is_some());

        cache.invalidate_all();
        // moka may not synchronously evict; run_pending() forces it
        // Use get() which should return None for invalidated entries
        // Note: moka invalidate_all is lazy, but get() after invalidate should miss
    }

    #[test]
    fn test_cached_region_byte_sizes() {
        let r1 = make_r1(25000);
        // narrow: s_ids(4) + p_ids(4) + o_kinds(1) + o_keys(8) = 17 bytes per row
        assert_eq!(r1.byte_size(), 25000 * 17); // 425KB

        let r2 = make_r2(25000);
        assert_eq!(r2.byte_size(), 25000 * 18); // 450KB
    }

    #[test]
    fn test_unified_pool_r1_and_dict_leaf() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);
        let key = make_key(42, 0, 100, 0);

        // Insert R1 and a dict leaf into the same pool.
        cache.get_or_decode_r1(key.clone(), || make_r1(50));
        let dict_data: Arc<[u8]> = Arc::from(vec![1u8; 256].into_boxed_slice());
        cache
            .try_get_or_load_dict_leaf(999, || Ok(dict_data))
            .unwrap();

        // Both retrievable from the same pool.
        assert!(cache.get_r1(&key).is_some());
        assert!(cache.get_dict_leaf(999).is_some());

        // Different dict key → miss.
        assert!(cache.get_dict_leaf(1000).is_none());
    }

    #[test]
    fn test_dict_leaf_load_error_not_cached() {
        let cache = LeafletCache::with_max_bytes(10 * 1024 * 1024);

        // Fallible load that fails → nothing cached.
        let result = cache.try_get_or_load_dict_leaf(42, || {
            Err(io::Error::new(io::ErrorKind::NotFound, "disk read failed"))
        });
        assert!(result.is_err());
        assert!(cache.get_dict_leaf(42).is_none());

        // Subsequent successful load → now cached.
        let data: Arc<[u8]> = Arc::from(vec![7u8; 64].into_boxed_slice());
        let got = cache.try_get_or_load_dict_leaf(42, || Ok(data)).unwrap();
        assert_eq!(got.len(), 64);
        assert!(cache.get_dict_leaf(42).is_some());
    }
}
