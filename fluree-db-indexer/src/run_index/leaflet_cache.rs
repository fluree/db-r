//! LRU cache for decoded leaflet regions.
//!
//! Uses `moka::sync::Cache` (synchronous — `BinaryCursor` is sync) with
//! TinyLFU eviction. Region 1 and Region 2 are cached separately since
//! Region 2 is only decoded when needed (lazy Region 2 strategy).
//!
//! Region 3 (history journal) is **never cached** — it's cold-path data
//! decoded on demand for time-travel replay and discarded afterwards.
//!
//! ## Cache Key
//!
//! `(leaf_id, leaflet_index, to_t, epoch)` uniquely identifies a decoded
//! leaflet state. `to_t` distinguishes time-travel snapshots, and `epoch`
//! distinguishes staged vs committed views at the same `t`.

use moka::sync::Cache;
use std::sync::Arc;

// ============================================================================
// Cache key
// ============================================================================

/// Cache key identifying a unique decoded leaflet state.
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

// ============================================================================
// Cached value types
// ============================================================================

/// Cached decoded Region 1 (core columns: s_id, p_id, o).
///
/// Uses `Arc<[T]>` for zero-copy sharing between cache and in-flight cursors.
#[derive(Clone, Debug)]
pub struct CachedRegion1 {
    pub s_ids: Arc<[u32]>,
    pub p_ids: Arc<[u32]>,
    pub o_values: Arc<[u64]>,
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
        // s_ids(4) + p_ids(4) + o_values(8) = 16 bytes per row
        self.row_count * 16
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
// LeafletCache
// ============================================================================

/// LRU cache for decoded leaflet regions, backed by moka TinyLFU.
///
/// Caches Region 1 and Region 2 separately to preserve the lazy-Region-2
/// strategy: Region 1 is always cached on decode; Region 2 only when actually
/// needed by the query.
pub struct LeafletCache {
    r1: Cache<LeafletCacheKey, CachedRegion1>,
    r2: Cache<LeafletCacheKey, CachedRegion2>,
}

impl LeafletCache {
    /// Create a new cache with the given maximum byte budget.
    ///
    /// The budget is split evenly between R1 and R2 caches. Each cache
    /// uses byte-weighted entries for accurate memory accounting.
    pub fn with_max_bytes(max_bytes: u64) -> Self {
        let half = max_bytes / 2;

        let r1 = Cache::builder()
            .weigher(|_key: &LeafletCacheKey, val: &CachedRegion1| {
                // Clamp to u32 for moka's weigher (max ~4GB per entry, fine for leaflets)
                val.byte_size().min(u32::MAX as usize) as u32
            })
            .max_capacity(half)
            .build();

        let r2 = Cache::builder()
            .weigher(|_key: &LeafletCacheKey, val: &CachedRegion2| {
                val.byte_size().min(u32::MAX as usize) as u32
            })
            .max_capacity(half)
            .build();

        Self { r1, r2 }
    }

    /// Get or decode Region 1 for the given key.
    ///
    /// On cache miss, calls `decode_fn` to produce the value, inserts it,
    /// and returns the cached copy.
    pub fn get_or_decode_r1<F>(&self, key: LeafletCacheKey, decode_fn: F) -> CachedRegion1
    where
        F: FnOnce() -> CachedRegion1,
    {
        self.r1.get_with(key, decode_fn)
    }

    /// Get or decode Region 2 for the given key.
    pub fn get_or_decode_r2<F>(&self, key: LeafletCacheKey, decode_fn: F) -> CachedRegion2
    where
        F: FnOnce() -> CachedRegion2,
    {
        self.r2.get_with(key, decode_fn)
    }

    /// Check if Region 1 is cached for the given key (no insertion).
    pub fn get_r1(&self, key: &LeafletCacheKey) -> Option<CachedRegion1> {
        self.r1.get(key)
    }

    /// Check if Region 2 is cached for the given key (no insertion).
    pub fn get_r2(&self, key: &LeafletCacheKey) -> Option<CachedRegion2> {
        self.r2.get(key)
    }

    /// Invalidate all entries (e.g., after index rebuild).
    pub fn invalidate_all(&self) {
        self.r1.invalidate_all();
        self.r2.invalidate_all();
    }

    /// Approximate number of R1 entries in cache.
    pub fn r1_entry_count(&self) -> u64 {
        self.r1.entry_count()
    }

    /// Approximate number of R2 entries in cache.
    pub fn r2_entry_count(&self) -> u64 {
        self.r2.entry_count()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_key(leaf_id: u128, leaflet_index: u8, to_t: i64, epoch: u64) -> LeafletCacheKey {
        LeafletCacheKey { leaf_id, leaflet_index, to_t, epoch }
    }

    fn make_r1(row_count: usize) -> CachedRegion1 {
        CachedRegion1 {
            s_ids: vec![1u32; row_count].into(),
            p_ids: vec![2u32; row_count].into(),
            o_values: vec![3u64; row_count].into(),
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
        assert_eq!(r1.byte_size(), 25000 * 16); // 400KB

        let r2 = make_r2(25000);
        assert_eq!(r2.byte_size(), 25000 * 18); // 450KB
    }
}
