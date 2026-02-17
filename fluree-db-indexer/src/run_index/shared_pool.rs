//! Thread-safe shared pools for vector and numbig arenas (Tier 2 parallel import).
//!
//! During parallel import, multiple parse workers may encounter vector or
//! BigInt/BigDecimal values for the same predicate. These pools provide
//! thread-safe access to per-predicate arenas with global handle assignment.
//!
//! Since predicate IDs (`p_id`) are globally assigned via
//! [`SharedDictAllocator`](super::global_dict::SharedDictAllocator),
//! the arena key is stable across workers.
//!
//! Contention is low because:
//! - Different predicates have different arenas (different `Mutex`)
//! - Vectors and big numbers are relatively rare in most datasets

use super::numbig_dict::NumBigArena;
use super::vector_arena::VectorArena;
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use parking_lot::{Mutex, RwLock};
use rustc_hash::FxHashMap;
use std::sync::Arc;

// ============================================================================
// SharedVectorArenaPool
// ============================================================================

/// Thread-safe pool of per-predicate vector arenas.
///
/// Handles returned by insert operations are globally unique within each
/// predicate's arena — no remap needed during the merge phase.
pub struct SharedVectorArenaPool {
    arenas: RwLock<FxHashMap<u32, Arc<Mutex<VectorArena>>>>,
}

impl SharedVectorArenaPool {
    /// Create an empty pool.
    pub fn new() -> Self {
        Self {
            arenas: RwLock::new(FxHashMap::default()),
        }
    }

    /// Insert an f32 vector for the given predicate.
    ///
    /// Creates the arena on first access. Returns a global handle.
    pub fn insert_f32(&self, p_id: u32, vec: &[f32]) -> Result<u32, String> {
        let arena = self.get_or_create(p_id);
        let mut guard = arena.lock();
        guard.insert_f32(vec)
    }

    /// Insert an f64 vector for the given predicate (downcasts to f32).
    pub fn insert_f64(&self, p_id: u32, vec: &[f64]) -> Result<u32, String> {
        let arena = self.get_or_create(p_id);
        let mut guard = arena.lock();
        guard.insert_f64(vec)
    }

    /// Consume the pool and return the per-predicate arenas.
    ///
    /// Used after import completes for persistence / CAS upload.
    pub fn into_arenas(self) -> FxHashMap<u32, VectorArena> {
        let map = self.arenas.into_inner();
        map.into_iter()
            .map(|(p_id, arc_mutex)| {
                let arena = Arc::try_unwrap(arc_mutex)
                    .expect("SharedVectorArenaPool::into_arenas called while arenas still shared")
                    .into_inner();
                (p_id, arena)
            })
            .collect()
    }

    /// Number of predicates with vector arenas.
    pub fn predicate_count(&self) -> usize {
        self.arenas.read().len()
    }

    fn get_or_create(&self, p_id: u32) -> Arc<Mutex<VectorArena>> {
        // Fast path: read lock
        {
            let arenas = self.arenas.read();
            if let Some(arena) = arenas.get(&p_id) {
                return Arc::clone(arena);
            }
        }
        // Slow path: write lock with double-check
        let mut arenas = self.arenas.write();
        Arc::clone(
            arenas
                .entry(p_id)
                .or_insert_with(|| Arc::new(Mutex::new(VectorArena::new()))),
        )
    }
}

impl Default for SharedVectorArenaPool {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// SharedNumBigPool
// ============================================================================

/// Thread-safe pool of per-predicate numbig arenas.
///
/// Handles returned by insert operations are globally unique within each
/// predicate's arena — no remap needed during the merge phase.
/// Deduplication is handled by the underlying `NumBigArena`.
pub struct SharedNumBigPool {
    arenas: RwLock<FxHashMap<u32, Arc<Mutex<NumBigArena>>>>,
}

impl SharedNumBigPool {
    /// Create an empty pool.
    pub fn new() -> Self {
        Self {
            arenas: RwLock::new(FxHashMap::default()),
        }
    }

    /// Insert a BigInt for the given predicate. Returns a global handle.
    ///
    /// Creates the arena on first access. Deduplicates within the arena
    /// (same BigInt value always returns the same handle).
    pub fn get_or_insert_bigint(&self, p_id: u32, bi: &BigInt) -> u32 {
        let arena = self.get_or_create(p_id);
        let mut guard = arena.lock();
        guard.get_or_insert_bigint(bi)
    }

    /// Insert a BigDecimal for the given predicate. Returns a global handle.
    pub fn get_or_insert_bigdec(&self, p_id: u32, bd: &BigDecimal) -> u32 {
        let arena = self.get_or_create(p_id);
        let mut guard = arena.lock();
        guard.get_or_insert_bigdec(bd)
    }

    /// Consume the pool and return the per-predicate arenas.
    pub fn into_arenas(self) -> FxHashMap<u32, NumBigArena> {
        let map = self.arenas.into_inner();
        map.into_iter()
            .map(|(p_id, arc_mutex)| {
                let arena = Arc::try_unwrap(arc_mutex)
                    .unwrap_or_else(|_| {
                        panic!("SharedNumBigPool::into_arenas called while arenas still shared")
                    })
                    .into_inner();
                (p_id, arena)
            })
            .collect()
    }

    /// Number of predicates with numbig arenas.
    pub fn predicate_count(&self) -> usize {
        self.arenas.read().len()
    }

    fn get_or_create(&self, p_id: u32) -> Arc<Mutex<NumBigArena>> {
        // Fast path: read lock
        {
            let arenas = self.arenas.read();
            if let Some(arena) = arenas.get(&p_id) {
                return Arc::clone(arena);
            }
        }
        // Slow path: write lock with double-check
        let mut arenas = self.arenas.write();
        Arc::clone(
            arenas
                .entry(p_id)
                .or_insert_with(|| Arc::new(Mutex::new(NumBigArena::new()))),
        )
    }
}

impl Default for SharedNumBigPool {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_pool_basic() {
        let pool = SharedVectorArenaPool::new();

        // Insert vectors for two different predicates
        let h0 = pool.insert_f32(1, &[1.0, 2.0, 3.0]).unwrap();
        let h1 = pool.insert_f32(1, &[4.0, 5.0, 6.0]).unwrap();
        let h2 = pool.insert_f32(2, &[7.0, 8.0, 9.0]).unwrap();

        assert_eq!(h0, 0); // first handle for predicate 1
        assert_eq!(h1, 1); // second handle for predicate 1
        assert_eq!(h2, 0); // first handle for predicate 2 (independent)
        assert_eq!(pool.predicate_count(), 2);
    }

    #[test]
    fn test_vector_pool_concurrent() {
        let pool = Arc::new(SharedVectorArenaPool::new());

        let handles: Vec<_> = (0..4)
            .map(|thread_id| {
                let pool = Arc::clone(&pool);
                std::thread::spawn(move || {
                    let mut results = Vec::new();
                    // Each thread inserts into the same predicate
                    for i in 0..10 {
                        let vec = vec![thread_id as f32 * 100.0 + i as f32; 3];
                        results.push(pool.insert_f32(42, &vec).unwrap());
                    }
                    results
                })
            })
            .collect();

        let all_handles: Vec<u32> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        // All handles should be unique (0..39)
        let unique: std::collections::HashSet<u32> = all_handles.iter().copied().collect();
        assert_eq!(unique.len(), 40);
        assert_eq!(pool.predicate_count(), 1);
    }

    #[test]
    fn test_vector_pool_into_arenas() {
        let pool = SharedVectorArenaPool::new();
        pool.insert_f32(1, &[1.0, 2.0]).unwrap();
        pool.insert_f32(2, &[3.0, 4.0]).unwrap();

        let arenas = pool.into_arenas();
        assert_eq!(arenas.len(), 2);
        assert_eq!(arenas[&1].len(), 1);
        assert_eq!(arenas[&2].len(), 1);
    }

    #[test]
    fn test_numbig_pool_basic() {
        let pool = SharedNumBigPool::new();

        let h0 = pool.get_or_insert_bigint(1, &BigInt::from(42));
        let h1 = pool.get_or_insert_bigint(1, &BigInt::from(99));
        // Dedup: same value returns same handle
        let h0_again = pool.get_or_insert_bigint(1, &BigInt::from(42));

        assert_eq!(h0, 0);
        assert_eq!(h1, 1);
        assert_eq!(h0, h0_again);
        assert_eq!(pool.predicate_count(), 1);
    }

    #[test]
    fn test_numbig_pool_bigdec() {
        let pool = SharedNumBigPool::new();

        let bd: BigDecimal = std::str::FromStr::from_str("3.14159").unwrap();
        let h0 = pool.get_or_insert_bigdec(5, &bd);
        let h0_again = pool.get_or_insert_bigdec(5, &bd);

        assert_eq!(h0, 0);
        assert_eq!(h0, h0_again);
    }

    #[test]
    fn test_numbig_pool_concurrent() {
        let pool = Arc::new(SharedNumBigPool::new());

        let handles: Vec<_> = (0..4)
            .map(|thread_id| {
                let pool = Arc::clone(&pool);
                std::thread::spawn(move || {
                    let mut results = Vec::new();
                    // Each thread inserts unique values
                    for i in 0..10 {
                        let val = BigInt::from(thread_id * 1000 + i);
                        results.push(pool.get_or_insert_bigint(42, &val));
                    }
                    results
                })
            })
            .collect();

        let all_handles: Vec<u32> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        // All handles should be unique (0..39, since all values are unique)
        let unique: std::collections::HashSet<u32> = all_handles.iter().copied().collect();
        assert_eq!(unique.len(), 40);
    }

    #[test]
    fn test_numbig_pool_into_arenas() {
        let pool = SharedNumBigPool::new();
        pool.get_or_insert_bigint(1, &BigInt::from(42));
        pool.get_or_insert_bigint(2, &BigInt::from(99));

        let arenas = pool.into_arenas();
        assert_eq!(arenas.len(), 2);
        assert_eq!(arenas[&1].len(), 1);
        assert_eq!(arenas[&2].len(), 1);
    }
}
