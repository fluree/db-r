//! Namespace registry for IRI prefix code allocation
//!
//! This module provides `NamespaceRegistry` for managing namespace codes
//! during transaction processing. New IRIs with unknown prefixes get new
//! codes allocated, and these allocations are tracked for persistence
//! in the commit record.
//!
//! For parallel import, [`SharedNamespaceAllocator`] provides thread-safe
//! allocation with [`WorkerCache`] for lock-free per-worker lookups.
//! [`NsAllocator`] abstracts over both single-threaded and parallel modes.
//!
//! ## Predefined Namespace Codes
//!
//! Fluree uses predefined codes for common namespaces to ensure compatibility
//! with existing databases. User-supplied namespaces start at `USER_START`.

use fluree_db_core::{LedgerSnapshot, PrefixTrie, Sid};
use fluree_vocab::namespaces::{BLANK_NODE, OVERFLOW, USER_START};
use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

/// Fallback split mode used when no registered prefix matches an IRI.
///
/// Default preserves existing behavior (`LastSlashOrHash`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NsFallbackMode {
    /// Split on the last `/` or `#` (prefix inclusive).
    LastSlashOrHash = 0,
    /// Coarse, outlier-oriented split:
    ///
    /// - Prefer declared prefixes via trie (unchanged).
    /// - Otherwise:
    ///   - http(s): allocate at most *two* path segments deep when the 2nd segment
    ///     looks like a stable bucket (e.g., `.../pid/69/`), else at one segment
    ///     deep (`.../rec/`, `.../db/`, `.../10.1007/`).
    ///   - non-http(s) with `:` but no `/` or `#`: split at the 2nd `:` when present
    ///     (e.g., `urn:isbn:`), else at the 1st `:`.
    ///
    /// This is intended to prevent namespace-code explosion for rare datasets
    /// with highly-segmented IRIs (e.g. DBLP).
    CoarseHeuristic = 1,
    /// Ultra-coarse fallback intended as a “fallback to the fallback” when
    /// the coarse heuristic still yields too many distinct namespace codes.
    ///
    /// - Prefer declared prefixes via trie (unchanged).
    /// - Otherwise:
    ///   - http(s): `scheme://host/`
    ///   - non-http(s) with `:` but no `/` or `#`: split at the 1st `:`
    ///   - else: last-slash-or-hash
    HostOnly = 2,
}

impl NsFallbackMode {
    #[inline]
    fn from_u8(v: u8) -> Self {
        match v {
            1 => NsFallbackMode::CoarseHeuristic,
            2 => NsFallbackMode::HostOnly,
            _ => NsFallbackMode::LastSlashOrHash,
        }
    }

    #[inline]
    fn as_u8(self) -> u8 {
        self as u8
    }
}

/// First code available for user-defined namespaces.
/// Re-exported from `fluree_vocab::namespaces::USER_START`.
pub const USER_NS_START: u16 = USER_START;

/// Blank node prefix (standard RDF blank node syntax)
pub const BLANK_NODE_PREFIX: &str = "_:";

/// Fluree blank node ID prefix (used in generated blank node names)
pub const BLANK_NODE_ID_PREFIX: &str = "fdb";

/// Build the default namespace mappings (single source of truth: `fluree-db-core`).
fn default_namespaces() -> HashMap<u16, String> {
    fluree_db_core::default_namespace_codes()
}

// ============================================================================
// NamespaceRegistry (single-threaded, used by serial import + transact paths)
// ============================================================================

/// Registry for namespace prefix codes
///
/// During transaction processing, new IRIs may introduce prefixes not yet
/// in the database's namespace table. This registry:
/// 1. Starts with predefined codes for common namespaces
/// 2. Loads existing codes from `snapshot.namespace_codes`
/// 3. Allocates new codes as needed (starting at USER_NS_START)
/// 4. Tracks new allocations in `delta` for commit persistence
///
/// Prefix lookups use a byte-level trie for O(len(iri)) longest-prefix
/// matching, independent of the number of registered namespaces.
#[derive(Debug, Clone)]
pub struct NamespaceRegistry {
    /// Prefix → code mapping (for exact lookups in get_or_allocate)
    codes: HashMap<String, u16>,

    /// Code → prefix mapping (for encoding, matches LedgerSnapshot.namespace_codes)
    names: HashMap<u16, String>,

    /// Next available code for allocation (>= USER_NS_START)
    next_code: u16,

    /// New allocations this transaction (code → prefix)
    pub(crate) delta: HashMap<u16, String>,

    /// Byte-level trie for O(len(iri)) longest-prefix matching.
    /// The empty prefix (code 0) is NOT stored in the trie — unmatched
    /// IRIs fall through to the split heuristic which may allocate it.
    trie: PrefixTrie,

    /// Fallback mode used only when no trie prefix matches.
    ///
    /// For “namespace explosion” datasets, this can be upgraded (persistently)
    /// by inferring from the DB state in `from_db()`.
    fallback_mode: NsFallbackMode,
}

impl NamespaceRegistry {
    /// Create a new registry with default namespaces
    pub fn new() -> Self {
        let names = default_namespaces();
        let codes: HashMap<String, u16> = names
            .iter()
            .map(|(code, prefix)| (prefix.clone(), *code))
            .collect();

        let mut trie = PrefixTrie::new();
        for (prefix, &code) in &codes {
            if !prefix.is_empty() {
                trie.insert(prefix, code);
            }
        }

        Self {
            codes,
            names,
            next_code: USER_NS_START,
            delta: HashMap::new(),
            trie,
            fallback_mode: NsFallbackMode::LastSlashOrHash,
        }
    }

    /// Create a registry seeded from a database's namespace codes
    ///
    /// This merges the database's codes with the predefined defaults,
    /// with the database taking precedence for any conflicts.
    pub fn from_db(snapshot: &LedgerSnapshot) -> Self {
        // Start with defaults
        let mut names = default_namespaces();

        // Merge in database codes (overwriting defaults if needed)
        for (code, prefix) in &snapshot.namespace_codes {
            names.insert(*code, prefix.clone());
        }

        let codes: HashMap<String, u16> = names
            .iter()
            .map(|(code, prefix)| (prefix.clone(), *code))
            .collect();

        // Next code is max of existing non-sentinel codes + 1, but at least USER_NS_START.
        // Exclude OVERFLOW from max_code since it's a sentinel, not an allocated code.
        let max_code = names
            .keys()
            .filter(|&&c| c < OVERFLOW)
            .max()
            .copied()
            .unwrap_or(0);
        let next_code = (max_code + 1).max(USER_NS_START);

        let mut trie = PrefixTrie::new();
        for (prefix, &code) in &codes {
            if !prefix.is_empty() {
                trie.insert(prefix, code);
            }
        }

        // Persisted behavior: once a DB has already crossed the u8-ish namespace budget,
        // keep future allocations at host-only granularity for unseen hosts to prevent
        // regression back to last-slash splitting in later transactions.
        let fallback_mode = if max_code > u8::MAX as u16 {
            NsFallbackMode::HostOnly
        } else {
            NsFallbackMode::LastSlashOrHash
        };

        Self {
            codes,
            names,
            next_code,
            delta: HashMap::new(),
            trie,
            fallback_mode,
        }
    }

    /// Set the fallback split mode for unknown IRIs.
    #[inline]
    pub fn set_fallback_mode(&mut self, mode: NsFallbackMode) {
        self.fallback_mode = mode;
    }

    /// Get the current fallback split mode.
    #[inline]
    pub fn fallback_mode(&self) -> NsFallbackMode {
        self.fallback_mode
    }

    /// Get the code for a prefix, allocating a new one if needed.
    ///
    /// If the prefix is not yet registered, a new code is allocated
    /// and recorded in the delta for persistence. If all codes in
    /// `USER_START..OVERFLOW` are exhausted, returns `OVERFLOW` as a
    /// pure sentinel — OVERFLOW is never inserted into codes/names/delta/trie.
    /// The caller (`sid_for_iri`) handles overflow by using the full IRI as
    /// the SID name.
    pub fn get_or_allocate(&mut self, prefix: &str) -> u16 {
        if let Some(&code) = self.codes.get(prefix) {
            return code;
        }

        // All codes exhausted — return OVERFLOW sentinel without registering.
        // The caller will use the full IRI as the SID name.
        if self.next_code >= OVERFLOW {
            return OVERFLOW;
        }

        let code = self.next_code;
        self.next_code += 1;

        self.codes.insert(prefix.to_string(), code);
        self.names.insert(code, prefix.to_string());
        self.delta.insert(code, prefix.to_string());

        // Insert into trie (skip empty prefix — handled by split heuristic)
        if !prefix.is_empty() {
            self.trie.insert(prefix, code);
        }

        code
    }

    /// Get the namespace code for blank nodes.
    pub fn blank_node_code(&self) -> u16 {
        BLANK_NODE
    }

    /// Look up a code without allocating
    pub fn get_code(&self, prefix: &str) -> Option<u16> {
        self.codes.get(prefix).copied()
    }

    /// Look up a prefix by code
    pub fn get_prefix(&self, code: u16) -> Option<&str> {
        self.names.get(&code).map(|s| s.as_str())
    }

    /// Check if a prefix is registered
    pub fn has_prefix(&self, prefix: &str) -> bool {
        self.codes.contains_key(prefix)
    }

    /// Number of registered namespace codes (predefined + user-allocated).
    pub fn code_count(&self) -> usize {
        self.codes.len()
    }

    /// Returns the set of all registered namespace codes (numeric values).
    pub fn all_codes(&self) -> FxHashSet<u16> {
        self.codes.values().copied().collect()
    }

    /// Take the delta (new allocations) and reset it
    ///
    /// Returns the map of new allocations (code → prefix) for
    /// inclusion in the commit record.
    pub fn take_delta(&mut self) -> HashMap<u16, String> {
        std::mem::take(&mut self.delta)
    }

    /// Get a reference to the delta without consuming it
    pub fn delta(&self) -> &HashMap<u16, String> {
        &self.delta
    }

    /// Check if there are any new allocations
    pub fn has_delta(&self) -> bool {
        !self.delta.is_empty()
    }

    /// Create a Sid for an IRI, allocating namespace code if needed
    ///
    /// Uses a byte-level trie for O(len(iri)) longest-prefix matching,
    /// independent of the number of registered namespaces. Falls back to
    /// splitting on the last `/` or `#` for IRIs with no matching prefix.
    ///
    /// When namespace codes are exhausted (`get_or_allocate` returns `OVERFLOW`),
    /// the full IRI is used as the SID name — no prefix splitting.
    pub fn sid_for_iri(&mut self, iri: &str) -> Sid {
        // Trie lookup: O(len(iri)), handles nested prefixes correctly
        if let Some((code, prefix_len)) = self.trie.longest_match(iri) {
            return Sid::new(code, &iri[prefix_len..]);
        }

        // No known prefix matched; fall back to configured split heuristic for new prefixes.
        let (prefix, local) = split_iri_fallback(iri, self.fallback_mode);

        let code = self.get_or_allocate(prefix);
        if code == OVERFLOW {
            // Overflow: store the full IRI as name, no prefix splitting
            Sid::new(OVERFLOW, iri)
        } else {
            Sid::new(code, local)
        }
    }

    /// Register a namespace code if not already present.
    ///
    /// Used to merge allocations from the shared allocator back into the
    /// serial registry (e.g., after commit-order publication). If the prefix
    /// is already registered (under any code), this is a no-op. OVERFLOW codes
    /// are ignored since they are pure sentinels and should never be registered.
    pub fn ensure_code(&mut self, code: u16, prefix: &str) {
        if code >= OVERFLOW {
            return; // OVERFLOW is a sentinel, never register it
        }
        if self.codes.contains_key(prefix) {
            return;
        }
        self.codes.insert(prefix.to_string(), code);
        self.names.insert(code, prefix.to_string());
        self.delta.insert(code, prefix.to_string());
        if !prefix.is_empty() {
            self.trie.insert(prefix, code);
        }
        if code >= self.next_code {
            self.next_code = code + 1;
        }
    }

    /// Create a Sid for a blank node.
    ///
    /// Blank nodes use the predefined `BLANK_NODE` namespace code and generate
    /// a unique local name in the format: `fdb-{unique_id}`
    ///
    /// The `unique_id` should be globally unique (e.g., timestamp + random suffix).
    /// The returned IRI will look like `_:fdb-1672531200000-a1b2c3d4`.
    pub fn blank_node_sid(&self, unique_id: &str) -> Sid {
        let local = format!("{}-{}", BLANK_NODE_ID_PREFIX, unique_id);
        Sid::new(BLANK_NODE, local)
    }
}

impl Default for NamespaceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// SharedNamespaceAllocator (thread-safe, for parallel import)
// ============================================================================

/// Internal state for the shared allocator, protected by RwLock.
struct SharedAllocInner {
    codes: HashMap<String, u16>,
    names: HashMap<u16, String>,
    next_code: u16,
    trie: PrefixTrie,
}

/// Thread-safe namespace allocator shared across parallel parse workers.
///
/// Allocation is concurrent via `RwLock`. Publication (which codes go into
/// which commit's `namespace_delta`) is handled separately in commit-order
/// by the serial finalizer.
///
/// ## Invariant
///
/// For each finalized commit at t, every `ns_code` referenced by its ops
/// must be resolvable from the cumulative `namespace_delta`s up to and
/// including commit t. This is enforced by the commit-order publication
/// logic in the import orchestrator, NOT by this allocator.
pub struct SharedNamespaceAllocator {
    inner: RwLock<SharedAllocInner>,
    fallback_mode: AtomicU8,
}

impl SharedNamespaceAllocator {
    /// Create a shared allocator seeded from an existing `NamespaceRegistry`.
    ///
    /// Used after chunk 0 has been parsed serially to establish the initial
    /// namespace mappings.
    pub fn from_registry(reg: &NamespaceRegistry) -> Self {
        Self {
            inner: RwLock::new(SharedAllocInner {
                codes: reg.codes.clone(),
                names: reg.names.clone(),
                next_code: reg.next_code,
                trie: reg.trie.clone(),
            }),
            fallback_mode: AtomicU8::new(NsFallbackMode::LastSlashOrHash.as_u8()),
        }
    }

    /// Set the fallback split mode for unknown IRIs.
    ///
    /// This is an atomic update and is safe to call while parse workers are running.
    pub fn set_fallback_mode(&self, mode: NsFallbackMode) {
        self.fallback_mode.store(mode.as_u8(), Ordering::Relaxed);
    }

    /// Get the current fallback split mode.
    #[inline]
    pub fn fallback_mode(&self) -> NsFallbackMode {
        NsFallbackMode::from_u8(self.fallback_mode.load(Ordering::Relaxed))
    }

    /// Thread-safe get-or-allocate with read-lock fast path.
    ///
    /// Returns `OVERFLOW` sentinel when codes are exhausted (never registers it).
    pub fn get_or_allocate(&self, prefix: &str) -> u16 {
        // Fast path: read lock
        {
            let inner = self.inner.read();
            if let Some(&code) = inner.codes.get(prefix) {
                return code;
            }
        }
        // Slow path: write lock with double-check
        let mut inner = self.inner.write();
        if let Some(&code) = inner.codes.get(prefix) {
            return code;
        }
        if inner.next_code >= OVERFLOW {
            return OVERFLOW;
        }
        let code = inner.next_code;
        inner.next_code += 1;
        inner.codes.insert(prefix.to_string(), code);
        inner.names.insert(code, prefix.to_string());
        if !prefix.is_empty() {
            inner.trie.insert(prefix, code);
        }

        // Fallback-to-fallback: if we enabled the coarse heuristic but it still
        // allocates beyond the u8 namespace-code budget, collapse future unknown
        // IRIs to host-only to avoid runaway prefix growth.
        //
        // IMPORTANT: This does not affect IRIs that match declared prefixes;
        // those continue to win via trie longest-match.
        if code > u8::MAX as u16
            && self.fallback_mode.load(Ordering::Relaxed) == NsFallbackMode::CoarseHeuristic.as_u8()
        {
            self.fallback_mode
                .store(NsFallbackMode::HostOnly.as_u8(), Ordering::Relaxed);
            tracing::info!(
                allocated_ns_code = code,
                "coarse namespace fallback exceeded u8 budget; switching to host-only fallback"
            );
        }
        code
    }

    /// Thread-safe SID resolution using the internal trie.
    ///
    /// Same logic as `NamespaceRegistry::sid_for_iri` but uses the RwLock.
    pub fn sid_for_iri(&self, iri: &str) -> Sid {
        // Trie lookup under read lock
        {
            let inner = self.inner.read();
            if let Some((code, prefix_len)) = inner.trie.longest_match(iri) {
                return Sid::new(code, &iri[prefix_len..]);
            }
        }
        // No match — split heuristic + allocate
        let (prefix, local) = split_iri_fallback(iri, self.fallback_mode());
        let code = self.get_or_allocate(prefix);
        if code == OVERFLOW {
            Sid::new(OVERFLOW, iri)
        } else {
            Sid::new(code, local)
        }
    }

    /// Create a Sid for a blank node (no lock needed — BLANK_NODE is predefined).
    pub fn blank_node_sid(&self, unique_id: &str) -> Sid {
        let local = format!("{}-{}", BLANK_NODE_ID_PREFIX, unique_id);
        Sid::new(BLANK_NODE, local)
    }

    /// Batch lookup of code→prefix mappings for publication.
    ///
    /// Returns mappings for all requested codes. Debug-asserts that every
    /// requested code is found (a missing code indicates a bug). Never
    /// returns OVERFLOW.
    pub fn lookup_codes(&self, codes: &FxHashSet<u16>) -> HashMap<u16, String> {
        let inner = self.inner.read();
        let mut result = HashMap::with_capacity(codes.len());
        for &code in codes {
            debug_assert!(code < OVERFLOW, "OVERFLOW must never be published");
            if let Some(prefix) = inner.names.get(&code) {
                result.insert(code, prefix.clone());
            } else {
                debug_assert!(false, "code {} not found in shared allocator", code);
                tracing::warn!(code, "namespace code not found in shared allocator");
            }
        }
        result
    }

    /// Look up the prefix string for a namespace code.
    ///
    /// Returns `None` if the code is not registered (should not happen for
    /// codes returned by `get_or_allocate` or `sid_for_iri`).
    pub fn get_prefix(&self, code: u16) -> Option<String> {
        self.inner.read().names.get(&code).cloned()
    }

    /// Synchronize codes from a `NamespaceRegistry` into this allocator.
    ///
    /// Used after serial import paths where namespace codes were allocated
    /// in the registry but not in the shared allocator. Preserves exact code
    /// assignments from the registry.
    pub fn sync_from_registry(&self, reg: &NamespaceRegistry) {
        let mut inner = self.inner.write();
        for (prefix, &code) in &reg.codes {
            if let Some(&existing_code) = inner.codes.get(prefix.as_str()) {
                debug_assert_eq!(
                    existing_code, code,
                    "namespace code conflict for prefix {:?}: shared has {}, registry has {}",
                    prefix, existing_code, code
                );
            } else if let Some(existing_prefix) = inner.names.get(&code) {
                debug_assert_eq!(
                    existing_prefix, prefix,
                    "namespace code {} conflict: shared has {:?}, registry has {:?}",
                    code, existing_prefix, prefix
                );
            } else {
                inner.codes.insert(prefix.clone(), code);
                inner.names.insert(code, prefix.clone());
                if !prefix.is_empty() {
                    inner.trie.insert(prefix, code);
                }
                if code >= inner.next_code {
                    inner.next_code = code + 1;
                }
            }
        }
    }

    /// Take a snapshot of the current state for worker initialization.
    ///
    /// Returns `(codes, trie, next_code)`. Workers use the trie for local
    /// lookups and `next_code` to identify which codes were allocated after
    /// the snapshot.
    pub fn snapshot(&self) -> (FxHashMap<String, u16>, PrefixTrie, u16) {
        let inner = self.inner.read();
        let codes: FxHashMap<String, u16> =
            inner.codes.iter().map(|(k, &v)| (k.clone(), v)).collect();
        (codes, inner.trie.clone(), inner.next_code)
    }
}

// ============================================================================
// WorkerCache (per-worker, lock-free lookups with local trie snapshot)
// ============================================================================

/// Per-worker namespace cache with a local trie snapshot.
///
/// Created at worker spawn time from a snapshot of [`SharedNamespaceAllocator`].
/// All `sid_for_iri()` calls use the local trie (no lock). Only genuinely
/// new prefix allocations touch the shared allocator, and the local trie
/// is updated immediately afterward.
///
/// Tracks `new_codes`: codes first observed by this worker that were not in
/// the initial snapshot (`code >= snapshot_next_code`). This includes codes
/// allocated by OTHER workers if this worker uses them — because if this
/// chunk's ops reference a code, the commit must publish it if not already
/// published by a prior commit.
pub struct WorkerCache {
    alloc: Arc<SharedNamespaceAllocator>,
    /// Local copy of prefix→code map (for exact lookups in get_or_allocate).
    local_codes: FxHashMap<String, u16>,
    /// Local copy of the trie (for longest-prefix match).
    local_trie: PrefixTrie,
    /// The allocator's `next_code` at snapshot time. Any code >= this value
    /// was allocated after the snapshot and might need publishing.
    snapshot_next_code: u16,
    /// Codes first observed after snapshot (code >= snapshot_next_code, < OVERFLOW).
    new_codes: FxHashSet<u16>,
}

impl WorkerCache {
    /// Create a new worker cache from a snapshot of the shared allocator.
    pub fn new(alloc: Arc<SharedNamespaceAllocator>) -> Self {
        let (local_codes, local_trie, snapshot_next_code) = alloc.snapshot();
        Self {
            alloc,
            local_codes,
            local_trie,
            snapshot_next_code,
            new_codes: FxHashSet::default(),
        }
    }

    /// Resolve an IRI to a SID using the local trie (no lock on hot path).
    ///
    /// Falls back to the shared allocator only when a genuinely new prefix
    /// must be allocated.
    pub fn sid_for_iri(&mut self, iri: &str) -> Sid {
        // Local trie lookup — no lock
        if let Some((code, prefix_len)) = self.local_trie.longest_match(iri) {
            self.track_code(code);
            return Sid::new(code, &iri[prefix_len..]);
        }

        // No local match — split heuristic + allocate via shared allocator
        let (prefix, local) = split_iri_fallback(iri, self.alloc.fallback_mode());

        let code = self.get_or_allocate(prefix);
        if code == OVERFLOW {
            Sid::new(OVERFLOW, iri)
        } else {
            Sid::new(code, local)
        }
    }

    /// Get or allocate a namespace code. Checks local cache first (no lock).
    pub fn get_or_allocate(&mut self, prefix: &str) -> u16 {
        // Local fast path
        if let Some(&code) = self.local_codes.get(prefix) {
            self.track_code(code);
            return code;
        }

        // Shared allocator (may lock)
        let code = self.alloc.get_or_allocate(prefix);

        // Update local state
        if code < OVERFLOW {
            self.local_codes.insert(prefix.to_string(), code);
            if !prefix.is_empty() {
                self.local_trie.insert(prefix, code);
            }
            self.track_code(code);
        }

        code
    }

    /// Create a Sid for a blank node (no lock needed).
    pub fn blank_node_sid(&self, unique_id: &str) -> Sid {
        let local = format!("{}-{}", BLANK_NODE_ID_PREFIX, unique_id);
        Sid::new(BLANK_NODE, local)
    }

    /// Consume the cache and return the set of codes first observed after
    /// the snapshot. The serial finalizer uses these for commit-order publication.
    pub fn into_new_codes(self) -> FxHashSet<u16> {
        self.new_codes
    }

    /// Track a code as potentially needing publication if it was allocated
    /// after our snapshot.
    #[inline]
    fn track_code(&mut self, code: u16) {
        if code >= self.snapshot_next_code && code < OVERFLOW {
            self.new_codes.insert(code);
        }
    }
}

// ============================================================================
// Fallback splitting helpers
// ============================================================================

#[inline]
fn split_iri_last_slash_or_hash(iri: &str) -> (&str, &str) {
    let split_pos = iri.rfind(['/', '#']);
    match split_pos {
        Some(pos) => (&iri[..=pos], &iri[pos + 1..]),
        None => ("", iri),
    }
}

#[inline]
fn is_short_digit_bucket(seg: &[u8]) -> bool {
    // Heuristic: small numeric buckets like "69" or "163" are common in DBLP pid paths.
    !seg.is_empty() && seg.len() <= 4 && seg.iter().all(|b| b.is_ascii_digit())
}

#[inline]
fn split_non_http_colonish(iri: &str) -> Option<(&str, &str)> {
    // Only consider colon split when there is no '/' or '#', so we don't fight normal IRIs.
    if iri.contains('/') || iri.contains('#') {
        return None;
    }
    let bytes = iri.as_bytes();
    let mut first = None;
    let mut second = None;
    for (i, &b) in bytes.iter().enumerate() {
        if b == b':' {
            if first.is_none() {
                first = Some(i);
            } else {
                second = Some(i);
                break;
            }
        }
    }
    match (first, second) {
        (Some(_first), Some(second)) => Some((&iri[..=second], &iri[second + 1..])),
        (Some(first), None) => Some((&iri[..=first], &iri[first + 1..])),
        _ => None,
    }
}

#[inline]
fn split_non_http_first_colon(iri: &str) -> Option<(&str, &str)> {
    // Only consider colon split when there is no '/' or '#', so we don't fight normal IRIs.
    if iri.contains('/') || iri.contains('#') {
        return None;
    }
    if let Some(pos) = iri.find(':') {
        Some((&iri[..=pos], &iri[pos + 1..]))
    } else {
        None
    }
}

#[inline]
fn split_iri_coarse_heuristic(iri: &str) -> (&str, &str) {
    let bytes = iri.as_bytes();
    let scheme_len = if bytes.starts_with(b"http://") {
        7
    } else if bytes.starts_with(b"https://") {
        8
    } else {
        // Non-http(s): try colon split (urn/isbn/etc), else legacy.
        return split_non_http_colonish(iri).unwrap_or_else(|| split_iri_last_slash_or_hash(iri));
    };

    // Find end of host (first '/' after scheme).
    let mut host_end = scheme_len;
    while host_end < bytes.len() && bytes[host_end] != b'/' {
        host_end += 1;
    }
    if host_end >= bytes.len() {
        return ("", iri);
    }
    if bytes[host_end] != b'/' {
        return split_iri_last_slash_or_hash(iri);
    }

    // seg1: bytes [host_end+1 .. seg1_end)
    let mut seg1_end = host_end + 1;
    while seg1_end < bytes.len() && bytes[seg1_end] != b'/' {
        seg1_end += 1;
    }
    if seg1_end >= bytes.len() || bytes[seg1_end] != b'/' {
        // No trailing slash after seg1 → host-only.
        return (&iri[..=host_end], &iri[host_end + 1..]);
    }

    let seg1 = &bytes[host_end + 1..seg1_end];

    // seg2 (optional): only use when it looks like a stable small bucket.
    let mut seg2_end = seg1_end + 1;
    while seg2_end < bytes.len() && bytes[seg2_end] != b'/' {
        seg2_end += 1;
    }
    if seg2_end < bytes.len() && bytes[seg2_end] == b'/' {
        let seg2 = &bytes[seg1_end + 1..seg2_end];
        if seg1 == b"pid" && is_short_digit_bucket(seg2) {
            // Use host/seg1/seg2/
            return (&iri[..=seg2_end], &iri[seg2_end + 1..]);
        }
    }

    // Default coarse: host/seg1/
    (&iri[..=seg1_end], &iri[seg1_end + 1..])
}

#[inline]
fn split_iri_host_only(iri: &str) -> (&str, &str) {
    let bytes = iri.as_bytes();
    let scheme_len = if bytes.starts_with(b"http://") {
        7
    } else if bytes.starts_with(b"https://") {
        8
    } else {
        // Non-http(s): split at the first ':' when possible, else legacy.
        return split_non_http_first_colon(iri)
            .unwrap_or_else(|| split_iri_last_slash_or_hash(iri));
    };

    // Find end of host (first '/' after scheme).
    let mut host_end = scheme_len;
    while host_end < bytes.len() && bytes[host_end] != b'/' {
        host_end += 1;
    }
    if host_end >= bytes.len() || bytes[host_end] != b'/' {
        return ("", iri);
    }
    // Host-only prefix includes trailing '/'.
    (&iri[..=host_end], &iri[host_end + 1..])
}

#[inline]
fn split_iri_fallback(iri: &str, mode: NsFallbackMode) -> (&str, &str) {
    match mode {
        NsFallbackMode::LastSlashOrHash => split_iri_last_slash_or_hash(iri),
        NsFallbackMode::CoarseHeuristic => split_iri_coarse_heuristic(iri),
        NsFallbackMode::HostOnly => split_iri_host_only(iri),
    }
}

// ============================================================================
// NsAllocator (enum wrapper abstracting over single-thread / parallel modes)
// ============================================================================

/// Abstraction over namespace allocation for both serial and parallel paths.
///
/// - `Exclusive`: wraps `&mut NamespaceRegistry` for serial paths (transact,
///   chunk 0, TriG).
/// - `Cached`: wraps `&mut WorkerCache` for parallel import workers.
pub enum NsAllocator<'a> {
    Exclusive(&'a mut NamespaceRegistry),
    Cached(&'a mut WorkerCache),
}

impl NsAllocator<'_> {
    /// Resolve an IRI to a SID.
    pub fn sid_for_iri(&mut self, iri: &str) -> Sid {
        match self {
            NsAllocator::Exclusive(reg) => reg.sid_for_iri(iri),
            NsAllocator::Cached(cache) => cache.sid_for_iri(iri),
        }
    }

    /// Get or allocate a namespace code for a prefix.
    pub fn get_or_allocate(&mut self, prefix: &str) -> u16 {
        match self {
            NsAllocator::Exclusive(reg) => reg.get_or_allocate(prefix),
            NsAllocator::Cached(cache) => cache.get_or_allocate(prefix),
        }
    }

    /// Create a Sid for a blank node.
    pub fn blank_node_sid(&self, unique_id: &str) -> Sid {
        match self {
            NsAllocator::Exclusive(reg) => reg.blank_node_sid(unique_id),
            NsAllocator::Cached(cache) => cache.blank_node_sid(unique_id),
        }
    }
}

// ============================================================================
// Free functions
// ============================================================================

/// Generate a unique blank node ID using ULID
///
/// Returns a ULID string. Combined with the `fdb` prefix, this creates
/// IDs like: `_:fdb-01ARZ3NDEKTSV4RRFFQ69G5FAV`
///
/// ULIDs are:
/// - Lexicographically sortable (timestamp-based prefix)
/// - 128-bit compatible with UUID
/// - Case insensitive (canonical is uppercase)
/// - URL-safe (no special characters)
pub fn generate_blank_node_id() -> String {
    ulid::Ulid::new().to_string()
}

/// Check if an IRI is a Fluree-generated blank node ID
pub fn is_blank_node_id(iri: &str) -> bool {
    iri.starts_with("_:fdb")
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::LedgerSnapshot;
    use fluree_vocab::namespaces::{DID_KEY, EMPTY, JSON_LD, XSD};

    #[test]
    fn test_predefined_codes() {
        let registry = NamespaceRegistry::new();

        // Check predefined codes are present
        assert_eq!(registry.get_code(""), Some(EMPTY));
        assert_eq!(registry.get_code("@"), Some(JSON_LD));
        assert_eq!(
            registry.get_code("http://www.w3.org/2001/XMLSchema#"),
            Some(XSD)
        );
        assert_eq!(registry.get_code("_:"), Some(BLANK_NODE));

        // Check reverse lookup
        assert_eq!(registry.get_prefix(BLANK_NODE), Some("_:"));
        assert_eq!(
            registry.get_prefix(XSD),
            Some("http://www.w3.org/2001/XMLSchema#")
        );
    }

    #[test]
    fn test_blank_node_code_is_fixed() {
        let registry = NamespaceRegistry::new();
        assert_eq!(registry.blank_node_code(), BLANK_NODE);
    }

    #[test]
    fn test_allocate_new_code() {
        let mut registry = NamespaceRegistry::new();

        let code1 = registry.get_or_allocate("http://example.org/");
        let code2 = registry.get_or_allocate("http://other.org/");
        let code1_again = registry.get_or_allocate("http://example.org/");

        assert_eq!(code1, code1_again);
        assert_ne!(code1, code2);
        assert!(code1 >= USER_NS_START);
        assert!(code2 >= USER_NS_START);
        assert_eq!(registry.delta.len(), 2);
    }

    #[test]
    fn test_blank_node_sid() {
        let registry = NamespaceRegistry::new();

        // Test with a ULID-style ID
        let sid = registry.blank_node_sid("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        assert_eq!(sid.namespace_code, BLANK_NODE);
        assert_eq!(sid.name.as_ref(), "fdb-01ARZ3NDEKTSV4RRFFQ69G5FAV");
    }

    #[test]
    fn test_is_blank_node_id() {
        assert!(is_blank_node_id("_:fdb-01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        assert!(is_blank_node_id("_:fdb"));
        assert!(!is_blank_node_id("_:b1"));
        assert!(!is_blank_node_id("http://example.org/thing"));
    }

    #[test]
    fn test_sid_for_iri() {
        let mut registry = NamespaceRegistry::new();

        let sid1 = registry.sid_for_iri("http://example.org/Person");
        let sid2 = registry.sid_for_iri("http://example.org/name");

        // Same prefix, should have same code
        assert_eq!(sid1.namespace_code, sid2.namespace_code);
        assert_eq!(sid1.name.as_ref(), "Person");
        assert_eq!(sid2.name.as_ref(), "name");
    }

    #[test]
    fn test_sid_for_iri_uses_longest_prefix_match() {
        let mut registry = NamespaceRegistry::new();

        // did:key: is a predefined prefix, so did:key:z6Mk... should use it
        let sid = registry.sid_for_iri("did:key:z6MkqtpqKGs4Et8mqBLBBAitDC1DPBiTJEbu26AcBX75B5rR");
        assert_eq!(sid.namespace_code, DID_KEY);
        assert_eq!(
            sid.name.as_ref(),
            "z6MkqtpqKGs4Et8mqBLBBAitDC1DPBiTJEbu26AcBX75B5rR"
        );

        // XSD namespace ends with #, should still work
        let sid3 = registry.sid_for_iri("http://www.w3.org/2001/XMLSchema#string");
        assert_eq!(sid3.namespace_code, XSD);
        assert_eq!(sid3.name.as_ref(), "string");

        // No delta should be created since all prefixes are predefined
        assert!(registry.delta.is_empty());
    }

    #[test]
    fn test_nested_prefixes_use_longest_match() {
        let mut registry = NamespaceRegistry::new();

        // Register a short prefix, then a longer nested prefix
        let short_code = registry.get_or_allocate("http://ex.org/");
        let long_code = registry.get_or_allocate("http://ex.org/foo/");
        assert_ne!(short_code, long_code);

        // IRI matching only the short prefix
        let sid1 = registry.sid_for_iri("http://ex.org/bar");
        assert_eq!(sid1.namespace_code, short_code);
        assert_eq!(sid1.name.as_ref(), "bar");

        // IRI matching BOTH — must use the longer prefix
        let sid2 = registry.sid_for_iri("http://ex.org/foo/bar");
        assert_eq!(sid2.namespace_code, long_code);
        assert_eq!(sid2.name.as_ref(), "bar");

        // Call again — trie always finds longest match, no cache issues
        let sid3 = registry.sid_for_iri("http://ex.org/foo/baz");
        assert_eq!(sid3.namespace_code, long_code);
        assert_eq!(sid3.name.as_ref(), "baz");

        // And the short prefix still works correctly after
        let sid4 = registry.sid_for_iri("http://ex.org/qux");
        assert_eq!(sid4.namespace_code, short_code);
        assert_eq!(sid4.name.as_ref(), "qux");
    }

    #[test]
    fn test_registry_from_db_enables_host_only_after_budget_crossed() {
        // Simulate a DB that has already allocated beyond the u8-ish namespace budget,
        // as would happen for outlier datasets after an import + index publish.
        let mut snapshot = LedgerSnapshot::genesis("test:main");
        snapshot
            .namespace_codes
            .insert(300, "http://already-allocated.example/".to_string());

        let mut registry = NamespaceRegistry::from_db(&snapshot);
        assert_eq!(registry.fallback_mode(), NsFallbackMode::HostOnly);

        let iri = "http://some-unseen-host/blah/123/456";
        let sid = registry.sid_for_iri(iri);

        // Host-only fallback allocates at scheme://host/ and keeps the full remainder as local.
        assert!(registry.has_prefix("http://some-unseen-host/"));
        assert!(!registry.has_prefix("http://some-unseen-host/blah/123/"));
        assert_eq!(sid.name.as_ref(), "blah/123/456");
    }

    #[test]
    fn test_registry_from_db_defaults_to_last_slash_under_budget() {
        // Simulate a normal DB (no namespace explosion): fallback remains last-slash-or-hash.
        let snapshot = LedgerSnapshot::genesis("test:main");
        let mut registry = NamespaceRegistry::from_db(&snapshot);
        assert_eq!(registry.fallback_mode(), NsFallbackMode::LastSlashOrHash);

        let iri = "http://some-unseen-host/blah/123/456";
        let sid = registry.sid_for_iri(iri);

        // Last-slash fallback allocates at the last path segment boundary.
        assert!(registry.has_prefix("http://some-unseen-host/blah/123/"));
        assert!(!registry.has_prefix("http://some-unseen-host/"));
        assert_eq!(sid.name.as_ref(), "456");
    }

    #[test]
    fn test_nested_prefixes_registered_in_reverse_order() {
        let mut registry = NamespaceRegistry::new();

        // Register LONG prefix first, then the short one
        let long_code = registry.get_or_allocate("http://ex.org/foo/");
        let short_code = registry.get_or_allocate("http://ex.org/");
        assert_ne!(short_code, long_code);

        // Warm the cache with the short prefix
        let sid1 = registry.sid_for_iri("http://ex.org/bar");
        assert_eq!(sid1.namespace_code, short_code);

        // Must still find the long prefix even though short was just used
        let sid2 = registry.sid_for_iri("http://ex.org/foo/bar");
        assert_eq!(sid2.namespace_code, long_code);
        assert_eq!(sid2.name.as_ref(), "bar");
    }

    #[test]
    fn test_trie_basic() {
        let mut trie = PrefixTrie::new();
        trie.insert("http://example.org/", 101);
        trie.insert("http://other.org/", 102);

        assert_eq!(
            trie.longest_match("http://example.org/foo"),
            Some((101, 19))
        );
        assert_eq!(trie.longest_match("http://other.org/bar"), Some((102, 17)));
        assert_eq!(trie.longest_match("http://unknown.org/baz"), None);
    }

    #[test]
    fn test_trie_nested_prefixes() {
        let mut trie = PrefixTrie::new();
        trie.insert("http://ex.org/", 101);
        trie.insert("http://ex.org/foo/", 102);

        // Short prefix only
        assert_eq!(trie.longest_match("http://ex.org/bar"), Some((101, 14)));
        // Longest match wins
        assert_eq!(trie.longest_match("http://ex.org/foo/bar"), Some((102, 18)));
    }

    #[test]
    fn test_trie_nested_reverse_insertion() {
        let mut trie = PrefixTrie::new();
        // Insert long first, then short
        trie.insert("http://ex.org/foo/", 102);
        trie.insert("http://ex.org/", 101);

        // Still finds longest match
        assert_eq!(trie.longest_match("http://ex.org/foo/bar"), Some((102, 18)));
        assert_eq!(trie.longest_match("http://ex.org/bar"), Some((101, 14)));
    }

    #[test]
    fn test_take_delta() {
        let mut registry = NamespaceRegistry::new();

        registry.get_or_allocate("http://example.org/");
        registry.get_or_allocate("http://other.org/");

        let delta = registry.take_delta();
        assert_eq!(delta.len(), 2);
        assert!(registry.delta.is_empty());
    }

    #[test]
    fn test_generate_blank_node_id() {
        let id1 = generate_blank_node_id();
        let id2 = generate_blank_node_id();

        // ULIDs are 26 characters (Crockford Base32)
        assert_eq!(id1.len(), 26);
        assert_eq!(id2.len(), 26);

        // IDs should be different (ULIDs include random component)
        assert_ne!(id1, id2);

        // Should be valid ULIDs (parseable)
        assert!(ulid::Ulid::from_string(&id1).is_ok());
        assert!(ulid::Ulid::from_string(&id2).is_ok());
    }

    #[test]
    fn test_all_codes() {
        let mut registry = NamespaceRegistry::new();
        let initial_count = registry.all_codes().len();

        registry.get_or_allocate("http://example.org/");
        let codes = registry.all_codes();
        assert_eq!(codes.len(), initial_count + 1);
    }

    // ========================================================================
    // SharedNamespaceAllocator tests
    // ========================================================================

    #[test]
    fn test_shared_alloc_from_registry() {
        let mut reg = NamespaceRegistry::new();
        reg.get_or_allocate("http://example.org/");
        let code = reg.get_code("http://example.org/").unwrap();

        let alloc = SharedNamespaceAllocator::from_registry(&reg);
        // Should find existing prefix
        assert_eq!(alloc.get_or_allocate("http://example.org/"), code);
        // Should allocate new prefix with next code
        let new_code = alloc.get_or_allocate("http://new.org/");
        assert_ne!(new_code, code);
        assert!(new_code >= USER_NS_START);
    }

    #[test]
    fn test_shared_alloc_concurrent_no_collisions() {
        let reg = NamespaceRegistry::new();
        let alloc = Arc::new(SharedNamespaceAllocator::from_registry(&reg));

        let handles: Vec<_> = (0..8)
            .map(|i| {
                let alloc = Arc::clone(&alloc);
                std::thread::spawn(move || {
                    let prefix = format!("http://thread-{}.org/", i);
                    alloc.get_or_allocate(&prefix)
                })
            })
            .collect();

        let codes: Vec<u16> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        // All codes should be unique
        let unique: FxHashSet<u16> = codes.iter().copied().collect();
        assert_eq!(unique.len(), 8);
    }

    #[test]
    fn test_shared_alloc_same_prefix_same_code() {
        let reg = NamespaceRegistry::new();
        let alloc = Arc::new(SharedNamespaceAllocator::from_registry(&reg));

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let alloc = Arc::clone(&alloc);
                std::thread::spawn(move || alloc.get_or_allocate("http://shared-prefix.org/"))
            })
            .collect();

        let codes: Vec<u16> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        // All should get the same code
        assert!(codes.iter().all(|&c| c == codes[0]));
    }

    #[test]
    fn test_shared_alloc_sid_for_iri() {
        let mut reg = NamespaceRegistry::new();
        reg.get_or_allocate("http://example.org/");
        let alloc = SharedNamespaceAllocator::from_registry(&reg);

        let sid = alloc.sid_for_iri("http://example.org/Person");
        assert_eq!(sid.name.as_ref(), "Person");
    }

    #[test]
    fn test_shared_alloc_lookup_codes() {
        let reg = NamespaceRegistry::new();
        let alloc = SharedNamespaceAllocator::from_registry(&reg);

        let code1 = alloc.get_or_allocate("http://a.org/");
        let code2 = alloc.get_or_allocate("http://b.org/");

        let mut query = FxHashSet::default();
        query.insert(code1);
        query.insert(code2);

        let result = alloc.lookup_codes(&query);
        assert_eq!(result.len(), 2);
        assert_eq!(result[&code1], "http://a.org/");
        assert_eq!(result[&code2], "http://b.org/");
    }

    // ========================================================================
    // WorkerCache tests
    // ========================================================================

    #[test]
    fn test_worker_cache_local_trie() {
        let mut reg = NamespaceRegistry::new();
        let code = reg.get_or_allocate("http://example.org/");
        let alloc = Arc::new(SharedNamespaceAllocator::from_registry(&reg));

        let mut cache = WorkerCache::new(Arc::clone(&alloc));

        // Should resolve from local trie (no lock)
        let sid = cache.sid_for_iri("http://example.org/Person");
        assert_eq!(sid.namespace_code, code);
        assert_eq!(sid.name.as_ref(), "Person");

        // Code was in snapshot, so not in new_codes
        assert!(cache.new_codes.is_empty());
    }

    #[test]
    fn test_worker_cache_new_alloc_tracked() {
        let reg = NamespaceRegistry::new();
        let alloc = Arc::new(SharedNamespaceAllocator::from_registry(&reg));

        let mut cache = WorkerCache::new(Arc::clone(&alloc));

        // Allocate a new prefix (not in snapshot)
        let code = cache.get_or_allocate("http://new-domain.org/");
        assert!(code >= USER_NS_START);

        // Should be in new_codes
        assert!(cache.new_codes.contains(&code));

        // Subsequent lookups should use local trie
        let sid = cache.sid_for_iri("http://new-domain.org/Thing");
        assert_eq!(sid.namespace_code, code);
        assert_eq!(sid.name.as_ref(), "Thing");
    }

    #[test]
    fn test_worker_cache_cross_worker_code_tracked() {
        let reg = NamespaceRegistry::new();
        let alloc = Arc::new(SharedNamespaceAllocator::from_registry(&reg));

        // Simulate: worker B allocates a code AFTER worker A takes its snapshot
        let mut cache_a = WorkerCache::new(Arc::clone(&alloc));

        // Another "worker" allocates a prefix directly on the shared allocator
        let code_from_b = alloc.get_or_allocate("http://domain-b.org/");

        // Worker A encounters the same prefix — gets the same code from shared allocator
        let code_from_a = cache_a.get_or_allocate("http://domain-b.org/");
        assert_eq!(code_from_a, code_from_b);

        // Worker A's new_codes should include it (code >= snapshot_next_code)
        assert!(cache_a.new_codes.contains(&code_from_a));
    }

    #[test]
    fn test_worker_cache_overflow_not_tracked() {
        // This test would require exhausting u16 codes which is impractical.
        // Instead, verify the track_code logic directly.
        let reg = NamespaceRegistry::new();
        let alloc = Arc::new(SharedNamespaceAllocator::from_registry(&reg));
        let mut cache = WorkerCache::new(Arc::clone(&alloc));

        // OVERFLOW should never be tracked
        cache.track_code(OVERFLOW);
        assert!(cache.new_codes.is_empty());
    }

    #[test]
    fn test_commit_order_publication() {
        // Simulate the full publication workflow:
        // chunk 0 (serial) → shared allocator → parallel workers → serial publication

        // Step 1: chunk 0 establishes "http://base.org/"
        let mut reg = NamespaceRegistry::new();
        reg.get_or_allocate("http://base.org/");
        let published_codes: FxHashSet<u16> = reg.all_codes();

        let alloc = Arc::new(SharedNamespaceAllocator::from_registry(&reg));

        // Step 2: Two workers take snapshots
        let mut cache_a = WorkerCache::new(Arc::clone(&alloc));
        let mut cache_b = WorkerCache::new(Arc::clone(&alloc));

        // Worker B allocates "http://new-b.org/"
        let code_b = cache_b.get_or_allocate("http://new-b.org/");

        // Worker A uses the SAME prefix (after B allocated it)
        let code_a = cache_a.get_or_allocate("http://new-b.org/");
        assert_eq!(code_a, code_b);

        // Worker A also uses a predefined prefix (already published)
        cache_a.sid_for_iri("http://www.w3.org/2001/XMLSchema#string");

        let new_codes_a = cache_a.into_new_codes();
        let new_codes_b = cache_b.into_new_codes();

        // Both should have code_b in their new_codes
        assert!(new_codes_a.contains(&code_b));
        assert!(new_codes_b.contains(&code_b));

        // Step 3: Serial publication — commit A first
        let mut published = published_codes;

        // Commit A's delta
        let unpublished_a: FxHashSet<u16> = new_codes_a
            .iter()
            .copied()
            .filter(|c| *c < OVERFLOW && !published.contains(c))
            .collect();
        let delta_a = alloc.lookup_codes(&unpublished_a);
        published.extend(&unpublished_a);

        // Commit A publishes code_b
        assert!(delta_a.contains_key(&code_b));

        // Commit B's delta — code_b already published
        let unpublished_b: FxHashSet<u16> = new_codes_b
            .iter()
            .copied()
            .filter(|c| *c < OVERFLOW && !published.contains(c))
            .collect();

        // Nothing new to publish for B
        assert!(unpublished_b.is_empty());
    }

    #[test]
    fn test_publication_is_minimal() {
        // A code published in commit t must not re-appear in t+1's delta.
        let mut reg = NamespaceRegistry::new();
        reg.get_or_allocate("http://base.org/");
        let alloc = Arc::new(SharedNamespaceAllocator::from_registry(&reg));

        let mut cache1 = WorkerCache::new(Arc::clone(&alloc));
        let mut cache2 = WorkerCache::new(Arc::clone(&alloc));

        // Both workers use the same new prefix
        let code = cache1.get_or_allocate("http://shared-new.org/");
        cache2.get_or_allocate("http://shared-new.org/");

        let new1 = cache1.into_new_codes();
        let new2 = cache2.into_new_codes();

        let mut published: FxHashSet<u16> = reg.all_codes();

        // Publish for chunk 1
        let unpub1: FxHashSet<u16> = new1
            .into_iter()
            .filter(|c| *c < OVERFLOW && !published.contains(c))
            .collect();
        assert!(unpub1.contains(&code));
        published.extend(&unpub1);

        // Publish for chunk 2 — code should NOT be re-published
        let unpub2: FxHashSet<u16> = new2
            .into_iter()
            .filter(|c| *c < OVERFLOW && !published.contains(c))
            .collect();
        assert!(!unpub2.contains(&code));
    }
}
