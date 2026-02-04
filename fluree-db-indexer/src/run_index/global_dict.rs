//! Global dictionaries for dictionary resolution (Phase B).
//!
//! Three dictionary types serve different scaling needs:
//!
//! - **SubjectDict**: xxh3_128 hash-based reverse map, file-backed forward map.
//!   Handles 100M+ subjects at ~52 bytes/entry (~5GB RAM at 100M — "big iron" mode).
//!   The forward file is NOT on the hot path — only read for projection/upgrade.
//!
//! - **PredicateDict** / **StringValueDict**: `HashMap<String, u32>` with `&str` lookup
//!   via the `Borrow` trait. Appropriate for small-to-medium cardinality.
//!   Predicate dictionaries are typically < 10K entries. String value dictionaries
//!   can grow large for high-NDV string properties; per-predicate dictionaries
//!   are a later optimization.
//!
//! - **LanguageTagDict**: Per-run `u16` assignment. Rebuilt at each run flush.
//!
//! ## Note on Doubles as LEX_ID
//!
//! In Phase 4, non-integer doubles are stored as LEX_ID in the global
//! StringValueDict. For datasets with high-NDV float properties (e.g., sensor
//! readings), this can cause the string dictionary to grow large. Per-predicate
//! NUM_FLOAT dictionaries with midpoint-splitting ranks are a later optimization.

use rustc_hash::FxHashMap;
use std::borrow::Borrow;
use std::hash::Hash;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use serde_json;

// ============================================================================
// SubjectDict (xxh3_128 reverse map, file-backed forward map)
// ============================================================================

/// Subject dictionary using xxh3_128 hashing for O(1) repeat-subject lookup.
///
/// Hot path (repeat subjects): compute xxh3_128 + FxHashMap lookup. No string
/// construction, no disk I/O.
///
/// Novel entries: construct full IRI, append to forward file, insert into map.
///
/// Subject IDs are namespace-structured 64-bit values (sid64):
///   `sid64 = (ns_code_u16 << 48) | local_id_u48`
/// Per-namespace counters ensure local_ids are dense within each namespace.
///
/// Memory: ~60 bytes/entry (HashMap entry ~48B + forward vecs 20B).
/// At 100M subjects: ~6GB RAM. This is intentionally "big iron" mode;
/// production dictionary uses the partitioned-on-disk strategy from VALUE_ID_PROPOSAL.
pub struct SubjectDict {
    /// Reverse: xxh3_128(iri) → sid64 (raw u64).
    /// 128-bit hash makes collisions negligible (~10^-22 at 100M entries).
    reverse: FxHashMap<u128, u64>,
    /// Forward: sequential insertion index → offset into forward_file.
    /// Separate vecs for proper alignment (avoids 16B padded tuple).
    forward_offsets: Vec<u64>,
    /// Forward: sequential insertion index → byte length of IRI in forward_file.
    forward_lens: Vec<u32>,
    /// Forward: sequential insertion index → sid64 (for writing sid mapping file).
    forward_sids: Vec<u64>,
    /// Append-only file of IRI bytes (no length prefix — lengths in forward_lens).
    forward_file: Option<BufWriter<std::fs::File>>,
    /// Path to the forward file (for diagnostics/reopening).
    #[allow(dead_code)]
    forward_path: PathBuf,
    /// Current write offset in the forward file.
    forward_write_offset: u64,
    /// Per-namespace next local_id counter. Indexed by ns_code (u16).
    /// Grows on demand when a new namespace is first seen.
    next_local_ids: Vec<u64>,
    /// Total number of entries across all namespaces.
    count: u64,
    /// Set when any namespace's local_id exceeds u16::MAX.
    /// Once set, never reverts. Determines narrow vs wide leaflet encoding.
    needs_wide: bool,
}

impl SubjectDict {
    /// Maximum local_id value within a 48-bit field.
    const MAX_LOCAL_ID: u64 = (1u64 << 48) - 1;

    /// Create a new SubjectDict with a forward file at the given path.
    pub fn new(forward_path: impl AsRef<Path>) -> io::Result<Self> {
        let path = forward_path.as_ref().to_path_buf();
        let file = std::fs::File::create(&path)?;
        Ok(Self {
            reverse: FxHashMap::default(),
            forward_offsets: Vec::new(),
            forward_lens: Vec::new(),
            forward_sids: Vec::new(),
            forward_file: Some(BufWriter::new(file)),
            forward_path: path,
            forward_write_offset: 0,
            next_local_ids: Vec::new(),
            count: 0,
            needs_wide: false,
        })
    }

    /// Create a SubjectDict without a forward file (in-memory only, for tests).
    pub fn new_memory() -> Self {
        Self {
            reverse: FxHashMap::default(),
            forward_offsets: Vec::new(),
            forward_lens: Vec::new(),
            forward_sids: Vec::new(),
            forward_file: None,
            forward_path: PathBuf::new(),
            forward_write_offset: 0,
            next_local_ids: Vec::new(),
            count: 0,
            needs_wide: false,
        }
    }

    /// Look up or insert an IRI by its pre-computed xxh3_128 hash.
    ///
    /// `ns_code` is the namespace code for this subject (determines which
    /// per-namespace counter allocates the local_id portion of the sid64).
    ///
    /// `iri_builder` is only called for novel entries (to get the full IRI
    /// for the forward file). Repeat subjects never call it.
    ///
    /// Returns the raw sid64 value (`(ns_code << 48) | local_id`).
    pub fn get_or_insert_with_hash<F>(
        &mut self,
        hash: u128,
        ns_code: u16,
        iri_builder: F,
    ) -> io::Result<u64>
    where
        F: FnOnce() -> String,
    {
        if let Some(&sid64) = self.reverse.get(&hash) {
            return Ok(sid64);
        }

        // Allocate next local_id for this namespace
        let ns_idx = ns_code as usize;
        if ns_idx >= self.next_local_ids.len() {
            self.next_local_ids.resize(ns_idx + 1, 0);
        }
        let local_id = self.next_local_ids[ns_idx];
        if local_id > Self::MAX_LOCAL_ID {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "SubjectDict: local_id overflow for ns_code {} (exceeded 2^48)",
                    ns_code
                ),
            ));
        }
        self.next_local_ids[ns_idx] = local_id + 1;

        // Track wide requirement
        if local_id > u16::MAX as u64 {
            self.needs_wide = true;
        }

        // Construct sid64
        let sid64 = ((ns_code as u64) << 48) | local_id;

        // Write to forward file if available
        let iri = iri_builder();
        let iri_bytes = iri.as_bytes();
        let offset = self.forward_write_offset;
        let len = iri_bytes.len() as u32;

        if let Some(ref mut writer) = self.forward_file {
            writer.write_all(iri_bytes)?;
        }

        self.forward_offsets.push(offset);
        self.forward_lens.push(len);
        self.forward_sids.push(sid64);
        self.forward_write_offset += len as u64;

        self.reverse.insert(hash, sid64);
        self.count += 1;
        Ok(sid64)
    }

    /// Convenience: compute xxh3_128 from the IRI string and insert.
    ///
    /// `ns_code` is the namespace code for this subject.
    pub fn get_or_insert(&mut self, iri: &str, ns_code: u16) -> io::Result<u64> {
        let hash = xxhash_rust::xxh3::xxh3_128(iri.as_bytes());
        let iri_owned = iri.to_string();
        self.get_or_insert_with_hash(hash, ns_code, move || iri_owned)
    }

    /// Number of entries in the dictionary.
    pub fn len(&self) -> u64 {
        self.count
    }

    /// Check if the dictionary is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Whether any namespace's local_id has exceeded u16::MAX.
    ///
    /// When true, leaflet columns must use wide (u64) encoding.
    /// When false, narrow (u32) encoding suffices.
    pub fn needs_wide(&self) -> bool {
        self.needs_wide
    }

    /// Per-namespace max assigned local_id watermarks for `DictNovelty`.
    ///
    /// Returns `watermarks[i]` = max local_id for namespace code `i`.
    /// 0 for namespaces with no assigned subjects.
    pub fn subject_watermarks(&self) -> Vec<u64> {
        self.next_local_ids
            .iter()
            .map(|&next| next.saturating_sub(1))
            .collect()
    }

    /// Flush the forward file buffer to disk.
    pub fn flush(&mut self) -> io::Result<()> {
        if let Some(ref mut writer) = self.forward_file {
            writer.flush()?;
        }
        Ok(())
    }

    /// Read all entries as (sid64, iri_bytes) pairs.
    ///
    /// Reads the forward file from disk. Call `flush()` first to ensure
    /// all buffered writes are visible.
    pub fn read_all_entries(&self) -> io::Result<Vec<(u64, Vec<u8>)>> {
        let data = std::fs::read(&self.forward_path)?;
        let mut entries = Vec::with_capacity(self.count as usize);
        for seq in 0..self.count as usize {
            let offset = self.forward_offsets[seq] as usize;
            let len = self.forward_lens[seq] as usize;
            let sid64 = self.forward_sids[seq];
            entries.push((sid64, data[offset..offset + len].to_vec()));
        }
        Ok(entries)
    }

    /// Forward offset table: `offsets[seq]` = byte offset into subjects.fwd.
    /// Indexed by sequential insertion order (not sid64).
    pub fn forward_offsets(&self) -> &[u64] {
        &self.forward_offsets
    }

    /// Forward length table: `lens[seq]` = byte length of IRI in subjects.fwd.
    /// Indexed by sequential insertion order (not sid64).
    pub fn forward_lens(&self) -> &[u32] {
        &self.forward_lens
    }

    /// Sid64 table: `sids[seq]` = sid64 for the seq-th inserted subject.
    /// Used to write the sid mapping file alongside the forward index.
    pub fn forward_sids(&self) -> &[u64] {
        &self.forward_sids
    }

    /// Write a reverse hash index to `subjects.rev` for O(log N) IRI → s_id lookup.
    ///
    /// Format: `SRV2` magic (4B) + count (u64) + sorted records of
    /// `(hash_hi: u64, hash_lo: u64, sid64: u64)` — 24 bytes per record.
    ///
    /// Sorted by (hash_hi, hash_lo) for binary search at query time.
    pub fn write_reverse_index(&self, path: &Path) -> io::Result<()> {
        use std::io::Write;

        let mut entries: Vec<(u64, u64, u64)> = self
            .reverse
            .iter()
            .map(|(&hash, &sid64)| {
                let hi = (hash >> 64) as u64;
                let lo = hash as u64;
                (hi, lo, sid64)
            })
            .collect();

        // Sort by (hash_hi, hash_lo) for binary search
        entries.sort_unstable();

        let mut file = io::BufWriter::new(std::fs::File::create(path)?);
        file.write_all(b"SRV2")?;
        file.write_all(&(entries.len() as u64).to_le_bytes())?;

        for &(hi, lo, sid64) in &entries {
            file.write_all(&hi.to_le_bytes())?;
            file.write_all(&lo.to_le_bytes())?;
            file.write_all(&sid64.to_le_bytes())?;
        }

        file.flush()?;
        tracing::info!(
            path = %path.display(),
            entries = entries.len(),
            size_mb = (entries.len() * 24) / (1024 * 1024),
            "subject reverse index written (SRV2)"
        );
        Ok(())
    }
}

// ============================================================================
// PredicateDict (simple, always small)
// ============================================================================

/// Simple string → u32 dictionary for predicates and graphs.
///
/// Uses `FxHashMap<String, u32>` with `&str` lookup via a newtype wrapper
/// that implements `Borrow<str>`. Appropriate for small cardinality (< 10K).
pub struct PredicateDict {
    forward: Vec<String>,
    reverse: FxHashMap<BorrowableString, u32>,
}

impl PredicateDict {
    pub fn new() -> Self {
        Self {
            forward: Vec::new(),
            reverse: FxHashMap::default(),
        }
    }

    /// Look up or insert a string, returning its sequential u32 ID.
    pub fn get_or_insert(&mut self, s: &str) -> u32 {
        if let Some(&id) = self.reverse.get(s as &str) {
            return id;
        }
        let id = self.forward.len() as u32;
        let owned = s.to_string();
        self.forward.push(owned.clone());
        self.reverse.insert(BorrowableString(owned), id);
        id
    }

    /// Look up or insert by prefix + name parts, avoiding heap allocation on hits.
    ///
    /// Uses a stack buffer to concatenate prefix + name for the HashMap lookup.
    /// Only allocates a heap String on miss (novel entry). Most predicate/graph
    /// IRIs are well under 256 bytes.
    pub fn get_or_insert_parts(&mut self, prefix: &str, name: &str) -> u32 {
        let total_len = prefix.len() + name.len();

        // Stack-based lookup for short IRIs (avoids heap allocation on hits)
        if total_len <= 256 {
            let mut buf = [0u8; 256];
            buf[..prefix.len()].copy_from_slice(prefix.as_bytes());
            buf[prefix.len()..total_len].copy_from_slice(name.as_bytes());
            // SAFETY: buf[..total_len] is copied from two valid UTF-8 &str slices.
            let iri = unsafe { std::str::from_utf8_unchecked(&buf[..total_len]) };

            if let Some(&id) = self.reverse.get(iri) {
                return id;
            }
        }

        // Miss (or rare long IRI): heap allocate for insertion
        let mut full_iri = String::with_capacity(total_len);
        full_iri.push_str(prefix);
        full_iri.push_str(name);
        self.get_or_insert(&full_iri)
    }

    /// Look up a string without inserting.
    pub fn get(&self, s: &str) -> Option<u32> {
        self.reverse.get(s as &str).copied()
    }

    /// Get the string for a given ID.
    pub fn resolve(&self, id: u32) -> Option<&str> {
        self.forward.get(id as usize).map(|s| s.as_str())
    }

    pub fn len(&self) -> u32 {
        self.forward.len() as u32
    }

    pub fn is_empty(&self) -> bool {
        self.forward.is_empty()
    }

    /// Return all entries as (id, value_bytes) pairs. Already in-memory.
    pub fn all_entries(&self) -> Vec<(u64, Vec<u8>)> {
        self.forward
            .iter()
            .enumerate()
            .map(|(i, s)| (i as u64, s.as_bytes().to_vec()))
            .collect()
    }

    /// Write a reverse hash index for O(log N) string → str_id lookup.
    ///
    /// Format: `LRV1` magic (4B) + count (u32) + sorted records of
    /// `(hash_hi: u64, hash_lo: u64, str_id: u32)` — 20 bytes per record.
    ///
    /// Uses xxh3_128 of the string bytes, sorted by (hash_hi, hash_lo)
    /// for binary search at query time. Mirrors the `subjects.rev` format.
    pub fn write_reverse_index(&self, path: &Path) -> io::Result<()> {
        let mut entries: Vec<(u64, u64, u32)> = Vec::with_capacity(self.reverse.len());

        for (key, &str_id) in &self.reverse {
            let s: &str = key.borrow();
            let hash = xxhash_rust::xxh3::xxh3_128(s.as_bytes());
            let hi = (hash >> 64) as u64;
            let lo = hash as u64;
            entries.push((hi, lo, str_id));
        }

        entries.sort_unstable();

        let mut file = BufWriter::new(std::fs::File::create(path)?);
        file.write_all(b"LRV1")?;
        file.write_all(&(entries.len() as u32).to_le_bytes())?;

        for &(hi, lo, str_id) in &entries {
            file.write_all(&hi.to_le_bytes())?;
            file.write_all(&lo.to_le_bytes())?;
            file.write_all(&str_id.to_le_bytes())?;
        }

        file.flush()?;
        tracing::info!(
            path = %path.display(),
            entries = entries.len(),
            size_mb = (entries.len() * 20) / (1024 * 1024),
            "string reverse index written"
        );
        Ok(())
    }
}

impl Default for PredicateDict {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// StringValueDict (global for Phase 4)
// ============================================================================

/// Global string value dictionary. Same structure as PredicateDict.
///
/// Phase 4 limitation: all string/decimal/double/JSON values share one
/// global dictionary. LEX_ID ordering is by insertion order, not lexicographic.
/// Per-predicate string dictionaries are a later optimization.
pub type StringValueDict = PredicateDict;

// ============================================================================
// LanguageTagDict (per-run)
// ============================================================================

/// Per-run language tag dictionary.
///
/// Maps language tags (e.g., "en", "fr") to u16 IDs. ID 0 means "no language
/// tag". Rebuilt at each run flush — downstream merge renumbers.
#[derive(Clone)]
pub struct LanguageTagDict {
    tags: Vec<String>,
    reverse: FxHashMap<BorrowableString, u16>,
}

impl LanguageTagDict {
    pub fn new() -> Self {
        Self {
            tags: Vec::new(),
            reverse: FxHashMap::default(),
        }
    }

    /// Look up or insert a language tag, returning its u16 ID (>= 1).
    /// Returns 0 if `tag` is None.
    pub fn get_or_insert(&mut self, tag: Option<&str>) -> u16 {
        let tag = match tag {
            Some(t) => t,
            None => return 0,
        };
        if let Some(&id) = self.reverse.get(tag as &str) {
            return id;
        }
        let id = (self.tags.len() as u16) + 1; // 1-based
        let owned = tag.to_string();
        self.tags.push(owned.clone());
        self.reverse.insert(BorrowableString(owned), id);
        id
    }

    /// Get the tag string for a given ID.
    pub fn resolve(&self, id: u16) -> Option<&str> {
        if id == 0 {
            return None;
        }
        self.tags.get((id - 1) as usize).map(|s| s.as_str())
    }

    /// Number of distinct language tags (excluding the "none" sentinel).
    pub fn len(&self) -> u16 {
        self.tags.len() as u16
    }

    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    /// Find the ID for a language tag (reverse lookup).
    ///
    /// Returns `None` if the tag is not in the dictionary.
    pub fn find_id(&self, tag: &str) -> Option<u16> {
        self.reverse.get(tag as &str).copied()
    }

    /// Iterator over (id, tag) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (u16, &str)> {
        self.tags
            .iter()
            .enumerate()
            .map(|(i, s)| ((i as u16) + 1, s.as_str()))
    }

    /// Clear and reset the dictionary (for per-run reuse).
    pub fn clear(&mut self) {
        self.tags.clear();
        self.reverse.clear();
    }
}

impl Default for LanguageTagDict {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Datatype dict constants (dt_ids)
// ============================================================================

/// Named constants for reserved datatype dict IDs.
///
/// These replace `DatatypeId::FOO` constants for the bulk import path.
/// Only types with special encoding/coercion rules get reserved IDs.
/// Everything else is dynamically assigned (ID 14+).
///
/// Type is `u16` to match `RunRecord.dt` — most datasets use ≤255 types
/// (encoded as u8 in leaf Region 2), but u16 supports up to 65535 distinct
/// datatype IRIs in a single import.
pub mod dt_ids {
    // These reserved IDs are stable (insertion order) and intentionally small.
    // The bulk import path currently enforces `dt_id <= 255` (u8), but the
    // overall system supports widening to u16 in the binary format when needed.
    pub const ID: u16 = 0;           // @id — IRI reference sentinel
    pub const STRING: u16 = 1;       // xsd:string
    pub const BOOLEAN: u16 = 2;      // xsd:boolean
    pub const INTEGER: u16 = 3;      // xsd:integer
    pub const LONG: u16 = 4;         // xsd:long
    pub const DECIMAL: u16 = 5;      // xsd:decimal
    pub const DOUBLE: u16 = 6;       // xsd:double
    pub const FLOAT: u16 = 7;        // xsd:float
    pub const DATE_TIME: u16 = 8;    // xsd:dateTime
    pub const DATE: u16 = 9;         // xsd:date
    pub const TIME: u16 = 10;        // xsd:time
    pub const LANG_STRING: u16 = 11; // rdf:langString
    pub const JSON: u16 = 12;        // @json
    pub const VECTOR: u16 = 13;      // @vector
    pub const RESERVED_COUNT: u16 = 14;
}

/// Create a new datatype dict with reserved entries pre-inserted.
///
/// Order matters: `get_or_insert` returns sequential IDs starting at 0.
/// Only types with special encoding/coercion rules are reserved.
pub(crate) fn new_datatype_dict() -> PredicateDict {
    let mut d = PredicateDict::new();
    d.get_or_insert("@id");                                       // 0
    d.get_or_insert(fluree_vocab::xsd::STRING);                   // 1
    d.get_or_insert(fluree_vocab::xsd::BOOLEAN);                  // 2
    d.get_or_insert(fluree_vocab::xsd::INTEGER);                  // 3
    d.get_or_insert(fluree_vocab::xsd::LONG);                     // 4
    d.get_or_insert(fluree_vocab::xsd::DECIMAL);                  // 5
    d.get_or_insert(fluree_vocab::xsd::DOUBLE);                   // 6
    d.get_or_insert(fluree_vocab::xsd::FLOAT);                    // 7
    d.get_or_insert(fluree_vocab::xsd::DATE_TIME);                // 8
    d.get_or_insert(fluree_vocab::xsd::DATE);                     // 9
    d.get_or_insert(fluree_vocab::xsd::TIME);                     // 10
    d.get_or_insert(fluree_vocab::rdf::LANG_STRING);              // 11
    d.get_or_insert("@json");                                     // 12
    d.get_or_insert("@vector");                                   // 13
    debug_assert_eq!(d.len(), 14);
    d
}

// ============================================================================
// GlobalDicts (bundle)
// ============================================================================

/// All global dictionaries needed for dictionary resolution.
pub struct GlobalDicts {
    pub subjects: SubjectDict,
    pub predicates: PredicateDict,
    pub graphs: PredicateDict,
    pub strings: StringValueDict,
    pub languages: LanguageTagDict,
    pub datatypes: PredicateDict,
    /// Per-predicate overflow numeric arenas (BigInt/BigDecimal). Key = p_id.
    pub numbigs: FxHashMap<u32, super::numbig_dict::NumBigArena>,
}

impl GlobalDicts {
    /// Create GlobalDicts with file-backed subject dict.
    ///
    /// Pre-inserts the txn-meta graph name (`ledger#transactions`) as the first
    /// graph entry, guaranteeing `g_id = 1` for txn-meta regardless of import order.
    pub fn new(subject_forward_path: impl AsRef<Path>) -> io::Result<Self> {
        let mut dicts = Self {
            subjects: SubjectDict::new(subject_forward_path)?,
            predicates: PredicateDict::new(),
            graphs: PredicateDict::new(),
            strings: StringValueDict::new(),
            languages: LanguageTagDict::new(),
            datatypes: new_datatype_dict(),
            numbigs: FxHashMap::default(),
        };
        // Reserve g_id=1 for txn-meta: graphs dict returns 0-based, +1 = g_id 1.
        dicts.graphs.get_or_insert_parts(
            fluree_vocab::fluree::LEDGER,
            "transactions",
        );
        Ok(dicts)
    }

    /// Create GlobalDicts with in-memory subject dict (for tests).
    ///
    /// Pre-inserts the txn-meta graph name (`ledger#transactions`) as the first
    /// graph entry, guaranteeing `g_id = 1` for txn-meta regardless of import order.
    pub fn new_memory() -> Self {
        let mut dicts = Self {
            subjects: SubjectDict::new_memory(),
            predicates: PredicateDict::new(),
            graphs: PredicateDict::new(),
            strings: StringValueDict::new(),
            languages: LanguageTagDict::new(),
            datatypes: new_datatype_dict(),
            numbigs: FxHashMap::default(),
        };
        // Reserve g_id=1 for txn-meta: graphs dict returns 0-based, +1 = g_id 1.
        dicts.graphs.get_or_insert_parts(
            fluree_vocab::fluree::LEDGER,
            "transactions",
        );
        dicts
    }

    /// Persist all dictionaries to disk alongside the run files.
    ///
    /// Writes:
    /// - `subjects.idx` — subject forward-file offset/len index
    /// - `subjects.sids` — sid64 mapping (sequential index → sid64)
    /// - `strings.fwd` + `strings.idx` — string value forward file + index
    /// - `graphs.dict` — graph dictionary
    /// - `predicates.json` — predicate id→IRI table (for index-build p_width + tooling)
    pub fn persist(&mut self, run_dir: &Path) -> io::Result<()> {
        use super::dict_io::{write_predicate_dict, write_string_dict, write_subject_index,
                             write_subject_sid_map};

        // Flush subject forward file
        self.subjects.flush()?;

        // Write subject index (offsets + lens for subjects.fwd)
        write_subject_index(
            &run_dir.join("subjects.idx"),
            self.subjects.forward_offsets(),
            self.subjects.forward_lens(),
        )?;

        // Write sid64 mapping (sequential index → sid64)
        write_subject_sid_map(
            &run_dir.join("subjects.sids"),
            self.subjects.forward_sids(),
        )?;

        // Write string dict (forward file + index)
        write_string_dict(
            &run_dir.join("strings.fwd"),
            &run_dir.join("strings.idx"),
            &self.strings,
        )?;

        // Write predicate id → IRI table (JSON array by id).
        // This is not a CAS artifact; the canonical query-time mapping is in the v2 root.
        let preds: Vec<&str> = (0..self.predicates.len())
            .map(|p_id| self.predicates.resolve(p_id).unwrap_or(""))
            .collect();
        std::fs::write(
            run_dir.join("predicates.json"),
            serde_json::to_vec(&preds).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
        )?;

        // Write graph dict
        write_predicate_dict(&run_dir.join("graphs.dict"), &self.graphs)?;

        // Write datatype dict
        write_predicate_dict(&run_dir.join("datatypes.dict"), &self.datatypes)?;

        // Write numbig arenas (one file per predicate)
        if !self.numbigs.is_empty() {
            let nb_dir = run_dir.join("numbig");
            std::fs::create_dir_all(&nb_dir)?;
            for (&p_id, arena) in &self.numbigs {
                super::numbig_dict::write_numbig_arena(
                    &nb_dir.join(format!("p_{}.nba", p_id)),
                    arena,
                )?;
            }
            tracing::info!(
                predicates = self.numbigs.len(),
                total_entries = self.numbigs.values().map(|a| a.len()).sum::<usize>(),
                "numbig arenas persisted"
            );
        }

        tracing::info!(
            subjects = self.subjects.len(),
            predicates = self.predicates.len(),
            strings = self.strings.len(),
            graphs = self.graphs.len(),
            datatypes = self.datatypes.len(),
            "dictionaries persisted"
        );

        Ok(())
    }
}

// ============================================================================
// BorrowableString — enables HashMap<String, _> lookup with &str
// ============================================================================

/// Wrapper around `String` that implements `Borrow<str>`, `Hash`, and `Eq`
/// based on the string content. This allows `FxHashMap<BorrowableString, _>`
/// to be queried with `&str` keys without allocating.
#[derive(Clone, Debug)]
struct BorrowableString(String);

impl PartialEq for BorrowableString {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for BorrowableString {}

impl Hash for BorrowableString {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.as_str().hash(state);
    }
}

impl Borrow<str> for BorrowableString {
    fn borrow(&self) -> &str {
        &self.0
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- SubjectDict tests ----

    #[test]
    fn test_subject_dict_insert_and_dedup() {
        let mut dict = SubjectDict::new_memory();
        let ns: u16 = 100; // test namespace

        let id1 = dict.get_or_insert("http://example.org/Alice", ns).unwrap();
        let id2 = dict.get_or_insert("http://example.org/Bob", ns).unwrap();
        let id1_again = dict.get_or_insert("http://example.org/Alice", ns).unwrap();

        // sid64 = (ns << 48) | local_id
        let expected_0 = (ns as u64) << 48;
        let expected_1 = ((ns as u64) << 48) | 1;
        assert_eq!(id1, expected_0);
        assert_eq!(id2, expected_1);
        assert_eq!(id1, id1_again);
        assert_eq!(dict.len(), 2);
    }

    #[test]
    fn test_subject_dict_streaming_hash() {
        let mut dict = SubjectDict::new_memory();
        let ns: u16 = 100;

        // Simulate streaming hash: prefix + name
        let prefix = "http://example.org/";
        let name = "Alice";

        use xxhash_rust::xxh3::Xxh3;
        let mut hasher = Xxh3::new();
        hasher.update(prefix.as_bytes());
        hasher.update(name.as_bytes());
        let hash = hasher.digest128();

        let full_iri = format!("{}{}", prefix, name);
        let id1 = dict
            .get_or_insert_with_hash(hash, ns, || full_iri.clone())
            .unwrap();

        // Same hash → same ID (no iri_builder called)
        let mut called = false;
        let id2 = dict
            .get_or_insert_with_hash(hash, ns, || {
                called = true;
                full_iri.clone()
            })
            .unwrap();

        assert_eq!(id1, id2);
        assert!(!called, "iri_builder should not be called for existing entry");
    }

    #[test]
    fn test_subject_dict_with_file() {
        let dir = std::env::temp_dir().join("fluree_test_subject_dict");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("subjects.fwd");

        let mut dict = SubjectDict::new(&path).unwrap();
        dict.get_or_insert("http://example.org/Alice", 100).unwrap();
        dict.get_or_insert("http://example.org/Bob", 100).unwrap();
        dict.flush().unwrap();

        // Verify forward file exists and has content
        let meta = std::fs::metadata(&path).unwrap();
        assert!(meta.len() > 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_subject_dict_per_namespace_ids() {
        let mut dict = SubjectDict::new_memory();

        // Insert subjects in two different namespaces
        let ns_a: u16 = 10;
        let ns_b: u16 = 20;

        let id_a0 = dict.get_or_insert("http://a.org/x", ns_a).unwrap();
        let id_b0 = dict.get_or_insert("http://b.org/y", ns_b).unwrap();
        let id_a1 = dict.get_or_insert("http://a.org/z", ns_a).unwrap();

        // Each namespace has its own local_id counter
        assert_eq!(id_a0, (10u64 << 48) | 0); // ns_a, local_id=0
        assert_eq!(id_b0, (20u64 << 48) | 0); // ns_b, local_id=0
        assert_eq!(id_a1, (10u64 << 48) | 1); // ns_a, local_id=1

        assert_eq!(dict.len(), 3);
        assert!(!dict.needs_wide());
    }

    #[test]
    fn test_subject_dict_needs_wide() {
        let mut dict = SubjectDict::new_memory();
        let ns: u16 = 5;

        // Insert u16::MAX + 1 subjects to trigger needs_wide
        for i in 0..=(u16::MAX as u64) {
            let iri = format!("http://example.org/entity/{}", i);
            dict.get_or_insert(&iri, ns).unwrap();
        }
        // At this point local_id went from 0 to 65535 (u16::MAX) → still narrow
        // The u16::MAX-th subject has local_id = 65535, which fits u16
        // But the (u16::MAX+1)-th subject gets local_id = 65536, which exceeds u16
        assert_eq!(dict.len(), (u16::MAX as u64) + 1);
        // local_ids 0..=65535 all fit u16, 65536 exceeds → needs_wide should be true
        // Actually: 65536 subjects means local_ids 0..65535. The 65536th (index 65535)
        // has local_id=65535 which equals u16::MAX. We inserted u16::MAX+1 subjects,
        // so the last one (65536th) has local_id=65535. Not yet wide.
        // Let's insert one more to actually trigger it.
        dict.get_or_insert("http://example.org/entity/overflow", ns).unwrap();
        assert!(dict.needs_wide(), "should need wide after exceeding u16::MAX local_id");
    }

    // ---- PredicateDict tests ----

    #[test]
    fn test_predicate_dict_insert_and_lookup() {
        let mut dict = PredicateDict::new();

        let id1 = dict.get_or_insert("http://example.org/name");
        let id2 = dict.get_or_insert("http://example.org/age");
        let id1_again = dict.get_or_insert("http://example.org/name");

        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id1, id1_again);
        assert_eq!(dict.len(), 2);
    }

    #[test]
    fn test_predicate_dict_str_lookup() {
        let mut dict = PredicateDict::new();
        dict.get_or_insert("http://example.org/name");
        dict.get_or_insert("http://example.org/age");

        // Lookup with &str (no allocation)
        assert_eq!(dict.get("http://example.org/name"), Some(0));
        assert_eq!(dict.get("http://example.org/age"), Some(1));
        assert_eq!(dict.get("http://example.org/missing"), None);
    }

    #[test]
    fn test_predicate_dict_resolve() {
        let mut dict = PredicateDict::new();
        dict.get_or_insert("alpha");
        dict.get_or_insert("beta");

        assert_eq!(dict.resolve(0), Some("alpha"));
        assert_eq!(dict.resolve(1), Some("beta"));
        assert_eq!(dict.resolve(2), None);
    }

    // ---- LanguageTagDict tests ----

    #[test]
    fn test_lang_dict_insert_and_resolve() {
        let mut dict = LanguageTagDict::new();

        assert_eq!(dict.get_or_insert(None), 0);
        assert_eq!(dict.get_or_insert(Some("en")), 1);
        assert_eq!(dict.get_or_insert(Some("fr")), 2);
        assert_eq!(dict.get_or_insert(Some("en")), 1); // dedup
        assert_eq!(dict.len(), 2);

        assert_eq!(dict.resolve(0), None);
        assert_eq!(dict.resolve(1), Some("en"));
        assert_eq!(dict.resolve(2), Some("fr"));
    }

    #[test]
    fn test_lang_dict_clear() {
        let mut dict = LanguageTagDict::new();
        dict.get_or_insert(Some("en"));
        dict.get_or_insert(Some("de"));
        assert_eq!(dict.len(), 2);

        dict.clear();
        assert_eq!(dict.len(), 0);
        assert!(dict.is_empty());

        // After clear, new insertions start fresh
        assert_eq!(dict.get_or_insert(Some("ja")), 1);
    }

    #[test]
    fn test_lang_dict_iter() {
        let mut dict = LanguageTagDict::new();
        dict.get_or_insert(Some("en"));
        dict.get_or_insert(Some("fr"));
        dict.get_or_insert(Some("de"));

        let pairs: Vec<_> = dict.iter().collect();
        assert_eq!(pairs, vec![(1, "en"), (2, "fr"), (3, "de")]);
    }

    // ---- GlobalDicts tests ----

    #[test]
    fn test_global_dicts_memory() {
        let mut dicts = GlobalDicts::new_memory();
        dicts.subjects.get_or_insert("http://example.org/Alice", 100).unwrap();
        dicts.predicates.get_or_insert("http://example.org/name");
        dicts.strings.get_or_insert("Alice");
        dicts.languages.get_or_insert(Some("en"));

        assert_eq!(dicts.subjects.len(), 1);
        assert_eq!(dicts.predicates.len(), 1);
        assert_eq!(dicts.strings.len(), 1);
        assert_eq!(dicts.languages.len(), 1);
        // datatypes dict has 14 reserved entries
        assert_eq!(dicts.datatypes.len(), 14);
    }

    // ---- Datatype dict tests ----

    #[test]
    fn test_new_datatype_dict_reserved_entries() {
        let d = new_datatype_dict();
        assert_eq!(d.len(), 14);

        // Verify reserved positions match dt_ids constants
        assert_eq!(d.get("@id"), Some(dt_ids::ID as u32));
        assert_eq!(d.get(fluree_vocab::xsd::STRING), Some(dt_ids::STRING as u32));
        assert_eq!(d.get(fluree_vocab::xsd::BOOLEAN), Some(dt_ids::BOOLEAN as u32));
        assert_eq!(d.get(fluree_vocab::xsd::INTEGER), Some(dt_ids::INTEGER as u32));
        assert_eq!(d.get(fluree_vocab::xsd::LONG), Some(dt_ids::LONG as u32));
        assert_eq!(d.get(fluree_vocab::xsd::DECIMAL), Some(dt_ids::DECIMAL as u32));
        assert_eq!(d.get(fluree_vocab::xsd::DOUBLE), Some(dt_ids::DOUBLE as u32));
        assert_eq!(d.get(fluree_vocab::xsd::FLOAT), Some(dt_ids::FLOAT as u32));
        assert_eq!(d.get(fluree_vocab::xsd::DATE_TIME), Some(dt_ids::DATE_TIME as u32));
        assert_eq!(d.get(fluree_vocab::xsd::DATE), Some(dt_ids::DATE as u32));
        assert_eq!(d.get(fluree_vocab::xsd::TIME), Some(dt_ids::TIME as u32));
        assert_eq!(d.get(fluree_vocab::rdf::LANG_STRING), Some(dt_ids::LANG_STRING as u32));
        assert_eq!(d.get("@json"), Some(dt_ids::JSON as u32));
        assert_eq!(d.get("@vector"), Some(dt_ids::VECTOR as u32));
    }

    #[test]
    fn test_datatype_dict_idempotent_insert() {
        let mut d = new_datatype_dict();
        // Re-inserting a reserved type returns the same ID
        assert_eq!(d.get_or_insert("@id"), dt_ids::ID as u32);
        assert_eq!(d.get_or_insert(fluree_vocab::xsd::STRING), dt_ids::STRING as u32);
        assert_eq!(d.len(), 14); // no new entries
    }

    #[test]
    fn test_datatype_dict_dynamic_assignment() {
        let mut d = new_datatype_dict();
        // Custom/unknown types get dynamic IDs starting at 14
        let g_year_id = d.get_or_insert("http://www.w3.org/2001/XMLSchema#gYear");
        assert_eq!(g_year_id, dt_ids::RESERVED_COUNT as u32); // 14
        let custom_id = d.get_or_insert("http://example.org/custom#myType");
        assert_eq!(custom_id, 15);
        assert_eq!(d.len(), 16);

        // Re-insert returns same ID
        assert_eq!(d.get_or_insert("http://www.w3.org/2001/XMLSchema#gYear"), g_year_id);
    }

    // ---- String reverse index tests ----

    #[test]
    fn test_string_reverse_index_write_and_read() {
        let dir = std::env::temp_dir().join("fluree_test_string_rev");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("strings.rev");

        let mut dict = PredicateDict::new();
        dict.get_or_insert("SIGIR");
        dict.get_or_insert("VLDB");
        dict.get_or_insert("SIGMOD");

        dict.write_reverse_index(&path).unwrap();

        // Verify file structure
        let data = std::fs::read(&path).unwrap();
        assert_eq!(&data[0..4], b"LRV1");
        let count = u32::from_le_bytes(data[4..8].try_into().unwrap());
        assert_eq!(count, 3);
        assert_eq!(data.len(), 8 + 3 * 20); // header + 3 records × 20 bytes

        // Verify round-trip: hash each string and find it via binary search
        for (expected_str, expected_id) in &[("SIGIR", 0u32), ("VLDB", 1), ("SIGMOD", 2)] {
            let hash = xxhash_rust::xxh3::xxh3_128(expected_str.as_bytes());
            let target_hi = (hash >> 64) as u64;
            let target_lo = hash as u64;

            // Binary search over the written records
            let record_data = &data[8..];
            let record_size = 20;
            let mut lo = 0usize;
            let mut hi = count as usize;
            let mut found = None;
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let off = mid * record_size;
                let mid_hi = u64::from_le_bytes(record_data[off..off + 8].try_into().unwrap());
                let mid_lo = u64::from_le_bytes(record_data[off + 8..off + 16].try_into().unwrap());
                match (mid_hi, mid_lo).cmp(&(target_hi, target_lo)) {
                    std::cmp::Ordering::Less => lo = mid + 1,
                    std::cmp::Ordering::Greater => hi = mid,
                    std::cmp::Ordering::Equal => {
                        let str_id = u32::from_le_bytes(record_data[off + 16..off + 20].try_into().unwrap());
                        found = Some(str_id);
                        break;
                    }
                }
            }
            assert_eq!(found, Some(*expected_id), "failed to find {} in reverse index", expected_str);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
