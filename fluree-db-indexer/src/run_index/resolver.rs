//! CommitResolver: transforms RawOps into RunRecords using global dictionaries.
//!
//! This is the core of Phase B — dictionary resolution. For each commit's ops:
//! 1. Look up namespace prefix from ns_code
//! 2. Hash prefix + name using streaming xxh3_128 (no IRI concatenation on hot path)
//! 3. Resolve to global u32 ID via SubjectDict/PredicateDict
//! 4. Encode object value as ValueId
//! 5. Resolve datatype → dict ID from (dt_ns_code, dt_name)
//! 6. Emit RunRecord

use super::global_dict::GlobalDicts;
use super::run_record::{RunRecord, NO_LIST_INDEX};
use super::run_writer::RecordSink;
use chrono;
use fluree_db_core::temporal::{Date, DateTime, Time};
use super::global_dict::dt_ids;
use fluree_db_core::value_id::ValueId;
use fluree_db_novelty::commit_v2::envelope::CommitV2Envelope;
use fluree_db_novelty::commit_v2::raw_reader::{CommitOps, RawObject, RawOp};
use fluree_db_novelty::commit_v2::{load_commit_ops, CommitV2Error};
use fluree_vocab::{fluree, ledger};
use std::collections::HashMap;
use std::io;
use xxhash_rust::xxh3::Xxh3;

/// Resolves commit-local ops into globally-addressed RunRecords.
pub struct CommitResolver {
    /// namespace_code → prefix IRI.
    /// Seeded from `default_namespace_codes()`, updated by commit namespace_deltas.
    ///
    /// **Invariant:** ns_code → prefix is stable once assigned. A namespace
    /// delta can introduce new codes but never changes existing mappings.
    ns_prefixes: HashMap<i32, String>,
    /// Reusable xxh3 streaming hasher (avoids per-op hasher construction).
    hasher: Xxh3,
}

impl CommitResolver {
    /// Create a new resolver seeded with the default namespace prefix mappings.
    pub fn new() -> Self {
        Self {
            ns_prefixes: fluree_db_core::default_namespace_codes(),
            hasher: Xxh3::new(),
        }
    }

    /// Apply a commit's namespace delta to update prefix mappings.
    ///
    /// New namespace codes are added; existing codes are never overwritten
    /// (the prefix for a code is stable once assigned).
    pub fn apply_namespace_delta(&mut self, delta: &HashMap<i32, String>) {
        for (&code, prefix) in delta {
            self.ns_prefixes.entry(code).or_insert_with(|| prefix.clone());
        }
    }

    /// Resolve one commit's ops into RunRecords, pushing them to the writer.
    ///
    /// Returns the number of records emitted.
    pub fn resolve_commit_ops<W: RecordSink>(
        &mut self,
        commit_ops: &CommitOps,
        dicts: &mut GlobalDicts,
        writer: &mut W,
    ) -> Result<u32, ResolverError> {
        let t = commit_ops.t;
        let mut count = 0u32;

        commit_ops.for_each_op(|raw_op: RawOp<'_>| {
            let record = self.resolve_single_op(&raw_op, t, dicts)?;
            writer
                .push(record, &mut dicts.languages)
                .map_err(|e| CommitV2Error::InvalidOp(format!("run writer error: {}", e)))?;
            count += 1;
            Ok(())
        })?;

        Ok(count)
    }

    /// Resolve a raw commit blob end-to-end: parse, apply namespace delta, resolve ops,
    /// and emit txn-meta records.
    ///
    /// Convenience wrapper that combines [`load_commit_ops`], [`apply_namespace_delta`],
    /// [`resolve_commit_ops`], and [`emit_txn_meta`]. Returns `(op_count, t)`.
    ///
    /// This is the primary entry point for downstream orchestration (e.g., the
    /// ingest crate) — callers just feed raw blob bytes without needing to know
    /// about `CommitOps` or namespace deltas.
    ///
    /// `commit_address` is the storage address of this commit (e.g.,
    /// `"fluree:file://ledger/commit/<hex>.json"`), used to derive the canonical
    /// commit IRI for txn-meta records.
    ///
    /// `ledger_alias` is the ledger name (e.g., `"my-ledger"`), stored as
    /// `ledger:alias` in the txn-meta graph.
    pub fn resolve_blob<W: RecordSink>(
        &mut self,
        bytes: &[u8],
        commit_address: &str,
        ledger_alias: &str,
        dicts: &mut GlobalDicts,
        writer: &mut W,
    ) -> Result<(u32, i64), ResolverError> {
        let commit_ops = load_commit_ops(bytes)?;
        self.apply_namespace_delta(&commit_ops.envelope.namespace_delta);
        let mut op_count = self.resolve_commit_ops(&commit_ops, dicts, writer)?;
        op_count += self.emit_txn_meta(
            commit_address,
            ledger_alias,
            &commit_ops.envelope,
            dicts,
            writer,
        )?;
        Ok((op_count, commit_ops.t))
    }

    /// Access the accumulated namespace prefix map (code → prefix IRI).
    ///
    /// Needed for persistence at end of run generation (Step 2: namespaces.json).
    pub fn ns_prefixes(&self) -> &HashMap<i32, String> {
        &self.ns_prefixes
    }

    /// Emit txn-meta RunRecords for a single commit into the txn-meta graph (g_id=1).
    ///
    /// Mirrors the canonical `generate_commit_flakes()` in
    /// `fluree-db-novelty/src/commit_flakes.rs`. Two subjects per commit:
    ///
    /// - **Commit subject** (`fluree:commit:sha256:<hex>`): address, alias, v, time, data, previous
    /// - **DB subject** (`fluree:db:sha256:<hex>`): t
    ///
    /// Returns the number of records emitted.
    pub fn emit_txn_meta<W: RecordSink>(
        &mut self,
        commit_address: &str,
        ledger_alias: &str,
        envelope: &CommitV2Envelope,
        dicts: &mut GlobalDicts,
        writer: &mut W,
    ) -> Result<u32, ResolverError> {
        // 1. Extract commit hash from address
        let hex = match extract_commit_hex(commit_address) {
            Some(h) => h,
            None => {
                tracing::warn!(
                    address = commit_address,
                    "cannot extract commit hash from address; skipping txn-meta"
                );
                return Ok(0);
            }
        };

        // 2. g_id=1 (pre-reserved in GlobalDicts::new())
        let g_id = dicts
            .graphs
            .get_or_insert_parts(fluree::LEDGER, "transactions")
            + 1;
        debug_assert_eq!(g_id, 1, "txn-meta graph must be g_id=1");

        let t = envelope.t;

        // 3. Resolve commit subject: "fluree:commit:sha256:<hex>"
        let commit_iri = format!("{}{}", fluree::COMMIT, hex);
        let commit_s_id = dicts.subjects.get_or_insert(&commit_iri)?;

        // 4. Resolve predicate p_ids
        let p_address = dicts
            .predicates
            .get_or_insert_parts(fluree::LEDGER, ledger::ADDRESS);
        let p_alias = dicts
            .predicates
            .get_or_insert_parts(fluree::LEDGER, ledger::ALIAS);
        let p_v = dicts
            .predicates
            .get_or_insert_parts(fluree::LEDGER, ledger::V);
        let p_time = dicts
            .predicates
            .get_or_insert_parts(fluree::LEDGER, ledger::TIME);
        let p_previous = dicts
            .predicates
            .get_or_insert_parts(fluree::LEDGER, ledger::PREVIOUS);
        let p_t = dicts
            .predicates
            .get_or_insert_parts(fluree::LEDGER, ledger::T);

        let mut count = 0u32;

        // Helper to push a record into the writer
        let mut push = |s_id: u32,
                        p_id: u32,
                        o: ValueId,
                        dt: u16|
         -> Result<(), ResolverError> {
            let record = RunRecord {
                g_id,
                s_id,
                p_id,
                dt,
                lang_id: 0,
                o,
                t,
                op: 1, // assert
                _pad: [0; 3],
                i: NO_LIST_INDEX,
            };
            writer
                .push(record, &mut dicts.languages)
                .map_err(ResolverError::Io)?;
            count += 1;
            Ok(())
        };

        // === Commit subject records ===

        // ledger:address (STRING)
        let addr_str_id = dicts.strings.get_or_insert(commit_address);
        push(
            commit_s_id,
            p_address,
            ValueId::lex_id(addr_str_id),
            dt_ids::STRING,
        )?;

        // ledger:alias (STRING)
        let alias_str_id = dicts.strings.get_or_insert(ledger_alias);
        push(
            commit_s_id,
            p_alias,
            ValueId::lex_id(alias_str_id),
            dt_ids::STRING,
        )?;

        // ledger:v (INTEGER)
        let v_val = ValueId::num_int(envelope.v as i64)
            .unwrap_or_else(|| ValueId::lex_id(dicts.strings.get_or_insert(&envelope.v.to_string())));
        push(commit_s_id, p_v, v_val, dt_ids::INTEGER)?;

        // ledger:time (LONG) — epoch milliseconds (skipped if ISO parse fails)
        if let Some(time_str) = &envelope.time {
            if let Some(epoch_ms) = iso_to_epoch_ms(time_str) {
                if let Some(time_val) = ValueId::num_int(epoch_ms) {
                    push(commit_s_id, p_time, time_val, dt_ids::LONG)?;
                }
            }
        }

        // ledger:t (INTEGER)
        let t_val = ValueId::num_int(t)
            .unwrap_or_else(|| ValueId::lex_id(dicts.strings.get_or_insert(&t.to_string())));
        push(commit_s_id, p_t, t_val, dt_ids::INTEGER)?;

        // ledger:previous (ID) — ref to previous commit
        if let Some(prev_ref) = &envelope.previous_ref {
            if let Some(prev_id) = &prev_ref.id {
                // prev_id is like "fluree:commit:sha256:<hex>"
                let prev_s_id = dicts.subjects.get_or_insert(prev_id)?;
                push(
                    commit_s_id,
                    p_previous,
                    ValueId::iri_id(prev_s_id),
                    dt_ids::ID,
                )?;
            }
        }

        Ok(count)
    }

    /// Resolve a single RawOp into a RunRecord.
    fn resolve_single_op(
        &mut self,
        op: &RawOp<'_>,
        t: i64,
        dicts: &mut GlobalDicts,
    ) -> Result<RunRecord, CommitV2Error> {
        // 1. Resolve graph
        let g_id = self.resolve_graph(op.g_ns_code, op.g_name, dicts)
            .map_err(|e| CommitV2Error::InvalidOp(format!("graph resolve: {}", e)))?;

        // 2. Resolve subject (streaming hash)
        let s_id = self.resolve_subject(op.s_ns_code, op.s_name, dicts)
            .map_err(|e| CommitV2Error::InvalidOp(format!("subject resolve: {}", e)))?;

        // 3. Resolve predicate
        let p_id = self.resolve_predicate(op.p_ns_code, op.p_name, dicts);

        // 4. Resolve datatype via dict lookup (lossless — any IRI gets an ID)
        let prefix = self.lookup_prefix(op.dt_ns_code);
        let dt_id = dicts.datatypes.get_or_insert_parts(prefix, op.dt_name);
        // Bulk import path: enforce u8 dt ids for now (imports are allowed to error here).
        // Operationally, the binary format supports widening dt to u16.
        if dt_id > u8::MAX as u32 {
            return Err(CommitV2Error::InvalidOp(format!(
                "import not available: datatype dict overflow (dt_id={} exceeds u8 max)",
                dt_id
            )));
        }
        let dt_id = dt_id as u16;

        // 5. Encode object → ValueId
        let o = self.resolve_object(&op.o, op.dt_ns_code, op.dt_name, dicts)
            .map_err(|e| CommitV2Error::InvalidOp(format!("object resolve: {}", e)))?;

        // 6. Language tag
        let lang_id = dicts.languages.get_or_insert(op.lang);

        // 7. List index
        let i = op.i;

        Ok(RunRecord {
            g_id,
            s_id,
            p_id,
            dt: dt_id,
            lang_id,
            o,
            t,
            op: op.op as u8,
            _pad: [0; 3],
            i: i.unwrap_or(NO_LIST_INDEX),
        })
    }

    // ---- Field resolvers ----

    /// Resolve graph: default graph (ns=0, name="") → g_id=0.
    /// Named graphs → g_id = graphs.get_or_insert(full_iri) + 1.
    fn resolve_graph(
        &mut self,
        ns_code: i32,
        name: &str,
        dicts: &mut GlobalDicts,
    ) -> io::Result<u32> {
        if ns_code == 0 && name.is_empty() {
            return Ok(0); // default graph
        }
        let prefix = self.lookup_prefix(ns_code);
        // +1 to reserve 0 for default graph
        Ok(dicts.graphs.get_or_insert_parts(prefix, name) + 1)
    }

    /// Resolve subject IRI → global s_id using streaming xxh3_128.
    fn resolve_subject(
        &mut self,
        ns_code: i32,
        name: &str,
        dicts: &mut GlobalDicts,
    ) -> io::Result<u32> {
        // Access ns_prefixes directly (not via lookup_prefix) so the borrow checker
        // can see that ns_prefixes and hasher are disjoint field borrows.
        let prefix = self.ns_prefixes.get(&ns_code).map(|s| s.as_str()).unwrap_or("");

        // Streaming hash: feed prefix + name without concatenation
        self.hasher.reset();
        self.hasher.update(prefix.as_bytes());
        self.hasher.update(name.as_bytes());
        let hash = self.hasher.digest128();

        // Closure captures &str refs — only allocates on miss (novel entry).
        dicts.subjects.get_or_insert_with_hash(hash, || {
            let mut s = String::with_capacity(prefix.len() + name.len());
            s.push_str(prefix);
            s.push_str(name);
            s
        })
    }

    /// Resolve predicate IRI → global p_id.
    fn resolve_predicate(
        &mut self,
        ns_code: i32,
        name: &str,
        dicts: &mut GlobalDicts,
    ) -> u32 {
        let prefix = self.lookup_prefix(ns_code);
        dicts.predicates.get_or_insert_parts(prefix, name)
    }

    /// Encode object value as ValueId.
    fn resolve_object(
        &mut self,
        obj: &RawObject<'_>,
        _dt_ns_code: i32,
        _dt_name: &str,
        dicts: &mut GlobalDicts,
    ) -> Result<ValueId, String> {
        match obj {
            RawObject::Long(v) => {
                // Try NUM_INT; fallback to LEX_ID if outside i60 range
                match ValueId::num_int(*v) {
                    Some(vid) => Ok(vid),
                    None => {
                        let id = dicts.strings.get_or_insert(&v.to_string());
                        Ok(ValueId::lex_id(id))
                    }
                }
            }
            RawObject::Double(v) => {
                // Phase 4: always LEX_ID for doubles (no NUM_FLOAT shortcut).
                // This means non-integer float values are ordered by string dict
                // insertion order, not by value. Per-predicate NUM_FLOAT dict
                // with midpoint-splitting ranks is a later optimization.
                let id = dicts.strings.get_or_insert(&v.to_string());
                Ok(ValueId::lex_id(id))
            }
            RawObject::Str(s) => {
                let id = dicts.strings.get_or_insert(s);
                Ok(ValueId::lex_id(id))
            }
            RawObject::Boolean(b) => {
                if *b {
                    Ok(ValueId::BOOL_TRUE)
                } else {
                    Ok(ValueId::BOOL_FALSE)
                }
            }
            RawObject::Ref { ns_code, name } => {
                // Resolve ref IRI → global subject ID → IRI_ID ValueId.
                // Access ns_prefixes directly for disjoint field borrow.
                let prefix = self.ns_prefixes.get(ns_code).map(|s| s.as_str()).unwrap_or("");
                self.hasher.reset();
                self.hasher.update(prefix.as_bytes());
                self.hasher.update(name.as_bytes());
                let hash = self.hasher.digest128();

                // Closure captures &str refs — only allocates on miss (novel entry).
                let id = dicts
                    .subjects
                    .get_or_insert_with_hash(hash, || {
                        let mut s = String::with_capacity(prefix.len() + name.len());
                        s.push_str(prefix);
                        s.push_str(name);
                        s
                    })
                    .map_err(|e| format!("ref resolve: {}", e))?;
                Ok(ValueId::iri_id(id))
            }
            RawObject::DateTimeStr(s) => {
                DateTime::parse(s)
                    .map_err(|e| format!("datetime parse: {}", e))
                    .and_then(|dt| {
                        let micros = dt.epoch_micros();
                        ValueId::datetime(micros).ok_or_else(|| {
                            format!("datetime {} epoch_micros {} exceeds i60 range", s, micros)
                        })
                    })
            }
            RawObject::DateStr(s) => {
                Date::parse(s)
                    .map(|d| ValueId::date(d.days_since_epoch()))
                    .map_err(|e| format!("date parse: {}", e))
            }
            RawObject::TimeStr(s) => {
                Time::parse(s)
                    .map(|t| ValueId::time(t.micros_since_midnight()))
                    .map_err(|e| format!("time parse: {}", e))
            }
            RawObject::BigIntStr(s) => {
                // Try to parse as i64 and use NUM_INT; fallback to LEX_ID
                if let Ok(v) = s.parse::<i64>() {
                    if let Some(vid) = ValueId::num_int(v) {
                        return Ok(vid);
                    }
                }
                let id = dicts.strings.get_or_insert(s);
                Ok(ValueId::lex_id(id))
            }
            RawObject::DecimalStr(s) => {
                // Always LEX_ID for decimals
                let id = dicts.strings.get_or_insert(s);
                Ok(ValueId::lex_id(id))
            }
            RawObject::JsonStr(s) => {
                let id = dicts.strings.get_or_insert(s);
                Ok(ValueId::json(id))
            }
            RawObject::Null => Ok(ValueId::NULL),
        }
    }

    /// Look up the prefix IRI for a namespace code.
    /// Returns "" if the code is unknown (should not happen with proper delta replay).
    fn lookup_prefix(&self, ns_code: i32) -> &str {
        self.ns_prefixes
            .get(&ns_code)
            .map(|s| s.as_str())
            .unwrap_or("")
    }
}

impl Default for CommitResolver {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Error type
// ============================================================================

/// Errors from the resolution pipeline.
#[derive(Debug)]
pub enum ResolverError {
    CommitV2(CommitV2Error),
    Io(io::Error),
    Resolve(String),
}

impl From<CommitV2Error> for ResolverError {
    fn from(e: CommitV2Error) -> Self {
        Self::CommitV2(e)
    }
}

impl From<io::Error> for ResolverError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl std::fmt::Display for ResolverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CommitV2(e) => write!(f, "commit-v2: {}", e),
            Self::Io(e) => write!(f, "I/O: {}", e),
            Self::Resolve(msg) => write!(f, "resolve: {}", msg),
        }
    }
}

impl std::error::Error for ResolverError {}

// ============================================================================
// Helper functions
// ============================================================================

/// Extract the hex hash from a commit storage address.
///
/// Addresses look like `fluree:file://ledger/commit/<64-hex>.json`.
/// Returns the hex portion (without `sha256:` prefix) or `None` if unparseable.
fn extract_commit_hex(address: &str) -> Option<&str> {
    // Strip the scheme: "fluree:file://..." → path after "://"
    let path = if let Some(rest) = address.strip_prefix("fluree:") {
        let pos = rest.find("://")?;
        &rest[pos + 3..]
    } else if let Some(pos) = address.find("://") {
        &address[pos + 3..]
    } else {
        return None;
    };

    // Last path segment, strip ".json" suffix
    let filename = path.rsplit('/').next()?;
    let hex = filename.strip_suffix(".json")?;

    // Validate: must be 64 hex chars (SHA-256)
    if hex.len() == 64 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(hex)
    } else {
        None
    }
}

/// Parse ISO-8601 timestamp to epoch milliseconds.
///
/// Returns `None` if parsing fails (caller skips emission rather than
/// poisoning the index with `0`).
fn iso_to_epoch_ms(iso: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(iso)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::run_writer::{RunWriter, RunWriterConfig};
    use crate::run_index::run_record::RunSortOrder;
    use fluree_db_novelty::commit_v2::op_codec::{encode_op, CommitDicts};
    use fluree_db_novelty::commit_v2::envelope::{encode_envelope_fields, CommitV2Envelope};
    use fluree_db_novelty::commit_v2::format::{self, CommitV2Footer, CommitV2Header, HEADER_LEN, FOOTER_LEN, HASH_LEN};
    use fluree_db_novelty::commit_v2::raw_reader::load_commit_ops;
    use fluree_db_core::{Flake, FlakeMeta, FlakeValue, Sid};
    use sha2::{Digest, Sha256};

    /// Build a minimal commit blob from flakes (reused from raw_reader tests).
    fn build_test_blob(flakes: &[Flake], t: i64) -> Vec<u8> {
        let mut dicts = CommitDicts::new();
        let mut ops_buf = Vec::new();
        for f in flakes {
            encode_op(f, &mut dicts, &mut ops_buf).unwrap();
        }

        let envelope = CommitV2Envelope {
            t, v: 0,
            previous: None, previous_ref: None,
            namespace_delta: HashMap::new(),
            txn: None, time: None, data: None, index: None, indexed_at: None,
        };
        let mut envelope_bytes = Vec::new();
        encode_envelope_fields(&envelope, &mut envelope_bytes).unwrap();

        let dict_bytes: Vec<Vec<u8>> = vec![
            dicts.graph.serialize(), dicts.subject.serialize(),
            dicts.predicate.serialize(), dicts.datatype.serialize(),
            dicts.object_ref.serialize(),
        ];

        let ops_section_len = ops_buf.len() as u32;
        let envelope_len = envelope_bytes.len() as u32;
        let dict_start = HEADER_LEN + envelope_bytes.len() + ops_buf.len();
        let mut dict_locations = [format::DictLocation::default(); 5];
        let mut offset = dict_start as u64;
        for (i, d) in dict_bytes.iter().enumerate() {
            dict_locations[i] = format::DictLocation { offset, len: d.len() as u32 };
            offset += d.len() as u64;
        }

        let footer = CommitV2Footer { dicts: dict_locations, ops_section_len };
        let header = CommitV2Header {
            version: format::VERSION, flags: 0, t,
            op_count: flakes.len() as u32, envelope_len,
        };

        let total_len = HEADER_LEN + envelope_bytes.len() + ops_buf.len()
            + dict_bytes.iter().map(|d| d.len()).sum::<usize>()
            + FOOTER_LEN + HASH_LEN;
        let mut blob = vec![0u8; total_len];

        let mut pos = 0;
        header.write_to(&mut blob[pos..]);
        pos += HEADER_LEN;
        blob[pos..pos + envelope_bytes.len()].copy_from_slice(&envelope_bytes);
        pos += envelope_bytes.len();
        blob[pos..pos + ops_buf.len()].copy_from_slice(&ops_buf);
        pos += ops_buf.len();
        for d in &dict_bytes {
            blob[pos..pos + d.len()].copy_from_slice(d);
            pos += d.len();
        }
        footer.write_to(&mut blob[pos..]);
        pos += FOOTER_LEN;
        let hash: [u8; 32] = Sha256::digest(&blob[..pos]).into();
        blob[pos..pos + HASH_LEN].copy_from_slice(&hash);
        blob
    }

    #[test]
    fn test_resolve_basic_ops() {
        let dir = std::env::temp_dir().join("fluree_test_resolver_basic");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let flakes = vec![
            Flake::new(
                Sid::new(101, "Alice"), Sid::new(101, "age"),
                FlakeValue::Long(30), Sid::new(2, "integer"), 1, true, None,
            ),
            Flake::new(
                Sid::new(101, "Alice"), Sid::new(101, "name"),
                FlakeValue::String("Alice".into()), Sid::new(2, "string"), 1, true, None,
            ),
            Flake::new(
                Sid::new(101, "Bob"), Sid::new(101, "age"),
                FlakeValue::Long(25), Sid::new(2, "integer"), 1, true, None,
            ),
        ];

        let blob = build_test_blob(&flakes, 1);
        let commit_ops = load_commit_ops(&blob).unwrap();

        let mut dicts = GlobalDicts::new_memory();
        let mut resolver = CommitResolver::new();

        // Add user namespace prefix (code 101)
        resolver.ns_prefixes.insert(101, "http://example.org/".to_string());

        let config = RunWriterConfig {
            buffer_budget_bytes: 1024 * 1024,
            sort_order: RunSortOrder::Spot,
            run_dir: dir.clone(),
        };
        let mut writer = RunWriter::new(config);

        let count = resolver.resolve_commit_ops(&commit_ops, &mut dicts, &mut writer).unwrap();
        assert_eq!(count, 3);

        // Check dictionary state
        assert_eq!(dicts.subjects.len(), 2); // Alice, Bob
        assert_eq!(dicts.predicates.len(), 2); // age, name
        assert_eq!(dicts.strings.len(), 1); // "Alice" (the string value)

        // Finish and read back
        let result = writer.finish(&mut dicts.languages).unwrap();
        assert_eq!(result.total_records, 3);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_resolve_ref_and_dedup() {
        let flakes = vec![
            // Alice knows Bob (Ref)
            Flake::new(
                Sid::new(101, "Alice"), Sid::new(101, "knows"),
                FlakeValue::Ref(Sid::new(101, "Bob")), Sid::new(1, "id"), 1, true, None,
            ),
            // Bob's age
            Flake::new(
                Sid::new(101, "Bob"), Sid::new(101, "age"),
                FlakeValue::Long(25), Sid::new(2, "integer"), 1, true, None,
            ),
        ];

        let blob = build_test_blob(&flakes, 1);
        let commit_ops = load_commit_ops(&blob).unwrap();

        let mut dicts = GlobalDicts::new_memory();
        let mut resolver = CommitResolver::new();
        resolver.ns_prefixes.insert(101, "http://example.org/".to_string());

        let dir = std::env::temp_dir().join("fluree_test_resolver_ref");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let config = RunWriterConfig {
            buffer_budget_bytes: 1024 * 1024,
            sort_order: RunSortOrder::Spot,
            run_dir: dir.clone(),
        };
        let mut writer = RunWriter::new(config);

        resolver.resolve_commit_ops(&commit_ops, &mut dicts, &mut writer).unwrap();

        // Alice and Bob are both in subject dict
        // Bob is referenced both as a subject and as an object ref
        // They should share the same global ID (dedup via hash)
        assert_eq!(dicts.subjects.len(), 2); // Alice, Bob
        // Predicate: knows, age
        assert_eq!(dicts.predicates.len(), 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_resolve_datetime() {
        let flakes = vec![
            Flake::new(
                Sid::new(101, "x"), Sid::new(101, "created"),
                FlakeValue::DateTime(Box::new(DateTime::parse("2024-01-15T10:30:00Z").unwrap())),
                Sid::new(2, "dateTime"), 1, true, None,
            ),
        ];

        let blob = build_test_blob(&flakes, 1);
        let commit_ops = load_commit_ops(&blob).unwrap();

        let mut dicts = GlobalDicts::new_memory();
        let mut resolver = CommitResolver::new();
        resolver.ns_prefixes.insert(101, "http://example.org/".to_string());

        let dir = std::env::temp_dir().join("fluree_test_resolver_dt");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let config = RunWriterConfig {
            buffer_budget_bytes: 1024 * 1024,
            sort_order: RunSortOrder::Spot,
            run_dir: dir.clone(),
        };
        let mut writer = RunWriter::new(config);

        let count = resolver.resolve_commit_ops(&commit_ops, &mut dicts, &mut writer).unwrap();
        assert_eq!(count, 1);

        let result = writer.finish(&mut dicts.languages).unwrap();
        let (_, _, records) = crate::run_index::read_run_file(&result.run_files[0].path).unwrap();
        assert_eq!(records.len(), 1);
        // Verify the ValueId tag is DATETIME (0x9)
        assert_eq!(records[0].o.tag(), 0x9);
        // Verify dt is DATE_TIME
        assert_eq!(records[0].dt, dt_ids::DATE_TIME);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_resolve_boolean_and_null() {
        let flakes = vec![
            Flake::new(
                Sid::new(101, "x"), Sid::new(101, "active"),
                FlakeValue::Boolean(true), Sid::new(2, "boolean"), 1, true, None,
            ),
            Flake::new(
                Sid::new(101, "x"), Sid::new(101, "deleted"),
                FlakeValue::Null, Sid::new(2, "string"), 1, true, None,
            ),
        ];

        let blob = build_test_blob(&flakes, 1);
        let commit_ops = load_commit_ops(&blob).unwrap();

        let mut dicts = GlobalDicts::new_memory();
        let mut resolver = CommitResolver::new();
        resolver.ns_prefixes.insert(101, "http://example.org/".to_string());

        let dir = std::env::temp_dir().join("fluree_test_resolver_bool");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let config = RunWriterConfig {
            buffer_budget_bytes: 1024 * 1024,
            sort_order: RunSortOrder::Spot,
            run_dir: dir.clone(),
        };
        let mut writer = RunWriter::new(config);

        resolver.resolve_commit_ops(&commit_ops, &mut dicts, &mut writer).unwrap();

        let result = writer.finish(&mut dicts.languages).unwrap();
        let (_, _, records) = crate::run_index::read_run_file(&result.run_files[0].path).unwrap();
        assert_eq!(records[0].o, ValueId::BOOL_TRUE);
        assert_eq!(records[1].o, ValueId::NULL);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_resolve_with_lang_tag() {
        let flakes = vec![
            Flake::new(
                Sid::new(101, "x"), Sid::new(101, "label"),
                FlakeValue::String("hello".into()), Sid::new(3, "langString"),
                1, true, Some(FlakeMeta::with_lang("en")),
            ),
            Flake::new(
                Sid::new(101, "x"), Sid::new(101, "label"),
                FlakeValue::String("bonjour".into()), Sid::new(3, "langString"),
                1, true, Some(FlakeMeta::with_lang("fr")),
            ),
        ];

        let blob = build_test_blob(&flakes, 1);
        let commit_ops = load_commit_ops(&blob).unwrap();

        let mut dicts = GlobalDicts::new_memory();
        let mut resolver = CommitResolver::new();
        resolver.ns_prefixes.insert(101, "http://example.org/".to_string());

        let dir = std::env::temp_dir().join("fluree_test_resolver_lang");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let config = RunWriterConfig {
            buffer_budget_bytes: 1024 * 1024,
            sort_order: RunSortOrder::Spot,
            run_dir: dir.clone(),
        };
        let mut writer = RunWriter::new(config);

        resolver.resolve_commit_ops(&commit_ops, &mut dicts, &mut writer).unwrap();

        // Should have 2 language tags
        assert_eq!(dicts.languages.len(), 2);
        assert_eq!(dicts.languages.resolve(1), Some("en"));
        assert_eq!(dicts.languages.resolve(2), Some("fr"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Txn-meta tests ----

    #[test]
    fn test_extract_commit_hex() {
        assert_eq!(
            super::extract_commit_hex(
                "fluree:file://test/main/commit/abc123def456abc123def456abc123def456abc123def456abc123def456abcd.json"
            ),
            Some("abc123def456abc123def456abc123def456abc123def456abc123def456abcd")
        );
        assert_eq!(
            super::extract_commit_hex(
                "fluree:s3://bucket/ledger/commit/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef.json"
            ),
            Some("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
        );
        // Too short
        assert_eq!(super::extract_commit_hex("fluree:file://test/commit/abc.json"), None);
        // No .json suffix
        assert_eq!(
            super::extract_commit_hex(
                "fluree:file://test/commit/abc123def456abc123def456abc123def456abc123def456abc123def456abcd"
            ),
            None
        );
    }

    #[test]
    fn test_iso_to_epoch_ms() {
        let ms = super::iso_to_epoch_ms("2025-01-20T12:00:00Z");
        assert!(ms.is_some());
        let ms = ms.unwrap();
        assert!(ms > 1737000000000);
        assert!(ms < 1738000000000);

        // Invalid timestamp returns None (skipped, not emitted as 0)
        assert_eq!(super::iso_to_epoch_ms("not-a-date"), None);
    }

    #[test]
    fn test_emit_txn_meta() {
        use fluree_db_novelty::commit_v2::envelope::CommitV2Envelope;
        use fluree_db_novelty::CommitRef;

        let dir = std::env::temp_dir().join("fluree_test_emit_txn_meta");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut dicts = GlobalDicts::new_memory();
        let mut resolver = CommitResolver::new();

        let config = RunWriterConfig {
            buffer_budget_bytes: 1024 * 1024,
            sort_order: RunSortOrder::Spot,
            run_dir: dir.clone(),
        };
        let mut writer = RunWriter::new(config);

        // Build an envelope with time + previous_ref
        let hex = "abc123def456abc123def456abc123def456abc123def456abc123def456abcd";
        let prev_hex = "0000000000000000000000000000000000000000000000000000000000000000";
        let commit_address = format!(
            "fluree:file://test/main/commit/{}.json", hex
        );
        let prev_commit_id = format!("fluree:commit:sha256:{}", prev_hex);

        let envelope = CommitV2Envelope {
            t: 42,
            v: 2,
            previous: None,
            previous_ref: Some(CommitRef::new("prev-addr").with_id(prev_commit_id.clone())),
            namespace_delta: HashMap::new(),
            txn: None,
            time: Some("2025-06-15T12:00:00Z".into()),
            data: None,
            index: None,
            indexed_at: None,
        };

        let count = resolver
            .emit_txn_meta(&commit_address, "test-ledger", &envelope, &mut dicts, &mut writer)
            .unwrap();

        // 6 records on commit subject: address, alias, v, time, t, previous
        assert_eq!(count, 6);

        // Verify g_id=1 reservation
        let g_id = dicts.graphs.get_or_insert_parts(
            fluree_vocab::fluree::LEDGER,
            "transactions",
        ) + 1;
        assert_eq!(g_id, 1);

        // Verify subjects created (commit + prev_commit; no separate DB subject)
        assert!(dicts.subjects.len() >= 2);

        // Flush and read back records
        let result = writer.finish(&mut dicts.languages).unwrap();
        assert_eq!(result.total_records, 6);

        let (_, _, records) = crate::run_index::read_run_file(&result.run_files[0].path).unwrap();
        assert_eq!(records.len(), 6);

        // All records should be in g_id=1
        for rec in &records {
            assert_eq!(rec.g_id, 1, "txn-meta records must be in g_id=1");
            assert_eq!(rec.op, 1, "txn-meta records must be asserts");
        }

        // Verify predicates were registered
        let p_address = dicts.predicates.get("https://ns.flur.ee/ledger#address");
        let p_time = dicts.predicates.get("https://ns.flur.ee/ledger#time");
        let p_t = dicts.predicates.get("https://ns.flur.ee/ledger#t");
        let p_previous = dicts.predicates.get("https://ns.flur.ee/ledger#previous");
        assert!(p_address.is_some(), "ledger:address predicate missing");
        assert!(p_time.is_some(), "ledger:time predicate missing");
        assert!(p_t.is_some(), "ledger:t predicate missing");
        assert!(p_previous.is_some(), "ledger:previous predicate missing");

        // Find the time record and verify it's NUM_INT (epoch ms) with dt_ids::LONG
        let time_pid = p_time.unwrap();
        let time_rec = records.iter().find(|r| r.p_id == time_pid).unwrap();
        assert_eq!(time_rec.dt, dt_ids::LONG, "ledger:time must be dt_ids::LONG");
        // Verify it's a NUM_INT (tag 0x3)
        assert_eq!(time_rec.o.tag(), 0x3, "ledger:time must be NUM_INT encoding");
        // Verify epoch ms is reasonable (2025)
        let epoch_ms = time_rec.o.decode_offset_binary();
        assert!(epoch_ms > 1718000000000, "epoch ms should be in 2025");

        // Find the t record (now on commit subject, not separate DB subject)
        let t_pid = p_t.unwrap();
        let t_rec = records.iter().find(|r| r.p_id == t_pid).unwrap();
        assert_eq!(t_rec.dt, dt_ids::INTEGER, "ledger:t must be dt_ids::INTEGER");
        assert_eq!(t_rec.o.tag(), 0x3, "ledger:t must be NUM_INT encoding");

        // Find the previous record and verify it's IRI_ID with dt_ids::ID
        let prev_pid = p_previous.unwrap();
        let prev_rec = records.iter().find(|r| r.p_id == prev_pid).unwrap();
        assert_eq!(prev_rec.dt, dt_ids::ID, "ledger:previous must be dt_ids::ID");
        assert_eq!(prev_rec.o.tag(), 0x5, "ledger:previous must be IRI_ID encoding");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_emit_txn_meta_minimal() {
        // Test with minimal envelope (no time, no previous_ref)
        use fluree_db_novelty::commit_v2::envelope::CommitV2Envelope;

        let dir = std::env::temp_dir().join("fluree_test_emit_txn_meta_min");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut dicts = GlobalDicts::new_memory();
        let mut resolver = CommitResolver::new();

        let config = RunWriterConfig {
            buffer_budget_bytes: 1024 * 1024,
            sort_order: RunSortOrder::Spot,
            run_dir: dir.clone(),
        };
        let mut writer = RunWriter::new(config);

        let hex = "abc123def456abc123def456abc123def456abc123def456abc123def456abcd";
        let commit_address = format!("fluree:file://test/main/commit/{}.json", hex);

        let envelope = CommitV2Envelope {
            t: 1,
            v: 2,
            previous: None,
            previous_ref: None,
            namespace_delta: HashMap::new(),
            txn: None,
            time: None,
            data: None,
            index: None,
            indexed_at: None,
        };

        let count = resolver
            .emit_txn_meta(&commit_address, "test-ledger", &envelope, &mut dicts, &mut writer)
            .unwrap();

        // 4 records on commit subject: address, alias, v, t
        // No time, no previous — those are conditional
        assert_eq!(count, 4);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_global_dicts_reserves_g_id_1() {
        let dicts = GlobalDicts::new_memory();
        // txn-meta graph is pre-inserted at position 0 in graphs dict
        let g_id = dicts.graphs.get("https://ns.flur.ee/ledger#transactions");
        assert_eq!(g_id, Some(0), "txn-meta graph must be first entry (dict id=0, g_id=0+1=1)");
    }
}
