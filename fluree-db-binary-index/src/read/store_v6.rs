//! V6 index store: loads FIR6 roots and provides value decoding via `o_type`.
//!
//! Parallels `BinaryIndexStore` (V5) but uses the V3 columnar format:
//! - `BranchManifestV3` for routing (V3 keys)
//! - `o_type` table for decode dispatch (replaces `ObjKind` if/else chain)
//! - `ColumnBatch` output (replaces Region1/Region2)
//!
//! Dict loading and arena loading reuse the same infrastructure as V5
//! (same `DictRefsV5`, same `ForwardPackReader`, same arena types).

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::value_id::ObjKey;
use fluree_db_core::GraphId;
use fluree_db_core::{ContentId, ContentStore, FlakeValue, PrefixTrie, Sid};

use crate::dict::forward_pack::{KIND_STRING_FWD, KIND_SUBJECT_FWD};
use crate::dict::global_dict::{LanguageTagDict, PredicateDict};
use crate::dict::pack_reader::ForwardPackReader;
use crate::format::branch_v3::{read_branch_v3_from_bytes, BranchManifestV3};
use crate::format::index_root_v6::{IndexRootV6, OTypeTableEntry};
use crate::format::run_record::RunSortOrder;

use super::binary_index_store::{
    cache_bytes_to_path_atomic, fetch_cached_bytes, fetch_cached_bytes_cid,
    load_dict_tree_from_cas, DictionarySet,
};
use super::leaflet_cache::LeafletCache;

// ============================================================================
// Per-graph V3 index data
// ============================================================================

struct GraphIndexV3 {
    orders: HashMap<RunSortOrder, BranchManifestV3>,
    numbig: HashMap<u32, crate::arena::numbig::NumBigArena>,
    vectors: HashMap<u32, crate::arena::vector::LazyVectorArena>,
    spatial: HashMap<u32, Arc<dyn fluree_db_spatial::SpatialIndexProvider>>,
    fulltext: HashMap<u32, Arc<crate::arena::fulltext::FulltextArena>>,
}

// ============================================================================
// BinaryIndexStoreV6
// ============================================================================

/// V6 index store — reads FLI3/FBR3/FHS1 artifacts via FIR6 root.
///
/// Parallels `BinaryIndexStore` with V3 format specifics:
/// - Routing via `BranchManifestV3` (V3 keys, sidecar CIDs)
/// - Value decoding via `o_type` table (replaces ObjKind dispatch)
/// - Dict/arena infrastructure shared with V5 (same types, same loading)
pub struct BinaryIndexStoreV6 {
    dicts: DictionarySet,
    graph_indexes: HashMap<GraphId, GraphIndexV3>,
    /// o_type table: full list (for iteration / serialization).
    o_type_table: Vec<OTypeTableEntry>,
    /// O(1) lookup: o_type value → index into o_type_table.
    o_type_index: HashMap<u16, usize>,
    cas: Option<Arc<dyn ContentStore>>,
    cache_dir: PathBuf,
    leaflet_cache: Option<Arc<LeafletCache>>,
    max_t: i64,
    base_t: i64,
    language_tags: Vec<String>,
}

impl BinaryIndexStoreV6 {
    /// Decode FIR6 bytes and load the store.
    pub async fn load_from_root_bytes(
        cs: Arc<dyn ContentStore>,
        bytes: &[u8],
        cache_dir: &Path,
        leaflet_cache: Option<Arc<LeafletCache>>,
    ) -> io::Result<Self> {
        let root = IndexRootV6::decode(bytes)?;
        Self::load_from_root_v6(cs, &root, cache_dir, leaflet_cache).await
    }

    /// Load from a parsed IndexRootV6.
    pub async fn load_from_root_v6(
        cs: Arc<dyn ContentStore>,
        root: &IndexRootV6,
        cache_dir: &Path,
        leaflet_cache: Option<Arc<LeafletCache>>,
    ) -> io::Result<Self> {
        tracing::info!("BinaryIndexStoreV6::load_from_root_v6 starting");
        std::fs::create_dir_all(cache_dir)?;

        // ── Dict loading (same as V5) ──────────────────────────────
        let dicts =
            build_dictionary_set(Arc::clone(&cs), root, cache_dir, leaflet_cache.as_ref()).await?;

        // ── Per-graph specialty arenas (same as V5) ────────────────
        let mut per_graph_arenas = load_per_graph_arenas(
            Arc::clone(&cs),
            &root.graph_arenas,
            cache_dir,
            leaflet_cache.as_ref(),
        )
        .await?;

        // ── Graph index routing ────────────────────────────────────
        let mut graph_indexes: HashMap<GraphId, GraphIndexV3> = HashMap::new();

        // Default graph (g_id=0): inline leaf entries from root.
        for dgo in &root.default_graph_orders {
            let branch = BranchManifestV3 {
                leaves: dgo.leaves.clone(),
            };
            let gi = graph_indexes.entry(0).or_insert_with(|| GraphIndexV3 {
                orders: HashMap::new(),
                numbig: HashMap::new(),
                vectors: HashMap::new(),
                spatial: HashMap::new(),
                fulltext: HashMap::new(),
            });
            gi.orders.insert(dgo.order, branch);
        }

        // Named graphs: fetch FBR3 branch manifests from CAS.
        for ng in &root.named_graphs {
            for (order, branch_cid) in &ng.orders {
                let branch_bytes =
                    fetch_cached_bytes_cid(cs.as_ref(), branch_cid, cache_dir).await?;
                let branch = read_branch_v3_from_bytes(&branch_bytes)?;
                let gi = graph_indexes
                    .entry(ng.g_id)
                    .or_insert_with(|| GraphIndexV3 {
                        orders: HashMap::new(),
                        numbig: HashMap::new(),
                        vectors: HashMap::new(),
                        spatial: HashMap::new(),
                        fulltext: HashMap::new(),
                    });
                gi.orders.insert(*order, branch);
            }
        }

        // Inject per-graph arenas into graph indexes.
        for (g_id, arenas) in per_graph_arenas.drain() {
            let gi = graph_indexes.entry(g_id).or_insert_with(|| GraphIndexV3 {
                orders: HashMap::new(),
                numbig: HashMap::new(),
                vectors: HashMap::new(),
                spatial: HashMap::new(),
                fulltext: HashMap::new(),
            });
            gi.numbig = arenas.numbig;
            gi.vectors = arenas.vectors;
            gi.spatial = arenas.spatial;
            gi.fulltext = arenas.fulltext;
        }

        let leaf_count: usize = graph_indexes
            .values()
            .flat_map(|gi| gi.orders.values())
            .map(|b| b.leaves.len())
            .sum();
        tracing::info!(
            graphs = graph_indexes.len(),
            leaves = leaf_count,
            "loaded V6 graph indexes"
        );

        let o_type_table = root.o_type_table.clone();
        let o_type_index: HashMap<u16, usize> = o_type_table
            .iter()
            .enumerate()
            .map(|(i, e)| (e.o_type, i))
            .collect();

        Ok(Self {
            dicts,
            graph_indexes,
            o_type_table,
            o_type_index,
            cas: Some(cs),
            cache_dir: cache_dir.to_path_buf(),
            leaflet_cache,
            max_t: root.index_t,
            base_t: root.base_t,
            language_tags: root.language_tags.clone(),
        })
    }

    // ── Public accessors ───────────────────────────────────────────

    pub fn max_t(&self) -> i64 {
        self.max_t
    }

    pub fn base_t(&self) -> i64 {
        self.base_t
    }

    /// Get the branch manifest for a graph + sort order.
    pub fn branch_for_order(
        &self,
        g_id: GraphId,
        order: RunSortOrder,
    ) -> Option<&BranchManifestV3> {
        self.graph_indexes
            .get(&g_id)
            .and_then(|gi| gi.orders.get(&order))
    }

    pub fn leaflet_cache(&self) -> Option<&Arc<LeafletCache>> {
        self.leaflet_cache.as_ref()
    }

    /// Fetch leaf bytes by CID: local path first, then CAS with caching.
    pub fn get_leaf_bytes_sync(&self, leaf_cid: &ContentId) -> io::Result<Vec<u8>> {
        let cs = self
            .cas
            .as_ref()
            .ok_or_else(|| io::Error::other("no content store"))?;

        // Try local path first.
        if let Some(local_path) = cs.resolve_local_path(leaf_cid) {
            return std::fs::read(local_path);
        }

        // Check cache.
        let cache_path = self.cache_dir.join(leaf_cid.to_string());
        if cache_path.exists() {
            return std::fs::read(&cache_path);
        }

        // Fetch from CAS via sync bridge: capture the Tokio handle on the caller's
        // thread (which has a runtime context), then spawn a dedicated OS thread that
        // uses it to block_on the async fetch. Same pattern as V5 ensure_index_leaf_cached.
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::other("leaf fetch requires a Tokio runtime"))?;
        let cs = Arc::clone(cs);
        let cid = leaf_cid.clone();
        let cache_path_owned = cache_path.clone();
        let (tx, rx) = std::sync::mpsc::sync_channel::<io::Result<Vec<u8>>>(1);
        std::thread::spawn(move || {
            let result = handle.block_on(async {
                let data = cs
                    .get(&cid)
                    .await
                    .map_err(|e| io::Error::other(format!("CAS fetch failed: {e}")))?;
                cache_bytes_to_path_atomic(&cache_path_owned, &data)?;
                Ok(data)
            });
            let _ = tx.send(result);
        });
        rx.recv()
            .map_err(|_| io::Error::other("leaf fetch thread panicked"))?
    }

    // ── Value decoding ─────────────────────────────────────────────

    /// Decode a value from `(o_type, o_key)` to `FlakeValue`.
    ///
    /// Routes via `OType::decode_kind()` for static dispatch. Per-graph arenas
    /// (NumBig, Vector) require `(g_id, p_id)` context. Dict-backed types
    /// (String, IRI, JSON, langString) go through forward pack lookups.
    ///
    /// Uses the same decode helpers as V5's `decode_value_no_graph` (ObjKey methods,
    /// chrono conversions, temporal parsing) to ensure output compatibility.
    pub fn decode_value_v3(
        &self,
        o_type: u16,
        o_key: u64,
        p_id: u32,
        g_id: GraphId,
    ) -> io::Result<FlakeValue> {
        let ot = OType::from_u16(o_type);
        let key = ObjKey::from_u64(o_key);

        match ot.decode_kind() {
            DecodeKind::Sentinel | DecodeKind::Null => Ok(FlakeValue::Null),
            DecodeKind::Bool => Ok(FlakeValue::Boolean(o_key != 0)),
            DecodeKind::I64 => Ok(FlakeValue::Long(key.decode_i64())),
            DecodeKind::F64 => Ok(FlakeValue::Double(key.decode_f64())),
            DecodeKind::Date => {
                let days = key.decode_date();
                let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                    .unwrap_or(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
                let iso = date.format("%Y-%m-%d").to_string();
                match fluree_db_core::temporal::Date::parse(&iso) {
                    Ok(d) => Ok(FlakeValue::Date(Box::new(d))),
                    Err(_) => Ok(FlakeValue::String(iso)),
                }
            }
            DecodeKind::Time => {
                let micros = key.decode_time();
                let secs = (micros / 1_000_000) as u32;
                let frac_micros = (micros % 1_000_000) as u32;
                let time =
                    chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, frac_micros * 1000)
                        .unwrap_or(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                let iso = time.format("%H:%M:%S%.6f").to_string();
                match fluree_db_core::temporal::Time::parse(&iso) {
                    Ok(t) => Ok(FlakeValue::Time(Box::new(t))),
                    Err(_) => Ok(FlakeValue::String(iso)),
                }
            }
            DecodeKind::DateTime => {
                let epoch_micros = key.decode_datetime();
                let dt = chrono::DateTime::from_timestamp_micros(epoch_micros).unwrap_or_default();
                let iso = dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();
                match fluree_db_core::temporal::DateTime::parse(&iso) {
                    Ok(d) => Ok(FlakeValue::DateTime(Box::new(d))),
                    Err(_) => Ok(FlakeValue::String(iso)),
                }
            }
            DecodeKind::GYear => Ok(FlakeValue::GYear(Box::new(
                fluree_db_core::temporal::GYear::from_year(key.decode_g_year()),
            ))),
            DecodeKind::GYearMonth => {
                let (year, month) = key.decode_g_year_month();
                Ok(FlakeValue::GYearMonth(Box::new(
                    fluree_db_core::temporal::GYearMonth::from_components(year, month),
                )))
            }
            DecodeKind::GMonth => Ok(FlakeValue::GMonth(Box::new(
                fluree_db_core::temporal::GMonth::from_month(key.decode_g_month()),
            ))),
            DecodeKind::GDay => Ok(FlakeValue::GDay(Box::new(
                fluree_db_core::temporal::GDay::from_day(key.decode_g_day()),
            ))),
            DecodeKind::GMonthDay => {
                let (month, day) = key.decode_g_month_day();
                Ok(FlakeValue::GMonthDay(Box::new(
                    fluree_db_core::temporal::GMonthDay::from_components(month, day),
                )))
            }
            DecodeKind::YearMonthDuration => Ok(FlakeValue::YearMonthDuration(Box::new(
                fluree_db_core::temporal::YearMonthDuration::from_months(
                    key.decode_year_month_dur(),
                ),
            ))),
            DecodeKind::DayTimeDuration => Ok(FlakeValue::DayTimeDuration(Box::new(
                fluree_db_core::temporal::DayTimeDuration::from_micros(key.decode_day_time_dur()),
            ))),
            DecodeKind::Duration => {
                // Compound duration — not yet fully supported in V5 either.
                Ok(FlakeValue::Null)
            }
            DecodeKind::GeoPoint => Ok(FlakeValue::GeoPoint(fluree_db_core::GeoPointBits(o_key))),
            DecodeKind::BlankNode => {
                // Blank node: o_key is an opaque bnode integer, not a subject dict ID.
                // Synthesize a blank node IRI `_:b{o_key}` and encode as a Sid.
                let bnode_iri = format!("_:b{}", o_key);
                Ok(FlakeValue::Ref(Sid::new(0, &bnode_iri)))
            }
            DecodeKind::IriRef => {
                let iri = self.resolve_subject_iri(o_key)?;
                Ok(FlakeValue::Ref(self.encode_iri(&iri)))
            }
            DecodeKind::StringDict => {
                let s = self.resolve_string_value(o_key as u32)?;
                Ok(FlakeValue::String(s))
            }
            DecodeKind::JsonArena => {
                // Despite the "Arena" name in DecodeKind, JSON values are currently
                // stored in the string dictionary (same as V5 ObjKind::JSON_ID).
                // A dedicated JSON arena may be introduced later.
                let json_str = self.resolve_string_value(o_key as u32)?;
                Ok(FlakeValue::Json(json_str))
            }
            DecodeKind::NumBigArena => {
                let handle = o_key as u32;
                let arena = self
                    .graph_indexes
                    .get(&g_id)
                    .and_then(|gi| gi.numbig.get(&p_id))
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("no NumBig arena for g_id={g_id}, p_id={p_id}"),
                        )
                    })?;
                let stored = arena.get_by_handle(handle).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("NumBig handle {handle} not found for g_id={g_id}, p_id={p_id}"),
                    )
                })?;
                Ok(stored.to_flake_value())
            }
            DecodeKind::VectorArena => {
                let handle = o_key as u32;
                let arena = self
                    .graph_indexes
                    .get(&g_id)
                    .and_then(|gi| gi.vectors.get(&p_id))
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("no vector arena for g_id={g_id}, p_id={p_id}"),
                        )
                    })?;
                let vs = arena.lookup_vector(handle)?.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("vector handle {handle} not found for g_id={g_id}, p_id={p_id}"),
                    )
                })?;
                let f64_vec: Vec<f64> = vs.as_f32().iter().map(|&x| x as f64).collect();
                Ok(FlakeValue::Vector(f64_vec))
            }
            DecodeKind::SpatialArena => Err(io::Error::other(
                "spatial arena decode not yet implemented in V6",
            )),
        }
    }

    /// Resolve a subject ID (u64) to its full IRI string.
    pub fn resolve_subject_iri(&self, s_id: u64) -> io::Result<String> {
        let sid = fluree_db_core::subject_id::SubjectId::from_u64(s_id);
        let ns_code = sid.ns_code();
        let local_id = sid.local_id();

        let reader = self
            .dicts
            .subject_forward_packs
            .get(&ns_code)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no subject forward pack for ns_code={ns_code}"),
                )
            })?;

        let suffix = reader.forward_lookup_str(local_id)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("subject local_id {} not found in ns {}", local_id, ns_code),
            )
        })?;
        let prefix = self.dicts.namespace_codes.get(&ns_code).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("no namespace prefix for code={ns_code}"),
            )
        })?;

        Ok(format!("{}{}", prefix, suffix))
    }

    /// Resolve a string dictionary ID to its value.
    pub fn resolve_string_value(&self, str_id: u32) -> io::Result<String> {
        self.dicts
            .string_forward_packs
            .forward_lookup_str(str_id as u64)?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("string id {} not found in forward packs", str_id),
                )
            })
    }

    /// Resolve a predicate ID to its IRI.
    pub fn resolve_predicate_iri(&self, p_id: u32) -> Option<&str> {
        self.dicts.predicates.resolve(p_id)
    }

    /// Lookup a predicate IRI → p_id.
    pub fn find_predicate_id(&self, iri: &str) -> Option<u32> {
        self.dicts.predicate_reverse.get(iri).copied()
    }

    /// Encode an IRI to a namespaced `Sid`.
    pub fn encode_iri(&self, iri: &str) -> Sid {
        match self.dicts.prefix_trie.longest_match(iri) {
            Some((code, prefix_len)) => Sid::new(code, &iri[prefix_len..]),
            None => Sid::new(0, iri),
        }
    }

    /// Resolve language ID to BCP 47 tag string.
    ///
    /// `lang_id` in the OType payload is 1-based (lang_id=1 is the first tag).
    /// `language_tags` is a 0-based Vec, so we subtract 1.
    pub fn resolve_lang_tag(&self, o_type: u16) -> Option<&str> {
        let ot = OType::from_u16(o_type);
        if ot.is_lang_string() {
            let lang_id = ot.lang_id()? as usize;
            if lang_id == 0 {
                return None; // lang_id=0 means "no tag"
            }
            self.language_tags.get(lang_id - 1).map(|s| s.as_str())
        } else {
            None
        }
    }

    /// Resolve o_type to a datatype Sid (for materializing datatype IRIs in output).
    /// O(1) via the pre-built o_type index.
    pub fn resolve_datatype_sid(&self, o_type: u16) -> Option<Sid> {
        let idx = self.o_type_index.get(&o_type)?;
        let entry = &self.o_type_table[*idx];
        let iri = entry.datatype_iri.as_deref()?;
        Some(self.encode_iri(iri))
    }

    /// Look up an o_type table entry by o_type value. O(1).
    pub fn lookup_o_type(&self, o_type: u16) -> Option<&OTypeTableEntry> {
        self.o_type_index
            .get(&o_type)
            .map(|&idx| &self.o_type_table[idx])
    }

    /// Reconstruct the full IRI string from a `Sid`.
    pub fn sid_to_iri(&self, sid: &Sid) -> String {
        let prefix = self
            .dicts
            .namespace_codes
            .get(&sid.namespace_code)
            .map(|s| s.as_str())
            .unwrap_or("");
        format!("{}{}", prefix, sid.name)
    }

    /// Reverse subject lookup: find the u64 s_id for a given IRI.
    pub fn find_subject_id(&self, iri: &str) -> io::Result<Option<u64>> {
        match &self.dicts.subject_reverse_tree {
            Some(tree) => {
                let (ns_code, prefix_len) =
                    self.dicts.prefix_trie.longest_match(iri).unwrap_or((0, 0));
                let suffix = &iri[prefix_len..];
                let key =
                    crate::dict::reverse_leaf::subject_reverse_key(ns_code, suffix.as_bytes());
                tree.reverse_lookup(&key)
            }
            None => Ok(None),
        }
    }

    /// Translate a `Sid` to `s_id` via the reverse subject dictionary.
    pub fn sid_to_s_id(&self, sid: &Sid) -> io::Result<Option<u64>> {
        let iri = self.sid_to_iri(sid);
        self.find_subject_id(&iri)
    }

    /// Translate a `Sid` to `p_id` via the predicate reverse map.
    pub fn sid_to_p_id(&self, sid: &Sid) -> Option<u32> {
        let iri = self.sid_to_iri(sid);
        self.find_predicate_id(&iri)
    }

    /// Access the datatype SIDs vector.
    pub fn dt_sids(&self) -> &[Sid] {
        &self.dicts.dt_sids
    }

    /// Reverse subject lookup by namespace parts (avoids IRI construction).
    pub fn find_subject_id_by_parts(&self, ns_code: u16, suffix: &str) -> io::Result<Option<u64>> {
        match &self.dicts.subject_reverse_tree {
            Some(tree) => {
                let key =
                    crate::dict::reverse_leaf::subject_reverse_key(ns_code, suffix.as_bytes());
                tree.reverse_lookup(&key)
            }
            None => Ok(None),
        }
    }

    /// Find all subject IDs whose suffix starts with `prefix` within a namespace.
    ///
    /// Uses a range scan on the reverse subject tree: scans the key range
    /// `[ns_code || prefix, ns_code || prefix~)` where `~` (0x7E) sorts after
    /// all printable ASCII. Returns the matching `(suffix_bytes, s_id)` pairs.
    pub fn find_subjects_by_prefix(&self, ns_code: u16, prefix: &str) -> io::Result<Vec<u64>> {
        match &self.dicts.subject_reverse_tree {
            Some(tree) => {
                let start_key =
                    crate::dict::reverse_leaf::subject_reverse_key(ns_code, prefix.as_bytes());
                // End key: prefix followed by 0xFF byte (sorts after all valid UTF-8).
                let mut end_suffix = prefix.as_bytes().to_vec();
                end_suffix.push(0xFF);
                let end_key = crate::dict::reverse_leaf::subject_reverse_key(ns_code, &end_suffix);

                let entries = tree.reverse_range_scan(&start_key, &end_key)?;
                Ok(entries.into_iter().map(|(_, id)| id).collect())
            }
            None => Ok(Vec::new()),
        }
    }

    /// Reverse string lookup: value → string_id.
    pub fn find_string_id(&self, value: &str) -> io::Result<Option<u32>> {
        match &self.dicts.string_reverse_tree {
            Some(tree) => tree
                .reverse_lookup(value.as_bytes())
                .map(|opt| opt.map(|id| id as u32)),
            None => Ok(None),
        }
    }

    /// Number of predicates in the persisted dictionary.
    pub fn predicate_count(&self) -> u32 {
        self.dicts.predicates.len()
    }

    /// Number of strings in the persisted forward dictionary.
    pub fn string_count(&self) -> u32 {
        self.dicts.string_count
    }

    /// Number of language tags in the persisted dictionary.
    pub fn language_tag_count(&self) -> u16 {
        self.dicts.language_tags.len()
    }

    /// Look up a predicate Sid by p_id, returning the full IRI as a Sid.
    pub fn predicate_sid(&self, p_id: u32) -> Option<Sid> {
        let iri = self.dicts.predicates.resolve(p_id)?;
        Some(self.encode_iri(iri))
    }

    /// Look up a datatype ID by its Sid.
    pub fn find_dt_id(&self, dt_sid: &Sid) -> u16 {
        self.dicts
            .dt_sids
            .iter()
            .position(|s| s == dt_sid)
            .map(|i| i as u16)
            .unwrap_or(0)
    }

    /// Find the 1-based lang_id for a language tag string. Returns None if not found.
    pub fn resolve_lang_id(&self, tag: &str) -> Option<u16> {
        self.dicts.language_tags.find(tag)
    }

    /// Augment namespace codes with entries from novelty commits.
    pub fn augment_namespace_codes(&mut self, codes: &std::collections::HashMap<u16, String>) {
        for (&code, prefix) in codes {
            self.dicts
                .namespace_codes
                .entry(code)
                .or_insert_with(|| prefix.clone());
            self.dicts
                .namespace_reverse
                .entry(prefix.clone())
                .or_insert(code);
        }
        // Rebuild prefix trie to include new entries.
        self.dicts.prefix_trie = PrefixTrie::from_namespace_codes(&self.dicts.namespace_codes);
    }

    /// Access the namespace codes table.
    pub fn namespace_codes(&self) -> &HashMap<u16, String> {
        &self.dicts.namespace_codes
    }

    /// Access the o_type table.
    pub fn o_type_table(&self) -> &[OTypeTableEntry] {
        &self.o_type_table
    }

    /// Spatial provider map for query context configuration.
    pub fn spatial_provider_map(
        &self,
    ) -> HashMap<String, Arc<dyn fluree_db_spatial::SpatialIndexProvider>> {
        let mut map = HashMap::new();
        for (g_id, gi) in &self.graph_indexes {
            for (p_id, provider) in &gi.spatial {
                let key = format!("{}:{}", g_id, p_id);
                map.insert(key, Arc::clone(provider));
            }
        }
        map
    }

    /// Fulltext provider map for query context configuration.
    pub fn fulltext_provider_map(
        &self,
    ) -> HashMap<String, Arc<crate::arena::fulltext::FulltextArena>> {
        let mut map = HashMap::new();
        for (g_id, gi) in &self.graph_indexes {
            for (p_id, arena) in &gi.fulltext {
                let key = format!("{}:{}", g_id, p_id);
                map.insert(key, Arc::clone(arena));
            }
        }
        map
    }
}

// ============================================================================
// BinaryGraphViewV3 — graph-scoped wrapper
// ============================================================================

/// Graph-scoped view for V6 store. Binds a specific `g_id` for value decoding.
pub struct BinaryGraphViewV3 {
    store: Arc<BinaryIndexStoreV6>,
    g_id: GraphId,
}

impl BinaryGraphViewV3 {
    pub fn new(store: Arc<BinaryIndexStoreV6>, g_id: GraphId) -> Self {
        Self { store, g_id }
    }

    pub fn decode_value(&self, o_type: u16, o_key: u64, p_id: u32) -> io::Result<FlakeValue> {
        self.store.decode_value_v3(o_type, o_key, p_id, self.g_id)
    }

    pub fn store(&self) -> &BinaryIndexStoreV6 {
        &self.store
    }

    pub fn clone_store(&self) -> Arc<BinaryIndexStoreV6> {
        Arc::clone(&self.store)
    }

    pub fn g_id(&self) -> GraphId {
        self.g_id
    }
}

// ============================================================================
// Dict loading (reuses V5 infrastructure)
// ============================================================================

/// Build the dictionary set from an IndexRootV6.
///
/// This mirrors the dict-loading portion of `BinaryIndexStore::load_from_root_v5`
/// but takes fields from `IndexRootV6`. The dict infrastructure is identical
/// (same `DictRefsV5`, same pack/tree formats).
async fn build_dictionary_set(
    cs: Arc<dyn ContentStore>,
    root: &IndexRootV6,
    cache_dir: &Path,
    leaflet_cache: Option<&Arc<LeafletCache>>,
) -> io::Result<DictionarySet> {
    // Predicates (inline in root).
    let (predicates, predicate_reverse) = {
        let mut dict = PredicateDict::new();
        let mut rev = HashMap::with_capacity(root.predicate_sids.len());
        for (p_id, (ns_code, suffix)) in root.predicate_sids.iter().enumerate() {
            let prefix = root.namespace_codes.get(ns_code).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("predicate[{p_id}]: unknown ns_code {ns_code}"),
                )
            })?;
            let iri = format!("{prefix}{suffix}");
            dict.get_or_insert(&iri);
            rev.insert(iri, p_id as u32);
        }
        (dict, rev)
    };

    // Subject forward packs.
    let mut subject_forward_packs = std::collections::BTreeMap::new();
    for (ns_code, ns_refs) in &root.dict_refs.forward_packs.subject_fwd_ns_packs {
        let reader = ForwardPackReader::from_pack_refs(
            Arc::clone(&cs),
            cache_dir,
            ns_refs,
            KIND_SUBJECT_FWD,
            *ns_code,
        )
        .await?;
        subject_forward_packs.insert(*ns_code, reader);
    }

    // Subject reverse tree.
    let subject_reverse_tree = Some(
        load_dict_tree_from_cas(
            Arc::clone(&cs),
            &root.dict_refs.subject_reverse,
            cache_dir,
            "srl",
            leaflet_cache,
        )
        .await?,
    );

    // String forward packs.
    let string_forward_packs = ForwardPackReader::from_pack_refs(
        Arc::clone(&cs),
        cache_dir,
        &root.dict_refs.forward_packs.string_fwd_packs,
        KIND_STRING_FWD,
        0,
    )
    .await?;

    // String reverse tree.
    let string_reverse_tree = Some(
        load_dict_tree_from_cas(
            Arc::clone(&cs),
            &root.dict_refs.string_reverse,
            cache_dir,
            "trl",
            leaflet_cache,
        )
        .await?,
    );

    // Namespace codes.
    let namespace_codes: HashMap<u16, String> = root
        .namespace_codes
        .iter()
        .map(|(&k, v)| (k, v.clone()))
        .collect();
    let namespace_reverse: HashMap<String, u16> = namespace_codes
        .iter()
        .map(|(&code, prefix)| (prefix.clone(), code))
        .collect();
    let prefix_trie = PrefixTrie::from_namespace_codes(&namespace_codes);

    // Language tags.
    let mut language_tags = LanguageTagDict::new();
    for tag in &root.language_tags {
        language_tags.get_or_insert(Some(tag));
    }

    // Datatype SIDs.
    if root.datatype_iris.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "index root missing datatype_iris",
        ));
    }
    let dt_sids: Vec<Sid> = root
        .datatype_iris
        .iter()
        .map(|iri| match prefix_trie.longest_match(iri) {
            Some((code, prefix_len)) => Sid::new(code, &iri[prefix_len..]),
            None => Sid::new(0, iri),
        })
        .collect();

    // Graph IRIs → dict_index (0-based). g_id = dict_index + 1 at lookup time.
    // Matches V5 convention: graphs_reverse stores 0-based dict index.
    let graphs_reverse: HashMap<String, GraphId> = root
        .graph_iris
        .iter()
        .enumerate()
        .map(|(id, iri)| (iri.to_string(), id as GraphId))
        .collect();

    // Subject count from watermarks.
    let subject_count = root.subject_watermarks.iter().sum::<u64>() as u32;

    Ok(DictionarySet {
        predicates,
        predicate_reverse,
        graphs_reverse,
        subject_forward_packs,
        subject_reverse_tree,
        string_forward_packs,
        string_reverse_tree,
        subject_count,
        string_count: root.string_watermark,
        namespace_codes,
        namespace_reverse,
        prefix_trie,
        language_tags,
        dt_sids,
    })
}

// ============================================================================
// Arena loading (reuses V5 infrastructure)
// ============================================================================

/// Per-graph arenas (before injection into GraphIndexV3).
struct LoadedArenas {
    numbig: HashMap<u32, crate::arena::numbig::NumBigArena>,
    vectors: HashMap<u32, crate::arena::vector::LazyVectorArena>,
    spatial: HashMap<u32, Arc<dyn fluree_db_spatial::SpatialIndexProvider>>,
    fulltext: HashMap<u32, Arc<crate::arena::fulltext::FulltextArena>>,
}

/// Load per-graph specialty arenas from GraphArenaRefsV5.
async fn load_per_graph_arenas(
    cs: Arc<dyn ContentStore>,
    graph_arenas: &[crate::format::index_root::GraphArenaRefsV5],
    cache_dir: &Path,
    leaflet_cache: Option<&Arc<LeafletCache>>,
) -> io::Result<HashMap<GraphId, LoadedArenas>> {
    let mut result = HashMap::new();

    for ga in graph_arenas {
        let mut numbig = HashMap::new();
        for (p_id, cid) in &ga.numbig {
            let bytes = fetch_cached_bytes(cs.as_ref(), cid, cache_dir, "nba").await?;
            let arena = crate::arena::numbig::read_numbig_arena_from_bytes(&bytes)?;
            numbig.insert(*p_id, arena);
        }

        let mut vectors = HashMap::new();
        for entry in &ga.vectors {
            let manifest_bytes =
                fetch_cached_bytes(cs.as_ref(), &entry.manifest, cache_dir, "vam").await?;
            let manifest = crate::arena::vector::read_vector_manifest(&manifest_bytes)?;

            let mut shard_sources = Vec::with_capacity(entry.shards.len());
            for shard_cid in &entry.shards {
                let cid_hash = LeafletCache::cid_cache_key(&shard_cid.to_bytes());
                if let Some(local) = cs.resolve_local_path(shard_cid) {
                    shard_sources.push(crate::arena::vector::ShardSource {
                        cid_hash,
                        cid: None,
                        path: local,
                        on_disk: std::sync::atomic::AtomicBool::new(true),
                    });
                } else {
                    let cache_path = cache_dir.join(format!("{}.vas", shard_cid));
                    let exists = cache_path.exists();
                    shard_sources.push(crate::arena::vector::ShardSource {
                        cid_hash,
                        cid: Some(shard_cid.clone()),
                        path: cache_path,
                        on_disk: std::sync::atomic::AtomicBool::new(exists),
                    });
                }
            }

            // LazyVectorArena needs a LeafletCache for shard caching and
            // an optional ContentStore for remote shard fetching.
            let shard_cache = leaflet_cache
                .cloned()
                .unwrap_or_else(|| Arc::new(LeafletCache::with_max_mb(64)));
            let arena = crate::arena::vector::LazyVectorArena::new(
                manifest,
                shard_sources,
                shard_cache,
                Some(Arc::clone(&cs)),
            );
            vectors.insert(entry.p_id, arena);
        }

        // Spatial and fulltext: load same as V5.
        let mut spatial = HashMap::new();
        for sp_ref in &ga.spatial {
            let root_bytes =
                fetch_cached_bytes(cs.as_ref(), &sp_ref.root_cid, cache_dir, "spr").await?;
            let spatial_root: fluree_db_spatial::SpatialIndexRoot =
                serde_json::from_slice(&root_bytes).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("spatial root: {e}"))
                })?;

            // Pre-fetch all spatial blobs, keyed by digest_hex.
            let mut blob_cache: HashMap<String, Vec<u8>> = HashMap::new();
            for cid in [&sp_ref.manifest, &sp_ref.arena]
                .into_iter()
                .chain(sp_ref.leaflets.iter())
            {
                let bytes = fetch_cached_bytes(cs.as_ref(), cid, cache_dir, "spa").await?;
                blob_cache.insert(cid.digest_hex(), bytes);
            }
            let blob_cache = Arc::new(blob_cache);

            let snapshot =
                fluree_db_spatial::SpatialIndexSnapshot::load_from_cas(spatial_root, move |hash| {
                    blob_cache.get(hash).cloned().ok_or_else(|| {
                        fluree_db_spatial::SpatialError::ChunkNotFound(hash.to_string())
                    })
                })
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("spatial snapshot load: {e}"),
                    )
                })?;
            let provider: Arc<dyn fluree_db_spatial::SpatialIndexProvider> =
                Arc::new(fluree_db_spatial::EmbeddedSpatialProvider::new(snapshot));
            spatial.insert(sp_ref.p_id, provider);
        }

        let mut fulltext = HashMap::new();
        for ft_ref in &ga.fulltext {
            let bytes =
                fetch_cached_bytes(cs.as_ref(), &ft_ref.arena_cid, cache_dir, "fta").await?;
            let arena = crate::arena::fulltext::FulltextArena::decode(&bytes)?;
            fulltext.insert(ft_ref.p_id, Arc::new(arena));
        }

        result.insert(
            ga.g_id,
            LoadedArenas {
                numbig,
                vectors,
                spatial,
                fulltext,
            },
        );
    }

    Ok(result)
}
