//! Ledger information API
//!
//! This module provides graph-scoped ledger metadata via `build_ledger_info`.
//!
//! ## Response Shape
//!
//! ```json
//! {
//!   "ledger": { "alias", "t", "commit-t", "index-t", "flakes", "size", "named-graphs" },
//!   "graph": "urn:default",
//!   "stats": { "flakes", "size", "properties": { ... }, "classes": { ... } },
//!   "commit": { ... },
//!   "nameservice": { ... },
//!   "index": { ... }
//! }
//! ```
//!
//! The `stats` block is always scoped to a single graph (default: g_id=0).
//! Use the builder API to select a different graph via name, IRI, or g_id.

use crate::format::iri::IriCompactor;
use fluree_db_core::address_path::ledger_id_to_path_prefix;
use fluree_db_core::comparator::IndexType;
use fluree_db_core::ids::GraphId;
use fluree_db_core::ledger_id::{format_ledger_id, split_ledger_id};
use fluree_db_core::value_id::ValueTypeTag;
use fluree_db_core::{
    is_rdf_type, ClassPropertyUsage, ClassRefCount, Flake, FlakeValue, LedgerSnapshot,
    OverlayProvider, Sid, Storage,
};
use fluree_db_core::{
    ClassStatEntry, GraphPropertyStatEntry, GraphStatsEntry, IndexSchema, IndexStats,
    SchemaPredicateInfo,
};
use fluree_db_indexer::run_index::BinaryIndexStore;
use fluree_db_ledger::LedgerState;
use fluree_db_nameservice::{GraphSourceRecord, NsRecord};
use fluree_db_novelty::{load_commit_by_id, Novelty};
use fluree_graph_json_ld::ParsedContext;
use serde_json::{json, Map, Value as JsonValue};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Which graph to scope the stats section to.
#[derive(Debug, Clone, Default)]
pub enum GraphSelector {
    /// Default graph (g_id = 0).
    #[default]
    Default,
    /// Select by numeric graph ID.
    ById(GraphId),
    /// Select by graph IRI (resolved via the binary index store).
    ByIri(String),
    /// Select by well-known name ("default" or "txn-meta").
    ByName(String),
}

/// Options controlling `ledger-info` stats detail and freshness.
///
/// Defaults preserve the fast, small "base" payload; callers can opt into
/// heavier/real-time details when needed.
#[derive(Debug, Clone, Default)]
pub struct LedgerInfoOptions {
    /// When true, augment property "details" with novelty deltas so the result
    /// is real-time (novelty-aware) rather than "as of last index".
    pub realtime_property_details: bool,

    /// When true, include `datatypes` under `stats.properties[*]`.
    ///
    /// By default the API omits datatype breakdowns at the top-level property
    /// map to keep payloads small.
    pub include_property_datatypes: bool,

    /// Which graph to scope the stats section to.
    pub graph: GraphSelector,
}

/// Schema index for fast SID -> hierarchy lookup
type SchemaIndex<'a> = HashMap<Sid, &'a SchemaPredicateInfo>;

/// Build a schema index for fast hierarchy lookups
fn build_schema_index(schema: &IndexSchema) -> SchemaIndex<'_> {
    schema
        .pred
        .vals
        .iter()
        .map(|info| (info.id.clone(), info))
        .collect()
}

/// Error type for ledger info operations
#[derive(Debug, thiserror::Error)]
pub enum LedgerInfoError {
    #[error("No commit ID available")]
    NoCommitId,

    #[error("Failed to load commit: {0}")]
    CommitLoad(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Unknown namespace code: {0}")]
    UnknownNamespace(u16),

    #[error("Class lookup failed: {0}")]
    ClassLookup(String),

    #[error("Unknown graph: {0}")]
    UnknownGraph(String),
}

/// Result type for ledger info operations
pub type Result<T> = std::result::Result<T, LedgerInfoError>;

/// Build comprehensive ledger metadata with Clojure parity.
///
/// Returns JSON containing:
/// - `ledger`: ledger-wide metadata
/// - `graph`: name of the graph being reported
/// - `stats`: graph-scoped statistics with decoded IRIs
/// - `commit`: Commit info in JSON-LD format
/// - `nameservice`: NsRecord in JSON-LD format
/// - `index`: Index metadata (if available)
pub async fn build_ledger_info<S: Storage + Clone>(
    ledger: &LedgerState,
    storage: &S,
    context: Option<&JsonValue>,
) -> Result<JsonValue> {
    build_ledger_info_with_options(ledger, storage, context, LedgerInfoOptions::default()).await
}

/// Build comprehensive ledger metadata, with optional extra/real-time stats.
pub async fn build_ledger_info_with_options<S: Storage + Clone>(
    ledger: &LedgerState,
    storage: &S,
    context: Option<&JsonValue>,
    options: LedgerInfoOptions,
) -> Result<JsonValue> {
    // Build the IRI compactor for stats decoding
    let parsed_context = context
        .map(|c| ParsedContext::parse(None, c).unwrap_or_default())
        .unwrap_or_default();
    let compactor = IriCompactor::new(&ledger.db.namespace_codes, &parsed_context);

    // Build schema index for hierarchy lookups
    let schema_index = ledger
        .db
        .schema
        .as_ref()
        .map(build_schema_index)
        .unwrap_or_default();

    // Try to get the BinaryIndexStore for IRI resolution
    let binary_store: Option<Arc<BinaryIndexStore>> = ledger
        .binary_store
        .as_ref()
        .and_then(|te| Arc::clone(&te.0).downcast::<BinaryIndexStore>().ok());

    // Resolve graph selector to g_id
    let g_id = resolve_graph_selector(&options.graph, binary_store.as_deref())?;

    // Determine graph display name
    let graph_name = graph_display_name(g_id, binary_store.as_deref());

    // Get current stats (always returns IndexStats)
    let mut stats = ledger.current_stats();

    // Optional: real-time novelty merge into the graph entry for the requested g_id.
    if options.realtime_property_details {
        if let Some(graphs) = stats.graphs.as_mut() {
            if let Some(graph_entry) = graphs.iter_mut().find(|g| g.g_id == g_id) {
                // Resolve graph IRI for filtering novelty flakes to this graph.
                let graph_iri: Option<String> = binary_store
                    .as_deref()
                    .and_then(|s| s.graph_iri_for_id(g_id).map(|s| s.to_string()));

                if let Some(store) = binary_store.as_deref() {
                    merge_graph_property_novelty(
                        graph_entry,
                        &ledger.novelty,
                        store,
                        &ledger.db.namespace_codes,
                        graph_iri.as_deref(),
                        options.include_property_datatypes,
                    );
                }
                merge_graph_class_ref_edges_from_novelty(
                    &ledger.db,
                    ledger.novelty.as_ref(),
                    ledger.t(),
                    g_id,
                    graph_entry,
                    &ledger.db.namespace_codes,
                    graph_iri.as_deref(),
                )
                .await?;
            }
        }
    }

    // Pre-index fallback: if no graph stats from index, try loading the pre-index manifest
    if stats.graphs.is_none() {
        let alias_prefix = ledger_id_to_path_prefix(&ledger.db.ledger_id)
            .unwrap_or_else(|_| ledger.db.ledger_id.replace(':', "/"));
        let manifest_addr_primary =
            format!("fluree:file://{}/stats/pre-index-stats.json", alias_prefix);
        if let Ok(bytes) = storage.read_bytes(&manifest_addr_primary).await {
            match parse_pre_index_manifest(&bytes) {
                Ok(graphs) => {
                    tracing::debug!(graphs = graphs.len(), "loaded pre-index stats manifest");
                    stats.graphs = Some(graphs);
                }
                Err(e) => {
                    tracing::warn!("failed to parse pre-index stats manifest: {}", e);
                }
            }
        }
    }

    // Build the response
    let mut result = Map::new();

    // 1. Ledger block (ledger-wide metadata)
    result.insert(
        "ledger".to_string(),
        build_ledger_block(ledger, &stats, binary_store.as_deref()),
    );

    // 2. Graph name
    result.insert("graph".to_string(), json!(graph_name));

    // 3. Graph-scoped stats section
    result.insert(
        "stats".to_string(),
        build_graph_scoped_stats(
            g_id,
            &stats,
            &compactor,
            &schema_index,
            binary_store.as_deref(),
            options.include_property_datatypes,
        )?,
    );

    // 4. Commit section (ALWAYS include, even if None - for Clojure parity)
    if let Some(head_cid) = &ledger.head_commit_id {
        match build_commit_jsonld(storage, head_cid, &ledger.db.ledger_id).await {
            Ok(commit_json) => {
                result.insert("commit".to_string(), commit_json);
            }
            Err(e) => {
                result.insert("commit".to_string(), json!({ "error": format!("{}", e) }));
            }
        }
    } else {
        result.insert("commit".to_string(), JsonValue::Null);
    }

    // Include content identifiers when available
    if let Some(ref cid) = ledger.head_commit_id {
        result.insert("commitId".to_string(), json!(cid.to_string()));
    }
    if let Some(ref cid) = ledger.head_index_id {
        result.insert("indexId".to_string(), json!(cid.to_string()));
    }

    // 5. Nameservice section
    if let Some(ns_record) = &ledger.ns_record {
        result.insert("nameservice".to_string(), ns_record_to_jsonld(ns_record));
    }

    // 6. Index section (if available)
    if let Some(ns_record) = &ledger.ns_record {
        if ns_record.index_head_id.is_some() || ns_record.index_t > 0 {
            let mut index_obj = json!({
                "t": ns_record.index_t,
            });
            if let Some(ref cid) = ledger.head_index_id {
                index_obj["id"] = json!(cid.to_string());
            } else if let Some(ref cid) = ns_record.index_head_id {
                index_obj["id"] = json!(cid.to_string());
            }
            result.insert("index".to_string(), index_obj);
        }
    }

    Ok(JsonValue::Object(result))
}

// ============================================================================
// Graph selector resolution
// ============================================================================

/// Resolve a `GraphSelector` to a numeric `g_id`.
fn resolve_graph_selector(
    selector: &GraphSelector,
    store: Option<&BinaryIndexStore>,
) -> Result<GraphId> {
    match selector {
        GraphSelector::Default => Ok(0),
        GraphSelector::ById(g_id) => Ok(*g_id),
        GraphSelector::ByName(name) => match name.as_str() {
            "default" | "urn:default" => Ok(0),
            "txn-meta" => Ok(1),
            other => {
                // Try as IRI
                if let Some(store) = store {
                    store
                        .graph_id_for_iri(other)
                        .ok_or_else(|| LedgerInfoError::UnknownGraph(other.to_string()))
                } else {
                    Err(LedgerInfoError::UnknownGraph(format!(
                        "no binary index store available to resolve graph name '{}'",
                        other
                    )))
                }
            }
        },
        GraphSelector::ByIri(iri) => {
            if iri == "urn:default" {
                return Ok(0);
            }
            if let Some(store) = store {
                store
                    .graph_id_for_iri(iri)
                    .ok_or_else(|| LedgerInfoError::UnknownGraph(iri.clone()))
            } else {
                Err(LedgerInfoError::UnknownGraph(format!(
                    "no binary index store available to resolve graph IRI '{}'",
                    iri
                )))
            }
        }
    }
}

/// Determine the display name for a graph ID.
fn graph_display_name(g_id: GraphId, store: Option<&BinaryIndexStore>) -> String {
    if g_id == 0 {
        return "urn:default".to_string();
    }
    if let Some(store) = store {
        if let Some(iri) = store.graph_iri_for_id(g_id) {
            return iri.to_string();
        }
    }
    format!("g:{}", g_id)
}

// ============================================================================
// Response builders
// ============================================================================

/// Build the `ledger` block with ledger-wide metadata.
fn build_ledger_block(
    ledger: &LedgerState,
    stats: &IndexStats,
    store: Option<&BinaryIndexStore>,
) -> JsonValue {
    let index_t = ledger
        .ns_record
        .as_ref()
        .map(|r| r.index_t)
        .unwrap_or(ledger.db.t);

    let commit_t = ledger
        .ns_record
        .as_ref()
        .map(|r| r.commit_t)
        .unwrap_or(ledger.t());

    // Build named-graphs list
    let mut named_graphs = Vec::new();
    // Always include default graph
    named_graphs.push(json!({"iri": "urn:default", "g-id": 0}));
    // Add named graphs from binary store
    if let Some(store) = store {
        for (g_id, iri) in store.graph_entries() {
            // Skip txn-meta (g_id=1) from the public list
            if g_id == 1 {
                continue;
            }
            named_graphs.push(json!({"iri": iri, "g-id": g_id}));
        }
    }

    json!({
        "alias": &ledger.db.ledger_id,
        "t": ledger.t(),
        "commit-t": commit_t,
        "index-t": index_t,
        "flakes": stats.flakes,
        "size": stats.size,
        "named-graphs": named_graphs,
    })
}

/// Build the graph-scoped `stats` section.
///
/// Extracts the `GraphStatsEntry` for the requested `g_id` and renders
/// its properties and classes with IRI compaction.
///
/// All graphs (including default g_id=0) use their `GraphStatsEntry` for
/// graph-scoped properties and classes.
fn build_graph_scoped_stats(
    g_id: GraphId,
    stats: &IndexStats,
    compactor: &IriCompactor,
    schema_index: &SchemaIndex,
    store: Option<&BinaryIndexStore>,
    include_property_datatypes: bool,
) -> Result<JsonValue> {
    // Find the GraphStatsEntry for the requested g_id (works for all graphs including default).
    let graph_entry = stats
        .graphs
        .as_ref()
        .and_then(|gs| gs.iter().find(|g| g.g_id == g_id));

    let (graph_flakes, graph_size) = graph_entry.map(|g| (g.flakes, g.size)).unwrap_or((0, 0));

    // Properties: always from graph-scoped GraphStatsEntry.
    let properties = if let (Some(entry), Some(store)) = (graph_entry, store) {
        decode_graph_property_stats(
            &entry.properties,
            compactor,
            schema_index,
            store,
            include_property_datatypes,
        )?
    } else {
        JsonValue::Object(Map::new())
    };

    // Classes: always from graph-scoped GraphStatsEntry.
    let classes = if let Some(entry) = graph_entry {
        decode_class_stats(&entry.classes, compactor, schema_index)?
    } else {
        JsonValue::Object(Map::new())
    };

    Ok(json!({
        "flakes": graph_flakes,
        "size": graph_size,
        "properties": properties,
        "classes": classes,
    }))
}

// ============================================================================
// Novelty merge helpers
// ============================================================================

/// Check if a novelty flake belongs to the specified graph.
///
/// - `graph_iri == None` → default graph: matches flakes with `g: None`
/// - `graph_iri == Some(iri)` → named graph: matches flakes whose graph Sid
///   resolves to the given IRI via namespace_codes
fn flake_in_graph(
    flake: &Flake,
    graph_iri: Option<&str>,
    namespace_codes: &HashMap<u16, String>,
) -> bool {
    match (graph_iri, &flake.g) {
        // Default graph: flakes with no graph annotation
        (None, None) => true,
        (None, Some(_)) => false,
        // Named graph: flakes must match the graph IRI
        (Some(_), None) => false,
        (Some(expected), Some(g_sid)) => {
            let Some(ns_prefix) = namespace_codes.get(&g_sid.namespace_code) else {
                return false;
            };
            let flake_graph_iri = format!("{}{}", ns_prefix, g_sid.name);
            flake_graph_iri == expected
        }
    }
}

/// Merge novelty deltas into a graph entry's property stats.
///
/// Only considers novelty flakes belonging to the specified graph:
/// - `graph_iri == None` → default graph (flakes with `g: None`)
/// - `graph_iri == Some(iri)` → named graph (flakes whose graph Sid resolves to `iri`)
///
/// Maps novelty predicate SIDs to p_ids via the binary index store, then
/// applies property count deltas and per-datatype count deltas to the
/// matching `GraphPropertyStatEntry`. Also updates the graph entry's
/// flake count and size.
///
/// When `merge_datatypes` is true, datatype breakdowns are also adjusted.
fn merge_graph_property_novelty(
    graph_entry: &mut GraphStatsEntry,
    novelty: &Novelty,
    store: &BinaryIndexStore,
    namespace_codes: &HashMap<u16, String>,
    graph_iri: Option<&str>,
    merge_datatypes: bool,
) {
    if novelty.is_empty() || graph_entry.properties.is_empty() {
        return;
    }

    // Property p_id -> (count_delta, datatype_tag -> dt_delta)
    let mut deltas: HashMap<u32, (i64, HashMap<u8, i64>)> = HashMap::new();
    let mut flakes_delta: i64 = 0;
    let mut size_delta: u64 = 0;
    for flake_id in novelty.iter_index(IndexType::Post) {
        let flake = novelty.get_flake(flake_id);

        // Skip commit-metadata namespace flakes (not user data).
        if flake.s.namespace_code == fluree_vocab::namespaces::FLUREE_COMMIT {
            continue;
        }

        // Graph filter: only include flakes belonging to the requested graph.
        if !flake_in_graph(flake, graph_iri, namespace_codes) {
            continue;
        }

        let delta = if flake.op { 1i64 } else { -1i64 };
        flakes_delta += delta;
        size_delta += flake.size_estimate_bytes();

        // Map predicate SID to p_id via namespace prefix + store lookup.
        let Some(ns_prefix) = namespace_codes.get(&flake.p.namespace_code) else {
            continue;
        };
        let full_iri = format!("{}{}", ns_prefix, flake.p.name);
        let Some(p_id) = store.find_predicate_id(&full_iri) else {
            continue;
        };

        let entry = deltas.entry(p_id).or_insert_with(|| (0, HashMap::new()));
        entry.0 += delta;

        if merge_datatypes {
            let tag = ValueTypeTag::from_ns_name(flake.dt.namespace_code, &flake.dt.name);
            if tag != ValueTypeTag::UNKNOWN {
                *entry.1.entry(tag.as_u8()).or_insert(0) += delta;
            }
        }
    }

    // Update graph-level flake count and size (graph-scoped, not total novelty).
    graph_entry.flakes = (graph_entry.flakes as i64 + flakes_delta).max(0) as u64;
    graph_entry.size += size_delta;

    if deltas.is_empty() {
        return;
    }

    // Index existing entries for in-place updates by p_id.
    let mut by_pid: HashMap<u32, usize> = HashMap::with_capacity(graph_entry.properties.len());
    for (idx, entry) in graph_entry.properties.iter().enumerate() {
        by_pid.insert(entry.p_id, idx);
    }

    for (p_id, (count_delta, dt_map)) in deltas {
        let Some(&idx) = by_pid.get(&p_id) else {
            continue;
        };
        let entry = &mut graph_entry.properties[idx];

        // Adjust property count.
        entry.count = (entry.count as i64 + count_delta).max(0) as u64;

        // Adjust datatype breakdown if requested.
        if merge_datatypes && !dt_map.is_empty() {
            let mut merged: HashMap<u8, i64> = entry
                .datatypes
                .iter()
                .map(|(tag, count)| (*tag, *count as i64))
                .collect();
            for (tag, delta) in dt_map {
                *merged.entry(tag).or_insert(0) += delta;
            }

            let mut out: Vec<(u8, u64)> = merged
                .into_iter()
                .filter_map(|(tag, count)| (count > 0).then_some((tag, count as u64)))
                .collect();
            out.sort_by(|a, b| a.0.cmp(&b.0));
            entry.datatypes = out;
        }
    }
}

/// Merge novelty ref-edge deltas into a graph entry's class stats.
///
/// Only considers novelty flakes belonging to the specified graph
/// (determined by `graph_iri`). Applies deltas from ref-valued novelty
/// flakes to the graph entry's `classes[*].properties[*].ref_classes`,
/// attributed using the *current* (novelty-aware) rdf:type of both the
/// subject and the referenced object.
///
async fn merge_graph_class_ref_edges_from_novelty(
    db: &LedgerSnapshot,
    novelty: &Novelty,
    to_t: i64,
    g_id: GraphId,
    graph_entry: &mut GraphStatsEntry,
    namespace_codes: &HashMap<u16, String>,
    graph_iri: Option<&str>,
) -> Result<()> {
    if novelty.is_empty() {
        return Ok(());
    }

    let mut subj_set: HashSet<Sid> = HashSet::new();
    let mut obj_set: HashSet<Sid> = HashSet::new();
    let mut events: Vec<(Sid, Sid, Sid, i64)> = Vec::new();

    for flake_id in novelty.iter_index(IndexType::Post) {
        let flake = novelty.get_flake(flake_id);

        // Graph filter: only include flakes belonging to the requested graph.
        if !flake_in_graph(flake, graph_iri, namespace_codes) {
            continue;
        }

        if is_rdf_type(&flake.p) {
            continue;
        }

        let FlakeValue::Ref(obj_sid) = &flake.o else {
            continue;
        };

        let delta = if flake.op { 1i64 } else { -1i64 };
        events.push((flake.s.clone(), flake.p.clone(), obj_sid.clone(), delta));
        subj_set.insert(flake.s.clone());
        obj_set.insert(obj_sid.clone());
    }

    if events.is_empty() {
        return Ok(());
    }

    let overlay: &dyn OverlayProvider = novelty;

    let mut subjects: Vec<Sid> = subj_set.into_iter().collect();
    subjects.sort();
    let mut objects: Vec<Sid> = obj_set.into_iter().collect();
    objects.sort();

    let subj_classes = fluree_db_policy::lookup_subject_classes(&subjects, db, overlay, to_t, g_id)
        .await
        .map_err(|e| LedgerInfoError::ClassLookup(e.to_string()))?;

    let obj_classes = fluree_db_policy::lookup_subject_classes(&objects, db, overlay, to_t, g_id)
        .await
        .map_err(|e| LedgerInfoError::ClassLookup(e.to_string()))?;

    // Aggregate deltas: subject_class -> predicate -> object_class -> delta
    let mut deltas: HashMap<Sid, HashMap<Sid, HashMap<Sid, i64>>> = HashMap::new();
    for (s, p, o, delta) in events {
        let Some(s_classes) = subj_classes.get(&s) else {
            continue;
        };
        let Some(o_classes) = obj_classes.get(&o) else {
            continue;
        };

        for sc in s_classes {
            for oc in o_classes {
                *deltas
                    .entry(sc.clone())
                    .or_default()
                    .entry(p.clone())
                    .or_default()
                    .entry(oc.clone())
                    .or_insert(0) += delta;
            }
        }
    }

    if deltas.is_empty() {
        return Ok(());
    }

    let classes = graph_entry.classes.get_or_insert_with(Vec::new);

    let mut class_idx: HashMap<Sid, usize> = HashMap::with_capacity(classes.len());
    for (i, c) in classes.iter().enumerate() {
        class_idx.insert(c.class_sid.clone(), i);
    }

    for (class_sid, prop_map) in deltas {
        let idx = match class_idx.get(&class_sid).copied() {
            Some(i) => i,
            None => {
                classes.push(ClassStatEntry {
                    class_sid: class_sid.clone(),
                    count: 0,
                    properties: Vec::new(),
                });
                let i = classes.len() - 1;
                class_idx.insert(class_sid.clone(), i);
                i
            }
        };

        let class_entry = &mut classes[idx];

        for (prop_sid, target_map) in prop_map {
            let pidx = class_entry
                .properties
                .iter()
                .position(|u| u.property_sid == prop_sid)
                .unwrap_or_else(|| {
                    class_entry.properties.push(ClassPropertyUsage {
                        property_sid: prop_sid.clone(),
                        datatypes: Vec::new(),
                        langs: Vec::new(),
                        ref_classes: Vec::new(),
                    });
                    class_entry.properties.len() - 1
                });

            let usage = &mut class_entry.properties[pidx];

            let mut merged: HashMap<Sid, i64> = usage
                .ref_classes
                .iter()
                .map(|rc| (rc.class_sid.clone(), rc.count as i64))
                .collect();

            for (target_class, delta) in target_map {
                *merged.entry(target_class).or_insert(0) += delta;
            }

            let mut out: Vec<ClassRefCount> = merged
                .into_iter()
                .filter_map(|(sid, count)| {
                    (count > 0).then_some(ClassRefCount {
                        class_sid: sid,
                        count: count as u64,
                    })
                })
                .collect();
            out.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
            usage.ref_classes = out;
        }

        class_entry
            .properties
            .sort_by(|a, b| a.property_sid.cmp(&b.property_sid));
    }

    classes.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
    Ok(())
}

// ============================================================================
// Commit / Nameservice JSON-LD helpers
// ============================================================================

/// Build commit JSON-LD in Clojure parity format.
async fn build_commit_jsonld<S: Storage + Clone>(
    storage: &S,
    head_id: &fluree_db_core::ContentId,
    alias: &str,
) -> Result<JsonValue> {
    let store = fluree_db_core::content_store_for(storage.clone(), alias);
    let commit = load_commit_by_id(&store, head_id)
        .await
        .map_err(|e| LedgerInfoError::CommitLoad(e.to_string()))?;

    let mut obj = json!({
        "@context": "https://ns.flur.ee/db/v1",
        "type": ["Commit"],
        "id": head_id.to_string(),
        "ledger_id": alias,
    });

    if let Some(id) = &commit.id {
        obj["id"] = json!(id.to_string());
    }

    if let Some(time) = &commit.time {
        obj["time"] = json!(time);
    }

    if let Some(prev_ref) = &commit.previous_ref {
        let prev_obj = json!({
            "type": ["Commit"],
            "id": prev_ref.id.to_string(),
        });
        obj["previous"] = prev_obj;
    }

    obj["data"] = json!({
        "type": ["DB"],
        "t": commit.t,
    });

    obj["ns"] = json!([{"id": alias}]);

    Ok(obj)
}

/// Convert NsRecord to JSON-LD format for nameservice queries.
pub fn ns_record_to_jsonld(record: &NsRecord) -> JsonValue {
    let ledger_name = split_ledger_id(&record.ledger_id)
        .map(|(ledger, _branch)| ledger)
        .unwrap_or_else(|_| record.name.clone());

    let canonical_id = format_ledger_id(&ledger_name, &record.branch);

    let status = if record.retracted {
        "retracted"
    } else {
        "ready"
    };

    let mut obj = json!({
        "@context": { "f": "https://ns.flur.ee/db#" },
        "@id": &canonical_id,
        "@type": ["f:LedgerSource"],
        "f:ledger": { "@id": &ledger_name },
        "f:branch": &record.branch,
        "f:t": record.commit_t,
        "f:status": status,
    });

    if let Some(ref cid) = record.commit_head_id {
        let mut commit_obj = serde_json::Map::new();
        commit_obj.insert("@id".to_string(), json!(cid.to_string()));
        obj["f:ledgerCommit"] = JsonValue::Object(commit_obj);
    }
    if let Some(ref cid) = record.index_head_id {
        let mut index_obj = serde_json::Map::new();
        index_obj.insert("@id".to_string(), json!(cid.to_string()));
        index_obj.insert("f:t".to_string(), json!(record.index_t));
        obj["f:ledgerIndex"] = JsonValue::Object(index_obj);
    }
    if let Some(ref ctx_cid) = record.default_context {
        obj["f:defaultContextCid"] = json!(ctx_cid.to_string());
    }

    obj
}

/// Convert GraphSourceRecord to JSON-LD format for nameservice queries.
pub fn gs_record_to_jsonld(record: &GraphSourceRecord) -> JsonValue {
    let canonical_id = format_ledger_id(&record.name, &record.branch);

    let status = if record.retracted {
        "retracted"
    } else {
        "ready"
    };

    let kind_type_str = match record.source_type.kind() {
        fluree_db_nameservice::GraphSourceKind::Index => "f:IndexSource",
        fluree_db_nameservice::GraphSourceKind::Mapped => "f:MappedSource",
        fluree_db_nameservice::GraphSourceKind::Ledger => "f:LedgerSource",
    };

    let mut obj = json!({
        "@context": { "f": "https://ns.flur.ee/db#" },
        "@id": &canonical_id,
        "@type": [kind_type_str, record.source_type.to_type_string()],
        "f:name": &record.name,
        "f:branch": &record.branch,
        "f:status": status,
        "f:graphSourceConfig": { "@value": &record.config },
        "f:graphSourceDependencies": &record.dependencies,
    });

    if let Some(ref index_id) = record.index_id {
        obj["f:graphSourceIndex"] = json!(index_id.to_string());
        obj["f:graphSourceIndexT"] = json!(record.index_t);
    }

    obj
}

// ============================================================================
// Stats rendering helpers
// ============================================================================

/// Decode graph-scoped property stats with IRI compaction.
///
/// Uses the `BinaryIndexStore` to resolve p_id -> predicate IRI.
fn decode_graph_property_stats(
    properties: &[GraphPropertyStatEntry],
    compactor: &IriCompactor,
    schema_index: &SchemaIndex,
    store: &BinaryIndexStore,
    include_datatypes: bool,
) -> Result<JsonValue> {
    let mut result = Map::new();

    for entry in properties {
        // Resolve p_id to IRI via the binary index store
        let Some(full_iri) = store.resolve_predicate_iri(entry.p_id) else {
            tracing::debug!(
                p_id = entry.p_id,
                "skipping unknown predicate in graph stats"
            );
            continue;
        };
        let compacted = compactor.compact_vocab_iri(full_iri);

        // Try to find the SID for schema lookups
        let sid_for_schema = compactor.try_encode_iri(full_iri);

        let mut prop_obj = Map::new();
        prop_obj.insert("count".to_string(), json!(entry.count));
        prop_obj.insert("ndv-values".to_string(), json!(entry.ndv_values));
        prop_obj.insert("ndv-subjects".to_string(), json!(entry.ndv_subjects));
        prop_obj.insert("last-modified-t".to_string(), json!(entry.last_modified_t));

        if include_datatypes {
            let mut dts = Map::new();
            for (tag, count) in &entry.datatypes {
                let label = datatype_display_string(*tag);
                dts.insert(label, json!(*count));
            }
            prop_obj.insert("datatypes".to_string(), JsonValue::Object(dts));
        }

        // Compute selectivity
        prop_obj.insert(
            "selectivity-value".to_string(),
            json!(compute_selectivity(entry.count, entry.ndv_values)),
        );
        prop_obj.insert(
            "selectivity-subject".to_string(),
            json!(compute_selectivity(entry.count, entry.ndv_subjects)),
        );

        // Add sub-property-of from schema hierarchy
        if let Some(sid) = &sid_for_schema {
            if let Some(schema_info) = schema_index.get(sid) {
                if !schema_info.parent_props.is_empty() {
                    let parent_iris: Vec<String> = schema_info
                        .parent_props
                        .iter()
                        .filter_map(|parent_sid| {
                            compactor
                                .decode_sid(parent_sid)
                                .ok()
                                .map(|iri| compactor.compact_vocab_iri(&iri))
                        })
                        .collect();
                    if !parent_iris.is_empty() {
                        prop_obj.insert("sub-property-of".to_string(), json!(parent_iris));
                    }
                }
            }
        }

        result.insert(compacted, JsonValue::Object(prop_obj));
    }

    Ok(JsonValue::Object(result))
}

/// Decode class statistics with IRI compaction, including types/langs/ref-classes.
fn decode_class_stats(
    classes: &Option<Vec<ClassStatEntry>>,
    compactor: &IriCompactor,
    schema_index: &SchemaIndex,
) -> Result<JsonValue> {
    let mut result = Map::new();

    let Some(classes) = classes else {
        return Ok(JsonValue::Object(result));
    };

    for entry in classes {
        let iri = compactor
            .decode_sid(&entry.class_sid)
            .map_err(|e| match e {
                crate::format::FormatError::UnknownNamespace(code) => {
                    LedgerInfoError::UnknownNamespace(code)
                }
                _ => LedgerInfoError::Storage(e.to_string()),
            })?;
        let compacted = compactor.compact_vocab_iri(&iri);

        let mut class_obj = Map::new();
        class_obj.insert("count".to_string(), json!(entry.count));

        // Add subclass-of from schema hierarchy
        if let Some(schema_info) = schema_index.get(&entry.class_sid) {
            if !schema_info.subclass_of.is_empty() {
                let parent_iris: Vec<String> = schema_info
                    .subclass_of
                    .iter()
                    .filter_map(|parent_sid| {
                        compactor
                            .decode_sid(parent_sid)
                            .ok()
                            .map(|iri| compactor.compact_vocab_iri(&iri))
                    })
                    .collect();
                if !parent_iris.is_empty() {
                    class_obj.insert("subclass-of".to_string(), json!(parent_iris));
                }
            }
        }

        // Decode class->property stats with types/langs/ref-classes
        let mut props_map = Map::new();
        let mut props_list: Vec<JsonValue> = Vec::new();

        for usage in &entry.properties {
            let prop_iri = compactor
                .decode_sid(&usage.property_sid)
                .map_err(|e| match e {
                    crate::format::FormatError::UnknownNamespace(code) => {
                        LedgerInfoError::UnknownNamespace(code)
                    }
                    _ => LedgerInfoError::Storage(e.to_string()),
                })?;
            let prop_compacted = compactor.compact_vocab_iri(&prop_iri);
            props_list.push(json!(prop_compacted.clone()));

            let mut prop_obj = Map::new();

            // types: per-datatype counts
            let mut types_obj = Map::new();
            for &(tag, count) in &usage.datatypes {
                let label = datatype_display_string(tag);
                types_obj.insert(label, json!(count));
            }
            prop_obj.insert("types".to_string(), JsonValue::Object(types_obj));

            // langs: per-language-tag counts
            let mut langs_obj = Map::new();
            for (lang, count) in &usage.langs {
                langs_obj.insert(lang.clone(), json!(*count));
            }
            prop_obj.insert("langs".to_string(), JsonValue::Object(langs_obj));

            // ref-classes: per-target-class ref counts
            let mut refs_obj = Map::new();
            for rc in &usage.ref_classes {
                let class_iri = compactor.decode_sid(&rc.class_sid).map_err(|e| match e {
                    crate::format::FormatError::UnknownNamespace(code) => {
                        LedgerInfoError::UnknownNamespace(code)
                    }
                    _ => LedgerInfoError::Storage(e.to_string()),
                })?;
                let class_compacted = compactor.compact_vocab_iri(&class_iri);
                refs_obj.insert(class_compacted, json!(rc.count));
            }
            prop_obj.insert("ref-classes".to_string(), JsonValue::Object(refs_obj));

            props_map.insert(prop_compacted, JsonValue::Object(prop_obj));
        }

        class_obj.insert("properties".to_string(), JsonValue::Object(props_map));
        class_obj.insert("property-list".to_string(), JsonValue::Array(props_list));

        result.insert(compacted, JsonValue::Object(class_obj));
    }

    Ok(JsonValue::Object(result))
}

// ============================================================================
// Utility helpers
// ============================================================================

/// Convert a ValueTypeTag raw u8 to a display string suitable for JSON keys.
///
/// Special-cases `JSON_LD_ID` (16) -> `"@id"`. All others use the standard
/// `ValueTypeTag::Display` implementation (e.g., `"xsd:string"`).
fn datatype_display_string(tag: u8) -> String {
    if tag == ValueTypeTag::JSON_LD_ID.as_u8() {
        "@id".to_string()
    } else {
        ValueTypeTag::from_u8(tag).to_string()
    }
}

/// Compute selectivity: ceil(count/ndv), minimum 1, as INTEGER.
fn compute_selectivity(count: u64, ndv: u64) -> u64 {
    if ndv == 0 {
        1
    } else {
        ((count as f64 / ndv as f64).ceil() as u64).max(1)
    }
}

/// Parse a pre-index stats manifest (JSON) into `GraphStatsEntry` entries.
pub fn parse_pre_index_manifest(bytes: &[u8]) -> std::result::Result<Vec<GraphStatsEntry>, String> {
    let json: JsonValue =
        serde_json::from_slice(bytes).map_err(|e| format!("invalid JSON: {}", e))?;

    let graphs_arr = json
        .get("graphs")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "missing 'graphs' array".to_string())?;

    let mut entries = Vec::with_capacity(graphs_arr.len());
    for g in graphs_arr {
        let g_id = g
            .get("g_id")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| "missing g_id".to_string())? as GraphId;
        let flakes = g.get("flakes").and_then(|v| v.as_u64()).unwrap_or(0);
        let size = g.get("size").and_then(|v| v.as_u64()).unwrap_or(0);

        let props_arr = g
            .get("properties")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut properties = Vec::with_capacity(props_arr.len());
        for p in &props_arr {
            let p_id = p
                .get("p_id")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| "missing p_id".to_string())? as u32;
            let count = p.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
            let ndv_values = p.get("ndv_values").and_then(|v| v.as_u64()).unwrap_or(0);
            let ndv_subjects = p.get("ndv_subjects").and_then(|v| v.as_u64()).unwrap_or(0);
            let last_modified_t = p
                .get("last_modified_t")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);

            let dt_arr = p
                .get("datatypes")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let datatypes: Vec<(u8, u64)> = dt_arr
                .iter()
                .filter_map(|pair| {
                    let arr = pair.as_array()?;
                    if arr.len() == 2 {
                        Some((arr[0].as_u64()? as u8, arr[1].as_u64()?))
                    } else {
                        None
                    }
                })
                .collect();

            properties.push(GraphPropertyStatEntry {
                p_id,
                count,
                ndv_values,
                ndv_subjects,
                last_modified_t,
                datatypes,
            });
        }

        entries.push(GraphStatsEntry {
            g_id,
            flakes,
            size,
            properties,
            classes: None,
        });
    }

    Ok(entries)
}

// ============================================================================
// LedgerInfoBuilder
// ============================================================================

use crate::{ApiError, Fluree};
use fluree_db_nameservice::NameService;

/// Builder for retrieving comprehensive ledger metadata.
///
/// Created via [`Fluree::ledger_info()`]. Provides a fluent API for configuring
/// and executing ledger info requests.
///
/// # Example
///
/// ```ignore
/// let info = fluree.ledger_info("mydb:main")
///     .with_context(&context)
///     .for_graph("default")
///     .execute()
///     .await?;
/// ```
pub struct LedgerInfoBuilder<'a, S: Storage + 'static, N> {
    fluree: &'a Fluree<S, N>,
    ledger_id: String,
    context: Option<&'a JsonValue>,
    options: LedgerInfoOptions,
}

impl<'a, S, N> LedgerInfoBuilder<'a, S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Clone + Send + Sync + 'static,
{
    /// Create a new builder (called by `Fluree::ledger_info()`).
    pub(crate) fn new(fluree: &'a Fluree<S, N>, ledger_id: String) -> Self {
        Self {
            fluree,
            ledger_id,
            context: None,
            options: LedgerInfoOptions::default(),
        }
    }

    /// Set the JSON-LD context for IRI compaction in stats.
    pub fn with_context(mut self, context: &'a JsonValue) -> Self {
        self.context = Some(context);
        self
    }

    /// Include datatype breakdowns under `stats.properties[*]`.
    pub fn with_property_datatypes(mut self, enabled: bool) -> Self {
        self.options.include_property_datatypes = enabled;
        self
    }

    /// When enabled, make property "details" real-time (novelty-aware).
    pub fn with_realtime_property_details(mut self, enabled: bool) -> Self {
        self.options.realtime_property_details = enabled;
        self.options.include_property_datatypes = enabled;
        self
    }

    /// Select which graph to scope stats to by well-known name.
    ///
    /// - `"default"` -> default graph (g_id = 0)
    /// - `"txn-meta"` -> transaction metadata graph (g_id = 1)
    /// - Any other string is tried as a graph IRI
    pub fn for_graph(mut self, name: &str) -> Self {
        self.options.graph = GraphSelector::ByName(name.to_string());
        self
    }

    /// Select which graph to scope stats to by IRI.
    pub fn for_graph_iri(mut self, iri: &str) -> Self {
        self.options.graph = GraphSelector::ByIri(iri.to_string());
        self
    }

    /// Select which graph to scope stats to by numeric graph ID.
    pub fn for_g_id(mut self, g_id: GraphId) -> Self {
        self.options.graph = GraphSelector::ById(g_id);
        self
    }

    /// Execute the ledger info request.
    pub async fn execute(self) -> crate::Result<JsonValue> {
        let ledger = self.fluree.ledger(&self.ledger_id).await?;

        build_ledger_info_with_options(&ledger, self.fluree.storage(), self.context, self.options)
            .await
            .map_err(|e| ApiError::internal(format!("ledger_info failed: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_selectivity() {
        assert_eq!(compute_selectivity(100, 50), 2);
        assert_eq!(compute_selectivity(100, 100), 1);
        assert_eq!(compute_selectivity(100, 0), 1);
        assert_eq!(compute_selectivity(0, 0), 1);
        assert_eq!(compute_selectivity(3, 2), 2); // ceil(1.5) = 2
        assert_eq!(compute_selectivity(1, 100), 1); // ceil(0.01) = 1, but min is 1
    }

    #[test]
    fn test_datatype_display_string() {
        assert_eq!(datatype_display_string(0), "xsd:string");
        assert_eq!(datatype_display_string(16), "@id");
        assert_eq!(datatype_display_string(7), "xsd:double");
        assert_eq!(datatype_display_string(14), "rdf:langString");
    }

    #[test]
    fn test_ns_record_to_jsonld() {
        use fluree_db_core::{ContentId, ContentKind};
        let commit_cid = ContentId::new(ContentKind::Commit, b"abc");
        let index_cid = ContentId::new(ContentKind::IndexRoot, b"def");
        let record = NsRecord {
            ledger_id: "mydb:main".to_string(),
            name: "mydb:main".to_string(),
            branch: "main".to_string(),
            commit_head_id: Some(commit_cid.clone()),
            config_id: None,
            commit_t: 42,
            index_head_id: Some(index_cid),
            index_t: 40,
            default_context: None,
            retracted: false,
        };

        let json = ns_record_to_jsonld(&record);

        assert_eq!(json["@id"], "mydb:main");
        assert_eq!(json["@type"], json!(["f:LedgerSource"]));
        assert_eq!(json["f:ledger"]["@id"], "mydb");
        assert_eq!(json["f:branch"], "main");
        assert_eq!(json["f:t"], 42);
        assert_eq!(json["f:status"], "ready");
        assert_eq!(json["f:ledgerCommit"]["@id"], commit_cid.to_string());
        assert_eq!(json["f:ledgerIndex"]["f:t"], 40);
    }

    #[test]
    fn test_ns_record_to_jsonld_retracted() {
        use fluree_db_core::{ContentId, ContentKind};
        let commit_cid = ContentId::new(ContentKind::Commit, b"commit-data");
        let record = NsRecord {
            ledger_id: "mydb:main".to_string(),
            name: "mydb:main".to_string(),
            branch: "main".to_string(),
            commit_head_id: Some(commit_cid),
            config_id: None,
            commit_t: 10,
            index_head_id: None,
            index_t: 0,
            default_context: None,
            retracted: true,
        };

        let json = ns_record_to_jsonld(&record);
        assert_eq!(json["f:status"], "retracted");
    }

    #[test]
    fn test_gs_record_to_jsonld() {
        use fluree_db_core::{ContentId, ContentKind};
        let index_cid = ContentId::new(ContentKind::IndexRoot, b"snapshot-data");
        let record = GraphSourceRecord {
            graph_source_id: "my-search:main".to_string(),
            name: "my-search".to_string(),
            branch: "main".to_string(),
            source_type: fluree_db_nameservice::GraphSourceType::Bm25,
            config: r#"{"k1":1.2,"b":0.75}"#.to_string(),
            dependencies: vec!["source-ledger:main".to_string()],
            index_id: Some(index_cid.clone()),
            index_t: 42,
            retracted: false,
        };

        let json = gs_record_to_jsonld(&record);

        assert_eq!(json["@id"], "my-search:main");
        assert_eq!(json["@type"], json!(["f:IndexSource", "f:Bm25Index"]));
        assert_eq!(json["f:name"], "my-search");
        assert_eq!(json["f:branch"], "main");
        assert_eq!(json["f:status"], "ready");
        assert_eq!(
            json["f:graphSourceConfig"]["@value"],
            r#"{"k1":1.2,"b":0.75}"#
        );
        assert_eq!(
            json["f:graphSourceDependencies"],
            json!(["source-ledger:main"])
        );
        assert_eq!(json["f:graphSourceIndex"], index_cid.to_string());
        assert_eq!(json["f:graphSourceIndexT"], 42);
    }

    #[test]
    fn test_graph_selector_default() {
        assert_eq!(
            resolve_graph_selector(&GraphSelector::Default, None).unwrap(),
            0
        );
    }

    #[test]
    fn test_graph_selector_by_id() {
        assert_eq!(
            resolve_graph_selector(&GraphSelector::ById(3), None).unwrap(),
            3
        );
    }

    #[test]
    fn test_graph_selector_by_name_default() {
        let sel = GraphSelector::ByName("default".to_string());
        assert_eq!(resolve_graph_selector(&sel, None).unwrap(), 0);
    }

    #[test]
    fn test_graph_selector_by_name_txn_meta() {
        let sel = GraphSelector::ByName("txn-meta".to_string());
        assert_eq!(resolve_graph_selector(&sel, None).unwrap(), 1);
    }
}
