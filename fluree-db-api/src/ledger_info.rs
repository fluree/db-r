//! Ledger information API
//!
//! This module provides the `build_ledger_info` function that returns comprehensive
//! metadata about a ledger, including commit info, nameservice record, namespace
//! mappings, and statistics with decoded IRIs.
//!
//! ## Clojure Parity
//!
//! The response format matches Clojure's `ledger-info` API exactly, including:
//! - Commit JSON-LD (uses `"id"` not `"@id"`, `"type"` as array)
//! - Nameservice JSON-LD (uses `"@id"`, `"@type"`, `"@context"`)
//! - Stats with decoded IRIs and computed selectivity

use crate::format::iri::IriCompactor;
use fluree_db_core::address_path::ledger_id_to_path_prefix;
use fluree_db_core::comparator::IndexType;
use fluree_db_core::ids::GraphId;
use fluree_db_core::ledger_id::{format_ledger_id, split_ledger_id};
use fluree_db_core::value_id::ValueTypeTag;
use fluree_db_core::{
    is_rdf_type, ClassPropertyUsage, ClassRefCount, Db, FlakeValue, OverlayProvider, Sid, Storage,
};
use fluree_db_core::{
    ClassStatEntry, GraphPropertyStatEntry, GraphStatsEntry, IndexSchema, IndexStats,
    PropertyStatEntry, SchemaPredicateInfo,
};
use fluree_db_ledger::LedgerState;
use fluree_db_nameservice::{GraphSourceRecord, NsRecord};
use fluree_db_novelty::{load_commit_by_id, Novelty};
use fluree_graph_json_ld::ParsedContext;
use serde_json::{json, Map, Value as JsonValue};
use std::collections::{HashMap, HashSet};

/// Options controlling `ledger-info` stats detail and freshness.
///
/// Defaults preserve the fast, small “base” payload; callers can opt into
/// heavier/real-time details when needed.
#[derive(Debug, Clone, Copy, Default)]
pub struct LedgerInfoOptions {
    /// When true, augment property “details” with novelty deltas so the result
    /// is real-time (novelty-aware) rather than “as of last index”.
    pub realtime_property_details: bool,

    /// When true, include `datatypes` under `stats.properties[*]`.
    ///
    /// By default the API omits datatype breakdowns at the top-level property
    /// map to keep payloads small.
    pub include_property_datatypes: bool,
}

/// Schema index for fast SID → hierarchy lookup
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
}

/// Result type for ledger info operations
pub type Result<T> = std::result::Result<T, LedgerInfoError>;

/// Build comprehensive ledger metadata with Clojure parity.
///
/// Returns JSON containing:
/// - `commit`: Commit info in JSON-LD format
/// - `nameservice`: NsRecord in JSON-LD format
/// - `stats`: Current statistics with decoded IRIs
/// - `index`: Index metadata (if available)
///
/// # Arguments
///
/// * `ledger` - The ledger state to get info for
/// * `context` - Optional JSON-LD context for IRI compaction in stats
///
/// # Response Format
///
/// The commit section uses non-standard JSON-LD:
/// - `"id"` instead of `"@id"`
/// - `"type"` as array instead of `"@type"`
/// - `@context` as string URL
///
/// The nameservice section uses standard JSON-LD keywords.
/// The stats section has IRIs optionally compacted via the provided context.
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

    // Get current stats (always returns IndexStats)
    let mut stats = ledger.current_stats();

    // Optional: real-time property details (merge novelty datatype deltas).
    //
    // `current_stats()` updates property counts and preserves indexed datatype
    // breakdowns, but does not adjust datatype counts for novelty by default.
    if options.realtime_property_details && options.include_property_datatypes {
        merge_property_datatypes_from_novelty(&mut stats, &ledger.novelty);
    }

    // Optional: real-time “details” (merge novelty ref-edge deltas).
    if options.realtime_property_details {
        merge_class_ref_edges_from_novelty(
            &ledger.db,
            ledger.novelty.as_ref(),
            ledger.t(),
            &mut stats,
        )
        .await?;
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

    // 1. Commit section (ALWAYS include, even if None - for Clojure parity)
    if let Some(head_cid) = &ledger.head_commit_id {
        match build_commit_jsonld(storage, head_cid, &ledger.db.ledger_id).await {
            Ok(commit_json) => {
                result.insert("commit".to_string(), commit_json);
            }
            Err(e) => {
                // Include error in response for debugging
                result.insert("commit".to_string(), json!({ "error": format!("{}", e) }));
            }
        }
    } else {
        // Always include commit key for parity (null when no commit)
        result.insert("commit".to_string(), JsonValue::Null);
    }

    // Include content identifiers when available
    if let Some(ref cid) = ledger.head_commit_id {
        result.insert("commitId".to_string(), json!(cid.to_string()));
    }
    if let Some(ref cid) = ledger.head_index_id {
        result.insert("indexId".to_string(), json!(cid.to_string()));
    }

    // 2. Nameservice section
    if let Some(ns_record) = &ledger.ns_record {
        result.insert("nameservice".to_string(), ns_record_to_jsonld(ns_record));
    }

    // 3. Stats section (with hierarchy fields)
    result.insert(
        "stats".to_string(),
        build_stats(ledger, &stats, &compactor, &schema_index, options)?,
    );

    // 5. Index section (if available)
    if let Some(ns_record) = &ledger.ns_record {
        if ns_record.index_head_id.is_some() || ns_record.index_t > 0 {
            let mut index_obj = json!({
                "t": ns_record.index_t,
            });
            // Prefer head_index_id from LedgerState; fall back to ns_record
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

/// Merge novelty deltas into top-level property datatype counts.
///
/// This is intentionally scoped to *property*-level datatype stats (not class-scoped),
/// so it can be computed cheaply from novelty without additional index lookups.
fn merge_property_datatypes_from_novelty(stats: &mut IndexStats, novelty: &Novelty) {
    if novelty.is_empty() {
        return;
    }

    let Some(props) = stats.properties.as_mut() else {
        return;
    };

    // Property SID -> datatype tag -> delta count
    let mut deltas: HashMap<(u16, String), HashMap<u8, i64>> = HashMap::new();
    for flake_id in novelty.iter_index(IndexType::Post) {
        let flake = novelty.get_flake(flake_id);
        let delta = if flake.op { 1i64 } else { -1i64 };

        let prop_sid = (flake.p.namespace_code, flake.p.name.to_string());
        let tag = ValueTypeTag::from_ns_name(flake.dt.namespace_code, &flake.dt.name);
        if tag == ValueTypeTag::UNKNOWN {
            continue;
        }

        *deltas
            .entry(prop_sid)
            .or_default()
            .entry(tag.as_u8())
            .or_insert(0) += delta;
    }

    if deltas.is_empty() {
        return;
    }

    // Index existing entries for in-place updates.
    let mut by_sid: HashMap<(u16, String), usize> = HashMap::with_capacity(props.len());
    for (idx, entry) in props.iter().enumerate() {
        by_sid.insert(entry.sid.clone(), idx);
    }

    for (sid, delta_map) in deltas {
        let Some(&idx) = by_sid.get(&sid) else {
            continue;
        };
        let entry = &mut props[idx];

        let mut merged: HashMap<u8, i64> = entry
            .datatypes
            .iter()
            .map(|(tag, count)| (*tag, *count as i64))
            .collect();
        for (tag, delta) in delta_map {
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

/// Merge novelty deltas into class-scoped ref-edge counts.
///
/// Updates `IndexStats.classes[*].properties[*].ref_classes` by applying deltas from
/// ref-valued novelty flakes, attributed using the *current* (novelty-aware) rdf:type
/// of both the subject and the referenced object.
///
/// Notes:
/// - This is intentionally *on-demand*; it can be more expensive than the base payload.
/// - This currently accounts for **ref assertions/retractions in novelty**. It does not
///   attempt to reattribute *indexed* ref edges when only rdf:type changes in novelty.
async fn merge_class_ref_edges_from_novelty(
    db: &Db,
    novelty: &Novelty,
    to_t: i64,
    stats: &mut IndexStats,
) -> Result<()> {
    if novelty.is_empty() {
        return Ok(());
    }

    // Collect ref flakes from novelty (subject, predicate, object, delta) and the set of
    // subjects/objects we need rdf:type for.
    let mut subj_set: HashSet<Sid> = HashSet::new();
    let mut obj_set: HashSet<Sid> = HashSet::new();
    let mut events: Vec<(Sid, Sid, Sid, i64)> = Vec::new();

    for flake_id in novelty.iter_index(IndexType::Post) {
        let flake = novelty.get_flake(flake_id);

        // Skip rdf:type itself (not an “edge property”).
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

    // Look up current classes for all involved subjects/objects (novelty-aware).
    let mut subjects: Vec<Sid> = subj_set.into_iter().collect();
    subjects.sort();
    let mut objects: Vec<Sid> = obj_set.into_iter().collect();
    objects.sort();

    let subj_classes = fluree_db_policy::lookup_subject_classes(&subjects, db, overlay, to_t)
        .await
        .map_err(|e| LedgerInfoError::ClassLookup(e.to_string()))?;

    let obj_classes = fluree_db_policy::lookup_subject_classes(&objects, db, overlay, to_t)
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

    let classes = stats.classes.get_or_insert_with(Vec::new);

    // Index class entries by SID for quick upsert.
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
            // Find or insert property usage.
            let pidx = class_entry
                .properties
                .iter()
                .position(|u| u.property_sid == prop_sid)
                .unwrap_or_else(|| {
                    class_entry.properties.push(ClassPropertyUsage {
                        property_sid: prop_sid.clone(),
                        ref_classes: Vec::new(),
                    });
                    class_entry.properties.len() - 1
                });

            let usage = &mut class_entry.properties[pidx];

            // Merge deltas into existing ref_classes.
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

        // Keep deterministic ordering.
        class_entry
            .properties
            .sort_by(|a, b| a.property_sid.cmp(&b.property_sid));
    }

    classes.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
    Ok(())
}

/// Build commit JSON-LD in Clojure parity format.
///
/// Uses `"id"` not `"@id"`, `"type"` as array not `"@type"`.
/// Returns (commit_json, index_id) tuple.
///
/// Loads commit by ContentId via the content store.
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

    // Add content-address IRI if available (now ContentId)
    if let Some(id) = &commit.id {
        obj["id"] = json!(id.to_string());
    }

    // Add timestamp if available
    if let Some(time) = &commit.time {
        obj["time"] = json!(time);
    }

    // NOTE: `t` is NOT on commit itself in Clojure - it's inside `data`

    // Previous commit reference (CommitRef now has only `id: ContentId`)
    if let Some(prev_ref) = &commit.previous_ref {
        let prev_obj = json!({
            "type": ["Commit"],
            "id": prev_ref.id.to_string(),
        });
        obj["previous"] = prev_obj;
    }

    // Data block - embedded DB metadata (t goes HERE, not on commit).
    // CommitData has been removed; we always include a minimal data block with t.
    obj["data"] = json!({
        "type": ["DB"],
        "t": commit.t,
    });

    // NS block
    obj["ns"] = json!([{"id": alias}]);

    // Index info is NOT embedded in commits — it is tracked via the nameservice
    // and LedgerState.head_index_id. The caller populates the index section
    // from those canonical sources.

    Ok(obj)
}

/// Convert NsRecord to JSON-LD format for nameservice queries.
///
/// Uses standard JSON-LD keywords: `@id`, `@type`, `@context`.
/// Includes `f:status` field that reflects retracted state.
///
/// This function is used both for `ledger-info` responses and for
/// `query-nameservice` temporary ledger population.
pub fn ns_record_to_jsonld(record: &NsRecord) -> JsonValue {
    // Use parse_alias for ledger name extraction (avoids edge cases)
    let ledger_name = split_ledger_id(&record.ledger_id)
        .map(|(ledger, _branch)| ledger)
        .unwrap_or_else(|_| record.name.clone());

    // Use canonical form for @id: "{ledger_name}:{branch}"
    let canonical_id = format_ledger_id(&ledger_name, &record.branch);

    // Reflect retracted state in status
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

    // Emit commit ref when CID is present
    if let Some(ref cid) = record.commit_head_id {
        let mut commit_obj = serde_json::Map::new();
        commit_obj.insert("@id".to_string(), json!(cid.to_string()));
        obj["f:ledgerCommit"] = JsonValue::Object(commit_obj);
    }
    // Emit index ref when CID is present
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
///
/// Uses standard JSON-LD keywords with `f:` namespace.
/// Includes `f:status` field that reflects retracted state.
pub fn gs_record_to_jsonld(record: &GraphSourceRecord) -> JsonValue {
    // Use canonical form for @id: "{name}:{branch}"
    let canonical_id = format_ledger_id(&record.name, &record.branch);

    // Reflect retracted state in status
    let status = if record.retracted {
        "retracted"
    } else {
        "ready"
    };

    // Determine the kind type string
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

    // Include index fields if present (matching ns@v2 on-disk format)
    if let Some(ref index_id) = record.index_id {
        obj["f:graphSourceIndex"] = json!(index_id.to_string());
        obj["f:graphSourceIndexT"] = json!(record.index_t);
    }

    obj
}

/// Build stats section with decoded IRIs and hierarchy fields.
fn build_stats(
    ledger: &LedgerState,
    stats: &IndexStats,
    compactor: &IriCompactor,
    schema_index: &SchemaIndex,
    options: LedgerInfoOptions,
) -> Result<JsonValue> {
    // CANONICAL RULE for indexed_t:
    // 1. Use ns_record.index_t if ns_record exists (even if 0 when no index yet)
    // 2. Fall back to db.t if no ns_record
    let indexed_t = ledger
        .ns_record
        .as_ref()
        .map(|r| r.index_t)
        .unwrap_or(ledger.db.t);

    let mut stats_obj = json!({
        "flakes": stats.flakes,
        "size": stats.size,
        "indexed": indexed_t,
        "properties": decode_property_stats(&stats.properties, compactor, schema_index, options)?,
        "classes": decode_class_stats(&stats.classes, compactor, schema_index)?,
    });

    // Add per-graph stats when available (ID-keyed; IRI resolution requires
    // predicate/graph dictionaries to be wired to the API layer).
    if let Some(ref graphs) = stats.graphs {
        stats_obj["graphs"] = encode_graph_stats(graphs);
    }

    Ok(stats_obj)
}

/// Decode property statistics with IRI compaction.
///
/// Property stats do NOT include types/ref-classes/langs - those only appear
/// in class→property breakdowns. Includes `sub-property-of` from schema.
fn decode_property_stats(
    properties: &Option<Vec<PropertyStatEntry>>,
    compactor: &IriCompactor,
    schema_index: &SchemaIndex,
    options: LedgerInfoOptions,
) -> Result<JsonValue> {
    let mut result = Map::new();

    let Some(properties) = properties else {
        return Ok(JsonValue::Object(result));
    };

    for entry in properties {
        let sid = Sid::new(entry.sid.0, &entry.sid.1);
        let iri = compactor.decode_sid(&sid).map_err(|e| match e {
            crate::format::FormatError::UnknownNamespace(code) => {
                LedgerInfoError::UnknownNamespace(code)
            }
            _ => LedgerInfoError::Storage(e.to_string()),
        })?;
        let compacted = compactor.compact_vocab_iri(&iri);

        let mut prop_obj = Map::new();
        prop_obj.insert("count".to_string(), json!(entry.count));
        prop_obj.insert("ndv-values".to_string(), json!(entry.ndv_values));
        prop_obj.insert("ndv-subjects".to_string(), json!(entry.ndv_subjects));
        prop_obj.insert("last-modified-t".to_string(), json!(entry.last_modified_t));

        // Optional datatype breakdown (normally omitted to keep payloads small).
        if options.include_property_datatypes {
            let mut dts = Map::new();
            for (tag, count) in &entry.datatypes {
                let label = ValueTypeTag::from_u8(*tag).to_string();
                dts.insert(label, json!(*count));
            }
            prop_obj.insert("datatypes".to_string(), JsonValue::Object(dts));
        }

        // Compute selectivity as integers
        prop_obj.insert(
            "selectivity-value".to_string(),
            json!(compute_selectivity(entry.count, entry.ndv_values)),
        );
        prop_obj.insert(
            "selectivity-subject".to_string(),
            json!(compute_selectivity(entry.count, entry.ndv_subjects)),
        );

        // Add sub-property-of from schema hierarchy
        if let Some(schema_info) = schema_index.get(&sid) {
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

        result.insert(compacted, JsonValue::Object(prop_obj));
    }

    Ok(JsonValue::Object(result))
}

/// Decode class statistics with IRI compaction and `subclass-of` from schema.
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

        // Decode class→property stats (map keyed by property IRI).
        //
        // This is used by:
        // - f:onClass policy indexing (class→property presence)
        // - Ontology/graph visualization (ref target class counts)
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

            if !usage.ref_classes.is_empty() {
                let mut refs_obj = Map::new();
                let mut total: u64 = 0;
                for rc in &usage.ref_classes {
                    let class_iri = compactor.decode_sid(&rc.class_sid).map_err(|e| match e {
                        crate::format::FormatError::UnknownNamespace(code) => {
                            LedgerInfoError::UnknownNamespace(code)
                        }
                        _ => LedgerInfoError::Storage(e.to_string()),
                    })?;
                    let class_compacted = compactor.compact_vocab_iri(&class_iri);
                    refs_obj.insert(class_compacted, json!(rc.count));
                    total = total.saturating_add(rc.count);
                }
                // Primary key (new): `refs`
                prop_obj.insert("refs".to_string(), JsonValue::Object(refs_obj.clone()));
                // Compatibility key: `ref-classes`
                prop_obj.insert("ref-classes".to_string(), JsonValue::Object(refs_obj));
                prop_obj.insert("count".to_string(), json!(total));
            }

            props_map.insert(prop_compacted, JsonValue::Object(prop_obj));
        }

        class_obj.insert("properties".to_string(), JsonValue::Object(props_map));
        class_obj.insert("property-list".to_string(), JsonValue::Array(props_list));

        result.insert(compacted, JsonValue::Object(class_obj));
    }

    Ok(JsonValue::Object(result))
}

/// Parse a pre-index stats manifest (JSON) into `GraphStatsEntry` entries.
///
/// The manifest is produced by `finalize_pre_index_stats` in the ingest tool.
/// It has the structure:
/// ```json
/// {
///   "graphs": [
///     {
///       "g_id": 0, "flakes": 1000, "size": 0,
///       "properties": [
///         { "p_id": 1, "count": 500, "ndv_values": 400, "ndv_subjects": 300,
///           "last_modified_t": -42, "datatypes": [[0, 500]] }
///       ]
///     }
///   ]
/// }
/// ```
/// Parse a pre-index stats manifest (JSON) into `GraphStatsEntry` entries.
///
/// This is produced by the ingest tool (`finalize_pre_index_stats`) and can be used
/// to feed query planning with NDV/count stats before an index refresh has published
/// its own `IndexStats.graphs`.
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
        });
    }

    Ok(entries)
}

/// Encode per-graph stats as a JSON array.
///
/// Each entry is keyed by numeric `g_id` and `p_id`. The `ValueTypeTag` values
/// are resolved to compact IRIs (e.g., `"xsd:string"`) via compile-time
/// constants. Full predicate and graph IRI resolution requires wiring the
/// predicate/graph dictionaries to the API layer (future work).
///
/// Excludes `g_id = 1` (transaction metadata graph) from the output.
fn encode_graph_stats(graphs: &[GraphStatsEntry]) -> JsonValue {
    let entries: Vec<JsonValue> = graphs
        .iter()
        .filter(|g| g.g_id != 1) // exclude txn-meta graph
        .map(|g| {
            let properties: Vec<JsonValue> = g
                .properties
                .iter()
                .map(|p| {
                    // Resolve ValueTypeTag to compact IRI via Display
                    let mut dt_obj = Map::new();
                    for &(dt_raw, count) in &p.datatypes {
                        let dt_iri = ValueTypeTag::from_u8(dt_raw).to_string();
                        dt_obj.insert(dt_iri, json!(count));
                    }

                    json!({
                        "p_id": p.p_id,
                        "count": p.count,
                        "ndv-values": p.ndv_values,
                        "ndv-subjects": p.ndv_subjects,
                        "last-modified-t": p.last_modified_t,
                        "datatypes": dt_obj,
                    })
                })
                .collect();

            json!({
                "g_id": g.g_id,
                "flakes": g.flakes,
                "size": g.size,
                "properties": properties,
            })
        })
        .collect();

    JsonValue::Array(entries)
}

/// Compute selectivity: ceil(count/ndv), minimum 1, as INTEGER.
fn compute_selectivity(count: u64, ndv: u64) -> u64 {
    if ndv == 0 {
        1
    } else {
        ((count as f64 / ndv as f64).ceil() as u64).max(1)
    }
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
    ///
    /// When provided, IRIs in the stats section will be compacted using
    /// prefixes from this context.
    pub fn with_context(mut self, context: &'a JsonValue) -> Self {
        self.context = Some(context);
        self
    }

    /// Include datatype breakdowns under `stats.properties[*]` (indexed view by default).
    pub fn with_property_datatypes(mut self, enabled: bool) -> Self {
        self.options.include_property_datatypes = enabled;
        self
    }

    /// When enabled, make property “details” real-time (novelty-aware).
    ///
    /// This enables the heavier, novelty-aware details used by UIs/optimizers:
    /// - merges novelty datatype deltas into `stats.properties[*].datatypes`
    /// - merges novelty ref-edge deltas into `stats.classes[*].properties[*].refs`
    pub fn with_realtime_property_details(mut self, enabled: bool) -> Self {
        self.options.realtime_property_details = enabled;
        // If you want real-time property details, include the datatype payload.
        self.options.include_property_datatypes = enabled;
        self
    }

    /// Execute the ledger info request.
    ///
    /// Loads the ledger (using cache if available) and returns comprehensive
    /// metadata including commit info, nameservice record, stats,
    /// and index information.
    pub async fn execute(self) -> crate::Result<JsonValue> {
        // Load the ledger (uses cache if caching is enabled)
        let ledger = self.fluree.ledger(&self.ledger_id).await?;

        // Build and return the ledger info
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
        // The index CID is rendered as a base32 multibase string
        assert_eq!(json["f:graphSourceIndex"], index_cid.to_string());
        assert_eq!(json["f:graphSourceIndexT"], 42);
    }
}
