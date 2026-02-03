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
use fluree_db_core::{
    ClassStatEntry, GraphPropertyStatEntry, IndexSchema, IndexStats, GraphStatsEntry,
    PropertyStatEntry, SchemaPredicateInfo,
};
use fluree_db_core::value_id::DatatypeId;
use fluree_db_core::{Sid, Storage};
use fluree_db_ledger::LedgerState;
use fluree_db_core::alias as core_alias;
use fluree_db_nameservice::{parse_alias, NsRecord, VgNsRecord};
use fluree_db_novelty::load_commit;
use fluree_graph_json_ld::ParsedContext;
use serde_json::{json, Map, Value as JsonValue};
use std::collections::HashMap;

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
    #[error("No commit address available")]
    NoCommitAddress,

    #[error("Failed to load commit: {0}")]
    CommitLoad(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Unknown namespace code: {0}")]
    UnknownNamespace(i32),
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
pub async fn build_ledger_info<S>(
    ledger: &LedgerState<S>,
    context: Option<&JsonValue>,
) -> Result<JsonValue>
where
    S: Storage + Clone + 'static,
{
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

    // Pre-index fallback: if no graph stats from index, try loading the pre-index manifest
    if stats.graphs.is_none() {
        let alias_prefix = fluree_db_core::address_path::alias_to_path_prefix(&ledger.db.alias)
            .unwrap_or_else(|_| ledger.db.alias.replace(':', "/"));
        let manifest_addr_primary =
            format!("fluree:file://{}/stats/pre-index-stats.json", alias_prefix);
        if let Ok(bytes) = ledger.db.storage.read_bytes(&manifest_addr_primary).await {
            match parse_pre_index_manifest(&bytes) {
                Ok(graphs) => {
                    tracing::debug!(
                        graphs = graphs.len(),
                        "loaded pre-index stats manifest"
                    );
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
    let mut index_id_from_commit: Option<String> = None;
    if let Some(commit_addr) = &ledger.head_commit {
        match build_commit_jsonld(&ledger.db.storage, commit_addr, &ledger.db.alias).await {
            Ok((commit_json, idx_id)) => {
                result.insert("commit".to_string(), commit_json);
                index_id_from_commit = idx_id;
            }
            Err(e) => {
                // Include error in response for debugging
                result.insert(
                    "commit".to_string(),
                    json!({ "error": format!("{}", e) }),
                );
            }
        }
    } else {
        // Always include commit key for parity (null when no commit)
        result.insert("commit".to_string(), JsonValue::Null);
    }

    // 2. Nameservice section
    if let Some(ns_record) = &ledger.ns_record {
        result.insert("nameservice".to_string(), ns_record_to_jsonld(ns_record));
    }

    // 3. Stats section (with hierarchy fields)
    result.insert(
        "stats".to_string(),
        build_stats(ledger, &stats, &compactor, &schema_index)?,
    );

    // 5. Index section (if available) - include id from commit
    if let Some(ns_record) = &ledger.ns_record {
        if let Some(index_addr) = &ns_record.index_address {
            let mut index_obj = json!({
                "t": ns_record.index_t,
                "address": index_addr,
            });
            // Add index id if we got it from the commit
            if let Some(idx_id) = index_id_from_commit {
                index_obj["id"] = json!(idx_id);
            }
            result.insert("index".to_string(), index_obj);
        }
    }

    Ok(JsonValue::Object(result))
}

/// Build commit JSON-LD in Clojure parity format.
///
/// Uses `"id"` not `"@id"`, `"type"` as array not `"@type"`.
/// Returns (commit_json, index_id) tuple.
async fn build_commit_jsonld<S: Storage>(
    storage: &S,
    commit_address: &str,
    alias: &str,
) -> Result<(JsonValue, Option<String>)> {
    let commit = load_commit(storage, commit_address)
        .await
        .map_err(|e| LedgerInfoError::CommitLoad(e.to_string()))?;

    let mut obj = json!({
        "@context": "https://ns.flur.ee/ledger/v1",
        "type": ["Commit"],
        "v": commit.v,
        "address": commit_address,
        "alias": alias,
    });

    // Add content-address IRI if available
    if let Some(id) = &commit.id {
        obj["id"] = json!(id);
    }

    // Add timestamp if available
    if let Some(time) = &commit.time {
        obj["time"] = json!(time);
    }

    // NOTE: `t` is NOT on commit itself in Clojure - it's inside `data`

    // Previous commit reference
    if let Some(prev_ref) = &commit.previous_ref {
        let mut prev_obj = json!({
            "type": ["Commit"],
            "address": &prev_ref.address,
        });
        if let Some(prev_id) = &prev_ref.id {
            prev_obj["id"] = json!(prev_id);
        }
        obj["previous"] = prev_obj;
    } else if let Some(prev_addr) = &commit.previous {
        // Legacy format - just address
        obj["previous"] = json!({
            "type": ["Commit"],
            "address": prev_addr,
        });
    }

    // Data block - embedded DB metadata (t goes HERE, not on commit)
    if let Some(data) = &commit.data {
        let mut data_obj = json!({
            "type": ["DB"],
            "t": commit.t,
            "flakes": data.flakes,
            "size": data.size,
        });
        if let Some(data_id) = &data.id {
            data_obj["id"] = json!(data_id);
        }
        if let Some(data_addr) = &data.address {
            data_obj["address"] = json!(data_addr);
        }
        // Add data.previous reference if available
        if let Some(prev_data) = &data.previous {
            let mut prev_data_obj = json!({
                "type": ["DB"],
            });
            if let Some(prev_id) = &prev_data.id {
                prev_data_obj["id"] = json!(prev_id);
            }
            if let Some(prev_addr) = &prev_data.address {
                prev_data_obj["address"] = json!(prev_addr);
            }
            data_obj["previous"] = prev_data_obj;
        }
        obj["data"] = data_obj;
    } else {
        // Even without CommitData struct, still include data block with t
        obj["data"] = json!({
            "type": ["DB"],
            "t": commit.t,
        });
    }

    // NS block
    obj["ns"] = json!([{"id": alias}]);

    // Index block (if indexed at this commit)
    let mut index_id_out: Option<String> = None;
    if let Some(index) = &commit.index {
        let mut index_obj = json!({
            "type": ["Index"],
            "address": &index.address,
            "v": index.v,
        });
        if let Some(index_id) = &index.id {
            index_obj["id"] = json!(index_id);
            index_id_out = Some(index_id.clone());
        }
        // Add index.data with the indexed t
        if let Some(index_t) = index.t {
            index_obj["data"] = json!({
                "type": ["DB"],
                "t": index_t,
            });
        }
        obj["index"] = index_obj;
    } else if let Some(indexed_at) = &commit.indexed_at {
        // Legacy format
        obj["index"] = json!({
            "type": ["Index"],
            "address": indexed_at,
            "v": 2,
        });
    }

    Ok((obj, index_id_out))
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
    let ledger_name = parse_alias(&record.alias)
        .map(|(ledger, _branch)| ledger)
        .unwrap_or_else(|_| record.alias.clone());

    // Use canonical form for @id: "{ledger_name}:{branch}"
    let canonical_id = core_alias::format_alias(&ledger_name, &record.branch);

    // Reflect retracted state in status
    let status = if record.retracted { "retracted" } else { "ready" };

    let mut obj = json!({
        "@context": { "f": "https://ns.flur.ee/ledger#" },
        "@id": &canonical_id,
        "@type": ["f:Database", "f:PhysicalDatabase"],
        "f:ledger": { "@id": &ledger_name },
        "f:branch": &record.branch,
        "f:t": record.commit_t,
        "f:status": status,
    });

    if let Some(ref commit_addr) = record.commit_address {
        obj["f:commit"] = json!({ "@id": commit_addr });
    }
    if let Some(ref index_addr) = record.index_address {
        obj["f:index"] = json!({
            "@id": index_addr,
            "f:t": record.index_t
        });
    }
    if let Some(ref ctx_addr) = record.default_context_address {
        obj["f:defaultContext"] = json!({ "@id": ctx_addr });
    }

    obj
}

/// Convert VgNsRecord to JSON-LD format for nameservice queries.
///
/// Uses standard JSON-LD keywords with both `f:` (ledger) and `fidx:` (index) namespaces.
/// Includes `f:status` field that reflects retracted state.
pub fn vg_record_to_jsonld(record: &VgNsRecord) -> JsonValue {
    // Use canonical form for @id: "{name}:{branch}"
    let canonical_id = core_alias::format_alias(&record.name, &record.branch);

    // Reflect retracted state in status
    let status = if record.retracted { "retracted" } else { "ready" };

    let mut obj = json!({
        "@context": {
            "f": "https://ns.flur.ee/ledger#",
            "fidx": "https://ns.flur.ee/index#"
        },
        "@id": &canonical_id,
        "@type": ["f:VirtualGraphDatabase", record.vg_type.to_type_string()],
        "f:name": &record.name,
        "f:branch": &record.branch,
        "f:status": status,
        "fidx:config": { "@value": &record.config },
        "fidx:dependencies": &record.dependencies,
    });

    // Include index fields if present (matching ns@v2 on-disk format)
    if let Some(ref index_addr) = record.index_address {
        obj["fidx:indexAddress"] = json!(index_addr);
        obj["fidx:indexT"] = json!(record.index_t);
    }

    obj
}

/// Build stats section with decoded IRIs and hierarchy fields.
fn build_stats<S>(
    ledger: &LedgerState<S>,
    stats: &IndexStats,
    compactor: &IriCompactor,
    schema_index: &SchemaIndex,
) -> Result<JsonValue>
where
    S: Storage + Clone + 'static,
{
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
        "properties": decode_property_stats(&stats.properties, compactor, schema_index)?,
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
        let iri = compactor.decode_sid(&entry.class_sid).map_err(|e| match e {
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

        // Decode class→property list (no per-property breakdowns here; those live in graph stats)
        let mut props_arr: Vec<JsonValue> = Vec::new();
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
            props_arr.push(json!(prop_compacted));
        }
        class_obj.insert("properties".to_string(), JsonValue::Array(props_arr));

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
            .ok_or_else(|| "missing g_id".to_string())? as u32;
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
/// Each entry is keyed by numeric `g_id` and `p_id`. The `DatatypeId` values
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
                    // Resolve DatatypeId to compact IRI via Display
                    let mut dt_obj = Map::new();
                    for &(dt_raw, count) in &p.datatypes {
                        let dt_iri = DatatypeId::from_u8(dt_raw).to_string();
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
    alias: String,
    context: Option<&'a JsonValue>,
}

impl<'a, S, N> LedgerInfoBuilder<'a, S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Clone + Send + Sync + 'static,
{
    /// Create a new builder (called by `Fluree::ledger_info()`).
    pub(crate) fn new(fluree: &'a Fluree<S, N>, alias: String) -> Self {
        Self {
            fluree,
            alias,
            context: None,
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

    /// Execute the ledger info request.
    ///
    /// Loads the ledger (using cache if available) and returns comprehensive
    /// metadata including commit info, nameservice record, stats,
    /// and index information.
    pub async fn execute(self) -> crate::Result<JsonValue> {
        // Load the ledger (uses cache if caching is enabled)
        let ledger = self.fluree.ledger(&self.alias).await?;

        // Build and return the ledger info
        build_ledger_info(&ledger, self.context)
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
        let record = NsRecord {
            address: "mydb:main".to_string(),
            alias: "mydb:main".to_string(),
            branch: "main".to_string(),
            commit_address: Some("fluree:file://mydb/main/commit/abc.json".to_string()),
            commit_t: 42,
            index_address: Some("fluree:file://mydb/main/index/def.json".to_string()),
            index_t: 40,
            default_context_address: None,
            retracted: false,
        };

        let json = ns_record_to_jsonld(&record);

        assert_eq!(json["@id"], "mydb:main");
        assert_eq!(json["@type"], json!(["f:Database", "f:PhysicalDatabase"]));
        assert_eq!(json["f:ledger"]["@id"], "mydb");
        assert_eq!(json["f:branch"], "main");
        assert_eq!(json["f:t"], 42);
        assert_eq!(json["f:status"], "ready");
        assert_eq!(
            json["f:commit"]["@id"],
            "fluree:file://mydb/main/commit/abc.json"
        );
        assert_eq!(json["f:index"]["f:t"], 40);
    }

    #[test]
    fn test_ns_record_to_jsonld_retracted() {
        let record = NsRecord {
            address: "mydb:main".to_string(),
            alias: "mydb:main".to_string(),
            branch: "main".to_string(),
            commit_address: Some("commit-addr".to_string()),
            commit_t: 10,
            index_address: None,
            index_t: 0,
            default_context_address: None,
            retracted: true,
        };

        let json = ns_record_to_jsonld(&record);
        assert_eq!(json["f:status"], "retracted");
    }

    #[test]
    fn test_vg_record_to_jsonld() {
        let record = VgNsRecord {
            address: "my-search:main".to_string(),
            name: "my-search".to_string(),
            branch: "main".to_string(),
            vg_type: fluree_db_nameservice::VgType::Bm25,
            config: r#"{"k1":1.2,"b":0.75}"#.to_string(),
            dependencies: vec!["source-ledger:main".to_string()],
            index_address: Some("fluree:file://vg/snapshot.bin".to_string()),
            index_t: 42,
            retracted: false,
        };

        let json = vg_record_to_jsonld(&record);

        assert_eq!(json["@id"], "my-search:main");
        assert_eq!(json["@type"], json!(["f:VirtualGraphDatabase", "fidx:BM25"]));
        assert_eq!(json["f:name"], "my-search");
        assert_eq!(json["f:branch"], "main");
        assert_eq!(json["f:status"], "ready");
        assert_eq!(json["fidx:config"]["@value"], r#"{"k1":1.2,"b":0.75}"#);
        assert_eq!(json["fidx:dependencies"], json!(["source-ledger:main"]));
        assert_eq!(json["fidx:indexAddress"], "fluree:file://vg/snapshot.bin");
        assert_eq!(json["fidx:indexT"], 42);
    }
}
