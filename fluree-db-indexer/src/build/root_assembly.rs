//! Root encode, CAS write, garbage chain, and IndexResult derivation.
//!
//! Both the full-rebuild and incremental pipelines end by encoding an
//! `IndexRoot` or `IndexRoot`, optionally attaching a garbage manifest,
//! writing the root to CAS, and deriving an `IndexResult`. This module
//! provides shared helpers to avoid duplicating that logic.

use fluree_db_binary_index::format::index_root::{DefaultGraphOrder, IndexRoot};
use fluree_db_binary_index::{BinaryGarbageRef, BinaryPrevIndexRef, DictRefs, GraphArenaRefs};
use fluree_db_core::{ContentId, ContentKind, Storage};
use std::collections::BTreeMap;

use super::types::{UploadedDicts, UploadedIndexes};

use crate::error::{IndexerError, Result};
use crate::gc;
use crate::{IndexResult, IndexStats};

/// Context for linking the GC chain to a previous index root.
///
/// Used by both pipelines, but computed differently:
/// - **Rebuild**: loads the old root from CAS, computes `all_cas_ids()` set
///   difference to find garbage CIDs.
/// - **Incremental**: `IncrementalRootBuilder` tracks replaced CIDs explicitly.
// Kept for: shared root finalization for both rebuild and incremental pipelines.
// Use when: rebuild.rs Phase F is refactored to use encode_and_write_root().
pub(crate) struct GarbageContext {
    /// CIDs that should be recorded as garbage (replaced by this new root).
    pub garbage_cids: Vec<ContentId>,
    /// Previous root linkage (for GC chain traversal).
    pub prev_index: Option<BinaryPrevIndexRef>,
}

/// Encode an `IndexRoot`, attach garbage/prev_index, write to CAS,
/// and return an `IndexResult`.
///
/// This is the shared "last mile" for both rebuild and incremental pipelines.
// Kept for: shared root finalization for both rebuild and incremental pipelines.
// Use when: rebuild.rs Phase F is refactored to use this shared helper.
#[expect(dead_code)]
pub(crate) async fn encode_and_write_root<S: Storage>(
    storage: &S,
    ledger_id: &str,
    mut root: IndexRoot,
    garbage_ctx: Option<GarbageContext>,
    result_stats: IndexStats,
) -> Result<IndexResult> {
    // Attach garbage manifest and prev_index if provided.
    if let Some(ctx) = garbage_ctx {
        if let Some(prev) = ctx.prev_index {
            root.prev_index = Some(prev);
        }

        if !ctx.garbage_cids.is_empty() {
            let garbage_strings: Vec<String> =
                ctx.garbage_cids.iter().map(|c| c.to_string()).collect();
            root.garbage =
                gc::write_garbage_record(storage, ledger_id, root.index_t, garbage_strings)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?
                    .map(|id| BinaryGarbageRef { id });

            tracing::info!(
                garbage_count = ctx.garbage_cids.len(),
                "GC chain: garbage record written"
            );
        }
    }

    tracing::info!(
        index_t = root.index_t,
        default_orders = root.default_graph_orders.len(),
        named_graphs = root.named_graphs.len(),
        "encoding and writing FIR6 root to CAS"
    );

    // Encode and write root.
    let root_bytes = root.encode();
    let write_result = storage
        .content_write_bytes(ContentKind::IndexRoot, ledger_id, &root_bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

    // Derive ContentId from the root's content hash.
    let root_id = ContentId::from_hex_digest(
        fluree_db_core::CODEC_FLUREE_INDEX_ROOT,
        &write_result.content_hash,
    )
    .ok_or_else(|| {
        IndexerError::StorageWrite(format!(
            "invalid content_hash from write result: {}",
            write_result.content_hash
        ))
    })?;

    tracing::info!(
        %root_id,
        index_t = root.index_t,
        root_bytes = root_bytes.len(),
        "index root published"
    );

    Ok(IndexResult {
        root_id,
        index_t: root.index_t,
        ledger_id: ledger_id.to_string(),
        stats: IndexStats {
            total_bytes: root_bytes.len(),
            ..result_stats
        },
    })
}

/// Compute garbage CIDs by comparing old root's CAS IDs with new root's CAS IDs.
///
/// Used by the full-rebuild pipeline which has access to the previous root via CAS.
// Kept for: shared GC chain computation for both rebuild and incremental pipelines.
// Use when: rebuild.rs Phase F.7 is refactored to use this shared helper.
#[expect(dead_code)]
pub(crate) async fn compute_garbage_from_prev_root(
    content_store: &dyn fluree_db_core::storage::ContentStore,
    new_root: &IndexRoot,
    prev_root_id: &ContentId,
) -> Option<GarbageContext> {
    let prev_bytes = content_store.get(prev_root_id).await.ok()?;
    let prev_root = IndexRoot::decode(&prev_bytes).ok()?;

    let prev_t = prev_root.index_t;
    let old_ids: std::collections::HashSet<ContentId> =
        prev_root.all_cas_ids().into_iter().collect();
    let new_ids: std::collections::HashSet<ContentId> =
        new_root.all_cas_ids().into_iter().collect();
    let garbage_cids: Vec<ContentId> = old_ids.difference(&new_ids).cloned().collect();

    Some(GarbageContext {
        garbage_cids,
        prev_index: Some(BinaryPrevIndexRef {
            t: prev_t,
            id: prev_root_id.clone(),
        }),
    })
}

// ============================================================================
// V6 (FIR6) root assembly
// ============================================================================

/// Inputs for assembling a V6 (FIR6) index root.
///
/// Collects all the pieces produced by the build pipeline (dicts, V3 indexes,
/// namespace codes, predicate SIDs) into a single struct for the root encoder.
pub(crate) struct Fir6Inputs {
    pub ledger_id: String,
    pub index_t: i64,
    pub namespace_codes: BTreeMap<u16, String>,
    /// Commit-derived namespace table for Rule 5 reconciliation.
    /// If provided, `encode_and_write_root_v6` validates that the index root's
    /// `namespace_codes` matches the commit-derived table entry-by-entry.
    /// A mismatch indicates an indexer/publisher bug.
    pub commit_derived_ns: Option<std::collections::HashMap<u16, String>>,
    pub predicate_sids: Vec<(u16, String)>,
    pub uploaded_dicts: UploadedDicts,
    pub v3_uploaded: UploadedIndexes,
    pub graph_arenas: Vec<GraphArenaRefs>,
    pub datatype_iris: Vec<String>,
    pub language_tags: Vec<String>,
    pub total_commit_size: u64,
    pub total_asserts: u64,
    pub total_retracts: u64,
    /// Full query-time stats (HLL-derived cardinalities, per-graph properties).
    /// `None` if stats collection was skipped or deferred.
    pub db_stats: Option<fluree_db_core::index_stats::IndexStats>,
    /// Schema hierarchy (rdfs:subClassOf / rdfs:subPropertyOf).
    pub db_schema: Option<fluree_db_core::IndexSchema>,
    /// CAS reference for the serialized HLL sketch blob.
    pub sketch_ref: Option<ContentId>,
}

/// Encode an `IndexRoot` (FIR6), write to CAS, and return an `IndexResult`.
///
/// This is the V3 equivalent of the V5 root assembly. It constructs the
/// `IndexRoot`, encodes it, writes to CAS with `ContentKind::IndexRoot`,
/// and derives the CID.
///
/// `gc_ctx` is `None` for this milestone (V3 GC chain is deferred).
pub(crate) async fn encode_and_write_root_v6<S: Storage>(
    storage: &S,
    inputs: Fir6Inputs,
    gc_ctx: Option<GarbageContext>,
    result_stats: IndexStats,
) -> Result<IndexResult> {
    // Rule 5 reconciliation at publish time:
    // If a commit-derived namespace table is provided for this `index_t`,
    // it must match the index root's materialized table exactly.
    if let Some(ref commit_ns) = inputs.commit_derived_ns {
        let commit_bt: BTreeMap<u16, String> = commit_ns
            .iter()
            .map(|(&k, v)| (k, v.clone()))
            .collect();
        if commit_bt != inputs.namespace_codes {
            // Find a representative mismatch for a targeted error message.
            let mut mismatch: Option<(u16, Option<&String>, Option<&String>)> = None;
            for (code, commit_prefix) in &commit_bt {
                match inputs.namespace_codes.get(code) {
                    Some(root_prefix) if root_prefix == commit_prefix => {}
                    other => {
                        mismatch = Some((*code, Some(commit_prefix), other));
                        break;
                    }
                }
            }
            if mismatch.is_none() {
                for (code, root_prefix) in &inputs.namespace_codes {
                    if !commit_bt.contains_key(code) {
                        mismatch = Some((*code, None, Some(root_prefix)));
                        break;
                    }
                }
            }

            let detail = if let Some((code, commit_p, root_p)) = mismatch {
                format!(
                    "example mismatch: code {code} commit={:?} root={:?}",
                    commit_p, root_p
                )
            } else {
                "mismatch: tables differ".to_string()
            };

            return Err(IndexerError::Core(fluree_db_core::Error::invalid_index(
                format!(
                    "Rule 5 violation at index publish (index_t={}): index root namespace_codes does not match \
                     commit-derived table — indexer/publisher bug ({detail})",
                    inputs.index_t
                ),
            )));
        }
    }

    // Convert DictRefs for root assembly.
    let dr = inputs.uploaded_dicts.dict_refs;
    let dict_refs = DictRefs {
        forward_packs: dr.forward_packs,
        subject_reverse: dr.subject_reverse,
        string_reverse: dr.string_reverse,
    };

    // Build default_graph_orders from V3 upload result.
    let default_graph_orders: Vec<DefaultGraphOrder> = inputs
        .v3_uploaded
        .default_graph_orders
        .into_iter()
        .map(|(order, leaves)| DefaultGraphOrder { order, leaves })
        .collect();

    // Custom datatype IRIs (non-reserved only, for o_type table).
    let custom_dt_iris: Vec<String> = inputs
        .datatype_iris
        .iter()
        .skip(fluree_db_core::DatatypeDictId::RESERVED_COUNT as usize)
        .cloned()
        .collect();

    let mut root = IndexRoot {
        ledger_id: inputs.ledger_id.clone(),
        index_t: inputs.index_t,
        base_t: 0,
        subject_id_encoding: inputs.uploaded_dicts.subject_id_encoding,
        namespace_codes: inputs.namespace_codes,
        predicate_sids: inputs.predicate_sids,
        graph_iris: inputs.uploaded_dicts.graph_iris,
        datatype_iris: inputs.datatype_iris,
        language_tags: inputs.language_tags.clone(),
        dict_refs,
        subject_watermarks: inputs.uploaded_dicts.subject_watermarks,
        string_watermark: inputs.uploaded_dicts.string_watermark,
        lex_sorted_string_ids: false,
        total_commit_size: inputs.total_commit_size,
        total_asserts: inputs.total_asserts,
        total_retracts: inputs.total_retracts,
        graph_arenas: inputs.graph_arenas,
        o_type_table: IndexRoot::build_o_type_table(&custom_dt_iris, &inputs.language_tags),
        default_graph_orders,
        named_graphs: inputs.v3_uploaded.named_graphs,
        stats: inputs.db_stats,
        schema: inputs.db_schema,
        prev_index: None,
        garbage: None,
        sketch_ref: inputs.sketch_ref,
    };

    // `IndexStats.size` is defined as total commit data size (bytes) for the ledger.
    // The root carries this as `total_commit_size`; ensure stats reflect it.
    if let Some(stats) = root.stats.as_mut() {
        stats.size = root.total_commit_size;

        // Populate per-graph `stats.graphs[*].size` as a proportional allocation of
        // total commit size based on each graph's flake count.
        //
        // This is an estimate (not exact storage bytes), but it avoids reporting 0
        // and remains consistent across rebuild/incremental paths.
        if let Some(graphs) = stats.graphs.as_mut() {
            let total_flakes: u64 = graphs.iter().map(|g| g.flakes).sum();
            if total_flakes > 0 && stats.size > 0 {
                let total_size = stats.size;
                let n = graphs.len();
                let mut assigned: u64 = 0;
                for (i, g) in graphs.iter_mut().enumerate() {
                    if i + 1 == n {
                        // Last graph gets remainder so sums match exactly.
                        g.size = total_size.saturating_sub(assigned);
                    } else {
                        let part = ((total_size as u128) * (g.flakes as u128)
                            / (total_flakes as u128)) as u64;
                        g.size = part;
                        assigned = assigned.saturating_add(part);
                    }
                }
            }
        }
    }

    // Attach garbage manifest and prev_index if provided.
    if let Some(ctx) = gc_ctx {
        if let Some(prev) = ctx.prev_index {
            root.prev_index = Some(prev);
        }

        if !ctx.garbage_cids.is_empty() {
            let garbage_strings: Vec<String> =
                ctx.garbage_cids.iter().map(|c| c.to_string()).collect();
            root.garbage = gc::write_garbage_record(
                storage,
                &inputs.ledger_id,
                inputs.index_t,
                garbage_strings,
            )
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?
            .map(|id| BinaryGarbageRef { id });

            tracing::info!(
                garbage_count = ctx.garbage_cids.len(),
                "GC chain: garbage record written"
            );
        }
    }

    tracing::info!(
        index_t = root.index_t,
        o_type_entries = root.o_type_table.len(),
        default_orders = root.default_graph_orders.len(),
        named_graphs = root.named_graphs.len(),
        "encoding and writing FIR6 root to CAS"
    );

    // Encode and write root.
    let root_bytes = root.encode();
    let write_result = storage
        .content_write_bytes(ContentKind::IndexRoot, &inputs.ledger_id, &root_bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
    let root_id = ContentId::from_hex_digest(
        fluree_db_core::content_kind::CODEC_FLUREE_INDEX_ROOT,
        &write_result.content_hash,
    )
    .ok_or_else(|| {
        IndexerError::StorageWrite(format!(
            "invalid root digest for FIR6: {}",
            write_result.content_hash
        ))
    })?;

    tracing::info!(
        %root_id,
        index_t = root.index_t,
        root_bytes = root_bytes.len(),
        "FIR6 index root published"
    );

    Ok(IndexResult {
        root_id,
        index_t: root.index_t,
        ledger_id: inputs.ledger_id,
        stats: IndexStats {
            total_bytes: root_bytes.len(),
            ..result_stats
        },
    })
}
