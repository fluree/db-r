//! Consolidated fast-path COUNT operators.
//!
//! This module groups the `fast_count_*` family into one place to reduce sprawl.
//! All operators here emit a single-row count batch via `FastPathOperator`
//! when `fast_path_store(ctx)` is available, otherwise they fall back to a planned
//! operator tree for correctness.

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, count_rows_for_predicate_psot, fast_path_store, leaf_entries_for_predicate,
    normalize_pred_sid, projection_okey_only, projection_otype_only, projection_sid_only,
    FastPathOperator,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::{
    cmp_v2_for_order, read_ordered_key_v2, RunRecordV2,
};
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::GraphId;
use fluree_vocab::namespaces;

// ---------------------------------------------------------------------------
// 1) COUNT(*) / COUNT(?x) for single predicate `?s <p> ?o`
// ---------------------------------------------------------------------------

/// Fast-path: `COUNT(*)` / `COUNT(?x)` for a single triple `?s <p> ?o`.
pub fn count_rows_operator(
    predicate: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let pred_sid = normalize_pred_sid(store, &predicate)?;
            let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                return Ok(Some(build_count_batch(out_var, 0)?));
            };
            let count = count_rows_for_predicate_psot(store, ctx.binary_g_id, p_id)?;
            Ok(Some(build_count_batch(out_var, count as i64)?))
        },
        fallback,
        "COUNT rows",
    )
}

// ---------------------------------------------------------------------------
// 2) COUNT(DISTINCT ?o) for single predicate `?s <p> ?o` (POST scan, encoded IDs)
// ---------------------------------------------------------------------------

/// Fast-path fused scan + COUNT(DISTINCT ?o) for a single predicate.
pub fn count_distinct_object_operator(
    predicate: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            match count_distinct_object_post(store, ctx.binary_g_id, &predicate)? {
                Some(count) => Ok(Some(build_count_batch(out_var, count as i64)?)),
                None => Ok(None), // Unsupported at runtime — fall through to planned pipeline.
            }
        },
        fallback,
        "COUNT(DISTINCT)",
    )
}

/// COUNT DISTINCT objects for a bound predicate by scanning POST.
///
/// Returns `None` when the fast-path cannot guarantee correctness (e.g., mixed o_type).
fn count_distinct_object_post(
    store: &BinaryIndexStore,
    g_id: GraphId,
    predicate: &Ref,
) -> Result<Option<u64>> {
    let pred_sid = normalize_pred_sid(store, predicate)?;
    let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
        // Predicate not present in the persisted dict — empty result.
        return Ok(Some(0));
    };

    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);

    // For now: only handle the common case where the object is an IRI ref (e.g., rdf:type).
    // This avoids all dictionary decoding and is already a huge win for DBLP.
    let required_o_type = OType::IRI_REF.as_u16();

    let projection = projection_okey_only();

    let mut prev_okey: Option<u64> = None;
    let mut distinct: u64 = 0;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;

        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 {
                continue;
            }
            if entry.p_const != Some(p_id) {
                continue;
            }
            // Require o_type_const and require it to be IRI_REF for now.
            if entry.o_type_const != Some(required_o_type) {
                return Ok(None);
            }

            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Post)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;

            for row in 0..batch.row_count {
                let okey = batch.o_key.get(row);
                if prev_okey != Some(okey) {
                    distinct += 1;
                    prev_okey = Some(okey);
                }
            }
        }
    }

    Ok(Some(distinct))
}

// ---------------------------------------------------------------------------
// 3) COUNT(*) / COUNT(?x) for `?s ?p ?o` and COUNT(DISTINCT ?lead)
// ---------------------------------------------------------------------------

/// Fast-path: count total triples across all patterns.
pub fn count_triples_operator(out_var: VarId, fallback: Option<BoxedOperator>) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let count = count_triples_from_branch_manifest(store, ctx.binary_g_id)?;
            let count_i64 = i64::try_from(count)
                .map_err(|_| QueryError::execution("COUNT exceeds i64 in triples fast-path"))?;
            Ok(Some(build_count_batch(out_var, count_i64)?))
        },
        fallback,
        "triples COUNT",
    )
}

fn count_triples_from_branch_manifest(store: &BinaryIndexStore, g_id: GraphId) -> Result<u64> {
    // Any permutation's leaf `row_count` sums to the total number of triples.
    // Prefer PSOT (commonly present and predicate-segmented).
    let order_preference = [
        RunSortOrder::Psot,
        RunSortOrder::Spot,
        RunSortOrder::Post,
        RunSortOrder::Opst,
    ];
    for order in order_preference {
        if let Some(branch) = store.branch_for_order(g_id, order) {
            return Ok(branch.leaves.iter().map(|l| l.row_count).sum());
        }
    }
    Ok(0)
}

/// Fast-path: count distinct subjects across all triples.
pub fn count_distinct_subjects_operator(
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            // SPOT key layout: s_id(8) + p_id(4) + o_type(2) + o_key(8) + o_i(4).
            // Distinct subjects = lead bytes [0..8].
            let count = count_distinct_lead_groups(store, ctx.binary_g_id, RunSortOrder::Spot, 8)?;
            let count_i64 = i64::try_from(count).map_err(|_| {
                QueryError::execution("COUNT(DISTINCT) exceeds i64 in distinct-subject fast-path")
            })?;
            Ok(Some(build_count_batch(out_var, count_i64)?))
        },
        fallback,
        "distinct subject COUNT",
    )
}

/// Fast-path: count distinct predicates across all triples.
pub fn count_distinct_predicates_operator(
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let count = count_distinct_predicates_psot(store, ctx.binary_g_id)?;
            let count_i64 = i64::try_from(count).map_err(|_| {
                QueryError::execution("COUNT(DISTINCT) exceeds i64 in distinct-predicate fast-path")
            })?;
            Ok(Some(build_count_batch(out_var, count_i64)?))
        },
        fallback,
        "distinct predicate COUNT",
    )
}

/// Fast-path: count distinct objects across all triples.
pub fn count_distinct_objects_operator(
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            // OPST key layout: o_type(2) + o_key(8) + o_i(4) + p_id(4) + s_id(8).
            // Distinct objects = lead bytes [0..10].
            let count = count_distinct_lead_groups(store, ctx.binary_g_id, RunSortOrder::Opst, 10)?;
            let count_i64 = i64::try_from(count).map_err(|_| {
                QueryError::execution("COUNT(DISTINCT) exceeds i64 in distinct-object fast-path")
            })?;
            Ok(Some(build_count_batch(out_var, count_i64)?))
        },
        fallback,
        "distinct object COUNT",
    )
}

/// Count distinct lead groups across all leaflets in a given sort order.
///
/// Uses `lead_group_count` from leaflet directory entries, deduplicating groups
/// that span leaflet boundaries by comparing lead key prefixes.
///
/// `lead_len` is the number of leading key bytes that define the grouping:
/// - SPOT distinct subjects: 8 bytes (s_id)
/// - OPST distinct objects: 10 bytes (o_type + o_key)
fn count_distinct_lead_groups(
    store: &BinaryIndexStore,
    g_id: GraphId,
    order: RunSortOrder,
    lead_len: usize,
) -> Result<u64> {
    let Some(branch) = store.branch_for_order(g_id, order) else {
        return Ok(0);
    };

    let mut prev_lead_last: Vec<u8> = Vec::new();
    let mut total: u64 = 0;

    for leaf_entry in &branch.leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();

        for entry in &dir.entries {
            if entry.row_count == 0 || entry.lead_group_count == 0 {
                continue;
            }

            let lead_first = &entry.first_key[..lead_len];
            let lead_last = &entry.last_key[..lead_len];

            total += u64::from(entry.lead_group_count);
            if !prev_lead_last.is_empty() && prev_lead_last == lead_first {
                total = total.saturating_sub(1);
            }
            prev_lead_last.clear();
            prev_lead_last.extend_from_slice(lead_last);
        }
    }

    Ok(total)
}

/// Distinct predicates uses p_const metadata rather than lead_group_count,
/// since PSOT leaflets are predicate-homogeneous.
fn count_distinct_predicates_psot(store: &BinaryIndexStore, g_id: GraphId) -> Result<u64> {
    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Psot) else {
        return Ok(0);
    };

    let mut prev_p: Option<u32> = None;
    let mut total: u64 = 0;

    for leaf_entry in &branch.leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();

        for entry in &dir.entries {
            if entry.row_count == 0 {
                continue;
            }

            let p_id = entry.p_const.unwrap_or_else(|| {
                let bytes: [u8; 4] = entry.first_key[0..4].try_into().unwrap();
                u32::from_be_bytes(bytes)
            });

            if prev_p != Some(p_id) {
                total += 1;
                prev_p = Some(p_id);
            }
        }
    }

    Ok(total)
}

// ---------------------------------------------------------------------------
// 4) Specialized global counts: literals and blank-node subjects
// ---------------------------------------------------------------------------

/// Fast-path: count triples with literal objects.
pub fn count_literal_objects_operator(
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let count = count_literal_rows_psot(store, ctx.binary_g_id)?;
            let count_i64 = i64::try_from(count)
                .map_err(|_| QueryError::execution("COUNT exceeds i64 in literal fast-path"))?;
            Ok(Some(build_count_batch(out_var, count_i64)?))
        },
        fallback,
        "literal COUNT",
    )
}

fn is_literal_otype(ot_u16: u16) -> bool {
    let ot = OType::from_u16(ot_u16);
    !ot.is_node_ref()
}

fn count_literal_rows_psot(store: &BinaryIndexStore, g_id: GraphId) -> Result<u64> {
    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Psot) else {
        return Ok(0);
    };
    let projection = projection_otype_only();
    let mut total: u64 = 0;

    for leaf_entry in &branch.leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 {
                continue;
            }
            if let Some(ot) = entry.o_type_const {
                if is_literal_otype(ot) {
                    total += entry.row_count as u64;
                }
                continue;
            }

            // Mixed types: decode OType column only.
            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Psot)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            for row in 0..batch.row_count {
                if is_literal_otype(batch.o_type.get(row)) {
                    total += 1;
                }
            }
        }
    }

    Ok(total)
}

/// Fast-path: count triples with blank-node subjects.
pub fn count_blank_node_subjects_operator(
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let count = count_blank_subject_rows_spot(store, ctx.binary_g_id)?;
            let count_i64 = i64::try_from(count)
                .map_err(|_| QueryError::execution("COUNT exceeds i64 in blank-node fast-path"))?;
            Ok(Some(build_count_batch(out_var, count_i64)?))
        },
        fallback,
        "blank-node COUNT",
    )
}

fn blank_subject_range() -> (u64, u64) {
    let ns = namespaces::BLANK_NODE;
    let min = SubjectId::new(ns, 0).as_u64();
    let max = SubjectId::new(ns, 0x0000_FFFF_FFFF_FFFF).as_u64();
    (min, max)
}

fn count_blank_subject_rows_spot(store: &BinaryIndexStore, g_id: GraphId) -> Result<u64> {
    let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Spot) else {
        return Ok(0);
    };
    let (s_min, s_max) = blank_subject_range();

    let min_key = RunRecordV2 {
        s_id: SubjectId(s_min),
        o_key: 0,
        p_id: 0,
        t: 0,
        o_i: 0,
        o_type: 0,
        g_id,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId(s_max),
        o_key: u64::MAX,
        p_id: u32::MAX,
        t: u32::MAX,
        o_i: u32::MAX,
        o_type: u16::MAX,
        g_id,
    };

    let cmp = cmp_v2_for_order(RunSortOrder::Spot);
    let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);
    let leaves = &branch.leaves[leaf_range];

    let projection = projection_sid_only();
    let mut total: u64 = 0;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 {
                continue;
            }
            let first = read_ordered_key_v2(RunSortOrder::Spot, &entry.first_key);
            let last = read_ordered_key_v2(RunSortOrder::Spot, &entry.last_key);
            let first_s = first.s_id.as_u64();
            let last_s = last.s_id.as_u64();

            if last_s < s_min || first_s > s_max {
                continue;
            }

            if first_s >= s_min && last_s <= s_max {
                total += entry.row_count as u64;
                continue;
            }

            // Boundary leaflet: count exact rows by scanning SId column only.
            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Spot)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            for row in 0..batch.row_count {
                let sid = batch.s_id.get(row);
                if (s_min..=s_max).contains(&sid) {
                    total += 1;
                }
            }
        }
    }

    Ok(total)
}

// (Removed) Regex anchored-prefix COUNT fast path: was parked behind
// `#[expect(dead_code)]` and not wired. If we revisit this, we should implement
// a correctness-first detector + a plan that doesn't require enumerating large
// string-id sets for common prefixes.
