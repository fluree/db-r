//! Consolidated fast-path COUNT operators.
//!
//! This module groups the `fast_count_*` family into one place to reduce sprawl.
//! All operators here emit a single-row count batch via `FastPathOperator`
//! when `fast_path_store(ctx)` is available, otherwise they fall back to a planned
//! operator tree for correctness.

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, count_rows_for_predicate_psot, fast_path_store, leaf_entries_for_predicate,
    normalize_pred_sid, projection_okey_only, projection_otype_okey, projection_otype_only,
    projection_sid_only, FastPathOperator,
};
use crate::ir::{Expression, Function};
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

// ---------------------------------------------------------------------------
// 5) COUNT(*) with `FILTER REGEX(?o, "^prefix")` on a bound predicate.
// ---------------------------------------------------------------------------

/// Fast-path: `SELECT (COUNT(*) AS ?c) WHERE { ?s <p> ?o FILTER REGEX(?o, "^prefix") }`
///
/// Uses the reverse string dictionary to enumerate matching string IDs by prefix,
/// then counts occurrences by seeking the POST index for each matching string ID.
///
/// Falls back when:
/// - the predicate's object type is not homogeneous (o_type_const missing or mixed)
/// - the homogeneous o_type is not string-dict-backed
// Kept for: regex-prefix COUNT fast-path detection in operator_tree.rs.
// Use when: detect_regex_anchored_prefix is wired into operator tree detection.
#[expect(dead_code)]
pub(crate) fn regex_prefix_count_operator(
    predicate: Ref,
    prefix: String,
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

            // Determine (small) set of string-backed o_types observed for this predicate in POST order.
            let Some(o_types) = string_otypes_for_predicate_post(store, ctx.binary_g_id, p_id)?
            else {
                return Ok(None); // Can't guarantee correctness — fall through to planned pipeline.
            };

            let ids = store
                .find_strings_by_prefix(&prefix)
                .map_err(|e| QueryError::Internal(format!("string prefix scan: {e}")))?;
            tracing::debug!(
                predicate_p_id = p_id,
                prefix = %prefix,
                string_ids = ids.len(),
                "regex-prefix fast-path: enumerated string ids"
            );
            if ids.is_empty() {
                return Ok(Some(build_count_batch(out_var, 0)?));
            }

            tracing::debug!(
                predicate_p_id = p_id,
                o_types = ?o_types,
                "regex-prefix fast-path: candidate string o_types"
            );
            let mut total: u64 = 0;
            for o_type in &o_types {
                for id in &ids {
                    total = total.saturating_add(count_predicate_okey_post(
                        store,
                        ctx.binary_g_id,
                        p_id,
                        *o_type,
                        *id as u64,
                    )?);
                }
            }

            tracing::debug!(
                predicate_p_id = p_id,
                prefix = %prefix,
                total,
                "regex-prefix fast-path: computed total rows"
            );
            let total_i64 = i64::try_from(total).map_err(|_| {
                QueryError::execution("COUNT exceeds i64 in regex-prefix fast-path")
            })?;
            Ok(Some(build_count_batch(out_var, total_i64)?))
        },
        fallback,
        "regex-prefix COUNT",
    )
}

fn string_otypes_for_predicate_post(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
) -> Result<Option<Vec<u16>>> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);
    if leaves.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let mut set: std::collections::BTreeSet<u16> = std::collections::BTreeSet::new();
    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();
        for entry in &dir.entries {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            let first = read_ordered_key_v2(RunSortOrder::Post, &entry.first_key);
            let last = read_ordered_key_v2(RunSortOrder::Post, &entry.last_key);
            for ot in [first.o_type, last.o_type] {
                if OType::from_u16(ot).decode_kind()
                    == fluree_db_core::o_type::DecodeKind::StringDict
                {
                    set.insert(ot);
                    if set.len() > 32 {
                        return Ok(None);
                    }
                }
            }
        }
    }
    Ok(Some(set.into_iter().collect()))
}

fn count_predicate_okey_post(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
    target_o_type: u16,
    target_o_key: u64,
) -> Result<u64> {
    let branch = store
        .branch_for_order(g_id, RunSortOrder::Post)
        .ok_or_else(|| QueryError::Internal("no POST branch for graph".to_string()))?;
    let cmp = cmp_v2_for_order(RunSortOrder::Post);

    let min_key = RunRecordV2 {
        s_id: SubjectId(0),
        o_key: target_o_key,
        p_id,
        t: 0,
        o_i: 0,
        o_type: target_o_type,
        g_id,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId(u64::MAX),
        o_key: target_o_key,
        p_id,
        t: u32::MAX,
        o_i: u32::MAX,
        o_type: target_o_type,
        g_id,
    };
    let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);

    let target_prefix = (target_o_type, target_o_key);
    let mut total: u64 = 0;
    let projection_ok = projection_okey_only();
    let projection_ot_ok = projection_otype_okey();

    for leaf_idx in leaf_range.clone() {
        let leaf_entry = &branch.leaves[leaf_idx];
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();

        for (i, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            let rec = read_ordered_key_v2(RunSortOrder::Post, &entry.first_key);
            let prefix = (rec.o_type, rec.o_key);
            if prefix < target_prefix {
                continue;
            }
            if prefix > target_prefix {
                break;
            }

            // Boundary-equality: check the next leaflet's first prefix.
            let next_prefix = if i + 1 < dir.entries.len() {
                let r = read_ordered_key_v2(RunSortOrder::Post, &dir.entries[i + 1].first_key);
                Some((r.o_type, r.o_key))
            } else if leaf_idx + 1 < leaf_range.end {
                let next_entry = &branch.leaves[leaf_idx + 1].first_key;
                Some((next_entry.o_type, next_entry.o_key))
            } else {
                None
            };

            if next_prefix == Some(target_prefix) {
                total += entry.row_count as u64;
                continue;
            }

            // Decode minimal columns and count exact rows for this prefix within the leaflet.
            if entry.o_type_const == Some(target_o_type) {
                let batch = handle
                    .load_columns(i, &projection_ok, RunSortOrder::Post)
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
                for row in 0..batch.row_count {
                    if batch.o_key.get(row) == target_o_key {
                        total += 1;
                    }
                }
            } else {
                let batch = handle
                    .load_columns(i, &projection_ot_ok, RunSortOrder::Post)
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
                for row in 0..batch.row_count {
                    if batch.o_type.get_or(row, 0) == target_o_type
                        && batch.o_key.get(row) == target_o_key
                    {
                        total += 1;
                    }
                }
            }
        }
    }

    Ok(total)
}

/// Detection helper: extract a constant anchored prefix from `REGEX(?var, "^prefix")`.
pub fn detect_regex_anchored_prefix(expr: &Expression, object_var: VarId) -> Option<String> {
    let Expression::Call {
        func: Function::Regex,
        args,
    } = expr
    else {
        return None;
    };
    if args.len() < 2 || args.len() > 3 {
        return None;
    }
    if args[0] != Expression::Var(object_var) {
        return None;
    }
    let Expression::Const(crate::ir::FilterValue::String(pat)) = &args[1] else {
        return None;
    };
    // Only support no flags or empty flags.
    if args.len() == 3 {
        let Expression::Const(crate::ir::FilterValue::String(flags)) = &args[2] else {
            return None;
        };
        if !flags.is_empty() {
            return None;
        }
    }
    let s = pat.as_str();
    let prefix = s.strip_prefix('^')?;
    if prefix.is_empty() {
        return None;
    }
    // Reject regex metacharacters in the remainder (simple literal prefix only).
    if prefix.bytes().any(|b| {
        matches!(
            b,
            b'.' | b'+'
                | b'*'
                | b'?'
                | b'['
                | b']'
                | b'('
                | b')'
                | b'{'
                | b'}'
                | b'|'
                | b'\\'
                | b'$'
        )
    }) {
        return None;
    }
    Some(prefix.to_string())
}
