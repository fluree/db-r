//! Fast-path: `COUNT(*)` for an outer triple with a 2-hop `MINUS` chain on the object.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE {
//!   ?a <p_outer> ?b .
//!   MINUS { ?b <p2> ?c . ?c <p3> ?d . }
//! }
//! ```
//!
//! Supported semantics:
//! - Shared var between outer and MINUS is `?b` (the outer object).
//! - MINUS removes an outer solution iff `?b` has at least one `p2` edge to a `?c`
//!   that has at least one `p3` edge.
//!
//! Therefore:
//! - Let `C = { c | c p3 ?d }`
//! - Let `B = { b | exists c in C: b p2 c }`
//! - Answer = `count_{p_outer}(*) - count_{p_outer}(object in B)`
//!
//! Correctness constraints (planner must enforce):
//! - All relevant objects are IRIs (`o_type_const == IRI_REF`) so join keys are stored in `o_key`.

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_set, count_rows_for_predicate_psot,
    fast_path_store, leaf_entries_for_predicate, normalize_pred_sid, projection_okey_only,
    projection_sid_okey, FastPathOperator,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::{BinaryIndexStore, ColumnBatch};
use fluree_db_core::o_type::OType;
use fluree_db_core::GraphId;
use rustc_hash::FxHashSet;

pub fn object_chain_minus_count_all_operator(
    p_outer: Ref,
    p2: Ref,
    p3: Ref,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            match count_object_chain_minus(store, ctx.binary_g_id, &p_outer, &p2, &p3)? {
                Some(count) => {
                    let n_i64 = i64::try_from(count).unwrap_or(i64::MAX);
                    Ok(Some(build_count_batch(out_var, n_i64)?))
                }
                None => Ok(None),
            }
        },
        fallback,
        "object-chain MINUS COUNT(*)",
    )
}

fn collect_subjects_with_object_in_set_psot_sorted(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
    object_subjects: &FxHashSet<u64>,
) -> Result<Option<Vec<u64>>> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
    let projection = projection_sid_okey();

    let mut out: Vec<u64> = Vec::new();

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;

        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            if entry.o_type_const != Some(OType::IRI_REF.as_u16()) {
                return Ok(None);
            }

            let batch: ColumnBatch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Psot)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;

            let mut i = 0usize;
            while i < batch.row_count {
                let b_id = batch.s_id.get(i);
                let mut ok = false;
                while i < batch.row_count && batch.s_id.get(i) == b_id {
                    let c_id = batch.o_key.get(i);
                    if object_subjects.contains(&c_id) {
                        ok = true;
                    }
                    i += 1;
                }
                if ok {
                    out.push(b_id);
                }
            }
        }
    }

    Ok(Some(out))
}

fn sum_post_object_group_counts_filtered(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
    allowed_objects_sorted: &[u64],
) -> Result<Option<u64>> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);
    let projection = projection_okey_only();

    let mut allowed_idx: usize = 0;
    let mut total: u64 = 0;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();

        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            if entry.o_type_const != Some(OType::IRI_REF.as_u16()) {
                return Ok(None);
            }

            let batch: ColumnBatch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Post)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;

            let mut i = 0usize;
            while i < batch.row_count {
                let b_id = batch.o_key.get(i);
                let mut count: u64 = 0;
                while i < batch.row_count && batch.o_key.get(i) == b_id {
                    count += 1;
                    i += 1;
                }

                while allowed_idx < allowed_objects_sorted.len()
                    && allowed_objects_sorted[allowed_idx] < b_id
                {
                    allowed_idx += 1;
                }
                if allowed_idx < allowed_objects_sorted.len()
                    && allowed_objects_sorted[allowed_idx] == b_id
                {
                    total = total.saturating_add(count);
                }
            }
        }
    }

    Ok(Some(total))
}

fn count_object_chain_minus(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_outer: &Ref,
    p2: &Ref,
    p3: &Ref,
) -> Result<Option<u64>> {
    let outer_sid = normalize_pred_sid(store, p_outer)?;
    let p2_sid = normalize_pred_sid(store, p2)?;
    let p3_sid = normalize_pred_sid(store, p3)?;

    let Some(p_outer_id) = store.sid_to_p_id(&outer_sid) else {
        return Ok(Some(0));
    };
    let Some(p2_id) = store.sid_to_p_id(&p2_sid) else {
        // MINUS block empty => removes nothing => count is total outer.
        return Ok(Some(count_rows_for_predicate_psot(
            store, g_id, p_outer_id,
        )?));
    };
    let Some(p3_id) = store.sid_to_p_id(&p3_sid) else {
        return Ok(Some(count_rows_for_predicate_psot(
            store, g_id, p_outer_id,
        )?));
    };

    // Total outer rows.
    let total = count_rows_for_predicate_psot(store, g_id, p_outer_id)?;

    let c_set = collect_subjects_for_predicate_set(store, g_id, p3_id)?;
    if c_set.is_empty() {
        return Ok(Some(total));
    }

    let Some(mut b_list) =
        collect_subjects_with_object_in_set_psot_sorted(store, g_id, p2_id, &c_set)?
    else {
        return Ok(None);
    };
    if b_list.is_empty() {
        return Ok(Some(total));
    }
    b_list.sort_unstable();
    b_list.dedup();

    let Some(in_set) = sum_post_object_group_counts_filtered(store, g_id, p_outer_id, &b_list)?
    else {
        return Ok(None);
    };

    Ok(Some(total.saturating_sub(in_set)))
}
