//! Fast-path for `COUNT(*)` with an outer triple and a 2-hop EXISTS chain on the object.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE {
//!   ?a <p_outer> ?b .
//!   FILTER EXISTS { ?b <p2> ?c . ?c <p3> ?d . }
//! }
//! ```
//!
//! This computes:
//! - Let `C = { c | c p3 ?d }`  (subjects that have any `p3` edge)
//! - Let `B = { b | b p2 c and c in C }`
//! - Answer = number of rows in `?a p_outer ?b` where `b in B`
//!
//! Implementation details (streaming, no row materialization):
//! - Build `C` by scanning PSOT(p3) and collecting distinct subject IDs.
//! - Build `B` by scanning PSOT(p2), grouping by subject `b`, and checking whether any
//!   object `c` is in `C`. Output `B` as a sorted Vec.
//! - Scan POST(p_outer), grouped by object `b`, summing group counts where `b in B`.
//!
//! Requires constant `o_type_const == IRI_REF` on the relevant leaflets so that `o_key`
//! is a subject ID (join key).

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_set, fast_path_store,
    leaf_entries_for_predicate, normalize_pred_sid, projection_okey_only, projection_sid_okey,
    FastPathOperator,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::{BinaryIndexStore, ColumnBatch};
use fluree_db_core::o_type::OType;
use fluree_db_core::GraphId;
use rustc_hash::FxHashSet;

pub fn predicate_object_chain_exists_join_count_all_operator(
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
            let Some(count) =
                count_object_chain_exists_join(store, ctx.binary_g_id, &p_outer, &p2, &p3)?
            else {
                return Ok(None);
            };
            Ok(Some(build_count_batch(out_var, count as i64)?))
        },
        fallback,
        "object-chain EXISTS COUNT(*)",
    )
}

/// Collect subjects `b` from PSOT(p2) where any object `c` is in `object_subjects`.
///
/// Returns a sorted `Vec<u64>` of qualifying subject IDs, or `None` if any
/// leaflet has a non-IRI_REF `o_type_const` (cannot guarantee correctness).
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
            // We require `?c` to be an IRI ref so its ID is stored in o_key.
            if entry.o_type_const != Some(OType::IRI_REF.as_u16()) {
                return Ok(None);
            }

            let batch = handle
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

/// Sum row counts from POST(p_outer) for object groups whose `o_key` is in `allowed_objects_sorted`.
///
/// Uses a merge-join between sorted POST groups and the sorted allowed list.
/// Returns `None` if any leaflet has non-IRI_REF `o_type_const`.
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
            // We require `?b` to be an IRI ref so its ID is stored in o_key.
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

fn count_object_chain_exists_join(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_outer: &Ref,
    p2: &Ref,
    p3: &Ref,
) -> Result<Option<u64>> {
    let p_outer_sid = normalize_pred_sid(store, p_outer)?;
    let p2_sid = normalize_pred_sid(store, p2)?;
    let p3_sid = normalize_pred_sid(store, p3)?;

    let Some(p_outer_id) = store.sid_to_p_id(&p_outer_sid) else {
        return Ok(Some(0));
    };
    let Some(p2_id) = store.sid_to_p_id(&p2_sid) else {
        return Ok(Some(0));
    };
    let Some(p3_id) = store.sid_to_p_id(&p3_sid) else {
        return Ok(Some(0));
    };

    // C = subjects that have any p3 edge.
    let c_subjects = collect_subjects_for_predicate_set(store, g_id, p3_id)?;
    if c_subjects.is_empty() {
        return Ok(Some(0));
    }

    // B = subjects b that have any (b p2 c) where c in C. Output as sorted Vec.
    let Some(mut b_subjects) =
        collect_subjects_with_object_in_set_psot_sorted(store, g_id, p2_id, &c_subjects)?
    else {
        return Ok(None);
    };
    if b_subjects.is_empty() {
        return Ok(Some(0));
    }
    // It is naturally produced in sorted s_id order; keep it monotone.
    // (Defensive: if future code changes ordering, sort.)
    if !b_subjects.windows(2).all(|w| w[0] <= w[1]) {
        b_subjects.sort_unstable();
        b_subjects.dedup();
    }

    sum_post_object_group_counts_filtered(store, g_id, p_outer_id, &b_subjects)
}
