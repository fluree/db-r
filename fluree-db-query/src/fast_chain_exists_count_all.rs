//! Fast-path for `COUNT(*)` with a 2-hop join chain and a simple EXISTS.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE {
//!   ?a <p1> ?b .
//!   ?b <p2> ?c .
//!   FILTER EXISTS { ?c <p3> ?d . }
//! }
//! ```
//!
//! This operator computes:
//!
//! \[
//!   \sum_{b} \bigl( count_{p1}(b) \times count_{p2,exists}(b) \bigr)
//! \]
//!
//! where:
//! - `count_{p1}(b)` is the number of `?a` such that `?a p1 b`
//! - `count_{p2,exists}(b)` is the number of `?c` such that `?b p2 c` and `c` satisfies the EXISTS.
//!
//! It avoids materializing join rows and never decodes to IRIs/strings.

use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_set, fast_path_store,
    leaf_entries_for_predicate, normalize_pred_sid, projection_sid_okey, FastPathOperator,
    PostObjectGroupCountIter,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::o_type::OType;
use fluree_db_core::GraphId;
use rustc_hash::FxHashSet;

pub fn predicate_chain_exists_join_count_all_operator(
    p1: Ref,
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
            let Some(count) = count_chain_exists_join(store, ctx.binary_g_id, &p1, &p2, &p3)?
            else {
                return Ok(None);
            };
            Ok(Some(build_count_batch(out_var, count as i64)?))
        },
        fallback,
        "chain+exists COUNT(*)",
    )
}

/// Streaming iterator over PSOT leaflets for a predicate that yields
/// `(subject_id, filtered_count)` groups — only counting objects that appear
/// in `exists_subjects`.
///
/// Requires `o_type_const == IRI_REF` so that `o_key` is a subject ID.
struct PsotFilteredCountIter<'a> {
    store: &'a BinaryIndexStore,
    p_id: u32,
    exists_subjects: &'a FxHashSet<u64>,
    leaf_entries: &'a [fluree_db_binary_index::format::branch::LeafEntry],
    leaf_pos: usize,
    leaflet_idx: usize,
    row: usize,
    handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
    batch: Option<fluree_db_binary_index::ColumnBatch>,
}

impl<'a> PsotFilteredCountIter<'a> {
    fn new(
        store: &'a BinaryIndexStore,
        g_id: GraphId,
        p_id: u32,
        exists_subjects: &'a FxHashSet<u64>,
    ) -> Result<Option<Self>> {
        let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);

        Ok(Some(Self {
            store,
            p_id,
            exists_subjects,
            leaf_entries: leaves,
            leaf_pos: 0,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
        }))
    }

    fn load_next_batch(&mut self) -> Result<Option<()>> {
        let projection = projection_sid_okey();

        loop {
            if self.handle.is_none() {
                if self.leaf_pos >= self.leaf_entries.len() {
                    return Ok(None);
                }
                let leaf_entry = &self.leaf_entries[self.leaf_pos];
                self.leaf_pos += 1;
                self.leaflet_idx = 0;
                self.row = 0;
                self.batch = None;
                self.handle = Some(
                    self.store
                        .open_leaf_handle(
                            &leaf_entry.leaf_cid,
                            leaf_entry.sidecar_cid.as_ref(),
                            false,
                        )
                        .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?,
                );
            }

            let handle = self.handle.as_ref().unwrap();
            let dir = handle.dir();
            while self.leaflet_idx < dir.entries.len() {
                let entry = &dir.entries[self.leaflet_idx];
                let idx = self.leaflet_idx;
                self.leaflet_idx += 1;
                if entry.row_count == 0 || entry.p_const != Some(self.p_id) {
                    continue;
                }
                // Require IRI refs for join key `?c` (object of p2), so o_key is a subject id.
                if entry.o_type_const != Some(OType::IRI_REF.as_u16()) {
                    return Ok(None);
                }
                let batch = handle
                    .load_columns(idx, &projection, RunSortOrder::Psot)
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
                self.row = 0;
                self.batch = Some(batch);
                return Ok(Some(()));
            }

            self.handle = None;
        }
    }

    fn next_group(&mut self) -> Result<Option<(u64, u64)>> {
        loop {
            if self.batch.is_none() && self.load_next_batch()?.is_none() {
                return Ok(None);
            }
            let batch = self.batch.as_ref().unwrap();
            if self.row >= batch.row_count {
                self.batch = None;
                continue;
            }

            let b_id = batch.s_id.get(self.row);
            let mut count: u64 = 0;
            while self.row < batch.row_count {
                let sid = batch.s_id.get(self.row);
                if sid != b_id {
                    break;
                }
                let c_id = batch.o_key.get(self.row);
                if self.exists_subjects.contains(&c_id) {
                    count += 1;
                }
                self.row += 1;
            }

            if count > 0 {
                return Ok(Some((b_id, count)));
            }
            // else: skip this b_id entirely (no qualifying c values).
        }
    }
}

fn count_chain_exists_join(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p1: &Ref,
    p2: &Ref,
    p3: &Ref,
) -> Result<Option<u64>> {
    let p1_sid = normalize_pred_sid(store, p1)?;
    let p2_sid = normalize_pred_sid(store, p2)?;
    let p3_sid = normalize_pred_sid(store, p3)?;

    let Some(p1_id) = store.sid_to_p_id(&p1_sid) else {
        return Ok(Some(0));
    };
    let Some(p2_id) = store.sid_to_p_id(&p2_sid) else {
        return Ok(Some(0));
    };
    let Some(p3_id) = store.sid_to_p_id(&p3_sid) else {
        return Ok(Some(0));
    };

    // EXISTS subjects are `?c` in: ?c p3 ?d .
    let exists_subjects = collect_subjects_for_predicate_set(store, g_id, p3_id)?;
    if exists_subjects.is_empty() {
        return Ok(Some(0));
    }

    let mut ab = match PostObjectGroupCountIter::new(store, g_id, p1_id)? {
        Some(it) => it,
        None => return Ok(None),
    };
    let mut bc = match PsotFilteredCountIter::new(store, g_id, p2_id, &exists_subjects)? {
        Some(it) => it,
        None => return Ok(None),
    };

    let mut left = ab.next_group()?;
    let mut right = bc.next_group()?;
    let mut total: u64 = 0;

    while let (Some((b1, a_count)), Some((b2, c_count))) = (left, right) {
        if b1 < b2 {
            left = ab.next_group()?;
        } else if b1 > b2 {
            right = bc.next_group()?;
        } else {
            total = total.saturating_add(a_count.saturating_mul(c_count));
            left = ab.next_group()?;
            right = bc.next_group()?;
        }
    }

    Ok(Some(total))
}
