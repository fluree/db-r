//! Fast-path fused scan + COUNT(DISTINCT ?o) for a single predicate.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(DISTINCT ?object) AS ?count)
//! WHERE { ?subject <p> ?object . }
//! ```
//!
//! The generic pipeline would scan and decode every matching row, then hash decoded
//! IRIs/literals for distinctness. For very large predicates (e.g., DBLP `rdf:type`),
//! per-row dictionary lookups dominate runtime.
//!
//! This operator instead scans the predicate's **POST** range and counts distinct
//! object IDs directly from the encoded `(o_type, o_key)` columns, without
//! materializing `Sid` / string values.

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, fast_path_store, leaf_entries_for_predicate, normalize_pred_sid,
    projection_okey_only, PrecomputedSingleBatchOperator,
};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::triple::Ref;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::o_type::OType;
use fluree_db_core::GraphId;

/// Fused operator that outputs a single-row batch with the COUNT(DISTINCT) result.
pub struct PredicateCountDistinctObjectOperator {
    predicate: Ref,
    out_var: VarId,
    state: OperatorState,
    done: bool,
    /// When fast-path is not available at runtime, fall back to this operator tree.
    fallback: Option<BoxedOperator>,
}

impl PredicateCountDistinctObjectOperator {
    pub fn new(predicate: Ref, out_var: VarId, fallback: Option<BoxedOperator>) -> Self {
        Self {
            predicate,
            out_var,
            state: OperatorState::Created,
            done: false,
            fallback,
        }
    }
}

#[async_trait]
impl Operator for PredicateCountDistinctObjectOperator {
    fn schema(&self) -> &[VarId] {
        std::slice::from_ref(&self.out_var)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        if let Some(store) = fast_path_store(ctx) {
            match count_distinct_object_post(store, ctx.binary_g_id, &self.predicate)? {
                Some(count) => {
                    self.state = OperatorState::Open;
                    self.done = false;
                    self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(
                        build_count_batch(self.out_var, count as i64)?,
                    )));
                    return Ok(());
                }
                None => {
                    // Unsupported at runtime — fall through to planned pipeline.
                }
            }
        }

        let Some(fallback) = &mut self.fallback else {
            return Err(QueryError::Internal(
                "COUNT(DISTINCT) fast-path unavailable and no fallback provided".to_string(),
            ));
        };
        fallback.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }

        let Some(fallback) = &mut self.fallback else {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        };
        let b = fallback.next_batch(ctx).await?;
        if b.is_none() {
            self.state = OperatorState::Exhausted;
        }
        Ok(b)
    }

    fn close(&mut self) {
        if let Some(fb) = &mut self.fallback {
            fb.close();
        }
        self.done = true;
        self.state = OperatorState::Closed;
    }
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
