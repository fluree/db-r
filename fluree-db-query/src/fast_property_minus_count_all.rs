//! Fast-path: `COUNT(*)` for a same-subject star join with a `MINUS` constraint.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE {
//!   ?s <p1> ?o1 .
//!   ?s <p2> ?o2 .
//!   MINUS { ?s <p_minus> ?x . }
//! }
//! ```
//!
//! Semantics for this supported shape:
//! - For each subject `s`, the outer join yields `Π_i count_{pi}(s)` rows.
//! - The MINUS block eliminates all rows for `s` iff `s` has at least one `<p_minus>` row.
//!
//! Therefore:
//!
//! \[
//!   \sum_{s \notin S_{minus}} \prod_i count_{p_i}(s)
//! \]
//!
//! This operator:
//! - Builds `S_minus` as sorted distinct subjects from PSOT(p_minus).
//! - Streams grouped per-subject counts for each outer predicate from PSOT.
//! - N-way merge-joins the count streams on `s_id`, skipping `s in S_minus`.
//! - Never materializes join rows or decodes values.

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, collect_subjects_for_predicate_sorted, fast_path_store, normalize_pred_sid,
    PrecomputedSingleBatchOperator, PsotSubjectCountIter,
};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::triple::Ref;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::GraphId;

pub struct PredicatePropertyMinusCountAllOperator {
    outer_predicates: Vec<Ref>,
    minus_predicate: Ref,
    out_var: VarId,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
}

impl PredicatePropertyMinusCountAllOperator {
    pub fn new(
        outer_predicates: Vec<Ref>,
        minus_predicate: Ref,
        out_var: VarId,
        fallback: Option<BoxedOperator>,
    ) -> Self {
        Self {
            outer_predicates,
            minus_predicate,
            out_var,
            state: OperatorState::Created,
            fallback,
        }
    }
}

#[async_trait]
impl Operator for PredicatePropertyMinusCountAllOperator {
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
            let count = count_property_minus(
                store,
                ctx.binary_g_id,
                &self.outer_predicates,
                &self.minus_predicate,
            )?;
            self.state = OperatorState::Open;
            self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(
                build_count_batch(self.out_var, i64::try_from(count).unwrap_or(i64::MAX))?,
            )));
            return Ok(());
        }

        let Some(fallback) = &mut self.fallback else {
            return Err(QueryError::Internal(
                "property+minus COUNT(*) fast-path unavailable and no fallback provided"
                    .to_string(),
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
        self.state = OperatorState::Closed;
    }
}

fn count_property_minus(
    store: &BinaryIndexStore,
    g_id: GraphId,
    outer_predicates: &[Ref],
    minus_predicate: &Ref,
) -> Result<u64> {
    if outer_predicates.len() < 2 {
        return Err(QueryError::Internal(
            "property+minus fast path requires 2+ outer predicates".to_string(),
        ));
    }

    // Excluded subjects.
    let minus_sid = normalize_pred_sid(store, minus_predicate)?;
    let Some(p_minus) = store.sid_to_p_id(&minus_sid) else {
        // If the minus predicate doesn't exist, nothing is removed.
        return count_property_join_all(store, g_id, outer_predicates, &[]);
    };
    let excluded = collect_subjects_for_predicate_sorted(store, g_id, p_minus)?;
    count_property_join_all(store, g_id, outer_predicates, &excluded)
}

fn count_property_join_all(
    store: &BinaryIndexStore,
    g_id: GraphId,
    outer_predicates: &[Ref],
    excluded_subjects_sorted: &[u64],
) -> Result<u64> {
    let mut iters: Vec<PsotSubjectCountIter<'_>> = Vec::with_capacity(outer_predicates.len());
    for p in outer_predicates {
        let sid = normalize_pred_sid(store, p)?;
        let Some(pid) = store.sid_to_p_id(&sid) else {
            return Ok(0);
        };
        iters.push(PsotSubjectCountIter::new(store, g_id, pid)?);
    }

    let mut curr: Vec<Option<(u64, u64)>> = Vec::with_capacity(iters.len());
    for it in &mut iters {
        curr.push(it.next_group()?);
    }

    let mut excl_idx: usize = 0;
    let mut total: u128 = 0;

    loop {
        if curr.iter().any(|c| c.is_none()) {
            break;
        }

        let max_s = curr.iter().filter_map(|c| c.map(|(s, _)| s)).max().unwrap();
        if curr.iter().all(|c| c.map(|(s, _)| s) == Some(max_s)) {
            while excl_idx < excluded_subjects_sorted.len()
                && excluded_subjects_sorted[excl_idx] < max_s
            {
                excl_idx += 1;
            }
            let excluded = excl_idx < excluded_subjects_sorted.len()
                && excluded_subjects_sorted[excl_idx] == max_s;
            if !excluded {
                let product: u128 = curr.iter().map(|c| c.unwrap().1 as u128).product();
                total = total.saturating_add(product);
            }
            for (i, it) in iters.iter_mut().enumerate() {
                curr[i] = it.next_group()?;
            }
        } else {
            for (i, it) in iters.iter_mut().enumerate() {
                if let Some((s_id, _)) = curr[i] {
                    if s_id < max_s {
                        curr[i] = it.next_group()?;
                    }
                }
            }
        }
    }

    Ok(total.min(u64::MAX as u128) as u64)
}
