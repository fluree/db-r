//! Fast-path for `COUNT(*)` with a 2-predicate star join and an EXISTS constraint.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (COUNT(*) AS ?count)
//! WHERE {
//!   ?s <p1> ?o1 .
//!   ?s <p2> ?o2 .
//!   FILTER EXISTS { ?s <p3> ?o3 . }
//! }
//! ```
//!
//! Semantics:
//! - For each subject `s`, there are `count_p1(s) * count_p2(s)` join rows.
//! - The EXISTS constraint keeps only subjects that have at least one `<p3>` row.
//!
//! This operator computes:
//!
//! \[
//!   \sum_{s \in S3} count_{p1}(s) \times count_{p2}(s)
//! \]
//!
//! where `S3 = { s | s p3 ?o3 }`.
//!
//! Implementation:
//! - Build `S3` as a sorted `Vec<s_id>` by scanning PSOT(p3) (SId column only).
//! - Stream grouped subject counts for p1 and p2 from PSOT (SId column only).
//! - Merge-join the two count streams on `s_id` to compute products.
//! - Merge-filter by `S3` (also sorted) and sum products.
//!
//! Avoids per-row EXISTS evaluation, join-row materialization, and value decoding.

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

pub struct PredicateStarExistsJoinCountAllOperator {
    p1: Ref,
    p2: Ref,
    p3_exists: Ref,
    out_var: VarId,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
}

impl PredicateStarExistsJoinCountAllOperator {
    pub fn new(
        p1: Ref,
        p2: Ref,
        p3_exists: Ref,
        out_var: VarId,
        fallback: Option<BoxedOperator>,
    ) -> Self {
        Self {
            p1,
            p2,
            p3_exists,
            out_var,
            state: OperatorState::Created,
            fallback,
        }
    }
}

#[async_trait]
impl Operator for PredicateStarExistsJoinCountAllOperator {
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
            let count = count_star_exists_join(
                store,
                ctx.binary_g_id,
                &self.p1,
                &self.p2,
                &self.p3_exists,
            )?;
            self.state = OperatorState::Open;
            self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(
                build_count_batch(self.out_var, count as i64)?,
            )));
            return Ok(());
        }

        let Some(fallback) = &mut self.fallback else {
            return Err(QueryError::Internal(
                "star+exists COUNT(*) fast-path unavailable and no fallback provided".to_string(),
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

fn count_star_exists_join(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p1: &Ref,
    p2: &Ref,
    p3_exists: &Ref,
) -> Result<u64> {
    let p1_sid = normalize_pred_sid(store, p1)?;
    let p2_sid = normalize_pred_sid(store, p2)?;
    let p3_sid = normalize_pred_sid(store, p3_exists)?;

    let Some(p1_id) = store.sid_to_p_id(&p1_sid) else {
        return Ok(0);
    };
    let Some(p2_id) = store.sid_to_p_id(&p2_sid) else {
        return Ok(0);
    };
    let Some(p3_id) = store.sid_to_p_id(&p3_sid) else {
        return Ok(0);
    };

    let s3 = collect_subjects_for_predicate_sorted(store, g_id, p3_id)?;
    if s3.is_empty() {
        return Ok(0);
    }

    let mut it1 = PsotSubjectCountIter::new(store, g_id, p1_id)?;
    let mut it2 = PsotSubjectCountIter::new(store, g_id, p2_id)?;
    let mut a = it1.next_group()?;
    let mut b = it2.next_group()?;

    // First merge: p1 and p2 counts -> stream of (s, prod).
    let mut prod_stream: Vec<(u64, u64)> = Vec::new();
    while let (Some((s1, c1)), Some((s2, c2))) = (a, b) {
        if s1 < s2 {
            a = it1.next_group()?;
        } else if s1 > s2 {
            b = it2.next_group()?;
        } else {
            prod_stream.push((s1, c1.saturating_mul(c2)));
            a = it1.next_group()?;
            b = it2.next_group()?;
        }
    }
    if prod_stream.is_empty() {
        return Ok(0);
    }

    // Second merge: filter by s3 (both sorted).
    let mut total: u64 = 0;
    let mut i: usize = 0;
    let mut j: usize = 0;
    while i < prod_stream.len() && j < s3.len() {
        let (s, prod) = prod_stream[i];
        let s_ok = s3[j];
        if s < s_ok {
            i += 1;
        } else if s > s_ok {
            j += 1;
        } else {
            total = total.saturating_add(prod);
            i += 1;
            j += 1;
        }
    }
    Ok(total)
}
