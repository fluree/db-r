//! Fast-path: `COUNT(*)` for a single transitive property path (`+`) with a fixed endpoint.
//!
//! Targets queries like:
//! - `SELECT (COUNT(*) AS ?count) WHERE { <S> <p>+ ?o }`
//! - (future) `SELECT (COUNT(*) AS ?count) WHERE { ?s <p>+ <O> }`
//!
//! This avoids the generic `PropertyPathOperator`'s repeated range scans by:
//! - scanning PSOT(p) once to build an adjacency map of ref-only edges
//! - running a BFS/visited traversal from the fixed seed and counting unique reachable endpoints
//!
//! Semantics for `+` (one-or-more):
//! - does NOT include the start node unless there is a non-zero-length cycle back to it
//! - traverses only IRI_REF edges (ref-only), matching existing property path behavior

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, build_iri_adjacency_from_cursor, build_psot_cursor_for_predicate,
    cursor_projection_sid_otype_okey, reach_count_plus, subject_ref_to_s_id,
};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::triple::Ref;
use crate::var_registry::VarId;
use async_trait::async_trait;
use std::sync::Arc;

pub struct PropertyPathPlusFixedSubjectCountAllOperator {
    predicate: fluree_db_core::Sid,
    subject: Ref,
    out_var: VarId,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
    emitted: bool,
    result: Option<i64>,
}

impl PropertyPathPlusFixedSubjectCountAllOperator {
    pub fn new(
        predicate: fluree_db_core::Sid,
        subject: Ref,
        out_var: VarId,
        fallback: Option<BoxedOperator>,
    ) -> Self {
        Self {
            predicate,
            subject,
            out_var,
            state: OperatorState::Created,
            fallback,
            emitted: false,
            result: None,
        }
    }
}

#[async_trait]
impl Operator for PropertyPathPlusFixedSubjectCountAllOperator {
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

        let allow_fast = !ctx.history_mode && ctx.from_t.is_none() && !ctx.has_policy();
        if allow_fast {
            if let Some(store) = ctx.binary_store.as_ref() {
                if ctx.to_t == store.max_t() {
                    if let Some(count) = count_reachable_plus_from_fixed_subject(
                        store,
                        ctx,
                        ctx.binary_g_id,
                        &self.predicate,
                        &self.subject,
                    )? {
                        self.result = Some(i64::try_from(count).unwrap_or(i64::MAX));
                        self.emitted = false;
                        self.state = OperatorState::Open;
                        self.fallback = None;
                        return Ok(());
                    }
                }
            }
        }

        let Some(fallback) = &mut self.fallback else {
            return Err(QueryError::Internal(
                "property-path+ COUNT(*) fast-path unavailable and no fallback provided".into(),
            ));
        };
        fallback.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if let Some(fb) = &mut self.fallback {
            return fb.next_batch(ctx).await;
        }

        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }
        if self.emitted {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }

        let n = self.result.unwrap_or(0);
        let b = build_count_batch(self.out_var, n)?;
        self.emitted = true;
        Ok(Some(b))
    }

    fn close(&mut self) {
        if let Some(fb) = &mut self.fallback {
            fb.close();
        }
        self.state = OperatorState::Closed;
        self.emitted = false;
        self.result = None;
    }
}

fn count_reachable_plus_from_fixed_subject(
    store: &Arc<fluree_db_binary_index::BinaryIndexStore>,
    ctx: &ExecutionContext<'_>,
    g_id: fluree_db_core::GraphId,
    pred_sid: &fluree_db_core::Sid,
    subj: &Ref,
) -> Result<Option<u64>> {
    let Some(seed) = subject_ref_to_s_id(store, subj)? else {
        return Ok(None);
    };
    let Some(p_id) = store.sid_to_p_id(pred_sid) else {
        return Ok(Some(0));
    };

    let projection = cursor_projection_sid_otype_okey();
    let mut cursor =
        build_psot_cursor_for_predicate(ctx, store, g_id, pred_sid.clone(), p_id, projection)?
            .ok_or_else(|| QueryError::Internal("property-path+: missing PSOT branch".into()))?;

    let adj = build_iri_adjacency_from_cursor(&mut cursor)?;
    Ok(Some(reach_count_plus(&adj, seed)))
}
