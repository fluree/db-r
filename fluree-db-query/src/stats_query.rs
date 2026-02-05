//! Fast-path operators for "stats queries".
//!
//! These operators answer certain aggregate queries directly from `IndexStats`
//! / `StatsView` without scanning the triple store.

use crate::binding::{Batch, Binding};
use crate::context::{ExecutionContext, WellKnownDatatypes};
use crate::error::{QueryError, Result};
use crate::operator::{Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::{FlakeValue, StatsView, Storage};
use std::sync::Arc;

/// Emit per-predicate counts using `StatsView` (no triple scan).
///
/// Intended to fast-path queries like:
/// `SELECT ?p (COUNT(?s) AS ?count) WHERE { ?s ?p ?o } GROUP BY ?p ORDER BY DESC(?count)`
pub struct StatsCountByPredicateOperator<S: Storage + 'static> {
    stats: Arc<StatsView>,
    pred_var: VarId,
    count_var: VarId,
    schema: Arc<[VarId]>,
    state: OperatorState,
    rows: Vec<(Binding, Binding)>,
    pos: usize,
    _phantom: std::marker::PhantomData<S>,
}

impl<S: Storage + 'static> StatsCountByPredicateOperator<S> {
    pub fn new(stats: Arc<StatsView>, pred_var: VarId, count_var: VarId) -> Self {
        let schema: Arc<[VarId]> = Arc::from(vec![pred_var, count_var].into_boxed_slice());
        Self {
            stats,
            pred_var,
            count_var,
            schema,
            state: OperatorState::Created,
            rows: Vec::new(),
            pos: 0,
            _phantom: std::marker::PhantomData,
        }
    }

    fn build_rows(&self, ctx: &ExecutionContext<'_, S>) -> Result<Vec<(Binding, Binding)>> {
        let dt = WellKnownDatatypes::new().xsd_long;

        // Prefer graph-scoped stats if present (and we can resolve p_id → Sid).
        if let (Some(store), Some(props)) = (
            ctx.binary_store.as_deref(),
            self.stats.get_graph_properties(ctx.binary_g_id),
        ) {
            let mut out = Vec::with_capacity(props.len());
            for (&p_id, data) in props.iter() {
                let Some(iri) = store.resolve_predicate_iri(p_id) else {
                    continue;
                };
                let pred_sid = store.encode_iri(iri);
                let pred = Binding::Sid(pred_sid);
                let count = Binding::lit(FlakeValue::Long(data.count as i64), dt.clone());
                out.push((pred, count));
            }
            return Ok(out);
        }

        // Fallback: aggregate SID-keyed stats (across graphs).
        if !self.stats.properties.is_empty() {
            let mut out = Vec::with_capacity(self.stats.properties.len());
            for (sid, data) in self.stats.properties.iter() {
                let pred = Binding::Sid(sid.clone());
                let count = Binding::lit(FlakeValue::Long(data.count as i64), dt.clone());
                out.push((pred, count));
            }
            return Ok(out);
        }

        Err(QueryError::InvalidQuery(
            "stats query fast-path requires IndexStats/StatsView".to_string(),
        ))
    }
}

#[async_trait]
impl<S: Storage + 'static> Operator<S> for StatsCountByPredicateOperator<S> {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_, S>) -> Result<()> {
        if !self.state.can_open() {
            return Err(QueryError::OperatorAlreadyOpened);
        }
        self.rows = self.build_rows(ctx)?;
        self.pos = 0;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_, S>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }
        if self.pos >= self.rows.len() {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }

        let end = (self.pos + ctx.batch_size).min(self.rows.len());
        let slice = &self.rows[self.pos..end];
        self.pos = end;

        let mut pred_col: Vec<Binding> = Vec::with_capacity(slice.len());
        let mut count_col: Vec<Binding> = Vec::with_capacity(slice.len());
        for (p, c) in slice {
            pred_col.push(p.clone());
            count_col.push(c.clone());
        }

        Ok(Some(Batch::new(
            self.schema.clone(),
            vec![pred_col, count_col],
        )?))
    }

    fn close(&mut self) {
        self.state = OperatorState::Closed;
        self.rows.clear();
        self.pos = 0;
    }

    fn estimated_rows(&self) -> Option<usize> {
        Some(self.rows.len())
    }
}

