//! Dataset operator — fans triple-pattern evaluation across multiple graphs.
//!
//! `DatasetOperator` implements the [`Operator`] trait and wraps one inner
//! operator per active graph (from a SPARQL FROM / FROM NAMED dataset). It
//! drives their lifecycle (`open`/`next_batch`/`close`), merges results, and
//! stamps ledger provenance (`Binding::IriMatch`) when results span multiple
//! ledgers.
//!
//! A [`DatasetBuilder`] trait (factory pattern, option C) separates *how* to
//! build per-graph operators from *when* they are built. The planner
//! constructs a builder at plan time; `DatasetOperator` calls it at execution
//! time during [`Operator::open`].
//!
//! # Nested composition
//!
//! Because `DatasetBuilder::build()` returns [`BoxedOperator`], the inner
//! operator can be anything — including another `DatasetOperator`. Provenance
//! stamping passes `Binding::IriMatch` through unchanged, so nested datasets
//! compose correctly.
//!
//! See `docs/design/query-execution.md` for the pipeline overview.

use std::sync::Arc;

use async_trait::async_trait;
use fluree_db_core::{IndexType, ObjectBounds, Sid};

use crate::binary_scan::{schema_from_pattern_with_emit, BinaryScanOperator, EmitMask};
use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::dataset::ActiveGraphs;
use crate::error::{QueryError, Result};
use crate::operator::inline::{extend_schema, InlineOperator};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::triple::TriplePattern;
use crate::var_registry::VarId;

// =============================================================================
// DatasetBuilder trait
// =============================================================================

/// Factory for building per-graph operators at execution time.
///
/// Constructed by the planner at plan time with all the parameters needed to
/// create the inner operator. Stateless — [`build`](DatasetBuilder::build) is
/// called once per active graph during [`DatasetOperator::open`] and each call
/// produces an independent operator.
pub trait DatasetBuilder: Send + Sync {
    /// Build an operator for a single graph.
    ///
    /// The returned operator will be opened with a per-graph
    /// [`ExecutionContext`] (via `ctx.with_graph_ref()`).
    fn build(&self) -> Result<BoxedOperator>;

    /// Output schema. Must be stable across all `build()` calls.
    fn schema(&self) -> &[VarId];
}

// =============================================================================
// ScanDatasetBuilder
// =============================================================================

/// Builder for triple-pattern scans across dataset graphs.
///
/// Produces a [`BinaryScanOperator`] for each graph. This is the primary
/// builder implementation — other backends (BM25, vector, etc.) will add
/// their own builders in follow-up steps.
pub struct ScanDatasetBuilder {
    pattern: TriplePattern,
    object_bounds: Option<ObjectBounds>,
    inline_ops: Vec<InlineOperator>,
    emit: EmitMask,
    index_hint: Option<IndexType>,
    schema: Arc<[VarId]>,
}

impl ScanDatasetBuilder {
    pub fn new(
        pattern: TriplePattern,
        object_bounds: Option<ObjectBounds>,
        inline_ops: Vec<InlineOperator>,
        emit: EmitMask,
        index_hint: Option<IndexType>,
    ) -> Self {
        let (base_schema, _, _, _) = schema_from_pattern_with_emit(&pattern, emit);
        let schema: Arc<[VarId]> = extend_schema(&base_schema, &inline_ops).into();
        Self {
            pattern,
            object_bounds,
            inline_ops,
            emit,
            index_hint,
            schema,
        }
    }
}

impl DatasetBuilder for ScanDatasetBuilder {
    fn build(&self) -> Result<BoxedOperator> {
        Ok(Box::new(BinaryScanOperator::new_with_emit_and_index(
            self.pattern.clone(),
            self.object_bounds.clone(),
            self.inline_ops.clone(),
            self.emit,
            self.index_hint,
        )))
    }

    fn schema(&self) -> &[VarId] {
        &self.schema
    }
}

// =============================================================================
// DatasetOperator
// =============================================================================

/// Per-graph inner operator with its provenance metadata.
struct DatasetMember {
    operator: BoxedOperator,
    ledger_id: Arc<str>,
}

/// Operator that fans triple-pattern evaluation across multiple graphs.
///
/// During [`open`](Operator::open), builds one inner operator per active graph
/// (via the [`DatasetBuilder`] factory) and opens each with a per-graph
/// [`ExecutionContext`]. During [`next_batch`](Operator::next_batch), drains
/// members in sequence and stamps ledger provenance when results span multiple
/// ledgers.
pub struct DatasetOperator {
    builder: Box<dyn DatasetBuilder>,
    state: OperatorState,
    /// Per-graph inner operators, indexed in the same order as
    /// `ctx.active_graphs()` returns graphs.
    members: Vec<DatasetMember>,
    /// Index of the member currently being drained.
    current_member: usize,
    /// True when members span multiple distinct ledger IDs, requiring
    /// `Binding::Sid` → `Binding::IriMatch` conversion.
    needs_provenance: bool,
}

impl DatasetOperator {
    /// Create a new dataset operator driven by the given builder.
    pub fn new(builder: Box<dyn DatasetBuilder>) -> Self {
        Self {
            builder,
            state: OperatorState::Created,
            members: Vec::new(),
            current_member: 0,
            needs_provenance: false,
        }
    }
}

/// Convert `Binding::Sid` values in a batch to `Binding::IriMatch` for
/// cross-ledger provenance tracking.
///
/// - `Binding::Sid` → decoded via `snapshot`, wrapped in `IriMatch` with
///   `ledger_id`. If the SID cannot be decoded, falls back to
///   `Binding::Sid` (preserves correctness for novelty-only SIDs that
///   may not have persisted IRIs yet).
/// - `Binding::IriMatch` → passed through unchanged (supports nested
///   `DatasetOperator` composition).
/// - All other binding types → unchanged.
fn stamp_provenance(
    batch: Batch,
    ledger_id: &Arc<str>,
    ctx: &ExecutionContext<'_>,
) -> Result<Batch> {
    let (schema, columns) = batch.into_parts();

    if columns.is_empty() {
        return Batch::empty(schema).map_err(|e| QueryError::Internal(e.to_string()));
    }

    let stamped_columns: Vec<Vec<Binding>> = columns
        .into_iter()
        .map(|col| {
            col.into_iter()
                .map(|binding| stamp_binding(binding, ledger_id, ctx))
                .collect()
        })
        .collect();

    Batch::new(schema, stamped_columns).map_err(|e| QueryError::Internal(e.to_string()))
}

/// Stamp a single binding with ledger provenance.
///
/// `Binding::Sid` is converted to `IriMatch`; all other variants are moved
/// through unchanged (no cloning).
fn stamp_binding(binding: Binding, ledger_id: &Arc<str>, ctx: &ExecutionContext<'_>) -> Binding {
    match binding {
        Binding::Sid(ref sid) => sid_to_iri_match(sid, ledger_id, ctx),
        other => other,
    }
}

/// Convert a `Sid` to `IriMatch` using the dataset's decoding context.
fn sid_to_iri_match(sid: &Sid, ledger_id: &Arc<str>, ctx: &ExecutionContext<'_>) -> Binding {
    if let Some(iri) = ctx.decode_sid_in_ledger(sid, ledger_id.as_ref()) {
        Binding::iri_match(
            Arc::<str>::from(iri.as_str()),
            sid.clone(),
            Arc::clone(ledger_id),
        )
    } else {
        // Cannot decode — preserve as Sid. This can happen for
        // novelty-only SIDs that haven't been persisted yet.
        Binding::Sid(sid.clone())
    }
}

#[async_trait]
impl Operator for DatasetOperator {
    fn schema(&self) -> &[VarId] {
        self.builder.schema()
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        match ctx.active_graphs() {
            ActiveGraphs::Single => {
                // Single-graph mode: build one operator, open with parent
                // context directly. No fanout, no provenance stamping.
                let mut inner = self.builder.build()?;
                inner.open(ctx).await?;
                self.members.push(DatasetMember {
                    operator: inner,
                    ledger_id: Arc::from(""),
                });
                self.needs_provenance = false;
            }
            ActiveGraphs::Many(graphs) => {
                let first_ledger_id = graphs.first().map(|g| g.ledger_id.as_ref());
                let mut all_same_ledger = true;

                for graph in &graphs {
                    let mut inner = self.builder.build()?;
                    let per_graph_ctx = ctx.with_graph_ref(graph);
                    inner.open(&per_graph_ctx).await?;

                    if all_same_ledger {
                        if let Some(first) = first_ledger_id {
                            if first != graph.ledger_id.as_ref() {
                                all_same_ledger = false;
                            }
                        }
                    }

                    self.members.push(DatasetMember {
                        operator: inner,
                        ledger_id: Arc::clone(&graph.ledger_id),
                    });
                }
                self.needs_provenance = !all_same_ledger;
            }
        }

        self.current_member = 0;
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

        let graphs = ctx.active_graphs();

        while self.current_member < self.members.len() {
            let member = &mut self.members[self.current_member];

            let batch = match &graphs {
                ActiveGraphs::Many(g) => {
                    let graph_ctx = ctx.with_graph_ref(g[self.current_member]);
                    member.operator.next_batch(&graph_ctx).await?
                }
                ActiveGraphs::Single => member.operator.next_batch(ctx).await?,
            };

            match batch {
                Some(batch) if batch.is_empty() => continue,
                Some(batch) => {
                    let result = if self.needs_provenance {
                        stamp_provenance(batch, &member.ledger_id, ctx)?
                    } else {
                        batch
                    };
                    return Ok(Some(result));
                }
                None => {
                    // This member is exhausted, move to next.
                    self.current_member += 1;
                }
            }
        }

        // All members exhausted.
        self.state = OperatorState::Exhausted;
        Ok(None)
    }

    fn close(&mut self) {
        for member in &mut self.members {
            member.operator.close();
        }
        self.members.clear();
        self.current_member = 0;
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::var_registry::VarId;

    /// Verify ScanDatasetBuilder produces operators with consistent schema.
    #[test]
    fn scan_dataset_builder_consistent_schema() {
        use crate::triple::{Ref, Term};
        use fluree_db_core::Sid;

        let s = VarId(0);
        let o = VarId(1);
        let pattern =
            TriplePattern::new(Ref::Var(s), Ref::Sid(Sid::new(100, "name")), Term::Var(o));

        let builder = ScanDatasetBuilder::new(pattern, None, Vec::new(), EmitMask::ALL, None);

        let schema = builder.schema();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0], s);
        assert_eq!(schema[1], o);

        // Build two operators — schemas must match.
        let op1 = builder.build().unwrap();
        let op2 = builder.build().unwrap();
        assert_eq!(op1.schema(), op2.schema());
        assert_eq!(op1.schema(), builder.schema());
    }
}
