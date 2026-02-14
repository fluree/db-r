//! Join operators for multi-pattern queries
//!
//! This module provides `NestedLoopJoinOperator` which implements nested-loop join
//! where left results drive right scans. It enforces var unification - shared
//! vars between left and right must match exactly.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::dataset::ActiveGraphs;
use crate::error::{QueryError, Result};
use crate::operator::{Operator, OperatorState};
use crate::pattern::{Term, TriplePattern};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::subject_id::{SubjectId, SubjectIdColumn};
use fluree_db_core::value_id::ObjKind;
use fluree_db_core::{ObjectBounds, Sid, BATCHED_JOIN_SIZE};
use fluree_db_indexer::run_index::BinaryIndexStore;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

/// Create a right-side scan operator for a join.
///
/// Uses `ScanOperator` which selects between `BinaryScanOperator` (streaming
/// cursor) and `RangeScanOperator` (range fallback) at `open()` time based
/// on the execution context.
fn make_right_scan(
    pattern: TriplePattern,
    object_bounds: &Option<ObjectBounds>,
    _ctx: &ExecutionContext<'_>,
) -> Box<dyn Operator> {
    Box::new(crate::binary_scan::ScanOperator::new(
        pattern,
        object_bounds.clone(),
        Vec::new(),
    ))
}

/// Position in a triple pattern (subject, predicate, object)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternPosition {
    Subject,
    Predicate,
    Object,
}

/// Binding instruction for right pattern
///
/// When executing the right pattern, substitute the value from left_col
/// into the specified position.
#[derive(Debug, Clone)]
pub struct BindInstruction {
    /// Position in right pattern to bind
    pub position: PatternPosition,
    /// Column index in left batch to get value from
    pub left_col: usize,
}

/// Unification check for shared variables
///
/// After right scan returns results, verify that the value at right_col
/// matches the value at left_col for shared variables.
#[derive(Debug, Clone)]
pub struct UnifyInstruction {
    /// Column index in left batch
    pub left_col: usize,
    /// Column index in right batch
    pub right_col: usize,
}

/// Identifies the source of a left row in `pending_output`.
#[derive(Debug, Clone)]
enum BatchRef {
    /// Left row is in `self.current_left_batch`
    Current,
    /// Left row is in `self.stored_left_batches[idx]`
    Stored(usize),
}

/// Check if the right pattern is eligible for batched subject join.
///
/// Eligible patterns have shape: `(?s, fixed_p, ?o)` where subject is bound
/// from the left, predicate is fixed (Term::Sid), object is a new unbound
/// variable, with no object bounds or datatype/language constraints.
fn is_batched_eligible(
    bind_instructions: &[BindInstruction],
    right_pattern: &TriplePattern,
    object_bounds: &Option<ObjectBounds>,
) -> bool {
    // Subject must have a BindInstruction (bound from left)
    let has_subject_bind = bind_instructions
        .iter()
        .any(|b| b.position == PatternPosition::Subject);
    // Predicate must be fixed (Term::Sid)
    let pred_fixed = matches!(&right_pattern.p, Term::Sid(_));
    // Object must be a variable
    let obj_is_var = matches!(&right_pattern.o, Term::Var(_));
    // No BindInstruction for Object (object is a new variable, not shared)
    let no_obj_bind = !bind_instructions
        .iter()
        .any(|b| b.position == PatternPosition::Object);
    // No object bounds (no FILTER range pushdown)
    let no_bounds = object_bounds.is_none();
    // No datatype or language constraints
    let no_dt = right_pattern.dt.is_none();
    let no_lang = right_pattern.lang.is_none();

    has_subject_bind && pred_fixed && obj_is_var && no_obj_bind && no_bounds && no_dt && no_lang
}

/// Bind-join operator for nested-loop join
///
/// For each row from the left operator, substitutes bound variables into
/// the right pattern, executes a scan, and combines results.
///
/// # Var Unification
///
/// When a variable appears in both left schema and right pattern output,
/// the join enforces that both sides produce the same value. Rows with
/// conflicting bindings are dropped.
///
/// # Invariant
///
/// We assume shared vars are never `Unbound` from the left side (except via
/// OPTIONAL which uses `Poisoned`). This is simpler than supporting "unbound
/// shared-vars" semantics.
pub struct NestedLoopJoinOperator {
    /// Left (driving) operator
    left: Box<dyn Operator>,
    /// Right pattern template (will be instantiated per left row)
    right_pattern: TriplePattern,
    /// Schema from left operator
    left_schema: Arc<[VarId]>,
    /// New variables introduced by right pattern (not in left schema)
    right_new_vars: Vec<VarId>,
    /// Combined output schema: left vars + new right vars
    combined_schema: Arc<[VarId]>,
    /// Instructions for binding left values into right pattern
    bind_instructions: Vec<BindInstruction>,
    /// Instructions for unification checks on shared vars
    unify_instructions: Vec<UnifyInstruction>,
    /// Current state
    state: OperatorState,
    /// Current left batch being processed
    current_left_batch: Option<Batch>,
    /// Current row index in left batch
    current_left_row: usize,
    /// Pending output rows (batch_ref, left_row_idx, right_batch)
    pending_output: VecDeque<(BatchRef, usize, Batch)>,
    /// Current row index within the front right_batch (for partial processing across calls)
    pending_right_row: usize,
    /// Optional object bounds for range filter pushdown
    object_bounds: Option<ObjectBounds>,
    /// Whether this join is eligible for batched subject join
    batched_eligible: bool,
    /// Column index in left batch for the subject binding (batched mode)
    subject_left_col: Option<usize>,
    /// The fixed predicate SID (batched mode)
    batched_predicate: Option<Sid>,
    /// Accumulated entries for batched processing: (stored_batch_idx, row_idx, subject_s_id)
    /// Stores the raw s_id directly to avoid dictionary round-trips with EncodedSid.
    batched_accumulator: Vec<(usize, usize, u64)>,
    /// Left batches retained for the batched flush
    stored_left_batches: Vec<Batch>,
    /// Pre-built output batches from the batched path, ready to emit
    batched_output: VecDeque<Batch>,
    /// Cached index into `stored_left_batches` for the currently active left batch.
    ///
    /// This prevents storing/cloning the same `current_left_batch` repeatedly.
    current_left_batch_stored_idx: Option<usize>,
}

impl NestedLoopJoinOperator {
    /// Create a new bind-join operator
    ///
    /// # Arguments
    ///
    /// * `left` - The left (driving) operator
    /// * `left_schema` - Schema of the left operator
    /// * `right_pattern` - Pattern to execute for each left row
    /// * `object_bounds` - Optional range bounds for object variable (filter pushdown)
    pub fn new(
        left: Box<dyn Operator>,
        left_schema: Arc<[VarId]>,
        right_pattern: TriplePattern,
        object_bounds: Option<ObjectBounds>,
    ) -> Self {
        // Build bind instructions: which left columns bind which right pattern positions
        let mut bind_instructions = Vec::new();
        let left_var_positions: std::collections::HashMap<VarId, usize> = left_schema
            .iter()
            .enumerate()
            .map(|(i, v)| (*v, i))
            .collect();

        // Check subject
        if let Term::Var(v) = &right_pattern.s {
            if let Some(&col) = left_var_positions.get(v) {
                bind_instructions.push(BindInstruction {
                    position: PatternPosition::Subject,
                    left_col: col,
                });
            }
        }

        // Check predicate
        if let Term::Var(v) = &right_pattern.p {
            if let Some(&col) = left_var_positions.get(v) {
                bind_instructions.push(BindInstruction {
                    position: PatternPosition::Predicate,
                    left_col: col,
                });
            }
        }

        // Check object
        if let Term::Var(v) = &right_pattern.o {
            if let Some(&col) = left_var_positions.get(v) {
                bind_instructions.push(BindInstruction {
                    position: PatternPosition::Object,
                    left_col: col,
                });
            }
        }

        // Determine right pattern output vars (vars that are still unbound after substitution)
        let right_output_vars: Vec<VarId> = right_pattern
            .variables()
            .into_iter()
            .filter(|v| !left_var_positions.contains_key(v))
            .collect();

        // Build combined schema: left schema + new right vars
        let mut combined = left_schema.to_vec();
        combined.extend(right_output_vars.iter().copied());
        let combined_schema: Arc<[VarId]> = Arc::from(combined.into_boxed_slice());

        // Build unify instructions for shared vars
        //
        // Variables that are in both left schema and right pattern need unification
        // UNLESS they are fully substituted into the pattern. However, substitution
        // depends on runtime binding types:
        // - Subject/Predicate: Only Sid bindings are substituted
        // - Object: Sid and Lit bindings are substituted
        //
        // We compute right_col based on right_output_vars (which excludes vars
        // expected to be substituted). At runtime, if substitution doesn't happen
        // (e.g., Lit at Subject position), the var remains in the right scan output
        // but at a different position than expected - this is handled by skipping
        // unification for such edge cases.
        //
        // Collect vars that WILL be substituted (based on position)
        // For Object position, we assume substitution will happen (Sid/Lit)
        // For Subject/Predicate, substitution requires Sid which we can't verify
        // at construction time, so we don't create unify instructions for these
        // positions when the var is in left schema (they have bind_instructions)
        let bound_vars: std::collections::HashSet<VarId> = bind_instructions
            .iter()
            .filter_map(|instr| match instr.position {
                PatternPosition::Subject => right_pattern.s.as_var(),
                PatternPosition::Predicate => right_pattern.p.as_var(),
                PatternPosition::Object => right_pattern.o.as_var(),
            })
            .collect();

        let mut unify_instructions = Vec::new();
        for var in right_pattern.variables() {
            // Skip vars that have bind_instructions - they will be substituted
            // (or if not substituted due to binding type, the row is handled
            // by the scan returning no results or the substitution leaving the var)
            if bound_vars.contains(&var) {
                continue;
            }

            if let Some(&left_col) = left_var_positions.get(&var) {
                // This var is shared but NOT bound - find its position in right output
                // Right output schema only includes non-bound vars
                if let Some(right_idx) = right_output_vars.iter().position(|v| *v == var) {
                    unify_instructions.push(UnifyInstruction {
                        left_col,
                        right_col: right_idx,
                    });
                }
            }
        }

        let batched_eligible =
            is_batched_eligible(&bind_instructions, &right_pattern, &object_bounds);
        let subject_left_col = if batched_eligible {
            bind_instructions
                .iter()
                .find(|b| b.position == PatternPosition::Subject)
                .map(|b| b.left_col)
        } else {
            None
        };
        let batched_predicate = if batched_eligible {
            match &right_pattern.p {
                Term::Sid(sid) => Some(sid.clone()),
                _ => None,
            }
        } else {
            None
        };

        Self {
            left,
            right_pattern,
            left_schema,
            right_new_vars: right_output_vars,
            combined_schema,
            bind_instructions,
            unify_instructions,
            state: OperatorState::Created,
            current_left_batch: None,
            current_left_row: 0,
            pending_output: VecDeque::new(),
            pending_right_row: 0,
            object_bounds,
            batched_eligible,
            subject_left_col,
            batched_predicate,
            batched_accumulator: Vec::new(),
            stored_left_batches: Vec::new(),
            batched_output: VecDeque::new(),
            current_left_batch_stored_idx: None,
        }
    }

    /// Check if any binding used in bind instructions is Poisoned
    ///
    /// If a left binding is Poisoned, the right pattern cannot match,
    /// so we should skip this left row entirely (produces no results).
    fn has_poisoned_binding(&self, left_batch: &Batch, row: usize) -> bool {
        self.bind_instructions
            .iter()
            .any(|instr| left_batch.get_by_col(row, instr.left_col).is_poisoned())
    }

    /// Check for invalid binding types on subject/predicate positions.
    ///
    /// Subject/predicate positions accept Sid and IriMatch bindings. If a shared var is
    /// bound to a literal, the pattern cannot match.
    fn has_invalid_binding_type(&self, left_batch: &Batch, row: usize) -> bool {
        self.bind_instructions.iter().any(|instr| {
            let binding = left_batch.get_by_col(row, instr.left_col);
            match instr.position {
                PatternPosition::Subject | PatternPosition::Predicate => {
                    // Unbound does not constrain; Sid, IriMatch, and encoded variants are valid.
                    // Anything else (Lit, raw Iri) cannot match subject/predicate.
                    !matches!(
                        binding,
                        Binding::Unbound
                            | Binding::Sid(_)
                            | Binding::IriMatch { .. }
                            | Binding::EncodedSid { .. }
                            | Binding::EncodedPid { .. }
                    )
                }
                PatternPosition::Object => {
                    // Raw IRIs from graph sources can't match native object values
                    // (they're not in the namespace table and can't be compared to Sids)
                    // Note: IriMatch is OK because it carries a SID.
                    // EncodedSid (refs) and EncodedLit (literals) are also valid.
                    matches!(binding, Binding::Iri(_))
                }
            }
        })
    }

    /// Substitute left row bindings into right pattern
    ///
    /// For IriMatch bindings, uses `Term::Iri` to carry the canonical IRI.
    /// The scan operator will encode this IRI for each target ledger's namespace
    /// table, enabling correct cross-ledger joins even when namespace tables differ.
    ///
    /// Note: Primarily used via substitute_pattern_with_store; kept for cases
    /// without binary store context.
    #[allow(dead_code)]
    fn substitute_pattern(&self, left_batch: &Batch, left_row: usize) -> TriplePattern {
        self.substitute_pattern_with_store(left_batch, left_row, None)
    }

    /// Substitute left row bindings into right pattern with optional store for encoded binding resolution.
    fn substitute_pattern_with_store(
        &self,
        left_batch: &Batch,
        left_row: usize,
        store: Option<&BinaryIndexStore>,
    ) -> TriplePattern {
        let mut pattern = self.right_pattern.clone();

        for instr in &self.bind_instructions {
            let binding = left_batch.get_by_col(left_row, instr.left_col);

            match instr.position {
                PatternPosition::Subject => {
                    match binding {
                        Binding::Sid(sid) => {
                            pattern.s = Term::Sid(sid.clone());
                        }
                        Binding::IriMatch { iri, .. } => {
                            // Use Term::Iri so scan can encode for each target ledger
                            pattern.s = Term::Iri(iri.clone());
                        }
                        Binding::EncodedSid { s_id } => {
                            // Resolve encoded s_id to IRI if store available
                            if let Some(store) = store {
                                if let Ok(iri) = store.resolve_subject_iri(*s_id) {
                                    pattern.s = Term::Iri(Arc::from(iri));
                                }
                            }
                            // Otherwise leave as variable
                        }
                        _ => {
                            // Leave as variable
                        }
                    }
                }
                PatternPosition::Predicate => {
                    match binding {
                        Binding::Sid(sid) => {
                            pattern.p = Term::Sid(sid.clone());
                        }
                        Binding::IriMatch { iri, .. } => {
                            // Use Term::Iri so scan can encode for each target ledger
                            pattern.p = Term::Iri(iri.clone());
                        }
                        Binding::EncodedPid { p_id } => {
                            // Resolve encoded p_id to IRI if store available
                            if let Some(store) = store {
                                if let Some(iri) = store.resolve_predicate_iri(*p_id) {
                                    pattern.p = Term::Iri(Arc::from(iri));
                                }
                            }
                            // Otherwise leave as variable
                        }
                        _ => {
                            // Leave as variable
                        }
                    }
                }
                PatternPosition::Object => {
                    match binding {
                        Binding::Sid(sid) => {
                            pattern.o = Term::Sid(sid.clone());
                        }
                        Binding::IriMatch { iri, .. } => {
                            // Use Term::Iri so scan can encode for each target ledger
                            pattern.o = Term::Iri(iri.clone());
                        }
                        Binding::Lit { val, .. } => {
                            pattern.o = Term::Value(val.clone());
                        }
                        Binding::EncodedLit {
                            o_kind,
                            o_key,
                            p_id,
                            ..
                        } => {
                            // Decode encoded literal if store available
                            if let Some(store) = store {
                                if let Ok(val) = store.decode_value(*o_kind, *o_key, *p_id) {
                                    pattern.o = Term::Value(val);
                                }
                            }
                            // Otherwise leave as variable
                        }
                        Binding::EncodedSid { s_id } => {
                            // Resolve encoded s_id to IRI if store available (object is a ref)
                            if let Some(store) = store {
                                if let Ok(iri) = store.resolve_subject_iri(*s_id) {
                                    pattern.o = Term::Iri(Arc::from(iri));
                                }
                            }
                            // Otherwise leave as variable
                        }
                        Binding::EncodedPid { .. } => {
                            // Predicate as object is unusual - leave as variable
                        }
                        Binding::Iri(_) => {
                            // Raw IRI from graph source can't be converted to native Term
                            // This case should not be reached due to has_invalid_binding_type check,
                            // but leave as variable defensively (row will produce no matches)
                        }
                        Binding::Unbound | Binding::Poisoned => {
                            // Leave as variable (Poisoned vars from OPTIONAL also remain unbound)
                        }
                        Binding::Grouped(_) => {
                            // Grouped bindings shouldn't appear in join codepaths
                            debug_assert!(false, "Grouped binding in join bind");
                            // Leave as variable
                        }
                    }
                }
            }
        }

        pattern
    }

    /// Check if left row bindings match right row bindings for shared vars
    ///
    /// Returns true if all shared vars have equal values on both sides.
    ///
    /// Uses `eq_for_join()` for same-ledger SID optimization when comparing
    /// `IriMatch` bindings from the same ledger.
    fn unify_check(
        &self,
        left_batch: &Batch,
        left_row: usize,
        right_batch: &Batch,
        right_row: usize,
    ) -> bool {
        self.unify_instructions.iter().all(|instr| {
            let left_val = left_batch.get_by_col(left_row, instr.left_col);
            let right_val = right_batch.get_by_col(right_row, instr.right_col);
            left_val.eq_for_join(right_val)
        })
    }

    /// Combine left row with right row into output row
    fn combine_rows(
        &self,
        left_batch: &Batch,
        left_row: usize,
        right_batch: &Batch,
        right_row: usize,
    ) -> Vec<Binding> {
        let right_schema = right_batch.schema();

        // Chain left columns with new right columns (skip shared vars already in left)
        (0..self.left_schema.len())
            .map(|col| left_batch.get_by_col(left_row, col).clone())
            .chain(self.right_new_vars.iter().map(|var| {
                right_schema
                    .iter()
                    .position(|v| v == var)
                    .map(|right_col| right_batch.get_by_col(right_row, right_col).clone())
                    .unwrap_or(Binding::Unbound)
            }))
            .collect()
    }
}

#[async_trait]
impl Operator for NestedLoopJoinOperator {
    fn schema(&self) -> &[VarId] {
        &self.combined_schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        // Open left operator
        self.left.open(ctx).await?;

        // Reset state for fresh execution
        self.pending_output.clear();
        self.pending_right_row = 0;
        self.current_left_batch = None;
        self.current_left_row = 0;

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

        // Check if batched mode is usable at runtime:
        // - Binary store must be available (batched path reads leaf files directly)
        // - Single-db mode (ActiveGraphs::Single), subjects are Binding::Sid
        // - Dataset mode with exactly one graph (ActiveGraphs::Many len==1), subjects are Binding::IriMatch
        let use_batched = self.batched_eligible
            && ctx.binary_store.is_some()
            && match ctx.active_graphs() {
                ActiveGraphs::Single => true,
                ActiveGraphs::Many(graphs) => graphs.len() == 1,
            };

        // Process until we have output or exhaust input
        loop {
            // 1. Pre-built output from batched flush
            if let Some(batch) = self.batched_output.pop_front() {
                return Ok(Some(batch));
            }

            // 2. Pending output from per-row path
            if !self.pending_output.is_empty() {
                return self.build_output_batch(ctx).await;
            }

            // 3. Need to process more left rows
            if self.current_left_batch.is_none() {
                match self.left.next_batch(ctx).await? {
                    Some(batch) => {
                        self.current_left_batch = Some(batch);
                        self.current_left_row = 0;
                        // New batch loaded: reset stored index cache.
                        self.current_left_batch_stored_idx = None;
                    }
                    None => {
                        // Left exhausted — flush any remaining accumulator
                        if !self.batched_accumulator.is_empty() {
                            self.flush_batched_accumulator_for_ctx(ctx).await?;
                            continue;
                        }
                        self.state = OperatorState::Exhausted;
                        return Ok(None);
                    }
                }
            }

            // Check if we've exhausted the current left batch
            let batch_len = self.current_left_batch.as_ref().unwrap().len();
            if self.current_left_row >= batch_len {
                self.current_left_batch = None;
                self.current_left_batch_stored_idx = None;
                continue;
            }

            let left_row = self.current_left_row;
            self.current_left_row += 1;

            // Check for poisoned bindings
            {
                let left_batch = self.current_left_batch.as_ref().unwrap();
                if self.has_poisoned_binding(left_batch, left_row) {
                    continue;
                }
                if self.has_invalid_binding_type(left_batch, left_row) {
                    continue;
                }
            }

            if use_batched {
                // Batched path: extract s_id directly to avoid dictionary round-trips.
                // - EncodedSid: use s_id directly (no lookup needed)
                // - Sid: call sid_to_s_id() once (cheap namespace lookup)
                let subject_col = self.subject_left_col.unwrap();
                let resolved_s_id: Option<u64> = {
                    let left_batch = self.current_left_batch.as_ref().unwrap();
                    let store = ctx.binary_store.as_deref();
                    match left_batch.get_by_col(left_row, subject_col) {
                        Binding::EncodedSid { s_id } => {
                            // Direct s_id - no dictionary lookup needed!
                            Some(*s_id)
                        }
                        Binding::Sid(sid) => {
                            // Resolve Sid to s_id (single lookup, not a round-trip)
                            store.and_then(|s| s.sid_to_s_id(sid).ok().flatten())
                        }
                        Binding::IriMatch { primary_sid, .. } => {
                            // Resolve primary_sid to s_id
                            store.and_then(|s| s.sid_to_s_id(primary_sid).ok().flatten())
                        }
                        _ => None,
                    }
                };

                if let Some(s_id) = resolved_s_id {
                    let batch_idx = self.ensure_current_batch_stored();
                    self.batched_accumulator.push((batch_idx, left_row, s_id));

                    if self.batched_accumulator.len() >= BATCHED_JOIN_SIZE {
                        self.flush_batched_accumulator_for_ctx(ctx).await?;
                    }
                } else {
                    // IriMatch, Unbound, etc. — fall back to per-row scan
                    let batch_idx = self.ensure_current_batch_stored();
                    let batch_ref = BatchRef::Stored(batch_idx);
                    let left_batch = self.stored_left_batches.last().unwrap();
                    let bound_pattern = self.substitute_pattern_with_store(
                        left_batch,
                        left_row,
                        ctx.binary_store.as_deref(),
                    );
                    let mut right_scan = make_right_scan(bound_pattern, &self.object_bounds, ctx);
                    right_scan.open(ctx).await?;
                    while let Some(right_batch) = right_scan.next_batch(ctx).await? {
                        if !right_batch.is_empty() {
                            self.pending_output.push_back((
                                batch_ref.clone(),
                                left_row,
                                right_batch,
                            ));
                        }
                    }
                    right_scan.close();
                }
            } else {
                // Non-batched path: existing per-row join
                let left_batch = self.current_left_batch.as_ref().unwrap();
                let bound_pattern = self.substitute_pattern_with_store(
                    left_batch,
                    left_row,
                    ctx.binary_store.as_deref(),
                );
                let mut right_scan = make_right_scan(bound_pattern, &self.object_bounds, ctx);
                right_scan.open(ctx).await?;
                while let Some(right_batch) = right_scan.next_batch(ctx).await? {
                    if !right_batch.is_empty() {
                        self.pending_output
                            .push_back((BatchRef::Current, left_row, right_batch));
                    }
                }
                right_scan.close();
            }
        }
    }

    fn close(&mut self) {
        self.left.close();
        self.current_left_batch = None;
        self.current_left_batch_stored_idx = None;
        self.pending_output.clear();
        self.pending_right_row = 0;
        self.batched_accumulator.clear();
        self.stored_left_batches.clear();
        self.batched_output.clear();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Estimate: left rows * some fanout factor
        self.left.estimated_rows().map(|n| n * 10)
    }
}

impl NestedLoopJoinOperator {
    /// Resolve the left batch for a given `BatchRef`.
    fn resolve_left_batch<'a>(&'a self, batch_ref: &BatchRef) -> Option<&'a Batch> {
        match batch_ref {
            BatchRef::Current => self.current_left_batch.as_ref(),
            BatchRef::Stored(idx) => self.stored_left_batches.get(*idx),
        }
    }

    /// Build output batch from pending results
    async fn build_output_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        let batch_size = ctx.batch_size;
        let mut output_columns: Vec<Vec<Binding>> = (0..self.combined_schema.len())
            .map(|_| Vec::with_capacity(batch_size))
            .collect();

        let mut rows_added = 0;

        while rows_added < batch_size && !self.pending_output.is_empty() {
            let (batch_ref, left_row, right_batch) = self.pending_output.front().unwrap();
            let left_row = *left_row;
            let right_batch_len = right_batch.len();

            let left_batch = match self.resolve_left_batch(batch_ref) {
                Some(b) => b,
                None => {
                    self.pending_output.pop_front();
                    self.pending_right_row = 0;
                    continue;
                }
            };

            // Process rows from this right batch, starting from where we left off
            let mut right_row = self.pending_right_row;
            while right_row < right_batch_len && rows_added < batch_size {
                // Unification check: shared vars must match
                if self.unify_check(left_batch, left_row, right_batch, right_row) {
                    // Combine and add to output
                    let combined = self.combine_rows(left_batch, left_row, right_batch, right_row);
                    for (col, val) in combined.into_iter().enumerate() {
                        output_columns[col].push(val);
                    }
                    rows_added += 1;
                }
                right_row += 1;
            }

            // Check if we've fully processed this right batch
            if right_row >= right_batch_len {
                // Fully processed - remove and reset
                self.pending_output.pop_front();
                self.pending_right_row = 0;
            } else {
                // Partially processed - save our position for next call
                self.pending_right_row = right_row;
            }
        }

        if rows_added == 0 {
            return Ok(None);
        }

        // Special-case: empty schema batches still need a correct row count.
        if self.combined_schema.is_empty() {
            return Ok(Some(Batch::empty_schema_with_len(rows_added)));
        }

        Ok(Some(Batch::new(
            self.combined_schema.clone(),
            output_columns,
        )?))
    }

    /// Ensure the current left batch is stored in `stored_left_batches` and
    /// return its index. If the current batch is already the last stored batch,
    /// returns the existing index.
    fn ensure_current_batch_stored(&mut self) -> usize {
        if let Some(idx) = self.current_left_batch_stored_idx {
            return idx;
        }

        let current = self.current_left_batch.as_ref().unwrap().clone();
        self.stored_left_batches.push(current);
        let idx = self.stored_left_batches.len() - 1;
        self.current_left_batch_stored_idx = Some(idx);
        idx
    }

    /// Flush batched accumulator using the appropriate db/overlay/to_t for the current context.
    ///
    /// - Single-db mode: uses ctx.db/ctx.overlay()/ctx.to_t
    /// - Dataset mode with exactly one graph: uses that graph's db/overlay/to_t
    async fn flush_batched_accumulator_for_ctx(
        &mut self,
        ctx: &ExecutionContext<'_>,
    ) -> Result<()> {
        if ctx.binary_store.is_none() {
            return Err(crate::error::QueryError::execution(
                "binary_store is required for batched joins — b-tree fallback has been removed",
            ));
        }

        let span = tracing::info_span!(
            "join_flush_batched_binary",
            accum_len = self.batched_accumulator.len(),
            batch_size = ctx.batch_size,
            to_t = ctx.to_t
        );
        let _g = span.enter();
        self.flush_batched_accumulator_binary(ctx).await
    }

    /// Clear all batched accumulator state after a flush or early return.
    fn clear_batched_state(&mut self) {
        self.batched_accumulator.clear();
        self.stored_left_batches.clear();
        self.current_left_batch_stored_idx = None;
    }

    /// Phase 1: Resolve the batched predicate SID to a binary-index p_id.
    ///
    /// Returns `None` if the predicate is not in the binary index (no results possible).
    fn resolve_batched_predicate(&self, store: &BinaryIndexStore) -> Option<u32> {
        let pred_sid = self.batched_predicate.as_ref().unwrap();
        store.sid_to_p_id(pred_sid)
    }

    /// Phase 2: Group accumulator entries by subject ID.
    ///
    /// Returns `(s_id → accumulator indices, sorted unique s_ids)`.
    /// The s_ids are already stored as raw u64 in the accumulator — no dictionary lookup needed.
    fn group_accumulator_by_subject(&self) -> (HashMap<u64, Vec<usize>>, Vec<u64>) {
        let mut s_id_to_accum: HashMap<u64, Vec<usize>> = HashMap::new();
        let mut unique_s_ids: Vec<u64> = Vec::new();
        for (accum_idx, (_, _, s_id)) in self.batched_accumulator.iter().enumerate() {
            s_id_to_accum.entry(*s_id).or_default().push(accum_idx);
            unique_s_ids.push(*s_id);
        }
        unique_s_ids.sort_unstable();
        unique_s_ids.dedup();
        (s_id_to_accum, unique_s_ids)
    }

    /// Phase 3: Compute the PSOT leaf range for a predicate + subject range.
    ///
    /// Restricts the scan to the predicate's PSOT partition AND the subject range
    /// of the current left batch. Without subject bounds we'd scan the entire
    /// predicate partition even when subjects fall into a narrow range.
    fn find_psot_leaf_range(
        branch: &fluree_db_indexer::run_index::branch::BranchManifest,
        g_id: u32,
        p_id: u32,
        min_s_id: u64,
        max_s_id: u64,
    ) -> std::ops::Range<usize> {
        use fluree_db_indexer::run_index::run_record::{cmp_psot, RunRecord};

        let min_key = RunRecord {
            g_id: g_id as u16,
            s_id: SubjectId::from_u64(min_s_id),
            p_id,
            dt: 0,
            o_kind: 0,
            op: 0,
            o_key: 0,
            t: 0,
            lang_id: 0,
            i: 0,
        };
        let max_key = RunRecord {
            g_id: g_id as u16,
            s_id: SubjectId::from_u64(max_s_id),
            p_id,
            dt: u16::MAX,
            o_kind: u8::MAX,
            op: u8::MAX,
            o_key: u64::MAX,
            t: u32::MAX,
            lang_id: u16::MAX,
            i: u32::MAX,
        };
        branch.find_leaves_in_range(&min_key, &max_key, cmp_psot)
    }

    /// Phase 4: Scan PSOT leaves, matching accumulated subjects and scattering results.
    ///
    /// Iterates candidate leaves, decodes region1/region2 (with cache), binary-searches
    /// for matching subjects within each leaflet's p_id segment, builds late-materialized
    /// bindings, and scatters them to accumulator positions.
    #[allow(clippy::too_many_arguments)]
    fn scan_leaves_into_scatter(
        &self,
        ctx: &ExecutionContext<'_>,
        store: &BinaryIndexStore,
        branch: &fluree_db_indexer::run_index::branch::BranchManifest,
        leaf_range: std::ops::Range<usize>,
        p_id: u32,
        unique_s_ids: &[u64],
        s_id_to_accum: &HashMap<u64, Vec<usize>>,
        scatter: &mut [Vec<Vec<Binding>>],
    ) -> Result<()> {
        use fluree_db_core::ListIndex;
        use fluree_db_indexer::run_index::leaf::read_leaf_header;
        use fluree_db_indexer::run_index::leaflet::{
            decode_leaflet_region1, decode_leaflet_region2, LeafletHeader,
        };
        use fluree_db_indexer::run_index::leaflet_cache::{
            CachedRegion1, CachedRegion2, LeafletCacheKey,
        };
        use fluree_db_indexer::run_index::run_record::RunSortOrder;
        use memmap2::Mmap;
        use std::sync::Arc as StdArc;
        use xxhash_rust::xxh3::xxh3_128;

        let cache = store.leaflet_cache();
        let total_leaf_count: usize = leaf_range.end.saturating_sub(leaf_range.start);

        let scan_span = tracing::info_span!(
            "join_flush_scan_spot",
            unique_subjects = unique_s_ids.len(),
            s_id_min = unique_s_ids.first().copied().unwrap_or(0),
            s_id_max = unique_s_ids.last().copied().unwrap_or(0),
            order = "psot",
            leaf_start = leaf_range.start,
            leaf_end = leaf_range.end,
            total_leaves = total_leaf_count
        );
        let scan_start = Instant::now();
        let _sg = scan_span.enter();

        let mut leaflets_scanned: u64 = 0;
        let mut region2_decodes: u64 = 0;
        let mut matched_rows: u64 = 0;
        let mut r1_cache_hits: u64 = 0;
        let mut r1_cache_misses: u64 = 0;
        let mut r2_cache_hits: u64 = 0;
        let mut r2_cache_misses: u64 = 0;

        // Coarse-grained phase timing (microseconds) to break down join_flush_scan_spot.
        // This is intentionally low-overhead (no per-row spans).
        let mut us_open_mmap: u64 = 0;
        let mut us_read_leaf_header: u64 = 0;
        let mut us_decode_r1: u64 = 0;
        let mut us_build_matches: u64 = 0;
        let mut us_decode_r2: u64 = 0;
        let mut us_emit_rows: u64 = 0;

        for leaf_idx in leaf_range {
            let leaf_entry = &branch.leaves[leaf_idx];
            let t_open = Instant::now();
            let file = std::fs::File::open(&leaf_entry.path)
                .map_err(|e| QueryError::Internal(format!("open leaf: {}", e)))?;
            let leaf_mmap = unsafe { Mmap::map(&file) }
                .map_err(|e| QueryError::Internal(format!("mmap leaf: {}", e)))?;
            us_open_mmap += t_open.elapsed().as_micros() as u64;

            let t_hdr = Instant::now();
            let header = read_leaf_header(&leaf_mmap)
                .map_err(|e| QueryError::Internal(format!("read leaf header: {}", e)))?;
            us_read_leaf_header += t_hdr.elapsed().as_micros() as u64;
            let leaf_id = xxh3_128(leaf_entry.content_hash.as_bytes());

            for (leaflet_idx, dir_entry) in header.leaflet_dir.iter().enumerate() {
                leaflets_scanned += 1;
                let end = dir_entry.offset as usize + dir_entry.compressed_len as usize;
                if end > leaf_mmap.len() {
                    break;
                }
                let leaflet_bytes = &leaf_mmap[dir_entry.offset as usize..end];

                let cache_key = LeafletCacheKey {
                    leaf_id,
                    leaflet_index: leaflet_idx as u8,
                    to_t: ctx.to_t,
                    epoch: 0,
                };

                // Region 1 (s_id, p_id, o_kind, o_key): cached across flushes and across runs.
                let (leaflet_header, s_ids, p_ids, o_kinds, o_keys) = if let Some(c) = cache {
                    if let Some(cached) = c.get_r1(&cache_key) {
                        r1_cache_hits += 1;
                        (
                            None,
                            cached.s_ids,
                            cached.p_ids,
                            cached.o_kinds,
                            cached.o_keys,
                        )
                    } else {
                        r1_cache_misses += 1;
                        let t_r1 = Instant::now();
                        let (lh, s_ids, p_ids, o_kinds, o_keys) = decode_leaflet_region1(
                            leaflet_bytes,
                            header.p_width,
                            RunSortOrder::Psot,
                        )
                        .map_err(|e| QueryError::Internal(format!("decode region1: {}", e)))?;
                        us_decode_r1 += t_r1.elapsed().as_micros() as u64;
                        let row_count = lh.row_count as usize;
                        let cached_r1 = CachedRegion1 {
                            s_ids: SubjectIdColumn::from_wide(
                                s_ids.into_iter().map(SubjectId::from_u64).collect(),
                            ),
                            p_ids: StdArc::from(p_ids.into_boxed_slice()),
                            o_kinds: StdArc::from(o_kinds.into_boxed_slice()),
                            o_keys: StdArc::from(o_keys.into_boxed_slice()),
                            row_count,
                        };
                        let s_ids = cached_r1.s_ids.clone();
                        let p_ids = cached_r1.p_ids.clone();
                        let o_kinds = cached_r1.o_kinds.clone();
                        let o_keys = cached_r1.o_keys.clone();
                        c.get_or_decode_r1(cache_key.clone(), || cached_r1);
                        (Some(lh), s_ids, p_ids, o_kinds, o_keys)
                    }
                } else {
                    let t_r1 = Instant::now();
                    let (lh, s_ids, p_ids, o_kinds, o_keys) =
                        decode_leaflet_region1(leaflet_bytes, header.p_width, RunSortOrder::Psot)
                            .map_err(|e| QueryError::Internal(format!("decode region1: {}", e)))?;
                    us_decode_r1 += t_r1.elapsed().as_micros() as u64;
                    (
                        Some(lh),
                        SubjectIdColumn::from_wide(
                            s_ids.into_iter().map(SubjectId::from_u64).collect(),
                        ),
                        StdArc::from(p_ids.into_boxed_slice()),
                        StdArc::from(o_kinds.into_boxed_slice()),
                        StdArc::from(o_keys.into_boxed_slice()),
                    )
                };

                let row_count = leaflet_header
                    .as_ref()
                    .map(|h| h.row_count as usize)
                    .unwrap_or_else(|| s_ids.len());
                debug_assert_eq!(s_ids.len(), row_count);
                debug_assert_eq!(p_ids.len(), row_count);
                debug_assert_eq!(o_kinds.len(), row_count);
                debug_assert_eq!(o_keys.len(), row_count);

                // Collect matching row indices using PSOT's `(p_id, s_id, ...)` ordering:
                // only consider subjects in this leaflet's subject range, then binary-search
                // their row ranges (avoids scanning every row).
                let t_match = Instant::now();
                let mut matches: Vec<(usize, u64)> = Vec::with_capacity(64); // (row_idx, s_id)

                // PSOT leaflets are sorted by p_id then s_id. Boundary leaflets may contain
                // adjacent predicates, so isolate the contiguous segment for our `p_id`.
                let p_start = p_ids.partition_point(|&x| x < p_id);
                let p_end = p_ids.partition_point(|&x| x <= p_id);
                if p_start == p_end {
                    continue;
                }
                let leaflet_s_min = s_ids.get(p_start).as_u64();
                let leaflet_s_max = s_ids.get(p_end - 1).as_u64();
                let subj_start = unique_s_ids.partition_point(|&x| x < leaflet_s_min);
                let subj_end = unique_s_ids.partition_point(|&x| x <= leaflet_s_max);
                if subj_start >= subj_end {
                    continue;
                }

                // Avoid allocating/copying the predicate segment's s_ids into a Vec<u64>.
                // This can be very expensive for high-cardinality predicates.
                #[inline]
                fn lower_bound_s_id(
                    s_ids: &SubjectIdColumn,
                    start: usize,
                    end: usize,
                    target: u64,
                ) -> usize {
                    let mut lo = start;
                    let mut hi = end;
                    while lo < hi {
                        let mid = (lo + hi) / 2;
                        if s_ids.get(mid).as_u64() < target {
                            lo = mid + 1;
                        } else {
                            hi = mid;
                        }
                    }
                    lo
                }

                #[inline]
                fn upper_bound_s_id(
                    s_ids: &SubjectIdColumn,
                    start: usize,
                    end: usize,
                    target: u64,
                ) -> usize {
                    let mut lo = start;
                    let mut hi = end;
                    while lo < hi {
                        let mid = (lo + hi) / 2;
                        if s_ids.get(mid).as_u64() <= target {
                            lo = mid + 1;
                        } else {
                            hi = mid;
                        }
                    }
                    lo
                }

                for &s_id in &unique_s_ids[subj_start..subj_end] {
                    // fast reject (should always be true, but keep it safe)
                    if !s_id_to_accum.contains_key(&s_id) {
                        continue;
                    }
                    let row_start = lower_bound_s_id(&s_ids, p_start, p_end, s_id);
                    let row_end = upper_bound_s_id(&s_ids, p_start, p_end, s_id);
                    if row_start == row_end {
                        continue;
                    }
                    for row in row_start..row_end {
                        // Within [p_start, p_end) the predicate matches by construction.
                        matches.push((row, s_id));
                    }
                }
                us_build_matches += t_match.elapsed().as_micros() as u64;
                if matches.is_empty() {
                    continue;
                }
                matched_rows += matches.len() as u64;

                // Need Region 2 for correct literal bindings (dt/lang/i/t)
                let r2 = if let Some(c) = cache {
                    if let Some(cached) = c.get_r2(&cache_key) {
                        r2_cache_hits += 1;
                        cached
                    } else {
                        r2_cache_misses += 1;
                        region2_decodes += 1;
                        // If R1 came from cache, we don't have `LeafletHeader` available.
                        // Re-read it from the leaflet bytes (fixed-size, no decompression).
                        let lh_owned;
                        let lh: &LeafletHeader = match leaflet_header.as_ref() {
                            Some(h) => h,
                            None => {
                                lh_owned =
                                    LeafletHeader::read_from(leaflet_bytes).map_err(|e| {
                                        QueryError::Internal(format!("read leaflet header: {}", e))
                                    })?;
                                &lh_owned
                            }
                        };
                        let t_r2 = Instant::now();
                        let decoded = decode_leaflet_region2(leaflet_bytes, lh, header.dt_width)
                            .map_err(|e| QueryError::Internal(format!("decode region2: {}", e)))?;
                        us_decode_r2 += t_r2.elapsed().as_micros() as u64;
                        let cached_r2 = CachedRegion2 {
                            dt_values: StdArc::from(decoded.dt_values.into_boxed_slice()),
                            t_values: StdArc::from(decoded.t_values.into_boxed_slice()),
                            lang: decoded.lang,
                            i_col: decoded.i_col,
                        };
                        c.get_or_decode_r2(cache_key.clone(), || cached_r2.clone());
                        cached_r2
                    }
                } else {
                    region2_decodes += 1;
                    let lh_owned;
                    let lh: &LeafletHeader = match leaflet_header.as_ref() {
                        Some(h) => h,
                        None => {
                            lh_owned = LeafletHeader::read_from(leaflet_bytes).map_err(|e| {
                                QueryError::Internal(format!("read leaflet header: {}", e))
                            })?;
                            &lh_owned
                        }
                    };
                    let t_r2 = Instant::now();
                    let decoded = decode_leaflet_region2(leaflet_bytes, lh, header.dt_width)
                        .map_err(|e| QueryError::Internal(format!("decode region2: {}", e)))?;
                    us_decode_r2 += t_r2.elapsed().as_micros() as u64;
                    CachedRegion2 {
                        dt_values: StdArc::from(decoded.dt_values.into_boxed_slice()),
                        t_values: StdArc::from(decoded.t_values.into_boxed_slice()),
                        lang: decoded.lang,
                        i_col: decoded.i_col,
                    }
                };

                let t_emit = Instant::now();
                for (row, s_id) in matches {
                    // Late materialization: do NOT call decode_value here.
                    // Emit EncodedSid for refs, EncodedLit for literals.
                    let t = r2.t_values[row] as i64;
                    let obj_binding = if o_kinds[row] == ObjKind::REF_ID.as_u8() {
                        // Object is a reference; emit EncodedSid for late materialization.
                        Binding::EncodedSid { s_id: o_keys[row] }
                    } else {
                        Binding::EncodedLit {
                            o_kind: o_kinds[row],
                            o_key: o_keys[row],
                            p_id: p_ids[row],
                            dt_id: r2.dt_values[row] as u16,
                            lang_id: r2.lang.as_ref().map_or(0, |c| c.get(row as u16)),
                            i_val: r2
                                .i_col
                                .as_ref()
                                .map_or(ListIndex::none().as_i32(), |c| c.get(row as u16)),
                            t,
                        }
                    };

                    if let Some(accum_indices) = s_id_to_accum.get(&s_id) {
                        for &accum_idx in accum_indices {
                            let (batch_idx, row_idx, _) = &self.batched_accumulator[accum_idx];
                            let left_batch = &self.stored_left_batches[*batch_idx];

                            let mut combined = Vec::with_capacity(self.combined_schema.len());
                            for col in 0..self.left_schema.len() {
                                combined.push(left_batch.get_by_col(*row_idx, col).clone());
                            }
                            for _ in &self.right_new_vars {
                                combined.push(obj_binding.clone());
                            }

                            scatter[accum_idx].push(combined);
                        }
                    }
                }
                us_emit_rows += t_emit.elapsed().as_micros() as u64;
            }
        }

        tracing::debug!(
            scan_ms = (scan_start.elapsed().as_secs_f64() * 1000.0) as u64,
            leaflets_scanned,
            region2_decodes,
            matched_rows,
            r1_cache_hits,
            r1_cache_misses,
            r2_cache_hits,
            r2_cache_misses,
            us_open_mmap,
            us_read_leaf_header,
            us_decode_r1,
            us_build_matches,
            us_decode_r2,
            us_emit_rows,
            "join batched binary scan complete"
        );

        Ok(())
    }

    /// Phase 5: Assemble scattered results into output batches in left-row order.
    ///
    /// Drains the scatter buffer (indexed by accumulator position) into columnar
    /// output batches, respecting `batch_size`. Clears accumulator state when done.
    fn emit_scatter_to_output(
        &mut self,
        mut scatter: Vec<Vec<Vec<Binding>>>,
        batch_size: usize,
    ) -> Result<()> {
        let accum_len = scatter.len();
        let num_cols = self.combined_schema.len();
        let mut output_columns: Vec<Vec<Binding>> = (0..num_cols)
            .map(|_| Vec::with_capacity(batch_size))
            .collect();
        let mut rows_added = 0;

        for scatter_item in scatter.iter_mut().take(accum_len) {
            for combined in scatter_item.drain(..) {
                for (col, val) in combined.into_iter().enumerate() {
                    output_columns[col].push(val);
                }
                rows_added += 1;

                if rows_added >= batch_size {
                    if num_cols == 0 {
                        self.batched_output
                            .push_back(Batch::empty_schema_with_len(rows_added));
                    } else {
                        let batch = Batch::new(self.combined_schema.clone(), output_columns)?;
                        self.batched_output.push_back(batch);
                    }
                    output_columns = (0..num_cols)
                        .map(|_| Vec::with_capacity(batch_size))
                        .collect();
                    rows_added = 0;
                }
            }
        }

        if rows_added > 0 {
            if num_cols == 0 {
                self.batched_output
                    .push_back(Batch::empty_schema_with_len(rows_added));
            } else {
                let batch = Batch::new(self.combined_schema.clone(), output_columns)?;
                self.batched_output.push_back(batch);
            }
        }

        self.clear_batched_state();
        Ok(())
    }

    /// Binary-index batched scan orchestrator.
    ///
    /// Implements a true batched scan over PSOT leaf files:
    /// 1. Resolve the fixed predicate to a binary-index p_id
    /// 2. Group accumulated subjects by s_id
    /// 3. Compute the PSOT leaf range for the subject range
    /// 4. Scan candidate leaves, matching subjects and scattering results
    /// 5. Emit output batches in left-row order
    ///
    /// This avoids opening/decompressing leaflets once per subject, which can be
    /// catastrophically slow for large left batches.
    async fn flush_batched_accumulator_binary(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        use fluree_db_indexer::run_index::run_record::RunSortOrder;

        if self.batched_accumulator.is_empty() {
            return Ok(());
        }

        let overall_start = Instant::now();
        let store = ctx.binary_store.as_ref().unwrap().clone();

        // Phase 1: Resolve predicate
        let p_id = match self.resolve_batched_predicate(&store) {
            Some(id) => id,
            None => {
                self.clear_batched_state();
                return Ok(());
            }
        };

        // Phase 2: Group by subject
        let (s_id_to_accum, unique_s_ids) = self.group_accumulator_by_subject();
        if unique_s_ids.is_empty() {
            self.clear_batched_state();
            return Ok(());
        }

        // Phase 3: Find leaf range
        let branch = store
            .branch_for_order(ctx.binary_g_id, RunSortOrder::Psot)
            .expect("PSOT index must exist for every graph");
        let leaf_range = Self::find_psot_leaf_range(
            branch,
            ctx.binary_g_id,
            p_id,
            unique_s_ids[0],
            *unique_s_ids.last().unwrap(),
        );

        // Phase 4: Scan leaves → scatter buffer
        let mut scatter: Vec<Vec<Vec<Binding>>> = vec![Vec::new(); self.batched_accumulator.len()];
        self.scan_leaves_into_scatter(
            ctx,
            &store,
            branch,
            leaf_range,
            p_id,
            &unique_s_ids,
            &s_id_to_accum,
            &mut scatter,
        )?;

        // Phase 5: Emit output batches
        self.emit_scatter_to_output(scatter, ctx.batch_size)?;

        tracing::debug!(
            total_ms = (overall_start.elapsed().as_secs_f64() * 1000.0) as u64,
            output_rows = self.batched_output.iter().map(|b| b.len()).sum::<usize>(),
            "join batched binary flush complete"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::Sid;

    #[test]
    fn test_bind_instruction_creation() {
        // Left schema: [?s, ?name]
        let left_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());

        // Right pattern: ?s :age ?age (shares ?s with left)
        let _right_pattern = TriplePattern::new(
            Term::Var(VarId(0)), // ?s - shared
            Term::Sid(Sid::new(100, "age")),
            Term::Var(VarId(2)), // ?age - new
        );

        // Create a mock left operator - we'll use this pattern to test bind instructions
        // For now, just verify the bind_instructions are built correctly
        let left_var_positions: std::collections::HashMap<VarId, usize> = left_schema
            .iter()
            .enumerate()
            .map(|(i, v)| (*v, i))
            .collect();

        // Check that ?s would be bound
        assert!(left_var_positions.contains_key(&VarId(0)));
        assert!(!left_var_positions.contains_key(&VarId(2))); // ?age not in left
    }

    #[test]
    fn test_combined_schema() {
        // Left: [?s]
        // Right: ?s :name ?name
        // Combined should be: [?s, ?name]

        let left_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let right_pattern = TriplePattern::new(
            Term::Var(VarId(0)), // ?s - shared
            Term::Sid(Sid::new(100, "name")),
            Term::Var(VarId(1)), // ?name - new
        );

        let left_var_positions: std::collections::HashMap<VarId, usize> = left_schema
            .iter()
            .enumerate()
            .map(|(i, v)| (*v, i))
            .collect();

        // Right pattern vars
        let right_vars = right_pattern.variables();
        assert_eq!(right_vars, vec![VarId(0), VarId(1)]);

        // New vars (not in left)
        let new_vars: Vec<VarId> = right_vars
            .into_iter()
            .filter(|v| !left_var_positions.contains_key(v))
            .collect();
        assert_eq!(new_vars, vec![VarId(1)]);

        // Combined schema
        let mut combined = left_schema.to_vec();
        combined.extend(new_vars);
        assert_eq!(combined, vec![VarId(0), VarId(1)]);
    }

    #[test]
    fn test_unify_instructions() {
        // Left: [?s, ?name]
        // Right: ?s :age ?age (only ?s is shared)
        // Unify should check: ?s in left col 0 matches ?s in right col 0

        let left_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let right_pattern = TriplePattern::new(
            Term::Var(VarId(0)), // ?s at right position 0
            Term::Sid(Sid::new(100, "age")),
            Term::Var(VarId(2)), // ?age at right position 1
        );

        let left_var_positions: std::collections::HashMap<VarId, usize> = left_schema
            .iter()
            .enumerate()
            .map(|(i, v)| (*v, i))
            .collect();

        // Build unify instructions
        let mut unify_instructions = Vec::new();
        for var in right_pattern.variables() {
            if let Some(&left_col) = left_var_positions.get(&var) {
                let right_vars = right_pattern.variables();
                if let Some(right_idx) = right_vars.iter().position(|v| *v == var) {
                    unify_instructions.push(UnifyInstruction {
                        left_col,
                        right_col: right_idx,
                    });
                }
            }
        }

        // Should have one unify instruction for ?s
        assert_eq!(unify_instructions.len(), 1);
        assert_eq!(unify_instructions[0].left_col, 0); // ?s is col 0 in left
        assert_eq!(unify_instructions[0].right_col, 0); // ?s is col 0 in right pattern output
    }

    #[test]
    fn test_has_poisoned_binding() {
        use fluree_db_core::FlakeValue;

        // Create a simple operator setup
        let left_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let right_pattern = TriplePattern::new(
            Term::Var(VarId(0)),
            Term::Sid(Sid::new(100, "age")),
            Term::Var(VarId(2)),
        );

        // Create a mock operator
        struct MockOp;
        #[async_trait]
        impl Operator for MockOp {
            fn schema(&self) -> &[VarId] {
                &[]
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(None)
            }
            fn close(&mut self) {}
        }

        let join = NestedLoopJoinOperator::new(
            Box::new(MockOp),
            left_schema.clone(),
            right_pattern,
            None, // No object bounds
        );

        // Create a batch with one row that has Poisoned in position 0 (used for binding)
        let columns_poisoned = vec![
            vec![Binding::Poisoned],
            vec![Binding::lit(
                FlakeValue::String("Alice".to_string()),
                Sid::new(2, "string"),
            )],
        ];
        let batch_poisoned = Batch::new(left_schema.clone(), columns_poisoned).unwrap();

        // Row 0 has Poisoned in position 0, which is used for subject binding
        assert!(join.has_poisoned_binding(&batch_poisoned, 0));

        // Create a batch with one row that has NO Poisoned bindings
        let columns_normal = vec![
            vec![Binding::Sid(Sid::new(1, "alice"))],
            vec![Binding::lit(
                FlakeValue::String("Alice".to_string()),
                Sid::new(2, "string"),
            )],
        ];
        let batch_normal = Batch::new(left_schema.clone(), columns_normal).unwrap();

        // Row 0 has no Poisoned bindings
        assert!(!join.has_poisoned_binding(&batch_normal, 0));

        // Create a batch where Poisoned is in position 1 (NOT used for binding)
        let columns_poisoned_unused = vec![
            vec![Binding::Sid(Sid::new(1, "alice"))],
            vec![Binding::Poisoned], // This is in position 1, not used for binding ?s
        ];
        let batch_poisoned_unused = Batch::new(left_schema, columns_poisoned_unused).unwrap();

        // Row 0 has Poisoned in position 1, but position 1 is not used for binding
        // (only position 0 is used for ?s)
        assert!(!join.has_poisoned_binding(&batch_poisoned_unused, 0));
    }

    /// Verify that shared vars at Object position with Lit bindings are substituted,
    /// not unified. This tests that the join correctly handles the case where a
    /// shared var is substituted into the pattern rather than requiring unification.
    ///
    /// When ?v is at Object position and has a Lit binding, it gets substituted
    /// into the pattern as a constant. The right scan then only outputs the
    /// remaining variables (?x in this case), and no unification check is needed.
    #[tokio::test]
    async fn test_join_substituted_var_no_unification() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::{Db, FlakeValue};

        // Minimal context (db is unused here; only batch_size matters).
        let db = Db::genesis("test/main");
        let mut vars = VarRegistry::new();
        let x = vars.get_or_insert("?x"); // VarId(0)
        let v = vars.get_or_insert("?v"); // VarId(1)
        let ctx = ExecutionContext::new(&db, &vars);

        // Left schema: [?v]
        let left_schema: Arc<[VarId]> = Arc::from(vec![v].into_boxed_slice());
        // Right pattern: ?x p ?v (shared ?v at Object position)
        let right_pattern =
            TriplePattern::new(Term::Var(x), Term::Sid(Sid::new(100, "p")), Term::Var(v));

        // Mock left operator (unused; we inject batches directly into join state).
        struct MockOp;
        #[async_trait]
        impl Operator for MockOp {
            fn schema(&self) -> &[VarId] {
                &[]
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(None)
            }
            fn close(&mut self) {}
        }

        let mut join = NestedLoopJoinOperator::new(
            Box::new(MockOp),
            left_schema.clone(),
            right_pattern,
            None, // No object bounds
        );

        // Verify that ?v is NOT in unify_instructions (it's substituted, not unified)
        assert!(
            join.unify_instructions.is_empty(),
            "No unify instructions expected when shared var at Object position has Lit binding"
        );

        // Left batch: ?v = 1
        let left_batch = Batch::new(
            left_schema,
            vec![vec![Binding::lit(FlakeValue::Long(1), Sid::new(2, "long"))]],
        )
        .unwrap();

        // Right batch from scan would only have ?x (since ?v was substituted)
        // The scan would have pattern: ?x p 1 (the literal value)
        let right_schema: Arc<[VarId]> = Arc::from(vec![x].into_boxed_slice());
        let right_batch = Batch::new(
            right_schema,
            vec![vec![Binding::lit(
                FlakeValue::Long(10),
                Sid::new(2, "long"),
            )]],
        )
        .unwrap();

        join.current_left_batch = Some(left_batch);
        join.pending_output
            .push_back((BatchRef::Current, 0, right_batch));

        // Should produce output since no unification check is needed
        let out = join.build_output_batch(&ctx).await.unwrap();
        assert!(
            out.is_some(),
            "Expected output when var is substituted (no unification)"
        );
        let batch = out.unwrap();
        assert_eq!(batch.len(), 1);
        // Output schema is [?v, ?x] (left vars + new right vars)
        assert_eq!(batch.schema(), &[v, x]);
    }

    /// Verify join produces correct output when unification IS needed.
    ///
    /// This tests a scenario where a shared variable is NOT substituted:
    /// - ?v appears in left schema
    /// - ?v appears at Subject position in right pattern
    /// - But left binding is Lit (not Sid), so substitution doesn't happen
    /// - Variable remains in right pattern, right scan outputs it
    /// - Unification check is needed
    ///
    /// However, since Subject position only substitutes Sid bindings, if the
    /// left binding is Lit, the scan will look for subject=?v (unbound) which
    /// returns ALL subjects. The unification then filters to matching values.
    ///
    /// Actually, the current implementation skips unify_instructions for ALL
    /// vars with bind_instructions, even if substitution might not happen.
    /// This test verifies the behavior when a shared var is at Subject position
    /// but has a Lit binding - currently this scenario isn't expected in practice
    /// because we typically have Sid bindings for Subject/Predicate joins.
    #[tokio::test]
    async fn test_join_multiple_new_vars() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::{Db, FlakeValue};

        let db = Db::genesis("test/main");
        let mut vars = VarRegistry::new();
        let s = vars.get_or_insert("?s"); // VarId(0)
        let x = vars.get_or_insert("?x"); // VarId(1)
        let y = vars.get_or_insert("?y"); // VarId(2)
        let ctx = ExecutionContext::new(&db, &vars);

        // Left schema: [?s] - a Sid (e.g., from VALUES or prior scan)
        let left_schema: Arc<[VarId]> = Arc::from(vec![s].into_boxed_slice());
        // Right pattern: ?s p ?x with separate object ?y
        // Actually let's test: ?x p ?y where neither is in left schema
        let right_pattern =
            TriplePattern::new(Term::Var(x), Term::Sid(Sid::new(100, "p")), Term::Var(y));

        struct MockOp;
        #[async_trait]
        impl Operator for MockOp {
            fn schema(&self) -> &[VarId] {
                &[]
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(None)
            }
            fn close(&mut self) {}
        }

        let mut join = NestedLoopJoinOperator::new(
            Box::new(MockOp),
            left_schema.clone(),
            right_pattern,
            None, // No object bounds
        );

        // No unify_instructions since no shared vars between left [?s] and right [?x, ?y]
        assert!(join.unify_instructions.is_empty());

        // Left batch: ?s = some:subject (a Sid)
        let left_batch = Batch::new(
            left_schema,
            vec![vec![Binding::Sid(Sid::new(1, "some:subject"))]],
        )
        .unwrap();

        // Right batch: ?x = some:other, ?y = 42
        let right_schema: Arc<[VarId]> = Arc::from(vec![x, y].into_boxed_slice());
        let right_batch = Batch::new(
            right_schema,
            vec![
                vec![Binding::Sid(Sid::new(1, "some:other"))],
                vec![Binding::lit(FlakeValue::Long(42), Sid::new(2, "long"))],
            ],
        )
        .unwrap();

        join.current_left_batch = Some(left_batch);
        join.pending_output
            .push_back((BatchRef::Current, 0, right_batch));

        let out = join
            .build_output_batch(&ctx)
            .await
            .unwrap()
            .expect("Expected output batch");

        assert_eq!(out.len(), 1);
        // Combined schema: left [?s] + new right vars [?x, ?y]
        assert_eq!(out.schema(), &[s, x, y]);
    }

    /// Regression test: in binary-index-only setups, a join where the RIGHT pattern
    /// has pushed-down object bounds must still be able to execute via the binary path.
    ///
    /// Previously, join right-scans with `object_bounds=Some(...)` could fall back to
    /// `ScanOperator` (B-tree), which yields 0 results in binary-only ingest mode.
    #[tokio::test]
    async fn test_join_right_scan_with_object_bounds_uses_binary_path() {
        use crate::execute::{build_operator_tree, run_operator, ExecutableQuery};
        use crate::ir::{Expression, FilterValue, Pattern};
        use crate::parse::ParsedQuery;
        use crate::pattern::Term;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::value_id::{ObjKey, ObjKind};
        use fluree_db_core::DatatypeDictId;
        use fluree_db_core::Db;
        use fluree_db_indexer::run_index::dict_io::{
            write_language_dict, write_predicate_dict, write_subject_index,
        };
        use fluree_db_indexer::run_index::global_dict::{
            LanguageTagDict, PredicateDict, SubjectDict,
        };
        use fluree_db_indexer::run_index::index_build::build_all_indexes;
        use fluree_db_indexer::run_index::run_file::write_run_file;
        use fluree_db_indexer::run_index::run_record::{cmp_for_order, RunRecord, RunSortOrder};
        use fluree_db_indexer::run_index::BinaryIndexStore;
        use fluree_graph_json_ld::ParsedContext;

        // --- Temp dirs ---
        let base =
            std::env::temp_dir().join(format!("fluree_test_binary_join_{}", uuid::Uuid::new_v4()));
        let run_dir = base.join("tmp_import");
        let spot_dir = run_dir.join("spot");
        let psot_dir = run_dir.join("psot");
        let post_dir = run_dir.join("post");
        let opst_dir = run_dir.join("opst");
        let index_dir = base.join("index");
        std::fs::create_dir_all(&spot_dir).unwrap();
        std::fs::create_dir_all(&psot_dir).unwrap();
        std::fs::create_dir_all(&post_dir).unwrap();
        std::fs::create_dir_all(&opst_dir).unwrap();
        std::fs::create_dir_all(&index_dir).unwrap();

        // --- Dictionaries ---
        // Predicates (id -> IRI) written as predicates.json (v2 root also inlines IRI -> p_id).
        let p_has_score = "http://example.com/score#hasScore";
        let p_refers = "http://example.com/score#refersInstance";
        let mut pred_dict = PredicateDict::new();
        let p_id_has_score = pred_dict.get_or_insert(p_has_score);
        let p_id_refers = pred_dict.get_or_insert(p_refers);
        let preds_by_id: Vec<&str> = (0..pred_dict.len())
            .map(|p_id| pred_dict.resolve(p_id).unwrap_or(""))
            .collect();
        std::fs::write(
            run_dir.join("predicates.json"),
            serde_json::to_vec(&preds_by_id).unwrap(),
        )
        .unwrap();

        // Datatypes.dict with reserved IDs up through DOUBLE (id=6).
        let mut dt_dict = PredicateDict::new();
        dt_dict.get_or_insert("@id"); // 0
        dt_dict.get_or_insert(fluree_vocab::xsd::STRING); // 1
        dt_dict.get_or_insert(fluree_vocab::xsd::BOOLEAN); // 2
        dt_dict.get_or_insert(fluree_vocab::xsd::INTEGER); // 3
        dt_dict.get_or_insert(fluree_vocab::xsd::LONG); // 4
        dt_dict.get_or_insert(fluree_vocab::xsd::DECIMAL); // 5
        dt_dict.get_or_insert(fluree_vocab::xsd::DOUBLE); // 6
        write_predicate_dict(&run_dir.join("datatypes.dict"), &dt_dict).unwrap();

        // Subjects forward + index + reverse.
        let subjects_fwd = run_dir.join("subjects.fwd");
        let mut subjects = SubjectDict::new(&subjects_fwd).unwrap();

        // Subjects: scores + concepts
        let score1 = "http://example.com/score#s1";
        let score2 = "http://example.com/score#s2";
        let concept1 = "http://example.com/concept#c1";
        let concept2 = "http://example.com/concept#c2";
        let ns: u16 = 100; // test namespace
        let s_id_score1 = subjects.get_or_insert(score1, ns).unwrap();
        let s_id_score2 = subjects.get_or_insert(score2, ns).unwrap();
        let s_id_concept1 = subjects.get_or_insert(concept1, ns).unwrap();
        let s_id_concept2 = subjects.get_or_insert(concept2, ns).unwrap();

        subjects.flush().unwrap();
        write_subject_index(
            &run_dir.join("subjects.idx"),
            subjects.forward_offsets(),
            subjects.forward_lens(),
        )
        .unwrap();
        fluree_db_indexer::run_index::dict_io::write_subject_sid_map(
            &run_dir.join("subjects.sids"),
            subjects.forward_sids(),
        )
        .unwrap();
        subjects
            .write_reverse_index(&run_dir.join("subjects.rev"))
            .unwrap();

        // Minimal languages dict (empty).
        write_language_dict(&run_dir.join("languages.dict"), &LanguageTagDict::new()).unwrap();

        // Write namespaces.json so the store's prefix_trie can decompose IRIs.
        // Without this, encode_iri assigns ns_code=0 for unknown prefixes, but
        // the reverse tree stores entries under ns_code=100.
        {
            let default_ns = fluree_db_core::default_namespace_codes();
            let mut ns_entries: Vec<serde_json::Value> = default_ns
                .iter()
                .map(|(&code, prefix)| serde_json::json!({"code": code, "prefix": prefix}))
                .collect();
            // Add the test namespace
            ns_entries.push(serde_json::json!({"code": ns, "prefix": "http://example.com/"}));
            std::fs::write(
                run_dir.join("namespaces.json"),
                serde_json::to_vec(&ns_entries).unwrap(),
            )
            .unwrap();
        }

        // --- Run records ---
        // Facts:
        // score1 hasScore 0.5 ; score1 refersInstance concept1
        // score2 hasScore 0.3 ; score2 refersInstance concept2
        // Filter (>0.4) should keep only score1 → concept1.
        let g_id: u16 = 0;
        let t: u32 = 1;
        let records = vec![
            // score1 hasScore 0.5
            RunRecord::new(
                g_id,
                SubjectId::from_u64(s_id_score1),
                p_id_has_score,
                ObjKind::NUM_F64,
                ObjKey::encode_f64(0.5).unwrap(),
                t,
                true,
                DatatypeDictId::DOUBLE.as_u16(),
                0,
                None,
            ),
            // score1 refersInstance concept1
            RunRecord::new(
                g_id,
                SubjectId::from_u64(s_id_score1),
                p_id_refers,
                ObjKind::REF_ID,
                ObjKey::encode_sid64(s_id_concept1),
                t,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
            // score2 hasScore 0.3
            RunRecord::new(
                g_id,
                SubjectId::from_u64(s_id_score2),
                p_id_has_score,
                ObjKind::NUM_F64,
                ObjKey::encode_f64(0.3).unwrap(),
                t,
                true,
                DatatypeDictId::DOUBLE.as_u16(),
                0,
                None,
            ),
            // score2 refersInstance concept2
            RunRecord::new(
                g_id,
                SubjectId::from_u64(s_id_score2),
                p_id_refers,
                ObjKind::REF_ID,
                ObjKey::encode_sid64(s_id_concept2),
                t,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
        ];

        // Write run files for all sort orders the query might choose.
        let lang = LanguageTagDict::new();
        let mut spot_records = records.clone();
        spot_records.sort_unstable_by(|a, b| cmp_for_order(RunSortOrder::Spot)(a, b));
        write_run_file(
            &spot_dir.join("run_00000.frn"),
            &spot_records,
            &lang,
            RunSortOrder::Spot,
            t,
            t,
        )
        .unwrap();

        let mut psot_records = records.clone();
        psot_records.sort_unstable_by(|a, b| cmp_for_order(RunSortOrder::Psot)(a, b));
        write_run_file(
            &psot_dir.join("run_00000.frn"),
            &psot_records,
            &lang,
            RunSortOrder::Psot,
            t,
            t,
        )
        .unwrap();

        let mut post_records = records.clone();
        post_records.sort_unstable_by(|a, b| cmp_for_order(RunSortOrder::Post)(a, b));
        write_run_file(
            &post_dir.join("run_00000.frn"),
            &post_records,
            &lang,
            RunSortOrder::Post,
            t,
            t,
        )
        .unwrap();

        // OPST contains only IRI references (ObjKind::REF_ID).
        let mut opst_records: Vec<RunRecord> = records
            .iter()
            .copied()
            .filter(|r| r.o_kind == ObjKind::REF_ID.as_u8())
            .collect();
        opst_records.sort_unstable_by(|a, b| cmp_for_order(RunSortOrder::Opst)(a, b));
        write_run_file(
            &opst_dir.join("run_00000.frn"),
            &opst_records,
            &lang,
            RunSortOrder::Opst,
            t,
            t,
        )
        .unwrap();

        // Build indexes for all orders.
        build_all_indexes(
            &run_dir,
            &index_dir,
            &[
                RunSortOrder::Spot,
                RunSortOrder::Psot,
                RunSortOrder::Post,
                RunSortOrder::Opst,
            ],
            64, // leaflet_rows
            2,  // leaflets_per_leaf
            0,  // zstd_level
            None,
        )
        .unwrap();

        let store = std::sync::Arc::new(BinaryIndexStore::load(&run_dir, &index_dir).unwrap());

        // --- Build query: (unbounded triple) JOIN (bounded triple) + FILTER ---
        let mut vars = VarRegistry::new();
        let v_score = vars.get_or_insert("?score");
        let v_concept = vars.get_or_insert("?concept");
        let v_score_v = vars.get_or_insert("?scoreV");

        let tp_refers = TriplePattern::new(
            Term::Var(v_score),
            Term::Sid(Sid::new(0, p_refers)),
            Term::Var(v_concept),
        );
        let tp_has_score = TriplePattern::new(
            Term::Var(v_score),
            Term::Sid(Sid::new(0, p_has_score)),
            Term::Var(v_score_v),
        );
        let filter = Expression::gt(
            Expression::Var(v_score_v),
            Expression::Const(FilterValue::Double(0.4)),
        );

        let mut pq = ParsedQuery::new(ParsedContext::default());
        pq.patterns = vec![
            Pattern::Triple(tp_refers),
            Pattern::Triple(tp_has_score),
            Pattern::Filter(filter),
        ];

        let exec = ExecutableQuery::simple(pq.clone());
        let operator = build_operator_tree(&pq, &exec.options, None).unwrap();

        let db = Db::genesis("test:main");
        let mut ctx = crate::context::ExecutionContext::new(&db, &vars).with_binary_store(store, 0);
        ctx.to_t = 1;

        let batches = run_operator(operator, &ctx).await.unwrap();
        let rows: usize = batches.iter().map(|b| b.len()).sum();
        assert_eq!(rows, 1, "expected only score1 to match filter");

        // Verify the surviving row binds concept1.
        let batch0 = &batches[0];
        let schema = batch0.schema();
        let concept_col = schema.iter().position(|v| *v == v_concept).unwrap();
        let bound = batch0.get_by_col(0, concept_col);
        match bound {
            Binding::Sid(sid) => assert!(sid.name.contains("concept#c1")),
            Binding::EncodedSid { s_id } => {
                // Late materialization: resolve the encoded s_id to verify it's concept1
                let iri = ctx
                    .binary_store
                    .as_ref()
                    .unwrap()
                    .resolve_subject_iri(*s_id)
                    .expect("should resolve encoded s_id");
                assert!(iri.contains("concept#c1"), "expected concept1, got {}", iri);
            }
            other => panic!("expected Sid or EncodedSid for ?concept, got {:?}", other),
        }

        // Best-effort cleanup
        let _ = std::fs::remove_dir_all(&base);
    }
}
