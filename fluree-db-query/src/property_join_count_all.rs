//! Fast-path operator for `COUNT(*)` over a same-subject multi-predicate star join.
//!
//! This targets queries of the form:
//!
//! ```text
//! SELECT (COUNT(*) AS ?count)
//! WHERE {
//!   ?s :p1 ?o1 .
//!   ?s :p2 ?o2 .
//!   ...
//! }
//! ```
//!
//! Semantics: SPARQL solution-set semantics imply a cartesian product across properties
//! per subject. Therefore:
//!
//!   COUNT(*) = sum_s Π_i count_pi(s)
//!
//! The standard `PropertyJoinOperator` produces the cartesian product rows and then
//! `COUNT(*)` counts them, which is correct but can be catastrophically expensive.
//!
//! This operator avoids materializing join rows entirely. It scans PSOT for each
//! predicate, produces a per-subject count stream, N-way merge-joins the streams, and
//! accumulates the sum-of-products.
//!
//! IMPORTANT: This operator is intentionally narrow and should only be selected by a
//! strict planner fast-path.

use crate::binding::{Batch, Binding};
use crate::context::{ExecutionContext, WellKnownDatatypes};
use crate::error::{QueryError, Result};
use crate::fast_path_common::{fast_path_store, normalize_pred_sid, PsotSubjectCountIter};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::triple::{Ref, Term, TriplePattern};
use async_trait::async_trait;
use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::{
    sort_overlay_ops, BinaryCursor, BinaryFilter, BinaryGraphView, BinaryIndexStore, ColumnBatch,
    ColumnProjection, ColumnSet, RunSortOrder,
};
// TODO: DecodedBatch was removed; code below still references it — needs migration to ColumnBatch.
type DecodedBatch = ColumnBatch;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::FlakeValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Fast-path: same-subject, N-predicate `COUNT(*)`.
pub struct PropertyJoinCountAllOperator {
    /// Triple patterns (must share the same subject var, bound predicate, var object).
    patterns: Vec<TriplePattern>,
    /// OPTIONAL groups (each a same-subject star of 1+ triple patterns).
    ///
    /// These do not filter subjects; they only multiply the per-subject multiplicity by
    /// `max(1, Π_i count_pi(s))` per OPTIONAL group.
    optional_groups: Vec<Vec<TriplePattern>>,
    /// Output var for the count binding.
    count_var: crate::var_registry::VarId,
    /// Output schema (single column: count var).
    schema: Arc<[crate::var_registry::VarId]>,
    /// Operator state.
    state: OperatorState,
    /// Emission guard (this operator yields exactly one row).
    emitted: bool,
    /// Computed count result.
    result: Option<i64>,
    /// Fallback operator for non-binary / incompatible execution contexts.
    fallback: Option<BoxedOperator>,
}

impl PropertyJoinCountAllOperator {
    pub fn new(
        subject_var: crate::var_registry::VarId,
        required: Vec<(Ref, crate::var_registry::VarId)>,
        optional_groups: Vec<Vec<(Ref, crate::var_registry::VarId)>>,
        count_var: crate::var_registry::VarId,
        fallback: Option<BoxedOperator>,
    ) -> Self {
        let schema: Arc<[crate::var_registry::VarId]> =
            Arc::from(vec![count_var].into_boxed_slice());
        let patterns: Vec<TriplePattern> = required
            .into_iter()
            .map(|(p, o)| TriplePattern::new(Ref::Var(subject_var), p, Term::Var(o)))
            .collect();
        let optional_groups: Vec<Vec<TriplePattern>> = optional_groups
            .into_iter()
            .map(|grp| {
                grp.into_iter()
                    .map(|(p, o)| TriplePattern::new(Ref::Var(subject_var), p, Term::Var(o)))
                    .collect()
            })
            .collect();
        Self {
            patterns,
            optional_groups,
            count_var,
            schema,
            state: OperatorState::Created,
            emitted: false,
            result: None,
            fallback,
        }
    }

    fn predicate_sid(store: &BinaryIndexStore, r: &Ref) -> Option<fluree_db_core::Sid> {
        match r {
            Ref::Sid(s) => Some(s.clone()),
            Ref::Iri(iri) => Some(store.encode_iri(iri)),
            Ref::Var(_) => None,
        }
    }

    fn build_cursor_for_predicate(
        ctx: &ExecutionContext<'_>,
        gv: &BinaryGraphView,
        pred_ref: &Ref,
    ) -> Result<Option<BinaryCursor>> {
        let order = RunSortOrder::Psot;
        let store = gv.store();
        let g_id = gv.g_id();

        let Some(pred_sid) = Self::predicate_sid(store, pred_ref) else {
            return Ok(None);
        };
        let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
            return Ok(None);
        };

        let Some(branch) = store.branch_for_order(g_id, order) else {
            return Ok(None);
        };
        let branch = Arc::new(branch.clone());

        let min_key = RunRecordV2 {
            s_id: SubjectId(0),
            o_key: 0,
            p_id,
            t: 0,
            o_i: 0,
            o_type: 0,
            g_id,
        };
        let max_key = RunRecordV2 {
            s_id: SubjectId(u64::MAX),
            o_key: u64::MAX,
            p_id,
            t: 0,
            o_i: u32::MAX,
            o_type: u16::MAX,
            g_id,
        };

        let filter = BinaryFilter {
            p_id: Some(p_id),
            ..Default::default()
        };

        // Only need s_id column for counting.
        let mut needed = ColumnSet::EMPTY;
        needed.insert(fluree_db_binary_index::format::column_block::ColumnId::SId);
        let projection = ColumnProjection {
            output: needed,
            internal: ColumnSet::EMPTY,
        };

        let mut cursor = BinaryCursor::new(
            gv.clone_store(),
            order,
            branch,
            &min_key,
            &max_key,
            filter,
            projection,
        );
        cursor.set_to_t(ctx.to_t);

        // Overlay merge (novelty) if present.
        if ctx.overlay.is_some() {
            let mut ops = crate::binary_scan::translate_overlay_flakes(
                ctx.overlay(),
                &gv.clone_store(),
                ctx.dict_novelty.as_ref(),
                ctx.to_t,
                g_id,
            );
            if !ops.is_empty() {
                let epoch = ctx.overlay().epoch();
                cursor.set_epoch(epoch);
                sort_overlay_ops(&mut ops, order);
                cursor.set_overlay_ops(ops);
            } else {
                cursor.set_epoch(ctx.overlay().epoch());
            }
        }

        Ok(Some(cursor))
    }

    /// Compute the star-join COUNT(*) using PSOT subject-count iterators directly.
    ///
    /// This avoids BinaryCursor setup/overlay plumbing and is only valid under the
    /// same conditions as other metadata-level fast paths (`fast_path_store` gating).
    fn count_psot_iters(
        store: &BinaryIndexStore,
        g_id: fluree_db_core::GraphId,
        required: &[TriplePattern],
        optional_groups: &[Vec<TriplePattern>],
    ) -> Result<u128> {
        // Required predicate IDs.
        let mut req_iters: Vec<PsotSubjectCountIter<'_>> = Vec::with_capacity(required.len());
        for tp in required {
            let sid = normalize_pred_sid(store, &tp.p)?;
            let Some(pid) = store.sid_to_p_id(&sid) else {
                // Missing required predicate => empty join.
                return Ok(0);
            };
            req_iters.push(PsotSubjectCountIter::new(store, g_id, pid)?);
        }

        // Optional groups: each group is a same-subject star; multiplier is max(1, Π counts).
        struct OptGroup<'a> {
            always_one: bool,
            iters: Vec<PsotSubjectCountIter<'a>>,
            cur: Vec<Option<(u64, u64)>>,
        }

        let mut opt_groups: Vec<OptGroup<'_>> = Vec::with_capacity(optional_groups.len());
        for grp in optional_groups {
            let mut always_one = false;
            let mut iters: Vec<PsotSubjectCountIter<'_>> = Vec::with_capacity(grp.len());
            for tp in grp {
                let sid = normalize_pred_sid(store, &tp.p)?;
                let Some(pid) = store.sid_to_p_id(&sid) else {
                    // Absent optional predicate => group never matches => multiplier 1.
                    always_one = true;
                    iters.clear();
                    break;
                };
                iters.push(PsotSubjectCountIter::new(store, g_id, pid)?);
            }
            let mut cur: Vec<Option<(u64, u64)>> = Vec::with_capacity(iters.len());
            for it in &mut iters {
                cur.push(it.next_group()?);
            }
            opt_groups.push(OptGroup {
                always_one,
                iters,
                cur,
            });
        }

        // Prime required cursors.
        let mut req_cur: Vec<Option<(u64, u64)>> = Vec::with_capacity(req_iters.len());
        for it in &mut req_iters {
            req_cur.push(it.next_group()?);
        }

        let mut total: u128 = 0;
        loop {
            if req_cur.iter().any(|c| c.is_none()) {
                break;
            }

            let max_s = req_cur
                .iter()
                .filter_map(|c| c.map(|(s, _)| s))
                .max()
                .unwrap();
            if req_cur.iter().all(|c| c.map(|(s, _)| s) == Some(max_s)) {
                // Required product at this subject.
                let mut product: u128 = req_cur.iter().map(|c| c.unwrap().1 as u128).product();

                // Multiply OPTIONAL group factors for this subject.
                for g in &mut opt_groups {
                    if g.always_one {
                        continue;
                    }
                    let mut g_prod: u128 = 1;
                    for i in 0..g.iters.len() {
                        while let Some((sid2, _)) = g.cur[i] {
                            if sid2 < max_s {
                                g.cur[i] = g.iters[i].next_group()?;
                                continue;
                            }
                            break;
                        }
                        let c = match g.cur[i] {
                            Some((sid2, c)) if sid2 == max_s => {
                                g.cur[i] = g.iters[i].next_group()?;
                                c
                            }
                            _ => 0u64,
                        };
                        if c == 0 {
                            g_prod = 0;
                            break;
                        }
                        g_prod = g_prod.saturating_mul(c as u128);
                    }
                    let mult = if g_prod == 0 { 1u128 } else { g_prod };
                    product = product.saturating_mul(mult);
                }

                total = total.saturating_add(product);

                // Advance required iterators.
                for (i, it) in req_iters.iter_mut().enumerate() {
                    req_cur[i] = it.next_group()?;
                }
            } else {
                // Advance smaller required subjects up to the current max.
                for (i, it) in req_iters.iter_mut().enumerate() {
                    if let Some((s_id, _)) = req_cur[i] {
                        if s_id < max_s {
                            req_cur[i] = it.next_group()?;
                        }
                    }
                }
            }
        }

        Ok(total)
    }

    fn open_v6(
        &mut self,
        ctx: &ExecutionContext<'_>,
        binary_index_store: &Arc<BinaryIndexStore>,
    ) -> Result<()> {
        let g_id = ctx.binary_g_id;
        let order = RunSortOrder::Psot;
        let effective_to_t = ctx.to_t;

        let mut streams: Vec<SubjectCountStreamV6> = Vec::with_capacity(self.patterns.len());
        for tp in &self.patterns {
            let Some(cursor) = build_cursor_for_predicate_v6(
                ctx,
                binary_index_store,
                g_id,
                &tp.p,
                order,
                effective_to_t,
            )?
            else {
                // Predicate absent → join result is empty → COUNT(*) = 0.
                self.result = Some(0);
                self.emitted = false;
                self.state = OperatorState::Open;
                return Ok(());
            };
            streams.push(SubjectCountStreamV6::new(cursor));
        }

        // OPTIONAL streams (predicates absent are treated as factor 1).
        struct OptPredStreamV6 {
            stream: SubjectCountStreamV6,
            cur: Option<(u64, u64)>,
        }
        struct OptGroupV6 {
            preds: Vec<OptPredStreamV6>,
            always_one: bool,
        }

        let mut opt_groups: Vec<OptGroupV6> = Vec::with_capacity(self.optional_groups.len());
        for grp in &self.optional_groups {
            let mut g = OptGroupV6 {
                preds: Vec::with_capacity(grp.len()),
                always_one: false,
            };
            for tp in grp {
                let Some(cursor) = build_cursor_for_predicate_v6(
                    ctx,
                    binary_index_store,
                    g_id,
                    &tp.p,
                    order,
                    effective_to_t,
                )?
                else {
                    // Absent predicate => group never matches => multiplier always 1.
                    g.always_one = true;
                    g.preds.clear();
                    break;
                };
                let mut s = SubjectCountStreamV6::new(cursor);
                let cur = s.next_subject_count()?;
                g.preds.push(OptPredStreamV6 { stream: s, cur });
            }
            opt_groups.push(g);
        }

        // N-way merge-join on subject ID (same algorithm as V5).
        let mut curr: Vec<Option<(u64, u64)>> = Vec::with_capacity(streams.len());
        for s in &mut streams {
            curr.push(s.next_subject_count()?);
        }

        let mut total: u128 = 0;
        loop {
            if curr.iter().any(|c| c.is_none()) {
                break;
            }
            let max_s = curr.iter().filter_map(|c| c.map(|(s, _)| s)).max().unwrap();
            if curr.iter().all(|c| c.map(|(s, _)| s) == Some(max_s)) {
                let mut product: u128 = curr.iter().map(|c| c.unwrap().1 as u128).product();
                // Multiply OPTIONAL group factors for this subject.
                for g in &mut opt_groups {
                    if g.always_one {
                        continue;
                    }
                    let mut g_prod: u128 = 1;
                    for p in &mut g.preds {
                        while let Some((sid2, _)) = p.cur {
                            if sid2 < max_s {
                                p.cur = p.stream.next_subject_count()?;
                                continue;
                            }
                            break;
                        }
                        let c = match p.cur {
                            Some((sid2, c)) if sid2 == max_s => {
                                p.cur = p.stream.next_subject_count()?;
                                c
                            }
                            _ => 0u64,
                        };
                        if c == 0 {
                            g_prod = 0;
                            break;
                        }
                        g_prod = g_prod.saturating_mul(c as u128);
                    }
                    let mult = if g_prod == 0 { 1u128 } else { g_prod };
                    product = product.saturating_mul(mult);
                }
                total = total.saturating_add(product);
                for (i, s) in streams.iter_mut().enumerate() {
                    curr[i] = s.next_subject_count()?;
                }
            } else {
                for (i, s) in streams.iter_mut().enumerate() {
                    if let Some((s_id, _)) = curr[i] {
                        if s_id < max_s {
                            curr[i] = s.next_subject_count()?;
                        }
                    }
                }
            }
        }

        self.result = Some(total.min(i64::MAX as u128) as i64);
        self.emitted = false;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn open_fallback(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        // Prefer a planner-provided fallback operator tree (must preserve OPTIONAL semantics).
        if let Some(fb) = &mut self.fallback {
            fb.open(ctx).await?;
            self.state = OperatorState::Open;
            return Ok(());
        }

        // Fallback: compute COUNT(*) by running the normal property join + streaming count-all.
        //
        // This keeps correctness in pre-index / multi-ledger / history / policy contexts,
        // while the planner can still pick this operator safely.
        let pj = crate::property_join::PropertyJoinOperator::new(
            self.patterns.as_slice(),
            HashMap::new(),
        );
        let agg_specs = vec![crate::group_aggregate::StreamingAggSpec {
            function: crate::aggregate::AggregateFn::CountAll,
            input_col: None,
            output_var: self.count_var,
            distinct: false,
        }];
        let mut op: BoxedOperator = Box::new(crate::group_aggregate::GroupAggregateOperator::new(
            Box::new(pj),
            vec![],
            agg_specs,
            None,
            false,
        ));
        op.open(ctx).await?;
        self.fallback = Some(op);
        self.state = OperatorState::Open;
        Ok(())
    }
}

/// Streaming per-subject count view over a PSOT predicate scan.
struct SubjectCountStream {
    cursor: BinaryCursor,
    current: Option<DecodedBatch>,
    row: usize,
}

impl SubjectCountStream {
    fn new(cursor: BinaryCursor) -> Self {
        Self {
            cursor,
            current: None,
            row: 0,
        }
    }

    fn next_subject_count(&mut self) -> Result<Option<(u64, u64)>> {
        let mut s_id: Option<u64> = None;
        let mut count: u64 = 0;

        loop {
            // Ensure we have a batch with remaining rows.
            if self
                .current
                .as_ref()
                .is_none_or(|b| self.row >= b.row_count)
            {
                match self.cursor.next_batch() {
                    Ok(Some(b)) => {
                        self.current = Some(b);
                        self.row = 0;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        return Err(QueryError::Internal(format!("binary cursor: {}", e)));
                    }
                }
            }

            let batch = self.current.as_ref().unwrap();
            if self.row >= batch.row_count {
                continue;
            }

            let row_s = batch.s_id.get(self.row);
            match s_id {
                None => {
                    s_id = Some(row_s);
                    count = 1;
                    self.row += 1;
                }
                Some(cur) if cur == row_s => {
                    count += 1;
                    self.row += 1;
                }
                Some(cur) => {
                    // New subject encountered; return prior.
                    return Ok(Some((cur, count)));
                }
            }
        }

        if let Some(cur) = s_id {
            Ok(Some((cur, count)))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl Operator for PropertyJoinCountAllOperator {
    fn schema(&self) -> &[crate::var_registry::VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        // Fast path is only available in the same execution mode as BinaryScanOperator.
        let allow_fast = !ctx.history_mode && ctx.from_t.is_none() && !ctx.has_policy();

        if !allow_fast {
            return self.open_fallback(ctx).await;
        }

        // Prefer metadata-level PSOT iterator fast path when allowed.
        // This is substantially faster than BinaryCursor-based counting for large star joins.
        if let Some(store) = fast_path_store(ctx) {
            let total = Self::count_psot_iters(
                store.as_ref(),
                ctx.binary_g_id,
                &self.patterns,
                &self.optional_groups,
            )?;
            self.result = Some(total.min(i64::MAX as u128) as i64);
            self.emitted = false;
            self.state = OperatorState::Open;
            self.fallback = None;
            return Ok(());
        }

        // Try V6 fast-path first.
        if let Some(binary_index_store) = ctx.binary_store.as_ref() {
            match self.open_v6(ctx, binary_index_store) {
                Ok(()) => {
                    // Fast path succeeded; ensure we don't delegate to fallback.
                    self.fallback = None;
                    return Ok(());
                }
                Err(_) => {
                    // V6 path failed — fall through to V5.
                }
            }
        }

        let use_v5 = ctx
            .binary_store
            .as_ref()
            .is_some_and(|s| ctx.to_t >= s.base_t());

        if !use_v5 {
            return self.open_fallback(ctx).await;
        }

        let gv = ctx.graph_view().unwrap();

        // Build cursors (PSOT predicate scans). If any predicate is absent from the index,
        // the join result is empty, so COUNT(*) = 0.
        let mut streams: Vec<SubjectCountStream> = Vec::with_capacity(self.patterns.len());
        for tp in &self.patterns {
            let Some(cursor) = Self::build_cursor_for_predicate(ctx, &gv, &tp.p)? else {
                self.result = Some(0);
                self.emitted = false;
                self.state = OperatorState::Open;
                return Ok(());
            };
            streams.push(SubjectCountStream::new(cursor));
        }

        struct OptPredStream {
            stream: SubjectCountStream,
            cur: Option<(u64, u64)>,
        }
        struct OptGroup {
            preds: Vec<OptPredStream>,
            always_one: bool,
        }

        let mut opt_groups: Vec<OptGroup> = Vec::with_capacity(self.optional_groups.len());
        for grp in &self.optional_groups {
            let mut g = OptGroup {
                preds: Vec::with_capacity(grp.len()),
                always_one: false,
            };
            for tp in grp {
                let Some(cursor) = Self::build_cursor_for_predicate(ctx, &gv, &tp.p)? else {
                    g.always_one = true;
                    g.preds.clear();
                    break;
                };
                let mut s = SubjectCountStream::new(cursor);
                let cur = s.next_subject_count()?;
                g.preds.push(OptPredStream { stream: s, cur });
            }
            opt_groups.push(g);
        }

        let mut curr: Vec<Option<(u64, u64)>> = Vec::with_capacity(streams.len());
        for s in &mut streams {
            curr.push(s.next_subject_count()?);
        }

        let mut total: u128 = 0;
        loop {
            if curr.iter().any(|c| c.is_none()) {
                break;
            }

            // Advance smaller subjects up to the current max subject.
            let target = curr.iter().map(|c| c.unwrap().0).max().unwrap_or(0);
            let mut any_advanced = false;
            for (i, stream) in streams.iter_mut().enumerate() {
                while let Some((s_id, _)) = curr[i] {
                    if s_id < target {
                        curr[i] = stream.next_subject_count()?;
                        any_advanced = true;
                        if curr[i].is_none() {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                if curr[i].is_none() {
                    break;
                }
            }
            if curr.iter().any(|c| c.is_none()) {
                break;
            }
            if any_advanced {
                continue;
            }

            // All subjects are aligned.
            let s_id = curr[0].unwrap().0;
            debug_assert!(curr.iter().all(|c| c.unwrap().0 == s_id));
            let mut prod: u128 = 1;
            for c in &curr {
                prod = prod.saturating_mul(c.unwrap().1 as u128);
            }
            for g in &mut opt_groups {
                if g.always_one {
                    continue;
                }
                let mut g_prod: u128 = 1;
                for p in &mut g.preds {
                    while let Some((sid2, _)) = p.cur {
                        if sid2 < s_id {
                            p.cur = p.stream.next_subject_count()?;
                            continue;
                        }
                        break;
                    }
                    let c = match p.cur {
                        Some((sid2, c)) if sid2 == s_id => {
                            p.cur = p.stream.next_subject_count()?;
                            c
                        }
                        _ => 0u64,
                    };
                    if c == 0 {
                        g_prod = 0;
                        break;
                    }
                    g_prod = g_prod.saturating_mul(c as u128);
                }
                let mult = if g_prod == 0 { 1u128 } else { g_prod };
                prod = prod.saturating_mul(mult);
            }
            total = total.saturating_add(prod);

            // Advance all.
            for (i, stream) in streams.iter_mut().enumerate() {
                curr[i] = stream.next_subject_count()?;
            }
        }

        if total > i64::MAX as u128 {
            return Err(QueryError::execution(
                "COUNT(*) overflowed i64 (xsd:long) in property-join fast-path".to_string(),
            ));
        }

        self.result = Some(total as i64);
        self.emitted = false;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if let Some(op) = &mut self.fallback {
            return op.next_batch(ctx).await;
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

        let dt = WellKnownDatatypes::new().xsd_long;
        let n = self.result.unwrap_or(0);
        let binding = Binding::lit(FlakeValue::Long(n), dt);

        self.emitted = true;
        Ok(Some(Batch::new(self.schema.clone(), vec![vec![binding]])?))
    }

    fn close(&mut self) {
        if let Some(op) = &mut self.fallback {
            op.close();
        }
        self.fallback = None;
        self.state = OperatorState::Closed;
        self.emitted = false;
        self.result = None;
    }

    fn estimated_rows(&self) -> Option<usize> {
        Some(1)
    }
}

// ---------------------------------------------------------------------------
// V6 fast-path helpers
// ---------------------------------------------------------------------------

/// Build a V6 PSOT cursor for a predicate, with overlay merge.
fn build_cursor_for_predicate_v6(
    ctx: &ExecutionContext<'_>,
    store: &Arc<BinaryIndexStore>,
    g_id: fluree_db_core::GraphId,
    pred_ref: &Ref,
    order: RunSortOrder,
    effective_to_t: i64,
) -> Result<Option<BinaryCursor>> {
    let pred_sid = match pred_ref {
        Ref::Sid(s) => s.clone(),
        Ref::Iri(i) => store.encode_iri(i),
        Ref::Var(_) => {
            return Err(QueryError::Internal(
                "property-join count requires bound predicates".to_string(),
            ))
        }
    };
    let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
        return Ok(None);
    };

    let Some(branch) = store.branch_for_order(g_id, order) else {
        return Ok(None);
    };
    let branch = Arc::new(branch.clone());

    let min_key = RunRecordV2 {
        s_id: SubjectId(0),
        o_key: 0,
        p_id,
        t: 0,
        o_i: 0,
        o_type: 0,
        g_id,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId(u64::MAX),
        o_key: u64::MAX,
        p_id,
        t: 0,
        o_i: u32::MAX,
        o_type: u16::MAX,
        g_id,
    };

    let filter = BinaryFilter {
        p_id: Some(p_id),
        ..Default::default()
    };

    // Only need s_id column for counting.
    let mut needed = ColumnSet::EMPTY;
    needed.insert(fluree_db_binary_index::format::column_block::ColumnId::SId);
    let projection = ColumnProjection {
        output: needed,
        internal: ColumnSet::EMPTY,
    };

    let mut cursor = BinaryCursor::new(
        Arc::clone(store),
        order,
        branch,
        &min_key,
        &max_key,
        filter,
        projection,
    );
    cursor.set_to_t(effective_to_t);

    // Overlay merge — pre-filter by predicate to avoid translating irrelevant flakes.
    if ctx.overlay.is_some() {
        let dn = ctx.dict_novelty.clone().unwrap_or_else(|| {
            Arc::new(fluree_db_core::dict_novelty::DictNovelty::new_uninitialized())
        });
        let mut ephemeral_preds = HashMap::new();
        let mut next_ep = store.predicate_count();
        let mut ops = Vec::new();

        ctx.overlay().for_each_overlay_flake(
            g_id,
            fluree_db_core::IndexType::Psot,
            None,
            None,
            true,
            effective_to_t,
            &mut |flake| {
                // Pre-filter: only translate flakes for this predicate.
                if flake.p != pred_sid {
                    return;
                }
                match crate::binary_scan::translate_one_flake_v3_pub(
                    flake,
                    store,
                    Some(&dn),
                    &mut ephemeral_preds,
                    &mut next_ep,
                ) {
                    Ok(op) => ops.push(op),
                    Err(e) => {
                        tracing::warn!(error = %e, "V6 property-join: failed to translate overlay flake");
                    }
                }
            },
        );

        if !ops.is_empty() {
            fluree_db_binary_index::read::types::sort_overlay_ops(&mut ops, order);
            cursor.set_overlay_ops(ops);
        }
        cursor.set_epoch(ctx.overlay().epoch());
    }

    Ok(Some(cursor))
}

/// V6 streaming per-subject count view over a PSOT V3 cursor.
struct SubjectCountStreamV6 {
    cursor: BinaryCursor,
    current: Option<fluree_db_binary_index::ColumnBatch>,
    row: usize,
}

impl SubjectCountStreamV6 {
    fn new(cursor: BinaryCursor) -> Self {
        Self {
            cursor,
            current: None,
            row: 0,
        }
    }

    fn next_subject_count(&mut self) -> Result<Option<(u64, u64)>> {
        let mut s_id: Option<u64> = None;
        let mut count: u64 = 0;

        loop {
            if self
                .current
                .as_ref()
                .is_none_or(|b| self.row >= b.row_count)
            {
                match self.cursor.next_batch() {
                    Ok(Some(b)) => {
                        self.current = Some(b);
                        self.row = 0;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        return Err(QueryError::Internal(format!("V6 cursor: {}", e)));
                    }
                }
            }

            let batch = self.current.as_ref().unwrap();
            if self.row >= batch.row_count {
                continue;
            }

            let row_s = batch.s_id.get(self.row);
            match s_id {
                None => {
                    s_id = Some(row_s);
                    count = 1;
                    self.row += 1;
                }
                Some(cur) if cur == row_s => {
                    count += 1;
                    self.row += 1;
                }
                Some(cur) => {
                    return Ok(Some((cur, count)));
                }
            }
        }

        if let Some(cur) = s_id {
            Ok(Some((cur, count)))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::{AggregateFn, AggregateSpec};
    use crate::execute::{build_operator_tree, run_operator};
    use crate::ir::Pattern;
    use crate::options::QueryOptions;
    use crate::parse::{ParsedQuery, QueryOutput};
    use crate::triple::{Ref, Term, TriplePattern};
    use crate::var_registry::VarRegistry;
    use fluree_db_binary_index::format::run_record::RunSortOrder;
    use fluree_db_binary_index::format::run_record_v2::{cmp_v2_for_order, RunRecordV2};
    use fluree_db_binary_index::BinaryIndexStore;
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::{LedgerSnapshot, Sid};
    use fluree_db_indexer::run_index::dict_io::{
        write_language_dict, write_predicate_dict, write_subject_index,
    };
    use fluree_db_indexer::run_index::global_dict::{LanguageTagDict, PredicateDict, SubjectDict};
    use fluree_db_indexer::run_index::index_build::{build_all_indexes, BuildAllConfig};
    use fluree_db_indexer::run_index::run_file::write_run_file;
    use fluree_graph_json_ld::ParsedContext;

    // TODO(V3 migration): This test builds a binary index from V2 RunRecords using
    // the on-disk pipeline. BinaryIndexStore::load (disk-based) was removed in the V3
    // migration; the store now loads from CAS via load_from_root_bytes. This test needs
    // a CAS-based loading helper to be re-enabled.
    #[ignore = "V3 migration: needs CAS-based BinaryIndexStore loading pipeline"]
    #[allow(unreachable_code, unused_variables, clippy::diverging_sub_expression)]
    #[tokio::test]
    async fn test_property_join_count_all_fast_path_correct() {
        // Build a tiny on-disk BinaryIndexStore with 3 predicates and 2 subjects:
        //
        // s1 hasSignature oA,oB (2)
        // s1 createdBy    c1    (1)
        // s1 title        t1,t2,t3 (3)
        // s2 hasSignature oC    (1)
        // s2 createdBy    c2,c3,c4 (3)
        // s2 title        t4 (1)
        //
        // COUNT(*) = 2*1*3 + 1*3*1 = 9

        let base =
            std::env::temp_dir().join(format!("fluree_test_pj_countall_{}", uuid::Uuid::new_v4()));
        let run_dir = base.join("tmp_import");
        let psot_dir = run_dir.join("psot");
        let index_dir = base.join("index");
        std::fs::create_dir_all(&psot_dir).unwrap();
        std::fs::create_dir_all(&index_dir).unwrap();

        // Predicates
        let p_has_sig = "http://example.com/dblp#hasSignature";
        let p_created_by = "http://example.com/dblp#createdBy";
        let p_title = "http://example.com/dblp#title";
        let mut pred_dict = PredicateDict::new();
        let p_id_has_sig = pred_dict.get_or_insert(p_has_sig);
        let p_id_created_by = pred_dict.get_or_insert(p_created_by);
        let p_id_title = pred_dict.get_or_insert(p_title);
        let preds_by_id: Vec<&str> = (0..pred_dict.len())
            .map(|p_id| pred_dict.resolve(p_id).unwrap_or(""))
            .collect();
        std::fs::create_dir_all(&run_dir).unwrap();
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

        // Subjects
        let subjects_fwd = run_dir.join("subjects.fwd");
        let mut subjects = SubjectDict::new(&subjects_fwd).unwrap();
        let ns: u16 = 100;
        let s1 = "http://example.com/dblp#s1";
        let s2 = "http://example.com/dblp#s2";
        let o_a = "http://example.com/dblp#oA";
        let o_b = "http://example.com/dblp#oB";
        let o_c = "http://example.com/dblp#oC";
        let c1 = "http://example.com/dblp#c1";
        let c2 = "http://example.com/dblp#c2";
        let c3 = "http://example.com/dblp#c3";
        let c4 = "http://example.com/dblp#c4";
        let t1 = "http://example.com/dblp#t1";
        let t2 = "http://example.com/dblp#t2";
        let t3 = "http://example.com/dblp#t3";
        let t4 = "http://example.com/dblp#t4";

        let s_id_1 = subjects.get_or_insert(s1, ns).unwrap();
        let s_id_2 = subjects.get_or_insert(s2, ns).unwrap();
        let s_id_oa = subjects.get_or_insert(o_a, ns).unwrap();
        let s_id_ob = subjects.get_or_insert(o_b, ns).unwrap();
        let s_id_oc = subjects.get_or_insert(o_c, ns).unwrap();
        let s_id_c1 = subjects.get_or_insert(c1, ns).unwrap();
        let s_id_c2 = subjects.get_or_insert(c2, ns).unwrap();
        let s_id_c3 = subjects.get_or_insert(c3, ns).unwrap();
        let s_id_c4 = subjects.get_or_insert(c4, ns).unwrap();
        let s_id_t1 = subjects.get_or_insert(t1, ns).unwrap();
        let s_id_t2 = subjects.get_or_insert(t2, ns).unwrap();
        let s_id_t3 = subjects.get_or_insert(t3, ns).unwrap();
        let s_id_t4 = subjects.get_or_insert(t4, ns).unwrap();

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

        // Minimal languages dict (empty)
        write_language_dict(&run_dir.join("languages.dict"), &LanguageTagDict::new()).unwrap();

        // namespaces.json so encode_iri uses ns=100 for http://example.com/*
        {
            let default_ns = fluree_db_core::default_namespace_codes();
            let mut ns_entries: Vec<serde_json::Value> = default_ns
                .iter()
                .map(|(&code, prefix)| serde_json::json!({"code": code, "prefix": prefix}))
                .collect();
            ns_entries.push(serde_json::json!({"code": ns, "prefix": "http://example.com/"}));
            std::fs::write(
                run_dir.join("namespaces.json"),
                serde_json::to_vec(&ns_entries).unwrap(),
            )
            .unwrap();
        }

        // Run records (PSOT only)
        let g_id: u16 = 0;
        let t: u32 = 1;
        let records = vec![
            // s1 hasSignature oA,oB
            RunRecordV2::new(
                g_id,
                SubjectId::from_u64(s_id_1),
                p_id_has_sig,
                OType::IRI_REF,
                s_id_oa,
                u32::MAX,
                t,
            ),
            RunRecordV2::new(
                g_id,
                SubjectId::from_u64(s_id_1),
                p_id_has_sig,
                OType::IRI_REF,
                s_id_ob,
                u32::MAX,
                t,
            ),
            // s1 createdBy c1
            RunRecordV2::new(
                g_id,
                SubjectId::from_u64(s_id_1),
                p_id_created_by,
                OType::IRI_REF,
                s_id_c1,
                u32::MAX,
                t,
            ),
            // s2 hasSignature oC
            RunRecordV2::new(
                g_id,
                SubjectId::from_u64(s_id_2),
                p_id_has_sig,
                OType::IRI_REF,
                s_id_oc,
                u32::MAX,
                t,
            ),
            // s2 createdBy c2,c3,c4
            RunRecordV2::new(
                g_id,
                SubjectId::from_u64(s_id_2),
                p_id_created_by,
                OType::IRI_REF,
                s_id_c2,
                u32::MAX,
                t,
            ),
            RunRecordV2::new(
                g_id,
                SubjectId::from_u64(s_id_2),
                p_id_created_by,
                OType::IRI_REF,
                s_id_c3,
                u32::MAX,
                t,
            ),
            RunRecordV2::new(
                g_id,
                SubjectId::from_u64(s_id_2),
                p_id_created_by,
                OType::IRI_REF,
                s_id_c4,
                u32::MAX,
                t,
            ),
            // s1 title t1,t2,t3
            RunRecordV2::new(
                g_id,
                SubjectId::from_u64(s_id_1),
                p_id_title,
                OType::IRI_REF,
                s_id_t1,
                u32::MAX,
                t,
            ),
            RunRecordV2::new(
                g_id,
                SubjectId::from_u64(s_id_1),
                p_id_title,
                OType::IRI_REF,
                s_id_t2,
                u32::MAX,
                t,
            ),
            RunRecordV2::new(
                g_id,
                SubjectId::from_u64(s_id_1),
                p_id_title,
                OType::IRI_REF,
                s_id_t3,
                u32::MAX,
                t,
            ),
            // s2 title t4
            RunRecordV2::new(
                g_id,
                SubjectId::from_u64(s_id_2),
                p_id_title,
                OType::IRI_REF,
                s_id_t4,
                u32::MAX,
                t,
            ),
        ];

        let mut psot_records = records.clone();
        psot_records.sort_unstable_by(|a, b| cmp_v2_for_order(RunSortOrder::Psot)(a, b));
        write_run_file(
            &psot_dir.join("run_00000.frn"),
            &psot_records,
            RunSortOrder::Psot,
            t,
            t,
        )
        .unwrap();

        build_all_indexes(&BuildAllConfig {
            base_run_dir: run_dir.clone(),
            index_dir: index_dir.clone(),
            leaflet_target_rows: 64,
            leaf_target_rows: 128,
            zstd_level: 0,
            skip_dedup: true,
            skip_history: true,
            g_id: 0,
            progress: None,
        })
        .unwrap();

        // TODO(V3 migration): BinaryIndexStore now loads from CAS via load_from_root_bytes.
        let _cache = Arc::new(fluree_db_binary_index::LeafletCache::with_max_mb(64));
        let store: Arc<BinaryIndexStore> =
            todo!("V3 migration: BinaryIndexStore now loads from CAS via load_from_root_bytes");

        // Build query: COUNT(*) over two property patterns on ?s
        let mut vars = VarRegistry::new();
        let s = vars.get_or_insert("?s");
        let o1 = vars.get_or_insert("?o1");
        let o2 = vars.get_or_insert("?o2");
        let o3 = vars.get_or_insert("?o3");
        let count = vars.get_or_insert("?count");

        let tp1 = TriplePattern::new(Ref::Var(s), Ref::Sid(Sid::new(0, p_has_sig)), Term::Var(o1));
        let tp2 = TriplePattern::new(
            Ref::Var(s),
            Ref::Sid(Sid::new(0, p_created_by)),
            Term::Var(o2),
        );
        let tp3 = TriplePattern::new(Ref::Var(s), Ref::Sid(Sid::new(0, p_title)), Term::Var(o3));

        let query = ParsedQuery {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::Select(vec![count]),
            patterns: vec![
                Pattern::Triple(tp1),
                Pattern::Triple(tp2),
                Pattern::Triple(tp3),
            ],
            options: QueryOptions::default(),
            graph_select: None,
        };
        let options = QueryOptions::default().with_aggregates(vec![AggregateSpec {
            function: AggregateFn::CountAll,
            input_var: None,
            output_var: count,
            distinct: false,
        }]);

        let op = build_operator_tree(&query, &options, None).unwrap();

        let snapshot = LedgerSnapshot::genesis("test:main");
        let mut ctx = ExecutionContext::new(&snapshot, &vars).with_binary_store(store, 0);
        ctx.to_t = 1;

        let batches = run_operator(op, &ctx).await.unwrap();
        let rows: usize = batches.iter().map(|b| b.len()).sum();
        assert_eq!(rows, 1);
        let b0 = &batches[0];
        let got = b0.get_by_col(0, 0);
        let Binding::Lit { val, .. } = got else {
            panic!("expected literal count binding, got {:?}", got);
        };
        assert_eq!(*val, FlakeValue::Long(9));

        let _ = std::fs::remove_dir_all(&base);
    }
}
