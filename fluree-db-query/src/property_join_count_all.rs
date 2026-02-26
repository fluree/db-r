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
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::triple::{Ref, Term, TriplePattern};
use async_trait::async_trait;
use fluree_db_binary_index::{
    sort_overlay_ops, BinaryCursor, BinaryFilter, BinaryGraphView, BinaryIndexStore, DecodedBatch,
    RunSortOrder,
};
use fluree_db_core::FlakeValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Fast-path: same-subject, N-predicate `COUNT(*)`.
pub struct PropertyJoinCountAllOperator {
    /// Triple patterns (must share the same subject var, bound predicate, var object).
    patterns: Vec<TriplePattern>,
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
        preds: Vec<(Ref, crate::var_registry::VarId)>,
        count_var: crate::var_registry::VarId,
    ) -> Self {
        let schema: Arc<[crate::var_registry::VarId]> =
            Arc::from(vec![count_var].into_boxed_slice());
        let patterns: Vec<TriplePattern> = preds
            .into_iter()
            .map(|(p, o)| TriplePattern::new(Ref::Var(subject_var), p, Term::Var(o)))
            .collect();
        Self {
            patterns,
            count_var,
            schema,
            state: OperatorState::Created,
            emitted: false,
            result: None,
            fallback: None,
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

        let Some((min_key, max_key)) = store
            .translate_range(None, Some(&pred_sid), None, order, g_id)
            .map_err(|e| QueryError::Internal(format!("translate_range: {}", e)))?
        else {
            return Ok(None);
        };

        let mut filter = BinaryFilter::new();
        filter.p_id = Some(p_id);

        // We do NOT need Region 2 (dt/lang/i/t) for counting; Region 1 is sufficient.
        let mut cursor = BinaryCursor::new(
            gv.clone_store(),
            order,
            g_id,
            &min_key,
            &max_key,
            filter,
            false,
        );
        cursor.set_to_t(ctx.to_t);

        // Overlay merge (novelty) if present.
        //
        // Note: this matches the binary scan path's behavior: overlay flakes are translated
        // to integer-ID space and merged by the cursor at the leaf level.
        if ctx.overlay.is_some() {
            // Translate overlay using a DictOverlay over the same binary store.
            let dn = ctx.dict_novelty.clone().unwrap_or_else(|| {
                Arc::new(fluree_db_core::dict_novelty::DictNovelty::new_uninitialized())
            });
            let dict_gv = BinaryGraphView::new(gv.clone_store(), g_id);
            let mut dict_ov = crate::dict_overlay::DictOverlay::new(dict_gv, dn);
            let mut ops = crate::binary_scan::translate_overlay_flakes(
                ctx.overlay(),
                &mut dict_ov,
                ctx.to_t,
                g_id,
            );
            if !ops.is_empty() {
                // Epoch is required for cache correctness.
                let epoch = ctx.overlay().epoch();
                cursor.set_epoch(epoch);
                sort_overlay_ops(&mut ops, order);
                cursor.set_overlay_ops(ops);
            } else {
                // Even without ops, still differentiate cache keys when overlay exists.
                cursor.set_epoch(ctx.overlay().epoch());
            }
        }

        Ok(Some(cursor))
    }

    async fn open_fallback(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
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
                match self.cursor.next_leaf() {
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

            let row_s = batch.s_ids[self.row];
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

        self.fallback = None;

        // Fast path is only available in the same execution mode as BinaryScanOperator.
        let use_fast = ctx.has_binary_store()
            && ctx
                .binary_store
                .as_ref()
                .is_some_and(|s| ctx.to_t >= s.base_t())
            && !ctx.history_mode
            && ctx.from_t.is_none()
            && !ctx.has_policy();

        if !use_fast {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::{AggregateFn, AggregateSpec};
    use crate::execute::{build_operator_tree, run_operator};
    use crate::ir::Pattern;
    use crate::options::QueryOptions;
    use crate::parse::{ParsedQuery, SelectMode};
    use crate::triple::{Ref, Term, TriplePattern};
    use crate::var_registry::VarRegistry;
    use fluree_db_binary_index::format::run_record::{cmp_for_order, RunRecord, RunSortOrder};
    use fluree_db_binary_index::BinaryIndexStore;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::{ObjKey, ObjKind};
    use fluree_db_core::{DatatypeDictId, LedgerSnapshot, Sid};
    use fluree_db_indexer::run_index::dict_io::{
        write_language_dict, write_predicate_dict, write_subject_index,
    };
    use fluree_db_indexer::run_index::global_dict::{LanguageTagDict, PredicateDict, SubjectDict};
    use fluree_db_indexer::run_index::index_build::build_all_indexes;
    use fluree_db_indexer::run_index::run_file::write_run_file;
    use fluree_graph_json_ld::ParsedContext;

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
            RunRecord::new(
                g_id,
                SubjectId::from_u64(s_id_1),
                p_id_has_sig,
                ObjKind::REF_ID,
                ObjKey::encode_sid64(s_id_oa),
                t,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
            RunRecord::new(
                g_id,
                SubjectId::from_u64(s_id_1),
                p_id_has_sig,
                ObjKind::REF_ID,
                ObjKey::encode_sid64(s_id_ob),
                t,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
            // s1 createdBy c1
            RunRecord::new(
                g_id,
                SubjectId::from_u64(s_id_1),
                p_id_created_by,
                ObjKind::REF_ID,
                ObjKey::encode_sid64(s_id_c1),
                t,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
            // s2 hasSignature oC
            RunRecord::new(
                g_id,
                SubjectId::from_u64(s_id_2),
                p_id_has_sig,
                ObjKind::REF_ID,
                ObjKey::encode_sid64(s_id_oc),
                t,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
            // s2 createdBy c2,c3,c4
            RunRecord::new(
                g_id,
                SubjectId::from_u64(s_id_2),
                p_id_created_by,
                ObjKind::REF_ID,
                ObjKey::encode_sid64(s_id_c2),
                t,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
            RunRecord::new(
                g_id,
                SubjectId::from_u64(s_id_2),
                p_id_created_by,
                ObjKind::REF_ID,
                ObjKey::encode_sid64(s_id_c3),
                t,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
            RunRecord::new(
                g_id,
                SubjectId::from_u64(s_id_2),
                p_id_created_by,
                ObjKind::REF_ID,
                ObjKey::encode_sid64(s_id_c4),
                t,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
            // s1 title t1,t2,t3
            RunRecord::new(
                g_id,
                SubjectId::from_u64(s_id_1),
                p_id_title,
                ObjKind::REF_ID,
                ObjKey::encode_sid64(s_id_t1),
                t,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
            RunRecord::new(
                g_id,
                SubjectId::from_u64(s_id_1),
                p_id_title,
                ObjKind::REF_ID,
                ObjKey::encode_sid64(s_id_t2),
                t,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
            RunRecord::new(
                g_id,
                SubjectId::from_u64(s_id_1),
                p_id_title,
                ObjKind::REF_ID,
                ObjKey::encode_sid64(s_id_t3),
                t,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
            // s2 title t4
            RunRecord::new(
                g_id,
                SubjectId::from_u64(s_id_2),
                p_id_title,
                ObjKind::REF_ID,
                ObjKey::encode_sid64(s_id_t4),
                t,
                true,
                DatatypeDictId::ID.as_u16(),
                0,
                None,
            ),
        ];

        let mut psot_records = records.clone();
        psot_records.sort_unstable_by(|a, b| cmp_for_order(RunSortOrder::Psot)(a, b));
        write_run_file(
            &psot_dir.join("run_00000.frn"),
            &psot_records,
            &LanguageTagDict::new(),
            RunSortOrder::Psot,
            t,
            t,
        )
        .unwrap();

        build_all_indexes(
            &run_dir,
            &index_dir,
            &[RunSortOrder::Psot],
            64, // leaflet_rows
            2,  // leaflets_per_leaf
            0,  // zstd_level
            None,
            false,
            false,
        )
        .unwrap();

        let store = Arc::new(BinaryIndexStore::load(&run_dir, &index_dir).unwrap());

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
            select: vec![count],
            patterns: vec![
                Pattern::Triple(tp1),
                Pattern::Triple(tp2),
                Pattern::Triple(tp3),
            ],
            options: QueryOptions::default(),
            select_mode: SelectMode::default(),
            construct_template: None,
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
