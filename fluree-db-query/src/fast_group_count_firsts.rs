use crate::binding::Binding;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::operator::BoxedOperator;
use crate::operator::{Operator, OperatorState};
use crate::triple::Term;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::{
    decode_leaflet_region1, decode_leaflet_region2, read_leaf_header, LeafletHeader, OverlayOp,
    RunRecord, RunSortOrder,
};
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::ObjKind;
use fluree_db_core::value_id::ValueTypeTag;
use fluree_db_core::StatsView;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
struct FactKey {
    s_id: u64,
    o_kind: u8,
    o_key: u64,
    dt: u16,
    lang_id: u16,
    i_val: i32,
}

fn overlay_ops_for_ctx(
    ctx: &ExecutionContext<'_>,
    gv: &fluree_db_binary_index::BinaryGraphView,
) -> Vec<OverlayOp> {
    let Some(overlay) = ctx.overlay else {
        return Vec::new();
    };
    let dn = ctx.dict_novelty.clone().unwrap_or_else(|| {
        Arc::new(fluree_db_core::dict_novelty::DictNovelty::new_uninitialized())
    });
    let dict_gv = fluree_db_binary_index::BinaryGraphView::new(gv.clone_store(), gv.g_id());
    let mut dict_ov = crate::dict_overlay::DictOverlay::new(dict_gv, dn);
    crate::binary_scan::translate_overlay_flakes(overlay, &mut dict_ov, ctx.to_t, gv.g_id())
}

#[inline]
fn group_key_for_fact(key: &FactKey, single_dt_id: Option<u16>) -> GroupKey {
    if key.o_kind == ObjKind::REF_ID.as_u8() {
        GroupKey::Ref(key.o_key)
    } else if let Some(dt_id) = single_dt_id {
        GroupKey::Lit {
            o_kind: key.o_kind,
            o_key: key.o_key,
            dt_id,
            lang_id: 0,
        }
    } else {
        GroupKey::Lit {
            o_kind: key.o_kind,
            o_key: key.o_key,
            dt_id: key.dt,
            lang_id: key.lang_id,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
enum GroupKey {
    Ref(u64),
    Lit {
        o_kind: u8,
        o_key: u64,
        dt_id: u16,
        lang_id: u16,
    },
}

/// Fast-path: `?s <p> ?o GROUP BY ?o (COUNT(?s) AS ?count)` with `ORDER BY DESC(?count) LIMIT k`.
///
/// Uses only per-leaflet uncompressed "FIRST" headers to skip decoding entire leaflets when
/// `FIRST(i).(p,o) == FIRST(i+1).(p,o)` (boundary-equality implies the whole leaflet is that (p,o) in POST order).
///
/// If stats indicate this predicate has exactly one datatype for this graph, the operator never
/// decodes Region 2 (dt/lang are treated as constant). Otherwise it falls back to decoding Region 2
/// and grouping by full RDF literal identity (dt/lang).
///
/// Requires:
/// - POST order access
pub struct PredicateGroupCountFirstsOperator {
    /// Output schema: [object_var, count_var]
    schema: Arc<[VarId]>,
    subject_var: VarId,
    object_var: VarId,
    count_var: VarId,
    /// Bound predicate reference (Sid or Iri).
    predicate: crate::triple::Ref,
    /// LIMIT k (top-k by count)
    limit: usize,
    /// Optional stats view used to detect single-datatype predicates.
    stats: Option<Arc<StatsView>>,
    /// Operator state
    state: OperatorState,
    /// Fallback operator for non-binary / overlay / policy / history contexts.
    fallback: Option<BoxedOperator>,
    /// Materialized results (already sorted and truncated to limit)
    results: Vec<(GroupKey, i64)>,
    /// Next result to emit
    pos: usize,
}

impl PredicateGroupCountFirstsOperator {
    pub fn new(
        subject_var: VarId,
        object_var: VarId,
        count_var: VarId,
        predicate: crate::triple::Ref,
        limit: usize,
        stats: Option<Arc<StatsView>>,
    ) -> Self {
        Self {
            schema: Arc::from(vec![object_var, count_var].into_boxed_slice()),
            subject_var,
            object_var,
            count_var,
            predicate,
            limit: limit.max(1),
            stats,
            state: OperatorState::Created,
            fallback: None,
            results: Vec::new(),
            pos: 0,
        }
    }

    fn read_leaflet_first(
        leaf_mmap: &[u8],
        dir: &fluree_db_binary_index::format::leaf::LeafletDirEntry,
    ) -> std::io::Result<LeafletHeader> {
        let end = dir.offset as usize + dir.compressed_len as usize;
        let leaflet_bytes = &leaf_mmap[dir.offset as usize..end];
        LeafletHeader::read_from(leaflet_bytes)
    }

    fn prefix_from_dir_or_leaflet(
        leaf_mmap: &[u8],
        dir: &fluree_db_binary_index::format::leaf::LeafletDirEntry,
    ) -> std::io::Result<(u32, u8, u64)> {
        if let (Some(k), Some(o)) = (dir.first_o_kind, dir.first_o_key) {
            Ok((dir.first_p_id, k, o))
        } else {
            // Older leaf versions: fall back to reading the leaflet header.
            let lh = Self::read_leaflet_first(leaf_mmap, dir)?;
            Ok(Self::prefix_from_leaflet_first(&lh))
        }
    }

    fn prefix_from_leaflet_first(lh: &LeafletHeader) -> (u32, u8, u64) {
        (lh.first_p_id, lh.first_o_kind, lh.first_o_key)
    }

    fn prefix_from_run_record(rec: &RunRecord) -> (u32, u8, u64) {
        (rec.p_id, rec.o_kind, rec.o_key)
    }

    fn add_count(map: &mut HashMap<GroupKey, i64>, key: GroupKey, add: i64) {
        *map.entry(key).or_insert(0) += add;
    }

    #[inline]
    fn io_to_query(where_: &'static str, e: std::io::Error) -> QueryError {
        QueryError::execution(format!("{where_}: {e}"))
    }

    #[inline]
    fn should_fallback(ctx: &ExecutionContext<'_>) -> bool {
        ctx.graph_view().is_none() || ctx.history_mode || ctx.policy_enforcer.is_some()
    }

    async fn open_fallback(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        use crate::aggregate::AggregateFn;
        use crate::binary_scan::{EmitMask, ScanOperator};
        use crate::group_aggregate::{GroupAggregateOperator, StreamingAggSpec};
        use crate::limit::LimitOperator;
        use crate::sort::{SortDirection, SortOperator, SortSpec};
        use crate::triple::{Ref, TriplePattern};

        let tp = TriplePattern::new(
            Ref::Var(self.subject_var),
            self.predicate.clone(),
            Term::Var(self.object_var),
        );

        let scan_emit = EmitMask {
            s: false,
            p: false,
            o: true,
        };
        let scan: BoxedOperator = Box::new(ScanOperator::new_with_emit_and_index(
            tp,
            None,
            Vec::new(),
            scan_emit,
            Some(fluree_db_core::IndexType::Post),
        ));

        let agg_specs = vec![StreamingAggSpec {
            function: AggregateFn::CountAll,
            input_col: None,
            output_var: self.count_var,
            distinct: false,
        }];
        let grouped: BoxedOperator = Box::new(GroupAggregateOperator::new(
            scan,
            vec![self.object_var],
            agg_specs,
            None,
            false,
        ));

        let sorted: BoxedOperator = Box::new(SortOperator::new(
            grouped,
            vec![SortSpec {
                var: self.count_var,
                direction: SortDirection::Descending,
            }],
        ));

        let mut limited: BoxedOperator = Box::new(LimitOperator::new(sorted, self.limit));
        limited.open(ctx).await?;
        self.fallback = Some(limited);
        Ok(())
    }
}

#[async_trait]
impl Operator for PredicateGroupCountFirstsOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            return Ok(());
        }
        self.state = OperatorState::Open;
        self.results.clear();
        self.pos = 0;
        self.fallback = None;

        if Self::should_fallback(ctx) {
            return self.open_fallback(ctx).await;
        }

        let Some(gv) = ctx.graph_view() else {
            // Defensive; should be handled by should_fallback().
            self.state = OperatorState::Exhausted;
            return Ok(());
        };
        let store = gv.clone_store();
        let g_id = gv.g_id();

        let p_id = match &self.predicate {
            crate::triple::Ref::Sid(sid) => store.sid_to_p_id(sid).ok_or_else(|| {
                QueryError::InvalidQuery("predicate not found in binary predicate dict".to_string())
            })?,
            crate::triple::Ref::Iri(iri) => store.find_predicate_id(iri).ok_or_else(|| {
                QueryError::InvalidQuery("predicate not found in binary predicate dict".to_string())
            })?,
            _ => {
                return Err(QueryError::InvalidQuery(
                    "predicate group-count fast-path requires a bound predicate".to_string(),
                ))
            }
        };

        let single_dt_id: Option<u16> = self.stats.as_ref().and_then(|stats| {
            let gp = stats.get_graph_property(g_id, p_id)?;
            if gp.datatypes.len() != 1 {
                return None;
            }
            let (tag, _count) = gp.datatypes[0];
            if tag == ValueTypeTag::LANG_STRING {
                return None;
            }
            tag.to_reserved_dict_id().map(|dt| dt.as_u16())
        });

        // Build a leaf range for POST where p_id is fixed and everything else is wildcard.
        let min_key = RunRecord {
            g_id,
            s_id: SubjectId::from_u64(0),
            p_id,
            dt: 0,
            o_kind: ObjKind::MIN.as_u8(),
            op: 0,
            o_key: 0,
            t: 0,
            lang_id: 0,
            i: 0,
        };
        let max_key = RunRecord {
            g_id,
            s_id: SubjectId::from_u64(u64::MAX),
            p_id,
            dt: u16::MAX,
            o_kind: ObjKind::MAX.as_u8(),
            op: 1,
            o_key: u64::MAX,
            t: u32::MAX,
            lang_id: u16::MAX,
            i: u32::MAX,
        };

        let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Post) else {
            self.state = OperatorState::Exhausted;
            return Ok(());
        };

        let cmp = fluree_db_binary_index::cmp_for_order(RunSortOrder::Post);
        let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);

        let mut counts: HashMap<GroupKey, i64> = HashMap::new();

        for leaf_idx in leaf_range.clone() {
            let leaf_entry = &branch.leaves[leaf_idx];
            let leaf_path = leaf_entry.resolved_path.as_ref().ok_or_else(|| {
                QueryError::execution(format!("leaf {} has no resolved path", leaf_entry.leaf_cid))
            })?;

            let file = match std::fs::File::open(leaf_path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    store
                        .ensure_index_leaf_cached(&leaf_entry.leaf_cid, leaf_path)
                        .map_err(|e| Self::io_to_query("ensure_index_leaf_cached", e))?;
                    std::fs::File::open(leaf_path)
                        .map_err(|e| Self::io_to_query("open leaf after cache", e))?
                }
                Err(e) => return Err(Self::io_to_query("open leaf", e)),
            };
            let leaf_mmap = unsafe {
                memmap2::Mmap::map(&file).map_err(|e| Self::io_to_query("mmap leaf", e))?
            };
            let leaf_header = read_leaf_header(&leaf_mmap)
                .map_err(|e| Self::io_to_query("read leaf header", e))?;

            let dirs = &leaf_header.leaflet_dir;
            for i in 0..dirs.len() {
                let dir = &dirs[i];
                let (p, o_kind, o_key) = Self::prefix_from_dir_or_leaflet(&leaf_mmap, dir)
                    .map_err(|e| Self::io_to_query("read leaflet first prefix", e))?;

                if p < p_id {
                    continue;
                }
                if p > p_id {
                    break;
                }

                // Determine the "next leaflet's FIRST" (same leaf, or next leaf's first_key).
                let next_prefix = if i + 1 < dirs.len() {
                    let next_dir = &dirs[i + 1];
                    Some(
                        Self::prefix_from_dir_or_leaflet(&leaf_mmap, next_dir)
                            .map_err(|e| Self::io_to_query("read next leaflet first prefix", e))?,
                    )
                } else if leaf_idx + 1 < leaf_range.end {
                    Some(Self::prefix_from_run_record(
                        &branch.leaves[leaf_idx + 1].first_key,
                    ))
                } else {
                    None
                };

                if let Some(dt_id) = single_dt_id {
                    if next_prefix == Some((p, o_kind, o_key)) {
                        // Boundary-equality implies this leaflet is entirely (p,o) in POST order.
                        let key = if o_kind == ObjKind::REF_ID.as_u8() {
                            GroupKey::Ref(o_key)
                        } else {
                            GroupKey::Lit {
                                o_kind,
                                o_key,
                                dt_id,
                                lang_id: 0,
                            }
                        };
                        Self::add_count(&mut counts, key, dir.row_count as i64);
                        continue;
                    }
                }

                let end = dir.offset as usize + dir.compressed_len as usize;
                let leaflet_bytes = &leaf_mmap[dir.offset as usize..end];
                let (lh2, _s_ids, p_ids, o_kinds, o_keys) =
                    decode_leaflet_region1(leaflet_bytes, leaf_header.p_width, RunSortOrder::Post)
                        .map_err(|e| Self::io_to_query("decode leaflet region1", e))?;

                if let Some(dt_id) = single_dt_id {
                    // Single-datatype mode: count from Region 1 only.
                    for row in 0..p_ids.len() {
                        if p_ids[row] != p_id {
                            continue;
                        }
                        let key = if o_kinds[row] == ObjKind::REF_ID.as_u8() {
                            GroupKey::Ref(o_keys[row])
                        } else {
                            GroupKey::Lit {
                                o_kind: o_kinds[row],
                                o_key: o_keys[row],
                                dt_id,
                                lang_id: 0,
                            }
                        };
                        Self::add_count(&mut counts, key, 1);
                    }
                } else {
                    // Fallback mode: decode Region 2 and group by (o_kind,o_key,dt_id,lang_id).
                    let r2 = decode_leaflet_region2(leaflet_bytes, &lh2, leaf_header.dt_width)
                        .map_err(|e| Self::io_to_query("decode leaflet region2", e))?;
                    for row in 0..p_ids.len() {
                        if p_ids[row] != p_id {
                            continue;
                        }
                        let key = if o_kinds[row] == ObjKind::REF_ID.as_u8() {
                            GroupKey::Ref(o_keys[row])
                        } else {
                            let dt_id = r2.dt_values[row] as u16;
                            let lang_id = r2.lang.as_ref().map(|c| c.get(row as u16)).unwrap_or(0);
                            GroupKey::Lit {
                                o_kind: o_kinds[row],
                                o_key: o_keys[row],
                                dt_id,
                                lang_id,
                            }
                        };
                        Self::add_count(&mut counts, key, 1);
                    }
                }
            }
        }

        // Merge novelty overlay deltas (when present).
        //
        // We keep the fast base counting via leaflet skipping, then adjust counts using
        // translated overlay ops. This is exact under the assumption that overlay
        // retractions only occur for facts that exist in the merged view at that time.
        if ctx.overlay.is_some() {
            let overlay_ops = overlay_ops_for_ctx(ctx, &gv);
            if !overlay_ops.is_empty() {
                let mut ops: Vec<(FactKey, i64, bool)> = overlay_ops
                    .into_iter()
                    .filter(|op| op.p_id == p_id)
                    .map(|op| {
                        (
                            FactKey {
                                s_id: op.s_id,
                                o_kind: op.o_kind,
                                o_key: op.o_key,
                                dt: op.dt,
                                lang_id: op.lang_id,
                                i_val: op.i_val,
                            },
                            op.t,
                            op.op,
                        )
                    })
                    .collect();
                // Group by fact identity and apply ops in time order (retract before assert for same t).
                ops.sort_unstable_by(|a, b| {
                    a.0.cmp(&b.0)
                        .then_with(|| a.1.cmp(&b.1))
                        .then_with(|| a.2.cmp(&b.2))
                });

                let mut idx = 0usize;
                while idx < ops.len() {
                    let key = ops[idx].0;
                    // If the first op is a retract, the fact must have existed before overlay.
                    let mut present = !ops[idx].2;

                    while idx < ops.len() && ops[idx].0 == key {
                        let op_is_assert = ops[idx].2;
                        if op_is_assert {
                            if !present {
                                present = true;
                                Self::add_count(
                                    &mut counts,
                                    group_key_for_fact(&key, single_dt_id),
                                    1,
                                );
                            }
                        } else if present {
                            present = false;
                            Self::add_count(
                                &mut counts,
                                group_key_for_fact(&key, single_dt_id),
                                -1,
                            );
                        }
                        idx += 1;
                    }
                }
            }
        }

        // Top-k by count desc.
        let mut rows: Vec<(GroupKey, i64)> = counts.into_iter().collect();
        rows.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        rows.truncate(self.limit);
        self.results = rows;

        Ok(())
    }

    async fn next_batch(
        &mut self,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<crate::binding::Batch>> {
        if let Some(op) = self.fallback.as_mut() {
            let batch = op.next_batch(ctx).await?;
            if batch.is_none() {
                self.state = OperatorState::Exhausted;
            }
            return Ok(batch);
        }
        if !self.state.can_next() {
            return Ok(None);
        }
        if self.pos >= self.results.len() {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }

        let Some(gv) = ctx.graph_view() else {
            return Err(QueryError::InvalidQuery(
                "predicate group-count fast-path requires binary index store".to_string(),
            ));
        };
        let store = gv.clone_store();
        let p_id = match &self.predicate {
            crate::triple::Ref::Sid(sid) => store.sid_to_p_id(sid).ok_or_else(|| {
                QueryError::InvalidQuery("predicate not found in binary predicate dict".to_string())
            })?,
            crate::triple::Ref::Iri(iri) => store.find_predicate_id(iri).ok_or_else(|| {
                QueryError::InvalidQuery("predicate not found in binary predicate dict".to_string())
            })?,
            _ => {
                return Err(QueryError::InvalidQuery(
                    "predicate group-count fast-path requires a bound predicate".to_string(),
                ))
            }
        };

        let batch_size = ctx.batch_size;
        let mut col_o: Vec<Binding> = Vec::with_capacity(batch_size);
        let mut col_c: Vec<Binding> = Vec::with_capacity(batch_size);

        while self.pos < self.results.len() && col_o.len() < batch_size {
            let (key, count) = self.results[self.pos];
            self.pos += 1;

            match key {
                GroupKey::Ref(s_id) => col_o.push(Binding::EncodedSid { s_id }),
                GroupKey::Lit {
                    o_kind,
                    o_key,
                    dt_id,
                    lang_id,
                } => col_o.push(Binding::EncodedLit {
                    o_kind,
                    o_key,
                    p_id,
                    dt_id,
                    lang_id,
                    i_val: fluree_db_core::ListIndex::none().as_i32(),
                    t: 0,
                }),
            }
            col_c.push(Binding::Lit {
                val: fluree_db_core::FlakeValue::Long(count),
                dt: fluree_db_core::Sid::xsd_integer(),
                lang: None,
                t: None,
                op: None,
                p_id: None,
            });
        }

        Ok(Some(crate::binding::Batch::new(
            self.schema.clone(),
            vec![col_o, col_c],
        )?))
    }

    fn close(&mut self) {
        self.state = OperatorState::Closed;
        if let Some(mut op) = self.fallback.take() {
            op.close();
        }
        self.results.clear();
        self.pos = 0;
    }

    fn estimated_rows(&self) -> Option<usize> {
        Some(self.limit)
    }
}

/// Fast-path: `SELECT (COUNT(?s) AS ?count) WHERE { ?s <p> <o> }` in POST order.
///
/// Uses only per-leaflet uncompressed "FIRST" headers to skip decoding entire leaflets when
/// `FIRST(i).(p,o) == FIRST(i+1).(p,o)` (boundary-equality implies the whole leaflet is that (p,o) in POST order).
///
/// Semantics: matches the current "loose" mode when no datatype/lang constraint is specified:
/// compare on Region1 `(o_kind, o_key)` only (dt/lang are ignored).
pub struct PredicateObjectCountFirstsOperator {
    /// Output schema: [count_var]
    schema: Arc<[VarId]>,
    subject_var: VarId,
    count_var: VarId,
    /// Bound predicate reference (Sid or Iri).
    predicate: crate::triple::Ref,
    /// Bound object term (Sid/Iri/Value).
    object: Term,
    /// Optional stats view (reserved for future scan-shape pruning).
    // Kept for: future scan-shape pruning (e.g. datatype-gated equality / pruning).
    // Use when: we extend this operator to apply StatsView-derived pruning beyond FIRST-based skipping.
    #[expect(dead_code)]
    stats: Option<Arc<StatsView>>,
    /// Operator state
    state: OperatorState,
    /// Fallback operator for non-binary / overlay / policy / history contexts.
    fallback: Option<BoxedOperator>,
    /// Computed count (materialized at open)
    count: i64,
    /// Whether the single row has been emitted
    emitted: bool,
}

impl PredicateObjectCountFirstsOperator {
    pub fn new(
        predicate: crate::triple::Ref,
        subject_var: VarId,
        object: Term,
        count_var: VarId,
        stats: Option<Arc<StatsView>>,
    ) -> Self {
        Self {
            schema: Arc::from(vec![count_var].into_boxed_slice()),
            subject_var,
            count_var,
            predicate,
            object,
            stats,
            state: OperatorState::Created,
            fallback: None,
            count: 0,
            emitted: false,
        }
    }

    fn read_leaflet_first(
        leaf_mmap: &[u8],
        dir: &fluree_db_binary_index::format::leaf::LeafletDirEntry,
    ) -> std::io::Result<LeafletHeader> {
        let end = dir.offset as usize + dir.compressed_len as usize;
        let leaflet_bytes = &leaf_mmap[dir.offset as usize..end];
        LeafletHeader::read_from(leaflet_bytes)
    }

    fn prefix_from_dir_or_leaflet(
        leaf_mmap: &[u8],
        dir: &fluree_db_binary_index::format::leaf::LeafletDirEntry,
    ) -> std::io::Result<(u32, u8, u64)> {
        if let (Some(k), Some(o)) = (dir.first_o_kind, dir.first_o_key) {
            Ok((dir.first_p_id, k, o))
        } else {
            // Older leaf versions: fall back to reading the leaflet header.
            let lh = Self::read_leaflet_first(leaf_mmap, dir)?;
            Ok((lh.first_p_id, lh.first_o_kind, lh.first_o_key))
        }
    }

    fn prefix_from_run_record(rec: &RunRecord) -> (u32, u8, u64) {
        (rec.p_id, rec.o_kind, rec.o_key)
    }

    #[inline]
    fn io_to_query(where_: &'static str, e: std::io::Error) -> QueryError {
        QueryError::execution(format!("{where_}: {e}"))
    }

    #[inline]
    fn should_fallback(ctx: &ExecutionContext<'_>) -> bool {
        ctx.graph_view().is_none() || ctx.history_mode || ctx.policy_enforcer.is_some()
    }

    async fn open_fallback(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        use crate::aggregate::AggregateFn;
        use crate::binary_scan::{EmitMask, ScanOperator};
        use crate::group_aggregate::{GroupAggregateOperator, StreamingAggSpec};
        use crate::triple::{Ref, TriplePattern};

        let tp = TriplePattern::new(
            Ref::Var(self.subject_var),
            self.predicate.clone(),
            self.object.clone(),
        );

        let scan_emit = EmitMask {
            s: true,
            p: false,
            o: false,
        };
        let scan: BoxedOperator =
            Box::new(ScanOperator::new_with_emit(tp, None, Vec::new(), scan_emit));

        let agg_specs = vec![StreamingAggSpec {
            function: AggregateFn::CountAll,
            input_col: None,
            output_var: self.count_var,
            distinct: false,
        }];
        let mut op: BoxedOperator = Box::new(GroupAggregateOperator::new(
            scan,
            vec![],
            agg_specs,
            None,
            false,
        ));
        op.open(ctx).await?;
        self.fallback = Some(op);
        Ok(())
    }
}

#[async_trait]
impl Operator for PredicateObjectCountFirstsOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            return Ok(());
        }
        self.state = OperatorState::Open;
        self.count = 0;
        self.emitted = false;
        self.fallback = None;

        if Self::should_fallback(ctx) {
            return self.open_fallback(ctx).await;
        }

        let Some(gv) = ctx.graph_view() else {
            self.state = OperatorState::Exhausted;
            return Ok(());
        };
        let store = gv.clone_store();
        let g_id = gv.g_id();

        let p_id = match &self.predicate {
            crate::triple::Ref::Sid(sid) => store.sid_to_p_id(sid).ok_or_else(|| {
                QueryError::InvalidQuery("predicate not found in binary predicate dict".to_string())
            })?,
            crate::triple::Ref::Iri(iri) => store.find_predicate_id(iri).ok_or_else(|| {
                QueryError::InvalidQuery("predicate not found in binary predicate dict".to_string())
            })?,
            _ => {
                return Err(QueryError::InvalidQuery(
                    "predicate-object count fast-path requires a bound predicate".to_string(),
                ))
            }
        };

        // Translate the bound object term into its Region1 `(o_kind, o_key)` encoding.
        let (target_kind, target_key): (u8, u64) = match &self.object {
            Term::Sid(sid) => match store
                .sid_to_s_id(sid)
                .map_err(|e| QueryError::execution(format!("sid_to_s_id for object: {e}")))?
            {
                Some(s_id) => (ObjKind::REF_ID.as_u8(), s_id),
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(());
                }
            },
            Term::Iri(iri) => match store.find_subject_id(iri).map_err(|e| {
                QueryError::execution(format!("find_subject_id for object IRI: {e}"))
            })? {
                Some(s_id) => (ObjKind::REF_ID.as_u8(), s_id),
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(());
                }
            },
            Term::Value(val) => {
                match gv.value_to_obj_pair_for_predicate(val, p_id).map_err(|e| {
                    QueryError::execution(format!("value_to_obj_pair_for_predicate: {e}"))
                })? {
                    Some((k, v)) => (k.as_u8(), v.as_u64()),
                    None => {
                        self.state = OperatorState::Exhausted;
                        return Ok(());
                    }
                }
            }
            Term::Var(_) => {
                return Err(QueryError::InvalidQuery(
                    "predicate-object count fast-path requires a bound object".to_string(),
                ))
            }
        };

        let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Post) else {
            self.state = OperatorState::Exhausted;
            return Ok(());
        };
        let cmp = fluree_db_binary_index::cmp_for_order(RunSortOrder::Post);

        // Range: fixed (p,o_kind,o_key), wildcard dt/lang/i/s/t.
        let min_key = RunRecord {
            g_id,
            s_id: SubjectId::from_u64(0),
            p_id,
            dt: 0,
            o_kind: target_kind,
            op: 0,
            o_key: target_key,
            t: 0,
            lang_id: 0,
            i: 0,
        };
        let max_key = RunRecord {
            g_id,
            s_id: SubjectId::from_u64(u64::MAX),
            p_id,
            dt: u16::MAX,
            o_kind: target_kind,
            op: 1,
            o_key: target_key,
            t: u32::MAX,
            lang_id: u16::MAX,
            i: u32::MAX,
        };

        let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);
        let target_prefix = (p_id, target_kind, target_key);

        let mut total: i64 = 0;

        for leaf_idx in leaf_range.clone() {
            let leaf_entry = &branch.leaves[leaf_idx];
            let leaf_path = leaf_entry.resolved_path.as_ref().ok_or_else(|| {
                QueryError::execution(format!("leaf {} has no resolved path", leaf_entry.leaf_cid))
            })?;

            let file = match std::fs::File::open(leaf_path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    store
                        .ensure_index_leaf_cached(&leaf_entry.leaf_cid, leaf_path)
                        .map_err(|e| Self::io_to_query("ensure_index_leaf_cached", e))?;
                    std::fs::File::open(leaf_path)
                        .map_err(|e| Self::io_to_query("open leaf after cache", e))?
                }
                Err(e) => return Err(Self::io_to_query("open leaf", e)),
            };

            let leaf_mmap = unsafe {
                memmap2::Mmap::map(&file).map_err(|e| Self::io_to_query("mmap leaf", e))?
            };
            let leaf_header = read_leaf_header(&leaf_mmap)
                .map_err(|e| Self::io_to_query("read leaf header", e))?;

            let dirs = &leaf_header.leaflet_dir;
            for i in 0..dirs.len() {
                let dir = &dirs[i];
                let prefix = Self::prefix_from_dir_or_leaflet(&leaf_mmap, dir)
                    .map_err(|e| Self::io_to_query("read leaflet first prefix", e))?;

                if prefix < target_prefix {
                    continue;
                }
                if prefix > target_prefix {
                    break;
                }

                // Determine the "next leaflet's FIRST" (same leaf, or next leaf's first_key).
                let next_prefix = if i + 1 < dirs.len() {
                    let next_dir = &dirs[i + 1];
                    Some(
                        Self::prefix_from_dir_or_leaflet(&leaf_mmap, next_dir)
                            .map_err(|e| Self::io_to_query("read next leaflet first prefix", e))?,
                    )
                } else if leaf_idx + 1 < leaf_range.end {
                    Some(Self::prefix_from_run_record(
                        &branch.leaves[leaf_idx + 1].first_key,
                    ))
                } else {
                    None
                };

                if next_prefix == Some(target_prefix) {
                    // Boundary-equality implies this leaflet is entirely (p,o).
                    total += dir.row_count as i64;
                    continue;
                }

                // Fallback for this leaflet: decode Region 1 and count exact (p,o_kind,o_key) matches.
                let end = dir.offset as usize + dir.compressed_len as usize;
                let leaflet_bytes = &leaf_mmap[dir.offset as usize..end];
                let (_lh2, _s_ids, p_ids, o_kinds, o_keys) =
                    decode_leaflet_region1(leaflet_bytes, leaf_header.p_width, RunSortOrder::Post)
                        .map_err(|e| Self::io_to_query("decode leaflet region1", e))?;

                for row in 0..p_ids.len() {
                    if p_ids[row] == p_id
                        && o_kinds[row] == target_kind
                        && o_keys[row] == target_key
                    {
                        total += 1;
                    }
                }
            }
        }

        // Merge novelty overlay deltas (when present).
        //
        // We keep the fast base counting via leaflet skipping, then adjust the scalar count
        // using translated overlay ops. This is exact under the assumption that overlay
        // retractions only occur for facts that exist in the merged view at that time, and
        // that overlay does not emit redundant assertions for already-present facts.
        if ctx.overlay.is_some() {
            let overlay_ops = overlay_ops_for_ctx(ctx, &gv);
            if !overlay_ops.is_empty() {
                let mut ops: Vec<(FactKey, i64, bool)> = overlay_ops
                    .into_iter()
                    .filter(|op| {
                        op.p_id == p_id && op.o_kind == target_kind && op.o_key == target_key
                    })
                    .map(|op| {
                        (
                            FactKey {
                                s_id: op.s_id,
                                o_kind: op.o_kind,
                                o_key: op.o_key,
                                dt: op.dt,
                                lang_id: op.lang_id,
                                i_val: op.i_val,
                            },
                            op.t,
                            op.op,
                        )
                    })
                    .collect();
                ops.sort_unstable_by(|a, b| {
                    a.0.cmp(&b.0)
                        .then_with(|| a.1.cmp(&b.1))
                        .then_with(|| a.2.cmp(&b.2))
                });

                let mut idx = 0usize;
                while idx < ops.len() {
                    let key = ops[idx].0;
                    let mut present = !ops[idx].2; // first retract => existed before overlay
                    while idx < ops.len() && ops[idx].0 == key {
                        let op_is_assert = ops[idx].2;
                        if op_is_assert {
                            if !present {
                                present = true;
                                total += 1;
                            }
                        } else if present {
                            present = false;
                            total -= 1;
                        }
                        idx += 1;
                    }
                }
            }
        }

        self.count = total;
        Ok(())
    }

    async fn next_batch(
        &mut self,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<crate::binding::Batch>> {
        if let Some(op) = self.fallback.as_mut() {
            let batch = op.next_batch(ctx).await?;
            if batch.is_none() {
                self.state = OperatorState::Exhausted;
            }
            return Ok(batch);
        }
        if !self.state.can_next() {
            return Ok(None);
        }
        if self.emitted {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }
        self.emitted = true;

        let col_c = vec![Binding::Lit {
            val: fluree_db_core::FlakeValue::Long(self.count),
            dt: fluree_db_core::Sid::xsd_integer(),
            lang: None,
            t: None,
            op: None,
            p_id: None,
        }];

        Ok(Some(crate::binding::Batch::new(
            self.schema.clone(),
            vec![col_c],
        )?))
    }

    fn close(&mut self) {
        self.state = OperatorState::Closed;
        if let Some(mut op) = self.fallback.take() {
            op.close();
        }
        self.count = 0;
        self.emitted = false;
    }

    fn estimated_rows(&self) -> Option<usize> {
        Some(1)
    }
}
