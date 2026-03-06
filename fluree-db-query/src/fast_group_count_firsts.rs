use crate::binding::Binding;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::operator::BoxedOperator;
use crate::operator::{Operator, OperatorState};
use crate::triple::Term;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::leaf_v3::{
    decode_leaf_dir_v3_with_base, decode_leaf_header_v3, LeafletDirEntryV3,
};
use fluree_db_binary_index::format::run_record_v2::{
    cmp_v2_for_order, read_ordered_key_v2, RunRecordV2,
};
use fluree_db_binary_index::read::column_loader::load_leaflet_columns;
use fluree_db_binary_index::{
    decode_leaflet_region1, decode_leaflet_region2, read_leaf_header, BinaryGraphView,
    BinaryIndexStore, BinaryIndexStoreV6, CachedRegion1, CachedRegion2, ColumnProjection,
    ColumnSet, LeafEntry, LeafletCacheKey, LeafletHeader, OverlayOp, RunRecord, RunSortOrder,
};
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::{SubjectId, SubjectIdColumn};
use fluree_db_core::value_id::ObjKind;
use fluree_db_core::value_id::ValueTypeTag;
use fluree_db_core::{GraphId, StatsView};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
struct FactKey {
    s_id: u64,
    o_kind: u8,
    o_key: u64,
    dt: u16,
    lang_id: u16,
    i_val: i32,
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

// ---------------------------------------------------------------------------
// Shared free functions (extracted from duplicate `impl` methods)
// ---------------------------------------------------------------------------

fn overlay_ops_for_ctx(ctx: &ExecutionContext<'_>, gv: &BinaryGraphView) -> Vec<OverlayOp> {
    let Some(overlay) = ctx.overlay else {
        return Vec::new();
    };
    let dn = ctx.dict_novelty.clone().unwrap_or_else(|| {
        Arc::new(fluree_db_core::dict_novelty::DictNovelty::new_uninitialized())
    });
    let dict_gv = BinaryGraphView::new(gv.clone_store(), gv.g_id());
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

fn read_leaflet_first(
    leaf_mmap: &[u8],
    dir: &fluree_db_binary_index::format::leaf::LeafletDirEntry,
) -> std::io::Result<LeafletHeader> {
    let end = dir.offset as usize + dir.compressed_len as usize;
    let leaflet_bytes = &leaf_mmap[dir.offset as usize..end];
    LeafletHeader::read_from(leaflet_bytes)
}

fn prefix_from_leaflet_first(lh: &LeafletHeader) -> (u32, u8, u64) {
    (lh.first_p_id, lh.first_o_kind, lh.first_o_key)
}

fn prefix_from_dir_or_leaflet(
    leaf_mmap: &[u8],
    dir: &fluree_db_binary_index::format::leaf::LeafletDirEntry,
) -> std::io::Result<(u32, u8, u64)> {
    if let (Some(k), Some(o)) = (dir.first_o_kind, dir.first_o_key) {
        Ok((dir.first_p_id, k, o))
    } else {
        // Older leaf versions: fall back to reading the leaflet header.
        let lh = read_leaflet_first(leaf_mmap, dir)?;
        Ok(prefix_from_leaflet_first(&lh))
    }
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
    // Fast-path is available if either V5 or V6 store is loaded.
    let has_store = ctx.graph_view().is_some() || ctx.binary_store_v6.is_some();
    !has_store || ctx.history_mode || ctx.policy_enforcer.is_some()
}

/// Resolve a predicate [`Ref`] to its binary index `p_id`.
fn resolve_predicate_id(
    predicate: &crate::triple::Ref,
    store: &BinaryIndexStore,
    context_label: &str,
) -> Result<u32> {
    match predicate {
        crate::triple::Ref::Sid(sid) => store.sid_to_p_id(sid).ok_or_else(|| {
            QueryError::InvalidQuery("predicate not found in binary predicate dict".to_string())
        }),
        crate::triple::Ref::Iri(iri) => store.find_predicate_id(iri).ok_or_else(|| {
            QueryError::InvalidQuery("predicate not found in binary predicate dict".to_string())
        }),
        _ => Err(QueryError::InvalidQuery(format!(
            "{context_label} requires a bound predicate"
        ))),
    }
}

/// Open a leaf file (with cache-miss retry), mmap it, and read the header.
fn open_and_read_leaf(
    store: &BinaryIndexStore,
    leaf_entry: &LeafEntry,
) -> Result<(
    memmap2::Mmap,
    fluree_db_binary_index::format::leaf::LeafFileHeader,
)> {
    let leaf_path = leaf_entry.resolved_path.as_ref().ok_or_else(|| {
        QueryError::execution(format!("leaf {} has no resolved path", leaf_entry.leaf_cid))
    })?;

    let file = match std::fs::File::open(leaf_path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            store
                .ensure_index_leaf_cached(&leaf_entry.leaf_cid, leaf_path)
                .map_err(|e| io_to_query("ensure_index_leaf_cached", e))?;
            std::fs::File::open(leaf_path).map_err(|e| io_to_query("open leaf after cache", e))?
        }
        Err(e) => return Err(io_to_query("open leaf", e)),
    };
    let leaf_mmap = unsafe { memmap2::Mmap::map(&file).map_err(|e| io_to_query("mmap leaf", e))? };
    let leaf_header =
        read_leaf_header(&leaf_mmap).map_err(|e| io_to_query("read leaf header", e))?;

    Ok((leaf_mmap, leaf_header))
}

/// Decoded Region 1 columns needed for counting.
///
/// The `header` is `Some` only on a cache miss (needed for Region 2 decode).
struct DecodedR1 {
    header: Option<LeafletHeader>,
    p_ids: Arc<[u32]>,
    o_kinds: Arc<[u8]>,
    o_keys: Arc<[u64]>,
}

/// Decode Region 1 with leaflet cache lookup/insert.
fn decode_r1_cached(
    leaflet_bytes: &[u8],
    leaf_header: &fluree_db_binary_index::format::leaf::LeafFileHeader,
    cache: Option<&fluree_db_binary_index::LeafletCache>,
    cache_key: &LeafletCacheKey,
) -> Result<DecodedR1> {
    if let Some(c) = cache {
        if let Some(cached) = c.get_r1(cache_key) {
            return Ok(DecodedR1 {
                header: None,
                p_ids: cached.p_ids,
                o_kinds: cached.o_kinds,
                o_keys: cached.o_keys,
            });
        }
    }

    let (lh, s_ids, p_ids, o_kinds, o_keys) =
        decode_leaflet_region1(leaflet_bytes, leaf_header.p_width, RunSortOrder::Post)
            .map_err(|e| io_to_query("decode leaflet region1", e))?;

    let row_count = lh.row_count as usize;
    let p_ids = Arc::from(p_ids.into_boxed_slice());
    let o_kinds = Arc::from(o_kinds.into_boxed_slice());
    let o_keys = Arc::from(o_keys.into_boxed_slice());

    if let Some(c) = cache {
        let cached = CachedRegion1 {
            s_ids: SubjectIdColumn::from_wide(s_ids.into_iter().map(SubjectId::from_u64).collect()),
            p_ids: Arc::clone(&p_ids),
            o_kinds: Arc::clone(&o_kinds),
            o_keys: Arc::clone(&o_keys),
            row_count,
        };
        c.get_or_decode_r1(cache_key.clone(), || cached);
    }

    Ok(DecodedR1 {
        header: Some(lh),
        p_ids,
        o_kinds,
        o_keys,
    })
}

/// Decode Region 2 with leaflet cache lookup/insert.
///
/// `leaflet_header` is the header from Region 1 decode (if available from a cache miss).
/// If `None` (R1 was a cache hit), re-reads the header from `leaflet_bytes`.
fn decode_r2_cached(
    leaflet_bytes: &[u8],
    leaflet_header: Option<&LeafletHeader>,
    leaf_header: &fluree_db_binary_index::format::leaf::LeafFileHeader,
    cache: Option<&fluree_db_binary_index::LeafletCache>,
    cache_key: &LeafletCacheKey,
) -> Result<CachedRegion2> {
    if let Some(c) = cache {
        if let Some(cached) = c.get_r2(cache_key) {
            return Ok(cached);
        }
    }

    let lh = match leaflet_header {
        Some(h) => h,
        None => &LeafletHeader::read_from(leaflet_bytes)
            .map_err(|e| io_to_query("re-read leaflet header for r2", e))?,
    };
    let decoded = decode_leaflet_region2(leaflet_bytes, lh, leaf_header.dt_width)
        .map_err(|e| io_to_query("decode leaflet region2", e))?;

    let r2 = CachedRegion2 {
        dt_values: Arc::from(decoded.dt_values.into_boxed_slice()),
        t_values: Arc::from(decoded.t_values.into_boxed_slice()),
        lang: decoded.lang,
        i_col: decoded.i_col,
    };

    if let Some(c) = cache {
        c.get_or_decode_r2(cache_key.clone(), || r2.clone());
    }

    Ok(r2)
}

/// Merge novelty overlay deltas into an accumulator.
///
/// Translates overlay ops, filters by `p_id` and an additional caller-supplied `filter`,
/// then replays assert/retract ops in time order and calls `apply_delta` for each net change.
fn merge_overlay_deltas<F, A>(
    ctx: &ExecutionContext<'_>,
    gv: &BinaryGraphView,
    p_id: u32,
    filter: F,
    mut apply_delta: A,
) where
    F: Fn(&OverlayOp) -> bool,
    A: FnMut(&FactKey, i64),
{
    if ctx.overlay.is_none() {
        return;
    }
    let overlay_ops = overlay_ops_for_ctx(ctx, gv);
    if overlay_ops.is_empty() {
        return;
    }

    let mut ops: Vec<(FactKey, i64, bool)> = overlay_ops
        .into_iter()
        .filter(|op| op.p_id == p_id && filter(op))
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
                    apply_delta(&key, 1);
                }
            } else if present {
                present = false;
                apply_delta(&key, -1);
            }
            idx += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Operator 1: PredicateGroupCountFirstsOperator
// ---------------------------------------------------------------------------

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
    /// V6 results: (o_type, o_key, count). When present, used instead of `results`.
    results_v6: Option<Vec<(u16, u64, i64)>>,
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
            results_v6: None,
            pos: 0,
        }
    }

    async fn open_fallback(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        use crate::aggregate::AggregateFn;
        use crate::binary_scan::ScanOperator;
        use crate::group_aggregate::{GroupAggregateOperator, StreamingAggSpec};
        use crate::limit::LimitOperator;
        use crate::sort::{SortDirection, SortOperator, SortSpec};
        use crate::triple::{Ref, TriplePattern};

        let tp = TriplePattern::new(
            Ref::Var(self.subject_var),
            self.predicate.clone(),
            Term::Var(self.object_var),
        );

        // Note: EmitMask pruning is only effective on the binary scan path.
        // RangeScanOperator (used in memory / pre-index fallback) ignores EmitMask,
        // so we use the default (ALL) to avoid a schema mismatch.
        let scan: BoxedOperator = Box::new(ScanOperator::new(tp, None, Vec::new()));

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

        if should_fallback(ctx) {
            return self.open_fallback(ctx).await;
        }

        // Try V6 fast-path first (only when no overlay — overlay delta merge not yet implemented).
        if ctx.overlay.is_none() {
            if let Some(store_v6) = ctx.binary_store_v6.as_ref() {
                match group_count_v6(store_v6, ctx.binary_g_id, &self.predicate, self.limit) {
                    Ok(v6_results) => {
                        self.results_v6 = Some(v6_results);
                        return Ok(());
                    }
                    Err(_) => {
                        // V6 path couldn't handle it — fall through to V5 or fallback.
                    }
                }
            }
        }

        let Some(gv) = ctx.graph_view() else {
            // No V5 store available — use generic scan/aggregate fallback.
            return self.open_fallback(ctx).await;
        };
        let store = gv.clone_store();
        let g_id = gv.g_id();

        let p_id =
            resolve_predicate_id(&self.predicate, &store, "predicate group-count fast-path")?;

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
        let cache = store.leaflet_cache();

        for leaf_idx in leaf_range.clone() {
            let leaf_entry = &branch.leaves[leaf_idx];
            let (leaf_mmap, leaf_header) = open_and_read_leaf(&store, leaf_entry)?;
            let leaf_id = xxhash_rust::xxh3::xxh3_128(&leaf_entry.leaf_cid.to_bytes());

            let dirs = &leaf_header.leaflet_dir;
            for i in 0..dirs.len() {
                let dir = &dirs[i];
                let (p, o_kind, o_key) = prefix_from_dir_or_leaflet(&leaf_mmap, dir)
                    .map_err(|e| io_to_query("read leaflet first prefix", e))?;

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
                        prefix_from_dir_or_leaflet(&leaf_mmap, next_dir)
                            .map_err(|e| io_to_query("read next leaflet first prefix", e))?,
                    )
                } else if leaf_idx + 1 < leaf_range.end {
                    Some(prefix_from_run_record(
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
                        add_count(&mut counts, key, dir.row_count as i64);
                        continue;
                    }
                }

                let end = dir.offset as usize + dir.compressed_len as usize;
                let leaflet_bytes = &leaf_mmap[dir.offset as usize..end];
                let cache_key = LeafletCacheKey {
                    leaf_id,
                    leaflet_index: i as u8,
                    to_t: ctx.to_t,
                    epoch: 0,
                };

                // Decode Region 1 with cache lookup.
                let r1 = decode_r1_cached(leaflet_bytes, &leaf_header, cache, &cache_key)?;

                if let Some(dt_id) = single_dt_id {
                    // Single-datatype mode: count from Region 1 only.
                    for row in 0..r1.p_ids.len() {
                        if r1.p_ids[row] != p_id {
                            continue;
                        }
                        let key = if r1.o_kinds[row] == ObjKind::REF_ID.as_u8() {
                            GroupKey::Ref(r1.o_keys[row])
                        } else {
                            GroupKey::Lit {
                                o_kind: r1.o_kinds[row],
                                o_key: r1.o_keys[row],
                                dt_id,
                                lang_id: 0,
                            }
                        };
                        add_count(&mut counts, key, 1);
                    }
                } else {
                    // Fallback mode: decode Region 2 and group by (o_kind,o_key,dt_id,lang_id).
                    let r2 = decode_r2_cached(
                        leaflet_bytes,
                        r1.header.as_ref(),
                        &leaf_header,
                        cache,
                        &cache_key,
                    )?;
                    for row in 0..r1.p_ids.len() {
                        if r1.p_ids[row] != p_id {
                            continue;
                        }
                        let key = if r1.o_kinds[row] == ObjKind::REF_ID.as_u8() {
                            GroupKey::Ref(r1.o_keys[row])
                        } else {
                            let dt_id = r2.dt_values[row] as u16;
                            let lang_id = r2.lang.as_ref().map(|c| c.get(row as u16)).unwrap_or(0);
                            GroupKey::Lit {
                                o_kind: r1.o_kinds[row],
                                o_key: r1.o_keys[row],
                                dt_id,
                                lang_id,
                            }
                        };
                        add_count(&mut counts, key, 1);
                    }
                }
            }
        }

        // Merge novelty overlay deltas (when present).
        merge_overlay_deltas(
            ctx,
            &gv,
            p_id,
            |_op| true,
            |key, delta| {
                add_count(&mut counts, group_key_for_fact(key, single_dt_id), delta);
            },
        );

        debug_assert!(
            counts.values().all(|&c| c >= 0),
            "group count went negative after overlay merge; overlay invariant violated"
        );

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

        // V6 results path: eagerly decode (o_type, o_key) → FlakeValue.
        if let Some(v6_results) = &self.results_v6 {
            if self.pos >= v6_results.len() {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }
            let store_v6 = ctx.binary_store_v6.as_ref().ok_or_else(|| {
                QueryError::Internal("V6 group-count results but no V6 store".to_string())
            })?;
            let g_id = ctx.binary_g_id;
            let p_id = resolve_predicate_id_v6(&self.predicate, store_v6)?;
            let view = fluree_db_binary_index::BinaryGraphViewV3::new(Arc::clone(store_v6), g_id);

            let batch_size = ctx.batch_size;
            let mut col_o: Vec<Binding> = Vec::with_capacity(batch_size);
            let mut col_c: Vec<Binding> = Vec::with_capacity(batch_size);

            while self.pos < v6_results.len() && col_o.len() < batch_size {
                let (o_type, o_key, count) = v6_results[self.pos];
                self.pos += 1;

                if o_type == OType::IRI_REF.as_u16() {
                    col_o.push(Binding::EncodedSid { s_id: o_key });
                } else {
                    let val = view
                        .decode_value(o_type, o_key, p_id)
                        .map_err(|e| QueryError::Internal(format!("V6 decode_value: {e}")))?;
                    let dt = store_v6
                        .resolve_datatype_sid(o_type)
                        .unwrap_or_else(|| fluree_db_core::Sid::new(0, ""));
                    let lang: Option<Arc<str>> = store_v6.resolve_lang_tag(o_type).map(Arc::from);
                    col_o.push(Binding::Lit {
                        val,
                        dt,
                        lang,
                        t: None,
                        op: None,
                        p_id: None,
                    });
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

            return Ok(Some(crate::binding::Batch::new(
                self.schema.clone(),
                vec![col_o, col_c],
            )?));
        }

        // V5 results path.
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
        let p_id =
            resolve_predicate_id(&self.predicate, &store, "predicate group-count fast-path")?;

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
        self.results_v6 = None;
        self.pos = 0;
    }

    fn estimated_rows(&self) -> Option<usize> {
        Some(self.limit)
    }
}

// ---------------------------------------------------------------------------
// Operator 2: PredicateObjectCountFirstsOperator
// ---------------------------------------------------------------------------

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

    async fn open_fallback(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        use crate::aggregate::AggregateFn;
        use crate::binary_scan::ScanOperator;
        use crate::group_aggregate::{GroupAggregateOperator, StreamingAggSpec};
        use crate::triple::{Ref, TriplePattern};

        let tp = TriplePattern::new(
            Ref::Var(self.subject_var),
            self.predicate.clone(),
            self.object.clone(),
        );

        // Note: EmitMask pruning is only effective on the binary scan path.
        // RangeScanOperator (used in memory / pre-index fallback) ignores EmitMask,
        // so we use the default (ALL) to avoid a schema mismatch.
        let scan: BoxedOperator = Box::new(ScanOperator::new(tp, None, Vec::new()));

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

        if should_fallback(ctx) {
            return self.open_fallback(ctx).await;
        }

        // Try V6 fast-path first (only when no overlay — overlay delta merge not yet implemented).
        if ctx.overlay.is_none() {
            if let Some(store_v6) = ctx.binary_store_v6.as_ref() {
                match count_bound_object_v6(
                    store_v6,
                    ctx.binary_g_id,
                    &self.predicate,
                    &self.object,
                ) {
                    Ok(total) => {
                        self.count = total;
                        return Ok(());
                    }
                    Err(_) => {
                        // V6 path couldn't handle it — fall through to V5 or fallback.
                    }
                }
            }
        }

        let Some(gv) = ctx.graph_view() else {
            // No V5 store available — use generic scan/aggregate fallback.
            return self.open_fallback(ctx).await;
        };
        let store = gv.clone_store();
        let g_id = gv.g_id();

        let p_id =
            resolve_predicate_id(&self.predicate, &store, "predicate-object count fast-path")?;

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
        let cache = store.leaflet_cache();

        for leaf_idx in leaf_range.clone() {
            let leaf_entry = &branch.leaves[leaf_idx];
            let (leaf_mmap, leaf_header) = open_and_read_leaf(&store, leaf_entry)?;
            let leaf_id = xxhash_rust::xxh3::xxh3_128(&leaf_entry.leaf_cid.to_bytes());

            let dirs = &leaf_header.leaflet_dir;
            for i in 0..dirs.len() {
                let dir = &dirs[i];
                let prefix = prefix_from_dir_or_leaflet(&leaf_mmap, dir)
                    .map_err(|e| io_to_query("read leaflet first prefix", e))?;

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
                        prefix_from_dir_or_leaflet(&leaf_mmap, next_dir)
                            .map_err(|e| io_to_query("read next leaflet first prefix", e))?,
                    )
                } else if leaf_idx + 1 < leaf_range.end {
                    Some(prefix_from_run_record(
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
                let cache_key = LeafletCacheKey {
                    leaf_id,
                    leaflet_index: i as u8,
                    to_t: ctx.to_t,
                    epoch: 0,
                };

                let r1 = decode_r1_cached(leaflet_bytes, &leaf_header, cache, &cache_key)?;

                for row in 0..r1.p_ids.len() {
                    if r1.p_ids[row] == p_id
                        && r1.o_kinds[row] == target_kind
                        && r1.o_keys[row] == target_key
                    {
                        total += 1;
                    }
                }
            }
        }

        // Merge novelty overlay deltas (when present).
        merge_overlay_deltas(
            ctx,
            &gv,
            p_id,
            |op| op.o_kind == target_kind && op.o_key == target_key,
            |_key, delta| {
                total += delta;
            },
        );

        debug_assert!(
            total >= 0,
            "total count went negative ({total}) after overlay merge; overlay invariant violated"
        );

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

// ---------------------------------------------------------------------------
// V6 fast-path implementations
// ---------------------------------------------------------------------------

/// Extract the object prefix `(o_type, o_key)` from a V3 leaflet directory entry's
/// `first_key` field, interpreted in POST order.
#[inline]
fn prefix_v6_from_entry(entry: &LeafletDirEntryV3) -> (u16, u64) {
    let rec = read_ordered_key_v2(RunSortOrder::Post, &entry.first_key);
    (rec.o_type, rec.o_key)
}

/// Resolve a predicate [`Ref`] to its V6 binary index `p_id`.
fn resolve_predicate_id_v6(
    predicate: &crate::triple::Ref,
    store: &BinaryIndexStoreV6,
) -> Result<u32> {
    let sid = match predicate {
        crate::triple::Ref::Sid(s) => s.clone(),
        crate::triple::Ref::Iri(i) => store.encode_iri(i),
        crate::triple::Ref::Var(_) => {
            return Err(QueryError::Internal(
                "fast-path requires bound predicate".to_string(),
            ))
        }
    };
    store
        .sid_to_p_id(&sid)
        .ok_or_else(|| QueryError::Internal("predicate not found in V6 dictionary".to_string()))
}

/// V6 fast-path: count rows for a bound `(predicate, object)` triple.
///
/// Scans the POST leaf range for the predicate, uses boundary-equality on
/// `(o_type, o_key)` to skip whole leaflets, and decodes only `o_key` + `o_type`
/// columns when needed.
fn count_bound_object_v6(
    store: &BinaryIndexStoreV6,
    g_id: GraphId,
    predicate: &crate::triple::Ref,
    object: &Term,
) -> Result<i64> {
    let p_id = resolve_predicate_id_v6(predicate, store)?;

    // Translate the bound object term into V6 (o_type, o_key).
    let (target_o_type, target_o_key) = translate_term_to_v6(object, store, p_id, g_id)?;

    let branch = store
        .branch_for_order(g_id, RunSortOrder::Post)
        .ok_or_else(|| QueryError::Internal("no POST branch for graph".to_string()))?;
    let cmp = cmp_v2_for_order(RunSortOrder::Post);

    let min_key = RunRecordV2 {
        s_id: SubjectId(0),
        o_key: target_o_key,
        p_id,
        t: 0,
        o_i: 0,
        o_type: target_o_type,
        g_id,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId(u64::MAX),
        o_key: target_o_key,
        p_id,
        t: 0,
        o_i: u32::MAX,
        o_type: target_o_type,
        g_id,
    };
    let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);

    let target_prefix = (target_o_type, target_o_key);
    let mut total: i64 = 0;

    for leaf_idx in leaf_range.clone() {
        let leaf_entry = &branch.leaves[leaf_idx];
        let bytes = store
            .get_leaf_bytes_sync(&leaf_entry.leaf_cid)
            .map_err(|e| QueryError::Internal(format!("leaf fetch: {e}")))?;
        let header =
            decode_leaf_header_v3(&bytes).map_err(|e| QueryError::Internal(e.to_string()))?;
        let dir = decode_leaf_dir_v3_with_base(&bytes, &header)
            .map_err(|e| QueryError::Internal(e.to_string()))?;

        for (i, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }

            let prefix = prefix_v6_from_entry(entry);
            if prefix < target_prefix {
                continue;
            }
            if prefix > target_prefix {
                break;
            }

            // Boundary-equality: check the next leaflet's first prefix.
            let next_prefix = if i + 1 < dir.entries.len() {
                Some(prefix_v6_from_entry(&dir.entries[i + 1]))
            } else if leaf_idx + 1 < leaf_range.end {
                let next_entry = &branch.leaves[leaf_idx + 1].first_key;
                Some((next_entry.o_type, next_entry.o_key))
            } else {
                None
            };

            if next_prefix == Some(target_prefix) {
                total += entry.row_count as i64;
                continue;
            }

            // Decode columns and count matches.
            let needs_o_type_col = entry.o_type_const.is_none();
            let mut needed = ColumnSet::EMPTY;
            needed.insert(ColumnId::OKey);
            if needs_o_type_col {
                needed.insert(ColumnId::OType);
            }
            let projection = ColumnProjection {
                output: ColumnSet::EMPTY,
                internal: needed,
            };

            let batch =
                load_leaflet_columns(&bytes, entry, dir.payload_base, &projection, header.order)
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;

            for row in 0..batch.row_count {
                let ot = entry
                    .o_type_const
                    .unwrap_or_else(|| batch.o_type.get_or(row, 0));
                if ot == target_o_type && batch.o_key.get(row) == target_o_key {
                    total += 1;
                }
            }
        }
    }

    Ok(total)
}

/// V6 fast-path: GROUP BY ?o COUNT(?s) for a predicate.
///
/// Returns `Vec<(o_type, o_key, count)>` sorted by count descending, truncated to `limit`.
fn group_count_v6(
    store: &BinaryIndexStoreV6,
    g_id: GraphId,
    predicate: &crate::triple::Ref,
    limit: usize,
) -> Result<Vec<(u16, u64, i64)>> {
    let p_id = resolve_predicate_id_v6(predicate, store)?;

    let branch = store
        .branch_for_order(g_id, RunSortOrder::Post)
        .ok_or_else(|| QueryError::Internal("no POST branch for graph".to_string()))?;
    let cmp = cmp_v2_for_order(RunSortOrder::Post);

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
    let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);

    let mut counts: HashMap<(u16, u64), i64> = HashMap::new();

    for leaf_idx in leaf_range.clone() {
        let leaf_entry = &branch.leaves[leaf_idx];
        let bytes = store
            .get_leaf_bytes_sync(&leaf_entry.leaf_cid)
            .map_err(|e| QueryError::Internal(format!("leaf fetch: {e}")))?;
        let header =
            decode_leaf_header_v3(&bytes).map_err(|e| QueryError::Internal(e.to_string()))?;
        let dir = decode_leaf_dir_v3_with_base(&bytes, &header)
            .map_err(|e| QueryError::Internal(e.to_string()))?;

        for (i, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }

            let prefix = prefix_v6_from_entry(entry);

            // Boundary-equality: check if this leaflet is entirely one object value.
            let next_prefix = if i + 1 < dir.entries.len() {
                Some(prefix_v6_from_entry(&dir.entries[i + 1]))
            } else if leaf_idx + 1 < leaf_range.end {
                let next_entry = &branch.leaves[leaf_idx + 1].first_key;
                Some((next_entry.o_type, next_entry.o_key))
            } else {
                None
            };

            if next_prefix == Some(prefix) {
                // Entire leaflet is one object value.
                *counts.entry(prefix).or_insert(0) += entry.row_count as i64;
                continue;
            }

            // Decode columns and count per (o_type, o_key).
            let needs_o_type_col = entry.o_type_const.is_none();
            let mut needed = ColumnSet::EMPTY;
            needed.insert(ColumnId::OKey);
            if needs_o_type_col {
                needed.insert(ColumnId::OType);
            }
            let projection = ColumnProjection {
                output: ColumnSet::EMPTY,
                internal: needed,
            };

            let batch =
                load_leaflet_columns(&bytes, entry, dir.payload_base, &projection, header.order)
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;

            for row in 0..batch.row_count {
                let ot = entry
                    .o_type_const
                    .unwrap_or_else(|| batch.o_type.get_or(row, 0));
                let ok = batch.o_key.get(row);
                *counts.entry((ot, ok)).or_insert(0) += 1;
            }
        }
    }

    // Sort by count desc, truncate.
    let mut rows: Vec<(u16, u64, i64)> = counts
        .into_iter()
        .map(|((ot, ok), c)| (ot, ok, c))
        .collect();
    rows.sort_unstable_by(|a, b| b.2.cmp(&a.2).then(a.0.cmp(&b.0)).then(a.1.cmp(&b.1)));
    rows.truncate(limit);

    Ok(rows)
}

/// Translate a bound object `Term` to V6 `(o_type, o_key)`.
fn translate_term_to_v6(
    term: &Term,
    store: &BinaryIndexStoreV6,
    _p_id: u32,
    _g_id: GraphId,
) -> Result<(u16, u64)> {
    match term {
        Term::Sid(sid) => {
            let s_id = store
                .sid_to_s_id(sid)
                .map_err(|e| QueryError::execution(format!("sid_to_s_id: {e}")))?
                .ok_or_else(|| {
                    QueryError::execution("bound object SID not found in V6 dict".to_string())
                })?;
            Ok((OType::IRI_REF.as_u16(), s_id))
        }
        Term::Iri(iri) => {
            let s_id = store
                .find_subject_id(iri)
                .map_err(|e| QueryError::execution(format!("find_subject_id: {e}")))?
                .ok_or_else(|| {
                    QueryError::execution("bound object IRI not found in V6 dict".to_string())
                })?;
            Ok((OType::IRI_REF.as_u16(), s_id))
        }
        Term::Value(val) => {
            // For literal values, we need the FlakeValue → (o_type, o_key) translation.
            // Use the Sid-based dt info from the FlakeValue if available.
            let (ot, ok) = crate::binary_scan_v3::value_to_otype_okey_simple(val, store)?;
            Ok((ot.as_u16(), ok))
        }
        Term::Var(_) => Err(QueryError::InvalidQuery(
            "fast-path requires a bound object".to_string(),
        )),
    }
}
