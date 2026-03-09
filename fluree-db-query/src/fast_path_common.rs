//! Shared primitives for fast-path query operators.
//!
//! All fast-path operators (fused scan + aggregate) share common building blocks:
//! predicate resolution, leaf range scanning, subject collection, and operator plumbing.
//! This module consolidates them to avoid ~1,100 lines of duplication across 9 files.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::operator::{Operator, OperatorState};
use crate::triple::Ref;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::format::branch::LeafEntry;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::{cmp_v2_for_order, RunRecordV2};
use fluree_db_binary_index::{BinaryIndexStore, ColumnBatch, ColumnProjection, ColumnSet};
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::{FlakeValue, GraphId, Sid};
use rustc_hash::FxHashSet;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// 1. Predicate resolution
// ---------------------------------------------------------------------------

/// Resolve a predicate `Ref` to its `Sid`, returning an error for variables.
pub fn normalize_pred_sid(store: &BinaryIndexStore, pred: &Ref) -> Result<Sid> {
    Ok(match pred {
        Ref::Sid(s) => s.clone(),
        Ref::Iri(i) => store.encode_iri(i),
        Ref::Var(_) => {
            return Err(QueryError::Internal(
                "fast-path requires bound predicates".to_string(),
            ))
        }
    })
}

/// Like [`normalize_pred_sid`] but returns `None` for variables instead of an error.
pub fn try_normalize_pred_sid(store: &BinaryIndexStore, pred: &Ref) -> Option<Sid> {
    match pred {
        Ref::Sid(s) => Some(s.clone()),
        Ref::Iri(i) => Some(store.encode_iri(i)),
        Ref::Var(_) => None,
    }
}

// ---------------------------------------------------------------------------
// 2. Column projection helpers
// ---------------------------------------------------------------------------

/// Projection that loads only the SId column (internal, not output).
#[inline]
pub fn projection_sid_only() -> ColumnProjection {
    ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::SId);
            s
        },
    }
}

/// Projection that loads only the OKey column (internal, not output).
#[inline]
pub fn projection_okey_only() -> ColumnProjection {
    ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::OKey);
            s
        },
    }
}

/// Projection that loads SId + OKey columns (internal, not output).
#[inline]
pub fn projection_sid_okey() -> ColumnProjection {
    ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::SId);
            s.insert(ColumnId::OKey);
            s
        },
    }
}

/// Projection that loads SId + OType + OKey columns (internal, not output).
#[inline]
pub fn projection_sid_otype_okey() -> ColumnProjection {
    ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::SId);
            s.insert(ColumnId::OType);
            s.insert(ColumnId::OKey);
            s
        },
    }
}

/// Projection that loads OType + OKey columns (internal, not output).
#[inline]
pub fn projection_otype_okey() -> ColumnProjection {
    ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::OType);
            s.insert(ColumnId::OKey);
            s
        },
    }
}

/// Projection that loads only the OType column (internal, not output).
#[inline]
pub fn projection_otype_only() -> ColumnProjection {
    ColumnProjection {
        output: ColumnSet::EMPTY,
        internal: {
            let mut s = ColumnSet::EMPTY;
            s.insert(ColumnId::OType);
            s
        },
    }
}

// ---------------------------------------------------------------------------
// 3. Leaf range scanning
// ---------------------------------------------------------------------------

/// Construct min/max `RunRecordV2` keys spanning all rows for a predicate.
#[inline]
pub fn predicate_range_keys(p_id: u32, g_id: GraphId) -> (RunRecordV2, RunRecordV2) {
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
        t: u32::MAX,
        o_i: u32::MAX,
        o_type: u16::MAX,
        g_id,
    };
    (min_key, max_key)
}

/// Find leaf entries for a predicate in a given sort order.
///
/// Returns an empty slice if the branch does not exist.
pub fn leaf_entries_for_predicate(
    store: &BinaryIndexStore,
    g_id: GraphId,
    order: RunSortOrder,
    p_id: u32,
) -> &[LeafEntry] {
    let Some(branch) = store.branch_for_order(g_id, order) else {
        return &[];
    };
    let cmp = cmp_v2_for_order(order);
    let (min_key, max_key) = predicate_range_keys(p_id, g_id);
    let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);
    &branch.leaves[leaf_range]
}

// ---------------------------------------------------------------------------
// 4. Subject collection
// ---------------------------------------------------------------------------

/// Collect distinct subject IDs from PSOT for a predicate as a sorted `Vec<u64>`.
///
/// PSOT guarantees subjects are emitted in sorted order within a fixed predicate,
/// so deduplication is a simple consecutive check.
pub fn collect_subjects_for_predicate_sorted(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
) -> Result<Vec<u64>> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
    let projection = projection_sid_only();

    let mut out: Vec<u64> = Vec::new();
    let mut prev: Option<u64> = None;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Psot)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            for row in 0..batch.row_count {
                let sid = batch.s_id.get(row);
                if prev != Some(sid) {
                    out.push(sid);
                    prev = Some(sid);
                }
            }
        }
    }
    Ok(out)
}

/// Collect distinct subject IDs from PSOT for a predicate as an `FxHashSet<u64>`.
///
/// Preferred when the caller needs O(1) membership tests rather than merge-join.
pub fn collect_subjects_for_predicate_set(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
) -> Result<FxHashSet<u64>> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
    let projection = projection_sid_only();

    let mut out: FxHashSet<u64> = FxHashSet::default();
    let mut prev: Option<u64> = None;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Psot)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            for row in 0..batch.row_count {
                let sid = batch.s_id.get(row);
                if prev != Some(sid) {
                    out.insert(sid);
                    prev = Some(sid);
                }
            }
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// 5. Sorted set operations
// ---------------------------------------------------------------------------

/// Two-pointer intersection of two sorted, deduplicated `u64` slices.
pub fn intersect_sorted(a: &[u64], b: &[u64]) -> Vec<u64> {
    let mut out = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        let (av, bv) = (a[i], b[j]);
        match av.cmp(&bv) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(av);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

/// N-way intersection of sorted `u64` lists. Sorts by length and folds pairwise.
pub fn intersect_many_sorted(mut lists: Vec<Vec<u64>>) -> Vec<u64> {
    if lists.is_empty() {
        return Vec::new();
    }
    lists.sort_by_key(|v| v.len());
    let mut acc = lists.remove(0);
    for next in lists {
        if acc.is_empty() {
            break;
        }
        acc = intersect_sorted(&acc, &next);
    }
    acc
}

// ---------------------------------------------------------------------------
// 6. Merge-count
// ---------------------------------------------------------------------------

/// Stream PSOT rows for a predicate grouped by subject, merge-filter by a sorted
/// subject list, and sum the matching row counts.
pub fn count_rows_psot_for_subjects_sorted(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
    subjects_sorted: &[u64],
) -> Result<u64> {
    if subjects_sorted.is_empty() {
        return Ok(0);
    }
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
    let projection = projection_sid_only();

    let mut count: u64 = 0;
    let mut subj_idx: usize = 0;
    let mut cur_s: Option<u64> = None;
    let mut cur_count: u64 = 0;

    let flush_group = |s_id: u64, n: u64, subj_idx: &mut usize, count: &mut u64| {
        while *subj_idx < subjects_sorted.len() && subjects_sorted[*subj_idx] < s_id {
            *subj_idx += 1;
        }
        if *subj_idx < subjects_sorted.len() && subjects_sorted[*subj_idx] == s_id {
            *count += n;
        }
    };

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();
        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Psot)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
            for row in 0..batch.row_count {
                let sid = batch.s_id.get(row);
                match cur_s {
                    None => {
                        cur_s = Some(sid);
                        cur_count = 1;
                    }
                    Some(prev) if prev == sid => cur_count += 1,
                    Some(prev) => {
                        flush_group(prev, cur_count, &mut subj_idx, &mut count);
                        cur_s = Some(sid);
                        cur_count = 1;
                    }
                }
            }
        }
    }
    if let Some(last) = cur_s {
        flush_group(last, cur_count, &mut subj_idx, &mut count);
    }
    Ok(count)
}

/// Count total rows for a predicate by summing PSOT leaflet directory `row_count`.
///
/// This is the fastest possible implementation of:
/// `SELECT (COUNT(*) AS ?c) WHERE { ?s <p> ?o }`
/// (and also `COUNT(?s)` / `COUNT(?o)` for the same single-triple pattern),
/// because every solution binding has all vars bound.
///
/// Assumes PSOT leaflets have `p_const` set (so we can filter without loading columns).
pub fn count_rows_for_predicate_psot(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
) -> Result<u64> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
    let mut total: u64 = 0;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();
        for entry in &dir.entries {
            if entry.row_count == 0 {
                continue;
            }
            if entry.p_const != Some(p_id) {
                continue;
            }
            total += entry.row_count as u64;
        }
    }

    Ok(total)
}

// ---------------------------------------------------------------------------
// 7. Streaming PSOT subject-count iterator
// ---------------------------------------------------------------------------

/// Streaming iterator over PSOT leaflets for a predicate that yields
/// `(subject_id, row_count)` groups in sorted subject order.
pub struct PsotSubjectCountIter<'a> {
    store: &'a BinaryIndexStore,
    p_id: u32,
    leaf_entries: &'a [LeafEntry],
    leaf_pos: usize,
    leaflet_idx: usize,
    row: usize,
    handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
    batch: Option<ColumnBatch>,
    /// Accumulated subject for a group that may span leaflet boundaries.
    cur_s: Option<u64>,
    cur_count: u64,
}

impl<'a> PsotSubjectCountIter<'a> {
    pub fn new(store: &'a BinaryIndexStore, g_id: GraphId, p_id: u32) -> Result<Self> {
        let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id);
        Ok(Self {
            store,
            p_id,
            leaf_entries: leaves,
            leaf_pos: 0,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
            cur_s: None,
            cur_count: 0,
        })
    }

    fn load_next_batch(&mut self) -> Result<Option<()>> {
        let projection = projection_sid_only();
        loop {
            if self.handle.is_none() {
                if self.leaf_pos >= self.leaf_entries.len() {
                    return Ok(None);
                }
                let leaf_entry = &self.leaf_entries[self.leaf_pos];
                self.leaf_pos += 1;
                self.leaflet_idx = 0;
                self.row = 0;
                self.batch = None;
                self.handle = Some(
                    self.store
                        .open_leaf_handle(
                            &leaf_entry.leaf_cid,
                            leaf_entry.sidecar_cid.as_ref(),
                            false,
                        )
                        .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?,
                );
            }

            let handle = self.handle.as_ref().unwrap();
            let dir = handle.dir();
            while self.leaflet_idx < dir.entries.len() {
                let entry = &dir.entries[self.leaflet_idx];
                let idx = self.leaflet_idx;
                self.leaflet_idx += 1;
                if entry.row_count == 0 || entry.p_const != Some(self.p_id) {
                    continue;
                }
                let batch = handle
                    .load_columns(idx, &projection, RunSortOrder::Psot)
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
                self.row = 0;
                self.batch = Some(batch);
                return Ok(Some(()));
            }

            self.handle = None;
        }
    }

    /// Return the next `(subject_id, count)` group.
    ///
    /// Groups span leaflet boundaries — a subject that straddles two leaflets
    /// will NOT be split across two calls (the group accumulates until the
    /// subject changes).
    pub fn next_group(&mut self) -> Result<Option<(u64, u64)>> {
        loop {
            // Load a batch if needed. If there are no more batches, flush any accumulated group.
            if self.batch.is_none() && self.load_next_batch()?.is_none() {
                if let Some(s) = self.cur_s.take() {
                    let n = std::mem::take(&mut self.cur_count);
                    return Ok(Some((s, n)));
                }
                return Ok(None);
            }

            let batch = self.batch.as_ref().unwrap();

            if self.row >= batch.row_count {
                self.batch = None;
                continue;
            }

            let sid = batch.s_id.get(self.row);

            match self.cur_s {
                None => {
                    self.cur_s = Some(sid);
                    self.cur_count = 0;
                }
                Some(cur) if cur != sid => {
                    // New subject starts; emit previous group without consuming this row.
                    let out_s = self.cur_s.take().expect("checked: cur_s is Some");
                    let out_n = std::mem::take(&mut self.cur_count);
                    return Ok(Some((out_s, out_n)));
                }
                Some(_) => {}
            }

            // Accumulate current subject (may span batches).
            self.cur_count += 1;
            self.row += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// 7b. Streaming POST object-group-count iterator
// ---------------------------------------------------------------------------

/// Streaming iterator over POST leaflets for a predicate that yields
/// `(object_key, row_count)` groups in POST order, restricted to IRI_REF objects.
///
/// Returns `Ok(None)` from `next_group` if a non-IRI_REF leaflet is encountered
/// (unless it's a mixed-type leaflet, in which case non-IRI rows are skipped).
pub struct PostObjectGroupCountIter<'a> {
    store: &'a BinaryIndexStore,
    p_id: u32,
    leaf_entries: &'a [LeafEntry],
    leaf_pos: usize,
    leaflet_idx: usize,
    row: usize,
    handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
    batch: Option<ColumnBatch>,
    mixed: bool,
}

impl<'a> PostObjectGroupCountIter<'a> {
    pub fn new(store: &'a BinaryIndexStore, g_id: GraphId, p_id: u32) -> Result<Option<Self>> {
        Ok(Some(Self {
            store,
            p_id,
            leaf_entries: leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id),
            leaf_pos: 0,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
            mixed: false,
        }))
    }

    fn load_next_batch(&mut self) -> Result<Option<()>> {
        let proj_okey = projection_okey_only();
        let proj_otype_okey = projection_otype_okey();
        loop {
            if self.handle.is_none() {
                if self.leaf_pos >= self.leaf_entries.len() {
                    return Ok(None);
                }
                let leaf_entry = &self.leaf_entries[self.leaf_pos];
                self.leaf_pos += 1;
                self.leaflet_idx = 0;
                self.row = 0;
                self.batch = None;
                self.handle = Some(
                    self.store
                        .open_leaf_handle(
                            &leaf_entry.leaf_cid,
                            leaf_entry.sidecar_cid.as_ref(),
                            false,
                        )
                        .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?,
                );
            }

            let handle = self.handle.as_ref().unwrap();
            let dir = handle.dir();
            while self.leaflet_idx < dir.entries.len() {
                let entry = &dir.entries[self.leaflet_idx];
                let idx = self.leaflet_idx;
                self.leaflet_idx += 1;
                if entry.row_count == 0 || entry.p_const != Some(self.p_id) {
                    continue;
                }
                let mixed = entry.o_type_const.is_none();
                if !mixed && entry.o_type_const != Some(OType::IRI_REF.as_u16()) {
                    return Ok(None);
                }
                let batch = handle
                    .load_columns(
                        idx,
                        if mixed { &proj_otype_okey } else { &proj_okey },
                        RunSortOrder::Post,
                    )
                    .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;
                self.row = 0;
                self.batch = Some(batch);
                self.mixed = mixed;
                return Ok(Some(()));
            }

            self.handle = None;
        }
    }

    /// Return the next `(object_key, count)` group.
    ///
    /// Only counts IRI_REF objects. Mixed-type leaflets are handled by filtering
    /// to IRI rows. Returns `None` when exhausted or if a non-IRI homogeneous
    /// leaflet is encountered.
    pub fn next_group(&mut self) -> Result<Option<(u64, u64)>> {
        loop {
            if self.batch.is_none() && self.load_next_batch()?.is_none() {
                return Ok(None);
            }
            let batch = self.batch.as_ref().unwrap();
            if self.row >= batch.row_count {
                self.batch = None;
                continue;
            }
            if !self.mixed {
                let b = batch.o_key.get(self.row);
                let mut count: u64 = 0;
                while self.row < batch.row_count && batch.o_key.get(self.row) == b {
                    count += 1;
                    self.row += 1;
                }
                return Ok(Some((b, count)));
            }

            // Mixed-type leaflet: skip non-IRI_REF rows and group by o_key.
            while self.row < batch.row_count
                && batch.o_type.get(self.row) != OType::IRI_REF.as_u16()
            {
                self.row += 1;
            }
            if self.row >= batch.row_count {
                self.batch = None;
                continue;
            }

            let b = batch.o_key.get(self.row);
            let mut count: u64 = 0;
            while self.row < batch.row_count
                && batch.o_type.get(self.row) == OType::IRI_REF.as_u16()
                && batch.o_key.get(self.row) == b
            {
                count += 1;
                self.row += 1;
            }
            return Ok(Some((b, count)));
        }
    }
}

// ---------------------------------------------------------------------------
// 8. Operator plumbing
// ---------------------------------------------------------------------------

/// Tiny helper operator: yields exactly one precomputed batch, then exhausts.
///
/// Starts in `Open` state since the batch is pre-computed at construction time.
pub struct PrecomputedSingleBatchOperator {
    batch: Option<Batch>,
    state: OperatorState,
}

impl PrecomputedSingleBatchOperator {
    pub fn new(batch: Batch) -> Self {
        Self {
            batch: Some(batch),
            state: OperatorState::Open,
        }
    }
}

#[async_trait]
impl Operator for PrecomputedSingleBatchOperator {
    fn schema(&self) -> &[VarId] {
        self.batch.as_ref().map(|b| b.schema()).unwrap_or(&[])
    }

    async fn open(&mut self, _ctx: &ExecutionContext<'_>) -> Result<()> {
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, _ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            return Ok(None);
        }
        let out = self.batch.take();
        if out.is_none() {
            self.state = OperatorState::Exhausted;
        }
        Ok(out)
    }

    fn close(&mut self) {
        self.batch = None;
        self.state = OperatorState::Closed;
    }
}

/// Build a single-row batch containing a count value (`xsd:integer`).
pub fn build_count_batch(out_var: VarId, count: i64) -> Result<Batch> {
    let schema: Arc<[VarId]> = Arc::from(vec![out_var].into_boxed_slice());
    let col = vec![Binding::lit(FlakeValue::Long(count), Sid::xsd_integer())];
    Batch::new(schema, vec![col])
        .map_err(|e| QueryError::execution(format!("fast-path count batch build: {e}")))
}

/// Check whether the execution context allows fast-path operators.
///
/// Fast paths are only valid when not in history mode, no `from_t`, no policy
/// enforcement (or root policy), and no uncommitted overlay.
#[inline]
pub fn allow_fast_path(ctx: &ExecutionContext<'_>) -> bool {
    !ctx.history_mode
        && ctx.from_t.is_none()
        && ctx.policy_enforcer.as_ref().is_none_or(|p| p.is_root())
        && ctx.overlay.map(|o| o.epoch()).unwrap_or(0) == 0
}

/// Combined fast-path eligibility: [`allow_fast_path`] + binary store present + `to_t == max_t`.
///
/// Returns the store reference if the fast path can proceed, `None` otherwise.
#[inline]
pub fn fast_path_store<'a>(ctx: &'a ExecutionContext<'_>) -> Option<&'a Arc<BinaryIndexStore>> {
    if !allow_fast_path(ctx) {
        return None;
    }
    let store = ctx.binary_store.as_ref()?;
    if ctx.to_t != store.max_t() {
        return None;
    }
    Some(store)
}
