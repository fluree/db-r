//! Fast-path: COUNT(*) for a single required triple with an OPTIONAL 2-hop chain.
//!
//! Targets benchmark query shape:
//! `SELECT (COUNT(*) AS ?count) WHERE { ?a <p1> ?b . OPTIONAL { ?b <p2> ?c . ?c <p3> ?d . } }`
//!
//! Semantics:
//! For each required row `(a,b)`, the OPTIONAL contributes `max(1, count_chain(b))` rows where
//! `count_chain(b) = Σ_{c in p2(b)} count_{p3}(c)`.
//!
//! Therefore:
//!   total = Σ_b count_{p1}(b) * max(1, Σ_{c in p2(b)} count_{p3}(c))
//!
//! We compute this streaming:
//! - Build `n3(c) = count_{p3}(c)` by scanning PSOT(p3) grouped by subject.
//! - Stream POST(p1) grouped by object `b` to get `w(b) = count_{p1}(b)`.
//! - Stream PSOT(p2) grouped by subject `b` and sum `n3(c)` across its objects `c`.
//! - Merge-join on `b` and apply `max(1, ...)`.
//!
//! This avoids materializing join rows and avoids per-row OPTIONAL execution.

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    build_count_batch, fast_path_store, leaf_entries_for_predicate, normalize_pred_sid,
    projection_sid_okey, projection_sid_otype_okey, PostObjectGroupCountIter,
    PrecomputedSingleBatchOperator, PsotSubjectCountIter,
};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::triple::Ref;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::{BinaryIndexStore, ColumnBatch};
use fluree_db_core::o_type::OType;
use fluree_db_core::GraphId;
use rustc_hash::FxHashMap;

pub struct PredicateOptionalChainHeadCountAllOperator {
    p1: Ref,
    p2: Ref,
    p3: Ref,
    out_var: VarId,
    state: OperatorState,
    fallback: Option<BoxedOperator>,
}

impl PredicateOptionalChainHeadCountAllOperator {
    pub fn new(p1: Ref, p2: Ref, p3: Ref, out_var: VarId, fallback: Option<BoxedOperator>) -> Self {
        Self {
            p1,
            p2,
            p3,
            out_var,
            state: OperatorState::Created,
            fallback,
        }
    }
}

#[async_trait]
impl Operator for PredicateOptionalChainHeadCountAllOperator {
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
            if let Some(count) =
                count_optional_chain_head(store, ctx.binary_g_id, &self.p1, &self.p2, &self.p3)?
            {
                self.state = OperatorState::Open;
                self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(
                    build_count_batch(self.out_var, i64::try_from(count).unwrap_or(i64::MAX))?,
                )));
                return Ok(());
            }
        }

        let Some(fallback) = &mut self.fallback else {
            return Err(QueryError::Internal(
                "optional chain-head COUNT(*) fast-path unavailable and no fallback provided"
                    .to_string(),
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

/// Yield `(b, sum_n3)` groups from PSOT(p2), where `sum_n3 = Σ_{c in p2(b)} n3(c)`.
struct PsotSubjectSumN3Iter<'a> {
    store: &'a BinaryIndexStore,
    p_id: u32,
    n3: &'a FxHashMap<u64, u64>,
    leaf_entries: &'a [fluree_db_binary_index::format::branch::LeafEntry],
    leaf_pos: usize,
    leaflet_idx: usize,
    row: usize,
    handle: Option<Box<dyn fluree_db_binary_index::read::leaf_access::LeafHandle>>,
    batch: Option<ColumnBatch>,
    cur_b: Option<u64>,
    cur_sum: u64,
    mixed: bool,
}

impl<'a> PsotSubjectSumN3Iter<'a> {
    fn new(
        store: &'a BinaryIndexStore,
        g_id: GraphId,
        p_id: u32,
        n3: &'a FxHashMap<u64, u64>,
    ) -> Result<Option<Self>> {
        Ok(Some(Self {
            store,
            p_id,
            n3,
            leaf_entries: leaf_entries_for_predicate(store, g_id, RunSortOrder::Psot, p_id),
            leaf_pos: 0,
            leaflet_idx: 0,
            row: 0,
            handle: None,
            batch: None,
            cur_b: None,
            cur_sum: 0,
            mixed: false,
        }))
    }

    fn load_next_batch(&mut self) -> Result<Option<()>> {
        let projection_sid_okey = projection_sid_okey();
        let projection_sid_otype_okey = projection_sid_otype_okey();
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
                        if mixed {
                            &projection_sid_otype_okey
                        } else {
                            &projection_sid_okey
                        },
                        RunSortOrder::Psot,
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

    fn next_group(&mut self) -> Result<Option<(u64, u64)>> {
        loop {
            if self.batch.is_none() {
                if self.load_next_batch()?.is_none() {
                    if let Some(b) = self.cur_b.take() {
                        let n = std::mem::take(&mut self.cur_sum);
                        return Ok(Some((b, n)));
                    }
                    return Ok(None);
                }
            }

            let batch = self.batch.as_ref().unwrap();
            if self.row >= batch.row_count {
                self.batch = None;
                continue;
            }

            let b = batch.s_id.get(self.row);
            let c = batch.o_key.get(self.row);
            let w = if self.mixed && batch.o_type.get(self.row) != OType::IRI_REF.as_u16() {
                0
            } else {
                self.n3.get(&c).copied().unwrap_or(0)
            };

            match self.cur_b {
                None => {
                    self.cur_b = Some(b);
                    self.cur_sum = 0;
                }
                Some(cur) if cur != b => {
                    // Emit previous group, but consume this row into the next group.
                    let out_b = self.cur_b.replace(b).expect("checked: cur_b is Some");
                    let out_n = std::mem::replace(&mut self.cur_sum, w);
                    self.row += 1;
                    return Ok(Some((out_b, out_n)));
                }
                Some(_) => {}
            }

            self.cur_sum += w;
            self.row += 1;
        }
    }
}

fn count_optional_chain_head(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p1: &Ref,
    p2: &Ref,
    p3: &Ref,
) -> Result<Option<u64>> {
    let sid1 = normalize_pred_sid(store, p1)?;
    let sid2 = normalize_pred_sid(store, p2)?;
    let sid3 = normalize_pred_sid(store, p3)?;

    let Some(p1_id) = store.sid_to_p_id(&sid1) else {
        return Ok(Some(0));
    };
    let Some(p2_id) = store.sid_to_p_id(&sid2) else {
        return Ok(Some(0));
    };
    let Some(p3_id) = store.sid_to_p_id(&sid3) else {
        // Optional chain can never match => multiplier is 1 for all b.
        let mut it1 = PostObjectGroupCountIter::new(store, g_id, p1_id)?.ok_or(
            QueryError::Internal("optional chain-head: POST iterator unavailable".into()),
        )?;
        let mut total = 0u64;
        while let Some((_b, w)) = it1.next_group()? {
            total += w;
        }
        return Ok(Some(total));
    };

    // Precompute n3(c) = count_{p3}(c).
    let mut n3: FxHashMap<u64, u64> = FxHashMap::default();
    let mut it3 = PsotSubjectCountIter::new(store, g_id, p3_id)?;
    while let Some((c, n)) = it3.next_group()? {
        n3.insert(c, n);
    }

    let mut it1 = PostObjectGroupCountIter::new(store, g_id, p1_id)?.ok_or(
        QueryError::Internal("optional chain-head: POST iterator unavailable".into()),
    )?;
    let mut it2 = PsotSubjectSumN3Iter::new(store, g_id, p2_id, &n3)?.ok_or(
        QueryError::Internal("optional chain-head: PSOT iterator unavailable".into()),
    )?;

    let mut p2_cur = it2.next_group()?;
    let mut total = 0u64;

    while let Some((b, w)) = it1.next_group()? {
        while let Some((b2, _)) = p2_cur {
            if b2 < b {
                p2_cur = it2.next_group()?;
                continue;
            }
            break;
        }
        let sum_n3 = match p2_cur {
            Some((b2, n)) if b2 == b => {
                p2_cur = it2.next_group()?;
                n
            }
            _ => 0u64,
        };
        let mult = if sum_n3 == 0 { 1 } else { sum_n3 };
        total = total.saturating_add(w.saturating_mul(mult));
    }

    Ok(Some(total))
}
