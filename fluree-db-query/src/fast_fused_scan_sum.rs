//! Fast-path fused scan + SUM(scalar(?o)) for a single predicate.
//!
//! Targets benchmark-style queries like:
//!
//! ```sparql
//! SELECT (SUM(DAY(?o)) AS ?sum)
//! WHERE { ?s <p> ?o }
//! ```
//!
//! SPARQL lowering desugars `SUM(DAY(?o))` to a pre-aggregation `BIND` of the
//! expression into a synthetic var, then `SUM(?synthetic)`. This operator bypasses
//! that pipeline by scanning the predicate's POST index range and aggregating
//! directly from encoded `(o_type/o_kind, o_key)` without materializing per-row
//! bindings.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::triple::Ref;
use crate::var_registry::VarId;
use async_trait::async_trait;
use chrono::Datelike;
use fluree_db_binary_index::format::column_block::ColumnId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::{cmp_v2_for_order, RunRecordV2};
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_binary_index::{ColumnProjection, ColumnSet};
use fluree_db_core::o_type::{DecodeKind, OType};
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::ObjKey;
use fluree_db_core::{FlakeValue, GraphId, Sid};

/// Supported datetime component functions for the fast-path.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DateComponentFn {
    Year,
    Month,
    Day,
}

impl DateComponentFn {
    fn name(self) -> &'static str {
        match self {
            Self::Year => "YEAR",
            Self::Month => "MONTH",
            Self::Day => "DAY",
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum NumericUnaryFn {
    Abs,
    Ceil,
    Floor,
    Round,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ScalarI64Fn {
    DateComponent(DateComponentFn),
    NumericUnary(NumericUnaryFn),
}

impl ScalarI64Fn {
    fn name(self) -> &'static str {
        match self {
            Self::DateComponent(c) => c.name(),
            Self::NumericUnary(NumericUnaryFn::Abs) => "ABS",
            Self::NumericUnary(NumericUnaryFn::Ceil) => "CEIL",
            Self::NumericUnary(NumericUnaryFn::Floor) => "FLOOR",
            Self::NumericUnary(NumericUnaryFn::Round) => "ROUND",
        }
    }

    fn constant_for_otype(self, o_type: u16) -> Option<i64> {
        match self {
            Self::DateComponent(component) => constant_component_for_otype(o_type, component),
            Self::NumericUnary(_) => None,
        }
    }

    fn eval_i64(self, o_type: u16, o_key: u64) -> Option<i64> {
        match self {
            Self::DateComponent(component) => component_from_otype_okey(o_type, o_key, component),
            Self::NumericUnary(func) => {
                let ot = OType::from_u16(o_type);
                if ot.decode_kind() != DecodeKind::I64 {
                    return None;
                }
                let v = ObjKey::from_u64(o_key).decode_i64();
                match func {
                    NumericUnaryFn::Abs => v.checked_abs(),
                    NumericUnaryFn::Ceil | NumericUnaryFn::Floor | NumericUnaryFn::Round => Some(v),
                }
            }
        }
    }
}

/// Fused operator that outputs a single-row batch with the SUM result.
pub struct PredicateFusedScanSumI64Operator {
    predicate: Ref,
    scalar: ScalarI64Fn,
    out_var: VarId,
    state: OperatorState,
    done: bool,
    /// When fast-path is not available at runtime, fall back to this operator tree.
    fallback: Option<BoxedOperator>,
}

impl PredicateFusedScanSumI64Operator {
    pub fn new(
        predicate: Ref,
        scalar: ScalarI64Fn,
        out_var: VarId,
        fallback: Option<BoxedOperator>,
    ) -> Self {
        Self {
            predicate,
            scalar,
            out_var,
            state: OperatorState::Created,
            done: false,
            fallback,
        }
    }

    fn schema_arc(&self) -> std::sync::Arc<[VarId]> {
        std::sync::Arc::from(vec![self.out_var].into_boxed_slice())
    }

    fn build_output_batch(&self, sum: i64) -> Result<Batch> {
        let schema = self.schema_arc();
        let col = vec![Binding::lit(FlakeValue::Long(sum), Sid::xsd_integer())];
        Batch::new(schema, vec![col])
            .map_err(|e| QueryError::execution(format!("fast sum batch build: {e}")))
    }
}

#[async_trait]
impl Operator for PredicateFusedScanSumI64Operator {
    fn schema(&self) -> &[VarId] {
        // Note: when a fallback exists, its schema is identical by construction.
        std::slice::from_ref(&self.out_var)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        // Runtime gating: allow fast-path only in the same contexts as binary scans.
        let allow_fast = !ctx.history_mode
            && ctx.from_t.is_none()
            && ctx.policy_enforcer.as_ref().is_none_or(|p| p.is_root())
            && ctx.overlay.map(|o| o.epoch()).unwrap_or(0) == 0;

        if allow_fast {
            if let Some(binary_index_store) = ctx.binary_store.as_ref() {
                if ctx.to_t == binary_index_store.max_t() {
                    match sum_scalar_i64(
                        binary_index_store,
                        ctx.binary_g_id,
                        &self.predicate,
                        self.scalar,
                    )? {
                        Some(sum) => {
                            self.state = OperatorState::Open;
                            self.done = false;
                            self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(
                                self.build_output_batch(sum)?,
                            )));
                            return Ok(());
                        }
                        None => {
                            // Unsupported at runtime — fall through to planned pipeline.
                        }
                    }
                }
            }
        }

        // Fallback: delegate to the planned operator tree.
        let Some(fallback) = &mut self.fallback else {
            return Err(QueryError::Internal(format!(
                "{} fast-path unavailable and no fallback provided",
                self.scalar.name()
            )));
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
        self.done = true;
        self.state = OperatorState::Closed;
    }
}

/// Tiny helper operator: yields exactly one precomputed batch.
struct PrecomputedSingleBatchOperator {
    batch: Option<Batch>,
    state: OperatorState,
}

impl PrecomputedSingleBatchOperator {
    fn new(batch: Batch) -> Self {
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

fn sum_scalar_i64(
    store: &BinaryIndexStore,
    g_id: GraphId,
    predicate: &Ref,
    scalar: ScalarI64Fn,
) -> Result<Option<i64>> {
    let pred_sid = match predicate {
        Ref::Sid(s) => s.clone(),
        Ref::Iri(i) => store.encode_iri(i),
        Ref::Var(_) => {
            return Err(QueryError::Internal(
                "fast SUM(scalar) requires bound predicate".to_string(),
            ))
        }
    };
    let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
        // Predicate not present in the persisted dict — empty result.
        return Ok(Some(0));
    };

    let branch = match store.branch_for_order(g_id, RunSortOrder::Post) {
        Some(b) => b,
        None => return Ok(Some(0)),
    };

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
        t: u32::MAX,
        o_i: u32::MAX,
        o_type: u16::MAX,
        g_id,
    };
    let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);

    let mut sum: i64 = 0;

    for leaf_entry in &branch.leaves[leaf_range] {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;

        let dir = handle.dir();

        for (leaflet_idx, entry) in dir.entries.iter().enumerate() {
            if entry.row_count == 0 {
                continue;
            }
            // POST should always be predicate-homogeneous.
            if entry.p_const != Some(p_id) {
                continue;
            }

            // Constant folding: if scalar is constant for this o_type, avoid any column IO.
            if let Some(ot) = entry.o_type_const {
                if let Some(const_val) = scalar.constant_for_otype(ot) {
                    sum = sum.saturating_add(const_val.saturating_mul(entry.row_count as i64));
                    continue;
                }
            }

            let mut needed = ColumnSet::EMPTY;
            needed.insert(ColumnId::OKey);
            if entry.o_type_const.is_none() {
                needed.insert(ColumnId::OType);
            }
            let projection = ColumnProjection {
                output: ColumnSet::EMPTY,
                internal: needed,
            };

            let batch = handle
                .load_columns(leaflet_idx, &projection, RunSortOrder::Post)
                .map_err(|e| QueryError::Internal(format!("load columns: {e}")))?;

            for row in 0..batch.row_count {
                let o_key = batch.o_key.get(row);
                let ot = entry
                    .o_type_const
                    .unwrap_or_else(|| batch.o_type.get_or(row, 0));
                let Some(v) = scalar.eval_i64(ot, o_key) else {
                    // Unsupported datatype mix for this scalar fast-path.
                    return Ok(None);
                };
                sum = sum.saturating_add(v);
            }
        }
    }

    Ok(Some(sum))
}

fn constant_component_for_otype(o_type: u16, component: DateComponentFn) -> Option<i64> {
    let ot = OType::from_u16(o_type);
    match component {
        DateComponentFn::Year => None,
        DateComponentFn::Month => {
            if ot == OType::XSD_G_YEAR || ot == OType::XSD_G_DAY {
                Some(1)
            } else {
                None
            }
        }
        DateComponentFn::Day => {
            if ot == OType::XSD_G_YEAR || ot == OType::XSD_G_YEAR_MONTH || ot == OType::XSD_G_MONTH
            {
                Some(1)
            } else {
                None
            }
        }
    }
}

fn component_from_otype_okey(o_type: u16, o_key: u64, component: DateComponentFn) -> Option<i64> {
    let ot = OType::from_u16(o_type);
    let key = ObjKey::from_u64(o_key);

    // Defaulting semantics match helpers.rs promotion:
    // - gYear → Jan 1, 00:00:00
    // - gYearMonth → day=1
    // - gMonth/gDay/gMonthDay → year=1970, missing parts default to 1
    const DEFAULT_YEAR: i64 = 1970;
    const DEFAULT_MONTH: i64 = 1;

    let (year, month, day) = if ot == OType::XSD_G_YEAR {
        (key.decode_g_year() as i64, 1, 1)
    } else if ot == OType::XSD_G_YEAR_MONTH {
        let (y, m) = key.decode_g_year_month();
        (y as i64, m as i64, 1)
    } else if ot == OType::XSD_G_MONTH {
        (DEFAULT_YEAR, key.decode_g_month() as i64, 1)
    } else if ot == OType::XSD_G_DAY {
        (DEFAULT_YEAR, DEFAULT_MONTH, key.decode_g_day() as i64)
    } else if ot == OType::XSD_G_MONTH_DAY {
        let (m, d) = key.decode_g_month_day();
        (DEFAULT_YEAR, m as i64, d as i64)
    } else if ot == OType::XSD_DATE {
        // xsd:date: days since Unix epoch (1970-01-01)
        let days = key.decode_date() as i64;
        let base = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
        let dt = base.checked_add_signed(chrono::Duration::days(days))?;
        (dt.year() as i64, dt.month() as i64, dt.day() as i64)
    } else if ot == OType::XSD_DATE_TIME {
        // xsd:dateTime: epoch micros; interpret in UTC for component extraction.
        let micros = key.decode_datetime();
        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(micros)?;
        (dt.year() as i64, dt.month() as i64, dt.day() as i64)
    } else {
        return None;
    };

    match component {
        DateComponentFn::Year => Some(year),
        DateComponentFn::Month => Some(month),
        DateComponentFn::Day => Some(day),
    }
}
