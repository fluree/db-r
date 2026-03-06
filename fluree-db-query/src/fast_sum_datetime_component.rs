//! Fast-path fused scan + SUM(datetime-component(?o)) for a single predicate.
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
use fluree_db_binary_index::format::leaf_v3::{
    decode_leaf_dir_v3_with_base, decode_leaf_header_v3,
};
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::{cmp_v2_for_order, RunRecordV2};
use fluree_db_binary_index::read::column_loader::load_leaflet_columns;
use fluree_db_binary_index::{BinaryCursor, BinaryFilter, BinaryIndexStore, BinaryIndexStoreV6};
use fluree_db_binary_index::{ColumnProjection, ColumnSet};
use fluree_db_core::o_type::OType;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::{ObjKey, ObjKind};
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

/// Fused operator that outputs a single-row batch with the SUM result.
pub struct PredicateSumDateComponentOperator {
    predicate: Ref,
    component: DateComponentFn,
    out_var: VarId,
    state: OperatorState,
    done: bool,
    /// When fast-path is not available at runtime, fall back to this operator tree.
    fallback: Option<BoxedOperator>,
}

impl PredicateSumDateComponentOperator {
    pub fn new(
        predicate: Ref,
        component: DateComponentFn,
        out_var: VarId,
        fallback: Option<BoxedOperator>,
    ) -> Self {
        Self {
            predicate,
            component,
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
impl Operator for PredicateSumDateComponentOperator {
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
            && ctx.policy_enforcer.as_ref().is_none_or(|p| p.is_root());

        if allow_fast {
            if let Some(store_v6) = ctx.binary_store_v6.as_ref() {
                let sum =
                    sum_component_v6(store_v6, ctx.binary_g_id, &self.predicate, self.component)?;
                self.state = OperatorState::Open;
                self.done = false;
                // Store sum in fallback-less path by stashing it in done-state batch generation.
                // We simply generate the batch on first next_batch().
                self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(
                    self.build_output_batch(sum)?,
                )));
                return Ok(());
            }

            if let Some(store_v5) = ctx.binary_store.as_ref() {
                let sum =
                    sum_component_v5(store_v5, ctx.binary_g_id, &self.predicate, self.component)?;
                self.state = OperatorState::Open;
                self.done = false;
                self.fallback = Some(Box::new(PrecomputedSingleBatchOperator::new(
                    self.build_output_batch(sum)?,
                )));
                return Ok(());
            }
        }

        // Fallback: delegate to the planned operator tree.
        let Some(fallback) = &mut self.fallback else {
            return Err(QueryError::Internal(format!(
                "{} fast-path unavailable and no fallback provided",
                self.component.name()
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
            state: OperatorState::Created,
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

// ============================================================================
// V6 (V3 format) implementation
// ============================================================================

fn sum_component_v6(
    store: &BinaryIndexStoreV6,
    g_id: GraphId,
    predicate: &Ref,
    component: DateComponentFn,
) -> Result<i64> {
    let pred_sid = match predicate {
        Ref::Sid(s) => s.clone(),
        Ref::Iri(i) => store.encode_iri(i),
        Ref::Var(_) => {
            return Err(QueryError::Internal(
                "fast SUM(component) requires bound predicate".to_string(),
            ))
        }
    };
    let p_id = store
        .sid_to_p_id(&pred_sid)
        .ok_or_else(|| QueryError::Internal("predicate not found in v6 dictionary".to_string()))?;

    // Restrict to the POST order leaf range for this predicate.
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
        g_id: g_id as u16,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId(u64::MAX),
        o_key: u64::MAX,
        p_id,
        t: u32::MAX,
        o_i: u32::MAX,
        o_type: u16::MAX,
        g_id: g_id as u16,
    };
    let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);

    let mut sum: i64 = 0;

    for leaf_entry in &branch.leaves[leaf_range] {
        let bytes = store
            .get_leaf_bytes_sync(&leaf_entry.leaf_cid)
            .map_err(|e| QueryError::Internal(format!("leaf fetch: {e}")))?;
        let header =
            decode_leaf_header_v3(&bytes).map_err(|e| QueryError::Internal(e.to_string()))?;
        let dir = decode_leaf_dir_v3_with_base(&bytes, &header)
            .map_err(|e| QueryError::Internal(e.to_string()))?;

        for entry in &dir.entries {
            if entry.row_count == 0 {
                continue;
            }
            // POST should always be predicate-homogeneous.
            if entry.p_const != Some(p_id) {
                continue;
            }

            // If the component is constant for this o_type, skip decoding all columns.
            if let Some(ot) = entry.o_type_const {
                if let Some(const_val) = constant_component_for_otype(ot, component) {
                    sum = sum.saturating_add(const_val.saturating_mul(entry.row_count as i64));
                    continue;
                }
            }

            // Decode only the minimal required columns (o_key, plus o_type when not constant).
            let mut needed = ColumnSet::EMPTY;
            needed.insert(ColumnId::OKey);
            let needs_o_type_col = entry.o_type_const.is_none();
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
                let o_key = batch.o_key.get(row);
                let ot = entry
                    .o_type_const
                    .unwrap_or_else(|| batch.o_type.get_or(row, 0));
                if let Some(v) = component_from_otype_okey(ot, o_key, component) {
                    sum = sum.saturating_add(v);
                }
            }
        }
    }

    Ok(sum)
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

// ============================================================================
// V5 (IRB1) implementation (fallback)
// ============================================================================

fn sum_component_v5(
    store: &std::sync::Arc<BinaryIndexStore>,
    g_id: GraphId,
    predicate: &Ref,
    component: DateComponentFn,
) -> Result<i64> {
    let pred_sid = match predicate {
        Ref::Sid(s) => s.clone(),
        Ref::Iri(i) => store.encode_iri(i),
        Ref::Var(_) => {
            return Err(QueryError::Internal(
                "fast SUM(component) requires bound predicate".to_string(),
            ))
        }
    };
    let p_id = store
        .sid_to_p_id(&pred_sid)
        .ok_or_else(|| QueryError::Internal("predicate not found in v5 dictionary".to_string()))?;

    // Use translate_range to get routing bounds for POST.
    let bounds = store
        .translate_range(None, Some(&pred_sid), None, RunSortOrder::Post, g_id)
        .map_err(|e| QueryError::Internal(format!("translate_range: {e}")))?
        .ok_or_else(|| QueryError::Internal("no range bounds for predicate".to_string()))?;
    let (min_key, max_key) = bounds;

    let mut filter = BinaryFilter::new();
    filter.p_id = Some(p_id);

    // need_region2=false: component extraction uses only o_kind/o_key.
    let mut cursor = BinaryCursor::new(
        std::sync::Arc::clone(store),
        RunSortOrder::Post,
        g_id,
        &min_key,
        &max_key,
        filter,
        false,
    );

    let mut sum: i64 = 0;
    while let Some(batch) = cursor
        .next_leaf()
        .map_err(|e| QueryError::Internal(format!("cursor: {e}")))?
    {
        for i in 0..batch.row_count {
            let ok = ObjKind::from_u8(batch.o_kinds[i]);
            let key = ObjKey::from_u64(batch.o_keys[i]);
            if let Some(v) = component_from_okey_v5(ok, key, component) {
                sum = sum.saturating_add(v);
            }
        }
    }
    Ok(sum)
}

fn component_from_okey_v5(kind: ObjKind, key: ObjKey, component: DateComponentFn) -> Option<i64> {
    // Same defaulting semantics as parse_datetime_from_binding() promotion.
    const DEFAULT_YEAR: i64 = 1970;
    const DEFAULT_MONTH: i64 = 1;

    let (year, month, day) = match kind.as_u8() {
        x if x == ObjKind::G_YEAR.as_u8() => (key.decode_g_year() as i64, 1, 1),
        x if x == ObjKind::G_YEAR_MONTH.as_u8() => {
            let (y, m) = key.decode_g_year_month();
            (y as i64, m as i64, 1)
        }
        x if x == ObjKind::G_MONTH.as_u8() => (DEFAULT_YEAR, key.decode_g_month() as i64, 1),
        x if x == ObjKind::G_DAY.as_u8() => {
            (DEFAULT_YEAR, DEFAULT_MONTH, key.decode_g_day() as i64)
        }
        x if x == ObjKind::G_MONTH_DAY.as_u8() => {
            let (m, d) = key.decode_g_month_day();
            (DEFAULT_YEAR, m as i64, d as i64)
        }
        x if x == ObjKind::DATE.as_u8() => {
            let days = key.decode_date() as i64;
            let base = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
            let dt = base.checked_add_signed(chrono::Duration::days(days))?;
            (dt.year() as i64, dt.month() as i64, dt.day() as i64)
        }
        x if x == ObjKind::DATE_TIME.as_u8() => {
            let micros = key.decode_datetime();
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(micros)?;
            (dt.year() as i64, dt.month() as i64, dt.day() as i64)
        }
        _ => return None,
    };

    match component {
        DateComponentFn::Year => Some(year),
        DateComponentFn::Month => Some(month),
        DateComponentFn::Day => Some(day),
    }
}
