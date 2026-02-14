//! Streaming GROUP BY + Aggregate operator for O(groups) memory usage.
//!
//! The standard `GroupByOperator` + `AggregateOperator` pipeline stores O(rows) in memory
//! because it collects all rows before computing aggregates. This is problematic for
//! queries like:
//!
//! ```sparql
//! SELECT ?venue (COUNT(?paper) as ?count)
//! WHERE { ?paper dblp:publishedIn ?venue . }
//! GROUP BY ?venue
//! ```
//!
//! With millions of papers but thousands of venues, storing all papers per venue
//! uses gigabytes of memory when we only need to count them.
//!
//! `GroupAggregateOperator` solves this by:
//! 1. Computing aggregates incrementally as rows arrive (streaming)
//! 2. Storing only `O(groups)` state, not `O(rows)`
//! 3. Using `JoinKey` for group keys (no decoding in single-ledger mode)
//!
//! # Supported Streamable Aggregates
//!
//! - COUNT, COUNT(*) - just increment counter
//! - COUNT(DISTINCT) - maintain HashSet (still O(distinct values) per group)
//! - SUM, AVG - track sum and count
//! - MIN, MAX - track current min/max
//!
//! # Non-Streamable Aggregates (fallback to collect)
//!
//! - GROUP_CONCAT - needs all values for concatenation
//! - MEDIAN - needs all values for sorting
//! - VARIANCE, STDDEV - could be done with Welford's algorithm but not yet
//! - SAMPLE - could pick first, but semantic might expect random
//!
//! When any non-streamable aggregate is present, we fall back to the standard
//! GROUP BY behavior for that column only.

use crate::aggregate::AggregateFn;
use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
// Note: JoinKey and Materializer would be used for multi-ledger/dataset mode
// but for now we use GroupKeyOwned for single-ledger simplicity
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::{FlakeValue, Sid};
use fluree_db_indexer::run_index::BinaryIndexStore;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;

/// Specification for a streaming aggregate
#[derive(Debug, Clone)]
pub struct StreamingAggSpec {
    /// The aggregate function
    pub function: AggregateFn,
    /// Input column index (None for COUNT(*))
    pub input_col: Option<usize>,
    /// Output variable ID
    pub output_var: VarId,
}

/// Per-group streaming aggregate state
#[derive(Debug)]
enum AggState {
    /// COUNT/COUNT(*) - just a counter
    Count { n: u64 },
    /// COUNT(DISTINCT) - HashSet of seen values
    CountDistinct { seen: HashSet<GroupKeyOwned> },
    /// SUM - running total (as f64 for mixed types)
    Sum { total: f64, has_int_only: bool },
    /// AVG - sum and count
    Avg { sum: f64, count: u64 },
    /// MIN - current minimum (stores materialized binding for correct comparison)
    Min { min: Option<Binding> },
    /// MAX - current maximum (stores materialized binding for correct comparison)
    Max { max: Option<Binding> },
    /// Fallback: collect all values (for GROUP_CONCAT, MEDIAN, etc.)
    Collect { values: Vec<Binding> },
}

/// Materialize an encoded binding for MIN/MAX comparison.
///
/// EncodedSid/EncodedPid raw IDs don't have semantic ordering (s_id=100 for "zebra"
/// would incorrectly compare > s_id=50 for "apple"). We must decode to get correct
/// term ordering via namespace/name comparison.
fn materialize_for_minmax(binding: &Binding, store: Option<&BinaryIndexStore>) -> Binding {
    let Some(store) = store else {
        // No store available - return as-is (will use raw ID comparison as fallback)
        return binding.clone();
    };

    match binding {
        Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            lang_id,
            i_val,
            t,
        } => {
            match store.decode_value(*o_kind, *o_key, *p_id) {
                Ok(fluree_db_core::FlakeValue::Ref(sid)) => Binding::Sid(sid),
                Ok(val) => {
                    let dt_sid = store
                        .dt_sids()
                        .get(*dt_id as usize)
                        .cloned()
                        .unwrap_or_else(|| Sid::new(0, ""));
                    let meta = store.decode_meta(*lang_id, *i_val);
                    Binding::Lit {
                        val,
                        dt: dt_sid,
                        lang: meta.and_then(|m| m.lang.map(Arc::from)),
                        t: Some(*t),
                        op: None,
                    }
                }
                Err(_) => binding.clone(), // Decode error - keep encoded
            }
        }
        Binding::EncodedSid { s_id } => {
            match store.resolve_subject_iri(*s_id) {
                Ok(iri) => Binding::Sid(store.encode_iri(&iri)),
                Err(_) => binding.clone(), // Resolution error - keep encoded
            }
        }
        Binding::EncodedPid { p_id } => {
            match store.resolve_predicate_iri(*p_id) {
                Some(iri) => Binding::Sid(store.encode_iri(iri)),
                None => binding.clone(), // Resolution error - keep encoded
            }
        }
        _ => binding.clone(), // Already materialized or special type
    }
}

impl AggState {
    fn new(func: &AggregateFn) -> Self {
        match func {
            AggregateFn::Count | AggregateFn::CountAll => AggState::Count { n: 0 },
            AggregateFn::CountDistinct => AggState::CountDistinct {
                seen: HashSet::new(),
            },
            AggregateFn::Sum => AggState::Sum {
                total: 0.0,
                has_int_only: true,
            },
            AggregateFn::Avg => AggState::Avg { sum: 0.0, count: 0 },
            AggregateFn::Min => AggState::Min { min: None },
            AggregateFn::Max => AggState::Max { max: None },
            // Non-streamable: collect all values
            AggregateFn::Median
            | AggregateFn::Variance
            | AggregateFn::Stddev
            | AggregateFn::GroupConcat { .. }
            | AggregateFn::Sample => AggState::Collect { values: Vec::new() },
        }
    }

    /// Update state with a new binding value
    ///
    /// # Arguments
    ///
    /// * `binding` - The binding value to incorporate
    /// * `store` - Optional binary store for materializing encoded bindings (needed for MIN/MAX)
    fn update(&mut self, binding: &Binding, store: Option<&BinaryIndexStore>) {
        match self {
            AggState::Count { n } => {
                // COUNT: count non-Unbound values (COUNT(*) counts all via CountAll variant)
                if !matches!(binding, Binding::Unbound | Binding::Poisoned) {
                    *n += 1;
                }
            }
            AggState::CountDistinct { seen } => {
                if !matches!(binding, Binding::Unbound | Binding::Poisoned) {
                    // Convert binding to owned group key for HashSet
                    let key = binding_to_group_key_owned(binding);
                    seen.insert(key);
                }
            }
            AggState::Sum {
                total,
                has_int_only,
            } => {
                if let Some(num) = extract_number(binding) {
                    *total += num;
                    if !is_int_binding(binding) {
                        *has_int_only = false;
                    }
                }
            }
            AggState::Avg { sum, count } => {
                if let Some(num) = extract_number(binding) {
                    *sum += num;
                    *count += 1;
                }
            }
            AggState::Min { min } => {
                if !matches!(
                    binding,
                    Binding::Unbound | Binding::Poisoned | Binding::Grouped(_)
                ) {
                    // Materialize encoded bindings for correct semantic comparison.
                    // Raw s_id comparison would give wrong ordering (s_id=100 for "zebra"
                    // would incorrectly be > s_id=50 for "apple").
                    let materialized = materialize_for_minmax(binding, store);
                    match min {
                        None => *min = Some(materialized),
                        Some(current) => {
                            if crate::sort::compare_bindings(&materialized, current)
                                == std::cmp::Ordering::Less
                            {
                                *min = Some(materialized);
                            }
                        }
                    }
                }
            }
            AggState::Max { max } => {
                if !matches!(
                    binding,
                    Binding::Unbound | Binding::Poisoned | Binding::Grouped(_)
                ) {
                    // Materialize encoded bindings for correct semantic comparison.
                    let materialized = materialize_for_minmax(binding, store);
                    match max {
                        None => *max = Some(materialized),
                        Some(current) => {
                            if crate::sort::compare_bindings(&materialized, current)
                                == std::cmp::Ordering::Greater
                            {
                                *max = Some(materialized);
                            }
                        }
                    }
                }
            }
            AggState::Collect { values } => {
                values.push(binding.clone());
            }
        }
    }

    /// Update for COUNT(*) - increment regardless of binding value
    fn update_count_all(&mut self) {
        if let AggState::Count { n } = self {
            *n += 1;
        }
    }

    /// Finalize the aggregate state into a result binding
    fn finalize(self, func: &AggregateFn) -> Binding {
        match self {
            AggState::Count { n } => Binding::lit(FlakeValue::Long(n as i64), xsd_long()),
            AggState::CountDistinct { seen } => {
                Binding::lit(FlakeValue::Long(seen.len() as i64), xsd_long())
            }
            AggState::Sum {
                total,
                has_int_only,
            } => {
                if has_int_only && total.fract() == 0.0 {
                    Binding::lit(FlakeValue::Long(total as i64), xsd_long())
                } else {
                    Binding::lit(FlakeValue::Double(total), xsd_double())
                }
            }
            AggState::Avg { sum, count } => {
                if count == 0 {
                    Binding::Unbound
                } else {
                    Binding::lit(FlakeValue::Double(sum / count as f64), xsd_double())
                }
            }
            AggState::Min { min } => min.unwrap_or(Binding::Unbound),
            AggState::Max { max } => max.unwrap_or(Binding::Unbound),
            AggState::Collect { values } => {
                // Use existing aggregate functions for non-streamable
                crate::aggregate::apply_aggregate(func, &Binding::Grouped(values))
            }
        }
    }
}

/// Owned group key for HashMap storage
/// Uses the same semantics as JoinKey but owns its data
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum GroupKeyOwned {
    /// Single-ledger: raw s_id
    Sid(u64),
    /// Single-ledger: raw p_id
    Pid(u32),
    /// Encoded literal key
    Lit {
        o_kind: u8,
        o_key: u64,
        /// `p_id` is only required for NUM_BIG decoding (per-predicate arena).
        /// For all other literal kinds, it must not affect grouping identity.
        p_id_for_numbig: Option<u32>,
        dt_id: u16,
        lang_id: u16,
    },
    /// Materialized Sid (namespace_code, name)
    MaterializedSid(u16, Arc<str>),
    /// Materialized literal value
    MaterializedLit(MaterializedLitKey),
    /// Unbound/Poisoned
    Absent,
}

/// Hashable key for materialized literals
/// Includes datatype and language for correct GROUP BY / COUNT(DISTINCT) semantics.
/// Without these, "1"^^xsd:string and "1"^^xsd:integer would incorrectly merge.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MaterializedLitKey {
    discriminant: u8,
    // For strings/json: the string value
    string_val: Option<Arc<str>>,
    // For numbers: bits representation
    number_bits: Option<u64>,
    // For booleans
    bool_val: Option<bool>,
    // Datatype SID (namespace_code, name) - critical for correct comparison
    dt: Option<(u16, Arc<str>)>,
    // Language tag
    lang: Option<Arc<str>>,
}

impl Hash for MaterializedLitKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.discriminant.hash(state);
        self.string_val.hash(state);
        self.number_bits.hash(state);
        self.bool_val.hash(state);
        // Critical: include datatype and language for correct GROUP BY / COUNT(DISTINCT) semantics
        // Without these, "1"^^xsd:string and "1"^^xsd:integer would incorrectly hash the same
        self.dt.hash(state);
        self.lang.hash(state);
    }
}

/// Convert a binding to an owned group key
fn binding_to_group_key_owned(binding: &Binding) -> GroupKeyOwned {
    match binding {
        Binding::EncodedSid { s_id } => GroupKeyOwned::Sid(*s_id),
        Binding::EncodedPid { p_id } => GroupKeyOwned::Pid(*p_id),
        Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            dt_id,
            lang_id,
            ..
        } => GroupKeyOwned::Lit {
            o_kind: *o_kind,
            o_key: *o_key,
            p_id_for_numbig: if *o_kind == fluree_db_core::ObjKind::NUM_BIG.as_u8() {
                Some(*p_id)
            } else {
                None
            },
            dt_id: *dt_id,
            lang_id: *lang_id,
        },
        Binding::Sid(sid) => GroupKeyOwned::MaterializedSid(sid.namespace_code, sid.name.clone()),
        Binding::Lit { val, dt, lang, .. } => {
            GroupKeyOwned::MaterializedLit(flake_value_to_key(val, dt, lang.as_ref()))
        }
        Binding::Unbound | Binding::Poisoned => GroupKeyOwned::Absent,
        Binding::IriMatch { iri, .. } => {
            // Use full IRI for cross-ledger correctness
            GroupKeyOwned::MaterializedSid(0, iri.clone())
        }
        Binding::Iri(iri) => {
            // Plain IRI string
            GroupKeyOwned::MaterializedSid(0, iri.clone())
        }
        Binding::Grouped(_) => GroupKeyOwned::Absent, // Shouldn't happen
    }
}

fn flake_value_to_key(val: &FlakeValue, dt: &Sid, lang: Option<&Arc<str>>) -> MaterializedLitKey {
    // Convert Sid to (namespace_code, name) tuple for dt
    let dt_key = Some((dt.namespace_code, dt.name.clone()));
    // Clone language tag
    let lang_key = lang.cloned();

    match val {
        FlakeValue::String(s) => MaterializedLitKey {
            discriminant: 1,
            string_val: Some(Arc::from(s.as_str())),
            number_bits: None,
            bool_val: None,
            dt: dt_key,
            lang: lang_key,
        },
        FlakeValue::Long(n) => MaterializedLitKey {
            discriminant: 2,
            string_val: None,
            number_bits: Some(*n as u64),
            bool_val: None,
            dt: dt_key,
            lang: lang_key,
        },
        FlakeValue::Double(d) => MaterializedLitKey {
            discriminant: 3,
            string_val: None,
            number_bits: Some(d.to_bits()),
            bool_val: None,
            dt: dt_key,
            lang: lang_key,
        },
        FlakeValue::Boolean(b) => MaterializedLitKey {
            discriminant: 4,
            string_val: None,
            number_bits: None,
            bool_val: Some(*b),
            dt: dt_key,
            lang: lang_key,
        },
        _ => MaterializedLitKey {
            discriminant: 0,
            string_val: Some(Arc::from(format!("{:?}", val))),
            number_bits: None,
            bool_val: None,
            dt: dt_key,
            lang: lang_key,
        },
    }
}

/// Composite group key (multiple columns)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CompositeGroupKey(Vec<GroupKeyOwned>);

/// Per-group state: the key bindings and aggregate states
struct GroupState {
    /// Original bindings for group key columns (for output)
    key_bindings: Vec<Binding>,
    /// Aggregate states (one per aggregate spec)
    agg_states: Vec<AggState>,
}

/// Streaming GROUP BY + Aggregate operator
///
/// Memory usage: O(groups) instead of O(rows)
pub struct GroupAggregateOperator {
    /// Child operator
    child: BoxedOperator,
    /// Output schema
    schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Group key column indices
    group_key_indices: Vec<usize>,
    /// Aggregate specifications
    agg_specs: Vec<StreamingAggSpec>,
    /// Accumulated groups: composite_key -> group_state
    groups: HashMap<CompositeGroupKey, GroupState>,
    /// Iterator for emitting results
    emit_iter: Option<std::collections::hash_map::IntoIter<CompositeGroupKey, GroupState>>,
    /// Binary index store for materializing encoded bindings (used for MIN/MAX semantic ordering).
    binary_store: Option<Arc<BinaryIndexStore>>,
}

impl GroupAggregateOperator {
    /// Create a streaming GROUP BY + Aggregate operator
    ///
    /// # Arguments
    ///
    /// * `child` - Input operator
    /// * `group_vars` - Variables to group by
    /// * `agg_specs` - Aggregate specifications (input col, function, output var)
    /// * `binary_store` - Optional binary store for encoded binding materialization
    pub fn new(
        child: BoxedOperator,
        group_vars: Vec<VarId>,
        agg_specs: Vec<StreamingAggSpec>,
        binary_store: Option<Arc<BinaryIndexStore>>,
    ) -> Self {
        let child_schema = child.schema().to_vec();

        // Compute indices for group key columns
        let group_key_indices: Vec<usize> = group_vars
            .iter()
            .map(|v| {
                child_schema
                    .iter()
                    .position(|sv| sv == v)
                    .unwrap_or_else(|| panic!("GROUP BY variable {:?} not in schema", v))
            })
            .collect();

        // Build output schema: group keys + aggregate outputs
        let mut output_vars: Vec<VarId> = group_vars.clone();
        for spec in &agg_specs {
            output_vars.push(spec.output_var);
        }

        let schema = Arc::from(output_vars.into_boxed_slice());

        Self {
            child,
            schema,
            state: OperatorState::Created,
            group_key_indices,
            agg_specs,
            groups: HashMap::new(),
            emit_iter: None,
            binary_store,
        }
    }

    /// Check if all aggregates are streamable (for planner optimization decisions)
    pub fn all_streamable(specs: &[StreamingAggSpec]) -> bool {
        specs.iter().all(|spec| {
            matches!(
                spec.function,
                AggregateFn::Count
                    | AggregateFn::CountAll
                    | AggregateFn::CountDistinct
                    | AggregateFn::Sum
                    | AggregateFn::Avg
                    | AggregateFn::Min
                    | AggregateFn::Max
            )
        })
    }

    /// Extract composite group key from a row
    fn extract_group_key(&self, batch: &Batch, row_idx: usize) -> CompositeGroupKey {
        let keys: Vec<GroupKeyOwned> = self
            .group_key_indices
            .iter()
            .map(|&col_idx| {
                let binding = batch.get_by_col(row_idx, col_idx);
                binding_to_group_key_owned(binding)
            })
            .collect();
        CompositeGroupKey(keys)
    }

    /// Extract original bindings for group key columns (for output)
    fn extract_key_bindings(&self, batch: &Batch, row_idx: usize) -> Vec<Binding> {
        self.group_key_indices
            .iter()
            .map(|&col_idx| batch.get_by_col(row_idx, col_idx).clone())
            .collect()
    }
}

#[async_trait]
impl Operator for GroupAggregateOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        self.groups.clear();
        self.emit_iter = None;
        // If the execution context has a binary store, use it to materialize
        // encoded bindings for correct MIN/MAX comparison semantics.
        if self.binary_store.is_none() {
            self.binary_store = ctx.binary_store.clone();
        }
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        // If we haven't consumed all input yet, do so now (streaming aggregation)
        if self.emit_iter.is_none() {
            let span = tracing::info_span!(
                "group_aggregate_streaming",
                group_key_cols = self.group_key_indices.len(),
                agg_count = self.agg_specs.len(),
                input_batches = tracing::field::Empty,
                input_rows = tracing::field::Empty,
                groups = tracing::field::Empty,
                drain_ms = tracing::field::Empty
            );
            let _g = span.enter();
            let drain_start = Instant::now();
            let mut input_batches: u64 = 0;
            let mut input_rows: u64 = 0;

            // Drain all input, updating aggregate states incrementally
            loop {
                let batch = match self.child.next_batch(ctx).await? {
                    Some(b) => b,
                    None => break,
                };
                input_batches += 1;

                if batch.is_empty() {
                    continue;
                }

                // Process each row
                for row_idx in 0..batch.len() {
                    input_rows += 1;

                    // Extract composite group key
                    let group_key = self.extract_group_key(&batch, row_idx);

                    // Extract key bindings BEFORE the mutable borrow to avoid borrow conflict
                    let key_bindings = self.extract_key_bindings(&batch, row_idx);

                    // Pre-compute aggregate states initialization
                    let agg_specs_ref = &self.agg_specs;

                    // Get or create group state
                    let group_state = self.groups.entry(group_key).or_insert_with(|| GroupState {
                        key_bindings,
                        agg_states: agg_specs_ref
                            .iter()
                            .map(|spec| AggState::new(&spec.function))
                            .collect(),
                    });

                    // Update each aggregate with this row's values
                    let store_ref = self.binary_store.as_deref();
                    for (agg_idx, spec) in self.agg_specs.iter().enumerate() {
                        match spec.input_col {
                            Some(col_idx) => {
                                let binding = batch.get_by_col(row_idx, col_idx);
                                group_state.agg_states[agg_idx].update(binding, store_ref);
                            }
                            None => {
                                // COUNT(*) - count all rows
                                group_state.agg_states[agg_idx].update_count_all();
                            }
                        }
                    }
                }
            }

            span.record("input_batches", input_batches);
            span.record("input_rows", input_rows);
            span.record("groups", self.groups.len() as u64);
            span.record(
                "drain_ms",
                (drain_start.elapsed().as_secs_f64() * 1000.0) as u64,
            );

            // Prepare iterator for emission
            let groups = std::mem::take(&mut self.groups);
            self.emit_iter = Some(groups.into_iter());
        }

        // Emit batches from accumulated groups
        let batch_size = ctx.batch_size;
        let num_cols = self.schema.len();
        let mut output_columns: Vec<Vec<Binding>> = (0..num_cols)
            .map(|_| Vec::with_capacity(batch_size))
            .collect();
        let mut rows_added = 0;

        if let Some(ref mut iter) = self.emit_iter {
            while rows_added < batch_size {
                match iter.next() {
                    Some((_composite_key, group_state)) => {
                        // Output group key columns
                        for (col_idx, key_binding) in
                            group_state.key_bindings.into_iter().enumerate()
                        {
                            output_columns[col_idx].push(key_binding);
                        }

                        // Output aggregate results
                        let key_col_count = self.group_key_indices.len();
                        for (agg_idx, agg_state) in group_state.agg_states.into_iter().enumerate() {
                            let result = agg_state.finalize(&self.agg_specs[agg_idx].function);
                            output_columns[key_col_count + agg_idx].push(result);
                        }

                        rows_added += 1;
                    }
                    None => {
                        self.state = OperatorState::Exhausted;
                        break;
                    }
                }
            }
        }

        if rows_added == 0 {
            return Ok(None);
        }

        Ok(Some(Batch::new(self.schema.clone(), output_columns)?))
    }

    fn close(&mut self) {
        self.child.close();
        self.groups.clear();
        self.emit_iter = None;
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // We don't know group count without running
        None
    }
}

/// XSD datatype SIDs
fn xsd_long() -> Sid {
    Sid::new(2, "long")
}

fn xsd_double() -> Sid {
    Sid::new(2, "double")
}

/// Extract numeric value from binding
fn extract_number(binding: &Binding) -> Option<f64> {
    use fluree_db_core::value_id::{ObjKey, ObjKind};

    match binding {
        Binding::Lit { val, .. } => match val {
            FlakeValue::Long(n) => Some(*n as f64),
            FlakeValue::Double(d) if !d.is_nan() => Some(*d),
            _ => None,
        },
        Binding::EncodedLit { o_kind, o_key, .. } => {
            // Decode numeric value from o_key based on o_kind
            // i_val is list index metadata, NOT the numeric value!
            if *o_kind == ObjKind::NUM_INT.as_u8() {
                // i64 encoded in o_key via order-preserving XOR transform
                Some(ObjKey::from_u64(*o_key).decode_i64() as f64)
            } else if *o_kind == ObjKind::NUM_F64.as_u8() {
                // f64 encoded in o_key via order-preserving bit transform
                let decoded = ObjKey::from_u64(*o_key).decode_f64();
                if decoded.is_nan() {
                    None
                } else {
                    Some(decoded)
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Check if binding is an integer type
fn is_int_binding(binding: &Binding) -> bool {
    match binding {
        Binding::Lit { val, .. } => matches!(val, FlakeValue::Long(_)),
        Binding::EncodedLit { o_kind, .. } => {
            // ObjKind for Long is typically stored as a specific value
            // This needs to match the actual encoding
            *o_kind == fluree_db_core::ObjKind::NUM_INT.as_u8()
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::Db;

    fn make_test_db() -> Db {
        Db::genesis("test/main")
    }

    #[tokio::test]
    async fn test_streaming_count() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;

        let db = make_test_db();
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&db, &vars);

        // Create input: 5 papers for venue A, 3 papers for venue B
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice()); // ?venue, ?paper
        let columns = vec![
            // ?venue
            vec![
                Binding::Sid(Sid::new(100, "venueA")),
                Binding::Sid(Sid::new(100, "venueA")),
                Binding::Sid(Sid::new(100, "venueA")),
                Binding::Sid(Sid::new(100, "venueA")),
                Binding::Sid(Sid::new(100, "venueA")),
                Binding::Sid(Sid::new(100, "venueB")),
                Binding::Sid(Sid::new(100, "venueB")),
                Binding::Sid(Sid::new(100, "venueB")),
            ],
            // ?paper
            vec![
                Binding::Sid(Sid::new(200, "paper1")),
                Binding::Sid(Sid::new(200, "paper2")),
                Binding::Sid(Sid::new(200, "paper3")),
                Binding::Sid(Sid::new(200, "paper4")),
                Binding::Sid(Sid::new(200, "paper5")),
                Binding::Sid(Sid::new(200, "paper6")),
                Binding::Sid(Sid::new(200, "paper7")),
                Binding::Sid(Sid::new(200, "paper8")),
            ],
        ];
        let batch = Batch::new(schema.clone(), columns).unwrap();

        struct BatchOperator {
            schema: Arc<[VarId]>,
            batch: Option<Batch>,
        }
        #[async_trait]
        impl Operator for BatchOperator {
            fn schema(&self) -> &[VarId] {
                &self.schema
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(self.batch.take())
            }
            fn close(&mut self) {}
        }

        let child: BoxedOperator = Box::new(BatchOperator {
            schema: schema.clone(),
            batch: Some(batch),
        });

        // GROUP BY ?venue, COUNT(?paper) as ?count
        let agg_specs = vec![StreamingAggSpec {
            function: AggregateFn::Count,
            input_col: Some(1), // ?paper
            output_var: VarId(2),
        }];

        let mut op = GroupAggregateOperator::new(child, vec![VarId(0)], agg_specs, None);
        op.open(&ctx).await.unwrap();

        // Collect results
        let mut results: Vec<(Binding, i64)> = Vec::new();
        while let Some(batch) = op.next_batch(&ctx).await.unwrap() {
            for row_idx in 0..batch.len() {
                let venue = batch.get_by_col(row_idx, 0).clone();
                let count = batch.get_by_col(row_idx, 1);
                if let Binding::Lit {
                    val: FlakeValue::Long(n),
                    ..
                } = count
                {
                    results.push((venue, *n));
                }
            }
        }

        // Verify results
        assert_eq!(results.len(), 2);

        // Find venueA and venueB counts
        let venue_a_count = results
            .iter()
            .find(|(v, _)| {
                if let Binding::Sid(sid) = v {
                    sid.name.as_ref() == "venueA"
                } else {
                    false
                }
            })
            .map(|(_, c)| *c);
        let venue_b_count = results
            .iter()
            .find(|(v, _)| {
                if let Binding::Sid(sid) = v {
                    sid.name.as_ref() == "venueB"
                } else {
                    false
                }
            })
            .map(|(_, c)| *c);

        assert_eq!(venue_a_count, Some(5));
        assert_eq!(venue_b_count, Some(3));

        op.close();
    }

    #[tokio::test]
    async fn test_streaming_sum_avg() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;

        let db = make_test_db();
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&db, &vars);

        // Create input: category A with values 10, 20, 30; category B with values 5, 15
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let columns = vec![
            // ?category
            vec![
                Binding::Sid(Sid::new(100, "catA")),
                Binding::Sid(Sid::new(100, "catA")),
                Binding::Sid(Sid::new(100, "catA")),
                Binding::Sid(Sid::new(100, "catB")),
                Binding::Sid(Sid::new(100, "catB")),
            ],
            // ?value
            vec![
                Binding::lit(FlakeValue::Long(10), xsd_long()),
                Binding::lit(FlakeValue::Long(20), xsd_long()),
                Binding::lit(FlakeValue::Long(30), xsd_long()),
                Binding::lit(FlakeValue::Long(5), xsd_long()),
                Binding::lit(FlakeValue::Long(15), xsd_long()),
            ],
        ];
        let batch = Batch::new(schema.clone(), columns).unwrap();

        struct BatchOperator {
            schema: Arc<[VarId]>,
            batch: Option<Batch>,
        }
        #[async_trait]
        impl Operator for BatchOperator {
            fn schema(&self) -> &[VarId] {
                &self.schema
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(self.batch.take())
            }
            fn close(&mut self) {}
        }

        let child: BoxedOperator = Box::new(BatchOperator {
            schema: schema.clone(),
            batch: Some(batch),
        });

        // GROUP BY ?category, SUM(?value), AVG(?value)
        let agg_specs = vec![
            StreamingAggSpec {
                function: AggregateFn::Sum,
                input_col: Some(1),
                output_var: VarId(2),
            },
            StreamingAggSpec {
                function: AggregateFn::Avg,
                input_col: Some(1),
                output_var: VarId(3),
            },
        ];

        let mut op = GroupAggregateOperator::new(child, vec![VarId(0)], agg_specs, None);
        op.open(&ctx).await.unwrap();

        let mut results: HashMap<String, (i64, f64)> = HashMap::new();
        while let Some(batch) = op.next_batch(&ctx).await.unwrap() {
            for row_idx in 0..batch.len() {
                let cat = batch.get_by_col(row_idx, 0);
                let sum_val = batch.get_by_col(row_idx, 1);
                let avg_val = batch.get_by_col(row_idx, 2);

                if let Binding::Sid(sid) = cat {
                    let sum = match sum_val {
                        Binding::Lit {
                            val: FlakeValue::Long(n),
                            ..
                        } => *n,
                        _ => panic!("Expected Long"),
                    };
                    let avg = match avg_val {
                        Binding::Lit {
                            val: FlakeValue::Double(d),
                            ..
                        } => *d,
                        _ => panic!("Expected Double"),
                    };
                    results.insert(sid.name.to_string(), (sum, avg));
                }
            }
        }

        assert_eq!(results.get("catA"), Some(&(60, 20.0)));
        assert_eq!(results.get("catB"), Some(&(20, 10.0)));

        op.close();
    }

    #[tokio::test]
    async fn test_streaming_min_materializes_encoded_sid_with_store() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;
        use fluree_db_core::subject_id::SubjectId;
        use fluree_db_core::value_id::{ObjKey, ObjKind};
        use fluree_db_core::DatatypeDictId;
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

        // Build a tiny on-disk BinaryIndexStore so we can materialize EncodedSid.
        // We intentionally insert subjects so that s_id order disagrees with lex order:
        // insert "zebra" first (smaller s_id), then "apple" (larger s_id).
        let base = std::env::temp_dir().join(format!(
            "fluree_test_group_agg_minmax_{}",
            uuid::Uuid::new_v4()
        ));
        let run_dir = base.join("tmp_import");
        let spot_dir = run_dir.join("spot");
        let index_dir = base.join("index");
        std::fs::create_dir_all(&spot_dir).unwrap();
        std::fs::create_dir_all(&index_dir).unwrap();

        // --- Dictionaries ---
        let p_iri = "http://example.com/p";
        let mut pred_dict = PredicateDict::new();
        let p_id = pred_dict.get_or_insert(p_iri);
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
        let ns: u16 = 100; // test namespace
        let zebra_iri = "http://example.com/zebra";
        let apple_iri = "http://example.com/apple";
        let s_id_zebra = subjects.get_or_insert(zebra_iri, ns).unwrap();
        let s_id_apple = subjects.get_or_insert(apple_iri, ns).unwrap();
        assert!(
            s_id_zebra < s_id_apple,
            "expected insertion order to yield zebra s_id < apple s_id"
        );

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

        // --- Run records (SPOT only) ---
        let g_id: u16 = 0;
        let t: u32 = 1;
        let records = vec![
            RunRecord::new(
                g_id,
                SubjectId::from_u64(s_id_zebra),
                p_id,
                ObjKind::NUM_INT,
                ObjKey::encode_i64(1),
                t,
                true,
                DatatypeDictId::INTEGER.as_u16(),
                0,
                None,
            ),
            RunRecord::new(
                g_id,
                SubjectId::from_u64(s_id_apple),
                p_id,
                ObjKind::NUM_INT,
                ObjKey::encode_i64(2),
                t,
                true,
                DatatypeDictId::INTEGER.as_u16(),
                0,
                None,
            ),
        ];
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

        // Build SPOT index + load store.
        build_all_indexes(&run_dir, &index_dir, &[RunSortOrder::Spot], 64, 2, 0, None).unwrap();
        let store = Arc::new(BinaryIndexStore::load(&run_dir, &index_dir).unwrap());

        // --- Build GroupAggregateOperator over encoded subject IDs ---
        let db = make_test_db();
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&db, &vars).with_binary_store(store.clone(), 0);

        // Input column: EncodedSid(zebra), EncodedSid(apple)
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice()); // ?x
        let batch = Batch::new(
            schema.clone(),
            vec![vec![
                Binding::EncodedSid { s_id: s_id_zebra },
                Binding::EncodedSid { s_id: s_id_apple },
            ]],
        )
        .unwrap();

        struct BatchOperator {
            schema: Arc<[VarId]>,
            batch: Option<Batch>,
        }
        #[async_trait]
        impl Operator for BatchOperator {
            fn schema(&self) -> &[VarId] {
                &self.schema
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(self.batch.take())
            }
            fn close(&mut self) {}
        }

        let child: BoxedOperator = Box::new(BatchOperator {
            schema: schema.clone(),
            batch: Some(batch),
        });

        // No GROUP BY keys (single global group), MIN(?x) as ?min
        let agg_specs = vec![StreamingAggSpec {
            function: AggregateFn::Min,
            input_col: Some(0),
            output_var: VarId(1),
        }];

        let mut op = GroupAggregateOperator::new(child, vec![], agg_specs, None);
        op.open(&ctx).await.unwrap();
        let out = op
            .next_batch(&ctx)
            .await
            .unwrap()
            .expect("expected output batch");
        assert_eq!(out.len(), 1);
        assert_eq!(out.schema().len(), 1);

        // Correct semantic MIN is "apple" (lex order), even though zebra has smaller s_id.
        match out.get_by_col(0, 0) {
            Binding::Sid(sid) => {
                let iri = store.sid_to_iri(sid);
                assert_eq!(iri, apple_iri);
            }
            other => panic!("expected materialized Sid for MIN output, got {:?}", other),
        }

        op.close();

        // Best-effort cleanup
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn test_all_streamable() {
        let streamable = vec![
            StreamingAggSpec {
                function: AggregateFn::Count,
                input_col: Some(0),
                output_var: VarId(1),
            },
            StreamingAggSpec {
                function: AggregateFn::Sum,
                input_col: Some(0),
                output_var: VarId(2),
            },
        ];
        assert!(GroupAggregateOperator::all_streamable(&streamable));

        let non_streamable = vec![
            StreamingAggSpec {
                function: AggregateFn::Count,
                input_col: Some(0),
                output_var: VarId(1),
            },
            StreamingAggSpec {
                function: AggregateFn::GroupConcat {
                    separator: ",".to_string(),
                },
                input_col: Some(0),
                output_var: VarId(2),
            },
        ];
        assert!(!GroupAggregateOperator::all_streamable(&non_streamable));
    }
}
