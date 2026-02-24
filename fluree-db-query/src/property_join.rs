//! Property-join operator for same-subject multi-predicate patterns
//!
//! The `PropertyJoinOperator` optimizes queries where multiple triple patterns
//! share the same subject variable and have bound predicates with variable objects.
//!
//! # Example Pattern
//!
//! ```text
//! ?s :name ?name
//! ?s :age ?age
//! ?s :email ?email
//! ```
//!
//! # Semantics
//!
//! PropertyJoinOperator produces a **cartesian product** across properties when
//! predicates are multi-cardinality. For example, if a subject has 2 names and
//! 3 emails, the operator produces 6 rows (not 1 row with nested values).
//! This matches SPARQL solution-set semantics.
//!
//! # Index Usage
//!
//! Uses PSOT index for each predicate scan, which is optimal for
//! "get all subjects with predicate P" queries.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::join::NestedLoopJoinOperator;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::seed::EmptyOperator;
use crate::triple::{Ref, Term, TriplePattern};
use crate::values::ValuesOperator;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::{ObjectBounds, Sid};
use rustc_hash::FxHashMap;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tracing::Instrument;

use crate::binary_scan::ScanOperator;

/// Internal temp var for object position in predicate scans.
///
/// We use VarId(u16::MAX - 1) as a sentinel value for temporary object variables
/// in internal scans. This is safe because:
/// 1. We only access scan results by column index, not by VarId
/// 2. VarRegistry panics if > 65534 vars are registered (u16::MAX - 1)
/// 3. This var never escapes to external schemas or user code
const TEMP_OBJECT_VAR: VarId = VarId(u16::MAX - 1);

fn make_property_join_scan(pattern: TriplePattern, bounds: Option<ObjectBounds>) -> BoxedOperator {
    Box::new(ScanOperator::new(pattern, bounds, Vec::new()))
}

/// Property-join operator for same-subject multi-predicate patterns
///
/// Optimizes queries of the form:
/// ```text
/// ?s :pred1 ?obj1
/// ?s :pred2 ?obj2
/// ...
/// ```
///
/// Where all patterns share the same subject variable.
///
/// # Multi-Ledger Support
///
/// In dataset mode, subjects are keyed by canonical IRI (`Arc<str>`) to ensure
/// correct cross-ledger joins. The operator accepts both `Binding::Sid` (single-ledger)
/// and `Binding::IriMatch` (multi-ledger) from scans and emits the appropriate
/// binding type in output rows.
pub struct PropertyJoinOperator {
    /// The shared subject variable
    subject_var: VarId,
    /// Predicates and their corresponding object variables
    /// Each entry is (predicate_ref, object_var, optional datatype constraint)
    /// predicate_ref can be Ref::Sid or Ref::Iri depending on how the query was lowered.
    predicates: Vec<(Ref, VarId, Option<Sid>)>,
    /// Output schema: [subject_var, obj_var_1, obj_var_2, ...]
    output_schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Collected values per subject, keyed by a join-safe subject key.
    ///
    /// - Single-ledger: prefer raw encoded subject IDs (no decoding)
    /// - Dataset/multi-ledger: use canonical IRI strings (cross-ledger safe)
    ///
    /// Value tuple: (subject_binding, vec of value-vectors per predicate).
    /// The subject_binding is preserved from the scan to emit the correct type.
    subject_values: FxHashMap<SubjectKey, (Binding, Vec<Vec<Binding>>)>,
    /// Subjects to process (collected after filtering)
    pending_subjects: Vec<SubjectKey>,
    /// Current index into pending_subjects
    subject_idx: usize,
    /// Optional object bounds for range filter pushdown (VarId -> ObjectBounds)
    object_bounds: HashMap<VarId, ObjectBounds>,
}

/// Join-safe subject key for PropertyJoinOperator.
///
/// This avoids eagerly decoding subjects to canonical IRI strings in single-ledger mode,
/// preserving the late-materialization benefits of `Binding::EncodedSid`.
#[derive(Clone, Debug, Eq)]
enum SubjectKey {
    /// Single-ledger: raw subject/ref ID from the binary index (`Binding::EncodedSid`)
    Id(u64),
    /// Single-ledger (range/overlay paths): already-materialized SID
    Sid(Sid),
    /// Multi-ledger (dataset): canonical IRI string
    Iri(Arc<str>),
}

impl PartialEq for SubjectKey {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SubjectKey::Id(a), SubjectKey::Id(b)) => a == b,
            (SubjectKey::Sid(a), SubjectKey::Sid(b)) => {
                a.namespace_code == b.namespace_code && a.name == b.name
            }
            (SubjectKey::Iri(a), SubjectKey::Iri(b)) => a == b,
            _ => false,
        }
    }
}

impl Hash for SubjectKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // discriminant
        match self {
            SubjectKey::Id(v) => {
                0u8.hash(state);
                v.hash(state);
            }
            SubjectKey::Sid(s) => {
                1u8.hash(state);
                s.namespace_code.hash(state);
                s.name.hash(state);
            }
            SubjectKey::Iri(i) => {
                2u8.hash(state);
                i.hash(state);
            }
        }
    }
}

impl PropertyJoinOperator {
    /// Create a new property-join operator from patterns
    ///
    /// # Arguments
    ///
    /// * `patterns` - Triple patterns forming a property-join shape
    /// * `object_bounds` - Optional range bounds for object variables (filter pushdown)
    ///
    /// # Panics
    ///
    /// Panics if patterns don't form a valid property-join shape
    /// (use `is_property_join_shape` to check first).
    pub fn new(patterns: &[TriplePattern], object_bounds: HashMap<VarId, ObjectBounds>) -> Self {
        assert!(
            crate::planner::is_property_join(patterns),
            "Patterns must form a property-join shape"
        );

        // Extract subject var (guaranteed same for all by is_property_join)
        let subject_var = match &patterns[0].s {
            Ref::Var(v) => *v,
            _ => panic!("Property-join requires variable subject"),
        };

        // Extract (predicate_ref, object_var, dt) triples
        // Predicate can be Ref::Sid or Ref::Iri depending on lowering
        let predicates: Vec<(Ref, VarId, Option<Sid>)> = patterns
            .iter()
            .map(|p| {
                let pred_ref = match &p.p {
                    Ref::Sid(_) | Ref::Iri(_) => p.p.clone(),
                    _ => panic!("Property-join requires bound predicates (Sid or Iri)"),
                };
                let obj_var = match &p.o {
                    Term::Var(v) => *v,
                    _ => panic!("Property-join requires variable objects"),
                };
                (pred_ref, obj_var, p.dt.clone())
            })
            .collect();

        // Build output schema: [subject_var, obj_var_1, obj_var_2, ...]
        let mut schema_vec = vec![subject_var];
        for (_, obj_var, _) in &predicates {
            schema_vec.push(*obj_var);
        }
        let output_schema: Arc<[VarId]> = Arc::from(schema_vec.into_boxed_slice());

        Self {
            subject_var,
            predicates,
            output_schema,
            state: OperatorState::Created,
            subject_values: FxHashMap::default(),
            pending_subjects: Vec::new(),
            subject_idx: 0,
            object_bounds,
        }
    }

    /// Get the subject variable
    pub fn subject_var(&self) -> VarId {
        self.subject_var
    }

    /// Get the predicates with their object variables
    pub fn predicates(&self) -> &[(Ref, VarId, Option<Sid>)] {
        &self.predicates
    }

    /// Get the output schema (non-trait method for tests)
    pub fn output_schema(&self) -> &Arc<[VarId]> {
        &self.output_schema
    }

    fn subject_key_single(subject: &Binding) -> Option<SubjectKey> {
        match subject {
            Binding::EncodedSid { s_id } => Some(SubjectKey::Id(*s_id)),
            Binding::Sid(sid) => Some(SubjectKey::Sid(sid.clone())),
            Binding::IriMatch { primary_sid, .. } => Some(SubjectKey::Sid(primary_sid.clone())),
            Binding::Iri(iri) => Some(SubjectKey::Iri(iri.clone())),
            _ => None,
        }
    }

    fn subject_key_multi(ctx: &ExecutionContext<'_>, subject: &Binding) -> Option<SubjectKey> {
        match subject {
            Binding::IriMatch { iri, .. } => Some(SubjectKey::Iri(iri.clone())),
            Binding::Iri(iri) => Some(SubjectKey::Iri(iri.clone())),
            Binding::Sid(sid) => {
                // In dataset mode, use canonical IRI strings as join keys.
                // Prefer decoding within the active ledger when available.
                let iri = ctx
                    .active_ledger_id()
                    .and_then(|addr| ctx.decode_sid_in_ledger(sid, addr))
                    .or_else(|| ctx.decode_sid(sid))?;
                Some(SubjectKey::Iri(Arc::from(iri)))
            }
            Binding::EncodedSid { s_id } => {
                // Resolve to canonical IRI for cross-ledger comparison.
                ctx.binary_store
                    .as_ref()
                    .and_then(|store| store.resolve_subject_iri(*s_id).ok())
                    .map(|iri| SubjectKey::Iri(Arc::from(iri)))
            }
            _ => None,
        }
    }

    fn subject_key(ctx: &ExecutionContext<'_>, subject: &Binding) -> Option<SubjectKey> {
        if ctx.is_multi_ledger() {
            Self::subject_key_multi(ctx, subject)
        } else {
            Self::subject_key_single(subject)
        }
    }

    /// Generate cartesian product rows for a given subject
    ///
    /// Takes the collected values for each predicate and produces
    /// all combinations. The subject_binding is cloned into each row.
    fn generate_rows(
        output_schema_len: usize,
        subject_binding: &Binding,
        values_per_pred: &[Vec<Binding>],
    ) -> Vec<Vec<Binding>> {
        if values_per_pred.is_empty() || values_per_pred.iter().any(|v| v.is_empty()) {
            return Vec::new();
        }

        // Calculate total combinations
        let total: usize = values_per_pred.iter().map(|v| v.len()).product();
        if total == 0 {
            return Vec::new();
        }

        let mut rows = Vec::with_capacity(total);

        // Generate cartesian product using indices
        let mut indices: Vec<usize> = vec![0; values_per_pred.len()];

        loop {
            // Build current row
            let mut row = Vec::with_capacity(output_schema_len);
            row.push(subject_binding.clone());
            for (pred_idx, val_idx) in indices.iter().enumerate() {
                row.push(values_per_pred[pred_idx][*val_idx].clone());
            }
            rows.push(row);

            // Increment indices (like odometer)
            let mut carry = true;
            for i in (0..indices.len()).rev() {
                if carry {
                    indices[i] += 1;
                    if indices[i] >= values_per_pred[i].len() {
                        indices[i] = 0;
                    } else {
                        carry = false;
                    }
                }
            }

            if carry {
                // Wrapped all the way around
                break;
            }
        }

        rows
    }
}

#[async_trait]
impl Operator for PropertyJoinOperator {
    fn schema(&self) -> &[VarId] {
        &self.output_schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        let span = tracing::debug_span!(
            "property_join_open",
            predicates = self.predicates.len(),
            multi_ledger = ctx.is_multi_ledger(),
            has_binary_store = ctx.binary_store.is_some(),
            has_bounds = !self.object_bounds.is_empty(),
        );
        async {
            self.state = OperatorState::Open;
            self.subject_values.clear();
            self.pending_subjects.clear();
            self.subject_idx = 0;

            // For each predicate, scan and collect (subject -> values) mappings.
            // Key by a join-safe subject key (encoded IDs in single-ledger mode).
            //
            // Optimization: if a predicate has object bounds (range filter pushdown),
            // scan it first to get a selective subject set, then use that as a semi-join
            // driver for subsequent predicates in single-ledger binary mode.
            //
            // This turns a common "date filter + vector predicate" workload from:
            //   scan(date with bounds) + scan(vec full) + intersect
            // into:
            //   scan(date with bounds) + batched probe(vec for matching subjects)
            //
            // NOTE: The batched probe path currently requires:
            // - single-ledger (no dataset)
            // - binary_store present
            // - no datatype constraint on the probed predicate (dt=None)
            let mut all_subject_values: FxHashMap<SubjectKey, (Binding, Vec<Vec<Binding>>)> =
                FxHashMap::default();

            let driver_pred_idx = self
                .predicates
                .iter()
                .enumerate()
                .find(|(_idx, (_p, obj_var, _dt))| self.object_bounds.contains_key(obj_var))
                .map(|(idx, _)| idx);
            tracing::debug!(?driver_pred_idx, "property_join: selected driver predicate");

            let mut scan_order: Vec<usize> = (0..self.predicates.len()).collect();
            if let Some(d) = driver_pred_idx {
                scan_order.swap(0, d);
            }

            let mut driver_subject_ids: Option<Vec<u64>> = None;
            let mut used_batched_probe = false;
            let mut probe_chunks: u64 = 0;
            let mut probe_subjects_total: u64 = 0;
            let mut scan_rows_total: u64 = 0;

            for (order_pos, pred_idx) in scan_order.iter().copied().enumerate() {
                let (pred_term, obj_var, dt) = &self.predicates[pred_idx];

                // If we have a driver subject set and we're in the right execution mode,
                // try a batched subject probe for this predicate.
                let can_batched_probe = order_pos > 0
                    && driver_subject_ids.is_some()
                    && ctx.has_binary_store()
                    && dt.is_none();

                if can_batched_probe {
                    let store = ctx.binary_store.as_ref().unwrap();
                    let pred_sid = match pred_term {
                        Ref::Sid(s) => Some(s.clone()),
                        Ref::Iri(iri) => Some(store.encode_iri(iri)),
                        Ref::Var(_) => None,
                    };

                    if let Some(pred_sid) = pred_sid {
                        let subject_ids = driver_subject_ids.as_ref().unwrap();
                        if !subject_ids.is_empty() {
                            // IMPORTANT: Batched join uses the min/max s_id range of the left batch
                            // to decide which leaf files/leaflets to scan. If the subject IDs are
                            // sparse across the full id space, a single huge batch can still scan
                            // nearly the entire predicate partition.
                            //
                            // To improve locality, chunk the subject IDs into smaller sorted ranges
                            // and probe each chunk independently.
                            const PROBE_CHUNK_SIZE: usize = 256;

                            let mut ids = subject_ids.clone();
                            ids.sort_unstable();

                            for chunk in ids.chunks(PROBE_CHUNK_SIZE) {
                                used_batched_probe = true;
                                probe_chunks += 1;
                                probe_subjects_total += chunk.len() as u64;

                                let rows: Vec<Vec<Binding>> = chunk
                                    .iter()
                                    .map(|&s_id| vec![Binding::EncodedSid { s_id }])
                                    .collect();

                                // Seed: VALUES ?s { <subjectIds...> }
                                let left = Box::new(ValuesOperator::new(
                                    Box::new(EmptyOperator::new()),
                                    vec![self.subject_var],
                                    rows,
                                ));
                                let left_schema: Arc<[VarId]> =
                                    Arc::from(vec![self.subject_var].into_boxed_slice());

                                // Probe: ?s <pred> ?o
                                let right_pattern = TriplePattern::new(
                                    Ref::Var(self.subject_var),
                                    Ref::Sid(pred_sid.clone()),
                                    Term::Var(TEMP_OBJECT_VAR),
                                );

                                let mut join = NestedLoopJoinOperator::new(
                                    left,
                                    left_schema,
                                    right_pattern,
                                    None, // bounds already applied in driver; keep probe unconstrained
                                    Vec::new(),
                                );
                                join.open(ctx).await?;
                                while let Some(batch) = join.next_batch(ctx).await? {
                                    let subject_col = batch.column_by_idx(0);
                                    let object_col = batch.column_by_idx(1);
                                    if let (Some(subjects), Some(objects)) =
                                        (subject_col, object_col)
                                    {
                                        scan_rows_total += batch.len() as u64;
                                        for (subject, object) in subjects.iter().zip(objects.iter())
                                        {
                                            if let Some(key) = Self::subject_key(ctx, subject) {
                                                if let Some(entry) =
                                                    all_subject_values.get_mut(&key)
                                                {
                                                    entry.1[pred_idx].push(object.clone());
                                                }
                                            }
                                        }
                                    }
                                }
                                join.close();
                            }

                            continue;
                        }
                    }
                }

                // Create pattern: ?s :pred ?o (temp var for object, accessed by index)
                // pred_term is already a Ref (Sid or Iri) so use it directly
                let pattern = if let Some(dt) = dt {
                    TriplePattern::with_dt(
                        Ref::Var(self.subject_var),
                        pred_term.clone(),
                        Term::Var(TEMP_OBJECT_VAR),
                        dt.clone(),
                    )
                } else {
                    TriplePattern::new(
                        Ref::Var(self.subject_var),
                        pred_term.clone(),
                        Term::Var(TEMP_OBJECT_VAR),
                    )
                };

                // Create scan with optional bounds pushdown for this object variable.
                //
                // `ScanOperator` selects between binary cursor and range fallback
                // at open() time based on the execution context.
                let bounds = self.object_bounds.get(obj_var).cloned();
                let mut scan: BoxedOperator = make_property_join_scan(pattern, bounds);
                scan.open(ctx).await?;

                while let Some(batch) = scan.next_batch(ctx).await? {
                    // Schema for this scan is [subject_var, temp_obj_var]
                    let subject_col = batch.column_by_idx(0);
                    let object_col = batch.column_by_idx(1);

                    if let (Some(subjects), Some(objects)) = (subject_col, object_col) {
                        scan_rows_total += batch.len() as u64;
                        for (subject, object) in subjects.iter().zip(objects.iter()) {
                            // Extract join-safe subject key for keying, preserving original binding for output.
                            if let Some(key) = Self::subject_key(ctx, subject) {
                                // If we've already built a driver subject set, don't insert new subjects.
                                // This prevents the unbounded predicate scan from ballooning the map.
                                if order_pos > 0 && !all_subject_values.is_empty() {
                                    if let Some(entry) = all_subject_values.get_mut(&key) {
                                        entry.1[pred_idx].push(object.clone());
                                    }
                                    continue;
                                }

                                let entry = all_subject_values.entry(key).or_insert_with(|| {
                                    // Initialize with the subject binding and empty vecs for each predicate
                                    (subject.clone(), vec![Vec::new(); self.predicates.len()])
                                });
                                entry.1[pred_idx].push(object.clone());
                            }
                        }
                    }
                }

                scan.close();

                // After the first scan (driver), capture the subject IDs for batched probing.
                if order_pos == 0 && driver_pred_idx.is_some() && ctx.has_binary_store() {
                    let mut ids: Vec<u64> = Vec::with_capacity(all_subject_values.len());
                    for k in all_subject_values.keys() {
                        if let SubjectKey::Id(s_id) = k {
                            ids.push(*s_id);
                        } else {
                            ids.clear();
                            break;
                        }
                    }
                    if !ids.is_empty() {
                        driver_subject_ids = Some(ids);
                    }
                }
            }

            // Filter to only subjects that have values for ALL predicates
            self.subject_values = all_subject_values
                .into_iter()
                .filter(|(_, (_, values))| values.iter().all(|v| !v.is_empty()))
                .collect();

            // Collect subjects for iteration
            self.pending_subjects = self.subject_values.keys().cloned().collect();

            tracing::debug!(
                subjects = self.pending_subjects.len(),
                used_batched_probe,
                probe_chunks,
                probe_subjects_total,
                scan_rows_total,
                "property_join: open complete"
            );

            Ok(())
        }
        .instrument(span)
        .await
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        // Collect rows for multiple subjects up to batch size
        let batch_size = ctx.batch_size;
        let mut all_rows: Vec<Vec<Binding>> = Vec::new();

        let schema_len = self.output_schema.len();

        while all_rows.len() < batch_size && self.subject_idx < self.pending_subjects.len() {
            let subject_key = &self.pending_subjects[self.subject_idx];
            self.subject_idx += 1;

            if let Some((subject_binding, values_per_pred)) = self.subject_values.get(subject_key) {
                let rows = Self::generate_rows(schema_len, subject_binding, values_per_pred);
                all_rows.extend(rows);
            }
        }

        if all_rows.is_empty() {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }

        // Convert rows to columnar batch
        let num_cols = self.output_schema.len();
        let mut columns: Vec<Vec<Binding>> = (0..num_cols).map(|_| Vec::new()).collect();

        for row in all_rows {
            for (col_idx, val) in row.into_iter().enumerate() {
                columns[col_idx].push(val);
            }
        }

        Ok(Some(Batch::new(self.output_schema.clone(), columns)?))
    }

    fn close(&mut self) {
        self.state = OperatorState::Closed;
        self.subject_values.clear();
        self.pending_subjects.clear();
        self.subject_idx = 0;
    }

    fn estimated_rows(&self) -> Option<usize> {
        None // Could potentially estimate based on predicate cardinality
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::Sid;

    fn make_property_join_patterns() -> Vec<TriplePattern> {
        vec![
            TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Sid(Sid::new(100, "name")),
                Term::Var(VarId(1)),
            ),
            TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Sid(Sid::new(101, "age")),
                Term::Var(VarId(2)),
            ),
        ]
    }

    #[test]
    fn test_property_join_creation() {
        let patterns = make_property_join_patterns();
        let op = PropertyJoinOperator::new(&patterns, HashMap::new());

        assert_eq!(op.subject_var(), VarId(0));
        assert_eq!(op.predicates().len(), 2);
        assert_eq!(op.output_schema().len(), 3); // subject + 2 object vars
    }

    #[test]
    fn test_property_join_schema() {
        let patterns = make_property_join_patterns();
        let op = PropertyJoinOperator::new(&patterns, HashMap::new());

        let schema = op.output_schema();
        assert_eq!(schema[0], VarId(0)); // subject
        assert_eq!(schema[1], VarId(1)); // name object
        assert_eq!(schema[2], VarId(2)); // age object
    }

    #[test]
    fn test_subject_key_single_prefers_encoded_ids() {
        // Single-ledger mode should not require IRI decoding for EncodedSid.
        let key = PropertyJoinOperator::subject_key_single(&Binding::EncodedSid { s_id: 42 });
        assert!(matches!(key, Some(SubjectKey::Id(42))));
    }

    #[test]
    fn test_generate_rows_single_values() {
        let patterns = make_property_join_patterns();
        let op = PropertyJoinOperator::new(&patterns, HashMap::new());

        let subject_sid = Sid::new(1, "alice");
        let subject_binding = Binding::Sid(subject_sid.clone());
        let values = vec![
            vec![Binding::Sid(Sid::new(200, "Alice"))], // name
            vec![Binding::Sid(Sid::new(201, "30"))],    // age
        ];

        let rows = PropertyJoinOperator::generate_rows(
            op.output_schema().len(),
            &subject_binding,
            &values,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 3);
        assert!(matches!(&rows[0][0], Binding::Sid(s) if *s == subject_sid));
    }

    #[test]
    fn test_generate_rows_cartesian_product() {
        let patterns = make_property_join_patterns();
        let op = PropertyJoinOperator::new(&patterns, HashMap::new());

        let subject_binding = Binding::Sid(Sid::new(1, "alice"));
        let values = vec![
            vec![
                Binding::Sid(Sid::new(200, "Alice")),
                Binding::Sid(Sid::new(201, "Alicia")),
            ], // 2 names
            vec![
                Binding::Sid(Sid::new(300, "30")),
                Binding::Sid(Sid::new(301, "31")),
                Binding::Sid(Sid::new(302, "32")),
            ], // 3 ages
        ];

        let rows = PropertyJoinOperator::generate_rows(
            op.output_schema().len(),
            &subject_binding,
            &values,
        );
        // Cartesian product: 2 * 3 = 6 rows
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn test_generate_rows_empty_pred() {
        let patterns = make_property_join_patterns();
        let op = PropertyJoinOperator::new(&patterns, HashMap::new());

        let subject_binding = Binding::Sid(Sid::new(1, "alice"));
        let values = vec![
            vec![Binding::Sid(Sid::new(200, "Alice"))], // has name
            vec![],                                     // no age
        ];

        let rows = PropertyJoinOperator::generate_rows(
            op.output_schema().len(),
            &subject_binding,
            &values,
        );
        // No rows if any predicate is missing
        assert_eq!(rows.len(), 0);
    }

    #[test]
    #[should_panic(expected = "property-join shape")]
    fn test_property_join_rejects_invalid_patterns() {
        // Different subjects - not a valid property-join
        let patterns = vec![
            TriplePattern::new(
                Ref::Var(VarId(0)),
                Ref::Sid(Sid::new(100, "name")),
                Term::Var(VarId(1)),
            ),
            TriplePattern::new(
                Ref::Var(VarId(2)), // Different subject!
                Ref::Sid(Sid::new(101, "age")),
                Term::Var(VarId(3)),
            ),
        ];
        let _op = PropertyJoinOperator::new(&patterns, HashMap::new());
    }
}
