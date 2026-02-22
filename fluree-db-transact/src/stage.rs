//! Transaction staging
//!
//! This module provides the `stage` function that executes a parsed transaction
//! against a ledger and produces a staged view with the resulting flakes.
//!
//! ## SHACL Validation
//!
//! When the `shacl` feature is enabled, you can use [`stage_with_shacl`] to validate
//! staged flakes against SHACL shapes before returning the view. This ensures that
//! data conforms to the defined shape constraints.

use crate::error::{Result, TransactError};
use crate::generate::{apply_cancellation, infer_datatype, FlakeGenerator};
use crate::ir::InlineValues;
use crate::ir::{TemplateTerm, Txn, TxnType};
use crate::namespace::NamespaceRegistry;
use fluree_db_core::OverlayProvider;
use fluree_db_core::Tracker;
use fluree_db_core::{Flake, FlakeValue, GraphId, Sid};
use fluree_db_ledger::{IndexConfig, LedgerState, LedgerView};
use fluree_db_policy::{
    is_schema_flake, populate_class_cache, PolicyContext, PolicyDecision, PolicyError,
};
use fluree_db_query::parse::{lower_unresolved_patterns, UnresolvedPattern};
use fluree_db_query::{
    Batch, Binding, Pattern, QueryPolicyExecutor, Term, TriplePattern, VarId, VarRegistry,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::Instrument;

#[cfg(feature = "shacl")]
use fluree_db_shacl::{ShaclCache, ShaclEngine, ValidationReport};

/// Build a reverse lookup from graph Sid → GraphId.
///
/// Given `graph_sids` (GraphId → Sid from `txn.graph_delta`), returns the
/// inverse mapping. Used by SHACL/policy to determine which graph a flake
/// belongs to based on its `Flake.g` field.
fn build_reverse_graph_lookup(graph_sids: &HashMap<GraphId, Sid>) -> HashMap<Sid, GraphId> {
    graph_sids
        .iter()
        .map(|(&g_id, sid)| (sid.clone(), g_id))
        .collect()
}

/// Resolve a flake's graph ID from its `Flake.g` field.
///
/// - `None` → default graph (g_id = 0)
/// - `Some(sid)` → looked up in `reverse_graph`; returns error if unknown
fn resolve_flake_graph_id(flake: &Flake, reverse_graph: &HashMap<Sid, GraphId>) -> Result<GraphId> {
    match &flake.g {
        None => Ok(0),
        Some(g_sid) => reverse_graph.get(g_sid).copied().ok_or_else(|| {
            TransactError::FlakeGeneration(format!(
                "staged flake references unknown graph Sid: {}",
                g_sid
            ))
        }),
    }
}

/// Options for transaction staging
///
/// This struct groups optional configuration parameters for the [`stage`] function,
/// reducing the number of function parameters and making call sites cleaner.
#[derive(Default, Clone)]
pub struct StageOptions<'a> {
    /// Index configuration for backpressure checks.
    /// If provided, staging will fail with `NoveltyAtMax` when novelty is at capacity.
    pub index_config: Option<&'a IndexConfig>,

    /// Policy context for authorization.
    /// If provided (and not root), modify policies will be enforced on staged flakes.
    pub policy_ctx: Option<&'a PolicyContext>,

    /// Tracker for fuel accounting.
    /// If provided, fuel will be consumed for each staged flake.
    pub tracker: Option<&'a Tracker>,

    /// Graph routing map for named-graph flakes.
    ///
    /// Maps `GraphId → Sid` so that `stage_flakes` can resolve each flake's
    /// `Flake.g` to a `GraphId` for per-graph policy enforcement and SHACL validation.
    ///
    /// **Required** when any flake has `g != None`. If `None` is provided and
    /// named-graph flakes are present, `stage_flakes` will return an error.
    ///
    /// The normal `stage()` path builds this internally from `txn.graph_delta`.
    pub graph_sids: Option<&'a HashMap<GraphId, Sid>>,
}

impl<'a> StageOptions<'a> {
    /// Create new stage options with all fields set to None
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the index configuration for backpressure checks
    pub fn with_index_config(mut self, config: &'a IndexConfig) -> Self {
        self.index_config = Some(config);
        self
    }

    /// Set the policy context for authorization
    pub fn with_policy(mut self, policy: &'a PolicyContext) -> Self {
        self.policy_ctx = Some(policy);
        self
    }

    /// Set the tracker for fuel accounting
    pub fn with_tracker(mut self, tracker: &'a Tracker) -> Self {
        self.tracker = Some(tracker);
        self
    }

    /// Set the graph routing map for named-graph flakes
    pub fn with_graph_sids(mut self, graph_sids: &'a HashMap<GraphId, Sid>) -> Self {
        self.graph_sids = Some(graph_sids);
        self
    }
}

/// Stage a transaction against a ledger
///
/// This function:
/// 1. Checks backpressure (rejects if novelty at max)
/// 2. Executes WHERE patterns against the ledger to get bindings
/// 3. Generates retractions from DELETE templates with those bindings
/// 4. Generates assertions from INSERT templates with those bindings
/// 5. Applies cancellation (matching assertion/retraction pairs cancel out)
/// 6. Returns a LedgerView with the staged flakes
///
/// # Arguments
///
/// * `ledger` - The ledger state (consumed by value)
/// * `txn` - The parsed transaction IR
/// * `ns_registry` - Namespace registry for IRI resolution
/// * `options` - Optional configuration for backpressure, policy, and tracking
///
/// # Unbound Variable Behavior
///
/// When a variable in a template is unbound (no matching WHERE result) or poisoned
/// (from an OPTIONAL that didn't match), the flake is **silently skipped**. This
/// follows SPARQL UPDATE semantics where:
///
/// - `DELETE { ?s :name ?name }` with unbound `?name` produces no retractions
/// - `INSERT { ?s :name ?name }` with unbound `?name` produces no assertions
///
/// This is intentional: it allows patterns like "delete all existing values before
/// inserting new ones" to work correctly when there are no existing values.
///
/// If you need to require that all variables are bound, validate the WHERE results
/// before calling stage.
///
/// # Errors
///
/// Returns `TransactError::NoveltyAtMax` if novelty is at the maximum size and
/// reindexing is required before new transactions can be processed.
///
///
/// # Example
///
/// ```ignore
/// let options = StageOptions::new().with_index_config(&config);
/// let view = stage(ledger, txn, ns_registry, options).await?;
/// // Query the view to see staged changes
/// // Or commit the view to persist changes
/// ```
pub async fn stage(
    ledger: LedgerState,
    mut txn: Txn,
    mut ns_registry: NamespaceRegistry,
    options: StageOptions<'_>,
) -> Result<(LedgerView, NamespaceRegistry)> {
    let span = tracing::debug_span!("txn_stage",
        current_t = ledger.t(),
        txn_type = ?txn.txn_type,
        insert_count = txn.insert_templates.len(),
        delete_count = txn.delete_templates.len()
    );
    async move {
        tracing::info!("starting transaction staging");

        // 1. Check backpressure - reject early if novelty is at max
        if let Some(config) = options.index_config {
            if ledger.at_max_novelty(config) {
                tracing::warn!("novelty at max, rejecting transaction");
                return Err(TransactError::NoveltyAtMax);
            }
        }

        let new_t = ledger.t() + 1;
        tracing::debug!(new_t = new_t, "computed new transaction t");

        // Execute WHERE patterns to get bindings
        // This lowers UnresolvedPattern to Pattern, assigning VarIds to variables
        let where_span = tracing::debug_span!(
            "where_exec",
            pattern_count = txn.where_patterns.len(),
            binding_rows = tracing::field::Empty,
        );
        let bindings = async {
            let bindings = execute_where(&ledger, &mut txn).await?;
            tracing::Span::current().record("binding_rows", bindings.len() as u64);
            Ok::<_, TransactError>(bindings)
        }
        .instrument(where_span)
        .await?;

        // Generate transaction ID for blank node skolemization
        let txn_id = generate_txn_id();

        // Convert graph_delta (g_id -> IRI) to graph_sids (g_id -> Sid) for named graph support
        let graph_sids: HashMap<GraphId, Sid> = txn
            .graph_delta
            .iter()
            .map(|(&g_id, iri)| (g_id, ns_registry.sid_for_iri(iri)))
            .collect();
        let reverse_graph = build_reverse_graph_lookup(&graph_sids);

        let mut generator = FlakeGenerator::new(new_t, &mut ns_registry, txn_id)
            .with_graph_sids(graph_sids.clone());

        // Generate retractions from DELETE templates
        let delete_span = tracing::debug_span!(
            "delete_gen",
            template_count = txn.delete_templates.len(),
            retraction_count = tracing::field::Empty,
        );
        let retractions = async {
            let mut retractions =
                generator.generate_retractions(&txn.delete_templates, &bindings)?;

            // Clojure parity: DELETE templates often omit list indices even when
            // retracting `@list` values. Hydrate retractions by copying the stored
            // list-index meta from the currently asserted flake (if present).
            hydrate_list_index_meta_for_retractions(&ledger, &mut retractions, &reverse_graph)
                .await?;

            // For Upsert: also generate deletions for existing values
            if txn.txn_type == TxnType::Upsert {
                tracing::debug!("generating upsert deletions");
                let upsert_retractions =
                    generate_upsert_deletions(&ledger, &txn, new_t, &graph_sids).await?;
                tracing::debug!(
                    upsert_retraction_count = upsert_retractions.len(),
                    "upsert deletions generated"
                );
                retractions.extend(upsert_retractions);
            }

            tracing::Span::current().record("retraction_count", retractions.len() as u64);
            Ok::<_, TransactError>(retractions)
        }
        .instrument(delete_span)
        .await?;

        // Generate assertions from INSERT templates
        let insert_span = tracing::debug_span!(
            "insert_gen",
            template_count = txn.insert_templates.len(),
            assertion_count = tracing::field::Empty,
        );
        let assertions = {
            let _guard = insert_span.enter();
            // Clojure parity: For UPDATE transactions, it's common to write:
            //   WHERE { ... maybe matches ... }
            //   DELETE { ... bound vars ... }
            //   INSERT { ... constant assertions ... }
            //
            // When WHERE has **no solutions**, DELETE should be a no-op
            // (handled by bindings.is_empty()), but constant INSERT templates
            // should still be applied once.
            let assertions = if bindings.is_empty() && txn.txn_type == TxnType::Update {
                let empty_solution = Batch::single_empty();
                generator.generate_assertions(&txn.insert_templates, &empty_solution)?
            } else {
                generator.generate_assertions(&txn.insert_templates, &bindings)?
            };
            insert_span.record("assertion_count", assertions.len() as u64);
            assertions
        };

        // Apply cancellation (retraction cancels assertion and vice versa)
        let flakes = {
            let _cancel_span = tracing::debug_span!("cancellation").entered();
            let mut all_flakes = retractions;
            all_flakes.extend(assertions);
            let total_before_cancel = all_flakes.len();
            let flakes = apply_cancellation(all_flakes);
            if flakes.len() != total_before_cancel {
                tracing::debug!(
                    before = total_before_cancel,
                    after = flakes.len(),
                    cancelled = total_before_cancel - flakes.len(),
                    "cancellation applied"
                );
            }
            flakes
        };

        // Enforce modify policies (if policy context provided and not root)
        if let Some(policy) = options.policy_ctx {
            if !policy.wrapper().is_root() {
                let policy_span = tracing::debug_span!("policy_enforce");
                async {
                    enforce_modify_policies(
                        &flakes,
                        policy,
                        &ledger,
                        options.tracker,
                        &reverse_graph,
                    )
                    .await
                }
                .instrument(policy_span)
                .await?;
            }
        }

        let total_flakes = flakes.len();
        let assertions = flakes.iter().filter(|f| f.op).count();
        let retractions = total_flakes - assertions;

        tracing::info!(
            flake_count = total_flakes,
            assertions = assertions,
            retractions = retractions,
            "transaction staging completed"
        );

        Ok((
            LedgerView::stage(ledger, flakes, &reverse_graph)?,
            ns_registry,
        ))
    }
    .instrument(span)
    .await
}

/// Stage pre-built flakes against a ledger (bypass WHERE/template pipeline).
///
/// This is the fast path for bulk INSERT from Turtle where flakes are already
/// constructed by [`FlakeSink`](crate::flake_sink::FlakeSink). No WHERE
/// execution, template materialization, or cancellation is performed.
///
/// # Named Graph Support
///
/// When flakes include named-graph data (`Flake.g = Some(_)`), the caller
/// **must** provide `StageOptions.graph_sids` so that policy enforcement
/// and SHACL validation can resolve each flake's graph. If named-graph flakes
/// are present without a routing map, this function returns an error.
///
/// # Arguments
/// * `ledger` - The ledger state (consumed)
/// * `flakes` - Pre-built assertion flakes
/// * `options` - Optional backpressure / policy / tracking / graph routing configuration
pub async fn stage_flakes(
    ledger: LedgerState,
    flakes: Vec<Flake>,
    options: StageOptions<'_>,
) -> Result<LedgerView> {
    let span = tracing::debug_span!("stage_flakes", flake_count = flakes.len());
    async move {
        // 1. Backpressure check
        if let Some(config) = options.index_config {
            if ledger.at_max_novelty(config) {
                tracing::warn!("novelty at max, rejecting transaction");
                return Err(TransactError::NoveltyAtMax);
            }
        }

        // 2. Build graph routing map.
        //
        // If the caller provided graph_sids (push/import path), use it.
        // Otherwise, verify no named-graph flakes are present — stage_flakes
        // cannot correctly enforce policy/SHACL without a routing map.
        let reverse_graph: HashMap<Sid, GraphId> = match options.graph_sids {
            Some(gs) => build_reverse_graph_lookup(gs),
            None => {
                if flakes.iter().any(|f| f.g.is_some()) {
                    return Err(TransactError::FlakeGeneration(
                        "stage_flakes received named-graph flakes but no graph_sids \
                         routing map was provided in StageOptions"
                            .to_string(),
                    ));
                }
                HashMap::new()
            }
        };

        // 3. Policy enforcement
        if let Some(policy) = options.policy_ctx {
            if !policy.wrapper().is_root() {
                tracing::debug!("enforcing modify policies on pre-built flakes");
                enforce_modify_policies(&flakes, policy, &ledger, options.tracker, &reverse_graph)
                    .await?;
            }
        }

        tracing::info!(flake_count = flakes.len(), "stage_flakes completed");
        Ok(LedgerView::stage(ledger, flakes, &reverse_graph)?)
    }
    .instrument(span)
    .await
}

async fn hydrate_list_index_meta_for_retractions(
    ledger: &LedgerState,
    retractions: &mut [Flake],
    reverse_graph: &HashMap<Sid, GraphId>,
) -> Result<()> {
    for flake in retractions.iter_mut() {
        // Only retractions with no metadata are candidates.
        if flake.op {
            continue;
        }
        if flake.m.is_some() {
            continue;
        }

        // Resolve the correct graph for this retraction flake.
        let g_id = resolve_flake_graph_id(flake, reverse_graph)?;

        // Find currently asserted matching flakes (db + novelty overlay) and copy list index meta if present.
        let rm = fluree_db_core::RangeMatch::new()
            .with_subject(flake.s.clone())
            .with_predicate(flake.p.clone())
            .with_object(flake.o.clone())
            .with_datatype(flake.dt.clone());

        let found = fluree_db_core::range_with_overlay(
            &ledger.snapshot,
            g_id,
            ledger.novelty.as_ref(),
            fluree_db_core::IndexType::Spot,
            fluree_db_core::RangeTest::Eq,
            rm,
            fluree_db_core::RangeOptions::new().with_to_t(ledger.t()),
        )
        .await?;

        if let Some(existing) = found
            .into_iter()
            .find(|f| f.op && f.m.as_ref().and_then(|m| m.i).is_some())
        {
            flake.m = existing.m;
        }
    }

    Ok(())
}

/// Enforce modify policies on staged flakes
///
/// This function handles the complete policy enforcement flow:
/// 1. Populates the class cache for f:onClass policy support (if needed)
/// 2. Enforces modify policies on each flake with full f:query support
///
/// Returns `Ok(())` if all flakes pass policy, or an error if any flake is denied
/// or if the class cache population fails.
async fn enforce_modify_policies(
    flakes: &[Flake],
    policy: &PolicyContext,
    ledger: &LedgerState,
    tracker: Option<&Tracker>,
    reverse_graph: &HashMap<Sid, GraphId>,
) -> Result<()> {
    // Pre-populate class cache for f:onClass policy support, per graph.
    if policy.wrapper().has_class_policies() {
        // Group subjects by graph to populate class cache with correct g_id.
        let mut subjects_by_graph: HashMap<GraphId, HashSet<Sid>> = HashMap::new();
        for flake in flakes {
            let g_id = resolve_flake_graph_id(flake, reverse_graph)?;
            subjects_by_graph
                .entry(g_id)
                .or_default()
                .insert(flake.s.clone());
        }

        for (g_id, subjects) in &subjects_by_graph {
            let subject_vec: Vec<Sid> = subjects.iter().cloned().collect();
            populate_class_cache(&subject_vec, ledger.as_graph_db_ref(*g_id), policy)
                .await
                .map_err(|e| {
                    TransactError::Query(fluree_db_query::QueryError::Internal(format!(
                        "Failed to populate class cache: {}",
                        e
                    )))
                })?;
        }
    }

    // Enforce modify policies with full f:query support
    enforce_modify_policy_per_flake(flakes, policy, ledger, tracker, reverse_graph).await
}

/// Enforce modify policies on each flake individually
///
/// Returns `Ok(())` if all flakes pass policy, or `Err(PolicyError)` with
/// the policy's f:exMessage if any flake is denied.
///
/// This function supports f:query policies by executing them against
/// the pre-transaction ledger view (db + novelty at current t).
async fn enforce_modify_policy_per_flake(
    flakes: &[Flake],
    policy: &PolicyContext,
    ledger: &LedgerState,
    tracker: Option<&Tracker>,
    reverse_graph: &HashMap<Sid, GraphId>,
) -> Result<()> {
    // Build per-graph QueryPolicyExecutors so f:query policies execute against
    // the correct graph. Cache executors to avoid rebuilding for every flake.
    let mut executors: HashMap<GraphId, QueryPolicyExecutor<'_>> = HashMap::new();

    // Clojure parity: fuel is counted for work performed. For transactions, we count
    // one unit of fuel per staged (non-schema) flake, regardless of whether the
    // transaction ultimately fails policy enforcement.
    if let Some(tracker) = tracker {
        for flake in flakes {
            if is_schema_flake(&flake.p, &flake.o) {
                continue;
            }
            // Ignore fuel errors here; upstream can choose to set max-fuel.
            let _ = tracker.consume_fuel_one();
        }
    }

    // Use the provided tracker for async calls. We already consumed fuel above,
    // so policy execution tracking is additive.
    let async_tracker = tracker.cloned().unwrap_or_else(Tracker::disabled);

    for flake in flakes {
        // Schema flakes always allowed (needed for internal operations)
        if is_schema_flake(&flake.p, &flake.o) {
            continue;
        }

        // Get subject classes from cache (empty if not cached)
        // Class cache is populated per-graph by enforce_modify_policies() above.
        let subject_classes = policy
            .get_cached_subject_classes(&flake.s)
            .unwrap_or_default();

        // Resolve the graph for this flake and get/create a cached executor.
        let g_id = resolve_flake_graph_id(flake, reverse_graph)?;
        let executor = executors.entry(g_id).or_insert_with(|| {
            // Clojure parity: modify policy queries see the state *before* this transaction.
            QueryPolicyExecutor::with_overlay(&ledger.snapshot, ledger.novelty.as_ref(), ledger.t())
                .with_graph_id(g_id)
        });

        // Evaluate modify policies with full f:query support using detailed API
        let decision = policy
            .allow_modify_flake_async_detailed(
                &flake.s,
                &flake.p,
                &flake.o,
                &subject_classes,
                executor,
                &async_tracker,
            )
            .await?;

        if let PolicyDecision::Denied { .. } = &decision {
            // Extract error message from the candidate restrictions, or use default
            let message = decision
                .deny_message()
                .unwrap_or("Policy enforcement prevents modification.");
            return Err(PolicyError::modify_denied(message.to_string()).into());
        }
    }
    Ok(())
}

/// Execute WHERE patterns and return bindings
///
/// This function lowers the `UnresolvedPattern` patterns (which use string IRIs)
/// to `Pattern` (with encoded Sids), then executes them against the ledger.
async fn execute_where(ledger: &LedgerState, txn: &mut Txn) -> Result<Batch> {
    // Lower UnresolvedPattern to Pattern using the ledger's LedgerSnapshot as the IRI encoder.
    // This also assigns VarIds to any variables referenced in WHERE patterns.
    let mut query_patterns =
        lower_where_patterns(&txn.where_patterns, &ledger.snapshot, &mut txn.vars)?;

    // If VALUES clause present, prepend it as first pattern (seeds the join)
    if let Some(inline_values) = &txn.values {
        let values_pattern = inline_values_to_pattern(inline_values)?;
        query_patterns.insert(0, values_pattern);
    }

    // If no patterns at all (no WHERE, no VALUES), return empty batch for simple INSERTs
    if query_patterns.is_empty() {
        let schema: Arc<[VarId]> = Arc::new([]);
        return Batch::empty(schema).map_err(|e| TransactError::Query(e.into()));
    }

    // Execute using the clean public API that handles multi-pattern joins
    //
    // IMPORTANT: Execute "as of" the ledger's current t (which may be ahead of db.t when novelty exists).
    let batches = fluree_db_query::execute_where_with_overlay_at_strict(
        ledger.as_graph_db_ref(0),
        &txn.vars,
        &query_patterns,
        None,
    )
    .await
    .map_err(TransactError::Query)?;

    // Merge batches into one
    merge_batches(batches, &txn.vars)
}

/// Lower UnresolvedPattern list to Pattern list
///
/// This converts string IRIs to encoded Sids using the database, and assigns
/// VarIds to variables using the provided VarRegistry (shared with INSERT/DELETE).
fn lower_where_patterns(
    patterns: &[UnresolvedPattern],
    db: &fluree_db_core::LedgerSnapshot,
    vars: &mut VarRegistry,
) -> Result<Vec<Pattern>> {
    let mut pp_counter: u32 = 0;
    lower_unresolved_patterns(patterns, db, vars, &mut pp_counter)
        .map_err(|e| TransactError::Parse(format!("WHERE pattern lowering: {}", e)))
}

/// Merge multiple batches into a single batch
fn merge_batches(batches: Vec<Batch>, vars: &VarRegistry) -> Result<Batch> {
    if batches.is_empty() {
        // Return empty batch with schema from vars
        let schema: Arc<[VarId]> = Arc::from(
            (0..vars.len())
                .map(|i| VarId(i as u16))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        );
        return Batch::empty(schema).map_err(|e| TransactError::Query(e.into()));
    }

    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }

    // Merge multiple batches
    let schema: Arc<[VarId]> = batches[0].schema().into();
    let num_cols = schema.len();
    let total_rows: usize = batches.iter().map(|b| b.len()).sum();

    let mut columns: Vec<Vec<Binding>> = (0..num_cols)
        .map(|_| Vec::with_capacity(total_rows))
        .collect();

    for batch in batches {
        for (col_idx, col) in columns.iter_mut().enumerate() {
            if let Some(src_col) = batch.column_by_idx(col_idx) {
                col.extend(src_col.iter().cloned());
            }
        }
    }

    Batch::new(schema, columns).map_err(|e| TransactError::Query(e.into()))
}

/// Generate a unique transaction ID for blank node skolemization
pub fn generate_txn_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}", now)
}

/// Convert a Binding to a (FlakeValue, datatype Sid) pair for flake generation
///
/// Returns `None` for non-materializable bindings (Unbound, Poisoned, Grouped, Iri).
/// This is used when generating retraction flakes from query results.
fn binding_to_flake_object(binding: &Binding) -> Option<(FlakeValue, Sid)> {
    match binding {
        Binding::Sid(sid) => Some((FlakeValue::Ref(sid.clone()), Sid::new(1, "id"))),
        Binding::IriMatch { primary_sid, .. } => {
            Some((FlakeValue::Ref(primary_sid.clone()), Sid::new(1, "id")))
        }
        Binding::Lit { val, dt, .. } => Some((val.clone(), dt.clone())),
        Binding::EncodedLit { .. } => None,
        Binding::EncodedSid { .. } => None, // Requires materialization
        Binding::EncodedPid { .. } => None, // Requires materialization
        // Non-materializable bindings
        Binding::Unbound | Binding::Poisoned => None,
        Binding::Grouped(_) => {
            debug_assert!(
                false,
                "Grouped binding encountered in flake generation (unexpected)"
            );
            None
        }
        Binding::Iri(_) => {
            debug_assert!(
                false,
                "Raw IRI binding cannot be materialized to flake (no SID)"
            );
            None
        }
    }
}

/// Convert a TemplateTerm to a Binding for VALUES clause
fn template_term_to_binding(term: &TemplateTerm) -> Result<Binding> {
    match term {
        TemplateTerm::Sid(sid) => Ok(Binding::Sid(sid.clone())),
        TemplateTerm::Value(val) => {
            let dt = infer_datatype(val);
            Ok(Binding::lit(val.clone(), dt))
        }
        TemplateTerm::Var(_) => Err(TransactError::InvalidTerm(
            "Variables not allowed in VALUES data rows".to_string(),
        )),
        TemplateTerm::BlankNode(_) => Err(TransactError::InvalidTerm(
            "Blank nodes not allowed in VALUES data rows".to_string(),
        )),
    }
}

/// Convert InlineValues to Pattern::Values
fn inline_values_to_pattern(values: &InlineValues) -> Result<Pattern> {
    let vars = values.vars.clone();
    let rows: Result<Vec<Vec<Binding>>> = values
        .rows
        .iter()
        .map(|row| row.iter().map(template_term_to_binding).collect())
        .collect();
    Ok(Pattern::Values { vars, rows: rows? })
}

/// Generate deletions for Upsert transactions
///
/// For each (subject, predicate, graph) tuple with concrete SIDs in the insert templates,
/// query existing values and generate retractions for them. This implements the
/// "replace mode" semantics of Upsert.
///
/// Named graph support: retractions are created in the same graph as the insert templates
/// to ensure proper cancellation with assertions.
async fn generate_upsert_deletions(
    ledger: &LedgerState,
    txn: &Txn,
    new_t: i64,
    graph_sids: &std::collections::HashMap<u16, Sid>,
) -> Result<Vec<fluree_db_core::Flake>> {
    use fluree_db_core::Flake;

    // Collect unique (subject, predicate, graph_id) tuples from insert templates
    // Include graph_id to ensure retractions are created in the correct graph
    let mut spg_tuples: HashSet<(Sid, Sid, Option<u16>)> = HashSet::new();
    for template in &txn.insert_templates {
        if let (TemplateTerm::Sid(s), TemplateTerm::Sid(p)) =
            (&template.subject, &template.predicate)
        {
            spg_tuples.insert((s.clone(), p.clone(), template.graph_id));
        }
        // Variables and blank nodes are skipped - we can't query for them
    }

    if spg_tuples.is_empty() {
        return Ok(Vec::new());
    }

    let mut retractions = Vec::new();

    // Query existing values for each (subject, predicate, graph) tuple
    let mut query_vars = VarRegistry::new();
    let o_var = query_vars.get_or_insert("?o");

    for (subject, predicate, graph_id) in spg_tuples {
        // IMPORTANT: `TripleTemplate.graph_id` is a transaction-local ID.
        // It must be translated to a ledger-stable GraphId before we can query
        // the correct per-graph index partition.
        //
        // txn_local_g_id -> graph IRI (txn.graph_delta) -> ledger g_id (GraphRegistry)
        let ledger_g_id: Option<u16> = graph_id.and_then(|txn_g_id| {
            txn.graph_delta
                .get(&txn_g_id)
                .and_then(|iri| ledger.snapshot.graph_registry.graph_id_for_iri(iri))
        });

        // Query: <subject> <predicate> ?o
        let pattern = TriplePattern::new(
            Term::Sid(subject.clone()),
            Term::Sid(predicate.clone()),
            Term::Var(o_var),
        );

        let batches = if graph_id.is_some() {
            // Named graph: translate txn-local g_id to ledger g_id before querying.
            match ledger_g_id {
                None => {
                    // Graph is not yet in the ledger registry (new graph in this txn),
                    // so there cannot be existing values to retract.
                    Vec::new()
                }
                Some(g_id) => {
                    if ledger.snapshot.range_provider.is_some() {
                        fluree_db_query::execute_pattern_with_overlay_at(
                            ledger.as_graph_db_ref(g_id),
                            &query_vars,
                            pattern,
                            None,
                        )
                        .await?
                    } else {
                        // No binary store available (genesis / not indexed): scan novelty directly.
                        query_novelty_for_graph(ledger, &subject, &predicate, g_id, o_var)
                    }
                }
            }
        } else {
            // Default graph: use standard query path through range_provider
            fluree_db_query::execute_pattern_with_overlay_at(
                ledger.as_graph_db_ref(0),
                &query_vars,
                pattern,
                None,
            )
            .await?
        };

        // Convert each result to a retraction flake in the appropriate graph.
        // Here we use the txn-local g_id to look up the graph Sid (flake.g).
        let graph_sid = graph_id.and_then(|g_id| graph_sids.get(&g_id).cloned());

        for batch in batches.iter() {
            for row in 0..batch.len() {
                if let Some((o, dt)) = batch.get(row, o_var).and_then(binding_to_flake_object) {
                    let flake = if let Some(g) = graph_sid.clone() {
                        Flake::new_in_graph(
                            g,
                            subject.clone(),
                            predicate.clone(),
                            o,
                            dt,
                            new_t,
                            false, // retraction
                            None,
                        )
                    } else {
                        Flake::new(
                            subject.clone(),
                            predicate.clone(),
                            o,
                            dt,
                            new_t,
                            false, // retraction
                            None,
                        )
                    };
                    retractions.push(flake);
                }
            }
        }
    }

    Ok(retractions)
}

/// Query novelty directly for a specific named graph
///
/// This function scans the novelty overlay for flakes matching the given
/// subject, predicate, and graph context. It's used for named graph upserts
/// because the db.range_provider is scoped to the default graph (g_id=0).
fn query_novelty_for_graph(
    ledger: &LedgerState,
    subject: &Sid,
    predicate: &Sid,
    target_g_id: u16,
    o_var: VarId,
) -> Vec<Batch> {
    use fluree_db_core::IndexType;

    // Collect matching flakes from novelty for the target graph
    let mut matching_values = Vec::new();
    ledger.novelty.for_each_overlay_flake(
        target_g_id,
        IndexType::Spot,
        None,
        None,
        true,
        ledger.t(),
        &mut |flake| {
            // Check if flake matches (subject, predicate) and is an assertion
            if &flake.s == subject && &flake.p == predicate && flake.op {
                matching_values.push((flake.o.clone(), flake.dt.clone()));
            }
        },
    );

    // Convert to batch format
    if matching_values.is_empty() {
        return Vec::new();
    }

    // Create a simple batch with just the object values
    let schema: Arc<[VarId]> = Arc::new([o_var]);
    let mut o_col = Vec::with_capacity(matching_values.len());
    for (o, dt) in &matching_values {
        o_col.push(Binding::from_object(o.clone(), dt.clone()));
    }

    match Batch::new(schema, vec![o_col]) {
        Ok(batch) => vec![batch],
        Err(_) => Vec::new(),
    }
}
/// Stage a transaction with SHACL validation
///
/// This is the same as [`stage`], but additionally validates the staged flakes
/// against SHACL shapes compiled from the database. If validation fails, the
/// function returns an error with the validation report.
///
/// # Arguments
///
/// * `ledger` - The ledger state (consumed by value)
/// * `txn` - The parsed transaction IR
/// * `ns_registry` - Namespace registry for IRI resolution
/// * `options` - Optional configuration for backpressure, policy, and tracking
/// * `shacl_cache` - Compiled SHACL shapes for validation
///
/// # Returns
///
/// Returns `(LedgerView, NamespaceRegistry)` if staging and validation succeed.
/// Returns `TransactError::ShaclViolation` if SHACL validation fails.
#[cfg(feature = "shacl")]
pub async fn stage_with_shacl(
    ledger: LedgerState,
    txn: Txn,
    ns_registry: NamespaceRegistry,
    options: StageOptions<'_>,
    shacl_cache: &ShaclCache,
) -> Result<(LedgerView, NamespaceRegistry)> {
    // Capture graph_delta before stage() consumes the txn (needed for per-graph SHACL).
    let graph_delta = txn.graph_delta.clone();

    // First, perform regular staging
    let (view, mut ns_registry) = stage(ledger, txn, ns_registry, options).await?;

    // Fast path: if there are no SHACL shapes, elide validation entirely.
    // This ensures SHACL has *zero* transaction-time overhead unless rules exist.
    if shacl_cache.is_empty() {
        return Ok((view, ns_registry));
    }

    // Rebuild graph_sids from the cloned graph_delta + returned ns_registry.
    // These IRIs were already resolved during stage(), so sid_for_iri will hit
    // the trie cache — no new allocations.
    let graph_sids: HashMap<GraphId, Sid> = graph_delta
        .iter()
        .map(|(&g_id, iri)| (g_id, ns_registry.sid_for_iri(iri)))
        .collect();

    // Create SHACL engine from cache
    let engine = ShaclEngine::new(shacl_cache.clone());

    // Validate staged flakes against shapes (per graph)
    let report = validate_staged_nodes(&view, &engine, Some(&graph_sids)).await?;

    if !report.conforms {
        return Err(TransactError::ShaclViolation(format_shacl_report(&report)));
    }

    Ok((view, ns_registry))
}

/// Validate a staged [`LedgerView`] against SHACL shapes.
///
/// This is a helper for callers that already have pre-built flakes and stage them
/// via [`stage_flakes`], but still want SHACL validation parity with [`stage_with_shacl`].
///
/// `graph_sids` provides the `GraphId → Sid` mapping for per-graph validation.
/// Pass `None` when the mapping is unavailable (e.g., commit-transfer path) —
/// validation will fall back to the default graph (g_id=0).
///
/// Returns `Ok(())` when conforming, or `TransactError::ShaclViolation` when any
/// violations are present.
#[cfg(feature = "shacl")]
pub async fn validate_view_with_shacl(
    view: &LedgerView,
    shacl_cache: &ShaclCache,
    graph_sids: Option<&HashMap<GraphId, Sid>>,
) -> Result<()> {
    // Fast path: if there are no SHACL shapes, elide validation entirely.
    if shacl_cache.is_empty() {
        return Ok(());
    }

    let engine = ShaclEngine::new(shacl_cache.clone());
    let report = validate_staged_nodes(view, &engine, graph_sids).await?;
    if !report.conforms {
        return Err(TransactError::ShaclViolation(format_shacl_report(&report)));
    }
    Ok(())
}

/// Validate staged nodes against SHACL shapes, per graph.
///
/// Groups staged subjects by their graph and validates each group with a
/// `GraphDbRef` targeting the correct `g_id`. Shape compilation stays at
/// g_id=0 (shapes are schema-level definitions in the default graph).
///
/// When `graph_sids` is `None` (e.g., commit-transfer path where the txn
/// context is unavailable), falls back to validating all subjects against
/// the default graph (g_id=0) — matching the previous behavior.
#[cfg(feature = "shacl")]
async fn validate_staged_nodes(
    view: &LedgerView,
    engine: &ShaclEngine,
    graph_sids: Option<&HashMap<GraphId, Sid>>,
) -> Result<ValidationReport> {
    use fluree_vocab::namespaces::RDF;
    use fluree_vocab::rdf_names;

    // Fast path: no shapes means no validation work.
    if engine.cache().all_shapes().is_empty() {
        return Ok(ValidationReport::conforming());
    }

    if !view.has_staged() {
        return Ok(ValidationReport::conforming());
    }

    // Group staged (subject, g_id) pairs. A subject may appear in multiple graphs.
    let reverse_graph = graph_sids.map(build_reverse_graph_lookup);
    let mut subjects_by_graph: HashMap<GraphId, HashSet<Sid>> = HashMap::new();
    for flake in view.staged_flakes() {
        let g_id = match (&flake.g, &reverse_graph) {
            (None, _) => 0,
            (Some(g_sid), Some(rev)) => rev.get(g_sid).copied().unwrap_or(0),
            // No reverse map (commit-transfer path): fall back to default graph
            (Some(_), None) => 0,
        };
        subjects_by_graph
            .entry(g_id)
            .or_default()
            .insert(flake.s.clone());
    }

    let snapshot = view.db();
    let mut all_results = Vec::new();

    for (g_id, subjects) in &subjects_by_graph {
        // Build GraphDbRef for this graph.
        // Use staged_t so GraphDbRef sees staged flakes (which have t > snapshot.t).
        let db = fluree_db_core::GraphDbRef::new(snapshot, *g_id, view, view.staged_t());

        for subject in subjects {
            // Get the node's types for shape targeting
            let rdf_type = Sid::new(RDF, rdf_names::TYPE);
            let type_flakes = db
                .range(
                    fluree_db_core::IndexType::Spot,
                    fluree_db_core::RangeTest::Eq,
                    fluree_db_core::RangeMatch::subject_predicate(subject.clone(), rdf_type),
                )
                .await?;

            let node_types: Vec<Sid> = type_flakes
                .iter()
                .filter_map(|f| {
                    if let fluree_db_core::FlakeValue::Ref(type_sid) = &f.o {
                        Some(type_sid.clone())
                    } else {
                        None
                    }
                })
                .collect();

            // Validate this node (view implements OverlayProvider)
            let report = engine.validate_node(db, subject, &node_types).await?;
            all_results.extend(report.results);
        }
    }

    // Check conformance
    let conforms = all_results
        .iter()
        .all(|r| r.severity != fluree_db_shacl::Severity::Violation);

    Ok(ValidationReport {
        conforms,
        results: all_results,
    })
}

/// Format a SHACL validation report as a human-readable string
#[cfg(feature = "shacl")]
fn format_shacl_report(report: &ValidationReport) -> String {
    use std::fmt::Write;

    let mut output = String::new();
    writeln!(
        &mut output,
        "SHACL validation failed with {} violation(s):",
        report.violation_count()
    )
    .ok();

    for (i, result) in report
        .results
        .iter()
        .filter(|r| r.severity == fluree_db_shacl::Severity::Violation)
        .enumerate()
    {
        writeln!(&mut output, "  {}. {}", i + 1, result.message).ok();
        writeln!(
            &mut output,
            "     Focus node: {}{}",
            result.focus_node.namespace_code, result.focus_node.name
        )
        .ok();
        if let Some(path) = &result.result_path {
            writeln!(
                &mut output,
                "     Path: {}{}",
                path.namespace_code, path.name
            )
            .ok();
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{TemplateTerm, TripleTemplate, Txn};
    use fluree_db_core::{FlakeValue, LedgerSnapshot, MemoryStorage, Sid};
    use fluree_db_novelty::Novelty;
    use fluree_db_query::parse::{UnresolvedTerm, UnresolvedTriplePattern};

    /// Helper to create an UnresolvedPattern::Triple for WHERE clauses in tests
    fn where_triple(s: UnresolvedTerm, p: &str, o: UnresolvedTerm) -> UnresolvedPattern {
        UnresolvedPattern::Triple(UnresolvedTriplePattern::new(s, UnresolvedTerm::iri(p), o))
    }

    #[tokio::test]
    async fn test_stage_simple_insert() {
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        // Create a simple insert transaction
        let txn = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, _ns_registry) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        assert_eq!(view.staged_len(), 1);
    }

    #[tokio::test]
    async fn test_stage_insert_multiple_triples() {
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        // Insert multiple triples
        let txn = Txn::insert()
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(Sid::new(1, "ex:alice")),
                TemplateTerm::Sid(Sid::new(1, "ex:name")),
                TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(Sid::new(1, "ex:alice")),
                TemplateTerm::Sid(Sid::new(1, "ex:age")),
                TemplateTerm::Value(FlakeValue::Long(30)),
            ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, _) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        assert_eq!(view.staged_len(), 2);
    }

    #[tokio::test]
    async fn test_stage_with_blank_nodes() {
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        // Insert with blank node
        let txn = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::BlankNode("_:b1".to_string()),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Anonymous".to_string())),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, ns_registry) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        assert_eq!(view.staged_len(), 1);
        // Blank nodes use the predefined _: prefix (BLANK_NODE code), no new namespace allocation needed
        assert!(ns_registry.has_prefix("_:"));
    }

    #[tokio::test]
    async fn test_stage_backpressure_at_max() {
        use fluree_db_core::Flake;

        let db = LedgerSnapshot::genesis("test:main");

        // Create novelty that's at max size
        let mut novelty = Novelty::new(0);
        // Add a lot of flakes to exceed the limit
        for i in 0..1000 {
            let flake = Flake::new(
                Sid::new(1, format!("s{}", i)),
                Sid::new(1, "p"),
                FlakeValue::Long(i),
                Sid::new(2, "long"),
                1,
                true,
                None,
            );
            novelty
                .apply_commit(vec![flake], 1, &HashMap::new())
                .unwrap();
        }

        let ledger = LedgerState::new(db, novelty);

        // Use a very small config to trigger backpressure
        let config = IndexConfig {
            reindex_min_bytes: 100,
            reindex_max_bytes: 500, // Small limit
        };

        let txn = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        // Stage should fail with NoveltyAtMax
        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let options = StageOptions::new().with_index_config(&config);
        let result = stage(ledger, txn, ns_registry, options).await;
        assert!(matches!(result, Err(TransactError::NoveltyAtMax)));
    }

    #[tokio::test]
    async fn test_insert_with_blank_node_always_succeeds() {
        // Blank nodes are always new, so insert should succeed even if
        // the blank node was used before (it gets a new skolemized ID)
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let txn = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::BlankNode("_:b1".to_string()),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Test".to_string())),
        ));

        // Should succeed - blank nodes don't trigger existence check
        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let result = stage(ledger, txn, ns_registry, StageOptions::default()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_upsert_replaces_existing_values() {
        use crate::commit::{commit, CommitOpts};
        use fluree_db_nameservice::memory::MemoryNameService;

        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();
        let config = IndexConfig::default();

        // First: insert ex:alice with name="Alice"
        let txn1 = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view1, ns_registry1) = stage(ledger, txn1, ns_registry, StageOptions::default())
            .await
            .unwrap();
        let (_receipt, state1) = commit(
            view1,
            ns_registry1,
            &storage,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        // Now: upsert ex:alice with name="Alicia" (should replace)
        let txn2 = Txn::upsert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alicia".to_string())),
        ));

        let ns_registry2 = NamespaceRegistry::from_db(&state1.snapshot);
        let (view2, _ns_registry2) = stage(state1, txn2, ns_registry2, StageOptions::default())
            .await
            .unwrap();

        // Check that we have both a retraction and an assertion
        let (_base, staged) = view2.into_parts();

        // Should have 2 flakes: one retraction for "Alice", one assertion for "Alicia"
        assert_eq!(staged.len(), 2);

        // Find retraction
        let retraction = staged
            .iter()
            .find(|f| !f.op)
            .expect("should have retraction");
        assert_eq!(retraction.s.name.as_ref(), "ex:alice");
        assert_eq!(retraction.p.name.as_ref(), "ex:name");
        assert_eq!(retraction.o, FlakeValue::String("Alice".to_string()));

        // Find assertion
        let assertion = staged.iter().find(|f| f.op).expect("should have assertion");
        assert_eq!(assertion.s.name.as_ref(), "ex:alice");
        assert_eq!(assertion.p.name.as_ref(), "ex:name");
        assert_eq!(assertion.o, FlakeValue::String("Alicia".to_string()));
    }

    #[tokio::test]
    async fn test_upsert_on_nonexistent_subject() {
        // Upsert on a subject that doesn't exist should just insert
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let txn = Txn::upsert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, _) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        // Should have just one assertion (no retraction since nothing existed)
        assert_eq!(view.staged_len(), 1);
        let (_base, staged) = view.into_parts();
        assert!(staged[0].op); // assertion
    }

    #[tokio::test]
    async fn test_where_uses_ledger_t_not_db_t() {
        // Test that WHERE patterns see data in novelty (committed but not indexed),
        // not just data in the indexed db. This is the "time boundary" correctness test.
        use crate::commit::{commit, CommitOpts};
        use fluree_db_nameservice::memory::MemoryNameService;

        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();
        let config = IndexConfig::default();

        // Commit 1: Insert schema:alice with schema:name="Alice"
        // Do NOT rely on pre-registered SCHEMA_ORG codes — this build intentionally keeps
        // the default namespace table minimal. Allocate via NamespaceRegistry.
        let mut ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let schema_alice = ns_registry.sid_for_iri("http://schema.org/alice");
        let schema_name = ns_registry.sid_for_iri("http://schema.org/name");
        let txn1 = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(schema_alice.clone()),
            TemplateTerm::Sid(schema_name.clone()),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        ));

        let (view1, ns1) = stage(ledger, txn1, ns_registry, StageOptions::default())
            .await
            .unwrap();
        let (_r1, state1) = commit(
            view1,
            ns1,
            &storage,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        // state1 now has t=1 with data in NOVELTY (not indexed)
        assert_eq!(state1.t(), 1);
        // Novelty includes 1 txn flake + commit metadata flakes
        assert!(
            !state1.novelty.is_empty(),
            "novelty should have at least 1 transaction flake (Alice's name)"
        );

        // Commit 2: Update with WHERE pattern that should match data in novelty
        // This UPDATE should find schema:alice's name (in novelty) and change it
        let mut vars = VarRegistry::new();
        let name_var = vars.get_or_insert("?name");

        // WHERE pattern uses UnresolvedPattern with string IRIs.
        // The variable "?name" will be assigned the same VarId during lowering
        // as was registered for DELETE/INSERT templates.
        let txn2 = Txn::update()
            .with_where(where_triple(
                UnresolvedTerm::iri("http://schema.org/alice"),
                "http://schema.org/name",
                UnresolvedTerm::var("?name"),
            ))
            .with_delete(TripleTemplate::new(
                TemplateTerm::Sid(schema_alice.clone()),
                TemplateTerm::Sid(schema_name.clone()),
                TemplateTerm::Var(name_var),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(schema_alice),
                TemplateTerm::Sid(schema_name),
                TemplateTerm::Value(FlakeValue::String("Alicia".to_string())),
            ))
            .with_vars(vars);

        let mut ns_registry2 = NamespaceRegistry::from_db(&state1.snapshot);
        // Ensure schema.org prefix is present in the registry used for lowering.
        // (Should already be in LedgerSnapshot.namespace_codes via commit delta, but this makes the test robust.)
        let _ = ns_registry2.sid_for_iri("http://schema.org/alice");
        let _ = ns_registry2.sid_for_iri("http://schema.org/name");
        let (view2, _ns2) = stage(state1, txn2, ns_registry2, StageOptions::default())
            .await
            .unwrap();

        // The WHERE should have found "Alice" (in novelty), so we should have:
        // - A retraction for "Alice"
        // - An assertion for "Alicia"
        let (_base2, staged2) = view2.into_parts();
        assert_eq!(staged2.len(), 2);

        // Verify we got the retraction (proving WHERE saw the novelty data)
        let retraction = staged2.iter().find(|f| !f.op);
        assert!(
            retraction.is_some(),
            "WHERE should have found data in novelty"
        );
        assert_eq!(
            retraction.unwrap().o,
            FlakeValue::String("Alice".to_string())
        );
    }

    #[tokio::test]
    async fn test_multi_pattern_where_join() {
        // Test that multiple WHERE patterns are joined correctly.
        // This verifies that execute_where_with_overlay_at handles joins.
        use crate::commit::{commit, CommitOpts};
        use fluree_db_nameservice::memory::MemoryNameService;

        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();
        let config = IndexConfig::default();

        // Commit 1: Insert schema:alice with name="Alice" and age=30
        let mut ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let schema_alice = ns_registry.sid_for_iri("http://schema.org/alice");
        let schema_name = ns_registry.sid_for_iri("http://schema.org/name");
        let schema_age = ns_registry.sid_for_iri("http://schema.org/age");
        let txn1 = Txn::insert()
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(schema_alice.clone()),
                TemplateTerm::Sid(schema_name.clone()),
                TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(schema_alice.clone()),
                TemplateTerm::Sid(schema_age.clone()),
                TemplateTerm::Value(FlakeValue::Long(30)),
            ));

        let (view1, ns1) = stage(ledger, txn1, ns_registry, StageOptions::default())
            .await
            .unwrap();
        let (_r1, state1) = commit(
            view1,
            ns1,
            &storage,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        // Commit 2: Also insert schema:bob with only a name (no age)
        let mut ns_registry2 = NamespaceRegistry::from_db(&state1.snapshot);
        let schema_bob = ns_registry2.sid_for_iri("http://schema.org/bob");
        let schema_name2 = ns_registry2.sid_for_iri("http://schema.org/name");
        let txn2 = Txn::insert().with_insert(TripleTemplate::new(
            TemplateTerm::Sid(schema_bob.clone()),
            TemplateTerm::Sid(schema_name2.clone()),
            TemplateTerm::Value(FlakeValue::String("Bob".to_string())),
        ));

        let (view2, ns2) = stage(state1, txn2, ns_registry2, StageOptions::default())
            .await
            .unwrap();
        let (_r2, state2) = commit(
            view2,
            ns2,
            &storage,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        // Now: Multi-pattern UPDATE
        // WHERE { ?s schema:name ?name . ?s schema:age ?age }  <- requires BOTH patterns to match
        // DELETE { ?s schema:age ?age }
        // INSERT { ?s schema:age 31 }
        //
        // This should ONLY match schema:alice (who has both name and age).
        // schema:bob should NOT match (has name but no age).

        let mut vars = VarRegistry::new();
        let s_var = vars.get_or_insert("?s");
        let _name_var = vars.get_or_insert("?name");
        let age_var = vars.get_or_insert("?age");

        // WHERE patterns use UnresolvedPattern with string IRIs and variable names
        let txn3 = Txn::update()
            .with_where(where_triple(
                UnresolvedTerm::var("?s"),
                "http://schema.org/name",
                UnresolvedTerm::var("?name"),
            ))
            .with_where(where_triple(
                UnresolvedTerm::var("?s"),
                "http://schema.org/age",
                UnresolvedTerm::var("?age"),
            ))
            .with_delete(TripleTemplate::new(
                TemplateTerm::Var(s_var),
                TemplateTerm::Sid(schema_age.clone()),
                TemplateTerm::Var(age_var),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Var(s_var),
                TemplateTerm::Sid(schema_age),
                TemplateTerm::Value(FlakeValue::Long(31)),
            ))
            .with_vars(vars);

        let mut ns_registry3 = NamespaceRegistry::from_db(&state2.snapshot);
        // Ensure schema.org prefix exists for lowering WHERE IRIs.
        let _ = ns_registry3.sid_for_iri("http://schema.org/age");
        let _ = ns_registry3.sid_for_iri("http://schema.org/name");
        let (view3, _ns3) = stage(state2, txn3, ns_registry3, StageOptions::default())
            .await
            .unwrap();

        // Should have exactly 2 flakes:
        // - Retraction of schema:alice schema:age 30
        // - Assertion of schema:alice schema:age 31
        let (_base3, staged3) = view3.into_parts();
        assert_eq!(
            staged3.len(),
            2,
            "Should have exactly 2 flakes (1 retraction + 1 assertion)"
        );

        // Verify the retraction is for alice's old age
        let retraction = staged3
            .iter()
            .find(|f| !f.op)
            .expect("should have retraction");
        assert_eq!(retraction.s.name.as_ref(), "alice");
        assert_eq!(retraction.p.name.as_ref(), "age");
        assert_eq!(retraction.o, FlakeValue::Long(30));

        // Verify the assertion is for alice's new age
        let assertion = staged3
            .iter()
            .find(|f| f.op)
            .expect("should have assertion");
        assert_eq!(assertion.s.name.as_ref(), "alice");
        assert_eq!(assertion.p.name.as_ref(), "age");
        assert_eq!(assertion.o, FlakeValue::Long(31));
    }

    #[tokio::test]
    async fn test_values_seeding_insert() {
        // Test that VALUES can seed bindings for INSERT templates.
        // This supports transactions like:
        //   VALUES ?s ?name { (ex:alice "Alice") (ex:bob "Bob") }
        //   INSERT { ?s ex:name ?name }
        // Which should create two triples with different subjects and names.
        use crate::ir::InlineValues;

        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        // Create a transaction with VALUES seeding - using named subjects
        let mut vars = VarRegistry::new();
        let s_var = vars.get_or_insert("?s");
        let name_var = vars.get_or_insert("?name");

        let values = InlineValues::new(
            vec![s_var, name_var],
            vec![
                vec![
                    TemplateTerm::Sid(Sid::new(1, "ex:alice")),
                    TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
                ],
                vec![
                    TemplateTerm::Sid(Sid::new(1, "ex:bob")),
                    TemplateTerm::Value(FlakeValue::String("Bob".to_string())),
                ],
            ],
        );

        let txn = Txn::insert()
            .with_insert(TripleTemplate::new(
                TemplateTerm::Var(s_var),
                TemplateTerm::Sid(Sid::new(1, "ex:name")),
                TemplateTerm::Var(name_var),
            ))
            .with_values(values)
            .with_vars(vars);

        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let (view, _) = stage(ledger, txn, ns_registry, StageOptions::default())
            .await
            .unwrap();

        // Should have 2 assertions (one for "Alice", one for "Bob")
        let (_base, staged) = view.into_parts();
        assert_eq!(staged.len(), 2, "Should have 2 flakes from VALUES seeding");

        // Both should be assertions
        assert!(
            staged.iter().all(|f| f.op),
            "All flakes should be assertions"
        );

        // Verify we got both names with correct subjects
        let alice_flake = staged.iter().find(|f| f.s.name.as_ref() == "ex:alice");
        let bob_flake = staged.iter().find(|f| f.s.name.as_ref() == "ex:bob");

        assert!(alice_flake.is_some(), "Should have alice flake");
        assert!(bob_flake.is_some(), "Should have bob flake");

        assert_eq!(
            alice_flake.unwrap().o,
            FlakeValue::String("Alice".to_string())
        );
        assert_eq!(bob_flake.unwrap().o, FlakeValue::String("Bob".to_string()));
    }

    #[tokio::test]
    async fn test_values_seeding_with_where_join() {
        // Test VALUES seeding combined with WHERE patterns.
        // This verifies that VALUES can constrain which subjects are matched.
        use crate::commit::{commit, CommitOpts};
        use crate::ir::InlineValues;
        use fluree_db_nameservice::memory::MemoryNameService;

        let storage = MemoryStorage::new();
        let db = LedgerSnapshot::genesis("test:main");
        let novelty = Novelty::new(0);
        let ledger = LedgerState::new(db, novelty);

        let nameservice = MemoryNameService::new();
        let config = IndexConfig::default();

        // Insert data: alice has age 30, bob has age 25
        let mut ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let schema_alice = ns_registry.sid_for_iri("http://schema.org/alice");
        let schema_bob = ns_registry.sid_for_iri("http://schema.org/bob");
        let schema_age = ns_registry.sid_for_iri("http://schema.org/age");
        let txn1 = Txn::insert()
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(schema_alice.clone()),
                TemplateTerm::Sid(schema_age.clone()),
                TemplateTerm::Value(FlakeValue::Long(30)),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Sid(schema_bob.clone()),
                TemplateTerm::Sid(schema_age.clone()),
                TemplateTerm::Value(FlakeValue::Long(25)),
            ));

        let (view1, ns1) = stage(ledger, txn1, ns_registry, StageOptions::default())
            .await
            .unwrap();
        let (_r1, state1) = commit(
            view1,
            ns1,
            &storage,
            &nameservice,
            &config,
            CommitOpts::default(),
        )
        .await
        .unwrap();

        // Verify state after first commit
        assert_eq!(state1.t(), 1);
        // Novelty includes 2 txn flakes + commit metadata flakes
        assert!(
            state1.novelty.len() >= 2,
            "novelty should have at least 2 transaction flakes (alice and bob's ages)"
        );

        // Now: Update with VALUES constraining to only alice
        // VALUES ?s { schema:alice }
        // WHERE { ?s schema:age ?age }
        // DELETE { ?s schema:age ?age }
        // INSERT { ?s schema:age 35 }
        let mut vars = VarRegistry::new();
        let s_var = vars.get_or_insert("?s");
        let age_var = vars.get_or_insert("?age");

        let values = InlineValues::new(
            vec![s_var],
            vec![vec![TemplateTerm::Sid(schema_alice.clone())]],
        );

        // WHERE pattern uses UnresolvedPattern with string variable names
        let txn2 = Txn::update()
            .with_where(where_triple(
                UnresolvedTerm::var("?s"),
                "http://schema.org/age",
                UnresolvedTerm::var("?age"),
            ))
            .with_delete(TripleTemplate::new(
                TemplateTerm::Var(s_var),
                TemplateTerm::Sid(schema_age.clone()),
                TemplateTerm::Var(age_var),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Var(s_var),
                TemplateTerm::Sid(schema_age),
                TemplateTerm::Value(FlakeValue::Long(35)),
            ))
            .with_values(values)
            .with_vars(vars);

        let mut ns_registry2 = NamespaceRegistry::from_db(&state1.snapshot);
        let _ = ns_registry2.sid_for_iri("http://schema.org/age");
        let result = stage(state1, txn2, ns_registry2, StageOptions::default()).await;

        // Check if stage succeeded
        let (view2, _ns2) = result.expect("stage should succeed");

        // Should have exactly 2 flakes (retraction + assertion for alice only)
        let (_base2, staged2) = view2.into_parts();
        assert_eq!(
            staged2.len(),
            2,
            "Should have 2 flakes (alice only, not bob)"
        );

        // Verify only alice's age was affected
        let retraction = staged2
            .iter()
            .find(|f| !f.op)
            .expect("should have retraction");
        assert_eq!(retraction.s.name.as_ref(), "alice");
        assert_eq!(retraction.o, FlakeValue::Long(30));

        let assertion = staged2
            .iter()
            .find(|f| f.op)
            .expect("should have assertion");
        assert_eq!(assertion.s.name.as_ref(), "alice");
        assert_eq!(assertion.o, FlakeValue::Long(35));
    }
}
