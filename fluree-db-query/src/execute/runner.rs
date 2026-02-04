//! Unified query execution runner
//!
//! This module provides the core execution pipeline that all `execute_with_*`
//! functions share. By extracting the common logic here, we eliminate duplication
//! and ensure consistent behavior (including tracing) across all execution paths.

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::dataset::DataSet;
use crate::error::Result;
use crate::ir::Pattern;
use crate::operator::BoxedOperator;
use crate::options::QueryOptions;
use crate::parse::ParsedQuery;
use crate::pattern::{Term, TriplePattern};
use crate::reasoning::ReasoningOverlay;
use crate::rewrite_owl_ql::Ontology;
use crate::var_registry::VarRegistry;
use fluree_db_core::{Db, StatsView, Storage, Tracker};
use fluree_db_indexer::run_index::BinaryIndexStore;
use fluree_db_reasoner::DerivedFactsOverlay;
use std::sync::Arc;
use std::time::Instant;
use tracing::Instrument;

use super::operator_tree::build_operator_tree;
use super::reasoning_prep::{
    compute_derived_facts, effective_reasoning_modes, schema_hierarchy_with_overlay,
};
use super::rewrite_glue::rewrite_query_patterns;

/// Query with execution options
///
/// Combines a parsed query with solution modifiers for execution.
/// The `options` field allows overriding the options embedded in `ParsedQuery`.
#[derive(Debug)]
pub struct ExecutableQuery {
    /// The parsed query (contains embedded options)
    pub query: ParsedQuery,
    /// Execution options (may override query.options)
    pub options: QueryOptions,
}

impl ExecutableQuery {
    /// Create a new executable query with explicit options override
    pub fn new(query: ParsedQuery, options: QueryOptions) -> Self {
        Self { query, options }
    }

    /// Create an executable query using the query's embedded options
    pub fn simple(query: ParsedQuery) -> Self {
        let options = query.options.clone();
        Self { query, options }
    }
}

/// Prepared execution environment
///
/// Contains all the pre-computed state needed to execute a query:
/// - Derived facts overlay (if any)
/// - Rewritten patterns
/// - Operator tree
///
/// This struct captures the result of the "preparation" phase, which is
/// common to all execution paths. The actual execution just needs to
/// run the operator tree with an appropriate ExecutionContext.
///
/// # Future Enhancements
///
/// Additional fields may be added to support:
/// - Schema hierarchy for context building
/// - Reasoning modes for diagnostics/debugging
pub struct PreparedExecution<S: Storage + 'static> {
    /// The operator tree to execute
    pub operator: BoxedOperator<S>,
    /// Derived facts overlay (kept alive during execution)
    pub derived_overlay: Option<Arc<DerivedFactsOverlay>>,
}

/// Prepare query execution with an overlay
///
/// This performs all the common preparation steps:
/// 1. Compute schema hierarchy from overlay
/// 2. Determine effective reasoning modes
/// 3. Compute derived facts (OWL2-RL / datalog)
/// 4. Build ontology for OWL2-QL (if enabled)
/// 5. Rewrite patterns for reasoning
/// 6. Build operator tree
///
/// The result can then be executed with any ExecutionContext.
pub async fn prepare_execution<S: Storage + 'static>(
    db: &Db<S>,
    overlay: &dyn fluree_db_core::OverlayProvider,
    query: &ExecutableQuery,
    to_t: i64,
) -> Result<PreparedExecution<S>> {
    let span = tracing::debug_span!(
        "query_prepare",
        db_t = db.t,
        to_t = to_t,
        pattern_count = query.query.patterns.len()
    );
    async {
        tracing::debug!("preparing query execution");

        // Steps 1-4: Reasoning preparation (hierarchy, modes, derived facts, ontology)
        let (hierarchy, reasoning, derived_overlay, ontology) = async {
            // Step 1: Compute schema hierarchy from overlay
            let hierarchy = schema_hierarchy_with_overlay(db, overlay, to_t);

            // Step 2: Determine effective reasoning modes
            let reasoning =
                effective_reasoning_modes(&query.options.reasoning, hierarchy.is_some());

            tracing::Span::current().record("rdfs", reasoning.rdfs);
            tracing::Span::current().record("owl2ql", reasoning.owl2ql);
            tracing::Span::current().record("owl2rl", reasoning.owl2rl);
            tracing::Span::current().record("datalog", reasoning.datalog);

            if reasoning.rdfs || reasoning.owl2ql || reasoning.owl2rl || reasoning.datalog {
                tracing::debug!("reasoning enabled");
            }

            // Step 3: Compute derived facts from OWL2-RL and/or datalog rules
            let derived_overlay = compute_derived_facts(db, overlay, to_t, &reasoning).await;

            // Step 4: Build ontology for OWL2-QL mode (if enabled)
            // Note: We need to use the effective overlay for ontology building
            let reasoning_overlay_for_ontology: Option<ReasoningOverlay<'_>> = derived_overlay
                .as_ref()
                .map(|derived| ReasoningOverlay::new(overlay, derived.clone()));

            let effective_overlay_for_ontology: &dyn fluree_db_core::OverlayProvider =
                reasoning_overlay_for_ontology
                    .as_ref()
                    .map(|o| o as &dyn fluree_db_core::OverlayProvider)
                    .unwrap_or(overlay);

            let ontology = if reasoning.owl2ql {
                tracing::debug!("building OWL2-QL ontology");
                Some(
                    Ontology::from_db_with_overlay(
                        db,
                        effective_overlay_for_ontology,
                        to_t as u64,
                        to_t,
                    )
                    .await?,
                )
            } else {
                None
            };

            Ok::<_, crate::error::QueryError>((hierarchy, reasoning, derived_overlay, ontology))
        }
        .instrument(tracing::debug_span!(
            "reasoning_prep",
            rdfs = tracing::field::Empty,
            owl2ql = tracing::field::Empty,
            owl2rl = tracing::field::Empty,
            datalog = tracing::field::Empty,
        ))
        .await?;

        // Step 5: Rewrite patterns for reasoning
        //
        // OWL2-QL rewriting (and current RDFS expansion) require SIDs for ontology/hierarchy lookup.
        // Lowering may produce `Term::Iri` to support cross-ledger joins; for single-ledger execution
        // we can safely encode IRIs to SIDs here.
        fn encode_term<S: Storage + 'static>(db: &Db<S>, t: &Term) -> Term {
            match t {
                Term::Iri(iri) => db
                    .encode_iri(iri)
                    .map(Term::Sid)
                    .unwrap_or_else(|| t.clone()),
                _ => t.clone(),
            }
        }

        fn encode_patterns_for_reasoning<S: Storage + 'static>(
            db: &Db<S>,
            patterns: &[Pattern],
        ) -> Vec<Pattern> {
            patterns
                .iter()
                .map(|p| match p {
                    Pattern::Triple(tp) => Pattern::Triple(TriplePattern {
                        s: encode_term(db, &tp.s),
                        p: encode_term(db, &tp.p),
                        o: encode_term(db, &tp.o),
                        dt: tp.dt.clone(),
                        lang: tp.lang.clone(),
                    }),
                    Pattern::Optional(inner) => {
                        Pattern::Optional(encode_patterns_for_reasoning(db, inner))
                    }
                    Pattern::Union(branches) => Pattern::Union(
                        branches
                            .iter()
                            .map(|b| encode_patterns_for_reasoning(db, b))
                            .collect(),
                    ),
                    Pattern::Minus(inner) => {
                        Pattern::Minus(encode_patterns_for_reasoning(db, inner))
                    }
                    Pattern::Exists(inner) => {
                        Pattern::Exists(encode_patterns_for_reasoning(db, inner))
                    }
                    Pattern::NotExists(inner) => {
                        Pattern::NotExists(encode_patterns_for_reasoning(db, inner))
                    }
                    Pattern::Graph { name, patterns } => Pattern::Graph {
                        name: name.clone(),
                        patterns: encode_patterns_for_reasoning(db, patterns),
                    },
                    _ => p.clone(),
                })
                .collect()
        }

        let rewritten_patterns = {
            let span = tracing::debug_span!(
                "pattern_rewrite",
                patterns_before = query.query.patterns.len(),
                patterns_after = tracing::field::Empty,
            );
            let _guard = span.enter();

            let patterns_for_rewrite = if reasoning.rdfs || reasoning.owl2ql {
                encode_patterns_for_reasoning(db, &query.query.patterns)
            } else {
                query.query.patterns.clone()
            };
            let (rewritten_patterns, _diag) = rewrite_query_patterns(
                &patterns_for_rewrite,
                hierarchy.clone(),
                &reasoning,
                ontology.as_ref(),
            );

            tracing::Span::current().record("patterns_after", rewritten_patterns.len());

            if rewritten_patterns.len() != query.query.patterns.len() {
                tracing::debug!("patterns rewritten for reasoning");
            }

            rewritten_patterns
        };

        // Build query with rewritten patterns
        let rewritten_query = query.query.with_patterns(rewritten_patterns);

        // Step 6: Build operator tree
        //
        // IMPORTANT (dataset queries):
        // In multi-ledger dataset execution, the caller passes a "primary" default-graph db
        // here. That db is used as the planning/optimization stats source for the whole query.
        // The actual scan operators will still union across all default graphs via the attached
        // `DataSet` in the `ExecutionContext`.
        let operator = {
            let span =
                tracing::debug_span!("plan", pattern_count = rewritten_query.patterns.len(),);
            let _guard = span.enter();

            let stats_view = db.stats.as_ref().map(|s| {
                Arc::new(StatsView::from_db_stats_with_namespaces(
                    s,
                    &db.namespace_codes,
                ))
            });
            build_operator_tree(&rewritten_query, &query.options, stats_view)?
        };

        Ok(PreparedExecution {
            operator,
            derived_overlay,
        })
    }
    .instrument(span)
    .await
}

/// Run an operator tree to completion and collect all result batches
///
/// This is the common execution loop used by all execution paths.
/// Includes consistent tracing for debugging and monitoring.
pub async fn run_operator<S: Storage + 'static>(
    mut operator: BoxedOperator<S>,
    ctx: &ExecutionContext<'_, S>,
) -> Result<Vec<Batch>> {
    let op_type = std::any::type_name_of_val(operator.as_ref());
    let span = tracing::info_span!(
        "query_run",
        operator = op_type,
        to_t = ctx.to_t,
        from_t = tracing::field::Empty,
        history_mode = ctx.history_mode,
        has_overlay = ctx.overlay.is_some(),
        batch_size = ctx.batch_size,
        open_ms = tracing::field::Empty,
        total_ms = tracing::field::Empty,
        total_batches = tracing::field::Empty,
        total_rows = tracing::field::Empty,
        max_batch_ms = tracing::field::Empty
    );
    async {
        tracing::Span::current().record("from_t", ctx.from_t);

        let open_start = Instant::now();
        operator.open(ctx).await?;
        tracing::Span::current().record(
            "open_ms",
            (open_start.elapsed().as_secs_f64() * 1000.0) as u64,
        );

        let mut results = Vec::new();
        let mut batch_count = 0;
        let mut total_rows: usize = 0;
        let mut max_batch_ms: u64 = 0;
        let run_start = Instant::now();
        while {
            let batch_start = Instant::now();
            let next = operator.next_batch(ctx).await?;
            let batch_ms = (batch_start.elapsed().as_secs_f64() * 1000.0) as u64;
            if batch_ms > max_batch_ms {
                max_batch_ms = batch_ms;
            }
            if let Some(batch) = next {
                batch_count += 1;
                total_rows += batch.len();
                tracing::debug!(
                    batch_num = batch_count,
                    row_count = batch.len(),
                    batch_ms,
                    "received batch"
                );
                results.push(batch);
                true
            } else {
                false
            }
        } {}

        operator.close();

        // If the operator is blocking, results often arrive in a small number of batches.
        // We record overall totals here; operator-level spans provide the breakdown.
        let total_ms = (run_start.elapsed().as_secs_f64() * 1000.0) as u64;
        tracing::Span::current().record("total_ms", total_ms);
        tracing::Span::current().record("total_batches", batch_count as u64);
        tracing::Span::current().record("total_rows", total_rows as u64);
        tracing::Span::current().record("max_batch_ms", max_batch_ms);
        tracing::info!(
            total_batches = batch_count,
            total_rows,
            "query execution completed"
        );

        Ok(results)
    }
    .instrument(span)
    .await
}

/// Execution context configuration
///
/// Specifies the optional components to add to the execution context.
/// This eliminates duplication in the execute_prepared_* functions.
pub struct ContextConfig<'a, 'b, S: Storage + 'static> {
    pub tracker: Option<&'a Tracker>,
    /// Policy enforcer for async policy evaluation with full f:query support.
    ///
    /// When set, scan operators will use per-leaf batch filtering via `filter_flakes`.
    /// Access the raw PolicyContext via `enforcer.policy()` if needed.
    pub policy_enforcer: Option<Arc<crate::policy::QueryPolicyEnforcer>>,
    pub dataset: Option<&'a DataSet<'a, S>>,
    pub r2rml: Option<(
        &'b dyn crate::r2rml::R2rmlProvider,
        &'b dyn crate::r2rml::R2rmlTableProvider,
    )>,
    /// BM25 index provider for `Pattern::IndexSearch` (virtual graph BM25 queries).
    ///
    /// When set, BM25 search operators can load indexes from virtual graphs.
    pub bm25_provider: Option<&'b dyn crate::bm25::Bm25IndexProvider>,
    /// Vector index provider for `Pattern::VectorSearch` (virtual graph vector queries).
    ///
    /// When set, vector search operators can load indexes from virtual graphs.
    pub vector_provider: Option<&'b dyn crate::vector::VectorIndexProvider>,
    /// Enable history mode - captures op metadata in bindings for @op support
    pub history_mode: bool,
    /// When true, bind evaluation errors become query errors.
    pub strict_bind_errors: bool,
    /// Binary columnar index store for `BinaryScanOperator`.
    ///
    /// This is the explicit path — separate from `Db.range_provider` which
    /// serves the transparent `range_with_overlay()` callers.
    pub binary_store: Option<Arc<BinaryIndexStore>>,
    /// Graph ID for binary index lookups (default 0 = default graph).
    pub binary_g_id: u32,
}

impl<'a, 'b, S: Storage + 'static> Default for ContextConfig<'a, 'b, S> {
    fn default() -> Self {
        Self {
            tracker: None,
            policy_enforcer: None,
            dataset: None,
            r2rml: None,
            bm25_provider: None,
            vector_provider: None,
            history_mode: false,
            strict_bind_errors: false,
            binary_store: None,
            binary_g_id: 0,
        }
    }
}

/// Execute a prepared query with configurable context options
///
/// This is the unified internal execution path that handles all variants.
/// The `config` parameter specifies which optional components to add to the context.
pub async fn execute_prepared<'a, 'b, S: Storage + 'static>(
    db: &Db<S>,
    vars: &VarRegistry,
    overlay: &'a dyn fluree_db_core::OverlayProvider,
    prepared: PreparedExecution<S>,
    to_t: i64,
    from_t: Option<i64>,
    config: ContextConfig<'a, 'b, S>,
) -> Result<Vec<Batch>> {
    // Create composite overlay if we have derived facts
    let reasoning_overlay: Option<ReasoningOverlay<'a>> = prepared
        .derived_overlay
        .as_ref()
        .map(|derived| ReasoningOverlay::new(overlay, derived.clone()));

    // Use composite overlay if available, otherwise base overlay
    let effective_overlay: &dyn fluree_db_core::OverlayProvider = reasoning_overlay
        .as_ref()
        .map(|o| o as &dyn fluree_db_core::OverlayProvider)
        .unwrap_or(overlay);

    // Build context with all configured options
    let mut ctx =
        ExecutionContext::with_time_and_overlay(db, vars, to_t, from_t, effective_overlay);

    if let Some(tracker) = config.tracker {
        ctx = ctx.with_tracker(tracker.clone());
    }
    if let Some(enforcer) = config.policy_enforcer {
        ctx = ctx.with_policy_enforcer(enforcer);
    }
    if let Some(dataset) = config.dataset {
        ctx = ctx.with_dataset(dataset);
    }
    if let Some((r2rml_provider, r2rml_table_provider)) = config.r2rml {
        ctx = ctx.with_r2rml_providers(r2rml_provider, r2rml_table_provider);
    }
    if let Some(p) = config.bm25_provider {
        ctx = ctx.with_bm25_provider(p);
    }
    if let Some(p) = config.vector_provider {
        ctx = ctx.with_vector_provider(p);
    }
    if config.history_mode {
        ctx = ctx.with_history_mode();
    }
    if config.strict_bind_errors {
        ctx = ctx.with_strict_bind_errors();
    }
    if let Some(store) = config.binary_store {
        ctx = ctx.with_binary_store(store, config.binary_g_id);
    }

    run_operator(prepared.operator, &ctx).await
}

// ============================================================================
// Convenience wrappers for backward compatibility
// ============================================================================

/// Execute a prepared query with an overlay
pub async fn execute_prepared_with_overlay<'a, S: Storage + 'static>(
    db: &Db<S>,
    vars: &VarRegistry,
    overlay: &'a dyn fluree_db_core::OverlayProvider,
    prepared: PreparedExecution<S>,
    to_t: i64,
    from_t: Option<i64>,
) -> Result<Vec<Batch>> {
    execute_prepared(
        db,
        vars,
        overlay,
        prepared,
        to_t,
        from_t,
        ContextConfig {
            strict_bind_errors: true,
            ..Default::default()
        },
    )
    .await
}

/// Execute with overlay, time bounds, and optional tracker
pub async fn execute_prepared_with_overlay_tracked<'a, S: Storage + 'static>(
    db: &Db<S>,
    vars: &VarRegistry,
    overlay: &'a dyn fluree_db_core::OverlayProvider,
    prepared: PreparedExecution<S>,
    to_t: i64,
    from_t: Option<i64>,
    tracker: Option<&Tracker>,
) -> Result<Vec<Batch>> {
    execute_prepared(
        db,
        vars,
        overlay,
        prepared,
        to_t,
        from_t,
        ContextConfig {
            tracker,
            strict_bind_errors: true,
            ..Default::default()
        },
    )
    .await
}

/// Execute with overlay, time bounds, and policy (with async f:query support)
pub async fn execute_prepared_with_policy<'a, S: Storage + 'static>(
    db: &Db<S>,
    vars: &VarRegistry,
    overlay: &'a dyn fluree_db_core::OverlayProvider,
    prepared: PreparedExecution<S>,
    to_t: i64,
    from_t: Option<i64>,
    policy: &'a fluree_db_policy::PolicyContext,
    tracker: Option<&'a Tracker>,
) -> Result<Vec<Batch>> {
    // Create policy enforcer for async f:query support
    let enforcer = Arc::new(crate::policy::QueryPolicyEnforcer::new(Arc::new(
        policy.clone(),
    )));

    execute_prepared(
        db,
        vars,
        overlay,
        prepared,
        to_t,
        from_t,
        ContextConfig {
            tracker,
            policy_enforcer: Some(enforcer),
            strict_bind_errors: true,
            ..Default::default()
        },
    )
    .await
}

/// Execute with overlay, time bounds, tracker, and R2RML providers
pub async fn execute_prepared_with_r2rml<'a, 'b, S: Storage + 'static>(
    db: &Db<S>,
    vars: &VarRegistry,
    overlay: &'a dyn fluree_db_core::OverlayProvider,
    prepared: PreparedExecution<S>,
    to_t: i64,
    from_t: Option<i64>,
    tracker: &'a Tracker,
    r2rml_provider: &'b dyn crate::r2rml::R2rmlProvider,
    r2rml_table_provider: &'b dyn crate::r2rml::R2rmlTableProvider,
) -> Result<Vec<Batch>> {
    execute_prepared(
        db,
        vars,
        overlay,
        prepared,
        to_t,
        from_t,
        ContextConfig {
            tracker: Some(tracker),
            r2rml: Some((r2rml_provider, r2rml_table_provider)),
            strict_bind_errors: true,
            ..Default::default()
        },
    )
    .await
}

/// Execute with dataset (multi-graph query)
pub async fn execute_prepared_with_dataset<'a, S: Storage + 'static>(
    db: &Db<S>,
    vars: &VarRegistry,
    overlay: &'a dyn fluree_db_core::OverlayProvider,
    prepared: PreparedExecution<S>,
    to_t: i64,
    from_t: Option<i64>,
    dataset: &'a DataSet<'a, S>,
    tracker: Option<&'a Tracker>,
) -> Result<Vec<Batch>> {
    execute_prepared_with_dataset_history(
        db, vars, overlay, prepared, to_t, from_t, dataset, tracker, false,
    )
    .await
}

/// Execute with dataset (multi-graph query), with optional history mode
pub async fn execute_prepared_with_dataset_history<'a, S: Storage + 'static>(
    db: &Db<S>,
    vars: &VarRegistry,
    overlay: &'a dyn fluree_db_core::OverlayProvider,
    prepared: PreparedExecution<S>,
    to_t: i64,
    from_t: Option<i64>,
    dataset: &'a DataSet<'a, S>,
    tracker: Option<&'a Tracker>,
    history_mode: bool,
) -> Result<Vec<Batch>> {
    execute_prepared(
        db,
        vars,
        overlay,
        prepared,
        to_t,
        from_t,
        ContextConfig {
            tracker,
            dataset: Some(dataset),
            history_mode,
            strict_bind_errors: true,
            ..Default::default()
        },
    )
    .await
}

/// Execute with dataset and policy
pub async fn execute_prepared_with_dataset_and_policy<'a, S: Storage + 'static>(
    db: &Db<S>,
    vars: &VarRegistry,
    overlay: &'a dyn fluree_db_core::OverlayProvider,
    prepared: PreparedExecution<S>,
    to_t: i64,
    from_t: Option<i64>,
    dataset: &'a DataSet<'a, S>,
    policy: &'a fluree_db_policy::PolicyContext,
    tracker: Option<&'a Tracker>,
) -> Result<Vec<Batch>> {
    execute_prepared_with_dataset_and_policy_history(
        db, vars, overlay, prepared, to_t, from_t, dataset, policy, tracker, false,
    )
    .await
}

/// Execute with dataset and policy, with optional history mode
pub async fn execute_prepared_with_dataset_and_policy_history<'a, S: Storage + 'static>(
    db: &Db<S>,
    vars: &VarRegistry,
    overlay: &'a dyn fluree_db_core::OverlayProvider,
    prepared: PreparedExecution<S>,
    to_t: i64,
    from_t: Option<i64>,
    dataset: &'a DataSet<'a, S>,
    policy: &'a fluree_db_policy::PolicyContext,
    tracker: Option<&'a Tracker>,
    history_mode: bool,
) -> Result<Vec<Batch>> {
    // Create policy enforcer for async f:query support
    let enforcer = Arc::new(crate::policy::QueryPolicyEnforcer::new(Arc::new(
        policy.clone(),
    )));

    execute_prepared(
        db,
        vars,
        overlay,
        prepared,
        to_t,
        from_t,
        ContextConfig {
            tracker,
            policy_enforcer: Some(enforcer),
            dataset: Some(dataset),
            history_mode,
            strict_bind_errors: true,
            ..Default::default()
        },
    )
    .await
}

/// Execute with dataset and BM25 provider (for virtual graph BM25 queries)
pub async fn execute_prepared_with_dataset_and_bm25<'a, 'b, S: Storage + 'static>(
    db: &Db<S>,
    vars: &VarRegistry,
    overlay: &'a dyn fluree_db_core::OverlayProvider,
    prepared: PreparedExecution<S>,
    to_t: i64,
    from_t: Option<i64>,
    dataset: &'a DataSet<'a, S>,
    bm25_provider: &'b dyn crate::bm25::Bm25IndexProvider,
    tracker: Option<&'a Tracker>,
) -> Result<Vec<Batch>> {
    execute_prepared(
        db,
        vars,
        overlay,
        prepared,
        to_t,
        from_t,
        ContextConfig {
            tracker,
            dataset: Some(dataset),
            bm25_provider: Some(bm25_provider),
            strict_bind_errors: true,
            ..Default::default()
        },
    )
    .await
}

/// Execute with dataset, policy, and BM25 provider (for virtual graph BM25 queries with policy)
pub async fn execute_prepared_with_dataset_and_policy_and_bm25<'a, 'b, S: Storage + 'static>(
    db: &Db<S>,
    vars: &VarRegistry,
    overlay: &'a dyn fluree_db_core::OverlayProvider,
    prepared: PreparedExecution<S>,
    to_t: i64,
    from_t: Option<i64>,
    dataset: &'a DataSet<'a, S>,
    policy: &'a fluree_db_policy::PolicyContext,
    bm25_provider: &'b dyn crate::bm25::Bm25IndexProvider,
    tracker: Option<&'a Tracker>,
) -> Result<Vec<Batch>> {
    // Create policy enforcer for async f:query support
    let enforcer = Arc::new(crate::policy::QueryPolicyEnforcer::new(Arc::new(
        policy.clone(),
    )));

    execute_prepared(
        db,
        vars,
        overlay,
        prepared,
        to_t,
        from_t,
        ContextConfig {
            tracker,
            policy_enforcer: Some(enforcer),
            dataset: Some(dataset),
            bm25_provider: Some(bm25_provider),
            strict_bind_errors: true,
            ..Default::default()
        },
    )
    .await
}

/// Execute with dataset and both BM25 and vector providers (for virtual graph queries)
pub async fn execute_prepared_with_dataset_and_providers<'a, 'b, S: Storage + 'static>(
    db: &Db<S>,
    vars: &VarRegistry,
    overlay: &'a dyn fluree_db_core::OverlayProvider,
    prepared: PreparedExecution<S>,
    to_t: i64,
    from_t: Option<i64>,
    dataset: &'a DataSet<'a, S>,
    bm25_provider: &'b dyn crate::bm25::Bm25IndexProvider,
    vector_provider: &'b dyn crate::vector::VectorIndexProvider,
    tracker: Option<&'a Tracker>,
) -> Result<Vec<Batch>> {
    execute_prepared(
        db,
        vars,
        overlay,
        prepared,
        to_t,
        from_t,
        ContextConfig {
            tracker,
            dataset: Some(dataset),
            bm25_provider: Some(bm25_provider),
            vector_provider: Some(vector_provider),
            strict_bind_errors: true,
            ..Default::default()
        },
    )
    .await
}

/// Execute with dataset, policy, and both BM25 and vector providers
pub async fn execute_prepared_with_dataset_and_policy_and_providers<
    'a,
    'b,
    S: Storage + 'static,
>(
    db: &Db<S>,
    vars: &VarRegistry,
    overlay: &'a dyn fluree_db_core::OverlayProvider,
    prepared: PreparedExecution<S>,
    to_t: i64,
    from_t: Option<i64>,
    dataset: &'a DataSet<'a, S>,
    policy: &'a fluree_db_policy::PolicyContext,
    bm25_provider: &'b dyn crate::bm25::Bm25IndexProvider,
    vector_provider: &'b dyn crate::vector::VectorIndexProvider,
    tracker: Option<&'a Tracker>,
) -> Result<Vec<Batch>> {
    // Create policy enforcer for async f:query support
    let enforcer = Arc::new(crate::policy::QueryPolicyEnforcer::new(Arc::new(
        policy.clone(),
    )));

    execute_prepared(
        db,
        vars,
        overlay,
        prepared,
        to_t,
        from_t,
        ContextConfig {
            tracker,
            policy_enforcer: Some(enforcer),
            dataset: Some(dataset),
            bm25_provider: Some(bm25_provider),
            vector_provider: Some(vector_provider),
            strict_bind_errors: true,
            ..Default::default()
        },
    )
    .await
}
