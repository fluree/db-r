//! Transaction APIs (stage + commit) for Fluree DB
//!
//! This module wires `fluree-db-transact` + nameservice publishing + optional
//! indexing triggers into the high-level `fluree-db-api` surface.

use std::collections::HashMap;
use std::sync::Arc;

use crate::config_resolver;
#[cfg(feature = "shacl")]
use crate::config_resolver::EffectiveShaclConfig;
use crate::{ApiError, Result};
use crate::{TrackedErrorResponse, Tracker, TrackingOptions, TrackingTally};
use fluree_db_core::ledger_config::LedgerConfig;
#[cfg(feature = "shacl")]
use fluree_db_core::ledger_config::ValidationMode;
use fluree_db_core::{
    range_with_overlay, ContentAddressedWrite, ContentId, ContentKind, FlakeValue, GraphId,
    IndexType, RangeMatch, RangeOptions, RangeTest, Sid, Storage,
};
use fluree_db_indexer::IndexerHandle;
use fluree_db_ledger::{IndexConfig, LedgerState, LedgerView};
use fluree_db_nameservice::{NameService, Publisher};
use fluree_db_novelty::TxnMetaEntry;
#[cfg(feature = "shacl")]
use fluree_db_shacl::ShaclEngine;
use fluree_db_transact::stage as stage_txn;
use fluree_db_transact::{
    commit as commit_txn, parse_transaction, resolve_trig_meta, CommitOpts, CommitReceipt,
    NamedGraphBlock, NamespaceRegistry, RawTrigMeta, StageOptions, TemplateTerm, TripleTemplate,
    Txn, TxnOpts, TxnType,
};
#[cfg(feature = "shacl")]
use fluree_db_transact::{stage_with_shacl, validate_view_with_shacl};
use fluree_vocab::config_iris;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

fn ledger_id_from_txn(txn_json: &JsonValue) -> Result<&str> {
    let obj = txn_json
        .as_object()
        .ok_or_else(|| ApiError::config("Invalid transaction, missing required key: ledger."))?;
    obj.get("ledger")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ApiError::config("Invalid transaction, missing required key: ledger."))
}

/// Input parameters for tracked transactions with policy enforcement.
///
/// Bundles the common transaction parameters to reduce argument count.
pub struct TrackedTransactionInput<'a> {
    /// Transaction type (insert, delete, etc.)
    pub txn_type: TxnType,
    /// Transaction JSON body
    pub txn_json: &'a JsonValue,
    /// Transaction options
    pub txn_opts: TxnOpts,
    /// Policy context for access control
    pub policy: &'a crate::PolicyContext,
}

impl<'a> TrackedTransactionInput<'a> {
    /// Create new tracked transaction input.
    pub fn new(
        txn_type: TxnType,
        txn_json: &'a JsonValue,
        txn_opts: TxnOpts,
        policy: &'a crate::PolicyContext,
    ) -> Self {
        Self {
            txn_type,
            txn_json,
            txn_opts,
            policy,
        }
    }
}

/// Create a tracker for fuel limits only (no time/policy tracking).
///
/// This mirrors query behavior: even non-tracked transactions respect max-fuel.
fn tracker_for_limits(txn_json: &JsonValue) -> Tracker {
    let opts = txn_json.as_object().and_then(|o| o.get("opts"));
    let tracking = TrackingOptions::from_opts_value(opts);
    match tracking.max_fuel.filter(|limit| *limit > 0) {
        Some(limit) => Tracker::new(TrackingOptions {
            track_time: false,
            track_fuel: true,
            track_policy: false,
            max_fuel: Some(limit),
        }),
        None => Tracker::disabled(),
    }
}

/// Check if a JSON-LD document represents an empty default graph.
///
/// This is the case when:
/// - The document is null
/// - The document is an empty array
/// - The document is an object with only JSON-LD keywords (@context, @id, etc.)
///   AND the @graph key is missing or empty
///
/// This correctly handles envelope-form JSON-LD where data is in @graph.
fn is_empty_default_graph(json: &JsonValue) -> bool {
    match json {
        JsonValue::Null => true,
        JsonValue::Array(arr) => arr.is_empty(),
        JsonValue::Object(obj) => {
            // Check if there are any non-@ keys (actual data predicates at top level)
            let has_data_keys = obj.keys().any(|k| !k.starts_with('@'));
            if has_data_keys {
                return false;
            }
            // No data keys at top level, check if @graph has content
            match obj.get("@graph") {
                Some(JsonValue::Array(arr)) => arr.is_empty(),
                Some(JsonValue::Object(inner_obj)) => inner_obj.is_empty(),
                Some(_) => false, // @graph has some other non-empty value
                None => true,     // No @graph key, and no data keys = empty
            }
        }
        _ => false,
    }
}

// =============================================================================
// Config-driven transaction helpers (shacl feature only)
// =============================================================================

/// Load config from the pre-transaction ledger state.
///
/// INVARIANT: Reads config as-of the input `LedgerState` only (head t
/// before staging). Config mutations inside the staged transaction
/// CANNOT relax constraints for that same transaction.
///
/// Returns `None` if config graph is empty or unreadable (best-effort).
async fn load_transaction_config(ledger: &LedgerState) -> Option<Arc<LedgerConfig>> {
    match config_resolver::resolve_ledger_config(&ledger.snapshot, &*ledger.novelty, ledger.t())
        .await
    {
        Ok(Some(config)) => Some(Arc::new(config)),
        Ok(None) => None,
        Err(e) => {
            tracing::debug!(error = %e, "Config graph read failed during staging — using defaults");
            None
        }
    }
}

/// Resolve SHACL config across all graphs affected by a transaction.
///
/// Starts from the ledger-wide baseline (`resolve_effective_config(config, None)`)
/// and overlays per-graph config for each named graph in `graph_delta`.
/// Returns the strictest combination:
/// - `enabled`: true if ANY graph has SHACL enabled
/// - `validation_mode`: `Reject` if ANY graph is `Reject`
///
/// The ledger-wide baseline is always included because SHACL shapes live in the
/// default/schema graph (g_id=0) and target instances in any graph. Even if a
/// transaction only touches named graphs, the ledger-wide SHACL posture applies.
///
/// Note: `graph_delta` for normal JSON-LD transactions (non-import) contains ALL
/// named graphs referenced by the transaction, not just newly-created ones.
/// The `GraphIdAssigner` is created fresh per transaction during JSON-LD parsing.
#[cfg(feature = "shacl")]
fn resolve_txn_shacl_config(
    config: &LedgerConfig,
    graph_delta: &FxHashMap<u16, String>,
) -> Option<EffectiveShaclConfig> {
    // Ledger-wide baseline: the default SHACL posture before per-graph overrides.
    let default_resolved = config_resolver::resolve_effective_config(config, None);
    let mut strictest = config_resolver::merge_shacl_opts(&default_resolved, None);

    // Overlay per-graph config for each named graph in the transaction.
    // Graphs without per-graph overrides inherit the ledger-wide baseline
    // via three-tier resolution inside resolve_effective_config.
    for graph_iri in graph_delta.values() {
        let resolved = config_resolver::resolve_effective_config(config, Some(graph_iri));
        if let Some(per_graph) = config_resolver::merge_shacl_opts(&resolved, None) {
            strictest = Some(match strictest {
                Some(s) => EffectiveShaclConfig {
                    // any-enabled wins (strictest)
                    enabled: s.enabled || per_graph.enabled,
                    validation_mode: match (s.validation_mode, per_graph.validation_mode) {
                        // reject wins over warn (strictest)
                        (ValidationMode::Reject, _) | (_, ValidationMode::Reject) => {
                            ValidationMode::Reject
                        }
                        _ => ValidationMode::Warn,
                    },
                },
                None => per_graph,
            });
        }
    }

    strictest
}

/// Perform staging with config-aware SHACL validation.
///
/// Decision tree:
/// 1. Load config from pre-transaction state
/// 2. If config exists: resolve per-graph, apply strictest-wins
/// 3. If no config: fall back to shapes-exist heuristic (backward compat)
/// 4. If SHACL disabled: plain `stage()`
/// 5. If SHACL enabled + Reject: `stage_with_shacl()` (existing path)
/// 6. If SHACL enabled + Warn: `stage()` + `validate_view_with_shacl()`, log warnings
#[cfg(feature = "shacl")]
async fn stage_with_config_shacl(
    ledger: LedgerState,
    txn: Txn,
    ns_registry: NamespaceRegistry,
    options: StageOptions<'_>,
) -> std::result::Result<(LedgerView, NamespaceRegistry), fluree_db_transact::TransactError> {
    // 1. Load config from pre-transaction state
    let config = load_transaction_config(&ledger).await;

    // 2. Resolve SHACL settings (per-graph strictest-wins)
    let shacl_config = config
        .as_ref()
        .and_then(|c| resolve_txn_shacl_config(c, &txn.graph_delta));

    // 3. Determine enablement
    //    - Config present → use config's enabled flag
    //    - No config → shapes-exist heuristic (build engine, check if cache is empty)
    let (shacl_enabled, validation_mode) = match &shacl_config {
        Some(c) => (c.enabled, c.validation_mode),
        None => {
            // No config: fall back to shapes-exist heuristic.
            // "true" means "try it" — empty cache → fast no-op below.
            (true, ValidationMode::Reject)
        }
    };

    if !shacl_enabled {
        return stage_txn(ledger, txn, ns_registry, options).await;
    }

    // 4. Build SHACL engine
    let engine = ShaclEngine::from_db_with_overlay(ledger.as_graph_db_ref(0), ledger.ledger_id())
        .await
        .map_err(fluree_db_transact::TransactError::from)?;
    let shacl_cache = engine.cache();

    // No config + no shapes → skip (backward compat: no shapes = no SHACL)
    if shacl_config.is_none() && shacl_cache.is_empty() {
        return stage_txn(ledger, txn, ns_registry, options).await;
    }

    // 5. Stage with appropriate validation mode
    match validation_mode {
        ValidationMode::Warn => {
            // Clone graph_delta before stage_txn consumes the txn — needed to
            // rebuild graph_sids for per-graph SHACL validation.
            let graph_delta = txn.graph_delta.clone();
            let (view, mut ns_registry) = stage_txn(ledger, txn, ns_registry, options).await?;
            if !shacl_cache.is_empty() {
                // Rebuild graph_sids from cloned graph_delta + ns_registry.
                // Same pattern as stage_with_shacl() — IRIs were resolved during
                // stage(), so sid_for_iri hits the trie cache (no new allocations).
                let graph_sids: HashMap<GraphId, Sid> = graph_delta
                    .iter()
                    .map(|(&g_id, iri)| (g_id, ns_registry.sid_for_iri(iri)))
                    .collect();
                if let Err(fluree_db_transact::TransactError::ShaclViolation(report)) =
                    validate_view_with_shacl(&view, shacl_cache, Some(&graph_sids)).await
                {
                    tracing::warn!(
                        report = %report,
                        "SHACL violations (config mode=Warn, continuing)"
                    );
                }
            }
            Ok((view, ns_registry))
        }
        ValidationMode::Reject => {
            stage_with_shacl(ledger, txn, ns_registry, options, shacl_cache).await
        }
    }
}

// =============================================================================
// Config-driven unique constraint enforcement
// =============================================================================

/// Run uniqueness enforcement after staging if configured.
///
/// Loads config from the pre-txn state (via `view.base()`) and checks
/// staged flakes against `f:enforceUnique` annotations. Zero-cost when
/// no `f:transactDefaults` / `f:uniqueEnabled` is configured.
async fn enforce_unique_after_staging(
    view: &LedgerView,
    graph_delta: &FxHashMap<u16, String>,
) -> std::result::Result<(), fluree_db_transact::TransactError> {
    let config = load_transaction_config(view.base()).await;
    if let Some(cfg) = &config {
        let per_graph_unique = resolve_per_graph_unique_sids(view, cfg, graph_delta).await?;
        enforce_unique_constraints(view, &per_graph_unique, graph_delta).await?;
    }
    Ok(())
}

/// Resolve per-graph unique property SIDs from `f:enforceUnique` annotations.
///
/// For each graph affected by staged flakes, resolves the effective transact
/// config, loads constraint annotations from the configured source graphs,
/// and returns a map of graph_id → set of property SIDs that must be unique.
///
/// Returns an empty map when no uniqueness constraints are configured (fast path).
async fn resolve_per_graph_unique_sids(
    view: &LedgerView,
    config: &LedgerConfig,
    graph_delta: &FxHashMap<u16, String>,
) -> std::result::Result<HashMap<GraphId, FxHashSet<Sid>>, fluree_db_transact::TransactError> {
    let snapshot = view.db();

    // Build reverse map: graph SID → g_id for flake graph resolution
    let mut sid_to_gid: HashMap<Sid, GraphId> = HashMap::new();
    for (&g_id, iri) in graph_delta {
        if let Some(sid) = snapshot.encode_iri(iri) {
            sid_to_gid.insert(sid, g_id);
        }
    }
    // Also include pre-existing named graphs from the registry
    for (g_id, iri) in snapshot.graph_registry.iter_entries() {
        if let Some(sid) = snapshot.encode_iri(iri) {
            sid_to_gid.entry(sid).or_insert(g_id);
        }
    }

    // Derive affected graph IDs from staged flakes (not graph_delta)
    let mut affected_g_ids: FxHashSet<GraphId> = FxHashSet::default();
    for flake in view.staged_flakes() {
        if !flake.op {
            continue;
        }
        let g_id = match &flake.g {
            None => 0u16,
            Some(g_sid) => sid_to_gid.get(g_sid).copied().unwrap_or_else(|| {
                tracing::debug!(
                    ?g_sid,
                    "Staged flake with unknown graph SID — treating as default"
                );
                0
            }),
        };
        affected_g_ids.insert(g_id);
    }

    let mut per_graph: HashMap<GraphId, FxHashSet<Sid>> = HashMap::new();

    for &g_id in &affected_g_ids {
        // Resolve graph IRI for per-graph config lookup
        let graph_iri = if g_id == 0 {
            None
        } else {
            graph_delta
                .get(&g_id)
                .map(|s| s.as_str())
                .or_else(|| snapshot.graph_registry.iri_for_graph_id(g_id))
        };

        let resolved = config_resolver::resolve_effective_config(config, graph_iri);
        let transact_config = match config_resolver::merge_transact_opts(&resolved) {
            Some(tc) => tc,
            None => continue,
        };

        // Resolve constraint source graph IDs
        let source_g_ids = if transact_config.constraints_sources.is_empty() {
            // Default: annotations in the default graph (g_id=0)
            vec![0u16]
        } else {
            resolve_constraint_source_g_ids(&transact_config.constraints_sources, snapshot)
        };

        // Load f:enforceUnique annotations from each source graph
        let mut unique_sids = FxHashSet::default();
        for source_g_id in source_g_ids {
            let annotations = read_enforce_unique_from_graph(view, source_g_id).await?;
            unique_sids.extend(annotations);
        }

        if !unique_sids.is_empty() {
            per_graph.insert(g_id, unique_sids);
        }
    }

    Ok(per_graph)
}

/// Resolve `GraphSourceRef` list to graph IDs.
///
/// Maps each `f:graphSelector` IRI to a concrete graph ID:
/// - `f:defaultGraph` → 0
/// - Named graph IRI → lookup in `GraphRegistry`
fn resolve_constraint_source_g_ids(
    sources: &[fluree_db_core::ledger_config::GraphSourceRef],
    snapshot: &fluree_db_core::LedgerSnapshot,
) -> Vec<GraphId> {
    let mut g_ids = Vec::new();
    for source in sources {
        let g_id = match source.graph_selector.as_deref() {
            Some(iri) if iri == config_iris::DEFAULT_GRAPH => Some(0u16),
            Some(iri) => snapshot.graph_registry.graph_id_for_iri(iri),
            None => Some(0u16), // no selector → default graph
        };
        if let Some(id) = g_id {
            g_ids.push(id);
        } else {
            tracing::debug!(
                selector = ?source.graph_selector,
                "Constraint source graph not found in registry — skipping"
            );
        }
    }
    g_ids
}

/// Read `f:enforceUnique true` annotations from a single graph.
///
/// Queries the POST index at the pre-transaction state for all subjects
/// where `?prop f:enforceUnique true`. Returns the set of property SIDs.
async fn read_enforce_unique_from_graph(
    view: &LedgerView,
    source_g_id: GraphId,
) -> std::result::Result<Vec<Sid>, fluree_db_transact::TransactError> {
    let snapshot = view.db();

    let enforce_unique_sid = match snapshot.encode_iri(config_iris::ENFORCE_UNIQUE) {
        Some(sid) => sid,
        None => return Ok(Vec::new()),
    };

    let xsd_boolean_sid = match snapshot.encode_iri("http://www.w3.org/2001/XMLSchema#boolean") {
        Some(sid) => sid,
        None => return Ok(Vec::new()),
    };

    // Query: all flakes where p=f:enforceUnique, o=true, dt=xsd:boolean
    // Uses pre-txn state (base novelty) for lagging annotation semantics.
    let base = view.base();
    let match_val = RangeMatch::predicate_object(enforce_unique_sid, FlakeValue::Boolean(true))
        .with_datatype(xsd_boolean_sid);

    let flakes = range_with_overlay(
        &base.snapshot,
        source_g_id,
        &*base.novelty,
        IndexType::Post,
        RangeTest::Eq,
        match_val,
        RangeOptions::new().with_to_t(base.t()),
    )
    .await
    .map_err(fluree_db_transact::TransactError::from)?;

    // Each matching flake's subject is a property IRI that has f:enforceUnique true
    let props: Vec<Sid> = flakes.iter().map(|f| f.s.clone()).collect();
    Ok(props)
}

/// Enforce unique constraints on staged flakes.
///
/// For each affected graph, checks that no two subjects hold the same value
/// for any property marked `f:enforceUnique`. Uses the POST index with
/// datatype-aware matching and stale-removal (last-op-wins).
///
/// Returns `Ok(())` if no violations, or a `UniqueConstraintViolation` error.
async fn enforce_unique_constraints(
    view: &LedgerView,
    per_graph_unique: &HashMap<GraphId, FxHashSet<Sid>>,
    graph_delta: &FxHashMap<u16, String>,
) -> std::result::Result<(), fluree_db_transact::TransactError> {
    // Fast path: nothing configured
    if per_graph_unique.is_empty() {
        return Ok(());
    }

    let snapshot = view.db();

    // Build reverse map for flake graph resolution
    let mut sid_to_gid: HashMap<Sid, GraphId> = HashMap::new();
    for (&g_id, iri) in graph_delta {
        if let Some(sid) = snapshot.encode_iri(iri) {
            sid_to_gid.insert(sid, g_id);
        }
    }
    for (g_id, iri) in snapshot.graph_registry.iter_entries() {
        if let Some(sid) = snapshot.encode_iri(iri) {
            sid_to_gid.entry(sid).or_insert(g_id);
        }
    }

    // Collect distinct (g_id, p, o) keys from staged asserts on unique properties.
    // Uniqueness ignores datatype and language tag — the key is the storage-layer
    // value identity (FlakeValue), not the RDF datatype IRI.
    let mut keys_to_check: FxHashSet<(GraphId, Sid, FlakeValue)> = FxHashSet::default();
    for flake in view.staged_flakes() {
        if !flake.op {
            continue;
        }
        let g_id = match &flake.g {
            None => 0u16,
            Some(g_sid) => sid_to_gid.get(g_sid).copied().unwrap_or(0),
        };
        if let Some(unique_set) = per_graph_unique.get(&g_id) {
            if unique_set.contains(&flake.p) {
                keys_to_check.insert((g_id, flake.p.clone(), flake.o.clone()));
            }
        }
    }

    // For each unique key, query POST index to check for multiple active subjects.
    // No .with_datatype() — matches all datatypes for the same (p, o) value.
    for (g_id, p, o) in &keys_to_check {
        let match_val = RangeMatch::predicate_object(p.clone(), o.clone());

        // Query with post-staging overlay: sees committed + staged data.
        // Stale removal ensures only currently-active assertions are returned.
        let flakes = range_with_overlay(
            snapshot,
            *g_id,
            view,
            IndexType::Post,
            RangeTest::Eq,
            match_val,
            RangeOptions::new().with_to_t(view.staged_t()),
        )
        .await
        .map_err(fluree_db_transact::TransactError::from)?;

        // Count distinct subjects with active assertions
        let mut seen_subjects: FxHashSet<&Sid> = FxHashSet::default();
        for f in &flakes {
            seen_subjects.insert(&f.s);
        }

        if seen_subjects.len() > 1 {
            // Build a descriptive error with decoded IRIs
            let property_iri = snapshot.decode_sid(p).unwrap_or_else(|| format!("{:?}", p));
            let graph_label = if *g_id == 0 {
                "default".to_string()
            } else {
                graph_delta
                    .get(g_id)
                    .cloned()
                    .or_else(|| {
                        snapshot
                            .graph_registry
                            .iri_for_graph_id(*g_id)
                            .map(|s| s.to_string())
                    })
                    .unwrap_or_else(|| format!("g_id={}", g_id))
            };
            let value_str = format!("{:?}", o);

            // Pick two subjects for the error message
            let mut subj_iter = seen_subjects.iter();
            let existing = subj_iter.next().unwrap();
            let conflicting = subj_iter.next().unwrap();
            let existing_iri = snapshot
                .decode_sid(existing)
                .unwrap_or_else(|| format!("{:?}", existing));
            let new_iri = snapshot
                .decode_sid(conflicting)
                .unwrap_or_else(|| format!("{:?}", conflicting));

            return Err(
                fluree_db_transact::TransactError::UniqueConstraintViolation {
                    property: property_iri,
                    value: value_str,
                    graph: graph_label,
                    existing_subject: existing_iri,
                    new_subject: new_iri,
                },
            );
        }
    }

    Ok(())
}

// =============================================================================
// Indexing Mode Configuration
// =============================================================================

/// Indexing mode configuration (server-level setting)
///
/// Controls whether transactions trigger background indexing or return hints
/// for an external indexer (e.g., Lambda).
#[derive(Debug, Clone, Default)]
pub enum IndexingMode {
    /// Disabled mode (Lambda/external indexer)
    ///
    /// Transactions complete without triggering indexing. The `IndexingStatus`
    /// in the result provides hints for external indexers.
    #[default]
    Disabled,
    /// Background mode with coalescing handle
    ///
    /// Triggers background indexing when `indexing_needed` is true.
    /// Uses a depth-1 coalescing queue (latest wins per ledger).
    Background(IndexerHandle),
}

impl IndexingMode {
    /// Returns true if background indexing is enabled
    pub fn is_enabled(&self) -> bool {
        matches!(self, IndexingMode::Background(_))
    }

    /// Returns the indexer handle if in background mode
    pub fn handle(&self) -> Option<&IndexerHandle> {
        match self {
            IndexingMode::Disabled => None,
            IndexingMode::Background(h) => Some(h),
        }
    }
}

/// Indexing status after a transaction
///
/// Provides hints for external indexers (in disabled mode) and confirms
/// indexing was triggered (in background mode).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexingStatus {
    /// True if indexing is enabled (background mode)
    pub enabled: bool,
    /// True if novelty is above `reindex_min_bytes` after commit
    pub needed: bool,
    /// Current novelty size in bytes
    pub novelty_size: usize,
    /// Transaction time of the indexed state
    pub index_t: i64,
    /// Transaction time after this commit
    pub commit_t: i64,
}

/// Result of a committed transaction
pub struct TransactResult {
    pub receipt: CommitReceipt,
    pub ledger: LedgerState,
    /// Indexing status and hints
    pub indexing: IndexingStatus,
}

impl std::fmt::Debug for TransactResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactResult")
            .field("receipt", &self.receipt)
            .field("indexing", &self.indexing)
            .finish_non_exhaustive()
    }
}

/// Result of a committed transaction via reference (cache already updated)
///
/// Unlike `TransactResult`, this does not contain the ledger state because
/// the LedgerHandle's internal state has already been updated in place.
#[derive(Debug)]
pub struct TransactResultRef {
    pub receipt: CommitReceipt,
    /// Indexing status and hints
    pub indexing: IndexingStatus,
}

/// Result of staging a transaction
pub struct StageResult {
    pub view: LedgerView,
    pub ns_registry: NamespaceRegistry,
    /// User-provided transaction metadata (extracted from envelope-form JSON-LD)
    pub txn_meta: Vec<TxnMetaEntry>,
    /// Named graph IRI to g_id mappings introduced by this transaction
    pub graph_delta: rustc_hash::FxHashMap<u16, String>,
}

/// Convert named graph blocks to TripleTemplates with proper graph_id assignments.
///
/// Returns a tuple of (templates, graph_delta) where:
/// - templates: Vec<TripleTemplate> with graph_id set for each template
/// - graph_delta: HashMap<u16, String> mapping g_id to graph IRI
///
/// Graph IDs are assigned starting at 2 (0=default, 1=txn-meta).
fn convert_named_graphs_to_templates(
    named_graphs: &[NamedGraphBlock],
    ns_registry: &mut NamespaceRegistry,
) -> Result<(Vec<TripleTemplate>, rustc_hash::FxHashMap<u16, String>)> {
    use fluree_db_transact::{RawObject, RawTerm};

    let mut templates = Vec::new();
    let mut graph_delta: rustc_hash::FxHashMap<u16, String> = rustc_hash::FxHashMap::default();
    let mut iri_to_id: std::collections::HashMap<String, u16> = std::collections::HashMap::new();
    let mut next_graph_id: u16 = 3; // 0=default, 1=txn-meta, 2=config

    // Helper to expand prefixed name to full IRI
    fn expand_prefixed_name(
        prefix: &str,
        local: &str,
        prefixes: &rustc_hash::FxHashMap<String, String>,
    ) -> Result<String> {
        prefixes
            .get(prefix)
            .map(|ns| format!("{}{}", ns, local))
            .ok_or_else(|| ApiError::query(format!("undefined prefix: {}", prefix)))
    }

    // Helper to convert RawTerm to TemplateTerm
    fn convert_term(
        term: &RawTerm,
        prefixes: &rustc_hash::FxHashMap<String, String>,
        ns_registry: &mut NamespaceRegistry,
    ) -> Result<TemplateTerm> {
        match term {
            RawTerm::Iri(iri) => {
                if let Some(local) = iri.strip_prefix("_:") {
                    Ok(TemplateTerm::BlankNode(local.to_string()))
                } else {
                    Ok(TemplateTerm::Sid(ns_registry.sid_for_iri(iri)))
                }
            }
            RawTerm::PrefixedName { prefix, local } => {
                let iri = expand_prefixed_name(prefix, local, prefixes)?;
                Ok(TemplateTerm::Sid(ns_registry.sid_for_iri(&iri)))
            }
        }
    }

    // Helper to convert RawObject to TemplateTerm and optional datatype/language
    fn convert_object(
        obj: &RawObject,
        prefixes: &rustc_hash::FxHashMap<String, String>,
        ns_registry: &mut NamespaceRegistry,
    ) -> Result<(TemplateTerm, Option<fluree_db_core::Sid>, Option<String>)> {
        use fluree_db_core::FlakeValue;
        match obj {
            RawObject::Iri(iri) => {
                if let Some(local) = iri.strip_prefix("_:") {
                    Ok((TemplateTerm::BlankNode(local.to_string()), None, None))
                } else {
                    Ok((TemplateTerm::Sid(ns_registry.sid_for_iri(iri)), None, None))
                }
            }
            RawObject::PrefixedName { prefix, local } => {
                let iri = expand_prefixed_name(prefix, local, prefixes)?;
                Ok((TemplateTerm::Sid(ns_registry.sid_for_iri(&iri)), None, None))
            }
            RawObject::String(s) => Ok((
                TemplateTerm::Value(FlakeValue::String(s.clone())),
                None,
                None,
            )),
            RawObject::Integer(n) => Ok((TemplateTerm::Value(FlakeValue::Long(*n)), None, None)),
            RawObject::Double(n) => Ok((TemplateTerm::Value(FlakeValue::Double(*n)), None, None)),
            RawObject::Boolean(b) => Ok((TemplateTerm::Value(FlakeValue::Boolean(*b)), None, None)),
            RawObject::LangString { value, lang } => Ok((
                TemplateTerm::Value(FlakeValue::String(value.clone())),
                None,
                Some(lang.clone()),
            )),
            RawObject::TypedLiteral { value, datatype } => {
                let dt_sid = ns_registry.sid_for_iri(datatype);
                Ok((
                    TemplateTerm::Value(FlakeValue::String(value.clone())),
                    Some(dt_sid),
                    None,
                ))
            }
        }
    }

    for block in named_graphs {
        // Assign a graph_id to this graph IRI (or reuse existing)
        let g_id = *iri_to_id.entry(block.iri.clone()).or_insert_with(|| {
            let id = next_graph_id;
            graph_delta.insert(id, block.iri.clone());
            next_graph_id += 1;
            id
        });

        // Convert each triple in this graph block
        for triple in &block.triples {
            let subject = triple
                .subject
                .as_ref()
                .ok_or_else(|| ApiError::query("named graph triple missing subject"))?;
            let subject_term = convert_term(subject, &block.prefixes, ns_registry)?;
            let predicate_term = convert_term(&triple.predicate, &block.prefixes, ns_registry)?;

            for obj in &triple.objects {
                let (object_term, datatype, language) =
                    convert_object(obj, &block.prefixes, ns_registry)?;
                let mut template =
                    TripleTemplate::new(subject_term.clone(), predicate_term.clone(), object_term);
                template = template.with_graph_id(g_id);
                if let Some(dt) = datatype {
                    template = template.with_datatype(dt);
                }
                if let Some(lang) = language {
                    template = template.with_language(lang);
                }
                templates.push(template);
            }
        }
    }

    Ok((templates, graph_delta))
}

impl<S, N> crate::Fluree<S, N>
where
    S: Storage + ContentAddressedWrite + Clone + 'static,
    N: NameService + Publisher,
{
    /// Stage a transaction against a ledger (no persistence).
    ///
    /// Respects `opts.max-fuel` in the transaction JSON for fuel limits (consistent with query behavior).
    pub async fn stage_transaction(
        &self,
        ledger: LedgerState,
        txn_type: TxnType,
        txn_json: &JsonValue,
        txn_opts: TxnOpts,
        index_config: Option<&IndexConfig>,
    ) -> Result<StageResult> {
        self.stage_transaction_with_trig_meta(
            ledger,
            txn_type,
            txn_json,
            txn_opts,
            index_config,
            None,
        )
        .await
    }

    /// Stage a transaction with optional TriG transaction metadata.
    ///
    /// This is the internal implementation that handles both JSON-LD and TriG inputs.
    /// For TriG inputs, the `trig_meta` parameter contains pre-parsed metadata that
    /// will be resolved and merged into the transaction's txn_meta.
    pub async fn stage_transaction_with_trig_meta(
        &self,
        ledger: LedgerState,
        txn_type: TxnType,
        txn_json: &JsonValue,
        txn_opts: TxnOpts,
        index_config: Option<&IndexConfig>,
        trig_meta: Option<&RawTrigMeta>,
    ) -> Result<StageResult> {
        self.stage_transaction_with_named_graphs(
            ledger,
            txn_type,
            txn_json,
            txn_opts,
            index_config,
            trig_meta,
            &[],
        )
        .await
    }

    /// Stage a transaction with optional TriG transaction metadata and named graphs.
    ///
    /// This is the full implementation that handles:
    /// - JSON-LD transactions (default graph)
    /// - TriG txn-meta (commit metadata)
    /// - TriG named graphs (user-defined graphs with separate g_id)
    #[allow(clippy::too_many_arguments)]
    pub async fn stage_transaction_with_named_graphs(
        &self,
        ledger: LedgerState,
        txn_type: TxnType,
        txn_json: &JsonValue,
        txn_opts: TxnOpts,
        index_config: Option<&IndexConfig>,
        trig_meta: Option<&RawTrigMeta>,
        named_graphs: &[NamedGraphBlock],
    ) -> Result<StageResult> {
        let mut ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);

        // Handle case where default graph is empty but named graphs are present
        // (e.g., TriG with only GRAPH blocks and no default graph triples)
        let mut txn = if is_empty_default_graph(txn_json) && !named_graphs.is_empty() {
            // Create empty transaction of the appropriate type
            match txn_type {
                TxnType::Insert => Txn::insert().with_opts(txn_opts),
                TxnType::Upsert => Txn::upsert().with_opts(txn_opts),
                TxnType::Update => Txn::update().with_opts(txn_opts),
            }
        } else {
            let parse_span = tracing::debug_span!("txn_parse", txn_type = ?txn_type);
            let _guard = parse_span.enter();
            parse_transaction(txn_json, txn_type, txn_opts, &mut ns_registry)?
        };

        // If TriG metadata was extracted, resolve it and merge into txn_meta
        if let Some(raw_meta) = trig_meta {
            let resolved = resolve_trig_meta(raw_meta, &mut ns_registry)?;
            txn.txn_meta.extend(resolved);
        }

        // Convert named graph blocks to TripleTemplates and merge into the transaction
        if !named_graphs.is_empty() {
            let (named_graph_templates, named_graph_delta) =
                convert_named_graphs_to_templates(named_graphs, &mut ns_registry)?;
            txn.insert_templates.extend(named_graph_templates);
            txn.graph_delta.extend(named_graph_delta);
        }

        // Extract txn_meta and graph_delta before staging consumes the Txn
        let txn_meta = txn.txn_meta.clone();
        let graph_delta = txn.graph_delta.clone();

        // Check for max-fuel in opts and create tracker if present (same pattern as queries)
        let tracker = tracker_for_limits(txn_json);

        let mut options = match index_config {
            Some(cfg) => StageOptions::new().with_index_config(cfg),
            None => StageOptions::default(),
        };
        if tracker.is_enabled() {
            options = options.with_tracker(&tracker);
        }

        #[cfg(feature = "shacl")]
        let (view, ns_registry) =
            stage_with_config_shacl(ledger, txn, ns_registry, options).await?;
        #[cfg(not(feature = "shacl"))]
        let (view, ns_registry) = stage_txn(ledger, txn, ns_registry, options).await?;

        // Enforce uniqueness constraints (independent of shacl feature)
        enforce_unique_after_staging(&view, &graph_delta).await?;

        Ok(StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        })
    }

    /// Stage a pre-built transaction IR (bypasses JSON/Turtle parsing).
    ///
    /// This is used for SPARQL UPDATE where the transaction has already been
    /// lowered to the IR representation.
    pub async fn stage_transaction_from_txn(
        &self,
        ledger: LedgerState,
        txn: fluree_db_transact::Txn,
        index_config: Option<&IndexConfig>,
    ) -> Result<StageResult> {
        let ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);

        // Extract txn_meta and graph_delta before staging consumes the Txn
        let txn_meta = txn.txn_meta.clone();
        let graph_delta = txn.graph_delta.clone();

        let options = match index_config {
            Some(cfg) => StageOptions::new().with_index_config(cfg),
            None => StageOptions::default(),
        };

        #[cfg(feature = "shacl")]
        let (view, ns_registry) =
            stage_with_config_shacl(ledger, txn, ns_registry, options).await?;
        #[cfg(not(feature = "shacl"))]
        let (view, ns_registry) = stage_txn(ledger, txn, ns_registry, options).await?;

        // Enforce uniqueness constraints (independent of shacl feature)
        enforce_unique_after_staging(&view, &graph_delta).await?;

        Ok(StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        })
    }

    /// Stage a transaction with policy enforcement + tracking (opts.meta / opts.max-fuel).
    ///
    /// This is the transaction-side equivalent of `query_connection_tracked_with_policy`.
    pub(crate) async fn stage_transaction_tracked_with_policy(
        &self,
        ledger: LedgerState,
        input: TrackedTransactionInput<'_>,
        index_config: Option<&IndexConfig>,
        tracker: &Tracker,
    ) -> std::result::Result<StageResult, TrackedErrorResponse> {
        let mut ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let txn = {
            let parse_span = tracing::debug_span!("txn_parse", txn_type = ?input.txn_type);
            let _guard = parse_span.enter();
            parse_transaction(
                input.txn_json,
                input.txn_type,
                input.txn_opts,
                &mut ns_registry,
            )
            .map_err(|e| TrackedErrorResponse::new(400, e.to_string(), tracker.tally()))?
        };

        // Extract txn_meta and graph_delta before staging consumes the Txn
        let txn_meta = txn.txn_meta.clone();
        let graph_delta = txn.graph_delta.clone();

        // Build stage options with policy and tracker
        let mut options = StageOptions::new()
            .with_policy(input.policy)
            .with_tracker(tracker);
        if let Some(cfg) = index_config {
            options = options.with_index_config(cfg);
        }

        #[cfg(feature = "shacl")]
        let (view, ns_registry) = stage_with_config_shacl(ledger, txn, ns_registry, options)
            .await
            .map_err(|e| TrackedErrorResponse::new(400, e.to_string(), tracker.tally()))?;
        #[cfg(not(feature = "shacl"))]
        let (view, ns_registry) = stage_txn(ledger, txn, ns_registry, options)
            .await
            .map_err(|e| TrackedErrorResponse::new(400, e.to_string(), tracker.tally()))?;

        // Enforce uniqueness constraints (independent of shacl feature)
        enforce_unique_after_staging(&view, &graph_delta)
            .await
            .map_err(|e| TrackedErrorResponse::new(400, e.to_string(), tracker.tally()))?;

        Ok(StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        })
    }

    /// Convenience: stage + commit + tracking + policy.
    ///
    /// Returns `(TransactResult, TrackingTally?)` on success, or `TrackedErrorResponse` on error.
    pub async fn transact_tracked_with_policy(
        &self,
        ledger: LedgerState,
        input: TrackedTransactionInput<'_>,
        commit_opts: CommitOpts,
        index_config: &IndexConfig,
    ) -> std::result::Result<(TransactResult, Option<TrackingTally>), TrackedErrorResponse> {
        let store_raw_txn = input.txn_opts.store_raw_txn.unwrap_or(false);
        let txn_json_for_commit = input.txn_json.clone();

        let opts = input.txn_json.as_object().and_then(|o| o.get("opts"));
        let tracker = Tracker::new(TrackingOptions::from_opts_value(opts));

        let StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        } = self
            .stage_transaction_tracked_with_policy(ledger, input, Some(index_config), &tracker)
            .await?;

        // Store raw transaction JSON ONLY when explicitly opted-in, or when already provided
        // (e.g., signed credential envelope for provenance).
        let commit_opts = if commit_opts.raw_txn.is_none() && store_raw_txn {
            commit_opts.with_raw_txn(txn_json_for_commit)
        } else {
            commit_opts
        };

        // Add extracted transaction metadata and graph delta to commit opts
        let commit_opts = commit_opts
            .with_txn_meta(txn_meta)
            .with_graph_delta(graph_delta.into_iter().collect());

        // Commit (no-op updates handled by existing transact; for the tracked path we just mirror it).
        let (receipt, ledger) = self
            .commit_staged(view, ns_registry, index_config, commit_opts)
            .await
            .map_err(|e| TrackedErrorResponse::new(500, e.to_string(), tracker.tally()))?;

        // Compute indexing status AFTER publish_commit succeeds
        let indexing_enabled = self.indexing_mode.is_enabled() && self.defaults_indexing_enabled();
        let indexing_needed = ledger.should_reindex(index_config);
        let indexing_status = IndexingStatus {
            enabled: indexing_enabled,
            needed: indexing_needed,
            novelty_size: ledger.novelty_size(),
            index_t: ledger.index_t(),
            commit_t: receipt.t,
        };

        if let IndexingMode::Background(handle) = &self.indexing_mode {
            if indexing_enabled && indexing_needed {
                handle.trigger(ledger.ledger_id(), receipt.t).await;
            }
        }

        Ok((
            TransactResult {
                receipt,
                ledger,
                indexing: indexing_status,
            },
            tracker.tally(),
        ))
    }

    /// Commit a staged transaction (persists commit record + publishes nameservice head).
    pub async fn commit_staged(
        &self,
        view: LedgerView,
        ns_registry: NamespaceRegistry,
        index_config: &IndexConfig,
        commit_opts: CommitOpts,
    ) -> Result<(CommitReceipt, LedgerState)> {
        let (receipt, ledger) = commit_txn(
            view,
            ns_registry,
            self.connection.storage(),
            &self.nameservice,
            index_config,
            commit_opts,
        )
        .await?;
        Ok((receipt, ledger))
    }

    /// Convenience: stage + commit.
    ///
    /// After a successful commit (including nameservice publish), this method:
    /// 1. Computes `IndexingStatus` with hints for external indexers
    /// 2. If `indexing_mode` is `Background` and `indexing_needed`, triggers indexing
    pub async fn transact(
        &self,
        ledger: LedgerState,
        txn_type: TxnType,
        txn_json: &JsonValue,
        txn_opts: TxnOpts,
        commit_opts: CommitOpts,
        index_config: &IndexConfig,
    ) -> Result<TransactResult> {
        let store_raw_txn = txn_opts.store_raw_txn.unwrap_or(false);

        let StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        } = self
            .stage_transaction(ledger, txn_type, txn_json, txn_opts, Some(index_config))
            .await?;

        // Store raw transaction JSON ONLY when explicitly opted-in, or when already provided
        // (e.g., signed credential envelope for provenance).
        let commit_opts = if commit_opts.raw_txn.is_none() && store_raw_txn {
            commit_opts.with_raw_txn(txn_json.clone())
        } else {
            commit_opts
        };

        // Add extracted transaction metadata and graph delta to commit opts
        let commit_opts = commit_opts
            .with_txn_meta(txn_meta)
            .with_graph_delta(graph_delta.into_iter().collect());

        // No-op updates: if WHERE matches nothing (or templates produce no flakes),
        // return success without committing (Clojure parity).
        //
        // This allows patterns like "delete if exists, then insert" to execute safely when
        // there are no matches, and supports conditional updates.
        let (receipt, ledger) =
            if !view.has_staged() && matches!(txn_type, TxnType::Update | TxnType::Upsert) {
                let (base, flakes) = view.into_parts();
                debug_assert!(
                    flakes.is_empty(),
                    "no-op transaction path requires zero staged flakes"
                );
                (
                    CommitReceipt {
                        commit_id: ContentId::new(ContentKind::Commit, &[]),
                        t: base.t(),
                        flake_count: 0,
                    },
                    base,
                )
            } else {
                self.commit_staged(view, ns_registry, index_config, commit_opts)
                    .await?
            };

        // Compute indexing status AFTER publish_commit succeeds
        let indexing_enabled = self.indexing_mode.is_enabled() && self.defaults_indexing_enabled();
        let indexing_needed = ledger.should_reindex(index_config);

        let indexing_status = IndexingStatus {
            enabled: indexing_enabled,
            needed: indexing_needed,
            novelty_size: ledger.novelty_size(),
            index_t: ledger.index_t(),
            commit_t: receipt.t,
        };

        // Trigger indexing AFTER publish_commit succeeds (fast operation)
        if let IndexingMode::Background(handle) = &self.indexing_mode {
            if indexing_enabled && indexing_needed {
                handle.trigger(ledger.ledger_id(), receipt.t).await;
            }
        }

        Ok(TransactResult {
            receipt,
            ledger,
            indexing: indexing_status,
        })
    }

    /// Execute a transaction with optional TriG metadata.
    ///
    /// This is similar to `transact` but accepts pre-extracted TriG metadata
    /// from Turtle inputs that had GRAPH blocks.
    #[allow(clippy::too_many_arguments)]
    pub async fn transact_with_trig_meta(
        &self,
        ledger: LedgerState,
        txn_type: TxnType,
        txn_json: &JsonValue,
        txn_opts: TxnOpts,
        commit_opts: CommitOpts,
        index_config: &IndexConfig,
        trig_meta: Option<&RawTrigMeta>,
    ) -> Result<TransactResult> {
        let store_raw_txn = txn_opts.store_raw_txn.unwrap_or(false);

        let StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        } = self
            .stage_transaction_with_trig_meta(
                ledger,
                txn_type,
                txn_json,
                txn_opts,
                Some(index_config),
                trig_meta,
            )
            .await?;

        // Store raw transaction JSON ONLY when explicitly opted-in, or when already provided
        // (e.g., signed credential envelope for provenance).
        let commit_opts = if commit_opts.raw_txn.is_none() && store_raw_txn {
            commit_opts.with_raw_txn(txn_json.clone())
        } else {
            commit_opts
        };

        // Add extracted transaction metadata and graph delta to commit opts
        let commit_opts = commit_opts
            .with_txn_meta(txn_meta)
            .with_graph_delta(graph_delta.into_iter().collect());

        // No-op updates: if WHERE matches nothing (or templates produce no flakes),
        // return success without committing (Clojure parity).
        let (receipt, ledger) =
            if !view.has_staged() && matches!(txn_type, TxnType::Update | TxnType::Upsert) {
                let (base, flakes) = view.into_parts();
                debug_assert!(
                    flakes.is_empty(),
                    "no-op transaction path requires zero staged flakes"
                );
                (
                    CommitReceipt {
                        commit_id: ContentId::new(ContentKind::Commit, &[]),
                        t: base.t(),
                        flake_count: 0,
                    },
                    base,
                )
            } else {
                self.commit_staged(view, ns_registry, index_config, commit_opts)
                    .await?
            };

        // Compute indexing status AFTER publish_commit succeeds
        let indexing_enabled = self.indexing_mode.is_enabled() && self.defaults_indexing_enabled();
        let indexing_needed = ledger.should_reindex(index_config);

        let indexing_status = IndexingStatus {
            enabled: indexing_enabled,
            needed: indexing_needed,
            novelty_size: ledger.novelty_size(),
            index_t: ledger.index_t(),
            commit_t: receipt.t,
        };

        // Trigger indexing AFTER publish_commit succeeds (fast operation)
        if let IndexingMode::Background(handle) = &self.indexing_mode {
            if indexing_enabled && indexing_needed {
                handle.trigger(ledger.ledger_id(), receipt.t).await;
            }
        }

        Ok(TransactResult {
            receipt,
            ledger,
            indexing: indexing_status,
        })
    }

    /// Execute a transaction with optional TriG metadata and named graphs.
    ///
    /// This is the full implementation that handles:
    /// - JSON-LD transactions (default graph)
    /// - TriG txn-meta (commit metadata)
    /// - TriG named graphs (user-defined graphs with separate g_id)
    #[allow(clippy::too_many_arguments)]
    pub async fn transact_with_named_graphs(
        &self,
        ledger: LedgerState,
        txn_type: TxnType,
        txn_json: &JsonValue,
        txn_opts: TxnOpts,
        commit_opts: CommitOpts,
        index_config: &IndexConfig,
        trig_meta: Option<&RawTrigMeta>,
        named_graphs: &[NamedGraphBlock],
    ) -> Result<TransactResult> {
        let store_raw_txn = txn_opts.store_raw_txn.unwrap_or(false);

        let StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        } = self
            .stage_transaction_with_named_graphs(
                ledger,
                txn_type,
                txn_json,
                txn_opts,
                Some(index_config),
                trig_meta,
                named_graphs,
            )
            .await?;

        // Store raw transaction JSON ONLY when explicitly opted-in, or when already provided
        // (e.g., signed credential envelope for provenance).
        let commit_opts = if commit_opts.raw_txn.is_none() && store_raw_txn {
            commit_opts.with_raw_txn(txn_json.clone())
        } else {
            commit_opts
        };

        // Add extracted transaction metadata and graph delta to commit opts
        let commit_opts = commit_opts
            .with_txn_meta(txn_meta)
            .with_graph_delta(graph_delta.into_iter().collect());

        // No-op updates: if WHERE matches nothing (or templates produce no flakes),
        // return success without committing (Clojure parity).
        let (receipt, ledger) =
            if !view.has_staged() && matches!(txn_type, TxnType::Update | TxnType::Upsert) {
                let (base, flakes) = view.into_parts();
                debug_assert!(
                    flakes.is_empty(),
                    "no-op transaction path requires zero staged flakes"
                );
                (
                    CommitReceipt {
                        commit_id: ContentId::new(ContentKind::Commit, &[]),
                        t: base.t(),
                        flake_count: 0,
                    },
                    base,
                )
            } else {
                self.commit_staged(view, ns_registry, index_config, commit_opts)
                    .await?
            };

        // Compute indexing status AFTER publish_commit succeeds
        let indexing_enabled = self.indexing_mode.is_enabled() && self.defaults_indexing_enabled();
        let indexing_needed = ledger.should_reindex(index_config);

        let indexing_status = IndexingStatus {
            enabled: indexing_enabled,
            needed: indexing_needed,
            novelty_size: ledger.novelty_size(),
            index_t: ledger.index_t(),
            commit_t: receipt.t,
        };

        // Trigger indexing AFTER publish_commit succeeds (fast operation)
        if let IndexingMode::Background(handle) = &self.indexing_mode {
            if indexing_enabled && indexing_needed {
                handle.trigger(ledger.ledger_id(), receipt.t).await;
            }
        }

        Ok(TransactResult {
            receipt,
            ledger,
            indexing: indexing_status,
        })
    }

    /// Insert new data into the ledger
    ///
    /// Fails if any subject with a concrete `@id` already has triples in the ledger.
    /// Blank nodes are always allowed (they generate fresh IDs).
    ///
    /// # Arguments
    ///
    /// * `ledger` - The ledger state (consumed)
    /// * `data` - JSON-LD data to insert
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = fluree.insert(ledger, json!({
    ///     "@context": {"ex": "http://example.org/"},
    ///     "@id": "ex:alice",
    ///     "ex:name": "Alice",
    ///     "ex:age": 30
    /// })).await?;
    /// ```
    pub async fn insert(&self, ledger: LedgerState, data: &JsonValue) -> Result<TransactResult> {
        let index_config = self.default_index_config();
        self.transact(
            ledger,
            TxnType::Insert,
            data,
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
        )
        .await
    }

    /// Insert new data from Turtle format (direct flake path).
    ///
    /// Parses Turtle directly into assertion flakes, bypassing the
    /// JSON-LD / Txn IR intermediate representations.
    ///
    /// # Arguments
    ///
    /// * `ledger` - The ledger state (consumed)
    /// * `turtle` - Turtle (TTL) format data
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = fluree.insert_turtle(ledger, r#"
    ///     @prefix ex: <http://example.org/> .
    ///     ex:alice ex:name "Alice" ;
    ///              ex:age 30 .
    /// "#).await?;
    /// ```
    pub async fn insert_turtle(&self, ledger: LedgerState, turtle: &str) -> Result<TransactResult> {
        let index_config = self.default_index_config();
        self.insert_turtle_with_opts(
            ledger,
            turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
        )
        .await
    }

    /// Insert new data from Turtle format with options (direct flake path).
    ///
    /// Same as `insert_turtle` but allows custom transaction and commit options.
    /// Prefer using the builder API: `fluree.transact(ledger).insert_turtle(ttl).txn_opts(...).execute()`.
    #[doc(hidden)]
    pub async fn insert_turtle_with_opts(
        &self,
        ledger: LedgerState,
        turtle: &str,
        txn_opts: TxnOpts,
        commit_opts: CommitOpts,
        index_config: &IndexConfig,
    ) -> Result<TransactResult> {
        let store_raw_txn = txn_opts.store_raw_txn.unwrap_or(false);

        let stage_result = self
            .stage_turtle_insert(ledger, turtle, Some(index_config))
            .await?;

        let StageResult {
            view,
            ns_registry,
            txn_meta,
            graph_delta,
        } = stage_result;

        // Store raw Turtle text when explicitly opted-in (same pattern as JSON path)
        let commit_opts = if commit_opts.raw_txn.is_none() && store_raw_txn {
            commit_opts.with_raw_txn(JsonValue::String(turtle.to_string()))
        } else {
            commit_opts
        };

        // Add transaction metadata and graph delta (graph_delta typically empty for Turtle)
        let commit_opts = commit_opts
            .with_txn_meta(txn_meta)
            .with_graph_delta(graph_delta.into_iter().collect());

        let (receipt, ledger) = self
            .commit_staged(view, ns_registry, index_config, commit_opts)
            .await?;

        // Compute indexing status (same logic as transact())
        let indexing_enabled = self.indexing_mode.is_enabled() && self.defaults_indexing_enabled();
        let indexing_needed = ledger.should_reindex(index_config);

        let indexing_status = IndexingStatus {
            enabled: indexing_enabled,
            needed: indexing_needed,
            novelty_size: ledger.novelty_size(),
            index_t: ledger.index_t(),
            commit_t: receipt.t,
        };

        // Trigger indexing AFTER publish_commit succeeds
        if let IndexingMode::Background(handle) = &self.indexing_mode {
            if indexing_enabled && indexing_needed {
                handle.trigger(ledger.ledger_id(), receipt.t).await;
            }
        }

        Ok(TransactResult {
            receipt,
            ledger,
            indexing: indexing_status,
        })
    }

    /// Stage a Turtle INSERT by parsing directly to flakes (bypass JSON-LD / IR).
    ///
    /// This is the fast path for Turtle ingestion. The Turtle is parsed using
    /// `FlakeSink` which converts parser events directly to flakes.
    pub async fn stage_turtle_insert(
        &self,
        ledger: LedgerState,
        turtle: &str,
        index_config: Option<&IndexConfig>,
    ) -> Result<StageResult> {
        use fluree_db_transact::{generate_txn_id, stage_flakes, FlakeSink};

        let span = tracing::debug_span!(
            "stage_turtle_insert",
            ledger_t = ledger.t(),
            new_t = ledger.t() + 1,
            turtle_bytes = turtle.len()
        );
        let _guard = span.enter();

        let mut ns_registry = NamespaceRegistry::from_db(&ledger.snapshot);
        let new_t = ledger.t() + 1;
        let txn_id = generate_txn_id();

        // Parse Turtle directly to flakes
        let parse_span =
            tracing::debug_span!("turtle_parse_to_flakes", turtle_bytes = turtle.len());
        let flakes = {
            let _g = parse_span.enter();
            let mut sink = FlakeSink::new(&mut ns_registry, new_t, txn_id);
            fluree_graph_turtle::parse(turtle, &mut sink)?;
            sink.finish()
        };
        tracing::info!(flake_count = flakes.len(), "turtle parsed to flakes");

        // Stage the flakes (backpressure + optional policy)
        let options = match index_config {
            Some(cfg) => StageOptions::new().with_index_config(cfg),
            None => StageOptions::default(),
        };
        let view = stage_flakes(ledger, flakes, options).await?;

        // Plain Turtle doesn't support named graphs or txn-meta extraction (TriG support handles these)
        Ok(StageResult {
            view,
            ns_registry,
            txn_meta: Vec::new(),
            graph_delta: rustc_hash::FxHashMap::default(),
        })
    }

    /// Insert new data with options
    ///
    /// Same as `insert` but allows custom transaction and commit options.
    /// Prefer using the builder API: `fluree.transact(ledger).insert(data).txn_opts(...).execute()`.
    #[doc(hidden)]
    pub async fn insert_with_opts(
        &self,
        ledger: LedgerState,
        data: &JsonValue,
        txn_opts: TxnOpts,
        commit_opts: CommitOpts,
        index_config: &IndexConfig,
    ) -> Result<TransactResult> {
        self.transact(
            ledger,
            TxnType::Insert,
            data,
            txn_opts,
            commit_opts,
            index_config,
        )
        .await
    }

    /// Upsert data into the ledger
    ///
    /// For each (subject, predicate) pair in the data, any existing values are
    /// retracted before the new values are asserted. This implements "replace mode"
    /// semantics.
    ///
    /// # Arguments
    ///
    /// * `ledger` - The ledger state (consumed)
    /// * `data` - JSON-LD data to upsert
    ///
    /// # Example
    ///
    /// ```ignore
    /// // If ex:alice already has an age, it will be replaced
    /// let result = fluree.upsert(ledger, json!({
    ///     "@context": {"ex": "http://example.org/"},
    ///     "@id": "ex:alice",
    ///     "ex:age": 31
    /// })).await?;
    /// ```
    pub async fn upsert(&self, ledger: LedgerState, data: &JsonValue) -> Result<TransactResult> {
        let index_config = self.default_index_config();
        self.transact(
            ledger,
            TxnType::Upsert,
            data,
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
        )
        .await
    }

    /// Upsert data from Turtle format
    ///
    /// Parses the Turtle input and upserts it into the ledger.
    /// For each (subject, predicate) pair, existing values are retracted
    /// before new values are asserted.
    ///
    /// # Arguments
    ///
    /// * `ledger` - The ledger state (consumed)
    /// * `turtle` - Turtle (TTL) format data
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = fluree.upsert_turtle(ledger, r#"
    ///     @prefix ex: <http://example.org/> .
    ///     ex:alice ex:age 31 .
    /// "#).await?;
    /// ```
    pub async fn upsert_turtle(&self, ledger: LedgerState, turtle: &str) -> Result<TransactResult> {
        let data = fluree_graph_turtle::parse_to_json(turtle)?;
        self.upsert(ledger, &data).await
    }

    /// Upsert data from Turtle format with options
    ///
    /// Same as `upsert_turtle` but allows custom transaction and commit options.
    /// Prefer using the builder API: `fluree.transact(ledger).upsert_turtle(ttl).txn_opts(...).execute()`.
    #[doc(hidden)]
    pub async fn upsert_turtle_with_opts(
        &self,
        ledger: LedgerState,
        turtle: &str,
        txn_opts: TxnOpts,
        commit_opts: CommitOpts,
        index_config: &IndexConfig,
    ) -> Result<TransactResult> {
        let data = fluree_graph_turtle::parse_to_json(turtle)?;
        self.upsert_with_opts(ledger, &data, txn_opts, commit_opts, index_config)
            .await
    }

    /// Upsert data with options
    ///
    /// Same as `upsert` but allows custom transaction and commit options.
    /// Prefer using the builder API: `fluree.transact(ledger).upsert(data).txn_opts(...).execute()`.
    #[doc(hidden)]
    pub async fn upsert_with_opts(
        &self,
        ledger: LedgerState,
        data: &JsonValue,
        txn_opts: TxnOpts,
        commit_opts: CommitOpts,
        index_config: &IndexConfig,
    ) -> Result<TransactResult> {
        self.transact(
            ledger,
            TxnType::Upsert,
            data,
            txn_opts,
            commit_opts,
            index_config,
        )
        .await
    }

    /// Update data with WHERE/DELETE/INSERT semantics
    ///
    /// Provides SPARQL UPDATE-style modifications where DELETE and INSERT
    /// templates reference variables bound by WHERE patterns.
    ///
    /// # Arguments
    ///
    /// * `ledger` - The ledger state (consumed)
    /// * `update_json` - Transaction with `where`, `delete`, and `insert` clauses
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Increment everyone's age by 1
    /// let result = fluree.update(ledger, json!({
    ///     "@context": {"ex": "http://example.org/"},
    ///     "where": { "@id": "?s", "ex:age": "?age" },
    ///     "delete": { "@id": "?s", "ex:age": "?age" },
    ///     "insert": { "@id": "?s", "ex:age": { "@value": "?newAge", "@type": "xsd:integer" } }
    /// })).await?;
    /// ```
    pub async fn update(
        &self,
        ledger: LedgerState,
        update_json: &JsonValue,
    ) -> Result<TransactResult> {
        let index_config = self.default_index_config();
        self.transact(
            ledger,
            TxnType::Update,
            update_json,
            TxnOpts::default(),
            CommitOpts::default(),
            &index_config,
        )
        .await
    }

    /// Update data with options
    ///
    /// Same as `update` but allows custom transaction and commit options.
    /// Prefer using the builder API: `fluree.transact(ledger).update(data).txn_opts(...).execute()`.
    #[doc(hidden)]
    pub async fn update_with_opts(
        &self,
        ledger: LedgerState,
        update_json: &JsonValue,
        txn_opts: TxnOpts,
        commit_opts: CommitOpts,
        index_config: &IndexConfig,
    ) -> Result<TransactResult> {
        self.transact(
            ledger,
            TxnType::Update,
            update_json,
            txn_opts,
            commit_opts,
            index_config,
        )
        .await
    }

    // ========================================================================
    // CREDENTIALED TRANSACTION METHODS
    // ========================================================================

    /// Execute a credentialed transaction (Clojure: credential-transact!)
    ///
    /// Verifies the signed credential, extracts the identity (DID), and executes
    /// the transaction with policy enforcement based on the verified identity.
    ///
    /// The original signed envelope is stored as `raw_txn` for provenance.
    ///
    /// # Arguments
    /// * `ledger` - The ledger state (consumed)
    /// * `credential` - JWS string or JSON object containing the signed transaction
    ///
    /// # Returns
    /// Transaction result with policy enforcement applied
    ///
    /// # Errors
    /// - Credential verification errors (400/401)
    /// - Transaction execution errors
    #[cfg(feature = "credential")]
    pub async fn credential_transact(
        &self,
        ledger: LedgerState,
        credential: crate::credential::Input<'_>,
    ) -> Result<TransactResult> {
        use fluree_db_credential::CredentialInput;

        // Convert credential to JsonValue for raw_txn storage
        // - JWS string -> JsonValue::String
        // - VC object -> JsonValue object
        let raw_credential: JsonValue = match &credential {
            CredentialInput::Jws(jws) => JsonValue::String(jws.to_string()),
            CredentialInput::Json(json) => (*json).clone(),
        };

        let verified = crate::credential::verify_credential(credential)?;

        // Build policy context with verified identity
        let opts = crate::QueryConnectionOptions {
            identity: Some(verified.did.clone()),
            ..Default::default()
        };
        let policy_ctx = crate::policy_builder::build_policy_context_from_opts(
            &ledger.snapshot,
            ledger.novelty.as_ref(),
            Some(ledger.novelty.as_ref()),
            ledger.t(),
            &opts,
        )
        .await?;

        // Context propagation: inject parent context if subject doesn't have one
        let mut txn_json = verified.subject.clone();
        if let (Some(parent_ctx), Some(obj)) = (&verified.parent_context, txn_json.as_object_mut())
        {
            if !obj.contains_key("@context") {
                obj.insert("@context".to_string(), parent_ctx.clone());
            }
        }

        // TxnOpts: author for policy checks, context for IRI expansion
        let txn_opts = TxnOpts {
            author: Some(verified.did.clone()),
            context: verified.parent_context,
            ..Default::default()
        };

        // Compute content-addressed txn_id from the raw credential bytes
        let txn_id = {
            use fluree_db_core::sha256_hex;
            let raw_bytes = serde_json::to_vec(&raw_credential).unwrap_or_default();
            let hash_hex = sha256_hex(&raw_bytes);
            format!("fluree:tx:sha256:{}", hash_hex)
        };

        // CommitOpts: author for provenance, raw_txn for storage, txn_signature for audit
        let commit_opts = CommitOpts::default()
            .author(verified.did.clone())
            .with_raw_txn(raw_credential)
            .with_txn_signature(fluree_db_novelty::TxnSignature {
                signer: verified.did.clone(),
                txn_id: Some(txn_id),
            });

        // Use transact_tracked_with_policy and extract result
        let index_config = self.default_index_config();
        let input = TrackedTransactionInput::new(
            TxnType::Update, // credential-transact! uses update! internally
            &txn_json,
            txn_opts,
            &policy_ctx,
        );
        let (result, _tally) = self
            .transact_tracked_with_policy(ledger, input, commit_opts, &index_config)
            .await
            .map_err(|e: TrackedErrorResponse| {
                // Map TrackedErrorResponse to ApiError, preserving HTTP status
                ApiError::http(e.status, e.error)
            })?;

        Ok(result)
    }
}

impl<S, N> crate::Fluree<S, N>
where
    S: Storage + ContentAddressedWrite + Clone + 'static,
    N: NameService + Publisher,
{
    /// Update data using a transaction that specifies the ledger ID.
    ///
    /// This mirrors Clojure's `update!` API where the transaction payload includes
    /// a `ledger` field. The ledger is loaded by alias before executing the update.
    pub async fn update_with_ledger(&self, update_json: &JsonValue) -> Result<TransactResult> {
        let ledger_id = ledger_id_from_txn(update_json)?;
        let ledger = self.ledger(ledger_id).await?;
        self.update(ledger, update_json).await
    }

    /// Update data using a ledger-specified transaction with tracking enabled.
    ///
    /// Returns the transaction result plus tracking tally (if requested by opts).
    pub async fn update_with_ledger_tracked(
        &self,
        update_json: &JsonValue,
    ) -> Result<(TransactResult, Option<TrackingTally>)> {
        let ledger_id = ledger_id_from_txn(update_json)?;
        let ledger = self.ledger(ledger_id).await?;
        let policy_ctx = crate::PolicyContext::new(fluree_db_policy::PolicyWrapper::root(), None);
        let index_config = self.default_index_config();
        let input = TrackedTransactionInput::new(
            TxnType::Update,
            update_json,
            TxnOpts::default(),
            &policy_ctx,
        );
        let (result, tally) = self
            .transact_tracked_with_policy(ledger, input, CommitOpts::default(), &index_config)
            .await
            .map_err(|e| ApiError::http(e.status, e.error))?;
        Ok((result, tally))
    }
}

// Keep ApiError used (avoid unused import warnings if features change)
#[allow(dead_code)]
fn _ensure_error_used(e: ApiError) -> ApiError {
    e
}
