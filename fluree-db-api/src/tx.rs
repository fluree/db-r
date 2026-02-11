//! Transaction APIs (stage + commit) for Fluree DB
//!
//! This module wires `fluree-db-transact` + nameservice publishing + optional
//! indexing triggers into the high-level `fluree-db-api` surface.

use crate::{ApiError, Result};
use crate::{TrackedErrorResponse, Tracker, TrackingOptions, TrackingTally};
use fluree_db_core::{ContentAddressedWrite, ContentId, ContentKind, Storage};
use fluree_db_indexer::IndexerHandle;
use fluree_db_ledger::{IndexConfig, LedgerState, LedgerView};
use fluree_db_nameservice::{NameService, Publisher};
use fluree_db_novelty::TxnMetaEntry;
#[cfg(feature = "shacl")]
use fluree_db_shacl::ShaclEngine;
#[cfg(not(feature = "shacl"))]
use fluree_db_transact::stage as stage_txn;
#[cfg(feature = "shacl")]
use fluree_db_transact::stage_with_shacl;
use fluree_db_transact::{
    commit as commit_txn, parse_transaction, resolve_trig_meta, CommitOpts, CommitReceipt,
    NamedGraphBlock, NamespaceRegistry, RawTrigMeta, StageOptions, TemplateTerm, TripleTemplate,
    Txn, TxnOpts, TxnType,
};
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
    pub graph_delta: rustc_hash::FxHashMap<u32, String>,
}

/// Convert named graph blocks to TripleTemplates with proper graph_id assignments.
///
/// Returns a tuple of (templates, graph_delta) where:
/// - templates: Vec<TripleTemplate> with graph_id set for each template
/// - graph_delta: HashMap<u32, String> mapping g_id to graph IRI
///
/// Graph IDs are assigned starting at 2 (0=default, 1=txn-meta).
fn convert_named_graphs_to_templates(
    named_graphs: &[NamedGraphBlock],
    ns_registry: &mut NamespaceRegistry,
) -> Result<(Vec<TripleTemplate>, rustc_hash::FxHashMap<u32, String>)> {
    use fluree_db_transact::{RawObject, RawTerm};

    let mut templates = Vec::new();
    let mut graph_delta: rustc_hash::FxHashMap<u32, String> = rustc_hash::FxHashMap::default();
    let mut iri_to_id: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    let mut next_graph_id: u32 = 2; // 0=default, 1=txn-meta

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
        let mut ns_registry = NamespaceRegistry::from_db(&ledger.db);

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

        #[cfg(feature = "shacl")]
        let (view, ns_registry) = {
            // Use from_db_with_overlay to include novelty flakes (shapes committed but not yet indexed)
            let engine =
                ShaclEngine::from_db_with_overlay(&ledger.db, &*ledger.novelty, ledger.ledger_id())
                    .await
                    .map_err(fluree_db_transact::TransactError::from)?;
            let shacl_cache = engine.cache().clone();
            let mut options = match index_config {
                Some(cfg) => StageOptions::new().with_index_config(cfg),
                None => StageOptions::default(),
            };
            if tracker.is_enabled() {
                options = options.with_tracker(&tracker);
            }
            stage_with_shacl(ledger, txn, ns_registry, options, &shacl_cache).await?
        };
        #[cfg(not(feature = "shacl"))]
        let (view, ns_registry) = {
            let mut options = match index_config {
                Some(cfg) => StageOptions::new().with_index_config(cfg),
                None => StageOptions::default(),
            };
            if tracker.is_enabled() {
                options = options.with_tracker(&tracker);
            }
            stage_txn(ledger, txn, ns_registry, options).await?
        };
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
        let ns_registry = NamespaceRegistry::from_db(&ledger.db);

        // Extract txn_meta and graph_delta before staging consumes the Txn
        let txn_meta = txn.txn_meta.clone();
        let graph_delta = txn.graph_delta.clone();

        #[cfg(feature = "shacl")]
        let (view, ns_registry) = {
            let engine =
                ShaclEngine::from_db_with_overlay(&ledger.db, &*ledger.novelty, ledger.ledger_id())
                    .await
                    .map_err(fluree_db_transact::TransactError::from)?;
            let shacl_cache = engine.cache().clone();
            let options = match index_config {
                Some(cfg) => StageOptions::new().with_index_config(cfg),
                None => StageOptions::default(),
            };
            stage_with_shacl(ledger, txn, ns_registry, options, &shacl_cache).await?
        };
        #[cfg(not(feature = "shacl"))]
        let (view, ns_registry) = {
            let options = match index_config {
                Some(cfg) => StageOptions::new().with_index_config(cfg),
                None => StageOptions::default(),
            };
            stage_txn(ledger, txn, ns_registry, options).await?
        };
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
        let mut ns_registry = NamespaceRegistry::from_db(&ledger.db);
        let txn = parse_transaction(
            input.txn_json,
            input.txn_type,
            input.txn_opts,
            &mut ns_registry,
        )
        .map_err(|e| TrackedErrorResponse::new(400, e.to_string(), tracker.tally()))?;

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
        let (view, ns_registry) = {
            // Use from_db_with_overlay to include novelty flakes (shapes committed but not yet indexed)
            let engine =
                ShaclEngine::from_db_with_overlay(&ledger.db, &*ledger.novelty, ledger.ledger_id())
                    .await
                    .map_err(|e| TrackedErrorResponse::new(400, e.to_string(), tracker.tally()))?;
            let shacl_cache = engine.cache().clone();
            stage_with_shacl(ledger, txn, ns_registry, options, &shacl_cache)
                .await
                .map_err(|e| TrackedErrorResponse::new(400, e.to_string(), tracker.tally()))?
        };
        #[cfg(not(feature = "shacl"))]
        let (view, ns_registry) = stage_txn(ledger, txn, ns_registry, options)
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

        let span = tracing::info_span!(
            "stage_turtle_insert",
            ledger_t = ledger.t(),
            new_t = ledger.t() + 1,
            turtle_bytes = turtle.len()
        );
        let _guard = span.enter();

        let mut ns_registry = NamespaceRegistry::from_db(&ledger.db);
        let new_t = ledger.t() + 1;
        let txn_id = generate_txn_id();

        // Parse Turtle directly to flakes
        let parse_span = tracing::info_span!("turtle_parse_to_flakes", turtle_bytes = turtle.len());
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
            &ledger.db,
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
