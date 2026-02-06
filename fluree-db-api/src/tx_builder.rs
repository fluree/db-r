//! Transaction builders: context-first, compile-time-safe transaction construction.
//!
//! Two builder types:
//! - [`OwnedTransactBuilder`] — consumes a `LedgerState`, returns updated state
//! - [`RefTransactBuilder`] — borrows a `LedgerHandle`, updates in-place
//!
//! Plus [`Staged`] — a first-class staged (uncommitted) transaction that is
//! queryable and committable.
//!
//! # Design
//!
//! - **Infallible setters**: All setters return `Self`; errors are accumulated
//!   and reported at `.execute()` / `.stage()` / `.validate()`.
//! - **Composition**: Both builders share `TransactCore` for common fields.

use serde_json::Value as JsonValue;

use crate::error::{BuilderError, BuilderErrors};
use crate::ledger_manager::LedgerHandle;
use crate::tx::{IndexingMode, IndexingStatus, StageResult, TransactResult, TransactResultRef};
use crate::{
    ApiError, Fluree, NameService, PolicyContext, Result, Storage, TrackedErrorResponse,
    TrackedTransactionInput, Tracker, TrackingOptions,
};
use fluree_db_core::ContentAddressedWrite;
use fluree_db_ledger::{IndexConfig, LedgerState, LedgerView};
use fluree_db_nameservice::Publisher;
use fluree_db_transact::{
    parse_trig_phase1, CommitOpts, NamedGraphBlock, NamespaceRegistry, RawTrigMeta, Txn, TxnOpts,
    TxnType,
};

// ============================================================================
// TransactOperation (private)
// ============================================================================

/// The type of transaction operation to perform.
pub(crate) enum TransactOperation<'a> {
    InsertJson(&'a JsonValue),
    UpsertJson(&'a JsonValue),
    UpdateJson(&'a JsonValue),
    InsertTurtle(&'a str),
    UpsertTurtle(&'a str),
}

/// Result of parsing a transaction operation to JSON.
/// For Turtle inputs with TriG GRAPH blocks, also includes raw txn-meta and named graphs.
pub(crate) struct ParsedOperation {
    pub json: JsonValue,
    pub trig_meta: Option<RawTrigMeta>,
    pub named_graphs: Vec<NamedGraphBlock>,
}

impl<'a> TransactOperation<'a> {
    /// Get the `TxnType` for this operation.
    pub(crate) fn txn_type(&self) -> TxnType {
        match self {
            TransactOperation::InsertJson(_) => TxnType::Insert,
            TransactOperation::UpsertJson(_) => TxnType::Upsert,
            TransactOperation::UpdateJson(_) => TxnType::Update,
            TransactOperation::InsertTurtle(_) => TxnType::Insert,
            TransactOperation::UpsertTurtle(_) => TxnType::Upsert,
        }
    }

    /// Parse the operation to JSON, extracting TriG txn-meta and named graphs from Turtle inputs.
    ///
    /// For Turtle inputs with `GRAPH <...> { ... }` blocks, this extracts:
    /// - Metadata from txn-meta graph
    /// - Named graph blocks for user-defined graphs
    ///
    /// The metadata can be resolved to `TxnMetaEntry` using `resolve_trig_meta()` once a
    /// `NamespaceRegistry` is available. Named graphs are converted to
    /// `TripleTemplate`s with appropriate graph_id during staging.
    pub(crate) fn to_json_with_trig_meta(&self) -> Result<ParsedOperation> {
        match self {
            TransactOperation::InsertJson(j) => Ok(ParsedOperation {
                json: (*j).clone(),
                trig_meta: None,
                named_graphs: Vec::new(),
            }),
            TransactOperation::UpsertJson(j) => Ok(ParsedOperation {
                json: (*j).clone(),
                trig_meta: None,
                named_graphs: Vec::new(),
            }),
            TransactOperation::UpdateJson(j) => Ok(ParsedOperation {
                json: (*j).clone(),
                trig_meta: None,
                named_graphs: Vec::new(),
            }),
            TransactOperation::InsertTurtle(ttl) | TransactOperation::UpsertTurtle(ttl) => {
                // Phase 1: Extract TriG GRAPH block (if present)
                let phase1 = parse_trig_phase1(ttl)?;

                // Parse cleaned Turtle to JSON
                let json = fluree_graph_turtle::parse_to_json(&phase1.turtle)?;

                Ok(ParsedOperation {
                    json,
                    trig_meta: phase1.raw_meta,
                    named_graphs: phase1.named_graphs,
                })
            }
        }
    }
}

// ============================================================================
// TransactCore (shared, private)
// ============================================================================

/// Shared fields for both transaction builders.
pub(crate) struct TransactCore<'a> {
    pub(crate) operation: Option<TransactOperation<'a>>,
    /// Pre-built transaction IR (bypasses parsing, used for SPARQL UPDATE)
    pub(crate) pre_built_txn: Option<Txn>,
    pub(crate) txn_opts: TxnOpts,
    pub(crate) commit_opts: CommitOpts,
    pub(crate) index_config: Option<IndexConfig>,
    pub(crate) tracking: Option<TrackingOptions>,
    pub(crate) policy: Option<PolicyContext>,
    errors: Vec<BuilderError>,
}

impl<'a> TransactCore<'a> {
    pub(crate) fn new() -> Self {
        Self {
            operation: None,
            pre_built_txn: None,
            txn_opts: TxnOpts::default(),
            commit_opts: CommitOpts::default(),
            index_config: None,
            tracking: None,
            policy: None,
            errors: Vec::new(),
        }
    }

    pub(crate) fn set_pre_built_txn(&mut self, txn: Txn) {
        if self.operation.is_some() || self.pre_built_txn.is_some() {
            self.errors.push(BuilderError::Conflict {
                field: "operation",
                message: "Transaction operation already set; cannot set pre-built txn".to_string(),
            });
        } else {
            self.pre_built_txn = Some(txn);
        }
    }

    pub(crate) fn set_operation(&mut self, op: TransactOperation<'a>) {
        if self.operation.is_some() {
            self.errors.push(BuilderError::Conflict {
                field: "operation",
                message: "Transaction operation already set; cannot set multiple operations"
                    .to_string(),
            });
        } else {
            self.operation = Some(op);
        }
    }

    pub(crate) fn validate(&self) -> std::result::Result<(), BuilderErrors> {
        let mut errors = self.errors.clone();
        // Either operation or pre_built_txn must be set
        if self.operation.is_none() && self.pre_built_txn.is_none() {
            errors.push(BuilderError::Missing {
                field: "operation",
                hint: "Call .insert(), .upsert(), .update(), .insert_turtle(), .upsert_turtle(), or .txn()",
            });
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(BuilderErrors(errors))
        }
    }
}

// ============================================================================
// Staged
// ============================================================================

/// A staged (uncommitted) transaction. Queryable and committable.
///
/// Created by [`OwnedTransactBuilder::stage()`]. The staged state can be:
/// - **Queried** via [`FlureeView::from_staged()`](crate::FlureeView) to
///   preview changes before committing
/// - **Committed** via [`Fluree::commit_staged()`](crate::Fluree) to persist
///
/// # Example
///
/// ```ignore
/// let staged = fluree.stage_owned(ledger)
///     .insert(&data)
///     .stage().await?;
///
/// // Query staged state
/// let graph = FlureeView::from_staged(&staged);
/// let preview = graph.query(&fluree).jsonld(&q).execute().await?;
///
/// // Commit if satisfied
/// let result = fluree.commit_staged(staged, CommitOpts::default()).await?;
/// ```
pub struct Staged<S: Storage + 'static> {
    /// The queryable staged view (base + overlay with staged flakes).
    pub view: LedgerView<S>,
    /// Namespace registry needed for commit.
    pub ns_registry: NamespaceRegistry,
}

impl<S: Storage + 'static> std::fmt::Debug for Staged<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Staged")
            .field("has_staged", &self.view.has_staged())
            .finish()
    }
}

// ============================================================================
// OwnedTransactBuilder
// ============================================================================

/// Builder for transactions that consume a `LedgerState`.
///
/// Created via [`Fluree::stage_owned()`]. Use this for CLI tools, scripts, or
/// tests where you manage your own ledger state. For server/application
/// contexts, prefer [`RefTransactBuilder`] via [`Fluree::stage()`].
///
/// The ledger state is consumed and returned in the result as an updated
/// `LedgerState`.
///
/// # Example
///
/// ```ignore
/// let result = fluree.stage_owned(ledger)
///     .insert(&data)
///     .execute().await?;
/// let ledger = result.ledger;
/// ```
pub struct OwnedTransactBuilder<'a, S: Storage + 'static, N> {
    fluree: &'a Fluree<S, N>,
    ledger: LedgerState<S>,
    core: TransactCore<'a>,
}

impl<'a, S, N> OwnedTransactBuilder<'a, S, N>
where
    S: Storage + ContentAddressedWrite + Clone + 'static,
    N: NameService + Publisher,
{
    /// Create a new builder (called by `Fluree::stage_owned()`).
    pub(crate) fn new(fluree: &'a Fluree<S, N>, ledger: LedgerState<S>) -> Self {
        Self {
            fluree,
            ledger,
            core: TransactCore::new(),
        }
    }

    // -- Operation setters --

    /// Set the operation to insert JSON-LD data.
    pub fn insert(mut self, data: &'a JsonValue) -> Self {
        self.core.set_operation(TransactOperation::InsertJson(data));
        self
    }

    /// Set the operation to upsert JSON-LD data.
    pub fn upsert(mut self, data: &'a JsonValue) -> Self {
        self.core.set_operation(TransactOperation::UpsertJson(data));
        self
    }

    /// Set the operation to update with WHERE/DELETE/INSERT semantics.
    pub fn update(mut self, data: &'a JsonValue) -> Self {
        self.core.set_operation(TransactOperation::UpdateJson(data));
        self
    }

    /// Set the operation to insert Turtle data.
    pub fn insert_turtle(mut self, turtle: &'a str) -> Self {
        self.core
            .set_operation(TransactOperation::InsertTurtle(turtle));
        self
    }

    /// Set the operation to upsert Turtle data.
    pub fn upsert_turtle(mut self, turtle: &'a str) -> Self {
        self.core
            .set_operation(TransactOperation::UpsertTurtle(turtle));
        self
    }

    /// Set a pre-built transaction IR (bypasses JSON/Turtle parsing).
    ///
    /// This is used for SPARQL UPDATE where the transaction is already
    /// lowered to the IR representation.
    pub fn txn(mut self, txn: Txn) -> Self {
        self.core.set_pre_built_txn(txn);
        self
    }

    // -- Option setters --

    /// Set transaction options (author, context, etc.).
    pub fn txn_opts(mut self, opts: TxnOpts) -> Self {
        self.core.txn_opts = opts;
        self
    }

    /// Set commit options (message, author, etc.).
    pub fn commit_opts(mut self, opts: CommitOpts) -> Self {
        self.core.commit_opts = opts;
        self
    }

    /// Override the index configuration.
    pub fn index_config(mut self, config: IndexConfig) -> Self {
        self.core.index_config = Some(config);
        self
    }

    /// Enable tracking with custom options.
    pub fn tracking(mut self, opts: TrackingOptions) -> Self {
        self.core.tracking = Some(opts);
        self
    }

    /// Set policy enforcement for the transaction.
    pub fn policy(mut self, ctx: PolicyContext) -> Self {
        self.core.policy = Some(ctx);
        self
    }

    // -- Terminal operations --

    /// Validate the builder configuration without executing.
    ///
    /// Returns all accumulated errors at once.
    pub fn validate(&self) -> std::result::Result<(), BuilderErrors> {
        self.core.validate()
    }

    /// Stage + commit the transaction, returning the updated ledger state.
    pub async fn execute(self) -> Result<TransactResult<S>> {
        self.core.validate().map_err(ApiError::Builder)?;

        let op = self.core.operation.unwrap(); // safe: validate checks

        // Direct flake path for InsertTurtle (bypass JSON-LD / IR)
        if let TransactOperation::InsertTurtle(turtle) = op {
            let index_config = self.core.index_config.unwrap_or_default();
            return self
                .fluree
                .insert_turtle_with_opts(
                    self.ledger,
                    turtle,
                    self.core.txn_opts,
                    self.core.commit_opts,
                    &index_config,
                )
                .await;
        }

        let txn_type = op.txn_type();
        // Parse transaction, extracting TriG metadata and named graphs for Turtle inputs
        let parsed = op.to_json_with_trig_meta()?;
        let txn_json = parsed.json;
        let trig_meta = parsed.trig_meta;
        let named_graphs = parsed.named_graphs;
        let index_config = self.core.index_config.unwrap_or_default();

        // Store raw transaction JSON ONLY when explicitly opted-in, or when already provided
        // (e.g., signed credential envelope for provenance).
        let store_raw_txn = self.core.txn_opts.store_raw_txn.unwrap_or(false);
        let commit_opts = if self.core.commit_opts.raw_txn.is_none() && store_raw_txn {
            self.core.commit_opts.with_raw_txn(txn_json.clone())
        } else {
            self.core.commit_opts
        };

        // If policy + tracking are set, use the tracked+policy path
        // TODO: Add named_graphs support to tracked+policy path
        if let Some(policy) = &self.core.policy {
            let input =
                TrackedTransactionInput::new(txn_type, &txn_json, self.core.txn_opts, policy);
            let (result, _tally) = self
                .fluree
                .transact_tracked_with_policy(self.ledger, input, commit_opts, &index_config)
                .await
                .map_err(|e: TrackedErrorResponse| ApiError::http(e.status, e.error))?;
            return Ok(result);
        }

        // Standard path: delegate to transact_with_named_graphs
        self.fluree
            .transact_with_named_graphs(
                self.ledger,
                txn_type,
                &txn_json,
                self.core.txn_opts,
                commit_opts,
                &index_config,
                trig_meta.as_ref(),
                &named_graphs,
            )
            .await
    }

    /// Stage the transaction without committing.
    ///
    /// Returns a [`Staged`] that can be queried and later committed.
    pub async fn stage(self) -> Result<Staged<S>> {
        self.core.validate().map_err(ApiError::Builder)?;

        let op = self.core.operation.unwrap();

        // Direct flake path for InsertTurtle
        if let TransactOperation::InsertTurtle(turtle) = op {
            let index_config = self.core.index_config.unwrap_or_default();
            let stage_result = self
                .fluree
                .stage_turtle_insert(self.ledger, turtle, Some(&index_config))
                .await?;
            return Ok(Staged {
                view: stage_result.view,
                ns_registry: stage_result.ns_registry,
            });
        }

        let txn_type = op.txn_type();
        // Parse transaction, extracting TriG metadata and named graphs for Turtle inputs
        let parsed = op.to_json_with_trig_meta()?;
        let txn_json = parsed.json;
        let trig_meta = parsed.trig_meta;
        let named_graphs = parsed.named_graphs;
        let index_config = self.core.index_config.unwrap_or_default();

        // If policy is set, use the tracked+policy staging path
        // TODO: Add named_graphs support to tracked+policy path
        if let Some(policy) = &self.core.policy {
            let tracker = Tracker::new(self.core.tracking.unwrap_or(TrackingOptions {
                track_time: true,
                track_fuel: true,
                track_policy: true,
                max_fuel: None,
            }));
            let input =
                TrackedTransactionInput::new(txn_type, &txn_json, self.core.txn_opts, policy);
            let stage_result = self
                .fluree
                .stage_transaction_tracked_with_policy(
                    self.ledger,
                    input,
                    Some(&index_config),
                    &tracker,
                )
                .await
                .map_err(|e: TrackedErrorResponse| ApiError::http(e.status, e.error))?;

            return Ok(Staged {
                view: stage_result.view,
                ns_registry: stage_result.ns_registry,
            });
        }

        // Standard staging path with named graphs support
        let stage_result = self
            .fluree
            .stage_transaction_with_named_graphs(
                self.ledger,
                txn_type,
                &txn_json,
                self.core.txn_opts,
                Some(&index_config),
                trig_meta.as_ref(),
                &named_graphs,
            )
            .await?;

        Ok(Staged {
            view: stage_result.view,
            ns_registry: stage_result.ns_registry,
        })
    }
}

// ============================================================================
// RefTransactBuilder
// ============================================================================

/// Builder for transactions using a cached [`LedgerHandle`].
///
/// Created via [`Fluree::stage()`]. This is the recommended way to transact
/// in server/application contexts. The handle is borrowed and updated
/// in-place on successful commit, ensuring concurrent readers see the update.
///
/// # Example
///
/// ```ignore
/// let handle = fluree.ledger_cached("mydb:main").await?;
/// let result = fluree.stage(&handle)
///     .insert(&data)
///     .execute().await?;
/// ```
pub struct RefTransactBuilder<'a, S: Storage + 'static, N> {
    fluree: &'a Fluree<S, N>,
    handle: &'a LedgerHandle<S>,
    core: TransactCore<'a>,
}

impl<'a, S, N> RefTransactBuilder<'a, S, N>
where
    S: Storage + ContentAddressedWrite + Clone + 'static,
    N: NameService + Publisher + Clone + Send + Sync + 'static,
{
    /// Create a new builder (called by `Fluree::stage()`).
    pub(crate) fn new(fluree: &'a Fluree<S, N>, handle: &'a LedgerHandle<S>) -> Self {
        Self {
            fluree,
            handle,
            core: TransactCore::new(),
        }
    }

    // -- Operation setters --

    /// Set the operation to insert JSON-LD data.
    pub fn insert(mut self, data: &'a JsonValue) -> Self {
        self.core.set_operation(TransactOperation::InsertJson(data));
        self
    }

    /// Set the operation to upsert JSON-LD data.
    pub fn upsert(mut self, data: &'a JsonValue) -> Self {
        self.core.set_operation(TransactOperation::UpsertJson(data));
        self
    }

    /// Set the operation to update with WHERE/DELETE/INSERT semantics.
    pub fn update(mut self, data: &'a JsonValue) -> Self {
        self.core.set_operation(TransactOperation::UpdateJson(data));
        self
    }

    /// Set the operation to insert Turtle data.
    pub fn insert_turtle(mut self, turtle: &'a str) -> Self {
        self.core
            .set_operation(TransactOperation::InsertTurtle(turtle));
        self
    }

    /// Set the operation to upsert Turtle data.
    pub fn upsert_turtle(mut self, turtle: &'a str) -> Self {
        self.core
            .set_operation(TransactOperation::UpsertTurtle(turtle));
        self
    }

    /// Set a pre-built transaction IR (bypasses JSON/Turtle parsing).
    ///
    /// This is used for SPARQL UPDATE where the transaction is already
    /// lowered to the IR representation.
    pub fn txn(mut self, txn: Txn) -> Self {
        self.core.set_pre_built_txn(txn);
        self
    }

    // -- Option setters --

    /// Set transaction options (author, context, etc.).
    pub fn txn_opts(mut self, opts: TxnOpts) -> Self {
        self.core.txn_opts = opts;
        self
    }

    /// Set commit options (message, author, etc.).
    pub fn commit_opts(mut self, opts: CommitOpts) -> Self {
        self.core.commit_opts = opts;
        self
    }

    /// Override the index configuration.
    pub fn index_config(mut self, config: IndexConfig) -> Self {
        self.core.index_config = Some(config);
        self
    }

    /// Enable tracking with custom options.
    pub fn tracking(mut self, opts: TrackingOptions) -> Self {
        self.core.tracking = Some(opts);
        self
    }

    /// Set policy enforcement for the transaction.
    pub fn policy(mut self, ctx: PolicyContext) -> Self {
        self.core.policy = Some(ctx);
        self
    }

    // -- Terminal operations --

    /// Validate the builder configuration without executing.
    pub fn validate(&self) -> std::result::Result<(), BuilderErrors> {
        self.core.validate()
    }

    /// Stage + commit the transaction, updating the handle in-place.
    pub async fn execute(self) -> Result<TransactResultRef> {
        commit_with_handle(self.fluree, self.handle, self.core).await
    }
}

// ============================================================================
// Shared commit helper (used by RefTransactBuilder and GraphTransactBuilder)
// ============================================================================

/// Stage and commit a transaction against a cached ledger handle.
///
/// This is the shared logic for `RefTransactBuilder::execute()` and
/// `GraphTransactBuilder::commit()`.
pub(crate) async fn commit_with_handle<S, N>(
    fluree: &Fluree<S, N>,
    handle: &LedgerHandle<S>,
    core: TransactCore<'_>,
) -> Result<TransactResultRef>
where
    S: Storage + ContentAddressedWrite + Clone + Send + Sync + 'static,
    N: NameService + Publisher + Clone + Send + Sync + 'static,
{
    core.validate().map_err(ApiError::Builder)?;

    let index_config = core.index_config.unwrap_or_default();
    let store_raw_txn = core.txn_opts.store_raw_txn.unwrap_or(false);

    // Acquire write lock
    let mut write_guard = handle.lock_for_write().await;
    let ledger_state = write_guard.clone_state();

    // Handle pre-built Txn (SPARQL UPDATE) vs operation-based transaction
    let (stage_result, txn_type, commit_opts) = if let Some(txn) = core.pre_built_txn {
        let txn_type = txn.txn_type;
        // For pre-built Txn, don't attach raw_txn (we don't have the original format)
        let stage_result = fluree
            .stage_transaction_from_txn(ledger_state, txn, Some(&index_config))
            .await?;
        (stage_result, txn_type, core.commit_opts)
    } else {
        let op = core.operation.unwrap(); // safe: validate checks

        // Direct flake path for InsertTurtle (bypass JSON-LD / IR)
        if let TransactOperation::InsertTurtle(turtle) = op {
            let stage_result = fluree
                .stage_turtle_insert(ledger_state, turtle, Some(&index_config))
                .await?;
            // Store raw Turtle text when explicitly opted-in
            let commit_opts = if core.commit_opts.raw_txn.is_none() && store_raw_txn {
                core.commit_opts
                    .with_raw_txn(serde_json::Value::String(turtle.to_string()))
            } else {
                core.commit_opts
            };
            (stage_result, TxnType::Insert, commit_opts)
        } else {
            let txn_type = op.txn_type();
            // Parse transaction, extracting TriG metadata and named graphs for Turtle inputs
            let parsed = op.to_json_with_trig_meta()?;
            let txn_json = parsed.json;
            let trig_meta = parsed.trig_meta;
            let named_graphs = parsed.named_graphs;

            // Store raw transaction JSON ONLY when explicitly opted-in, or when already provided
            // (e.g., signed credential envelope for provenance).
            let commit_opts = if core.commit_opts.raw_txn.is_none() && store_raw_txn {
                core.commit_opts.with_raw_txn(txn_json.clone())
            } else {
                core.commit_opts
            };

            // Stage
            // TODO: Add named_graphs support to tracked+policy path
            let stage_result = if let Some(policy) = &core.policy {
                let tracker = Tracker::new(core.tracking.unwrap_or(TrackingOptions {
                    track_time: true,
                    track_fuel: true,
                    track_policy: true,
                    max_fuel: None,
                }));
                let input =
                    TrackedTransactionInput::new(txn_type, &txn_json, core.txn_opts, policy);
                fluree
                    .stage_transaction_tracked_with_policy(
                        ledger_state,
                        input,
                        Some(&index_config),
                        &tracker,
                    )
                    .await
                    .map_err(|e: TrackedErrorResponse| ApiError::http(e.status, e.error))?
            } else {
                fluree
                    .stage_transaction_with_named_graphs(
                        ledger_state,
                        txn_type,
                        &txn_json,
                        core.txn_opts,
                        Some(&index_config),
                        trig_meta.as_ref(),
                        &named_graphs,
                    )
                    .await?
            };
            (stage_result, txn_type, commit_opts)
        }
    };

    let StageResult {
        view,
        ns_registry,
        txn_meta,
        graph_delta,
    } = stage_result;

    // Add extracted transaction metadata and graph delta to commit opts
    let commit_opts = commit_opts
        .with_txn_meta(txn_meta)
        .with_graph_delta(graph_delta.into_iter().collect());

    // Handle no-op
    let (receipt, new_state) =
        if !view.has_staged() && matches!(txn_type, TxnType::Update | TxnType::Upsert) {
            let (base, _) = view.into_parts();
            (
                fluree_db_transact::CommitReceipt {
                    address: String::new(),
                    commit_id: String::new(),
                    t: base.t(),
                    flake_count: 0,
                },
                base,
            )
        } else {
            fluree
                .commit_staged(view, ns_registry, &index_config, commit_opts)
                .await?
        };

    // Compute indexing status
    let indexing_status = IndexingStatus {
        enabled: fluree.indexing_mode.is_enabled(),
        needed: new_state.should_reindex(&index_config),
        novelty_size: new_state.novelty_size(),
        index_t: new_state.index_t(),
        commit_t: receipt.t,
    };

    // Trigger background indexing if needed
    if let IndexingMode::Background(h) = &fluree.indexing_mode {
        if indexing_status.needed {
            h.trigger(new_state.alias(), receipt.t).await;
        }
    }

    // Update cache
    write_guard.replace(new_state);

    Ok(TransactResultRef {
        receipt,
        indexing: indexing_status,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FlureeBuilder;
    use serde_json::json;

    // ========================================================================
    // Validation tests
    // ========================================================================

    #[test]
    fn test_transact_core_missing_operation() {
        let core = TransactCore::new();
        let result = core.validate();
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert_eq!(errs.0.len(), 1);
        assert!(matches!(
            &errs.0[0],
            BuilderError::Missing {
                field: "operation",
                ..
            }
        ));
    }

    #[test]
    fn test_transact_core_double_operation_conflict() {
        let json1 = json!({"@id": "ex:a", "ex:name": "Alice"});
        let json2 = json!({"@id": "ex:b", "ex:name": "Bob"});
        let mut core = TransactCore::new();
        core.set_operation(TransactOperation::InsertJson(&json1));
        core.set_operation(TransactOperation::UpsertJson(&json2));
        let result = core.validate();
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert!(errs.0.iter().any(|e| matches!(
            e,
            BuilderError::Conflict {
                field: "operation",
                ..
            }
        )));
    }

    #[test]
    fn test_transact_core_valid_insert() {
        let json = json!({"@id": "ex:a", "ex:name": "Alice"});
        let mut core = TransactCore::new();
        core.set_operation(TransactOperation::InsertJson(&json));
        let result = core.validate();
        assert!(result.is_ok());
    }

    // ========================================================================
    // OwnedTransactBuilder validation tests
    // ========================================================================

    #[tokio::test]
    async fn test_owned_builder_missing_operation() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("testdb").await.unwrap();

        let result = fluree.stage_owned(ledger).execute().await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status_code(), 400);
    }

    #[tokio::test]
    async fn test_owned_builder_double_operation() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("testdb").await.unwrap();

        let data1 = json!({"insert": [{"@id": "ex:a", "ex:name": "Alice"}]});
        let data2 = json!({"insert": [{"@id": "ex:b", "ex:name": "Bob"}]});

        let result = fluree
            .stage_owned(ledger)
            .insert(&data1)
            .upsert(&data2)
            .execute()
            .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status_code(), 400);
    }

    // ========================================================================
    // Integration tests
    // ========================================================================

    #[tokio::test]
    async fn test_owned_builder_insert() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("testdb").await.unwrap();

        let data = json!({"insert": [{"@id": "ex:a", "ex:name": "Alice"}]});
        let result = fluree.stage_owned(ledger).insert(&data).execute().await;
        assert!(result.is_ok());
        let txn_result = result.unwrap();
        assert_eq!(txn_result.receipt.t, 1);
    }

    #[tokio::test]
    async fn test_owned_builder_upsert() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("testdb").await.unwrap();

        let data = json!({"insert": [{"@id": "ex:a", "ex:name": "Alice"}]});
        let result = fluree.stage_owned(ledger).upsert(&data).execute().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_owned_builder_with_commit_opts() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("testdb").await.unwrap();

        let data = json!({"insert": [{"@id": "ex:a", "ex:name": "Alice"}]});
        let result = fluree
            .stage_owned(ledger)
            .insert(&data)
            .commit_opts(CommitOpts::with_message("test commit"))
            .execute()
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_owned_builder_equivalence_with_convenience() {
        let fluree = FlureeBuilder::memory().build_memory();

        // Via convenience method
        let ledger1 = fluree.create_ledger("testdb1").await.unwrap();
        let data = json!({"insert": [{"@id": "ex:a", "ex:name": "Alice"}]});
        let result1 = fluree.insert(ledger1, &data).await.unwrap();

        // Via builder
        let ledger2 = fluree.create_ledger("testdb2").await.unwrap();
        let result2 = fluree
            .stage_owned(ledger2)
            .insert(&data)
            .execute()
            .await
            .unwrap();

        // Both should succeed at t=1
        assert_eq!(result1.receipt.t, 1);
        assert_eq!(result2.receipt.t, 1);
    }

    #[tokio::test]
    async fn test_owned_builder_stage_without_commit() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("testdb").await.unwrap();

        let data = json!({"insert": [{"@id": "ex:a", "ex:name": "Alice"}]});
        let staged = fluree.stage_owned(ledger).insert(&data).stage().await;
        assert!(staged.is_ok());
        let staged = staged.unwrap();
        assert!(staged.view.has_staged());
    }

    #[tokio::test]
    async fn test_owned_builder_validate() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree.create_ledger("testdb").await.unwrap();

        let data = json!({"insert": [{"@id": "ex:a", "ex:name": "Alice"}]});

        // Valid builder
        let builder = fluree.stage_owned(ledger).insert(&data);
        assert!(builder.validate().is_ok());
    }

    #[tokio::test]
    async fn test_ref_builder_insert() {
        let fluree = FlureeBuilder::memory().with_ledger_caching().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();
        let handle = fluree.ledger_cached("testdb:main").await.unwrap();

        let data = json!({"insert": [{"@id": "ex:a", "ex:name": "Alice"}]});
        let result = fluree.stage(&handle).insert(&data).execute().await;
        assert!(result.is_ok());
        let txn_result = result.unwrap();
        assert_eq!(txn_result.receipt.t, 1);

        // Handle should be updated
        let snapshot = handle.snapshot().await;
        assert_eq!(snapshot.t, 1);
    }
}
