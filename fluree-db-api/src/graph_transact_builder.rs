//! Transaction builder for the [`Graph`] API.
//!
//! - [`GraphTransactBuilder`] — lazy transaction from a [`Graph`] handle
//! - [`StagedGraph`] — staged (uncommitted) transaction queryable via [`GraphSnapshotQueryBuilder`]

use serde_json::Value as JsonValue;

use crate::error::BuilderErrors;
use crate::graph::Graph;
use crate::graph_query_builder::GraphSnapshotQueryBuilder;
use crate::tx_builder::{commit_with_handle, Staged, TransactCore, TransactOperation};
use crate::view::FlureeView;
use crate::{
    ApiError, Fluree, NameService, PolicyContext, Result, Storage, TrackedErrorResponse, Tracker,
    TrackingOptions, TransactResultRef,
};
use fluree_db_core::ContentAddressedWrite;
use fluree_db_ledger::IndexConfig;
use fluree_db_nameservice::Publisher;
use fluree_db_transact::{CommitOpts, TxnOpts};

// ============================================================================
// GraphTransactBuilder
// ============================================================================

/// Transaction builder from a lazy [`Graph`] handle.
///
/// No I/O occurs until a terminal method (`.commit()`, `.stage()`) is called.
///
/// # Examples
///
/// ```ignore
/// // Commit directly
/// let out = fluree
///     .graph("mydb:main")
///     .transact()
///     .insert(&data)
///     .commit()
///     .await?;
///
/// // Stage without committing
/// let staged = fluree
///     .graph("mydb:main")
///     .transact()
///     .insert(&data)
///     .stage()
///     .await?;
/// ```
pub struct GraphTransactBuilder<'a, 'g, S: Storage + 'static, N> {
    graph: &'g Graph<'a, S, N>,
    core: TransactCore<'g>,
}

impl<'a, 'g, S, N> GraphTransactBuilder<'a, 'g, S, N>
where
    S: Storage + ContentAddressedWrite + Clone + Send + Sync + 'static,
    N: NameService + Publisher + Clone + Send + Sync + 'static,
{
    /// Create a new builder (called by `Graph::transact()`).
    pub(crate) fn new(graph: &'g Graph<'a, S, N>) -> Self {
        Self {
            graph,
            core: TransactCore::new(),
        }
    }

    // -- Operation setters --

    /// Set the operation to insert JSON-LD data.
    pub fn insert(mut self, data: &'g JsonValue) -> Self {
        self.core.set_operation(TransactOperation::InsertJson(data));
        self
    }

    /// Set the operation to upsert JSON-LD data.
    pub fn upsert(mut self, data: &'g JsonValue) -> Self {
        self.core.set_operation(TransactOperation::UpsertJson(data));
        self
    }

    /// Set the operation to update with WHERE/DELETE/INSERT semantics.
    pub fn update(mut self, data: &'g JsonValue) -> Self {
        self.core.set_operation(TransactOperation::UpdateJson(data));
        self
    }

    /// Set the operation to insert Turtle data.
    pub fn insert_turtle(mut self, turtle: &'g str) -> Self {
        self.core
            .set_operation(TransactOperation::InsertTurtle(turtle));
        self
    }

    /// Set the operation to upsert Turtle data.
    pub fn upsert_turtle(mut self, turtle: &'g str) -> Self {
        self.core
            .set_operation(TransactOperation::UpsertTurtle(turtle));
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

    /// Stage + commit the transaction against the latest ledger head.
    ///
    /// Loads the cached ledger handle, acquires a write lock, stages,
    /// commits (with head-check), and updates the cache.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let out = fluree
    ///     .graph("mydb:main")
    ///     .transact()
    ///     .insert(&data)
    ///     .commit()
    ///     .await?;
    /// ```
    pub async fn commit(self) -> Result<TransactResultRef> {
        let handle = self.graph.fluree.ledger_cached(&self.graph.alias).await?;
        commit_with_handle(self.graph.fluree, &handle, self.core).await
    }

    /// Stage the transaction without committing.
    ///
    /// Returns a [`StagedGraph`] that can be queried to preview changes.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let staged = fluree
    ///     .graph("mydb:main")
    ///     .transact()
    ///     .insert(&data)
    ///     .stage()
    ///     .await?;
    ///
    /// let preview = staged.query().jsonld(&q).execute().await?;
    /// ```
    pub async fn stage(self) -> Result<StagedGraph<'a, S, N>> {
        self.core.validate().map_err(|e| ApiError::Builder(e))?;

        let op = self.core.operation.unwrap();
        let txn_type = op.txn_type();
        let txn_json_cow = op.to_json()?;
        let index_config = self.core.index_config.unwrap_or_default();

        // Load the current ledger state
        let ledger_state = self.graph.fluree.ledger(&self.graph.alias).await?;

        // Stage
        let stage_result = if let Some(policy) = &self.core.policy {
            let tracker = Tracker::new(self.core.tracking.unwrap_or_else(|| TrackingOptions {
                track_time: true,
                track_fuel: true,
                track_policy: true,
                max_fuel: None,
            }));
            self.graph
                .fluree
                .stage_transaction_tracked_with_policy(
                    ledger_state,
                    txn_type,
                    &txn_json_cow,
                    self.core.txn_opts,
                    Some(&index_config),
                    policy,
                    &tracker,
                )
                .await
                .map_err(|e: TrackedErrorResponse| ApiError::http(e.status, e.error))?
        } else {
            self.graph
                .fluree
                .stage_transaction(
                    ledger_state,
                    txn_type,
                    &txn_json_cow,
                    self.core.txn_opts,
                    Some(&index_config),
                )
                .await?
        };

        // Pre-build the FlureeView from staged so query() can borrow it
        let staged = Staged {
            view: stage_result.view,
            ns_registry: stage_result.ns_registry,
        };
        let staged_view = FlureeView::from_staged(&staged)?;

        Ok(StagedGraph {
            fluree: self.graph.fluree,
            staged,
            staged_view,
        })
    }
}

// ============================================================================
// StagedGraph
// ============================================================================

/// A staged (uncommitted) transaction bound to an executor.
///
/// Queries against this type see the staged changes.
/// Stage-on-stage and commit-from-staged are TBD.
///
/// # Example
///
/// ```ignore
/// let staged = fluree
///     .graph("mydb:main")
///     .transact()
///     .insert(&data)
///     .stage()
///     .await?;
///
/// let preview = staged.query().jsonld(&q).execute().await?;
/// ```
pub struct StagedGraph<'a, S: Storage + 'static, N> {
    fluree: &'a Fluree<S, N>,
    staged: Staged<S>,
    staged_view: FlureeView<S>,
}

impl<'a, S, N> StagedGraph<'a, S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Clone + Send + Sync + 'static,
{
    /// Create a query builder that sees the staged changes.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let preview = staged.query().jsonld(&q).execute().await?;
    /// ```
    pub fn query(&self) -> GraphSnapshotQueryBuilder<'a, '_, S, N> {
        GraphSnapshotQueryBuilder::new_from_parts(self.fluree, &self.staged_view)
    }

    /// Access the underlying [`Staged`] transaction.
    pub fn staged(&self) -> &Staged<S> {
        &self.staged
    }

    /// Check if the transaction produced any staged changes.
    pub fn has_staged(&self) -> bool {
        self.staged.view.has_staged()
    }
}
