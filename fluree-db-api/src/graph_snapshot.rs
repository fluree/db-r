//! Materialized graph snapshot bound to an executor.
//!
//! [`GraphSnapshot`] holds both a [`FlureeView`] and a reference to [`Fluree`],
//! so queries no longer need `&fluree` passed separately.

use crate::graph_query_builder::GraphSnapshotQueryBuilder;
use crate::view::FlureeView;
use crate::{Fluree, NameService, SimpleCache, Storage};

/// A materialized, queryable graph snapshot.
///
/// Holds both the immutable snapshot and a reference to the executor,
/// so queries can be run without passing `&fluree` at each call site.
///
/// # Examples
///
/// ```ignore
/// let snapshot = fluree.graph("mydb:main").load().await?;
///
/// // Query multiple times — no re-loading
/// let r1 = snapshot.query().sparql("SELECT ...").execute().await?;
/// let r2 = snapshot.query().jsonld(&q).execute().await?;
///
/// // Access the underlying view if needed
/// let view = snapshot.view();
/// ```
pub struct GraphSnapshot<'a, S: Storage + 'static, N> {
    pub(crate) fluree: &'a Fluree<S, SimpleCache, N>,
    pub(crate) view: FlureeView<S, SimpleCache>,
}

impl<'a, S, N> GraphSnapshot<'a, S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Clone + Send + Sync + 'static,
{
    /// Create a new snapshot (called internally by `Graph::load()`).
    pub(crate) fn new(
        fluree: &'a Fluree<S, SimpleCache, N>,
        view: FlureeView<S, SimpleCache>,
    ) -> Self {
        Self { fluree, view }
    }

    /// Create a query builder for this snapshot.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let snapshot = fluree.graph("mydb:main").load().await?;
    /// let result = snapshot.query().jsonld(&q).execute().await?;
    /// ```
    pub fn query(&self) -> GraphSnapshotQueryBuilder<'a, '_, S, N> {
        GraphSnapshotQueryBuilder::new_from_parts(self.fluree, &self.view)
    }

    /// Access the underlying [`FlureeView`] snapshot.
    pub fn view(&self) -> &FlureeView<S, SimpleCache> {
        &self.view
    }

    /// Unwrap into the underlying [`FlureeView`] snapshot.
    pub fn into_view(self) -> FlureeView<S, SimpleCache> {
        self.view
    }
}

