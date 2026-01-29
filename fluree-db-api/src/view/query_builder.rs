//! Builder entry points on FlureeView and FlureeDataSetView.
//!
//! These `impl` blocks provide the `.query()` method that returns the
//! appropriate builder type (ViewQueryBuilder or DatasetQueryBuilder).

use crate::query::builder::{DatasetQueryBuilder, ViewQueryBuilder};
use crate::view::{FlureeDataSetView, FlureeView};
use crate::{Fluree, NameService, SimpleCache, Storage};

impl<S> FlureeView<S, SimpleCache>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    /// Create a query builder for this graph/view.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let view = fluree.view("mydb:main").await?;
    /// let result = view.query(&fluree)
    ///     .jsonld(&query)
    ///     .execute().await?;
    /// ```
    pub fn query<'a, N: NameService>(
        &'a self,
        fluree: &'a Fluree<S, SimpleCache, N>,
    ) -> ViewQueryBuilder<'a, S, N> {
        ViewQueryBuilder::new(fluree, self)
    }
}

impl<S> FlureeDataSetView<S, SimpleCache>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    /// Create a query builder for this dataset.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let dataset = FlureeDataSetView::new()
    ///     .with_default(view_a)
    ///     .with_named("other", view_b);
    /// let result = dataset.query(&fluree)
    ///     .jsonld(&query)
    ///     .execute().await?;
    /// ```
    pub fn query<'a, N: NameService>(
        &'a self,
        fluree: &'a Fluree<S, SimpleCache, N>,
    ) -> DatasetQueryBuilder<'a, S, N> {
        DatasetQueryBuilder::new(fluree, self)
    }
}
