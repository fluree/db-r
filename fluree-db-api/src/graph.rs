//! Lazy graph handle: zero-cost alias + time spec + executor reference.
//!
//! [`Graph`] is returned by [`Fluree::graph()`] and [`Fluree::graph_at()`].
//! No I/O occurs until a terminal method is called (`.load()`, `.query().execute()`,
//! `.transact().commit()`).

use crate::dataset::TimeSpec;
use crate::graph_query_builder::GraphQueryBuilder;
use crate::graph_snapshot::GraphSnapshot;
use crate::graph_transact_builder::GraphTransactBuilder;
use crate::{Fluree, NameService, Result, SimpleCache, Storage};
use fluree_db_nameservice::Publisher;

/// A lazy, zero-cost handle to a ledger graph.
///
/// No I/O occurs until a terminal method is called (`.load()`, `.query().execute()`,
/// `.transact().commit()`, `.transact().stage()`).
///
/// # Examples
///
/// ```ignore
/// // Lazy query — no intermediate .await?
/// let result = fluree
///     .graph("mydb:main")
///     .query()
///     .sparql("SELECT ?s WHERE { ?s ?p ?o }")
///     .execute()
///     .await?;
///
/// // Lazy transact + commit
/// let out = fluree
///     .graph("mydb:main")
///     .transact()
///     .insert(&data)
///     .commit()
///     .await?;
///
/// // Materialize for reuse
/// let snapshot = fluree.graph("mydb:main").load().await?;
/// let r1 = snapshot.query().sparql("SELECT ...").execute().await?;
/// let r2 = snapshot.query().jsonld(&q).execute().await?;
/// ```
pub struct Graph<'a, S: Storage + 'static, N> {
    pub(crate) fluree: &'a Fluree<S, SimpleCache, N>,
    pub(crate) alias: String,
    pub(crate) time_spec: TimeSpec,
}

impl<'a, S, N> Graph<'a, S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Clone + Send + Sync + 'static,
{
    /// Create a new lazy graph handle.
    pub(crate) fn new(
        fluree: &'a Fluree<S, SimpleCache, N>,
        alias: String,
        time_spec: TimeSpec,
    ) -> Self {
        Self {
            fluree,
            alias,
            time_spec,
        }
    }

    /// Materialize the snapshot, producing a [`GraphSnapshot`] that can be queried
    /// multiple times without re-loading.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let snapshot = fluree.graph("mydb:main").load().await?;
    /// let r1 = snapshot.query().sparql("...").execute().await?;
    /// let r2 = snapshot.query().jsonld(&q).execute().await?;
    /// ```
    pub async fn load(&self) -> Result<GraphSnapshot<'a, S, N>> {
        let view = self
            .fluree
            .load_view_at(&self.alias, self.time_spec.clone())
            .await?;
        Ok(GraphSnapshot::new(self.fluree, view))
    }

    /// Create a query builder. No I/O occurs until `.execute().await?`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = fluree
    ///     .graph("mydb:main")
    ///     .query()
    ///     .sparql("SELECT ?s WHERE { ?s ?p ?o }")
    ///     .execute()
    ///     .await?;
    /// ```
    pub fn query(&self) -> GraphQueryBuilder<'a, '_, S, N> {
        GraphQueryBuilder::new(self)
    }

    /// Create a transaction builder. No I/O occurs until `.commit().await?`
    /// or `.stage().await?`.
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
    pub fn transact(&self) -> GraphTransactBuilder<'a, '_, S, N>
    where
        S: fluree_db_core::ContentAddressedWrite,
        N: Publisher,
    {
        GraphTransactBuilder::new(self)
    }
}
