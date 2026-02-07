use serde_json::Value as JsonValue;

use crate::query::helpers::{build_query_result, parse_jsonld_query, parse_sparql_to_ir};
use crate::query::nameservice_builder::NameserviceQueryBuilder;
use crate::{
    DataSource, ExecutableQuery, Fluree, GraphSourcePublisher, LedgerState, QueryResult, Result,
    Storage,
};

impl<S, N> Fluree<S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: crate::NameService + GraphSourcePublisher + Clone + Send + Sync + 'static,
{
    /// Create a builder for querying nameservice metadata.
    ///
    /// Returns a [`NameserviceQueryBuilder`] for fluent query construction
    /// against all ledger and graph source records in the nameservice.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Find all ledgers on main branch
    /// let query = json!({
    ///     "@context": {"f": "https://ns.flur.ee/ledger#"},
    ///     "select": ["?ledger"],
    ///     "where": [{"@id": "?ns", "f:ledger": "?ledger", "f:branch": "main"}]
    /// });
    ///
    /// let results = fluree.nameservice_query()
    ///     .jsonld(&query)
    ///     .execute_formatted()
    ///     .await?;
    /// ```
    ///
    /// # Available Properties
    ///
    /// Ledger records (`@type: "f:PhysicalDatabase"`):
    /// - `f:ledger` - Ledger name
    /// - `f:branch` - Branch name
    /// - `f:t` - Transaction number
    /// - `f:status` - Status ("ready" or "retracted")
    /// - `f:commit` - Commit address
    /// - `f:index` - Index info
    ///
    /// Graph source records (`@type: "f:GraphSource"`):
    /// - `f:name` - Graph source name
    /// - `f:branch` - Branch name
    /// - `fidx:config` - Configuration
    /// - `fidx:dependencies` - Source ledgers
    pub fn nameservice_query(&self) -> NameserviceQueryBuilder<'_, S, N> {
        NameserviceQueryBuilder::new(self)
    }

    /// Execute a query against all nameservice records (convenience method).
    ///
    /// This is a shorthand for:
    /// ```ignore
    /// fluree.nameservice_query()
    ///     .jsonld(&query)
    ///     .execute_formatted()
    ///     .await
    /// ```
    ///
    /// For more control over formatting, use [`nameservice_query()`](Self::nameservice_query).
    pub async fn query_nameservice(&self, query_json: &JsonValue) -> Result<JsonValue> {
        crate::nameservice_query::query_nameservice(&self.nameservice, query_json).await
    }

    /// Execute a JSON-LD query with R2RML graph source support.
    pub async fn query_graph_source(
        &self,
        ledger: &LedgerState<S>,
        query_json: &JsonValue,
    ) -> Result<QueryResult> {
        let (vars, parsed) = parse_jsonld_query(query_json, &ledger.db)?;
        let executable = ExecutableQuery::simple(parsed.clone());

        let r2rml_provider = crate::r2rml_provider!(self);
        let tracker = crate::Tracker::disabled();
        let source = DataSource::new(&ledger.db, ledger.novelty.as_ref(), ledger.t());
        let batches = crate::execute_with_r2rml(
            source,
            &vars,
            &executable,
            &tracker,
            &r2rml_provider,
            &r2rml_provider,
        )
        .await?;

        Ok(build_query_result(
            vars,
            parsed,
            batches,
            ledger.t(),
            Some(ledger.novelty.clone()),
            None,
        ))
    }

    /// Execute a SPARQL query with R2RML graph source support.
    pub async fn sparql_graph_source(
        &self,
        ledger: &LedgerState<S>,
        sparql: &str,
    ) -> Result<QueryResult> {
        let (vars, parsed) = parse_sparql_to_ir(sparql, &ledger.db)?;
        let executable = ExecutableQuery::simple(parsed.clone());

        let r2rml_provider = crate::r2rml_provider!(self);
        let tracker = crate::Tracker::disabled();
        let source = DataSource::new(&ledger.db, ledger.novelty.as_ref(), ledger.t());
        let batches = crate::execute_with_r2rml(
            source,
            &vars,
            &executable,
            &tracker,
            &r2rml_provider,
            &r2rml_provider,
        )
        .await?;

        Ok(build_query_result(
            vars,
            parsed,
            batches,
            ledger.t(),
            Some(ledger.novelty.clone()),
            None,
        ))
    }
}
