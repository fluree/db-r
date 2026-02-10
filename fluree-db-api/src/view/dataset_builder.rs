//! Builder for FlureeDataSetView from DatasetSpec
//!
//! Provides utilities to construct `FlureeDataSetView` from query dataset
//! specifications, applying time travel, policy, and reasoning wrappers.

use crate::view::{FlureeDataSetView, FlureeView};
use crate::{
    dataset, time_resolve, ApiError, DatasetSpec, Fluree, NameService, QueryConnectionOptions,
    Result, Storage,
};
use chrono::DateTime;

// ============================================================================
// Dataset View Builder
// ============================================================================

impl<S, N> Fluree<S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + Clone + Send + Sync + 'static,
{
    /// Build a `FlureeDataSetView` from a `DatasetSpec`.
    ///
    /// This loads views for all graphs in the spec, applying time travel
    /// specifications and per-source policy overrides where present.
    ///
    /// # Per-Source Policy
    ///
    /// If a `GraphSource` has a `policy_override` set, that policy is applied
    /// to that source's view. This enables fine-grained access control where
    /// different graphs in the same query can have different policies.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (spec, opts) = DatasetSpec::from_query_json(&query)?;
    /// let dataset = fluree.build_dataset_view(&spec).await?;
    /// let result = fluree.query_dataset_view(&dataset, &query).await?;
    /// ```
    pub async fn build_dataset_view(&self, spec: &DatasetSpec) -> Result<FlureeDataSetView<S>> {
        // History/changes queries are a Fluree dataset extension.
        // In this mode, the "from" array specifies a (from,to) range on ONE ledger,
        // not two distinct default graphs.
        if let Some(range) = spec.history_range() {
            let ledger = self.ledger(&range.identifier).await?;
            let latest_t = ledger.t();

            let from_t = resolve_history_endpoint_t(&ledger, &range.from, latest_t).await?;
            let to_t = resolve_history_endpoint_t(&ledger, &range.to, latest_t).await?;

            let view = FlureeView::from_ledger_state(&ledger);
            return Ok(FlureeDataSetView::single(view).with_history_range(from_t, to_t));
        }

        let mut dataset = FlureeDataSetView::new();

        // Load default graphs, applying per-source policy if present
        for source in &spec.default_graphs {
            let view = self.load_view_from_source(source).await?;
            let view = self.maybe_apply_source_policy(view, source).await?;
            dataset = dataset.with_default(view);
        }

        // Load named graphs, applying per-source policy if present
        for source in &spec.named_graphs {
            let view = self.load_view_from_source(source).await?;
            let view = self.maybe_apply_source_policy(view, source).await?;
            // Add by identifier (primary key)
            dataset = dataset.with_named(source.identifier.as_str(), view.clone());
            // Also add by alias if present (enables ["graph", "<alias>", ...] lookup)
            if let Some(alias) = &source.source_alias {
                dataset = dataset.with_named(alias.as_str(), view);
            }
        }

        Ok(dataset)
    }

    /// Build a `FlureeDataSetView` with policy applied to all views.
    ///
    /// Policy is built from `QueryConnectionOptions` and applied uniformly
    /// to all views in the dataset, unless a source has a per-source policy
    /// override which takes precedence.
    ///
    /// # Policy Precedence
    ///
    /// Per-source `policy_override` takes precedence over global `opts`:
    /// - If source has `policy_override` with any fields set → use per-source policy
    /// - Otherwise → use global `opts` policy
    pub async fn build_dataset_view_with_policy(
        &self,
        spec: &DatasetSpec,
        opts: &QueryConnectionOptions,
    ) -> Result<FlureeDataSetView<S>> {
        // History mode: load head view, resolve range, then wrap policy.
        if let Some(range) = spec.history_range() {
            let ledger = self.ledger(&range.identifier).await?;
            let latest_t = ledger.t();

            let from_t = resolve_history_endpoint_t(&ledger, &range.from, latest_t).await?;
            let to_t = resolve_history_endpoint_t(&ledger, &range.to, latest_t).await?;

            let view = FlureeView::from_ledger_state(&ledger);
            let view = self.wrap_policy(view, opts).await?;
            return Ok(FlureeDataSetView::single(view).with_history_range(from_t, to_t));
        }

        let mut dataset = FlureeDataSetView::new();

        // Load default graphs with policy (per-source overrides global)
        for source in &spec.default_graphs {
            let view = self.load_view_from_source(source).await?;
            let view = self.apply_policy_with_override(view, source, opts).await?;
            dataset = dataset.with_default(view);
        }

        // Load named graphs with policy (per-source overrides global)
        for source in &spec.named_graphs {
            let view = self.load_view_from_source(source).await?;
            let view = self.apply_policy_with_override(view, source, opts).await?;
            // Add by identifier (primary key)
            dataset = dataset.with_named(source.identifier.as_str(), view.clone());
            // Also add by alias if present (enables ["graph", "<alias>", ...] lookup)
            if let Some(alias) = &source.source_alias {
                dataset = dataset.with_named(alias.as_str(), view);
            }
        }

        Ok(dataset)
    }

    /// Apply per-source policy if present, otherwise no policy.
    ///
    /// This is used by `build_dataset_view` when no global policy is provided.
    async fn maybe_apply_source_policy(
        &self,
        view: FlureeView<S>,
        source: &dataset::GraphSource,
    ) -> Result<FlureeView<S>> {
        if let Some(policy_override) = &source.policy_override {
            if policy_override.has_policy() {
                let opts = policy_override.to_query_connection_options();
                return self.wrap_policy(view, &opts).await;
            }
        }
        Ok(view)
    }

    /// Apply policy with per-source override taking precedence over global.
    ///
    /// This is used by `build_dataset_view_with_policy` to allow per-source
    /// policy to override the global policy from `QueryConnectionOptions`.
    async fn apply_policy_with_override(
        &self,
        view: FlureeView<S>,
        source: &dataset::GraphSource,
        global_opts: &QueryConnectionOptions,
    ) -> Result<FlureeView<S>> {
        // Per-source policy override takes precedence
        if let Some(policy_override) = &source.policy_override {
            if policy_override.has_policy() {
                let opts = policy_override.to_query_connection_options();
                return self.wrap_policy(view, &opts).await;
            }
        }
        // Fall back to global policy
        self.wrap_policy(view, global_opts).await
    }

    /// Build a single `FlureeView` from a `GraphSource`.
    ///
    /// Applies time travel specification and graph selector if present.
    ///
    /// Graph selection can come from two sources (mutually exclusive):
    /// - Fragment syntax in identifier: `ledger:main#txn-meta`
    /// - Explicit `graph_selector` field in the GraphSource
    ///
    /// If `graph_selector` is set, it overrides any fragment in the identifier
    /// (but this combination is rejected at parse time as ambiguous).
    pub(crate) async fn load_view_from_source(
        &self,
        source: &dataset::GraphSource,
    ) -> Result<FlureeView<S>> {
        let view = match &source.time_spec {
            None => self.view(&source.identifier).await?,
            Some(time_spec) => {
                // Convert dataset::TimeSpec to crate::TimeSpec
                let ts = convert_time_spec(time_spec)?;
                self.view_at(&source.identifier, ts).await?
            }
        };

        // Apply explicit graph selector if set.
        // Note: If the identifier contained a fragment like #txn-meta, that was
        // already applied by view()/view_at(). The parser rejects the ambiguous
        // case where both fragment and graph_selector are present.
        match &source.graph_selector {
            Some(selector) => Self::apply_graph_selector(view, selector),
            None => Ok(view),
        }
    }

    /// Check if a DatasetSpec represents a single-ledger query.
    ///
    /// Returns the single view if it's a single-ledger fast-path candidate.
    pub async fn try_single_view_from_spec(
        &self,
        spec: &DatasetSpec,
    ) -> Result<Option<FlureeView<S>>> {
        // Single default graph, no named graphs, no history range = single-ledger
        // (load_view_from_source handles both with and without time_spec)
        if spec.default_graphs.len() == 1
            && spec.named_graphs.is_empty()
            && spec.history_range.is_none()
        {
            let source = &spec.default_graphs[0];
            let view = self.load_view_from_source(source).await?;
            return Ok(Some(view));
        }

        Ok(None)
    }

    /// Check if spec qualifies for single-ledger fast path (no time override).
    ///
    /// This is used to decide whether to take the optimized single-ledger path
    /// in query_connection.
    pub fn is_single_ledger_fast_path(spec: &DatasetSpec) -> bool {
        spec.default_graphs.len() == 1
            && spec.named_graphs.is_empty()
            && spec.default_graphs[0].time_spec.is_none()
    }
}

async fn resolve_history_endpoint_t<S: Storage + Clone + Send + Sync + 'static>(
    ledger: &fluree_db_ledger::LedgerState<S>,
    spec: &dataset::TimeSpec,
    latest_t: i64,
) -> Result<i64> {
    match spec {
        dataset::TimeSpec::AtT(t) => Ok(*t),
        dataset::TimeSpec::Latest => Ok(latest_t),
        dataset::TimeSpec::AtTime(iso) => {
            let dt = DateTime::parse_from_rfc3339(iso).map_err(|e| {
                ApiError::internal(format!(
                    "Invalid ISO-8601 timestamp for time travel: {} ({})",
                    iso, e
                ))
            })?;
            // See `Fluree::load_view_at` for rationale: `ledger#time` is epoch-ms and we
            // ceiling sub-ms ISO inputs to avoid truncation off-by-one.
            let mut target_epoch_ms = dt.timestamp_millis();
            if dt.timestamp_subsec_nanos() % 1_000_000 != 0 {
                target_epoch_ms += 1;
            }

            time_resolve::datetime_to_t(
                &ledger.db,
                Some(ledger.novelty.as_ref()),
                target_epoch_ms,
                latest_t,
            )
            .await
        }
        dataset::TimeSpec::AtCommit(commit_prefix) => {
            time_resolve::commit_to_t(
                &ledger.db,
                Some(ledger.novelty.as_ref()),
                commit_prefix,
                latest_t,
            )
            .await
        }
    }
}

/// Convert dataset::TimeSpec to crate::TimeSpec
fn convert_time_spec(ts: &dataset::TimeSpec) -> Result<crate::TimeSpec> {
    match ts {
        dataset::TimeSpec::AtT(t) => Ok(crate::TimeSpec::AtT(*t)),
        dataset::TimeSpec::AtTime(iso) => Ok(crate::TimeSpec::AtTime(iso.clone())),
        dataset::TimeSpec::AtCommit(sha) => Ok(crate::TimeSpec::AtCommit(sha.clone())),
        dataset::TimeSpec::Latest => Ok(crate::TimeSpec::Latest),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::GraphSource;
    use crate::FlureeBuilder;

    #[tokio::test]
    async fn test_build_dataset_view_single() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        let spec = DatasetSpec::new().with_default(GraphSource::new("testdb:main"));
        let dataset = fluree.build_dataset_view(&spec).await.unwrap();

        assert!(dataset.is_single_ledger());
        assert!(dataset.primary().is_some());
    }

    #[tokio::test]
    async fn test_build_dataset_view_multiple() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger1 = fluree.create_ledger("db1").await.unwrap();
        let _ledger2 = fluree.create_ledger("db2").await.unwrap();

        let spec = DatasetSpec::new()
            .with_default(GraphSource::new("db1:main"))
            .with_named(GraphSource::new("db2:main"));

        let dataset = fluree.build_dataset_view(&spec).await.unwrap();

        assert!(!dataset.is_single_ledger());
        assert_eq!(dataset.len(), 2);
    }

    #[tokio::test]
    async fn test_try_single_view_from_spec() {
        let fluree = FlureeBuilder::memory().build_memory();
        let _ledger = fluree.create_ledger("testdb").await.unwrap();

        // Single default, no time spec - should return Some
        let spec = DatasetSpec::new().with_default(GraphSource::new("testdb:main"));
        let result = fluree.try_single_view_from_spec(&spec).await.unwrap();
        assert!(result.is_some());

        // Single default with time spec - should still return Some (single ledger)
        let spec = DatasetSpec::new()
            .with_default(GraphSource::new("testdb:main").with_time(dataset::TimeSpec::AtT(0)));
        let result = fluree.try_single_view_from_spec(&spec).await.unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_is_single_ledger_fast_path() {
        // No time spec - fast path
        let spec = DatasetSpec::new().with_default(GraphSource::new("testdb:main"));
        assert!(
            Fluree::<crate::MemoryStorage, crate::MemoryNameService>::is_single_ledger_fast_path(
                &spec
            )
        );

        // With time spec - not fast path (needs time resolution)
        let spec = DatasetSpec::new()
            .with_default(GraphSource::new("testdb:main").with_time(dataset::TimeSpec::AtT(5)));
        assert!(
            !Fluree::<crate::MemoryStorage, crate::MemoryNameService>::is_single_ledger_fast_path(
                &spec
            )
        );

        // Multiple graphs - not fast path
        let spec = DatasetSpec::new()
            .with_default(GraphSource::new("db1:main"))
            .with_default(GraphSource::new("db2:main"));
        assert!(
            !Fluree::<crate::MemoryStorage, crate::MemoryNameService>::is_single_ledger_fast_path(
                &spec
            )
        );
    }
}
