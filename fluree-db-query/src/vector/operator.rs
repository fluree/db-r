//! Vector Search Operator (Pattern::VectorSearch)
//!
//! This operator executes vector similarity search against a vector index provider
//! and emits bindings for:
//! - idx:id      -> `Binding::IriMatch` (canonical IRI with ledger provenance for cross-ledger joins)
//!   or `Binding::Iri` (if IRI cannot be encoded to SID)
//! - idx:score   -> `Binding::Lit` (xsd:double, similarity score)
//! - idx:ledger  -> `Binding::Lit` (xsd:string; ledger alias) [optional]
//!
//! # Provider Abstraction
//!
//! The `VectorIndexProvider` trait abstracts the vector search backend.
//! Implementations can connect to:
//! - Embedded in-process indexes (requires `vector` feature)
//! - External services (future)
//!
//! # Example Query
//!
//! ```json
//! {
//!   "where": [{
//!     "idx:graph": "embeddings:main",
//!     "idx:vector": [0.1, 0.2, 0.3],
//!     "idx:metric": "cosine",
//!     "idx:limit": 10,
//!     "idx:result": {"idx:id": "?doc", "idx:score": "?score"}
//!   }],
//!   "select": ["?doc", "?score"]
//! }
//! ```

use crate::binding::{Batch, Binding};
use crate::context::{ExecutionContext, WellKnownDatatypes};
use crate::error::{QueryError, Result};
use crate::ir::{VectorSearchPattern, VectorSearchTarget};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::{FlakeValue, NodeCache, Storage};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::DistanceMetric;

/// A single hit from vector search
#[derive(Debug, Clone)]
pub struct VectorSearchHit {
    /// Document IRI
    pub iri: Arc<str>,
    /// Source ledger alias
    pub ledger_alias: Arc<str>,
    /// Similarity score (interpretation depends on metric)
    pub score: f64,
}

impl VectorSearchHit {
    /// Create a new search hit
    pub fn new(iri: impl Into<Arc<str>>, ledger_alias: impl Into<Arc<str>>, score: f64) -> Self {
        Self {
            iri: iri.into(),
            ledger_alias: ledger_alias.into(),
            score,
        }
    }
}

/// Provider for vector similarity search.
///
/// This trait abstracts the vector search backend, allowing different
/// implementations for embedded indexes or external services.
///
/// # Implementors
///
/// - `MockVectorProvider` - for testing
#[async_trait]
pub trait VectorIndexProvider: std::fmt::Debug + Send + Sync {
    /// Search for similar vectors.
    ///
    /// # Arguments
    ///
    /// * `vg_alias` - Virtual graph alias (e.g., "embeddings:main")
    /// * `query_vector` - The query vector to find similar vectors for
    /// * `metric` - Distance metric to use
    /// * `limit` - Maximum number of results
    /// * `as_of_t` - Target transaction time (for time-travel queries)
    ///
    /// In dataset (multi-ledger) mode, there is no meaningful "dataset t".
    /// Callers should pass `None` unless the query provides an unambiguous
    /// as-of anchor (e.g., graph-specific time selection).
    /// * `sync` - Whether to sync before querying
    /// * `timeout_ms` - Query timeout in milliseconds
    ///
    /// # Returns
    ///
    /// Vector of search hits, ordered by similarity (best first).
    async fn search(
        &self,
        vg_alias: &str,
        query_vector: &[f32],
        metric: DistanceMetric,
        limit: usize,
        as_of_t: Option<i64>,
        sync: bool,
        timeout_ms: Option<u64>,
    ) -> Result<Vec<VectorSearchHit>>;

    /// Check if a collection exists for the given VG alias
    async fn collection_exists(&self, vg_alias: &str) -> Result<bool>;
}

/// Vector search operator for `Pattern::VectorSearch`.
pub struct VectorSearchOperator<S: Storage + 'static, C: NodeCache + 'static> {
    /// Child operator providing input solutions (may be EmptyOperator seed)
    child: BoxedOperator<S, C>,
    /// Search pattern
    pattern: VectorSearchPattern,
    /// Output schema (child schema + any new vars from the search result)
    schema: Arc<[VarId]>,
    /// Mapping from variables to output column positions
    out_pos: HashMap<VarId, usize>,
    /// Datatypes for typed literal bindings
    datatypes: WellKnownDatatypes,
    /// State
    state: OperatorState,
}

impl<S: Storage + 'static, C: NodeCache + 'static> VectorSearchOperator<S, C> {
    pub fn new(child: BoxedOperator<S, C>, pattern: VectorSearchPattern) -> Self {
        let child_schema = child.schema();

        // Build output schema: start with child vars, then add id/score/ledger vars if missing.
        let mut schema_vars: Vec<VarId> = child_schema.to_vec();
        let mut seen: HashSet<VarId> = schema_vars.iter().copied().collect();

        // id var is required
        if seen.insert(pattern.id_var) {
            schema_vars.push(pattern.id_var);
        }
        if let Some(v) = pattern.score_var {
            if seen.insert(v) {
                schema_vars.push(v);
            }
        }
        if let Some(v) = pattern.ledger_var {
            if seen.insert(v) {
                schema_vars.push(v);
            }
        }

        let schema: Arc<[VarId]> = Arc::from(schema_vars.into_boxed_slice());
        let out_pos: HashMap<VarId, usize> =
            schema.iter().enumerate().map(|(i, v)| (*v, i)).collect();

        Self {
            child,
            pattern,
            schema,
            out_pos,
            datatypes: WellKnownDatatypes::new(),
            state: OperatorState::Created,
        }
    }

    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    /// Resolve the query vector from the pattern (constant or variable)
    fn resolve_vector_from_row(
        &self,
        _ctx: &ExecutionContext<'_, S, C>,
        row: &crate::binding::RowView<'_>,
    ) -> Result<Option<Vec<f32>>> {
        match &self.pattern.target {
            VectorSearchTarget::Const(vec) => Ok(Some(vec.clone())),
            VectorSearchTarget::Var(v) => match row.get(*v) {
                None | Some(Binding::Unbound) => Ok(None),
                Some(Binding::Poisoned) => Ok(None),
                Some(Binding::Lit { val, .. }) => match val {
                    FlakeValue::Vector(v) => {
                        // Convert f64 to f32
                        Ok(Some(v.iter().map(|x| *x as f32).collect()))
                    }
                    _ => Ok(None),
                },
                Some(Binding::Sid(_))
                | Some(Binding::IriMatch { .. })
                | Some(Binding::Iri(_))
                | Some(Binding::Grouped(_)) => Ok(None),
            },
        }
    }

    fn bind_or_check(existing: Option<&Binding>, candidate: &Binding) -> bool {
        match existing {
            None => true,
            Some(Binding::Unbound) => true,
            Some(Binding::Poisoned) => false,
            Some(other) => other == candidate,
        }
    }
}

#[async_trait]
impl<S: Storage + 'static, C: NodeCache + 'static> Operator<S, C> for VectorSearchOperator<S, C> {
    fn schema(&self) -> &[VarId] {
        self.schema()
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_, S, C>) -> Result<()> {
        self.child.open(ctx).await?;

        let _provider = ctx.vector_provider.ok_or_else(|| {
            QueryError::InvalidQuery(
                "VectorSearch requires ExecutionContext.vector_provider (not configured)"
                    .to_string(),
            )
        })?;

        // If target is a variable, ensure it's available from the child schema.
        if let VectorSearchTarget::Var(v) = &self.pattern.target {
            if !self.child.schema().iter().any(|vv| vv == v) {
                return Err(QueryError::InvalidQuery(format!(
                    "VectorSearch target variable {:?} is not bound by previous patterns",
                    v
                )));
            }
        }

        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_, S, C>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        let provider = ctx.vector_provider.ok_or_else(|| {
            QueryError::InvalidQuery("Vector provider not configured".to_string())
        })?;

        // Pull one child batch; expand each row by vector search results.
        let input_batch = match self.child.next_batch(ctx).await? {
            Some(b) => b,
            None => {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }
        };

        if input_batch.is_empty() {
            return Ok(Some(Batch::empty(self.schema.clone())?));
        }

        // Output columns
        let num_cols = self.schema.len();
        let mut columns: Vec<Vec<Binding>> = (0..num_cols)
            .map(|_| Vec::with_capacity(input_batch.len()))
            .collect();

        let child_schema = self.child.schema();
        let child_cols: Vec<&[Binding]> = (0..child_schema.len())
            .map(|i| {
                input_batch
                    .column_by_idx(i)
                    .expect("child batch schema mismatch")
            })
            .collect();

        let limit = self.pattern.limit.unwrap_or(10);

        for row_idx in 0..input_batch.len() {
            let row_view = input_batch.row_view(row_idx).unwrap();
            let Some(query_vector) = self.resolve_vector_from_row(ctx, &row_view)? else {
                continue;
            };

            // Empty vectors produce no results
            if query_vector.is_empty() {
                continue;
            }

            // Execute vector search
            let results = provider
                .search(
                    &self.pattern.vg_alias,
                    &query_vector,
                    self.pattern.metric,
                    limit,
                    if ctx.dataset.is_some() {
                        None
                    } else {
                        Some(ctx.to_t)
                    },
                    self.pattern.sync,
                    self.pattern.timeout,
                )
                .await?;

            // For each search result, merge with the child row.
            for hit in results {
                // Create IriMatch binding for correct cross-ledger joins.
                // The hit already contains the canonical IRI and ledger alias.
                // IMPORTANT: Encode SID using the hit's source ledger (not primary db)
                // so that primary_sid is consistent with ledger_alias.
                let id_binding = if let Some(sid) =
                    ctx.encode_iri_in_ledger(hit.iri.as_ref(), hit.ledger_alias.as_ref())
                {
                    // Have a valid SID in the hit's source ledger - use IriMatch with full provenance
                    Binding::iri_match(hit.iri.clone(), sid, hit.ledger_alias.clone())
                } else {
                    // Can't encode to SID (IRI not in hit ledger's namespace table) - use raw IRI
                    // This allows the result to participate in IRI-based comparisons
                    // even if it can't be looked up directly. Note: Binding::Iri won't constrain
                    // scans in join substitution (only Sid and IriMatch do), but that's correct
                    // since "cannot encode ⇒ cannot scan anyway".
                    Binding::Iri(hit.iri.clone())
                };

                let score_binding = Binding::lit(
                    FlakeValue::Double(hit.score),
                    self.datatypes.xsd_double.clone(),
                );
                let ledger_binding = Binding::lit(
                    FlakeValue::String(hit.ledger_alias.to_string()),
                    self.datatypes.xsd_string.clone(),
                );

                // Compatibility checks for overlapping vars:
                let existing_id = row_view.get(self.pattern.id_var);
                if !Self::bind_or_check(existing_id, &id_binding) {
                    continue;
                }
                if let Some(v) = self.pattern.score_var {
                    let existing = row_view.get(v);
                    if !Self::bind_or_check(existing, &score_binding) {
                        continue;
                    }
                }
                if let Some(v) = self.pattern.ledger_var {
                    let existing = row_view.get(v);
                    if !Self::bind_or_check(existing, &ledger_binding) {
                        continue;
                    }
                }

                // Emit output row (columnar): start with child columns.
                for (col_idx, &var) in child_schema.iter().enumerate() {
                    let out_idx = *self
                        .out_pos
                        .get(&var)
                        .expect("output schema missing child var");
                    columns[out_idx].push(child_cols[col_idx][row_idx].clone());
                }

                // Add/override id/score/ledger vars
                let id_pos = *self.out_pos.get(&self.pattern.id_var).unwrap();
                columns[id_pos].push(id_binding);

                if let Some(v) = self.pattern.score_var {
                    let pos = *self.out_pos.get(&v).unwrap();
                    columns[pos].push(score_binding);
                }
                if let Some(v) = self.pattern.ledger_var {
                    let pos = *self.out_pos.get(&v).unwrap();
                    columns[pos].push(ledger_binding);
                }
            }
        }

        if columns.first().map(|c| c.is_empty()).unwrap_or(true) {
            return Ok(Some(Batch::empty(self.schema.clone())?));
        }

        Ok(Some(Batch::new(self.schema.clone(), columns)?))
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Conservative: child rows * limit (if set)
        let child = self.child.estimated_rows();
        let lim = self.pattern.limit;
        match (child, lim) {
            (Some(c), Some(l)) => Some(c.saturating_mul(l)),
            (Some(c), None) => Some(c),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execute::build_where_operators_seeded;
    use crate::ir::{Pattern, VectorSearchTarget};
    use crate::seed::EmptyOperator;
    use crate::var_registry::VarRegistry;
    use fluree_db_core::{Db, MemoryStorage, NoCache};
    use std::sync::Mutex;

    #[derive(Debug)]
    struct MockVectorProvider {
        results: Vec<VectorSearchHit>,
        /// Track calls to verify search was invoked correctly
        search_calls: Mutex<Vec<SearchCall>>,
    }

    #[derive(Debug, Clone)]
    struct SearchCall {
        vg_alias: String,
        query_vector: Vec<f32>,
        metric: DistanceMetric,
        limit: usize,
    }

    impl Default for MockVectorProvider {
        fn default() -> Self {
            Self {
                results: Vec::new(),
                search_calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl MockVectorProvider {
        fn with_results(results: Vec<VectorSearchHit>) -> Self {
            Self {
                results,
                search_calls: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl VectorIndexProvider for MockVectorProvider {
        async fn search(
            &self,
            vg_alias: &str,
            query_vector: &[f32],
            metric: DistanceMetric,
            limit: usize,
            _as_of_t: Option<i64>,
            _sync: bool,
            _timeout_ms: Option<u64>,
        ) -> Result<Vec<VectorSearchHit>> {
            // Record the call
            self.search_calls.lock().unwrap().push(SearchCall {
                vg_alias: vg_alias.to_string(),
                query_vector: query_vector.to_vec(),
                metric,
                limit,
            });
            Ok(self.results.iter().take(limit).cloned().collect())
        }

        async fn collection_exists(&self, _vg_alias: &str) -> Result<bool> {
            Ok(true)
        }
    }

    fn make_test_db() -> Db<MemoryStorage, NoCache> {
        let mut db = Db::genesis(MemoryStorage::new(), NoCache, "test/main");
        // Ensure example IRIs used by tests are encodable to SIDs.
        db.namespace_codes
            .insert(100, "http://example.org/".to_string());
        db
    }

    #[test]
    fn test_vector_search_hit() {
        let hit = VectorSearchHit::new("http://example.org/doc1", "ledger:main", 0.95);
        assert_eq!(hit.iri.as_ref(), "http://example.org/doc1");
        assert_eq!(hit.ledger_alias.as_ref(), "ledger:main");
        assert!((hit.score - 0.95).abs() < 0.001);
    }

    #[test]
    fn test_distance_metric_default() {
        assert_eq!(DistanceMetric::default(), DistanceMetric::Cosine);
    }

    #[tokio::test]
    async fn test_vector_operator_constant_target() {
        let db = make_test_db();
        let mut vars = VarRegistry::new();
        let id = vars.get_or_insert("?doc");
        let score = vars.get_or_insert("?score");

        // Create mock provider with some results
        let provider = MockVectorProvider::with_results(vec![
            VectorSearchHit::new("http://example.org/doc1", "ledger:main", 0.95),
            VectorSearchHit::new("http://example.org/doc2", "ledger:main", 0.85),
        ]);

        // Create vector search pattern with constant vector
        let vsp = VectorSearchPattern::new(
            "embeddings:main",
            VectorSearchTarget::Const(vec![0.1, 0.2, 0.3]),
            id,
        )
        .with_metric(DistanceMetric::Cosine)
        .with_score_var(score)
        .with_limit(10);

        let patterns = vec![Pattern::VectorSearch(vsp)];

        // Build operator with explicit seed
        let seed: BoxedOperator<MemoryStorage, NoCache> = Box::new(EmptyOperator::new());
        let mut op =
            build_where_operators_seeded::<MemoryStorage, NoCache>(Some(seed), &patterns, None)
                .expect("build operators");

        let mut ctx = ExecutionContext::new(&db, &vars);
        ctx.vector_provider = Some(&provider);

        op.open(&ctx).await.unwrap();
        let batch = op.next_batch(&ctx).await.unwrap().unwrap();

        // Should have results
        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 2);

        // Schema should include id and score
        assert!(batch.schema().contains(&id));
        assert!(batch.schema().contains(&score));

        // Verify search was called correctly
        let calls = provider.search_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].vg_alias, "embeddings:main");
        assert_eq!(calls[0].query_vector, vec![0.1, 0.2, 0.3]);
        assert_eq!(calls[0].metric, DistanceMetric::Cosine);
        assert_eq!(calls[0].limit, 10);
    }

    #[tokio::test]
    async fn test_vector_operator_empty_results() {
        let db = make_test_db();
        let mut vars = VarRegistry::new();
        let id = vars.get_or_insert("?doc");

        // Empty provider
        let provider = MockVectorProvider::default();

        let vsp = VectorSearchPattern::new(
            "embeddings:main",
            VectorSearchTarget::Const(vec![0.1, 0.2]),
            id,
        );

        let patterns = vec![Pattern::VectorSearch(vsp)];

        let seed: BoxedOperator<MemoryStorage, NoCache> = Box::new(EmptyOperator::new());
        let mut op =
            build_where_operators_seeded::<MemoryStorage, NoCache>(Some(seed), &patterns, None)
                .expect("build operators");

        let mut ctx = ExecutionContext::new(&db, &vars);
        ctx.vector_provider = Some(&provider);

        op.open(&ctx).await.unwrap();
        let batch = op.next_batch(&ctx).await.unwrap().unwrap();

        // Should be empty
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn test_vector_operator_respects_limit() {
        let db = make_test_db();
        let mut vars = VarRegistry::new();
        let id = vars.get_or_insert("?doc");

        // Provider with many results
        let provider = MockVectorProvider::with_results(vec![
            VectorSearchHit::new("http://example.org/doc1", "ledger:main", 0.95),
            VectorSearchHit::new("http://example.org/doc2", "ledger:main", 0.90),
            VectorSearchHit::new("http://example.org/doc3", "ledger:main", 0.85),
            VectorSearchHit::new("http://example.org/doc4", "ledger:main", 0.80),
            VectorSearchHit::new("http://example.org/doc5", "ledger:main", 0.75),
        ]);

        // Limit to 2 results
        let vsp = VectorSearchPattern::new(
            "embeddings:main",
            VectorSearchTarget::Const(vec![0.5, 0.5]),
            id,
        )
        .with_limit(2);

        let patterns = vec![Pattern::VectorSearch(vsp)];

        let seed: BoxedOperator<MemoryStorage, NoCache> = Box::new(EmptyOperator::new());
        let mut op =
            build_where_operators_seeded::<MemoryStorage, NoCache>(Some(seed), &patterns, None)
                .expect("build operators");

        let mut ctx = ExecutionContext::new(&db, &vars);
        ctx.vector_provider = Some(&provider);

        op.open(&ctx).await.unwrap();
        let batch = op.next_batch(&ctx).await.unwrap().unwrap();

        // Should only have 2 results
        assert_eq!(batch.len(), 2);
    }

    #[tokio::test]
    async fn test_vector_operator_missing_provider_error() {
        let db = make_test_db();
        let mut vars = VarRegistry::new();
        let id = vars.get_or_insert("?doc");

        let vsp = VectorSearchPattern::new(
            "embeddings:main",
            VectorSearchTarget::Const(vec![0.1, 0.2]),
            id,
        );

        let patterns = vec![Pattern::VectorSearch(vsp)];

        let seed: BoxedOperator<MemoryStorage, NoCache> = Box::new(EmptyOperator::new());
        let mut op =
            build_where_operators_seeded::<MemoryStorage, NoCache>(Some(seed), &patterns, None)
                .expect("build operators");

        // No vector_provider set
        let ctx = ExecutionContext::new(&db, &vars);

        let err = op.open(&ctx).await.unwrap_err();
        assert!(err.to_string().contains("vector_provider"));
    }

    #[tokio::test]
    async fn test_vector_operator_with_ledger_var() {
        let db = make_test_db();
        let mut vars = VarRegistry::new();
        let id = vars.get_or_insert("?doc");
        let ledger = vars.get_or_insert("?source");

        let provider = MockVectorProvider::with_results(vec![VectorSearchHit::new(
            "http://example.org/doc1",
            "docs:main",
            0.9,
        )]);

        let vsp =
            VectorSearchPattern::new("embeddings:main", VectorSearchTarget::Const(vec![0.5]), id)
                .with_ledger_var(ledger);

        let patterns = vec![Pattern::VectorSearch(vsp)];

        let seed: BoxedOperator<MemoryStorage, NoCache> = Box::new(EmptyOperator::new());
        let mut op =
            build_where_operators_seeded::<MemoryStorage, NoCache>(Some(seed), &patterns, None)
                .expect("build operators");

        let mut ctx = ExecutionContext::new(&db, &vars);
        ctx.vector_provider = Some(&provider);

        op.open(&ctx).await.unwrap();
        let batch = op.next_batch(&ctx).await.unwrap().unwrap();

        // Should have ledger var in schema
        assert!(batch.schema().contains(&ledger));
    }
}
