//! Intermediate Representation for query execution
//!
//! This module provides the logical pattern IR that represents query structure.
//! The planner transforms this IR into physical operators.
//!
//! # Design
//!
//! - `Query` is the top-level structure with `select` and ordered `where_` patterns
//! - `Pattern` enum mirrors the where clause structure, preserving order for filter inlining
//! - The planner chooses physical join operators based on pattern analysis

use crate::binding::Binding;
use crate::pattern::{Term, TriplePattern};
use crate::sort::SortSpec;
use crate::var_registry::VarId;
use fluree_db_core::Sid;

/// Top-level query structure
///
/// Represents a parsed and lowered query ready for planning.
#[derive(Debug, Clone)]
pub struct Query {
    /// Selected variables (output columns)
    pub select: Vec<VarId>,
    /// Where clause patterns (ordered!)
    pub where_: Vec<Pattern>,
}

impl Query {
    /// Create a new query
    pub fn new(select: Vec<VarId>, where_: Vec<Pattern>) -> Self {
        Self { select, where_ }
    }

    /// Create a query with a single triple pattern
    pub fn single(select: Vec<VarId>, pattern: TriplePattern) -> Self {
        Self {
            select,
            where_: vec![Pattern::Triple(pattern)],
        }
    }

    /// Get all triple patterns in the where clause (flattening nested structures)
    pub fn triple_patterns(&self) -> Vec<&TriplePattern> {
        fn collect<'a>(patterns: &'a [Pattern], out: &mut Vec<&'a TriplePattern>) {
            for p in patterns {
                match p {
                    Pattern::Triple(tp) => out.push(tp),
                    Pattern::Optional(inner)
                    | Pattern::Minus(inner)
                    | Pattern::Exists(inner)
                    | Pattern::NotExists(inner) => collect(inner, out),
                    Pattern::Union(branches) => {
                        for branch in branches {
                            collect(branch, out);
                        }
                    }
                    Pattern::Graph { patterns, .. } => collect(patterns, out),
                    Pattern::Filter(_)
                    | Pattern::Bind { .. }
                    | Pattern::Values { .. }
                    | Pattern::PropertyPath(_)
                    | Pattern::Subquery(_)
                    | Pattern::IndexSearch(_)
                    | Pattern::VectorSearch(_)
                    | Pattern::R2rml(_) => {}
                }
            }
        }

        let mut result = Vec::new();
        collect(&self.where_, &mut result);
        result
    }
}

// ============================================================================
// Graph crawl select types (resolved)
// ============================================================================

/// Root of a graph crawl select (resolved)
///
/// Supports both variable and IRI constant roots:
/// - Variable root: from query results (e.g., `?person`)
/// - IRI constant root: direct subject fetch (e.g., `ex:alice`)
#[derive(Debug, Clone, PartialEq)]
pub enum Root {
    /// Variable root - value comes from query results
    Var(VarId),
    /// IRI constant root - direct subject lookup
    Sid(Sid),
}

/// Nested selection specification for sub-crawls (resolved)
///
/// This type captures the full selection state for nested property expansion,
/// including both forward and reverse properties.
#[derive(Debug, Clone, PartialEq)]
pub struct NestedSelectSpec {
    /// Forward property selections
    pub forward: Vec<SelectionSpec>,
    /// Reverse property selections (predicate Sid → optional nested spec)
    /// None means no sub-selections (just return @id), Some means nested expansion
    pub reverse: std::collections::HashMap<Sid, Option<Box<NestedSelectSpec>>>,
    /// Whether wildcard was specified at this level
    pub has_wildcard: bool,
}

impl NestedSelectSpec {
    /// Create a new nested select spec
    pub fn new(
        forward: Vec<SelectionSpec>,
        reverse: std::collections::HashMap<Sid, Option<Box<NestedSelectSpec>>>,
        has_wildcard: bool,
    ) -> Self {
        Self {
            forward,
            reverse,
            has_wildcard,
        }
    }

    /// Check if this spec is empty (no selections)
    pub fn is_empty(&self) -> bool {
        self.forward.is_empty() && self.reverse.is_empty()
    }
}

/// Selection specification for graph crawl (resolved)
///
/// Defines what properties to include at each level of expansion.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectionSpec {
    /// Explicit @id selection (include @id even when wildcard is not specified)
    Id,
    /// Wildcard - select all properties at this level
    Wildcard,
    /// Property selection with optional nested expansion
    Property {
        /// Predicate Sid
        predicate: Sid,
        /// Optional nested selection spec for expanding this property's values
        /// Uses Box to avoid infinite type recursion
        sub_spec: Option<Box<NestedSelectSpec>>,
    },
}

/// Graph crawl selection specification (resolved, with Sids)
///
/// This is the resolved form of `UnresolvedGraphSelectSpec`, with all IRIs
/// encoded as Sids. Used during result formatting for nested JSON-LD output.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphSelectSpec {
    /// Root of the crawl - variable or IRI constant
    pub root: Root,
    /// Forward property selections
    pub selections: Vec<SelectionSpec>,
    /// Reverse property selections (predicate Sid → optional nested spec)
    /// None means no sub-selections (just return @id), Some means nested expansion
    pub reverse: std::collections::HashMap<Sid, Option<Box<NestedSelectSpec>>>,
    /// Max depth for auto-expansion (0 = no auto-expand)
    pub depth: usize,
    /// Whether wildcard was specified (controls @id inclusion)
    pub has_wildcard: bool,
}

impl GraphSelectSpec {
    /// Create a new graph select spec
    pub fn new(root: Root, selections: Vec<SelectionSpec>) -> Self {
        let has_wildcard = selections
            .iter()
            .any(|s| matches!(s, SelectionSpec::Wildcard));
        Self {
            root,
            selections,
            reverse: std::collections::HashMap::new(),
            depth: 0,
            has_wildcard,
        }
    }

    /// Generate a hash for cache keying purposes
    ///
    /// Used to differentiate the same Sid expanded under different specs.
    /// The cache key is `(Sid, spec_hash, depth_remaining)`.
    pub fn spec_hash(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();

        self.has_wildcard.hash(&mut hasher);
        self.selections.len().hash(&mut hasher);

        // Hash selection structure
        fn hash_selection(spec: &SelectionSpec, hasher: &mut impl Hasher) {
            match spec {
                SelectionSpec::Id => {
                    2u8.hash(hasher);
                }
                SelectionSpec::Wildcard => {
                    0u8.hash(hasher);
                }
                SelectionSpec::Property {
                    predicate,
                    sub_spec,
                } => {
                    1u8.hash(hasher);
                    predicate.hash(hasher);
                    if let Some(nested) = sub_spec {
                        // Hash nested spec
                        nested.has_wildcard.hash(hasher);
                        nested.forward.len().hash(hasher);
                        for sub in &nested.forward {
                            hash_selection(sub, hasher);
                        }
                        // Hash nested reverse
                        nested.reverse.len().hash(hasher);
                        let mut nested_rev_keys: Vec<_> = nested.reverse.keys().collect();
                        nested_rev_keys.sort();
                        for key in nested_rev_keys {
                            key.hash(hasher);
                            if let Some(nested_nested) = nested.reverse.get(key) {
                                if let Some(spec) = nested_nested {
                                    1u8.hash(hasher);
                                    hash_nested_spec(spec, hasher);
                                } else {
                                    0u8.hash(hasher);
                                }
                            }
                        }
                    } else {
                        0usize.hash(hasher);
                    }
                }
            }
        }

        fn hash_nested_spec(spec: &NestedSelectSpec, hasher: &mut impl Hasher) {
            spec.has_wildcard.hash(hasher);
            spec.forward.len().hash(hasher);
            for sub in &spec.forward {
                hash_selection(sub, hasher);
            }
            spec.reverse.len().hash(hasher);
            let mut rev_keys: Vec<_> = spec.reverse.keys().collect();
            rev_keys.sort();
            for key in rev_keys {
                key.hash(hasher);
                if let Some(nested) = spec.reverse.get(key) {
                    if let Some(inner) = nested {
                        1u8.hash(hasher);
                        hash_nested_spec(inner, hasher);
                    } else {
                        0u8.hash(hasher);
                    }
                }
            }
        }

        for sel in &self.selections {
            hash_selection(sel, &mut hasher);
        }

        // Hash reverse properties
        self.reverse.len().hash(&mut hasher);
        // Sort by Sid for deterministic hashing
        let mut reverse_keys: Vec<_> = self.reverse.keys().collect();
        reverse_keys.sort();
        for key in reverse_keys {
            key.hash(&mut hasher);
            if let Some(nested) = self.reverse.get(key) {
                if let Some(spec) = nested {
                    1u8.hash(&mut hasher);
                    hash_nested_spec(spec, &mut hasher);
                } else {
                    0u8.hash(&mut hasher);
                }
            }
        }

        hasher.finish()
    }
}

/// Property path modifier (transitive operators)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathModifier {
    /// + : one or more (at least one hop)
    OneOrMore,
    /// * : zero or more (includes starting node)
    ZeroOrMore,
}

/// Resolved property path pattern for transitive traversal.
///
/// Produced by `@path` aliases with `+` or `*` modifiers, e.g.:
/// `{"@context": {"knowsPlus": {"@path": "ex:knows+"}}, "where": [{"@id": "ex:alice", "knowsPlus": "?who"}]}`
#[derive(Debug, Clone)]
pub struct PropertyPathPattern {
    /// Subject term (Var or Sid - literals not allowed)
    pub subject: Term,
    /// Predicate to traverse (always resolved to Sid)
    pub predicate: Sid,
    /// Path modifier (+ or *)
    pub modifier: PathModifier,
    /// Object term (Var or Sid - literals not allowed)
    pub object: Term,
}

impl PropertyPathPattern {
    /// Create a new property path pattern
    pub fn new(subject: Term, predicate: Sid, modifier: PathModifier, object: Term) -> Self {
        Self {
            subject,
            predicate,
            modifier,
            object,
        }
    }

    /// Get variables from subject and object
    pub fn variables(&self) -> Vec<VarId> {
        let mut vars = Vec::with_capacity(2);
        if let Term::Var(v) = &self.subject {
            vars.push(*v);
        }
        if let Term::Var(v) = &self.object {
            vars.push(*v);
        }
        vars
    }
}

/// Resolved subquery pattern
///
/// Represents a nested query within a WHERE clause. The subquery's results
/// are merged with the parent solution on shared variables (correlated join).
///
/// Syntax: `["query", { "select": [...], "where": {...}, ... }]`
#[derive(Debug, Clone)]
pub struct SubqueryPattern {
    /// Variables to select from the subquery (output schema)
    pub select: Vec<VarId>,
    /// WHERE patterns of the subquery
    pub patterns: Vec<Pattern>,
    /// Limit on results (None = unlimited)
    pub limit: Option<usize>,
    /// Offset to skip (None = 0)
    pub offset: Option<usize>,
    /// Whether to apply DISTINCT to results
    pub distinct: bool,
    /// ORDER BY specifications
    pub order_by: Vec<SortSpec>,
    /// GROUP BY variables (for aggregates)
    pub group_by: Vec<VarId>,
    /// Aggregate specifications
    pub aggregates: Vec<crate::aggregate::AggregateSpec>,
    /// HAVING filter (post-aggregate)
    pub having: Option<FilterExpr>,
}

impl SubqueryPattern {
    /// Create a new subquery pattern
    pub fn new(select: Vec<VarId>, patterns: Vec<Pattern>) -> Self {
        Self {
            select,
            patterns,
            limit: None,
            offset: None,
            distinct: false,
            order_by: Vec::new(),
            group_by: Vec::new(),
            aggregates: Vec::new(),
            having: None,
        }
    }

    /// Set limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set offset
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Set distinct
    pub fn with_distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Set ORDER BY specifications
    pub fn with_order_by(mut self, specs: Vec<SortSpec>) -> Self {
        self.order_by = specs;
        self
    }

    /// Set GROUP BY variables
    pub fn with_group_by(mut self, vars: Vec<VarId>) -> Self {
        self.group_by = vars;
        self
    }

    /// Set aggregate specifications
    pub fn with_aggregates(mut self, specs: Vec<crate::aggregate::AggregateSpec>) -> Self {
        self.aggregates = specs;
        self
    }

    /// Get variables from the select list
    pub fn variables(&self) -> Vec<VarId> {
        self.select.clone()
    }
}

// ============================================================================
// Index Search Pattern (BM25 Full-Text Search)
// ============================================================================

/// Index search pattern for BM25 full-text queries.
///
/// Represents a search against a virtual graph (e.g., BM25 index) with
/// result bindings for document ID, score, and optional ledger alias.
///
/// # Example Query Syntax
///
/// Direct variable result:
/// ```json
/// {
///   "graph": "my-search:main",
///   "idx:target": "software engineer",
///   "idx:limit": 10,
///   "idx:result": "?doc"
/// }
/// ```
///
/// Nested result with score:
/// ```json
/// {
///   "graph": "my-search:main",
///   "idx:target": "software engineer",
///   "idx:result": {
///     "idx:id": "?doc",
///     "idx:score": "?score",
///     "idx:ledger": "?source"
///   }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct IndexSearchPattern {
    /// Virtual graph alias (e.g., "my-search:main")
    pub vg_alias: String,

    /// Search query target - can be a constant string or variable
    pub target: IndexSearchTarget,

    /// Maximum number of results (optional)
    pub limit: Option<usize>,

    /// Variable to bind the document IRI (required)
    pub id_var: VarId,

    /// Variable to bind the BM25 score (optional)
    pub score_var: Option<VarId>,

    /// Variable to bind the source ledger alias (optional, for multi-ledger)
    pub ledger_var: Option<VarId>,

    /// Whether to sync before query (default: false)
    pub sync: bool,

    /// Query timeout in milliseconds (optional)
    pub timeout: Option<u64>,
}

/// Target for index search - can be a constant query string or variable.
#[derive(Debug, Clone)]
pub enum IndexSearchTarget {
    /// Constant search query string
    Const(String),
    /// Variable reference (bound at runtime)
    Var(VarId),
}

impl IndexSearchPattern {
    /// Create a new index search pattern with just ID binding
    pub fn new(vg_alias: impl Into<String>, target: IndexSearchTarget, id_var: VarId) -> Self {
        Self {
            vg_alias: vg_alias.into(),
            target,
            limit: None,
            id_var,
            score_var: None,
            ledger_var: None,
            sync: false,
            timeout: None,
        }
    }

    /// Set the result limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the score binding variable
    pub fn with_score_var(mut self, var: VarId) -> Self {
        self.score_var = Some(var);
        self
    }

    /// Set the ledger binding variable
    pub fn with_ledger_var(mut self, var: VarId) -> Self {
        self.ledger_var = Some(var);
        self
    }

    /// Set sync mode
    pub fn with_sync(mut self, sync: bool) -> Self {
        self.sync = sync;
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, timeout: u64) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Get all variables referenced by this pattern
    pub fn variables(&self) -> Vec<VarId> {
        let mut vars = vec![self.id_var];

        if let IndexSearchTarget::Var(v) = &self.target {
            vars.push(*v);
        }

        if let Some(v) = self.score_var {
            vars.push(v);
        }

        if let Some(v) = self.ledger_var {
            vars.push(v);
        }

        vars
    }
}

// ============================================================================
// Vector Search Pattern
// ============================================================================

/// Vector similarity search pattern for querying vector virtual graphs.
///
/// # Example
///
/// Simple search with constant vector:
/// ```json
/// {
///   "idx:graph": "embeddings:main",
///   "idx:vector": [0.1, 0.2, 0.3],
///   "idx:metric": "cosine",
///   "idx:limit": 10,
///   "idx:result": "?doc"
/// }
/// ```
///
/// Search with variable vector:
/// ```json
/// {
///   "idx:graph": "embeddings:main",
///   "idx:vector": "?queryVec",
///   "idx:metric": "dot",
///   "idx:result": {
///     "idx:id": "?doc",
///     "idx:score": "?score"
///   }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct VectorSearchPattern {
    /// Virtual graph alias (e.g., "embeddings:main")
    pub vg_alias: String,

    /// Search target - can be a constant vector or variable
    pub target: VectorSearchTarget,

    /// Distance metric for similarity search
    pub metric: crate::vector::DistanceMetric,

    /// Maximum number of results (optional, defaults to 10)
    pub limit: Option<usize>,

    /// Variable to bind the document IRI (required)
    pub id_var: VarId,

    /// Variable to bind the similarity score (optional)
    pub score_var: Option<VarId>,

    /// Variable to bind the source ledger alias (optional, for multi-ledger)
    pub ledger_var: Option<VarId>,

    /// Whether to sync before query (default: false)
    pub sync: bool,

    /// Query timeout in milliseconds (optional)
    pub timeout: Option<u64>,
}

/// Target for vector search - can be a constant vector or variable.
#[derive(Debug, Clone)]
pub enum VectorSearchTarget {
    /// Constant vector (f32 for efficiency)
    Const(Vec<f32>),
    /// Variable reference (bound at runtime to a fluree:vector value)
    Var(VarId),
}

impl VectorSearchPattern {
    /// Create a new vector search pattern with just ID binding
    pub fn new(vg_alias: impl Into<String>, target: VectorSearchTarget, id_var: VarId) -> Self {
        Self {
            vg_alias: vg_alias.into(),
            target,
            metric: crate::vector::DistanceMetric::default(),
            limit: None,
            id_var,
            score_var: None,
            ledger_var: None,
            sync: false,
            timeout: None,
        }
    }

    /// Set the distance metric
    pub fn with_metric(mut self, metric: crate::vector::DistanceMetric) -> Self {
        self.metric = metric;
        self
    }

    /// Set the result limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the score binding variable
    pub fn with_score_var(mut self, var: VarId) -> Self {
        self.score_var = Some(var);
        self
    }

    /// Set the ledger binding variable
    pub fn with_ledger_var(mut self, var: VarId) -> Self {
        self.ledger_var = Some(var);
        self
    }

    /// Set sync mode
    pub fn with_sync(mut self, sync: bool) -> Self {
        self.sync = sync;
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, timeout: u64) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Get all variables referenced by this pattern
    pub fn variables(&self) -> Vec<VarId> {
        let mut vars = vec![self.id_var];

        if let VectorSearchTarget::Var(v) = &self.target {
            vars.push(*v);
        }

        if let Some(v) = self.score_var {
            vars.push(v);
        }

        if let Some(v) = self.ledger_var {
            vars.push(v);
        }

        vars
    }
}

// ============================================================================
// R2RML Pattern
// ============================================================================

/// R2RML scan pattern for querying Iceberg virtual graphs via R2RML mappings.
///
/// This pattern scans an Iceberg table through R2RML term maps and produces
/// RDF term bindings for subject and optional object variables.
///
/// # Example Query Pattern
///
/// ```sparql
/// ?person a ex:Person .
/// ?person ex:name ?name .
/// ```
///
/// With an R2RML mapping, this could be lowered to:
/// - R2rmlPattern with subject_var=?person, triples_map for ex:Person class
/// - R2rmlPattern with subject_var=?person, object_var=?name, predicate ex:name
#[derive(Debug, Clone)]
pub struct R2rmlPattern {
    /// Virtual graph alias (e.g., "airlines-vg:main")
    pub vg_alias: String,

    /// Variable to bind the subject IRI
    pub subject_var: VarId,

    /// Variable to bind the object value (optional)
    ///
    /// If None, this pattern only materializes subjects (e.g., for rdf:type patterns).
    pub object_var: Option<VarId>,

    /// Specific TriplesMap IRI to use (optional)
    ///
    /// If provided, only this TriplesMap is scanned. Otherwise, the planner
    /// selects appropriate TriplesMap(s) based on class/predicate filters.
    pub triples_map_iri: Option<String>,

    /// Predicate IRI filter (optional)
    ///
    /// Limits scan to PredicateObjectMaps with this predicate.
    pub predicate_filter: Option<String>,

    /// Subject class filter (optional)
    ///
    /// Limits scan to TriplesMap(s) that produce this rdf:type.
    pub class_filter: Option<String>,
}

impl R2rmlPattern {
    /// Create a new R2RML pattern with subject and object variables.
    pub fn new(vg_alias: impl Into<String>, subject_var: VarId, object_var: Option<VarId>) -> Self {
        Self {
            vg_alias: vg_alias.into(),
            subject_var,
            object_var,
            triples_map_iri: None,
            predicate_filter: None,
            class_filter: None,
        }
    }

    /// Set the specific TriplesMap IRI to use.
    pub fn with_triples_map(mut self, iri: impl Into<String>) -> Self {
        self.triples_map_iri = Some(iri.into());
        self
    }

    /// Set the predicate filter.
    pub fn with_predicate(mut self, predicate: impl Into<String>) -> Self {
        self.predicate_filter = Some(predicate.into());
        self
    }

    /// Set the class filter.
    pub fn with_class(mut self, class: impl Into<String>) -> Self {
        self.class_filter = Some(class.into());
        self
    }

    /// Get all variables referenced by this pattern.
    pub fn variables(&self) -> Vec<VarId> {
        let mut vars = vec![self.subject_var];
        if let Some(obj_var) = self.object_var {
            vars.push(obj_var);
        }
        vars
    }
}

// ============================================================================
// Graph Pattern Types
// ============================================================================

/// Graph name in a GRAPH pattern - use strings, not Sids
///
/// Graph names in datasets are ledger aliases/IRIs, not guaranteed
/// to be encodable via any single DB's namespace table.
#[derive(Debug, Clone, PartialEq)]
pub enum GraphName {
    /// Concrete graph IRI (string, not Sid)
    Iri(std::sync::Arc<str>),
    /// Variable (iterates all named graphs, binds as IRI string)
    Var(VarId),
}

impl GraphName {
    /// Check if this is a variable
    pub fn is_var(&self) -> bool {
        matches!(self, GraphName::Var(_))
    }

    /// Get the variable ID if this is a variable
    pub fn as_var(&self) -> Option<VarId> {
        match self {
            GraphName::Var(v) => Some(*v),
            GraphName::Iri(_) => None,
        }
    }

    /// Get the IRI if this is a concrete graph
    pub fn as_iri(&self) -> Option<&str> {
        match self {
            GraphName::Iri(iri) => Some(iri),
            GraphName::Var(_) => None,
        }
    }
}

// ============================================================================
// Pattern Enum
// ============================================================================

/// Logical pattern IR - mirrors where clause structure
///
/// Each variant represents a different pattern type in the query.
/// Ordering is preserved to enable filter inlining at the correct position.
#[derive(Debug, Clone)]
pub enum Pattern {
    /// A basic triple pattern (subject, predicate, object)
    Triple(TriplePattern),

    /// A filter expression to evaluate against each solution
    /// Positioned in where clause order for inline attachment
    Filter(FilterExpr),

    /// Optional clause - left join semantics
    /// Contains ordered patterns that may or may not match
    Optional(Vec<Pattern>),

    /// Union of pattern branches - any branch may match
    Union(Vec<Vec<Pattern>>),

    /// Bind a computed value to a variable
    Bind { var: VarId, expr: FilterExpr },

    /// Inline values - constant rows to join with
    Values {
        vars: Vec<VarId>,
        rows: Vec<Vec<Binding>>,
    },

    /// MINUS clause - anti-join semantics (set difference)
    /// Contains patterns to match; solutions matching these are removed
    Minus(Vec<Pattern>),

    /// EXISTS clause - filter rows where subquery matches
    Exists(Vec<Pattern>),

    /// NOT EXISTS clause - filter rows where subquery does NOT match
    NotExists(Vec<Pattern>),

    /// Property path pattern (transitive traversal)
    PropertyPath(PropertyPathPattern),

    /// Subquery pattern - nested query with result merging
    ///
    /// Executes a nested query and merges results with the parent solution.
    /// Shared variables are correlated (joined on matching values).
    Subquery(SubqueryPattern),

    /// Index search pattern - BM25 full-text search against a virtual graph
    ///
    /// Queries a virtual graph (e.g., BM25 index) and produces result bindings.
    IndexSearch(IndexSearchPattern),

    /// Vector search pattern - similarity search against a vector virtual graph
    ///
    /// Queries a vector index and produces result bindings.
    VectorSearch(VectorSearchPattern),

    /// R2RML scan pattern - queries Iceberg virtual graph via R2RML mappings
    ///
    /// Scans Iceberg tables through R2RML term maps and produces RDF bindings.
    R2rml(R2rmlPattern),

    /// Named graph pattern - scopes inner patterns to a specific graph
    ///
    /// SPARQL: `GRAPH <iri> { ... }` or `GRAPH ?g { ... }`
    ///
    /// Semantics:
    /// - `GraphName::Iri(s)`: Execute inner patterns against that specific named graph
    /// - `GraphName::Var(v)`: If bound, use that graph; if unbound, iterate all named
    ///   graphs and bind `?v` to each graph IRI
    ///
    /// Graph-not-found produces empty result (Clojure parity), not an error.
    Graph {
        /// The graph name (concrete IRI or variable)
        name: GraphName,
        /// Inner patterns to execute within the graph context
        patterns: Vec<Pattern>,
    },
}

impl Pattern {
    /// Check if this is a triple pattern
    pub fn is_triple(&self) -> bool {
        matches!(self, Pattern::Triple(_))
    }

    /// Get the triple pattern if this is a Triple
    pub fn as_triple(&self) -> Option<&TriplePattern> {
        match self {
            Pattern::Triple(tp) => Some(tp),
            _ => None,
        }
    }

    /// Get all variables referenced by this pattern
    pub fn variables(&self) -> Vec<VarId> {
        match self {
            Pattern::Triple(tp) => tp.variables(),
            Pattern::Filter(expr) => expr.variables(),
            Pattern::Optional(inner) => inner.iter().flat_map(|p| p.variables()).collect(),
            Pattern::Union(branches) => branches
                .iter()
                .flat_map(|branch| branch.iter().flat_map(|p| p.variables()))
                .collect(),
            Pattern::Bind { var, expr } => {
                let mut vars = expr.variables();
                vars.push(*var);
                vars
            }
            Pattern::Values { vars, .. } => vars.clone(),
            Pattern::Minus(inner) | Pattern::Exists(inner) | Pattern::NotExists(inner) => {
                inner.iter().flat_map(|p| p.variables()).collect()
            }
            Pattern::PropertyPath(pp) => pp.variables(),
            Pattern::Subquery(sq) => sq.variables(),
            Pattern::IndexSearch(isp) => isp.variables(),
            Pattern::VectorSearch(vsp) => vsp.variables(),
            Pattern::R2rml(r2rml) => r2rml.variables(),
            Pattern::Graph { name, patterns } => {
                let mut vars = patterns
                    .iter()
                    .flat_map(|p| p.variables())
                    .collect::<Vec<_>>();
                if let GraphName::Var(v) = name {
                    vars.push(*v);
                }
                vars
            }
        }
    }
}

/// Filter expression AST
///
/// Represents expressions that can be evaluated against solution bindings.
/// This is a placeholder that will be expanded in Phase 3 (Filter Implementation).
#[derive(Debug, Clone, PartialEq)]
pub enum FilterExpr {
    /// Variable reference
    Var(VarId),
    /// Constant value
    Const(FilterValue),
    /// Comparison operation
    Compare {
        op: CompareOp,
        left: Box<FilterExpr>,
        right: Box<FilterExpr>,
    },
    /// Arithmetic operation (+, -, *, /)
    Arithmetic {
        op: ArithmeticOp,
        left: Box<FilterExpr>,
        right: Box<FilterExpr>,
    },
    /// Unary negation (-)
    Negate(Box<FilterExpr>),
    /// Logical AND
    And(Vec<FilterExpr>),
    /// Logical OR
    Or(Vec<FilterExpr>),
    /// Logical NOT
    Not(Box<FilterExpr>),
    /// Conditional expression (IF(cond, then, else))
    If {
        condition: Box<FilterExpr>,
        then_expr: Box<FilterExpr>,
        else_expr: Box<FilterExpr>,
    },
    /// IN expression (?x IN (1, 2, 3))
    In {
        expr: Box<FilterExpr>,
        values: Vec<FilterExpr>,
        negated: bool,
    },
    /// Function call
    Function {
        name: FunctionName,
        args: Vec<FilterExpr>,
    },
}

impl FilterExpr {
    /// Get all variables referenced by this expression
    pub fn variables(&self) -> Vec<VarId> {
        match self {
            FilterExpr::Var(v) => vec![*v],
            FilterExpr::Const(_) => vec![],
            FilterExpr::Compare { left, right, .. }
            | FilterExpr::Arithmetic { left, right, .. } => {
                let mut vars = left.variables();
                vars.extend(right.variables());
                vars
            }
            FilterExpr::Negate(expr) | FilterExpr::Not(expr) => expr.variables(),
            FilterExpr::And(exprs) | FilterExpr::Or(exprs) => {
                exprs.iter().flat_map(|e| e.variables()).collect()
            }
            FilterExpr::If {
                condition,
                then_expr,
                else_expr,
            } => {
                let mut vars = condition.variables();
                vars.extend(then_expr.variables());
                vars.extend(else_expr.variables());
                vars
            }
            FilterExpr::In { expr, values, .. } => {
                let mut vars = expr.variables();
                vars.extend(values.iter().flat_map(|v| v.variables()));
                vars
            }
            FilterExpr::Function { args, .. } => args.iter().flat_map(|a| a.variables()).collect(),
        }
    }

    /// Returns Some(var) if filter references exactly one variable
    ///
    /// Used to determine if a filter can be attached inline to a pattern.
    pub fn single_var(&self) -> Option<VarId> {
        let vars = self.variables();
        let unique: std::collections::HashSet<_> = vars.into_iter().collect();
        if unique.len() == 1 {
            unique.into_iter().next()
        } else {
            None
        }
    }

    /// Returns true if this filter can be pushed down to index scans as range bounds.
    ///
    /// "Range-safe" filters can be converted to contiguous range constraints on the
    /// object position of index scans, enabling early filtering at the storage layer
    /// rather than post-scan filtering in the operator pipeline.
    ///
    /// # Accepted patterns
    ///
    /// - **Simple comparisons** (`<`, `<=`, `>`, `>=`, `=`) between a variable and constant
    /// - **Conjunctions** (`AND`) of range-safe expressions
    ///
    /// # Rejected patterns (NOT range-safe)
    ///
    /// - `!=` (not-equal) - cannot be represented as a contiguous range
    /// - `OR` - would require multiple disjoint ranges
    /// - `NOT` - negation cannot be efficiently bounded
    /// - Arithmetic expressions - require evaluation, not just bounds
    /// - Function calls - require runtime evaluation
    /// - `IN` clauses - multiple discrete values, not a range
    /// - Variable-to-variable comparisons - no constant bound available
    ///
    /// # Usage
    ///
    /// Filters that are range-safe are extracted during query planning and converted
    /// to `ObjectBounds` for index scans. Non-range-safe filters are applied as
    /// `FilterOperator` nodes after the scan completes.
    ///
    /// # Example
    ///
    /// ```text
    /// FILTER(?age > 18 AND ?age < 65)  -> range-safe (becomes scan bounds)
    /// FILTER(?age != 30)               -> NOT range-safe (post-scan filter)
    /// FILTER(?x > ?y)                  -> NOT range-safe (no constant bound)
    /// ```
    pub fn is_range_safe(&self) -> bool {
        match self {
            FilterExpr::Compare { op, left, right } => {
                // Not-equal cannot be represented as a single contiguous range.
                if matches!(op, CompareOp::Ne) {
                    return false;
                }
                // Only var vs const comparisons are range-safe
                matches!(
                    (left.as_ref(), right.as_ref()),
                    (FilterExpr::Var(_), FilterExpr::Const(_))
                        | (FilterExpr::Const(_), FilterExpr::Var(_))
                )
            }
            FilterExpr::And(exprs) => exprs.iter().all(|e| e.is_range_safe()),
            // Arithmetic, Or, Not, If, In, functions are NOT range-safe
            FilterExpr::Arithmetic { .. }
            | FilterExpr::Negate(_)
            | FilterExpr::Or(_)
            | FilterExpr::Not(_)
            | FilterExpr::If { .. }
            | FilterExpr::In { .. }
            | FilterExpr::Function { .. }
            | FilterExpr::Var(_)
            | FilterExpr::Const(_) => false,
        }
    }
}

/// Comparison operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// Arithmetic operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithmeticOp {
    Add,
    Sub,
    Mul,
    Div,
}

/// Constant value in filter expressions
#[derive(Debug, Clone, PartialEq)]
pub enum FilterValue {
    Long(i64),
    Double(f64),
    String(String),
    Bool(bool),
    /// Temporal or duration value (wraps any temporal/duration FlakeValue)
    Temporal(fluree_db_core::value::FlakeValue),
}

// =============================================================================
// From implementations for lowering unresolved AST types
// =============================================================================

impl From<crate::parse::ast::UnresolvedCompareOp> for CompareOp {
    fn from(op: crate::parse::ast::UnresolvedCompareOp) -> Self {
        use crate::parse::ast::UnresolvedCompareOp;
        match op {
            UnresolvedCompareOp::Eq => CompareOp::Eq,
            UnresolvedCompareOp::Ne => CompareOp::Ne,
            UnresolvedCompareOp::Lt => CompareOp::Lt,
            UnresolvedCompareOp::Le => CompareOp::Le,
            UnresolvedCompareOp::Gt => CompareOp::Gt,
            UnresolvedCompareOp::Ge => CompareOp::Ge,
        }
    }
}

impl From<crate::parse::ast::UnresolvedArithmeticOp> for ArithmeticOp {
    fn from(op: crate::parse::ast::UnresolvedArithmeticOp) -> Self {
        use crate::parse::ast::UnresolvedArithmeticOp;
        match op {
            UnresolvedArithmeticOp::Add => ArithmeticOp::Add,
            UnresolvedArithmeticOp::Sub => ArithmeticOp::Sub,
            UnresolvedArithmeticOp::Mul => ArithmeticOp::Mul,
            UnresolvedArithmeticOp::Div => ArithmeticOp::Div,
        }
    }
}

impl From<&crate::parse::ast::UnresolvedFilterValue> for FilterValue {
    fn from(val: &crate::parse::ast::UnresolvedFilterValue) -> Self {
        use crate::parse::ast::UnresolvedFilterValue;
        match val {
            UnresolvedFilterValue::Long(l) => FilterValue::Long(*l),
            UnresolvedFilterValue::Double(d) => FilterValue::Double(*d),
            UnresolvedFilterValue::String(s) => FilterValue::String(s.to_string()),
            UnresolvedFilterValue::Bool(b) => FilterValue::Bool(*b),
        }
    }
}

/// Built-in function names
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionName {
    /// String functions
    Strlen,
    Substr,
    Ucase,
    Lcase,
    Contains,
    StrStarts,
    StrEnds,
    Regex,
    Concat,
    StrBefore,
    StrAfter,
    Replace,
    Str,
    StrDt,
    StrLang,
    EncodeForUri,
    /// Numeric functions
    Abs,
    Round,
    Ceil,
    Floor,
    Rand,
    /// RDF term constructors
    Iri,
    Bnode,
    /// DateTime functions
    Now,
    Year,
    Month,
    Day,
    Hours,
    Minutes,
    Seconds,
    Tz,
    /// Type functions
    IsIri,
    IsBlank,
    IsLiteral,
    IsNumeric,
    /// RDF term functions
    Lang,
    Datatype,
    LangMatches,
    SameTerm,
    /// Fluree-specific: transaction time
    T,
    /// Fluree-specific: operation type for history queries ("assert" or "retract")
    Op,
    /// Hash functions
    Md5,
    Sha1,
    Sha256,
    Sha384,
    Sha512,
    /// UUID functions
    Uuid,
    StrUuid,
    /// Vector/embedding similarity functions
    DotProduct,
    CosineSimilarity,
    EuclideanDistance,
    /// Geospatial functions
    GeofDistance,
    /// Other
    Bound,
    If,
    Coalesce,
    /// Custom/unknown function
    Custom(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pattern::Term;
    use fluree_db_core::Sid;

    fn test_pattern() -> TriplePattern {
        TriplePattern::new(
            Term::Var(VarId(0)),
            Term::Sid(Sid::new(100, "name")),
            Term::Var(VarId(1)),
        )
    }

    #[test]
    fn test_query_new() {
        let pattern = test_pattern();
        let query = Query::new(vec![VarId(0), VarId(1)], vec![Pattern::Triple(pattern)]);

        assert_eq!(query.select.len(), 2);
        assert_eq!(query.where_.len(), 1);
    }

    #[test]
    fn test_query_single() {
        let pattern = test_pattern();
        let query = Query::single(vec![VarId(0)], pattern);

        assert_eq!(query.where_.len(), 1);
        assert!(query.where_[0].is_triple());
    }

    #[test]
    fn test_query_triple_patterns() {
        let p1 = TriplePattern::new(
            Term::Var(VarId(0)),
            Term::Sid(Sid::new(100, "name")),
            Term::Var(VarId(1)),
        );
        let p2 = TriplePattern::new(
            Term::Var(VarId(0)),
            Term::Sid(Sid::new(101, "age")),
            Term::Var(VarId(2)),
        );

        let query = Query::new(
            vec![VarId(0), VarId(1), VarId(2)],
            vec![Pattern::Triple(p1), Pattern::Triple(p2)],
        );

        let triples = query.triple_patterns();
        assert_eq!(triples.len(), 2);
    }

    #[test]
    fn test_pattern_variables() {
        let pattern = Pattern::Triple(test_pattern());
        let vars = pattern.variables();
        assert_eq!(vars.len(), 2);
        assert!(vars.contains(&VarId(0)));
        assert!(vars.contains(&VarId(1)));
    }

    #[test]
    fn test_filter_expr_single_var() {
        // Single var: ?x > 10
        let expr = FilterExpr::Compare {
            op: CompareOp::Gt,
            left: Box::new(FilterExpr::Var(VarId(0))),
            right: Box::new(FilterExpr::Const(FilterValue::Long(10))),
        };
        assert_eq!(expr.single_var(), Some(VarId(0)));

        // Two vars: ?x > ?y
        let expr2 = FilterExpr::Compare {
            op: CompareOp::Gt,
            left: Box::new(FilterExpr::Var(VarId(0))),
            right: Box::new(FilterExpr::Var(VarId(1))),
        };
        assert_eq!(expr2.single_var(), None);
    }

    #[test]
    fn test_filter_expr_is_range_safe() {
        // Range-safe: ?x > 10
        let expr = FilterExpr::Compare {
            op: CompareOp::Gt,
            left: Box::new(FilterExpr::Var(VarId(0))),
            right: Box::new(FilterExpr::Const(FilterValue::Long(10))),
        };
        assert!(expr.is_range_safe());

        // Range-safe: AND of range-safe
        let and_expr = FilterExpr::And(vec![
            FilterExpr::Compare {
                op: CompareOp::Ge,
                left: Box::new(FilterExpr::Var(VarId(0))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(18))),
            },
            FilterExpr::Compare {
                op: CompareOp::Lt,
                left: Box::new(FilterExpr::Var(VarId(0))),
                right: Box::new(FilterExpr::Const(FilterValue::Long(65))),
            },
        ]);
        assert!(and_expr.is_range_safe());

        // Not range-safe: OR
        let or_expr = FilterExpr::Or(vec![FilterExpr::Compare {
            op: CompareOp::Eq,
            left: Box::new(FilterExpr::Var(VarId(0))),
            right: Box::new(FilterExpr::Const(FilterValue::Long(1))),
        }]);
        assert!(!or_expr.is_range_safe());
    }
}
