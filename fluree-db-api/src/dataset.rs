//! Dataset types for multi-graph query execution
//!
//! This module provides the API-layer types for declaring and resolving datasets:
//!
//! - [`DatasetSpec`]: Declarative specification from query parsing (unresolved)
//! - [`GraphSource`]: Individual graph source with optional time specification
//! - [`TimeSpec`]: Time-travel specification (at t, commit, or time)
//! - [`FlureeDataSetView`](crate::view::FlureeDataSetView): Resolved dataset composed of views
//!
//! # Architecture
//!
//! Dataset resolution follows a clear separation:
//!
//! | Layer | Responsibility |
//! |-------|---------------|
//! | `fluree-db-api` | Parse `DatasetSpec` from query, resolve aliases via nameservice, apply time-travel, build `FlureeDataSetView` |
//! | `fluree-db-query` | Receive runtime `DataSet<'a>` (borrowed views), execute with graph-aware scanning |
//!
//! `fluree-db-query` should NOT know about ledger aliases, nameservice, or time-travel resolution.
//!
//! # Example
//!
//! ```ignore
//! // Parse dataset from FQL query
//! let spec = DatasetSpec::from_json(&query)?;
//!
//! // Resolve via nameservice
//! let dataset = fluree.build_dataset_view(&spec).await?;
//!
//! // Execute query with dataset
//! let result = fluree.query_dataset_view(&dataset, &query).await?;
//! ```

use fluree_db_core::{alias as core_alias, TrackingOptions};
use fluree_db_sparql::ast::{DatasetClause as SparqlDatasetClause, IriValue};

/// Convert a SPARQL IriValue to a string for use as a ledger identifier.
///
/// - Full IRIs (from `<...>` syntax) return the IRI string directly
/// - Prefixed IRIs return `prefix:local` (unexpanded)
///
/// # Note on Prefixed IRIs
///
/// SPARQL `FROM` clauses typically use full IRI syntax: `FROM <ledger:main>`.
/// The angle brackets make this a full IRI, even if it looks like a CURIE.
/// Actual prefixed names (`ex:graph` without brackets) would need the prologue
/// prefix map to expand properly.
///
/// For dataset identifiers (ledger aliases), we expect full IRIs in `<...>` form.
/// If prefixed names appear, they're passed through as-is and will likely fail
/// nameservice resolution unless the identifier happens to match.
fn iri_value_to_string(iri: &IriValue) -> String {
    match iri {
        IriValue::Full(s) => s.to_string(),
        IriValue::Prefixed { prefix, local } => {
            if prefix.is_empty() {
                format!(":{}", local)
            } else {
                format!("{}:{}", prefix, local)
            }
        }
    }
}

/// Declarative dataset specification from query parsing
///
/// This is the API-layer type containing unresolved ledger aliases
/// and time-travel specs. It represents what the user requested,
/// before resolution via nameservice.
///
/// # Examples
///
/// FQL:
/// ```json
/// {
///   "from": "ledger:main",
///   "from-named": ["graph1", "graph2"]
/// }
/// ```
///
/// SPARQL:
/// ```sparql
/// FROM <ledger:main>
/// FROM NAMED <graph1>
/// FROM NAMED <graph2>
/// ```
#[derive(Debug, Clone, Default)]
pub struct DatasetSpec {
    /// Default graphs - unioned for non-GRAPH patterns
    pub default_graphs: Vec<GraphSource>,
    /// Named graphs - accessible via GRAPH patterns
    pub named_graphs: Vec<GraphSource>,
    /// History mode time range (if detected)
    ///
    /// Set when explicit `from` and `to` keys are provided with time-specced endpoints
    /// for the same ledger (e.g., `"from": "ledger@t:1", "to": "ledger@t:latest"`).
    /// This indicates a history/changes query rather than a point-in-time query.
    pub history_range: Option<HistoryTimeRange>,
}

impl DatasetSpec {
    /// Create an empty dataset spec
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a default graph
    pub fn with_default(mut self, source: GraphSource) -> Self {
        self.default_graphs.push(source);
        self
    }

    /// Add a named graph
    pub fn with_named(mut self, source: GraphSource) -> Self {
        self.named_graphs.push(source);
        self
    }

    /// Check if this spec is empty (no graphs specified)
    pub fn is_empty(&self) -> bool {
        self.default_graphs.is_empty()
            && self.named_graphs.is_empty()
            && self.history_range.is_none()
    }

    /// Get total number of graphs specified
    pub fn num_graphs(&self) -> usize {
        self.default_graphs.len() + self.named_graphs.len()
    }

    /// Check if this is a history/changes query
    ///
    /// History mode is detected when explicit `from` and `to` keys are provided
    /// with time-specced endpoints for the same ledger, e.g.:
    /// ```json
    /// { "from": "ledger:main@t:1", "to": "ledger:main@t:latest" }
    /// ```
    pub fn is_history_mode(&self) -> bool {
        self.history_range.is_some()
    }

    /// Get the history time range if in history mode
    pub fn history_range(&self) -> Option<&HistoryTimeRange> {
        self.history_range.as_ref()
    }

    /// Create a DatasetSpec from a SPARQL DatasetClause
    ///
    /// Converts SPARQL FROM and FROM NAMED clauses to the API-layer
    /// DatasetSpec format.
    ///
    /// # Example
    ///
    /// ```sparql
    /// SELECT ?s
    /// FROM <http://example.org/graph1>
    /// FROM <http://example.org/graph2>
    /// FROM NAMED <http://example.org/named1>
    /// WHERE { ?s ?p ?o }
    /// ```
    ///
    /// Would produce a DatasetSpec with:
    /// - 2 default graphs (graph1, graph2)
    /// - 1 named graph (named1)
    ///
    /// ## Fluree Extension: History Range
    ///
    /// ```sparql
    /// SELECT ?s ?t ?op
    /// FROM <ledger:main@t:1> TO <ledger:main@t:latest>
    /// WHERE { ... }
    /// ```
    ///
    /// When `TO` clause is present, creates a history range query.
    pub fn from_sparql_clause(clause: &SparqlDatasetClause) -> Result<Self, DatasetParseError> {
        let default_graphs = clause
            .default_graphs
            .iter()
            .map(|iri| {
                let iri_str = iri_value_to_string(&iri.value);
                let (identifier, time_spec) = parse_alias_time_travel(&iri_str)?;
                let mut source = GraphSource::new(identifier);
                source.time_spec = time_spec;
                Ok(source)
            })
            .collect::<Result<Vec<_>, DatasetParseError>>()?;

        let named_graphs = clause
            .named_graphs
            .iter()
            .map(|iri| {
                let iri_str = iri_value_to_string(&iri.value);
                let (identifier, time_spec) = parse_alias_time_travel(&iri_str)?;
                let mut source = GraphSource::new(identifier);
                source.time_spec = time_spec;
                Ok(source)
            })
            .collect::<Result<Vec<_>, DatasetParseError>>()?;

        // Check for explicit TO clause (Fluree extension for history range)
        let history_range = if let Some(to_iri) = &clause.to_graph {
            // Explicit FROM...TO syntax
            if default_graphs.is_empty() {
                return Err(DatasetParseError::InvalidFrom(
                    "FROM...TO requires a FROM graph".to_string(),
                ));
            }
            let from_source = &default_graphs[0];
            let from_time = from_source.time_spec.as_ref().ok_or_else(|| {
                DatasetParseError::InvalidFrom(
                    "FROM graph in history range must have time specification".to_string(),
                )
            })?;

            let to_iri_str = iri_value_to_string(&to_iri.value);
            let (to_identifier, to_time_spec) = parse_alias_time_travel(&to_iri_str)?;
            let to_time = to_time_spec.ok_or_else(|| {
                DatasetParseError::InvalidFrom("TO graph must have time specification".to_string())
            })?;

            // Verify same ledger
            if from_source.identifier != to_identifier {
                return Err(DatasetParseError::InvalidFrom(format!(
                    "FROM and TO must reference the same ledger: {} vs {}",
                    from_source.identifier, to_identifier
                )));
            }

            Some(HistoryTimeRange::new(
                &from_source.identifier,
                from_time.clone(),
                to_time,
            ))
        } else {
            // No TO clause = not a history query
            // Multiple FROM clauses are treated as a union query, not history
            None
        };

        Ok(Self {
            default_graphs,
            named_graphs,
            history_range,
        })
    }
}

/// Individual graph source with optional time specification
///
/// Represents a single graph in a dataset, identified by a ledger alias
/// (IRI) and optionally pinned to a specific time.
#[derive(Debug, Clone)]
pub struct GraphSource {
    /// Ledger alias or IRI (e.g., "mydb:main", "http://example.org/ledger1")
    pub identifier: String,
    /// Optional time-travel specification
    pub time_spec: Option<TimeSpec>,
}

impl GraphSource {
    /// Create a graph source from an identifier
    pub fn new(identifier: impl Into<String>) -> Self {
        Self {
            identifier: identifier.into(),
            time_spec: None,
        }
    }

    /// Set time specification
    pub fn with_time(mut self, time_spec: TimeSpec) -> Self {
        self.time_spec = Some(time_spec);
        self
    }
}

impl From<&str> for GraphSource {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for GraphSource {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

/// Time specification for graph sources
///
/// Allows pinning a graph to a specific point in time.
#[derive(Debug, Clone, PartialEq)]
pub enum TimeSpec {
    /// At a specific transaction number
    AtT(i64),
    /// At a specific commit hash
    AtCommit(String),
    /// At a specific ISO 8601 timestamp
    AtTime(String),
    /// "latest" keyword - resolves to current ledger t
    Latest,
}

impl TimeSpec {
    /// Create at-t specification
    pub fn at_t(t: i64) -> Self {
        Self::AtT(t)
    }

    /// Create at-commit specification
    pub fn at_commit(commit: impl Into<String>) -> Self {
        Self::AtCommit(commit.into())
    }

    /// Create at-time specification
    pub fn at_time(time: impl Into<String>) -> Self {
        Self::AtTime(time.into())
    }

    /// Create latest specification
    pub fn latest() -> Self {
        Self::Latest
    }
}

/// Time range for history queries
///
/// Represents a range of time for querying changes/history.
/// Detected when `from` is an array with two time-specced endpoints
/// for the same ledger (e.g., `["ledger@t:1", "ledger@t:latest"]`).
#[derive(Debug, Clone)]
pub struct HistoryTimeRange {
    /// The ledger identifier (without time suffix)
    pub identifier: String,
    /// Start of the time range
    pub from: TimeSpec,
    /// End of the time range
    pub to: TimeSpec,
}

impl HistoryTimeRange {
    /// Create a new history time range
    pub fn new(identifier: impl Into<String>, from: TimeSpec, to: TimeSpec) -> Self {
        Self {
            identifier: identifier.into(),
            from,
            to,
        }
    }
}

// =============================================================================
// FQL Parsing
// =============================================================================

use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Error type for dataset spec parsing
#[derive(Debug, Clone)]
pub enum DatasetParseError {
    /// Invalid "from" value type
    InvalidFrom(String),
    /// Invalid "from-named" value type
    InvalidFromNamed(String),
    /// Invalid graph source object
    InvalidGraphSource(String),
    /// Invalid query-connection options object
    InvalidOptions(String),
}

impl std::fmt::Display for DatasetParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidFrom(msg) => write!(f, "Invalid 'from' value: {}", msg),
            Self::InvalidFromNamed(msg) => write!(f, "Invalid 'from-named' value: {}", msg),
            Self::InvalidGraphSource(msg) => write!(f, "Invalid graph source: {}", msg),
            Self::InvalidOptions(msg) => write!(f, "Invalid query options: {}", msg),
        }
    }
}

impl std::error::Error for DatasetParseError {}

impl DatasetSpec {
    /// Parse a DatasetSpec from FQL JSON query options
    ///
    /// Extracts "from" and "from-named" keys from the query object.
    ///
    /// # Supported formats
    ///
    /// **"from" (default graphs)**:
    /// - Single string: `"from": "ledger:main"`
    /// - Array of strings: `"from": ["ledger1:main", "ledger2:main"]`
    /// - Object with time: `"from": {"@id": "ledger:main", "t": 42}`
    /// - Array of objects: `"from": [{"@id": "ledger1", "t": 10}, "ledger2"]`
    ///
    /// **"from-named" (named graphs)**:
    /// - Single string: `"from-named": "graph1"`
    /// - Array: `"from-named": ["graph1", "graph2"]`
    ///
    /// # Example
    ///
    /// ```ignore
    /// let query = json!({
    ///     "from": ["ledger1:main", "ledger2:main"],
    ///     "from-named": ["graph1"],
    ///     "select": ["?s"],
    ///     "where": {"@id": "?s"}
    /// });
    ///
    /// let spec = DatasetSpec::from_json(&query)?;
    /// assert_eq!(spec.num_graphs(), 3);
    /// ```
    pub fn from_json(json: &JsonValue) -> Result<Self, DatasetParseError> {
        let obj = match json.as_object() {
            Some(o) => o,
            None => return Ok(Self::new()), // Not an object, return empty spec
        };

        let mut spec = Self::new();

        // Parse "from" (default graphs)
        if let Some(from_val) = obj.get("from") {
            spec.default_graphs = parse_graph_sources(from_val, "from")?;
        }

        // Check for explicit "to" key (history query)
        // Syntax: { "from": "ledger@t:1", "to": "ledger@t:latest" }
        // This mirrors SPARQL's FROM ... TO ... syntax
        if let Some(to_val) = obj.get("to") {
            // Must have exactly one "from" graph
            if spec.default_graphs.len() != 1 {
                return Err(DatasetParseError::InvalidFrom(
                    "'to' requires exactly one 'from' graph".to_string(),
                ));
            }
            let from_source = &spec.default_graphs[0];
            let to_source = parse_single_graph_source(to_val, "to")?;

            // Validate same ledger
            if from_source.identifier != to_source.identifier {
                return Err(DatasetParseError::InvalidFrom(format!(
                    "'from' and 'to' must reference the same ledger: '{}' vs '{}'",
                    from_source.identifier, to_source.identifier
                )));
            }

            // Require time specs on both
            let from_time = from_source.time_spec.as_ref().ok_or_else(|| {
                DatasetParseError::InvalidFrom(
                    "'from' graph in history query must have time specification (e.g., ledger@t:1)"
                        .to_string(),
                )
            })?;
            let to_time = to_source.time_spec.as_ref().ok_or_else(|| {
                DatasetParseError::InvalidFrom(
                    "'to' graph must have time specification (e.g., ledger@t:latest)".to_string(),
                )
            })?;

            spec.history_range = Some(HistoryTimeRange::new(
                &from_source.identifier,
                from_time.clone(),
                to_time.clone(),
            ));
        }

        // Parse "from-named" (named graphs)
        if let Some(from_named_val) = obj.get("from-named") {
            spec.named_graphs = parse_graph_sources(from_named_val, "from-named")?;
        }

        Ok(spec)
    }

    /// Parse dataset + connection options from a query JSON object.
    ///
    /// Mirrors Clojure `query-connection` semantics:
    /// - Dataset spec may live at top-level (`from`, `from-named`, `ledger`) OR inside `opts`.
    /// - Connection/policy-related options are read from `opts`.
    /// - History queries use explicit `to` key: `{ "from": "ledger@t:1", "to": "ledger@t:latest" }`
    pub fn from_query_json(
        json: &JsonValue,
    ) -> Result<(Self, QueryConnectionOptions), DatasetParseError> {
        let obj = match json.as_object() {
            Some(o) => o,
            None => return Ok((Self::new(), QueryConnectionOptions::default())),
        };

        let opts_obj = obj.get("opts").and_then(|v| v.as_object());

        // Dataset location precedence matches Clojure:
        // default aliases: opts.from || opts.ledger || query.from || query.ledger
        // named aliases:   opts.from-named || query.from-named
        // to (history):    opts.to || query.to
        let from_val = opts_obj
            .and_then(|o| o.get("from"))
            .or_else(|| opts_obj.and_then(|o| o.get("ledger")))
            .or_else(|| obj.get("from"))
            .or_else(|| obj.get("ledger"));

        let from_named_val = opts_obj
            .and_then(|o| o.get("from-named"))
            .or_else(|| obj.get("from-named"));

        let to_val = opts_obj.and_then(|o| o.get("to")).or_else(|| obj.get("to"));

        let mut spec = Self::new();
        if let Some(v) = from_val {
            spec.default_graphs = parse_graph_sources(v, "from")?;
        }

        // Check for explicit "to" key (history query)
        if let Some(to_v) = to_val {
            // Must have exactly one "from" graph
            if spec.default_graphs.len() != 1 {
                return Err(DatasetParseError::InvalidFrom(
                    "'to' requires exactly one 'from' graph".to_string(),
                ));
            }
            let from_source = &spec.default_graphs[0];
            let to_source = parse_single_graph_source(to_v, "to")?;

            // Validate same ledger
            if from_source.identifier != to_source.identifier {
                return Err(DatasetParseError::InvalidFrom(format!(
                    "'from' and 'to' must reference the same ledger: '{}' vs '{}'",
                    from_source.identifier, to_source.identifier
                )));
            }

            // Require time specs on both
            let from_time = from_source.time_spec.as_ref().ok_or_else(|| {
                DatasetParseError::InvalidFrom(
                    "'from' graph in history query must have time specification (e.g., ledger@t:1)"
                        .to_string(),
                )
            })?;
            let to_time = to_source.time_spec.as_ref().ok_or_else(|| {
                DatasetParseError::InvalidFrom(
                    "'to' graph must have time specification (e.g., ledger@t:latest)".to_string(),
                )
            })?;

            spec.history_range = Some(HistoryTimeRange::new(
                &from_source.identifier,
                from_time.clone(),
                to_time.clone(),
            ));
        }

        if let Some(v) = from_named_val {
            spec.named_graphs = parse_graph_sources(v, "from-named")?;
        }

        let qc_opts = QueryConnectionOptions::from_json(json)?;
        Ok((spec, qc_opts))
    }
}

/// Parsed query-connection options (policy/identity-related).
///
/// Supported keys in the query `opts` object:
/// - `identity`
/// - `policy-class`
/// - `policy`
/// - `policy-values`
/// - `default-allow`
/// - `meta` (tracking enablement: bool or object)
/// - `max-fuel` (fuel limit, also enables fuel tracking)
#[derive(Debug, Clone, Default)]
pub struct QueryConnectionOptions {
    pub identity: Option<String>,
    pub policy_class: Option<Vec<String>>,
    pub policy: Option<JsonValue>,
    pub policy_values: Option<HashMap<String, JsonValue>>,
    pub default_allow: bool,
    /// Tracking options parsed from `meta` and `max-fuel` in opts
    pub tracking: TrackingOptions,
}

impl QueryConnectionOptions {
    pub fn from_json(query: &JsonValue) -> Result<Self, DatasetParseError> {
        let obj = match query.as_object() {
            Some(o) => o,
            None => return Ok(Self::default()),
        };

        let opts_val = obj.get("opts");
        let opts = match opts_val {
            None | Some(JsonValue::Null) => return Ok(Self::default()),
            Some(JsonValue::Object(o)) => o,
            Some(other) => {
                return Err(DatasetParseError::InvalidOptions(format!(
                    "'opts' must be an object, got {}",
                    other
                )))
            }
        };

        // Parse tracking options from opts
        let tracking = TrackingOptions::from_opts_value(opts_val);

        let identity = opts
            .get("identity")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let policy_class_val = opts
            .get("policy-class")
            .or_else(|| opts.get("policy_class"))
            .or_else(|| opts.get("policyClass"));
        let policy_class = match policy_class_val {
            None | Some(JsonValue::Null) => None,
            Some(JsonValue::String(s)) => Some(vec![s.to_string()]),
            Some(JsonValue::Array(arr)) => {
                let mut out = Vec::with_capacity(arr.len());
                for v in arr {
                    let Some(s) = v.as_str() else {
                        return Err(DatasetParseError::InvalidOptions(
                            "'policy-class' must be a string or array of strings".to_string(),
                        ));
                    };
                    out.push(s.to_string());
                }
                Some(out)
            }
            Some(_) => {
                return Err(DatasetParseError::InvalidOptions(
                    "'policy-class' must be a string or array of strings".to_string(),
                ))
            }
        };

        let policy = opts.get("policy").cloned().and_then(|v| match v {
            JsonValue::Null => None,
            other => Some(other),
        });

        let policy_values_val = opts
            .get("policy-values")
            .or_else(|| opts.get("policy_values"))
            .or_else(|| opts.get("policyValues"));
        let policy_values = match policy_values_val {
            None | Some(JsonValue::Null) => None,
            Some(JsonValue::Object(map)) => {
                Some(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            }
            Some(_) => {
                return Err(DatasetParseError::InvalidOptions(
                    "'policy-values' must be an object".to_string(),
                ))
            }
        };

        let default_allow = opts
            .get("default-allow")
            .or_else(|| opts.get("default_allow"))
            .or_else(|| opts.get("defaultAllow"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        Ok(Self {
            identity,
            policy_class,
            policy,
            policy_values,
            default_allow,
            tracking,
        })
    }

    pub fn has_any_policy_inputs(&self) -> bool {
        self.identity.is_some()
            || self.policy_class.as_ref().is_some_and(|v| !v.is_empty())
            || self.policy.is_some()
            || self.policy_values.as_ref().is_some_and(|m| !m.is_empty())
            || self.default_allow
    }
}

/// Parse time-travel specification from ledger alias string.
///
/// Supports Clojure-compatible formats:
/// - `ledger:main@t:42` → identifier="ledger:main", TimeSpec::AtT(42)
/// - `ledger:main@t:latest` → identifier="ledger:main", TimeSpec::Latest
/// - `ledger:main@iso:2025-01-01T00:00:00Z` → identifier="ledger:main", TimeSpec::AtTime(...)
/// - `ledger:main@sha:abc123` → identifier="ledger:main", TimeSpec::AtCommit(...)
///
/// Returns (identifier, Option<TimeSpec>).
fn parse_alias_time_travel(alias: &str) -> Result<(String, Option<TimeSpec>), DatasetParseError> {
    // Support optional named-graph fragment selector after time spec:
    //   ledger:main@t:42#txn-meta
    // We parse time-travel on the portion before '#', then re-attach the fragment
    // to the identifier (so the identifier remains stable and time is separate).
    let (before_fragment, fragment) = match alias.split_once('#') {
        Some((left, right)) => {
            if right.is_empty() {
                return Err(DatasetParseError::InvalidGraphSource(
                    "Missing named graph after '#'".to_string(),
                ));
            }
            (left, Some(right))
        }
        None => (alias, None),
    };
    let fragment_suffix = fragment.map(|f| format!("#{f}")).unwrap_or_default();

    // Check for @t:latest special case before standard parsing
    if let Some(base) = before_fragment.strip_suffix("@t:latest") {
        if base.is_empty() {
            return Err(DatasetParseError::InvalidGraphSource(
                "Alias cannot be empty before '@'".to_string(),
            ));
        }
        let identifier = format!("{base}{fragment_suffix}");
        return Ok((identifier, Some(TimeSpec::Latest)));
    }

    let (identifier, time) = core_alias::split_time_travel_suffix(before_fragment)
        .map_err(|e| DatasetParseError::InvalidGraphSource(e.to_string()))?;

    let time_spec = time.map(|spec| match spec {
        core_alias::AliasTimeSpec::AtT(t) => TimeSpec::AtT(t),
        core_alias::AliasTimeSpec::AtIso(value) => TimeSpec::AtTime(value),
        core_alias::AliasTimeSpec::AtSha(value) => TimeSpec::AtCommit(value),
    });

    Ok((format!("{identifier}{fragment_suffix}"), time_spec))
}

/// Parse graph sources from a JSON value
///
/// Accepts:
/// - String: single graph source (may include @t:/@iso:/@sha: time-travel syntax)
/// - Array: multiple graph sources
/// - Object: single graph source with time spec
fn parse_graph_sources(
    val: &JsonValue,
    field_name: &str,
) -> Result<Vec<GraphSource>, DatasetParseError> {
    match val {
        JsonValue::String(s) => {
            let (identifier, time_spec) = parse_alias_time_travel(s)?;
            let mut source = GraphSource::new(identifier);
            source.time_spec = time_spec;
            Ok(vec![source])
        }
        JsonValue::Array(arr) => arr
            .iter()
            .map(|item| parse_single_graph_source(item, field_name))
            .collect(),
        JsonValue::Object(_) => Ok(vec![parse_single_graph_source(val, field_name)?]),
        JsonValue::Null => Ok(vec![]),
        _ => Err(DatasetParseError::InvalidFrom(format!(
            "'{}' must be a string, array, or object",
            field_name
        ))),
    }
}

/// Parse a single graph source from a JSON value
///
/// Accepts:
/// - String: identifier (may include @t:/@iso:/@sha: time-travel syntax)
/// - Object: `{"@id": "ledger:main", "t": 42}` or `{"@id": "ledger:main", "at": "commit:abc"}`
fn parse_single_graph_source(
    val: &JsonValue,
    field_name: &str,
) -> Result<GraphSource, DatasetParseError> {
    match val {
        JsonValue::String(s) => {
            let (identifier, time_spec) = parse_alias_time_travel(s)?;
            let mut source = GraphSource::new(identifier);
            source.time_spec = time_spec;
            Ok(source)
        }
        JsonValue::Object(obj) => {
            // Get identifier from @id or id
            let identifier = obj
                .get("@id")
                .or_else(|| obj.get("id"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    DatasetParseError::InvalidGraphSource(format!(
                        "'{}' object must have '@id' or 'id' string field",
                        field_name
                    ))
                })?;

            let mut source = GraphSource::new(identifier);

            // Parse time specification
            if let Some(t_val) = obj.get("t") {
                if let Some(t) = t_val.as_i64() {
                    source.time_spec = Some(TimeSpec::AtT(t));
                }
            } else if let Some(at_val) = obj.get("at") {
                if let Some(at_str) = at_val.as_str() {
                    // Determine if it's a commit hash or timestamp
                    if let Some(commit_hash) = at_str.strip_prefix("commit:") {
                        source.time_spec = Some(TimeSpec::AtCommit(commit_hash.to_string()));
                    } else {
                        // Assume ISO timestamp
                        source.time_spec = Some(TimeSpec::AtTime(at_str.to_string()));
                    }
                }
            }

            Ok(source)
        }
        _ => Err(DatasetParseError::InvalidGraphSource(format!(
            "'{}' item must be a string or object",
            field_name
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dataset_spec_empty() {
        let spec = DatasetSpec::new();
        assert!(spec.is_empty());
        assert_eq!(spec.num_graphs(), 0);
    }

    #[test]
    fn test_dataset_spec_with_graphs() {
        let spec = DatasetSpec::new()
            .with_default(GraphSource::new("ledger1:main"))
            .with_default(GraphSource::new("ledger2:main"))
            .with_named(GraphSource::new("graph1"));

        assert!(!spec.is_empty());
        assert_eq!(spec.num_graphs(), 3);
        assert_eq!(spec.default_graphs.len(), 2);
        assert_eq!(spec.named_graphs.len(), 1);
    }

    #[test]
    fn test_graph_source_with_time() {
        let source = GraphSource::new("mydb:main").with_time(TimeSpec::at_t(42));

        assert_eq!(source.identifier, "mydb:main");
        assert!(matches!(source.time_spec, Some(TimeSpec::AtT(42))));
    }

    #[test]
    fn test_graph_source_from_str() {
        let source: GraphSource = "test:ledger".into();
        assert_eq!(source.identifier, "test:ledger");
        assert!(source.time_spec.is_none());
    }

    #[test]
    fn test_time_spec_variants() {
        let t = TimeSpec::at_t(100);
        assert!(matches!(t, TimeSpec::AtT(100)));

        let commit = TimeSpec::at_commit("abc123");
        assert!(matches!(commit, TimeSpec::AtCommit(ref s) if s == "abc123"));

        let time = TimeSpec::at_time("2024-01-01T00:00:00Z");
        assert!(matches!(time, TimeSpec::AtTime(ref s) if s == "2024-01-01T00:00:00Z"));
    }

    // FQL Parsing Tests

    use serde_json::json;

    #[test]
    fn test_parse_from_single_string() {
        let query = json!({
            "from": "ledger:main",
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert!(spec.named_graphs.is_empty());
    }

    #[test]
    fn test_parse_from_array() {
        let query = json!({
            "from": ["ledger1:main", "ledger2:main"],
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 2);
        assert_eq!(spec.default_graphs[0].identifier, "ledger1:main");
        assert_eq!(spec.default_graphs[1].identifier, "ledger2:main");
    }

    #[test]
    fn test_parse_from_with_time_t() {
        let query = json!({
            "from": {"@id": "ledger:main", "t": 42},
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec,
            Some(TimeSpec::AtT(42))
        ));
    }

    #[test]
    fn test_parse_from_with_commit() {
        let query = json!({
            "from": {"@id": "ledger:main", "at": "commit:abc123"},
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert!(matches!(
            &spec.default_graphs[0].time_spec,
            Some(TimeSpec::AtCommit(s)) if s == "abc123"
        ));
    }

    // Ledger alias time-travel syntax tests (@t:, @iso:, @sha:)

    #[test]
    fn test_parse_alias_at_t() {
        let query = json!({
            "from": "ledger:main@t:42",
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec,
            Some(TimeSpec::AtT(42))
        ));
    }

    #[test]
    fn test_parse_alias_at_t_with_named_graph_fragment() {
        let query = json!({
            "from": "ledger:main@t:42#txn-meta",
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main#txn-meta");
        assert!(matches!(
            spec.default_graphs[0].time_spec,
            Some(TimeSpec::AtT(42))
        ));
    }

    #[test]
    fn test_parse_alias_at_iso() {
        let query = json!({
            "from": "ledger:main@iso:2025-01-20T00:00:00Z",
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert!(matches!(
            &spec.default_graphs[0].time_spec,
            Some(TimeSpec::AtTime(s)) if s == "2025-01-20T00:00:00Z"
        ));
    }

    #[test]
    fn test_parse_alias_at_sha() {
        let query = json!({
            "from": "ledger:main@sha:abc123def456",
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert!(matches!(
            &spec.default_graphs[0].time_spec,
            Some(TimeSpec::AtCommit(s)) if s == "abc123def456"
        ));
    }

    #[test]
    fn test_parse_alias_at_sha_too_short() {
        let query = json!({
            "from": "ledger:main@sha:abc",
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let result = DatasetSpec::from_json(&query);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_alias_array_mixed_time_specs() {
        let query = json!({
            "from": ["ledger1:main@t:10", "ledger2:main", "ledger3:main@iso:2025-01-01T00:00:00Z"],
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 3);

        assert_eq!(spec.default_graphs[0].identifier, "ledger1:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec,
            Some(TimeSpec::AtT(10))
        ));

        assert_eq!(spec.default_graphs[1].identifier, "ledger2:main");
        assert!(spec.default_graphs[1].time_spec.is_none());

        assert_eq!(spec.default_graphs[2].identifier, "ledger3:main");
        assert!(matches!(
            &spec.default_graphs[2].time_spec,
            Some(TimeSpec::AtTime(s)) if s == "2025-01-01T00:00:00Z"
        ));
    }

    #[test]
    fn test_parse_alias_invalid_time_format() {
        let query = json!({
            "from": "ledger:main@invalid:123",
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let result = DatasetSpec::from_json(&query);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_from_named() {
        let query = json!({
            "from": "default:main",
            "from-named": ["graph1", "graph2"],
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.named_graphs.len(), 2);
        assert_eq!(spec.named_graphs[0].identifier, "graph1");
        assert_eq!(spec.named_graphs[1].identifier, "graph2");
    }

    #[test]
    fn test_parse_mixed_from_array() {
        let query = json!({
            "from": [
                "ledger1:main",
                {"@id": "ledger2:main", "t": 10}
            ],
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 2);
        assert_eq!(spec.default_graphs[0].identifier, "ledger1:main");
        assert!(spec.default_graphs[0].time_spec.is_none());
        assert_eq!(spec.default_graphs[1].identifier, "ledger2:main");
        assert!(matches!(
            spec.default_graphs[1].time_spec,
            Some(TimeSpec::AtT(10))
        ));
    }

    #[test]
    fn test_parse_no_dataset() {
        let query = json!({
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(spec.is_empty());
    }

    #[test]
    fn test_parse_null_from() {
        let query = json!({
            "from": null,
            "select": ["?s"],
            "where": {"@id": "?s"}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(spec.default_graphs.is_empty());
    }

    // SPARQL DatasetClause Conversion Tests

    use fluree_db_sparql::ast::{DatasetClause as SparqlDatasetClause, Iri};
    use fluree_db_sparql::SourceSpan;

    fn make_span() -> SourceSpan {
        SourceSpan::new(0, 0)
    }

    #[test]
    fn test_from_sparql_clause_empty() {
        let clause = SparqlDatasetClause {
            default_graphs: vec![],
            named_graphs: vec![],
            to_graph: None,
            span: make_span(),
        };

        let spec = DatasetSpec::from_sparql_clause(&clause).unwrap();
        assert!(spec.is_empty());
    }

    #[test]
    fn test_from_sparql_clause_single_default() {
        let clause = SparqlDatasetClause {
            default_graphs: vec![Iri::full("http://example.org/graph1", make_span())],
            named_graphs: vec![],
            to_graph: None,
            span: make_span(),
        };

        let spec = DatasetSpec::from_sparql_clause(&clause).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(
            spec.default_graphs[0].identifier,
            "http://example.org/graph1"
        );
        assert!(spec.named_graphs.is_empty());
    }

    #[test]
    fn test_from_sparql_clause_multiple_default() {
        let clause = SparqlDatasetClause {
            default_graphs: vec![
                Iri::full("http://example.org/graph1", make_span()),
                Iri::full("http://example.org/graph2", make_span()),
            ],
            named_graphs: vec![],
            to_graph: None,
            span: make_span(),
        };

        let spec = DatasetSpec::from_sparql_clause(&clause).unwrap();
        assert_eq!(spec.default_graphs.len(), 2);
        assert_eq!(
            spec.default_graphs[0].identifier,
            "http://example.org/graph1"
        );
        assert_eq!(
            spec.default_graphs[1].identifier,
            "http://example.org/graph2"
        );
    }

    #[test]
    fn test_from_sparql_clause_named_graphs() {
        let clause = SparqlDatasetClause {
            default_graphs: vec![],
            named_graphs: vec![
                Iri::full("http://example.org/named1", make_span()),
                Iri::full("http://example.org/named2", make_span()),
            ],
            to_graph: None,
            span: make_span(),
        };

        let spec = DatasetSpec::from_sparql_clause(&clause).unwrap();
        assert!(spec.default_graphs.is_empty());
        assert_eq!(spec.named_graphs.len(), 2);
        assert_eq!(spec.named_graphs[0].identifier, "http://example.org/named1");
        assert_eq!(spec.named_graphs[1].identifier, "http://example.org/named2");
    }

    #[test]
    fn test_from_sparql_clause_mixed() {
        let clause = SparqlDatasetClause {
            default_graphs: vec![Iri::full("http://example.org/default1", make_span())],
            named_graphs: vec![
                Iri::full("http://example.org/named1", make_span()),
                Iri::full("http://example.org/named2", make_span()),
            ],
            to_graph: None,
            span: make_span(),
        };

        let spec = DatasetSpec::from_sparql_clause(&clause).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.named_graphs.len(), 2);
        assert_eq!(
            spec.default_graphs[0].identifier,
            "http://example.org/default1"
        );
    }

    #[test]
    fn test_from_sparql_clause_prefixed_iri() {
        let clause = SparqlDatasetClause {
            default_graphs: vec![Iri::prefixed("ex", "graph1", make_span())],
            named_graphs: vec![Iri::prefixed("", "localname", make_span())],
            to_graph: None,
            span: make_span(),
        };

        let spec = DatasetSpec::from_sparql_clause(&clause).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ex:graph1");
        assert_eq!(spec.named_graphs.len(), 1);
        assert_eq!(spec.named_graphs[0].identifier, ":localname");
    }

    #[test]
    fn test_from_sparql_clause_time_travel_suffix() {
        let clause = SparqlDatasetClause {
            default_graphs: vec![
                Iri::full("ledger:main@t:42", make_span()),
                Iri::full("ledger:main@iso:2025-01-01T00:00:00Z", make_span()),
            ],
            named_graphs: vec![Iri::full("ledger:main@sha:abc123def456", make_span())],
            to_graph: None,
            span: make_span(),
        };

        let spec = DatasetSpec::from_sparql_clause(&clause).unwrap();
        assert_eq!(spec.default_graphs.len(), 2);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec,
            Some(TimeSpec::AtT(42))
        ));
        assert_eq!(spec.default_graphs[1].identifier, "ledger:main");
        assert!(matches!(
            &spec.default_graphs[1].time_spec,
            Some(TimeSpec::AtTime(s)) if s == "2025-01-01T00:00:00Z"
        ));

        assert_eq!(spec.named_graphs.len(), 1);
        assert_eq!(spec.named_graphs[0].identifier, "ledger:main");
        assert!(matches!(
            &spec.named_graphs[0].time_spec,
            Some(TimeSpec::AtCommit(s)) if s == "abc123def456"
        ));
    }

    #[test]
    fn test_from_sparql_clause_to_graph_history_range() {
        // FROM <ledger:main@t:1> TO <ledger:main@t:latest>
        let clause = SparqlDatasetClause {
            default_graphs: vec![Iri::full("ledger:main@t:1", make_span())],
            named_graphs: vec![],
            to_graph: Some(Iri::full("ledger:main@t:latest", make_span())),
            span: make_span(),
        };

        let spec = DatasetSpec::from_sparql_clause(&clause).unwrap();
        assert!(
            spec.is_history_mode(),
            "Should detect history mode from TO clause"
        );

        let range = spec.history_range().expect("Should have history range");
        assert_eq!(range.identifier, "ledger:main");
        assert!(matches!(range.from, TimeSpec::AtT(1)));
        assert!(matches!(range.to, TimeSpec::Latest));
    }

    // History Mode Detection Tests - Explicit "to" Syntax
    //
    // History mode is now detected via explicit "to" key syntax, mirroring SPARQL FROM ... TO ...
    // The old heuristic (detecting from two-element arrays) was removed as it was ambiguous:
    // - `from: ["ledger@t:1", "ledger@t:latest"]` could mean either:
    //   1. History query (show changes between t:1 and t:latest)
    //   2. Union query (join two immutable views of the same ledger)
    //
    // New explicit syntax:
    // - History query: `{ "from": "ledger@t:1", "to": "ledger@t:latest" }`
    // - Union query:   `{ "from": ["ledger@t:1", "ledger@t:latest"] }`

    #[test]
    fn test_history_mode_explicit_to_key() {
        // Explicit "to" key = history mode
        let query = json!({
            "from": "ledger:main@t:1",
            "to": "ledger:main@t:latest",
            "select": ["?t", "?op", "?age"],
            "where": {"@id": "ex:alice", "ex:age": {"@value": "?age", "@t": "?t", "@op": "?op"}}
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(
            spec.is_history_mode(),
            "Should detect history mode from explicit 'to' key"
        );

        let range = spec.history_range().expect("Should have history range");
        assert_eq!(range.identifier, "ledger:main");
        assert!(matches!(range.from, TimeSpec::AtT(1)));
        assert!(matches!(range.to, TimeSpec::Latest));
    }

    #[test]
    fn test_history_mode_with_iso_range_explicit() {
        // Explicit "to" key with ISO dates
        let query = json!({
            "from": "ledger:main@iso:2024-01-01T00:00:00Z",
            "to": "ledger:main@iso:2024-12-31T23:59:59Z",
            "select": ["?t", "?age"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(
            spec.is_history_mode(),
            "Should detect history mode with ISO dates"
        );

        let range = spec.history_range().expect("Should have history range");
        assert_eq!(range.identifier, "ledger:main");
        assert!(matches!(&range.from, TimeSpec::AtTime(s) if s == "2024-01-01T00:00:00Z"));
        assert!(matches!(&range.to, TimeSpec::AtTime(s) if s == "2024-12-31T23:59:59Z"));
    }

    #[test]
    fn test_history_mode_mixed_time_types_explicit() {
        // Different time types (sha and t) for same ledger with explicit "to"
        let query = json!({
            "from": "ledger:main@sha:abc123def456",
            "to": "ledger:main@t:latest",
            "select": ["?t", "?age"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(
            spec.is_history_mode(),
            "Mixed time types should be history mode"
        );

        let range = spec.history_range().expect("Should have history range");
        assert!(matches!(&range.from, TimeSpec::AtCommit(s) if s == "abc123def456"));
        assert!(matches!(range.to, TimeSpec::Latest));
    }

    #[test]
    fn test_not_history_mode_array_same_ledger_different_times() {
        // Array syntax with same ledger at different times = union query, NOT history mode
        // This is the key semantic change: arrays are always union queries, even with time specs
        let query = json!({
            "from": ["ledger:main@t:1", "ledger:main@t:latest"],
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(
            !spec.is_history_mode(),
            "Array syntax should NOT be history mode (use explicit 'to')"
        );
        assert!(spec.history_range().is_none());
        // Should have two separate graphs
        assert_eq!(spec.default_graphs.len(), 2);
    }

    #[test]
    fn test_not_history_mode_different_ledgers() {
        // Two endpoints for DIFFERENT ledgers = NOT history mode
        let query = json!({
            "from": ["ledger1:main@t:1", "ledger2:main@t:latest"],
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(
            !spec.is_history_mode(),
            "Different ledgers should not be history mode"
        );
        assert!(spec.history_range().is_none());
    }

    #[test]
    fn test_not_history_mode_single_endpoint() {
        // Single endpoint = NOT history mode (point-in-time query)
        let query = json!({
            "from": "ledger:main@t:100",
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(
            !spec.is_history_mode(),
            "Single endpoint should not be history mode"
        );
    }

    #[test]
    fn test_not_history_mode_no_time_specs() {
        // Array without time specs = NOT history mode (multi-ledger union)
        let query = json!({
            "from": ["ledger1:main", "ledger2:main"],
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(
            !spec.is_history_mode(),
            "No time specs should not be history mode"
        );
    }

    #[test]
    fn test_not_history_mode_partial_time_specs() {
        // Only one endpoint has time spec = NOT history mode
        let query = json!({
            "from": ["ledger:main@t:1", "ledger:main"],
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert!(
            !spec.is_history_mode(),
            "Partial time specs should not be history mode"
        );
    }

    // Error cases for explicit "to" syntax

    #[test]
    fn test_to_requires_single_from_graph() {
        // "to" key requires exactly one "from" graph
        let query = json!({
            "from": ["ledger:main@t:1", "ledger2:main@t:1"],
            "to": "ledger:main@t:latest",
            "select": ["?s"]
        });

        let result = DatasetSpec::from_json(&query);
        assert!(
            result.is_err(),
            "'to' with multiple 'from' graphs should error"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("exactly one"),
            "Error should mention 'exactly one': {}",
            err
        );
    }

    #[test]
    fn test_to_requires_same_ledger_as_from() {
        // "from" and "to" must reference the same ledger
        let query = json!({
            "from": "ledger1:main@t:1",
            "to": "ledger2:main@t:latest",
            "select": ["?s"]
        });

        let result = DatasetSpec::from_json(&query);
        assert!(
            result.is_err(),
            "'from' and 'to' with different ledgers should error"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("same ledger"),
            "Error should mention 'same ledger': {}",
            err
        );
    }

    #[test]
    fn test_to_requires_time_spec_on_from() {
        // "from" in history query must have time specification
        let query = json!({
            "from": "ledger:main",
            "to": "ledger:main@t:latest",
            "select": ["?s"]
        });

        let result = DatasetSpec::from_json(&query);
        assert!(result.is_err(), "'from' without time spec should error");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("time specification"),
            "Error should mention 'time specification': {}",
            err
        );
    }

    #[test]
    fn test_to_requires_time_spec_on_to() {
        // "to" must have time specification
        let query = json!({
            "from": "ledger:main@t:1",
            "to": "ledger:main",
            "select": ["?s"]
        });

        let result = DatasetSpec::from_json(&query);
        assert!(result.is_err(), "'to' without time spec should error");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("time specification"),
            "Error should mention 'time specification': {}",
            err
        );
    }

    #[test]
    fn test_parse_latest_keyword() {
        let query = json!({
            "from": "ledger:main@t:latest",
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main");
        assert!(matches!(
            spec.default_graphs[0].time_spec,
            Some(TimeSpec::Latest)
        ));
    }

    #[test]
    fn test_parse_latest_keyword_with_named_graph_fragment() {
        let query = json!({
            "from": "ledger:main@t:latest#txn-meta",
            "select": ["?s"]
        });

        let spec = DatasetSpec::from_json(&query).unwrap();
        assert_eq!(spec.default_graphs.len(), 1);
        assert_eq!(spec.default_graphs[0].identifier, "ledger:main#txn-meta");
        assert!(matches!(
            spec.default_graphs[0].time_spec,
            Some(TimeSpec::Latest)
        ));
    }
}
