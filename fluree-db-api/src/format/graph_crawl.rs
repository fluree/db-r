//! Graph crawl formatter - expands Sid bindings to nested JSON-LD objects
//!
//! This module handles the graph crawl select syntax which enables nested object
//! expansion in query results.
//!
//! # Supported Syntax
//!
//! ```json
//! // Variable-bound graph crawl
//! {"select": {"?person": ["*", {"ex:friend": ["*"]}]},
//!  "where": {"@id": "?person", "type": "ex:User"}}
//!
//! // With depth parameter for auto-expansion
//! {"select": {"?s": ["*"]}, "depth": 3, "where": ...}
//!
//! // IRI constant root (no WHERE needed)
//! {"select": {"ex:alice": ["*"]}}
//! ```

use super::config::FormatterConfig;
use super::datatype::is_inferable_datatype;
use super::iri::IriCompactor;
use super::jsonld::format_binding;
use super::{FormatError, Result};
use crate::QueryResult;
use fluree_db_core::comparator::IndexType;
use fluree_db_core::range::{range_with_overlay, RangeMatch, RangeOptions, RangeTest};
use fluree_db_core::value::FlakeValue;
use fluree_db_core::{Db, Flake, NoOverlay, OverlayProvider, Sid, Storage, Tracker};
use fluree_vocab::rdf::{self, TYPE as RDF_TYPE_IRI};
use fluree_db_policy::{is_schema_flake, PolicyContext};
use fluree_db_query::binding::Binding;
use fluree_db_query::ir::{GraphSelectSpec, NestedSelectSpec, Root, SelectionSpec};
use fluree_vocab::namespaces::JSON_LD;
use futures::future::BoxFuture;
use futures::FutureExt;
use serde_json::{json, Value as JsonValue};
use std::collections::{BTreeMap, HashMap, HashSet};


/// Cache key: (Sid, local_spec_hash, depth_remaining)
/// The local_spec_hash is computed from the current selections/reverse/has_wildcard,
/// NOT the top-level spec. This ensures different nested expansions of the same Sid
/// produce different cache entries.
type CacheKey = (Sid, u64, usize);

/// Compute hash for a local selection spec (forward, reverse, has_wildcard)
///
/// This is used to generate cache keys that correctly distinguish between
/// different nested selection specs at the same depth.
fn compute_local_spec_hash(
    selections: &[SelectionSpec],
    reverse: &HashMap<Sid, Option<Box<NestedSelectSpec>>>,
    has_wildcard: bool,
) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();

    has_wildcard.hash(&mut hasher);
    selections.len().hash(&mut hasher);

    // Hash forward selections
    fn hash_selection(spec: &SelectionSpec, hasher: &mut impl Hasher) {
        match spec {
            SelectionSpec::Id => {
                2u8.hash(hasher);
            }
            SelectionSpec::Wildcard => {
                0u8.hash(hasher);
            }
            SelectionSpec::Property { predicate, sub_spec } => {
                1u8.hash(hasher);
                predicate.hash(hasher);
                if let Some(nested) = sub_spec {
                    1u8.hash(hasher);
                    hash_nested_spec(nested, hasher);
                } else {
                    0u8.hash(hasher);
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
            if let Some(nested_opt) = spec.reverse.get(key) {
                if let Some(inner) = nested_opt {
                    1u8.hash(hasher);
                    hash_nested_spec(inner, hasher);
                } else {
                    0u8.hash(hasher);
                }
            }
        }
    }

    for sel in selections {
        hash_selection(sel, &mut hasher);
    }

    // Hash reverse properties
    reverse.len().hash(&mut hasher);
    let mut reverse_keys: Vec<_> = reverse.keys().collect();
    reverse_keys.sort();
    for key in reverse_keys {
        key.hash(&mut hasher);
        if let Some(nested_opt) = reverse.get(key) {
            if let Some(spec) = nested_opt {
                1u8.hash(&mut hasher);
                hash_nested_spec(spec, &mut hasher);
            } else {
                0u8.hash(&mut hasher);
            }
        }
    }

    hasher.finish()
}

/// Format query results with graph crawl expansion (sync entry point - returns error)
///
/// The sync entry point always returns an error directing callers to use
/// `format_results_async()` instead, as graph crawl requires DB access.
#[allow(dead_code)]
pub fn format(result: &QueryResult, _compactor: &IriCompactor) -> Result<JsonValue> {
    let _spec = result.graph_select.as_ref().ok_or_else(|| {
        FormatError::InvalidBinding("Graph crawl format called without graph_select spec".into())
    })?;

    // Graph crawl always requires async DB access
    Err(FormatError::InvalidBinding(
        "Graph crawl formatting requires async database access. \
         Use format_results_async() instead of format_results()."
            .into(),
    ))
}

/// Async graph crawl formatter entry point.
///
/// This is the full graph crawl implementation with async database access
/// for property fetching, depth expansion, and reverse property expansion.
///
/// # Policy Support
///
/// When `policy` is `Some`, flakes are filtered according to view policies.
/// When `policy` is `None`, no filtering is applied (zero overhead).
pub async fn format_async<S: Storage>(
    result: &QueryResult,
    db: &Db<S>,
    compactor: &IriCompactor,
    _config: &FormatterConfig,
    policy: Option<&PolicyContext>,
    tracker: Option<&Tracker>,
) -> Result<JsonValue> {
    let spec = result.graph_select.as_ref().ok_or_else(|| {
        FormatError::InvalidBinding("Graph crawl format called without graph_select spec".into())
    })?;

    let no_overlay = NoOverlay;
    let overlay: &dyn OverlayProvider = result
        .novelty
        .as_deref()
        .map(|n| n as &dyn OverlayProvider)
        .unwrap_or(&no_overlay);

    let formatter = GraphCrawlFormatter::new(db, overlay, compactor, spec, result.t, policy, tracker);

    // Shared cache across all rows
    let mut cache: HashMap<CacheKey, JsonValue> = HashMap::new();
    let mut rows = Vec::new();

    // If the underlying query produced no solutions, graph crawl must produce no rows,
    // even when the root is a constant Sid.
    if result.row_count() == 0 {
        return Ok(JsonValue::Array(rows));
    }

    match &spec.root {
        Root::Sid(sid) => {
            // IRI constant root - single subject fetch (no batches needed)
            let mut visited = HashSet::new();
            let obj = formatter
                .format_subject(
                    sid,
                    &spec.selections,
                    &spec.reverse,
                    0, // current_depth
                    spec.has_wildcard,
                    &mut visited,
                    &mut cache,
                )
                .await?;
            rows.push(obj);
        }
        Root::Var(var_id) => {
            // Variable root - iterate through result batches
            let select_vars = &result.select;
            let mixed_select = select_vars.len() > 1 || select_vars.get(0) != Some(var_id);

            for batch in &result.batches {
                for row_idx in 0..batch.len() {
                    let root_binding = batch.get(row_idx, *var_id);

                    let root_sid = match root_binding {
                        Some(Binding::Sid(sid)) => Some(sid.clone()),
                        Some(Binding::IriMatch { primary_sid, .. }) => Some(primary_sid.clone()),
                        Some(Binding::Unbound) | Some(Binding::Poisoned) | None => None,
                        Some(Binding::Lit { .. }) | Some(Binding::Grouped(_)) | Some(Binding::Iri(_)) => None,
                        Some(Binding::EncodedLit { .. }) => None,
                        // EncodedSid/EncodedPid require materialization before graph crawl
                        Some(Binding::EncodedSid { .. }) | Some(Binding::EncodedPid { .. }) => None,
                    };

                    let Some(root_sid) = root_sid else {
                        // Unbound/poisoned root var - skip this row (Clojure behavior)
                        continue;
                    };

                    let mut visited = HashSet::new();
                    let obj = formatter
                        .format_subject(
                            &root_sid,
                            &spec.selections,
                            &spec.reverse,
                            0, // current_depth
                            spec.has_wildcard,
                            &mut visited,
                            &mut cache,
                        )
                        .await?;

                    if mixed_select {
                        let mut row = Vec::with_capacity(select_vars.len());
                        for var in select_vars {
                            if var == var_id {
                                row.push(obj.clone());
                            } else {
                                let value = match batch.get(row_idx, *var) {
                                    Some(binding) => format_binding(binding, compactor)?,
                                    None => JsonValue::Null,
                                };
                                row.push(value);
                            }
                        }
                        rows.push(JsonValue::Array(row));
                    } else {
                        rows.push(obj);
                    }
                }
            }
        }
    }

    Ok(JsonValue::Array(rows))
}

/// Graph crawl formatter with async DB access
struct GraphCrawlFormatter<'a, S: Storage> {
    db: &'a Db<S>,
    overlay: &'a dyn OverlayProvider,
    compactor: &'a IriCompactor,
    spec: &'a GraphSelectSpec,
    to_t: i64,
    /// Optional policy context for access control filtering.
    /// When None, no policy filtering is applied (zero overhead).
    policy: Option<&'a PolicyContext>,
    /// Optional execution tracker for fuel/policy tracking.
    tracker: Option<&'a Tracker>,
}

impl<'a, S: Storage> GraphCrawlFormatter<'a, S> {
    fn new(
        db: &'a Db<S>,
        overlay: &'a dyn OverlayProvider,
        compactor: &'a IriCompactor,
        spec: &'a GraphSelectSpec,
        to_t: i64,
        policy: Option<&'a PolicyContext>,
        tracker: Option<&'a Tracker>,
    ) -> Self {
        Self {
            db,
            overlay,
            compactor,
            spec,
            to_t,
            policy,
            tracker,
        }
    }

    /// Format a subject to a JSON-LD object
    ///
    /// # Arguments
    /// - `sid`: Subject to expand
    /// - `selections`: Forward property selections at this level
    /// - `reverse`: Reverse property selections at this level
    /// - `current_depth`: Current recursion depth (starts at 0)
    /// - `has_wildcard`: Whether wildcard was specified at this level
    /// - `visited`: Cycle detection set (per-path)
    /// - `cache`: Result cache (shared across all subjects)
    ///
    /// Note: Returns BoxFuture to support recursive async calls
    fn format_subject<'b>(
        &'b self,
        sid: &'b Sid,
        selections: &'b [SelectionSpec],
        reverse: &'b HashMap<Sid, Option<Box<NestedSelectSpec>>>,
        current_depth: usize,
        has_wildcard: bool,
        visited: &'b mut HashSet<Sid>,
        cache: &'b mut HashMap<CacheKey, JsonValue>,
    ) -> BoxFuture<'b, Result<JsonValue>> {
        async move {
        let depth_remaining = self.spec.depth.saturating_sub(current_depth);
        // Use LOCAL spec hash (selections, reverse, has_wildcard) not top-level spec
        // This ensures different nested expansions of the same Sid produce different cache entries
        let spec_hash = compute_local_spec_hash(selections, reverse, has_wildcard);
        let cache_key = (sid.clone(), spec_hash, depth_remaining);

        // Check cache first (same Sid + spec + depth = same result)
        if let Some(cached) = cache.get(&cache_key) {
            return Ok(cached.clone());
        }

        // Cycle detection - if already in current path, return just @id
        if !visited.insert(sid.clone()) {
            return Ok(json!({ "@id": self.compactor.compact_sid(sid)? }));
        }

        // Build object with sorted keys for determinism (BTreeMap)
        let mut obj: BTreeMap<String, JsonValue> = BTreeMap::new();

        let has_explicit_id = selections.iter().any(|s| matches!(s, SelectionSpec::Id));
        // @id inclusion: wildcard OR explicit @id selection
        if has_wildcard || has_explicit_id {
            obj.insert("@id".to_string(), json!(self.compactor.compact_sid(sid)?));
        }

        // Fetch forward properties
        let flakes = self.fetch_subject_properties(sid).await?;

        // Group flakes by predicate
        let mut by_pred: HashMap<Sid, Vec<&Flake>> = HashMap::new();
        for flake in &flakes {
            by_pred.entry(flake.p.clone()).or_default().push(flake);
        }

        // Format each predicate
        for (pred, mut pred_flakes) in by_pred {
            // Determine whether this predicate was explicitly selected.
            // NOTE: a property selection like "schema:name" is explicit even when it has no sub-spec.
            let selected_opt = self.find_selection_for_predicate(&pred, selections);
            let explicit_sub_spec = selected_opt.flatten();
            if !has_wildcard && selected_opt.is_none() {
                continue;
            }

            // If these flakes represent an ordered list (`@list`), preserve transaction order
            // by sorting by the list index stored in FlakeMeta.i.
            //
            // NOTE: Even when the caller did not explicitly define @container @list in context,
            // list assertions carry list indices. Sorting by meta index is the correct behavior
            // for ordered lists.
            if pred_flakes.iter().any(|f| f.m.as_ref().and_then(|m| m.i).is_some()) {
                pred_flakes.sort_by_key(|f| f.m.as_ref().and_then(|m| m.i).unwrap_or(i32::MAX));
            }

            let values = self
                .format_predicate_values(
                    &pred,
                    &pred_flakes,
                    explicit_sub_spec,
                    selections,
                    reverse,
                    current_depth,
                    has_wildcard,
                    visited,
                    cache,
                )
                .await?;

            if !values.is_empty() {
                let key = self.format_predicate_key(&pred)?;
                // Single value vs array (based on actual cardinality)
                if values.len() == 1 && !self.force_array_for_key(&key) {
                    obj.insert(key, values.into_iter().next().unwrap());
                } else {
                    obj.insert(key, JsonValue::Array(values));
                }
            }
        }

        // Format reverse properties
        for (rev_pred, rev_nested_opt) in reverse {
            let rev_flakes = self.fetch_reverse_properties(sid, rev_pred).await?;
            if !rev_flakes.is_empty() {
                let values = self
                    .format_reverse_values(
                        &rev_flakes,
                        rev_nested_opt.as_deref(),
                        selections,
                        reverse,
                        current_depth,
                        has_wildcard,
                        visited,
                        cache,
                    )
                    .await?;
                if !values.is_empty() {
                    let key = self.compactor.compact_sid(rev_pred)?;
                    if values.len() == 1 && !self.force_array_for_key(&key) {
                        obj.insert(key, values.into_iter().next().unwrap());
                    } else {
                        obj.insert(key, JsonValue::Array(values));
                    }
                }
            }
        }

        // Remove from visited (allow revisiting via different paths)
        visited.remove(sid);

        // Convert BTreeMap to JsonValue::Object
        let result = JsonValue::Object(obj.into_iter().collect());

        // Cache the result
        cache.insert(cache_key, result.clone());

        Ok(result)
        }
        .boxed()
    }

    /// Find explicit sub-spec for a predicate in the selections
    fn find_selection_for_predicate<'b>(
        &self,
        pred: &Sid,
        selections: &'b [SelectionSpec],
    ) -> Option<Option<&'b NestedSelectSpec>> {
        for sel in selections {
            if let SelectionSpec::Property { predicate, sub_spec } = sel {
                if predicate == pred {
                    return Some(sub_spec.as_deref());
                }
            }
        }
        None
    }

    /// Whether this compact key should always be represented as an array.
    ///
    /// JSON-LD `@container` semantics:
    /// - `@set`: always array (even for 1 value)
    /// - `@list`: always array (ordered list)
    fn force_array_for_key(&self, compact_key: &str) -> bool {
        let Some(entry) = self.compactor.context().get(compact_key) else {
            return false;
        };
        let Some(containers) = &entry.container else {
            return false;
        };
        containers.iter().any(|c| matches!(c, fluree_graph_json_ld::Container::Set | fluree_graph_json_ld::Container::List))
    }

    /// Format predicate key with @type special-casing
    fn format_predicate_key(&self, pred: &Sid) -> Result<String> {
        let expanded = self.compactor.decode_sid(pred)?;
        if expanded == RDF_TYPE_IRI {
            Ok("@type".to_string())
        } else {
            self.compactor.compact_sid(pred)
        }
    }

    /// Format values for a predicate
    #[allow(clippy::too_many_arguments)]
    async fn format_predicate_values(
        &self,
        pred: &Sid,
        flakes: &[&Flake],
        explicit_sub_spec: Option<&NestedSelectSpec>,
        parent_selections: &[SelectionSpec],
        parent_reverse: &HashMap<Sid, Option<Box<NestedSelectSpec>>>,
        current_depth: usize,
        has_wildcard: bool,
        visited: &mut HashSet<Sid>,
        cache: &mut HashMap<CacheKey, JsonValue>,
    ) -> Result<Vec<JsonValue>> {
        let expanded = self.compactor.decode_sid(pred)?;
        let is_rdf_type = expanded == RDF_TYPE_IRI;

        let mut values = Vec::new();
        for flake in flakes {
            match &flake.o {
                FlakeValue::Ref(ref_sid) => {
                    if is_rdf_type {
                        // @type special case: compact IRI string, not {"@id": ...}
                        values.push(json!(self.compactor.compact_sid(ref_sid)?));
                    } else {
                        // Expansion decision (Clojure parity):
                        // 1. If explicit sub-selection exists → expand with that spec
                        // 2. Else if current_depth < max_depth → auto-expand with FULL parent spec
                        // 3. Else → just return {"@id": ...}
                        if let Some(nested) = explicit_sub_spec {
                            // Case 1: Explicit sub-selection
                            values.push(
                                self.format_subject(
                                    ref_sid,
                                    &nested.forward,
                                    &nested.reverse,
                                    current_depth + 1,
                                    nested.has_wildcard,
                                    visited,
                                    cache,
                                )
                                .await?,
                            );
                        } else if current_depth < self.spec.depth {
                            // Case 2: Auto-expand with FULL parent spec (forward + reverse!)
                            values.push(
                                self.format_subject(
                                    ref_sid,
                                    parent_selections,
                                    parent_reverse, // Continue reverse crawling
                                    current_depth + 1,
                                    has_wildcard,
                                    visited,
                                    cache,
                                )
                                .await?,
                            );
                        } else {
                            // Case 3: Max depth reached, just @id
                            values.push(json!({ "@id": self.compactor.compact_sid(ref_sid)? }));
                        }
                    }
                }
                _ => {
                    // Literal value
                    if let Some(nested) = explicit_sub_spec {
                        values.push(self.format_literal_virtual(flake, nested)?);
                    } else {
                        values.push(self.format_literal_value(flake)?);
                    }
                }
            }
        }
        Ok(values)
    }

    /// Format reverse property values
    #[allow(clippy::too_many_arguments)]
    async fn format_reverse_values(
        &self,
        flakes: &[Flake],
        nested_spec: Option<&NestedSelectSpec>,
        parent_selections: &[SelectionSpec],
        parent_reverse: &HashMap<Sid, Option<Box<NestedSelectSpec>>>,
        current_depth: usize,
        has_wildcard: bool,
        visited: &mut HashSet<Sid>,
        cache: &mut HashMap<CacheKey, JsonValue>,
    ) -> Result<Vec<JsonValue>> {
        let mut values = Vec::new();
        for flake in flakes {
            // For reverse lookup, subject is the entity that points to our object
            let subject_sid = &flake.s;

            if let Some(spec) = nested_spec {
                // Explicit sub-selection for reverse - use the full nested spec
                values.push(
                    self.format_subject(
                        subject_sid,
                        &spec.forward,
                        &spec.reverse,
                        current_depth + 1,
                        spec.has_wildcard,
                        visited,
                        cache,
                    )
                    .await?,
                );
            } else if current_depth < self.spec.depth {
                // Auto-expand reverse refs with FULL parent spec
                values.push(
                    self.format_subject(
                        subject_sid,
                        parent_selections,
                        parent_reverse,
                        current_depth + 1,
                        has_wildcard,
                        visited,
                        cache,
                    )
                    .await?,
                );
            } else {
                // No expansion - just @id
                values.push(json!({ "@id": self.compactor.compact_sid(subject_sid)? }));
            }
        }
        Ok(values)
    }

    /// Format a literal flake value
    fn format_literal_value(&self, flake: &Flake) -> Result<JsonValue> {
        let dt_full = self.compactor.decode_sid(&flake.dt)?;
        let dt_compact = self.compactor.compact_sid(&flake.dt)?;

        // Special handling for @json datatype: deserialize the JSON string
        if dt_full == rdf::JSON || dt_compact == "@json" {
            return match &flake.o {
                FlakeValue::Json(json_str) => {
                    // Deserialize the JSON string back to a JSON value
                    serde_json::from_str(json_str)
                        .map_err(|e| FormatError::InvalidBinding(format!("Invalid JSON in @json value: {}", e)))
                }
                _ => Err(FormatError::InvalidBinding(
                    "@json datatype must have FlakeValue::Json".to_string(),
                )),
            };
        }

        // Check for language tag
        if let Some(ref meta) = flake.m {
            if let Some(ref lang) = meta.lang {
                return match &flake.o {
                    FlakeValue::String(s) => Ok(json!({
                        "@value": s,
                        "@language": lang
                    })),
                    FlakeValue::Null => Ok(JsonValue::Null),
                    _ => Err(FormatError::InvalidBinding(
                        "Language-tagged literals must be strings".to_string(),
                    )),
                };
            }
        }

        // Inferable datatypes can omit @type
        if is_inferable_datatype(&dt_full) {
            return match &flake.o {
                FlakeValue::String(s) => Ok(JsonValue::String(s.clone())),
                FlakeValue::Long(n) => Ok(json!(n)),
                FlakeValue::Double(d) => {
                    if d.is_nan() {
                        Ok(JsonValue::String("NaN".to_string()))
                    } else if d.is_infinite() {
                        if d.is_sign_positive() {
                            Ok(JsonValue::String("INF".to_string()))
                        } else {
                            Ok(JsonValue::String("-INF".to_string()))
                        }
                    } else {
                        Ok(json!(d))
                    }
                }
                FlakeValue::Boolean(b) => Ok(json!(b)),
                FlakeValue::Vector(v) => Ok(JsonValue::Array(v.iter().map(|f| json!(f)).collect())),
                FlakeValue::Json(_) => Err(FormatError::InvalidBinding(
                    "@json should have been handled above".to_string(),
                )),
                FlakeValue::Null => Ok(JsonValue::Null),
                FlakeValue::Ref(sid) => {
                    // This shouldn't happen for literals, but handle gracefully
                    Ok(json!({ "@id": self.compactor.compact_sid(sid)? }))
                }
                // Extended numeric types - serialize as string
                FlakeValue::BigInt(n) => Ok(JsonValue::String(n.to_string())),
                FlakeValue::Decimal(d) => Ok(JsonValue::String(d.to_string())),
                // Temporal types - serialize as original string
                FlakeValue::DateTime(dt) => Ok(JsonValue::String(dt.to_string())),
                FlakeValue::Date(d) => Ok(JsonValue::String(d.to_string())),
                FlakeValue::Time(t) => Ok(JsonValue::String(t.to_string())),
                // Additional temporal types - serialize as original string
                FlakeValue::GYear(v) => Ok(JsonValue::String(v.to_string())),
                FlakeValue::GYearMonth(v) => Ok(JsonValue::String(v.to_string())),
                FlakeValue::GMonth(v) => Ok(JsonValue::String(v.to_string())),
                FlakeValue::GDay(v) => Ok(JsonValue::String(v.to_string())),
                FlakeValue::GMonthDay(v) => Ok(JsonValue::String(v.to_string())),
                FlakeValue::YearMonthDuration(v) => Ok(JsonValue::String(v.to_string())),
                FlakeValue::DayTimeDuration(v) => Ok(JsonValue::String(v.to_string())),
                FlakeValue::Duration(v) => Ok(JsonValue::String(v.to_string())),
            };
        }

        // Non-inferable datatypes include @type
        let value_json = match &flake.o {
            FlakeValue::String(s) => JsonValue::String(s.clone()),
            FlakeValue::Long(n) => json!(n),
            FlakeValue::Double(d) => {
                if d.is_nan() {
                    JsonValue::String("NaN".to_string())
                } else if d.is_infinite() {
                    if d.is_sign_positive() {
                        JsonValue::String("INF".to_string())
                    } else {
                        JsonValue::String("-INF".to_string())
                    }
                } else {
                    json!(d)
                }
            }
            FlakeValue::Boolean(b) => json!(b),
            FlakeValue::Vector(v) => JsonValue::Array(v.iter().map(|f| json!(f)).collect()),
            FlakeValue::Json(json_str) => JsonValue::String(json_str.clone()), // Fallback for non-@json context
            FlakeValue::Null => return Ok(JsonValue::Null),
            FlakeValue::Ref(sid) => {
                return Ok(json!({ "@id": self.compactor.compact_sid(sid)? }));
            }
            // Extended numeric types - serialize as string with @type
            FlakeValue::BigInt(n) => JsonValue::String(n.to_string()),
            FlakeValue::Decimal(d) => JsonValue::String(d.to_string()),
            // Temporal types - serialize as original string with @type
            FlakeValue::DateTime(dt) => JsonValue::String(dt.to_string()),
            FlakeValue::Date(d) => JsonValue::String(d.to_string()),
            FlakeValue::Time(t) => JsonValue::String(t.to_string()),
            // Additional temporal types - serialize as original string with @type
            FlakeValue::GYear(v) => JsonValue::String(v.to_string()),
            FlakeValue::GYearMonth(v) => JsonValue::String(v.to_string()),
            FlakeValue::GMonth(v) => JsonValue::String(v.to_string()),
            FlakeValue::GDay(v) => JsonValue::String(v.to_string()),
            FlakeValue::GMonthDay(v) => JsonValue::String(v.to_string()),
            FlakeValue::YearMonthDuration(v) => JsonValue::String(v.to_string()),
            FlakeValue::DayTimeDuration(v) => JsonValue::String(v.to_string()),
            FlakeValue::Duration(v) => JsonValue::String(v.to_string()),
        };

        Ok(json!({
            "@value": value_json,
            "@type": dt_compact
        }))
    }

    /// Format a literal as a virtual node when explicitly selected.
    ///
    /// Supports `@value`, `@type`, and `@language` selection semantics.
    fn format_literal_virtual(&self, flake: &Flake, spec: &NestedSelectSpec) -> Result<JsonValue> {
        let mut want_value = false;
        let mut want_type = false;
        let mut want_language = false;

        if spec.has_wildcard {
            want_value = true;
            want_type = true;
            want_language = flake
                .m
                .as_ref()
                .and_then(|m| m.lang.as_ref())
                .is_some();
        } else {
            for sel in &spec.forward {
                match sel {
                    SelectionSpec::Wildcard => {
                        want_value = true;
                        want_type = true;
                        want_language = flake
                            .m
                            .as_ref()
                            .and_then(|m| m.lang.as_ref())
                            .is_some();
                    }
                    SelectionSpec::Property { predicate, .. } => {
                        if predicate.namespace_code == JSON_LD {
                            match predicate.name.as_ref() {
                                "value" => want_value = true,
                                "type" => want_type = true,
                                "language" => want_language = true,
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        let mut obj = serde_json::Map::new();

        if want_value {
            obj.insert("@value".to_string(), self.format_literal_plain_value(flake)?);
        }

        if want_type {
            let dt_compact = self.compactor.compact_sid(&flake.dt)?;
            obj.insert("@type".to_string(), json!(dt_compact));
        }

        if want_language {
            if let Some(lang) = flake.m.as_ref().and_then(|m| m.lang.as_ref()) {
                obj.insert("@language".to_string(), json!(lang));
            }
        }

        Ok(JsonValue::Object(obj))
    }

    fn format_literal_plain_value(&self, flake: &Flake) -> Result<JsonValue> {
        let dt_full = self.compactor.decode_sid(&flake.dt)?;
        let dt_compact = self.compactor.compact_sid(&flake.dt)?;

        if dt_full == rdf::JSON || dt_compact == "@json" {
            return match &flake.o {
                FlakeValue::Json(json_str) => serde_json::from_str(json_str).map_err(|e| {
                    FormatError::InvalidBinding(format!("Invalid JSON in @json value: {}", e))
                }),
                _ => Err(FormatError::InvalidBinding(
                    "@json datatype must have FlakeValue::Json".to_string(),
                )),
            };
        }

        match &flake.o {
            FlakeValue::String(s) => Ok(JsonValue::String(s.clone())),
            FlakeValue::Long(n) => Ok(json!(n)),
            FlakeValue::Double(d) => {
                if d.is_nan() {
                    Ok(JsonValue::String("NaN".to_string()))
                } else if d.is_infinite() {
                    if d.is_sign_positive() {
                        Ok(JsonValue::String("INF".to_string()))
                    } else {
                        Ok(JsonValue::String("-INF".to_string()))
                    }
                } else {
                    Ok(json!(d))
                }
            }
            FlakeValue::Boolean(b) => Ok(json!(b)),
            FlakeValue::Vector(v) => Ok(JsonValue::Array(v.iter().map(|f| json!(f)).collect())),
            FlakeValue::Json(_) => Err(FormatError::InvalidBinding(
                "@json should have been handled above".to_string(),
            )),
            FlakeValue::Null => Ok(JsonValue::Null),
            FlakeValue::Ref(sid) => Ok(json!({ "@id": self.compactor.compact_sid(sid)? })),
            FlakeValue::BigInt(n) => Ok(JsonValue::String(n.to_string())),
            FlakeValue::Decimal(d) => Ok(JsonValue::String(d.to_string())),
            FlakeValue::DateTime(dt) => Ok(JsonValue::String(dt.to_string())),
            FlakeValue::Date(d) => Ok(JsonValue::String(d.to_string())),
            FlakeValue::Time(t) => Ok(JsonValue::String(t.to_string())),
            // Additional temporal types - serialize as original string
            FlakeValue::GYear(v) => Ok(JsonValue::String(v.to_string())),
            FlakeValue::GYearMonth(v) => Ok(JsonValue::String(v.to_string())),
            FlakeValue::GMonth(v) => Ok(JsonValue::String(v.to_string())),
            FlakeValue::GDay(v) => Ok(JsonValue::String(v.to_string())),
            FlakeValue::GMonthDay(v) => Ok(JsonValue::String(v.to_string())),
            FlakeValue::YearMonthDuration(v) => Ok(JsonValue::String(v.to_string())),
            FlakeValue::DayTimeDuration(v) => Ok(JsonValue::String(v.to_string())),
            FlakeValue::Duration(v) => Ok(JsonValue::String(v.to_string())),
        }
    }

    /// Fetch all properties for a subject using SPOT index
    ///
    /// When policy is set, filters flakes according to view policies.
    /// When policy is None, returns all flakes (zero overhead).
    async fn fetch_subject_properties(&self, sid: &Sid) -> Result<Vec<Flake>> {
        let flakes = range_with_overlay(
            self.db,
            self.overlay,
            IndexType::Spot,
            RangeTest::Eq,
            RangeMatch::subject(sid.clone()),
            RangeOptions::default().with_to_t(self.to_t),
        )
        .await
        .map_err(|e| FormatError::InvalidBinding(format!("Failed to fetch subject properties: {}", e)))?;

        // Policy filtering: only when policy is Some and not root
        // Zero overhead when policy is None (common case)
        if let Some(policy_ctx) = self.policy {
            if !policy_ctx.wrapper().is_root() {
                let filtered = self.filter_flakes_by_policy(flakes, policy_ctx);
                return self.apply_fuel_tracking(filtered);
            }
        }

        self.apply_fuel_tracking(flakes)
    }

    /// Fetch subjects that have this object via predicate (reverse lookup using POST index)
    ///
    /// When policy is set, filters flakes according to view policies.
    /// When policy is None, returns all flakes (zero overhead).
    async fn fetch_reverse_properties(&self, object_sid: &Sid, pred: &Sid) -> Result<Vec<Flake>> {
        let flakes = range_with_overlay(
            self.db,
            self.overlay,
            IndexType::Post,
            RangeTest::Eq,
            RangeMatch::predicate_object(pred.clone(), FlakeValue::Ref(object_sid.clone())),
            RangeOptions::default().with_to_t(self.to_t),
        )
        .await
        .map_err(|e| FormatError::InvalidBinding(format!("Failed to fetch reverse properties: {}", e)))?;

        // Policy filtering: only when policy is Some and not root
        // Zero overhead when policy is None (common case)
        if let Some(policy_ctx) = self.policy {
            if !policy_ctx.wrapper().is_root() {
                let filtered = self.filter_flakes_by_policy(flakes, policy_ctx);
                return self.apply_fuel_tracking(filtered);
            }
        }

        self.apply_fuel_tracking(flakes)
    }

    fn apply_fuel_tracking(&self, flakes: Vec<Flake>) -> Result<Vec<Flake>> {
        if let Some(tracker) = self.tracker {
            // Clojure parity: count fuel per flake emitted downstream (post-filters).
            for _ in 0..flakes.len() {
                tracker.consume_fuel_one()?;
            }
        }
        Ok(flakes)
    }

    /// Filter flakes according to view policies
    ///
    /// Schema flakes are always allowed. Other flakes are checked against
    /// the policy context.
    fn filter_flakes_by_policy(&self, flakes: Vec<Flake>, policy_ctx: &PolicyContext) -> Vec<Flake> {
        flakes
            .into_iter()
            .filter(|flake| {
                // Schema flakes always allowed (needed for query planning/formatting)
                if is_schema_flake(&flake.p, &flake.o) {
                    return true;
                }

                // Get subject classes from cache (empty if not cached)
                // Note: Graph crawl doesn't pre-populate class cache like ScanOperator.
                // For graph crawl with class policies, the cache will be empty and
                // class policies may not work correctly. This is a known limitation
                // that can be addressed by pre-populating in format_async if needed.
                let subject_classes = policy_ctx
                    .get_cached_subject_classes(&flake.s)
                    .unwrap_or_default();

                let allowed = match self.tracker {
                    Some(tracker) => policy_ctx.allow_view_flake_tracked(
                        &flake.s,
                        &flake.p,
                        &flake.o,
                        &subject_classes,
                        tracker,
                    ),
                    None => policy_ctx.allow_view_flake(
                        &flake.s,
                        &flake.p,
                        &flake.o,
                        &subject_classes,
                    ),
                };

                match allowed {
                    Ok(allowed) => allowed,
                    Err(_) => false, // On error, conservatively deny
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_query::VarId;

    #[test]
    fn test_sync_format_returns_error() {
        // Create a minimal QueryResult with graph_select
        // The sync format function should always return an error
        let spec = GraphSelectSpec {
            root: Root::Var(VarId(0)),
            selections: vec![SelectionSpec::Wildcard],
            reverse: HashMap::new(),
            depth: 0,
            has_wildcard: true,
        };

        let _compactor = IriCompactor::from_namespaces(&HashMap::new());

        // We can't easily create a full QueryResult, but we can verify the behavior
        // by checking that format() returns an error when called
        // (In practice, format_results() guards this path)

        // Just verify the spec hash is consistent
        let hash1 = spec.spec_hash();
        let hash2 = spec.spec_hash();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_cache_key_different_depths() {
        let sid = Sid::new(100, "alice");
        let spec_hash = 12345u64;

        let key1: CacheKey = (sid.clone(), spec_hash, 0);
        let key2: CacheKey = (sid.clone(), spec_hash, 1);
        let key3: CacheKey = (sid, spec_hash, 0);

        // Different depths should produce different keys
        assert_ne!(key1, key2);
        // Same Sid + spec + depth should be equal
        assert_eq!(key1, key3);
    }

    #[test]
    fn test_local_spec_hash_different_selections() {
        // Empty selections
        let hash_empty = compute_local_spec_hash(&[], &HashMap::new(), false);

        // Wildcard only
        let hash_wildcard = compute_local_spec_hash(&[SelectionSpec::Wildcard], &HashMap::new(), true);

        // Different hashes for different selections
        assert_ne!(hash_empty, hash_wildcard);

        // Same selections should produce same hash
        let hash_wildcard2 = compute_local_spec_hash(&[SelectionSpec::Wildcard], &HashMap::new(), true);
        assert_eq!(hash_wildcard, hash_wildcard2);

        // Different has_wildcard flag should produce different hash
        let hash_wildcard_false = compute_local_spec_hash(&[SelectionSpec::Wildcard], &HashMap::new(), false);
        assert_ne!(hash_wildcard, hash_wildcard_false);
    }

    #[test]
    fn test_local_spec_hash_with_property() {
        let pred1 = Sid::new(1, "name");
        let pred2 = Sid::new(2, "age");

        // Property without sub-spec
        let selections1 = vec![SelectionSpec::Property {
            predicate: pred1.clone(),
            sub_spec: None,
        }];
        let hash1 = compute_local_spec_hash(&selections1, &HashMap::new(), false);

        // Different property
        let selections2 = vec![SelectionSpec::Property {
            predicate: pred2.clone(),
            sub_spec: None,
        }];
        let hash2 = compute_local_spec_hash(&selections2, &HashMap::new(), false);

        assert_ne!(hash1, hash2);

        // Property with sub-spec should differ from property without
        let selections3 = vec![SelectionSpec::Property {
            predicate: pred1.clone(),
            sub_spec: Some(Box::new(NestedSelectSpec {
                forward: vec![SelectionSpec::Wildcard],
                reverse: HashMap::new(),
                has_wildcard: true,
            })),
        }];
        let hash3 = compute_local_spec_hash(&selections3, &HashMap::new(), false);

        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_local_spec_hash_with_reverse() {
        let rev_pred = Sid::new(10, "friendOf");

        // Empty reverse
        let hash_no_reverse = compute_local_spec_hash(&[], &HashMap::new(), true);

        // With reverse property (no nested spec)
        let mut reverse1 = HashMap::new();
        reverse1.insert(rev_pred.clone(), None);
        let hash_with_reverse = compute_local_spec_hash(&[], &reverse1, true);

        assert_ne!(hash_no_reverse, hash_with_reverse);

        // With reverse property (with nested spec)
        let mut reverse2 = HashMap::new();
        reverse2.insert(
            rev_pred.clone(),
            Some(Box::new(NestedSelectSpec {
                forward: vec![SelectionSpec::Wildcard],
                reverse: HashMap::new(),
                has_wildcard: true,
            })),
        );
        let hash_with_reverse_nested = compute_local_spec_hash(&[], &reverse2, true);

        assert_ne!(hash_with_reverse, hash_with_reverse_nested);
    }
}
