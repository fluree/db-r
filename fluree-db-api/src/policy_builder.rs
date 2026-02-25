//! Policy building from query connection options
//!
//! This module provides functions to build `PolicyContext` from query connection options:
//! - Identity-based policies (via `f:policyClass` on identity subject)
//! - Class-based policies (policies of given types/classes)
//! - Inline policy JSON-LD
//!
//! # Clojure Parity
//!
//! This mirrors the Clojure functions:
//! - `wrap-identity-policy` - Load policies via identity's `f:policyClass`
//! - `wrap-class-policy` - Load policies of given classes
//! - `wrap-policy` - Parse inline policy JSON-LD

use crate::dataset::QueryConnectionOptions;
use crate::error::{ApiError, Result};
use fluree_db_core::{is_rdf_type, ClassPropertyUsage, ClassStatEntry, IndexStats};
use fluree_db_core::{FlakeValue, GraphDbRef, IndexType, LedgerSnapshot, Sid};
use fluree_db_novelty::Novelty;
use fluree_db_policy::{
    build_policy_set, PolicyAction, PolicyContext, PolicyQuery, PolicyRestriction, PolicyValue,
    PolicyWrapper, TargetMode,
};
use fluree_db_query::{execute_pattern_with_overlay_at, Binding, Term, TriplePattern, VarRegistry};
use fluree_vocab::rdf::TYPE as RDF_TYPE_IRI;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};

async fn augment_class_property_stats_from_novelty(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    mut stats: IndexStats,
    novelty: &Novelty,
) -> Result<IndexStats> {
    // Collect per-subject property usage from novelty (ignore rdf:type itself).
    let mut subject_props: HashMap<Sid, HashSet<Sid>> = HashMap::new();
    for flake_id in novelty.iter_index(IndexType::Post) {
        let flake = novelty.get_flake(flake_id);
        if is_rdf_type(&flake.p) {
            continue;
        }
        subject_props
            .entry(flake.s.clone())
            .or_default()
            .insert(flake.p.clone());
    }

    if subject_props.is_empty() {
        return Ok(stats);
    }

    // Look up each subject's current classes as-of (db + overlay, to_t).
    let subjects: Vec<Sid> = subject_props.keys().cloned().collect();
    let db = fluree_db_core::GraphDbRef::new(snapshot, 0, overlay, to_t);
    let subject_classes = fluree_db_policy::lookup_subject_classes(&subjects, db)
        .await
        .map_err(|e| ApiError::Internal(format!("policy class lookup failed: {}", e)))?;

    // Build mutable class -> (count, properties set) view.
    let mut class_map: HashMap<Sid, (u64, HashSet<Sid>)> = HashMap::new();
    if let Some(ref classes) = stats.classes {
        for c in classes {
            let props: HashSet<Sid> = c
                .properties
                .iter()
                .map(|u| u.property_sid.clone())
                .collect();
            class_map.insert(c.class_sid.clone(), (c.count, props));
        }
    }

    // Attribute novelty properties to the subject's current classes.
    for (subject, props) in subject_props {
        let Some(classes) = subject_classes.get(&subject) else {
            continue;
        };
        for class_sid in classes {
            let entry = class_map
                .entry(class_sid.clone())
                .or_insert_with(|| (0, HashSet::new()));
            entry.1.extend(props.iter().cloned());
        }
    }

    // Convert back to deterministic `ClassStatEntry` list.
    let mut out_classes: Vec<ClassStatEntry> = class_map
        .into_iter()
        .map(|(class_sid, (count, props))| {
            let mut props_vec: Vec<ClassPropertyUsage> = props
                .into_iter()
                .map(|property_sid| ClassPropertyUsage {
                    property_sid,
                    datatypes: Vec::new(),
                    langs: Vec::new(),
                    ref_classes: Vec::new(),
                })
                .collect();
            props_vec.sort_by(|a, b| a.property_sid.cmp(&b.property_sid));
            ClassStatEntry {
                class_sid,
                count,
                properties: props_vec,
            }
        })
        .collect();
    out_classes.sort_by(|a, b| a.class_sid.cmp(&b.class_sid));
    stats.classes = if out_classes.is_empty() {
        None
    } else {
        Some(out_classes)
    };

    Ok(stats)
}

// ============================================================================
// Constants - Fluree policy vocabulary IRIs (from fluree-vocab)
// ============================================================================

use fluree_vocab::{fluree, policy_iris};

// ============================================================================
// Public API
// ============================================================================

/// Build a `PolicyContext` from `QueryConnectionOptions`.
///
/// Handles the three policy modes:
/// 1. **identity**: Query for policies via the identity's `f:policyClass` property
/// 2. **policy_class**: Query for policies of the given class types
/// 3. **policy**: Parse inline policy JSON-LD
///
/// Priority follows Clojure semantics: identity > policy_class > policy
///
/// # Arguments
///
/// * `snapshot` - The database snapshot to query against
/// * `overlay` - Overlay provider for query execution
/// * `novelty_for_stats` - Optional novelty for computing current stats (needed for f:onClass)
/// * `to_t` - Time bound for queries
/// * `opts` - Query connection options with policy configuration
pub async fn build_policy_context_from_opts(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    novelty_for_stats: Option<&Novelty>,
    to_t: i64,
    opts: &QueryConnectionOptions,
) -> Result<PolicyContext> {
    // Build policy values map first (SID mappings for policy variables)
    let mut policy_values = build_policy_values(snapshot, &opts.policy_values)?;

    // Resolve identity SID (used for ?$identity binding)
    // Priority: opts.identity > policy_values["?$identity"]
    let identity_sid = if let Some(identity_iri) = &opts.identity {
        // Explicit identity option takes precedence
        let sid = resolve_iri_to_sid(snapshot, identity_iri)?;
        // Also add to policy_values so policy queries can bind ?$identity
        policy_values.insert("?$identity".to_string(), sid.clone());
        Some(sid)
    } else if let Some(sid) = policy_values.get("?$identity") {
        // Identity provided via policy_values (Clojure parity for wrap-policy)
        Some(sid.clone())
    } else if let Some(pv) = &opts.policy_values {
        // Check if ?$identity was provided but failed to encode (treat as error)
        if pv.contains_key("?$identity") {
            return Err(ApiError::query(
                "?$identity provided in policy-values but could not be encoded",
            ));
        }
        None
    } else {
        None
    };

    // Load or parse policies based on options.
    // Priority follows Clojure semantics: identity > policy_class > policy
    // If identity is present, it triggers f:policyClass lookup AND binds ?$identity.
    // For inline policies with identity binding, use policy_values["?$identity"] instead.
    let restrictions = if let Some(identity) = &opts.identity {
        // Identity-based: query for policies via f:policyClass (highest priority)
        load_policies_by_identity(snapshot, overlay, to_t, identity).await?
    } else if let Some(classes) = &opts.policy_class {
        // Class-based: query for policies of given types
        load_policies_by_class(snapshot, overlay, to_t, classes).await?
    } else if let Some(policy_json) = &opts.policy {
        // Inline: parse policy JSON-LD (lowest priority)
        parse_inline_policy(snapshot, policy_json)?
    } else {
        // No policy specified - return empty (will use default_allow)
        vec![]
    };

    let has_class_policies = restrictions.iter().any(|r| r.class_policy);

    // Build policy sets (view and modify)
    //
    // Stats are critical for f:onClass policies - they need class→property relationships
    // to know which properties to index. Without stats, OnClass policies only match
    // @id and rdf:type properties (the implicit ones).
    //
    // We use current_stats() which merges indexed stats with novelty updates.
    // This ensures class→property relationships from uncommitted transactions are included.
    let mut stats: Option<IndexStats> = if let Some(novelty) = novelty_for_stats {
        let indexed = snapshot.stats.as_ref().cloned().unwrap_or_default();
        Some(fluree_db_novelty::current_stats(&indexed, novelty))
    } else {
        snapshot.stats.clone()
    };

    // Ensure class→property mappings include novelty property usage even when rdf:type
    // is not restated for updated subjects. Without this, f:onClass policies may fail
    // to be indexed under newly introduced properties and can be skipped entirely.
    if has_class_policies {
        if let (Some(novelty), Some(current)) = (novelty_for_stats, stats.take()) {
            stats = Some(
                augment_class_property_stats_from_novelty(
                    snapshot, overlay, to_t, current, novelty,
                )
                .await?,
            );
        }
    }

    let view_set = build_policy_set(restrictions.clone(), stats.as_ref(), PolicyAction::View);
    let modify_set = build_policy_set(restrictions, stats.as_ref(), PolicyAction::Modify);

    // Check if this is a root policy (unrestricted access)
    let is_root = view_set.restrictions.is_empty() && modify_set.restrictions.is_empty();

    // Create wrapper
    let wrapper = PolicyWrapper::new(
        view_set,
        modify_set,
        is_root,
        opts.default_allow,
        policy_values,
    );

    // Create context with identity
    Ok(PolicyContext::new(wrapper, identity_sid))
}

// ============================================================================
// Identity-based policy loading
// ============================================================================

/// Load policies by querying the identity's `f:policyClass` property.
///
/// Clojure equivalent: `wrap-identity-policy`
///
/// Query pattern:
/// ```sparql
/// SELECT ?policy WHERE {
///   <identity> f:policyClass ?class .
///   ?policy a ?class .
/// }
/// ```
async fn load_policies_by_identity(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    identity_iri: &str,
) -> Result<Vec<PolicyRestriction>> {
    // Step 1: Get policy classes for identity
    let identity_sid = resolve_iri_to_sid(snapshot, identity_iri)?;
    let policy_class_sid = resolve_iri_to_sid(snapshot, policy_iris::POLICY_CLASS)?;

    let mut vars = VarRegistry::new();
    let class_var = vars.get_or_insert("?class");

    // Query: <identity> f:policyClass ?class
    let pattern = TriplePattern::new(
        Term::Sid(identity_sid.clone()),
        Term::Sid(policy_class_sid),
        Term::Var(class_var),
    );

    let db = GraphDbRef::new(snapshot, 0, overlay, to_t);
    let batches = execute_pattern_with_overlay_at(db, &vars, pattern, None).await?;

    // Collect class SIDs
    let mut class_sids: Vec<Sid> = Vec::new();
    for batch in &batches {
        for row in 0..batch.len() {
            if let Some(binding) = batch.get(row, class_var) {
                if let Some(sid) = binding.as_sid() {
                    class_sids.push(sid.clone());
                }
            }
        }
    }

    if class_sids.is_empty() {
        return Ok(vec![]);
    }

    // Step 2: Get policies of those classes
    load_policies_of_classes(snapshot, overlay, to_t, &class_sids).await
}

// ============================================================================
// Class-based policy loading
// ============================================================================

/// Load policies by querying for subjects of the given class types.
///
/// Clojure equivalent: `wrap-class-policy`
async fn load_policies_by_class(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    class_iris: &[String],
) -> Result<Vec<PolicyRestriction>> {
    // Resolve class IRIs to SIDs
    let mut class_sids = Vec::with_capacity(class_iris.len());
    for iri in class_iris {
        class_sids.push(resolve_iri_to_sid(snapshot, iri)?);
    }

    load_policies_of_classes(snapshot, overlay, to_t, &class_sids).await
}

/// Load policies that are instances of the given classes.
///
/// Query pattern:
/// ```sparql
/// SELECT ?policy WHERE {
///   ?policy a ?class .
/// }
/// ```
/// Then load each policy's properties.
async fn load_policies_of_classes(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    class_sids: &[Sid],
) -> Result<Vec<PolicyRestriction>> {
    let rdf_type_sid = resolve_iri_to_sid(snapshot, RDF_TYPE_IRI)?;

    let mut vars = VarRegistry::new();
    let policy_var = vars.get_or_insert("?policy");

    // Collect all policy subjects
    let mut policy_sids: HashSet<Sid> = HashSet::new();

    for class_sid in class_sids {
        // Query: ?policy a <class>
        let pattern = TriplePattern::new(
            Term::Var(policy_var),
            Term::Sid(rdf_type_sid.clone()),
            Term::Sid(class_sid.clone()),
        );

        let db = GraphDbRef::new(snapshot, 0, overlay, to_t);
        let batches = execute_pattern_with_overlay_at(db, &vars, pattern, None).await?;

        for batch in &batches {
            for row in 0..batch.len() {
                if let Some(binding) = batch.get(row, policy_var) {
                    if let Some(sid) = binding.as_sid() {
                        policy_sids.insert(sid.clone());
                    }
                }
            }
        }
    }

    // Load each policy's restrictions
    let mut restrictions = Vec::new();
    for policy_sid in policy_sids {
        if let Some(restriction) =
            load_policy_restriction(snapshot, overlay, to_t, &policy_sid).await?
        {
            restrictions.push(restriction);
        }
    }

    Ok(restrictions)
}

/// Load a single policy's restriction from the database.
///
/// NOTE: This function uses explicit predicate queries (not wildcard `?pred`)
/// because the scan layer filters out internal `fluree:ledger` predicates
/// when the predicate is a variable. Since all policy vocabulary predicates
/// are in the `fluree:ledger` namespace, we must query them explicitly.
async fn load_policy_restriction(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    policy_sid: &Sid,
) -> Result<Option<PolicyRestriction>> {
    // Collect properties using explicit predicate queries
    // (wildcard ?pred would be filtered by scan layer for fluree:ledger predicates)
    let mut allow: Option<bool> = None;
    let mut action: Option<PolicyAction> = None;
    let mut on_property: HashSet<Sid> = HashSet::new();
    let mut on_subject: HashSet<Sid> = HashSet::new();
    let mut on_class: HashSet<Sid> = HashSet::new();
    let mut required = false;
    let mut message: Option<String> = None;
    let mut policy_query_json: Option<String> = None;

    // Resolve predicate SIDs we need to query
    let view_sid = resolve_iri_to_sid(snapshot, policy_iris::VIEW).ok();
    let modify_sid = resolve_iri_to_sid(snapshot, policy_iris::MODIFY).ok();

    // Query each policy predicate explicitly
    // f:allow
    if let Ok(allow_sid) = resolve_iri_to_sid(snapshot, policy_iris::ALLOW) {
        let bindings = query_predicate(snapshot, overlay, to_t, policy_sid, &allow_sid).await?;
        for binding in bindings {
            if let Binding::Lit {
                val: FlakeValue::Boolean(b),
                ..
            } = binding
            {
                allow = Some(b);
                break;
            }
        }
    }

    // f:action - collect all action values to determine View, Modify, or Both
    if let Ok(action_sid) = resolve_iri_to_sid(snapshot, policy_iris::ACTION) {
        let bindings = query_predicate(snapshot, overlay, to_t, policy_sid, &action_sid).await?;
        let mut has_view = false;
        let mut has_modify = false;
        for binding in bindings {
            if let Some(action_ref) = binding.as_sid() {
                if view_sid.as_ref() == Some(action_ref) {
                    has_view = true;
                } else if modify_sid.as_ref() == Some(action_ref) {
                    has_modify = true;
                }
            }
        }
        action = match (has_view, has_modify) {
            (true, true) => Some(PolicyAction::Both),
            (true, false) => Some(PolicyAction::View),
            (false, true) => Some(PolicyAction::Modify),
            (false, false) => None,
        };
    }

    // f:onProperty (can have multiple values)
    if let Ok(pred_sid) = resolve_iri_to_sid(snapshot, policy_iris::ON_PROPERTY) {
        let bindings = query_predicate(snapshot, overlay, to_t, policy_sid, &pred_sid).await?;
        for binding in bindings {
            if let Some(sid) = binding.as_sid() {
                on_property.insert(sid.clone());
            }
        }
    }

    // f:onSubject (can have multiple values)
    if let Ok(pred_sid) = resolve_iri_to_sid(snapshot, policy_iris::ON_SUBJECT) {
        let bindings = query_predicate(snapshot, overlay, to_t, policy_sid, &pred_sid).await?;
        for binding in bindings {
            if let Some(sid) = binding.as_sid() {
                on_subject.insert(sid.clone());
            }
        }
    }

    // f:onClass (can have multiple values)
    if let Ok(pred_sid) = resolve_iri_to_sid(snapshot, policy_iris::ON_CLASS) {
        let bindings = query_predicate(snapshot, overlay, to_t, policy_sid, &pred_sid).await?;
        for binding in bindings {
            if let Some(sid) = binding.as_sid() {
                on_class.insert(sid.clone());
            }
        }
    }

    // f:required
    if let Ok(pred_sid) = resolve_iri_to_sid(snapshot, policy_iris::REQUIRED) {
        let bindings = query_predicate(snapshot, overlay, to_t, policy_sid, &pred_sid).await?;
        for binding in bindings {
            if let Binding::Lit {
                val: FlakeValue::Boolean(b),
                ..
            } = binding
            {
                required = b;
                break;
            }
        }
    }

    // f:exMessage
    if let Ok(pred_sid) = resolve_iri_to_sid(snapshot, policy_iris::EX_MESSAGE) {
        let bindings = query_predicate(snapshot, overlay, to_t, policy_sid, &pred_sid).await?;
        for binding in bindings {
            if let Binding::Lit {
                val: FlakeValue::String(s),
                ..
            } = binding
            {
                message = Some(s.clone());
                break;
            }
        }
    }

    // f:query
    if let Ok(pred_sid) = resolve_iri_to_sid(snapshot, policy_iris::QUERY) {
        let bindings = query_predicate(snapshot, overlay, to_t, policy_sid, &pred_sid).await?;
        for binding in bindings {
            match binding {
                Binding::Lit {
                    val: FlakeValue::Json(s),
                    ..
                } => {
                    policy_query_json = Some(s.clone());
                    break;
                }
                Binding::Lit {
                    val: FlakeValue::String(s),
                    ..
                } => {
                    policy_query_json = Some(s.clone());
                    break;
                }
                _ => {}
            }
        }
    }

    // Determine target mode and targets
    let (target_mode, targets, for_classes) = if !on_property.is_empty() {
        (TargetMode::OnProperty, on_property, HashSet::new())
    } else if !on_subject.is_empty() {
        (TargetMode::OnSubject, on_subject, HashSet::new())
    } else if !on_class.is_empty() {
        (TargetMode::OnClass, HashSet::new(), on_class)
    } else {
        // Default policy
        (TargetMode::Default, HashSet::new(), HashSet::new())
    };

    // Decode policy SID to IRI for better tracking/debugging
    let policy_id = snapshot
        .decode_sid(policy_sid)
        .unwrap_or_else(|| policy_sid.name.to_string());

    // Determine policy value (allow/deny/query)
    // Priority: f:allow takes precedence over f:query (per Clojure semantics)
    let value = match allow {
        Some(true) => PolicyValue::Allow,
        Some(false) => PolicyValue::Deny,
        None => {
            // No explicit allow/deny - check for f:query
            if let Some(query_json) = policy_query_json {
                // Store raw policy query JSON. Parsing/lowering is handled by the query engine.
                // We still validate that it's valid JSON to preserve previous "deny on parse error"
                // behavior without duplicating query parsing logic.
                match serde_json::from_str::<JsonValue>(&query_json) {
                    Ok(_) => PolicyValue::Query(PolicyQuery { json: query_json }),
                    Err(e) => {
                        tracing::warn!(
                            "Policy '{}': failed to parse f:query JSON, defaulting to deny: {}",
                            policy_id,
                            e
                        );
                        PolicyValue::Deny // Fall back to deny on parse error
                    }
                }
            } else {
                // No f:allow and no f:query - this is likely a misconfigured policy
                tracing::warn!(
                    "Policy '{}': missing both f:allow and f:query, defaulting to deny",
                    policy_id
                );
                PolicyValue::Deny
            }
        }
    };

    // Warn if policy has no action specified (will default to Both)
    if action.is_none() {
        tracing::debug!(
            "Policy '{}': no f:action specified, applying to both view and modify",
            policy_id
        );
    }

    // Create restriction
    let restriction = PolicyRestriction {
        id: policy_id,
        target_mode,
        targets,
        action: action.unwrap_or(PolicyAction::Both),
        value,
        required,
        message,
        class_policy: !for_classes.is_empty(),
        for_classes,
        class_check_needed: false, // Will be set by build_policy_set
    };

    Ok(Some(restriction))
}

/// Query for a specific predicate on a subject and return all object bindings.
///
/// Uses an explicit predicate SID (not a variable) to avoid the scan layer's
/// filtering of internal `fluree:ledger` predicates.
async fn query_predicate(
    snapshot: &LedgerSnapshot,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    subject_sid: &Sid,
    predicate_sid: &Sid,
) -> Result<Vec<Binding>> {
    let mut vars = VarRegistry::new();
    let obj_var = vars.get_or_insert("?obj");

    let pattern = TriplePattern::new(
        Term::Sid(subject_sid.clone()),
        Term::Sid(predicate_sid.clone()),
        Term::Var(obj_var),
    );

    let db = GraphDbRef::new(snapshot, 0, overlay, to_t);
    let batches = execute_pattern_with_overlay_at(db, &vars, pattern, None).await?;

    let mut results = Vec::new();
    for batch in &batches {
        for row in 0..batch.len() {
            if let Some(binding) = batch.get(row, obj_var) {
                results.push(binding.clone());
            }
        }
    }

    Ok(results)
}

// ============================================================================
// Inline policy parsing
// ============================================================================

/// Parse inline policy JSON-LD into restrictions.
///
/// Clojure equivalent: `wrap-policy` with inline policy
fn parse_inline_policy(
    snapshot: &LedgerSnapshot,
    policy_json: &JsonValue,
) -> Result<Vec<PolicyRestriction>> {
    // The inline policy can be a single object or an array of objects
    let policies = match policy_json {
        JsonValue::Array(arr) => arr.clone(),
        JsonValue::Object(_) => vec![policy_json.clone()],
        _ => {
            return Err(ApiError::query(
                "Invalid policy: expected object or array of policy objects",
            ))
        }
    };

    let mut restrictions = Vec::new();

    for (idx, policy) in policies.iter().enumerate() {
        let obj = policy.as_object().ok_or_else(|| {
            ApiError::query(format!("Invalid policy at index {}: expected object", idx))
        })?;

        // Extract policy ID early for use in logging
        let id = obj
            .get("@id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("inline-policy-{}", idx));

        // Extract f:allow (optional). If absent, policy may be driven by f:query.
        let allow: Option<bool> = obj
            .get("f:allow")
            .or_else(|| obj.get(&format!("{}allow", fluree::DB)))
            .and_then(|v| v.as_bool());

        // Extract f:query (optional). For inline policies we accept:
        // - String: JSON query string
        // - Object: {"@type":"@json","@value":{...}} where @value is serialized to JSON string
        //
        // Clojure parity: @json values can have object @value (not just string).
        let policy_query_json: Option<String> = obj
            .get("f:query")
            .or_else(|| obj.get(&format!("{}query", fluree::DB)))
            .and_then(|v| match v {
                JsonValue::String(s) => Some(s.clone()),
                JsonValue::Object(o) => {
                    // Handle @json typed values
                    let inner = o.get("@value")?;
                    match inner {
                        // @value is a string (already serialized JSON)
                        JsonValue::String(s) => Some(s.clone()),
                        // @value is an object (needs serialization)
                        JsonValue::Object(_) | JsonValue::Array(_) => {
                            serde_json::to_string(inner).ok()
                        }
                        _ => None,
                    }
                }
                _ => None,
            });

        // Extract f:action - can be string, object with @id, or array of these
        let action_value = obj
            .get("f:action")
            .or_else(|| obj.get(&format!("{}action", fluree::DB)));

        let action = parse_action_value(action_value);

        // Extract targets - track whether targeting was specified for validation
        let mut on_property: HashSet<Sid> = HashSet::new();
        let mut on_subject: HashSet<Sid> = HashSet::new();
        let mut on_class: HashSet<Sid> = HashSet::new();
        let mut had_on_property = false;
        let mut had_on_subject = false;
        let mut had_on_class = false;

        // f:onProperty
        if let Some(props) = obj
            .get("f:onProperty")
            .or_else(|| obj.get(&format!("{}onProperty", fluree::DB)))
        {
            had_on_property = true;
            for iri in extract_iris(props) {
                match resolve_iri_to_sid(snapshot, &iri) {
                    Ok(sid) => {
                        on_property.insert(sid);
                    }
                    Err(_) => {
                        tracing::warn!(
                            policy = %id,
                            iri = %iri,
                            key = "f:onProperty",
                            "IRI could not be resolved - namespace may not be registered"
                        );
                    }
                }
            }
        }

        // f:onSubject
        if let Some(subjs) = obj
            .get("f:onSubject")
            .or_else(|| obj.get(&format!("{}onSubject", fluree::DB)))
        {
            had_on_subject = true;
            for iri in extract_iris(subjs) {
                match resolve_iri_to_sid(snapshot, &iri) {
                    Ok(sid) => {
                        on_subject.insert(sid);
                    }
                    Err(_) => {
                        tracing::warn!(
                            policy = %id,
                            iri = %iri,
                            key = "f:onSubject",
                            "IRI could not be resolved"
                        );
                    }
                }
            }
        }

        // f:onClass
        if let Some(classes) = obj
            .get("f:onClass")
            .or_else(|| obj.get(&format!("{}onClass", fluree::DB)))
        {
            had_on_class = true;
            for iri in extract_iris(classes) {
                match resolve_iri_to_sid(snapshot, &iri) {
                    Ok(sid) => {
                        on_class.insert(sid);
                    }
                    Err(_) => {
                        tracing::warn!(
                            policy = %id,
                            iri = %iri,
                            key = "f:onClass",
                            "IRI could not be resolved"
                        );
                    }
                }
            }
        }

        // Validate: if targeting was specified but all IRIs failed to resolve,
        // this is likely a configuration error. We log a warning but allow the
        // policy to proceed (it will effectively be inactive).
        if had_on_property && on_property.is_empty() {
            tracing::warn!(
                policy = %id,
                "f:onProperty specified but no IRIs could be resolved - policy will not match any property"
            );
        }
        if had_on_subject && on_subject.is_empty() {
            tracing::warn!(
                policy = %id,
                "f:onSubject specified but no IRIs could be resolved - policy will not match any subject"
            );
        }
        if had_on_class && on_class.is_empty() {
            tracing::warn!(
                policy = %id,
                "f:onClass specified but no IRIs could be resolved - policy will not match any class"
            );
        }

        // f:required
        let required = obj
            .get("f:required")
            .or_else(|| obj.get(&format!("{}required", fluree::DB)))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // f:exMessage
        let message = obj
            .get("f:exMessage")
            .or_else(|| obj.get(&format!("{}exMessage", fluree::DB)))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Determine target mode
        //
        // When f:onProperty is combined with f:onClass, the policy targets those
        // properties ONLY on instances of those classes (Clojure parity).
        // The `for_classes` field carries the class restriction.
        let (target_mode, targets, for_classes) = if !on_property.is_empty() {
            // OnProperty may also have a class restriction
            (TargetMode::OnProperty, on_property, on_class)
        } else if !on_subject.is_empty() {
            (TargetMode::OnSubject, on_subject, HashSet::new())
        } else if !on_class.is_empty() {
            (TargetMode::OnClass, HashSet::new(), on_class)
        } else {
            (TargetMode::Default, HashSet::new(), HashSet::new())
        };

        // Policy value (allow/deny/query)
        // Priority: f:allow takes precedence over f:query (Clojure parity)
        let value = match allow {
            Some(true) => PolicyValue::Allow,
            Some(false) => PolicyValue::Deny,
            None => {
                if let Some(query_json) = policy_query_json {
                    match serde_json::from_str::<JsonValue>(&query_json) {
                        Ok(_) => PolicyValue::Query(PolicyQuery { json: query_json }),
                        Err(e) => {
                            tracing::warn!("Failed to parse inline policy query JSON: {}", e);
                            PolicyValue::Deny
                        }
                    }
                } else {
                    PolicyValue::Deny
                }
            }
        };

        restrictions.push(PolicyRestriction {
            id,
            target_mode,
            targets,
            action,
            value,
            required,
            message,
            class_policy: !for_classes.is_empty(),
            for_classes,
            class_check_needed: false,
        });
    }

    Ok(restrictions)
}

// ============================================================================
// Helper functions
// ============================================================================

// NOTE: Policy query parsing is intentionally delegated to the query engine
// (`fluree-db-query`) to avoid duplicating query parsing/lowering and to ensure
// full feature support (e.g., FILTER patterns) in f:query policies.

/// Resolve an IRI string to a SID using the database's encoding.
fn resolve_iri_to_sid(snapshot: &LedgerSnapshot, iri: &str) -> Result<Sid> {
    snapshot
        .encode_iri(iri)
        .ok_or_else(|| ApiError::query(format!("Failed to resolve IRI '{}'", iri)))
}

/// Build policy values map from JSON values.
fn build_policy_values(
    snapshot: &LedgerSnapshot,
    values: &Option<HashMap<String, JsonValue>>,
) -> Result<HashMap<String, Sid>> {
    let mut result = HashMap::new();

    if let Some(vals) = values {
        for (key, val) in vals {
            // Try to extract IRI from value
            let iri = match val {
                JsonValue::String(s) => s.clone(),
                JsonValue::Object(obj) => {
                    // Check for {"@id": "..."} or {"@value": "..."}
                    obj.get("@id")
                        .or_else(|| obj.get("@value"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .ok_or_else(|| {
                            ApiError::query(format!(
                                "Invalid policy value for '{}': expected IRI",
                                key
                            ))
                        })?
                }
                _ => {
                    return Err(ApiError::query(format!(
                        "Invalid policy value for '{}': expected string or object with @id",
                        key
                    )))
                }
            };

            if let Ok(sid) = resolve_iri_to_sid(snapshot, &iri) {
                result.insert(key.clone(), sid);
            }
        }
    }

    Ok(result)
}

/// Parse f:action value into PolicyAction.
///
/// Handles multiple formats:
/// - String: "f:view", "f:modify", or full IRI
/// - Object with @id: {"@id": "f:view"}
/// - Array of the above: [{"@id": "f:view"}, {"@id": "f:modify"}]
///
/// Returns PolicyAction::Both if both view and modify are specified or if
/// the value cannot be parsed.
fn parse_action_value(value: Option<&JsonValue>) -> PolicyAction {
    let value = match value {
        Some(v) => v,
        None => return PolicyAction::Both,
    };

    // Collect all action IRIs from the value
    let action_strs = extract_action_strings(value);

    let mut has_view = false;
    let mut has_modify = false;

    for s in action_strs {
        if s.contains("view") {
            has_view = true;
        }
        if s.contains("modify") {
            has_modify = true;
        }
    }

    match (has_view, has_modify) {
        (true, true) => PolicyAction::Both,
        (true, false) => PolicyAction::View,
        (false, true) => PolicyAction::Modify,
        (false, false) => PolicyAction::Both, // Default if no recognized action
    }
}

/// Extract action strings from a JSON value (string, object with @id, or array).
fn extract_action_strings(value: &JsonValue) -> Vec<String> {
    match value {
        JsonValue::String(s) => vec![s.clone()],
        JsonValue::Object(obj) => {
            if let Some(id) = obj.get("@id").and_then(|v| v.as_str()) {
                vec![id.to_string()]
            } else {
                vec![]
            }
        }
        JsonValue::Array(arr) => arr.iter().flat_map(extract_action_strings).collect(),
        _ => vec![],
    }
}

/// Extract IRIs from a JSON value (single string, object with @id, or array).
fn extract_iris(value: &JsonValue) -> Vec<String> {
    match value {
        JsonValue::String(s) => vec![s.clone()],
        JsonValue::Object(obj) => {
            if let Some(id) = obj.get("@id").and_then(|v| v.as_str()) {
                vec![id.to_string()]
            } else {
                vec![]
            }
        }
        JsonValue::Array(arr) => arr.iter().flat_map(extract_iris).collect(),
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // extract_iris tests
    // ========================================================================

    #[test]
    fn test_extract_iris_string() {
        let v = JsonValue::String("http://example.org/foo".to_string());
        assert_eq!(extract_iris(&v), vec!["http://example.org/foo"]);
    }

    #[test]
    fn test_extract_iris_object() {
        let v = serde_json::json!({"@id": "http://example.org/bar"});
        assert_eq!(extract_iris(&v), vec!["http://example.org/bar"]);
    }

    #[test]
    fn test_extract_iris_array() {
        let v = serde_json::json!(["http://example.org/a", {"@id": "http://example.org/b"}]);
        assert_eq!(
            extract_iris(&v),
            vec!["http://example.org/a", "http://example.org/b"]
        );
    }

    // NOTE: expand_iri tests removed - IRI expansion is tested in the json-ld crate.
    // The json_ld::expand_iri function requires a ParsedContext, and IRI expansion
    // happens at the JSON-LD parsing boundary, not in policy_builder.

    // NOTE: f:query policy parsing/lowering is tested in `fluree-db-query` now.

    // ========================================================================
    // parse_action_value tests
    // ========================================================================

    #[test]
    fn test_parse_action_none() {
        assert_eq!(parse_action_value(None), PolicyAction::Both);
    }

    #[test]
    fn test_parse_action_string_view() {
        let v = serde_json::json!("f:view");
        assert_eq!(parse_action_value(Some(&v)), PolicyAction::View);
    }

    #[test]
    fn test_parse_action_string_modify() {
        let v = serde_json::json!("f:modify");
        assert_eq!(parse_action_value(Some(&v)), PolicyAction::Modify);
    }

    #[test]
    fn test_parse_action_object_view() {
        let v = serde_json::json!({"@id": "f:view"});
        assert_eq!(parse_action_value(Some(&v)), PolicyAction::View);
    }

    #[test]
    fn test_parse_action_object_modify() {
        let v = serde_json::json!({"@id": "https://ns.flur.ee/db#modify"});
        assert_eq!(parse_action_value(Some(&v)), PolicyAction::Modify);
    }

    #[test]
    fn test_parse_action_array_view_only() {
        let v = serde_json::json!([{"@id": "f:view"}]);
        assert_eq!(parse_action_value(Some(&v)), PolicyAction::View);
    }

    #[test]
    fn test_parse_action_array_modify_only() {
        let v = serde_json::json!([{"@id": "f:modify"}]);
        assert_eq!(parse_action_value(Some(&v)), PolicyAction::Modify);
    }

    #[test]
    fn test_parse_action_array_both() {
        let v = serde_json::json!([{"@id": "f:view"}, {"@id": "f:modify"}]);
        assert_eq!(parse_action_value(Some(&v)), PolicyAction::Both);
    }

    #[test]
    fn test_parse_action_array_full_iris() {
        let v = serde_json::json!([
            {"@id": "https://ns.flur.ee/db#view"},
            {"@id": "https://ns.flur.ee/db#modify"}
        ]);
        assert_eq!(parse_action_value(Some(&v)), PolicyAction::Both);
    }
}
