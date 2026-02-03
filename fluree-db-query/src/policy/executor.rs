//! Policy query executor implementation
//!
//! Implements `PolicyQueryExecutor` using the query engine asynchronously.

use crate::context::ExecutionContext;
use crate::execute::build_where_operators_seeded;
use crate::var_registry::VarRegistry;
use fluree_db_core::{Db, OverlayProvider, Sid, Storage};
use fluree_db_policy::{PolicyQuery, PolicyQueryExecutor, PolicyQueryFut, Result as PolicyResult, UNBOUND_IDENTITY_PREFIX};
use std::collections::HashMap;

/// Policy query executor that runs queries against a database
///
/// This executor converts `PolicyQuery` to the query engine's IR and
/// executes with a root context (no policy filtering).
pub struct QueryPolicyExecutor<'a, S: Storage> {
    /// The database to query
    pub db: &'a Db<S>,
    /// Optional overlay provider (for staged flakes)
    pub overlay: Option<&'a dyn OverlayProvider>,
    /// Target transaction time
    pub to_t: i64,
}

impl<'a, S: Storage> QueryPolicyExecutor<'a, S> {
    /// Create a new query executor
    pub fn new(db: &'a Db<S>) -> Self {
        Self {
            db,
            overlay: None,
            to_t: db.t,
        }
    }

    /// Create a query executor with overlay support
    pub fn with_overlay(db: &'a Db<S>, overlay: &'a dyn OverlayProvider, to_t: i64) -> Self {
        Self {
            db,
            overlay: Some(overlay),
            to_t,
        }
    }
}

impl<'a, S: Storage + 'static> PolicyQueryExecutor
    for QueryPolicyExecutor<'a, S>
{
    fn evaluate_policy_query<'b>(
        &'b self,
        query: &'b PolicyQuery,
        bindings: &'b HashMap<String, Sid>,
    ) -> PolicyQueryFut<'b> {
        Box::pin(self.evaluate_async(query, bindings))
    }
}

impl<'a, S: Storage + 'static> QueryPolicyExecutor<'a, S> {
    /// Async implementation of policy query evaluation
    async fn evaluate_async(
        &self,
        query: &PolicyQuery,
        bindings: &HashMap<String, Sid>,
    ) -> PolicyResult<bool> {
        // Parse and lower the policy's f:query using the main query parser/IR.
        //
        // We intentionally do NOT implement a bespoke parser here; this ensures full
        // feature parity (e.g., FILTER patterns) and avoids divergence.
        //
        // Clojure parity: policy queries behave like existence checks, with:
        // - select forced to ["?$this"]
        // - limit forced to 1
        // - VALUES injected into WHERE for special variables (?$this, ?$identity, etc.)
        let mut query_json: serde_json::Value = serde_json::from_str(&query.json).map_err(|e| {
            fluree_db_policy::PolicyError::QueryExecution {
                message: format!("Invalid policy query JSON: {}", e),
            }
        })?;

        let obj = query_json.as_object_mut().ok_or_else(|| fluree_db_policy::PolicyError::QueryExecution {
            message: "Policy query must be a JSON object".to_string(),
        })?;

        // Force select + limit for policy queries (Clojure semantics)
        obj.insert(
            "select".to_string(),
            serde_json::Value::Array(vec![serde_json::Value::String("?$this".to_string())]),
        );
        obj.insert("limit".to_string(), serde_json::Value::from(1));

        // Build VALUES clause JSON for special variables.
        // Clojure parity: inject VALUES into WHERE clause BEFORE parsing.
        // This ensures even empty queries (no WHERE) work - the VALUES provides the pattern.
        //
        // Format: ["values", [["?$this", "?$identity", ...], [[iri1, iri2, ...]]]]
        let mut var_names: Vec<String> = bindings.keys().cloned().collect();
        var_names.sort();

        // Build VALUES row with IRIs for each variable
        // Special case: unbound identity uses null (UNDEF) to ensure it never matches
        let values_row: Vec<serde_json::Value> = var_names
            .iter()
            .map(|name| {
                let sid = bindings.get(name).expect("binding value exists");
                // Check if this is an unbound identity - use null (UNDEF) instead of IRI
                // This ensures patterns referencing ?$identity won't match anything
                if sid.name.starts_with(UNBOUND_IDENTITY_PREFIX) {
                    return serde_json::Value::Null;
                }
                // Decode SID to IRI for JSON representation
                let iri = self.db.decode_sid(sid).unwrap_or_else(|| sid.name.to_string());
                serde_json::json!({"@id": iri})
            })
            .collect();

        let values_clause = serde_json::json!([
            "values",
            [var_names.clone(), [values_row]]
        ]);

        // Inject VALUES into WHERE clause (or create WHERE if missing)
        let where_clause = obj.get_mut("where");
        match where_clause {
            Some(serde_json::Value::Array(arr)) => {
                // WHERE is an array - prepend VALUES
                arr.insert(0, values_clause);
            }
            Some(serde_json::Value::Object(_)) => {
                // WHERE is an object (single pattern) - wrap in array with VALUES
                let existing = obj.remove("where").unwrap();
                obj.insert(
                    "where".to_string(),
                    serde_json::json!([values_clause, existing]),
                );
            }
            Some(_) | None => {
                // No WHERE or invalid - create with just VALUES
                // This handles empty queries like {} which Clojure allows
                obj.insert("where".to_string(), serde_json::json!([values_clause]));
            }
        }

        // Create a variable registry for this query execution
        let mut vars = VarRegistry::new();

        // Pre-register special variables so they are present even if not referenced.
        // This matches the "always ground" behavior from Clojure.
        for var_name in &var_names {
            vars.get_or_insert(var_name);
        }

        let parsed = crate::parse::parse_query(&query_json, self.db, &mut vars).map_err(|e| {
            fluree_db_policy::PolicyError::QueryExecution {
                message: format!("Failed to parse policy query: {}", e),
            }
        })?;

        let patterns = parsed.patterns;

        // Create the execution context WITHOUT policy (root context)
        // This is critical - policy queries must not be filtered by policy
        let ctx = if let Some(overlay) = self.overlay {
            ExecutionContext::with_time_and_overlay(self.db, &vars, self.to_t, None, overlay)
        } else {
            ExecutionContext::with_time(self.db, &vars, self.to_t, None)
        };
        // Note: policy_enforcer is None by default (root context)

        // Build the where clause operators (VALUES is now part of parsed patterns)
        let mut operator = build_where_operators_seeded(None, &patterns, None)
            .map_err(|e| fluree_db_policy::PolicyError::QueryExecution {
                message: e.to_string(),
            })?;

        // Execute with limit 1 (we only need to know if there are any results)
        operator
            .open(&ctx)
            .await
            .map_err(|e| fluree_db_policy::PolicyError::QueryExecution {
                message: e.to_string(),
            })?;

        // Check if there's at least one result
        let has_results = match operator.next_batch(&ctx).await {
            Ok(Some(batch)) => batch.len() > 0,
            Ok(None) => false,
            Err(e) => {
                operator.close();
                return Err(fluree_db_policy::PolicyError::QueryExecution {
                    message: e.to_string(),
                });
            }
        };

        operator.close();

        Ok(has_results)
    }
}
