//! Ledger configuration types.
//!
//! Parsed, validated representation of a ledger's config graph contents.
//! These types are pure data structures — no I/O, no async, no storage dependency.
//! Resolution from raw flakes happens in `fluree-db-api::config_resolver`.
//!
//! # Config graph layout
//!
//! The config graph lives at g_id=2 (`urn:fluree:{ledger_id}#config`) and contains
//! one `f:LedgerConfig` resource with optional setting groups and per-graph overrides.
//! See `LEDGER-CONFIG-GRAPH.md` for the full proposal.

use std::collections::HashSet;
use std::sync::Arc;

// ============================================================================
// Top-level config
// ============================================================================

/// Parsed ledger configuration from the config graph.
///
/// Resolved once per snapshot and cached on `GraphDb`. Fields are `Option` —
/// `None` means "unconfigured, use system defaults."
#[derive(Debug, Clone, Default)]
pub struct LedgerConfig {
    /// The `@id` of the `f:LedgerConfig` resource.
    pub config_id: Option<String>,
    /// Policy defaults (`f:policyDefaults`).
    pub policy: Option<PolicyDefaults>,
    /// SHACL validation defaults (`f:shaclDefaults`).
    pub shacl: Option<ShaclDefaults>,
    /// Reasoning defaults (`f:reasoningDefaults`).
    pub reasoning: Option<ReasoningDefaults>,
    /// Datalog rules defaults (`f:datalogDefaults`).
    pub datalog: Option<DatalogDefaults>,
    /// Per-graph config overrides (`f:graphOverrides`).
    pub graph_overrides: Vec<GraphConfig>,
}

/// Effective config for a specific graph within a ledger.
///
/// Produced by [`resolve_effective_config`](fluree_db_api::config_resolver::resolve_effective_config)
/// which merges ledger-wide defaults with per-graph overrides.
/// Carried on `GraphDb` so downstream callers can apply identity gating at
/// request time without re-reading the config graph.
#[derive(Debug, Clone, Default)]
pub struct ResolvedConfig {
    /// Effective policy defaults (ledger-wide merged with per-graph override).
    pub policy: Option<PolicyDefaults>,
    /// Effective SHACL defaults.
    pub shacl: Option<ShaclDefaults>,
    /// Effective reasoning defaults.
    pub reasoning: Option<ReasoningDefaults>,
    /// Effective datalog defaults.
    pub datalog: Option<DatalogDefaults>,
}

// ============================================================================
// Setting groups
// ============================================================================

/// Policy defaults from the config graph (`f:policyDefaults`).
#[derive(Debug, Clone, Default)]
pub struct PolicyDefaults {
    /// `f:defaultAllow` — `None` means use system default (true).
    pub default_allow: Option<bool>,
    /// `f:policySource` — reference to graph containing policy rules.
    pub policy_source: Option<GraphSourceRef>,
    /// `f:policyClass` — default policy classes to apply.
    pub policy_class: Option<Vec<String>>,
    /// Override control for this setting group.
    pub override_control: OverrideControl,
}

/// SHACL validation defaults from the config graph (`f:shaclDefaults`).
#[derive(Debug, Clone, Default)]
pub struct ShaclDefaults {
    /// `f:shaclEnabled` — enable/disable SHACL validation.
    pub enabled: Option<bool>,
    /// `f:shapesSource` — reference to graph containing SHACL shapes.
    pub shapes_source: Option<GraphSourceRef>,
    /// `f:validationMode` — reject or warn on validation failure.
    pub validation_mode: Option<ValidationMode>,
    /// Override control for this setting group.
    pub override_control: OverrideControl,
}

/// Reasoning defaults from the config graph (`f:reasoningDefaults`).
#[derive(Debug, Clone, Default)]
pub struct ReasoningDefaults {
    /// `f:reasoningModes` — e.g., `["rdfs"]`, `["owl2-rl"]`.
    pub modes: Option<Vec<String>>,
    /// `f:schemaSource` — reference to graph containing schema hierarchy.
    pub schema_source: Option<GraphSourceRef>,
    /// Override control for this setting group.
    pub override_control: OverrideControl,
}

/// Datalog rules defaults from the config graph (`f:datalogDefaults`).
#[derive(Debug, Clone, Default)]
pub struct DatalogDefaults {
    /// `f:datalogEnabled` — enable/disable datalog rule evaluation.
    pub enabled: Option<bool>,
    /// `f:rulesSource` — reference to graph containing `f:rule` resources.
    pub rules_source: Option<GraphSourceRef>,
    /// `f:allowQueryTimeRules` — allow query-time rule injection.
    pub allow_query_time_rules: Option<bool>,
    /// Override control for this setting group.
    pub override_control: OverrideControl,
}

// ============================================================================
// Per-graph overrides
// ============================================================================

/// Per-graph config override (`f:GraphConfig`).
///
/// Identifies a target graph by IRI and overrides specific settings.
/// Only include settings being overridden — absent groups inherit
/// from ledger-wide config.
#[derive(Debug, Clone)]
pub struct GraphConfig {
    /// `f:targetGraph` — IRI of the target graph, or `f:defaultGraph` /
    /// `f:txnMetaGraph` sentinel.
    pub target_graph: String,
    /// Policy overrides for this graph.
    pub policy: Option<PolicyDefaults>,
    /// SHACL overrides for this graph.
    pub shacl: Option<ShaclDefaults>,
    /// Reasoning overrides for this graph.
    pub reasoning: Option<ReasoningDefaults>,
    /// Datalog overrides for this graph.
    pub datalog: Option<DatalogDefaults>,
}

// ============================================================================
// Override control
// ============================================================================

/// Controls whether higher-priority sources (per-graph configs, query-time opts)
/// can override a setting group's values.
///
/// Permissiveness ordering: `None` < `IdentityRestricted` < `AllowAll`.
///
/// Default is `AllowAll` (existing query behavior is unrestricted).
#[derive(Debug, Clone, Default)]
pub enum OverrideControl {
    /// No overrides permitted, regardless of identity.
    None,
    /// Any request can override.
    #[default]
    AllowAll,
    /// Only requests with a verified identity in `allowed_identities` can override.
    IdentityRestricted {
        allowed_identities: HashSet<Arc<str>>,
    },
}

impl OverrideControl {
    /// Permissiveness level for ordering comparisons.
    /// `None` (0) < `IdentityRestricted` (1) < `AllowAll` (2).
    fn permissiveness(&self) -> u8 {
        match self {
            OverrideControl::None => 0,
            OverrideControl::IdentityRestricted { .. } => 1,
            OverrideControl::AllowAll => 2,
        }
    }

    /// Compute effective override control as the minimum of two controls.
    ///
    /// Per-graph configs can only **tighten** (restrict), not loosen, the
    /// ledger-wide override control. This function computes `min(self, other)`
    /// under the permissiveness ordering.
    ///
    /// When both are `IdentityRestricted`, the effective `allowed_identities`
    /// is the **intersection** of the two sets.
    pub fn effective_min(&self, other: &OverrideControl) -> OverrideControl {
        let self_perm = self.permissiveness();
        let other_perm = other.permissiveness();

        if self_perm < other_perm {
            // self is stricter → use self
            self.clone()
        } else if other_perm < self_perm {
            // other is stricter → use other
            other.clone()
        } else {
            // Same level — special handling for IdentityRestricted intersection
            match (self, other) {
                (
                    OverrideControl::IdentityRestricted {
                        allowed_identities: a,
                    },
                    OverrideControl::IdentityRestricted {
                        allowed_identities: b,
                    },
                ) => OverrideControl::IdentityRestricted {
                    allowed_identities: a.intersection(b).cloned().collect(),
                },
                // Both None or both AllowAll
                _ => self.clone(),
            }
        }
    }

    /// Check if a given request identity is permitted to override.
    ///
    /// `request_identity` is the server-verified canonical DID string.
    /// `None` means anonymous (no verified identity).
    pub fn permits_override(&self, request_identity: Option<&str>) -> bool {
        match self {
            OverrideControl::None => false,
            OverrideControl::AllowAll => true,
            OverrideControl::IdentityRestricted { allowed_identities } => request_identity
                .map(|id| allowed_identities.contains(id))
                .unwrap_or(false),
        }
    }
}

// ============================================================================
// Graph source references
// ============================================================================

/// Reference to a graph source (local or remote) with trust controls.
///
/// Named `GraphSourceRef` (not `GraphRef`) to avoid confusion with the
/// internal `GraphRef` enum used for fragment parsing in `fluree_ext.rs`.
///
/// Corresponds to `f:GraphRef` in the config graph schema.
#[derive(Debug, Clone)]
pub struct GraphSourceRef {
    /// Ledger identifier (e.g., `"mydb:main"`). `None` = same ledger.
    pub ledger: Option<String>,
    /// Graph selector: `"f:defaultGraph"`, `"f:txnMetaGraph"`, or a graph IRI.
    pub graph_selector: Option<String>,
    /// Pin to a specific commit number. Mutually exclusive with other temporal selectors.
    pub at_t: Option<i64>,
    /// Trust policy for this reference.
    pub trust_policy: Option<TrustPolicy>,
    /// Rollback guard — freshness constraints for this reference.
    pub rollback_guard: Option<RollbackGuard>,
}

/// Trust verification model for a [`GraphSourceRef`].
#[derive(Debug, Clone)]
pub struct TrustPolicy {
    /// How to validate the referenced graph.
    pub trust_mode: TrustMode,
}

/// Freshness constraints for a [`GraphSourceRef`].
///
/// Corresponds to `f:RollbackGuard` in the config graph schema.
/// Prevents accepting stale or rolled-back heads from a nameservice.
#[derive(Debug, Clone)]
pub struct RollbackGuard {
    /// Reject any resolved head where `head_t < min_t`.
    pub min_t: Option<i64>,
}

/// How to validate a remote graph reference.
///
/// Values are IRIs in the `f:` namespace (e.g., `f:Trusted`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrustMode {
    /// `f:Trusted` — accept nameservice head without additional validation.
    Trusted,
    /// `f:SignedIndex` — verify signed index root.
    SignedIndex,
    /// `f:CommitVerified` — full commit chain verification.
    CommitVerified,
}

/// SHACL validation mode.
///
/// Values are IRIs in the `f:` namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationMode {
    /// `f:ValidationReject` — reject transactions that fail SHACL validation.
    Reject,
    /// `f:ValidationWarn` — warn but allow transactions that fail SHACL validation.
    Warn,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn identity_set(ids: &[&str]) -> HashSet<Arc<str>> {
        ids.iter().map(|s| Arc::from(*s)).collect()
    }

    // --- OverrideControl::effective_min ---

    #[test]
    fn effective_min_none_with_allow_all() {
        let result = OverrideControl::None.effective_min(&OverrideControl::AllowAll);
        assert!(matches!(result, OverrideControl::None));
    }

    #[test]
    fn effective_min_allow_all_with_none() {
        let result = OverrideControl::AllowAll.effective_min(&OverrideControl::None);
        assert!(matches!(result, OverrideControl::None));
    }

    #[test]
    fn effective_min_none_with_identity_restricted() {
        let restricted = OverrideControl::IdentityRestricted {
            allowed_identities: identity_set(&["did:key:alice"]),
        };
        let result = OverrideControl::None.effective_min(&restricted);
        assert!(matches!(result, OverrideControl::None));
    }

    #[test]
    fn effective_min_identity_restricted_with_none() {
        let restricted = OverrideControl::IdentityRestricted {
            allowed_identities: identity_set(&["did:key:alice"]),
        };
        let result = restricted.effective_min(&OverrideControl::None);
        assert!(matches!(result, OverrideControl::None));
    }

    #[test]
    fn effective_min_allow_all_with_identity_restricted() {
        let restricted = OverrideControl::IdentityRestricted {
            allowed_identities: identity_set(&["did:key:alice"]),
        };
        let result = OverrideControl::AllowAll.effective_min(&restricted);
        match result {
            OverrideControl::IdentityRestricted { allowed_identities } => {
                assert!(allowed_identities.contains("did:key:alice"));
                assert_eq!(allowed_identities.len(), 1);
            }
            other => panic!("Expected IdentityRestricted, got {:?}", other),
        }
    }

    #[test]
    fn effective_min_identity_restricted_with_allow_all() {
        let restricted = OverrideControl::IdentityRestricted {
            allowed_identities: identity_set(&["did:key:alice"]),
        };
        let result = restricted.effective_min(&OverrideControl::AllowAll);
        match result {
            OverrideControl::IdentityRestricted { allowed_identities } => {
                assert!(allowed_identities.contains("did:key:alice"));
                assert_eq!(allowed_identities.len(), 1);
            }
            other => panic!("Expected IdentityRestricted, got {:?}", other),
        }
    }

    #[test]
    fn effective_min_two_identity_restricted_intersects() {
        let a = OverrideControl::IdentityRestricted {
            allowed_identities: identity_set(&["did:key:alice", "did:key:bob"]),
        };
        let b = OverrideControl::IdentityRestricted {
            allowed_identities: identity_set(&["did:key:alice", "did:key:carol"]),
        };
        let result = a.effective_min(&b);
        match result {
            OverrideControl::IdentityRestricted { allowed_identities } => {
                assert!(allowed_identities.contains("did:key:alice"));
                assert!(!allowed_identities.contains("did:key:bob"));
                assert!(!allowed_identities.contains("did:key:carol"));
                assert_eq!(allowed_identities.len(), 1);
            }
            other => panic!("Expected IdentityRestricted, got {:?}", other),
        }
    }

    #[test]
    fn effective_min_two_identity_restricted_empty_intersection() {
        let a = OverrideControl::IdentityRestricted {
            allowed_identities: identity_set(&["did:key:alice"]),
        };
        let b = OverrideControl::IdentityRestricted {
            allowed_identities: identity_set(&["did:key:bob"]),
        };
        let result = a.effective_min(&b);
        match result {
            OverrideControl::IdentityRestricted { allowed_identities } => {
                assert!(allowed_identities.is_empty());
            }
            other => panic!("Expected IdentityRestricted, got {:?}", other),
        }
    }

    #[test]
    fn effective_min_none_with_none() {
        let result = OverrideControl::None.effective_min(&OverrideControl::None);
        assert!(matches!(result, OverrideControl::None));
    }

    #[test]
    fn effective_min_allow_all_with_allow_all() {
        let result = OverrideControl::AllowAll.effective_min(&OverrideControl::AllowAll);
        assert!(matches!(result, OverrideControl::AllowAll));
    }

    // --- OverrideControl::permits_override ---

    #[test]
    fn permits_override_none_always_false() {
        assert!(!OverrideControl::None.permits_override(Some("did:key:alice")));
        assert!(!OverrideControl::None.permits_override(None));
    }

    #[test]
    fn permits_override_allow_all_always_true() {
        assert!(OverrideControl::AllowAll.permits_override(Some("did:key:alice")));
        assert!(OverrideControl::AllowAll.permits_override(None));
    }

    #[test]
    fn permits_override_identity_restricted_matching() {
        let ctrl = OverrideControl::IdentityRestricted {
            allowed_identities: identity_set(&["did:key:alice", "did:key:bob"]),
        };
        assert!(ctrl.permits_override(Some("did:key:alice")));
        assert!(ctrl.permits_override(Some("did:key:bob")));
    }

    #[test]
    fn permits_override_identity_restricted_non_matching() {
        let ctrl = OverrideControl::IdentityRestricted {
            allowed_identities: identity_set(&["did:key:alice"]),
        };
        assert!(!ctrl.permits_override(Some("did:key:bob")));
    }

    #[test]
    fn permits_override_identity_restricted_anonymous() {
        let ctrl = OverrideControl::IdentityRestricted {
            allowed_identities: identity_set(&["did:key:alice"]),
        };
        assert!(!ctrl.permits_override(None));
    }

    #[test]
    fn permits_override_identity_restricted_empty_set() {
        let ctrl = OverrideControl::IdentityRestricted {
            allowed_identities: HashSet::new(),
        };
        assert!(!ctrl.permits_override(Some("did:key:alice")));
        assert!(!ctrl.permits_override(None));
    }

    // --- LedgerConfig defaults ---

    #[test]
    fn ledger_config_default_is_unconfigured() {
        let config = LedgerConfig::default();
        assert!(config.config_id.is_none());
        assert!(config.policy.is_none());
        assert!(config.shacl.is_none());
        assert!(config.reasoning.is_none());
        assert!(config.datalog.is_none());
        assert!(config.graph_overrides.is_empty());
    }

    #[test]
    fn override_control_default_is_allow_all() {
        assert!(matches!(
            OverrideControl::default(),
            OverrideControl::AllowAll
        ));
    }
}
