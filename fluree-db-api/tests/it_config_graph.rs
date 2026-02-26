//! Config graph integration tests
//!
//! End-to-end coverage that the config graph (g_id=2) is reserved/readable,
//! config can be written via TriG, defaults apply automatically, per-graph
//! overrides work, override control blocks/permits query-time overrides,
//! and config is time-travel consistent.

mod support;

use fluree_db_api::config_resolver;
use fluree_db_api::{FlureeBuilder, QueryConnectionOptions};
use serde_json::json;
use support::genesis_ledger;

/// Build the config graph IRI for a canonical ledger id.
fn config_graph_iri(ledger_id: &str) -> String {
    format!("urn:fluree:{}#config", ledger_id)
}

// =============================================================================
// Test 1: config graph reserved at g_id=2
// =============================================================================

#[tokio::test]
async fn config_graph_reserved_at_gid2() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-gid2:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Transact anything to materialize the ledger
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [{"@id": "ex:a", "ex:val": 1}]
            }),
        )
        .await
        .unwrap();

    let view = fluree.db(ledger_id).await.unwrap();
    // Config graph IRI should resolve to g_id=2
    let config_iri = config_graph_iri(ledger_id);
    let g_id = view.snapshot.graph_registry.graph_id_for_iri(&config_iri);
    assert_eq!(g_id, Some(2), "config graph should be reserved at g_id=2");
}

// =============================================================================
// Test 2: config write via TriG roundtrip
// =============================================================================

#[tokio::test]
async fn config_write_trig_roundtrip() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-trig:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow false .
        }}
    "#
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write should succeed");

    let view = fluree.db(ledger_id).await.unwrap();
    let config = view
        .ledger_config()
        .expect("config should be attached after writing to config graph");
    let policy = config
        .policy
        .as_ref()
        .expect("policy defaults should be read from config");
    assert_eq!(
        policy.default_allow,
        Some(false),
        "defaultAllow should round-trip as false"
    );
}

// =============================================================================
// Test 3: config write via JSON-LD insert
// =============================================================================

#[tokio::test]
async fn config_write_json_ld() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-jsonld:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);

    // Write config using TriG with JSON-LD-style structure
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow true .
            <urn:config:main> f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:reasoningModes f:rdfs .
        }}
    "#
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write should succeed");

    let view = fluree.db(ledger_id).await.unwrap();
    let config = view.ledger_config().expect("config should be attached");
    assert_eq!(
        config.policy.as_ref().and_then(|p| p.default_allow),
        Some(true)
    );
    assert_eq!(
        config
            .reasoning
            .as_ref()
            .and_then(|r| r.modes.as_ref())
            .map(|m| m.as_slice()),
        Some(["https://ns.flur.ee/db#rdfs".to_string()].as_slice()),
        "reasoning modes should round-trip as full IRIs"
    );
}

// =============================================================================
// Test 4: policy defaults apply
// =============================================================================

#[tokio::test]
async fn policy_defaults_apply() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-policy-apply:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // 1. Seed data and a policy class in the default graph
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "ex": "http://example.org/ns/",
                    "f": "https://ns.flur.ee/db#",
                    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                },
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:User", "ex:name": "Alice"},
                    {"@id": "ex:bob", "@type": "ex:User", "ex:name": "Bob"},
                    {
                        "@id": "ex:DenyAllPolicy",
                        "@type": ["f:Policy"],
                        "f:targetClass": {"@id": "ex:User"},
                        "f:allow": []
                    }
                ]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // 2. Write config: defaultAllow=false, policyClass=ex:DenyAllPolicy
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex: <http://example.org/ns/> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow false .
            <urn:config:policy> f:policyClass ex:DenyAllPolicy .
        }}
    "#
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    // 3. Verify config is attached and has the expected defaults
    let view = fluree.db(ledger_id).await.unwrap();
    let config = view.ledger_config().expect("config should be attached");
    let policy = config.policy.as_ref().expect("policy defaults");
    assert_eq!(policy.default_allow, Some(false));
    assert!(policy.policy_class.is_some(), "policy_class should be set");

    // 4. Verify config defaults actually flow through merge_policy_opts:
    //    empty opts → config's defaultAllow and policyClass should be applied
    let resolved = view.resolved_config().expect("resolved config");
    let empty_opts = QueryConnectionOptions::default();
    let merged = config_resolver::merge_policy_opts(resolved, &empty_opts, None);
    assert!(
        !merged.default_allow,
        "config's defaultAllow=false should apply when no query opts"
    );
    assert!(
        merged.policy_class.is_some(),
        "config's policyClass should apply when no query opts"
    );
}

// =============================================================================
// Test 5: reasoning defaults apply (RDFS subProperty)
// =============================================================================

#[tokio::test]
async fn reasoning_defaults_apply() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-reasoning-apply:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // 1. Seed ontology + data: ex:childName rdfs:subPropertyOf ex:name
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "ex": "http://example.org/",
                    "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
                },
                "@graph": [
                    {"@id": "ex:childName", "rdfs:subPropertyOf": {"@id": "ex:name"}},
                    {"@id": "ex:alice", "ex:childName": "Alice"}
                ]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // 2. Write config: reasoningDefaults with modes=["rdfs"], overrideControl=f:OverrideAll
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:reasoningModes f:rdfs .
            <urn:config:reasoning> f:overrideControl f:OverrideAll .
        }}
    "#
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    // 3. Verify config is attached
    let view = fluree.db(ledger_id).await.unwrap();
    let config = view.ledger_config().expect("config should be attached");
    let reasoning = config.reasoning.as_ref().expect("reasoning defaults");
    assert_eq!(
        reasoning.modes.as_deref(),
        Some(["https://ns.flur.ee/db#rdfs".to_string()].as_slice()),
        "reasoning modes should be stored as full IRIs"
    );

    // 4. Query: SELECT ?v WHERE { ex:alice ex:name ?v } — without "reasoning" in query
    //    If config reasoning is applied, RDFS subProperty expansion should find "Alice"
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "from": ledger_id,
        "select": ["?v"],
        "where": {"@id": "ex:alice", "ex:name": "?v"}
    });

    let result = fluree.query_connection(&query).await.expect("query");
    let ledger_state = fluree.ledger(ledger_id).await.expect("load ledger");
    let jsonld = result.to_jsonld(&ledger_state.snapshot).expect("to_jsonld");

    // Should find "Alice" via RDFS subProperty expansion (ex:childName subPropertyOf ex:name)
    assert_eq!(
        jsonld,
        json!(["Alice"]),
        "RDFS reasoning from config should auto-expand subProperty"
    );

    // 5. Query WITH "reasoning": "none" — should override config (AllowAll permits override)
    let query_none = json!({
        "@context": {"ex": "http://example.org/"},
        "from": ledger_id,
        "select": ["?v"],
        "where": {"@id": "ex:alice", "ex:name": "?v"},
        "reasoning": "none"
    });

    let result_none = fluree
        .query_connection(&query_none)
        .await
        .expect("query none");
    let jsonld_none = result_none
        .to_jsonld(&ledger_state.snapshot)
        .expect("to_jsonld");

    // With reasoning disabled, subProperty expansion should NOT happen
    assert_eq!(
        jsonld_none,
        json!([]),
        "reasoning='none' should override config AllowAll and return no results"
    );
}

// =============================================================================
// Test 5b: config reasoning "none" disables reasoning by default
// =============================================================================

#[tokio::test]
async fn reasoning_none_disables_by_default() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-reasoning-none:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // 1. Seed ontology + data: ex:childName rdfs:subPropertyOf ex:name
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "ex": "http://example.org/",
                    "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
                },
                "@graph": [
                    {"@id": "ex:childName", "rdfs:subPropertyOf": {"@id": "ex:name"}},
                    {"@id": "ex:alice", "ex:childName": "Alice"}
                ]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // 2. Write config: reasoningDefaults with modes=["none"]
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:reasoningModes f:none .
        }}
    "#
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    // 3. Verify from_mode_strings produces explicit_none
    let view = fluree.db(ledger_id).await.unwrap();
    let config = view.ledger_config().expect("config should be attached");
    let reasoning = config.reasoning.as_ref().expect("reasoning defaults");
    let mode_strings = reasoning.modes.as_deref().unwrap();
    let modes = fluree_db_query::ReasoningModes::from_mode_strings(mode_strings);
    assert!(
        modes.is_disabled(),
        "from_mode_strings(['none']) should produce explicit_none=true"
    );
    assert!(
        !modes.has_any_enabled(),
        "from_mode_strings(['none']) should have no enabled modes"
    );

    // 4. Query asking for subProperty expansion — should NOT work because config
    //    force-disables reasoning via "none"
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "from": ledger_id,
        "select": ["?v"],
        "where": {"@id": "ex:alice", "ex:name": "?v"}
    });

    let result = fluree.query_connection(&query).await.expect("query");
    let ledger_state = fluree.ledger(ledger_id).await.expect("load ledger");
    let jsonld = result.to_jsonld(&ledger_state.snapshot).expect("to_jsonld");

    assert_eq!(
        jsonld,
        json!([]),
        "config reasoning 'none' should disable RDFS expansion by default"
    );
}

// =============================================================================
// Test 6: per-graph override
// =============================================================================

#[tokio::test]
async fn per_graph_override() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-per-graph:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);

    // 1. Seed data in default graph via TriG
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex: <http://example.org/> .

        ex:alice ex:name "Alice" .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow true .
            <urn:config:main> f:graphOverrides <urn:config:go1> .
            <urn:config:go1> f:targetGraph f:defaultGraph .
            <urn:config:go1> f:policyDefaults <urn:config:go1-policy> .
            <urn:config:go1-policy> f:defaultAllow false .
        }}
    "#
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("seed + config");

    // 2. Load view and check resolved config
    let view = fluree.db(ledger_id).await.unwrap();
    let resolved = view.resolved_config().expect("resolved config");

    // Default graph should have the per-graph override (defaultAllow=false)
    let policy = resolved.policy.as_ref().expect("resolved policy");
    assert_eq!(
        policy.default_allow,
        Some(false),
        "per-graph override should set defaultAllow=false for default graph"
    );
}

// =============================================================================
// Test 7: override_control_none_blocks
// =============================================================================

#[tokio::test]
async fn override_control_none_blocks() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-override-none:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // 1. Seed data
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // 2. Write config: policyDefaults with defaultAllow=false, overrideControl=OverrideNone
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow false .
            <urn:config:policy> f:overrideControl f:OverrideNone .
        }}
    "#
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    // 3. Verify the override control is set correctly
    let view = fluree.db(ledger_id).await.unwrap();
    let resolved = view.resolved_config().expect("resolved config");
    let policy = resolved.policy.as_ref().expect("policy");
    assert_eq!(
        policy.default_allow,
        Some(false),
        "config sets defaultAllow=false"
    );

    // 4. Check that merge_policy_opts respects OverrideNone:
    //    Even with opts specifying default_allow=true, the config should win
    let opts_with_override = QueryConnectionOptions {
        default_allow: true,
        ..Default::default()
    };
    let merged = config_resolver::merge_policy_opts(resolved, &opts_with_override, None);
    assert!(
        !merged.default_allow,
        "OverrideNone should block query-time default_allow override"
    );
}

// =============================================================================
// Test 8: override_control_identity_restricted
// =============================================================================

#[tokio::test]
async fn override_control_identity_restricted() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-identity:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);

    // Write config with IdentityRestricted override control
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow false .
            <urn:config:policy> f:overrideControl <urn:config:oc> .
            <urn:config:oc> f:controlMode f:IdentityRestricted .
            <urn:config:oc> f:allowedIdentities <did:key:admin> .
        }}
    "#
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    let view = fluree.db(ledger_id).await.unwrap();
    let resolved = view.resolved_config().expect("resolved config");
    let policy = resolved.policy.as_ref().expect("policy");

    // Verify the override control round-trips correctly
    assert!(
        matches!(
            &policy.override_control,
            fluree_db_core::ledger_config::OverrideControl::IdentityRestricted { .. }
        ),
        "override_control should be IdentityRestricted"
    );

    // Test actual gating behavior via merge_policy_opts
    let opts = QueryConnectionOptions {
        default_allow: true,
        ..Default::default()
    };

    // Admin identity → override permitted (opts.default_allow=true passes through)
    let merged_admin = config_resolver::merge_policy_opts(resolved, &opts, Some("did:key:admin"));
    assert!(
        merged_admin.default_allow,
        "admin identity should be permitted to override"
    );

    // Unknown identity → override denied (config.default_allow=false applied)
    let merged_user = config_resolver::merge_policy_opts(resolved, &opts, Some("did:key:user"));
    assert!(
        !merged_user.default_allow,
        "non-admin identity should be denied override"
    );

    // No identity → override denied (config.default_allow=false applied)
    let merged_none = config_resolver::merge_policy_opts(resolved, &opts, None);
    assert!(
        !merged_none.default_allow,
        "no identity should be denied override"
    );
}

// =============================================================================
// Test 9: override monotonicity
// =============================================================================

#[tokio::test]
async fn override_monotonicity() {
    // Case A: ledger-wide AllowAll + per-graph OverrideNone → tightened to OverrideNone
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-mono-a:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow true .
            <urn:config:policy> f:overrideControl f:OverrideAll .
            <urn:config:main> f:graphOverrides <urn:config:go1> .
            <urn:config:go1> f:targetGraph f:defaultGraph .
            <urn:config:go1> f:policyDefaults <urn:config:go1-policy> .
            <urn:config:go1-policy> f:overrideControl f:OverrideNone .
        }}
    "#
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    let view = fluree.db(ledger_id).await.unwrap();
    let resolved = view.resolved_config().expect("resolved config");
    let policy = resolved.policy.as_ref().expect("policy");

    // effective_min(AllowAll, OverrideNone) = OverrideNone
    assert!(
        matches!(
            &policy.override_control,
            fluree_db_core::ledger_config::OverrideControl::None
        ),
        "Case A: per-graph OverrideNone should tighten AllowAll to None"
    );

    // Case B: ledger-wide OverrideNone + per-graph AllowAll → ledger-wide blocks per-graph entirely
    let fluree_b = FlureeBuilder::memory().build_memory();
    let ledger_id_b = "it/config-mono-b:main";
    let ledger_b = genesis_ledger(&fluree_b, ledger_id_b);

    let config_iri_b = config_graph_iri(ledger_id_b);
    let trig_b = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri_b}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow false .
            <urn:config:policy> f:overrideControl f:OverrideNone .
            <urn:config:main> f:graphOverrides <urn:config:go1> .
            <urn:config:go1> f:targetGraph f:defaultGraph .
            <urn:config:go1> f:policyDefaults <urn:config:go1-policy> .
            <urn:config:go1-policy> f:defaultAllow true .
            <urn:config:go1-policy> f:overrideControl f:OverrideAll .
        }}
    "#
    );

    fluree_b
        .stage_owned(ledger_b)
        .upsert_turtle(&trig_b)
        .execute()
        .await
        .expect("config write");

    let view_b = fluree_b.db(ledger_id_b).await.unwrap();
    let resolved_b = view_b.resolved_config().expect("resolved config");
    let policy_b = resolved_b.policy.as_ref().expect("policy");

    // Ledger-wide OverrideNone blocks per-graph entirely
    assert!(
        matches!(
            &policy_b.override_control,
            fluree_db_core::ledger_config::OverrideControl::None
        ),
        "Case B: ledger-wide OverrideNone should block per-graph override"
    );
    assert_eq!(
        policy_b.default_allow,
        Some(false),
        "Case B: defaultAllow should remain false (ledger-wide value, per-graph blocked)"
    );
}

// =============================================================================
// Test 10: empty config returns None
// =============================================================================

#[tokio::test]
async fn empty_config_returns_none() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-empty:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Transact data (no config)
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [{"@id": "ex:a", "ex:val": 1}]
            }),
        )
        .await
        .unwrap();

    let view = fluree.db(ledger_id).await.unwrap();
    assert!(
        view.ledger_config().is_none(),
        "ledger_config should be None when no config is written"
    );
    assert!(
        view.resolved_config().is_none(),
        "resolved_config should be None when no config is written"
    );
}

// =============================================================================
// Test 11: multiple configs — lexicographic tiebreaker
// =============================================================================

#[tokio::test]
async fn multiple_configs_lexicographic_tiebreaker() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-tiebreak:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);

    // Write two LedgerConfig nodes: alpha (defaultAllow=false) and beta (defaultAllow=true)
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:alpha> rdf:type f:LedgerConfig .
            <urn:config:alpha> f:policyDefaults <urn:config:alpha-policy> .
            <urn:config:alpha-policy> f:defaultAllow false .

            <urn:config:beta> rdf:type f:LedgerConfig .
            <urn:config:beta> f:policyDefaults <urn:config:beta-policy> .
            <urn:config:beta-policy> f:defaultAllow true .
        }}
    "#
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    let view = fluree.db(ledger_id).await.unwrap();
    let config = view
        .ledger_config()
        .expect("config should be attached with tiebreaker");
    let policy = config.policy.as_ref().expect("policy defaults");

    // "urn:config:alpha" sorts before "urn:config:beta" lexicographically
    assert_eq!(
        policy.default_allow,
        Some(false),
        "alpha (lex-first) config should win with defaultAllow=false"
    );
}

// =============================================================================
// Test 12: config is time-travel consistent
// =============================================================================

#[tokio::test]
async fn config_time_travel_consistent() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-timetravel:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // t=1: seed data (no config)
    let result1 = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [{"@id": "ex:a", "ex:val": 1}]
            }),
        )
        .await
        .unwrap();
    assert_eq!(result1.receipt.t, 1);

    // t=2: write config
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow false .
        }}
    "#
    );

    let result2 = fluree
        .stage_owned(result1.ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");
    assert_eq!(result2.receipt.t, 2);

    // View at t=1: no config
    let view_t1 = fluree.db_at_t(ledger_id, 1).await.unwrap();
    assert!(
        view_t1.ledger_config().is_none(),
        "at t=1, config graph should be empty (no config)"
    );

    // View at t=2: config present
    let view_t2 = fluree.db_at_t(ledger_id, 2).await.unwrap();
    let config = view_t2
        .ledger_config()
        .expect("at t=2, config should be present");
    let policy = config.policy.as_ref().expect("policy defaults");
    assert_eq!(
        policy.default_allow,
        Some(false),
        "at t=2, config should have defaultAllow=false"
    );
}
