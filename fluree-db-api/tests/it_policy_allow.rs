//! Policy f:allow integration tests (Clojure parity)
//!
//! Ports from `db-clojure/test/fluree/db/policy/allow_test.clj`.
//! Tests f:allow true/false, precedence over f:query, and targeting modes.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{assert_index_defaults, genesis_ledger, normalize_rows};

/// Helper to seed test data with users having sensitive SSN property.
async fn seed_user_data(fluree: &support::MemoryFluree, alias: &str) {
    let ledger0 = genesis_ledger(fluree, alias);

    let txn = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:ssn": "111-11-1111",
                "ex:age": 30
            },
            {
                "@id": "ex:bob",
                "@type": "ex:User",
                "schema:name": "Bob",
                "schema:ssn": "222-22-2222",
                "ex:age": 25
            },
            {
                "@id": "ex:carol",
                "@type": "ex:Admin",
                "schema:name": "Carol",
                "schema:ssn": "333-33-3333",
                "ex:age": 35
            }
        ]
    });

    let _ = fluree
        .insert(ledger0, &txn)
        .await
        .expect("seed should succeed");
}

/// Test f:allow false for property-level deny.
///
/// Clojure parity: f:allow: false on a property should deny access to that property
/// even when default-allow is true.
#[tokio::test]
async fn policy_allow_false_denies_property() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    seed_user_data(&fluree, "policy/allow-false:main").await;

    // Policy that denies SSN property but allows everything else
    let policy = json!([
        {
            "@id": "ex:denySsnPolicy",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:onProperty": [{"@id": "http://schema.org/ssn"}],
            "f:allow": false
        },
        {
            "@id": "ex:allowAllPolicy",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:allow": true
        }
    ]);

    // Query for SSN - should return empty due to deny
    let query_ssn = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/allow-false:main",
        "opts": {
            "policy": policy.clone(),
            "default-allow": true
        },
        "select": ["?name", "?ssn"],
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:name": "?name",
            "schema:ssn": "?ssn"
        }
    });

    let result = fluree
        .query_connection(&query_ssn)
        .await
        .expect("query_connection");
    let ledger = fluree
        .ledger("policy/allow-false:main")
        .await
        .expect("ledger");
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    // SSN should be denied, so WHERE can't match
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([])),
        "f:allow: false on SSN property should deny access"
    );

    // Query for name only - should work
    let query_name = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/allow-false:main",
        "opts": {
            "policy": policy,
            "default-allow": true
        },
        "select": "?name",
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:name": "?name"
        }
    });

    let result_name = fluree
        .query_connection(&query_name)
        .await
        .expect("query_connection");
    let jsonld_name = result_name.to_jsonld(&ledger.db).expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld_name),
        normalize_rows(&json!(["Alice", "Bob"])),
        "Other properties should still be accessible"
    );
}

/// Test f:allow precedence over f:query.
///
/// Clojure parity: When a policy has BOTH f:allow and f:query, f:allow takes precedence.
/// This is useful for defining a "static deny" that doesn't evaluate the query.
#[tokio::test]
async fn policy_allow_precedence_over_fquery() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    seed_user_data(&fluree, "policy/allow-precedence:main").await;

    // Policy with both f:allow: false AND f:query (f:allow should win)
    let policy = json!([
        {
            "@id": "ex:staticDenyPolicy",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:onProperty": [{"@id": "http://schema.org/ssn"}],
            // Both f:allow and f:query present - f:allow takes precedence
            "f:allow": false,
            "f:query": {
                "@type": "@json",
                "@value": {
                    // This query WOULD match, but f:allow: false overrides it
                    "where": [{"@id": "?$this"}]
                }
            }
        },
        {
            "@id": "ex:allowAllPolicy",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:allow": true
        }
    ]);

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/allow-precedence:main",
        "opts": {
            "policy": policy,
            "default-allow": true
        },
        "select": ["?name", "?ssn"],
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:name": "?name",
            "schema:ssn": "?ssn"
        }
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree
        .ledger("policy/allow-precedence:main")
        .await
        .expect("ledger");
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    // f:allow: false should override f:query
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([])),
        "f:allow: false should take precedence over f:query"
    );
}

/// Test f:onClass targeting with f:query.
///
/// Clojure parity: Policies can target specific classes using f:onClass.
/// The policy only applies to instances of that class.
#[tokio::test]
async fn policy_onclass_with_fquery() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    seed_user_data(&fluree, "policy/onclass:main").await;

    // Policy that only allows viewing Users (not Admins) via f:onClass
    let policy = json!([
        {
            "@id": "ex:userOnlyPolicy",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:onClass": [{"@id": "http://example.org/ns/User"}],
            "f:allow": true
        }
        // No default allow - Admins should be denied
    ]);

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/onclass:main",
        "opts": {
            "policy": policy,
            "default-allow": false
        },
        "select": "?name",
        "where": {
            "@id": "?s",
            "schema:name": "?name"
        }
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree.ledger("policy/onclass:main").await.expect("ledger");
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    // Only Users (Alice, Bob) should be visible, not Admin (Carol)
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!(["Alice", "Bob"])),
        "f:onClass should restrict policy to User instances only"
    );
}

/// Test f:onClass with f:query that uses ?$this.
///
/// Clojure parity: f:onClass combined with f:query that checks properties of ?$this.
#[tokio::test]
async fn policy_onclass_with_fquery_this_check() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    seed_user_data(&fluree, "policy/onclass-fquery:main").await;

    // Policy that allows viewing Users only if age >= 30
    let policy = json!([
        {
            "@id": "ex:matureUserPolicy",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:onClass": [{"@id": "http://example.org/ns/User"}],
            "f:query": {
                "@type": "@json",
                "@value": {
                    "@context": {"ex": "http://example.org/ns/"},
                    "where": [
                        {"@id": "?$this", "ex:age": "?age"},
                        ["filter", "(>= ?age 30)"]
                    ]
                }
            }
        },
        // Allow Admins unconditionally
        {
            "@id": "ex:adminPolicy",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:onClass": [{"@id": "http://example.org/ns/Admin"}],
            "f:allow": true
        }
    ]);

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/onclass-fquery:main",
        "opts": {
            "policy": policy,
            "default-allow": false
        },
        "select": "?name",
        "where": {
            "@id": "?s",
            "schema:name": "?name"
        }
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree
        .ledger("policy/onclass-fquery:main")
        .await
        .expect("ledger");
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    // Alice (User, age 30) and Carol (Admin) should be visible
    // Bob (User, age 25) should be denied by the age filter
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!(["Alice", "Carol"])),
        "f:onClass with f:query should filter by class AND query condition"
    );
}

/// Test f:onProperty with f:query combining targeting and dynamic evaluation.
///
/// Clojure parity: f:onProperty targets specific properties, f:query provides
/// dynamic evaluation for whether access is granted.
#[tokio::test]
async fn policy_onproperty_with_fquery() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    seed_user_data(&fluree, "policy/onprop-fquery:main").await;

    // Policy that allows SSN access only for users age >= 30
    let policy = json!([
        {
            "@id": "ex:ssnAgePolicy",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:onProperty": [{"@id": "http://schema.org/ssn"}],
            "f:query": {
                "@type": "@json",
                "@value": {
                    "@context": {"ex": "http://example.org/ns/"},
                    "where": [
                        {"@id": "?$this", "ex:age": "?age"},
                        ["filter", "(>= ?age 30)"]
                    ]
                }
            }
        },
        // Allow all other properties
        {
            "@id": "ex:allowOtherProps",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:allow": true
        }
    ]);

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/onprop-fquery:main",
        "opts": {
            "policy": policy,
            "default-allow": false
        },
        "select": ["?name", "?ssn"],
        "where": {
            "@id": "?s",
            "schema:name": "?name",
            "schema:ssn": "?ssn"
        }
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree
        .ledger("policy/onprop-fquery:main")
        .await
        .expect("ledger");
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    // Only Alice (age 30) and Carol (age 35) have SSN visible
    // Bob (age 25) SSN is denied by f:query
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["Alice", "111-11-1111"], ["Carol", "333-33-3333"]])),
        "f:onProperty with f:query should conditionally allow SSN based on age"
    );
}

/// Test multiple f:onProperty values (union behavior).
///
/// Clojure parity: When f:onProperty contains multiple values, the policy
/// applies to ANY of those properties.
#[tokio::test]
async fn policy_onproperty_multiple_values() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    seed_user_data(&fluree, "policy/onprop-multi:main").await;

    // Policy that denies both SSN and age properties
    let policy = json!([
        {
            "@id": "ex:denySensitivePolicy",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:onProperty": [
                {"@id": "http://schema.org/ssn"},
                {"@id": "http://example.org/ns/age"}
            ],
            "f:allow": false
        },
        // Allow all other properties
        {
            "@id": "ex:allowOtherProps",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:allow": true
        }
    ]);

    // Query for name and age - age should be denied
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/onprop-multi:main",
        "opts": {
            "policy": policy.clone(),
            "default-allow": true
        },
        "select": ["?name", "?age"],
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:name": "?name",
            "ex:age": "?age"
        }
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree
        .ledger("policy/onprop-multi:main")
        .await
        .expect("ledger");
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    // Age is denied, so WHERE clause fails to match
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([])),
        "f:onProperty with multiple values should deny all listed properties"
    );

    // Query for name only - should work
    let query_name = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/onprop-multi:main",
        "opts": {
            "policy": policy,
            "default-allow": true
        },
        "select": "?name",
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:name": "?name"
        }
    });

    let result_name = fluree
        .query_connection(&query_name)
        .await
        .expect("query_connection");
    let jsonld_name = result_name.to_jsonld(&ledger.db).expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld_name),
        normalize_rows(&json!(["Alice", "Bob"])),
        "Non-denied properties should still be accessible"
    );
}

/// Test f:required flag behavior.
///
/// Clojure parity: f:required: true means this policy MUST allow for access
/// to be granted. It's a "required policy" that cannot be bypassed.
#[tokio::test]
async fn policy_required_flag() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    seed_user_data(&fluree, "policy/required:main").await;

    // Policy with f:required: true that denies young users
    let policy = json!([
        {
            "@id": "ex:requiredAgePolicy",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:required": true,
            "f:query": {
                "@type": "@json",
                "@value": {
                    "@context": {"ex": "http://example.org/ns/"},
                    "where": [
                        {"@id": "?$this", "ex:age": "?age"},
                        ["filter", "(>= ?age 30)"]
                    ]
                }
            }
        },
        // Allow-all policy that would normally allow everything
        {
            "@id": "ex:allowAllPolicy",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:allow": true
        }
    ]);

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/required:main",
        "opts": {
            "policy": policy,
            "default-allow": true
        },
        "select": "?name",
        "where": {
            "@id": "?s",
            "schema:name": "?name"
        }
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree.ledger("policy/required:main").await.expect("ledger");
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    // f:required policy must allow - Bob (age 25) is denied despite allow-all
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!(["Alice", "Carol"])),
        "f:required: true policy must allow for access to be granted"
    );
}

/// Test combining f:onClass and f:onProperty.
///
/// Clojure parity: When both f:onClass and f:onProperty are present,
/// the policy targets those properties ONLY on instances of those classes.
#[tokio::test]
async fn policy_onclass_and_onproperty_combined() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    seed_user_data(&fluree, "policy/combined-targeting:main").await;

    // Policy that denies SSN only for User class (not Admin)
    let policy = json!([
        {
            "@id": "ex:userSsnDenyPolicy",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:onClass": [{"@id": "http://example.org/ns/User"}],
            "f:onProperty": [{"@id": "http://schema.org/ssn"}],
            "f:allow": false
        },
        // Allow all other access
        {
            "@id": "ex:allowAllPolicy",
            "@type": "f:AccessPolicy",
            "f:action": "f:view",
            "f:allow": true
        }
    ]);

    // Query all SSNs
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/combined-targeting:main",
        "opts": {
            "policy": policy,
            "default-allow": true
        },
        "select": ["?name", "?ssn"],
        "where": {
            "@id": "?s",
            "schema:name": "?name",
            "schema:ssn": "?ssn"
        }
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree
        .ledger("policy/combined-targeting:main")
        .await
        .expect("ledger");
    let jsonld = result.to_jsonld(&ledger.db).expect("to_jsonld");

    // Only Carol (Admin) SSN should be visible
    // Alice and Bob (Users) SSN should be denied
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["Carol", "333-33-3333"]])),
        "f:onClass + f:onProperty should deny SSN only for User instances"
    );
}
