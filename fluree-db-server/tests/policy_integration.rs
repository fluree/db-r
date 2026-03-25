use axum::body::Body;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ed25519_dalek::{Signer, SigningKey};
use fluree_db_credential::did_from_pubkey;
use fluree_db_server::config::DataAuthMode;
use fluree_db_server::routes::build_router;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

// ── helpers ──────────────────────────────────────────────────────────────────

async fn json_body(resp: http::Response<Body>) -> (StatusCode, JsonValue) {
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    let json: JsonValue = serde_json::from_slice(&bytes).unwrap_or(JsonValue::Null);
    (status, json)
}

fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn create_jws(claims: &JsonValue, signing_key: &SigningKey) -> String {
    let pubkey = signing_key.verifying_key().to_bytes();
    let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);

    let header = serde_json::json!({
        "alg": "EdDSA",
        "jwk": {
            "kty": "OKP",
            "crv": "Ed25519",
            "x": pubkey_b64
        }
    });

    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string().as_bytes());
    let payload_b64 = URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes());

    let signing_input = format!("{}.{}", header_b64, payload_b64);
    let signature = signing_key.sign(signing_input.as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

    format!("{}.{}.{}", header_b64, payload_b64, sig_b64)
}

fn identity_token(signing_key: &SigningKey, identity: &str, ledger: &str) -> String {
    let claims = serde_json::json!({
        "iss": did_from_pubkey(&signing_key.verifying_key().to_bytes()),
        "exp": now_secs() + 3600,
        "iat": now_secs(),
        "fluree.identity": identity,
        "fluree.ledger.read.ledgers": [ledger],
    });
    create_jws(&claims, signing_key)
}

fn policy_test_state() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        data_auth_mode: DataAuthMode::Optional,
        data_auth_insecure_accept_any_issuer: true,
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).expect("AppState::new"));
    (tmp, state)
}

/// Creates the ledger, inserts 3 documents, then inserts policies + identity nodes.
///
/// Documents:
///   ex:doc1 — "Public Post"        classification=public
///   ex:doc2 — "Internal Memo"      classification=internal
///   ex:doc3 — "Executive Salaries" classification=confidential
///
/// Policy classes:
///   ex:PublicClass   → only public
///   ex:EmployeeClass → public OR internal
///   ex:ManagerClass  → f:allow true (all)
///
/// Identity nodes (in-ledger):
///   ex:public-user   → ex:PublicClass
///   ex:employee-user → ex:EmployeeClass
///   ex:manager-user  → ex:ManagerClass
async fn setup_policy_ledger(app: axum::Router, ledger: &str) -> axum::Router {
    // 1. Create ledger
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "ledger": ledger }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "create ledger");

    // 2. Insert sample documents
    let docs_tx = serde_json::json!({
        "@context": {
            "ex": "http://example.org/",
            "schema": "http://schema.org/"
        },
        "insert": [
            {
                "@id": "ex:doc1",
                "@type": "ex:Document",
                "schema:name": "Public Post",
                "ex:classification": "public",
                "ex:content": "visible to all"
            },
            {
                "@id": "ex:doc2",
                "@type": "ex:Document",
                "schema:name": "Internal Memo",
                "ex:classification": "internal",
                "ex:content": "visible to employees"
            },
            {
                "@id": "ex:doc3",
                "@type": "ex:Document",
                "schema:name": "Executive Salaries",
                "ex:classification": "confidential",
                "ex:content": "visible to managers"
            }
        ]
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/transact?ledger={}", ledger))
                .header("content-type", "application/json")
                .body(Body::from(docs_tx.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "insert documents");

    // 3. Insert policies + identity nodes
    let policy_tx = serde_json::json!({
        "@context": {
            "f": "https://ns.flur.ee/db#",
            "ex": "http://example.org/"
        },
        "insert": [
            // Public policy: only ex:classification = "public"
            {
                "@id": "ex:public-policy",
                "@type": ["f:AccessPolicy", "ex:PublicClass"],
                "f:action": [{"@id": "f:view"}],
                "f:query": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [{"@id": "?$this", "ex:classification": "public"}]
                    }
                }
            },
            // Employee policy: public OR internal
            {
                "@id": "ex:employee-policy",
                "@type": ["f:AccessPolicy", "ex:EmployeeClass"],
                "f:action": [{"@id": "f:view"}],
                "f:query": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [
                            ["union",
                                {"@id": "?$this", "ex:classification": "public"},
                                {"@id": "?$this", "ex:classification": "internal"}
                            ]
                        ]
                    }
                }
            },
            // Manager policy: f:allow true — bypass filter entirely
            {
                "@id": "ex:manager-policy",
                "@type": ["f:AccessPolicy", "ex:ManagerClass"],
                "f:action": [{"@id": "f:view"}],
                "f:allow": true
            },
            // Identity nodes using full IRIs (not compact CURIEs).
            // JSON-LD would expand "ex:public-user" → "http://example.org/public-user"
            // during ingestion, so the token's fluree.identity must also be the
            // full IRI for resolve_identity_iri_to_sid to find the subject node.
            {
                "@id": "http://example.org/public-user",
                "f:policyClass": [{"@id": "ex:PublicClass"}]
            },
            {
                "@id": "http://example.org/employee-user",
                "f:policyClass": [{"@id": "ex:EmployeeClass"}]
            },
            {
                "@id": "http://example.org/manager-user",
                "f:policyClass": [{"@id": "ex:ManagerClass"}]
            }
        ]
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/fluree/transact?ledger={}", ledger))
                .header("content-type", "application/json")
                .body(Body::from(policy_tx.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "insert policies");

    app
}

/// Runs the standard document query and returns the result rows.
///
/// The query selects `?name` and `?class` for all `ex:Document` subjects.
/// `default-allow` is controlled by the caller. No `opts.identity` — the
/// server injects it from the Bearer token via `force_query_auth_opts`.
async fn query_docs(
    app: axum::Router,
    ledger: &str,
    token: Option<&str>,
    default_allow: bool,
) -> (StatusCode, JsonValue) {
    let body = serde_json::json!({
        "@context": {
            "ex": "http://example.org/",
            "schema": "http://schema.org/"
        },
        "opts": { "default-allow": default_allow },
        "select": ["?name", "?class"],
        "where": [
            {"@id": "?doc", "@type": "ex:Document"},
            {"@id": "?doc", "schema:name": "?name"},
            {"@id": "?doc", "ex:classification": "?class"}
        ]
    });

    let mut req = Request::builder()
        .method("POST")
        .uri(format!("/v1/fluree/query/{}", ledger))
        .header("content-type", "application/json");

    if let Some(tok) = token {
        req = req.header("authorization", format!("Bearer {}", tok));
    }

    let resp = app
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();

    json_body(resp).await
}

fn names_from_results(results: &JsonValue) -> Vec<&str> {
    results
        .as_array()
        .expect("results should be an array")
        .iter()
        .filter_map(|row| row.get(0).and_then(|v| v.as_str()))
        .collect()
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Public identity can only see documents classified as "public".
#[tokio::test]
async fn public_identity_sees_only_public_docs() {
    let (_tmp, state) = policy_test_state();
    let app = setup_policy_ledger(build_router(state), "policy1:main").await;

    let signing_key = SigningKey::from_bytes(&[1u8; 32]);
    let token = identity_token(
        &signing_key,
        "http://example.org/public-user",
        "policy1:main",
    );

    let (status, json) = query_docs(app, "policy1:main", Some(&token), false).await;
    assert_eq!(status, StatusCode::OK);

    let names = names_from_results(&json);
    assert_eq!(
        names.len(),
        1,
        "public user should see exactly 1 document; got: {:?}",
        names
    );
    assert!(
        names.contains(&"Public Post"),
        "expected 'Public Post'; got: {:?}",
        names
    );
}

/// Employee identity sees public + internal documents, but not confidential.
///
/// This is the primary regression test for issue #106: `execute_query` was
/// building a plain `GraphDb` and discarding `opts.identity`, so no policy
/// filtering was applied and all 3 documents were returned.
#[tokio::test]
async fn employee_identity_sees_public_and_internal() {
    let (_tmp, state) = policy_test_state();
    let app = setup_policy_ledger(build_router(state), "policy2:main").await;

    let signing_key = SigningKey::from_bytes(&[2u8; 32]);
    let token = identity_token(
        &signing_key,
        "http://example.org/employee-user",
        "policy2:main",
    );

    let (status, json) = query_docs(app, "policy2:main", Some(&token), false).await;
    assert_eq!(status, StatusCode::OK);

    let names = names_from_results(&json);
    assert_eq!(
        names.len(),
        2,
        "employee should see exactly 2 documents; got: {:?}",
        names
    );
    assert!(
        names.contains(&"Public Post"),
        "expected 'Public Post'; got: {:?}",
        names
    );
    assert!(
        names.contains(&"Internal Memo"),
        "expected 'Internal Memo'; got: {:?}",
        names
    );
    assert!(
        !names.contains(&"Executive Salaries"),
        "employee must NOT see 'Executive Salaries'; got: {:?}",
        names
    );
}

/// Manager identity has `f:allow: true` and should see all documents.
#[tokio::test]
async fn manager_identity_sees_all_documents() {
    let (_tmp, state) = policy_test_state();
    let app = setup_policy_ledger(build_router(state), "policy3:main").await;

    let signing_key = SigningKey::from_bytes(&[3u8; 32]);
    let token = identity_token(
        &signing_key,
        "http://example.org/manager-user",
        "policy3:main",
    );

    let (status, json) = query_docs(app, "policy3:main", Some(&token), false).await;
    assert_eq!(status, StatusCode::OK);

    let names = names_from_results(&json);
    assert_eq!(
        names.len(),
        3,
        "manager should see all 3 documents; got: {:?}",
        names
    );
}

/// An identity with no policyClass node in the ledger and `default-allow: false`
/// should see nothing — fail-closed.
///
/// Regression test for issue #106 Bug 2: the server was returning a 500 when
/// looking up an identity IRI that had no subject node in the ledger.
#[tokio::test]
async fn identity_without_policy_class_default_allow_false_denies_all() {
    let (_tmp, state) = policy_test_state();
    let app = setup_policy_ledger(build_router(state), "policy4:main").await;

    let signing_key = SigningKey::from_bytes(&[4u8; 32]);
    // This identity IRI has no node in the ledger — no f:policyClass binding
    let token = identity_token(
        &signing_key,
        "http://example.org/unknown-user",
        "policy4:main",
    );

    let (status, json) = query_docs(app, "policy4:main", Some(&token), false).await;
    assert_eq!(status, StatusCode::OK);

    let names = names_from_results(&json);
    assert!(
        names.is_empty(),
        "unknown identity + default-allow:false must see nothing; got: {:?}",
        names
    );
}

/// An identity with no subject node in the ledger must be denied even when
/// `default-allow: true` is set.
///
/// Rationale: `default-allow` governs access for *known* identities that happen
/// to have no policy restrictions attached. An identity that is completely unknown
/// to the ledger cannot be vouched for by any policy, so exposing data via a
/// permissive default would silently bypass the intent of identity-scoped access
/// control — a footgun for DB admins who rely on "data defends itself" in open
/// deployments. Callers who want open access can simply omit `opts.identity`.
#[tokio::test]
async fn unknown_identity_denied_even_with_default_allow_true() {
    let (_tmp, state) = policy_test_state();
    let app = setup_policy_ledger(build_router(state), "policy5:main").await;

    let signing_key = SigningKey::from_bytes(&[5u8; 32]);
    let token = identity_token(
        &signing_key,
        "http://example.org/unknown-user",
        "policy5:main",
    );

    let (status, json) = query_docs(app, "policy5:main", Some(&token), true).await;
    assert_eq!(status, StatusCode::OK);

    let names = names_from_results(&json);
    assert!(
        names.is_empty(),
        "unknown identity must be denied even with default-allow:true; got: {:?}",
        names
    );
}

/// A property-level `f:allow: false` on `ex:content` should strip that field
/// from results for the employee identity. Documents that pass the row-level
/// filter should still appear, but without the `ex:content` binding.
#[tokio::test]
async fn property_level_deny_hides_ex_content_field() {
    let (_tmp, state) = policy_test_state();
    let app = setup_policy_ledger(build_router(state), "policy6:main").await;

    // Add a property-level deny policy for ex:content on the employee class
    let deny_tx = serde_json::json!({
        "@context": {
            "f": "https://ns.flur.ee/db#",
            "ex": "http://example.org/"
        },
        "insert": [
            {
                "@id": "ex:employee-deny-content",
                "@type": ["f:AccessPolicy", "ex:EmployeeClass"],
                "f:action": [{"@id": "f:view"}],
                "f:onProperty": [{"@id": "ex:content"}],
                "f:allow": false
            }
        ]
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/transact?ledger=policy6:main")
                .header("content-type", "application/json")
                .body(Body::from(deny_tx.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "insert content deny policy");

    let signing_key = SigningKey::from_bytes(&[6u8; 32]);
    let token = identity_token(
        &signing_key,
        "http://example.org/employee-user",
        "policy6:main",
    );

    // Query 1: without ex:content in WHERE — verifies row-level policy still works.
    // Employee should see public + internal docs (2), not the confidential one.
    let row_level_body = serde_json::json!({
        "@context": {
            "ex": "http://example.org/",
            "schema": "http://schema.org/"
        },
        "opts": { "default-allow": false },
        "select": ["?name", "?class"],
        "where": [
            {"@id": "?doc", "@type": "ex:Document"},
            {"@id": "?doc", "schema:name": "?name"},
            {"@id": "?doc", "ex:classification": "?class"}
        ]
    });

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query/policy6:main")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {}", token))
                .body(Body::from(row_level_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    let rows = json.as_array().expect("results should be array");
    let names: Vec<&str> = rows
        .iter()
        .filter_map(|row| row.get(0).and_then(|v| v.as_str()))
        .collect();
    assert!(
        !names.contains(&"Executive Salaries"),
        "employee must NOT see confidential doc; got: {:?}",
        names
    );
    assert_eq!(
        rows.len(),
        2,
        "employee should see 2 documents; got: {:?}",
        names
    );

    // Query 2: with ex:content as a required triple pattern — verifies property-level deny.
    // Because ex:content is property-denied for EmployeeClass, the triple pattern
    // `?doc ex:content ?content` never matches, so the entire query returns 0 rows.
    // This is the expected fail-closed behavior: denied properties are invisible,
    // so required patterns on them produce no results.
    let property_deny_body = serde_json::json!({
        "@context": {
            "ex": "http://example.org/",
            "schema": "http://schema.org/"
        },
        "opts": { "default-allow": false },
        "select": ["?name", "?class", "?content"],
        "where": [
            {"@id": "?doc", "@type": "ex:Document"},
            {"@id": "?doc", "schema:name": "?name"},
            {"@id": "?doc", "ex:classification": "?class"},
            {"@id": "?doc", "ex:content": "?content"}
        ]
    });

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query/policy6:main")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {}", token))
                .body(Body::from(property_deny_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    let rows_with_content = json.as_array().expect("results should be array");
    assert_eq!(
        rows_with_content.len(),
        0,
        "property deny on ex:content must cause required triple pattern to match nothing; got: {:?}",
        rows_with_content
    );
}
