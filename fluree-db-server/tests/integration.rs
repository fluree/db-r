use axum::body::Body;
use fluree_db_server::config::{AdminAuthMode, DataAuthMode, EventsAuthMode};
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

fn test_state() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).expect("AppState::new"));
    (tmp, state)
}

async fn json_body(resp: http::Response<Body>) -> (StatusCode, JsonValue) {
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    let json: JsonValue = serde_json::from_slice(&bytes).expect("valid JSON response");
    (status, json)
}

fn json_contains_string(v: &JsonValue, needle: &str) -> bool {
    match v {
        JsonValue::Null => false,
        JsonValue::Bool(_) => false,
        JsonValue::Number(_) => false,
        JsonValue::String(s) => s.contains(needle),
        JsonValue::Array(a) => a.iter().any(|x| json_contains_string(x, needle)),
        JsonValue::Object(o) => o.values().any(|x| json_contains_string(x, needle)),
    }
}

#[tokio::test]
async fn health_check_ok() {
    let (_tmp, state) = test_state();
    let app = build_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json.get("status").and_then(|v| v.as_str()), Some("ok"));
    assert!(json.get("version").and_then(|v| v.as_str()).is_some());
}

#[tokio::test]
async fn stats_ok() {
    let (_tmp, state) = test_state();
    let app = build_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert!(json.get("uptime_secs").and_then(|v| v.as_u64()).is_some());
    assert_eq!(
        json.get("storage_type").and_then(|v| v.as_str()),
        Some("file")
    );
}

#[tokio::test]
async fn create_ledger_then_ledger_info() {
    let (_tmp, state) = test_state();
    let app = build_router(state.clone());

    // Create empty ledger - should return 201 with Clojure-compatible format
    let create_body = serde_json::json!({ "ledger": "test:main" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "Create should return 201 Created"
    );
    assert_eq!(
        json.get("ledger").and_then(|v| v.as_str()),
        Some("test:main")
    );
    // Empty ledger has t=0
    assert_eq!(json.get("t").and_then(|v| v.as_i64()), Some(0));
    // Should have tx-id and commit fields (Clojure parity)
    assert!(
        json.get("tx-id").is_some(),
        "Response should have tx-id field"
    );
    assert!(
        json.get("commit").is_some(),
        "Response should have commit field"
    );

    // Ledger info
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/info/test:main")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json.get("ledger").and_then(|v| v.as_str()),
        Some("test:main")
    );
    // New ledger has no commits yet
    assert_eq!(json.get("t").and_then(|v| v.as_i64()), Some(0));
}

#[tokio::test]
async fn insert_then_query_finds_value() {
    let (_tmp, state) = test_state();
    let app = build_router(state.clone());

    // Create
    let create_body = serde_json::json!({ "ledger": "test:main" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Insert - should return Clojure-compatible response format
    let insert_body = serde_json::json!({
      "@context": { "ex": "http://example.org/" },
      "@id": "ex:alice",
      "ex:name": "Alice"
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/insert")
                .header("content-type", "application/json")
                .header("fluree-ledger", "test:main")
                .body(Body::from(insert_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    // Verify Clojure-compatible response format
    assert_eq!(
        json.get("ledger").and_then(|v| v.as_str()),
        Some("test:main")
    );
    assert!(json.get("t").and_then(|v| v.as_i64()).unwrap_or(0) >= 1);
    assert!(
        json.get("tx-id").is_some(),
        "Response should have tx-id field"
    );
    let commit = json
        .get("commit")
        .expect("Response should have commit field");
    assert!(
        commit.get("address").is_some(),
        "Commit should have address"
    );
    assert!(commit.get("hash").is_some(), "Commit should have hash");

    // Query
    let query_body = serde_json::json!({
      "@context": { "ex": "http://example.org/" },
      "from": "test:main",
      "select": ["?name"],
      "where": { "@id": "?s", "ex:name": "?name" }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query")
                .header("content-type", "application/json")
                .body(Body::from(query_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        json_contains_string(&json, "Alice"),
        "Expected query response to contain 'Alice', got: {}",
        json
    );
}

#[tokio::test]
async fn ledger_with_slash_works_via_op_prefixed_routes() {
    let (_tmp, state) = test_state();
    let app = build_router(state.clone());

    // Create ledger with `/` in name
    let ledger = "group/test:main";
    let create_body = serde_json::json!({ "ledger": ledger });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Insert using `/v1/fluree/insert/<ledger...>`
    let insert_body = serde_json::json!({
      "@context": { "ex": "http://example.org/" },
      "@id": "ex:alice",
      "ex:name": "Alice"
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/insert/group/test:main")
                .header("content-type", "application/json")
                .body(Body::from(insert_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Query using `/v1/fluree/query/<ledger...>` without a FROM clause
    let query_body = serde_json::json!({
      "@context": { "ex": "http://example.org/" },
      "select": ["?name"],
      "where": { "@id": "?s", "ex:name": "?name" }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query/group/test:main")
                .header("content-type", "application/json")
                .body(Body::from(query_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        json_contains_string(&json, "Alice"),
        "Expected query response to contain 'Alice', got: {}",
        json
    );
}

#[tokio::test]
async fn transact_endpoint_accepts_jsonld_transactions() {
    let (_tmp, state) = test_state();
    let app = build_router(state.clone());

    // Create
    let create_body = serde_json::json!({ "ledger": "test:update" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Use /v1/fluree/transact
    let update_body = serde_json::json!({
        "ledger": "test:update",
        "@context": { "ex": "http://example.org/" },
        "insert": {
            "@id": "ex:bob",
            "ex:name": "Bob"
        }
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/transact")
                .header("content-type", "application/json")
                .body(Body::from(update_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json.get("ledger").and_then(|v| v.as_str()),
        Some("test:update")
    );
    assert!(json.get("t").and_then(|v| v.as_i64()).unwrap_or(0) >= 1);

    // Query to verify
    let query_body = serde_json::json!({
        "@context": { "ex": "http://example.org/" },
        "from": "test:update",
        "select": ["?name"],
        "where": { "@id": "?s", "ex:name": "?name" }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query")
                .header("content-type", "application/json")
                .body(Body::from(query_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert!(json_contains_string(&json, "Bob"));
}

#[tokio::test]
async fn ledger_scoped_insert_upsert_history() {
    let (_tmp, state) = test_state();
    let app = build_router(state.clone());

    // Create ledger
    let create_body = serde_json::json!({ "ledger": "scoped:test" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Ledger-scoped insert via /v1/fluree/insert/<ledger...>
    let insert_body = serde_json::json!({
        "@context": { "ex": "http://example.org/" },
        "@id": "ex:carol",
        "ex:name": "Carol"
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/insert/scoped:test")
                .header("content-type", "application/json")
                .body(Body::from(insert_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json.get("ledger").and_then(|v| v.as_str()),
        Some("scoped:test")
    );
    assert_eq!(json.get("t").and_then(|v| v.as_i64()), Some(1));

    // Ledger-scoped upsert via /v1/fluree/upsert/<ledger...>
    let upsert_body = serde_json::json!({
        "@context": { "ex": "http://example.org/" },
        "@id": "ex:carol",
        "ex:email": "carol@example.org"
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/upsert/scoped:test")
                .header("content-type", "application/json")
                .body(Body::from(upsert_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json.get("t").and_then(|v| v.as_i64()), Some(2));

    // History query via /v1/fluree/query/<ledger...> using explicit "from" + "to" keys
    // This replaces the old /:ledger/history endpoint - history queries now go through unified query interface
    // Note: Array syntax "from": ["ledger@t:1", "ledger@t:latest"] is a UNION query, not history mode
    // Use explicit "to" key for history queries
    let history_body = serde_json::json!({
        "@context": { "ex": "http://example.org/" },
        "from": "scoped:test@t:1",
        "to": "scoped:test@t:latest",
        "select": ["?p", "?v", "?t", "?op"],
        "where": { "@id": "ex:carol", "?p": { "@value": "?v", "@t": "?t", "@op": "?op" } },
        "orderBy": "?t"
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query/scoped:test")
                .header("content-type", "application/json")
                .body(Body::from(history_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK, "History query failed: {}", json);
    // History should return array of changes (assertions for name at t=1 and email at t=2)
    assert!(
        json.is_array(),
        "History should return an array, got: {}",
        json
    );
    let history = json.as_array().unwrap();
    assert!(
        history.len() >= 2,
        "Should have at least 2 history entries (name at t=1, email at t=2)"
    );
}

#[tokio::test]
async fn sparql_query_connection_from_clause_finds_value() {
    let (_tmp, state) = test_state();
    let app = build_router(state.clone());

    // Create
    let create_body = serde_json::json!({ "ledger": "test:main" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Insert
    let insert_body = serde_json::json!({
      "@context": { "ex": "http://example.org/" },
      "@id": "ex:alice",
      "ex:name": "Alice"
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/insert")
                .header("content-type", "application/json")
                .header("fluree-ledger", "test:main")
                .body(Body::from(insert_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // SPARQL query via connection (requires FROM clause)
    let sparql = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?name
        FROM <test:main>
        WHERE { ?s ex:name ?name }
    "#;
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query")
                .header("content-type", "application/sparql-query")
                .body(Body::from(sparql))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        json_contains_string(&json, "Alice"),
        "Expected SPARQL response to contain 'Alice', got: {}",
        json
    );
}

#[tokio::test]
async fn sparql_query_ledger_scoped_path_finds_value_without_from() {
    let (_tmp, state) = test_state();
    let app = build_router(state.clone());

    // Create
    let create_body = serde_json::json!({ "ledger": "test:main" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Insert
    let insert_body = serde_json::json!({
      "@context": { "ex": "http://example.org/" },
      "@id": "ex:alice",
      "ex:name": "Alice"
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/insert")
                .header("content-type", "application/json")
                .header("fluree-ledger", "test:main")
                .body(Body::from(insert_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // SPARQL query ledger-scoped via fluree-ledger header (FROM optional)
    let sparql = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?name
        WHERE { ?s ex:name ?name }
    "#;
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query/test:main")
                .header("content-type", "application/sparql-query")
                .body(Body::from(sparql))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        json_contains_string(&json, "Alice"),
        "Expected SPARQL response to contain 'Alice', got: {}",
        json
    );
}

#[tokio::test]
async fn sparql_update_on_query_endpoint_returns_bad_request() {
    let (_tmp, state) = test_state();
    let app = build_router(state);

    // SPARQL UPDATE requests should go to /v1/fluree/transact, not /v1/fluree/query.
    // The query endpoint returns 400 Bad Request with a helpful message.
    let sparql_update = r#"
        PREFIX ex: <http://example.org/>
        INSERT DATA { <http://example.org/alice> ex:name "Alice" }
    "#;
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query")
                .header("content-type", "application/sparql-update")
                .body(Body::from(sparql_update))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // Verify the error message guides users to the correct endpoint
    let (_, json) = json_body(resp).await;
    let error_msg = json["error"].as_str().unwrap_or("");
    assert!(
        error_msg.contains("/v1/fluree/transact"),
        "Expected error to mention /v1/fluree/transact endpoint, got: {}",
        error_msg
    );
}

#[tokio::test]
async fn sparql_query_generic_requires_from_clause_even_with_no_header() {
    let (_tmp, state) = test_state();
    let app = build_router(state.clone());

    // A SPARQL query without FROM on the generic endpoint should fail.
    // (Ledger-scoped /:ledger/query can omit FROM.)
    let sparql = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?name
        WHERE { ?s ex:name ?name }
    "#;
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query")
                .header("content-type", "application/sparql-query")
                .body(Body::from(sparql))
                .unwrap(),
        )
        .await
        .unwrap();

    // The API reports this as a query error; server maps it as 500 today.
    // If/when ApiError::query() is changed to a client error variant, this should become 400.
    assert!(
        resp.status() == StatusCode::INTERNAL_SERVER_ERROR
            || resp.status() == StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn soft_drop_blocks_recreate() {
    let (_tmp, state) = test_state();
    let app = build_router(state.clone());

    // Create
    let create_body = serde_json::json!({ "ledger": "test:main" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Soft drop
    let drop_body = serde_json::json!({ "ledger": "test:main", "hard": false });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/drop")
                .header("content-type", "application/json")
                .body(Body::from(drop_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Create again should conflict (must hard-drop to reuse alias)
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn query_missing_ledger_is_400() {
    let (_tmp, state) = test_state();
    let app = build_router(state);

    let query_body = serde_json::json!({
      "@context": { "ex": "http://example.org/" },
      "select": ["?name"],
      "where": { "@id": "?s", "ex:name": "?name" }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query")
                .header("content-type", "application/json")
                .body(Body::from(query_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn query_with_tracking_returns_headers() {
    let (_tmp, state) = test_state();
    let app = build_router(state.clone());

    // Create ledger
    let create_body = serde_json::json!({ "ledger": "test:tracking" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Insert data
    let insert_body = serde_json::json!({
        "@context": { "ex": "http://example.org/" },
        "@id": "ex:alice",
        "ex:name": "Alice"
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/insert")
                .header("content-type", "application/json")
                .header("fluree-ledger", "test:tracking")
                .body(Body::from(insert_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Query with tracking enabled (meta: true)
    let query_body = serde_json::json!({
        "@context": { "ex": "http://example.org/" },
        "from": "test:tracking",
        "select": ["?name"],
        "where": { "@id": "?s", "ex:name": "?name" },
        "opts": { "meta": true }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query")
                .header("content-type", "application/json")
                .body(Body::from(query_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    // Check tracking headers are present (Clojure parity)
    assert!(
        resp.headers().get("x-fdb-time").is_some(),
        "Response should have x-fdb-time header when tracking is enabled"
    );

    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    // Response body should also contain tracking info
    assert!(
        json.get("status").is_some(),
        "Response should have status field"
    );
    assert!(
        json.get("result").is_some(),
        "Response should have result field"
    );
    assert!(
        json.get("time").is_some(),
        "Response should have time field in body"
    );
    assert!(json_contains_string(&json, "Alice"));
}

#[tokio::test]
async fn query_with_max_fuel_returns_fuel_header() {
    let (_tmp, state) = test_state();
    let app = build_router(state.clone());

    // Create ledger
    let create_body = serde_json::json!({ "ledger": "test:fuel" });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Insert data
    let insert_body = serde_json::json!({
        "@context": { "ex": "http://example.org/" },
        "@id": "ex:bob",
        "ex:name": "Bob"
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/insert")
                .header("content-type", "application/json")
                .header("fluree-ledger", "test:fuel")
                .body(Body::from(insert_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Query with max-fuel AND meta:true to enable full tracking
    let query_body = serde_json::json!({
        "@context": { "ex": "http://example.org/" },
        "from": "test:fuel",
        "select": ["?name"],
        "where": { "@id": "?s", "ex:name": "?name" },
        "opts": { "max-fuel": 10000, "meta": true }
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/query")
                .header("content-type", "application/json")
                .body(Body::from(query_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    // Check fuel header is present
    assert!(
        resp.headers().get("x-fdb-fuel").is_some(),
        "Response should have x-fdb-fuel header when max-fuel is set"
    );
    // Time header should also be present
    assert!(
        resp.headers().get("x-fdb-time").is_some(),
        "Response should have x-fdb-time header"
    );

    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);

    // Response body should also contain fuel info
    assert!(
        json.get("fuel").is_some(),
        "Response should have fuel field in body"
    );
    assert!(json_contains_string(&json, "Bob"));
}

// ============================================================================
// Discovery endpoint: /.well-known/fluree.json
// ============================================================================

fn test_state_with_auth(
    events: EventsAuthMode,
    data: DataAuthMode,
    admin: AdminAuthMode,
) -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        events_auth_mode: events,
        data_auth_mode: data,
        admin_auth_mode: admin,
        // When auth is required, we need at least one trust source to pass validation.
        // Use insecure flags so tests don't need real keys.
        events_auth_insecure_accept_any_issuer: events != EventsAuthMode::None,
        data_auth_insecure_accept_any_issuer: data != DataAuthMode::None,
        admin_auth_insecure_accept_any_issuer: admin != AdminAuthMode::None,
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).expect("AppState::new"));
    (tmp, state)
}

async fn get_discovery(state: Arc<AppState>) -> (StatusCode, JsonValue) {
    let app = build_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/.well-known/fluree.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    json_body(resp).await
}

#[tokio::test]
async fn discovery_no_auth_omits_auth_block() {
    let (_tmp, state) = test_state(); // all auth modes None
    let (status, json) = get_discovery(state).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["version"], 1);
    assert!(
        json.get("auth").is_none(),
        "no auth block when all modes are None"
    );
}

#[tokio::test]
async fn discovery_data_auth_required_returns_token_type() {
    let (_tmp, state) = test_state_with_auth(
        EventsAuthMode::None,
        DataAuthMode::Required,
        AdminAuthMode::None,
    );
    let (status, json) = get_discovery(state).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["version"], 1);
    assert_eq!(json["auth"]["type"], "token");
}

#[tokio::test]
async fn discovery_events_auth_optional_returns_token_type() {
    let (_tmp, state) = test_state_with_auth(
        EventsAuthMode::Optional,
        DataAuthMode::None,
        AdminAuthMode::None,
    );
    let (status, json) = get_discovery(state).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["version"], 1);
    assert_eq!(json["auth"]["type"], "token");
}

#[tokio::test]
async fn discovery_admin_auth_required_returns_token_type() {
    let (_tmp, state) = test_state_with_auth(
        EventsAuthMode::None,
        DataAuthMode::None,
        AdminAuthMode::Required,
    );
    let (status, json) = get_discovery(state).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["version"], 1);
    assert_eq!(json["auth"]["type"], "token");
}
