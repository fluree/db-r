use axum::body::Body;
use fluree_db_api::{ExportCommitsResponse, PushCommitsRequest};
use fluree_db_core::{ContentId, ContentKind, Flake, FlakeMeta, FlakeValue, Sid};
use fluree_db_novelty::{Commit, CommitRef};
use fluree_db_server::config::{AdminAuthMode, DataAuthMode, EventsAuthMode};
use fluree_db_server::{routes::build_router, AppState, ServerConfig, TelemetryConfig};
use fluree_vocab::namespaces::{FLUREE_DB, XSD};
use fluree_vocab::xsd_names;
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

fn make_commit_bytes(t: i64, previous: Option<&str>, flakes: Vec<Flake>) -> Vec<u8> {
    let mut c = Commit::new(t, flakes);
    if let Some(prev) = previous {
        let prev_cid = ContentId::new(ContentKind::Commit, prev.as_bytes());
        c = c.with_previous_ref(CommitRef::new(prev_cid));
    }
    let res = fluree_db_novelty::commit_v2::write_commit(&c, true, None).expect("write_commit");
    res.bytes
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
async fn push_endpoint_accepts_single_commit_and_advances_head() {
    let (_tmp, state) = test_state();
    let app = build_router(state.clone());

    // Create empty ledger.
    let create_body = serde_json::json!({ "ledger": "push:main" });
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
    let (status, _json) = json_body(resp).await;
    assert_eq!(status, StatusCode::CREATED);

    // Push commit t=1 (no previous).
    let s = Sid::new(FLUREE_DB, "alice");
    let p = Sid::new(FLUREE_DB, "name");
    let o = FlakeValue::String("Alice".to_string());
    let dt = Sid::new(XSD, xsd_names::STRING);
    let flakes = vec![Flake::new(s, p, o, dt, 1, true, None)];
    let bytes = make_commit_bytes(1, None, flakes);
    let push_req = PushCommitsRequest {
        commits: vec![fluree_db_api::Base64Bytes(bytes)],
        blobs: std::collections::HashMap::new(),
    };

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/push/push:main")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&push_req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json.get("accepted").and_then(|v| v.as_u64()), Some(1));
    assert_eq!(json.pointer("/head/t").and_then(|v| v.as_i64()), Some(1));
    let head_addr = json
        .pointer("/head/address")
        .and_then(|v| v.as_str())
        .expect("head.address")
        .to_string();

    // Re-pushing the same commit should now be rejected (next-t mismatch).
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/push/push:main")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&push_req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CONFLICT,
        "expected conflict when re-pushing commit already applied (head={})",
        head_addr
    );
}

#[tokio::test]
async fn push_rejects_first_commit_t_mismatch_with_409() {
    let (_tmp, state) = test_state();
    let app = build_router(state.clone());

    let create_body = serde_json::json!({ "ledger": "push-t:main" });
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

    let s = Sid::new(FLUREE_DB, "alice");
    let p = Sid::new(FLUREE_DB, "name");
    let o = FlakeValue::String("Alice".to_string());
    let dt = Sid::new(XSD, xsd_names::STRING);
    let flakes = vec![Flake::new(s, p, o, dt, 2, true, None)];
    let bytes = make_commit_bytes(2, None, flakes);
    let push_req = PushCommitsRequest {
        commits: vec![fluree_db_api::Base64Bytes(bytes)],
        blobs: std::collections::HashMap::new(),
    };

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/push/push-t:main")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&push_req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn push_rejects_retraction_without_existing_assertion_with_422() {
    let (_tmp, state) = test_state();
    let app = build_router(state.clone());

    let create_body = serde_json::json!({ "ledger": "push-ret:main" });
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

    let s = Sid::new(FLUREE_DB, "alice");
    let p = Sid::new(FLUREE_DB, "name");
    let o = FlakeValue::String("Alice".to_string());
    let dt = Sid::new(XSD, xsd_names::STRING);
    let flakes = vec![Flake::new(s, p, o, dt, 1, false, None)];
    let bytes = make_commit_bytes(1, None, flakes);
    let push_req = PushCommitsRequest {
        commits: vec![fluree_db_api::Base64Bytes(bytes)],
        blobs: std::collections::HashMap::new(),
    };

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/push/push-ret:main")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&push_req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(json_contains_string(&json, "retraction invariant"));
}

#[tokio::test]
async fn push_rejects_list_retraction_missing_meta_with_422() {
    let (_tmp, state) = test_state();
    let app = build_router(state.clone());

    // Create empty ledger.
    let create_body = serde_json::json!({ "ledger": "push-list:main" });
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

    // Push commit t=1 asserting a list item with index meta.
    let s = Sid::new(FLUREE_DB, "alice");
    let p = Sid::new(FLUREE_DB, "tags");
    let o = FlakeValue::String("one".to_string());
    let dt = Sid::new(XSD, xsd_names::STRING);
    let flakes = vec![Flake::new(
        s.clone(),
        p.clone(),
        o.clone(),
        dt.clone(),
        1,
        true,
        Some(FlakeMeta::with_index(0)),
    )];
    let bytes = make_commit_bytes(1, None, flakes);
    let push_req = PushCommitsRequest {
        commits: vec![fluree_db_api::Base64Bytes(bytes)],
        blobs: std::collections::HashMap::new(),
    };
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/push/push-list:main")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&push_req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::OK);
    let head_addr = json
        .pointer("/head/address")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();

    // Push commit t=2 attempting to retract the value but WITHOUT list meta.
    let retract = Flake::new(s, p, o, dt, 2, false, None);
    let bytes = make_commit_bytes(2, Some(&head_addr), vec![retract]);
    let push_req = PushCommitsRequest {
        commits: vec![fluree_db_api::Base64Bytes(bytes)],
        blobs: std::collections::HashMap::new(),
    };
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/push/push-list:main")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&push_req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, json) = json_body(resp).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(json_contains_string(&json, "retraction invariant"));
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

// ============================================================================
// Commits export endpoint tests
// ============================================================================

/// JWS token helpers for storage proxy auth in tests.
mod storage_auth {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use ed25519_dalek::Signer;
    use serde_json::json;

    /// Generate a signing key and did:key for testing.
    pub fn test_key() -> (ed25519_dalek::SigningKey, String) {
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&[42u8; 32]);
        let did = fluree_db_credential::did_from_pubkey(&signing_key.verifying_key().to_bytes());
        (signing_key, did)
    }

    /// Create a JWS token (embedded JWK) with custom claims.
    pub fn create_jws(claims: &serde_json::Value, key: &ed25519_dalek::SigningKey) -> String {
        let pubkey = key.verifying_key().to_bytes();
        let pubkey_b64 = URL_SAFE_NO_PAD.encode(pubkey);

        let header = json!({
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
        let signature = key.sign(signing_input.as_bytes());
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

        format!("{}.{}.{}", header_b64, payload_b64, sig_b64)
    }

    pub fn now_secs() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    /// Create a storage-all token valid for 1 hour.
    pub fn storage_all_token() -> String {
        let (key, did) = test_key();
        let claims = json!({
            "iss": did,
            "sub": "test-peer",
            "exp": now_secs() + 3600,
            "iat": now_secs(),
            "fluree.storage.all": true
        });
        create_jws(&claims, &key)
    }
}

fn test_state_with_storage_proxy() -> (TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let cfg = ServerConfig {
        cors_enabled: false,
        indexing_enabled: false,
        storage_path: Some(tmp.path().to_path_buf()),
        storage_proxy_enabled: true,
        storage_proxy_insecure_accept_any_issuer: true,
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&cfg);
    let state = Arc::new(AppState::new(cfg, telemetry).expect("AppState::new"));
    (tmp, state)
}

/// Helper: create a ledger and push N commits, returning head address after each push.
async fn create_and_push_commits(app: &axum::Router, ledger: &str, n: usize) -> Vec<(String, i64)> {
    // Create ledger.
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

    let s = Sid::new(FLUREE_DB, "entity");
    let p = Sid::new(FLUREE_DB, "seq");
    let dt = Sid::new(XSD, xsd_names::INTEGER);

    let mut heads: Vec<(String, i64)> = Vec::new();
    let mut prev_addr: Option<String> = None;

    for i in 1..=n {
        let t = i as i64;
        let o = FlakeValue::Long(t);
        let flakes = vec![Flake::new(
            s.clone(),
            p.clone(),
            o,
            dt.clone(),
            t,
            true,
            None,
        )];
        let bytes = make_commit_bytes(t, prev_addr.as_deref(), flakes);
        let push_req = PushCommitsRequest {
            commits: vec![fluree_db_api::Base64Bytes(bytes)],
            blobs: std::collections::HashMap::new(),
        };

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/fluree/push/{}", ledger))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&push_req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, json) = json_body(resp).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "push commit t={} failed: {}",
            t,
            json
        );

        let addr = json
            .pointer("/head/address")
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();
        heads.push((addr.clone(), t));
        prev_addr = Some(addr);
    }

    heads
}

/// Helper: GET /v1/fluree/commits/{ledger} with auth and optional query params.
async fn fetch_commits(
    app: &axum::Router,
    ledger: &str,
    cursor: Option<&str>,
    limit: Option<usize>,
) -> (StatusCode, JsonValue) {
    let token = storage_auth::storage_all_token();
    let mut uri = format!("/v1/fluree/commits/{}", ledger);
    let mut params = Vec::new();
    if let Some(c) = cursor {
        params.push(format!("cursor={}", urlencoding::encode(c)));
    }
    if let Some(l) = limit {
        params.push(format!("limit={}", l));
    }
    if !params.is_empty() {
        uri.push('?');
        uri.push_str(&params.join("&"));
    }

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&uri)
                .header("authorization", format!("Bearer {}", token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    json_body(resp).await
}

#[tokio::test]
async fn commits_endpoint_returns_paginated_commits() {
    let (_tmp, state) = test_state_with_storage_proxy();
    let app = build_router(state);

    // Push 5 commits.
    create_and_push_commits(&app, "export:main", 5).await;

    // Page 1: limit=2 → 2 commits (t=5, t=4).
    let (status, json) = fetch_commits(&app, "export:main", None, Some(2)).await;
    assert_eq!(status, StatusCode::OK, "page 1 failed: {}", json);
    let page1: ExportCommitsResponse = serde_json::from_value(json).expect("parse page 1");
    assert_eq!(page1.count, 2);
    assert_eq!(page1.newest_t, 5);
    assert_eq!(page1.oldest_t, 4);
    assert_eq!(page1.head_t, 5);
    assert_eq!(page1.effective_limit, 2);
    assert!(page1.next_cursor.is_some(), "should have next page");

    // Page 2: cursor from page 1 → 2 commits (t=3, t=2).
    let cursor = page1.next_cursor.as_deref().unwrap();
    let (status, json) = fetch_commits(&app, "export:main", Some(cursor), Some(2)).await;
    assert_eq!(status, StatusCode::OK, "page 2 failed: {}", json);
    let page2: ExportCommitsResponse = serde_json::from_value(json).expect("parse page 2");
    assert_eq!(page2.count, 2);
    assert_eq!(page2.newest_t, 3);
    assert_eq!(page2.oldest_t, 2);
    assert!(page2.next_cursor.is_some(), "should have page 3");

    // Page 3: cursor from page 2 → 1 commit (t=1, genesis).
    let cursor = page2.next_cursor.as_deref().unwrap();
    let (status, json) = fetch_commits(&app, "export:main", Some(cursor), Some(2)).await;
    assert_eq!(status, StatusCode::OK, "page 3 failed: {}", json);
    let page3: ExportCommitsResponse = serde_json::from_value(json).expect("parse page 3");
    assert_eq!(page3.count, 1);
    assert_eq!(page3.newest_t, 1);
    assert_eq!(page3.oldest_t, 1);
    assert!(
        page3.next_cursor.is_none(),
        "genesis reached, no more pages"
    );
}

#[tokio::test]
async fn commits_endpoint_cursor_stability() {
    let (_tmp, state) = test_state_with_storage_proxy();
    let app = build_router(state);

    // Push 4 commits.
    create_and_push_commits(&app, "cursor:main", 4).await;

    // Page 1: limit=2 → (t=4, t=3).
    let (status, json) = fetch_commits(&app, "cursor:main", None, Some(2)).await;
    assert_eq!(status, StatusCode::OK);
    let page1: ExportCommitsResponse = serde_json::from_value(json).expect("parse page 1");
    assert_eq!(page1.newest_t, 4);
    let cursor = page1.next_cursor.clone().expect("need cursor for page 2");

    // Push a NEW commit (t=5) between page fetches.
    let s = Sid::new(FLUREE_DB, "entity");
    let p = Sid::new(FLUREE_DB, "seq");
    let o = FlakeValue::Long(5);
    let dt = Sid::new(XSD, xsd_names::INTEGER);
    // Need head address of t=4 for previous ref.
    let head_addr = page1.head_address.clone();
    let bytes = make_commit_bytes(
        5,
        Some(&head_addr),
        vec![Flake::new(s, p, o, dt, 5, true, None)],
    );
    let push_req = PushCommitsRequest {
        commits: vec![fluree_db_api::Base64Bytes(bytes)],
        blobs: std::collections::HashMap::new(),
    };
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/fluree/push/cursor:main")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&push_req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "push t=5 should succeed");

    // Page 2 with old cursor: should still see (t=2, t=1) — unaffected by new head.
    let (status, json) = fetch_commits(&app, "cursor:main", Some(&cursor), Some(2)).await;
    assert_eq!(status, StatusCode::OK);
    let page2: ExportCommitsResponse = serde_json::from_value(json).expect("parse page 2");
    assert_eq!(page2.newest_t, 2, "cursor should resume at t=2");
    assert_eq!(page2.oldest_t, 1, "should reach genesis");
    assert!(page2.next_cursor.is_none(), "genesis reached");
}

#[tokio::test]
async fn commits_endpoint_rejects_without_storage_proxy() {
    // Default test state has storage proxy DISABLED.
    let (_tmp, state) = test_state();
    let app = build_router(state);

    // Create a ledger so there's something to query.
    let create_body = serde_json::json!({ "ledger": "noauth:main" });
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

    // Without storage proxy enabled → 404 "Storage proxy not enabled".
    let token = storage_auth::storage_all_token();
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/commits/noauth:main")
                .header("authorization", format!("Bearer {}", token))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // Without token AND without storage proxy → also 404.
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/commits/noauth:main")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn commits_endpoint_returns_effective_limit() {
    let (_tmp, state) = test_state_with_storage_proxy();
    let app = build_router(state);

    // Push 2 commits.
    create_and_push_commits(&app, "limit:main", 2).await;

    // Request limit=9999 → effective_limit should be clamped to server max (500).
    let (status, json) = fetch_commits(&app, "limit:main", None, Some(9999)).await;
    assert_eq!(status, StatusCode::OK);
    let resp: ExportCommitsResponse = serde_json::from_value(json).expect("parse response");
    assert_eq!(resp.effective_limit, 500, "server should clamp to max 500");
    assert_eq!(resp.count, 2, "only 2 commits exist");
}

#[tokio::test]
async fn commits_endpoint_without_token_returns_401() {
    let (_tmp, state) = test_state_with_storage_proxy();
    let app = build_router(state);

    // Storage proxy enabled but no token → 401.
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/fluree/commits/any:main")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "missing token should return 401 when storage proxy is enabled"
    );
}
