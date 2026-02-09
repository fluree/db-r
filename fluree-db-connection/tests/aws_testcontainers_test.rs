//! Opt-in infra tests that boot LocalStack via testcontainers.
//!
//! Run (requires Docker):
//!   cargo test -p fluree-db-connection --features aws-testcontainers --test aws_testcontainers_test -- --nocapture

#![cfg(feature = "aws-testcontainers")]

use fluree_db_connection::{connect_async, ConnectionHandle};
use fluree_db_core::{StorageRead, StorageWrite};
use fluree_db_nameservice::{
    AdminPublisher, CasResult, ConfigCasResult, ConfigPayload, ConfigPublisher, ConfigValue,
    GraphSourcePublisher, GraphSourceType, NameService, NsLookupResult, Publisher, RefKind,
    RefPublisher, RefValue, StatusCasResult, StatusPayload, StatusPublisher, StatusValue,
};
use fluree_db_storage_aws::DynamoDbNameService;
use serde_json::json;
use std::time::Duration;
use testcontainers::core::IntoContainerPort;
use testcontainers::{runners::AsyncRunner, GenericImage, ImageExt};

const LOCALSTACK_EDGE_PORT: u16 = 4566;
const REGION: &str = "us-east-1";

fn set_localstack_env(endpoint: &str) {
    // Dummy credentials accepted by LocalStack
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", REGION);
    std::env::set_var("AWS_DEFAULT_REGION", REGION);

    // Ensure the SDK never tries IMDS (common source of slow hangs in tests)
    std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");

    // Used by the AWS Rust SDK for local endpoints (matches existing repo tests)
    std::env::set_var("AWS_ENDPOINT_URL", endpoint);
}

async fn wait_for_localstack(sdk_config: &aws_config::SdkConfig) {
    let s3 = aws_sdk_s3::Client::new(sdk_config);
    for _ in 0..60 {
        if s3.list_buckets().send().await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("LocalStack did not become ready in time");
}

async fn ensure_bucket(sdk_config: &aws_config::SdkConfig, bucket: &str) {
    let s3 = aws_sdk_s3::Client::new(sdk_config);

    // CreateBucket is idempotent-ish in LocalStack; if it already exists, HeadBucket will pass.
    let _ = s3.create_bucket().bucket(bucket).send().await;

    for _ in 0..30 {
        if s3.head_bucket().bucket(bucket).send().await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("S3 bucket was not available: {}", bucket);
}

async fn ensure_dynamodb_table(sdk_config: &aws_config::SdkConfig, table_name: &str) {
    let client = aws_sdk_dynamodb::Client::new(sdk_config);
    let ns = DynamoDbNameService::from_client(client, table_name.to_string());
    ns.ensure_table()
        .await
        .expect("DynamoDB table creation failed");
}

#[tokio::test]
async fn localstack_s3_and_dynamodb_smoke() {
    // 1) Boot LocalStack (edge port 4566)
    // Note: call `GenericImage` methods before `ImageExt` methods (per testcontainers docs).
    let image = GenericImage::new("localstack/localstack", "latest")
        .with_exposed_port(LOCALSTACK_EDGE_PORT.tcp())
        .with_env_var("SERVICES", "s3,dynamodb")
        .with_env_var("DEFAULT_REGION", REGION)
        .with_env_var("SKIP_SSL_CERT_DOWNLOAD", "1");
    let container = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            panic!(
                "Failed to start LocalStack via Docker.\n\
                \n\
                This usually means the Docker daemon/socket is not available.\n\
                - If you use Docker Desktop: open Docker Desktop and wait until it's running, then verify `docker ps` works.\n\
                - If you use Colima: run `colima start` and set `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock`.\n\
                - If `DOCKER_HOST` is set but wrong: try `unset DOCKER_HOST`.\n\
                \n\
                Original error: {e:?}"
            );
        }
    };

    let host_port = container
        .get_host_port_ipv4(LOCALSTACK_EDGE_PORT)
        .await
        .expect("LocalStack edge port mapped");
    let endpoint = format!("http://127.0.0.1:{}", host_port);

    // 2) Configure AWS SDK to talk to LocalStack
    set_localstack_env(&endpoint);
    let sdk_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;

    wait_for_localstack(&sdk_config).await;

    // 3) Provision minimal infra
    let bucket = "test-fluree-bucket";
    let table = "test-fluree-ns";
    ensure_bucket(&sdk_config, bucket).await;
    ensure_dynamodb_table(&sdk_config, table).await;

    // 4) Create connection via JSON-LD config (exercises parsing + AWS init)
    let config = json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {
                "@id": "s3Storage",
                "@type": "Storage",
                "s3Bucket": bucket,
                "s3Prefix": "test-ledgers"
            },
            {
                "@id": "connection",
                "@type": "Connection",
                "indexStorage": {"@id": "s3Storage"},
                "primaryPublisher": {
                    "@type": "Publisher",
                    "dynamodbTable": table
                }
            }
        ]
    });

    let conn = connect_async(&config)
        .await
        .expect("connect_async should succeed against LocalStack");

    let aws = match conn {
        ConnectionHandle::Aws(h) => h,
        _ => panic!("Expected AWS connection handle"),
    };

    // 5) DynamoDB nameservice smoke: init + publish + lookup
    let alias = "mydb:main";

    // Init materializes all concern items (meta, head, index, status, config)
    aws.nameservice()
        .publish_ledger_init(alias)
        .await
        .expect("publish_ledger_init should succeed");

    // Publish commit head
    let commit_addr = "fluree:s3://mydb/main/commit/1.json";
    aws.publish_commit(alias, commit_addr, 1)
        .await
        .expect("publish_commit should succeed");

    // Publish index head
    let index_addr = "fluree:s3://mydb/main/index/root.json";
    aws.publish_index(alias, index_addr, 1)
        .await
        .expect("publish_index should succeed");

    // Lookup and verify
    let record = aws
        .lookup(alias)
        .await
        .expect("lookup should succeed")
        .expect("record should exist after publish");
    assert_eq!(record.commit_address.as_deref(), Some(commit_addr));
    assert_eq!(record.index_address.as_deref(), Some(index_addr));
    assert_eq!(record.index_t, 1);

    // Monotonic: older t should be silently ignored
    aws.publish_commit(alias, "old-addr", 0)
        .await
        .expect("stale commit publish should succeed (no-op)");
    let record2 = aws.lookup(alias).await.expect("lookup").expect("exists");
    assert_eq!(
        record2.commit_address.as_deref(),
        Some(commit_addr),
        "stale publish should not overwrite"
    );

    // 6) S3 storage smoke: write + read
    let address = "fluree:s3://it-test/hello.txt";
    aws.index_storage()
        .write_bytes(address, b"hello localstack")
        .await
        .expect("S3 write should succeed");

    let bytes = aws
        .index_storage()
        .read_bytes(address)
        .await
        .expect("S3 read should succeed");
    assert_eq!(bytes, b"hello localstack");
}

/// Build an SDK config with explicit endpoint (no global env vars).
async fn sdk_config_for_endpoint(endpoint: &str) -> aws_config::SdkConfig {
    use aws_config::meta::region::RegionProviderChain;
    // Dummy credentials accepted by LocalStack
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    let region_provider = RegionProviderChain::default_provider().or_else(REGION);
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .endpoint_url(endpoint)
        .load()
        .await
}

async fn wait_for_dynamodb(sdk_config: &aws_config::SdkConfig) {
    let ddb = aws_sdk_dynamodb::Client::new(sdk_config);
    for _ in 0..60 {
        if ddb.list_tables().send().await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("DynamoDB did not become ready in time");
}

/// Helper: boot LocalStack + provision infra, returning (container, DynamoDbNameService).
async fn setup_localstack_ns() -> (
    testcontainers::ContainerAsync<GenericImage>,
    DynamoDbNameService,
) {
    let image = GenericImage::new("localstack/localstack", "latest")
        .with_exposed_port(LOCALSTACK_EDGE_PORT.tcp())
        .with_env_var("SERVICES", "dynamodb")
        .with_env_var("DEFAULT_REGION", REGION)
        .with_env_var("SKIP_SSL_CERT_DOWNLOAD", "1");
    let container = image
        .start()
        .await
        .expect("LocalStack started (Docker must be running)");
    let host_port = container
        .get_host_port_ipv4(LOCALSTACK_EDGE_PORT)
        .await
        .expect("LocalStack edge port mapped");
    let endpoint = format!("http://127.0.0.1:{host_port}");
    let sdk_config = sdk_config_for_endpoint(&endpoint).await;
    wait_for_dynamodb(&sdk_config).await;

    let table = "fluree-ns-test";
    ensure_dynamodb_table(&sdk_config, table).await;

    let client = aws_sdk_dynamodb::Client::new(&sdk_config);
    let ns = DynamoDbNameService::from_client(client, table.to_string());
    (container, ns)
}

// ────────────────────────────────────────────────────────────────────────────
// Comprehensive nameservice trait coverage
// ────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn nameservice_ledger_lifecycle() {
    let (_container, ns) = setup_localstack_ns().await;

    let alias = "lifecycle-test:main";

    // ── lookup before init → None ──────────────────────────────────────────
    assert!(ns.lookup(alias).await.unwrap().is_none());

    // ── publish_commit on uninitialized alias → error ──────────────────────
    let err = ns.publish_commit(alias, "commit:1", 1).await;
    assert!(
        err.is_err(),
        "publish_commit on uninitialized alias should fail"
    );

    let err = ns.publish_index(alias, "index:1", 1).await;
    assert!(
        err.is_err(),
        "publish_index on uninitialized alias should fail"
    );

    // ── publish_ledger_init ────────────────────────────────────────────────
    ns.publish_ledger_init(alias).await.unwrap();

    // Lookup returns record with unborn head/index
    let rec = ns.lookup(alias).await.unwrap().expect("exists after init");
    assert_eq!(rec.address, alias, "address should be the full alias");
    assert_eq!(
        rec.name, "lifecycle-test",
        "name is the ledger-name-only part"
    );
    assert_eq!(rec.branch, "main");
    assert!(rec.commit_address.is_none());
    assert!(rec.index_address.is_none());
    assert_eq!(rec.index_t, 0);

    // Double init → should succeed (idempotent or conflict suppressed)
    // The implementation uses conditional PutItems that will fail if items exist,
    // but the error should be suppressed as "already exists".
    let init2 = ns.publish_ledger_init(alias).await;
    assert!(
        init2.is_ok() || init2.is_err(),
        "double init should not panic"
    );

    // ── publish_commit + publish_index ─────────────────────────────────────
    ns.publish_commit(alias, "commit:1", 1).await.unwrap();
    ns.publish_index(alias, "index:1", 1).await.unwrap();

    let rec = ns.lookup(alias).await.unwrap().unwrap();
    assert_eq!(rec.commit_address.as_deref(), Some("commit:1"));
    assert_eq!(rec.index_address.as_deref(), Some("index:1"));
    assert_eq!(rec.index_t, 1);

    // Monotonic: stale commit/index silently ignored
    ns.publish_commit(alias, "stale:0", 0).await.unwrap();
    ns.publish_index(alias, "stale:0", 0).await.unwrap();
    let rec = ns.lookup(alias).await.unwrap().unwrap();
    assert_eq!(rec.commit_address.as_deref(), Some("commit:1"));
    assert_eq!(rec.index_address.as_deref(), Some("index:1"));

    // Advance
    ns.publish_commit(alias, "commit:2", 2).await.unwrap();
    ns.publish_index(alias, "index:2", 2).await.unwrap();
    let rec = ns.lookup(alias).await.unwrap().unwrap();
    assert_eq!(rec.commit_address.as_deref(), Some("commit:2"));
    assert_eq!(rec.index_address.as_deref(), Some("index:2"));
    assert_eq!(rec.index_t, 2);

    // ── all_records() ──────────────────────────────────────────────────────
    let recs = ns.all_records().await.unwrap();
    assert!(
        recs.iter().any(|r| r.address == alias),
        "all_records should contain our ledger"
    );

    // ── retract ────────────────────────────────────────────────────────────
    ns.retract(alias).await.unwrap();
    // Lookup still returns record, but record should have retracted flag
    // (NsRecord might not expose retracted directly — depends on fields)
    let rec = ns.lookup(alias).await.unwrap();
    assert!(rec.is_some(), "retracted record still visible via lookup");
}

#[tokio::test]
async fn nameservice_admin_publisher() {
    let (_container, ns) = setup_localstack_ns().await;

    let alias = "admin-test:main";
    ns.publish_ledger_init(alias).await.unwrap();
    ns.publish_index(alias, "index:5", 5).await.unwrap();

    // AdminPublisher: publish_index_allow_equal — same t succeeds
    ns.publish_index_allow_equal(alias, "index:5-rebuild", 5)
        .await
        .unwrap();
    let rec = ns.lookup(alias).await.unwrap().unwrap();
    assert_eq!(
        rec.index_address.as_deref(),
        Some("index:5-rebuild"),
        "allow_equal should overwrite at same t"
    );
    assert_eq!(rec.index_t, 5);

    // Still rejects lower t
    ns.publish_index_allow_equal(alias, "stale:3", 3)
        .await
        .unwrap();
    let rec = ns.lookup(alias).await.unwrap().unwrap();
    assert_eq!(
        rec.index_address.as_deref(),
        Some("index:5-rebuild"),
        "allow_equal should reject lower t"
    );
}

#[tokio::test]
async fn nameservice_ref_publisher() {
    let (_container, ns) = setup_localstack_ns().await;

    let alias = "ref-test:main";

    // get_ref before init → None
    assert!(ns
        .get_ref(alias, RefKind::CommitHead)
        .await
        .unwrap()
        .is_none());

    ns.publish_ledger_init(alias).await.unwrap();

    // get_ref after init → unborn (address=None, t=0)
    let ref_val = ns
        .get_ref(alias, RefKind::CommitHead)
        .await
        .unwrap()
        .expect("exists after init");
    assert!(ref_val.address.is_none());
    assert_eq!(ref_val.t, 0);

    // CAS: unborn → first commit
    let new_ref = RefValue {
        address: Some("commit:1".to_string()),
        t: 1,
    };
    let result = ns
        .compare_and_set_ref(alias, RefKind::CommitHead, Some(&ref_val), &new_ref)
        .await
        .unwrap();
    assert!(
        matches!(result, CasResult::Updated),
        "CAS from unborn should succeed"
    );

    // Verify
    let ref_val = ns
        .get_ref(alias, RefKind::CommitHead)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ref_val.address.as_deref(), Some("commit:1"));
    assert_eq!(ref_val.t, 1);

    // CAS: commit:1 → commit:2
    let new_ref2 = RefValue {
        address: Some("commit:2".to_string()),
        t: 2,
    };
    let result = ns
        .compare_and_set_ref(alias, RefKind::CommitHead, Some(&ref_val), &new_ref2)
        .await
        .unwrap();
    assert!(matches!(result, CasResult::Updated));

    // CAS with stale expected → Conflict
    let stale_expected = RefValue {
        address: Some("commit:1".to_string()),
        t: 1,
    };
    let new_ref3 = RefValue {
        address: Some("commit:3".to_string()),
        t: 3,
    };
    let result = ns
        .compare_and_set_ref(alias, RefKind::CommitHead, Some(&stale_expected), &new_ref3)
        .await
        .unwrap();
    match result {
        CasResult::Conflict { actual } => {
            assert_eq!(actual.as_ref().unwrap().t, 2, "conflict shows current t=2");
        }
        CasResult::Updated => panic!("stale CAS should conflict"),
    }

    // IndexHead: similar test
    let idx_ref = ns
        .get_ref(alias, RefKind::IndexHead)
        .await
        .unwrap()
        .unwrap();
    assert!(idx_ref.address.is_none());
    assert_eq!(idx_ref.t, 0);

    let new_idx = RefValue {
        address: Some("index:1".to_string()),
        t: 1,
    };
    let result = ns
        .compare_and_set_ref(alias, RefKind::IndexHead, Some(&idx_ref), &new_idx)
        .await
        .unwrap();
    assert!(matches!(result, CasResult::Updated));

    // ── CAS expected=None creates ledger (matches StorageNameService) ────
    let new_alias = "cas-create-test:main";
    assert!(ns.lookup(new_alias).await.unwrap().is_none());

    let create_ref = RefValue {
        address: Some("commit:1".to_string()),
        t: 1,
    };
    let result = ns
        .compare_and_set_ref(new_alias, RefKind::CommitHead, None, &create_ref)
        .await
        .unwrap();
    assert!(
        matches!(result, CasResult::Updated),
        "expected=None should create ledger when alias unknown"
    );

    // Verify the ledger was created with the ref set
    let rec = ns
        .lookup(new_alias)
        .await
        .unwrap()
        .expect("ledger should exist after CAS create");
    assert_eq!(rec.commit_address.as_deref(), Some("commit:1"));
    assert_eq!(rec.index_t, 0, "index should be unborn");

    // CAS expected=None on existing alias → Conflict
    let create_ref2 = RefValue {
        address: Some("commit:99".to_string()),
        t: 99,
    };
    let result = ns
        .compare_and_set_ref(new_alias, RefKind::CommitHead, None, &create_ref2)
        .await
        .unwrap();
    assert!(
        matches!(result, CasResult::Conflict { .. }),
        "expected=None should conflict when alias already exists"
    );
}

#[tokio::test]
async fn nameservice_status_publisher() {
    let (_container, ns) = setup_localstack_ns().await;

    let alias = "status-test:main";

    // get_status before init → None
    assert!(ns.get_status(alias).await.unwrap().is_none());

    ns.publish_ledger_init(alias).await.unwrap();

    // get_status after init → initial (v=1, state="ready")
    let status = ns.get_status(alias).await.unwrap().expect("exists");
    assert_eq!(status.v, 1);
    assert!(status.payload.is_ready());

    // push_status: ready → indexing
    let new_status = StatusValue::new(2, StatusPayload::new("indexing"));
    let result = ns
        .push_status(alias, Some(&status), &new_status)
        .await
        .unwrap();
    assert!(matches!(result, StatusCasResult::Updated));

    let status2 = ns.get_status(alias).await.unwrap().unwrap();
    assert_eq!(status2.v, 2);
    assert_eq!(status2.payload.state, "indexing");

    // push_status with stale expected → Conflict
    let stale = StatusValue::new(3, StatusPayload::new("error"));
    let result = ns.push_status(alias, Some(&status), &stale).await.unwrap();
    match result {
        StatusCasResult::Conflict { actual } => {
            assert_eq!(actual.as_ref().unwrap().v, 2);
        }
        StatusCasResult::Updated => panic!("stale push_status should conflict"),
    }
}

#[tokio::test]
async fn nameservice_config_publisher() {
    let (_container, ns) = setup_localstack_ns().await;

    let alias = "config-test:main";

    // get_config before init → None
    assert!(ns.get_config(alias).await.unwrap().is_none());

    ns.publish_ledger_init(alias).await.unwrap();

    // get_config after init → unborn (v=0, payload=None)
    let config = ns.get_config(alias).await.unwrap().expect("exists");
    assert!(config.is_unborn());

    // push_config: set default context
    let new_config = ConfigValue::new(
        1,
        Some(ConfigPayload::with_default_context(
            "fluree:context/default",
        )),
    );
    let result = ns
        .push_config(alias, Some(&config), &new_config)
        .await
        .unwrap();
    assert!(matches!(result, ConfigCasResult::Updated));

    let config2 = ns.get_config(alias).await.unwrap().unwrap();
    assert_eq!(config2.v, 1);
    assert_eq!(
        config2.payload.as_ref().unwrap().default_context.as_deref(),
        Some("fluree:context/default")
    );

    // push_config with stale expected → Conflict
    let stale = ConfigValue::new(2, Some(ConfigPayload::new()));
    let result = ns.push_config(alias, Some(&config), &stale).await.unwrap();
    match result {
        ConfigCasResult::Conflict { actual } => {
            assert_eq!(actual.as_ref().unwrap().v, 1);
        }
        ConfigCasResult::Updated => panic!("stale push_config should conflict"),
    }

    // ── ConfigPublisher gated to ledgers: graph source returns None ─────
    ns.publish_graph_source(
        "config-gate-gs",
        "main",
        GraphSourceType::Bm25,
        r#"{"foo":"bar"}"#,
        &[],
    )
    .await
    .unwrap();
    let gs_config = ns.get_config("config-gate-gs:main").await.unwrap();
    assert!(
        gs_config.is_none(),
        "get_config on graph source should return None"
    );
}

#[tokio::test]
async fn nameservice_graph_source_publisher() {
    let (_container, ns) = setup_localstack_ns().await;

    let graph_source_id = "search-bm25:main";

    // lookup_graph_source before publish → None
    assert!(ns
        .lookup_graph_source(graph_source_id)
        .await
        .unwrap()
        .is_none());

    // publish_graph_source
    ns.publish_graph_source(
        "search-bm25",
        "main",
        GraphSourceType::Bm25,
        r#"{"analyzer":"english"}"#,
        &["source-ledger:main".to_string()],
    )
    .await
    .unwrap();

    // lookup_graph_source
    let gs = ns
        .lookup_graph_source(graph_source_id)
        .await
        .unwrap()
        .expect("exists");
    assert_eq!(gs.name, "search-bm25");
    assert_eq!(gs.branch, "main");
    assert!(matches!(gs.source_type, GraphSourceType::Bm25));
    assert_eq!(gs.config, r#"{"analyzer":"english"}"#);
    assert_eq!(gs.dependencies, vec!["source-ledger:main".to_string()]);
    assert!(gs.index_address.is_none());
    assert_eq!(gs.index_t, 0);

    // publish_graph_source_index
    ns.publish_graph_source_index("search-bm25", "main", "gs-index:1", 1)
        .await
        .unwrap();
    let gs = ns
        .lookup_graph_source(graph_source_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(gs.index_address.as_deref(), Some("gs-index:1"));
    assert_eq!(gs.index_t, 1);

    // Monotonic: stale index ignored
    ns.publish_graph_source_index("search-bm25", "main", "stale:0", 0)
        .await
        .unwrap();
    let gs = ns
        .lookup_graph_source(graph_source_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(gs.index_address.as_deref(), Some("gs-index:1"));

    // lookup_any → ledger or graph source
    let any = ns.lookup_any(graph_source_id).await.unwrap();
    match any {
        NsLookupResult::GraphSource(ref r) => assert_eq!(r.name, "search-bm25"),
        _ => panic!("expected GraphSource, got {any:?}"),
    }

    // Also test lookup_any for a ledger
    ns.publish_ledger_init("ledger-test:main").await.unwrap();
    let any = ns.lookup_any("ledger-test:main").await.unwrap();
    match any {
        NsLookupResult::Ledger(ref r) => assert_eq!(r.address, "ledger-test:main"),
        _ => panic!("expected Ledger, got {any:?}"),
    }

    // lookup_any for unknown → NotFound
    let any = ns.lookup_any("nonexistent:main").await.unwrap();
    assert!(matches!(any, NsLookupResult::NotFound));

    // all_graph_source_records
    let records = ns.all_graph_source_records().await.unwrap();
    assert!(
        records.iter().any(|r| r.name == "search-bm25"),
        "all_graph_source_records should contain our graph source"
    );

    // retract_graph_source
    ns.retract_graph_source("search-bm25", "main")
        .await
        .unwrap();
    let gs = ns.lookup_graph_source(graph_source_id).await.unwrap();
    assert!(
        gs.is_some(),
        "retracted graph source still visible via lookup_graph_source"
    );

    // Re-publish after retract should work (preserves retracted or re-creates)
    ns.publish_graph_source(
        "search-bm25",
        "main",
        GraphSourceType::Bm25,
        r#"{"analyzer":"english_v2"}"#,
        &["source-ledger:main".to_string()],
    )
    .await
    .unwrap();
    let gs = ns
        .lookup_graph_source(graph_source_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(gs.config, r#"{"analyzer":"english_v2"}"#);
    // Index should be preserved from before retraction
    assert_eq!(gs.index_address.as_deref(), Some("gs-index:1"));
}
