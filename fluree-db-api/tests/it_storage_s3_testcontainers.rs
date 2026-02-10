//! S3 storage integration tests using testcontainers + LocalStack (Clojure parity).
//!
//! Ports from:
//! - `db/test/fluree/db/storage/s3_testcontainers_test.clj`
//!
//! Run (requires Docker):
//!   cargo test -p fluree-db-api --features aws-testcontainers --test it_storage_s3_testcontainers -- --nocapture

#![cfg(feature = "aws-testcontainers")]

mod support;

use aws_config::meta::region::RegionProviderChain;
use fluree_db_api::{tx, Fluree};
use fluree_db_connection::{Connection, ConnectionConfig};
use fluree_db_indexer::IndexerConfig;
use fluree_db_nameservice::NameService;
use fluree_db_storage_aws::{DynamoDbConfig, DynamoDbNameService, S3Config, S3Storage};
use serde_json::json;
use std::time::Duration;
use testcontainers::core::IntoContainerPort;
use testcontainers::{runners::AsyncRunner, GenericImage, ImageExt};

const LOCALSTACK_EDGE_PORT: u16 = 4566;
const REGION: &str = "us-east-1";

fn set_test_aws_env() {
    // Dummy credentials accepted by LocalStack.
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", REGION);
    std::env::set_var("AWS_DEFAULT_REGION", REGION);
    // Avoid IMDS lookups that can hang tests.
    std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
}

async fn sdk_config_for_localstack(endpoint: &str) -> aws_config::SdkConfig {
    set_test_aws_env();
    let region_provider = RegionProviderChain::default_provider().or_else(REGION);
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .endpoint_url(endpoint)
        .load()
        .await
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

fn build_fluree(
    storage: S3Storage,
    nameservice: DynamoDbNameService,
) -> Fluree<S3Storage, DynamoDbNameService> {
    let cfg = ConnectionConfig::default();
    let conn = Connection::new(cfg, storage);
    Fluree::new(conn, nameservice)
}

async fn list_object_keys(sdk_config: &aws_config::SdkConfig, bucket: &str) -> Vec<String> {
    let s3 = aws_sdk_s3::Client::new(sdk_config);
    let resp = s3
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .expect("list_objects_v2");
    resp.contents()
        .iter()
        .filter_map(|o| o.key().map(|k| k.to_string()))
        .collect()
}

#[tokio::test]
async fn s3_testcontainers_basic_test() {
    // Boot LocalStack (edge port 4566)
    let image = GenericImage::new("localstack/localstack", "latest")
        .with_exposed_port(LOCALSTACK_EDGE_PORT.tcp())
        .with_env_var("SERVICES", "s3,dynamodb")
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

    // Configure AWS SDK to route ALL services to LocalStack
    let sdk_config = sdk_config_for_localstack(&endpoint).await;
    wait_for_localstack(&sdk_config).await;

    // Provision infra
    let bucket = "fluree-test";
    let table = "fluree-test-ns";
    ensure_bucket(&sdk_config, bucket).await;
    ensure_dynamodb_table(&sdk_config, table).await;

    // Create Fluree over S3 + DynamoDB nameservice
    let storage = S3Storage::new(
        &sdk_config,
        S3Config {
            bucket: bucket.to_string(),
            prefix: Some("test".to_string()),
            endpoint: None,
            timeout_ms: Some(30_000),
            max_retries: None,
            retry_base_delay_ms: None,
            retry_max_delay_ms: None,
        },
    )
    .await
    .expect("S3Storage::new");

    let nameservice = DynamoDbNameService::new(
        &sdk_config,
        DynamoDbConfig {
            table_name: table.to_string(),
            region: None,
            endpoint: None,
            timeout_ms: Some(30_000),
        },
    )
    .await
    .expect("DynamoDbNameService::new");

    let fluree = build_fluree(storage.clone(), nameservice.clone());

    // Create ledger + insert data + query
    let alias = "testcontainers-test:main";
    let ledger0 = fluree.create_ledger(alias).await.expect("create ledger");

    let tx = json!({
        "@context": [support::default_context(), {"ex": "http://example.org/ns/"}],
        "insert": [
            {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice"},
            {"@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob"}
        ]
    });
    let ledger1 = fluree.update(ledger0, &tx).await.expect("update").ledger;

    let q = json!({
        "@context": [support::default_context(), {"ex": "http://example.org/ns/"}],
        "select": ["?s", "?name"],
        "where": {"@id": "?s", "@type": "ex:Person", "ex:name": "?name"}
    });
    let results = fluree
        .query(&ledger1, &q)
        .await
        .expect("query")
        .to_jsonld_async(&ledger1.db)
        .await
        .expect("to_jsonld_async");

    assert_eq!(results.as_array().unwrap().len(), 2);
    assert_eq!(results, json!([["ex:alice", "Alice"], ["ex:bob", "Bob"]]));

    // Reload from a "fresh connection" (new cache) and re-query
    let fluree2 = build_fluree(storage.clone(), nameservice.clone());
    let reloaded = fluree2.ledger(alias).await.expect("ledger reload");
    let reload_results = fluree2
        .query(&reloaded, &q)
        .await
        .expect("query reload")
        .to_jsonld_async(&reloaded.db)
        .await
        .expect("to_jsonld_async");
    assert_eq!(results, reload_results);

    // Verify no double slashes in stored S3 keys (Clojure parity)
    let keys = list_object_keys(&sdk_config, bucket).await;
    assert!(!keys.is_empty(), "expected objects in bucket after commit");
    assert!(
        keys.iter().all(|k| !k.contains("//")),
        "paths should not contain double slashes: {keys:?}"
    );
}

#[tokio::test]
#[cfg(feature = "native")]
async fn s3_testcontainers_indexing_test() {
    use fluree_db_transact::{CommitOpts, TxnOpts};

    // Boot LocalStack
    let image = GenericImage::new("localstack/localstack", "latest")
        .with_exposed_port(LOCALSTACK_EDGE_PORT.tcp())
        .with_env_var("SERVICES", "s3,dynamodb")
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

    let sdk_config = sdk_config_for_localstack(&endpoint).await;
    wait_for_localstack(&sdk_config).await;

    let bucket = "fluree-indexing-test";
    let table = "fluree-indexing-test-ns";
    ensure_bucket(&sdk_config, bucket).await;
    ensure_dynamodb_table(&sdk_config, table).await;

    let storage = S3Storage::new(
        &sdk_config,
        S3Config {
            bucket: bucket.to_string(),
            prefix: Some("indexing".to_string()),
            endpoint: None,
            timeout_ms: Some(30_000),
            max_retries: None,
            retry_base_delay_ms: None,
            retry_max_delay_ms: None,
        },
    )
    .await
    .expect("S3Storage::new");

    let nameservice = DynamoDbNameService::new(
        &sdk_config,
        DynamoDbConfig {
            table_name: table.to_string(),
            region: None,
            endpoint: None,
            timeout_ms: Some(30_000),
        },
    )
    .await
    .expect("DynamoDbNameService::new");

    let mut fluree = build_fluree(storage.clone(), nameservice.clone());

    // Start background indexing worker + handle (LocalSet since worker may be !Send)
    let (local, handle) = support::start_background_indexer_local(
        fluree.storage().clone(),
        fluree.nameservice().clone(),
        IndexerConfig::small(),
    );
    fluree.set_indexing_mode(tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let alias = "indexing-test:main";
            let ledger0 = fluree.create_ledger(alias).await.expect("create ledger");

            // Insert enough data to justify indexing and force indexing_needed=true.
            let tx = json!({
                "@context": [support::default_context(), {"ex": "http://example.org/ns/"}],
                "insert": (0..50).map(|i| json!({
                    "@id": format!("ex:person{}", i),
                    "@type": "ex:Person",
                    "ex:name": format!("Person {}", i),
                    "ex:age": i
                })).collect::<Vec<_>>()
            });

            let index_cfg = fluree_db_api::IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 1_000_000,
            };

            let result = fluree
                .insert_with_opts(
                    ledger0,
                    &tx,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert_with_opts");

            // Trigger indexing and wait
            let completion = handle
                .trigger(result.ledger.ledger_id(), result.receipt.t)
                .await;
            match completion.wait().await {
                fluree_db_api::IndexOutcome::Completed { index_t, root_id } => {
                    assert!(index_t >= result.receipt.t);
                    assert!(root_id.is_some(), "expected root_id after indexing");
                }
                fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
                fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
            }

            // Verify index address got published to nameservice
            let rec = fluree
                .nameservice()
                .lookup(result.ledger.ledger_id())
                .await
                .expect("nameservice lookup")
                .expect("record exists");
            assert!(rec.index_head_id.is_some(), "expected published index id");

            // Verify bucket contains index artifacts and no double slashes
            let keys = list_object_keys(&sdk_config, bucket).await;
            assert!(!keys.is_empty());
            assert!(
                keys.iter().all(|k| !k.contains("//")),
                "paths should not contain double slashes: {keys:?}"
            );
            assert!(
                keys.iter().any(|k| k.contains("index")),
                "expected some index files in S3: {keys:?}"
            );
        })
        .await;
}
