//! Opt-in infra tests that boot LocalStack via testcontainers.
//!
//! Run (requires Docker):
//!   cargo test -p fluree-db-connection --features aws-testcontainers --test aws_testcontainers_test -- --nocapture

#![cfg(feature = "aws-testcontainers")]

use fluree_db_connection::{connect_async, ConnectionHandle};
use fluree_db_core::{Storage, StorageWrite};
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
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
    };

    let ddb = aws_sdk_dynamodb::Client::new(sdk_config);

    // If it exists, we're done.
    if ddb
        .describe_table()
        .table_name(table_name)
        .send()
        .await
        .is_ok()
    {
        return;
    }

    let _ = ddb
        .create_table()
        .table_name(table_name)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("ledger_alias")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("valid attribute definition"),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("ledger_alias")
                .key_type(KeyType::Hash)
                .build()
                .expect("valid key schema element"),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await;

    // Wait until the table is ACTIVE (or at least describable).
    for _ in 0..60 {
        if ddb
            .describe_table()
            .table_name(table_name)
            .send()
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("DynamoDB table was not available: {}", table_name);
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

    // 5) DynamoDB nameservice smoke: publish + lookup
    let alias = "mydb:main";
    let index_addr = "fluree:s3://mydb/main/index/root.json";
    aws.publish_index(alias, index_addr, 1)
        .await
        .expect("publish_index should succeed");

    let record = aws
        .lookup(alias)
        .await
        .expect("lookup should succeed")
        .expect("record should exist after publish_index");
    assert_eq!(record.index_address.as_deref(), Some(index_addr));
    assert_eq!(record.index_t, 1);

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
