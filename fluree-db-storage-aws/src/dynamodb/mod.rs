//! DynamoDB nameservice implementation
//!
//! Provides `DynamoDbNameService` which implements the `NameService` and `Publisher`
//! traits for storing ledger metadata in Amazon DynamoDB.

pub mod schema;

use crate::error::Result;
use async_trait::async_trait;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use aws_smithy_types::timeout::TimeoutConfig;
use fluree_db_core::alias::{self as core_alias, DEFAULT_BRANCH};
use fluree_db_nameservice::{
    ConfigCasResult, ConfigPayload, ConfigPublisher, ConfigValue, NameService, NameServiceError,
    NsRecord, Publisher, StatusCasResult, StatusPayload, StatusPublisher, StatusValue,
};
use schema::*;
use serde_json;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// DynamoDB nameservice configuration
#[derive(Debug, Clone)]
pub struct DynamoDbConfig {
    /// DynamoDB table name
    pub table_name: String,
    /// AWS region (optional, uses SDK default if not specified)
    pub region: Option<String>,
    /// Optional endpoint override (e.g. LocalStack)
    pub endpoint: Option<String>,
    /// Timeout in milliseconds
    pub timeout_ms: Option<u64>,
}

/// DynamoDB-based nameservice
///
/// Stores ledger metadata in a DynamoDB table with conditional updates
/// for monotonic commit/index publishing.
#[derive(Clone)]
pub struct DynamoDbNameService {
    client: Client,
    table_name: String,
}

impl std::fmt::Debug for DynamoDbNameService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamoDbNameService")
            .field("table_name", &self.table_name)
            .finish()
    }
}

impl DynamoDbNameService {
    /// Create a new DynamoDB nameservice
    ///
    /// Configuration:
    /// - `region`: Override SDK region (uses SDK default if not specified)
    /// - `timeout_ms`: Operation timeout in milliseconds
    pub async fn new(sdk_config: &aws_config::SdkConfig, config: DynamoDbConfig) -> Result<Self> {
        // Build DynamoDB config by inheriting from SdkConfig (preserves HTTP client,
        // retry config, endpoints, sleep impl, etc.) then apply our overrides
        let mut builder = aws_sdk_dynamodb::config::Builder::from(sdk_config);

        // Apply region override if specified
        if let Some(region_str) = config.region {
            builder = builder.region(aws_sdk_dynamodb::config::Region::new(region_str));
        }

        // Apply endpoint override if configured (e.g. LocalStack)
        if let Some(endpoint) = config.endpoint {
            builder = builder.endpoint_url(endpoint);
        }

        // Apply timeout if configured
        if let Some(timeout_ms) = config.timeout_ms {
            let timeout_config = TimeoutConfig::builder()
                .operation_timeout(Duration::from_millis(timeout_ms))
                .build();
            builder = builder.timeout_config(timeout_config);
        }

        let client = Client::from_conf(builder.build());

        Ok(Self {
            client,
            table_name: config.table_name,
        })
    }

    /// Create from a pre-built client (for testing)
    pub fn from_client(client: Client, table_name: String) -> Self {
        Self { client, table_name }
    }

    /// Convert DynamoDB item to NsRecord
    fn item_to_record(item: &HashMap<String, AttributeValue>) -> Option<NsRecord> {
        let ledger_alias = item.get(ATTR_LEDGER_ALIAS)?.as_s().ok()?;
        let ledger_name = item
            .get(ATTR_LEDGER_NAME)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_default();
        let branch = item
            .get(ATTR_BRANCH)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_else(|| DEFAULT_BRANCH.to_string());

        let status = item
            .get(ATTR_STATUS)
            .and_then(|v| v.as_s().ok())
            .map(|s| s.as_str())
            .unwrap_or(STATUS_READY);

        Some(NsRecord {
            address: ledger_alias.clone(),
            alias: ledger_name,
            branch,
            commit_address: item
                .get(ATTR_COMMIT_ADDRESS)
                .and_then(|v| v.as_s().ok())
                .cloned(),
            commit_t: item
                .get(ATTR_COMMIT_T)
                .and_then(|v| v.as_n().ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            index_address: item
                .get(ATTR_INDEX_ADDRESS)
                .and_then(|v| v.as_s().ok())
                .cloned(),
            index_t: item
                .get(ATTR_INDEX_T)
                .and_then(|v| v.as_n().ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            default_context_address: item
                .get(ATTR_DEFAULT_CONTEXT_ADDRESS)
                .and_then(|v| v.as_s().ok())
                .cloned(),
            retracted: status == STATUS_RETRACTED,
        })
    }

    /// Get current Unix epoch seconds
    fn now_epoch() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// Check if an UpdateItem error is a conditional check failure
    fn is_conditional_check_failed(
        err: &aws_sdk_dynamodb::error::SdkError<
            aws_sdk_dynamodb::operation::update_item::UpdateItemError,
        >,
    ) -> bool {
        use aws_sdk_dynamodb::error::SdkError;
        use aws_sdk_dynamodb::operation::update_item::UpdateItemError;

        match err {
            SdkError::ServiceError(service_err) => {
                matches!(
                    service_err.err(),
                    UpdateItemError::ConditionalCheckFailedException(_)
                )
            }
            _ => false,
        }
    }

    /// Check if a PutItem error is a conditional check failure
    fn is_put_conditional_check_failed(
        err: &aws_sdk_dynamodb::error::SdkError<
            aws_sdk_dynamodb::operation::put_item::PutItemError,
        >,
    ) -> bool {
        use aws_sdk_dynamodb::error::SdkError;
        use aws_sdk_dynamodb::operation::put_item::PutItemError;

        match err {
            SdkError::ServiceError(service_err) => {
                matches!(
                    service_err.err(),
                    PutItemError::ConditionalCheckFailedException(_)
                )
            }
            _ => false,
        }
    }
}

#[async_trait]
impl NameService for DynamoDbNameService {
    async fn lookup(
        &self,
        ledger_address: &str,
    ) -> std::result::Result<Option<NsRecord>, NameServiceError> {
        let pk = core_alias::normalize_alias(ledger_address)
            .unwrap_or_else(|_| ledger_address.to_string());

        let response = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(ATTR_LEDGER_ALIAS, AttributeValue::S(pk))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| NameServiceError::storage(format!("DynamoDB GetItem failed: {}", e)))?;

        Ok(response.item().and_then(Self::item_to_record))
    }

    async fn alias(
        &self,
        ledger_address: &str,
    ) -> std::result::Result<Option<String>, NameServiceError> {
        Ok(self.lookup(ledger_address).await?.map(|r| r.alias))
    }

    async fn all_records(&self) -> std::result::Result<Vec<NsRecord>, NameServiceError> {
        let mut records = Vec::new();
        let mut last_evaluated_key = None;

        loop {
            let mut request = self.client.scan().table_name(&self.table_name);

            if let Some(key) = last_evaluated_key.take() {
                request = request.set_exclusive_start_key(Some(key));
            }

            let response = request
                .send()
                .await
                .map_err(|e| NameServiceError::storage(format!("DynamoDB Scan failed: {}", e)))?;

            for item in response.items() {
                if let Some(record) = Self::item_to_record(item) {
                    records.push(record);
                }
            }

            match response.last_evaluated_key() {
                Some(key) if !key.is_empty() => {
                    last_evaluated_key = Some(key.clone());
                }
                _ => break,
            }
        }

        Ok(records)
    }
}

#[async_trait]
impl Publisher for DynamoDbNameService {
    async fn publish_ledger_init(&self, alias: &str) -> std::result::Result<(), NameServiceError> {
        let pk = core_alias::normalize_alias(alias).unwrap_or_else(|_| alias.to_string());
        let (ledger_name, branch) = core_alias::split_alias(alias)
            .unwrap_or_else(|_| (alias.to_string(), DEFAULT_BRANCH.to_string()));
        let now = Self::now_epoch();

        // Use PutItem with condition_expression to atomically check-and-create.
        // The condition ensures we only create if no record exists (including retracted).
        let result = self
            .client
            .put_item()
            .table_name(&self.table_name)
            .item(ATTR_LEDGER_ALIAS, AttributeValue::S(pk.clone()))
            .item(ATTR_LEDGER_NAME, AttributeValue::S(ledger_name))
            .item(ATTR_BRANCH, AttributeValue::S(branch))
            .item(ATTR_STATUS, AttributeValue::S(STATUS_READY.to_string()))
            .item(ATTR_UPDATED_AT, AttributeValue::N(now.to_string()))
            // commit_t=0 indicates no commits yet
            .item(ATTR_COMMIT_T, AttributeValue::N("0".to_string()))
            // index_t=0 indicates no index yet
            .item(ATTR_INDEX_T, AttributeValue::N("0".to_string()))
            .condition_expression("attribute_not_exists(#pk)")
            .expression_attribute_names("#pk", ATTR_LEDGER_ALIAS)
            .send()
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) if Self::is_put_conditional_check_failed(&e) => {
                // Record already exists (including retracted)
                Err(NameServiceError::ledger_already_exists(&pk))
            }
            Err(e) => Err(NameServiceError::storage(format!(
                "DynamoDB PutItem failed: {}",
                e
            ))),
        }
    }

    async fn publish_commit(
        &self,
        alias: &str,
        commit_addr: &str,
        commit_t: i64,
    ) -> std::result::Result<(), NameServiceError> {
        let pk = core_alias::normalize_alias(alias).unwrap_or_else(|_| alias.to_string());
        let (ledger_name, branch) = core_alias::split_alias(alias)
            .unwrap_or_else(|_| (alias.to_string(), DEFAULT_BRANCH.to_string()));
        let now = Self::now_epoch();

        let result = self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key(ATTR_LEDGER_ALIAS, AttributeValue::S(pk))
            // Use ExpressionAttributeNames for reserved words (status)
            .update_expression(
                "SET #ca = :addr, #ct = :t, #ln = :ln, #br = :br, #st = :ready, #ua = :now",
            )
            .condition_expression("attribute_not_exists(#ct) OR #ct < :t")
            .expression_attribute_names("#ca", ATTR_COMMIT_ADDRESS)
            .expression_attribute_names("#ct", ATTR_COMMIT_T)
            .expression_attribute_names("#ln", ATTR_LEDGER_NAME)
            .expression_attribute_names("#br", ATTR_BRANCH)
            .expression_attribute_names("#st", ATTR_STATUS)
            .expression_attribute_names("#ua", ATTR_UPDATED_AT)
            .expression_attribute_values(":addr", AttributeValue::S(commit_addr.to_string()))
            .expression_attribute_values(":t", AttributeValue::N(commit_t.to_string()))
            .expression_attribute_values(":ln", AttributeValue::S(ledger_name))
            .expression_attribute_values(":br", AttributeValue::S(branch))
            .expression_attribute_values(":ready", AttributeValue::S(STATUS_READY.to_string()))
            .expression_attribute_values(":now", AttributeValue::N(now.to_string()))
            .send()
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) if Self::is_conditional_check_failed(&e) => {
                // Existing value is newer - this is expected under contention
                Ok(())
            }
            Err(e) => Err(NameServiceError::storage(format!(
                "DynamoDB UpdateItem failed: {}",
                e
            ))),
        }
    }

    async fn publish_index(
        &self,
        alias: &str,
        index_addr: &str,
        index_t: i64,
    ) -> std::result::Result<(), NameServiceError> {
        let pk = core_alias::normalize_alias(alias).unwrap_or_else(|_| alias.to_string());
        let (ledger_name, branch) = core_alias::split_alias(alias)
            .unwrap_or_else(|_| (alias.to_string(), DEFAULT_BRANCH.to_string()));
        let now = Self::now_epoch();

        let result = self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key(ATTR_LEDGER_ALIAS, AttributeValue::S(pk))
            .update_expression(
                "SET #ia = :addr, #it = :t, #ln = :ln, #br = :br, #st = :ready, #ua = :now",
            )
            .condition_expression("attribute_not_exists(#it) OR #it < :t")
            .expression_attribute_names("#ia", ATTR_INDEX_ADDRESS)
            .expression_attribute_names("#it", ATTR_INDEX_T)
            .expression_attribute_names("#ln", ATTR_LEDGER_NAME)
            .expression_attribute_names("#br", ATTR_BRANCH)
            .expression_attribute_names("#st", ATTR_STATUS)
            .expression_attribute_names("#ua", ATTR_UPDATED_AT)
            .expression_attribute_values(":addr", AttributeValue::S(index_addr.to_string()))
            .expression_attribute_values(":t", AttributeValue::N(index_t.to_string()))
            .expression_attribute_values(":ln", AttributeValue::S(ledger_name))
            .expression_attribute_values(":br", AttributeValue::S(branch))
            .expression_attribute_values(":ready", AttributeValue::S(STATUS_READY.to_string()))
            .expression_attribute_values(":now", AttributeValue::N(now.to_string()))
            .send()
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) if Self::is_conditional_check_failed(&e) => {
                // Existing value is newer - this is expected under contention
                Ok(())
            }
            Err(e) => Err(NameServiceError::storage(format!(
                "DynamoDB UpdateItem failed: {}",
                e
            ))),
        }
    }

    async fn retract(&self, alias: &str) -> std::result::Result<(), NameServiceError> {
        let pk = core_alias::normalize_alias(alias).unwrap_or_else(|_| alias.to_string());
        let now = Self::now_epoch();

        // Update status to retracted and increment status_v
        // Uses ADD to increment status_v (or set to 2 if missing, since default is 1)
        self.client
            .update_item()
            .table_name(&self.table_name)
            .key(ATTR_LEDGER_ALIAS, AttributeValue::S(pk))
            .update_expression("SET #st = :retracted, #ua = :now ADD #sv :one")
            .expression_attribute_names("#st", ATTR_STATUS)
            .expression_attribute_names("#ua", ATTR_UPDATED_AT)
            .expression_attribute_names("#sv", ATTR_STATUS_V)
            .expression_attribute_values(
                ":retracted",
                AttributeValue::S(STATUS_RETRACTED.to_string()),
            )
            .expression_attribute_values(":now", AttributeValue::N(now.to_string()))
            .expression_attribute_values(":one", AttributeValue::N("1".to_string()))
            .send()
            .await
            .map_err(|e| NameServiceError::storage(format!("DynamoDB UpdateItem failed: {}", e)))?;

        Ok(())
    }

    fn publishing_address(&self, alias: &str) -> Option<String> {
        Some(core_alias::normalize_alias(alias).unwrap_or_else(|_| alias.to_string()))
    }
}

// ---------------------------------------------------------------------------
// V2 Extension: StatusPublisher and ConfigPublisher
// ---------------------------------------------------------------------------

#[async_trait]
impl StatusPublisher for DynamoDbNameService {
    async fn get_status(
        &self,
        alias: &str,
    ) -> std::result::Result<Option<StatusValue>, NameServiceError> {
        let pk = core_alias::normalize_alias(alias).unwrap_or_else(|_| alias.to_string());

        let response = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(ATTR_LEDGER_ALIAS, AttributeValue::S(pk))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| NameServiceError::storage(format!("DynamoDB GetItem failed: {}", e)))?;

        let Some(item) = response.item() else {
            return Ok(None);
        };

        // Extract status_v (defaults to 1 if missing for legacy records)
        let v = item
            .get(ATTR_STATUS_V)
            .and_then(|v| v.as_n().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);

        // Extract status state (defaults to "ready")
        let state = item
            .get(ATTR_STATUS)
            .and_then(|v| v.as_s().ok())
            .cloned()
            .unwrap_or_else(|| STATUS_READY.to_string());

        // Extract status_meta if present
        let extra = item
            .get(ATTR_STATUS_META)
            .and_then(|v| v.as_m().ok())
            .map(|m| Self::dynamo_map_to_json_map(m))
            .unwrap_or_default();

        Ok(Some(StatusValue::new(
            v,
            StatusPayload::with_extra(state, extra),
        )))
    }

    async fn push_status(
        &self,
        alias: &str,
        expected: Option<&StatusValue>,
        new: &StatusValue,
    ) -> std::result::Result<StatusCasResult, NameServiceError> {
        let pk = core_alias::normalize_alias(alias).unwrap_or_else(|_| alias.to_string());
        let now = Self::now_epoch();

        let Some(exp) = expected else {
            // Cannot create status without expected (record must exist)
            // Read current to return conflict
            let current = self.get_status(alias).await?;
            return Ok(StatusCasResult::Conflict { actual: current });
        };

        // Monotonic guard: new watermark must advance
        if new.v <= exp.v {
            let current = self.get_status(alias).await?;
            return Ok(StatusCasResult::Conflict { actual: current });
        }

        // Build condition expression: status_v must match expected AND new.v > expected.v
        // For legacy records without status_v, expected.v should be 1
        // Also handle legacy records that may lack the status attribute
        let condition = if exp.v == 1 {
            // Could be legacy record (no status_v or status) or v=1
            "(attribute_not_exists(#sv) OR #sv = :expected_v) AND (attribute_not_exists(#st) OR #st = :expected_state) AND :new_v > :expected_v"
        } else {
            "#sv = :expected_v AND #st = :expected_state AND :new_v > :expected_v"
        };

        // Build update expression
        let mut update_expr = "SET #st = :new_state, #sv = :new_v, #ua = :now".to_string();
        let mut request = self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key(ATTR_LEDGER_ALIAS, AttributeValue::S(pk.clone()))
            .condition_expression(condition)
            .expression_attribute_names("#st", ATTR_STATUS)
            .expression_attribute_names("#sv", ATTR_STATUS_V)
            .expression_attribute_names("#ua", ATTR_UPDATED_AT)
            .expression_attribute_values(":expected_v", AttributeValue::N(exp.v.to_string()))
            .expression_attribute_values(
                ":expected_state",
                AttributeValue::S(exp.payload.state.clone()),
            )
            .expression_attribute_values(":new_state", AttributeValue::S(new.payload.state.clone()))
            .expression_attribute_values(":new_v", AttributeValue::N(new.v.to_string()))
            .expression_attribute_values(":now", AttributeValue::N(now.to_string()));

        // Add status_meta if present
        if !new.payload.extra.is_empty() {
            update_expr.push_str(", #sm = :new_meta");
            request = request
                .expression_attribute_names("#sm", ATTR_STATUS_META)
                .expression_attribute_values(
                    ":new_meta",
                    Self::json_map_to_dynamo_map(&new.payload.extra),
                );
        } else {
            // Remove status_meta if empty
            update_expr.push_str(" REMOVE #sm");
            request = request.expression_attribute_names("#sm", ATTR_STATUS_META);
        }

        let result = request.update_expression(update_expr).send().await;

        match result {
            Ok(_) => Ok(StatusCasResult::Updated),
            Err(e) if Self::is_conditional_check_failed(&e) => {
                // Conflict - read current and return
                let current = self.get_status(alias).await?;
                Ok(StatusCasResult::Conflict { actual: current })
            }
            Err(e) => Err(NameServiceError::storage(format!(
                "DynamoDB UpdateItem failed: {}",
                e
            ))),
        }
    }
}

#[async_trait]
impl ConfigPublisher for DynamoDbNameService {
    async fn get_config(
        &self,
        alias: &str,
    ) -> std::result::Result<Option<ConfigValue>, NameServiceError> {
        let pk = core_alias::normalize_alias(alias).unwrap_or_else(|_| alias.to_string());

        let response = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(ATTR_LEDGER_ALIAS, AttributeValue::S(pk))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| NameServiceError::storage(format!("DynamoDB GetItem failed: {}", e)))?;

        let Some(item) = response.item() else {
            return Ok(None);
        };

        // Extract default_context_address if present
        let default_context = item
            .get(ATTR_DEFAULT_CONTEXT_ADDRESS)
            .and_then(|v| v.as_s().ok())
            .cloned();

        // Extract config_meta if present
        let config_meta = item.get(ATTR_CONFIG_META).and_then(|v| v.as_m().ok());

        // config_v defaults based on whether default_context exists:
        // - If default_context exists but config_v is missing, treat as v=1 (legacy record)
        // - If neither exists, treat as v=0 (unborn)
        let v = item
            .get(ATTR_CONFIG_V)
            .and_then(|v| v.as_n().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| {
                if default_context.is_some() || config_meta.is_some() {
                    1 // Legacy record with config data
                } else {
                    0 // Unborn
                }
            });

        // Build ConfigPayload if we have any config data
        let payload = if v == 0 && default_context.is_none() && config_meta.is_none() {
            None
        } else {
            let extra = config_meta
                .map(|m| Self::dynamo_map_to_json_map(m))
                .unwrap_or_default();
            Some(ConfigPayload {
                default_context,
                extra,
            })
        };

        Ok(Some(ConfigValue { v, payload }))
    }

    async fn push_config(
        &self,
        alias: &str,
        expected: Option<&ConfigValue>,
        new: &ConfigValue,
    ) -> std::result::Result<ConfigCasResult, NameServiceError> {
        let pk = core_alias::normalize_alias(alias).unwrap_or_else(|_| alias.to_string());
        let now = Self::now_epoch();

        let Some(exp) = expected else {
            // Cannot create config without expected (record must exist)
            let current = self.get_config(alias).await?;
            return Ok(ConfigCasResult::Conflict { actual: current });
        };

        // Monotonic guard: new watermark must advance
        if new.v <= exp.v {
            let current = self.get_config(alias).await?;
            return Ok(ConfigCasResult::Conflict { actual: current });
        }

        // Check if expected payload has any legacy config data (default_context or extra)
        let has_legacy_config_data = exp.payload.as_ref().map_or(false, |p| {
            p.default_context.is_some() || !p.extra.is_empty()
        });

        // Build condition based on expected state, with monotonic guard
        let condition = if exp.v == 0 {
            // Unborn: config_v must not exist or be 0, and no default_context
            // Monotonic: new.v > 0 (always true if new.v > exp.v and exp.v == 0)
            "(attribute_not_exists(#cv) OR #cv = :zero) AND attribute_not_exists(#dc)"
        } else if exp.v == 1 && has_legacy_config_data {
            // Legacy record: may not have config_v, but has default_context or config_meta
            "(attribute_not_exists(#cv) OR #cv = :expected_v) AND :new_v > :expected_v"
        } else {
            "#cv = :expected_v AND :new_v > :expected_v"
        };

        // Build update expression
        let mut update_parts = vec!["#cv = :new_v", "#ua = :now"];
        let mut request = self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key(ATTR_LEDGER_ALIAS, AttributeValue::S(pk.clone()))
            .expression_attribute_names("#cv", ATTR_CONFIG_V)
            .expression_attribute_names("#ua", ATTR_UPDATED_AT)
            .expression_attribute_names("#dc", ATTR_DEFAULT_CONTEXT_ADDRESS)
            .expression_attribute_values(":new_v", AttributeValue::N(new.v.to_string()))
            .expression_attribute_values(":now", AttributeValue::N(now.to_string()));

        if exp.v == 0 {
            request =
                request.expression_attribute_values(":zero", AttributeValue::N("0".to_string()));
        } else {
            request = request
                .expression_attribute_values(":expected_v", AttributeValue::N(exp.v.to_string()));
        }

        let mut remove_parts: Vec<&str> = vec![];

        // Handle default_context
        if let Some(ref payload) = new.payload {
            if let Some(ref ctx) = payload.default_context {
                update_parts.push("#dc = :new_dc");
                request =
                    request.expression_attribute_values(":new_dc", AttributeValue::S(ctx.clone()));
            } else {
                remove_parts.push("#dc");
            }

            // Handle config_meta
            if !payload.extra.is_empty() {
                update_parts.push("#cm = :new_meta");
                request = request
                    .expression_attribute_names("#cm", ATTR_CONFIG_META)
                    .expression_attribute_values(
                        ":new_meta",
                        Self::json_map_to_dynamo_map(&payload.extra),
                    );
            } else {
                request = request.expression_attribute_names("#cm", ATTR_CONFIG_META);
                remove_parts.push("#cm");
            }
        } else {
            // No payload - remove default_context and config_meta
            remove_parts.push("#dc");
            request = request.expression_attribute_names("#cm", ATTR_CONFIG_META);
            remove_parts.push("#cm");
        }

        let mut update_expr = format!("SET {}", update_parts.join(", "));
        if !remove_parts.is_empty() {
            update_expr.push_str(&format!(" REMOVE {}", remove_parts.join(", ")));
        }

        let result = request
            .condition_expression(condition)
            .update_expression(update_expr)
            .send()
            .await;

        match result {
            Ok(_) => Ok(ConfigCasResult::Updated),
            Err(e) if Self::is_conditional_check_failed(&e) => {
                // Conflict - read current and return
                let current = self.get_config(alias).await?;
                Ok(ConfigCasResult::Conflict { actual: current })
            }
            Err(e) => Err(NameServiceError::storage(format!(
                "DynamoDB UpdateItem failed: {}",
                e
            ))),
        }
    }
}

impl DynamoDbNameService {
    /// Convert DynamoDB Map to JSON HashMap
    fn dynamo_map_to_json_map(
        map: &HashMap<String, AttributeValue>,
    ) -> std::collections::HashMap<String, serde_json::Value> {
        map.iter()
            .filter_map(|(k, v)| {
                let json_val = Self::dynamo_attr_to_json(v)?;
                Some((k.clone(), json_val))
            })
            .collect()
    }

    /// Convert a single DynamoDB AttributeValue to JSON Value
    fn dynamo_attr_to_json(attr: &AttributeValue) -> Option<serde_json::Value> {
        match attr {
            AttributeValue::S(s) => Some(serde_json::Value::String(s.clone())),
            AttributeValue::N(n) => {
                // Try to parse as i64 first, then f64
                if let Ok(i) = n.parse::<i64>() {
                    Some(serde_json::Value::Number(i.into()))
                } else if let Ok(f) = n.parse::<f64>() {
                    serde_json::Number::from_f64(f).map(serde_json::Value::Number)
                } else {
                    None
                }
            }
            AttributeValue::Bool(b) => Some(serde_json::Value::Bool(*b)),
            AttributeValue::Null(_) => Some(serde_json::Value::Null),
            AttributeValue::L(list) => {
                let items: Vec<_> = list.iter().filter_map(Self::dynamo_attr_to_json).collect();
                Some(serde_json::Value::Array(items))
            }
            AttributeValue::M(map) => {
                let obj: serde_json::Map<_, _> = map
                    .iter()
                    .filter_map(|(k, v)| Self::dynamo_attr_to_json(v).map(|val| (k.clone(), val)))
                    .collect();
                Some(serde_json::Value::Object(obj))
            }
            _ => None, // Skip binary and other types
        }
    }

    /// Convert JSON HashMap to DynamoDB Map AttributeValue
    fn json_map_to_dynamo_map(
        map: &std::collections::HashMap<String, serde_json::Value>,
    ) -> AttributeValue {
        let dynamo_map: HashMap<String, AttributeValue> = map
            .iter()
            .filter_map(|(k, v)| Self::json_to_dynamo_attr(v).map(|attr| (k.clone(), attr)))
            .collect();
        AttributeValue::M(dynamo_map)
    }

    /// Convert a single JSON Value to DynamoDB AttributeValue
    fn json_to_dynamo_attr(val: &serde_json::Value) -> Option<AttributeValue> {
        match val {
            serde_json::Value::Null => Some(AttributeValue::Null(true)),
            serde_json::Value::Bool(b) => Some(AttributeValue::Bool(*b)),
            serde_json::Value::Number(n) => Some(AttributeValue::N(n.to_string())),
            serde_json::Value::String(s) => Some(AttributeValue::S(s.clone())),
            serde_json::Value::Array(arr) => {
                let items: Vec<_> = arr.iter().filter_map(Self::json_to_dynamo_attr).collect();
                Some(AttributeValue::L(items))
            }
            serde_json::Value::Object(obj) => {
                let map: HashMap<String, AttributeValue> = obj
                    .iter()
                    .filter_map(|(k, v)| Self::json_to_dynamo_attr(v).map(|attr| (k.clone(), attr)))
                    .collect();
                Some(AttributeValue::M(map))
            }
        }
    }
}
