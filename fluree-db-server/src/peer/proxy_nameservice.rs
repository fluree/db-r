//! Proxy nameservice implementation for peer mode
//!
//! Fetches nameservice records via the transaction server's `/v1/fluree/storage/ns/{alias}`
//! endpoint instead of direct file access. This allows peers to operate without storage
//! credentials.

use async_trait::async_trait;
use fluree_db_nameservice::{
    NameService, NameServiceError, NsRecord, Publication, Result, Subscription, SubscriptionScope,
};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::fmt::Debug;
use std::time::Duration;
use tokio::sync::broadcast;

/// NameService implementation that proxies lookups through the transaction server
#[derive(Clone)]
pub struct ProxyNameService {
    client: Client,
    base_url: String,
    token: String,
}

impl Debug for ProxyNameService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyNameService")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

/// Response from nameservice lookup endpoint
/// Must match `NsRecordResponse` from routes/storage_proxy.rs
#[derive(Debug, Deserialize)]
struct NsRecordResponse {
    ledger_address: String,
    branch: String,
    commit_address: Option<String>,
    commit_t: i64,
    index_address: Option<String>,
    index_t: i64,
    retracted: bool,
}

impl NsRecordResponse {
    /// Convert to NsRecord, using the original lookup key as the address
    fn into_ns_record(self, lookup_key: &str) -> NsRecord {
        NsRecord {
            // address is the key used for lookup (may differ from name)
            address: lookup_key.to_string(),
            name: self.ledger_address,
            branch: self.branch,
            commit_address: self.commit_address,
            commit_t: self.commit_t,
            index_address: self.index_address,
            index_t: self.index_t,
            default_context_address: None, // Not exposed via proxy API
            retracted: self.retracted,
        }
    }
}

impl ProxyNameService {
    /// Create a new proxy nameservice client
    ///
    /// # Arguments
    ///
    /// * `base_url` - Base URL of the transaction server (e.g., `https://tx.fluree.internal:8090`)
    /// * `token` - Bearer token for authentication (with `fluree.storage.*` claims)
    pub fn new(base_url: String, token: String) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30)) // 30 seconds for NS lookups
            .build()
            .expect("Failed to create proxy nameservice client");

        // Normalize base_url by trimming trailing slashes
        let base_url = base_url.trim_end_matches('/').to_string();

        Self {
            client,
            base_url,
            token,
        }
    }

    /// Build the nameservice lookup endpoint URL
    fn ns_url(&self, alias: &str) -> String {
        format!(
            "{}/v1/fluree/storage/ns/{}",
            self.base_url,
            urlencoding::encode(alias)
        )
    }
}

#[async_trait]
impl NameService for ProxyNameService {
    async fn lookup(&self, ledger_address: &str) -> Result<Option<NsRecord>> {
        let url = self.ns_url(ledger_address);

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await
            .map_err(|e| {
                NameServiceError::storage(format!("Nameservice proxy request failed: {}", e))
            })?;

        let status = response.status();

        match status {
            StatusCode::OK => {
                let ns_response: NsRecordResponse = response.json().await.map_err(|e| {
                    NameServiceError::storage(format!("Failed to parse NS response: {}", e))
                })?;
                Ok(Some(ns_response.into_ns_record(ledger_address)))
            }
            StatusCode::NOT_FOUND => Ok(None),
            StatusCode::UNAUTHORIZED => Err(NameServiceError::storage(format!(
                "Nameservice proxy authentication failed for {}: check token validity",
                ledger_address
            ))),
            StatusCode::FORBIDDEN => {
                // Not in token scope - treat as not found (no existence leak)
                Ok(None)
            }
            _ => Err(NameServiceError::storage(format!(
                "Nameservice proxy unexpected status {} for {}",
                status, ledger_address
            ))),
        }
    }

    async fn all_records(&self) -> Result<Vec<NsRecord>> {
        // Peers use SSE for discovery, not all_records()
        // Return empty - this is intentional for proxy mode
        // The peer maintains its own view of known ledgers via SSE events
        Ok(Vec::new())
    }
}

#[async_trait]
impl Publication for ProxyNameService {
    async fn subscribe(&self, scope: SubscriptionScope) -> Result<Subscription> {
        // Create a dummy broadcast channel that will never receive events.
        // In proxy mode, peers don't serve the /fluree/events endpoint;
        // they subscribe to the tx server's events via HTTP/SSE instead.
        // This implementation exists for type compatibility only.
        let (_, receiver) = broadcast::channel(1);
        Ok(Subscription { scope, receiver })
    }

    async fn unsubscribe(&self, _scope: &SubscriptionScope) -> Result<()> {
        // No-op for proxy mode
        Ok(())
    }

    async fn known_addresses(&self, _alias: &str) -> Result<Vec<String>> {
        // Proxy mode doesn't track commit history locally.
        // Return empty - the transaction server has this information.
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proxy_nameservice_debug() {
        let ns = ProxyNameService::new(
            "http://localhost:8090".to_string(),
            "test-token".to_string(),
        );
        let debug = format!("{:?}", ns);
        assert!(debug.contains("ProxyNameService"));
        assert!(debug.contains("localhost:8090"));
        // Token should NOT be in debug output
        assert!(!debug.contains("test-token"));
    }

    #[test]
    fn test_ns_url() {
        let ns = ProxyNameService::new(
            "http://localhost:8090".to_string(),
            "test-token".to_string(),
        );
        assert_eq!(
            ns.ns_url("books:main"),
            "http://localhost:8090/v1/fluree/storage/ns/books%3Amain"
        );
    }

    #[test]
    fn test_ns_url_no_special_chars() {
        let ns = ProxyNameService::new(
            "http://localhost:8090".to_string(),
            "test-token".to_string(),
        );
        // Alias without colon doesn't need encoding
        assert_eq!(
            ns.ns_url("books"),
            "http://localhost:8090/v1/fluree/storage/ns/books"
        );
    }

    #[test]
    fn test_ns_record_conversion() {
        let response = NsRecordResponse {
            ledger_address: "books:main".to_string(),
            branch: "main".to_string(),
            commit_address: Some("fluree:file://books:main/commit/abc.json".to_string()),
            commit_t: 42,
            index_address: Some("fluree:file://books/main/index/def.json".to_string()),
            index_t: 40,
            retracted: false,
        };

        // Use the lookup key as address (simulating lookup("books"))
        let record = response.into_ns_record("books");
        // address should be the lookup key, not the alias
        assert_eq!(record.address, "books");
        assert_eq!(record.name, "books:main");
        assert_eq!(record.branch, "main");
        assert_eq!(record.commit_t, 42);
        assert_eq!(record.index_t, 40);
        assert!(!record.retracted);
        // default_context_address is not exposed via proxy API
        assert!(record.default_context_address.is_none());
    }
}
