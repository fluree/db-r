//! HTTP client for remote ledger query/transact operations
//!
//! Used by the CLI's "track" mode to forward data operations to a remote
//! Fluree server instead of executing them locally. This is distinct from
//! `fluree-db-nameservice-sync`'s `HttpRemoteClient`, which handles only
//! nameservice ref-level operations (lookup, push, snapshot).
//!
//! When a `RefreshConfig` is provided, the client automatically attempts
//! token refresh on 401 responses and retries the request once. Callers
//! should check `take_refreshed_tokens()` after operations to persist any
//! updated tokens.

use parking_lot::Mutex;
use reqwest::{Client, StatusCode};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for automatic token refresh on 401.
#[derive(Clone, Debug)]
pub struct RefreshConfig {
    /// Exchange endpoint URL for token refresh.
    pub exchange_url: String,
    /// Refresh token for silent renewal.
    pub refresh_token: String,
}

/// New token values after a successful refresh. Callers should persist these.
#[derive(Clone, Debug)]
pub struct RefreshedTokens {
    pub access_token: String,
    pub refresh_token: Option<String>,
}

/// HTTP client for ledger data operations against a remote Fluree server.
///
/// Supports query (FQL/SPARQL), insert, upsert, transact, ledger-info, and
/// existence checks via the server's REST API. Optionally performs automatic
/// token refresh on 401 when a `RefreshConfig` is provided.
#[derive(Clone)]
pub struct RemoteLedgerClient {
    client: Client,
    base_url: String,
    token: Arc<Mutex<Option<String>>>,
    refresh_config: Option<Arc<Mutex<RefreshConfig>>>,
    refreshed: Arc<Mutex<Option<RefreshedTokens>>>,
}

impl fmt::Debug for RemoteLedgerClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteLedgerClient")
            .field("base_url", &self.base_url)
            .field("has_token", &self.token.lock().is_some())
            .field("has_refresh", &self.refresh_config.is_some())
            .finish()
    }
}

/// Error type for remote ledger operations.
#[derive(Debug)]
pub enum RemoteLedgerError {
    /// Network or connection error
    Network(String),
    /// 401 Unauthorized
    Unauthorized,
    /// 403 Forbidden
    Forbidden,
    /// 404 Not Found (includes server message if any)
    NotFound(String),
    /// 400 Bad Request (includes server error message)
    BadRequest(String),
    /// 5xx Server Error (includes server error message)
    ServerError(String),
    /// Response could not be parsed as expected
    InvalidResponse(String),
}

impl fmt::Display for RemoteLedgerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RemoteLedgerError::Network(msg) => write!(f, "network error: {msg}"),
            RemoteLedgerError::Unauthorized => write!(
                f,
                "authentication failed (401). Token may be expired or revoked.\n  \
                 Run `fluree auth login` to store a new token, or \
                 `fluree auth status` to check expiry."
            ),
            RemoteLedgerError::Forbidden => write!(f, "access denied (403)"),
            RemoteLedgerError::NotFound(msg) => write!(f, "not found: {msg}"),
            RemoteLedgerError::BadRequest(msg) => write!(f, "bad request: {msg}"),
            RemoteLedgerError::ServerError(msg) => write!(f, "server error: {msg}"),
            RemoteLedgerError::InvalidResponse(msg) => write!(f, "invalid response: {msg}"),
        }
    }
}

impl RemoteLedgerClient {
    /// Create a new remote ledger client.
    ///
    /// `base_url` is the Fluree API base (e.g., `http://localhost:8090/fluree`
    /// or `https://example.com/v1/fluree`). Trailing slashes are stripped.
    ///
    /// For backwards compatibility with older configs that stored the server
    /// root, this method appends `/fluree` if it is missing.
    pub fn new(base_url: &str, auth_token: Option<String>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client");

        let trimmed = base_url.trim_end_matches('/');
        let normalized = if trimmed.ends_with("/fluree") {
            trimmed.to_string()
        } else {
            format!("{}/fluree", trimmed)
        };

        Self {
            client,
            base_url: normalized,
            token: Arc::new(Mutex::new(auth_token)),
            refresh_config: None,
            refreshed: Arc::new(Mutex::new(None)),
        }
    }

    /// Attach refresh configuration for automatic 401 retry.
    pub fn with_refresh(mut self, config: RefreshConfig) -> Self {
        self.refresh_config = Some(Arc::new(Mutex::new(config)));
        self
    }

    /// Take any refreshed tokens (consuming them). Callers should persist
    /// these back to config.toml after the operation completes.
    pub fn take_refreshed_tokens(&self) -> Option<RefreshedTokens> {
        self.refreshed.lock().take()
    }

    fn add_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let token = self.token.lock();
        if let Some(ref t) = *token {
            req.bearer_auth(t)
        } else {
            req
        }
    }

    /// Map a non-2xx response to a `RemoteLedgerError`.
    async fn map_error(resp: reqwest::Response) -> RemoteLedgerError {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();

        match status {
            StatusCode::UNAUTHORIZED => RemoteLedgerError::Unauthorized,
            StatusCode::FORBIDDEN => RemoteLedgerError::Forbidden,
            StatusCode::NOT_FOUND => RemoteLedgerError::NotFound(if body.is_empty() {
                "resource not found".to_string()
            } else {
                body
            }),
            StatusCode::BAD_REQUEST => RemoteLedgerError::BadRequest(if body.is_empty() {
                "bad request".to_string()
            } else {
                body
            }),
            s if s.is_server_error() => RemoteLedgerError::ServerError(if body.is_empty() {
                format!("status {s}")
            } else {
                body
            }),
            _ => RemoteLedgerError::ServerError(format!("unexpected status {status}: {body}")),
        }
    }

    /// Map a reqwest error (network/timeout) to a `RemoteLedgerError`.
    fn map_network_error(e: reqwest::Error) -> RemoteLedgerError {
        if e.is_timeout() {
            RemoteLedgerError::Network(format!("request timed out: {e}"))
        } else if e.is_connect() {
            RemoteLedgerError::Network(format!("connection failed: {e}"))
        } else {
            RemoteLedgerError::Network(e.to_string())
        }
    }

    /// Attempt to refresh the access token using the stored refresh_token.
    /// Returns true if refresh succeeded and the token was updated.
    async fn try_refresh(&self) -> bool {
        let refresh_cfg = match &self.refresh_config {
            Some(cfg) => cfg.clone(),
            None => return false,
        };

        let (exchange_url, refresh_token) = {
            let cfg = refresh_cfg.lock();
            (cfg.exchange_url.clone(), cfg.refresh_token.clone())
        };

        let body = serde_json::json!({
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        });

        let resp = match self.client.post(&exchange_url).json(&body).send().await {
            Ok(r) => r,
            Err(_) => return false,
        };

        if !resp.status().is_success() {
            return false;
        }

        let resp_body: serde_json::Value = match resp.json().await {
            Ok(b) => b,
            Err(_) => return false,
        };

        let new_access = match resp_body.get("access_token").and_then(|v| v.as_str()) {
            Some(t) => t.to_string(),
            None => return false,
        };

        let new_refresh = resp_body
            .get("refresh_token")
            .and_then(|v| v.as_str())
            .map(String::from);

        // Update the token
        *self.token.lock() = Some(new_access.clone());

        // Update refresh_token if a new one was provided
        if let Some(ref new_rt) = new_refresh {
            refresh_cfg.lock().refresh_token = new_rt.clone();
        }

        // Store refreshed tokens for caller to persist
        *self.refreshed.lock() = Some(RefreshedTokens {
            access_token: new_access,
            refresh_token: new_refresh,
        });

        eprintln!("  (token refreshed automatically)");
        true
    }

    // =========================================================================
    // Generic request execution with 401 retry
    // =========================================================================

    /// Execute a request. On 401, attempt token refresh and retry once.
    async fn send_json(
        &self,
        method: reqwest::Method,
        url: &str,
        content_type: &str,
        body: Option<RequestBody<'_>>,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        // First attempt
        let resp = self
            .build_request(method.clone(), url, content_type, &body)
            .send()
            .await
            .map_err(Self::map_network_error)?;

        if resp.status().is_success() {
            return resp
                .json()
                .await
                .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()));
        }

        if resp.status() == StatusCode::UNAUTHORIZED && self.try_refresh().await {
            // Retry with refreshed token
            let resp2 = self
                .build_request(method, url, content_type, &body)
                .send()
                .await
                .map_err(Self::map_network_error)?;

            if resp2.status().is_success() {
                return resp2
                    .json()
                    .await
                    .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()));
            }
            return Err(Self::map_error(resp2).await);
        }

        Err(Self::map_error(resp).await)
    }

    fn build_request(
        &self,
        method: reqwest::Method,
        url: &str,
        content_type: &str,
        body: &Option<RequestBody<'_>>,
    ) -> reqwest::RequestBuilder {
        let mut req = self.add_auth(self.client.request(method, url));
        req = req.header("Content-Type", content_type);
        match body {
            Some(RequestBody::Json(v)) => req.json(*v),
            Some(RequestBody::Text(s)) => req.body(s.to_string()),
            None => req,
        }
    }

    fn ledger_tail(ledger: &str) -> &str {
        ledger.trim_start_matches('/')
    }

    fn op_url(&self, op: &str, ledger: &str) -> String {
        format!("{}/{}/{}", self.base_url, op, Self::ledger_tail(ledger))
    }

    // =========================================================================
    // Query
    // =========================================================================

    /// Execute an FQL (JSON-LD) query against a ledger.
    pub async fn query_fql(
        &self,
        ledger: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url("query", ledger);
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/json",
            Some(RequestBody::Json(body)),
        )
        .await
    }

    /// Execute a SPARQL query against a ledger.
    pub async fn query_sparql(
        &self,
        ledger: &str,
        sparql: &str,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url("query", ledger);
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/sparql-query",
            Some(RequestBody::Text(sparql)),
        )
        .await
    }

    // =========================================================================
    // Insert
    // =========================================================================

    /// Insert JSON-LD data into a ledger.
    pub async fn insert_jsonld(
        &self,
        ledger: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url("insert", ledger);
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/json",
            Some(RequestBody::Json(body)),
        )
        .await
    }

    /// Insert Turtle data into a ledger.
    pub async fn insert_turtle(
        &self,
        ledger: &str,
        turtle: &str,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url("insert", ledger);
        self.send_json(
            reqwest::Method::POST,
            &url,
            "text/turtle",
            Some(RequestBody::Text(turtle)),
        )
        .await
    }

    // =========================================================================
    // Upsert
    // =========================================================================

    /// Upsert JSON-LD data into a ledger.
    pub async fn upsert_jsonld(
        &self,
        ledger: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url("upsert", ledger);
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/json",
            Some(RequestBody::Json(body)),
        )
        .await
    }

    /// Upsert Turtle data into a ledger.
    pub async fn upsert_turtle(
        &self,
        ledger: &str,
        turtle: &str,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url("upsert", ledger);
        self.send_json(
            reqwest::Method::POST,
            &url,
            "text/turtle",
            Some(RequestBody::Text(turtle)),
        )
        .await
    }

    // =========================================================================
    // Transact
    // =========================================================================

    // Kept for: `fluree transact` CLI command (combined insert+delete with WHERE).
    // Use when: a `transact` subcommand is added to the CLI.
    /// Execute a full JSON-LD transaction (insert + delete with WHERE).
    #[expect(dead_code)]
    pub async fn transact_jsonld(
        &self,
        ledger: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url("transact", ledger);
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/json",
            Some(RequestBody::Json(body)),
        )
        .await
    }

    // Kept for: `fluree transact` CLI command (SPARQL UPDATE support).
    // Use when: a `transact` subcommand is added to the CLI.
    /// Execute a SPARQL UPDATE transaction.
    #[expect(dead_code)]
    pub async fn transact_sparql(
        &self,
        ledger: &str,
        sparql: &str,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url("transact", ledger);
        self.send_json(
            reqwest::Method::POST,
            &url,
            "application/sparql-update",
            Some(RequestBody::Text(sparql)),
        )
        .await
    }

    // =========================================================================
    // Ledger Info / Exists
    // =========================================================================

    /// Get ledger info from the remote server.
    pub async fn ledger_info(&self, ledger: &str) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = self.op_url("info", ledger);
        self.send_json(reqwest::Method::GET, &url, "application/json", None)
            .await
    }

    /// Check if a ledger exists on the remote server.
    pub async fn ledger_exists(&self, ledger: &str) -> Result<bool, RemoteLedgerError> {
        let url = self.op_url("exists", ledger);

        let resp = self
            .build_request(reqwest::Method::GET, &url, "application/json", &None)
            .send()
            .await
            .map_err(Self::map_network_error)?;

        if resp.status().is_success() {
            let body: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()))?;
            Ok(body
                .get("exists")
                .and_then(|v| v.as_bool())
                .unwrap_or(false))
        } else if resp.status() == StatusCode::NOT_FOUND {
            Ok(false)
        } else if resp.status() == StatusCode::UNAUTHORIZED && self.try_refresh().await {
            // Retry after refresh
            let resp2 = self
                .build_request(reqwest::Method::GET, &url, "application/json", &None)
                .send()
                .await
                .map_err(Self::map_network_error)?;

            if resp2.status().is_success() {
                let body: serde_json::Value = resp2
                    .json()
                    .await
                    .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()))?;
                Ok(body
                    .get("exists")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false))
            } else if resp2.status() == StatusCode::NOT_FOUND {
                Ok(false)
            } else {
                Err(Self::map_error(resp2).await)
            }
        } else {
            Err(Self::map_error(resp).await)
        }
    }
}

/// Request body variants for the generic send method.
enum RequestBody<'a> {
    Json(&'a serde_json::Value),
    Text(&'a str),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_debug_hides_token() {
        let client = RemoteLedgerClient::new("http://localhost:8090", Some("secret".to_string()));
        let debug = format!("{:?}", client);
        assert!(debug.contains("RemoteLedgerClient"));
        assert!(debug.contains("localhost:8090"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn test_client_strips_trailing_slash() {
        let client = RemoteLedgerClient::new("http://localhost:8090/", None);
        assert_eq!(client.base_url, "http://localhost:8090/fluree");
    }

    #[test]
    fn test_error_display() {
        let err = RemoteLedgerError::Unauthorized;
        let msg = format!("{err}");
        assert!(msg.contains("authentication failed"));
        assert!(msg.contains("fluree auth login"));

        let err = RemoteLedgerError::BadRequest("invalid query syntax".to_string());
        assert_eq!(format!("{err}"), "bad request: invalid query syntax");
    }

    #[test]
    fn test_with_refresh_config() {
        let client = RemoteLedgerClient::new("http://localhost:8090", Some("token".to_string()))
            .with_refresh(RefreshConfig {
                exchange_url: "http://localhost:8090/auth/exchange".to_string(),
                refresh_token: "rt_123".to_string(),
            });
        let debug = format!("{:?}", client);
        assert!(debug.contains("has_refresh: true"));
    }
}
