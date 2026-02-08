//! HTTP client for remote ledger query/transact operations
//!
//! Used by the CLI's "track" mode to forward data operations to a remote
//! Fluree server instead of executing them locally. This is distinct from
//! `fluree-db-nameservice-sync`'s `HttpRemoteClient`, which handles only
//! nameservice ref-level operations (lookup, push, snapshot).

use reqwest::{Client, StatusCode};
use std::fmt;
use std::time::Duration;

/// HTTP client for ledger data operations against a remote Fluree server.
///
/// Supports query (FQL/SPARQL), insert, upsert, transact, ledger-info, and
/// existence checks via the server's REST API.
#[derive(Clone)]
pub struct RemoteLedgerClient {
    client: Client,
    base_url: String,
    auth_token: Option<String>,
}

impl fmt::Debug for RemoteLedgerClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteLedgerClient")
            .field("base_url", &self.base_url)
            .field("has_token", &self.auth_token.is_some())
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
    /// `base_url` is the server root (e.g., `http://localhost:8090`).
    /// Trailing slashes are stripped.
    pub fn new(base_url: &str, auth_token: Option<String>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            auth_token,
        }
    }

    fn add_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(ref token) = self.auth_token {
            req.bearer_auth(token)
        } else {
            req
        }
    }

    /// Map a non-2xx response to a `RemoteLedgerError`.
    ///
    /// Reads the response body as text to include in error messages.
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

    // =========================================================================
    // Query
    // =========================================================================

    /// Execute an FQL (JSON-LD) query against a ledger.
    ///
    /// `body` is the FQL query JSON (the `"from"` field is optional since
    /// we use the ledger-scoped endpoint).
    pub async fn query_fql(
        &self,
        ledger: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = format!("{}/{}/query", self.base_url, ledger);
        let resp = self
            .add_auth(self.client.post(&url))
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await
            .map_err(Self::map_network_error)?;

        if resp.status().is_success() {
            resp.json()
                .await
                .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()))
        } else {
            Err(Self::map_error(resp).await)
        }
    }

    /// Execute a SPARQL query against a ledger.
    ///
    /// The response is already-formatted JSON from the server (SPARQL results
    /// format or construct output). CLI formatters should treat it as
    /// pre-formatted rather than re-processing through `to_sparql_json()`.
    pub async fn query_sparql(
        &self,
        ledger: &str,
        sparql: &str,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = format!("{}/{}/query", self.base_url, ledger);
        let resp = self
            .add_auth(self.client.post(&url))
            .header("Content-Type", "application/sparql-query")
            .body(sparql.to_string())
            .send()
            .await
            .map_err(Self::map_network_error)?;

        if resp.status().is_success() {
            resp.json()
                .await
                .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()))
        } else {
            Err(Self::map_error(resp).await)
        }
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
        let url = format!("{}/{}/insert", self.base_url, ledger);
        let resp = self
            .add_auth(self.client.post(&url))
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await
            .map_err(Self::map_network_error)?;

        if resp.status().is_success() {
            resp.json()
                .await
                .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()))
        } else {
            Err(Self::map_error(resp).await)
        }
    }

    /// Insert Turtle data into a ledger.
    pub async fn insert_turtle(
        &self,
        ledger: &str,
        turtle: &str,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = format!("{}/{}/insert", self.base_url, ledger);
        let resp = self
            .add_auth(self.client.post(&url))
            .header("Content-Type", "text/turtle")
            .body(turtle.to_string())
            .send()
            .await
            .map_err(Self::map_network_error)?;

        if resp.status().is_success() {
            resp.json()
                .await
                .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()))
        } else {
            Err(Self::map_error(resp).await)
        }
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
        let url = format!("{}/{}/upsert", self.base_url, ledger);
        let resp = self
            .add_auth(self.client.post(&url))
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await
            .map_err(Self::map_network_error)?;

        if resp.status().is_success() {
            resp.json()
                .await
                .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()))
        } else {
            Err(Self::map_error(resp).await)
        }
    }

    /// Upsert Turtle data into a ledger.
    pub async fn upsert_turtle(
        &self,
        ledger: &str,
        turtle: &str,
    ) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = format!("{}/{}/upsert", self.base_url, ledger);
        let resp = self
            .add_auth(self.client.post(&url))
            .header("Content-Type", "text/turtle")
            .body(turtle.to_string())
            .send()
            .await
            .map_err(Self::map_network_error)?;

        if resp.status().is_success() {
            resp.json()
                .await
                .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()))
        } else {
            Err(Self::map_error(resp).await)
        }
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
        let url = format!("{}/{}/transact", self.base_url, ledger);
        let resp = self
            .add_auth(self.client.post(&url))
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await
            .map_err(Self::map_network_error)?;

        if resp.status().is_success() {
            resp.json()
                .await
                .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()))
        } else {
            Err(Self::map_error(resp).await)
        }
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
        let url = format!("{}/{}/transact", self.base_url, ledger);
        let resp = self
            .add_auth(self.client.post(&url))
            .header("Content-Type", "application/sparql-update")
            .body(sparql.to_string())
            .send()
            .await
            .map_err(Self::map_network_error)?;

        if resp.status().is_success() {
            resp.json()
                .await
                .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()))
        } else {
            Err(Self::map_error(resp).await)
        }
    }

    // =========================================================================
    // Ledger Info / Exists
    // =========================================================================

    /// Get ledger info from the remote server.
    pub async fn ledger_info(&self, ledger: &str) -> Result<serde_json::Value, RemoteLedgerError> {
        let url = format!("{}/fluree/ledger-info?ledger={}", self.base_url, ledger);
        let resp = self
            .add_auth(self.client.get(&url))
            .send()
            .await
            .map_err(Self::map_network_error)?;

        if resp.status().is_success() {
            resp.json()
                .await
                .map_err(|e| RemoteLedgerError::InvalidResponse(e.to_string()))
        } else {
            Err(Self::map_error(resp).await)
        }
    }

    /// Check if a ledger exists on the remote server.
    pub async fn ledger_exists(&self, ledger: &str) -> Result<bool, RemoteLedgerError> {
        let url = format!("{}/fluree/exists?ledger={}", self.base_url, ledger);
        let resp = self
            .add_auth(self.client.get(&url))
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
            // 404 could mean "ledger doesn't exist" or "unauthorized" (no existence leak)
            Ok(false)
        } else {
            Err(Self::map_error(resp).await)
        }
    }
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
        assert_eq!(client.base_url, "http://localhost:8090");
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
}
