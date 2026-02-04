//! Credential extraction for signed requests (JWS/VC)
//!
//! This module provides an extractor that detects signed credentials in request bodies,
//! verifies them, and extracts the DID (identity) and unwrapped payload.
//!
//! Similar to Clojure server's `unwrap-credential` middleware.
//!
//! When the `credential` feature is disabled, this module still provides the
//! `MaybeCredential` extractor but it never verifies credentials - they pass
//! through as regular JSON bodies.

use axum::body::Bytes;
use axum::extract::FromRequest;
use axum::http::header::CONTENT_TYPE;
use axum::http::header::HeaderMap;
use axum::http::Request;
use serde_json::Value as JsonValue;

use crate::error::{Result, ServerError};

#[cfg(feature = "credential")]
use fluree_db_api::credential::{self, Input as CredentialInput};

/// Extracted credential information from a signed request
#[derive(Debug, Clone)]
pub struct ExtractedCredential {
    /// The signing identity (did:key:z6Mk...)
    pub did: String,
    /// The unwrapped payload (query/transaction JSON or SPARQL string)
    pub payload: CredentialPayload,
    /// The original raw body (for audit/logging if needed)
    pub raw_body: Bytes,
}

/// The unwrapped credential payload
#[derive(Debug, Clone)]
pub enum CredentialPayload {
    /// JSON payload (FQL queries, transactions)
    Json(JsonValue),
    /// Raw string payload (SPARQL queries)
    Sparql(String),
}

/// Request body that may or may not be a signed credential
///
/// This extractor:
/// 1. Reads the request body
/// 2. Detects if it's a signed credential (JWS format or application/jwt content-type)
/// 3. If signed: verifies and extracts DID + unwrapped payload (requires `credential` feature)
/// 4. If not signed: passes through as regular body
///
/// When the `credential` feature is disabled, credential detection is skipped
/// and all requests pass through as unsigned.
#[derive(Debug, Clone)]
pub struct MaybeCredential {
    /// Raw HTTP headers (for telemetry/tracing)
    pub headers: HeaderMap,
    /// Extracted credential info if request was signed
    pub credential: Option<ExtractedCredential>,
    /// The body to use for processing (unwrapped if signed, original if not)
    pub body: Bytes,
    /// Whether this was a SPARQL query content type
    pub is_sparql: bool,
    /// Whether this was a SPARQL UPDATE content type
    pub is_sparql_update: bool,
}

impl MaybeCredential {
    /// Get the DID if this was a signed request
    pub fn did(&self) -> Option<&str> {
        self.credential.as_ref().map(|c| c.did.as_str())
    }

    /// Check if this request was signed
    pub fn is_signed(&self) -> bool {
        self.credential.is_some()
    }

    /// Get the body as JSON (for FQL queries/transactions)
    pub fn body_json(&self) -> Result<JsonValue> {
        serde_json::from_slice(&self.body).map_err(ServerError::from)
    }

    /// Get the body as string (for SPARQL queries/updates)
    pub fn body_string(&self) -> Result<String> {
        String::from_utf8(self.body.to_vec())
            .map_err(|_| ServerError::bad_request("Invalid UTF-8 in request body"))
    }

    /// Check if this is a SPARQL UPDATE request (Content-Type: application/sparql-update)
    pub fn is_sparql_update(&self) -> bool {
        self.is_sparql_update
    }

    /// Extract credential from a request (manual extraction, same as FromRequest)
    ///
    /// This is useful when you need to inspect the request before deciding
    /// whether to forward or process locally.
    pub async fn extract(req: Request<axum::body::Body>) -> Result<Self> {
        extract_credential(req).await
    }
}

/// Check if a string looks like a JWS compact format (header.payload.signature)
#[cfg(feature = "credential")]
fn looks_like_jws(s: &str) -> bool {
    let trimmed = s.trim();
    // JWS compact format: base64url.base64url.base64url
    // Header always starts with {"alg": which encodes to eyJ
    if !trimmed.starts_with("eyJ") {
        return false;
    }
    // Should have exactly 2 dots
    trimmed.matches('.').count() == 2
}

/// Check if JSON looks like a verifiable credential or wrapped JWS
#[cfg(feature = "credential")]
fn looks_like_credential_json(json: &JsonValue) -> bool {
    // Check for VC structure
    if json.get("type").is_some() || json.get("@type").is_some() {
        if let Some(types) = json.get("type").or_else(|| json.get("@type")) {
            if let Some(arr) = types.as_array() {
                return arr.iter().any(|t| {
                    t.as_str()
                        .map(|s| s.contains("Credential"))
                        .unwrap_or(false)
                });
            }
            if let Some(s) = types.as_str() {
                return s.contains("Credential");
            }
        }
    }
    // Check for JWS wrapper
    if json.get("jws").is_some() {
        return true;
    }
    false
}

/// Extract and optionally verify credential from request
#[cfg(feature = "credential")]
async fn extract_credential(req: Request<axum::body::Body>) -> Result<MaybeCredential> {
    let headers = req.headers().clone();
    
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let is_jwt_content_type = content_type.contains("application/jwt");
    let is_sparql_query_content_type = content_type.contains("application/sparql-query");
    let is_sparql_update_content_type = content_type.contains("application/sparql-update");
    let is_sparql_content_type = is_sparql_query_content_type || is_sparql_update_content_type;

    // Read the body
    let body = axum::body::to_bytes(req.into_body(), usize::MAX)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {}", e)))?;

    let body_str = std::str::from_utf8(&body).ok();

    // Detect if this is a signed credential
    let is_jws = is_jwt_content_type
        || body_str
            .map(looks_like_jws)
            .unwrap_or(false);

    // If it's a JWS string
    if is_jws {
        let jws_str = body_str
            .ok_or_else(|| ServerError::bad_request("JWS must be valid UTF-8"))?
            .trim();

        if is_sparql_content_type {
            // SPARQL JWS - payload is raw SPARQL string
            let result = credential::verify_jws_sparql(jws_str)?;
            return Ok(MaybeCredential {
                headers,
                credential: Some(ExtractedCredential {
                    did: result.did,
                    payload: CredentialPayload::Sparql(result.payload.clone()),
                    raw_body: body.clone(),
                }),
                body: Bytes::from(result.payload),
                is_sparql: true,
                is_sparql_update: is_sparql_update_content_type,
            });
        } else {
            // JSON JWS - payload is JSON query/transaction
            let result = credential::verify_credential(CredentialInput::Jws(jws_str))?;
            let payload_bytes = serde_json::to_vec(&result.subject)
                .map_err(|e| ServerError::internal(format!("Failed to serialize payload: {}", e)))?;
            return Ok(MaybeCredential {
                headers,
                credential: Some(ExtractedCredential {
                    did: result.did,
                    payload: CredentialPayload::Json(result.subject.clone()),
                    raw_body: body.clone(),
                }),
                body: Bytes::from(payload_bytes),
                is_sparql: false,
                is_sparql_update: false,
            });
        }
    }

    // Check if it's a JSON body that might be a VC
    if let Some(body_str) = body_str {
        if let Ok(json) = serde_json::from_str::<JsonValue>(body_str) {
            if looks_like_credential_json(&json) {
                let result = credential::verify_credential(CredentialInput::Json(&json))?;
                let payload_bytes = serde_json::to_vec(&result.subject)
                    .map_err(|e| ServerError::internal(format!("Failed to serialize payload: {}", e)))?;
                return Ok(MaybeCredential {
                    headers,
                    credential: Some(ExtractedCredential {
                        did: result.did,
                        payload: CredentialPayload::Json(result.subject.clone()),
                        raw_body: body.clone(),
                    }),
                    body: Bytes::from(payload_bytes),
                    is_sparql: false,
                    is_sparql_update: false,
                });
            }
        }
    }

    // Not a credential - pass through as-is
    Ok(MaybeCredential {
        headers,
        credential: None,
        body,
        is_sparql: is_sparql_query_content_type,
        is_sparql_update: is_sparql_update_content_type,
    })
}

/// Extract body without credential verification (credential feature disabled)
#[cfg(not(feature = "credential"))]
async fn extract_credential(req: Request<axum::body::Body>) -> Result<MaybeCredential> {
    let headers = req.headers().clone();

    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let is_sparql_query_content_type = content_type.contains("application/sparql-query");
    let is_sparql_update_content_type = content_type.contains("application/sparql-update");

    // Read the body
    let body = axum::body::to_bytes(req.into_body(), usize::MAX)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {}", e)))?;

    // No credential verification - pass through as-is
    Ok(MaybeCredential {
        headers,
        credential: None,
        body,
        is_sparql: is_sparql_query_content_type,
        is_sparql_update: is_sparql_update_content_type,
    })
}

/// Axum extractor implementation
#[axum::async_trait]
impl<S> FromRequest<S> for MaybeCredential
where
    S: Send + Sync,
{
    type Rejection = ServerError;

    async fn from_request(req: Request<axum::body::Body>, _state: &S) -> std::result::Result<Self, Self::Rejection> {
        extract_credential(req).await
    }
}

#[cfg(all(test, feature = "credential"))]
mod tests {
    use super::*;

    #[test]
    fn test_looks_like_jws() {
        // Valid JWS format
        assert!(looks_like_jws("eyJhbGciOiJFZERTQSJ9.eyJmb28iOiJiYXIifQ.signature"));
        assert!(looks_like_jws("  eyJhbGciOiJFZERTQSJ9.eyJmb28iOiJiYXIifQ.signature  "));

        // Not JWS
        assert!(!looks_like_jws("{\"select\": [\"?s\"]}"));
        assert!(!looks_like_jws("SELECT ?s WHERE { ?s ?p ?o }"));
        assert!(!looks_like_jws("eyJhbGciOiJFZERTQSJ9")); // Missing dots
        assert!(!looks_like_jws("foo.bar.baz")); // Doesn't start with eyJ
    }

    #[test]
    fn test_looks_like_credential_json() {
        // VC structure
        let vc = serde_json::json!({
            "type": ["VerifiableCredential"],
            "credentialSubject": {}
        });
        assert!(looks_like_credential_json(&vc));

        // JWS wrapper
        let jws_wrapper = serde_json::json!({
            "jws": "eyJ..."
        });
        assert!(looks_like_credential_json(&jws_wrapper));

        // Regular query
        let query = serde_json::json!({
            "select": ["?s"],
            "where": {}
        });
        assert!(!looks_like_credential_json(&query));
    }
}
