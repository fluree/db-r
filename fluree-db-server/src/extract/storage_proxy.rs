//! Storage proxy Bearer token extraction
//!
//! Provides an Axum extractor specifically for `/fluree/storage/*` endpoints.
//! Unlike `MaybeBearer` (for events), this extractor:
//! - Always parses Bearer tokens (regardless of `events_auth_mode`)
//! - Validates using `StorageProxyConfig` (not `EventsAuthConfig`)
//! - Requires `fluree.storage.*` claims (not `fluree.events.*`)

use axum::async_trait;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use std::collections::HashSet;
use std::sync::Arc;

use super::bearer::extract_bearer_token;
use crate::error::ServerError;
use crate::state::AppState;
use fluree_db_credential::{verify_jws, EventsTokenPayload};

/// Verified principal from storage proxy Bearer token
#[derive(Debug, Clone)]
pub struct StorageProxyPrincipal {
    /// Issuer did:key (from iss claim, verified against signing key)
    pub issuer: String,
    /// Subject (from sub claim)
    pub subject: Option<String>,
    /// Resolved identity (fluree.identity ?? sub)
    pub identity: Option<String>,
    /// fluree.storage.all claim
    pub storage_all: bool,
    /// fluree.storage.ledgers claim (HashSet for O(1) lookup)
    pub storage_ledgers: HashSet<String>,
}

impl StorageProxyPrincipal {
    /// Check if principal has any storage proxy permissions
    pub fn has_storage_permissions(&self) -> bool {
        self.storage_all || !self.storage_ledgers.is_empty()
    }

    /// Check if principal is authorized for a specific ledger alias
    pub fn is_authorized_for_ledger(&self, alias: &str) -> bool {
        self.storage_all || self.storage_ledgers.contains(alias)
    }
}

/// Storage proxy Bearer token extractor.
///
/// Unlike `MaybeBearer`, this extractor:
/// - Always attempts to parse Bearer tokens (ignores `events_auth_mode`)
/// - Validates against `StorageProxyConfig` (with fallback to events trusted issuers)
/// - Requires `fluree.storage.*` claims
/// - Returns 404 if storage proxy is disabled (no existence leak)
/// - Returns 401 if token is missing/invalid
#[derive(Debug)]
pub struct StorageProxyBearer(pub StorageProxyPrincipal);

#[async_trait]
impl FromRequestParts<Arc<AppState>> for StorageProxyBearer {
    type Rejection = ServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let storage_config = state.config.storage_proxy();
        let events_auth = state.config.events_auth();

        // Check if storage proxy is enabled
        if !storage_config.enabled {
            return Err(ServerError::not_found("Storage proxy not enabled"));
        }

        // Extract Authorization header (case-insensitive, trim whitespace)
        let token = extract_bearer_token(&parts.headers)
            .ok_or_else(|| ServerError::unauthorized("Bearer token required"))?;

        // Verify JWS (embedded JWK mode)
        let verified = verify_jws(&token)
            .map_err(|e| ServerError::unauthorized(format!("Invalid token: {}", e)))?;

        // Parse combined payload
        let payload: EventsTokenPayload = serde_json::from_str(&verified.payload)
            .map_err(|e| ServerError::unauthorized(format!("Invalid claims: {}", e)))?;

        // Validate standard claims (don't require identity - that's optional for storage proxy)
        payload
            .validate(
                None, // No audience requirement for storage proxy
                &verified.did,
                false, // Identity not required
            )
            .map_err(|e| ServerError::unauthorized(e.to_string()))?;

        // Check issuer trust using StorageProxyConfig (with fallback to events auth)
        if !storage_config.is_issuer_trusted(&payload.iss, &events_auth) {
            return Err(ServerError::unauthorized("Untrusted issuer"));
        }

        // Check token grants STORAGE permissions (not events permissions)
        if !payload.has_storage_permissions() {
            return Err(ServerError::unauthorized(
                "Token lacks storage proxy permissions",
            ));
        }

        // Build principal
        let principal = StorageProxyPrincipal {
            issuer: payload.iss.clone(),
            subject: payload.sub.clone(),
            identity: payload.resolve_identity(),
            storage_all: payload.storage_all.unwrap_or(false),
            storage_ledgers: payload
                .storage_ledgers
                .unwrap_or_default()
                .into_iter()
                .collect(),
        };

        Ok(StorageProxyBearer(principal))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_proxy_principal_fields() {
        let principal = StorageProxyPrincipal {
            issuer: "did:key:z6MkexampleExampleExampleExampleExampleExam".to_string(),
            subject: Some("peer@example.com".to_string()),
            identity: Some("ex:PeerServiceAccount".to_string()),
            storage_all: false,
            storage_ledgers: vec!["books:main".to_string(), "users:main".to_string()]
                .into_iter()
                .collect(),
        };

        assert!(principal.has_storage_permissions());
        assert!(principal.is_authorized_for_ledger("books:main"));
        assert!(principal.is_authorized_for_ledger("users:main"));
        assert!(!principal.is_authorized_for_ledger("other:main"));
    }

    #[test]
    fn test_storage_all_authorization() {
        let principal = StorageProxyPrincipal {
            issuer: "did:key:z6MkexampleExampleExampleExampleExampleExam".to_string(),
            subject: Some("peer@example.com".to_string()),
            identity: None,
            storage_all: true,
            storage_ledgers: HashSet::new(),
        };

        assert!(principal.has_storage_permissions());
        assert!(principal.is_authorized_for_ledger("any:ledger"));
        assert!(principal.is_authorized_for_ledger("books:main"));
    }

    #[test]
    fn test_no_storage_permissions() {
        let principal = StorageProxyPrincipal {
            issuer: "did:key:z6MkexampleExampleExampleExampleExampleExam".to_string(),
            subject: None,
            identity: None,
            storage_all: false,
            storage_ledgers: HashSet::new(),
        };

        assert!(!principal.has_storage_permissions());
        assert!(!principal.is_authorized_for_ledger("any:ledger"));
    }
}
