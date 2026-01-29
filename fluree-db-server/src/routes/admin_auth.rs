//! Bearer token authentication for admin endpoints
//!
//! Provides middleware to verify JWT/JWS Bearer tokens for admin endpoints
//! (/fluree/create, /fluree/drop). Reuses the same token format and
//! verification as other authenticated endpoints.

use crate::config::{AdminAuthConfig, AdminAuthMode};
use crate::error::ServerError;
use crate::extract::extract_bearer_token;
use crate::state::AppState;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use fluree_db_credential::{verify_jws, EventsTokenPayload};
use std::sync::Arc;

/// Verified principal from admin Bearer token
#[derive(Debug, Clone)]
pub struct AdminPrincipal {
    /// Issuer did:key (from iss claim, verified against signing key)
    pub issuer: String,
    /// Subject (from sub claim)
    pub subject: Option<String>,
    /// Resolved identity (fluree.identity ?? sub)
    pub identity: Option<String>,
}

/// Middleware to validate admin Bearer tokens.
///
/// When admin auth mode is `Required`, all requests to admin endpoints
/// must have a valid Bearer token from a trusted issuer.
///
/// When mode is `None`, requests pass through without authentication.
pub async fn require_admin_token(
    State(state): State<Arc<AppState>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let admin_auth = state.config.admin_auth();
    let events_auth = state.config.events_auth();

    // Skip if auth not required
    if admin_auth.mode == AdminAuthMode::None {
        return next.run(request).await;
    }

    // Extract Bearer token from Authorization header
    let token = match extract_bearer_token(request.headers()) {
        Some(t) => t,
        None => {
            tracing::debug!("Admin request missing Bearer token");
            return (
                StatusCode::UNAUTHORIZED,
                "Bearer token required for admin endpoint",
            )
                .into_response();
        }
    };

    // Verify token and build principal
    let principal = match verify_admin_token(&token, &admin_auth, &events_auth) {
        Ok(p) => p,
        Err(e) => {
            // Log detailed error but return generic message to client
            tracing::warn!(error = %e, "Admin token verification failed");
            return (StatusCode::UNAUTHORIZED, "Invalid or unauthorized token").into_response();
        }
    };

    tracing::debug!(
        issuer = %principal.issuer,
        identity = ?principal.identity,
        "Admin token verified"
    );

    // Store principal in request extensions for handlers to access if needed
    request.extensions_mut().insert(principal);

    // Continue to the handler
    next.run(request).await
}

/// Verify admin token and build principal
fn verify_admin_token(
    token: &str,
    admin_auth: &AdminAuthConfig,
    events_auth: &crate::config::EventsAuthConfig,
) -> Result<AdminPrincipal, ServerError> {
    // 1. Verify JWS (embedded JWK mode)
    let verified = verify_jws(token)
        .map_err(|e| ServerError::unauthorized(format!("Invalid token: {}", e)))?;

    // 2. Parse payload (reuse EventsTokenPayload for standard claims)
    let payload: EventsTokenPayload = serde_json::from_str(&verified.payload)
        .map_err(|e| ServerError::unauthorized(format!("Invalid claims: {}", e)))?;

    // 3. Validate standard claims (exp, iss matches signing key)
    // We don't require specific audience for admin tokens
    payload
        .validate(None, &verified.did, false)
        .map_err(|e| ServerError::unauthorized(e.to_string()))?;

    // 4. Check issuer trust
    if !admin_auth.is_issuer_trusted(&payload.iss, events_auth) {
        return Err(ServerError::unauthorized("Untrusted issuer"));
    }

    // 5. Build principal
    let identity = payload.resolve_identity();
    Ok(AdminPrincipal {
        issuer: payload.iss,
        subject: payload.sub,
        identity,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EventsAuthConfig;
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use ed25519_dalek::{Signer, SigningKey};
    use fluree_db_credential::did_from_pubkey;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn create_test_token(claims: &serde_json::Value, signing_key: &SigningKey) -> String {
        let pubkey = signing_key.verifying_key().to_bytes();
        let pubkey_b64 = URL_SAFE_NO_PAD.encode(&pubkey);

        let header = serde_json::json!({
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
        let signature = signing_key.sign(signing_input.as_bytes());
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes());

        format!("{}.{}.{}", header_b64, payload_b64, sig_b64)
    }

    #[test]
    fn test_verify_admin_token_trusted_issuer() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        let claims = serde_json::json!({
            "iss": did,
            "sub": "admin@example.com",
            "exp": now_secs() + 3600
        });

        let token = create_test_token(&claims, &signing_key);

        let admin_auth = AdminAuthConfig {
            mode: AdminAuthMode::Required,
            trusted_issuers: vec![did.clone()],
            insecure_accept_any_issuer: false,
        };

        let events_auth = EventsAuthConfig::default();

        let result = verify_admin_token(&token, &admin_auth, &events_auth);
        assert!(result.is_ok());

        let principal = result.unwrap();
        assert_eq!(principal.issuer, did);
        assert_eq!(principal.subject, Some("admin@example.com".to_string()));
    }

    #[test]
    fn test_verify_admin_token_untrusted_issuer() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        let claims = serde_json::json!({
            "iss": did,
            "sub": "admin@example.com",
            "exp": now_secs() + 3600
        });

        let token = create_test_token(&claims, &signing_key);

        let admin_auth = AdminAuthConfig {
            mode: AdminAuthMode::Required,
            trusted_issuers: vec!["did:key:z6MkOTHER".to_string()],
            insecure_accept_any_issuer: false,
        };

        let events_auth = EventsAuthConfig::default();

        let result = verify_admin_token(&token, &admin_auth, &events_auth);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Untrusted issuer"));
    }

    #[test]
    fn test_verify_admin_token_fallback_to_events_issuers() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        let claims = serde_json::json!({
            "iss": did,
            "sub": "admin@example.com",
            "exp": now_secs() + 3600
        });

        let token = create_test_token(&claims, &signing_key);

        // Admin has no trusted issuers, but events_auth does
        let admin_auth = AdminAuthConfig {
            mode: AdminAuthMode::Required,
            trusted_issuers: vec![],
            insecure_accept_any_issuer: false,
        };

        let events_auth = EventsAuthConfig {
            trusted_issuers: vec![did.clone()],
            ..Default::default()
        };

        let result = verify_admin_token(&token, &admin_auth, &events_auth);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_admin_token_insecure_mode() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        let claims = serde_json::json!({
            "iss": did,
            "sub": "admin@example.com",
            "exp": now_secs() + 3600
        });

        let token = create_test_token(&claims, &signing_key);

        let admin_auth = AdminAuthConfig {
            mode: AdminAuthMode::Required,
            trusted_issuers: vec![],
            insecure_accept_any_issuer: true,
        };

        let events_auth = EventsAuthConfig::default();

        let result = verify_admin_token(&token, &admin_auth, &events_auth);
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_admin_token_expired() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        let did = did_from_pubkey(&pubkey);

        let claims = serde_json::json!({
            "iss": did,
            "sub": "admin@example.com",
            "exp": now_secs() - 120 // Expired 2 minutes ago
        });

        let token = create_test_token(&claims, &signing_key);

        let admin_auth = AdminAuthConfig {
            mode: AdminAuthMode::Required,
            trusted_issuers: vec![did.clone()],
            insecure_accept_any_issuer: false,
        };

        let events_auth = EventsAuthConfig::default();

        let result = verify_admin_token(&token, &admin_auth, &events_auth);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expired"));
    }
}
