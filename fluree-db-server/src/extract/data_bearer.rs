//! Bearer token extraction for data API authentication (query/transact/info/exists).
//!
//! This extractor verifies JWT/JWS Bearer tokens and yields a `DataPrincipal`
//! containing ledger read/write scopes and policy identity.
//!
//! Signed requests (JWS/VC in request body) are handled separately by
//! [`MaybeCredential`](crate::extract::MaybeCredential). Data endpoints can accept
//! either mechanism depending on `data_auth.mode`.

use axum::async_trait;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use std::collections::HashSet;
use std::sync::Arc;

use crate::config::{DataAuthMode, ServerConfig};
use crate::error::ServerError;
use crate::state::AppState;
use fluree_db_credential::{verify_jws, EventsTokenPayload};

/// Verified principal from a data API Bearer token
#[derive(Debug, Clone)]
pub struct DataPrincipal {
    /// Issuer did:key (from iss claim, verified against signing key)
    pub issuer: String,
    /// Subject (from sub claim)
    pub subject: Option<String>,
    /// Resolved identity (fluree.identity ?? sub)
    pub identity: Option<String>,
    /// Read access to all ledgers
    pub read_all: bool,
    /// Read access to specific ledgers (HashSet for O(1) lookup)
    pub read_ledgers: HashSet<String>,
    /// Write access to all ledgers
    pub write_all: bool,
    /// Write access to specific ledgers (HashSet for O(1) lookup)
    pub write_ledgers: HashSet<String>,
}

impl DataPrincipal {
    pub fn can_read(&self, ledger_address: &str) -> bool {
        self.read_all || self.read_ledgers.contains(ledger_address)
    }

    pub fn can_write(&self, ledger_address: &str) -> bool {
        self.write_all || self.write_ledgers.contains(ledger_address)
    }
}

/// Optional/required data API Bearer token extractor.
#[derive(Debug)]
pub struct MaybeDataBearer(pub Option<DataPrincipal>);

#[async_trait]
impl FromRequestParts<Arc<AppState>> for MaybeDataBearer {
    type Rejection = ServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let config = state.config.data_auth();

        // In None mode, ignore token entirely
        if config.mode == DataAuthMode::None {
            return Ok(MaybeDataBearer(None));
        }

        // Extract Authorization header (case-insensitive, trim whitespace)
        let token = match super::extract_bearer_token(&parts.headers) {
            Some(t) => t,
            None => {
                return match config.mode {
                    DataAuthMode::Required => {
                        Err(ServerError::unauthorized("Bearer token required"))
                    }
                    _ => Ok(MaybeDataBearer(None)),
                };
            }
        };

        verify_data_token(&token, &state.config).await
    }
}

/// Verify token and build `DataPrincipal`.
async fn verify_data_token(
    token: &str,
    server_config: &ServerConfig,
) -> Result<MaybeDataBearer, ServerError> {
    let config = server_config.data_auth();

    // 1) Verify JWS (embedded JWK mode)
    let verified = verify_jws(token)
        .map_err(|e| ServerError::unauthorized(format!("Invalid token: {}", e)))?;

    // 2) Parse combined payload (re-uses EventsTokenPayload for shared claim parsing)
    let payload: EventsTokenPayload = serde_json::from_str(&verified.payload)
        .map_err(|e| ServerError::unauthorized(format!("Invalid claims: {}", e)))?;

    // 3) Validate standard claims (aud, exp, iss match)
    payload
        .validate(
            config.audience.as_deref(),
            &verified.did,
            false, // identity not strictly required; policy can be root/no-identity
        )
        .map_err(|e| ServerError::unauthorized(e.to_string()))?;

    // 4) Check issuer trust (unless insecure flag)
    if !config.is_issuer_trusted(&payload.iss) {
        return Err(ServerError::unauthorized("Untrusted issuer"));
    }

    // 5) Require some data permissions
    if !payload.has_ledger_read_permissions() && !payload.has_ledger_write_permissions() {
        return Err(ServerError::unauthorized("token authorizes no resources"));
    }

    let principal = DataPrincipal {
        issuer: payload.iss.clone(),
        subject: payload.sub.clone(),
        identity: payload.resolve_identity(),
        // Read: use explicit ledger.read.* if present, else fall back to storage.* (handled in helper)
        read_all: payload.ledger_read_all.unwrap_or(false) || payload.storage_all.unwrap_or(false),
        read_ledgers: payload
            .ledger_read_ledgers
            .clone()
            .or_else(|| payload.storage_ledgers.clone())
            .unwrap_or_default()
            .into_iter()
            .collect(),
        write_all: payload.ledger_write_all.unwrap_or(false),
        write_ledgers: payload
            .ledger_write_ledgers
            .unwrap_or_default()
            .into_iter()
            .collect(),
    };

    Ok(MaybeDataBearer(Some(principal)))
}
