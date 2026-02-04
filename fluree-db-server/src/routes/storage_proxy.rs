//! Storage proxy endpoints for peer mode
//!
//! These endpoints allow query peers to access storage through the transaction
//! server instead of directly accessing the storage backend. This is useful when
//! peers don't have direct storage credentials.
//!
//! # Endpoints
//! - `GET /fluree/storage/ns/{alias}` - Fetch nameservice record for a ledger
//! - `POST /fluree/storage/block` - Fetch a block by address
//!
//! # Authorization
//! All endpoints require a Bearer token with storage proxy permissions:
//! - `fluree.storage.all: true` - Access all ledgers
//! - `fluree.storage.ledgers: [...]` - Access specific ledgers
//!
//! # Security
//! - Unauthorized requests return 404 (no existence leak)
//! - VG artifacts return 404 in v1 (ledger-only scope)

use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::Response,
    Json,
};
use fluree_db_api::{policy_builder, NameService, QueryConnectionOptions, StorageRead};
use fluree_db_core::flake::Flake;
use fluree_db_core::{NoOverlay, OverlayProvider, Tracker};
use fluree_db_query::QueryPolicyEnforcer;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::error::ServerError;
use crate::extract::{StorageProxyBearer, StorageProxyPrincipal};
use crate::state::AppState;
use fluree_db_core::serde::flakes_transport::{encode_flakes, TransportFlake};

// ============================================================================
// Address Context Parsing
// ============================================================================

/// Context inferred from a storage address.
///
/// Addresses encode their context in the path structure:
/// - Commits: `fluree:file://{alias}/commit/...`
/// - Indexes: `fluree:file://{ledger}/{branch}/index/...`
/// - VG artifacts: `fluree:file://virtual-graphs/{name}/{branch}/...`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddressContext {
    /// Ledger commit: `fluree:file://{alias}/commit/...`
    LedgerCommit { alias: String },
    /// Ledger index: `fluree:file://{ledger}/{branch}/index/...`
    LedgerIndex { ledger: String, branch: String },
    /// VG artifact: `fluree:file://virtual-graphs/{name}/{branch}/...`
    VgArtifact { name: String, branch: String },
    /// Unknown format
    Unknown,
}

impl AddressContext {
    /// Get the ledger alias if this is a ledger address
    pub fn ledger_alias(&self) -> Option<String> {
        match self {
            AddressContext::LedgerCommit { alias } => Some(alias.clone()),
            AddressContext::LedgerIndex { ledger, branch } => Some(format!("{}:{}", ledger, branch)),
            _ => None,
        }
    }

    /// Check if this is a VG artifact address
    #[cfg(test)]
    pub fn is_vg(&self) -> bool {
        matches!(self, AddressContext::VgArtifact { .. })
    }
}

/// Parse address to determine context and authorization scope.
///
/// This function is security-critical: it determines what ledger/VG
/// an address belongs to for authorization decisions.
///
/// # Examples
/// ```ignore
/// parse_address_context("fluree:file://books:main/commit/abc.json")
///     // => LedgerCommit { alias: "books:main" }
///
/// parse_address_context("fluree:file://books/main/index/abc.json")
///     // => LedgerIndex { ledger: "books", branch: "main" }
///
/// parse_address_context("fluree:file://virtual-graphs/search/main/snapshot.bin")
///     // => VgArtifact { name: "search", branch: "main" }
/// ```
pub fn parse_address_context(address: &str) -> AddressContext {
    let Some(path) = address.strip_prefix("fluree:file://") else {
        return AddressContext::Unknown;
    };

    // VG format: virtual-graphs/{name}/{branch}/...
    if let Some(vg_path) = path.strip_prefix("virtual-graphs/") {
        let parts: Vec<&str> = vg_path.splitn(3, '/').collect();
        if parts.len() >= 2 && !parts[0].is_empty() && !parts[1].is_empty() {
            return AddressContext::VgArtifact {
                name: parts[0].to_string(),
                branch: parts[1].to_string(),
            };
        }
        return AddressContext::Unknown;
    }

    // Commit format: {alias}/commit/... (alias may contain :)
    if path.contains("/commit/") {
        if let Some(alias) = path.split("/commit/").next() {
            if !alias.is_empty() {
                // New canonical layout uses `ledger/branch/commit/...` (no ':').
                // Convert to `ledger:branch` for authorization + ledger_cached lookup.
                if !alias.contains(':') && alias.contains('/') {
                    let parts: Vec<&str> = alias.splitn(2, '/').collect();
                    if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
                        return AddressContext::LedgerCommit {
                            alias: format!("{}:{}", parts[0], parts[1]),
                        };
                    }
                }
                return AddressContext::LedgerCommit {
                    alias: alias.to_string(),
                };
            }
        }
        return AddressContext::Unknown;
    }

    // Index format: {ledger}/{branch}/index/... (colon normalized to /)
    if path.contains("/index/") {
        if let Some(prefix) = path.split("/index/").next() {
            let parts: Vec<&str> = prefix.splitn(2, '/').collect();
            if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
                return AddressContext::LedgerIndex {
                    ledger: parts[0].to_string(),
                    branch: parts[1].to_string(),
                };
            }
        }
        return AddressContext::Unknown;
    }

    // Address doesn't match any known format
    AddressContext::Unknown
}

/// Check if principal is authorized for this address.
///
/// # Returns
/// - `true` if the principal can access this address
/// - `false` if unauthorized (should return 404, not 403)
fn authorize_address(principal: &StorageProxyPrincipal, context: &AddressContext) -> bool {
    match context {
        AddressContext::LedgerCommit { alias } => principal.is_authorized_for_ledger(alias),
        AddressContext::LedgerIndex { ledger, branch } => {
            let alias = format!("{}:{}", ledger, branch);
            principal.is_authorized_for_ledger(&alias)
        }
        AddressContext::VgArtifact { .. } => {
            // VG artifacts not authorized in v1 (ledger-only scope)
            // Add fluree.storage.vgs claim in v2 if needed
            false
        }
        AddressContext::Unknown => {
            // Unknown address formats are never authorized (security)
            false
        }
    }
}

// ============================================================================
// Filterability Detection
// ============================================================================

/// Result of attempting to parse block as filterable leaf
#[derive(Debug)]
pub enum FilterableBlock {
    /// Successfully parsed as leaf - contains the flakes
    Leaf(Vec<Flake>),
    /// VG artifact (detected from address context)
    VgArtifact,
    /// Not a filterable leaf (branch, unknown, or parse failed)
    NotFilterable,
}

/// Attempt to parse block as a filterable leaf
///
/// Uses the existing leaf parser from fluree-db-core. If parsing succeeds,
/// the block is filterable. This avoids having to guess JSON structure.
///
/// # Fast paths
/// - VG addresses return immediately (no parse attempt)
/// - octet-stream requests skip this entirely (called from handler)
fn try_parse_as_leaf(
    bytes: Vec<u8>,
    address_context: &AddressContext,
) -> FilterableBlock {
    // Fast path: VG artifacts detected from address pattern
    if matches!(address_context, AddressContext::VgArtifact { .. }) {
        return FilterableBlock::VgArtifact;
    }

    // Only ledger addresses (commit or index) are candidates for filtering
    if address_context.ledger_alias().is_none() {
        return FilterableBlock::NotFilterable;
    }

    // Attempt to parse as leaf using existing core parser
    match fluree_db_core::serde::parse_leaf_node(bytes) {
        Ok(flakes) => FilterableBlock::Leaf(flakes),
        Err(_) => {
            // Parse failed - could be branch, corrupted, or different format
            // Not an error - just means this block isn't filterable
            FilterableBlock::NotFilterable
        }
    }
}

// ============================================================================
// Content Negotiation
// ============================================================================

/// Accepted response formats for block fetching
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AcceptFormat {
    /// Raw bytes (application/octet-stream) - default
    OctetStream,
    /// Binary flakes (application/x-fluree-flakes)
    FlakesBinary,
    /// JSON flakes (application/x-fluree-flakes+json) - debug only
    FlakesJson,
}

/// Parse Accept header with defined precedence:
/// 1. `application/x-fluree-flakes+json` (debug JSON format)
/// 2. `application/x-fluree-flakes` (binary format)
/// 3. Everything else → `application/octet-stream` (raw bytes)
///
/// Note: Uses `contains()` for simplicity. Multiple Accept values
/// are handled by first match in precedence order.
fn parse_accept_header(headers: &HeaderMap) -> AcceptFormat {
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream");

    // Check in precedence order (JSON > binary > raw)
    if accept.contains("application/x-fluree-flakes+json") {
        AcceptFormat::FlakesJson
    } else if accept.contains("application/x-fluree-flakes") {
        AcceptFormat::FlakesBinary
    } else {
        AcceptFormat::OctetStream
    }
}

// ============================================================================
// Response Builders
// ============================================================================

fn build_raw_response(bytes: Vec<u8>) -> Result<Response, ServerError> {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(bytes))
        .map_err(|e| ServerError::internal(e.to_string()))
}

/// Build binary flakes response with optional debug headers
fn build_binary_flakes_response(
    flakes: &[Flake],
    policy_applied: bool,
    emit_debug_headers: bool,
) -> Result<Response, ServerError> {
    let bytes = encode_flakes(flakes).map_err(|e| ServerError::internal(e.to_string()))?;

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-fluree-flakes");

    if emit_debug_headers {
        builder = builder
            .header("X-Fluree-Block-Type", "ledger-leaf")
            .header("X-Fluree-Policy-Applied", if policy_applied { "true" } else { "false" });
    }

    builder
        .body(Body::from(bytes))
        .map_err(|e| ServerError::internal(e.to_string()))
}

/// Build JSON flakes response with optional debug headers
fn build_json_flakes_response(
    flakes: &[Flake],
    policy_applied: bool,
    emit_debug_headers: bool,
) -> Result<Response, ServerError> {
    // Convert to transport format for JSON serialization
    let transport: Vec<TransportFlake> = flakes.iter().map(TransportFlake::from).collect();

    let json =
        serde_json::to_vec(&transport).map_err(|e| ServerError::internal(e.to_string()))?;

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-fluree-flakes+json");

    if emit_debug_headers {
        builder = builder
            .header("X-Fluree-Block-Type", "ledger-leaf")
            .header("X-Fluree-Policy-Applied", if policy_applied { "true" } else { "false" });
    }

    builder
        .body(Body::from(json))
        .map_err(|e| ServerError::internal(e.to_string()))
}

// ============================================================================
// Request/Response Types
// ============================================================================

/// Response for nameservice record endpoint
#[derive(Debug, Clone, Serialize)]
pub struct NsRecordResponse {
    pub alias: String,
    pub branch: String,
    pub commit_address: Option<String>,
    pub commit_t: i64,
    pub index_address: Option<String>,
    pub index_t: i64,
    pub retracted: bool,
}

/// Request body for block fetch endpoint
#[derive(Debug, Clone, Deserialize)]
pub struct BlockRequest {
    pub address: String,
}

// ============================================================================
// Handlers
// ============================================================================

/// GET /fluree/storage/ns/{alias}
///
/// Returns the nameservice record for a ledger.
/// Requires Bearer token with access to the requested alias.
///
/// Note: The `StorageProxyBearer` extractor handles:
/// - Checking if storage proxy is enabled (returns 404 if not)
/// - Validating the Bearer token
/// - Verifying issuer trust against StorageProxyConfig
/// - Checking that token has storage permissions
pub async fn get_ns_record(
    State(state): State<Arc<AppState>>,
    Path(alias): Path<String>,
    StorageProxyBearer(principal): StorageProxyBearer,
) -> Result<Json<NsRecordResponse>, ServerError> {
    // Check authorization for this specific ledger
    if !principal.is_authorized_for_ledger(&alias) {
        // Return 404 for unauthorized (no existence leak)
        return Err(ServerError::not_found("Ledger not found"));
    }

    // Look up the nameservice record
    // Storage proxy is only enabled on transaction servers (validated in config)
    let ns_record = state
        .fluree
        .as_file()
        .nameservice()
        .lookup(&alias)
        .await
        .map_err(|e| ServerError::internal(format!("Nameservice lookup failed: {}", e)))?
        .ok_or_else(|| ServerError::not_found("Ledger not found"))?;

    Ok(Json(NsRecordResponse {
        alias: ns_record.alias.clone(),
        branch: ns_record.branch.clone(),
        commit_address: ns_record.commit_address.clone(),
        commit_t: ns_record.commit_t,
        index_address: ns_record.index_address.clone(),
        index_t: ns_record.index_t,
        retracted: ns_record.retracted,
    }))
}

/// POST /fluree/storage/block
///
/// Fetches a block (branch or leaf) from storage.
/// Ledger context is inferred from the address.
///
/// # Content Negotiation
/// - `application/octet-stream`: raw bytes (default, unchanged)
/// - `application/x-fluree-flakes`: binary CBOR flakes format (policy-filtered)
/// - `application/x-fluree-flakes+json`: JSON flakes format (debug only)
///
/// Note: The `StorageProxyBearer` extractor handles:
/// - Checking if storage proxy is enabled (returns 404 if not)
/// - Validating the Bearer token
/// - Verifying issuer trust against StorageProxyConfig
/// - Checking that token has storage permissions
pub async fn get_block(
    State(state): State<Arc<AppState>>,
    StorageProxyBearer(principal): StorageProxyBearer,
    headers: HeaderMap,
    Json(body): Json<BlockRequest>,
) -> Result<Response, ServerError> {
    // Parse address context
    let context = parse_address_context(&body.address);

    // Authorize based on inferred context
    if !authorize_address(&principal, &context) {
        // Return 404 for unauthorized (no existence leak)
        return Err(ServerError::not_found("Block not found"));
    }

    // Read raw bytes from storage
    // Storage proxy is only enabled on transaction servers (validated in config)
    let bytes = state
        .fluree
        .as_file()
        .storage()
        .read_bytes(&body.address)
        .await
        .map_err(|e| {
            // Check if it's a NotFound error
            if matches!(e, fluree_db_core::Error::NotFound(_)) {
                ServerError::not_found("Block not found")
            } else {
                ServerError::internal(format!("Storage read failed: {}", e))
            }
        })?;

    // Parse Accept header
    let accept = parse_accept_header(&headers);

    // For octet-stream, return raw bytes immediately (fast path)
    if matches!(accept, AcceptFormat::OctetStream) {
        return build_raw_response(bytes);
    }

    // For flakes formats, we need to parse the leaf block
    // Get ledger alias for loading the interner
    let alias = context.ledger_alias().ok_or_else(|| {
        // Non-ledger address (VG or unknown) with flakes format = 406
        ServerError::not_acceptable("Flakes format only available for ledger blocks")
    })?;

    // Get cached ledger state
    let fluree = state.fluree.as_file();
    let handle = fluree.ledger_cached(&alias).await.map_err(|e| {
        ServerError::internal(format!("Ledger load failed: {}", e))
    })?;

    // Get snapshot (brief lock, then released)
    let snapshot = handle.snapshot().await;

    // Attempt to parse as leaf using actual core parser
    let parsed = try_parse_as_leaf(bytes, &context);

    let flakes = match parsed {
        FilterableBlock::Leaf(flakes) => flakes,
        FilterableBlock::VgArtifact | FilterableBlock::NotFilterable => {
            return Err(ServerError::not_acceptable(
                "Flakes format only available for ledger leaf blocks",
            ));
        }
    };

    // Get storage proxy config for defaults and debug headers
    let proxy_config = state.config.storage_proxy();

    // Compute effective identity: token claim → config default → none
    let effective_identity = principal
        .identity
        .clone()
        .or_else(|| proxy_config.default_identity.clone());

    // Compute effective policy class: config default (token claim not yet supported)
    let effective_policy_class = proxy_config.default_policy_class.clone();

    // Apply policy filtering if we have identity or policy class
    let (filtered, policy_applied) = if effective_identity.is_some() || effective_policy_class.is_some() {
        // Build policy context from effective identity and policy class
        let opts = QueryConnectionOptions {
            identity: effective_identity.clone(),
            policy_class: effective_policy_class.clone().map(|c| vec![c]),
            ..Default::default()
        };

        // Build policy context using the ledger's db
        let overlay: &dyn OverlayProvider = &NoOverlay;
        let to_t = snapshot.db.t;

        let policy_ctx = policy_builder::build_policy_context_from_opts(
            &snapshot.db,
            overlay,
            None, // No novelty needed for filtering existing flakes
            to_t,
            &opts,
        )
        .await
        .map_err(|e| ServerError::internal(format!("Policy context build failed: {}", e)))?;

        // Check if this is a root policy (no restrictions)
        if policy_ctx.wrapper().is_root() {
            tracing::debug!(
                address = %body.address,
                identity = ?effective_identity,
                policy_class = ?effective_policy_class,
                "Root policy - returning all flakes unfiltered"
            );
            (flakes, false) // No filtering occurred
        } else {
            // Create enforcer and filter flakes
            let enforcer = QueryPolicyEnforcer::new(Arc::new(policy_ctx));
            let tracker = Tracker::disabled();

            let original_count = flakes.len();
            let filtered = enforcer
                .filter_flakes_for_graph(&snapshot.db, overlay, to_t, &tracker, flakes)
                .await
                .map_err(|e| ServerError::internal(format!("Policy filtering failed: {}", e)))?;

            tracing::debug!(
                address = %body.address,
                identity = ?effective_identity,
                policy_class = ?effective_policy_class,
                original_count = original_count,
                filtered_count = filtered.len(),
                "Applied policy filtering"
            );
            (filtered, true) // Filtering was applied
        }
    } else {
        // No identity and no policy class - return all flakes (equivalent to root policy)
        tracing::debug!(
            address = %body.address,
            flake_count = flakes.len(),
            "No identity or policy class - returning all flakes unfiltered"
        );
        (flakes, false) // No filtering occurred
    };

    // Encode response with appropriate headers
    let emit_debug_headers = proxy_config.emit_debug_headers;
    match accept {
        AcceptFormat::FlakesBinary => {
            build_binary_flakes_response(&filtered, policy_applied, emit_debug_headers)
        }
        AcceptFormat::FlakesJson => {
            build_json_flakes_response(&filtered, policy_applied, emit_debug_headers)
        }
        AcceptFormat::OctetStream => unreachable!(), // handled above
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    

    fn make_principal(storage_all: bool, storage_ledgers: Vec<&str>) -> StorageProxyPrincipal {
        StorageProxyPrincipal {
            issuer: "did:key:z6Mk...".to_string(),
            subject: None,
            identity: None,
            storage_all,
            storage_ledgers: storage_ledgers.into_iter().map(String::from).collect(),
        }
    }

    #[test]
    fn test_parse_commit_address() {
        let addr = "fluree:file://books:main/commit/abc123.json";
        let ctx = parse_address_context(addr);
        assert_eq!(
            ctx,
            AddressContext::LedgerCommit {
                alias: "books:main".to_string()
            }
        );
        assert_eq!(ctx.ledger_alias(), Some("books:main".to_string()));
    }

    #[test]
    fn test_parse_index_address() {
        let addr = "fluree:file://books/main/index/def456.json";
        let ctx = parse_address_context(addr);
        assert_eq!(
            ctx,
            AddressContext::LedgerIndex {
                ledger: "books".to_string(),
                branch: "main".to_string()
            }
        );
        assert_eq!(ctx.ledger_alias(), Some("books:main".to_string()));
    }

    #[test]
    fn test_parse_vg_address() {
        let addr = "fluree:file://virtual-graphs/search/main/snapshot.bin";
        let ctx = parse_address_context(addr);
        assert_eq!(
            ctx,
            AddressContext::VgArtifact {
                name: "search".to_string(),
                branch: "main".to_string()
            }
        );
        assert!(ctx.is_vg());
        assert_eq!(ctx.ledger_alias(), None);
    }

    #[test]
    fn test_parse_unknown_address() {
        let addr = "fluree:file://something/else";
        let ctx = parse_address_context(addr);
        assert_eq!(ctx, AddressContext::Unknown);
    }

    #[test]
    fn test_parse_non_fluree_address() {
        let addr = "s3://bucket/key";
        let ctx = parse_address_context(addr);
        assert_eq!(ctx, AddressContext::Unknown);
    }

    #[test]
    fn test_parse_empty_alias() {
        let addr = "fluree:file:///commit/abc.json";
        let ctx = parse_address_context(addr);
        assert_eq!(ctx, AddressContext::Unknown);
    }

    #[test]
    fn test_parse_empty_ledger() {
        let addr = "fluree:file:///main/index/abc.json";
        let ctx = parse_address_context(addr);
        assert_eq!(ctx, AddressContext::Unknown);
    }

    #[test]
    fn test_authorize_commit_allowed() {
        let principal = make_principal(false, vec!["books:main"]);
        let ctx = AddressContext::LedgerCommit {
            alias: "books:main".to_string(),
        };
        assert!(authorize_address(&principal, &ctx));
    }

    #[test]
    fn test_authorize_commit_denied() {
        let principal = make_principal(false, vec!["other:main"]);
        let ctx = AddressContext::LedgerCommit {
            alias: "books:main".to_string(),
        };
        assert!(!authorize_address(&principal, &ctx));
    }

    #[test]
    fn test_authorize_index_allowed() {
        let principal = make_principal(false, vec!["books:main"]);
        let ctx = AddressContext::LedgerIndex {
            ledger: "books".to_string(),
            branch: "main".to_string(),
        };
        assert!(authorize_address(&principal, &ctx));
    }

    #[test]
    fn test_authorize_storage_all() {
        let principal = make_principal(true, vec![]);
        let ctx = AddressContext::LedgerCommit {
            alias: "any:ledger".to_string(),
        };
        assert!(authorize_address(&principal, &ctx));
    }

    #[test]
    fn test_authorize_vg_denied_v1() {
        // VG artifacts are not authorized in v1, even with storage_all
        let principal = make_principal(true, vec![]);
        let ctx = AddressContext::VgArtifact {
            name: "search".to_string(),
            branch: "main".to_string(),
        };
        assert!(!authorize_address(&principal, &ctx));
    }

    #[test]
    fn test_authorize_unknown_denied() {
        let principal = make_principal(true, vec![]);
        let ctx = AddressContext::Unknown;
        assert!(!authorize_address(&principal, &ctx));
    }
}
