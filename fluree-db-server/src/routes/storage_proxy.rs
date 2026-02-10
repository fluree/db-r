//! Storage proxy endpoints for peer mode
//!
//! These endpoints allow query peers to access storage through the transaction
//! server instead of directly accessing the storage backend. This is useful when
//! peers don't have direct storage credentials.
//!
//! # Endpoints
//! - `GET /fluree/storage/ns/{ledger_id}` - Fetch nameservice record for a ledger
//! - `POST /fluree/storage/block` - Fetch a block by address
//!
//! # Authorization
//! All endpoints require a Bearer token with storage proxy permissions:
//! - `fluree.storage.all: true` - Access all ledgers
//! - `fluree.storage.ledgers: [...]` - Access specific ledgers
//!
//! # Security
//! - Unauthorized requests return 404 (no existence leak)
//! - Graph source artifacts return 404 in v1 (ledger-only scope)

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::Response,
    Json,
};
use fluree_db_api::block_fetch::{self, BlockContent, EnforcementMode, LedgerBlockContext};
use fluree_db_api::{verify_commit_v2_blob, NameService, StorageRead};
use fluree_db_core::flake::Flake;
use fluree_db_core::storage::ContentKind;
use fluree_db_core::{ContentId, CODEC_FLUREE_COMMIT};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::error::ServerError;
use crate::extract::StorageProxyBearer;
use crate::state::AppState;
use fluree_db_core::serde::flakes_transport::{encode_flakes, TransportFlake};

// ============================================================================
// Block Fetch Error Mapping
// ============================================================================

/// Map `BlockFetchError` to `ServerError` for HTTP responses.
fn map_block_fetch_error(e: block_fetch::BlockFetchError) -> ServerError {
    use block_fetch::BlockFetchError::*;
    match e {
        NotFound(msg) => ServerError::not_found(msg),
        StorageRead(e) => {
            if matches!(e, fluree_db_core::Error::NotFound(_)) {
                ServerError::not_found("Block not found")
            } else {
                ServerError::internal(format!("Storage: {e}"))
            }
        }
        UnknownAddress(_) | GraphSourceNotAuthorized | LedgerMismatch { .. } => {
            // No existence leak — unauthorized/unknown → 404
            ServerError::not_found("Block not found")
        }
        MissingBinaryStore => {
            ServerError::not_acceptable("Leaf decoding unavailable for this ledger")
        }
        MissingDbContext => ServerError::internal("Missing database context for policy filtering"),
        LeafRawForbidden => {
            // Should be unreachable — server always goes through fetch_and_decode_block
            // which handles enforcement internally. Map to not_acceptable as safe fallback.
            ServerError::not_acceptable("Raw leaf bytes not available under policy enforcement")
        }
        LeafDecode(e) => ServerError::internal(format!("Leaf decode: {e}")),
        PolicyBuild(msg) => ServerError::internal(format!("Policy: {msg}")),
        PolicyFilter(msg) => ServerError::internal(format!("Policy filter: {msg}")),
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
        builder = builder.header("X-Fluree-Block-Type", "ledger-leaf").header(
            "X-Fluree-Policy-Applied",
            if policy_applied { "true" } else { "false" },
        );
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

    let json = serde_json::to_vec(&transport).map_err(|e| ServerError::internal(e.to_string()))?;

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-fluree-flakes+json");

    if emit_debug_headers {
        builder = builder.header("X-Fluree-Block-Type", "ledger-leaf").header(
            "X-Fluree-Policy-Applied",
            if policy_applied { "true" } else { "false" },
        );
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
    /// Canonical ledger id (e.g., "mydb:main")
    pub ledger_id: String,
    /// Ledger name without branch (e.g., "mydb")
    pub name: String,
    pub branch: String,
    pub commit_head_id: Option<String>,
    pub commit_t: i64,
    pub index_head_id: Option<String>,
    pub index_t: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_context: Option<String>,
    pub retracted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_id: Option<String>,
    /// Computed storage address for the head commit (derived from CID + storage method).
    /// Peers need this to fetch commit blobs via the block endpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_address: Option<String>,
    /// Computed storage address for the head index root (derived from CID + storage method).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_address: Option<String>,
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
/// Requires Bearer token with access to the requested ledger ID.
///
/// Note: The `StorageProxyBearer` extractor handles:
/// - Checking if storage proxy is enabled (returns 404 if not)
/// - Validating the Bearer token
/// - Verifying issuer trust against StorageProxyConfig
/// - Checking that token has storage permissions
pub async fn get_ns_record(
    State(state): State<Arc<AppState>>,
    Path(ledger_id): Path<String>,
    StorageProxyBearer(principal): StorageProxyBearer,
) -> Result<Json<NsRecordResponse>, ServerError> {
    // Check authorization for this specific ledger
    if !principal.is_authorized_for_ledger(&ledger_id) {
        // Return 404 for unauthorized (no existence leak)
        return Err(ServerError::not_found("Ledger not found"));
    }

    // Look up the nameservice record
    // Storage proxy is only enabled on transaction servers (validated in config)
    let ns_record = state
        .fluree
        .as_file()
        .nameservice()
        .lookup(&ledger_id)
        .await
        .map_err(|e| ServerError::internal(format!("Nameservice lookup failed: {}", e)))?
        .ok_or_else(|| ServerError::not_found("Ledger not found"))?;

    // Derive storage addresses from CIDs so peers can fetch blobs via the block endpoint.
    let method = fluree_db_core::StorageMethod::storage_method(state.fluree.as_file().storage());
    let commit_address = ns_record.commit_head_id.as_ref().map(|cid| {
        fluree_db_core::content_address(
            method,
            fluree_db_core::ContentKind::Commit,
            &ns_record.ledger_id,
            &cid.digest_hex(),
        )
    });
    let index_address = ns_record.index_head_id.as_ref().map(|cid| {
        fluree_db_core::content_address(
            method,
            fluree_db_core::ContentKind::IndexRoot,
            &ns_record.ledger_id,
            &cid.digest_hex(),
        )
    });

    Ok(Json(NsRecordResponse {
        // IMPORTANT: this endpoint is consumed by `fluree-db-nameservice-sync` which
        // deserializes into `NsRecord`. Therefore we must include all required
        // `NsRecord` fields with matching names and semantics.
        ledger_id: ns_record.ledger_id.clone(),
        name: ns_record.name.clone(),
        branch: ns_record.branch.clone(),
        commit_head_id: ns_record.commit_head_id.as_ref().map(|id| id.to_string()),
        commit_t: ns_record.commit_t,
        index_head_id: ns_record.index_head_id.as_ref().map(|id| id.to_string()),
        index_t: ns_record.index_t,
        default_context: ns_record.default_context.clone(),
        retracted: ns_record.retracted,
        config_id: ns_record.config_id.as_ref().map(|id| id.to_string()),
        commit_address,
        index_address,
    }))
}

/// POST /fluree/storage/block
///
/// Fetches a block (branch or leaf) from storage with policy enforcement.
/// Ledger context is inferred from the address.
///
/// # Security
/// All blocks are fetched through `block_fetch::fetch_and_decode_block` with
/// `PolicyEnforced` mode. This ensures leaf blocks are always decoded and
/// policy-filtered — they can never be returned as raw bytes to end users.
///
/// # Content Negotiation
/// The Accept header selects **representation**, not enforcement:
/// - `application/octet-stream`: raw bytes for non-leaf blocks; encoded flakes for leaves
/// - `application/x-fluree-flakes`: binary CBOR flakes format
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
    // Parse address context and authorize
    let context = block_fetch::parse_address_context(&body.address);
    block_fetch::authorize_address(&principal.to_block_access_scope(), &context)
        .map_err(|_| ServerError::not_found("Block not found"))?;

    // Get storage proxy config for defaults and debug headers
    let proxy_config = state.config.storage_proxy();

    // Compute effective identity: token claim → config default → none
    let effective_identity = principal
        .identity
        .clone()
        .or_else(|| proxy_config.default_identity.clone());

    // Compute effective policy class: config default (token claim not yet supported)
    let effective_policy_class = proxy_config.default_policy_class.clone();

    // Build enforcement mode — always PolicyEnforced for end-user requests
    let mode = EnforcementMode::PolicyEnforced {
        identity: effective_identity,
        policy_class: effective_policy_class,
    };

    // Load ledger context if this is a ledger ID
    let fluree = state.fluree.as_file();
    let ledger_ctx_data;
    let ledger_ctx = if let Some(ledger_addr) = context.ledger_id() {
        let handle = fluree
            .ledger_cached(&ledger_addr)
            .await
            .map_err(|e| ServerError::internal(format!("Ledger load failed: {}", e)))?;
        let snapshot = handle.snapshot().await;
        let to_t = snapshot.db.t;
        ledger_ctx_data = Some((snapshot, to_t));
        ledger_ctx_data
            .as_ref()
            .map(|(snap, to_t)| LedgerBlockContext {
                db: &snap.db,
                to_t: *to_t,
                binary_store: snap.binary_store.as_deref(),
            })
    } else {
        None
    };

    // Fetch and decode with enforcement
    let fetched = block_fetch::fetch_and_decode_block(
        fluree.storage(),
        &body.address,
        &context,
        ledger_ctx.as_ref(),
        &mode,
    )
    .await
    .map_err(map_block_fetch_error)?;

    // Parse Accept header (selects representation, not enforcement)
    let accept = parse_accept_header(&headers);
    let emit_debug_headers = proxy_config.emit_debug_headers;

    // Build response based on content and Accept header
    match fetched.content {
        BlockContent::RawBytes(bytes) => {
            // Non-leaf block — return raw bytes regardless of Accept
            build_raw_response(bytes)
        }
        BlockContent::DecodedFlakes {
            flakes,
            policy_applied,
        } => {
            // Leaf block — encode in requested format
            match accept {
                AcceptFormat::FlakesBinary | AcceptFormat::OctetStream => {
                    // For octet-stream, use binary flakes as the closest safe representation
                    build_binary_flakes_response(&flakes, policy_applied, emit_debug_headers)
                }
                AcceptFormat::FlakesJson => {
                    build_json_flakes_response(&flakes, policy_applied, emit_debug_headers)
                }
            }
        }
    }
}

// ============================================================================
// CID Object Fetch
// ============================================================================

/// Query params for the CID object endpoint.
#[derive(Debug, Deserialize)]
pub struct ObjectQuery {
    /// Ledger alias (e.g., "mydb:main"). Required because storage paths are
    /// ledger-scoped.
    pub ledger: String,
}

/// Content kinds allowed through the CID object endpoint.
///
/// Only replication-essential kinds are served. Index artifacts (branches,
/// leaves, dicts) are NOT included — they go through `/storage/block` with
/// policy enforcement, or a future `/storage/objects-index/:cid` endpoint.
fn is_allowed_object_kind(kind: ContentKind) -> bool {
    matches!(
        kind,
        ContentKind::Commit | ContentKind::Txn | ContentKind::LedgerConfig
    )
}

/// Verify object bytes against a CID, with format-sniffing for commits.
///
/// Commit blobs are sniffed by magic bytes rather than assuming a fixed format:
/// - `FCV2` magic → commit-v2 sub-range hash verification
/// - Anything else → full-bytes SHA-256 (future commit formats, txn, config, etc.)
fn verify_object_integrity(id: &ContentId, bytes: &[u8]) -> bool {
    const COMMIT_V2_MAGIC: &[u8] = b"FCV2";

    if id.codec() == CODEC_FLUREE_COMMIT && bytes.starts_with(COMMIT_V2_MAGIC) {
        // Commit-v2: canonical sub-range hash (excludes trailing hash + sig block).
        match verify_commit_v2_blob(bytes) {
            Ok(derived_id) => derived_id == *id,
            Err(_) => false,
        }
    } else {
        // All other kinds + future commit formats: full-bytes SHA-256.
        id.verify(bytes)
    }
}

/// GET /fluree/storage/objects/:cid?ledger=mydb:main
///
/// Fetch a CAS object by its content identifier (CID). Returns the raw bytes
/// of the stored object after verifying integrity.
///
/// # Kind Allowlist
///
/// Only replication-essential kinds are served:
/// - `Commit` — commit chain blobs
/// - `Txn` — transaction data blobs
/// - `LedgerConfig` — origin discovery config
///
/// All other kinds (index artifacts, dicts, garbage records) return 404.
///
/// # Path Parameters
/// - `cid`: CIDv1 string (base32-lower, e.g., `"bafybeig..."`)
///
/// # Query Parameters
/// - `ledger`: Ledger alias (required, e.g., `"mydb:main"`)
///
/// # Response Headers
/// - `Content-Type: application/octet-stream`
/// - `X-Fluree-Content-Kind`: content kind label (commit, txn, config)
///
/// # Errors
/// - 400: Invalid CID string
/// - 404: Object not found, disallowed kind, or not authorized
/// - 500: Hash verification failed (storage corruption)
pub async fn get_object_by_cid(
    State(state): State<Arc<AppState>>,
    Path(cid_str): Path<String>,
    Query(query): Query<ObjectQuery>,
    StorageProxyBearer(principal): StorageProxyBearer,
) -> Result<Response, ServerError> {
    // 1. Parse CID
    let id: ContentId = cid_str
        .parse()
        .map_err(|_| ServerError::bad_request(format!("Invalid CID: {cid_str}")))?;

    // 2. Resolve content kind — unknown codec or disallowed kind → 404
    //    (404, not 400, to avoid becoming a discoverability oracle)
    let kind = match id.content_kind() {
        Some(k) if is_allowed_object_kind(k) => k,
        _ => return Err(ServerError::not_found("Object not found")),
    };

    // 3. Authorize: principal must have access to this ledger
    if !principal.is_authorized_for_ledger(&query.ledger) {
        return Err(ServerError::not_found("Object not found"));
    }

    // 4. Resolve CID → storage address and read bytes
    let method = fluree_db_core::StorageMethod::storage_method(state.fluree.as_file().storage());
    let address = fluree_db_core::content_address(method, kind, &query.ledger, &id.digest_hex());

    let bytes = state
        .fluree
        .as_file()
        .storage()
        .read_bytes(&address)
        .await
        .map_err(|e| match e {
            fluree_db_core::Error::NotFound(_) => ServerError::not_found("Object not found"),
            other => ServerError::internal(format!("Storage read: {other}")),
        })?;

    // 5. Verify integrity (format-sniffing for commits)
    if !verify_object_integrity(&id, &bytes) {
        return Err(ServerError::internal(format!(
            "Hash verification failed for CID {cid_str}"
        )));
    }

    // 6. Build response
    let kind_label = kind.codec_dir_name();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header("X-Fluree-Content-Kind", kind_label)
        .body(Body::from(bytes))
        .map_err(|e| ServerError::internal(e.to_string()))
}
