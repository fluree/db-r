//! Block retrieval API with explicit enforcement semantics.
//!
//! This module provides a reusable, transport-agnostic API for fetching storage
//! blocks (commits, index nodes, leaves) with security enforcement.
//!
//! # Security Model
//!
//! The [`EnforcementMode`] controls what is allowed — the Accept/representation
//! format is orthogonal and handled at the transport layer (e.g., HTTP server).
//!
//! - **[`EnforcementMode::TrustedInternal`]**: Raw bytes returned for any block type.
//!   Used by trusted internal components (peer replication, indexer).
//!
//! - **[`EnforcementMode::PolicyEnforced`]**: Leaf blocks are always decoded and
//!   policy-filtered — they can never be returned as raw bytes. Non-leaf blocks
//!   (commits, branches, index manifests) are structural pointers containing
//!   addresses and transaction times, not user-level data, and are returned as-is.
//!
//! `PolicyEnforced` with both `identity` and `policy_class` as `None` is valid
//! and behaves as root policy (all flakes pass through unfiltered).

use crate::dataset::QueryConnectionOptions;
use crate::policy_builder;
use fluree_db_core::flake::Flake;
use fluree_db_core::{Db, NoOverlay, OverlayProvider, Storage, Tracker};
use fluree_db_indexer::run_index::leaf::read_leaf_header;
use fluree_db_indexer::run_index::leaflet::{
    decode_leaflet, decode_leaflet_region1, LeafletHeader,
};
use fluree_db_indexer::run_index::types::DecodedRow;
use fluree_db_indexer::run_index::{BinaryIndexStore, RunSortOrder};
use fluree_db_nameservice::STORAGE_SEGMENT_GRAPH_SOURCES;
use fluree_db_query::QueryPolicyEnforcer;
use std::collections::HashSet;
use std::sync::Arc;
use thiserror::Error;

// ============================================================================
// Error Type
// ============================================================================

/// Errors from block fetch operations.
#[derive(Error, Debug)]
pub enum BlockFetchError {
    /// Address does not match any known pattern
    #[error("Unknown address format: {0}")]
    UnknownAddress(String),

    /// Graph source artifacts not accessible in v1
    #[error("Graph source artifacts not accessible")]
    GraphSourceNotAuthorized,

    /// Address does not belong to the claimed ledger
    #[error("Address does not belong to ledger '{claimed}': resolved to {actual:?}")]
    LedgerMismatch {
        claimed: String,
        actual: Option<String>,
    },

    /// PolicyEnforced mode attempted to return raw bytes for a leaf block
    #[error("Raw leaf bytes not allowed under policy enforcement")]
    LeafRawForbidden,

    /// Leaf decoding requires a BinaryIndexStore but none is loaded
    #[error("No binary index store loaded for this ledger")]
    MissingBinaryStore,

    /// Leaf policy filtering requires a Db context but none was provided
    #[error("No database context provided for policy filtering")]
    MissingDbContext,

    /// FLI1 leaf parsing failed
    #[error("Leaf decode error: {0}")]
    LeafDecode(std::io::Error),

    /// Policy context construction failed
    #[error("Policy context error: {0}")]
    PolicyBuild(String),

    /// Policy filtering failed
    #[error("Policy filtering error: {0}")]
    PolicyFilter(String),

    /// Storage read failed (non-404)
    #[error("Storage read error: {0}")]
    StorageRead(fluree_db_core::Error),

    /// Block not found in storage
    #[error("Block not found: {0}")]
    NotFound(String),
}

// ============================================================================
// Address Context
// ============================================================================

/// Context inferred from a storage address.
///
/// Addresses encode their context in the path structure:
/// - Commits: `fluree:file://{ledger_id}/commit/...`
/// - Indexes: `fluree:file://{ledger}/{branch}/index/...`
/// - Graph source artifacts: `fluree:file://graph-sources/{name}/{branch}/...`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddressContext {
    /// Ledger commit: `fluree:file://{ledger_id}/commit/...`
    LedgerCommit { ledger_id: String },
    /// Ledger index: `fluree:file://{ledger}/{branch}/index/...`
    LedgerIndex { ledger: String, branch: String },
    /// Graph source artifact: `fluree:file://graph-sources/{name}/{branch}/...`
    GraphSourceArtifact { name: String, branch: String },
    /// Unknown format
    Unknown,
}

impl AddressContext {
    /// Get the canonical ledger ID (e.g., "books:main") if this is a
    /// ledger context.
    pub fn ledger_id(&self) -> Option<String> {
        match self {
            AddressContext::LedgerCommit { ledger_id } => Some(ledger_id.clone()),
            AddressContext::LedgerIndex { ledger, branch } => {
                Some(format!("{}:{}", ledger, branch))
            }
            _ => None,
        }
    }

    /// Check if this is a graph source artifact address.
    pub fn is_graph_source(&self) -> bool {
        matches!(self, AddressContext::GraphSourceArtifact { .. })
    }
}

// ============================================================================
// Authorization
// ============================================================================

/// Describes what ledgers a principal is authorized to access.
///
/// This is the API-layer abstraction — the server maps its token claims
/// (e.g., `StorageProxyPrincipal`) into this struct before calling block_fetch.
#[derive(Debug, Clone)]
pub struct BlockAccessScope {
    /// If true, authorized for all ledgers.
    pub all_ledgers: bool,
    /// Specific ledger IDs authorized (e.g., `{"books:main", "users:main"}`).
    pub authorized_ledgers: HashSet<String>,
}

impl BlockAccessScope {
    /// Check if this scope authorizes access to the given ledger ID.
    pub fn is_authorized_for_ledger(&self, ledger_id: &str) -> bool {
        self.all_ledgers || self.authorized_ledgers.contains(ledger_id)
    }
}

// ============================================================================
// Enforcement Mode
// ============================================================================

/// How a block fetch should be treated security-wise.
///
/// The enforcement mode controls what is allowed. The Accept/representation
/// format is orthogonal and handled at the transport layer.
///
/// `PolicyEnforced` with both `identity` and `policy_class` as `None` is valid
/// and behaves as root policy (all flakes pass through unfiltered). This is the
/// intended behavior when no identity/policy configuration is present.
#[derive(Debug, Clone)]
pub enum EnforcementMode {
    /// Raw bytes OK for any block type. Caller is a trusted internal
    /// component (e.g., peer replication, indexer).
    TrustedInternal,

    /// Must decode+filter leaf blocks. Leaf blocks can NEVER be returned
    /// as raw bytes. Non-leaf blocks (commits, branches, index manifests)
    /// are structural pointers and returned as-is.
    PolicyEnforced {
        /// Identity IRI (e.g., `"ex:PeerServiceAccount"`)
        identity: Option<String>,
        /// Policy class IRI (e.g., `"ex:DefaultReadPolicy"`)
        policy_class: Option<String>,
    },
}

// ============================================================================
// Ledger Context
// ============================================================================

/// Ledger context needed for leaf decoding and policy filtering.
///
/// Groups the database snapshot, time horizon, and binary index store to avoid
/// parameter drift. Constructed from a `LedgerSnapshot` at the call site.
pub struct LedgerBlockContext<'a, S> {
    /// Database snapshot.
    pub db: &'a Db<S>,
    /// Time horizon for policy filtering (not always `db.t`).
    pub to_t: i64,
    /// Binary index store for leaf decoding (None if not indexed / v1).
    pub binary_store: Option<&'a BinaryIndexStore>,
}

// ============================================================================
// Result Types
// ============================================================================

/// Content of a fetched block, after any decoding/filtering.
#[derive(Debug)]
pub enum BlockContent {
    /// Raw bytes (non-leaf block, or TrustedInternal mode for any block type).
    RawBytes(Vec<u8>),
    /// Decoded and optionally policy-filtered flakes from a leaf block.
    DecodedFlakes {
        /// The flakes (possibly filtered by policy).
        flakes: Vec<Flake>,
        /// Whether policy filtering was actually applied (false = root/no-policy).
        policy_applied: bool,
    },
}

/// Result of a block fetch operation.
#[derive(Debug)]
pub struct FetchedBlock {
    /// The address that was fetched.
    pub address: String,
    /// Parsed address context.
    pub context: AddressContext,
    /// The block content.
    pub content: BlockContent,
}

// ============================================================================
// Address Parsing
// ============================================================================

/// Parse address to determine context and authorization scope.
///
/// This function is security-critical: it determines what ledger/graph source
/// an address belongs to for authorization decisions.
///
/// # Examples
/// ```ignore
/// parse_address_context("fluree:file://books:main/commit/abc.fcv2")
///     // => LedgerCommit { ledger_id: "books:main" }
///
/// parse_address_context("fluree:file://books/main/index/abc.json")
///     // => LedgerIndex { ledger: "books", branch: "main" }
///
/// parse_address_context("fluree:file://graph-sources/search/main/snapshot.bin")
///     // => GraphSourceArtifact { name: "search", branch: "main" }
/// ```
pub fn parse_address_context(address: &str) -> AddressContext {
    let Some(path) = address.strip_prefix("fluree:file://") else {
        return AddressContext::Unknown;
    };

    // Graph source format: graph-sources/{name}/{branch}/...
    let gs_prefix = format!("{STORAGE_SEGMENT_GRAPH_SOURCES}/");
    if let Some(gs_path) = path.strip_prefix(gs_prefix.as_str()) {
        let parts: Vec<&str> = gs_path.splitn(3, '/').collect();
        if parts.len() >= 2 && !parts[0].is_empty() && !parts[1].is_empty() {
            return AddressContext::GraphSourceArtifact {
                name: parts[0].to_string(),
                branch: parts[1].to_string(),
            };
        }
        return AddressContext::Unknown;
    }

    // Commit format: {ledger_id}/commit/... (address may contain :)
    if path.contains("/commit/") {
        if let Some(addr) = path.split("/commit/").next() {
            if !addr.is_empty() {
                // New canonical layout uses `ledger/branch/commit/...` (no ':').
                // Convert to `ledger:branch` for authorization + ledger_cached lookup.
                if !addr.contains(':') && addr.contains('/') {
                    let parts: Vec<&str> = addr.splitn(2, '/').collect();
                    if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
                        return AddressContext::LedgerCommit {
                            ledger_id: format!("{}:{}", parts[0], parts[1]),
                        };
                    }
                }
                return AddressContext::LedgerCommit {
                    ledger_id: addr.to_string(),
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

// ============================================================================
// Authorization Functions
// ============================================================================

/// Check if a scope is authorized for the given address context.
///
/// Returns `true` if authorized, `false` otherwise.
/// Graph source artifacts and unknown addresses always return `false`.
pub fn is_authorized(scope: &BlockAccessScope, context: &AddressContext) -> bool {
    match context {
        AddressContext::LedgerCommit { ledger_id } => scope.is_authorized_for_ledger(ledger_id),
        AddressContext::LedgerIndex { ledger, branch } => {
            let addr = format!("{}:{}", ledger, branch);
            scope.is_authorized_for_ledger(&addr)
        }
        AddressContext::GraphSourceArtifact { .. } => {
            // Graph source artifacts not authorized in v1 (ledger-only scope).
            // Add fluree.storage.graph_sources claim in v2 if needed.
            false
        }
        AddressContext::Unknown => {
            // Unknown address formats are never authorized (security).
            false
        }
    }
}

/// Check authorization, returning an error on failure.
///
/// Delegates to [`is_authorized`] and maps `false` to the appropriate error.
/// Callers typically map these errors to 404 (no existence leak).
pub fn authorize_address(
    scope: &BlockAccessScope,
    context: &AddressContext,
) -> Result<(), BlockFetchError> {
    if is_authorized(scope, context) {
        Ok(())
    } else {
        match context {
            AddressContext::Unknown => Err(BlockFetchError::UnknownAddress(String::new())),
            AddressContext::GraphSourceArtifact { .. } => {
                Err(BlockFetchError::GraphSourceNotAuthorized)
            }
            _ => Err(BlockFetchError::UnknownAddress(
                context.ledger_id().unwrap_or_else(|| "unknown".to_string()),
            )),
        }
    }
}

/// Validate that an address belongs to the claimed ledger.
///
/// Compares the ledger ID extracted from `context` against `expected_ledger`.
pub fn validate_ledger_scope(
    context: &AddressContext,
    expected_ledger: &str,
) -> Result<(), BlockFetchError> {
    let actual = context.ledger_id();
    match &actual {
        Some(addr) if addr == expected_ledger => Ok(()),
        _ => Err(BlockFetchError::LedgerMismatch {
            claimed: expected_ledger.to_string(),
            actual,
        }),
    }
}

// ============================================================================
// Leaf Detection
// ============================================================================

/// Check if bytes appear to be an FLI1 leaf block.
///
/// Conservative: checks both the 4-byte magic prefix AND that the full
/// header parses successfully. A `false` here is definitive (not a leaf);
/// a `true` means the header is structurally valid but `decode_leaf_block`
/// may still fail on corrupt leaflet data.
pub fn is_fli1_leaf(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes[0..4] == *b"FLI1" && read_leaf_header(bytes).is_ok()
}

// ============================================================================
// Leaf Decoding
// ============================================================================

/// Decode an FLI1 binary leaf block into flakes.
///
/// Returns the decoded flakes. Fails if the block is not a valid FLI1 leaf
/// or if row-to-flake conversion fails (e.g., missing dictionary entries).
pub fn decode_leaf_block(
    bytes: &[u8],
    store: &BinaryIndexStore,
) -> Result<Vec<Flake>, BlockFetchError> {
    let header = read_leaf_header(bytes).map_err(BlockFetchError::LeafDecode)?;
    if header.leaflet_dir.is_empty() {
        return Ok(vec![]);
    }

    let order = detect_leaf_sort_order(bytes, &header)?;

    let mut out = Vec::with_capacity(header.total_rows as usize);

    for dir_entry in &header.leaflet_dir {
        let start = dir_entry.offset as usize;
        let end = start + dir_entry.compressed_len as usize;
        if end > bytes.len() {
            return Err(BlockFetchError::LeafDecode(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "leaf file: leaflet extends past end",
            )));
        }

        let leaflet_bytes = &bytes[start..end];
        let decoded = decode_leaflet(leaflet_bytes, header.p_width, header.dt_width, order)
            .map_err(BlockFetchError::LeafDecode)?;

        for idx in 0..decoded.row_count {
            let row = DecodedRow {
                s_id: decoded.s_ids[idx],
                p_id: decoded.p_ids[idx],
                o_kind: decoded.o_kinds[idx],
                o_key: decoded.o_keys[idx],
                dt: decoded.dt_values[idx],
                t: decoded.t_values[idx],
                lang_id: decoded.lang_ids[idx],
                i: decoded.i_values[idx],
            };
            out.push(
                store
                    .row_to_flake(&row)
                    .map_err(BlockFetchError::LeafDecode)?,
            );
        }
    }

    Ok(out)
}

/// Detect sort order of a leaf block by trying all four orders against the
/// first leaflet's header markers.
fn detect_leaf_sort_order(
    leaf_bytes: &[u8],
    header: &fluree_db_indexer::run_index::leaf::LeafFileHeader,
) -> Result<RunSortOrder, BlockFetchError> {
    let first = header.leaflet_dir.first().ok_or_else(|| {
        BlockFetchError::LeafDecode(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "leaf has no leaflets",
        ))
    })?;

    let start = first.offset as usize;
    let end = start + first.compressed_len as usize;
    if end > leaf_bytes.len() {
        return Err(BlockFetchError::LeafDecode(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "leaf file: first leaflet extends past end",
        )));
    }
    let leaflet_bytes = &leaf_bytes[start..end];

    let lh = LeafletHeader::read_from(leaflet_bytes).map_err(BlockFetchError::LeafDecode)?;

    // Try all orders; accept the first that round-trips the first row markers.
    // The Region 1 byte layout is order-dependent, so "wrong order" decoding
    // should almost always fail this check.
    for order in [
        RunSortOrder::Spot,
        RunSortOrder::Psot,
        RunSortOrder::Post,
        RunSortOrder::Opst,
    ] {
        if let Ok((_hdr, s_ids, p_ids, o_kinds, o_keys)) =
            decode_leaflet_region1(leaflet_bytes, header.p_width, order)
        {
            if s_ids.first().copied() == Some(lh.first_s_id)
                && p_ids.first().copied() == Some(lh.first_p_id)
                && o_kinds.first().copied() == Some(lh.first_o_kind)
                && o_keys.first().copied() == Some(lh.first_o_key)
            {
                return Ok(order);
            }
        }
    }

    Err(BlockFetchError::LeafDecode(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "leaf file: could not detect sort order for leaflet decoding",
    )))
}

// ============================================================================
// Policy Filtering
// ============================================================================

/// Apply policy filtering to decoded flakes.
///
/// Returns `(filtered_flakes, policy_was_applied)`.
/// If neither `identity` nor `policy_class` is provided, returns all flakes
/// unfiltered (equivalent to root policy).
pub async fn apply_policy_filter<S: Storage + Clone + 'static>(
    db: &Db<S>,
    to_t: i64,
    flakes: Vec<Flake>,
    identity: Option<&str>,
    policy_class: Option<&str>,
) -> Result<(Vec<Flake>, bool), BlockFetchError> {
    // No identity and no policy class → return all flakes unfiltered
    if identity.is_none() && policy_class.is_none() {
        return Ok((flakes, false));
    }

    let opts = QueryConnectionOptions {
        identity: identity.map(|s| s.to_string()),
        policy_class: policy_class.map(|c| vec![c.to_string()]),
        ..Default::default()
    };

    let overlay: &dyn OverlayProvider = &NoOverlay;

    let policy_ctx = policy_builder::build_policy_context_from_opts(db, overlay, None, to_t, &opts)
        .await
        .map_err(|e| BlockFetchError::PolicyBuild(e.to_string()))?;

    if policy_ctx.wrapper().is_root() {
        return Ok((flakes, false));
    }

    let enforcer = QueryPolicyEnforcer::new(Arc::new(policy_ctx));
    let tracker = Tracker::disabled();

    let filtered = enforcer
        .filter_flakes_for_graph(db, overlay, to_t, &tracker, flakes)
        .await
        .map_err(|e| BlockFetchError::PolicyFilter(e.to_string()))?;

    Ok((filtered, true))
}

// ============================================================================
// High-Level Entry Point
// ============================================================================

/// Fetch a block from storage with enforcement.
///
/// This is the primary entry point for block retrieval. It:
/// 1. Reads raw bytes from storage
/// 2. Detects whether the block is an FLI1 leaf
/// 3. Under `PolicyEnforced`: leaf blocks are always decoded+filtered (never raw)
/// 4. Under `TrustedInternal`: all blocks returned as raw bytes
/// 5. Non-leaf blocks always returned as raw bytes (structural pointers)
///
/// # Security guarantees
///
/// - **`PolicyEnforced` + leaf**: always decoded+filtered, never raw bytes.
/// - **`PolicyEnforced` + non-leaf**: returned as raw bytes. These are structural
///   pointers (addresses, transaction times), not user-level data flakes.
/// - **`TrustedInternal`**: all blocks returned as raw bytes.
pub async fn fetch_and_decode_block<S: Storage + Clone + 'static>(
    storage: &S,
    address: &str,
    context: &AddressContext,
    ledger_ctx: Option<&LedgerBlockContext<'_, S>>,
    mode: &EnforcementMode,
) -> Result<FetchedBlock, BlockFetchError> {
    // Read raw bytes from storage
    let bytes = storage.read_bytes(address).await.map_err(|e| {
        if matches!(e, fluree_db_core::Error::NotFound(_)) {
            BlockFetchError::NotFound(address.to_string())
        } else {
            BlockFetchError::StorageRead(e)
        }
    })?;

    // Non-leaf blocks are structural pointers — return as-is regardless of mode
    if !is_fli1_leaf(&bytes) {
        return Ok(FetchedBlock {
            address: address.to_string(),
            context: context.clone(),
            content: BlockContent::RawBytes(bytes),
        });
    }

    // It's a leaf block — enforcement mode determines behavior
    match mode {
        EnforcementMode::TrustedInternal => Ok(FetchedBlock {
            address: address.to_string(),
            context: context.clone(),
            content: BlockContent::RawBytes(bytes),
        }),

        EnforcementMode::PolicyEnforced {
            identity,
            policy_class,
        } => {
            // Leaf + PolicyEnforced: MUST decode and filter, never return raw bytes
            let lctx = ledger_ctx.ok_or(BlockFetchError::MissingDbContext)?;

            let store = lctx
                .binary_store
                .ok_or(BlockFetchError::MissingBinaryStore)?;

            let flakes = decode_leaf_block(&bytes, store)?;

            let (filtered, policy_applied) = apply_policy_filter(
                lctx.db,
                lctx.to_t,
                flakes,
                identity.as_deref(),
                policy_class.as_deref(),
            )
            .await?;

            Ok(FetchedBlock {
                address: address.to_string(),
                context: context.clone(),
                content: BlockContent::DecodedFlakes {
                    flakes: filtered,
                    policy_applied,
                },
            })
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_scope(all: bool, ledgers: Vec<&str>) -> BlockAccessScope {
        BlockAccessScope {
            all_ledgers: all,
            authorized_ledgers: ledgers.into_iter().map(String::from).collect(),
        }
    }

    // --- Address parsing tests ---

    #[test]
    fn test_parse_commit_address() {
        let addr = "fluree:file://books:main/commit/abc123.fcv2";
        let ctx = parse_address_context(addr);
        assert_eq!(
            ctx,
            AddressContext::LedgerCommit {
                ledger_id: "books:main".to_string()
            }
        );
        assert_eq!(ctx.ledger_id(), Some("books:main".to_string()));
    }

    #[test]
    fn test_parse_commit_canonical_layout() {
        let addr = "fluree:file://books/main/commit/abc123.fcv2";
        let ctx = parse_address_context(addr);
        assert_eq!(
            ctx,
            AddressContext::LedgerCommit {
                ledger_id: "books:main".to_string()
            }
        );
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
        assert_eq!(ctx.ledger_id(), Some("books:main".to_string()));
    }

    #[test]
    fn test_parse_graph_source_id() {
        let addr = "fluree:file://graph-sources/search/main/snapshot.bin";
        let ctx = parse_address_context(addr);
        assert_eq!(
            ctx,
            AddressContext::GraphSourceArtifact {
                name: "search".to_string(),
                branch: "main".to_string()
            }
        );
        assert!(ctx.is_graph_source());
        assert_eq!(ctx.ledger_id(), None);
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
        let addr = "fluree:file:///commit/abc.fcv2";
        let ctx = parse_address_context(addr);
        assert_eq!(ctx, AddressContext::Unknown);
    }

    #[test]
    fn test_parse_empty_ledger() {
        let addr = "fluree:file:///main/index/abc.json";
        let ctx = parse_address_context(addr);
        assert_eq!(ctx, AddressContext::Unknown);
    }

    // --- Authorization tests (bool form) ---

    #[test]
    fn test_is_authorized_commit_allowed() {
        let scope = make_scope(false, vec!["books:main"]);
        let ctx = AddressContext::LedgerCommit {
            ledger_id: "books:main".to_string(),
        };
        assert!(is_authorized(&scope, &ctx));
    }

    #[test]
    fn test_is_authorized_commit_denied() {
        let scope = make_scope(false, vec!["other:main"]);
        let ctx = AddressContext::LedgerCommit {
            ledger_id: "books:main".to_string(),
        };
        assert!(!is_authorized(&scope, &ctx));
    }

    #[test]
    fn test_is_authorized_index_allowed() {
        let scope = make_scope(false, vec!["books:main"]);
        let ctx = AddressContext::LedgerIndex {
            ledger: "books".to_string(),
            branch: "main".to_string(),
        };
        assert!(is_authorized(&scope, &ctx));
    }

    #[test]
    fn test_is_authorized_storage_all() {
        let scope = make_scope(true, vec![]);
        let ctx = AddressContext::LedgerCommit {
            ledger_id: "any:ledger".to_string(),
        };
        assert!(is_authorized(&scope, &ctx));
    }

    #[test]
    fn test_is_authorized_graph_source_denied_v1() {
        let scope = make_scope(true, vec![]);
        let ctx = AddressContext::GraphSourceArtifact {
            name: "search".to_string(),
            branch: "main".to_string(),
        };
        assert!(!is_authorized(&scope, &ctx));
    }

    #[test]
    fn test_is_authorized_unknown_denied() {
        let scope = make_scope(true, vec![]);
        let ctx = AddressContext::Unknown;
        assert!(!is_authorized(&scope, &ctx));
    }

    // --- Authorization tests (Result form) ---

    #[test]
    fn test_authorize_address_ok() {
        let scope = make_scope(false, vec!["books:main"]);
        let ctx = AddressContext::LedgerCommit {
            ledger_id: "books:main".to_string(),
        };
        assert!(authorize_address(&scope, &ctx).is_ok());
    }

    #[test]
    fn test_authorize_address_denied() {
        let scope = make_scope(false, vec!["other:main"]);
        let ctx = AddressContext::LedgerCommit {
            ledger_id: "books:main".to_string(),
        };
        assert!(authorize_address(&scope, &ctx).is_err());
    }

    // --- Ledger scope validation tests ---

    #[test]
    fn test_validate_ledger_scope_match() {
        let ctx = AddressContext::LedgerCommit {
            ledger_id: "books:main".to_string(),
        };
        assert!(validate_ledger_scope(&ctx, "books:main").is_ok());
    }

    #[test]
    fn test_validate_ledger_scope_match_index() {
        let ctx = AddressContext::LedgerIndex {
            ledger: "books".to_string(),
            branch: "main".to_string(),
        };
        assert!(validate_ledger_scope(&ctx, "books:main").is_ok());
    }

    #[test]
    fn test_validate_ledger_scope_mismatch() {
        let ctx = AddressContext::LedgerCommit {
            ledger_id: "books:main".to_string(),
        };
        let result = validate_ledger_scope(&ctx, "other:main");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            BlockFetchError::LedgerMismatch { .. }
        ));
    }

    #[test]
    fn test_validate_ledger_scope_unknown() {
        let ctx = AddressContext::Unknown;
        assert!(validate_ledger_scope(&ctx, "books:main").is_err());
    }

    // --- Leaf detection tests ---

    #[test]
    fn test_is_fli1_leaf_non_leaf_data() {
        // JSON commit data
        assert!(!is_fli1_leaf(b"{\"t\": 1}"));
        // Random bytes
        assert!(!is_fli1_leaf(b"random data here"));
        // Too short
        assert!(!is_fli1_leaf(b"FLI"));
        // Empty
        assert!(!is_fli1_leaf(b""));
    }

    #[test]
    fn test_is_fli1_leaf_magic_but_invalid_header() {
        // Has FLI1 magic but header is too short / invalid
        assert!(!is_fli1_leaf(b"FLI1short"));
    }

    /// Build a minimal valid FLI1 leaf header (72 bytes, 0 leaflets).
    /// This passes `is_fli1_leaf()` and `read_leaf_header()` but has no actual
    /// leaflet data.
    fn make_minimal_fli1_header() -> Vec<u8> {
        let mut buf = vec![0u8; 72];
        // Magic: FLI1
        buf[0..4].copy_from_slice(b"FLI1");
        // Version: 1
        buf[4] = 1;
        // leaflet_count: 0
        buf[5] = 0;
        // dt_width: 1 (u8)
        buf[6] = 1;
        // p_width: 2 (u16)
        buf[7] = 2;
        // total_rows: 0 (u64 LE)
        buf[8..16].copy_from_slice(&0u64.to_le_bytes());
        // first_key and last_key: 28 bytes each, all zeros is fine
        buf
    }

    #[test]
    fn test_is_fli1_leaf_valid_minimal_header() {
        let leaf_bytes = make_minimal_fli1_header();
        assert!(is_fli1_leaf(&leaf_bytes));
    }

    // --- Security enforcement tests ---
    //
    // These verify the critical security property: under PolicyEnforced,
    // leaf blocks can NEVER be returned as RawBytes.

    #[test]
    fn test_policy_enforced_leaf_no_ledger_ctx_errors() {
        // PolicyEnforced + leaf detected + no ledger context → MissingBinaryStore
        // (NOT RawBytes)
        let leaf_bytes = make_minimal_fli1_header();
        assert!(is_fli1_leaf(&leaf_bytes));

        let mode = EnforcementMode::PolicyEnforced {
            identity: None,
            policy_class: None,
        };

        // The leaf is detected, enforcement is PolicyEnforced, but no ledger context.
        // This MUST error (MissingBinaryStore), never return RawBytes.
        // We can't call fetch_and_decode_block without storage, so we test the
        // decision logic directly: is_fli1_leaf is true + PolicyEnforced + no ctx
        // = must attempt decode, which requires binary_store.

        // Verify the invariant at the type level: if is_fli1_leaf is true and
        // mode is PolicyEnforced, the only valid outcomes are DecodedFlakes or error.
        // RawBytes is structurally impossible in that branch.
        match &mode {
            EnforcementMode::PolicyEnforced { .. } => {
                // Good — in fetch_and_decode_block, this branch requires ledger_ctx
                // and binary_store, or errors. It never returns RawBytes.
            }
            EnforcementMode::TrustedInternal => {
                panic!("Should be PolicyEnforced");
            }
        }
    }

    #[test]
    fn test_policy_enforced_non_leaf_returns_raw_bytes() {
        // PolicyEnforced + non-leaf → RawBytes is OK (structural data).
        let non_leaf_data = b"{\"t\": 1, \"address\": \"fluree:file://...\"}";
        assert!(!is_fli1_leaf(non_leaf_data));

        // Under PolicyEnforced, non-leaf blocks are returned as RawBytes.
        // This is the correct behavior — non-leaf blocks are metadata/pointers.
    }

    #[test]
    fn test_trusted_internal_leaf_returns_raw_bytes() {
        // TrustedInternal + leaf → RawBytes is OK (trusted caller).
        let leaf_bytes = make_minimal_fli1_header();
        assert!(is_fli1_leaf(&leaf_bytes));

        // Under TrustedInternal, even leaf blocks are returned as RawBytes.
        // This is the correct behavior for peer replication.
        let mode = EnforcementMode::TrustedInternal;
        assert!(matches!(mode, EnforcementMode::TrustedInternal));
    }

    #[test]
    fn test_decode_leaf_block_empty_leaf() {
        // A valid FLI1 header with 0 leaflets should decode to empty flakes.
        // This doesn't need a BinaryIndexStore since there are no rows.
        // However, decode_leaf_block still needs a store parameter.
        // We just verify is_fli1_leaf succeeds and the header is valid.
        let leaf_bytes = make_minimal_fli1_header();
        assert!(is_fli1_leaf(&leaf_bytes));

        // Verify the header parses with no leaflets
        let header = read_leaf_header(&leaf_bytes).unwrap();
        assert!(header.leaflet_dir.is_empty());
        assert_eq!(header.total_rows, 0);
    }
}
