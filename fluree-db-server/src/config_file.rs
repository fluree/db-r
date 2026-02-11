//! Configuration file support for Fluree server.
//!
//! Loads server configuration from `.fluree/config.toml` (or JSON), merges with
//! CLI args and environment variables. The config file is shared with the Fluree CLI;
//! server-specific settings live under the `[server]` TOML section.
//!
//! ## Precedence (highest to lowest)
//!
//! 1. CLI arguments
//! 2. Environment variables
//! 3. Profile overlay (`[profiles.<name>.server]`)
//! 4. Config file (`[server]`)
//! 5. Hardcoded defaults

use clap::ArgMatches;
use fluree_db_api::server_defaults::{self, FLUREE_DIR};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

use crate::config::{
    AdminAuthMode, DataAuthMode, EventsAuthMode, ServerConfig, ServerRole, StorageAccessMode,
};

// ---------------------------------------------------------------------------
// File config serde types (all Option — absence means "not set in file")
// ---------------------------------------------------------------------------

/// Top-level config file structure. CLI sections (remotes, upstreams, etc.) are
/// ignored via `#[serde(default)]`; only `server` and `profiles` are read.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct FlureeFileConfig {
    /// Server configuration section `[server]`
    #[serde(default)]
    pub server: Option<ServerFileConfig>,

    /// Profile overrides `[profiles.<name>]`
    #[serde(default)]
    pub profiles: Option<HashMap<String, ProfileConfig>>,
    // CLI-managed sections — ignored by the server but tolerated during parse.
    // Using `flatten` with `deny_unknown_fields` would break, so we just skip them.
}

/// A named profile containing server overrides.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ProfileConfig {
    /// Server overrides for this profile `[profiles.<name>.server]`
    #[serde(default)]
    pub server: Option<ServerFileConfig>,
}

/// The `[server]` section. Every field is `Option` so the file only needs to
/// contain values the user wants to set.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct ServerFileConfig {
    pub listen_addr: Option<String>,
    pub storage_path: Option<String>,
    pub log_level: Option<String>,
    pub cors_enabled: Option<bool>,
    pub body_limit: Option<usize>,
    pub cache_max_entries: Option<usize>,

    /// `[server.indexing]`
    #[serde(default)]
    pub indexing: Option<IndexingFileConfig>,

    /// `[server.auth]`
    #[serde(default)]
    pub auth: Option<AuthFileConfig>,

    /// `[server.peer]`
    #[serde(default)]
    pub peer: Option<PeerFileConfig>,

    /// `[server.mcp]`
    #[serde(default)]
    pub mcp: Option<McpFileConfig>,

    /// `[server.storage_proxy]`
    #[serde(default)]
    pub storage_proxy: Option<StorageProxyFileConfig>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct IndexingFileConfig {
    pub enabled: Option<bool>,
    pub reindex_min_bytes: Option<usize>,
    pub reindex_max_bytes: Option<usize>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct AuthFileConfig {
    /// `[server.auth.events]`
    #[serde(default)]
    pub events: Option<AuthEndpointFileConfig>,
    /// `[server.auth.data]`
    #[serde(default)]
    pub data: Option<DataAuthFileConfig>,
    /// `[server.auth.admin]`
    #[serde(default)]
    pub admin: Option<AuthEndpointFileConfig>,
    /// `[server.auth.jwks]`
    #[serde(default)]
    pub jwks: Option<JwksFileConfig>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct AuthEndpointFileConfig {
    pub mode: Option<String>,
    pub audience: Option<String>,
    pub trusted_issuers: Option<Vec<String>>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct DataAuthFileConfig {
    pub mode: Option<String>,
    pub audience: Option<String>,
    pub trusted_issuers: Option<Vec<String>>,
    pub default_policy_class: Option<String>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct JwksFileConfig {
    pub issuers: Option<Vec<String>>,
    pub cache_ttl: Option<u64>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct PeerFileConfig {
    pub role: Option<String>,
    pub tx_server_url: Option<String>,
    pub events_url: Option<String>,
    pub events_token: Option<String>,
    pub subscribe_all: Option<bool>,
    pub ledgers: Option<Vec<String>>,
    pub graph_sources: Option<Vec<String>>,
    /// `[server.peer.reconnect]`
    #[serde(default)]
    pub reconnect: Option<PeerReconnectFileConfig>,
    pub storage_access_mode: Option<String>,
    pub storage_proxy_token: Option<String>,
    pub storage_proxy_token_file: Option<String>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct PeerReconnectFileConfig {
    pub initial_ms: Option<u64>,
    pub max_ms: Option<u64>,
    pub multiplier: Option<f64>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct McpFileConfig {
    pub enabled: Option<bool>,
    pub auth_trusted_issuers: Option<Vec<String>>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct StorageProxyFileConfig {
    pub enabled: Option<bool>,
    pub trusted_issuers: Option<Vec<String>>,
    pub default_identity: Option<String>,
    pub default_policy_class: Option<String>,
    pub debug_headers: Option<bool>,
}

// ---------------------------------------------------------------------------
// Config file loading
// ---------------------------------------------------------------------------

/// Load a config file from the given path. Detects format by extension:
/// `.json` → JSON, everything else → TOML.
pub fn load_config(path: &Path) -> Result<FlureeFileConfig, ConfigFileError> {
    let content = std::fs::read_to_string(path).map_err(|e| ConfigFileError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;

    if content.trim().is_empty() {
        return Ok(FlureeFileConfig::default());
    }

    let is_json = path
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("json") || ext.eq_ignore_ascii_case("jsonld"));

    if is_json {
        // For .jsonld files, validate the @context using the JSON-LD library
        if path
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("jsonld"))
        {
            if let Ok(raw) = serde_json::from_str::<serde_json::Value>(&content) {
                if let Err(e) = fluree_db_api::server_defaults::validate_jsonld_context(&raw) {
                    warn!(path = %path.display(), error = %e, "JSON-LD config context validation issue");
                }
            }
        }

        serde_json::from_str(&content).map_err(|e| ConfigFileError::Parse {
            path: path.to_path_buf(),
            detail: e.to_string(),
        })
    } else {
        toml::from_str(&content).map_err(|e| ConfigFileError::Parse {
            path: path.to_path_buf(),
            detail: e.to_string(),
        })
    }
}

/// Errors from config file loading.
#[derive(Debug, thiserror::Error)]
pub enum ConfigFileError {
    #[error("Failed to read config file {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("Failed to parse config file {path}: {detail}")]
    Parse { path: PathBuf, detail: String },
    #[error("Profile '{name}' not found in config file")]
    ProfileNotFound { name: String },
    #[error("Invalid config value: {0}")]
    InvalidValue(String),
}

// ---------------------------------------------------------------------------
// Config file discovery
// ---------------------------------------------------------------------------

/// Find a config file in a directory using the shared detection logic.
/// Logs a warning if both TOML and JSON-LD configs exist.
fn find_config_in_dir(dir: &Path) -> Option<PathBuf> {
    let detection = server_defaults::detect_config_in_dir(dir)?;
    if detection.both_exist {
        warn!(
            dir = %dir.display(),
            "Both config.toml and config.jsonld found; using config.toml"
        );
    }
    Some(detection.path)
}

/// Resolve the config file path.
///
/// 1. Use explicit `--config` override if provided
/// 2. Walk up from cwd looking for `.fluree/config.toml` or `.fluree/config.jsonld`
/// 3. Check `~/.fluree/config.{toml,jsonld}` as global fallback
///
/// Returns `None` if no config file is found (this is not an error).
pub fn resolve_config_path(explicit: Option<&Path>) -> Option<PathBuf> {
    if let Some(p) = explicit {
        // Explicit path: could be a file or a directory containing config
        if p.is_file() {
            return Some(p.to_path_buf());
        }
        if p.is_dir() {
            if let Some(found) = find_config_in_dir(p) {
                return Some(found);
            }
        }
        // Try as parent of a .fluree/ directory
        let fluree_subdir = p.join(FLUREE_DIR);
        if let Some(found) = find_config_in_dir(&fluree_subdir) {
            return Some(found);
        }
        // Explicit path not found — warn and continue without file config
        warn!(path = %p.display(), "Config file not found at specified path");
        return None;
    }

    // Walk up from cwd looking for .fluree/config.{toml,jsonld}
    if let Ok(cwd) = std::env::current_dir() {
        let mut current = cwd.to_path_buf();
        loop {
            let fluree_subdir = current.join(FLUREE_DIR);
            if let Some(found) = find_config_in_dir(&fluree_subdir) {
                return Some(found);
            }
            if !current.pop() {
                break;
            }
        }
    }

    // Global fallback: ~/.fluree/config.{toml,jsonld}
    if let Some(home) = dirs_home() {
        let fluree_subdir = home.join(FLUREE_DIR);
        if let Some(found) = find_config_in_dir(&fluree_subdir) {
            return Some(found);
        }
    }

    None
}

fn dirs_home() -> Option<PathBuf> {
    dirs::home_dir()
}

// ---------------------------------------------------------------------------
// Deep merge (profile overlay onto base)
// ---------------------------------------------------------------------------

/// Deep-merge `overlay` onto `base`. Only `Some` values in the overlay replace
/// the corresponding base values; `None` in the overlay means "inherit from base".
pub fn deep_merge(base: &mut ServerFileConfig, overlay: &ServerFileConfig) {
    macro_rules! merge_field {
        ($field:ident) => {
            if overlay.$field.is_some() {
                base.$field = overlay.$field.clone();
            }
        };
    }

    merge_field!(listen_addr);
    merge_field!(storage_path);
    merge_field!(log_level);
    merge_field!(cors_enabled);
    merge_field!(body_limit);
    merge_field!(cache_max_entries);

    // Nested: indexing
    if let Some(ref ovr) = overlay.indexing {
        let b = base.indexing.get_or_insert_with(Default::default);
        if ovr.enabled.is_some() {
            b.enabled = ovr.enabled;
        }
        if ovr.reindex_min_bytes.is_some() {
            b.reindex_min_bytes = ovr.reindex_min_bytes;
        }
        if ovr.reindex_max_bytes.is_some() {
            b.reindex_max_bytes = ovr.reindex_max_bytes;
        }
    }

    // Nested: auth
    if let Some(ref ovr) = overlay.auth {
        let b = base.auth.get_or_insert_with(Default::default);
        merge_auth_endpoint(&mut b.events, &ovr.events);
        merge_data_auth(&mut b.data, &ovr.data);
        merge_auth_endpoint(&mut b.admin, &ovr.admin);
        if let Some(ref ovr_jwks) = ovr.jwks {
            let bj = b.jwks.get_or_insert_with(Default::default);
            if ovr_jwks.issuers.is_some() {
                bj.issuers = ovr_jwks.issuers.clone();
            }
            if ovr_jwks.cache_ttl.is_some() {
                bj.cache_ttl = ovr_jwks.cache_ttl;
            }
        }
    }

    // Nested: peer
    if let Some(ref ovr) = overlay.peer {
        let b = base.peer.get_or_insert_with(Default::default);
        if ovr.role.is_some() {
            b.role = ovr.role.clone();
        }
        if ovr.tx_server_url.is_some() {
            b.tx_server_url = ovr.tx_server_url.clone();
        }
        if ovr.events_url.is_some() {
            b.events_url = ovr.events_url.clone();
        }
        if ovr.events_token.is_some() {
            b.events_token = ovr.events_token.clone();
        }
        if ovr.subscribe_all.is_some() {
            b.subscribe_all = ovr.subscribe_all;
        }
        if ovr.ledgers.is_some() {
            b.ledgers = ovr.ledgers.clone();
        }
        if ovr.graph_sources.is_some() {
            b.graph_sources = ovr.graph_sources.clone();
        }
        if ovr.storage_access_mode.is_some() {
            b.storage_access_mode = ovr.storage_access_mode.clone();
        }
        if ovr.storage_proxy_token.is_some() {
            b.storage_proxy_token = ovr.storage_proxy_token.clone();
        }
        if ovr.storage_proxy_token_file.is_some() {
            b.storage_proxy_token_file = ovr.storage_proxy_token_file.clone();
        }
        if let Some(ref ovr_r) = ovr.reconnect {
            let br = b.reconnect.get_or_insert_with(Default::default);
            if ovr_r.initial_ms.is_some() {
                br.initial_ms = ovr_r.initial_ms;
            }
            if ovr_r.max_ms.is_some() {
                br.max_ms = ovr_r.max_ms;
            }
            if ovr_r.multiplier.is_some() {
                br.multiplier = ovr_r.multiplier;
            }
        }
    }

    // Nested: mcp
    if let Some(ref ovr) = overlay.mcp {
        let b = base.mcp.get_or_insert_with(Default::default);
        if ovr.enabled.is_some() {
            b.enabled = ovr.enabled;
        }
        if ovr.auth_trusted_issuers.is_some() {
            b.auth_trusted_issuers = ovr.auth_trusted_issuers.clone();
        }
    }

    // Nested: storage_proxy
    if let Some(ref ovr) = overlay.storage_proxy {
        let b = base.storage_proxy.get_or_insert_with(Default::default);
        if ovr.enabled.is_some() {
            b.enabled = ovr.enabled;
        }
        if ovr.trusted_issuers.is_some() {
            b.trusted_issuers = ovr.trusted_issuers.clone();
        }
        if ovr.default_identity.is_some() {
            b.default_identity = ovr.default_identity.clone();
        }
        if ovr.default_policy_class.is_some() {
            b.default_policy_class = ovr.default_policy_class.clone();
        }
        if ovr.debug_headers.is_some() {
            b.debug_headers = ovr.debug_headers;
        }
    }
}

fn merge_auth_endpoint(
    base: &mut Option<AuthEndpointFileConfig>,
    overlay: &Option<AuthEndpointFileConfig>,
) {
    if let Some(ref ovr) = overlay {
        let b = base.get_or_insert_with(Default::default);
        if ovr.mode.is_some() {
            b.mode = ovr.mode.clone();
        }
        if ovr.audience.is_some() {
            b.audience = ovr.audience.clone();
        }
        if ovr.trusted_issuers.is_some() {
            b.trusted_issuers = ovr.trusted_issuers.clone();
        }
    }
}

fn merge_data_auth(base: &mut Option<DataAuthFileConfig>, overlay: &Option<DataAuthFileConfig>) {
    if let Some(ref ovr) = overlay {
        let b = base.get_or_insert_with(Default::default);
        if ovr.mode.is_some() {
            b.mode = ovr.mode.clone();
        }
        if ovr.audience.is_some() {
            b.audience = ovr.audience.clone();
        }
        if ovr.trusted_issuers.is_some() {
            b.trusted_issuers = ovr.trusted_issuers.clone();
        }
        if ovr.default_policy_class.is_some() {
            b.default_policy_class = ovr.default_policy_class.clone();
        }
    }
}

// ---------------------------------------------------------------------------
// Apply file config to ServerConfig (value_source gating)
// ---------------------------------------------------------------------------

/// All clap arg IDs referenced by `apply_to_server_config`.
///
/// This list is validated by `test_config_file_arg_ids_match_server_config`
/// to ensure every ID actually exists in `ServerConfig`'s clap definition.
/// When adding new config file fields, add the arg ID here AND in the
/// `apply_to_server_config` function body.
pub const CONFIG_FILE_ARG_IDS: &[&str] = &[
    "listen_addr",
    "storage_path",
    "log_level",
    "cors_enabled",
    "body_limit",
    "cache_max_entries",
    "indexing_enabled",
    "reindex_min_bytes",
    "reindex_max_bytes",
    "events_auth_mode",
    "events_auth_audience",
    "events_auth_trusted_issuers",
    "data_auth_mode",
    "data_auth_audience",
    "data_auth_trusted_issuers",
    "data_auth_default_policy_class",
    "admin_auth_mode",
    "admin_auth_trusted_issuers",
    "server_role",
    "tx_server_url",
    "peer_events_url",
    "peer_events_token",
    "peer_subscribe_all",
    "peer_ledgers",
    "peer_graph_sources",
    "storage_access_mode",
    "storage_proxy_token",
    "storage_proxy_token_file",
    "peer_reconnect_initial_ms",
    "peer_reconnect_max_ms",
    "peer_reconnect_multiplier",
    "mcp_enabled",
    "mcp_auth_trusted_issuers",
    "storage_proxy_enabled",
    "storage_proxy_trusted_issuers",
    "storage_proxy_default_identity",
    "storage_proxy_default_policy_class",
    "storage_proxy_debug_headers",
];

/// Arg IDs that are only available when the `oidc` feature is enabled.
#[cfg(feature = "oidc")]
pub const CONFIG_FILE_ARG_IDS_OIDC: &[&str] = &["jwks_issuers", "jwks_cache_ttl"];

/// Apply config file values to a `ServerConfig`, but only for fields where
/// the user did NOT provide a CLI argument or environment variable.
///
/// Uses clap's `value_source` to determine whether each field came from
/// an explicit user input (CLI/env) or a default.
pub fn apply_to_server_config(
    file: &ServerFileConfig,
    config: &mut ServerConfig,
    matches: &ArgMatches,
) {
    use clap::parser::ValueSource;

    // Helper: returns true if the field was NOT explicitly set (i.e., is using default).
    // For scalar fields with `default_value`, value_source is Some(DefaultValue).
    // For Vec/Option fields without defaults, value_source is None.
    let is_default = |arg_name: &str| -> bool {
        matches!(
            matches.value_source(arg_name),
            None | Some(ValueSource::DefaultValue)
        )
    };

    // --- Top-level scalars ---
    // NOTE: clap derive uses the field name (underscores) as the arg ID,
    // NOT the kebab-case long flag name. Using hyphens here would panic in
    // debug builds and silently return None in release builds.
    if is_default("listen_addr") {
        if let Some(ref addr_str) = file.listen_addr {
            if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                config.listen_addr = addr;
            } else {
                warn!(
                    value = addr_str,
                    "Invalid listen_addr in config file, ignoring"
                );
            }
        }
    }
    if is_default("storage_path") {
        if let Some(ref path) = file.storage_path {
            config.storage_path = Some(PathBuf::from(path));
        }
    }
    if is_default("log_level") {
        if let Some(ref level) = file.log_level {
            config.log_level = level.clone();
        }
    }
    if is_default("cors_enabled") {
        if let Some(v) = file.cors_enabled {
            config.cors_enabled = v;
        }
    }
    if is_default("body_limit") {
        if let Some(v) = file.body_limit {
            config.body_limit = v;
        }
    }
    if is_default("cache_max_entries") {
        if let Some(v) = file.cache_max_entries {
            config.cache_max_entries = v;
        }
    }

    // --- Indexing ---
    if let Some(ref idx) = file.indexing {
        if is_default("indexing_enabled") {
            if let Some(v) = idx.enabled {
                config.indexing_enabled = v;
            }
        }
        if is_default("reindex_min_bytes") {
            if let Some(v) = idx.reindex_min_bytes {
                config.reindex_min_bytes = v;
            }
        }
        if is_default("reindex_max_bytes") {
            if let Some(v) = idx.reindex_max_bytes {
                config.reindex_max_bytes = v;
            }
        }
    }

    // --- Auth: events ---
    if let Some(ref auth) = file.auth {
        if let Some(ref events) = auth.events {
            if is_default("events_auth_mode") {
                if let Some(ref mode) = events.mode {
                    if let Some(m) = parse_events_auth_mode(mode) {
                        config.events_auth_mode = m;
                    }
                }
            }
            if is_default("events_auth_audience") {
                if let Some(ref v) = events.audience {
                    config.events_auth_audience = Some(v.clone());
                }
            }
            if is_default("events_auth_trusted_issuers") {
                if let Some(ref v) = events.trusted_issuers {
                    config.events_auth_trusted_issuers = v.clone();
                }
            }
        }

        // --- Auth: data ---
        if let Some(ref data) = auth.data {
            if is_default("data_auth_mode") {
                if let Some(ref mode) = data.mode {
                    if let Some(m) = parse_data_auth_mode(mode) {
                        config.data_auth_mode = m;
                    }
                }
            }
            if is_default("data_auth_audience") {
                if let Some(ref v) = data.audience {
                    config.data_auth_audience = Some(v.clone());
                }
            }
            if is_default("data_auth_trusted_issuers") {
                if let Some(ref v) = data.trusted_issuers {
                    config.data_auth_trusted_issuers = v.clone();
                }
            }
            if is_default("data_auth_default_policy_class") {
                if let Some(ref v) = data.default_policy_class {
                    config.data_auth_default_policy_class = Some(v.clone());
                }
            }
        }

        // --- Auth: admin ---
        if let Some(ref admin) = auth.admin {
            if is_default("admin_auth_mode") {
                if let Some(ref mode) = admin.mode {
                    if let Some(m) = parse_admin_auth_mode(mode) {
                        config.admin_auth_mode = m;
                    }
                }
            }
            if is_default("admin_auth_trusted_issuers") {
                if let Some(ref v) = admin.trusted_issuers {
                    config.admin_auth_trusted_issuers = v.clone();
                }
            }
        }

        // --- Auth: jwks ---
        #[cfg(feature = "oidc")]
        if let Some(ref jwks) = auth.jwks {
            if is_default("jwks_issuers") {
                if let Some(ref v) = jwks.issuers {
                    config.jwks_issuers = v.clone();
                }
            }
            if is_default("jwks_cache_ttl") {
                if let Some(v) = jwks.cache_ttl {
                    config.jwks_cache_ttl = v;
                }
            }
        }
    }

    // --- Peer ---
    if let Some(ref peer) = file.peer {
        if is_default("server_role") {
            if let Some(ref role) = peer.role {
                if let Some(r) = parse_server_role(role) {
                    config.server_role = r;
                }
            }
        }
        if is_default("tx_server_url") {
            if let Some(ref v) = peer.tx_server_url {
                config.tx_server_url = Some(v.clone());
            }
        }
        if is_default("peer_events_url") {
            if let Some(ref v) = peer.events_url {
                config.peer_events_url = Some(v.clone());
            }
        }
        if is_default("peer_events_token") {
            if let Some(ref v) = peer.events_token {
                match server_defaults::resolve_at_filepath(v) {
                    Ok(resolved) => {
                        if server_defaults::is_plaintext_secret(v) {
                            warn!(
                                "peer.events_token is stored as plaintext in the config file. \
                                 Consider using @filepath (e.g. \"@/etc/fluree/token.jwt\") \
                                 or the FLUREE_PEER_EVENTS_TOKEN env var instead."
                            );
                        }
                        config.peer_events_token = Some(resolved);
                    }
                    Err(e) => {
                        warn!(
                            field = "peer.events_token",
                            error = %e,
                            "Failed to read @filepath for peer events token"
                        );
                    }
                }
            }
        }
        if is_default("peer_subscribe_all") {
            if let Some(v) = peer.subscribe_all {
                config.peer_subscribe_all = v;
            }
        }
        if is_default("peer_ledgers") {
            if let Some(ref v) = peer.ledgers {
                config.peer_ledgers = v.clone();
            }
        }
        if is_default("peer_graph_sources") {
            if let Some(ref v) = peer.graph_sources {
                config.peer_graph_sources = v.clone();
            }
        }
        if is_default("storage_access_mode") {
            if let Some(ref mode) = peer.storage_access_mode {
                if let Some(m) = parse_storage_access_mode(mode) {
                    config.storage_access_mode = m;
                }
            }
        }
        if is_default("storage_proxy_token") {
            if let Some(ref v) = peer.storage_proxy_token {
                match server_defaults::resolve_at_filepath(v) {
                    Ok(resolved) => {
                        if server_defaults::is_plaintext_secret(v) {
                            warn!(
                                "peer.storage_proxy_token is stored as plaintext in the config file. \
                                 Consider using @filepath (e.g. \"@/etc/fluree/token.jwt\") \
                                 or the FLUREE_STORAGE_PROXY_TOKEN env var instead."
                            );
                        }
                        config.storage_proxy_token = Some(resolved);
                    }
                    Err(e) => {
                        warn!(
                            field = "peer.storage_proxy_token",
                            error = %e,
                            "Failed to read @filepath for storage proxy token"
                        );
                    }
                }
            }
        }
        if is_default("storage_proxy_token_file") {
            if let Some(ref v) = peer.storage_proxy_token_file {
                config.storage_proxy_token_file = Some(PathBuf::from(v));
            }
        }

        // Peer reconnect
        if let Some(ref reconnect) = peer.reconnect {
            if is_default("peer_reconnect_initial_ms") {
                if let Some(v) = reconnect.initial_ms {
                    config.peer_reconnect_initial_ms = v;
                }
            }
            if is_default("peer_reconnect_max_ms") {
                if let Some(v) = reconnect.max_ms {
                    config.peer_reconnect_max_ms = v;
                }
            }
            if is_default("peer_reconnect_multiplier") {
                if let Some(v) = reconnect.multiplier {
                    config.peer_reconnect_multiplier = v;
                }
            }
        }
    }

    // --- MCP ---
    if let Some(ref mcp) = file.mcp {
        if is_default("mcp_enabled") {
            if let Some(v) = mcp.enabled {
                config.mcp_enabled = v;
            }
        }
        if is_default("mcp_auth_trusted_issuers") {
            if let Some(ref v) = mcp.auth_trusted_issuers {
                config.mcp_auth_trusted_issuers = v.clone();
            }
        }
    }

    // --- Storage proxy ---
    if let Some(ref sp) = file.storage_proxy {
        if is_default("storage_proxy_enabled") {
            if let Some(v) = sp.enabled {
                config.storage_proxy_enabled = v;
            }
        }
        if is_default("storage_proxy_trusted_issuers") {
            if let Some(ref v) = sp.trusted_issuers {
                config.storage_proxy_trusted_issuers = v.clone();
            }
        }
        if is_default("storage_proxy_default_identity") {
            if let Some(ref v) = sp.default_identity {
                config.storage_proxy_default_identity = Some(v.clone());
            }
        }
        if is_default("storage_proxy_default_policy_class") {
            if let Some(ref v) = sp.default_policy_class {
                config.storage_proxy_default_policy_class = Some(v.clone());
            }
        }
        if is_default("storage_proxy_debug_headers") {
            if let Some(v) = sp.debug_headers {
                config.storage_proxy_debug_headers = v;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Enum parsers (string → enum for config file values)
// ---------------------------------------------------------------------------

fn parse_events_auth_mode(s: &str) -> Option<EventsAuthMode> {
    match s.to_lowercase().as_str() {
        "none" => Some(EventsAuthMode::None),
        "optional" => Some(EventsAuthMode::Optional),
        "required" => Some(EventsAuthMode::Required),
        _ => {
            warn!(value = s, "Invalid events auth mode in config file");
            None
        }
    }
}

fn parse_data_auth_mode(s: &str) -> Option<DataAuthMode> {
    match s.to_lowercase().as_str() {
        "none" => Some(DataAuthMode::None),
        "optional" => Some(DataAuthMode::Optional),
        "required" => Some(DataAuthMode::Required),
        _ => {
            warn!(value = s, "Invalid data auth mode in config file");
            None
        }
    }
}

fn parse_admin_auth_mode(s: &str) -> Option<AdminAuthMode> {
    match s.to_lowercase().as_str() {
        "none" => Some(AdminAuthMode::None),
        "required" => Some(AdminAuthMode::Required),
        _ => {
            warn!(value = s, "Invalid admin auth mode in config file");
            None
        }
    }
}

fn parse_server_role(s: &str) -> Option<ServerRole> {
    match s.to_lowercase().as_str() {
        "transaction" => Some(ServerRole::Transaction),
        "peer" => Some(ServerRole::Peer),
        _ => {
            warn!(value = s, "Invalid server role in config file");
            None
        }
    }
}

fn parse_storage_access_mode(s: &str) -> Option<StorageAccessMode> {
    match s.to_lowercase().as_str() {
        "shared" => Some(StorageAccessMode::Shared),
        "proxy" => Some(StorageAccessMode::Proxy),
        _ => {
            warn!(value = s, "Invalid storage access mode in config file");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Top-level entry point: load config file and merge into ServerConfig
// ---------------------------------------------------------------------------

/// Load the config file (if found) and merge its values into `config`,
/// respecting the precedence: CLI > env > profile > file > defaults.
pub fn load_and_merge_config(
    config: &mut ServerConfig,
    matches: &ArgMatches,
) -> Result<(), ConfigFileError> {
    let config_path = resolve_config_path(config.config_file.as_deref());

    let Some(path) = config_path else {
        debug!("No config file found, using CLI args and defaults only");
        return Ok(());
    };

    info!(path = %path.display(), "Loading configuration file");

    let file_config = load_config(&path)?;

    let mut server_section = file_config.server.unwrap_or_default();

    // Apply profile overlay if --profile is specified
    if let Some(ref profile_name) = config.profile {
        if let Some(ref profiles) = file_config.profiles {
            if let Some(profile) = profiles.get(profile_name) {
                if let Some(ref overlay) = profile.server {
                    info!(profile = profile_name, "Applying configuration profile");
                    deep_merge(&mut server_section, overlay);
                }
            } else {
                return Err(ConfigFileError::ProfileNotFound {
                    name: profile_name.clone(),
                });
            }
        } else {
            return Err(ConfigFileError::ProfileNotFound {
                name: profile_name.clone(),
            });
        }
    }

    apply_to_server_config(&server_section, config, matches);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_toml_with_server_section() {
        let toml = r#"
[[remotes]]
name = "origin"
type = "Http"
base_url = "http://localhost:8090/fluree"

[server]
listen_addr = "127.0.0.1:9090"
storage_path = "/var/lib/fluree"
log_level = "debug"
cors_enabled = false
cache_max_entries = 5000

[server.indexing]
enabled = true
reindex_min_bytes = 200000
reindex_max_bytes = 2000000

[server.auth.events]
mode = "required"
trusted_issuers = ["did:key:z6MkTest"]

[server.auth.data]
mode = "optional"
default_policy_class = "ex:DefaultPolicy"
"#;

        let config: FlureeFileConfig = toml::from_str(toml).unwrap();
        let server = config.server.unwrap();
        assert_eq!(server.listen_addr.as_deref(), Some("127.0.0.1:9090"));
        assert_eq!(server.storage_path.as_deref(), Some("/var/lib/fluree"));
        assert_eq!(server.log_level.as_deref(), Some("debug"));
        assert_eq!(server.cors_enabled, Some(false));
        assert_eq!(server.cache_max_entries, Some(5000));

        let idx = server.indexing.unwrap();
        assert_eq!(idx.enabled, Some(true));
        assert_eq!(idx.reindex_min_bytes, Some(200000));
        assert_eq!(idx.reindex_max_bytes, Some(2000000));

        let auth = server.auth.unwrap();
        let events = auth.events.unwrap();
        assert_eq!(events.mode.as_deref(), Some("required"));
        assert_eq!(
            events.trusted_issuers,
            Some(vec!["did:key:z6MkTest".to_string()])
        );

        let data = auth.data.unwrap();
        assert_eq!(data.mode.as_deref(), Some("optional"));
        assert_eq!(
            data.default_policy_class.as_deref(),
            Some("ex:DefaultPolicy")
        );
    }

    #[test]
    fn test_load_json_config() {
        let json = r#"{
            "server": {
                "listen_addr": "0.0.0.0:7070",
                "indexing": {
                    "enabled": true
                }
            },
            "profiles": {
                "dev": {
                    "server": {
                        "log_level": "debug"
                    }
                }
            }
        }"#;

        let config: FlureeFileConfig = serde_json::from_str(json).unwrap();
        let server = config.server.unwrap();
        assert_eq!(server.listen_addr.as_deref(), Some("0.0.0.0:7070"));
        assert_eq!(server.indexing.unwrap().enabled, Some(true));

        let profiles = config.profiles.unwrap();
        let dev = profiles.get("dev").unwrap();
        let dev_server = dev.server.as_ref().unwrap();
        assert_eq!(dev_server.log_level.as_deref(), Some("debug"));
    }

    #[test]
    fn test_deep_merge_profile() {
        let mut base = ServerFileConfig {
            listen_addr: Some("0.0.0.0:8090".into()),
            log_level: Some("info".into()),
            indexing: Some(IndexingFileConfig {
                enabled: Some(false),
                reindex_min_bytes: Some(100_000),
                reindex_max_bytes: Some(1_000_000),
            }),
            ..Default::default()
        };

        let overlay = ServerFileConfig {
            log_level: Some("debug".into()),
            indexing: Some(IndexingFileConfig {
                enabled: Some(true),
                reindex_min_bytes: None, // should NOT override
                reindex_max_bytes: None, // should NOT override
            }),
            ..Default::default()
        };

        deep_merge(&mut base, &overlay);

        // listen_addr unchanged (overlay had None)
        assert_eq!(base.listen_addr.as_deref(), Some("0.0.0.0:8090"));
        // log_level overridden
        assert_eq!(base.log_level.as_deref(), Some("debug"));
        // indexing.enabled overridden
        let idx = base.indexing.unwrap();
        assert_eq!(idx.enabled, Some(true));
        // indexing thresholds NOT overridden (overlay had None)
        assert_eq!(idx.reindex_min_bytes, Some(100_000));
        assert_eq!(idx.reindex_max_bytes, Some(1_000_000));
    }

    #[test]
    fn test_empty_config_file() {
        let config: FlureeFileConfig = toml::from_str("").unwrap();
        assert!(config.server.is_none());
        assert!(config.profiles.is_none());
    }

    #[test]
    fn test_cli_only_sections_ignored() {
        // A file with only CLI sections should parse fine (server is None)
        let toml = r#"
[[remotes]]
name = "origin"
type = "Http"
base_url = "http://localhost:8090/fluree"

[[upstreams]]
local_alias = "mydb:main"
remote = "origin"
remote_alias = "mydb:main"
auto_pull = true
"#;

        let config: FlureeFileConfig = toml::from_str(toml).unwrap();
        assert!(config.server.is_none());
    }

    #[test]
    fn test_load_jsonld_config_with_context() {
        // @context is silently ignored by serde — config values parse normally
        let json = r#"{
            "@context": {
                "@vocab": "https://ns.flur.ee/config#"
            },
            "_comment": "Test config",
            "server": {
                "listen_addr": "0.0.0.0:7070",
                "indexing": {
                    "enabled": true
                }
            },
            "profiles": {
                "dev": {
                    "server": {
                        "log_level": "debug"
                    }
                }
            }
        }"#;

        let config: FlureeFileConfig = serde_json::from_str(json).unwrap();
        let server = config.server.unwrap();
        assert_eq!(server.listen_addr.as_deref(), Some("0.0.0.0:7070"));
        assert_eq!(server.indexing.unwrap().enabled, Some(true));

        let profiles = config.profiles.unwrap();
        let dev = profiles.get("dev").unwrap();
        let dev_server = dev.server.as_ref().unwrap();
        assert_eq!(dev_server.log_level.as_deref(), Some("debug"));
    }

    /// Verify that every arg ID referenced by `apply_to_server_config` (via
    /// `CONFIG_FILE_ARG_IDS`) actually exists in the clap definition of
    /// `ServerConfig`. This catches silent breakage when fields are renamed.
    #[test]
    fn test_config_file_arg_ids_match_server_config() {
        use clap::CommandFactory;

        let cmd = ServerConfig::command();
        let known_args: Vec<&str> = cmd
            .get_arguments()
            .map(|arg| arg.get_id().as_str())
            .collect();

        for id in CONFIG_FILE_ARG_IDS {
            assert!(
                known_args.contains(id),
                "CONFIG_FILE_ARG_IDS contains '{id}' which does not exist in \
                 ServerConfig's clap definition. Did a field get renamed?"
            );
        }

        #[cfg(feature = "oidc")]
        for id in CONFIG_FILE_ARG_IDS_OIDC {
            assert!(
                known_args.contains(id),
                "CONFIG_FILE_ARG_IDS_OIDC contains '{id}' which does not exist in \
                 ServerConfig's clap definition. Did a field get renamed?"
            );
        }
    }
}
