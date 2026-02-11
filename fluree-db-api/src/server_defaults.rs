//! Default values for Fluree server configuration.
//!
//! These constants are the single source of truth shared between
//! `fluree-db-server` (clap `default_value` attributes) and
//! `fluree-db-cli` (the `fluree init` config template).

use serde_json::json;

// ── Top-level server settings ───────────────────────────────────────

pub const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:8090";
pub const DEFAULT_STORAGE_PATH: &str = ".fluree/storage";
pub const DEFAULT_LOG_LEVEL: &str = "info";
pub const DEFAULT_CORS_ENABLED: bool = true;
pub const DEFAULT_BODY_LIMIT: usize = 52_428_800; // 50 MB
pub const DEFAULT_CACHE_MAX_ENTRIES: usize = 10_000;

// ── Indexing ────────────────────────────────────────────────────────

pub const DEFAULT_INDEXING_ENABLED: bool = false;
pub const DEFAULT_REINDEX_MIN_BYTES: usize = 100_000;
pub const DEFAULT_REINDEX_MAX_BYTES: usize = 1_000_000;

// ── Auth ────────────────────────────────────────────────────────────

pub const DEFAULT_AUTH_MODE: &str = "none";
pub const DEFAULT_JWKS_CACHE_TTL: u64 = 300;

// ── MCP ─────────────────────────────────────────────────────────────

pub const DEFAULT_MCP_ENABLED: bool = false;

// ── Peer ────────────────────────────────────────────────────────────

pub const DEFAULT_PEER_ROLE: &str = "transaction";
pub const DEFAULT_PEER_RECONNECT_INITIAL_MS: u64 = 1000;
pub const DEFAULT_PEER_RECONNECT_MAX_MS: u64 = 30000;
pub const DEFAULT_PEER_RECONNECT_MULTIPLIER: f64 = 2.0;

// ── Storage proxy ───────────────────────────────────────────────────

pub const DEFAULT_STORAGE_PROXY_ENABLED: bool = false;

// ── Config format ──────────────────────────────────────────────────

/// The JSON-LD vocabulary IRI for Fluree config properties.
pub const CONFIG_VOCAB: &str = "https://ns.flur.ee/config#";

/// Config file format for `fluree init`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigFormat {
    Toml,
    JsonLd,
}

impl ConfigFormat {
    /// File name for this format.
    pub fn filename(&self) -> &'static str {
        match self {
            Self::Toml => "config.toml",
            Self::JsonLd => "config.jsonld",
        }
    }
}

// ── Template generation ─────────────────────────────────────────────

/// Generate a commented-out TOML config template using the canonical
/// default values above.  Written by `fluree init`.
pub fn generate_config_template() -> String {
    format!(
        r#"# Fluree Configuration
#
# This file is shared by the Fluree CLI and Fluree Server.
# CLI-managed sections (remotes, upstreams) are managed by `fluree` commands.
# Server settings live under [server] and can be customized below.
#
# Precedence (highest to lowest):
#   1. CLI arguments
#   2. Environment variables (FLUREE_*)
#   3. Profile overrides ([profiles.<name>.server])
#   4. This file ([server])
#   5. Built-in defaults

# ──────────────────────────────────────────────────────────────────────
# Server Configuration
# ──────────────────────────────────────────────────────────────────────
# Uncomment and modify values as needed.

# [server]
# listen_addr = "{listen_addr}"
# storage_path = "{storage_path}"   # relative to working directory
# log_level = "{log_level}"                 # trace, debug, info, warn, error
# cors_enabled = {cors_enabled}
# body_limit = {body_limit}              # 50 MB
# cache_max_entries = {cache_max_entries}

# [server.indexing]
# enabled = {indexing_enabled}
# reindex_min_bytes = {reindex_min_bytes}         # {reindex_min_kb} KB — triggers background reindexing
# reindex_max_bytes = {reindex_max_bytes}        # {reindex_max_kb} KB — blocks commits until reindexed

# [server.auth.events]
# mode = "{auth_mode}"                      # none, optional, required
# # audience = "https://my-app.example.com"
# # trusted_issuers = ["did:key:z6Mk..."]

# [server.auth.data]
# mode = "{auth_mode}"                      # none, optional, required
# # audience = "https://my-app.example.com"
# # trusted_issuers = ["did:key:z6Mk..."]
# # default_policy_class = "ex:DefaultPolicy"

# [server.auth.admin]
# mode = "{auth_mode}"                      # none, required
# # trusted_issuers = ["did:key:z6Mk..."]

# [server.auth.jwks]
# # issuers = ["https://auth.example.com=https://auth.example.com/.well-known/jwks.json"]
# # cache_ttl = {jwks_cache_ttl}                  # seconds

# [server.mcp]
# enabled = {mcp_enabled}
# # auth_trusted_issuers = ["did:key:z6Mk..."]

# [server.peer]
# role = "{peer_role}"               # transaction, peer
# # tx_server_url = "http://tx.internal:8090"
# # subscribe_all = false
# # ledgers = ["books:main"]
# # graph_sources = []

# [server.peer.reconnect]
# initial_ms = {reconnect_initial_ms}
# max_ms = {reconnect_max_ms}
# multiplier = {reconnect_multiplier}

# [server.storage_proxy]
# enabled = {storage_proxy_enabled}
# # trusted_issuers = ["did:key:z6Mk..."]
# # default_identity = "ex:ServiceAccount"
# # default_policy_class = "ex:ProxyPolicy"
# # debug_headers = false

# ──────────────────────────────────────────────────────────────────────
# Profiles
# ──────────────────────────────────────────────────────────────────────
# Activate with: fluree-server --profile <name>
# Profile values are deep-merged onto [server].

# [profiles.dev.server]
# log_level = "debug"

# [profiles.prod.server]
# log_level = "warn"
# [profiles.prod.server.indexing]
# enabled = true
# [profiles.prod.server.auth.data]
# mode = "required"
"#,
        listen_addr = DEFAULT_LISTEN_ADDR,
        storage_path = DEFAULT_STORAGE_PATH,
        log_level = DEFAULT_LOG_LEVEL,
        cors_enabled = DEFAULT_CORS_ENABLED,
        body_limit = DEFAULT_BODY_LIMIT,
        cache_max_entries = DEFAULT_CACHE_MAX_ENTRIES,
        indexing_enabled = DEFAULT_INDEXING_ENABLED,
        reindex_min_bytes = DEFAULT_REINDEX_MIN_BYTES,
        reindex_min_kb = DEFAULT_REINDEX_MIN_BYTES / 1000,
        reindex_max_bytes = DEFAULT_REINDEX_MAX_BYTES,
        reindex_max_kb = DEFAULT_REINDEX_MAX_BYTES / 1000,
        auth_mode = DEFAULT_AUTH_MODE,
        jwks_cache_ttl = DEFAULT_JWKS_CACHE_TTL,
        mcp_enabled = DEFAULT_MCP_ENABLED,
        peer_role = DEFAULT_PEER_ROLE,
        reconnect_initial_ms = DEFAULT_PEER_RECONNECT_INITIAL_MS,
        reconnect_max_ms = DEFAULT_PEER_RECONNECT_MAX_MS,
        reconnect_multiplier = DEFAULT_PEER_RECONNECT_MULTIPLIER,
        storage_proxy_enabled = DEFAULT_STORAGE_PROXY_ENABLED,
    )
}

/// Generate a JSON-LD config template with `@context` and all default values.
///
/// The `@context` maps config keys to the Fluree config vocabulary, making
/// the file valid JSON-LD that can be processed by standard JSON-LD tooling.
/// Serde ignores `@context` during deserialization, so the same serde types
/// work for both JSON and JSON-LD config files.
pub fn generate_jsonld_config_template() -> String {
    let template = json!({
        "@context": {
            "@vocab": CONFIG_VOCAB
        },
        "_comment": "Fluree Configuration — JSON-LD format. Precedence: CLI > env > profile > file > defaults. Remove keys to use built-in defaults.",
        "server": {
            "listen_addr": DEFAULT_LISTEN_ADDR,
            "storage_path": DEFAULT_STORAGE_PATH,
            "log_level": DEFAULT_LOG_LEVEL,
            "cors_enabled": DEFAULT_CORS_ENABLED,
            "body_limit": DEFAULT_BODY_LIMIT,
            "cache_max_entries": DEFAULT_CACHE_MAX_ENTRIES,
            "indexing": {
                "enabled": DEFAULT_INDEXING_ENABLED,
                "reindex_min_bytes": DEFAULT_REINDEX_MIN_BYTES,
                "reindex_max_bytes": DEFAULT_REINDEX_MAX_BYTES
            },
            "auth": {
                "events": { "mode": DEFAULT_AUTH_MODE },
                "data": { "mode": DEFAULT_AUTH_MODE },
                "admin": { "mode": DEFAULT_AUTH_MODE },
                "jwks": { "cache_ttl": DEFAULT_JWKS_CACHE_TTL }
            },
            "mcp": { "enabled": DEFAULT_MCP_ENABLED },
            "peer": {
                "role": DEFAULT_PEER_ROLE,
                "reconnect": {
                    "initial_ms": DEFAULT_PEER_RECONNECT_INITIAL_MS,
                    "max_ms": DEFAULT_PEER_RECONNECT_MAX_MS,
                    "multiplier": DEFAULT_PEER_RECONNECT_MULTIPLIER
                }
            },
            "storage_proxy": { "enabled": DEFAULT_STORAGE_PROXY_ENABLED }
        }
    });
    serde_json::to_string_pretty(&template).expect("template serialization cannot fail")
}

/// Generate a config template in the given format.
pub fn generate_config_template_for(format: ConfigFormat) -> String {
    match format {
        ConfigFormat::Toml => generate_config_template(),
        ConfigFormat::JsonLd => generate_jsonld_config_template(),
    }
}

/// Validate the `@context` in a JSON-LD config file.
///
/// Uses `fluree_graph_json_ld::parse_context()` to confirm the context is
/// well-formed. Logs a warning if `@vocab` is not the standard Fluree config
/// vocabulary. Returns `Ok(())` on success or if no `@context` is present.
pub fn validate_jsonld_context(json: &serde_json::Value) -> Result<(), String> {
    let Some(ctx_value) = json.get("@context") else {
        return Ok(());
    };

    let parsed = fluree_graph_json_ld::parse_context(ctx_value)
        .map_err(|e| format!("invalid @context in config: {e}"))?;

    if parsed.vocab.as_deref() != Some(CONFIG_VOCAB) {
        tracing::warn!(
            vocab = ?parsed.vocab,
            expected = CONFIG_VOCAB,
            "JSON-LD config @vocab does not match the standard Fluree config vocabulary"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_contains_default_values() {
        let t = generate_config_template();
        // Spot-check a few interpolated values
        assert!(t.contains(&format!("# listen_addr = \"{}\"", DEFAULT_LISTEN_ADDR)));
        assert!(t.contains(&format!(
            "# reindex_min_bytes = {}",
            DEFAULT_REINDEX_MIN_BYTES
        )));
        assert!(t.contains(&format!("# log_level = \"{}\"", DEFAULT_LOG_LEVEL)));
        assert!(t.contains(&format!("# enabled = {}", DEFAULT_INDEXING_ENABLED)));
        assert!(t.contains(&format!(
            "# cache_max_entries = {}",
            DEFAULT_CACHE_MAX_ENTRIES
        )));
    }

    #[test]
    fn jsonld_template_is_valid_json_with_context() {
        let t = generate_jsonld_config_template();
        let v: serde_json::Value = serde_json::from_str(&t).unwrap();

        // Has @context with correct @vocab
        let vocab = v["@context"]["@vocab"].as_str().unwrap();
        assert_eq!(vocab, CONFIG_VOCAB);

        // Has server settings with correct defaults
        assert_eq!(
            v["server"]["listen_addr"].as_str().unwrap(),
            DEFAULT_LISTEN_ADDR
        );
        assert_eq!(
            v["server"]["indexing"]["reindex_min_bytes"]
                .as_u64()
                .unwrap() as usize,
            DEFAULT_REINDEX_MIN_BYTES
        );
    }

    #[test]
    fn validate_context_accepts_valid_vocab() {
        let doc = json!({
            "@context": { "@vocab": CONFIG_VOCAB },
            "server": {}
        });
        assert!(validate_jsonld_context(&doc).is_ok());
    }

    #[test]
    fn validate_context_accepts_missing_context() {
        let doc = json!({ "server": {} });
        assert!(validate_jsonld_context(&doc).is_ok());
    }

    #[test]
    fn validate_context_rejects_malformed() {
        // An integer is not a valid @context
        let doc = json!({ "@context": 42 });
        assert!(validate_jsonld_context(&doc).is_err());
    }

    #[test]
    fn config_format_filenames() {
        assert_eq!(ConfigFormat::Toml.filename(), "config.toml");
        assert_eq!(ConfigFormat::JsonLd.filename(), "config.jsonld");
    }

    #[test]
    fn generate_template_for_dispatches() {
        let toml = generate_config_template_for(ConfigFormat::Toml);
        assert!(toml.starts_with("# Fluree Configuration"));

        let jsonld = generate_config_template_for(ConfigFormat::JsonLd);
        let v: serde_json::Value = serde_json::from_str(&jsonld).unwrap();
        assert!(v.get("@context").is_some());
    }
}
