use crate::config;
use crate::error::CliResult;

/// Default config.toml template with commented-out server defaults.
const DEFAULT_CONFIG_TEMPLATE: &str = r#"# Fluree Configuration
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
# listen_addr = "0.0.0.0:8090"
# storage_path = "/var/lib/fluree"
# log_level = "info"                 # trace, debug, info, warn, error
# cors_enabled = true
# body_limit = 52428800              # 50 MB
# cache_max_entries = 10000

# [server.indexing]
# enabled = false
# reindex_min_bytes = 100000         # 100 KB — triggers background reindexing
# reindex_max_bytes = 1000000        # 1 MB — blocks commits until reindexed

# [server.auth.events]
# mode = "none"                      # none, optional, required
# # audience = "https://my-app.example.com"
# # trusted_issuers = ["did:key:z6Mk..."]

# [server.auth.data]
# mode = "none"                      # none, optional, required
# # audience = "https://my-app.example.com"
# # trusted_issuers = ["did:key:z6Mk..."]
# # default_policy_class = "ex:DefaultPolicy"

# [server.auth.admin]
# mode = "none"                      # none, required
# # trusted_issuers = ["did:key:z6Mk..."]

# [server.auth.jwks]
# # issuers = ["https://auth.example.com=https://auth.example.com/.well-known/jwks.json"]
# # cache_ttl = 300                  # seconds

# [server.mcp]
# enabled = false
# # auth_trusted_issuers = ["did:key:z6Mk..."]

# [server.peer]
# role = "transaction"               # transaction, peer
# # tx_server_url = "http://tx.internal:8090"
# # subscribe_all = false
# # ledgers = ["books:main"]
# # graph_sources = []

# [server.peer.reconnect]
# initial_ms = 1000
# max_ms = 30000
# multiplier = 2.0

# [server.storage_proxy]
# enabled = false
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
"#;

pub fn run(global: bool) -> CliResult<()> {
    let fluree_dir = config::init_fluree_dir(global, DEFAULT_CONFIG_TEMPLATE)?;
    println!("Initialized Fluree in {}", fluree_dir.display());
    Ok(())
}
