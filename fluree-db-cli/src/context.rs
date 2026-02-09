use crate::config::{self, TomlSyncConfigStore, TrackedLedgerConfig};
use crate::error::{CliError, CliResult};
use crate::remote_client::{RefreshConfig, RemoteLedgerClient};
use fluree_db_api::{FileStorage, Fluree, FlureeBuilder};
use fluree_db_nameservice::file::FileNameService;
use fluree_db_nameservice::RemoteName;
use fluree_db_nameservice_sync::{
    RemoteAuth, RemoteAuthType, RemoteConfig, RemoteEndpoint, SyncConfigStore,
};
use std::path::Path;

/// Resolved ledger mode: either local or tracked (remote-only).
pub enum LedgerMode {
    /// Local ledger via Fluree API (traditional path).
    Local {
        fluree: Box<Fluree<FileStorage, FileNameService>>,
        alias: String,
    },
    /// Remote-only tracked ledger via HTTP.
    Tracked {
        client: RemoteLedgerClient,
        /// The alias on the remote server.
        remote_alias: String,
        /// The local alias the user used.
        local_alias: String,
        /// The remote config name (for persisting refreshed tokens).
        remote_name: String,
    },
}

/// Resolve which ledger to operate on and how (local vs tracked).
///
/// Resolution precedence:
/// 1. `--remote <name>` flag → temporary RemoteLedgerClient (caller provides this)
/// 2. Local ledger with this alias exists → LedgerMode::Local
/// 3. Tracked config for this alias exists → LedgerMode::Tracked
/// 4. Error
pub async fn resolve_ledger_mode(
    explicit: Option<&str>,
    fluree_dir: &Path,
) -> CliResult<LedgerMode> {
    let alias = resolve_ledger(explicit, fluree_dir)?;
    let fluree = build_fluree(fluree_dir)?;

    // Check if local ledger exists (local wins)
    let address = to_ledger_id(&alias);
    if fluree.ledger_exists(&address).await.unwrap_or(false) {
        return Ok(LedgerMode::Local {
            fluree: Box::new(fluree),
            alias,
        });
    }

    // Check tracked config
    let store = TomlSyncConfigStore::new(fluree_dir.to_path_buf());
    if let Some(tracked) = store.get_tracked(&alias) {
        return build_tracked_mode(&store, &tracked, &alias).await;
    }

    // Also try the normalized address (user might have typed "mydb" but tracked as "mydb:main")
    if alias != address {
        if let Some(tracked) = store.get_tracked(&address) {
            return build_tracked_mode(&store, &tracked, &address).await;
        }
    }

    // Not found locally or tracked
    Err(CliError::NotFound(format!(
        "ledger '{}' not found locally or in tracked config.\n  \
         Use `fluree create {}` to create locally, or `fluree track add {}` to track a remote.",
        alias, alias, alias
    )))
}

/// Build a `LedgerMode::Tracked` from a tracked config entry.
async fn build_tracked_mode(
    store: &TomlSyncConfigStore,
    tracked: &TrackedLedgerConfig,
    local_alias: &str,
) -> CliResult<LedgerMode> {
    let remote_name = RemoteName::new(&tracked.remote);
    let remote = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| {
            CliError::Config(format!(
                "remote '{}' referenced by tracked ledger '{}' not found in config",
                tracked.remote, local_alias
            ))
        })?;

    let base_url = match &remote.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        _ => {
            return Err(CliError::Config(format!(
                "remote '{}' is not an HTTP remote; tracking requires HTTP",
                tracked.remote
            )));
        }
    };

    let client = build_client_from_auth(&base_url, &remote.auth);
    Ok(LedgerMode::Tracked {
        client,
        remote_alias: tracked.remote_alias.clone(),
        local_alias: local_alias.to_string(),
        remote_name: tracked.remote.clone(),
    })
}

/// Build a `LedgerMode::Tracked` for a one-shot --remote flag.
pub async fn build_remote_mode(
    remote_name_str: &str,
    ledger_alias: &str,
    fluree_dir: &Path,
) -> CliResult<LedgerMode> {
    let store = TomlSyncConfigStore::new(fluree_dir.to_path_buf());
    let remote_name = RemoteName::new(remote_name_str);
    let remote = store
        .get_remote(&remote_name)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{}' not found", remote_name_str)))?;

    let base_url = match &remote.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        _ => {
            return Err(CliError::Config(format!(
                "remote '{}' is not an HTTP remote",
                remote_name_str
            )));
        }
    };

    let client = build_client_from_auth(&base_url, &remote.auth);
    Ok(LedgerMode::Tracked {
        client,
        remote_alias: ledger_alias.to_string(),
        local_alias: ledger_alias.to_string(),
        remote_name: remote_name_str.to_string(),
    })
}

/// Build a `RemoteLedgerClient` from auth config, wiring up refresh if available.
fn build_client_from_auth(base_url: &str, auth: &RemoteAuth) -> RemoteLedgerClient {
    let client = RemoteLedgerClient::new(base_url, auth.token.clone());

    // Attach refresh config for OIDC remotes that have a refresh_token + exchange_url
    if auth.auth_type.as_ref() == Some(&RemoteAuthType::OidcDevice) {
        if let (Some(exchange_url), Some(refresh_token)) = (&auth.exchange_url, &auth.refresh_token)
        {
            return client.with_refresh(RefreshConfig {
                exchange_url: exchange_url.clone(),
                refresh_token: refresh_token.clone(),
            });
        }
    }

    client
}

/// Resolve which ledger to operate on.
///
/// Priority: explicit argument > active ledger > error.
pub fn resolve_ledger(explicit: Option<&str>, fluree_dir: &Path) -> CliResult<String> {
    if let Some(alias) = explicit {
        return Ok(alias.to_string());
    }
    config::read_active_ledger(fluree_dir).ok_or(CliError::NoActiveLedger)
}

/// Build a Fluree instance backed by the `.fluree/storage/` directory.
pub fn build_fluree(fluree_dir: &Path) -> CliResult<Fluree<FileStorage, FileNameService>> {
    let storage = config::storage_path(fluree_dir);
    let storage_str = storage.to_string_lossy().to_string();
    FlureeBuilder::file(storage_str)
        .build()
        .map_err(|e| CliError::Config(format!("failed to initialize Fluree: {e}")))
}

/// Normalize an alias to include a branch suffix if missing.
///
/// The nameservice uses canonical addresses like `mydb:main`.
/// When users provide just `mydb`, we append `:main`.
pub fn to_ledger_id(alias: &str) -> String {
    if alias.contains(':') {
        alias.to_string()
    } else {
        format!("{alias}:main")
    }
}

/// Persist any refreshed tokens back to config.toml after a remote operation.
///
/// If the client performed a silent token refresh during a 401 retry, this
/// writes the new access_token (and optionally rotated refresh_token) back
/// to the remote's auth section in config.toml so subsequent commands use
/// the refreshed credentials.
pub async fn persist_refreshed_tokens(
    client: &RemoteLedgerClient,
    remote_name: &str,
    fluree_dir: &Path,
) {
    let refreshed = match client.take_refreshed_tokens() {
        Some(t) => t,
        None => return,
    };

    let store = TomlSyncConfigStore::new(fluree_dir.to_path_buf());
    let name = RemoteName::new(remote_name);

    let remote = match store.get_remote(&name).await {
        Ok(Some(r)) => r,
        _ => return, // Can't persist if remote config disappeared
    };

    let mut updated_auth = remote.auth.clone();
    updated_auth.token = Some(refreshed.access_token);
    if let Some(new_rt) = refreshed.refresh_token {
        updated_auth.refresh_token = Some(new_rt);
    }

    let updated = RemoteConfig {
        auth: updated_auth,
        ..remote
    };

    if store.set_remote(&updated).await.is_err() {
        eprintln!("  warning: failed to persist refreshed token to config");
    }
}
