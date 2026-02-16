use crate::config::{self, TomlSyncConfigStore, TrackedLedgerConfig};
use crate::error::{CliError, CliResult};
use crate::remote_client::{RefreshConfig, RemoteLedgerClient};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::{FileStorage, Fluree, FlureeBuilder};
use fluree_db_nameservice::file::FileNameService;
use fluree_db_nameservice::RemoteName;
use fluree_db_nameservice_sync::{
    RemoteAuth, RemoteAuthType, RemoteConfig, RemoteEndpoint, SyncConfigStore,
};

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
/// 2. Compound `remote/ledger` syntax (e.g., "origin/mydb") → remote query
/// 3. Local ledger with this alias exists → LedgerMode::Local
/// 4. Tracked config for this alias exists → LedgerMode::Tracked
/// 5. Error
pub async fn resolve_ledger_mode(
    explicit: Option<&str>,
    dirs: &FlureeDir,
) -> CliResult<LedgerMode> {
    let alias = resolve_ledger(explicit, dirs)?;

    // Strip `#fragment` (e.g., `#txn-meta`) for ledger resolution.
    // The fragment selects a named graph and is handled later by
    // `fluree.view()` / `parse_graph_ref()`. Existence checks and
    // tracked-config lookups must use just the ledger portion.
    let (ledger_part, _graph_fragment) = match alias.split_once('#') {
        Some((base, _frag)) => (base, Some(_frag)),
        None => (alias.as_str(), None),
    };

    // Try compound remote/ledger syntax (e.g., "origin/mydb")
    if let Some(mode) = try_compound_remote_syntax(ledger_part, dirs).await? {
        // For remote mode, pass through the full alias (with fragment) so the
        // server can resolve the graph. Currently remote doesn't support this,
        // but it avoids silently dropping the fragment.
        return Ok(mode);
    }

    let fluree = build_fluree(dirs)?;

    // Check if local ledger exists (local wins)
    let ledger_id = to_ledger_id(ledger_part);
    if fluree.ledger_exists(&ledger_id).await.unwrap_or(false) {
        return Ok(LedgerMode::Local {
            fluree: Box::new(fluree),
            alias,
        });
    }

    // Check tracked config
    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    if let Some(tracked) = store.get_tracked(ledger_part) {
        return build_tracked_mode(&store, &tracked, ledger_part).await;
    }

    // Also try the normalized ledger_id (user might have typed "mydb" but tracked as "mydb:main")
    if ledger_part != ledger_id {
        if let Some(tracked) = store.get_tracked(&ledger_id) {
            return build_tracked_mode(&store, &tracked, &ledger_id).await;
        }
    }

    // Also try the base name without branch suffix (user typed "mydb:main" but tracked as "mydb").
    // This handles configs created before track-time normalization was added.
    if let Some(base) = ledger_part.split(':').next() {
        if base != ledger_part && base != ledger_id {
            if let Some(tracked) = store.get_tracked(base) {
                return build_tracked_mode(&store, &tracked, ledger_part).await;
            }
        }
    }

    // Not found locally or tracked
    let display = ledger_part;
    Err(CliError::NotFound(format!(
        "ledger '{}' not found locally or in tracked config.\n  \
         Use `fluree create {}` to create locally, `fluree track add {}` to track a remote,\n  \
         or use remote/ledger syntax (e.g., origin/{}).",
        display, display, display, display
    )))
}

/// Try to parse `alias` as `remote_name/ledger_alias` compound syntax.
///
/// If the alias contains `/` and the part before the first `/` matches
/// a configured remote name, returns `Some(LedgerMode::Tracked)`.
/// Otherwise returns `None` to let the caller fall through to other resolution.
///
/// Remote names are validated to not contain `/` on `fluree remote add`,
/// so the first `/` is always an unambiguous delimiter.
async fn try_compound_remote_syntax(
    alias: &str,
    dirs: &FlureeDir,
) -> CliResult<Option<LedgerMode>> {
    let slash_pos = match alias.find('/') {
        Some(pos) if pos > 0 => pos,
        _ => return Ok(None),
    };

    let remote_name = &alias[..slash_pos];
    let ledger_alias = &alias[slash_pos + 1..];

    if ledger_alias.is_empty() {
        return Ok(None);
    }

    // Check if a remote with this name exists
    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let remote_key = RemoteName::new(remote_name);
    let remote = match store.get_remote(&remote_key).await {
        Ok(Some(r)) => r,
        Ok(None) => return Ok(None), // Not a known remote — fall through
        Err(e) => return Err(CliError::Config(e.to_string())),
    };

    let base_url = match &remote.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        _ => {
            return Err(CliError::Config(format!(
                "remote '{}' is not an HTTP remote",
                remote_name
            )));
        }
    };

    let client = build_client_from_auth(&base_url, &remote.auth);
    Ok(Some(LedgerMode::Tracked {
        client,
        remote_alias: ledger_alias.to_string(),
        local_alias: alias.to_string(),
        remote_name: remote_name.to_string(),
    }))
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
    dirs: &FlureeDir,
) -> CliResult<LedgerMode> {
    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
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

/// Build a `RemoteLedgerClient` for a named remote (no ledger alias needed).
///
/// Returns `(client, remote_name)` for use by commands that operate on the
/// remote itself rather than a specific ledger (e.g., `list --remote`).
pub async fn build_remote_client(
    remote_name_str: &str,
    dirs: &FlureeDir,
) -> CliResult<RemoteLedgerClient> {
    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
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

    Ok(build_client_from_auth(&base_url, &remote.auth))
}

/// Build a `RemoteLedgerClient` from auth config, wiring up refresh if available.
pub fn build_client_from_auth(base_url: &str, auth: &RemoteAuth) -> RemoteLedgerClient {
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
pub fn resolve_ledger(explicit: Option<&str>, dirs: &FlureeDir) -> CliResult<String> {
    if let Some(alias) = explicit {
        return Ok(alias.to_string());
    }
    config::read_active_ledger(dirs.data_dir()).ok_or(CliError::NoActiveLedger)
}

/// Build a Fluree instance using the resolved storage path.
///
/// Honors `[server].storage_path` from the config file if set,
/// otherwise falls back to `dirs.data_dir()/storage`.
pub fn build_fluree(dirs: &FlureeDir) -> CliResult<Fluree<FileStorage, FileNameService>> {
    let storage = config::resolve_storage_path(dirs);
    let storage_str = storage.to_string_lossy().to_string();
    FlureeBuilder::file(storage_str)
        .with_ledger_caching()
        .build()
        .map_err(|e| CliError::Config(format!("failed to initialize Fluree: {e}")))
}

/// Normalize a ledger identifier to include a branch suffix if missing.
///
/// The nameservice uses canonical ledger IDs like `mydb:main`.
/// When users provide just `mydb`, we append `:main`.
pub fn to_ledger_id(ledger_id: &str) -> String {
    fluree_db_core::normalize_ledger_id(ledger_id).unwrap_or_else(|_| ledger_id.to_string())
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
    dirs: &FlureeDir,
) {
    let refreshed = match client.take_refreshed_tokens() {
        Some(t) => t,
        None => return,
    };

    let store = TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
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
