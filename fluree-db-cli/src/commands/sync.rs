//! Sync commands: fetch, pull, push

use crate::config::{storage_path, TomlSyncConfigStore};
use crate::context;
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_nameservice::{
    FileTrackingStore, RefKind, RefPublisher, RemoteName, RemoteTrackingStore,
};
use fluree_db_nameservice_sync::{
    FetchResult, HttpRemoteClient, RemoteEndpoint, SyncConfigStore, SyncDriver,
};
use futures::StreamExt;
use std::path::Path;
use std::sync::Arc;

fn token_has_storage_permissions(token: &str) -> Option<bool> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let payload_bytes = URL_SAFE_NO_PAD.decode(parts[1]).ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&payload_bytes).ok()?;

    let storage_all = claims
        .get("fluree.storage.all")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let storage_ledgers_len = claims
        .get("fluree.storage.ledgers")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);

    Some(storage_all || storage_ledgers_len > 0)
}

fn replication_permission_error(remote: &str) -> CliError {
    CliError::Config(format!(
        "this operation replicates ledger refs (pull/push/fetch) and requires a root token with `fluree.storage.*` permissions.\n  {} if you only have query access, use `fluree track` (or `--remote`) and run queries/transactions remotely.\n  {} fluree auth login --remote {}",
        "hint:".cyan().bold(),
        "hint:".cyan().bold(),
        remote
    ))
}

fn map_sync_auth_error(remote: &str, err: &str) -> Option<CliError> {
    // `fluree-db-nameservice-sync` reports remote failures as strings; match the common
    // permission-related server errors and provide a clearer CLI message.
    if err.contains("401")
        || err.contains("403")
        || err.contains("Bearer token required")
        || err.contains("Untrusted issuer")
        || err.contains("Token lacks storage proxy permissions")
        || err.contains("Storage proxy not enabled")
    {
        Some(replication_permission_error(remote))
    } else {
        None
    }
}

/// Build a SyncDriver with all configured remotes
async fn build_sync_driver(fluree_dir: &Path) -> CliResult<(SyncDriver, Arc<TomlSyncConfigStore>)> {
    let fluree = context::build_fluree(fluree_dir)?;
    let config_store = Arc::new(TomlSyncConfigStore::new(fluree_dir.to_path_buf()));

    // Get the nameservice as RefPublisher
    let ns = fluree.nameservice();
    let local: Arc<dyn RefPublisher> = Arc::new(ns.clone());

    // Create a FileTrackingStore using the same storage path
    let storage = storage_path(fluree_dir);
    let tracking: Arc<dyn RemoteTrackingStore> = Arc::new(FileTrackingStore::new(&storage));

    let mut driver = SyncDriver::new(
        local,
        tracking,
        config_store.clone() as Arc<dyn SyncConfigStore>,
    );

    // Add HTTP clients for all configured remotes
    let remotes = config_store
        .list_remotes()
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    for remote in remotes {
        match &remote.endpoint {
            RemoteEndpoint::Http { base_url } => {
                let client = Arc::new(HttpRemoteClient::new(
                    base_url.clone(),
                    remote.auth.token.clone(),
                ));
                driver.add_client(&remote.name, client);
            }
            RemoteEndpoint::Sse { .. } | RemoteEndpoint::Storage { .. } => {
                // Skip non-HTTP remotes for now
                eprintln!(
                    "{} skipping non-HTTP remote '{}'",
                    "warning:".yellow().bold(),
                    remote.name.as_str()
                );
            }
        }
    }

    Ok((driver, config_store))
}

/// Fetch refs from a remote (like git fetch)
pub async fn run_fetch(remote: &str, fluree_dir: &Path) -> CliResult<()> {
    // Proactively fail with a clear message for query-only tokens.
    let store = TomlSyncConfigStore::new(fluree_dir.to_path_buf());
    let remote_cfg = store
        .get_remote(&RemoteName::new(remote))
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{}' not found", remote)))?;
    if let Some(tok) = &remote_cfg.auth.token {
        if let Some(false) = token_has_storage_permissions(tok) {
            return Err(replication_permission_error(remote));
        }
    }

    let (driver, _config) = build_sync_driver(fluree_dir).await?;
    let remote_name = RemoteName::new(remote);

    println!("Fetching from '{}'...", remote.cyan());

    let result = driver.fetch_remote(&remote_name).await.map_err(|e| {
        let msg = e.to_string();
        map_sync_auth_error(remote, &msg)
            .unwrap_or_else(|| CliError::Config(format!("fetch failed: {msg}")))
    })?;

    print_fetch_result(&result);
    Ok(())
}

fn print_fetch_result(result: &FetchResult) {
    if result.updated.is_empty() && result.unchanged.is_empty() {
        println!("No ledgers found on remote.");
        return;
    }

    if !result.updated.is_empty() {
        println!("{}", "Updated:".green().bold());
        for (ledger_id, tracking) in &result.updated {
            let t = tracking.commit_ref.as_ref().map(|r| r.t).unwrap_or(0);
            println!("  {} -> t={}", ledger_id, t);
        }
    }

    if !result.unchanged.is_empty() {
        println!(
            "{} {} ledger(s) unchanged",
            "Already up to date:".dimmed(),
            result.unchanged.len()
        );
    }
}

/// Pull commits from upstream and apply them to the local ledger.
///
/// Downloads commit blobs page-by-page (newest→oldest) until we reach local
/// history, then applies them oldest→newest via `import_commits_incremental`.
pub async fn run_pull(ledger: Option<&str>, fluree_dir: &Path) -> CliResult<()> {
    let ledger_id = context::resolve_ledger(ledger, fluree_dir)?;
    let ledger_id = context::to_ledger_id(&ledger_id);

    let config_store = TomlSyncConfigStore::new(fluree_dir.to_path_buf());
    let upstream = config_store
        .get_upstream(&ledger_id)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;

    let Some(upstream) = upstream else {
        return Err(CliError::Config(format!(
            "no upstream configured for '{}'\n  {} fluree upstream set {} <remote>",
            ledger_id,
            "hint:".cyan().bold(),
            ledger_id
        )));
    };

    // Resolve remote config → build client.
    let remote_cfg = config_store
        .get_remote(&upstream.remote)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{}' not found", upstream.remote)))?;

    // Proactively fail with a clear message for query-only tokens.
    if let Some(tok) = &remote_cfg.auth.token {
        if let Some(false) = token_has_storage_permissions(tok) {
            return Err(replication_permission_error(upstream.remote.as_str()));
        }
    }

    let base_url = match &remote_cfg.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        _ => {
            return Err(CliError::Config(format!(
                "remote '{}' is not an HTTP remote",
                upstream.remote.as_str()
            )));
        }
    };

    let client = context::build_client_from_auth(&base_url, &remote_cfg.auth);

    // Use the remote ledger ID for all remote API calls (may differ from local ledger_id).
    // Note: UpstreamConfig field is named `remote_alias` (pre-existing; should be `remote_id`).
    let remote_ledger_id = &upstream.remote_alias;

    // Resolve remote head.
    let info = client
        .ledger_info(remote_ledger_id)
        .await
        .map_err(|e| CliError::Config(format!("pull failed (remote ledger info): {e}")))?;
    let remote_t = info
        .get("t")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| CliError::Config("remote ledger-info response missing 't'".into()))?;

    // Resolve local head.
    let fluree = context::build_fluree(fluree_dir)?;
    let local_ref = fluree
        .nameservice()
        .get_ref(&ledger_id, RefKind::CommitHead)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("local ledger '{}' not found", ledger_id)))?;

    if remote_t <= local_ref.t {
        println!("{} '{}' is already up to date", "✓".green(), ledger_id);
        context::persist_refreshed_tokens(&client, upstream.remote.as_str(), fluree_dir).await;
        return Ok(());
    }

    println!(
        "Pulling '{}' from '{}' (local t={}, remote t={})...",
        ledger_id.cyan(),
        upstream.remote.as_str().cyan(),
        local_ref.t,
        remote_t
    );

    // Fetch pages (newest→oldest) until we reach local history.
    let mut all_commits: Vec<fluree_db_api::Base64Bytes> = Vec::new();
    let mut all_blobs: std::collections::HashMap<String, fluree_db_api::Base64Bytes> =
        std::collections::HashMap::new();
    let mut cursor: Option<String> = None;

    loop {
        let page = client
            .fetch_commits(remote_ledger_id, cursor.as_deref(), 100)
            .await
            .map_err(|e| {
                let msg = e.to_string();
                map_sync_auth_error(upstream.remote.as_str(), &msg).unwrap_or_else(|| {
                    CliError::Config(format!("pull failed (fetch commits): {msg}"))
                })
            })?;

        for commit in &page.commits {
            all_commits.push(commit.clone());
        }
        for (addr, blob) in &page.blobs {
            all_blobs
                .entry(addr.clone())
                .or_insert_with(|| blob.clone());
        }

        // If this page reached our local history, stop fetching.
        if page.oldest_t <= local_ref.t + 1 {
            break;
        }
        match page.next_cursor_id {
            Some(cid) => cursor = Some(cid.to_string()),
            None => break, // Reached genesis.
        }
    }

    // Filter to only commits with t > local_t, then reverse to oldest→newest.
    use fluree_db_novelty::commit_v2::format::{CommitV2Header, HEADER_LEN};
    let mut to_import: Vec<fluree_db_api::Base64Bytes> = Vec::new();
    for commit in &all_commits {
        if commit.0.len() < HEADER_LEN {
            continue;
        }
        let header = CommitV2Header::read_from(&commit.0)
            .map_err(|e| CliError::Config(format!("invalid commit in pull response: {e}")))?;
        if header.t > local_ref.t {
            to_import.push(commit.clone());
        }
    }
    to_import.reverse(); // oldest→newest

    if to_import.is_empty() {
        println!("{} '{}' is already up to date", "✓".green(), ledger_id);
        context::persist_refreshed_tokens(&client, upstream.remote.as_str(), fluree_dir).await;
        return Ok(());
    }

    let count = to_import.len();

    // Import incrementally (validates chain, ancestry, writes blobs, advances head, updates novelty).
    let handle = fluree
        .ledger_cached(&ledger_id)
        .await
        .map_err(|e| CliError::Config(format!("failed to load ledger: {e}")))?;

    let result = fluree
        .import_commits_incremental(&handle, to_import, all_blobs)
        .await
        .map_err(|e| CliError::Config(format!("pull failed (import): {e}")))?;

    println!(
        "{} '{}' pulled {} commit(s) (new head t={})",
        "✓".green(),
        ledger_id,
        count,
        result.head_t
    );

    // Persist refreshed token if auto-refresh happened.
    context::persist_refreshed_tokens(&client, upstream.remote.as_str(), fluree_dir).await;
    Ok(())
}

/// Push a ledger to its upstream remote
pub async fn run_push(ledger: Option<&str>, fluree_dir: &Path) -> CliResult<()> {
    let ledger_id = context::resolve_ledger(ledger, fluree_dir)?;
    let ledger_id = context::to_ledger_id(&ledger_id);

    let config_store = TomlSyncConfigStore::new(fluree_dir.to_path_buf());
    let upstream = config_store
        .get_upstream(&ledger_id)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?;
    let Some(upstream) = upstream else {
        return Err(CliError::Config(format!(
            "no upstream configured for '{}'\n  {} fluree upstream set {} <remote>",
            ledger_id,
            "hint:".cyan().bold(),
            ledger_id
        )));
    };

    println!(
        "Pushing '{}' to '{}'...",
        ledger_id.cyan(),
        upstream.remote.as_str()
    );

    // Resolve remote config (HTTP only for commit-push).
    let remote_cfg = config_store
        .get_remote(&upstream.remote)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{}' not found", upstream.remote)))?;

    let base_url = match &remote_cfg.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        _ => {
            return Err(CliError::Config(format!(
                "remote '{}' is not an HTTP remote",
                upstream.remote.as_str()
            )));
        }
    };

    // Build remote ledger client (wire refresh if configured).
    let client = context::build_client_from_auth(&base_url, &remote_cfg.auth);

    // Use the remote ledger ID for all remote API calls (may differ from local ledger_id).
    let remote_ledger_id = &upstream.remote_alias;

    // Resolve remote head (t + commit CID).
    let info = client
        .ledger_info(remote_ledger_id)
        .await
        .map_err(|e| CliError::Config(format!("push failed (remote ledger info): {e}")))?;
    let remote_t = info
        .get("t")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| CliError::Config("remote ledger-info response missing 't'".into()))?;
    let remote_commit_id: Option<fluree_db_core::ContentId> = info
        .get("commitId")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok());

    // Resolve local head.
    let fluree = context::build_fluree(fluree_dir)?;
    let local_ref = fluree
        .nameservice()
        .get_ref(&ledger_id, RefKind::CommitHead)
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("local ledger '{}' not found", ledger_id)))?;

    if local_ref.t < remote_t {
        return Err(CliError::Config(format!(
            "push rejected; remote is ahead (local t={}, remote t={}). Pull first.",
            local_ref.t, remote_t
        )));
    }

    // Collect commits to push (oldest -> newest), ensuring the remote head is in our history.
    let local_head_cid = local_ref.id.clone().ok_or_else(|| {
        CliError::Config(format!(
            "local ledger '{}' has no commit head; nothing to push",
            ledger_id
        ))
    })?;

    // Use ContentStore for CID-based chain walking (storage-agnostic).
    let content_store =
        fluree_db_core::storage::content_store_for(fluree.storage().clone(), &ledger_id);

    let mut to_push_cids: Vec<fluree_db_core::ContentId> = Vec::new();
    let mut found_base = remote_t == 0 && remote_commit_id.is_none();

    let stream = fluree_db_novelty::trace_commit_envelopes_by_id(
        content_store.clone(),
        local_head_cid.clone(),
        remote_t,
    );
    futures::pin_mut!(stream);
    while let Some(item) = stream.next().await {
        let (cid, env) = item.map_err(|e| CliError::Config(e.to_string()))?;

        if env.t == remote_t {
            if remote_commit_id.as_ref() == Some(&cid)
                || (remote_t == 0 && remote_commit_id.is_none())
            {
                found_base = true;
                break;
            }
            return Err(CliError::Config(format!(
                "cannot push: histories diverged at t={remote_t} (remote head != local history). Pull first."
            )));
        }

        if env.t > remote_t {
            to_push_cids.push(cid);
        }
    }

    if !found_base {
        return Err(CliError::Config(
            "cannot push: remote head not found in local history. Pull first.".into(),
        ));
    }

    if to_push_cids.is_empty() {
        println!("{} '{}' is already up to date", "✓".green(), ledger_id);
        context::persist_refreshed_tokens(&client, upstream.remote.as_str(), fluree_dir).await;
        return Ok(());
    }

    to_push_cids.reverse(); // oldest -> newest

    // Build request: commit bytes + any referenced txn blobs.
    let mut commits = Vec::with_capacity(to_push_cids.len());
    let mut blobs: std::collections::HashMap<String, fluree_db_api::Base64Bytes> =
        std::collections::HashMap::new();

    for cid in &to_push_cids {
        use fluree_db_core::ContentStore;
        let bytes = content_store
            .get(cid)
            .await
            .map_err(|e| CliError::Config(format!("failed to read local commit {cid}: {e}")))?;
        let commit = fluree_db_novelty::commit_v2::read_commit(&bytes)
            .map_err(|e| CliError::Config(format!("failed to decode local commit {cid}: {e}")))?;
        commits.push(fluree_db_api::Base64Bytes(bytes));

        if let Some(txn_cid) = &commit.txn {
            let txn_key = txn_cid.to_string();
            if let std::collections::hash_map::Entry::Vacant(e) = blobs.entry(txn_key.clone()) {
                let txn_bytes = content_store.get(txn_cid).await.map_err(|e| {
                    CliError::Config(format!(
                        "commit references txn blob '{}' but it is not readable locally: {}",
                        txn_key, e
                    ))
                })?;
                e.insert(fluree_db_api::Base64Bytes(txn_bytes));
            }
        }
    }

    let req = fluree_db_api::PushCommitsRequest { commits, blobs };
    let resp = client
        .push_commits(remote_ledger_id, &req)
        .await
        .map_err(|e| CliError::Config(format!("push failed: {e}")))?;

    println!(
        "{} '{}' pushed {} commit(s) (new head t={})",
        "✓".green(),
        ledger_id,
        resp.accepted,
        resp.head.t
    );

    // Persist refreshed token if auto-refresh happened.
    context::persist_refreshed_tokens(&client, upstream.remote.as_str(), fluree_dir).await;
    Ok(())
}

/// Clone a ledger from a remote server.
///
/// Downloads all commits via paginated export (bulk import), sets the commit
/// head, and configures upstream tracking.
///
/// Usage: `fluree clone <remote> <ledger> [--alias <local-name>]`
pub async fn run_clone(
    remote_name: &str,
    ledger: &str,
    alias: Option<&str>,
    fluree_dir: &Path,
) -> CliResult<()> {
    let ledger_id = context::to_ledger_id(ledger);
    let local_id = alias.map_or_else(|| ledger_id.clone(), context::to_ledger_id);

    // Clone aliasing is supported: commits use CID-based references (not
    // storage addresses), so the local ledger ID can differ from the remote.

    // Resolve remote config.
    let config_store = TomlSyncConfigStore::new(fluree_dir.to_path_buf());
    let remote_cfg = config_store
        .get_remote(&RemoteName::new(remote_name))
        .await
        .map_err(|e| CliError::Config(e.to_string()))?
        .ok_or_else(|| CliError::NotFound(format!("remote '{}' not found", remote_name)))?;

    if let Some(tok) = &remote_cfg.auth.token {
        if let Some(false) = token_has_storage_permissions(tok) {
            return Err(replication_permission_error(remote_name));
        }
    }

    let base_url = match &remote_cfg.endpoint {
        RemoteEndpoint::Http { base_url } => base_url.clone(),
        _ => {
            return Err(CliError::Config(format!(
                "remote '{}' is not an HTTP remote",
                remote_name
            )));
        }
    };

    let client = context::build_client_from_auth(&base_url, &remote_cfg.auth);

    // Verify the remote ledger exists.
    let info = client
        .ledger_info(&ledger_id)
        .await
        .map_err(|e| CliError::Config(format!("clone failed (remote ledger info): {e}")))?;
    let remote_t = info
        .get("t")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| CliError::Config("remote ledger-info response missing 't'".into()))?;

    if remote_t == 0 {
        return Err(CliError::Config(format!(
            "remote ledger '{}' is empty (t=0); nothing to clone",
            ledger_id
        )));
    }

    println!(
        "Cloning '{}' from '{}' (remote t={})...",
        local_id.cyan(),
        remote_name.cyan(),
        remote_t
    );

    // Create the local ledger.
    let fluree = context::build_fluree(fluree_dir)?;
    fluree
        .create_ledger(&local_id)
        .await
        .map_err(|e| CliError::Config(format!("failed to create local ledger: {e}")))?;

    // Fetch all pages (bulk mode).
    let mut head_commit_id: Option<fluree_db_core::ContentId> = None;
    let mut head_t: i64 = 0;
    let mut total: usize = 0;
    let mut cursor: Option<String> = None;

    let handle = fluree
        .ledger_cached(&local_id)
        .await
        .map_err(|e| CliError::Config(format!("failed to load ledger: {e}")))?;

    loop {
        let page = client
            .fetch_commits(&ledger_id, cursor.as_deref(), 500)
            .await
            .map_err(|e| {
                let msg = e.to_string();
                map_sync_auth_error(remote_name, &msg).unwrap_or_else(|| {
                    CliError::Config(format!("clone failed (fetch commits): {msg}"))
                })
            })?;

        if head_commit_id.is_none() {
            head_commit_id = Some(page.head_commit_id.clone());
            head_t = page.head_t;
        }

        fluree
            .import_commits_bulk(&handle, &page)
            .await
            .map_err(|e| CliError::Config(format!("clone failed (import): {e}")))?;

        total += page.count;
        eprint!("  fetched {} commits...\r", total);

        match page.next_cursor_id {
            Some(cid) => cursor = Some(cid.to_string()),
            None => break,
        }
    }

    eprintln!(); // Clear progress line.

    // Set the commit head.
    let head_cid = head_commit_id
        .ok_or_else(|| CliError::Config("clone failed: no commits fetched from remote".into()))?;
    fluree
        .set_commit_head(&handle, &head_cid, head_t)
        .await
        .map_err(|e| CliError::Config(format!("clone failed (set head): {e}")))?;

    // Configure upstream tracking.
    // Note: UpstreamConfig fields use `_alias` naming (pre-existing; should be `_id`).
    use fluree_db_nameservice_sync::UpstreamConfig;
    config_store
        .set_upstream(&UpstreamConfig {
            local_alias: local_id.clone(),
            remote: RemoteName::new(remote_name),
            remote_alias: ledger_id.clone(),
            auto_pull: false,
        })
        .await
        .map_err(|e| CliError::Config(format!("failed to set upstream: {e}")))?;

    println!(
        "{} Cloned '{}' ({} commits, head t={})",
        "✓".green(),
        local_id,
        total,
        head_t
    );
    println!(
        "  {} upstream set to '{}/{}'",
        "→".cyan(),
        remote_name,
        ledger_id
    );

    // Persist refreshed token if auto-refresh happened.
    context::persist_refreshed_tokens(&client, remote_name, fluree_dir).await;
    Ok(())
}
