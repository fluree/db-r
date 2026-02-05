use crate::error::{CliError, CliResult};
use std::fs;
use std::path::{Path, PathBuf};

const FLUREE_DIR: &str = ".fluree";
const ACTIVE_FILE: &str = "active";
const STORAGE_DIR: &str = "storage";
const CONFIG_FILE: &str = "config.toml";
const PREFIXES_FILE: &str = "prefixes.json";

/// Walk up from `start` looking for a `.fluree/` directory.
fn find_fluree_dir_from(start: &Path) -> Option<PathBuf> {
    let mut current = start.to_path_buf();
    loop {
        let candidate = current.join(FLUREE_DIR);
        if candidate.is_dir() {
            return Some(candidate);
        }
        if !current.pop() {
            return None;
        }
    }
}

/// Find `.fluree/` by walking up from cwd. Returns `None` if not found.
pub fn find_fluree_dir() -> Option<PathBuf> {
    let cwd = std::env::current_dir().ok()?;
    find_fluree_dir_from(&cwd)
}

/// Find `.fluree/` by walking up from cwd, falling back to `~/.fluree/`.
pub fn find_or_global_fluree_dir() -> Option<PathBuf> {
    if let Some(d) = find_fluree_dir() {
        return Some(d);
    }
    let global = dirs::home_dir()?.join(FLUREE_DIR);
    if global.is_dir() {
        Some(global)
    } else {
        None
    }
}

/// Resolve a `--config` override to a `.fluree/` directory path.
///
/// Accepts either:
/// - A file path (e.g., `--config /path/to/.fluree/config.toml`) → uses parent dir
/// - A directory path (e.g., `--config /path/to/.fluree/`) → uses it directly
///
/// Validates the resolved directory exists and contains expected structure.
fn resolve_config_override(p: &Path) -> CliResult<PathBuf> {
    // Canonicalize to handle relative paths like `--config config.toml`
    let resolved = if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir()?.join(p)
    };

    if resolved.is_file() {
        // It's a file; use its parent directory as the .fluree/ dir
        let dir = resolved
            .parent()
            .ok_or_else(|| {
                CliError::Config(format!(
                    "cannot determine parent of: {}",
                    resolved.display()
                ))
            })?
            .to_path_buf();
        if dir.is_dir() {
            return Ok(dir);
        }
        return Err(CliError::Config(format!(
            "parent directory does not exist: {}",
            dir.display()
        )));
    }

    if resolved.is_dir() {
        return Ok(resolved);
    }

    Err(CliError::Config(format!(
        "config path does not exist: {}",
        p.display()
    )))
}

/// Require a local `.fluree/` directory (for mutating commands).
pub fn require_fluree_dir(config_override: Option<&Path>) -> CliResult<PathBuf> {
    if let Some(p) = config_override {
        return resolve_config_override(p);
    }
    find_fluree_dir().ok_or(CliError::NoFlureeDir)
}

/// Require a `.fluree/` directory, allowing global fallback (for read-only commands).
pub fn require_fluree_dir_or_global(config_override: Option<&Path>) -> CliResult<PathBuf> {
    if let Some(p) = config_override {
        return resolve_config_override(p);
    }
    find_or_global_fluree_dir().ok_or(CliError::NoFlureeDir)
}

/// Create `.fluree/` directory with empty config and storage subdirectory.
pub fn init_fluree_dir(global: bool) -> CliResult<PathBuf> {
    let base = if global {
        dirs::home_dir()
            .ok_or_else(|| CliError::Config("cannot determine home directory".into()))?
    } else {
        std::env::current_dir()?
    };
    let fluree_dir = base.join(FLUREE_DIR);
    let storage_dir = fluree_dir.join(STORAGE_DIR);

    fs::create_dir_all(&storage_dir).map_err(|e| {
        CliError::Config(format!("failed to create {}: {e}", storage_dir.display()))
    })?;

    let config_path = fluree_dir.join(CONFIG_FILE);
    if !config_path.exists() {
        fs::write(&config_path, "").map_err(|e| {
            CliError::Config(format!("failed to create {}: {e}", config_path.display()))
        })?;
    }

    Ok(fluree_dir)
}

/// Read the currently active ledger alias from `.fluree/active`.
pub fn read_active_ledger(fluree_dir: &Path) -> Option<String> {
    let path = fluree_dir.join(ACTIVE_FILE);
    fs::read_to_string(&path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Write the active ledger alias to `.fluree/active`.
pub fn write_active_ledger(fluree_dir: &Path, alias: &str) -> CliResult<()> {
    let path = fluree_dir.join(ACTIVE_FILE);
    fs::write(&path, alias).map_err(|e| {
        CliError::Config(format!(
            "failed to write active ledger to {}: {e}",
            path.display()
        ))
    })
}

/// Clear the active ledger (remove `.fluree/active`).
pub fn clear_active_ledger(fluree_dir: &Path) -> CliResult<()> {
    let path = fluree_dir.join(ACTIVE_FILE);
    if path.exists() {
        fs::remove_file(&path)
            .map_err(|e| CliError::Config(format!("failed to clear active ledger: {e}")))?;
    }
    Ok(())
}

/// Resolve the storage path for the Fluree instance.
pub fn storage_path(fluree_dir: &Path) -> PathBuf {
    fluree_dir.join(STORAGE_DIR)
}

/// Prefix map type: prefix -> IRI namespace
pub type PrefixMap = std::collections::HashMap<String, String>;

/// Read stored prefixes from `.fluree/prefixes.json`.
pub fn read_prefixes(fluree_dir: &Path) -> PrefixMap {
    let path = fluree_dir.join(PREFIXES_FILE);
    fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

/// Write prefixes to `.fluree/prefixes.json`.
pub fn write_prefixes(fluree_dir: &Path, prefixes: &PrefixMap) -> CliResult<()> {
    let path = fluree_dir.join(PREFIXES_FILE);
    let json = serde_json::to_string_pretty(prefixes)
        .map_err(|e| CliError::Config(format!("failed to serialize prefixes: {e}")))?;
    fs::write(&path, json).map_err(|e| CliError::Config(format!("failed to write prefixes: {e}")))
}

/// Add a prefix mapping.
pub fn add_prefix(fluree_dir: &Path, prefix: &str, iri: &str) -> CliResult<()> {
    let mut prefixes = read_prefixes(fluree_dir);
    prefixes.insert(prefix.to_string(), iri.to_string());
    write_prefixes(fluree_dir, &prefixes)
}

/// Remove a prefix mapping.
pub fn remove_prefix(fluree_dir: &Path, prefix: &str) -> CliResult<bool> {
    let mut prefixes = read_prefixes(fluree_dir);
    let existed = prefixes.remove(prefix).is_some();
    if existed {
        write_prefixes(fluree_dir, &prefixes)?;
    }
    Ok(existed)
}

/// Expand a compact IRI (e.g., "ex:alice") using stored prefixes.
/// Returns the original string if no prefix matches.
pub fn expand_iri(fluree_dir: &Path, compact: &str) -> String {
    if let Some((prefix, local)) = compact.split_once(':') {
        // Don't expand if it looks like an absolute IRI
        if local.starts_with("//") {
            return compact.to_string();
        }
        let prefixes = read_prefixes(fluree_dir);
        if let Some(namespace) = prefixes.get(prefix) {
            return format!("{namespace}{local}");
        }
    }
    compact.to_string()
}

/// Build a JSON-LD @context object from stored prefixes.
pub fn prefixes_to_context(fluree_dir: &Path) -> serde_json::Value {
    let prefixes = read_prefixes(fluree_dir);
    serde_json::Value::Object(
        prefixes
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::String(v)))
            .collect(),
    )
}
