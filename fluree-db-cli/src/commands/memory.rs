use crate::cli::MemoryAction;
use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_memory::{
    MemoryFilter, MemoryInput, MemoryKind, MemoryStore, MemoryUpdate, RecallEngine, RecallResult,
    Scope, SecretDetector, Sensitivity,
};

pub async fn run(action: MemoryAction, dirs: &FlureeDir) -> CliResult<()> {
    match action {
        MemoryAction::Init => run_init(dirs).await,
        MemoryAction::Add {
            kind,
            text,
            tags,
            refs,
            severity,
            scope,
            sensitivity,
            rationale,
            alternatives,
            fact_kind,
            pref_scope,
            artifact_kind,
            format,
        } => {
            run_add(
                kind,
                text,
                tags,
                refs,
                severity,
                scope,
                sensitivity,
                rationale,
                alternatives,
                fact_kind,
                pref_scope,
                artifact_kind,
                &format,
                dirs,
            )
            .await
        }
        MemoryAction::Recall {
            query,
            limit,
            kind,
            tags,
            scope,
            format,
        } => run_recall(&query, limit, kind, tags, scope, &format, dirs).await,
        MemoryAction::Update {
            id,
            text,
            tags,
            refs,
            format,
        } => run_update(&id, text, tags, refs, &format, dirs).await,
        MemoryAction::Forget { id } => run_forget(&id, dirs).await,
        MemoryAction::Explain { id } => run_explain(&id, dirs).await,
        MemoryAction::Status => run_status(dirs).await,
        MemoryAction::Export => run_export(dirs).await,
        MemoryAction::Import { file } => run_import(&file, dirs).await,
        MemoryAction::McpInstall { ide } => run_mcp_install(ide.as_deref()),
    }
}

fn build_store(dirs: &FlureeDir) -> CliResult<MemoryStore> {
    let fluree = context::build_fluree(dirs)?;

    // Determine memory_dir: use .fluree/memory/ in local mode
    let memory_dir = if dirs.is_unified() {
        // Local mode — data_dir is typically .fluree/
        let dir = dirs.data_dir().join("memory");
        if dir.exists() || dirs.data_dir().join("storage").exists() {
            Some(dir)
        } else {
            None
        }
    } else {
        None // Global mode — no file sharing
    };

    Ok(MemoryStore::new(fluree, memory_dir))
}

async fn run_init(dirs: &FlureeDir) -> CliResult<()> {
    let store = build_store(dirs)?;

    // initialize() handles ledger + file structure (dirs, .gitignore, .ttl files)
    store.initialize().await.map_err(memory_err)?;

    // Migration: export existing ledger memories to .ttl files
    if let Some(memory_dir) = store.memory_dir() {
        let memory_dir = memory_dir.to_path_buf();
        let repo_ttl = fluree_db_memory::turtle_io::repo_ttl_path(&memory_dir);
        let user_ttl = fluree_db_memory::turtle_io::user_ttl_path(&memory_dir);

        let existing = store
            .current_memories(&MemoryFilter::default())
            .await
            .map_err(memory_err)?;
        if !existing.is_empty() {
            let repo_mems: Vec<_> = existing
                .iter()
                .filter(|m| m.scope == fluree_db_memory::Scope::Repo)
                .cloned()
                .collect();
            let user_mems: Vec<_> = existing
                .iter()
                .filter(|m| m.scope == fluree_db_memory::Scope::User)
                .cloned()
                .collect();

            if !repo_mems.is_empty() {
                fluree_db_memory::turtle_io::write_memory_file(
                    &repo_ttl,
                    &repo_mems,
                    fluree_db_memory::turtle_io::REPO_HEADER,
                )
                .map_err(memory_err)?;
            }
            if !user_mems.is_empty() {
                fluree_db_memory::turtle_io::write_memory_file(
                    &user_ttl,
                    &user_mems,
                    fluree_db_memory::turtle_io::USER_HEADER,
                )
                .map_err(memory_err)?;
            }

            fluree_db_memory::file_sync::update_hash(&memory_dir).map_err(memory_err)?;

            println!(
                "Migrated {} existing memories to .ttl files.",
                existing.len()
            );
        }

        println!("Memory store initialized at {}", memory_dir.display());
        println!();
        println!("To share repo memories via git, ensure .fluree/memory/ is not gitignored.");
        println!("If your .gitignore has '.fluree/', add these exceptions:");
        println!();
        println!("  !.fluree/memory/");
        println!("  !.fluree/memory/repo.ttl");
        println!("  !.fluree/memory/.gitignore");
    } else {
        println!("Memory store initialized.");
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_add(
    kind_str: String,
    text: Option<String>,
    tags: Vec<String>,
    refs: Vec<String>,
    severity: Option<String>,
    scope: Option<String>,
    sensitivity: Option<String>,
    rationale: Option<String>,
    alternatives: Option<String>,
    fact_kind: Option<String>,
    pref_scope: Option<String>,
    artifact_kind: Option<String>,
    format: &str,
    dirs: &FlureeDir,
) -> CliResult<()> {
    let kind = MemoryKind::parse(&kind_str).ok_or_else(|| {
        CliError::Usage(format!(
            "invalid memory kind '{}'; valid: fact, decision, constraint, preference, artifact",
            kind_str
        ))
    })?;

    let content = match text {
        Some(t) => t,
        None => {
            // Read from stdin
            use std::io::Read;
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .map_err(|e| CliError::Input(format!("failed to read stdin: {e}")))?;
            buf.trim().to_string()
        }
    };

    if content.is_empty() {
        return Err(CliError::Usage(
            "no content provided; use --text or pipe via stdin".to_string(),
        ));
    }

    // Check for secrets
    let content = if SecretDetector::has_secrets(&content) {
        eprintln!(
            "  warning: secrets detected in content — storing redacted version.\n  \
             Original content contained sensitive data that was replaced with [REDACTED]."
        );
        SecretDetector::redact(&content)
    } else {
        content
    };

    let severity = severity
        .map(|s| {
            fluree_db_memory::Severity::parse_str(&s).ok_or_else(|| {
                CliError::Usage(format!(
                    "invalid severity '{}'; valid: must, should, prefer",
                    s
                ))
            })
        })
        .transpose()?;

    let scope = scope
        .map(|s| {
            Scope::parse_str(&s)
                .ok_or_else(|| CliError::Usage(format!("invalid scope '{}'; valid: repo, user", s)))
        })
        .transpose()?
        .unwrap_or_default();

    let sensitivity = sensitivity
        .map(|s| {
            Sensitivity::parse_str(&s).ok_or_else(|| {
                CliError::Usage(format!(
                    "invalid sensitivity '{}'; valid: public, internal, client, secret",
                    s
                ))
            })
        })
        .transpose()?
        .unwrap_or_default();

    let branch = fluree_db_memory::detect_git_branch();

    let input = MemoryInput {
        kind,
        content,
        tags,
        scope,
        sensitivity,
        severity,
        artifact_refs: refs,
        branch,
        valid_from: None,
        valid_to: None,
        rationale,
        alternatives,
        fact_kind,
        pref_scope,
        artifact_kind,
    };

    let store = build_store(dirs)?;
    store.ensure_synced().await.map_err(memory_err)?;
    let id = store.add(input).await.map_err(memory_err)?;

    match format {
        "json" => {
            if let Some(mem) = store.get(&id).await.map_err(memory_err)? {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&fluree_db_memory::format_json(&mem))
                        .unwrap_or_default()
                );
            }
        }
        _ => {
            println!("Stored memory: {}", id);
        }
    }

    Ok(())
}

async fn run_recall(
    query: &str,
    limit: usize,
    kind: Option<String>,
    tags: Vec<String>,
    scope: Option<String>,
    format: &str,
    dirs: &FlureeDir,
) -> CliResult<()> {
    let kind_filter = kind
        .map(|s| {
            MemoryKind::parse(&s)
                .ok_or_else(|| CliError::Usage(format!("invalid memory kind '{}'", s)))
        })
        .transpose()?;

    let scope_filter = scope
        .map(|s| {
            Scope::parse_str(&s)
                .ok_or_else(|| CliError::Usage(format!("invalid scope '{}'; valid: repo, user", s)))
        })
        .transpose()?;

    let filter = MemoryFilter {
        kind: kind_filter,
        tags,
        branch: None,
        scope: scope_filter,
    };

    let store = build_store(dirs)?;
    store.ensure_synced().await.map_err(memory_err)?;

    // BM25 fulltext search for content relevance
    let bm25_hits = store
        .recall_fulltext(query, limit)
        .await
        .map_err(memory_err)?;

    // Load full memory objects for metadata re-ranking
    let all = store.current_memories(&filter).await.map_err(memory_err)?;

    let branch = fluree_db_memory::detect_git_branch();
    let scored = if bm25_hits.is_empty() {
        // Fallback to metadata-only scoring when BM25 returns nothing
        RecallEngine::recall_metadata_only(query, &all, branch.as_deref(), Some(limit))
    } else {
        RecallEngine::rerank(query, &bm25_hits, &all, branch.as_deref())
    };

    let result = RecallResult {
        query: query.to_string(),
        memories: scored.clone(),
        total_count: all.len(),
    };

    match format {
        "json" => {
            println!(
                "{}",
                serde_json::to_string_pretty(&fluree_db_memory::format_recall_json(&result))
                    .unwrap_or_default()
            );
        }
        "context" => {
            print!("{}", fluree_db_memory::format_context(&scored));
        }
        _ => {
            print!("{}", fluree_db_memory::format_recall_text(&result));
        }
    }

    Ok(())
}

async fn run_update(
    id: &str,
    text: Option<String>,
    tags: Option<Vec<String>>,
    refs: Option<Vec<String>>,
    format: &str,
    dirs: &FlureeDir,
) -> CliResult<()> {
    // Check for secrets in new content
    let text = text.map(|t| {
        if SecretDetector::has_secrets(&t) {
            eprintln!("  warning: secrets detected — storing redacted version.");
            SecretDetector::redact(&t)
        } else {
            t
        }
    });

    let update = MemoryUpdate {
        content: text,
        tags,
        severity: None,
        artifact_refs: refs,
        valid_from: None,
        valid_to: None,
        rationale: None,
        alternatives: None,
    };

    let store = build_store(dirs)?;
    store.ensure_synced().await.map_err(memory_err)?;
    let new_id = store.update(id, update).await.map_err(memory_err)?;

    match format {
        "json" => {
            if let Some(mem) = store.get(&new_id).await.map_err(memory_err)? {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&fluree_db_memory::format_json(&mem))
                        .unwrap_or_default()
                );
            }
        }
        _ => {
            println!("Updated: {} → {}", id, new_id);
        }
    }

    Ok(())
}

async fn run_forget(id: &str, dirs: &FlureeDir) -> CliResult<()> {
    let store = build_store(dirs)?;
    store.ensure_synced().await.map_err(memory_err)?;
    store.forget(id).await.map_err(memory_err)?;
    println!("Forgotten: {}", id);
    Ok(())
}

async fn run_explain(id: &str, dirs: &FlureeDir) -> CliResult<()> {
    let store = build_store(dirs)?;
    store.ensure_synced().await.map_err(memory_err)?;
    let chain = store.supersession_chain(id).await.map_err(memory_err)?;
    print!("{}", fluree_db_memory::format_explain(&chain));
    Ok(())
}

async fn run_status(dirs: &FlureeDir) -> CliResult<()> {
    let store = build_store(dirs)?;
    store.ensure_synced().await.map_err(memory_err)?;
    let status = store.status().await.map_err(memory_err)?;
    print!("{}", fluree_db_memory::format_status_text(&status));
    Ok(())
}

async fn run_export(dirs: &FlureeDir) -> CliResult<()> {
    let store = build_store(dirs)?;
    store.ensure_synced().await.map_err(memory_err)?;
    let data = store.export().await.map_err(memory_err)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&data).unwrap_or_default()
    );
    Ok(())
}

async fn run_import(file: &std::path::Path, dirs: &FlureeDir) -> CliResult<()> {
    let content = std::fs::read_to_string(file)
        .map_err(|e| CliError::Input(format!("failed to read {}: {e}", file.display())))?;
    let data: serde_json::Value = serde_json::from_str(&content)?;

    let store = build_store(dirs)?;
    store.ensure_synced().await.map_err(memory_err)?;
    let count = store.import(data).await.map_err(memory_err)?;
    println!("Imported {} memories.", count);
    Ok(())
}

fn run_mcp_install(ide: Option<&str>) -> CliResult<()> {
    // Resolve the fluree binary path
    let fluree_bin = std::env::current_exe()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| "fluree".to_string());

    let ide = ide
        .map(String::from)
        .unwrap_or_else(|| detect_ide().unwrap_or_else(|| "claude-code".to_string()));

    match ide.as_str() {
        "claude-code" => install_claude_code(&fluree_bin),
        "claude-vscode" => install_claude_vscode(&fluree_bin),
        "cursor" => install_cursor(&fluree_bin),
        other => Err(CliError::Usage(format!(
            "unknown IDE '{}'; valid: claude-code, claude-vscode, cursor",
            other
        ))),
    }
}

/// Auto-detect which IDE environment we're in.
fn detect_ide() -> Option<String> {
    // Check for Cursor-specific markers
    if std::path::Path::new(".cursor").exists() {
        return Some("cursor".to_string());
    }
    // Check for VS Code workspace
    if std::path::Path::new(".vscode").exists() {
        return Some("claude-vscode".to_string());
    }
    // Default to Claude Code CLI
    None
}

fn mcp_config_json(fluree_bin: &str) -> serde_json::Value {
    serde_json::json!({
        "mcpServers": {
            "fluree-memory": {
                "command": fluree_bin,
                "args": ["mcp", "serve", "--transport", "stdio"]
            }
        }
    })
}

fn install_claude_code(fluree_bin: &str) -> CliResult<()> {
    let config_path = std::path::Path::new(".mcp.json");

    let mut config = if config_path.exists() {
        let content = std::fs::read_to_string(config_path)
            .map_err(|e| CliError::Input(format!("failed to read .mcp.json: {e}")))?;
        serde_json::from_str::<serde_json::Value>(&content)?
    } else {
        serde_json::json!({ "mcpServers": {} })
    };

    // Add our server entry
    if let Some(servers) = config.get_mut("mcpServers").and_then(|v| v.as_object_mut()) {
        servers.insert(
            "fluree-memory".to_string(),
            serde_json::json!({
                "command": fluree_bin,
                "args": ["mcp", "serve", "--transport", "stdio"]
            }),
        );
    }

    std::fs::write(
        config_path,
        serde_json::to_string_pretty(&config).unwrap_or_default(),
    )
    .map_err(|e| CliError::Config(format!("failed to write .mcp.json: {e}")))?;

    println!("Installed MCP config: .mcp.json");

    // Append rules snippet to CLAUDE.md if it doesn't already mention fluree memory
    let claude_md = std::path::Path::new("CLAUDE.md");
    if claude_md.exists() {
        let content = std::fs::read_to_string(claude_md)
            .map_err(|e| CliError::Input(format!("failed to read CLAUDE.md: {e}")))?;
        if !content.contains("fluree memory") && !content.contains("memory_recall") {
            let snippet = "\n\n## Developer Memory\n\n\
                Use the `memory_recall` MCP tool at the start of tasks to retrieve project context.\n\
                Use `memory_add` to store important facts, decisions, and constraints.\n\
                See `fluree memory --help` for CLI usage.\n";
            std::fs::write(claude_md, format!("{}{}", content, snippet))
                .map_err(|e| CliError::Config(format!("failed to update CLAUDE.md: {e}")))?;
            println!("Appended memory instructions to CLAUDE.md");
        }
    }

    Ok(())
}

fn install_claude_vscode(fluree_bin: &str) -> CliResult<()> {
    let vscode_dir = std::path::Path::new(".vscode");
    std::fs::create_dir_all(vscode_dir)
        .map_err(|e| CliError::Config(format!("failed to create .vscode/: {e}")))?;

    let config_path = vscode_dir.join("mcp.json");

    // VS Code Claude extension uses "servers" key (not "mcpServers")
    let config = serde_json::json!({
        "servers": {
            "fluree-memory": {
                "command": fluree_bin,
                "args": ["mcp", "serve", "--transport", "stdio"]
            }
        }
    });

    std::fs::write(
        &config_path,
        serde_json::to_string_pretty(&config).unwrap_or_default(),
    )
    .map_err(|e| CliError::Config(format!("failed to write .vscode/mcp.json: {e}")))?;

    println!("Installed MCP config: .vscode/mcp.json");

    // Copy rules file
    let rules_src = include_str!("../../../fluree-db-memory/rules/fluree_rules.md");
    let rules_path = vscode_dir.join("fluree_rules.md");
    std::fs::write(&rules_path, rules_src)
        .map_err(|e| CliError::Config(format!("failed to write rules: {e}")))?;
    println!("Installed agent rules: .vscode/fluree_rules.md");

    Ok(())
}

fn install_cursor(fluree_bin: &str) -> CliResult<()> {
    let cursor_dir = std::path::Path::new(".cursor");
    std::fs::create_dir_all(cursor_dir)
        .map_err(|e| CliError::Config(format!("failed to create .cursor/: {e}")))?;

    let config_path = cursor_dir.join("mcp.json");
    let config = mcp_config_json(fluree_bin);

    std::fs::write(
        &config_path,
        serde_json::to_string_pretty(&config).unwrap_or_default(),
    )
    .map_err(|e| CliError::Config(format!("failed to write .cursor/mcp.json: {e}")))?;

    println!("Installed MCP config: .cursor/mcp.json");

    // Copy rules file
    let rules_dir = cursor_dir.join("rules");
    std::fs::create_dir_all(&rules_dir)
        .map_err(|e| CliError::Config(format!("failed to create .cursor/rules/: {e}")))?;

    let rules_src = include_str!("../../../fluree-db-memory/rules/fluree_rules.md");
    let rules_path = rules_dir.join("fluree_rules.md");
    std::fs::write(&rules_path, rules_src)
        .map_err(|e| CliError::Config(format!("failed to write rules: {e}")))?;
    println!("Installed agent rules: .cursor/rules/fluree_rules.md");

    Ok(())
}

/// Convert MemoryError to CliError.
fn memory_err(e: fluree_db_memory::MemoryError) -> CliError {
    match e {
        fluree_db_memory::MemoryError::NotFound(id) => {
            CliError::NotFound(format!("memory '{}' not found", id))
        }
        fluree_db_memory::MemoryError::Api(api_err) => CliError::Api(api_err),
        _ => CliError::Config(e.to_string()),
    }
}
