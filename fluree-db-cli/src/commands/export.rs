use crate::context;
use crate::error::{CliError, CliResult};
use colored::Colorize;
use fluree_db_api::server_defaults::FlureeDir;
use fluree_vocab::xsd;
use std::path::Path;

pub async fn run(
    explicit_ledger: Option<&str>,
    format_str: &str,
    at: Option<&str>,
    output: Option<&Path>,
    dirs: &FlureeDir,
) -> CliResult<()> {
    // Check for tracked ledger — export requires local data
    let store = crate::config::TomlSyncConfigStore::new(dirs.config_dir().to_path_buf());
    let alias = context::resolve_ledger(explicit_ledger, dirs)?;
    if store.get_tracked(&alias).is_some()
        || store.get_tracked(&context::to_ledger_id(&alias)).is_some()
    {
        return Err(CliError::Usage(
            "export is not available for tracked ledgers (no local data).".to_string(),
        ));
    }

    match format_str.to_lowercase().as_str() {
        "ledger" | "flpack" => {
            if at.is_some() {
                return Err(CliError::Usage(
                    "--at is not supported with ledger format (exports full history)".to_string(),
                ));
            }
            run_ledger_export(&alias, output, dirs).await
        }
        _ => run_data_export(&alias, format_str, at, dirs).await,
    }
}

/// Export ledger data as Turtle or JSON-LD (point-in-time snapshot).
async fn run_data_export(
    alias: &str,
    format_str: &str,
    at: Option<&str>,
    dirs: &FlureeDir,
) -> CliResult<()> {
    let fluree = context::build_fluree(dirs)?;

    let graph = match at {
        Some(at_str) => {
            let spec = super::query::parse_time_spec(at_str);
            fluree.graph_at(alias, spec)
        }
        None => fluree.graph(alias),
    };

    let ledger = fluree.ledger(alias).await?;

    match format_str.to_lowercase().as_str() {
        "jsonld" | "json-ld" | "json" => {
            // CONSTRUCT all triples as JSON-LD graph
            let result = graph
                .query()
                .sparql("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")
                .execute()
                .await?;
            let json = result.to_construct(&ledger.snapshot)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&json).unwrap_or_else(|_| json.to_string())
            );
        }
        "turtle" | "ttl" => {
            // SELECT all triples and format as N-Triples
            let result = graph
                .query()
                .sparql("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
                .execute()
                .await?;
            let json = result.to_sparql_json(&ledger.snapshot)?;
            let output = format_ntriples(&json);
            print!("{output}");
        }
        other => {
            return Err(CliError::Usage(format!(
                "unknown export format '{other}'; valid formats: turtle, jsonld, ledger"
            )));
        }
    }

    Ok(())
}

/// Export the full native ledger (commits + indexes + dicts) as a `.flpack` file.
async fn run_ledger_export(alias: &str, output: Option<&Path>, dirs: &FlureeDir) -> CliResult<()> {
    use fluree_db_api::pack::{compute_missing_commits, compute_missing_index_artifacts};
    use fluree_db_core::pack::{
        encode_data_frame, encode_end_frame, encode_header_frame, encode_manifest_frame,
        estimate_pack_bytes, write_stream_preamble, PackHeader,
    };
    use fluree_db_core::storage::content_store_for;
    use fluree_db_core::ContentStore;
    use fluree_db_nameservice::NameService;
    use fluree_db_novelty::commit_v2::envelope::decode_envelope;
    use fluree_db_novelty::commit_v2::format::{CommitV2Header, HEADER_LEN};
    use std::collections::HashSet;
    use std::io::Write;

    let fluree = context::build_fluree(dirs)?;
    let ledger_id = context::to_ledger_id(alias);

    // Look up the nameservice record for this ledger.
    let ns_record = fluree
        .nameservice()
        .lookup(&ledger_id)
        .await
        .map_err(|e| CliError::Config(format!("nameservice lookup failed: {e}")))?
        .ok_or_else(|| CliError::NotFound(format!("ledger '{}' not found", alias)))?;

    let commit_head_id = ns_record.commit_head_id.as_ref().ok_or_else(|| {
        CliError::Usage(format!(
            "ledger '{}' has no commits (t=0); nothing to export",
            alias
        ))
    })?;

    // Determine output path.
    let default_name = format!("{}.flpack", alias.replace(':', "_"));
    let out_path = output
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from(&default_name));

    eprintln!(
        "Exporting ledger '{}' (t={}) to {}...",
        alias.cyan(),
        ns_record.commit_t,
        out_path.display()
    );

    let content_store = content_store_for(fluree.storage().clone(), &ledger_id);

    // Compute all commits (oldest-first).
    let missing_commits = compute_missing_commits(
        &content_store,
        std::slice::from_ref(commit_head_id),
        &HashSet::new(),
    )
    .await
    .map_err(|e| CliError::Config(format!("failed to walk commit chain: {e}")))?;

    // Compute index artifacts (if an index exists).
    let index_artifacts = if let Some(ref index_id) = ns_record.index_head_id {
        let artifacts = compute_missing_index_artifacts(&content_store, index_id, None)
            .await
            .map_err(|e| CliError::Config(format!("failed to enumerate index artifacts: {e}")))?;
        Some(artifacts)
    } else {
        None
    };

    let artifact_count = index_artifacts.as_ref().map_or(0, |a| a.len());

    // Build the pack stream into a file.
    let file = std::fs::File::create(&out_path)
        .map_err(|e| CliError::Input(format!("failed to create {}: {e}", out_path.display())))?;
    let mut writer = std::io::BufWriter::new(file);

    // Preamble + header.
    let mut buf = Vec::with_capacity(256);
    write_stream_preamble(&mut buf);

    let header = if artifact_count > 0 {
        let estimated = estimate_pack_bytes(missing_commits.len() as u32);
        PackHeader::with_indexes(
            Some(missing_commits.len() as u32),
            Some(artifact_count as u32),
            estimated,
        )
    } else {
        PackHeader::commits_only(Some(missing_commits.len() as u32))
    };
    encode_header_frame(&header, &mut buf);
    writer
        .write_all(&buf)
        .map_err(|e| CliError::Config(format!("write error: {e}")))?;

    // Stream commits + txn blobs.
    let mut commits_written = 0usize;
    let mut txn_blobs_written = 0usize;
    let mut txn_cids_sent = HashSet::new();

    for commit_cid in &missing_commits {
        let raw_bytes = content_store
            .get(commit_cid)
            .await
            .map_err(|e| CliError::Config(format!("failed to read commit {commit_cid}: {e}")))?;

        buf.clear();
        encode_data_frame(commit_cid, &raw_bytes, &mut buf);
        writer
            .write_all(&buf)
            .map_err(|e| CliError::Config(format!("write error: {e}")))?;

        commits_written += 1;

        // Decode envelope to find txn blob CID.
        let hdr = CommitV2Header::read_from(&raw_bytes).map_err(|e| {
            CliError::Config(format!("invalid commit header for {commit_cid}: {e}"))
        })?;
        let envelope_start = HEADER_LEN;
        let envelope_end = envelope_start + hdr.envelope_len as usize;
        if envelope_end <= raw_bytes.len() {
            if let Ok(env) = decode_envelope(&raw_bytes[envelope_start..envelope_end]) {
                if let Some(ref txn_cid) = env.txn {
                    if txn_cids_sent.insert(txn_cid.clone()) {
                        let txn_bytes = content_store.get(txn_cid).await.map_err(|e| {
                            CliError::Config(format!("failed to read txn blob {txn_cid}: {e}"))
                        })?;
                        buf.clear();
                        encode_data_frame(txn_cid, &txn_bytes, &mut buf);
                        writer
                            .write_all(&buf)
                            .map_err(|e| CliError::Config(format!("write error: {e}")))?;

                        txn_blobs_written += 1;
                    }
                }
            }
        }

        if commits_written.is_multiple_of(100) {
            eprint!("  {} commits...\r", commits_written);
        }
    }

    // Stream index artifacts.
    let mut index_written = 0usize;
    if let Some(ref artifacts) = index_artifacts {
        let index_root_id = ns_record.index_head_id.as_ref().unwrap();

        // Manifest frame announcing index phase.
        buf.clear();
        let manifest = serde_json::json!({
            "phase": "indexes",
            "root_id": index_root_id.to_string(),
            "artifact_count": artifacts.len(),
        });
        encode_manifest_frame(&manifest, &mut buf);
        writer
            .write_all(&buf)
            .map_err(|e| CliError::Config(format!("write error: {e}")))?;

        for artifact_cid in artifacts {
            let artifact_bytes = content_store.get(artifact_cid).await.map_err(|e| {
                CliError::Config(format!("failed to read index artifact {artifact_cid}: {e}"))
            })?;
            buf.clear();
            encode_data_frame(artifact_cid, &artifact_bytes, &mut buf);
            writer
                .write_all(&buf)
                .map_err(|e| CliError::Config(format!("write error: {e}")))?;

            index_written += 1;
        }
    }

    // Nameservice manifest — carries the metadata needed to reconstruct
    // the ns record on import.
    buf.clear();
    let ns_manifest = serde_json::json!({
        "phase": "nameservice",
        "ledger_id": ns_record.ledger_id,
        "name": ns_record.name,
        "branch": ns_record.branch,
        "commit_head_id": commit_head_id.to_string(),
        "commit_t": ns_record.commit_t,
        "index_head_id": ns_record.index_head_id.as_ref().map(ToString::to_string),
        "index_t": ns_record.index_t,
    });
    encode_manifest_frame(&ns_manifest, &mut buf);
    writer
        .write_all(&buf)
        .map_err(|e| CliError::Config(format!("write error: {e}")))?;

    // End frame.
    buf.clear();
    encode_end_frame(&mut buf);
    writer
        .write_all(&buf)
        .map_err(|e| CliError::Config(format!("write error: {e}")))?;

    writer
        .flush()
        .map_err(|e| CliError::Config(format!("flush error: {e}")))?;

    // File size.
    let file_size = std::fs::metadata(&out_path).map(|m| m.len()).unwrap_or(0);

    let objects = commits_written + txn_blobs_written + index_written;
    println!(
        "{} Exported '{}' — {} commits, {} txn blobs, {} index artifacts ({} objects, {})",
        "✓".green(),
        alias,
        commits_written,
        txn_blobs_written,
        index_written,
        objects,
        format_human_bytes(file_size),
    );

    Ok(())
}

/// Format bytes as a human-readable size.
fn format_human_bytes(bytes: u64) -> String {
    const GIB: f64 = 1_073_741_824.0;
    const MIB: f64 = 1_048_576.0;
    const KIB: f64 = 1_024.0;

    let b = bytes as f64;
    if b >= GIB {
        format!("{:.1} GiB", b / GIB)
    } else if b >= MIB {
        format!("{:.0} MiB", b / MIB)
    } else if b >= KIB {
        format!("{:.0} KiB", b / KIB)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Format SPARQL JSON bindings as N-Triples.
fn format_ntriples(json: &serde_json::Value) -> String {
    let bindings = match json.pointer("/results/bindings").and_then(|v| v.as_array()) {
        Some(b) => b,
        None => return String::new(),
    };

    let mut lines = Vec::new();
    for row in bindings {
        let s = extract_ntriples_term(row.get("s"));
        let p = extract_ntriples_term(row.get("p"));
        let o = extract_ntriples_term(row.get("o"));
        if !s.is_empty() && !p.is_empty() && !o.is_empty() {
            lines.push(format!("{s} {p} {o} ."));
        }
    }
    if !lines.is_empty() {
        lines.push(String::new()); // trailing newline
    }
    lines.join("\n")
}

/// Convert a SPARQL JSON binding value to N-Triples term syntax.
fn extract_ntriples_term(binding: Option<&serde_json::Value>) -> String {
    let b = match binding {
        Some(v) => v,
        None => return String::new(),
    };

    let value = b.get("value").and_then(|v| v.as_str()).unwrap_or("");
    let typ = b.get("type").and_then(|v| v.as_str()).unwrap_or("");

    match typ {
        "uri" => format!("<{value}>"),
        "literal" => {
            let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
            if let Some(lang) = b.get("xml:lang").and_then(|v| v.as_str()) {
                format!("\"{escaped}\"@{lang}")
            } else if let Some(dt) = b.get("datatype").and_then(|v| v.as_str()) {
                if dt == xsd::STRING {
                    format!("\"{escaped}\"")
                } else {
                    format!("\"{escaped}\"^^<{dt}>")
                }
            } else {
                format!("\"{escaped}\"")
            }
        }
        "bnode" => format!("_:{value}"),
        _ => format!("<{value}>"),
    }
}
