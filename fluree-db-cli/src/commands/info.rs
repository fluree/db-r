use crate::context::{self, LedgerMode};
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_api::GraphSourcePublisher;
use fluree_db_nameservice::NameService;

pub async fn run(
    ledger: Option<&str>,
    dirs: &FlureeDir,
    remote_flag: Option<&str>,
    direct: bool,
) -> CliResult<()> {
    // Resolve ledger mode: --remote flag, local, tracked, or auto-route to local server.
    // If resolution fails (not found), try graph source lookup before giving up.
    let mode = if let Some(remote_name) = remote_flag {
        let alias = context::resolve_ledger(ledger, dirs)?;
        Ok(context::build_remote_mode(remote_name, &alias, dirs).await?)
    } else {
        let mode = context::resolve_ledger_mode(ledger, dirs).await;
        match mode {
            Ok(m) => Ok(if direct {
                m
            } else {
                context::try_server_route(m, dirs)
            }),
            Err(CliError::NotFound(_)) => {
                // Ledger not found — try graph source lookup
                let alias = context::resolve_ledger(ledger, dirs)?;
                let fluree = context::build_fluree(dirs)?;
                let gs_id = context::to_ledger_id(&alias);
                if let Some(gs) = fluree.nameservice().lookup_graph_source(&gs_id).await? {
                    print_graph_source_info(&gs);
                    return Ok(());
                }
                // Neither ledger nor graph source
                return Err(CliError::NotFound(format!(
                    "'{}' not found as a ledger or graph source",
                    alias
                )));
            }
            Err(e) => Err(e),
        }
    }?;

    match mode {
        LedgerMode::Tracked {
            client,
            remote_alias,
            local_alias,
            remote_name,
        } => {
            let info = client.ledger_info(&remote_alias).await?;

            context::persist_refreshed_tokens(&client, &remote_name, dirs).await;

            println!(
                "Ledger:         {} (tracked)",
                info.get("ledger")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&local_alias)
            );
            if let Some(t) = info.get("t").and_then(|v| v.as_i64()) {
                println!("t:              {}", t);
            }
            if let Some(commit) = info
                .get("commitId")
                .and_then(|v| v.as_str())
                .or_else(|| info.get("commit_head_id").and_then(|v| v.as_str()))
            {
                println!("Commit ID:      {}", commit);
            }
            if let Some(index) = info
                .get("indexId")
                .and_then(|v| v.as_str())
                .or_else(|| info.get("index_head_id").and_then(|v| v.as_str()))
            {
                println!("Index ID:       {}", index);
            }

            // Print full JSON if there are stats
            if info.get("stats").is_some() {
                println!();
                println!(
                    "{}",
                    serde_json::to_string_pretty(&info).unwrap_or_default()
                );
            }
        }
        LedgerMode::Local { fluree, alias } => {
            let ledger_id = context::to_ledger_id(&alias);

            // Try ledger first, then graph source
            if let Some(record) = fluree.nameservice().lookup(&ledger_id).await? {
                println!("Ledger:         {}", record.name);
                println!("Branch:         {}", record.branch);
                println!("Type:           Ledger");
                println!("Ledger ID:      {}", record.ledger_id);
                println!("Commit t:       {}", record.commit_t);
                println!(
                    "Commit ID:      {}",
                    record
                        .commit_head_id
                        .as_ref()
                        .map(|id| id.to_string())
                        .as_deref()
                        .unwrap_or("(none)")
                );
                println!("Index t:        {}", record.index_t);
                println!(
                    "Index ID:       {}",
                    record
                        .index_head_id
                        .as_ref()
                        .map(|id| id.to_string())
                        .as_deref()
                        .unwrap_or("(none)")
                );
            } else if let Some(gs) = fluree.nameservice().lookup_graph_source(&ledger_id).await? {
                print_graph_source_info(&gs);
            } else {
                return Err(CliError::NotFound(format!(
                    "'{}' not found as a ledger or graph source",
                    alias
                )));
            }
        }
    }

    Ok(())
}

fn print_graph_source_info(gs: &fluree_db_nameservice::GraphSourceRecord) {
    println!("Name:           {}", gs.name);
    println!("Branch:         {}", gs.branch);
    println!("Type:           {}", format_source_type(&gs.source_type));
    println!("ID:             {}", gs.graph_source_id);
    println!("Retracted:      {}", gs.retracted);
    println!("Index t:        {}", gs.index_t);
    println!(
        "Index ID:       {}",
        gs.index_id
            .as_ref()
            .map(|id| id.to_string())
            .as_deref()
            .unwrap_or("(none)")
    );

    if !gs.dependencies.is_empty() {
        println!("Dependencies:   {}", gs.dependencies.join(", "));
    }

    // Print config JSON (pretty)
    if !gs.config.is_empty() && gs.config != "{}" {
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&gs.config) {
            println!();
            println!("Configuration:");
            println!(
                "{}",
                serde_json::to_string_pretty(&parsed).unwrap_or_else(|_| gs.config.clone())
            );
        }
    }
}

fn format_source_type(st: &fluree_db_nameservice::GraphSourceType) -> String {
    match st {
        fluree_db_nameservice::GraphSourceType::Bm25 => "BM25".to_string(),
        fluree_db_nameservice::GraphSourceType::Vector => "Vector".to_string(),
        fluree_db_nameservice::GraphSourceType::Geo => "Geo".to_string(),
        fluree_db_nameservice::GraphSourceType::R2rml => "R2RML".to_string(),
        fluree_db_nameservice::GraphSourceType::Iceberg => "Iceberg".to_string(),
        fluree_db_nameservice::GraphSourceType::Unknown(s) => format!("Unknown({s})"),
    }
}
