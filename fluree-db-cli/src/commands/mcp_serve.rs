use crate::context;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;
use fluree_db_memory::{MemoryStore, MemoryToolService};
use rmcp::ServiceExt;

/// Run the MCP server with the given transport.
pub async fn run(transport: &str, dirs: &FlureeDir) -> CliResult<()> {
    match transport {
        "stdio" => run_stdio(dirs).await,
        other => Err(CliError::Usage(format!(
            "unsupported MCP transport '{}'; valid: stdio",
            other
        ))),
    }
}

/// Launch the MCP server on stdio (stdin/stdout).
///
/// This is the primary transport for IDE integration. The IDE spawns
/// `fluree mcp serve --transport stdio` and communicates via JSON-RPC
/// over stdin/stdout.
async fn run_stdio(dirs: &FlureeDir) -> CliResult<()> {
    let fluree = context::build_fluree(dirs)?;

    // Determine memory_dir (same logic as CLI)
    let memory_dir = if dirs.is_unified() {
        let dir = dirs.data_dir().join("memory");
        if dir.exists() || dirs.data_dir().join("storage").exists() {
            Some(dir)
        } else {
            None
        }
    } else {
        None
    };

    let store = MemoryStore::new(fluree, memory_dir);
    let service = MemoryToolService::new(store);

    let transport = rmcp::transport::io::stdio();

    let server = service
        .serve(transport)
        .await
        .map_err(|e| CliError::Config(format!("failed to start MCP server: {}", e)))?;

    // Block until the client disconnects
    server
        .waiting()
        .await
        .map_err(|e| CliError::Config(format!("MCP server error: {}", e)))?;

    Ok(())
}
