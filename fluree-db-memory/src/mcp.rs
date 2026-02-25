//! MCP tool service for the developer memory layer.
//!
//! Provides `MemoryToolService` with tools for storing, recalling, updating,
//! and forgetting memories. Designed for IDE agent integration via stdio transport.

use crate::format::{format_context, format_json, format_status_text};
use crate::recall::RecallEngine;
use crate::secrets::SecretDetector;
use crate::store::MemoryStore;
use crate::types::{
    MemoryFilter, MemoryInput, MemoryKind, MemoryUpdate, Scope, Sensitivity, Severity,
};
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::*;
use rmcp::{tool, tool_handler, tool_router, RoleServer, ServerHandler};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info};

/// Request parameters for the `memory_add` tool.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct MemoryAddRequest {
    /// The kind of memory: fact, decision, constraint, preference, artifact
    #[schemars(
        description = "Memory kind: 'fact', 'decision', 'constraint', 'preference', or 'artifact'"
    )]
    pub kind: String,

    /// The content text to store
    #[schemars(description = "The content text of the memory")]
    pub content: String,

    /// Tags for categorization and recall
    #[schemars(description = "Tags for categorization (e.g., ['testing', 'rust'])")]
    #[serde(default)]
    pub tags: Vec<String>,

    /// File or artifact references
    #[schemars(description = "File paths or artifact references related to this memory")]
    #[serde(default)]
    pub refs: Vec<String>,

    /// Scope: repo (default) or user
    #[schemars(
        description = "Memory scope: 'repo' (project-wide, default) or 'user' (follows developer across repos)"
    )]
    #[serde(default)]
    pub scope: Option<String>,

    /// Sensitivity: public (default), internal, client, secret
    #[schemars(
        description = "Sensitivity level: 'public' (default), 'internal', 'client', or 'secret'"
    )]
    #[serde(default)]
    pub sensitivity: Option<String>,

    /// Severity for constraints: must, should, prefer
    #[schemars(description = "Severity level for constraints: 'must', 'should', or 'prefer'")]
    pub severity: Option<String>,

    /// Rationale for decisions
    #[schemars(description = "Why this decision was made (for kind='decision')")]
    pub rationale: Option<String>,

    /// Alternatives considered for decisions
    #[schemars(
        description = "What alternatives were considered (for kind='decision', comma-separated)"
    )]
    pub alternatives: Option<String>,

    /// Sub-categorization for facts
    #[schemars(
        description = "Fact sub-type: 'command', 'architecture', 'dependency', 'configuration', or 'api'"
    )]
    pub fact_kind: Option<String>,

    /// Convention scope for preferences
    #[schemars(description = "Whether this preference is a 'user', 'team', or 'repo' convention")]
    pub pref_scope: Option<String>,

    /// Artifact sub-type
    #[schemars(
        description = "Artifact sub-type: 'file', 'symbol', 'crate', 'module', 'config', or 'endpoint'"
    )]
    pub artifact_kind: Option<String>,
}

/// Request parameters for the `memory_recall` tool.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct MemoryRecallRequest {
    /// The search query
    #[schemars(description = "Natural language search query to find relevant memories")]
    pub query: String,

    /// Maximum number of results
    #[schemars(description = "Maximum number of memories to return (default: 10)")]
    pub limit: Option<usize>,

    /// Filter by kind
    #[schemars(description = "Filter to a specific memory kind")]
    pub kind: Option<String>,

    /// Filter by tags
    #[schemars(description = "Filter to memories with these tags")]
    #[serde(default)]
    pub tags: Vec<String>,

    /// Filter by scope
    #[schemars(description = "Filter to a specific scope: 'repo' or 'user'")]
    pub scope: Option<String>,
}

/// Request parameters for the `memory_update` tool.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct MemoryUpdateRequest {
    /// ID of the memory to update (supersede)
    #[schemars(description = "The ID of the memory to update (e.g., 'mem:fact-01JDXYZ...')")]
    pub id: String,

    /// New content text (if changing)
    #[schemars(description = "New content text (replaces existing if provided)")]
    pub content: Option<String>,

    /// New tags (replaces all existing tags if provided)
    #[schemars(description = "New tags (replaces all existing tags if provided)")]
    pub tags: Option<Vec<String>>,

    /// New artifact refs (replaces all existing if provided)
    #[schemars(description = "New artifact references (replaces all existing if provided)")]
    pub refs: Option<Vec<String>>,

    /// New rationale (for decisions)
    #[schemars(description = "Updated rationale for a decision")]
    pub rationale: Option<String>,

    /// New alternatives (for decisions)
    #[schemars(description = "Updated alternatives considered for a decision")]
    pub alternatives: Option<String>,
}

/// Request parameters for the `memory_forget` tool.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct MemoryForgetRequest {
    /// ID of the memory to delete
    #[schemars(description = "The ID of the memory to delete (e.g., 'mem:fact-01JDXYZ...')")]
    pub id: String,
}

/// Empty request parameters for `memory_status` (no inputs needed).
///
/// Exists to ensure rmcp generates a valid `{"type": "object"}` JSON Schema
/// for the tool's `inputSchema`. An empty schema `{}` causes some MCP clients
/// (including Claude Code) to fail tool registration.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct MemoryStatusRequest {}

/// Request parameters for the `kg_query` tool.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct KgQueryRequest {
    /// SPARQL query to execute against the memory graph
    #[schemars(
        description = "A SPARQL SELECT query to execute against the memory knowledge graph. The memory namespace is 'https://ns.flur.ee/memory#' (prefix 'mem:')."
    )]
    pub query: String,
}

/// MCP tool service for Fluree developer memory.
///
/// Provides tools for:
/// - `memory_add`: Store a new memory (fact, decision, constraint, preference, artifact)
/// - `memory_recall`: Search and retrieve relevant memories
/// - `memory_update`: Update (supersede) an existing memory
/// - `memory_forget`: Delete a memory
/// - `memory_status`: Show memory store status
/// - `kg_query`: Execute raw SPARQL queries against the memory graph
#[derive(Clone)]
pub struct MemoryToolService {
    store: std::sync::Arc<MemoryStore>,
    tool_router: ToolRouter<MemoryToolService>,
}

#[tool_router]
impl MemoryToolService {
    /// Create a new MemoryToolService wrapping a MemoryStore.
    pub fn new(store: MemoryStore) -> Self {
        Self {
            store: std::sync::Arc::new(store),
            tool_router: Self::tool_router(),
        }
    }

    /// Store a new memory (fact, decision, constraint, preference, or artifact).
    ///
    /// Memories persist across sessions and are used to maintain project context.
    /// Secrets (API keys, passwords, tokens) are automatically detected and redacted.
    #[tool(
        description = "Store a new memory that persists across sessions. Choose a kind: 'fact' (commands, architecture, config), 'decision' (choices made and why — include rationale), 'constraint' (rules that must be followed — include severity), 'preference' (conventions, style), 'artifact' (important files/resources). Always include descriptive tags for better searchability. Secrets (API keys, passwords) are auto-detected and redacted."
    )]
    async fn memory_add(
        &self,
        Parameters(req): Parameters<MemoryAddRequest>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        // Auto-initialize if needed
        if let Err(e) = self.ensure_initialized().await {
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Failed to initialize memory store: {}",
                e
            ))]));
        }

        let kind = MemoryKind::parse(&req.kind).ok_or_else(|| {
            rmcp::ErrorData::invalid_params(
                format!(
                    "Invalid memory kind '{}'. Valid: fact, decision, constraint, preference, artifact",
                    req.kind
                ),
                None,
            )
        })?;

        let severity = req
            .severity
            .as_deref()
            .map(|s| {
                Severity::parse_str(s).ok_or_else(|| {
                    rmcp::ErrorData::invalid_params(
                        format!("Invalid severity '{}'. Valid: must, should, prefer", s),
                        None,
                    )
                })
            })
            .transpose()?;

        // Check for and redact secrets
        let (content, redacted) = if SecretDetector::has_secrets(&req.content) {
            (SecretDetector::redact(&req.content), true)
        } else {
            (req.content, false)
        };

        let scope = req
            .scope
            .as_deref()
            .map(|s| {
                Scope::parse_str(s).ok_or_else(|| {
                    rmcp::ErrorData::invalid_params(
                        format!("Invalid scope '{}'. Valid: repo, user", s),
                        None,
                    )
                })
            })
            .transpose()?
            .unwrap_or_default();

        let sensitivity = req
            .sensitivity
            .as_deref()
            .map(|s| {
                Sensitivity::parse_str(s).ok_or_else(|| {
                    rmcp::ErrorData::invalid_params(
                        format!(
                            "Invalid sensitivity '{}'. Valid: public, internal, client, secret",
                            s
                        ),
                        None,
                    )
                })
            })
            .transpose()?
            .unwrap_or_default();

        let branch = crate::detect_git_branch();

        // Capture preview before content is moved into MemoryInput
        let preview: String = content
            .lines()
            .next()
            .unwrap_or("")
            .chars()
            .take(80)
            .collect();
        let ellipsis = if content.len() > preview.len() {
            "..."
        } else {
            ""
        };
        let preview_line = format!("{}{}", preview, ellipsis);

        let input = MemoryInput {
            kind,
            content,
            tags: req.tags,
            scope,
            sensitivity,
            severity,
            artifact_refs: req.refs,
            branch,
            valid_from: None,
            valid_to: None,
            rationale: req.rationale,
            alternatives: req.alternatives,
            fact_kind: req.fact_kind,
            pref_scope: req.pref_scope,
            artifact_kind: req.artifact_kind,
        };

        match self.store.add(input).await {
            Ok(id) => {
                info!(id = %id, kind = %req.kind, "Memory added");

                let mut text = format!("Stored memory: {} | {}: {}", id, req.kind, preview_line);

                if redacted {
                    text.push_str("\n\nWarning: Secrets were detected and automatically redacted.");
                }

                Ok(CallToolResult::success(vec![Content::text(text)]))
            }
            Err(e) => {
                error!(error = %e, "Failed to store memory");
                Ok(CallToolResult::error(vec![Content::text(format!(
                    "Failed to store memory: {}",
                    e
                ))]))
            }
        }
    }

    /// Search and retrieve relevant memories for a query.
    ///
    /// Returns memories ranked by relevance, formatted as XML context blocks
    /// suitable for LLM consumption.
    #[tool(
        description = "Search memories using BM25 keyword search. Returns ranked results as structured context. IMPORTANT: Use specific topic keywords (e.g., 'error handling', 'test commands', 'database schema'). Generic queries like 'all' or 'everything' will return no results. Use memory_status first to see what topics are stored."
    )]
    async fn memory_recall(
        &self,
        Parameters(req): Parameters<MemoryRecallRequest>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(e) = self.ensure_initialized().await {
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Failed to initialize memory store: {}",
                e
            ))]));
        }

        let kind_filter = req
            .kind
            .as_deref()
            .map(|s| {
                MemoryKind::parse(s).ok_or_else(|| {
                    rmcp::ErrorData::invalid_params(format!("Invalid memory kind '{}'", s), None)
                })
            })
            .transpose()?;

        let scope_filter = req
            .scope
            .as_deref()
            .map(|s| {
                Scope::parse_str(s).ok_or_else(|| {
                    rmcp::ErrorData::invalid_params(
                        format!("Invalid scope '{}'. Valid: repo, user", s),
                        None,
                    )
                })
            })
            .transpose()?;

        let filter = MemoryFilter {
            kind: kind_filter,
            tags: req.tags,
            branch: None,
            scope: scope_filter,
        };

        let limit = req.limit.unwrap_or(10);

        debug!(query = %req.query, limit = limit, "Memory recall request");

        // BM25 fulltext search for content relevance
        let bm25_hits = match self.store.recall_fulltext(&req.query, limit).await {
            Ok(hits) => {
                debug!(hits = hits.len(), "BM25 search complete");
                hits
            }
            Err(e) => {
                error!(error = %e, query = %req.query, "BM25 search failed");
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "Failed to search memories: {}",
                    e
                ))]));
            }
        };

        // Load full memory objects for metadata re-ranking
        match self.store.current_memories(&filter).await {
            Ok(all) => {
                debug!(
                    total_current = all.len(),
                    "Loaded current memories for re-ranking"
                );
                let branch = crate::detect_git_branch();
                let scored = if bm25_hits.is_empty() {
                    RecallEngine::recall_metadata_only(
                        &req.query,
                        &all,
                        branch.as_deref(),
                        Some(limit),
                    )
                } else {
                    RecallEngine::rerank(&req.query, &bm25_hits, &all, branch.as_deref())
                };

                if scored.is_empty() {
                    debug!(query = %req.query, "No relevant memories found");
                    return Ok(CallToolResult::success(vec![Content::text(
                        "No relevant memories found. Tip: use specific topic keywords, not generic terms.",
                    )]));
                }

                info!(query = %req.query, results = scored.len(), "Memory recall complete");
                // Return as XML context for LLM consumption
                let context = format_context(&scored);
                Ok(CallToolResult::success(vec![Content::text(context)]))
            }
            Err(e) => {
                error!(error = %e, "Failed to load current memories");
                Ok(CallToolResult::error(vec![Content::text(format!(
                    "Failed to recall memories: {}",
                    e
                ))]))
            }
        }
    }

    /// Update (supersede) an existing memory with new content or metadata.
    ///
    /// Creates a new memory that supersedes the old one. The old memory remains
    /// in the graph for audit/explain purposes but is no longer returned by recall.
    #[tool(
        description = "Update (supersede) an existing memory. Creates a new version that replaces the old one. The old version is kept for audit purposes. Provide the memory ID and any fields to change."
    )]
    async fn memory_update(
        &self,
        Parameters(req): Parameters<MemoryUpdateRequest>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(e) = self.ensure_initialized().await {
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Failed to initialize memory store: {}",
                e
            ))]));
        }

        // Check for secrets in new content
        let content = req.content.map(|c| {
            if SecretDetector::has_secrets(&c) {
                SecretDetector::redact(&c)
            } else {
                c
            }
        });

        let update = MemoryUpdate {
            content,
            tags: req.tags,
            severity: None,
            artifact_refs: req.refs,
            valid_from: None,
            valid_to: None,
            rationale: req.rationale,
            alternatives: req.alternatives,
        };

        match self.store.update(&req.id, update).await {
            Ok(new_id) => {
                let mut text = format!("Updated: {} → {}", req.id, new_id);
                if let Ok(Some(mem)) = self.store.get(&new_id).await {
                    text = serde_json::to_string_pretty(&format_json(&mem)).unwrap_or(text);
                }
                Ok(CallToolResult::success(vec![Content::text(text)]))
            }
            Err(e) => Ok(CallToolResult::error(vec![Content::text(format!(
                "Failed to update memory: {}",
                e
            ))])),
        }
    }

    /// Delete a memory permanently.
    #[tool(
        description = "Delete a memory permanently by its ID. Use this to remove incorrect or outdated memories that should not be recalled."
    )]
    async fn memory_forget(
        &self,
        Parameters(req): Parameters<MemoryForgetRequest>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(e) = self.ensure_initialized().await {
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Failed to initialize memory store: {}",
                e
            ))]));
        }

        match self.store.forget(&req.id).await {
            Ok(()) => Ok(CallToolResult::success(vec![Content::text(format!(
                "Forgotten: {}",
                req.id
            ))])),
            Err(e) => Ok(CallToolResult::error(vec![Content::text(format!(
                "Failed to forget memory: {}",
                e
            ))])),
        }
    }

    /// Show the memory store status summary.
    #[tool(
        description = "Show memory store status including total memories, counts by kind, and a preview of recent memories. Call this first to understand what knowledge is stored and what keywords to use with memory_recall."
    )]
    async fn memory_status(
        &self,
        Parameters(_req): Parameters<MemoryStatusRequest>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(e) = self.ensure_initialized().await {
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Failed to initialize memory store: {}",
                e
            ))]));
        }

        match self.store.status().await {
            Ok(status) => {
                debug!(total = status.total_memories, "Memory status requested");
                let text = format_status_text(&status);
                Ok(CallToolResult::success(vec![Content::text(text)]))
            }
            Err(e) => {
                error!(error = %e, "Failed to get memory status");
                Ok(CallToolResult::error(vec![Content::text(format!(
                    "Failed to get memory status: {}",
                    e
                ))]))
            }
        }
    }

    /// Execute a raw SPARQL query against the memory knowledge graph.
    #[tool(
        description = "Advanced: Execute a raw SPARQL SELECT query against the memory knowledge graph. Prefer memory_recall for searching. Namespace: 'https://ns.flur.ee/memory#' (prefix 'mem:'). Classes: mem:Fact, mem:Decision, mem:Constraint, mem:Preference, mem:Artifact. Key properties: mem:content, mem:tag, mem:scope, mem:severity, mem:createdAt. Example: SELECT ?id ?content WHERE { ?id a mem:Fact ; mem:content ?content } LIMIT 20"
    )]
    async fn kg_query(
        &self,
        Parameters(req): Parameters<KgQueryRequest>,
        _context: rmcp::service::RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        if let Err(e) = self.ensure_initialized().await {
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "Failed to initialize memory store: {}",
                e
            ))]));
        }

        debug!(query = %req.query, "SPARQL query requested");
        match self.store.query_sparql(&req.query).await {
            Ok(result) => {
                let text =
                    serde_json::to_string_pretty(&result).unwrap_or_else(|_| result.to_string());
                Ok(CallToolResult::success(vec![Content::text(text)]))
            }
            Err(e) => {
                error!(error = %e, query = %req.query, "SPARQL query failed");
                Ok(CallToolResult::error(vec![Content::text(format!(
                    "SPARQL query error: {}",
                    e
                ))]))
            }
        }
    }
}

impl MemoryToolService {
    /// Auto-initialize the memory store if not already initialized.
    async fn ensure_initialized(&self) -> std::result::Result<(), String> {
        if !self.store.is_initialized().await.unwrap_or(false) {
            info!("Memory store not initialized, initializing");
            self.store.initialize().await.map_err(|e| {
                error!(error = %e, "Memory initialization failed");
                format!("initialization failed: {}", e)
            })?;
            info!("Memory store initialized");
        }
        // Rebuild ledger from .ttl files if they've changed (e.g. git pull)
        self.store.ensure_synced().await.map_err(|e| {
            error!(error = %e, "File sync failed");
            format!("file sync failed: {}", e)
        })?;
        Ok(())
    }
}

#[tool_handler]
impl ServerHandler for MemoryToolService {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::LATEST,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "fluree-memory".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                title: Some("Fluree Developer Memory".to_string()),
                icons: None,
                website_url: Some("https://flur.ee".to_string()),
            },
            instructions: Some(
                "Fluree Developer Memory MCP server. Stores and retrieves project knowledge \
                 (facts, decisions, constraints, preferences, artifacts) as an RDF knowledge graph.\n\n\
                 Available tools:\n\
                 - memory_recall: Search for relevant memories. Use at the start of tasks.\n\
                 - memory_add: Store new project knowledge.\n\
                 - memory_update: Update existing memories (creates a new version).\n\
                 - memory_forget: Delete incorrect or outdated memories.\n\
                 - memory_status: Check what knowledge is stored.\n\
                 - kg_query: Run raw SPARQL queries against the memory graph.\n\n\
                 IMPORTANT USAGE GUIDELINES:\n\
                 1. memory_recall uses BM25 keyword search. Use specific topic words \
                    (e.g., 'error handling rust', 'test commands', 'database schema'). \
                    Vague queries like 'all' or 'everything' will return nothing.\n\
                 2. Use memory_status FIRST to see what's stored — it shows counts by kind \
                    and previews of recent memories.\n\
                 3. When storing memories, choose the right kind:\n\
                    - fact: things that are true (commands, architecture, config)\n\
                    - decision: choices made and why (with rationale)\n\
                    - constraint: rules that must be followed\n\
                    - preference: how things should be done (conventions)\n\
                    - artifact: important files or resources\n\
                 4. Always add descriptive tags for better recall.\n\
                 5. kg_query is for advanced use only — prefer memory_recall for searching."
                    .to_string(),
            ),
        }
    }
}
