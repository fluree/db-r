use serde::{Deserialize, Serialize};

/// The kind of memory being stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemoryKind {
    Fact,
    Decision,
    Constraint,
    Preference,
    Artifact,
}

impl MemoryKind {
    /// Short lowercase string for use in IDs and queries.
    pub fn as_str(&self) -> &'static str {
        match self {
            MemoryKind::Fact => "fact",
            MemoryKind::Decision => "decision",
            MemoryKind::Constraint => "constraint",
            MemoryKind::Preference => "preference",
            MemoryKind::Artifact => "artifact",
        }
    }

    /// RDF class IRI for this kind.
    pub fn class_iri(&self) -> &'static str {
        match self {
            MemoryKind::Fact => "mem:Fact",
            MemoryKind::Decision => "mem:Decision",
            MemoryKind::Constraint => "mem:Constraint",
            MemoryKind::Preference => "mem:Preference",
            MemoryKind::Artifact => "mem:Artifact",
        }
    }

    /// Parse from a string (case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "fact" => Some(MemoryKind::Fact),
            "decision" => Some(MemoryKind::Decision),
            "constraint" => Some(MemoryKind::Constraint),
            "preference" => Some(MemoryKind::Preference),
            "artifact" => Some(MemoryKind::Artifact),
            _ => None,
        }
    }
}

impl std::fmt::Display for MemoryKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Scope of a memory — which named graph it belongs to.
///
/// Stored as IRIs in the knowledge graph (e.g., `mem:repo`, `mem:user`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    /// Repo-wide memory, visible to all agents in this repo.
    #[default]
    Repo,
    /// User-private memory, follows the developer across repos.
    User,
}

impl Scope {
    /// The IRI used in the knowledge graph for this scope.
    pub fn iri(&self) -> &'static str {
        match self {
            Scope::Repo => "https://ns.flur.ee/memory#repo",
            Scope::User => "https://ns.flur.ee/memory#user",
        }
    }

    /// The short prefixed form (for display and SPARQL with prefix).
    pub fn prefixed(&self) -> &'static str {
        match self {
            Scope::Repo => "mem:repo",
            Scope::User => "mem:user",
        }
    }

    /// Parse from a string (accepts IRI, prefixed, or short form).
    pub fn parse_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "repo" | "mem:repo" | "https://ns.flur.ee/memory#repo" => Some(Scope::Repo),
            "user" | "mem:user" | "https://ns.flur.ee/memory#user" => Some(Scope::User),
            // Backwards compat with old string values
            "project" => Some(Scope::Repo),
            "global" => Some(Scope::User),
            _ => None,
        }
    }
}

impl std::fmt::Display for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Scope::Repo => f.write_str("repo"),
            Scope::User => f.write_str("user"),
        }
    }
}

/// Sensitivity level for content.
///
/// Stored as string literals in the knowledge graph.
/// - `public`: General knowledge, safe to share broadly.
/// - `internal`: Team/org internal, not for external sharing.
/// - `client`: Client-specific, restricted to project scope.
/// - `secret`: Contains credentials or secrets — auto-redacted on ingest.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Sensitivity {
    #[default]
    Public,
    Internal,
    Client,
    Secret,
}

impl Sensitivity {
    /// Short lowercase string for use in queries and storage.
    pub fn as_str(&self) -> &'static str {
        match self {
            Sensitivity::Public => "public",
            Sensitivity::Internal => "internal",
            Sensitivity::Client => "client",
            Sensitivity::Secret => "secret",
        }
    }

    /// Parse from a string (case-insensitive, with backwards compat).
    pub fn parse_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "public" => Some(Sensitivity::Public),
            "internal" => Some(Sensitivity::Internal),
            "client" => Some(Sensitivity::Client),
            "secret" => Some(Sensitivity::Secret),
            // Backwards compat with old values
            "normal" => Some(Sensitivity::Public),
            "sensitive" => Some(Sensitivity::Internal),
            _ => None,
        }
    }
}

impl std::fmt::Display for Sensitivity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Severity of a constraint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Must,
    Should,
    Prefer,
}

impl Severity {
    /// Parse from a string (case-insensitive).
    pub fn parse_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "must" => Some(Severity::Must),
            "should" => Some(Severity::Should),
            "prefer" => Some(Severity::Prefer),
            _ => None,
        }
    }
}

/// A stored memory record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Memory {
    /// Unique ID (e.g., `mem:fact-01JDXYZ...`).
    pub id: String,
    /// Kind of memory.
    pub kind: MemoryKind,
    /// The content text.
    pub content: String,
    /// Tags for categorization and recall.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Scope (project or global).
    #[serde(default)]
    pub scope: Scope,
    /// Sensitivity level.
    #[serde(default)]
    pub sensitivity: Sensitivity,
    /// Severity (for constraints).
    pub severity: Option<Severity>,
    /// File/artifact references.
    #[serde(default)]
    pub artifact_refs: Vec<String>,
    /// Git branch when created.
    pub branch: Option<String>,
    /// ID of the memory this supersedes.
    pub supersedes: Option<String>,
    /// Valid-from timestamp (bi-temporal).
    pub valid_from: Option<String>,
    /// Valid-to timestamp (bi-temporal).
    pub valid_to: Option<String>,
    /// Creation timestamp.
    pub created_at: String,

    // -- Type-specific predicates --

    /// Why this decision was made (for `Decision` kind).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
    /// What alternatives were considered (for `Decision` kind).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alternatives: Option<String>,
    /// Sub-categorization for facts: `command`, `architecture`, `dependency`, `configuration`, `api`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fact_kind: Option<String>,
    /// Whether this preference is a `user`, `team`, or `repo` convention.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pref_scope: Option<String>,
    /// Artifact sub-type: `file`, `symbol`, `crate`, `module`, `config`, `endpoint`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_kind: Option<String>,
}

/// Input for creating a new memory.
#[derive(Debug, Clone)]
pub struct MemoryInput {
    pub kind: MemoryKind,
    pub content: String,
    pub tags: Vec<String>,
    pub scope: Scope,
    pub sensitivity: Sensitivity,
    pub severity: Option<Severity>,
    pub artifact_refs: Vec<String>,
    pub branch: Option<String>,
    pub valid_from: Option<String>,
    pub valid_to: Option<String>,
    // Type-specific
    pub rationale: Option<String>,
    pub alternatives: Option<String>,
    pub fact_kind: Option<String>,
    pub pref_scope: Option<String>,
    pub artifact_kind: Option<String>,
}

/// Input for updating (superseding) a memory.
#[derive(Debug, Clone)]
pub struct MemoryUpdate {
    pub content: Option<String>,
    pub tags: Option<Vec<String>>,
    pub severity: Option<Severity>,
    pub artifact_refs: Option<Vec<String>>,
    pub valid_from: Option<String>,
    pub valid_to: Option<String>,
    // Type-specific
    pub rationale: Option<String>,
    pub alternatives: Option<String>,
}

/// Filter for querying memories.
#[derive(Debug, Clone, Default)]
pub struct MemoryFilter {
    pub kind: Option<MemoryKind>,
    pub tags: Vec<String>,
    pub scope: Option<Scope>,
    pub branch: Option<String>,
}

/// A memory scored by relevance for a recall query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredMemory {
    pub memory: Memory,
    pub score: f64,
}

/// Result from a recall operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecallResult {
    pub query: String,
    pub memories: Vec<ScoredMemory>,
    pub total_count: usize,
}

/// Summary status of the memory store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryStatus {
    pub initialized: bool,
    pub total_memories: usize,
    pub by_kind: Vec<(MemoryKind, usize)>,
    pub total_tags: usize,
    /// Preview of recent memories (content truncated) for discoverability.
    pub recent: Vec<MemoryPreview>,
}

/// A compact summary of a memory for status/listing output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryPreview {
    pub id: String,
    pub kind: MemoryKind,
    /// Content truncated to ~100 chars.
    pub summary: String,
    pub tags: Vec<String>,
}
