use crate::error::{MemoryError, Result};
use crate::schema::{memory_schema_jsonld, memory_to_jsonld};
use crate::types::{Memory, MemoryFilter, MemoryInput, MemoryKind, MemoryStatus, MemoryUpdate};
use chrono::Utc;
use fluree_db_api::{FileStorage, Fluree};
use fluree_db_nameservice::file::FileNameService;
use serde_json::{json, Value};
use tracing::debug;

/// Name of the internal memory ledger.
pub const MEMORY_LEDGER: &str = "__memory";

/// Normalized ledger ID (with branch suffix).
const MEMORY_LEDGER_ID: &str = "__memory:main";

/// The memory store: CRUD operations backed by a Fluree ledger.
pub struct MemoryStore {
    fluree: Fluree<FileStorage, FileNameService>,
}

impl MemoryStore {
    /// Create a new memory store wrapping a Fluree instance.
    pub fn new(fluree: Fluree<FileStorage, FileNameService>) -> Self {
        Self { fluree }
    }

    /// Check if the memory ledger has been initialized.
    pub async fn is_initialized(&self) -> Result<bool> {
        Ok(self
            .fluree
            .ledger_exists(MEMORY_LEDGER_ID)
            .await
            .unwrap_or(false))
    }

    /// Initialize the memory ledger (create + transact schema).
    ///
    /// Idempotent — returns Ok if already initialized.
    pub async fn initialize(&self) -> Result<()> {
        if self.is_initialized().await? {
            debug!("Memory ledger already initialized");
            return Ok(());
        }

        debug!("Creating memory ledger");
        self.fluree.create_ledger(MEMORY_LEDGER).await?;

        debug!("Transacting memory schema");
        let schema = memory_schema_jsonld();
        self.fluree
            .graph(MEMORY_LEDGER_ID)
            .transact()
            .insert(&schema)
            .commit()
            .await?;

        debug!("Memory ledger initialized");
        Ok(())
    }

    /// Ensure the memory store is initialized, returning an error if not.
    async fn require_initialized(&self) -> Result<()> {
        if !self.is_initialized().await? {
            return Err(MemoryError::NotInitialized);
        }
        Ok(())
    }

    /// Add a new memory to the store.
    ///
    /// Returns the generated memory ID.
    pub async fn add(&self, input: MemoryInput) -> Result<String> {
        self.require_initialized().await?;

        let id = crate::id::generate_memory_id(input.kind);
        let created_at = Utc::now().to_rfc3339();

        let mem = Memory {
            id: id.clone(),
            kind: input.kind,
            content: input.content,
            tags: input.tags,
            scope: input.scope,
            sensitivity: input.sensitivity,
            severity: input.severity,
            artifact_refs: input.artifact_refs,
            branch: input.branch,
            supersedes: None,
            valid_from: input.valid_from,
            valid_to: input.valid_to,
            created_at,
            rationale: input.rationale,
            alternatives: input.alternatives,
            fact_kind: input.fact_kind,
            pref_scope: input.pref_scope,
            artifact_kind: input.artifact_kind,
        };

        let doc = memory_to_jsonld(&mem);

        self.fluree
            .graph(MEMORY_LEDGER_ID)
            .transact()
            .insert(&doc)
            .commit()
            .await?;

        debug!(id = %id, kind = %mem.kind, "Memory added");
        Ok(id)
    }

    /// Get a single memory by ID.
    pub async fn get(&self, id: &str) -> Result<Option<Memory>> {
        self.require_initialized().await?;

        let sparql = format!(
            r#"SELECT ?type ?content ?scope ?sensitivity ?severity ?tag ?artifactRef ?branch ?supersedes ?validFrom ?validTo ?createdAt ?rationale ?alternatives ?factKind ?prefScope ?artifactKind
WHERE {{
  <{id}> a ?type .
  <{id}> <https://ns.flur.ee/memory#content> ?content .
  <{id}> <https://ns.flur.ee/memory#createdAt> ?createdAt .
  OPTIONAL {{ <{id}> <https://ns.flur.ee/memory#scope> ?scope }}
  OPTIONAL {{ <{id}> <https://ns.flur.ee/memory#sensitivity> ?sensitivity }}
  OPTIONAL {{ <{id}> <https://ns.flur.ee/memory#severity> ?severity }}
  OPTIONAL {{ <{id}> <https://ns.flur.ee/memory#tag> ?tag }}
  OPTIONAL {{ <{id}> <https://ns.flur.ee/memory#artifactRef> ?artifactRef }}
  OPTIONAL {{ <{id}> <https://ns.flur.ee/memory#branch> ?branch }}
  OPTIONAL {{ <{id}> <https://ns.flur.ee/memory#supersedes> ?supersedes }}
  OPTIONAL {{ <{id}> <https://ns.flur.ee/memory#validFrom> ?validFrom }}
  OPTIONAL {{ <{id}> <https://ns.flur.ee/memory#validTo> ?validTo }}
  OPTIONAL {{ <{id}> <https://ns.flur.ee/memory#rationale> ?rationale }}
  OPTIONAL {{ <{id}> <https://ns.flur.ee/memory#alternatives> ?alternatives }}
  OPTIONAL {{ <{id}> <https://ns.flur.ee/memory#factKind> ?factKind }}
  OPTIONAL {{ <{id}> <https://ns.flur.ee/memory#prefScope> ?prefScope }}
  OPTIONAL {{ <{id}> <https://ns.flur.ee/memory#artifactKind> ?artifactKind }}
}}"#
        );

        let result = self
            .fluree
            .graph(MEMORY_LEDGER_ID)
            .query()
            .sparql(&sparql)
            .execute_formatted()
            .await?;

        parse_memory_from_sparql_results(id, &result)
    }

    /// Update (supersede) an existing memory.
    ///
    /// Creates a new memory that supersedes the given one.
    /// Returns the new memory's ID.
    pub async fn update(&self, id: &str, update: MemoryUpdate) -> Result<String> {
        self.require_initialized().await?;

        // Load the existing memory
        let existing = self
            .get(id)
            .await?
            .ok_or_else(|| MemoryError::NotFound(id.to_string()))?;

        // Build the new memory, applying updates over existing values
        let new_id = crate::id::generate_memory_id(existing.kind);
        let created_at = Utc::now().to_rfc3339();

        let merged = Memory {
            id: new_id.clone(),
            kind: existing.kind,
            content: update.content.unwrap_or(existing.content),
            tags: update.tags.unwrap_or(existing.tags),
            scope: existing.scope,
            sensitivity: existing.sensitivity,
            severity: update.severity.or(existing.severity),
            artifact_refs: update.artifact_refs.unwrap_or(existing.artifact_refs),
            branch: existing.branch,
            supersedes: Some(id.to_string()),
            valid_from: update.valid_from.or(existing.valid_from),
            valid_to: update.valid_to.or(existing.valid_to),
            created_at,
            rationale: update.rationale.or(existing.rationale),
            alternatives: update.alternatives.or(existing.alternatives),
            fact_kind: existing.fact_kind,
            pref_scope: existing.pref_scope,
            artifact_kind: existing.artifact_kind,
        };

        let doc = memory_to_jsonld(&merged);

        self.fluree
            .graph(MEMORY_LEDGER_ID)
            .transact()
            .insert(&doc)
            .commit()
            .await?;

        debug!(new_id = %new_id, supersedes = %id, "Memory updated (superseded)");
        Ok(new_id)
    }

    /// Delete a memory by retracting all its triples.
    pub async fn forget(&self, id: &str) -> Result<()> {
        self.require_initialized().await?;

        // Verify the memory exists
        if self.get(id).await?.is_none() {
            return Err(MemoryError::NotFound(id.to_string()));
        }

        // Use the update (WHERE/DELETE) pattern to retract all triples for this subject.
        let delete_doc = json!({
            "@context": {
                "mem": "https://ns.flur.ee/memory#"
            },
            "where": { "@id": id, "?p": "?o" },
            "delete": { "@id": id, "?p": "?o" }
        });

        self.fluree
            .graph(MEMORY_LEDGER_ID)
            .transact()
            .update(&delete_doc)
            .commit()
            .await?;

        debug!(id = %id, "Memory forgotten");
        Ok(())
    }

    /// Get all current (non-superseded) memories matching the filter.
    pub async fn current_memories(&self, filter: &MemoryFilter) -> Result<Vec<Memory>> {
        self.require_initialized().await?;

        // Build SPARQL query with filters
        let mut where_clauses = vec![
            "?id a ?type".to_string(),
            "?id <https://ns.flur.ee/memory#content> ?content".to_string(),
            "?id <https://ns.flur.ee/memory#createdAt> ?createdAt".to_string(),
        ];

        let filter_clauses = [
            // Exclude superseded memories
            "FILTER NOT EXISTS { ?newer <https://ns.flur.ee/memory#supersedes> ?id }".to_string(),
        ];

        let optional_clauses = vec![
            "OPTIONAL { ?id <https://ns.flur.ee/memory#scope> ?scope }".to_string(),
            "OPTIONAL { ?id <https://ns.flur.ee/memory#sensitivity> ?sensitivity }".to_string(),
            "OPTIONAL { ?id <https://ns.flur.ee/memory#severity> ?severity }".to_string(),
            "OPTIONAL { ?id <https://ns.flur.ee/memory#tag> ?tag }".to_string(),
            "OPTIONAL { ?id <https://ns.flur.ee/memory#artifactRef> ?artifactRef }".to_string(),
            "OPTIONAL { ?id <https://ns.flur.ee/memory#branch> ?branch }".to_string(),
            "OPTIONAL { ?id <https://ns.flur.ee/memory#supersedes> ?supersedes }".to_string(),
            "OPTIONAL { ?id <https://ns.flur.ee/memory#validFrom> ?validFrom }".to_string(),
            "OPTIONAL { ?id <https://ns.flur.ee/memory#validTo> ?validTo }".to_string(),
            "OPTIONAL { ?id <https://ns.flur.ee/memory#rationale> ?rationale }".to_string(),
            "OPTIONAL { ?id <https://ns.flur.ee/memory#alternatives> ?alternatives }".to_string(),
            "OPTIONAL { ?id <https://ns.flur.ee/memory#factKind> ?factKind }".to_string(),
            "OPTIONAL { ?id <https://ns.flur.ee/memory#prefScope> ?prefScope }".to_string(),
            "OPTIONAL { ?id <https://ns.flur.ee/memory#artifactKind> ?artifactKind }".to_string(),
        ];

        // Apply kind filter
        if let Some(kind) = &filter.kind {
            where_clauses.push(format!(
                "?id a <{}>",
                kind.class_iri()
                    .replace("mem:", "https://ns.flur.ee/memory#")
            ));
        }

        // Apply tag filter
        for tag in &filter.tags {
            where_clauses.push(format!("?id <https://ns.flur.ee/memory#tag> \"{}\"", tag));
        }

        // Apply branch filter
        if let Some(branch) = &filter.branch {
            where_clauses.push(format!(
                "?id <https://ns.flur.ee/memory#branch> \"{}\"",
                branch
            ));
        }

        // Apply scope filter (IRI-based)
        if let Some(scope) = &filter.scope {
            where_clauses.push(format!(
                "?id <https://ns.flur.ee/memory#scope> <{}>",
                scope.iri()
            ));
        }

        let sparql = format!(
            "SELECT ?id ?type ?content ?scope ?sensitivity ?severity ?tag ?artifactRef ?branch ?supersedes ?validFrom ?validTo ?createdAt ?rationale ?alternatives ?factKind ?prefScope ?artifactKind\nWHERE {{\n  {}\n  {}\n  {}\n}}\nORDER BY DESC(?createdAt)",
            where_clauses.join(" .\n  "),
            optional_clauses.join("\n  "),
            filter_clauses.join("\n  "),
        );

        let result = self
            .fluree
            .graph(MEMORY_LEDGER_ID)
            .query()
            .sparql(&sparql)
            .execute_formatted()
            .await?;

        parse_memories_from_sparql_results(&result)
    }

    /// Get memory store status summary.
    pub async fn status(&self) -> Result<MemoryStatus> {
        if !self.is_initialized().await? {
            return Ok(MemoryStatus {
                initialized: false,
                total_memories: 0,
                by_kind: Vec::new(),
                total_tags: 0,
            });
        }

        let all = self.current_memories(&MemoryFilter::default()).await?;

        let mut by_kind: std::collections::HashMap<MemoryKind, usize> =
            std::collections::HashMap::new();
        let mut total_tags = 0;

        for m in &all {
            *by_kind.entry(m.kind).or_default() += 1;
            total_tags += m.tags.len();
        }

        let by_kind: Vec<(MemoryKind, usize)> = by_kind.into_iter().collect();

        Ok(MemoryStatus {
            initialized: true,
            total_memories: all.len(),
            by_kind,
            total_tags,
        })
    }

    /// Get the supersession chain for a memory (newest first).
    pub async fn supersession_chain(&self, id: &str) -> Result<Vec<Memory>> {
        self.require_initialized().await?;

        let mut chain = Vec::new();

        // Walk backward through supersession chain
        let mut current_id = id.to_string();
        while let Some(mem) = self.get(&current_id).await? {
            let supersedes = mem.supersedes.clone();
            chain.push(mem);
            match supersedes {
                Some(prev_id) => current_id = prev_id,
                None => break,
            }
        }

        // Also walk backward: find memories that supersede `id`
        let sparql = format!(
            r#"SELECT ?newer
WHERE {{
  ?newer <https://ns.flur.ee/memory#supersedes> <{id}> .
}}"#
        );

        let result = self
            .fluree
            .graph(MEMORY_LEDGER_ID)
            .query()
            .sparql(&sparql)
            .execute_formatted()
            .await?;

        if let Some(bindings) = result.get("results").and_then(|r| r.get("bindings")) {
            if let Some(arr) = bindings.as_array() {
                for binding in arr {
                    if let Some(newer_id) = binding
                        .get("newer")
                        .and_then(|v| v.get("value"))
                        .and_then(|v| v.as_str())
                    {
                        if let Some(mem) = self.get(newer_id).await? {
                            chain.insert(0, mem);
                        }
                    }
                }
            }
        }

        Ok(chain)
    }

    /// Export all memories as JSON.
    pub async fn export(&self) -> Result<Value> {
        self.require_initialized().await?;
        let all = self.current_memories(&MemoryFilter::default()).await?;
        Ok(serde_json::to_value(&all)?)
    }

    /// Execute a raw SPARQL query against the memory ledger.
    ///
    /// Returns the raw JSON result from Fluree.
    pub async fn query_sparql(&self, sparql: &str) -> Result<Value> {
        self.require_initialized().await?;

        let result = self
            .fluree
            .graph(MEMORY_LEDGER_ID)
            .query()
            .sparql(sparql)
            .execute_formatted()
            .await?;

        Ok(result)
    }

    /// Import memories from a JSON array.
    ///
    /// Returns the number of memories imported.
    pub async fn import(&self, data: Value) -> Result<usize> {
        self.require_initialized().await?;

        let memories: Vec<Memory> = serde_json::from_value(data)?;
        let count = memories.len();

        for mem in &memories {
            let doc = memory_to_jsonld(mem);

            self.fluree
                .graph(MEMORY_LEDGER_ID)
                .transact()
                .insert(&doc)
                .commit()
                .await?;
        }

        Ok(count)
    }
}

// ---------------------------------------------------------------------------
// SPARQL result parsing helpers
// ---------------------------------------------------------------------------

/// Parse SPARQL JSON results format into a list of Memory structs.
///
/// The SPARQL results format is:
/// ```json
/// { "results": { "bindings": [ { "var": { "value": "..." }, ... }, ... ] } }
/// ```
///
/// Or the Fluree flat table format:
/// ```json
/// [ [ val1, val2, ... ], ... ]
/// ```
fn parse_memories_from_sparql_results(result: &Value) -> Result<Vec<Memory>> {
    // Try SPARQL JSON Results format first
    if let Some(bindings) = result
        .get("results")
        .and_then(|r| r.get("bindings"))
        .and_then(|b| b.as_array())
    {
        return parse_from_sparql_bindings(bindings);
    }

    // Try Fluree flat array format: [[val, val, ...], ...]
    if let Some(rows) = result.as_array() {
        return parse_from_flat_rows(rows);
    }

    Ok(Vec::new())
}

/// Parse a single memory from SPARQL results.
fn parse_memory_from_sparql_results(id: &str, result: &Value) -> Result<Option<Memory>> {
    let memories = parse_memories_from_sparql_results(result)?;

    if memories.is_empty() {
        return Ok(None);
    }

    // Merge rows for the same ID (multiple rows from OPTIONAL multi-value patterns like tags)
    let merged = merge_memory_rows(id, &memories);
    Ok(merged)
}

/// Parse memories from SPARQL bindings format.
fn parse_from_sparql_bindings(bindings: &[Value]) -> Result<Vec<Memory>> {
    use std::collections::HashMap;

    // Group bindings by subject ID
    let mut grouped: HashMap<String, Vec<&Value>> = HashMap::new();
    for binding in bindings {
        if let Some(id) = extract_binding_value(binding, "id") {
            grouped.entry(id).or_default().push(binding);
        }
    }

    let mut memories = Vec::new();
    for (id, rows) in grouped {
        if let Some(mem) = merge_bindings_to_memory(&id, &rows) {
            memories.push(mem);
        }
    }

    Ok(memories)
}

/// Parse memories from Fluree flat row format.
///
/// Expected column order matches the SELECT clause:
/// `?id ?type ?content ?scope ?sensitivity ?severity ?tag ?artifactRef ?branch ?supersedes ?validFrom ?validTo ?createdAt`
fn parse_from_flat_rows(rows: &[Value]) -> Result<Vec<Memory>> {
    use std::collections::HashMap;

    let mut grouped: HashMap<String, Vec<&Value>> = HashMap::new();
    for row in rows {
        if let Some(arr) = row.as_array() {
            if let Some(id) = arr.first().and_then(|v| v.as_str()) {
                grouped.entry(id.to_string()).or_default().push(row);
            }
        }
    }

    let mut memories = Vec::new();
    for (id, rows) in grouped {
        if let Some(mem) = merge_flat_rows_to_memory(&id, &rows) {
            memories.push(mem);
        }
    }

    Ok(memories)
}

fn extract_binding_value(binding: &Value, var: &str) -> Option<String> {
    binding
        .get(var)
        .and_then(|v| v.get("value").or(Some(v)))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn merge_bindings_to_memory(id: &str, bindings: &[&Value]) -> Option<Memory> {
    let first = bindings.first()?;

    let type_str = extract_binding_value(first, "type")?;
    let content = extract_binding_value(first, "content")?;
    let created_at = extract_binding_value(first, "createdAt")?;

    let kind = iri_to_kind(&type_str)?;

    // Collect multi-value fields across rows
    let mut tags: Vec<String> = Vec::new();
    let mut artifact_refs: Vec<String> = Vec::new();
    for b in bindings {
        if let Some(tag) = extract_binding_value(b, "tag") {
            if !tags.contains(&tag) {
                tags.push(tag);
            }
        }
        if let Some(aref) = extract_binding_value(b, "artifactRef") {
            if !artifact_refs.contains(&aref) {
                artifact_refs.push(aref);
            }
        }
    }

    let scope = extract_binding_value(first, "scope")
        .and_then(|s| crate::types::Scope::parse_str(&s))
        .unwrap_or_default();

    let sensitivity = extract_binding_value(first, "sensitivity")
        .and_then(|s| crate::types::Sensitivity::parse_str(&s))
        .unwrap_or_default();

    let severity = extract_binding_value(first, "severity")
        .and_then(|s| crate::types::Severity::parse_str(&s));

    Some(Memory {
        id: id.to_string(),
        kind,
        content,
        tags,
        scope,
        sensitivity,
        severity,
        artifact_refs,
        branch: extract_binding_value(first, "branch"),
        supersedes: extract_binding_value(first, "supersedes"),
        valid_from: extract_binding_value(first, "validFrom"),
        valid_to: extract_binding_value(first, "validTo"),
        created_at,
        rationale: extract_binding_value(first, "rationale"),
        alternatives: extract_binding_value(first, "alternatives"),
        fact_kind: extract_binding_value(first, "factKind"),
        pref_scope: extract_binding_value(first, "prefScope"),
        artifact_kind: extract_binding_value(first, "artifactKind"),
    })
}

fn merge_flat_rows_to_memory(id: &str, rows: &[&Value]) -> Option<Memory> {
    let first = rows.first()?.as_array()?;
    if first.len() < 13 {
        return None;
    }

    // Column indices match SELECT order:
    // 0=id, 1=type, 2=content, 3=scope, 4=sensitivity, 5=severity,
    // 6=tag, 7=artifactRef, 8=branch, 9=supersedes, 10=validFrom, 11=validTo, 12=createdAt,
    // 13=rationale, 14=alternatives, 15=factKind, 16=prefScope, 17=artifactKind
    let type_str = first.get(1)?.as_str()?;
    let content = first.get(2)?.as_str()?.to_string();
    let created_at = first.get(12)?.as_str()?.to_string();

    let kind = iri_to_kind(type_str)?;

    let mut tags: Vec<String> = Vec::new();
    let mut artifact_refs: Vec<String> = Vec::new();
    for row in rows {
        if let Some(arr) = row.as_array() {
            if let Some(tag) = arr.get(6).and_then(|v| v.as_str()) {
                if !tags.contains(&tag.to_string()) {
                    tags.push(tag.to_string());
                }
            }
            if let Some(aref) = arr.get(7).and_then(|v| v.as_str()) {
                if !artifact_refs.contains(&aref.to_string()) {
                    artifact_refs.push(aref.to_string());
                }
            }
        }
    }

    let scope = first
        .get(3)
        .and_then(|v| v.as_str())
        .and_then(crate::types::Scope::parse_str)
        .unwrap_or_default();

    let sensitivity = first
        .get(4)
        .and_then(|v| v.as_str())
        .and_then(crate::types::Sensitivity::parse_str)
        .unwrap_or_default();

    let severity = first
        .get(5)
        .and_then(|v| v.as_str())
        .and_then(crate::types::Severity::parse_str);

    Some(Memory {
        id: id.to_string(),
        kind,
        content,
        tags,
        scope,
        sensitivity,
        severity,
        artifact_refs,
        branch: first.get(8).and_then(|v| v.as_str()).map(String::from),
        supersedes: first.get(9).and_then(|v| v.as_str()).map(String::from),
        valid_from: first.get(10).and_then(|v| v.as_str()).map(String::from),
        valid_to: first.get(11).and_then(|v| v.as_str()).map(String::from),
        created_at,
        rationale: first.get(13).and_then(|v| v.as_str()).map(String::from),
        alternatives: first.get(14).and_then(|v| v.as_str()).map(String::from),
        fact_kind: first.get(15).and_then(|v| v.as_str()).map(String::from),
        pref_scope: first.get(16).and_then(|v| v.as_str()).map(String::from),
        artifact_kind: first.get(17).and_then(|v| v.as_str()).map(String::from),
    })
}

/// Merge multiple result rows for the same ID into one Memory.
fn merge_memory_rows(id: &str, memories: &[Memory]) -> Option<Memory> {
    let first = memories.first()?;
    if memories.len() == 1 {
        return Some(first.clone());
    }

    let mut tags: Vec<String> = Vec::new();
    let mut artifact_refs: Vec<String> = Vec::new();
    for m in memories {
        for tag in &m.tags {
            if !tags.contains(tag) {
                tags.push(tag.clone());
            }
        }
        for aref in &m.artifact_refs {
            if !artifact_refs.contains(aref) {
                artifact_refs.push(aref.clone());
            }
        }
    }

    Some(Memory {
        id: id.to_string(),
        kind: first.kind,
        content: first.content.clone(),
        tags,
        scope: first.scope.clone(),
        sensitivity: first.sensitivity.clone(),
        severity: first.severity.clone(),
        artifact_refs,
        branch: first.branch.clone(),
        supersedes: first.supersedes.clone(),
        valid_from: first.valid_from.clone(),
        valid_to: first.valid_to.clone(),
        created_at: first.created_at.clone(),
        rationale: first.rationale.clone(),
        alternatives: first.alternatives.clone(),
        fact_kind: first.fact_kind.clone(),
        pref_scope: first.pref_scope.clone(),
        artifact_kind: first.artifact_kind.clone(),
    })
}

/// Convert a full IRI or prefixed name to a MemoryKind.
fn iri_to_kind(iri: &str) -> Option<MemoryKind> {
    let local = if let Some(stripped) = iri.strip_prefix("https://ns.flur.ee/memory#") {
        stripped
    } else if let Some(stripped) = iri.strip_prefix("mem:") {
        stripped
    } else {
        iri
    };

    match local {
        "Fact" => Some(MemoryKind::Fact),
        "Decision" => Some(MemoryKind::Decision),
        "Constraint" => Some(MemoryKind::Constraint),
        "Preference" => Some(MemoryKind::Preference),
        "Artifact" => Some(MemoryKind::Artifact),
        _ => None,
    }
}
