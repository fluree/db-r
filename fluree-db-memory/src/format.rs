use crate::types::{Memory, MemoryStatus, RecallResult, ScoredMemory};

/// Format a memory for human-readable text output.
pub fn format_text(memory: &Memory) -> String {
    let mut out = String::new();
    out.push_str(&format!("ID:      {}\n", memory.id));
    out.push_str(&format!("Kind:    {}\n", memory.kind));
    out.push_str(&format!("Content: {}\n", memory.content));
    if !memory.tags.is_empty() {
        out.push_str(&format!("Tags:    {}\n", memory.tags.join(", ")));
    }
    if !memory.artifact_refs.is_empty() {
        out.push_str(&format!("Refs:    {}\n", memory.artifact_refs.join(", ")));
    }
    if let Some(branch) = &memory.branch {
        out.push_str(&format!("Branch:  {}\n", branch));
    }
    if let Some(rationale) = &memory.rationale {
        out.push_str(&format!("Rationale: {}\n", rationale));
    }
    if let Some(alternatives) = &memory.alternatives {
        out.push_str(&format!("Alternatives: {}\n", alternatives));
    }
    if let Some(fact_kind) = &memory.fact_kind {
        out.push_str(&format!("Fact kind: {}\n", fact_kind));
    }
    if let Some(pref_scope) = &memory.pref_scope {
        out.push_str(&format!("Pref scope: {}\n", pref_scope));
    }
    if let Some(artifact_kind) = &memory.artifact_kind {
        out.push_str(&format!("Artifact kind: {}\n", artifact_kind));
    }
    if let Some(supersedes) = &memory.supersedes {
        out.push_str(&format!("Supersedes: {}\n", supersedes));
    }
    out.push_str(&format!("Created: {}\n", memory.created_at));
    out
}

/// Format a recall result for human-readable text output.
pub fn format_recall_text(result: &RecallResult) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "Recall: \"{}\" ({} matches)\n\n",
        result.query,
        result.memories.len()
    ));

    for (i, scored) in result.memories.iter().enumerate() {
        out.push_str(&format!(
            "{}. [score: {:.1}] {}\n   {}\n",
            i + 1,
            scored.score,
            scored.memory.id,
            scored.memory.content,
        ));
        if !scored.memory.tags.is_empty() {
            out.push_str(&format!("   Tags: {}\n", scored.memory.tags.join(", ")));
        }
        out.push('\n');
    }

    out
}

/// Format a memory as JSON.
pub fn format_json(memory: &Memory) -> serde_json::Value {
    serde_json::to_value(memory).unwrap_or(serde_json::Value::Null)
}

/// Format a recall result as JSON.
pub fn format_recall_json(result: &RecallResult) -> serde_json::Value {
    serde_json::to_value(result).unwrap_or(serde_json::Value::Null)
}

/// Format memories as an XML context block for LLM injection.
///
/// This is the format that agents receive when they call the `memory_recall` MCP tool.
pub fn format_context(memories: &[ScoredMemory]) -> String {
    let mut out = String::new();
    out.push_str("<memory-context>\n");

    for scored in memories {
        let mem = &scored.memory;
        out.push_str(&format!(
            "  <memory id=\"{}\" kind=\"{}\" score=\"{:.1}\">\n",
            mem.id, mem.kind, scored.score
        ));
        out.push_str(&format!(
            "    <content>{}</content>\n",
            xml_escape(&mem.content)
        ));
        if !mem.tags.is_empty() {
            out.push_str(&format!("    <tags>{}</tags>\n", mem.tags.join(", ")));
        }
        if !mem.artifact_refs.is_empty() {
            out.push_str(&format!(
                "    <refs>{}</refs>\n",
                mem.artifact_refs.join(", ")
            ));
        }
        if let Some(severity) = &mem.severity {
            out.push_str(&format!("    <severity>{:?}</severity>\n", severity));
        }
        if let Some(rationale) = &mem.rationale {
            out.push_str(&format!(
                "    <rationale>{}</rationale>\n",
                xml_escape(rationale)
            ));
        }
        if let Some(alternatives) = &mem.alternatives {
            out.push_str(&format!(
                "    <alternatives>{}</alternatives>\n",
                xml_escape(alternatives)
            ));
        }
        out.push_str("  </memory>\n");
    }

    out.push_str("</memory-context>");
    out
}

/// Format the explain view for a supersession chain.
pub fn format_explain(chain: &[Memory]) -> String {
    if chain.is_empty() {
        return "No memories in chain.".to_string();
    }

    let mut out = String::new();
    out.push_str("Supersession chain (newest first):\n\n");

    for (i, mem) in chain.iter().enumerate() {
        let marker = if i == 0 { " (current)" } else { "" };
        out.push_str(&format!(
            "{}. {}{}\n   Kind: {}\n   Content: {}\n   Created: {}\n",
            i + 1,
            mem.id,
            marker,
            mem.kind,
            mem.content,
            mem.created_at,
        ));
        if let Some(sup) = &mem.supersedes {
            out.push_str(&format!("   Supersedes: {}\n", sup));
        }
        out.push('\n');
    }

    out
}

/// Format memory status for human-readable output.
pub fn format_status_text(status: &MemoryStatus) -> String {
    let mut out = String::new();

    if !status.initialized {
        return "Memory store not initialized. Run `fluree memory init` to set up.".to_string();
    }

    out.push_str("Memory Store Status\n");
    out.push_str(&format!("  Total memories: {}\n", status.total_memories));
    out.push_str(&format!("  Total tags:     {}\n", status.total_tags));

    if !status.by_kind.is_empty() {
        out.push_str("  By kind:\n");
        for (kind, count) in &status.by_kind {
            out.push_str(&format!("    {}: {}\n", kind, count));
        }
    }

    out
}

/// Minimal XML escaping for content text.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{MemoryKind, Scope, Sensitivity};

    fn sample_memory() -> Memory {
        Memory {
            id: "mem:fact-test123".to_string(),
            kind: MemoryKind::Fact,
            content: "Use nextest for running tests".to_string(),
            tags: vec!["testing".to_string(), "cargo".to_string()],
            scope: Scope::Repo,
            sensitivity: Sensitivity::Public,
            severity: None,
            artifact_refs: vec!["Cargo.toml".to_string()],
            branch: Some("main".to_string()),
            supersedes: None,
            valid_from: None,
            valid_to: None,
            created_at: "2026-02-21T12:00:00Z".to_string(),
            rationale: None,
            alternatives: None,
            fact_kind: None,
            pref_scope: None,
            artifact_kind: None,
        }
    }

    #[test]
    fn text_format_includes_all_fields() {
        let text = format_text(&sample_memory());
        assert!(text.contains("mem:fact-test123"));
        assert!(text.contains("nextest"));
        assert!(text.contains("testing, cargo"));
        assert!(text.contains("Cargo.toml"));
        assert!(text.contains("main"));
    }

    #[test]
    fn context_format_is_xml() {
        let scored = vec![ScoredMemory {
            memory: sample_memory(),
            score: 15.0,
        }];
        let ctx = format_context(&scored);
        assert!(ctx.starts_with("<memory-context>"));
        assert!(ctx.ends_with("</memory-context>"));
        assert!(ctx.contains("score=\"15.0\""));
    }

    #[test]
    fn xml_escape_works() {
        assert_eq!(xml_escape("a < b & c > d"), "a &lt; b &amp; c &gt; d");
    }
}
