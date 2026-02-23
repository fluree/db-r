use crate::types::{Memory, MemoryKind, ScoredMemory};
use chrono::{DateTime, Utc};

/// Keyword-based recall engine for scoring memories against a query.
///
/// Phase 1: No embeddings, no LLM. Pure keyword + metadata matching.
/// Phase 2 will add vector similarity via Fluree Cloud embeddings.
pub struct RecallEngine;

impl RecallEngine {
    /// Score and rank memories against a query string.
    ///
    /// Returns memories sorted by descending score, filtered to those with score > 0.
    pub fn recall(
        query: &str,
        memories: &[Memory],
        current_branch: Option<&str>,
        limit: Option<usize>,
    ) -> Vec<ScoredMemory> {
        let query_lower = query.to_lowercase();
        let query_words: Vec<&str> = query_lower
            .split_whitespace()
            .filter(|w| w.len() > 2) // Skip very short words
            .collect();

        let now = Utc::now();

        let mut scored: Vec<ScoredMemory> = memories
            .iter()
            .map(|mem| {
                let score = score_memory(mem, &query_lower, &query_words, current_branch, &now);
                ScoredMemory {
                    memory: mem.clone(),
                    score,
                }
            })
            .filter(|sm| sm.score > 0.0)
            .collect();

        // Sort by descending score
        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        if let Some(limit) = limit {
            scored.truncate(limit);
        }

        scored
    }
}

/// Score a single memory against a query.
fn score_memory(
    mem: &Memory,
    query_lower: &str,
    query_words: &[&str],
    current_branch: Option<&str>,
    now: &DateTime<Utc>,
) -> f64 {
    let mut score = 0.0;

    // Tag exact match: +10 per matching tag
    for tag in &mem.tags {
        let tag_lower = tag.to_lowercase();
        if query_words.iter().any(|w| tag_lower.contains(w)) {
            score += 10.0;
        }
    }

    // Artifact ref path match: +8
    for aref in &mem.artifact_refs {
        let aref_lower = aref.to_lowercase();
        if query_words.iter().any(|w| aref_lower.contains(w)) {
            score += 8.0;
        }
    }

    // Type match: +6 if query mentions the memory kind
    let kind_names = match mem.kind {
        MemoryKind::Fact => &["fact", "facts"][..],
        MemoryKind::Decision => &["decision", "decisions", "decided"][..],
        MemoryKind::Constraint => &[
            "constraint",
            "constraints",
            "rule",
            "rules",
            "must",
            "never",
        ][..],
        MemoryKind::Preference => &["preference", "preferences", "prefer", "preferred"][..],
        MemoryKind::Artifact => &["artifact", "artifacts", "file", "files"][..],
    };
    if kind_names.iter().any(|kn| query_lower.contains(kn)) {
        score += 6.0;
    }

    // Content keyword overlap: +1 per matching word
    let content_lower = mem.content.to_lowercase();
    for word in query_words {
        if content_lower.contains(word) {
            score += 1.0;
        }
    }

    // Recency bonus
    if let Ok(created) = DateTime::parse_from_rfc3339(&mem.created_at) {
        let age = *now - created.to_utc();
        if age.num_days() < 7 {
            score += 2.0;
        } else if age.num_days() < 30 {
            score += 1.0;
        }
    }

    // Branch match: +3
    if let (Some(mem_branch), Some(cur_branch)) = (&mem.branch, current_branch) {
        if mem_branch == cur_branch {
            score += 3.0;
        }
    }

    score
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Memory, MemoryKind, Scope, Sensitivity};

    fn make_memory(content: &str, tags: &[&str], kind: MemoryKind) -> Memory {
        Memory {
            id: format!("mem:{}-test", kind.as_str()),
            kind,
            content: content.to_string(),
            tags: tags.iter().map(|t| t.to_string()).collect(),
            scope: Scope::Repo,
            sensitivity: Sensitivity::Public,
            severity: None,
            artifact_refs: Vec::new(),
            branch: None,
            supersedes: None,
            valid_from: None,
            valid_to: None,
            created_at: Utc::now().to_rfc3339(),
            rationale: None,
            alternatives: None,
            fact_kind: None,
            pref_scope: None,
            artifact_kind: None,
        }
    }

    #[test]
    fn tag_match_scores_higher() {
        let memories = vec![
            make_memory("Use nextest for tests", &["testing"], MemoryKind::Fact),
            make_memory(
                "The database uses RDF triples",
                &["database"],
                MemoryKind::Fact,
            ),
        ];

        let results = RecallEngine::recall("how to run testing", &memories, None, None);
        assert!(!results.is_empty());
        assert_eq!(results[0].memory.tags, vec!["testing"]);
    }

    #[test]
    fn content_match_works() {
        let memories = vec![
            make_memory("Use cargo nextest for running tests", &[], MemoryKind::Fact),
            make_memory("Database config is in config.toml", &[], MemoryKind::Fact),
        ];

        let results = RecallEngine::recall("nextest tests", &memories, None, None);
        assert!(!results.is_empty());
        assert!(results[0].memory.content.contains("nextest"));
    }

    #[test]
    fn limit_works() {
        let memories = vec![
            make_memory("Fact one about testing", &["testing"], MemoryKind::Fact),
            make_memory("Fact two about testing", &["testing"], MemoryKind::Fact),
            make_memory("Fact three about testing", &["testing"], MemoryKind::Fact),
        ];

        let results = RecallEngine::recall("testing", &memories, None, Some(2));
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn zero_score_filtered() {
        // Use an old timestamp to avoid the recency bonus
        let mut mem = make_memory(
            "Completely unrelated topic",
            &["unrelated"],
            MemoryKind::Fact,
        );
        mem.created_at = "2020-01-01T00:00:00Z".to_string();
        let memories = vec![mem];

        let results = RecallEngine::recall("testing cargo nextest", &memories, None, None);
        assert!(results.is_empty());
    }
}
