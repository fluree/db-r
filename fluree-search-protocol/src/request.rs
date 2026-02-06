//! Search request types.

use serde::{Deserialize, Serialize};

use crate::DEFAULT_LIMIT;

/// Search request envelope.
///
/// This is the main request type for the `/v1/search` endpoint.
/// It supports both BM25 full-text search and vector similarity search
/// through the [`QueryVariant`] enum.
///
/// # Semantics
///
/// - **`as_of_t`**: If `Some(t)`, search the newest snapshot with watermark <= t.
///   If `None`, search the latest available snapshot.
/// - **`sync`**: If `true`, wait for the latest index head to be loaded before searching.
///   If `false`, search whatever snapshot is already available (fast path).
/// - **`timeout_ms`**: Maximum time to wait for sync + search. Applies to both
///   the sync wait (if `sync=true`) and the search operation itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    /// Protocol version (must match server's supported version).
    pub protocol_version: String,

    /// Optional client-provided request ID for correlation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,

    /// Virtual graph alias (e.g., "products-search:main").
    pub vg_alias: String,

    /// Maximum number of hits to return.
    #[serde(default = "default_limit")]
    pub limit: usize,

    /// Target transaction time for time-travel queries.
    ///
    /// - `Some(t)`: Search snapshot with watermark <= t
    /// - `None`: Search latest available snapshot
    #[serde(skip_serializing_if = "Option::is_none")]
    pub as_of_t: Option<i64>,

    /// Whether to sync to latest index head before searching.
    ///
    /// - `true`: Wait for latest snapshot to be loaded (within timeout)
    /// - `false`: Search immediately with whatever is cached
    #[serde(default)]
    pub sync: bool,

    /// Timeout in milliseconds for the entire operation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,

    /// The search query (BM25 or vector).
    pub query: QueryVariant,
}

fn default_limit() -> usize {
    DEFAULT_LIMIT
}

impl SearchRequest {
    /// Create a BM25 search request.
    pub fn bm25(vg_alias: impl Into<String>, text: impl Into<String>, limit: usize) -> Self {
        Self {
            protocol_version: crate::PROTOCOL_VERSION.to_string(),
            request_id: None,
            vg_alias: vg_alias.into(),
            limit,
            as_of_t: None,
            sync: false,
            timeout_ms: None,
            query: QueryVariant::Bm25 { text: text.into() },
        }
    }

    /// Create a vector search request.
    pub fn vector(vg_alias: impl Into<String>, vector: Vec<f32>, limit: usize) -> Self {
        Self {
            protocol_version: crate::PROTOCOL_VERSION.to_string(),
            request_id: None,
            vg_alias: vg_alias.into(),
            limit,
            as_of_t: None,
            sync: false,
            timeout_ms: None,
            query: QueryVariant::Vector {
                vector,
                metric: None,
            },
        }
    }

    /// Create a vector-similar-to search request.
    pub fn vector_similar_to(
        vg_alias: impl Into<String>,
        to_iri: impl Into<String>,
        limit: usize,
    ) -> Self {
        Self {
            protocol_version: crate::PROTOCOL_VERSION.to_string(),
            request_id: None,
            vg_alias: vg_alias.into(),
            limit,
            as_of_t: None,
            sync: false,
            timeout_ms: None,
            query: QueryVariant::VectorSimilarTo {
                to_iri: to_iri.into(),
                metric: None,
            },
        }
    }

    /// Set the request ID.
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    /// Set the as_of_t for time-travel.
    pub fn with_as_of_t(mut self, t: i64) -> Self {
        self.as_of_t = Some(t);
        self
    }

    /// Enable sync mode.
    pub fn with_sync(mut self, sync: bool) -> Self {
        self.sync = sync;
        self
    }

    /// Set the timeout.
    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }
}

/// Query variant: either BM25 full-text or vector similarity.
///
/// The `kind` field is used as the JSON discriminator for tagged serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QueryVariant {
    /// BM25 full-text search.
    Bm25 {
        /// The search query text.
        text: String,
    },

    /// Vector similarity search with an explicit embedding vector.
    Vector {
        /// The query embedding vector.
        vector: Vec<f32>,

        /// Distance metric (optional; must match VG config if provided).
        #[serde(skip_serializing_if = "Option::is_none")]
        metric: Option<String>,
    },

    /// Vector similarity search by entity IRI.
    ///
    /// The server resolves the entity's embedding and searches for similar vectors.
    /// This requires the VG to have access to the source ledger.
    #[serde(rename = "vector_similar_to")]
    VectorSimilarTo {
        /// The IRI of the entity to find similar items to.
        to_iri: String,

        /// Distance metric (optional; must match VG config if provided).
        #[serde(skip_serializing_if = "Option::is_none")]
        metric: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bm25_request_serialization() {
        let request = SearchRequest {
            protocol_version: "1.0".to_string(),
            request_id: Some("test-123".to_string()),
            vg_alias: "products:main".to_string(),
            limit: 20,
            as_of_t: Some(100),
            sync: true,
            timeout_ms: Some(5000),
            query: QueryVariant::Bm25 {
                text: "wireless headphones".to_string(),
            },
        };

        let json = serde_json::to_string_pretty(&request).unwrap();
        let parsed: SearchRequest = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.protocol_version, "1.0");
        assert_eq!(parsed.vg_alias, "products:main");
        assert_eq!(parsed.limit, 20);
        assert_eq!(parsed.as_of_t, Some(100));
        assert!(parsed.sync);

        match parsed.query {
            QueryVariant::Bm25 { text } => assert_eq!(text, "wireless headphones"),
            _ => panic!("Expected BM25 query"),
        }
    }

    #[test]
    fn test_vector_request_serialization() {
        let request = SearchRequest {
            protocol_version: "1.0".to_string(),
            request_id: None,
            vg_alias: "embeddings:main".to_string(),
            limit: 10,
            as_of_t: None,
            sync: false,
            timeout_ms: None,
            query: QueryVariant::Vector {
                vector: vec![0.1, 0.2, 0.3],
                metric: Some("cosine".to_string()),
            },
        };

        let json = serde_json::to_string(&request).unwrap();
        let parsed: SearchRequest = serde_json::from_str(&json).unwrap();

        match parsed.query {
            QueryVariant::Vector { vector, metric } => {
                assert_eq!(vector, vec![0.1, 0.2, 0.3]);
                assert_eq!(metric, Some("cosine".to_string()));
            }
            _ => panic!("Expected Vector query"),
        }
    }

    #[test]
    fn test_default_limit() {
        let json = r#"{
            "protocol_version": "1.0",
            "vg_alias": "test:main",
            "query": { "kind": "bm25", "text": "test" }
        }"#;

        let parsed: SearchRequest = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.limit, DEFAULT_LIMIT);
        assert!(!parsed.sync);
    }

    #[test]
    fn test_query_variant_discriminator() {
        let bm25_json = r#"{"kind": "bm25", "text": "hello"}"#;
        let vector_json = r#"{"kind": "vector", "vector": [1.0, 2.0]}"#;
        let similar_json = r#"{"kind": "vector_similar_to", "to_iri": "ex:item-1"}"#;

        let bm25: QueryVariant = serde_json::from_str(bm25_json).unwrap();
        let vector: QueryVariant = serde_json::from_str(vector_json).unwrap();
        let similar: QueryVariant = serde_json::from_str(similar_json).unwrap();

        assert!(matches!(bm25, QueryVariant::Bm25 { .. }));
        assert!(matches!(vector, QueryVariant::Vector { .. }));
        assert!(matches!(similar, QueryVariant::VectorSimilarTo { .. }));
    }
}
