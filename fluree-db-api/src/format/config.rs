//! Result formatting configuration types
//!
//! This module provides configuration for controlling how query results
//! are formatted. Supports JSON-based formats (JSON-LD, SPARQL JSON, TypedJson)
//! and high-performance delimited-text formats (TSV, CSV).

// Re-export SelectMode and QueryOutput from fluree-db-query (canonical source)
pub use fluree_db_query::parse::QueryOutput;
pub use fluree_db_query::SelectMode;

/// Output format selection
///
/// Determines which format to use for query results. JSON formats produce
/// `serde_json::Value`; TSV produces bytes/strings directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    /// JSON-LD Query format (default)
    ///
    /// Simple JSON with compact IRIs. Row shape controlled by `JsonLdRowShape`.
    /// - Array mode: `[["ex:alice", "Alice", 30], ...]`
    /// - Object mode: `[{"?s": "ex:alice", "?name": "Alice"}, ...]`
    #[default]
    JsonLd,

    /// W3C SPARQL 1.1 Query Results JSON format
    ///
    /// Standard format with type metadata:
    /// ```json
    /// {
    ///   "head": {"vars": ["s", "name"]},
    ///   "results": {"bindings": [{"s": {"type": "uri", "value": "..."}}]}
    /// }
    /// ```
    SparqlJson,

    /// Typed JSON format
    ///
    /// Always includes explicit datatype (even for inferable types):
    /// ```json
    /// [{"?s": {"@id": "ex:alice"}, "?name": {"@value": "Alice", "@type": "xsd:string"}}]
    /// ```
    TypedJson,

    /// Tab-separated values (high-performance path)
    ///
    /// Produces a header row of variable names followed by tab-separated values.
    /// IRIs are compacted via `@context`. Bypasses JSON DOM construction and JSON
    /// serialization entirely — writes directly to a byte buffer.
    ///
    /// **Note**: TSV produces `Vec<u8>` / `String`, not `JsonValue`. Use
    /// `format_results_string()`, `QueryResult::to_tsv()`, or `to_tsv_bytes()`
    /// instead of `format_results()`.
    Tsv,

    /// Comma-separated values (high-performance path)
    ///
    /// Same approach as TSV but with comma delimiter and RFC 4180 quoting.
    /// IRIs are compacted via `@context`. Bypasses JSON DOM construction and JSON
    /// serialization entirely — writes directly to a byte buffer.
    ///
    /// **Note**: CSV produces `Vec<u8>` / `String`, not `JsonValue`. Use
    /// `format_results_string()`, `QueryResult::to_csv()`, or `to_csv_bytes()`
    /// instead of `format_results()`.
    Csv,
}

/// JSON-LD Query row shape
///
/// Controls whether rows are formatted as arrays or objects.
/// Only used when `OutputFormat::JsonLd` is selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JsonLdRowShape {
    /// Rows are arrays aligned to select order (Clojure parity)
    ///
    /// ```json
    /// [["ex:alice", "Alice", 30], ["ex:bob", "Bob", 25]]
    /// ```
    #[default]
    Array,

    /// Rows are maps keyed by variable name (API-friendly)
    ///
    /// ```json
    /// [{"?s": "ex:alice", "?name": "Alice"}, {"?s": "ex:bob", "?name": "Bob"}]
    /// ```
    Object,
}

/// Configuration for result formatting
///
/// Controls all aspects of how query results are converted to JSON.
#[derive(Debug, Clone, Default)]
pub struct FormatterConfig {
    /// Output format to use
    pub format: OutputFormat,

    /// Row shape for JSON-LD Query format
    ///
    /// Only used when `format == OutputFormat::JsonLd`.
    pub jsonld_row_shape: JsonLdRowShape,

    /// Select mode (from parsed query)
    ///
    /// Controls whether result is an array or single value.
    pub select_mode: SelectMode,

    /// Pretty-print JSON output
    ///
    /// When true, uses indentation and newlines for human readability.
    pub pretty: bool,
}

impl FormatterConfig {
    /// Create a default JSON-LD Query config (array rows)
    pub fn jsonld() -> Self {
        Self::default()
    }

    /// Create a JSON-LD Query config with object rows
    pub fn jsonld_objects() -> Self {
        Self {
            jsonld_row_shape: JsonLdRowShape::Object,
            ..Default::default()
        }
    }

    /// Create a SPARQL JSON config
    pub fn sparql_json() -> Self {
        Self {
            format: OutputFormat::SparqlJson,
            ..Default::default()
        }
    }

    /// Create a TypedJson config
    pub fn typed_json() -> Self {
        Self {
            format: OutputFormat::TypedJson,
            ..Default::default()
        }
    }

    /// Create a TSV config (high-performance path)
    pub fn tsv() -> Self {
        Self {
            format: OutputFormat::Tsv,
            ..Default::default()
        }
    }

    /// Create a CSV config (high-performance path)
    pub fn csv() -> Self {
        Self {
            format: OutputFormat::Csv,
            ..Default::default()
        }
    }

    /// Set the select mode
    pub fn with_select_mode(mut self, mode: SelectMode) -> Self {
        self.select_mode = mode;
        self
    }

    /// Enable pretty printing
    pub fn with_pretty(mut self) -> Self {
        self.pretty = true;
        self
    }

    /// Set the JSON-LD row shape
    pub fn with_row_shape(mut self, shape: JsonLdRowShape) -> Self {
        self.jsonld_row_shape = shape;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = FormatterConfig::default();
        assert_eq!(config.format, OutputFormat::JsonLd);
        assert_eq!(config.jsonld_row_shape, JsonLdRowShape::Array);
        assert_eq!(config.select_mode, SelectMode::Many);
        assert!(!config.pretty);
    }

    #[test]
    fn test_jsonld_objects_config() {
        let config = FormatterConfig::jsonld_objects();
        assert_eq!(config.format, OutputFormat::JsonLd);
        assert_eq!(config.jsonld_row_shape, JsonLdRowShape::Object);
    }

    #[test]
    fn test_sparql_json_config() {
        let config = FormatterConfig::sparql_json();
        assert_eq!(config.format, OutputFormat::SparqlJson);
    }

    #[test]
    fn test_typed_json_config() {
        let config = FormatterConfig::typed_json();
        assert_eq!(config.format, OutputFormat::TypedJson);
    }

    #[test]
    fn test_tsv_config() {
        let config = FormatterConfig::tsv();
        assert_eq!(config.format, OutputFormat::Tsv);
    }

    #[test]
    fn test_csv_config() {
        let config = FormatterConfig::csv();
        assert_eq!(config.format, OutputFormat::Csv);
    }

    #[test]
    fn test_builder_methods() {
        let config = FormatterConfig::jsonld()
            .with_select_mode(SelectMode::One)
            .with_pretty()
            .with_row_shape(JsonLdRowShape::Object);

        assert_eq!(config.select_mode, SelectMode::One);
        assert!(config.pretty);
        assert_eq!(config.jsonld_row_shape, JsonLdRowShape::Object);
    }
}
