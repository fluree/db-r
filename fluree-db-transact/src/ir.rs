//! Transaction Intermediate Representation (IR)
//!
//! This module defines the internal representation for transactions,
//! including transaction types, patterns, and templates.
//!
//! # Architecture
//!
//! Transactions reuse the query parser for WHERE clauses. This ensures
//! consistent semantics for pattern matching (OPTIONAL, UNION, FILTER, etc.)
//! between queries and transactions.
//!
//! - **WHERE clause**: Uses `UnresolvedPattern` from the query parser, which
//!   keeps IRIs as strings. These are lowered to `Pattern` (with encoded Sids)
//!   during transaction staging.
//!
//! - **INSERT/DELETE templates**: Uses `TripleTemplate` which resolves IRIs to
//!   Sids during parsing. This is appropriate because templates generate flakes,
//!   not match patterns.

use fluree_db_core::{FlakeValue, Sid};
use fluree_db_novelty::TxnMetaEntry;
use fluree_db_query::parse::UnresolvedPattern;
use fluree_db_query::{VarId, VarRegistry};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

/// Type of transaction operation
///
/// Each type has different semantics for handling existing data:
///
/// - **Insert**: No lookups, just add triples (standard JSON-LD semantics, fastest path).
/// - **Upsert**: Replace mode. Deletes existing values for provided predicates, then inserts.
/// - **Update**: Pattern-based modification. Uses WHERE clause to find and modify data.
///
/// All types use the same underlying mechanism (DELETE/INSERT templates with WHERE bindings),
/// but differ in automatic template generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnType {
    /// Insert new data (no lookups, fastest path)
    ///
    /// Use this for standard JSON-LD insert semantics. Triples are added directly
    /// without any existence checks or deletions. If a subject already exists,
    /// new triples are simply added to it.
    Insert,

    /// Insert or update (delete old values for provided predicates, insert new)
    ///
    /// Use this when you want to set property values regardless of whether the
    /// subject exists. For each (subject, predicate) pair in the insert, any
    /// existing values are retracted before the new values are asserted.
    Upsert,

    /// Update existing data based on WHERE pattern matching
    ///
    /// Use this for SPARQL UPDATE-style modifications where DELETE and INSERT
    /// templates reference variables bound by WHERE patterns.
    ///
    /// **Unbound variable behavior**: When a variable in a template is unbound
    /// (no WHERE match) or poisoned (from OPTIONAL), that flake is silently
    /// skipped. This allows patterns like "delete if exists, then insert" to
    /// work without errors when no existing data matches.
    Update,
}

/// A complete parsed transaction
#[derive(Debug)]
pub struct Txn {
    /// Type of transaction
    pub txn_type: TxnType,

    /// WHERE patterns to match (may be empty for insert)
    ///
    /// Uses `UnresolvedPattern` from the query parser, which keeps IRIs as strings.
    /// These are lowered to `Pattern` (with encoded Sids) during staging.
    /// This reuses the query parser's full pattern support (OPTIONAL, UNION, etc.).
    pub where_patterns: Vec<UnresolvedPattern>,

    /// Templates for flakes to delete
    pub delete_templates: Vec<TripleTemplate>,

    /// Templates for flakes to insert
    pub insert_templates: Vec<TripleTemplate>,

    /// Optional inline VALUES bindings
    pub values: Option<InlineValues>,

    /// Transaction options
    pub opts: TxnOpts,

    /// Variable registry for this transaction
    pub vars: VarRegistry,

    /// User-provided transaction metadata (extracted from envelope-form JSON-LD).
    ///
    /// Only populated when the transaction uses envelope form (has `@graph`).
    /// Each entry becomes a triple in the txn-meta graph (`g_id=1`) with the
    /// commit as subject.
    pub txn_meta: Vec<TxnMetaEntry>,

    /// Named graph IRI to g_id mappings introduced by this transaction.
    ///
    /// When a transaction references named graphs (via TriG GRAPH blocks or
    /// JSON-LD @graph with graph IRIs), this map tracks the g_id assignment
    /// for each graph IRI. These mappings are stored in the commit envelope
    /// for replay-safe persistence.
    ///
    /// Reserved g_ids:
    /// - `0`: default graph
    /// - `1`: txn-meta graph (`https://ns.flur.ee/ledger#transactions`)
    /// - `2+`: user-defined named graphs
    pub graph_delta: FxHashMap<u32, String>,
}

impl Txn {
    /// Create a new empty insert transaction
    pub fn insert() -> Self {
        Self {
            txn_type: TxnType::Insert,
            where_patterns: Vec::new(),
            delete_templates: Vec::new(),
            insert_templates: Vec::new(),
            values: None,
            opts: TxnOpts::default(),
            vars: VarRegistry::new(),
            txn_meta: Vec::new(),
            graph_delta: FxHashMap::default(),
        }
    }

    /// Create a new empty upsert transaction
    pub fn upsert() -> Self {
        Self {
            txn_type: TxnType::Upsert,
            ..Self::insert()
        }
    }

    /// Create a new empty update transaction
    pub fn update() -> Self {
        Self {
            txn_type: TxnType::Update,
            ..Self::insert()
        }
    }

    /// Add a WHERE pattern
    pub fn with_where(mut self, pattern: UnresolvedPattern) -> Self {
        self.where_patterns.push(pattern);
        self
    }

    /// Add multiple WHERE patterns
    pub fn with_wheres(mut self, patterns: Vec<UnresolvedPattern>) -> Self {
        self.where_patterns.extend(patterns);
        self
    }

    /// Add a DELETE template
    pub fn with_delete(mut self, template: TripleTemplate) -> Self {
        self.delete_templates.push(template);
        self
    }

    /// Add multiple DELETE templates
    pub fn with_deletes(mut self, templates: Vec<TripleTemplate>) -> Self {
        self.delete_templates.extend(templates);
        self
    }

    /// Add an INSERT template
    pub fn with_insert(mut self, template: TripleTemplate) -> Self {
        self.insert_templates.push(template);
        self
    }

    /// Add multiple INSERT templates
    pub fn with_inserts(mut self, templates: Vec<TripleTemplate>) -> Self {
        self.insert_templates.extend(templates);
        self
    }

    /// Set transaction options
    pub fn with_opts(mut self, opts: TxnOpts) -> Self {
        self.opts = opts;
        self
    }

    /// Set variable registry
    pub fn with_vars(mut self, vars: VarRegistry) -> Self {
        self.vars = vars;
        self
    }

    /// Set inline VALUES bindings
    pub fn with_values(mut self, values: InlineValues) -> Self {
        self.values = Some(values);
        self
    }

    /// Set transaction metadata
    pub fn with_txn_meta(mut self, txn_meta: Vec<TxnMetaEntry>) -> Self {
        self.txn_meta = txn_meta;
        self
    }
}

/// A triple template with potential variables
///
/// Used in both WHERE patterns (for matching) and INSERT/DELETE (for generation).
///
/// # List support
///
/// The `list_index` field supports ordered lists (JSON-LD `@list`). When parsing
/// list-valued properties, each list item becomes a separate template with its
/// position stored in `list_index`. This maps directly to `FlakeMeta.i` in flakes.
///
/// - `list_index: None` - normal multi-valued predicate (unordered)
/// - `list_index: Some(i)` - list element at position `i` for this (subject, predicate)
#[derive(Debug, Clone)]
pub struct TripleTemplate {
    /// Subject term
    pub subject: TemplateTerm,

    /// Predicate term
    pub predicate: TemplateTerm,

    /// Object term
    pub object: TemplateTerm,

    /// Datatype for the object (resolved during parsing, None if not yet known)
    pub datatype: Option<Sid>,

    /// Language tag for string literals (e.g., "en", "fr")
    pub language: Option<String>,

    /// List index for ordered collections (maps to FlakeMeta.i)
    ///
    /// - `None`: normal triple (unordered multi-value)
    /// - `Some(i)`: list element at position `i`
    pub list_index: Option<i32>,

    /// Graph ID for named graphs (maps to RunRecord.g_id in indexer)
    ///
    /// - `0`: default graph
    /// - `1`: txn-meta graph (reserved)
    /// - `2+`: user-defined named graphs
    ///
    /// If None, defaults to 0 (default graph).
    pub graph_id: Option<u32>,
}

impl TripleTemplate {
    /// Create a new triple template
    pub fn new(subject: TemplateTerm, predicate: TemplateTerm, object: TemplateTerm) -> Self {
        Self {
            subject,
            predicate,
            object,
            datatype: None,
            language: None,
            list_index: None,
            graph_id: None,
        }
    }

    /// Set the datatype
    pub fn with_datatype(mut self, dt: Sid) -> Self {
        self.datatype = Some(dt);
        self
    }

    /// Set the language tag
    pub fn with_language(mut self, lang: impl Into<String>) -> Self {
        self.language = Some(lang.into());
        self
    }

    /// Set the list index (for ordered collections / @list support)
    pub fn with_list_index(mut self, index: i32) -> Self {
        self.list_index = Some(index);
        self
    }

    /// Set the graph ID (for named graph support)
    ///
    /// - `0`: default graph
    /// - `1`: txn-meta graph (reserved for commit metadata)
    /// - `2+`: user-defined named graphs
    pub fn with_graph_id(mut self, graph_id: u32) -> Self {
        self.graph_id = Some(graph_id);
        self
    }
}

/// A term in a triple template
#[derive(Debug, Clone)]
pub enum TemplateTerm {
    /// Variable reference (will be substituted from bindings)
    Var(VarId),

    /// Constant IRI/node (already resolved to a Sid)
    Sid(Sid),

    /// Constant literal value
    Value(FlakeValue),

    /// Blank node (will be skolemized to a Sid during flake generation)
    BlankNode(String),
}

impl TemplateTerm {
    /// Check if this term is a variable
    pub fn is_var(&self) -> bool {
        matches!(self, TemplateTerm::Var(_))
    }

    /// Check if this term is a blank node
    pub fn is_blank_node(&self) -> bool {
        matches!(self, TemplateTerm::BlankNode(_))
    }

    /// Check if this term is bound (not a variable)
    pub fn is_bound(&self) -> bool {
        !self.is_var()
    }
}

/// Inline VALUES bindings for initial solutions
#[derive(Debug, Clone)]
pub struct InlineValues {
    /// Variables that are bound by this VALUES clause
    pub vars: Vec<VarId>,

    /// Rows of values (each row has same length as vars)
    pub rows: Vec<Vec<TemplateTerm>>,
}

impl InlineValues {
    /// Create new inline values
    pub fn new(vars: Vec<VarId>, rows: Vec<Vec<TemplateTerm>>) -> Self {
        Self { vars, rows }
    }
}

/// Transaction options
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TxnOpts {
    /// Optional commit message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// Optional author identity (DID or IRI)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,

    /// Branch to commit to (defaults to main branch)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,

    /// JSON-LD context for IRI expansion
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<serde_json::Value>,

    /// Whether to parse bare "?var" object strings as variables in update transactions.
    ///
    /// When false, bare "?x" object values are treated as literal strings unless
    /// explicitly wrapped as {"@variable": "?x"}.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_var_parsing: Option<bool>,

    /// Whether to store the original transaction payload for audit/history.
    ///
    /// When `Some(true)`, the API layer may persist the raw transaction JSON
    /// alongside the commit record (so it can be retrieved via history with
    /// `txn: true`). When `None`/`Some(false)`, the raw transaction is not stored
    /// unless explicitly provided (e.g., signed credential envelope).
    ///
    /// Note: This flag is intentionally *opt-in* to avoid large memory and
    /// storage overhead for bulk ingest (e.g., Turtle expanded to huge JSON-LD).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store_raw_txn: Option<bool>,
}

impl TxnOpts {
    /// Create new options with a message
    pub fn with_message(message: impl Into<String>) -> Self {
        Self {
            message: Some(message.into()),
            ..Default::default()
        }
    }

    /// Set the author
    pub fn author(mut self, author: impl Into<String>) -> Self {
        self.author = Some(author.into());
        self
    }

    /// Set the branch
    pub fn branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = Some(branch.into());
        self
    }

    /// Set the context
    pub fn context(mut self, ctx: serde_json::Value) -> Self {
        self.context = Some(ctx);
        self
    }

    /// Opt in/out of storing raw transaction payload for audit/history.
    pub fn store_raw_txn(mut self, enabled: bool) -> Self {
        self.store_raw_txn = Some(enabled);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_txn_builder() {
        let mut vars = VarRegistry::new();
        let s_var = vars.get_or_insert("?s");
        let p_var = vars.get_or_insert("?p");

        // Note: WHERE patterns are now parsed by the query parser (UnresolvedPattern),
        // so this test focuses on the DELETE/INSERT template builder methods.
        let txn = Txn::update()
            .with_delete(TripleTemplate::new(
                TemplateTerm::Var(s_var),
                TemplateTerm::Sid(Sid::new(1, "ex:name")),
                TemplateTerm::Var(p_var),
            ))
            .with_insert(TripleTemplate::new(
                TemplateTerm::Var(s_var),
                TemplateTerm::Sid(Sid::new(1, "ex:name")),
                TemplateTerm::Value(FlakeValue::String("New Name".to_string())),
            ))
            .with_vars(vars)
            .with_opts(TxnOpts::with_message("Update name"));

        assert_eq!(txn.txn_type, TxnType::Update);
        assert!(txn.where_patterns.is_empty()); // No WHERE patterns added in this test
        assert_eq!(txn.delete_templates.len(), 1);
        assert_eq!(txn.insert_templates.len(), 1);
        assert_eq!(txn.opts.message, Some("Update name".to_string()));
    }

    #[test]
    fn test_template_term_checks() {
        let var_term = TemplateTerm::Var(VarId(0));
        let sid_term = TemplateTerm::Sid(Sid::new(1, "ex:test"));
        let blank_term = TemplateTerm::BlankNode("_:b1".to_string());

        assert!(var_term.is_var());
        assert!(!var_term.is_bound());

        assert!(!sid_term.is_var());
        assert!(sid_term.is_bound());

        assert!(!blank_term.is_var());
        assert!(blank_term.is_blank_node());
        assert!(blank_term.is_bound());
    }
}
