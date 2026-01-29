//! Turtle Token types.
//!
//! Tokens are the output of lexical analysis, ready for parsing.

use std::sync::Arc;

/// A token with its source span.
#[derive(Clone, Debug, PartialEq)]
pub struct Token {
    /// The token kind
    pub kind: TokenKind,
    /// Source location (start byte offset)
    pub start: usize,
    /// Source location (end byte offset)
    pub end: usize,
}

impl Token {
    /// Create a new token.
    pub fn new(kind: TokenKind, start: usize, end: usize) -> Self {
        Self { kind, start, end }
    }

    /// Check if this is an EOF token.
    pub fn is_eof(&self) -> bool {
        matches!(self.kind, TokenKind::Eof)
    }
}

/// Token kinds for Turtle.
#[derive(Clone, Debug, PartialEq)]
pub enum TokenKind {
    // =========================================================================
    // IRIs
    // =========================================================================
    /// Full IRI: `<http://example.org/>`
    Iri(Arc<str>),

    /// Prefixed name namespace: `prefix:` (just the prefix, no local)
    PrefixedNameNs(Arc<str>),

    /// Prefixed name with local: `prefix:local`
    PrefixedName {
        /// Namespace prefix (without colon)
        prefix: Arc<str>,
        /// Local name
        local: Arc<str>,
    },

    // =========================================================================
    // Blank Nodes
    // =========================================================================
    /// Labeled blank node: `_:name`
    BlankNodeLabel(Arc<str>),

    /// Anonymous blank node: `[]`
    Anon,

    /// NIL (empty list): `()`
    Nil,

    // =========================================================================
    // Literals
    // =========================================================================
    /// String literal (unescaped content)
    String(Arc<str>),

    /// Integer literal
    Integer(i64),

    /// Decimal literal (stored as string to preserve precision)
    Decimal(Arc<str>),

    /// Double literal (floating point with exponent)
    Double(f64),

    /// Language tag (e.g., `@en`, `@en-US`)
    /// Stored without the `@` prefix.
    LangTag(Arc<str>),

    // =========================================================================
    // Keywords / Directives
    // =========================================================================
    /// `@prefix` directive
    KwPrefix,

    /// `@base` directive
    KwBase,

    /// SPARQL-style `PREFIX` (without @)
    KwSparqlPrefix,

    /// SPARQL-style `BASE` (without @)
    KwSparqlBase,

    /// `a` keyword (shorthand for rdf:type)
    KwA,

    /// `true` boolean literal
    KwTrue,

    /// `false` boolean literal
    KwFalse,

    // =========================================================================
    // Punctuation
    // =========================================================================
    /// `.`
    Dot,
    /// `,`
    Comma,
    /// `;`
    Semicolon,
    /// `^^` (datatype marker)
    DoubleCaret,
    /// `[`
    LBracket,
    /// `]`
    RBracket,
    /// `(`
    LParen,
    /// `)`
    RParen,

    // =========================================================================
    // Special
    // =========================================================================
    /// End of input
    Eof,
}

impl std::fmt::Display for TokenKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenKind::Iri(s) => write!(f, "<{}>", s),
            TokenKind::PrefixedNameNs(s) => write!(f, "{}:", s),
            TokenKind::PrefixedName { prefix, local } => write!(f, "{}:{}", prefix, local),
            TokenKind::BlankNodeLabel(s) => write!(f, "_:{}", s),
            TokenKind::Anon => write!(f, "[]"),
            TokenKind::Nil => write!(f, "()"),
            TokenKind::String(s) => write!(f, "\"{}\"", s),
            TokenKind::Integer(n) => write!(f, "{}", n),
            TokenKind::Decimal(s) => write!(f, "{}", s),
            TokenKind::Double(n) => write!(f, "{:e}", n),
            TokenKind::LangTag(s) => write!(f, "@{}", s),
            TokenKind::KwPrefix => write!(f, "@prefix"),
            TokenKind::KwBase => write!(f, "@base"),
            TokenKind::KwSparqlPrefix => write!(f, "PREFIX"),
            TokenKind::KwSparqlBase => write!(f, "BASE"),
            TokenKind::KwA => write!(f, "a"),
            TokenKind::KwTrue => write!(f, "true"),
            TokenKind::KwFalse => write!(f, "false"),
            TokenKind::Dot => write!(f, "."),
            TokenKind::Comma => write!(f, ","),
            TokenKind::Semicolon => write!(f, ";"),
            TokenKind::DoubleCaret => write!(f, "^^"),
            TokenKind::LBracket => write!(f, "["),
            TokenKind::RBracket => write!(f, "]"),
            TokenKind::LParen => write!(f, "("),
            TokenKind::RParen => write!(f, ")"),
            TokenKind::Eof => write!(f, "EOF"),
        }
    }
}
