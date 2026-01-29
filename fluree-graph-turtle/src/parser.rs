//! Turtle parser that emits to GraphSink.
//!
//! Parses Turtle syntax and emits triple events to a GraphSink implementation.

use std::collections::HashMap;

use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, TermId};
use fluree_vocab::rdf;

use crate::error::{Result, TurtleError};
use crate::lex::{tokenize, Token, TokenKind};

/// RDF well-known IRIs (imported from vocab crate)
const RDF_TYPE: &str = rdf::TYPE;
const RDF_FIRST: &str = rdf::FIRST;
const RDF_REST: &str = rdf::REST;
const RDF_NIL: &str = rdf::NIL;

/// Turtle parser state.
pub struct Parser<'a, S> {
    tokens: Vec<Token>,
    pos: usize,
    sink: &'a mut S,
    /// Prefix mappings (prefix -> namespace IRI)
    prefixes: HashMap<String, String>,
    /// Base IRI for relative IRI resolution
    base: Option<String>,
}

impl<'a, S: GraphSink> Parser<'a, S> {
    /// Create a new parser.
    pub fn new(input: &str, sink: &'a mut S) -> Result<Self> {
        Ok(Self {
            tokens: tokenize(input)?,
            pos: 0,
            sink,
            prefixes: HashMap::new(),
            base: None,
        })
    }

    /// Parse the entire Turtle document.
    pub fn parse(mut self) -> Result<()> {
        while !self.is_at_end() {
            self.parse_statement()?;
        }
        Ok(())
    }

    /// Check if we're at the end of input.
    fn is_at_end(&self) -> bool {
        matches!(self.current().kind, TokenKind::Eof)
    }

    /// Get the current token.
    fn current(&self) -> &Token {
        &self.tokens[self.pos]
    }

    /// Advance to the next token.
    fn advance(&mut self) -> &Token {
        let token = &self.tokens[self.pos];
        if !self.is_at_end() {
            self.pos += 1;
        }
        token
    }

    /// Check if the current token matches the expected kind.
    fn check(&self, kind: &TokenKind) -> bool {
        std::mem::discriminant(&self.current().kind) == std::mem::discriminant(kind)
    }

    /// Consume a token of the expected kind, or return an error.
    fn expect(&mut self, kind: &TokenKind) -> Result<&Token> {
        if self.check(kind) {
            Ok(self.advance())
        } else {
            Err(TurtleError::parse(
                self.current().start,
                format!("expected {:?}, found {:?}", kind, self.current().kind),
            ))
        }
    }

    /// Parse a single statement (directive or triples).
    fn parse_statement(&mut self) -> Result<()> {
        match &self.current().kind {
            TokenKind::KwPrefix | TokenKind::KwSparqlPrefix => self.parse_prefix_directive(),
            TokenKind::KwBase | TokenKind::KwSparqlBase => self.parse_base_directive(),
            TokenKind::Eof => Ok(()),
            _ => self.parse_triples(),
        }
    }

    /// Parse @prefix or PREFIX directive.
    fn parse_prefix_directive(&mut self) -> Result<()> {
        let is_sparql_style = matches!(self.current().kind, TokenKind::KwSparqlPrefix);
        self.advance(); // consume @prefix or PREFIX

        // Get prefix name (must be PrefixedNameNs)
        let prefix = match &self.current().kind {
            TokenKind::PrefixedNameNs(p) => p.to_string(),
            _ => {
                return Err(TurtleError::parse(
                    self.current().start,
                    "expected prefix namespace",
                ))
            }
        };
        self.advance();

        // Get namespace IRI
        let namespace = match &self.current().kind {
            TokenKind::Iri(iri) => self.resolve_iri(iri)?,
            _ => {
                return Err(TurtleError::parse(
                    self.current().start,
                    "expected IRI for prefix namespace",
                ))
            }
        };
        self.advance();

        // Register prefix
        self.sink.on_prefix(&prefix, &namespace);
        self.prefixes.insert(prefix, namespace);

        // Consume trailing dot (required for @prefix, not for PREFIX)
        if !is_sparql_style {
            self.expect(&TokenKind::Dot)?;
        }

        Ok(())
    }

    /// Parse @base or BASE directive.
    fn parse_base_directive(&mut self) -> Result<()> {
        let is_sparql_style = matches!(self.current().kind, TokenKind::KwSparqlBase);
        self.advance(); // consume @base or BASE

        // Get base IRI
        let base_iri = match &self.current().kind {
            TokenKind::Iri(iri) => iri.to_string(),
            _ => {
                return Err(TurtleError::parse(
                    self.current().start,
                    "expected IRI for base",
                ))
            }
        };
        self.advance();

        // Set base
        self.sink.on_base(&base_iri);
        self.base = Some(base_iri);

        // Consume trailing dot (required for @base, not for BASE)
        if !is_sparql_style {
            self.expect(&TokenKind::Dot)?;
        }

        Ok(())
    }

    /// Parse a triple statement.
    fn parse_triples(&mut self) -> Result<()> {
        // Parse subject
        let subject = self.parse_subject()?;

        // Parse predicate-object list
        self.parse_predicate_object_list(subject)?;

        // Consume trailing dot
        self.expect(&TokenKind::Dot)?;

        Ok(())
    }

    /// Parse a subject term.
    fn parse_subject(&mut self) -> Result<TermId> {
        match &self.current().kind.clone() {
            TokenKind::Iri(iri) => {
                let resolved = self.resolve_iri(iri)?;
                self.advance();
                Ok(self.sink.term_iri(&resolved))
            }
            TokenKind::PrefixedName { prefix, local } => {
                let iri = self.expand_prefixed_name(prefix, local)?;
                self.advance();
                Ok(self.sink.term_iri(&iri))
            }
            TokenKind::PrefixedNameNs(prefix) => {
                let iri = self.expand_prefixed_name(prefix, "")?;
                self.advance();
                Ok(self.sink.term_iri(&iri))
            }
            TokenKind::BlankNodeLabel(label) => {
                self.advance();
                Ok(self.sink.term_blank(Some(label)))
            }
            TokenKind::Anon => {
                self.advance();
                Ok(self.sink.term_blank(None))
            }
            TokenKind::LBracket => {
                // Blank node with property list: [ ... ]
                self.parse_blank_node_property_list()
            }
            TokenKind::LParen => {
                // Collection (RDF list)
                self.parse_collection()
            }
            TokenKind::Nil => {
                // Empty collection () is tokenized as Nil
                self.advance();
                Ok(self.sink.term_iri(RDF_NIL))
            }
            _ => Err(TurtleError::parse(
                self.current().start,
                format!("expected subject, found {:?}", self.current().kind),
            )),
        }
    }

    /// Parse a predicate-object list.
    fn parse_predicate_object_list(&mut self, subject: TermId) -> Result<()> {
        loop {
            // Parse predicate
            let predicate = self.parse_predicate()?;

            // Parse object list
            self.parse_object_list(subject, predicate)?;

            // Check for semicolon (more predicate-object pairs)
            if matches!(self.current().kind, TokenKind::Semicolon) {
                self.advance();
                // Semicolon can be followed by another predicate or end
                if matches!(
                    self.current().kind,
                    TokenKind::Dot | TokenKind::RBracket | TokenKind::Eof
                ) {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Parse a predicate.
    fn parse_predicate(&mut self) -> Result<TermId> {
        match &self.current().kind.clone() {
            TokenKind::Iri(iri) => {
                let resolved = self.resolve_iri(iri)?;
                self.advance();
                Ok(self.sink.term_iri(&resolved))
            }
            TokenKind::PrefixedName { prefix, local } => {
                let iri = self.expand_prefixed_name(prefix, local)?;
                self.advance();
                Ok(self.sink.term_iri(&iri))
            }
            TokenKind::PrefixedNameNs(prefix) => {
                let iri = self.expand_prefixed_name(prefix, "")?;
                self.advance();
                Ok(self.sink.term_iri(&iri))
            }
            TokenKind::KwA => {
                self.advance();
                Ok(self.sink.term_iri(RDF_TYPE))
            }
            _ => Err(TurtleError::parse(
                self.current().start,
                format!("expected predicate, found {:?}", self.current().kind),
            )),
        }
    }

    /// Parse an object list (comma-separated objects).
    fn parse_object_list(&mut self, subject: TermId, predicate: TermId) -> Result<()> {
        loop {
            // Parse object
            let object = self.parse_object()?;

            // Emit triple
            self.sink.emit_triple(subject, predicate, object);

            // Check for comma (more objects)
            if matches!(self.current().kind, TokenKind::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Parse an object term.
    fn parse_object(&mut self) -> Result<TermId> {
        match &self.current().kind.clone() {
            TokenKind::Iri(iri) => {
                let resolved = self.resolve_iri(iri)?;
                self.advance();
                Ok(self.sink.term_iri(&resolved))
            }
            TokenKind::PrefixedName { prefix, local } => {
                let iri = self.expand_prefixed_name(prefix, local)?;
                self.advance();
                Ok(self.sink.term_iri(&iri))
            }
            TokenKind::PrefixedNameNs(prefix) => {
                let iri = self.expand_prefixed_name(prefix, "")?;
                self.advance();
                Ok(self.sink.term_iri(&iri))
            }
            TokenKind::BlankNodeLabel(label) => {
                self.advance();
                Ok(self.sink.term_blank(Some(label)))
            }
            TokenKind::Anon => {
                self.advance();
                Ok(self.sink.term_blank(None))
            }
            TokenKind::LBracket => {
                self.parse_blank_node_property_list()
            }
            TokenKind::LParen => {
                self.parse_collection()
            }
            TokenKind::Nil => {
                // Empty collection () is tokenized as Nil
                self.advance();
                Ok(self.sink.term_iri(RDF_NIL))
            }
            TokenKind::String(_) => self.parse_literal(),
            TokenKind::Integer(_) => self.parse_literal(),
            TokenKind::Decimal(_) => self.parse_literal(),
            TokenKind::Double(_) => self.parse_literal(),
            TokenKind::KwTrue | TokenKind::KwFalse => self.parse_literal(),
            _ => Err(TurtleError::parse(
                self.current().start,
                format!("expected object, found {:?}", self.current().kind),
            )),
        }
    }

    /// Parse a literal (string with optional language tag or datatype).
    fn parse_literal(&mut self) -> Result<TermId> {
        match &self.current().kind.clone() {
            TokenKind::String(value) => {
                let value = value.clone();
                self.advance();

                // Check for language tag or datatype
                match &self.current().kind.clone() {
                    TokenKind::LangTag(lang) => {
                        let lang = lang.clone();
                        self.advance();
                        Ok(self.sink.term_literal(&value, Datatype::rdf_lang_string(), Some(&lang)))
                    }
                    TokenKind::DoubleCaret => {
                        self.advance();
                        let datatype_iri = self.parse_datatype_iri()?;
                        let datatype = Datatype::from_iri(&datatype_iri);
                        Ok(self.sink.term_literal(&value, datatype, None))
                    }
                    _ => {
                        // Plain string literal
                        Ok(self.sink.term_literal(&value, Datatype::xsd_string(), None))
                    }
                }
            }
            TokenKind::Integer(n) => {
                let n = *n;
                self.advance();
                Ok(self.sink.term_literal_value(LiteralValue::Integer(n), Datatype::xsd_integer()))
            }
            TokenKind::Decimal(s) => {
                let s = s.clone();
                self.advance();
                Ok(self.sink.term_literal(&s, Datatype::xsd_decimal(), None))
            }
            TokenKind::Double(n) => {
                let n = *n;
                self.advance();
                Ok(self.sink.term_literal_value(LiteralValue::Double(n), Datatype::xsd_double()))
            }
            TokenKind::KwTrue => {
                self.advance();
                Ok(self.sink.term_literal_value(LiteralValue::Boolean(true), Datatype::xsd_boolean()))
            }
            TokenKind::KwFalse => {
                self.advance();
                Ok(self.sink.term_literal_value(LiteralValue::Boolean(false), Datatype::xsd_boolean()))
            }
            _ => Err(TurtleError::parse(
                self.current().start,
                format!("expected literal, found {:?}", self.current().kind),
            )),
        }
    }

    /// Parse a datatype IRI after ^^.
    fn parse_datatype_iri(&mut self) -> Result<String> {
        match &self.current().kind.clone() {
            TokenKind::Iri(iri) => {
                let resolved = self.resolve_iri(iri)?;
                self.advance();
                Ok(resolved)
            }
            TokenKind::PrefixedName { prefix, local } => {
                let iri = self.expand_prefixed_name(prefix, local)?;
                self.advance();
                Ok(iri)
            }
            TokenKind::PrefixedNameNs(prefix) => {
                let iri = self.expand_prefixed_name(prefix, "")?;
                self.advance();
                Ok(iri)
            }
            _ => Err(TurtleError::parse(
                self.current().start,
                format!("expected datatype IRI, found {:?}", self.current().kind),
            )),
        }
    }

    /// Parse a blank node property list: `[ predicate object ; ... ]`
    fn parse_blank_node_property_list(&mut self) -> Result<TermId> {
        self.expect(&TokenKind::LBracket)?;

        // Create anonymous blank node
        let bnode = self.sink.term_blank(None);

        // Parse property list if not empty
        if !matches!(self.current().kind, TokenKind::RBracket) {
            self.parse_predicate_object_list(bnode)?;
        }

        self.expect(&TokenKind::RBracket)?;

        Ok(bnode)
    }

    /// Parse a collection (RDF list): `( item1 item2 ... )`
    fn parse_collection(&mut self) -> Result<TermId> {
        self.expect(&TokenKind::LParen)?;

        // Check for empty collection
        if matches!(self.current().kind, TokenKind::RParen) {
            self.advance();
            return Ok(self.sink.term_iri(RDF_NIL));
        }

        // Parse items and build linked list
        let rdf_first = self.sink.term_iri(RDF_FIRST);
        let rdf_rest = self.sink.term_iri(RDF_REST);
        let rdf_nil = self.sink.term_iri(RDF_NIL);

        let first_node = self.sink.term_blank(None);
        let mut current_node = first_node;

        loop {
            // Parse the item
            let item = self.parse_object()?;

            // Emit rdf:first triple
            self.sink.emit_triple(current_node, rdf_first, item);

            // Check if there are more items
            if matches!(self.current().kind, TokenKind::RParen) {
                // End of list - emit rdf:rest rdf:nil
                self.sink.emit_triple(current_node, rdf_rest, rdf_nil);
                break;
            } else {
                // More items - create next node and emit rdf:rest
                let next_node = self.sink.term_blank(None);
                self.sink.emit_triple(current_node, rdf_rest, next_node);
                current_node = next_node;
            }
        }

        self.expect(&TokenKind::RParen)?;

        Ok(first_node)
    }

    /// Resolve a potentially relative IRI against the base (RFC3986).
    ///
    /// Implements the reference resolution algorithm from RFC 3986 Section 5.
    fn resolve_iri(&self, reference: &str) -> Result<String> {
        // Empty reference = base
        if reference.is_empty() {
            return match &self.base {
                Some(base) => Ok(base.clone()),
                None => Err(TurtleError::IriResolution(
                    "empty IRI reference without base".to_string(),
                )),
            };
        }

        // Check if reference has a scheme (absolute IRI)
        if let Some(colon_pos) = reference.find(':') {
            // Only treat as absolute if the part before ':' looks like a scheme
            // (letters followed by letters/digits/+/-/.)
            let potential_scheme = &reference[..colon_pos];
            if !potential_scheme.is_empty()
                && potential_scheme.chars().next().unwrap().is_ascii_alphabetic()
                && potential_scheme
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.')
            {
                // Absolute IRI - return as-is
                return Ok(reference.to_string());
            }
        }

        // Relative reference - need base
        let base = match &self.base {
            Some(b) => b,
            None => {
                return Err(TurtleError::IriResolution(format!(
                    "relative IRI '{}' without base",
                    reference
                )));
            }
        };

        // Parse base IRI components
        let (base_scheme, base_authority, base_path, _base_query) = parse_iri_components(base);

        // RFC3986 Section 5.2.2 - Transform References
        let (scheme, authority, path, query) = if reference.starts_with("//") {
            // Reference has authority - use base scheme only
            let (ref_authority, ref_path, ref_query) = parse_hier_part(&reference[2..]);
            (
                base_scheme.to_string(),
                Some(ref_authority),
                remove_dot_segments(&ref_path),
                ref_query,
            )
        } else if reference.starts_with('/') {
            // Absolute path reference
            let (ref_path, ref_query) = split_path_query(reference);
            (
                base_scheme.to_string(),
                base_authority.map(|s| s.to_string()),
                remove_dot_segments(ref_path),
                ref_query.map(|s| s.to_string()),
            )
        } else if reference.starts_with('?') {
            // Query-only reference
            (
                base_scheme.to_string(),
                base_authority.map(|s| s.to_string()),
                base_path.to_string(),
                Some(reference[1..].to_string()),
            )
        } else if reference.starts_with('#') {
            // Fragment-only reference (fragment not typically in IRI, but handle gracefully)
            (
                base_scheme.to_string(),
                base_authority.map(|s| s.to_string()),
                base_path.to_string(),
                None,
            )
        } else {
            // Relative path reference - merge with base
            let (ref_path, ref_query) = split_path_query(reference);
            let merged = if base_authority.is_some() && base_path.is_empty() {
                format!("/{}", ref_path)
            } else {
                // Remove last segment of base path and append reference
                let base_dir = match base_path.rfind('/') {
                    Some(pos) => &base_path[..=pos],
                    None => "",
                };
                format!("{}{}", base_dir, ref_path)
            };
            (
                base_scheme.to_string(),
                base_authority.map(|s| s.to_string()),
                remove_dot_segments(&merged),
                ref_query.map(|s| s.to_string()),
            )
        };

        // Reconstruct the target IRI
        let mut result = scheme;
        result.push(':');
        if let Some(auth) = authority {
            result.push_str("//");
            result.push_str(&auth);
        }
        result.push_str(&path);
        if let Some(q) = query {
            result.push('?');
            result.push_str(&q);
        }

        Ok(result)
    }

    /// Expand a prefixed name to a full IRI.
    fn expand_prefixed_name(&self, prefix: &str, local: &str) -> Result<String> {
        if let Some(namespace) = self.prefixes.get(prefix) {
            Ok(format!("{}{}", namespace, local))
        } else {
            Err(TurtleError::UndefinedPrefix(prefix.to_string()))
        }
    }
}

// =============================================================================
// RFC3986 IRI Resolution Helpers
// =============================================================================

/// Parse an IRI into (scheme, authority, path, query) components.
fn parse_iri_components(iri: &str) -> (&str, Option<&str>, &str, Option<&str>) {
    // Find scheme
    let (scheme, rest) = match iri.find(':') {
        Some(pos) => (&iri[..pos], &iri[pos + 1..]),
        None => return ("", None, iri, None),
    };

    // Check for authority (starts with //)
    let (authority, path_query) = if rest.starts_with("//") {
        let after_slashes = &rest[2..];
        // Authority ends at /, ?, or #
        let auth_end = after_slashes
            .find(|c| c == '/' || c == '?' || c == '#')
            .unwrap_or(after_slashes.len());
        (
            Some(&after_slashes[..auth_end]),
            &after_slashes[auth_end..],
        )
    } else {
        (None, rest)
    };

    // Split path and query
    let (path, query) = split_path_query(path_query);

    (scheme, authority, path, query)
}

/// Parse hierarchical part after "//" - returns (authority, path, query).
fn parse_hier_part(s: &str) -> (String, String, Option<String>) {
    // Authority ends at /, ?, or #
    let auth_end = s
        .find(|c| c == '/' || c == '?' || c == '#')
        .unwrap_or(s.len());
    let authority = s[..auth_end].to_string();
    let rest = &s[auth_end..];

    let (path, query) = split_path_query(rest);
    (authority, path.to_string(), query.map(|q| q.to_string()))
}

/// Split a path from its query component.
fn split_path_query(s: &str) -> (&str, Option<&str>) {
    // Also handle fragments for completeness
    let s = match s.find('#') {
        Some(pos) => &s[..pos],
        None => s,
    };

    match s.find('?') {
        Some(pos) => (&s[..pos], Some(&s[pos + 1..])),
        None => (s, None),
    }
}

/// Remove dot segments from a path (RFC3986 Section 5.2.4).
fn remove_dot_segments(path: &str) -> String {
    let mut output: Vec<&str> = Vec::new();

    for segment in path.split('/') {
        match segment {
            "." => {
                // Skip single dot
            }
            ".." => {
                // Go up one level (but not above root)
                output.pop();
            }
            s => {
                output.push(s);
            }
        }
    }

    // Preserve leading slash
    let result = output.join("/");
    if path.starts_with('/') && !result.starts_with('/') {
        format!("/{}", result)
    } else {
        result
    }
}

/// Parse a Turtle document into GraphSink events.
pub fn parse<S: GraphSink>(input: &str, sink: &mut S) -> Result<()> {
    Parser::new(input, sink)?.parse()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_graph_ir::{Graph, GraphCollectorSink, Term};

    fn parse_to_graph(input: &str) -> Result<Graph> {
        let mut sink = GraphCollectorSink::new();
        parse(input, &mut sink)?;
        Ok(sink.finish())
    }

    #[test]
    fn test_simple_triple() {
        let input = r#"<http://example.org/alice> <http://xmlns.com/foaf/0.1/name> "Alice" ."#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        assert!(matches!(&triple.s, Term::Iri(iri) if iri.as_ref() == "http://example.org/alice"));
        assert!(matches!(&triple.p, Term::Iri(iri) if iri.as_ref() == "http://xmlns.com/foaf/0.1/name"));
    }

    #[test]
    fn test_prefix_directive() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            @prefix foaf: <http://xmlns.com/foaf/0.1/> .
            ex:alice foaf:name "Alice" .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        assert!(matches!(&triple.s, Term::Iri(iri) if iri.as_ref() == "http://example.org/alice"));
        assert!(matches!(&triple.p, Term::Iri(iri) if iri.as_ref() == "http://xmlns.com/foaf/0.1/name"));
    }

    #[test]
    fn test_a_keyword() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            ex:alice a ex:Person .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        assert!(matches!(&triple.p, Term::Iri(iri) if iri.as_ref() == RDF_TYPE));
    }

    #[test]
    fn test_semicolon_syntax() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            ex:alice ex:name "Alice" ;
                     ex:age 30 .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 2);
    }

    #[test]
    fn test_comma_syntax() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            ex:alice ex:knows ex:bob, ex:charlie .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 2);
    }

    #[test]
    fn test_blank_node() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            _:b1 ex:name "Bob" .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        assert!(matches!(&triple.s, Term::BlankNode(_)));
    }

    #[test]
    fn test_blank_node_property_list() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            ex:alice ex:knows [ ex:name "Bob" ] .
        "#;
        let graph = parse_to_graph(input).unwrap();

        // Should have 2 triples: alice knows _:b, _:b name "Bob"
        assert_eq!(graph.len(), 2);
    }

    #[test]
    fn test_typed_literal() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            ex:alice ex:birthdate "2000-01-01"^^xsd:date .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        if let Term::Literal { datatype, .. } = &triple.o {
            assert_eq!(datatype.as_iri(), "http://www.w3.org/2001/XMLSchema#date");
        } else {
            panic!("Expected literal");
        }
    }

    #[test]
    fn test_language_tagged_literal() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            ex:alice ex:name "Alice"@en .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        if let Term::Literal { language, .. } = &triple.o {
            assert_eq!(language.as_deref(), Some("en"));
        } else {
            panic!("Expected literal");
        }
    }

    #[test]
    fn test_integer_literal() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            ex:alice ex:age 30 .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        if let Term::Literal { value: LiteralValue::Integer(n), .. } = &triple.o {
            assert_eq!(*n, 30);
        } else {
            panic!("Expected integer literal");
        }
    }

    #[test]
    fn test_boolean_literal() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            ex:alice ex:active true .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        if let Term::Literal { value: LiteralValue::Boolean(b), .. } = &triple.o {
            assert!(*b);
        } else {
            panic!("Expected boolean literal");
        }
    }

    #[test]
    fn test_collection() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            ex:alice ex:friends ( ex:bob ex:charlie ) .
        "#;
        let graph = parse_to_graph(input).unwrap();

        // Collection (bob, charlie) produces:
        // _:b1 rdf:first ex:bob
        // _:b1 rdf:rest _:b2
        // _:b2 rdf:first ex:charlie
        // _:b2 rdf:rest rdf:nil
        // Plus: ex:alice ex:friends _:b1
        assert_eq!(graph.len(), 5);
    }

    #[test]
    fn test_empty_collection() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            ex:alice ex:friends () .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        assert!(matches!(&triple.o, Term::Iri(iri) if iri.as_ref() == RDF_NIL));
    }

    #[test]
    fn test_sparql_prefix_syntax() {
        let input = r#"
            PREFIX ex: <http://example.org/>
            ex:alice ex:name "Alice" .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
    }

    #[test]
    fn test_base_iri_resolution() {
        // Test RFC3986 base IRI resolution
        let input = r#"
            @base <http://example.org/path/> .
            <alice> <name> "Alice" .
            <../bob> <name> "Bob" .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 2);

        let triples: Vec<_> = graph.iter().collect();

        // Check that relative IRIs were resolved correctly
        let alice_triple = triples.iter().find(|t| {
            matches!(&t.o, Term::Literal { value, .. } if matches!(value, fluree_graph_ir::LiteralValue::String(s) if s.as_ref() == "Alice"))
        }).unwrap();
        assert!(matches!(&alice_triple.s, Term::Iri(iri) if iri.as_ref() == "http://example.org/path/alice"));
        assert!(matches!(&alice_triple.p, Term::Iri(iri) if iri.as_ref() == "http://example.org/path/name"));

        let bob_triple = triples.iter().find(|t| {
            matches!(&t.o, Term::Literal { value, .. } if matches!(value, fluree_graph_ir::LiteralValue::String(s) if s.as_ref() == "Bob"))
        }).unwrap();
        // ../bob from http://example.org/path/ should resolve to http://example.org/bob
        assert!(matches!(&bob_triple.s, Term::Iri(iri) if iri.as_ref() == "http://example.org/bob"));
    }

    #[test]
    fn test_base_iri_absolute_path() {
        let input = r#"
            @base <http://example.org/a/b/c> .
            </d/e> <name> "test" .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        // Absolute path /d/e should become http://example.org/d/e
        assert!(matches!(&triple.s, Term::Iri(iri) if iri.as_ref() == "http://example.org/d/e"));
    }

    #[test]
    fn test_empty_iri_resolves_to_base() {
        let input = r#"
            @base <http://example.org/doc> .
            <> <name> "The Document" .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        // Empty IRI should resolve to base
        assert!(matches!(&triple.s, Term::Iri(iri) if iri.as_ref() == "http://example.org/doc"));
    }
}
