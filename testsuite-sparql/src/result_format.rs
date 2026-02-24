//! Parse W3C SPARQL expected result files (.srx, .srj, .ttl) into a
//! format-independent [`SparqlResults`] representation.

use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use fluree_graph_ir::{GraphCollectorSink, Term as IrTerm};
use fluree_graph_turtle::parse as parse_turtle;
use quick_xml::events::Event;
use quick_xml::Reader;

use crate::files::read_file_to_string;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A single RDF term in a SPARQL result binding.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RdfTerm {
    Iri(String),
    BlankNode(String),
    Literal {
        value: String,
        datatype: Option<String>,
        language: Option<String>,
    },
}

/// An RDF triple in a CONSTRUCT/DESCRIBE result graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Triple {
    pub subject: RdfTerm,
    pub predicate: RdfTerm,
    pub object: RdfTerm,
}

/// Normalized SPARQL query result (format-independent).
#[derive(Debug)]
pub enum SparqlResults {
    /// SELECT query: variable names + solution multiset.
    Solutions {
        variables: Vec<String>,
        solutions: Vec<HashMap<String, RdfTerm>>,
    },
    /// ASK query: boolean result.
    Boolean(bool),
    /// CONSTRUCT/DESCRIBE result: set of RDF triples.
    Graph(Vec<Triple>),
}

// ---------------------------------------------------------------------------
// Dispatch by file extension
// ---------------------------------------------------------------------------

/// Parse an expected result file referenced by URL.
///
/// Dispatches to the appropriate parser based on file extension:
/// - `.srx` → SPARQL Results XML
/// - `.srj` → SPARQL Results JSON
/// - `.ttl` → Turtle (CONSTRUCT results — not yet supported)
pub fn parse_expected_results(url: &str) -> Result<SparqlResults> {
    let content =
        read_file_to_string(url).with_context(|| format!("Reading expected result file: {url}"))?;

    if url.ends_with(".srx") {
        parse_srx(&content).with_context(|| format!("Parsing .srx: {url}"))
    } else if url.ends_with(".srj") {
        parse_srj(&content).with_context(|| format!("Parsing .srj: {url}"))
    } else if url.ends_with(".ttl") || url.ends_with(".rdf") {
        parse_ttl_graph(&content).with_context(|| format!("Parsing .ttl/.rdf graph: {url}"))
    } else {
        bail!("Unknown result file format: {url}")
    }
}

// ---------------------------------------------------------------------------
// SPARQL Results XML (.srx) parser
// ---------------------------------------------------------------------------

/// Parse SPARQL Results XML format.
///
/// Handles both SELECT results (`<results>` with `<result>` children)
/// and ASK results (`<boolean>`).
pub fn parse_srx(xml: &str) -> Result<SparqlResults> {
    let mut reader = Reader::from_str(xml);

    let mut variables: Vec<String> = Vec::new();
    let mut solutions: Vec<HashMap<String, RdfTerm>> = Vec::new();

    // Current parsing state
    let mut current_binding_name: Option<String> = None;
    let mut current_solution: Option<HashMap<String, RdfTerm>> = None;

    // What kind of term element are we inside?
    #[derive(Clone)]
    enum TermKind {
        Uri,
        Bnode,
        Literal {
            datatype: Option<String>,
            language: Option<String>,
        },
    }
    let mut current_term: Option<TermKind> = None;
    let mut text_buf = String::new();
    let mut in_boolean = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => {
                let local_name = e.local_name();
                match local_name.as_ref() {
                    b"variable" => {
                        for attr in e.attributes().flatten() {
                            if attr.key.local_name().as_ref() == b"name" {
                                let name = String::from_utf8_lossy(&attr.value).to_string();
                                variables.push(name);
                            }
                        }
                    }
                    b"result" => {
                        current_solution = Some(HashMap::new());
                    }
                    b"binding" => {
                        for attr in e.attributes().flatten() {
                            if attr.key.local_name().as_ref() == b"name" {
                                current_binding_name =
                                    Some(String::from_utf8_lossy(&attr.value).to_string());
                            }
                        }
                        text_buf.clear();
                    }
                    b"uri" => {
                        current_term = Some(TermKind::Uri);
                        text_buf.clear();
                    }
                    b"bnode" => {
                        current_term = Some(TermKind::Bnode);
                        text_buf.clear();
                    }
                    b"literal" => {
                        let mut datatype = None;
                        let mut language = None;
                        for attr in e.attributes().flatten() {
                            let key = attr.key.local_name();
                            if key.as_ref() == b"datatype" {
                                datatype = Some(String::from_utf8_lossy(&attr.value).to_string());
                            } else if key.as_ref() == b"lang" {
                                language = Some(String::from_utf8_lossy(&attr.value).to_string());
                            }
                        }
                        // Also check for xml:lang
                        for attr in e.attributes().flatten() {
                            let key_bytes = attr.key.0;
                            if key_bytes == b"xml:lang" {
                                language = Some(String::from_utf8_lossy(&attr.value).to_string());
                            }
                        }
                        current_term = Some(TermKind::Literal { datatype, language });
                        text_buf.clear();
                    }
                    b"boolean" => {
                        in_boolean = true;
                        text_buf.clear();
                    }
                    _ => {}
                }
            }
            Ok(Event::End(ref e)) => {
                let local_name = e.local_name();
                match local_name.as_ref() {
                    b"result" => {
                        if let Some(solution) = current_solution.take() {
                            solutions.push(solution);
                        }
                    }
                    b"binding" => {
                        current_binding_name = None;
                    }
                    b"uri" => {
                        if let Some(TermKind::Uri) = current_term {
                            if let Some(ref name) = current_binding_name {
                                if let Some(ref mut solution) = current_solution {
                                    solution.insert(name.clone(), RdfTerm::Iri(text_buf.clone()));
                                }
                            }
                        }
                        current_term = None;
                    }
                    b"bnode" => {
                        if let Some(TermKind::Bnode) = current_term {
                            if let Some(ref name) = current_binding_name {
                                if let Some(ref mut solution) = current_solution {
                                    solution
                                        .insert(name.clone(), RdfTerm::BlankNode(text_buf.clone()));
                                }
                            }
                        }
                        current_term = None;
                    }
                    b"literal" => {
                        if let Some(TermKind::Literal { datatype, language }) = current_term.clone()
                        {
                            if let Some(ref name) = current_binding_name {
                                if let Some(ref mut solution) = current_solution {
                                    solution.insert(
                                        name.clone(),
                                        RdfTerm::Literal {
                                            value: text_buf.clone(),
                                            datatype,
                                            language,
                                        },
                                    );
                                }
                            }
                        }
                        current_term = None;
                    }
                    b"boolean" => {
                        let val = text_buf.trim();
                        return Ok(SparqlResults::Boolean(val == "true" || val == "1"));
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(ref e)) => {
                if current_term.is_some() || in_boolean {
                    text_buf.push_str(
                        &e.unescape()
                            .context("Failed to unescape XML text content")?,
                    );
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => bail!("XML parse error: {e}"),
            _ => {}
        }
    }

    Ok(SparqlResults::Solutions {
        variables,
        solutions,
    })
}

// ---------------------------------------------------------------------------
// SPARQL Results JSON (.srj) parser
// ---------------------------------------------------------------------------

/// Parse SPARQL Results JSON format.
pub fn parse_srj(json: &str) -> Result<SparqlResults> {
    let value: serde_json::Value =
        serde_json::from_str(json).context("Invalid JSON in .srj file")?;

    // Check for ASK result
    if let Some(boolean) = value.get("boolean") {
        return Ok(SparqlResults::Boolean(
            boolean.as_bool().context("'boolean' field is not a bool")?,
        ));
    }

    // SELECT result
    let head = value.get("head").context("Missing 'head' in .srj")?;
    let vars = head
        .get("vars")
        .and_then(|v| v.as_array())
        .context("Missing 'head.vars' in .srj")?;
    let variables: Vec<String> = vars
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();

    let bindings = value
        .get("results")
        .and_then(|r| r.get("bindings"))
        .and_then(|b| b.as_array())
        .context("Missing 'results.bindings' in .srj")?;

    let solutions: Vec<HashMap<String, RdfTerm>> = bindings
        .iter()
        .map(|binding| {
            let mut solution = HashMap::new();
            if let Some(obj) = binding.as_object() {
                for (var_name, term_value) in obj {
                    if let Some(term) = parse_srj_term(term_value) {
                        solution.insert(var_name.clone(), term);
                    }
                }
            }
            solution
        })
        .collect();

    Ok(SparqlResults::Solutions {
        variables,
        solutions,
    })
}

/// Parse a single term from SPARQL JSON result format.
fn parse_srj_term(value: &serde_json::Value) -> Option<RdfTerm> {
    let obj = value.as_object()?;
    let term_type = obj.get("type")?.as_str()?;
    let val = obj.get("value")?.as_str()?;

    match term_type {
        "uri" => Some(RdfTerm::Iri(val.to_string())),
        "bnode" => Some(RdfTerm::BlankNode(val.to_string())),
        "literal" | "typed-literal" => {
            let datatype = obj
                .get("datatype")
                .and_then(|d| d.as_str())
                .map(String::from);
            let language = obj
                .get("xml:lang")
                .or_else(|| obj.get("lang"))
                .and_then(|l| l.as_str())
                .map(String::from);
            Some(RdfTerm::Literal {
                value: val.to_string(),
                datatype,
                language,
            })
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Convert Fluree SPARQL JSON output → SparqlResults
// ---------------------------------------------------------------------------

/// Convert Fluree's `to_sparql_json()` output into a [`SparqlResults`].
///
/// Fluree produces the W3C SPARQL Results JSON format, so we parse it
/// the same way we parse `.srj` files.
pub fn fluree_json_to_sparql_results(json: &serde_json::Value) -> Result<SparqlResults> {
    let json_str = serde_json::to_string(json)?;
    parse_srj(&json_str)
}

// ---------------------------------------------------------------------------
// Turtle / RDF graph parsing (CONSTRUCT expected results)
// ---------------------------------------------------------------------------

/// Parse a Turtle document into a [`SparqlResults::Graph`].
///
/// Used for `.ttl` and `.rdf` CONSTRUCT expected result files.
fn parse_ttl_graph(content: &str) -> Result<SparqlResults> {
    let mut sink = GraphCollectorSink::new();
    parse_turtle(content, &mut sink).context("Turtle parse error")?;
    let graph = sink.finish();

    let triples: Vec<Triple> = graph
        .iter()
        .map(|t| Triple {
            subject: ir_term_to_rdf_term(&t.s),
            predicate: ir_term_to_rdf_term(&t.p),
            object: ir_term_to_rdf_term(&t.o),
        })
        .collect();

    Ok(SparqlResults::Graph(triples))
}

/// Convert a `fluree_graph_ir::Term` to our local [`RdfTerm`].
fn ir_term_to_rdf_term(term: &IrTerm) -> RdfTerm {
    match term {
        IrTerm::Iri(iri) => RdfTerm::Iri(iri.to_string()),
        IrTerm::BlankNode(id) => RdfTerm::BlankNode(id.as_str().to_string()),
        IrTerm::Literal {
            value,
            datatype,
            language,
        } => {
            let dt_iri = datatype.as_iri();
            let datatype_opt = if datatype.is_xsd_string() {
                None
            } else {
                Some(dt_iri.to_string())
            };
            let language_opt = language.as_ref().map(|l| l.to_string());
            RdfTerm::Literal {
                value: value.lexical(),
                datatype: datatype_opt,
                language: language_opt,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Convert Fluree CONSTRUCT JSON-LD output → SparqlResults::Graph
// ---------------------------------------------------------------------------

/// Convert Fluree's CONSTRUCT JSON-LD output into a [`SparqlResults::Graph`].
///
/// Expects a JSON-LD `@graph` array (or a single node object). Each node has
/// `@id` as the subject; every other key is a predicate whose values are objects.
pub fn fluree_construct_to_sparql_results(json: &serde_json::Value) -> Result<SparqlResults> {
    let nodes = if let Some(graph) = json.get("@graph").and_then(|g| g.as_array()) {
        graph.clone()
    } else if json.is_array() {
        json.as_array().unwrap().clone()
    } else if json.is_object() {
        vec![json.clone()]
    } else {
        bail!("CONSTRUCT result is not a JSON-LD graph: {json}");
    };

    let mut triples = Vec::new();

    for node in &nodes {
        let obj = node
            .as_object()
            .context("CONSTRUCT graph node is not an object")?;

        let subject = match obj.get("@id").and_then(|v| v.as_str()) {
            Some(id) => match id.strip_prefix("_:") {
                Some(label) => RdfTerm::BlankNode(label.to_string()),
                None => RdfTerm::Iri(id.to_string()),
            },
            None => continue, // skip nodes without @id
        };

        for (key, value) in obj {
            if key == "@id" {
                continue;
            }

            if key == "@type" {
                let rdf_type =
                    RdfTerm::Iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type".to_string());
                let types = match value {
                    serde_json::Value::Array(arr) => arr.clone(),
                    other => vec![other.clone()],
                };
                for type_val in &types {
                    if let Some(t) = type_val.as_str() {
                        triples.push(Triple {
                            subject: subject.clone(),
                            predicate: rdf_type.clone(),
                            object: RdfTerm::Iri(t.to_string()),
                        });
                    }
                }
                continue;
            }

            let predicate = RdfTerm::Iri(key.clone());
            let values = match value {
                serde_json::Value::Array(arr) => arr.clone(),
                other => vec![other.clone()],
            };

            for val in &values {
                if let Some(term) = json_ld_value_to_rdf_term(val) {
                    triples.push(Triple {
                        subject: subject.clone(),
                        predicate: predicate.clone(),
                        object: term,
                    });
                }
            }
        }
    }

    Ok(SparqlResults::Graph(triples))
}

/// Convert a JSON-LD value node to an [`RdfTerm`].
///
/// Handles `{"@id": "..."}`, `{"@value": "...", "@type": "...", "@language": "..."}`,
/// and plain string/number values.
fn json_ld_value_to_rdf_term(val: &serde_json::Value) -> Option<RdfTerm> {
    if let Some(obj) = val.as_object() {
        // Node reference: {"@id": "http://..."}
        if let Some(id) = obj.get("@id").and_then(|v| v.as_str()) {
            return Some(match id.strip_prefix("_:") {
                Some(label) => RdfTerm::BlankNode(label.to_string()),
                None => RdfTerm::Iri(id.to_string()),
            });
        }

        // Value node: {"@value": "...", "@type"?: "...", "@language"?: "..."}
        if let Some(value_field) = obj.get("@value") {
            let lexical = match value_field {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                _ => return None,
            };
            let datatype = obj.get("@type").and_then(|v| v.as_str()).map(String::from);
            let language = obj
                .get("@language")
                .and_then(|v| v.as_str())
                .map(String::from);
            return Some(RdfTerm::Literal {
                value: lexical,
                datatype,
                language,
            });
        }

        None
    } else if let Some(s) = val.as_str() {
        // Plain string — treat as untyped literal
        Some(RdfTerm::Literal {
            value: s.to_string(),
            datatype: None,
            language: None,
        })
    } else if let Some(n) = val.as_i64() {
        Some(RdfTerm::Literal {
            value: n.to_string(),
            datatype: Some("http://www.w3.org/2001/XMLSchema#integer".to_string()),
            language: None,
        })
    } else if let Some(n) = val.as_f64() {
        Some(RdfTerm::Literal {
            value: n.to_string(),
            datatype: Some("http://www.w3.org/2001/XMLSchema#double".to_string()),
            language: None,
        })
    } else {
        val.as_bool().map(|b| RdfTerm::Literal {
            value: b.to_string(),
            datatype: Some("http://www.w3.org/2001/XMLSchema#boolean".to_string()),
            language: None,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_srx_select() {
        let xml = r#"<?xml version="1.0"?>
<sparql xmlns="http://www.w3.org/2005/sparql-results#">
  <head>
    <variable name="x"/>
    <variable name="y"/>
  </head>
  <results>
    <result>
      <binding name="x"><uri>http://example.org/a</uri></binding>
      <binding name="y"><literal>hello</literal></binding>
    </result>
    <result>
      <binding name="x"><bnode>b0</bnode></binding>
      <binding name="y"><literal datatype="http://www.w3.org/2001/XMLSchema#integer">42</literal></binding>
    </result>
  </results>
</sparql>"#;

        let result = parse_srx(xml).unwrap();
        match result {
            SparqlResults::Solutions {
                variables,
                solutions,
            } => {
                assert_eq!(variables, vec!["x", "y"]);
                assert_eq!(solutions.len(), 2);
                assert_eq!(
                    solutions[0]["x"],
                    RdfTerm::Iri("http://example.org/a".into())
                );
                assert_eq!(
                    solutions[0]["y"],
                    RdfTerm::Literal {
                        value: "hello".into(),
                        datatype: None,
                        language: None,
                    }
                );
                assert_eq!(solutions[1]["x"], RdfTerm::BlankNode("b0".into()));
            }
            _ => panic!("Expected Solutions"),
        }
    }

    #[test]
    fn test_parse_srx_boolean() {
        let xml = r#"<?xml version="1.0"?>
<sparql xmlns="http://www.w3.org/2005/sparql-results#">
  <head></head>
  <boolean>true</boolean>
</sparql>"#;

        let result = parse_srx(xml).unwrap();
        assert!(matches!(result, SparqlResults::Boolean(true)));
    }

    #[test]
    fn test_parse_srj_select() {
        let json = r#"{
  "head": { "vars": ["s", "name"] },
  "results": {
    "bindings": [
      { "s": { "type": "uri", "value": "http://example.org/alice" },
        "name": { "type": "literal", "value": "Alice" } }
    ]
  }
}"#;
        let result = parse_srj(json).unwrap();
        match result {
            SparqlResults::Solutions {
                variables,
                solutions,
            } => {
                assert_eq!(variables, vec!["s", "name"]);
                assert_eq!(solutions.len(), 1);
                assert_eq!(
                    solutions[0]["s"],
                    RdfTerm::Iri("http://example.org/alice".into())
                );
            }
            _ => panic!("Expected Solutions"),
        }
    }

    #[test]
    fn test_parse_srj_boolean() {
        let json = r#"{ "head": {}, "boolean": false }"#;
        let result = parse_srj(json).unwrap();
        assert!(matches!(result, SparqlResults::Boolean(false)));
    }
}
