//! Flake generation from triple templates
//!
//! This module provides `FlakeGenerator` for materializing triple templates
//! with variable bindings into concrete flakes.

use crate::error::{Result, TransactError};
use crate::ir::{TemplateTerm, TripleTemplate};
use crate::namespace::NS_FLUREE_LEDGER;
use crate::namespace::NamespaceRegistry;
use fluree_db_core::{Flake, FlakeMeta, FlakeValue, Sid};
use fluree_vocab::namespaces::{JSON_LD, XSD};
use fluree_db_query::{Batch, Binding};

// Well-known datatype SIDs (match fluree-db-core + fluree-db-query conventions)

fn dt_id() -> Sid {
    Sid::new(JSON_LD, "id")
}

fn dt_xsd(name: &str) -> Sid {
    Sid::new(XSD, name)
}

fn dt_fluree_ledger(name: &str) -> Sid {
    Sid::new(NS_FLUREE_LEDGER, name)
}

/// Generates flakes from triple templates
///
/// The generator materializes templates by substituting variable bindings with
/// concrete values. When a variable is unbound or poisoned (from OPTIONAL), the
/// entire flake is silently skipped rather than producing an error.
///
/// This follows SPARQL UPDATE semantics where unbound variables in templates
/// simply produce no output for that row of bindings.
pub struct FlakeGenerator<'a> {
    /// Transaction time for generated flakes
    t: i64,

    /// Namespace registry for encoding IRIs and skolemizing blank nodes
    ns_registry: &'a mut NamespaceRegistry,

    /// Transaction ID for blank node skolemization
    txn_id: String,
}

impl<'a> FlakeGenerator<'a> {
    /// Create a new flake generator
    ///
    /// # Arguments
    /// * `t` - Transaction time for generated flakes
    /// * `ns_registry` - Namespace registry for encoding IRIs
    /// * `txn_id` - Transaction ID for blank node skolemization
    pub fn new(t: i64, ns_registry: &'a mut NamespaceRegistry, txn_id: String) -> Self {
        Self {
            t,
            ns_registry,
            txn_id,
        }
    }

    /// Generate assertion flakes from insert templates
    pub fn generate_assertions(
        &mut self,
        templates: &[TripleTemplate],
        bindings: &Batch,
    ) -> Result<Vec<Flake>> {
        self.generate_flakes(templates, bindings, true)
    }

    /// Generate retraction flakes from delete templates
    pub fn generate_retractions(
        &mut self,
        templates: &[TripleTemplate],
        bindings: &Batch,
    ) -> Result<Vec<Flake>> {
        self.generate_flakes(templates, bindings, false)
    }

    /// Generate flakes from templates with given operation flag
    fn generate_flakes(
        &mut self,
        templates: &[TripleTemplate],
        bindings: &Batch,
        op: bool,
    ) -> Result<Vec<Flake>> {
        let mut flakes = Vec::new();

        // Row count semantics:
        // - INSERT without WHERE produces an "empty bindings" batch (0 vars, 0 rows). We still need
        //   to materialize templates once, so treat it as a single empty row.
        // - UPDATE/UPSERT where WHERE matches nothing produces an empty batch with a non-empty
        //   schema (vars present but 0 rows). In that case, there are **zero solution rows** and
        //   templates must produce **zero flakes** (no-op).
        let row_count = if bindings.is_empty() {
            if bindings.schema().is_empty() {
                1
            } else {
                0
            }
        } else {
            bindings.len()
        };

        for row_idx in 0..row_count {
            for template in templates {
                flakes.extend(self.materialize_template(template, bindings, row_idx, op)?);
            }
        }

        Ok(flakes)
    }

    /// Materialize a single template with bindings into a flake
    fn materialize_template(
        &mut self,
        template: &TripleTemplate,
        bindings: &Batch,
        row_idx: usize,
        op: bool,
    ) -> Result<Option<Flake>> {
        // Resolve each component
        let s = self.resolve_subject(&template.subject, bindings, row_idx)?;
        let p = self.resolve_predicate(&template.predicate, bindings, row_idx)?;
        let (o, dt) =
            self.resolve_object(&template.object, &template.datatype, bindings, row_idx)?;

        let bound_lang = match &template.object {
            TemplateTerm::Var(var_id) => match bindings.get(row_idx, *var_id) {
                Some(Binding::Lit { lang: Some(lang), .. }) => Some(lang.to_string()),
                _ => None,
            },
            _ => None,
        };

        // Language-tagged literals use rdf:langString datatype (Clojure parity).
        let dt = if template.language.is_some() || bound_lang.is_some() {
            dt.map(|_| Sid::new(3, "langString"))
        } else {
            dt
        };

        // If any component is None (unbound variable), skip this flake
        let (s, p, o, dt) = match (s, p, o, dt) {
            (Some(s), Some(p), Some(o), Some(dt)) => (s, p, o, dt),
            _ => return Ok(None),
        };

        // Create metadata if language tag or list_index is present
        let meta_lang = template.language.clone().or(bound_lang);
        let meta = match (&meta_lang, &template.list_index) {
            (Some(lang), Some(idx)) => {
                // Both language and list_index
                Some(FlakeMeta {
                    lang: Some(lang.clone()),
                    i: Some(*idx),
                })
            }
            (Some(lang), None) => Some(FlakeMeta::with_lang(lang)),
            (None, Some(idx)) => Some(FlakeMeta::with_index(*idx)),
            (None, None) => None,
        };

        Ok(Some(Flake::new(s, p, o, dt, self.t, op, meta)))
    }

    /// Resolve a subject term
    fn resolve_subject(
        &mut self,
        term: &TemplateTerm,
        bindings: &Batch,
        row: usize,
    ) -> Result<Option<Sid>> {
        match term {
            TemplateTerm::Sid(sid) => Ok(Some(sid.clone())),
            TemplateTerm::Var(var_id) => {
                if bindings.is_empty() {
                    return Err(TransactError::UnboundVariable(format!("var_{:?}", var_id)));
                }
                if let Some(binding) = bindings.get(row, *var_id) {
                    match binding {
                        Binding::Sid(sid) => Ok(Some(sid.clone())),
                        Binding::IriMatch { primary_sid, .. } => Ok(Some(primary_sid.clone())),
                        Binding::Unbound | Binding::Poisoned => Ok(None),
                        Binding::Grouped(_) => Err(TransactError::InvalidTerm(
                            "Subject cannot be a grouped value (GROUP BY output)".to_string(),
                        )),
                        Binding::Lit { .. } => Err(TransactError::InvalidTerm(
                            "Subject must be a Sid, not a literal".to_string(),
                        )),
                        Binding::Iri(_) => Err(TransactError::InvalidTerm(
                            "Raw IRI from virtual graph cannot be used as subject for flake generation".to_string(),
                        )),
                    }
                } else {
                    Ok(None)
                }
            }
            TemplateTerm::BlankNode(label) => {
                let sid = self.skolemize_blank_node(label);
                Ok(Some(sid))
            }
            TemplateTerm::Value(_) => Err(TransactError::InvalidTerm(
                "Subject cannot be a literal value".to_string(),
            )),
        }
    }

    /// Resolve a predicate term
    fn resolve_predicate(
        &mut self,
        term: &TemplateTerm,
        bindings: &Batch,
        row: usize,
    ) -> Result<Option<Sid>> {
        match term {
            TemplateTerm::Sid(sid) => Ok(Some(sid.clone())),
            TemplateTerm::Var(var_id) => {
                if bindings.is_empty() {
                    return Err(TransactError::UnboundVariable(format!("var_{:?}", var_id)));
                }
                if let Some(binding) = bindings.get(row, *var_id) {
                    match binding {
                        Binding::Sid(sid) => Ok(Some(sid.clone())),
                        Binding::IriMatch { primary_sid, .. } => Ok(Some(primary_sid.clone())),
                        Binding::Unbound | Binding::Poisoned => Ok(None),
                        Binding::Grouped(_) => Err(TransactError::InvalidTerm(
                            "Predicate cannot be a grouped value (GROUP BY output)".to_string(),
                        )),
                        Binding::Lit { .. } => Err(TransactError::InvalidTerm(
                            "Predicate must be a Sid, not a literal".to_string(),
                        )),
                        Binding::Iri(_) => Err(TransactError::InvalidTerm(
                            "Raw IRI from virtual graph cannot be used as predicate for flake generation".to_string(),
                        )),
                    }
                } else {
                    Ok(None)
                }
            }
            TemplateTerm::BlankNode(_) => Err(TransactError::InvalidTerm(
                "Predicate cannot be a blank node".to_string(),
            )),
            TemplateTerm::Value(_) => Err(TransactError::InvalidTerm(
                "Predicate cannot be a literal value".to_string(),
            )),
        }
    }

    /// Resolve an object term
    fn resolve_object(
        &mut self,
        term: &TemplateTerm,
        explicit_dt: &Option<Sid>,
        bindings: &Batch,
        row: usize,
    ) -> Result<(Option<FlakeValue>, Option<Sid>)> {
        match term {
            TemplateTerm::Sid(sid) => {
                // Reference type
                Ok((Some(FlakeValue::Ref(sid.clone())), Some(dt_id())))
            }
            TemplateTerm::Value(val) => {
                let dt = explicit_dt
                    .clone()
                    .unwrap_or_else(|| infer_datatype(val));
                Ok((Some(val.clone()), Some(dt)))
            }
            TemplateTerm::Var(var_id) => {
                if bindings.is_empty() {
                    return Err(TransactError::UnboundVariable(format!("var_{:?}", var_id)));
                }
                if let Some(binding) = bindings.get(row, *var_id) {
                    match binding {
                        Binding::Sid(sid) => {
                            Ok((Some(FlakeValue::Ref(sid.clone())), Some(dt_id())))
                        }
                        Binding::IriMatch { primary_sid, .. } => {
                            Ok((Some(FlakeValue::Ref(primary_sid.clone())), Some(dt_id())))
                        }
                        Binding::Lit { val, dt, .. } => Ok((Some(val.clone()), Some(dt.clone()))),
                        Binding::Unbound | Binding::Poisoned => Ok((None, None)),
                        Binding::Grouped(_) => Err(TransactError::InvalidTerm(
                            "Object cannot be a grouped value (GROUP BY output)".to_string(),
                        )),
                        Binding::Iri(_) => Err(TransactError::InvalidTerm(
                            "Raw IRI from virtual graph cannot be used as object for flake generation".to_string(),
                        )),
                    }
                } else {
                    Ok((None, None))
                }
            }
            TemplateTerm::BlankNode(label) => {
                let sid = self.skolemize_blank_node(label);
                Ok((Some(FlakeValue::Ref(sid)), Some(dt_id())))
            }
        }
    }

    /// Skolemize a blank node to a Sid
    ///
    /// Creates a unique Sid for a blank node label within this transaction.
    /// The format is: `_:fdb-{txn_id}-{label}` where label is the user's
    /// blank node identifier stripped of the `_:` prefix.
    fn skolemize_blank_node(&mut self, label: &str) -> Sid {
        let local = label.trim_start_matches("_:");
        // Combine txn_id and local label to create a unique ID
        let unique_id = format!("{}-{}", self.txn_id, local);
        self.ns_registry.blank_node_sid(&unique_id)
    }
}

/// Infer datatype from a FlakeValue
///
/// Returns the appropriate XSD/RDF datatype Sid for the given value.
/// This is used both for flake generation and for VALUES clause binding conversion.
pub fn infer_datatype(val: &FlakeValue) -> Sid {
    match val {
        FlakeValue::Ref(_) => dt_id(),
        FlakeValue::Boolean(_) => dt_xsd("boolean"),
        // JSON-LD default for integral numbers is xsd:integer.
        // Preserve explicit xsd:long only when the source literal is explicitly typed.
        FlakeValue::Long(_) => dt_xsd("integer"),
        FlakeValue::Double(_) => dt_xsd("double"),
        FlakeValue::BigInt(_) => dt_xsd("integer"),
        FlakeValue::Decimal(_) => dt_xsd("decimal"),
        FlakeValue::DateTime(_) => dt_xsd("dateTime"),
        FlakeValue::Date(_) => dt_xsd("date"),
        FlakeValue::Time(_) => dt_xsd("time"),
        FlakeValue::String(_) => dt_xsd("string"),
        FlakeValue::Json(_) => Sid::new(3, "JSON"), // rdf:JSON (namespace code 3 = RDF)
        FlakeValue::Vector(_) => dt_fluree_ledger("vector"),
        // Null isn't a standard RDF literal; treat as xsd:string for now (MVP).
        FlakeValue::Null => dt_xsd("string"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_query::VarId;
    use std::sync::Arc;

    fn make_empty_batch() -> Batch {
        let schema: Arc<[VarId]> = Arc::new([]);
        Batch::empty(schema).unwrap()
    }

    #[test]
    fn test_generate_simple_assertion() {
        let mut registry = NamespaceRegistry::new();
        let mut generator = FlakeGenerator::new(1, &mut registry, "txn1".to_string());

        let templates = vec![TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        )];

        let batch = make_empty_batch();
        let flakes = generator.generate_assertions(&templates, &batch).unwrap();

        assert_eq!(flakes.len(), 1);
        assert!(flakes[0].op);
        assert_eq!(flakes[0].s.name.as_ref(), "ex:alice");
        assert_eq!(flakes[0].p.name.as_ref(), "ex:name");
    }

    #[test]
    fn test_generate_retraction() {
        let mut registry = NamespaceRegistry::new();
        let mut generator = FlakeGenerator::new(1, &mut registry, "txn1".to_string());

        let templates = vec![TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        )];

        let batch = make_empty_batch();
        let flakes = generator.generate_retractions(&templates, &batch).unwrap();

        assert_eq!(flakes.len(), 1);
        assert!(!flakes[0].op);
    }

    #[test]
    fn test_blank_node_skolemization() {
        let mut registry = NamespaceRegistry::new();
        let mut generator = FlakeGenerator::new(1, &mut registry, "txn123".to_string());

        let templates = vec![TripleTemplate::new(
            TemplateTerm::BlankNode("_:b1".to_string()),
            TemplateTerm::Sid(Sid::new(1, "ex:name")),
            TemplateTerm::Value(FlakeValue::String("Test".to_string())),
        )];

        let batch = make_empty_batch();
        let flakes = generator.generate_assertions(&templates, &batch).unwrap();

        assert_eq!(flakes.len(), 1);
        // Check that the blank node was skolemized
        assert!(flakes[0].s.name.contains("b1"));
    }

    #[test]
    fn test_infer_datatype() {
        assert_eq!(
            infer_datatype(&FlakeValue::Long(42)).name.as_ref(),
            "integer"
        );
        assert_eq!(infer_datatype(&FlakeValue::Double(3.14)).name.as_ref(), "double");
        assert_eq!(
            infer_datatype(&FlakeValue::String("test".to_string())).name.as_ref(),
            "string"
        );
        assert_eq!(infer_datatype(&FlakeValue::Boolean(true)).name.as_ref(), "boolean");
    }

    #[test]
    fn test_generate_with_list_index() {
        let mut registry = NamespaceRegistry::new();
        let mut generator = FlakeGenerator::new(1, &mut registry, "txn1".to_string());

        // Create templates with list indices
        let templates = vec![
            TripleTemplate::new(
                TemplateTerm::Sid(Sid::new(1, "ex:alice")),
                TemplateTerm::Sid(Sid::new(1, "ex:colors")),
                TemplateTerm::Value(FlakeValue::String("red".to_string())),
            )
            .with_list_index(0),
            TripleTemplate::new(
                TemplateTerm::Sid(Sid::new(1, "ex:alice")),
                TemplateTerm::Sid(Sid::new(1, "ex:colors")),
                TemplateTerm::Value(FlakeValue::String("green".to_string())),
            )
            .with_list_index(1),
            TripleTemplate::new(
                TemplateTerm::Sid(Sid::new(1, "ex:alice")),
                TemplateTerm::Sid(Sid::new(1, "ex:colors")),
                TemplateTerm::Value(FlakeValue::String("blue".to_string())),
            )
            .with_list_index(2),
        ];

        let batch = make_empty_batch();
        let flakes = generator.generate_assertions(&templates, &batch).unwrap();

        assert_eq!(flakes.len(), 3);

        // Check list indices are preserved in metadata
        assert_eq!(flakes[0].m.as_ref().and_then(|m| m.i), Some(0));
        assert_eq!(flakes[1].m.as_ref().and_then(|m| m.i), Some(1));
        assert_eq!(flakes[2].m.as_ref().and_then(|m| m.i), Some(2));

        // Check values
        assert_eq!(flakes[0].o, FlakeValue::String("red".to_string()));
        assert_eq!(flakes[1].o, FlakeValue::String("green".to_string()));
        assert_eq!(flakes[2].o, FlakeValue::String("blue".to_string()));
    }

    #[test]
    fn test_generate_with_language_and_list_index() {
        let mut registry = NamespaceRegistry::new();
        let mut generator = FlakeGenerator::new(1, &mut registry, "txn1".to_string());

        // Create template with both language and list_index
        let templates = vec![TripleTemplate::new(
            TemplateTerm::Sid(Sid::new(1, "ex:alice")),
            TemplateTerm::Sid(Sid::new(1, "ex:names")),
            TemplateTerm::Value(FlakeValue::String("Alice".to_string())),
        )
        .with_language("en")
        .with_list_index(0)];

        let batch = make_empty_batch();
        let flakes = generator.generate_assertions(&templates, &batch).unwrap();

        assert_eq!(flakes.len(), 1);

        // Check both language and list_index are present in metadata
        let meta = flakes[0].m.as_ref().expect("should have metadata");
        assert_eq!(meta.lang.as_deref(), Some("en"));
        assert_eq!(meta.i, Some(0));
    }
}
