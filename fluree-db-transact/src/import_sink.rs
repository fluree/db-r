//! ImportSink — a `GraphSink` that streams triples directly to a `StreamingCommitWriter`.
//!
//! Like [`FlakeSink`](crate::flake_sink::FlakeSink), this resolves parser events
//! (IRIs, blank nodes, literals) into `Flake` components. But instead of
//! accumulating `Vec<Flake>`, each flake is immediately pushed to a
//! [`StreamingCommitWriter`](crate::commit_v2::StreamingCommitWriter) which
//! encodes the op and spools it through zstd compression to a tempfile.
//!
//! This eliminates the intermediate flake buffer entirely, keeping memory
//! bounded regardless of how large the input is.

mod inner {
    use crate::commit_v2::StreamingCommitWriter;
    use crate::generate::{infer_datatype, DT_ID, DT_JSON, DT_LANG_STRING};
    use crate::namespace::NamespaceRegistry;
    use crate::value_convert::{convert_native_literal, convert_string_literal};
    use fluree_db_core::{Flake, FlakeMeta, FlakeValue, Sid};
    use fluree_db_novelty::commit_v2::CommitV2Error;
    use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, TermId};
    use std::collections::HashMap;

    // -----------------------------------------------------------------------
    // ResolvedTerm — local term representation (same shape as FlakeSink's)
    // -----------------------------------------------------------------------

    /// A term resolved to its Flake-ready form.
    enum ResolvedTerm {
        /// IRI or blank node (already resolved to a Sid)
        Sid(Sid),
        /// Literal value with datatype and optional language
        Literal {
            value: FlakeValue,
            dt_sid: Sid,
            language: Option<String>,
        },
    }

    // -----------------------------------------------------------------------
    // ImportSink
    // -----------------------------------------------------------------------

    /// A `GraphSink` that streams parsed triples directly to a commit-v2 writer.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut sink = ImportSink::new(&mut ns, t, txn_id, true)?;
    /// fluree_graph_turtle::parse(ttl, &mut sink)?;
    /// let writer = sink.finish();
    /// let result = writer.finish(&envelope)?;
    /// ```
    pub struct ImportSink<'a> {
        terms: Vec<ResolvedTerm>,
        blank_labels: HashMap<String, TermId>,
        blank_counter: u32,
        ns_registry: &'a mut NamespaceRegistry,
        t: i64,
        txn_id: String,
        writer: StreamingCommitWriter,
        /// First encoding error encountered (checked after parse).
        encode_error: Option<CommitV2Error>,
    }

    impl<'a> ImportSink<'a> {
        /// Create a new ImportSink.
        ///
        /// # Arguments
        /// * `ns_registry` — namespace registry (seeded from predefined codes)
        /// * `t` — transaction time
        /// * `txn_id` — unique ID for blank node skolemization
        /// * `compress` — whether to zstd-compress the ops stream
        pub fn new(
            ns_registry: &'a mut NamespaceRegistry,
            t: i64,
            txn_id: String,
            compress: bool,
        ) -> Result<Self, CommitV2Error> {
            Ok(Self {
                terms: Vec::new(),
                blank_labels: HashMap::new(),
                blank_counter: 0,
                ns_registry,
                t,
                txn_id,
                writer: StreamingCommitWriter::new(compress)?,
                encode_error: None,
            })
        }

        /// Consume the sink and return the writer for finalization.
        ///
        /// Returns an error if any flake failed to encode during parsing.
        pub fn finish(self) -> Result<StreamingCommitWriter, CommitV2Error> {
            if let Some(err) = self.encode_error {
                return Err(err);
            }
            Ok(self.writer)
        }

        // -- helpers ---------------------------------------------------------

        fn add_term(&mut self, term: ResolvedTerm) -> TermId {
            let id = TermId::new(self.terms.len() as u32);
            self.terms.push(term);
            id
        }

        fn skolemize(&mut self, local: &str) -> Sid {
            let unique_id = format!("{}-{}", self.txn_id, local);
            self.ns_registry.blank_node_sid(&unique_id)
        }

        fn resolve_sid(&self, id: TermId) -> Option<Sid> {
            match &self.terms[id.index() as usize] {
                ResolvedTerm::Sid(sid) => Some(sid.clone()),
                ResolvedTerm::Literal { .. } => None,
            }
        }

        fn resolve_object(&self, id: TermId) -> Option<(FlakeValue, Sid, Option<String>)> {
            match &self.terms[id.index() as usize] {
                ResolvedTerm::Sid(sid) => Some((FlakeValue::Ref(sid.clone()), DT_ID.clone(), None)),
                ResolvedTerm::Literal {
                    value,
                    dt_sid,
                    language,
                } => Some((value.clone(), dt_sid.clone(), language.clone())),
            }
        }

        fn push_triple(
            &mut self,
            subject: TermId,
            predicate: TermId,
            object: TermId,
            list_index: Option<i32>,
        ) {
            let Some(s) = self.resolve_sid(subject) else {
                return;
            };
            let Some(p) = self.resolve_sid(predicate) else {
                return;
            };
            let Some((o, mut dt, lang)) = self.resolve_object(object) else {
                return;
            };

            if lang.is_some() {
                dt = DT_LANG_STRING.clone();
            }

            let meta = match (&lang, list_index) {
                (Some(l), Some(i)) => Some(FlakeMeta {
                    lang: Some(l.clone()),
                    i: Some(i),
                }),
                (Some(l), None) => Some(FlakeMeta::with_lang(l)),
                (None, Some(i)) => Some(FlakeMeta::with_index(i)),
                (None, None) => None,
            };

            let flake = Flake::new(s, p, o, dt, self.t, true, meta);
            if let Err(e) = self.writer.push_flake(&flake) {
                if self.encode_error.is_none() {
                    tracing::error!("ImportSink: flake encode failed: {}", e);
                    self.encode_error = Some(e);
                }
            }
        }
    }

    impl<'a> GraphSink for ImportSink<'a> {
        fn on_base(&mut self, _base_iri: &str) {
            // No-op — parser resolves relative IRIs before calling term_iri
        }

        fn on_prefix(&mut self, _prefix: &str, namespace_iri: &str) {
            self.ns_registry.get_or_allocate(namespace_iri);
        }

        fn term_iri(&mut self, iri: &str) -> TermId {
            let sid = self.ns_registry.sid_for_iri(iri);
            self.add_term(ResolvedTerm::Sid(sid))
        }

        fn term_blank(&mut self, label: Option<&str>) -> TermId {
            match label {
                Some(l) => {
                    if let Some(&id) = self.blank_labels.get(l) {
                        return id;
                    }
                    let sid = self.skolemize(l);
                    let id = self.add_term(ResolvedTerm::Sid(sid));
                    self.blank_labels.insert(l.to_string(), id);
                    id
                }
                None => {
                    self.blank_counter += 1;
                    let label = format!("b{}", self.blank_counter);
                    let sid = self.skolemize(&label);
                    self.add_term(ResolvedTerm::Sid(sid))
                }
            }
        }

        fn term_literal(
            &mut self,
            value: &str,
            datatype: Datatype,
            language: Option<&str>,
        ) -> TermId {
            let lang = language.map(|s| s.to_string());
            let dt_iri = datatype.as_iri();
            let (flake_value, dt_sid) = convert_string_literal(value, dt_iri, self.ns_registry);

            let dt_sid = if lang.is_some() {
                DT_LANG_STRING.clone()
            } else {
                dt_sid
            };

            self.add_term(ResolvedTerm::Literal {
                value: flake_value,
                dt_sid,
                language: lang,
            })
        }

        fn term_literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
            let flake_value = convert_native_literal(&value);
            let dt_sid = if datatype.is_json() {
                DT_JSON.clone()
            } else {
                infer_datatype(&flake_value)
            };

            self.add_term(ResolvedTerm::Literal {
                value: flake_value,
                dt_sid,
                language: None,
            })
        }

        fn emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId) {
            self.push_triple(subject, predicate, object, None);
        }

        fn emit_list_item(
            &mut self,
            subject: TermId,
            predicate: TermId,
            object: TermId,
            index: i32,
        ) {
            self.push_triple(subject, predicate, object, Some(index));
        }
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[cfg(test)]
    mod tests {
        use super::*;
        use fluree_db_novelty::commit_v2::read_commit;

        fn make_sink_and_parse(
            ns: &mut NamespaceRegistry,
            t: i64,
        ) -> Result<ImportSink<'_>, CommitV2Error> {
            ImportSink::new(ns, t, "test-txn".to_string(), true)
        }

        fn make_envelope(t: i64) -> crate::commit_v2::CommitV2Envelope {
            crate::commit_v2::CommitV2Envelope {
                t,
                previous_ref: None,
                namespace_delta: HashMap::new(),
                txn: None,
                time: None,
                txn_signature: None,
                txn_meta: Vec::new(),
                graph_delta: HashMap::new(),
            }
        }

        #[test]
        fn test_basic_iri_triple() {
            let mut ns = NamespaceRegistry::new();
            let mut sink = make_sink_and_parse(&mut ns, 1).unwrap();

            let s = sink.term_iri("http://example.org/alice");
            let p = sink.term_iri("http://example.org/name");
            let o = sink.term_literal("Alice", Datatype::xsd_string(), None);
            sink.emit_triple(s, p, o);

            let writer = sink.finish().unwrap();
            assert_eq!(writer.op_count(), 1);

            let result = writer.finish(&make_envelope(1)).unwrap();
            let decoded = read_commit(&result.bytes).unwrap();
            assert_eq!(decoded.flakes.len(), 1);
            assert!(decoded.flakes[0].op);
            assert!(matches!(&decoded.flakes[0].o, FlakeValue::String(s) if s == "Alice"));
        }

        #[test]
        fn test_all_value_types() {
            let mut ns = NamespaceRegistry::new();
            let mut sink = make_sink_and_parse(&mut ns, 1).unwrap();

            let s = sink.term_iri("http://example.org/x");

            // String
            let p = sink.term_iri("http://example.org/str");
            let o = sink.term_literal("hello", Datatype::xsd_string(), None);
            sink.emit_triple(s, p, o);

            // Integer
            let p = sink.term_iri("http://example.org/num");
            let o = sink.term_literal_value(LiteralValue::Integer(42), Datatype::xsd_integer());
            sink.emit_triple(s, p, o);

            // Double
            let p = sink.term_iri("http://example.org/dbl");
            let o = sink.term_literal_value(LiteralValue::Double(3.13), Datatype::xsd_double());
            sink.emit_triple(s, p, o);

            // Boolean
            let p = sink.term_iri("http://example.org/flag");
            let o = sink.term_literal_value(LiteralValue::Boolean(true), Datatype::xsd_boolean());
            sink.emit_triple(s, p, o);

            // Ref (IRI in object position)
            let p = sink.term_iri("http://example.org/knows");
            let o = sink.term_iri("http://example.org/bob");
            sink.emit_triple(s, p, o);

            let writer = sink.finish().unwrap();
            assert_eq!(writer.op_count(), 5);

            let result = writer.finish(&make_envelope(1)).unwrap();
            let decoded = read_commit(&result.bytes).unwrap();
            assert_eq!(decoded.flakes.len(), 5);
            assert!(matches!(&decoded.flakes[0].o, FlakeValue::String(s) if s == "hello"));
            assert!(matches!(&decoded.flakes[1].o, FlakeValue::Long(42)));
            assert!(matches!(&decoded.flakes[2].o, FlakeValue::Double(_)));
            assert!(matches!(&decoded.flakes[3].o, FlakeValue::Boolean(true)));
            assert!(matches!(&decoded.flakes[4].o, FlakeValue::Ref(_)));
        }

        #[test]
        fn test_language_tagged_literal() {
            let mut ns = NamespaceRegistry::new();
            let mut sink = make_sink_and_parse(&mut ns, 1).unwrap();

            let s = sink.term_iri("http://example.org/alice");
            let p = sink.term_iri("http://example.org/name");
            let o = sink.term_literal("Alice", Datatype::rdf_lang_string(), Some("en"));
            sink.emit_triple(s, p, o);

            let writer = sink.finish().unwrap();
            let result = writer.finish(&make_envelope(1)).unwrap();
            let decoded = read_commit(&result.bytes).unwrap();

            assert_eq!(decoded.flakes.len(), 1);
            let f = &decoded.flakes[0];
            assert_eq!(f.dt, Sid::new(fluree_vocab::namespaces::RDF, "langString"));
            let meta = f.m.as_ref().expect("should have meta");
            assert_eq!(meta.lang.as_deref(), Some("en"));
        }

        #[test]
        fn test_blank_node_consistency() {
            let mut ns = NamespaceRegistry::new();
            let mut sink = make_sink_and_parse(&mut ns, 1).unwrap();

            let b1 = sink.term_blank(Some("foo"));
            let b2 = sink.term_blank(Some("foo"));
            let b3 = sink.term_blank(Some("bar"));
            let b4 = sink.term_blank(None);

            assert_eq!(b1, b2);
            assert_ne!(b1, b3);
            assert_ne!(b3, b4);
        }

        #[test]
        fn test_list_items() {
            let mut ns = NamespaceRegistry::new();
            let mut sink = make_sink_and_parse(&mut ns, 1).unwrap();

            let s = sink.term_iri("http://example.org/alice");
            let p = sink.term_iri("http://example.org/scores");
            let o0 = sink.term_literal_value(LiteralValue::Integer(10), Datatype::xsd_integer());
            let o1 = sink.term_literal_value(LiteralValue::Integer(20), Datatype::xsd_integer());
            let o2 = sink.term_literal_value(LiteralValue::Integer(30), Datatype::xsd_integer());
            sink.emit_list_item(s, p, o0, 0);
            sink.emit_list_item(s, p, o1, 1);
            sink.emit_list_item(s, p, o2, 2);

            let writer = sink.finish().unwrap();
            assert_eq!(writer.op_count(), 3);

            let result = writer.finish(&make_envelope(1)).unwrap();
            let decoded = read_commit(&result.bytes).unwrap();
            assert_eq!(decoded.flakes.len(), 3);
            for (i, f) in decoded.flakes.iter().enumerate() {
                let meta = f.m.as_ref().expect("list items should have meta");
                assert_eq!(meta.i, Some(i as i32));
            }
        }
    }
}

pub use inner::ImportSink;
