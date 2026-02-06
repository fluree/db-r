//! Import mode: parse TTL/TriG → stream to commit-v2 blob → store.
//!
//! Bypasses the full staging/novelty pipeline for bulk import of clean
//! RDF data. No WHERE evaluation, no cancellation, no policy enforcement,
//! no novelty index merge. Duplicate facts within a chunk are written as-is;
//! dedup happens during indexing or at query time.
//!
//! # Supported Formats
//!
//! - **Turtle**: Default graph triples only
//! - **TriG**: Default graph + named GRAPH blocks with g_id allocation
//!
//! See the Phase 3 plan for full semantics documentation.

mod inner {
    use crate::commit_v2::CommitV2Envelope;
    use crate::commit_v2::StreamingCommitWriter;
    use crate::error::{Result, TransactError};
    use crate::generate::{infer_datatype, DT_ID, DT_JSON, DT_LANG_STRING};
    use crate::import_sink::ImportSink;
    use crate::namespace::NamespaceRegistry;
    use crate::parse::trig_meta::{parse_trig_phase1, resolve_trig_meta, NamedGraphBlock, RawObject, RawTerm};
    use crate::value_convert::{convert_native_literal, convert_string_literal};
    use fluree_db_core::{ContentAddressedWrite, ContentKind, Flake, FlakeMeta, FlakeValue, Sid};
    use fluree_db_novelty::CommitRef;
    use std::collections::HashMap;

    /// Mutable state carried across chunks during an import session.
    pub struct ImportState {
        /// Current transaction number. Starts at 0; first chunk produces t=1.
        pub t: i64,
        /// Reference to the previous commit (address + id).
        pub previous_ref: Option<CommitRef>,
        /// Namespace registry (accumulates across chunks).
        pub ns_registry: NamespaceRegistry,
        /// Cumulative flake count across all commits (for progress reporting).
        pub cumulative_flakes: u64,
        /// Import start time (reused for all commits to avoid per-chunk Utc::now).
        pub import_time: String,
        /// Named graph IRI → g_id mapping (stable across chunks).
        /// g_id 0 = default graph, g_id 1 = txn-meta, g_id 2+ = user-defined.
        pub graph_ids: HashMap<String, u32>,
        /// Next available g_id for user-defined named graphs.
        pub next_gid: u32,
    }

    impl ImportState {
        /// Create a new import state for a fresh ledger.
        ///
        /// `NamespaceRegistry::new()` includes all predefined namespace codes
        /// (rdf, xsd, etc). User-defined namespaces are added as chunks are parsed.
        pub fn new() -> Self {
            Self {
                t: 0,
                previous_ref: None,
                ns_registry: NamespaceRegistry::new(),
                cumulative_flakes: 0,
                import_time: chrono::Utc::now().to_rfc3339(),
                graph_ids: HashMap::new(),
                next_gid: 2, // 0=default, 1=txn-meta
            }
        }
    }

    impl Default for ImportState {
        fn default() -> Self {
            Self::new()
        }
    }

    /// Result of importing a single TTL chunk.
    pub struct ImportCommitResult {
        /// Storage address of the committed blob.
        pub address: String,
        /// Content ID (e.g. "sha256:abcd...").
        pub commit_id: String,
        /// Transaction number.
        pub t: i64,
        /// Number of flakes in this commit.
        pub flake_count: u32,
        /// Size of the committed blob in bytes.
        pub blob_bytes: usize,
        /// The raw commit blob (moved, not cloned — zero extra cost).
        /// Available for downstream consumers (e.g., run generation)
        /// without re-reading from storage.
        pub commit_blob: Vec<u8>,
    }

    /// Import a single TTL chunk as a v2 commit blob.
    ///
    /// Parses the Turtle input, streams flakes through the commit-v2 writer,
    /// and stores the resulting blob. Advances `state` for the next chunk.
    ///
    /// # Arguments
    /// * `state` — mutable import state (carried across chunks)
    /// * `ttl` — Turtle input text
    /// * `storage` — storage backend for writing commit blobs
    /// * `ledger_alias` — ledger name for storage path construction
    /// * `compress` — whether to zstd-compress the ops stream
    pub async fn import_commit<S>(
        state: &mut ImportState,
        ttl: &str,
        storage: &S,
        ledger_alias: &str,
        compress: bool,
    ) -> Result<ImportCommitResult>
    where
        S: ContentAddressedWrite,
    {
        let new_t = state.t + 1;
        let txn_id = format!("{}-{}", ledger_alias, new_t);

        // 1. Create ImportSink + parse TTL
        let ns_codes_before = state.ns_registry.code_count();
        let _parse_span = tracing::info_span!(
            "import_parse",
            t = new_t,
            ttl_bytes = ttl.len(),
            ns_codes = ns_codes_before,
        )
        .entered();
        let mut sink = ImportSink::new(&mut state.ns_registry, new_t, txn_id, compress)
            .map_err(|e| TransactError::Parse(format!("failed to create import sink: {}", e)))?;
        fluree_graph_turtle::parse(ttl, &mut sink)
            .map_err(|e| TransactError::Parse(e.to_string()))?;
        drop(_parse_span);

        // 2. Retrieve writer, get namespace delta, build envelope
        let (writer, op_count, envelope) = {
            let _span = tracing::info_span!("import_build_envelope", t = new_t).entered();
            let writer = sink
                .finish()
                .map_err(|e| TransactError::Parse(format!("flake encode error: {}", e)))?;
            let op_count = writer.op_count();
            let ns_delta = state.ns_registry.take_delta();
            let ns_codes_after = state.ns_registry.code_count();

            tracing::info!(
                op_count,
                ns_delta_size = ns_delta.len(),
                ns_codes = ns_codes_after,
                "import sink finalized"
            );

            // 3. Update cumulative flake count
            state.cumulative_flakes += op_count as u64;

            let envelope = CommitV2Envelope {
                t: new_t,
                v: 2,
                previous_ref: state.previous_ref.clone(),
                namespace_delta: ns_delta,
                txn: None,
                time: Some(state.import_time.clone()),
                data: None, // DB stats not maintained during import
                index: None,
                txn_signature: None,
                txn_meta: Vec::new(),
                graph_delta: HashMap::new(),
            };

            (writer, op_count, envelope)
        };

        // 4. Finalize blob
        let result = {
            let _span = tracing::info_span!("import_finish_blob", t = new_t, op_count).entered();
            writer.finish(&envelope)?
        };
        let commit_id = format!("sha256:{}", &result.content_hash_hex);
        let blob_bytes = result.bytes.len();

        // 5. Store
        let write_res = {
            let _span = tracing::info_span!("import_store", t = new_t, blob_bytes).entered();
            storage
                .content_write_bytes_with_hash(
                    ContentKind::Commit,
                    ledger_alias,
                    &result.content_hash_hex,
                    &result.bytes,
                )
                .await?
        };

        tracing::info!(
            t = new_t,
            flakes = op_count,
            blob_bytes,
            address = %write_res.address,
            "import commit stored"
        );

        // 8. Advance state
        state.t = new_t;
        state.previous_ref = Some(
            CommitRef::new(&write_res.address).with_id(format!("fluree:commit:{}", commit_id)),
        );

        Ok(ImportCommitResult {
            address: write_res.address,
            commit_id,
            t: new_t,
            flake_count: op_count,
            blob_bytes,
            commit_blob: result.bytes,
        })
    }

    /// Import a single TriG chunk as a v2 commit blob (with named graph support).
    ///
    /// Like `import_commit`, but supports TriG format with GRAPH blocks for named graphs.
    /// If the input contains GRAPH blocks, they are processed and stored with appropriate
    /// graph IDs, and `graph_delta` is populated in the commit envelope.
    ///
    /// # Named Graph ID Allocation
    ///
    /// - g_id 0: default graph (implicit)
    /// - g_id 1: txn-meta graph (reserved)
    /// - g_id 2+: user-defined named graphs
    ///
    /// # Arguments
    /// * `state` — mutable import state (carried across chunks)
    /// * `trig` — TriG input text (Turtle-compatible if no GRAPH blocks)
    /// * `storage` — storage backend for writing commit blobs
    /// * `ledger_alias` — ledger name for storage path construction
    /// * `compress` — whether to zstd-compress the ops stream
    pub async fn import_trig_commit<S>(
        state: &mut ImportState,
        trig: &str,
        storage: &S,
        ledger_alias: &str,
        compress: bool,
    ) -> Result<ImportCommitResult>
    where
        S: ContentAddressedWrite,
    {
        let new_t = state.t + 1;
        let txn_id = format!("{}-{}", ledger_alias, new_t);

        // 1. Parse TriG to extract GRAPH blocks
        let phase1 = parse_trig_phase1(trig)?;

        // If no named graphs and no txn-meta, use the faster pure-Turtle path
        if phase1.named_graphs.is_empty() && phase1.raw_meta.is_none() {
            return import_commit(state, trig, storage, ledger_alias, compress).await;
        }

        let _parse_span = tracing::info_span!(
            "import_trig_parse",
            t = new_t,
            trig_bytes = trig.len(),
            named_graph_count = phase1.named_graphs.len(),
        )
        .entered();

        // 2. Create ImportSink and parse default graph Turtle
        let mut sink = ImportSink::new(&mut state.ns_registry, new_t, txn_id.clone(), compress)
            .map_err(|e| TransactError::Parse(format!("failed to create import sink: {}", e)))?;
        fluree_graph_turtle::parse(&phase1.turtle, &mut sink)
            .map_err(|e| TransactError::Parse(e.to_string()))?;

        // 3. Retrieve writer for named graph flakes
        let mut writer = sink
            .finish()
            .map_err(|e| TransactError::Parse(format!("flake encode error: {}", e)))?;
        let mut op_count = writer.op_count();

        // 4. Process named graphs
        // Use session-level graph ID allocation for stability across chunks.
        // Only new mappings (introduced by this commit) go into graph_delta.
        let mut graph_delta: HashMap<u32, String> = HashMap::new();

        for block in &phase1.named_graphs {
            // Allocate or reuse g_id for this graph IRI from session state
            let g_id = if let Some(&existing) = state.graph_ids.get(&block.iri) {
                existing
            } else {
                // New graph IRI in this session — allocate and record in delta
                let id = state.next_gid;
                state.next_gid += 1;
                state.graph_ids.insert(block.iri.clone(), id);
                graph_delta.insert(id, block.iri.clone());
                id
            };

            // Create a graph Sid (using the graph IRI's namespace + local name)
            let graph_sid = state.ns_registry.sid_for_iri(&block.iri);

            // Process each triple in this named graph
            for triple in &block.triples {
                let subject = triple.subject.as_ref()
                    .ok_or_else(|| TransactError::Parse("named graph triple missing subject".to_string()))?;

                let s = expand_term(subject, &block.prefixes, &mut state.ns_registry)?;
                let p = expand_term(&triple.predicate, &block.prefixes, &mut state.ns_registry)?;

                for obj in &triple.objects {
                    let (o, dt, lang) = expand_object(obj, &block.prefixes, &mut state.ns_registry)?;

                    let meta = lang.map(|l| FlakeMeta::with_lang(&l));
                    let flake = Flake::new_in_graph(graph_sid.clone(), s.clone(), p.clone(), o, dt, new_t, true, meta);

                    writer.push_flake(&flake)
                        .map_err(|e| TransactError::Parse(format!("failed to encode named graph flake: {}", e)))?;
                    op_count += 1;
                }
            }

            // Suppress unused variable warning - g_id is used for stability tracking
            let _ = g_id;
        }
        drop(_parse_span);

        // 5. Resolve txn-meta if present
        let txn_meta = if let Some(ref raw_meta) = phase1.raw_meta {
            resolve_trig_meta(raw_meta, &mut state.ns_registry)?
        } else {
            Vec::new()
        };

        // 6. Build envelope
        let ns_delta = state.ns_registry.take_delta();
        let named_graph_count = graph_delta.len();

        tracing::info!(
            op_count,
            ns_delta_size = ns_delta.len(),
            graph_delta_size = named_graph_count,
            txn_meta_count = txn_meta.len(),
            "import trig sink finalized"
        );

        state.cumulative_flakes += op_count as u64;

        let envelope = CommitV2Envelope {
            t: new_t,
            v: 2,
            previous_ref: state.previous_ref.clone(),
            namespace_delta: ns_delta,
            txn: None,
            time: Some(state.import_time.clone()),
            data: None,
            index: None,
            txn_signature: None,
            txn_meta,
            graph_delta,
        };

        // 7. Finalize blob
        let result = {
            let _span = tracing::info_span!("import_trig_finish_blob", t = new_t, op_count).entered();
            writer.finish(&envelope)?
        };
        let commit_id = format!("sha256:{}", &result.content_hash_hex);
        let blob_bytes = result.bytes.len();

        // 8. Store
        let write_res = {
            let _span = tracing::info_span!("import_trig_store", t = new_t, blob_bytes).entered();
            storage
                .content_write_bytes_with_hash(
                    ContentKind::Commit,
                    ledger_alias,
                    &result.content_hash_hex,
                    &result.bytes,
                )
                .await?
        };

        tracing::info!(
            t = new_t,
            flakes = op_count,
            blob_bytes,
            address = %write_res.address,
            named_graphs = named_graph_count,
            "import trig commit stored"
        );

        // 9. Advance state
        state.t = new_t;
        state.previous_ref = Some(
            CommitRef::new(&write_res.address)
                .with_id(format!("fluree:commit:{}", commit_id)),
        );

        Ok(ImportCommitResult {
            address: write_res.address,
            commit_id,
            t: new_t,
            flake_count: op_count,
            blob_bytes,
            commit_blob: result.bytes,
        })
    }

    /// Expand a RawTerm to a Sid using the prefix map and namespace registry.
    fn expand_term(
        term: &RawTerm,
        prefixes: &rustc_hash::FxHashMap<String, String>,
        ns_registry: &mut NamespaceRegistry,
    ) -> Result<Sid> {
        match term {
            RawTerm::Iri(iri) => {
                if iri.starts_with("_:") {
                    // Blank node - skolemize
                    Ok(ns_registry.blank_node_sid(&iri[2..]))
                } else {
                    Ok(ns_registry.sid_for_iri(iri))
                }
            }
            RawTerm::PrefixedName { prefix, local } => {
                let ns = prefixes.get(prefix.as_str())
                    .ok_or_else(|| TransactError::Parse(format!("undefined prefix: {}", prefix)))?;
                let iri = format!("{}{}", ns, local);
                Ok(ns_registry.sid_for_iri(&iri))
            }
        }
    }

    /// Expand a RawObject to FlakeValue + datatype Sid + optional language.
    fn expand_object(
        obj: &RawObject,
        prefixes: &rustc_hash::FxHashMap<String, String>,
        ns_registry: &mut NamespaceRegistry,
    ) -> Result<(FlakeValue, Sid, Option<String>)> {
        match obj {
            RawObject::Iri(iri) => {
                let sid = if iri.starts_with("_:") {
                    ns_registry.blank_node_sid(&iri[2..])
                } else {
                    ns_registry.sid_for_iri(iri)
                };
                Ok((FlakeValue::Ref(sid), DT_ID.clone(), None))
            }
            RawObject::PrefixedName { prefix, local } => {
                let ns = prefixes.get(prefix.as_str())
                    .ok_or_else(|| TransactError::Parse(format!("undefined prefix: {}", prefix)))?;
                let iri = format!("{}{}", ns, local);
                let sid = ns_registry.sid_for_iri(&iri);
                Ok((FlakeValue::Ref(sid), DT_ID.clone(), None))
            }
            RawObject::String(s) => {
                Ok((FlakeValue::String(s.clone()), infer_datatype(&FlakeValue::String(s.clone())), None))
            }
            RawObject::Integer(n) => {
                Ok((FlakeValue::Long(*n), infer_datatype(&FlakeValue::Long(*n)), None))
            }
            RawObject::Double(n) => {
                Ok((FlakeValue::Double(*n), infer_datatype(&FlakeValue::Double(*n)), None))
            }
            RawObject::Boolean(b) => {
                Ok((FlakeValue::Boolean(*b), infer_datatype(&FlakeValue::Boolean(*b)), None))
            }
            RawObject::LangString { value, lang } => {
                Ok((FlakeValue::String(value.clone()), DT_LANG_STRING.clone(), Some(lang.clone())))
            }
            RawObject::TypedLiteral { value, datatype } => {
                let (fv, dt) = convert_string_literal(value, datatype, ns_registry);
                Ok((fv, dt, None))
            }
        }
    }

    // ========================================================================
    // Parallel-friendly split: parse_chunk + finalize_parsed_chunk
    // ========================================================================

    /// Result of parsing a single TTL chunk (parallelizable step).
    ///
    /// Contains the `StreamingCommitWriter` (tempfile-backed, Send) and the
    /// namespace delta from the parser's cloned registry. Can be sent across
    /// threads and finalized later on the commit thread.
    pub struct ParsedChunk {
        /// The streaming writer with all encoded ops spooled to a tempfile.
        pub writer: StreamingCommitWriter,
        /// Number of flakes (ops) encoded.
        pub op_count: u32,
        /// Namespace allocations from this chunk's cloned registry.
        /// Usually empty after chunk 0 establishes all known namespaces.
        pub ns_delta: HashMap<u16, String>,
    }

    /// Parse a TTL chunk into a `StreamingCommitWriter`. Thread-safe.
    ///
    /// Takes an **owned** `NamespaceRegistry` (cloned per worker thread)
    /// instead of `&mut ImportState`. This allows multiple chunks to be
    /// parsed concurrently, each with an independent registry clone.
    ///
    /// The `t` value is pre-assigned by the caller (chunk_index + 1).
    pub fn parse_chunk(
        ttl: &str,
        mut ns_registry: NamespaceRegistry,
        t: i64,
        ledger_alias: &str,
        compress: bool,
    ) -> Result<ParsedChunk> {
        let txn_id = format!("{}-{}", ledger_alias, t);

        let _parse_span = tracing::info_span!("parse_chunk", t, ttl_bytes = ttl.len(),).entered();

        let mut sink = ImportSink::new(&mut ns_registry, t, txn_id, compress)
            .map_err(|e| TransactError::Parse(format!("failed to create import sink: {}", e)))?;
        fluree_graph_turtle::parse(ttl, &mut sink)
            .map_err(|e| TransactError::Parse(e.to_string()))?;
        drop(_parse_span);

        let writer = sink
            .finish()
            .map_err(|e| TransactError::Parse(format!("flake encode error: {}", e)))?;
        let op_count = writer.op_count();
        let ns_delta = ns_registry.take_delta();

        Ok(ParsedChunk {
            writer,
            op_count,
            ns_delta,
        })
    }

    /// Finalize a parsed chunk: build envelope, store blob, update state.
    ///
    /// Must be called **serially in chunk order** because each commit
    /// references the previous commit's address (hash chain).
    pub async fn finalize_parsed_chunk<S>(
        state: &mut ImportState,
        parsed: ParsedChunk,
        storage: &S,
        ledger_alias: &str,
    ) -> Result<ImportCommitResult>
    where
        S: ContentAddressedWrite,
    {
        let new_t = state.t + 1;

        let _span = tracing::info_span!("finalize_parsed_chunk", t = new_t).entered();

        // Merge any new namespaces from clone into main registry
        for (code, prefix) in &parsed.ns_delta {
            state.ns_registry.ensure_code(*code, prefix);
        }
        let ns_delta = if !parsed.ns_delta.is_empty() {
            state.ns_registry.take_delta()
        } else {
            HashMap::new()
        };

        state.cumulative_flakes += parsed.op_count as u64;

        let envelope = CommitV2Envelope {
            t: new_t,
            v: 2,
            previous_ref: state.previous_ref.clone(),
            namespace_delta: ns_delta,
            txn: None,
            time: Some(state.import_time.clone()),
            data: None,
            index: None,
            txn_signature: None,
            txn_meta: Vec::new(),
            graph_delta: HashMap::new(),
        };

        let result = parsed.writer.finish(&envelope)?;
        let commit_id = format!("sha256:{}", &result.content_hash_hex);
        let blob_bytes = result.bytes.len();

        let write_res = storage
            .content_write_bytes_with_hash(
                ContentKind::Commit,
                ledger_alias,
                &result.content_hash_hex,
                &result.bytes,
            )
            .await?;

        tracing::info!(
            t = new_t,
            flakes = parsed.op_count,
            blob_bytes,
            address = %write_res.address,
            "parsed chunk finalized and stored"
        );

        state.t = new_t;
        state.previous_ref = Some(
            CommitRef::new(&write_res.address).with_id(format!("fluree:commit:{}", commit_id)),
        );

        Ok(ImportCommitResult {
            address: write_res.address,
            commit_id,
            t: new_t,
            flake_count: parsed.op_count,
            blob_bytes,
            commit_blob: result.bytes,
        })
    }
}

pub use inner::{
    finalize_parsed_chunk, import_commit, import_trig_commit, parse_chunk, ImportCommitResult,
    ImportState, ParsedChunk,
};
