//! Import mode: parse TTL → stream to commit-v2 blob → store.
//!
//! Bypasses the full staging/novelty pipeline for bulk import of clean
//! Turtle data. No WHERE evaluation, no cancellation, no policy enforcement,
//! no novelty index merge. Duplicate facts within a chunk are written as-is;
//! dedup happens during indexing or at query time.
//!
//! See the Phase 3 plan for full semantics documentation.

#[cfg(feature = "commit-v2")]
mod inner {
    use crate::commit_v2::CommitV2Envelope;
    use crate::commit_v2::StreamingCommitWriter;
    use crate::error::{Result, TransactError};
    use crate::import_sink::ImportSink;
    use crate::namespace::NamespaceRegistry;
    use fluree_db_core::{ContentAddressedWrite, ContentKind};
    use fluree_db_novelty::CommitRef;
    use std::collections::HashMap;

    /// Mutable state carried across chunks during an import session.
    pub struct ImportState {
        /// Current transaction number. Starts at 0; first chunk produces t=1.
        pub t: i64,
        /// Address of the previous commit blob (for commit chain linking).
        pub previous_address: Option<String>,
        /// Reference to the previous commit (address + id).
        pub previous_ref: Option<CommitRef>,
        /// Namespace registry (accumulates across chunks).
        pub ns_registry: NamespaceRegistry,
        /// Cumulative flake count across all commits (for progress reporting).
        pub cumulative_flakes: u64,
        /// Import start time (reused for all commits to avoid per-chunk Utc::now).
        pub import_time: String,
    }

    impl ImportState {
        /// Create a new import state for a fresh ledger.
        ///
        /// `NamespaceRegistry::new()` includes all predefined namespace codes
        /// (rdf, xsd, etc). User-defined namespaces are added as chunks are parsed.
        pub fn new() -> Self {
            Self {
                t: 0,
                previous_address: None,
                previous_ref: None,
                ns_registry: NamespaceRegistry::new(),
                cumulative_flakes: 0,
                import_time: chrono::Utc::now().to_rfc3339(),
            }
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
        let _parse_span = tracing::debug_span!(
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
            let _span = tracing::debug_span!("import_build_envelope", t = new_t).entered();
            let writer = sink.finish()
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
            previous: state.previous_address.clone(),
            previous_ref: state.previous_ref.clone(),
            namespace_delta: ns_delta,
            txn: None,
            time: Some(state.import_time.clone()),
            data: None, // DB stats not maintained during import
            index: None,
            indexed_at: None,
        };

            (writer, op_count, envelope)
        };

        // 4. Finalize blob
        let result = {
            let _span = tracing::debug_span!("import_finish_blob", t = new_t, op_count).entered();
            writer.finish(&envelope)?
        };
        let commit_id = format!("sha256:{}", &result.content_hash_hex);
        let blob_bytes = result.bytes.len();

        // 5. Store
        let write_res = {
            let _span = tracing::debug_span!("import_store", t = new_t, blob_bytes).entered();
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
        state.previous_address = Some(write_res.address.clone());
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
        pub ns_delta: HashMap<i32, String>,
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

        let _parse_span = tracing::debug_span!(
            "parse_chunk",
            t,
            ttl_bytes = ttl.len(),
        )
        .entered();

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

        let _span = tracing::debug_span!("finalize_parsed_chunk", t = new_t).entered();

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
            previous: state.previous_address.clone(),
            previous_ref: state.previous_ref.clone(),
            namespace_delta: ns_delta,
            txn: None,
            time: Some(state.import_time.clone()),
            data: None,
            index: None,
            indexed_at: None,
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
        state.previous_address = Some(write_res.address.clone());
        state.previous_ref = Some(
            CommitRef::new(&write_res.address)
                .with_id(format!("fluree:commit:{}", commit_id)),
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

#[cfg(feature = "commit-v2")]
pub use inner::{
    finalize_parsed_chunk, import_commit, parse_chunk, ImportCommitResult, ImportState, ParsedChunk,
};
