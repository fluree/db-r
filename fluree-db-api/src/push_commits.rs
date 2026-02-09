//! Push precomputed commit blobs to a transactor.
//!
//! This module implements a "push commits" protocol where clients submit
//! locally-written commit v2 blobs. The server validates them against the
//! current ledger state (sequencing, retraction invariants, policy, SHACL),
//! writes the commit bytes to storage, and advances `CommitHead` via CAS.
//!
//! Key invariants (first cut):
//! - The first commit's `t` MUST equal server `next_t` (strict sequencing).
//! - Retractions MUST target facts that are currently asserted at that point
//!   in the push batch (strict existence invariant).
//! - List retractions require exact metadata match (no hydration).
//! - Commit bytes are stored verbatim; the server does not rebuild commits.

use crate::dataset::QueryConnectionOptions;
use crate::error::{ApiError, Result};
use crate::policy_builder::build_policy_context_from_opts;
use crate::{Fluree, IndexConfig, LedgerHandle};
use base64::Engine as _;
use fluree_db_core::storage::extract_hash_from_address;
use fluree_db_core::storage::sha256_hex;
use fluree_db_core::{
    range_with_overlay, ContentAddressedWrite, ContentKind, DictNovelty, Flake, IndexType,
};
use fluree_db_core::{RangeMatch, RangeOptions, RangeTest, Storage};
use fluree_db_ledger::LedgerState;
use fluree_db_nameservice::{CasResult, NameService, RefKind, RefPublisher, RefValue};
use fluree_db_novelty::{generate_commit_flakes, Novelty};
use fluree_db_policy::PolicyContext;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::error;

#[cfg(feature = "shacl")]
use fluree_db_shacl::ShaclEngine;

/// Base64-encoded bytes for JSON payloads.
///
/// In JSON, we encode as a base64 string to avoid large `[0,1,2,...]` arrays.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Base64Bytes(pub Vec<u8>);

impl Base64Bytes {
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl Serialize for Base64Bytes {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = base64::engine::general_purpose::STANDARD.encode(&self.0);
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for Base64Bytes {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(s.as_bytes())
            .map_err(serde::de::Error::custom)?;
        Ok(Base64Bytes(bytes))
    }
}

/// Request body for pushing commits to a transactor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushCommitsRequest {
    /// Commit v2 blobs, in order (oldest -> newest).
    pub commits: Vec<Base64Bytes>,
    /// Optional additional blobs referenced by commits (e.g. `commit.txn`).
    ///
    /// Map key is the referenced address string.
    #[serde(default)]
    pub blobs: HashMap<String, Base64Bytes>,
}

/// Response body for a successful push.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushCommitsResponse {
    pub ledger: String,
    pub accepted: usize,
    pub head: PushedHead,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushedHead {
    pub t: i64,
    pub address: String,
}

/// Push precomputed commits to a ledger handle (transaction server mode).
///
/// This acquires the ledger write mutex for the duration of validation + publish,
/// so pushes are serialized with normal transactions.
impl<S, N> Fluree<S, N>
where
    S: Storage + ContentAddressedWrite + Clone + Send + Sync + 'static,
    N: NameService + RefPublisher + Send + Sync,
{
    pub async fn push_commits_with_handle(
        &self,
        handle: &LedgerHandle<S>,
        request: PushCommitsRequest,
        opts: &QueryConnectionOptions,
        index_config: &IndexConfig,
    ) -> Result<PushCommitsResponse> {
        if request.commits.is_empty() {
            return Err(ApiError::http(400, "missing required field 'commits'"));
        }

        // 0) Lock ledger state for write (serialize with transact).
        let mut guard = handle.lock_for_write().await;
        let base_state = guard.clone_state();

        // 1) Read current head ref (CAS expected).
        let current_ref = self
            .nameservice()
            .get_ref(base_state.ledger_address(), RefKind::CommitHead)
            .await?;
        let Some(current_ref) = current_ref else {
            return Err(ApiError::NotFound(format!(
                "Ledger not found: {}",
                base_state.ledger_address()
            )));
        };

        // 2) Decode commits and preflight strict sequencing.
        let decoded = decode_and_validate_commit_chain(base_state.ledger_address(), &request)
            .map_err(|e| e.into_api_error())?;

        preflight_strict_next_t_and_prev(&current_ref, &decoded).map_err(|e| e.into_api_error())?;

        // 3) Validate referenced blobs are provided (if any) and pre-validate hashes.
        validate_required_blobs(&decoded, &request.blobs).map_err(|e| e.into_api_error())?;

        // 4) Validate each commit against evolving server view.
        //
        // We maintain an owned Novelty overlay that starts as the current novelty and
        // is extended with each accepted commit (including derived commit-metadata flakes),
        // so later commits validate against the state they would observe when applied.
        let mut evolving_novelty = (*base_state.novelty).clone();

        // Track per-commit "all flakes" so we can update state after CAS.
        let mut accepted_all_flakes: Vec<(i64, Vec<Flake>)> = Vec::with_capacity(decoded.len());

        for c in &decoded {
            // Current state is base db + evolving novelty.
            let current_t = base_state.db.t.max(evolving_novelty.t);

            // 4.1 Retraction invariant (strict).
            assert_retractions_exist(
                &base_state.db,
                &evolving_novelty,
                current_t,
                &c.commit.flakes,
            )
            .await
            .map_err(|e| e.into_api_error())?;

            // 4.2 Policy enforcement: build policy context from opts against current state.
            let policy_ctx =
                build_policy_ctx_for_push(&base_state, &evolving_novelty, current_t, opts).await?;

            // 4.3 Stage flakes (policy/backpressure). No WHERE/cancellation; flakes are prebuilt.
            let staged_view = stage_commit_flakes(
                base_state.clone_with_novelty(Arc::new(evolving_novelty.clone())),
                &c.commit.flakes,
                index_config,
                &policy_ctx,
            )
            .await
            .map_err(|e| e.into_api_error())?;

            // 4.4 SHACL (optional feature).
            #[cfg(feature = "shacl")]
            {
                let engine = ShaclEngine::from_db_with_overlay(
                    &base_state.db,
                    &evolving_novelty,
                    base_state.ledger_address(),
                )
                .await
                .map_err(|e| ApiError::Transact(fluree_db_transact::TransactError::from(e)))?;
                let shacl_cache = engine.cache().clone();
                fluree_db_transact::validate_view_with_shacl(&staged_view, &shacl_cache)
                    .await
                    .map_err(|e| ApiError::http(422, e.to_string()))?;
            }
            #[cfg(not(feature = "shacl"))]
            {
                let _ = &staged_view;
            }

            // 4.5 Advance evolving novelty with this commit's flakes + derived metadata flakes.
            let mut all_flakes = c.commit.flakes.clone();
            let meta_flakes =
                generate_commit_flakes(&c.commit, base_state.ledger_address(), c.commit.t);
            all_flakes.extend(meta_flakes);

            // Note: Novelty::apply_commit bumps to max(commit_t) internally.
            evolving_novelty
                .apply_commit(all_flakes.clone(), c.commit.t)
                .map_err(ApiError::Novelty)?;

            accepted_all_flakes.push((c.commit.t, all_flakes));
        }

        // 5) Write required blobs and commit bytes to storage (safe before CAS).
        write_required_blobs(
            self.storage(),
            base_state.ledger_address(),
            &request.blobs,
            &decoded,
        )
        .await
        .map_err(|e| e.into_api_error())?;

        let stored_commits =
            write_commit_blobs(self.storage(), base_state.ledger_address(), &decoded)
                .await
                .map_err(|e| e.into_api_error())?;

        let final_head = stored_commits.last().expect("non-empty stored_commits");
        let new_ref = RefValue {
            address: Some(final_head.address.clone()),
            t: final_head.t,
        };

        // 6) CAS update CommitHead (single CAS, strict).
        match self
            .nameservice()
            .compare_and_set_ref(
                base_state.ledger_address(),
                RefKind::CommitHead,
                Some(&current_ref),
                &new_ref,
            )
            .await?
        {
            CasResult::Updated => {}
            CasResult::Conflict { actual } => {
                return Err(ApiError::http(
                    409,
                    format!(
                        "commit head changed during push (expected t={}, actual={:?})",
                        current_ref.t, actual
                    ),
                ));
            }
        }

        // 7) Update in-memory ledger state (now committed).
        let new_state =
            apply_pushed_commits_to_state(base_state, &accepted_all_flakes, &stored_commits);
        guard.replace(new_state);

        Ok(PushCommitsResponse {
            ledger: handle.ledger_address().to_string(),
            accepted: decoded.len(),
            head: PushedHead {
                t: final_head.t,
                address: final_head.address.clone(),
            },
        })
    }
}

// === Internal helpers ===

#[derive(Debug)]
struct PushCommitDecoded {
    commit: fluree_db_novelty::Commit,
    bytes: Vec<u8>,
    content_hash_hex: String,
}

#[derive(Debug, Clone)]
struct StoredCommit {
    t: i64,
    address: String,
}

#[derive(Debug)]
enum PushError {
    Conflict(String),
    Invalid(String),
    Forbidden(String),
    Internal(String),
}

impl PushError {
    fn into_api_error(self) -> ApiError {
        match self {
            PushError::Conflict(m) => ApiError::http(409, m),
            PushError::Invalid(m) => ApiError::http(422, m),
            PushError::Forbidden(m) => ApiError::http(403, m),
            PushError::Internal(m) => ApiError::internal(m),
        }
    }
}

fn decode_and_validate_commit_chain(
    _ledger_address: &str,
    request: &PushCommitsRequest,
) -> std::result::Result<Vec<PushCommitDecoded>, PushError> {
    let mut out = Vec::with_capacity(request.commits.len());

    let mut prev_t: Option<i64> = None;
    let mut prev_hash: Option<String> = None;

    for (idx, b64) in request.commits.iter().enumerate() {
        let bytes = b64.0.clone();
        let commit = fluree_db_novelty::commit_v2::read_commit(&bytes)
            .map_err(|e| PushError::Invalid(format!("invalid commit[{}]: {}", idx, e)))?;

        // Reject empty commits (no flakes) - keep semantics clear.
        if commit.flakes.is_empty() {
            return Err(PushError::Invalid(format!(
                "invalid commit[{}]: empty commit (no flakes)",
                idx
            )));
        }

        // Extract the embedded content hash (trailing hash bytes) as hex.
        let content_hash_hex = commit_hash_hex_from_bytes(&bytes).map_err(PushError::Invalid)?;

        // Note: commit blobs are applied to the server-selected `ledger_address` via CAS.
        // We do not currently enforce a ledger identity embedded inside the commit bytes.

        // Chain validation: strict contiguous t (+1).
        if let Some(pt) = prev_t {
            if commit.t != pt + 1 {
                return Err(PushError::Invalid(format!(
                    "commit chain is not contiguous: commit[{}].t={} does not equal prior+1={}",
                    idx,
                    commit.t,
                    pt + 1
                )));
            }
        }

        // Chain validation: previous reference must match prior commit (by id if present).
        if let Some(prev_hash_hex) = &prev_hash {
            let prev_id = format!("fluree:commit:sha256:{}", prev_hash_hex);
            let ok = commit
                .previous_ref
                .as_ref()
                .and_then(|r| r.id.as_ref())
                .map(|id| id == &prev_id)
                .unwrap_or(false);
            if !ok {
                return Err(PushError::Invalid(format!(
                    "commit chain previous mismatch at commit[{}]: expected previous id '{}'",
                    idx, prev_id
                )));
            }
        }

        prev_t = Some(commit.t);
        prev_hash = Some(content_hash_hex.clone());

        out.push(PushCommitDecoded {
            commit,
            bytes,
            content_hash_hex,
        });
    }

    Ok(out)
}

fn preflight_strict_next_t_and_prev(
    current: &RefValue,
    decoded: &[PushCommitDecoded],
) -> std::result::Result<(), PushError> {
    let first = decoded.first().expect("non-empty decoded");

    let expected_t = current.t + 1;
    if first.commit.t != expected_t {
        return Err(PushError::Conflict(format!(
            "first commit t mismatch: expected next-t={}, got {}",
            expected_t, first.commit.t
        )));
    }

    let expected_prev = current.address.as_deref();
    let actual_prev = first
        .commit
        .previous_ref
        .as_ref()
        .map(|r| r.address.as_str());

    if expected_prev != actual_prev {
        return Err(PushError::Conflict(format!(
            "first commit previous mismatch: expected {:?}, got {:?}",
            expected_prev, actual_prev
        )));
    }

    Ok(())
}

fn validate_required_blobs(
    decoded: &[PushCommitDecoded],
    provided: &HashMap<String, Base64Bytes>,
) -> std::result::Result<(), PushError> {
    let mut required: HashSet<&str> = HashSet::new();
    for c in decoded {
        if let Some(txn_addr) = c.commit.txn.as_deref() {
            required.insert(txn_addr);
        }
    }

    for addr in required {
        if !provided.contains_key(addr) {
            return Err(PushError::Invalid(format!(
                "missing required blob for referenced address: {}",
                addr
            )));
        }
        // Also validate address contains a hash (so we can do safe CAS writes).
        if extract_hash_from_address(addr).is_none() {
            return Err(PushError::Invalid(format!(
                "referenced blob address is not content-addressed: {}",
                addr
            )));
        }
    }

    Ok(())
}

async fn build_policy_ctx_for_push<S: Storage + Clone + 'static>(
    base: &LedgerState<S>,
    evolving: &Novelty,
    current_t: i64,
    opts: &QueryConnectionOptions,
) -> Result<PolicyContext> {
    // Build policy context from opts against current state (db + evolving novelty).
    build_policy_context_from_opts(&base.db, evolving, Some(evolving), current_t, opts).await
}

async fn stage_commit_flakes<S: Storage + Clone + Send + Sync + 'static>(
    ledger: LedgerState<S>,
    flakes: &[Flake],
    index_config: &IndexConfig,
    policy_ctx: &PolicyContext,
) -> std::result::Result<fluree_db_ledger::LedgerView<S>, PushError> {
    let mut options = fluree_db_transact::StageOptions::new().with_index_config(index_config);
    if !policy_ctx.wrapper().is_root() {
        options = options.with_policy(policy_ctx);
    }
    fluree_db_transact::stage_flakes(ledger, flakes.to_vec(), options)
        .await
        .map_err(|e| match e {
            fluree_db_transact::TransactError::PolicyViolation(p) => {
                PushError::Forbidden(p.to_string())
            }
            other => PushError::Invalid(other.to_string()),
        })
}

async fn assert_retractions_exist<S: Storage + Clone + 'static>(
    db: &fluree_db_core::Db<S>,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    flakes: &[Flake],
) -> std::result::Result<(), PushError> {
    for (idx, f) in flakes.iter().enumerate() {
        if f.op {
            continue;
        }

        if !is_currently_asserted(db, overlay, to_t, f).await? {
            return Err(PushError::Invalid(format!(
                "retraction invariant violated at flake[{}]: retract targets non-existent assertion",
                idx
            )));
        }
    }
    Ok(())
}

async fn is_currently_asserted<S: Storage + Clone + 'static>(
    db: &fluree_db_core::Db<S>,
    overlay: &dyn fluree_db_core::OverlayProvider,
    to_t: i64,
    target: &Flake,
) -> std::result::Result<bool, PushError> {
    let rm = RangeMatch::new()
        .with_subject(target.s.clone())
        .with_predicate(target.p.clone())
        .with_object(target.o.clone())
        .with_datatype(target.dt.clone());

    let found = range_with_overlay(
        db,
        overlay,
        IndexType::Spot,
        RangeTest::Eq,
        rm,
        RangeOptions::new().with_to_t(to_t),
    )
    .await
    .map_err(|e| PushError::Internal(e.to_string()))?;

    let mut last_t: Option<i64> = None;
    let mut last_op: bool = false;

    for f in found.into_iter() {
        // Exact match on graph + metadata. (RangeMatch does not include these fields.)
        if f.g != target.g || f.m != target.m {
            continue;
        }
        // Current truth is the op of the latest t <= to_t.
        if last_t.map(|t| f.t > t).unwrap_or(true) {
            last_t = Some(f.t);
            last_op = f.op;
        }
    }

    Ok(last_t.is_some() && last_op)
}

async fn write_required_blobs<
    S: Storage + ContentAddressedWrite + Clone + Send + Sync + 'static,
>(
    storage: &S,
    ledger_address: &str,
    provided: &HashMap<String, Base64Bytes>,
    decoded: &[PushCommitDecoded],
) -> std::result::Result<(), PushError> {
    // Build required set (txn addresses only, for now).
    let mut required: HashSet<&str> = HashSet::new();
    for c in decoded {
        if let Some(txn_addr) = c.commit.txn.as_deref() {
            required.insert(txn_addr);
        }
    }

    for addr in required {
        let Some(expected_hash) = extract_hash_from_address(addr) else {
            return Err(PushError::Invalid(format!(
                "referenced blob address is not content-addressed: {}",
                addr
            )));
        };
        let bytes = provided
            .get(addr)
            .ok_or_else(|| PushError::Invalid(format!("missing required blob: {}", addr)))?
            .0
            .clone();

        // Integrity: verify provided bytes match the referenced hash.
        let actual_hash = sha256_hex(&bytes);
        if actual_hash != expected_hash {
            return Err(PushError::Invalid(format!(
                "blob hash mismatch for '{}': expected {}, got {}",
                addr, expected_hash, actual_hash
            )));
        }

        // Write using the hash from the address to ensure deterministic placement.
        let res = storage
            .content_write_bytes_with_hash(ContentKind::Txn, ledger_address, &expected_hash, &bytes)
            .await
            .map_err(|e| PushError::Internal(e.to_string()))?;

        // Ensure the storage's canonical address matches the referenced address.
        if res.address != addr {
            return Err(PushError::Invalid(format!(
                "uploaded blob address mismatch: commit references '{}', storage wrote '{}'",
                addr, res.address
            )));
        }
    }

    Ok(())
}

async fn write_commit_blobs<S: Storage + ContentAddressedWrite + Clone + Send + Sync + 'static>(
    storage: &S,
    ledger_address: &str,
    decoded: &[PushCommitDecoded],
) -> std::result::Result<Vec<StoredCommit>, PushError> {
    let mut stored = Vec::with_capacity(decoded.len());
    for (idx, c) in decoded.iter().enumerate() {
        let res = storage
            .content_write_bytes_with_hash(
                ContentKind::Commit,
                ledger_address,
                &c.content_hash_hex,
                &c.bytes,
            )
            .await
            .map_err(|e| PushError::Internal(e.to_string()))?;
        if res.content_hash != c.content_hash_hex {
            return Err(PushError::Invalid(format!(
                "commit[{}] hash mismatch after write: expected {}, storage reported {}",
                idx, c.content_hash_hex, res.content_hash
            )));
        }
        stored.push(StoredCommit {
            t: c.commit.t,
            address: res.address,
        });
    }
    Ok(stored)
}

fn apply_pushed_commits_to_state<S: Storage + Clone + 'static>(
    mut base: LedgerState<S>,
    accepted_all_flakes: &[(i64, Vec<Flake>)],
    stored_commits: &[StoredCommit],
) -> LedgerState<S> {
    // Apply namespace deltas from commits to in-memory Db (Clojure parity).
    //
    // NOTE: We only have access to the full commit structs during validation; the
    // committed SIDs in flakes do not require namespace_codes, but keeping Db updated
    // is important for subsequent API calls that encode IRIs.
    //
    // For now, we conservatively reload namespace deltas by decoding the stored commits
    // would be expensive; instead, we rely on the fact that namespace delta flakes
    // are already applied via novelty, and encoding uses namespace_codes primarily
    // for new allocations. This is acceptable for push-only path.

    // Apply all flakes to novelty (we already validated them; re-apply for state).
    let mut novelty = (*base.novelty).clone();
    let mut dict_novelty = base.dict_novelty.clone();

    for (t, flakes) in accepted_all_flakes {
        // Populate dict novelty similarly to transact commit path.
        populate_dict_novelty(Arc::make_mut(&mut dict_novelty), flakes);
        // Apply to novelty.
        if let Err(e) = novelty.apply_commit(flakes.clone(), *t) {
            error!(
                error = ?e,
                commit_t = *t,
                "post-CAS novelty apply_commit failed; in-memory state may be stale until reload"
            );
        }
    }

    base.novelty = Arc::new(novelty);
    base.dict_novelty = dict_novelty;
    base.head_commit = stored_commits.last().map(|c| c.address.clone());
    if let Some(ref mut r) = base.ns_record {
        if let Some(last) = stored_commits.last() {
            r.commit_address = Some(last.address.clone());
            r.commit_t = last.t;
        }
    }
    base
}

fn populate_dict_novelty(dict_novelty: &mut DictNovelty, flakes: &[Flake]) {
    use fluree_db_core::FlakeValue;

    dict_novelty.ensure_initialized();

    for flake in flakes {
        // Subject
        dict_novelty
            .subjects
            .assign_or_lookup(flake.s.namespace_code, &flake.s.name);

        // Predicate
        dict_novelty
            .subjects
            .assign_or_lookup(flake.p.namespace_code, &flake.p.name);

        // Object references
        if let FlakeValue::Ref(ref sid) = flake.o {
            dict_novelty
                .subjects
                .assign_or_lookup(sid.namespace_code, &sid.name);
        }

        // String values
        match &flake.o {
            FlakeValue::String(s) | FlakeValue::Json(s) => {
                dict_novelty.strings.assign_or_lookup(s);
            }
            _ => {}
        }
    }
}

fn commit_hash_hex_from_bytes(bytes: &[u8]) -> std::result::Result<String, String> {
    use fluree_db_novelty::commit_v2::format::{
        CommitV2Header, FLAG_HAS_COMMIT_SIG, HASH_LEN, HEADER_LEN,
    };

    if bytes.len() < HEADER_LEN + HASH_LEN {
        return Err("commit too small".to_string());
    }
    let header = CommitV2Header::read_from(bytes).map_err(|e| e.to_string())?;
    let blob_len = bytes.len();
    let sig_block_len = header.sig_block_len as usize;
    let has_sig_block = header.flags & FLAG_HAS_COMMIT_SIG != 0 && sig_block_len > 0;
    let hash_offset = if has_sig_block {
        blob_len
            .checked_sub(sig_block_len + HASH_LEN)
            .ok_or_else(|| "commit too small (sig)".to_string())?
    } else {
        blob_len - HASH_LEN
    };
    let hash_bytes = &bytes[hash_offset..hash_offset + HASH_LEN];
    Ok(hex::encode(hash_bytes))
}

trait LedgerStateCloneExt<S> {
    fn clone_with_novelty(&self, novelty: Arc<Novelty>) -> Self;
}

impl<S: Storage + Clone + 'static> LedgerStateCloneExt<S> for LedgerState<S> {
    fn clone_with_novelty(&self, novelty: Arc<Novelty>) -> Self {
        let mut s = self.clone();
        s.novelty = novelty;
        s
    }
}
