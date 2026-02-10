//! Commit transfer: push, export, and import of commit v2 blobs.
//!
//! ## Push (client → server)
//!
//! Clients submit locally-written commit v2 blobs. The server validates them
//! against the current ledger state (sequencing, retraction invariants, policy,
//! SHACL), writes the commit bytes to storage, and advances `CommitHead` via CAS.
//!
//! Key invariants:
//! - The first commit's `t` MUST equal server `next_t` (strict sequencing).
//! - Retractions MUST target facts currently asserted at that point in the batch.
//! - List retractions require exact metadata match (no hydration).
//! - Commit bytes are stored verbatim; the server does not rebuild commits.
//!
//! ## Export (server → client)
//!
//! Paginated export of commit blobs using address-cursor pagination.
//! Pages walk backward via `previous_ref` — O(limit) per page regardless of
//! ledger size. Used by pull and clone operations.

use crate::dataset::QueryConnectionOptions;
use crate::error::{ApiError, Result};
use crate::policy_builder::build_policy_context_from_opts;
use crate::{Fluree, IndexConfig, LedgerHandle};
use base64::Engine as _;
use fluree_db_core::storage::extract_hash_from_address;
use fluree_db_core::ContentId;
use fluree_db_core::{
    range_with_overlay, ContentAddressedWrite, ContentKind, DictNovelty, Flake, IndexType,
};
use fluree_db_core::{RangeMatch, RangeOptions, RangeTest, Storage};
use fluree_db_core::{CODEC_FLUREE_COMMIT, CODEC_FLUREE_TXN};
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
    /// Map key is a CID string or legacy address string.
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
            .get_ref(base_state.ledger_id(), RefKind::CommitHead)
            .await?;
        let Some(current_ref) = current_ref else {
            return Err(ApiError::NotFound(format!(
                "Ledger not found: {}",
                base_state.ledger_id()
            )));
        };

        // 2) Decode commits and preflight strict sequencing.
        let decoded = decode_and_validate_commit_chain(base_state.ledger_id(), &request)
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
                    base_state.ledger_id(),
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
            let meta_flakes = generate_commit_flakes(&c.commit, base_state.ledger_id(), c.commit.t);
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
            base_state.ledger_id(),
            &request.blobs,
            &decoded,
        )
        .await
        .map_err(|e| e.into_api_error())?;

        let stored_commits = write_commit_blobs(self.storage(), base_state.ledger_id(), &decoded)
            .await
            .map_err(|e| e.into_api_error())?;

        let final_head = stored_commits.last().expect("non-empty stored_commits");
        let new_ref = RefValue {
            id: final_head.commit_id.clone(),
            t: final_head.t,
        };

        // 6) CAS update CommitHead (single CAS, strict).
        match self
            .nameservice()
            .compare_and_set_ref(
                base_state.ledger_id(),
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
            ledger: handle.ledger_id().to_string(),
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
    commit_id: Option<ContentId>,
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
    _ledger_id: &str,
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

        // Note: commit blobs are applied to the server-selected `ledger_id` via CAS.
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
            let ok = commit
                .previous_ref
                .as_ref()
                .map(|r| r.id.digest_hex() == *prev_hash_hex)
                .unwrap_or(false);
            if !ok {
                return Err(PushError::Invalid(format!(
                    "commit chain previous mismatch at commit[{}]: expected previous digest '{}'",
                    idx, prev_hash_hex
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

    // Validate previous reference matches current head via CID.
    let expected_prev_id: Option<ContentId> = current.id.clone();

    let actual_prev_id = first.commit.previous_ref.as_ref().map(|r| r.id.clone());

    if expected_prev_id != actual_prev_id {
        return Err(PushError::Conflict(format!(
            "first commit previous mismatch: expected {:?}, got {:?}",
            expected_prev_id, actual_prev_id
        )));
    }

    Ok(())
}

fn validate_required_blobs(
    decoded: &[PushCommitDecoded],
    provided: &HashMap<String, Base64Bytes>,
) -> std::result::Result<(), PushError> {
    let mut required: HashSet<String> = HashSet::new();
    for c in decoded {
        if let Some(txn_cid) = &c.commit.txn {
            required.insert(txn_cid.to_string());
        }
    }

    for addr in &required {
        if !provided.contains_key(addr) {
            return Err(PushError::Invalid(format!(
                "missing required blob for referenced address: {}",
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
    ledger_id: &str,
    provided: &HashMap<String, Base64Bytes>,
    decoded: &[PushCommitDecoded],
) -> std::result::Result<(), PushError> {
    // Build required set (txn CID strings, for now).
    let mut required: HashSet<String> = HashSet::new();
    for c in decoded {
        if let Some(txn_cid) = &c.commit.txn {
            required.insert(txn_cid.to_string());
        }
    }

    for addr in &required {
        let txn_id: ContentId = addr.parse().map_err(|e| {
            PushError::Invalid(format!("invalid txn CID reference '{}': {}", addr, e))
        })?;
        if txn_id.codec() != CODEC_FLUREE_TXN {
            return Err(PushError::Invalid(format!(
                "referenced txn CID has unexpected codec {}: {}",
                txn_id.codec(),
                addr
            )));
        }

        let bytes = provided
            .get(addr)
            .ok_or_else(|| PushError::Invalid(format!("missing required blob: {}", addr)))?
            .0
            .clone();

        // Integrity: server MUST re-hash bytes and verify the derived CID.
        if !txn_id.verify(&bytes) {
            return Err(PushError::Invalid(format!(
                "referenced txn CID does not match provided bytes: {}",
                addr
            )));
        }

        // Write using the CID digest to ensure deterministic placement.
        // (digest hex matches the legacy CAS hash during the transition period)
        let expected_hash = txn_id.digest_hex();
        let _res = storage
            .content_write_bytes_with_hash(ContentKind::Txn, ledger_id, &expected_hash, &bytes)
            .await
            .map_err(|e| PushError::Internal(e.to_string()))?;
    }

    Ok(())
}

/// Derive the legacy txn blob address from a commit blob address and txn digest hex.
///
/// Assumes the current storage layout where commits live under `/commit/<hash>.fcv2`
/// and txns under `/txn/<hash>.json` for the same ledger prefix and storage method.
fn txn_address_from_commit_address(commit_address: &str, txn_hash_hex: &str) -> Option<String> {
    let (prefix, _rest) = commit_address.split_once("/commit/")?;
    Some(format!("{}/txn/{}.json", prefix, txn_hash_hex))
}

/// Replace the filename stem (between the last `/` and last `.`) with `new_hash_hex`.
fn replace_hash_in_address(address: &str, new_hash_hex: &str) -> Option<String> {
    let slash = address.rfind('/')?;
    let dot = address.rfind('.')?;
    if dot <= slash + 1 {
        return None;
    }
    let mut out = String::with_capacity(address.len() - (dot - (slash + 1)) + new_hash_hex.len());
    out.push_str(&address[..slash + 1]);
    out.push_str(new_hash_hex);
    out.push_str(&address[dot..]);
    Some(out)
}

async fn write_commit_blobs<S: Storage + ContentAddressedWrite + Clone + Send + Sync + 'static>(
    storage: &S,
    ledger_id: &str,
    decoded: &[PushCommitDecoded],
) -> std::result::Result<Vec<StoredCommit>, PushError> {
    let mut stored = Vec::with_capacity(decoded.len());
    for (idx, c) in decoded.iter().enumerate() {
        let res = storage
            .content_write_bytes_with_hash(
                ContentKind::Commit,
                ledger_id,
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
        let commit_id = ContentId::from_hex_digest(CODEC_FLUREE_COMMIT, &c.content_hash_hex);
        stored.push(StoredCommit {
            t: c.commit.t,
            address: res.address,
            commit_id,
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
    base.head_commit_id = stored_commits.last().and_then(|c| c.commit_id.clone());
    if let Some(ref mut r) = base.ns_record {
        if let Some(last) = stored_commits.last() {
            r.commit_head_id = last.commit_id.clone();
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

// ============================================================================
// Export (server → client)
// ============================================================================

/// Maximum commits per export page (server-enforced cap).
const EXPORT_MAX_LIMIT: usize = 500;

/// Default commits per export page when no limit is specified.
const EXPORT_DEFAULT_LIMIT: usize = 100;

/// Query parameters for paginated commit export.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportCommitsRequest {
    /// Commit cursor to start from.
    ///
    /// Backward-compatible field:
    /// - legacy: commit address (e.g. `fluree:file://.../commit/<hash>.fcv2`)
    /// - new: commit CID string (e.g. `bafy...`)
    ///
    /// When both `cursor` and `cursor_id` are provided, `cursor_id` wins.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Commit CID cursor to start from (storage-agnostic identity).
    ///
    /// Preferred for cross-backend sync. When `None`, starts from the current head.
    #[serde(default)]
    pub cursor_id: Option<ContentId>,
    /// Maximum commits per page. Clamped to server max (500).
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Paginated response containing commit blobs (newest → oldest).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportCommitsResponse {
    /// Ledger identifier.
    pub ledger: String,
    /// Head address at time of export (informational; clone uses this for final head).
    pub head_address: String,
    /// Head commit CID at time of export (storage-agnostic identity).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_commit_id: Option<ContentId>,
    /// Head `t` at time of export (informational).
    pub head_t: i64,
    /// Raw commit v2 blobs, newest → oldest within this page.
    pub commits: Vec<Base64Bytes>,
    /// Referenced blobs (txn blobs) keyed by CID string.
    #[serde(default)]
    pub blobs: HashMap<String, Base64Bytes>,
    /// Highest `t` in this page.
    pub newest_t: i64,
    /// Lowest `t` in this page.
    pub oldest_t: i64,
    /// Cursor for the next page (legacy: previous commit address).
    /// `None` when genesis has been reached.
    pub next_cursor: Option<String>,
    /// Cursor for the next page (previous commit CID).
    /// `None` when genesis has been reached.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor_id: Option<ContentId>,
    /// Number of commits in this page.
    pub count: usize,
    /// Actual limit used (after server clamping).
    pub effective_limit: usize,
}

/// Export a paginated range of commits from a ledger.
///
/// Uses address-cursor pagination: each page walks backward from the cursor
/// via `previous_ref` for up to `limit` commits. Each page is O(limit)
/// regardless of total ledger size.
///
/// Commits are returned newest → oldest. The client reverses for import.
impl<S, N> Fluree<S, N>
where
    S: Storage + Clone + Send + Sync + 'static,
    N: NameService + RefPublisher + Send + Sync,
{
    pub async fn export_commit_range(
        &self,
        handle: &LedgerHandle<S>,
        request: &ExportCommitsRequest,
    ) -> Result<ExportCommitsResponse> {
        use fluree_db_novelty::commit_v2::envelope::decode_envelope;
        use fluree_db_novelty::commit_v2::format::{CommitV2Header, HEADER_LEN};

        let effective_limit = request
            .limit
            .unwrap_or(EXPORT_DEFAULT_LIMIT)
            .min(EXPORT_MAX_LIMIT);

        let ledger_id = handle.ledger_id();

        // Read current head.
        let head_ref = self
            .nameservice()
            .get_ref(ledger_id, RefKind::CommitHead)
            .await?;
        let Some(head_ref) = head_ref else {
            return Err(ApiError::NotFound(format!(
                "Ledger not found: {}",
                ledger_id
            )));
        };
        let head_commit_id = head_ref
            .id
            .clone()
            .ok_or_else(|| ApiError::NotFound("Ledger has no commits".to_string()))?;
        let head_t = head_ref.t;
        let method = self.storage().storage_method().to_string();
        let head_address = fluree_db_core::storage::content_address(
            &method,
            ContentKind::Commit,
            ledger_id,
            &head_commit_id.digest_hex(),
        );
        let head_method = Some(method);

        // Determine start cursor.
        let mut cursor_is_address = false;
        let mut start_cursor_id: Option<ContentId> = request.cursor_id.clone();
        let mut start_address_opt: Option<String> = None;

        if start_cursor_id.is_none() {
            if let Some(raw) = request.cursor.as_deref() {
                // Accept CID in the legacy `cursor` field for convenience.
                if let Ok(cid) = raw.parse::<ContentId>() {
                    start_cursor_id = Some(cid);
                } else {
                    cursor_is_address = true;
                    start_address_opt = Some(raw.to_string());
                }
            }
        }

        let start_address: String = if let Some(cid) = &start_cursor_id {
            let method = head_method.as_deref().ok_or_else(|| {
                ApiError::internal(format!(
                    "cannot resolve cursor_id without a fluree head address (got '{}')",
                    head_address
                ))
            })?;
            fluree_db_core::storage::content_address(
                method,
                ContentKind::Commit,
                ledger_id,
                &cid.digest_hex(),
            )
        } else if let Some(addr) = start_address_opt {
            addr
        } else {
            head_address.clone()
        };

        // Validate cursor belongs to this ledger (address embeds alias path).
        if cursor_is_address {
            use fluree_db_core::storage::alias_prefix_for_path;
            let alias_path = alias_prefix_for_path(ledger_id);
            // Address format: "fluree:{method}://{alias_path}/commit/{hash}.ext"
            // Check that the alias_path segment appears after the "://" separator.
            let after_scheme = start_address
                .find("://")
                .map(|i| &start_address[i + 3..])
                .unwrap_or(&start_address);
            // Important: require a path boundary to avoid prefix collisions
            // (e.g. "mydb/main2/..." must not match "mydb/main").
            let expected_prefix = format!("{}/", alias_path);
            if !after_scheme.starts_with(&expected_prefix) {
                return Err(ApiError::http(
                    400,
                    format!(
                        "cursor address does not belong to ledger '{}': {}",
                        ledger_id, start_address
                    ),
                ));
            }
        }

        let storage = self.storage().clone();
        let mut commits = Vec::with_capacity(effective_limit);
        let mut blobs: HashMap<String, Base64Bytes> = HashMap::new();
        let mut newest_t: Option<i64> = None;
        let mut oldest_t: Option<i64> = None;
        let mut next_cursor: Option<String> = None;
        let mut next_cursor_id: Option<ContentId> = None;
        let mut current_address = start_address;

        for _ in 0..effective_limit {
            // Read raw commit bytes from CAS.
            let raw_bytes = storage.read_bytes(&current_address).await.map_err(|e| {
                ApiError::internal(format!(
                    "failed to read commit at '{}': {}",
                    current_address, e
                ))
            })?;

            // Lightweight decode: header + envelope only (skip ops decompression).
            let header = CommitV2Header::read_from(&raw_bytes).map_err(|e| {
                ApiError::internal(format!(
                    "invalid commit header at '{}': {}",
                    current_address, e
                ))
            })?;

            let envelope_start = HEADER_LEN;
            let envelope_end = envelope_start + header.envelope_len as usize;
            if envelope_end > raw_bytes.len() {
                return Err(ApiError::internal(format!(
                    "commit envelope extends past blob at '{}'",
                    current_address
                )));
            }
            let env = decode_envelope(&raw_bytes[envelope_start..envelope_end]).map_err(|e| {
                ApiError::internal(format!(
                    "failed to decode envelope at '{}': {}",
                    current_address, e
                ))
            })?;

            // Track t range.
            let t = header.t;
            if newest_t.is_none() {
                newest_t = Some(t);
            }
            oldest_t = Some(t);

            commits.push(Base64Bytes(raw_bytes));

            // Collect referenced txn blob.
            if let Some(ref txn_cid) = env.txn {
                let txn_hash_hex = txn_cid.digest_hex();
                let txn_address = match head_method.as_deref() {
                    Some(method) => fluree_db_core::storage::content_address(
                        method,
                        ContentKind::Txn,
                        ledger_id,
                        &txn_hash_hex,
                    ),
                    None => txn_address_from_commit_address(&current_address, &txn_hash_hex)
                        .ok_or_else(|| {
                            ApiError::internal(format!(
                                "failed to derive txn address from commit address '{}'",
                                current_address
                            ))
                        })?,
                };

                // Response blobs are keyed by CID string (logical identity), but bytes are read from legacy address.
                let txn_key = txn_cid.to_string();
                if let std::collections::hash_map::Entry::Vacant(e) = blobs.entry(txn_key.clone()) {
                    let txn_bytes = storage.read_bytes(&txn_address).await.map_err(|e| {
                        ApiError::internal(format!(
                            "failed to read txn blob '{}' (addr '{}'): {}",
                            txn_key, txn_address, e
                        ))
                    })?;
                    e.insert(Base64Bytes(txn_bytes));
                }
            }

            // Follow chain backward.
            match env.previous_ref {
                Some(prev) => {
                    let prev_hash_hex = prev.id.digest_hex();
                    let prev_address = match head_method.as_deref() {
                        Some(method) => fluree_db_core::storage::content_address(
                            method,
                            ContentKind::Commit,
                            ledger_id,
                            &prev_hash_hex,
                        ),
                        None => replace_hash_in_address(&current_address, &prev_hash_hex)
                            .ok_or_else(|| {
                                ApiError::internal(format!(
                                    "failed to derive previous commit address from '{}'",
                                    current_address
                                ))
                            })?,
                    };
                    next_cursor = Some(prev_address.clone());
                    next_cursor_id = Some(prev.id);
                    current_address = prev_address;
                }
                None => {
                    // Reached genesis — no more commits.
                    next_cursor = None;
                    next_cursor_id = None;
                    break;
                }
            }
        }

        // If we exited the loop by hitting the limit (not genesis), next_cursor
        // is already set to the address of the next commit to read.

        let count = commits.len();

        Ok(ExportCommitsResponse {
            ledger: ledger_id.to_string(),
            head_address,
            head_commit_id: Some(head_commit_id),
            head_t,
            commits,
            blobs,
            newest_t: newest_t.unwrap_or(0),
            oldest_t: oldest_t.unwrap_or(0),
            next_cursor,
            next_cursor_id,
            count,
            effective_limit,
        })
    }
}

// ============================================================================
// Import (client ← server)
// ============================================================================

/// Result of a bulk import (clone path — CAS writes only, no novelty).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkImportResult {
    /// Number of commit blobs written to local CAS.
    pub stored: usize,
    /// Number of txn/reference blobs written to local CAS.
    pub blobs_stored: usize,
}

/// Result of an incremental import (pull path — validated + novelty applied).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitImportResult {
    /// Number of commits imported.
    pub imported: usize,
    /// New local head `t`.
    pub head_t: i64,
    /// New local head address.
    pub head_address: String,
}

impl<S, N> Fluree<S, N>
where
    S: Storage + ContentAddressedWrite + Clone + Send + Sync + 'static,
    N: NameService + RefPublisher + Send + Sync,
{
    /// Bulk-import commit blobs to local CAS (clone path).
    ///
    /// Writes commit and txn blobs without validation or novelty updates.
    /// Order doesn't matter — CAS writes are idempotent.
    /// Call [`set_commit_head`] after all pages are imported.
    pub async fn import_commits_bulk(
        &self,
        handle: &LedgerHandle<S>,
        response: &ExportCommitsResponse,
    ) -> Result<BulkImportResult> {
        let ledger_id = handle.ledger_id();
        let storage = self.storage();
        let mut stored = 0usize;
        let mut blobs_stored = 0usize;

        // Write commit blobs to local CAS.
        //
        // The CAS hash for a commit blob is the embedded trailing hash (SHA-256 of
        // everything before the hash itself), NOT SHA-256 of the full blob.
        // We must extract it with `commit_hash_hex_from_bytes` to match the CID
        // that the remote's `previous_ref` chain references.
        for b64 in &response.commits {
            let bytes = &b64.0;
            let hash_hex = commit_hash_hex_from_bytes(bytes)
                .map_err(|e| ApiError::internal(format!("invalid commit blob in export: {}", e)))?;
            storage
                .content_write_bytes_with_hash(ContentKind::Commit, ledger_id, &hash_hex, bytes)
                .await
                .map_err(|e| ApiError::internal(format!("failed to write commit blob: {}", e)))?;
            stored += 1;
        }

        // Write referenced txn blobs to local CAS.
        for (cid_str, b64) in &response.blobs {
            let bytes = &b64.0;
            let txn_id: ContentId = cid_str.parse().map_err(|e| {
                ApiError::http(422, format!("invalid txn CID '{}': {}", cid_str, e))
            })?;
            if txn_id.codec() != CODEC_FLUREE_TXN {
                return Err(ApiError::http(
                    422,
                    format!(
                        "referenced txn CID has unexpected codec {}: {}",
                        txn_id.codec(),
                        cid_str
                    ),
                ));
            }
            // Integrity: verify bytes match claimed CID.
            if !txn_id.verify(bytes) {
                return Err(ApiError::http(
                    422,
                    format!("txn CID does not match provided bytes: {}", cid_str),
                ));
            }
            let expected_hash = txn_id.digest_hex();
            storage
                .content_write_bytes_with_hash(ContentKind::Txn, ledger_id, &expected_hash, bytes)
                .await
                .map_err(|e| ApiError::internal(format!("failed to write txn blob: {}", e)))?;
            blobs_stored += 1;
        }

        Ok(BulkImportResult {
            stored,
            blobs_stored,
        })
    }

    /// Set the commit head after bulk import (clone finalization).
    ///
    /// Points `CommitHead` at the given CID/t. Used after all pages of a
    /// bulk clone have been imported to make the ledger loadable.
    pub async fn set_commit_head(
        &self,
        handle: &LedgerHandle<S>,
        head_address: &str,
        head_t: i64,
    ) -> Result<()> {
        let ledger_id = handle.ledger_id();
        let new_ref = RefValue {
            id: extract_hash_from_address(head_address)
                .and_then(|h| ContentId::from_hex_digest(CODEC_FLUREE_COMMIT, &h)),
            t: head_t,
        };

        // Read current head for CAS.
        let current_ref = self
            .nameservice()
            .get_ref(ledger_id, RefKind::CommitHead)
            .await?;

        match self
            .nameservice()
            .compare_and_set_ref(
                ledger_id,
                RefKind::CommitHead,
                current_ref.as_ref(),
                &new_ref,
            )
            .await?
        {
            CasResult::Updated => Ok(()),
            CasResult::Conflict { actual } => Err(ApiError::http(
                409,
                format!(
                    "commit head changed during clone finalization (expected {:?}, actual {:?})",
                    current_ref, actual
                ),
            )),
        }
    }

    /// Incrementally import commits (pull path).
    ///
    /// Validates the commit chain, verifies ancestry against the local head,
    /// writes blobs to CAS, advances `CommitHead`, and updates in-memory novelty.
    ///
    /// `commits` must be ordered oldest → newest.
    pub async fn import_commits_incremental(
        &self,
        handle: &LedgerHandle<S>,
        commits: Vec<Base64Bytes>,
        blobs: HashMap<String, Base64Bytes>,
    ) -> Result<CommitImportResult> {
        if commits.is_empty() {
            return Err(ApiError::http(400, "no commits to import"));
        }

        let mut guard = handle.lock_for_write().await;
        let base_state = guard.clone_state();

        // 1) Read current head ref.
        let current_ref = self
            .nameservice()
            .get_ref(base_state.ledger_id(), RefKind::CommitHead)
            .await?;
        let Some(current_ref) = current_ref else {
            return Err(ApiError::NotFound(format!(
                "Ledger not found: {}",
                base_state.ledger_id()
            )));
        };

        // 2) Build a PushCommitsRequest-compatible structure for validation reuse.
        let request = PushCommitsRequest {
            commits,
            blobs: blobs.clone(),
        };

        // 3) Decode and validate chain.
        let decoded = decode_and_validate_commit_chain(base_state.ledger_id(), &request)
            .map_err(|e| e.into_api_error())?;

        // 4) Ancestry preflight: verify first commit's previous_ref matches local head.
        preflight_strict_next_t_and_prev(&current_ref, &decoded).map_err(|e| e.into_api_error())?;

        // 5) Validate referenced blobs are provided.
        validate_required_blobs(&decoded, &request.blobs).map_err(|e| e.into_api_error())?;

        // 6) Write blobs + commit bytes to local CAS.
        write_required_blobs(
            self.storage(),
            base_state.ledger_id(),
            &request.blobs,
            &decoded,
        )
        .await
        .map_err(|e| e.into_api_error())?;

        let stored_commits = write_commit_blobs(self.storage(), base_state.ledger_id(), &decoded)
            .await
            .map_err(|e| e.into_api_error())?;

        let final_head = stored_commits.last().expect("non-empty stored_commits");
        let new_ref = RefValue {
            id: final_head.commit_id.clone(),
            t: final_head.t,
        };

        // 7) CAS update CommitHead.
        match self
            .nameservice()
            .compare_and_set_ref(
                base_state.ledger_id(),
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
                        "commit head changed during import (expected t={}, actual={:?})",
                        current_ref.t, actual
                    ),
                ));
            }
        }

        // 8) Update in-memory state (novelty + dict novelty + namespace deltas).
        let all_flakes: Vec<(i64, Vec<Flake>)> = decoded
            .iter()
            .map(|c| {
                let mut flakes = c.commit.flakes.clone();
                let meta_flakes =
                    generate_commit_flakes(&c.commit, base_state.ledger_id(), c.commit.t);
                flakes.extend(meta_flakes);
                (c.commit.t, flakes)
            })
            .collect();

        let new_state = apply_pushed_commits_to_state(base_state, &all_flakes, &stored_commits);
        guard.replace(new_state);

        Ok(CommitImportResult {
            imported: decoded.len(),
            head_t: final_head.t,
            head_address: final_head.address.clone(),
        })
    }
}
