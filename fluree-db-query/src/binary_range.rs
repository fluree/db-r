//! Binary index range query shim.
//!
//! Provides `range()` semantics using `BinaryCursor` against the binary
//! columnar index.  This is the binary equivalent of
//! `fluree_db_core::range::range_with_overlay()`.
//!
//! ## Design
//!
//! - Sync (local mmap reads via `BinaryCursor`)
//! - Converts `RangeMatch` → integer-ID bounds via `BinaryIndexStore::translate_range`
//! - Iterates `DecodedBatch` leaves and reconstructs `Vec<Flake>`
//! - Supports overlay via `DictOverlay` (ephemeral IDs for uncommitted entities)
//!
//! ## Supported tests
//!
//! Currently supports `RangeTest::Eq` (the only variant used by the reasoner
//! and the majority of callers).  Inequality tests (Lt, Le, Gt, Ge) and
//! bounded-range queries will be added when callers migrate from the b-tree
//! `range()` path.

use crate::binary_scan::index_type_to_sort_order;
use crate::dict_overlay::DictOverlay;
use fluree_db_core::dict_novelty::DictNovelty;
use fluree_db_core::range::{ObjectBounds, RangeMatch, RangeOptions, RangeTest};
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::ObjKind;
use fluree_db_core::ListIndex;
use fluree_db_core::{Flake, IndexType, OverlayProvider, RangeProvider, Sid};
use fluree_db_indexer::run_index::run_record::{RunRecord, RunSortOrder};
use fluree_db_indexer::run_index::{
    sort_overlay_ops, BinaryCursor, BinaryFilter, BinaryIndexStore, DecodedBatch,
};
use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;

// ============================================================================
// Public API
// ============================================================================

/// Execute a range query against the binary columnar index.
///
/// This is the binary equivalent of `fluree_db_core::range_with_overlay()`.
/// Returns `Vec<Flake>` in index order, matching the same contract as the
/// b-tree `range()` functions.
///
/// # Arguments
///
/// * `store` — loaded `BinaryIndexStore` (shared, immutable)
/// * `g_id` — graph ID (typically 0 for default graph)
/// * `index` — which index order to scan
/// * `test` — comparison operator (currently only `Eq` is supported)
/// * `match_val` — components to match
/// * `opts` — query options (limit, time bounds)
/// * `overlay` — optional overlay provider + DictOverlay for novelty merge
///
/// # Errors
///
/// Returns `io::Error` on I/O failures or if the match components cannot be
/// translated to integer IDs (e.g., unknown subject/predicate).
pub fn binary_range(
    store: &Arc<BinaryIndexStore>,
    g_id: u32,
    index: IndexType,
    test: RangeTest,
    match_val: &RangeMatch,
    opts: &RangeOptions,
    overlay: Option<(&dyn OverlayProvider, &mut DictOverlay)>,
) -> io::Result<Vec<Flake>> {
    match test {
        RangeTest::Eq => binary_range_eq(store, g_id, index, match_val, opts, overlay),
        _ => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("binary_range: RangeTest::{test:?} not yet supported (only Eq)"),
        )),
    }
}

// ============================================================================
// Internals
// ============================================================================

/// Batched lookup for ref-valued predicate objects across many subjects.
///
/// This is primarily used for fast `rdf:type` lookups in policy enforcement and
/// for class→property stats refresh.
#[allow(clippy::too_many_arguments)]
fn binary_lookup_subject_predicate_refs_batched(
    store: &Arc<BinaryIndexStore>,
    g_id: u32,
    index: IndexType,
    predicate: &Sid,
    subjects: &[Sid],
    opts: &RangeOptions,
    overlay: &dyn OverlayProvider,
    dict_novelty: &Arc<DictNovelty>,
) -> io::Result<HashMap<Sid, Vec<Sid>>> {
    if index != IndexType::Psot {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "batched predicate+subject lookup currently supports PSOT only",
        ));
    }

    if subjects.is_empty() {
        return Ok(HashMap::new());
    }

    // Per-call DictOverlay for ephemeral subject handling + overlay translation.
    let mut dict_ov = DictOverlay::new(store.clone(), dict_novelty.clone());

    // Predicate ID (supports ephemeral predicates, though rdf:type is always persisted).
    let p_id = dict_ov.assign_predicate_id_from_sid(predicate);

    // Translate subjects to s_id and build s_id -> Sid map.
    let mut s_ids: Vec<u64> = Vec::with_capacity(subjects.len());
    let mut s_id_to_sid: HashMap<u64, Sid> = HashMap::with_capacity(subjects.len());
    for sid in subjects {
        let s_id = dict_ov.assign_subject_id_from_sid(sid)?;
        s_ids.push(s_id);
        // Preserve the original Sid to avoid resolve cost and to keep exact identity.
        s_id_to_sid.entry(s_id).or_insert_with(|| sid.clone());
    }
    if s_ids.is_empty() {
        return Ok(HashMap::new());
    }
    s_ids.sort_unstable();
    s_ids.dedup();

    let min_s_id = s_ids[0];
    let max_s_id = *s_ids.last().unwrap();

    // Cursor bounds: PSOT key interval restricted to [min_s_id, max_s_id] within this predicate.
    let min_key = RunRecord {
        g_id,
        s_id: SubjectId::from_u64(min_s_id),
        p_id,
        dt: 0,
        o_kind: 0,
        op: 0,
        o_key: 0,
        t: i64::MIN,
        lang_id: 0,
        i: ListIndex::none().as_i32(),
    };
    let max_key = RunRecord {
        g_id,
        s_id: SubjectId::from_u64(max_s_id),
        p_id,
        dt: u16::MAX,
        o_kind: u8::MAX,
        op: u8::MAX,
        o_key: u64::MAX,
        t: i64::MAX,
        lang_id: u16::MAX,
        i: i32::MAX,
    };

    // Filter: keep the predicate fixed. Subject set filtering happens post-decode.
    let mut filter = BinaryFilter::new();
    filter.p_id = Some(p_id);

    // Region 2 is needed for time filtering / stale removal at `to_t`.
    let need_region2 = true;
    let order = RunSortOrder::Psot;

    let mut cursor = BinaryCursor::new(
        store.clone(),
        order,
        g_id,
        &min_key,
        &max_key,
        filter,
        need_region2,
    );

    // Time-travel: constrain to_t if set, otherwise use store max.
    let effective_to_t = opts.to_t.unwrap_or_else(|| store.max_t());
    cursor.set_to_t(effective_to_t);

    // Overlay merge: translate overlay flakes to integer-ID overlay ops, sort to PSOT, attach.
    let overlay_ops =
        crate::binary_scan::translate_overlay_flakes(overlay, &mut dict_ov, effective_to_t, g_id);
    if !overlay_ops.is_empty() {
        cursor.set_epoch(overlay.epoch());
        let mut sorted = overlay_ops;
        sort_overlay_ops(&mut sorted, order);
        cursor.set_overlay_ops(sorted);
    }

    // Membership filter for s_id (fast O(1)).
    let s_id_set: HashSet<u64> = s_ids.into_iter().collect();

    // Collect results.
    let mut out: HashMap<Sid, Vec<Sid>> = HashMap::new();

    while let Some(batch) = cursor.next_leaf()? {
        // NOTE: DecodedBatch rows are already filtered to `to_t` and have overlay merged.
        for i in 0..batch.row_count {
            let s_id = batch.s_ids[i];
            if !s_id_set.contains(&s_id) {
                continue;
            }
            if batch.p_ids[i] != p_id {
                continue;
            }
            if batch.o_kinds[i] != ObjKind::REF_ID.as_u8() {
                continue;
            }

            // Subject Sid: prefer original, otherwise resolve via DictOverlay.
            let subj_sid = match s_id_to_sid.get(&s_id) {
                Some(s) => s.clone(),
                None => dict_ov.resolve_subject_sid(s_id)?,
            };
            let class_sid = dict_ov.resolve_subject_sid(batch.o_keys[i])?;

            out.entry(subj_sid).or_default().push(class_sid);
        }
    }

    // Dedup class vectors per subject for stable policy semantics.
    for classes in out.values_mut() {
        classes.sort();
        classes.dedup();
    }

    Ok(out)
}

/// Equality range query: all flakes matching the bound components exactly.
fn binary_range_eq(
    store: &Arc<BinaryIndexStore>,
    g_id: u32,
    index: IndexType,
    match_val: &RangeMatch,
    opts: &RangeOptions,
    overlay: Option<(&dyn OverlayProvider, &mut DictOverlay)>,
) -> io::Result<Vec<Flake>> {
    let order = index_type_to_sort_order(index);

    // Translate match components to integer-ID bounds.
    let bounds = store.translate_range(
        match_val.s.as_ref(),
        match_val.p.as_ref(),
        match_val.o.as_ref(),
        order,
        g_id,
    )?;

    let (min_key, max_key) = match bounds {
        Some(b) => b,
        None => return Ok(Vec::new()), // untranslatable → no results
    };

    // Build filter for exact-match post-filtering within leaves.
    let mut filter = BinaryFilter::new();
    if let Some(ref s) = match_val.s {
        if let Some(s_id) = store.sid_to_s_id(s)? {
            filter.s_id = Some(s_id);
        }
    }
    if let Some(ref p) = match_val.p {
        if let Some(p_id) = store.sid_to_p_id(p) {
            filter.p_id = Some(p_id);
        }
    }
    // Object filter: only for exact-match (Eq) queries with a bound object.
    if let Some(ref o) = match_val.o {
        if let Ok(Some((ok, okey))) = store.value_to_obj_pair(o) {
            filter.o_kind = Some(ok.as_u8());
            filter.o_key = Some(okey.as_u64());
        }
    }

    // Region 2 is needed for Flake reconstruction (t, dt, lang, list_index).
    let need_region2 = true;

    let mut cursor = BinaryCursor::new(
        store.clone(),
        order,
        g_id,
        &min_key,
        &max_key,
        filter,
        need_region2,
    );

    // Time-travel: if opts.to_t is set, constrain the cursor.
    let effective_to_t = opts.to_t.unwrap_or_else(|| store.max_t());
    if opts.to_t.is_some() {
        cursor.set_to_t(effective_to_t);
    }

    // Overlay merge.
    if let Some((ovl, dict_ov)) = overlay {
        let overlay_ops =
            crate::binary_scan::translate_overlay_flakes(ovl, dict_ov, effective_to_t, g_id);
        if !overlay_ops.is_empty() {
            cursor.set_epoch(ovl.epoch());

            // Sort overlay ops to match the cursor's sort order.
            let mut sorted = overlay_ops;
            sort_overlay_ops(&mut sorted, order);
            cursor.set_overlay_ops(sorted);
        }
    }

    // Iterate leaves and collect Flakes.
    //
    // NOTE: `RangeOptions` has both `limit` (subject limit) and `flake_limit`.
    // Historically, the binary shim treated `limit` as a flake cap. To preserve
    // existing behavior while supporting callers that correctly use `flake_limit`
    // (e.g., ISO time resolution), we apply:
    //   effective_flake_limit = flake_limit.or(limit).unwrap_or(usize::MAX)
    let effective_flake_limit = opts.flake_limit.or(opts.limit).unwrap_or(usize::MAX);
    let bounds = opts.object_bounds.as_ref();
    let mut offset_remaining = opts.offset.unwrap_or(0);
    let mut flakes = Vec::new();

    while let Some(batch) = cursor.next_leaf()? {
        decode_batch_to_flakes_filtered(
            store,
            &batch,
            &mut flakes,
            bounds,
            &mut offset_remaining,
            effective_flake_limit,
        )?;
        if flakes.len() >= effective_flake_limit {
            flakes.truncate(effective_flake_limit);
            break;
        }
    }

    Ok(flakes)
}

/// Convert a `DecodedBatch` into `Vec<Flake>` by resolving integer IDs back
/// to Sid/FlakeValue.
fn decode_batch_to_flakes_filtered(
    store: &BinaryIndexStore,
    batch: &DecodedBatch,
    out: &mut Vec<Flake>,
    bounds: Option<&ObjectBounds>,
    offset_remaining: &mut usize,
    flake_limit: usize,
) -> io::Result<()> {
    let dt_sids = store.dt_sids();

    for i in 0..batch.row_count {
        let s_id = batch.s_ids[i];
        let p_id = batch.p_ids[i];
        let o_kind = batch.o_kinds[i];
        let o_key = batch.o_keys[i];
        let dt_id = batch.dt_values[i];
        let t = batch.t_values[i];
        let lang_id = batch.lang_ids[i];
        let i_val = batch.i_values[i];

        // s_id → Sid
        let s_iri = store.resolve_subject_iri(s_id)?;
        let s_sid = store.encode_iri(&s_iri);

        // p_id → Sid
        let p_iri = store.resolve_predicate_iri(p_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown predicate ID {p_id}"),
            )
        })?;
        let p_sid = store.encode_iri(p_iri);

        // (o_kind, o_key) → FlakeValue
        let o_val = store.decode_value(o_kind, o_key, p_id)?;

        // Optional post-filter for object bounds (RangeOptions semantics).
        if let Some(b) = bounds {
            if !b.matches(&o_val) {
                continue;
            }
        }

        // dt_id → Sid for datatype
        let dt = dt_sids
            .get(dt_id as usize)
            .cloned()
            .unwrap_or_else(Sid::min);

        // Language tag + list index → FlakeMeta
        let meta = store.decode_meta(lang_id, i_val);

        // Offset applies to the filtered stream (RangeOptions semantics).
        if *offset_remaining > 0 {
            *offset_remaining -= 1;
            continue;
        }

        out.push(Flake::new(s_sid, p_sid, o_val, dt, t, true, meta));

        // Stop early once we've reached the caller's flake limit.
        if out.len() >= flake_limit {
            break;
        }
    }

    Ok(())
}

// ============================================================================
// RangeProvider implementation
// ============================================================================

/// Binary columnar index implementation of `RangeProvider`.
///
/// Wraps a `BinaryIndexStore` and a default graph ID to serve range queries
/// from the binary index.  When attached to a `Db` via
/// `db.with_range_provider()`, all callers of `range_with_overlay()` —
/// including the reasoner, API, policy, and SHACL crates — automatically
/// use the binary index without code changes.
pub struct BinaryRangeProvider {
    store: Arc<BinaryIndexStore>,
    dict_novelty: Arc<DictNovelty>,
    g_id: u32,
}

impl BinaryRangeProvider {
    /// Create a new provider for the given store, dict novelty, and default graph.
    pub fn new(store: Arc<BinaryIndexStore>, dict_novelty: Arc<DictNovelty>, g_id: u32) -> Self {
        Self {
            store,
            dict_novelty,
            g_id,
        }
    }
}

impl RangeProvider for BinaryRangeProvider {
    fn range(
        &self,
        index: IndexType,
        test: RangeTest,
        match_val: &RangeMatch,
        opts: &RangeOptions,
        overlay: &dyn OverlayProvider,
    ) -> io::Result<Vec<Flake>> {
        // Create a per-call DictOverlay for ephemeral ID handling.
        let mut dict_ov = DictOverlay::new(self.store.clone(), self.dict_novelty.clone());

        // Always pass the overlay — translate_overlay_flakes handles empty
        // overlays by returning an empty vec, which is a no-op.
        binary_range(
            &self.store,
            self.g_id,
            index,
            test,
            match_val,
            opts,
            Some((overlay, &mut dict_ov)),
        )
    }

    fn lookup_subject_predicate_refs_batched(
        &self,
        index: IndexType,
        predicate: &Sid,
        subjects: &[Sid],
        opts: &RangeOptions,
        overlay: &dyn OverlayProvider,
    ) -> io::Result<HashMap<Sid, Vec<Sid>>> {
        binary_lookup_subject_predicate_refs_batched(
            &self.store,
            self.g_id,
            index,
            predicate,
            subjects,
            opts,
            overlay,
            &self.dict_novelty,
        )
    }
}
