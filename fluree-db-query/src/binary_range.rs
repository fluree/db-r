//! V3 range provider — implements `RangeProvider` for V6 indexes.
//!
//! Plugs into `range_with_overlay()` so all 25+ callers (policy, SHACL,
//! reasoner, property paths, API) transparently query V3 indexes.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_binary_index::{
    BinaryCursor, BinaryFilter, BinaryGraphView, BinaryIndexStore, ColumnProjection, RunSortOrder,
};
use fluree_db_core::dict_novelty::DictNovelty;
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::{
    Flake, GraphId, IndexType, OType, OverlayProvider, RangeMatch, RangeOptions, RangeProvider,
    RangeTest, Sid,
};

use crate::binary_scan::index_type_to_sort_order;

/// Try persisted lookup first, then DictNovelty. Returns `None` if neither resolves.
fn resolve_or_novelty<T>(
    persisted: Option<T>,
    dict_novelty: &DictNovelty,
    novelty_lookup: impl FnOnce() -> Option<T>,
) -> Option<T> {
    match persisted {
        Some(id) => Some(id),
        None if dict_novelty.is_initialized() => novelty_lookup(),
        None => None,
    }
}

/// V3 range provider: wraps `BinaryIndexStore` to serve `range_with_overlay()` callers.
///
/// Graph ID is passed per-call (not embedded), so one provider serves all graphs.
pub struct BinaryRangeProvider {
    store: Arc<BinaryIndexStore>,
    dict_novelty: Arc<DictNovelty>,
}

impl BinaryRangeProvider {
    pub fn new(store: Arc<BinaryIndexStore>, dict_novelty: Arc<DictNovelty>) -> Self {
        Self {
            store,
            dict_novelty,
        }
    }

    /// Access the underlying `BinaryIndexStore`.
    pub fn store(&self) -> &Arc<BinaryIndexStore> {
        &self.store
    }

    /// Access the `DictNovelty` used for overlay decoding.
    pub fn dict_novelty(&self) -> &Arc<DictNovelty> {
        &self.dict_novelty
    }
}

impl RangeProvider for BinaryRangeProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn range(
        &self,
        g_id: GraphId,
        index: IndexType,
        test: RangeTest,
        match_val: &RangeMatch,
        opts: &RangeOptions,
        overlay: &dyn OverlayProvider,
    ) -> std::io::Result<Vec<Flake>> {
        match test {
            RangeTest::Eq => binary_range_eq_v3(
                &self.store,
                &self.dict_novelty,
                g_id,
                index,
                match_val,
                opts,
                overlay,
            ),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                format!("V3 range provider: unsupported RangeTest {:?}", test),
            )),
        }
    }

    fn range_bounded(
        &self,
        g_id: GraphId,
        index: IndexType,
        start_bound: &Flake,
        end_bound: &Flake,
        opts: &RangeOptions,
        overlay: &dyn OverlayProvider,
    ) -> std::io::Result<Vec<Flake>> {
        binary_range_bounded_v3(
            &self.store,
            &self.dict_novelty,
            g_id,
            index,
            start_bound,
            end_bound,
            opts,
            overlay,
        )
    }

    fn lookup_subject_predicate_refs_batched(
        &self,
        g_id: GraphId,
        index: IndexType,
        predicate: &Sid,
        subjects: &[Sid],
        opts: &RangeOptions,
        overlay: &dyn OverlayProvider,
    ) -> std::io::Result<HashMap<Sid, Vec<Sid>>> {
        binary_lookup_subject_predicate_refs_batched_v3(
            &self.store,
            &self.dict_novelty,
            g_id,
            index,
            predicate,
            subjects,
            opts,
            overlay,
        )
    }
}

/// V3 equality range query: scan the appropriate index order with filters,
/// decode each row to a `Flake`, apply overlay merge.
fn binary_range_eq_v3(
    store: &Arc<BinaryIndexStore>,
    dict_novelty: &Arc<DictNovelty>,
    g_id: GraphId,
    index: IndexType,
    match_val: &RangeMatch,
    opts: &RangeOptions,
    overlay: &dyn OverlayProvider,
) -> std::io::Result<Vec<fluree_db_core::Flake>> {
    let order = index_type_to_sort_order(index);
    let view =
        BinaryGraphView::with_novelty(Arc::clone(store), g_id, Some(Arc::clone(dict_novelty)));

    // Build filter from bound match components.
    let mut filter = BinaryFilter::default();

    if let Some(s_sid) = &match_val.s {
        // Prefer persisted reverse dict, then DictNovelty. If neither can map
        // this subject to an s_id, there are no base rows to scan; return
        // overlay-only matches.
        match resolve_or_novelty(store.sid_to_s_id(s_sid)?, dict_novelty, || {
            dict_novelty
                .subjects
                .find_subject(s_sid.namespace_code, &s_sid.name)
        }) {
            Some(id) => filter.s_id = Some(id),
            None => return overlay_only_flakes(store, g_id, index, match_val, opts, overlay),
        }
    }
    if let Some(p_sid) = &match_val.p {
        match store.sid_to_p_id(p_sid) {
            Some(id) => filter.p_id = Some(id),
            None => {
                // Unknown predicate in persisted dict: base scan cannot match.
                // Overlay may still contain this predicate (novelty), so return overlay-only.
                return overlay_only_flakes(store, g_id, index, match_val, opts, overlay);
            }
        }
    }
    if let Some(o_val) = &match_val.o {
        match o_val {
            fluree_db_core::FlakeValue::Ref(sid) => {
                // Resolve ref object to an s_id (persisted → DictNovelty).
                let o_id = match resolve_or_novelty(store.sid_to_s_id(sid)?, dict_novelty, || {
                    dict_novelty
                        .subjects
                        .find_subject(sid.namespace_code, &sid.name)
                }) {
                    Some(id) => id,
                    None => {
                        return overlay_only_flakes(store, g_id, index, match_val, opts, overlay)
                    }
                };
                filter.o_type = Some(OType::IRI_REF.as_u16());
                filter.o_key = Some(o_id);
            }
            fluree_db_core::FlakeValue::String(s) => {
                // Resolve string dict id (persisted → DictNovelty).
                let str_id =
                    match resolve_or_novelty(store.find_string_id(s)?, dict_novelty, || {
                        dict_novelty.strings.find_string(s)
                    }) {
                        Some(id) => id,
                        None => {
                            return overlay_only_flakes(
                                store, g_id, index, match_val, opts, overlay,
                            )
                        }
                    };
                filter.o_type = Some(OType::XSD_STRING.as_u16());
                filter.o_key = Some(str_id as u64);
            }
            fluree_db_core::FlakeValue::Json(s) => {
                // JSON values share the string dictionary but use OType::RDF_JSON.
                // Same persisted → DictNovelty resolution as strings.
                let str_id =
                    match resolve_or_novelty(store.find_string_id(s)?, dict_novelty, || {
                        dict_novelty.strings.find_string(s)
                    }) {
                        Some(id) => id,
                        None => {
                            return overlay_only_flakes(
                                store, g_id, index, match_val, opts, overlay,
                            )
                        }
                    };
                filter.o_type = Some(OType::RDF_JSON.as_u16());
                filter.o_key = Some(str_id as u64);
            }
            _ => {
                // Best-effort: encode basic inline types for equality filtering.
                if let Ok((ot, key)) = crate::binary_scan::value_to_otype_okey_simple(o_val, store)
                {
                    filter.o_type = Some(ot.as_u16());
                    filter.o_key = Some(key);
                }
            }
        }
    }

    // Get branch manifest.
    let branch = match store.branch_for_order(g_id, order) {
        Some(b) => Arc::new(b.clone()),
        None => {
            // No branch for this order — return overlay-only results if any.
            return overlay_only_flakes(store, g_id, index, match_val, opts, overlay);
        }
    };

    // Create cursor: use range-narrowed scan when any filter field is bound,
    // matching the pattern in BinaryScanOperator::open. For novelty-only subjects
    // this yields an empty leaf_range, so the cursor drains overlay ops directly
    // with zero leaf I/O.
    let projection = ColumnProjection::all();
    let use_range = filter.s_id.is_some()
        || filter.p_id.is_some()
        || filter.o_type.is_some()
        || filter.o_key.is_some();

    let mut cursor = if use_range {
        let min_key = RunRecordV2 {
            s_id: SubjectId(filter.s_id.unwrap_or(0)),
            o_key: filter.o_key.unwrap_or(0),
            p_id: filter.p_id.unwrap_or(0),
            t: 0,
            o_i: 0,
            o_type: filter.o_type.unwrap_or(0),
            g_id,
        };
        let max_key = RunRecordV2 {
            s_id: SubjectId(filter.s_id.unwrap_or(u64::MAX)),
            o_key: filter.o_key.unwrap_or(u64::MAX),
            p_id: filter.p_id.unwrap_or(u32::MAX),
            t: u32::MAX,
            o_i: u32::MAX,
            o_type: filter.o_type.unwrap_or(u16::MAX),
            g_id,
        };
        BinaryCursor::new(
            Arc::clone(store),
            order,
            branch,
            &min_key,
            &max_key,
            filter,
            projection,
        )
    } else {
        BinaryCursor::scan_all(Arc::clone(store), order, branch, filter, projection)
    };

    // Apply overlay.
    let effective_to_t = opts.to_t.unwrap_or_else(|| store.max_t());
    cursor.set_to_t(effective_to_t);

    // Always attempt overlay translation — for_each_overlay_flake is a no-op
    // when the overlay is empty (NoOverlay returns immediately).
    let mut ephemeral_preds = std::collections::HashMap::new();
    let mut next_ep = store.predicate_count();
    {
        let mut ops = Vec::new();

        overlay.for_each_overlay_flake(
            g_id,
            index,
            None,
            None,
            true,
            effective_to_t,
            &mut |flake| match crate::binary_scan::translate_one_flake_v3_pub(
                flake,
                store,
                Some(dict_novelty),
                &mut ephemeral_preds,
                &mut next_ep,
            ) {
                Ok(op) => ops.push(op),
                Err(e) => {
                    tracing::warn!(error = %e, "V3 range: failed to translate overlay flake");
                }
            },
        );

        if !ops.is_empty() {
            fluree_db_binary_index::read::types::sort_overlay_ops(&mut ops, order);
            let epoch = overlay.epoch();
            cursor.set_overlay_ops(ops);
            cursor.set_epoch(epoch);
        }
    }

    // Build reverse map for novelty-only predicates: ephemeral p_id → Sid.
    // These predicates don't exist in the persisted index dictionary and need
    // this map to be decoded back to IRIs when cursor rows are materialized.
    let ephemeral_p_id_to_sid: HashMap<u32, Sid> = ephemeral_preds
        .into_iter()
        .map(|(iri, id)| (id, store.encode_iri(&iri)))
        .collect();

    // Iterate and decode to Flakes.
    let limit = opts.flake_limit.or(opts.limit).unwrap_or(usize::MAX);
    let offset = opts.offset.unwrap_or(0);
    let mut flakes = Vec::new();
    let mut skipped = 0usize;

    while let Some(batch) = cursor.next_batch()? {
        for i in 0..batch.row_count {
            let s_id = batch.s_id.get(i);
            let p_id = batch.p_id.get_or(i, 0);
            let o_type = batch.o_type.get_or(i, 0);
            let o_key = batch.o_key.get(i);
            let t = batch.t.get_or(i, 0) as i64;
            let o_i = batch.o_i.get_or(i, u32::MAX);

            // Resolve subject.
            let s_sid = resolve_sid(s_id, &view)?;
            // Resolve predicate: persisted dict first, then ephemeral overlay map.
            let p_sid = match store.resolve_predicate_iri(p_id) {
                Some(iri) => store.encode_iri(iri),
                None => match ephemeral_p_id_to_sid.get(&p_id) {
                    Some(sid) => sid.clone(),
                    None => continue, // truly unknown — shouldn't happen
                },
            };
            // Decode object.
            let o_val = view.decode_value(o_type, o_key, p_id)?;
            // Resolve datatype.
            let dt = store
                .resolve_datatype_sid(o_type)
                .unwrap_or_else(|| Sid::new(0, ""));
            // Language tag.
            let lang = store.resolve_lang_tag(o_type).map(|s| s.to_string());
            // List index.
            let meta = if lang.is_some() || o_i != u32::MAX {
                Some(fluree_db_core::FlakeMeta {
                    lang,
                    i: if o_i != u32::MAX {
                        Some(o_i as i32)
                    } else {
                        None
                    },
                })
            } else {
                None
            };

            // Apply object bounds if present.
            if let Some(bounds) = &opts.object_bounds {
                if !bounds.matches(&o_val) {
                    continue;
                }
            }

            // Offset.
            if skipped < offset {
                skipped += 1;
                continue;
            }

            flakes.push(fluree_db_core::Flake {
                g: None,
                s: s_sid,
                p: p_sid,
                o: o_val,
                dt,
                t,
                op: true,
                m: meta,
            });

            if flakes.len() >= limit {
                return Ok(flakes);
            }
        }
    }

    Ok(flakes)
}

/// Resolve a subject integer ID to Sid.
///
/// Delegates to `BinaryGraphView::resolve_subject_sid` which handles
/// watermark-based novelty routing internally: novel subjects return
/// `Sid::new(ns_code, suffix)` directly (no IRI string + trie lookup).
#[inline]
fn resolve_sid(s_id: u64, view: &BinaryGraphView) -> std::io::Result<Sid> {
    view.resolve_subject_sid(s_id)
}

/// Batched lookup for ref-valued predicate objects across many subjects (V3).
///
/// For a fixed predicate, scans PSOT within the `[min_s_id, max_s_id]` range,
/// filters to the requested subject set, and returns only IRI-ref-typed objects.
/// Used by policy (`rdf:type` lookups) and stats refresh.
#[allow(clippy::too_many_arguments)]
fn binary_lookup_subject_predicate_refs_batched_v3(
    store: &Arc<BinaryIndexStore>,
    dict_novelty: &Arc<DictNovelty>,
    g_id: GraphId,
    index: IndexType,
    predicate: &Sid,
    subjects: &[Sid],
    opts: &RangeOptions,
    overlay: &dyn OverlayProvider,
) -> std::io::Result<HashMap<Sid, Vec<Sid>>> {
    if index != IndexType::Psot {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "V3 batched predicate+subject lookup currently supports PSOT only",
        ));
    }

    if subjects.is_empty() {
        return Ok(HashMap::new());
    }

    let view =
        BinaryGraphView::with_novelty(Arc::clone(store), g_id, Some(Arc::clone(dict_novelty)));

    // Resolve predicate.
    let p_id = match store.sid_to_p_id(predicate) {
        Some(id) => id,
        None => return Ok(HashMap::new()), // unknown predicate → no results
    };

    // Translate subjects to s_id and build s_id → Sid map.
    let mut s_ids: Vec<u64> = Vec::with_capacity(subjects.len());
    let mut s_id_to_sid: HashMap<u64, Sid> = HashMap::with_capacity(subjects.len());
    for sid in subjects {
        if let Ok(Some(s_id)) = store.sid_to_s_id(sid) {
            s_id_to_sid.entry(s_id).or_insert_with(|| sid.clone());
            s_ids.push(s_id);
        } else if dict_novelty.is_initialized() {
            // Try DictNovelty for uncommitted subjects.
            if let Some(s_id) = dict_novelty
                .subjects
                .find_subject(sid.namespace_code, &sid.name)
            {
                s_id_to_sid.entry(s_id).or_insert_with(|| sid.clone());
                s_ids.push(s_id);
            }
        }
    }
    if s_ids.is_empty() {
        return Ok(HashMap::new());
    }
    s_ids.sort_unstable();
    s_ids.dedup();

    let min_s_id = s_ids[0];
    let max_s_id = *s_ids.last().unwrap();

    // PSOT key bounds: restrict to [min_s_id, max_s_id] within this predicate.
    let min_key = RunRecordV2 {
        s_id: SubjectId::from_u64(min_s_id),
        o_key: 0,
        p_id,
        t: 0,
        o_i: 0,
        o_type: 0,
        g_id,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId::from_u64(max_s_id),
        o_key: u64::MAX,
        p_id,
        t: 0,
        o_i: u32::MAX,
        o_type: u16::MAX,
        g_id,
    };

    // Get branch manifest.
    let branch = match store.branch_for_order(g_id, RunSortOrder::Psot) {
        Some(b) => Arc::new(b.clone()),
        None => {
            // No PSOT branch — try overlay only.
            return batched_refs_overlay_only(
                store,
                dict_novelty,
                g_id,
                predicate,
                subjects,
                opts,
                overlay,
            );
        }
    };

    let filter = BinaryFilter {
        p_id: Some(p_id),
        ..Default::default()
    };

    let projection = ColumnProjection::all();
    let mut cursor = BinaryCursor::new(
        Arc::clone(store),
        RunSortOrder::Psot,
        branch,
        &min_key,
        &max_key,
        filter,
        projection,
    );

    let effective_to_t = opts.to_t.unwrap_or_else(|| store.max_t());
    cursor.set_to_t(effective_to_t);

    // Overlay merge — pre-filter to avoid translating irrelevant flakes.
    // Only translate flakes that match the target predicate and subject set.
    {
        let subject_sid_set: HashSet<&Sid> = subjects.iter().collect();
        let mut ephemeral_preds = HashMap::new();
        let mut next_ep = store.predicate_count();
        let mut ops = Vec::new();

        overlay.for_each_overlay_flake(
            g_id,
            IndexType::Psot,
            None,
            None,
            true,
            effective_to_t,
            &mut |flake| {
                // Pre-filter: skip flakes for other predicates or other subjects.
                if flake.p != *predicate {
                    return;
                }
                if !subject_sid_set.contains(&flake.s) {
                    return;
                }

                match crate::binary_scan::translate_one_flake_v3_pub(
                    flake,
                    store,
                    Some(dict_novelty),
                    &mut ephemeral_preds,
                    &mut next_ep,
                ) {
                    Ok(op) => ops.push(op),
                    Err(e) => {
                        tracing::warn!(error = %e, "V3 batched refs: failed to translate overlay flake");
                    }
                }
            },
        );

        if !ops.is_empty() {
            fluree_db_binary_index::read::types::sort_overlay_ops(&mut ops, RunSortOrder::Psot);
            cursor.set_overlay_ops(ops);
            cursor.set_epoch(overlay.epoch());
        }
    }

    // Membership filter for s_id (fast O(1)).
    let s_id_set: HashSet<u64> = s_ids.into_iter().collect();
    let iri_ref_o_type = OType::IRI_REF.as_u16();

    let mut out: HashMap<Sid, Vec<Sid>> = HashMap::new();

    while let Some(batch) = cursor.next_batch()? {
        for i in 0..batch.row_count {
            let s_id = batch.s_id.get(i);
            if !s_id_set.contains(&s_id) {
                continue;
            }

            let o_type = batch.o_type.get_or(i, 0);
            if o_type != iri_ref_o_type {
                continue;
            }

            let o_key = batch.o_key.get(i);

            // Subject Sid: prefer the original input Sid.
            let subj_sid = match s_id_to_sid.get(&s_id) {
                Some(s) => s.clone(),
                None => resolve_sid(s_id, &view)?,
            };

            // Resolve object (IRI ref) to Sid.
            let class_sid = resolve_sid(o_key, &view)?;

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

/// Overlay-only fallback for batched ref lookup when no PSOT branch exists.
#[allow(clippy::too_many_arguments)]
fn batched_refs_overlay_only(
    store: &Arc<BinaryIndexStore>,
    dict_novelty: &Arc<DictNovelty>,
    g_id: GraphId,
    predicate: &Sid,
    subjects: &[Sid],
    opts: &RangeOptions,
    overlay: &dyn OverlayProvider,
) -> std::io::Result<HashMap<Sid, Vec<Sid>>> {
    let effective_to_t = opts.to_t.unwrap_or_else(|| store.max_t());
    let subject_set: HashSet<&Sid> = subjects.iter().collect();
    let iri_ref_o_type = OType::IRI_REF.as_u16();
    let view =
        BinaryGraphView::with_novelty(Arc::clone(store), g_id, Some(Arc::clone(dict_novelty)));

    let mut out: HashMap<Sid, Vec<Sid>> = HashMap::new();

    let mut ephemeral_preds = HashMap::new();
    let mut next_ep = store.predicate_count();

    overlay.for_each_overlay_flake(
        g_id,
        IndexType::Psot,
        None,
        None,
        true,
        effective_to_t,
        &mut |flake| {
            if !flake.op {
                return;
            }
            if !subject_set.contains(&flake.s) {
                return;
            }
            if flake.p != *predicate {
                return;
            }

            // Translate to check o_type.
            if let Ok(op) = crate::binary_scan::translate_one_flake_v3_pub(
                flake,
                store,
                Some(dict_novelty),
                &mut ephemeral_preds,
                &mut next_ep,
            ) {
                if op.o_type != iri_ref_o_type {
                    return;
                }
                // Resolve the class Sid.
                if let Ok(class_sid) = resolve_sid(op.o_key, &view) {
                    out.entry(flake.s.clone()).or_default().push(class_sid);
                }
            }
        },
    );

    for classes in out.values_mut() {
        classes.sort();
        classes.dedup();
    }

    Ok(out)
}

/// Bounded range query: scan between `start_bound` and `end_bound` in index order.
///
/// Used for subject-range queries (e.g., SHA prefix scans in `time_resolve`).
/// Currently only supports SPOT index order.
///
/// Since subject s_ids are NOT in IRI lexicographic order (they're assigned in
/// first-seen/insertion order), we cannot simply create a bounded SPOT cursor
/// between two s_ids. Instead, we:
/// 1. Use the reverse subject tree to find all persisted subjects whose suffix
///    falls in the [start_name, end_name) range within the namespace.
/// 2. Also collect overlay subjects matching the prefix (so novelty-only subjects
///    are not dropped when persisted matches exist).
/// 3. Build a HashSet of matching s_ids, create a SPOT cursor bounded to
///    [min_s_id, max_s_id] for leaf selection, then post-filter rows.
#[allow(clippy::too_many_arguments)]
fn binary_range_bounded_v3(
    store: &Arc<BinaryIndexStore>,
    dict_novelty: &Arc<DictNovelty>,
    g_id: GraphId,
    index: IndexType,
    start_bound: &Flake,
    end_bound: &Flake,
    opts: &RangeOptions,
    overlay: &dyn OverlayProvider,
) -> std::io::Result<Vec<Flake>> {
    // Guard: range_bounded is designed for SPOT subject-prefix scans.
    if index != IndexType::Spot {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("V3 range_bounded: only SPOT is supported, got {:?}", index),
        ));
    }

    let order = index_type_to_sort_order(index);
    let ns_code = start_bound.s.namespace_code;
    let start_name: &str = &start_bound.s.name;
    let end_name: &str = &end_bound.s.name;
    let effective_to_t = opts.to_t.unwrap_or_else(|| store.max_t());

    // Step 1: Find persisted subjects in the IRI prefix range via reverse tree.
    let matching_s_ids = store.find_subjects_by_prefix(ns_code, start_name)?;
    let mut s_id_set: HashSet<u64> = matching_s_ids.into_iter().collect();

    // Step 2: Translate overlay flakes and collect novelty-only subject s_ids
    // that match the prefix range. This ensures uncommitted subjects aren't
    // dropped when persisted matches also exist.
    let mut ephemeral_preds = HashMap::new();
    let mut next_ep = store.predicate_count();
    let mut overlay_ops = Vec::new();

    overlay.for_each_overlay_flake(
        g_id,
        index,
        None,
        None,
        true,
        effective_to_t,
        &mut |flake| {
            // Check if overlay subject matches the prefix range.
            if flake.s.namespace_code == ns_code {
                let name: &str = &flake.s.name;
                if name < start_name || name >= end_name {
                    return;
                }
            } else {
                return;
            }

            match crate::binary_scan::translate_one_flake_v3_pub(
                flake,
                store,
                Some(dict_novelty),
                &mut ephemeral_preds,
                &mut next_ep,
            ) {
                Ok(op) => {
                    // Add novelty-only subject s_ids to the filter set.
                    s_id_set.insert(op.s_id);
                    overlay_ops.push(op);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "V3 range_bounded: failed to translate overlay flake");
                }
            }
        },
    );

    // Build reverse map for novelty-only predicates: ephemeral p_id → Sid.
    let ephemeral_p_id_to_sid: HashMap<u32, Sid> = ephemeral_preds
        .into_iter()
        .map(|(iri, id)| (id, store.encode_iri(&iri)))
        .collect();

    if s_id_set.is_empty() {
        // No matching subjects at all (persisted or overlay).
        return Ok(Vec::new());
    }

    let branch = match store.branch_for_order(g_id, order) {
        Some(b) => Arc::new(b.clone()),
        None => {
            // No SPOT branch — return overlay-only results (already translated above).
            return overlay_only_flakes_bounded(
                store,
                g_id,
                index,
                start_bound,
                end_bound,
                opts,
                overlay,
            );
        }
    };

    // Compute s_id bounds for leaf selection (narrows the leaf range).
    let min_s_id = *s_id_set.iter().min().unwrap();
    let max_s_id = *s_id_set.iter().max().unwrap();

    let min_key = RunRecordV2 {
        s_id: SubjectId::from_u64(min_s_id),
        o_key: 0,
        p_id: 0,
        t: 0,
        o_i: 0,
        o_type: 0,
        g_id,
    };
    let max_key = RunRecordV2 {
        s_id: SubjectId::from_u64(max_s_id),
        o_key: u64::MAX,
        p_id: u32::MAX,
        t: 0,
        o_i: u32::MAX,
        o_type: u16::MAX,
        g_id,
    };

    let filter = BinaryFilter::default();
    let projection = ColumnProjection::all();
    let mut cursor = BinaryCursor::new(
        Arc::clone(store),
        order,
        branch,
        &min_key,
        &max_key,
        filter,
        projection,
    );

    cursor.set_to_t(effective_to_t);

    // Attach pre-translated overlay ops.
    if !overlay_ops.is_empty() {
        fluree_db_binary_index::read::types::sort_overlay_ops(&mut overlay_ops, order);
        cursor.set_overlay_ops(overlay_ops);
        cursor.set_epoch(overlay.epoch());
    }

    let view =
        BinaryGraphView::with_novelty(Arc::clone(store), g_id, Some(Arc::clone(dict_novelty)));
    let limit = opts.flake_limit.or(opts.limit).unwrap_or(usize::MAX);
    let offset = opts.offset.unwrap_or(0);
    let mut flakes = Vec::new();
    let mut skipped = 0usize;

    while let Some(batch) = cursor.next_batch()? {
        for i in 0..batch.row_count {
            let s_id = batch.s_id.get(i);

            // Post-filter: only accept rows for subjects in our prefix range.
            if !s_id_set.contains(&s_id) {
                continue;
            }

            let p_id = batch.p_id.get_or(i, 0);
            let o_type = batch.o_type.get_or(i, 0);
            let o_key = batch.o_key.get(i);
            let t = batch.t.get_or(i, 0) as i64;
            let o_i = batch.o_i.get_or(i, u32::MAX);

            let s_sid = resolve_sid(s_id, &view)?;

            // Double-check the subject name is in [start_name, end_name).
            if s_sid.namespace_code == ns_code {
                let name: &str = &s_sid.name;
                if name < start_name || name >= end_name {
                    continue;
                }
            }

            // Resolve predicate: persisted dict first, then ephemeral overlay map.
            let p_sid = match store.resolve_predicate_iri(p_id) {
                Some(iri) => store.encode_iri(iri),
                None => match ephemeral_p_id_to_sid.get(&p_id) {
                    Some(sid) => sid.clone(),
                    None => continue, // truly unknown — shouldn't happen
                },
            };
            let o_val = view.decode_value(o_type, o_key, p_id)?;
            let dt = store
                .resolve_datatype_sid(o_type)
                .unwrap_or_else(|| Sid::new(0, ""));
            let lang = store.resolve_lang_tag(o_type).map(|s| s.to_string());
            let meta = if lang.is_some() || o_i != u32::MAX {
                Some(fluree_db_core::FlakeMeta {
                    lang,
                    i: if o_i != u32::MAX {
                        Some(o_i as i32)
                    } else {
                        None
                    },
                })
            } else {
                None
            };

            if let Some(bounds) = &opts.object_bounds {
                if !bounds.matches(&o_val) {
                    continue;
                }
            }

            if skipped < offset {
                skipped += 1;
                continue;
            }

            flakes.push(Flake {
                g: None,
                s: s_sid,
                p: p_sid,
                o: o_val,
                dt,
                t,
                op: true,
                m: meta,
            });

            if flakes.len() >= limit {
                return Ok(flakes);
            }
        }
    }

    Ok(flakes)
}

/// Overlay-only path for range_bounded when no branch exists.
#[allow(clippy::too_many_arguments)]
fn overlay_only_flakes_bounded(
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    index: IndexType,
    start_bound: &Flake,
    end_bound: &Flake,
    opts: &RangeOptions,
    overlay: &dyn OverlayProvider,
) -> std::io::Result<Vec<Flake>> {
    let effective_to_t = opts.to_t.unwrap_or_else(|| store.max_t());
    let limit = opts.flake_limit.or(opts.limit).unwrap_or(usize::MAX);
    let offset = opts.offset.unwrap_or(0);

    let mut skipped = 0usize;
    let mut collected = 0usize;
    let mut flakes = Vec::new();

    overlay.for_each_overlay_flake(
        g_id,
        index,
        None,
        None,
        true,
        effective_to_t,
        &mut |flake| {
            if collected >= limit {
                return;
            }
            if !flake.op {
                return;
            }

            // Check subject bounds: start_bound.s <= flake.s < end_bound.s.
            if flake.s < start_bound.s || flake.s >= end_bound.s {
                return;
            }

            if let Some(ref bounds) = opts.object_bounds {
                if !bounds.matches(&flake.o) {
                    return;
                }
            }

            if skipped < offset {
                skipped += 1;
                return;
            }

            flakes.push(flake.clone());
            collected += 1;
        },
    );

    Ok(flakes)
}

/// Overlay-only results when no branch exists for the requested order.
///
/// Collects flakes directly from the overlay provider, applies match filtering
/// and options (offset/limit). Used at genesis or before first indexing when
/// no persisted branch exists for the requested sort order.
fn overlay_only_flakes(
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    index: IndexType,
    match_val: &RangeMatch,
    opts: &RangeOptions,
    overlay: &dyn OverlayProvider,
) -> std::io::Result<Vec<fluree_db_core::Flake>> {
    let effective_to_t = opts.to_t.unwrap_or_else(|| store.max_t());
    let limit = opts.flake_limit.or(opts.limit).unwrap_or(usize::MAX);
    let offset = opts.offset.unwrap_or(0);

    // Use Cell for early-exit: once we've collected offset+limit, stop cloning.
    let mut skipped = 0usize;
    let mut collected = 0usize;
    let mut flakes = Vec::new();

    overlay.for_each_overlay_flake(
        g_id,
        index,
        None,
        None,
        true,
        effective_to_t,
        &mut |flake| {
            // Early exit: already have enough results.
            if collected >= limit {
                return;
            }

            // Only include asserts (op=true).
            if !flake.op {
                return;
            }

            // Filter by match components.
            if let Some(ref s_sid) = match_val.s {
                if flake.s != *s_sid {
                    return;
                }
            }
            if let Some(ref p_sid) = match_val.p {
                if flake.p != *p_sid {
                    return;
                }
            }
            if let Some(ref o_val) = match_val.o {
                if flake.o != *o_val {
                    return;
                }
            }

            // Apply object bounds (same as persisted path).
            if let Some(ref bounds) = opts.object_bounds {
                if !bounds.matches(&flake.o) {
                    return;
                }
            }

            // Apply offset.
            if skipped < offset {
                skipped += 1;
                return;
            }

            flakes.push(flake.clone());
            collected += 1;
        },
    );

    Ok(flakes)
}
