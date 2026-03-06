//! V3 range provider — implements `RangeProvider` for V6 indexes.
//!
//! Plugs into `range_with_overlay()` so all 25+ callers (policy, SHACL,
//! reasoner, property paths, API) transparently query V3 indexes.

use std::sync::Arc;

use fluree_db_binary_index::{
    BinaryCursorV3, BinaryFilterV3, BinaryGraphViewV3, BinaryIndexStoreV6, ColumnProjection,
};
use fluree_db_core::dict_novelty::DictNovelty;
use fluree_db_core::{
    GraphId, IndexType, OverlayProvider, RangeMatch, RangeOptions, RangeProvider, RangeTest, Sid,
};

use crate::binary_scan::index_type_to_sort_order;

/// V3 range provider: wraps `BinaryIndexStoreV6` to serve `range_with_overlay()` callers.
///
/// Graph ID is passed per-call (not embedded), so one provider serves all graphs.
pub struct BinaryRangeProviderV3 {
    store: Arc<BinaryIndexStoreV6>,
    dict_novelty: Arc<DictNovelty>,
}

impl BinaryRangeProviderV3 {
    pub fn new(store: Arc<BinaryIndexStoreV6>, dict_novelty: Arc<DictNovelty>) -> Self {
        Self {
            store,
            dict_novelty,
        }
    }
}

impl RangeProvider for BinaryRangeProviderV3 {
    fn range(
        &self,
        g_id: GraphId,
        index: IndexType,
        test: RangeTest,
        match_val: &RangeMatch,
        opts: &RangeOptions,
        overlay: &dyn OverlayProvider,
    ) -> std::io::Result<Vec<fluree_db_core::Flake>> {
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
}

/// V3 equality range query: scan the appropriate index order with filters,
/// decode each row to a `Flake`, apply overlay merge.
fn binary_range_eq_v3(
    store: &Arc<BinaryIndexStoreV6>,
    dict_novelty: &Arc<DictNovelty>,
    g_id: GraphId,
    index: IndexType,
    match_val: &RangeMatch,
    opts: &RangeOptions,
    overlay: &dyn OverlayProvider,
) -> std::io::Result<Vec<fluree_db_core::Flake>> {
    let order = index_type_to_sort_order(index);
    let view = BinaryGraphViewV3::new(Arc::clone(store), g_id);

    // Build filter from bound match components.
    let mut filter = BinaryFilterV3::default();

    if let Some(s_sid) = &match_val.s {
        filter.s_id = store.sid_to_s_id(s_sid)?;
    }
    if let Some(p_sid) = &match_val.p {
        filter.p_id = store.sid_to_p_id(p_sid);
    }

    // Get branch manifest.
    let branch = match store.branch_for_order(g_id, order) {
        Some(b) => Arc::new(b.clone()),
        None => {
            // No branch for this order — return overlay-only results if any.
            return overlay_only_flakes(store, g_id, index, match_val, opts, overlay);
        }
    };

    // Create cursor: full scan with filter.
    let projection = ColumnProjection::all();
    let mut cursor = BinaryCursorV3::scan_all(Arc::clone(store), order, branch, filter, projection);

    // Apply overlay.
    let effective_to_t = opts.to_t.unwrap_or_else(|| store.max_t());
    cursor.set_to_t(effective_to_t);

    // Always attempt overlay translation — for_each_overlay_flake is a no-op
    // when the overlay is empty (NoOverlay returns immediately).
    {
        let mut ephemeral_preds = std::collections::HashMap::new();
        let mut next_ep = store.predicate_count();
        let mut ops = Vec::new();

        overlay.for_each_overlay_flake(
            g_id,
            index,
            None,
            None,
            true,
            effective_to_t,
            &mut |flake| match crate::binary_scan_v3::translate_one_flake_v3_pub(
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
            fluree_db_binary_index::read::types_v3::sort_overlay_ops_v3(&mut ops, order);
            let epoch = overlay.epoch();
            cursor.set_overlay_ops(ops);
            cursor.set_epoch(epoch);
        }
    }

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
            let s_sid = resolve_sid(s_id, store, dict_novelty)?;
            // Resolve predicate.
            let p_sid = match store.resolve_predicate_iri(p_id) {
                Some(iri) => store.encode_iri(iri),
                None => continue, // ephemeral predicate from overlay — skip in flake output
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

/// Resolve a subject integer ID to Sid using persisted dict then DictNovelty.
fn resolve_sid(
    s_id: u64,
    store: &BinaryIndexStoreV6,
    dict_novelty: &Arc<DictNovelty>,
) -> std::io::Result<Sid> {
    // Try persisted first.
    match store.resolve_subject_iri(s_id) {
        Ok(iri) => Ok(store.encode_iri(&iri)),
        Err(_) => {
            // Try DictNovelty forward lookup: returns (ns_code, suffix).
            if dict_novelty.is_initialized() {
                if let Some((ns_code, suffix)) = dict_novelty.subjects.resolve_subject(s_id) {
                    return Ok(Sid::new(ns_code, suffix));
                }
            }
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("subject s_id={s_id} not found in persisted or novelty dict"),
            ))
        }
    }
}

/// Overlay-only results when no branch exists for the requested order.
///
/// Collects flakes directly from the overlay provider, applies match filtering
/// and options (offset/limit). Used at genesis or before first indexing when
/// no persisted branch exists for the requested sort order.
fn overlay_only_flakes(
    store: &Arc<BinaryIndexStoreV6>,
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
