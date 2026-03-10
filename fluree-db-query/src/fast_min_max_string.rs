//! Fast-path: `MIN(?o)` / `MAX(?o)` for a single triple `?s <p> ?o` where `?o` is string-dict-backed.
//!
//! QLever answers these kinds of aggregates by exploiting permutation order and metadata
//! to avoid scanning all rows. For Fluree's V3 index, we can do something similar:
//! - scan only the POST leaflet directory entries for the predicate
//! - consider each leaflet's first_key (for MIN) or last_key (for MAX) as a candidate
//! - pick the global min/max using the same ordering we apply for `Binding::EncodedLit`
//!
//! This reduces work from O(rows) dictionary operations to O(leaflets) key decoding.

use crate::binding::{Batch, Binding};
use crate::error::{QueryError, Result};
use crate::fast_path_common::{
    fast_path_store, leaf_entries_for_predicate, normalize_pred_sid, FastPathOperator,
};
use crate::operator::BoxedOperator;
use crate::triple::Ref;
use crate::var_registry::VarId;
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::read_ordered_key_v2;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::ids::DatatypeDictId;
use fluree_db_core::o_type::OType;
use fluree_db_core::value_id::ObjKind;
use fluree_db_core::GraphId;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MinMaxMode {
    Min,
    Max,
}

/// Create a fused operator that outputs a single-row batch containing the MIN/MAX result.
pub fn predicate_min_max_string_operator(
    predicate: Ref,
    mode: MinMaxMode,
    out_var: VarId,
    fallback: Option<BoxedOperator>,
) -> FastPathOperator {
    FastPathOperator::new(
        out_var,
        move |ctx| {
            let Some(store) = fast_path_store(ctx) else {
                return Ok(None);
            };
            let pred_sid = normalize_pred_sid(store, &predicate)?;
            let Some(p_id) = store.sid_to_p_id(&pred_sid) else {
                // Predicate absent -> empty input -> aggregate result is unbound.
                let batch = Batch::single_row(
                    Arc::from(vec![out_var].into_boxed_slice()),
                    vec![Binding::Unbound],
                )
                .map_err(|e| QueryError::execution(format!("min/max batch build: {e}")))?;
                return Ok(Some(batch));
            };

            if let Some(b) = minmax_string_dict_post(store, ctx.binary_g_id, p_id, mode)? {
                let batch = Batch::single_row(Arc::from(vec![out_var].into_boxed_slice()), vec![b])
                    .map_err(|e| QueryError::execution(format!("min/max batch build: {e}")))?;
                return Ok(Some(batch));
            }
            // Unsupported at runtime (mixed non-string objects) — fall through to planned pipeline.
            Ok(None)
        },
        fallback,
        "MIN/MAX string",
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EncodedStringIdentity {
    /// String dictionary ID (NOT lexicographically ordered).
    str_id: u32,
    /// Datatype identity for ordering/equality (xsd:string vs rdf:langString vs fulltext).
    dt_id: u16,
    /// Language identity for rdf:langString (0 for non-langString).
    lang_id: u16,
}

fn encoded_lit_from_otype(
    o_type: u16,
    o_key: u64,
    p_id: u32,
) -> Option<(EncodedStringIdentity, Binding)> {
    let ot = OType::from_u16(o_type);
    if ot.decode_kind() != fluree_db_core::o_type::DecodeKind::StringDict {
        return None;
    }
    let str_id = u32::try_from(o_key).ok()?;

    let (dt_id, lang_id) = if ot.is_lang_string() {
        (DatatypeDictId::LANG_STRING.as_u16(), ot.payload())
    } else if o_type == OType::FULLTEXT.as_u16() {
        (DatatypeDictId::FULL_TEXT.as_u16(), 0)
    } else {
        // Default string dict values to xsd:string to match late-materialization behavior.
        (DatatypeDictId::STRING.as_u16(), 0)
    };

    let ident = EncodedStringIdentity {
        str_id,
        dt_id,
        lang_id,
    };
    let b = Binding::EncodedLit {
        o_kind: ObjKind::LEX_ID.as_u8(),
        o_key,
        p_id,
        dt_id,
        lang_id,
        i_val: i32::MIN,
        t: 0,
    };
    Some((ident, b))
}

/// Compute MIN/MAX candidate for a predicate by scanning POST leaflets and considering
/// only directory keys (first/last key per leaflet).
///
/// Returns `None` when leaflets contain non-string objects (to avoid semantic surprises).
fn minmax_string_dict_post(
    store: &BinaryIndexStore,
    g_id: GraphId,
    p_id: u32,
    mode: MinMaxMode,
) -> Result<Option<Binding>> {
    let leaves = leaf_entries_for_predicate(store, g_id, RunSortOrder::Post, p_id);

    let mut best: Option<(EncodedStringIdentity, Binding)> = None;

    for leaf_entry in leaves {
        let handle = store
            .open_leaf_handle(&leaf_entry.leaf_cid, leaf_entry.sidecar_cid.as_ref(), false)
            .map_err(|e| QueryError::Internal(format!("leaf open: {e}")))?;
        let dir = handle.dir();

        for entry in &dir.entries {
            if entry.row_count == 0 || entry.p_const != Some(p_id) {
                continue;
            }
            let rr = match mode {
                MinMaxMode::Min => read_ordered_key_v2(RunSortOrder::Post, &entry.first_key),
                MinMaxMode::Max => read_ordered_key_v2(RunSortOrder::Post, &entry.last_key),
            };
            let Some(candidate) = encoded_lit_from_otype(rr.o_type, rr.o_key, p_id) else {
                return Ok(None);
            };

            match &best {
                None => best = Some(candidate),
                Some((best_id, _)) => {
                    // We can compare lexicographically without materialization *only*
                    // when both candidates share the same datatype+lang identity.
                    if candidate.0.dt_id != best_id.dt_id || candidate.0.lang_id != best_id.lang_id
                    {
                        return Ok(None);
                    }
                    let ord = store
                        .compare_string_lex(candidate.0.str_id, best_id.str_id)
                        .map_err(|e| QueryError::Internal(format!("compare string lex: {e}")))?;
                    let better = match mode {
                        MinMaxMode::Min => ord.is_lt(),
                        MinMaxMode::Max => ord.is_gt(),
                    };
                    if better {
                        best = Some(candidate);
                    }
                }
            }
        }
    }

    Ok(best.map(|(_, b)| b))
}
