//! Unified, "safe" DictNovelty population helpers.
//!
//! Goal: ensure we **never** allocate a novelty ID for an entry that already exists in the
//! persisted dictionaries (`BinaryIndexStore`). This prevents multiple internal IDs from
//! decoding to the same logical IRI / string.

use crate::BinaryIndexStore;
use fluree_db_core::{DictNovelty, Flake, FlakeValue, Sid};
use std::io;

#[inline]
fn subject_is_persisted(store: &BinaryIndexStore, sid: &Sid) -> io::Result<bool> {
    if store
        .find_subject_id_by_parts(sid.namespace_code, &sid.name)?
        .is_some()
    {
        return Ok(true);
    }
    // Fallback: handle non-canonical `(ns_code, suffix)` encodings by reconstructing the full IRI.
    // Under the "ns_code invariant", this should be a fast miss and never taken.
    //
    // sid_to_s_id may fail if the namespace code was allocated after the index
    // was built (post-index allocation). In that case, the subject can't be
    // persisted in the index, so return false.
    match store.sid_to_s_id(sid) {
        Ok(found) => Ok(found.is_some()),
        Err(e) => {
            // Namespace code not in the binary store's table. Most commonly this
            // is a post-index allocation (expected), but could also indicate
            // corruption if the code should have been present.
            tracing::warn!(
                ns_code = sid.namespace_code,
                suffix = %sid.name,
                error = %e,
                "sid_to_s_id failed (post-index namespace code); treating as not persisted"
            );
            Ok(false)
        }
    }
}

#[inline]
fn string_is_persisted(store: &BinaryIndexStore, s: &str) -> io::Result<bool> {
    Ok(store.find_string_id(s)?.is_some())
}

/// Populate `DictNovelty` from a flake iterator, without shadowing persisted entries.
///
/// Contract:
/// - persisted dict wins (no novelty allocation)
/// - then novelty dict wins (no duplicate novelty allocation)
/// - then allocate
pub fn populate_dict_novelty_safe<'a>(
    dict_novelty: &mut DictNovelty,
    store: Option<&BinaryIndexStore>,
    flakes: impl IntoIterator<Item = &'a Flake>,
) -> io::Result<()> {
    dict_novelty.ensure_initialized();

    for flake in flakes {
        // Subject
        let s = &flake.s;
        let persisted = match store {
            Some(store) => subject_is_persisted(store, s)?,
            None => false,
        };
        if !persisted
            && dict_novelty
                .subjects
                .find_subject(s.namespace_code, &s.name)
                .is_none()
        {
            dict_novelty
                .subjects
                .assign_or_lookup(s.namespace_code, &s.name);
        }

        // Object references
        if let FlakeValue::Ref(ref sid) = flake.o {
            let persisted = match store {
                Some(store) => subject_is_persisted(store, sid)?,
                None => false,
            };
            if !persisted
                && dict_novelty
                    .subjects
                    .find_subject(sid.namespace_code, &sid.name)
                    .is_none()
            {
                dict_novelty
                    .subjects
                    .assign_or_lookup(sid.namespace_code, &sid.name);
            }
        }

        // String-ish
        match &flake.o {
            FlakeValue::String(s) | FlakeValue::Json(s) => {
                let persisted = match store {
                    Some(store) => string_is_persisted(store, s)?,
                    None => false,
                };
                if !persisted && dict_novelty.strings.find_string(s).is_none() {
                    dict_novelty.strings.assign_or_lookup(s);
                }
            }
            _ => {}
        }
    }

    Ok(())
}
