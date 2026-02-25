//! Batched PSOT lookup for ref-valued predicate objects across many subjects.
//!
//! Used by the incremental stats pipeline to efficiently query base-root
//! class membership (rdf:type assertions) for subjects touched by novelty.
//! Instead of one PSOT cursor per subject, this module scans contiguous
//! subject-ID ranges in bulk, filtering to the requested subject set
//! in-memory.
//!
//! Mirrors Clojure's `batched-get-subject-classes` strategy.

use super::binary_cursor::{BinaryCursor, BinaryFilter};
use super::binary_index_store::BinaryIndexStore;
use crate::format::run_record::{RunRecord, RunSortOrder};
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::ObjKind;
use fluree_db_core::GraphId;
use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;

/// Batched PSOT lookup for ref-valued predicate objects across many subjects.
///
/// Returns `HashMap<sid64_subject, Vec<sid64_class>>` of current assertions
/// from the persisted index at `to_t`. No overlay/novelty merge -- caller
/// applies novelty deltas separately.
///
/// Mirrors Clojure's `batched-get-subject-classes` strategy: one streaming
/// pass over PSOT bounded by the subject range, filtering to the requested
/// subject set in-memory.
///
/// For sparse subject ranges, chunks the scan to avoid scanning large gaps.
pub fn batched_lookup_predicate_refs(
    store: &Arc<BinaryIndexStore>,
    g_id: GraphId,
    p_id: u32,
    subjects: &[u64],
    to_t: i64,
) -> io::Result<HashMap<u64, Vec<u64>>> {
    if subjects.is_empty() {
        return Ok(HashMap::new());
    }

    let mut sorted_subjects = subjects.to_vec();
    sorted_subjects.sort_unstable();
    sorted_subjects.dedup();

    let s_id_set: HashSet<u64> = sorted_subjects.iter().copied().collect();
    let mut out: HashMap<u64, Vec<u64>> = HashMap::new();

    // Chunk by subject-ID span to avoid scanning huge gaps.
    // If subjects are dense (within MAX_SPAN), process as one chunk.
    // Otherwise, break into sub-ranges.
    const MAX_SPAN: u64 = 100_000; // Subject IDs within 100K -> one cursor pass
    const MAX_CHUNK: usize = 1000; // At most 1000 subjects per chunk

    let chunks = chunk_subjects(&sorted_subjects, MAX_SPAN, MAX_CHUNK);

    let ref_kind = ObjKind::REF_ID.as_u8();

    for chunk in &chunks {
        let min_s = chunk[0];
        let max_s = *chunk.last().unwrap();

        let min_key = RunRecord {
            g_id,
            s_id: SubjectId::from_u64(min_s),
            p_id,
            dt: 0,
            o_kind: 0,
            op: 0,
            o_key: 0,
            t: 0,
            lang_id: 0,
            i: 0,
        };
        let max_key = RunRecord {
            g_id,
            s_id: SubjectId::from_u64(max_s),
            p_id,
            dt: u16::MAX,
            o_kind: u8::MAX,
            op: u8::MAX,
            o_key: u64::MAX,
            t: u32::MAX,
            lang_id: u16::MAX,
            i: u32::MAX,
        };

        let mut filter = BinaryFilter::new();
        filter.p_id = Some(p_id);

        let mut cursor = BinaryCursor::new(
            store.clone(),
            RunSortOrder::Psot,
            g_id,
            &min_key,
            &max_key,
            filter,
            true, // need_region2 for time filtering
        );
        cursor.set_to_t(to_t);

        while let Some(batch) = cursor.next_leaf()? {
            for i in 0..batch.row_count {
                let s_id = batch.s_ids[i];
                if !s_id_set.contains(&s_id) {
                    continue;
                }
                if batch.p_ids[i] != p_id {
                    continue;
                }
                if batch.o_kinds[i] != ref_kind {
                    continue;
                }
                out.entry(s_id).or_default().push(batch.o_keys[i]);
            }
        }
    }

    // Dedup class vectors.
    for classes in out.values_mut() {
        classes.sort_unstable();
        classes.dedup();
    }

    Ok(out)
}

/// Break sorted subjects into chunks where each chunk spans at most
/// `max_span` IDs and contains at most `max_chunk` subjects.
fn chunk_subjects(sorted: &[u64], max_span: u64, max_chunk: usize) -> Vec<&[u64]> {
    if sorted.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut start = 0;
    for i in 1..sorted.len() {
        let span = sorted[i] - sorted[start];
        let size = i - start;
        if span > max_span || size >= max_chunk {
            chunks.push(&sorted[start..i]);
            start = i;
        }
    }
    chunks.push(&sorted[start..]);
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_subjects_empty() {
        let result = chunk_subjects(&[], 100, 10);
        assert!(result.is_empty());
    }

    #[test]
    fn chunk_subjects_single() {
        let subjects = [42];
        let result = chunk_subjects(&subjects, 100, 10);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], &[42]);
    }

    #[test]
    fn chunk_subjects_dense() {
        let subjects: Vec<u64> = (100..110).collect();
        let result = chunk_subjects(&subjects, 100, 1000);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 10);
    }

    #[test]
    fn chunk_subjects_sparse() {
        // Two clusters far apart
        let mut subjects = vec![100, 101, 102, 200_000, 200_001, 200_002];
        subjects.sort_unstable();
        let result = chunk_subjects(&subjects, 1_000, 1000);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], &[100, 101, 102]);
        assert_eq!(result[1], &[200_000, 200_001, 200_002]);
    }

    #[test]
    fn chunk_subjects_max_chunk_size() {
        // 5 subjects, max_chunk=2
        let subjects: Vec<u64> = (0..5).collect();
        let result = chunk_subjects(&subjects, u64::MAX, 2);
        assert_eq!(result.len(), 3); // [0,1], [2,3], [4]
        assert_eq!(result[0], &[0, 1]);
        assert_eq!(result[1], &[2, 3]);
        assert_eq!(result[2], &[4]);
    }
}
