//! K-way merge of N sorted run file streams.
//!
//! Uses a min-heap (`BinaryHeap<Reverse<…>>`) to merge N
//! `StreamingRunReader` streams into a single globally-sorted sequence.
//! Provides both raw (`next`) and deduplicating (`next_deduped`) iteration.

use super::run_record::{RunRecord, NO_LIST_INDEX};
use super::streaming_reader::StreamingRunReader;
use super::global_dict::dt_ids;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::io;

/// Comparator function type for RunRecord ordering.
type CmpFn = fn(&RunRecord, &RunRecord) -> Ordering;

/// Entry in the min-heap: a record + which stream it came from.
struct HeapEntry {
    record: RunRecord,
    stream_idx: usize,
    cmp_fn: CmpFn,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        (self.cmp_fn)(&self.record, &other.record) == Ordering::Equal
            && self.stream_idx == other.stream_idx
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.cmp_fn)(&self.record, &other.record)
            .then(self.stream_idx.cmp(&other.stream_idx))
    }
}

/// K-way merge iterator over sorted run file streams.
///
/// Merges N `StreamingRunReader` streams using a min-heap.
/// Records emerge in the order defined by the provided comparator.
pub struct KWayMerge {
    heap: BinaryHeap<Reverse<HeapEntry>>,
    streams: Vec<StreamingRunReader>,
    cmp_fn: CmpFn,
}

impl KWayMerge {
    /// Create a merge from opened streams. Seeds the heap with the first
    /// record from each non-empty stream.
    pub fn new(streams: Vec<StreamingRunReader>, cmp_fn: CmpFn) -> io::Result<Self> {
        let mut heap = BinaryHeap::with_capacity(streams.len());

        for (idx, stream) in streams.iter().enumerate() {
            if let Some(rec) = stream.peek() {
                heap.push(Reverse(HeapEntry {
                    record: *rec,
                    stream_idx: idx,
                    cmp_fn,
                }));
            }
        }

        Ok(Self { heap, streams, cmp_fn })
    }

    /// Pop the next record in global SPOT order (no deduplication).
    pub fn next(&mut self) -> io::Result<Option<RunRecord>> {
        let entry = match self.heap.pop() {
            Some(Reverse(e)) => e,
            None => return Ok(None),
        };

        let record = entry.record;
        let idx = entry.stream_idx;

        // Advance the stream and push its next record into the heap
        self.streams[idx].advance()?;
        if let Some(next_rec) = self.streams[idx].peek() {
            self.heap.push(Reverse(HeapEntry {
                record: *next_rec,
                stream_idx: idx,
                cmp_fn: self.cmp_fn,
            }));
        }

        Ok(Some(record))
    }

    /// Pop the next deduplicated record.
    ///
    /// Conditional identity for dedup:
    /// - Base: `(g_id, s_id, p_id, o, dt)`
    /// - If `dt == LANG_STRING`: also compare `lang_id`
    /// - If `i != NO_LIST_INDEX`: also compare `i`
    ///
    /// Among records with the same identity, keeps the one with the highest `t`.
    /// Records are consumed in SPOT order (ascending `t` for same base key),
    /// so the last consumed duplicate has the highest `t`.
    pub fn next_deduped(&mut self) -> io::Result<Option<RunRecord>> {
        let mut best = match self.next()? {
            Some(r) => r,
            None => return Ok(None),
        };

        // Consume all consecutive duplicates with the same identity
        loop {
            match self.heap.peek() {
                Some(Reverse(entry)) if same_identity(&best, &entry.record) => {
                    let dup = self.next()?.unwrap();
                    // Keep the record with higher t (for same identity, later t wins)
                    if dup.t > best.t || (dup.t == best.t && dup.op >= best.op) {
                        best = dup;
                    }
                }
                _ => break,
            }
        }

        Ok(Some(best))
    }

    /// True when all streams are exhausted.
    pub fn is_exhausted(&self) -> bool {
        self.heap.is_empty()
    }
}

/// Check if two records have the same identity for dedup purposes.
///
/// Base identity: `(g_id, s_id, p_id, o, dt)`.
/// Extended with `lang_id` when `dt == LANG_STRING` (because "Alice"@en
/// and "Alice"@fr are distinct RDF literals).
/// Extended with `i` when `i != NO_LIST_INDEX` (because list entries
/// at different positions are distinct facts).
#[inline]
fn same_identity(a: &RunRecord, b: &RunRecord) -> bool {
    // Base identity
    if a.g_id != b.g_id || a.s_id != b.s_id || a.p_id != b.p_id || a.o != b.o || a.dt != b.dt {
        return false;
    }

    // Conditional: lang_id for rdf:langString
    if a.dt == dt_ids::LANG_STRING && a.lang_id != b.lang_id {
        return false;
    }

    // Conditional: list index — if either record has an index, require equality
    if (a.i != NO_LIST_INDEX || b.i != NO_LIST_INDEX) && a.i != b.i {
        return false;
    }

    true
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::global_dict::LanguageTagDict;
    use crate::run_index::run_record::cmp_spot;
    use crate::run_index::run_file::write_run_file;
    use crate::run_index::run_record::RunSortOrder;
    use fluree_db_core::value_id::ValueId;

    fn make_record(s_id: u32, p_id: u32, val: i64, t: i64) -> RunRecord {
        RunRecord::new(
            0,
            s_id,
            p_id,
            ValueId::num_int(val).unwrap(),
            t,
            true,
            dt_ids::INTEGER,
            0,
            None,
        )
    }

    fn make_lang_record(s_id: u32, p_id: u32, lex_id: u32, t: i64, lang_id: u16) -> RunRecord {
        RunRecord::new(
            0,
            s_id,
            p_id,
            ValueId::lex_id(lex_id),
            t,
            true,
            dt_ids::LANG_STRING,
            lang_id,
            None,
        )
    }

    fn make_list_record(s_id: u32, p_id: u32, val: i64, t: i64, i: i32) -> RunRecord {
        RunRecord::new(
            0,
            s_id,
            p_id,
            ValueId::num_int(val).unwrap(),
            t,
            true,
            dt_ids::INTEGER,
            0,
            Some(i),
        )
    }

    /// Write a sorted run file from records (sorts in place).
    fn write_sorted_run(dir: &std::path::Path, name: &str, mut records: Vec<RunRecord>) -> std::path::PathBuf {
        records.sort_unstable_by(cmp_spot);
        let path = dir.join(name);
        let lang_dict = LanguageTagDict::new();
        let (min_t, max_t) = records.iter().fold(
            (i64::MAX, i64::MIN),
            |(min, max), r| (min.min(r.t), max.max(r.t)),
        );
        write_run_file(
            &path,
            &records,
            &lang_dict,
            RunSortOrder::Spot,
            if records.is_empty() { 0 } else { min_t },
            if records.is_empty() { 0 } else { max_t },
        )
        .unwrap();
        path
    }

    fn open_streams(paths: &[std::path::PathBuf]) -> Vec<StreamingRunReader> {
        paths
            .iter()
            .map(|p| StreamingRunReader::open(p, vec![]).unwrap())
            .collect()
    }

    #[test]
    fn test_merge_three_streams() {
        let dir = std::env::temp_dir().join("fluree_test_merge_three");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let p0 = write_sorted_run(&dir, "r0.frn", vec![
            make_record(1, 1, 10, 1),
            make_record(3, 1, 30, 1),
            make_record(5, 1, 50, 1),
        ]);
        let p1 = write_sorted_run(&dir, "r1.frn", vec![
            make_record(2, 1, 20, 1),
            make_record(4, 1, 40, 1),
        ]);
        let p2 = write_sorted_run(&dir, "r2.frn", vec![
            make_record(6, 1, 60, 1),
        ]);

        let streams = open_streams(&[p0, p1, p2]);
        let mut merge = KWayMerge::new(streams, cmp_spot).unwrap();

        let mut results = Vec::new();
        while let Some(rec) = merge.next().unwrap() {
            results.push(rec);
        }

        assert_eq!(results.len(), 6);
        // Verify global SPOT order
        assert_eq!(results[0].s_id, 1);
        assert_eq!(results[1].s_id, 2);
        assert_eq!(results[2].s_id, 3);
        assert_eq!(results[3].s_id, 4);
        assert_eq!(results[4].s_id, 5);
        assert_eq!(results[5].s_id, 6);

        assert!(merge.is_exhausted());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_merge_dedup_same_fact_different_t() {
        let dir = std::env::temp_dir().join("fluree_test_merge_dedup_t");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Same fact (s=1, p=1, o=10) at t=1 and t=2 in different runs
        let p0 = write_sorted_run(&dir, "r0.frn", vec![
            make_record(1, 1, 10, 1),
        ]);
        let p1 = write_sorted_run(&dir, "r1.frn", vec![
            make_record(1, 1, 10, 2),
        ]);

        let streams = open_streams(&[p0, p1]);
        let mut merge = KWayMerge::new(streams, cmp_spot).unwrap();

        let mut results = Vec::new();
        while let Some(rec) = merge.next_deduped().unwrap() {
            results.push(rec);
        }

        // Should keep only t=2
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].t, 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_merge_dedup_lang_string_distinct() {
        let dir = std::env::temp_dir().join("fluree_test_merge_dedup_lang");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Same (s,p,o,dt=LANG_STRING) but different lang_id → distinct facts
        let p0 = write_sorted_run(&dir, "r0.frn", vec![
            make_lang_record(1, 1, 0, 1, 1), // lang_id=1 (e.g., "en")
            make_lang_record(1, 1, 0, 1, 2), // lang_id=2 (e.g., "fr")
        ]);

        let streams = open_streams(&[p0]);
        let mut merge = KWayMerge::new(streams, cmp_spot).unwrap();

        let mut results = Vec::new();
        while let Some(rec) = merge.next_deduped().unwrap() {
            results.push(rec);
        }

        // Both should survive — different lang_id means different facts
        assert_eq!(results.len(), 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_merge_dedup_list_index_distinct() {
        let dir = std::env::temp_dir().join("fluree_test_merge_dedup_list");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Same (s,p,o,dt) but different list index → distinct facts
        let p0 = write_sorted_run(&dir, "r0.frn", vec![
            make_list_record(1, 1, 10, 1, 0),
            make_list_record(1, 1, 10, 1, 1),
        ]);

        let streams = open_streams(&[p0]);
        let mut merge = KWayMerge::new(streams, cmp_spot).unwrap();

        let mut results = Vec::new();
        while let Some(rec) = merge.next_deduped().unwrap() {
            results.push(rec);
        }

        // Both should survive — different list index
        assert_eq!(results.len(), 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_merge_dedup_list_index_asymmetric() {
        let dir = std::env::temp_dir().join("fluree_test_merge_dedup_list_asym");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Same (s,p,o,dt) but one has list index and one doesn't → distinct facts
        let p0 = write_sorted_run(&dir, "r0.frn", vec![
            make_record(1, 1, 10, 1),            // i = NO_LIST_INDEX
            make_list_record(1, 1, 10, 1, 0),    // i = 0
        ]);

        let streams = open_streams(&[p0]);
        let mut merge = KWayMerge::new(streams, cmp_spot).unwrap();

        let mut results = Vec::new();
        while let Some(rec) = merge.next_deduped().unwrap() {
            results.push(rec);
        }

        // Both should survive — one has list index, the other doesn't
        assert_eq!(results.len(), 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_merge_empty_streams() {
        let dir = std::env::temp_dir().join("fluree_test_merge_empty");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let p0 = write_sorted_run(&dir, "r0.frn", vec![]);
        let p1 = write_sorted_run(&dir, "r1.frn", vec![make_record(1, 1, 10, 1)]);

        let streams = open_streams(&[p0, p1]);
        let mut merge = KWayMerge::new(streams, cmp_spot).unwrap();

        let rec = merge.next().unwrap().unwrap();
        assert_eq!(rec.s_id, 1);
        assert!(merge.next().unwrap().is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_merge_interleaved() {
        let dir = std::env::temp_dir().join("fluree_test_merge_interleaved");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Overlapping key ranges across streams
        let p0 = write_sorted_run(&dir, "r0.frn", vec![
            make_record(1, 1, 10, 1),
            make_record(2, 1, 20, 1),
            make_record(5, 1, 50, 1),
        ]);
        let p1 = write_sorted_run(&dir, "r1.frn", vec![
            make_record(1, 2, 15, 1),
            make_record(3, 1, 30, 1),
            make_record(4, 1, 40, 1),
        ]);

        let streams = open_streams(&[p0, p1]);
        let mut merge = KWayMerge::new(streams, cmp_spot).unwrap();

        let mut results = Vec::new();
        while let Some(rec) = merge.next().unwrap() {
            results.push(rec);
        }

        assert_eq!(results.len(), 6);
        // Verify SPOT order (s_id primary, p_id secondary)
        assert_eq!((results[0].s_id, results[0].p_id), (1, 1));
        assert_eq!((results[1].s_id, results[1].p_id), (1, 2));
        assert_eq!((results[2].s_id, results[2].p_id), (2, 1));
        assert_eq!((results[3].s_id, results[3].p_id), (3, 1));
        assert_eq!((results[4].s_id, results[4].p_id), (4, 1));
        assert_eq!((results[5].s_id, results[5].p_id), (5, 1));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
