//! K-way merge of N sorted streams.
//!
//! Uses a manual min-heap to merge N streams into a single globally-sorted
//! sequence. Provides both raw (`next_record`) and deduplicating
//! (`next_deduped`) iteration.
//!
//! Generic over [`MergeSource`] — works with both [`StreamingRunReader`]
//! (34-byte run files) and [`StreamingSortedCommitReader`] (36-byte sorted
//! commit files with on-the-fly remap).
//!
//! The comparator `F` is a generic type parameter, enabling the compiler to
//! monomorphize each instantiation and inline the comparator in heap
//! operations. This eliminates indirect function-pointer calls on the hot
//! path (~2.7 billion comparisons for 543M records).

use super::run_record::{RunRecord, LIST_INDEX_NONE};
use super::streaming_reader::StreamingRunReader;
use fluree_db_core::DatatypeDictId;
use std::cmp::Ordering;
use std::io;

// ============================================================================
// MergeSource trait
// ============================================================================

/// Trait for buffered, forward-only record streams that can be k-way merged.
///
/// Implemented by [`StreamingRunReader`] (run files) and
/// [`StreamingSortedCommitReader`] (sorted commit files with remap).
pub trait MergeSource {
    /// Peek at the current record without advancing. Returns `None` if exhausted.
    fn peek(&self) -> Option<&RunRecord>;

    /// Advance to the next record, refilling the internal buffer from disk
    /// if needed.
    fn advance(&mut self) -> io::Result<()>;

    /// True when all records have been consumed.
    fn is_exhausted(&self) -> bool;
}

impl MergeSource for StreamingRunReader {
    #[inline]
    fn peek(&self) -> Option<&RunRecord> {
        StreamingRunReader::peek(self)
    }

    fn advance(&mut self) -> io::Result<()> {
        StreamingRunReader::advance(self)
    }

    fn is_exhausted(&self) -> bool {
        StreamingRunReader::is_exhausted(self)
    }
}

/// Comparator function type for RunRecord ordering (backward-compat alias).
pub type CmpFn = fn(&RunRecord, &RunRecord) -> Ordering;

// ============================================================================
// HeapEntry (no comparator — comparator lives on KWayMerge)
// ============================================================================

/// Entry in the min-heap: a record + which stream it came from.
///
/// 48 bytes: RunRecord (40) + stream_idx (8). No comparator stored per-entry.
struct HeapEntry {
    record: RunRecord,
    stream_idx: usize,
}

// ============================================================================
// KWayMerge — manual min-heap with generic comparator
// ============================================================================

/// K-way merge iterator over sorted streams.
///
/// Merges N [`MergeSource`] streams using a manual min-heap with a
/// monomorphized comparator. Records emerge in the order defined by `F`.
pub struct KWayMerge<T: MergeSource, F: Fn(&RunRecord, &RunRecord) -> Ordering> {
    heap: Vec<HeapEntry>,
    streams: Vec<T>,
    cmp: F,
}

impl<T: MergeSource, F: Fn(&RunRecord, &RunRecord) -> Ordering> KWayMerge<T, F> {
    /// Create a merge from opened streams. Seeds the heap with the first
    /// record from each non-empty stream.
    pub fn new(streams: Vec<T>, cmp: F) -> io::Result<Self> {
        let cap = streams.len();
        let mut heap = Vec::with_capacity(cap);

        for (idx, stream) in streams.iter().enumerate() {
            if let Some(rec) = stream.peek() {
                heap.push(HeapEntry {
                    record: *rec,
                    stream_idx: idx,
                });
            }
        }

        let mut me = Self { heap, streams, cmp };

        // Build-heap: heapify from the last internal node down to root.
        if me.heap.len() > 1 {
            let last_internal = (me.heap.len() / 2).saturating_sub(1);
            for i in (0..=last_internal).rev() {
                me.sift_down(i);
            }
        }

        Ok(me)
    }

    // ---- Manual min-heap operations ----

    /// Compare two heap entries: first by record comparator, then stream_idx.
    #[inline]
    fn heap_less(&self, i: usize, j: usize) -> bool {
        let ord = (self.cmp)(&self.heap[i].record, &self.heap[j].record);
        match ord {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => self.heap[i].stream_idx < self.heap[j].stream_idx,
        }
    }

    /// Sift an element down to its correct position (restore heap after root replacement).
    #[inline]
    fn sift_down(&mut self, mut pos: usize) {
        let len = self.heap.len();
        loop {
            let left = 2 * pos + 1;
            if left >= len {
                break;
            }
            let right = left + 1;
            let mut smallest = left;
            if right < len && self.heap_less(right, left) {
                smallest = right;
            }
            if self.heap_less(pos, smallest) || !self.heap_less(smallest, pos) {
                // pos <= smallest (equal counts as "no swap needed")
                break;
            }
            self.heap.swap(pos, smallest);
            pos = smallest;
        }
    }

    /// Sift an element up to its correct position (restore heap after push).
    #[inline]
    fn sift_up(&mut self, mut pos: usize) {
        while pos > 0 {
            let parent = (pos - 1) / 2;
            if !self.heap_less(pos, parent) {
                break;
            }
            self.heap.swap(pos, parent);
            pos = parent;
        }
    }

    /// Pop the minimum element from the heap.
    fn heap_pop(&mut self) -> Option<HeapEntry> {
        if self.heap.is_empty() {
            return None;
        }
        let last = self.heap.len() - 1;
        self.heap.swap(0, last);
        let entry = self.heap.pop().unwrap();
        if !self.heap.is_empty() {
            self.sift_down(0);
        }
        Some(entry)
    }

    /// Push an element onto the heap.
    fn heap_push(&mut self, entry: HeapEntry) {
        self.heap.push(entry);
        let pos = self.heap.len() - 1;
        self.sift_up(pos);
    }

    // ---- Public API ----

    /// Pop the next record in merge order (no deduplication).
    pub fn next_record(&mut self) -> io::Result<Option<RunRecord>> {
        let entry = match self.heap_pop() {
            Some(e) => e,
            None => return Ok(None),
        };

        let record = entry.record;
        let idx = entry.stream_idx;

        // Advance the stream and push its next record into the heap
        self.streams[idx].advance()?;
        if let Some(next_rec) = self.streams[idx].peek() {
            self.heap_push(HeapEntry {
                record: *next_rec,
                stream_idx: idx,
            });
        }

        Ok(Some(record))
    }

    /// Pop the next deduplicated record.
    ///
    /// Conditional identity for dedup:
    /// - Base: `(s_id, p_id, o, dt)`
    /// - If `dt == LANG_STRING`: also compare `lang_id`
    /// - If `i != LIST_INDEX_NONE`: also compare `i`
    ///
    /// Among records with the same identity, keeps the one with the highest `t`.
    /// Records are consumed in SPOT order (ascending `t` for same base key),
    /// so the last consumed duplicate has the highest `t`.
    pub fn next_deduped(&mut self) -> io::Result<Option<RunRecord>> {
        let mut best = match self.next_record()? {
            Some(r) => r,
            None => return Ok(None),
        };

        // Consume all consecutive duplicates with the same identity
        loop {
            match self.heap.first() {
                Some(entry) if same_identity(&best, &entry.record) => {
                    let dup = self.next_record()?.unwrap();
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

    /// Pop the next deduplicated record, collecting non-winning duplicates.
    ///
    /// Works like [`next_deduped`] but also captures all consumed non-winning
    /// records into `history`. The caller must clear `history` before each
    /// call (the method does NOT clear it, allowing pre-allocated reuse).
    ///
    /// For single-version facts (no duplicates), `history` remains empty.
    /// For multi-version facts, `history` contains the older/losing records
    /// (intermediate asserts, retractions) — everything except the winner.
    pub fn next_deduped_with_history(
        &mut self,
        history: &mut Vec<RunRecord>,
    ) -> io::Result<Option<RunRecord>> {
        let mut best = match self.next_record()? {
            Some(r) => r,
            None => return Ok(None),
        };

        // Consume all consecutive duplicates with the same identity
        loop {
            match self.heap.first() {
                Some(entry) if same_identity(&best, &entry.record) => {
                    let dup = self.next_record()?.unwrap();
                    // Keep the record with higher t (for same identity, later t wins)
                    if dup.t > best.t || (dup.t == best.t && dup.op >= best.op) {
                        history.push(best); // old best becomes history
                        best = dup;
                    } else {
                        history.push(dup); // dup is older, goes to history
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
/// Base identity: `(s_id, p_id, o, dt)`. `g_id` is NOT part of identity —
/// each graph is indexed independently, so all records in a merge stream
/// share the same graph.
///
/// Extended with `lang_id` when `dt == LANG_STRING` (because "Alice"@en
/// and "Alice"@fr are distinct RDF literals).
/// Extended with `i` when `i != LIST_INDEX_NONE` (because list entries
/// at different positions are distinct facts).
#[inline]
fn same_identity(a: &RunRecord, b: &RunRecord) -> bool {
    // Base identity (no g_id — graph is implicit)
    if a.s_id != b.s_id
        || a.p_id != b.p_id
        || a.o_kind != b.o_kind
        || a.o_key != b.o_key
        || a.dt != b.dt
    {
        return false;
    }

    // Conditional: lang_id for rdf:langString
    if a.dt == DatatypeDictId::LANG_STRING.as_u16() && a.lang_id != b.lang_id {
        return false;
    }

    // Conditional: list index — if either record has an index, require equality
    if (a.i != LIST_INDEX_NONE || b.i != LIST_INDEX_NONE) && a.i != b.i {
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
    use crate::run_index::run_file::write_run_file;
    use crate::run_index::run_record::cmp_spot;
    use crate::run_index::run_record::RunSortOrder;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::{ObjKey, ObjKind};

    fn make_record(s_id: u64, p_id: u32, val: i64, t: u32) -> RunRecord {
        RunRecord::new(
            0,
            SubjectId::from_u64(s_id),
            p_id,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(val),
            t,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        )
    }

    fn make_lang_record(s_id: u64, p_id: u32, lex_id: u32, t: u32, lang_id: u16) -> RunRecord {
        RunRecord::new(
            0,
            SubjectId::from_u64(s_id),
            p_id,
            ObjKind::LEX_ID,
            ObjKey::encode_u32_id(lex_id),
            t,
            true,
            DatatypeDictId::LANG_STRING.as_u16(),
            lang_id,
            None,
        )
    }

    fn make_list_record(s_id: u64, p_id: u32, val: i64, t: u32, i: u32) -> RunRecord {
        RunRecord::new(
            0,
            SubjectId::from_u64(s_id),
            p_id,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(val),
            t,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            Some(i),
        )
    }

    /// Write a sorted run file from records (sorts in place).
    fn write_sorted_run(
        dir: &std::path::Path,
        name: &str,
        mut records: Vec<RunRecord>,
    ) -> std::path::PathBuf {
        records.sort_unstable_by(cmp_spot);
        let path = dir.join(name);
        let lang_dict = LanguageTagDict::new();
        let (min_t, max_t) = records
            .iter()
            .fold((u32::MAX, 0u32), |(min, max), r| (min.min(r.t), max.max(r.t)));
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

        let p0 = write_sorted_run(
            &dir,
            "r0.frn",
            vec![
                make_record(1, 1, 10, 1),
                make_record(3, 1, 30, 1),
                make_record(5, 1, 50, 1),
            ],
        );
        let p1 = write_sorted_run(
            &dir,
            "r1.frn",
            vec![make_record(2, 1, 20, 1), make_record(4, 1, 40, 1)],
        );
        let p2 = write_sorted_run(&dir, "r2.frn", vec![make_record(6, 1, 60, 1)]);

        let streams = open_streams(&[p0, p1, p2]);
        let mut merge = KWayMerge::new(streams, cmp_spot).unwrap();

        let mut results = Vec::new();
        while let Some(rec) = merge.next_record().unwrap() {
            results.push(rec);
        }

        assert_eq!(results.len(), 6);
        // Verify global SPOT order
        assert_eq!(results[0].s_id, SubjectId::from_u64(1));
        assert_eq!(results[1].s_id, SubjectId::from_u64(2));
        assert_eq!(results[2].s_id, SubjectId::from_u64(3));
        assert_eq!(results[3].s_id, SubjectId::from_u64(4));
        assert_eq!(results[4].s_id, SubjectId::from_u64(5));
        assert_eq!(results[5].s_id, SubjectId::from_u64(6));

        assert!(merge.is_exhausted());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_merge_dedup_same_fact_different_t() {
        let dir = std::env::temp_dir().join("fluree_test_merge_dedup_t");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Same fact (s=1, p=1, o=10) at t=1 and t=2 in different runs
        let p0 = write_sorted_run(&dir, "r0.frn", vec![make_record(1, 1, 10, 1)]);
        let p1 = write_sorted_run(&dir, "r1.frn", vec![make_record(1, 1, 10, 2)]);

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
        let p0 = write_sorted_run(
            &dir,
            "r0.frn",
            vec![
                make_lang_record(1, 1, 0, 1, 1), // lang_id=1 (e.g., "en")
                make_lang_record(1, 1, 0, 1, 2), // lang_id=2 (e.g., "fr")
            ],
        );

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
        let p0 = write_sorted_run(
            &dir,
            "r0.frn",
            vec![
                make_list_record(1, 1, 10, 1, 0),
                make_list_record(1, 1, 10, 1, 1),
            ],
        );

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
        let p0 = write_sorted_run(
            &dir,
            "r0.frn",
            vec![
                make_record(1, 1, 10, 1),         // i = ListIndex::none()
                make_list_record(1, 1, 10, 1, 0), // i = 0
            ],
        );

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

        let rec = merge.next_record().unwrap().unwrap();
        assert_eq!(rec.s_id, SubjectId::from_u64(1));
        assert!(merge.next_record().unwrap().is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_merge_interleaved() {
        let dir = std::env::temp_dir().join("fluree_test_merge_interleaved");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Overlapping key ranges across streams
        let p0 = write_sorted_run(
            &dir,
            "r0.frn",
            vec![
                make_record(1, 1, 10, 1),
                make_record(2, 1, 20, 1),
                make_record(5, 1, 50, 1),
            ],
        );
        let p1 = write_sorted_run(
            &dir,
            "r1.frn",
            vec![
                make_record(1, 2, 15, 1),
                make_record(3, 1, 30, 1),
                make_record(4, 1, 40, 1),
            ],
        );

        let streams = open_streams(&[p0, p1]);
        let mut merge = KWayMerge::new(streams, cmp_spot).unwrap();

        let mut results = Vec::new();
        while let Some(rec) = merge.next_record().unwrap() {
            results.push(rec);
        }

        assert_eq!(results.len(), 6);
        // Verify SPOT order (s_id primary, p_id secondary)
        assert_eq!(
            (results[0].s_id, results[0].p_id),
            (SubjectId::from_u64(1), 1)
        );
        assert_eq!(
            (results[1].s_id, results[1].p_id),
            (SubjectId::from_u64(1), 2)
        );
        assert_eq!(
            (results[2].s_id, results[2].p_id),
            (SubjectId::from_u64(2), 1)
        );
        assert_eq!(
            (results[3].s_id, results[3].p_id),
            (SubjectId::from_u64(3), 1)
        );
        assert_eq!(
            (results[4].s_id, results[4].p_id),
            (SubjectId::from_u64(4), 1)
        );
        assert_eq!(
            (results[5].s_id, results[5].p_id),
            (SubjectId::from_u64(5), 1)
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Tests for next_deduped_with_history ----

    /// Make a record with explicit assert/retract flag.
    fn make_record_op(s_id: u64, p_id: u32, val: i64, t: u32, assert: bool) -> RunRecord {
        RunRecord::new(
            0,
            SubjectId::from_u64(s_id),
            p_id,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(val),
            t,
            assert,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        )
    }

    #[test]
    fn test_dedup_with_history_single_version() {
        // Single version of each fact — history should be empty.
        let dir = std::env::temp_dir().join("fluree_test_dedup_hist_single");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let p0 = write_sorted_run(
            &dir,
            "r0.frn",
            vec![make_record(1, 1, 10, 1), make_record(2, 1, 20, 1)],
        );

        let streams = open_streams(&[p0]);
        let mut merge = KWayMerge::new(streams, cmp_spot).unwrap();
        let mut history = Vec::new();

        // First record: s=1
        history.clear();
        let rec = merge.next_deduped_with_history(&mut history).unwrap().unwrap();
        assert_eq!(rec.s_id, SubjectId::from_u64(1));
        assert!(history.is_empty(), "single-version fact should have no history");

        // Second record: s=2
        history.clear();
        let rec = merge.next_deduped_with_history(&mut history).unwrap().unwrap();
        assert_eq!(rec.s_id, SubjectId::from_u64(2));
        assert!(history.is_empty());

        // Done
        history.clear();
        assert!(merge.next_deduped_with_history(&mut history).unwrap().is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_dedup_with_history_two_versions() {
        // Same fact at t=1 and t=2 — winner is t=2, history is t=1.
        let dir = std::env::temp_dir().join("fluree_test_dedup_hist_two");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let p0 = write_sorted_run(&dir, "r0.frn", vec![make_record(1, 1, 10, 1)]);
        let p1 = write_sorted_run(&dir, "r1.frn", vec![make_record(1, 1, 10, 2)]);

        let streams = open_streams(&[p0, p1]);
        let mut merge = KWayMerge::new(streams, cmp_spot).unwrap();
        let mut history = Vec::new();

        let rec = merge.next_deduped_with_history(&mut history).unwrap().unwrap();
        assert_eq!(rec.t, 2, "winner should be t=2");
        assert_eq!(history.len(), 1, "should have one history entry");
        assert_eq!(history[0].t, 1, "history entry should be t=1");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_dedup_with_history_assert_retract_reassert() {
        // Same fact: assert t=1, retract t=2, assert t=3
        // Winner: assert at t=3. History: [assert t=1, retract t=2]
        let dir = std::env::temp_dir().join("fluree_test_dedup_hist_ara");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let p0 = write_sorted_run(
            &dir,
            "r0.frn",
            vec![make_record_op(1, 1, 10, 1, true)], // assert t=1
        );
        let p1 = write_sorted_run(
            &dir,
            "r1.frn",
            vec![make_record_op(1, 1, 10, 2, false)], // retract t=2
        );
        let p2 = write_sorted_run(
            &dir,
            "r2.frn",
            vec![make_record_op(1, 1, 10, 3, true)], // assert t=3
        );

        let streams = open_streams(&[p0, p1, p2]);
        let mut merge = KWayMerge::new(streams, cmp_spot).unwrap();
        let mut history = Vec::new();

        let rec = merge.next_deduped_with_history(&mut history).unwrap().unwrap();
        assert_eq!(rec.t, 3, "winner should be assert at t=3");
        assert_eq!(rec.op, 1, "winner should be assert");
        assert_eq!(history.len(), 2, "should have two history entries");

        // History contains both the t=1 assert and t=2 retract
        let hist_ts: Vec<u32> = history.iter().map(|r| r.t).collect();
        assert!(hist_ts.contains(&1), "history should contain t=1");
        assert!(hist_ts.contains(&2), "history should contain t=2");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_dedup_with_history_retract_wins() {
        // Same fact: assert t=1, retract t=2
        // Winner: retract at t=2. History: [assert t=1]
        let dir = std::env::temp_dir().join("fluree_test_dedup_hist_retract_wins");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let p0 = write_sorted_run(
            &dir,
            "r0.frn",
            vec![make_record_op(1, 1, 10, 1, true)],
        );
        let p1 = write_sorted_run(
            &dir,
            "r1.frn",
            vec![make_record_op(1, 1, 10, 2, false)],
        );

        let streams = open_streams(&[p0, p1]);
        let mut merge = KWayMerge::new(streams, cmp_spot).unwrap();
        let mut history = Vec::new();

        let rec = merge.next_deduped_with_history(&mut history).unwrap().unwrap();
        assert_eq!(rec.t, 2, "winner should be retract at t=2");
        assert_eq!(rec.op, 0, "winner should be retract");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].t, 1);
        assert_eq!(history[0].op, 1, "history should contain the assert");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_dedup_with_history_mixed_facts() {
        // Two different facts: s=1 (multi-version) and s=2 (single-version)
        let dir = std::env::temp_dir().join("fluree_test_dedup_hist_mixed");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let p0 = write_sorted_run(
            &dir,
            "r0.frn",
            vec![make_record(1, 1, 10, 1), make_record(2, 1, 20, 1)],
        );
        let p1 = write_sorted_run(
            &dir,
            "r1.frn",
            vec![make_record(1, 1, 10, 5)], // s=1 updated at t=5
        );

        let streams = open_streams(&[p0, p1]);
        let mut merge = KWayMerge::new(streams, cmp_spot).unwrap();
        let mut history = Vec::new();

        // s=1: multi-version, winner t=5, history [t=1]
        history.clear();
        let rec = merge.next_deduped_with_history(&mut history).unwrap().unwrap();
        assert_eq!(rec.s_id, SubjectId::from_u64(1));
        assert_eq!(rec.t, 5);
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].t, 1);

        // s=2: single-version, no history
        history.clear();
        let rec = merge.next_deduped_with_history(&mut history).unwrap().unwrap();
        assert_eq!(rec.s_id, SubjectId::from_u64(2));
        assert_eq!(rec.t, 1);
        assert!(history.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
