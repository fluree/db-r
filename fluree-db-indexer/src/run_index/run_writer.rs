//! Memory-bounded run writer: buffers records, sorts, and flushes to run files.
//!
//! Implements the external-sort pattern: accumulate records up to a memory
//! budget, sort by the target index order (SPOT), and write as an immutable
//! run file. The resulting run files are later merged (Phase C).

use super::global_dict::LanguageTagDict;
use super::run_file::{write_run_file, RunFileInfo};
use super::run_record::{cmp_for_order, RunRecord, RunSortOrder};
use std::io;
use std::path::PathBuf;

/// Trait for anything that can receive RunRecords.
///
/// Implemented by `RunWriter` (single-order) and `MultiOrderRunWriter` (multi-order).
pub trait RecordSink {
    fn push(&mut self, record: RunRecord, lang_dict: &mut LanguageTagDict) -> io::Result<()>;
}

/// Configuration for the RunWriter.
#[derive(Debug, Clone)]
pub struct RunWriterConfig {
    /// Maximum memory budget for the record buffer in bytes.
    /// Default: 256 MB.
    pub buffer_budget_bytes: usize,
    /// Sort order for output run files.
    pub sort_order: RunSortOrder,
    /// Directory where run files are written.
    pub run_dir: PathBuf,
}

impl RunWriterConfig {
    /// Maximum number of records that fit in the budget.
    pub fn max_records(&self) -> usize {
        self.buffer_budget_bytes / std::mem::size_of::<RunRecord>()
    }
}

impl Default for RunWriterConfig {
    fn default() -> Self {
        Self {
            buffer_budget_bytes: 256 * 1024 * 1024, // 256 MB
            sort_order: RunSortOrder::Spot,
            run_dir: PathBuf::from("."),
        }
    }
}

/// Result from a completed background flush thread.
struct FlushResult {
    info: RunFileInfo,
    /// The now-empty buffer, returned for reuse (avoids reallocation).
    buffer: Vec<RunRecord>,
}

/// Memory-bounded buffer that accumulates RunRecords and flushes sorted run files.
///
/// When a flush is triggered, the full buffer is handed off to a background
/// thread for sort + write while the main thread continues pushing records
/// into a spare buffer. At most one background flush is in flight per writer.
pub struct RunWriter {
    config: RunWriterConfig,
    buffer: Vec<RunRecord>,
    /// Number of run files written so far.
    run_count: u32,
    /// Total records across all flushed runs + current buffer.
    total_records: u64,
    /// Completed run file infos.
    run_files: Vec<RunFileInfo>,
    /// Min t seen across all records.
    min_t: i64,
    /// Max t seen across all records.
    max_t: i64,
    /// Whether to clear the language dict after each flush.
    /// True for standalone writers; false for sub-writers inside MultiOrderRunWriter
    /// (avoids lang_id mismatch when writers flush at different times).
    clear_lang_on_flush: bool,
    /// Background flush thread handle (if a flush is in progress).
    pending_flush: Option<std::thread::JoinHandle<io::Result<FlushResult>>>,
    /// Spare buffer returned from a completed background flush, ready for reuse.
    spare_buffer: Option<Vec<RunRecord>>,
}

impl RunWriter {
    pub fn new(config: RunWriterConfig) -> Self {
        let max_records = config.max_records();
        tracing::info!(
            budget_bytes = config.buffer_budget_bytes,
            max_records,
            record_size = std::mem::size_of::<RunRecord>(),
            run_dir = %config.run_dir.display(),
            "RunWriter initialized"
        );
        Self {
            config,
            buffer: Vec::with_capacity(max_records.min(1_000_000)), // cap initial alloc
            run_count: 0,
            total_records: 0,
            run_files: Vec::new(),
            min_t: i64::MAX,
            max_t: i64::MIN,
            clear_lang_on_flush: true,
            pending_flush: None,
            spare_buffer: None,
        }
    }

    /// Push a record into the buffer. Flushes to disk if budget exceeded.
    pub fn push(&mut self, record: RunRecord, lang_dict: &mut LanguageTagDict) -> io::Result<()> {
        // Track t range
        if record.t < self.min_t {
            self.min_t = record.t;
        }
        if record.t > self.max_t {
            self.max_t = record.t;
        }

        self.buffer.push(record);
        self.total_records += 1;

        if self.buffer.len() >= self.config.max_records() {
            self.flush_buffer(lang_dict)?;
        }

        Ok(())
    }

    /// Wait for any in-flight background flush to complete and collect its result.
    fn join_pending_flush(&mut self) -> io::Result<()> {
        if let Some(handle) = self.pending_flush.take() {
            let result = handle.join().expect("flush thread panicked")?;
            self.run_files.push(result.info);
            self.spare_buffer = Some(result.buffer);
        }
        Ok(())
    }

    /// Flush the current buffer by handing it to a background thread for sort + write.
    ///
    /// The main thread swaps in a spare (or fresh) buffer and continues immediately.
    /// At most one background flush is in flight; if one is pending we join it first.
    fn flush_buffer(&mut self, lang_dict: &mut LanguageTagDict) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Capacity we allow for the returned spare buffer.
        //
        // IMPORTANT: The "full" buffer handed to the background thread can grow to the
        // configured max_records (i.e., up to the byte budget). If we simply `clear()`
        // and return it as the spare buffer, we permanently retain that large allocation,
        // which can explode RSS (especially with MultiOrderRunWriter × N workers).
        //
        // Instead we drop the large allocation after each flush and return a smaller
        // pre-sized buffer for reuse. This trades some re-allocation work for stable,
        // predictable memory usage during large imports.
        let reuse_capacity = self.config.max_records().min(1_000_000);

        // 1. Wait for any pending background flush
        self.join_pending_flush()?;

        // 2. Capture parameters for the background thread
        let run_index = self.run_count;
        let run_path = self
            .config
            .run_dir
            .join(format!("run_{:05}.frn", run_index));
        let sort_order = self.config.sort_order;
        let lang_snapshot = lang_dict.clone(); // small — typically < 1KB
        let reuse_capacity = reuse_capacity;

        // 3. Swap buffers: take full buffer, replace with spare (or fresh)
        let full_buffer = std::mem::replace(
            &mut self.buffer,
            self.spare_buffer
                .take()
                .unwrap_or_else(|| Vec::with_capacity(self.config.max_records().min(1_000_000))),
        );
        let record_count = full_buffer.len();

        // 4. Spawn background sort + write
        self.pending_flush = Some(
            std::thread::Builder::new()
                .name(format!("flush-{}", sort_order.dir_name()))
                .spawn(move || {
                    let mut buf = full_buffer;

                    let sort_start = std::time::Instant::now();
                    buf.sort_unstable_by(cmp_for_order(sort_order));
                    let sort_elapsed = sort_start.elapsed();

                    let (run_min_t, run_max_t) =
                        buf.iter().fold((i64::MAX, i64::MIN), |(min, max), r| {
                            (min.min(r.t), max.max(r.t))
                        });

                    let write_start = std::time::Instant::now();
                    let info = write_run_file(
                        &run_path,
                        &buf,
                        &lang_snapshot,
                        sort_order,
                        run_min_t,
                        run_max_t,
                    )?;
                    let write_elapsed = write_start.elapsed();

                    let file_bytes = info.record_count * super::run_record::RECORD_WIRE_SIZE as u64;
                    tracing::info!(
                        run = run_index,
                        records = record_count,
                        file_mb = file_bytes as f64 / (1024.0 * 1024.0),
                        sort_ms = sort_elapsed.as_millis(),
                        write_ms = write_elapsed.as_millis(),
                        path = %run_path.display(),
                        "run file flushed (background)"
                    );

                    // Drop the large buffer allocation after the flush to avoid retaining
                    // multi-GB capacities across runs.
                    drop(buf);
                    Ok(FlushResult {
                        info,
                        buffer: Vec::with_capacity(reuse_capacity),
                    })
                })?,
        );

        self.run_count += 1;

        // 5. Optionally clear per-run lang dict (synchronous — affects subsequent records)
        if self.clear_lang_on_flush {
            lang_dict.clear();
        }

        Ok(())
    }

    /// Finish writing: flush any remaining buffered records and return results.
    pub fn finish(mut self, lang_dict: &mut LanguageTagDict) -> io::Result<RunWriterResult> {
        // Flush remaining records (spawns background thread)
        self.flush_buffer(lang_dict)?;
        // Wait for the final background flush to complete
        self.join_pending_flush()?;

        Ok(RunWriterResult {
            run_files: self.run_files,
            total_records: self.total_records,
            min_t: if self.min_t == i64::MAX {
                0
            } else {
                self.min_t
            },
            max_t: if self.max_t == i64::MIN {
                0
            } else {
                self.max_t
            },
        })
    }

    /// Number of records currently buffered (not yet flushed).
    pub fn buffered_count(&self) -> usize {
        self.buffer.len()
    }

    /// Number of run files written so far.
    pub fn run_count(&self) -> u32 {
        self.run_count
    }

    /// Total records processed (flushed + buffered).
    pub fn total_records(&self) -> u64 {
        self.total_records
    }
}

impl RecordSink for RunWriter {
    fn push(&mut self, record: RunRecord, lang_dict: &mut LanguageTagDict) -> io::Result<()> {
        self.push(record, lang_dict)
    }
}

/// Result of a completed run generation.
#[derive(Debug)]
pub struct RunWriterResult {
    pub run_files: Vec<RunFileInfo>,
    pub total_records: u64,
    pub min_t: i64,
    pub max_t: i64,
}

// ============================================================================
// Multi-order run writer
// ============================================================================

/// Configuration for multi-order run generation.
#[derive(Debug, Clone)]
pub struct MultiOrderConfig {
    /// Total memory budget split evenly across all orders.
    pub total_budget_bytes: usize,
    /// Which orders to generate runs for.
    pub orders: Vec<RunSortOrder>,
    /// Base directory; per-order subdirs are created beneath it.
    pub base_run_dir: PathBuf,
}

/// Writes records to multiple `RunWriter`s, one per sort order.
///
/// Each push fans out to every writer. OPST writers skip non-IRI records
/// before buffering (saves memory).
pub struct MultiOrderRunWriter {
    writers: Vec<(RunSortOrder, RunWriter)>,
}

impl MultiOrderRunWriter {
    /// Create a multi-order writer. Creates per-order subdirectories.
    pub fn new(config: MultiOrderConfig) -> io::Result<Self> {
        let per_order_budget = config.total_budget_bytes / config.orders.len().max(1);
        let mut writers = Vec::with_capacity(config.orders.len());

        for &order in &config.orders {
            let run_dir = config.base_run_dir.join(order.dir_name());
            std::fs::create_dir_all(&run_dir)?;

            let wc = RunWriterConfig {
                buffer_budget_bytes: per_order_budget,
                sort_order: order,
                run_dir,
            };
            let mut w = RunWriter::new(wc);
            w.clear_lang_on_flush = false;
            writers.push((order, w));
        }

        tracing::info!(
            orders = ?config.orders.iter().map(|o| o.dir_name()).collect::<Vec<_>>(),
            per_order_budget_mb = per_order_budget as f64 / (1024.0 * 1024.0),
            "MultiOrderRunWriter initialized"
        );

        Ok(Self { writers })
    }

    /// Push a record to all writers.
    pub fn push(&mut self, record: RunRecord, lang_dict: &mut LanguageTagDict) -> io::Result<()> {
        for (_order, writer) in &mut self.writers {
            writer.push(record, lang_dict)?;
        }
        Ok(())
    }

    /// Finish all writers, returning per-order results.
    pub fn finish(
        self,
        lang_dict: &mut LanguageTagDict,
    ) -> io::Result<Vec<(RunSortOrder, RunWriterResult)>> {
        let mut results = Vec::with_capacity(self.writers.len());
        for (order, writer) in self.writers {
            let result = writer.finish(lang_dict)?;
            results.push((order, result));
        }
        Ok(results)
    }

    /// Total records processed across all orders (flushed + buffered).
    pub fn total_records(&self) -> u64 {
        self.writers.iter().map(|(_, w)| w.total_records()).sum()
    }

    /// Total run files written across all orders.
    pub fn run_count(&self) -> u32 {
        self.writers.iter().map(|(_, w)| w.run_count()).sum()
    }
}

impl RecordSink for MultiOrderRunWriter {
    fn push(&mut self, record: RunRecord, lang_dict: &mut LanguageTagDict) -> io::Result<()> {
        self.push(record, lang_dict)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::{ObjKey, ObjKind};
    use fluree_db_core::DatatypeDictId;

    fn make_test_record(s_id: u64, p_id: u32, val: i64, t: i64) -> RunRecord {
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

    #[test]
    fn test_single_flush() {
        let dir = std::env::temp_dir().join("fluree_test_run_writer_single");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let config = RunWriterConfig {
            buffer_budget_bytes: 1024 * 1024, // 1MB — won't trigger auto-flush
            sort_order: RunSortOrder::Spot,
            run_dir: dir.clone(),
        };

        let mut writer = RunWriter::new(config);
        let mut lang_dict = LanguageTagDict::new();

        // Push a few records
        writer
            .push(make_test_record(3, 1, 10, 1), &mut lang_dict)
            .unwrap();
        writer
            .push(make_test_record(1, 1, 20, 1), &mut lang_dict)
            .unwrap();
        writer
            .push(make_test_record(2, 1, 30, 2), &mut lang_dict)
            .unwrap();

        let result = writer.finish(&mut lang_dict).unwrap();
        assert_eq!(result.run_files.len(), 1);
        assert_eq!(result.total_records, 3);
        assert_eq!(result.min_t, 1);
        assert_eq!(result.max_t, 2);

        // Verify file is sorted
        let (header, _, records) =
            super::super::run_file::read_run_file(&result.run_files[0].path).unwrap();
        assert_eq!(header.record_count, 3);
        // SPOT order: s_id 1, 2, 3
        assert_eq!(records[0].s_id, SubjectId::from_u64(1));
        assert_eq!(records[1].s_id, SubjectId::from_u64(2));
        assert_eq!(records[2].s_id, SubjectId::from_u64(3));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_budget_triggers_multiple_runs() {
        let dir = std::env::temp_dir().join("fluree_test_run_writer_multi");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Set budget to exactly 2 records (96 bytes at 48 bytes/record)
        let config = RunWriterConfig {
            buffer_budget_bytes: 96,
            sort_order: RunSortOrder::Spot,
            run_dir: dir.clone(),
        };

        let mut writer = RunWriter::new(config);
        let mut lang_dict = LanguageTagDict::new();

        // Push 5 records — should produce 3 run files (2+2+1)
        for i in 0..5u64 {
            writer
                .push(make_test_record(i, 1, i as i64, 1), &mut lang_dict)
                .unwrap();
        }

        let result = writer.finish(&mut lang_dict).unwrap();
        assert_eq!(result.total_records, 5);
        // 5 records / 2 per run = 3 run files (2+2+1)
        assert_eq!(result.run_files.len(), 3);
        assert_eq!(result.run_files[0].record_count, 2);
        assert_eq!(result.run_files[1].record_count, 2);
        assert_eq!(result.run_files[2].record_count, 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_empty_writer() {
        let dir = std::env::temp_dir().join("fluree_test_run_writer_empty");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let config = RunWriterConfig {
            run_dir: dir.clone(),
            ..Default::default()
        };

        let writer = RunWriter::new(config);
        let mut lang_dict = LanguageTagDict::new();
        let result = writer.finish(&mut lang_dict).unwrap();

        assert_eq!(result.total_records, 0);
        assert!(result.run_files.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_multi_order_writer_basic() {
        let dir = std::env::temp_dir().join("fluree_test_multi_order_basic");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let config = MultiOrderConfig {
            total_budget_bytes: 4 * 1024 * 1024, // 4MB → 1MB each
            orders: vec![
                RunSortOrder::Spot,
                RunSortOrder::Psot,
                RunSortOrder::Post,
                RunSortOrder::Opst,
            ],
            base_run_dir: dir.clone(),
        };

        let mut writer = MultiOrderRunWriter::new(config).unwrap();
        let mut lang_dict = LanguageTagDict::new();

        // Push 3 non-IRI records (integers)
        writer
            .push(make_test_record(3, 1, 10, 1), &mut lang_dict)
            .unwrap();
        writer
            .push(make_test_record(1, 1, 20, 1), &mut lang_dict)
            .unwrap();
        writer
            .push(make_test_record(2, 1, 30, 2), &mut lang_dict)
            .unwrap();

        let results = writer.finish(&mut lang_dict).unwrap();
        assert_eq!(results.len(), 4);

        // All orders should have 3 records
        for (order, result) in &results {
            assert_eq!(result.total_records, 3, "{:?} should have 3 records", order);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_multi_order_writer_opst_mixed_types() {
        let dir = std::env::temp_dir().join("fluree_test_multi_order_opst");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let config = MultiOrderConfig {
            total_budget_bytes: 2 * 1024 * 1024,
            orders: vec![RunSortOrder::Spot, RunSortOrder::Opst],
            base_run_dir: dir.clone(),
        };

        let mut writer = MultiOrderRunWriter::new(config).unwrap();
        let mut lang_dict = LanguageTagDict::new();

        // Push 2 integer records
        writer
            .push(make_test_record(1, 1, 10, 1), &mut lang_dict)
            .unwrap();
        writer
            .push(make_test_record(2, 1, 20, 1), &mut lang_dict)
            .unwrap();

        // Push 1 IRI record
        let iri_rec = RunRecord::new(
            0,
            SubjectId::from_u64(3),
            1,
            ObjKind::REF_ID,
            ObjKey::encode_u32_id(42),
            1,
            true,
            DatatypeDictId::ID.as_u16(),
            0,
            None,
        );
        writer.push(iri_rec, &mut lang_dict).unwrap();

        let results = writer.finish(&mut lang_dict).unwrap();

        // All orders receive all records (OPST no longer filters by type)
        for (order, result) in &results {
            assert_eq!(result.total_records, 3, "{:?} should have 3 records", order);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
