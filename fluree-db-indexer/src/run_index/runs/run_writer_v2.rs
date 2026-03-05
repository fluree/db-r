//! V2 run writer — buffers `RunRecordV2` records, sorts, and flushes to V2 run files.
//!
//! Same background-flush architecture as V1 `RunWriter`, but operates on
//! `RunRecordV2` with V2 sort comparators and V2 wire format.

use super::run_file_v2::{write_run_file_v2, RunFileInfoV2};
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::format::run_record_v2::{cmp_v2_for_order, RunRecordV2};
use std::io;
use std::path::PathBuf;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for a V2 run writer.
pub struct RunWriterV2Config {
    /// Total memory budget in bytes for the record buffer.
    pub buffer_budget_bytes: usize,
    /// Sort order for this writer's run files.
    pub sort_order: RunSortOrder,
    /// Directory where run files are written.
    pub run_dir: PathBuf,
}

impl RunWriterV2Config {
    /// Maximum records that fit in the budget.
    pub fn max_records(&self) -> usize {
        self.buffer_budget_bytes / std::mem::size_of::<RunRecordV2>()
    }
}

// ============================================================================
// RunWriterV2
// ============================================================================

/// V2 run writer with background flush.
pub struct RunWriterV2 {
    config: RunWriterV2Config,
    buffer: Vec<RunRecordV2>,
    run_count: u32,
    total_records: u64,
    run_files: Vec<RunFileInfoV2>,
    min_t: u32,
    max_t: u32,
    pending_flush: Option<std::thread::JoinHandle<io::Result<FlushResultV2>>>,
    spare_buffer: Option<Vec<RunRecordV2>>,
}

struct FlushResultV2 {
    info: RunFileInfoV2,
    spare: Vec<RunRecordV2>,
}

impl RunWriterV2 {
    pub fn new(config: RunWriterV2Config) -> Self {
        let cap = config.max_records().min(1_000_000);
        Self {
            buffer: Vec::with_capacity(cap),
            config,
            run_count: 0,
            total_records: 0,
            run_files: Vec::new(),
            min_t: u32::MAX,
            max_t: 0,
            pending_flush: None,
            spare_buffer: None,
        }
    }

    /// Push a record. Flushes to disk when the buffer exceeds the budget.
    pub fn push(&mut self, record: RunRecordV2) -> io::Result<()> {
        if record.t < self.min_t {
            self.min_t = record.t;
        }
        if record.t > self.max_t {
            self.max_t = record.t;
        }
        self.buffer.push(record);
        if self.buffer.len() >= self.config.max_records() {
            self.flush_buffer()?;
        }
        Ok(())
    }

    /// Flush the current buffer to a run file in a background thread.
    fn flush_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Join any pending flush first.
        self.join_pending_flush()?;

        // Swap buffers.
        let mut flush_buf = if let Some(mut spare) = self.spare_buffer.take() {
            spare.clear();
            std::mem::swap(&mut spare, &mut self.buffer);
            spare
        } else {
            let cap = self.config.max_records().min(1_000_000);
            let mut new_buf = Vec::with_capacity(cap);
            std::mem::swap(&mut new_buf, &mut self.buffer);
            new_buf
        };

        let run_index = self.run_count;
        self.run_count += 1;
        self.total_records += flush_buf.len() as u64;
        let order = self.config.sort_order;
        let run_path = self.config.run_dir.join(format!("run_{run_index:05}.frn"));
        let cmp = cmp_v2_for_order(order);

        let handle = std::thread::spawn(move || {
            // Sort in place.
            flush_buf.sort_unstable_by(cmp);

            let min_t = flush_buf.iter().map(|r| r.t).min().unwrap_or(0);
            let max_t = flush_buf.iter().map(|r| r.t).max().unwrap_or(0);

            let info = write_run_file_v2(&run_path, &flush_buf, order, min_t, max_t)?;

            // Shrink buffer for reuse.
            flush_buf.clear();
            flush_buf.shrink_to(1_000_000);

            Ok(FlushResultV2 {
                info,
                spare: flush_buf,
            })
        });

        self.pending_flush = Some(handle);
        Ok(())
    }

    fn join_pending_flush(&mut self) -> io::Result<()> {
        if let Some(handle) = self.pending_flush.take() {
            let result = handle
                .join()
                .map_err(|_| io::Error::other("flush thread panicked"))??;
            self.run_files.push(result.info);
            self.spare_buffer = Some(result.spare);
        }
        Ok(())
    }

    /// Finish writing, flush remaining records, return all run file metadata.
    pub fn finish(mut self) -> io::Result<RunWriterV2Result> {
        if !self.buffer.is_empty() {
            self.flush_buffer()?;
        }
        self.join_pending_flush()?;

        Ok(RunWriterV2Result {
            run_files: self.run_files,
            total_records: self.total_records,
            min_t: self.min_t,
            max_t: self.max_t,
        })
    }
}

/// Result of finishing a V2 run writer.
#[derive(Debug)]
pub struct RunWriterV2Result {
    pub run_files: Vec<RunFileInfoV2>,
    pub total_records: u64,
    pub min_t: u32,
    pub max_t: u32,
}

// ============================================================================
// MultiOrderRunWriterV2
// ============================================================================

/// Configuration for multi-order V2 run writer.
pub struct MultiOrderV2Config {
    /// Total memory budget split evenly across orders.
    pub total_budget_bytes: usize,
    /// Which orders to write.
    pub orders: Vec<RunSortOrder>,
    /// Base directory; per-order subdirs created automatically.
    pub base_run_dir: PathBuf,
}

/// Fans out `RunRecordV2` records to per-order `RunWriterV2` instances.
pub struct MultiOrderRunWriterV2 {
    writers: Vec<(RunSortOrder, RunWriterV2)>,
}

impl MultiOrderRunWriterV2 {
    pub fn new(config: MultiOrderV2Config) -> io::Result<Self> {
        let per_order_budget = config.total_budget_bytes / config.orders.len().max(1);
        let mut writers = Vec::with_capacity(config.orders.len());

        for &order in &config.orders {
            let run_dir = config.base_run_dir.join(order.dir_name());
            std::fs::create_dir_all(&run_dir)?;
            let w = RunWriterV2::new(RunWriterV2Config {
                buffer_budget_bytes: per_order_budget,
                sort_order: order,
                run_dir,
            });
            writers.push((order, w));
        }

        Ok(Self { writers })
    }

    /// Push a record to all order writers.
    pub fn push(&mut self, record: RunRecordV2) -> io::Result<()> {
        for (_, writer) in &mut self.writers {
            writer.push(record)?;
        }
        Ok(())
    }

    /// Finish all writers and return per-order results.
    pub fn finish(self) -> io::Result<Vec<(RunSortOrder, RunWriterV2Result)>> {
        let mut results = Vec::with_capacity(self.writers.len());
        for (order, writer) in self.writers {
            results.push((order, writer.finish()?));
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_binary_index::format::run_record::LIST_INDEX_NONE;
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;

    fn make_rec(s_id: u64, p_id: u32, o_type: u16, o_key: u64, t: u32) -> RunRecordV2 {
        RunRecordV2 {
            s_id: SubjectId(s_id),
            o_key,
            p_id,
            t,
            o_i: LIST_INDEX_NONE,
            o_type,
            g_id: 0,
        }
    }

    #[test]
    fn single_writer_basic() {
        let dir = std::env::temp_dir().join("fluree_test_run_writer_v2_basic");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut writer = RunWriterV2::new(RunWriterV2Config {
            buffer_budget_bytes: 1024, // tiny budget to force multiple flushes
            sort_order: RunSortOrder::Spot,
            run_dir: dir.clone(),
        });

        // Push enough records to trigger at least one flush.
        for i in 0..100 {
            writer
                .push(make_rec(
                    i,
                    1,
                    OType::XSD_INTEGER.as_u16(),
                    i * 10,
                    (i + 1) as u32,
                ))
                .unwrap();
        }

        let result = writer.finish().unwrap();
        assert_eq!(result.total_records, 100);
        assert!(!result.run_files.is_empty());
        assert_eq!(result.min_t, 1);
        assert_eq!(result.max_t, 100);

        // Verify run files exist.
        for info in &result.run_files {
            assert!(info.path.exists());
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn multi_order_writer() {
        let dir = std::env::temp_dir().join("fluree_test_multi_order_v2");
        let _ = std::fs::remove_dir_all(&dir);

        let mut writer = MultiOrderRunWriterV2::new(MultiOrderV2Config {
            total_budget_bytes: 256 * 1024,
            orders: vec![RunSortOrder::Spot, RunSortOrder::Post, RunSortOrder::Opst],
            base_run_dir: dir.clone(),
        })
        .unwrap();

        for i in 0..50 {
            writer
                .push(make_rec(
                    i,
                    (i % 5) as u32,
                    OType::XSD_INTEGER.as_u16(),
                    i * 10,
                    1,
                ))
                .unwrap();
        }

        let results = writer.finish().unwrap();
        assert_eq!(results.len(), 3);
        for (order, result) in &results {
            assert_eq!(result.total_records, 50);
            // Verify subdirectory was created.
            assert!(dir.join(order.dir_name()).exists());
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
