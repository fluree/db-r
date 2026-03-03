use crate::binding::Binding;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::operator::{Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_binary_index::{
    decode_leaflet_region1, read_leaf_header, LeafletHeader, RunRecord, RunSortOrder,
};
use fluree_db_core::subject_id::SubjectId;
use fluree_db_core::value_id::ObjKind;
use fluree_vocab::rdf;
use std::collections::HashMap;
use std::sync::Arc;

/// Fast-path: `?s rdf:type ?o GROUP BY ?o (COUNT(?s) AS ?count)` with `ORDER BY DESC(?count) LIMIT k`.
///
/// Uses only per-leaflet uncompressed "FIRST" headers to skip decoding entire leaflets when
/// `FIRST(i).(p,o) == FIRST(i+1).(p,o)` (boundary-equality implies the whole leaflet is that (p,o) in POST order).
///
/// This is a first-step prototype that requires:
/// - POST order access
/// - ref-only objects (enforced at runtime; returns error if non-ref is seen)
pub struct RdfTypeGroupCountFirstsOperator {
    /// Output schema: [object_var, count_var]
    schema: Arc<[VarId]>,
    /// Bound predicate id (p_id for rdf:type).
    ///
    /// When `None`, resolves `rdf:type` from the binary store at `open()`.
    p_id: Option<u32>,
    /// LIMIT k (top-k by count)
    limit: usize,
    /// Operator state
    state: OperatorState,
    /// Materialized results (already sorted and truncated to limit)
    results: Vec<(u64, i64)>,
    /// Next result to emit
    pos: usize,
}

impl RdfTypeGroupCountFirstsOperator {
    pub fn new(object_var: VarId, count_var: VarId, p_id: Option<u32>, limit: usize) -> Self {
        Self {
            schema: Arc::from(vec![object_var, count_var].into_boxed_slice()),
            p_id,
            limit: limit.max(1),
            state: OperatorState::Created,
            results: Vec::new(),
            pos: 0,
        }
    }

    fn read_leaflet_first(
        leaf_mmap: &[u8],
        dir: &fluree_db_binary_index::format::leaf::LeafletDirEntry,
    ) -> std::io::Result<LeafletHeader> {
        let end = dir.offset as usize + dir.compressed_len as usize;
        let leaflet_bytes = &leaf_mmap[dir.offset as usize..end];
        LeafletHeader::read_from(leaflet_bytes)
    }

    fn prefix_from_dir_or_leaflet(
        leaf_mmap: &[u8],
        dir: &fluree_db_binary_index::format::leaf::LeafletDirEntry,
    ) -> std::io::Result<(u32, u8, u64)> {
        if let (Some(k), Some(o)) = (dir.first_o_kind, dir.first_o_key) {
            Ok((dir.first_p_id, k, o))
        } else {
            // Older leaf versions: fall back to reading the leaflet header.
            let lh = Self::read_leaflet_first(leaf_mmap, dir)?;
            Ok(Self::prefix_from_leaflet_first(&lh))
        }
    }

    fn prefix_from_leaflet_first(lh: &LeafletHeader) -> (u32, u8, u64) {
        (lh.first_p_id, lh.first_o_kind, lh.first_o_key)
    }

    fn prefix_from_run_record(rec: &RunRecord) -> (u32, u8, u64) {
        (rec.p_id, rec.o_kind, rec.o_key)
    }

    fn add_count(map: &mut HashMap<u64, i64>, o_s_id: u64, add: i64) {
        *map.entry(o_s_id).or_insert(0) += add;
    }

    #[inline]
    fn io_to_query(where_: &'static str, e: std::io::Error) -> QueryError {
        QueryError::execution(format!("{where_}: {e}"))
    }
}

#[async_trait]
impl Operator for RdfTypeGroupCountFirstsOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            return Ok(());
        }
        self.state = OperatorState::Open;
        self.results.clear();
        self.pos = 0;

        let Some(gv) = ctx.graph_view() else {
            return Err(QueryError::InvalidQuery(
                "rdf:type count fast-path requires binary index store".to_string(),
            ));
        };
        let store = gv.clone_store();
        let g_id = gv.g_id();

        let p_id = match self.p_id {
            Some(p) => p,
            None => store.find_predicate_id(rdf::TYPE).ok_or_else(|| {
                QueryError::InvalidQuery(
                    "rdf:type predicate not found in binary predicate dict".to_string(),
                )
            })?,
        };

        // Build a leaf range for POST where p_id is fixed and everything else is wildcard.
        let min_key = RunRecord {
            g_id,
            s_id: SubjectId::from_u64(0),
            p_id,
            dt: 0,
            o_kind: ObjKind::MIN.as_u8(),
            op: 0,
            o_key: 0,
            t: 0,
            lang_id: 0,
            i: 0,
        };
        let max_key = RunRecord {
            g_id,
            s_id: SubjectId::from_u64(u64::MAX),
            p_id,
            dt: u16::MAX,
            o_kind: ObjKind::MAX.as_u8(),
            op: 1,
            o_key: u64::MAX,
            t: u32::MAX,
            lang_id: u16::MAX,
            i: u32::MAX,
        };

        let Some(branch) = store.branch_for_order(g_id, RunSortOrder::Post) else {
            self.state = OperatorState::Exhausted;
            return Ok(());
        };

        let cmp = fluree_db_binary_index::cmp_for_order(RunSortOrder::Post);
        let leaf_range = branch.find_leaves_in_range(&min_key, &max_key, cmp);

        let mut counts: HashMap<u64, i64> = HashMap::new();

        for leaf_idx in leaf_range.clone() {
            let leaf_entry = &branch.leaves[leaf_idx];
            let leaf_path = leaf_entry.resolved_path.as_ref().ok_or_else(|| {
                QueryError::execution(format!("leaf {} has no resolved path", leaf_entry.leaf_cid))
            })?;

            let file = match std::fs::File::open(leaf_path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    store
                        .ensure_index_leaf_cached(&leaf_entry.leaf_cid, leaf_path)
                        .map_err(|e| Self::io_to_query("ensure_index_leaf_cached", e))?;
                    std::fs::File::open(leaf_path)
                        .map_err(|e| Self::io_to_query("open leaf after cache", e))?
                }
                Err(e) => return Err(Self::io_to_query("open leaf", e)),
            };
            let leaf_mmap = unsafe {
                memmap2::Mmap::map(&file).map_err(|e| Self::io_to_query("mmap leaf", e))?
            };
            let leaf_header = read_leaf_header(&leaf_mmap)
                .map_err(|e| Self::io_to_query("read leaf header", e))?;

            let dirs = &leaf_header.leaflet_dir;
            for i in 0..dirs.len() {
                let dir = &dirs[i];
                let (p, o_kind, o_key) = Self::prefix_from_dir_or_leaflet(&leaf_mmap, dir)
                    .map_err(|e| Self::io_to_query("read leaflet first prefix", e))?;

                if p < p_id {
                    continue;
                }
                if p > p_id {
                    break;
                }

                if o_kind != ObjKind::REF_ID.as_u8() {
                    return Err(QueryError::InvalidQuery(
                        "rdf:type count fast-path encountered non-ref object; requires ref-only objects".to_string(),
                    ));
                }

                // Determine the "next leaflet's FIRST" (same leaf, or next leaf's first_key).
                let next_prefix = if i + 1 < dirs.len() {
                    let next_dir = &dirs[i + 1];
                    Some(
                        Self::prefix_from_dir_or_leaflet(&leaf_mmap, next_dir)
                            .map_err(|e| Self::io_to_query("read next leaflet first prefix", e))?,
                    )
                } else if leaf_idx + 1 < leaf_range.end {
                    Some(Self::prefix_from_run_record(
                        &branch.leaves[leaf_idx + 1].first_key,
                    ))
                } else {
                    None
                };

                if next_prefix == Some((p, o_kind, o_key)) {
                    // Boundary-equality implies this leaflet is entirely (p,o).
                    Self::add_count(&mut counts, o_key, dir.row_count as i64);
                    continue;
                }

                // Fallback for this leaflet: decode Region 1 and count by object.
                let end = dir.offset as usize + dir.compressed_len as usize;
                let leaflet_bytes = &leaf_mmap[dir.offset as usize..end];
                let (_lh2, _s_ids, p_ids, o_kinds, o_keys) =
                    decode_leaflet_region1(leaflet_bytes, leaf_header.p_width, RunSortOrder::Post)
                        .map_err(|e| Self::io_to_query("decode leaflet region1", e))?;
                for row in 0..p_ids.len() {
                    if p_ids[row] != p_id {
                        continue;
                    }
                    if o_kinds[row] != ObjKind::REF_ID.as_u8() {
                        return Err(QueryError::InvalidQuery(
                            "rdf:type count fast-path encountered non-ref object; requires ref-only objects".to_string(),
                        ));
                    }
                    Self::add_count(&mut counts, o_keys[row], 1);
                }
            }
        }

        // Top-k by count desc.
        let mut rows: Vec<(u64, i64)> = counts.into_iter().collect();
        rows.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        rows.truncate(self.limit);
        self.results = rows;

        Ok(())
    }

    async fn next_batch(
        &mut self,
        ctx: &ExecutionContext<'_>,
    ) -> Result<Option<crate::binding::Batch>> {
        if !self.state.can_next() {
            return Ok(None);
        }
        if self.pos >= self.results.len() {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }

        let batch_size = ctx.batch_size;
        let mut col_o: Vec<Binding> = Vec::with_capacity(batch_size);
        let mut col_c: Vec<Binding> = Vec::with_capacity(batch_size);

        while self.pos < self.results.len() && col_o.len() < batch_size {
            let (o_s_id, count) = self.results[self.pos];
            self.pos += 1;

            col_o.push(Binding::EncodedSid { s_id: o_s_id });
            col_c.push(Binding::Lit {
                val: fluree_db_core::FlakeValue::Long(count),
                dt: fluree_db_core::Sid::xsd_integer(),
                lang: None,
                t: None,
                op: None,
                p_id: None,
            });
        }

        Ok(Some(crate::binding::Batch::new(
            self.schema.clone(),
            vec![col_o, col_c],
        )?))
    }

    fn close(&mut self) {
        self.state = OperatorState::Closed;
        self.results.clear();
        self.pos = 0;
    }

    fn estimated_rows(&self) -> Option<usize> {
        Some(self.limit)
    }
}
