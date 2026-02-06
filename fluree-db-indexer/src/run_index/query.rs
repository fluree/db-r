//! Simple subject-lookup query tool for validation.
//!
//! Given a subject IRI or s_id, find all its facts from the SPOT index.
//! This is a validation tool, not a production query engine.

use super::branch::{find_branch_file, read_branch_manifest, BranchManifest};
use super::dict_io::{read_forward_entry, read_forward_index};
use super::global_dict::PredicateDict;
use super::leaf::read_leaf_header;
use super::leaflet::decode_leaflet;
use super::run_record::RunSortOrder;
use fluree_db_core::value_id::{ObjKey, ObjKind};
use fluree_db_core::ListIndex;
use serde_json;
use std::collections::HashMap;
use std::io;
use std::path::Path;

// ============================================================================
// FactRow: human-readable query result
// ============================================================================

/// A single fact from the index, with human-readable projections.
#[derive(Debug)]
pub struct FactRow {
    pub s_id: u64,
    pub p_id: u32,
    pub p_iri: String,
    pub o_kind: u8,
    pub o_key: u64,
    pub o_display: String,
    pub dt: u32,
    pub t: i64,
    pub lang_id: u16,
    pub i: i32,
}

// ============================================================================
// SpotQuery
// ============================================================================

/// Query engine for SPOT indexes. Holds all the state needed to look up
/// and project facts from the on-disk index.
pub struct SpotQuery {
    /// Per-graph branch manifests.
    pub branches: HashMap<u32, BranchManifest>,
    /// Predicate dictionary (small, fully in memory).
    pub predicates: PredicateDict,
    /// Memory-mapped strings.fwd.
    pub string_forward: Option<memmap2::Mmap>,
    /// String dict offset table.
    pub string_offsets: Vec<u64>,
    /// String dict length table.
    pub string_lens: Vec<u32>,
    /// Memory-mapped subjects.fwd.
    pub subject_forward: Option<memmap2::Mmap>,
    /// Subject dict offset table.
    pub subject_offsets: Vec<u64>,
    /// Subject dict length table.
    pub subject_lens: Vec<u32>,
}

impl SpotQuery {
    /// Load a SpotQuery from the index directory.
    ///
    /// Expects the following files in `run_dir`:
    /// - `predicates.json`
    /// - `strings.fwd`, `strings.idx`
    /// - `subjects.fwd`, `subjects.idx`
    ///
    /// And per-graph indexes in `index_dir`:
    /// - `graph_{g_id}/spot/branch.fbr`
    pub fn load(run_dir: &Path, index_dir: &Path) -> io::Result<Self> {
        // Load predicate ids (id -> IRI)
        let pred_path = run_dir.join("predicates.json");
        let predicates = if pred_path.exists() {
            let bytes = std::fs::read(&pred_path)?;
            let by_id: Vec<String> = serde_json::from_slice(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let mut dict = PredicateDict::new();
            for iri in &by_id {
                dict.get_or_insert(iri);
            }
            dict
        } else {
            PredicateDict::new()
        };

        // Load string dict (forward file + index)
        let str_fwd_path = run_dir.join("strings.fwd");
        let str_idx_path = run_dir.join("strings.idx");
        let (string_forward, string_offsets, string_lens) = if str_idx_path.exists() {
            let (offsets, lens) = read_forward_index(&str_idx_path)?;
            let mmap = if str_fwd_path.exists() {
                let file = std::fs::File::open(&str_fwd_path)?;
                Some(unsafe { memmap2::Mmap::map(&file)? })
            } else {
                None
            };
            (mmap, offsets, lens)
        } else {
            (None, Vec::new(), Vec::new())
        };

        // Load subject dict (forward file + index)
        let subj_fwd_path = run_dir.join("subjects.fwd");
        let subj_idx_path = run_dir.join("subjects.idx");
        let (subject_forward, subject_offsets, subject_lens) = if subj_idx_path.exists() {
            let (offsets, lens) = read_forward_index(&subj_idx_path)?;
            let mmap = if subj_fwd_path.exists() {
                let file = std::fs::File::open(&subj_fwd_path)?;
                Some(unsafe { memmap2::Mmap::map(&file)? })
            } else {
                None
            };
            (mmap, offsets, lens)
        } else {
            (None, Vec::new(), Vec::new())
        };

        // Load per-graph branch manifests
        let mut branches = HashMap::new();
        for entry in std::fs::read_dir(index_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("graph_") {
                if let Some(g_id_str) = name.strip_prefix("graph_") {
                    if let Ok(g_id) = g_id_str.parse::<u32>() {
                        let spot_dir = entry.path().join("spot");
                        if spot_dir.exists() {
                            if let Ok(branch_path) = find_branch_file(&spot_dir) {
                                let manifest = read_branch_manifest(&branch_path)?;
                                branches.insert(g_id, manifest);
                            }
                        }
                    }
                }
            }
        }

        Ok(Self {
            branches,
            predicates,
            string_forward,
            string_offsets,
            string_lens,
            subject_forward,
            subject_offsets,
            subject_lens,
        })
    }

    /// Look up all facts for a subject by s_id in a specific graph.
    pub fn query_by_sid(&self, g_id: u32, s_id: u64) -> io::Result<Vec<FactRow>> {
        let manifest = match self.branches.get(&g_id) {
            Some(m) => m,
            None => return Ok(Vec::new()),
        };

        let range = manifest.find_leaves_for_subject(g_id, s_id);
        if range.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();

        for leaf_idx in range {
            let leaf_entry = &manifest.leaves[leaf_idx];
            let leaf_data = std::fs::read(&leaf_entry.path)?;
            let header = read_leaf_header(&leaf_data)?;

            // Scan leaflets that might contain this s_id
            for dir_entry in &header.leaflet_dir {
                let leaflet_data = &leaf_data[dir_entry.offset as usize
                    ..dir_entry.offset as usize + dir_entry.compressed_len as usize];
                let decoded = decode_leaflet(
                    leaflet_data,
                    header.p_width,
                    header.dt_width,
                    RunSortOrder::Spot,
                )?;

                for row in 0..decoded.row_count {
                    if decoded.s_ids[row] == s_id {
                        let o_kind = decoded.o_kinds[row];
                        let o_key = decoded.o_keys[row];
                        let dt = decoded.dt_values[row];
                        let p_id = decoded.p_ids[row];
                        let lang_id = decoded.lang_ids[row];
                        let i_val = decoded.i_values[row];

                        let p_iri = self
                            .predicates
                            .resolve(p_id)
                            .unwrap_or("<unknown>")
                            .to_string();

                        let o_display = self.project_object(o_kind, o_key, dt);

                        results.push(FactRow {
                            s_id,
                            p_id,
                            p_iri,
                            o_kind,
                            o_key,
                            o_display,
                            dt,
                            t: decoded.t_values[row],
                            lang_id,
                            i: i_val,
                        });
                    }
                }
            }
        }

        Ok(results)
    }

    /// Look up all facts for a subject by IRI (brute-force scan).
    ///
    /// Iterates all entries in `subjects.idx` + `subjects.fwd` to find the
    /// IRI by string comparison. O(N) in subject count.
    pub fn query_by_iri(&self, iri: &str, g_id: u32) -> io::Result<Vec<FactRow>> {
        let s_id = match self.find_subject_id(iri)? {
            Some(id) => id,
            None => return Ok(Vec::new()),
        };
        self.query_by_sid(g_id, s_id)
    }

    /// Brute-force search for a subject IRI → s_id.
    pub fn find_subject_id(&self, iri: &str) -> io::Result<Option<u64>> {
        let mmap = match &self.subject_forward {
            Some(m) => m,
            None => return Ok(None),
        };

        for (id, (&offset, &len)) in self
            .subject_offsets
            .iter()
            .zip(self.subject_lens.iter())
            .enumerate()
        {
            let entry = read_forward_entry(mmap, offset, len)?;
            if entry == iri {
                return Ok(Some(id as u64));
            }
        }

        Ok(None)
    }

    /// Project an object (o_kind, o_key) pair to a human-readable string.
    fn project_object(&self, o_kind: u8, o_key: u64, _dt: u32) -> String {
        let kind = ObjKind::from_u8(o_kind);
        let key = ObjKey::from_u64(o_key);
        match kind {
            ObjKind::MIN => "MIN".to_string(),
            ObjKind::NULL => "null".to_string(),
            ObjKind::BOOL => {
                if o_key == 0 {
                    "false".to_string()
                } else {
                    "true".to_string()
                }
            }
            ObjKind::NUM_INT => {
                format!("{}", key.decode_i64())
            }
            ObjKind::NUM_F64 => {
                format!("{}", key.decode_f64())
            }
            ObjKind::REF_ID => {
                let id = key.decode_u32_id();
                self.resolve_subject_iri(id as u64)
                    .unwrap_or_else(|_| format!("<iri:{}>", id))
            }
            ObjKind::LEX_ID => {
                let id = key.decode_u32_id();
                self.resolve_string_value(id)
                    .unwrap_or_else(|_| format!("<lex:{}>", id))
            }
            ObjKind::DATE => {
                format!("date(days={})", key.decode_date())
            }
            ObjKind::TIME => {
                format!("time(micros={})", key.decode_time())
            }
            ObjKind::DATE_TIME => {
                format!("datetime(micros={})", key.decode_datetime())
            }
            ObjKind::JSON_ID => {
                let id = key.decode_u32_id();
                self.resolve_string_value(id)
                    .unwrap_or_else(|_| format!("<json:{}>", id))
            }
            ObjKind::NUM_BIG => {
                format!("numbig(handle={})", key.decode_u32_id())
            }
            _ => format!("unknown(kind={:#x}, key={})", o_kind, o_key),
        }
    }

    /// Resolve a subject IRI by s_id from the mmap'd forward file.
    fn resolve_subject_iri(&self, s_id: u64) -> io::Result<String> {
        let mmap = self
            .subject_forward
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "subjects.fwd not loaded"))?;

        let idx = s_id as usize;
        if idx >= self.subject_offsets.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "s_id {} out of range (max {})",
                    s_id,
                    self.subject_offsets.len()
                ),
            ));
        }

        let entry = read_forward_entry(mmap, self.subject_offsets[idx], self.subject_lens[idx])?;
        Ok(entry.to_string())
    }

    /// Resolve a string value by id from the mmap'd forward file.
    fn resolve_string_value(&self, str_id: u32) -> io::Result<String> {
        let mmap = self
            .string_forward
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "strings.fwd not loaded"))?;

        let idx = str_id as usize;
        if idx >= self.string_offsets.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "str_id {} out of range (max {})",
                    str_id,
                    self.string_offsets.len()
                ),
            ));
        }

        let entry = read_forward_entry(mmap, self.string_offsets[idx], self.string_lens[idx])?;
        Ok(entry.to_string())
    }
}

/// Format a `FactRow` for display.
impl std::fmt::Display for FactRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "s:{} p:{} ({}) o:{} dt:{} t:{}",
            self.s_id, self.p_id, self.p_iri, self.o_display, self.dt, self.t,
        )?;
        if self.lang_id != 0 {
            write!(f, " lang:{}", self.lang_id)?;
        }
        if self.i != ListIndex::none().as_i32() {
            write!(f, " i:{}", self.i)?;
        }
        Ok(())
    }
}
