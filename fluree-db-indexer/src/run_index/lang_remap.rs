//! Language tag reconciliation across run files.
//!
//! Each run file has its own per-run `lang_id` mapping. Before merging,
//! we read all lang dicts and build a unified `LanguageTagDict` with
//! per-run remap tables.

use super::global_dict::LanguageTagDict;
use super::run_file::{deserialize_lang_dict, RunFileHeader, RUN_HEADER_LEN};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// Read all run file lang dicts and build a unified mapping.
///
/// Returns `(unified_dict, per_run_remaps)` where `per_run_remaps[i]`
/// is the remap table for run file `i`. Each remap table has
/// `remap[0] = 0` (sentinel) and `remap[local_id] = global_id`.
pub fn build_lang_remap(
    run_paths: &[PathBuf],
) -> io::Result<(LanguageTagDict, Vec<Vec<u16>>)> {
    let mut unified = LanguageTagDict::new();
    let mut remaps = Vec::with_capacity(run_paths.len());

    for path in run_paths {
        let local_dict = read_run_lang_dict(path)?;

        // Build remap: local_id → global_id
        // remap[0] = 0 always (sentinel for "no lang tag")
        let max_local_id = local_dict.len(); // number of tags (1-based)
        let mut remap = vec![0u16; (max_local_id as usize) + 1];

        for (local_id, tag) in local_dict.iter() {
            let global_id = unified.get_or_insert(Some(tag));
            remap[local_id as usize] = global_id;
        }

        remaps.push(remap);
    }

    Ok((unified, remaps))
}

/// Read just the header + lang dict from a run file (skip records).
fn read_run_lang_dict(path: &Path) -> io::Result<LanguageTagDict> {
    let mut file = std::fs::File::open(path)?;

    // Read header
    let mut header_buf = [0u8; RUN_HEADER_LEN];
    file.read_exact(&mut header_buf)?;
    let header = RunFileHeader::read_from(&header_buf)?;

    if header.lang_dict_len == 0 {
        return Ok(LanguageTagDict::new());
    }

    // Read lang dict
    file.seek(SeekFrom::Start(header.lang_dict_offset))?;
    let mut ld_buf = vec![0u8; header.lang_dict_len as usize];
    file.read_exact(&mut ld_buf)?;

    deserialize_lang_dict(&ld_buf)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_index::run_file::write_run_file;
    use crate::run_index::run_record::RunSortOrder;

    #[test]
    fn test_build_lang_remap_disjoint() {
        let dir = std::env::temp_dir().join("fluree_test_lang_remap_disjoint");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Run 0: en, fr
        let mut dict0 = LanguageTagDict::new();
        dict0.get_or_insert(Some("en"));
        dict0.get_or_insert(Some("fr"));
        let path0 = dir.join("run0.frn");
        write_run_file(&path0, &[], &dict0, RunSortOrder::Spot, 0, 0).unwrap();

        // Run 1: de, ja
        let mut dict1 = LanguageTagDict::new();
        dict1.get_or_insert(Some("de"));
        dict1.get_or_insert(Some("ja"));
        let path1 = dir.join("run1.frn");
        write_run_file(&path1, &[], &dict1, RunSortOrder::Spot, 0, 0).unwrap();

        let (unified, remaps) = build_lang_remap(&[path0, path1]).unwrap();

        // Unified should have 4 tags
        assert_eq!(unified.len(), 4);
        assert_eq!(unified.resolve(1), Some("en"));
        assert_eq!(unified.resolve(2), Some("fr"));
        assert_eq!(unified.resolve(3), Some("de"));
        assert_eq!(unified.resolve(4), Some("ja"));

        // Run 0 remap: 0→0, 1→1(en), 2→2(fr)
        assert_eq!(remaps[0], vec![0, 1, 2]);
        // Run 1 remap: 0→0, 1→3(de), 2→4(ja)
        assert_eq!(remaps[1], vec![0, 3, 4]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_build_lang_remap_overlapping() {
        let dir = std::env::temp_dir().join("fluree_test_lang_remap_overlap");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Run 0: en, fr
        let mut dict0 = LanguageTagDict::new();
        dict0.get_or_insert(Some("en"));
        dict0.get_or_insert(Some("fr"));
        let path0 = dir.join("run0.frn");
        write_run_file(&path0, &[], &dict0, RunSortOrder::Spot, 0, 0).unwrap();

        // Run 1: fr, de (fr overlaps)
        let mut dict1 = LanguageTagDict::new();
        dict1.get_or_insert(Some("fr"));
        dict1.get_or_insert(Some("de"));
        let path1 = dir.join("run1.frn");
        write_run_file(&path1, &[], &dict1, RunSortOrder::Spot, 0, 0).unwrap();

        let (unified, remaps) = build_lang_remap(&[path0, path1]).unwrap();

        // Unified should have 3 tags (en, fr, de)
        assert_eq!(unified.len(), 3);

        // Run 0: local 1(en)→1, local 2(fr)→2
        assert_eq!(remaps[0][1], 1);
        assert_eq!(remaps[0][2], 2);

        // Run 1: local 1(fr)→2 (deduped), local 2(de)→3
        assert_eq!(remaps[1][1], 2); // fr maps to same global id
        assert_eq!(remaps[1][2], 3);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_build_lang_remap_empty() {
        let dir = std::env::temp_dir().join("fluree_test_lang_remap_empty");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let dict = LanguageTagDict::new();
        let path = dir.join("run0.frn");
        write_run_file(&path, &[], &dict, RunSortOrder::Spot, 0, 0).unwrap();

        let (unified, remaps) = build_lang_remap(&[path]).unwrap();

        assert_eq!(unified.len(), 0);
        assert_eq!(remaps[0], vec![0]); // just sentinel

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_sentinel_always_zero() {
        let dir = std::env::temp_dir().join("fluree_test_lang_sentinel");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut dict = LanguageTagDict::new();
        dict.get_or_insert(Some("en"));
        let path = dir.join("run0.frn");
        write_run_file(&path, &[], &dict, RunSortOrder::Spot, 0, 0).unwrap();

        let (_, remaps) = build_lang_remap(&[path]).unwrap();

        // remap[0] must always be 0
        assert_eq!(remaps[0][0], 0);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
