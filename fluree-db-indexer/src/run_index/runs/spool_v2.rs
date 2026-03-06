//! V2 spool remap: reads V1 spool records, converts to V2 via OTypeRegistry,
//! applies V2-aware remap, and writes to `MultiOrderRunWriterV2`.
//!
//! This is the bridge between the existing import parser (which produces V1
//! spool records) and the V2 index build pipeline.

use super::run_writer_v2::{MultiOrderRunWriterV2, MultiOrderRunWriterV2WithOp};
use super::spool::{remap_record, SpoolReader, StringRemap, SubjectRemap};
use fluree_db_binary_index::format::run_record::RunRecord;
use fluree_db_binary_index::format::run_record_v2::RunRecordV2;
use fluree_db_core::o_type_registry::OTypeRegistry;
use std::io;
use std::path::Path;

/// Remap V2-aware: converts a V1 `RunRecord` to `RunRecordV2` and applies
/// global remap on the V2 record.
///
/// The OType-aware remap logic differs from V1:
/// - Subject remap on `s_id` (same as V1)
/// - If `o_type.is_iri_ref()` or `o_type.is_blank_node()`: subject remap on `o_key`
/// - If `o_type.is_dict_backed()` AND NOT a ref/bnode: string remap on `o_key` (u32 range)
/// - If `o_type.is_embedded()`: no remap on `o_key`
#[inline]
pub fn remap_v1_to_v2<S: SubjectRemap + ?Sized, R: StringRemap + ?Sized>(
    v1: &RunRecord,
    registry: &OTypeRegistry,
    subject_remap: &S,
    string_remap: &R,
    lang_remap: Option<&[u16]>,
) -> io::Result<RunRecordV2> {
    // First do V1 remap on the V1 record (subject + string remaps).
    let mut remapped_v1 = *v1;
    remap_record(&mut remapped_v1, subject_remap, string_remap)?;

    // Apply language remap before V2 conversion (so lang_id is global).
    if let Some(lr) = lang_remap {
        if !lr.is_empty() && remapped_v1.lang_id != 0 {
            if let Some(&global_id) = lr.get(remapped_v1.lang_id as usize) {
                remapped_v1.lang_id = global_id;
            }
        }
    }

    // Convert to V2 (with global IDs already in place).
    Ok(RunRecordV2::from_v1(&remapped_v1, registry))
}

/// Read a V1 sorted commit file (.fsc), convert each record to V2, and
/// write to a `MultiOrderRunWriterV2`.
///
/// This is the V2 equivalent of `remap_commit_to_runs()` from spool.rs.
#[allow(clippy::too_many_arguments)]
pub fn remap_commit_to_runs_v2<S: SubjectRemap, R: StringRemap>(
    commit_path: &Path,
    record_count: u64,
    subject_remap: &S,
    string_remap: &R,
    lang_remap: &[u16],
    registry: &OTypeRegistry,
    writer: &mut MultiOrderRunWriterV2,
) -> io::Result<u64> {
    let reader = SpoolReader::open(commit_path, record_count)?;
    let lang_remap_opt = if lang_remap.is_empty() {
        None
    } else {
        Some(lang_remap)
    };

    let mut count = 0u64;
    for result in reader {
        let v1_record = result?;
        let v2_record = remap_v1_to_v2(
            &v1_record,
            registry,
            subject_remap,
            string_remap,
            lang_remap_opt,
        )?;
        writer.push(v2_record)?;
        count += 1;
    }

    debug_assert_eq!(
        count, record_count,
        "expected {record_count} records, got {count}"
    );
    Ok(count)
}

/// Read a V1 sorted commit file (.fsc), convert each record to V2 with op
/// sideband, and write to a `MultiOrderRunWriterV2WithOp`.
///
/// Same as [`remap_commit_to_runs_v2`] but preserves the V1 record's `op`
/// field (1=assert, 0=retract) through to the run file. Used by the rebuild
/// path where retractions must survive into the merge phase.
#[allow(clippy::too_many_arguments)]
pub(crate) fn remap_commit_to_runs_v2_with_op<S: SubjectRemap, R: StringRemap>(
    commit_path: &Path,
    record_count: u64,
    subject_remap: &S,
    string_remap: &R,
    lang_remap: &[u16],
    registry: &OTypeRegistry,
    writer: &mut MultiOrderRunWriterV2WithOp,
) -> io::Result<u64> {
    let reader = SpoolReader::open(commit_path, record_count)?;
    let lang_remap_opt = if lang_remap.is_empty() {
        None
    } else {
        Some(lang_remap)
    };

    let mut count = 0u64;
    for result in reader {
        let v1_record = result?;
        let op = v1_record.op;
        let v2_record = remap_v1_to_v2(
            &v1_record,
            registry,
            subject_remap,
            string_remap,
            lang_remap_opt,
        )?;
        writer.push(v2_record, op)?;
        count += 1;
    }

    debug_assert_eq!(
        count, record_count,
        "expected {record_count} records, got {count}"
    );
    Ok(count)
}

/// Read a V1 unsorted spool file, convert to V2, and write to runs.
///
/// V2 equivalent of `remap_spool_to_runs()`.
pub fn remap_spool_to_runs_v2<S: SubjectRemap, R: StringRemap>(
    spool_path: &Path,
    record_count: u64,
    subject_remap: &S,
    string_remap: &R,
    lang_remap: &[u16],
    registry: &OTypeRegistry,
    writer: &mut MultiOrderRunWriterV2,
) -> io::Result<u64> {
    let reader = SpoolReader::open(spool_path, record_count)?;
    let lang_remap_opt = if lang_remap.is_empty() {
        None
    } else {
        Some(lang_remap)
    };

    let mut count = 0u64;
    for result in reader {
        let v1_record = result?;
        let v2_record = remap_v1_to_v2(
            &v1_record,
            registry,
            subject_remap,
            string_remap,
            lang_remap_opt,
        )?;
        writer.push(v2_record)?;
        count += 1;
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::ids::DatatypeDictId;
    use fluree_db_core::o_type::OType;
    use fluree_db_core::subject_id::SubjectId;
    use fluree_db_core::value_id::{ObjKey, ObjKind};

    /// Identity remap (no actual remapping).
    struct IdentitySubjectRemap;
    impl SubjectRemap for IdentitySubjectRemap {
        fn len(&self) -> usize {
            usize::MAX
        }
        fn is_empty(&self) -> bool {
            false
        }
        fn get(&self, idx: usize) -> io::Result<u64> {
            Ok(idx as u64)
        }
    }

    struct IdentityStringRemap;
    impl StringRemap for IdentityStringRemap {
        fn len(&self) -> usize {
            usize::MAX
        }
        fn is_empty(&self) -> bool {
            false
        }
        fn get(&self, idx: usize) -> io::Result<u32> {
            Ok(idx as u32)
        }
    }

    #[test]
    fn remap_integer_passthrough() {
        let registry = OTypeRegistry::builtin_only();
        let v1 = RunRecord::new(
            0,
            SubjectId(100),
            5,
            ObjKind::NUM_INT,
            ObjKey::encode_i64(42),
            1,
            true,
            DatatypeDictId::INTEGER.as_u16(),
            0,
            None,
        );

        let v2 = remap_v1_to_v2(
            &v1,
            &registry,
            &IdentitySubjectRemap,
            &IdentityStringRemap,
            None,
        )
        .unwrap();

        assert_eq!(v2.o_type(), OType::XSD_INTEGER);
        assert_eq!(v2.o_key, ObjKey::encode_i64(42).as_u64());
        assert_eq!(v2.s_id.as_u64(), 100);
    }

    #[test]
    fn remap_iri_ref() {
        let registry = OTypeRegistry::builtin_only();
        // REF_ID with o_key = chunk-local subject ID 50.
        let v1 = RunRecord::new(
            0,
            SubjectId(10),
            5,
            ObjKind::REF_ID,
            ObjKey::from_u64(50), // chunk-local subject ID
            1,
            true,
            DatatypeDictId::ID.as_u16(),
            0,
            None,
        );

        // Identity remap: local ID 50 → global 50.
        let v2 = remap_v1_to_v2(
            &v1,
            &registry,
            &IdentitySubjectRemap,
            &IdentityStringRemap,
            None,
        )
        .unwrap();

        assert_eq!(v2.o_type(), OType::IRI_REF);
        assert_eq!(v2.o_key, 50); // remapped (identity in this case)
    }

    #[test]
    fn remap_lang_string_with_remap() {
        let registry = OTypeRegistry::builtin_only();
        // LEX_ID with dt=LANG_STRING, lang_id=1 (chunk-local).
        let v1 = RunRecord::new(
            0,
            SubjectId(10),
            5,
            ObjKind::LEX_ID,
            ObjKey::encode_u32_id(42), // chunk-local string ID
            1,
            true,
            DatatypeDictId::LANG_STRING.as_u16(),
            1, // chunk-local lang_id
            None,
        );

        // lang_remap: chunk lang_id 1 → global lang_id 5.
        let lang_remap = vec![0, 5]; // index 0 = unused, index 1 = 5

        let v2 = remap_v1_to_v2(
            &v1,
            &registry,
            &IdentitySubjectRemap,
            &IdentityStringRemap,
            Some(&lang_remap),
        )
        .unwrap();

        // o_type should encode lang_id=5 (global).
        assert!(v2.o_type().is_lang_string());
        assert_eq!(v2.o_type().lang_id(), Some(5));
    }
}
