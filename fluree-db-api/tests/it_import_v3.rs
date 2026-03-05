//! Integration tests for V3 (FLI3) index format via the bulk import pipeline.
//!
//! Exercises: `fluree.create("db").import(path).index_format_version(3).execute()`
//! Verifies: V3 artifacts (FLI3 leaves) are produced with correct magic bytes
//! and segmentation properties.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use std::io::Write;
use std::path::Path;

fn write_ttl(dir: &Path, name: &str, content: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).expect("create ttl file");
    f.write_all(content.as_bytes()).expect("write ttl");
    path
}

/// Recursively find files whose first 4 bytes match the given magic.
fn find_files_with_magic(dir: &Path, magic: &[u8; 4]) -> Vec<std::path::PathBuf> {
    let mut results = Vec::new();
    scan_dir_recursive(dir, magic, &mut results);
    results
}

fn scan_dir_recursive(dir: &Path, magic: &[u8; 4], results: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            scan_dir_recursive(&path, magic, results);
        } else if path.is_file() {
            if let Ok(data) = std::fs::read(&path) {
                if data.len() >= 4 && &data[0..4] == magic {
                    results.push(path);
                }
            }
        }
    }
}

#[tokio::test]
async fn import_v3_produces_fli3_artifacts() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    let ttl = r#"
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:alice a ex:User ;
    schema:name "Alice" ;
    schema:age 42 .

ex:bob a ex:User ;
    schema:name "Bob" ;
    schema:age 22 .

ex:cam a ex:User ;
    schema:name "Cam" ;
    schema:age 34 ;
    ex:friend ex:alice, ex:bob .
"#;

    let ttl_path = write_ttl(data_dir.path(), "people.ttl", ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    // Import with V3 format.
    let result = fluree
        .create("test/import-v3:main")
        .import(&ttl_path)
        .threads(1)
        .memory_budget_mb(128)
        .cleanup(false)
        .index_format_version(3)
        .execute()
        .await;

    assert!(result.is_ok(), "V3 import failed: {:?}", result.err());

    // ---- Byte-level artifact inspection ----
    //
    // Scan the file-backed CAS directory for FLI3 leaf files and FBR3 branch files.
    // The CAS layout is: db_dir/data/index-leaf/<cid> (or similar content-addressed paths).
    // Since the exact CAS layout may vary, scan the entire db_dir for matching magic.

    let fli3_files = find_files_with_magic(db_dir.path(), b"FLI3");
    assert!(
        !fli3_files.is_empty(),
        "expected at least one FLI3 leaf file in CAS, found none under {}",
        db_dir.path().display()
    );

    // Verify first FLI3 file has valid header.
    use fluree_db_binary_index::format::leaf_v3::{
        decode_leaf_dir_v3, decode_leaf_header_v3, LEAF_V3_MAGIC,
    };

    for fli3_path in &fli3_files {
        let data = std::fs::read(fli3_path).expect("read FLI3 leaf file");
        assert_eq!(&data[0..4], LEAF_V3_MAGIC, "magic mismatch");

        let header = decode_leaf_header_v3(&data).expect("decode FLI3 header");
        assert!(
            header.leaflet_count > 0,
            "leaf must have at least one leaflet"
        );
        assert!(header.total_rows > 0, "leaf must have rows");

        let dir_entries =
            decode_leaf_dir_v3(&data, &header).expect("decode FLI3 leaflet directory");
        assert_eq!(dir_entries.len(), header.leaflet_count as usize);

        // Verify each leaflet has column refs.
        for entry in &dir_entries {
            assert!(
                !entry.column_refs.is_empty(),
                "leaflet must have column blocks"
            );
            assert!(entry.row_count > 0, "leaflet must have rows");
        }

        // For POST order, verify predicate-homogeneous segmentation.
        use fluree_db_binary_index::format::run_record::RunSortOrder;
        if header.order == RunSortOrder::Post {
            for entry in &dir_entries {
                assert!(
                    entry.p_const.is_some(),
                    "POST leaflets must have p_const set"
                );
            }
        }

        // For OPST order, verify type-homogeneous segmentation.
        if header.order == RunSortOrder::Opst {
            for entry in &dir_entries {
                assert!(
                    entry.o_type_const.is_some(),
                    "OPST leaflets must have o_type_const set"
                );
            }
        }
    }

    // FBR3 branch files are NOT uploaded to CAS for the default graph (g_id=0) —
    // the branch is embedded inline in the root. FBR3 files would only appear
    // for named graphs (g_id != 0). For this single-graph test, we verify that
    // FLI3 leaves were produced and are structurally valid (above assertions).
}
