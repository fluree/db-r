//! Integration tests for V3 (FLI3) index format via the bulk import pipeline.
//!
//! Exercises: `fluree.create("db").import(path).index_format_version(3).execute()`
//! Verifies:
//! - V3 artifacts (FLI3 leaves) are produced with correct magic bytes
//! - Full E2E: import → FIR6 root → load → V3 cursor → SPARQL query → results

#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
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

// ============================================================================
// E2E: import V3 → load → query → verify results
// ============================================================================

/// Full end-to-end: import TTL with V3 format → load ledger → run queries → verify.
///
/// Validates the complete V3 read path: FIR6 root decode → BinaryIndexStoreV6 load →
/// BinaryCursorV3 scan → decode_value_v3 → query results.
///
/// Covers:
/// - Plain xsd:string (schema:name "Alice")
/// - rdf:langString (schema:description "A user"@en)
/// - IRI reference / @id (rdf:type → ex:User)
/// - Integer literal (schema:age 42)
#[tokio::test]
async fn import_v3_and_query() {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");

    // TTL with diverse value types for decode_value_v3 coverage.
    let ttl = r#"
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:alice a ex:User ;
    schema:name "Alice" ;
    schema:description "A user"@en ;
    schema:age 42 ;
    ex:friend ex:bob .

ex:bob a ex:User ;
    schema:name "Bob" ;
    schema:description "Another user"@de ;
    schema:age 22 .
"#;

    let ttl_path = write_ttl(data_dir.path(), "typed.ttl", ttl);

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    // Import with V3 format.
    let result = fluree
        .create("test/v3-query:main")
        .import(&ttl_path)
        .threads(1)
        .memory_budget_mb(128)
        .cleanup(false)
        .index_format_version(3)
        .execute()
        .await
        .expect("V3 import should succeed");

    assert!(result.t > 0, "should have at least one commit");
    assert!(result.root_id.is_some(), "index should have been built");

    // Verify FIR6 root was produced (not IRB1).
    let fir6_files = find_files_with_magic(db_dir.path(), b"FIR6");
    assert!(
        !fir6_files.is_empty(),
        "expected FIR6 root file, found none under {}",
        db_dir.path().display()
    );

    // Load the ledger (this triggers FIR6 → BinaryIndexStoreV6 loading).
    let ledger = fluree
        .ledger("test/v3-query:main")
        .await
        .expect("load V3 ledger");

    // ── Query 1: plain xsd:string ──
    let names_result = support::query_sparql(
        &fluree,
        &ledger,
        r#"
        PREFIX schema: <http://schema.org/>
        SELECT ?name WHERE { ?s schema:name ?name }
        ORDER BY ?name
        "#,
    )
    .await
    .expect("string query");
    let names_json = names_result
        .to_sparql_json(&ledger.snapshot)
        .expect("format sparql json");
    let bindings = names_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    let names: Vec<&str> = bindings
        .iter()
        .map(|b| b["name"]["value"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["Alice", "Bob"], "xsd:string query failed");

    // ── Query 1b: bound xsd:string object (must filter correctly) ──
    let alice_subject_result = support::query_sparql(
        &fluree,
        &ledger,
        r#"
        PREFIX schema: <http://schema.org/>
        SELECT ?s WHERE { ?s schema:name "Alice" }
        "#,
    )
    .await
    .expect("bound string object query");
    let alice_subject_json = alice_subject_result
        .to_sparql_json(&ledger.snapshot)
        .expect("format sparql json");
    let alice_subject_bindings = alice_subject_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    assert_eq!(
        alice_subject_bindings.len(),
        1,
        "expected exactly one subject for name=\"Alice\""
    );
    let alice_s = alice_subject_bindings[0]["s"]["value"].as_str().unwrap();
    assert!(
        alice_s == "http://example.org/ns/alice" || alice_s == "ex:alice",
        "bound string object scan mismatch: {alice_s}"
    );

    // ── Query 2: rdf:langString ──
    let desc_result = support::query_sparql(
        &fluree,
        &ledger,
        r#"
        PREFIX schema: <http://schema.org/>
        SELECT ?desc WHERE { ?s schema:description ?desc }
        ORDER BY ?desc
        "#,
    )
    .await
    .expect("langString query");
    let desc_json = desc_result
        .to_sparql_json(&ledger.snapshot)
        .expect("format sparql json");
    let desc_bindings = desc_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    let descs: Vec<&str> = desc_bindings
        .iter()
        .map(|b| b["desc"]["value"].as_str().unwrap())
        .collect();
    assert_eq!(
        descs,
        vec!["A user", "Another user"],
        "rdf:langString query failed"
    );
    // Verify language tags are preserved.
    let langs: Vec<&str> = desc_bindings
        .iter()
        .filter_map(|b| b["desc"]["xml:lang"].as_str())
        .collect();
    assert_eq!(langs, vec!["en", "de"], "language tags not preserved");

    // ── Query 2b: bound rdf:langString object + lang constraint ──
    let alice_desc_subject_result = support::query_sparql(
        &fluree,
        &ledger,
        r#"
        PREFIX schema: <http://schema.org/>
        SELECT ?s WHERE { ?s schema:description "A user"@en }
        "#,
    )
    .await
    .expect("bound langString query");
    let alice_desc_subject_json = alice_desc_subject_result
        .to_sparql_json(&ledger.snapshot)
        .expect("format sparql json");
    let alice_desc_subject_bindings = alice_desc_subject_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    assert_eq!(
        alice_desc_subject_bindings.len(),
        1,
        "expected exactly one subject for description=\"A user\"@en"
    );
    let alice_s2 = alice_desc_subject_bindings[0]["s"]["value"].as_str().unwrap();
    assert!(
        alice_s2 == "http://example.org/ns/alice" || alice_s2 == "ex:alice",
        "bound langString object scan mismatch: {alice_s2}"
    );

    // ── Query 3: IRI reference (rdf:type) ──
    let type_result = support::query_sparql(
        &fluree,
        &ledger,
        r#"
        PREFIX ex: <http://example.org/ns/>
        SELECT ?s WHERE { ?s a ex:User }
        ORDER BY ?s
        "#,
    )
    .await
    .expect("IRI ref query");
    let type_json = type_result
        .to_sparql_json(&ledger.snapshot)
        .expect("format sparql json");
    let type_bindings = type_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    assert_eq!(type_bindings.len(), 2, "expected 2 ex:User instances");
    // Subjects should be IRIs.
    for b in type_bindings {
        assert_eq!(
            b["s"]["type"].as_str().unwrap(),
            "uri",
            "subject should be URI type"
        );
    }

    // ── Query 4: integer literal ──
    let age_result = support::query_sparql(
        &fluree,
        &ledger,
        r#"
        PREFIX schema: <http://schema.org/>
        PREFIX ex: <http://example.org/ns/>
        SELECT ?age WHERE { ex:alice schema:age ?age }
        "#,
    )
    .await
    .expect("integer query");
    let age_json = age_result
        .to_sparql_json(&ledger.snapshot)
        .expect("format sparql json");
    let age_bindings = age_json["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    assert_eq!(age_bindings.len(), 1, "expected 1 age result");
    let age_val = age_bindings[0]["age"]["value"].as_str().expect("age value");
    assert_eq!(age_val, "42", "integer literal value mismatch");

    // ── Query 5: JSON-LD query (cross-check scan path) ──
    let jld_result = support::query_jsonld(
        &fluree,
        &ledger,
        &json!({
            "@context": {
                "ex": "http://example.org/ns/",
                "schema": "http://schema.org/"
            },
            "select": ["?name"],
            "where": { "schema:name": "?name" }
        }),
    )
    .await
    .expect("JSON-LD query");
    let jld_json = jld_result
        .to_jsonld(&ledger.snapshot)
        .expect("format jsonld");
    let mut jld_names: Vec<String> = jld_json
        .as_array()
        .expect("array")
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    jld_names.sort();
    assert_eq!(jld_names, vec!["Alice", "Bob"], "JSON-LD query failed");
}
