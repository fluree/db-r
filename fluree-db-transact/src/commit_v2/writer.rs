//! Commit v2 writer: Commit -> binary blob.
//!
//! Uses the codec from `fluree_db_novelty::commit_v2` to encode flakes
//! with Sid-direct encoding (no NamespaceRegistry needed).

use fluree_db_novelty::commit_v2::envelope;
use fluree_db_novelty::commit_v2::format::{
    CommitV2Footer, CommitV2Header, DictLocation, FLAG_ZSTD, FOOTER_LEN, HASH_LEN, HEADER_LEN,
    VERSION,
};
use fluree_db_novelty::commit_v2::op_codec::{encode_op, CommitDicts};
use fluree_db_novelty::commit_v2::CommitV2Error;
use fluree_db_novelty::Commit;
use sha2::{Digest, Sha256};

/// Result of writing a v2 commit blob.
pub struct CommitWriteResult {
    /// The complete binary blob.
    pub bytes: Vec<u8>,
    /// Hex-encoded SHA-256 of the blob (excluding the trailing 32-byte hash).
    /// Suitable for use as a content address.
    pub content_hash_hex: String,
}

/// Write a commit as a v2 binary blob.
///
/// Encodes flakes using Sid-direct encoding (namespace_code + name dict entries).
/// No NamespaceRegistry is needed — Sid fields are read directly from flakes.
///
/// Returns `CommitWriteResult` containing the blob bytes and the hex-encoded
/// SHA-256 content hash.
pub fn write_commit(commit: &Commit, compress: bool) -> Result<CommitWriteResult, CommitV2Error> {
    let op_count = commit.flakes.len();

    // 1. Serialize envelope (binary)
    let envelope_bytes = {
        let _span = tracing::debug_span!("v2_write_envelope").entered();
        let mut buf = Vec::new();
        envelope::encode_envelope(commit, &mut buf)?;
        buf
    };

    // 2. Encode all ops into a buffer, populating dictionaries
    let (ops_raw, dicts) = {
        let _span = tracing::debug_span!("v2_encode_ops", op_count).entered();
        let mut dicts = CommitDicts::new();
        let mut ops_raw = Vec::new();
        for flake in &commit.flakes {
            encode_op(flake, &mut dicts, &mut ops_raw)?;
        }
        (ops_raw, dicts)
    };

    // 3. Optionally compress ops
    let (ops_section, is_compressed) = if compress && !ops_raw.is_empty() {
        let _span = tracing::debug_span!("v2_compress_ops", raw_bytes = ops_raw.len()).entered();
        let compressed = zstd::encode_all(ops_raw.as_slice(), 3)
            .map_err(CommitV2Error::CompressionFailed)?;
        // Only use compressed if it's actually smaller
        if compressed.len() < ops_raw.len() {
            tracing::debug!(
                raw = ops_raw.len(),
                compressed = compressed.len(),
                ratio = format_args!("{:.1}x", ops_raw.len() as f64 / compressed.len() as f64),
                "ops compressed"
            );
            (compressed, true)
        } else {
            (ops_raw, false)
        }
    } else {
        (ops_raw, false)
    };

    // 4. Serialize dictionaries
    let dict_bytes: Vec<Vec<u8>> = [
        &dicts.graph,
        &dicts.subject,
        &dicts.predicate,
        &dicts.datatype,
        &dicts.object_ref,
    ]
    .iter()
    .map(|d| d.serialize())
    .collect();

    // 5. Calculate total size and allocate output
    let total_size = HEADER_LEN
        + envelope_bytes.len()
        + ops_section.len()
        + dict_bytes.iter().map(|d| d.len()).sum::<usize>()
        + FOOTER_LEN
        + HASH_LEN;
    let mut output = Vec::with_capacity(total_size);

    // 6. Write header
    let mut flags = 0u8;
    if is_compressed {
        flags |= FLAG_ZSTD;
    }
    let header = CommitV2Header {
        version: VERSION,
        flags,
        t: commit.t,
        op_count: commit.flakes.len() as u32,
        envelope_len: envelope_bytes.len() as u32,
    };
    let mut header_buf = [0u8; HEADER_LEN];
    header.write_to(&mut header_buf);
    output.extend_from_slice(&header_buf);

    // 7. Write envelope
    output.extend_from_slice(&envelope_bytes);

    // 8. Write ops section
    output.extend_from_slice(&ops_section);

    // 9. Write dictionaries, recording locations
    let mut dict_locations = [DictLocation::default(); 5];
    for (i, bytes) in dict_bytes.iter().enumerate() {
        dict_locations[i] = DictLocation {
            offset: output.len() as u64,
            len: bytes.len() as u32,
        };
        output.extend_from_slice(bytes);
    }

    // 10. Write footer
    let footer = CommitV2Footer {
        dicts: dict_locations,
        ops_section_len: ops_section.len() as u32,
    };
    let mut footer_buf = [0u8; FOOTER_LEN];
    footer.write_to(&mut footer_buf);
    output.extend_from_slice(&footer_buf);

    // 11. Compute SHA-256 of everything so far, append as trailing hash
    let hash = {
        let _span = tracing::debug_span!("v2_write_hash", blob_bytes = output.len()).entered();
        Sha256::digest(&output)
    };
    let hash_bytes: [u8; 32] = hash.into();
    let content_hash_hex = hex::encode(hash_bytes);
    output.extend_from_slice(&hash_bytes);

    debug_assert_eq!(output.len(), total_size);

    tracing::debug!(
        blob_bytes = output.len(),
        op_count,
        envelope_bytes = envelope_bytes.len(),
        ops_bytes = ops_section.len(),
        compressed = is_compressed,
        "v2 commit written"
    );

    Ok(CommitWriteResult {
        bytes: output,
        content_hash_hex,
    })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{Flake, FlakeMeta, FlakeValue, Sid};
    use fluree_db_novelty::commit_v2::{read_commit, read_commit_envelope, MAGIC};
    use fluree_db_novelty::{CommitRef, IndexRef};
    use std::collections::HashMap;

    fn make_test_commit(flakes: Vec<Flake>, t: i64) -> Commit {
        Commit::new("", t, flakes)
    }

    fn assert_flake_eq(a: &Flake, b: &Flake) {
        assert_eq!(a.s.namespace_code, b.s.namespace_code, "s namespace_code");
        assert_eq!(a.s.name.as_ref(), b.s.name.as_ref(), "s name");
        assert_eq!(a.p.namespace_code, b.p.namespace_code, "p namespace_code");
        assert_eq!(a.p.name.as_ref(), b.p.name.as_ref(), "p name");
        assert_eq!(a.dt.namespace_code, b.dt.namespace_code, "dt namespace_code");
        assert_eq!(a.dt.name.as_ref(), b.dt.name.as_ref(), "dt name");
        assert_eq!(a.o, b.o, "object value");
        assert_eq!(a.op, b.op, "op flag");
        assert_eq!(a.t, b.t, "t");
        // Compare metadata
        match (&a.m, &b.m) {
            (None, None) => {}
            (Some(am), Some(bm)) => {
                assert_eq!(am.lang, bm.lang, "meta lang");
                assert_eq!(am.i, bm.i, "meta i");
            }
            _ => panic!("meta mismatch: {:?} vs {:?}", a.m, b.m),
        }
    }

    #[test]
    fn test_round_trip_basic() {
        let flakes = vec![
            Flake::new(
                Sid::new(101, "Alice"),
                Sid::new(101, "name"),
                FlakeValue::String("Alice Smith".to_string()),
                Sid::new(2, "string"),
                1,
                true,
                None,
            ),
            Flake::new(
                Sid::new(101, "Alice"),
                Sid::new(101, "age"),
                FlakeValue::Long(30),
                Sid::new(2, "integer"),
                1,
                true,
                None,
            ),
        ];

        let commit = make_test_commit(flakes, 1);
        let result = write_commit(&commit, false).unwrap();
        assert!(!result.content_hash_hex.is_empty());

        // Verify magic
        assert_eq!(&result.bytes[0..4], &MAGIC);

        // Round-trip
        let decoded = read_commit(&result.bytes).unwrap();
        assert_eq!(decoded.t, 1);
        assert_eq!(decoded.flakes.len(), 2);
        for (orig, dec) in commit.flakes.iter().zip(decoded.flakes.iter()) {
            assert_flake_eq(orig, dec);
        }
    }

    #[test]
    fn test_round_trip_with_compression() {
        // Many similar flakes to make compression effective
        let flakes: Vec<Flake> = (0..100)
            .map(|i| {
                Flake::new(
                    Sid::new(101, &format!("node{}", i)),
                    Sid::new(101, "value"),
                    FlakeValue::Long(i),
                    Sid::new(2, "integer"),
                    5,
                    true,
                    None,
                )
            })
            .collect();

        let commit = make_test_commit(flakes, 5);
        let result_c = write_commit(&commit, true).unwrap();
        let result_u = write_commit(&commit, false).unwrap();

        // Compressed should be smaller (100 similar flakes)
        assert!(
            result_c.bytes.len() < result_u.bytes.len(),
            "compressed {} should be < uncompressed {}",
            result_c.bytes.len(),
            result_u.bytes.len()
        );

        // Hashes should differ (different blob contents)
        assert_ne!(result_c.content_hash_hex, result_u.content_hash_hex);

        // Both should round-trip
        let dec_c = read_commit(&result_c.bytes).unwrap();
        let dec_u = read_commit(&result_u.bytes).unwrap();
        assert_eq!(dec_c.flakes.len(), 100);
        assert_eq!(dec_u.flakes.len(), 100);
        for (orig, dec) in commit.flakes.iter().zip(dec_c.flakes.iter()) {
            assert_flake_eq(orig, dec);
        }
    }

    #[test]
    fn test_round_trip_ref_values() {
        let flakes = vec![Flake::new(
            Sid::new(101, "Alice"),
            Sid::new(101, "knows"),
            FlakeValue::Ref(Sid::new(101, "Bob")),
            Sid::new(1, "id"),
            1,
            true,
            None,
        )];

        let commit = make_test_commit(flakes, 1);
        let result = write_commit(&commit, false).unwrap();
        let decoded = read_commit(&result.bytes).unwrap();

        match &decoded.flakes[0].o {
            FlakeValue::Ref(sid) => {
                assert_eq!(sid.namespace_code, 101);
                assert_eq!(sid.name.as_ref(), "Bob");
            }
            other => panic!("expected Ref, got {:?}", other),
        }
    }

    #[test]
    fn test_round_trip_mixed_value_types() {
        let flakes = vec![
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "str"),
                FlakeValue::String("hello".into()),
                Sid::new(2, "string"),
                1, true, None,
            ),
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "num"),
                FlakeValue::Long(-42),
                Sid::new(2, "long"),
                1, true, None,
            ),
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "dbl"),
                FlakeValue::Double(3.14),
                Sid::new(2, "double"),
                1, true, None,
            ),
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "flag"),
                FlakeValue::Boolean(true),
                Sid::new(2, "boolean"),
                1, true, None,
            ),
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "empty"),
                FlakeValue::Null,
                Sid::new(2, "string"),
                1, false, None,
            ),
        ];

        let commit = make_test_commit(flakes, 1);
        let result = write_commit(&commit, false).unwrap();
        let decoded = read_commit(&result.bytes).unwrap();

        assert_eq!(decoded.flakes.len(), 5);
        for (orig, dec) in commit.flakes.iter().zip(decoded.flakes.iter()) {
            assert_flake_eq(orig, dec);
        }
    }

    #[test]
    fn test_round_trip_with_metadata() {
        let flakes = vec![
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "name"),
                FlakeValue::String("Alice".into()),
                Sid::new(3, "langString"),
                1, true,
                Some(FlakeMeta::with_lang("en")),
            ),
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "items"),
                FlakeValue::Long(42),
                Sid::new(2, "integer"),
                1, true,
                Some(FlakeMeta::with_index(0)),
            ),
            Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "items"),
                FlakeValue::Long(99),
                Sid::new(2, "integer"),
                1, true,
                Some(FlakeMeta {
                    lang: Some("de".into()),
                    i: Some(1),
                }),
            ),
        ];

        let commit = make_test_commit(flakes, 1);
        let result = write_commit(&commit, false).unwrap();
        let decoded = read_commit(&result.bytes).unwrap();

        assert_eq!(decoded.flakes.len(), 3);
        for (orig, dec) in commit.flakes.iter().zip(decoded.flakes.iter()) {
            assert_flake_eq(orig, dec);
        }
    }

    #[test]
    fn test_envelope_only_read() {
        let mut commit = make_test_commit(
            vec![Flake::new(
                Sid::new(101, "x"),
                Sid::new(101, "v"),
                FlakeValue::Long(1),
                Sid::new(2, "integer"),
                5, true, None,
            )],
            5,
        );
        commit.previous_ref = Some(CommitRef::new("prev-addr").with_id("fluree:commit:sha256:abc"));
        commit.namespace_delta = HashMap::from([(200, "ex:".to_string())]);

        let result = write_commit(&commit, false).unwrap();
        let envelope = read_commit_envelope(&result.bytes).unwrap();

        assert_eq!(envelope.t, 5);
        assert_eq!(
            envelope.previous_ref.as_ref().unwrap().address,
            "prev-addr"
        );
        assert_eq!(
            envelope.namespace_delta.get(&200),
            Some(&"ex:".to_string())
        );
    }

    #[test]
    fn test_hash_integrity() {
        let flakes = vec![Flake::new(
            Sid::new(101, "x"),
            Sid::new(101, "v"),
            FlakeValue::Long(1),
            Sid::new(2, "integer"),
            1, true, None,
        )];

        let commit = make_test_commit(flakes, 1);
        let mut result = write_commit(&commit, false).unwrap();

        // Corrupt a byte in the middle
        let mid = result.bytes.len() / 2;
        result.bytes[mid] ^= 0xFF;

        let read_result = read_commit(&result.bytes);
        assert!(
            read_result.is_err(),
            "corrupted blob should fail hash check"
        );
    }

    #[test]
    fn test_envelope_fields_round_trip() {
        let mut commit = make_test_commit(vec![], 10);
        commit.v = 2;
        commit.time = Some("2024-01-01T00:00:00Z".into());
        commit.txn = Some("fluree:file://txn/abc123.json".into());
        commit.index = Some(IndexRef::new("fluree:file://index/xyz").with_t(8));

        // Need at least one flake to avoid empty ops
        commit.flakes.push(Flake::new(
            Sid::new(101, "x"),
            Sid::new(101, "v"),
            FlakeValue::Long(1),
            Sid::new(2, "integer"),
            10, true, None,
        ));

        let result = write_commit(&commit, false).unwrap();
        let decoded = read_commit(&result.bytes).unwrap();

        assert_eq!(decoded.t, 10);
        assert_eq!(decoded.v, 2);
        assert_eq!(decoded.time.as_deref(), Some("2024-01-01T00:00:00Z"));
        assert_eq!(decoded.txn.as_deref(), Some("fluree:file://txn/abc123.json"));
        assert_eq!(decoded.index.as_ref().unwrap().address, "fluree:file://index/xyz");
        assert_eq!(decoded.index.as_ref().unwrap().t, Some(8));
    }

    #[test]
    fn test_large_commit() {
        // 1000 flakes with varied data
        let flakes: Vec<Flake> = (0..1000)
            .map(|i| {
                let value = if i % 3 == 0 {
                    FlakeValue::Long(i)
                } else if i % 3 == 1 {
                    FlakeValue::String(format!("value_{}", i))
                } else {
                    FlakeValue::Ref(Sid::new(101, &format!("ref_{}", i)))
                };
                let dt = if i % 3 == 2 {
                    Sid::new(1, "id")
                } else if i % 3 == 0 {
                    Sid::new(2, "integer")
                } else {
                    Sid::new(2, "string")
                };
                Flake::new(
                    Sid::new(101, &format!("s_{}", i)),
                    Sid::new(101, &format!("p_{}", i % 10)),
                    value,
                    dt,
                    42,
                    i % 5 != 0, // every 5th is a retract
                    None,
                )
            })
            .collect();

        let commit = make_test_commit(flakes, 42);
        let result = write_commit(&commit, true).unwrap();
        let decoded = read_commit(&result.bytes).unwrap();

        assert_eq!(decoded.flakes.len(), 1000);
        assert_eq!(decoded.t, 42);
        for (orig, dec) in commit.flakes.iter().zip(decoded.flakes.iter()) {
            assert_flake_eq(orig, dec);
        }
    }
}
