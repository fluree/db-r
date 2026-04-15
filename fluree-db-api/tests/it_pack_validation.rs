//! Validation tests for `stream_pack` — guard against silent empty packs.
//!
//! Run with:
//!   cargo test -p fluree-db-api --test it_pack_validation --features native

#![cfg(feature = "native")]

use fluree_db_api::pack::{full_ledger_pack_request, stream_pack, PackChunk};
use fluree_db_api::FlureeBuilder;
use fluree_db_core::pack::{
    decode_frame, read_stream_preamble, PackFrame, PackRequest, DEFAULT_MAX_PAYLOAD, PACK_PROTOCOL,
};
use serde_json::json;
use tokio::sync::mpsc;

/// Drain the pack stream receiver into a single Vec<u8>.
async fn drain_frames(mut rx: mpsc::Receiver<PackChunk>) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(chunk) = rx.recv().await {
        out.extend_from_slice(&chunk.expect("pack chunk"));
    }
    out
}

/// Decode all frames in a pack stream (no preamble required — empty-want
/// streams emit only Error + End frames with no preamble).
fn decode_all_frames_no_preamble(data: &[u8]) -> Vec<PackFrame> {
    // When validation fails in stream_pack, no preamble is written — the
    // stream is just Error + End. When it succeeds, there is a preamble.
    let mut pos = read_stream_preamble(data).unwrap_or(0);
    let mut frames = Vec::new();
    loop {
        let (frame, consumed) =
            decode_frame(&data[pos..], DEFAULT_MAX_PAYLOAD).expect("valid frame");
        pos += consumed;
        let is_end = matches!(frame, PackFrame::End);
        frames.push(frame);
        if is_end {
            break;
        }
    }
    frames
}

#[tokio::test]
async fn stream_pack_rejects_empty_want_with_error_frame() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("build fluree");

    let ledger_id = "pack-validation/empty-want:main";
    let src_db = fluree_db_core::LedgerSnapshot::genesis(ledger_id);
    let src_state = fluree_db_api::LedgerState::new(src_db, fluree_db_api::Novelty::new(0));
    let committed = fluree
        .insert(
            src_state,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:a", "ex:name": "A"}]
            }),
        )
        .await
        .expect("insert");
    assert_eq!(committed.receipt.t, 1);

    let handle = fluree.ledger_cached(ledger_id).await.expect("handle");

    let bad_request = PackRequest {
        protocol: PACK_PROTOCOL.to_string(),
        want: vec![],
        have: vec![],
        include_indexes: true,
        want_index_root_id: None,
        have_index_root_id: None,
    };

    let (tx, rx) = mpsc::channel(64);
    let stats = stream_pack(&fluree, &handle, &bad_request, tx).await;

    assert_eq!(stats.commits_sent, 0);
    assert_eq!(stats.txn_blobs_sent, 0);
    assert_eq!(stats.index_artifacts_sent, 0);

    let bytes = drain_frames(rx).await;
    let frames = decode_all_frames_no_preamble(&bytes);

    let has_error = frames
        .iter()
        .any(|f| matches!(f, PackFrame::Error(msg) if msg.contains("want list must not be empty")));
    assert!(
        has_error,
        "expected an Error frame for empty want, got frames: {:?}",
        frames
    );

    // The header-only silent-success path must not appear.
    let has_header = frames.iter().any(|f| matches!(f, PackFrame::Header(_)));
    assert!(
        !has_header,
        "invalid request must not produce a Header frame (silent empty pack): {:?}",
        frames
    );
}

#[tokio::test]
async fn full_ledger_pack_request_builds_valid_request() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("build fluree");

    let ledger_id = "pack-validation/full-helper:main";
    let src_db = fluree_db_core::LedgerSnapshot::genesis(ledger_id);
    let src_state = fluree_db_api::LedgerState::new(src_db, fluree_db_api::Novelty::new(0));
    fluree
        .insert(
            src_state,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:a", "ex:name": "A"}]
            }),
        )
        .await
        .expect("insert");

    let handle = fluree.ledger_cached(ledger_id).await.expect("handle");

    let request = full_ledger_pack_request(&handle, /* include_indexes */ false)
        .await
        .expect("build full-ledger request");

    assert_eq!(request.protocol, PACK_PROTOCOL);
    assert_eq!(request.want.len(), 1, "want should contain head commit CID");
    assert!(
        request.have.is_empty(),
        "have should be empty for a full archive"
    );
    assert!(!request.include_indexes);

    // Sanity: the built request passes validation.
    assert!(fluree_db_api::pack::validate_pack_request(&request).is_ok());
}

#[tokio::test]
async fn full_ledger_pack_request_errors_when_no_head_commit() {
    // Build a LedgerHandle directly from a genesis LedgerState with no
    // commits. This simulates the "empty ledger" edge case without
    // needing nameservice registration.
    let ledger_id = "pack-validation/no-head:main";
    let snapshot = fluree_db_core::LedgerSnapshot::genesis(ledger_id);
    let state = fluree_db_api::LedgerState::new(snapshot, fluree_db_api::Novelty::new(0));
    let handle = fluree_db_api::LedgerHandle::new(ledger_id.to_string(), state, None);

    let err = full_ledger_pack_request(&handle, false)
        .await
        .expect_err("expected error when ledger has no head commit");
    let msg = err.to_string();
    assert!(
        msg.contains("no head commit"),
        "error message should mention missing head commit, got: {msg}"
    );
}
