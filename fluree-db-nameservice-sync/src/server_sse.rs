//! Adapter for Fluree server `/fluree/events` SSE payloads.
//!
//! The server's SSE payload schema is intentionally stable and independent from
//! internal `NsRecord` / `VgNsRecord` serialization. This module parses the
//! server-emitted JSON and converts it into canonical `fluree-db-nameservice`
//! types used by the sync layer.

use crate::watch::RemoteEvent;
use fluree_db_nameservice::{NsRecord, VgNsRecord, VgType};

#[derive(Debug, thiserror::Error)]
pub enum ServerSseParseError {
    #[error("invalid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
}

/// Parse a single raw SSE event from the server into an optional `RemoteEvent`.
///
/// Returns:
/// - `Ok(Some(..))` for recognized events
/// - `Ok(None)` for ignored events (keepalive / unknown event types)
/// - `Err(..)` for malformed server events of recognized types
pub fn parse_server_sse_event(
    event: &fluree_sse::SseEvent,
) -> Result<Option<RemoteEvent>, ServerSseParseError> {
    let Some(event_type) = event.event_type.as_deref() else {
        return Ok(None);
    };

    match event_type {
        "ns-record" => parse_ns_record(&event.data),
        "ns-retracted" => parse_ns_retracted(&event.data),
        _ => Ok(None),
    }
}

// ============================================================================
// Payload parsing (matches fluree-db-server/src/routes/events.rs)
// ============================================================================

#[derive(Debug, serde::Deserialize)]
struct NsRecordEnvelope {
    kind: String,
    record: serde_json::Value,
}

#[derive(Debug, serde::Deserialize)]
struct NsRetractedEnvelope {
    kind: String,
    alias: String,
}

#[derive(Debug, serde::Deserialize)]
struct LedgerSseRecord {
    /// Canonical alias, e.g. "books:main"
    alias: String,
    branch: String,
    commit_address: Option<String>,
    commit_t: i64,
    index_address: Option<String>,
    index_t: i64,
    retracted: bool,
}

#[derive(Debug, serde::Deserialize)]
struct VgSseRecord {
    /// Canonical alias, e.g. "search:main"
    alias: String,
    name: String,
    branch: String,
    /// String form of VG type, e.g. "fidx:BM25"
    vg_type: String,
    config: String,
    dependencies: Vec<String>,
    index_address: Option<String>,
    index_t: i64,
    retracted: bool,
}

fn parse_ns_record(data: &str) -> Result<Option<RemoteEvent>, ServerSseParseError> {
    let payload: NsRecordEnvelope = serde_json::from_str(data)?;

    match payload.kind.as_str() {
        "ledger" => {
            let record: LedgerSseRecord = serde_json::from_value(payload.record)?;
            Ok(Some(RemoteEvent::LedgerUpdated(ledger_sse_to_ns_record(
                record,
            ))))
        }
        "virtual-graph" => {
            let record: VgSseRecord = serde_json::from_value(payload.record)?;
            Ok(Some(RemoteEvent::VgUpdated(vg_sse_to_vg_ns_record(record))))
        }
        // Unknown kind is not an error; ignore for forwards compatibility.
        _ => Ok(None),
    }
}

fn parse_ns_retracted(data: &str) -> Result<Option<RemoteEvent>, ServerSseParseError> {
    let payload: NsRetractedEnvelope = serde_json::from_str(data)?;

    match payload.kind.as_str() {
        "ledger" => Ok(Some(RemoteEvent::LedgerRetracted {
            alias: payload.alias,
        })),
        "virtual-graph" => Ok(Some(RemoteEvent::VgRetracted {
            alias: payload.alias,
        })),
        _ => Ok(None),
    }
}

fn ledger_sse_to_ns_record(record: LedgerSseRecord) -> NsRecord {
    let (alias_name, branch) = split_alias_or_fallback(&record.alias, &record.branch);
    NsRecord {
        address: record.alias.clone(),
        alias: alias_name,
        branch,
        commit_address: record.commit_address,
        commit_t: record.commit_t,
        index_address: record.index_address,
        index_t: record.index_t,
        default_context_address: None,
        retracted: record.retracted,
    }
}

fn vg_sse_to_vg_ns_record(record: VgSseRecord) -> VgNsRecord {
    VgNsRecord {
        address: record.alias,
        name: record.name,
        branch: record.branch,
        vg_type: VgType::from_type_string(&record.vg_type),
        config: record.config,
        dependencies: record.dependencies,
        index_address: record.index_address,
        index_t: record.index_t,
        retracted: record.retracted,
    }
}

/// Split canonical alias into (name, branch) using the last ':' as delimiter.
///
/// Supports ledger names with '/' and other characters; branch delimiter is ':'.
fn split_alias_or_fallback(alias: &str, fallback_branch: &str) -> (String, String) {
    match alias.rsplit_once(':') {
        Some((name, branch)) if !name.is_empty() && !branch.is_empty() => {
            (name.to_string(), branch.to_string())
        }
        _ => (alias.to_string(), fallback_branch.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_sse::SseEvent;

    #[test]
    fn test_parse_ledger_ns_record_event() {
        let event = SseEvent {
            event_type: Some("ns-record".to_string()),
            data: r#"{
                "action": "ns-record",
                "kind": "ledger",
                "alias": "mydb:main",
                "record": {
                    "alias": "mydb:main",
                    "branch": "main",
                    "commit_address": "fluree:file://commit/abc",
                    "commit_t": 5,
                    "index_address": null,
                    "index_t": 0,
                    "retracted": false
                },
                "emitted_at": "2025-01-01T00:00:00Z"
            }"#
            .to_string(),
            id: None,
        };

        match parse_server_sse_event(&event).unwrap() {
            Some(RemoteEvent::LedgerUpdated(record)) => {
                assert_eq!(record.commit_t, 5);
                assert_eq!(record.address, "mydb:main");
                assert_eq!(record.alias, "mydb");
                assert_eq!(record.branch, "main");
            }
            other => panic!("expected LedgerUpdated, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_retracted_event() {
        let event = SseEvent {
            event_type: Some("ns-retracted".to_string()),
            data: r#"{
                "action": "ns-retracted",
                "kind": "ledger",
                "alias": "mydb:main",
                "emitted_at": "2025-01-01T00:00:00Z"
            }"#
            .to_string(),
            id: None,
        };

        match parse_server_sse_event(&event).unwrap() {
            Some(RemoteEvent::LedgerRetracted { alias }) => assert_eq!(alias, "mydb:main"),
            other => panic!("expected LedgerRetracted, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_virtual_graph_ns_record_event() {
        let event = SseEvent {
            event_type: Some("ns-record".to_string()),
            data: r#"{
                "action": "ns-record",
                "kind": "virtual-graph",
                "alias": "search:main",
                "record": {
                    "alias": "search:main",
                    "name": "search",
                    "branch": "main",
                    "vg_type": "fidx:BM25",
                    "config": "{\"k1\":1.2}",
                    "dependencies": ["books:main"],
                    "index_address": null,
                    "index_t": 0,
                    "retracted": false
                },
                "emitted_at": "2025-01-01T00:00:00Z"
            }"#
            .to_string(),
            id: None,
        };

        match parse_server_sse_event(&event).unwrap() {
            Some(RemoteEvent::VgUpdated(record)) => {
                assert_eq!(record.address, "search:main");
                assert_eq!(record.name, "search");
                assert_eq!(record.branch, "main");
                assert_eq!(record.index_t, 0);
            }
            other => panic!("expected VgUpdated, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_unknown_event_type_ignored() {
        let event = SseEvent {
            event_type: Some("keepalive".to_string()),
            data: "{}".to_string(),
            id: None,
        };

        assert!(parse_server_sse_event(&event).unwrap().is_none());
    }

    #[test]
    fn test_parse_event_without_type_ignored() {
        let event = SseEvent {
            event_type: None,
            data: "hello".to_string(),
            id: None,
        };

        assert!(parse_server_sse_event(&event).unwrap().is_none());
    }
}
