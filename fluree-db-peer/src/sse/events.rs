//! SSE event types and parsing
//!
//! Types representing the events received from the `/fluree/events` SSE endpoint.

use serde::Deserialize;

/// Ledger record from SSE ns-record event
#[derive(Debug, Clone, Deserialize)]
pub struct LedgerRecord {
    pub alias: String,
    #[serde(default)]
    pub branch: Option<String>,
    pub commit_address: Option<String>,
    pub commit_t: i64,
    pub index_address: Option<String>,
    pub index_t: i64,
    #[serde(default)]
    pub retracted: bool,
}

/// Virtual graph record from SSE ns-record event
#[derive(Debug, Clone, Deserialize)]
pub struct VgRecord {
    pub alias: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub vg_type: Option<String>,
    #[serde(default)]
    pub config: Option<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    pub index_address: Option<String>,
    pub index_t: i64,
    #[serde(default)]
    pub retracted: bool,
}

/// Wrapper for the SSE event payload
#[derive(Debug, Clone, Deserialize)]
pub struct NsRecordEvent {
    pub action: String,
    pub kind: String,
    pub alias: String,
    pub record: serde_json::Value,
    pub emitted_at: String,
}

/// Wrapper for the retraction event payload
#[derive(Debug, Clone, Deserialize)]
pub struct NsRetractedEvent {
    pub action: String,
    pub kind: String,
    pub alias: String,
    pub emitted_at: String,
}

/// Snapshot complete event payload
///
/// NOTE: The server does not currently emit `snapshot-complete` events.
/// This type exists for future compatibility when the server adds this feature.
/// Until then, `SseClientEvent::SnapshotComplete` will never fire.
#[derive(Debug, Clone, Deserialize)]
pub struct SnapshotCompleteEvent {
    pub hash: Option<String>,
}

/// Events emitted by the SSE client to the runtime
#[derive(Debug, Clone)]
pub enum SseClientEvent {
    /// Connected and receiving snapshot
    Connected,
    /// Snapshot complete marker received
    ///
    /// NOTE: The server does not currently emit this event.
    /// This variant exists for future compatibility.
    SnapshotComplete { hash: String },
    /// Ledger record received
    LedgerRecord(LedgerRecord),
    /// VG record received
    VgRecord(VgRecord),
    /// Resource retracted
    Retracted { kind: String, alias: String },
    /// Connection lost (will reconnect)
    Disconnected { reason: String },
    /// Fatal error (will not reconnect)
    Fatal { error: String },
}

impl LedgerRecord {
    /// Compute a config hash for change detection
    /// Since ledgers don't have config, use commit_t + index_t
    pub fn state_hash(&self) -> String {
        format!("{}:{}", self.commit_t, self.index_t)
    }
}

impl VgRecord {
    /// Compute a config hash for change detection.
    ///
    /// Uses SHA-256 truncated to 8 hex chars (4 bytes) to match
    /// the server's VG SSE event ID format.
    pub fn config_hash(&self) -> String {
        use sha2::{Digest, Sha256};

        let config_str = self.config.as_deref().unwrap_or("");
        let hash = Sha256::digest(config_str.as_bytes());
        // Take first 4 bytes = 8 hex chars (matches server's sha256_short)
        hex::encode(&hash[..4])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ledger_record() {
        let json = r#"{
            "alias": "books:main",
            "branch": "main",
            "commit_address": "fluree:file://commit/abc123",
            "commit_t": 5,
            "index_address": "fluree:file://index/def456",
            "index_t": 3,
            "retracted": false
        }"#;

        let record: LedgerRecord = serde_json::from_str(json).unwrap();
        assert_eq!(record.alias, "books:main");
        assert_eq!(record.commit_t, 5);
        assert_eq!(record.index_t, 3);
    }

    #[test]
    fn test_parse_vg_record() {
        let json = r#"{
            "alias": "search:main",
            "name": "search",
            "branch": "main",
            "vg_type": "fulltext",
            "config": "{\"analyzer\": \"standard\"}",
            "dependencies": ["books:main"],
            "index_address": "fluree:file://vg/search",
            "index_t": 2,
            "retracted": false
        }"#;

        let record: VgRecord = serde_json::from_str(json).unwrap();
        assert_eq!(record.alias, "search:main");
        assert_eq!(record.index_t, 2);
        assert_eq!(record.dependencies, vec!["books:main"]);
    }

    #[test]
    fn test_parse_ns_record_event() {
        let json = r#"{
            "action": "ns-record",
            "kind": "ledger",
            "alias": "books:main",
            "record": {"alias": "books:main", "commit_t": 1, "index_t": 1},
            "emitted_at": "2024-01-01T00:00:00Z"
        }"#;

        let event: NsRecordEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.action, "ns-record");
        assert_eq!(event.kind, "ledger");
    }

    #[test]
    fn test_parse_ns_retracted_event() {
        let json = r#"{
            "action": "ns-retracted",
            "kind": "ledger",
            "alias": "books:main",
            "emitted_at": "2024-01-01T00:00:00Z"
        }"#;

        let event: NsRetractedEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.action, "ns-retracted");
        assert_eq!(event.alias, "books:main");
    }

    #[test]
    fn test_vg_config_hash_sha256() {
        // Test that config_hash produces SHA-256 truncated to 8 hex chars
        // This should match the server's sha256_short function
        let record = VgRecord {
            alias: "test:main".to_string(),
            name: None,
            branch: None,
            vg_type: None,
            config: Some("test config".to_string()),
            dependencies: vec![],
            index_address: None,
            index_t: 1,
            retracted: false,
        };

        let hash = record.config_hash();
        // SHA-256 of "test config" truncated to first 4 bytes (8 hex chars)
        // Verify it's 8 hex chars and consistent
        assert_eq!(hash.len(), 8);
        assert_eq!(hash, "4369f6f9");
    }

    #[test]
    fn test_vg_config_hash_empty() {
        // Empty config should still produce valid hash
        let record = VgRecord {
            alias: "test:main".to_string(),
            name: None,
            branch: None,
            vg_type: None,
            config: None, // None config
            dependencies: vec![],
            index_address: None,
            index_t: 1,
            retracted: false,
        };

        let hash = record.config_hash();
        assert_eq!(hash.len(), 8);
        // SHA-256 of empty string starts with e3b0c442...
        assert_eq!(hash, "e3b0c442");
    }
}
