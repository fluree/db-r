//! In-memory nameservice implementation for testing
//!
//! This implementation stores all records in memory using `Arc<RwLock>` for
//! interior mutability, making it thread-safe and suitable for multi-threaded
//! async runtimes.

use crate::{
    parse_address, AdminPublisher, CasResult, ConfigCasResult, ConfigPublisher, ConfigValue,
    GraphSourcePublisher, GraphSourceRecord, GraphSourceType, NameService, NameServiceEvent,
    NsLookupResult, NsRecord, Publication, Publisher, RefKind, RefPublisher, RefValue, Result,
    StatusCasResult, StatusPayload, StatusPublisher, StatusValue, Subscription,
};
use async_trait::async_trait;
use fluree_db_core::alias as core_alias;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::broadcast;

/// In-memory nameservice for testing
///
/// Stores all records in a `HashMap` with `Arc<RwLock>` for interior mutability.
/// This implementation is thread-safe and suitable for multi-threaded runtimes.
#[derive(Clone)]
pub struct MemoryNameService {
    /// Ledger records keyed by canonical address (e.g., "mydb:main")
    records: Arc<RwLock<HashMap<String, NsRecord>>>,
    /// Graph source records keyed by canonical address (e.g., "my-search:main")
    graph_source_records: Arc<RwLock<HashMap<String, GraphSourceRecord>>>,
    /// Status values keyed by canonical address (v2 extension)
    status_values: Arc<RwLock<HashMap<String, StatusValue>>>,
    /// Config values keyed by canonical address (v2 extension)
    config_values: Arc<RwLock<HashMap<String, ConfigValue>>>,
    /// In-process event sender for reactive subscriptions.
    event_tx: broadcast::Sender<NameServiceEvent>,
}

impl Default for MemoryNameService {
    fn default() -> Self {
        // Small buffer; consumers should treat this as best-effort.
        let (event_tx, _event_rx) = broadcast::channel(128);
        Self {
            records: Arc::new(RwLock::new(HashMap::new())),
            graph_source_records: Arc::new(RwLock::new(HashMap::new())),
            status_values: Arc::new(RwLock::new(HashMap::new())),
            config_values: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
        }
    }
}

impl Debug for MemoryNameService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let records = self.records.read();
        let graph_source_records = self.graph_source_records.read();
        let status_values = self.status_values.read();
        let config_values = self.config_values.read();
        f.debug_struct("MemoryNameService")
            .field("record_count", &records.len())
            .field("graph_source_record_count", &graph_source_records.len())
            .field("status_count", &status_values.len())
            .field("config_count", &config_values.len())
            .finish()
    }
}

impl MemoryNameService {
    /// Create a new empty in-memory nameservice
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a record for a new ledger
    ///
    /// This is a convenience method for tests to bootstrap a ledger.
    pub fn create_ledger(&self, ledger_address: &str) -> Result<()> {
        let (ledger_name, branch) = parse_address(ledger_address)?;
        let record = NsRecord::new(ledger_name, branch);
        self.records.write().insert(record.address.clone(), record);
        Ok(())
    }

    /// Get a record by address (internal helper)
    fn get_record(&self, ledger_address: &str) -> Option<NsRecord> {
        // Try direct lookup first
        if let Some(record) = self.records.read().get(ledger_address).cloned() {
            return Some(record);
        }

        // Try with default branch
        let with_branch = match core_alias::normalize_alias(ledger_address) {
            Ok(value) => value,
            Err(_) => return None,
        };

        self.records.read().get(&with_branch).cloned()
    }

    /// Normalize ledger address to canonical form
    fn normalize_address(&self, ledger_address: &str) -> String {
        core_alias::normalize_alias(ledger_address).unwrap_or_else(|_| ledger_address.to_string())
    }
}

#[async_trait]
impl NameService for MemoryNameService {
    async fn lookup(&self, ledger_address: &str) -> Result<Option<NsRecord>> {
        Ok(self.get_record(ledger_address))
    }

    async fn all_records(&self) -> Result<Vec<NsRecord>> {
        Ok(self.records.read().values().cloned().collect())
    }
}

#[async_trait]
impl Publisher for MemoryNameService {
    async fn publish_ledger_init(&self, ledger_address: &str) -> Result<()> {
        let key = self.normalize_address(ledger_address);

        // Check if record already exists (including retracted)
        if self.records.read().contains_key(&key) {
            return Err(crate::NameServiceError::ledger_already_exists(&key));
        }

        // Create minimal NsRecord
        let (ledger_name, branch) = parse_address(ledger_address)?;
        let record = NsRecord::new(ledger_name, branch);
        self.records.write().insert(key, record);

        Ok(())
    }

    async fn publish_commit(
        &self,
        ledger_address: &str,
        commit_addr: &str,
        commit_t: i64,
    ) -> Result<()> {
        let key = self.normalize_address(ledger_address);
        let mut records = self.records.write();
        let mut did_update = false;

        if let Some(record) = records.get_mut(&key) {
            // Only update if new_t > existing_t (strictly monotonic)
            if commit_t > record.commit_t {
                record.commit_address = Some(commit_addr.to_string());
                record.commit_t = commit_t;
                did_update = true;
            }
            // If commit_t <= existing, silently ignore (monotonic guarantee)
        } else {
            // Create new record
            let (ledger_name, branch) = parse_address(ledger_address)?;
            let mut record = NsRecord::new(ledger_name, branch);
            record.commit_address = Some(commit_addr.to_string());
            record.commit_t = commit_t;
            records.insert(key, record);
            did_update = true;
        }

        if did_update {
            let _ = self.event_tx.send(NameServiceEvent::LedgerCommitPublished {
                ledger_address: self.normalize_address(ledger_address),
                commit_address: commit_addr.to_string(),
                commit_t,
            });
        }
        Ok(())
    }

    async fn publish_index(
        &self,
        ledger_address: &str,
        index_addr: &str,
        index_t: i64,
    ) -> Result<()> {
        let key = self.normalize_address(ledger_address);
        let mut records = self.records.write();
        let mut did_update = false;

        if let Some(record) = records.get_mut(&key) {
            // Only update if new_t > existing_t (strictly monotonic)
            if index_t > record.index_t {
                record.index_address = Some(index_addr.to_string());
                record.index_t = index_t;
                did_update = true;
            }
            // If index_t <= existing, silently ignore (monotonic guarantee)
        } else {
            // Create new record
            let (ledger_name, branch) = parse_address(ledger_address)?;
            let mut record = NsRecord::new(ledger_name, branch);
            record.index_address = Some(index_addr.to_string());
            record.index_t = index_t;
            records.insert(key, record);
            did_update = true;
        }

        if did_update {
            let _ = self.event_tx.send(NameServiceEvent::LedgerIndexPublished {
                ledger_address: self.normalize_address(ledger_address),
                index_address: index_addr.to_string(),
                index_t,
            });
        }
        Ok(())
    }

    async fn retract(&self, ledger_address: &str) -> Result<()> {
        let key = self.normalize_address(ledger_address);
        let mut records = self.records.write();
        let mut did_update = false;

        if let Some(record) = records.get_mut(&key) {
            if !record.retracted {
                record.retracted = true;
                did_update = true;
            }
        }

        if did_update {
            // Advance status_v when retracting
            let mut status_values = self.status_values.write();
            let current_v = status_values.get(&key).map(|s| s.v).unwrap_or(1); // Default to 1 if no status exists
            status_values.insert(
                key.clone(),
                StatusValue::new(current_v + 1, StatusPayload::new("retracted")),
            );

            let _ = self.event_tx.send(NameServiceEvent::LedgerRetracted {
                ledger_address: key,
            });
        }
        Ok(())
    }

    fn publishing_address(&self, ledger_address: &str) -> Option<String> {
        // Memory nameservice always returns the normalized address as the publishing address
        Some(self.normalize_address(ledger_address))
    }
}

#[async_trait]
impl AdminPublisher for MemoryNameService {
    async fn publish_index_allow_equal(
        &self,
        ledger_address: &str,
        index_addr: &str,
        index_t: i64,
    ) -> Result<()> {
        let key = self.normalize_address(ledger_address);
        let mut records = self.records.write();

        if let Some(record) = records.get_mut(&key) {
            // Allow update when new_t >= existing_t (not strictly monotonic)
            if index_t >= record.index_t {
                record.index_address = Some(index_addr.to_string());
                record.index_t = index_t;

                // Emit event (preserve semantics with regular publish_index)
                let _ = self.event_tx.send(NameServiceEvent::LedgerIndexPublished {
                    ledger_address: key.clone(),
                    index_address: index_addr.to_string(),
                    index_t,
                });
            }
            // If index_t < existing, silently ignore (protect time-travel invariants)
        } else {
            // Create new record (same as publish_index)
            let (ledger_name, branch) = parse_address(ledger_address)?;
            let mut record = NsRecord::new(ledger_name, branch);
            record.index_address = Some(index_addr.to_string());
            record.index_t = index_t;
            records.insert(key.clone(), record);

            let _ = self.event_tx.send(NameServiceEvent::LedgerIndexPublished {
                ledger_address: key,
                index_address: index_addr.to_string(),
                index_t,
            });
        }

        Ok(())
    }
}

#[async_trait]
impl RefPublisher for MemoryNameService {
    async fn get_ref(&self, ledger_address: &str, kind: RefKind) -> Result<Option<RefValue>> {
        let key = self.normalize_address(ledger_address);
        let records = self.records.read();

        match records.get(&key) {
            None => Ok(None),
            Some(record) => match kind {
                RefKind::CommitHead => Ok(Some(RefValue {
                    address: record.commit_address.clone(),
                    t: record.commit_t,
                })),
                RefKind::IndexHead => Ok(Some(RefValue {
                    address: record.index_address.clone(),
                    t: record.index_t,
                })),
            },
        }
    }

    async fn compare_and_set_ref(
        &self,
        ledger_address: &str,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> Result<CasResult> {
        let key = self.normalize_address(ledger_address);
        let mut records = self.records.write();

        let current_ref = records.get(&key).map(|r| match kind {
            RefKind::CommitHead => RefValue {
                address: r.commit_address.clone(),
                t: r.commit_t,
            },
            RefKind::IndexHead => RefValue {
                address: r.index_address.clone(),
                t: r.index_t,
            },
        });

        // Compare expected with current.
        match (&expected, &current_ref) {
            (None, None) => {
                // Creating a new ref — record must not exist yet.
                // Initialize the ledger record.
                let (ledger_name, branch) = parse_address(ledger_address)?;
                let mut record = NsRecord::new(ledger_name, branch);
                match kind {
                    RefKind::CommitHead => {
                        record.commit_address = new.address.clone();
                        record.commit_t = new.t;
                    }
                    RefKind::IndexHead => {
                        record.index_address = new.address.clone();
                        record.index_t = new.t;
                    }
                }
                records.insert(key.clone(), record);
                // Emit event for new record creation.
                match kind {
                    RefKind::CommitHead => {
                        if let Some(addr) = &new.address {
                            let _ = self.event_tx.send(NameServiceEvent::LedgerCommitPublished {
                                ledger_address: key,
                                commit_address: addr.clone(),
                                commit_t: new.t,
                            });
                        }
                    }
                    RefKind::IndexHead => {
                        if let Some(addr) = &new.address {
                            let _ = self.event_tx.send(NameServiceEvent::LedgerIndexPublished {
                                ledger_address: key,
                                index_address: addr.clone(),
                                index_t: new.t,
                            });
                        }
                    }
                }
                return Ok(CasResult::Updated);
            }
            (None, Some(actual)) => {
                // Expected None but record exists — conflict.
                return Ok(CasResult::Conflict {
                    actual: Some(actual.clone()),
                });
            }
            (Some(_), None) => {
                // Expected a value but record doesn't exist — conflict.
                return Ok(CasResult::Conflict { actual: None });
            }
            (Some(exp), Some(actual)) => {
                // Both exist — compare addresses.
                if exp.address != actual.address {
                    return Ok(CasResult::Conflict {
                        actual: Some(actual.clone()),
                    });
                }
            }
        }

        // Address matches — check monotonic guard.
        let current = current_ref.as_ref().unwrap();
        let guard_ok = match kind {
            RefKind::CommitHead => new.t > current.t,
            RefKind::IndexHead => new.t >= current.t,
        };
        if !guard_ok {
            return Ok(CasResult::Conflict {
                actual: Some(current.clone()),
            });
        }

        // Apply the update.
        let record = records.get_mut(&key).unwrap();
        match kind {
            RefKind::CommitHead => {
                record.commit_address = new.address.clone();
                record.commit_t = new.t;
                // Emit event.
                if let Some(addr) = &new.address {
                    let _ = self.event_tx.send(NameServiceEvent::LedgerCommitPublished {
                        ledger_address: key.clone(),
                        commit_address: addr.clone(),
                        commit_t: new.t,
                    });
                }
            }
            RefKind::IndexHead => {
                record.index_address = new.address.clone();
                record.index_t = new.t;
                if let Some(addr) = &new.address {
                    let _ = self.event_tx.send(NameServiceEvent::LedgerIndexPublished {
                        ledger_address: key.clone(),
                        index_address: addr.clone(),
                        index_t: new.t,
                    });
                }
            }
        }

        Ok(CasResult::Updated)
    }
}

#[async_trait]
impl Publication for MemoryNameService {
    async fn subscribe(&self, scope: crate::SubscriptionScope) -> Result<Subscription> {
        Ok(Subscription {
            scope,
            receiver: self.event_tx.subscribe(),
        })
    }

    async fn unsubscribe(&self, _scope: &crate::SubscriptionScope) -> Result<()> {
        Ok(())
    }

    async fn known_addresses(&self, ledger_address: &str) -> Result<Vec<String>> {
        let key = self.normalize_address(ledger_address);
        let records = self.records.read();

        if let Some(record) = records.get(&key) {
            let mut addresses = Vec::new();
            if let Some(addr) = &record.commit_address {
                addresses.push(addr.clone());
            }
            if let Some(addr) = &record.index_address {
                addresses.push(addr.clone());
            }
            Ok(addresses)
        } else {
            Ok(vec![])
        }
    }
}

#[async_trait]
impl GraphSourcePublisher for MemoryNameService {
    async fn publish_graph_source(
        &self,
        name: &str,
        branch: &str,
        source_type: GraphSourceType,
        config: &str,
        dependencies: &[String],
    ) -> Result<()> {
        let key = core_alias::format_alias(name, branch);
        let mut graph_source_records = self.graph_source_records.write();

        if let Some(record) = graph_source_records.get_mut(&key) {
            // Update config but preserve retracted status if already set
            record.source_type = source_type.clone();
            record.config = config.to_string();
            record.dependencies = dependencies.to_vec();
        } else {
            // Create new graph source record
            let record = GraphSourceRecord::new(
                name,
                branch,
                source_type.clone(),
                config,
                dependencies.to_vec(),
            );
            graph_source_records.insert(key, record);
        }

        let _ = self
            .event_tx
            .send(NameServiceEvent::GraphSourceConfigPublished {
                address: core_alias::format_alias(name, branch),
                source_type,
                dependencies: dependencies.to_vec(),
            });
        Ok(())
    }

    async fn publish_graph_source_index(
        &self,
        name: &str,
        branch: &str,
        index_addr: &str,
        index_t: i64,
    ) -> Result<()> {
        let key = core_alias::format_alias(name, branch);
        let mut graph_source_records = self.graph_source_records.write();
        let mut did_update = false;

        if let Some(record) = graph_source_records.get_mut(&key) {
            // Strictly monotonic: only update if new_t > existing_t
            if index_t > record.index_t {
                record.index_address = Some(index_addr.to_string());
                record.index_t = index_t;
                did_update = true;
            }
        }
        // If graph source doesn't exist, silently ignore (index requires config first)

        if did_update {
            let _ = self
                .event_tx
                .send(NameServiceEvent::GraphSourceIndexPublished {
                    address: key,
                    index_address: index_addr.to_string(),
                    index_t,
                });
        }
        Ok(())
    }

    async fn retract_graph_source(&self, name: &str, branch: &str) -> Result<()> {
        let key = core_alias::format_alias(name, branch);
        let mut graph_source_records = self.graph_source_records.write();
        let mut did_update = false;

        if let Some(record) = graph_source_records.get_mut(&key) {
            if !record.retracted {
                record.retracted = true;
                did_update = true;
            }
        }

        if did_update {
            let _ = self
                .event_tx
                .send(NameServiceEvent::GraphSourceRetracted { address: key });
        }
        Ok(())
    }

    async fn lookup_graph_source(&self, address: &str) -> Result<Option<GraphSourceRecord>> {
        let key = self.normalize_address(address);
        Ok(self.graph_source_records.read().get(&key).cloned())
    }

    async fn lookup_any(&self, address: &str) -> Result<NsLookupResult> {
        let key = self.normalize_address(address);

        // Check graph source records first
        if let Some(record) = self.graph_source_records.read().get(&key).cloned() {
            return Ok(NsLookupResult::GraphSource(record));
        }

        // Then check ledger records
        if let Some(record) = self.records.read().get(&key).cloned() {
            return Ok(NsLookupResult::Ledger(record));
        }

        Ok(NsLookupResult::NotFound)
    }

    async fn all_graph_source_records(&self) -> Result<Vec<GraphSourceRecord>> {
        Ok(self.graph_source_records.read().values().cloned().collect())
    }
}

#[async_trait]
impl StatusPublisher for MemoryNameService {
    async fn get_status(&self, ledger_address: &str) -> Result<Option<StatusValue>> {
        let key = self.normalize_address(ledger_address);
        let status_values = self.status_values.read();

        // If status exists, return it
        if let Some(status) = status_values.get(&key).cloned() {
            return Ok(Some(status));
        }

        // If the ledger record exists but no status, return initial status (v=1)
        let records = self.records.read();
        if records.contains_key(&key) {
            return Ok(Some(StatusValue::initial()));
        }

        // No record exists
        Ok(None)
    }

    async fn push_status(
        &self,
        ledger_address: &str,
        expected: Option<&StatusValue>,
        new: &StatusValue,
    ) -> Result<StatusCasResult> {
        let key = self.normalize_address(ledger_address);

        // Get current status (or initial if record exists but no status)
        let current = {
            let status_values = self.status_values.read();
            let records = self.records.read();

            if let Some(status) = status_values.get(&key).cloned() {
                Some(status)
            } else if records.contains_key(&key) {
                // Record exists but no status → treat as initial
                Some(StatusValue::initial())
            } else {
                None
            }
        };

        // Compare expected with current
        match (&expected, &current) {
            (None, None) => {
                // Cannot create status without record
                return Ok(StatusCasResult::Conflict { actual: None });
            }
            (None, Some(actual)) => {
                // Expected None but status exists → conflict
                return Ok(StatusCasResult::Conflict {
                    actual: Some(actual.clone()),
                });
            }
            (Some(_), None) => {
                // Expected a value but record doesn't exist → conflict
                return Ok(StatusCasResult::Conflict { actual: None });
            }
            (Some(exp), Some(actual)) => {
                // Both exist — compare watermarks and payloads
                if exp.v != actual.v || exp.payload != actual.payload {
                    return Ok(StatusCasResult::Conflict {
                        actual: Some(actual.clone()),
                    });
                }
            }
        }

        // Validate monotonic constraint: new.v > current.v
        let current_v = current.as_ref().map(|c| c.v).unwrap_or(0);
        if new.v <= current_v {
            return Ok(StatusCasResult::Conflict { actual: current });
        }

        // Apply the update
        self.status_values.write().insert(key, new.clone());

        Ok(StatusCasResult::Updated)
    }
}

#[async_trait]
impl ConfigPublisher for MemoryNameService {
    async fn get_config(&self, ledger_address: &str) -> Result<Option<ConfigValue>> {
        let key = self.normalize_address(ledger_address);
        let config_values = self.config_values.read();

        // If config exists, return it
        if let Some(config) = config_values.get(&key).cloned() {
            return Ok(Some(config));
        }

        // If the ledger record exists but no config, return unborn config (v=0)
        let records = self.records.read();
        if records.contains_key(&key) {
            return Ok(Some(ConfigValue::unborn()));
        }

        // No record exists
        Ok(None)
    }

    async fn push_config(
        &self,
        ledger_address: &str,
        expected: Option<&ConfigValue>,
        new: &ConfigValue,
    ) -> Result<ConfigCasResult> {
        let key = self.normalize_address(ledger_address);

        // Get current config (or unborn if record exists but no config)
        let current = {
            let config_values = self.config_values.read();
            let records = self.records.read();

            if let Some(config) = config_values.get(&key).cloned() {
                Some(config)
            } else if records.contains_key(&key) {
                // Record exists but no config → treat as unborn
                Some(ConfigValue::unborn())
            } else {
                None
            }
        };

        // Compare expected with current
        match (&expected, &current) {
            (None, None) => {
                // Cannot create config without record
                return Ok(ConfigCasResult::Conflict { actual: None });
            }
            (None, Some(actual)) => {
                // Expected None but config exists (even unborn) → conflict
                return Ok(ConfigCasResult::Conflict {
                    actual: Some(actual.clone()),
                });
            }
            (Some(_), None) => {
                // Expected a value but record doesn't exist → conflict
                return Ok(ConfigCasResult::Conflict { actual: None });
            }
            (Some(exp), Some(actual)) => {
                // Both exist — compare watermarks and payloads
                if exp.v != actual.v || exp.payload != actual.payload {
                    return Ok(ConfigCasResult::Conflict {
                        actual: Some(actual.clone()),
                    });
                }
            }
        }

        // Validate monotonic constraint: new.v > current.v
        let current_v = current.as_ref().map(|c| c.v).unwrap_or(0);
        if new.v <= current_v {
            return Ok(ConfigCasResult::Conflict { actual: current });
        }

        // Apply the update to config_values
        self.config_values.write().insert(key.clone(), new.clone());

        // Sync default_context to NsRecord.default_context_address
        let new_default_context = new.payload.as_ref().and_then(|p| p.default_context.clone());
        if let Some(record) = self.records.write().get_mut(&key) {
            record.default_context_address = new_default_context;
        }

        Ok(ConfigCasResult::Updated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConfigPayload, StatusPayload};
    use tokio::sync::broadcast::error::TryRecvError;

    #[tokio::test]
    async fn test_memory_ns_publish_commit() {
        let ns = MemoryNameService::new();

        // First publish
        ns.publish_commit("mydb:main", "commit-1", 1).await.unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_address, Some("commit-1".to_string()));
        assert_eq!(record.commit_t, 1);

        // Higher t should update
        ns.publish_commit("mydb:main", "commit-2", 5).await.unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_address, Some("commit-2".to_string()));
        assert_eq!(record.commit_t, 5);

        // Lower t should be ignored (monotonic)
        ns.publish_commit("mydb:main", "commit-old", 3)
            .await
            .unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_address, Some("commit-2".to_string()));
        assert_eq!(record.commit_t, 5);
    }

    #[tokio::test]
    async fn test_memory_ns_publish_index_separate() {
        let ns = MemoryNameService::new();

        // Publish commit first
        ns.publish_commit("mydb:main", "commit-1", 10)
            .await
            .unwrap();

        // Publish index (can lag behind commit)
        ns.publish_index("mydb:main", "index-1", 5).await.unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert_eq!(record.commit_t, 10);
        assert_eq!(record.index_t, 5);
        assert!(record.has_novelty()); // commit_t > index_t
    }

    #[tokio::test]
    async fn test_memory_ns_lookup_default_branch() {
        let ns = MemoryNameService::new();

        ns.publish_commit("mydb:main", "commit-1", 1).await.unwrap();

        // Lookup without branch should find default
        let record = ns.lookup("mydb").await.unwrap();
        assert!(record.is_some());

        // Lookup with branch should also work
        let record = ns.lookup("mydb:main").await.unwrap();
        assert!(record.is_some());
    }

    #[tokio::test]
    async fn test_memory_ns_retract() {
        let ns = MemoryNameService::new();

        ns.publish_commit("mydb:main", "commit-1", 1).await.unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert!(!record.retracted);

        ns.retract("mydb:main").await.unwrap();

        let record = ns.lookup("mydb:main").await.unwrap().unwrap();
        assert!(record.retracted);
    }

    #[tokio::test]
    async fn test_memory_ns_all_records() {
        let ns = MemoryNameService::new();

        ns.publish_commit("db1:main", "commit-1", 1).await.unwrap();
        ns.publish_commit("db2:main", "commit-2", 1).await.unwrap();
        ns.publish_commit("db3:dev", "commit-3", 1).await.unwrap();

        let records = ns.all_records().await.unwrap();
        assert_eq!(records.len(), 3);
    }

    #[tokio::test]
    async fn test_memory_ns_publishing_address() {
        let ns = MemoryNameService::new();

        assert_eq!(ns.publishing_address("mydb"), Some("mydb:main".to_string()));
        assert_eq!(
            ns.publishing_address("mydb:dev"),
            Some("mydb:dev".to_string())
        );
    }

    #[tokio::test]
    async fn test_memory_ns_emits_events_on_publish_commit_monotonic() {
        let ns = MemoryNameService::new();
        let mut sub = ns
            .subscribe(crate::SubscriptionScope::address("mydb:main"))
            .await
            .unwrap();

        ns.publish_commit("mydb:main", "commit-1", 1).await.unwrap();
        let evt = sub.receiver.recv().await.unwrap();
        assert_eq!(
            evt,
            NameServiceEvent::LedgerCommitPublished {
                ledger_address: "mydb:main".to_string(),
                commit_address: "commit-1".to_string(),
                commit_t: 1
            }
        );

        // Lower t should not emit a new event.
        ns.publish_commit("mydb:main", "commit-old", 0)
            .await
            .unwrap();
        assert!(matches!(sub.receiver.try_recv(), Err(TryRecvError::Empty)));
    }

    // ========== Graph Source Tests ==========

    #[tokio::test]
    async fn test_memory_graph_source_publish_and_lookup() {
        let ns = MemoryNameService::new();

        ns.publish_graph_source(
            "my-search",
            "main",
            GraphSourceType::Bm25,
            r#"{"k1":1.2}"#,
            &["source:main".to_string()],
        )
        .await
        .unwrap();

        let record = ns
            .lookup_graph_source("my-search:main")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.name, "my-search");
        assert_eq!(record.branch, "main");
        assert_eq!(record.source_type, GraphSourceType::Bm25);
        assert_eq!(record.config, r#"{"k1":1.2}"#);
        assert!(!record.retracted);
    }

    #[tokio::test]
    async fn test_memory_graph_source_index_monotonic() {
        let ns = MemoryNameService::new();

        ns.publish_graph_source("gs", "main", GraphSourceType::Bm25, "{}", &[])
            .await
            .unwrap();

        ns.publish_graph_source_index("gs", "main", "index-v1", 10)
            .await
            .unwrap();

        ns.publish_graph_source_index("gs", "main", "index-v2", 20)
            .await
            .unwrap();

        let record = ns.lookup_graph_source("gs:main").await.unwrap().unwrap();
        assert_eq!(record.index_address, Some("index-v2".to_string()));
        assert_eq!(record.index_t, 20);

        // Lower t should be ignored
        ns.publish_graph_source_index("gs", "main", "index-old", 15)
            .await
            .unwrap();

        let record = ns.lookup_graph_source("gs:main").await.unwrap().unwrap();
        assert_eq!(record.index_address, Some("index-v2".to_string()));
        assert_eq!(record.index_t, 20);
    }

    #[tokio::test]
    async fn test_memory_graph_source_retract() {
        let ns = MemoryNameService::new();

        ns.publish_graph_source("gs", "main", GraphSourceType::Bm25, "{}", &[])
            .await
            .unwrap();

        ns.retract_graph_source("gs", "main").await.unwrap();

        let record = ns.lookup_graph_source("gs:main").await.unwrap().unwrap();
        assert!(record.retracted);
    }

    #[tokio::test]
    async fn test_memory_graph_source_lookup_any() {
        let ns = MemoryNameService::new();

        ns.publish_commit("ledger:main", "commit-1", 1)
            .await
            .unwrap();
        ns.publish_graph_source("gs", "main", GraphSourceType::Bm25, "{}", &[])
            .await
            .unwrap();

        match ns.lookup_any("ledger:main").await.unwrap() {
            NsLookupResult::Ledger(r) => assert_eq!(r.name, "ledger"),
            other => panic!("Expected Ledger, got {:?}", other),
        }

        match ns.lookup_any("gs:main").await.unwrap() {
            NsLookupResult::GraphSource(r) => assert_eq!(r.name, "gs"),
            other => panic!("Expected GraphSource, got {:?}", other),
        }

        match ns.lookup_any("nonexistent:main").await.unwrap() {
            NsLookupResult::NotFound => {}
            other => panic!("Expected NotFound, got {:?}", other),
        }
    }

    // =========================================================================
    // RefPublisher tests
    // =========================================================================

    #[tokio::test]
    async fn test_ref_get_ref_unknown_alias() {
        let ns = MemoryNameService::new();
        let result = ns
            .get_ref("nonexistent:main", RefKind::CommitHead)
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_ref_get_ref_after_publish() {
        let ns = MemoryNameService::new();
        ns.publish_commit("mydb:main", "commit-1", 5).await.unwrap();

        let commit = ns
            .get_ref("mydb:main", RefKind::CommitHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(commit.address, Some("commit-1".to_string()));
        assert_eq!(commit.t, 5);

        // Index should exist but be at t=0 with no address (unborn-like)
        let index = ns
            .get_ref("mydb:main", RefKind::IndexHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(index.address, None);
        assert_eq!(index.t, 0);
    }

    #[tokio::test]
    async fn test_ref_cas_create_new() {
        let ns = MemoryNameService::new();
        let new_ref = RefValue {
            address: Some("commit-1".to_string()),
            t: 1,
        };

        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, None, &new_ref)
            .await
            .unwrap();
        assert_eq!(result, CasResult::Updated);

        let current = ns
            .get_ref("mydb:main", RefKind::CommitHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.address, Some("commit-1".to_string()));
        assert_eq!(current.t, 1);
    }

    #[tokio::test]
    async fn test_ref_cas_conflict_already_exists() {
        let ns = MemoryNameService::new();
        ns.publish_commit("mydb:main", "commit-1", 1).await.unwrap();

        let new_ref = RefValue {
            address: Some("commit-2".to_string()),
            t: 2,
        };
        // expected=None but record exists → conflict
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, None, &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { actual } => {
                let a = actual.unwrap();
                assert_eq!(a.address, Some("commit-1".to_string()));
                assert_eq!(a.t, 1);
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_ref_cas_conflict_address_mismatch() {
        let ns = MemoryNameService::new();
        ns.publish_commit("mydb:main", "commit-1", 1).await.unwrap();

        let expected = RefValue {
            address: Some("wrong-address".to_string()),
            t: 1,
        };
        let new_ref = RefValue {
            address: Some("commit-2".to_string()),
            t: 2,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { actual } => {
                assert_eq!(actual.unwrap().address, Some("commit-1".to_string()));
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_ref_cas_success_address_matches() {
        let ns = MemoryNameService::new();
        ns.publish_commit("mydb:main", "commit-1", 1).await.unwrap();

        let expected = RefValue {
            address: Some("commit-1".to_string()),
            t: 1,
        };
        let new_ref = RefValue {
            address: Some("commit-2".to_string()),
            t: 2,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        assert_eq!(result, CasResult::Updated);

        let current = ns
            .get_ref("mydb:main", RefKind::CommitHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.address, Some("commit-2".to_string()));
        assert_eq!(current.t, 2);
    }

    #[tokio::test]
    async fn test_ref_cas_commit_strict_monotonic() {
        let ns = MemoryNameService::new();
        ns.publish_commit("mydb:main", "commit-1", 5).await.unwrap();

        let expected = RefValue {
            address: Some("commit-1".to_string()),
            t: 5,
        };
        // Same t should fail (strict for CommitHead)
        let new_ref = RefValue {
            address: Some("commit-2".to_string()),
            t: 5,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { .. } => {}
            _ => panic!("expected conflict for same t on CommitHead"),
        }

        // Lower t should also fail
        let new_ref = RefValue {
            address: Some("commit-2".to_string()),
            t: 3,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { .. } => {}
            _ => panic!("expected conflict for lower t on CommitHead"),
        }
    }

    #[tokio::test]
    async fn test_ref_cas_index_allows_equal_t() {
        let ns = MemoryNameService::new();
        ns.publish_commit("mydb:main", "commit-1", 5).await.unwrap();
        ns.publish_index("mydb:main", "index-1", 5).await.unwrap();

        let expected = RefValue {
            address: Some("index-1".to_string()),
            t: 5,
        };
        // Same t should succeed for IndexHead (non-strict)
        let new_ref = RefValue {
            address: Some("index-2".to_string()),
            t: 5,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::IndexHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        assert_eq!(result, CasResult::Updated);
    }

    #[tokio::test]
    async fn test_ref_fast_forward_commit_success() {
        let ns = MemoryNameService::new();
        ns.publish_commit("mydb:main", "commit-1", 1).await.unwrap();

        let new_ref = RefValue {
            address: Some("commit-5".to_string()),
            t: 5,
        };
        let result = ns
            .fast_forward_commit("mydb:main", &new_ref, 3)
            .await
            .unwrap();
        assert_eq!(result, CasResult::Updated);

        let current = ns
            .get_ref("mydb:main", RefKind::CommitHead)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.t, 5);
    }

    #[tokio::test]
    async fn test_ref_fast_forward_commit_rejected_stale() {
        let ns = MemoryNameService::new();
        ns.publish_commit("mydb:main", "commit-1", 10)
            .await
            .unwrap();

        let new_ref = RefValue {
            address: Some("commit-old".to_string()),
            t: 5,
        };
        let result = ns
            .fast_forward_commit("mydb:main", &new_ref, 3)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { actual } => {
                assert_eq!(actual.unwrap().t, 10);
            }
            _ => panic!("expected conflict for stale fast-forward"),
        }
    }

    #[tokio::test]
    async fn test_ref_cas_expected_some_but_missing() {
        let ns = MemoryNameService::new();
        let expected = RefValue {
            address: Some("commit-1".to_string()),
            t: 1,
        };
        let new_ref = RefValue {
            address: Some("commit-2".to_string()),
            t: 2,
        };
        let result = ns
            .compare_and_set_ref("mydb:main", RefKind::CommitHead, Some(&expected), &new_ref)
            .await
            .unwrap();
        match result {
            CasResult::Conflict { actual } => {
                assert_eq!(actual, None);
            }
            _ => panic!("expected conflict when ref doesn't exist"),
        }
    }

    #[tokio::test]
    async fn test_ref_cas_emits_commit_event() {
        let ns = MemoryNameService::new();
        let mut sub = ns
            .subscribe(crate::SubscriptionScope::address("mydb:main"))
            .await
            .unwrap();

        let new_ref = RefValue {
            address: Some("commit-1".to_string()),
            t: 1,
        };
        ns.compare_and_set_ref("mydb:main", RefKind::CommitHead, None, &new_ref)
            .await
            .unwrap();

        match sub.receiver.recv().await.unwrap() {
            NameServiceEvent::LedgerCommitPublished {
                ledger_address,
                commit_address,
                commit_t,
            } => {
                assert_eq!(ledger_address, "mydb:main");
                assert_eq!(commit_address, "commit-1");
                assert_eq!(commit_t, 1);
            }
            other => panic!("expected LedgerCommitPublished, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_ref_cas_emits_index_event() {
        let ns = MemoryNameService::new();
        ns.publish_commit("mydb:main", "commit-1", 5).await.unwrap();
        let mut sub = ns
            .subscribe(crate::SubscriptionScope::address("mydb:main"))
            .await
            .unwrap();

        let expected = RefValue {
            address: None,
            t: 0,
        };
        let new_ref = RefValue {
            address: Some("index-1".to_string()),
            t: 5,
        };
        ns.compare_and_set_ref("mydb:main", RefKind::IndexHead, Some(&expected), &new_ref)
            .await
            .unwrap();

        match sub.receiver.recv().await.unwrap() {
            NameServiceEvent::LedgerIndexPublished {
                ledger_address,
                index_address,
                index_t,
            } => {
                assert_eq!(ledger_address, "mydb:main");
                assert_eq!(index_address, "index-1");
                assert_eq!(index_t, 5);
            }
            other => panic!("expected LedgerIndexPublished, got {:?}", other),
        }
    }

    // =========================================================================
    // StatusPublisher tests
    // =========================================================================

    #[tokio::test]
    async fn test_status_get_nonexistent() {
        let ns = MemoryNameService::new();
        let result = ns.get_status("nonexistent:main").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_status_get_initial() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let status = ns.get_status("mydb:main").await.unwrap().unwrap();
        assert_eq!(status.v, 1);
        assert_eq!(status.payload.state, "ready");
    }

    #[tokio::test]
    async fn test_status_push_update() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        // Get initial status
        let initial = ns.get_status("mydb:main").await.unwrap().unwrap();
        assert_eq!(initial.v, 1);

        // Push new status
        let new_status = StatusValue::new(2, StatusPayload::new("ready"));
        let result = ns
            .push_status("mydb:main", Some(&initial), &new_status)
            .await
            .unwrap();
        assert!(matches!(result, StatusCasResult::Updated));

        // Verify update
        let current = ns.get_status("mydb:main").await.unwrap().unwrap();
        assert_eq!(current.v, 2);
        assert_eq!(current.payload.state, "ready");
    }

    #[tokio::test]
    async fn test_status_push_conflict_wrong_expected() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        // Try to push with wrong expected value
        let wrong_expected = StatusValue::new(5, StatusPayload::new("wrong"));
        let new_status = StatusValue::new(6, StatusPayload::new("ready"));
        let result = ns
            .push_status("mydb:main", Some(&wrong_expected), &new_status)
            .await
            .unwrap();

        match result {
            StatusCasResult::Conflict { actual } => {
                let a = actual.unwrap();
                assert_eq!(a.v, 1); // Initial status
                assert_eq!(a.payload.state, "ready");
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_status_push_conflict_non_monotonic() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let initial = ns.get_status("mydb:main").await.unwrap().unwrap();

        // Push with v=2 (valid)
        let status_v2 = StatusValue::new(2, StatusPayload::new("ready"));
        ns.push_status("mydb:main", Some(&initial), &status_v2)
            .await
            .unwrap();

        // Try to push with v=1 (non-monotonic)
        let status_v1 = StatusValue::new(1, StatusPayload::new("old"));
        let result = ns
            .push_status("mydb:main", Some(&status_v2), &status_v1)
            .await
            .unwrap();

        match result {
            StatusCasResult::Conflict { actual } => {
                assert_eq!(actual.unwrap().v, 2);
            }
            _ => panic!("expected conflict for non-monotonic update"),
        }
    }

    #[tokio::test]
    async fn test_status_push_with_extra_metadata() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let initial = ns.get_status("mydb:main").await.unwrap().unwrap();

        // Push status with extra metadata
        let mut extra = std::collections::HashMap::new();
        extra.insert("queue_depth".to_string(), serde_json::json!(5));
        extra.insert("last_commit_ms".to_string(), serde_json::json!(42));

        let new_status = StatusValue::new(2, StatusPayload::with_extra("indexing", extra));
        ns.push_status("mydb:main", Some(&initial), &new_status)
            .await
            .unwrap();

        let current = ns.get_status("mydb:main").await.unwrap().unwrap();
        assert_eq!(current.payload.state, "indexing");
        assert_eq!(
            current.payload.extra.get("queue_depth"),
            Some(&serde_json::json!(5))
        );
    }

    // =========================================================================
    // ConfigPublisher tests
    // =========================================================================

    #[tokio::test]
    async fn test_config_get_nonexistent() {
        let ns = MemoryNameService::new();
        let result = ns.get_config("nonexistent:main").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_config_get_unborn() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let config = ns.get_config("mydb:main").await.unwrap().unwrap();
        assert!(config.is_unborn());
        assert_eq!(config.v, 0);
        assert!(config.payload.is_none());
    }

    #[tokio::test]
    async fn test_config_push_from_unborn() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let unborn = ns.get_config("mydb:main").await.unwrap().unwrap();
        assert!(unborn.is_unborn());

        // Push first config
        let new_config = ConfigValue::new(
            1,
            Some(ConfigPayload::with_default_context("fluree:ctx/v1")),
        );
        let result = ns
            .push_config("mydb:main", Some(&unborn), &new_config)
            .await
            .unwrap();
        assert!(matches!(result, ConfigCasResult::Updated));

        // Verify update
        let current = ns.get_config("mydb:main").await.unwrap().unwrap();
        assert!(!current.is_unborn());
        assert_eq!(current.v, 1);
        assert_eq!(
            current.payload.as_ref().unwrap().default_context,
            Some("fluree:ctx/v1".to_string())
        );
    }

    #[tokio::test]
    async fn test_config_push_update() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let unborn = ns.get_config("mydb:main").await.unwrap().unwrap();

        // Push first config
        let config_v1 = ConfigValue::new(
            1,
            Some(ConfigPayload::with_default_context("fluree:ctx/v1")),
        );
        ns.push_config("mydb:main", Some(&unborn), &config_v1)
            .await
            .unwrap();

        // Push updated config
        let config_v2 = ConfigValue::new(
            2,
            Some(ConfigPayload::with_default_context("fluree:ctx/v2")),
        );
        let result = ns
            .push_config("mydb:main", Some(&config_v1), &config_v2)
            .await
            .unwrap();
        assert!(matches!(result, ConfigCasResult::Updated));

        let current = ns.get_config("mydb:main").await.unwrap().unwrap();
        assert_eq!(current.v, 2);
        assert_eq!(
            current.payload.as_ref().unwrap().default_context,
            Some("fluree:ctx/v2".to_string())
        );
    }

    #[tokio::test]
    async fn test_config_push_conflict_wrong_expected() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        // Try to push with wrong expected value
        let wrong_expected = ConfigValue::new(5, Some(ConfigPayload::new()));
        let new_config = ConfigValue::new(6, Some(ConfigPayload::new()));
        let result = ns
            .push_config("mydb:main", Some(&wrong_expected), &new_config)
            .await
            .unwrap();

        match result {
            ConfigCasResult::Conflict { actual } => {
                let a = actual.unwrap();
                assert!(a.is_unborn()); // Was unborn
            }
            _ => panic!("expected conflict"),
        }
    }

    #[tokio::test]
    async fn test_config_push_conflict_non_monotonic() {
        let ns = MemoryNameService::new();
        ns.publish_ledger_init("mydb:main").await.unwrap();

        let unborn = ns.get_config("mydb:main").await.unwrap().unwrap();

        // Push v=1
        let config_v1 = ConfigValue::new(1, Some(ConfigPayload::new()));
        ns.push_config("mydb:main", Some(&unborn), &config_v1)
            .await
            .unwrap();

        // Push v=2
        let config_v2 = ConfigValue::new(2, Some(ConfigPayload::new()));
        ns.push_config("mydb:main", Some(&config_v1), &config_v2)
            .await
            .unwrap();

        // Try to push v=1 (non-monotonic)
        let config_old = ConfigValue::new(1, Some(ConfigPayload::new()));
        let result = ns
            .push_config("mydb:main", Some(&config_v2), &config_old)
            .await
            .unwrap();

        match result {
            ConfigCasResult::Conflict { actual } => {
                assert_eq!(actual.unwrap().v, 2);
            }
            _ => panic!("expected conflict for non-monotonic update"),
        }
    }

    #[tokio::test]
    async fn test_config_push_no_record() {
        let ns = MemoryNameService::new();

        // Try to push config without ledger record
        let new_config = ConfigValue::new(1, Some(ConfigPayload::new()));
        let result = ns
            .push_config("nonexistent:main", None, &new_config)
            .await
            .unwrap();

        match result {
            ConfigCasResult::Conflict { actual } => {
                assert!(actual.is_none());
            }
            _ => panic!("expected conflict when no record exists"),
        }
    }
}
