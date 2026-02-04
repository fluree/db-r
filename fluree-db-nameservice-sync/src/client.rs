//! Remote nameservice client
//!
//! Provides an abstraction for communicating with a remote Fluree nameservice
//! via HTTP REST endpoints.

use crate::error::{Result, SyncError};
use async_trait::async_trait;
use fluree_db_nameservice::{CasResult, NsRecord, RefKind, RefValue, VgNsRecord};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Snapshot of all remote records
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteSnapshot {
    pub ledgers: Vec<NsRecord>,
    #[serde(default)]
    pub vgs: Vec<VgNsRecord>,
}

/// Request body for CAS push
#[derive(Debug, Serialize)]
struct PushRefRequest<'a> {
    expected: Option<&'a RefValue>,
    new: &'a RefValue,
}

/// Response from a push operation
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Fields read during deserialization
struct PushRefResponse {
    status: String,
    #[serde(rename = "ref")]
    ref_value: Option<RefValue>,
    actual: Option<RefValue>,
}

/// Response from init operation
#[derive(Debug, Deserialize)]
struct InitResponse {
    created: bool,
}

/// Client for communicating with a remote nameservice
#[async_trait]
pub trait RemoteNameserviceClient: Debug + Send + Sync {
    /// Look up a single ledger record on the remote
    async fn lookup(&self, alias: &str) -> Result<Option<NsRecord>>;

    /// Get a full snapshot of all remote records (ledgers + VGs)
    async fn snapshot(&self) -> Result<RemoteSnapshot>;

    /// CAS push for a ref on the remote
    async fn push_ref(
        &self,
        alias: &str,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> Result<CasResult>;

    /// Initialize a ledger on the remote (create-if-absent)
    ///
    /// Returns `true` if created, `false` if already existed.
    async fn init_ledger(&self, alias: &str) -> Result<bool>;
}

/// HTTP-based remote client
#[derive(Debug)]
pub struct HttpRemoteClient {
    base_url: String,
    http: reqwest::Client,
    auth_token: Option<String>,
}

impl HttpRemoteClient {
    pub fn new(base_url: impl Into<String>, auth_token: Option<String>) -> Self {
        Self {
            base_url: base_url.into(),
            http: reqwest::Client::new(),
            auth_token,
        }
    }

    fn add_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(ref token) = self.auth_token {
            req.bearer_auth(token)
        } else {
            req
        }
    }

    fn kind_path(kind: RefKind) -> &'static str {
        match kind {
            RefKind::CommitHead => "commit",
            RefKind::IndexHead => "index",
        }
    }
}

#[async_trait]
impl RemoteNameserviceClient for HttpRemoteClient {
    async fn lookup(&self, alias: &str) -> Result<Option<NsRecord>> {
        let url = format!("{}/fluree/storage/ns/{}", self.base_url, alias);
        let resp = self.add_auth(self.http.get(&url)).send().await?;

        match resp.status().as_u16() {
            200 => {
                let record: NsRecord = resp.json().await?;
                Ok(Some(record))
            }
            404 => Ok(None),
            status => Err(SyncError::Remote(format!(
                "Unexpected status {} from {}",
                status, url
            ))),
        }
    }

    async fn snapshot(&self) -> Result<RemoteSnapshot> {
        let url = format!("{}/fluree/nameservice/snapshot", self.base_url);
        let resp = self.add_auth(self.http.get(&url)).send().await?;

        if !resp.status().is_success() {
            return Err(SyncError::Remote(format!(
                "Snapshot failed with status {}",
                resp.status()
            )));
        }

        let snapshot: RemoteSnapshot = resp.json().await?;
        Ok(snapshot)
    }

    async fn push_ref(
        &self,
        alias: &str,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> Result<CasResult> {
        let kind_path = Self::kind_path(kind);
        let url = format!(
            "{}/fluree/nameservice/refs/{}/{}",
            self.base_url, alias, kind_path
        );

        let body = PushRefRequest { expected, new };
        let resp = self
            .add_auth(self.http.post(&url))
            .json(&body)
            .send()
            .await?;

        match resp.status().as_u16() {
            200 => Ok(CasResult::Updated),
            409 => {
                let push_resp: PushRefResponse = resp.json().await?;
                Ok(CasResult::Conflict {
                    actual: push_resp.actual,
                })
            }
            status => Err(SyncError::Remote(format!(
                "Push failed with status {} for {}/{}",
                status, alias, kind_path
            ))),
        }
    }

    async fn init_ledger(&self, alias: &str) -> Result<bool> {
        let url = format!("{}/fluree/nameservice/refs/{}/init", self.base_url, alias);

        let resp = self.add_auth(self.http.post(&url)).send().await?;

        if !resp.status().is_success() {
            return Err(SyncError::Remote(format!(
                "Init failed with status {}",
                resp.status()
            )));
        }

        let init_resp: InitResponse = resp.json().await?;
        Ok(init_resp.created)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_serde() {
        let json = r#"{
            "ledgers": [
                {
                    "address": "mydb:main",
                    "alias": "mydb",
                    "branch": "main",
                    "commit_address": "fluree:file://commit/abc",
                    "commit_t": 5,
                    "index_address": "fluree:file://index/def",
                    "index_t": 3,
                    "retracted": false
                }
            ],
            "vgs": []
        }"#;

        let snapshot: RemoteSnapshot = serde_json::from_str(json).unwrap();
        assert_eq!(snapshot.ledgers.len(), 1);
        assert_eq!(snapshot.ledgers[0].commit_t, 5);
        assert!(snapshot.vgs.is_empty());
    }

    #[test]
    fn test_snapshot_missing_vgs_field() {
        // vgs field is optional (default empty)
        let json = r#"{
            "ledgers": []
        }"#;

        let snapshot: RemoteSnapshot = serde_json::from_str(json).unwrap();
        assert!(snapshot.ledgers.is_empty());
        assert!(snapshot.vgs.is_empty());
    }
}
