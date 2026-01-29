//! Sync configuration types and storage
//!
//! Defines how remotes are configured and how local aliases map to remote aliases.

use crate::error::Result;
use async_trait::async_trait;
use fluree_db_nameservice::RemoteName;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// How to reach a remote
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RemoteEndpoint {
    /// HTTP-based remote (REST API)
    Http { base_url: String },
    /// SSE event stream endpoint
    Sse { events_url: String },
    /// Direct storage access (same backend, different prefix)
    Storage { prefix: String },
}

/// Authentication for a remote
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RemoteAuth {
    /// Bearer token for HTTP requests
    pub token: Option<String>,
}

/// Configuration for a named remote
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteConfig {
    /// Remote name (e.g., "origin")
    pub name: RemoteName,
    /// How to reach the remote
    pub endpoint: RemoteEndpoint,
    /// Authentication credentials
    pub auth: RemoteAuth,
    /// Polling interval in seconds (for polling fallback)
    pub fetch_interval_secs: Option<u64>,
}

/// Maps a local alias to a remote alias for automatic sync
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UpstreamConfig {
    /// Local ledger alias (e.g., "mydb:main")
    pub local_alias: String,
    /// Which remote this tracks
    pub remote: RemoteName,
    /// Alias on the remote (usually same as local_alias)
    pub remote_alias: String,
    /// Whether to automatically fast-forward local on fetch
    pub auto_pull: bool,
}

/// Store for sync configuration (remotes and upstreams)
#[async_trait]
pub trait SyncConfigStore: Debug + Send + Sync {
    async fn get_remote(&self, name: &RemoteName) -> Result<Option<RemoteConfig>>;
    async fn set_remote(&self, config: &RemoteConfig) -> Result<()>;
    async fn remove_remote(&self, name: &RemoteName) -> Result<()>;
    async fn list_remotes(&self) -> Result<Vec<RemoteConfig>>;

    async fn get_upstream(&self, local_alias: &str) -> Result<Option<UpstreamConfig>>;
    async fn set_upstream(&self, config: &UpstreamConfig) -> Result<()>;
    async fn remove_upstream(&self, local_alias: &str) -> Result<()>;
    async fn list_upstreams(&self) -> Result<Vec<UpstreamConfig>>;
}

/// In-memory sync configuration store for testing
#[derive(Debug, Default)]
pub struct MemorySyncConfigStore {
    remotes: parking_lot::RwLock<std::collections::HashMap<String, RemoteConfig>>,
    upstreams: parking_lot::RwLock<std::collections::HashMap<String, UpstreamConfig>>,
}

impl MemorySyncConfigStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl SyncConfigStore for MemorySyncConfigStore {
    async fn get_remote(&self, name: &RemoteName) -> Result<Option<RemoteConfig>> {
        Ok(self.remotes.read().get(name.as_str()).cloned())
    }

    async fn set_remote(&self, config: &RemoteConfig) -> Result<()> {
        self.remotes
            .write()
            .insert(config.name.as_str().to_string(), config.clone());
        Ok(())
    }

    async fn remove_remote(&self, name: &RemoteName) -> Result<()> {
        self.remotes.write().remove(name.as_str());
        Ok(())
    }

    async fn list_remotes(&self) -> Result<Vec<RemoteConfig>> {
        Ok(self.remotes.read().values().cloned().collect())
    }

    async fn get_upstream(&self, local_alias: &str) -> Result<Option<UpstreamConfig>> {
        Ok(self.upstreams.read().get(local_alias).cloned())
    }

    async fn set_upstream(&self, config: &UpstreamConfig) -> Result<()> {
        self.upstreams
            .write()
            .insert(config.local_alias.clone(), config.clone());
        Ok(())
    }

    async fn remove_upstream(&self, local_alias: &str) -> Result<()> {
        self.upstreams.write().remove(local_alias);
        Ok(())
    }

    async fn list_upstreams(&self) -> Result<Vec<UpstreamConfig>> {
        Ok(self.upstreams.read().values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn origin() -> RemoteName {
        RemoteName::new("origin")
    }

    #[tokio::test]
    async fn test_remote_config_crud() {
        let store = MemorySyncConfigStore::new();

        let config = RemoteConfig {
            name: origin(),
            endpoint: RemoteEndpoint::Http {
                base_url: "http://localhost:8090".to_string(),
            },
            auth: RemoteAuth::default(),
            fetch_interval_secs: Some(30),
        };

        store.set_remote(&config).await.unwrap();

        let fetched = store.get_remote(&origin()).await.unwrap().unwrap();
        assert_eq!(fetched.name, origin());

        let all = store.list_remotes().await.unwrap();
        assert_eq!(all.len(), 1);

        store.remove_remote(&origin()).await.unwrap();
        assert!(store.get_remote(&origin()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_upstream_config_crud() {
        let store = MemorySyncConfigStore::new();

        let config = UpstreamConfig {
            local_alias: "mydb:main".to_string(),
            remote: origin(),
            remote_alias: "mydb:main".to_string(),
            auto_pull: true,
        };

        store.set_upstream(&config).await.unwrap();

        let fetched = store.get_upstream("mydb:main").await.unwrap().unwrap();
        assert_eq!(fetched.remote, origin());
        assert!(fetched.auto_pull);

        store.remove_upstream("mydb:main").await.unwrap();
        assert!(store.get_upstream("mydb:main").await.unwrap().is_none());
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = RemoteConfig {
            name: origin(),
            endpoint: RemoteEndpoint::Http {
                base_url: "https://example.com".to_string(),
            },
            auth: RemoteAuth {
                token: Some("secret".to_string()),
            },
            fetch_interval_secs: Some(60),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: RemoteConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, origin());
    }
}
