//! File-based implementation of [`RemoteTrackingStore`]
//!
//! Stores tracking records at `{base_path}/ns-sync/remotes/{remote_name}/{alias_encoded}.json`.
//! Aliases are percent-encoded to handle `:` and `/` safely in filenames.
//!
//! This module is only available with the `native` feature (requires filesystem access).

use crate::tracking::{RemoteName, RemoteTrackingStore, TrackingRecord};
use crate::{NameServiceError, Result};
use async_trait::async_trait;
use std::fmt::Debug;
use std::path::{Path, PathBuf};

/// File-based tracking store.
///
/// Stores tracking state outside the `ns@v2/` tree at:
/// `{base_path}/ns-sync/remotes/{remote}/{alias_encoded}.json`
#[derive(Debug)]
pub struct FileTrackingStore {
    base_path: PathBuf,
}

impl FileTrackingStore {
    /// Create a new file-based tracking store.
    ///
    /// `base_path` is the same root used by `FileNameService` — tracking
    /// state goes into `{base_path}/ns-sync/` (outside `ns@v2/`).
    pub fn new(base_path: impl AsRef<Path>) -> Self {
        Self {
            base_path: base_path.as_ref().to_path_buf(),
        }
    }

    /// Directory for a specific remote's tracking files.
    fn remote_dir(&self, remote: &RemoteName) -> PathBuf {
        self.base_path
            .join("ns-sync")
            .join("remotes")
            .join(&remote.0)
    }

    /// Full path to a tracking record file.
    fn record_path(&self, remote: &RemoteName, alias: &str) -> PathBuf {
        self.remote_dir(remote)
            .join(format!("{}.json", encode_alias(alias)))
    }
}

/// Percent-encode an alias for use as a filename.
///
/// Encodes `:`, `/`, `\`, and `%` to avoid path traversal and filesystem issues.
fn encode_alias(alias: &str) -> String {
    let mut encoded = String::with_capacity(alias.len());
    for ch in alias.chars() {
        match ch {
            '%' => encoded.push_str("%25"),
            ':' => encoded.push_str("%3A"),
            '/' => encoded.push_str("%2F"),
            '\\' => encoded.push_str("%5C"),
            _ => encoded.push(ch),
        }
    }
    encoded
}

/// Decode a percent-encoded alias filename back to the original alias.
#[cfg(test)]
fn decode_alias(encoded: &str) -> String {
    let mut decoded = String::with_capacity(encoded.len());
    let mut chars = encoded.chars();
    while let Some(ch) = chars.next() {
        if ch == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            match hex.as_str() {
                "25" => decoded.push('%'),
                "3A" | "3a" => decoded.push(':'),
                "2F" | "2f" => decoded.push('/'),
                "5C" | "5c" => decoded.push('\\'),
                _ => {
                    decoded.push('%');
                    decoded.push_str(&hex);
                }
            }
        } else {
            decoded.push(ch);
        }
    }
    decoded
}

#[async_trait]
impl RemoteTrackingStore for FileTrackingStore {
    async fn get_tracking(
        &self,
        remote: &RemoteName,
        alias: &str,
    ) -> Result<Option<TrackingRecord>> {
        let path = self.record_path(remote, alias);
        let path_clone = path.clone();

        tokio::task::spawn_blocking(move || match std::fs::read_to_string(&path_clone) {
            Ok(contents) => {
                let record: TrackingRecord = serde_json::from_str(&contents)?;
                Ok(Some(record))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(NameServiceError::storage(format!(
                "Failed to read tracking record {}: {}",
                path_clone.display(),
                e
            ))),
        })
        .await
        .map_err(|e| NameServiceError::storage(format!("Task join error: {}", e)))?
    }

    async fn set_tracking(&self, record: &TrackingRecord) -> Result<()> {
        let path = self.record_path(&record.remote, &record.alias);
        let json = serde_json::to_string_pretty(record)?;

        tokio::task::spawn_blocking(move || {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    NameServiceError::storage(format!(
                        "Failed to create tracking directory {}: {}",
                        parent.display(),
                        e
                    ))
                })?;
            }
            // Atomic write via temp file + rename
            let tmp_path = path.with_extension("json.tmp");
            std::fs::write(&tmp_path, json.as_bytes()).map_err(|e| {
                NameServiceError::storage(format!(
                    "Failed to write tracking record {}: {}",
                    tmp_path.display(),
                    e
                ))
            })?;
            std::fs::rename(&tmp_path, &path).map_err(|e| {
                NameServiceError::storage(format!(
                    "Failed to rename tracking record {}: {}",
                    path.display(),
                    e
                ))
            })?;
            Ok(())
        })
        .await
        .map_err(|e| NameServiceError::storage(format!("Task join error: {}", e)))?
    }

    async fn list_tracking(&self, remote: &RemoteName) -> Result<Vec<TrackingRecord>> {
        let dir = self.remote_dir(remote);

        tokio::task::spawn_blocking(move || {
            let entries = match std::fs::read_dir(&dir) {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
                Err(e) => {
                    return Err(NameServiceError::storage(format!(
                        "Failed to list tracking directory {}: {}",
                        dir.display(),
                        e
                    )))
                }
            };

            let mut records = Vec::new();
            for entry in entries {
                let entry = entry.map_err(|e| {
                    NameServiceError::storage(format!("Failed to read directory entry: {}", e))
                })?;
                let path = entry.path();
                if path
                    .extension()
                    .is_some_and(|ext| ext == "json")
                {
                    // Skip tmp files
                    if path
                        .file_name()
                        .and_then(|f| f.to_str())
                        .is_some_and(|f| f.ends_with(".json.tmp"))
                    {
                        continue;
                    }

                    match std::fs::read_to_string(&path) {
                        Ok(contents) => match serde_json::from_str::<TrackingRecord>(&contents) {
                            Ok(record) => records.push(record),
                            Err(e) => {
                                tracing::warn!(
                                    "Skipping malformed tracking record {}: {}",
                                    path.display(),
                                    e
                                );
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                "Failed to read tracking record {}: {}",
                                path.display(),
                                e
                            );
                        }
                    }
                }
            }
            Ok(records)
        })
        .await
        .map_err(|e| NameServiceError::storage(format!("Task join error: {}", e)))?
    }

    async fn remove_tracking(&self, remote: &RemoteName, alias: &str) -> Result<()> {
        let path = self.record_path(remote, alias);

        tokio::task::spawn_blocking(move || match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()), // idempotent
            Err(e) => Err(NameServiceError::storage(format!(
                "Failed to remove tracking record {}: {}",
                path.display(),
                e
            ))),
        })
        .await
        .map_err(|e| NameServiceError::storage(format!("Task join error: {}", e)))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RefValue;
    use tempfile::TempDir;

    fn origin() -> RemoteName {
        RemoteName::new("origin")
    }

    #[test]
    fn test_encode_decode_alias() {
        let alias = "mydb:main";
        let encoded = encode_alias(alias);
        assert_eq!(encoded, "mydb%3Amain");
        assert_eq!(decode_alias(&encoded), alias);

        let alias_with_slash = "org/mydb:main";
        let encoded2 = encode_alias(alias_with_slash);
        assert_eq!(encoded2, "org%2Fmydb%3Amain");
        assert_eq!(decode_alias(&encoded2), alias_with_slash);

        // Roundtrip for percent
        let with_percent = "a%b:c";
        let enc = encode_alias(with_percent);
        assert_eq!(decode_alias(&enc), with_percent);
    }

    #[tokio::test]
    async fn test_file_tracking_get_empty() {
        let tmp = TempDir::new().unwrap();
        let store = FileTrackingStore::new(tmp.path());
        let result = store.get_tracking(&origin(), "mydb:main").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_file_tracking_set_and_get() {
        let tmp = TempDir::new().unwrap();
        let store = FileTrackingStore::new(tmp.path());

        let mut record = TrackingRecord::new(origin(), "mydb:main");
        record.commit_ref = Some(RefValue {
            address: Some("commit-1".to_string()),
            t: 5,
        });

        store.set_tracking(&record).await.unwrap();

        let fetched = store
            .get_tracking(&origin(), "mydb:main")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.alias, "mydb:main");
        assert_eq!(fetched.commit_ref.as_ref().unwrap().t, 5);
    }

    #[tokio::test]
    async fn test_file_tracking_stored_outside_ns_v2() {
        let tmp = TempDir::new().unwrap();
        let store = FileTrackingStore::new(tmp.path());

        store
            .set_tracking(&TrackingRecord::new(origin(), "mydb:main"))
            .await
            .unwrap();

        // Verify file is under ns-sync/, not ns@v2/
        let expected_path = tmp
            .path()
            .join("ns-sync/remotes/origin/mydb%3Amain.json");
        assert!(expected_path.exists(), "File should exist at {:?}", expected_path);

        // Verify ns@v2 directory does NOT exist
        assert!(!tmp.path().join("ns@v2").exists());
    }

    #[tokio::test]
    async fn test_file_tracking_list() {
        let tmp = TempDir::new().unwrap();
        let store = FileTrackingStore::new(tmp.path());

        store
            .set_tracking(&TrackingRecord::new(origin(), "db1:main"))
            .await
            .unwrap();
        store
            .set_tracking(&TrackingRecord::new(origin(), "db2:main"))
            .await
            .unwrap();

        let upstream = RemoteName::new("upstream");
        store
            .set_tracking(&TrackingRecord::new(upstream.clone(), "db3:main"))
            .await
            .unwrap();

        let origin_records = store.list_tracking(&origin()).await.unwrap();
        assert_eq!(origin_records.len(), 2);

        let upstream_records = store.list_tracking(&upstream).await.unwrap();
        assert_eq!(upstream_records.len(), 1);
    }

    #[tokio::test]
    async fn test_file_tracking_remove() {
        let tmp = TempDir::new().unwrap();
        let store = FileTrackingStore::new(tmp.path());

        store
            .set_tracking(&TrackingRecord::new(origin(), "mydb:main"))
            .await
            .unwrap();
        assert!(store
            .get_tracking(&origin(), "mydb:main")
            .await
            .unwrap()
            .is_some());

        store
            .remove_tracking(&origin(), "mydb:main")
            .await
            .unwrap();
        assert!(store
            .get_tracking(&origin(), "mydb:main")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_file_tracking_remove_nonexistent() {
        let tmp = TempDir::new().unwrap();
        let store = FileTrackingStore::new(tmp.path());
        // Should not error
        store
            .remove_tracking(&origin(), "nonexistent:main")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_file_tracking_list_empty_remote() {
        let tmp = TempDir::new().unwrap();
        let store = FileTrackingStore::new(tmp.path());
        let records = store.list_tracking(&origin()).await.unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn test_file_tracking_overwrite() {
        let tmp = TempDir::new().unwrap();
        let store = FileTrackingStore::new(tmp.path());

        let mut record = TrackingRecord::new(origin(), "mydb:main");
        record.commit_ref = Some(RefValue {
            address: Some("commit-1".to_string()),
            t: 1,
        });
        store.set_tracking(&record).await.unwrap();

        record.commit_ref = Some(RefValue {
            address: Some("commit-2".to_string()),
            t: 5,
        });
        store.set_tracking(&record).await.unwrap();

        let fetched = store
            .get_tracking(&origin(), "mydb:main")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.commit_ref.as_ref().unwrap().t, 5);
    }
}
