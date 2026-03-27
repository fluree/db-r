//! In memory storage

use crate::error::Result;
use crate::ContentWriteResult;
use crate::StorageMethod;
use crate::{
    content_address, CasAction, CasOutcome, ContentAddressedWrite, ContentId, ContentKind,
    ContentStore, StorageCas, StorageExtError, StorageExtResult, StorageRead, StorageWrite,
};
use async_trait::async_trait;
use parking_lot::RwLock;
use std::sync::Arc;

/// Storage method for in-memory storage (testing).
pub const STORAGE_METHOD_MEMORY: &str = "memory";

/// Simple in-memory storage
///
/// This implementation stores data in a HashMap with interior mutability
/// (via `Arc<RwLock<...>>`) to support both reading and writing.
/// Useful for unit tests and in-memory ledger operations.
#[derive(Debug, Clone)]
pub struct MemoryStorage {
    data: Arc<RwLock<std::collections::HashMap<String, Vec<u8>>>>,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStorage {
    /// Create a new empty memory storage
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Insert data at the given address
    ///
    /// Note: This method takes `&self` (not `&mut self`) due to interior mutability.
    pub fn insert(&self, address: impl Into<String>, data: Vec<u8>) {
        self.data.write().insert(address.into(), data);
    }

    /// Insert JSON data at the given address
    ///
    /// Note: This method takes `&self` (not `&mut self`) due to interior mutability.
    pub fn insert_json<T: serde::Serialize>(
        &self,
        address: impl Into<String>,
        value: &T,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        self.insert(address, bytes);
        Ok(())
    }
}

#[async_trait]
impl StorageRead for MemoryStorage {
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>> {
        self.data
            .read()
            .get(address)
            .cloned()
            .ok_or_else(|| crate::error::Error::not_found(address))
    }

    async fn exists(&self, address: &str) -> Result<bool> {
        Ok(self.data.read().contains_key(address))
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let data = self.data.read();
        Ok(data
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn read_byte_range(&self, address: &str, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        if range.start >= range.end {
            return Ok(Vec::new());
        }
        let data = self.data.read();
        let full = data
            .get(address)
            .ok_or_else(|| crate::error::Error::not_found(address))?;
        let start = range.start as usize;
        let end = (range.end as usize).min(full.len());
        if start >= full.len() {
            return Ok(Vec::new());
        }
        Ok(full[start..end].to_vec())
    }
}

#[async_trait]
impl StorageWrite for MemoryStorage {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> Result<()> {
        self.data
            .write()
            .insert(address.to_string(), bytes.to_vec());
        Ok(())
    }

    async fn delete(&self, address: &str) -> Result<()> {
        // Idempotent: ok even if not found
        self.data.write().remove(address);
        Ok(())
    }
}

impl StorageMethod for MemoryStorage {
    fn storage_method(&self) -> &str {
        STORAGE_METHOD_MEMORY
    }
}

#[async_trait]
impl ContentAddressedWrite for MemoryStorage {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        let address = content_address(STORAGE_METHOD_MEMORY, kind, ledger_id, content_hash_hex);
        self.write_bytes(&address, bytes).await?;
        Ok(ContentWriteResult {
            address,
            content_hash: content_hash_hex.to_string(),
            size_bytes: bytes.len(),
        })
    }
}

#[async_trait]
impl StorageCas for MemoryStorage {
    async fn insert(&self, address: &str, bytes: &[u8]) -> StorageExtResult<bool> {
        let mut data = self.data.write();
        if data.contains_key(address) {
            Ok(false)
        } else {
            data.insert(address.to_string(), bytes.to_vec());
            Ok(true)
        }
    }

    async fn compare_and_swap<T, F>(&self, address: &str, f: F) -> StorageExtResult<CasOutcome<T>>
    where
        F: Fn(Option<&[u8]>) -> std::result::Result<CasAction<T>, StorageExtError> + Send + Sync,
        T: Send,
    {
        let mut data = self.data.write();
        let current = data.get(address).map(|v| v.as_slice());
        match f(current)? {
            CasAction::Write(new_bytes) => {
                data.insert(address.to_string(), new_bytes);
                Ok(CasOutcome::Written)
            }
            CasAction::Abort(t) => Ok(CasOutcome::Aborted(t)),
        }
    }
}

/// In-memory content store keyed by `ContentId`.
///
/// This is the CID-first counterpart to [`MemoryStorage`].
#[derive(Debug, Clone)]
pub struct MemoryContentStore {
    data: Arc<RwLock<std::collections::HashMap<ContentId, Vec<u8>>>>,
}

impl Default for MemoryContentStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryContentStore {
    /// Create a new empty in-memory content store.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }
}

#[async_trait]
impl ContentStore for MemoryContentStore {
    async fn has(&self, id: &ContentId) -> Result<bool> {
        Ok(self.data.read().contains_key(id))
    }

    async fn get(&self, id: &ContentId) -> Result<Vec<u8>> {
        self.data
            .read()
            .get(id)
            .cloned()
            .ok_or_else(|| crate::error::Error::not_found(id.to_string()))
    }

    async fn put(&self, kind: ContentKind, bytes: &[u8]) -> Result<ContentId> {
        let id = ContentId::new(kind, bytes);
        self.data.write().insert(id.clone(), bytes.to_vec());
        Ok(id)
    }

    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> Result<()> {
        if !id.verify(bytes) {
            return Err(crate::error::Error::storage(format!(
                "CID verification failed: provided CID {} does not match bytes",
                id
            )));
        }
        self.data.write().insert(id.clone(), bytes.to_vec());
        Ok(())
    }

    async fn get_range(&self, id: &ContentId, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        let data = self.data.read();
        let full = data
            .get(id)
            .ok_or_else(|| crate::error::Error::not_found(id.to_string()))?;
        let start = range.start as usize;
        let end = (range.end as usize).min(full.len());
        if start >= full.len() {
            return Ok(Vec::new());
        }
        Ok(full[start..end].to_vec())
    }
}
