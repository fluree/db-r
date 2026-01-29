//! Index writer - serialization, content addressing, and storage
//!
//! Handles writing index nodes to storage with content-addressed names.

use crate::error::{IndexerError, Result};
use fluree_db_core::{ContentAddressedWrite, ContentKind};

/// Index writer for persisting nodes to storage
pub struct IndexWriter<'a, S> {
    storage: &'a S,
    ledger_alias: String,
    kind: ContentKind,
    leaf_count: usize,
    branch_count: usize,
    total_bytes: usize,
}

impl<'a, S: ContentAddressedWrite> IndexWriter<'a, S> {
    /// Create a new index writer
    pub fn new(storage: &'a S, alias: impl Into<String>, kind: ContentKind) -> Self {
        Self {
            storage,
            ledger_alias: alias.into(),
            kind,
            leaf_count: 0,
            branch_count: 0,
            total_bytes: 0,
        }
    }

    /// Get number of leaves written
    pub fn leaf_count(&self) -> usize {
        self.leaf_count
    }

    /// Get number of branches written
    pub fn branch_count(&self) -> usize {
        self.branch_count
    }

    /// Get total bytes written
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Reset counts (for per-index-type tracking)
    pub fn reset_counts(&mut self) {
        self.leaf_count = 0;
        self.branch_count = 0;
    }

    /// Write a leaf node and return its storage address
    pub async fn write_leaf(&mut self, bytes: &[u8]) -> Result<String> {
        let res = self
            .storage
            .content_write_bytes(self.kind, &self.ledger_alias, bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        self.leaf_count += 1;
        self.total_bytes += bytes.len();
        Ok(res.address)
    }

    /// Write a branch node and return its storage address
    pub async fn write_branch(&mut self, bytes: &[u8]) -> Result<String> {
        let res = self
            .storage
            .content_write_bytes(self.kind, &self.ledger_alias, bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        self.branch_count += 1;
        self.total_bytes += bytes.len();
        Ok(res.address)
    }

    /// Write the index root and return its storage address
    pub async fn write_root(&mut self, bytes: &[u8]) -> Result<String> {
        let res = self
            .storage
            .content_write_bytes(self.kind, &self.ledger_alias, bytes)
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
        self.total_bytes += bytes.len();
        Ok(res.address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::MemoryStorage;
    use fluree_db_core::IndexType;

    #[tokio::test]
    async fn test_writer_write_leaf() {
        let storage = MemoryStorage::new();
        let mut writer = IndexWriter::new(
            &storage,
            "test:db",
            ContentKind::IndexNode {
                index_type: IndexType::Spot,
            },
        );

        let bytes = b"{\"flakes\": []}";
        let address = writer.write_leaf(bytes).await.unwrap();

        assert!(address.contains("index/spot/"));
        assert!(address.ends_with(".json"));
        assert_eq!(writer.leaf_count(), 1);
        assert_eq!(writer.total_bytes(), bytes.len());
    }

    #[tokio::test]
    async fn test_writer_write_branch() {
        let storage = MemoryStorage::new();
        let mut writer = IndexWriter::new(
            &storage,
            "test:db",
            ContentKind::IndexNode {
                index_type: IndexType::Spot,
            },
        );

        let bytes = b"{\"children\": []}";
        let address = writer.write_branch(bytes).await.unwrap();

        assert!(address.contains("index/spot/"));
        assert_eq!(writer.branch_count(), 1);
    }

    #[tokio::test]
    async fn test_writer_write_root() {
        let storage = MemoryStorage::new();
        let mut writer = IndexWriter::new(&storage, "test:db", ContentKind::IndexRoot);

        let bytes = b"{\"t\": 1}";
        let address = writer.write_root(bytes).await.unwrap();

        assert!(address.contains("index/root/"));
    }

    #[tokio::test]
    async fn test_writer_reset_counts() {
        let storage = MemoryStorage::new();
        let mut writer = IndexWriter::new(
            &storage,
            "test:db",
            ContentKind::IndexNode {
                index_type: IndexType::Spot,
            },
        );

        writer.write_leaf(b"leaf1").await.unwrap();
        writer.write_branch(b"branch1").await.unwrap();

        assert_eq!(writer.leaf_count(), 1);
        assert_eq!(writer.branch_count(), 1);

        writer.reset_counts();

        assert_eq!(writer.leaf_count(), 0);
        assert_eq!(writer.branch_count(), 0);
        // total_bytes is not reset
        assert!(writer.total_bytes() > 0);
    }
}
