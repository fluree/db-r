//! BM25 Index Serialization
//!
//! Provides snapshot serialization and deserialization for the BM25 index
//! using the postcard binary format. This enables persistence of the index
//! for virtual graph storage.

use std::io::{Read, Write};

use super::index::Bm25Index;

/// Error type for serialization operations.
#[derive(Debug, thiserror::Error)]
pub enum SerializeError {
    #[error("Postcard serialization error: {0}")]
    Postcard(#[from] postcard::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid snapshot format: {0}")]
    InvalidFormat(String),
}

pub type Result<T> = std::result::Result<T, SerializeError>;

/// Magic bytes for BM25 snapshot files
const SNAPSHOT_MAGIC: &[u8; 4] = b"BM25";

/// Current snapshot format version
const SNAPSHOT_VERSION: u8 = 1;

/// Serialize a BM25 index to bytes.
pub fn serialize(index: &Bm25Index) -> Result<Vec<u8>> {
    let mut data = Vec::new();

    // Write header
    data.extend_from_slice(SNAPSHOT_MAGIC);
    data.push(SNAPSHOT_VERSION);

    // Serialize the index with postcard
    let index_bytes = postcard::to_allocvec(index)?;

    // Write length prefix (4 bytes, big-endian)
    let len = index_bytes.len() as u32;
    data.extend_from_slice(&len.to_be_bytes());

    // Write index data
    data.extend_from_slice(&index_bytes);

    Ok(data)
}

/// Deserialize a BM25 index from bytes.
pub fn deserialize(data: &[u8]) -> Result<Bm25Index> {
    if data.len() < 9 {
        return Err(SerializeError::InvalidFormat(
            "Data too short for header".to_string(),
        ));
    }

    // Check magic bytes
    if &data[0..4] != SNAPSHOT_MAGIC {
        return Err(SerializeError::InvalidFormat(
            "Invalid magic bytes".to_string(),
        ));
    }

    // Check version
    let version = data[4];
    if version != SNAPSHOT_VERSION {
        return Err(SerializeError::InvalidFormat(format!(
            "Unsupported version: {} (expected {})",
            version, SNAPSHOT_VERSION
        )));
    }

    // Read length prefix
    let len_bytes: [u8; 4] = data[5..9].try_into().unwrap();
    let len = u32::from_be_bytes(len_bytes) as usize;

    if data.len() < 9 + len {
        return Err(SerializeError::InvalidFormat(
            "Data truncated".to_string(),
        ));
    }

    // Deserialize index
    let index = postcard::from_bytes(&data[9..9 + len])?;

    Ok(index)
}

/// Write a BM25 index snapshot to a writer.
pub fn write_snapshot<W: Write>(index: &Bm25Index, mut writer: W) -> Result<()> {
    let data = serialize(index)?;
    writer.write_all(&data)?;
    Ok(())
}

/// Read a BM25 index snapshot from a reader.
pub fn read_snapshot<R: Read>(mut reader: R) -> Result<Bm25Index> {
    let mut data = Vec::new();
    reader.read_to_end(&mut data)?;
    deserialize(&data)
}

/// Compute a checksum of the index for verification.
///
/// Uses the index contents to generate a deterministic hash.
pub fn compute_checksum(index: &Bm25Index) -> u64 {
    use std::hash::{Hash, Hasher};
    use std::collections::hash_map::DefaultHasher;

    let mut hasher = DefaultHasher::new();

    // Hash key components
    index.stats.num_docs.hash(&mut hasher);
    index.stats.total_terms.hash(&mut hasher);
    index.config.k1.to_bits().hash(&mut hasher);
    index.config.b.to_bits().hash(&mut hasher);

    // Hash number of terms and documents
    index.terms.len().hash(&mut hasher);
    index.doc_vectors.len().hash(&mut hasher);

    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bm25::index::{Bm25Config, DocKey};
    use std::collections::HashMap;

    fn build_test_index() -> Bm25Index {
        let mut index = Bm25Index::with_config(Bm25Config::new(1.5, 0.8));

        // Add some documents
        let doc1 = DocKey::new("test:main", "http://example.org/doc1");
        let mut tf1 = HashMap::new();
        tf1.insert("hello", 2);
        tf1.insert("world", 1);
        index.add_document(doc1, tf1);

        let doc2 = DocKey::new("test:main", "http://example.org/doc2");
        let mut tf2 = HashMap::new();
        tf2.insert("hello", 1);
        tf2.insert("rust", 3);
        index.add_document(doc2, tf2);

        // Set watermarks
        index.watermark.update("test:main", 42);

        index
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let original = build_test_index();

        let data = serialize(&original).expect("serialize failed");
        let restored = deserialize(&data).expect("deserialize failed");

        // Verify basic properties
        assert_eq!(restored.num_docs(), original.num_docs());
        assert_eq!(restored.num_terms(), original.num_terms());
        assert_eq!(restored.stats.total_terms, original.stats.total_terms);
        assert_eq!(restored.config.k1, original.config.k1);
        assert_eq!(restored.config.b, original.config.b);

        // Verify watermarks
        assert_eq!(
            restored.watermark.get("test:main"),
            original.watermark.get("test:main")
        );

        // Verify documents exist
        let doc1 = DocKey::new("test:main", "http://example.org/doc1");
        let doc2 = DocKey::new("test:main", "http://example.org/doc2");
        assert!(restored.contains_doc(&doc1));
        assert!(restored.contains_doc(&doc2));

        // Verify document vectors
        let vec1 = restored.get_doc_vector(&doc1).unwrap();
        assert_eq!(vec1.doc_length(), 3); // "hello" x2 + "world" x1
    }

    #[test]
    fn test_serialize_empty_index() {
        let original = Bm25Index::new();

        let data = serialize(&original).expect("serialize failed");
        let restored = deserialize(&data).expect("deserialize failed");

        assert_eq!(restored.num_docs(), 0);
        assert_eq!(restored.num_terms(), 0);
    }

    #[test]
    fn test_invalid_magic_bytes() {
        let data = b"XXXXsome data here";
        let result = deserialize(data);
        assert!(matches!(result, Err(SerializeError::InvalidFormat(_))));
    }

    #[test]
    fn test_invalid_version() {
        let mut data = Vec::new();
        data.extend_from_slice(SNAPSHOT_MAGIC);
        data.push(99); // Invalid version
        data.extend_from_slice(&[0, 0, 0, 0]); // Zero length

        let result = deserialize(&data);
        assert!(matches!(result, Err(SerializeError::InvalidFormat(_))));
    }

    #[test]
    fn test_truncated_data() {
        let original = build_test_index();
        let data = serialize(&original).expect("serialize failed");

        // Truncate the data
        let truncated = &data[0..data.len() / 2];
        let result = deserialize(truncated);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_read_snapshot() {
        let original = build_test_index();

        let mut buffer = Vec::new();
        write_snapshot(&original, &mut buffer).expect("write failed");

        let cursor = std::io::Cursor::new(buffer);
        let restored = read_snapshot(cursor).expect("read failed");

        assert_eq!(restored.num_docs(), original.num_docs());
    }

    #[test]
    fn test_compute_checksum() {
        let index1 = build_test_index();
        let index2 = build_test_index();

        // Same index should have same checksum
        let checksum1 = compute_checksum(&index1);
        let checksum2 = compute_checksum(&index2);
        assert_eq!(checksum1, checksum2);

        // Different index should have different checksum
        let mut index3 = build_test_index();
        let doc3 = DocKey::new("test:main", "http://example.org/doc3");
        let mut tf3 = HashMap::new();
        tf3.insert("different", 1);
        index3.add_document(doc3, tf3);

        let checksum3 = compute_checksum(&index3);
        assert_ne!(checksum1, checksum3);
    }
}
