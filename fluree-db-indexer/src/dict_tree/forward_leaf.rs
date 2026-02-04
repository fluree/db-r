//! Forward leaf format: numeric ID → variable-length value.
//!
//! Used by both subject forward (sid64 → IRI) and string forward
//! (string_id → value) trees. Entries are sorted by ID for binary search.
//!
//! ## Binary layout
//!
//! ```text
//! [magic: 4B "DLF1"]
//! [entry_count: u32]
//! [offset_table: u32 × entry_count]  // byte offset of each entry in data section
//! [data section: entries...]
//!   entry := [id: u64] [value_len: u32] [value_bytes: u8 × value_len]
//! ```
//!
//! The offset table enables O(log n) binary search by random-accessing
//! any entry without scanning previous entries.

use std::io;

/// Magic bytes for a forward leaf file.
pub const FORWARD_LEAF_MAGIC: [u8; 4] = *b"DLF1";

/// Header size: magic (4) + entry_count (4).
const HEADER_SIZE: usize = 8;

/// Fixed overhead per entry in the data section: id (8) + value_len (4).
const ENTRY_OVERHEAD: usize = 12;

/// A single forward leaf entry (id → value).
#[derive(Debug, Clone)]
pub struct ForwardEntry {
    pub id: u64,
    pub value: Vec<u8>,
}

/// Encode a sorted slice of forward entries into a leaf blob.
///
/// Entries **must** be sorted by `id` in ascending order.
/// Returns the complete leaf bytes ready for CAS upload.
pub fn encode_forward_leaf(entries: &[ForwardEntry]) -> Vec<u8> {
    let entry_count = entries.len() as u32;

    // Pre-compute data section size
    let data_size: usize = entries.iter().map(|e| ENTRY_OVERHEAD + e.value.len()).sum();
    let offset_table_size = entries.len() * 4;
    let total = HEADER_SIZE + offset_table_size + data_size;

    let mut buf = Vec::with_capacity(total);

    // Header
    buf.extend_from_slice(&FORWARD_LEAF_MAGIC);
    buf.extend_from_slice(&entry_count.to_le_bytes());

    // Build offset table (offsets relative to start of data section)
    let mut offset: u32 = 0;
    for e in entries {
        buf.extend_from_slice(&offset.to_le_bytes());
        offset += (ENTRY_OVERHEAD + e.value.len()) as u32;
    }

    // Data section
    for e in entries {
        buf.extend_from_slice(&e.id.to_le_bytes());
        buf.extend_from_slice(&(e.value.len() as u32).to_le_bytes());
        buf.extend_from_slice(&e.value);
    }

    debug_assert_eq!(buf.len(), total);
    buf
}

/// Decoded forward leaf providing O(log n) lookup by ID.
pub struct ForwardLeaf<'a> {
    data: &'a [u8],
    entry_count: u32,
    offset_table_start: usize,
    data_section_start: usize,
}

impl<'a> ForwardLeaf<'a> {
    /// Parse a forward leaf from raw bytes.
    pub fn from_bytes(data: &'a [u8]) -> io::Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "forward leaf: too small for header",
            ));
        }
        if data[0..4] != FORWARD_LEAF_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "forward leaf: invalid magic",
            ));
        }
        let entry_count = u32::from_le_bytes(data[4..8].try_into().unwrap());
        let offset_table_start = HEADER_SIZE;
        let data_section_start = offset_table_start + (entry_count as usize) * 4;

        if data.len() < data_section_start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "forward leaf: truncated offset table",
            ));
        }

        Ok(Self {
            data,
            entry_count,
            offset_table_start,
            data_section_start,
        })
    }

    /// Number of entries in this leaf.
    pub fn entry_count(&self) -> u32 {
        self.entry_count
    }

    /// Read the ID at the given index (0-based).
    #[inline]
    fn id_at(&self, index: usize) -> u64 {
        let offset = self.entry_offset(index);
        u64::from_le_bytes(self.data[offset..offset + 8].try_into().unwrap())
    }

    /// Read the value bytes at the given index.
    fn value_at(&self, index: usize) -> &'a [u8] {
        let offset = self.entry_offset(index);
        let value_len =
            u32::from_le_bytes(self.data[offset + 8..offset + 12].try_into().unwrap()) as usize;
        &self.data[offset + 12..offset + 12 + value_len]
    }

    /// Byte offset of the entry at `index` within `self.data`.
    #[inline]
    fn entry_offset(&self, index: usize) -> usize {
        let table_pos = self.offset_table_start + index * 4;
        let relative =
            u32::from_le_bytes(self.data[table_pos..table_pos + 4].try_into().unwrap()) as usize;
        self.data_section_start + relative
    }

    /// Look up a value by ID using binary search. Returns `None` if not found.
    pub fn lookup(&self, target_id: u64) -> Option<&'a [u8]> {
        if self.entry_count == 0 {
            return None;
        }

        let mut lo = 0usize;
        let mut hi = self.entry_count as usize;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_id = self.id_at(mid);
            if mid_id < target_id {
                lo = mid + 1;
            } else if mid_id > target_id {
                hi = mid;
            } else {
                return Some(self.value_at(mid));
            }
        }

        None
    }

    /// Return the first ID in this leaf (for branch boundary keys).
    pub fn first_id(&self) -> Option<u64> {
        if self.entry_count > 0 {
            Some(self.id_at(0))
        } else {
            None
        }
    }

    /// Return the last ID in this leaf.
    pub fn last_id(&self) -> Option<u64> {
        if self.entry_count > 0 {
            Some(self.id_at(self.entry_count as usize - 1))
        } else {
            None
        }
    }

    /// Iterate all entries in order.
    pub fn iter(&'a self) -> ForwardLeafIter<'a> {
        ForwardLeafIter {
            leaf: self,
            index: 0,
        }
    }
}

/// Iterator over forward leaf entries.
pub struct ForwardLeafIter<'a> {
    leaf: &'a ForwardLeaf<'a>,
    index: usize,
}

impl<'a> Iterator for ForwardLeafIter<'a> {
    type Item = (u64, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.leaf.entry_count as usize {
            return None;
        }
        let id = self.leaf.id_at(self.index);
        let value = self.leaf.value_at(self.index);
        self.index += 1;
        Some((id, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entries(n: usize) -> Vec<ForwardEntry> {
        (0..n)
            .map(|i| ForwardEntry {
                id: (i as u64) * 10,
                value: format!("http://example.org/entity/{}", i).into_bytes(),
            })
            .collect()
    }

    #[test]
    fn test_round_trip_small() {
        let entries = make_entries(5);
        let blob = encode_forward_leaf(&entries);
        let leaf = ForwardLeaf::from_bytes(&blob).unwrap();

        assert_eq!(leaf.entry_count(), 5);
        assert_eq!(leaf.first_id(), Some(0));
        assert_eq!(leaf.last_id(), Some(40));

        for e in &entries {
            let val = leaf.lookup(e.id).unwrap();
            assert_eq!(val, &e.value[..]);
        }

        // Miss
        assert!(leaf.lookup(5).is_none());
        assert!(leaf.lookup(999).is_none());
    }

    #[test]
    fn test_round_trip_empty() {
        let blob = encode_forward_leaf(&[]);
        let leaf = ForwardLeaf::from_bytes(&blob).unwrap();
        assert_eq!(leaf.entry_count(), 0);
        assert!(leaf.first_id().is_none());
        assert!(leaf.lookup(0).is_none());
    }

    #[test]
    fn test_round_trip_single() {
        let entries = vec![ForwardEntry {
            id: 42,
            value: b"hello".to_vec(),
        }];
        let blob = encode_forward_leaf(&entries);
        let leaf = ForwardLeaf::from_bytes(&blob).unwrap();
        assert_eq!(leaf.lookup(42), Some(b"hello".as_slice()));
        assert!(leaf.lookup(0).is_none());
    }

    #[test]
    fn test_iterator() {
        let entries = make_entries(3);
        let blob = encode_forward_leaf(&entries);
        let leaf = ForwardLeaf::from_bytes(&blob).unwrap();

        let collected: Vec<_> = leaf.iter().collect();
        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0].0, 0);
        assert_eq!(collected[1].0, 10);
        assert_eq!(collected[2].0, 20);
    }

    #[test]
    fn test_sid64_keys() {
        // Simulate sid64 keys: (ns_code << 48) | local_id
        let entries = vec![
            ForwardEntry {
                id: (2u64 << 48) | 0,
                value: b"http://a.org/x".to_vec(),
            },
            ForwardEntry {
                id: (2u64 << 48) | 1,
                value: b"http://a.org/y".to_vec(),
            },
            ForwardEntry {
                id: (3u64 << 48) | 0,
                value: b"http://b.org/z".to_vec(),
            },
        ];
        let blob = encode_forward_leaf(&entries);
        let leaf = ForwardLeaf::from_bytes(&blob).unwrap();

        assert_eq!(
            leaf.lookup((2u64 << 48) | 1),
            Some(b"http://a.org/y".as_slice())
        );
        assert_eq!(
            leaf.lookup((3u64 << 48) | 0),
            Some(b"http://b.org/z".as_slice())
        );
        assert!(leaf.lookup((3u64 << 48) | 1).is_none());
    }

    #[test]
    fn test_large_leaf() {
        let entries = make_entries(10_000);
        let blob = encode_forward_leaf(&entries);
        let leaf = ForwardLeaf::from_bytes(&blob).unwrap();

        // Spot-check a few
        assert_eq!(leaf.entry_count(), 10_000);
        assert!(leaf.lookup(0).is_some());
        assert!(leaf.lookup(50_000).is_some());
        assert!(leaf.lookup(99_990).is_some());
        assert!(leaf.lookup(99_991).is_none()); // not a multiple of 10
    }
}
