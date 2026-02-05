# BM25 Index Optimization Proposal

This document outlines recommendations for improving the BM25 full-text search implementation for better performance, compactness, and time travel support.

## Current Implementation Summary

### Data Structures

| Component | Current Implementation | Location |
|-----------|----------------------|----------|
| Term Dictionary | `BTreeMap<Arc<str>, TermEntry>` | `fluree-db-query/src/bm25/index.rs` |
| Document Vectors | `BTreeMap<DocKey, SparseVector>` | `fluree-db-query/src/bm25/index.rs` |
| Sparse Vector | `Vec<(u32, u32)>` (term_idx, tf) | `fluree-db-query/src/bm25/index.rs` |
| Serialization | Postcard binary format | `fluree-db-query/src/bm25/serialize.rs` |
| Time Travel | Full snapshots at each version | `fluree-db-api/src/virtual_graph/bm25.rs` |

### Architecture

The current design uses a **forward index** (document → terms):
- Each document stores a sparse vector of `(term_idx, term_frequency)` pairs
- Queries scan ALL documents via `iter_docs()` to compute BM25 scores
- Time travel stores complete snapshots at each indexed version

### Key Files

- `fluree-db-query/src/bm25/index.rs` - Core data structures
- `fluree-db-query/src/bm25/builder.rs` - Index building and incremental updates
- `fluree-db-query/src/bm25/scoring.rs` - BM25 scoring algorithm
- `fluree-db-query/src/bm25/analyzer.rs` - Text analysis pipeline
- `fluree-db-query/src/bm25/serialize.rs` - Snapshot serialization
- `fluree-db-api/src/virtual_graph/bm25.rs` - Index lifecycle management

---

## Identified Limitations

| Issue | Current Behavior | Impact | Severity |
|-------|-----------------|--------|----------|
| No posting lists | Must scan ALL documents for every query | O(n) query time regardless of selectivity | **High** |
| Full string term dictionary | Stores complete term strings with no prefix sharing | ~50-70% of index size in typical corpora | Medium |
| Plain integer encoding | Uses fixed 4-byte u32 for all integers | ~2-4x larger than varint encoding | Medium |
| Full snapshot storage | Each version stores complete index | Storage grows linearly with versions | Medium |
| No skip lists | Cannot skip non-matching document ranges | Slow intersection for multi-term queries | Medium |
| Linear scoring | `score_all()` computes scores for every document | Cannot early-terminate for top-k queries | Medium |

---

## Recommended Improvements

### 1. Add Inverted Posting Lists (Highest Priority)

**Problem**: The current forward index (document → terms) requires scanning all documents for every query. For a corpus with 100K documents, even a rare term that appears in only 10 documents requires evaluating all 100K documents.

**Solution**: Add an inverted index (term → documents) with posting lists.

```rust
/// Posting list entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Posting {
    pub doc_id: u32,       // Internal document ID (compact, not DocKey)
    pub term_freq: u32,    // Term frequency in this document
}

/// Posting list for a term
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostingList {
    /// Postings sorted by doc_id for efficient intersection
    pub postings: Vec<Posting>,
    /// Document frequency (number of docs containing this term)
    pub doc_freq: u32,
}

/// Extended index with posting lists
pub struct Bm25Index {
    // Existing fields...
    
    /// Inverted index: term_idx -> PostingList
    pub posting_lists: Vec<PostingList>,
    
    /// Document metadata: doc_id -> (DocKey, doc_length)
    pub doc_meta: Vec<(DocKey, u32)>,
    
    /// Reverse lookup: DocKey -> doc_id  
    pub doc_id_lookup: HashMap<DocKey, u32>,
}
```

**Query optimization**:

```rust
impl Bm25Scorer {
    /// Score using posting lists - only visits documents containing query terms
    pub fn score_with_postings(&self) -> Vec<(DocKey, f64)> {
        // Collect posting lists for query terms
        let posting_lists: Vec<&PostingList> = self.query_idfs
            .iter()
            .filter_map(|(term_idx, _)| self.index.posting_lists.get(*term_idx as usize))
            .collect();
        
        if posting_lists.is_empty() {
            return vec![];
        }
        
        // Use document-at-a-time (DAAT) scoring
        // Merge posting lists and score only documents that appear
        let mut results = Vec::new();
        let mut cursors: Vec<usize> = vec![0; posting_lists.len()];
        
        loop {
            // Find minimum doc_id across all cursors
            let min_doc_id = posting_lists.iter()
                .zip(&cursors)
                .filter_map(|(pl, &cursor)| pl.postings.get(cursor).map(|p| p.doc_id))
                .min();
            
            let Some(doc_id) = min_doc_id else { break };
            
            // Score this document
            let score = self.score_document_from_postings(doc_id, &posting_lists, &cursors);
            if score > 0.0 {
                let doc_key = &self.index.doc_meta[doc_id as usize].0;
                results.push((doc_key.clone(), score));
            }
            
            // Advance cursors that point to this doc_id
            for (i, pl) in posting_lists.iter().enumerate() {
                if pl.postings.get(cursors[i]).map(|p| p.doc_id) == Some(doc_id) {
                    cursors[i] += 1;
                }
            }
        }
        
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results
    }
}
```

**Expected improvement**: Query time becomes O(sum of posting list lengths) instead of O(num_docs). For selective queries, this is 10-1000x faster.

---

### 2. Variable-Length Integer Encoding

**Problem**: Posting lists and sparse vectors use fixed 4-byte integers, wasting space for small values (which are common due to delta encoding).

**Solution**: Use varint encoding for integers.

```rust
/// Encode u32 as varint (1-5 bytes, smaller values use fewer bytes)
pub fn encode_varint(mut n: u32, buf: &mut Vec<u8>) {
    while n >= 0x80 {
        buf.push((n as u8) | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
}

/// Decode varint from buffer
pub fn decode_varint(buf: &[u8], pos: &mut usize) -> u32 {
    let mut result = 0u32;
    let mut shift = 0;
    loop {
        let byte = buf[*pos];
        *pos += 1;
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    result
}

/// Compressed posting list with delta + varint encoding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressedPostingList {
    /// Delta-encoded, varint-compressed doc IDs
    pub doc_ids_compressed: Vec<u8>,
    /// Varint-compressed term frequencies
    pub term_freqs_compressed: Vec<u8>,
    /// Number of postings
    pub len: u32,
    /// Document frequency
    pub doc_freq: u32,
}

impl CompressedPostingList {
    pub fn encode(postings: &[Posting]) -> Self {
        let mut doc_ids = Vec::new();
        let mut term_freqs = Vec::new();
        let mut prev_doc_id = 0;
        
        for posting in postings {
            // Delta encode doc IDs (sorted, so deltas are small)
            encode_varint(posting.doc_id - prev_doc_id, &mut doc_ids);
            prev_doc_id = posting.doc_id;
            
            // Term frequencies are typically small (1-10)
            encode_varint(posting.term_freq, &mut term_freqs);
        }
        
        Self {
            doc_ids_compressed: doc_ids,
            term_freqs_compressed: term_freqs,
            len: postings.len() as u32,
            doc_freq: postings.len() as u32,
        }
    }
    
    pub fn decode(&self) -> Vec<Posting> {
        let mut postings = Vec::with_capacity(self.len as usize);
        let mut doc_pos = 0;
        let mut freq_pos = 0;
        let mut doc_id = 0;
        
        for _ in 0..self.len {
            let delta = decode_varint(&self.doc_ids_compressed, &mut doc_pos);
            doc_id += delta;
            let term_freq = decode_varint(&self.term_freqs_compressed, &mut freq_pos);
            postings.push(Posting { doc_id, term_freq });
        }
        
        postings
    }
}
```

**Expected improvement**: 40-60% reduction in posting list size.

---

### 3. FST-Based Term Dictionary

**Problem**: The term dictionary stores full strings with no prefix sharing. Terms like "http://schema.org/name" and "http://schema.org/description" share a long prefix that is stored redundantly.

**Solution**: Use a Finite State Transducer (FST) for the term dictionary. The `fst` crate provides an efficient implementation.

```rust
use fst::{Map, MapBuilder, Streamer};

/// FST-based term dictionary
pub struct FstTermDictionary {
    /// FST mapping term bytes -> term_idx
    fst: Map<Vec<u8>>,
    /// Term metadata: term_idx -> TermEntry
    entries: Vec<TermEntry>,
}

impl FstTermDictionary {
    /// Build from sorted terms
    pub fn build(terms: &BTreeMap<Arc<str>, TermEntry>) -> Result<Self, fst::Error> {
        let mut builder = MapBuilder::memory();
        let mut entries = Vec::with_capacity(terms.len());
        
        // FST requires terms in sorted order (BTreeMap guarantees this)
        for (term, entry) in terms {
            builder.insert(term.as_bytes(), entries.len() as u64)?;
            entries.push(entry.clone());
        }
        
        let fst_bytes = builder.into_inner()?;
        let fst = Map::new(fst_bytes)?;
        
        Ok(Self { fst, entries })
    }
    
    /// Look up a term
    pub fn get(&self, term: &str) -> Option<&TermEntry> {
        self.fst.get(term.as_bytes())
            .map(|idx| &self.entries[idx as usize])
    }
    
    /// Prefix search (useful for autocomplete)
    pub fn prefix_search(&self, prefix: &str) -> Vec<(&str, &TermEntry)> {
        // FST supports efficient prefix iteration
        let mut stream = self.fst.range().ge(prefix).lt(&format!("{}~", prefix)).into_stream();
        let mut results = Vec::new();
        while let Some((key, idx)) = stream.next() {
            if let Ok(term) = std::str::from_utf8(key) {
                results.push((term, &self.entries[idx as usize]));
            }
        }
        results
    }
}
```

**Dependencies to add**:
```toml
[dependencies]
fst = "0.4"
```

**Expected improvement**: 
- 3-5x reduction in term dictionary size
- Memory-mapped friendly (can query without loading entire dict)
- Enables prefix/wildcard queries

---

### 4. Segment-Based Time Travel Architecture

**Problem**: Each time travel snapshot stores the complete index, causing storage to grow linearly with the number of versions.

**Solution**: Use a segment-based architecture inspired by Lucene:

```rust
use roaring::RoaringBitmap;

/// A segment represents documents indexed in a version range
#[derive(Serialize, Deserialize)]
pub struct Bm25Segment {
    /// Segment identifier
    pub id: u64,
    
    /// Version range this segment covers (inclusive)
    pub from_t: i64,
    pub to_t: i64,
    
    /// Term dictionary for this segment
    pub terms: BTreeMap<Arc<str>, SegmentTermEntry>,
    
    /// Posting lists for this segment
    pub postings: Vec<CompressedPostingList>,
    
    /// Document metadata: local_doc_id -> (DocKey, doc_length)
    pub doc_meta: Vec<(DocKey, u32)>,
    
    /// Base document ID (for global doc_id calculation)
    pub base_doc_id: u32,
    
    /// Deleted documents bitmap
    /// Documents deleted AFTER this segment was created
    pub deleted: RoaringBitmap,
    
    /// Deletion version: doc_id -> t when deleted (for time travel)
    pub deletion_times: HashMap<u32, i64>,
}

#[derive(Serialize, Deserialize)]
pub struct SegmentTermEntry {
    pub local_idx: u32,      // Index within this segment's postings
    pub doc_freq: u32,       // DF within this segment
}

/// Multi-segment index with time travel support
pub struct Bm25Index {
    /// Immutable segments (sorted by from_t)
    pub segments: Vec<Bm25Segment>,
    
    /// Global corpus statistics
    pub stats: Bm25Stats,
    
    /// BM25 configuration
    pub config: Bm25Config,
    
    /// Watermarks
    pub watermark: VgWatermark,
    
    /// Property dependencies
    pub property_deps: PropertyDeps,
}

impl Bm25Index {
    /// Query at a specific time
    pub fn query_at(&self, query_terms: &[&str], as_of_t: i64, limit: usize) -> Vec<(DocKey, f64)> {
        // Only search segments that existed at as_of_t
        let relevant_segments: Vec<&Bm25Segment> = self.segments
            .iter()
            .filter(|s| s.from_t <= as_of_t)
            .collect();
        
        // Merge results from all relevant segments
        let mut all_results = Vec::new();
        
        for segment in relevant_segments {
            let segment_results = self.query_segment(segment, query_terms, as_of_t);
            all_results.extend(segment_results);
        }
        
        // Sort and limit
        all_results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        all_results.truncate(limit);
        all_results
    }
    
    fn query_segment(
        &self, 
        segment: &Bm25Segment, 
        query_terms: &[&str], 
        as_of_t: i64
    ) -> Vec<(DocKey, f64)> {
        let mut results = Vec::new();
        
        // Get posting lists for query terms in this segment
        let posting_lists: Vec<Option<&CompressedPostingList>> = query_terms
            .iter()
            .map(|term| {
                segment.terms.get(*term)
                    .map(|entry| &segment.postings[entry.local_idx as usize])
            })
            .collect();
        
        // Score documents using posting list intersection
        // Skip documents that were deleted before as_of_t
        for posting_list in posting_lists.into_iter().flatten() {
            for posting in posting_list.decode() {
                let global_doc_id = segment.base_doc_id + posting.doc_id;
                
                // Check if deleted before as_of_t
                if let Some(&deletion_t) = segment.deletion_times.get(&posting.doc_id) {
                    if deletion_t <= as_of_t {
                        continue; // Skip deleted document
                    }
                }
                
                // Score and add to results
                let (doc_key, doc_len) = &segment.doc_meta[posting.doc_id as usize];
                let score = self.compute_score(posting.term_freq, *doc_len);
                results.push((doc_key.clone(), score));
            }
        }
        
        results
    }
    
    /// Add new documents (creates or appends to current segment)
    pub fn add_documents(&mut self, docs: Vec<(DocKey, HashMap<String, u32>)>, t: i64) {
        // Create new segment or append to current
        // ...
    }
    
    /// Mark documents as deleted at time t
    pub fn delete_documents(&mut self, doc_keys: &[DocKey], t: i64) {
        for segment in &mut self.segments {
            for (local_id, (key, _)) in segment.doc_meta.iter().enumerate() {
                if doc_keys.contains(key) {
                    segment.deleted.insert(local_id as u32);
                    segment.deletion_times.insert(local_id as u32, t);
                }
            }
        }
    }
    
    /// Merge small segments (background maintenance)
    pub fn merge_segments(&mut self, segment_ids: &[u64]) -> Bm25Segment {
        // Combine multiple segments into one
        // Preserve deletion information for time travel
        // ...
        todo!()
    }
}
```

**Dependencies to add**:
```toml
[dependencies]
roaring = "0.10"
```

**Benefits**:
- Incremental updates append new segments (no full rewrite)
- Time travel queries filter by segment time range
- Deleted documents tracked with compact bitmaps
- Background segment merging maintains query performance
- Storage efficient: only changes are stored, not full copies

---

### 5. Block-Based Skip Lists

**Problem**: For large posting lists, sequential scanning is slow during intersection operations.

**Solution**: Organize posting lists into blocks with skip pointers.

```rust
/// Block size (Lucene uses 128)
const BLOCK_SIZE: usize = 128;

/// A block of postings with skip information
#[derive(Serialize, Deserialize)]
pub struct PostingBlock {
    /// Compressed postings in this block
    pub data: Vec<u8>,
    /// Number of postings in this block
    pub len: u32,
    /// Maximum doc_id in this block (for skipping)
    pub max_doc_id: u32,
    /// Sum of term frequencies in block (for scoring upper bounds)
    pub sum_tf: u32,
}

/// Posting list with skip structure
#[derive(Serialize, Deserialize)]
pub struct BlockPostingList {
    pub blocks: Vec<PostingBlock>,
    pub doc_freq: u32,
}

impl BlockPostingList {
    /// Skip to first block that might contain doc_id >= target
    pub fn skip_to(&self, target_doc_id: u32) -> Option<usize> {
        // Binary search on block max_doc_ids
        self.blocks
            .binary_search_by_key(&target_doc_id, |b| b.max_doc_id)
            .map_or_else(|i| if i < self.blocks.len() { Some(i) } else { None }, Some)
    }
}
```

**Expected improvement**: Log(n) skip time instead of linear scan during posting list intersection.

---

### 6. WAND Algorithm for Top-K Queries

**Problem**: The current `top_k()` implementation computes scores for ALL matching documents, then sorts and truncates.

**Solution**: Implement WAND (Weak AND) or Block-Max WAND for early termination.

```rust
impl Bm25Scorer {
    /// WAND algorithm for efficient top-k retrieval
    pub fn top_k_wand(&self, k: usize) -> Vec<(DocKey, f64)> {
        use std::collections::BinaryHeap;
        use std::cmp::Reverse;
        
        // Min-heap to track top-k (use Reverse for min-heap behavior)
        let mut heap: BinaryHeap<Reverse<(OrderedFloat<f64>, u32)>> = BinaryHeap::new();
        let mut threshold = 0.0;
        
        // Posting list cursors sorted by current doc_id
        let mut cursors: Vec<PostingCursor> = self.query_idfs
            .iter()
            .filter_map(|(term_idx, idf)| {
                self.index.get_posting_list(*term_idx)
                    .map(|pl| PostingCursor::new(pl, *idf))
            })
            .collect();
        
        // Pre-compute upper bounds for each term
        let term_upper_bounds: Vec<f64> = cursors
            .iter()
            .map(|c| self.compute_term_upper_bound(c.idf))
            .collect();
        
        while !cursors.is_empty() {
            // Sort cursors by current doc_id
            cursors.sort_by_key(|c| c.current_doc_id());
            
            // Find pivot: smallest index where cumulative upper bound > threshold
            let pivot = self.find_wand_pivot(&cursors, &term_upper_bounds, threshold);
            
            match pivot {
                Some(pivot_idx) => {
                    let pivot_doc_id = cursors[pivot_idx].current_doc_id();
                    
                    // Check if all cursors up to pivot point to same document
                    if cursors[0].current_doc_id() == pivot_doc_id {
                        // Full evaluation
                        let score = self.score_document_full(&cursors, pivot_doc_id);
                        
                        if score > threshold {
                            heap.push(Reverse((OrderedFloat(score), pivot_doc_id)));
                            if heap.len() > k {
                                heap.pop();
                                threshold = heap.peek()
                                    .map(|Reverse((s, _))| s.0)
                                    .unwrap_or(0.0);
                            }
                        }
                        
                        // Advance all cursors past pivot_doc_id
                        for cursor in &mut cursors {
                            cursor.advance_past(pivot_doc_id);
                        }
                    } else {
                        // Move first cursor to pivot document
                        cursors[0].skip_to(pivot_doc_id);
                    }
                }
                None => break, // No more candidates above threshold
            }
            
            // Remove exhausted cursors
            cursors.retain(|c| !c.is_exhausted());
        }
        
        // Convert heap to sorted results
        let mut results: Vec<(DocKey, f64)> = heap
            .into_iter()
            .map(|Reverse((score, doc_id))| {
                let doc_key = &self.index.doc_meta[doc_id as usize].0;
                (doc_key.clone(), score.0)
            })
            .collect();
        
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results
    }
    
    fn find_wand_pivot(
        &self,
        cursors: &[PostingCursor],
        upper_bounds: &[f64],
        threshold: f64,
    ) -> Option<usize> {
        let mut cumulative = 0.0;
        for (i, ub) in upper_bounds.iter().enumerate() {
            cumulative += ub;
            if cumulative > threshold {
                return Some(i);
            }
        }
        None
    }
}

struct PostingCursor<'a> {
    posting_list: &'a BlockPostingList,
    block_idx: usize,
    pos_in_block: usize,
    current_posting: Option<Posting>,
    idf: f64,
}
```

**Expected improvement**: 2-10x faster for top-k queries, especially when k is small relative to result set size.

---

## Implementation Roadmap

### Phase 1: Posting Lists (Recommended First)

1. Add `PostingList` structure alongside existing `doc_vectors`
2. Build posting lists during index construction
3. Add `score_with_postings()` method to scorer
4. Benchmark and validate correctness
5. Migrate query path to use posting lists

**Estimated effort**: 2-3 days
**Expected improvement**: 10-100x query speedup for selective queries

### Phase 2: Compression

1. Implement varint encoding utilities
2. Add `CompressedPostingList` with delta + varint encoding
3. Update serialization to use compressed format
4. Benchmark size reduction

**Estimated effort**: 1-2 days
**Expected improvement**: 40-60% reduction in index size

### Phase 3: FST Term Dictionary

1. Add `fst` crate dependency
2. Implement `FstTermDictionary` wrapper
3. Update index to use FST for term lookup
4. Add serialization support

**Estimated effort**: 1-2 days
**Expected improvement**: 3-5x reduction in term dictionary size

### Phase 4: Segment Architecture

1. Design segment schema and storage format
2. Implement segment creation and querying
3. Add deletion tracking with RoaringBitmap
4. Update time travel queries to use segments
5. Implement segment merging

**Estimated effort**: 5-7 days
**Expected improvement**: Efficient time travel, incremental updates

### Phase 5: Advanced Optimizations

1. Block-based posting lists with skip pointers
2. WAND algorithm for top-k
3. Block-Max WAND for even better pruning

**Estimated effort**: 3-5 days
**Expected improvement**: Additional 2-5x for top-k queries

---

## Dependencies to Add

```toml
[dependencies]
# FST for term dictionary compression
fst = "0.4"

# Roaring bitmaps for deletion tracking
roaring = "0.10"

# Ordered floats for heap operations (may already have this)
ordered-float = "4.0"
```

---

## Backwards Compatibility

The segment-based architecture should maintain backwards compatibility:

1. **Migration path**: Existing snapshots can be loaded and converted to single-segment indexes
2. **API stability**: Query interfaces remain the same
3. **Serialization**: Add version field to distinguish old vs new format

```rust
const SNAPSHOT_VERSION_LEGACY: u8 = 1;  // Current format
const SNAPSHOT_VERSION_SEGMENTS: u8 = 2; // New segment-based format

pub fn deserialize(data: &[u8]) -> Result<Bm25Index> {
    let version = data[4];
    match version {
        SNAPSHOT_VERSION_LEGACY => deserialize_legacy(data),
        SNAPSHOT_VERSION_SEGMENTS => deserialize_segments(data),
        _ => Err(SerializeError::InvalidFormat(...)),
    }
}
```

---

## Benchmarking Plan

Create benchmarks to measure:

1. **Query latency** by query selectivity (rare vs common terms)
2. **Index size** vs document count
3. **Time travel query latency** vs history depth
4. **Incremental update time** vs batch size
5. **Memory usage** during queries

```rust
#[bench]
fn bench_query_rare_term(b: &mut Bencher) {
    let index = load_test_index(); // 100K documents
    let scorer = Bm25Scorer::new(&index, &["xyzzy"]); // Rare term
    
    b.iter(|| {
        scorer.top_k(10)
    });
}

#[bench]
fn bench_query_common_term(b: &mut Bencher) {
    let index = load_test_index();
    let scorer = Bm25Scorer::new(&index, &["the"]); // Common term
    
    b.iter(|| {
        scorer.top_k(10)
    });
}
```

---

## References

- [Lucene's Index File Formats](https://lucene.apache.org/core/9_0_0/core/org/apache/lucene/codecs/lucene90/package-summary.html)
- [FST Crate Documentation](https://docs.rs/fst/latest/fst/)
- [WAND Algorithm Paper](https://www.sciencedirect.com/science/article/abs/pii/S0306437903000364)
- [Block-Max WAND](https://dl.acm.org/doi/10.1145/1988008.1988018)
- [Roaring Bitmaps](https://roaringbitmap.org/)
