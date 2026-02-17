# Vector scaling guide: Flat rank vs HNSW (and a storage layout proposal)

This document summarizes guidance for when customers should use **flat ranking** (brute-force similarity search) versus **HNSW** (approximate nearest neighbors), and proposes a storage layout that makes flat ranking fast for the dominant workload: **dense, normalized transformer embeddings** (e.g. SentenceTransformers, OpenAI embeddings).

## Executive summary (customer guidance)

- **If vectors per property are < ~1M**: flat ranking is typically viable for interactive queries on decent hardware, especially when vectors are cached/hot.
- **Between ~1M and ~10M vectors per property**: customers should **start considering HNSW** depending on:
  - **latency target** (e.g. 50–200ms vs 1–2s),
  - **hardware** (CPU + memory bandwidth, local NVMe vs object storage),
  - **cache hit rate** (warm vs cold),
  - **vector representation** (f32 vs f64; normalized vs not).
- **Above ~10M vectors per property**: flat ranking becomes increasingly I/O- and cache-bound for low latency; HNSW (or other ANN) becomes the default recommendation unless queries can tolerate seconds of latency or data is fully memory-resident.

This aligns with the desired guidance: **“consider HNSW between 1M and 10M vectors per property depending on hardware and latency requirements.”**

## Observed performance (baseline from recent benchmarks)

These numbers come from the recent SIMD + algorithm improvements:

- **Dot product @ 768 dims**: `1000 vectors in 186.6 µs` (NEON SIMD), i.e. ~**1.72×** faster than the older scalar path.
- **Cosine similarity @ 768 dims**: ~**2.30×** faster than the older three-pass implementation, primarily due to a **single-pass dot+norms** algorithm reducing memory traffic.

### Compute-only throughput estimate (hot vectors / in-memory)

From `1000 / 186.6 µs` at 768 dims:

- Approx dot-product throughput:  
  \[
  \frac{1000}{186.6\times10^{-6}} \approx 5.36\ \text{million dots/sec}
  \]
- In ~2 seconds (compute-only upper bound): **~10.7 million dot products**

This is a useful “best case” bound when vectors are already in RAM and the workload is compute-dominated.

## Practical throughput when vectors are “on disk”

Once vectors are not already in RAM, flat ranking is usually **I/O + memory-bandwidth dominated**. A quick back-of-the-envelope bound:

- Payload per vector:
  - **f64**: \(768 \times 8 = 6144\) bytes (~6 KB)
  - **f32** (typical embeddings): \(768 \times 4 = 3072\) bytes (~3 KB)

Vectors scanned in time \(T\) with sustained bandwidth \(B\) is roughly:

- **f64**: \(\text{vectors} \approx (T \cdot B)/6144\)
- **f32**: \(\text{vectors} \approx (T \cdot B)/3072\)

### Example: 2-second budget

| Sustained bandwidth | f64 vectors in 2s | f32 vectors in 2s |
|---:|---:|---:|
| 1 GB/s | ~0.33M | ~0.65M |
| 2 GB/s | ~0.65M | ~1.30M |
| 5 GB/s | ~1.63M | ~3.26M |
| 10 GB/s | ~3.26M | ~6.51M |

**Interpretation:**
- If vectors are **cold** and must be streamed, you hit the “consider HNSW” regime much earlier (often around **1–3M**) unless you can tolerate multi-second latencies.
- If vectors are **warm/hot** (OS page cache, memory cache) then you move closer to the **compute-only** bound (multi-million per second).

## Key optimization assumption: normalized transformer embeddings

For the dominant embedding workload (normalized vectors), cosine similarity simplifies:

- If vectors are unit-normalized (\(\|a\|=\|b\|=1\)), then:  
  \[
  \cos(a,b) = a\cdot b
  \]

**Implication:** for most customer workloads, “cosine search” can be executed as **dot product**, avoiding magnitude computation entirely. This is one of the highest-leverage optimizations for flat ranking.

## Proposal: per-property vector arena + sharding

Today, vectors in the binary index path are effectively stored via **string dictionaries** (vector serialized to a string, then referenced as `VECTOR_ID -> string_id`). That representation is not ideal for “scan all vectors for property `ex:embedding`”:

- it introduces extra decode work (string lookup + parse),
- it prevents efficient sequential reads of dense bytes,
- it creates poor access patterns for object storage.

### Goals

- **Object-store friendly (S3 Express One):**
  - minimize per-vector random reads,
  - prefer sequential/range GETs against a small set of objects,
  - shard so single-object fetches remain “reasonable” and cacheable.
- **Optimized for local disk:**
  - allow `mmap` and tight sequential scanning,
  - enable SIMD math on raw packed data with minimal allocation.
- **Group vectors by property** (e.g. “all `ex:embedding` vectors”) to support flat ranking efficiently.
- Provide **sharding per property** so very large properties don’t become single monolithic blobs.

### Suggested on-disk layout (conceptual)

Store vectors as a **binary arena** per property, sharded by size:

- Key idea: `VECTOR_ID`’s `ObjKey` points to a **vector handle** (not a string-id).
- Each property gets one or more shards (blobs), plus a manifest.

Example layout:

```
<ledger>/index/objects/vectors/
  p_<p_id>/
    manifest.json
    shard_00000.vec   (or .fvec / .bin)
    shard_00001.vec
    ...
```

Where:
- `manifest.json` records:
  - `dims` (e.g. 768),
  - `dtype` (`f32` strongly preferred),
  - `normalized: true|false` (so cosine can map to dot),
  - shard list + sizes + offsets/counts.
- Each shard stores vectors in fixed-size records when dims are fixed:
  - For 768-dim **f32**: record size = **3072 bytes**
  - For 768-dim **f64**: record size = **6144 bytes**

### Shard sizing (object store friendliness)

We want a shard large enough to make range reads efficient, but small enough to avoid huge cold downloads.

If the target is “~1–2 MB compressed” per object fetch:
- with **f32**, 1 MB uncompressed holds ~**341 vectors**; 2 MB holds ~**682 vectors**.
- with **f64**, 1 MB holds ~**170 vectors**; 2 MB holds ~**341 vectors**.

In practice, for S3 Express One we likely want **larger uncompressed shards** (tens to hundreds of MB) but fetch them in **1–2 MB ranges** (chunk cache), because range GETs are the main lever:

- **Shard**: a large contiguous object (good for local disk + fewer S3 objects)
- **Chunk**: fixed-size ranges within the shard (good for caching + bounded reads)

Recommended approach:
- shard size: ~256MB–1GB (operationally manageable)
- chunk size: ~1–4MB (cache unit; aligns with your “~1–2MB compressed” goal depending on compression)

### “Group by property” via sort order / index strategy

To support fast enumeration of all vectors for a given property:
- Ensure the index scan can efficiently produce rows for a predicate (`p_id`) with minimal extra work.
- Candidate access patterns:
  - POST-like scan (predicate → object → subject → t) is naturally predicate-first, but object ordering for vectors is not meaningful; we care about “all rows where p_id == embedding and o_kind == VECTOR_ID”.
  - A dedicated per-predicate vector manifest can also record the list of vector handles (or a packed list of `(s_id, handle)` pairs) for the property.

This doc doesn’t pick one final mechanism; the key is that **the property (`p_id`) should be a first-class partitioning key** so we don’t interleave unrelated vectors.

### Query-time execution model (flat rank)

For a query vector `q` and property `p`:

1. Resolve property to `p_id`
2. Enumerate candidate vectors for `p_id` (all, or filtered by other predicates first)
3. Fetch vector bytes from the arena in **chunked batches**
4. Compute:
   - normalized embeddings: **score = dot(q, v)**
   - non-normalized: cosine or L2 as requested
5. Maintain top-k via a heap (or selection algorithm)

This keeps the hot loop in “bytes → SIMD dot → top-k”.

## What we’re *not* optimizing for (and that’s ok)

The above is optimized for the 95% case: **dense, fixed-dimension, transformer-style embeddings**.

Other vector needs we can still support, but may not be as fast:

- **Variable dimensions per vector** (requires offset tables / variable record sizes)
- **Non-normalized vectors where cosine must compute norms** (still workable; just slower)
- **Very small vectors** (SIMD dispatch overhead can dominate; scalar is fine)
- **Sparse vectors / bag-of-words style** (better served by different data structures)
- **High-cardinality multi-vectors per subject** (may want a different mapping layer)
- **Exact f64 semantics end-to-end** (possible but higher storage + lower SIMD width on aarch64)

For these workloads (or for strict latency at high N), HNSW remains the recommended solution.

## Recommendation: simple customer-facing rule of thumb

When a customer asks “when do I need HNSW?”:

- If they want **sub-second** similarity search on cold-ish data: consider HNSW starting around **~1M vectors per property**.
- If they can tolerate **~1–2 seconds** and have good caching / local NVMe: flat rank can work into the **several million** range.
- If vectors are **fully memory-resident** and they accept a couple seconds: flat rank can reach into the **~10M** range (compute-only bound), hardware permitting.

In all cases: **normalized embeddings** push the crossover higher because cosine becomes dot.

