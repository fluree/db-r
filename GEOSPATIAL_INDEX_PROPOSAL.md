# Geospatial indexing: S2 cell-based spatial index with GeoSPARQL query support

This document proposes adding geospatial indexing to Fluree using Google's S2 geometry library as the primary spatial index structure, exposed through the OGC GeoSPARQL standard for SPARQL and an `idx:` virtual-graph pattern for JSON-LD queries.

The design follows the existing virtual-graph pattern used by BM25 and vector search: a sidecar index built from an indexing query, stored as a content-addressed snapshot, and queried via a `Pattern::GeoSearch` operator that emits candidate bindings into the standard query pipeline.

**Key design goals:**

1. **Time-travel native** — Spatial queries respect Fluree's `@t` semantics; "what was within this region at t=1000" works naturally
2. **Append-only compatible** — No rebalancing; cell IDs are immutable
3. **Scalable** — Supports both embedded indexes and remote specialized servers via nameservice RPC
4. **Standards-based** — Full GeoSPARQL 1.1 compliance for interoperability

---

## Background: why S2 cells

### The problem with R-trees in Fluree's architecture

Classical R-tree indexes (including R\*-tree) organize data by recursively partitioning the *objects* into bounding-box groups. This creates two fundamental problems for Fluree:

1. **Mutation requires rebalancing.** Inserting or deleting a geometry can trigger node splits that propagate upward, reshaping intermediate nodes. In an append-only, immutable storage model, this means rewriting large portions of the tree on every commit.

2. **Node shape depends on data.** An intermediate R-tree node's bounding box is a function of all the objects it contains. This non-local dependency makes it impossible to version the tree cheaply — you can't share structure between snapshots the way Fluree shares index leaves across commits.

CockroachDB encountered the same problem and chose S2 cells specifically because R-trees are "fundamentally incompatible" with their horizontally-scaled, LSM-tree-based architecture. The same reasoning applies to Fluree's append-only columnar indexes.

**Exception: ephemeral R-trees for spatial joins.** While persistent R-trees don't fit Fluree's model, *temporary* R-trees built at query time are extremely effective for spatial joins. When joining two geometry sets (e.g., "find all buildings within each park"), we can build an in-memory R-tree on the smaller side and probe it with the larger side — similar to how hash joins build a temporary hash table. The `geo-index` crate provides an immutable, packed R-tree stored as a single `Vec<u8>` that's perfect for this use case. See "Spatial joins" below.

### How S2 cells work

S2 projects Earth's surface onto the six faces of a cube, then recursively subdivides each face using a Hilbert curve into a hierarchy of **cells** at 31 levels (level 0 = hemisphere, level 30 ≈ 1 cm²).

Key properties:

- **Cell IDs are u64 integers.** Each cell has a unique 64-bit identifier. The Hilbert-curve ordering means spatially adjacent cells have numerically close IDs, providing excellent locality.

- **Cell IDs are totally ordered.** They sort lexicographically, meaning they can be stored in any sorted structure (B-tree, LSM-tree, Fluree's binary columnar index) and queried via range scans.

- **The hierarchy is predetermined.** Cell shapes don't depend on the data. Inserting a new geometry computes its covering cells from the fixed hierarchy — no rebalancing, no locking, no structural sharing concerns.

- **Coverings approximate any geometry.** A polygon or line is approximated by a set of cells at various levels. The S2 library provides a `RegionCoverer` that finds a minimal set of cells covering any region, with configurable precision (`max_cells`, `min_level`, `max_level`).

- **Containment is a cell-range query.** A cell `C` at level `L` contains all descendant cells whose IDs fall in the range `[C.range_min(), C.range_max()]`. This turns spatial containment into a set of integer range scans.

### S2 vs alternatives considered

| Property | S2 Cells | Geohash | H3 (hexagonal) | R-tree |
|----------|----------|---------|-----------------|--------|
| Append-only friendly | Yes — cell IDs are static | Yes | Yes | No — rebalancing |
| Cell shape quality | Good — low area distortion | Poor — rectangles at poles | Excellent — uniform hexagons | N/A |
| Hierarchy | Quadtree (31 levels) | Quadtree (12 levels) | Mixed pentagon/hexagon (16 levels) | Data-dependent |
| ID type | u64 (Hilbert-ordered) | String (base32) or u64 | u64 | N/A |
| Containment queries | Range scan on ID space | Prefix scan | Not directly hierarchical | Tree traversal |
| Rust ecosystem | `s2` crate | Manual encoding | `h3o` crate (good) | `rstar`, `geo-index` |
| Point-in-polygon precision | Exact (spherical geometry) | Approximate (planar) | Approximate (grid-based) | Exact |
| Edge/boundary handling | Exact great-circle edges | No — Z-curve discontinuities | Approximate | Exact |

**Decision: S2 cells for persistent indexing, ephemeral R-trees for spatial joins.** S2's u64 cell IDs, hierarchical containment via range scans, and append-only compatibility make it ideal for Fluree's persistent spatial index. For the specific case of spatial joins (variable vs variable), ephemeral packed R-trees built at query time provide superior performance.

---

## Design

### Architecture overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              Query layer                                      │
│                                                                              │
│  SPARQL: geof:sfWithin, geof:distance, ...                                   │
│  JSON-LD: idx:op "within", idx:geometry "POINT(...)", ...                    │
│                                                                              │
│         ┌──────────────────────────────────┐                                 │
│         │     GeoSearchOperator            │                                 │
│         │     (Pattern::GeoSearch)         │                                 │
│         └──────────────┬───────────────────┘                                 │
│                        │                                                     │
│         ┌──────────────▼───────────────────┐     ┌─────────────────────────┐ │
│         │   Local Spatial Index            │ OR  │  Remote Spatial Server  │ │
│         │   ┌────────────────────────────┐ │     │  (nameservice RPC)      │ │
│         │   │ Inline GeoPoint (POST)     │ │     │                         │ │
│         │   │ for point queries          │ │     │  idx:deployment:        │ │
│         │   ├────────────────────────────┤ │     │    endpoint: "..."      │ │
│         │   │ S2 Cell Index              │ │     │    protocol: grpc/http  │ │
│         │   │ (cell_id, subject_id, t)   │ │     └─────────────────────────┘ │
│         │   ├────────────────────────────┤ │                                 │
│         │   │ Geometry Arena             │ │                                 │
│         │   │ (handle → metadata + WKT)  │ │                                 │
│         │   └────────────────────────────┘ │                                 │
│         └──────────────┬───────────────────┘                                 │
│                        │                                                     │
│         ┌──────────────▼───────────────────┐                                 │
│         │   geo crate                      │                                 │
│         │   (exact spatial predicates)     │                                 │
│         └──────────────────────────────────┘                                 │
└──────────────────────────────────────────────────────────────────────────────┘
```

The geospatial index is a **virtual graph** — the same architectural pattern as BM25 and vector search. It is:

- Built from an **indexing query** that selects entities with geometry predicates
- Stored as a **content-addressed snapshot** in CAS storage
- Queried via a **`Pattern::GeoSearch`** operator in the query IR
- **Time-travel aware** — index entries carry transaction time (`t`) and operation (`op`)
- **Multi-ledger capable** — uses canonical IRIs with ledger aliases for cross-ledger spatial queries
- **Deployment-flexible** — can run embedded or delegate to a remote specialized server

### Data model: what gets stored

#### Inline GeoPoint encoding in the main index

For the common case of point geometries, Fluree encodes coordinates directly in the main flake index using a new `GeoPoint` ObjKind. This enables point proximity queries via standard POST range scans without consulting the sidecar spatial index.

**ObjKind encoding:**

```
0x0D GeoPoint

o_key layout (64 bits total, 60 used):
┌────────────────────────────────────┬────────────────────────────────────┐
│         latitude (30 bits)         │        longitude (30 bits)         │
│    scaled to [0, 2^30-1]           │    scaled to [0, 2^30-1]           │
└────────────────────────────────────┴────────────────────────────────────┘
     bits 59-30                            bits 29-0
```

**Coordinate encoding:**

```rust
impl GeoPoint {
    const LAT_BITS: u32 = 30;
    const LNG_BITS: u32 = 30;
    const MAX_ENCODED: u64 = (1 << 30) - 1;  // 1,073,741,823
    
    /// Encode lat/lng to 60-bit representation.
    /// Precision loss is ~0.3mm at equator (centimeter-scale).
    pub fn to_o_key(lat: f64, lng: f64) -> u64 {
        // Normalize to [0, 1] range
        let lat_norm = (lat + 90.0) / 180.0;   // [-90, 90] → [0, 1]
        let lng_norm = (lng + 180.0) / 360.0;  // [-180, 180] → [0, 1]
        
        // Scale to integer range
        let lat_encoded = (lat_norm * Self::MAX_ENCODED as f64).round() as u64;
        let lng_encoded = (lng_norm * Self::MAX_ENCODED as f64).round() as u64;
        
        // Pack: lat in upper 30 bits, lng in lower 30 bits
        (lat_encoded << Self::LNG_BITS) | lng_encoded
    }
    
    /// Decode 60-bit representation to lat/lng.
    pub fn from_o_key(o_key: u64) -> (f64, f64) {
        let lat_encoded = (o_key >> Self::LNG_BITS) & Self::MAX_ENCODED;
        let lng_encoded = o_key & Self::MAX_ENCODED;
        
        let lat = (lat_encoded as f64 / Self::MAX_ENCODED as f64) * 180.0 - 90.0;
        let lng = (lng_encoded as f64 / Self::MAX_ENCODED as f64) * 360.0 - 180.0;
        
        (lat, lng)
    }
}
```

**Why this encoding (not S2 cell IDs):**

While S2 cell IDs provide Hilbert-curve spatial locality, they require converting to/from lat/lng for display and exact distance calculations. Storing lat/lng directly:

- Enables direct coordinate extraction for query results
- Supports exact haversine distance computation without S2 library calls
- Still enables proximity queries via bounding-box range scans on (lat, lng)
- Precision (~0.3mm) exceeds requirements for virtually all applications

**Proximity queries via POST:**

Because GeoPoint values sort by latitude (upper 30 bits), a proximity query "find points within R meters of (lat, lng)" becomes:

```
1. Compute lat/lng bounds:
   lat_min, lat_max = lat ± (R / 111_320)  // ~111km per degree
   lng_min, lng_max = lng ± (R / (111_320 * cos(lat)))

2. Execute POST range scan:
   p = geo:location, o_kind = GeoPoint, o_key IN [encode(lat_min, lng_min), encode(lat_max, lng_max)]
   
3. Post-filter: exact haversine distance check
```

This leverages Fluree's existing binary scan infrastructure for point-in-region queries without touching the sidecar spatial index.

**Latitude-band false positives:**

The concatenated `(lat << 30) | lng` encoding creates a **latitude-primary sort order**. A single POST range scan from `encode(lat_min, lng_min)` to `encode(lat_max, lng_max)` is effectively a latitude-band scan — it returns all points in the latitude range, regardless of longitude.

```
Scan range visualization (lat on Y, lng on X):

  lat_max ─────┬─────────────────────────────┬─────
               │  ████ ACTUAL QUERY BOX ████ │
               │  ████                  ████ │
  lat_min ─────┴─────────────────────────────┴─────
               lng_min                    lng_max
               
  What the single range scan returns:
  
  lat_max ═════╪═════════════════════════════╪═════
               ║▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓║  ← Entire latitude band
               ║▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓║    (all longitudes)
  lat_min ═════╪═════════════════════════════╪═════
          -180°                            +180°
```

Points with latitude between `lat_min` and `lat_max` but with longitude outside `[lng_min, lng_max]` are included in the scan results. The haversine post-filter (Step 3) correctly excludes these false positives.

**False positive rates:**

| Scenario | False positive rate | Notes |
|----------|---------------------|-------|
| Equator (lat ≈ 0°) | ~22% | Circular query inscribed in rectangular band |
| Mid-latitudes (lat ≈ 45°) | ~35% | Longitude span grows by 1/cos(45°) ≈ 1.4× |
| High latitudes (lat ≈ 70°) | ~55% | Longitude span grows by 1/cos(70°) ≈ 2.9× |
| Polar regions (lat > 80°) | ~70%+ | Longitude nearly meaningless |

For v1, we **accept this false positive rate**. The rationale:
- Haversine distance is cheap (~50ns per point)
- The latitude-band filter is still a massive reduction over a full table scan
- Most real-world data is concentrated in mid-latitudes (populated areas)
- High-latitude queries are rare in typical workloads

**Alternative encodings (future consideration):**

Two alternatives could reduce false positives at the cost of complexity:

1. **Z-order (Morton) encoding:** Interleaves bits as `lat[29] lng[29] lat[28] lng[28] ...`. This provides better 2D locality — a bounding-box query becomes a set of contiguous Z-ranges. False positive rates drop to ~5-10% for typical queries. Tradeoff: more complex encoding/decoding, multiple range scans per query.

   ```
   Concatenated: lat lat lat ... lng lng lng (1D locality)
   Z-order:      lat lng lat lng lat lng ... (2D locality)
   ```

2. **Dual-scan strategy:** Split the POST scan into multiple narrower latitude sub-bands, each with tighter longitude bounds. Trades more index seeks for fewer false positives.

   ```
   Instead of: scan [lat_min, lat_max] × [-180, 180]
   Do:         scan [lat_min, lat_min+δ] × [lng_min, lng_max]
               scan [lat_min+δ, lat_min+2δ] × [lng_min, lng_max]
               ...
   ```

3. **S2 cell ID encoding:** Store `S2CellId::from_lat_lng().id()` instead of packed lat/lng. S2's Hilbert curve provides excellent 2D locality with single-range queries. Tradeoff: requires S2 library for encoding/decoding; cannot directly extract lat/lng without conversion.

For v1, the simple concatenated encoding with haversine post-filter is recommended. Z-order encoding is the most promising future optimization if false positive rates become problematic for high-latitude use cases.

**Antimeridian handling (180°/-180° boundary):**

A proximity query near the international date line requires special handling. The GeoPoint encoding maps longitude continuously from -180° → 0 to +180° → MAX_ENCODED, so a query that crosses the antimeridian needs **two POST range scans**:

```
Query: "Find points within 500m of POINT(179.99, -17.8)" (Fiji)

lng_delta = 500 / (111_320 * cos(-17.8°)) ≈ 0.0047°
lng_min = 179.99 - 0.0047 = 179.9853°  (valid)
lng_max = 179.99 + 0.0047 = 179.9947°  (valid, but what if we need to go past 180°?)

If lng_max > 180°:
  lng_max_wrapped = lng_max - 360 = -179.9953°
  
  Scan 1: lat_band × [lng_min, +180°]      → eastern hemisphere portion
  Scan 2: lat_band × [-180°, lng_max_wrapped] → western hemisphere portion
```

The bounding box computation should detect antimeridian crossing:

```rust
fn compute_proximity_scans(center: (f64, f64), radius_m: f64) -> Vec<(u64, u64)> {
    let (lat, lng) = center;
    let lat_delta = radius_m / 111_320.0;
    let lng_delta = radius_m / (111_320.0 * lat.to_radians().cos());
    
    let lat_min = (lat - lat_delta).max(-90.0);
    let lat_max = (lat + lat_delta).min(90.0);
    let lng_min = lng - lng_delta;
    let lng_max = lng + lng_delta;
    
    if lng_min < -180.0 {
        // Crosses antimeridian westward
        vec![
            (encode(lat_min, -180.0), encode(lat_max, lng_max)),        // main portion
            (encode(lat_min, lng_min + 360.0), encode(lat_max, 180.0)), // wrapped portion
        ]
    } else if lng_max > 180.0 {
        // Crosses antimeridian eastward
        vec![
            (encode(lat_min, lng_min), encode(lat_max, 180.0)),         // main portion
            (encode(lat_min, -180.0), encode(lat_max, lng_max - 360.0)), // wrapped portion
        ]
    } else {
        // Normal case: single scan
        vec![(encode(lat_min, lng_min), encode(lat_max, lng_max))]
    }
}
```

**Pole handling:**

At very high latitudes (|lat| > 80°), the longitude span for a given radius grows dramatically because `1 / cos(lat)` approaches infinity as lat → ±90°:

| Latitude | 1 / cos(lat) | 1km query lng span |
|----------|--------------|-------------------|
| 0° (equator) | 1.0 | ±0.009° |
| 45° | 1.4 | ±0.013° |
| 70° | 2.9 | ±0.026° |
| 80° | 5.8 | ±0.052° |
| 85° | 11.5 | ±0.103° |
| 89° | 57.3 | ±0.515° |

At 89° latitude, a 1km proximity query spans ~1° of longitude — still manageable. But at 89.9°, the span is ~5°, and the latitude-band scan captures a significant fraction of all points at that latitude.

**This is not a correctness issue** — the haversine post-filter always produces correct results. It's a performance consideration:

- For typical workloads (data concentrated at |lat| < 70°), pole effects are negligible
- For polar data (Arctic/Antarctic research, polar shipping), the false positive rate increases but queries remain correct
- If profiling shows polar queries are slow, consider:
  1. Using the S2 cell index instead of inline GeoPoint for polar regions
  2. Implementing latitude-aware scan splitting
  3. Capping the longitude span at ±180° and accepting full-latitude scans near poles

#### POINT detection at ingestion time

When processing a `geo:wktLiteral` value during transaction, Fluree must decide whether to store it as:

- **`ObjKind::GeoPoint`** — inline encoding with direct coordinate access
- **`ObjKind::LexId`** — string dictionary entry for full WKT

This decision happens in the **transaction processor** (performance-critical path) and uses a **lightweight prefix check**, not a full WKT parser:

```rust
/// Detect POINT geometry and extract coordinates without full WKT parsing.
/// Returns Some((lat, lng)) for valid POINT literals, None otherwise.
fn try_extract_point(wkt: &str) -> Option<(f64, f64)> {
    let wkt = wkt.trim();
    
    // Fast prefix check — no regex needed
    if !wkt.starts_with("POINT") {
        return None;
    }
    
    // Find parentheses
    let start = wkt.find('(')?;
    let end = wkt.rfind(')')?;
    if start >= end {
        return None;
    }
    
    // Extract coordinate string: "lng lat" (WKT order is lng, lat)
    let coords = wkt[start + 1..end].trim();
    let mut parts = coords.split_whitespace();
    
    let lng: f64 = parts.next()?.parse().ok()?;
    let lat: f64 = parts.next()?.parse().ok()?;
    
    // Validate ranges
    if lat < -90.0 || lat > 90.0 || lng < -180.0 || lng > 180.0 {
        return None;
    }
    
    // Reject if there are extra coordinates (POINT Z, POINT M, etc.)
    if parts.next().is_some() {
        return None;  // Fall back to LexId for 3D/4D points
    }
    
    Some((lat, lng))
}

/// Transaction processor integration
fn process_wkt_literal(value: &str) -> ObjValue {
    match try_extract_point(value) {
        Some((lat, lng)) => ObjValue::GeoPoint(GeoPoint::new(lat, lng)),
        None => ObjValue::LexId(intern_string(value)),
    }
}
```

**Performance characteristics:**

| Operation | Cost |
|-----------|------|
| Prefix check (`starts_with("POINT")`) | O(5) — constant time |
| Parenthesis scan | O(n) worst case, but early exit for non-POINT |
| Float parsing (2×) | ~50-100ns per coordinate |
| Full WKT parse (avoided) | ~1-10µs depending on complexity |

For a transaction inserting 10,000 point geometries, this saves ~10-100ms compared to full WKT parsing.

**What triggers LexId storage (no inline GeoPoint):**

- Non-point geometry types: `POLYGON(...)`, `LINESTRING(...)`, `MULTIPOINT(...)`, etc.
- 3D/4D points: `POINT Z(...)`, `POINT M(...)`, `POINT ZM(...)`
- Malformed WKT (parsing fails gracefully)
- Empty points: `POINT EMPTY`

These values are stored as `LexId` (string dictionary entries) and parsed fully only during **spatial index building**, not at transaction time.

#### Complex geometry storage

Geometries other than points (polygons, linestrings, multi-geometries) are stored as WKT or GeoJSON string literals in the main flake index, using `geo:asWKT` or `geo:asGeoJSON` predicates with datatype `geo:wktLiteral` or `geo:geoJSONLiteral`. In terms of the ObjKind encoding, these are `LexId` values — string dictionary entries.

The sidecar spatial index provides the search capability for these complex geometries.

#### Avoiding redundant indexing: GeoPoint exclusion from S2 cell index

When a predicate stores point geometries as inline `GeoPoint` ObjKind, those points should **not** also appear in the S2 cell index. This avoids double indexing with no benefit.

**Index builder behavior:**

```rust
fn should_index_in_cell_index(geometry: &geo::Geometry, config: &SpatialConfig) -> bool {
    match geometry {
        // Points are handled by inline GeoPoint in the main index
        Geometry::Point(_) => false,
        
        // All other geometries go into the S2 cell index
        _ => true,
    }
}
```

During spatial index building, the builder:
1. Parses each WKT/GeoJSON literal
2. Detects `POINT(...)` geometries
3. Skips cell entry generation for points
4. Only generates cell entries for polygons, linestrings, and multi-geometries

**Query planner routing:**

The query planner selects the appropriate index based on the query pattern:

| Query pattern | Index used | Rationale |
|---------------|------------|-----------|
| `geof:distance(?point, constant) < R` | Inline GeoPoint (POST) | Point proximity with constant center |
| `geof:sfWithin(?geom, constant_polygon)` | S2 cell index | Region containment |
| `geof:sfIntersects(?geom1, ?geom2)` | Ephemeral R-tree | Spatial join (variable × variable) |
| `geof:sfContains(?polygon, ?point)` | S2 cell index + GeoPoint | Polygon lookup, then point filter |

**Mixed geometry predicates:**

If a predicate contains both points and polygons (e.g., `geo:hasGeometry` on a heterogeneous dataset):

- Points are stored as inline `GeoPoint` ObjKind
- Polygons/linestrings are stored as `LexId` (WKT strings)
- The S2 cell index only contains entries for the non-point geometries
- The query planner may need to union results from both indexes for certain queries

**Configuration option:**

For rare cases where you want points in the S2 cell index (e.g., if you need to disable inline GeoPoint for compatibility), add a configuration flag:

```rust
SpatialCreateConfig::new(...)
    .with_index_points_in_cell_index(true)  // default: false
```

This flag should almost never be needed. The default behavior (points via GeoPoint, complex geometries via S2) provides optimal query performance with minimal index overhead.

#### Spatial index structure

The spatial index is a sorted collection of *cell entries*:

```
CellEntry {
    cell_id:    u64,       // S2 cell ID (Hilbert-ordered)
    subject_id: u64,       // Canonical subject identifier (SID or IRI hash)
    geo_handle: u32,       // Handle into geometry arena
    t:          i64,       // Transaction time
    op:         bool,      // true = assert, false = retract
}
```

Entries are sorted by `(cell_id, subject_id, t DESC)`. This ordering enables:

- **Containment queries**: range scan over `[cell.range_min(), cell.range_max()]` for each covering cell
- **Subject dedup**: entries for the same subject are adjacent within a cell range
- **Temporal filtering**: newest-first `t` ordering enables efficient point-in-time resolution

Each geometry produces **multiple cell entries** (one per covering cell). A point typically produces 1 cell entry at the maximum level. A polygon produces `max_cells` entries (default: 8) at various levels.

#### Geometry arena with precomputed metadata

The geometry arena stores the full geometry, its precomputed S2 covering, and **optional geometry metadata** for each indexed entity. The metadata enables cheap pre-filtering before expensive exact spatial tests.

```
GeoArenaEntry {
    handle:     u32,               // Sequential handle (0, 1, 2, ...)
    iri:        Arc<str>,          // Canonical IRI of the feature
    ledger:     Arc<str>,          // Source ledger alias
    wkt:        String,            // Original WKT literal
    covering:   Vec<S2CellId>,     // Precomputed S2 covering
    
    // Precomputed metadata (optional, see geometry_metadata feature)
    metadata:   Option<GeometryMetadata>,
}

/// Precomputed geometry properties for fast filtering.
/// Computed once at index build time; avoids WKT parsing during queries.
struct GeometryMetadata {
    /// Bounding box as (min_lat, min_lng, max_lat, max_lng)
    /// Encoded as 4 × f32 = 16 bytes (sufficient precision for filtering)
    bounding_box: [f32; 4],
    
    /// Centroid as (lat, lng)
    /// Useful for distance approximations and visualization
    centroid: (f32, f32),
    
    /// Geometry type (Point=1, LineString=2, Polygon=3, MultiPoint=4, etc.)
    geometry_type: u8,
    
    /// Area in square meters (for polygons/multipolygons)
    /// None for non-area geometries (points, linestrings)
    area_m2: Option<f64>,
    
    /// Length in meters (for linestrings, polygon perimeters)
    /// None for points
    length_m: Option<f64>,
    
    /// Number of child geometries (for multi-* and geometry collections)
    num_geometries: u32,
}
```

**Why geometry metadata matters:**

1. **Bounding box pre-filtering:** Before computing exact S2 coverings or spatial predicates, a cheap axis-aligned bounding box (AABB) intersection test can reject most non-matching geometries. For a "find polygons intersecting region R" query:
   - AABB test: ~5 nanoseconds (4 float comparisons)
   - S2 covering intersection: ~500 nanoseconds  
   - Exact polygon intersection: ~5,000+ nanoseconds
   
   Rejecting 90% of candidates via AABB provides ~10x speedup.

2. **Query planning:** The geometry type enables smarter query plans:
   - Point-only indexes can skip S2 coverings entirely
   - Polygon-heavy indexes benefit from tighter `max_cells` settings
   
3. **Result enrichment:** Area, length, and centroid can be returned without re-parsing WKT:
   ```sparql
   SELECT ?park ?name (geof:area(?geom) AS ?area)
   WHERE { ?park a ex:Park ; geo:hasGeometry ?geom }
   ```

4. **Spatial join optimization:** When joining two geometry sets, knowing the bounding boxes enables efficient join-order selection and early termination.

**Metadata computation:**

```rust
impl GeometryMetadata {
    /// Compute metadata from a parsed geometry.
    /// Uses spherical (WGS84) calculations for area/length.
    pub fn from_geometry(geom: &geo::Geometry) -> Self {
        use geo::{BoundingRect, Centroid, GeodesicArea, GeodesicLength};
        
        let bbox = geom.bounding_rect().unwrap_or_default();
        let centroid = geom.centroid().unwrap_or(geo::Point::new(0.0, 0.0));
        
        // Area only meaningful for polygons and multipolygons
        let area_m2 = match geom {
            Geometry::Polygon(_) | Geometry::MultiPolygon(_) => {
                Some(geom.geodesic_area_unsigned())
            }
            _ => None,
        };
        
        // Length meaningful for everything except points
        let length_m = match geom {
            Geometry::Point(_) | Geometry::MultiPoint(_) => None,
            _ => Some(geom.geodesic_length()),
        };
        
        Self {
            bounding_box: [
                bbox.min().y as f32,  // min_lat
                bbox.min().x as f32,  // min_lng
                bbox.max().y as f32,  // max_lat
                bbox.max().x as f32,  // max_lng
            ],
            centroid: (centroid.y() as f32, centroid.x() as f32),
            geometry_type: geometry_type_code(geom),
            area_m2,
            length_m,
            num_geometries: count_geometries(geom),
        }
    }
}
```

The arena provides:

- **Forward lookup**: `handle → GeoArenaEntry` for exact spatial tests during query post-filtering
- **IRI dedup**: `(ledger, iri) → handle` for incremental updates
- **Multi-ledger support**: same pattern as BM25/vector — canonical IRIs with ledger provenance
- **Fast pre-filtering**: bounding box checks before expensive exact tests

### S2 covering computation

When a geometry is ingested, its S2 covering is computed:

```rust
fn compute_covering(geometry: &geo::Geometry, config: &SpatialConfig) -> Vec<S2CellId> {
    let coverer = RegionCoverer::new()
        .min_level(config.s2_min_level)    // default: 4  (~80km cells)
        .max_level(config.s2_max_level)    // default: 23 (~1m cells)
        .max_cells(config.s2_max_cells);   // default: 8

    match geometry {
        Geometry::Point(p) => {
            // Points get a single cell at max_level
            vec![S2CellId::from_lat_lng(p.y(), p.x()).parent(config.s2_max_level)]
        }
        _ => {
            // Polygons, lines, etc. get a multi-cell covering
            let region = geometry_to_s2_region(geometry);
            coverer.covering(&region).into()
        }
    }
}
```

**Covering configuration trade-offs:**

| Setting | Tighter covering | Looser covering |
|---------|-----------------|-----------------|
| `max_cells` | Higher (16–32) | Lower (4–8) |
| Effect on index size | More cell entries per geometry | Fewer cell entries |
| Effect on query | Fewer false positives | More false positives |
| Best for | Dense urban data, complex polygons | Sparse point data, large regions |

The default `max_cells: 8` is a practical starting point. CockroachDB uses a similar default and reports that augmenting cell coverings with bounding-box checks reduces false positives by ~3x for problematic geometries.

### Query execution: how spatial queries work

#### Point proximity queries (inline GeoPoint path)

For the common case of "find points within distance D of location L", the query can be answered directly from the POST index without the sidecar spatial index:

```
Query: "Find sensors within 500m of POINT(2.3522 48.8566)"

Step 1: Compute bounding box
  lat_delta = 500 / 111_320 = 0.00449°
  lng_delta = 500 / (111_320 * cos(48.8566°)) = 0.00683°
  
  Box: [48.852, 2.345] to [48.861, 2.359]

Step 2: Encode bounds for POST scan
  o_key_min = GeoPoint::to_o_key(48.852, 2.345)
  o_key_max = GeoPoint::to_o_key(48.861, 2.359)

Step 3: POST range scan
  p = ex:location, o_kind = GeoPoint, o_key IN [o_key_min, o_key_max]
  
Step 4: Post-filter with exact haversine distance
  For each candidate (s_id, o_key):
    (lat, lng) = GeoPoint::from_o_key(o_key)
    dist = haversine_distance((lat, lng), (48.8566, 2.3522))
    if dist <= 500: emit result

Step 5: Apply time-travel filter (if @t specified)
  Filter by t <= query.to_t using standard Region 1/Region 3 logic
```

This path:
- Requires no sidecar index
- Uses existing binary scan infrastructure
- Supports full time-travel semantics
- Works for any point-valued predicate (not just `geo:asWKT`)

#### Complex geometry queries (S2 cell index path)

For polygon containment, intersection, and complex geometry queries:

**Step 1: Bounding box pre-filter (if metadata available)**

```rust
// Quick rejection using geometry metadata
let query_bbox = query_geom.bounding_rect();
let candidates: Vec<_> = arena.entries()
    .filter(|e| e.metadata.as_ref()
        .map(|m| bbox_intersects(&m.bounding_box, &query_bbox))
        .unwrap_or(true))  // Include if no metadata
    .collect();
```

**Step 2: Compute query covering**

The query geometry (e.g., a search polygon) is converted to S2 cells:

```
Query: "Find all features within POLYGON((2.2 48.8, 2.4 48.8, 2.4 48.9, 2.2 48.9, 2.2 48.8))"

  → S2 covering: [cell_A (level 12), cell_B (level 14), cell_C (level 14)]
```

**Step 3: Generate cell range scans**

For each covering cell, compute the ID range that includes all descendant cells:

```
cell_A (level 12): scan range [cell_A.range_min(), cell_A.range_max()]
cell_B (level 14): scan range [cell_B.range_min(), cell_B.range_max()]
cell_C (level 14): scan range [cell_C.range_min(), cell_C.range_max()]
```

These range scans are executed against the sorted cell index. Each scan returns candidate `(subject_id, geo_handle)` pairs.

**Step 4: Temporal filtering**

For time-travel queries, candidates are filtered by transaction time:

```
For each candidate (cell_id, subject_id, t, op):
    if t > query.to_t:
        skip (future transaction)
    if op == retract:
        skip subject_id (geometry was deleted by this time)
    if op == assert && t <= query.to_t:
        include as candidate
```

This uses the same `t`-bounded resolution logic as the main index. Because entries are sorted by `t DESC` within `(cell_id, subject_id)`, the first entry at or below `to_t` determines the subject's status.

**Step 5: Exact geometric test (post-filter)**

Candidates from the cell range scans are *approximate* — the S2 covering is a superset of the actual geometry. Each candidate is post-filtered using exact geometric predicates from the `geo` crate:

```rust
for candidate in cell_scan_results {
    let entry = arena.get(candidate.geo_handle);
    
    // Optional: secondary bounding box check
    if let Some(ref meta) = entry.metadata {
        if !bbox_intersects(&meta.bounding_box, &query_bbox) {
            continue;  // Cheap rejection
        }
    }
    
    let passes = match query.spatial_op {
        SpatialOp::Within     => query_geom.contains(&entry.geometry),
        SpatialOp::Contains   => entry.geometry.contains(&query_geom),
        SpatialOp::Intersects => entry.geometry.intersects(&query_geom),
        SpatialOp::Distance(max_d) => {
            let d = entry.geometry.haversine_distance(&query_geom);
            d <= max_d
        }
        // ... other operations
    };
    if passes {
        emit Binding::Sid(entry.subject_id)
        // or Binding::IriMatch(entry.iri, entry.ledger) for multi-ledger
    }
}
```

**Step 6: Emit bindings into query pipeline**

The `GeoSearchOperator` produces `Batch` results in the same format as `Bm25SearchOperator` and `VectorSearchOperator`:

```rust
pub struct GeoSearchOperator {
    config: GeoSearchConfig,
    spatial_index: Arc<SpatialIndex>,
    state: OperatorState,
}

#[async_trait]
impl Operator for GeoSearchOperator {
    async fn next(&mut self) -> Result<Option<Batch>> {
        // 1. Bounding box pre-filter (if metadata enabled)
        // 2. Compute query covering (once, on first call)
        // 3. Execute cell range scans
        // 4. Temporal filter
        // 5. Exact geometric test
        // 6. Emit Binding::Sid or Binding::IriMatch
        // 7. Optionally bind distance to a variable
    }
}
```

The emitted bindings join with subsequent patterns in the query via the subject variable, exactly as BM25 and vector results do.

### Spatial joins

When both arguments to a spatial predicate are variables (e.g., `geof:sfIntersects(?parkWkt, ?bldgWkt)`), this becomes a **spatial join** — potentially the most expensive operation in geospatial querying.

**The ephemeral R-tree strategy:**

Rather than attempting to use the S2 cell index for spatial joins (which produces many false positives for complex polygons), we build a temporary R-tree at query time:

```rust
/// Execute a spatial join between two geometry sets.
/// Builds an ephemeral R-tree on the smaller side, probes with the larger side.
pub async fn spatial_join(
    left: &[GeoArenaEntry],
    right: &[GeoArenaEntry],
    predicate: SpatialPredicate,
    ctx: &ExecutionContext,
) -> Result<Vec<(u32, u32)>> {  // (left_handle, right_handle) pairs
    
    // 1. Determine build side (smaller set)
    let (build_side, probe_side, swapped) = if left.len() <= right.len() {
        (left, right, false)
    } else {
        (right, left, true)
    };
    
    // 2. Build packed R-tree on build side bounding boxes
    //    Uses geo-index crate: single Vec<u8>, immutable, ~2x faster than rstar
    let rtree = geo_index::rtree::RTreeBuilder::new(build_side.len())
        .with_node_size(16)  // Tuned for spatial join workloads
        .build(build_side.iter().map(|e| {
            let bb = get_bounding_box(e);
            geo_index::rtree::Rect::new(bb[1], bb[0], bb[3], bb[2])  // lng, lat order
        }));
    
    // 3. Probe R-tree with each geometry from probe side
    let mut results = Vec::new();
    for (probe_idx, probe_entry) in probe_side.iter().enumerate() {
        ctx.check_cancelled()?;
        
        let probe_bb = get_bounding_box(probe_entry);
        let probe_rect = geo_index::rtree::Rect::new(
            probe_bb[1], probe_bb[0], probe_bb[3], probe_bb[2]
        );
        
        // R-tree returns candidate indices whose bounding boxes intersect
        for build_idx in rtree.search(&probe_rect) {
            let build_entry = &build_side[build_idx];
            
            // Exact predicate check
            let passes = match predicate {
                SpatialPredicate::Intersects => 
                    probe_entry.geometry.intersects(&build_entry.geometry),
                SpatialPredicate::Contains =>
                    probe_entry.geometry.contains(&build_entry.geometry),
                SpatialPredicate::Within =>
                    build_entry.geometry.contains(&probe_entry.geometry),
                // ... other predicates
            };
            
            if passes {
                let pair = if swapped {
                    (build_idx as u32, probe_idx as u32)
                } else {
                    (probe_idx as u32, build_idx as u32)
                };
                results.push(pair);
            }
        }
    }
    
    Ok(results)
}

/// Extract bounding box from arena entry.
/// Uses precomputed metadata if available; otherwise computes from geometry.
fn get_bounding_box(entry: &GeoArenaEntry) -> [f32; 4] {
    use geo::BoundingRect;
    
    match &entry.metadata {
        Some(m) => m.bounding_box,
        None => {
            // Fallback: compute bbox from the parsed geometry
            // This is slower but handles the metadata_enabled=false case
            let rect = entry.geometry.bounding_rect()
                .expect("geometry should have a bounding rect");
            [
                rect.min().y as f32,  // min_lat
                rect.min().x as f32,  // min_lng
                rect.max().y as f32,  // max_lat
                rect.max().x as f32,  // max_lng
            ]
        }
    }
}
```

**Note on metadata dependency:** When `metadata_enabled: false`, spatial joins still work but incur additional overhead computing bounding boxes on the fly. For join-heavy workloads, keeping metadata enabled (the default) is strongly recommended.

**Performance characteristics:**

| Dataset size | Nested loop | Ephemeral R-tree |
|--------------|-------------|------------------|
| 1K × 1K      | ~1 second   | ~10 ms           |
| 10K × 10K    | ~100 seconds| ~200 ms          |
| 100K × 100K  | ~3 hours    | ~5 seconds       |

The R-tree construction is O(n log n) but with excellent constants due to the packed format. The join itself is O(n × k) where k is the average number of R-tree candidates per probe geometry — typically much smaller than n.

**Time-travel for spatial joins:**

Both sides of the join are resolved to point-in-time before building the R-tree. The temporal filtering happens during candidate materialization, not during the join itself.

### Time travel

The spatial index supports time travel natively because every cell entry carries `(t, op)`:

**Point-in-time query (`@t:NNN`):** The `GeoSearchOperator` receives `to_t` from the `ExecutionContext`. During temporal filtering, it filters cell entries to only those at or before `to_t`, applying retraction logic. A geometry that was added at `t=5` and removed at `t=10` will appear in spatial queries at `@t:7` but not at `@t:12`.

**Historical spatial queries:** For history-mode queries (`from_t` + `to_t`), the operator can emit entries with their `t` values, enabling "show me all geometries that were within this region between t=5 and t=50."

**Index cumulativity:** Like the main binary index, the spatial index is cumulative — a single snapshot contains all historical cell entries up to its watermark `t`. No intermediate snapshots are needed for time travel. The cell index is rebuilt from scratch during re-indexing (not incrementally patched), matching the BM25/vector pattern.

**Incremental updates:** Between full re-indexes, new commits produce novelty cell entries that are merged with the snapshot at query time. Property dependencies (extracted from the indexing query) determine which commits affect the spatial index.

**Novelty merge strategy:**

The spatial index uses a **sorted novelty buffer + merge-at-read** approach, matching the main index's novelty overlay pattern:

```rust
struct SpatialIndexSnapshot {
    /// Immutable sorted array from last full rebuild
    /// Content-addressed, loaded via mmap
    cell_index: Arc<[CellEntry]>,
    
    /// Sorted buffer of entries added since last rebuild
    /// Kept small; triggers rebuild when exceeds threshold
    novelty: Vec<CellEntry>,
    
    /// Watermark: snapshot covers t <= snapshot_t
    snapshot_t: i64,
    
    /// Novelty covers snapshot_t < t <= novelty_t  
    novelty_t: i64,
}

impl SpatialIndexSnapshot {
    /// Execute a cell range scan, merging snapshot and novelty.
    fn cell_range_scan(&self, range: Range<u64>, to_t: i64) -> impl Iterator<Item = &CellEntry> {
        // Binary search both structures for range start
        let snapshot_iter = self.cell_index
            .binary_search_range(range.clone())
            .filter(|e| e.t <= to_t);
            
        let novelty_iter = self.novelty
            .binary_search_range(range)
            .filter(|e| e.t <= to_t);
        
        // Merge two sorted iterators
        // Entries with same (cell_id, subject_id) resolved by t DESC
        MergeSortedIter::new(snapshot_iter, novelty_iter)
    }
    
    /// Add new cell entries from a commit.
    fn apply_novelty(&mut self, entries: Vec<CellEntry>) {
        // Insert-sort into novelty buffer (small, so fast)
        for entry in entries {
            let pos = self.novelty.binary_search(&entry).unwrap_or_else(|p| p);
            self.novelty.insert(pos, entry);
        }
        self.novelty_t = entries.iter().map(|e| e.t).max().unwrap_or(self.novelty_t);
    }
}
```

**Why this approach:**

1. **Cell range scans work on both structures.** The S2-based query (compute covering → range scan) applies identically to the snapshot array and the novelty buffer. No need to change the query algorithm.

2. **Novelty stays small.** Between rebuilds, novelty typically contains entries from a few commits. Binary search on a small sorted vec is fast.

3. **Matches the main index pattern.** Developers familiar with the Region 1 + novelty overlay pattern will recognize this design.

4. **Rebuild is straightforward.** When novelty exceeds a threshold (e.g., 10K entries or 10% of snapshot size), trigger a full rebuild that merges everything into a new immutable snapshot.

**Alternative considered (append-only novelty):** Appending entries unsorted and scanning linearly would simplify writes but make queries O(n) in novelty size. Since spatial queries can be frequent and novelty can grow between rebuilds, the sorted approach is preferred.

**Inline GeoPoint time travel:** For point queries using the inline `GeoPoint` ObjKind, time travel works exactly like any other flake type — Region 1 contains current state, Region 3 contains the history journal. No special handling required.

### Persistence and storage

Following the BM25/vector virtual-graph pattern:

```
Storage path:
  virtual-graphs/{name}/{branch}/spatial/t{index_t}/

Artifacts:
  cell_index.bin      — Sorted cell entries (cell_id, subject_id, geo_handle, t, op)
  geometry_arena.bin  — Geometry arena (handle → metadata + WKT + covering)
  config.json         — Index configuration snapshot
  manifest.json       — Root metadata (watermark, entry count, covering stats)
```

All artifacts are content-addressed (SHA-256) and uploaded to CAS storage. The manifest records the addresses of all artifacts, enabling atomic snapshot publication.

**Cell index format:**

The cell index is a flat sorted array of fixed-width entries, enabling binary search and range scans without tree overhead:

```
Entry layout (32 bytes):
┌──────────┬────────────┬────────────┬──────┬────┬──────────┐
│ cell_id  │ subject_id │ geo_handle │  t   │ op │ reserved │
│  u64     │    u64     │    u32     │ i64  │ u8 │   3B     │
└──────────┴────────────┴────────────┴──────┴────┴──────────┘
```

32-byte alignment enables efficient memory-mapped access. The flat array format is similar in spirit to `geo-index`'s packed R-tree — fully immutable, zero-copy, excellent cache locality.

**Geometry arena format (with metadata):**

Variable-length entries, accessed by handle:

```
Arena header:
  version: u8              — Format version (currently 2)
  flags: u8                — Bit 0: metadata_enabled
  entry_count: u32
  offset_table: [u32; entry_count]  — byte offset of each entry

Per entry:
  iri_len: u16
  iri: [u8; iri_len]
  ledger_len: u16
  ledger: [u8; ledger_len]
  wkt_len: u32
  wkt: [u8; wkt_len]
  covering_count: u8
  covering: [u64; covering_count]   — S2 cell IDs
  
  # If metadata_enabled:
  bounding_box: [f32; 4]            — min_lat, min_lng, max_lat, max_lng
  centroid: [f32; 2]                — lat, lng
  geometry_type: u8
  area_m2: f64                      — NaN encodes None
  length_m: f64                     — NaN encodes None
  num_geometries: u32
```

**Option<f64> serialization:**

The binary format uses NaN as a sentinel value for `None`, keeping fixed-width fields for efficient mmap access:

```rust
fn write_optional_f64(w: &mut impl Write, opt: Option<f64>) -> io::Result<()> {
    let val = opt.unwrap_or(f64::NAN);
    w.write_all(&val.to_le_bytes())
}

fn read_optional_f64(r: &mut impl Read) -> io::Result<Option<f64>> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    let val = f64::from_le_bytes(buf);
    Ok(if val.is_nan() { None } else { Some(val) })
}
```

This approach keeps the binary format simple (no tag bytes, fixed offsets) while the Rust API uses idiomatic `Option<f64>`.

The parsed `geo::Geometry` is reconstructed from WKT at load time (not persisted). This keeps the arena format simple and avoids coupling to internal geometry representations.

### Remote spatial index server

For high-scale deployments or specialized spatial workloads, the spatial index can be delegated to a remote server. This follows the same pattern as BM25 and vector search remote providers.

**Configuration via nameservice:**

```json
{
  "@id": "ns:spatial-index/buildings",
  "@type": "ns:SpatialVirtualGraph",
  "ns:deployment": {
    "@type": "ns:RemoteDeployment",
    "ns:endpoint": "grpc://spatial-server.example.com:9090",
    "ns:protocol": "grpc",
    "ns:authMethod": "mtls",
    "ns:healthCheck": "/health"
  },
  "ns:sourceGraph": "mydb:main",
  "ns:indexingQuery": { ... },
  "ns:spatialConfig": {
    "ns:s2MaxLevel": 23,
    "ns:s2MaxCells": 12,
    "ns:metadataEnabled": true
  }
}
```

**RPC protocol:**

The remote spatial server implements a simple query interface:

```protobuf
service SpatialIndex {
  // Execute a spatial query (within, contains, intersects, nearby)
  rpc Query(SpatialQueryRequest) returns (stream SpatialQueryResult);
  
  // Execute a spatial join between two geometry sets
  rpc SpatialJoin(SpatialJoinRequest) returns (stream SpatialJoinResult);
  
  // Get index status and statistics
  rpc GetStatus(StatusRequest) returns (IndexStatus);
}

message SpatialQueryRequest {
  string index_name = 1;
  SpatialOp op = 2;
  bytes query_geometry_wkt = 3;
  optional int64 to_t = 4;           // Time-travel: point-in-time
  optional int64 from_t = 5;         // Time-travel: history mode
  optional uint32 limit = 6;
  optional double max_distance_m = 7; // For nearby queries
}

message SpatialQueryResult {
  uint64 subject_id = 1;
  string iri = 2;
  optional double distance_m = 3;
  int64 t = 4;                        // Transaction time (for history mode)
}
```

**Use cases for remote deployment:**

1. **Massive scale:** Indexes with 100M+ geometries that don't fit in memory
2. **Specialized hardware:** GPU-accelerated spatial operations
3. **Multi-tenant isolation:** Dedicated spatial servers per tenant
4. **Hot/cold separation:** Frequently-queried spatial data on fast storage

The `GeoSearchOperator` abstracts over local vs. remote execution — the query planner doesn't need to know where the index lives.

---

## User configuration

### Creating a spatial index

Following the BM25/vector virtual-graph pattern, a spatial index is created by specifying an indexing query and spatial configuration:

```rust
pub struct SpatialCreateConfig {
    pub name: String,                    // VG name (e.g., "spatial")
    pub branch: Option<String>,          // Branch (defaults to "main")
    pub ledger: String,                  // Source ledger alias
    pub query: JsonValue,                // Indexing query
    pub geometry_property: String,       // Property path to geometry literal
    pub srid: Option<u32>,               // Spatial reference system (default: 4326 / WGS84)
    pub s2_min_level: Option<u8>,        // Min covering level (default: 4)
    pub s2_max_level: Option<u8>,        // Max covering level (default: 23)
    pub s2_max_cells: Option<u8>,        // Max cells per covering (default: 8)
    pub metadata_enabled: Option<bool>,  // Precompute geometry metadata (default: true)
    pub deployment: Option<SearchDeploymentConfig>,
}
```

**Builder pattern:**

```rust
let query = json!({
    "@context": {
        "geo": "http://www.opengis.net/ont/geosparql#",
        "ex": "http://example.org/"
    },
    "where": [{"@id": "?x", "@type": "ex:Building"}],
    "select": {"?x": ["@id", "geo:asWKT"]}
});

let cfg = SpatialCreateConfig::new("spatial", "mydb:main", query, "geo:asWKT")
    .with_s2_max_level(23)
    .with_s2_max_cells(12)
    .with_metadata_enabled(true);

let created = fluree.create_spatial_index(cfg).await?;
```

**Remote deployment:**

```rust
let cfg = SpatialCreateConfig::new("spatial", "mydb:main", query, "geo:asWKT")
    .with_deployment(SearchDeploymentConfig::Remote {
        endpoint: "grpc://spatial.example.com:9090".into(),
        protocol: Protocol::Grpc,
    });
```

### Schema: representing geospatial data

Data follows the GeoSPARQL ontology. Features have geometries; geometries have serializations.

**Inserting a point feature (uses inline GeoPoint):**

```json
{
  "@context": {
    "geo": "http://www.opengis.net/ont/geosparql#",
    "ex": "http://example.org/"
  },
  "@id": "ex:sensor-1",
  "@type": ["ex:Sensor", "geo:Feature"],
  "ex:name": "Temperature Sensor #1",
  "geo:hasGeometry": {
    "@id": "ex:sensor-1-geom",
    "@type": "geo:Point",
    "geo:asWKT": {
      "@value": "POINT(2.2945 48.8584)",
      "@type": "geo:wktLiteral"
    }
  }
}
```

When the geometry is a `POINT(...)`, Fluree automatically stores it using the inline `GeoPoint` ObjKind, enabling efficient proximity queries.

**Inserting a polygon feature:**

```json
{
  "@id": "ex:park-1",
  "@type": ["ex:Park", "geo:Feature"],
  "ex:name": "Luxembourg Gardens",
  "geo:hasGeometry": {
    "@id": "ex:park-1-geom",
    "@type": "geo:Geometry",
    "geo:asWKT": {
      "@value": "POLYGON((2.3325 48.8462, 2.3395 48.8462, 2.3395 48.8498, 2.3325 48.8498, 2.3325 48.8462))",
      "@type": "geo:wktLiteral"
    }
  }
}
```

Polygons and other complex geometries are stored as WKT strings and indexed via the S2 cell index.

**Simplified form (geometry directly on feature):**

For the common case where a feature has exactly one geometry, a shorthand that places the WKT literal directly on the feature can be supported by the indexing query:

```json
{
  "@id": "ex:sensor-1",
  "@type": "ex:Sensor",
  "ex:location": {
    "@value": "POINT(-73.9857 40.7484)",
    "@type": "geo:wktLiteral"
  }
}
```

With indexing query:

```json
{
  "where": [{"@id": "?x", "@type": "ex:Sensor"}],
  "select": {"?x": ["@id", "ex:location"]}
}
```

The indexing query determines which property holds the geometry. The spatial index builder extracts and parses the WKT/GeoJSON value from whatever property path is configured.

### Covering configuration guidelines

| Use case | `s2_max_level` | `s2_max_cells` | Rationale |
|----------|---------------|----------------|-----------|
| City-scale points (buildings, sensors) | 23 (~1m) | 8 | High precision, points only need 1 cell |
| Country-scale polygons (counties, parks) | 18 (~100m) | 12 | Medium precision, moderate coverings |
| Global points (cities, airports) | 15 (~1km) | 4 | Low precision sufficient, minimal index |
| Mixed (points + polygons in same index) | 23 | 8 | Default — works for most cases |

### Coordinate reference system handling

**Default: WGS84 (EPSG:4326)**

All coordinates are assumed to be WGS84 (latitude/longitude) unless otherwise specified. This matches GeoSPARQL's default CRS.

**Non-WGS84 data:**

If source data uses a different coordinate reference system (e.g., UTM, State Plane), it should be transformed to WGS84 before ingestion. Fluree does not perform automatic CRS transformation.

Future versions may support:
- CRS metadata on geometry literals (`geo:coordinateReferenceSystem`)
- On-the-fly transformation via the `proj` crate
- Per-index CRS configuration

---

## Query syntax

### SPARQL — GeoSPARQL standard

Fluree implements the function-based query pattern from the OGC GeoSPARQL 1.1 standard. Spatial operations appear as `FILTER` expressions using `geof:` functions.

**Namespace prefixes:**

```sparql
PREFIX geo:  <http://www.opengis.net/ont/geosparql#>
PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
PREFIX uom:  <http://www.opengis.net/def/uom/OGC/1.0/>
```

#### Proximity query (distance)

```sparql
SELECT ?building ?name ?dist
WHERE {
  ?building a ex:Building ;
            rdfs:label ?name ;
            geo:hasGeometry/geo:asWKT ?wkt .
  BIND(geof:distance(?wkt, "POINT(2.3522 48.8566)"^^geo:wktLiteral, uom:metre) AS ?dist)
  FILTER(?dist < 1000)
}
ORDER BY ?dist
LIMIT 10
```

**Query plan:** The query planner recognizes the `geof:distance(...) < constant` pattern and rewrites it into a `Pattern::GeoSearch` with `SpatialOp::Nearby { center, radius_m }`. For point geometries with inline `GeoPoint` encoding, this becomes a POST range scan. For complex geometries, it uses the S2 cell index.

#### Region containment (sfWithin)

```sparql
SELECT ?sensor ?reading
WHERE {
  ?sensor a ex:Sensor ;
          ex:lastReading ?reading ;
          geo:hasGeometry/geo:asWKT ?loc .
  FILTER(geof:sfWithin(?loc,
    "POLYGON((2.2 48.8, 2.4 48.8, 2.4 48.9, 2.2 48.9, 2.2 48.8))"^^geo:wktLiteral))
}
```

**Query plan:** The `geof:sfWithin(?var, constant_geom)` pattern is rewritten to `Pattern::GeoSearch` with `SpatialOp::Within`. The constant geometry is converted to an S2 covering and used for index-accelerated range scans.

#### Spatial join (sfIntersects)

```sparql
SELECT ?park ?building
WHERE {
  ?park a ex:Park ;
        geo:hasGeometry/geo:asWKT ?parkWkt .
  ?building a ex:Building ;
            geo:hasGeometry/geo:asWKT ?bldgWkt .
  FILTER(geof:sfIntersects(?parkWkt, ?bldgWkt))
}
```

**Query plan:** When both arguments are variables (spatial join), the planner materializes one side (e.g., parks) and builds an ephemeral R-tree. The other side (buildings) probes the R-tree for candidates, which are then verified with exact predicate tests.

#### Time-travel + geospatial

```sparql
# Fluree extension: point-in-time geospatial query
# Using the @t:1000 query option (passed via query parameters, not inline SPARQL)

SELECT ?building ?name
WHERE {
  ?building a ex:Building ;
            rdfs:label ?name ;
            geo:hasGeometry/geo:asWKT ?wkt .
  FILTER(geof:sfWithin(?wkt,
    "POLYGON((...) )"^^geo:wktLiteral))
}
```

The `to_t` from the query context propagates into the `GeoSearchOperator`, which filters cell entries temporally.

#### Functions: implementation priority

**P0 — core spatial queries (initial release):**

| Function | Signature | Description |
|----------|-----------|-------------|
| `geof:sfWithin` | `(geom1, geom2) → bool` | geom1 is spatially within geom2 |
| `geof:sfContains` | `(geom1, geom2) → bool` | geom1 spatially contains geom2 |
| `geof:sfIntersects` | `(geom1, geom2) → bool` | geometries share any space |
| `geof:distance` | `(geom1, geom2, unit) → double` | shortest distance between geometries |
| `geof:sfDisjoint` | `(geom1, geom2) → bool` | no spatial overlap (complement of intersects) |

**P1 — common operations (second release):**

| Function | Signature | Description |
|----------|-----------|-------------|
| `geof:sfTouches` | `(geom1, geom2) → bool` | boundary-only contact |
| `geof:buffer` | `(geom, radius, unit) → geom` | expand geometry by distance |
| `geof:envelope` | `(geom) → geom` | bounding box |
| `geof:sfEquals` | `(geom1, geom2) → bool` | spatially equal (same point set) |

**P2 — advanced operations (future):**

| Function | Signature | Description |
|----------|-----------|-------------|
| `geof:sfCrosses` | `(geom1, geom2) → bool` | line/polygon crossing |
| `geof:sfOverlaps` | `(geom1, geom2) → bool` | partial overlap (same dimension) |
| `geof:convexHull` | `(geom) → geom` | convex hull computation |
| `geof:union` | `(geom1, geom2) → geom` | geometry union |
| `geof:intersection` | `(geom1, geom2) → geom` | geometry intersection |
| `geof:area` | `(geom, unit) → double` | area measurement |

### JSON-LD query syntax

Geospatial queries in JSON-LD follow the `idx:` virtual-graph pattern established by BM25 and vector search.

#### Proximity search (nearby)

```json
{
  "@context": {
    "geo": "http://www.opengis.net/ont/geosparql#",
    "idx": "https://ns.flur.ee/index#",
    "ex": "http://example.org/"
  },
  "select": ["?building", "?name", "?dist"],
  "where": [
    {
      "idx:graph": "spatial:main",
      "idx:op": "nearby",
      "idx:geometry": "POINT(2.3522 48.8566)",
      "idx:radius": 1000,
      "idx:unit": "metre",
      "idx:limit": 10,
      "idx:result": {
        "idx:id": "?building",
        "idx:distance": "?dist"
      }
    },
    {"@id": "?building", "rdfs:label": "?name"}
  ],
  "orderBy": "?dist"
}
```

#### Region containment (within)

```json
{
  "select": ["?sensor", "?reading"],
  "where": [
    {
      "idx:graph": "spatial:main",
      "idx:op": "within",
      "idx:geometry": "POLYGON((2.2 48.8, 2.4 48.8, 2.4 48.9, 2.2 48.9, 2.2 48.8))",
      "idx:result": "?sensor"
    },
    {"@id": "?sensor", "ex:lastReading": "?reading"}
  ]
}
```

#### Intersection with variable geometry

```json
{
  "select": ["?park", "?building"],
  "where": [
    {"@id": "?park", "@type": "ex:Park"},
    {"@id": "?park", "geo:hasGeometry": "?parkGeom"},
    {"@id": "?parkGeom", "geo:asWKT": "?parkWkt"},
    {
      "idx:graph": "spatial:main",
      "idx:op": "intersects",
      "idx:geometry": "?parkWkt",
      "idx:result": "?building"
    }
  ]
}
```

#### Using filter syntax for GeoSPARQL functions

For filtering already-bound variables (not index-accelerated):

```json
{
  "select": ["?building", "?name"],
  "where": [
    {"@id": "?building", "@type": "ex:Building", "schema:name": "?name"},
    {"@id": "?building", "geo:hasGeometry": "?geom"},
    {"@id": "?geom", "geo:asWKT": "?wkt"},
    ["filter", ["geof:sfWithin", "?wkt",
      "POLYGON((2.2 48.8, 2.4 48.8, 2.4 48.9, 2.2 48.9, 2.2 48.8))"]]
  ]
}
```

#### Time-travel + geospatial

```json
{
  "from": ["mydb:main@t:1000"],
  "select": ["?building", "?name"],
  "where": [
    {
      "idx:graph": "spatial:main",
      "idx:op": "within",
      "idx:geometry": "POLYGON((2.2 48.8, 2.4 48.8, 2.4 48.9, 2.2 48.9, 2.2 48.8))",
      "idx:result": "?building"
    },
    {"@id": "?building", "rdfs:label": "?name"}
  ]
}
```

#### JSON-LD index search options

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `idx:graph` | string | yes | Virtual graph alias (e.g., `"spatial:main"`) |
| `idx:op` | string | yes | `"nearby"`, `"within"`, `"contains"`, `"intersects"`, `"disjoint"` |
| `idx:geometry` | string or var | yes | WKT literal or bound variable |
| `idx:radius` | number | for `nearby` | Search radius |
| `idx:unit` | string | for `nearby`/`distance` | `"metre"`, `"kilometre"`, `"mile"` |
| `idx:limit` | number | no | Max results |
| `idx:result` | var or object | yes | Variable, or `{idx:id, idx:distance, idx:ledger}` |
| `idx:sync` | boolean | no | Force index sync before query (default: false) |
| `idx:timeout` | number | no | Query timeout in ms |

---

## Implementation plan

### Crate structure

```
fluree-db-spatial/ (new crate)
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── config.rs          — SpatialConfig, SpatialCreateConfig
│   ├── geopoint.rs        — Inline GeoPoint ObjKind encoding/decoding
│   ├── covering.rs        — S2 covering computation
│   ├── cell_index.rs      — Sorted cell index (build, query, serialize)
│   ├── geometry_arena.rs  — Geometry arena (WKT parsing, metadata, handle management)
│   ├── metadata.rs        — GeometryMetadata computation (bbox, centroid, area)
│   ├── spatial_index.rs   — Top-level SpatialIndex (combines cell index + arena)
│   ├── builder.rs         — SpatialIndexBuilder (parallel to Bm25IndexBuilder)
│   ├── snapshot.rs        — Persistence (read/write snapshot to CAS)
│   ├── operator.rs        — GeoSearchOperator (query execution)
│   ├── spatial_op.rs      — SpatialOp enum (Within, Contains, Nearby, etc.)
│   ├── spatial_join.rs    — Ephemeral R-tree spatial join implementation
│   └── remote/
│       ├── mod.rs
│       ├── client.rs      — gRPC/HTTP client for remote spatial server
│       └── proto.rs       — Protocol buffer definitions
```

### Changes to existing crates

**`fluree-db-core`:**

- Add `ObjKind::GeoPoint = 0x0D` to the encoding schedule
- Implement `GeoPoint::to_o_key()` and `GeoPoint::from_o_key()` in `value_id.rs`
- Add `geo:wktLiteral` datatype recognition in transaction processing
- Implement `try_extract_point()` for lightweight POINT detection (prefix check + coordinate extraction, no full WKT parsing)
- Route detected points to `ObjKind::GeoPoint`, all other WKT to `ObjKind::LexId`

**`fluree-db-query`:**

- Add `Pattern::GeoSearch` variant to pattern IR
- Add `SpatialIndexProvider` trait (parallel to `Bm25IndexProvider`, `VectorIndexProvider`)
- Add `geo_search` field to `ExecutionContext`
- Wire `GeoSearchOperator` into operator construction
- Add POST range scan optimization for `GeoPoint` proximity queries

**`fluree-db-sparql`:**

- Recognize `geof:` function namespace in expression parser
- Add lowering rules:
  - `FILTER(geof:sfWithin(?var, constant))` → `Pattern::GeoSearch { op: Within, geometry: constant, result: ?var }`
  - `BIND(geof:distance(?var, constant, unit) AS ?dist)` + `FILTER(?dist < N)` → `Pattern::GeoSearch { op: Nearby, ... }`
  - Fallback: unrecognized `geof:*` functions lower to filter expressions evaluated against the `geo` crate

**`fluree-db-query/src/parse/node_map.rs`:**

- Recognize `idx:op` as the discriminator for spatial index search (parallel to `idx:target` for BM25 and `idx:vector` for vector)
- Parse `idx:geometry`, `idx:radius`, `idx:unit`, `idx:op` fields
- Produce `Pattern::GeoSearch` in the IR

**`fluree-db-api/src/virtual_graph/`:**

- Add `spatial.rs` module (parallel to `bm25.rs`, `vector.rs`)
- Implement `create_spatial_index`, `rebuild_spatial_index`
- Register spatial VG type in virtual-graph dispatch

### Rust dependencies

```toml
[dependencies]
s2 = "0.0.12"              # S2 cell computation, coverings
geo = "0.28"               # Geometry types, spatial predicates (contains, intersects, distance)
geo-types = "0.7"          # Core geometry type definitions
wkt = "0.10"               # WKT parsing
geojson = "0.24"           # GeoJSON parsing (optional)
geo-index = "0.2"          # Packed immutable R-tree for spatial joins
```

---

## Tradeoffs and decisions

### Why inline GeoPoint in addition to the virtual graph

**Decision: both inline GeoPoint AND virtual graph.**

The inline `GeoPoint` ObjKind and the S2 cell-based virtual graph serve complementary purposes:

| Aspect | Inline GeoPoint | S2 Virtual Graph |
|--------|-----------------|------------------|
| Geometry types | Points only | All (polygons, lines, multi-*) |
| Storage | Main flake index | Sidecar index |
| Time travel | Native (Region 1/3) | Native (t, op in cell entries) |
| Query path | POST range scan | Cell index range scan |
| Index overhead | Zero (part of main index) | Additional storage |
| Best for | Point proximity, k-NN | Region containment, complex predicates |

Most geospatial workloads are point-heavy (sensors, buildings, events). Inline GeoPoint handles these with zero additional index overhead. Complex geometries (polygons, linestrings) use the virtual graph.

### Why concatenated lat/lng encoding (not Z-order or S2)

**Decision: concatenated encoding `(lat << 30) | lng` for v1.**

Three encoding options were considered for the inline GeoPoint ObjKind:

| Encoding | Sort order | 2D locality | False positives | Complexity |
|----------|------------|-------------|-----------------|------------|
| **Concatenated** | Latitude-primary | Poor | 22-70% (varies by latitude) | Simple |
| **Z-order (Morton)** | Interleaved | Good | 5-10% | Medium |
| **S2 cell ID** | Hilbert curve | Excellent | ~1% | High (requires S2 lib) |

**Why concatenated wins for v1:**

1. **Simple encoding/decoding.** Two shifts and two masks. No lookup tables, no library dependencies.

2. **Direct coordinate access.** Extracting lat/lng for display or distance computation is trivial. Z-order requires bit-deinterleaving; S2 requires library calls.

3. **Acceptable false positive rate.** For typical mid-latitude queries (where most data lives), ~35% false positives means scanning 1.35× the necessary rows. With haversine post-filtering at ~50ns/point, this overhead is negligible compared to I/O.

4. **Worst case is rare.** High-latitude queries (>70°) are uncommon in typical workloads. Polar regions have minimal population density and infrastructure.

**When to reconsider:**

- If a significant use case emerges for high-latitude data (Arctic shipping, polar research)
- If profiling shows haversine post-filtering is a bottleneck
- If query patterns favor very large radius searches where the latitude band includes millions of out-of-range points

**Future migration path:**

Adding Z-order encoding would be a minor change — same 60-bit payload, different bit arrangement. The ObjKind discriminator could distinguish `GeoPointConcat` (0x0D) from `GeoPointZOrder` (0x0E), allowing gradual migration.

### Why points are excluded from the S2 cell index

**Decision: the S2 cell index only contains non-point geometries.**

When inline GeoPoint is enabled (the default), point geometries are:
- Stored in the main flake index as `ObjKind::GeoPoint`
- Queryable via POST range scans
- **NOT** added to the S2 cell index

This avoids redundant indexing. Without this rule:
- A dataset with 10M points would have 10M entries in both the main index AND the S2 cell index
- Point proximity queries would need to choose between two equivalent paths
- Index storage doubles for no benefit

**Query planner implications:**

The planner must route queries appropriately:

| If the query involves... | Use... |
|--------------------------|--------|
| Point proximity (constant center) | Inline GeoPoint (POST scan) |
| Region containment (polygon search region) | S2 cell index |
| Spatial join (variable × variable) | Ephemeral R-tree |
| Mixed (e.g., "polygons containing point X") | S2 cell index for polygon lookup, then exact test |

**Edge case: points in both indexes:**

The `index_points_in_cell_index: true` config flag exists for rare cases where you want points in the S2 cell index (e.g., if a remote spatial server doesn't have access to the main index). This should almost never be needed.

### Why geometry metadata is optional

**Decision: metadata enabled by default, but configurable.**

Computing bounding boxes, centroids, and area at index time adds ~10% to index build time. For point-only indexes where this metadata isn't useful, it can be disabled:

```rust
SpatialCreateConfig::new(...).with_metadata_enabled(false)
```

### Why ephemeral R-tree for spatial joins (not S2)

**Decision: build R-tree at query time for spatial joins.**

S2 cell coverings produce too many false positives for spatial joins between complex geometries. A polygon's covering might overlap with many other coverings that don't actually intersect.

The ephemeral R-tree approach:
- Builds a packed R-tree on the smaller side (~100ms for 100K geometries)
- Uses bounding box intersection for candidate generation
- Verifies candidates with exact predicates

This matches how hash joins work — build a temporary structure on the smaller side, probe with the larger side. The packed R-tree from `geo-index` is ideal: immutable, cache-friendly, and builds 2x faster than `rstar`.

### Why a virtual graph instead of only inline in the main index

**Decision: virtual graph for complex geometries.**

Arguments for virtual graph:
- Follows established BM25/vector pattern — no new architectural concepts
- Spatial index can be rebuilt independently of the main binary index
- Doesn't increase main index size for non-spatial workloads
- Multi-cell coverings don't fit the one-flake-per-fact model
- Spatial index configuration (S2 levels, max_cells) can vary per virtual graph
- Supports remote deployment for high-scale workloads

### Why flat sorted array instead of a tree for the cell index

**Decision: flat sorted array with binary search.**

The cell index is a sorted array of fixed-width 32-byte entries. This is chosen over a tree structure (B-tree, LSM-tree) because:

- **Immutability.** The index is rebuilt from scratch during re-indexing, not incrementally mutated. A flat array is the simplest immutable structure.
- **Cache locality.** Fixed-width entries in a contiguous buffer have optimal cache behavior for range scans.
- **Zero overhead.** No node pointers, no balancing metadata, no page splits. The binary search cost is O(log N) with excellent constants.
- **Content-addressable.** A single `Vec<u8>` hashes to a single CAS address. No multi-artifact tree structure to manage.

**Cell index sizing:**

The entry count is `sum(covering_cells_per_geometry)`, not the geometry count. Each geometry produces 1 to `max_cells` entries depending on its complexity:

| Geometry type | Typical covering cells | Example |
|---------------|------------------------|---------|
| Point | 1 | Single cell at max_level |
| Small polygon | 4-8 | Building footprint |
| Large polygon | 8 (capped by max_cells) | City boundary |
| Complex polygon | 8 (capped) | Irregular park shape |

**Example sizing calculation:**

```
Dataset: 1M polygons (avg 6 covering cells) + 9M points (1 cell each)
Cell entries: (1M × 6) + (9M × 1) = 15M entries
Cell index size: 15M × 32 bytes = 480 MB
```

For a typical mixed workload with 10M geometries (mostly points), expect ~12-15M cell entries → 384-480 MB. This fits in memory for most deployments. For larger indexes, the array can be partitioned into chunks (like the main index's leaf/leaflet structure) with a lightweight manifest for chunk routing.

### Why S2 level 23 default (not level 30)

S2 level 30 provides ~1 cm² precision, but:

- Most geospatial workloads operate at meter-to-kilometer scales
- Level 30 cell IDs for a polygon covering would produce enormous index entries
- Level 23 (~1 m²) provides adequate precision for urban-scale applications
- Users who need centimeter precision can configure `s2_max_level: 30`

The default `s2_max_level: 23` balances precision against index size. For point-only workloads, the covering is a single cell regardless of level, so the level primarily affects polygon covering granularity.

### Why WKT as the primary geometry serialization

**Decision: WKT primary, GeoJSON secondary.**

- WKT (`geo:wktLiteral`) is the GeoSPARQL standard's primary serialization
- WKT is compact for simple geometries (`POINT(2.29 48.86)` vs GeoJSON's `{"type":"Point","coordinates":[2.29,48.86]}`)
- The `wkt` Rust crate provides zero-copy parsing
- GeoJSON support is added via the `geojson` crate for interoperability, but WKT is the recommended default

### Why haversine distance (not Vincenty or Karney)

For `geof:distance`, the default distance calculation uses the haversine formula (great-circle distance on a sphere). This is chosen over geodesic distance (Vincenty/Karney on the WGS84 ellipsoid) because:

- Haversine is ~10x faster to compute
- Error vs geodesic is <0.3% for most practical distances
- The `geo` crate's `HaversineDistance` trait is well-tested
- Geodesic distance can be added as an option for surveying-grade applications

---

## Future optimizations

### GPU-accelerated spatial operations

For workloads with very large geometry sets, GPU acceleration could significantly speed up:
- R-tree construction for spatial joins
- Batch point-in-polygon tests
- Distance matrix computation

The remote spatial server pattern enables this without changing the core Fluree architecture — a specialized GPU server can be deployed and accessed via nameservice RPC.

### Spatial aggregation

GeoSPARQL 1.1 defines spatial aggregate functions (union of geometries, bounding box of a set). These would be implemented as aggregate operators in the query pipeline:

```sparql
SELECT ?district (geof:aggUnion(?wkt) AS ?combined)
WHERE {
  ?parcel ex:district ?district ;
          geo:hasGeometry/geo:asWKT ?wkt .
}
GROUP BY ?district
```

### DGGS (Discrete Global Grid System) support

GeoSPARQL 1.1 includes a DGGS conformance class. S2 cells are themselves a DGGS, so Fluree could expose S2 cell IDs as a native DGGS representation alongside WKT/GeoJSON.

### Multi-resolution indexing

For workloads with widely varying geometry scales (city blocks alongside continents), multiple spatial indexes at different S2 level ranges could be maintained. The query planner would select the appropriate index based on the query geometry's scale.

### Coordinate reference system transformation

Future versions may support:
- CRS metadata on geometry literals (`geo:coordinateReferenceSystem`)
- On-the-fly transformation via the `proj` crate
- Per-index CRS configuration

---

## References

- [OGC GeoSPARQL 1.1 Standard](https://docs.ogc.org/is/22-047r1/22-047r1.html) — the authoritative specification for geospatial SPARQL
- [S2 Geometry Library](https://s2geometry.io/) — Google's spherical geometry and spatial indexing library
- [CockroachDB: How We Built Spatial Indexing](https://www.cockroachlabs.com/blog/how-we-built-spatial-indexing/) — rationale for S2 over R-trees in an LSM-tree database
- [DuckDB Spatial Joins](https://duckdb.org/2025/08/08/spatial-joins.html) — ephemeral R-tree strategy for spatial joins
- [geo-index: Packed Immutable R-tree](https://github.com/georust/geo-index) — immutable R-tree for Rust
- [geo crate](https://crates.io/crates/geo) — Rust geospatial primitives and algorithms
- [s2 crate](https://crates.io/crates/s2) — Rust S2 geometry library
- [Apache Jena GeoSPARQL](https://jena.apache.org/documentation/geosparql/) — reference implementation
- [GraphDB GeoSPARQL Support](https://graphdb.ontotext.com/documentation/11.1/geosparql-support.html) — another reference implementation
- [Eclipse RDF4J GeoSPARQL](https://rdf4j.org/documentation/programming/geosparql/) — Java implementation reference
