# Geospatial indexing: S2 cell-based spatial index with GeoSPARQL query support

This document tracks the design and implementation of geospatial indexing in Fluree using Google's S2 geometry library as the primary spatial index structure, exposed through the OGC GeoSPARQL standard. Point proximity queries use a unified `geof:distance` bind+filter pattern in both SPARQL and JSON-LD. Complex geometry queries use the `idx:spatial` JSON-LD pattern (with planned SPARQL `geof:sf*` support).

**Key design goals:**

1. **Time-travel native** — Spatial queries respect Fluree's `@t` semantics
2. **Append-only compatible** — No rebalancing; cell IDs are immutable
3. **Scalable** — Supports both embedded indexes and remote specialized servers
4. **Standards-based** — GeoSPARQL 1.1 compliance for interoperability

---

## Implementation status

### Completed

#### Inline GeoPoint encoding (fluree-db-core)

Points are stored directly in the main flake index using `ObjKind::GEO_POINT = 0x14`. The 60-bit packed representation (`(lat << 30) | lng`) provides ~0.3mm precision. Key pieces:

- `GeoPointBits` type in [value.rs](fluree-db-core/src/value.rs) — packed u64 with `new()`, `lat()`, `lng()`, `Display` (WKT format)
- `encode_geo_point()` / `decode_geo_point()` in [value_id.rs](fluree-db-core/src/value_id.rs)
- `try_extract_point()` in [geo.rs](fluree-db-core/src/geo.rs) — lightweight POINT detection at ingestion (prefix check, no full WKT parse)
- `haversine_distance()` and `geo_proximity_bounds()` in [geo.rs](fluree-db-core/src/geo.rs) — antimeridian-aware bounding box computation

Non-point WKT geometries fall through to `ObjKind::LexId` (string dictionary).

#### S2 cell index for complex geometries (fluree-db-spatial)

Full sidecar virtual graph for polygons, linestrings, and multi-geometries. Follows the BM25/vector pattern.

| Module | Purpose |
|--------|---------|
| [config.rs](fluree-db-spatial/src/config.rs) | `SpatialCreateConfig`, `S2CoveringConfig` (default: min=4, max=16, cells=8), `MetadataConfig` |
| [builder.rs](fluree-db-spatial/src/builder.rs) | Builds index from geometry data; skips POINTs when `index_points=false` (default) |
| [cell_index.rs](fluree-db-spatial/src/cell_index.rs) | Sorted `(cell_id, subject_id, t DESC)` entries, CAS-backed chunked storage ("FSC1" format) |
| [geometry.rs](fluree-db-spatial/src/geometry.rs) | Geometry arena — WKT parsing, handle management, optional metadata (bbox, centroid) |
| [covering.rs](fluree-db-spatial/src/covering.rs) | S2 covering generation from `geo::Geometry` |
| [snapshot.rs](fluree-db-spatial/src/snapshot.rs) | Content-addressed snapshot assembly |
| [novelty.rs](fluree-db-spatial/src/novelty.rs) | Derives cell entries from commits since snapshot watermark |
| [replay.rs](fluree-db-spatial/src/replay.rs) | Time-travel replay with temporal filtering |
| [dedup.rs](fluree-db-spatial/src/dedup.rs) | Cross-cell deduplication by subject_id |
| [provider.rs](fluree-db-spatial/src/provider.rs) | `SpatialIndexProvider` trait + `EmbeddedSpatialProvider` impl |

CellEntry format: `cell_id: u64, subject_id: u64, geo_handle: u32, t: i64, op: u8` (32 bytes, fixed-width).

#### Query operators (fluree-db-query)

Two operators, each using a different index:

**GeoSearchOperator** ([geo_search.rs](fluree-db-query/src/geo_search.rs)) — Inline GeoPoint path:
1. Convert center + radius → latitude-band bounds via `geo_proximity_bounds()`
2. POST cursor with `o_kind = GEO_POINT`
3. Haversine post-filter, dedup by subject_id (keep min distance)
4. Sort by distance, apply limit

**S2SearchOperator** ([s2_search.rs](fluree-db-query/src/s2_search.rs)) — S2 cell index path:
- Operations: `Within`, `Contains`, `Intersects`, `Nearby { radius_meters }`
- Graph-scoped provider routing: key `"g{g_id}:{predicate_iri}"`
- Late materialization via `Binding::EncodedSid`

Both operators support time-travel (`to_t`) and novelty overlays.

#### Query syntax

**Point proximity** — Unified `geof:distance` bind+filter pattern (JSON-LD and SPARQL):

Both JSON-LD and SPARQL express point proximity as Triple + Bind(geof:distance) + Filter.
The query optimizer in `geo_rewrite.rs` detects this pattern and rewrites it to
`Pattern::GeoSearch` for index acceleration.

```json
{
  "where": [
    { "@id": "?building", "ex:location": "?loc" },
    ["bind", "?dist", "(geof:distance ?loc \"POINT(2.3522 48.8566)\")"],
    ["filter", "(<= ?dist 1000)"]
  ]
}
```

**S2 spatial search** (`idx:spatial`) — JSON-LD only:
```json
{
  "idx:spatial": "within",
  "idx:geometry": "POLYGON((2.2 48.8, 2.4 48.8, 2.4 48.9, 2.2 48.9, 2.2 48.8))",
  "idx:property": "ex:hasGeometry",
  "idx:result": "?building"
}
```

Operations: `within`, `contains`, `intersects`, `nearby`. Optional: `idx:radius`, `idx:limit`, `idx:property`, `idx:index`.

#### geof:distance → GeoSearch rewrite (geo_rewrite.rs)

[geo_rewrite.rs](fluree-db-query/src/geo_rewrite.rs) runs for both SPARQL and JSON-LD queries. It detects the pattern:

```
Triple(?s, pred, ?loc) → Bind(?dist = geof:distance(?loc, WKT)) → Filter(?dist < radius)
```

and rewrites it to `Pattern::GeoSearch` for index acceleration. Handles:
- IRI predicate encoding via callback
- `?loc` liveness (preserves Triple if var used elsewhere)
- Nested scopes (Optional, Union, Graph, Minus, Exists)
- 9 unit tests

#### geof:distance function evaluation

`FunctionName::GeofDistance` in [filter/functions.rs](fluree-db-query/src/filter/functions.rs) — haversine distance from two WKT points or GeoPoint values. Used in both BIND and FILTER clauses.

#### Test coverage

- [it_query_geo.rs](fluree-db-api/tests/it_query_geo.rs) (370 lines, 6 tests) — roundtrip, distance filters, SPARQL path
- [it_query_geo_search.rs](fluree-db-api/tests/it_query_geo_search.rs) (984 lines, 7 tests) — time-travel, retraction, dedup, limit/kNN, named graphs, SPARQL optimizer

---

### Remaining work

What follows are the milestones in priority order. Each builds on the previous.

---

## Milestone A: SPARQL spatial predicates → Pattern::S2Search

**Status: not started**

### Problem

`geo_rewrite.rs` currently only rewrites `geof:distance` patterns into `Pattern::GeoSearch` (inline GeoPoint path). There is no SPARQL-level rewrite for `geof:sfWithin`, `geof:sfContains`, or `geof:sfIntersects` to `Pattern::S2Search`. These predicates work today only via the JSON-LD `idx:spatial` pattern.

Additionally, `fluree-db-sparql` has zero `geof:` namespace awareness for spatial predicates — all handling is in `fluree-db-query`'s query pipeline (the `geof:distance` rewrite runs in `geo_rewrite.rs` after both SPARQL and JSON-LD lowering).

### What to build

**1. Extend `geo_rewrite.rs` for spatial predicates**

The same rewrite machinery used for `geof:distance` should detect and rewrite these patterns:

```
FILTER(geof:sfWithin(?wkt, "POLYGON(...)"))
  → Pattern::S2Search { op: Within, query_geom: Wkt("POLYGON(...)"), subject_var: ?s }

FILTER(geof:sfContains(?wkt, "POINT(...)"))
  → Pattern::S2Search { op: Contains, query_geom: Wkt("POINT(...)"), subject_var: ?s }

FILTER(geof:sfIntersects(?wkt, "POLYGON(...)"))
  → Pattern::S2Search { op: Intersects, query_geom: Wkt("POLYGON(...)"), subject_var: ?s }
```

The pattern to detect is simpler than distance (no Bind + Filter combo needed):

```
Triple(?s, pred, ?wkt) → Filter(geof:sfWithin(?wkt, constant_geom))
```

When one argument is a variable bound via a triple and the other is a constant WKT literal, the rewrite fires. When both are variables, it falls back to filter evaluation (or future spatial join).

**2. Add `FunctionName` variants**

Add `GeofSfWithin`, `GeofSfContains`, `GeofSfIntersects`, `GeofSfDisjoint` to the `FunctionName` enum in `ir.rs`. Wire them in `parse/lower.rs` for recognition of `"geof:sfWithin"` etc.

**3. Add filter-evaluation fallback**

For cases where the rewrite doesn't fire (both args are variables, or no S2 index available), implement filter evaluation in `filter/functions.rs` using the `geo` crate's `Contains`, `Intersects` traits on parsed WKT.

**4. SPARQL parser awareness (fluree-db-sparql)**

Register the `geof:` namespace prefix and function names in the SPARQL parser so that `FILTER(geof:sfWithin(...))` parses correctly into the IR `FilterExpr::Function` nodes that `geo_rewrite.rs` can detect.

### Priority

**P0 functions (this milestone):**

| Function | Signature | Rewrite target |
|----------|-----------|----------------|
| `geof:sfWithin` | `(geom1, geom2) → bool` | `S2Search { op: Within }` |
| `geof:sfContains` | `(geom1, geom2) → bool` | `S2Search { op: Contains }` |
| `geof:sfIntersects` | `(geom1, geom2) → bool` | `S2Search { op: Intersects }` |
| `geof:sfDisjoint` | `(geom1, geom2) → bool` | Complement of Intersects |
| `geof:distance` | `(geom1, geom2, unit) → double` | Already done (GeoSearch) |

**P1 functions (future):**

`geof:sfTouches`, `geof:buffer`, `geof:envelope`, `geof:sfEquals`

**P2 functions (future):**

`geof:sfCrosses`, `geof:sfOverlaps`, `geof:convexHull`, `geof:union`, `geof:intersection`, `geof:area`

---

## Milestone B: Point cell index (true point kNN)

**Status: not started**

### Problem

The current inline GeoPoint path (`GeoSearchOperator`) uses a latitude-band POST scan. This works but has structural limitations:

1. **False positive rate scales with latitude.** At 45° the scan returns ~35% extra rows; at 70° it's ~55%; near poles it's 70%+. The haversine post-filter is cheap (~50ns/point) but the I/O cost of scanning the full latitude band can be significant for large datasets or large radii.

2. **No true kNN.** The current approach scans the full radius, collects all matches, sorts by distance, and applies limit. There's no way to start near the center and expand outward — you must scan the entire band.

3. **Large radius performance.** A 100km proximity query at mid-latitudes scans a latitude band ~200km wide across all longitudes. For dense point distributions this can be millions of false positives.

### What to build

A point-specific S2 cell index stored as a sidecar artifact, using the same CAS-backed chunked storage pattern as the existing S2 complex-geometry index.

#### Point index entry format

```rust
PointEntry {
    cell_id:    u64,   // S2 cell ID at configured level
    subject_id: u64,   // Subject identifier
    geo_bits:   u64,   // Packed GeoPointBits (distance is self-contained)
    t:          i64,   // Transaction time
    op:         u8,    // assert/retract
}
```

Sort order: `(cell_id, subject_id, t DESC, op ASC)` — same time-travel + tie-breaking model as the complex geometry cell index.

Storing `geo_bits` in the entry makes the index self-contained: exact haversine distance can be computed without lookups into the main index.

#### Indexer pipeline

The point index builder:
1. Scans the ledger's GeoPoint facts per `(g_id, p_id)` during index build
2. Computes `cell_id = S2CellId::from_lat_lng(lat, lng).parent(level).id()`
3. Emits `PointEntry` records (including retracts from novelty)
4. Persists as chunked leaflets with a manifest for lookup

This can share substantial infrastructure with the existing `fluree-db-spatial` crate (CAS storage, chunking, manifest format).

#### Query operator + planner routing

Add a `PointCellSearchOperator` (or extend `GeoSearch` planning):

```
geof:distance bind+filter → geo_rewrite → Pattern::GeoSearch
  planner chooses:
    PointCellSearchOperator (new fast path) — if point cell index exists for (g_id, predicate)
    GeoSearchOperator (latitude-band POST scan) — fallback
```

#### Radius query algorithm

1. Compute S2 covering for the query circle (typically 4-8 cells)
2. Convert covering cells → `cell_id` ranges (`[cell.range_min(), cell.range_max()]`)
3. Range scan point index leaflets for those ranges
4. Apply replay at `to_t` (same temporal filtering as existing index)
5. Exact haversine check using embedded `geo_bits`
6. Sort by distance, apply limit

This replaces the latitude-band scan with a tight multi-range scan. False positive rates drop from 22-70% to ~1-5%.

#### kNN query algorithm (future extension)

1. Start with a small covering near the center (1-2 cells at a fine level)
2. Scan, compute exact distances
3. If fewer than K results, expand outward (add neighboring cells at coarser levels)
4. Repeat until K candidates found with a known distance bound
5. One final scan to catch any points within the bound that might have been in cells not yet scanned

For the minimal v1: build the point index + radius query path first. Support LIMIT by sorting candidates by exact distance after scanning. Ring-expanding kNN can be added later.

#### Cell level tradeoff

| Level | Cell size | Scans per query | False positives |
|-------|-----------|-----------------|-----------------|
| 12 | ~3km | Few (coarse covering) | Higher |
| 16 | ~150m | Moderate | Moderate |
| 20 | ~10m | Many (fine covering) | Very low |
| 23 | ~1m | Many | Minimal |

Start with level 16 (matching the existing S2 config default). Make it configurable via `SpatialCreateConfig`.

---

## Milestone C: Streaming / early-stop improvements for S2 queries

**Status: not started**

### Problem

The current S2 search path collects and filters all candidates in memory per query. For large coverings or low-selectivity regions, this means:
- High peak memory for intermediate candidate sets
- No way to short-circuit once `LIMIT` is satisfied
- Tail latency spikes on queries with big polygons

### What to build

**1. Streaming replay + dedup + filter pipeline**

Instead of `collect → filter → dedup → sort`, process entries as a streaming pipeline:

```rust
cell_range_scans(covering)        // yields CellEntry stream
  → replay_filter(to_t)           // temporal resolution, streaming
  → dedup_by_subject(seen_set)    // hash set, emit on first encounter
  → bbox_prefilter(query_bbox)    // cheap rejection using metadata
  → exact_spatial_test(query_geom) // geo crate predicates
  → limit(n)                      // early termination
```

Each stage is an iterator adapter. When the final `limit` stage has enough results, the entire pipeline stops — no need to scan remaining cells.

**2. Priority-ordered cell scanning for nearby queries**

For `S2SpatialOp::Nearby`, scan covering cells in order of distance from query center. This way the nearest candidates appear first, and with a limit the scan can terminate early.

**3. Chunked I/O with prefetching**

When scanning CAS-backed cell index chunks, issue reads for the next chunk while processing the current one. This overlaps I/O with computation.

---

## Milestone D: Antimeridian-crossing geometry normalization

**Status: not started**

### Problem

Geometries that cross the international date line (180°/-180° longitude) produce pathological S2 coverings. A polygon spanning from 179° to -179° (a narrow strip across the date line) can produce 200+ covering cells because the S2 coverer sees it as wrapping nearly the entire globe.

This isn't a correctness issue — queries return correct results. But the explosion in covering cells degrades:
- Index size (many more cell entries per geometry)
- Query performance (many more range scans)
- Memory usage during index build

### What to build

**1. Detect antimeridian-crossing bboxes**

During covering computation, detect when a geometry's longitude span crosses the date line:

```rust
fn crosses_antimeridian(geom: &geo::Geometry) -> bool {
    let bbox = geom.bounding_rect();
    // If min_lng > max_lng after normalization, it crosses
    bbox.min().x > bbox.max().x
}
```

**2. Split geometries at the antimeridian**

For geometries that cross, split into two halves:
- Eastern hemisphere portion: `[lng_min, 180°]`
- Western hemisphere portion: `[-180°, lng_max]`

Compute separate S2 coverings for each half and merge. This produces far fewer cells than a single covering of the "wrapped" geometry.

**3. Normalize longitude in bbox prefilters**

The bbox prefilter in the geometry arena must handle two-interval longitude ranges:

```rust
struct BoundingBox {
    min_lat: f32,
    max_lat: f32,
    // Two longitude intervals to handle antimeridian
    lng_intervals: Vec<(f32, f32)>,  // typically 1, occasionally 2
}
```

---

## Milestone E: Spatial joins (ephemeral R-tree)

**Status: not started**

### Problem

When both arguments to a spatial predicate are variables (`geof:sfIntersects(?parkWkt, ?bldgWkt)`), this is a spatial join. The current system has no optimized path for this — it would fall back to nested-loop evaluation.

### What to build

Build a temporary packed R-tree at query time on the smaller side, probe with the larger side:

1. Materialize both sides to point-in-time
2. Build packed R-tree on smaller set (using `geo-index` crate)
3. For each geometry on larger side, probe R-tree for bbox candidates
4. Exact predicate test on candidates
5. Emit matching pairs

**Dependencies:** Add `geo-index = "0.2"` crate for packed immutable R-trees.

**Performance expectations:**

| Dataset size | Nested loop | Ephemeral R-tree |
|--------------|-------------|------------------|
| 1K × 1K | ~1 second | ~10 ms |
| 10K × 10K | ~100 seconds | ~200 ms |
| 100K × 100K | ~3 hours | ~5 seconds |

This is lower priority than Milestones A-D because spatial joins are less common than single-geometry queries in typical workloads.

---

## Milestone F: GeoJSON ingestion + P1 functions

**Status: not started**

### GeoJSON support

Add `geo:geoJSONLiteral` datatype handling in the transaction processor. Parse GeoJSON to `geo::Geometry` and route to the same inline GeoPoint / S2 cell index paths as WKT.

**Dependency:** Add `geojson = "0.24"` crate.

### P1 GeoSPARQL functions

| Function | Signature | Implementation |
|----------|-----------|----------------|
| `geof:sfTouches` | `(geom1, geom2) → bool` | `geo` crate's boundary intersection |
| `geof:buffer` | `(geom, radius, unit) → geom` | `geo` crate's buffer operation |
| `geof:envelope` | `(geom) → geom` | `geo` crate's `BoundingRect` |
| `geof:sfEquals` | `(geom1, geom2) → bool` | Topological equality check |

---

## Milestone G: Remote spatial index server

**Status: stub exists** (`fluree-db-spatial` has `remote` feature flag and placeholder struct in `provider.rs`)

A gRPC/HTTP client implementing `SpatialIndexProvider` that delegates to a remote specialized spatial server. Useful for:
- Indexes with 100M+ geometries
- GPU-accelerated spatial operations
- Multi-tenant isolation

This is the lowest priority milestone — the embedded provider handles most workloads.

---

## Background: design decisions (reference)

### Why S2 cells

S2 projects Earth's surface onto a cube, recursively subdivided via Hilbert curve into 31 levels of cells (level 0 = hemisphere, level 30 ≈ 1 cm²). Key properties:

- **Cell IDs are u64 integers** — Hilbert-ordered, so spatially adjacent cells have close IDs
- **Totally ordered** — storable in any sorted structure, queryable via range scans
- **Hierarchy is predetermined** — no rebalancing, no locking
- **Containment = range query** — cell C contains all descendants in `[C.range_min(), C.range_max()]`

R-trees require rebalancing on mutation and can't share structure between snapshots. S2 cells avoid both problems. Ephemeral R-trees remain useful for query-time spatial joins.

### Why concatenated lat/lng encoding (not Z-order or S2) for inline GeoPoint

The `(lat << 30) | lng` encoding was chosen for v1 simplicity:
- Two shifts and two masks — no lookup tables or library deps
- Direct coordinate extraction for display and distance computation
- Acceptable false positive rate at mid-latitudes (~35%)

Z-order (Morton) encoding would reduce false positives to ~5-10% but adds complexity. S2 cell IDs provide ~1% false positives but require the S2 library for decode. The point cell index (Milestone B) provides the tight candidate generation that Z-order/S2 encoding would give, without changing the inline encoding.

### Why points excluded from S2 cell index by default

Inline GeoPoint handles points with zero additional index overhead via POST scans. Adding points to the S2 cell index would double storage for no benefit. Config flag `index_points: true` exists for edge cases.

### Why flat sorted array for cell index

Immutable, cache-friendly, zero overhead. 32-byte fixed-width entries enable binary search with excellent constants. Content-addressable as a single artifact.

### Why haversine distance (not geodesic)

~10x faster, <0.3% error vs Vincenty/Karney for practical distances. Geodesic can be added as an option later.

---

## Dependencies

```toml
# Current
s2 = "0.0.12"       # S2 cell computation, coverings
geo = "0.29"         # Geometry types, spatial predicates
geo-types = "0.7"    # Core geometry type definitions
wkt = "0.11"         # WKT parsing

# Planned
geo-index = "0.2"    # Packed immutable R-tree (Milestone E)
geojson = "0.24"     # GeoJSON parsing (Milestone F)
```

---

## References

- [OGC GeoSPARQL 1.1 Standard](https://docs.ogc.org/is/22-047r1/22-047r1.html)
- [S2 Geometry Library](https://s2geometry.io/)
- [CockroachDB: How We Built Spatial Indexing](https://www.cockroachlabs.com/blog/how-we-built-spatial-indexing/)
- [geo-index: Packed Immutable R-tree](https://github.com/georust/geo-index)
- [geo crate](https://crates.io/crates/geo)
- [s2 crate](https://crates.io/crates/s2)
