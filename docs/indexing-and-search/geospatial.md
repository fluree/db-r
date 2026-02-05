# Geospatial Data

Fluree provides native support for geographic point data using the OGC GeoSPARQL standard. POINT geometries from `geo:wktLiteral` values are stored in an optimized binary format enabling efficient storage and future proximity queries.

## Status

Geospatial support is implemented with:

- **Inline GeoPoint encoding**: POINT geometries stored as packed 60-bit lat/lng values
- **Automatic detection**: `geo:wktLiteral` POINT values automatically converted to native format
- **Full round-trip**: GeoPoints preserved through commit, index, and query paths
- **~0.3mm precision**: 30-bit encoding per coordinate provides sub-millimeter accuracy

Non-POINT geometries (polygons, lines, etc.) are stored as strings with the `geo:wktLiteral` datatype for future sidecar spatial index support.

## Storing Geographic Data

### WKT Literal Format

Geographic data uses the Well-Known Text (WKT) format with the `geo:wktLiteral` datatype:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "geo": "http://www.opengis.net/ont/geosparql#"
  },
  "@graph": [
    {
      "@id": "ex:eiffel-tower",
      "@type": "ex:Landmark",
      "ex:name": "Eiffel Tower",
      "ex:location": {
        "@value": "POINT(2.2945 48.8584)",
        "@type": "geo:wktLiteral"
      }
    }
  ]
}
```

**Important**: WKT uses `POINT(longitude latitude)` order (X, Y), which is the opposite of common lat/lng conventions.

### Coordinate Order

| Format | Order | Example |
|--------|-------|---------|
| WKT | longitude, latitude | `POINT(2.2945 48.8584)` |
| Common conventions | latitude, longitude | `48.8584, 2.2945` |

Fluree handles the conversion internally, storing coordinates in latitude-primary order for efficient latitude-band index scans.

### Valid POINT Syntax

Fluree recognizes these POINT formats:

```
POINT(2.2945 48.8584)           # Standard 2D point
POINT( 2.2945  48.8584 )        # Whitespace is flexible
POINT(-122.4194 37.7749)        # Negative coordinates (San Francisco)
```

The following are **not** supported for native GeoPoint storage (stored as strings instead):

```
POINT EMPTY                      # Empty point
POINT Z(2.2945 48.8584 100)     # 3D point with altitude
POINT M(2.2945 48.8584 1.0)     # Point with measure
POINT ZM(2.2945 48.8584 100 1)  # 3D point with measure
<http://...>POINT(...)          # SRID prefix
point(2.2945 48.8584)           # Lowercase (case-sensitive)
```

### Coordinate Validation

Coordinates must be within valid ranges:

- **Latitude**: -90.0 to 90.0 (degrees)
- **Longitude**: -180.0 to 180.0 (degrees)
- **Finite values only**: NaN and infinity are rejected

Invalid coordinates cause the value to be stored as a plain string rather than a native GeoPoint.

## Querying Geographic Data

### Basic Retrieval

GeoPoints are returned in WKT format in query results:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "geo": "http://www.opengis.net/ont/geosparql#"
  },
  "from": "places:main",
  "where": [
    { "@id": "?place", "@type": "ex:Landmark" },
    { "@id": "?place", "ex:location": "?loc" }
  ],
  "select": ["?place", "?loc"]
}
```

Result:

```json
[
  ["ex:eiffel-tower", "POINT(2.2945 48.8584)"]
]
```

### SPARQL Queries

```sparql
PREFIX ex: <http://example.org/>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>

SELECT ?place ?location
WHERE {
  ?place a ex:Landmark ;
         ex:location ?location .
}
```

### Output Formats

GeoPoints appear differently based on output format:

**JSON-LD (default):**
```json
{
  "@id": "ex:eiffel-tower",
  "ex:location": {
    "@value": "POINT(2.2945 48.8584)",
    "@type": "geo:wktLiteral"
  }
}
```

**SPARQL JSON:**
```json
{
  "type": "literal",
  "value": "POINT(2.2945 48.8584)",
  "datatype": "http://www.opengis.net/ont/geosparql#wktLiteral"
}
```

**Typed JSON:**
```json
{
  "@value": "POINT(2.2945 48.8584)",
  "@type": "geo:wktLiteral"
}
```

## Storage Encoding

### Binary Format

GeoPoints are stored using a compact 60-bit encoding:

- **Upper 30 bits**: Latitude scaled from [-90, 90] to [0, 2^30-1]
- **Lower 30 bits**: Longitude scaled from [-180, 180] to [0, 2^30-1]

This provides:

- **8 bytes total storage** per point (vs ~25+ bytes for WKT string)
- **~0.3mm precision** at the equator
- **Ordered encoding** enabling efficient range scans by latitude band

### Index Structure

GeoPoints use `ObjKind::GEO_POINT` (0x14) in the binary index:

| Component | Encoding |
|-----------|----------|
| Object kind | 1 byte (0x14) |
| Object key | 8 bytes (packed lat/lng) |

The latitude-primary encoding enables POST index scans that efficiently retrieve all points within a latitude band.

## Proximity Queries (Future)

The current implementation provides the foundation for proximity queries. The following capabilities are planned:

### Bounding Box Scans

```json
{
  "where": [
    { "@id": "?place", "ex:location": "?loc" },
    { "geo:within": { "?loc": { "lat": [48.0, 49.0], "lng": [2.0, 3.0] } } }
  ]
}
```

### Distance Filtering

```json
{
  "where": [
    { "@id": "?place", "ex:location": "?loc" },
    { "geo:distance": { "?loc": "POINT(2.2945 48.8584)", "radius": 10000 } }
  ]
}
```

### Query Implementation Notes

Proximity queries will use:

1. **POST latitude-band scan**: Returns all points in the latitude range (may include false positives outside longitude bounds)
2. **Haversine post-filter**: Filters results to actual distance threshold

The 30/30 encoding produces efficient latitude-band scans with acceptable false positive rates (22-70% depending on latitude and search radius).

## Examples

### Storing Multiple Locations

```json
{
  "@context": {
    "ex": "http://example.org/",
    "geo": "http://www.opengis.net/ont/geosparql#"
  },
  "@graph": [
    {
      "@id": "ex:paris",
      "@type": "ex:City",
      "ex:name": "Paris",
      "ex:center": { "@value": "POINT(2.3522 48.8566)", "@type": "geo:wktLiteral" }
    },
    {
      "@id": "ex:london",
      "@type": "ex:City",
      "ex:name": "London",
      "ex:center": { "@value": "POINT(-0.1278 51.5074)", "@type": "geo:wktLiteral" }
    },
    {
      "@id": "ex:tokyo",
      "@type": "ex:City",
      "ex:name": "Tokyo",
      "ex:center": { "@value": "POINT(139.6917 35.6895)", "@type": "geo:wktLiteral" }
    }
  ]
}
```

### Turtle Format

```turtle
@prefix ex: <http://example.org/> .
@prefix geo: <http://www.opengis.net/ont/geosparql#> .

ex:sensor-1 a ex:WeatherStation ;
    ex:name "Central Park Station" ;
    ex:location "POINT(-73.9654 40.7829)"^^geo:wktLiteral .

ex:sensor-2 a ex:WeatherStation ;
    ex:name "Times Square Station" ;
    ex:location "POINT(-73.9855 40.7580)"^^geo:wktLiteral .
```

### Mixed Geometry Types

Non-POINT geometries are stored as strings:

```json
{
  "@context": {
    "ex": "http://example.org/",
    "geo": "http://www.opengis.net/ont/geosparql#"
  },
  "@graph": [
    {
      "@id": "ex:central-park",
      "@type": "ex:Park",
      "ex:name": "Central Park",
      "ex:entrance": {
        "@value": "POINT(-73.9654 40.7829)",
        "@type": "geo:wktLiteral"
      },
      "ex:boundary": {
        "@value": "POLYGON((-73.9819 40.7681, -73.9580 40.8006, -73.9493 40.7969, -73.9732 40.7644, -73.9819 40.7681))",
        "@type": "geo:wktLiteral"
      }
    }
  ]
}
```

The `ex:entrance` POINT is stored as a native GeoPoint, while the `ex:boundary` POLYGON is stored as a string.

## GeoSPARQL Compatibility

Fluree supports a subset of the OGC GeoSPARQL standard:

| Feature | Status |
|---------|--------|
| `geo:wktLiteral` datatype | Supported |
| POINT geometry | Native encoding |
| LINESTRING geometry | String storage |
| POLYGON geometry | String storage |
| `geo:asWKT` property | Use any property with wktLiteral type |
| `geof:distance` function | Planned |
| `geof:within` function | Planned |
| Spatial indexes (R-tree/S2) | Future (sidecar index) |

## Best Practices

### Use geo:wktLiteral for All Geometry

Always declare the datatype explicitly:

```json
// Correct
{ "@value": "POINT(2.3522 48.8566)", "@type": "geo:wktLiteral" }

// Incorrect - stored as plain string
{ "@value": "POINT(2.3522 48.8566)" }
```

### Coordinate Precision

While Fluree stores ~0.3mm precision, consider your source data accuracy:

```json
// Excessive precision (GPS typically ±3-5m)
"POINT(2.352219834765 48.856614892341)"

// Appropriate precision for most applications
"POINT(2.3522 48.8566)"
```

### Coordinate Validation

Validate coordinates before insertion:

- Latitude: -90 to 90
- Longitude: -180 to 180
- No NaN or infinity values

Invalid coordinates are stored as strings and won't benefit from native GeoPoint indexing.

## Related Documentation

- [Datatypes](../concepts/datatypes.md) - Type system overview
- [Vector Search](vector-search.md) - Similarity search
- [BM25](bm25.md) - Full-text search
