# Output Formats

Fluree supports multiple output formats for query results, each optimized for different use cases. You can choose the format that best fits your application's needs.

## Supported Formats

### JSON-LD Format

**Default format** for JSON-LD Query. Provides compact, context-aware JSON with IRI expansion/compaction.

**Characteristics:**
- Uses `@context` for IRI compaction
- Compact IRIs (e.g., `ex:alice` instead of full IRIs)
- Type-preserving (datatypes maintained)
- Language tags preserved

**Example:**

```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "@graph": [
    {
      "@id": "ex:alice",
      "ex:name": "Alice",
      "ex:age": {
        "@value": "30",
        "@type": "http://www.w3.org/2001/XMLSchema#integer"
      }
    }
  ]
}
```

**Row Format:**

```json
[
  ["Alice", 30],
  ["Bob", 25]
]
```

### SPARQL JSON Format

Standard SPARQL 1.1 result format for SPARQL queries.

**Characteristics:**
- W3C SPARQL 1.1 compliant
- Standard `results` and `bindings` structure
- Datatype information included
- Language tags included

**Example:**

```json
{
  "head": {
    "vars": ["name", "age"]
  },
  "results": {
    "bindings": [
      {
        "name": {
          "type": "literal",
          "value": "Alice"
        },
        "age": {
          "type": "literal",
          "value": "30",
          "datatype": "http://www.w3.org/2001/XMLSchema#integer"
        }
      },
      {
        "name": {
          "type": "literal",
          "value": "Bob"
        },
        "age": {
          "type": "literal",
          "value": "25",
          "datatype": "http://www.w3.org/2001/XMLSchema#integer"
        }
      }
    ]
  }
}
```

### Typed JSON Format

Type-preserving JSON format with explicit datatype information.

**Characteristics:**
- Explicit datatype information
- Language tags preserved
- IRI expansion/compaction
- Suitable for type-aware applications

**Example:**

```json
[
  {
    "name": {
      "value": "Alice",
      "type": "http://www.w3.org/2001/XMLSchema#string"
    },
    "age": {
      "value": 30,
      "type": "http://www.w3.org/2001/XMLSchema#integer"
    }
  },
  {
    "name": {
      "value": "Bob",
      "type": "http://www.w3.org/2001/XMLSchema#string"
    },
    "age": {
      "value": 25,
      "type": "http://www.w3.org/2001/XMLSchema#integer"
    }
  }
]
```

## Format Selection

### JSON-LD Query

JSON-LD Query defaults to JSON-LD format. You can specify the format explicitly:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "select": ["?name", "?age"],
  "where": [
    { "@id": "?person", "ex:name": "?name", "ex:age": "?age" }
  ],
  "format": "jsonld"
}
```

### SPARQL

SPARQL queries return SPARQL JSON format by default:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name ?age
WHERE {
  ?person ex:name ?name .
  ?person ex:age ?age .
}
```

## Datatype Handling

### String Types

**JSON-LD:**

```json
{
  "@value": "Hello",
  "@type": "http://www.w3.org/2001/XMLSchema#string"
}
```

**SPARQL JSON:**

```json
{
  "type": "literal",
  "value": "Hello"
}
```

### Numeric Types

**JSON-LD:**

```json
{
  "@value": "42",
  "@type": "http://www.w3.org/2001/XMLSchema#integer"
}
```

**SPARQL JSON:**

```json
{
  "type": "literal",
  "value": "42",
  "datatype": "http://www.w3.org/2001/XMLSchema#integer"
}
```

### Language-Tagged Strings

**JSON-LD:**

```json
{
  "@value": "Hello",
  "@language": "en"
}
```

**SPARQL JSON:**

```json
{
  "type": "literal",
  "value": "Hello",
  "xml:lang": "en"
}
```

### IRIs

**JSON-LD:**

```json
{
  "@id": "ex:alice"
}
```

**SPARQL JSON:**

```json
{
  "type": "uri",
  "value": "http://example.org/ns/alice"
}
```

## Best Practices

1. **Use JSON-LD for Applications**: Most applications benefit from JSON-LD's compact format
2. **Use SPARQL JSON for SPARQL Tools**: Standard format for SPARQL endpoints
3. **Use Typed JSON for Type-Aware Apps**: When explicit type information is needed
4. **Consider Performance**: JSON-LD is typically most efficient

## Related Documentation

- [JSON-LD Query](jsonld-query.md): JSON-LD Query language
- [SPARQL](sparql.md): SPARQL query language
- [Datatypes](../concepts/datatypes.md): Type system details
