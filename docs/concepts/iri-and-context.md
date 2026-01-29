# IRIs, Namespaces, and JSON-LD @context

## Internationalized Resource Identifiers (IRIs)

In Fluree, all data identifiers use **Internationalized Resource Identifiers (IRIs)** - the internationalized version of URIs. IRIs uniquely identify:

- **Subjects**: Entities in your data (people, products, concepts)
- **Predicates**: Relationships or properties
- **Objects**: Values or other entities
- **Graphs**: Named data partitions

### IRI Examples

```turtle
# Full IRIs
<http://example.org/person/alice> <http://xmlns.com/foaf/0.1/name> "Alice" .
<http://example.org/person/alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .

# IRIs with Unicode characters
<http://例え.org/人物/アリス> <http://xmlns.com/foaf/0.1/name> "アリス" .
```

### IRI Best Practices

- **Use stable domains**: Choose domains you control or well-established standards
- **Hierarchical structure**: Organize IRIs with meaningful paths
- **Avoid query parameters**: IRIs should be clean identifiers, not URLs with parameters
- **Internationalization**: IRIs support Unicode characters for global identifiers

## Namespaces

**Namespaces** provide shorthand notation for IRIs, making data more readable and manageable. A namespace maps a prefix to a base IRI.

### Defining Namespaces

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "xsd": "http://www.w3.org/2001/XMLSchema#"
  }
}
```

### Using Namespaced IRIs

With the above context, you can write compact IRIs:

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "foaf": "http://xmlns.com/foaf/0.1/"
  },
  "@graph": [
    {
      "@id": "ex:alice",
      "@type": "foaf:Person",
      "foaf:name": "Alice Smith"
    }
  ]
}
```

This expands to:

```json
{
  "@graph": [
    {
      "@id": "http://example.org/ns/alice",
      "@type": "http://xmlns.com/foaf/0.1/Person",
      "http://xmlns.com/foaf/0.1/name": "Alice Smith"
    }
  ]
}
```

## JSON-LD @context

The **@context** is a JSON-LD mechanism that defines how to interpret the data. In Fluree, @context serves multiple purposes:

### IRI Expansion/Compaction

```json
{
  "@context": {
    "name": "http://xmlns.com/foaf/0.1/name",
    "Person": "http://xmlns.com/foaf/0.1/Person"
  },
  "@graph": [
    {
      "@id": "http://example.org/alice",
      "@type": "Person",
      "name": "Alice"
    }
  ]
}
```

The @context maps `name` → `http://xmlns.com/foaf/0.1/name` and `Person` → `http://xmlns.com/foaf/0.1/Person`.

### Standard Prefixes

Fluree includes many standard prefixes by default:

```json
{
  "@context": {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "dc": "http://purl.org/dc/elements/1.1/"
  }
}
```

### @context in Queries

@context is also used in query results for compact output:

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "foaf": "http://xmlns.com/foaf/0.1/"
  },
  "@graph": [
    {
      "@id": "ex:alice",
      "@type": "foaf:Person",
      "foaf:name": "Alice"
    }
  ]
}
```

## IRI Resolution Rules

Fluree follows strict IRI resolution rules:

### Absolute IRIs

These are used as-is:
- `http://example.org/person/alice`
- `https://data.example.com/product/123`

### Prefixed IRIs

These expand using @context:
- `ex:alice` → `http://example.org/ns/alice` (if `ex` maps to `http://example.org/ns/`)
- `foaf:name` → `http://xmlns.com/foaf/0.1/name`

### Relative IRIs

These are resolved relative to a base IRI:
- `alice` → `http://example.org/ns/alice` (if base is `http://example.org/ns/`)

## Blank Nodes and Anonymous Entities

**Blank nodes** represent entities without global identifiers:

```json
{
  "@graph": [
    {
      "@id": "_:b1",
      "foaf:name": "Anonymous Person"
    }
  ]
}
```

Blank nodes are:
- Local to a single transaction
- Cannot be referenced across transactions
- Useful for temporary or anonymous data

## Best Practices

### Namespace Organization

1. **Use stable prefixes**: Don't change prefix mappings once data is committed
2. **Standard vocabularies**: Use well-known prefixes (foaf, dc, rdf, etc.)
3. **Custom domains**: Use your own domain for application-specific terms
4. **Versioning**: Consider versioning in namespace IRIs for evolution

### IRI Design

1. **Descriptive paths**: Use meaningful hierarchical paths
2. **Avoid special characters**: Stick to URL-safe characters
3. **Consistent casing**: Use consistent capitalization conventions
4. **Future-proofing**: Design IRIs to accommodate future extensions

### @context Management

1. **Shared contexts**: Reuse @context definitions across transactions
2. **Minimal contexts**: Only define prefixes you actually use
3. **Documentation**: Document custom prefixes and their meanings
4. **Evolution**: Plan for @context changes over time

## Integration with Standards

Fluree's IRI system is fully compatible with:

- **RDF Standards**: Works with RDF/XML, Turtle, N-Triples
- **SPARQL**: IRIs work seamlessly in SPARQL queries
- **Linked Data**: Enables publishing and consuming linked data
- **Semantic Web**: Supports OWL ontologies and RDF Schema

This foundation enables Fluree to participate in the broader semantic web ecosystem while providing the convenience of JSON-LD's compact syntax.