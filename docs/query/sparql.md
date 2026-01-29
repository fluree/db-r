# SPARQL

Fluree provides full support for SPARQL 1.1, the W3C standard query language for RDF. SPARQL enables compatibility with existing RDF tools, knowledge graphs, and semantic web applications.

## Overview

SPARQL (SPARQL Protocol and RDF Query Language) is the industry standard for querying RDF data. Fluree implements SPARQL 1.1, providing full compatibility with SPARQL endpoints and tools.

### Basic SPARQL Query

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name ?age
WHERE {
  ?person ex:name ?name .
  ?person ex:age ?age .
}
```

## Query Forms

### SELECT Queries

Return variable bindings:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name ?email
WHERE {
  ?person ex:name ?name .
  ?person ex:email ?email .
}
```

**DISTINCT Results:**

```sparql
SELECT DISTINCT ?name
WHERE {
  ?person ex:name ?name .
}
```

**Reduced Results:**

```sparql
SELECT REDUCED ?name
WHERE {
  ?person ex:name ?name .
}
```

### CONSTRUCT Queries

Generate RDF graphs from query results:

```sparql
PREFIX ex: <http://example.org/ns/>

CONSTRUCT {
  ?person ex:displayName ?name .
}
WHERE {
  ?person ex:name ?name .
}
```

See [CONSTRUCT Queries](construct.md) for details.

### ASK Queries

Return boolean indicating if query matches:

```sparql
PREFIX ex: <http://example.org/ns/>

ASK {
  ?person ex:name "Alice" .
}
```

### DESCRIBE Queries

Return RDF description of resources:

```sparql
PREFIX ex: <http://example.org/ns/>

DESCRIBE ex:alice
```

## Basic Graph Patterns

### Triple Patterns

Match RDF triples:

```sparql
?person ex:name ?name .
```

### Multiple Patterns

Combine patterns with AND semantics:

```sparql
?person ex:name ?name .
?person ex:age ?age .
?person ex:email ?email .
```

### Property Paths

Express relationships:

**Simple Path:**

```sparql
?person ex:friend/ex:name ?friendName .
```

**Alternative Paths:**

```sparql
?person (ex:friend|ex:colleague) ?other .
```

**Inverse Paths:**

```sparql
?person ^ex:friend ?other .
```

**Zero or More:**

```sparql
?person ex:ancestor* ?ancestor .
```

**One or More:**

```sparql
?person ex:ancestor+ ?ancestor .
```

**Zero or One:**

```sparql
?person ex:optional? ?value .
```

## Query Modifiers

### FILTER

Filter results with conditions:

```sparql
SELECT ?name ?age
WHERE {
  ?person ex:name ?name .
  ?person ex:age ?age .
  FILTER (?age > 18)
}
```

**Multiple Filters:**

```sparql
FILTER (?age > 18 && ?age < 65)
FILTER (regex(?name, "^A"))
```

### OPTIONAL

Match optional patterns:

```sparql
SELECT ?name ?email
WHERE {
  ?person ex:name ?name .
  OPTIONAL { ?person ex:email ?email . }
}
```

**Multiple Optionals:**

```sparql
SELECT ?name ?email ?phone
WHERE {
  ?person ex:name ?name .
  OPTIONAL { ?person ex:email ?email . }
  OPTIONAL { ?person ex:phone ?phone . }
}
```

### UNION

Match alternative patterns:

```sparql
SELECT ?name
WHERE {
  { ?person ex:name ?name . }
  UNION
  { ?person ex:alias ?name . }
}
```

### MINUS

Exclude matching patterns:

```sparql
SELECT ?person
WHERE {
  ?person ex:type ex:User .
  MINUS { ?person ex:status ex:Inactive . }
}
```

### BIND

Compute values:

```sparql
SELECT ?name ?nextAge
WHERE {
  ?person ex:name ?name .
  ?person ex:age ?age .
  BIND (?age + 1 AS ?nextAge)
}
```

### VALUES

Provide initial bindings:

```sparql
SELECT ?person ?name
WHERE {
  VALUES ?name { "Alice" "Bob" "Carol" }
  ?person ex:name ?name .
}
```

## Aggregation

### GROUP BY

Group results:

```sparql
SELECT ?category (COUNT(?product) AS ?count)
WHERE {
  ?product ex:category ?category .
}
GROUP BY ?category
```

### HAVING

Filter grouped results:

```sparql
SELECT ?category (COUNT(?product) AS ?count)
WHERE {
  ?product ex:category ?category .
}
GROUP BY ?category
HAVING (COUNT(?product) > 10)
```

### Aggregation Functions

- `COUNT(?var)` - Count non-null values
- `SUM(?var)` - Sum numeric values
- `AVG(?var)` - Average numeric values
- `MIN(?var)` - Minimum value
- `MAX(?var)` - Maximum value
- `SAMPLE(?var)` - Sample value
- `GROUP_CONCAT(?var)` - Concatenate values

## Sorting and Limiting

### ORDER BY

Sort results:

```sparql
SELECT ?name ?age
WHERE {
  ?person ex:name ?name .
  ?person ex:age ?age .
}
ORDER BY ?name
```

**Descending:**

```sparql
ORDER BY DESC(?age)
```

**Multiple Sort Keys:**

```sparql
ORDER BY ?last ASC(?first) DESC(?age)
```

### LIMIT

Limit number of results:

```sparql
SELECT ?name
WHERE {
  ?person ex:name ?name .
}
LIMIT 10
```

### OFFSET

Skip results:

```sparql
SELECT ?name
WHERE {
  ?person ex:name ?name .
}
OFFSET 20
LIMIT 10
```

## Datasets

### FROM

Specify default graph:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name
FROM <mydb:main>
WHERE {
  ?person ex:name ?name .
}
```

**Multiple Default Graphs:**

```sparql
SELECT ?name
FROM <mydb:main>
FROM <otherdb:main>
WHERE {
  ?person ex:name ?name .
}
```

### FROM NAMED

Specify named graphs:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?graph ?name
FROM NAMED <mydb:main>
FROM NAMED <otherdb:main>
WHERE {
  GRAPH ?graph {
    ?person ex:name ?name .
  }
}
```

See [Datasets](datasets.md) for details.

## SPARQL Functions

### String Functions

- `STR(?x)` - String value
- `LANG(?x)` - Language tag
- `LANGMATCHES(?lang, ?pattern)` - Language match
- `REGEX(?str, ?pattern)` - Regular expression
- `REPLACE(?str, ?pattern, ?replacement)` - Replace
- `SUBSTR(?str, ?start, ?length)` - Substring
- `STRLEN(?str)` - String length
- `UCASE(?str)` - Uppercase
- `LCASE(?str)` - Lowercase
- `ENCODE_FOR_URI(?str)` - URI encode
- `CONCAT(?str1, ?str2, ...)` - Concatenate

### Numeric Functions

- `ABS(?x)` - Absolute value
- `ROUND(?x)` - Round
- `CEIL(?x)` - Ceiling
- `FLOOR(?x)` - Floor
- `RAND()` - Random number

### Date/Time Functions

- `NOW()` - Current timestamp
- `YEAR(?date)` - Year
- `MONTH(?date)` - Month
- `DAY(?date)` - Day
- `HOURS(?time)` - Hours
- `MINUTES(?time)` - Minutes
- `SECONDS(?time)` - Seconds

### Type Conversion

- `STRDT(?str, ?datatype)` - String to typed literal
- `STRLANG(?str, ?lang)` - String with language
- `DATATYPE(?literal)` - Datatype
- `IRI(?str)` - IRI from string
- `URI(?str)` - URI from string
- `BNODE(?str)` - Blank node

### Logical Functions

- `BOUND(?var)` - Variable is bound
- `IF(?condition, ?then, ?else)` - Conditional
- `COALESCE(?x, ?y, ...)` - First non-null value
- `ISIRI(?x)` - Is IRI
- `ISURI(?x)` - Is URI
- `ISBLANK(?x)` - Is blank node
- `ISLITERAL(?x)` - Is literal
- `ISNUMERIC(?x)` - Is numeric

## Subqueries

Nest queries:

```sparql
SELECT ?person ?name
WHERE {
  ?person ex:name ?name .
  {
    SELECT ?person
    WHERE {
      ?person ex:age ?age .
      FILTER (?age > 18)
    }
  }
}
```

## Service Queries

Federated queries (planned):

```sparql
SELECT ?name
WHERE {
  SERVICE <http://example.org/sparql> {
    ?person ex:name ?name .
  }
}
```

## Time Travel

### Point-in-Time Queries

Query data as it existed at a specific time using time specifiers in the `FROM` clause:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?name ?age
FROM <ledger:main@t:100>
WHERE {
  ?person ex:name ?name ;
          ex:age ?age .
}
```

Time specifiers:
- `@t:100` - Transaction number
- `@iso:2024-01-15T10:30:00Z` - ISO 8601 datetime
- `@sha:abc123def456` - Commit hash prefix
- `@t:latest` - Current/latest state

### History Queries

Query all changes (assertions and retractions) within a time range using `FROM...TO` with RDF-star syntax:

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX f: <https://ns.flur.ee/ledger#>

SELECT ?age ?t ?op
FROM <ledger:main@t:1>
TO <ledger:main@t:latest>
WHERE {
  << ex:alice ex:age ?age >> f:t ?t .
  << ex:alice ex:age ?age >> f:op ?op .
}
ORDER BY ?t
```

The `<< subject predicate object >>` syntax (RDF-star) treats the triple as an entity that can have metadata:
- `f:t` - Transaction time when the fact was asserted or retracted
- `f:op` - Operation type: `"assert"` or `"retract"`

**Filter by operation type:**

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX f: <https://ns.flur.ee/ledger#>

SELECT ?age ?t
FROM <ledger:main@t:1>
TO <ledger:main@t:latest>
WHERE {
  << ex:alice ex:age ?age >> f:t ?t .
  << ex:alice ex:age ?age >> f:op ?op .
  FILTER(?op = "retract")
}
```

**History with ISO datetime range:**

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX f: <https://ns.flur.ee/ledger#>

SELECT ?name ?t ?op
FROM <ledger:main@iso:2024-01-01T00:00:00Z>
TO <ledger:main@iso:2024-12-31T23:59:59Z>
WHERE {
  << ex:alice ex:name ?name >> f:t ?t .
  << ex:alice ex:name ?name >> f:op ?op .
}
```

## SPARQL UPDATE

Fluree supports SPARQL 1.1 Update for modifying data using standard SPARQL syntax. SPARQL UPDATE requests use the `application/sparql-update` content type and are sent to the transact endpoints.

### INSERT DATA

Insert ground triples (no variables):

```sparql
PREFIX ex: <http://example.org/ns/>

INSERT DATA {
  ex:alice ex:name "Alice" .
  ex:alice ex:age 30 .
  ex:alice ex:email "alice@example.org" .
}
```

**HTTP Request:**

```bash
curl -X POST http://localhost:8090/ledger/mydb:main/transact \
  -H "Content-Type: application/sparql-update" \
  -d 'PREFIX ex: <http://example.org/ns/>
      INSERT DATA { ex:alice ex:name "Alice" }'
```

### DELETE DATA

Delete specific ground triples:

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE DATA {
  ex:alice ex:email "alice@example.org" .
}
```

### DELETE WHERE

Delete triples matching a pattern:

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE WHERE {
  ex:alice ex:age ?age .
}
```

This finds all `ex:age` values for `ex:alice` and deletes them.

### DELETE/INSERT (Modify)

The most powerful form combines WHERE, DELETE, and INSERT clauses:

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE {
  ?person ex:age ?oldAge .
}
INSERT {
  ?person ex:age ?newAge .
}
WHERE {
  ?person ex:name "Alice" .
  ?person ex:age ?oldAge .
  BIND(?oldAge + 1 AS ?newAge)
}
```

**Update multiple properties:**

```sparql
PREFIX ex: <http://example.org/ns/>

DELETE {
  ?person ex:name ?oldName .
  ?person ex:status ?oldStatus .
}
INSERT {
  ?person ex:name "Alicia" .
  ?person ex:status ex:Active .
}
WHERE {
  ?person ex:name "Alice" .
  OPTIONAL { ?person ex:name ?oldName }
  OPTIONAL { ?person ex:status ?oldStatus }
}
```

### Blank Nodes in INSERT

Blank nodes can be used in INSERT templates to create new entities:

```sparql
PREFIX ex: <http://example.org/ns/>

INSERT DATA {
  _:newPerson ex:name "Bob" .
  _:newPerson ex:age 25 .
}
```

### Typed Literals

Specify datatypes explicitly:

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

INSERT DATA {
  ex:alice ex:birthDate "1990-05-15"^^xsd:date .
  ex:alice ex:salary "75000.00"^^xsd:decimal .
  ex:alice ex:active "true"^^xsd:boolean .
}
```

### Language-Tagged Strings

Insert strings with language tags:

```sparql
PREFIX ex: <http://example.org/ns/>

INSERT DATA {
  ex:alice ex:name "Alice"@en .
  ex:alice ex:name "Alicia"@es .
  ex:alice ex:name "アリス"@ja .
}
```

### SPARQL UPDATE Restrictions

Current MVP restrictions:

- **WHERE patterns**: Only basic triple patterns are supported. OPTIONAL, FILTER, UNION, and VALUES in WHERE clauses are not yet supported.
- **Blank nodes in WHERE**: Blank nodes cannot be used in WHERE patterns (use variables instead).
- **WITH/USING clauses**: Graph scoping via WITH and USING is not yet supported.

### Endpoint Usage

SPARQL UPDATE uses the transact endpoints with `Content-Type: application/sparql-update`:

| Endpoint | Description |
|----------|-------------|
| `POST /fluree/transact` | Connection-scoped, requires `Fluree-Ledger` header |
| `POST /:ledger/transact` | Ledger-scoped, ledger from URL path |

**Examples:**

```bash
# Ledger-scoped (recommended)
curl -X POST http://localhost:8090/ledger/mydb:main/transact \
  -H "Content-Type: application/sparql-update" \
  -d 'PREFIX ex: <http://example.org/ns/>
      INSERT DATA { ex:alice ex:name "Alice" }'

# Connection-scoped with header
curl -X POST http://localhost:8090/fluree/transact \
  -H "Content-Type: application/sparql-update" \
  -H "Fluree-Ledger: mydb:main" \
  -d 'PREFIX ex: <http://example.org/ns/>
      INSERT DATA { ex:alice ex:name "Alice" }'
```

## Best Practices

1. **Use PREFIX Declarations**: Makes queries readable
2. **Optimize Patterns**: Order patterns for efficient execution
3. **Use FILTER Early**: Place filters as early as possible
4. **Limit Results**: Use LIMIT for large result sets
5. **Avoid Cartesian Products**: Structure queries to avoid large joins

## Related Documentation

- [JSON-LD Query](jsonld-query.md): Fluree's native query language
- [CONSTRUCT Queries](construct.md): Generating RDF graphs
- [Datasets](datasets.md): Multi-graph queries
- [Output Formats](output-formats.md): Query result formats
- [Transactions](../transactions/overview.md): JSON-LD transaction format
