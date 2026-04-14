# Cookbook: SHACL Validation

SHACL (Shapes Constraint Language) is a W3C standard for defining constraints on graph data. In Fluree, SHACL shapes are validated at transaction time — invalid data is rejected before it's committed.

## Quick start

### 1. Enable SHACL validation

Configure the ledger to enable SHACL and point to the graph containing your shapes:

```bash
fluree insert '
@prefix f: <https://ns.flur.ee/db#> .

<urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
  f:shaclDefaults [
    f:shaclEnabled true ;
    f:shapesSource "default"
  ] .
'
```

`f:shapesSource "default"` means shapes are stored in the same default graph as your data.

### 2. Define a shape

Shapes describe what valid data looks like:

```bash
fluree insert '
@prefix sh:     <http://www.w3.org/ns/shacl#> .
@prefix schema: <http://schema.org/> .
@prefix ex:     <http://example.org/> .
@prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .

ex:PersonShape a sh:NodeShape ;
  sh:targetClass schema:Person ;
  sh:property [
    sh:path schema:name ;
    sh:datatype xsd:string ;
    sh:minCount 1 ;
    sh:maxCount 1 ;
    sh:message "Every person must have exactly one name (string)"
  ] ;
  sh:property [
    sh:path schema:email ;
    sh:datatype xsd:string ;
    sh:pattern "^[^@]+@[^@]+\\.[^@]+$" ;
    sh:message "Email must be a valid email address"
  ] ;
  sh:property [
    sh:path ex:age ;
    sh:datatype xsd:integer ;
    sh:minInclusive 0 ;
    sh:maxInclusive 200 ;
    sh:message "Age must be between 0 and 200"
  ] .
'
```

### 3. Insert valid data

```bash
fluree insert '
@prefix schema: <http://schema.org/> .
@prefix ex:     <http://example.org/> .

ex:alice a schema:Person ;
  schema:name  "Alice Chen" ;
  schema:email "alice@example.com" ;
  ex:age       30 .
'
```

This succeeds — the data conforms to the shape.

### 4. Insert invalid data

```bash
fluree insert '
@prefix schema: <http://schema.org/> .
@prefix ex:     <http://example.org/> .

ex:invalid a schema:Person ;
  schema:email "not-an-email" ;
  ex:age       -5 .
'
```

This **fails** with validation errors:
- Missing required property `schema:name` (minCount 1)
- `schema:email` doesn't match the email pattern
- `ex:age` is below minInclusive (0)

## Common constraint patterns

### Required properties

```turtle
ex:PersonShape a sh:NodeShape ;
  sh:targetClass schema:Person ;
  sh:property [
    sh:path schema:name ;
    sh:minCount 1 ;
    sh:message "Name is required"
  ] .
```

### Datatype validation

```turtle
ex:ProductShape a sh:NodeShape ;
  sh:targetClass ex:Product ;
  sh:property [
    sh:path ex:price ;
    sh:datatype xsd:decimal ;
    sh:message "Price must be a decimal number"
  ] ;
  sh:property [
    sh:path ex:inStock ;
    sh:datatype xsd:boolean ;
    sh:message "In-stock must be a boolean"
  ] .
```

### Value ranges

```turtle
ex:OrderShape a sh:NodeShape ;
  sh:targetClass ex:Order ;
  sh:property [
    sh:path ex:quantity ;
    sh:datatype xsd:integer ;
    sh:minInclusive 1 ;
    sh:maxInclusive 10000 ;
    sh:message "Quantity must be between 1 and 10,000"
  ] .
```

### String constraints

```turtle
ex:UserShape a sh:NodeShape ;
  sh:targetClass ex:User ;
  sh:property [
    sh:path ex:username ;
    sh:datatype xsd:string ;
    sh:minLength 3 ;
    sh:maxLength 32 ;
    sh:pattern "^[a-zA-Z0-9_]+$" ;
    sh:message "Username must be 3-32 alphanumeric characters"
  ] .
```

### Cardinality

```turtle
ex:ArticleShape a sh:NodeShape ;
  sh:targetClass ex:Article ;
  sh:property [
    sh:path ex:title ;
    sh:minCount 1 ;
    sh:maxCount 1 ;
    sh:message "Article must have exactly one title"
  ] ;
  sh:property [
    sh:path ex:tag ;
    sh:minCount 1 ;
    sh:message "Article must have at least one tag"
  ] .
```

### Allowed values (in)

```turtle
ex:TaskShape a sh:NodeShape ;
  sh:targetClass ex:Task ;
  sh:property [
    sh:path ex:status ;
    sh:in ("todo" "in-progress" "review" "done") ;
    sh:message "Status must be one of: todo, in-progress, review, done"
  ] ;
  sh:property [
    sh:path ex:priority ;
    sh:in ("low" "medium" "high" "critical") ;
    sh:message "Priority must be low, medium, high, or critical"
  ] .
```

### Reference constraints (class of target)

```turtle
ex:OrderShape a sh:NodeShape ;
  sh:targetClass ex:Order ;
  sh:property [
    sh:path ex:customer ;
    sh:class schema:Person ;
    sh:minCount 1 ;
    sh:message "Order must reference a Person as customer"
  ] ;
  sh:property [
    sh:path ex:product ;
    sh:class ex:Product ;
    sh:minCount 1 ;
    sh:message "Order must reference at least one Product"
  ] .
```

### Logical constraints (or, and, not)

```turtle
ex:ContactShape a sh:NodeShape ;
  sh:targetClass ex:Contact ;
  sh:or (
    [ sh:property [ sh:path schema:email ; sh:minCount 1 ] ]
    [ sh:property [ sh:path schema:telephone ; sh:minCount 1 ] ]
  ) ;
  sh:message "Contact must have either an email or phone number" .
```

## SHACL with JSON-LD

You can also define shapes in JSON-LD:

```json
{
  "@context": {
    "sh": "http://www.w3.org/ns/shacl#",
    "schema": "http://schema.org/",
    "ex": "http://example.org/",
    "xsd": "http://www.w3.org/2001/XMLSchema#"
  },
  "@id": "ex:PersonShape",
  "@type": "sh:NodeShape",
  "sh:targetClass": {"@id": "schema:Person"},
  "sh:property": [
    {
      "sh:path": {"@id": "schema:name"},
      "sh:datatype": {"@id": "xsd:string"},
      "sh:minCount": 1,
      "sh:maxCount": 1
    },
    {
      "sh:path": {"@id": "schema:email"},
      "sh:datatype": {"@id": "xsd:string"},
      "sh:pattern": "^[^@]+@[^@]+\\.[^@]+$"
    }
  ]
}
```

## Configuration options

SHACL validation is configured in the [ledger config graph](../ledger-config/setting-groups.md#shacl-defaults):

| Field | Type | Default | Description |
|---|---|---|---|
| `f:shaclEnabled` | boolean | `false` | Enable or disable SHACL validation |
| `f:shapesSource` | GraphRef | (none) | Graph containing SHACL shapes |

The `f:shapesSource` controls where shapes are loaded from. Use `"default"` for the default graph, or provide a named graph IRI.

## Shapes are data

Because SHACL shapes are stored as regular data in the ledger:

- **Time-travelable** — See what validation rules were in effect at any point
- **Versionable** — Change shapes through normal transactions
- **Queryable** — Query your shapes: `SELECT ?shape ?target WHERE { ?shape sh:targetClass ?target }`
- **Branchable** — Test new validation rules on a branch before merging

## Best practices

1. **Start with required properties** — `sh:minCount 1` catches the most common data quality issues
2. **Add shapes incrementally** — Don't try to validate everything at once
3. **Use descriptive messages** — `sh:message` helps debug validation failures
4. **Test shapes on a branch** — Create a branch, add shapes, run test transactions, then merge
5. **Validate existing data** — After adding a shape, query for entities that would violate it

## Related documentation

- [Setting Groups — SHACL](../ledger-config/setting-groups.md#shacl-defaults) — Configuration reference
- [Override Control](../ledger-config/override-control.md) — Per-transaction override rules
- [Ledger Configuration](../ledger-config/README.md) — Config graph overview
