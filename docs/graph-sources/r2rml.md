# R2RML (Relational to RDF Mapping)

R2RML (RDB to RDF Mapping Language) is a W3C standard for mapping tabular data into RDF triples. In Fluree, R2RML mappings are used to expose **Iceberg tables** as RDF graph sources, enabling you to query data lake tables using SPARQL or JSON-LD Query.

## What is R2RML?

R2RML defines how to map:
- Database tables to RDF classes
- Table columns to RDF properties
- Rows to RDF resources
- Foreign keys to RDF relationships

This enables querying existing relational databases as if they were RDF graphs.

## Configuration

### Create R2RML Graph Source (Iceberg-backed)

Use `R2rmlCreateConfig` to register a graph source that combines:

- an Iceberg table (REST catalog or Direct S3), and
- an R2RML mapping (Turtle) that materializes table rows into RDF triples.

If you use **Direct S3** mode, Fluree resolves the current Iceberg metadata by reading `metadata/version-hint.text` under the configured `table_location`, then loading the metadata file referenced by the hint. The Iceberg table layout must already exist at that location.

```rust
use fluree_db_api::{FlureeBuilder, R2rmlCreateConfig};

let fluree = FlureeBuilder::default().build().await?;

let config = R2rmlCreateConfig::new_direct(
    "airlines-rdf",
    "s3://bucket/warehouse/openflights/airlines",
    "fluree:file://mappings/airlines.ttl",
)
.with_s3_region("us-east-1")
.with_s3_path_style(true)
.with_mapping_media_type("text/turtle");

fluree.create_r2rml_graph_source(config).await?;
```

## R2RML Mapping

### Basic Mapping

Map a table to RDF class:

```turtle
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.org/ns/> .
@prefix schema: <http://schema.org/> .

<#CustomerMapping>
  a rr:TriplesMap ;
  
  rr:logicalTable [
    rr:tableName "customers"
  ] ;
  
  rr:subjectMap [
    rr:template "http://example.org/customer/{id}" ;
    rr:class schema:Person
  ] ;
  
  rr:predicateObjectMap [
    rr:predicate schema:name ;
    rr:objectMap [ rr:column "name" ]
  ] ;
  
  rr:predicateObjectMap [
    rr:predicate schema:email ;
    rr:objectMap [ rr:column "email" ]
  ] ;
  
  rr:predicateObjectMap [
    rr:predicate ex:customerId ;
    rr:objectMap [ rr:column "id" ]
  ] .
```

This maps the `customers` table:

```sql
CREATE TABLE customers (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255),
  email VARCHAR(255)
);
```

To RDF triples:

```turtle
<http://example.org/customer/1>
  a schema:Person ;
  schema:name "Alice" ;
  schema:email "alice@example.org" ;
  ex:customerId "1" .
```

### Foreign Key Mapping

Map relationships:

```turtle
<#OrderMapping>
  a rr:TriplesMap ;
  
  rr:logicalTable [
    rr:tableName "orders"
  ] ;
  
  rr:subjectMap [
    rr:template "http://example.org/order/{id}" ;
    rr:class ex:Order
  ] ;
  
  rr:predicateObjectMap [
    rr:predicate ex:orderId ;
    rr:objectMap [ rr:column "id" ]
  ] ;
  
  rr:predicateObjectMap [
    rr:predicate ex:customer ;
    rr:objectMap [
      rr:parentTriplesMap <#CustomerMapping> ;
      rr:joinCondition [
        rr:child "customer_id" ;
        rr:parent "id"
      ]
    ]
  ] ;
  
  rr:predicateObjectMap [
    rr:predicate ex:total ;
    rr:objectMap [ rr:column "total" ]
  ] .
```

Maps foreign key `customer_id` to RDF object property linking to customer resource.

### Complex Queries

Use SQL views for complex mappings:

```turtle
<#SalesReportMapping>
  a rr:TriplesMap ;
  
  rr:logicalTable [
    rr:sqlQuery """
      SELECT
        c.id as customer_id,
        c.name as customer_name,
        SUM(o.total) as total_spent,
        COUNT(o.id) as order_count
      FROM customers c
      JOIN orders o ON o.customer_id = c.id
      WHERE o.order_date >= '2024-01-01'
      GROUP BY c.id, c.name
    """
  ] ;
  
  rr:subjectMap [
    rr:template "http://example.org/customer/{customer_id}" ;
    rr:class ex:Customer
  ] ;
  
  rr:predicateObjectMap [
    rr:predicate schema:name ;
    rr:objectMap [ rr:column "customer_name" ]
  ] ;
  
  rr:predicateObjectMap [
    rr:predicate ex:totalSpent ;
    rr:objectMap [ rr:column "total_spent" ; rr:datatype xsd:decimal ]
  ] ;
  
  rr:predicateObjectMap [
    rr:predicate ex:orderCount ;
    rr:objectMap [ rr:column "order_count" ; rr:datatype xsd:integer ]
  ] .
```

## Querying R2RML Graph Sources

R2RML graph sources are queried using standard SPARQL and JSON-LD query syntax — no special API calls or configuration needed. When the `iceberg` feature is compiled, R2RML support is automatically enabled for all query paths.

Graph sources can be:
- **Queried directly** as the target: `fluree query my-gs 'SELECT * WHERE { ?s ?p ?o }'`
- **Referenced in FROM clauses**: `SELECT * FROM <my-gs:main> WHERE { ... }`
- **Referenced in GRAPH patterns**: `SELECT * WHERE { GRAPH <my-gs:main> { ... } }` (useful for joining with ledger data)

### Basic Query

```json
{
  "@context": {
    "schema": "http://schema.org/",
    "ex": "http://example.org/ns/"
  },
  "from": "sql-customers:main",
  "select": ["?name", "?email"],
  "where": [
    { "@id": "?customer", "@type": "schema:Person" },
    { "@id": "?customer", "schema:name": "?name" },
    { "@id": "?customer", "schema:email": "?email" }
  ]
}
```

The mapping controls how subjects and predicate/object values are produced from the scanned table columns.

### SPARQL Query

```sparql
PREFIX schema: <http://schema.org/>
PREFIX ex: <http://example.org/ns/>

SELECT ?name ?email
FROM <sql-customers:main>
WHERE {
  ?customer a schema:Person .
  ?customer schema:name ?name .
  ?customer schema:email ?email .
}
```

### Filters

```json
{
  "from": "sql-customers:main",
  "select": ["?name", "?email"],
  "where": [
    { "@id": "?customer", "schema:name": "?name" },
    { "@id": "?customer", "schema:email": "?email" },
    { "@id": "?customer", "ex:status": "?status" }
  ],
  "filter": "?status == 'active'"
}
```

Generates SQL with WHERE clause:

```sql
SELECT c.name, c.email
FROM customers c
WHERE c.status = 'active'
```

### Joins

```json
{
  "from": "sql-db:main",
  "select": ["?customerName", "?orderTotal"],
  "where": [
    { "@id": "?customer", "schema:name": "?customerName" },
    { "@id": "?order", "ex:customer": "?customer" },
    { "@id": "?order", "ex:total": "?orderTotal" }
  ]
}
```

Generates SQL with JOIN:

```sql
SELECT
  c.name as customerName,
  o.total as orderTotal
FROM customers c
JOIN orders o ON o.customer_id = c.id
```

## Combining with Fluree Data

Join SQL database data with Fluree ledgers:

```json
{
  "from": ["products:main", "sql-inventory:main"],
  "select": ["?productName", "?stockLevel"],
  "where": [
    { "@id": "?product", "schema:name": "?productName" },
    { "@id": "?product", "ex:sku": "?sku" },
    { "@id": "?inventory", "ex:sku": "?sku" },
    { "@id": "?inventory", "ex:stockLevel": "?stockLevel" }
  ]
}
```

Combines product data from Fluree with inventory from an Iceberg-backed R2RML graph source.

## Performance Notes

R2RML graph sources execute by scanning the underlying Iceberg table and materializing RDF terms according to the mapping. For performance, prefer:

- filtering early,
- projecting only the columns needed by your query and mapping,
- and partitioning Iceberg tables by common filters.

## Performance

### Query Performance

SQL queries execute at native database speed:
- Indexes used automatically
- Query optimizer in SQL database
- Minimal overhead from RDF mapping

### Best Practices

1. **Use Database Indexes:**
   ```sql
   CREATE INDEX idx_customers_email ON customers(email);
   ```

2. **Filter Early:**
   ```json
   {
     "where": [...],
     "filter": "?status == 'active'"  // Pushed to SQL WHERE
   }
   ```

3. **Limit Results:**
   ```json
   {
     "where": [...],
     "limit": 100  // Pushed to SQL LIMIT
   }
   ```

4. **Use SQL Views:**
   For complex queries, create SQL views and map those:
   ```sql
   CREATE VIEW active_customers AS
   SELECT * FROM customers WHERE status = 'active';
   ```

## Use Cases

### Existing System Integration

Query existing systems without moving data:

```json
{
  "from": ["new-system:main", "existing-system:main"],
  "select": ["?customerName", "?newData", "?existingData"],
  "where": [...]
}
```

### Incremental Adoption

Adopt new systems while keeping existing systems operational:

```text
Phase 1: Both systems running, joined in queries
Phase 2: Write to new system, read from both
Phase 3: Copy data (optional)
Phase 4: Retire the existing system (optional)
```

### Unified Reporting

Report across multiple databases:

```json
{
  "from": [
    "sales-db:main",
    "inventory-db:main",
    "customer-db:main"
  ],
  "select": [...],
  "where": [...]
}
```

## Configuration Options

### Connection Pooling

```json
{
  "type": "r2rml",
  "database": "postgresql://localhost/mydb",
  "pool": {
    "min_connections": 5,
    "max_connections": 20,
    "connection_timeout": 30
  }
}
```

### Query Timeout

```json
{
  "type": "r2rml",
  "database": "postgresql://localhost/mydb",
  "query_timeout": 60000  // 60 seconds
}
```

### SSL/TLS

```json
{
  "type": "r2rml",
  "database": "postgresql://localhost/mydb",
  "ssl": {
    "enabled": true,
    "ca_cert": "/path/to/ca.pem",
    "client_cert": "/path/to/client.pem",
    "client_key": "/path/to/client-key.pem"
  }
}
```

## Limitations

1. **Read-Only:** R2RML graph sources are read-only (no writes via Fluree)
2. **Performance:** Complex joins across Fluree + Iceberg may be slow
3. **Schema Changes:** Requires mapping updates when referenced columns change

## Troubleshooting

### Connection Errors

```json
{
  "error": "IcebergConnectionError",
  "message": "Cannot load table metadata"
}
```

**Solutions:**
- Check catalog configuration (REST vs Direct)
- Verify AWS credentials and S3 access
- Verify `version-hint.text` is present for Direct mode

### Mapping Errors

```json
{
  "error": "R2RMLMappingError",
  "message": "Invalid R2RML mapping: table 'customers' not found"
}
```

**Solutions:**
- Verify table name / location
- Check referenced column names in the mapping
- Validate R2RML syntax (Turtle)

### Slow Queries

**Causes:**
- Missing indexes
- Large result sets
- Complex joins

**Solutions:**
- Add database indexes
- Use LIMIT clause
- Optimize R2RML mapping
- Use SQL views for complex queries

## Related Documentation

- [Graph Sources Overview](overview.md) - Graph source concepts
- [Iceberg](iceberg.md) - Data lake integration
- [Query Datasets](../query/datasets.md) - Multi-graph queries
