# R2RML (Relational to RDF Mapping)

R2RML (RDB to RDF Mapping Language) is a W3C standard for mapping relational databases to RDF. Fluree uses R2RML to create graph sources over SQL databases, enabling you to query relational data using SPARQL or JSON-LD Query.

## What is R2RML?

R2RML defines how to map:
- Database tables to RDF classes
- Table columns to RDF properties
- Rows to RDF resources
- Foreign keys to RDF relationships

This enables querying existing relational databases as if they were RDF graphs.

## Configuration

### Create R2RML Graph Source

```bash
curl -X POST http://localhost:8090/graph-source \
  -H "Content-Type: application/json" \
  -d '{
    "name": "sql-customers",
    "type": "r2rml",
    "database": "postgresql://user:pass@localhost/mydb",
    "mapping_file": "customer-mapping.ttl"
  }'
```

### Supported Databases

- PostgreSQL
- MySQL / MariaDB
- SQLite
- Microsoft SQL Server
- Oracle

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

Fluree generates SQL:

```sql
SELECT
  c.name,
  c.email
FROM customers c
```

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

Combines product data from Fluree with inventory from SQL database.

## SQL Generation

### Filter Pushdown

Fluree pushes filters to SQL:

```text
SPARQL: FILTER (?price < 100)
  ↓
SQL: WHERE price < 100
```

### Column Pruning

Only requested columns are selected:

```text
SELECT ?name ?email
  ↓
SQL: SELECT name, email  (not SELECT *)
```

### Join Optimization

Fluree optimizes join order based on query patterns.

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

1. **Read-Only:** R2RML graph sources are read-only (no writes)
2. **SQL Compatibility:** Advanced SQL features may not map cleanly to RDF
3. **Performance:** Complex joins across Fluree + SQL may be slow
4. **Schema Changes:** Requires mapping update when schema changes

## Troubleshooting

### Connection Errors

```json
{
  "error": "DatabaseConnectionError",
  "message": "Cannot connect to database"
}
```

**Solutions:**
- Check database URL
- Verify credentials
- Check network connectivity
- Verify database is running

### Mapping Errors

```json
{
  "error": "R2RMLMappingError",
  "message": "Invalid R2RML mapping: table 'customers' not found"
}
```

**Solutions:**
- Verify table names
- Check column names
- Validate R2RML syntax
- Test SQL queries independently

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
