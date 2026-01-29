# Iceberg / Parquet

Fluree integrates with Apache Iceberg to query data lake tables as virtual graphs. This enables querying large-scale analytical data stored in Parquet format using the same RDF query interface.

## What is Apache Iceberg?

Apache Iceberg is an open table format for huge analytical datasets. It provides:
- ACID transactions on data lakes
- Time travel and versioning
- Schema evolution
- Partition management
- Optimized file organization (Parquet)

## Configuration

### Create Iceberg Virtual Graph

```bash
curl -X POST http://localhost:8090/virtual-graph \
  -H "Content-Type: application/json" \
  -d '{
    "name": "warehouse-orders",
    "type": "iceberg",
    "catalog": "glue",
    "warehouse": "s3://my-data-warehouse/",
    "table": "sales.orders",
    "mapping": {
      "order_id": "ex:orderId",
      "customer_id": "ex:customerId",
      "product_id": "ex:productId",
      "quantity": "ex:quantity",
      "total": "ex:total",
      "order_date": "ex:orderDate"
    }
  }'
```

### Catalog Types

**AWS Glue:**
```json
{
  "catalog": "glue",
  "warehouse": "s3://bucket/path/",
  "aws_region": "us-east-1"
}
```

**Hive Metastore:**
```json
{
  "catalog": "hive",
  "metastore_uri": "thrift://localhost:9083",
  "warehouse": "hdfs://namenode:8020/warehouse/"
}
```

**REST Catalog:**
```json
{
  "catalog": "rest",
  "catalog_uri": "http://localhost:8181/",
  "warehouse": "s3://bucket/path/"
}
```

**Hadoop:**
```json
{
  "catalog": "hadoop",
  "warehouse": "s3://bucket/path/"
}
```

## Column Mapping

Map Iceberg columns to RDF predicates:

```json
{
  "mapping": {
    "id": "ex:id",
    "name": "schema:name",
    "email": "schema:email",
    "created_at": "ex:createdAt",
    "status": "ex:status"
  }
}
```

### Type Mapping

Iceberg types map to XSD types:

| Iceberg Type | RDF Type |
|--------------|----------|
| int, long | xsd:integer |
| float, double | xsd:decimal |
| string | xsd:string |
| boolean | xsd:boolean |
| date | xsd:date |
| timestamp | xsd:dateTime |
| uuid | xsd:string |

## Querying Iceberg Tables

### Basic Query

```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "from": "warehouse-orders:main",
  "select": ["?orderId", "?total"],
  "where": [
    { "@id": "?order", "ex:orderId": "?orderId" },
    { "@id": "?order", "ex:total": "?total" }
  ],
  "limit": 100
}
```

### SPARQL Query

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?orderId ?total ?date
FROM <warehouse-orders:main>
WHERE {
  ?order ex:orderId ?orderId .
  ?order ex:total ?total .
  ?order ex:orderDate ?date .
  FILTER (?date >= "2024-01-01"^^xsd:date)
}
ORDER BY DESC(?date)
LIMIT 100
```

## Partition Pruning

Iceberg's partition pruning optimizes queries:

```json
{
  "from": "warehouse-orders:main",
  "select": ["?orderId", "?total"],
  "where": [
    { "@id": "?order", "ex:orderId": "?orderId" },
    { "@id": "?order", "ex:total": "?total" },
    { "@id": "?order", "ex:orderDate": "?date" }
  ],
  "filter": "?date >= '2024-01-01' && ?date < '2024-02-01'"
}
```

If `orderDate` is a partition column, Iceberg only scans January 2024 partitions.

## Combining with Fluree Data

Join Iceberg data with Fluree ledgers:

```json
{
  "from": ["customers:main", "warehouse-orders:main"],
  "select": ["?customerName", "?orderTotal", "?orderDate"],
  "where": [
    { "@id": "?customer", "schema:name": "?customerName" },
    { "@id": "?customer", "ex:customerId": "?customerId" },
    { "@id": "?order", "ex:customerId": "?customerId" },
    { "@id": "?order", "ex:total": "?orderTotal" },
    { "@id": "?order", "ex:orderDate": "?orderDate" }
  ],
  "filter": "?orderDate >= '2024-01-01'",
  "orderBy": ["-?orderDate"]
}
```

Combines customer data from Fluree with order data from Iceberg.

## Time Travel

Query historical Iceberg snapshots:

```json
{
  "from": "warehouse-orders:main@snapshot:12345",
  "select": ["?orderId", "?total"],
  "where": [
    { "@id": "?order", "ex:orderId": "?orderId" },
    { "@id": "?order", "ex:total": "?total" }
  ]
}
```

Or by timestamp:

```json
{
  "from": "warehouse-orders:main@timestamp:2024-01-01T00:00:00Z",
  "select": ["?orderId", "?total"],
  "where": [...]
}
```

## Aggregations

Aggregate Iceberg data:

```sparql
PREFIX ex: <http://example.org/ns/>

SELECT ?date (SUM(?total) AS ?dailyRevenue) (COUNT(?order) AS ?orderCount)
FROM <warehouse-orders:main>
WHERE {
  ?order ex:orderDate ?date .
  ?order ex:total ?total .
  FILTER (?date >= "2024-01-01"^^xsd:date)
}
GROUP BY ?date
ORDER BY ?date
```

## Performance

### Query Planning

Fluree pushes filters to Iceberg:

```text
Query: SELECT ?id WHERE { ?order ex:orderDate ?date } FILTER (?date > "2024-01-01")
  ↓
Pushed to Iceberg:
  SELECT order_id FROM sales.orders WHERE order_date > '2024-01-01'
  ↓
Iceberg optimizations:
  - Partition pruning (only scan 2024 partitions)
  - File skipping (skip files outside date range)
  - Column pruning (only read order_id, order_date)
```

### Best Practices

1. **Partition by Common Filters:**
   ```sql
   -- Partition Iceberg table by date
   PARTITIONED BY (YEAR(order_date), MONTH(order_date))
   ```

2. **Use Filters:**
   ```json
   {
     "where": [...],
     "filter": "?date >= '2024-01-01'"  // Enables partition pruning
   }
   ```

3. **Limit Results:**
   ```json
   {
     "where": [...],
     "limit": 1000
   }
   ```

4. **Project Only Needed Columns:**
   ```json
   {
     "select": ["?orderId", "?total"],  // Only these columns read from Parquet
     "where": [...]
   }
   ```

## Schema Evolution

Iceberg supports schema evolution. Update virtual graph when schema changes:

```bash
curl -X POST http://localhost:8090/virtual-graph/warehouse-orders:main/refresh-schema
```

Fluree will reload Iceberg schema and update mapping.

## Configuration Options

### AWS Credentials

For S3-backed Iceberg:

```bash
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export AWS_REGION=us-east-1
```

Or configure in virtual graph:

```json
{
  "catalog": "glue",
  "warehouse": "s3://bucket/path/",
  "aws_credentials": {
    "access_key": "your-key",
    "secret_key": "your-secret",
    "region": "us-east-1"
  }
}
```

### Caching

Enable Parquet file caching:

```json
{
  "type": "iceberg",
  "cache": {
    "enabled": true,
    "max_size_mb": 1024,
    "ttl_seconds": 3600
  }
}
```

## Use Cases

### Analytics on Historical Data

Query years of historical data:

```sparql
SELECT ?year (SUM(?revenue) AS ?totalRevenue)
FROM <warehouse-sales:main>
WHERE {
  ?sale ex:year ?year .
  ?sale ex:revenue ?revenue .
  FILTER (?year >= 2020 && ?year <= 2023)
}
GROUP BY ?year
ORDER BY ?year
```

### Data Warehouse Integration

Combine real-time Fluree data with warehouse analytics:

```json
{
  "from": ["products:main", "warehouse-sales:main"],
  "select": ["?productName", "?totalSold"],
  "where": [
    { "@id": "?product", "schema:name": "?productName" },
    { "@id": "?product", "ex:productId": "?pid" },
    { "@id": "?sale", "ex:productId": "?pid" }
  ]
}
```

### Large-Scale Reporting

Generate reports from petabyte-scale data:

```sparql
SELECT ?region ?category (SUM(?amount) AS ?total)
FROM <warehouse-transactions:main>
WHERE {
  ?txn ex:region ?region .
  ?txn ex:category ?category .
  ?txn ex:amount ?amount .
  FILTER (?year = 2024)
}
GROUP BY ?region ?category
ORDER BY DESC(?total)
```

## Limitations

1. **Read-Only:** Iceberg virtual graphs are read-only (no writes via Fluree)
2. **Eventual Consistency:** May lag behind Iceberg table updates
3. **Complex Joins:** Large joins between Fluree and Iceberg may be slow
4. **No Full-Text Search:** Use Fluree's BM25 for text search

## Troubleshooting

### Connection Issues

```json
{
  "error": "IcebergConnectionError",
  "message": "Cannot connect to Glue catalog"
}
```

**Solutions:**
- Check AWS credentials
- Verify IAM permissions
- Check network connectivity

### Schema Mismatch

```json
{
  "error": "SchemaMismatchError",
  "message": "Column 'order_date' not found in Iceberg table"
}
```

**Solutions:**
- Refresh schema: `POST /virtual-graph/.../refresh-schema`
- Update mapping configuration
- Verify table name and catalog

### Slow Queries

**Causes:**
- Large result sets
- No partition pruning
- Scanning many files

**Solutions:**
- Add date filters to enable partition pruning
- Use LIMIT clause
- Optimize Iceberg table partitioning
- Use Iceberg file compaction

## Related Documentation

- [Virtual Graphs Overview](overview.md) - Virtual graph concepts
- [R2RML](r2rml.md) - Relational database mapping
- [Query Datasets](../query/datasets.md) - Multi-graph queries
