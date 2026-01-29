# Tracking and Fuel Limits

Fluree provides query tracking and fuel limits to monitor and control query execution, ensuring system stability and performance.

## Query Tracking

Query tracking provides visibility into query execution, helping you understand query behavior and performance.

### Enable Tracking

Enable tracking in queries:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ],
  "track": true
}
```

### Tracked Information

Tracking provides:
- **Execution Time**: Query execution duration
- **Rows Processed**: Number of rows processed
- **Index Scans**: Number of index scans performed
- **Joins**: Number of joins executed
- **Filters Applied**: Number of filters evaluated

## Fuel Limits

Fuel limits control resource consumption, preventing runaway queries from consuming excessive resources.

### What Is Fuel?

Fuel is a measure of query execution cost:
- **Index Scans**: Each scan consumes fuel
- **Joins**: Each join consumes fuel
- **Filter Evaluations**: Each filter consumes fuel
- **Row Processing**: Each row processed consumes fuel

### Setting Fuel Limits

Set fuel limits in queries:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ],
  "fuel": 10000
}
```

### Fuel Limit Behavior

When fuel limit is exceeded:
- Query execution stops
- Error returned to client
- Partial results not returned

## Best Practices

### Tracking

1. **Enable for Debugging**: Use tracking to debug slow queries
2. **Monitor Performance**: Track query performance over time
3. **Identify Bottlenecks**: Use tracking to identify performance bottlenecks

### Fuel Limits

1. **Set Appropriate Limits**: Set fuel limits based on expected query complexity
2. **Monitor Fuel Usage**: Track fuel usage to optimize queries
3. **Prevent Runaway Queries**: Use fuel limits to prevent resource exhaustion

## Related Documentation

- [JSON-LD Query](jsonld-query.md): JSON-LD Query syntax
- [SPARQL](sparql.md): SPARQL syntax
- [Explain Plans](explain.md): Query execution plans
