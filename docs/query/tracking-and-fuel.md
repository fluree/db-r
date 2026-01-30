# Tracking and Fuel Limits

Fluree provides query tracking and fuel limits to monitor and control query execution, ensuring system stability and performance.

## Query Tracking

Query tracking provides visibility into query execution, helping you understand query behavior and performance.

### Enable Tracking

Enable tracking via the `opts` object. Use `"meta": true` to enable all tracking, or selectively enable specific metrics:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ],
  "opts": { "meta": true }
}
```

Or enable specific metrics:

```json
{
  "opts": {
    "meta": {
      "time": true,
      "fuel": true,
      "policy": true
    }
  }
}
```

### Tracked Information

Tracking provides:
- **time**: Query execution duration (formatted as "12.34ms")
- **fuel**: Number of items/flakes processed
- **policy**: Policy evaluation statistics (`{policy-id: {executed: N, allowed: M}}`)

## Fuel Limits

Fuel limits control resource consumption, preventing runaway queries from consuming excessive resources.

### What Is Fuel?

Fuel is a measure of query execution cost. One unit of fuel is consumed for each item emitted:
- Each flake/triple matched during index scans
- Each item expanded during graph crawl formatting
- Each non-schema flake staged during transactions

### Setting Fuel Limits

Set fuel limits via `opts.max-fuel`. Setting a fuel limit implicitly enables fuel tracking:

```json
{
  "@context": { "ex": "http://example.org/ns/" },
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ],
  "opts": { "max-fuel": 10000 }
}
```

You can also use `"maxFuel"` or `"max_fuel"` as alternative key names.

### Fuel Limit Behavior

When fuel limit is exceeded:
- Query execution stops
- Error returned to client
- Partial results not returned

## Response Format

When tracking is enabled, the response includes tracking information as top-level siblings:

```json
{
  "status": 200,
  "result": [...],
  "time": "12.34ms",
  "fuel": 42,
  "policy": {
    "http://example.org/myPolicy": {
      "executed": 10,
      "allowed": 8
    }
  }
}
```

## Tracking and Deep Tracing

When query tracking is enabled (via `opts.meta` or `opts.track`), the
tracking metrics are also recorded as fields on the `query_execute` tracing
span. This means they appear in Jaeger, Tempo, or any OTEL-compatible backend
alongside the full span waterfall.

| Span field | Source | Description |
|------------|--------|-------------|
| `tracker_time` | `tally.time` | Query execution duration (e.g., `"12.34ms"`) |
| `tracker_fuel` | `tally.fuel` | Total items processed (flakes scanned + graph nodes crawled) |

This is useful for correlating tracking data with the detailed phase breakdown
provided by deep tracing. For example, you can search Jaeger for traces where
`tracker_fuel > 100000` to find expensive queries, then drill into their span
waterfalls to see which phase (scanning, joining, formatting, etc.) consumed
the most time.

When tracking is not enabled, these span fields remain empty and do not appear
in trace output.

For a full walkthrough of using tracing for performance investigation, see the
[Performance Investigation](../troubleshooting/performance-investigation.md)
guide.

## Best Practices

### Tracking

1. **Enable for Debugging**: Use `"opts": {"meta": true}` to debug slow queries
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
