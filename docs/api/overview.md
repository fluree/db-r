# API Overview

The Fluree HTTP API provides a complete RESTful interface for database operations. This document provides a high-level overview of API design principles and capabilities.

## API Design Principles

### Resource-Oriented

The API is organized around resources:
- **Ledgers**: Database instances
- **Transactions**: Write operations
- **Queries**: Read operations
- **Commits**: Transaction history

### Standard HTTP Methods

Operations use standard HTTP methods:
- `GET` - Retrieve information (idempotent, cacheable)
- `POST` - Submit operations (transactions, queries)
- `PUT` - Update resources (planned)
- `DELETE` - Remove resources (planned)

### JSON-First

All request and response bodies use JSON by default:
- Native JSON-LD support
- Clean, readable syntax
- Easy integration with modern applications

### Stateless

All requests are stateless:
- No session management required
- Each request contains complete information
- Enables horizontal scaling

## Core Concepts

### Ledger Addressing

Ledgers are addressed using aliases with branch names:

```text
ledger-name:branch-name
```

Examples:
- `mydb:main` - Main branch of mydb ledger
- `customers:prod` - Production branch of customers ledger
- `tenant/app:dev` - Development branch with hierarchical naming

### Time Travel in URLs

Historical queries use time specifiers in ledger addresses:

```text
ledger:branch@t:100           # Transaction number
ledger:branch@iso:2024-01-15  # ISO timestamp
ledger:branch@sha:abc123      # Commit SHA
```

These work in all query contexts (FROM clauses, dataset specs, etc.).

### Content Type Negotiation

Request format determined by `Content-Type` header:
- `application/json` - JSON-LD (default)
- `application/sparql-query` - SPARQL
- `text/turtle` - Turtle RDF

Response format determined by `Accept` header:
- `application/json` - Compact JSON (default)
- `application/ld+json` - Full JSON-LD with context
- `application/sparql-results+json` - SPARQL result format

## API Endpoints

### Transaction Endpoints

**POST /transact**
- Submit transactions (insert/upsert/update)
- Parameters: `ledger`, `mode`, `context`
- Returns: Transaction receipt with commit info

### Query Endpoints

**POST /query**
- Execute queries (JSON-LD Query or SPARQL)
- Parameters: None (ledger specified in query body)
- Returns: Query results
- Supports history queries via time range in `from` clause (see [Time Travel](../concepts/time-travel.md))

**POST /nameservice/query**
- Query metadata about all ledgers and virtual graphs
- Parameters: None (query in request body)
- Returns: Query results over nameservice records

### Ledger Management

**GET /ledgers**
- List all ledgers
- Parameters: None
- Returns: Array of ledger metadata

**GET /ledgers/:alias**
- Get specific ledger metadata
- Parameters: `alias` (ledger:branch)
- Returns: Ledger details (commit_t, index_t, etc.)

**POST /ledgers**
- Create a new ledger explicitly
- Parameters: `alias`, `config`
- Returns: Ledger metadata
- Note: The HTTP server also supports implicit creation on first transaction; the Rust library API requires explicit `create_ledger` before transacting

### System Endpoints

**GET /health**
- Health check endpoint
- Parameters: None
- Returns: Server health status

**GET /status**
- Server status and statistics
- Parameters: None
- Returns: Detailed server state

**GET /version**
- Server version information
- Parameters: None
- Returns: Version details

## Request Format

### URL Structure

```text
https://[host]:[port]/[endpoint]?[parameters]
```

Example:
```text
http://localhost:8090/transact?ledger=mydb:main&mode=replace
```

### Query Parameters

Common parameters:
- `ledger` - Target ledger (format: `name:branch`)
- `mode` - Transaction mode (`default`, `replace`)
- `context` - Default context URL
- `format` - Response format override

### Request Headers

Essential headers:
```http
Content-Type: application/json
Accept: application/json
Authorization: Bearer [token]
```

See [Headers](headers.md) for complete list.

### Request Body

JSON-LD format for transactions:

```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "@graph": [
    { "@id": "ex:alice", "ex:name": "Alice" }
  ]
}
```

JSON-LD Query format:

```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "from": "mydb:main",
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ]
}
```

## Response Format

### Success Response

Successful operations return appropriate status codes with JSON bodies.

**Transaction Response:**
```json
{
  "t": 5,
  "timestamp": "2024-01-22T10:30:00.000Z",
  "commit_sha": "abc123def456...",
  "address": "fluree:memory:commit:abc123...",
  "flakes_added": 3,
  "flakes_retracted": 1
}
```

**Query Response:**
```json
[
  { "name": "Alice" },
  { "name": "Bob" }
]
```

### Error Response

Errors return appropriate HTTP status codes with structured error objects:

```json
{
  "error": "TransactionError",
  "message": "Invalid IRI: not a valid URI",
  "code": "INVALID_IRI",
  "details": {
    "iri": "not a uri",
    "line": 5
  }
}
```

See [Errors and Status Codes](errors.md) for complete error reference.

## Authentication

### Development Mode

No authentication required (default for local development):

```bash
curl http://localhost:8090/query -d '{...}'
```

### Production Modes

#### API Key Authentication

Simple token-based authentication:

```bash
curl http://api.example.com/query \
  -H "X-API-Key: your-api-key" \
  -d '{...}'
```

#### Bearer Token Authentication

JWT-based authentication:

```bash
curl http://api.example.com/query \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..." \
  -d '{...}'
```

#### Signed Requests (JWS/VC)

Cryptographically signed requests using JSON Web Signatures or Verifiable Credentials:

```bash
curl http://api.example.com/query \
  -H "Content-Type: application/jose" \
  -d '{
    "payload": "[base64-encoded-request]",
    "signatures": [{
      "protected": "[base64-encoded-header]",
      "signature": "[base64-encoded-signature]"
    }]
  }'
```

See [Signed Requests](signed-requests.md) for detailed documentation.

## Rate Limiting

### Default Limits

Production deployments should implement rate limiting:
- Queries: 100 requests per minute
- Transactions: 10 requests per minute
- History: 50 requests per minute

### Rate Limit Headers

Responses include rate limit information:

```http
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1642857600
```

### Exceeding Limits

When limits are exceeded:
- Status code: `429 Too Many Requests`
- Response body includes retry information
- `Retry-After` header indicates wait time

## API Versioning

### Current Version

The current API is version 1 (v1).

### Version in URL (Future)

Future versions may use URL-based versioning:

```text
https://api.example.com/v2/query
```

### Backward Compatibility

The API maintains backward compatibility within major versions:
- New endpoints may be added
- New optional parameters may be added
- Response format remains compatible
- Breaking changes trigger major version bump

## Common Patterns

### Idempotent Writes

Use upsert mode for idempotent transactions:

```bash
curl -X POST "http://localhost:8090/transact?ledger=mydb:main&mode=replace" \
  -H "Content-Type: application/json" \
  -d '{...}'
```

### Batch Operations

Submit multiple entities in a single transaction:

```json
{
  "@graph": [
    { "@id": "ex:alice", "ex:name": "Alice" },
    { "@id": "ex:bob", "ex:name": "Bob" },
    { "@id": "ex:carol", "ex:name": "Carol" }
  ]
}
```

### Conditional Updates

Use WHERE/DELETE/INSERT for conditional changes:

```json
{
  "where": [
    { "@id": "ex:alice", "ex:age": "?oldAge" }
  ],
  "delete": [
    { "@id": "ex:alice", "ex:age": "?oldAge" }
  ],
  "insert": [
    { "@id": "ex:alice", "ex:age": 31 }
  ]
}
```

### Historical Queries

Query past states using time specifiers:

```json
{
  "from": "mydb:main@t:100",
  "select": ["?name"],
  "where": [
    { "@id": "?person", "ex:name": "?name" }
  ]
}
```

## Best Practices

### 1. Use Appropriate HTTP Methods

- GET for read-only operations (health, status)
- POST for write and query operations

### 2. Set Correct Content-Type

Always specify the request format:

```http
Content-Type: application/json
```

### 3. Handle Errors Gracefully

Check status codes and parse error responses:

```javascript
if (response.status !== 200) {
  const error = await response.json();
  console.error(`Error ${error.code}: ${error.message}`);
}
```

### 4. Use Connection Pooling

Reuse HTTP connections for better performance:

```javascript
const agent = new https.Agent({ keepAlive: true });
```

### 5. Implement Retry Logic

Retry failed requests with exponential backoff:

```javascript
async function retryRequest(fn, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await fn();
    } catch (err) {
      if (i === maxRetries - 1) throw err;
      await sleep(Math.pow(2, i) * 1000);
    }
  }
}
```

### 6. Monitor Rate Limits

Track rate limit headers and back off when approaching limits.

### 7. Use Compression

Enable compression for large payloads:

```http
Accept-Encoding: gzip, deflate
```

## Security Considerations

### HTTPS in Production

Always use HTTPS in production:
- Prevents eavesdropping
- Protects credentials
- Enables trust

### Validate Input

Validate all user input before sending to API:
- Check IRI formats
- Validate JSON structure
- Sanitize user data

### Secure Credentials

Never expose credentials in code or logs:
- Use environment variables
- Rotate keys regularly
- Use signed requests for highest security

### Implement CORS Carefully

If exposing API to web applications, configure CORS appropriately:

```http
Access-Control-Allow-Origin: https://your-app.com
Access-Control-Allow-Methods: POST, GET
Access-Control-Allow-Headers: Content-Type, Authorization
```

## Performance Tips

### 1. Batch Related Operations

Combine related entities in single transactions for better performance.

### 2. Use Appropriate Time Specifiers

- `@t:NNN` is fastest (direct lookup)
- `@iso:DATETIME` requires binary search
- `@sha:PREFIX` requires scan (use 7+ character prefixes)

### 3. Limit Result Sets

Always use LIMIT for potentially large result sets:

```json
{
  "select": ["?name"],
  "where": [...],
  "limit": 100
}
```

### 4. Cache Historical Queries

Historical queries (with time specifiers) are immutable and cache well.

### 5. Use Streaming for Large Results

For very large result sets, consider streaming responses (when supported).

## Related Documentation

- [Endpoints](endpoints.md) - Complete endpoint reference
- [Headers](headers.md) - HTTP headers and content types
- [Signed Requests](signed-requests.md) - Cryptographic authentication
- [Errors](errors.md) - Error codes and troubleshooting
