# Errors and Status Codes

This document provides a complete reference for HTTP status codes and error responses in the Fluree API.

## Error Response Format

All errors return a consistent JSON structure:

```json
{
  "error": "ErrorType",
  "message": "Human-readable error description",
  "code": "MACHINE_READABLE_CODE",
  "details": {
    "field": "Additional context"
  }
}
```

**Fields:**
- `error`: Error type (class of error)
- `message`: Human-readable description
- `code`: Machine-readable error code for programmatic handling
- `details`: Optional additional context (varies by error)

## HTTP Status Codes

### Success Codes (2xx)

#### 200 OK

The request succeeded.

**Used for:**
- Successful queries
- Successful transactions
- Successful GET requests

**Example:**
```json
{
  "t": 5,
  "timestamp": "2024-01-22T10:30:00.000Z",
  "commit_sha": "abc123..."
}
```

#### 201 Created

A new resource was created.

**Used for:**
- Ledger creation
- Index creation

**Example:**
```json
{
  "alias": "mydb:main",
  "created": "2024-01-22T10:00:00.000Z"
}
```

#### 204 No Content

Request succeeded with no response body.

**Used for:**
- DELETE operations
- Administrative commands

### Client Error Codes (4xx)

#### 400 Bad Request

The request is malformed or contains invalid data.

**Common Causes:**
- Invalid JSON syntax
- Invalid JSON-LD structure
- Invalid SPARQL syntax
- Invalid IRI format
- Type mismatch

**Error Codes:**
- `PARSE_ERROR` - JSON or SPARQL parsing failed
- `INVALID_IRI` - Malformed IRI
- `INVALID_CONTEXT` - Invalid JSON-LD @context
- `TYPE_ERROR` - Type mismatch
- `CONSTRAINT_VIOLATION` - Data constraint violated

**Example:**
```json
{
  "error": "ParseError",
  "message": "Invalid JSON-LD: unexpected token at line 5",
  "code": "PARSE_ERROR",
  "details": {
    "line": 5,
    "column": 12,
    "token": "}"
  }
}
```

**How to Fix:**
- Validate JSON syntax
- Check IRI formats
- Verify JSON-LD structure
- Review error details for specific issue

#### 401 Unauthorized

Authentication is required but not provided or invalid.

**Common Causes:**
- Missing authentication credentials
- Invalid API key
- Expired JWT token
- Invalid signature (for signed requests)

**Error Codes:**
- `MISSING_AUTH` - No authentication provided
- `INVALID_API_KEY` - API key not valid
- `TOKEN_EXPIRED` - JWT token expired
- `INVALID_SIGNATURE` - Signature verification failed

**Example:**
```json
{
  "error": "Unauthorized",
  "message": "Authentication required",
  "code": "MISSING_AUTH"
}
```

**How to Fix:**
- Provide valid authentication credentials
- Check API key or token
- Renew expired tokens
- Verify signature process for signed requests

#### 403 Forbidden

Authentication succeeded but authorization failed.

**Common Causes:**
- Insufficient permissions for operation
- Policy denies access
- Ledger access restricted

**Error Codes:**
- `FORBIDDEN` - Operation not allowed
- `POLICY_DENIED` - Policy explicitly denies access
- `INSUFFICIENT_PERMISSIONS` - User lacks required permissions

**Example:**
```json
{
  "error": "Forbidden",
  "message": "Policy denies access to ledger mydb:main",
  "code": "POLICY_DENIED",
  "details": {
    "ledger": "mydb:main",
    "subject": "did:key:z6Mkh...",
    "action": "transact"
  }
}
```

**How to Fix:**
- Verify user has required permissions
- Check policy configuration
- Contact administrator for access

#### 404 Not Found

The requested resource doesn't exist.

**Common Causes:**
- Ledger doesn't exist
- Entity not found
- Endpoint doesn't exist

**Error Codes:**
- `LEDGER_NOT_FOUND` - Ledger doesn't exist
- `ENTITY_NOT_FOUND` - Entity not found
- `RESOURCE_NOT_FOUND` - General resource not found

**Example:**
```json
{
  "error": "NotFound",
  "message": "Ledger not found: mydb:main",
  "code": "LEDGER_NOT_FOUND",
  "details": {
    "ledger": "mydb:main"
  }
}
```

**How to Fix:**
- Verify ledger name spelling
- Check if ledger was created
- Verify entity IRI

#### 408 Request Timeout

The request took too long to process.

**Common Causes:**
- Query timeout exceeded
- Complex query taking too long
- Database under heavy load

**Error Codes:**
- `QUERY_TIMEOUT` - Query execution timeout
- `TRANSACTION_TIMEOUT` - Transaction processing timeout

**Example:**
```json
{
  "error": "Timeout",
  "message": "Query execution exceeded timeout of 30000ms",
  "code": "QUERY_TIMEOUT",
  "details": {
    "timeout_ms": 30000,
    "elapsed_ms": 31245
  }
}
```

**How to Fix:**
- Simplify query
- Add more specific filters
- Use LIMIT clause
- Increase timeout setting
- Check server load

#### 409 Conflict

The request conflicts with current server state.

**Common Causes:**
- Concurrent modification conflict
- Ledger already exists
- Resource state conflict

**Error Codes:**
- `CONFLICT` - General conflict
- `LEDGER_EXISTS` - Ledger already exists
- `CONCURRENT_MODIFICATION` - Concurrent update conflict

**Example:**
```json
{
  "error": "Conflict",
  "message": "Ledger already exists: mydb:main",
  "code": "LEDGER_EXISTS",
  "details": {
    "ledger": "mydb:main"
  }
}
```

**How to Fix:**
- Use different ledger name
- Handle concurrent modifications with retry logic
- Check resource state before modifying

#### 413 Payload Too Large

The request or response exceeds size limits.

**Common Causes:**
- Transaction too large
- Query result too large
- Request body exceeds limit

**Error Codes:**
- `PAYLOAD_TOO_LARGE` - Request too large
- `RESPONSE_TOO_LARGE` - Result set too large

**Example:**
```json
{
  "error": "PayloadTooLarge",
  "message": "Transaction exceeds maximum size of 10485760 bytes",
  "code": "PAYLOAD_TOO_LARGE",
  "details": {
    "max_size": 10485760,
    "actual_size": 15000000
  }
}
```

**How to Fix:**
- Split large transactions into batches
- Use LIMIT clause for queries
- Use pagination for large result sets
- Increase size limits (if appropriate)

#### 415 Unsupported Media Type

The Content-Type is not supported.

**Common Causes:**
- Wrong Content-Type header
- Unsupported format
- Missing Content-Type header

**Error Codes:**
- `UNSUPPORTED_MEDIA_TYPE` - Content-Type not supported

**Example:**
```json
{
  "error": "UnsupportedMediaType",
  "message": "Content-Type not supported: text/plain",
  "code": "UNSUPPORTED_MEDIA_TYPE",
  "details": {
    "provided": "text/plain",
    "supported": ["application/json", "application/sparql-query", "text/turtle"]
  }
}
```

**How to Fix:**
- Set correct Content-Type header
- Use supported format
- Check API documentation for supported types

#### 422 Unprocessable Entity

The request is well-formed but semantically invalid.

**Common Causes:**
- Invalid data values
- Business rule violation
- Semantic constraint violation

**Error Codes:**
- `VALIDATION_ERROR` - Data validation failed
- `SEMANTIC_ERROR` - Semantic constraint violated

**Example:**
```json
{
  "error": "ValidationError",
  "message": "Invalid email format",
  "code": "VALIDATION_ERROR",
  "details": {
    "field": "schema:email",
    "value": "not-an-email",
    "constraint": "must be valid email"
  }
}
```

**How to Fix:**
- Validate data before submitting
- Check business rules
- Review constraint requirements

#### 429 Too Many Requests

Rate limit exceeded.

**Common Causes:**
- Too many requests in time window
- Exceeded quota

**Error Codes:**
- `RATE_LIMIT_EXCEEDED` - Too many requests

**Example:**
```json
{
  "error": "RateLimitExceeded",
  "message": "Rate limit exceeded: 100 requests per minute",
  "code": "RATE_LIMIT_EXCEEDED",
  "details": {
    "limit": 100,
    "window_seconds": 60,
    "retry_after": 45
  }
}
```

**Response Headers:**
```http
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1642857645
Retry-After: 45
```

**How to Fix:**
- Wait before retrying (check Retry-After header)
- Implement exponential backoff
- Reduce request rate
- Request higher rate limit

### Server Error Codes (5xx)

#### 500 Internal Server Error

An unexpected error occurred on the server.

**Common Causes:**
- Unhandled exception
- Database error
- Internal logic error

**Error Codes:**
- `INTERNAL_ERROR` - General internal error
- `DATABASE_ERROR` - Database operation failed

**Example:**
```json
{
  "error": "InternalError",
  "message": "An unexpected error occurred",
  "code": "INTERNAL_ERROR",
  "details": {
    "request_id": "abc-123-def-456"
  }
}
```

**How to Fix:**
- Check server logs
- Report to system administrator
- Retry request
- Contact support if persists

#### 502 Bad Gateway

Error communicating with upstream service.

**Common Causes:**
- Storage backend unavailable
- Nameservice unavailable
- Network error

**Error Codes:**
- `BAD_GATEWAY` - Upstream service error
- `STORAGE_ERROR` - Storage backend error

**Example:**
```json
{
  "error": "BadGateway",
  "message": "Cannot connect to storage backend",
  "code": "STORAGE_ERROR",
  "details": {
    "backend": "s3",
    "error": "Connection timeout"
  }
}
```

**How to Fix:**
- Check storage backend status
- Verify network connectivity
- Check AWS/cloud service status
- Retry with backoff

#### 503 Service Unavailable

The server is temporarily unavailable.

**Common Causes:**
- Server overloaded
- Maintenance mode
- Resource exhaustion

**Error Codes:**
- `SERVICE_UNAVAILABLE` - Service temporarily down
- `OVERLOADED` - Server at capacity
- `MAINTENANCE` - Maintenance mode

**Example:**
```json
{
  "error": "ServiceUnavailable",
  "message": "Server is under maintenance",
  "code": "MAINTENANCE",
  "details": {
    "retry_after": 300
  }
}
```

**Response Headers:**
```http
Retry-After: 300
```

**How to Fix:**
- Wait and retry (check Retry-After header)
- Implement retry logic with exponential backoff
- Check service status page

#### 504 Gateway Timeout

Upstream service didn't respond in time.

**Common Causes:**
- Storage backend timeout
- Long-running query
- Network latency

**Error Codes:**
- `GATEWAY_TIMEOUT` - Upstream timeout

**Example:**
```json
{
  "error": "GatewayTimeout",
  "message": "Storage backend did not respond in time",
  "code": "GATEWAY_TIMEOUT"
}
```

**How to Fix:**
- Retry request
- Check storage backend performance
- Simplify query
- Increase timeout settings

## Error Codes Reference

### Transaction Errors

| Code | Description | HTTP Status |
|------|-------------|-------------|
| `PARSE_ERROR` | Invalid JSON-LD syntax | 400 |
| `INVALID_IRI` | Malformed IRI | 400 |
| `INVALID_CONTEXT` | Invalid @context | 400 |
| `TYPE_ERROR` | Type mismatch | 400 |
| `CONSTRAINT_VIOLATION` | Data constraint violated | 422 |
| `LEDGER_NOT_FOUND` | Ledger doesn't exist | 404 |
| `TRANSACTION_TIMEOUT` | Transaction took too long | 408 |
| `PAYLOAD_TOO_LARGE` | Transaction too large | 413 |

### Query Errors

| Code | Description | HTTP Status |
|------|-------------|-------------|
| `PARSE_ERROR` | Invalid query syntax | 400 |
| `INVALID_FILTER` | Invalid filter expression | 400 |
| `LEDGER_NOT_FOUND` | Ledger doesn't exist | 404 |
| `QUERY_TIMEOUT` | Query execution timeout | 408 |
| `RESPONSE_TOO_LARGE` | Result set too large | 413 |
| `FUEL_EXHAUSTED` | Query fuel limit exceeded | 503 |

### Authentication/Authorization Errors

| Code | Description | HTTP Status |
|------|-------------|-------------|
| `MISSING_AUTH` | No credentials provided | 401 |
| `INVALID_API_KEY` | API key invalid | 401 |
| `TOKEN_EXPIRED` | JWT token expired | 401 |
| `INVALID_SIGNATURE` | Signature verification failed | 401 |
| `FORBIDDEN` | Operation not allowed | 403 |
| `POLICY_DENIED` | Policy denies access | 403 |
| `INSUFFICIENT_PERMISSIONS` | Lacks permissions | 403 |

### System Errors

| Code | Description | HTTP Status |
|------|-------------|-------------|
| `INTERNAL_ERROR` | Unexpected server error | 500 |
| `DATABASE_ERROR` | Database operation failed | 500 |
| `STORAGE_ERROR` | Storage backend error | 502 |
| `SERVICE_UNAVAILABLE` | Service temporarily down | 503 |
| `OVERLOADED` | Server at capacity | 503 |
| `MAINTENANCE` | Maintenance mode | 503 |
| `GATEWAY_TIMEOUT` | Upstream timeout | 504 |

## Error Handling Best Practices

### 1. Always Check Status Codes

Check HTTP status before parsing response:

```javascript
const response = await fetch(url, options);
if (!response.ok) {
  const error = await response.json();
  throw new Error(`${error.code}: ${error.message}`);
}
```

### 2. Implement Retry Logic

Retry transient errors with exponential backoff:

```javascript
async function retryRequest(fn, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await fn();
    } catch (err) {
      if (!isRetryable(err) || i === maxRetries - 1) {
        throw err;
      }
      await sleep(Math.pow(2, i) * 1000);
    }
  }
}

function isRetryable(err) {
  return [408, 429, 502, 503, 504].includes(err.status);
}
```

### 3. Handle Rate Limits

Respect rate limit headers:

```javascript
if (response.status === 429) {
  const retryAfter = response.headers.get('Retry-After');
  await sleep(retryAfter * 1000);
  return retryRequest(fn);
}
```

### 4. Log Error Details

Log complete error context for debugging:

```javascript
console.error({
  status: response.status,
  error: errorData.error,
  code: errorData.code,
  message: errorData.message,
  details: errorData.details,
  requestId: response.headers.get('X-Request-ID')
});
```

### 5. User-Friendly Messages

Show appropriate messages to users:

```javascript
function getUserMessage(error) {
  switch (error.code) {
    case 'LEDGER_NOT_FOUND':
      return 'Database not found. Please check the name.';
    case 'QUERY_TIMEOUT':
      return 'Query took too long. Please try a simpler query.';
    case 'RATE_LIMIT_EXCEEDED':
      return 'Too many requests. Please wait a moment.';
    default:
      return 'An error occurred. Please try again.';
  }
}
```

### 6. Graceful Degradation

Handle errors gracefully:

```javascript
try {
  const data = await query(ledger);
  return data;
} catch (err) {
  if (err.code === 'LEDGER_NOT_FOUND') {
    // Create ledger and retry
    await createLedger(ledger);
    return await query(ledger);
  }
  throw err;
}
```

### 7. Circuit Breaker Pattern

Prevent cascading failures:

```javascript
class CircuitBreaker {
  constructor(threshold = 5, timeout = 60000) {
    this.failures = 0;
    this.threshold = threshold;
    this.timeout = timeout;
    this.state = 'CLOSED';
  }
  
  async execute(fn) {
    if (this.state === 'OPEN') {
      throw new Error('Circuit breaker is OPEN');
    }
    
    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (err) {
      this.onFailure();
      throw err;
    }
  }
  
  onSuccess() {
    this.failures = 0;
    this.state = 'CLOSED';
  }
  
  onFailure() {
    this.failures++;
    if (this.failures >= this.threshold) {
      this.state = 'OPEN';
      setTimeout(() => {
        this.state = 'HALF_OPEN';
        this.failures = 0;
      }, this.timeout);
    }
  }
}
```

## Related Documentation

- [Overview](overview.md) - API overview
- [Endpoints](endpoints.md) - API endpoints
- [Signed Requests](signed-requests.md) - Authentication
- [Troubleshooting](../troubleshooting/README.md) - General troubleshooting
- [Common Errors](../troubleshooting/common-errors.md) - Common error solutions
