# Policy in Transactions

Policies are enforced during transaction processing, validating that users have permission to write data. This document explains how policies affect write operations.

## Transaction-Time Authorization

When a transaction is submitted, Fluree:
1. Identifies the subject (from signed transaction)
2. Parses the transaction
3. Collects applicable policies
4. Validates each assertion/retraction
5. Rejects transaction if any operation is unauthorized

**Unauthorized writes are rejected entirely.**

## Basic Example

### Without Policy

Transaction:
```json
{
  "@graph": [
    { "@id": "ex:alice", "schema:age": 31 }
  ]
}
```

Result: Success (no restrictions)

### With Policy

Policy (owner-only writes):
```json
{
  "db:subject": "?user",
  "db:action": "transact",
  "db:resource": {
    "ex:owner": "?user"
  },
  "db:allow": true
}
```

Transaction from different user:
```json
{
  "@graph": [
    { "@id": "ex:alice", "schema:age": 31 }
  ]
}
```

Result: **REJECTED** (ex:alice not owned by user)

## Authorization Points

Policies check authorization at multiple points:

### 1. Entity Creation

Creating new entities:

Policy:
```json
{
  "db:subject": { "ex:role": "admin" },
  "db:action": "transact",
  "db:resource": { "@type": "ex:User" },
  "db:allow": true
}
```

Only admins can create User entities.

### 2. Property Updates

Updating existing properties:

Policy:
```json
{
  "db:subject": "?user",
  "db:action": "transact",
  "db:resource": {
    "@id": "?entity",
    "ex:owner": "?user"
  },
  "db:allow": true
}
```

Users can only update entities they own.

### 3. Property Addition

Adding new properties:

Policy:
```json
{
  "db:subject": "*",
  "db:action": "transact",
  "db:resource": {
    "db:predicate": "ex:verified"
  },
  "db:allow": false
}
```

Nobody can set "verified" flag (except admins via separate policy).

### 4. Retractions

Removing data:

Policy:
```json
{
  "db:subject": { "ex:role": "admin" },
  "db:action": "transact",
  "db:operation": "retract",
  "db:allow": true
}
```

Only admins can retract data.

## Transaction Validation

### Per-Triple Validation

Each triple is validated independently:

Transaction:
```json
{
  "@graph": [
    { "@id": "ex:doc1", "schema:title": "Public Doc" },
    { "@id": "ex:doc2", "schema:title": "Private Doc" }
  ]
}
```

If user can create ex:doc1 but not ex:doc2, entire transaction rejected.

### Atomic Transactions

Transactions are atomic:
- All operations must be authorized
- One unauthorized operation = entire transaction rejected
- No partial commits

## Operation Types

### Insert Operations

Policy for inserts:

```json
{
  "db:subject": "*",
  "db:action": "transact",
  "db:operation": "assert",
  "db:resource": { "@type": "ex:PublicData" },
  "db:allow": true
}
```

Anyone can insert public data.

### Update Operations

Policy for updates (retract + assert):

```json
{
  "db:subject": "?user",
  "db:action": "transact",
  "db:operation": ["retract", "assert"],
  "db:resource": {
    "ex:author": "?user"
  },
  "db:allow": true
}
```

Users can update data they authored.

### Delete Operations

Policy for retractions:

```json
{
  "db:subject": { "ex:role": "moderator" },
  "db:action": "transact",
  "db:operation": "retract",
  "db:allow": true
}
```

Only moderators can delete data.

## Property-Level Authorization

### Restricting Specific Properties

Prevent writes to sensitive properties:

```json
{
  "db:subject": "*",
  "db:action": "transact",
  "db:resource": {
    "db:predicate": "ex:balance"
  },
  "db:allow": false
}
```

Nobody can directly modify balance (must use specific API).

### Whitelist Approach

Allow only specific properties:

```json
{
  "db:subject": "?user",
  "db:action": "transact",
  "db:resource": {
    "@id": "?user",
    "db:predicate": ["schema:name", "schema:email", "schema:telephone"]
  },
  "db:allow": true
}
```

Users can only update their name, email, and phone.

## Entity-Level Authorization

### Owner-Based Access

```json
{
  "db:subject": "?user",
  "db:action": "transact",
  "db:resource": {
    "@id": "?entity",
    "ex:owner": "?user"
  },
  "db:allow": true
}
```

### Creator Rights

```json
{
  "db:subject": "?user",
  "db:action": "transact",
  "db:resource": {
    "@id": "?entity",
    "ex:createdBy": "?user"
  },
  "db:allow": true
}
```

### Hierarchical Permissions

```json
{
  "db:subject": "?manager",
  "db:action": "transact",
  "db:resource": {
    "ex:reportsTo": "?manager"
  },
  "db:allow": true
}
```

Managers can modify records of their reports.

## Conditional Authorization

### Status-Based

```json
{
  "db:subject": "?user",
  "db:action": "transact",
  "db:resource": {
    "@id": "?doc",
    "ex:status": "draft"
  },
  "db:condition": [
    { "@id": "?doc", "ex:author": "?user" }
  ],
  "db:allow": true
}
```

Authors can modify documents only while in draft status.

### Time-Based

```json
{
  "db:subject": "?user",
  "db:action": "transact",
  "db:resource": {
    "ex:submittedAt": "?submitTime"
  },
  "db:condition": [
    { "db:filter": "NOW() - ?submitTime < 3600" }
  ],
  "db:allow": true
}
```

Can modify submission within 1 hour.

### Value-Based

```json
{
  "db:subject": "?user",
  "db:action": "transact",
  "db:resource": {
    "ex:amount": "?amount"
  },
  "db:condition": [
    { "db:filter": "?amount <= 1000" },
    { "@id": "?user", "ex:approvalLimit": "?limit" },
    { "db:filter": "?amount <= ?limit" }
  ],
  "db:allow": true
}
```

Users can approve transactions up to their limit.

## Replace Mode (Upsert)

Policy evaluation with replace mode:

```bash
POST /transact?ledger=mydb:main&mode=replace
```

Fluree checks:
1. Permission to retract existing triples
2. Permission to assert new triples
3. Both must be authorized

Policy:
```json
{
  "db:subject": "?user",
  "db:action": "transact",
  "db:operation": ["retract", "assert"],
  "db:resource": {
    "ex:owner": "?user"
  },
  "db:allow": true
}
```

## WHERE/DELETE/INSERT Updates

Policy evaluation for updates:

Transaction:
```json
{
  "where": [
    { "@id": "ex:alice", "schema:age": "?oldAge" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:age": "?oldAge" }
  ],
  "insert": [
    { "@id": "ex:alice", "schema:age": 32 }
  ]
}
```

Fluree checks:
1. Permission to query (WHERE clause)
2. Permission to retract (DELETE clause)
3. Permission to assert (INSERT clause)

## Error Responses

### Unauthorized Transaction

```json
{
  "error": "Forbidden",
  "message": "Policy denies transact on ex:alice",
  "code": "POLICY_DENIED",
  "details": {
    "subject": "did:key:z6Mkh...",
    "action": "transact",
    "resource": "ex:alice",
    "policy_evaluated": [
      {
        "id": "ex:owner-policy",
        "matched": true,
        "condition_met": false,
        "decision": "deny"
      }
    ]
  }
}
```

### Property Not Allowed

```json
{
  "error": "Forbidden",
  "message": "Not authorized to modify property ex:verified",
  "code": "PROPERTY_DENIED",
  "details": {
    "subject": "did:key:z6Mkh...",
    "entity": "ex:alice",
    "predicate": "ex:verified",
    "operation": "assert"
  }
}
```

## Signed Transactions

Link transaction to identity:

```javascript
const transaction = {
  "@graph": [
    { "@id": "ex:alice", "schema:name": "Alice" }
  ]
};

const signedTxn = await signTransaction(transaction, privateKey);

await fetch('http://localhost:8090/transact?ledger=mydb:main', {
  method: 'POST',
  headers: { 'Content-Type': 'application/jose' },
  body: signedTxn
});
```

Policy uses signer's DID for authorization.

## Provenance Tracking

Policy can enforce provenance:

```json
{
  "db:subject": "?user",
  "db:action": "transact",
  "db:resource": {
    "@id": "?entity"
  },
  "db:condition": [
    { "@id": "?entity", "ex:createdBy": "?user" }
  ],
  "db:allow": true,
  "db:augment": [
    { "@id": "?entity", "ex:modifiedBy": "?user" },
    { "@id": "?entity", "ex:modifiedAt": "NOW()" }
  ]
}
```

Automatically adds modification metadata.

## Common Patterns

### Create Own, Edit Own

```json
[
  {
    "@id": "ex:create-policy",
    "db:subject": "*",
    "db:action": "transact",
    "db:operation": "assert",
    "db:resource": { "@type": "ex:Document" },
    "db:allow": true,
    "db:augment": [
      { "@id": "?newEntity", "ex:owner": "?subject" }
    ]
  },
  {
    "@id": "ex:edit-own-policy",
    "db:subject": "?user",
    "db:action": "transact",
    "db:resource": {
      "ex:owner": "?user"
    },
    "db:allow": true
  }
]
```

### Approval Workflow

```json
[
  {
    "@id": "ex:submit-policy",
    "db:subject": "*",
    "db:action": "transact",
    "db:resource": {
      "@type": "ex:Request",
      "ex:status": "pending"
    },
    "db:allow": true
  },
  {
    "@id": "ex:approve-policy",
    "db:subject": { "ex:role": "approver" },
    "db:action": "transact",
    "db:resource": {
      "@type": "ex:Request",
      "ex:status": "approved"
    },
    "db:allow": true
  }
]
```

### Immutable Records

```json
{
  "db:subject": "*",
  "db:action": "transact",
  "db:operation": "retract",
  "db:resource": { "@type": "ex:AuditLog" },
  "db:allow": false
}
```

Audit logs cannot be modified or deleted.

## Debugging Transaction Policies

### Policy Trace

```bash
curl -X POST "http://localhost:8090/transact?ledger=mydb:main" \
  -H "X-Fluree-Policy-Trace: true" \
  -d '{...}'
```

Response (on error):
```json
{
  "error": "Forbidden",
  "message": "Policy denied transaction",
  "policy_trace": [
    {
      "triple": ["ex:alice", "schema:age", 32],
      "operation": "assert",
      "policies_evaluated": [
        {
          "id": "ex:owner-policy",
          "matched": true,
          "decision": "deny",
          "reason": "ownership condition not met"
        }
      ]
    }
  ]
}
```

### Dry Run

Test transaction without committing:

```bash
curl -X POST "http://localhost:8090/transact?ledger=mydb:main&dryRun=true" \
  -d '{...}'
```

Returns success/failure without actually committing.

## Performance Considerations

### Policy Evaluation Overhead

Transaction validation overhead:
- Type-based policies: Minimal overhead
- Property-based policies: Low overhead
- Complex condition policies: Higher overhead

### Batch Transactions

Policies evaluated per-triple:
- Large transactions take longer to validate
- Consider batch size vs validation time

### Policy Caching

Fluree caches compiled policies:
- First evaluation: Compiles policy
- Subsequent: Uses cached version
- Restart clears cache

## Best Practices

### 1. Default Deny for Writes

```json
{
  "db:subject": "*",
  "db:action": "transact",
  "db:allow": false,
  "db:priority": -1000
}
```

### 2. Separate Create/Update Policies

```json
[
  {
    "@id": "ex:create-policy",
    "db:operation": "assert",
    ...
  },
  {
    "@id": "ex:update-policy",
    "db:operation": ["retract", "assert"],
    ...
  }
]
```

### 3. Validate Business Rules

```json
{
  "db:resource": {
    "ex:price": "?price"
  },
  "db:condition": [
    { "db:filter": "?price > 0" }
  ],
  "db:allow": true
}
```

### 4. Audit Trail

```json
{
  "db:augment": [
    { "@id": "?entity", "ex:lastModifiedBy": "?subject" },
    { "@id": "?entity", "ex:lastModifiedAt": "NOW()" }
  ]
}
```

### 5. Test Transaction Policies

```javascript
async function testTransactionPolicy() {
  const txn = { "@graph": [...] };
  
  try {
    await transact(txn, { subject: "user1" });
    console.log("✓ Authorized");
  } catch (err) {
    if (err.code === "POLICY_DENIED") {
      console.log("✓ Correctly denied");
    } else {
      throw err;
    }
  }
}
```

## Related Documentation

- [Policy Model](policy-model.md) - Policy structure
- [Policy in Queries](policy-in-queries.md) - Read-time enforcement
- [Signed Transactions](../transactions/signed-transactions.md) - Transaction signing
- [Transaction Overview](../transactions/overview.md) - Transaction lifecycle
