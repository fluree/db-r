# Policy Model and Inputs

Fluree's policy system provides fine-grained access control by evaluating policies against requests. This document explains the policy model, structure, and evaluation process.

## Policy Structure

A policy consists of four main components:

```json
{
  "@context": {
    "db": "https://ns.flur.ee/db#",
    "ex": "http://example.org/ns/"
  },
  "@id": "ex:example-policy",
  "@type": "db:Policy",
  "db:subject": "did:key:z6Mkh...",
  "db:action": "query",
  "db:resource": {
    "@type": "schema:Person"
  },
  "db:allow": true
}
```

### 1. Subject (Who)

Specifies who the policy applies to:

**Specific DID:**
```json
{
  "db:subject": "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
}
```

**Any Subject (wildcard):**
```json
{
  "db:subject": "*"
}
```

**Variable (for conditions):**
```json
{
  "db:subject": "?user"
}
```

**Role-Based:**
```json
{
  "db:subject": {
    "ex:role": "admin"
  }
}
```

**Group-Based:**
```json
{
  "db:subject": {
    "ex:memberOf": "ex:engineering-team"
  }
}
```

### 2. Action (What)

Specifies which operation:

**Query:**
```json
{
  "db:action": "query"
}
```

**Transact:**
```json
{
  "db:action": "transact"
}
```

**Multiple Actions:**
```json
{
  "db:action": ["query", "transact"]
}
```

**All Actions:**
```json
{
  "db:action": "*"
}
```

### 3. Resource (Which Data)

Specifies what data the policy applies to:

**By Type:**
```json
{
  "db:resource": {
    "@type": "schema:Person"
  }
}
```

**By Predicate:**
```json
{
  "db:resource": {
    "db:predicate": "ex:salary"
  }
}
```

**Specific Entity:**
```json
{
  "db:resource": {
    "@id": "ex:alice"
  }
}
```

**Pattern with Variables:**
```json
{
  "db:resource": {
    "@type": "ex:Document",
    "ex:department": "?dept"
  }
}
```

**All Resources:**
```json
{
  "db:resource": "*"
}
```

### 4. Allow/Deny

Specifies whether to grant or deny access:

**Allow:**
```json
{
  "db:allow": true
}
```

**Deny:**
```json
{
  "db:allow": false
}
```

## Conditions

Policies can include conditions that must be satisfied:

```json
{
  "@id": "ex:same-department-policy",
  "db:subject": "?user",
  "db:action": "query",
  "db:resource": {
    "@type": "schema:Person",
    "ex:department": "?dept"
  },
  "db:condition": [
    { "@id": "?user", "ex:department": "?dept" }
  ],
  "db:allow": true
}
```

This allows users to query people in their own department.

### Multiple Conditions

```json
{
  "db:subject": "?user",
  "db:resource": {
    "@id": "?doc",
    "ex:status": "published"
  },
  "db:condition": [
    { "@id": "?user", "ex:clearanceLevel": "?level" },
    { "@id": "?doc", "ex:requiredClearance": "?reqLevel" },
    { "db:filter": "?level >= ?reqLevel" }
  ],
  "db:allow": true
}
```

## Policy Evaluation

### Input Context

When evaluating policies, Fluree has access to:

**Request Context:**
- **subject**: DID from signed request or authentication
- **action**: Operation being performed (query, transact)
- **resource**: Target entity/pattern being accessed
- **timestamp**: Current time

**Data Context:**
- **graph**: Current ledger state
- **entity properties**: Properties of entities being accessed
- **relationships**: Graph connections
- **history**: Historical data (if needed)

**Example:**
```text
Request: Query for ex:alice's data
Context:
  - subject: did:key:z6Mkh...
  - action: query
  - resource: ex:alice
  - graph: mydb:main@t:100
```

### Evaluation Steps

1. **Collect Applicable Policies**
   - Match subject (is this user covered?)
   - Match action (is this operation covered?)
   - Match resource (is this data covered?)

2. **Evaluate Conditions**
   - Execute condition queries
   - Check filters
   - Variable bindings must match

3. **Combine Results**
   - Apply combining algorithm
   - Resolve conflicts

4. **Return Decision**
   - Allow or Deny
   - With reasons (for debugging)

### Evaluation Example

**Policy:**
```json
{
  "db:subject": "did:key:z6Mkhabc...",
  "db:action": "query",
  "db:resource": { "@type": "ex:PublicData" },
  "db:allow": true
}
```

**Request:**
```text
subject: did:key:z6Mkhabc...
action: query
resource: ex:document-123 (type: ex:PublicData)
```

**Evaluation:**
```text
✓ Subject matches: did:key:z6Mkhabc...
✓ Action matches: query
✓ Resource matches: ex:document-123 is ex:PublicData
→ Result: ALLOW
```

## Combining Algorithms

### Deny Overrides (Default)

Most restrictive policy wins:

```text
Policy 1: ALLOW
Policy 2: DENY
→ Result: DENY
```

Logic:
1. If any policy denies → DENY
2. If any policy allows → ALLOW
3. If no policies match → DENY (default deny)

### Allow Overrides

Most permissive policy wins:

```text
Policy 1: DENY
Policy 2: ALLOW
→ Result: ALLOW
```

Logic:
1. If any policy allows → ALLOW
2. If any policy denies → DENY
3. If no policies match → DENY (default deny)

### First Applicable

First matching policy wins:

```text
Policy 1 (matches): ALLOW
Policy 2 (matches): DENY
→ Result: ALLOW (first match)
```

## Default Policies

### Default Deny

Recommended for production:

```json
{
  "@id": "ex:default-deny",
  "db:subject": "*",
  "db:action": "*",
  "db:resource": "*",
  "db:allow": false,
  "db:priority": -1000
}
```

All access denied unless explicitly allowed.

### Default Allow

For development only:

```json
{
  "@id": "ex:default-allow",
  "db:subject": "*",
  "db:action": "*",
  "db:resource": "*",
  "db:allow": true
}
```

All access allowed unless explicitly denied.

## Policy Priority

Control policy evaluation order with priority:

```json
{
  "@id": "ex:admin-override",
  "db:subject": { "ex:role": "admin" },
  "db:action": "*",
  "db:allow": true,
  "db:priority": 1000
}
```

Higher priority policies evaluated first.

## Variable Binding

Variables in policies bind to values from context:

```json
{
  "db:subject": "?user",
  "db:resource": {
    "ex:owner": "?user"
  },
  "db:allow": true
}
```

**Evaluation:**
```text
Request subject: did:key:z6Mkhabc...
Bind: ?user = did:key:z6Mkhabc...

Check resource:
  ex:document-123 ex:owner did:key:z6Mkhabc...
  
Match! → ALLOW
```

## Pattern Matching

Policies can match patterns:

```json
{
  "db:resource": {
    "@type": "?type",
    "ex:visibility": "public"
  },
  "db:condition": [
    { "db:filter": "?type != ex:SensitiveData" }
  ],
  "db:allow": true
}
```

Allows access to any public data except SensitiveData.

## Time-Based Policies

Policies can be time-dependent:

```json
{
  "db:subject": "?user",
  "db:action": "query",
  "db:resource": {
    "ex:availableFrom": "?startDate",
    "ex:availableUntil": "?endDate"
  },
  "db:condition": [
    { "db:filter": "NOW() >= ?startDate && NOW() <= ?endDate" }
  ],
  "db:allow": true
}
```

## Property-Level Access Control

Control access to specific properties:

```json
{
  "@id": "ex:hide-salary",
  "db:subject": "*",
  "db:action": "query",
  "db:resource": {
    "db:predicate": "ex:salary"
  },
  "db:allow": false
}
```

```json
{
  "@id": "ex:show-salary-to-hr",
  "db:subject": { "ex:role": "hr" },
  "db:action": "query",
  "db:resource": {
    "db:predicate": "ex:salary"
  },
  "db:allow": true
}
```

## Entity-Level Access Control

Control access to specific entities:

```json
{
  "db:subject": "?user",
  "db:action": "*",
  "db:resource": {
    "@id": "?entity",
    "ex:owner": "?user"
  },
  "db:allow": true
}
```

Users can access entities they own.

## Policy Examples

### Public Read, Authenticated Write

```json
[
  {
    "@id": "ex:public-read",
    "db:subject": "*",
    "db:action": "query",
    "db:allow": true
  },
  {
    "@id": "ex:authenticated-write",
    "db:subject": "?user",
    "db:action": "transact",
    "db:condition": [
      { "@id": "?user", "@type": "ex:AuthenticatedUser" }
    ],
    "db:allow": true
  }
]
```

### Department Isolation

```json
{
  "@id": "ex:department-isolation",
  "db:subject": "?user",
  "db:action": "*",
  "db:resource": {
    "ex:department": "?dept"
  },
  "db:condition": [
    { "@id": "?user", "ex:department": "?dept" }
  ],
  "db:allow": true
}
```

### Hierarchical Permissions

```json
{
  "@id": "ex:manager-access",
  "db:subject": "?manager",
  "db:action": "*",
  "db:resource": {
    "ex:reportsTo": "?manager"
  },
  "db:allow": true
}
```

Managers can access data of their reports.

### Time-Window Access

```json
{
  "@id": "ex:business-hours-only",
  "db:subject": "?user",
  "db:action": "transact",
  "db:condition": [
    { "db:filter": "HOUR(NOW()) >= 9 && HOUR(NOW()) <= 17" }
  ],
  "db:allow": true
}
```

### Clearance-Level Access

```json
{
  "@id": "ex:clearance-policy",
  "db:subject": "?user",
  "db:resource": {
    "ex:classificationLevel": "?docLevel"
  },
  "db:condition": [
    { "@id": "?user", "ex:clearance": "?userLevel" },
    { "db:filter": "?userLevel >= ?docLevel" }
  ],
  "db:allow": true
}
```

## Policy Debugging

### Policy Trace

Enable policy tracing to see evaluation:

```bash
curl -X POST http://localhost:8090/query \
  -H "X-Fluree-Policy-Trace: true" \
  -d '{...}'
```

Response includes trace:
```json
{
  "results": [...],
  "policy_trace": [
    {
      "policy": "ex:policy-1",
      "matched": true,
      "conditions_met": true,
      "decision": "allow"
    },
    {
      "policy": "ex:policy-2",
      "matched": false,
      "reason": "subject mismatch"
    }
  ],
  "final_decision": "allow"
}
```

### Test Policies

Test policy evaluation:

```javascript
async function testPolicy(policyId, testCases) {
  for (const test of testCases) {
    const result = await evaluatePolicy({
      policy: policyId,
      subject: test.subject,
      action: test.action,
      resource: test.resource
    });
    
    console.log(`Test: ${test.name}`);
    console.log(`Expected: ${test.expected}`);
    console.log(`Actual: ${result.decision}`);
    console.log(`Match: ${result.decision === test.expected ? 'PASS' : 'FAIL'}`);
  }
}
```

## Best Practices

### 1. Start with Default Deny

```json
{
  "db:subject": "*",
  "db:action": "*",
  "db:allow": false,
  "db:priority": -1000
}
```

### 2. Use Specific Policies

Prefer specific over general:

Good:
```json
{
  "db:resource": { "@type": "ex:PublicDocument" },
  "db:allow": true
}
```

Less secure:
```json
{
  "db:resource": "*",
  "db:allow": true
}
```

### 3. Organize by Role

Group policies by role:

```json
{
  "@id": "ex:admin-policies",
  "@type": "ex:PolicySet",
  "ex:includes": [
    "ex:admin-query-policy",
    "ex:admin-transact-policy",
    "ex:admin-delete-policy"
  ]
}
```

### 4. Document Policies

Add descriptions:

```json
{
  "@id": "ex:policy-1",
  "rdfs:label": "Public read access",
  "rdfs:comment": "Allows anyone to read public documents",
  "db:subject": "*",
  "db:action": "query",
  "db:resource": { "ex:visibility": "public" },
  "db:allow": true
}
```

### 5. Test Thoroughly

Test all policy paths:
- Positive cases (should allow)
- Negative cases (should deny)
- Edge cases
- Condition evaluation

### 6. Monitor Policy Usage

Log policy decisions:

```javascript
policyLogger.info({
  timestamp: new Date(),
  subject: request.subject,
  action: request.action,
  resource: request.resource,
  decision: policyResult.decision,
  policies_evaluated: policyResult.policies
});
```

## Related Documentation

- [Policy in Queries](policy-in-queries.md) - Query-time enforcement
- [Policy in Transactions](policy-in-transactions.md) - Transaction-time enforcement
- [Signed Requests](../api/signed-requests.md) - Authentication
- [Policy Enforcement Concepts](../concepts/policy-enforcement.md) - High-level overview
