# Auth contract (CLI ↔ Server)

This document defines the wire-level contract between the Fluree CLI and any Fluree-compatible server (a standalone `fluree-server`, an OIDC-capable application embedding Fluree, or future products). Any implementation that exposes these endpoints will get zero-configuration CLI auth.

For the overall authentication model, see [Authentication](../security/authentication.md).

## Auth discovery

### `GET /.well-known/fluree.json`

The CLI fetches this endpoint when a remote is added (`fluree remote add`) to auto-configure auth. The server MAY expose this endpoint. If absent, the CLI falls back to manual token configuration.

**Response** (200 OK, `application/json`):

```json
{
  "version": 1,
  "auth": {
    "type": "oidc_device",
    "issuer": "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc123",
    "client_id": "fluree-cli",
    "exchange_url": "https://solo.example.com/fluree/auth/exchange",
    "scopes": ["openid", "profile"]
  }
}
```

### `auth.type` values

| Type | Meaning | CLI behavior |
|------|---------|--------------|
| `oidc_device` | OIDC Device Authorization Grant + token exchange | `fluree auth login` runs device flow, then calls `exchange_url` |
| `token` | Manual Bearer token (no automated login flow) | `fluree auth login --token <value>` |

### Field reference (`oidc_device`)

| Field | Required | Description |
|-------|----------|-------------|
| `issuer` | Yes | OIDC issuer URL (used for `/.well-known/openid-configuration` discovery) |
| `client_id` | Yes | OAuth client ID for the CLI (must be registered for device flow) |
| `exchange_url` | Yes | Absolute URL for the Fluree token exchange endpoint |
| `scopes` | No | OAuth scopes to request (default: `["openid"]`) |

### Fallback behavior

- Discovery endpoint absent (404 or connection error) → CLI assumes `token` type, prompts user to provide a token manually
- `version` > 1 → CLI warns but attempts to parse known fields

## Token exchange

### `POST {exchange_url}`

After the CLI completes the OIDC device flow with the IdP, it calls the exchange endpoint to trade the IdP token for a Fluree-scoped Bearer token. This endpoint is hosted by the application that manages authorization (e.g., an app embedding Fluree and maintaining user entitlements).

**Request:**

```http
POST /fluree/auth/exchange HTTP/1.1
Content-Type: application/json

{
  "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
  "subject_token": "<idp-access-token-or-id-token>",
  "subject_token_type": "urn:ietf:params:oauth:token-type:access_token"
}
```

**Success response** (200 OK):

```json
{
  "access_token": "<fluree-bearer-token>",
  "token_type": "Bearer",
  "expires_in": 3600,
  "refresh_token": "<optional-refresh-token>"
}
```

**Error response** (401/403):

```json
{
  "error": "invalid_grant",
  "error_description": "IdP token is invalid or user is not authorized for Fluree access"
}
```

### Contract

- The exchange endpoint validates the IdP token (against the IdP's JWKS or userinfo), looks up the user's Fluree entitlements, and mints a Fluree-scoped JWT.
- The returned `access_token` MUST be a JWT that `fluree-server` can verify (via JWKS). It MUST include the standard Fluree claims (`fluree.identity`, `fluree.ledger.*`, and optionally `fluree.storage.*`). See [Bearer token claim set](../security/authentication.md#bearer-token-claim-set).
- `refresh_token` is OPTIONAL. If present, the CLI stores it and uses it for silent refresh.
- `subject_token_type` MAY be `urn:ietf:params:oauth:token-type:id_token` if the CLI sends the ID token instead of the access token.

This loosely follows [RFC 8693 (OAuth 2.0 Token Exchange)](https://datatracker.ietf.org/doc/html/rfc8693).

## Token refresh

### `POST {exchange_url}`

If the CLI holds a `refresh_token`, it can request a new access token without user interaction.

**Request:**

```json
{
  "grant_type": "refresh_token",
  "refresh_token": "<stored-refresh-token>"
}
```

**Success response:** Same shape as token exchange success.

**Failure:** CLI clears stored tokens and prompts `fluree auth login`.

## CLI TOML config format

The CLI stores auth configuration per-remote in `.fluree/config.toml`:

```toml
[[remotes]]
name = "solo-prod"
type = "Http"
base_url = "https://solo.example.com"

[remotes.auth]
type = "oidc_device"
issuer = "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_abc123"
client_id = "fluree-cli"
exchange_url = "https://solo.example.com/fluree/auth/exchange"
token = "eyJ..."           # cached Fluree Bearer token (written by 'fluree auth login')
refresh_token = "eyJ..."   # refresh token (written by 'fluree auth login')

[[remotes]]
name = "local"
type = "Http"
base_url = "http://localhost:8090"

[remotes.auth]
type = "token"
token = "eyJ..."           # manually provided via 'fluree auth login --token'
```

**Backward compatibility:** If `type` is absent, infer `"token"` if `token` is present, otherwise treat as unauthenticated.

## CLI `fluree auth login` behavior

```
fluree auth login [--remote <name>]
```

1. Resolve the target remote.
2. Check `auth.type`:
   - **`oidc_device`**:
     1. Discover OIDC endpoints from `{issuer}/.well-known/openid-configuration`.
     2. POST to `device_authorization_endpoint` to get `device_code`, `user_code`, `verification_uri`.
     3. Print: `Open {verification_uri} and enter code: {user_code}`
     4. Poll `token_endpoint` until user completes browser auth.
     5. POST IdP token to `exchange_url` → get Fluree Bearer token.
     6. Store `token` and `refresh_token` in remote config.
   - **`token`**: Prompt for token (or accept `--token <value|@file|@->`). Store in config.
   - **Unset / no discovery**: Attempt discovery at `{base_url}/.well-known/fluree.json`. If found, configure auth type and proceed. If not found, fall back to `token` flow.

See [CLI auth command](../cli/auth.md) for full command reference.

## CLI auto-refresh on 401

Auto-refresh applies to **data-plane commands** (`query`, `insert`, `upsert`, `info`) that use `RemoteLedgerClient` in tracked mode or `--remote` mode.

When a data-plane command receives a 401 from the remote:

1. If `auth.type == "oidc_device"` and `refresh_token` is present:
   - Attempt silent refresh via the exchange endpoint.
   - On success: update stored token and (if rotated) refresh token in `.fluree/config.toml`, retry the original request once.
   - On failure: clear tokens, print `Token expired. Run: fluree auth login --remote <name>`
2. Otherwise: print `Authentication failed. Run: fluree auth login --remote <name>`

### Replication commands (`fetch`, `pull`, `push`)

Replication commands use `HttpRemoteClient` (from `fluree-db-nameservice-sync`) which does **not** perform auto-refresh. This is intentional:

- Replication requires `fluree.storage.*` scopes, which are reserved for operators and service accounts.
- Operator tokens are typically long-lived or non-expiring. If an operator token expires, the user should run `fluree auth login` to obtain a new one.
- Regular users who only have query-scoped tokens should use `fluree track` + `--remote` mode instead of `fetch`/`pull`/`push`.

## Scope rules

- The exchange endpoint MUST NOT grant `fluree.storage.*` to regular users. Replication scope is for operators and service accounts only. See [Replication vs query boundary](../security/authentication.md#replication-vs-query-access-boundary).
- If a user with only query-scoped tokens attempts `fluree pull` or `fluree fetch`, the CLI MUST fail with a clear message explaining that replication requires `fluree.storage.*` and suggesting `fluree track` instead.

## Token diagnostic endpoint

### `GET /fluree/whoami`

A verified diagnostic endpoint that performs full cryptographic verification of the Bearer token (if present) using the same code path as data endpoints. This is the recommended way for the CLI or an implementing application to validate a token without side effects.

**No token:**

```json
{ "token_present": false }
```

**Valid token (verified):**

```json
{
  "token_present": true,
  "verified": true,
  "auth_method": "embedded_jwk",
  "issuer": "did:key:z6Mk...",
  "subject": "admin@example.com",
  "identity": "did:key:z6Mk...",
  "expires_at": 1739012345,
  "scopes": {
    "ledger_read_all": true,
    "ledger_write_all": true
  }
}
```

**Invalid token (verification failed):**

```json
{
  "token_present": true,
  "verified": false,
  "error": "Token expired",
  "issuer": "did:key:z6Mk...",
  "subject": "admin@example.com",
  "expires_at": 1738900000
}
```

When verification fails, the response includes **unverified** decoded claims (base64-decoded without signature check) for debugging. These fields are explicitly untrustworthy — they help diagnose _why_ verification failed (e.g., wrong issuer, expired token) but must never be used for authorization decisions.

The `auth_method` field is only present on successful verification: `"embedded_jwk"` for Ed25519/JWS tokens, `"oidc"` for JWKS/RS256 tokens.

This endpoint always returns `200` regardless of token validity — it is diagnostic, not a gate.

## Error semantics

### Standard error response shape

`fluree-server` returns errors as JSON with a consistent structure. Implementers
SHOULD follow this shape so the CLI can display meaningful diagnostics.

```json
{
  "error": "<human-readable description>",
  "status": 401,
  "@type": "err:db/Unauthorized",
  "cause": {
    "error": "<nested cause (optional)>",
    "status": 400,
    "@type": "err:db/JsonParse"
  }
}
```

Notes:
- `error` is the primary human-readable message. The CLI may pattern-match on substrings inside this field.
- `@type` is a compact error type IRI used as a stable, machine-readable code.
- `cause` is optional and may be nested.
- Implementers MAY include additional fields, but MUST keep `error` stable and human-readable.

### Status codes

| Code | Meaning | When |
|------|---------|------|
| `200` | Success | Request completed successfully |
| `400` | Bad request | Malformed body, invalid JSON, missing required fields |
| `401` | Unauthorized | Missing Bearer token, expired token, invalid signature, unknown signing key |
| `403` | Forbidden | Valid token but insufficient scope (e.g., query-only token on admin endpoint) |
| `404` | Not found **or** unauthorized | Ledger does not exist, **or** token lacks access to this ledger (anti-leak) |
| `409` | Conflict | Ledger already exists (`/fluree/create`), concurrent transaction conflict |
| `500` | Internal error | Server-side failure |

### Anti-leak pattern: 404 for out-of-scope ledgers

Data endpoints (`/fluree/query`, `/fluree/transact`, etc.) return `404` rather than `403` when a valid token lacks access to the requested ledger. This prevents authenticated users from discovering the existence of ledgers they are not authorized to access.

**Implication for CLI and implementers:** A `404` on a data endpoint can mean either:
- The ledger genuinely does not exist, or
- The token does not have scope for that ledger.

The CLI should present both possibilities in error messages. Implementers should not attempt to distinguish these cases client-side.

### Token verification errors (401)

Common `401` error messages and their causes:

| Server message | Cause | CLI hint |
|----------------|-------|----------|
| `Bearer token required` | No `Authorization: Bearer ...` header | `fluree auth login --remote <name>` |
| `Invalid token` | Malformed JWT/JWS, bad signature | Re-issue token; check signing key |
| `Token expired` | `exp` claim is in the past | Refresh or re-login |
| `Untrusted issuer` | `iss` / signing key not in trusted list | Check `--trusted-issuer` / `--jwks-issuer` config |
| `OIDC issuer not configured` | Token has `kid` header but no JWKS configured | Add `--jwks-issuer` to server config |
| `Token lacks storage proxy permissions` | Valid token but missing `fluree.storage.*` | Use operator token or `fluree track` instead |

## Implementor checklist

Any Fluree-compatible server that wants zero-config CLI auth must:

1. Expose `GET /.well-known/fluree.json` with the discovery payload
2. Implement `POST {exchange_url}` for token exchange and refresh
3. Issue Fluree-scoped JWTs with the [standard claim set](../security/authentication.md#bearer-token-claim-set)
4. Publish a JWKS endpoint so `fluree-server` can verify issued tokens (configured via `--jwks-issuer`)

### Conformance checklist (status codes)

Implementors MUST return these status codes consistently so the CLI can provide good diagnostics:

| Endpoint | Success | Missing token | Bad token | Insufficient scope | Not found / no access |
|----------|---------|---------------|-----------|---------------------|-----------------------|
| `GET /.well-known/fluree.json` | `200` | n/a | n/a | n/a | `404` (not implemented) |
| `POST /fluree/create` | `201` | `401` | `401` | `403` | n/a |
| `POST /fluree/drop` | `200` | `401` | `401` | `403` | `404` |
| `POST /fluree/query` | `200` | `401` | `401` | `404` (anti-leak) | `404` (anti-leak) |
| `POST /fluree/transact` | `200` | `401` | `401` | `404` (anti-leak) | `404` (anti-leak) |
| `POST /fluree/auth/exchange` | `200` | n/a | `401` | `403` | n/a |
| `GET /fluree/whoami` | `200` | `200` (token_present=false) | `200` (verified=false) | n/a | n/a |

### Conformance checklist (error bodies)

All error responses MUST include a JSON body. The body SHOULD include at least an `error` or `message` field. The CLI pattern-matches on specific substrings (e.g., `"Bearer token required"`, `"Untrusted issuer"`) to provide targeted hints, so error messages should be stable across releases.

## See also

- [Authentication](../security/authentication.md) — Auth model, modes, claim set, and access boundaries
- [Configuration — OIDC](../operations/configuration.md#oidc--jwks-token-verification) — Server `--jwks-issuer` setup
- [CLI auth command](../cli/auth.md) — `auth login`, `auth status`, `auth logout`
- [CLI token command](../cli/token.md) — Ed25519 token minting (Mode 2)
