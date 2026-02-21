# Unified Auth Story (CLI, fluree-server, Solo)

This document defines a **uniform authentication model** for:

- **`fluree-db-cli`**: the command-line interface used by developers and operators
- **`fluree-server`** (db-r server): a standalone database server (may have no UI)
- **Solo**: an application that embeds Fluree DB plus additional services, and can be configured to use **any OIDC/OAuth2 identity provider** (Cognito is just the default)

The main goal is to keep **Fluree’s decentralized `did:key` identity + dataset policies** as first-class, while also supporting modern **Bearer token** workflows for better UX and interoperability.

---

## The model: identity vs transport

### Identity (who)
Fluree policy enforcement is ultimately about an identity, ideally a DID:

- **Preferred**: `did:key:...` (portable across environments without a central identity server)
- **Also possible**: other DIDs or IRIs mapped into Fluree policy (e.g. `ex:alice`)

### Transport / session (how requests authenticate)
There are two “on-the-wire” ways requests authenticate:

- **Signed requests**: JWS/VC/signed envelope containing the DID (proves possession)
- **Bearer tokens**: short-lived tokens that carry identity + authorization claims

**Important:** Bearer tokens should not replace the identity model. They are a UX and deployment convenience layer.

---

## Three supported auth modes (target state)

### Mode 1 — Decentralized: `did:key` signed requests (no IdP)

- CLI holds an Ed25519 keypair → derives `did:key:...`
- CLI signs write (and optionally read) requests using existing “credential” / signed envelope format
- Server verifies signature and uses DID as the principal
- Dataset policies decide allow/deny

**Why keep this:** It preserves the core Fluree value proposition: no “single identity server” required.

### Mode 2 — Standalone `fluree-server` bootstrap (no external IdP, but tokens allowed)

Designed for: “stand up a one-user database server somewhere” (local, EC2, etc.).

**Phase 1 (recommended): Offline minting only (simplest)**:

- Admin mints a short-lived Bearer token using a local signing key (did:key JWS)
- Admin provides token to the CLI user (or stores it in a secret store)
- Server validates token and enforces scope + policy identity

In both cases, the **policy identity** that Fluree enforces should remain DID-based (e.g. `fluree.identity = did:key:...`) so authorization stays dataset/policy driven.

**Future (optional): Registration endpoints**

We can later add a nicer “register a DID then mint tokens” flow, but it is explicitly out of scope for the immediate phase.

### Mode 3 — Solo with external IdP (OIDC/OAuth2)

Designed for: Solo as the front door using Cognito (or any OIDC IdP).

- IdP authenticates user (device flow or PKCE)
- Solo (not the IdP) knows Fluree dataset entitlements
- Solo issues (or exchanges for) **Fluree-scoped tokens** that carry:
  - identity (ideally a DID)
  - ledger read/write scopes
  - optional policy class

This preserves the separation:

- **IdP**: authentication (who logged in)
- **Solo**: authorization (what they can access in Fluree)

---

## Uniform CLI behavior (how CLI stays consistent across modes)

The CLI should treat “auth” as choosing one of two request credentials:

- **Bearer credential**: attach `Authorization: Bearer <token>`
- **Signer credential**: sign request body (JWS/VC) and send as `application/jwt` (or existing signed request format)

The CLI can expose this as profiles:

- `fluree auth login <remote>`: obtain Bearer tokens (Mode 2 or Mode 3)
- `fluree auth key add`: add/import did:key signing key (Mode 1)
- `fluree auth status`: show current credential for the active remote

Internally, command handlers should not care: they ask for a “credential” and a “remote.”

### CLI command evolution: `token` vs `auth`

The CLI already has a “low-level” token toolbox:

- `fluree token keygen`
- `fluree token create`
- `fluree token inspect`
- remotes can store a `--token` (or token file) for bearer auth

Recommendation:

- Keep `fluree token ...` as **low-level plumbing** (useful for Mode 2 offline minting and debugging).
- Add `fluree auth ...` as the **high-level workflow**:
  - OIDC login (Mode 3)
  - credential storage/selection per remote
  - refresh handling and clear error UX

---

## What is already in place (db-r)

### fluree-server: Data API auth (Bearer + signed request)
Implemented in this repo:

- **Data auth config**:
  - `--data-auth-mode {none|optional|required}`
  - trusted issuers
  - optional default policy class
- **Bearer verification for data endpoints** (query/transact/info/exists):
  - new extractor `MaybeDataBearer` → `DataPrincipal`
- **Scope claims** for ledgers:
  - `fluree.ledger.read.all`
  - `fluree.ledger.read.ledgers`
  - `fluree.ledger.write.all`
  - `fluree.ledger.write.ledgers`
  - Back-compat: `fluree.storage.*` implies **read** scope on data endpoints
- **Identity spoofing prevention**:
  - when auth is present, server forces `opts.identity` (and optional policy class) instead of trusting client headers/body
- **“No existence leak”** behavior:
  - out-of-scope ledgers return `404` (not found) rather than `403`

Docs: `docs/operations/configuration.md` includes the new data-auth flags.

### fluree-server: Admin endpoint auth exists
`fluree-server` already has **admin auth configuration** for endpoints like create/drop.

If we adopt OIDC/JWKS verification broadly, we should decide whether admin auth should also accept:

- did:key JWS (current style), and/or
- OIDC JWTs (JWKS-based) for Solo compatibility

### fluree-db-cli: token minting
Implemented in this repo:

- `fluree token create` can mint tokens with:
  - `--read-all`, `--read-ledger <alias>`
  - `--write-all`, `--write-ledger <alias>`
  - `--identity <iri-or-did>`

This is sufficient for Mode 2 **offline minting** (admin creates token, user uses it).

---

## Already in place, but needs adjustment (gaps)

### 1) Token verification is currently “did:key JWS (embedded JWK)”
`fluree-server` currently verifies tokens using the **did:key JWS approach** used in this repo.

To support Solo’s external IdP mode (Mode 3), `fluree-server` needs to also support:

- verifying **standard OIDC JWTs** (RS256/ES256) using **remote JWKS**
- issuer + audience validation
- key rotation (`kid`-based selection)

#### JWKS caching strategy (recommended)

For JWKS-based verification:

- Fetch JWKS on first request (or at startup).
- Cache keys with a TTL.
- On `kid` cache miss: refresh JWKS and retry once.
- If still missing: reject (likely misconfiguration or key rotation not yet propagated).

This is a net-new verification path; see “Net-new work” below.

### 2) Connection-scoped SPARQL authorization
Ledger-scoped endpoints can enforce ledger read/write scopes cleanly.

Connection-scoped SPARQL (e.g. `/fluree/query` with `FROM`) may reference multiple ledgers.
Rule (pragmatic + predictable):

- If a Bearer token is used, require **all** `FROM` / `FROM NAMED` ledgers referenced by the query to be within the Bearer scope.
- If any referenced ledger is out of scope: reject (and avoid existence leaks where practical).

This keeps the behavior simple and composable.

### 3) Consistent “identity source of truth”
Target precedence (recommended):

1. Signed request DID (proof-of-possession)
2. Bearer token `fluree.identity` (or mapped identity)
3. Client-provided headers/body identity (only if server is in unauthenticated mode)

Server already enforces this precedence for FQL opts injection; ensure SPARQL and other paths are consistent.

---

## Net-new work: Mode 2 bootstrap (standalone fluree-server without an IdP)

Mode 2 can be supported **immediately** with offline token minting:

- Admin generates a signing key
- Admin runs `fluree token create ...` to mint a scoped token
- User configures CLI to use that token for the remote

If we want the nicer “register a DID then mint tokens” flow, net-new server work is required (future phase):

### A) Minimal authz store
Store:

- user identity (DID)
- ledger scopes (read/write, all vs list)
- optional “policy class” default

Possible storage:

- local file (`.toml`/`json`) for single-node deployments
- or a dedicated internal ledger/collection (advanced; optional)

### B) Minimal admin bootstrap
Admin provides:

- an **admin signing key** (did:key)
- config enabling data auth required
- a way to authorize a new DID (and assign scopes)

### C) Registration + token mint endpoints (optional)
Proposed minimal endpoints:

- `POST /fluree/admin/register-did`
  - auth: admin token (Bearer) or admin-signed request
  - body: `{ did, scopes, identity?, policy_class? }`
- `POST /fluree/admin/mint-token`
  - auth: admin token or admin-signed request
  - body: `{ did, expires_in, scopes? }`

If we don’t want these endpoints, keep Mode 2 as “offline minting” only.

---

## Net-new work: Mode 3 Solo (OIDC-backed) support

Solo uses an external IdP (Cognito or any OIDC). The IdP does **authentication**; Solo performs **authorization** for Fluree datasets.

Two viable integration patterns:

### Pattern 3.1 — Token exchange (recommended)

- CLI authenticates with IdP → gets IdP access token
- CLI calls Solo: “exchange IdP token for Fluree-scoped token”
- Solo validates IdP token (JWKS), loads entitlements, mints Fluree token
- CLI uses Fluree token to call Fluree services (Solo, fluree-server, etc.)

What Solo must provide:

- an exchange endpoint (OAuth2 Token Exchange RFC 8693, or a pragmatic equivalent)
- a signing key + JWKS for the Fluree tokens it issues
- a stable issuer URL for the tokens
- entitlement store: user → dataset/ledger scopes + identity mapping

What fluree-server must provide:

- OIDC JWT verification (JWKS), or trust Solo-issued tokens (recommended)

### Pattern 3.2 — IdP embeds Fluree claims

- Solo configures IdP (e.g. Cognito trigger) to include Fluree scopes/identity claims directly
- fluree-server accepts IdP JWTs

This is possible, but increases IdP coupling and complexity.

---

## Recommended claim set for “Fluree-scoped” tokens (Mode 2 + Mode 3)

Minimum:

- `iss`: issuer (Solo or fluree-server bootstrap issuer)
- `aud`: target service (e.g. `fluree-server`, `solo`)
- `exp`, `iat`, `sub`
- `fluree.identity`: DID/IRI used for policy enforcement
- `fluree.ledger.read.*` and `fluree.ledger.write.*` claims

Optional:

- `fluree.policy.class` (or use headers if server allows defaulting/overrides)
- `jti` for token replay/blacklist semantics (if you implement revocation)

---

## Concrete gap-closure checklist (next steps)

### For fluree-server (standalone + Solo compatibility)

1) ~~**Add OIDC JWT verification** (JWKS-based) as an alternative to did:key JWS.~~ **DONE** — all Bearer-token endpoints (data, admin, events, storage proxy, nameservice refs) now support dual-path Ed25519 + OIDC/JWKS verification via `--jwks-issuer`.
2) ~~Implement connection-scoped SPARQL scope enforcement: **all FROM ledgers must be within scope**.~~ **DONE** — `routes/query.rs` parses FROM/FROM NAMED and checks against token read scope.
3) (Optional, later) Add Mode 2 registration/mint endpoints if we want better UX than offline minting.
4) ~~Decide whether admin auth should also accept OIDC JWTs (JWKS-based) in addition to existing mechanisms.~~ **DONE** — admin endpoints use dual-path dispatch.
5) (Optional) Add `GET /.well-known/fluree.json` metadata endpoint for CLI auth discovery (see contract below). Only needed if standalone `fluree-server` wants to support auto-discovery; Solo will implement this on its side.

### For CLI (uniform UX)

1) **Implement OIDC device code flow** in `fluree auth login` (see CLI ↔ Server contract below).
   - Auto-discover auth config from `/.well-known/fluree.json` on `fluree remote add`.
   - Run OAuth 2.0 Device Authorization Grant against the discovered IdP.
   - Call the exchange endpoint to trade IdP token for Fluree-scoped Bearer token.
2) **Extend `RemoteAuth` config** with `auth_type`, OIDC fields, and `refresh_token` (see TOML format below).
3) **Implement auto-refresh on 401**: if `auth_type == oidc_device` and `refresh_token` present, attempt silent refresh before failing.
4) Standardize attaching identity:
   - Bearer: no identity headers needed; token carries identity
   - Signed: DID comes from signature; server uses it
5) (Future) OS keychain integration for token storage.

### For Solo (OIDC-backed)

1) **Expose `GET /.well-known/fluree.json`** with `auth.type = "oidc_device"` (see discovery contract below).
2) **Implement `POST /fluree/auth/exchange`** — trade IdP tokens for Fluree-scoped Bearer tokens (see exchange contract below).
3) Implement entitlements model + mapping to Fluree ledger scopes.
4) Publish JWKS and issuer metadata for issued tokens, and configure `fluree-server` to trust Solo-issued tokens via `--jwks-issuer`.

---

## Replication vs query access boundary

Fluree draws a hard boundary between **replication-scoped** and **query-scoped** access:

### Replication access (`fluree.storage.*`)

Replication operations — nameservice sync, storage proxy reads, and CLI `fetch`/`pull`/`push` — require **root-level** `fluree.storage.*` claims. These operations transfer raw commit data and index blocks; they bypass dataset policy because the data must be bit-identical to what the transaction server wrote.

- `fluree.storage.all: true` — access all ledgers
- `fluree.storage.ledgers: [...]` — access specific ledgers

Replication tokens are intended for **operator / service-account use** (e.g. a peer server's storage-proxy token, or an admin's CLI pull/push workflow). They should never be issued to end users.

> **Policy-filtered storage proxy** (`Accept: application/x-fluree-flakes`) is a partial exception: leaf flakes are decoded and filtered through dataset policy before transport. However, branch and commit structure is still transferred unfiltered (a v1 limitation), so even this mode is not suitable for untrusted clients.

### Query access (`fluree.ledger.read/write.*`)

Query operations — `/:ledger/query`, `/:ledger/insert`, etc. — use `fluree.ledger.read.*` and `fluree.ledger.write.*` claims. These operations go through the full query engine and dataset policy enforcement. The server never exposes raw storage bytes through query endpoints.

- `fluree.ledger.read.all: true` / `fluree.ledger.read.ledgers: [...]` — read scope
- `fluree.ledger.write.all: true` / `fluree.ledger.write.ledgers: [...]` — write scope

Query tokens are appropriate for **end users and application service accounts**. When combined with a `fluree.identity` claim and dataset policies, the server enforces fine-grained access control.

### CLI consequence: `track` vs `pull`

- **`fluree pull`** (replication): downloads raw commits and indexes into local storage. Requires `fluree.storage.*`. The local copy is bit-identical to the remote.
- **`fluree track`** (query): registers a remote ledger without local data. All queries and transactions are forwarded to the remote server. Requires only `fluree.ledger.read/write.*`.

If a user holds only policy-scoped tokens (no `fluree.storage.*`), they **cannot** clone or pull a ledger. They can only `track` it and issue queries/transactions against the remote.

---

## CLI ↔ Server auth contract

This section defines the wire-level contract between the CLI and any Fluree-compatible server (Solo, standalone `fluree-server`, or future products). Any implementation that exposes these endpoints will get zero-configuration CLI auth.

### Auth discovery: `GET /.well-known/fluree.json`

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

**`auth.type` values:**

| Type | Meaning | CLI behavior |
|------|---------|--------------|
| `oidc_device` | OIDC Device Authorization Grant + token exchange | `fluree auth login` runs device flow, then calls `exchange_url` |
| `token` | Manual Bearer token (no automated login flow) | `fluree auth login --token <value>` |

**Field reference (`oidc_device`):**

| Field | Required | Description |
|-------|----------|-------------|
| `issuer` | Yes | OIDC issuer URL (used for `/.well-known/openid-configuration` discovery) |
| `client_id` | Yes | OAuth client ID for the CLI (must be registered for device flow) |
| `exchange_url` | Yes | Absolute URL for the Fluree token exchange endpoint (see below) |
| `scopes` | No | OAuth scopes to request (default: `["openid"]`) |

**Fallback behavior:**
- Discovery endpoint absent (404/connection error) → CLI assumes `token` type, prompts user to provide a token manually.
- `version` > 1 → CLI warns but attempts to parse known fields.

### Token exchange: `POST {exchange_url}`

After the CLI completes the OIDC device flow with the IdP, it calls the exchange endpoint to trade the IdP token for a Fluree-scoped Bearer token. This endpoint is hosted by Solo (or any Fluree application that manages authorization).

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

**Contract:**
- The exchange endpoint validates the IdP token (against the IdP's JWKS or userinfo), looks up the user's Fluree entitlements, and mints a Fluree-scoped JWT.
- The returned `access_token` MUST be a JWT that `fluree-server` can verify (via JWKS). It MUST include the standard Fluree claims (`fluree.identity`, `fluree.ledger.*`, and optionally `fluree.storage.*`).
- `refresh_token` is OPTIONAL. If present, the CLI stores it and uses it for silent refresh (see below).
- The `subject_token_type` MAY be `urn:ietf:params:oauth:token-type:id_token` if the CLI sends the ID token instead of the access token.

### Token refresh: `POST {exchange_url}`

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

### CLI TOML config format

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

### CLI `fluree auth login` behavior

```
fluree auth login [--remote <name>]
```

1. Resolve the target remote.
2. Check `auth.type`:
   - **`oidc_device`**:
     a. Discover OIDC endpoints from `{issuer}/.well-known/openid-configuration`.
     b. POST to `device_authorization_endpoint` to get `device_code`, `user_code`, `verification_uri`.
     c. Print: `Open {verification_uri} and enter code: {user_code}`
     d. Poll `token_endpoint` until user completes browser auth.
     e. POST IdP token to `exchange_url` → get Fluree Bearer token.
     f. Store `token` and `refresh_token` in remote config.
   - **`token`**: Prompt for token (or accept `--token <value|@file|@->`). Store in config.
   - **Unset / no discovery**: Attempt discovery at `{base_url}/.well-known/fluree.json`. If found, configure auth type and proceed. If not found, fall back to `token` flow.

### CLI auto-refresh on 401

When any CLI command gets a 401 from the remote:

1. If `auth.type == "oidc_device"` and `refresh_token` is present:
   - Attempt silent refresh via the exchange endpoint.
   - On success: update stored token, retry the original request once.
   - On failure: clear tokens, print `Token expired. Run: fluree auth login --remote <name>`
2. Otherwise: print `Authentication failed. Run: fluree auth login --remote <name>`

### Scope rules (hard line)

- Exchange endpoint MUST NOT grant `fluree.storage.*` to regular users. Replication scope is for operators and service accounts only.
- If a user with only query-scoped tokens attempts `fluree pull` or `fluree fetch`, the CLI MUST fail with a clear message explaining that replication requires `fluree.storage.*` and suggesting `fluree track` instead.

---

## Summary: the uniform story

- Fluree's **policy identity remains DID-centric** (portable, decentralized).
- The CLI supports three **auth flows**: OIDC device code + token exchange (Solo), manual token import (any server), and did:key signed requests (standalone server).
- `fluree remote add <url>` auto-discovers auth requirements via `/.well-known/fluree.json`. Zero-config for Solo users.
- Solo (or any Fluree application) exposes a **token exchange endpoint** that trades IdP tokens for Fluree-scoped Bearer tokens.
- Standalone `fluree-server` can start with **offline-minted tokens** immediately, optionally adding registration endpoints later.
- **Replication is root-only**: `fluree.storage.*` tokens grant raw storage access for `fetch`/`pull`/`push` and peer sync. Policy-scoped tokens (`fluree.ledger.read/write.*`) are query-only — users can `track` a remote ledger but cannot clone it.

