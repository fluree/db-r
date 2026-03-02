# Ledger Configuration (Config Graph)

Fluree stores **ledger-level configuration as data** inside each ledger, in a dedicated system graph called the **config graph**. This is distinct from [server configuration](../operations/configuration.md) (TOML files, environment variables) which controls how the Fluree process runs.

The config graph holds RDF triples that define operational defaults for the ledger: which policy rules apply, whether SHACL validation runs, what reasoning modes are active, which properties enforce uniqueness, and more. Because config lives inside the ledger, it is:

- **Immutable and time-travelable** — config at any historical `t` is recoverable
- **Auditable** — every config change is a signed, committed transaction
- **Replicable** — config travels with the ledger across nodes and forks
- **Replay-safe** — deterministic interpretation without runtime environment state

## Graph layout

Every ledger reserves system named graphs:

| Graph | IRI pattern | Purpose |
|-------|-------------|---------|
| Default graph | (implicit) | Application data |
| Txn-meta | `urn:fluree:{ledger_id}#txn-meta` | Commit metadata |
| **Config graph** | `urn:fluree:{ledger_id}#config` | Ledger configuration |

User-defined named graphs (created via TriG) are identified by their IRI and allocated after the system graphs.

The config graph IRI is deterministic — derived from the ledger identifier. For a ledger `mydb:main`, the config graph is `urn:fluree:mydb:main#config`.

## Core concepts

### `f:LedgerConfig`

A single `f:LedgerConfig` resource in the config graph defines ledger-wide defaults. If multiple exist, the one with the lexicographically smallest `@id` wins (with a logged warning).

### Setting groups

Configuration is organized into independent **setting groups**, each governing a different subsystem:

| Setting group | Subsystem | Key fields |
|---------------|-----------|------------|
| [`f:policyDefaults`](setting-groups.md#policy-defaults) | Policy enforcement | `f:defaultAllow`, `f:policySource`, `f:policyClass` |
| [`f:shaclDefaults`](setting-groups.md#shacl-defaults) | SHACL validation | `f:shaclEnabled`, `f:shapesSource`, `f:validationMode` |
| [`f:reasoningDefaults`](setting-groups.md#reasoning-defaults) | OWL/RDFS reasoning | `f:reasoningModes`, `f:schemaSource` |
| [`f:datalogDefaults`](setting-groups.md#datalog-defaults) | Datalog rules | `f:datalogEnabled`, `f:rulesSource` |
| [`f:transactDefaults`](setting-groups.md#transact-defaults) | Transaction constraints | `f:uniqueEnabled`, `f:constraintsSource` |

Each group is resolved independently — locking down policy does not affect whether reasoning can be overridden.

### Per-graph overrides

Ledger-wide defaults apply to all graphs. For finer control, `f:graphOverrides` on the `f:LedgerConfig` contains `f:GraphConfig` entries that override settings for specific named graphs. See [Override control](override-control.md) for the full resolution model.

### Privileged system read

Config is read via a **privileged system read** that bypasses policy enforcement. This is necessary because config defines the policy — reading it through the policy-enforced path would create a circular dependency. User queries against the config graph still go through normal policy enforcement.

### Lagging config

Config changes take effect on the **next** transaction, not the current one. The transaction pipeline reads config from the pre-transaction state. This prevents a transaction from "authorizing itself" by changing config within its own payload.

## In this section

- [Writing config data](writing-config.md) — How to create and update config via TriG, SPARQL, or JSON-LD
- [Setting groups](setting-groups.md) — All setting groups with fields and examples
- [Override control](override-control.md) — Resolution precedence, identity gating, monotonicity
- [Unique constraints](unique-constraints.md) — Enforcing property value uniqueness with `f:enforceUnique`
