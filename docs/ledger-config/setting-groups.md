# Setting Groups

Each setting group configures a different subsystem. Groups are resolved independently — locking down one group does not affect others.

All setting groups can appear on both `f:LedgerConfig` (ledger-wide defaults) and `f:GraphConfig` (per-graph overrides), except where noted.

---

## Policy defaults

**Group predicate**: `f:policyDefaults`

Controls default policy enforcement behavior.

| Field | Type | Description |
|-------|------|-------------|
| `f:defaultAllow` | boolean | Allow (`true`) or deny (`false`) when no policy rule matches |
| `f:policySource` | `f:GraphRef` | Graph containing policy rules (`f:Allow`, `f:Modify`, etc.) |
| `f:policyClass` | IRI or list | Default policy classes to apply |
| `f:overrideControl` | IRI or object | Override gating (see [Override control](override-control.md)) |

`f:policySource` is non-overridable — it can only be changed by writing to the config graph, not at query time. `f:defaultAllow` and `f:policyClass` are overridable (subject to override control).

### Example

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main#config#ledger> a f:LedgerConfig ;
    f:policyDefaults [
      f:defaultAllow false ;
      f:policySource [
        a f:GraphRef ;
        f:graphSource [ f:graphSelector f:defaultGraph ]
      ] ;
      f:overrideControl f:OverrideAll
    ] .
}
```

---

## SHACL defaults

**Group predicate**: `f:shaclDefaults`

Controls SHACL shape validation at transaction time. Requires the `shacl` feature flag at compile time.

| Field | Type | Description |
|-------|------|-------------|
| `f:shaclEnabled` | boolean | Enable or disable SHACL validation |
| `f:shapesSource` | `f:GraphRef` | Graph containing SHACL shapes |
| `f:validationMode` | IRI | `f:ValidationReject` (reject invalid data) or `f:ValidationWarn` (log warning, allow) |
| `f:overrideControl` | IRI or object | Override gating |

`f:shapesSource` is non-overridable. `f:shaclEnabled` and `f:validationMode` are overridable.

### Example

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main#config#ledger> a f:LedgerConfig ;
    f:shaclDefaults [
      f:shaclEnabled true ;
      f:shapesSource [
        a f:GraphRef ;
        f:graphSource [ f:graphSelector f:defaultGraph ]
      ] ;
      f:validationMode f:ValidationReject ;
      f:overrideControl f:OverrideNone
    ] .
}
```

---

## Reasoning defaults

**Group predicate**: `f:reasoningDefaults`

Controls OWL/RDFS reasoning applied at query time.

| Field | Type | Description |
|-------|------|-------------|
| `f:reasoningModes` | IRI or list | Reasoning modes: `f:RDFS`, `f:OWL2QL`, `f:OWL2RL`, `f:Datalog` |
| `f:schemaSource` | `f:GraphRef` | Graph containing schema triples (`rdfs:subClassOf`, etc.) |
| `f:overrideControl` | IRI or object | Override gating |

`f:schemaSource` is non-overridable. `f:reasoningModes` is overridable.

### Example

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main#config#ledger> a f:LedgerConfig ;
    f:reasoningDefaults [
      f:reasoningModes f:RDFS ;
      f:schemaSource [
        a f:GraphRef ;
        f:graphSource [ f:graphSelector f:defaultGraph ]
      ] ;
      f:overrideControl f:OverrideAll
    ] .
}
```

---

## Datalog defaults

**Group predicate**: `f:datalogDefaults`

Controls Fluree's stored datalog rules (`f:rule`).

| Field | Type | Description |
|-------|------|-------------|
| `f:datalogEnabled` | boolean | Enable or disable datalog rule evaluation |
| `f:rulesSource` | `f:GraphRef` | Graph containing `f:rule` definitions |
| `f:allowQueryTimeRules` | boolean | Allow queries to supply ad-hoc rules |
| `f:overrideControl` | IRI or object | Override gating |

`f:rulesSource` is non-overridable. `f:datalogEnabled` and `f:allowQueryTimeRules` are overridable.

### Example

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main#config#ledger> a f:LedgerConfig ;
    f:datalogDefaults [
      f:datalogEnabled true ;
      f:rulesSource [
        a f:GraphRef ;
        f:graphSource [ f:graphSelector f:defaultGraph ]
      ] ;
      f:allowQueryTimeRules false ;
      f:overrideControl f:OverrideNone
    ] .
}
```

---

## Transact defaults

**Group predicate**: `f:transactDefaults`

Controls transaction-time constraint enforcement, such as property value uniqueness.

| Field | Type | Description |
|-------|------|-------------|
| `f:uniqueEnabled` | boolean | Enable unique constraint enforcement |
| `f:constraintsSource` | `f:GraphRef` or list | Graph(s) containing constraint annotations (e.g., `f:enforceUnique`) |
| `f:overrideControl` | IRI or object | Override gating |

When `f:uniqueEnabled` is `true` and `f:constraintsSource` is omitted, the default graph is used as the constraint source.

### Additive merge semantics

Unlike other setting groups where per-graph values **replace** ledger-wide values field-by-field, transact defaults use **additive** merge semantics:

- **`f:uniqueEnabled`**: Once enabled at the ledger level, it stays enabled for all graphs. Per-graph configs cannot disable it.
- **`f:constraintsSource`**: Per-graph sources are **added to** ledger-wide sources, not substituted. A graph checks annotations from all sources (ledger-wide + graph-specific).

This prevents a per-graph override from accidentally disabling enforcement or dropping constraint sources.

Note: additive merge is still subject to override control. If the ledger-wide `f:overrideControl` for `f:transactDefaults` is `f:OverrideNone`, per-graph additions are blocked entirely — the ledger-wide settings are final.

### Example

```trig
@prefix f: <https://ns.flur.ee/db#> .
@prefix ex: <http://example.org/ns/> .

# Define constraint annotations in the default graph
ex:email f:enforceUnique true .
ex:ssn   f:enforceUnique true .

# Enable enforcement via config
GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main#config#ledger> a f:LedgerConfig ;
    f:transactDefaults [
      f:uniqueEnabled true ;
      f:constraintsSource [
        a f:GraphRef ;
        f:graphSource [ f:graphSelector f:defaultGraph ]
      ]
    ] .
}
```

See [Unique constraints](unique-constraints.md) for full details on `f:enforceUnique`.

---

## Ledger-scoped settings

Some settings are structurally tied to the ledger as a whole and are **not meaningful per-graph**. They live exclusively on `f:LedgerConfig` and are ignored if present on `f:GraphConfig`:

| Field | Description |
|-------|-------------|
| `f:authzSource` | Identity/relationship graph used by policy evaluation |

Override control does not apply to ledger-scoped settings — they are changed only by writing to the config graph.

---

## `f:GraphRef`: referencing source graphs

Several fields (`f:policySource`, `f:shapesSource`, `f:schemaSource`, `f:rulesSource`, `f:constraintsSource`) use `f:GraphRef` to point at graphs containing rules, shapes, schema, or constraints.

A `f:GraphRef` has two levels: the outer node carries the type and optional trust/rollback settings, and a nested `f:graphSource` object carries the source coordinates:

| Field | Level | Type | Description |
|-------|-------|------|-------------|
| `f:graphSource` | `f:GraphRef` | object | Nested source coordinates (required) |
| `f:trustPolicy` | `f:GraphRef` | object | How to verify the referenced graph (future) |
| `f:rollbackGuard` | `f:GraphRef` | object | Freshness constraints (future) |
| `f:graphSelector` | `f:graphSource` | IRI | Target graph: `f:defaultGraph`, `f:txnMetaGraph`, or a named graph IRI |
| `f:ledger` | `f:graphSource` | IRI | Ledger identifier (for cross-ledger references; not yet supported for constraint sources) |
| `f:atT` | `f:graphSource` | integer | Pin to a specific transaction time (optional) |

For the common case of referencing a graph within the same ledger, only `f:graphSelector` is needed inside `f:graphSource`:

```trig
f:shapesSource [
  a f:GraphRef ;
  f:graphSource [ f:graphSelector f:defaultGraph ]
] .
```

For referencing the config graph itself (co-resident rules/shapes):

```trig
f:policySource [
  a f:GraphRef ;
  f:graphSource [ f:graphSelector <urn:fluree:mydb:main#config> ]
] .
```

Cross-ledger `f:GraphRef` (using `f:ledger` to reference another ledger) is defined in the schema but not yet supported for constraint source resolution. Currently, only local graph references are resolved.
