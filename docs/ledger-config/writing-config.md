# Writing Config Data

The config graph is mutated using normal ledger transactions — config writes are signed, versioned, and replicable like any other write. The only difference is that the triples target the config graph IRI.

## Config graph IRI

Each ledger's config graph has a deterministic IRI:

```
urn:fluree:{ledger_id}#config
```

For a ledger named `mydb:main`, the config graph is `urn:fluree:mydb:main#config`.

## Writing via TriG

TriG is the most natural format for writing to named graphs. Wrap your config triples in a `GRAPH` block targeting the config graph IRI:

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main#config#ledger> a f:LedgerConfig ;
    f:policyDefaults [
      f:defaultAllow false
    ] ;
    f:shaclDefaults [
      f:shaclEnabled true ;
      f:validationMode f:ValidationReject
    ] .
}
```

## Writing via SPARQL UPDATE

Use `INSERT DATA` with a `GRAPH` clause:

```sparql
PREFIX f: <https://ns.flur.ee/db#>

INSERT DATA {
  GRAPH <urn:fluree:mydb:main#config> {
    <urn:fluree:mydb:main#config#ledger> a f:LedgerConfig ;
      f:reasoningDefaults [
        f:reasoningModes f:RDFS ;
        f:schemaSource [
          a f:GraphRef ;
          f:graphSource [ f:graphSelector f:defaultGraph ]
        ]
      ] .
  }
}
```

## Writing via JSON-LD

Use the `@graph` key with a named graph wrapper:

```json
{
  "@context": { "f": "https://ns.flur.ee/db#" },
  "@graph": [
    {
      "@id": "urn:fluree:mydb:main#config#ledger",
      "@type": "f:LedgerConfig",
      "@graph": "urn:fluree:mydb:main#config",
      "f:shaclDefaults": {
        "f:shaclEnabled": true,
        "f:validationMode": { "@id": "f:ValidationReject" }
      }
    }
  ]
}
```

## Updating config

Config changes are normal ledger operations. To change a setting, use a `DELETE/INSERT WHERE` pattern that binds the existing blank node:

```sparql
PREFIX f: <https://ns.flur.ee/db#>

DELETE {
  GRAPH <urn:fluree:mydb:main#config> {
    ?policy f:defaultAllow false .
  }
}
INSERT {
  GRAPH <urn:fluree:mydb:main#config> {
    ?policy f:defaultAllow true .
  }
}
WHERE {
  GRAPH <urn:fluree:mydb:main#config> {
    <urn:fluree:mydb:main#config#ledger> f:policyDefaults ?policy .
    ?policy f:defaultAllow false .
  }
}
```

This pattern binds `?policy` to the existing setting-group blank node, retracts the old value, and asserts the new one. It avoids the problem of `DELETE DATA` with blank nodes (which cannot match stored blank node identities).

Alternatively, give setting-group nodes explicit IRIs so they can be addressed directly:

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main#config#ledger> a f:LedgerConfig ;
    f:policyDefaults <urn:fluree:mydb:main#config#policy> .

  <urn:fluree:mydb:main#config#policy>
    f:defaultAllow false ;
    f:overrideControl f:OverrideAll .
}
```

With explicit IRIs, individual fields can be retracted by subject IRI without binding.

Retracting a field returns the ledger to the system default for that setting (as if the field were absent).

## Config mutation governance

Config writes go through the normal policy-enforced transaction path. This means:

- **Reading** config is privileged (system read, bypasses policy) — necessary to bootstrap.
- **Writing** config is **not** privileged — policy enforcement applies.

A `defaultAllow: false` config is self-protecting: the policy it defines must explicitly grant write access to the config graph for any changes to be possible.

If a ledger becomes unmodifiable due to a policy misconfiguration (no authorized config writers), recovery requires a ledger fork/restore — there is no superuser bypass.

## Recommended subject IRI

For operational simplicity, use a stable, conventional subject IRI:

```
urn:fluree:{ledger_id}#config#ledger
```

This avoids accidental multiple-config instances and makes debugging simpler.

## Lagging semantics

Config changes take effect on the **next** transaction. The transaction pipeline reads config from the pre-transaction state (`t - 1`). This prevents a transaction from changing the rules it is validated against.

This means:
- Enabling SHACL in the same transaction as invalid data will **not** reject that data
- Enabling `f:uniqueEnabled` in the same transaction as duplicate values will **not** reject those duplicates
- The next transaction after the config change will be validated against the new config
