# fluree iceberg

Manage Apache Iceberg table connections.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `map` | Map an Iceberg table as a graph source |

## fluree iceberg map

Map an Iceberg table as a queryable graph source.

### Usage

```bash
fluree iceberg map <NAME> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<NAME>` | Graph source name (e.g., "warehouse-orders") |

### Options

**Catalog mode:**

| Option | Description |
|--------|-------------|
| `--mode <MODE>` | Catalog mode: `rest` (default) or `direct` |

**REST catalog mode options:**

| Option | Description |
|--------|-------------|
| `--catalog-uri <URI>` | REST catalog URI (required for rest mode) |
| `--table <ID>` | Table identifier in `namespace.table` format (required without `--r2rml`) |
| `--warehouse <NAME>` | Warehouse identifier |
| `--no-vended-credentials` | Disable vended credentials (enabled by default) |

**Direct S3 mode options:**

| Option | Description |
|--------|-------------|
| `--table-location <URI>` | S3 table location (required for direct mode, e.g., `s3://bucket/warehouse/ns/table`) |

**R2RML mapping:**

| Option | Description |
|--------|-------------|
| `--r2rml <PATH>` | R2RML mapping file (Turtle format). When provided, table references come from the mapping's `rr:tableName` entries. |
| `--r2rml-type <TYPE>` | Mapping media type (e.g., `text/turtle`); inferred from extension if omitted |

**Authentication:**

| Option | Description |
|--------|-------------|
| `--auth-bearer <TOKEN>` | Bearer token for REST catalog authentication |
| `--oauth2-token-url <URL>` | OAuth2 token URL for client credentials auth |
| `--oauth2-client-id <ID>` | OAuth2 client ID |
| `--oauth2-client-secret <SECRET>` | OAuth2 client secret |

**S3 overrides:**

| Option | Description |
|--------|-------------|
| `--s3-region <REGION>` | S3 region override |
| `--s3-endpoint <URL>` | S3 endpoint override (for MinIO, LocalStack) |
| `--s3-path-style` | Use path-style S3 URLs |

**Other:**

| Option | Description |
|--------|-------------|
| `--branch <NAME>` | Branch name (defaults to "main") |

### Description

Maps an Apache Iceberg table as a graph source that can be queried using SPARQL or JSON-LD queries. The table is accessed read-only; Fluree does not modify the Iceberg table.

Two catalog modes are supported:

- **REST mode** (default): Connects to an Iceberg REST catalog (e.g., Apache Polaris) to discover table metadata. Supports vended credentials and warehouse selection.
- **Direct S3 mode**: Reads table metadata directly from S3 by resolving `version-hint.text` in the table's `metadata/` directory. No catalog server required.

When `--r2rml` is provided, the mapping file defines how Iceberg table rows are transformed into RDF triples. Without `--r2rml`, the table is accessible as a raw Iceberg graph source.

### Examples

```bash
# REST catalog, raw Iceberg (no R2RML)
fluree iceberg map warehouse-orders \
  --catalog-uri https://polaris.example.com/api/catalog \
  --table sales.orders \
  --auth-bearer $POLARIS_TOKEN \
  --warehouse my-warehouse

# REST catalog with R2RML mapping
fluree iceberg map airlines \
  --catalog-uri https://polaris.example.com/api/catalog \
  --r2rml mappings/airlines.ttl \
  --auth-bearer $POLARIS_TOKEN

# Direct S3 (no catalog server)
fluree iceberg map execution-log \
  --mode direct \
  --table-location s3://my-bucket/warehouse/logs/execution_log \
  --s3-region us-east-1

# OAuth2 authentication
fluree iceberg map orders \
  --catalog-uri https://polaris.example.com/api/catalog \
  --table sales.orders \
  --oauth2-token-url https://auth.example.com/token \
  --oauth2-client-id my-client \
  --oauth2-client-secret $CLIENT_SECRET
```

### Output

Without R2RML:
```
Mapped Iceberg table as graph source 'warehouse-orders:main'
  Table:       sales.orders
  Catalog:     https://polaris.example.com/api/catalog
  Connection:  verified
```

With R2RML:
```
Mapped Iceberg table as R2RML graph source 'airlines:main'
  Table:       openflights.airlines
  Catalog:     https://polaris.example.com/api/catalog
  R2RML:       mappings/airlines.ttl
  TriplesMaps: 3
  Connection:  verified
  Mapping:     validated
```

### After Mapping

Once mapped, the graph source appears in standard commands:

```bash
# Listed alongside ledgers
fluree list

# Inspect configuration
fluree info warehouse-orders

# Query via SPARQL GRAPH pattern
fluree query mydb 'SELECT ?id ?total FROM <mydb:main> WHERE { GRAPH <warehouse-orders:main> { ?o ex:id ?id ; ex:total ?total } }'

# Remove the mapping
fluree drop warehouse-orders --force
```

### Feature Flag

Requires the `iceberg` feature flag. Without it, the command returns:
```
error: Iceberg support not compiled. Rebuild with `--features iceberg`.
```

## See Also

- [Iceberg / Parquet](../graph-sources/iceberg.md) - Iceberg integration details
- [R2RML](../graph-sources/r2rml.md) - R2RML mapping reference
- [list](list.md) - List ledgers and graph sources
- [info](info.md) - Show graph source details
- [drop](drop.md) - Remove a graph source
