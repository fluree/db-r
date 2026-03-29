# fluree export

Export ledger data as Turtle, JSON-LD, or a native ledger pack.

## Usage

```bash
fluree export [LEDGER] [OPTIONS]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to active ledger) |

## Options

| Option | Description |
|--------|-------------|
| `--format <FORMAT>` | Output format: `turtle`, `jsonld`, or `ledger` (default: `turtle`) |
| `--at <TIME>` | Export at a specific point in time (turtle/jsonld only) |
| `-o, --output <PATH>` | Output file path (ledger format only; defaults to `<ledger>.flpack`) |

## Formats

### turtle / jsonld (data snapshot)

Exports a point-in-time snapshot of all triples in the ledger. Output goes to stdout.

### ledger (native pack)

Exports the full native ledger — all commits, transaction blobs, indexes, and dictionaries — as a `.flpack` file. This format preserves the complete history and can be imported into a new Fluree instance via `fluree create <name> --from <file>.flpack`.

The `.flpack` format uses the `fluree-pack-v1` binary wire protocol (the same format used by `fluree clone` and `fluree pull` for network transfers).

## Examples

```bash
# Export as Turtle (default)
fluree export > backup.ttl

# Export as JSON-LD
fluree export --format jsonld > backup.jsonld

# Export specific ledger
fluree export production > prod-backup.ttl

# Export historical snapshot
fluree export --at 10 > snapshot-t10.ttl

# Export full native ledger (all history + indexes)
fluree export --format ledger
fluree export mydb --format ledger -o /backups/mydb.flpack

# Import into a new instance (can use a different name)
fluree create newdb --from mydb.flpack
```

## Output

### Turtle

```turtle
@prefix ex: <http://example.org/> .

ex:alice a ex:Person ;
    ex:name "Alice" ;
    ex:age 30 .

ex:bob a ex:Person ;
    ex:name "Bob" .
```

### JSON-LD

```json
{
  "@context": {...},
  "@graph": [
    {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice", "ex:age": 30},
    {"@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob"}
  ]
}
```

### Ledger (.flpack)

Binary file containing the full pack stream. Use `fluree create <name> --from <file>.flpack` to import.

## Time Travel

Export historical snapshots (turtle/jsonld only):

```bash
# Export at transaction 5
fluree export --at 5 > snapshot.ttl

# Export at specific commit
fluree export --at abc123def > snapshot.ttl
```

## See Also

- [create](create.md) - Create ledger with `--from` to import `.flpack` files
- [publish](publish.md) - Publish a local ledger to a remote server
- [query](query.md) - Run custom queries
