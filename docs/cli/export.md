# fluree export

Export ledger data as Turtle or JSON-LD.

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
| `--format <FORMAT>` | Output format: `turtle` or `jsonld` (default: `turtle`) |
| `--at <TIME>` | Export at a specific point in time |

## Description

Exports all data from a ledger in the specified format. Output goes to stdout so it can be redirected to a file or piped to other tools.

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

# Pipe to other tools
fluree export | grep "example.org"
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

## Time Travel

Export historical snapshots:

```bash
# Export at transaction 5
fluree export --at 5 > snapshot.ttl

# Export at specific commit
fluree export --at abc123def > snapshot.ttl
```

## See Also

- [query](query.md) - Run custom queries
- [create](create.md) - Create ledger with `--from` to import
