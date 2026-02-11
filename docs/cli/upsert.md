# fluree upsert

Upsert data into a ledger (insert or update existing).

## Usage

```bash
fluree upsert [LEDGER] [FILE] [OPTIONS]
```

## Arguments

| Arguments | Behavior |
|-----------|----------|
| (none) | Active ledger + stdin or `-e` expression |
| `<arg>` | If file exists: active ledger + file; else: ledger ID + stdin/-e |
| `<ledger> <file>` | Specified ledger + file |

## Options

| Option | Description |
|--------|-------------|
| `-e, --expr <EXPR>` | Inline data expression |
| `-m, --message <MSG>` | Commit message |
| `--format <FORMAT>` | Data format: `turtle` or `jsonld` (auto-detected if omitted) |

## Description

Upserts RDF data into a ledger. Unlike `insert`, upsert will:
- Insert new entities
- Replace existing values for entities that already exist (matched by `@id`)

This is useful for updating data without needing to know whether it exists.

## Examples

```bash
# Update or insert a user
fluree upsert -e '@prefix ex: <http://example.org/> .
ex:alice ex:name "Alice Smith" ; ex:age 31 .'

# Upsert from file
fluree upsert updates.ttl

# Upsert with commit message
fluree upsert -e '{"@id": "ex:alice", "ex:status": "active"}' -m "Updated Alice status"
```

## Output

```
Committed t=2 (3 flakes)
```

## Difference from Insert

| Operation | Existing Entity | New Entity |
|-----------|-----------------|------------|
| `insert` | Adds new triples (may create duplicates) | Creates entity |
| `upsert` | Replaces values for given predicates | Creates entity |

## See Also

- [insert](insert.md) - Insert without replacement
- [query](query.md) - Query data
- [history](history.md) - View change history
