# fluree insert

Insert data into a ledger.

## Usage

```bash
fluree insert [LEDGER] [FILE] [OPTIONS]
```

## Arguments

| Arguments | Behavior |
|-----------|----------|
| (none) | Active ledger + stdin or `-e` expression |
| `<arg>` | If file exists: active ledger + file; else: ledger alias + stdin/-e |
| `<ledger> <file>` | Specified ledger + file |

## Options

| Option | Description |
|--------|-------------|
| `-e, --expr <EXPR>` | Inline data expression |
| `-m, --message <MSG>` | Commit message |
| `--format <FORMAT>` | Data format: `turtle` or `jsonld` (auto-detected if omitted) |

## Description

Inserts RDF data into a ledger. Supports both Turtle and JSON-LD formats. Data can come from:
- A file (positional argument)
- Inline expression (`-e`)
- Standard input (pipe or interactive)

## Examples

```bash
# Insert from file
fluree insert data.ttl

# Insert inline Turtle
fluree insert -e '@prefix ex: <http://example.org/> .
ex:alice a ex:Person ; ex:name "Alice" .'

# Insert inline JSON-LD
fluree insert -e '{"@id": "ex:bob", "ex:name": "Bob"}'

# Insert with commit message
fluree insert data.ttl -m "Added initial users"

# Insert into specific ledger
fluree insert production data.ttl

# Pipe from stdin
cat data.ttl | fluree insert
```

## Output

```
Committed t=1 (42 flakes)
```

With verbose mode:
```
Committed t=1 (42 flakes)
Address: fluree:file://storage/mydb/commit/abc123...
```

## Data Format Detection

The format is auto-detected:
- `@prefix` or `@base` at line start → Turtle
- Starts with `{` or `[` → JSON-LD
- `.ttl` file extension → Turtle
- `.json` or `.jsonld` extension → JSON-LD

Override with `--format turtle` or `--format jsonld`.

## See Also

- [upsert](upsert.md) - Insert or update existing data
- [query](query.md) - Query the inserted data
- [export](export.md) - Export all data
