# fluree create

Create a new ledger.

## Usage

```bash
fluree create <LEDGER> [OPTIONS]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `<LEDGER>` | Name for the new ledger |

## Options

| Option | Description |
|--------|-------------|
| `--from <PATH>` | Import data from a file (Turtle or JSON-LD) |

## Description

Creates a new empty ledger with the given name and sets it as the active ledger. The ledger is stored in `.fluree/storage/`.

Use `--from` to create a ledger pre-populated with data from a Turtle or JSON-LD file.

## Examples

```bash
# Create an empty ledger
fluree create mydb

# Create with initial data
fluree create mydb --from seed-data.ttl

# Create from JSON-LD
fluree create mydb --from initial.jsonld
```

## Output

```
Created ledger 'mydb'
Set 'mydb' as active ledger
```

With `--from`:
```
Created ledger 'mydb'
Committed t=1 (42 flakes)
Set 'mydb' as active ledger
```

## See Also

- [list](list.md) - List all ledgers
- [use](use.md) - Switch active ledger
- [drop](drop.md) - Delete a ledger
