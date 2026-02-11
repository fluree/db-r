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
| `--chunk-size-mb <MB>` | Chunk size in MB for splitting large Turtle files (0 = derive from memory budget). Only used when `--from` points to a `.ttl` file. |

**Global flags** that affect bulk import when using `--from` (see [CLI README](README.md#global-options)):

- `--memory-budget-mb <MB>` — Memory budget in MB (0 = auto: 75% of system RAM). Drives chunk size, concurrency, and indexer run budget.
- `--parallelism <N>` — Number of parallel parse threads (0 = auto: system cores, cap 6).

## Description

Creates a new empty ledger with the given name and sets it as the active ledger. The ledger is stored in `.fluree/storage/`.

Use `--from` to create a ledger pre-populated with data from a Turtle or JSON-LD file. For large Turtle files, the CLI splits work into chunks and runs parallel parse threads; tune with `--memory-budget-mb` and `--parallelism` if needed.

## Examples

```bash
# Create an empty ledger
fluree create mydb

# Create with initial data
fluree create mydb --from seed-data.ttl

# Create from JSON-LD
fluree create mydb --from initial.jsonld

# Create with explicit memory and parallelism for a large Turtle file
fluree create mydb --from large.ttl --memory-budget-mb 4096 --parallelism 8
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
