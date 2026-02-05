# fluree drop

Drop (delete) a ledger.

## Usage

```bash
fluree drop <NAME> --force
```

## Arguments

| Argument | Description |
|----------|-------------|
| `<NAME>` | Ledger name to drop |

## Options

| Option | Description |
|--------|-------------|
| `--force` | Required flag to confirm deletion |

## Description

Permanently deletes a ledger and all its data. The `--force` flag is required to prevent accidental deletion.

## Examples

```bash
# Delete a ledger (requires --force)
fluree drop oldledger --force
```

## Output

```
Dropped ledger 'oldledger'
```

## Errors

Without `--force`:
```
error: dropping a ledger is destructive; use --force to confirm
```

## See Also

- [create](create.md) - Create a new ledger
- [list](list.md) - List all ledgers
