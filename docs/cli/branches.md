# fluree branches

List all branches for a ledger.

## Usage

```bash
fluree branches [LEDGER] [OPTIONS]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to active ledger) |

## Options

| Option | Description |
|--------|-------------|
| `--remote <REMOTE>` | List branches on a remote server |

## Description

Lists all non-retracted branches for a ledger, showing each branch's name, current transaction time, and source branch (if it was created via `fluree branch`).

## Examples

```bash
# List branches for the active ledger
fluree branches

# List branches for a specific ledger
fluree branches mydb

# List branches on a remote server
fluree branches mydb --remote origin
```

## Output

```
 BRANCH     T   SOURCE
 main       5   -
 dev        7   main
 feature-x  8   dev
```

## See Also

- [branch](branch.md) - Create a new branch
- [list](list.md) - List all ledgers
- [info](info.md) - Show ledger details
