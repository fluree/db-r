# fluree info

Show detailed information about a ledger.

## Usage

```bash
fluree info [LEDGER]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to active ledger) |

## Description

Displays detailed information about a ledger including:
- Ledger alias and ID
- Current transaction number (t)
- Branch information
- Commit details

## Examples

```bash
# Info for active ledger
fluree info

# Info for specific ledger
fluree info production
```

## Output

```
Ledger: mydb
  Alias:    mydb
  Branch:   main
  t:        5
  Commit ID: bafybeig...
```

## See Also

- [list](list.md) - List all ledgers
- [log](log.md) - Show commit history
