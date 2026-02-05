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
- Ledger alias and address
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
  Address:  fluree:file://storage/mydb
  t:        5
  Commit:   fluree:file://storage/mydb/commit/abc123...
```

## See Also

- [list](list.md) - List all ledgers
- [log](log.md) - Show commit history
