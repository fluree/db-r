# fluree info

Show detailed information about a ledger or graph source.

## Usage

```bash
fluree info [NAME]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `[NAME]` | Ledger or graph source name (defaults to active ledger) |

## Description

Displays detailed information about a ledger or graph source. The command first checks for a matching ledger; if none is found, it checks for a graph source with the same name.

For ledgers, displays:
- Ledger ID, branch, and type
- Current transaction number (t)
- Commit and index details

For graph sources (Iceberg, R2RML, BM25, etc.), displays:
- Name, branch, and type
- Graph source ID
- Index status
- Dependencies
- Configuration (catalog URI, table, mapping, etc.)

## Examples

```bash
# Info for active ledger
fluree info

# Info for specific ledger
fluree info production

# Info for a graph source
fluree info warehouse-orders
```

## Output

Ledger:
```
Ledger:         mydb
Branch:         main
Type:           Ledger
Ledger ID:      mydb:main
Commit t:       5
Commit ID:      bafybeig...
Index t:        5
Index ID:       bafybeig...
```

Graph source (Iceberg):
```
Name:           warehouse-orders
Branch:         main
Type:           Iceberg
ID:             warehouse-orders:main
Retracted:      false
Index t:        0
Index ID:       (none)

Configuration:
{
  "catalog": {
    "type": "rest",
    "uri": "https://polaris.example.com/api/catalog"
  },
  "table": "sales.orders",
  ...
}
```

## See Also

- [list](list.md) - List all ledgers and graph sources
- [iceberg](iceberg.md) - Map Iceberg tables as graph sources
- [log](log.md) - Show commit history
