# fluree list

List all ledgers.

## Usage

```bash
fluree list
```

## Description

Lists all ledgers in the current Fluree directory. The active ledger is marked with an asterisk (`*`).

## Examples

```bash
fluree list
```

## Output

```
* mydb
  production
  staging
```

If no ledgers exist:
```
(no ledgers found)
```

## See Also

- [create](create.md) - Create a new ledger
- [info](info.md) - Show detailed ledger information
- [use](use.md) - Switch active ledger
