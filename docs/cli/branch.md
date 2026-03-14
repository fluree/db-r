# fluree branch

Create a new branch for a ledger.

## Usage

```bash
fluree branch <NAME> [OPTIONS]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `<NAME>` | Name for the new branch (e.g., "dev", "feature-x") |

## Options

| Option | Description |
|--------|-------------|
| `--ledger <LEDGER>` | Ledger name (defaults to active ledger) |
| `--from <BRANCH>` | Source branch to create from (defaults to "main") |
| `--remote <REMOTE>` | Execute against a remote server |

## Description

Creates a new branch for a ledger. The branch starts at the same transaction time as the source branch and is fully isolated -- subsequent transactions on either branch are invisible to the other.

Branches can be nested: you can create a branch from any existing branch, not just "main".

## Examples

```bash
# Create a branch from main (default)
fluree branch dev

# Create a branch for a specific ledger
fluree branch dev --ledger mydb

# Create a branch from another branch
fluree branch feature-x --from dev

# Create a branch on a remote server
fluree branch staging --ledger mydb --remote origin
```

## Output

```
Created branch 'dev' from 'main' at t=5
Ledger ID: mydb:dev
```

## See Also

- [branches](branches.md) - List all branches
- [create](create.md) - Create a new ledger
- [use](use.md) - Switch active ledger
