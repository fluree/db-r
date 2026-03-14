# fluree branch

Manage branches for a ledger.

## Subcommands

### fluree branch create

Create a new branch.

**Usage:**

```bash
fluree branch create <NAME> [OPTIONS]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `<NAME>` | Name for the new branch (e.g., "dev", "feature-x") |

**Options:**

| Option | Description |
|--------|-------------|
| `--ledger <LEDGER>` | Ledger name (defaults to active ledger) |
| `--from <BRANCH>` | Source branch to create from (defaults to "main") |
| `--remote <REMOTE>` | Execute against a remote server |

**Description:**

Creates a new branch for a ledger. The branch starts at the same transaction time as the source branch and is fully isolated -- subsequent transactions on either branch are invisible to the other.

Branches can be nested: you can create a branch from any existing branch, not just "main".

**Examples:**

```bash
# Create a branch from main (default)
fluree branch create dev

# Create a branch for a specific ledger
fluree branch create dev --ledger mydb

# Create a branch from another branch
fluree branch create feature-x --from dev

# Create a branch on a remote server
fluree branch create staging --ledger mydb --remote origin
```

**Output:**

```
Created branch 'dev' from 'main' at t=5
Ledger ID: mydb:dev
```

### fluree branch list

List all branches for a ledger.

**Usage:**

```bash
fluree branch list [LEDGER] [OPTIONS]
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to active ledger) |

**Options:**

| Option | Description |
|--------|-------------|
| `--remote <REMOTE>` | List branches on a remote server |

**Examples:**

```bash
# List branches for the active ledger
fluree branch list

# List branches for a specific ledger
fluree branch list mydb

# List branches on a remote server
fluree branch list mydb --remote origin
```

**Output:**

```
 BRANCH     T   SOURCE
 main       5   -
 dev        7   main
 feature-x  8   dev
```

## See Also

- [create](create.md) - Create a new ledger
- [list](list.md) - List all ledgers
- [info](info.md) - Show ledger details
- [use](use.md) - Switch active ledger
