# fluree push

Push local ledger changes to upstream remote, similar to `git push`.

## Usage

```bash
fluree push [LEDGER]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to active ledger) |

## Description

Pushes local commits to the configured upstream remote. The ledger must have an upstream configured (see `fluree upstream set`).

The push uses compare-and-set (CAS) semantics - it will be rejected if the remote has commits that you don't have locally. In that case, you need to `pull` first.

This is a **replication** operation. It requires a Bearer token with **root / storage-proxy** permissions (`fluree.storage.*`). If you only have permissioned/query access to a ledger, you should use `fluree track` (or `--remote`) and run queries/transactions against the remote instead.

## Examples

```bash
# Push active ledger
fluree push

# Push specific ledger
fluree push mydb
```

## Output

Successful push:
```
Pushing 'mydb:main' to 'origin'...
✓ 'mydb:main' pushed successfully (t=42)
```

Push rejected (remote is ahead):
```
Pushing 'mydb:main' to 'origin'...
✗ 'mydb:main' rejected: local t=10, remote t=42
  hint: the remote has commits you don't have; pull first
```

No upstream configured:
```
error: no upstream configured for 'mydb:main'
  hint: fluree upstream set mydb:main <remote>
```

## Errors

| Error | Description |
|-------|-------------|
| No upstream configured | Run `fluree upstream set <ledger> <remote>` first |
| Push rejected | Remote has newer commits; run `fluree pull` first |

## Workflow

Typical sync workflow:

```bash
# Configure remote and upstream (one time)
fluree remote add origin https://api.example.com --token @~/.fluree/token
fluree upstream set mydb origin

# Daily workflow
fluree pull mydb        # Get latest changes
# ... make local changes ...
fluree push mydb        # Push your changes
```

## See Also

- [upstream](upstream.md) - Configure upstream tracking
- [pull](pull.md) - Pull changes from upstream
- [fetch](fetch.md) - Fetch refs without modifying local ledger
