# fluree pull

Pull commits from upstream and apply them to the local ledger, similar to `git pull`.

## Usage

```bash
fluree pull [LEDGER]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to active ledger) |

## Description

Downloads commit blobs from the configured upstream remote and applies them to the local ledger:

1. Queries the remote for its current head (`t` and commit ContentId)
2. Compares with the local head
3. Fetches commit pages (newest → oldest) until local history is reached
4. Filters and reorders commits (oldest → newest)
5. Imports incrementally: validates chain, checks ancestry, writes blobs, advances head, updates novelty

This is a **replication** operation. It requires a Bearer token with **root / storage-proxy** permissions (`fluree.storage.*`). If you only have permissioned/query access to a ledger, you should use `fluree track` (or `--remote`) and run queries/transactions against the remote instead.

The ledger must have an upstream configured (see `fluree upstream set`).

**Restart safety:** If interrupted, the local head reflects the last successful import. The next pull resumes from the local head automatically.

## Examples

```bash
# Pull changes for active ledger
fluree pull

# Pull changes for specific ledger
fluree pull mydb
```

## Output

Successful pull:
```
Pulling 'mydb:main' from 'origin' (local t=10, remote t=42)...
✓ 'mydb:main' pulled 32 commit(s) (new head t=42)
```

Already up to date:
```
✓ 'mydb:main' is already up to date
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
| Ancestry mismatch | Remote chain does not descend from local head (histories diverged) |
| Import validation failure | Commit chain or retraction invariant violation |

## See Also

- [clone](clone.md) - Clone a ledger from a remote server
- [upstream](upstream.md) - Configure upstream tracking
- [fetch](fetch.md) - Fetch refs without modifying local ledger
- [push](push.md) - Push local changes to upstream
