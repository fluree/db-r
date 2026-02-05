# fluree pull

Pull changes from upstream (fetch + fast-forward), similar to `git pull`.

## Usage

```bash
fluree pull [LEDGER]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to active ledger) |

## Description

Pulls changes from the configured upstream remote:

1. Fetches the latest refs from the upstream remote
2. Fast-forwards the local ledger to match the remote

The ledger must have an upstream configured (see `fluree upstream set`).

## Examples

```bash
# Pull changes for active ledger
fluree pull

# Pull changes for specific ledger
fluree pull mydb
```

## Output

Successful fast-forward:
```
Fetching from 'origin'...
Pulling 'mydb:main'...
✓ 'mydb:main' fast-forwarded: t=10 -> t=42
```

Already up to date:
```
Fetching from 'origin'...
Pulling 'mydb:main'...
✓ 'mydb:main' is already up to date
```

Diverged histories (local has commits not on remote):
```
Fetching from 'origin'...
Pulling 'mydb:main'...
✗ 'mydb:main' has diverged: local t=15, remote t=42
  hint: your local has commits the remote does not have
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
| Diverged histories | Local and remote have incompatible commit histories |
| No tracking data | Run `fluree fetch <remote>` first |

## See Also

- [upstream](upstream.md) - Configure upstream tracking
- [fetch](fetch.md) - Fetch refs without fast-forwarding
- [push](push.md) - Push local changes to upstream
