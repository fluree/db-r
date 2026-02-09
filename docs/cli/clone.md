# fluree clone

Clone a ledger from a remote server, similar to `git clone`.

## Usage

```bash
fluree clone <REMOTE> <LEDGER>
```

## Arguments

| Argument | Description |
|----------|-------------|
| `<REMOTE>` | Remote name (configured via `fluree remote add`) |
| `<LEDGER>` | Ledger name on the remote server |

## Description

Downloads all commits from a remote ledger and creates a local copy:

1. Verifies the remote ledger exists and has commits
2. Creates a local ledger with the same name as on the remote
3. Fetches commit pages in bulk (newest → oldest, 500 per page)
4. Stores all commit and transaction blobs to local CAS
5. Sets the local head to match the remote head
6. Configures the remote as upstream for future `pull`/`push`

This is a **replication** operation. It requires a Bearer token with **root / storage-proxy** permissions (`fluree.storage.*`). If you only have permissioned/query access to a ledger, you should use `fluree track` (or `--remote`) and run queries/transactions against the remote instead.

**Idempotent CAS writes:** If interrupted mid-clone, CAS blob writes are idempotent. Re-running the clone command will re-fetch all pages (duplicate writes are harmless). The local head is only set after all pages are downloaded.

## Examples

```bash
# Clone a ledger from a configured remote
fluree clone origin mydb

# Full workflow: add remote, then clone
fluree remote add production https://api.example.com --token @~/.fluree/token
fluree clone production customers
```

## Output

Successful clone:
```
Cloning 'mydb:main' from 'origin'...
  fetched 500 commits...
  fetched 1000 commits...
  fetched 1042 commits...
✓ Cloned 'mydb:main' (1042 commits, head t=1042)
  upstream set to 'origin'
```

Remote ledger has no commits:
```
Remote ledger 'mydb:main' has no commits (t=0), nothing to clone.
```

## Errors

| Error | Description |
|-------|-------------|
| Remote not configured | Run `fluree remote add <name> <url>` first |
| Ledger not found on remote | Verify the ledger name matches the remote server |
| Auth failure | Token missing or lacks `fluree.storage.*` permissions |
| Local ledger already exists | Drop the existing ledger first |

## Limitations

- **Same ledger name locally:** The local ledger must use the same name as the remote. CAS addresses embed the ledger path, so renaming would break commit chain traversal. `--alias` is reserved for a future address-rewriting feature.
- **Same storage backend required:** Client and server must use the same storage method (both `file`, both `s3`, etc.). Cross-backend clone (e.g., server on S3, client on file) is not yet supported.
- **Post-clone indexing:** After cloning a large ledger, you may want to run `fluree reindex` to build a binary index. Without an index, queries must replay all novelty from commits, which can be slow for large ledgers.

## See Also

- [pull](pull.md) - Pull new commits from upstream
- [push](push.md) - Push local commits to upstream
- [remote](remote.md) - Configure remote servers
- [upstream](upstream.md) - Configure upstream tracking
