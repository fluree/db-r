# fluree clone

Clone a ledger from a remote server, similar to `git clone`.

## Usage

```bash
# Named-remote clone
fluree clone <REMOTE> <LEDGER>

# Origin-based clone (no pre-configured remote)
fluree clone --origin <URI> [--token <TOKEN>] <LEDGER>
```

## Arguments

| Argument | Description |
|----------|-------------|
| `<REMOTE>` | Remote name (configured via `fluree remote add`) |
| `<LEDGER>` | Ledger name on the remote server |
| `--origin <URI>` | Bootstrap URI for CID-based clone (replaces `<REMOTE>`) |
| `--token <TOKEN>` | Auth token for origin server (with `--origin` only) |

## Description

Downloads all commits from a remote ledger and creates a local copy:

1. Verifies the remote ledger exists and has commits
2. Creates a local ledger with the same name as on the remote
3. Attempts bulk download via the **pack protocol** (single streaming request)
4. Falls back to paginated JSON export if the server does not support pack
5. Stores all commit and transaction blobs to local CAS
6. Sets the local head to match the remote head
7. Configures the remote as upstream for future `pull`/`push` (named-remote only)

### Transport

The CLI uses the **pack protocol** (`fluree-pack-v1`) as the primary transport for clone and pull. Pack transfers all missing CAS objects (commits + txn blobs) in a single streaming HTTP request, avoiding per-object round-trips.

If the remote server does not support the pack endpoint (returns 404, 405, 406, or 501), the CLI automatically falls back to:
- **Named-remote mode**: paginated JSON export via `GET /commits/{ledger}` (500 commits per page)
- **Origin mode**: CID chain walk via `GET /storage/objects/{cid}` (one round-trip per commit)

This fallback is transparent -- no user action is required.

### Origin-based clone

The `--origin` flag enables CID-based clone from a server URL without pre-configuring a named remote:

```bash
fluree clone --origin http://localhost:8090 mydb
fluree clone --origin https://api.example.com --token @~/.fluree/token mydb
```

This mode:
1. Fetches the NsRecord from the origin to discover the head commit CID
2. Optionally upgrades to a multi-origin fetcher if a LedgerConfig is advertised
3. Downloads commits via pack (or CID chain walk as fallback)
4. Stores the LedgerConfig locally for future origin-based `pull`
5. Does **not** configure upstream tracking (use `fluree upstream set` manually)

This is a **replication** operation. It requires a Bearer token with **root / storage-proxy** permissions (`fluree.storage.*`). If you only have permissioned/query access to a ledger, you should use `fluree track` (or `--remote`) and run queries/transactions against the remote instead.

**Idempotent CAS writes:** If interrupted mid-clone, CAS blob writes are idempotent. Re-running the clone command will re-fetch all pages (duplicate writes are harmless). The local head is only set after all data is downloaded.

## Examples

```bash
# Clone a ledger from a configured remote
fluree clone origin mydb

# Full workflow: add remote, then clone
fluree remote add production https://api.example.com --token @~/.fluree/token
fluree clone production customers

# Origin-based clone (no remote setup needed)
fluree clone --origin http://localhost:8090 mydb

# Origin-based clone with auth
fluree clone --origin https://api.example.com --token @~/.fluree/token mydb
```

## Output

Successful clone (via pack):
```
Cloning 'mydb:main' from 'origin' (remote t=1042)...
  fetched 2084 object(s) via pack
✓ Cloned 'mydb:main' (1042 commits, head t=1042)
  → upstream set to 'origin/mydb:main'
```

Successful clone (fallback to paginated export):
```
Cloning 'mydb:main' from 'origin' (remote t=1042)...
  fetched 500 commits...
  fetched 1000 commits...
  fetched 1042 commits...
✓ Cloned 'mydb:main' (1042 commits, head t=1042)
  → upstream set to 'origin/mydb:main'
```

Origin-based clone:
```
Cloning 'mydb:main' from 'http://localhost:8090' (remote t=50)...
  fetched 100 object(s) via pack
✓ Cloned 'mydb:main' (50 commit(s), head t=50)
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

- **Post-clone indexing:** After cloning a large ledger, you may want to run `fluree reindex` to build a binary index. Without an index, queries must replay all novelty from commits, which can be slow for large ledgers.
- **Graph source indexes not replicated:** Graph source snapshots (BM25/vector/geo, etc.) are not replicated by `fluree clone` yet. After cloning, rebuild graph source indexes in the target environment as needed.

## See Also

- [pull](pull.md) - Pull new commits from upstream
- [push](push.md) - Push local commits to upstream
- [remote](remote.md) - Configure remote servers
- [upstream](upstream.md) - Configure upstream tracking
