# fluree drop

Drop (delete) a ledger.

## Usage

```bash
fluree drop <NAME> --force [--remote <REMOTE>]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `<NAME>` | Ledger name to drop |

## Options

| Option | Description |
|--------|-------------|
| `--force` | Required flag to confirm deletion |
| `--remote <REMOTE>` | Drop on a remote server (by remote name, e.g., "origin") |

## Description

Permanently deletes a ledger and all its data. The `--force` flag is required to prevent accidental deletion.

### Local drop

When no `--remote` flag is provided, the ledger is dropped from local storage using a hard drop (nameservice retraction + file deletion).

### Remote drop

With `--remote`, the command sends a hard drop request to the remote server's `POST /fluree/drop` endpoint. This requires admin-level authentication on the remote. Auth tokens (including OIDC) are handled automatically.

## Examples

```bash
# Delete a local ledger (requires --force)
fluree drop oldledger --force

# Delete a ledger on a remote server
fluree drop oldledger --force --remote origin
```

## Output

```
Dropped ledger 'oldledger'
```

Remote:
```
Dropped ledger 'oldledger' on remote 'origin' (42 files deleted)
```

## Errors

Without `--force`:
```
error: use --force to confirm deletion of ledger 'oldledger'
```

## See Also

- [create](create.md) - Create a new ledger
- [list](list.md) - List all ledgers
- [remote](remote.md) - Manage remote servers
