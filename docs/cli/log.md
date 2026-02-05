# fluree log

Show commit log for a ledger.

## Usage

```bash
fluree log [LEDGER] [OPTIONS]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `[LEDGER]` | Ledger name (defaults to active ledger) |

## Options

| Option | Description |
|--------|-------------|
| `--oneline` | Show one-line summary per commit |
| `-n, --count <N>` | Maximum number of commits to show |

## Description

Displays the commit history for a ledger, similar to `git log`. Shows transaction numbers, timestamps, and commit details.

## Examples

```bash
# Show full commit log
fluree log

# Show last 5 commits
fluree log -n 5

# One-line format
fluree log --oneline

# Specific ledger
fluree log production --oneline -n 10
```

## Output

### Full Format (default)

```
commit fluree:file://storage/mydb/commit/abc123...
t: 3
Date: 2024-01-15T10:30:00Z

    Added new users

commit fluree:file://storage/mydb/commit/def456...
t: 2
Date: 2024-01-14T09:15:00Z

commit fluree:file://storage/mydb/commit/ghi789...
t: 1
Date: 2024-01-13T08:00:00Z

    Initial data load
```

### One-line Format

```
abc123 t=3 Added new users
def456 t=2
ghi789 t=1 Initial data load
```

## See Also

- [info](info.md) - Show ledger details
- [history](history.md) - Show entity change history
