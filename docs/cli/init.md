# fluree init

Initialize a new Fluree project directory.

## Usage

```bash
fluree init [OPTIONS]
```

## Options

| Option     | Description                                                                     |
| ---------- | ------------------------------------------------------------------------------- |
| `--global` | Create global config in the platform data directory instead of local `.fluree/` |

## Description

Creates a `.fluree/` directory in the current working directory (or a global directory with `--global`). This directory stores:

- `active` - The currently active ledger name
- `config.toml` - Configuration settings
- `prefixes.json` - IRI prefix mappings for compact IRIs
- `storage/` - Ledger data

Running `init` is idempotent - it won't overwrite existing configuration.

## Examples

```bash
# Initialize in current directory
fluree init

# Initialize global config
fluree init --global
```

## Global Directory

With `--global`, the directory is determined by:

1. `$FLUREE_HOME` environment variable (if set)
2. Platform data directory:
   - **Linux**: `$XDG_DATA_HOME/fluree` (default: `~/.local/share/fluree`)
   - **macOS**: `~/Library/Application Support/fluree`
   - **Windows**: `C:\Users\<user>\AppData\Local\fluree`

## See Also

- [create](create.md) - Create a new ledger after initialization
