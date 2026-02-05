# fluree init

Initialize a new Fluree project directory.

## Usage

```bash
fluree init [OPTIONS]
```

## Options

| Option | Description |
|--------|-------------|
| `--global` | Create global config at `~/.fluree/` instead of local `.fluree/` |

## Description

Creates a `.fluree/` directory in the current working directory (or `~/.fluree/` with `--global`). This directory stores:

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

## See Also

- [create](create.md) - Create a new ledger after initialization
