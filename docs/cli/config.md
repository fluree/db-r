# fluree config

Manage configuration settings.

## Usage

```bash
fluree config <COMMAND>
```

## Subcommands

| Command | Description |
|---------|-------------|
| `get <KEY>` | Get a configuration value |
| `set <KEY> <VALUE>` | Set a configuration value |
| `list` | List all configuration values |

## Description

Manages configuration stored in `.fluree/config.toml`. Configuration uses dotted keys for nested values (e.g., `storage.path`).

## Examples

### Get a value

```bash
fluree config get storage.path
```

Output:
```
/custom/storage/path
```

### Set a value

```bash
fluree config set storage.path /custom/storage/path
```

Output:
```
Set 'storage.path' = "/custom/storage/path"
```

### List all values

```bash
fluree config list
```

Output:
```
storage.path = "/custom/storage/path"
storage.encryption = "aes256"
```

If no configuration is set:
```
(no configuration set)
```

## Configuration File

Configuration is stored in `.fluree/config.toml`:

```toml
[storage]
path = "/custom/storage/path"
encryption = "aes256"
```

## Errors

Getting a key that doesn't exist:
```
error: configuration key 'nonexistent' is not set
```

## See Also

- [init](init.md) - Initialize project directory
- [prefix](prefix.md) - Manage IRI prefixes
