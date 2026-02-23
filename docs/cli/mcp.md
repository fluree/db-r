# fluree mcp

Model Context Protocol (MCP) server for IDE agent integration.

## Usage

```bash
fluree mcp <COMMAND>
```

## Subcommands

| Command | Description |
|---------|-------------|
| `serve` | Start the MCP server |

## fluree mcp serve

Start an MCP server that exposes developer memory tools to IDE agents.

```bash
fluree mcp serve [--transport <TRANSPORT>]
```

### Options

| Option | Description |
|--------|-------------|
| `--transport <TRANSPORT>` | Transport protocol: `stdio` (default) |

The `stdio` transport reads JSON-RPC requests from stdin and writes responses to stdout. This is the standard transport for IDE integration — the IDE spawns the process and communicates over pipes.

### Available tools

The MCP server exposes 6 tools:

| Tool | Description |
|------|-------------|
| `memory_add` | Store a new memory (fact, decision, constraint, preference, artifact) |
| `memory_recall` | Search and retrieve relevant memories as XML context |
| `memory_update` | Update (supersede) an existing memory |
| `memory_forget` | Delete a memory |
| `memory_status` | Show memory store summary |
| `kg_query` | Execute raw SPARQL against the memory graph |

The server auto-initializes the memory store on first tool call. No separate `fluree memory init` is needed.

### IDE configuration

The easiest way to configure your IDE is with `fluree memory mcp-install`:

```bash
fluree memory mcp-install --ide cursor
```

Or manually add to your IDE's MCP config:

```json
{
  "mcpServers": {
    "fluree-memory": {
      "command": "/path/to/fluree",
      "args": ["mcp", "serve", "--transport", "stdio"]
    }
  }
}
```

### Testing with JSON-RPC

To test the server directly, pipe JSON-RPC to stdin:

```bash
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' | fluree mcp serve
```

### Tracing

CLI tracing is disabled when running `fluree mcp serve` to avoid any log output on stderr that could interfere with the JSON-RPC protocol.

## See Also

- [memory](memory.md) - CLI commands for memory management
- [Developer memory layer](../design/memory-layer.md) - Design doc with architecture and data model
