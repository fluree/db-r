# fluree memory

Developer memory — store and recall facts, decisions, constraints, preferences, and artifact references.

## Usage

```bash
fluree memory <COMMAND>
```

## Subcommands

| Command | Description |
|---------|-------------|
| `init` | Initialize the memory store (creates `__memory` ledger) |
| `add` | Store a new memory |
| `recall` | Search and rank relevant memories |
| `update <ID>` | Update (supersede) an existing memory |
| `forget <ID>` | Delete a memory |
| `explain <ID>` | Show the supersession chain for a memory |
| `status` | Show memory store status |
| `export` | Export all current memories as JSON |
| `import <FILE>` | Import memories from a JSON file |
| `mcp-install` | Install MCP configuration for an IDE |

## Description

The memory system stores project knowledge as RDF triples in a dedicated `__memory` Fluree ledger. Memories persist across sessions and are searchable by keyword-scored recall.

Run `fluree memory init` before using other memory commands. The MCP server auto-initializes on first tool call.

See [Developer memory layer](../design/memory-layer.md) for the full design.

## fluree memory init

Create the `__memory` ledger and transact the memory schema. Idempotent — safe to run multiple times.

```bash
fluree memory init
```

Output:
```
Memory store initialized.
```

## fluree memory add

Store a new memory.

```bash
fluree memory add [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `--kind <KIND>` | Memory kind: `fact`, `decision`, `constraint`, `preference`, `artifact` (default: `fact`) |
| `--text <TEXT>` | Content text (or provide via stdin) |
| `--tags <T1,T2>` | Comma-separated tags for categorization |
| `--refs <R1,R2>` | Comma-separated file/artifact references |
| `--severity <SEV>` | For constraints: `must`, `should`, `prefer` |
| `--scope <SCOPE>` | Scope: `repo` (default) or `user` |
| `--sensitivity <SENS>` | Sensitivity: `public` (default), `internal`, `client`, `secret` |
| `--rationale <TEXT>` | Why this decision was made (for `--kind decision`) |
| `--alternatives <TEXT>` | Alternatives considered (for `--kind decision`) |
| `--fact-kind <FK>` | Fact sub-type: `command`, `architecture`, `dependency`, `configuration`, `api` |
| `--pref-scope <PS>` | Preference convention scope: `user`, `team`, `repo` |
| `--artifact-kind <AK>` | Artifact sub-type: `file`, `symbol`, `crate`, `module`, `config`, `endpoint` |
| `--format <FMT>` | Output format: `text` (default) or `json` |

### Examples

```bash
# Store a fact
fluree memory add --kind fact --text "Tests use cargo nextest" --tags testing,cargo

# Store a constraint with severity
fluree memory add --kind constraint --text "Never suppress dead code with underscore prefix" \
  --tags code-style --severity must

# Store from stdin
echo "The index format uses postcard encoding" | fluree memory add --kind fact --tags indexer

# Store a decision with rationale and alternatives
fluree memory add --kind decision --text "Use postcard for compact index encoding" \
  --rationale "no_std compatible, smaller output than bincode" \
  --alternatives "bincode, CBOR, MessagePack" --refs fluree-db-indexer/

# Store a user-scoped preference
fluree memory add --kind preference --text "Always run clippy with --all-features" \
  --scope user --pref-scope user --tags code-style

# Store an artifact reference
fluree memory add --kind artifact --text "Error pattern defined here" \
  --refs fluree-db-core/src/error.rs --tags errors --artifact-kind file
```

Output (text):
```
Stored memory: mem:fact-01JDXYZ5A2B3C4D5E6F7G8H9J0
```

### Secret detection

If the content contains secrets (API keys, passwords, tokens, connection strings), they are automatically redacted and a warning is printed:

```
  warning: secrets detected in content — storing redacted version.
  Original content contained sensitive data that was replaced with [REDACTED].
Stored memory: mem:fact-01JDXYZ...
```

## fluree memory recall

Search and retrieve relevant memories ranked by score.

```bash
fluree memory recall <QUERY> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<QUERY>` | Natural language search query |

### Options

| Option | Description |
|--------|-------------|
| `-n, --limit <N>` | Maximum results per page (default: 3) |
| `--offset <N>` | Skip the first N results — use for pagination (default: 0) |
| `--kind <KIND>` | Filter to a specific memory kind |
| `--tags <T1,T2>` | Filter to memories with these tags |
| `--scope <SCOPE>` | Filter by scope: `repo` or `user` |
| `--format <FMT>` | Output: `text` (default), `json`, or `context` (XML for LLM) |

### Examples

```bash
# Basic recall (returns top 3)
fluree memory recall "how to run tests"

# Get the next page
fluree memory recall "how to run tests" --offset 3

# Return up to 10 results
fluree memory recall "error handling" -n 10

# Filter by kind and tags
fluree memory recall "error handling" --kind constraint --tags errors

# Output as XML context (for LLM injection)
fluree memory recall "testing patterns" --format context
```

Output (text):
```
Recall: "how to run tests" (2 matches)

1. [score: 13.0] mem:fact-01JDXYZ...
   Tests use cargo nextest
   Tags: testing, cargo

2. [score: 8.0] mem:fact-01JDABC...
   Integration tests use assert_cmd + predicates
   Tags: testing

  (showing results 1–3; use --offset 3 for more)
```

Output (context):
```xml
<memory-context>
  <memory id="mem:fact-01JDXYZ..." kind="fact" score="13.0">
    <content>Tests use cargo nextest</content>
    <tags>testing, cargo</tags>
  </memory>
  <pagination shown="1" offset="0" total_in_store="13" />
</memory-context>
```

When results are cut off, the pagination element includes a hint:

```xml
  <pagination shown="3" offset="0" limit="3" total_in_store="13">Results 1–3. Use offset=3 to retrieve more.</pagination>
```

## fluree memory update

Supersede an existing memory with new content or metadata. Creates a new memory linked to the old one via `mem:supersedes`.

```bash
fluree memory update <ID> [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `--text <TEXT>` | New content text |
| `--tags <T1,T2>` | New tags (replaces all existing) |
| `--refs <R1,R2>` | New artifact refs (replaces all existing) |
| `--format <FMT>` | Output: `text` or `json` |

### Example

```bash
fluree memory update mem:fact-01JDXYZ... --text "Tests use cargo nextest with --no-fail-fast"
```

Output:
```
Updated: mem:fact-01JDXYZ... → mem:fact-01JDNEW...
```

## fluree memory forget

Delete a memory by retracting all its triples.

```bash
fluree memory forget <ID>
```

Output:
```
Forgotten: mem:fact-01JDXYZ...
```

## fluree memory explain

Show the supersession chain for a memory (newest first).

```bash
fluree memory explain <ID>
```

Output:
```
Supersession chain (newest first):

1. mem:fact-01JDNEW... (current)
   Kind: fact
   Content: Tests use cargo nextest with --no-fail-fast
   Created: 2026-02-22T15:30:00Z
   Supersedes: mem:fact-01JDXYZ...

2. mem:fact-01JDXYZ...
   Kind: fact
   Content: Tests use cargo nextest
   Created: 2026-02-22T14:00:00Z
```

## fluree memory status

Show a summary of the memory store.

```bash
fluree memory status
```

Output:
```
Memory Store Status
  Total memories: 12
  Total tags:     25
  By kind:
    fact: 7
    decision: 2
    constraint: 3
```

## fluree memory export / import

Export all current (non-superseded) memories as JSON, or import from a file.

```bash
fluree memory export > memories.json
fluree memory import memories.json
```

## fluree memory mcp-install

Install MCP configuration for an IDE so agents can use memory tools.

```bash
fluree memory mcp-install [--ide <IDE>]
```

### Options

| Option | Description |
|--------|-------------|
| `--ide <IDE>` | Target IDE: `claude-code`, `claude-vscode`, `cursor` (auto-detected if omitted) |

Auto-detection checks for `.cursor/` then `.vscode/` directories; defaults to `claude-code`.

### Example

```bash
fluree memory mcp-install --ide cursor
```

Output:
```
Installed MCP config: .cursor/mcp.json
Installed agent rules: .cursor/rules/fluree_rules.md
```

## See Also

- [mcp](mcp.md) - MCP server for IDE agent integration
- [Developer memory layer](../design/memory-layer.md) - Design doc with architecture, data model, and scoring algorithm
