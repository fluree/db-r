# Developer Memory Layer

The developer memory layer provides persistent, structured project knowledge storage backed by a dedicated Fluree ledger. It stores facts, decisions, constraints, preferences, and artifact references as RDF triples, making them queryable via SPARQL and retrievable by keyword-scored recall. IDE agents interact via MCP tools; developers interact via CLI.

Phase 1 (current) is fully local with no cloud dependency: template-based extraction and keyword-based recall. Phase 2 will add Fluree Cloud integration for LLM extraction and vector-based hybrid retrieval.

## Architecture

```text
┌──────────────┐     ┌───────────────────┐     ┌──────────────────┐
│  IDE Agent   │────▶│ fluree mcp serve  │────▶│                  │
│ (Claude Code,│ MCP │   --stdio         │     │  fluree-db-memory│
│  Cursor)     │stdio│ (in CLI binary)   │     │  (library crate) │
└──────────────┘     └───────────────────┘     │                  │
                                                │  MemoryStore     │
┌──────────────┐     ┌───────────────────┐     │  RecallEngine    │
│  Developer   │────▶│ fluree memory ... │────▶│  SecretDetector  │
│  (terminal)  │ CLI │ (CLI commands)    │     │  Formatters      │
└──────────────┘     └───────────────────┘     └────────┬─────────┘
                                                        │
                                                        ▼
                                               ┌──────────────────┐
                                               │  fluree-db-api   │
                                               │  (graph API)     │
                                               │                  │
                                               │  __memory ledger │
                                               │  .fluree-memory/ │
                                               └──────────────────┘
```

**Key design choices:**

- `fluree-db-memory` is a standalone library crate, reusable by CLI and server.
- CLI commands operate directly on the memory ledger (no server required).
- MCP via `fluree mcp serve --stdio` in the CLI binary (for IDE agents).
- The `__memory` ledger uses `FileStorage` for persistence across sessions.
- Memories are standard RDF triples, queryable via SPARQL like any Fluree ledger.

## Crate layout

| Module | Purpose |
|--------|---------|
| `types.rs` | `MemoryKind`, `Scope`, `Sensitivity`, `Severity`, `Memory`, `MemoryInput`, `MemoryUpdate`, `MemoryFilter`, `ScoredMemory`, `RecallResult`, `MemoryStatus` |
| `store.rs` | `MemoryStore` — CRUD operations backed by the `__memory` Fluree ledger |
| `schema.rs` | `memory_schema_jsonld()` — JSON-LD schema for the `mem:` namespace classes and properties |
| `recall.rs` | `RecallEngine` — keyword-based scoring and ranking |
| `secrets.rs` | `SecretDetector` — regex-based secret detection and redaction |
| `format.rs` | Output formatters: text, JSON, XML context (for LLM injection), explain, status |
| `mcp.rs` | `MemoryToolService` — MCP tool service (behind `mcp` feature flag) |
| `branch.rs` | `detect_git_branch()` via `git rev-parse` |
| `id.rs` | `generate_memory_id(kind)` — ULID-based IDs (`mem:<kind>-<ulid>`) |
| `vocab.rs` | Constants for `mem:` namespace IRIs |
| `file_sync.rs` | TTL ↔ ledger sync via SHA-256 build-hash watermark |
| `turtle_io.rs` | Read/write memories as Turtle (`.ttl`) files |

## Data model

Memories are stored as RDF triples in a dedicated `__memory` ledger under the `mem:` namespace (`https://ns.flur.ee/memory#`).

### Classes

| Class | Description |
|-------|-------------|
| `mem:Fact` | Things that are true about the project (e.g., "tests use nextest") |
| `mem:Decision` | Choices made and their rationale (e.g., "chose keyword matching for Phase 1") |
| `mem:Constraint` | Rules that must be followed (e.g., "never suppress dead code with underscore") |
| `mem:Preference` | How things should be done (e.g., "use thiserror, not anyhow") |
| `mem:Artifact` | Important files or resources (e.g., "fluree-db-core/src/error.rs defines the error pattern") |

### Shared properties

| Property | Range | Cardinality | Description |
|----------|-------|-------------|-------------|
| `mem:content` | `xsd:string` | 1 | The content text |
| `mem:tag` | `xsd:string` | 0..n | Tags for categorization and recall |
| `mem:scope` | `rdfs:Resource` | 1 | IRI-based named graph: `mem:repo` or `mem:user` |
| `mem:sensitivity` | `xsd:string` | 1 | `"public"`, `"internal"`, `"client"`, or `"secret"` |
| `mem:severity` | `xsd:string` | 0..1 | For constraints: `"must"`, `"should"`, `"prefer"` |
| `mem:artifactRef` | `xsd:string` | 0..n | File paths or artifact references |
| `mem:branch` | `xsd:string` | 0..1 | Git branch at creation time |
| `mem:supersedes` | `rdfs:Resource` | 0..1 | ID of the memory this one replaces |
| `mem:validFrom` | `xsd:dateTime` | 0..1 | Bi-temporal: when this memory becomes valid |
| `mem:validTo` | `xsd:dateTime` | 0..1 | Bi-temporal: when this memory expires |
| `mem:createdAt` | `xsd:dateTime` | 1 | Transaction timestamp |

### Type-specific properties

| Property | Range | Applies to | Description |
|----------|-------|------------|-------------|
| `mem:rationale` | `xsd:string` | `mem:Decision` | Why this decision was made |
| `mem:alternatives` | `xsd:string` | `mem:Decision` | Alternatives that were considered |
| `mem:factKind` | `xsd:string` | `mem:Fact` | Sub-category: `command`, `architecture`, `dependency`, `configuration`, `api` |
| `mem:prefScope` | `xsd:string` | `mem:Preference` | Convention scope: `user`, `team`, `repo` |
| `mem:artifactKind` | `xsd:string` | `mem:Artifact` | Sub-category: `file`, `symbol`, `crate`, `module`, `config`, `endpoint` |

### Memory IDs

IDs follow the pattern `mem:<kind>-<ulid>`, for example `mem:fact-01JDXYZ5A2B3C4D5E6F7G8H9J0`. The ULID provides both uniqueness and chronological sorting.

### Supersession model

Updates do not mutate existing triples. Instead, `memory update` creates a new memory with a `mem:supersedes` link to the old one. The old memory remains in the graph for audit/explain but is excluded from recall queries via:

```sparql
FILTER NOT EXISTS { ?newer mem:supersedes ?id }
```

The `explain` command walks the supersession chain to show how a memory evolved over time.

## CLI commands

All commands require a Fluree project directory (`.fluree/`). Run `fluree init` first if needed. Memory data is stored in `.fluree-memory/` at the project root (separate from `.fluree/` so the latter can be fully gitignored).

| Command | Description |
|---------|-------------|
| `fluree memory init` | Create the `__memory` ledger and transact the schema |
| `fluree memory add --kind fact --text "..." --tags t1,t2` | Store a new memory |
| `fluree memory recall "query string" [-n 3] [--offset 0]` | Search and rank memories by relevance (default: top 3; use `--offset` to paginate) |
| `fluree memory update <id> --text "new content"` | Supersede a memory with updated content |
| `fluree memory forget <id>` | Delete a memory (retracts all triples) |
| `fluree memory explain <id>` | Show the supersession chain |
| `fluree memory status` | Show memory store summary (counts by kind) |
| `fluree memory export` | Export all current memories as JSON |
| `fluree memory import <file>` | Import memories from a JSON file |
| `fluree memory mcp-install --ide cursor` | Install MCP config for an IDE (or auto-detected during init) |

The `add` command reads from `--text` or stdin. Output format is controlled via `--format` (`text`, `json`, or `context` for recall).

## MCP tools

The `fluree mcp serve --transport stdio` command starts an MCP server that exposes 6 tools to IDE agents:

| Tool | Description |
|------|-------------|
| `memory_add` | Store a new memory (auto-initializes if needed) |
| `memory_recall` | Search and retrieve relevant memories as XML context. Returns up to `limit` results (default: 3) starting at `offset` (default: 0). Response includes a `<pagination>` element indicating remaining results. |
| `memory_update` | Supersede an existing memory |
| `memory_forget` | Delete a memory |
| `memory_status` | Show memory store summary |
| `kg_query` | Execute raw SPARQL against the memory graph |

The MCP server auto-initializes the memory store on first tool call, so agents do not need to run `fluree memory init` separately.

### IDE setup

`fluree memory init` auto-detects installed AI coding tools and offers to install MCP config for each. You can also run `fluree memory mcp-install --ide <tool>` directly:

| Tool | Config written | Rules / extras |
|------|----------------|----------------|
| `claude-code` | `.mcp.json` | Snippet appended to `CLAUDE.md`; also runs `claude mcp add` for VS Code extension |
| `vscode` | `.vscode/mcp.json` (key: `servers`) | `.vscode/fluree_rules.md` |
| `cursor` | `.cursor/mcp.json` | `.cursor/rules/fluree_rules.md` |
| `windsurf` | `~/.codeium/windsurf/mcp_config.json` (global) | — |
| `zed` | `.zed/settings.json` (key: `context_servers`) | — |

All configs point to `fluree mcp serve --transport stdio`. Use `--yes` on init to auto-confirm all installations (non-interactive). Use `--no-mcp` to skip tool detection entirely.

## Recall scoring

Phase 1 uses BM25 fulltext search (via Fluree's native `fulltext()` function) combined with metadata bonuses. The BM25 step fetches the top `offset + limit` candidates from the ledger; the `RecallEngine` re-ranks them by adding metadata bonuses and then applies the offset/limit slice.

By default, `limit=3` and `offset=0` — returning the top 3 results. Use `--offset` (CLI) or the `offset` parameter (MCP) to paginate. The response always includes a `<pagination>` element; when results are cut off it shows the next offset to use.

When BM25 returns no hits (e.g., very short or generic queries), the engine falls back to metadata-only scoring over all current memories.

**BM25 (primary):** Fluree's native `fulltext()` function scores memories by content relevance and returns up to `offset + limit` candidates.

**Metadata bonuses (additive re-ranking):**

| Signal | Points | Description |
|--------|--------|-------------|
| Tag match | +10 per tag | Query word appears in a tag |
| Artifact ref match | +8 per ref | Query word appears in a file/artifact reference |
| Kind match | +6 | Query contains a kind keyword (e.g., "constraint", "rule", "must") |
| Branch match | +3 | Memory's git branch matches current branch |
| Recency (< 7 days) | +2 | Memory created within the last week |
| Recency (< 30 days) | +1 | Memory created within the last month |

Query words shorter than 3 characters are ignored. All matching is case-insensitive.

## Secret detection

The `SecretDetector` scans memory content at ingestion time using compiled regex patterns. If secrets are detected, the content is automatically redacted (replaced with `[REDACTED]`) and a warning is emitted.

| Pattern | Example match |
|---------|---------------|
| AWS Access Key | `AKIAIOSFODNN7EXAMPLE` |
| Generic API Key | `api_key=abc123...` |
| OpenAI Key | `sk-proj1234567890...` |
| Anthropic Key | `sk-ant-abc123...` |
| Fluree API Key | `flk_a1b2c3d4e5...` |
| GitHub Token | `ghp_abc123...` (36+ chars) |
| Password | `password=mysecret...` |
| Connection String | `postgres://user:pass@host` |
| Private Key (PEM) | `-----BEGIN RSA PRIVATE KEY-----` |
| Bearer Token | `bearer eyJabc...` (20+ chars) |
| JWT Token | `eyJ...eyJ...signature` |

Secret detection is applied in both CLI commands and MCP tools.

## Storage

Memory data lives in `.fluree-memory/` at the project root, separate from `.fluree/` (which holds derived ledger data and can be fully gitignored).

```text
.fluree-memory/
├── repo.ttl          # Repo-scoped memories (git-tracked, shared with team)
├── .gitignore        # Ignores .local/
└── .local/
    ├── user.ttl      # User-scoped memories (private, not committed)
    └── mcp.log       # MCP server log
```

The TTL files are the **source of truth**. On startup, a SHA-256 build-hash watermark detects changes and rebuilds the `__memory` Fluree ledger (stored in `.fluree/`) as a derived BM25 query cache. This means `.fluree/` can be deleted and regenerated at any time.

The memory store is a standard Fluree ledger, so all Fluree capabilities apply: SPARQL queries, time-travel, commit logs, export/import. The `kg_query` MCP tool exposes raw SPARQL access to the memory graph.

## Phase 2: Fluree Cloud + AI (planned)

Phase 2 adds cloud-powered intelligence to the memory layer. These features are not yet implemented but the architecture is designed to support them.

### Authentication

`fluree auth login` will use a PKCE flow to Cognito, storing an API key at `~/.fluree/credentials.json`. The API key exchanges for short-lived access tokens (1h cache) via `POST /v1/auth/token`.

### LLM extraction

A `--extract` flag on `memory add` will send unstructured text to Fluree Cloud's `POST /v1/responses` endpoint for structured memory extraction. The LLM identifies facts, decisions, and constraints from free-form text (meeting notes, design discussions, code review comments) and returns structured `MemoryInput` objects.

### Vector-based hybrid retrieval

Fluree Cloud's `POST /v1/embeddings` endpoint will generate vector embeddings (text-embedding-3-small, 256 dimensions) for each memory's content. Recall will combine vector similarity with the existing keyword scoring for hybrid retrieval, significantly improving recall quality for semantic queries.

### Global user memory

A global memory store at `~/.local/share/fluree/` (or platform equivalent) will hold cross-project memories with `scope: global`. These persist across projects and are merged into recall results with a scope-based weight adjustment.

### Server MCP integration

Memory tools will also be added to the existing HTTP MCP in `fluree-db-server`, so users running the Fluree server get memory capabilities through the same MCP endpoint alongside `sparql_query` and `get_data_model`.
