# Implementation Plan: `fluree server` CLI Subcommand

## Goal

Add a `fluree server` command group to the CLI so users can manage a Fluree HTTP
server directly from a project directory. The server inherits the same `.fluree/`
context (config, storage, etc.) that the CLI already uses — one directory, two
modes of interaction.

## Command Hierarchy

```
fluree server run      # Foreground (Ctrl-C to stop). Default entrypoint.
fluree server start    # Background daemon. Writes PID + log files.
fluree server stop     # Stop a backgrounded server.
fluree server status   # Running? PID, addr, uptime, storage path, ledger count.
fluree server restart  # stop + start, preserving original flags.
fluree server logs     # Tail/view logs from a backgrounded server.
```

Six subcommands — clean lifecycle + observability.

---

## Design Decisions

### 1. `run` vs `start` (foreground/background split)

| Command | Behavior |
|---------|----------|
| `run`   | Foreground. Inherits terminal. Ctrl-C = graceful shutdown. Logs to stderr. |
| `start` | Background. Forks via `_child` internal subcommand. Writes PID/log/metadata to `data_dir`. |

No `--detach` flag on `start` — background is the only mode. Users who want
foreground use `run`. This eliminates the "did I mean foreground?" ambiguity
and matches the `pg_ctl` mental model (`pg_ctl start` = daemon).

### 2. Embedding via feature flag + `_child` pattern

The CLI binary embeds `fluree-db-server` behind an optional Cargo feature:

```toml
# fluree-db-cli/Cargo.toml
[features]
server = ["dep:fluree-db-server"]

[dependencies]
fluree-db-server = { path = "../fluree-db-server", optional = true }
```

**Background spawning** uses a hidden internal subcommand rather than
double-forking or exec'ing a separate binary:

```
fluree server start [user flags]
  └─ spawns: current_exe() server _child [resolved flags]
       └─ runs FlureeServer::new(config).run() in-process
       └─ stdout/stderr redirected to log file
```

Benefits:
- Single binary distribution — no "server binary not found" failure mode.
- `current_exe()` guarantees version match between CLI and server.
- `_child` is hidden from `--help` and docs.

If the `server` feature is not compiled in, `fluree server *` prints a
clear error: "Server support not compiled. Rebuild with `--features server`."

### 3. Runtime state location

All runtime artifacts live under `dirs.data_dir()` (same resolution the CLI
already uses — local `.fluree/` or global platform dirs):

```
.fluree/
├── config.toml           # shared config (existing)
├── storage/              # ledger data (existing)
├── server.pid            # PID of backgrounded server
├── server.log            # stdout+stderr from backgrounded server
└── server.meta.json      # metadata for restart/status
```

**`server.meta.json`** stores enough to support `restart` and `status`:

```json
{
  "pid": 12345,
  "listen_addr": "0.0.0.0:8090",
  "storage_path": "/path/to/.fluree/storage",
  "config_path": "/path/to/.fluree/config.toml",
  "started_at": "2026-02-16T10:30:00Z",
  "args": ["--log-level", "debug"]
}
```

### 4. `status` — PID + HTTP health check

Don't trust PID alone (stale PID files are common). Status does:

1. Read `server.pid` — if missing, "not running."
2. Check process alive (`kill -0 pid`).
3. HTTP GET to `listen_addr` from `server.meta.json` hitting `/health`.
4. On success: print pid, addr, uptime, version, storage path, cached ledgers.
5. On failure: "PID exists but server not responding" (stale PID).

### 5. Flag passthrough — small common set + `--` escape hatch

`run` and `start` accept a curated set of common overrides:

| Flag | Description |
|------|-------------|
| `--listen-addr` | Override listen address |
| `--storage-path` | Override storage path |
| `--log-level` | Override log level |
| `--profile` | Select config profile |

Everything else (peer config, auth modes, OIDC, MCP, etc.) comes from
`[server]` in `config.toml`. Power users can pass arbitrary server flags via:

```bash
fluree server run -- --peer-subscribe-all --mcp-enabled
```

The `--` separator forwards remaining args directly to the server config
parser. This keeps the CLI surface stable while providing full access.

### 6. `start --dry-run`

Prints the fully resolved config (file + env + flag overrides merged) without
actually starting the server:

```bash
fluree server start --dry-run
# → listen_addr: 0.0.0.0:8090
# → storage_path: /Users/me/project/.fluree/storage
# → log_level: info
# → indexing: disabled
# → pid_file: /Users/me/project/.fluree/server.pid
# → log_file: /Users/me/project/.fluree/server.log
```

Answers "why is it using port X?" without growing the command hierarchy.

### 7. Log management

- **`run`**: logs go to stderr (standard foreground behavior).
- **`start`**: redirects child stdout+stderr to `server.log`.
- **`logs`** subcommand:
  - `fluree server logs` — prints last 50 lines (default).
  - `fluree server logs --follow` / `-f` — tails the log file.
  - `fluree server logs --lines N` / `-n N` — last N lines.

Log rotation is out of scope for v1 — users can use `logrotate` or similar.
The log file path is recorded in `server.meta.json`.

### 8. `stop` and `restart`

**`stop`**:
1. Read `server.pid`.
2. Send `SIGTERM` (graceful shutdown).
3. Wait up to 10s for process exit.
4. If still alive after timeout, warn (do NOT `SIGKILL` without `--force`).
5. Clean up `server.pid` and `server.meta.json`.

**`restart`**:
1. Read `server.meta.json` to recover original `args`.
2. `stop`.
3. `start` with the recovered args (+ any new flag overrides).

---

## Implementation Steps

### Phase 1: Feature flag + CLI structure

#### Step 1.1: Add `server` feature to `fluree-db-cli/Cargo.toml`
- Add `fluree-db-server` as optional dependency.
- Propagate `native` and `credential` features.
- Feature-gate the entire `commands/server.rs` module.

#### Step 1.2: Define `Commands::Server` in `cli.rs`
```rust
/// Manage the Fluree HTTP server
Server {
    #[command(subcommand)]
    action: ServerAction,
},
```

With `ServerAction` enum:
```rust
pub enum ServerAction {
    /// Run the server in the foreground (Ctrl-C to stop)
    Run { /* common flags + trailing_args */ },
    /// Start the server as a background process
    Start { /* common flags + --dry-run */ },
    /// Stop a backgrounded server
    Stop { /* --force */ },
    /// Show server status
    Status,
    /// Restart a backgrounded server
    Restart { /* common flags */ },
    /// View server logs
    Logs { /* --follow, --lines */ },
    /// (hidden) Internal child process entry point
    #[command(hide = true)]
    Child { /* full server args */ },
}
```

#### Step 1.3: Create `commands/server.rs` module
- Feature-gated: `#[cfg(feature = "server")]`.
- Stub out handler functions for each subcommand.

### Phase 2: `run` (foreground)

#### Step 2.1: Config resolution
- Resolve `FlureeDir` (existing `config::require_fluree_dir`).
- Build `ServerConfig` from config file merge (reuse `load_and_merge_config`
  from `fluree-db-server/src/config_file.rs`).
- Apply flag overrides (`--listen-addr`, `--storage-path`, `--log-level`).
- Apply trailing `--` args via clap raw parsing.

#### Step 2.2: Server lifecycle
- Initialize telemetry (`init_logging`).
- `FlureeServer::new(config).await`.
- `server.run().await` — blocks until Ctrl-C / SIGTERM.
- `shutdown_tracer().await`.

### Phase 3: `start` / `stop` / `restart` (background)

#### Step 3.1: `_child` internal subcommand
- Entry point for the backgrounded server process.
- Receives fully resolved flags (no config file re-resolution needed).
- Runs identical code to `run` (Phase 2).

#### Step 3.2: `start` command
- Resolve config (same as `run`).
- If `--dry-run`: print resolved config and exit.
- Check for existing `server.pid` — refuse if already running.
- Spawn `std::process::Command::new(std::env::current_exe())` with
  `server _child [resolved args]`.
- Redirect stdout/stderr to `server.log` (open file, pass as `Stdio::from`).
- Write `server.pid` and `server.meta.json`.
- Wait briefly, then health-check to confirm startup.
- Print confirmation: "Server started (pid XXXX) on http://addr:port".

#### Step 3.3: `stop` command
- Read `server.pid`.
- `kill(pid, SIGTERM)` via `nix` crate or `libc::kill`.
- Poll for exit (check `/proc/pid` or `kill -0`) with 10s timeout.
- On timeout: warn. With `--force`: `SIGKILL`.
- Remove `server.pid`, `server.meta.json`.

#### Step 3.4: `restart` command
- Read `server.meta.json` for original args.
- Execute `stop`.
- Execute `start` with recovered args + any new overrides.

### Phase 4: `status` and `logs`

#### Step 4.1: `status` command
- Read `server.pid` and `server.meta.json`.
- Check process alive.
- HTTP GET `http://{listen_addr}/health` (reqwest, already a CLI dep).
- Print: pid, listen_addr, uptime (from `started_at`), storage_path,
  server version (from health response).

#### Step 4.2: `logs` command
- Read `server.meta.json` for log file path (or default to
  `data_dir/server.log`).
- `--follow`: use `notify` crate or poll-tail loop.
- `--lines N`: seek to end, walk back N newlines, print.
- Default: last 50 lines.

### Phase 5: Docs + tests

#### Step 5.1: Documentation
- New `docs/cli/server.md` documenting all 6 subcommands.
- Update `docs/cli/README.md` command table.
- Update `docs/SUMMARY.md`.
- Update `docs/getting-started/quickstart-server.md` to mention CLI option.

#### Step 5.2: Tests
- Unit: config resolution merges CLI flags with config file correctly.
- Unit: `server.meta.json` round-trip.
- Integration: `start` → `status` → `stop` lifecycle (may need to pick
  a random port to avoid conflicts in CI).
- Integration: `run` with immediate SIGTERM (clean shutdown).

---

## Files Modified / Created

| File | Action | Description |
|------|--------|-------------|
| `fluree-db-cli/Cargo.toml` | Modify | Add `server` feature + `fluree-db-server` dep |
| `fluree-db-cli/src/cli.rs` | Modify | Add `Commands::Server` + `ServerAction` enum |
| `fluree-db-cli/src/commands/mod.rs` | Modify | Add `server` module (feature-gated) |
| `fluree-db-cli/src/commands/server.rs` | **Create** | All server subcommand handlers |
| `fluree-db-cli/src/main.rs` | Modify | Dispatch `Commands::Server` |
| `fluree-db-cli/src/error.rs` | Modify | Add server error variants |
| `docs/cli/server.md` | **Create** | Server subcommand documentation |
| `docs/cli/README.md` | Modify | Add server to command table |
| `docs/SUMMARY.md` | Modify | Add server doc entry |

---

## Open Questions

1. **Log rotation** — Should `start` truncate `server.log` on each start,
   or append? Appending is safer (preserves crash context) but grows unbounded.
   Recommend: append, with a note in docs about external rotation.

2. **Multiple servers** — Should we support multiple backgrounded servers from
   the same `.fluree/` (e.g., different ports/profiles)? If so, namespace
   PID/log files by profile or port: `server-8090.pid`, `server-dev.pid`.
   Recommend: defer to v2 unless trivially easy.

3. **Windows support** — `SIGTERM` / `kill` are Unix-specific. The `_child`
   pattern works cross-platform, but `stop` needs a Windows equivalent
   (e.g., `TerminateProcess`). Recommend: Unix-only for v1 with a
   `#[cfg(unix)]` gate and clear error on Windows.
