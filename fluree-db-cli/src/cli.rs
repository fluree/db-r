use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "fluree", about = "Fluree database CLI", version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Enable verbose output
    #[arg(long, short = 'v', global = true, conflicts_with = "quiet")]
    pub verbose: bool,

    /// Suppress non-essential output
    #[arg(long, short = 'q', global = true, conflicts_with = "verbose")]
    pub quiet: bool,

    /// Disable colored output (also respects NO_COLOR env var)
    #[arg(long, global = true)]
    pub no_color: bool,

    /// Path to config file
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    /// Memory budget in MB for bulk import (0 = auto: 75% of system RAM).
    /// Derives chunk size, concurrency limits, and run budget when not set explicitly.
    #[arg(long, global = true, default_value_t = 0)]
    pub memory_budget_mb: usize,

    /// Number of parallel parse threads for bulk import.
    /// 0 = auto (system cores, default cap 6). Explicit values are not capped.
    #[arg(long, global = true, default_value_t = 0)]
    pub parallelism: usize,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize a new Fluree project directory
    Init {
        /// Create global config at ~/.fluree/ instead of local .fluree/
        #[arg(long)]
        global: bool,
    },

    /// Create a new ledger
    Create {
        /// Ledger name
        ledger: String,

        /// Import data from a file or directory
        #[arg(long)]
        from: Option<PathBuf>,

        /// Chunk size in MB for splitting large Turtle files (0 = derive from memory budget).
        /// Only used when --from points to a .ttl file.
        #[arg(long, default_value_t = 0)]
        chunk_size_mb: usize,
    },

    /// Set the active ledger
    Use {
        /// Ledger name to set as active
        ledger: String,
    },

    /// List all ledgers
    List {
        /// List ledgers on a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,
    },

    /// Show detailed information about a ledger
    Info {
        /// Ledger name (defaults to active ledger)
        ledger: Option<String>,

        /// Query a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,
    },

    /// Drop (delete) a ledger
    Drop {
        /// Ledger name to drop
        name: String,

        /// Required flag to confirm deletion
        #[arg(long)]
        force: bool,
    },

    /// Insert data into a ledger
    ///
    /// Examples:
    ///   fluree insert -e '<http://example.org/alice> a <http://example.org/Person> .'
    ///   fluree insert mydb data.ttl
    ///   fluree insert data.jsonld
    ///   cat data.ttl | fluree insert
    Insert {
        /// Optional ledger name and/or data file path.
        ///
        /// With 0 args: uses active ledger; provide data via -e or stdin.
        /// With 1 arg: if a file exists at that path, reads data from it
        ///   (uses active ledger); otherwise treats it as a ledger name
        ///   (provide data via -e or stdin).
        /// With 2 args: first is ledger name, second is data file path.
        #[arg(num_args = 0..=2)]
        args: Vec<String>,

        /// Inline data expression (Turtle or JSON-LD).
        /// Use this instead of a file when passing data directly.
        #[arg(short = 'e', long = "expr")]
        expr: Option<String>,

        /// Commit message
        #[arg(short = 'm', long = "message")]
        message: Option<String>,

        /// Data format (turtle or jsonld); auto-detected if omitted
        #[arg(long)]
        format: Option<String>,

        /// Execute against a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,
    },

    /// Upsert data into a ledger (insert or update existing)
    ///
    /// Examples:
    ///   fluree upsert -e '<http://example.org/alice> <http://example.org/name> "Alice" .'
    ///   fluree upsert mydb data.ttl
    ///   cat data.jsonld | fluree upsert
    Upsert {
        /// Optional ledger name and/or data file path.
        ///
        /// With 0 args: uses active ledger; provide data via -e or stdin.
        /// With 1 arg: if a file exists at that path, reads data from it
        ///   (uses active ledger); otherwise treats it as a ledger name
        ///   (provide data via -e or stdin).
        /// With 2 args: first is ledger name, second is data file path.
        #[arg(num_args = 0..=2)]
        args: Vec<String>,

        /// Inline data expression (Turtle or JSON-LD).
        /// Use this instead of a file when passing data directly.
        #[arg(short = 'e', long = "expr")]
        expr: Option<String>,

        /// Commit message
        #[arg(short = 'm', long = "message")]
        message: Option<String>,

        /// Data format (turtle or jsonld); auto-detected if omitted
        #[arg(long)]
        format: Option<String>,

        /// Execute against a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,
    },

    /// Query a ledger
    ///
    /// Examples:
    ///   fluree query -e 'SELECT ?s ?p ?o WHERE { ?s ?p ?o }'
    ///   fluree query mydb -e '{"select": ["*"], "where": {"@type": "Person"}}'
    ///   fluree query mydb query.sparql
    ///   fluree query query.sparql
    ///   cat query.rq | fluree query
    Query {
        /// Optional ledger name and/or query file path.
        ///
        /// With 0 args: uses active ledger; provide query via -e or stdin.
        /// With 1 arg: if a file exists at that path, reads query from it
        ///   (uses active ledger); otherwise treats it as a ledger name
        ///   (provide query via -e or stdin).
        /// With 2 args: first is ledger name, second is query file path.
        #[arg(num_args = 0..=2)]
        args: Vec<String>,

        /// Inline query expression (SPARQL or FQL JSON).
        /// Use this instead of a file when passing a query directly.
        #[arg(short = 'e', long = "expr")]
        expr: Option<String>,

        /// Output format (json, table, or csv)
        #[arg(long, default_value = "json")]
        format: String,

        /// Force SPARQL query format
        #[arg(long, conflicts_with = "fql")]
        sparql: bool,

        /// Force FQL query format
        #[arg(long, conflicts_with = "sparql")]
        fql: bool,

        /// Query at a specific point in time (transaction number, commit hash, or ISO-8601 timestamp)
        #[arg(long)]
        at: Option<String>,

        /// Execute against a remote server (by remote name, e.g., "origin")
        #[arg(long)]
        remote: Option<String>,
    },

    /// Show change history for an entity
    History {
        /// Entity IRI (e.g., "ex:alice" or full IRI). Uses stored prefixes for expansion.
        entity: String,

        /// Ledger name (defaults to active ledger)
        #[arg(long)]
        ledger: Option<String>,

        /// Start of time range (transaction number, default: 1)
        #[arg(long, default_value = "1")]
        from: String,

        /// End of time range (transaction number or "latest", default: latest)
        #[arg(long, default_value = "latest")]
        to: String,

        /// Filter to specific predicate
        #[arg(short = 'p', long)]
        predicate: Option<String>,

        /// Output format (json, table, or csv)
        #[arg(long, default_value = "table")]
        format: String,
    },

    /// Export ledger data as Turtle or JSON-LD
    Export {
        /// Ledger name (defaults to active ledger)
        ledger: Option<String>,

        /// Output format: turtle or jsonld (default: turtle)
        #[arg(long, default_value = "turtle")]
        format: String,

        /// Query at a specific point in time
        #[arg(long)]
        at: Option<String>,
    },

    /// Show commit log for a ledger
    Log {
        /// Ledger name (defaults to active ledger)
        ledger: Option<String>,

        /// Show one-line summary per commit
        #[arg(long)]
        oneline: bool,

        /// Maximum number of commits to show
        #[arg(short = 'n', long)]
        count: Option<usize>,
    },

    /// Manage configuration
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },

    /// Manage IRI prefix mappings
    Prefix {
        #[command(subcommand)]
        action: PrefixAction,
    },

    /// Generate shell completions
    Completions {
        /// Shell to generate completions for (bash, zsh, fish, powershell, elvish)
        shell: clap_complete::Shell,
    },

    /// Manage JWS tokens for authentication
    Token {
        #[command(subcommand)]
        action: TokenAction,
    },

    /// Manage remote servers
    Remote {
        #[command(subcommand)]
        action: RemoteAction,
    },

    /// Manage authentication tokens for remotes
    Auth {
        #[command(subcommand)]
        action: AuthAction,
    },

    /// Manage upstream tracking configuration
    Upstream {
        #[command(subcommand)]
        action: UpstreamAction,
    },

    /// Fetch refs from a remote (like git fetch)
    Fetch {
        /// Remote name (e.g., "origin")
        remote: String,
    },

    /// Pull (fetch + fast-forward) a ledger from its upstream
    Pull {
        /// Ledger name (defaults to active ledger)
        ledger: Option<String>,
    },

    /// Push a ledger to its upstream remote
    Push {
        /// Ledger name (defaults to active ledger)
        ledger: Option<String>,
    },

    /// Clone a ledger from a remote server (downloads all commits)
    ///
    /// Usage:
    ///   fluree clone <remote> <ledger>                        # named-remote clone
    ///   fluree clone --origin <uri> <ledger>                  # CID-based clone
    ///   fluree clone --origin <uri> --token <tok> <ledger>    # with auth
    Clone {
        /// Positional args: <remote> <ledger> (named-remote) or <ledger> (with --origin)
        #[arg(num_args = 1..=2)]
        args: Vec<String>,

        /// Bootstrap URI (e.g., "http://localhost:8090") — CID-based clone
        #[arg(long)]
        origin: Option<String>,

        /// Auth token for the origin server
        #[arg(long, requires = "origin")]
        token: Option<String>,

        /// Local alias for the cloned ledger (defaults to remote ledger name).
        /// Not yet supported: CAS addresses embed ledger paths, so aliasing
        /// requires address rewriting (planned for a future release).
        #[arg(long, hide = true)]
        alias: Option<String>,
    },

    /// Track a remote ledger (remote-only, no local data)
    Track {
        #[command(subcommand)]
        action: TrackAction,
    },
}

#[derive(Subcommand)]
pub enum TrackAction {
    /// Start tracking a remote ledger
    Add {
        /// Ledger alias (local name for this tracked ledger)
        ledger: String,

        /// Remote name (e.g., "origin"); defaults to the only configured remote
        #[arg(long)]
        remote: Option<String>,

        /// Alias on the remote (defaults to local alias)
        #[arg(long)]
        remote_alias: Option<String>,
    },

    /// Stop tracking a remote ledger
    Remove {
        /// Ledger alias to stop tracking
        ledger: String,
    },

    /// List all tracked ledgers
    List,

    /// Show status of tracked ledger(s) from remote
    Status {
        /// Ledger alias (shows all if omitted)
        ledger: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum ConfigAction {
    /// Get a configuration value
    Get {
        /// Configuration key (e.g., "storage.path")
        key: String,
    },

    /// Set a configuration value
    Set {
        /// Configuration key
        key: String,

        /// Configuration value
        value: String,
    },

    /// List all configuration values
    List,

    /// Set origin configuration for a ledger (content origins for CID-based fetch)
    SetOrigins {
        /// Ledger name
        ledger: String,

        /// Path to origins config JSON file
        #[arg(long)]
        file: PathBuf,
    },
}

#[derive(Subcommand)]
pub enum PrefixAction {
    /// Add a prefix mapping (e.g., "ex" "http://example.org/")
    Add {
        /// Prefix (e.g., "ex")
        prefix: String,

        /// IRI namespace (e.g., "http://example.org/")
        iri: String,
    },

    /// Remove a prefix mapping
    Remove {
        /// Prefix to remove
        prefix: String,
    },

    /// List all prefix mappings
    List,
}

/// Arguments for `fluree token create` (extracted to reduce enum size).
#[derive(Debug, Clone, clap::Args)]
pub struct TokenCreateArgs {
    /// Ed25519 private key (hex with 0x prefix, base58btc, @filepath, or @- for stdin)
    #[arg(long, required = true)]
    pub private_key: String,

    /// Token lifetime (e.g., "1h", "30m", "7d", "1w") [default: 1h]
    #[arg(long, default_value = "1h")]
    pub expires_in: String,

    /// Subject claim (sub) - identity of the token holder
    #[arg(long)]
    pub subject: Option<String>,

    /// Audience claim (aud) - repeatable for multiple audiences
    #[arg(long = "audience")]
    pub audiences: Vec<String>,

    /// Fluree identity claim (fluree.identity) - takes precedence over sub for policy
    #[arg(long)]
    pub identity: Option<String>,

    /// Grant access to all ledgers (fluree.events.all=true, fluree.storage.all=true)
    #[arg(long)]
    pub all: bool,

    /// Grant events access to specific ledger (repeatable)
    #[arg(long = "events-ledger")]
    pub events_ledgers: Vec<String>,

    /// Grant storage access to specific ledger (repeatable)
    #[arg(long = "storage-ledger")]
    pub storage_ledgers: Vec<String>,

    /// Grant data API read access to all ledgers (fluree.ledger.read.all=true)
    #[arg(long)]
    pub read_all: bool,

    /// Grant data API read access to a specific ledger (repeatable)
    #[arg(long = "read-ledger")]
    pub read_ledgers: Vec<String>,

    /// Grant data API write access to all ledgers (fluree.ledger.write.all=true)
    #[arg(long)]
    pub write_all: bool,

    /// Grant data API write access to a specific ledger (repeatable)
    #[arg(long = "write-ledger")]
    pub write_ledgers: Vec<String>,

    /// Grant access to specific graph source (repeatable)
    #[arg(long = "graph-source")]
    pub graph_sources: Vec<String>,

    /// Output format
    #[arg(long, default_value = "token", value_enum)]
    pub output: TokenOutputFormat,

    /// Print decoded claims to stderr (for verification)
    #[arg(long)]
    pub print_claims: bool,
}

#[derive(Subcommand)]
pub enum TokenAction {
    /// Create a new JWS token for authentication
    Create(Box<TokenCreateArgs>),

    /// Generate a new Ed25519 keypair
    Keygen {
        /// Output format for the keypair
        #[arg(long, default_value = "hex", value_enum)]
        format: KeyFormat,

        /// Write private key to file (otherwise prints to stdout)
        #[arg(long, short = 'o')]
        output: Option<PathBuf>,
    },

    /// Inspect (decode and verify) a JWS token
    Inspect {
        /// JWS token string or @filepath
        token: String,

        /// Skip signature verification
        #[arg(long)]
        no_verify: bool,

        /// Output format
        #[arg(long, default_value = "pretty", value_enum)]
        output: InspectOutputFormat,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum TokenOutputFormat {
    /// Just the JWS token string
    Token,
    /// JSON object with token and decoded claims
    Json,
    /// Ready-to-use curl command for events endpoint
    Curl,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum KeyFormat {
    /// Hex with 0x prefix (64 chars)
    Hex,
    /// Base58btc with z prefix (multibase)
    Base58,
    /// JSON object with hex, base58, and did:key
    Json,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum InspectOutputFormat {
    /// Human-readable formatted output
    Pretty,
    /// Raw JSON
    Json,
    /// Table format for claims
    Table,
}

#[derive(Subcommand)]
pub enum RemoteAction {
    /// Add a remote server
    Add {
        /// Remote name (e.g., "origin")
        name: String,

        /// Server URL (e.g., "http://localhost:8090")
        url: String,

        /// Authentication token (or @filepath to read from file)
        #[arg(long)]
        token: Option<String>,
    },

    /// Remove a remote
    Remove {
        /// Remote name to remove
        name: String,
    },

    /// List all remotes
    List,

    /// Show details for a remote
    Show {
        /// Remote name
        name: String,
    },
}

#[derive(Subcommand)]
pub enum AuthAction {
    /// Show authentication status for a remote
    Status {
        /// Remote name (defaults to only configured remote)
        #[arg(long)]
        remote: Option<String>,
    },

    /// Store a bearer token for a remote
    Login {
        /// Remote name (defaults to only configured remote)
        #[arg(long)]
        remote: Option<String>,

        /// Token value, @filepath to read from file, or @- for stdin
        #[arg(long)]
        token: Option<String>,
    },

    /// Clear the stored token for a remote
    Logout {
        /// Remote name (defaults to only configured remote)
        #[arg(long)]
        remote: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum UpstreamAction {
    /// Set upstream tracking for a ledger
    Set {
        /// Local ledger alias (e.g., "mydb" or "mydb:main")
        local: String,

        /// Remote name (e.g., "origin")
        remote: String,

        /// Remote ledger alias (defaults to local alias)
        #[arg(long)]
        remote_alias: Option<String>,

        /// Automatically pull on fetch
        #[arg(long)]
        auto_pull: bool,
    },

    /// Remove upstream tracking for a ledger
    Remove {
        /// Local ledger alias
        local: String,
    },

    /// List all upstream configurations
    List,
}
