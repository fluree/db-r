use clap::{Parser, Subcommand};
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
    },

    /// Set the active ledger
    Use {
        /// Ledger name to set as active
        ledger: String,
    },

    /// List all ledgers
    List,

    /// Show detailed information about a ledger
    Info {
        /// Ledger name (defaults to active ledger)
        ledger: Option<String>,
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
    Insert {
        /// Positional args: [<ledger>] [<file>]
        /// With 0 args: active ledger + stdin/-e
        /// With 1 arg: if file exists, active ledger + file; else ledger alias + stdin/-e
        /// With 2 args: ledger alias + file
        #[arg(num_args = 0..=2)]
        args: Vec<String>,

        /// Inline expression
        #[arg(short = 'e', long = "expr")]
        expr: Option<String>,

        /// Commit message
        #[arg(short = 'm', long = "message")]
        message: Option<String>,

        /// Data format (turtle or jsonld); auto-detected if omitted
        #[arg(long)]
        format: Option<String>,
    },

    /// Upsert data into a ledger (insert or update existing)
    Upsert {
        /// Positional args: [<ledger>] [<file>]
        /// Same resolution as insert
        #[arg(num_args = 0..=2)]
        args: Vec<String>,

        /// Inline expression
        #[arg(short = 'e', long = "expr")]
        expr: Option<String>,

        /// Commit message
        #[arg(short = 'm', long = "message")]
        message: Option<String>,

        /// Data format (turtle or jsonld); auto-detected if omitted
        #[arg(long)]
        format: Option<String>,
    },

    /// Query a ledger
    Query {
        /// Positional args: [<ledger>] [<file>]
        /// Same resolution as insert
        #[arg(num_args = 0..=2)]
        args: Vec<String>,

        /// Inline expression
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
