mod cli;
mod commands;
mod config;
mod context;
mod detect;
mod error;
mod input;
mod output;
mod remote_client;

use clap::Parser;
use cli::{Cli, Commands};
use error::exit_with_error;

fn init_tracing(cli: &Cli) {
    // CLI tracing policy:
    //   --quiet  → always "off" (no logs, no matter what)
    //   --verbose → "info" level for fluree crates (useful diagnostics)
    //   default  → "off" (clean terminal, progress bars only)
    //   RUST_LOG → honoured only when neither --verbose nor --quiet is set,
    //              so developers can still get fine-grained control.
    let filter = if cli.quiet {
        tracing_subscriber::EnvFilter::new("off")
    } else if cli.verbose {
        // --verbose: honour RUST_LOG if set, otherwise show info for fluree crates.
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "info".into())
    } else {
        // Default: suppress all logs. RUST_LOG is intentionally ignored so that
        // developer env vars don't leak log lines into the user-facing CLI output
        // (which uses progress bars on stderr). Use --verbose to see logs.
        tracing_subscriber::EnvFilter::new("off")
    };

    let ansi = !(cli.no_color || std::env::var_os("NO_COLOR").is_some());

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(ansi)
        .with_target(true)
        .with_writer(std::io::stderr)
        .init();
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Disable color when --no-color flag or NO_COLOR env var is set.
    // We intentionally do NOT disable on "stdout is not a TTY" because errors
    // go to stderr — piping stdout (e.g., `fluree query ... | jq`) should not
    // strip color from error messages that appear on the terminal's stderr.
    if cli.no_color || std::env::var_os("NO_COLOR").is_some() {
        colored::control::set_override(false);
    }

    init_tracing(&cli);

    if let Err(e) = run(cli).await {
        exit_with_error(e);
    }
}

async fn run(cli: Cli) -> error::CliResult<()> {
    let config_path = cli.config.as_deref();

    match cli.command {
        Commands::Init { global, format } => {
            let config_format = match format {
                cli::InitFormat::Toml => fluree_db_api::server_defaults::ConfigFormat::Toml,
                cli::InitFormat::Jsonld => fluree_db_api::server_defaults::ConfigFormat::JsonLd,
            };
            commands::init::run(global, config_format)
        }

        Commands::Create {
            ledger,
            from,
            chunk_size_mb,
            memory_budget_mb,
            parallelism,
            leaflet_rows,
            leaflets_per_leaf,
        } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            // Create-specific flags take precedence; fall back to global flags.
            let import_opts = commands::create::ImportOpts {
                memory_budget_mb: if memory_budget_mb > 0 {
                    memory_budget_mb
                } else {
                    cli.memory_budget_mb
                },
                parallelism: if parallelism > 0 {
                    parallelism
                } else {
                    cli.parallelism
                },
                chunk_size_mb,
                leaflet_rows,
                leaflets_per_leaf,
            };
            commands::create::run(
                &ledger,
                from.as_deref(),
                &fluree_dir,
                cli.verbose,
                cli.quiet,
                &import_opts,
            )
            .await
        }

        Commands::Use { ledger } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::use_cmd::run(&ledger, &fluree_dir).await
        }

        Commands::List { remote } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::list::run(&fluree_dir, remote.as_deref()).await
        }

        Commands::Info { ledger, remote } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::info::run(ledger.as_deref(), &fluree_dir, remote.as_deref()).await
        }

        Commands::Drop { name, force } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::drop::run(&name, force, &fluree_dir).await
        }

        Commands::Insert {
            args,
            expr,
            message,
            format,
            remote,
        } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::insert::run(
                &args,
                expr.as_deref(),
                message.as_deref(),
                format.as_deref(),
                &fluree_dir,
                remote.as_deref(),
            )
            .await
        }

        Commands::Upsert {
            args,
            expr,
            message,
            format,
            remote,
        } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::upsert::run(
                &args,
                expr.as_deref(),
                message.as_deref(),
                format.as_deref(),
                &fluree_dir,
                remote.as_deref(),
            )
            .await
        }

        Commands::Query {
            args,
            expr,
            format,
            bench,
            sparql,
            jsonld,
            at,
            remote,
        } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::query::run(
                &args,
                expr.as_deref(),
                &format,
                bench,
                sparql,
                jsonld,
                at.as_deref(),
                &fluree_dir,
                remote.as_deref(),
            )
            .await
        }

        Commands::History {
            entity,
            ledger,
            from,
            to,
            predicate,
            format,
        } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::history::run(
                &entity,
                ledger.as_deref(),
                &from,
                &to,
                predicate.as_deref(),
                &format,
                &fluree_dir,
            )
            .await
        }

        Commands::Export { ledger, format, at } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::export::run(ledger.as_deref(), &format, at.as_deref(), &fluree_dir).await
        }

        Commands::Log {
            ledger,
            oneline,
            count,
        } => {
            let fluree_dir = config::require_fluree_dir_or_global(config_path)?;
            commands::log::run(ledger.as_deref(), oneline, count, &fluree_dir).await
        }

        Commands::Config { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            match action {
                cli::ConfigAction::SetOrigins { ledger, file } => {
                    commands::config_cmd::run_set_origins(&ledger, &file, &fluree_dir).await
                }
                other => commands::config_cmd::run(other, &fluree_dir),
            }
        }

        Commands::Prefix { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::prefix::run(action, &fluree_dir)
        }

        Commands::Completions { shell } => {
            commands::completions::run(shell);
            Ok(())
        }

        Commands::Token { action } => commands::token::run(action),

        Commands::Remote { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::remote::run(action, &fluree_dir).await
        }

        Commands::Auth { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::auth::run(action, &fluree_dir).await
        }

        Commands::Upstream { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::upstream::run(action, &fluree_dir).await
        }

        Commands::Fetch { remote } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::sync::run_fetch(&remote, &fluree_dir).await
        }

        Commands::Pull { ledger, no_indexes } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::sync::run_pull(ledger.as_deref(), no_indexes, &fluree_dir).await
        }

        Commands::Push { ledger } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::sync::run_push(ledger.as_deref(), &fluree_dir).await
        }

        Commands::Clone {
            args,
            origin,
            token,
            alias,
            no_indexes,
        } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            if let Some(origin_uri) = origin {
                // --origin mode: args = [ledger]
                if args.len() != 1 {
                    return Err(error::CliError::Usage(
                        "with --origin, provide exactly one positional arg: <ledger>".into(),
                    ));
                }
                commands::sync::run_clone_origin(
                    &origin_uri,
                    token.as_deref(),
                    &args[0],
                    alias.as_deref(),
                    no_indexes,
                    &fluree_dir,
                )
                .await
            } else {
                // Named-remote mode: args = [remote, ledger]
                if args.len() != 2 {
                    return Err(error::CliError::Usage(
                        "usage: fluree clone <remote> <ledger>  or  fluree clone --origin <uri> <ledger>".into(),
                    ));
                }
                commands::sync::run_clone(
                    &args[0],
                    &args[1],
                    alias.as_deref(),
                    no_indexes,
                    &fluree_dir,
                )
                .await
            }
        }

        Commands::Track { action } => {
            let fluree_dir = config::require_fluree_dir(config_path)?;
            commands::track::run(action, &fluree_dir).await
        }
    }
}
