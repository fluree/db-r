use crate::cli::IcebergMapArgs;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;

// =============================================================================
// fluree iceberg map
// =============================================================================

#[cfg(feature = "iceberg")]
pub async fn run_iceberg_map(args: IcebergMapArgs, dirs: &FlureeDir) -> CliResult<()> {
    let fluree = crate::context::build_fluree(dirs)?;
    let iceberg_config = build_iceberg_config(&args)?;

    if let Some(ref r2rml_path) = args.r2rml {
        // R2RML mode: map with an R2RML mapping file
        let mapping_source = r2rml_path.to_string_lossy().to_string();

        let config = fluree_db_api::R2rmlCreateConfig {
            iceberg: iceberg_config,
            mapping_source,
            mapping_media_type: args.r2rml_type.clone(),
        };

        let result = fluree.create_r2rml_graph_source(config).await?;

        println!(
            "Mapped Iceberg table as R2RML graph source '{}'",
            result.graph_source_id
        );
        println!("  Table:       {}", result.table_identifier);
        println!("  Catalog:     {}", result.catalog_uri);
        println!("  R2RML:       {}", result.mapping_source);
        println!("  TriplesMaps: {}", result.triples_map_count);
        println!(
            "  Connection:  {}",
            if result.connection_tested {
                "verified"
            } else {
                "not tested"
            }
        );
        println!(
            "  Mapping:     {}",
            if result.mapping_validated {
                "validated"
            } else {
                "not validated (check mapping source)"
            }
        );
    } else {
        // Raw Iceberg mode: no R2RML mapping
        let result = fluree.create_iceberg_graph_source(iceberg_config).await?;

        println!(
            "Mapped Iceberg table as graph source '{}'",
            result.graph_source_id
        );
        println!("  Table:       {}", result.table_identifier);
        println!("  Catalog:     {}", result.catalog_uri);
        println!(
            "  Connection:  {}",
            if result.connection_tested {
                "verified"
            } else {
                "not tested (direct mode or catalog unreachable)"
            }
        );
    }

    Ok(())
}

#[cfg(not(feature = "iceberg"))]
pub async fn run_iceberg_map(_args: IcebergMapArgs, _dirs: &FlureeDir) -> CliResult<()> {
    Err(CliError::Usage(
        "Iceberg support not compiled. Rebuild with `--features iceberg`.".into(),
    ))
}

// =============================================================================
// Helpers
// =============================================================================

#[cfg(feature = "iceberg")]
fn build_iceberg_config(args: &IcebergMapArgs) -> CliResult<fluree_db_api::IcebergCreateConfig> {
    let mode = args.mode.to_lowercase();
    let mut config = match mode.as_str() {
        "rest" => {
            let catalog_uri = args
                .catalog_uri
                .as_ref()
                .ok_or_else(|| CliError::Usage("--catalog-uri is required for rest mode".into()))?;
            // --table is required for rest mode unless --r2rml is provided
            // (R2RML mappings define their own table references)
            let table = args.table.as_deref().unwrap_or_default();
            if table.is_empty() && args.r2rml.is_none() {
                return Err(CliError::Usage(
                    "--table is required for rest mode (or use --r2rml to define tables via mapping)"
                        .into(),
                ));
            }
            let table = if table.is_empty() {
                // Placeholder — R2RML mapping provides the real table references
                "default.default"
            } else {
                table
            };
            fluree_db_api::IcebergCreateConfig::new(&args.name, catalog_uri, table)
        }
        "direct" => {
            let location = args.table_location.as_ref().ok_or_else(|| {
                CliError::Usage("--table-location is required for direct mode".into())
            })?;
            fluree_db_api::IcebergCreateConfig::new_direct(&args.name, location)
        }
        other => {
            return Err(CliError::Usage(format!(
                "unknown catalog mode '{}'. Use 'rest' or 'direct'.",
                other
            )));
        }
    };

    if let Some(ref branch) = args.branch {
        config = config.with_branch(branch);
    }
    if let Some(ref token) = args.auth_bearer {
        config = config.with_auth_bearer(token);
    }
    if let (Some(ref url), Some(ref id), Some(ref secret)) = (
        &args.oauth2_token_url,
        &args.oauth2_client_id,
        &args.oauth2_client_secret,
    ) {
        config = config.with_auth_oauth2(url, id, secret);
    }
    if let Some(ref wh) = args.warehouse {
        config = config.with_warehouse(wh);
    }
    if args.no_vended_credentials {
        config = config.with_vended_credentials(false);
    }
    if let Some(ref region) = args.s3_region {
        config = config.with_s3_region(region);
    }
    if let Some(ref endpoint) = args.s3_endpoint {
        config = config.with_s3_endpoint(endpoint);
    }
    if args.s3_path_style {
        config = config.with_s3_path_style(true);
    }

    Ok(config)
}
