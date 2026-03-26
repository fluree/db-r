use crate::cli::IcebergMapArgs;
use crate::error::{CliError, CliResult};
use fluree_db_api::server_defaults::FlureeDir;

// =============================================================================
// fluree iceberg map
// =============================================================================

pub async fn run_iceberg_map(
    args: IcebergMapArgs,
    dirs: &FlureeDir,
    direct: bool,
) -> CliResult<()> {
    // Try server routing (unless --direct)
    if !direct {
        if let Some(client) = crate::context::try_server_route_client(dirs) {
            return run_iceberg_map_remote(&client, &args).await;
        }
    }

    // Local execution
    run_iceberg_map_local(args, dirs).await
}

async fn run_iceberg_map_remote(
    client: &crate::remote_client::RemoteLedgerClient,
    args: &IcebergMapArgs,
) -> CliResult<()> {
    let body = args_to_json(args);
    let result = client.iceberg_map(&body).await?;

    // Print response
    let gs_id = result
        .get("graph_source_id")
        .and_then(|v| v.as_str())
        .unwrap_or("(unknown)");
    let table = result
        .get("table_identifier")
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let catalog = result
        .get("catalog_uri")
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    let connection = result
        .get("connection_tested")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    if let Some(mapping) = result.get("mapping_source").and_then(|v| v.as_str()) {
        println!("Mapped Iceberg table as R2RML graph source '{gs_id}'");
        println!("  Table:       {table}");
        println!("  Catalog:     {catalog}");
        println!("  R2RML:       {mapping}");
        if let Some(count) = result.get("triples_map_count").and_then(|v| v.as_u64()) {
            println!("  TriplesMaps: {count}");
        }
        println!(
            "  Connection:  {}",
            if connection { "verified" } else { "not tested" }
        );
        let validated = result
            .get("mapping_validated")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        println!(
            "  Mapping:     {}",
            if validated {
                "validated"
            } else {
                "not validated (check mapping source)"
            }
        );
    } else {
        println!("Mapped Iceberg table as graph source '{gs_id}'");
        println!("  Table:       {table}");
        println!("  Catalog:     {catalog}");
        println!(
            "  Connection:  {}",
            if connection {
                "verified"
            } else {
                "not tested (direct mode or catalog unreachable)"
            }
        );
    }

    Ok(())
}

/// Convert CLI args to a JSON body for the server endpoint.
fn args_to_json(args: &IcebergMapArgs) -> serde_json::Value {
    let mut body = serde_json::json!({
        "name": args.name,
        "mode": args.mode,
    });
    let obj = body.as_object_mut().unwrap();

    if let Some(ref v) = args.catalog_uri {
        obj.insert("catalog_uri".into(), v.clone().into());
    }
    if let Some(ref v) = args.table {
        obj.insert("table".into(), v.clone().into());
    }
    if let Some(ref v) = args.table_location {
        obj.insert("table_location".into(), v.clone().into());
    }
    if let Some(ref v) = args.r2rml {
        // Read file content and send it (not the path)
        let content = std::fs::read_to_string(v).unwrap_or_default();
        obj.insert("r2rml".into(), content.into());
    }
    if let Some(ref v) = args.r2rml_type {
        obj.insert("r2rml_type".into(), v.clone().into());
    }
    if let Some(ref v) = args.branch {
        obj.insert("branch".into(), v.clone().into());
    }
    if let Some(ref v) = args.auth_bearer {
        obj.insert("auth_bearer".into(), v.clone().into());
    }
    if let Some(ref v) = args.oauth2_token_url {
        obj.insert("oauth2_token_url".into(), v.clone().into());
    }
    if let Some(ref v) = args.oauth2_client_id {
        obj.insert("oauth2_client_id".into(), v.clone().into());
    }
    if let Some(ref v) = args.oauth2_client_secret {
        obj.insert("oauth2_client_secret".into(), v.clone().into());
    }
    if let Some(ref v) = args.warehouse {
        obj.insert("warehouse".into(), v.clone().into());
    }
    if args.no_vended_credentials {
        obj.insert("no_vended_credentials".into(), true.into());
    }
    if let Some(ref v) = args.s3_region {
        obj.insert("s3_region".into(), v.clone().into());
    }
    if let Some(ref v) = args.s3_endpoint {
        obj.insert("s3_endpoint".into(), v.clone().into());
    }
    if args.s3_path_style {
        obj.insert("s3_path_style".into(), true.into());
    }

    body
}

// =============================================================================
// Local execution (feature-gated)
// =============================================================================

#[cfg(feature = "iceberg")]
async fn run_iceberg_map_local(args: IcebergMapArgs, dirs: &FlureeDir) -> CliResult<()> {
    let fluree = crate::context::build_fluree(dirs)?;
    let iceberg_config = build_iceberg_config(&args)?;

    if let Some(ref r2rml_path) = args.r2rml {
        // Read mapping file content
        let mapping_content = std::fs::read_to_string(r2rml_path).map_err(|e| {
            crate::error::CliError::Input(format!(
                "Failed to read R2RML mapping file '{}': {}",
                r2rml_path.display(),
                e
            ))
        })?;

        let config = fluree_db_api::R2rmlCreateConfig {
            iceberg: iceberg_config,
            mapping: fluree_db_api::R2rmlMappingInput::Content(mapping_content),
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
async fn run_iceberg_map_local(_args: IcebergMapArgs, _dirs: &FlureeDir) -> CliResult<()> {
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
            let table = args.table.as_deref().unwrap_or_default();
            if table.is_empty() && args.r2rml.is_none() {
                return Err(CliError::Usage(
                    "--table is required for rest mode (or use --r2rml to define tables via mapping)"
                        .into(),
                ));
            }
            let table = if table.is_empty() {
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
