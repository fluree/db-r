use crate::config;
use crate::error::CliResult;
use fluree_db_api::server_defaults::generate_config_template;

pub fn run(global: bool) -> CliResult<()> {
    let template = generate_config_template();
    let fluree_dir = config::init_fluree_dir(global, &template)?;
    println!("Initialized Fluree in {}", fluree_dir.display());
    Ok(())
}
