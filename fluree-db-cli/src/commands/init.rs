use crate::config;
use crate::error::CliResult;
use fluree_db_api::server_defaults::{generate_config_template_for, ConfigFormat};

pub fn run(global: bool, format: ConfigFormat) -> CliResult<()> {
    let template = generate_config_template_for(format);
    let fluree_dir = config::init_fluree_dir(global, &template, format.filename())?;
    println!("Initialized Fluree in {}", fluree_dir.display());
    Ok(())
}
