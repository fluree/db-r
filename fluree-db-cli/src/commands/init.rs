use crate::config;
use crate::error::CliResult;

pub fn run(global: bool) -> CliResult<()> {
    let fluree_dir = config::init_fluree_dir(global)?;
    println!("Initialized Fluree in {}", fluree_dir.display());
    Ok(())
}
