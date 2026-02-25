//! Binary index wire formats: IRB1 (index root), FBR2 (branch), FLI2 (leaf),
//! leaflet column encoding, run record layout, and stats/schema encoding.

pub mod branch;
pub mod index_root;
pub mod leaf;
pub mod leaflet;
pub mod run_record;
pub mod stats_wire;
