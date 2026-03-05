//! Binary index wire formats: IRB1 (index root), FBR2 (branch), FLI2 (leaf),
//! leaflet column encoding, run record layout, and stats/schema encoding.

pub mod branch;
pub mod branch_v3;
pub mod column_block;
pub mod history_sidecar;
pub mod index_root;
pub mod index_root_v6;
pub mod leaf;
pub mod leaf_v3;
pub mod leaflet;
pub mod leaflet_v3;
pub mod run_record;
pub mod run_record_v2;
pub mod stats_wire;
