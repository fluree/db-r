//! Data generation for benchmarks.
//!
//! Generates deterministic, relational supply-chain data at arbitrary scale.

pub mod scale;
pub mod supply_chain;

pub use scale::compute_units;
