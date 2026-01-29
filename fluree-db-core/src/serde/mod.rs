//! JSON serialization/deserialization for Fluree index data
//!
//! This module handles the JSON format used by Fluree for storing index nodes.
//!
//! ## Flake Format
//!
//! Flakes are serialized as 7-element arrays:
//! ```json
//! [s, p, o, dt, t, op, m]
//! ```
//! Where:
//! - `s`, `p`, `dt` are SIDs: `[namespace_code, name]`
//! - `o` is polymorphic (string, number, boolean, or SID for refs)
//! - `t` is transaction time (integer)
//! - `op` is operation (boolean)
//! - `m` is metadata (object or null)
//!
//! ## Node Types
//!
//! - **DbRoot**: Index metadata with ledger-alias, t, namespace-codes, and index roots
//! - **BranchNode**: Contains children array
//! - **LeafNode**: Contains flakes array

pub mod flakes_transport;
pub mod json;

pub use flakes_transport::*;
pub use json::*;
