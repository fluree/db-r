//! Transaction parsing
//!
//! This module provides parsers for converting JSON-LD transaction
//! representations into the internal Transaction IR.

pub mod jsonld;

pub use jsonld::parse_transaction;
