//! Function evaluation module
//!
//! This module provides unified evaluation of SPARQL expressions and functions.
//! It contains all function implementations organized by category, as well as
//! the core expression evaluation logic.
//!
//! # Module Structure
//!
//! - `value`: ComparableValue type and conversions
//! - `compare`: Value comparison logic
//! - `helpers`: Shared utilities (regex caching, arity checks, etc.)
//! - `eval`: Core expression evaluation
//! - `dispatch`: Main function dispatcher
//! - Category submodules: `string`, `numeric`, `datetime`, `hash`, `uuid`,
//!   `vector`, `geo`, `types`, `rdf`, `conditional`, `fluree`

mod compare;
mod conditional;
mod datetime;
mod dispatch;
mod eval;
mod fluree;
mod geo;
mod hash;
mod helpers;
mod numeric;
mod rdf;
mod string;
mod types;
mod uuid;
mod value;
mod vector;

// Re-export public API
pub use eval::{evaluate, evaluate_to_binding, evaluate_to_binding_strict};
pub use value::{ArithmeticError, ComparableValue, NullValueError};
