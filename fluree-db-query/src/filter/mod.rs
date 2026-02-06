//! Filter operator and expression evaluation
//!
//! This module provides:
//! - `FilterOperator`: Wraps a child operator and filters rows
//! - `evaluate()`: Evaluates FilterExpr against RowView
//!
//! # Filter Evaluation Semantics
//!
//! This uses **two-valued logic** (true/false), not SQL 3-valued NULL logic:
//!
//! - **Unbound variables**: Comparisons involving unbound vars yield `false`
//! - **Type mismatches**: Comparisons between incompatible types yield `false`
//!   (except `!=` which yields `true` for mismatched types)
//! - **NaN**: Comparisons involving NaN yield `false` (except `!=` → `true`)
//! - **Logical operators**: Standard boolean logic (AND, OR, NOT)
//!
//! Note: `NOT(unbound_comparison)` evaluates to `true` because the inner
//! comparison returns `false`, which is then negated. This differs from
//! SQL NULL semantics where NULL comparisons propagate.
//!
//! # Module Structure
//!
//! - `operator`: The `FilterOperator` struct
//! - `eval`: Core evaluation functions (`evaluate`, `eval_to_comparable`)
//! - `functions`: SPARQL function evaluation (`eval_function`)
//! - `value`: `ComparableValue` type and conversions
//! - `compare`: Value comparison logic
//! - `helpers`: Shared utilities (regex caching, arity checks, etc.)

mod compare;
mod eval;
mod functions;
mod helpers;
mod operator;
mod value;

// Re-export public API
pub use eval::{
    // Internal exports for use by other modules in the crate
    eval_to_comparable,
    eval_to_comparable_inner,
    evaluate,
    evaluate_to_binding,
    evaluate_to_binding_with_context,
    evaluate_to_binding_with_context_strict,
    evaluate_with_context,
};
pub use operator::FilterOperator;
pub use value::ComparableValue;
