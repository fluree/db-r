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
    evaluate, evaluate_to_binding, evaluate_to_binding_with_context,
    evaluate_to_binding_with_context_strict, evaluate_with_context,
    // Internal exports for use by other modules in the crate
    eval_to_comparable, eval_to_comparable_inner,
};
pub use operator::FilterOperator;
pub use value::ComparableValue;

// Re-export for internal crate use
pub(crate) use compare::compare_values;
pub(crate) use functions::{eval_function, eval_function_to_bool};
pub(crate) use helpers::{
    build_regex_with_flags, check_arity, format_datatype_sid, has_unbound_vars,
    parse_datetime_from_binding, WELL_KNOWN_DATATYPES,
};
pub(crate) use value::{
    comparable_to_flake, comparable_to_str_value, comparable_to_string, eval_arithmetic,
    filter_value_to_comparable, flake_value_to_comparable,
};
