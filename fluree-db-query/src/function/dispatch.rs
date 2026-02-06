//! Function dispatch - routes FunctionName to specialized implementations
//!
//! This module provides the main entry points for function evaluation:
//! - `eval_function()` - evaluate function to ComparableValue
//! - `eval_function_to_bool()` - evaluate function in boolean context (uses EBV)

use crate::binding::RowView;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{FilterExpr, FunctionName};
use fluree_db_core::Storage;

use super::value::ComparableValue;
use super::{conditional, datetime, fluree, geo, hash, numeric, rdf, string, types, uuid, vector};

/// Evaluate a SPARQL function to its value (unified entry point)
///
/// This is THE entry point for function evaluation. All functions go through here.
/// For boolean context, use `eval_function_to_bool` which calls this and applies EBV.
pub fn eval_function<S: Storage>(
    name: &FunctionName,
    args: &[FilterExpr],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    match name {
        // String functions
        FunctionName::Str
        | FunctionName::Lang
        | FunctionName::Lcase
        | FunctionName::Ucase
        | FunctionName::Strlen
        | FunctionName::Contains
        | FunctionName::StrStarts
        | FunctionName::StrEnds
        | FunctionName::Regex
        | FunctionName::Concat
        | FunctionName::StrBefore
        | FunctionName::StrAfter
        | FunctionName::Replace
        | FunctionName::Substr
        | FunctionName::EncodeForUri
        | FunctionName::StrDt
        | FunctionName::StrLang => string::eval_string_function(name, args, row, ctx),

        // Numeric functions
        FunctionName::Abs
        | FunctionName::Round
        | FunctionName::Ceil
        | FunctionName::Floor
        | FunctionName::Rand => numeric::eval_numeric_function(name, args, row, ctx),

        // DateTime functions
        FunctionName::Now
        | FunctionName::Year
        | FunctionName::Month
        | FunctionName::Day
        | FunctionName::Hours
        | FunctionName::Minutes
        | FunctionName::Seconds
        | FunctionName::Tz => datetime::eval_datetime_function(name, args, row, ctx),

        // Type-checking functions
        FunctionName::Bound
        | FunctionName::IsIri
        | FunctionName::IsLiteral
        | FunctionName::IsNumeric
        | FunctionName::IsBlank => types::eval_type_function(name, args, row, ctx),

        // RDF term functions
        FunctionName::Datatype
        | FunctionName::LangMatches
        | FunctionName::SameTerm
        | FunctionName::Iri
        | FunctionName::Bnode => rdf::eval_rdf_function(name, args, row, ctx),

        // Conditional functions
        FunctionName::If | FunctionName::Coalesce => {
            conditional::eval_conditional_function(name, args, row, ctx)
        }

        // Hash functions
        FunctionName::Md5
        | FunctionName::Sha1
        | FunctionName::Sha256
        | FunctionName::Sha384
        | FunctionName::Sha512 => hash::eval_hash_function(name, args, row, ctx),

        // UUID functions
        FunctionName::Uuid | FunctionName::StrUuid => uuid::eval_uuid_function(name, args, row),

        // Vector functions
        FunctionName::DotProduct
        | FunctionName::CosineSimilarity
        | FunctionName::EuclideanDistance => vector::eval_vector_function(name, args, row, ctx),

        // Geospatial functions
        FunctionName::GeofDistance => geo::eval_geo_function(name, args, row, ctx),

        // Fluree-specific functions
        FunctionName::T | FunctionName::Op => fluree::eval_fluree_function(name, args, row),

        // Unknown function
        FunctionName::Custom(name) => Err(QueryError::InvalidFilter(format!(
            "Unknown function: {}",
            name
        ))),
    }
}

/// Evaluate a function in boolean context using EBV
///
/// This calls `eval_function` and applies Effective Boolean Value (EBV) rules.
pub fn eval_function_to_bool<S: Storage>(
    name: &FunctionName,
    args: &[FilterExpr],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<bool> {
    let value = eval_function(name, args, row, ctx)?;

    // Apply EBV rules
    Ok(match value {
        Some(ComparableValue::Bool(b)) => b,
        Some(ComparableValue::Long(n)) => n != 0,
        Some(ComparableValue::Double(d)) => !d.is_nan() && d != 0.0,
        Some(ComparableValue::String(s)) => !s.is_empty(),
        Some(_) => true, // Other non-null values are truthy
        None => false,   // Null/error is falsy
    })
}
