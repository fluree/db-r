//! Function dispatch - routes Function to specialized implementations
//!
//! This module provides the `Function::eval` method and `eval_function_to_bool` helper.

use crate::binding::RowView;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{Expression, Function};
use fluree_db_core::Storage;

use super::value::ComparableValue;
use super::{conditional, datetime, fluree, geo, hash, numeric, rdf, string, types, uuid, vector};

impl Function {
    /// Evaluate this function to its value.
    ///
    /// This is THE entry point for function evaluation. All functions go through here.
    /// For boolean context, use `eval_to_bool` which calls this and applies EBV.
    pub fn eval<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        match self {
            // String functions
            Function::Str
            | Function::Lang
            | Function::Lcase
            | Function::Ucase
            | Function::Strlen
            | Function::Contains
            | Function::StrStarts
            | Function::StrEnds
            | Function::Regex
            | Function::Concat
            | Function::StrBefore
            | Function::StrAfter
            | Function::Replace
            | Function::Substr
            | Function::EncodeForUri
            | Function::StrDt
            | Function::StrLang => string::eval_string_function(self, args, row, ctx),

            // Numeric functions
            Function::Abs | Function::Round | Function::Ceil | Function::Floor | Function::Rand => {
                numeric::eval_numeric_function(self, args, row, ctx)
            }

            // DateTime functions
            Function::Now
            | Function::Year
            | Function::Month
            | Function::Day
            | Function::Hours
            | Function::Minutes
            | Function::Seconds
            | Function::Tz => datetime::eval_datetime_function(self, args, row, ctx),

            // Type-checking functions
            Function::Bound
            | Function::IsIri
            | Function::IsLiteral
            | Function::IsNumeric
            | Function::IsBlank => types::eval_type_function(self, args, row, ctx),

            // RDF term functions
            Function::Datatype
            | Function::LangMatches
            | Function::SameTerm
            | Function::Iri
            | Function::Bnode => rdf::eval_rdf_function(self, args, row, ctx),

            // Conditional functions
            Function::If | Function::Coalesce => {
                conditional::eval_conditional_function(self, args, row, ctx)
            }

            // Hash functions
            Function::Md5
            | Function::Sha1
            | Function::Sha256
            | Function::Sha384
            | Function::Sha512 => hash::eval_hash_function(self, args, row, ctx),

            // UUID functions
            Function::Uuid | Function::StrUuid => uuid::eval_uuid_function(self, args, row),

            // Vector functions
            Function::DotProduct | Function::CosineSimilarity | Function::EuclideanDistance => {
                vector::eval_vector_function(self, args, row, ctx)
            }

            // Geospatial functions
            Function::GeofDistance => geo::eval_geo_function(self, args, row, ctx),

            // Fluree-specific functions
            Function::T | Function::Op => fluree::eval_fluree_function(self, args, row),

            // Unknown function
            Function::Custom(name) => Err(QueryError::InvalidFilter(format!(
                "Unknown function: {}",
                name
            ))),
        }
    }

    /// Evaluate this function in boolean context using EBV.
    ///
    /// This calls `eval` and applies Effective Boolean Value (EBV) rules.
    pub fn eval_to_bool<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<bool> {
        let value = self.eval(args, row, ctx)?;

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
}
