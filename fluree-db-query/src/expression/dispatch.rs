//! Function dispatch - routes Function to specialized implementations
//!
//! This module provides the `Function::eval` method and `eval_function_to_bool` helper.

use crate::binding::RowView;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{Expression, Function};
use fluree_db_core::Storage;

use super::value::ComparableValue;
use super::{
    arithmetic, compare, conditional, datetime, fluree, geo, hash, logical, numeric, rdf, string,
    types, uuid, vector,
};

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
            // Comparison operators
            Function::Eq => compare::eval_eq(args, row, ctx),
            Function::Ne => compare::eval_ne(args, row, ctx),
            Function::Lt => compare::eval_lt(args, row, ctx),
            Function::Le => compare::eval_le(args, row, ctx),
            Function::Gt => compare::eval_gt(args, row, ctx),
            Function::Ge => compare::eval_ge(args, row, ctx),

            // Arithmetic operators
            Function::Add => arithmetic::eval_add(args, row, ctx),
            Function::Sub => arithmetic::eval_sub(args, row, ctx),
            Function::Mul => arithmetic::eval_mul(args, row, ctx),
            Function::Div => arithmetic::eval_div(args, row, ctx),
            Function::Negate => arithmetic::eval_negate(args, row, ctx),

            // Logical operators
            Function::And => logical::eval_and(args, row, ctx),
            Function::Or => logical::eval_or(args, row, ctx),
            Function::Not => logical::eval_not(args, row, ctx),
            Function::In => logical::eval_in(args, row, ctx),
            Function::NotIn => logical::eval_not_in(args, row, ctx),

            // String functions
            Function::Str => string::eval_str(args, row, ctx),
            Function::Lang => string::eval_lang(args, row, ctx),
            Function::Lcase => string::eval_lcase(args, row, ctx),
            Function::Ucase => string::eval_ucase(args, row, ctx),
            Function::Strlen => string::eval_strlen(args, row, ctx),
            Function::Contains => string::eval_contains(args, row, ctx),
            Function::StrStarts => string::eval_str_starts(args, row, ctx),
            Function::StrEnds => string::eval_str_ends(args, row, ctx),
            Function::Regex => string::eval_regex(args, row, ctx),
            Function::Concat => string::eval_concat(args, row, ctx),
            Function::StrBefore => string::eval_str_before(args, row, ctx),
            Function::StrAfter => string::eval_str_after(args, row, ctx),
            Function::Replace => string::eval_replace(args, row, ctx),
            Function::Substr => string::eval_substr(args, row, ctx),
            Function::EncodeForUri => string::eval_encode_for_uri(args, row, ctx),
            Function::StrDt => string::eval_str_dt(args, row, ctx),
            Function::StrLang => string::eval_str_lang(args, row, ctx),

            // Numeric functions
            Function::Abs => numeric::eval_abs(args, row, ctx),
            Function::Round => numeric::eval_round(args, row, ctx),
            Function::Ceil => numeric::eval_ceil(args, row, ctx),
            Function::Floor => numeric::eval_floor(args, row, ctx),
            Function::Rand => numeric::eval_rand(args),

            // DateTime functions
            Function::Now => datetime::eval_now(args),
            Function::Year => datetime::eval_year(args, row),
            Function::Month => datetime::eval_month(args, row),
            Function::Day => datetime::eval_day(args, row),
            Function::Hours => datetime::eval_hours(args, row),
            Function::Minutes => datetime::eval_minutes(args, row),
            Function::Seconds => datetime::eval_seconds(args, row),
            Function::Tz => datetime::eval_tz(args, row),

            // Type-checking functions
            Function::Bound => types::eval_bound(args, row),
            Function::IsIri => types::eval_is_iri(args, row, ctx),
            Function::IsLiteral => types::eval_is_literal(args, row, ctx),
            Function::IsNumeric => types::eval_is_numeric(args, row, ctx),
            Function::IsBlank => types::eval_is_blank(),

            // RDF term functions
            Function::Datatype => rdf::eval_datatype(args, row),
            Function::LangMatches => rdf::eval_lang_matches(args, row, ctx),
            Function::SameTerm => rdf::eval_same_term(args, row, ctx),
            Function::Iri => rdf::eval_iri(args, row, ctx),
            Function::Bnode => rdf::eval_bnode(args),

            // Conditional functions
            Function::If => conditional::eval_if(args, row, ctx),
            Function::Coalesce => conditional::eval_coalesce(args, row, ctx),

            // Hash functions
            Function::Md5 => hash::eval_md5(args, row, ctx),
            Function::Sha1 => hash::eval_sha1(args, row, ctx),
            Function::Sha256 => hash::eval_sha256(args, row, ctx),
            Function::Sha384 => hash::eval_sha384(args, row, ctx),
            Function::Sha512 => hash::eval_sha512(args, row, ctx),

            // UUID functions
            Function::Uuid => uuid::eval_uuid(args),
            Function::StrUuid => uuid::eval_struuid(args),

            // Vector functions
            Function::DotProduct => vector::eval_dot_product(args, row, ctx),
            Function::CosineSimilarity => vector::eval_cosine_similarity(args, row, ctx),
            Function::EuclideanDistance => vector::eval_euclidean_distance(args, row, ctx),

            // Geospatial functions
            Function::GeofDistance => geo::eval_geof_distance(args, row, ctx),

            // Fluree-specific functions
            Function::T => fluree::eval_t(args, row),
            Function::Op => fluree::eval_op(args, row),

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
        Ok(value.is_some_and(Into::into))
    }
}

