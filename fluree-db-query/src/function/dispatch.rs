//! Function dispatch - routes Function to specialized implementations
//!
//! This module provides the `Function::eval` method and `eval_function_to_bool` helper.

use crate::binding::RowView;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{Expression, Function};
use fluree_db_core::Storage;

use super::value::ComparableValue;

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
            Function::Str => self.eval_str(args, row, ctx),
            Function::Lang => self.eval_lang(args, row, ctx),
            Function::Lcase => self.eval_lcase(args, row, ctx),
            Function::Ucase => self.eval_ucase(args, row, ctx),
            Function::Strlen => self.eval_strlen(args, row, ctx),
            Function::Contains => self.eval_contains(args, row, ctx),
            Function::StrStarts => self.eval_str_starts(args, row, ctx),
            Function::StrEnds => self.eval_str_ends(args, row, ctx),
            Function::Regex => self.eval_regex(args, row, ctx),
            Function::Concat => self.eval_concat(args, row, ctx),
            Function::StrBefore => self.eval_str_before(args, row, ctx),
            Function::StrAfter => self.eval_str_after(args, row, ctx),
            Function::Replace => self.eval_replace(args, row, ctx),
            Function::Substr => self.eval_substr(args, row, ctx),
            Function::EncodeForUri => self.eval_encode_for_uri(args, row, ctx),
            Function::StrDt => self.eval_str_dt(args, row, ctx),
            Function::StrLang => self.eval_str_lang(args, row, ctx),

            // Numeric functions
            Function::Abs => self.eval_abs(args, row, ctx),
            Function::Round => self.eval_round(args, row, ctx),
            Function::Ceil => self.eval_ceil(args, row, ctx),
            Function::Floor => self.eval_floor(args, row, ctx),
            Function::Rand => self.eval_rand(args),

            // DateTime functions
            Function::Now => self.eval_now(args),
            Function::Year => self.eval_year(args, row),
            Function::Month => self.eval_month(args, row),
            Function::Day => self.eval_day(args, row),
            Function::Hours => self.eval_hours(args, row),
            Function::Minutes => self.eval_minutes(args, row),
            Function::Seconds => self.eval_seconds(args, row),
            Function::Tz => self.eval_tz(args, row),

            // Type-checking functions
            Function::Bound => self.eval_bound(args, row),
            Function::IsIri => self.eval_is_iri(args, row, ctx),
            Function::IsLiteral => self.eval_is_literal(args, row, ctx),
            Function::IsNumeric => self.eval_is_numeric(args, row, ctx),
            Function::IsBlank => self.eval_is_blank(),

            // RDF term functions
            Function::Datatype => self.eval_datatype(args, row),
            Function::LangMatches => self.eval_lang_matches(args, row, ctx),
            Function::SameTerm => self.eval_same_term(args, row, ctx),
            Function::Iri => self.eval_iri(args, row, ctx),
            Function::Bnode => self.eval_bnode(args),

            // Conditional functions
            Function::If => self.eval_if(args, row, ctx),
            Function::Coalesce => self.eval_coalesce(args, row, ctx),

            // Hash functions
            Function::Md5 => self.eval_md5(args, row, ctx),
            Function::Sha1 => self.eval_sha1(args, row, ctx),
            Function::Sha256 => self.eval_sha256(args, row, ctx),
            Function::Sha384 => self.eval_sha384(args, row, ctx),
            Function::Sha512 => self.eval_sha512(args, row, ctx),

            // UUID functions
            Function::Uuid => self.eval_uuid(args),
            Function::StrUuid => self.eval_struuid(args),

            // Vector functions
            Function::DotProduct => self.eval_dot_product(args, row, ctx),
            Function::CosineSimilarity => self.eval_cosine_similarity(args, row, ctx),
            Function::EuclideanDistance => self.eval_euclidean_distance(args, row, ctx),

            // Geospatial functions
            Function::GeofDistance => self.eval_geof_distance(args, row, ctx),

            // Fluree-specific functions
            Function::T => self.eval_t(args, row),
            Function::Op => self.eval_op(args, row),

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
        Ok(value.map(|v| v.ebv()).unwrap_or(false))
    }
}
