//! String function implementations
//!
//! Implements SPARQL string functions: STR, LANG, LCASE, UCASE, STRLEN,
//! CONTAINS, STRSTARTS, STRENDS, REGEX, CONCAT, STRBEFORE, STRAFTER,
//! REPLACE, SUBSTR, ENCODE_FOR_URI, STRDT, STRLANG

use crate::binding::{Binding, RowView};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{Expression, Function};
use fluree_db_core::{FlakeValue, Storage};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use std::sync::Arc;

use super::helpers::{build_regex_with_flags, check_arity};
use super::value::ComparableValue;

impl Function {
    pub(super) fn eval_str<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 1, "STR")?;
        let val = args[0].eval_to_comparable(row, ctx)?;
        Ok(val.and_then(|v| v.into_string_value()))
    }

    pub(super) fn eval_lang<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 1, "LANG")?;
        let tag = match &args[0] {
            Expression::Var(var_id) => match row.get(*var_id) {
                Some(Binding::Lit { lang, .. }) => {
                    lang.as_ref().map(|l| l.to_string()).unwrap_or_default()
                }
                Some(Binding::EncodedLit { lang_id, .. }) => {
                    if let Some(store) = ctx.and_then(|c| c.binary_store.as_deref()) {
                        store
                            .resolve_lang_id(*lang_id)
                            .map(|s| s.to_string())
                            .unwrap_or_default()
                    } else {
                        String::new()
                    }
                }
                _ => String::new(),
            },
            _ => String::new(),
        };
        Ok(Some(ComparableValue::String(Arc::from(tag))))
    }

    pub(super) fn eval_lcase<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 1, "LCASE")?;
        match args[0].eval_to_comparable(row, ctx)? {
            Some(v) => match v.as_str() {
                Some(s) => Ok(Some(ComparableValue::String(Arc::from(s.to_lowercase())))),
                None => Err(QueryError::InvalidFilter(
                    "LCASE requires a string argument".to_string(),
                )),
            },
            None => Ok(None),
        }
    }

    pub(super) fn eval_ucase<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 1, "UCASE")?;
        match args[0].eval_to_comparable(row, ctx)? {
            Some(v) => match v.as_str() {
                Some(s) => Ok(Some(ComparableValue::String(Arc::from(s.to_uppercase())))),
                None => Err(QueryError::InvalidFilter(
                    "UCASE requires a string argument".to_string(),
                )),
            },
            None => Ok(None),
        }
    }

    pub(super) fn eval_strlen<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 1, "STRLEN")?;
        match args[0].eval_to_comparable(row, ctx)? {
            Some(v) => match v.as_str() {
                Some(s) => Ok(Some(ComparableValue::Long(s.len() as i64))),
                None => Err(QueryError::InvalidFilter(
                    "STRLEN requires a string argument".to_string(),
                )),
            },
            None => Ok(None),
        }
    }

    pub(super) fn eval_contains<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 2, "CONTAINS")?;
        let haystack = args[0].eval_to_comparable(row, ctx)?;
        let needle = args[1].eval_to_comparable(row, ctx)?;
        match (haystack, needle) {
            (Some(ComparableValue::String(h)), Some(ComparableValue::String(n))) => {
                Ok(Some(ComparableValue::Bool(h.contains(n.as_ref()))))
            }
            (None, _) | (_, None) => Ok(None),
            _ => Err(QueryError::InvalidFilter(
                "CONTAINS requires string arguments".to_string(),
            )),
        }
    }

    pub(super) fn eval_str_starts<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 2, "STRSTARTS")?;
        let haystack = args[0].eval_to_comparable(row, ctx)?;
        let prefix = args[1].eval_to_comparable(row, ctx)?;
        match (haystack, prefix) {
            (Some(ComparableValue::String(h)), Some(ComparableValue::String(p))) => {
                Ok(Some(ComparableValue::Bool(h.starts_with(p.as_ref()))))
            }
            (None, _) | (_, None) => Ok(None),
            _ => Err(QueryError::InvalidFilter(
                "STRSTARTS requires string arguments".to_string(),
            )),
        }
    }

    pub(super) fn eval_str_ends<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 2, "STRENDS")?;
        let haystack = args[0].eval_to_comparable(row, ctx)?;
        let suffix = args[1].eval_to_comparable(row, ctx)?;
        match (haystack, suffix) {
            (Some(ComparableValue::String(h)), Some(ComparableValue::String(s))) => {
                Ok(Some(ComparableValue::Bool(h.ends_with(s.as_ref()))))
            }
            (None, _) | (_, None) => Ok(None),
            _ => Err(QueryError::InvalidFilter(
                "STRENDS requires string arguments".to_string(),
            )),
        }
    }

    pub(super) fn eval_regex<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        if args.len() < 2 {
            return Err(QueryError::InvalidFilter(
                "REGEX requires 2-3 arguments".to_string(),
            ));
        }
        let text = args[0].eval_to_comparable(row, ctx)?;
        let pattern = args[1].eval_to_comparable(row, ctx)?;
        let flags = if args.len() > 2 {
            match args[2].eval_to_comparable(row, ctx)? {
                Some(v) => v
                    .as_str()
                    .map(|s| s.to_string())
                    .ok_or_else(|| QueryError::InvalidFilter(
                        "REGEX flags must be a string".to_string(),
                    ))?,
                None => return Ok(None),
            }
        } else {
            String::new()
        };

        match (text, pattern) {
            (Some(ComparableValue::String(t)), Some(ComparableValue::String(p))) => {
                let re = build_regex_with_flags(&p, &flags)?;
                Ok(Some(ComparableValue::Bool(re.is_match(&t))))
            }
            (None, _) | (_, None) => Ok(None),
            _ => Err(QueryError::InvalidFilter(
                "REGEX requires string arguments".to_string(),
            )),
        }
    }

    pub(super) fn eval_concat<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        let mut result = String::new();
        for arg in args {
            if let Some(val) = arg.eval_to_comparable(row, ctx)? {
                if let Some(s) = val.as_str() {
                    result.push_str(s);
                }
            }
        }
        Ok(Some(ComparableValue::String(Arc::from(result))))
    }

    pub(super) fn eval_str_before<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 2, "STRBEFORE")?;
        let arg1 = args[0].eval_to_comparable(row, ctx)?;
        let arg2 = args[1].eval_to_comparable(row, ctx)?;
        match (arg1, arg2) {
            (Some(ComparableValue::String(s)), Some(ComparableValue::String(d))) => {
                let result = s.find(d.as_ref()).map(|pos| &s[..pos]).unwrap_or("");
                Ok(Some(ComparableValue::String(Arc::from(result))))
            }
            (None, _) | (_, None) => Ok(None),
            _ => Err(QueryError::InvalidFilter(
                "STRBEFORE requires string arguments".to_string(),
            )),
        }
    }

    pub(super) fn eval_str_after<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 2, "STRAFTER")?;
        let arg1 = args[0].eval_to_comparable(row, ctx)?;
        let arg2 = args[1].eval_to_comparable(row, ctx)?;
        match (arg1, arg2) {
            (Some(ComparableValue::String(s)), Some(ComparableValue::String(d))) => {
                let result = s
                    .find(d.as_ref())
                    .map(|pos| &s[pos + d.len()..])
                    .unwrap_or("");
                Ok(Some(ComparableValue::String(Arc::from(result))))
            }
            (None, _) | (_, None) => Ok(None),
            _ => Err(QueryError::InvalidFilter(
                "STRAFTER requires string arguments".to_string(),
            )),
        }
    }

    pub(super) fn eval_replace<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        if args.len() < 3 {
            return Err(QueryError::InvalidFilter(
                "REPLACE requires 3-4 arguments".to_string(),
            ));
        }
        let input = args[0].eval_to_comparable(row, ctx)?;
        let pattern = args[1].eval_to_comparable(row, ctx)?;
        let replacement = args[2].eval_to_comparable(row, ctx)?;
        let flags = if args.len() > 3 {
            match args[3].eval_to_comparable(row, ctx)? {
                Some(v) => v
                    .as_str()
                    .map(|s| s.to_string())
                    .ok_or_else(|| QueryError::InvalidFilter(
                        "REPLACE flags must be a string".to_string(),
                    ))?,
                None => return Ok(None),
            }
        } else {
            String::new()
        };

        match (input, pattern, replacement) {
            (
                Some(ComparableValue::String(s)),
                Some(ComparableValue::String(p)),
                Some(ComparableValue::String(r)),
            ) => {
                let re = build_regex_with_flags(&p, &flags)?;
                Ok(Some(ComparableValue::String(Arc::from(
                    re.replace_all(&s, r.as_ref()).into_owned(),
                ))))
            }
            (None, _, _) | (_, None, _) | (_, _, None) => Ok(None),
            _ => Err(QueryError::InvalidFilter(
                "REPLACE requires string arguments".to_string(),
            )),
        }
    }

    pub(super) fn eval_substr<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        if args.len() < 2 || args.len() > 3 {
            return Err(QueryError::InvalidFilter(
                "SUBSTR requires 2-3 arguments".to_string(),
            ));
        }
        let input = args[0].eval_to_comparable(row, ctx)?;
        let start = args[1].eval_to_comparable(row, ctx)?;
        let length = if args.len() > 2 {
            args[2].eval_to_comparable(row, ctx)?
        } else {
            None
        };

        let s = match input {
            Some(ComparableValue::String(s)) => s,
            None => return Ok(None),
            _ => {
                return Err(QueryError::InvalidFilter(
                    "SUBSTR requires a string as first argument".to_string(),
                ))
            }
        };

        let start_1 = match start {
            Some(ComparableValue::Long(n)) => n,
            None => return Ok(None),
            _ => {
                return Err(QueryError::InvalidFilter(
                    "SUBSTR requires an integer as second argument".to_string(),
                ))
            }
        };

        let start_0 = if start_1 < 1 {
            0
        } else {
            (start_1 - 1) as usize
        };

        if start_0 >= s.len() {
            return Ok(Some(ComparableValue::String(Arc::from(""))));
        }

        let result = match length {
            Some(ComparableValue::Long(len)) if len > 0 => {
                let end = (start_0 + (len as usize)).min(s.len());
                &s[start_0..end]
            }
            Some(ComparableValue::Long(_)) => "",
            None => &s[start_0..],
            Some(_) => {
                return Err(QueryError::InvalidFilter(
                    "SUBSTR requires an integer as third argument".to_string(),
                ))
            }
        };
        Ok(Some(ComparableValue::String(Arc::from(result))))
    }

    pub(super) fn eval_encode_for_uri<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 1, "ENCODE_FOR_URI")?;
        match args[0].eval_to_comparable(row, ctx)? {
            Some(v) => match v.as_str() {
                Some(s) => Ok(Some(ComparableValue::String(Arc::from(
                    utf8_percent_encode(s, NON_ALPHANUMERIC).to_string(),
                )))),
                None => Err(QueryError::InvalidFilter(
                    "ENCODE_FOR_URI requires a string argument".to_string(),
                )),
            },
            None => Ok(None),
        }
    }

    pub(super) fn eval_str_dt<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 2, "STRDT")?;
        let val = args[0].eval_to_comparable(row, ctx)?;
        let dt = args[1].eval_to_comparable(row, ctx)?;
        match (val, dt) {
            (Some(ComparableValue::String(s)), Some(dt_val)) => {
                Ok(Some(ComparableValue::TypedLiteral {
                    val: FlakeValue::String(s.to_string()),
                    dt_iri: dt_val.as_str().map(Arc::from),
                    lang: None,
                }))
            }
            (Some(_), Some(_)) => Err(QueryError::InvalidFilter(
                "STRDT requires a string lexical form".to_string(),
            )),
            _ => Ok(None),
        }
    }

    pub(super) fn eval_str_lang<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 2, "STRLANG")?;
        let val = args[0].eval_to_comparable(row, ctx)?;
        let lang = args[1].eval_to_comparable(row, ctx)?;
        match (val, lang) {
            (Some(ComparableValue::String(s)), Some(lang_val)) => {
                Ok(Some(ComparableValue::TypedLiteral {
                    val: FlakeValue::String(s.to_string()),
                    dt_iri: None,
                    lang: lang_val.as_str().map(Arc::from),
                }))
            }
            (Some(_), Some(_)) => Err(QueryError::InvalidFilter(
                "STRLANG requires a string lexical form".to_string(),
            )),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Batch;
    use crate::var_registry::VarId;
    use fluree_db_core::Sid;

    fn make_string_batch() -> Batch {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let col = vec![Binding::lit(
            FlakeValue::String("Hello World".to_string()),
            Sid::new(2, "string"),
        )];
        Batch::new(schema, vec![col]).unwrap()
    }

    #[test]
    fn test_strlen() {
        let batch = make_string_batch();
        let row = batch.row_view(0).unwrap();
        let result = Function::Strlen
            .eval_strlen::<fluree_db_core::MemoryStorage>(
                &[Expression::Var(VarId(0))],
                &row,
                None,
            )
            .unwrap();
        assert_eq!(result, Some(ComparableValue::Long(11)));
    }

    #[test]
    fn test_ucase() {
        let batch = make_string_batch();
        let row = batch.row_view(0).unwrap();
        let result = Function::Ucase
            .eval_ucase::<fluree_db_core::MemoryStorage>(
                &[Expression::Var(VarId(0))],
                &row,
                None,
            )
            .unwrap();
        assert_eq!(
            result,
            Some(ComparableValue::String(Arc::from("HELLO WORLD")))
        );
    }

    #[test]
    fn test_contains() {
        let batch = make_string_batch();
        let row = batch.row_view(0).unwrap();
        let result = Function::Contains
            .eval_contains::<fluree_db_core::MemoryStorage>(
                &[
                    Expression::Var(VarId(0)),
                    Expression::Const(crate::ir::FilterValue::String("World".to_string())),
                ],
                &row,
                None,
            )
            .unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }
}
