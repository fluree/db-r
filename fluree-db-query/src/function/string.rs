//! String function implementations
//!
//! Implements SPARQL string functions: STR, LANG, LCASE, UCASE, STRLEN,
//! CONTAINS, STRSTARTS, STRENDS, REGEX, CONCAT, STRBEFORE, STRAFTER,
//! REPLACE, SUBSTR, ENCODE_FOR_URI, STRDT, STRLANG

use crate::binding::{Binding, RowView};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{FilterExpr, FunctionName};
use fluree_db_core::{FlakeValue, Storage};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use std::sync::Arc;

use super::eval::eval_to_comparable_inner;
use super::helpers::{build_regex_with_flags, check_arity};
use super::value::{comparable_to_str_value, comparable_to_string, ComparableValue};

/// Evaluate a string function
pub fn eval_string_function<S: Storage>(
    name: &FunctionName,
    args: &[FilterExpr],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    match name {
        FunctionName::Str => {
            check_arity(args, 1, "STR")?;
            let val = eval_to_comparable_inner(&args[0], row, ctx)?;
            Ok(val.and_then(comparable_to_str_value))
        }

        FunctionName::Lang => {
            check_arity(args, 1, "LANG")?;
            let tag = match &args[0] {
                FilterExpr::Var(var_id) => match row.get(*var_id) {
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

        FunctionName::Lcase => {
            check_arity(args, 1, "LCASE")?;
            let val = eval_to_comparable_inner(&args[0], row, ctx)?;
            Ok(val.and_then(|v| {
                comparable_to_string(&v)
                    .map(|s| ComparableValue::String(Arc::from(s.to_lowercase())))
            }))
        }

        FunctionName::Ucase => {
            check_arity(args, 1, "UCASE")?;
            let val = eval_to_comparable_inner(&args[0], row, ctx)?;
            Ok(val.and_then(|v| {
                comparable_to_string(&v)
                    .map(|s| ComparableValue::String(Arc::from(s.to_uppercase())))
            }))
        }

        FunctionName::Strlen => {
            check_arity(args, 1, "STRLEN")?;
            let val = eval_to_comparable_inner(&args[0], row, ctx)?;
            Ok(val.and_then(|v| {
                comparable_to_string(&v).map(|s| ComparableValue::Long(s.len() as i64))
            }))
        }

        FunctionName::Contains => {
            check_arity(args, 2, "CONTAINS")?;
            let haystack = eval_to_comparable_inner(&args[0], row, ctx)?;
            let needle = eval_to_comparable_inner(&args[1], row, ctx)?;
            Ok(Some(ComparableValue::Bool(match (haystack, needle) {
                (Some(ComparableValue::String(h)), Some(ComparableValue::String(n))) => {
                    h.contains(n.as_ref())
                }
                _ => false,
            })))
        }

        FunctionName::StrStarts => {
            check_arity(args, 2, "STRSTARTS")?;
            let haystack = eval_to_comparable_inner(&args[0], row, ctx)?;
            let prefix = eval_to_comparable_inner(&args[1], row, ctx)?;
            Ok(Some(ComparableValue::Bool(match (haystack, prefix) {
                (Some(ComparableValue::String(h)), Some(ComparableValue::String(p))) => {
                    h.starts_with(p.as_ref())
                }
                _ => false,
            })))
        }

        FunctionName::StrEnds => {
            check_arity(args, 2, "STRENDS")?;
            let haystack = eval_to_comparable_inner(&args[0], row, ctx)?;
            let suffix = eval_to_comparable_inner(&args[1], row, ctx)?;
            Ok(Some(ComparableValue::Bool(match (haystack, suffix) {
                (Some(ComparableValue::String(h)), Some(ComparableValue::String(s))) => {
                    h.ends_with(s.as_ref())
                }
                _ => false,
            })))
        }

        FunctionName::Regex => {
            if args.len() < 2 {
                return Err(QueryError::InvalidFilter(
                    "REGEX requires 2-3 arguments".to_string(),
                ));
            }
            let text = eval_to_comparable_inner(&args[0], row, ctx)?;
            let pattern = eval_to_comparable_inner(&args[1], row, ctx)?;
            let flags = if args.len() > 2 {
                eval_to_comparable_inner(&args[2], row, ctx)?
                    .and_then(|v| comparable_to_string(&v).map(|s| s.to_string()))
                    .unwrap_or_default()
            } else {
                String::new()
            };

            match (text, pattern) {
                (Some(ComparableValue::String(t)), Some(ComparableValue::String(p))) => {
                    let re = build_regex_with_flags(&p, &flags)?;
                    Ok(Some(ComparableValue::Bool(re.is_match(&t))))
                }
                _ => Ok(Some(ComparableValue::Bool(false))),
            }
        }

        FunctionName::Concat => {
            let mut result = String::new();
            for arg in args {
                if let Some(val) = eval_to_comparable_inner(arg, row, ctx)? {
                    if let Some(s) = comparable_to_string(&val) {
                        result.push_str(s);
                    }
                }
            }
            Ok(Some(ComparableValue::String(Arc::from(result))))
        }

        FunctionName::StrBefore => {
            check_arity(args, 2, "STRBEFORE")?;
            let arg1 = eval_to_comparable_inner(&args[0], row, ctx)?;
            let arg2 = eval_to_comparable_inner(&args[1], row, ctx)?;
            Ok(match (arg1, arg2) {
                (Some(ComparableValue::String(s)), Some(ComparableValue::String(d))) => {
                    let result = s.find(d.as_ref()).map(|pos| &s[..pos]).unwrap_or("");
                    Some(ComparableValue::String(Arc::from(result)))
                }
                _ => None,
            })
        }

        FunctionName::StrAfter => {
            check_arity(args, 2, "STRAFTER")?;
            let arg1 = eval_to_comparable_inner(&args[0], row, ctx)?;
            let arg2 = eval_to_comparable_inner(&args[1], row, ctx)?;
            Ok(match (arg1, arg2) {
                (Some(ComparableValue::String(s)), Some(ComparableValue::String(d))) => {
                    let result = s
                        .find(d.as_ref())
                        .map(|pos| &s[pos + d.len()..])
                        .unwrap_or("");
                    Some(ComparableValue::String(Arc::from(result)))
                }
                _ => None,
            })
        }

        FunctionName::Replace => {
            if args.len() < 3 {
                return Err(QueryError::InvalidFilter(
                    "REPLACE requires 3-4 arguments".to_string(),
                ));
            }
            let input = eval_to_comparable_inner(&args[0], row, ctx)?;
            let pattern = eval_to_comparable_inner(&args[1], row, ctx)?;
            let replacement = eval_to_comparable_inner(&args[2], row, ctx)?;
            let flags = if args.len() > 3 {
                eval_to_comparable_inner(&args[3], row, ctx)?
                    .and_then(|v| comparable_to_string(&v).map(|s| s.to_string()))
                    .unwrap_or_default()
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
                _ => Ok(None),
            }
        }

        FunctionName::Substr => {
            if args.len() < 2 || args.len() > 3 {
                return Err(QueryError::InvalidFilter(
                    "SUBSTR requires 2-3 arguments".to_string(),
                ));
            }
            let input = eval_to_comparable_inner(&args[0], row, ctx)?;
            let start = eval_to_comparable_inner(&args[1], row, ctx)?;
            let length = if args.len() > 2 {
                eval_to_comparable_inner(&args[2], row, ctx)?
            } else {
                None
            };

            match (input, start) {
                (Some(ComparableValue::String(s)), Some(ComparableValue::Long(start_1))) => {
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
                        _ => return Ok(None),
                    };
                    Ok(Some(ComparableValue::String(Arc::from(result))))
                }
                _ => Ok(None),
            }
        }

        FunctionName::EncodeForUri => {
            check_arity(args, 1, "ENCODE_FOR_URI")?;
            let val = eval_to_comparable_inner(&args[0], row, ctx)?;
            Ok(val.and_then(|v| {
                comparable_to_string(&v).map(|s| {
                    ComparableValue::String(Arc::from(
                        utf8_percent_encode(s, NON_ALPHANUMERIC).to_string(),
                    ))
                })
            }))
        }

        FunctionName::StrDt => {
            check_arity(args, 2, "STRDT")?;
            let val = eval_to_comparable_inner(&args[0], row, ctx)?;
            let dt = eval_to_comparable_inner(&args[1], row, ctx)?;
            match (val, dt) {
                (Some(ComparableValue::String(s)), Some(dt_val)) => {
                    Ok(Some(ComparableValue::TypedLiteral {
                        val: FlakeValue::String(s.to_string()),
                        dt_iri: comparable_to_string(&dt_val).map(Arc::from),
                        lang: None,
                    }))
                }
                (Some(_), Some(_)) => Err(QueryError::InvalidFilter(
                    "STRDT requires a string lexical form".to_string(),
                )),
                _ => Ok(None),
            }
        }

        FunctionName::StrLang => {
            check_arity(args, 2, "STRLANG")?;
            let val = eval_to_comparable_inner(&args[0], row, ctx)?;
            let lang = eval_to_comparable_inner(&args[1], row, ctx)?;
            match (val, lang) {
                (Some(ComparableValue::String(s)), Some(lang_val)) => {
                    Ok(Some(ComparableValue::TypedLiteral {
                        val: FlakeValue::String(s.to_string()),
                        dt_iri: None,
                        lang: comparable_to_string(&lang_val).map(Arc::from),
                    }))
                }
                (Some(_), Some(_)) => Err(QueryError::InvalidFilter(
                    "STRLANG requires a string lexical form".to_string(),
                )),
                _ => Ok(None),
            }
        }

        _ => unreachable!("Non-string function routed to string module: {:?}", name),
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
        let result = eval_string_function::<fluree_db_core::MemoryStorage>(
            &FunctionName::Strlen,
            &[FilterExpr::Var(VarId(0))],
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
        let result = eval_string_function::<fluree_db_core::MemoryStorage>(
            &FunctionName::Ucase,
            &[FilterExpr::Var(VarId(0))],
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
        let result = eval_string_function::<fluree_db_core::MemoryStorage>(
            &FunctionName::Contains,
            &[
                FilterExpr::Var(VarId(0)),
                FilterExpr::Const(crate::ir::FilterValue::String("World".to_string())),
            ],
            &row,
            None,
        )
        .unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }
}
