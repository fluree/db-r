//! SPARQL function evaluation
//!
//! This module provides unified function evaluation with a single entry point
//! that handles both value-returning and boolean contexts.
//!
//! Key functions:
//! - `eval_function()` - evaluate function to ComparableValue (unified entry)
//! - `eval_function_to_bool()` - evaluate function in boolean context (uses EBV)

use crate::binding::{Binding, RowView};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{FilterExpr, FunctionName};
use chrono::{DateTime, Datelike, FixedOffset, SecondsFormat, Timelike, Utc};
use fluree_db_core::temporal::DateTime as FlureeDateTime;
use fluree_db_core::{FlakeValue, Storage};
use md5::{Digest as Md5Digest, Md5};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use rand::random;
use sha1::Sha1;
use sha2::{Sha256, Sha384, Sha512};
use std::sync::Arc;
use uuid::Uuid;

use super::eval::{eval_to_comparable_inner, evaluate_inner};
use super::helpers::{
    build_regex_with_flags, check_arity, format_datatype_sid, parse_datetime_from_binding,
};
use super::value::{comparable_to_str_value, comparable_to_string, ComparableValue};

// =============================================================================
// Unified Function Evaluation
// =============================================================================

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
        // =================================================================
        // String functions (context-aware for EncodedLit)
        // =================================================================
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

        // =================================================================
        // Numeric functions
        // =================================================================
        FunctionName::Abs => {
            check_arity(args, 1, "ABS")?;
            let val = eval_to_comparable_inner(&args[0], row, ctx)?;
            Ok(match val {
                Some(ComparableValue::Long(n)) => Some(ComparableValue::Long(n.abs())),
                Some(ComparableValue::Double(d)) => Some(ComparableValue::Double(d.abs())),
                _ => None,
            })
        }

        FunctionName::Round => {
            check_arity(args, 1, "ROUND")?;
            let val = eval_to_comparable_inner(&args[0], row, ctx)?;
            Ok(match val {
                Some(ComparableValue::Long(n)) => Some(ComparableValue::Long(n)),
                Some(ComparableValue::Double(d)) => Some(ComparableValue::Double(d.round())),
                _ => None,
            })
        }

        FunctionName::Ceil => {
            check_arity(args, 1, "CEIL")?;
            let val = eval_to_comparable_inner(&args[0], row, ctx)?;
            Ok(match val {
                Some(ComparableValue::Long(n)) => Some(ComparableValue::Long(n)),
                Some(ComparableValue::Double(d)) => Some(ComparableValue::Double(d.ceil())),
                _ => None,
            })
        }

        FunctionName::Floor => {
            check_arity(args, 1, "FLOOR")?;
            let val = eval_to_comparable_inner(&args[0], row, ctx)?;
            Ok(match val {
                Some(ComparableValue::Long(n)) => Some(ComparableValue::Long(n)),
                Some(ComparableValue::Double(d)) => Some(ComparableValue::Double(d.floor())),
                _ => None,
            })
        }

        FunctionName::Rand => {
            check_arity(args, 0, "RAND")?;
            Ok(Some(ComparableValue::Double(random::<f64>())))
        }

        // =================================================================
        // Boolean/type-checking functions
        // =================================================================
        FunctionName::Bound => {
            check_arity(args, 1, "BOUND")?;
            match &args[0] {
                FilterExpr::Var(var) => Ok(Some(ComparableValue::Bool(!matches!(
                    row.get(*var),
                    Some(Binding::Unbound) | Some(Binding::Poisoned) | None
                )))),
                _ => Err(QueryError::InvalidFilter(
                    "BOUND argument must be a variable".to_string(),
                )),
            }
        }

        FunctionName::IsIri => {
            check_arity(args, 1, "isIRI")?;
            let val = eval_to_comparable_inner(&args[0], row, ctx)?;
            Ok(Some(ComparableValue::Bool(val.is_some_and(|v| {
                matches!(v, ComparableValue::Sid(_) | ComparableValue::Iri(_))
            }))))
        }

        FunctionName::IsLiteral => {
            check_arity(args, 1, "isLiteral")?;
            let val = eval_to_comparable_inner(&args[0], row, ctx)?;
            Ok(Some(ComparableValue::Bool(val.is_some_and(|v| {
                matches!(
                    v,
                    ComparableValue::Long(_)
                        | ComparableValue::Double(_)
                        | ComparableValue::String(_)
                        | ComparableValue::Bool(_)
                )
            }))))
        }

        FunctionName::IsNumeric => {
            check_arity(args, 1, "isNumeric")?;
            let val = eval_to_comparable_inner(&args[0], row, ctx)?;
            Ok(Some(ComparableValue::Bool(val.is_some_and(|v| {
                matches!(v, ComparableValue::Long(_) | ComparableValue::Double(_))
            }))))
        }

        FunctionName::IsBlank => Ok(Some(ComparableValue::Bool(false))),

        // =================================================================
        // Conditional functions
        // =================================================================
        FunctionName::If => {
            check_arity(args, 3, "IF")?;
            let cond = evaluate_inner(&args[0], row, ctx)?;
            if cond {
                eval_to_comparable_inner(&args[1], row, ctx)
            } else {
                eval_to_comparable_inner(&args[2], row, ctx)
            }
        }

        FunctionName::Coalesce => {
            for arg in args {
                let val = eval_to_comparable_inner(arg, row, ctx)?;
                if val.is_some() {
                    return Ok(val);
                }
            }
            Ok(None)
        }

        // =================================================================
        // DateTime functions
        // =================================================================
        FunctionName::Now => {
            let now = Utc::now();
            let formatted = now.to_rfc3339_opts(SecondsFormat::Millis, true);
            let parsed = FlureeDateTime::parse(&formatted)
                .map_err(|e| QueryError::InvalidFilter(format!("now parse error: {}", e)))?;
            Ok(Some(ComparableValue::DateTime(Box::new(parsed))))
        }

        FunctionName::Year => eval_datetime_component(args, row, "YEAR", |dt| dt.year() as i64),
        FunctionName::Month => eval_datetime_component(args, row, "MONTH", |dt| dt.month() as i64),
        FunctionName::Day => eval_datetime_component(args, row, "DAY", |dt| dt.day() as i64),
        FunctionName::Hours => eval_datetime_component(args, row, "HOURS", |dt| dt.hour() as i64),
        FunctionName::Minutes => {
            eval_datetime_component(args, row, "MINUTES", |dt| dt.minute() as i64)
        }
        FunctionName::Seconds => {
            eval_datetime_component(args, row, "SECONDS", |dt| dt.second() as i64)
        }

        FunctionName::Tz => {
            check_arity(args, 1, "TZ")?;
            if let FilterExpr::Var(var_id) = &args[0] {
                if let Some(binding) = row.get(*var_id) {
                    if let Some(dt) = parse_datetime_from_binding(binding) {
                        let offset = dt.offset();
                        let total_secs = offset.local_minus_utc();
                        let hours = total_secs / 3600;
                        let mins = (total_secs.abs() % 3600) / 60;
                        let sign = if total_secs >= 0 { '+' } else { '-' };
                        let tz_str = format!("{}{:02}:{:02}", sign, hours.abs(), mins);
                        return Ok(Some(ComparableValue::String(Arc::from(tz_str))));
                    }
                }
            }
            Ok(None)
        }

        // =================================================================
        // RDF term functions
        // =================================================================
        FunctionName::Datatype => {
            check_arity(args, 1, "DATATYPE")?;
            if let FilterExpr::Var(var_id) = &args[0] {
                if let Some(binding) = row.get(*var_id) {
                    return Ok(match binding {
                        Binding::Lit { dt, .. } => Some(format_datatype_sid(dt)),
                        Binding::Sid(_) | Binding::IriMatch { .. } | Binding::Iri(_) => {
                            Some(ComparableValue::String(Arc::from("@id")))
                        }
                        _ => None,
                    });
                }
            }
            Ok(None)
        }

        FunctionName::T => {
            check_arity(args, 1, "T")?;
            if let FilterExpr::Var(var_id) = &args[0] {
                if let Some(binding) = row.get(*var_id) {
                    match binding {
                        Binding::Lit { t: Some(t), .. } => {
                            return Ok(Some(ComparableValue::Long(*t)));
                        }
                        // Late-materialized binary bindings still carry `t` directly.
                        Binding::EncodedLit { t, .. } => {
                            return Ok(Some(ComparableValue::Long(*t)));
                        }
                        _ => {}
                    }
                }
            }
            Ok(None)
        }

        FunctionName::Op => {
            check_arity(args, 1, "OP")?;
            if let FilterExpr::Var(var_id) = &args[0] {
                if let Some(Binding::Lit { op: Some(op), .. }) = row.get(*var_id) {
                    let op_str = if *op { "assert" } else { "retract" };
                    return Ok(Some(ComparableValue::String(Arc::from(op_str))));
                }
            }
            Ok(None)
        }

        FunctionName::LangMatches => {
            check_arity(args, 2, "LANGMATCHES")?;
            let tag = eval_to_comparable_inner(&args[0], row, ctx)?;
            let range = eval_to_comparable_inner(&args[1], row, ctx)?;
            let result = match (tag, range) {
                (Some(ComparableValue::String(t)), Some(ComparableValue::String(r))) => {
                    if r.as_ref() == "*" {
                        !t.is_empty()
                    } else {
                        let t_lower = t.to_lowercase();
                        let r_lower = r.to_lowercase();
                        t_lower == r_lower
                            || (t_lower.starts_with(&r_lower)
                                && t_lower.chars().nth(r_lower.len()) == Some('-'))
                    }
                }
                _ => false,
            };
            Ok(Some(ComparableValue::Bool(result)))
        }

        FunctionName::SameTerm => {
            check_arity(args, 2, "SAMETERM")?;
            let v1 = eval_to_comparable_inner(&args[0], row, ctx)?;
            let v2 = eval_to_comparable_inner(&args[1], row, ctx)?;
            let same = matches!((v1, v2), (Some(a), Some(b)) if a == b);
            Ok(Some(ComparableValue::Bool(same)))
        }

        // =================================================================
        // Hash functions
        // =================================================================
        FunctionName::Md5 => eval_hash_function(args, row, ctx, "MD5", |s| {
            let mut hasher = Md5::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        }),

        FunctionName::Sha1 => eval_hash_function(args, row, ctx, "SHA1", |s| {
            let mut hasher = Sha1::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        }),

        FunctionName::Sha256 => eval_hash_function(args, row, ctx, "SHA256", |s| {
            let mut hasher = Sha256::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        }),

        FunctionName::Sha384 => eval_hash_function(args, row, ctx, "SHA384", |s| {
            let mut hasher = Sha384::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        }),

        FunctionName::Sha512 => eval_hash_function(args, row, ctx, "SHA512", |s| {
            let mut hasher = Sha512::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        }),

        // =================================================================
        // UUID functions
        // =================================================================
        FunctionName::Uuid => Ok(Some(ComparableValue::String(Arc::from(format!(
            "urn:uuid:{}",
            Uuid::new_v4()
        ))))),

        FunctionName::StrUuid => Ok(Some(ComparableValue::String(Arc::from(
            Uuid::new_v4().to_string(),
        )))),

        // =================================================================
        // Vector functions
        // =================================================================
        FunctionName::DotProduct => eval_binary_vector_fn(args, row, ctx, "dotProduct", |a, b| {
            Some(a.iter().zip(b.iter()).map(|(x, y)| x * y).sum())
        }),

        FunctionName::CosineSimilarity => {
            eval_binary_vector_fn(args, row, ctx, "cosineSimilarity", |a, b| {
                let dot: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                let mag_a: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
                let mag_b: f64 = b.iter().map(|x| x * x).sum::<f64>().sqrt();
                if mag_a == 0.0 || mag_b == 0.0 {
                    None
                } else {
                    Some(dot / (mag_a * mag_b))
                }
            })
        }

        FunctionName::EuclideanDistance => {
            eval_binary_vector_fn(args, row, ctx, "euclideanDistance", |a, b| {
                let sum_sq: f64 = a
                    .iter()
                    .zip(b.iter())
                    .map(|(x, y)| {
                        let diff = x - y;
                        diff * diff
                    })
                    .sum();
                Some(sum_sq.sqrt())
            })
        }

        // =================================================================
        // Typed literal construction
        // =================================================================
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

        FunctionName::Iri => {
            check_arity(args, 1, "IRI")?;
            match eval_to_comparable_inner(&args[0], row, ctx)? {
                Some(ComparableValue::String(s)) => Ok(Some(ComparableValue::Iri(s))),
                Some(ComparableValue::Sid(sid)) => Ok(Some(ComparableValue::Sid(sid))),
                Some(_) => Err(QueryError::InvalidFilter(
                    "IRI requires a string or IRI argument".to_string(),
                )),
                None => Ok(None),
            }
        }

        FunctionName::Bnode => {
            if !args.is_empty() {
                return Err(QueryError::InvalidFilter(
                    "BNODE requires no arguments".to_string(),
                ));
            }
            Ok(Some(ComparableValue::Iri(Arc::from(format!(
                "_:fdb-{}",
                Uuid::new_v4()
            )))))
        }

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

// =============================================================================
// Helper functions
// =============================================================================

/// Extract a datetime component from a binding
fn eval_datetime_component<F>(
    args: &[FilterExpr],
    row: &RowView,
    fn_name: &str,
    extract: F,
) -> Result<Option<ComparableValue>>
where
    F: Fn(&DateTime<FixedOffset>) -> i64,
{
    check_arity(args, 1, fn_name)?;
    if let FilterExpr::Var(var) = &args[0] {
        if let Some(binding) = row.get(*var) {
            if let Some(dt) = parse_datetime_from_binding(binding) {
                return Ok(Some(ComparableValue::Long(extract(&dt))));
            }
        }
    }
    Ok(None)
}

/// Evaluate a hash function
fn eval_hash_function<S: Storage, F>(
    args: &[FilterExpr],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
    fn_name: &str,
    hash_fn: F,
) -> Result<Option<ComparableValue>>
where
    F: Fn(&str) -> String,
{
    check_arity(args, 1, fn_name)?;
    let val = eval_to_comparable_inner(&args[0], row, ctx)?;
    Ok(val.and_then(|v| {
        comparable_to_string(&v).map(|s| ComparableValue::String(Arc::from(hash_fn(s))))
    }))
}

/// Evaluate a binary vector function
fn eval_binary_vector_fn<S: Storage, F>(
    args: &[FilterExpr],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
    fn_name: &str,
    compute: F,
) -> Result<Option<ComparableValue>>
where
    F: Fn(&[f64], &[f64]) -> Option<f64>,
{
    check_arity(args, 2, fn_name)?;
    let v1 = eval_to_comparable_inner(&args[0], row, ctx)?;
    let v2 = eval_to_comparable_inner(&args[1], row, ctx)?;
    match (v1, v2) {
        (Some(ComparableValue::Vector(a)), Some(ComparableValue::Vector(b))) => {
            if a.len() != b.len() {
                Ok(None)
            } else {
                Ok(compute(&a, &b).map(ComparableValue::Double))
            }
        }
        _ => Ok(None),
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
        let result = eval_function::<fluree_db_core::MemoryStorage>(
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
        let result = eval_function::<fluree_db_core::MemoryStorage>(
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
        let result = eval_function::<fluree_db_core::MemoryStorage>(
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

    #[test]
    fn test_bound() {
        let batch = make_string_batch();
        let row = batch.row_view(0).unwrap();
        let result = eval_function::<fluree_db_core::MemoryStorage>(
            &FunctionName::Bound,
            &[FilterExpr::Var(VarId(0))],
            &row,
            None,
        )
        .unwrap();
        assert_eq!(result, Some(ComparableValue::Bool(true)));
    }

    #[test]
    fn test_ebv_numeric() {
        let batch = Batch::new(
            Arc::from(vec![VarId(0)].into_boxed_slice()),
            vec![vec![Binding::lit(FlakeValue::Long(0), Sid::new(2, "long"))]],
        )
        .unwrap();
        let row = batch.row_view(0).unwrap();

        // STRLEN returns 0 for empty string, which should be falsy
        let result = eval_function_to_bool::<fluree_db_core::MemoryStorage>(
            &FunctionName::Abs,
            &[FilterExpr::Var(VarId(0))],
            &row,
            None,
        )
        .unwrap();
        assert!(!result); // 0 is falsy
    }
}
