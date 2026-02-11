//! RDF term function implementations
//!
//! Implements SPARQL RDF term functions: DATATYPE, LANGMATCHES, SAMETERM, IRI, BNODE

use crate::binding::{Binding, RowAccess};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::Expression;
use fluree_db_core::Storage;
use std::sync::Arc;
use uuid::Uuid;

use super::helpers::{check_arity, format_datatype_sid};
use super::value::ComparableValue;

pub fn eval_datatype<R: RowAccess>(
    args: &[Expression],
    row: &R,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "DATATYPE")?;
    if let Expression::Var(var_id) = &args[0] {
        match row.get(*var_id) {
            Some(binding) => match binding {
                Binding::Lit { dt, .. } => Ok(Some(format_datatype_sid(dt))),
                Binding::Sid(_) | Binding::IriMatch { .. } | Binding::Iri(_) => {
                    Ok(Some(ComparableValue::String(Arc::from("@id"))))
                }
                _ => Err(QueryError::InvalidFilter(
                    "DATATYPE requires a literal or IRI argument".to_string(),
                )),
            },
            None => Ok(None), // unbound variable
        }
    } else {
        Err(QueryError::InvalidFilter(
            "DATATYPE requires a variable argument".to_string(),
        ))
    }
}

pub fn eval_lang_matches<S: Storage, R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "LANGMATCHES")?;
    let tag = args[0].eval_to_comparable(row, ctx)?;
    let range = args[1].eval_to_comparable(row, ctx)?;
    match (tag, range) {
        (Some(ComparableValue::String(t)), Some(ComparableValue::String(r))) => {
            let result = if r.as_ref() == "*" {
                !t.is_empty()
            } else {
                let t_lower = t.to_lowercase();
                let r_lower = r.to_lowercase();
                t_lower == r_lower
                    || (t_lower.starts_with(&r_lower)
                        && t_lower.chars().nth(r_lower.len()) == Some('-'))
            };
            Ok(Some(ComparableValue::Bool(result)))
        }
        (None, _) | (_, None) => Ok(None),
        _ => Err(QueryError::InvalidFilter(
            "LANGMATCHES requires string arguments".to_string(),
        )),
    }
}

pub fn eval_same_term<S: Storage, R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "SAMETERM")?;
    let v1 = args[0].eval_to_comparable(row, ctx)?;
    let v2 = args[1].eval_to_comparable(row, ctx)?;
    let same = matches!((v1, v2), (Some(a), Some(b)) if a == b);
    Ok(Some(ComparableValue::Bool(same)))
}

pub fn eval_iri<S: Storage, R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "IRI")?;
    match args[0].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::String(s)) => Ok(Some(ComparableValue::Iri(s))),
        Some(ComparableValue::Sid(sid)) => Ok(Some(ComparableValue::Sid(sid))),
        Some(_) => Err(QueryError::InvalidFilter(
            "IRI requires a string or IRI argument".to_string(),
        )),
        None => Ok(None),
    }
}

pub fn eval_bnode(args: &[Expression]) -> Result<Option<ComparableValue>> {
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
