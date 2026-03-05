//! RDF term function implementations
//!
//! Implements SPARQL RDF term functions: DATATYPE, LANGMATCHES, SAMETERM, IRI, BNODE

use crate::binding::{Binding, RowAccess};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::Expression;
use fluree_db_core::DatatypeDictId;
use std::sync::Arc;
use uuid::Uuid;

use super::helpers::{check_arity, format_datatype_sid};
use super::value::ComparableValue;

#[inline]
fn format_reserved_datatype(dt_id: DatatypeDictId) -> Option<ComparableValue> {
    // Matches the compact rendering produced by `format_datatype_sid()` for common XSD/RDF types.
    // For reserved dt_ids, we can answer without consulting the datatype dictionary.
    let s: &'static str = match dt_id {
        DatatypeDictId::ID => "@id",
        DatatypeDictId::STRING => "xsd:string",
        DatatypeDictId::BOOLEAN => "xsd:boolean",
        DatatypeDictId::INTEGER => "xsd:integer",
        DatatypeDictId::LONG => "xsd:long",
        DatatypeDictId::DECIMAL => "xsd:decimal",
        DatatypeDictId::DOUBLE => "xsd:double",
        DatatypeDictId::FLOAT => "xsd:float",
        DatatypeDictId::DATE_TIME => "xsd:dateTime",
        DatatypeDictId::DATE => "xsd:date",
        DatatypeDictId::TIME => "xsd:time",
        DatatypeDictId::LANG_STRING => "rdf:langString",
        DatatypeDictId::JSON => "@json",
        DatatypeDictId::VECTOR => "@vector",
        DatatypeDictId::FULL_TEXT => "@fulltext",
        _ => return None,
    };
    Some(ComparableValue::String(Arc::from(s)))
}

pub fn eval_datatype<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "DATATYPE")?;
    if let Expression::Var(var_id) = &args[0] {
        match row.get(*var_id) {
            Some(binding) => match binding {
                Binding::Lit { dtc, .. } => Ok(Some(format_datatype_sid(dtc.datatype()))),
                Binding::EncodedLit { dt_id, .. } => {
                    let dt_id = DatatypeDictId::from_u16(*dt_id);
                    if let Some(v) = format_reserved_datatype(dt_id) {
                        return Ok(Some(v));
                    }

                    let Some(store) = ctx.and_then(|c| c.binary_store.as_deref()) else {
                        return Err(QueryError::InvalidExpression(
                            "DATATYPE requires a literal or IRI argument".to_string(),
                        ));
                    };
                    let dt_sid = store
                        .dt_sids()
                        .get(dt_id.as_u16() as usize)
                        .cloned()
                        .ok_or_else(|| {
                            QueryError::InvalidExpression(format!(
                                "DATATYPE could not resolve datatype id {}",
                                dt_id.as_u16()
                            ))
                        })?;
                    Ok(Some(format_datatype_sid(&dt_sid)))
                }
                Binding::Sid(_) | Binding::IriMatch { .. } | Binding::Iri(_) => {
                    Ok(Some(ComparableValue::String(Arc::from("@id"))))
                }
                Binding::Unbound | Binding::Poisoned => Ok(None),
                _ => Err(QueryError::InvalidExpression(
                    "DATATYPE requires a literal or IRI argument".to_string(),
                )),
            },
            None => Ok(None), // unbound variable
        }
    } else {
        Err(QueryError::InvalidExpression(
            "DATATYPE requires a variable argument".to_string(),
        ))
    }
}

pub fn eval_lang_matches<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
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
        _ => Err(QueryError::InvalidExpression(
            "LANGMATCHES requires string arguments".to_string(),
        )),
    }
}

pub fn eval_same_term<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 2, "SAMETERM")?;
    let v1 = args[0].eval_to_comparable(row, ctx)?;
    let v2 = args[1].eval_to_comparable(row, ctx)?;
    let same = matches!((v1, v2), (Some(a), Some(b)) if a == b);
    Ok(Some(ComparableValue::Bool(same)))
}

pub fn eval_iri<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    check_arity(args, 1, "IRI")?;
    match args[0].eval_to_comparable(row, ctx)? {
        Some(ComparableValue::String(s)) => Ok(Some(ComparableValue::Iri(s))),
        Some(ComparableValue::Iri(iri)) => Ok(Some(ComparableValue::Iri(iri))),
        Some(ComparableValue::Sid(sid)) => Ok(Some(ComparableValue::Sid(sid))),
        Some(_) => Ok(None),
        None => Ok(None),
    }
}

pub fn eval_bnode<R: RowAccess>(
    args: &[Expression],
    row: &R,
    ctx: Option<&ExecutionContext<'_>>,
) -> Result<Option<ComparableValue>> {
    match args.len() {
        0 => {
            // No args: generate a fresh blank node
            Ok(Some(ComparableValue::Iri(Arc::from(format!(
                "_:fdb-{}",
                Uuid::new_v4()
            )))))
        }
        1 => {
            // Label arg: deterministic blank node for the same label within a query
            match args[0].eval_to_comparable(row, ctx)? {
                Some(v) => match v.as_str() {
                    Some(label) => {
                        use std::collections::hash_map::DefaultHasher;
                        use std::hash::{Hash, Hasher};
                        let mut hasher = DefaultHasher::new();
                        label.hash(&mut hasher);
                        let hash = hasher.finish();
                        Ok(Some(ComparableValue::Iri(Arc::from(format!(
                            "_:b{:x}",
                            hash
                        )))))
                    }
                    None => Ok(None),
                },
                None => Ok(None),
            }
        }
        _ => Err(QueryError::InvalidExpression(
            "BNODE requires 0 or 1 arguments".to_string(),
        )),
    }
}
