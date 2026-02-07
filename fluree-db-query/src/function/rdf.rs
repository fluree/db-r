//! RDF term function implementations
//!
//! Implements SPARQL RDF term functions: DATATYPE, LANGMATCHES, SAMETERM, IRI, BNODE

use crate::binding::{Binding, RowView};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{Expression, FunctionName};
use fluree_db_core::Storage;
use std::sync::Arc;
use uuid::Uuid;

use super::eval::eval_to_comparable_inner;
use super::helpers::{check_arity, format_datatype_sid};
use super::value::ComparableValue;

/// Evaluate an RDF term function
pub fn eval_rdf_function<S: Storage>(
    name: &FunctionName,
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    match name {
        FunctionName::Datatype => {
            check_arity(args, 1, "DATATYPE")?;
            if let Expression::Var(var_id) = &args[0] {
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

        _ => unreachable!("Non-RDF function routed to rdf module: {:?}", name),
    }
}
