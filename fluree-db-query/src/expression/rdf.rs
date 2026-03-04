//! RDF term function implementations
//!
//! Implements SPARQL RDF term functions: DATATYPE, LANGMATCHES, SAMETERM, IRI, BNODE

use crate::binding::{Binding, RowAccess};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::Expression;
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
        _ => Err(QueryError::InvalidFilter(
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
        Some(ComparableValue::String(s)) => {
            // Try to resolve the IRI string to a Sid using the execution context.
            // This is critical for FILTER comparisons like `?type = ex:Reptile`
            // where the variable binding is a Sid but the constant IRI would
            // otherwise become ComparableValue::Iri — an incomparable type pair.
            //
            // Use encode_iri_strict so unknown namespaces stay as IRI strings
            // rather than silently mapping to the EMPTY namespace (code 0).
            if let Some(sid) = ctx.and_then(|c| c.encode_iri_strict(&s)) {
                Ok(Some(ComparableValue::Sid(sid)))
            } else {
                Ok(Some(ComparableValue::Iri(s)))
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Batch;
    use crate::ir::FilterValue;
    use crate::var_registry::VarId;
    use fluree_db_core::Sid;

    #[test]
    fn eval_iri_string_without_context_returns_iri() {
        // Without an ExecutionContext, IRI() on a string returns ComparableValue::Iri
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let col = vec![Binding::Iri(Arc::from("http://example.org/ns/Reptile"))];
        let batch = Batch::new(schema, vec![col]).unwrap();
        let row = batch.row_view(0).unwrap();

        // Evaluate IRI(?x) where ?x is an IRI string
        let args = [Expression::Const(FilterValue::String(
            "http://unknown.org/ns/Foo".to_string(),
        ))];
        let result = eval_iri(&args, &row, None).unwrap();
        assert!(
            matches!(result, Some(ComparableValue::Iri(_))),
            "IRI of unknown namespace without context should return Iri, got: {result:?}"
        );
    }

    #[test]
    fn eval_iri_sid_passthrough() {
        // IRI() of a Sid should return the Sid unchanged
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let sid = Sid::new(100, "x");
        let col = vec![Binding::Sid(sid.clone())];
        let batch = Batch::new(schema, vec![col]).unwrap();
        let row = batch.row_view(0).unwrap();

        let args = [Expression::Var(VarId(0))];
        let result = eval_iri(&args, &row, None).unwrap();
        assert_eq!(result, Some(ComparableValue::Sid(sid)));
    }

    #[test]
    fn eval_iri_none_returns_none() {
        // IRI() of an unbound var returns None
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let col = vec![Binding::Unbound];
        let batch = Batch::new(schema, vec![col]).unwrap();
        let row = batch.row_view(0).unwrap();

        let args = [Expression::Var(VarId(0))];
        let result = eval_iri(&args, &row, None).unwrap();
        assert_eq!(result, None);
    }
}
