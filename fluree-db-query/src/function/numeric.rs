//! Numeric function implementations
//!
//! Implements SPARQL numeric functions: ABS, ROUND, CEIL, FLOOR, RAND

use crate::binding::RowView;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::{Expression, Function};
use fluree_db_core::Storage;
use rand::random;

use super::helpers::check_arity;
use super::value::ComparableValue;

impl Function {
    pub(super) fn eval_abs<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 1, "ABS")?;
        let val = args[0].eval_to_comparable(row, ctx)?;
        Ok(match val {
            Some(ComparableValue::Long(n)) => Some(ComparableValue::Long(n.abs())),
            Some(ComparableValue::Double(d)) => Some(ComparableValue::Double(d.abs())),
            _ => None,
        })
    }

    pub(super) fn eval_round<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 1, "ROUND")?;
        let val = args[0].eval_to_comparable(row, ctx)?;
        Ok(match val {
            Some(ComparableValue::Long(n)) => Some(ComparableValue::Long(n)),
            Some(ComparableValue::Double(d)) => Some(ComparableValue::Double(d.round())),
            _ => None,
        })
    }

    pub(super) fn eval_ceil<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 1, "CEIL")?;
        let val = args[0].eval_to_comparable(row, ctx)?;
        Ok(match val {
            Some(ComparableValue::Long(n)) => Some(ComparableValue::Long(n)),
            Some(ComparableValue::Double(d)) => Some(ComparableValue::Double(d.ceil())),
            _ => None,
        })
    }

    pub(super) fn eval_floor<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 1, "FLOOR")?;
        let val = args[0].eval_to_comparable(row, ctx)?;
        Ok(match val {
            Some(ComparableValue::Long(n)) => Some(ComparableValue::Long(n)),
            Some(ComparableValue::Double(d)) => Some(ComparableValue::Double(d.floor())),
            _ => None,
        })
    }

    pub(super) fn eval_rand(
        &self,
        args: &[Expression],
    ) -> Result<Option<ComparableValue>> {
        check_arity(args, 0, "RAND")?;
        Ok(Some(ComparableValue::Double(random::<f64>())))
    }
}
