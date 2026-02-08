//! UUID function implementations
//!
//! Implements SPARQL UUID functions: UUID, STRUUID

use crate::error::Result;
use crate::ir::{Expression, Function};
use std::sync::Arc;
use uuid::Uuid;

use super::helpers::check_arity;
use super::value::ComparableValue;

impl Function {
    pub(super) fn eval_uuid(&self, args: &[Expression]) -> Result<Option<ComparableValue>> {
        check_arity(args, 0, "UUID")?;
        Ok(Some(ComparableValue::String(Arc::from(format!(
            "urn:uuid:{}",
            Uuid::new_v4()
        )))))
    }

    pub(super) fn eval_struuid(&self, args: &[Expression]) -> Result<Option<ComparableValue>> {
        check_arity(args, 0, "STRUUID")?;
        Ok(Some(ComparableValue::String(Arc::from(
            Uuid::new_v4().to_string(),
        ))))
    }
}
