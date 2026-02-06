//! UUID function implementations
//!
//! Implements SPARQL UUID functions: UUID, STRUUID

use crate::binding::RowView;
use crate::error::Result;
use crate::ir::{FilterExpr, FunctionName};
use std::sync::Arc;
use uuid::Uuid;

use super::value::ComparableValue;

/// Evaluate a UUID function
pub fn eval_uuid_function(
    name: &FunctionName,
    _args: &[FilterExpr],
    _row: &RowView,
) -> Result<Option<ComparableValue>> {
    match name {
        FunctionName::Uuid => Ok(Some(ComparableValue::String(Arc::from(format!(
            "urn:uuid:{}",
            Uuid::new_v4()
        ))))),

        FunctionName::StrUuid => Ok(Some(ComparableValue::String(Arc::from(
            Uuid::new_v4().to_string(),
        )))),

        _ => unreachable!("Non-UUID function routed to uuid module: {:?}", name),
    }
}
