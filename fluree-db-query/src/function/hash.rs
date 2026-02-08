//! Hash function implementations
//!
//! Implements SPARQL hash functions: MD5, SHA1, SHA256, SHA384, SHA512

use crate::binding::RowView;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::{Expression, Function};
use fluree_db_core::Storage;
use md5::{Digest as Md5Digest, Md5};
use sha1::Sha1;
use sha2::{Sha256, Sha384, Sha512};
use std::sync::Arc;

use super::helpers::check_arity;
use super::value::ComparableValue;

impl Function {
    pub(super) fn eval_md5<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        eval_hash(args, row, ctx, "MD5", |s| {
            let mut hasher = Md5::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        })
    }

    pub(super) fn eval_sha1<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        eval_hash(args, row, ctx, "SHA1", |s| {
            let mut hasher = Sha1::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        })
    }

    pub(super) fn eval_sha256<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        eval_hash(args, row, ctx, "SHA256", |s| {
            let mut hasher = Sha256::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        })
    }

    pub(super) fn eval_sha384<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        eval_hash(args, row, ctx, "SHA384", |s| {
            let mut hasher = Sha384::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        })
    }

    pub(super) fn eval_sha512<S: Storage>(
        &self,
        args: &[Expression],
        row: &RowView,
        ctx: Option<&ExecutionContext<'_, S>>,
    ) -> Result<Option<ComparableValue>> {
        eval_hash(args, row, ctx, "SHA512", |s| {
            let mut hasher = Sha512::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        })
    }
}

/// Evaluate a hash function with the given hasher
fn eval_hash<S: Storage, F>(
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
    fn_name: &str,
    hash_fn: F,
) -> Result<Option<ComparableValue>>
where
    F: Fn(&str) -> String,
{
    check_arity(args, 1, fn_name)?;
    let val = args[0].eval_to_comparable(row, ctx)?;
    Ok(val.and_then(|v| {
        v.as_str()
            .map(|s| ComparableValue::String(Arc::from(hash_fn(s))))
    }))
}
