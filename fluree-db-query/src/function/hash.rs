//! Hash function implementations
//!
//! Implements SPARQL hash functions: MD5, SHA1, SHA256, SHA384, SHA512

use crate::binding::RowView;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::{Expression, FunctionName};
use fluree_db_core::Storage;
use md5::{Digest as Md5Digest, Md5};
use sha1::Sha1;
use sha2::{Sha256, Sha384, Sha512};
use std::sync::Arc;

use super::eval::eval_to_comparable_inner;
use super::helpers::check_arity;
use super::value::ComparableValue;

/// Evaluate a hash function
pub fn eval_hash_function<S: Storage>(
    name: &FunctionName,
    args: &[Expression],
    row: &RowView,
    ctx: Option<&ExecutionContext<'_, S>>,
) -> Result<Option<ComparableValue>> {
    match name {
        FunctionName::Md5 => eval_hash(args, row, ctx, "MD5", |s| {
            let mut hasher = Md5::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        }),

        FunctionName::Sha1 => eval_hash(args, row, ctx, "SHA1", |s| {
            let mut hasher = Sha1::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        }),

        FunctionName::Sha256 => eval_hash(args, row, ctx, "SHA256", |s| {
            let mut hasher = Sha256::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        }),

        FunctionName::Sha384 => eval_hash(args, row, ctx, "SHA384", |s| {
            let mut hasher = Sha384::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        }),

        FunctionName::Sha512 => eval_hash(args, row, ctx, "SHA512", |s| {
            let mut hasher = Sha512::new();
            hasher.update(s.as_bytes());
            format!("{:x}", hasher.finalize())
        }),

        _ => unreachable!("Non-hash function routed to hash module: {:?}", name),
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
    let val = eval_to_comparable_inner(&args[0], row, ctx)?;
    Ok(val.and_then(|v| {
        v.as_str()
            .map(|s| ComparableValue::String(Arc::from(hash_fn(s))))
    }))
}
