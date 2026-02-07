//! Common filter parsing utilities
//!
//! Shared utilities for parsing filter expressions from both:
//! - S-expressions: `(> ?age 18)`
//! - Data expressions: `[">", "?age", 18]`
//!
//! Provides generic helpers to reduce duplication between parsing formats.

use super::ast::{UnresolvedArithmeticOp, UnresolvedCompareOp, UnresolvedExpression};
use super::error::{ParseError, Result};

/// Validate argument count and return error if it doesn't match expected count
///
/// # Example
///
/// ```
/// use fluree_db_query::parse::filter_common::validate_arg_count;
///
/// assert!(validate_arg_count(&[1, 2], 2, "comparison operator").is_ok());
/// assert!(validate_arg_count(&[1], 2, "comparison operator").is_err());
/// ```
pub fn validate_arg_count<T>(args: &[T], expected: usize, context: &str) -> Result<()> {
    if args.len() != expected {
        return Err(ParseError::InvalidFilter(format!(
            "{} requires exactly {} argument{}, got {}",
            context,
            expected,
            if expected == 1 { "" } else { "s" },
            args.len()
        )));
    }
    Ok(())
}

/// Validate minimum argument count
pub fn validate_min_arg_count<T>(args: &[T], min: usize, context: &str) -> Result<()> {
    if args.len() < min {
        return Err(ParseError::InvalidFilter(format!(
            "{} requires at least {} argument{}, got {}",
            context,
            min,
            if min == 1 { "" } else { "s" },
            args.len()
        )));
    }
    Ok(())
}

/// Build a binary comparison expression from left and right operands
///
/// Generic over the input type `T` so it works with both:
/// - `&UnresolvedExpression` (already parsed, for S-expressions)
/// - `&JsonValue` (needs parsing, for data expressions)
///
/// The `parser` function converts `T` to `UnresolvedExpression`.
pub fn build_binary_compare<T, F>(
    args: &[T],
    op: UnresolvedCompareOp,
    parser: F,
    context: &str,
) -> Result<UnresolvedExpression>
where
    F: Fn(&T) -> Result<UnresolvedExpression>,
{
    validate_arg_count(args, 2, context)?;

    let left = parser(&args[0])?;
    let right = parser(&args[1])?;

    Ok(UnresolvedExpression::Compare {
        op,
        left: Box::new(left),
        right: Box::new(right),
    })
}

/// Build a binary arithmetic expression from left and right operands
///
/// Generic over the input type `T` so it works with both:
/// - `&UnresolvedExpression` (already parsed, for S-expressions)
/// - `&JsonValue` (needs parsing, for data expressions)
///
/// The `parser` function converts `T` to `UnresolvedExpression`.
pub fn build_binary_arithmetic<T, F>(
    args: &[T],
    op: UnresolvedArithmeticOp,
    parser: F,
    context: &str,
) -> Result<UnresolvedExpression>
where
    F: Fn(&T) -> Result<UnresolvedExpression>,
{
    validate_arg_count(args, 2, context)?;

    let left = parser(&args[0])?;
    let right = parser(&args[1])?;

    Ok(UnresolvedExpression::Arithmetic {
        op,
        left: Box::new(left),
        right: Box::new(right),
    })
}

/// Build a logical AND expression from a list of sub-expressions
pub fn build_and<T, F>(args: &[T], parser: F) -> Result<UnresolvedExpression>
where
    F: Fn(&T) -> Result<UnresolvedExpression>,
{
    validate_min_arg_count(args, 1, "'and'")?;

    let exprs: Result<Vec<_>> = args.iter().map(parser).collect();
    Ok(UnresolvedExpression::And(exprs?))
}

/// Build a logical OR expression from a list of sub-expressions
pub fn build_or<T, F>(args: &[T], parser: F) -> Result<UnresolvedExpression>
where
    F: Fn(&T) -> Result<UnresolvedExpression>,
{
    validate_min_arg_count(args, 1, "'or'")?;

    let exprs: Result<Vec<_>> = args.iter().map(parser).collect();
    Ok(UnresolvedExpression::Or(exprs?))
}

/// Build a logical NOT expression from a single sub-expression
pub fn build_not<T, F>(args: &[T], parser: F) -> Result<UnresolvedExpression>
where
    F: Fn(&T) -> Result<UnresolvedExpression>,
{
    validate_arg_count(args, 1, "'not'")?;

    let expr = parser(&args[0])?;
    Ok(UnresolvedExpression::Not(Box::new(expr)))
}

/// Build a unary negation expression (arithmetic negation: -x)
pub fn build_negate<T, F>(args: &[T], parser: F) -> Result<UnresolvedExpression>
where
    F: Fn(&T) -> Result<UnresolvedExpression>,
{
    validate_arg_count(args, 1, "unary negation")?;

    let expr = parser(&args[0])?;
    Ok(UnresolvedExpression::Negate(Box::new(expr)))
}

/// Map operator name string to comparison operator enum
///
/// Handles multiple aliases:
/// - `=`, `eq` → Eq
/// - `!=`, `<>`, `ne` → Ne
/// - `<`, `lt` → Lt
/// - `<=`, `le` → Le
/// - `>`, `gt` → Gt
/// - `>=`, `ge` → Ge
pub fn parse_compare_op(op: &str) -> Option<UnresolvedCompareOp> {
    match op.to_lowercase().as_str() {
        "=" | "eq" => Some(UnresolvedCompareOp::Eq),
        "!=" | "<>" | "ne" => Some(UnresolvedCompareOp::Ne),
        "<" | "lt" => Some(UnresolvedCompareOp::Lt),
        "<=" | "le" => Some(UnresolvedCompareOp::Le),
        ">" | "gt" => Some(UnresolvedCompareOp::Gt),
        ">=" | "ge" => Some(UnresolvedCompareOp::Ge),
        _ => None,
    }
}

/// Map operator name string to arithmetic operator enum
///
/// Handles multiple aliases:
/// - `+`, `add` → Add
/// - `-`, `sub` → Sub
/// - `*`, `mul` → Mul
/// - `/`, `div` → Div
pub fn parse_arithmetic_op(op: &str) -> Option<UnresolvedArithmeticOp> {
    match op.to_lowercase().as_str() {
        "+" | "add" => Some(UnresolvedArithmeticOp::Add),
        "-" | "sub" => Some(UnresolvedArithmeticOp::Sub),
        "*" | "mul" => Some(UnresolvedArithmeticOp::Mul),
        "/" | "div" => Some(UnresolvedArithmeticOp::Div),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_arg_count() {
        let args = vec![1, 2];
        assert!(validate_arg_count(&args, 2, "test").is_ok());
        assert!(validate_arg_count(&args, 3, "test").is_err());
        assert!(validate_arg_count(&args, 1, "test").is_err());
    }

    #[test]
    fn test_validate_min_arg_count() {
        let args = vec![1, 2, 3];
        assert!(validate_min_arg_count(&args, 1, "test").is_ok());
        assert!(validate_min_arg_count(&args, 3, "test").is_ok());
        assert!(validate_min_arg_count(&args, 4, "test").is_err());
    }

    #[test]
    fn test_parse_compare_op() {
        assert_eq!(parse_compare_op("="), Some(UnresolvedCompareOp::Eq));
        assert_eq!(parse_compare_op("eq"), Some(UnresolvedCompareOp::Eq));
        assert_eq!(parse_compare_op("EQ"), Some(UnresolvedCompareOp::Eq));

        assert_eq!(parse_compare_op("!="), Some(UnresolvedCompareOp::Ne));
        assert_eq!(parse_compare_op("<>"), Some(UnresolvedCompareOp::Ne));
        assert_eq!(parse_compare_op("ne"), Some(UnresolvedCompareOp::Ne));

        assert_eq!(parse_compare_op("<"), Some(UnresolvedCompareOp::Lt));
        assert_eq!(parse_compare_op("lt"), Some(UnresolvedCompareOp::Lt));

        assert_eq!(parse_compare_op("<="), Some(UnresolvedCompareOp::Le));
        assert_eq!(parse_compare_op("le"), Some(UnresolvedCompareOp::Le));

        assert_eq!(parse_compare_op(">"), Some(UnresolvedCompareOp::Gt));
        assert_eq!(parse_compare_op("gt"), Some(UnresolvedCompareOp::Gt));

        assert_eq!(parse_compare_op(">="), Some(UnresolvedCompareOp::Ge));
        assert_eq!(parse_compare_op("ge"), Some(UnresolvedCompareOp::Ge));

        assert_eq!(parse_compare_op("unknown"), None);
    }

    #[test]
    fn test_parse_arithmetic_op() {
        assert_eq!(parse_arithmetic_op("+"), Some(UnresolvedArithmeticOp::Add));
        assert_eq!(
            parse_arithmetic_op("add"),
            Some(UnresolvedArithmeticOp::Add)
        );
        assert_eq!(
            parse_arithmetic_op("ADD"),
            Some(UnresolvedArithmeticOp::Add)
        );

        assert_eq!(parse_arithmetic_op("-"), Some(UnresolvedArithmeticOp::Sub));
        assert_eq!(
            parse_arithmetic_op("sub"),
            Some(UnresolvedArithmeticOp::Sub)
        );

        assert_eq!(parse_arithmetic_op("*"), Some(UnresolvedArithmeticOp::Mul));
        assert_eq!(
            parse_arithmetic_op("mul"),
            Some(UnresolvedArithmeticOp::Mul)
        );

        assert_eq!(parse_arithmetic_op("/"), Some(UnresolvedArithmeticOp::Div));
        assert_eq!(
            parse_arithmetic_op("div"),
            Some(UnresolvedArithmeticOp::Div)
        );

        assert_eq!(parse_arithmetic_op("unknown"), None);
    }

    #[test]
    fn test_build_binary_compare() {
        // Test with a simple identity parser
        let args = vec![1, 2];
        let parser =
            |x: &i32| -> Result<UnresolvedExpression> { Ok(UnresolvedExpression::long(*x as i64)) };

        let expr = build_binary_compare(&args, UnresolvedCompareOp::Eq, parser, "test comparison")
            .unwrap();

        match expr {
            UnresolvedExpression::Compare { op, left, right } => {
                assert_eq!(op, UnresolvedCompareOp::Eq);
                assert!(matches!(*left, UnresolvedExpression::Const(_)));
                assert!(matches!(*right, UnresolvedExpression::Const(_)));
            }
            _ => panic!("Expected Compare expression"),
        }
    }

    #[test]
    fn test_build_and() {
        let args = vec![true, false];
        let parser =
            |x: &bool| -> Result<UnresolvedExpression> { Ok(UnresolvedExpression::boolean(*x)) };

        let expr = build_and(&args, parser).unwrap();

        match expr {
            UnresolvedExpression::And(exprs) => {
                assert_eq!(exprs.len(), 2);
            }
            _ => panic!("Expected And expression"),
        }
    }

    #[test]
    fn test_build_not() {
        let args = vec![true];
        let parser =
            |x: &bool| -> Result<UnresolvedExpression> { Ok(UnresolvedExpression::boolean(*x)) };

        let expr = build_not(&args, parser).unwrap();

        match expr {
            UnresolvedExpression::Not(inner) => {
                assert!(matches!(*inner, UnresolvedExpression::Const(_)));
            }
            _ => panic!("Expected Not expression"),
        }
    }
}
