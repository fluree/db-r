//! Expression lowering.
//!
//! Converts SPARQL expressions (comparisons, arithmetic, function calls, etc.)
//! to the query engine's `FilterExpr` representation.

use crate::ast::expr::{BinaryOp, Expression, FunctionName, UnaryOp};
use crate::ast::term::{Literal, LiteralValue};
use crate::span::SourceSpan;

use fluree_db_core::FlakeValue;
use fluree_db_query::ir::{
    ArithmeticOp, CompareOp, FilterExpr, FilterValue, FunctionName as IrFunctionName,
};
use fluree_db_query::parse::encode::IriEncoder;

use super::{LowerError, LoweringContext, Result};

impl<'a, E: IriEncoder> LoweringContext<'a, E> {
    pub(super) fn lower_expression(&mut self, expr: &Expression) -> Result<FilterExpr> {
        match expr {
            Expression::Var(v) => {
                let var_id = self.register_var(v);
                Ok(FilterExpr::Var(var_id))
            }

            Expression::Literal(lit) => {
                let value = self.lower_filter_value(lit)?;
                Ok(FilterExpr::Const(value))
            }

            Expression::Iri(iri) => {
                // IRIs in expressions become string constants for now
                let full_iri = self.expand_iri(iri)?;
                Ok(FilterExpr::Const(FilterValue::String(full_iri)))
            }

            Expression::Binary {
                op, left, right, ..
            } => match op {
                BinaryOp::And => {
                    let l = self.lower_expression(left)?;
                    let r = self.lower_expression(right)?;
                    Ok(FilterExpr::And(vec![l, r]))
                }
                BinaryOp::Or => {
                    let l = self.lower_expression(left)?;
                    let r = self.lower_expression(right)?;
                    Ok(FilterExpr::Or(vec![l, r]))
                }
                BinaryOp::Eq => self.lower_comparison(CompareOp::Eq, left, right),
                BinaryOp::Ne => self.lower_comparison(CompareOp::Ne, left, right),
                BinaryOp::Lt => self.lower_comparison(CompareOp::Lt, left, right),
                BinaryOp::Le => self.lower_comparison(CompareOp::Le, left, right),
                BinaryOp::Gt => self.lower_comparison(CompareOp::Gt, left, right),
                BinaryOp::Ge => self.lower_comparison(CompareOp::Ge, left, right),
                BinaryOp::Add => self.lower_arithmetic(ArithmeticOp::Add, left, right),
                BinaryOp::Sub => self.lower_arithmetic(ArithmeticOp::Sub, left, right),
                BinaryOp::Mul => self.lower_arithmetic(ArithmeticOp::Mul, left, right),
                BinaryOp::Div => self.lower_arithmetic(ArithmeticOp::Div, left, right),
            },

            Expression::Unary { op, operand, .. } => match op {
                UnaryOp::Not => {
                    let inner = self.lower_expression(operand)?;
                    Ok(FilterExpr::Not(Box::new(inner)))
                }
                UnaryOp::Pos => self.lower_expression(operand),
                UnaryOp::Neg => {
                    let inner = self.lower_expression(operand)?;
                    Ok(FilterExpr::Negate(Box::new(inner)))
                }
            },

            Expression::FunctionCall { name, args, .. } => {
                self.lower_function_call(name, args, expr.span())
            }

            Expression::Aggregate {
                function,
                expr: agg_expr,
                distinct,
                separator,
                span,
            } => {
                if let Some(aliases) = &self.aggregate_aliases {
                    let key =
                        self.aggregate_key(function, agg_expr, *distinct, separator, *span)?;
                    if let Some(var_id) = aliases.get(&key) {
                        return Ok(FilterExpr::Var(*var_id));
                    }
                }
                Err(LowerError::not_implemented(
                    format!("Aggregate function {:?}", function),
                    *span,
                ))
            }

            Expression::Exists { span, .. } | Expression::NotExists { span, .. } => {
                // EXISTS/NOT EXISTS as part of compound expressions (e.g., EXISTS && ?x > 5)
                // is not supported. Use standalone FILTER EXISTS { ... } instead.
                Err(LowerError::not_implemented(
                    "EXISTS/NOT EXISTS in compound expressions (use standalone FILTER EXISTS)",
                    *span,
                ))
            }

            Expression::In {
                expr,
                list,
                negated,
                ..
            } => {
                let lowered_expr = self.lower_expression(expr)?;
                let lowered_values: Vec<FilterExpr> = list
                    .iter()
                    .map(|v| self.lower_expression(v))
                    .collect::<Result<Vec<_>>>()?;
                Ok(FilterExpr::In {
                    expr: Box::new(lowered_expr),
                    values: lowered_values,
                    negated: *negated,
                })
            }

            Expression::If {
                condition,
                then_expr,
                else_expr,
                ..
            } => {
                let cond = self.lower_expression(condition)?;
                let then_e = self.lower_expression(then_expr)?;
                let else_e = self.lower_expression(else_expr)?;
                Ok(FilterExpr::If {
                    condition: Box::new(cond),
                    then_expr: Box::new(then_e),
                    else_expr: Box::new(else_e),
                })
            }

            Expression::Coalesce { args, .. } => {
                let lowered_args: Vec<FilterExpr> = args
                    .iter()
                    .map(|a| self.lower_expression(a))
                    .collect::<Result<Vec<_>>>()?;
                Ok(FilterExpr::Function {
                    name: IrFunctionName::Coalesce,
                    args: lowered_args,
                })
            }

            Expression::Bracketed { inner, .. } => {
                // Bracketed expressions just unwrap to their inner expression
                self.lower_expression(inner)
            }
        }
    }

    fn lower_comparison(
        &mut self,
        op: CompareOp,
        left: &Expression,
        right: &Expression,
    ) -> Result<FilterExpr> {
        let l = self.lower_expression(left)?;
        let r = self.lower_expression(right)?;
        Ok(FilterExpr::Compare {
            op,
            left: Box::new(l),
            right: Box::new(r),
        })
    }

    fn lower_arithmetic(
        &mut self,
        op: ArithmeticOp,
        left: &Expression,
        right: &Expression,
    ) -> Result<FilterExpr> {
        let l = self.lower_expression(left)?;
        let r = self.lower_expression(right)?;
        Ok(FilterExpr::Arithmetic {
            op,
            left: Box::new(l),
            right: Box::new(r),
        })
    }

    fn lower_filter_value(&self, lit: &Literal) -> Result<FilterValue> {
        match &lit.value {
            LiteralValue::Simple(s) => Ok(FilterValue::String(s.to_string())),
            LiteralValue::LangTagged { value, .. } => Ok(FilterValue::String(value.to_string())),
            LiteralValue::Integer(i) => Ok(FilterValue::Long(*i)),
            LiteralValue::Double(d) => Ok(FilterValue::Double(*d)),
            LiteralValue::Decimal(d) => {
                let val: f64 = d
                    .parse()
                    .map_err(|_| LowerError::invalid_decimal(d.as_ref(), lit.span))?;
                Ok(FilterValue::Double(val))
            }
            LiteralValue::Boolean(b) => Ok(FilterValue::Bool(*b)),
            LiteralValue::Typed { value, datatype } => {
                let fv = self.lower_typed_literal(value, datatype)?;
                match fv {
                    FlakeValue::Long(n) => Ok(FilterValue::Long(n)),
                    FlakeValue::Double(d) => Ok(FilterValue::Double(d)),
                    FlakeValue::Boolean(b) => Ok(FilterValue::Bool(b)),
                    FlakeValue::String(s) => Ok(FilterValue::String(s)),
                    fv if fv.is_temporal() || fv.is_duration() => Ok(FilterValue::Temporal(fv)),
                    _ => Ok(FilterValue::String(value.to_string())),
                }
            }
        }
    }

    fn lower_function_call(
        &mut self,
        name: &FunctionName,
        args: &[Expression],
        span: SourceSpan,
    ) -> Result<FilterExpr> {
        let ir_name = match name {
            // Type checking functions
            FunctionName::Bound => IrFunctionName::Bound,
            FunctionName::IsIri | FunctionName::IsUri => IrFunctionName::IsIri,
            FunctionName::IsBlank => IrFunctionName::IsBlank,
            FunctionName::IsLiteral => IrFunctionName::IsLiteral,
            FunctionName::IsNumeric => IrFunctionName::IsNumeric,

            // RDF term functions
            FunctionName::Lang => IrFunctionName::Lang,
            FunctionName::Datatype => IrFunctionName::Datatype,

            // String functions
            FunctionName::Strlen => IrFunctionName::Strlen,
            FunctionName::Substr => IrFunctionName::Substr,
            FunctionName::Ucase => IrFunctionName::Ucase,
            FunctionName::Lcase => IrFunctionName::Lcase,
            FunctionName::Contains => IrFunctionName::Contains,
            FunctionName::StrStarts => IrFunctionName::StrStarts,
            FunctionName::StrEnds => IrFunctionName::StrEnds,
            FunctionName::Regex => IrFunctionName::Regex,
            FunctionName::Concat => IrFunctionName::Concat,
            FunctionName::StrBefore => IrFunctionName::StrBefore,
            FunctionName::StrAfter => IrFunctionName::StrAfter,
            FunctionName::Replace => IrFunctionName::Replace,

            // Numeric functions
            FunctionName::Abs => IrFunctionName::Abs,
            FunctionName::Round => IrFunctionName::Round,
            FunctionName::Ceil => IrFunctionName::Ceil,
            FunctionName::Floor => IrFunctionName::Floor,

            // DateTime functions
            FunctionName::Now => IrFunctionName::Now,
            FunctionName::Year => IrFunctionName::Year,
            FunctionName::Month => IrFunctionName::Month,
            FunctionName::Day => IrFunctionName::Day,
            FunctionName::Hours => IrFunctionName::Hours,
            FunctionName::Minutes => IrFunctionName::Minutes,
            FunctionName::Seconds => IrFunctionName::Seconds,
            FunctionName::Timezone | FunctionName::Tz => IrFunctionName::Tz,

            // Accessor functions
            FunctionName::Str => IrFunctionName::Str,
            FunctionName::EncodeForUri => IrFunctionName::EncodeForUri,

            // RDF term comparison
            FunctionName::LangMatches => IrFunctionName::LangMatches,
            FunctionName::SameTerm => IrFunctionName::SameTerm,

            // Hash functions
            FunctionName::Md5 => IrFunctionName::Md5,
            FunctionName::Sha1 => IrFunctionName::Sha1,
            FunctionName::Sha256 => IrFunctionName::Sha256,
            FunctionName::Sha384 => IrFunctionName::Sha384,
            FunctionName::Sha512 => IrFunctionName::Sha512,

            // UUID functions
            FunctionName::Uuid => IrFunctionName::Uuid,
            FunctionName::StrUuid => IrFunctionName::StrUuid,

            // Control flow (usually handled as special expression forms)
            FunctionName::If => IrFunctionName::If,
            FunctionName::Coalesce => IrFunctionName::Coalesce,

            // Extension functions
            FunctionName::Extension(iri) => {
                let full_iri = self.expand_iri(iri)?;
                IrFunctionName::Custom(full_iri)
            }

            // Not yet implemented
            _ => {
                return Err(LowerError::not_implemented(
                    format!("Function {:?}", name),
                    span,
                ))
            }
        };

        let lowered_args: Vec<FilterExpr> = args
            .iter()
            .map(|a| self.lower_expression(a))
            .collect::<Result<Vec<_>>>()?;

        Ok(FilterExpr::Function {
            name: ir_name,
            args: lowered_args,
        })
    }
}
