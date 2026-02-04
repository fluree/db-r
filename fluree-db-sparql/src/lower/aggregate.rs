//! Aggregate extraction and lowering.
//!
//! Handles extraction of aggregate specifications from SELECT clauses,
//! mapping SPARQL aggregate functions to engine functions, and collecting
//! aggregates referenced in HAVING conditions.

use crate::ast::expr::{AggregateFunction, Expression};
use crate::ast::query::{SelectClause, SelectVariable, SelectVariables};
use crate::span::SourceSpan;

use fluree_db_query::aggregate::{AggregateFn, AggregateSpec};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::var_registry::VarId;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::{LowerError, LoweringContext, Result};

impl<'a, E: IriEncoder> LoweringContext<'a, E> {
    pub(super) fn aggregate_key(
        &self,
        function: &AggregateFunction,
        expr: &Option<Box<Expression>>,
        distinct: bool,
        separator: &Option<Arc<str>>,
        span: SourceSpan,
    ) -> Result<String> {
        let input = match expr {
            Some(inner) => match inner.unwrap_bracketed() {
                Expression::Var(var) => format!("?{}", var.name),
                _ => {
                    return Err(LowerError::not_implemented(
                        "Aggregate with expression input",
                        span,
                    ))
                }
            },
            None => "*".to_string(),
        };
        let sep = separator.as_deref().unwrap_or("");
        Ok(format!(
            "{}|{}|{}|{}",
            function.as_str(),
            input,
            distinct,
            sep
        ))
    }

    pub(super) fn build_aggregate_aliases(
        &mut self,
        select: &SelectClause,
    ) -> Result<HashMap<String, VarId>> {
        let mut aliases = HashMap::new();

        if let SelectVariables::Explicit(vars) = &select.variables {
            for var in vars {
                if let SelectVariable::Expr { expr, alias, .. } = var {
                    if let Expression::Aggregate {
                        function,
                        expr: agg_expr,
                        distinct,
                        separator,
                        span,
                    } = expr
                    {
                        let key =
                            self.aggregate_key(function, agg_expr, *distinct, separator, *span)?;
                        let var_id = self.register_var(alias);
                        aliases.insert(key, var_id);
                    }
                }
            }
        }

        Ok(aliases)
    }

    pub(super) fn aggregate_spec_from_expr(
        &mut self,
        function: &AggregateFunction,
        expr: &Option<Box<Expression>>,
        distinct: bool,
        separator: &Option<Arc<str>>,
        span: SourceSpan,
        output_var: VarId,
    ) -> Result<AggregateSpec> {
        let (input_var, agg_fn) = match expr {
            Some(inner) => match inner.unwrap_bracketed() {
                Expression::Var(v) => {
                    let var_id = self.register_var(v);
                    let fn_kind = self.map_aggregate_function(
                        function,
                        distinct,
                        separator.as_ref().map(|s| s.as_ref()),
                    );
                    (Some(var_id), fn_kind)
                }
                _ => {
                    return Err(LowerError::not_implemented(
                        "Aggregate with expression input",
                        span,
                    ))
                }
            },
            None => (None, AggregateFn::CountAll),
        };

        Ok(AggregateSpec {
            function: agg_fn,
            input_var,
            output_var,
        })
    }

    pub(super) fn collect_having_aggregates(
        &mut self,
        expr: &Expression,
        aliases: &mut HashMap<String, VarId>,
        aggregates: &mut Vec<AggregateSpec>,
    ) -> Result<()> {
        match expr.unwrap_bracketed() {
            Expression::Aggregate {
                function,
                expr: agg_expr,
                distinct,
                separator,
                span,
            } => {
                let key = self.aggregate_key(function, agg_expr, *distinct, separator, *span)?;
                if !aliases.contains_key(&key) {
                    let output_var = self
                        .vars
                        .get_or_insert(&format!("?__having_agg_{}", aliases.len()));
                    let spec = self.aggregate_spec_from_expr(
                        function, agg_expr, *distinct, separator, *span, output_var,
                    )?;
                    aliases.insert(key, output_var);
                    aggregates.push(spec);
                }
                Ok(())
            }
            Expression::Binary { left, right, .. } => {
                self.collect_having_aggregates(left, aliases, aggregates)?;
                self.collect_having_aggregates(right, aliases, aggregates)?;
                Ok(())
            }
            Expression::Unary { operand, .. } => {
                self.collect_having_aggregates(operand, aliases, aggregates)
            }
            Expression::FunctionCall { args, .. } => {
                for arg in args {
                    self.collect_having_aggregates(arg, aliases, aggregates)?;
                }
                Ok(())
            }
            Expression::If {
                condition,
                then_expr,
                else_expr,
                ..
            } => {
                self.collect_having_aggregates(condition, aliases, aggregates)?;
                self.collect_having_aggregates(then_expr, aliases, aggregates)?;
                self.collect_having_aggregates(else_expr, aliases, aggregates)
            }
            Expression::Coalesce { args, .. } => {
                for arg in args {
                    self.collect_having_aggregates(arg, aliases, aggregates)?;
                }
                Ok(())
            }
            Expression::In { expr, list, .. } => {
                self.collect_having_aggregates(expr, aliases, aggregates)?;
                for arg in list {
                    self.collect_having_aggregates(arg, aliases, aggregates)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub(super) fn expr_references_vars(&self, expr: &Expression, vars: &HashSet<Arc<str>>) -> bool {
        match expr.unwrap_bracketed() {
            Expression::Var(var) => vars.contains(&var.name),
            Expression::Literal(_) | Expression::Iri(_) => false,
            Expression::Unary { operand, .. } => self.expr_references_vars(operand, vars),
            Expression::Binary { left, right, .. } => {
                self.expr_references_vars(left, vars) || self.expr_references_vars(right, vars)
            }
            Expression::FunctionCall { args, .. } => {
                args.iter().any(|a| self.expr_references_vars(a, vars))
            }
            Expression::If {
                condition,
                then_expr,
                else_expr,
                ..
            } => {
                self.expr_references_vars(condition, vars)
                    || self.expr_references_vars(then_expr, vars)
                    || self.expr_references_vars(else_expr, vars)
            }
            Expression::Coalesce { args, .. } => {
                args.iter().any(|a| self.expr_references_vars(a, vars))
            }
            Expression::In { expr, list, .. } => {
                self.expr_references_vars(expr, vars)
                    || list.iter().any(|a| self.expr_references_vars(a, vars))
            }
            Expression::Exists { .. }
            | Expression::NotExists { .. }
            | Expression::Aggregate { .. } => false,
            Expression::Bracketed { inner, .. } => self.expr_references_vars(inner, vars),
        }
    }

    /// Extract aggregate specifications from SELECT clause.
    ///
    /// Walks the SELECT variables looking for aggregate expressions like:
    ///   SELECT (COUNT(?x) AS ?count) ...
    ///
    /// Returns AggregateSpecs for each aggregate found.
    pub(super) fn extract_aggregates(
        &mut self,
        select: &SelectClause,
    ) -> Result<Vec<AggregateSpec>> {
        let mut aggregates = Vec::new();

        if let SelectVariables::Explicit(vars) = &select.variables {
            for var in vars {
                if let SelectVariable::Expr { expr, alias, .. } = var {
                    if let Expression::Aggregate {
                        function,
                        expr: agg_expr,
                        distinct,
                        separator,
                        span,
                    } = expr
                    {
                        let output_var = self.register_var(alias);
                        let spec = self.aggregate_spec_from_expr(
                            function, agg_expr, *distinct, separator, *span, output_var,
                        )?;
                        aggregates.push(spec);
                    }
                }
            }
        }

        Ok(aggregates)
    }

    /// Map SPARQL AggregateFunction to engine AggregateFn.
    fn map_aggregate_function(
        &self,
        function: &AggregateFunction,
        distinct: bool,
        separator: Option<&str>,
    ) -> AggregateFn {
        match function {
            AggregateFunction::Count => {
                if distinct {
                    AggregateFn::CountDistinct
                } else {
                    AggregateFn::Count
                }
            }
            AggregateFunction::Sum => AggregateFn::Sum,
            AggregateFunction::Avg => AggregateFn::Avg,
            AggregateFunction::Min => AggregateFn::Min,
            AggregateFunction::Max => AggregateFn::Max,
            AggregateFunction::GroupConcat => AggregateFn::GroupConcat {
                separator: separator.unwrap_or(" ").to_string(),
            },
            AggregateFunction::Sample => AggregateFn::Sample,
        }
    }

    /// Collect non-aggregate SELECT variables for implicit GROUP BY.
    ///
    /// When a query has aggregates but no explicit GROUP BY, SPARQL requires
    /// all non-aggregated variables in SELECT to be grouped.
    pub(super) fn collect_non_aggregate_select_vars(
        &mut self,
        select: &SelectClause,
    ) -> Vec<VarId> {
        let mut group_vars = Vec::new();

        if let SelectVariables::Explicit(vars) = &select.variables {
            for var in vars {
                match var {
                    SelectVariable::Var(v) => {
                        // Plain variables are non-aggregate
                        let var_id = self.register_var(v);
                        group_vars.push(var_id);
                    }
                    SelectVariable::Expr { expr, .. } => {
                        // Skip aggregate expressions - they don't go in GROUP BY
                        if !matches!(expr, Expression::Aggregate { .. }) {
                            // For non-aggregate expressions with alias, we could
                            // add the alias var, but for MVP this is a complex case
                            // that requires BIND semantics - skip for now
                        }
                    }
                }
            }
        }

        group_vars
    }
}
