//! Graph pattern lowering.
//!
//! Converts SPARQL graph patterns (BGP, OPTIONAL, UNION, FILTER, BIND,
//! VALUES, MINUS, GRAPH, etc.) to the query engine's `Pattern` representation.

use crate::ast::expr::Expression;
use crate::ast::pattern::GraphPattern as SparqlGraphPattern;
use crate::ast::term::{Term as SparqlTerm, Var};

use fluree_db_query::binding::Binding;
use fluree_db_query::ir::{GraphName as IrGraphName, Pattern, ServiceEndpoint as IrServiceEndpoint, ServicePattern};
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::var_registry::VarId;

use super::{LowerError, LoweringContext, Result};

impl<'a, E: IriEncoder> LoweringContext<'a, E> {
    pub(super) fn lower_graph_pattern(
        &mut self,
        pattern: &SparqlGraphPattern,
    ) -> Result<Vec<Pattern>> {
        match pattern {
            SparqlGraphPattern::Bgp { patterns, .. } => self.lower_bgp_with_rdf_star(patterns),

            SparqlGraphPattern::Group { patterns, .. } => {
                let mut result = Vec::new();
                for p in patterns {
                    let lowered = self.lower_graph_pattern(p)?;
                    result.extend(lowered);
                }
                Ok(result)
            }

            SparqlGraphPattern::Optional { pattern, .. } => {
                let inner = self.lower_graph_pattern(pattern)?;
                let groups = self.split_optional_groups(inner);
                Ok(groups.into_iter().map(Pattern::Optional).collect())
            }

            SparqlGraphPattern::Union { left, right, .. } => {
                let left_patterns = self.lower_graph_pattern(left)?;
                let right_patterns = self.lower_graph_pattern(right)?;
                Ok(vec![Pattern::Union(vec![left_patterns, right_patterns])])
            }

            SparqlGraphPattern::Filter { expr, .. } => self.lower_filter_pattern(expr),

            SparqlGraphPattern::Bind { expr, var, .. } => self.lower_bind_pattern(expr, var),

            SparqlGraphPattern::Values { vars, data, .. } => self.lower_values_pattern(vars, data),

            SparqlGraphPattern::Minus { left, right, .. } => {
                // Lower left patterns first (the base patterns to match)
                let mut result = self.lower_graph_pattern(left)?;
                // Lower right patterns and wrap in MINUS
                let right_patterns = self.lower_graph_pattern(right)?;
                result.push(Pattern::Minus(right_patterns));
                Ok(result)
            }

            SparqlGraphPattern::Graph { name, pattern, .. } => {
                self.lower_named_graph_pattern(name, pattern)
            }

            SparqlGraphPattern::Service {
                silent,
                endpoint,
                pattern,
                ..
            } => self.lower_service_pattern(*silent, endpoint, pattern),

            SparqlGraphPattern::SubSelect { query, span } => self.lower_subselect(query, *span),

            SparqlGraphPattern::Path {
                subject,
                path,
                object,
                span,
            } => self.lower_property_path(subject, path, object, *span),
        }
    }

    fn split_optional_groups(&self, patterns: Vec<Pattern>) -> Vec<Vec<Pattern>> {
        let mut groups: Vec<Vec<Pattern>> = Vec::new();
        let mut current: Vec<Pattern> = Vec::new();
        let mut has_anchor = false;

        for pattern in patterns {
            let is_anchor = matches!(
                pattern,
                Pattern::Triple(_)
                    | Pattern::PropertyPath(_)
                    | Pattern::Subquery(_)
                    | Pattern::Graph { .. }
                    | Pattern::Service(_)
                    | Pattern::IndexSearch(_)
                    | Pattern::VectorSearch(_)
                    | Pattern::R2rml(_)
            );

            if is_anchor {
                if !current.is_empty() {
                    groups.push(std::mem::take(&mut current));
                }
                has_anchor = true;
                current.push(pattern);
                continue;
            }

            match pattern {
                Pattern::Filter(_) | Pattern::Bind { .. } => {
                    if !has_anchor && current.is_empty() {
                        // Allow standalone filter/bind groups if no anchor is present.
                        // This mirrors the JSON-LD optional grouping behavior.
                    }
                    current.push(pattern);
                }
                other => {
                    if current.is_empty() && !has_anchor {
                        groups.push(vec![other]);
                    } else {
                        current.push(other);
                    }
                }
            }
        }

        if !current.is_empty() {
            groups.push(current);
        }

        groups
    }

    /// Lower FILTER pattern, handling EXISTS/NOT EXISTS specially
    fn lower_filter_pattern(&mut self, expr: &Expression) -> Result<Vec<Pattern>> {
        // Handle standalone EXISTS/NOT EXISTS as patterns
        // (combined expressions like "EXISTS {...} && ?x > 5" are not supported)
        match expr {
            Expression::Exists { pattern, .. } => {
                let inner = self.lower_graph_pattern(pattern)?;
                Ok(vec![Pattern::Exists(inner)])
            }
            Expression::NotExists { pattern, .. } => {
                let inner = self.lower_graph_pattern(pattern)?;
                Ok(vec![Pattern::NotExists(inner)])
            }
            _ => {
                let filter_expr = self.lower_expression(expr)?;
                Ok(vec![Pattern::Filter(filter_expr)])
            }
        }
    }

    /// Lower BIND pattern
    fn lower_bind_pattern(&mut self, expr: &Expression, var: &Var) -> Result<Vec<Pattern>> {
        let filter_expr = self.lower_expression(expr)?;
        let var_id = self.register_var(var);
        Ok(vec![Pattern::Bind {
            var: var_id,
            expr: filter_expr,
        }])
    }

    /// Lower VALUES pattern
    fn lower_values_pattern(
        &mut self,
        vars: &[Var],
        data: &[Vec<Option<SparqlTerm>>],
    ) -> Result<Vec<Pattern>> {
        let var_ids: Vec<VarId> = vars.iter().map(|v| self.register_var(v)).collect();

        let rows: Vec<Vec<Binding>> = data
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| match cell {
                        Some(term) => self.term_to_binding(term),
                        None => Ok(Binding::Unbound),
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(vec![Pattern::Values {
            vars: var_ids,
            rows,
        }])
    }

    /// Lower GRAPH { name pattern } pattern
    fn lower_named_graph_pattern(
        &mut self,
        name: &crate::ast::pattern::GraphName,
        pattern: &SparqlGraphPattern,
    ) -> Result<Vec<Pattern>> {
        // Lower the graph name (IRI or variable)
        let ir_name = match name {
            crate::ast::pattern::GraphName::Iri(iri) => {
                let expanded = self.expand_iri(iri)?;
                // Use string, not Sid - graph names are ledger identifiers, not encoded
                IrGraphName::Iri(std::sync::Arc::from(expanded))
            }
            crate::ast::pattern::GraphName::Var(v) => {
                let var_id = self.register_var(v);
                IrGraphName::Var(var_id)
            }
        };
        // Lower the inner pattern
        let inner_patterns = self.lower_graph_pattern(pattern)?;
        Ok(vec![Pattern::Graph {
            name: ir_name,
            patterns: inner_patterns,
        }])
    }

    /// Lower SERVICE pattern
    ///
    /// SERVICE <endpoint> { ... } or SERVICE SILENT <endpoint> { ... }
    ///
    /// For local Fluree ledgers, the endpoint should be `fluree:ledger:<alias>:<branch>`
    /// or just `fluree:ledger:<alias>` (defaults to :main branch).
    fn lower_service_pattern(
        &mut self,
        silent: bool,
        endpoint: &crate::ast::pattern::ServiceEndpoint,
        pattern: &SparqlGraphPattern,
    ) -> Result<Vec<Pattern>> {
        // Lower the endpoint (IRI or variable)
        let ir_endpoint = match endpoint {
            crate::ast::pattern::ServiceEndpoint::Iri(iri) => {
                let expanded = self.expand_iri(iri)?;
                IrServiceEndpoint::Iri(std::sync::Arc::from(expanded))
            }
            crate::ast::pattern::ServiceEndpoint::Var(v) => {
                let var_id = self.register_var(v);
                IrServiceEndpoint::Var(var_id)
            }
        };

        // Lower the inner pattern
        let inner_patterns = self.lower_graph_pattern(pattern)?;

        Ok(vec![Pattern::Service(ServicePattern::new(
            silent,
            ir_endpoint,
            inner_patterns,
        ))])
    }
}
