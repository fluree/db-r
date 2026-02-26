//! ASK query lowering.
//!
//! Converts SPARQL ASK queries to `ParsedQuery` with `SelectMode::Boolean`.
//! ASK tests whether a graph pattern has any solution — no variables are projected.

use crate::ast::query::AskQuery;

use fluree_db_query::options::QueryOptions;
use fluree_db_query::parse::encode::IriEncoder;
use fluree_db_query::parse::{ParsedQuery, SelectMode};

use super::{LoweringContext, Result};

impl<'a, E: IriEncoder> LoweringContext<'a, E> {
    /// Lower an ASK query to a ParsedQuery.
    pub(super) fn lower_ask(&mut self, ask: &AskQuery) -> Result<ParsedQuery> {
        // Lower WHERE clause patterns
        let patterns = self.lower_graph_pattern(&ask.where_clause.pattern)?;

        // ASK supports ORDER BY, LIMIT, OFFSET but not GROUP BY/HAVING/aggregates
        let mut options = QueryOptions::default();
        self.lower_base_modifiers(&ask.modifiers, &mut options)?;

        // LIMIT 1 for efficiency — only need to know if any solution exists
        options.limit = Some(1);

        let ctx = self.build_jsonld_context()?;

        Ok(ParsedQuery {
            context: ctx,
            orig_context: None,
            select: Vec::new(), // ASK doesn't project variables
            patterns,
            options,
            select_mode: SelectMode::Boolean,
            construct_template: None,
            graph_select: None,
        })
    }
}
