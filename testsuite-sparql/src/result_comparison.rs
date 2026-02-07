//! Isomorphic comparison of SPARQL query results.
//!
//! Handles blank node equivalence (different labels, same structure)
//! and unordered solution multiset matching for SELECT queries.

use std::collections::HashMap;

use crate::result_format::{RdfTerm, SparqlResults};

/// Compare two SPARQL result sets for isomorphism.
///
/// - **Solutions**: Same variables, same number of solutions, and each expected
///   solution has a matching actual solution (unordered) with consistent blank
///   node mapping.
/// - **Boolean**: Direct equality.
pub fn are_results_isomorphic(expected: &SparqlResults, actual: &SparqlResults) -> bool {
    match (expected, actual) {
        (
            SparqlResults::Solutions {
                variables: exp_vars,
                solutions: exp_solutions,
            },
            SparqlResults::Solutions {
                variables: act_vars,
                solutions: act_solutions,
            },
        ) => {
            // Variables must match (as sets — order doesn't matter)
            let mut exp_sorted = exp_vars.clone();
            exp_sorted.sort();
            let mut act_sorted = act_vars.clone();
            act_sorted.sort();
            if exp_sorted != act_sorted {
                return false;
            }

            // Same number of solutions
            if exp_solutions.len() != act_solutions.len() {
                return false;
            }

            // Try to find a consistent blank node mapping that matches all solutions.
            // Use a greedy approach with backtracking.
            let mut bnode_map: HashMap<String, String> = HashMap::new();
            let mut used: Vec<bool> = vec![false; act_solutions.len()];

            match_solutions(exp_solutions, act_solutions, 0, &mut bnode_map, &mut used)
        }
        (SparqlResults::Boolean(exp), SparqlResults::Boolean(act)) => exp == act,
        _ => false, // Type mismatch
    }
}

/// Recursively match expected solutions to actual solutions with backtracking.
///
/// For each expected solution, tries to find an unused actual solution that
/// matches (with consistent blank node mapping).
fn match_solutions(
    expected: &[HashMap<String, RdfTerm>],
    actual: &[HashMap<String, RdfTerm>],
    exp_idx: usize,
    bnode_map: &mut HashMap<String, String>,
    used: &mut [bool],
) -> bool {
    // All expected solutions matched successfully
    if exp_idx >= expected.len() {
        return true;
    }

    let exp_solution = &expected[exp_idx];

    for act_idx in 0..actual.len() {
        if used[act_idx] {
            continue;
        }

        // Try matching this pair
        let saved_map = bnode_map.clone();
        if solution_matches(exp_solution, &actual[act_idx], bnode_map) {
            used[act_idx] = true;
            if match_solutions(expected, actual, exp_idx + 1, bnode_map, used) {
                return true;
            }
            used[act_idx] = false;
        }
        // Backtrack: restore the bnode map
        *bnode_map = saved_map;
    }

    false
}

/// Check if two solutions match, updating the blank node mapping.
fn solution_matches(
    expected: &HashMap<String, RdfTerm>,
    actual: &HashMap<String, RdfTerm>,
    bnode_map: &mut HashMap<String, String>,
) -> bool {
    // Every binding in expected must exist and match in actual (and vice versa)
    if expected.len() != actual.len() {
        return false;
    }

    for (var, exp_term) in expected {
        match actual.get(var) {
            Some(act_term) => {
                if !terms_match(exp_term, act_term, bnode_map) {
                    return false;
                }
            }
            None => return false,
        }
    }

    true
}

/// Check if two RDF terms match, handling blank node isomorphism.
fn terms_match(
    expected: &RdfTerm,
    actual: &RdfTerm,
    bnode_map: &mut HashMap<String, String>,
) -> bool {
    match (expected, actual) {
        (RdfTerm::BlankNode(exp_label), RdfTerm::BlankNode(act_label)) => {
            // Check if we've already mapped this expected bnode
            if let Some(mapped) = bnode_map.get(exp_label) {
                mapped == act_label
            } else {
                // Check that the actual label isn't already mapped to a different expected label
                let already_mapped = bnode_map.values().any(|v| v == act_label);
                if already_mapped {
                    return false;
                }
                bnode_map.insert(exp_label.clone(), act_label.clone());
                true
            }
        }
        (
            RdfTerm::Literal {
                value: ev,
                datatype: ed,
                language: el,
            },
            RdfTerm::Literal {
                value: av,
                datatype: ad,
                language: al,
            },
        ) => ev == av && normalize_datatype(ed) == normalize_datatype(ad) && el == al,
        (RdfTerm::Iri(e), RdfTerm::Iri(a)) => e == a,
        _ => false, // Type mismatch
    }
}

/// Normalize datatype: treat `None` and `Some(xsd:string)` as equivalent.
fn normalize_datatype(dt: &Option<String>) -> Option<&str> {
    match dt.as_deref() {
        None | Some("http://www.w3.org/2001/XMLSchema#string") => None,
        Some(s) => Some(s),
    }
}

/// Format a diff between expected and actual results for error messages.
pub fn format_results_diff(expected: &SparqlResults, actual: &SparqlResults) -> String {
    match (expected, actual) {
        (
            SparqlResults::Solutions {
                variables: exp_vars,
                solutions: exp_solutions,
            },
            SparqlResults::Solutions {
                variables: act_vars,
                solutions: act_solutions,
            },
        ) => {
            let mut msg = String::new();
            msg.push_str(&format!(
                "Expected vars: {:?}\nActual vars:   {:?}\n",
                exp_vars, act_vars,
            ));
            msg.push_str(&format!(
                "Expected {} solution(s), got {}\n",
                exp_solutions.len(),
                act_solutions.len(),
            ));

            // Show first few expected vs actual solutions
            let show_count = 5;
            if !exp_solutions.is_empty() {
                msg.push_str("\nExpected (first few):\n");
                for (i, sol) in exp_solutions.iter().take(show_count).enumerate() {
                    msg.push_str(&format!("  [{i}]: {sol:?}\n"));
                }
            }
            if !act_solutions.is_empty() {
                msg.push_str("\nActual (first few):\n");
                for (i, sol) in act_solutions.iter().take(show_count).enumerate() {
                    msg.push_str(&format!("  [{i}]: {sol:?}\n"));
                }
            }
            msg
        }
        (SparqlResults::Boolean(exp), SparqlResults::Boolean(act)) => {
            format!("Expected: {exp}, Actual: {act}")
        }
        _ => format!(
            "Result type mismatch: expected {}, got {}",
            result_type_name(expected),
            result_type_name(actual),
        ),
    }
}

fn result_type_name(r: &SparqlResults) -> &'static str {
    match r {
        SparqlResults::Solutions { .. } => "Solutions",
        SparqlResults::Boolean(_) => "Boolean",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn iri(s: &str) -> RdfTerm {
        RdfTerm::Iri(s.to_string())
    }

    fn lit(s: &str) -> RdfTerm {
        RdfTerm::Literal {
            value: s.to_string(),
            datatype: None,
            language: None,
        }
    }

    fn bnode(s: &str) -> RdfTerm {
        RdfTerm::BlankNode(s.to_string())
    }

    #[test]
    fn test_identical_solutions() {
        let r1 = SparqlResults::Solutions {
            variables: vec!["x".into()],
            solutions: vec![{
                let mut m = HashMap::new();
                m.insert("x".into(), iri("http://example.org/a"));
                m
            }],
        };
        let r2 = SparqlResults::Solutions {
            variables: vec!["x".into()],
            solutions: vec![{
                let mut m = HashMap::new();
                m.insert("x".into(), iri("http://example.org/a"));
                m
            }],
        };
        assert!(are_results_isomorphic(&r1, &r2));
    }

    #[test]
    fn test_unordered_solutions() {
        let r1 = SparqlResults::Solutions {
            variables: vec!["x".into()],
            solutions: vec![
                {
                    let mut m = HashMap::new();
                    m.insert("x".into(), lit("a"));
                    m
                },
                {
                    let mut m = HashMap::new();
                    m.insert("x".into(), lit("b"));
                    m
                },
            ],
        };
        // Same solutions in reverse order
        let r2 = SparqlResults::Solutions {
            variables: vec!["x".into()],
            solutions: vec![
                {
                    let mut m = HashMap::new();
                    m.insert("x".into(), lit("b"));
                    m
                },
                {
                    let mut m = HashMap::new();
                    m.insert("x".into(), lit("a"));
                    m
                },
            ],
        };
        assert!(are_results_isomorphic(&r1, &r2));
    }

    #[test]
    fn test_blank_node_isomorphism() {
        // Expected: ?x = _:a, ?y = _:a (same bnode)
        // Actual:   ?x = _:z, ?y = _:z (same bnode, different label)
        let r1 = SparqlResults::Solutions {
            variables: vec!["x".into(), "y".into()],
            solutions: vec![{
                let mut m = HashMap::new();
                m.insert("x".into(), bnode("a"));
                m.insert("y".into(), bnode("a"));
                m
            }],
        };
        let r2 = SparqlResults::Solutions {
            variables: vec!["x".into(), "y".into()],
            solutions: vec![{
                let mut m = HashMap::new();
                m.insert("x".into(), bnode("z"));
                m.insert("y".into(), bnode("z"));
                m
            }],
        };
        assert!(are_results_isomorphic(&r1, &r2));
    }

    #[test]
    fn test_blank_node_different_structure() {
        // Expected: ?x = _:a, ?y = _:a (same bnode)
        // Actual:   ?x = _:z, ?y = _:w (different bnodes — not isomorphic)
        let r1 = SparqlResults::Solutions {
            variables: vec!["x".into(), "y".into()],
            solutions: vec![{
                let mut m = HashMap::new();
                m.insert("x".into(), bnode("a"));
                m.insert("y".into(), bnode("a"));
                m
            }],
        };
        let r2 = SparqlResults::Solutions {
            variables: vec!["x".into(), "y".into()],
            solutions: vec![{
                let mut m = HashMap::new();
                m.insert("x".into(), bnode("z"));
                m.insert("y".into(), bnode("w"));
                m
            }],
        };
        assert!(!are_results_isomorphic(&r1, &r2));
    }

    #[test]
    fn test_boolean_match() {
        assert!(are_results_isomorphic(
            &SparqlResults::Boolean(true),
            &SparqlResults::Boolean(true)
        ));
        assert!(!are_results_isomorphic(
            &SparqlResults::Boolean(true),
            &SparqlResults::Boolean(false)
        ));
    }

    #[test]
    fn test_type_mismatch() {
        assert!(!are_results_isomorphic(
            &SparqlResults::Boolean(true),
            &SparqlResults::Solutions {
                variables: vec![],
                solutions: vec![],
            }
        ));
    }

    #[test]
    fn test_literal_datatype_normalization() {
        // xsd:string and no datatype should be equivalent
        let r1 = SparqlResults::Solutions {
            variables: vec!["x".into()],
            solutions: vec![{
                let mut m = HashMap::new();
                m.insert(
                    "x".into(),
                    RdfTerm::Literal {
                        value: "hello".into(),
                        datatype: None,
                        language: None,
                    },
                );
                m
            }],
        };
        let r2 = SparqlResults::Solutions {
            variables: vec!["x".into()],
            solutions: vec![{
                let mut m = HashMap::new();
                m.insert(
                    "x".into(),
                    RdfTerm::Literal {
                        value: "hello".into(),
                        datatype: Some("http://www.w3.org/2001/XMLSchema#string".to_string()),
                        language: None,
                    },
                );
                m
            }],
        };
        assert!(are_results_isomorphic(&r1, &r2));
    }
}
