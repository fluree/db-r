//! Subject class lookup for policy enforcement
//!
//! This module provides functions to look up the classes (rdf:type) of subjects
//! from the database. This is needed for f:onClass policy enforcement.

use fluree_db_core::{
    range_with_overlay, Db, FlakeValue, OverlayProvider, RangeMatch, RangeOptions,
    RangeTest, Sid, Storage,
};
use fluree_vocab::namespaces::RDF;
use fluree_vocab::predicates::RDF_TYPE;
use std::collections::{HashMap, HashSet};

use crate::error::PolicyError;
use crate::Result;

/// Look up the classes (rdf:type values) for a set of subjects.
///
/// This function queries the database for rdf:type flakes for each subject
/// and returns a map from subject to its class SIDs.
///
/// # Arguments
///
/// * `subjects` - The subject SIDs to look up classes for
/// * `db` - The database to query
/// * `overlay` - Optional overlay (novelty) to include staged flakes
/// * `to_t` - The transaction time to query as-of
///
/// # Returns
///
/// A map from subject SID to a vector of class SIDs. Subjects with no
/// rdf:type assertions will not be present in the map.
pub async fn lookup_subject_classes<S, O>(
    subjects: &[Sid],
    db: &Db<S>,
    overlay: &O,
    to_t: i64,
) -> Result<HashMap<Sid, Vec<Sid>>>
where
    S: Storage,
    O: OverlayProvider + ?Sized,
{
    if subjects.is_empty() {
        return Ok(HashMap::new());
    }

    // Create the rdf:type SID
    let rdf_type = Sid::new(RDF, RDF_TYPE);

    let mut result: HashMap<Sid, Vec<Sid>> = HashMap::new();

    // Query rdf:type for each unique subject
    let unique_subjects: HashSet<&Sid> = subjects.iter().collect();

    for subject in unique_subjects {
        let range_match = RangeMatch::subject_predicate(subject.clone(), rdf_type.clone());
        let opts = RangeOptions::new().with_to_t(to_t);

        let flakes = range_with_overlay(
            db,
            overlay,
            fluree_db_core::IndexType::Spot,
            RangeTest::Eq,
            range_match,
            opts,
        )
        .await
        .map_err(|e| PolicyError::ClassLookup {
            message: format!("Failed to look up classes for subject: {}", e),
        })?;

        // Extract class SIDs from rdf:type flakes
        let classes: Vec<Sid> = flakes
            .into_iter()
            .filter_map(|f| {
                if let FlakeValue::Ref(class_sid) = f.o {
                    Some(class_sid)
                } else {
                    None
                }
            })
            .collect();

        if !classes.is_empty() {
            result.insert(subject.clone(), classes);
        }
    }

    Ok(result)
}

/// Look up classes for subjects and populate the policy context's class cache.
///
/// This is a convenience function that combines lookup with cache population.
///
/// # Arguments
///
/// * `subjects` - The subject SIDs to look up classes for
/// * `db` - The database to query
/// * `overlay` - Optional overlay (novelty) to include staged flakes
/// * `to_t` - The transaction time to query as-of
/// * `policy_ctx` - The policy context whose cache to populate
pub async fn populate_class_cache<S, O>(
    subjects: &[Sid],
    db: &Db<S>,
    overlay: &O,
    to_t: i64,
    policy_ctx: &crate::evaluate::PolicyContext,
) -> Result<()>
where
    S: Storage,
    O: OverlayProvider + ?Sized,
{
    // Skip if no class policies need checking
    if !policy_ctx.wrapper().has_class_policies() {
        return Ok(());
    }

    let class_map = lookup_subject_classes(subjects, db, overlay, to_t).await?;

    for (subject, classes) in class_map {
        policy_ctx.cache_subject_classes(subject, classes);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rdf_type_sid_creation() {
        let rdf_type = Sid::new(RDF, RDF_TYPE);
        assert_eq!(rdf_type.namespace_code, RDF);
        assert_eq!(rdf_type.name.as_ref(), "type");
    }
}
