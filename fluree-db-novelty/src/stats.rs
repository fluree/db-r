//! Stats computation from novelty
//!
//! Provides `current_stats()` which merges indexed stats with novelty updates.
//! Used for policy enforcement where `f:onClass` policies require current stats.
//!
//! # What Gets Updated
//!
//! - Property counts (total assertions per predicate)
//! - Class counts (total instances per class)
//! - Class->property list: properties used by instances
//!
//! # What Gets Preserved
//!
//! - NDV estimates (HLL sketches - expensive to update)
//! - Selectivity metrics

use crate::Novelty;
use fluree_db_core::comparator::IndexType;
use fluree_db_core::is_rdf_type;
use fluree_db_core::{ClassPropertyUsage, ClassStatEntry, IndexStats, PropertyStatEntry};
use fluree_db_core::{FlakeValue, Sid};
use std::collections::{HashMap, HashSet};

/// Compute current stats by merging indexed stats with novelty updates.
///
/// This function produces an up-to-date view of statistics by applying
/// novelty flakes (both assertions and retractions) to the indexed stats.
///
/// # Arguments
///
/// * `indexed` - Base stats from the last index operation
/// * `novelty` - Uncommitted flakes to merge
///
/// # Returns
///
/// Updated `IndexStats` with:
/// - Property counts updated from novelty
/// - Class counts updated from novelty
/// - Class->property details updated from novelty
/// - NDV/selectivity preserved from indexed stats
///
/// If novelty is empty, returns a clone of the indexed stats.
pub fn current_stats(indexed: &IndexStats, novelty: &Novelty) -> IndexStats {
    if novelty.is_empty() {
        return indexed.clone();
    }

    // Build a mutable representation for updates
    let mut property_counts = build_property_counts(indexed);
    let mut class_data = build_class_data(indexed);

    // First pass: collect rdf:type flakes to build subject->classes mapping
    let subject_classes = build_subject_class_map(novelty, &mut class_data);

    // Second pass: update property counts and class->property list
    for flake_id in novelty.iter_index(IndexType::Post) {
        let flake = novelty.get_flake(flake_id);
        let delta = if flake.op { 1i64 } else { -1i64 };

        // Update property counts
        let sid_key = (flake.p.namespace_code, flake.p.name.to_string());
        let prop_count = property_counts.entry(sid_key).or_insert(0);
        *prop_count += delta;

        // Skip rdf:type for class-property list (already handled)
        if is_rdf_type(&flake.p) {
            continue;
        }

        // Update class->property list for subjects with known classes
        if let Some(classes) = subject_classes.get(&flake.s) {
            for class_sid in classes {
                let class = class_data.entry(class_sid.clone()).or_default();
                let prop = class.properties.entry(flake.p.clone()).or_default();
                // Track presence (conservative superset)
                prop.count_delta += delta;
            }
        }
    }

    // Convert back to IndexStats format
    finalize_stats(indexed, property_counts, class_data)
}

/// Property count by (namespace_code, name)
type PropertyCountMap = HashMap<(u16, String), i64>;

/// Build property counts from indexed stats
fn build_property_counts(indexed: &IndexStats) -> PropertyCountMap {
    let mut counts = HashMap::new();
    if let Some(ref props) = indexed.properties {
        for entry in props {
            counts.insert(entry.sid.clone(), entry.count as i64);
        }
    }
    counts
}

/// Internal mutable class data for stats computation
#[derive(Debug, Default)]
struct ClassDataMut {
    count_delta: i64,
    properties: HashMap<Sid, PropertyDataMut>,
}

/// Internal mutable property data for stats computation
#[derive(Debug, Default)]
struct PropertyDataMut {
    /// Count of asserted flakes for this (class, property) pair (delta).
    ///
    /// We intentionally do NOT track datatype/ref/lang breakdowns here; detailed
    /// property stats live in graph-scoped stats (`IndexStats.graphs`).
    count_delta: i64,
}

/// Build class data from indexed stats
fn build_class_data(indexed: &IndexStats) -> HashMap<Sid, ClassDataMut> {
    let mut class_data = HashMap::new();
    if let Some(ref classes) = indexed.classes {
        for entry in classes {
            let mut props = HashMap::new();
            for prop_usage in &entry.properties {
                // Prior stats only carry the property identity. Treat presence as 1 so we
                // preserve the property list, but do not attempt to “undo” it on retractions.
                let prop_data = PropertyDataMut { count_delta: 1 };
                props.insert(prop_usage.property_sid.clone(), prop_data);
            }
            let data = ClassDataMut {
                count_delta: entry.count as i64,
                properties: props,
            };
            class_data.insert(entry.class_sid.clone(), data);
        }
    }
    class_data
}

/// Build subject->classes map from novelty rdf:type flakes
///
/// Also updates class counts in `class_data`.
fn build_subject_class_map(
    novelty: &Novelty,
    class_data: &mut HashMap<Sid, ClassDataMut>,
) -> HashMap<Sid, HashSet<Sid>> {
    let mut subject_classes: HashMap<Sid, HashSet<Sid>> = HashMap::new();

    for flake_id in novelty.iter_index(IndexType::Post) {
        let flake = novelty.get_flake(flake_id);

        if !is_rdf_type(&flake.p) {
            continue;
        }

        if let FlakeValue::Ref(class_sid) = &flake.o {
            let classes = subject_classes.entry(flake.s.clone()).or_default();

            if flake.op {
                // Assertion: subject is instance of class
                classes.insert(class_sid.clone());
                let data = class_data.entry(class_sid.clone()).or_default();
                data.count_delta += 1;
            } else {
                // Retraction: subject is no longer instance of class
                classes.remove(class_sid);
                if let Some(data) = class_data.get_mut(class_sid) {
                    data.count_delta -= 1;
                }
            }
        }
    }

    subject_classes
}

/// Convert mutable stats back to IndexStats format
fn finalize_stats(
    indexed: &IndexStats,
    property_counts: PropertyCountMap,
    class_data: HashMap<Sid, ClassDataMut>,
) -> IndexStats {
    // Convert property counts, preserving NDV from indexed
    let properties = if property_counts.is_empty() {
        indexed.properties.clone()
    } else {
        // Build lookup for indexed property data (for NDV preservation)
        let indexed_props: HashMap<(u16, String), &PropertyStatEntry> = indexed
            .properties
            .as_ref()
            .map(|props| props.iter().map(|p| (p.sid.clone(), p)).collect())
            .unwrap_or_default();

        // Sort by SID for determinism
        let mut entries: Vec<_> = property_counts.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let props: Vec<PropertyStatEntry> = entries
            .into_iter()
            .filter(|(_, count)| *count > 0)
            .map(|(sid, count)| {
                let indexed_entry = indexed_props.get(&sid);
                PropertyStatEntry {
                    sid,
                    count: count.max(0) as u64,
                    // Preserve NDV from indexed stats (expensive to recompute)
                    ndv_values: indexed_entry.map(|e| e.ndv_values).unwrap_or(0),
                    ndv_subjects: indexed_entry.map(|e| e.ndv_subjects).unwrap_or(0),
                    last_modified_t: indexed_entry.map(|e| e.last_modified_t).unwrap_or(0),
                    // Preserve datatype breakdown from indexed stats when available.
                    datatypes: indexed_entry
                        .map(|e| e.datatypes.clone())
                        .unwrap_or_default(),
                }
            })
            .collect();

        if props.is_empty() {
            None
        } else {
            Some(props)
        }
    };

    // Convert class data
    let classes = if class_data.is_empty() {
        indexed.classes.clone()
    } else {
        // Sort by class SID for determinism
        let mut entries: Vec<_> = class_data.into_iter().collect();
        entries.sort_by(|a, b| sid_cmp(&a.0, &b.0));

        let class_entries: Vec<ClassStatEntry> = entries
            .into_iter()
            .filter(|(_, data)| data.count_delta > 0)
            .map(|(class_sid, data)| {
                // Sort properties by SID
                let mut prop_entries: Vec<_> = data.properties.into_iter().collect();
                prop_entries.sort_by(|a, b| sid_cmp(&a.0, &b.0));

                let properties: Vec<ClassPropertyUsage> = prop_entries
                    .into_iter()
                    .filter(|(_, prop)| prop.count_delta > 0)
                    .map(|(property_sid, _prop)| ClassPropertyUsage { property_sid })
                    .collect();

                ClassStatEntry {
                    class_sid,
                    count: data.count_delta.max(0) as u64,
                    properties,
                }
            })
            .collect();

        if class_entries.is_empty() {
            None
        } else {
            Some(class_entries)
        }
    };

    IndexStats {
        flakes: indexed.flakes,
        size: indexed.size,
        properties,
        classes,
        // Graph stats (ID-keyed) are preserved from indexed stats unchanged.
        // The novelty layer operates on Sid-keyed data and lacks the Sid-to-p_id
        // mapping (lives in GlobalDicts in the indexer) needed to update the
        // numeric-ID-keyed graph entries. The flat `properties` field above
        // carries the novelty-adjusted Sid-keyed counts for the query planner.
        // Full novelty adjustment of graph stats will be possible once Flake
        // carries a graph field and the ID mapping is available here.
        graphs: indexed.graphs.clone(),
    }
}

/// Compare two SIDs for sorting
fn sid_cmp(a: &Sid, b: &Sid) -> std::cmp::Ordering {
    match a.namespace_code.cmp(&b.namespace_code) {
        std::cmp::Ordering::Equal => a.name.cmp(&b.name),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Novelty;
    use fluree_db_core::{Flake, FlakeMeta, Sid};

    fn make_sid(ns: u16, name: &str) -> Sid {
        Sid::new(ns, name)
    }

    fn make_type_flake(subject: Sid, class: Sid, t: i64, op: bool) -> Flake {
        Flake::new(
            subject,
            make_sid(3, "type"), // rdf:type
            FlakeValue::Ref(class),
            make_sid(1, "id"),
            t,
            op,
            None,
        )
    }

    fn make_prop_flake(subject: Sid, prop: Sid, value: i64, t: i64) -> Flake {
        Flake::new(
            subject,
            prop,
            FlakeValue::Long(value),
            make_sid(2, "long"), // xsd:long
            t,
            true,
            None,
        )
    }

    fn make_lang_flake(subject: Sid, prop: Sid, value: &str, lang: &str, t: i64) -> Flake {
        Flake::new(
            subject,
            prop,
            FlakeValue::String(value.to_string()),
            make_sid(3, "langString"), // rdf:langString
            t,
            true,
            Some(FlakeMeta::with_lang(lang)),
        )
    }

    #[test]
    fn test_empty_novelty_returns_indexed() {
        let indexed = IndexStats {
            flakes: 100,
            size: 5000,
            properties: Some(vec![PropertyStatEntry {
                sid: (100, "name".to_string()),
                count: 50,
                ndv_values: 25,
                ndv_subjects: 20,
                last_modified_t: 10,
                datatypes: vec![],
            }]),
            classes: None,
            graphs: None,
        };

        let novelty = Novelty::new(0);
        let result = current_stats(&indexed, &novelty);

        assert_eq!(result.flakes, 100);
        assert_eq!(result.properties.as_ref().unwrap()[0].count, 50);
    }

    #[test]
    fn test_property_count_update() {
        let indexed = IndexStats {
            flakes: 10,
            size: 500,
            properties: Some(vec![PropertyStatEntry {
                sid: (100, "name".to_string()),
                count: 10,
                ndv_values: 5,
                ndv_subjects: 5,
                last_modified_t: 5,
                datatypes: vec![],
            }]),
            classes: None,
            graphs: None,
        };

        let mut novelty = Novelty::new(5);
        let flakes = vec![
            make_prop_flake(make_sid(100, "alice"), make_sid(100, "name"), 42, 6),
            make_prop_flake(make_sid(100, "bob"), make_sid(100, "name"), 43, 6),
        ];
        novelty.apply_commit(flakes, 6).unwrap();

        let result = current_stats(&indexed, &novelty);
        let props = result.properties.unwrap();

        // Find the "name" property
        let name_prop = props.iter().find(|p| p.sid == (100, "name".to_string()));
        assert!(name_prop.is_some());
        assert_eq!(name_prop.unwrap().count, 12); // 10 + 2

        // NDV should be preserved
        assert_eq!(name_prop.unwrap().ndv_values, 5);
    }

    #[test]
    fn test_class_count_from_type_flakes() {
        let indexed = IndexStats::default();

        let mut novelty = Novelty::new(0);
        let flakes = vec![
            make_type_flake(make_sid(100, "alice"), make_sid(100, "Person"), 1, true),
            make_type_flake(make_sid(100, "bob"), make_sid(100, "Person"), 1, true),
            make_type_flake(make_sid(100, "acme"), make_sid(100, "Company"), 1, true),
        ];
        novelty.apply_commit(flakes, 1).unwrap();

        let result = current_stats(&indexed, &novelty);
        let classes = result.classes.unwrap();

        // Should have Person with count 2 and Company with count 1
        let person = classes
            .iter()
            .find(|c| c.class_sid == make_sid(100, "Person"));
        let company = classes
            .iter()
            .find(|c| c.class_sid == make_sid(100, "Company"));

        assert_eq!(person.unwrap().count, 2);
        assert_eq!(company.unwrap().count, 1);
    }

    #[test]
    fn test_class_property_details() {
        let indexed = IndexStats::default();

        let mut novelty = Novelty::new(0);
        let alice = make_sid(100, "alice");
        let bob = make_sid(100, "bob");
        let person = make_sid(100, "Person");
        let name_prop = make_sid(100, "name");

        let flakes = vec![
            make_type_flake(alice.clone(), person.clone(), 1, true),
            make_type_flake(bob.clone(), person.clone(), 1, true),
            make_prop_flake(alice.clone(), name_prop.clone(), 42, 1),
            make_prop_flake(bob.clone(), name_prop.clone(), 43, 1),
        ];
        novelty.apply_commit(flakes, 1).unwrap();

        let result = current_stats(&indexed, &novelty);
        let classes = result.classes.unwrap();

        let person_class = classes
            .iter()
            .find(|c| c.class_sid == person)
            .expect("Person class should exist");

        // Person should have the name property tracked
        let name_usage = person_class
            .properties
            .iter()
            .find(|p| p.property_sid == name_prop);

        assert!(name_usage.is_some());
    }

    #[test]
    fn test_retraction_decrements_counts() {
        let indexed = IndexStats {
            flakes: 10,
            size: 500,
            properties: Some(vec![PropertyStatEntry {
                sid: (100, "name".to_string()),
                count: 10,
                ndv_values: 5,
                ndv_subjects: 5,
                last_modified_t: 5,
                datatypes: vec![],
            }]),
            classes: None,
            graphs: None,
        };

        let mut novelty = Novelty::new(5);

        // Retract one flake (op = false)
        let flakes = vec![Flake::new(
            make_sid(100, "alice"),
            make_sid(100, "name"),
            FlakeValue::Long(42),
            make_sid(2, "long"),
            6,
            false, // retraction
            None,
        )];
        novelty.apply_commit(flakes, 6).unwrap();

        let result = current_stats(&indexed, &novelty);
        let props = result.properties.unwrap();

        let name_prop = props.iter().find(|p| p.sid == (100, "name".to_string()));
        assert_eq!(name_prop.unwrap().count, 9); // 10 - 1
    }

    #[test]
    fn test_property_presence_tracking_with_lang_values() {
        let indexed = IndexStats::default();

        let mut novelty = Novelty::new(0);
        let alice = make_sid(100, "alice");
        let person = make_sid(100, "Person");
        let label = make_sid(3, "label"); // rdfs:label

        let flakes = vec![
            make_type_flake(alice.clone(), person.clone(), 1, true),
            make_lang_flake(alice.clone(), label.clone(), "Alice", "en", 1),
            make_lang_flake(alice.clone(), label.clone(), "Alicia", "es", 1),
        ];
        novelty.apply_commit(flakes, 1).unwrap();

        let result = current_stats(&indexed, &novelty);
        let classes = result.classes.unwrap();

        let person_class = classes.iter().find(|c| c.class_sid == person).unwrap();
        let label_usage = person_class
            .properties
            .iter()
            .find(|p| p.property_sid == label)
            .unwrap();
        // Language-tag details are not tracked at class-property level anymore;
        // this test only verifies the property is associated with the class.
        let _ = label_usage;
    }
}
