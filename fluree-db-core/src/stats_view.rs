//! Pre-built statistics lookup for query optimization.
//!
//! `StatsView` provides O(1) lookups of property and class statistics,
//! built from `DbRootStats` at query time.

use crate::serde::json::DbRootStats;
use crate::sid::Sid;
use std::collections::HashMap;
use std::sync::Arc;

/// Pre-built stats lookup for query optimization.
///
/// Built from `DbRootStats` at query time, provides O(1) lookups for
/// property and class statistics used in selectivity estimation.
#[derive(Debug, Default, Clone)]
pub struct StatsView {
    /// Property SID -> (count, ndv_values, ndv_subjects)
    pub properties: HashMap<Sid, PropertyStatData>,
    /// Class SID -> instance count
    pub classes: HashMap<Sid, u64>,
    /// Property IRI -> (count, ndv_values, ndv_subjects)
    ///
    /// This is derived from `properties` using the db's namespace table.
    /// It exists to support planners that keep IRIs unencoded (e.g. cross-ledger-aware planning).
    pub properties_by_iri: HashMap<Arc<str>, PropertyStatData>,
    /// Class IRI -> instance count
    ///
    /// This is derived from `classes` using the db's namespace table.
    pub classes_by_iri: HashMap<Arc<str>, u64>,
}

/// Statistics for a single property.
#[derive(Debug, Clone, Copy)]
pub struct PropertyStatData {
    /// Total number of flakes with this property
    pub count: u64,
    /// Estimated number of distinct object values (from HLL)
    pub ndv_values: u64,
    /// Estimated number of distinct subjects using this property (from HLL)
    pub ndv_subjects: u64,
}

impl StatsView {
    /// Build from DbRootStats.
    ///
    /// Note: `PropertyStatEntry.sid` is already `(i32, String)` matching `Sid::new` shape,
    /// so no namespace_codes lookup is needed.
    pub fn from_db_stats(stats: &DbRootStats) -> Self {
        let mut view = StatsView::default();

        if let Some(ref props) = stats.properties {
            for entry in props {
                // entry.sid is (namespace_code, name) - directly usable
                let sid = Sid::new(entry.sid.0, &entry.sid.1);
                view.properties.insert(
                    sid,
                    PropertyStatData {
                        count: entry.count,
                        ndv_values: entry.ndv_values,
                        ndv_subjects: entry.ndv_subjects,
                    },
                );
            }
        }

        if let Some(ref classes) = stats.classes {
            for entry in classes {
                view.classes.insert(entry.class_sid.clone(), entry.count);
            }
        }

        view
    }

    /// Build from DbRootStats, also deriving IRI-keyed maps using a namespace table.
    ///
    /// This does **not** change how stats are persisted (still SID-keyed in `DbRootStats`).
    /// It just builds additional lookup maps that allow planning code to consult stats
    /// when query terms are represented as IRIs rather than SIDs.
    pub fn from_db_stats_with_namespaces(
        stats: &DbRootStats,
        namespace_codes: &HashMap<i32, String>,
    ) -> Self {
        let mut view = StatsView::from_db_stats(stats);

        // Derive IRI-keyed property stats.
        // If a SID's namespace code is missing, skip it.
        for (sid, data) in view.properties.iter() {
            if let Some(prefix) = namespace_codes.get(&sid.namespace_code) {
                let iri: Arc<str> = Arc::from(format!("{}{}", prefix, sid.name));
                view.properties_by_iri.insert(iri, *data);
            }
        }

        // Derive IRI-keyed class stats.
        for (sid, count) in view.classes.iter() {
            if let Some(prefix) = namespace_codes.get(&sid.namespace_code) {
                let iri: Arc<str> = Arc::from(format!("{}{}", prefix, sid.name));
                view.classes_by_iri.insert(iri, *count);
            }
        }

        view
    }

    /// Get property statistics by SID.
    pub fn get_property(&self, sid: &Sid) -> Option<&PropertyStatData> {
        self.properties.get(sid)
    }

    /// Get property statistics by IRI.
    pub fn get_property_by_iri(&self, iri: &str) -> Option<&PropertyStatData> {
        self.properties_by_iri.get(iri)
    }

    /// Get class instance count by SID.
    pub fn get_class_count(&self, sid: &Sid) -> Option<u64> {
        self.classes.get(sid).copied()
    }

    /// Get class instance count by IRI.
    pub fn get_class_count_by_iri(&self, iri: &str) -> Option<u64> {
        self.classes_by_iri.get(iri).copied()
    }

    /// Check if any property statistics are available.
    pub fn has_property_stats(&self) -> bool {
        !self.properties.is_empty()
    }

    /// Check if any class statistics are available.
    pub fn has_class_stats(&self) -> bool {
        !self.classes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::json::{ClassStatEntry, PropertyStatEntry};

    #[test]
    fn test_empty_stats() {
        let stats = DbRootStats {
            flakes: 0,
            size: 0,
            properties: None,
            classes: None,
        };
        let view = StatsView::from_db_stats(&stats);
        assert!(!view.has_property_stats());
        assert!(!view.has_class_stats());
    }

    #[test]
    fn test_property_lookup() {
        let stats = DbRootStats {
            flakes: 100,
            size: 1000,
            properties: Some(vec![PropertyStatEntry {
                sid: (1, "name".to_string()),
                count: 50,
                ndv_values: 40,
                ndv_subjects: 45,
                last_modified_t: 10,
            }]),
            classes: None,
        };
        let view = StatsView::from_db_stats(&stats);
        assert!(view.has_property_stats());

        let sid = Sid::new(1, "name");
        let prop = view.get_property(&sid).unwrap();
        assert_eq!(prop.count, 50);
        assert_eq!(prop.ndv_values, 40);
        assert_eq!(prop.ndv_subjects, 45);
    }

    #[test]
    fn test_class_lookup() {
        let class_sid = Sid::new(2, "Person");
        let stats = DbRootStats {
            flakes: 100,
            size: 1000,
            properties: None,
            classes: Some(vec![ClassStatEntry {
                class_sid: class_sid.clone(),
                count: 25,
                properties: vec![],
            }]),
        };
        let view = StatsView::from_db_stats(&stats);
        assert!(view.has_class_stats());

        let count = view.get_class_count(&class_sid).unwrap();
        assert_eq!(count, 25);
    }
}
