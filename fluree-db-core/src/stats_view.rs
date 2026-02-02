//! Pre-built statistics lookup for query optimization.
//!
//! `StatsView` provides O(1) lookups of property and class statistics,
//! built from `DbRootStats` at query time.

use crate::serde::json::DbRootStats;
use crate::sid::Sid;
use crate::value_id::DatatypeId;
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
    /// Graph-scoped property stats keyed by numeric IDs: g_id -> (p_id -> data).
    ///
    /// Populated from `DbRootStats.graphs` when present. Provides per-graph
    /// property lookups with datatype breakdown. The aggregate Sid-keyed
    /// `properties` map remains the primary source for the query planner.
    pub graph_properties: HashMap<u32, HashMap<u32, GraphPropertyStatData>>,
}

/// Per-property statistics within a graph, keyed by numeric IDs.
#[derive(Debug, Clone)]
pub struct GraphPropertyStatData {
    /// Total number of flakes with this property in this graph
    pub count: u64,
    /// Estimated number of distinct object values (from HLL)
    pub ndv_values: u64,
    /// Estimated number of distinct subjects using this property (from HLL)
    pub ndv_subjects: u64,
    /// Per-datatype flake counts
    pub datatypes: Vec<(DatatypeId, u64)>,
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

        if let Some(ref graphs) = stats.graphs {
            for g_entry in graphs {
                let mut prop_map = HashMap::new();
                for p_entry in &g_entry.properties {
                    prop_map.insert(
                        p_entry.p_id,
                        GraphPropertyStatData {
                            count: p_entry.count,
                            ndv_values: p_entry.ndv_values,
                            ndv_subjects: p_entry.ndv_subjects,
                            datatypes: p_entry
                                .datatypes
                                .iter()
                                .map(|&(dt, c)| (DatatypeId::from_u8(dt), c))
                                .collect(),
                        },
                    );
                }
                view.graph_properties.insert(g_entry.g_id, prop_map);
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

    /// Get property stats within a specific graph by numeric IDs.
    pub fn get_graph_property(&self, g_id: u32, p_id: u32) -> Option<&GraphPropertyStatData> {
        self.graph_properties.get(&g_id)?.get(&p_id)
    }

    /// Get all property stats for a specific graph.
    pub fn get_graph_properties(&self, g_id: u32) -> Option<&HashMap<u32, GraphPropertyStatData>> {
        self.graph_properties.get(&g_id)
    }

    /// Return the set of graph IDs that have stats.
    pub fn graph_ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.graph_properties.keys().copied()
    }

    /// Check if any graph-scoped statistics are available.
    pub fn has_graph_stats(&self) -> bool {
        !self.graph_properties.is_empty()
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
            graphs: None,
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
            graphs: None,
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
            graphs: None,
        };
        let view = StatsView::from_db_stats(&stats);
        assert!(view.has_class_stats());

        let count = view.get_class_count(&class_sid).unwrap();
        assert_eq!(count, 25);
    }
}
