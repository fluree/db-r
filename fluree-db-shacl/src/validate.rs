//! SHACL validation engine
//!
//! This module provides the core validation logic for checking RDF data
//! against SHACL shapes.

use crate::cache::{ShaclCache, ShaclCacheKey};
use crate::compile::{CompiledShape, PropertyShape, Severity, ShapeCompiler, TargetType};
use crate::constraints::cardinality::{validate_max_count, validate_min_count};
use crate::constraints::datatype::{validate_datatype, validate_node_kind};
use crate::constraints::pattern::{validate_max_length, validate_min_length, validate_pattern};
use crate::constraints::value::{
    validate_has_value, validate_in, validate_max_exclusive, validate_max_inclusive,
    validate_min_exclusive, validate_min_inclusive,
};
use crate::constraints::{Constraint, ConstraintViolation, NestedShape};
use crate::error::Result;
use fluree_db_core::{
    range_with_overlay, Db, FlakeValue, IndexType, NoOverlay, NodeCache, OverlayProvider,
    RangeMatch, RangeOptions, RangeTest, SchemaHierarchy, Sid, Storage,
};
use fluree_vocab::namespaces::RDF;
use fluree_vocab::rdf_names;
use std::collections::HashSet;

/// Create RangeOptions with high to_t to include overlay flakes
///
/// This is essential when validating staged data, as staged flakes have
/// t values higher than the base db.t. Without this, overlay flakes would
/// be filtered out by the default to_t = db.t filtering.
fn range_opts_include_overlay() -> RangeOptions {
    RangeOptions::default().with_to_t(i64::MAX)
}

/// SHACL validation engine
///
/// When constructed with a `SchemaHierarchy`, the engine properly handles RDFS
/// reasoning for `sh:targetClass`:
/// - A shape targeting `Animal` will also apply to instances of `Dog`
///   (if `Dog rdfs:subClassOf Animal`)
pub struct ShaclEngine {
    /// Cached compiled shapes
    cache: ShaclCache,
    /// Schema hierarchy for RDFS reasoning (optional)
    hierarchy: Option<SchemaHierarchy>,
}

impl ShaclEngine {
    /// Create a new engine from a cache (without hierarchy support)
    ///
    /// For full RDFS reasoning support, use `new_with_hierarchy` instead.
    pub fn new(cache: ShaclCache) -> Self {
        Self {
            cache,
            hierarchy: None,
        }
    }

    /// Create a new engine from a cache with hierarchy support
    ///
    /// The hierarchy enables RDFS reasoning for `sh:targetClass`:
    /// shapes targeting a class will also apply to instances of subclasses.
    pub fn new_with_hierarchy(cache: ShaclCache, hierarchy: SchemaHierarchy) -> Self {
        Self {
            cache,
            hierarchy: Some(hierarchy),
        }
    }

    /// Build an engine by compiling shapes from a database with optional overlay
    ///
    /// The overlay (typically novelty) allows compiling shapes that were
    /// transacted in previous commits but haven't been indexed yet.
    /// This is important because SHACL shapes need to be available
    /// immediately after they are committed.
    ///
    /// Automatically extracts the schema hierarchy for RDFS reasoning.
    pub async fn from_db_with_overlay<S: Storage, C: NodeCache, O: OverlayProvider>(
        db: &Db<S, C>,
        overlay: &O,
        ledger_id: impl Into<String>,
    ) -> Result<Self> {
        let shapes = ShapeCompiler::compile_from_db(db, overlay).await?;
        let key = ShaclCacheKey::new(ledger_id, db.t as u64);

        // Get hierarchy for RDFS reasoning (class expansion in cache indexing)
        let hierarchy = db.schema_hierarchy();
        let cache = ShaclCache::new(key, shapes, hierarchy.as_ref());

        Ok(Self { cache, hierarchy })
    }

    /// Build an engine by compiling shapes from a database (no overlay)
    ///
    /// This is a convenience method for when there is no novelty to consider,
    /// such as when loading from a fully indexed database.
    ///
    /// Automatically extracts the schema hierarchy for RDFS reasoning.
    pub async fn from_db<S: Storage, C: NodeCache>(
        db: &Db<S, C>,
        ledger_id: impl Into<String>,
    ) -> Result<Self> {
        Self::from_db_with_overlay(db, &fluree_db_core::NoOverlay, ledger_id).await
    }

    /// Validate a focus node against applicable shapes
    ///
    /// This is the main entry point for validation. It:
    /// 1. Finds all shapes that target the focus node
    /// 2. Validates the node against each shape's constraints
    /// 3. Returns a validation report
    pub async fn validate_node<S: Storage, C: NodeCache, O: OverlayProvider>(
        &self,
        db: &Db<S, C>,
        overlay: &O,
        focus_node: &Sid,
        node_types: &[Sid],
    ) -> Result<ValidationReport> {
        let mut results = Vec::new();

        // Find shapes that apply to this node
        let mut applicable_shapes: Vec<&CompiledShape> = Vec::new();

        // By explicit target node
        applicable_shapes.extend(self.cache.shapes_for_node(focus_node));

        // By class targeting
        for class in node_types {
            applicable_shapes.extend(self.cache.shapes_for_class(class));
        }

        // Remove duplicates
        let mut seen = HashSet::new();
        applicable_shapes.retain(|s| seen.insert(&s.id));

        // Collect all shapes for logical constraint resolution
        let all_shapes: Vec<&CompiledShape> = self.cache.all_shapes().iter().collect();

        // Validate against each shape
        for shape in applicable_shapes {
            if shape.deactivated {
                continue;
            }

            let shape_results = validate_shape(db, overlay, focus_node, shape, &all_shapes).await?;
            results.extend(shape_results);
        }

        let conforms = results.iter().all(|r| r.severity != Severity::Violation);

        Ok(ValidationReport { conforms, results })
    }

    /// Validate a focus node without an overlay
    pub async fn validate_node_no_overlay<S: Storage, C: NodeCache>(
        &self,
        db: &Db<S, C>,
        focus_node: &Sid,
        node_types: &[Sid],
    ) -> Result<ValidationReport> {
        self.validate_node(db, &NoOverlay, focus_node, node_types)
            .await
    }

    /// Validate all focus nodes targeted by shapes
    pub async fn validate_all<S: Storage, C: NodeCache, O: OverlayProvider>(
        &self,
        db: &Db<S, C>,
        overlay: &O,
    ) -> Result<ValidationReport> {
        let mut all_results = Vec::new();

        // Collect all shapes for logical constraint resolution
        let all_shapes: Vec<&CompiledShape> = self.cache.all_shapes().iter().collect();

        for shape in self.cache.all_shapes() {
            if shape.deactivated {
                continue;
            }

            // Get focus nodes for this shape (with hierarchy expansion)
            let focus_nodes = get_focus_nodes(db, overlay, shape, self.hierarchy.as_ref()).await?;

            for focus_node in focus_nodes {
                let results = validate_shape(db, overlay, &focus_node, shape, &all_shapes).await?;
                all_results.extend(results);
            }
        }

        let conforms = all_results
            .iter()
            .all(|r| r.severity != Severity::Violation);

        Ok(ValidationReport {
            conforms,
            results: all_results,
        })
    }

    /// Validate all focus nodes without an overlay
    pub async fn validate_all_no_overlay<S: Storage, C: NodeCache>(
        &self,
        db: &Db<S, C>,
    ) -> Result<ValidationReport> {
        self.validate_all(db, &NoOverlay).await
    }

    /// Get the underlying cache
    pub fn cache(&self) -> &ShaclCache {
        &self.cache
    }

    // ========================================================================
    // Optimization: Early exit when no shapes
    // ========================================================================
    // Following the Clojure pattern: `(if (empty? shapes) :valid ...)`
    // This elides all validation work when no SHACL shapes are defined.

    /// Check if there are any shapes to validate against
    ///
    /// Use this for early exit: if no shapes exist, validation is a no-op.
    /// This follows the Clojure SHACL implementation optimization.
    #[inline]
    pub fn has_shapes(&self) -> bool {
        !self.cache.is_empty()
    }

    /// Check if there are no shapes (validation will be a no-op)
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Get the number of shapes
    #[inline]
    pub fn shape_count(&self) -> usize {
        self.cache.len()
    }

    /// Validate only the subjects that were modified in a transaction
    ///
    /// This is the primary entry point for transaction-time validation.
    /// It validates only the subjects present in `modified_subjects` against
    /// applicable shapes, returning early if no shapes exist.
    ///
    /// # Arguments
    /// * `db` - The database to validate against
    /// * `overlay` - Overlay containing staged changes (so validation sees new data)
    /// * `modified_subjects` - Set of subject SIDs that were modified in the transaction
    ///
    /// # Returns
    /// * `ValidationReport` - conforming if no violations, or containing all violations
    pub async fn validate_staged<S: Storage, C: NodeCache, O: OverlayProvider>(
        &self,
        db: &Db<S, C>,
        overlay: &O,
        modified_subjects: &HashSet<Sid>,
    ) -> Result<ValidationReport> {
        // Early exit: no shapes means automatic conformance
        // This is the key optimization from Clojure: (if (empty? shapes) :valid ...)
        if self.cache.is_empty() {
            return Ok(ValidationReport::conforming());
        }

        // Early exit: no modified subjects means nothing to validate
        if modified_subjects.is_empty() {
            return Ok(ValidationReport::conforming());
        }

        let mut all_results = Vec::new();

        // For each modified subject, find its types and validate
        let rdf_type = db.sid_interner.intern(RDF, rdf_names::TYPE);

        for subject in modified_subjects {
            // Get the types of this subject (through the overlay so we see staged data)
            let type_flakes = range_with_overlay(
                db,
                overlay,
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject_predicate(subject.clone(), rdf_type.clone()),
                range_opts_include_overlay(),
            )
            .await?;

            let node_types: Vec<Sid> = type_flakes
                .iter()
                .filter_map(|f| {
                    if let FlakeValue::Ref(t) = &f.o {
                        Some(t.clone())
                    } else {
                        None
                    }
                })
                .collect();

            // Validate this node against applicable shapes
            let report = self
                .validate_node(db, overlay, subject, &node_types)
                .await?;
            all_results.extend(report.results);
        }

        let conforms = all_results
            .iter()
            .all(|r| r.severity != Severity::Violation);

        Ok(ValidationReport {
            conforms,
            results: all_results,
        })
    }

    /// Validate staged changes, returning an error if validation fails
    ///
    /// This is a convenience wrapper around `validate_staged` that converts
    /// validation failures into errors, suitable for use in transaction staging.
    pub async fn validate_staged_or_error<S: Storage, C: NodeCache, O: OverlayProvider>(
        &self,
        db: &Db<S, C>,
        overlay: &O,
        modified_subjects: &HashSet<Sid>,
    ) -> Result<()> {
        let report = self.validate_staged(db, overlay, modified_subjects).await?;

        if report.conforms {
            Ok(())
        } else {
            // Build detailed error messages (limit to first 10 to avoid huge errors)
            let details: Vec<String> = report
                .results
                .iter()
                .filter(|r| r.severity == Severity::Violation)
                .take(10)
                .map(|r| {
                    if let Some(ref path) = r.result_path {
                        format!(
                            "Node {}: property {}: {}",
                            r.focus_node.name, path.name, r.message
                        )
                    } else {
                        format!("Node {}: {}", r.focus_node.name, r.message)
                    }
                })
                .collect();

            Err(crate::error::ShaclError::ValidationFailed {
                violation_count: report.violation_count(),
                warning_count: report.warning_count(),
                details,
            })
        }
    }
}

/// Get focus nodes for a shape based on its targeting declarations
///
/// When a hierarchy is provided, `TargetType::Class` targets are expanded
/// to include instances of all subclasses. For example, a shape targeting
/// `Animal` will also match instances of `Dog` (if `Dog rdfs:subClassOf Animal`).
async fn get_focus_nodes<S: Storage, C: NodeCache, O: OverlayProvider>(
    db: &Db<S, C>,
    overlay: &O,
    shape: &CompiledShape,
    hierarchy: Option<&SchemaHierarchy>,
) -> Result<Vec<Sid>> {
    let mut focus_nodes = Vec::new();

    for target in &shape.targets {
        match target {
            TargetType::Class(class) | TargetType::ImplicitClass(class) => {
                // Build list of classes to query: target class + all subclasses
                let mut classes_to_query = vec![class.clone()];
                if let Some(h) = hierarchy {
                    classes_to_query.extend(h.subclasses_of(class).iter().cloned());
                }

                // Find all instances of each class
                let rdf_type = db.sid_interner.intern(RDF, rdf_names::TYPE);
                for cls in classes_to_query {
                    let flakes = range_with_overlay(
                        db,
                        overlay,
                        IndexType::Psot,
                        RangeTest::Eq,
                        RangeMatch::predicate_object(rdf_type.clone(), FlakeValue::Ref(cls)),
                        range_opts_include_overlay(),
                    )
                    .await?;

                    for flake in flakes {
                        focus_nodes.push(flake.s.clone());
                    }
                }
            }
            TargetType::Node(nodes) => {
                focus_nodes.extend(nodes.iter().cloned());
            }
            TargetType::SubjectsOf(predicate) => {
                // Find all subjects that have this predicate
                let flakes = range_with_overlay(
                    db,
                    overlay,
                    IndexType::Psot,
                    RangeTest::Eq,
                    RangeMatch::predicate(predicate.clone()),
                    range_opts_include_overlay(),
                )
                .await?;

                for flake in flakes {
                    focus_nodes.push(flake.s.clone());
                }
            }
            TargetType::ObjectsOf(predicate) => {
                // Find all objects of triples with this predicate
                let flakes = range_with_overlay(
                    db,
                    overlay,
                    IndexType::Psot,
                    RangeTest::Eq,
                    RangeMatch::predicate(predicate.clone()),
                    range_opts_include_overlay(),
                )
                .await?;

                for flake in flakes {
                    if let FlakeValue::Ref(obj) = &flake.o {
                        focus_nodes.push(obj.clone());
                    }
                }
            }
        }
    }

    // Remove duplicates
    let mut seen = HashSet::new();
    focus_nodes.retain(|n| seen.insert(n.clone()));

    Ok(focus_nodes)
}

/// Validate a focus node against a single shape
///
/// Note: This function uses `Box::pin` for recursive calls to avoid infinitely-sized futures.
fn validate_shape<'a, S: Storage, C: NodeCache, O: OverlayProvider>(
    db: &'a Db<S, C>,
    overlay: &'a O,
    focus_node: &'a Sid,
    shape: &'a CompiledShape,
    all_shapes: &'a [&'a CompiledShape],
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<ValidationResult>>> + Send + 'a>>
{
    Box::pin(async move {
        let mut results = Vec::new();

        // Validate property shapes
        for prop_shape in &shape.property_shapes {
            let prop_results =
                validate_property_shape(db, overlay, focus_node, prop_shape, shape).await?;
            results.extend(prop_results);
        }

        // Validate structural constraints (closed, logical)
        for constraint in &shape.structural_constraints {
            let constraint_results = validate_structural_constraint(
                db, overlay, focus_node, constraint, shape, all_shapes,
            )
            .await?;
            results.extend(constraint_results);
        }

        Ok(results)
    })
}

/// Validate a structural (node-level) constraint
///
/// Note: This function uses `Box::pin` for recursive calls to avoid infinitely-sized futures.
fn validate_structural_constraint<'a, S: Storage, C: NodeCache, O: OverlayProvider>(
    db: &'a Db<S, C>,
    overlay: &'a O,
    focus_node: &'a Sid,
    constraint: &'a crate::constraints::NodeConstraint,
    parent_shape: &'a CompiledShape,
    all_shapes: &'a [&'a CompiledShape],
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<ValidationResult>>> + Send + 'a>>
{
    Box::pin(async move {
        use crate::compile::Severity;
        use crate::constraints::NodeConstraint;

        let mut results = Vec::new();

        match constraint {
            NodeConstraint::Closed {
                is_closed,
                ignored_properties,
            } => {
                if *is_closed {
                    // Get all properties used by the focus node
                    let node_flakes = range_with_overlay(
                        db,
                        overlay,
                        IndexType::Spot,
                        RangeTest::Eq,
                        RangeMatch::subject(focus_node.clone()),
                        range_opts_include_overlay(),
                    )
                    .await?;

                    // Collect declared properties from the shape's property shapes
                    let declared_properties: std::collections::HashSet<&Sid> = parent_shape
                        .property_shapes
                        .iter()
                        .map(|ps| &ps.path)
                        .collect();

                    // Per SHACL spec section 4.8.1, rdf:type is implicitly ignored
                    let rdf_type_sid = db.sid_interner.intern(RDF, rdf_names::TYPE);
                    let mut effective_ignored = ignored_properties.clone();
                    effective_ignored.insert(rdf_type_sid);

                    // Check each property on the node
                    for flake in node_flakes {
                        let prop = &flake.p;
                        if !declared_properties.contains(prop) && !effective_ignored.contains(prop)
                        {
                            results.push(ValidationResult {
                                focus_node: focus_node.clone(),
                                result_path: Some(prop.clone()),
                                source_shape: parent_shape.id.clone(),
                                source_constraint: None,
                                severity: Severity::Violation,
                                message: format!(
                                    "Property {} not allowed by closed shape",
                                    prop.name
                                ),
                                value: Some(flake.o.clone()),
                            });
                        }
                    }
                }
            }

            NodeConstraint::Not(nested_shape) => {
                // sh:not - the nested shape must NOT match
                // Find the referenced shape and validate against it
                if let Some(ref_shape) = all_shapes.iter().find(|s| s.id == nested_shape.id) {
                    let nested_results =
                        validate_shape(db, overlay, focus_node, ref_shape, all_shapes).await?;
                    // If the nested shape has NO violations, that's a violation of sh:not
                    if nested_results.is_empty()
                        || nested_results
                            .iter()
                            .all(|r| r.severity != Severity::Violation)
                    {
                        results.push(ValidationResult {
                            focus_node: focus_node.clone(),
                            result_path: None,
                            source_shape: parent_shape.id.clone(),
                            source_constraint: None,
                            severity: Severity::Violation,
                            message: format!(
                                "Node conforms to shape {} which is not allowed (sh:not)",
                                nested_shape.id.name
                            ),
                            value: None,
                        });
                    }
                }
            }

            NodeConstraint::And(nested_shapes) => {
                // sh:and - ALL nested shapes must match (no violations)
                for nested in nested_shapes {
                    let nested_results = validate_nested_shape(
                        db,
                        overlay,
                        focus_node,
                        nested.as_ref(),
                        parent_shape,
                        all_shapes,
                    )
                    .await?;
                    // Include violations from the nested shape
                    for r in nested_results {
                        if r.severity == Severity::Violation {
                            results.push(ValidationResult {
                                focus_node: focus_node.clone(),
                                result_path: r.result_path,
                                source_shape: parent_shape.id.clone(),
                                source_constraint: None,
                                severity: Severity::Violation,
                                message: format!("sh:and constraint - {}", r.message),
                                value: r.value,
                            });
                        }
                    }
                }
            }

            NodeConstraint::Or(nested_shapes) => {
                // sh:or - at least ONE nested shape must match (have no violations)
                let mut any_conforms = false;
                let mut all_messages = Vec::new();

                for nested in nested_shapes {
                    let nested_results = validate_nested_shape(
                        db,
                        overlay,
                        focus_node,
                        nested.as_ref(),
                        parent_shape,
                        all_shapes,
                    )
                    .await?;
                    let has_violations = nested_results
                        .iter()
                        .any(|r| r.severity == Severity::Violation);
                    if !has_violations {
                        any_conforms = true;
                        break;
                    } else {
                        // Collect messages for reporting if none match
                        for r in nested_results {
                            if r.severity == Severity::Violation {
                                all_messages.push(format!("{}: {}", nested.id.name, r.message));
                            }
                        }
                    }
                }

                if !any_conforms && !nested_shapes.is_empty() {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: None,
                        source_shape: parent_shape.id.clone(),
                        source_constraint: None,
                        severity: Severity::Violation,
                        message: format!(
                            "Node does not conform to any shape in sh:or. Violations: {}",
                            all_messages.join("; ")
                        ),
                        value: None,
                    });
                }
            }

            NodeConstraint::Xone(nested_shapes) => {
                // sh:xone - exactly ONE nested shape must match
                let mut conforming_count = 0;
                let mut conforming_shapes = Vec::new();

                for nested in nested_shapes {
                    let nested_results = validate_nested_shape(
                        db,
                        overlay,
                        focus_node,
                        nested.as_ref(),
                        parent_shape,
                        all_shapes,
                    )
                    .await?;
                    let has_violations = nested_results
                        .iter()
                        .any(|r| r.severity == Severity::Violation);
                    if !has_violations {
                        conforming_count += 1;
                        conforming_shapes.push(nested.id.name.clone());
                    }
                }

                if conforming_count == 0 {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: None,
                        source_shape: parent_shape.id.clone(),
                        source_constraint: None,
                        severity: Severity::Violation,
                        message: "Node does not conform to any shape in sh:xone".to_string(),
                        value: None,
                    });
                } else if conforming_count > 1 {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: None,
                        source_shape: parent_shape.id.clone(),
                        source_constraint: None,
                        severity: Severity::Violation,
                        message: format!(
                            "Node conforms to {} shapes in sh:xone (must be exactly 1): {}",
                            conforming_count,
                            conforming_shapes.join(", ")
                        ),
                        value: None,
                    });
                }
            }
        }

        Ok(results)
    })
}

/// Validate a focus node against a nested shape (inline shape from sh:and/or/xone)
///
/// Unlike `validate_shape` which validates against a `CompiledShape`, this validates
/// directly against the constraints embedded in a `NestedShape`.
fn validate_nested_shape<'a, S: Storage, C: NodeCache, O: OverlayProvider>(
    db: &'a Db<S, C>,
    overlay: &'a O,
    focus_node: &'a Sid,
    nested: &'a NestedShape,
    parent_shape: &'a CompiledShape,
    all_shapes: &'a [&'a CompiledShape],
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<ValidationResult>>> + Send + 'a>>
{
    Box::pin(async move {
        // If the NestedShape has no inline constraints, try to find the referenced shape
        // in all_shapes (for top-level shapes referenced by ID in sh:and/or/xone)
        if nested.property_constraints.is_empty() && nested.node_constraints.is_empty() {
            if let Some(ref_shape) = all_shapes.iter().find(|s| s.id == nested.id) {
                return validate_shape(db, overlay, focus_node, ref_shape, all_shapes).await;
            }
        }

        let mut results = Vec::new();

        // Validate property constraints
        for (path, constraints) in &nested.property_constraints {
            // Get all values for this property on the focus node
            let flakes = range_with_overlay(
                db,
                overlay,
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject_predicate(focus_node.clone(), path.clone()),
                range_opts_include_overlay(),
            )
            .await?;

            let values: Vec<FlakeValue> = flakes.iter().map(|f| f.o.clone()).collect();
            let datatypes: Vec<Sid> = flakes.iter().map(|f| f.dt.clone()).collect();

            // Validate each constraint
            for constraint in constraints {
                // Handle pair constraints separately since they need db access
                match constraint {
                    Constraint::Equals(target_prop) => {
                        // Get values for the target property
                        let target_flakes = range_with_overlay(
                            db,
                            overlay,
                            IndexType::Spot,
                            RangeTest::Eq,
                            RangeMatch::subject_predicate(focus_node.clone(), target_prop.clone()),
                            range_opts_include_overlay(),
                        )
                        .await?;
                        let target_values: std::collections::HashSet<_> =
                            target_flakes.iter().map(|f| &f.o).collect();
                        let source_values: std::collections::HashSet<_> = values.iter().collect();

                        if source_values != target_values {
                            results.push(ValidationResult {
                                focus_node: focus_node.clone(),
                                result_path: Some(path.clone()),
                                source_shape: parent_shape.id.clone(),
                                source_constraint: Some(nested.id.clone()),
                                severity: Severity::Violation,
                                message: format!(
                                    "Value set for {} does not equal value set for {}",
                                    path.name, target_prop.name
                                ),
                                value: None,
                            });
                        }
                    }
                    _ => {
                        let violations = validate_constraint(constraint, &values, &datatypes)?;
                        for violation in violations {
                            results.push(ValidationResult {
                                focus_node: focus_node.clone(),
                                result_path: Some(path.clone()),
                                source_shape: parent_shape.id.clone(),
                                source_constraint: Some(nested.id.clone()),
                                severity: Severity::Violation,
                                message: violation.message,
                                value: violation.value,
                            });
                        }
                    }
                }
            }
        }

        // Validate nested node constraints recursively
        for node_constraint in &nested.node_constraints {
            let nested_results = validate_structural_constraint(
                db,
                overlay,
                focus_node,
                node_constraint,
                parent_shape,
                all_shapes,
            )
            .await?;
            results.extend(nested_results);
        }

        Ok(results)
    })
}

/// Validate a focus node against a property shape
async fn validate_property_shape<S: Storage, C: NodeCache, O: OverlayProvider>(
    db: &Db<S, C>,
    overlay: &O,
    focus_node: &Sid,
    prop_shape: &PropertyShape,
    parent_shape: &CompiledShape,
) -> Result<Vec<ValidationResult>> {
    let mut results = Vec::new();

    // Get all values for this property on the focus node
    let flakes = range_with_overlay(
        db,
        overlay,
        IndexType::Spot,
        RangeTest::Eq,
        RangeMatch::subject_predicate(focus_node.clone(), prop_shape.path.clone()),
        range_opts_include_overlay(),
    )
    .await?;

    let values: Vec<FlakeValue> = flakes.iter().map(|f| f.o.clone()).collect();
    let datatypes: Vec<Sid> = flakes.iter().map(|f| f.dt.clone()).collect();

    // Validate each constraint
    for constraint in &prop_shape.constraints {
        // Handle pair constraints separately since they need db access
        match constraint {
            Constraint::Equals(target_prop) => {
                // Get values for the target property
                let target_flakes = range_with_overlay(
                    db,
                    overlay,
                    IndexType::Spot,
                    RangeTest::Eq,
                    RangeMatch::subject_predicate(focus_node.clone(), target_prop.clone()),
                    range_opts_include_overlay(),
                )
                .await?;
                let target_values: std::collections::HashSet<_> =
                    target_flakes.iter().map(|f| &f.o).collect();
                let source_values: std::collections::HashSet<_> = values.iter().collect();

                // sh:equals requires the value sets to be identical
                if source_values != target_values {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: Some(prop_shape.path.clone()),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: format!(
                            "Value set for {} does not equal value set for {}",
                            prop_shape.path.name, target_prop.name
                        ),
                        value: None,
                    });
                }
            }
            _ => {
                // Handle other constraints
                let violations = validate_constraint(constraint, &values, &datatypes)?;

                for violation in violations {
                    results.push(ValidationResult {
                        focus_node: focus_node.clone(),
                        result_path: Some(prop_shape.path.clone()),
                        source_shape: parent_shape.id.clone(),
                        source_constraint: Some(prop_shape.id.clone()),
                        severity: prop_shape.severity,
                        message: violation.message,
                        value: violation.value,
                    });
                }
            }
        }
    }

    Ok(results)
}

/// Validate a constraint against a set of values
fn validate_constraint(
    constraint: &Constraint,
    values: &[FlakeValue],
    datatypes: &[Sid],
) -> Result<Vec<ConstraintViolation>> {
    let mut violations = Vec::new();

    match constraint {
        // Cardinality constraints apply to the value set
        Constraint::MinCount(min) => {
            if let Some(v) = validate_min_count(values, *min) {
                violations.push(v);
            }
        }
        Constraint::MaxCount(max) => {
            if let Some(v) = validate_max_count(values, *max) {
                violations.push(v);
            }
        }

        // Value constraints apply to the value set
        Constraint::HasValue(expected) => {
            if let Some(v) = validate_has_value(values, expected) {
                violations.push(v);
            }
        }

        // Per-value constraints
        Constraint::Datatype(expected_dt) => {
            for (i, value) in values.iter().enumerate() {
                if let Some(actual_dt) = datatypes.get(i) {
                    if let Some(v) = validate_datatype(value, actual_dt, expected_dt) {
                        violations.push(v);
                    }
                }
            }
        }
        Constraint::NodeKind(kind) => {
            for value in values {
                if let Some(v) = validate_node_kind(value, *kind) {
                    violations.push(v);
                }
            }
        }
        Constraint::Class(_class) => {
            // TODO: Check rdf:type for each value
            // This requires querying the database for each value's types
        }
        Constraint::MinInclusive(min) => {
            for value in values {
                if let Some(v) = validate_min_inclusive(value, min) {
                    violations.push(v);
                }
            }
        }
        Constraint::MaxInclusive(max) => {
            for value in values {
                if let Some(v) = validate_max_inclusive(value, max) {
                    violations.push(v);
                }
            }
        }
        Constraint::MinExclusive(min) => {
            for value in values {
                if let Some(v) = validate_min_exclusive(value, min) {
                    violations.push(v);
                }
            }
        }
        Constraint::MaxExclusive(max) => {
            for value in values {
                if let Some(v) = validate_max_exclusive(value, max) {
                    violations.push(v);
                }
            }
        }
        Constraint::Pattern(pattern, flags) => {
            for value in values {
                if let Some(v) = validate_pattern(value, pattern, flags.as_deref())? {
                    violations.push(v);
                }
            }
        }
        Constraint::MinLength(min) => {
            for value in values {
                if let Some(v) = validate_min_length(value, *min) {
                    violations.push(v);
                }
            }
        }
        Constraint::MaxLength(max) => {
            for value in values {
                if let Some(v) = validate_max_length(value, *max) {
                    violations.push(v);
                }
            }
        }
        Constraint::In(allowed) => {
            for value in values {
                if let Some(v) = validate_in(value, allowed) {
                    violations.push(v);
                }
            }
        }

        // Pair constraints - these require access to another property's values
        // They are validated separately in validate_property_shape_with_context
        Constraint::Equals(_)
        | Constraint::Disjoint(_)
        | Constraint::LessThan(_)
        | Constraint::LessThanOrEquals(_) => {
            // Handled in validate_property_shape where we have access to the db
        }

        // Language constraints
        // Note: Language tags are stored in the flake's datatype field (rdf:langString)
        // with the language as a separate attribute. Full validation requires access to
        // language metadata which is not available in this simplified validation path.
        Constraint::UniqueLang(_unique) => {
            // TODO: Implement when language metadata is available
            // Requires checking the language tag from flake metadata, not FlakeValue
        }
        Constraint::LanguageIn(_allowed_langs) => {
            // TODO: Implement when language metadata is available
            // Requires checking the language tag from flake metadata, not FlakeValue
        }

        // Qualified value shape - requires nested validation
        Constraint::QualifiedValueShape { .. } => {
            // TODO: Implement qualified value shape validation
            // This requires recursive shape validation
        }
    }

    Ok(violations)
}

/// SHACL validation report
#[derive(Debug, Clone)]
pub struct ValidationReport {
    /// Whether all shapes conform (no Violation-level results)
    pub conforms: bool,
    /// Individual validation results
    pub results: Vec<ValidationResult>,
}

impl ValidationReport {
    /// Create an empty conforming report
    pub fn conforming() -> Self {
        Self {
            conforms: true,
            results: Vec::new(),
        }
    }

    /// Count violations (Severity::Violation results)
    pub fn violation_count(&self) -> usize {
        self.results
            .iter()
            .filter(|r| r.severity == Severity::Violation)
            .count()
    }

    /// Count warnings (Severity::Warning results)
    pub fn warning_count(&self) -> usize {
        self.results
            .iter()
            .filter(|r| r.severity == Severity::Warning)
            .count()
    }
}

/// Individual validation result
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// The focus node that was validated
    pub focus_node: Sid,
    /// The property path (if property constraint)
    pub result_path: Option<Sid>,
    /// The shape that produced this result
    pub source_shape: Sid,
    /// The constraint component that produced this result
    pub source_constraint: Option<Sid>,
    /// Severity level
    pub severity: Severity,
    /// Human-readable message
    pub message: String,
    /// The value that caused the violation (if applicable)
    pub value: Option<FlakeValue>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::ShaclCacheKey;

    #[test]
    fn test_engine_no_shapes_optimization() {
        // Create an empty cache (no shapes)
        let key = ShaclCacheKey::new("test", 1);
        let cache = ShaclCache::new(key, vec![], None);
        let engine = ShaclEngine::new(cache);

        // Engine should report no shapes
        assert!(!engine.has_shapes());
        assert!(engine.is_empty());
        assert_eq!(engine.shape_count(), 0);
    }

    #[test]
    fn test_engine_with_shapes() {
        use crate::compile::{CompiledShape, TargetType};
        use fluree_db_core::SidInterner;

        let interner = SidInterner::new();
        let shape = CompiledShape {
            id: interner.intern(100, "TestShape"),
            targets: vec![TargetType::Node(vec![interner.intern(100, "ex:alice")])],
            property_shapes: vec![],
            node_constraints: vec![],
            structural_constraints: vec![],
            severity: Severity::Violation,
            name: None,
            message: None,
            deactivated: false,
        };

        let key = ShaclCacheKey::new("test", 1);
        let cache = ShaclCache::new(key, vec![shape], None);
        let engine = ShaclEngine::new(cache);

        // Engine should report having shapes
        assert!(engine.has_shapes());
        assert!(!engine.is_empty());
        assert_eq!(engine.shape_count(), 1);
    }

    #[tokio::test]
    async fn test_validate_staged_empty_shapes_returns_conforming() {
        // This is the key optimization test:
        // When there are no shapes, validate_staged should return immediately
        // without doing any database work.

        use fluree_db_core::{Db, MemoryStorage, NoCache};

        let storage = MemoryStorage::new();
        let cache = NoCache::new();
        let db = Db::genesis(storage, cache, "test:main");

        // Empty cache (no shapes)
        let key = ShaclCacheKey::new("test", 1);
        let shacl_cache = ShaclCache::new(key, vec![], None);
        let engine = ShaclEngine::new(shacl_cache);

        // Even with subjects to validate, should return conforming immediately
        let mut modified_subjects = HashSet::new();
        modified_subjects.insert(db.sid_interner.intern(100, "ex:alice"));
        modified_subjects.insert(db.sid_interner.intern(100, "ex:bob"));

        let report = engine
            .validate_staged(&db, &NoOverlay, &modified_subjects)
            .await
            .expect("validation should succeed");

        // Should conform (no shapes = nothing to violate)
        assert!(report.conforms);
        assert_eq!(report.results.len(), 0);
    }

    #[tokio::test]
    async fn test_validate_staged_empty_subjects_returns_conforming() {
        use fluree_db_core::{Db, MemoryStorage, NoCache};

        let storage = MemoryStorage::new();
        let cache = NoCache::new();
        let db = Db::genesis(storage, cache, "test:main");

        // Even with shapes, if no subjects modified, should return conforming
        use crate::compile::{CompiledShape, TargetType};

        let shape = CompiledShape {
            id: db.sid_interner.intern(100, "TestShape"),
            targets: vec![TargetType::Class(db.sid_interner.intern(100, "ex:Person"))],
            property_shapes: vec![],
            node_constraints: vec![],
            structural_constraints: vec![],
            severity: Severity::Violation,
            name: None,
            message: None,
            deactivated: false,
        };

        let key = ShaclCacheKey::new("test", 1);
        let shacl_cache = ShaclCache::new(key, vec![shape], None);
        let engine = ShaclEngine::new(shacl_cache);

        // Empty subject set
        let modified_subjects = HashSet::new();

        let report = engine
            .validate_staged(&db, &NoOverlay, &modified_subjects)
            .await
            .expect("validation should succeed");

        // Should conform (no subjects = nothing to validate)
        assert!(report.conforms);
        assert_eq!(report.results.len(), 0);
    }

    #[test]
    fn test_validation_report_conforming() {
        let report = ValidationReport::conforming();
        assert!(report.conforms);
        assert_eq!(report.results.len(), 0);
        assert_eq!(report.violation_count(), 0);
        assert_eq!(report.warning_count(), 0);
    }
}
