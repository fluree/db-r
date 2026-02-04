//! SHACL validation engine for Fluree DB
//!
//! This crate provides SHACL (Shapes Constraint Language) validation for RDF data
//! in Fluree databases. It supports validation of node shapes and property shapes
//! against a focus node set.
//!
//! # Overview
//!
//! SHACL validation works by:
//! 1. Compiling shape definitions from database flakes into `CompiledShape` structures
//! 2. Determining target nodes for each shape (via `sh:targetClass`, `sh:targetNode`, etc.)
//! 3. Validating each focus node against applicable shape constraints
//! 4. Producing a `ValidationReport` with conformance status and any violations
//!
//! # Supported Constraints
//!
//! Currently supported constraint types:
//! - Cardinality: `sh:minCount`, `sh:maxCount`
//! - Value type: `sh:datatype`, `sh:nodeKind`, `sh:class`
//! - Value range: `sh:minInclusive`, `sh:maxInclusive`, `sh:minExclusive`, `sh:maxExclusive`
//! - String: `sh:pattern`, `sh:minLength`, `sh:maxLength`
//! - Value: `sh:hasValue`, `sh:in`
//! - Closed: `sh:closed`, `sh:ignoredProperties`
//! - Pair: `sh:equals`, `sh:disjoint`, `sh:lessThan`, `sh:lessThanOrEquals`
//! - Logical: `sh:not`, `sh:and`, `sh:or`, `sh:xone`
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_shacl::{ShaclEngine, ValidationReport};
//!
//! // Build SHACL engine from database shapes
//! let engine = ShaclEngine::from_db(&db).await?;
//!
//! // Validate a staged transaction view
//! let report = engine.validate(&view).await?;
//!
//! if !report.conforms {
//!     for violation in &report.results {
//!         println!("Violation: {:?}", violation);
//!     }
//! }
//! ```

pub mod cache;
pub mod compile;
pub mod constraints;
pub mod error;
pub mod validate;

pub use cache::{ShaclCache, ShaclCacheKey};
pub use compile::{CompiledShape, PropertyShape, Severity, ShapeId, TargetType};
pub use constraints::Constraint;
pub use error::{Result, ShaclError};
pub use validate::{ShaclEngine, ValidationReport, ValidationResult};

/// SHACL namespace code (re-exported from fluree-vocab)
pub use fluree_vocab::namespaces::SHACL;

/// SHACL vocabulary full IRIs (re-exported from fluree-vocab)
pub use fluree_vocab::shacl;

/// Well-known SHACL predicate local names (re-exported from fluree-vocab)
///
/// These are the local name portions of SHACL predicates, used for SID construction.
/// For full IRIs, use the `shacl` module instead.
pub use fluree_vocab::shacl_names as predicates;
