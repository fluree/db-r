//! RDF Vocabulary Constants and Namespace Codes for Fluree
//!
//! This crate provides a centralized location for RDF vocabulary IRIs,
//! namespace codes, and other common constants used throughout the Fluree ecosystem.
//!
//! # Organization
//!
//! Constants are organized by vocabulary:
//! - `rdf` - RDF vocabulary (http://www.w3.org/1999/02/22-rdf-syntax-ns#)
//! - `rdfs` - RDFS vocabulary (http://www.w3.org/2000/01/rdf-schema#)
//! - `xsd` - XSD vocabulary (http://www.w3.org/2001/XMLSchema#)
//! - `owl` - OWL vocabulary (http://www.w3.org/2002/07/owl#)
//! - `namespaces` - Namespace codes used for IRI encoding
//! - `errors` - Error type compact IRIs for API responses

pub mod errors;

/// RDF vocabulary constants
pub mod rdf {
    /// rdf:type IRI
    pub const TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

    /// rdf:langString IRI
    pub const LANG_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";

    /// rdf:JSON IRI
    pub const JSON: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON";

    /// rdf:first IRI (RDF list head)
    pub const FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";

    /// rdf:rest IRI (RDF list tail)
    pub const REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";

    /// rdf:nil IRI (RDF list terminator)
    pub const NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
}

/// RDFS vocabulary constants
pub mod rdfs {
    /// rdfs:subClassOf IRI
    pub const SUB_CLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";

    /// rdfs:subPropertyOf IRI
    pub const SUB_PROPERTY_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";

    /// rdfs:domain IRI
    pub const DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";

    /// rdfs:range IRI
    pub const RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
}

/// XSD vocabulary constants
pub mod xsd {
    /// xsd:string IRI
    pub const STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

    /// xsd:integer IRI
    pub const INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

    /// xsd:long IRI
    pub const LONG: &str = "http://www.w3.org/2001/XMLSchema#long";

    /// xsd:int IRI
    pub const INT: &str = "http://www.w3.org/2001/XMLSchema#int";

    /// xsd:short IRI
    pub const SHORT: &str = "http://www.w3.org/2001/XMLSchema#short";

    /// xsd:byte IRI
    pub const BYTE: &str = "http://www.w3.org/2001/XMLSchema#byte";

    /// xsd:unsignedLong IRI
    pub const UNSIGNED_LONG: &str = "http://www.w3.org/2001/XMLSchema#unsignedLong";

    /// xsd:unsignedInt IRI
    pub const UNSIGNED_INT: &str = "http://www.w3.org/2001/XMLSchema#unsignedInt";

    /// xsd:unsignedShort IRI
    pub const UNSIGNED_SHORT: &str = "http://www.w3.org/2001/XMLSchema#unsignedShort";

    /// xsd:unsignedByte IRI
    pub const UNSIGNED_BYTE: &str = "http://www.w3.org/2001/XMLSchema#unsignedByte";

    /// xsd:nonNegativeInteger IRI
    pub const NON_NEGATIVE_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#nonNegativeInteger";

    /// xsd:positiveInteger IRI
    pub const POSITIVE_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#positiveInteger";

    /// xsd:nonPositiveInteger IRI
    pub const NON_POSITIVE_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#nonPositiveInteger";

    /// xsd:negativeInteger IRI
    pub const NEGATIVE_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#negativeInteger";

    /// xsd:decimal IRI
    pub const DECIMAL: &str = "http://www.w3.org/2001/XMLSchema#decimal";

    /// xsd:float IRI
    pub const FLOAT: &str = "http://www.w3.org/2001/XMLSchema#float";

    /// xsd:double IRI
    pub const DOUBLE: &str = "http://www.w3.org/2001/XMLSchema#double";

    /// xsd:boolean IRI
    pub const BOOLEAN: &str = "http://www.w3.org/2001/XMLSchema#boolean";

    /// xsd:dateTime IRI
    pub const DATE_TIME: &str = "http://www.w3.org/2001/XMLSchema#dateTime";

    /// xsd:date IRI
    pub const DATE: &str = "http://www.w3.org/2001/XMLSchema#date";

    /// xsd:time IRI
    pub const TIME: &str = "http://www.w3.org/2001/XMLSchema#time";

    /// xsd:gYear IRI
    pub const G_YEAR: &str = "http://www.w3.org/2001/XMLSchema#gYear";

    /// xsd:gYearMonth IRI
    pub const G_YEAR_MONTH: &str = "http://www.w3.org/2001/XMLSchema#gYearMonth";

    /// xsd:gMonth IRI
    pub const G_MONTH: &str = "http://www.w3.org/2001/XMLSchema#gMonth";

    /// xsd:gDay IRI
    pub const G_DAY: &str = "http://www.w3.org/2001/XMLSchema#gDay";

    /// xsd:gMonthDay IRI
    pub const G_MONTH_DAY: &str = "http://www.w3.org/2001/XMLSchema#gMonthDay";

    /// xsd:duration IRI
    pub const DURATION: &str = "http://www.w3.org/2001/XMLSchema#duration";

    /// xsd:dayTimeDuration IRI
    pub const DAY_TIME_DURATION: &str = "http://www.w3.org/2001/XMLSchema#dayTimeDuration";

    /// xsd:yearMonthDuration IRI
    pub const YEAR_MONTH_DURATION: &str = "http://www.w3.org/2001/XMLSchema#yearMonthDuration";

    /// xsd:anyURI IRI
    pub const ANY_URI: &str = "http://www.w3.org/2001/XMLSchema#anyURI";

    /// xsd:normalizedString IRI
    pub const NORMALIZED_STRING: &str = "http://www.w3.org/2001/XMLSchema#normalizedString";

    /// xsd:token IRI
    pub const TOKEN: &str = "http://www.w3.org/2001/XMLSchema#token";

    /// xsd:language IRI
    pub const LANGUAGE: &str = "http://www.w3.org/2001/XMLSchema#language";

    /// xsd:base64Binary IRI
    pub const BASE64_BINARY: &str = "http://www.w3.org/2001/XMLSchema#base64Binary";

    /// xsd:hexBinary IRI
    pub const HEX_BINARY: &str = "http://www.w3.org/2001/XMLSchema#hexBinary";

    // ========================================================================
    // Datatype Normalization Helpers
    // ========================================================================
    //
    // These functions normalize XSD datatypes to canonical forms for storage.
    // This ensures consistency between transact and query paths.

    /// Normalize integer-family datatypes to xsd:integer
    ///
    /// XSD defines a type hierarchy where int, short, byte, long are subtypes
    /// of integer. For storage consistency, we normalize all of these to
    /// xsd:integer since they all map to the same Rust type (i64 or BigInt).
    ///
    /// # Arguments
    /// * `datatype_iri` - The full IRI of the datatype
    ///
    /// # Returns
    /// * `xsd:integer` IRI if input is an integer-family type
    /// * The original IRI unchanged otherwise
    #[inline]
    pub fn normalize_integer_family(datatype_iri: &str) -> &str {
        match datatype_iri {
            LONG
            | INT
            | SHORT
            | BYTE
            | UNSIGNED_LONG
            | UNSIGNED_INT
            | UNSIGNED_SHORT
            | UNSIGNED_BYTE
            | NON_NEGATIVE_INTEGER
            | POSITIVE_INTEGER
            | NON_POSITIVE_INTEGER
            | NEGATIVE_INTEGER => INTEGER,
            _ => datatype_iri,
        }
    }

    /// Normalize float to double
    ///
    /// XSD float and double both map to f64 in Rust. Normalize to double
    /// for storage consistency.
    #[inline]
    pub fn normalize_float_family(datatype_iri: &str) -> &str {
        match datatype_iri {
            FLOAT => DOUBLE,
            _ => datatype_iri,
        }
    }

    /// Normalize all numeric datatypes to their canonical storage form
    ///
    /// Combines integer-family and float-family normalization:
    /// - xsd:int, xsd:short, xsd:byte, xsd:long → xsd:integer
    /// - xsd:float → xsd:double
    /// - All other types pass through unchanged
    #[inline]
    pub fn normalize_numeric_datatype(datatype_iri: &str) -> &str {
        match datatype_iri {
            LONG
            | INT
            | SHORT
            | BYTE
            | UNSIGNED_LONG
            | UNSIGNED_INT
            | UNSIGNED_SHORT
            | UNSIGNED_BYTE
            | NON_NEGATIVE_INTEGER
            | POSITIVE_INTEGER
            | NON_POSITIVE_INTEGER
            | NEGATIVE_INTEGER => INTEGER,
            FLOAT => DOUBLE,
            _ => datatype_iri,
        }
    }

    /// Check if a datatype IRI is a numeric type
    #[inline]
    pub fn is_numeric_datatype(datatype_iri: &str) -> bool {
        matches!(
            datatype_iri,
            INTEGER
                | LONG
                | INT
                | SHORT
                | BYTE
                | UNSIGNED_LONG
                | UNSIGNED_INT
                | UNSIGNED_SHORT
                | UNSIGNED_BYTE
                | NON_NEGATIVE_INTEGER
                | POSITIVE_INTEGER
                | NON_POSITIVE_INTEGER
                | NEGATIVE_INTEGER
                | DECIMAL
                | FLOAT
                | DOUBLE
        )
    }

    /// Check if a datatype IRI is an integer-family type
    #[inline]
    pub fn is_integer_family(datatype_iri: &str) -> bool {
        matches!(
            datatype_iri,
            INTEGER
                | LONG
                | INT
                | SHORT
                | BYTE
                | UNSIGNED_LONG
                | UNSIGNED_INT
                | UNSIGNED_SHORT
                | UNSIGNED_BYTE
                | NON_NEGATIVE_INTEGER
                | POSITIVE_INTEGER
                | NON_POSITIVE_INTEGER
                | NEGATIVE_INTEGER
        )
    }

    /// Check if a datatype IRI is a string-like type
    ///
    /// String-like types are those that can hold string values and should not
    /// accept implicit coercion from numbers or booleans.
    #[inline]
    pub fn is_string_like(datatype_iri: &str) -> bool {
        matches!(
            datatype_iri,
            STRING | NORMALIZED_STRING | TOKEN | LANGUAGE | ANY_URI
        )
    }

    /// Check if a datatype IRI is a temporal type
    #[inline]
    pub fn is_temporal(datatype_iri: &str) -> bool {
        matches!(datatype_iri, DATE_TIME | DATE | TIME)
    }

    // ========================================================================
    // Integer Range Validation
    // ========================================================================

    /// Get the valid range bounds for an integer subtype as (min, max) inclusive.
    ///
    /// Returns `None` for unbounded types (xsd:integer) or non-integer types.
    /// Uses i128 to accommodate the full range of xsd:unsignedLong.
    ///
    /// # Practical Limitation
    ///
    /// Per XSD spec, sign-constrained types (`positiveInteger`, `nonNegativeInteger`,
    /// `negativeInteger`, `nonPositiveInteger`) are semantically unbounded—they only
    /// constrain the sign, not the magnitude. However, for practical purposes, we
    /// bound them to the i128 range. Values outside this range will be rejected.
    ///
    /// Use `xsd:integer` (returns `None`) for truly unbounded integers.
    #[inline]
    pub fn integer_bounds(datatype_iri: &str) -> Option<(i128, i128)> {
        match datatype_iri {
            BYTE => Some((i8::MIN as i128, i8::MAX as i128)),
            SHORT => Some((i16::MIN as i128, i16::MAX as i128)),
            INT => Some((i32::MIN as i128, i32::MAX as i128)),
            LONG => Some((i64::MIN as i128, i64::MAX as i128)),
            UNSIGNED_BYTE => Some((0, u8::MAX as i128)),
            UNSIGNED_SHORT => Some((0, u16::MAX as i128)),
            UNSIGNED_INT => Some((0, u32::MAX as i128)),
            UNSIGNED_LONG => Some((0, u64::MAX as i128)),
            // Sign-constrained types: semantically unbounded per XSD, but we bound
            // to i128 range for practical purposes (see doc comment above)
            POSITIVE_INTEGER => Some((1, i128::MAX)),
            NON_NEGATIVE_INTEGER => Some((0, i128::MAX)),
            NEGATIVE_INTEGER => Some((i128::MIN, -1)),
            NON_POSITIVE_INTEGER => Some((i128::MIN, 0)),
            // xsd:integer is truly unbounded
            INTEGER => None,
            _ => None,
        }
    }

    /// Validate that an i64 value is within the valid range for the given integer datatype.
    ///
    /// Returns `Ok(())` if valid, or `Err` with a descriptive message if out of range.
    pub fn validate_integer_range_i64(datatype_iri: &str, value: i64) -> Result<(), String> {
        if let Some((min, max)) = integer_bounds(datatype_iri) {
            let v = value as i128;
            if v < min || v > max {
                return Err(format!(
                    "Value {} is out of range for {}: expected {} to {}",
                    value,
                    datatype_local_name(datatype_iri).unwrap_or(datatype_iri),
                    min,
                    max
                ));
            }
        }
        Ok(())
    }

    /// Validate that an i128 value is within the valid range for the given integer datatype.
    ///
    /// This is useful for BigInt values that may exceed i64 range.
    pub fn validate_integer_range_i128(datatype_iri: &str, value: i128) -> Result<(), String> {
        if let Some((min, max)) = integer_bounds(datatype_iri) {
            if value < min || value > max {
                return Err(format!(
                    "Value {} is out of range for {}: expected {} to {}",
                    value,
                    datatype_local_name(datatype_iri).unwrap_or(datatype_iri),
                    min,
                    max
                ));
            }
        }
        Ok(())
    }

    /// Get the local name portion of a datatype IRI (e.g., "integer" from xsd:integer)
    #[inline]
    pub fn datatype_local_name(datatype_iri: &str) -> Option<&str> {
        datatype_iri.rsplit('#').next()
    }
}

/// XSD datatype local names (for SID construction)
///
/// XSD datatypes are encoded with namespace code 2 (XSD).
/// These constants provide the local name portion for constructing SIDs.
///
/// # Example
/// ```
/// use fluree_vocab::xsd_names;
///
/// assert_eq!(xsd_names::STRING, "string");
/// assert_eq!(xsd_names::LONG, "long");
/// ```
pub mod xsd_names {
    /// xsd:string local name
    pub const STRING: &str = "string";

    /// xsd:integer local name
    pub const INTEGER: &str = "integer";

    /// xsd:long local name
    pub const LONG: &str = "long";

    /// xsd:int local name
    pub const INT: &str = "int";

    /// xsd:short local name
    pub const SHORT: &str = "short";

    /// xsd:byte local name
    pub const BYTE: &str = "byte";

        /// xsd:unsignedLong local name
        pub const UNSIGNED_LONG: &str = "unsignedLong";

        /// xsd:unsignedInt local name
        pub const UNSIGNED_INT: &str = "unsignedInt";

        /// xsd:unsignedShort local name
        pub const UNSIGNED_SHORT: &str = "unsignedShort";

        /// xsd:unsignedByte local name
        pub const UNSIGNED_BYTE: &str = "unsignedByte";

        /// xsd:nonNegativeInteger local name
        pub const NON_NEGATIVE_INTEGER: &str = "nonNegativeInteger";

        /// xsd:positiveInteger local name
        pub const POSITIVE_INTEGER: &str = "positiveInteger";

        /// xsd:nonPositiveInteger local name
        pub const NON_POSITIVE_INTEGER: &str = "nonPositiveInteger";

        /// xsd:negativeInteger local name
        pub const NEGATIVE_INTEGER: &str = "negativeInteger";

    /// xsd:decimal local name
    pub const DECIMAL: &str = "decimal";

    /// xsd:float local name
    pub const FLOAT: &str = "float";

    /// xsd:double local name
    pub const DOUBLE: &str = "double";

    /// xsd:boolean local name
    pub const BOOLEAN: &str = "boolean";

    /// xsd:dateTime local name
    pub const DATE_TIME: &str = "dateTime";

    /// xsd:date local name
    pub const DATE: &str = "date";

    /// xsd:time local name
    pub const TIME: &str = "time";

    /// xsd:duration local name
    pub const DURATION: &str = "duration";

    /// xsd:dayTimeDuration local name
    pub const DAY_TIME_DURATION: &str = "dayTimeDuration";

    /// xsd:yearMonthDuration local name
    pub const YEAR_MONTH_DURATION: &str = "yearMonthDuration";

    /// xsd:anyURI local name
    pub const ANY_URI: &str = "anyURI";

        /// xsd:normalizedString local name
        pub const NORMALIZED_STRING: &str = "normalizedString";

        /// xsd:token local name
        pub const TOKEN: &str = "token";

        /// xsd:language local name
        pub const LANGUAGE: &str = "language";

    /// xsd:base64Binary local name
    pub const BASE64_BINARY: &str = "base64Binary";

    /// xsd:hexBinary local name
    pub const HEX_BINARY: &str = "hexBinary";

    /// xsd:gYear local name
    pub const G_YEAR: &str = "gYear";

    /// xsd:gMonth local name
    pub const G_MONTH: &str = "gMonth";

    /// xsd:gDay local name
    pub const G_DAY: &str = "gDay";

    /// xsd:gYearMonth local name
    pub const G_YEAR_MONTH: &str = "gYearMonth";

    /// xsd:gMonthDay local name
    pub const G_MONTH_DAY: &str = "gMonthDay";

    // ========================================================================
    // Classification Helpers (for SID-based datatype checking)
    // ========================================================================

    /// Check if a local name is an integer-family type
    ///
    /// This is useful when you have a SID and want to check the datatype
    /// without reconstructing the full IRI.
    #[inline]
    pub fn is_integer_family_name(name: &str) -> bool {
        matches!(
            name,
            INTEGER
                | LONG
                | INT
                | SHORT
                | BYTE
                | UNSIGNED_LONG
                | UNSIGNED_INT
                | UNSIGNED_SHORT
                | UNSIGNED_BYTE
                | NON_NEGATIVE_INTEGER
                | POSITIVE_INTEGER
                | NON_POSITIVE_INTEGER
                | NEGATIVE_INTEGER
        )
    }

    /// Check if a local name is a string-like type
    #[inline]
    pub fn is_string_like_name(name: &str) -> bool {
        matches!(name, STRING | NORMALIZED_STRING | TOKEN | LANGUAGE | ANY_URI)
    }

    /// Check if a local name is a temporal type
    #[inline]
    pub fn is_temporal_name(name: &str) -> bool {
        matches!(name, DATE_TIME | DATE | TIME)
    }
}

/// RDF vocabulary local names (for SID construction)
///
/// RDF terms are encoded with namespace code 3 (RDF).
pub mod rdf_names {
    /// rdf:type local name
    pub const TYPE: &str = "type";

    /// rdf:langString local name
    pub const LANG_STRING: &str = "langString";

    /// rdf:JSON local name
    pub const JSON: &str = "JSON";

    /// rdf:first local name
    pub const FIRST: &str = "first";

    /// rdf:rest local name
    pub const REST: &str = "rest";

    /// rdf:nil local name
    pub const NIL: &str = "nil";
}

/// OWL vocabulary constants
pub mod owl {
    /// owl:inverseOf IRI
    pub const INVERSE_OF: &str = "http://www.w3.org/2002/07/owl#inverseOf";

    /// owl:equivalentClass IRI
    pub const EQUIVALENT_CLASS: &str = "http://www.w3.org/2002/07/owl#equivalentClass";

    /// owl:equivalentProperty IRI
    pub const EQUIVALENT_PROPERTY: &str = "http://www.w3.org/2002/07/owl#equivalentProperty";

    /// owl:sameAs IRI
    pub const SAME_AS: &str = "http://www.w3.org/2002/07/owl#sameAs";

    /// owl:SymmetricProperty IRI
    pub const SYMMETRIC_PROPERTY: &str = "http://www.w3.org/2002/07/owl#SymmetricProperty";

    /// owl:TransitiveProperty IRI
    pub const TRANSITIVE_PROPERTY: &str = "http://www.w3.org/2002/07/owl#TransitiveProperty";

    /// owl:FunctionalProperty IRI
    pub const FUNCTIONAL_PROPERTY: &str = "http://www.w3.org/2002/07/owl#FunctionalProperty";

    /// owl:InverseFunctionalProperty IRI
    pub const INVERSE_FUNCTIONAL_PROPERTY: &str = "http://www.w3.org/2002/07/owl#InverseFunctionalProperty";

    /// owl:propertyChainAxiom IRI
    pub const PROPERTY_CHAIN_AXIOM: &str = "http://www.w3.org/2002/07/owl#propertyChainAxiom";

    /// owl:hasKey IRI
    pub const HAS_KEY: &str = "http://www.w3.org/2002/07/owl#hasKey";

    /// owl:Restriction IRI
    pub const RESTRICTION: &str = "http://www.w3.org/2002/07/owl#Restriction";

    /// owl:onProperty IRI
    pub const ON_PROPERTY: &str = "http://www.w3.org/2002/07/owl#onProperty";

    /// owl:hasValue IRI
    pub const HAS_VALUE: &str = "http://www.w3.org/2002/07/owl#hasValue";

    /// owl:someValuesFrom IRI
    pub const SOME_VALUES_FROM: &str = "http://www.w3.org/2002/07/owl#someValuesFrom";

    /// owl:allValuesFrom IRI
    pub const ALL_VALUES_FROM: &str = "http://www.w3.org/2002/07/owl#allValuesFrom";

    /// owl:maxCardinality IRI
    pub const MAX_CARDINALITY: &str = "http://www.w3.org/2002/07/owl#maxCardinality";

    /// owl:maxQualifiedCardinality IRI
    pub const MAX_QUALIFIED_CARDINALITY: &str = "http://www.w3.org/2002/07/owl#maxQualifiedCardinality";

    /// owl:onClass IRI
    pub const ON_CLASS: &str = "http://www.w3.org/2002/07/owl#onClass";

    /// owl:intersectionOf IRI
    pub const INTERSECTION_OF: &str = "http://www.w3.org/2002/07/owl#intersectionOf";

    /// owl:unionOf IRI
    pub const UNION_OF: &str = "http://www.w3.org/2002/07/owl#unionOf";

    /// owl:oneOf IRI
    pub const ONE_OF: &str = "http://www.w3.org/2002/07/owl#oneOf";
}

/// OWL local names (for SID construction)
pub mod owl_names {
    /// owl:inverseOf local name
    pub const INVERSE_OF: &str = "inverseOf";

    /// owl:equivalentClass local name
    pub const EQUIVALENT_CLASS: &str = "equivalentClass";

    /// owl:equivalentProperty local name
    pub const EQUIVALENT_PROPERTY: &str = "equivalentProperty";

    /// owl:sameAs local name
    pub const SAME_AS: &str = "sameAs";

    /// owl:SymmetricProperty local name
    pub const SYMMETRIC_PROPERTY: &str = "SymmetricProperty";

    /// owl:TransitiveProperty local name
    pub const TRANSITIVE_PROPERTY: &str = "TransitiveProperty";

    /// owl:FunctionalProperty local name
    pub const FUNCTIONAL_PROPERTY: &str = "FunctionalProperty";

    /// owl:InverseFunctionalProperty local name
    pub const INVERSE_FUNCTIONAL_PROPERTY: &str = "InverseFunctionalProperty";

    /// owl:propertyChainAxiom local name
    pub const PROPERTY_CHAIN_AXIOM: &str = "propertyChainAxiom";

    /// owl:hasKey local name
    pub const HAS_KEY: &str = "hasKey";

    /// owl:Restriction local name
    pub const RESTRICTION: &str = "Restriction";

    /// owl:onProperty local name
    pub const ON_PROPERTY: &str = "onProperty";

    /// owl:hasValue local name
    pub const HAS_VALUE: &str = "hasValue";

    /// owl:someValuesFrom local name
    pub const SOME_VALUES_FROM: &str = "someValuesFrom";

    /// owl:allValuesFrom local name
    pub const ALL_VALUES_FROM: &str = "allValuesFrom";

    /// owl:maxCardinality local name
    pub const MAX_CARDINALITY: &str = "maxCardinality";

    /// owl:maxQualifiedCardinality local name
    pub const MAX_QUALIFIED_CARDINALITY: &str = "maxQualifiedCardinality";

    /// owl:onClass local name
    pub const ON_CLASS: &str = "onClass";

    /// owl:intersectionOf local name
    pub const INTERSECTION_OF: &str = "intersectionOf";

    /// owl:unionOf local name
    pub const UNION_OF: &str = "unionOf";

    /// owl:oneOf local name
    pub const ONE_OF: &str = "oneOf";
}

/// JSON-LD keyword local names (for SID construction)
///
/// JSON-LD keywords like `@id`, `@type`, `@value` are encoded with namespace code 1 (JSON_LD).
/// These constants provide the local name portion for constructing SIDs.
pub mod jsonld_names {
    /// JSON-LD @id keyword local name
    ///
    /// Used as the datatype for Ref values (IRI references).
    /// SID construction: `Sid::new(namespaces::JSON_LD, ID)` → `$id`
    pub const ID: &str = "id";

    /// JSON-LD @type keyword local name
    pub const TYPE: &str = "type";

    /// JSON-LD @value keyword local name
    pub const VALUE: &str = "value";

    /// JSON-LD @language keyword local name
    pub const LANGUAGE: &str = "language";

    /// JSON-LD @graph keyword local name
    pub const GRAPH: &str = "graph";

    /// JSON-LD @context keyword local name
    pub const CONTEXT: &str = "context";
}

/// SHACL vocabulary constants
pub mod shacl {
    /// SHACL namespace IRI
    pub const NS: &str = "http://www.w3.org/ns/shacl#";

    // ========================================================================
    // Shape Classes
    // ========================================================================

    /// sh:NodeShape IRI
    pub const NODE_SHAPE: &str = "http://www.w3.org/ns/shacl#NodeShape";

    /// sh:PropertyShape IRI
    pub const PROPERTY_SHAPE: &str = "http://www.w3.org/ns/shacl#PropertyShape";

    // ========================================================================
    // Targeting
    // ========================================================================

    /// sh:targetClass IRI
    pub const TARGET_CLASS: &str = "http://www.w3.org/ns/shacl#targetClass";

    /// sh:targetNode IRI
    pub const TARGET_NODE: &str = "http://www.w3.org/ns/shacl#targetNode";

    /// sh:targetSubjectsOf IRI
    pub const TARGET_SUBJECTS_OF: &str = "http://www.w3.org/ns/shacl#targetSubjectsOf";

    /// sh:targetObjectsOf IRI
    pub const TARGET_OBJECTS_OF: &str = "http://www.w3.org/ns/shacl#targetObjectsOf";

    // ========================================================================
    // Property Shape
    // ========================================================================

    /// sh:property IRI
    pub const PROPERTY: &str = "http://www.w3.org/ns/shacl#property";

    /// sh:path IRI
    pub const PATH: &str = "http://www.w3.org/ns/shacl#path";

    // ========================================================================
    // Cardinality Constraints
    // ========================================================================

    /// sh:minCount IRI
    pub const MIN_COUNT: &str = "http://www.w3.org/ns/shacl#minCount";

    /// sh:maxCount IRI
    pub const MAX_COUNT: &str = "http://www.w3.org/ns/shacl#maxCount";

    // ========================================================================
    // Value Type Constraints
    // ========================================================================

    /// sh:datatype IRI
    pub const DATATYPE: &str = "http://www.w3.org/ns/shacl#datatype";

    /// sh:nodeKind IRI
    pub const NODE_KIND: &str = "http://www.w3.org/ns/shacl#nodeKind";

    /// sh:class IRI
    pub const CLASS: &str = "http://www.w3.org/ns/shacl#class";

    // ========================================================================
    // Value Range Constraints
    // ========================================================================

    /// sh:minInclusive IRI
    pub const MIN_INCLUSIVE: &str = "http://www.w3.org/ns/shacl#minInclusive";

    /// sh:maxInclusive IRI
    pub const MAX_INCLUSIVE: &str = "http://www.w3.org/ns/shacl#maxInclusive";

    /// sh:minExclusive IRI
    pub const MIN_EXCLUSIVE: &str = "http://www.w3.org/ns/shacl#minExclusive";

    /// sh:maxExclusive IRI
    pub const MAX_EXCLUSIVE: &str = "http://www.w3.org/ns/shacl#maxExclusive";

    // ========================================================================
    // String Constraints
    // ========================================================================

    /// sh:pattern IRI
    pub const PATTERN: &str = "http://www.w3.org/ns/shacl#pattern";

    /// sh:flags IRI
    pub const FLAGS: &str = "http://www.w3.org/ns/shacl#flags";

    /// sh:minLength IRI
    pub const MIN_LENGTH: &str = "http://www.w3.org/ns/shacl#minLength";

    /// sh:maxLength IRI
    pub const MAX_LENGTH: &str = "http://www.w3.org/ns/shacl#maxLength";

    // ========================================================================
    // Value Constraints
    // ========================================================================

    /// sh:hasValue IRI
    pub const HAS_VALUE: &str = "http://www.w3.org/ns/shacl#hasValue";

    /// sh:in IRI
    pub const IN: &str = "http://www.w3.org/ns/shacl#in";

    // ========================================================================
    // Pair Constraints
    // ========================================================================

    /// sh:equals IRI
    pub const EQUALS: &str = "http://www.w3.org/ns/shacl#equals";

    /// sh:disjoint IRI
    pub const DISJOINT: &str = "http://www.w3.org/ns/shacl#disjoint";

    /// sh:lessThan IRI
    pub const LESS_THAN: &str = "http://www.w3.org/ns/shacl#lessThan";

    /// sh:lessThanOrEquals IRI
    pub const LESS_THAN_OR_EQUALS: &str = "http://www.w3.org/ns/shacl#lessThanOrEquals";

    // ========================================================================
    // Closed Shape Constraints
    // ========================================================================

    /// sh:closed IRI
    pub const CLOSED: &str = "http://www.w3.org/ns/shacl#closed";

    /// sh:ignoredProperties IRI
    pub const IGNORED_PROPERTIES: &str = "http://www.w3.org/ns/shacl#ignoredProperties";

    // ========================================================================
    // Logical Constraints
    // ========================================================================

    /// sh:not IRI
    pub const NOT: &str = "http://www.w3.org/ns/shacl#not";

    /// sh:and IRI
    pub const AND: &str = "http://www.w3.org/ns/shacl#and";

    /// sh:or IRI
    pub const OR: &str = "http://www.w3.org/ns/shacl#or";

    /// sh:xone IRI
    pub const XONE: &str = "http://www.w3.org/ns/shacl#xone";

    // ========================================================================
    // Qualified Value Shape
    // ========================================================================

    /// sh:qualifiedValueShape IRI
    pub const QUALIFIED_VALUE_SHAPE: &str = "http://www.w3.org/ns/shacl#qualifiedValueShape";

    /// sh:qualifiedMinCount IRI
    pub const QUALIFIED_MIN_COUNT: &str = "http://www.w3.org/ns/shacl#qualifiedMinCount";

    /// sh:qualifiedMaxCount IRI
    pub const QUALIFIED_MAX_COUNT: &str = "http://www.w3.org/ns/shacl#qualifiedMaxCount";

    /// sh:qualifiedValueShapesDisjoint IRI
    pub const QUALIFIED_VALUE_SHAPES_DISJOINT: &str = "http://www.w3.org/ns/shacl#qualifiedValueShapesDisjoint";

    // ========================================================================
    // Language Constraints
    // ========================================================================

    /// sh:uniqueLang IRI
    pub const UNIQUE_LANG: &str = "http://www.w3.org/ns/shacl#uniqueLang";

    /// sh:languageIn IRI
    pub const LANGUAGE_IN: &str = "http://www.w3.org/ns/shacl#languageIn";

    // ========================================================================
    // Node Kind Values
    // ========================================================================

    /// sh:BlankNode IRI
    pub const BLANK_NODE: &str = "http://www.w3.org/ns/shacl#BlankNode";

    /// sh:IRI IRI
    pub const IRI: &str = "http://www.w3.org/ns/shacl#IRI";

    /// sh:Literal IRI
    pub const LITERAL: &str = "http://www.w3.org/ns/shacl#Literal";

    /// sh:BlankNodeOrIRI IRI
    pub const BLANK_NODE_OR_IRI: &str = "http://www.w3.org/ns/shacl#BlankNodeOrIRI";

    /// sh:BlankNodeOrLiteral IRI
    pub const BLANK_NODE_OR_LITERAL: &str = "http://www.w3.org/ns/shacl#BlankNodeOrLiteral";

    /// sh:IRIOrLiteral IRI
    pub const IRI_OR_LITERAL: &str = "http://www.w3.org/ns/shacl#IRIOrLiteral";

    // ========================================================================
    // Severity Levels
    // ========================================================================

    /// sh:severity IRI
    pub const SEVERITY: &str = "http://www.w3.org/ns/shacl#severity";

    /// sh:Violation IRI
    pub const VIOLATION: &str = "http://www.w3.org/ns/shacl#Violation";

    /// sh:Warning IRI
    pub const WARNING: &str = "http://www.w3.org/ns/shacl#Warning";

    /// sh:Info IRI
    pub const INFO: &str = "http://www.w3.org/ns/shacl#Info";

    // ========================================================================
    // Result Reporting
    // ========================================================================

    /// sh:message IRI
    pub const MESSAGE: &str = "http://www.w3.org/ns/shacl#message";

    /// sh:name IRI
    pub const NAME: &str = "http://www.w3.org/ns/shacl#name";

    /// sh:description IRI
    pub const DESCRIPTION: &str = "http://www.w3.org/ns/shacl#description";

    // ========================================================================
    // Validation Report
    // ========================================================================

    /// sh:ValidationReport IRI
    pub const VALIDATION_REPORT: &str = "http://www.w3.org/ns/shacl#ValidationReport";

    /// sh:ValidationResult IRI
    pub const VALIDATION_RESULT: &str = "http://www.w3.org/ns/shacl#ValidationResult";

    /// sh:conforms IRI
    pub const CONFORMS: &str = "http://www.w3.org/ns/shacl#conforms";

    /// sh:result IRI
    pub const RESULT: &str = "http://www.w3.org/ns/shacl#result";

    /// sh:focusNode IRI
    pub const FOCUS_NODE: &str = "http://www.w3.org/ns/shacl#focusNode";

    /// sh:resultPath IRI
    pub const RESULT_PATH: &str = "http://www.w3.org/ns/shacl#resultPath";

    /// sh:value IRI
    pub const VALUE: &str = "http://www.w3.org/ns/shacl#value";

    /// sh:sourceShape IRI
    pub const SOURCE_SHAPE: &str = "http://www.w3.org/ns/shacl#sourceShape";

    /// sh:sourceConstraintComponent IRI
    pub const SOURCE_CONSTRAINT_COMPONENT: &str = "http://www.w3.org/ns/shacl#sourceConstraintComponent";

    /// sh:resultSeverity IRI
    pub const RESULT_SEVERITY: &str = "http://www.w3.org/ns/shacl#resultSeverity";

    /// sh:resultMessage IRI
    pub const RESULT_MESSAGE: &str = "http://www.w3.org/ns/shacl#resultMessage";
}

/// SHACL vocabulary local names (for SID construction)
///
/// SHACL terms are encoded with namespace code 5 (SHACL).
/// These constants provide the local name portion for constructing SIDs.
///
/// # Example
/// ```
/// use fluree_vocab::shacl_names;
///
/// assert_eq!(shacl_names::TARGET_CLASS, "targetClass");
/// assert_eq!(shacl_names::MIN_COUNT, "minCount");
/// ```
pub mod shacl_names {
    // ========================================================================
    // Shape Classes
    // ========================================================================

    /// sh:NodeShape local name
    pub const NODE_SHAPE: &str = "NodeShape";

    /// sh:PropertyShape local name
    pub const PROPERTY_SHAPE: &str = "PropertyShape";

    // ========================================================================
    // Targeting
    // ========================================================================

    /// sh:targetClass local name
    pub const TARGET_CLASS: &str = "targetClass";

    /// sh:targetNode local name
    pub const TARGET_NODE: &str = "targetNode";

    /// sh:targetSubjectsOf local name
    pub const TARGET_SUBJECTS_OF: &str = "targetSubjectsOf";

    /// sh:targetObjectsOf local name
    pub const TARGET_OBJECTS_OF: &str = "targetObjectsOf";

    // ========================================================================
    // Property Shape
    // ========================================================================

    /// sh:property local name
    pub const PROPERTY: &str = "property";

    /// sh:path local name
    pub const PATH: &str = "path";

    // ========================================================================
    // Cardinality Constraints
    // ========================================================================

    /// sh:minCount local name
    pub const MIN_COUNT: &str = "minCount";

    /// sh:maxCount local name
    pub const MAX_COUNT: &str = "maxCount";

    // ========================================================================
    // Value Type Constraints
    // ========================================================================

    /// sh:datatype local name
    pub const DATATYPE: &str = "datatype";

    /// sh:nodeKind local name
    pub const NODE_KIND: &str = "nodeKind";

    /// sh:class local name
    pub const CLASS: &str = "class";

    // ========================================================================
    // Value Range Constraints
    // ========================================================================

    /// sh:minInclusive local name
    pub const MIN_INCLUSIVE: &str = "minInclusive";

    /// sh:maxInclusive local name
    pub const MAX_INCLUSIVE: &str = "maxInclusive";

    /// sh:minExclusive local name
    pub const MIN_EXCLUSIVE: &str = "minExclusive";

    /// sh:maxExclusive local name
    pub const MAX_EXCLUSIVE: &str = "maxExclusive";

    // ========================================================================
    // String Constraints
    // ========================================================================

    /// sh:pattern local name
    pub const PATTERN: &str = "pattern";

    /// sh:flags local name
    pub const FLAGS: &str = "flags";

    /// sh:minLength local name
    pub const MIN_LENGTH: &str = "minLength";

    /// sh:maxLength local name
    pub const MAX_LENGTH: &str = "maxLength";

    // ========================================================================
    // Value Constraints
    // ========================================================================

    /// sh:hasValue local name
    pub const HAS_VALUE: &str = "hasValue";

    /// sh:in local name
    pub const IN: &str = "in";

    // ========================================================================
    // Pair Constraints
    // ========================================================================

    /// sh:equals local name
    pub const EQUALS: &str = "equals";

    /// sh:disjoint local name
    pub const DISJOINT: &str = "disjoint";

    /// sh:lessThan local name
    pub const LESS_THAN: &str = "lessThan";

    /// sh:lessThanOrEquals local name
    pub const LESS_THAN_OR_EQUALS: &str = "lessThanOrEquals";

    // ========================================================================
    // Closed Shape Constraints
    // ========================================================================

    /// sh:closed local name
    pub const CLOSED: &str = "closed";

    /// sh:ignoredProperties local name
    pub const IGNORED_PROPERTIES: &str = "ignoredProperties";

    // ========================================================================
    // Logical Constraints
    // ========================================================================

    /// sh:not local name
    pub const NOT: &str = "not";

    /// sh:and local name
    pub const AND: &str = "and";

    /// sh:or local name
    pub const OR: &str = "or";

    /// sh:xone local name
    pub const XONE: &str = "xone";

    // ========================================================================
    // Qualified Value Shape
    // ========================================================================

    /// sh:qualifiedValueShape local name
    pub const QUALIFIED_VALUE_SHAPE: &str = "qualifiedValueShape";

    /// sh:qualifiedMinCount local name
    pub const QUALIFIED_MIN_COUNT: &str = "qualifiedMinCount";

    /// sh:qualifiedMaxCount local name
    pub const QUALIFIED_MAX_COUNT: &str = "qualifiedMaxCount";

    /// sh:qualifiedValueShapesDisjoint local name
    pub const QUALIFIED_VALUE_SHAPES_DISJOINT: &str = "qualifiedValueShapesDisjoint";

    // ========================================================================
    // Language Constraints
    // ========================================================================

    /// sh:uniqueLang local name
    pub const UNIQUE_LANG: &str = "uniqueLang";

    /// sh:languageIn local name
    pub const LANGUAGE_IN: &str = "languageIn";

    // ========================================================================
    // Node Kind Values
    // ========================================================================

    /// sh:BlankNode local name
    pub const BLANK_NODE: &str = "BlankNode";

    /// sh:IRI local name
    pub const IRI: &str = "IRI";

    /// sh:Literal local name
    pub const LITERAL: &str = "Literal";

    /// sh:BlankNodeOrIRI local name
    pub const BLANK_NODE_OR_IRI: &str = "BlankNodeOrIRI";

    /// sh:BlankNodeOrLiteral local name
    pub const BLANK_NODE_OR_LITERAL: &str = "BlankNodeOrLiteral";

    /// sh:IRIOrLiteral local name
    pub const IRI_OR_LITERAL: &str = "IRIOrLiteral";

    // ========================================================================
    // Severity Levels
    // ========================================================================

    /// sh:severity local name
    pub const SEVERITY: &str = "severity";

    /// sh:Violation local name
    pub const VIOLATION: &str = "Violation";

    /// sh:Warning local name
    pub const WARNING: &str = "Warning";

    /// sh:Info local name
    pub const INFO: &str = "Info";

    // ========================================================================
    // Result Reporting
    // ========================================================================

    /// sh:message local name
    pub const MESSAGE: &str = "message";

    /// sh:name local name
    pub const NAME: &str = "name";

    /// sh:description local name
    pub const DESCRIPTION: &str = "description";

    // ========================================================================
    // Validation Report
    // ========================================================================

    /// sh:ValidationReport local name
    pub const VALIDATION_REPORT: &str = "ValidationReport";

    /// sh:ValidationResult local name
    pub const VALIDATION_RESULT: &str = "ValidationResult";

    /// sh:conforms local name
    pub const CONFORMS: &str = "conforms";

    /// sh:result local name
    pub const RESULT: &str = "result";

    /// sh:focusNode local name
    pub const FOCUS_NODE: &str = "focusNode";

    /// sh:resultPath local name
    pub const RESULT_PATH: &str = "resultPath";

    /// sh:value local name
    pub const VALUE: &str = "value";

    /// sh:sourceShape local name
    pub const SOURCE_SHAPE: &str = "sourceShape";

    /// sh:sourceConstraintComponent local name
    pub const SOURCE_CONSTRAINT_COMPONENT: &str = "sourceConstraintComponent";

    /// sh:resultSeverity local name
    pub const RESULT_SEVERITY: &str = "resultSeverity";

    /// sh:resultMessage local name
    pub const RESULT_MESSAGE: &str = "resultMessage";
}

/// Fluree-specific vocabulary constants
pub mod fluree {
    /// Fluree ledger namespace IRI
    pub const LEDGER: &str = "https://ns.flur.ee/ledger#";

    /// f:rule IRI - datalog rule definition predicate
    pub const RULE: &str = "https://ns.flur.ee/ledger#rule";

    /// Fluree DB namespace IRI
    pub const DB: &str = "fluree:db:sha256:";

    /// Fluree commit namespace IRI
    pub const COMMIT: &str = "fluree:commit:sha256:";

    /// Fluree vector datatype IRI
    pub const VECTOR: &str = "https://ns.flur.ee/ledger#vector";

    /// Fluree memory storage IRI
    pub const MEMORY: &str = "fluree:memory://";

    /// Fluree file storage IRI
    pub const FILE: &str = "fluree:file://";

    /// Fluree IPFS storage IRI
    pub const IPFS: &str = "fluree:ipfs://";

    /// Fluree S3 storage IRI
    pub const S3: &str = "fluree:s3://";

    /// Fluree index namespace IRI
    pub const INDEX: &str = "https://ns.flur.ee/index#";
}

/// Namespace codes for IRI encoding
///
/// Fluree reserves namespace codes 0-100 for built-in namespaces.
/// User-defined namespaces start at 101.
pub mod namespaces {
    /// Code 0: empty / relative IRI prefix
    pub const EMPTY: i32 = 0;

    /// Code 1: JSON-LD keywords / internal "@" namespace
    pub const JSON_LD: i32 = 1;

    /// Code 2: XSD datatypes
    pub const XSD: i32 = 2;

    /// Code 3: RDF
    pub const RDF: i32 = 3;

    /// Code 4: RDFS
    pub const RDFS: i32 = 4;

    /// Code 5: SHACL
    pub const SHACL: i32 = 5;

    /// Code 6: OWL
    pub const OWL: i32 = 6;

    /// Code 7: W3C Verifiable Credentials
    pub const CREDENTIALS: i32 = 7;

    /// Code 8: Fluree ledger namespace
    pub const FLUREE_LEDGER: i32 = 8;

    /// Code 10: Fluree DB content address prefix
    pub const FLUREE_DB: i32 = 10;

    /// Code 11: DID key prefix
    pub const DID_KEY: i32 = 11;

    /// Code 12: Fluree commit content address prefix
    pub const FLUREE_COMMIT: i32 = 12;

    /// Code 13: fluree:memory://
    pub const FLUREE_MEMORY: i32 = 13;

    /// Code 14: fluree:file://
    pub const FLUREE_FILE: i32 = 14;

    /// Code 15: fluree:ipfs://
    pub const FLUREE_IPFS: i32 = 15;

    /// Code 16: fluree:s3://
    pub const FLUREE_S3: i32 = 16;

    /// Code 17: schema.org
    pub const SCHEMA_ORG: i32 = 17;

    /// Code 18: wikidata
    pub const WIKIDATA: i32 = 18;

    /// Code 19: foaf
    pub const FOAF: i32 = 19;

    /// Code 20: skos
    pub const SKOS: i32 = 20;

    /// Code 21: urn:uuid
    pub const UUID: i32 = 21;

    /// Code 22: urn:isbn:
    pub const ISBN: i32 = 22;

    /// Code 23: urn:issn:
    pub const ISSN: i32 = 23;

    /// Code 24: blank nodes (_:)
    pub const BLANK_NODE: i32 = 24;

    /// Code 25: Fluree index namespace
    pub const FLUREE_INDEX: i32 = 25;

    /// First code available for user-defined namespaces
    pub const USER_START: i32 = 101;
}

/// Common predicate local names (for schema extraction, validation, etc.)
pub mod predicates {
    /// rdf:type local name
    pub const RDF_TYPE: &str = "type";

    /// rdf:first local name (RDF list head element)
    pub const RDF_FIRST: &str = "first";

    /// rdf:rest local name (RDF list tail)
    pub const RDF_REST: &str = "rest";

    /// rdf:nil local name (RDF list terminator)
    pub const RDF_NIL: &str = "nil";

    /// rdfs:subClassOf local name
    pub const RDFS_SUBCLASSOF: &str = "subClassOf";

    /// rdfs:subPropertyOf local name
    pub const RDFS_SUBPROPERTYOF: &str = "subPropertyOf";

    /// rdfs:domain local name
    pub const RDFS_DOMAIN: &str = "domain";

    /// rdfs:range local name
    pub const RDFS_RANGE: &str = "range";

    /// owl:inverseOf local name
    pub const OWL_INVERSEOF: &str = "inverseOf";

    /// owl:equivalentClass local name
    pub const OWL_EQUIVALENTCLASS: &str = "equivalentClass";

    /// owl:equivalentProperty local name
    pub const OWL_EQUIVALENTPROPERTY: &str = "equivalentProperty";

    /// owl:sameAs local name
    pub const OWL_SAMEAS: &str = "sameAs";

    /// owl:SymmetricProperty local name (class, not predicate)
    pub const OWL_SYMMETRICPROPERTY: &str = "SymmetricProperty";

    /// owl:TransitiveProperty local name (class, not predicate)
    pub const OWL_TRANSITIVEPROPERTY: &str = "TransitiveProperty";
}

/// Fluree Ledger namespace predicate local names
pub mod ledger {
    /// ledger:address - storage address (commit or DB snapshot)
    pub const ADDRESS: &str = "address";

    /// ledger:alias - ledger alias
    pub const ALIAS: &str = "alias";

    /// ledger:v - version number
    pub const V: &str = "v";

    /// ledger:previous - reference to previous commit
    pub const PREVIOUS: &str = "previous";

    /// ledger:time - commit timestamp (epoch milliseconds)
    pub const TIME: &str = "time";

    /// ledger:data - reference to DB data subject
    pub const DATA: &str = "data";

    /// ledger:message - commit message (optional)
    pub const MESSAGE: &str = "message";

    /// ledger:author - commit author (optional)
    pub const AUTHOR: &str = "author";

    /// ledger:txn - transaction address (optional)
    pub const TXN: &str = "txn";

    /// ledger:t - transaction number (on DB data subject)
    pub const T: &str = "t";

    /// ledger:size - cumulative data size in bytes (on DB data subject)
    pub const SIZE: &str = "size";

    /// ledger:flakes - cumulative flake count (on DB data subject)
    pub const FLAKES: &str = "flakes";

    /// ledger:rule - datalog rule definition
    /// Used to store user-defined reasoning rules with where/insert clauses
    pub const RULE: &str = "rule";
}

/// Fluree Index namespace predicate local names
pub mod index {
    /// idx:target - search query text for BM25 queries
    pub const TARGET: &str = "target";

    /// idx:limit - maximum number of results to return
    pub const LIMIT: &str = "limit";

    /// idx:result - result pattern specification (can be a var or node ref)
    pub const RESULT: &str = "result";

    /// idx:id - document ID binding in result pattern
    pub const ID: &str = "id";

    /// idx:score - BM25 score binding in result pattern
    pub const SCORE: &str = "score";

    /// idx:ledger - optional ledger alias binding for multi-ledger disambiguation
    pub const LEDGER: &str = "ledger";

    /// idx:sync - synchronization mode (true = sync before query)
    pub const SYNC: &str = "sync";

    /// idx:timeout - query timeout in milliseconds
    pub const TIMEOUT: &str = "timeout";

    /// BM25-specific configuration properties
    ///
    /// fidx:BM25 - BM25 index type identifier
    pub const BM25: &str = "BM25";

    /// fidx:k1 - BM25 k1 parameter (term frequency saturation)
    pub const BM25_K1: &str = "k1";

    /// fidx:b - BM25 b parameter (document length normalization)
    pub const BM25_B: &str = "b";

    /// fidx:property - property to index for BM25
    pub const PROPERTY: &str = "property";

    /// fidx:vector - vector property for similarity search
    pub const VECTOR: &str = "vector";

    /// Virtual graph configuration properties
    ///
    /// fidx:config - VG configuration JSON (stored in nameservice)
    pub const CONFIG: &str = "config";

    /// fidx:dependencies - VG dependency ledger aliases
    pub const DEPENDENCIES: &str = "dependencies";

    /// fidx:index - VG index address
    pub const INDEX: &str = "index";

    /// fidx:indexT - VG index watermark (commit t value)
    pub const INDEX_T: &str = "indexT";
}