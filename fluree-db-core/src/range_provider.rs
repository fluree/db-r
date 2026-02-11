//! Abstract range query provider.
//!
//! `RangeProvider` decouples callers of `range_with_overlay()` from the
//! underlying index implementation.  When a `RangeProvider` is present on
//! a `Db`, `range_with_overlay()` delegates to it instead of traversing the
//! b-tree.  This allows the binary columnar index to serve all range queries
//! without modifying the 25+ callers across the reasoner, API, policy, and
//! SHACL crates.
//!
//! The trait is defined in `fluree-db-core` (where the callers live) and
//! implemented for `BinaryIndexStore` in `fluree-db-query`.

use crate::comparator::IndexType;
use crate::flake::Flake;
use crate::overlay::OverlayProvider;
use crate::query_bounds::{RangeMatch, RangeOptions, RangeTest};
use crate::sid::Sid;
use std::collections::HashMap;

/// A range query backend that can execute range queries against an index.
///
/// This trait abstracts the index implementation so callers can use the same
/// `range_with_overlay()` API regardless of which index is active.
///
/// Implementations must return results in the correct index order (SPOT, PSOT,
/// POST, or OPST) matching the requested `IndexType`.
pub trait RangeProvider: Send + Sync {
    /// Execute a range query, returning matching flakes in index order.
    ///
    /// # Arguments
    ///
    /// * `index` — which index order to scan (SPOT, PSOT, POST, OPST)
    /// * `test` — comparison operator (Eq, Lt, Le, Gt, Ge)
    /// * `match_val` — components to match (subject, predicate, object)
    /// * `opts` — query options (limit, time bounds, object bounds)
    /// * `overlay` — overlay provider for uncommitted novelty flakes
    ///
    /// # Errors
    ///
    /// Returns `io::Error` on I/O failures or if match components cannot be
    /// translated to the index's internal representation.
    fn range(
        &self,
        index: IndexType,
        test: RangeTest,
        match_val: &RangeMatch,
        opts: &RangeOptions,
        overlay: &dyn OverlayProvider,
    ) -> std::io::Result<Vec<Flake>>;

    /// Execute a bounded range query with explicit start/end flakes.
    ///
    /// This is the bounded-range equivalent of [`range()`](Self::range).
    /// Used for subject-range queries (e.g., SHA prefix scans) that need
    /// to scan between two different subjects.
    ///
    /// The default implementation returns `Unsupported`.  Implementors that
    /// support arbitrary interval scans should override this method.
    fn range_bounded(
        &self,
        _index: IndexType,
        _start_bound: &Flake,
        _end_bound: &Flake,
        _opts: &RangeOptions,
        _overlay: &dyn OverlayProvider,
    ) -> std::io::Result<Vec<Flake>> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "range_bounded not supported by this provider",
        ))
    }

    /// Batched lookup: for a fixed predicate, retrieve ref-valued objects for many subjects.
    ///
    /// This supports latency-sensitive callers like policy enforcement (`rdf:type` lookups)
    /// without issuing one range query per subject and without scanning the full predicate
    /// partition.
    ///
    /// Implementations should respect `opts.to_t` and must merge overlay ops.
    ///
    /// Default implementation returns `Unsupported`.
    fn lookup_subject_predicate_refs_batched(
        &self,
        _index: IndexType,
        _predicate: &Sid,
        _subjects: &[Sid],
        _opts: &RangeOptions,
        _overlay: &dyn OverlayProvider,
    ) -> std::io::Result<HashMap<Sid, Vec<Sid>>> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "lookup_subject_predicate_refs_batched not supported by this provider",
        ))
    }
}
