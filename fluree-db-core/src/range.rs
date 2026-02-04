//! Range query implementation
//!
//! This module provides the public `range` API for querying flakes from an index.
//!
//! ## Example
//!
//! ```ignore
//! use fluree_db_core::{range, IndexType, RangeTest, RangeMatch, RangeOptions};
//!
//! let flakes = range(
//!     &db,
//!     IndexType::Spot,
//!     RangeTest::Eq,
//!     RangeMatch::subject(subject_sid),
//!     RangeOptions::default(),
//! ).await?;
//! ```

use crate::cache::{CacheKey, NodeCache};
use crate::comparator::IndexType;
use crate::db::{Db, EMPTY_NODE_ID};
#[cfg(feature = "native")]
use crate::error::Error;
use crate::error::Result;
use crate::flake::Flake;
use crate::index::{ChildRef, IndexNode, ResolvedNode};
use crate::overlay::{NoOverlay, OverlayProvider};
use crate::serde::flakes_transport::{decode_flakes_interned, is_flkb_format};
use crate::serde::json::{parse_branch_node, parse_leaf_node_interned};
use crate::sid::Sid;
use crate::storage::{ReadHint, Storage};
use crate::value::FlakeValue;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::sync::Arc;

// ============================================================================
// Bounded CPU parallelism for leaf parsing (native only)
// ============================================================================

/// Default maximum concurrent leaf parse operations.
///
/// This limits how many `spawn_blocking` calls for leaf parsing can run
/// simultaneously. Set conservatively to avoid oversubscribing CPU and
/// thrashing cache locks.
#[cfg(feature = "native")]
pub const DEFAULT_MAX_CONCURRENT_LEAF_PARSES: usize = 4;

/// Global semaphore for bounding concurrent leaf parsing operations.
///
/// Leaf parsing (simd-json deserialization + SID interning) is CPU-intensive.
/// Without bounds, many concurrent prefetch tasks could spawn too many blocking
/// threads, causing:
/// - Memory pressure from parallel allocations
/// - Cache lock contention
/// - CPU oversubscription
///
/// This semaphore ensures at most N leaf parses run concurrently.
/// The default is [`DEFAULT_MAX_CONCURRENT_LEAF_PARSES`].
///
/// To customize, call [`set_max_concurrent_leaf_parses`] before any queries.
#[cfg(feature = "native")]
static LEAF_PARSE_SEMAPHORE: std::sync::OnceLock<tokio::sync::Semaphore> =
    std::sync::OnceLock::new();

/// Set the maximum number of concurrent leaf parse operations.
///
/// Must be called before any queries are executed. If called after the
/// semaphore has been initialized, this is a no-op and returns `false`.
///
/// # Example
///
/// ```ignore
/// // At application startup, before any queries:
/// fluree_db_core::set_max_concurrent_leaf_parses(num_cpus::get().saturating_sub(1).max(1));
/// ```
#[cfg(feature = "native")]
pub fn set_max_concurrent_leaf_parses(max: usize) -> bool {
    LEAF_PARSE_SEMAPHORE
        .set(tokio::sync::Semaphore::new(max.max(1)))
        .is_ok()
}

/// Get the leaf parse semaphore, initializing with default if needed.
#[cfg(feature = "native")]
fn leaf_parse_semaphore() -> &'static tokio::sync::Semaphore {
    LEAF_PARSE_SEMAPHORE.get_or_init(|| {
        // Default: min(DEFAULT, num_cpus - 1), at least 1
        let max = std::thread::available_parallelism()
            .map(|p| p.get().saturating_sub(1).max(1))
            .unwrap_or(DEFAULT_MAX_CONCURRENT_LEAF_PARSES)
            .min(DEFAULT_MAX_CONCURRENT_LEAF_PARSES);
        tokio::sync::Semaphore::new(max)
    })
}

/// Comparison test operators for range queries
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeTest {
    /// Equal to (becomes >= and <=)
    Eq,
    /// Less than
    Lt,
    /// Less than or equal
    Le,
    /// Greater than
    Gt,
    /// Greater than or equal
    Ge,
}

/// Components to match in a range query
///
/// Use the builder methods to construct a match for specific components.
/// Unset components are wildcards (use min/max bounds).
#[derive(Clone, Debug, Default)]
pub struct RangeMatch {
    /// Subject to match
    pub s: Option<Sid>,
    /// Predicate to match
    pub p: Option<Sid>,
    /// Object to match
    pub o: Option<FlakeValue>,
    /// Datatype to match
    pub dt: Option<Sid>,
    /// Transaction time to match
    pub t: Option<i64>,
}

impl RangeMatch {
    /// Create an empty match (matches everything)
    pub fn new() -> Self {
        Self::default()
    }

    /// Match a specific subject
    pub fn subject(s: Sid) -> Self {
        Self {
            s: Some(s),
            ..Default::default()
        }
    }

    /// Match a specific subject and predicate
    pub fn subject_predicate(s: Sid, p: Sid) -> Self {
        Self {
            s: Some(s),
            p: Some(p),
            ..Default::default()
        }
    }

    /// Match a specific predicate
    pub fn predicate(p: Sid) -> Self {
        Self {
            p: Some(p),
            ..Default::default()
        }
    }

    /// Match a specific predicate and object
    pub fn predicate_object(p: Sid, o: FlakeValue) -> Self {
        Self {
            p: Some(p),
            o: Some(o),
            ..Default::default()
        }
    }

    /// Match a specific transaction time
    pub fn at_t(t: i64) -> Self {
        Self {
            t: Some(t),
            ..Default::default()
        }
    }

    /// Set subject
    pub fn with_subject(mut self, s: Sid) -> Self {
        self.s = Some(s);
        self
    }

    /// Set predicate
    pub fn with_predicate(mut self, p: Sid) -> Self {
        self.p = Some(p);
        self
    }

    /// Set object
    pub fn with_object(mut self, o: FlakeValue) -> Self {
        self.o = Some(o);
        self
    }

    /// Set datatype
    pub fn with_datatype(mut self, dt: Sid) -> Self {
        self.dt = Some(dt);
        self
    }

    /// Set transaction time
    pub fn with_t(mut self, t: i64) -> Self {
        self.t = Some(t);
        self
    }
}

/// Object value bounds for range filtering
///
/// Used for filter pushdown to narrow scan results based on object value comparisons.
/// Bounds are applied as a post-filter after the range scan.
#[derive(Clone, Debug, Default)]
pub struct ObjectBounds {
    /// Lower bound: (value, inclusive)
    /// For `?x > 10`, use `(10, false)`. For `?x >= 10`, use `(10, true)`.
    pub lower: Option<(FlakeValue, bool)>,
    /// Upper bound: (value, inclusive)
    /// For `?x < 100`, use `(100, false)`. For `?x <= 100`, use `(100, true)`.
    pub upper: Option<(FlakeValue, bool)>,
}

impl ObjectBounds {
    /// Create empty bounds (no filtering)
    pub fn new() -> Self {
        Self::default()
    }

    /// Set lower bound
    pub fn with_lower(mut self, value: FlakeValue, inclusive: bool) -> Self {
        self.lower = Some((value, inclusive));
        self
    }

    /// Set upper bound
    pub fn with_upper(mut self, value: FlakeValue, inclusive: bool) -> Self {
        self.upper = Some((value, inclusive));
        self
    }

    /// Check if a value satisfies the bounds
    ///
    /// Uses **type class comparison** for Clojure parity:
    /// - All numeric types (Long, Double, BigInt, Decimal) are comparable to each other
    /// - Temporal types are only comparable within the same kind (Date vs Date, etc.)
    /// - Other types require exact type match
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Numeric class comparison: Long(10) bound matches Double(15.5)
    /// let bounds = ObjectBounds::new().with_lower(FlakeValue::Long(10), true);
    /// assert!(bounds.matches(&FlakeValue::Double(15.5))); // true!
    /// assert!(bounds.matches(&FlakeValue::Long(15)));     // true
    /// assert!(!bounds.matches(&FlakeValue::Long(5)));     // false
    /// ```
    pub fn matches(&self, value: &FlakeValue) -> bool {
        // Check lower bound
        if let Some((lower, inclusive)) = &self.lower {
            match Self::class_cmp(value, lower) {
                None => return false, // Incompatible types
                Some(std::cmp::Ordering::Less) => return false,
                Some(std::cmp::Ordering::Equal) if !inclusive => return false,
                _ => {}
            }
        }

        // Check upper bound
        if let Some((upper, inclusive)) = &self.upper {
            match Self::class_cmp(value, upper) {
                None => return false, // Incompatible types
                Some(std::cmp::Ordering::Greater) => return false,
                Some(std::cmp::Ordering::Equal) if !inclusive => return false,
                _ => {}
            }
        }

        true
    }

    /// Compare values within their type class.
    ///
    /// Returns `Some(Ordering)` if the values are comparable, `None` if incompatible.
    ///
    /// - **Numeric class**: All numeric types are comparable (Long, Double, BigInt, Decimal)
    /// - **Temporal class**: Same temporal type only (Date vs Date, Time vs Time, DateTime vs DateTime)
    /// - **Same type**: Always comparable
    /// - **Different type classes**: Incompatible (returns None)
    fn class_cmp(a: &FlakeValue, b: &FlakeValue) -> Option<std::cmp::Ordering> {
        // Numeric class: all numeric types are comparable
        if a.is_numeric() && b.is_numeric() {
            return a.numeric_cmp(b);
        }

        // Temporal class: same temporal type only
        if a.is_temporal() && b.is_temporal() {
            return a.temporal_cmp(b);
        }

        // Same type: use standard comparison
        if std::mem::discriminant(a) == std::mem::discriminant(b) {
            return Some(a.cmp(b));
        }

        // Incompatible types
        None
    }

    /// Returns true if no bounds are set
    pub fn is_empty(&self) -> bool {
        self.lower.is_none() && self.upper.is_none()
    }
}

/// Default number of leaves to prefetch ahead during cold scans.
///
/// Matches Clojure's `default-prefetch-n` of 3. Larger values can cause
/// too many concurrent I/O operations, hurting cold query performance.
pub const DEFAULT_PREFETCH_N: usize = 100;

/// Options for range query execution
#[derive(Clone, Debug, Default)]
pub struct RangeOptions {
    /// Maximum number of subjects to return
    pub limit: Option<usize>,
    /// Number of subjects to skip
    pub offset: Option<usize>,
    /// Maximum number of flakes to return
    pub flake_limit: Option<usize>,
    /// "As-of" time - only include flakes where t <= to_t
    /// If None, uses the database's current t
    pub to_t: Option<i64>,
    /// Start time for history queries - only include flakes where t >= from_t
    /// Used together with to_t for time-range queries
    pub from_t: Option<i64>,
    /// Optional object value bounds (for filter pushdown)
    pub object_bounds: Option<ObjectBounds>,
    /// History mode: when true, skip stale removal to return all flakes
    /// including retractions. Used by history queries to show full history.
    pub history_mode: bool,
    /// Number of leaves to prefetch ahead during traversal.
    /// Set to 0 to disable prefetch. Default is 3.
    /// Prefetch overlaps I/O with processing to reduce cold query latency.
    pub prefetch_n: Option<usize>,
}

impl RangeOptions {
    /// Create default options (no limits)
    pub fn new() -> Self {
        Self::default()
    }

    /// Set subject limit
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set subject offset
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Set flake limit
    pub fn with_flake_limit(mut self, flake_limit: usize) -> Self {
        self.flake_limit = Some(flake_limit);
        self
    }

    /// Set "as-of" time for time travel queries
    ///
    /// Only flakes with t <= to_t will be included in results.
    pub fn with_to_t(mut self, to_t: i64) -> Self {
        self.to_t = Some(to_t);
        self
    }

    /// Set start time for history queries
    ///
    /// Only flakes with t >= from_t will be included.
    /// Use together with `with_to_t` for time-range queries.
    pub fn with_from_t(mut self, from_t: i64) -> Self {
        self.from_t = Some(from_t);
        self
    }

    /// Set both from_t and to_t for a time range query
    pub fn with_time_range(mut self, from_t: i64, to_t: i64) -> Self {
        self.from_t = Some(from_t);
        self.to_t = Some(to_t);
        self
    }

    /// Set object value bounds for filter pushdown
    ///
    /// Bounds are applied as a post-filter after the range scan, retaining only
    /// flakes whose object value falls within the specified range.
    pub fn with_object_bounds(mut self, bounds: ObjectBounds) -> Self {
        self.object_bounds = Some(bounds);
        self
    }

    /// Enable history mode
    ///
    /// When enabled, skips stale removal to return all flakes including
    /// retractions. Used by history queries to show the full history of changes.
    pub fn with_history_mode(mut self) -> Self {
        self.history_mode = true;
        self
    }

    /// Set the number of leaves to prefetch ahead during traversal
    ///
    /// Prefetch overlaps I/O with processing - while one leaf is being processed,
    /// the next N leaves are loaded in parallel. This significantly reduces cold
    /// query latency.
    ///
    /// Set to 0 to disable prefetch. Default is 3 (if None).
    pub fn with_prefetch_n(mut self, n: usize) -> Self {
        self.prefetch_n = Some(n);
        self
    }

    /// Disable prefetch
    pub fn without_prefetch(mut self) -> Self {
        self.prefetch_n = Some(0);
        self
    }
}

/// Execute a range query on a database
///
/// Returns flakes matching the query criteria in index order.
///
/// # Arguments
///
/// * `db` - The database to query
/// * `index` - Which index to use
/// * `test` - Comparison operator (=, <, <=, >, >=)
/// * `match_val` - Components to match
/// * `opts` - Query options (limits, offset)
///
/// # Example
///
/// ```ignore
/// // Find all flakes for subject X
/// let flakes = range(&db, IndexType::Spot, RangeTest::Eq,
///     RangeMatch::subject(x), RangeOptions::default()).await?;
/// ```
pub async fn range<S, C>(
    db: &Db<S, C>,
    index: IndexType,
    test: RangeTest,
    match_val: RangeMatch,
    opts: RangeOptions,
) -> Result<Vec<Flake>>
where
    S: Storage,
    C: NodeCache,
{
    range_with_overlay(db, &NoOverlay, index, test, match_val, opts).await
}

/// Execute a range query on a database with an overlay provider (novelty).
///
/// The overlay is applied at leaf materialization time, and its epoch is included
/// in leaf materialization cache keys to avoid stale results.
///
/// **Prefetch optimization**: When `prefetch_n` is set (default 3), upcoming leaf
/// nodes are resolved in parallel to overlap I/O with processing. This significantly
/// reduces cold query latency by loading data ahead of when it's needed.
pub async fn range_with_overlay<S, C, O>(
    db: &Db<S, C>,
    overlay: &O,
    index: IndexType,
    test: RangeTest,
    match_val: RangeMatch,
    opts: RangeOptions,
) -> Result<Vec<Flake>>
where
    S: Storage,
    C: NodeCache,
    O: OverlayProvider + ?Sized,
{
    // Expand the test into start/end bounds
    let (mut start_bound, mut end_bound) = expand_range_bounds(index, test, &match_val);

    // For POST index with object bounds, incorporate bounds into start/end flakes.
    // This enables B-tree seeking by object value rather than post-filtering.
    if index == IndexType::Post {
        if let Some(ref bounds) = opts.object_bounds {
            apply_object_bounds_to_flakes(&mut start_bound, &mut end_bound, bounds);
        }
    }

    range_bounded_with_overlay(db, overlay, index, start_bound, end_bound, opts).await
}

/// Execute a bounded range query with explicit start and end flakes.
///
/// This variant allows specifying explicit start and end bound flakes,
/// which is useful for subject-range queries (e.g., SHA prefix scans)
/// that need to scan between two different subjects.
///
/// # Arguments
///
/// * `db` - The database to query
/// * `overlay` - Overlay provider (e.g., novelty) for uncommitted data
/// * `index` - Which index to use (typically SPOT for subject-range queries)
/// * `start_bound` - Starting flake bound (inclusive)
/// * `end_bound` - Ending flake bound (inclusive, matching `trim_to_range` semantics)
/// * `opts` - Query options (limits, time bounds, etc.)
///
/// # Example
///
/// ```ignore
/// // Find all flakes for subjects with names starting with "abc"
/// let start = Flake::min_for_subject(Sid::new(12, "abc"));
/// let end = Flake::min_for_subject(Sid::new(12, "abd")); // "abd" > "abc*"
/// let flakes = range_bounded_with_overlay(
///     &db, &overlay, IndexType::Spot, start, end, RangeOptions::default()
/// ).await?;
/// ```
pub async fn range_bounded_with_overlay<S, C, O>(
    db: &Db<S, C>,
    overlay: &O,
    index: IndexType,
    start_bound: Flake,
    end_bound: Flake,
    opts: RangeOptions,
) -> Result<Vec<Flake>>
where
    S: Storage,
    C: NodeCache,
    O: OverlayProvider + ?Sized,
{
    // Determine effective time bounds
    let to_t = opts.to_t.unwrap_or(db.t);
    let from_t = opts.from_t;
    let _prefetch_n = opts.prefetch_n.unwrap_or(DEFAULT_PREFETCH_N);

    // Get the index root
    let root = db.get_index_root(index)?;

    // Execute tree traversal
    let mut results = Vec::new();
    let mut stack = vec![root];
    let cmp = index.comparator();
    let overlay_epoch = overlay.epoch();
    let mut prefetches: FuturesUnordered<BoxFuture<'_, ()>> = FuturesUnordered::new();

    while let Some(node) = stack.pop() {
        if !node_intersects_range(&node, &start_bound, &end_bound, cmp) {
            continue;
        }

        let mut resolved_fut = resolve_node_materialized_with_overlay(
            db,
            overlay,
            overlay_epoch,
            &node,
            from_t,
            to_t,
            opts.history_mode,
        )
        .boxed()
        .fuse();

        let resolved = loop {
            if prefetches.is_empty() {
                break resolved_fut.await?;
            }
            futures::select! {
                res = resolved_fut => break res?,
                _ = prefetches.next().fuse() => {}
            }
        };

        match resolved {
            ResolvedNode::Leaf { flakes, .. } => {
                #[cfg(feature = "native")]
                if _prefetch_n > 0 {
                    prefetch_upcoming_leaves(
                        &mut prefetches,
                        db,
                        overlay,
                        overlay_epoch,
                        &stack,
                        &start_bound,
                        &end_bound,
                        cmp,
                        from_t,
                        to_t,
                        opts.history_mode,
                        _prefetch_n,
                    );
                }

                let trimmed = trim_to_range(flakes.as_ref(), &start_bound, &end_bound, cmp);

                for flake in trimmed {
                    if let Some(ref bounds) = opts.object_bounds {
                        if !bounds.matches(&flake.o) {
                            continue;
                        }
                    }

                    results.push(flake.clone());
                    if let Some(limit) = opts.flake_limit {
                        if results.len() >= limit {
                            return Ok(results);
                        }
                    }
                }
            }
            ResolvedNode::Branch {
                children,
                node: branch_node,
            } => {
                let filtered: Vec<_> = children
                    .iter()
                    .filter(|c| child_intersects_range(c, &start_bound, &end_bound, cmp))
                    .map(|c| IndexNode::from_child_ref(c, index, branch_node.alias.clone()))
                    .collect();

                stack.extend(filtered.into_iter().rev());

                #[cfg(feature = "native")]
                if _prefetch_n > 0 {
                    prefetch_upcoming_leaves(
                        &mut prefetches,
                        db,
                        overlay,
                        overlay_epoch,
                        &stack,
                        &start_bound,
                        &end_bound,
                        cmp,
                        from_t,
                        to_t,
                        opts.history_mode,
                        _prefetch_n,
                    );
                }
            }
        }
    }

    if opts.limit.is_some() || opts.offset.is_some() {
        results = apply_subject_pagination(results, opts.offset, opts.limit);
    }

    Ok(results)
}

/// Prefetch upcoming nodes (branches and leaves) to overlap I/O with traversal.
///
/// This function looks at the remaining nodes in the stack and kicks off
/// resolution for up to `n` leaf nodes. Due to cache deduplication, if the
/// main loop reaches these nodes before prefetch completes, it will wait
/// on the in-flight fetch rather than starting a new one.
///
/// Note: This does **not** spawn background tasks. The queued futures are driven
/// opportunistically while `range_with_overlay` awaits its normal work.
///
/// Returns the number of prefetch futures that were enqueued.
#[cfg(feature = "native")]
fn prefetch_upcoming_leaves<'a, S, C, O>(
    prefetches: &mut FuturesUnordered<BoxFuture<'a, ()>>,
    db: &'a Db<S, C>,
    overlay: &'a O,
    overlay_epoch: u64,
    stack: &[IndexNode],
    start_bound: &Flake,
    end_bound: &Flake,
    cmp: fn(&Flake, &Flake) -> std::cmp::Ordering,
    from_t: Option<i64>,
    to_t: i64,
    history_mode: bool,
    n: usize,
) -> usize
where
    S: Storage,
    C: NodeCache,
    O: OverlayProvider + ?Sized,
{
    // Collect up to n nodes from the stack that intersect the range.
    // We prefer leaves first (they're typically the expensive ones), but we also
    // prefetch branches because they are cached as Raw and can reduce cold reads.
    let mut prefetch_nodes = Vec::with_capacity(n);
    for node in stack.iter().rev().filter(|n| n.leaf) {
        if prefetch_nodes.len() >= n {
            break;
        }
        if node_intersects_range(node, start_bound, end_bound, cmp) {
            prefetch_nodes.push(node.clone());
        }
    }
    for node in stack.iter().rev().filter(|n| !n.leaf) {
        if prefetch_nodes.len() >= n {
            break;
        }
        if node_intersects_range(node, start_bound, end_bound, cmp) {
            prefetch_nodes.push(node.clone());
        }
    }

    if prefetch_nodes.is_empty() {
        return 0;
    }

    let count = prefetch_nodes.len();

    // Enqueue all prefetches. Errors are ignored; the main loop will retry/report.
    for node in prefetch_nodes {
        prefetches.push(
            async move {
                let _ = resolve_node_materialized_with_overlay(
                    db,
                    overlay,
                    overlay_epoch,
                    &node,
                    from_t,
                    to_t,
                    history_mode,
                )
                .await;
            }
            .boxed(),
        );
    }

    count
}

/// Expand a single test into start/end bounds with explicit min/max sentinels
fn expand_range_bounds(
    index: IndexType,
    test: RangeTest,
    match_val: &RangeMatch,
) -> (Flake, Flake) {
    let start_bound = match_to_flake(index, match_val, true);
    let end_bound = match_to_flake(index, match_val, false);

    match test {
        RangeTest::Eq => (start_bound, end_bound),
        RangeTest::Lt => {
            let min = index_min_flake(index);
            (min, start_bound)
        }
        RangeTest::Le => {
            let min = index_min_flake(index);
            (min, end_bound)
        }
        RangeTest::Gt => {
            let max = index_max_flake(index);
            (end_bound, max)
        }
        RangeTest::Ge => {
            let max = index_max_flake(index);
            (start_bound, max)
        }
    }
}

/// Adjust exclusive double bounds to the next/previous representable value.
///
/// For exclusive lower bounds on doubles, returns `next_up_f64(d)` so the
/// inclusive B-tree seek skips the exact boundary value. For non-double types
/// or inclusive bounds, returns the value as-is.
fn adjust_exclusive_bound(val: &FlakeValue, inclusive: bool, is_lower: bool) -> FlakeValue {
    if inclusive {
        return val.clone();
    }
    match val {
        FlakeValue::Double(d) => {
            let adjusted = if is_lower {
                next_up_f64(*d)
            } else {
                next_down_f64(*d)
            };
            FlakeValue::Double(adjusted)
        }
        _ => val.clone(),
    }
}

/// Incorporate object bounds into start/end flakes for POST-index B-tree seeking.
///
/// POST orders by: predicate → object → subject → time. By baking object
/// bounds into the seek flakes, we let the B-tree skip irrelevant nodes
/// rather than relying solely on post-filtering.
fn apply_object_bounds_to_flakes(start: &mut Flake, end: &mut Flake, bounds: &ObjectBounds) {
    if let Some((lower_val, inclusive)) = &bounds.lower {
        let adjusted = adjust_exclusive_bound(lower_val, *inclusive, true);
        *start = Flake::new(
            start.s.clone(),
            start.p.clone(),
            adjusted,
            start.dt.clone(),
            start.t,
            start.op,
            start.m.clone(),
        );
    }
    if let Some((upper_val, inclusive)) = &bounds.upper {
        let adjusted = adjust_exclusive_bound(upper_val, *inclusive, false);
        *end = Flake::new(
            end.s.clone(),
            end.p.clone(),
            adjusted,
            end.dt.clone(),
            end.t,
            end.op,
            end.m.clone(),
        );
    }
}

/// Convert a match to a flake bound
///
/// `is_start` determines whether to use min or max for unbound components.
fn match_to_flake(_index: IndexType, match_val: &RangeMatch, is_start: bool) -> Flake {
    let (s, p, o, dt, t) = if is_start {
        (
            match_val.s.clone().unwrap_or_else(Sid::min),
            match_val.p.clone().unwrap_or_else(Sid::min),
            match_val.o.clone().unwrap_or_else(FlakeValue::min),
            match_val.dt.clone().unwrap_or_else(Sid::min),
            match_val.t.unwrap_or(i64::MIN),
        )
    } else {
        (
            match_val.s.clone().unwrap_or_else(Sid::max),
            match_val.p.clone().unwrap_or_else(Sid::max),
            match_val.o.clone().unwrap_or_else(FlakeValue::max),
            match_val.dt.clone().unwrap_or_else(Sid::max),
            match_val.t.unwrap_or(i64::MAX),
        )
    };

    let (op, m) = if is_start {
        (false, Some(crate::flake::FlakeMeta::min()))
    } else {
        (true, Some(crate::flake::FlakeMeta::max()))
    };

    Flake::new(s, p, o, dt, t, op, m)
}

/// Get minimum flake for an index
fn index_min_flake(_index: IndexType) -> Flake {
    Flake::min_spot()
}

/// Get maximum flake for an index
fn index_max_flake(_index: IndexType) -> Flake {
    Flake::max_spot()
}

/// Get the next representable f64 value greater than x.
///
/// This is equivalent to Java's `Math.nextUp(x)` or Rust nightly's `f64::next_up()`.
/// Used to convert exclusive lower bounds (> x) to inclusive bounds (>= next_up(x)).
fn next_up_f64(x: f64) -> f64 {
    if x.is_nan() {
        return x;
    }
    if x == f64::NEG_INFINITY {
        return f64::MIN;
    }
    if x == f64::INFINITY {
        return x;
    }
    // Handle -0.0 -> smallest positive
    if x == 0.0 {
        return f64::from_bits(1);
    }

    let bits = x.to_bits();
    // For positive numbers, increment bits; for negative, decrement
    let next_bits = if x > 0.0 { bits + 1 } else { bits - 1 };
    f64::from_bits(next_bits)
}

/// Get the next representable f64 value less than x.
///
/// This is equivalent to Java's `Math.nextDown(x)` or Rust nightly's `f64::next_down()`.
/// Used to convert exclusive upper bounds (< x) to inclusive bounds (<= next_down(x)).
fn next_down_f64(x: f64) -> f64 {
    if x.is_nan() {
        return x;
    }
    if x == f64::INFINITY {
        return f64::MAX;
    }
    if x == f64::NEG_INFINITY {
        return x;
    }
    // Handle +0.0 -> smallest negative (which is -0.0, then smallest negative)
    if x == 0.0 {
        return -f64::from_bits(1);
    }

    let bits = x.to_bits();
    // For positive numbers, decrement bits; for negative, increment
    let next_bits = if x > 0.0 { bits - 1 } else { bits + 1 };
    f64::from_bits(next_bits)
}

/// Check if a node's range intersects the query range
fn node_intersects_range(
    node: &IndexNode,
    start: &Flake,
    end: &Flake,
    _cmp: fn(&Flake, &Flake) -> std::cmp::Ordering,
) -> bool {
    node.intersects_range(start, end)
}

/// Check if a child's range intersects the query range
fn child_intersects_range(
    child: &ChildRef,
    start: &Flake,
    end: &Flake,
    cmp: fn(&Flake, &Flake) -> std::cmp::Ordering,
) -> bool {
    child.intersects_range(start, end, cmp)
}

/// Trim flakes to the query range using binary search
fn trim_to_range<'a>(
    flakes: &'a [Flake],
    start: &Flake,
    end: &Flake,
    cmp: fn(&Flake, &Flake) -> std::cmp::Ordering,
) -> &'a [Flake] {
    if flakes.is_empty() {
        return flakes;
    }

    // Find start index
    let start_idx = flakes.partition_point(|f| cmp(f, start) == std::cmp::Ordering::Less);

    // Find end index
    let end_idx = flakes.partition_point(|f| cmp(f, end) != std::cmp::Ordering::Greater);

    &flakes[start_idx..end_idx]
}

/// Check if a flake falls within the given time window.
///
/// Returns `true` when `f.t <= to_t` and (if `from_t` is set) `f.t >= from_t`.
fn in_time_window(f: &Flake, from_t: Option<i64>, to_t: i64) -> bool {
    f.t <= to_t && from_t.is_none_or(|ft| f.t >= ft)
}

// =========================================================================
// Stale-flake removal (fact key deduplication)
// =========================================================================

/// A fact key that borrows from a `Flake`, used for deduplication.
///
/// Ignores `t` and `op` so that the newest occurrence of each fact
/// (subject, predicate, object, datatype, meta) wins.
#[derive(Clone, Copy, Hash, PartialEq, Eq)]
struct FactKeyRef<'a> {
    s: &'a crate::sid::Sid,
    p: &'a crate::sid::Sid,
    o: &'a crate::value::FlakeValue,
    dt: &'a crate::sid::Sid,
    meta_hash: i32,
}

impl<'a> FactKeyRef<'a> {
    fn from_flake(f: &'a Flake) -> Self {
        Self {
            s: &f.s,
            p: &f.p,
            o: &f.o,
            dt: &f.dt,
            meta_hash: flake_meta_hash(f),
        }
    }
}

/// Stable hash of a flake's metadata (interned id or language tag).
///
/// Used as part of the fact key for deduplication.
fn flake_meta_hash(f: &Flake) -> i32 {
    match &f.m {
        None => 0,
        Some(m) => {
            if let Some(i) = m.i {
                i
            } else if let Some(lang) = &m.lang {
                use std::hash::{Hash, Hasher};
                let mut h = std::collections::hash_map::DefaultHasher::new();
                lang.hash(&mut h);
                (h.finish() & 0x7FFF_FFFF) as i32
            } else {
                0
            }
        }
    }
}

/// Remove stale flakes from an owned vector.
///
/// Mirrors Clojure's `remove-stale-flakes` approach: iterate in reverse
/// (newest first for identical facts), keep only the first occurrence of
/// each fact key, and drop retractions.
fn remove_stale_flakes(flakes: Vec<Flake>) -> Vec<Flake> {
    use std::collections::HashSet;

    let mut seen: HashSet<FactKeyRef<'_>> = HashSet::new();
    let mut keep = vec![false; flakes.len()];

    for (idx, f) in flakes.iter().enumerate().rev() {
        if !seen.insert(FactKeyRef::from_flake(f)) {
            continue;
        }
        // Keep only assertions; drop retractions (and prevent older asserts from being kept).
        if f.op {
            keep[idx] = true;
        }
    }

    flakes
        .into_iter()
        .zip(keep)
        .filter_map(|(f, k)| k.then_some(f))
        .collect()
}

/// Remove stale flakes from a borrowed slice, applying a time filter.
///
/// Combines time-window filtering with fact-key deduplication in a single
/// reverse pass, cloning only the assertions that survive both filters.
fn remove_stale_flakes_in_window(flakes: &[Flake], from_t: Option<i64>, to_t: i64) -> Vec<Flake> {
    use std::collections::HashSet;

    let mut seen: HashSet<FactKeyRef<'_>> = HashSet::new();
    let mut out_rev: Vec<Flake> = Vec::new();

    for f in flakes.iter().rev() {
        if !in_time_window(f, from_t, to_t) {
            continue;
        }
        if !seen.insert(FactKeyRef::from_flake(f)) {
            continue;
        }
        if f.op {
            out_rev.push(f.clone());
        }
    }

    out_rev.reverse();
    out_rev
}

fn materialize_raw_leaf_no_overlay(
    raw_flakes: &[Flake],
    from_t: Option<i64>,
    to_t: i64,
    history_mode: bool,
) -> Vec<Flake> {
    if history_mode {
        // History mode: keep all flakes (including retractions) within time window.
        return raw_flakes
            .iter()
            .filter(|f| in_time_window(f, from_t, to_t))
            .cloned()
            .collect();
    }

    // Non-history mode: time-filter + deduplicate in a single reverse pass,
    // cloning only assertions that survive.
    remove_stale_flakes_in_window(raw_flakes, from_t, to_t)
}

fn merge_sorted(
    left: Vec<Flake>,
    right: Vec<Flake>,
    cmp: fn(&Flake, &Flake) -> std::cmp::Ordering,
) -> Vec<Flake> {
    if left.is_empty() {
        return right;
    }
    if right.is_empty() {
        return left;
    }

    let cap = left.len() + right.len();
    let mut out = Vec::with_capacity(cap);

    let mut left = left.into_iter().peekable();
    let mut right = right.into_iter().peekable();

    loop {
        match (left.peek(), right.peek()) {
            (Some(l), Some(r)) => {
                if cmp(l, r) != std::cmp::Ordering::Greater {
                    out.push(left.next().expect("peeked Some"));
                } else {
                    out.push(right.next().expect("peeked Some"));
                }
            }
            (Some(_), None) => {
                out.extend(left);
                break;
            }
            (None, Some(_)) => {
                out.extend(right);
                break;
            }
            (None, None) => break,
        }
    }

    out
}

/// Resolve a node from storage/cache, materializing leaves for the requested time bounds,
/// and incorporating overlay flakes for this leaf range.
///
/// This is the core resolution function used by both range queries and the prefetch service.
/// It handles:
/// - Raw branch node caching (time-independent)
/// - Leaf node materialization with time filtering (to_t, from_t)
/// - Overlay flake merging
/// - Stale flake removal (unless history_mode)
///
/// The cache key includes overlay epoch, so overlay changes invalidate cached materializations.
pub async fn resolve_node_materialized_with_overlay<S, C, O>(
    db: &Db<S, C>,
    overlay: &O,
    overlay_epoch: u64,
    node: &IndexNode,
    from_t: Option<i64>,
    to_t: i64,
    history_mode: bool,
) -> Result<ResolvedNode>
where
    S: Storage,
    C: NodeCache,
    O: OverlayProvider + ?Sized,
{
    // Special-case the genesis empty root (see EMPTY_NODE_ID in db.rs).
    // This allows querying genesis databases without storage reads.
    // Still need to check overlay for novelty flakes.
    if node.leaf && node.id == EMPTY_NODE_ID {
        let cmp = node.index_type.comparator();

        // Collect overlay flakes for this range
        let mut overlay_flakes: Vec<Flake> = Vec::new();
        overlay.for_each_overlay_flake(
            node.index_type,
            node.first.as_ref(),
            node.rhs.as_ref(),
            node.leftmost,
            to_t,
            &mut |f| {
                if in_time_window(f, from_t, to_t) {
                    overlay_flakes.push(f.clone());
                }
            },
        );

        // Sort overlay flakes by index comparator
        overlay_flakes.sort_by(cmp);

        // Remove stale flakes unless in history mode
        let materialized = if history_mode {
            overlay_flakes
        } else {
            remove_stale_flakes(overlay_flakes)
        };

        return Ok(ResolvedNode::leaf(
            node.clone(),
            Arc::from(materialized.into_boxed_slice()),
        ));
    }

    if !node.leaf {
        // Branch nodes are time-independent; cache raw.
        let key = CacheKey::raw(&node.id);
        return db
            .cache
            .get_or_fetch(&key, || async {
                let bytes = db.storage.read_bytes(&node.id).await?;

                let children = parse_branch_node(bytes)?;

                Ok(ResolvedNode::branch(
                    node.clone(),
                    Arc::from(children.into_boxed_slice()),
                ))
            })
            .await;
    }

    // Leaf: first ensure we have the raw decoded leaf cached.
    let raw_key = CacheKey::raw(&node.id);
    let raw_leaf = db
        .cache
        .get_or_fetch(&raw_key, || async {
            // Use ReadHint::PreferLeafFlakes to request policy-filtered flakes
            // if the storage supports it (e.g., ProxyStorage). The storage may return
            // either FLKB-encoded filtered flakes or traditional JSON-encoded raw flakes.
            let bytes = db
                .storage
                .read_bytes_hint(&node.id, ReadHint::PreferLeafFlakes)
                .await?;

            // Detect format: FLKB (policy-filtered flakes) vs JSON (traditional)
            //
            // FLKB format: Policy-filtered flakes from ProxyStorage content negotiation.
            // These are pre-filtered by the tx server based on peer identity/policy.
            //
            // JSON format: Traditional JSON-encoded leaf nodes from direct storage.
            //
            // On native, we offload the CPU-intensive parsing to a blocking thread
            // so we don't block the async executor.
            //
            // We use a semaphore to bound concurrency, preventing:
            // - Memory pressure from too many parallel parses
            // - Cache lock contention
            // - CPU oversubscription
            #[cfg(feature = "native")]
            let flakes = {
                // Acquire permit before spawning (bounds concurrent parses)
                let _permit = leaf_parse_semaphore()
                    .acquire()
                    .await
                    .map_err(|_| Error::Other("leaf parse semaphore closed".to_string()))?;

                let interner = db.sid_interner.clone();
                tokio::task::spawn_blocking(move || {
                    if is_flkb_format(&bytes) {
                        // FLKB: Decode policy-filtered transport flakes
                        decode_flakes_interned(&bytes, &interner).map_err(|e| {
                            crate::error::Error::other(format!(
                                "Failed to decode transport flakes: {}",
                                e
                            ))
                        })
                    } else {
                        // JSON: Parse traditional leaf node with SID interning integrated
                        parse_leaf_node_interned(bytes, &interner)
                    }
                })
                .await
                .map_err(|e| Error::Other(format!("spawn_blocking join error: {}", e)))??
                // _permit dropped here, releasing the semaphore slot
            };

            #[cfg(not(feature = "native"))]
            let flakes = if is_flkb_format(&bytes) {
                // FLKB: Decode policy-filtered transport flakes
                decode_flakes_interned(&bytes, &db.sid_interner).map_err(|e| {
                    crate::error::Error::other(format!("Failed to decode transport flakes: {}", e))
                })?
            } else {
                // JSON: Parse traditional leaf node with SID interning integrated
                parse_leaf_node_interned(bytes, &db.sid_interner)?
            };

            Ok(ResolvedNode::leaf(
                node.clone(),
                Arc::from(flakes.into_boxed_slice()),
            ))
        })
        .await?;

    let raw_flakes = match raw_leaf {
        ResolvedNode::Leaf { flakes, .. } => flakes,
        ResolvedNode::Branch { .. } => unreachable!("raw leaf cache returned branch"),
    };

    // Now cache the materialized version for the requested time window.
    let mat_key = match from_t {
        Some(ft) => CacheKey::leaf_history_range_with_epoch_and_mode(
            &node.id,
            ft,
            to_t,
            overlay_epoch,
            history_mode,
        ),
        None => {
            CacheKey::leaf_t_range_with_epoch_and_mode(&node.id, to_t, overlay_epoch, history_mode)
        }
    };

    db.cache
        .get_or_fetch(&mat_key, || async {
            let cmp = node.index_type.comparator();

            // Overlay flakes (must be yielded in sorted order)
            let mut overlay_flakes: Vec<Flake> = Vec::new();
            overlay.for_each_overlay_flake(
                node.index_type,
                node.first.as_ref(),
                node.rhs.as_ref(),
                node.leftmost,
                to_t,
                &mut |f| {
                    if in_time_window(f, from_t, to_t) {
                        overlay_flakes.push(f.clone());
                    }
                },
            );

            // Fast path: no overlay, so we can materialize directly from the raw leaf
            // without cloning the entire leaf into a temp vec.
            let materialized = if overlay_flakes.is_empty() {
                materialize_raw_leaf_no_overlay(raw_flakes.as_ref(), from_t, to_t, history_mode)
            } else {
                // General path: time-filter leaf into owned vec, merge with overlay, then stale-remove.
                // (Overlay can override stored facts, so stale removal must happen after merge.)
                let leaf_flakes: Vec<Flake> = raw_flakes
                    .iter()
                    .filter(|f| in_time_window(f, from_t, to_t))
                    .cloned()
                    .collect();

                // Overlay flakes are yielded sorted by overlay implementation; defensively sort.
                overlay_flakes.sort_by(&cmp);

                let merged = merge_sorted(leaf_flakes, overlay_flakes, cmp);
                if history_mode {
                    merged
                } else {
                    remove_stale_flakes(merged)
                }
            };

            Ok(ResolvedNode::leaf(
                node.clone(),
                Arc::from(materialized.into_boxed_slice()),
            ))
        })
        .await
}

/// Apply subject-based pagination
fn apply_subject_pagination(
    flakes: Vec<Flake>,
    offset: Option<usize>,
    limit: Option<usize>,
) -> Vec<Flake> {
    if offset.is_none() && limit.is_none() {
        return flakes;
    }

    let mut result = Vec::new();
    let mut current_subject: Option<Sid> = None;
    let mut subject_count = 0;
    let offset = offset.unwrap_or(0);
    let limit = limit.unwrap_or(usize::MAX);

    for flake in flakes {
        let is_new_subject = current_subject
            .as_ref()
            .map(|s| s != &flake.s)
            .unwrap_or(true);

        if is_new_subject {
            subject_count += 1;
            current_subject = Some(flake.s.clone());
        }

        // Skip if we haven't passed offset
        if subject_count <= offset {
            continue;
        }

        // Stop if we've hit the limit
        if subject_count > offset + limit {
            break;
        }

        result.push(flake);
    }

    result
}

// ============================================================================
// RangeCursor: Stateful iterator for chunked B-tree traversal
// ============================================================================

/// A stateful cursor for iterating over index flakes one leaf at a time.
///
/// `RangeCursor` encapsulates B-tree traversal state, enabling chunked iteration
/// without loading all matching flakes into memory upfront. This provides bounded
/// memory usage regardless of result set size.
///
/// # Usage
///
/// ```ignore
/// use fluree_db_core::{RangeCursor, IndexType, RangeTest, RangeMatch, RangeOptions};
///
/// // Create cursor (just sets up traversal state, no I/O)
/// let mut cursor = RangeCursor::new(
///     &db,
///     IndexType::Spot,
///     RangeTest::Eq,
///     RangeMatch::subject(sid),
///     RangeOptions::default().with_to_t(100),
/// )?;
///
/// // Iterate leaf-by-leaf
/// while let Some(leaf_flakes) = cursor.next_leaf(&db, &overlay).await? {
///     for flake in leaf_flakes {
///         // Process flake...
///     }
/// }
/// ```
///
/// # Memory Model
///
/// - **Cursor state**: O(tree_depth) for the stack (~tens of nodes)
/// - **Per-leaf**: O(leaf_size) flakes loaded at a time (~3000 flakes, ~300KB)
/// - **Total**: Bounded regardless of total matching flakes
///
/// # Limitations
///
/// The cursor does **not** implement:
/// - `RangeOptions.limit` / `offset` (subject-based pagination)
/// - `RangeOptions.flake_limit`
///
/// These are intentionally omitted because `ScanOperator` handles limits at a
/// higher level. Callers using `collect_all()` should be aware that results
/// may differ from `range_with_overlay()` when those options are set.
///
/// # Thread Safety
///
/// The cursor itself is not thread-safe. Each caller should have its own cursor.
/// The underlying `Db` and overlay can be shared across threads.
pub struct RangeCursor {
    /// B-tree traversal stack (nodes to visit)
    stack: Vec<IndexNode>,
    /// Start bound (inclusive)
    start_bound: Flake,
    /// End bound (inclusive)
    end_bound: Flake,
    /// Comparator for this index
    cmp: fn(&Flake, &Flake) -> std::cmp::Ordering,
    /// Index type being scanned
    index: IndexType,
    /// Upper time bound (as-of time)
    to_t: i64,
    /// Optional lower time bound (for history queries)
    from_t: Option<i64>,
    /// History mode flag
    history_mode: bool,
    /// Optional object bounds for filter pushdown
    object_bounds: Option<ObjectBounds>,
    /// Whether the cursor has been exhausted
    exhausted: bool,
    /// Number of leaves to prefetch (0 to disable).
    /// Used only with `native` feature; silence dead_code warning for WASM builds.
    #[allow(dead_code)]
    prefetch_n: usize,
    /// Nodes to prefetch on next `next_leaf()` call.
    ///
    /// We store nodes rather than futures to avoid lifetime issues across calls.
    /// At the start of each `next_leaf()`, we fire off prefetch for these nodes.
    /// Used only with `native` feature.
    #[allow(dead_code)]
    pending_prefetch_nodes: Vec<IndexNode>,
}

impl RangeCursor {
    /// Create a new range cursor.
    ///
    /// This sets up traversal state without performing any I/O.
    /// The actual traversal happens during `next_leaf()` calls.
    ///
    /// # Arguments
    ///
    /// * `db` - Database to query (used for root node and time bounds)
    /// * `index` - Which index to use
    /// * `test` - Comparison operator
    /// * `match_val` - Components to match
    /// * `opts` - Query options
    ///
    /// # Returns
    ///
    /// A cursor ready for iteration, or an error if the index root cannot be loaded.
    pub fn new<S, C>(
        db: &Db<S, C>,
        index: IndexType,
        test: RangeTest,
        match_val: RangeMatch,
        opts: RangeOptions,
    ) -> Result<Self>
    where
        S: Storage,
        C: NodeCache,
    {
        let to_t = opts.to_t.unwrap_or(db.t);
        let from_t = opts.from_t;
        let prefetch_n = opts.prefetch_n.unwrap_or(DEFAULT_PREFETCH_N);

        // Expand the test into start/end bounds
        let (mut start_bound, mut end_bound) = expand_range_bounds(index, test, &match_val);

        // For POST index with object bounds, incorporate bounds into start/end flakes
        if index == IndexType::Post {
            if let Some(ref bounds) = opts.object_bounds {
                apply_object_bounds_to_flakes(&mut start_bound, &mut end_bound, bounds);
            }
        }

        // Get the index root
        let root = db.get_index_root(index)?;
        let cmp = index.comparator();

        Ok(Self {
            stack: vec![root],
            start_bound,
            end_bound,
            cmp,
            index,
            to_t,
            from_t,
            history_mode: opts.history_mode,
            object_bounds: opts.object_bounds,
            exhausted: false,
            prefetch_n,
            pending_prefetch_nodes: Vec::new(),
        })
    }

    /// Create a cursor with explicit start and end bounds.
    ///
    /// This variant is useful for subject-range queries (e.g., SHA prefix scans)
    /// that need to scan between two different subjects.
    pub fn new_bounded<S, C>(
        db: &Db<S, C>,
        index: IndexType,
        start_bound: Flake,
        end_bound: Flake,
        opts: RangeOptions,
    ) -> Result<Self>
    where
        S: Storage,
        C: NodeCache,
    {
        let to_t = opts.to_t.unwrap_or(db.t);
        let from_t = opts.from_t;
        let prefetch_n = opts.prefetch_n.unwrap_or(DEFAULT_PREFETCH_N);

        let root = db.get_index_root(index)?;
        let cmp = index.comparator();

        Ok(Self {
            stack: vec![root],
            start_bound,
            end_bound,
            cmp,
            index,
            to_t,
            from_t,
            history_mode: opts.history_mode,
            object_bounds: opts.object_bounds,
            exhausted: false,
            prefetch_n,
            pending_prefetch_nodes: Vec::new(),
        })
    }

    /// Check if the cursor is exhausted.
    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    /// Get the next leaf's worth of flakes.
    ///
    /// Returns `Ok(Some(flakes))` with the flakes from the next leaf in traversal order,
    /// `Ok(None)` when the cursor is exhausted, or `Err` on I/O or processing errors.
    ///
    /// The flakes are already:
    /// - Filtered to the time range (to_t, from_t)
    /// - Merged with overlay (novelty)
    /// - Filtered for stale removal (unless history_mode)
    /// - Trimmed to the range bounds
    /// - Filtered by object_bounds (if specified)
    ///
    /// # Arguments
    ///
    /// * `db` - Database for storage access
    /// * `overlay` - Overlay provider (novelty) for uncommitted data
    pub async fn next_leaf<S, C, O>(
        &mut self,
        db: &Db<S, C>,
        overlay: &O,
    ) -> Result<Option<Vec<Flake>>>
    where
        S: Storage,
        C: NodeCache,
        O: OverlayProvider + ?Sized,
    {
        if self.exhausted {
            return Ok(None);
        }

        let overlay_epoch = overlay.epoch();

        // Copy time/mode fields to locals before building async blocks.
        // This avoids capturing `self` in closures while also mutably borrowing it.
        let from_t = self.from_t;
        let to_t = self.to_t;
        let history_mode = self.history_mode;

        // Prefetch futures for upcoming leaves (includes any pending from previous call)
        let mut prefetches: FuturesUnordered<BoxFuture<'_, ()>> = FuturesUnordered::new();

        // Fire off prefetch for nodes saved from the previous call.
        // This ensures prefetch work scheduled when returning a leaf actually runs.
        //
        // NOTE: If these prefetch futures are orphaned (dropped before completion),
        // the cache will handle it gracefully by retrying on the next request for
        // that node. See SimpleCache::get_or_fetch orphan handling.
        #[cfg(feature = "native")]
        if self.prefetch_n > 0 && !self.pending_prefetch_nodes.is_empty() {
            for node in self.pending_prefetch_nodes.drain(..) {
                prefetches.push(
                    async move {
                        let _ = resolve_node_materialized_with_overlay(
                            db,
                            overlay,
                            overlay_epoch,
                            &node,
                            from_t,
                            to_t,
                            history_mode,
                        )
                        .await;
                    }
                    .boxed(),
                );
            }
        }

        while let Some(node) = self.stack.pop() {
            // Check if node intersects range
            if !node_intersects_range(&node, &self.start_bound, &self.end_bound, self.cmp) {
                continue;
            }

            // Resolve the node with overlay merging and time filtering
            let mut resolved_fut = resolve_node_materialized_with_overlay(
                db,
                overlay,
                overlay_epoch,
                &node,
                from_t,
                to_t,
                history_mode,
            )
            .boxed()
            .fuse();

            // Drive prefetches while awaiting the main resolve
            let resolved = loop {
                if prefetches.is_empty() {
                    break resolved_fut.await?;
                }
                futures::select! {
                    res = resolved_fut => break res?,
                    _ = prefetches.next().fuse() => {}
                }
            };

            match resolved {
                ResolvedNode::Leaf { flakes, .. } => {
                    // Trim to range and apply object bounds filter
                    let trimmed = trim_to_range(
                        flakes.as_ref(),
                        &self.start_bound,
                        &self.end_bound,
                        self.cmp,
                    );

                    let result: Vec<Flake> = if let Some(ref bounds) = self.object_bounds {
                        trimmed
                            .iter()
                            .filter(|f| bounds.matches(&f.o))
                            .cloned()
                            .collect()
                    } else {
                        trimmed.to_vec()
                    };

                    // Return this leaf's flakes (even if empty - let caller decide)
                    if !result.is_empty() {
                        // Save nodes for prefetch on next call.
                        // We collect nodes here (not fire futures) to avoid orphaning
                        // in-flight prefetch. The cache handles orphans gracefully, but
                        // it's still better to give prefetch the full next call to complete.
                        //
                        // NOTE: When using PrefetchService (recommended for native), callers
                        // should use prefetch_candidates() and enqueue to the service instead
                        // of relying on this local prefetch which can be orphaned.
                        #[cfg(feature = "native")]
                        if self.prefetch_n > 0 {
                            self.pending_prefetch_nodes = self.prefetch_candidates();
                        }
                        return Ok(Some(result));
                    }
                    // Empty leaf after filtering - continue to next
                }
                ResolvedNode::Branch {
                    children,
                    node: branch_node,
                } => {
                    // Filter children that intersect the range and push in reverse order
                    let filtered: Vec<_> = children
                        .iter()
                        .filter(|c| {
                            child_intersects_range(c, &self.start_bound, &self.end_bound, self.cmp)
                        })
                        .map(|c| {
                            IndexNode::from_child_ref(c, self.index, branch_node.alias.clone())
                        })
                        .collect();

                    // Push in reverse so we process left-to-right
                    self.stack.extend(filtered.into_iter().rev());

                    // NOTE: We do NOT call prefetch_upcoming_leaves here like range_with_overlay does.
                    // Instead, we collect nodes when returning a leaf and fire them at the START
                    // of the next next_leaf() call. This gives prefetch more time to complete,
                    // and if orphaned, the cache handles it gracefully (retry on next request).
                }
            }
        }

        // Stack exhausted
        self.exhausted = true;
        Ok(None)
    }

    /// Collect nodes to prefetch from the current stack.
    ///
    /// Returns up to `prefetch_n` nodes that intersect the range, prioritizing leaves.
    /// This is useful for callers that want to enqueue prefetch requests to a
    /// background service (e.g., `PrefetchService`) for true background warming.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // After getting a leaf, enqueue upcoming nodes for prefetch
    /// if let Some(flakes) = cursor.next_leaf(&db, &overlay).await? {
    ///     for node in cursor.prefetch_candidates() {
    ///         prefetch_service.try_enqueue(PrefetchRequest { node, ... });
    ///     }
    /// }
    /// ```
    pub fn prefetch_candidates(&self) -> Vec<IndexNode> {
        let mut nodes = Vec::with_capacity(self.prefetch_n);

        // Collect leaves first (they're the expensive ones)
        for node in self.stack.iter().rev().filter(|n| n.leaf) {
            if nodes.len() >= self.prefetch_n {
                break;
            }
            if node_intersects_range(node, &self.start_bound, &self.end_bound, self.cmp) {
                nodes.push(node.clone());
            }
        }

        // Then branches
        for node in self.stack.iter().rev().filter(|n| !n.leaf) {
            if nodes.len() >= self.prefetch_n {
                break;
            }
            if node_intersects_range(node, &self.start_bound, &self.end_bound, self.cmp) {
                nodes.push(node.clone());
            }
        }

        nodes
    }

    /// Get the next leaf's flakes with a callback for prefetch at branch expansion.
    ///
    /// This is the recommended method when using `PrefetchService`. The callback
    /// is invoked **immediately when a branch is expanded** with the newly-pushed
    /// children (up to `prefetch_n`). This gives prefetch maximum lead time.
    ///
    /// The callback receives nodes that:
    /// - Were just pushed onto the stack from a branch expansion
    /// - Intersect the query range (based on their `first/rhs` boundaries)
    /// - Are limited to `prefetch_n` items
    ///
    /// # Arguments
    ///
    /// * `db` - Database for storage access
    /// * `overlay` - Overlay provider for uncommitted data
    /// * `on_prefetch` - Callback invoked with nodes to prefetch when a branch is expanded
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = cursor.next_leaf_with_prefetch(&db, &overlay, |nodes| {
    ///     for node in nodes {
    ///         prefetch_service.try_enqueue(PrefetchRequest {
    ///             db: db_arc.clone(),
    ///             overlay: overlay_arc.clone(),
    ///             node,
    ///             to_t,
    ///             from_t,
    ///             history_mode,
    ///         });
    ///     }
    /// }).await?;
    /// ```
    pub async fn next_leaf_with_prefetch<S, C, O, F>(
        &mut self,
        db: &Db<S, C>,
        overlay: &O,
        mut on_prefetch: F,
    ) -> Result<Option<Vec<Flake>>>
    where
        S: Storage,
        C: NodeCache,
        O: OverlayProvider + ?Sized,
        F: FnMut(Vec<IndexNode>),
    {
        if self.exhausted {
            return Ok(None);
        }

        let overlay_epoch = overlay.epoch();
        let from_t = self.from_t;
        let to_t = self.to_t;
        let history_mode = self.history_mode;

        while let Some(node) = self.stack.pop() {
            // Check if node intersects range
            if !node_intersects_range(&node, &self.start_bound, &self.end_bound, self.cmp) {
                continue;
            }

            // Resolve the node
            let resolved = resolve_node_materialized_with_overlay(
                db,
                overlay,
                overlay_epoch,
                &node,
                from_t,
                to_t,
                history_mode,
            )
            .await?;

            match resolved {
                ResolvedNode::Leaf { flakes, .. } => {
                    // Trim to range and apply object bounds filter
                    let trimmed = trim_to_range(
                        flakes.as_ref(),
                        &self.start_bound,
                        &self.end_bound,
                        self.cmp,
                    );

                    let result: Vec<Flake> = if let Some(ref bounds) = self.object_bounds {
                        trimmed
                            .iter()
                            .filter(|f| bounds.matches(&f.o))
                            .cloned()
                            .collect()
                    } else {
                        trimmed.to_vec()
                    };

                    if !result.is_empty() {
                        return Ok(Some(result));
                    }
                    // Empty leaf after filtering - continue to next
                }
                ResolvedNode::Branch {
                    children,
                    node: branch_node,
                } => {
                    // Filter children that intersect the range
                    let filtered: Vec<_> = children
                        .iter()
                        .filter(|c| {
                            child_intersects_range(c, &self.start_bound, &self.end_bound, self.cmp)
                        })
                        .map(|c| {
                            IndexNode::from_child_ref(c, self.index, branch_node.alias.clone())
                        })
                        .collect();

                    // PREFETCH: Call callback with upcoming nodes BEFORE pushing to stack.
                    // This is the ideal prefetch point - we know exactly what's coming.
                    //
                    // IMPORTANT: Skip the FIRST node - mainline will pop and fetch it immediately.
                    // Prefetch nodes 2..N+1 instead. This gives prefetch a head start on the
                    // nodes mainline will need NEXT, avoiding the race where both try to
                    // fetch the same node simultaneously.
                    //
                    // Limited to prefetch_n to bound worst-case over-fetching.
                    if self.prefetch_n > 0 {
                        let prefetch_nodes: Vec<_> = filtered
                            .iter()
                            .skip(1) // Skip first - mainline will get it
                            .take(self.prefetch_n)
                            .cloned()
                            .collect();
                        if !prefetch_nodes.is_empty() {
                            on_prefetch(prefetch_nodes);
                        }
                    }

                    // Push in reverse so we process left-to-right
                    self.stack.extend(filtered.into_iter().rev());
                }
            }
        }

        // Stack exhausted
        self.exhausted = true;
        Ok(None)
    }

    /// Collect all remaining flakes from the cursor.
    ///
    /// This is a convenience method that drains the cursor into a Vec.
    /// For large result sets, prefer using `next_leaf()` directly.
    pub async fn collect_all<S, C, O>(&mut self, db: &Db<S, C>, overlay: &O) -> Result<Vec<Flake>>
    where
        S: Storage,
        C: NodeCache,
        O: OverlayProvider + ?Sized,
    {
        let mut results = Vec::new();
        while let Some(leaf_flakes) = self.next_leaf(db, overlay).await? {
            results.extend(leaf_flakes);
        }
        Ok(results)
    }
}

/// Batch size constant for batched subject joins.
///
/// When `NestedLoopJoinOperator` accumulates left rows for the batched seek path,
/// it flushes after this many Sid-bearing left rows.
pub const BATCHED_JOIN_SIZE: usize = 100_000;

/// State of a resolved leaf during multi-seek traversal.
struct LeafState {
    /// Resolved flakes (time-filtered, overlay-merged, stale-removed).
    flakes: Vec<Flake>,
    /// Current scan position within `flakes`.
    pos: usize,
    /// Right-hand boundary of this leaf (for computing next seek target).
    rhs: Option<Flake>,
}

/// A cursor that seeks through multiple sorted, non-contiguous ranges in a B-tree
/// using explicit seek-to-leaf descents from the root.
///
/// Unlike `RangeCursor` (which walks all leaves left-to-right via a DFS stack),
/// `MultiSeekCursor` descends from root for each new range, skipping all
/// intervening leaves. Branch nodes are cached, so repeated seeks pay O(1)
/// amortized for branches; only leaf resolution touches storage.
///
/// # Usage
///
/// ```ignore
/// let ranges = vec![
///     (Flake::min_for_subject_predicate(s1, p), Flake::max_for_subject_predicate(s1, p)),
///     (Flake::min_for_subject_predicate(s2, p), Flake::max_for_subject_predicate(s2, p)),
/// ];
/// let mut cursor = MultiSeekCursor::new(&db, IndexType::Psot, ranges, opts)?;
/// while let Some(flakes) = cursor.next_range_flakes(&db, &overlay).await? {
///     // flakes belong to ranges[cursor.last_range_index()]
/// }
/// ```
pub struct MultiSeekCursor {
    /// Sorted ranges to seek through (sorted by start bound using index comparator).
    ranges: Vec<(Flake, Flake)>,
    /// Index of the current range being processed (0-based).
    current_range: usize,
    /// Root node of the index tree (kept for re-seeking).
    root: IndexNode,
    /// Comparator for this index type.
    cmp: fn(&Flake, &Flake) -> std::cmp::Ordering,
    /// Index type being queried.
    index: IndexType,
    /// Upper time bound (as-of time).
    to_t: i64,
    /// Optional lower time bound (for history queries).
    from_t: Option<i64>,
    /// History mode flag.
    history_mode: bool,
    /// Buffered leaf from the last seek (may span into the next range).
    current_leaf: Option<LeafState>,
}

impl MultiSeekCursor {
    /// Create a new multi-seek cursor.
    ///
    /// `ranges` must be pre-sorted by start bound using the index comparator.
    /// No validation is performed on ordering.
    pub fn new<S, C>(
        db: &Db<S, C>,
        index: IndexType,
        ranges: Vec<(Flake, Flake)>,
        opts: RangeOptions,
    ) -> Result<Self>
    where
        S: Storage,
        C: NodeCache,
    {
        let to_t = opts.to_t.unwrap_or(db.t);
        let root = db.get_index_root(index)?;
        let cmp = index.comparator();

        Ok(Self {
            ranges,
            current_range: 0,
            root,
            cmp,
            index,
            to_t,
            from_t: opts.from_t,
            history_mode: opts.history_mode,
            current_leaf: None,
        })
    }

    /// Returns the 0-based index of the range whose flakes were just returned.
    /// Valid after a successful `next_range_flakes()` call that returned `Some`.
    pub fn last_range_index(&self) -> usize {
        self.current_range.saturating_sub(1)
    }

    /// Get flakes for the next range.
    ///
    /// Returns `Ok(Some(flakes))` for each range (may be empty if no flakes match),
    /// `Ok(None)` when all ranges are exhausted.
    pub async fn next_range_flakes<S, C, O>(
        &mut self,
        db: &Db<S, C>,
        overlay: &O,
    ) -> Result<Option<Vec<Flake>>>
    where
        S: Storage,
        C: NodeCache,
        O: OverlayProvider + ?Sized,
    {
        if self.current_range >= self.ranges.len() {
            return Ok(None);
        }

        let mut result = Vec::new();

        // Phase 1: drain any buffered leaf from a previous range
        let mut need_seek_after_rhs: Option<Flake> = None;

        if let Some(ref mut leaf) = self.current_leaf {
            let (start, end) = &self.ranges[self.current_range];
            Self::drain_matching_flakes(leaf, start, end, self.cmp, &mut result);

            if leaf.pos < leaf.flakes.len() {
                // Leaf still has flakes past `end` — range is complete
                self.current_range += 1;
                return Ok(Some(result));
            }

            // Leaf exhausted — range may span beyond this leaf
            if let Some(ref rhs) = leaf.rhs {
                let (_, end) = &self.ranges[self.current_range];
                if (self.cmp)(rhs, end) >= std::cmp::Ordering::Equal {
                    // Leaf covers through end of range — range is complete
                    self.current_leaf = None;
                    self.current_range += 1;
                    return Ok(Some(result));
                }
                need_seek_after_rhs = Some(rhs.clone());
            } else {
                // Rightmost leaf, nothing more
                self.current_leaf = None;
                self.current_range += 1;
                return Ok(Some(result));
            }
        }
        self.current_leaf = None;

        // Phase 2: seek loop
        let overlay_epoch = overlay.epoch();
        let (start, end) = self.ranges[self.current_range].clone();

        // Determine initial seek target and mode
        let (mut seek_target, mut after) = match need_seek_after_rhs {
            Some(rhs) => (rhs, true),
            None => (start.clone(), false),
        };

        loop {
            let leaf = Self::seek_leaf(
                &self.root,
                &seek_target,
                after,
                self.cmp,
                self.index,
                db,
                overlay,
                overlay_epoch,
                self.to_t,
                self.from_t,
                self.history_mode,
            )
            .await?;

            let mut leaf = match leaf {
                Some(l) => l,
                None => break, // past end of index
            };

            Self::drain_matching_flakes(&mut leaf, &start, &end, self.cmp, &mut result);

            if leaf.pos < leaf.flakes.len() {
                // Leaf still has flakes past `end` — range complete, save for next range
                self.current_leaf = Some(leaf);
                break;
            }

            // Check if this leaf covers through end of range
            match &leaf.rhs {
                None => break, // rightmost leaf
                Some(rhs) if (self.cmp)(rhs, &end) >= std::cmp::Ordering::Equal => break,
                Some(rhs) => {
                    // Range spans beyond this leaf — seek to next leaf
                    seek_target = rhs.clone();
                    after = true;
                }
            }
        }

        self.current_range += 1;
        Ok(Some(result))
    }

    /// Drain flakes from `leaf` that fall within `[start, end]`.
    ///
    /// Advances `leaf.pos` past all consumed flakes. Stops when a flake
    /// exceeds `end` or the leaf is exhausted.
    fn drain_matching_flakes(
        leaf: &mut LeafState,
        start: &Flake,
        end: &Flake,
        cmp: fn(&Flake, &Flake) -> std::cmp::Ordering,
        result: &mut Vec<Flake>,
    ) {
        while leaf.pos < leaf.flakes.len() {
            let f = &leaf.flakes[leaf.pos];
            if cmp(f, start) == std::cmp::Ordering::Less {
                // Before range start — skip
                leaf.pos += 1;
                continue;
            }
            if cmp(f, end) == std::cmp::Ordering::Greater {
                // Past range end — stop (don't advance pos)
                break;
            }
            // Within range
            result.push(f.clone());
            leaf.pos += 1;
        }
    }

    /// Seek from root to the leaf containing (or just after) `target`.
    ///
    /// # Arguments
    ///
    /// * `after` - If `false`, seek to the leaf whose range contains `target`.
    ///   If `true`, seek to the leaf **after** the one whose `rhs` equals `target`,
    ///   preventing stalling when continuing past a leaf boundary.
    async fn seek_leaf<S, C, O>(
        root: &IndexNode,
        target: &Flake,
        after: bool,
        cmp: fn(&Flake, &Flake) -> std::cmp::Ordering,
        index: IndexType,
        db: &Db<S, C>,
        overlay: &O,
        overlay_epoch: u64,
        to_t: i64,
        from_t: Option<i64>,
        history_mode: bool,
    ) -> Result<Option<LeafState>>
    where
        S: Storage,
        C: NodeCache,
        O: OverlayProvider + ?Sized,
    {
        let mut current_node = root.clone();

        loop {
            let resolved = resolve_node_materialized_with_overlay(
                db,
                overlay,
                overlay_epoch,
                &current_node,
                from_t,
                to_t,
                history_mode,
            )
            .await?;

            match resolved {
                ResolvedNode::Branch {
                    children,
                    node: branch_node,
                } => {
                    match Self::floor_child(&children, target, after, cmp) {
                        Some(child) => {
                            current_node =
                                IndexNode::from_child_ref(child, index, branch_node.alias.clone());
                        }
                        None => return Ok(None), // target is past end of index
                    }
                }
                ResolvedNode::Leaf {
                    flakes,
                    node: leaf_node,
                } => {
                    return Ok(Some(LeafState {
                        flakes: flakes.to_vec(),
                        pos: 0,
                        rhs: leaf_node.rhs.clone(),
                    }));
                }
            }
        }
    }

    /// Find the child whose range should contain `target`.
    ///
    /// Uses gap-safe floor logic:
    /// 1. Binary-search for the last child where `child.first <= target`.
    /// 2. If the selected child's `rhs < target` (gap, normal mode)
    ///    or `rhs <= target` (after mode), advance to the next child.
    /// 3. `rhs == None` is treated as +∞ (never advance).
    ///
    /// Returns `None` if target is past all children.
    fn floor_child<'a>(
        children: &'a [ChildRef],
        target: &Flake,
        after: bool,
        cmp: fn(&Flake, &Flake) -> std::cmp::Ordering,
    ) -> Option<&'a ChildRef> {
        if children.is_empty() {
            return None;
        }

        // Find the last child where first <= target.
        // partition_point returns the first index where the predicate is false,
        // so we get the count of children where first <= target.
        let pp = children.partition_point(|child| match &child.first {
            None => true,                      // leftmost: always <= target
            Some(_) if child.leftmost => true, // leftmost flag: always <= target
            Some(first) => cmp(first, target) != std::cmp::Ordering::Greater,
        });

        let mut idx = if pp == 0 { 0 } else { pp - 1 };

        // Gap fix: check if the selected child's rhs is before the target
        if let Some(ref rhs) = children[idx].rhs {
            let ord = cmp(rhs, target);
            let should_advance = if after {
                // After mode: advance if rhs <= target
                ord != std::cmp::Ordering::Greater
            } else {
                // Normal mode: advance if rhs < target (gap)
                ord == std::cmp::Ordering::Less
            };

            if should_advance {
                idx += 1;
                if idx >= children.len() {
                    return None; // past end of index
                }
            }
        }
        // rhs == None → +∞, never advance

        Some(&children[idx])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_match_builders() {
        let s = Sid::new(1, "test");
        let p = Sid::new(2, "prop");

        let m1 = RangeMatch::subject(s.clone());
        assert_eq!(m1.s, Some(s.clone()));
        assert!(m1.p.is_none());

        let m2 = RangeMatch::subject_predicate(s.clone(), p.clone());
        assert_eq!(m2.s, Some(s));
        assert_eq!(m2.p, Some(p));
    }

    #[test]
    fn test_trim_to_range() {
        use crate::comparator::cmp_spot;

        fn make_flake(s: i32) -> Flake {
            Flake::new(
                Sid::new(s, ""),
                Sid::new(1, ""),
                FlakeValue::Long(1),
                Sid::new(2, ""),
                1,
                true,
                None,
            )
        }

        let flakes: Vec<_> = (0..10).map(make_flake).collect();
        let start = make_flake(3);
        let end = make_flake(7);

        let trimmed = trim_to_range(&flakes, &start, &end, cmp_spot);
        assert_eq!(trimmed.len(), 5); // 3, 4, 5, 6, 7

        // Verify bounds
        assert_eq!(trimmed[0].s.namespace_code, 3);
        assert_eq!(trimmed[4].s.namespace_code, 7);
    }

    #[test]
    fn test_subject_pagination() {
        fn make_flake(s: i32, p: i32) -> Flake {
            Flake::new(
                Sid::new(s, ""),
                Sid::new(p, ""),
                FlakeValue::Long(1),
                Sid::new(2, ""),
                1,
                true,
                None,
            )
        }

        // 3 subjects, 2 flakes each
        let flakes = vec![
            make_flake(1, 1),
            make_flake(1, 2),
            make_flake(2, 1),
            make_flake(2, 2),
            make_flake(3, 1),
            make_flake(3, 2),
        ];

        // Offset 1, limit 1 -> subject 2 only
        let result = apply_subject_pagination(flakes.clone(), Some(1), Some(1));
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].s.namespace_code, 2);

        // No pagination
        let result = apply_subject_pagination(flakes.clone(), None, None);
        assert_eq!(result.len(), 6);
    }

    #[test]
    fn test_object_bounds_matches() {
        // Test inclusive lower bound
        let bounds = ObjectBounds::new().with_lower(FlakeValue::Long(10), true);
        assert!(!bounds.matches(&FlakeValue::Long(9)));
        assert!(bounds.matches(&FlakeValue::Long(10)));
        assert!(bounds.matches(&FlakeValue::Long(11)));

        // Test exclusive lower bound
        let bounds = ObjectBounds::new().with_lower(FlakeValue::Long(10), false);
        assert!(!bounds.matches(&FlakeValue::Long(9)));
        assert!(!bounds.matches(&FlakeValue::Long(10)));
        assert!(bounds.matches(&FlakeValue::Long(11)));

        // Test inclusive upper bound
        let bounds = ObjectBounds::new().with_upper(FlakeValue::Long(100), true);
        assert!(bounds.matches(&FlakeValue::Long(99)));
        assert!(bounds.matches(&FlakeValue::Long(100)));
        assert!(!bounds.matches(&FlakeValue::Long(101)));

        // Test exclusive upper bound
        let bounds = ObjectBounds::new().with_upper(FlakeValue::Long(100), false);
        assert!(bounds.matches(&FlakeValue::Long(99)));
        assert!(!bounds.matches(&FlakeValue::Long(100)));
        assert!(!bounds.matches(&FlakeValue::Long(101)));
    }

    #[test]
    fn test_object_bounds_two_sided() {
        // Test both bounds: 10 < x < 100
        let bounds = ObjectBounds::new()
            .with_lower(FlakeValue::Long(10), false)
            .with_upper(FlakeValue::Long(100), false);
        assert!(!bounds.matches(&FlakeValue::Long(10)));
        assert!(bounds.matches(&FlakeValue::Long(11)));
        assert!(bounds.matches(&FlakeValue::Long(50)));
        assert!(bounds.matches(&FlakeValue::Long(99)));
        assert!(!bounds.matches(&FlakeValue::Long(100)));

        // Test both bounds: 10 <= x <= 100
        let bounds = ObjectBounds::new()
            .with_lower(FlakeValue::Long(10), true)
            .with_upper(FlakeValue::Long(100), true);
        assert!(!bounds.matches(&FlakeValue::Long(9)));
        assert!(bounds.matches(&FlakeValue::Long(10)));
        assert!(bounds.matches(&FlakeValue::Long(100)));
        assert!(!bounds.matches(&FlakeValue::Long(101)));
    }

    #[test]
    fn test_object_bounds_with_doubles() {
        let bounds = ObjectBounds::new()
            .with_lower(FlakeValue::Double(0.0.into()), true)
            .with_upper(FlakeValue::Double(1.0.into()), false);

        assert!(!bounds.matches(&FlakeValue::Double((-0.1).into())));
        assert!(bounds.matches(&FlakeValue::Double(0.0.into())));
        assert!(bounds.matches(&FlakeValue::Double(0.5.into())));
        assert!(bounds.matches(&FlakeValue::Double(0.99.into())));
        assert!(!bounds.matches(&FlakeValue::Double(1.0.into())));
    }

    #[test]
    fn test_object_bounds_type_mismatch() {
        // Long bounds should not match strings
        let bounds = ObjectBounds::new().with_lower(FlakeValue::Long(10), true);
        assert!(!bounds.matches(&FlakeValue::String("hello".to_string())));

        // String bounds should match strings with lexical comparison
        let bounds = ObjectBounds::new()
            .with_lower(FlakeValue::String("b".to_string()), true)
            .with_upper(FlakeValue::String("d".to_string()), false);
        assert!(!bounds.matches(&FlakeValue::String("a".to_string())));
        assert!(bounds.matches(&FlakeValue::String("b".to_string())));
        assert!(bounds.matches(&FlakeValue::String("c".to_string())));
        assert!(!bounds.matches(&FlakeValue::String("d".to_string())));
    }

    #[test]
    fn test_object_bounds_numeric_class_comparison() {
        // Long bounds should match Double values (numeric class comparison)
        // This is Clojure parity: "number is a number"
        let bounds = ObjectBounds::new()
            .with_lower(FlakeValue::Long(10), true)
            .with_upper(FlakeValue::Long(100), false);

        // Double 15.5 is within [10, 100)
        assert!(bounds.matches(&FlakeValue::Double(15.5)));
        // Double 9.9 is below lower bound
        assert!(!bounds.matches(&FlakeValue::Double(9.9)));
        // Double 100.0 is at exclusive upper bound (should not match)
        assert!(!bounds.matches(&FlakeValue::Double(100.0)));

        // Double bounds should match Long values
        let bounds = ObjectBounds::new().with_lower(FlakeValue::Double(3.5), true);
        // Long 4 > 3.5, should match
        assert!(bounds.matches(&FlakeValue::Long(4)));
        // Long 3 < 3.5, should not match
        assert!(!bounds.matches(&FlakeValue::Long(3)));
    }

    #[test]
    fn test_object_bounds_mixed_numeric_range() {
        // Query: ?score > 3 (where 3 is Long)
        // Should match Double values like 3.5
        let bounds = ObjectBounds::new().with_lower(FlakeValue::Long(3), false); // exclusive: > 3

        assert!(bounds.matches(&FlakeValue::Double(3.5))); // 3.5 > 3 ✓
        assert!(bounds.matches(&FlakeValue::Long(4))); // 4 > 3 ✓
        assert!(!bounds.matches(&FlakeValue::Long(3))); // 3 not > 3
        assert!(!bounds.matches(&FlakeValue::Double(3.0))); // 3.0 not > 3
        assert!(!bounds.matches(&FlakeValue::Double(2.9))); // 2.9 not > 3
    }

    #[test]
    fn test_object_bounds_empty() {
        let bounds = ObjectBounds::new();
        assert!(bounds.is_empty());
        // Empty bounds match everything
        assert!(bounds.matches(&FlakeValue::Long(0)));
        assert!(bounds.matches(&FlakeValue::Long(i64::MAX)));
        assert!(bounds.matches(&FlakeValue::String("anything".to_string())));
    }

    /// Integration test: run actual range queries against test database
    ///
    /// Run with: cargo test integration_range_query -- --ignored --nocapture
    #[tokio::test]
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    #[ignore = "Requires external test-database/ directory"]
    async fn integration_range_query_subject() {
        use crate::cache::SimpleCache;
        use crate::db::Db;
        use crate::storage::FileStorage;
        use std::path::PathBuf;

        // Path to test database
        let test_db_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("test-database");

        if !test_db_path.exists() {
            eprintln!("Test database not found, skipping");
            return;
        }

        let storage = FileStorage::new(&test_db_path);
        let cache = SimpleCache::new(10000);

        // Find a root file
        let root_dir = test_db_path.join("test/range-scan/index/root");
        let root_file = std::fs::read_dir(&root_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "json").unwrap_or(false))
            .next()
            .unwrap();

        let root_address = format!(
            "fluree:file://test/range-scan/index/root/{}",
            root_file.file_name().to_string_lossy()
        );

        let db = Db::load(storage, cache, &root_address).await.unwrap();
        println!("Loaded db at t={}", db.t);

        // Find the namespace code for http://example.org/
        let example_ns_code = db
            .namespace_codes
            .iter()
            .find(|(_, v)| v.as_str() == "http://example.org/")
            .map(|(k, _)| *k)
            .expect("Should have example.org namespace");

        println!("example.org namespace code: {}", example_ns_code);

        // Query for a specific person's flakes
        // Based on data model: person-001-000 should exist at T=1
        let subject = Sid::new(example_ns_code, "person-001-000");
        let match_val = RangeMatch::subject(subject.clone());

        println!("Querying SPOT index for subject: {:?}", subject);

        let results = range(
            &db,
            IndexType::Spot,
            RangeTest::Eq,
            match_val,
            RangeOptions::default(),
        )
        .await
        .unwrap();

        println!("Found {} flakes for person-001-000", results.len());

        // person-001-000 should have 7 properties:
        // rdf:type, ex:name, ex:age, ex:score, ex:active, ex:category, ex:birthYear
        // Score = T*100 + P = 1*100 + 0 = 100
        assert!(
            results.len() >= 7,
            "Expected at least 7 flakes, got {}",
            results.len()
        );

        // Print the flakes for debugging
        for flake in &results {
            let p_name = &flake.p.name;
            println!("  {:?} = {:?} (t={})", p_name, flake.o, flake.t);
        }

        // Verify score value
        let score_flake = results
            .iter()
            .find(|f| f.p.name.as_ref() == "score")
            .expect("Should have score property");

        match &score_flake.o {
            FlakeValue::Long(v) => assert_eq!(*v, 100, "Score should be 100"),
            FlakeValue::String(s) => assert_eq!(s, "100", "Score should be '100'"),
            other => panic!("Unexpected score type: {:?}", other),
        }

        println!("Integration test passed!");
    }

    /// Integration test: verify range query returns no results for deleted subjects
    #[tokio::test]
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    #[ignore = "Requires external test-database/ directory"]
    async fn integration_range_query_deleted() {
        use crate::cache::SimpleCache;
        use crate::db::Db;
        use crate::storage::FileStorage;
        use std::path::PathBuf;

        let test_db_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("test-database");

        if !test_db_path.exists() {
            return;
        }

        let storage = FileStorage::new(&test_db_path);
        let cache = SimpleCache::new(10000);

        let root_dir = test_db_path.join("test/range-scan/index/root");
        let root_file = std::fs::read_dir(&root_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "json").unwrap_or(false))
            .next()
            .unwrap();

        let root_address = format!(
            "fluree:file://test/range-scan/index/root/{}",
            root_file.file_name().to_string_lossy()
        );

        let db = Db::load(storage, cache, &root_address).await.unwrap();

        // At T=100, persons from T=5 (scores 500-599) should be deleted
        // person-005-000 has score=500, should be deleted at T=20
        let example_ns_code = db
            .namespace_codes
            .iter()
            .find(|(_, v)| v.as_str() == "http://example.org/")
            .map(|(k, _)| *k)
            .unwrap();

        // This person should exist (T=1, not in any deletion batch)
        let existing = Sid::new(example_ns_code, "person-001-050");
        let match_existing = RangeMatch::subject(existing);

        let results = range(
            &db,
            IndexType::Spot,
            RangeTest::Eq,
            match_existing,
            RangeOptions::default(),
        )
        .await
        .unwrap();

        println!("person-001-050 has {} flakes", results.len());
        assert!(results.len() >= 7, "person-001-050 should exist");

        println!("Deletion test passed!");
    }

    /// Integration test: time travel queries at different t values
    ///
    /// Tests that we can query "as of" a specific transaction time.
    #[tokio::test]
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    #[ignore = "Requires external test-database/ directory"]
    async fn integration_time_travel() {
        use crate::cache::SimpleCache;
        use crate::db::Db;
        use crate::storage::FileStorage;
        use std::path::PathBuf;

        let test_db_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("test-database");

        if !test_db_path.exists() {
            eprintln!("Test database not found, skipping");
            return;
        }

        let storage = FileStorage::new(&test_db_path);
        let cache = SimpleCache::new(10000);

        let root_dir = test_db_path.join("test/range-scan/index/root");
        let root_file = std::fs::read_dir(&root_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "json").unwrap_or(false))
            .next()
            .unwrap();

        let root_address = format!(
            "fluree:file://test/range-scan/index/root/{}",
            root_file.file_name().to_string_lossy()
        );

        let db = Db::load(storage, cache, &root_address).await.unwrap();
        println!("Loaded db at t={}", db.t);

        let example_ns_code = db
            .namespace_codes
            .iter()
            .find(|(_, v)| v.as_str() == "http://example.org/")
            .map(|(k, _)| *k)
            .unwrap();

        // person-001-000 was created at T=1
        let subject = Sid::new(example_ns_code, "person-001-000");
        let match_val = RangeMatch::subject(subject.clone());

        // Query at T=1 - should see exactly the flakes created at T=1
        println!("\n--- Query as-of T=1 ---");
        let results_t1 = range(
            &db,
            IndexType::Spot,
            RangeTest::Eq,
            match_val.clone(),
            RangeOptions::default().with_to_t(1),
        )
        .await
        .unwrap();

        println!("Found {} flakes at T=1", results_t1.len());
        for flake in &results_t1 {
            println!("  {:?} = {:?} (t={})", flake.p.name, flake.o, flake.t);
            assert_eq!(flake.t, 1, "All flakes should be from T=1");
        }
        assert_eq!(results_t1.len(), 7, "Should have 7 flakes at T=1");

        // Query at T=50 - should see same flakes (no changes to this person)
        println!("\n--- Query as-of T=50 ---");
        let results_t50 = range(
            &db,
            IndexType::Spot,
            RangeTest::Eq,
            match_val.clone(),
            RangeOptions::default().with_to_t(50),
        )
        .await
        .unwrap();

        println!("Found {} flakes at T=50", results_t50.len());
        assert_eq!(results_t50.len(), 7, "Should have same 7 flakes at T=50");

        // Query latest (no to_t) - should be same
        println!("\n--- Query at latest T ---");
        let results_latest = range(
            &db,
            IndexType::Spot,
            RangeTest::Eq,
            match_val.clone(),
            RangeOptions::default(),
        )
        .await
        .unwrap();

        println!("Found {} flakes at latest", results_latest.len());
        assert_eq!(
            results_latest.len(),
            7,
            "Should have same 7 flakes at latest"
        );

        // Test from_t - query changes in a time range
        println!("\n--- Query time range T=1 to T=10 ---");
        let results_range = range(
            &db,
            IndexType::Spot,
            RangeTest::Eq,
            match_val.clone(),
            RangeOptions::default().with_time_range(1, 10),
        )
        .await
        .unwrap();

        println!("Found {} flakes in range T=1-10", results_range.len());
        for flake in &results_range {
            assert!(flake.t >= 1 && flake.t <= 10, "Flake t should be in range");
        }

        println!("\nTime travel test passed!");
    }

    /// Integration test: verify querying before a person exists returns nothing
    #[tokio::test]
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    #[ignore = "Requires external test-database/ directory"]
    async fn integration_time_travel_before_exists() {
        use crate::cache::SimpleCache;
        use crate::db::Db;
        use crate::storage::FileStorage;
        use std::path::PathBuf;

        let test_db_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("test-database");

        if !test_db_path.exists() {
            return;
        }

        let storage = FileStorage::new(&test_db_path);
        let cache = SimpleCache::new(10000);

        let root_dir = test_db_path.join("test/range-scan/index/root");
        let root_file = std::fs::read_dir(&root_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "json").unwrap_or(false))
            .next()
            .unwrap();

        let root_address = format!(
            "fluree:file://test/range-scan/index/root/{}",
            root_file.file_name().to_string_lossy()
        );

        let db = Db::load(storage, cache, &root_address).await.unwrap();

        let example_ns_code = db
            .namespace_codes
            .iter()
            .find(|(_, v)| v.as_str() == "http://example.org/")
            .map(|(k, _)| *k)
            .unwrap();

        // person-050-000 was created at T=50
        // Query at T=49 should return nothing
        let subject = Sid::new(example_ns_code, "person-050-000");
        let match_val = RangeMatch::subject(subject);

        println!("Querying person-050-000 at T=49 (before creation)");
        let results_before = range(
            &db,
            IndexType::Spot,
            RangeTest::Eq,
            match_val.clone(),
            RangeOptions::default().with_to_t(49),
        )
        .await
        .unwrap();

        println!("Found {} flakes at T=49", results_before.len());
        assert_eq!(results_before.len(), 0, "Should find nothing before T=50");

        // Query at T=50 should return the person
        println!("Querying person-050-000 at T=50 (at creation)");
        let results_at = range(
            &db,
            IndexType::Spot,
            RangeTest::Eq,
            match_val,
            RangeOptions::default().with_to_t(50),
        )
        .await
        .unwrap();

        println!("Found {} flakes at T=50", results_at.len());
        assert_eq!(results_at.len(), 7, "Should find 7 flakes at T=50");

        println!("Time travel before-exists test passed!");
    }

    /// Benchmark test: measure range query performance
    ///
    /// Run with: cargo test benchmark_range_query -- --ignored --nocapture
    #[tokio::test]
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    #[ignore = "Benchmark: requires external test-database/ directory"]
    async fn benchmark_range_query() {
        use crate::cache::SimpleCache;
        use crate::db::Db;
        use crate::storage::FileStorage;
        use std::path::PathBuf;
        use std::time::Instant;

        let test_db_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("test-database");

        if !test_db_path.exists() {
            eprintln!("Test database not found, skipping benchmark");
            return;
        }

        let storage = FileStorage::new(&test_db_path);
        let cache = SimpleCache::new(10000);

        let root_dir = test_db_path.join("test/range-scan/index/root");
        let root_file = std::fs::read_dir(&root_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "json").unwrap_or(false))
            .next()
            .unwrap();

        let root_address = format!(
            "fluree:file://test/range-scan/index/root/{}",
            root_file.file_name().to_string_lossy()
        );

        let db = Db::load(storage, cache, &root_address).await.unwrap();

        let example_ns_code = db
            .namespace_codes
            .iter()
            .find(|(_, v)| v.as_str() == "http://example.org/")
            .map(|(k, _)| *k)
            .unwrap();

        println!("\n=== Rust Range Query Benchmark ===");
        println!("Database: t={}, version={}", db.t, db.version);

        // Apples-to-apples with the Clojure hot-cache harness:
        // - fixed subject pool of 1000 subjects (t=1..10, p=0..99)
        // - deterministic query schedule with fixed seed
        let subjects: Vec<Sid> = (1..=10)
            .flat_map(|t| {
                (0..100)
                    .map(move |p| Sid::new(example_ns_code, format!("person-{:03}-{:03}", t, p)))
            })
            .collect();
        assert_eq!(subjects.len(), 1000);

        fn xorshift64(state: &mut u64) -> u64 {
            // Simple deterministic PRNG for benchmarks (no external deps)
            let mut x = *state;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *state = x;
            x
        }

        // Seed used for deterministic schedules in benchmarks.

        // Warmup: query the entire pool to populate cache (fully hot)
        println!("\n--- Warmup Phase ---");
        let warmup_start = Instant::now();
        let mut warmup_flakes = 0;
        for subject in &subjects {
            let match_val = RangeMatch::subject(subject.clone());
            let results = range(
                &db,
                IndexType::Spot,
                RangeTest::Eq,
                match_val,
                RangeOptions::default(),
            )
            .await
            .unwrap();
            warmup_flakes += results.len();
        }
        let warmup_time = warmup_start.elapsed();
        println!(
            "Warmup: {} queries, {} flakes in {:?}",
            1000, warmup_flakes, warmup_time
        );

        let stats_after_warmup = db.cache.stats();
        println!(
            "Cache after warmup: {} entries, {} hits, {} misses, {:.1}% hit rate",
            db.cache.len(),
            stats_after_warmup.hits,
            stats_after_warmup.misses,
            stats_after_warmup.hit_rate() * 100.0
        );

        // Reset stats for benchmark
        db.cache.reset_stats();

        // Benchmark 1: Single subject queries (hot cache, steady-state)
        //
        // Note: keep iteration count large enough that timing noise doesn't dominate.
        println!("\n--- Benchmark 1: Single Subject Queries (Hot Cache) ---");
        let num_queries: usize = 100_000;
        let mut rng_state_hot: u64 = 42;
        let idxs_hot: Vec<usize> = (0..num_queries)
            .map(|_| (xorshift64(&mut rng_state_hot) as usize) % subjects.len())
            .collect();
        let start = Instant::now();
        let mut total_flakes = 0;

        for i in 0..num_queries {
            let idx = idxs_hot[i];
            let subject = subjects[idx].clone();
            let match_val = RangeMatch::subject(subject);
            let results = range(
                &db,
                IndexType::Spot,
                RangeTest::Eq,
                match_val,
                RangeOptions::default(),
            )
            .await
            .unwrap();
            total_flakes += results.len();
        }

        let elapsed = start.elapsed();
        let queries_per_sec = num_queries as f64 / elapsed.as_secs_f64();
        let flakes_per_sec = total_flakes as f64 / elapsed.as_secs_f64();
        let ns_per_query = (elapsed.as_secs_f64() * 1e9) / (num_queries as f64);

        println!("Queries: {}", num_queries);
        println!("Total flakes: {}", total_flakes);
        println!("Time: {:?}", elapsed);
        println!(
            "Throughput: {:.0} queries/sec, {:.0} flakes/sec ({:.0} ns/query)",
            queries_per_sec, flakes_per_sec, ns_per_query
        );

        let stats = db.cache.stats();
        println!(
            "Cache: {} hits, {} misses, {:.1}% hit rate",
            stats.hits,
            stats.misses,
            stats.hit_rate() * 100.0
        );

        // Benchmark 1b: Hot cache first-hit (clear cache then run once)
        //
        // This measures the miss-path for the same access pattern.
        println!(
            "\n--- Benchmark 1b: Single Subject Queries (First-Hit / Cold In-Memory Cache) ---"
        );
        db.cache.clear();
        db.cache.reset_stats();
        let start = Instant::now();
        let mut total_flakes = 0usize;
        for i in 0..num_queries {
            let idx = idxs_hot[i];
            let subject = subjects[idx].clone();
            let results = range(
                &db,
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject(subject),
                RangeOptions::default(),
            )
            .await
            .unwrap();
            total_flakes += results.len();
        }
        let elapsed = start.elapsed();
        let queries_per_sec = num_queries as f64 / elapsed.as_secs_f64();
        let ns_per_query = (elapsed.as_secs_f64() * 1e9) / (num_queries as f64);
        println!("Queries: {}", num_queries);
        println!("Total flakes: {}", total_flakes);
        println!("Time: {:?}", elapsed);
        println!(
            "Throughput: {:.0} queries/sec ({:.0} ns/query)",
            queries_per_sec, ns_per_query
        );
        let stats = db.cache.stats();
        println!(
            "Cache: {} hits, {} misses, {:.1}% hit rate",
            stats.hits,
            stats.misses,
            stats.hit_rate() * 100.0
        );

        // Benchmark 2: Time-travel queries at different t values
        //
        // Important: time-travel has a one-time cost per leaf per `to_t` to materialize
        // the leaf. To compare steady-state performance vs Clojure's warmed runs,
        // we do two passes:
        // - Pass A: warm the materialized-leaf cache for each `to_t` (not timed)
        // - Pass B: measure steady-state (timed)
        println!("\n--- Benchmark 2: Time-Travel Queries ---");
        let to_ts = [1_i64, 10, 25, 50, 75, 90];
        let queries_per_t: usize = 10_000;
        let total_queries = queries_per_t * to_ts.len();

        // Precompute a deterministic schedule of (to_t, subject Sid).
        let mut rng_state_tt: u64 = 4242;
        let tt_schedule: Vec<(i64, Sid)> = (0..total_queries)
            .map(|i| {
                let to_t = to_ts[i % to_ts.len()];
                let t = ((xorshift64(&mut rng_state_tt) as usize) % (to_t as usize).min(90)) + 1;
                let p = (xorshift64(&mut rng_state_tt) as usize) % 100;
                let name = format!("person-{:03}-{:03}", t, p);
                let sid = db.sid_interner.intern(example_ns_code, &name);
                (to_t, sid)
            })
            .collect();

        println!("Warm pass (not timed): {} queries", total_queries);
        for (to_t, subject) in &tt_schedule {
            let _ = range(
                &db,
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject(subject.clone()),
                RangeOptions::default().with_to_t(*to_t),
            )
            .await
            .unwrap();
        }

        db.cache.reset_stats();
        let start = Instant::now();
        let mut total_flakes = 0usize;

        for (to_t, subject) in &tt_schedule {
            let results = range(
                &db,
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject(subject.clone()),
                RangeOptions::default().with_to_t(*to_t),
            )
            .await
            .unwrap();
            total_flakes += results.len();
        }

        let elapsed = start.elapsed();
        let queries_per_sec = (total_queries as f64) / elapsed.as_secs_f64();
        let ns_per_query = (elapsed.as_secs_f64() * 1e9) / (total_queries as f64);

        println!("Queries: {}", total_queries);
        println!("Total flakes: {}", total_flakes);
        println!("Time: {:?}", elapsed);
        println!(
            "Throughput: {:.0} queries/sec ({:.0} ns/query)",
            queries_per_sec, ns_per_query
        );

        let stats = db.cache.stats();
        println!(
            "Cache: {} hits, {} misses, {:.1}% hit rate",
            stats.hits,
            stats.misses,
            stats.hit_rate() * 100.0
        );

        // Benchmark 3: Cold cache (clear and re-query)
        //
        // Apples-to-apples with Clojure cold-cache:
        // - clear in-memory cache
        // - run queries against a fixed subject pool (t=1..10, p=0..99) so we can see
        //   the "first-hit" miss cost plus subsequent hits within the same run.
        println!("\n--- Benchmark 3: Cold Cache Performance ---");
        db.cache.clear();
        db.cache.reset_stats();
        let start = Instant::now();
        let mut total_flakes = 0;

        let cold_queries: usize = 1000;
        let mut rng_state_cold: u64 = 9001;
        let idxs_cold: Vec<usize> = (0..cold_queries)
            .map(|_| (xorshift64(&mut rng_state_cold) as usize) % subjects.len())
            .collect();

        for i in 0..cold_queries {
            let idx = idxs_cold[i];
            let match_val = RangeMatch::subject(subjects[idx].clone());
            let results = range(
                &db,
                IndexType::Spot,
                RangeTest::Eq,
                match_val,
                RangeOptions::default(),
            )
            .await
            .unwrap();
            total_flakes += results.len();
        }

        let elapsed = start.elapsed();
        let queries_per_sec = (cold_queries as f64) / elapsed.as_secs_f64();
        let ns_per_query = (elapsed.as_secs_f64() * 1e9) / (cold_queries as f64);

        println!("Queries: {}", cold_queries);
        println!("Total flakes: {}", total_flakes);
        println!("Time: {:?}", elapsed);
        println!(
            "Throughput: {:.0} queries/sec ({:.0} ns/query)",
            queries_per_sec, ns_per_query
        );

        let stats = db.cache.stats();
        println!(
            "Cache: {} hits, {} misses, {:.1}% hit rate",
            stats.hits,
            stats.misses,
            stats.hit_rate() * 100.0
        );

        println!("\n=== Benchmark Complete ===");
    }

    /// Verification test to compare results with Clojure implementation.
    ///
    /// Run with: cargo test --release verify_range_results -- --ignored --nocapture
    #[tokio::test]
    #[cfg(all(feature = "native", not(target_arch = "wasm32")))]
    #[ignore = "Verification: requires external test-database/ directory"]
    async fn verify_range_results() {
        use crate::cache::SimpleCache;
        use crate::db::Db;
        use crate::flake::Flake;
        use crate::storage::FileStorage;
        use std::path::PathBuf;

        let test_db_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("test-database");

        if !test_db_path.exists() {
            eprintln!("Test database not found at {:?}", test_db_path);
            return;
        }

        let storage = FileStorage::new(&test_db_path);
        let cache = SimpleCache::new(10000);

        let root_dir = test_db_path.join("test/range-scan/index/root");
        let root_file = std::fs::read_dir(&root_dir)
            .expect("Failed to read root dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "json").unwrap_or(false))
            .next()
            .expect("No root file found");

        let root_address = format!(
            "fluree:file://test/range-scan/index/root/{}",
            root_file.file_name().to_string_lossy()
        );

        let db = Db::load(storage, cache, &root_address)
            .await
            .expect("Failed to load database");

        println!("\n");
        println!("╔══════════════════════════════════════════════════════════════╗");
        println!("║         Fluree Rust Range Query Verification                 ║");
        println!("╚══════════════════════════════════════════════════════════════╝");
        println!();
        println!("Database: t={}, version={}", db.t, db.version);

        // Helper to format flake summary
        fn flake_summary(f: &Flake) -> String {
            format!(
                "s={} p={} o={:?} dt={} t={} op={}",
                f.s, f.p, f.o, f.dt, f.t, f.op
            )
        }

        // Helper to print results summary
        fn print_summary(label: &str, flakes: &[Flake]) {
            println!("  {}:", label);
            println!("    count: {}", flakes.len());
            if flakes.is_empty() {
                println!("    first: nil");
                println!("    last:  nil");
            } else {
                // Sort for consistent ordering
                let mut sorted = flakes.to_vec();
                sorted.sort_by(|a, b| {
                    (&a.s, &a.p, &a.o, &a.dt, a.t).cmp(&(&b.s, &b.p, &b.o, &b.dt, b.t))
                });
                println!("    first: {}", flake_summary(&sorted[0]));
                println!("    last:  {}", flake_summary(sorted.last().unwrap()));
            }
        }

        // Get the example.org namespace code
        let example_ns_code = db
            .namespace_codes
            .iter()
            .find(|(_, v)| v.as_str() == "http://example.org/")
            .map(|(k, _)| *k)
            .expect("Should have example.org namespace");

        // Helper to create person SID
        let person_sid = |t: i64, p: i64| -> Sid {
            let name = format!("person-{:03}-{:03}", t, p);
            Sid::new(example_ns_code, name)
        };

        // =====================================================================
        // Test 1: Single subject queries at current t
        // =====================================================================
        println!("\n{}", "=".repeat(60));
        println!("TEST 1: Single Subject Queries (Current T)");
        println!("{}", "=".repeat(60));

        // Query specific subjects
        let test_subjects = [(1, 0), (1, 50), (5, 25), (10, 99), (50, 0)];

        for (t, p) in test_subjects {
            let sid = person_sid(t, p);
            println!("\n--- Single Subject: t={} p={} (sid={}) ---", t, p, sid);

            let results = range(
                &db,
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject(sid),
                RangeOptions::default(),
            )
            .await
            .unwrap();

            print_summary("Results", &results);
        }

        // =====================================================================
        // Test 2: Time travel queries
        // =====================================================================
        println!("\n{}", "=".repeat(60));
        println!("TEST 2: Time Travel Queries");
        println!("{}", "=".repeat(60));

        // (t, p, to_t)
        let tt_tests = [
            (1, 0, 10),
            (1, 0, 50),
            (5, 25, 15),
            (5, 25, 25),
            (10, 99, 10),
        ];

        for (t, p, to_t) in tt_tests {
            let sid = person_sid(t, p);
            println!(
                "\n--- Time Travel: t={} p={} to_t={} (sid={}) ---",
                t, p, to_t, sid
            );

            let results = range(
                &db,
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject(sid),
                RangeOptions::default().with_to_t(to_t),
            )
            .await
            .unwrap();

            print_summary("Results", &results);
        }

        // =====================================================================
        // Test 3: Aggregate verification
        // =====================================================================
        println!("\n{}", "=".repeat(60));
        println!("TEST 3: Aggregate Verification");
        println!("{}", "=".repeat(60));

        // Query 100 subjects (t=1..10, p=0..9) at current t
        println!("\nQuerying 100 subjects (t=1..10, p=0..9) at current t...");
        let mut all_flakes = Vec::new();
        for t in 1..=10 {
            for p in 0..10 {
                let sid = person_sid(t, p);
                let results = range(
                    &db,
                    IndexType::Spot,
                    RangeTest::Eq,
                    RangeMatch::subject(sid),
                    RangeOptions::default(),
                )
                .await
                .unwrap();
                all_flakes.extend(results);
            }
        }
        print_summary("Aggregate (100 subjects)", &all_flakes);

        // Time travel aggregate at to_t=50
        println!("\nQuerying 100 subjects (t=1..10, p=0..9) at to_t=50...");
        let mut all_flakes_tt = Vec::new();
        for t in 1..=10 {
            for p in 0..10 {
                let sid = person_sid(t, p);
                let results = range(
                    &db,
                    IndexType::Spot,
                    RangeTest::Eq,
                    RangeMatch::subject(sid),
                    RangeOptions::default().with_to_t(50),
                )
                .await
                .unwrap();
                all_flakes_tt.extend(results);
            }
        }
        print_summary("Aggregate at to_t=50", &all_flakes_tt);

        println!("\n{}", "=".repeat(60));
        println!("Verification Complete");
        println!("{}", "=".repeat(60));
    }
}
