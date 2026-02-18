//! Tab-separated values formatter (high-performance path)
//!
//! Bypasses IRI compaction, JSON DOM construction, and JSON serialization entirely.
//! Resolves bindings directly to bytes in a pre-allocated buffer.
//!
//! # Performance
//!
//! - Writes to `Vec<u8>` via `extend_from_slice` (no `fmt::Write` overhead)
//! - Uses `itoa`/`ryu` for zero-alloc numeric formatting
//! - Column indices computed once per batch (not per cell)
//! - No `IriCompactor` or `ContextCompactor` construction
//! - No `serde_json::Value` allocation
//!
//! # Format
//!
//! Plain TSV: header row with variable names (no `?` prefix), followed by
//! tab-separated data rows. IRIs are full (from `db.namespace_codes`), not
//! compacted via `@context`. This is faster and more useful for data consumers
//! than the W3C SPARQL TSV format (which requires angle-bracketed IRIs).

use super::{FormatError, Result};
use crate::QueryResult;
use fluree_db_core::value_id::ObjKind;
use fluree_db_core::{Db, FlakeValue, Sid};
use fluree_db_indexer::run_index::BinaryIndexStore;
use fluree_db_query::binding::Binding;
use fluree_db_query::{SelectMode, VarId};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Format query results as TSV bytes.
///
/// This is the primary entry point for server use. Returns raw bytes that can
/// be sent directly as an HTTP response body without UTF-8 validation overhead.
///
/// # Errors
///
/// Returns `FormatError::InvalidBinding` for non-tabular results (CONSTRUCT,
/// graph crawl) which cannot be meaningfully represented as TSV.
pub fn format_tsv_bytes(result: &QueryResult, db: &Db) -> Result<Vec<u8>> {
    reject_non_tabular(result)?;

    let store = result.binary_store.as_deref();
    let select_vars = resolve_select_vars(result);

    // Pre-allocate: rough estimate of 80 bytes per cell (saturating to avoid overflow)
    let est_size = (result.row_count() + 1)
        .saturating_mul(select_vars.len())
        .saturating_mul(80);
    let mut out = Vec::with_capacity(est_size);

    write_header(&mut out, result, &select_vars);
    out.push(b'\n');

    write_data_rows(&mut out, result, &select_vars, db, store, None)?;

    Ok(out)
}

/// Format query results as a TSV string.
///
/// Convenience wrapper over `format_tsv_bytes` for CLI and test use.
pub fn format_tsv(result: &QueryResult, db: &Db) -> Result<String> {
    let bytes = format_tsv_bytes(result, db)?;
    // Safety: all writes are ASCII or valid UTF-8 (IRIs, numeric formatting,
    // sanitized strings). In debug builds we validate; in release we skip.
    #[cfg(debug_assertions)]
    {
        Ok(String::from_utf8(bytes).expect("TSV output should always be valid UTF-8"))
    }
    #[cfg(not(debug_assertions))]
    {
        // SAFETY: We only write ASCII bytes, valid UTF-8 strings from IRIs/literals,
        // and numeric formatting (itoa/ryu produce ASCII). sanitize_and_push replaces
        // control chars with ASCII space.
        Ok(unsafe { String::from_utf8_unchecked(bytes) })
    }
}

/// Format TSV with a row limit (for benchmark/preview use).
///
/// Returns `(tsv_bytes, total_row_count)`.
pub fn format_tsv_bytes_limited(
    result: &QueryResult,
    db: &Db,
    limit: usize,
) -> Result<(Vec<u8>, usize)> {
    reject_non_tabular(result)?;

    let store = result.binary_store.as_deref();
    let select_vars = resolve_select_vars(result);
    let total = result.row_count();

    let est_size = (limit.min(total) + 1)
        .saturating_mul(select_vars.len())
        .saturating_mul(80);
    let mut out = Vec::with_capacity(est_size);

    write_header(&mut out, result, &select_vars);
    out.push(b'\n');

    write_data_rows(&mut out, result, &select_vars, db, store, Some(limit))?;

    Ok((out, total))
}

/// Format TSV string with a row limit (for CLI bench/preview).
///
/// Returns `(tsv_string, total_row_count)`.
pub fn format_tsv_limited(result: &QueryResult, db: &Db, limit: usize) -> Result<(String, usize)> {
    let (bytes, total) = format_tsv_bytes_limited(result, db, limit)?;
    #[cfg(debug_assertions)]
    let s = String::from_utf8(bytes).expect("TSV output should always be valid UTF-8");
    #[cfg(not(debug_assertions))]
    let s = unsafe { String::from_utf8_unchecked(bytes) };
    Ok((s, total))
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/// Reject non-tabular results (CONSTRUCT, graph crawl) that cannot be
/// meaningfully represented as TSV.
fn reject_non_tabular(result: &QueryResult) -> Result<()> {
    if result.select_mode == SelectMode::Construct {
        return Err(FormatError::InvalidBinding(
            "TSV format not supported for CONSTRUCT queries (use JSON-LD instead)".to_string(),
        ));
    }
    if result.graph_select.is_some() {
        return Err(FormatError::InvalidBinding(
            "TSV format not supported for graph crawl queries (use JSON-LD instead)".to_string(),
        ));
    }
    Ok(())
}

/// Resolve the select variable list, handling Wildcard mode.
fn resolve_select_vars(result: &QueryResult) -> Vec<VarId> {
    match result.select_mode {
        SelectMode::Wildcard => {
            let mut pairs: Vec<(String, VarId)> = result
                .batches
                .first()
                .map(|b| {
                    b.schema()
                        .iter()
                        .copied()
                        .filter(|&vid| !result.vars.name(vid).starts_with("?__"))
                        .map(|vid| {
                            let name = result
                                .vars
                                .name(vid)
                                .strip_prefix('?')
                                .unwrap_or(result.vars.name(vid))
                                .to_string();
                            (name, vid)
                        })
                        .collect()
                })
                .unwrap_or_default();
            pairs.sort_by(|(a, _), (b, _)| a.cmp(b));
            pairs.into_iter().map(|(_, vid)| vid).collect()
        }
        _ => result.select.clone(),
    }
}

/// Write the header row (variable names without `?` prefix, tab-separated).
fn write_header(out: &mut Vec<u8>, result: &QueryResult, vars: &[VarId]) {
    for (i, &var) in vars.iter().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        let name = result.vars.name(var);
        let stripped = name.strip_prefix('?').unwrap_or(name);
        out.extend_from_slice(stripped.as_bytes());
    }
}

/// Write data rows. If `limit` is Some, stop after that many rows.
fn write_data_rows(
    out: &mut Vec<u8>,
    result: &QueryResult,
    select_vars: &[VarId],
    db: &Db,
    store: Option<&BinaryIndexStore>,
    limit: Option<usize>,
) -> Result<()> {
    let max_rows = limit.unwrap_or(usize::MAX);
    let mut emitted = 0usize;

    let select_one = result.select_mode == SelectMode::One;

    for batch in &result.batches {
        // Compute column indices once per batch (O(select_vars.len() * schema.len())).
        // This avoids the per-cell linear scan in batch.get(row, var).
        let schema = batch.schema();
        let col_indices: Vec<usize> = select_vars
            .iter()
            .map(|&v| {
                schema.iter().position(|&sv| sv == v).ok_or_else(|| {
                    FormatError::InvalidBinding(format!(
                        "Variable {:?} not found in batch schema",
                        result.vars.name(v)
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        for row in 0..batch.len() {
            for (i, &col) in col_indices.iter().enumerate() {
                if i > 0 {
                    out.push(b'\t');
                }
                let binding = batch.get_by_col(row, col);
                write_binding_cell(out, binding, db, store)?;
            }
            out.push(b'\n');

            emitted += 1;
            if emitted >= max_rows || select_one {
                return Ok(());
            }
        }
    }

    Ok(())
}

/// Write a single binding value directly to the output buffer.
///
/// This is the hot path. All variants produce bytes without intermediate String
/// allocation (except where `BinaryIndexStore` resolution returns a new String,
/// which is unavoidable).
fn write_binding_cell(
    out: &mut Vec<u8>,
    binding: &Binding,
    db: &Db,
    store: Option<&BinaryIndexStore>,
) -> Result<()> {
    match binding {
        Binding::Unbound | Binding::Poisoned => {
            // Empty cell
        }
        Binding::Sid(sid) => {
            write_sid_iri(out, db, sid);
        }
        Binding::IriMatch { iri, .. } => {
            sanitize_and_push(out, iri.as_bytes());
        }
        Binding::Iri(iri) => {
            sanitize_and_push(out, iri.as_bytes());
        }
        Binding::Lit { val, .. } => {
            write_flake_value(out, val, db);
        }
        Binding::EncodedSid { s_id } => {
            let store = require_store(store)?;
            let start = out.len();
            store.write_subject_iri_bytes(*s_id, out).map_err(|e| {
                FormatError::InvalidBinding(format!(
                    "Failed to resolve subject IRI for s_id {}: {}",
                    s_id, e
                ))
            })?;
            sanitize_in_place(out, start);
        }
        Binding::EncodedPid { p_id } => {
            let store = require_store(store)?;
            let iri = store.resolve_predicate_iri(*p_id).ok_or_else(|| {
                FormatError::InvalidBinding(format!(
                    "Failed to resolve predicate IRI for p_id {}",
                    p_id
                ))
            })?;
            sanitize_and_push(out, iri.as_bytes());
        }
        Binding::EncodedLit {
            o_kind,
            o_key,
            p_id,
            ..
        } => {
            let store = require_store(store)?;
            // Fast path: dict-tree-backed types write directly into `out`,
            // skipping the intermediate FlakeValue + String allocations.
            if *o_kind == ObjKind::LEX_ID.as_u8() || *o_kind == ObjKind::JSON_ID.as_u8() {
                let start = out.len();
                store
                    .write_string_value_bytes(*o_key as u32, out)
                    .map_err(|e| {
                        FormatError::InvalidBinding(format!(
                            "Failed to resolve string (kind={}, key={}): {}",
                            o_kind, o_key, e
                        ))
                    })?;
                sanitize_in_place(out, start);
            } else if *o_kind == ObjKind::REF_ID.as_u8() {
                let start = out.len();
                store.write_subject_iri_bytes(*o_key, out).map_err(|e| {
                    FormatError::InvalidBinding(format!(
                        "Failed to resolve ref IRI for s_id {}: {}",
                        o_key, e
                    ))
                })?;
                sanitize_in_place(out, start);
            } else {
                // Inline types (bool, int, float, date, etc.) — no dict tree,
                // decode through FlakeValue (cheap, no allocations from trees).
                let val = store.decode_value(*o_kind, *o_key, *p_id).map_err(|e| {
                    FormatError::InvalidBinding(format!(
                        "Failed to decode value (kind={}, key={}, p_id={}): {}",
                        o_kind, o_key, p_id, e
                    ))
                })?;
                write_flake_value(out, &val, db);
            }
        }
        Binding::Grouped(values) => {
            // TSV cannot represent multi-valued cells natively.
            // Semicolon-separate for pragmatic consumption.
            for (j, val) in values.iter().enumerate() {
                if j > 0 {
                    out.push(b';');
                }
                write_binding_cell(out, val, db, store)?;
            }
        }
    }
    Ok(())
}

/// Require a BinaryIndexStore for encoded binding resolution.
fn require_store(store: Option<&BinaryIndexStore>) -> Result<&BinaryIndexStore> {
    store.ok_or_else(|| {
        FormatError::InvalidBinding(
            "Encountered encoded binding but QueryResult has no binary_store".to_string(),
        )
    })
}

/// Write a Sid as full IRI using db.namespace_codes.
///
/// Writes `prefix + name` directly, sanitizing inline. No intermediate allocation.
fn write_sid_iri(out: &mut Vec<u8>, db: &Db, sid: &Sid) {
    match db.namespace_codes.get(&sid.namespace_code) {
        Some(prefix) => {
            sanitize_and_push(out, prefix.as_bytes());
            sanitize_and_push(out, sid.name.as_bytes());
        }
        None => {
            // Fallback: code:name
            let mut buf = itoa::Buffer::new();
            out.extend_from_slice(buf.format(sid.namespace_code).as_bytes());
            out.push(b':');
            sanitize_and_push(out, sid.name.as_bytes());
        }
    }
}

/// Write a FlakeValue directly to bytes.
fn write_flake_value(out: &mut Vec<u8>, val: &FlakeValue, db: &Db) {
    match val {
        FlakeValue::String(s) => sanitize_and_push(out, s.as_bytes()),
        FlakeValue::Ref(sid) => write_sid_iri(out, db, sid),
        FlakeValue::Long(n) => {
            let mut buf = itoa::Buffer::new();
            out.extend_from_slice(buf.format(*n).as_bytes());
        }
        FlakeValue::Double(d) => {
            let mut buf = ryu::Buffer::new();
            out.extend_from_slice(buf.format(*d).as_bytes());
        }
        FlakeValue::Boolean(b) => {
            out.extend_from_slice(if *b { b"true" } else { b"false" });
        }
        FlakeValue::Null => {
            // Empty cell
        }
        FlakeValue::BigInt(n) => {
            // BigInt.to_string() allocates, but BigInt values are rare
            sanitize_and_push(out, n.to_string().as_bytes());
        }
        FlakeValue::Decimal(d) => {
            sanitize_and_push(out, d.to_string().as_bytes());
        }
        FlakeValue::DateTime(dt) => {
            sanitize_and_push(out, dt.to_string().as_bytes());
        }
        FlakeValue::Date(d) => {
            sanitize_and_push(out, d.to_string().as_bytes());
        }
        FlakeValue::Time(t) => {
            sanitize_and_push(out, t.to_string().as_bytes());
        }
        FlakeValue::GYear(v) => {
            sanitize_and_push(out, v.to_string().as_bytes());
        }
        FlakeValue::GYearMonth(v) => {
            sanitize_and_push(out, v.to_string().as_bytes());
        }
        FlakeValue::GMonth(v) => {
            sanitize_and_push(out, v.to_string().as_bytes());
        }
        FlakeValue::GDay(v) => {
            sanitize_and_push(out, v.to_string().as_bytes());
        }
        FlakeValue::GMonthDay(v) => {
            sanitize_and_push(out, v.to_string().as_bytes());
        }
        FlakeValue::YearMonthDuration(v) => {
            sanitize_and_push(out, v.to_string().as_bytes());
        }
        FlakeValue::DayTimeDuration(v) => {
            sanitize_and_push(out, v.to_string().as_bytes());
        }
        FlakeValue::Duration(v) => {
            sanitize_and_push(out, v.to_string().as_bytes());
        }
        FlakeValue::Vector(v) => {
            // Vectors are rare in typical queries; allocating is acceptable
            let s = serde_json::to_string(v).unwrap_or_else(|_| "[]".to_string());
            sanitize_and_push(out, s.as_bytes());
        }
        FlakeValue::Json(json_str) => {
            sanitize_and_push(out, json_str.as_bytes());
        }
        FlakeValue::GeoPoint(v) => {
            sanitize_and_push(out, v.to_string().as_bytes());
        }
    }
}

/// Push bytes to output, replacing \t, \n, \r with space.
///
/// Operates byte-by-byte (valid since \t/\n/\r are single-byte ASCII and
/// replacement is also single-byte ASCII, so UTF-8 multi-byte sequences
/// are preserved correctly).
#[inline]
fn sanitize_and_push(out: &mut Vec<u8>, s: &[u8]) {
    out.reserve(s.len());
    for &b in s {
        match b {
            b'\t' | b'\n' | b'\r' => out.push(b' '),
            _ => out.push(b),
        }
    }
}

/// Sanitize bytes already appended to `out` starting at `start`.
///
/// Replaces \t, \n, \r with space in-place. Used after direct-to-buffer
/// writes (e.g., `write_subject_iri_bytes`) where the bytes are already in
/// the output buffer and we want to avoid a second copy.
#[inline]
fn sanitize_in_place(out: &mut [u8], start: usize) {
    for b in &mut out[start..] {
        if *b == b'\t' || *b == b'\n' || *b == b'\r' {
            *b = b' ';
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::Sid;
    use fluree_db_query::VarRegistry;
    use std::sync::Arc;

    fn make_test_db() -> Db {
        let mut db = Db::genesis("test:main");
        db.namespace_codes
            .insert(100, "http://example.org/".to_string());
        // ns code 2 = xsd: is already in default_namespace_codes via genesis()
        db
    }

    fn make_result(var_names: &[&str], rows: Vec<Vec<Binding>>) -> (QueryResult, Vec<VarId>) {
        let mut vars = VarRegistry::new();
        let var_ids: Vec<VarId> = var_names
            .iter()
            .map(|&name| vars.get_or_insert(name))
            .collect();

        let mut columns: Vec<Vec<Binding>> = vec![Vec::new(); var_ids.len()];
        for row in rows {
            for (col, binding) in row.into_iter().enumerate() {
                columns[col].push(binding);
            }
        }

        let batch = fluree_db_query::binding::Batch::new(
            Arc::from(var_ids.clone().into_boxed_slice()),
            columns,
        )
        .expect("test batch construction should not fail");

        let result = QueryResult {
            vars,
            t: 0,
            novelty: None,
            context: crate::ParsedContext::default(),
            orig_context: None,
            select: var_ids.clone(),
            select_mode: SelectMode::Many,
            batches: vec![batch],
            binary_store: None,
            construct_template: None,
            graph_select: None,
        };

        (result, var_ids)
    }

    #[test]
    fn test_empty_result() {
        let db = make_test_db();
        let (result, _) = make_result(&["?s", "?name"], vec![]);
        let tsv = format_tsv(&result, &db).unwrap();
        assert_eq!(tsv, "s\tname\n");
    }

    #[test]
    fn test_sid_binding() {
        let db = make_test_db();
        let (result, _) = make_result(&["?s"], vec![vec![Binding::Sid(Sid::new(100, "alice"))]]);
        let tsv = format_tsv(&result, &db).unwrap();
        assert_eq!(tsv, "s\nhttp://example.org/alice\n");
    }

    #[test]
    fn test_literal_bindings() {
        let db = make_test_db();
        let (result, _) = make_result(
            &["?name", "?age"],
            vec![vec![
                Binding::lit(
                    FlakeValue::String("Alice".to_string()),
                    Sid::new(2, "string"),
                ),
                Binding::lit(FlakeValue::Long(30), Sid::new(2, "long")),
            ]],
        );
        let tsv = format_tsv(&result, &db).unwrap();
        // Explicit select preserves user-specified column order
        assert_eq!(tsv, "name\tage\nAlice\t30\n");
    }

    #[test]
    fn test_sanitization() {
        let db = make_test_db();
        let (result, _) = make_result(
            &["?val"],
            vec![vec![Binding::lit(
                FlakeValue::String("hello\tworld\nfoo\rbar".to_string()),
                Sid::new(2, "string"),
            )]],
        );
        let tsv = format_tsv(&result, &db).unwrap();
        assert_eq!(tsv, "val\nhello world foo bar\n");
    }

    #[test]
    fn test_unbound_binding() {
        let db = make_test_db();
        let (result, _) = make_result(
            &["?a", "?b"],
            vec![vec![Binding::Sid(Sid::new(100, "x")), Binding::Unbound]],
        );
        let tsv = format_tsv(&result, &db).unwrap();
        assert_eq!(tsv, "a\tb\nhttp://example.org/x\t\n");
    }

    #[test]
    fn test_multiple_rows() {
        let db = make_test_db();
        let (result, _) = make_result(
            &["?s"],
            vec![
                vec![Binding::Sid(Sid::new(100, "a"))],
                vec![Binding::Sid(Sid::new(100, "b"))],
                vec![Binding::Sid(Sid::new(100, "c"))],
            ],
        );
        let tsv = format_tsv(&result, &db).unwrap();
        assert_eq!(
            tsv,
            "s\nhttp://example.org/a\nhttp://example.org/b\nhttp://example.org/c\n"
        );
    }

    #[test]
    fn test_limited_output() {
        let db = make_test_db();
        let (result, _) = make_result(
            &["?s"],
            vec![
                vec![Binding::Sid(Sid::new(100, "a"))],
                vec![Binding::Sid(Sid::new(100, "b"))],
                vec![Binding::Sid(Sid::new(100, "c"))],
            ],
        );
        let (tsv, total) = format_tsv_limited(&result, &db, 2).unwrap();
        assert_eq!(total, 3);
        assert_eq!(tsv, "s\nhttp://example.org/a\nhttp://example.org/b\n");
    }

    #[test]
    fn test_boolean_and_double() {
        let db = make_test_db();
        let (result, _) = make_result(
            &["?flag", "?score"],
            vec![vec![
                Binding::lit(FlakeValue::Boolean(true), Sid::new(2, "boolean")),
                Binding::lit(FlakeValue::Double(3.14), Sid::new(2, "double")),
            ]],
        );
        let tsv = format_tsv(&result, &db).unwrap();
        let lines: Vec<&str> = tsv.lines().collect();
        assert_eq!(lines[0], "flag\tscore");
        assert!(lines[1].starts_with("true\t"));
    }

    #[test]
    fn test_iri_binding() {
        let db = make_test_db();
        let (result, _) = make_result(
            &["?g"],
            vec![vec![Binding::Iri(Arc::from("http://example.org/graph1"))]],
        );
        let tsv = format_tsv(&result, &db).unwrap();
        assert_eq!(tsv, "g\nhttp://example.org/graph1\n");
    }

    #[test]
    fn test_grouped_binding() {
        let db = make_test_db();
        let (result, _) = make_result(
            &["?vals"],
            vec![vec![Binding::Grouped(vec![
                Binding::lit(FlakeValue::Long(1), Sid::new(2, "long")),
                Binding::lit(FlakeValue::Long(2), Sid::new(2, "long")),
                Binding::lit(FlakeValue::Long(3), Sid::new(2, "long")),
            ])]],
        );
        let tsv = format_tsv(&result, &db).unwrap();
        assert_eq!(tsv, "vals\n1;2;3\n");
    }
}
