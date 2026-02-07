use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Root directory of the rdf-tests submodule.
fn rdf_tests_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("rdf-tests")
}

/// Map a W3C test URL to a local filesystem path.
///
/// The W3C manifest files reference test resources by URL, e.g.:
///   `https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/syntax-select-expr-01.rq`
///
/// The `rdf-tests` submodule mirrors this structure at:
///   `{CARGO_MANIFEST_DIR}/rdf-tests/sparql/sparql11/syntax-query/syntax-select-expr-01.rq`
pub fn url_to_path(url: &str) -> Result<PathBuf> {
    // Strip the W3C host prefix + "rdf-tests/" to get the relative path.
    // URL:  https://w3c.github.io/rdf-tests/sparql/sparql11/...
    // Local: {CARGO_MANIFEST_DIR}/rdf-tests/sparql/sparql11/...
    let relative = url
        .strip_prefix("https://w3c.github.io/rdf-tests/")
        .or_else(|| url.strip_prefix("http://w3c.github.io/rdf-tests/"))
        .with_context(|| format!("URL does not match W3C pattern: {url}"))?;

    let path = rdf_tests_dir().join(relative);
    Ok(path)
}

/// Read a test resource file to a string, resolving from a W3C URL.
pub fn read_file_to_string(url: &str) -> Result<String> {
    let path = url_to_path(url)?;
    fs::read_to_string(&path).with_context(|| format!("Failed to read {}", path.display()))
}

/// Read a local file to a string.
pub fn read_local_file(path: &Path) -> Result<String> {
    fs::read_to_string(path).with_context(|| format!("Failed to read {}", path.display()))
}

/// Convert a relative IRI from a manifest to an absolute URL.
///
/// Manifest files use relative IRIs for test resources. For example, in
/// `sparql/sparql11/syntax-query/manifest.ttl`, the action `<syntax-select-expr-01.rq>`
/// resolves against the manifest's base IRI.
pub fn resolve_relative_iri(base: &str, relative: &str) -> String {
    if relative.starts_with("http://") || relative.starts_with("https://") {
        return relative.to_string();
    }

    // Strip fragment and query from base
    let base_no_fragment = base.split('#').next().unwrap_or(base);

    // Find the last '/' to get the base directory
    if let Some(pos) = base_no_fragment.rfind('/') {
        format!("{}/{}", &base_no_fragment[..pos], relative)
    } else {
        relative.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_to_path() {
        let url =
            "https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/syntax-select-expr-01.rq";
        let path = url_to_path(url).unwrap();
        assert!(path.ends_with("sparql/sparql11/syntax-query/syntax-select-expr-01.rq"));
    }

    #[test]
    fn test_resolve_relative_iri() {
        let base = "https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/manifest.ttl";
        let resolved = resolve_relative_iri(base, "syntax-select-expr-01.rq");
        assert_eq!(
            resolved,
            "https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/syntax-select-expr-01.rq"
        );
    }

    #[test]
    fn test_resolve_absolute_iri() {
        let base = "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest.ttl";
        let absolute = "https://example.org/test.rq";
        assert_eq!(resolve_relative_iri(base, absolute), absolute);
    }

    #[test]
    fn test_resolve_with_subdirectory() {
        let base = "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl";
        let resolved = resolve_relative_iri(base, "syntax-query/manifest.ttl");
        assert_eq!(
            resolved,
            "https://w3c.github.io/rdf-tests/sparql/sparql11/syntax-query/manifest.ttl"
        );
    }
}
