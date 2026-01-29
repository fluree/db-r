/// Parse a compact IRI like "schema:name" into (prefix, suffix).
/// Returns None if not a valid compact IRI.
///
/// A compact IRI has the form prefix:suffix where:
/// - prefix does not contain : or /
/// - suffix does not start with //
///
/// Special case: ":suffix" is valid with prefix = ":"
pub fn parse_prefix(s: &str) -> Option<(String, String)> {
    // Special case: prefix is ":"
    if s.starts_with(':') && s.len() > 1 {
        let suffix = &s[1..];
        return Some((":".to_string(), suffix.to_string()));
    }

    // Find the first colon
    if let Some(colon_pos) = s.find(':') {
        let prefix = &s[..colon_pos];
        let suffix = &s[colon_pos + 1..];

        // Prefix must not contain / (would indicate absolute IRI like http://)
        if prefix.contains('/') {
            return None;
        }

        // Suffix must not start with // (would indicate absolute IRI)
        if suffix.starts_with("//") {
            return None;
        }

        // Prefix must not be empty (except for special ":" case handled above)
        if prefix.is_empty() {
            return None;
        }

        return Some((prefix.to_string(), suffix.to_string()));
    }

    None
}

/// Returns true if string contains a colon (looks like an IRI or compact IRI)
pub fn any_iri(s: &str) -> bool {
    s.contains(':')
}

/// Returns true if the IRI is absolute (has an RFC 3986 scheme).
///
/// An absolute IRI starts with a scheme: `ALPHA *( ALPHA / DIGIT / "+" / "-" / "." ) ":"`.
/// This handles all schemes (http, https, urn, did, mailto, ftp, ipfs, etc.)
/// without maintaining a hardcoded list.
pub fn is_absolute(iri: &str) -> bool {
    if let Some(colon_pos) = iri.find(':') {
        let scheme = &iri[..colon_pos];
        !scheme.is_empty()
            && scheme.as_bytes()[0].is_ascii_alphabetic()
            && scheme
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'+' || b == b'-' || b == b'.')
    } else {
        false
    }
}

/// Ensure IRI ends with '/' or '#'
pub fn add_trailing_slash(iri: &str) -> String {
    if iri.ends_with('/') || iri.ends_with('#') {
        iri.to_string()
    } else {
        format!("{}/", iri)
    }
}

/// Join base IRI with relative IRI
pub fn join(base: &str, relative: &str) -> String {
    if relative.starts_with('#') {
        // Fragment: append to base
        format!("{}{}", base.trim_end_matches('/'), relative)
    } else if is_absolute(relative) {
        // Already absolute
        relative.to_string()
    } else {
        // Relative: ensure base ends with / and append
        let base = if base.ends_with('/') || base.ends_with('#') {
            base.to_string()
        } else {
            format!("{}/", base)
        };
        format!("{}{}", base, relative)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_prefix() {
        assert_eq!(
            parse_prefix("schema:name"),
            Some(("schema".to_string(), "name".to_string()))
        );
        assert_eq!(
            parse_prefix("ex:Person"),
            Some(("ex".to_string(), "Person".to_string()))
        );
        assert_eq!(
            parse_prefix(":localName"),
            Some((":".to_string(), "localName".to_string()))
        );

        // Not compact IRIs
        assert_eq!(parse_prefix("http://example.org"), None);
        assert_eq!(parse_prefix("https://schema.org/"), None);
        assert_eq!(parse_prefix("noColon"), None);
    }

    #[test]
    fn test_any_iri() {
        assert!(any_iri("schema:name"));
        assert!(any_iri("http://example.org"));
        assert!(!any_iri("localName"));
    }

    #[test]
    fn test_is_absolute() {
        assert!(is_absolute("http://example.org"));
        assert!(is_absolute("https://schema.org/"));
        assert!(is_absolute("urn:isbn:0451450523"));
        assert!(is_absolute("did:example:123"));
        assert!(is_absolute("file:///path/to/file"));
        assert!(is_absolute("mailto:user@example.com"));
        assert!(is_absolute("ftp://ftp.example.org"));
        assert!(is_absolute("ipfs:QmHash123"));
        // Compact IRIs look like schemes but are valid — is_absolute returns true
        // for anything with a valid scheme-like prefix. The distinction between
        // compact IRIs and absolute IRIs is handled by parse_prefix() which
        // rejects patterns like "http://..." (suffix starts with "//").
        assert!(is_absolute("schema:name"));
        assert!(!is_absolute("localName"));
        assert!(!is_absolute(""));
    }

    #[test]
    fn test_add_trailing_slash() {
        assert_eq!(add_trailing_slash("http://example.org"), "http://example.org/");
        assert_eq!(add_trailing_slash("http://example.org/"), "http://example.org/");
        assert_eq!(add_trailing_slash("http://example.org#"), "http://example.org#");
    }

    #[test]
    fn test_join() {
        assert_eq!(
            join("http://example.org/", "name"),
            "http://example.org/name"
        );
        assert_eq!(
            join("http://example.org", "name"),
            "http://example.org/name"
        );
        assert_eq!(
            join("http://example.org/", "#fragment"),
            "http://example.org#fragment"
        );
    }
}
