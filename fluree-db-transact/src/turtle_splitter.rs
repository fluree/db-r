//! Turtle file splitter for large-file import.
//!
//! Splits a single large `.ttl` file into chunk byte ranges, each representing
//! a set of complete Turtle statements. Every chunk is independently parseable
//! because the prefix block from the file header is prepended on read.
//!
//! ## Design
//!
//! 1. **Prefix extraction** — tokenize the first 1 MB with `fluree_graph_turtle::tokenize()`
//!    to locate all `@prefix` / `@base` / `PREFIX` / `BASE` directives. The raw source text
//!    of these directives (including interleaved comments) is captured verbatim.
//!
//! 2. **Pre-scan** — a single sequential pass through the file with a lightweight state
//!    machine that tracks string literal, IRI, and comment context. Statement boundaries
//!    (`.` followed by whitespace, `#`, or EOF in `Normal` state) are recorded at byte
//!    offsets. The first boundary after each `chunk_size` multiple becomes a chunk split.
//!
//! 3. **Chunk reading** — each chunk opens its own file handle, seeks to the byte range,
//!    reads the raw bytes, and prepends the prefix block. The result is a valid Turtle
//!    document.

use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use fluree_graph_turtle::{tokenize, TokenKind};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for splitting a large Turtle file.
#[derive(Debug, Clone)]
pub struct TurtleSplitConfig {
    /// Target chunk size in bytes. Actual chunks may be slightly larger because
    /// splits only occur at statement boundaries.
    pub chunk_size_bytes: u64,
}

// ============================================================================
// Errors
// ============================================================================

/// Errors specific to Turtle file splitting.
#[derive(Debug, thiserror::Error)]
pub enum SplitError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Turtle tokenization error: {0}")]
    Tokenize(String),

    #[error("prefix/base directive found after data at byte offset {offset}; all directives must appear in the file header")]
    PrefixAfterData { offset: u64 },

    #[error("no statement boundary found within 64 MB of target at byte offset {offset}; possible unterminated statement or corrupt Turtle")]
    NoBoundary { offset: u64 },

    #[error("file is empty or contains only prefix directives")]
    EmptyData,
}

// ============================================================================
// ScanState — lightweight state machine for boundary detection
// ============================================================================

/// Parser context for the byte-level pre-scan.
///
/// Tracks whether we are inside a string literal, IRI, or comment so that
/// `.` characters in those contexts are correctly ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanState {
    /// Outside any literal, IRI, or comment.
    Normal,
    /// Inside `"..."` (short double-quoted string).
    InShortDoubleString,
    /// Inside `"""..."""` (long double-quoted string).
    InLongDoubleString,
    /// Inside `'...'` (short single-quoted string).
    InShortSingleString,
    /// Inside `'''...'''` (long single-quoted string).
    InLongSingleString,
    /// After `\` inside a string. `return_to` encoded as discriminant.
    InStringEscape { return_to: EscapeReturn },
    /// Inside `<...>` (IRI reference).
    InIri,
    /// After `#` until end of line.
    InComment,
}

/// Which string state to return to after processing an escape character.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EscapeReturn {
    ShortDouble,
    LongDouble,
    ShortSingle,
    LongSingle,
}

impl EscapeReturn {
    fn to_state(self) -> ScanState {
        match self {
            EscapeReturn::ShortDouble => ScanState::InShortDoubleString,
            EscapeReturn::LongDouble => ScanState::InLongDoubleString,
            EscapeReturn::ShortSingle => ScanState::InShortSingleString,
            EscapeReturn::LongSingle => ScanState::InLongSingleString,
        }
    }
}

// ============================================================================
// Prefix block extraction
// ============================================================================

/// Size of the header region to tokenize for prefix extraction.
const PREFIX_SCAN_SIZE: usize = 1024 * 1024; // 1 MB

/// Extract the prefix/base directive block from the beginning of a Turtle file.
///
/// Returns `(prefix_text, data_start_byte)` where `prefix_text` is the verbatim
/// source text of all leading directives (including interleaved comments and
/// whitespace) and `data_start_byte` is the byte offset where actual triple
/// data begins.
pub fn extract_prefix_block(path: &Path) -> Result<(String, u64), SplitError> {
    let file = File::open(path)?;
    let file_size = file.metadata()?.len();
    let scan_size = std::cmp::min(PREFIX_SCAN_SIZE as u64, file_size) as usize;

    let mut buf = vec![0u8; scan_size];
    let mut reader = BufReader::new(file);
    reader.read_exact(&mut buf)?;

    // Tokenize the header region. If it ends mid-token, tokenize() may error;
    // we try to find a safe truncation point by scanning backwards for a newline.
    let header_str = find_safe_header(&buf);

    let tokens = tokenize(header_str).map_err(|e| SplitError::Tokenize(e.to_string()))?;

    // Walk tokens: collect prefix/base directives. Stop at the first token that
    // is not a directive, whitespace filler, or the terminating dot of a directive.
    //
    // Turtle-style directives (`@prefix`, `@base`) require a trailing `.`.
    // SPARQL-style directives (`PREFIX`, `BASE`) do NOT require a dot — the
    // directive ends after the IRI token.
    let mut last_directive_end: u32 = 0;
    let mut in_directive = false;
    let mut sparql_directive = false;
    let mut saw_any_directive = false;

    for tok in &tokens {
        match tok.kind {
            TokenKind::KwPrefix | TokenKind::KwBase => {
                in_directive = true;
                sparql_directive = false;
                saw_any_directive = true;
            }
            TokenKind::KwSparqlPrefix | TokenKind::KwSparqlBase => {
                in_directive = true;
                sparql_directive = true;
                saw_any_directive = true;
            }
            TokenKind::Dot if in_directive => {
                // Turtle-style directive terminator (also valid after SPARQL-style).
                in_directive = false;
                last_directive_end = tok.end;
            }
            TokenKind::Iri if in_directive && sparql_directive => {
                // SPARQL-style directives end after the IRI (no dot required).
                // `PREFIX ns: <iri>` and `BASE <iri>` both terminate here.
                in_directive = false;
                last_directive_end = tok.end;
            }
            TokenKind::Eof => break,
            _ if in_directive => {
                // Part of the directive (PrefixedNameNs, Iri for Turtle-style, etc.)
            }
            _ => {
                // First non-directive token — this is where data starts.
                break;
            }
        }
    }

    if !saw_any_directive {
        // No prefix block — data starts at byte 0.
        return Ok((String::new(), 0));
    }

    let prefix_end = last_directive_end as usize;

    // Include any trailing whitespace/newlines after the last directive dot
    // so that the prefix block ends cleanly.
    let mut data_start = prefix_end;
    let header_bytes = header_str.as_bytes();
    while data_start < header_bytes.len() && header_bytes[data_start].is_ascii_whitespace() {
        data_start += 1;
    }

    let prefix_text = header_str[..prefix_end].to_string();
    // Ensure prefix block ends with a newline for clean concatenation.
    let prefix_text = if prefix_text.ends_with('\n') {
        prefix_text
    } else {
        format!("{prefix_text}\n")
    };

    Ok((prefix_text, data_start as u64))
}

/// Find a safe truncation point in the buffer for tokenization.
///
/// If the buffer might end mid-token (e.g. a long IRI or string that crosses
/// the 1 MB read boundary), tokenization will fail. We handle this by:
/// 1. Truncating to valid UTF-8
/// 2. Scanning backwards for a newline to get a clean line boundary
///
/// Since Turtle directives are line-oriented (`@prefix ... .\n`), truncating
/// at a newline guarantees we don't split mid-directive or mid-IRI.
fn find_safe_header(buf: &[u8]) -> &str {
    // Get valid UTF-8 range.
    let valid = match std::str::from_utf8(buf) {
        Ok(s) => return s, // whole buffer is valid — no truncation needed
        Err(e) => &buf[..e.valid_up_to()],
    };

    // Scan backwards for a newline to avoid cutting mid-token.
    if let Some(nl_pos) = valid.iter().rposition(|&b| b == b'\n') {
        // Safety: we already know buf[..valid_end] is valid UTF-8, and
        // nl_pos+1 is at a char boundary (byte after \n).
        std::str::from_utf8(&valid[..nl_pos + 1]).unwrap_or("")
    } else {
        // No newline at all — use whatever we have.
        std::str::from_utf8(valid).unwrap_or("")
    }
}

// ============================================================================
// Chunk boundary computation
// ============================================================================

/// Read buffer size for the pre-scan pass.
const SCAN_BUF_SIZE: usize = 64 * 1024; // 64 KB

/// Maximum distance (in bytes) past a chunk target before we error.
const MAX_BOUNDARY_SEARCH: u64 = 64 * 1024 * 1024; // 64 MB

/// Pending lookahead state carried across buffer boundaries.
#[derive(Debug, Default)]
struct Lookahead {
    /// A `.` was the last byte processed; waiting to see if next byte is
    /// whitespace/`#`/EOF to confirm a statement boundary.
    pending_dot: Option<u64>,
    /// Count of consecutive `"` or `'` seen at the end of a buffer (1 or 2),
    /// for detecting triple-quote openings.
    pending_quotes: u8,
    /// Which quote character is pending (`"` or `'`).
    pending_quote_char: u8,
    /// `\r` was the last byte — check for `\n` to form CRLF.
    pending_cr: bool,
}

/// Compute chunk byte ranges by scanning the file for statement boundaries.
///
/// Returns a `Vec<(start, end)>` of byte ranges. Each range represents a chunk
/// that contains one or more complete Turtle statements.
///
/// # Errors
///
/// - `PrefixAfterData` if a `@prefix`/`@base`/`PREFIX`/`BASE` directive is
///   detected after data has started.
/// - `NoBoundary` if no statement boundary is found within 64 MB of a target.
pub fn compute_chunk_boundaries(
    path: &Path,
    data_start: u64,
    chunk_size: u64,
) -> Result<Vec<(u64, u64)>, SplitError> {
    let file = File::open(path)?;
    let file_size = file.metadata()?.len();

    if data_start >= file_size {
        return Err(SplitError::EmptyData);
    }

    let mut reader = BufReader::with_capacity(SCAN_BUF_SIZE, file);
    reader.seek(SeekFrom::Start(data_start))?;

    let mut state = ScanState::Normal;
    let mut lookahead = Lookahead::default();
    let mut boundaries: Vec<u64> = Vec::new();
    let mut next_target = data_start + chunk_size;
    let mut byte_pos = data_start;
    let mut buf = vec![0u8; SCAN_BUF_SIZE];
    let mut prefix_check = PrefixCheck::new();

    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            // EOF — if we have a pending dot, it's a boundary (dot at EOF).
            if let Some(dot_pos) = lookahead.pending_dot.take() {
                let boundary_pos = dot_pos + 1;
                if boundary_pos >= next_target {
                    boundaries.push(boundary_pos);
                }
            }
            break;
        }

        let chunk = &buf[..n];

        for (i, &b) in chunk.iter().enumerate() {
            let abs_pos = byte_pos + i as u64;

            // Handle pending dot: check if this byte confirms a boundary.
            if let Some(dot_pos) = lookahead.pending_dot.take() {
                if is_boundary_follower(b) {
                    let boundary_pos = dot_pos + 1;
                    if boundary_pos >= next_target {
                        boundaries.push(boundary_pos);
                        next_target = boundary_pos + chunk_size;
                    }
                }
                // The dot was not a boundary — fall through to process `b` normally.
            }

            // Handle pending CR for CRLF detection.
            if lookahead.pending_cr {
                lookahead.pending_cr = false;
                // \r\n is just one newline — no special action needed beyond
                // clearing the flag; the \r already triggered comment exit.
            }

            // Handle pending quotes for triple-quote detection.
            // Context matters: in Normal state, """ opens a long string;
            // in a long string state, """ closes it.
            if lookahead.pending_quotes > 0 {
                let pq_char = lookahead.pending_quote_char;
                let pq_count = lookahead.pending_quotes;

                if b == pq_char {
                    let total = pq_count + 1;
                    if total >= 3 {
                        // Triple quote confirmed.
                        lookahead.pending_quotes = 0;
                        state = match state {
                            ScanState::InLongDoubleString | ScanState::InLongSingleString => {
                                // Closing triple quote — exit to Normal.
                                ScanState::Normal
                            }
                            _ => {
                                // Opening triple quote — enter long string.
                                if pq_char == b'"' {
                                    ScanState::InLongDoubleString
                                } else {
                                    ScanState::InLongSingleString
                                }
                            }
                        };
                        continue;
                    } else {
                        lookahead.pending_quotes = total;
                        continue;
                    }
                } else {
                    // Not a triple quote.
                    lookahead.pending_quotes = 0;
                    match state {
                        ScanState::InLongDoubleString | ScanState::InLongSingleString => {
                            // Inside long string, saw 1-2 quotes but not 3.
                            // Stay in long string; fall through to process `b`.
                        }
                        _ => {
                            // Opening short string.
                            state = if pq_char == b'"' {
                                ScanState::InShortDoubleString
                            } else {
                                ScanState::InShortSingleString
                            };
                            // Fall through to process `b` in new state.
                        }
                    }
                }
            }

            // Main state machine.
            state = advance_state(state, b, abs_pos, &mut lookahead, &mut prefix_check)?;
        }

        byte_pos += n as u64;

        // Check for overshoot: no boundary found within 64 MB past target.
        if byte_pos > next_target + MAX_BOUNDARY_SEARCH {
            return Err(SplitError::NoBoundary {
                offset: next_target,
            });
        }
    }

    // If no boundaries were recorded (file smaller than chunk_size), use the
    // whole data region as a single chunk.
    if boundaries.is_empty() {
        return Ok(vec![(data_start, file_size)]);
    }

    // Convert boundary positions to (start, end) ranges.
    // The last chunk always extends to EOF (absorbing any trailing whitespace
    // after the final statement boundary).
    let mut ranges = Vec::with_capacity(boundaries.len());
    let mut start = data_start;
    for &end in &boundaries {
        if end > start {
            ranges.push((start, end));
        }
        start = end;
    }
    // Extend the last chunk to EOF instead of creating a tiny trailing range.
    if start < file_size {
        if let Some(last) = ranges.last_mut() {
            last.1 = file_size;
        } else {
            ranges.push((start, file_size));
        }
    }

    Ok(ranges)
}

/// Returns true if `b` can follow a `.` to confirm a statement boundary.
fn is_boundary_follower(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\r' | b'\n' | b'#')
}

/// Advance the scan state machine by one byte.
fn advance_state(
    state: ScanState,
    b: u8,
    abs_pos: u64,
    lookahead: &mut Lookahead,
    prefix_check: &mut PrefixCheck,
) -> Result<ScanState, SplitError> {
    match state {
        ScanState::Normal => {
            // Check for prefix/base directives appearing after data.
            prefix_check.feed(b, abs_pos)?;

            match b {
                b'"' => {
                    // Could be start of """ or just "
                    lookahead.pending_quotes = 1;
                    lookahead.pending_quote_char = b'"';
                    Ok(ScanState::Normal) // resolved on next byte
                }
                b'\'' => {
                    lookahead.pending_quotes = 1;
                    lookahead.pending_quote_char = b'\'';
                    Ok(ScanState::Normal)
                }
                b'<' => Ok(ScanState::InIri),
                b'#' => Ok(ScanState::InComment),
                b'.' => {
                    lookahead.pending_dot = Some(abs_pos);
                    Ok(ScanState::Normal)
                }
                _ => Ok(ScanState::Normal),
            }
        }

        ScanState::InShortDoubleString => match b {
            b'\\' => Ok(ScanState::InStringEscape {
                return_to: EscapeReturn::ShortDouble,
            }),
            b'"' => Ok(ScanState::Normal),
            b'\n' | b'\r' => {
                // Short strings can't span lines in valid Turtle, but we
                // just return to Normal to be resilient.
                Ok(ScanState::Normal)
            }
            _ => Ok(ScanState::InShortDoubleString),
        },

        ScanState::InLongDoubleString => match b {
            b'\\' => Ok(ScanState::InStringEscape {
                return_to: EscapeReturn::LongDouble,
            }),
            b'"' => {
                // Could be end of long string (""") — we need lookahead.
                // For simplicity, we track consecutive quotes.
                lookahead.pending_quotes = 1;
                lookahead.pending_quote_char = b'"';
                // Stay in long string state; the lookahead handler will
                // transition back to Normal when it sees 3 quotes.
                // Actually, we need special handling here: if we see """,
                // we exit. Let's use a dedicated approach.
                Ok(ScanState::InLongDoubleString)
            }
            _ => Ok(ScanState::InLongDoubleString),
        },

        ScanState::InShortSingleString => match b {
            b'\\' => Ok(ScanState::InStringEscape {
                return_to: EscapeReturn::ShortSingle,
            }),
            b'\'' => Ok(ScanState::Normal),
            b'\n' | b'\r' => Ok(ScanState::Normal),
            _ => Ok(ScanState::InShortSingleString),
        },

        ScanState::InLongSingleString => match b {
            b'\\' => Ok(ScanState::InStringEscape {
                return_to: EscapeReturn::LongSingle,
            }),
            b'\'' => {
                lookahead.pending_quotes = 1;
                lookahead.pending_quote_char = b'\'';
                Ok(ScanState::InLongSingleString)
            }
            _ => Ok(ScanState::InLongSingleString),
        },

        ScanState::InStringEscape { return_to } => {
            // Consume one escaped character and return.
            Ok(return_to.to_state())
        }

        ScanState::InIri => match b {
            b'>' => Ok(ScanState::Normal),
            _ => Ok(ScanState::InIri),
        },

        ScanState::InComment => match b {
            b'\n' => Ok(ScanState::Normal),
            b'\r' => {
                lookahead.pending_cr = true;
                Ok(ScanState::Normal)
            }
            _ => Ok(ScanState::InComment),
        },
    }
}

// ============================================================================
// Prefix-after-data check
// ============================================================================

/// Lightweight detector for `@prefix`, `@base`, `PREFIX`, `BASE` appearing
/// at line start after data has begun.
///
/// We track whether we're at column 0 (or leading whitespace) and match
/// against the keyword prefixes byte-by-byte.
struct PrefixCheck {
    /// Whether any non-whitespace data byte has been seen.
    data_started: bool,
    /// Whether we're at the start of a line (or only whitespace so far on this line).
    at_line_start: bool,
    /// Current match position into one of the keywords.
    match_pos: usize,
    /// Which keyword we're trying to match (index into KEYWORDS).
    match_keyword: usize,
    /// All keyword bytes matched; waiting for a delimiter to confirm.
    awaiting_delimiter: bool,
}

/// Keywords to detect (case-sensitive).
const KEYWORDS: &[&[u8]] = &[b"@prefix", b"@base", b"PREFIX", b"BASE"];

impl PrefixCheck {
    fn new() -> Self {
        Self {
            data_started: false,
            at_line_start: true,
            match_pos: 0,
            match_keyword: usize::MAX,
            awaiting_delimiter: false,
        }
    }

    /// Returns true if `b` is a valid delimiter after a directive keyword.
    /// Directive keywords must be followed by whitespace or `:` (@prefix) or `<` (BASE).
    fn is_keyword_delimiter(b: u8) -> bool {
        matches!(b, b' ' | b'\t' | b':' | b'<')
    }

    fn feed(&mut self, b: u8, abs_pos: u64) -> Result<(), SplitError> {
        // If we matched all keyword bytes, check if the next byte is a delimiter.
        if self.awaiting_delimiter {
            self.awaiting_delimiter = false;
            if Self::is_keyword_delimiter(b) && self.data_started {
                let kw = KEYWORDS[self.match_keyword];
                let kw_start = abs_pos - kw.len() as u64;
                return Err(SplitError::PrefixAfterData { offset: kw_start });
            }
            // Not a delimiter — false alarm (e.g. "BASELINE"), reset.
            self.match_keyword = usize::MAX;
            self.match_pos = 0;
            self.at_line_start = false;
            self.data_started = true;
            return Ok(());
        }

        match b {
            b'\n' | b'\r' => {
                self.at_line_start = true;
                self.match_pos = 0;
                self.match_keyword = usize::MAX;
            }
            b' ' | b'\t' if self.at_line_start => {
                // Still at line start, leading whitespace.
            }
            _ => {
                if self.at_line_start && self.match_pos == 0 {
                    // Try to start matching a keyword.
                    for (i, kw) in KEYWORDS.iter().enumerate() {
                        if b == kw[0] {
                            self.match_keyword = i;
                            self.match_pos = 1;
                            if self.match_pos >= kw.len() {
                                // Single-byte keyword (shouldn't happen with current set,
                                // but handle for correctness).
                                self.awaiting_delimiter = true;
                            }
                            self.at_line_start = false;
                            self.data_started = true;
                            return Ok(());
                        }
                    }
                    // No keyword match — just data.
                    self.at_line_start = false;
                    self.data_started = true;
                } else if self.match_keyword < KEYWORDS.len() {
                    let kw = KEYWORDS[self.match_keyword];
                    if self.match_pos < kw.len() && b == kw[self.match_pos] {
                        self.match_pos += 1;
                        if self.match_pos >= kw.len() {
                            // All keyword bytes matched — need delimiter confirmation.
                            self.awaiting_delimiter = true;
                            return Ok(());
                        }
                    } else {
                        // Mismatch — reset.
                        self.match_keyword = usize::MAX;
                        self.match_pos = 0;
                    }
                    self.at_line_start = false;
                    self.data_started = true;
                } else {
                    self.at_line_start = false;
                    self.data_started = true;
                }
            }
        }
        Ok(())
    }
}

// ============================================================================
// TurtleChunkReader
// ============================================================================

/// Reader for chunks of a large Turtle file.
///
/// Created by [`TurtleChunkReader::new`], which performs the prefix extraction
/// and pre-scan in the constructor. Chunk reads are thread-safe (each opens
/// its own file handle).
pub struct TurtleChunkReader {
    path: PathBuf,
    prefix_block: String,
    /// Byte ranges: `(start, end)` for each chunk.
    ranges: Vec<(u64, u64)>,
}

impl TurtleChunkReader {
    /// Create a new reader by scanning the file for chunk boundaries.
    ///
    /// This performs:
    /// 1. Prefix block extraction (tokenize first 1 MB)
    /// 2. Pre-scan for statement boundaries
    ///
    /// Logs progress for large files.
    pub fn new(path: &Path, config: &TurtleSplitConfig) -> Result<Self, SplitError> {
        let file_size = std::fs::metadata(path)?.len();
        tracing::info!(
            path = %path.display(),
            file_size_mb = file_size / (1024 * 1024),
            chunk_size_mb = config.chunk_size_bytes / (1024 * 1024),
            "scanning large file for chunk boundaries..."
        );

        let (prefix_block, data_start) = extract_prefix_block(path)?;
        tracing::debug!(
            prefix_bytes = prefix_block.len(),
            data_start,
            "prefix block extracted"
        );

        let ranges = compute_chunk_boundaries(path, data_start, config.chunk_size_bytes)?;
        tracing::info!(chunk_count = ranges.len(), "chunk boundaries computed");

        Ok(Self {
            path: path.to_path_buf(),
            prefix_block,
            ranges,
        })
    }

    /// Number of chunks.
    pub fn chunk_count(&self) -> usize {
        self.ranges.len()
    }

    /// Read chunk `index`, prepending the prefix block.
    ///
    /// Returns `None` if `index` is out of range.
    ///
    /// Each call opens its own file handle and seeks to the byte range,
    /// making this safe to call from multiple threads.
    pub fn read_chunk(&self, index: usize) -> io::Result<Option<String>> {
        let Some(&(start, end)) = self.ranges.get(index) else {
            return Ok(None);
        };

        let len = (end - start) as usize;
        let mut buf = vec![0u8; len];

        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut buf)?;

        let data = String::from_utf8(buf).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "chunk {} (bytes {}..{}) contains invalid UTF-8: {}",
                    index, start, end, e
                ),
            )
        })?;

        let mut result = String::with_capacity(self.prefix_block.len() + data.len());
        result.push_str(&self.prefix_block);
        result.push_str(&data);

        Ok(Some(result))
    }

    /// The prefix block text (all directives from the file header).
    pub fn prefix_block(&self) -> &str {
        &self.prefix_block
    }

    /// The byte ranges for each chunk (for diagnostics).
    pub fn ranges(&self) -> &[(u64, u64)] {
        &self.ranges
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Write content to a temp file and return the path.
    fn write_temp(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    // ---- extract_prefix_block tests ----

    #[test]
    fn test_prefix_extraction_basic() {
        let ttl = "\
@prefix ex: <http://example.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:alice foaf:name \"Alice\" .
";
        let f = write_temp(ttl);
        let (prefix, data_start) = extract_prefix_block(f.path()).unwrap();
        assert!(prefix.contains("@prefix ex:"));
        assert!(prefix.contains("@prefix foaf:"));
        // data_start should be at or after the second directive's dot
        let data_region = &ttl[data_start as usize..];
        assert!(
            data_region.starts_with("ex:alice") || data_region.trim_start().starts_with("ex:alice"),
            "data_region starts with: {:?}",
            &data_region[..40.min(data_region.len())]
        );
    }

    #[test]
    fn test_prefix_extraction_sparql_style() {
        let ttl = "\
PREFIX ex: <http://example.org/>
BASE <http://example.org/>

ex:alice ex:name \"Alice\" .
";
        let f = write_temp(ttl);
        let (prefix, data_start) = extract_prefix_block(f.path()).unwrap();
        assert!(prefix.contains("PREFIX ex:"));
        assert!(prefix.contains("BASE"));
        // data_start must point at or before "ex:alice", not at byte 0
        // and not past the data.
        let data_region = &ttl[data_start as usize..];
        assert!(
            data_region.starts_with("ex:alice") || data_region.trim_start().starts_with("ex:alice"),
            "expected data to start at ex:alice, got: {:?}",
            &data_region[..40.min(data_region.len())]
        );
        // Prefix block must NOT contain data triples.
        assert!(
            !prefix.contains("ex:alice"),
            "prefix block should not contain data triples"
        );
    }

    #[test]
    fn test_prefix_extraction_sparql_no_dot_then_turtle() {
        // Mix: SPARQL-style (no dot) followed by Turtle-style (with dot).
        let ttl = "\
PREFIX ex: <http://example.org/>
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:alice foaf:name \"Alice\" .
";
        let f = write_temp(ttl);
        let (prefix, data_start) = extract_prefix_block(f.path()).unwrap();
        assert!(prefix.contains("PREFIX ex:"));
        assert!(prefix.contains("@prefix foaf:"));
        let data_region = &ttl[data_start as usize..];
        assert!(
            data_region.trim_start().starts_with("ex:alice"),
            "expected data to start at ex:alice, got: {:?}",
            &data_region[..40.min(data_region.len())]
        );
    }

    #[test]
    fn test_prefix_extraction_no_prefixes() {
        let ttl = "<http://example.org/alice> <http://example.org/name> \"Alice\" .\n";
        let f = write_temp(ttl);
        let (prefix, data_start) = extract_prefix_block(f.path()).unwrap();
        assert!(prefix.is_empty());
        assert_eq!(data_start, 0);
    }

    #[test]
    fn test_prefix_extraction_with_comments() {
        let ttl = "\
# This is a Turtle file
@prefix ex: <http://example.org/> .
# Another comment
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:alice foaf:name \"Alice\" .
";
        let f = write_temp(ttl);
        let (prefix, _) = extract_prefix_block(f.path()).unwrap();
        // The prefix block is the raw source up to the end of the last
        // directive's `.` — interleaved comments ARE included because we
        // capture the verbatim text range, not individual tokens.
        assert!(prefix.contains("@prefix ex:"));
        assert!(prefix.contains("@prefix foaf:"));
        assert!(
            prefix.contains("# Another comment"),
            "interleaved comments should be preserved in prefix block"
        );
    }

    // ---- ScanState boundary detection tests ----

    #[test]
    fn test_dot_in_short_string_not_boundary() {
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:name \"Alice. B.\" .
ex:bob ex:name \"Bob\" .
";
        let f = write_temp(ttl);
        let (_, data_start) = extract_prefix_block(f.path()).unwrap();
        // With a small chunk size, boundaries should only be at real statement dots.
        let ranges = compute_chunk_boundaries(f.path(), data_start, 1).unwrap();
        // Each statement should be its own chunk (chunk_size=1 forces split at every boundary).
        assert_eq!(ranges.len(), 2, "expected 2 chunks for 2 statements");
    }

    #[test]
    fn test_dot_in_long_string_not_boundary() {
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:desc \"\"\"This is a long.
Multi-line. Description.\"\"\" .
ex:bob ex:name \"Bob\" .
";
        let f = write_temp(ttl);
        let (_, data_start) = extract_prefix_block(f.path()).unwrap();
        let ranges = compute_chunk_boundaries(f.path(), data_start, 1).unwrap();
        assert_eq!(ranges.len(), 2, "expected 2 chunks for 2 statements");
    }

    #[test]
    fn test_dot_in_iri_not_boundary() {
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:homepage <http://alice.example.org/home.html> .
ex:bob ex:name \"Bob\" .
";
        let f = write_temp(ttl);
        let (_, data_start) = extract_prefix_block(f.path()).unwrap();
        let ranges = compute_chunk_boundaries(f.path(), data_start, 1).unwrap();
        assert_eq!(ranges.len(), 2, "expected 2 chunks for 2 statements");
    }

    #[test]
    fn test_dot_in_comment_not_boundary() {
        let ttl = "\
@prefix ex: <http://example.org/> .

# This is a comment. With dots.
ex:alice ex:name \"Alice\" .
ex:bob ex:name \"Bob\" .
";
        let f = write_temp(ttl);
        let (_, data_start) = extract_prefix_block(f.path()).unwrap();
        let ranges = compute_chunk_boundaries(f.path(), data_start, 1).unwrap();
        assert_eq!(ranges.len(), 2);
    }

    #[test]
    fn test_escape_in_string() {
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:name \"Ali\\\"ce.\" .
ex:bob ex:name \"Bob\" .
";
        let f = write_temp(ttl);
        let (_, data_start) = extract_prefix_block(f.path()).unwrap();
        let ranges = compute_chunk_boundaries(f.path(), data_start, 1).unwrap();
        assert_eq!(ranges.len(), 2);
    }

    #[test]
    fn test_single_chunk_small_file() {
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:name \"Alice\" .
ex:bob ex:name \"Bob\" .
";
        let f = write_temp(ttl);
        let (_, data_start) = extract_prefix_block(f.path()).unwrap();
        // chunk_size larger than file → single chunk.
        let ranges = compute_chunk_boundaries(f.path(), data_start, 1024 * 1024).unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].0, data_start);
    }

    #[test]
    fn test_roundtrip_chunks_preserve_all_triples() {
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:name \"Alice\" ; ex:age 30 .
ex:bob ex:name \"Bob\" ; ex:age 25 .
ex:carol ex:name \"Carol\" ; ex:age 28 .
ex:dave ex:name \"Dave\" ; ex:age 35 .
";
        let f = write_temp(ttl);
        let config = TurtleSplitConfig {
            chunk_size_bytes: 40, // force multiple chunks
        };
        let reader = TurtleChunkReader::new(f.path(), &config).unwrap();

        // Parse each chunk individually and collect all subjects.
        let mut all_subjects = Vec::new();
        for i in 0..reader.chunk_count() {
            let chunk_text = reader.read_chunk(i).unwrap().unwrap();
            // Parse with fluree_graph_turtle to verify it's valid Turtle.
            let json = fluree_graph_turtle::parse_to_json(&chunk_text).unwrap();
            let arr = json.as_array().unwrap();
            for node in arr {
                all_subjects.push(node["@id"].as_str().unwrap().to_string());
            }
        }

        all_subjects.sort();
        assert_eq!(
            all_subjects,
            vec![
                "http://example.org/alice",
                "http://example.org/bob",
                "http://example.org/carol",
                "http://example.org/dave",
            ]
        );
    }

    #[test]
    fn test_crlf_line_endings() {
        let ttl = "@prefix ex: <http://example.org/> .\r\n\r\nex:alice ex:name \"Alice\" .\r\nex:bob ex:name \"Bob\" .\r\n";
        let f = write_temp(ttl);
        let (_, data_start) = extract_prefix_block(f.path()).unwrap();
        let ranges = compute_chunk_boundaries(f.path(), data_start, 1).unwrap();
        assert_eq!(ranges.len(), 2);
    }

    #[test]
    fn test_single_quote_strings() {
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:name 'Alice. B.' .
ex:bob ex:desc '''Long.
Multi. line.''' .
";
        let f = write_temp(ttl);
        let (_, data_start) = extract_prefix_block(f.path()).unwrap();
        let ranges = compute_chunk_boundaries(f.path(), data_start, 1).unwrap();
        assert_eq!(ranges.len(), 2);
    }

    #[test]
    fn test_dot_followed_by_comment() {
        // `. #comment` should be a boundary.
        let ttl = "\
@prefix ex: <http://example.org/> .

ex:alice ex:name \"Alice\" .#comment
ex:bob ex:name \"Bob\" .
";
        let f = write_temp(ttl);
        let (_, data_start) = extract_prefix_block(f.path()).unwrap();
        let ranges = compute_chunk_boundaries(f.path(), data_start, 1).unwrap();
        assert_eq!(ranges.len(), 2);
    }

    #[test]
    fn test_prefix_check_requires_delimiter() {
        // "BASELINE" starts with "BASE" but is not a directive keyword.
        // PrefixCheck must require a delimiter (space, tab, :, <) after the
        // keyword to confirm. This tests the PrefixCheck directly since
        // bare "BASELINE" isn't valid Turtle syntax.
        let mut check = PrefixCheck::new();
        // Feed data so data_started becomes true.
        for (i, &b) in b"ex:alice ex:name \"Alice\" .\n".iter().enumerate() {
            check.feed(b, i as u64).unwrap();
        }
        let offset = 27u64;
        // "BASELINE" at line start should NOT trigger PrefixAfterData.
        for (i, &b) in b"BASELINE stuff\n".iter().enumerate() {
            check
                .feed(b, offset + i as u64)
                .expect("BASELINE should not trigger PrefixAfterData");
        }
        // But "BASE <" (with delimiter) SHOULD trigger.
        let offset2 = offset + 15;
        let mut check2 = PrefixCheck::new();
        for (i, &b) in b"ex:alice ex:name \"Alice\" .\n".iter().enumerate() {
            check2.feed(b, i as u64).unwrap();
        }
        let result = (|| {
            for (i, &b) in b"BASE <http://example.org/>\n".iter().enumerate() {
                check2.feed(b, offset2 + i as u64)?;
            }
            Ok::<(), SplitError>(())
        })();
        assert!(
            matches!(result, Err(SplitError::PrefixAfterData { .. })),
            "BASE followed by delimiter should trigger PrefixAfterData"
        );
    }
}
