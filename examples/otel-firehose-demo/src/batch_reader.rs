//! Line-by-line Turtle batch reader.
//!
//! Reads a `.ttl` file and yields self-contained batches of statements, each
//! prefixed with the collected `@prefix`/`@base` header so it can be parsed
//! independently.
//!
//! Copied from `examples/bulk-turtle-import/src/main.rs` — the logic is
//! identical but lives here so this crate avoids depending on `fluree-db-api`.

use std::io::{self, BufRead, BufReader};

/// A single batch of Turtle statements ready for parsing.
pub struct Batch {
    /// Self-contained Turtle text (prefix header + statement body).
    pub turtle: String,
    /// Number of statements in this batch.
    pub statements: usize,
}

/// Line-by-line Turtle reader that yields self-contained batches.
///
/// - Collects `@prefix` / `@base` directives into a header that is prepended
///   to every batch, making each batch independently parseable.
/// - Counts statement boundaries (lines ending with ` .`) and yields a new
///   [`Batch`] each time `batch_size` statements have accumulated.
/// - Any remaining statements at EOF are yielded as a final (smaller) batch.
pub struct BatchReader<R> {
    inner: BufReader<R>,
    prefix_header: String,
    batch_size: usize,
    line_buf: String,
    done: bool,
}

impl<R: io::Read> BatchReader<R> {
    pub fn new(reader: R, batch_size: usize) -> Self {
        Self {
            inner: BufReader::with_capacity(256 * 1024, reader),
            prefix_header: String::new(),
            batch_size,
            line_buf: String::new(),
            done: false,
        }
    }
}

impl<R: io::Read> Iterator for BatchReader<R> {
    type Item = io::Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let mut body = String::new();
        let mut stmt_count: usize = 0;

        loop {
            self.line_buf.clear();
            match self.inner.read_line(&mut self.line_buf) {
                Ok(0) => {
                    // EOF
                    self.done = true;
                    break;
                }
                Ok(_) => {
                    let trimmed = self.line_buf.trim();

                    // Skip blank lines and comments.
                    if trimmed.is_empty() || trimmed.starts_with('#') {
                        continue;
                    }

                    // Collect prefix/base directives into the shared header.
                    if is_directive(trimmed) {
                        self.prefix_header.push_str(&self.line_buf);
                        continue;
                    }

                    // Accumulate statement body.
                    body.push_str(&self.line_buf);

                    if is_statement_end(trimmed) {
                        stmt_count += 1;
                        if stmt_count >= self.batch_size {
                            break;
                        }
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }

        if body.is_empty() {
            return None;
        }

        let mut turtle = String::with_capacity(self.prefix_header.len() + body.len() + 1);
        turtle.push_str(&self.prefix_header);
        turtle.push('\n');
        turtle.push_str(&body);

        Some(Ok(Batch {
            turtle,
            statements: stmt_count,
        }))
    }
}

/// Returns `true` for Turtle `@prefix`/`@base` and SPARQL-style `PREFIX`/`BASE`.
fn is_directive(trimmed: &str) -> bool {
    let lower = trimmed.to_ascii_lowercase();
    lower.starts_with("@prefix ")
        || lower.starts_with("@base ")
        || lower.starts_with("prefix ")
        || lower.starts_with("base ")
}

/// Heuristic: a Turtle statement terminates when the trimmed line ends with
/// ` .`, `\t.`, or is exactly `.`.
///
/// This works reliably for well-formatted Turtle output from standard tools
/// (rapper, riot, rdflib, etc.). It can misidentify boundaries inside
/// multi-line string literals, but those are rare in practice.
fn is_statement_end(trimmed: &str) -> bool {
    trimmed == "." || trimmed.ends_with(" .") || trimmed.ends_with("\t.")
}
