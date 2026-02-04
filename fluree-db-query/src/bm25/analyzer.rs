//! Text Analyzer with Clojure Parity
//!
//! Implements a text analysis pipeline matching the Clojure implementation:
//! 1. Lowercase (in tokenizer)
//! 2. Split on `[^\w]+` (regex word split)
//! 3. Stopword filtering
//! 4. Snowball stemming
//!
//! Note: Java/Clojure `\w` may differ slightly from Rust regex `\w`.
//! Both should include `[a-zA-Z0-9_]` but Unicode handling may vary.

use std::collections::HashSet;

use once_cell::sync::Lazy;
use regex::Regex;
use rust_stemmers::{Algorithm, Stemmer};

// ============================================================================
// Token
// ============================================================================

/// A token produced by the tokenizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub text: String,
}

impl Token {
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }
}

// ============================================================================
// Tokenizer
// ============================================================================

/// Trait for tokenizers that split text into tokens.
pub trait Tokenizer: Send + Sync {
    fn tokenize(&self, text: &str) -> Vec<Token>;
}

/// Regex for splitting on non-word characters.
/// Matches the Clojure `(str/split text #"[^\w]+")` pattern.
static WORD_SPLIT: Lazy<Regex> = Lazy::new(|| {
    // `[^\w]+` in regex means one or more non-word characters
    // In Rust, `\w` matches `[a-zA-Z0-9_]` (ASCII only by default)
    Regex::new(r"[^\w]+").expect("Invalid regex")
});

/// Clojure-parity tokenizer.
///
/// Performs:
/// 1. Lowercase the input text
/// 2. Split on `[^\w]+` (non-word characters)
///
/// Note: Lowercase is done in the tokenizer (not as a separate filter)
/// to match the Clojure flow: `(-> text str/lower-case (str/split #"[^\w]+"))`
#[derive(Debug, Default, Clone)]
pub struct ClojureParityTokenizer;

impl Tokenizer for ClojureParityTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        // Lowercase first (matches Clojure's str/lower-case before split)
        let lowercased = text.to_lowercase();

        WORD_SPLIT
            .split(&lowercased)
            .filter(|s| !s.is_empty())
            .map(|s| Token::new(s.to_string()))
            .collect()
    }
}

// ============================================================================
// Token Filters
// ============================================================================

/// Trait for filters that transform or remove tokens.
pub trait TokenFilter: Send + Sync {
    fn filter(&self, tokens: Vec<Token>) -> Vec<Token>;
}

/// Stopword filter that removes common words.
#[derive(Debug, Clone)]
pub struct StopwordFilter {
    stopwords: HashSet<String>,
}

impl StopwordFilter {
    /// Create a new stopword filter with the given stopwords.
    pub fn new(stopwords: HashSet<String>) -> Self {
        Self { stopwords }
    }

    /// Create an English stopword filter.
    pub fn english() -> Self {
        Self::new(ENGLISH_STOPWORDS.clone())
    }

    /// Check if a word is a stopword.
    pub fn is_stopword(&self, word: &str) -> bool {
        self.stopwords.contains(word)
    }
}

impl TokenFilter for StopwordFilter {
    fn filter(&self, tokens: Vec<Token>) -> Vec<Token> {
        tokens
            .into_iter()
            .filter(|t| !self.stopwords.contains(&t.text))
            .collect()
    }
}

/// Snowball stemmer filter.
pub struct SnowballStemmerFilter {
    stemmer: Stemmer,
    algorithm: Algorithm,
}

impl Clone for SnowballStemmerFilter {
    fn clone(&self) -> Self {
        // Recreate the stemmer since Stemmer doesn't implement Clone
        Self {
            stemmer: Stemmer::create(self.algorithm),
            algorithm: self.algorithm,
        }
    }
}

impl std::fmt::Debug for SnowballStemmerFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnowballStemmerFilter")
            .field("algorithm", &self.algorithm)
            .finish()
    }
}

impl SnowballStemmerFilter {
    /// Create a new Snowball stemmer filter for the given language.
    pub fn new(algorithm: Algorithm) -> Self {
        Self {
            stemmer: Stemmer::create(algorithm),
            algorithm,
        }
    }

    /// Create an English Snowball stemmer filter.
    pub fn english() -> Self {
        Self::new(Algorithm::English)
    }

    /// Stem a single word.
    pub fn stem(&self, word: &str) -> String {
        self.stemmer.stem(word).into_owned()
    }
}

impl TokenFilter for SnowballStemmerFilter {
    fn filter(&self, tokens: Vec<Token>) -> Vec<Token> {
        tokens
            .into_iter()
            .map(|t| Token::new(self.stemmer.stem(&t.text).into_owned()))
            .collect()
    }
}

// ============================================================================
// Analyzer
// ============================================================================

/// Text analyzer combining a tokenizer and filters.
pub struct Analyzer {
    tokenizer: Box<dyn Tokenizer>,
    filters: Vec<Box<dyn TokenFilter>>,
}

impl Analyzer {
    /// Create a new analyzer with the given tokenizer and filters.
    pub fn new(tokenizer: Box<dyn Tokenizer>, filters: Vec<Box<dyn TokenFilter>>) -> Self {
        Self { tokenizer, filters }
    }

    /// Create the Clojure-parity English analyzer.
    ///
    /// Pipeline:
    /// 1. ClojureParityTokenizer (lowercase + word split)
    /// 2. StopwordFilter (English stopwords)
    /// 3. SnowballStemmerFilter (English stemmer)
    pub fn clojure_parity_english() -> Self {
        Self {
            tokenizer: Box::new(ClojureParityTokenizer),
            // NO LowercaseFilter - already done in tokenizer
            filters: vec![
                Box::new(StopwordFilter::english()),
                Box::new(SnowballStemmerFilter::english()),
            ],
        }
    }

    /// Analyze text into tokens.
    pub fn analyze(&self, text: &str) -> Vec<Token> {
        let mut tokens = self.tokenizer.tokenize(text);

        for filter in &self.filters {
            tokens = filter.filter(tokens);
        }

        tokens
    }

    /// Analyze text and return just the token strings.
    pub fn analyze_to_strings(&self, text: &str) -> Vec<String> {
        self.analyze(text).into_iter().map(|t| t.text).collect()
    }

    /// Analyze text and compute term frequencies.
    pub fn analyze_to_term_freqs(&self, text: &str) -> std::collections::HashMap<String, u32> {
        let mut freqs = std::collections::HashMap::new();
        for token in self.analyze(text) {
            *freqs.entry(token.text).or_insert(0) += 1;
        }
        freqs
    }
}

// ============================================================================
// English Stopwords
// ============================================================================

/// English stopwords ported from Clojure's stopwords/en.txt
///
/// This list is loaded at compile time from the resources directory for
/// exact parity with the Clojure implementation.
static ENGLISH_STOPWORDS: Lazy<HashSet<String>> = Lazy::new(|| {
    // Load the stopwords file at compile time
    const STOPWORDS_FILE: &str = include_str!("../../resources/stopwords/en.txt");

    STOPWORDS_FILE
        .lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .map(|s| s.to_string())
        .collect()
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clojure_parity_tokenizer() {
        let tokenizer = ClojureParityTokenizer;

        let tokens = tokenizer.tokenize("Hello, World!");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].text, "hello");
        assert_eq!(tokens[1].text, "world");
    }

    #[test]
    fn test_tokenizer_unicode() {
        let tokenizer = ClojureParityTokenizer;

        // Rust regex \w includes Unicode letters by default (better for i18n)
        let tokens = tokenizer.tokenize("café résumé");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].text, "café");
        assert_eq!(tokens[1].text, "résumé");
    }

    #[test]
    fn test_tokenizer_numbers() {
        let tokenizer = ClojureParityTokenizer;

        let tokens = tokenizer.tokenize("test123 hello_world foo42bar");
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].text, "test123");
        assert_eq!(tokens[1].text, "hello_world");
        assert_eq!(tokens[2].text, "foo42bar");
    }

    #[test]
    fn test_stopword_filter() {
        let filter = StopwordFilter::english();

        let tokens = vec![
            Token::new("the"),
            Token::new("quick"),
            Token::new("brown"),
            Token::new("fox"),
        ];

        let filtered = filter.filter(tokens);
        assert_eq!(filtered.len(), 3);
        assert!(!filtered.iter().any(|t| t.text == "the"));
    }

    #[test]
    fn test_stemmer_filter() {
        let filter = SnowballStemmerFilter::english();

        let tokens = vec![
            Token::new("running"),
            Token::new("jumped"),
            Token::new("foxes"),
        ];

        let stemmed = filter.filter(tokens);
        assert_eq!(stemmed[0].text, "run");
        assert_eq!(stemmed[1].text, "jump");
        assert_eq!(stemmed[2].text, "fox");
    }

    #[test]
    fn test_analyzer_full_pipeline() {
        let analyzer = Analyzer::clojure_parity_english();

        let terms = analyzer.analyze_to_strings("The quick brown foxes are running!");

        // "the" and "are" should be filtered as stopwords
        // "foxes" -> "fox", "running" -> "run"
        assert!(terms.contains(&"quick".to_string()));
        assert!(terms.contains(&"brown".to_string()));
        assert!(terms.contains(&"fox".to_string()));
        assert!(terms.contains(&"run".to_string()));
        assert!(!terms.contains(&"the".to_string()));
        assert!(!terms.contains(&"are".to_string()));
    }

    #[test]
    fn test_analyzer_term_freqs() {
        let analyzer = Analyzer::clojure_parity_english();

        let freqs = analyzer.analyze_to_term_freqs("fox fox fox dog dog cat");

        assert_eq!(freqs.get("fox"), Some(&3));
        assert_eq!(freqs.get("dog"), Some(&2));
        assert_eq!(freqs.get("cat"), Some(&1));
    }

    #[test]
    fn test_analyzer_empty_input() {
        let analyzer = Analyzer::clojure_parity_english();

        let terms = analyzer.analyze_to_strings("");
        assert!(terms.is_empty());

        let terms = analyzer.analyze_to_strings("   ");
        assert!(terms.is_empty());
    }

    #[test]
    fn test_analyzer_only_stopwords() {
        let analyzer = Analyzer::clojure_parity_english();

        let terms = analyzer.analyze_to_strings("the a an is are");
        assert!(terms.is_empty());
    }
}
