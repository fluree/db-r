//! Multi-lingual text analyzer for BM25 scoring and fulltext indexing.
//!
//! Implements the text analysis pipeline used for BM25 scoring and fulltext
//! arena building:
//! 1. Lowercase (in tokenizer)
//! 2. Split on `[^\w]+` (regex word split)
//! 3. Stopword filtering (per-language)
//! 4. Snowball stemming (per-language)
//!
//! Supports all 18 Snowball languages plus a no-stemmer fallback for
//! unsupported languages. The `Language` enum maps BCP-47 tags to the
//! appropriate pipeline components.
//!
//! This module is the single source of truth for text analysis. Both the
//! query-time BM25 scorer (`fluree-db-query`) and the index-time fulltext
//! arena builder (`fluree-db-indexer`) consume it, ensuring identical
//! tokenization/stemming so that indexed and novelty data produce consistent
//! BM25 scores.

use std::collections::{HashMap, HashSet};

use once_cell::sync::Lazy;
use regex::Regex;
use rust_stemmers::{Algorithm, Stemmer};

// ============================================================================
// Language
// ============================================================================

/// Supported text analysis languages.
///
/// Each variant maps to a Snowball stemming algorithm and a language-specific
/// stopword list. `Unknown` is used for languages without Snowball support
/// (e.g., Chinese, Japanese) — it applies tokenization only, with no stemming
/// and no stopword removal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Language {
    Arabic,
    Danish,
    Dutch,
    English,
    Finnish,
    French,
    German,
    Greek,
    Hungarian,
    Italian,
    Norwegian,
    Portuguese,
    Romanian,
    Russian,
    Spanish,
    Swedish,
    Tamil,
    Turkish,
    /// Fallback for unsupported languages: tokenize + lowercase only.
    Unknown,
}

/// All languages that have Snowball stemmer support.
pub const SUPPORTED_LANGUAGES: &[Language] = &[
    Language::Arabic,
    Language::Danish,
    Language::Dutch,
    Language::English,
    Language::Finnish,
    Language::French,
    Language::German,
    Language::Greek,
    Language::Hungarian,
    Language::Italian,
    Language::Norwegian,
    Language::Portuguese,
    Language::Romanian,
    Language::Russian,
    Language::Spanish,
    Language::Swedish,
    Language::Tamil,
    Language::Turkish,
];

impl Language {
    /// Parse a BCP-47 language tag into a `Language`.
    ///
    /// Matches on the primary subtag only (e.g., `"fr-CA"` → `French`).
    /// Returns `Unknown` for unrecognized tags.
    pub fn from_bcp47(tag: &str) -> Self {
        // Extract primary subtag (everything before the first '-')
        let primary = tag.split('-').next().unwrap_or(tag);
        match primary {
            "ar" => Self::Arabic,
            "da" => Self::Danish,
            "nl" => Self::Dutch,
            "en" => Self::English,
            "fi" => Self::Finnish,
            "fr" => Self::French,
            "de" => Self::German,
            "el" => Self::Greek,
            "hu" => Self::Hungarian,
            "it" => Self::Italian,
            "nb" | "nn" | "no" => Self::Norwegian,
            "pt" => Self::Portuguese,
            "ro" => Self::Romanian,
            "ru" => Self::Russian,
            "es" => Self::Spanish,
            "sv" => Self::Swedish,
            "ta" => Self::Tamil,
            "tr" => Self::Turkish,
            _ => Self::Unknown,
        }
    }

    /// Returns the canonical BCP-47 primary subtag for this language.
    ///
    /// Returns `None` for `Unknown`.
    pub fn tag(self) -> Option<&'static str> {
        match self {
            Self::Arabic => Some("ar"),
            Self::Danish => Some("da"),
            Self::Dutch => Some("nl"),
            Self::English => Some("en"),
            Self::Finnish => Some("fi"),
            Self::French => Some("fr"),
            Self::German => Some("de"),
            Self::Greek => Some("el"),
            Self::Hungarian => Some("hu"),
            Self::Italian => Some("it"),
            Self::Norwegian => Some("nb"),
            Self::Portuguese => Some("pt"),
            Self::Romanian => Some("ro"),
            Self::Russian => Some("ru"),
            Self::Spanish => Some("es"),
            Self::Swedish => Some("sv"),
            Self::Tamil => Some("ta"),
            Self::Turkish => Some("tr"),
            Self::Unknown => None,
        }
    }

    /// Returns the Snowball stemming algorithm for this language.
    ///
    /// Returns `None` for `Unknown`.
    pub fn algorithm(self) -> Option<Algorithm> {
        match self {
            Self::Arabic => Some(Algorithm::Arabic),
            Self::Danish => Some(Algorithm::Danish),
            Self::Dutch => Some(Algorithm::Dutch),
            Self::English => Some(Algorithm::English),
            Self::Finnish => Some(Algorithm::Finnish),
            Self::French => Some(Algorithm::French),
            Self::German => Some(Algorithm::German),
            Self::Greek => Some(Algorithm::Greek),
            Self::Hungarian => Some(Algorithm::Hungarian),
            Self::Italian => Some(Algorithm::Italian),
            Self::Norwegian => Some(Algorithm::Norwegian),
            Self::Portuguese => Some(Algorithm::Portuguese),
            Self::Romanian => Some(Algorithm::Romanian),
            Self::Russian => Some(Algorithm::Russian),
            Self::Spanish => Some(Algorithm::Spanish),
            Self::Swedish => Some(Algorithm::Swedish),
            Self::Tamil => Some(Algorithm::Tamil),
            Self::Turkish => Some(Algorithm::Turkish),
            Self::Unknown => None,
        }
    }

    /// Returns `true` if this language has Snowball stemmer support.
    pub fn has_stemmer(self) -> bool {
        self.algorithm().is_some()
    }

    /// Returns the analyzer version string for this language.
    ///
    /// Used in the search protocol to verify index/query analyzer compatibility.
    pub fn analyzer_version(self) -> &'static str {
        match self {
            Self::Arabic => "arabic_v1",
            Self::Danish => "danish_v1",
            Self::Dutch => "dutch_v1",
            Self::English => "english_default_v1",
            Self::Finnish => "finnish_v1",
            Self::French => "french_v1",
            Self::German => "german_v1",
            Self::Greek => "greek_v1",
            Self::Hungarian => "hungarian_v1",
            Self::Italian => "italian_v1",
            Self::Norwegian => "norwegian_v1",
            Self::Portuguese => "portuguese_v1",
            Self::Romanian => "romanian_v1",
            Self::Russian => "russian_v1",
            Self::Spanish => "spanish_v1",
            Self::Swedish => "swedish_v1",
            Self::Tamil => "tamil_v1",
            Self::Turkish => "turkish_v1",
            Self::Unknown => "nostem_v1",
        }
    }
}

impl std::fmt::Display for Language {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.tag() {
            Some(tag) => write!(f, "{tag}"),
            None => write!(f, "unknown"),
        }
    }
}

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
static WORD_SPLIT: Lazy<Regex> = Lazy::new(|| {
    // `[^\w]+` in regex means one or more non-word characters
    // Note: `\w` semantics depend on the regex engine's Unicode configuration.
    Regex::new(r"[^\w]+").expect("Invalid regex")
});

/// Default English tokenizer.
///
/// Performs:
/// 1. Lowercase the input text
/// 2. Split on `[^\w]+` (non-word characters)
///
/// Note: Lowercase is done in the tokenizer (not as a separate filter).
#[derive(Debug, Default, Clone)]
pub struct DefaultEnglishTokenizer;

impl Tokenizer for DefaultEnglishTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        // Lowercase first.
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

    /// Create a stopword filter for a specific language.
    ///
    /// Returns `None` for `Language::Unknown` (no stopwords to filter).
    pub fn for_language(lang: Language) -> Option<Self> {
        stopwords_for(lang).map(|sw| Self::new(sw.clone()))
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

    /// Create the default English analyzer.
    ///
    /// Pipeline:
    /// 1. DefaultEnglishTokenizer (lowercase + word split)
    /// 2. StopwordFilter (English stopwords)
    /// 3. SnowballStemmerFilter (English stemmer)
    pub fn english_default() -> Self {
        Self::for_language(Language::English)
    }

    /// Create an analyzer for the given language.
    ///
    /// For known Snowball languages: tokenize → stopword filter → Snowball stemmer.
    /// For `Unknown`: tokenize only (lowercase + word split, no stemming, no stopwords).
    pub fn for_language(lang: Language) -> Self {
        let tokenizer: Box<dyn Tokenizer> = Box::new(DefaultEnglishTokenizer);

        let mut filters: Vec<Box<dyn TokenFilter>> = Vec::new();

        if let Some(sw_filter) = StopwordFilter::for_language(lang) {
            filters.push(Box::new(sw_filter));
        }

        if let Some(algo) = lang.algorithm() {
            filters.push(Box::new(SnowballStemmerFilter::new(algo)));
        }

        Self { tokenizer, filters }
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
    pub fn analyze_to_term_freqs(&self, text: &str) -> HashMap<String, u32> {
        let mut freqs = HashMap::new();
        for token in self.analyze(text) {
            *freqs.entry(token.text).or_insert(0) += 1;
        }
        freqs
    }
}

// ============================================================================
// Standalone analysis functions (no trait dispatch)
// ============================================================================

/// English Snowball stemmer (shared static instance).
static ENGLISH_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::English));

/// Analyze text into term frequencies using the default English pipeline.
///
/// This is the non-trait-dispatch equivalent of
/// `Analyzer::english_default().analyze_to_term_freqs(text)`. It produces
/// identical results but avoids `Box<dyn>` overhead, making it suitable for
/// hot paths such as bulk index building.
///
/// Pipeline: lowercase → split on `[^\w]+` → stopword removal → Snowball stem → count.
pub fn analyze_to_term_freqs(text: &str) -> HashMap<String, u32> {
    analyze_to_term_freqs_with_lang(text, Language::English)
}

/// Analyze text into term frequencies using the pipeline for the given language.
///
/// Non-trait-dispatch equivalent of
/// `Analyzer::for_language(lang).analyze_to_term_freqs(text)`.
///
/// For known Snowball languages: lowercase → split → stopword removal → stem → count.
/// For `Unknown`: lowercase → split → count (no stopwords, no stemming).
pub fn analyze_to_term_freqs_with_lang(text: &str, lang: Language) -> HashMap<String, u32> {
    let lowered = text.to_lowercase();
    let stopwords = stopwords_for(lang);
    let stemmer = stemmer_for(lang);
    let mut freqs = HashMap::new();

    for token in WORD_SPLIT.split(&lowered) {
        if token.is_empty() {
            continue;
        }
        if let Some(sw) = stopwords {
            if sw.contains(token) {
                continue;
            }
        }
        let term = match stemmer {
            Some(s) => s.stem(token).into_owned(),
            None => token.to_string(),
        };
        if term.is_empty() {
            continue;
        }
        *freqs.entry(term).or_insert(0) += 1;
    }
    freqs
}

// ============================================================================
// Per-language stopwords (compile-time embedded)
// ============================================================================

/// Helper to parse a stopwords file into a `HashSet`.
fn parse_stopwords(file: &str) -> HashSet<String> {
    file.lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|s| s.to_lowercase())
        .collect()
}

/// Return the static stopword set for a language, or `None` for `Unknown`.
fn stopwords_for(lang: Language) -> Option<&'static HashSet<String>> {
    match lang {
        Language::Arabic => Some(&*ARABIC_STOPWORDS),
        Language::Danish => Some(&*DANISH_STOPWORDS),
        Language::Dutch => Some(&*DUTCH_STOPWORDS),
        Language::English => Some(&*ENGLISH_STOPWORDS),
        Language::Finnish => Some(&*FINNISH_STOPWORDS),
        Language::French => Some(&*FRENCH_STOPWORDS),
        Language::German => Some(&*GERMAN_STOPWORDS),
        Language::Greek => Some(&*GREEK_STOPWORDS),
        Language::Hungarian => Some(&*HUNGARIAN_STOPWORDS),
        Language::Italian => Some(&*ITALIAN_STOPWORDS),
        Language::Norwegian => Some(&*NORWEGIAN_STOPWORDS),
        Language::Portuguese => Some(&*PORTUGUESE_STOPWORDS),
        Language::Romanian => Some(&*ROMANIAN_STOPWORDS),
        Language::Russian => Some(&*RUSSIAN_STOPWORDS),
        Language::Spanish => Some(&*SPANISH_STOPWORDS),
        Language::Swedish => Some(&*SWEDISH_STOPWORDS),
        Language::Tamil => Some(&*TAMIL_STOPWORDS),
        Language::Turkish => Some(&*TURKISH_STOPWORDS),
        Language::Unknown => None,
    }
}

/// Return a static stemmer for a language, or `None` for `Unknown`.
fn stemmer_for(lang: Language) -> Option<&'static Stemmer> {
    match lang {
        Language::Arabic => Some(&*ARABIC_STEMMER),
        Language::Danish => Some(&*DANISH_STEMMER),
        Language::Dutch => Some(&*DUTCH_STEMMER),
        Language::English => Some(&*ENGLISH_STEMMER),
        Language::Finnish => Some(&*FINNISH_STEMMER),
        Language::French => Some(&*FRENCH_STEMMER),
        Language::German => Some(&*GERMAN_STEMMER),
        Language::Greek => Some(&*GREEK_STEMMER),
        Language::Hungarian => Some(&*HUNGARIAN_STEMMER),
        Language::Italian => Some(&*ITALIAN_STEMMER),
        Language::Norwegian => Some(&*NORWEGIAN_STEMMER),
        Language::Portuguese => Some(&*PORTUGUESE_STEMMER),
        Language::Romanian => Some(&*ROMANIAN_STEMMER),
        Language::Russian => Some(&*RUSSIAN_STEMMER),
        Language::Spanish => Some(&*SPANISH_STEMMER),
        Language::Swedish => Some(&*SWEDISH_STEMMER),
        Language::Tamil => Some(&*TAMIL_STEMMER),
        Language::Turkish => Some(&*TURKISH_STEMMER),
        Language::Unknown => None,
    }
}

// -- Stopword sets (compile-time embedded from resources/stopwords/) ----------

static ARABIC_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/ar.txt")));
static DANISH_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/da.txt")));
static DUTCH_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/nl.txt")));
static ENGLISH_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/en.txt")));
static FINNISH_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/fi.txt")));
static FRENCH_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/fr.txt")));
static GERMAN_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/de.txt")));
static GREEK_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/el.txt")));
static HUNGARIAN_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/hu.txt")));
static ITALIAN_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/it.txt")));
static NORWEGIAN_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/nb.txt")));
static PORTUGUESE_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/pt.txt")));
static ROMANIAN_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/ro.txt")));
static RUSSIAN_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/ru.txt")));
static SPANISH_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/es.txt")));
static SWEDISH_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/sv.txt")));
static TAMIL_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/ta.txt")));
static TURKISH_STOPWORDS: Lazy<HashSet<String>> =
    Lazy::new(|| parse_stopwords(include_str!("../resources/stopwords/tr.txt")));

// -- Per-language stemmers (lazy-initialized) --------------------------------

static ARABIC_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::Arabic));
static DANISH_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::Danish));
static DUTCH_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::Dutch));
// ENGLISH_STEMMER defined above (used by both `analyze_to_term_freqs` and this table).
static FINNISH_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::Finnish));
static FRENCH_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::French));
static GERMAN_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::German));
static GREEK_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::Greek));
static HUNGARIAN_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::Hungarian));
static ITALIAN_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::Italian));
static NORWEGIAN_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::Norwegian));
static PORTUGUESE_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::Portuguese));
static ROMANIAN_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::Romanian));
static RUSSIAN_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::Russian));
static SPANISH_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::Spanish));
static SWEDISH_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::Swedish));
static TAMIL_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::Tamil));
static TURKISH_STEMMER: Lazy<Stemmer> = Lazy::new(|| Stemmer::create(Algorithm::Turkish));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_english_tokenizer() {
        let tokenizer = DefaultEnglishTokenizer;

        let tokens = tokenizer.tokenize("Hello, World!");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].text, "hello");
        assert_eq!(tokens[1].text, "world");
    }

    #[test]
    fn test_tokenizer_unicode() {
        let tokenizer = DefaultEnglishTokenizer;

        // Rust regex \w includes Unicode letters by default (better for i18n)
        let tokens = tokenizer.tokenize("café résumé");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].text, "café");
        assert_eq!(tokens[1].text, "résumé");
    }

    #[test]
    fn test_tokenizer_numbers() {
        let tokenizer = DefaultEnglishTokenizer;

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
        let analyzer = Analyzer::english_default();

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
        let analyzer = Analyzer::english_default();

        let freqs = analyzer.analyze_to_term_freqs("fox fox fox dog dog cat");

        assert_eq!(freqs.get("fox"), Some(&3));
        assert_eq!(freqs.get("dog"), Some(&2));
        assert_eq!(freqs.get("cat"), Some(&1));
    }

    #[test]
    fn test_analyzer_empty_input() {
        let analyzer = Analyzer::english_default();

        let terms = analyzer.analyze_to_strings("");
        assert!(terms.is_empty());

        let terms = analyzer.analyze_to_strings("   ");
        assert!(terms.is_empty());
    }

    #[test]
    fn test_analyzer_only_stopwords() {
        let analyzer = Analyzer::english_default();

        let terms = analyzer.analyze_to_strings("the a an is are");
        assert!(terms.is_empty());
    }

    // ========================================================================
    // Standalone function tests
    // ========================================================================

    #[test]
    fn test_standalone_analyze_basic() {
        let freqs = analyze_to_term_freqs("The quick brown fox jumps over the lazy dog");
        // "the" and "over" are stopwords
        assert!(!freqs.contains_key("the"));
        assert!(!freqs.contains_key("over"));
        // "quick", "brown", "fox", "jump" (stemmed), "lazi" (stemmed), "dog"
        assert!(freqs.contains_key("quick"));
        assert!(freqs.contains_key("fox"));
        // "jumps" stems to "jump"
        assert!(freqs.contains_key("jump"));
    }

    #[test]
    fn test_standalone_analyze_stemming() {
        let freqs = analyze_to_term_freqs("indexing indexed indexes");
        // All should stem to "index"
        assert_eq!(freqs.len(), 1);
        assert_eq!(freqs["index"], 3);
    }

    #[test]
    fn test_standalone_analyze_empty() {
        let freqs = analyze_to_term_freqs("");
        assert!(freqs.is_empty());

        // All stopwords
        let freqs = analyze_to_term_freqs("the a an is are was");
        assert!(freqs.is_empty());
    }

    #[test]
    fn test_standalone_matches_analyzer() {
        // The standalone function must produce identical output to the trait-based Analyzer.
        let analyzer = Analyzer::english_default();
        let texts = [
            "The quick brown fox jumps over the lazy dog",
            "indexing indexed indexes",
            "Rust programming language is awesome",
            "",
            "the a an is are was",
            "Hello, World! This is a test of the analyzer.",
        ];
        for text in &texts {
            let trait_freqs = analyzer.analyze_to_term_freqs(text);
            let fn_freqs = analyze_to_term_freqs(text);
            assert_eq!(trait_freqs, fn_freqs, "mismatch for input: {text:?}");
        }
    }

    // ========================================================================
    // Language enum tests
    // ========================================================================

    #[test]
    fn test_bcp47_parsing() {
        assert_eq!(Language::from_bcp47("en"), Language::English);
        assert_eq!(Language::from_bcp47("en-US"), Language::English);
        assert_eq!(Language::from_bcp47("en-GB"), Language::English);
        assert_eq!(Language::from_bcp47("fr"), Language::French);
        assert_eq!(Language::from_bcp47("fr-CA"), Language::French);
        assert_eq!(Language::from_bcp47("de"), Language::German);
        assert_eq!(Language::from_bcp47("de-AT"), Language::German);
        assert_eq!(Language::from_bcp47("es"), Language::Spanish);
        assert_eq!(Language::from_bcp47("pt"), Language::Portuguese);
        assert_eq!(Language::from_bcp47("pt-BR"), Language::Portuguese);
        assert_eq!(Language::from_bcp47("ru"), Language::Russian);
        assert_eq!(Language::from_bcp47("ar"), Language::Arabic);
        assert_eq!(Language::from_bcp47("ta"), Language::Tamil);
        assert_eq!(Language::from_bcp47("tr"), Language::Turkish);
        // Norwegian variants
        assert_eq!(Language::from_bcp47("nb"), Language::Norwegian);
        assert_eq!(Language::from_bcp47("nn"), Language::Norwegian);
        assert_eq!(Language::from_bcp47("no"), Language::Norwegian);
        // Unknown
        assert_eq!(Language::from_bcp47("zh"), Language::Unknown);
        assert_eq!(Language::from_bcp47("ja"), Language::Unknown);
        assert_eq!(Language::from_bcp47("ko"), Language::Unknown);
        assert_eq!(Language::from_bcp47(""), Language::Unknown);
    }

    #[test]
    fn test_language_tag_roundtrip() {
        for lang in SUPPORTED_LANGUAGES {
            let tag = lang.tag().expect("supported language should have a tag");
            let parsed = Language::from_bcp47(tag);
            assert_eq!(*lang, parsed, "roundtrip failed for {lang:?}");
        }
    }

    #[test]
    fn test_language_algorithm_mapping() {
        assert_eq!(Language::English.algorithm(), Some(Algorithm::English));
        assert_eq!(Language::French.algorithm(), Some(Algorithm::French));
        assert_eq!(Language::German.algorithm(), Some(Algorithm::German));
        assert_eq!(Language::Unknown.algorithm(), None);
    }

    #[test]
    fn test_all_supported_languages_have_stopwords() {
        for lang in SUPPORTED_LANGUAGES {
            let sw = stopwords_for(*lang);
            assert!(
                sw.is_some(),
                "missing stopwords for {lang:?}"
            );
            assert!(
                !sw.unwrap().is_empty(),
                "empty stopwords for {lang:?}"
            );
        }
    }

    #[test]
    fn test_unknown_has_no_stopwords() {
        assert!(stopwords_for(Language::Unknown).is_none());
    }

    // ========================================================================
    // Multi-lingual analyzer tests
    // ========================================================================

    #[test]
    fn test_french_analyzer() {
        let analyzer = Analyzer::for_language(Language::French);
        let terms = analyzer.analyze_to_strings("Les chats mangent des souris");
        // "les" and "des" are French stopwords
        assert!(!terms.iter().any(|t| t == "les"));
        assert!(!terms.iter().any(|t| t == "des"));
        // "chats" should be stemmed
        assert!(terms.iter().any(|t| t == "chat"), "expected 'chat' in {terms:?}");
    }

    #[test]
    fn test_german_analyzer() {
        let analyzer = Analyzer::for_language(Language::German);
        let terms = analyzer.analyze_to_strings("Die Katzen fressen die Mäuse");
        // "die" is a German stopword
        assert!(!terms.iter().any(|t| t == "die"));
        // "Katzen" → "katz" (Snowball German)
        assert!(terms.iter().any(|t| t == "katz"), "expected 'katz' in {terms:?}");
    }

    #[test]
    fn test_spanish_analyzer() {
        let analyzer = Analyzer::for_language(Language::Spanish);
        let terms = analyzer.analyze_to_strings("Los gatos comen los ratones");
        // "los" is a Spanish stopword
        assert!(!terms.iter().any(|t| t == "los"));
        // "gatos" should be stemmed to "gat"
        assert!(terms.iter().any(|t| t == "gat"), "expected 'gat' in {terms:?}");
    }

    #[test]
    fn test_russian_analyzer() {
        let analyzer = Analyzer::for_language(Language::Russian);
        let terms = analyzer.analyze_to_strings("кошки едят мышей");
        // Should tokenize and stem Russian text
        assert!(!terms.is_empty(), "Russian text should produce tokens");
    }

    #[test]
    fn test_unknown_language_no_stemming() {
        let analyzer = Analyzer::for_language(Language::Unknown);
        // "running" should NOT be stemmed for unknown language
        let terms = analyzer.analyze_to_strings("running foxes");
        assert!(
            terms.contains(&"running".to_string()),
            "Unknown language should not stem: {terms:?}"
        );
        assert!(
            terms.contains(&"foxes".to_string()),
            "Unknown language should not stem: {terms:?}"
        );
    }

    #[test]
    fn test_unknown_language_no_stopwords() {
        let analyzer = Analyzer::for_language(Language::Unknown);
        // "the" would be an English stopword, but Unknown has no stopwords
        let terms = analyzer.analyze_to_strings("the quick brown fox");
        assert!(
            terms.contains(&"the".to_string()),
            "Unknown language should keep all tokens: {terms:?}"
        );
    }

    // ========================================================================
    // Standalone with_lang function tests
    // ========================================================================

    #[test]
    fn test_standalone_with_lang_french() {
        let freqs = analyze_to_term_freqs_with_lang("Les chats mangent des souris", Language::French);
        assert!(!freqs.contains_key("les"));
        assert!(!freqs.contains_key("des"));
        assert!(freqs.contains_key("chat"), "expected 'chat' in {freqs:?}");
    }

    #[test]
    fn test_standalone_with_lang_unknown() {
        let freqs = analyze_to_term_freqs_with_lang("running foxes the", Language::Unknown);
        // No stemming, no stopwords
        assert!(freqs.contains_key("running"));
        assert!(freqs.contains_key("foxes"));
        assert!(freqs.contains_key("the"));
    }

    #[test]
    fn test_standalone_with_lang_matches_trait_analyzer() {
        // For every supported language, the standalone function must produce
        // identical results to the trait-based Analyzer.
        let test_text = "The quick brown fox jumps over the lazy dog";
        for lang in SUPPORTED_LANGUAGES {
            let analyzer = Analyzer::for_language(*lang);
            let trait_freqs = analyzer.analyze_to_term_freqs(test_text);
            let fn_freqs = analyze_to_term_freqs_with_lang(test_text, *lang);
            assert_eq!(trait_freqs, fn_freqs, "mismatch for {lang:?}");
        }
        // Also test Unknown
        let analyzer = Analyzer::for_language(Language::Unknown);
        let trait_freqs = analyzer.analyze_to_term_freqs(test_text);
        let fn_freqs = analyze_to_term_freqs_with_lang(test_text, Language::Unknown);
        assert_eq!(trait_freqs, fn_freqs, "mismatch for Unknown");
    }

    #[test]
    fn test_english_backward_compat() {
        // Verify that the refactored english_default() produces identical
        // results to what the old hardcoded implementation would have.
        let analyzer = Analyzer::english_default();
        let freqs_trait = analyzer.analyze_to_term_freqs("The quick brown foxes are running!");
        let freqs_fn = analyze_to_term_freqs("The quick brown foxes are running!");
        assert_eq!(freqs_trait, freqs_fn);
    }
}
