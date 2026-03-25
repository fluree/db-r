//! Text analyzer (default English) — re-exported from `fluree-db-binary-index`.
//!
//! The canonical implementation lives in `fluree_db_binary_index::analyzer`.
//! This module re-exports everything so that existing consumers within
//! `fluree-db-query` (and downstream crates) continue to compile unchanged.

pub use fluree_db_binary_index::analyzer::{
    analyze_to_term_freqs, analyze_to_term_freqs_with_lang, Analyzer, DefaultEnglishTokenizer,
    Language, SnowballStemmerFilter, StopwordFilter, Token, TokenFilter, Tokenizer,
    SUPPORTED_LANGUAGES,
};
